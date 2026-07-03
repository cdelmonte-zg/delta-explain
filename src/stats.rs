use std::collections::HashMap;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{DynObjectStore, ObjectStore, ObjectStoreExt};
use serde_json::Value;
use url::Url;

use crate::error::Error;

/// Per-file statistics extracted from the Delta log.
pub struct FileStats {
    pub num_records: Option<u64>,
    /// column_name -> (min, max) as display strings
    pub columns: HashMap<String, ColumnStats>,
}

pub struct ColumnStats {
    pub min: Option<String>,
    pub max: Option<String>,
    pub null_count: Option<u64>,
}

/// Parse a `stats` JSON payload into [`FileStats`]. Returns `None` when the
/// payload is not valid JSON or not a JSON object, so a malformed stats
/// string counts as missing statistics (`[no stats]` in the verbose view,
/// flagged by `--assert-stats`) rather than silently passing as an empty
/// entry.
pub(crate) fn parse_stats_json(stats_str: &str) -> Option<FileStats> {
    let stats = serde_json::from_str::<Value>(stats_str).ok()?;
    if !stats.is_object() {
        return None;
    }

    let num_records = stats.get("numRecords").and_then(|v| v.as_u64());

    let mut columns: HashMap<String, ColumnStats> = HashMap::new();

    // Delta nests stats for struct columns: minValues.profile = {age, score}.
    // Flatten them to dotted leaf keys (profile.age, profile.score) so each leaf
    // reports its own min/max, matching how the kernel skips on nested fields.
    {
        for (key, val) in flatten_leaves(stats.get("minValues")) {
            col_entry(&mut columns, key).min = Some(format_stat_value(val));
        }
        for (key, val) in flatten_leaves(stats.get("maxValues")) {
            col_entry(&mut columns, key).max = Some(format_stat_value(val));
        }
        for (key, val) in flatten_leaves(stats.get("nullCount")) {
            col_entry(&mut columns, key).null_count = val.as_u64();
        }
    }

    Some(FileStats {
        num_records,
        columns,
    })
}

/// Read partition columns from the `metadata` action in the Delta log.
///
/// Scans the JSON log files for the last `metaData` action and extracts the
/// `partitionColumns` array: the authoritative source per the Delta protocol,
/// and the only one that covers empty tables. On a fully checkpointed log no
/// JSON `metaData` survives and this returns empty; the caller then falls
/// back to `scan::partition_columns_from_files`, which derives the columns
/// from the kernel-replayed `partitionValues` keys.
pub fn read_partition_columns_from_log(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
) -> Result<Vec<String>, Error> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| Error::Storage(format!("Cannot create tokio runtime: {e}")))?;
    rt.block_on(read_partition_columns_async(table_url, store))
}

async fn read_partition_columns_async(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
) -> Result<Vec<String>, Error> {
    let (_, table_prefix) = object_store::parse_url(table_url)
        .map_err(|e| Error::Storage(format!("Cannot parse table URL: {e}")))?;

    let log_prefix = if table_prefix.as_ref().is_empty() {
        ObjectPath::from("_delta_log")
    } else {
        ObjectPath::from(format!(
            "{}/_delta_log",
            table_prefix.as_ref().trim_end_matches('/')
        ))
    };

    let objects: Vec<_> = store
        .list(Some(&log_prefix))
        .try_collect()
        .await
        .map_err(|e| Error::Storage(format!("Cannot list delta log: {e}")))?;

    let mut json_paths: Vec<ObjectPath> = objects
        .into_iter()
        .filter(|obj| obj.location.to_string().ends_with(".json"))
        .map(|obj| obj.location)
        .collect();
    json_paths.sort();

    // The last metaData action wins (schema evolution can replace it).
    let mut partition_columns = Vec::new();

    for path in json_paths {
        let data = store
            .get(&path)
            .await
            .map_err(|e| Error::Storage(format!("Cannot read {path}: {e}")))?
            .bytes()
            .await
            .map_err(|e| Error::Storage(format!("Cannot read bytes {path}: {e}")))?;

        let content = String::from_utf8_lossy(&data);

        for line in content.lines() {
            let Ok(action) = serde_json::from_str::<Value>(line) else {
                continue;
            };

            if let Some(meta) = action.get("metaData")
                && let Some(cols) = meta.get("partitionColumns").and_then(|v| v.as_array())
            {
                partition_columns = cols
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
            }
        }
    }

    Ok(partition_columns)
}

fn col_entry(columns: &mut HashMap<String, ColumnStats>, key: String) -> &mut ColumnStats {
    columns.entry(key).or_insert_with(|| ColumnStats {
        min: None,
        max: None,
        null_count: None,
    })
}

/// Flatten a stats object (minValues / maxValues / nullCount) into (dotted key,
/// leaf value) pairs. A scalar leaf is emitted as-is; a struct object recurses,
/// joining names with `.` (profile -> profile.age, profile.score).
fn flatten_leaves(value: Option<&Value>) -> Vec<(String, &Value)> {
    let mut out = Vec::new();
    if let Some(Value::Object(map)) = value {
        for (k, v) in map {
            push_leaves(k, v, &mut out);
        }
    }
    out
}

fn push_leaves<'a>(prefix: &str, value: &'a Value, out: &mut Vec<(String, &'a Value)>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                push_leaves(&format!("{prefix}.{k}"), v, out);
            }
        }
        _ => out.push((prefix.to_string(), value)),
    }
}

fn format_stat_value(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(f) = n.as_f64() {
                format!("{f}")
            } else {
                n.to_string()
            }
        }
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".into(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flattens_nested_struct_stats_to_dotted_keys() {
        let stats_str = "{\"numRecords\":2,\
            \"minValues\":{\"name\":\"Eve\",\"profile\":{\"age\":45,\"score\":71.5}},\
            \"maxValues\":{\"name\":\"Frank\",\"profile\":{\"age\":55,\"score\":82.0}},\
            \"nullCount\":{\"name\":0,\"profile\":{\"age\":0,\"score\":0}}}";

        let stats = parse_stats_json(stats_str).expect("valid stats JSON");

        let age = stats.columns.get("profile.age").expect("profile.age leaf");
        assert_eq!(age.min.as_deref(), Some("45"));
        assert_eq!(age.max.as_deref(), Some("55"));
        assert_eq!(age.null_count, Some(0));

        let score = stats
            .columns
            .get("profile.score")
            .expect("profile.score leaf");
        assert_eq!(score.min.as_deref(), Some("71.5"));

        // top-level scalar columns stay flat; the raw struct key is gone
        assert!(stats.columns.contains_key("name"));
        assert!(!stats.columns.contains_key("profile"));
    }

    #[test]
    fn malformed_stats_json_counts_as_missing() {
        assert!(parse_stats_json("not json").is_none());
        assert!(parse_stats_json("").is_none());
        // valid JSON of the wrong shape is just as missing as broken JSON
        assert!(parse_stats_json("\"a string\"").is_none());
        assert!(parse_stats_json("42").is_none());
        assert!(parse_stats_json("[1, 2]").is_none());
        assert!(parse_stats_json("null").is_none());
        // an empty object is a present-but-empty payload, kept as such
        assert!(parse_stats_json("{}").is_some());
    }
}
