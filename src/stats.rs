use std::collections::HashMap;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{DynObjectStore, ObjectStore};
use serde_json::Value;
use url::Url;

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

impl FileStats {
    pub fn format_compact(&self) -> String {
        if self.columns.is_empty() {
            return if self.num_records.is_some() {
                String::new()
            } else {
                "  [no stats]".into()
            };
        }

        let mut parts: Vec<String> = Vec::new();
        let mut cols: Vec<(&String, &ColumnStats)> = self.columns.iter().collect();
        cols.sort_by_key(|(k, _)| *k);

        for (col, stats) in cols {
            match (&stats.min, &stats.max) {
                (Some(min), Some(max)) => parts.push(format!("{col}: {min}..{max}")),
                (Some(min), None) => parts.push(format!("{col}: min={min}")),
                (None, Some(max)) => parts.push(format!("{col}: max={max}")),
                (None, None) => {}
            }
        }

        if parts.is_empty() {
            String::new()
        } else {
            format!("  stats({})", parts.join(", "))
        }
    }
}

/// Read stats from delta log JSON files via object_store (works for local and remote).
pub fn read_stats_from_log(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
) -> Result<HashMap<String, FileStats>, delta_kernel::Error> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| delta_kernel::Error::Generic(format!("Cannot create tokio runtime: {e}")))?;
    rt.block_on(read_stats_async(table_url, store))
}

async fn read_stats_async(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
) -> Result<HashMap<String, FileStats>, delta_kernel::Error> {
    // store_from_url_opts returns a store rooted at some prefix.
    // For local files, the store is rooted at "/" and the table path is absolute.
    // We need to compute the object_store path prefix for the _delta_log directory.
    let (_, table_prefix) = object_store::parse_url(table_url)
        .map_err(|e| delta_kernel::Error::Generic(format!("Cannot parse table URL: {e}")))?;

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
        .map_err(|e| delta_kernel::Error::Generic(format!("Cannot list delta log: {e}")))?;

    let mut json_paths: Vec<ObjectPath> = objects
        .into_iter()
        .filter(|obj| obj.location.to_string().ends_with(".json"))
        .map(|obj| obj.location)
        .collect();
    json_paths.sort();

    let mut result = HashMap::new();

    for path in json_paths {
        let data = store
            .get(&path)
            .await
            .map_err(|e| delta_kernel::Error::Generic(format!("Cannot read {path}: {e}")))?
            .bytes()
            .await
            .map_err(|e| delta_kernel::Error::Generic(format!("Cannot read bytes {path}: {e}")))?;

        let content = String::from_utf8_lossy(&data);

        for line in content.lines() {
            let Ok(action) = serde_json::from_str::<Value>(line) else {
                continue;
            };

            if let Some(add) = action.get("add") {
                let Some(file_path) = add.get("path").and_then(|v| v.as_str()) else {
                    continue;
                };
                // Only track files whose Add action actually carries a `stats`
                // payload. Files without log-level stats are skipped here; the
                // report layer treats their absence as "no stats".
                if add.get("stats").and_then(|v| v.as_str()).is_some() {
                    let stats = parse_add_stats(add);
                    result.insert(file_path.to_string(), stats);
                }
            }

            if let Some(remove) = action.get("remove")
                && let Some(file_path) = remove.get("path").and_then(|v| v.as_str())
            {
                result.remove(file_path);
            }
        }
    }

    Ok(result)
}

fn parse_add_stats(add: &Value) -> FileStats {
    let stats_json = add
        .get("stats")
        .and_then(|v| v.as_str())
        .and_then(|s| serde_json::from_str::<Value>(s).ok());

    let num_records = stats_json
        .as_ref()
        .and_then(|s| s.get("numRecords"))
        .and_then(|v| v.as_u64());

    let mut columns: HashMap<String, ColumnStats> = HashMap::new();

    // Delta nests stats for struct columns: minValues.profile = {age, score}.
    // Flatten them to dotted leaf keys (profile.age, profile.score) so each leaf
    // reports its own min/max, matching how the kernel skips on nested fields.
    if let Some(ref stats) = stats_json {
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

    FileStats {
        num_records,
        columns,
    }
}

/// Read partition columns from the `metadata` action in the Delta log.
///
/// Scans the JSON log files for the last `metaData` action and extracts the
/// `partitionColumns` array. This is the correct way to determine partition
/// columns per the Delta protocol — never infer from file partition values.
pub fn read_partition_columns_from_log(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
) -> Result<Vec<String>, delta_kernel::Error> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| delta_kernel::Error::Generic(format!("Cannot create tokio runtime: {e}")))?;
    rt.block_on(read_partition_columns_async(table_url, store))
}

async fn read_partition_columns_async(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
) -> Result<Vec<String>, delta_kernel::Error> {
    let (_, table_prefix) = object_store::parse_url(table_url)
        .map_err(|e| delta_kernel::Error::Generic(format!("Cannot parse table URL: {e}")))?;

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
        .map_err(|e| delta_kernel::Error::Generic(format!("Cannot list delta log: {e}")))?;

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
            .map_err(|e| delta_kernel::Error::Generic(format!("Cannot read {path}: {e}")))?
            .bytes()
            .await
            .map_err(|e| delta_kernel::Error::Generic(format!("Cannot read bytes {path}: {e}")))?;

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
    use serde_json::json;

    #[test]
    fn flattens_nested_struct_stats_to_dotted_keys() {
        let add = json!({
            "path": "f.parquet",
            "stats": "{\"numRecords\":2,\
                \"minValues\":{\"name\":\"Eve\",\"profile\":{\"age\":45,\"score\":71.5}},\
                \"maxValues\":{\"name\":\"Frank\",\"profile\":{\"age\":55,\"score\":82.0}},\
                \"nullCount\":{\"name\":0,\"profile\":{\"age\":0,\"score\":0}}}"
        });

        let stats = parse_add_stats(&add);

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
}
