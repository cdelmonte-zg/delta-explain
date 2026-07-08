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

/// Table-level facts read straight from the JSON commits of the Delta log,
/// collected in one pass.
pub struct LogMetadata {
    /// From the last `metaData` action: authoritative per the protocol, and
    /// the only source that covers empty tables. Empty on a fully
    /// checkpointed log; the caller falls back to
    /// `scan::partition_columns_from_files`.
    pub partition_columns: Vec<String>,
    /// Raw `configuration` payload of the last surviving `delta.clustering`
    /// domainMetadata action. delta-kernel 0.24 exposes no public accessor
    /// for system domains, so this comes from the JSON commits directly;
    /// on a fully checkpointed log clustering goes undetected (same blind
    /// spot as the partition columns above).
    pub clustering_domain: Option<String>,
    /// Reader/writer table features from the last `protocol` action in the
    /// JSON commits (empty on pre-feature protocols and on fully
    /// checkpointed logs).
    pub reader_features: Vec<String>,
    pub writer_features: Vec<String>,
}

/// Read partition columns and the clustering domain from the Delta log in a
/// single pass over the JSON commits.
pub fn read_log_metadata(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
) -> Result<LogMetadata, Error> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| Error::Storage(format!("Cannot create tokio runtime: {e}")))?;
    rt.block_on(read_log_metadata_async(table_url, store))
}

/// The `_delta_log` prefix inside the store, derived from the table URL's
/// path alone. The store handed to the reader is already scoped exactly
/// like the one the engine uses (both come from `store_from_url_opts`), so
/// re-parsing the URL into a second store is unnecessary - and on `az://`
/// it is impossible without credentials in hand, which is how Azure tables
/// used to fail here ("Account must be specified") before this derivation.
fn log_prefix_for(table_url: &Url) -> Result<ObjectPath, Error> {
    let table_prefix = ObjectPath::from_url_path(table_url.path().trim_matches('/'))
        .map_err(|e| Error::Storage(format!("Cannot derive log path from table URL: {e}")))?;
    Ok(if table_prefix.as_ref().is_empty() {
        ObjectPath::from("_delta_log")
    } else {
        ObjectPath::from(format!("{}/_delta_log", table_prefix.as_ref()))
    })
}

async fn read_log_metadata_async(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
) -> Result<LogMetadata, Error> {
    let log_prefix = log_prefix_for(table_url)?;

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

    // The last action of each kind wins (schema evolution can replace the
    // metaData; a clustering domain can be rewritten or tombstoned).
    let mut partition_columns = Vec::new();
    let mut clustering_domain: Option<String> = None;
    let mut reader_features: Vec<String> = Vec::new();
    let mut writer_features: Vec<String> = Vec::new();

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

            if let Some(protocol) = action.get("protocol") {
                let feature_list = |key: &str| -> Vec<String> {
                    protocol
                        .get(key)
                        .and_then(|v| v.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|f| f.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default()
                };
                reader_features = feature_list("readerFeatures");
                writer_features = feature_list("writerFeatures");
            }

            if let Some(dm) = action.get("domainMetadata")
                && dm.get("domain").and_then(|v| v.as_str()) == Some("delta.clustering")
            {
                let removed = dm.get("removed").and_then(|v| v.as_bool()).unwrap_or(false);
                clustering_domain = if removed {
                    None
                } else {
                    dm.get("configuration")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                        .or(Some(String::new()))
                };
            }
        }
    }

    Ok(LogMetadata {
        partition_columns,
        clustering_domain,
        reader_features,
        writer_features,
    })
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
    fn log_prefix_derives_from_the_url_path_for_every_scheme() {
        let cases = [
            ("s3://bucket/prefix/table", "prefix/table/_delta_log"),
            ("s3://bucket/table", "table/_delta_log"),
            ("s3://bucket", "_delta_log"),
            ("s3://bucket/", "_delta_log"),
            // az:// carries the container in the host and the account only
            // in the options: deriving from the path must not require a
            // second store (the old parse_url-based code failed here).
            ("az://container/table", "table/_delta_log"),
            ("gs://bucket/lake/users", "lake/users/_delta_log"),
            ("file:///home/user/table", "home/user/table/_delta_log"),
            ("s3://bucket/pre%20fix/table", "pre fix/table/_delta_log"),
        ];
        for (url, expect) in cases {
            let u = Url::parse(url).unwrap();
            assert_eq!(log_prefix_for(&u).unwrap().as_ref(), expect, "url: {url}");
        }
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
