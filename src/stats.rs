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
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::json;
        let mut cols = serde_json::Map::new();
        for (col, cs) in &self.columns {
            let mut entry = serde_json::Map::new();
            if let Some(ref min) = cs.min {
                entry.insert("min".into(), json!(min));
            }
            if let Some(ref max) = cs.max {
                entry.insert("max".into(), json!(max));
            }
            if let Some(nc) = cs.null_count {
                entry.insert("null_count".into(), json!(nc));
            }
            cols.insert(col.clone(), Value::Object(entry));
        }
        let mut obj = serde_json::Map::new();
        if let Some(nr) = self.num_records {
            obj.insert("num_records".into(), json!(nr));
        }
        obj.insert("columns".into(), Value::Object(cols));
        Value::Object(obj)
    }

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
                let stats = parse_add_stats(add);
                result.insert(file_path.to_string(), stats);
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

    if let Some(ref stats) = stats_json {
        if let Some(min_values) = stats.get("minValues").and_then(|v| v.as_object()) {
            for (col, val) in min_values {
                let entry = columns.entry(col.clone()).or_insert_with(|| ColumnStats {
                    min: None,
                    max: None,
                    null_count: None,
                });
                entry.min = Some(format_stat_value(val));
            }
        }

        if let Some(max_values) = stats.get("maxValues").and_then(|v| v.as_object()) {
            for (col, val) in max_values {
                let entry = columns.entry(col.clone()).or_insert_with(|| ColumnStats {
                    min: None,
                    max: None,
                    null_count: None,
                });
                entry.max = Some(format_stat_value(val));
            }
        }

        if let Some(null_counts) = stats.get("nullCount").and_then(|v| v.as_object()) {
            for (col, val) in null_counts {
                let entry = columns.entry(col.clone()).or_insert_with(|| ColumnStats {
                    min: None,
                    max: None,
                    null_count: None,
                });
                entry.null_count = val.as_u64();
            }
        }
    }

    FileStats {
        num_records,
        columns,
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
