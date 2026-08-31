
use std::collections::HashMap;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct FileStats {
    pub num_records: Option<u64>,
    pub columns: HashMap<String, ColumnStats>,
}

#[derive(Debug, Clone)]
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

fn col_entry(columns: &mut HashMap<String, ColumnStats>, key: String) -> &mut ColumnStats {
    columns.entry(key).or_insert_with(|| ColumnStats {
        min: None,
        max: None,
        null_count: None,
    })
}

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
