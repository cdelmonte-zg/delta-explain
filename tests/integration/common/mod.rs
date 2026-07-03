//! Shared helpers for the integration suite: the binary under test, fixture
//! paths, and a builder that synthesizes Delta logs programmatically.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use assert_cmd::Command;
use tempfile::TempDir;

pub fn cmd() -> Command {
    Command::cargo_bin("delta-explain").unwrap()
}

/// Absolute path of a checked-in fixture under `fixtures/`.
pub fn fixture(name: &str) -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/fixtures/{name}")
}

/// Synthesizes a Delta log in a temp directory: protocol, metaData, and add
/// actions. delta-explain never reads parquet data, so the referenced data
/// files do not need to exist; scenarios that would take megabytes of
/// checked-in fixtures (scale, exotic protocol features) take a few lines
/// of code instead.
///
/// Reader/writer features force protocol (3, 7); otherwise (1, 2) is used.
pub struct LogBuilder {
    columns: Vec<(String, serde_json::Value)>,
    partition_columns: Vec<String>,
    configuration: BTreeMap<String, String>,
    reader_features: Vec<String>,
    writer_features: Vec<String>,
    adds: Vec<serde_json::Value>,
}

impl LogBuilder {
    pub fn new() -> Self {
        LogBuilder {
            columns: Vec::new(),
            partition_columns: Vec::new(),
            configuration: BTreeMap::new(),
            reader_features: Vec::new(),
            writer_features: Vec::new(),
            adds: Vec::new(),
        }
    }

    /// A primitive column: `kind` is the Delta type name ("string", "long",
    /// "integer", "double", "date", ...).
    pub fn column(mut self, name: &str, kind: &str) -> Self {
        self.columns.push((name.into(), serde_json::json!(kind)));
        self
    }

    pub fn partition_column(mut self, name: &str, kind: &str) -> Self {
        self.partition_columns.push(name.into());
        self.column(name, kind)
    }

    #[allow(dead_code)]
    pub fn property(mut self, key: &str, value: &str) -> Self {
        self.configuration.insert(key.into(), value.into());
        self
    }

    #[allow(dead_code)]
    pub fn reader_feature(mut self, feature: &str) -> Self {
        self.reader_features.push(feature.into());
        self
    }

    #[allow(dead_code)]
    pub fn writer_feature(mut self, feature: &str) -> Self {
        self.writer_features.push(feature.into());
        self
    }

    /// One add action. `partition_values` must name every partition column;
    /// `stats` is the stats JSON blob, or None for a stats-less file.
    pub fn add_file(
        mut self,
        path: &str,
        partition_values: &[(&str, &str)],
        stats: Option<serde_json::Value>,
    ) -> Self {
        let pv: BTreeMap<&str, &str> = partition_values.iter().copied().collect();
        let mut add = serde_json::json!({
            "path": path,
            "partitionValues": pv,
            "size": 1024,
            "modificationTime": 1_750_000_000_000_u64,
            "dataChange": true,
        });
        if let Some(s) = stats {
            add["stats"] = serde_json::json!(s.to_string());
        }
        self.adds.push(serde_json::json!({ "add": add }));
        self
    }

    /// Bulk generation for scale scenarios: `f(i)` returns
    /// (path, partition_values, stats) for the i-th file.
    pub fn add_files<F>(mut self, n: usize, f: F) -> Self
    where
        F: Fn(usize) -> (String, Vec<(String, String)>, Option<serde_json::Value>),
    {
        for i in 0..n {
            let (path, pv, stats) = f(i);
            let pv_ref: Vec<(&str, &str)> =
                pv.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            self = self.add_file(&path, &pv_ref, stats);
        }
        self
    }

    /// Writes the log and returns the table handle; the directory lives as
    /// long as the returned value.
    pub fn build(self) -> TempTable {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path().join("_delta_log");
        std::fs::create_dir_all(&log_dir).unwrap();

        let protocol = if self.reader_features.is_empty() && self.writer_features.is_empty() {
            serde_json::json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}})
        } else {
            serde_json::json!({"protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": self.reader_features,
                "writerFeatures": self.writer_features,
            }})
        };

        let fields: Vec<serde_json::Value> = self
            .columns
            .iter()
            .map(|(name, kind)| {
                serde_json::json!({
                    "name": name, "type": kind, "nullable": true, "metadata": {}
                })
            })
            .collect();
        let schema = serde_json::json!({"type": "struct", "fields": fields});
        let metadata = serde_json::json!({"metaData": {
            "id": "00000000-0000-0000-0000-000000000001",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema.to_string(),
            "partitionColumns": self.partition_columns,
            "configuration": self.configuration,
            "createdTime": 1_750_000_000_000_u64,
        }});

        let mut commit = String::new();
        writeln!(commit, "{protocol}").unwrap();
        writeln!(commit, "{metadata}").unwrap();
        for add in &self.adds {
            writeln!(commit, "{add}").unwrap();
        }
        std::fs::write(log_dir.join("00000000000000000000.json"), commit).unwrap();

        TempTable { dir }
    }
}

pub struct TempTable {
    dir: TempDir,
}

impl TempTable {
    pub fn path(&self) -> String {
        self.dir.path().to_string_lossy().into_owned()
    }
}

/// Stats blob for a file with a single int column range.
pub fn int_range_stats(column: &str, min: i64, max: i64, num_records: u64) -> serde_json::Value {
    serde_json::json!({
        "numRecords": num_records,
        "minValues": { column: min },
        "maxValues": { column: max },
        "nullCount": { column: 0 },
    })
}
