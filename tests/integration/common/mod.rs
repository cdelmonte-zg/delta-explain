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
    column_metadata: Vec<serde_json::Value>,
    partition_columns: Vec<String>,
    configuration: BTreeMap<String, String>,
    reader_features: Vec<String>,
    writer_features: Vec<String>,
    domain_metadata: Vec<serde_json::Value>,
    adds: Vec<serde_json::Value>,
    closed_commits: Vec<Vec<serde_json::Value>>,
    compactions: Vec<(u64, u64)>,
}

impl LogBuilder {
    pub fn new() -> Self {
        LogBuilder {
            columns: Vec::new(),
            column_metadata: Vec::new(),
            partition_columns: Vec::new(),
            configuration: BTreeMap::new(),
            reader_features: Vec::new(),
            writer_features: Vec::new(),
            domain_metadata: Vec::new(),
            adds: Vec::new(),
            closed_commits: Vec::new(),
            compactions: Vec::new(),
        }
    }

    /// Close the current commit: actions added so far (since the last
    /// boundary) become one log version, and subsequent adds start the
    /// next one. Without any boundary the whole log is a single commit 0.
    #[allow(dead_code)]
    pub fn commit(mut self) -> Self {
        let actions = std::mem::take(&mut self.adds);
        self.closed_commits.push(actions);
        self
    }

    /// Write a log-compaction file covering commits `start..=end`
    /// (`<start>.<end>.compacted.json`), containing the reconciled actions
    /// of that range. The original commit files stay in place: readers are
    /// expected to prefer the compacted file and must not double-count.
    #[allow(dead_code)]
    pub fn compaction(mut self, start: u64, end: u64) -> Self {
        self.compactions.push((start, end));
        self
    }

    /// A primitive column: `kind` is the Delta type name ("string", "long",
    /// "integer", "double", "date", ...).
    pub fn column(mut self, name: &str, kind: &str) -> Self {
        self.columns.push((name.into(), serde_json::json!(kind)));
        self.column_metadata.push(serde_json::json!({}));
        self
    }

    /// A column-mapped primitive column: carries the per-field
    /// `delta.columnMapping.id` / `physicalName` metadata the mapping modes
    /// require.
    #[allow(dead_code)]
    pub fn mapped_column(mut self, name: &str, kind: &str, id: i64, physical: &str) -> Self {
        self.columns.push((name.into(), serde_json::json!(kind)));
        self.column_metadata.push(serde_json::json!({
            "delta.columnMapping.id": id,
            "delta.columnMapping.physicalName": physical,
        }));
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

    /// A domainMetadata action, e.g. domain "delta.clustering" with a
    /// clusteringColumns JSON payload.
    #[allow(dead_code)]
    pub fn domain_metadata(mut self, domain: &str, configuration: &str) -> Self {
        self.domain_metadata.push(serde_json::json!({
            "domainMetadata": {
                "domain": domain,
                "configuration": configuration,
                "removed": false,
            }
        }));
        self
    }

    /// One add action. `partition_values` must name every partition column;
    /// `stats` is the stats JSON blob, or None for a stats-less file.
    pub fn add_file(
        self,
        path: &str,
        partition_values: &[(&str, &str)],
        stats: Option<serde_json::Value>,
    ) -> Self {
        let nullable: Vec<(&str, Option<&str>)> = partition_values
            .iter()
            .map(|(k, v)| (*k, Some(*v)))
            .collect();
        self.add_file_nullable(path, &nullable, stats)
    }

    /// Like `add_file`, but a partition value may be `None`, which the log
    /// stores as JSON null - the shape the kernel later reports as an
    /// absent key in the file's partition values.
    #[allow(dead_code)]
    pub fn add_file_nullable(
        mut self,
        path: &str,
        partition_values: &[(&str, Option<&str>)],
        stats: Option<serde_json::Value>,
    ) -> Self {
        let pv: BTreeMap<&str, serde_json::Value> = partition_values
            .iter()
            .map(|(k, v)| {
                (
                    *k,
                    v.map_or(serde_json::Value::Null, |s| serde_json::json!(s)),
                )
            })
            .collect();
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

    /// Like `add_file`, but the add action carries an inline deletion vector
    /// descriptor. A metadata scan never dereferences the vector, so the
    /// payload only needs to be structurally valid.
    #[allow(dead_code)]
    pub fn add_file_with_dv(
        mut self,
        path: &str,
        partition_values: &[(&str, &str)],
        stats: Option<serde_json::Value>,
    ) -> Self {
        self = self.add_file(path, partition_values, stats);
        if let Some(serde_json::Value::Object(add)) =
            self.adds.last_mut().and_then(|a| a.get_mut("add"))
        {
            add.insert(
                "deletionVector".into(),
                serde_json::json!({
                    "storageType": "u",
                    "pathOrInlineDv": "vBn[lx{q8@P<9wq",
                    "offset": 1,
                    "sizeInBytes": 36,
                    "cardinality": 2,
                }),
            );
        }
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
    pub fn build(mut self) -> TempTable {
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
            .zip(&self.column_metadata)
            .map(|((name, kind), metadata)| {
                serde_json::json!({
                    "name": name, "type": kind, "nullable": true, "metadata": metadata
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

        // Whatever accumulated after the last boundary is the final commit.
        let trailing = std::mem::take(&mut self.adds);
        let mut commits = self.closed_commits;
        if !trailing.is_empty() || commits.is_empty() {
            commits.push(trailing);
        }

        for (version, actions) in commits.iter().enumerate() {
            let mut content = String::new();
            if version == 0 {
                writeln!(content, "{protocol}").unwrap();
                writeln!(content, "{metadata}").unwrap();
                for dm in &self.domain_metadata {
                    writeln!(content, "{dm}").unwrap();
                }
            }
            for action in actions {
                writeln!(content, "{action}").unwrap();
            }
            std::fs::write(log_dir.join(format!("{version:020}.json")), content).unwrap();
        }

        // Compaction files carry the reconciled actions of their range; with
        // append-only synthetic commits that is the concatenation, plus
        // protocol/metaData when commit 0 is in range.
        for (start, end) in &self.compactions {
            let mut content = String::new();
            if *start == 0 {
                writeln!(content, "{protocol}").unwrap();
                writeln!(content, "{metadata}").unwrap();
                for dm in &self.domain_metadata {
                    writeln!(content, "{dm}").unwrap();
                }
            }
            for actions in commits.iter().take(*end as usize + 1).skip(*start as usize) {
                for action in actions {
                    writeln!(content, "{action}").unwrap();
                }
            }
            std::fs::write(
                log_dir.join(format!("{start:020}.{end:020}.compacted.json")),
                content,
            )
            .unwrap();
        }

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
