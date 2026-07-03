//! Table protocol feature detection: detect and declare, no semantics.
//!
//! A tool that says "I cannot attribute this correctly" is credible; one
//! that prints wrong numbers is not. This module detects the protocol
//! features that distort or reframe the report's numbers (deletion
//! vectors, column mapping, liquid clustering) and turns them into
//! declarations and warnings. It never changes how pruning is computed.

use std::sync::Arc;

use delta_kernel::Snapshot;
use delta_kernel::table_features::ColumnMappingMode;

use crate::predicate_analyzer::AnalysisNote;
use crate::report::FileInfo;

#[derive(Debug, Clone, Default)]
pub struct TableFeatures {
    pub deletion_vectors_enabled: bool,
    pub files_with_deletion_vectors: usize,
    /// "name" or "id"; None when mapping is absent or explicitly none.
    pub column_mapping_mode: Option<String>,
    /// Some(columns) when the table is liquid-clustered; the vec is empty
    /// if the clustering domain exists but its payload is unreadable.
    pub clustering_columns: Option<Vec<String>>,
    /// Commit timestamps live inside the commits, not in file metadata.
    /// Declared for completeness; nothing this tool reports depends on it.
    pub in_commit_timestamps: bool,
    /// Writer features outside the set this tool knows to be irrelevant to
    /// its numbers. Reader features never appear here: an unknown reader
    /// feature makes the kernel refuse the table long before detection.
    pub unrecognized_writer_features: Vec<String>,
}

/// Writer features known not to affect what this tool reports. Everything
/// else earns an UNRECOGNIZED_TABLE_FEATURE warning: honesty about unknown
/// territory beats silence.
const BENIGN_WRITER_FEATURES: &[&str] = &[
    "appendOnly",
    "invariants",
    "checkConstraints",
    "changeDataFeed",
    "generatedColumns",
    "identityColumns",
    "columnMapping",
    "deletionVectors",
    "timestampNtz",
    "domainMetadata",
    "v2Checkpoint",
    "rowTracking",
    "icebergCompatV1",
    "icebergCompatV2",
    "clustering",
    "inCommitTimestamp",
    "vacuumProtocolCheck",
    "typeWidening",
    "typeWidening-preview",
    "variantType",
    "variantType-preview",
];

/// `clustering_domain` is the raw configuration of the table's
/// `delta.clustering` domain metadata, pre-read from the log by
/// `stats::read_log_metadata` (delta-kernel 0.24 exposes no public
/// accessor for system domains).
pub fn detect(
    snapshot: &Arc<Snapshot>,
    files: &[FileInfo],
    clustering_domain: Option<&str>,
    writer_features: &[String],
) -> TableFeatures {
    let props = snapshot.table_properties();

    let column_mapping_mode = match props.column_mapping_mode {
        Some(ColumnMappingMode::Name) => Some("name".to_string()),
        Some(ColumnMappingMode::Id) => Some("id".to_string()),
        Some(ColumnMappingMode::None) | None => None,
    };

    TableFeatures {
        deletion_vectors_enabled: props.enable_deletion_vectors.unwrap_or(false),
        files_with_deletion_vectors: files.iter().filter(|f| f.has_deletion_vector).count(),
        column_mapping_mode,
        clustering_columns: clustering_domain.map(parse_clustering_columns),
        in_commit_timestamps: props.enable_in_commit_timestamps.unwrap_or(false)
            || writer_features.iter().any(|f| f == "inCommitTimestamp"),
        unrecognized_writer_features: writer_features
            .iter()
            .filter(|f| !BENIGN_WRITER_FEATURES.contains(&f.as_str()))
            .cloned()
            .collect(),
    }
}

/// The catalog-managed reader features: their commits live in the catalog,
/// not (only) in the filesystem log, so a filesystem-only read cannot be
/// trusted. delta-kernel refuses such tables; this check exists to say so
/// in this tool's words before the kernel says it in API jargon.
pub fn catalog_managed_feature(reader_features: &[String]) -> Option<&str> {
    reader_features
        .iter()
        .find(|f| *f == "catalogManaged" || *f == "catalogOwned-preview")
        .map(String::as_str)
}

/// The clustering domain payload is
/// `{"clusteringColumns": [["col"], ["nested", "path"]]}`. A domain that
/// exists but does not parse still means "clustered": the columns are
/// reported as unknown (empty) rather than the feature as absent.
fn parse_clustering_columns(raw: &str) -> Vec<String> {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|v| {
            let cols = v.get("clusteringColumns")?.as_array()?.clone();
            Some(
                cols.iter()
                    .filter_map(|path| {
                        let parts = path.as_array()?;
                        Some(
                            parts
                                .iter()
                                .filter_map(|p| p.as_str())
                                .collect::<Vec<_>>()
                                .join("."),
                        )
                    })
                    .collect::<Vec<String>>(),
            )
        })
        .unwrap_or_default()
}

impl TableFeatures {
    /// The warnings this table's features deserve. Only what distorts or
    /// reframes the numbers warns; a feature that is merely enabled but
    /// not in effect (deletion vectors with no vectors written) does not.
    pub fn notes(&self, total_files: usize) -> Vec<AnalysisNote> {
        let mut notes = Vec::new();

        if self.files_with_deletion_vectors > 0 {
            notes.push(AnalysisNote {
                code: "DELETION_VECTORS".into(),
                message: format!(
                    "{} of {} files carry deletion vectors: record counts include \
                     soft-deleted rows, so they overcount the live data",
                    self.files_with_deletion_vectors, total_files
                ),
            });
        }

        if let Some(mode) = &self.column_mapping_mode {
            notes.push(AnalysisNote {
                code: "COLUMN_MAPPING".into(),
                message: format!(
                    "column mapping mode '{mode}': the log stores physical column \
                     names, so verbose statistics may display physical instead of \
                     logical names; kernel pruning itself resolves the mapping"
                ),
            });
        }

        if let Some(cols) = &self.clustering_columns {
            let on = if cols.is_empty() {
                "unknown columns".to_string()
            } else {
                cols.join(", ")
            };
            notes.push(AnalysisNote {
                code: "LIQUID_CLUSTERING".into(),
                message: format!(
                    "table is liquid-clustered on {on}: file layout is managed by \
                     clustering, not directory partitions; data skipping on min/max \
                     statistics still applies"
                ),
            });
        }

        if !self.unrecognized_writer_features.is_empty() {
            notes.push(AnalysisNote {
                code: "UNRECOGNIZED_TABLE_FEATURE".into(),
                message: format!(
                    "writer feature(s) this tool does not know: {}; the numbers \
                     reported here do not account for whatever they imply",
                    self.unrecognized_writer_features.join(", ")
                ),
            });
        }

        notes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_detected_produces_no_notes() {
        let f = TableFeatures::default();
        assert!(f.notes(6).is_empty());
    }

    #[test]
    fn enabled_but_absent_deletion_vectors_stay_silent() {
        let f = TableFeatures {
            deletion_vectors_enabled: true,
            files_with_deletion_vectors: 0,
            ..Default::default()
        };
        assert!(f.notes(6).is_empty());
    }

    #[test]
    fn present_deletion_vectors_warn_with_counts() {
        let f = TableFeatures {
            files_with_deletion_vectors: 2,
            ..Default::default()
        };
        let notes = f.notes(6);
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].code, "DELETION_VECTORS");
        assert!(notes[0].message.contains("2 of 6"));
    }

    #[test]
    fn every_feature_warns_once() {
        let f = TableFeatures {
            deletion_vectors_enabled: true,
            files_with_deletion_vectors: 1,
            column_mapping_mode: Some("name".into()),
            clustering_columns: Some(vec!["age".into()]),
            in_commit_timestamps: true, // declared, never warned
            unrecognized_writer_features: vec!["someFutureFeature".into()],
        };
        let codes: Vec<String> = f.notes(3).into_iter().map(|n| n.code).collect();
        assert_eq!(
            codes,
            vec![
                "DELETION_VECTORS",
                "COLUMN_MAPPING",
                "LIQUID_CLUSTERING",
                "UNRECOGNIZED_TABLE_FEATURE"
            ]
        );
    }

    #[test]
    fn in_commit_timestamps_declare_without_warning() {
        let f = TableFeatures {
            in_commit_timestamps: true,
            ..Default::default()
        };
        assert!(f.notes(3).is_empty());
    }

    #[test]
    fn catalog_managed_features_are_recognized() {
        let owned = vec!["catalogOwned-preview".to_string()];
        assert_eq!(
            catalog_managed_feature(&owned),
            Some("catalogOwned-preview")
        );
        let managed = vec!["deletionVectors".to_string(), "catalogManaged".to_string()];
        assert_eq!(catalog_managed_feature(&managed), Some("catalogManaged"));
        assert_eq!(catalog_managed_feature(&[]), None);
    }

    #[test]
    fn unreadable_clustering_domain_reports_unknown_columns() {
        let f = TableFeatures {
            clustering_columns: Some(Vec::new()),
            ..Default::default()
        };
        let notes = f.notes(1);
        assert!(notes[0].message.contains("unknown columns"));
    }
}
