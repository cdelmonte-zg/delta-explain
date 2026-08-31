use std::sync::Arc;

use delta_kernel::Snapshot;
use delta_kernel::table_features::ColumnMappingMode;

use crate::v2::metadata::scan::FileInfo;

pub struct TableFeatures {
    pub deletion_vectors_enabled: bool,
    pub files_with_deletion_vectors: usize,
    pub column_mapping_mode: Option<String>,
    pub clustering_columns: Option<Vec<String>>,
    pub in_commit_timestamps: bool,
    pub unrecognized_writer_features: Vec<String>,
}

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

pub fn catalog_managed_feature(reader_features: &[String]) -> Option<&str> {
    reader_features
        .iter()
        .find(|f| *f == "catalogManaged" || *f == "catalogOwned-preview")
        .map(String::as_str)
}

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
