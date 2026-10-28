use self::{features::TableFeatures, snapshot::SnapshotMetadata, scan::BaselineScan};

pub mod features;
pub mod snapshot;
pub mod scan;
pub mod stats;

pub struct TableMetadata {
    pub log: snapshot::SnapshotMetadata,
    pub baseline: scan::BaselineScan,
    pub partition_columns: Vec<String>,
    pub features: TableFeatures,
}

pub fn resolve_partition_columns(log: &SnapshotMetadata, baseline: &BaselineScan) -> Vec<String> {
    if log.partition_columns.is_empty() {
        scan::partition_columns_from_files(&baseline.files)
    } else {
        log.partition_columns.clone()
    }
}
