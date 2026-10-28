use self::{features::TableFeatures, scan::BaselineScan, snapshot::SnapshotMetadata};

pub mod features;
pub mod scan;
pub mod snapshot;
pub mod stats;

pub struct TableMetadata {
    pub log: SnapshotMetadata,
    pub baseline: BaselineScan,
    pub partition_columns: Vec<String>,
    pub features: TableFeatures,
}
