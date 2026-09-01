use self::{features::TableFeatures, log::LogMetadata, scan::BaselineScan};

pub mod features;
pub mod log;
pub mod scan;
pub mod stats;

pub struct TableMetadata {
    pub log: log::LogMetadata,
    pub baseline: scan::BaselineScan,
    pub partition_columns: Vec<String>,
    pub features: TableFeatures,
}

pub fn resolve_partition_columns(log: &LogMetadata, baseline: &BaselineScan) -> Vec<String> {
    if log.partition_columns.is_empty() {
        scan::partition_columns_from_files(&baseline.files)
    } else {
        log.partition_columns.clone()
    }
}
