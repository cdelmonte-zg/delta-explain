use delta_kernel::{Engine, Snapshot};

use crate::error::Error;

#[derive(Debug)]
pub struct SnapshotMetadata {
    pub partition_columns: Vec<String>,
    pub clustering_domain: Option<String>,
    pub reader_features: Vec<String>,
    pub writer_features: Vec<String>,
}

pub fn read_snapshot_metadata(
    snapshot: &Snapshot,
    engine: &dyn Engine,
) -> Result<SnapshotMetadata, Error> {
    let config = snapshot.table_configuration();
    let protocol = config.protocol();

    Ok(SnapshotMetadata {
        partition_columns: config.metadata().partition_columns().to_vec(),

        clustering_domain: snapshot.get_domain_metadata_internal("delta.clustering", engine)?,

        reader_features: protocol
            .reader_features()
            .unwrap_or_default()
            .iter()
            .map(ToString::to_string)
            .collect(),

        writer_features: protocol
            .writer_features()
            .unwrap_or_default()
            .iter()
            .map(ToString::to_string)
            .collect(),
    })
}
