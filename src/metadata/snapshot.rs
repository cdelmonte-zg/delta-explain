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

    let has_clustering = protocol.writer_features().is_some_and(|feature| {
        feature.contains(&delta_kernel::table_features::TableFeature::ClusteredTable)
    });

    let clustering_domain = if has_clustering {
        snapshot.get_domain_metadata_internal("delta.clustering", engine)?
    } else {
        None
    };

    Ok(SnapshotMetadata {
        partition_columns: config.metadata().partition_columns().to_vec(),

        clustering_domain,

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
