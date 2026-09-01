use std::sync::Arc;

use delta_kernel::{Engine, Snapshot};
use object_store::DynObjectStore;
use url::Url;

use super::error::{Error, Result};
use super::metadata;

pub struct TableState {
    pub snapshot: Arc<Snapshot>,
    pub metadata: metadata::TableMetadata,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OpenOptions {
    pub version: Option<u64>,
}

pub fn open(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
    engine: &dyn Engine,
    options: OpenOptions,
) -> Result<TableState> {
    let log = metadata::log::read_log_metadata(table_url, store, options.version)?;

    if let Some(feature) = metadata::features::catalog_managed_feature(&log.reader_features) {
        return Err(Error::UnsupportedTable(format!(
            "table is catalog-managed \
                     (reader feature '{feature}'): \
                     its latest commits live in the \
                     catalog, not the filesystem log, \
                     so a filesystem-only analysis \
                     cannot be trusted. delta-explain \
                     does not support catalog-managed \
                     tables yet"
        )));
    }

    let mut snapshot_builder = Snapshot::builder_for(table_url.clone());

    if let Some(version) = options.version {
        snapshot_builder = snapshot_builder.at_version(version);
    }

    let snapshot = snapshot_builder.build(engine)?;

    let baseline = metadata::scan::scan_baseline(snapshot.clone(), engine)?;

    let partition_columns = metadata::resolve_partition_columns(&log, &baseline);

    let features = metadata::features::detect(
        &snapshot,
        &baseline.files,
        log.clustering_domain.as_deref(),
        &log.writer_features,
    );

    Ok(TableState {
        snapshot,

        metadata: metadata::TableMetadata {
            log,
            baseline,
            partition_columns,
            features,
        },
    })
}
