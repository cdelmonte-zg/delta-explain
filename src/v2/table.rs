use super::error::{Error, Result};
use super::metadata;
use delta_kernel::{Engine, Snapshot};
use object_store::DynObjectStore;
use std::sync::Arc;
use url::Url;

pub struct TableState {
    pub snapshot: Arc<Snapshot>,
    pub metadata: metadata::TableMetadata,
}

pub fn open(
    table_url: &Url,
    store: &Arc<DynObjectStore>,
    engine: &dyn Engine,
) -> Result<TableState> {
    let log = metadata::log::read_log_metadata(table_url, store)?;

    if let Some(feature) = metadata::features::catalog_managed_feature(&log.reader_features) {
        return Err(Error::UnsupportedTable(format!(
            "table is catalog-managed (reader feature '{feature}'): its latest \
            commits live in the catalog, not the filesystem log, so a \
            filesystem-only analysis cannot be trusted. delta-explain does \
            not support catalog-managed tables yet"
        )));
    }

    let snapshot = Snapshot::builder_for(table_url).build(engine)?;
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
