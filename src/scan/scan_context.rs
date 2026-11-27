use std::sync::Arc;

use delta_kernel::{Engine, Snapshot};

use crate::scan::scan_cache::CachedScanMetadata;

pub(crate) struct ScanContext<'a> {
    pub(crate) snapshot: Arc<Snapshot>,
    pub(crate) engine: &'a dyn Engine,
    pub(crate) scan_cache: &'a CachedScanMetadata,
}
