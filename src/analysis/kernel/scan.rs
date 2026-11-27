use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::expressions::Predicate;
use delta_kernel::scan::ScanBuilder;
use delta_kernel::scan::state::ScanFile;

use crate::error::{Error, Result};
use crate::scan::scan_context::ScanContext;

/// Execute a metadata-only kernel scan and return the paths of the files
/// surviving the supplied predicate.
///
/// No Parquet data files are opened here; pruning is performed through the
/// kernel metadata path.
pub(in crate::analysis) fn surviving_files(
    context: &ScanContext<'_>,
    predicate: &Predicate,
) -> Result<HashSet<String>> {
    if context.snapshot.version() != context.scan_cache.version() {
        return Err(Error::ScanCacheVersionMismatch {
            cached: context.scan_cache.version(),
            snapshot: context.snapshot.version(),
        });
    }

    let scan = ScanBuilder::new(Arc::clone(&context.snapshot))
        .with_predicate(Arc::new(predicate.clone()))
        .build()?;

    let mut survivors = HashSet::new();

    for result in scan.scan_metadata_from(
        context.engine,
        context.scan_cache.version(),
        context.scan_cache.engine_data(),
        None,
    )? {
        let scan_metadata = result?;
        survivors = scan_metadata.visit_scan_files(survivors, push_path)?;
    }

    Ok(survivors)
}

fn push_path(paths: &mut HashSet<String>, file: ScanFile) {
    paths.insert(file.path);
}
