use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::expressions::Predicate;
use delta_kernel::scan::ScanBuilder;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::{Engine, Snapshot};

use crate::error::Result;

/// Execute a metadata-only kernel scan and return the paths of the files
/// surviving the supplied predicate.
///
/// No Parquet data files are opened here; pruning is performed through the
/// kernel metadata path.
pub(in crate::analysis) fn surviving_files(
    snapshot: Arc<Snapshot>,
    engine: &dyn Engine,
    predicate: &Predicate,
) -> Result<HashSet<String>> {
    let scan = ScanBuilder::new(snapshot)
        .with_predicate(Arc::new(predicate.clone()))
        .build()?;

    let mut survivors = HashSet::new();

    for result in scan.scan_metadata(engine)? {
        let scan_metadata = result?;

        survivors = scan_metadata.visit_scan_files(survivors, push_path)?;
    }

    Ok(survivors)
}

fn push_path(paths: &mut HashSet<String>, file: ScanFile) {
    paths.insert(file.path.clone());
}
