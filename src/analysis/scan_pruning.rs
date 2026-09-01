use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::schema::SchemaRef;
use delta_kernel::{Engine, Snapshot};

use crate::error::Result;
use crate::instrumentation::Instrumentation;
use crate::metadata::scan::FileInfo;

use super::kernel;
use super::model::{PartitionAnalysis, PredicateClassification, ScanAnalysis};

/// Execute the general pruning phase.
///
/// This phase handles:
///
/// - stats-safe fragments,
/// - mixed-axis fragments that the kernel can evaluate,
/// - predicates from which unsupported fragments were stripped.
///
/// Partition-exact fragments never reach the kernel. Their survivor set
/// was already computed by the partition phase and is intersected here
/// with the kernel result.
pub(super) fn prune(
    classification: &PredicateClassification,
    partition: &PartitionAnalysis,
    files: &[FileInfo],
    snapshot: Arc<Snapshot>,
    engine: &dyn Engine,
    schema: &SchemaRef,
    instrumentation: &mut dyn Instrumentation,
) -> Result<ScanAnalysis> {
    if !classification.requires_scan_phase() {
        return Ok(ScanAnalysis { survivors: None });
    }

    let mut survivors = match classification.scan_predicate() {
        Some(predicate) => {
            let kernel_predicate = kernel::lower(&predicate, schema)?;

            instrumentation.scan_kernel_predicate_lowered(&predicate, &kernel_predicate)?;

            kernel::surviving_files(snapshot, engine, &kernel_predicate)?
        }

        None => {
            instrumentation.scan_without_predicate()?;

            baseline_paths(files)
        }
    };

    if let Some(partition_survivors) = &partition.survivors {
        survivors.retain(|path| partition_survivors.contains(path));
    }

    Ok(ScanAnalysis {
        survivors: Some(survivors),
    })
}

fn baseline_paths(files: &[FileInfo]) -> HashSet<String> {
    files.iter().map(|file| file.path.clone()).collect()
}
