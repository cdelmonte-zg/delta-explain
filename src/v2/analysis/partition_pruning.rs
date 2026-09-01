use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::schema::SchemaRef;
use delta_kernel::{Engine, Snapshot};

use crate::v2::error::Result;
use crate::v2::instrumentation::Instrumentation;
use crate::v2::metadata::scan::FileInfo;

use super::kernel;
use super::model::{PartitionAnalysis, PredicateClassification};
use super::partition_eval::{self, Truth};

pub(super) fn prune(
    classification: &PredicateClassification,
    files: &[FileInfo],
    snapshot: Arc<Snapshot>,
    engine: &dyn Engine,
    schema: &SchemaRef,
    instrumentation: &mut dyn Instrumentation,
) -> Result<PartitionAnalysis> {
    let kernel_survivors =
        scan_partition_safe(classification, snapshot, engine, schema, instrumentation)?;

    let (exact_survivors, evaluation_gaps) =
        evaluate_partition_exact(classification, files, schema, instrumentation)?;

    let survivors = combine_survivors(kernel_survivors, exact_survivors);

    Ok(PartitionAnalysis {
        survivors,
        evaluation_gaps,
    })
}

fn scan_partition_safe(
    classification: &PredicateClassification,
    snapshot: Arc<Snapshot>,
    engine: &dyn Engine,
    schema: &SchemaRef,
    instrumentation: &mut dyn Instrumentation,
) -> Result<Option<HashSet<String>>> {
    let Some(predicate) = classification.partition_safe_predicate() else {
        return Ok(None);
    };

    let kernel_predicate = kernel::lower(&predicate, schema)?;

    instrumentation.partition_kernel_predicate_lowered(&predicate, &kernel_predicate)?;

    let survivors = kernel::surviving_files(snapshot, engine, &kernel_predicate)?;

    Ok(Some(survivors))
}

fn evaluate_partition_exact(
    classification: &PredicateClassification,
    files: &[FileInfo],
    schema: &SchemaRef,
    instrumentation: &mut dyn Instrumentation,
) -> Result<(Option<HashSet<String>>, usize)> {
    let Some(predicate) = classification.partition_exact_predicate() else {
        return Ok((None, 0));
    };

    let mut survivors = HashSet::new();

    let mut evaluation_gaps = 0usize;

    for file in files {
        match partition_eval::eval(&predicate, &file.partition_values, schema) {
            Truth::True => {
                survivors.insert(file.path.clone());
            }

            Truth::Unknown => {
                // Unknown is evaluator ignorance,
                // not SQL NULL.
                //
                // Keep the file conservatively.
                survivors.insert(file.path.clone());

                evaluation_gaps += 1;
            }

            Truth::False | Truth::Null => {}
        }
    }

    instrumentation.partition_evaluated(
        &predicate,
        survivors.len(),
        files.len(),
        evaluation_gaps,
    )?;

    Ok((Some(survivors), evaluation_gaps))
}

fn combine_survivors(
    kernel: Option<HashSet<String>>,
    exact: Option<HashSet<String>>,
) -> Option<HashSet<String>> {
    match (kernel, exact) {
        (Some(kernel), Some(exact)) => Some(kernel.intersection(&exact).cloned().collect()),

        (Some(kernel), None) => Some(kernel),

        (None, exact) => exact,
    }
}
