use crate::v2::analysis::model::UnsplittableHandling;
use crate::v2::diagnostics::{Explanation, Warning};

pub(super) fn warning_message(warning: &Warning) -> String {
    match warning {
        Warning::UnsupportedExpression { predicate, reasons } => {
            let reason = if reasons.is_empty() {
                "Unsupported expression".to_string()
            } else {
                reasons.join("; ")
            };

            format!(
                "{reason}; the fragment '{predicate}' \
                 cannot contribute to pruning and is \
                 applied conservatively (keeps all files)"
            )
        }

        Warning::UnsplittableOr { .. } => "Mixed expression across partition and \
             non-partition columns; cannot separate \
             safely, routed as unsplittable"
            .to_string(),

        Warning::PartitionEvaluationGap { count } => {
            if *count == 1 {
                "A partition value could not be \
                 evaluated exactly; the file was \
                 kept conservatively"
                    .to_string()
            } else {
                format!(
                    "{count} partition values could \
                     not be evaluated exactly; those \
                     files were kept conservatively"
                )
            }
        }

        Warning::DeletionVectors {
            files_with_deletion_vectors,
            total_files,
        } => {
            format!(
                "{files_with_deletion_vectors} of {total_files} files carry \
                 deletion vectors: record counts include soft-deleted rows, \
                 so they overcount the live data"
            )
        }

        Warning::ColumnMapping { mode } => {
            format!(
                "column mapping mode '{mode}': the log stores physical column \
                 names, so verbose statistics may display physical instead of \
                 logical names; kernel pruning itself resolves the mapping"
            )
        }

        Warning::LiquidClustering { columns } => {
            let on = if columns.is_empty() {
                "unknown columns".to_string()
            } else {
                columns.join(", ")
            };

            format!(
                "table is liquid-clustered on {on}: file layout is managed by \
                 clustering, not directory partitions; data skipping on min/max \
                 statistics still applies"
            )
        }

        Warning::UnrecognizedTableFeature { features } => {
            format!(
                "writer feature(s) this tool does not know: {}; the numbers \
                 reported here do not account for whatever they imply",
                features.join(", ")
            )
        }
    }
}

pub(super) fn explanation_message(explanation: &Explanation) -> String {
    match explanation {
        Explanation::NoPartitionFilter { partition_columns } => {
            format!(
                "The table is partitioned by {}, but the predicate \
                 filters on none of those columns, so partition \
                 pruning cannot run.",
                partition_columns.join(", ")
            )
        }

        Explanation::StatsAbsent { predicate } => {
            format!(
                "The table carries no file statistics, so data \
                 skipping cannot prune on '{predicate}'."
            )
        }

        Explanation::WeakDataSkipping { predicate } => {
            format!(
                "Data skipping eliminated no files for \
                 '{predicate}': the per-file min/max ranges all \
                 overlap the predicate's bound."
            )
        }

        Explanation::UnsupportedFragment {
            predicate,
            handling,
        } => match handling {
            UnsplittableHandling::Scanned => {
                format!(
                    "The fragment '{predicate}' cannot be split safely into \
                     independent pruning fragments, so it was evaluated as a \
                     whole by the pruning backend."
                )
            }

            UnsplittableHandling::Stripped => {
                format!(
                    "The fragment '{predicate}' is outside the pruning language \
                     and was applied conservatively, keeping all files."
                )
            }
        },
    }
}

pub(super) fn explanation_suggestion(explanation: &Explanation) -> Option<String> {
    match explanation {
        Explanation::NoPartitionFilter { partition_columns } => Some(format!(
            "Filter on a partition column ({}) to eliminate \
             whole directories before data skipping.",
            partition_columns.join(", ")
        )),

        Explanation::StatsAbsent { .. } => Some(
            "Have the writer record statistics \
             (delta.dataSkippingNumIndexedCols covers the \
             columns you filter on)."
                .to_string(),
        ),

        Explanation::WeakDataSkipping { .. } => Some(
            "Ranges this wide usually mean the data is not \
             sorted or clustered by that column; ordering by \
             it so each file covers a narrower range may \
             enable skipping."
                .to_string(),
        ),

        Explanation::UnsupportedFragment { handling, .. } => Some(match handling {
            UnsplittableHandling::Scanned => {
                "Rewrite mixed partition/data OR expressions as independent \
                 conjuncts when equivalent; this may allow partition pruning \
                 and data skipping to operate separately."
                    .to_string()
            }

            UnsplittableHandling::Stripped => {
                "Function calls, arithmetic and subqueries cannot currently \
                 contribute to pruning; rewrite the predicate using supported \
                 column/literal comparisons when possible."
                    .to_string()
            }
        }),
    }
}
