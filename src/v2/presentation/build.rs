use crate::v2::analysis::model::{
    Confidence, PhaseKind, PredicateClassification, UnsplittableHandling,
};
use crate::v2::analysis::predicate::Pred;
use crate::v2::diagnostics::{Explanation, Warning};
use crate::v2::gates::{AssertionResult, GateOutcome, GateStatus};
use crate::v2::report::Report;

use super::model::{
    AnalysisView, AssertionView, DiagnosticScope, ExplanationView, PhaseView, Presentation,
    PresentationOptions, StatsView, StatusView, TableFeaturesView, TableView, WarningView,
};

pub fn build(
    report: &Report,
    gates: &GateOutcome,
    elapsed_ms: u128,
    options: PresentationOptions,
) -> Presentation {
    let total_files = report.table.total_files;

    let final_files = report
        .predicate
        .as_ref()
        .and_then(|predicate| predicate.phases.last().map(|phase| phase.output_count))
        .unwrap_or(total_files);

    let analysis = report.predicate.as_ref().map(|predicate| {
        let classification = &predicate.classification;

        AnalysisView {
            partition_safe: conjunction(&classification.partition_safe),

            partition_exact: conjunction(&classification.partition_exact),

            stats_safe: conjunction(&classification.stats_safe),

            unsplittable: unsplittable_conjunction(classification),

            confidence: confidence_label(predicate.confidence),
        }
    });

    let phases = report
        .predicate
        .as_ref()
        .map(|predicate| {
            predicate
                .phases
                .iter()
                .map(|phase| {
                    build_phase(
                        phase.kind,
                        phase.confidence,
                        phase.input_count,
                        phase.output_count,
                        &predicate.classification,
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    let stats = build_stats(report);

    let features = &report.table.features;

    let table = TableView {
        path: report.table.path.clone(),

        version: report.table.version,

        total_files,

        stats,

        features: TableFeaturesView {
            deletion_vectors_enabled: features.deletion_vectors_enabled,

            files_with_deletion_vectors: features.files_with_deletion_vectors,

            column_mapping_mode: features.column_mapping_mode.clone(),

            clustering_columns: features.clustering_columns.clone(),

            in_commit_timestamps: features.in_commit_timestamps,

            unrecognized_writer_features: features.unrecognized_writer_features.clone(),
        },
    };

    let warnings = report.warnings.iter().map(build_warning).collect();

    let explanations = options
        .explain_why
        .then(|| report.explanations.iter().map(build_explanation).collect());

    let assertions = gates.assertions.iter().map(build_assertion).collect();

    Presentation {
        elapsed_ms,

        table,

        predicate: report
            .predicate
            .as_ref()
            .map(|predicate| predicate.input.clone()),

        analysis,

        phases,

        final_files,

        total_pruning_pct: pruning_pct(total_files, final_files),

        warnings,

        explanations,

        assertions,

        result: gates.overall.map(status),
    }
}

fn build_stats(report: &Report) -> StatsView {
    let present = report.table.files_with_stats;

    let total = report.table.total_files;

    let pct = if total == 0 {
        0.0
    } else {
        present as f64 / total as f64 * 100.0
    };

    let mode = if total == 0 || present == 0 {
        "absent"
    } else if present == total {
        "exact"
    } else {
        "partial"
    };

    StatsView {
        mode,
        files_with_stats: present,
        total_files: total,
        pct,
    }
}

fn build_phase(
    kind: PhaseKind,
    confidence: Confidence,
    input: usize,
    output: usize,
    classification: &PredicateClassification,
) -> PhaseView {
    let (predicate, scanned_predicate, conservative_fragments) = match kind {
        PhaseKind::PartitionPruning => (
            classification
                .partition_safe
                .iter()
                .chain(classification.partition_exact.iter())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" AND "),
            None,
            0,
        ),

        PhaseKind::DataSkipping => {
            let predicate = classification
                .stats_safe
                .iter()
                .map(ToString::to_string)
                .chain(
                    classification
                        .unsplittable
                        .iter()
                        .map(|fragment| fragment.predicate.to_string()),
                )
                .collect::<Vec<_>>()
                .join(" AND ");

            let conservative_fragments = classification.stripped_count();

            let scanned = classification
                .stats_safe
                .iter()
                .map(ToString::to_string)
                .chain(
                    classification
                        .unsplittable
                        .iter()
                        .filter(|fragment| fragment.handling == UnsplittableHandling::Scanned)
                        .map(|fragment| fragment.predicate.to_string()),
                )
                .collect::<Vec<_>>();

            let scanned_predicate = if conservative_fragments > 0 && !scanned.is_empty() {
                Some(scanned.join(" AND "))
            } else {
                None
            };

            (predicate, scanned_predicate, conservative_fragments)
        }
    };

    PhaseView {
        name: phase_name(kind),

        confidence: confidence_label(confidence),

        predicate,

        scanned_predicate,

        conservative_fragments,

        input_files: input,

        output_files: output,

        pruned_files: input.saturating_sub(output),

        pruning_pct: pruning_pct(input, output),
    }
}

fn build_warning(warning: &Warning) -> WarningView {
    WarningView {
        code: warning.code(),

        schema_code: match warning {
            Warning::PartitionEvaluationGap { .. } => "PARTITION_EVAL_GAP",

            _ => warning.code(),
        },

        message: warning_message(warning),

        scope: match warning {
            Warning::UnsupportedExpression { .. }
            | Warning::UnsplittableOr { .. }
            | Warning::PartitionEvaluationGap { .. } => DiagnosticScope::Analysis,

            Warning::DeletionVectors { .. }
            | Warning::ColumnMapping { .. }
            | Warning::LiquidClustering { .. }
            | Warning::UnrecognizedTableFeature { .. } => DiagnosticScope::Table,
        },
    }
}

fn build_explanation(explanation: &Explanation) -> ExplanationView {
    ExplanationView {
        code: explanation.code(),

        severity: match explanation {
            Explanation::UnsupportedFragment { .. } => "info",

            _ => "warning",
        },

        message: explanation_message(explanation),

        suggestion: explanation_suggestion(explanation),
    }
}

fn build_assertion(assertion: &AssertionResult) -> AssertionView {
    match assertion {
        AssertionResult::MinPruning {
            threshold,
            actual,
            status,
        } => AssertionView::MinPruning {
            threshold: *threshold,
            actual: *actual,
            status: self::status(*status),
        },

        AssertionResult::StatsComplete {
            missing_files,
            status,
        } => AssertionView::StatsComplete {
            missing_files: missing_files.clone(),

            status: self::status(*status),
        },
    }
}

fn status(status: GateStatus) -> StatusView {
    match status {
        GateStatus::Pass => StatusView::Pass,

        GateStatus::Fail => StatusView::Fail,
    }
}

fn conjunction(predicates: &[Pred]) -> Option<String> {
    if predicates.is_empty() {
        None
    } else {
        Some(
            predicates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" AND "),
        )
    }
}

fn unsplittable_conjunction(classification: &PredicateClassification) -> Option<String> {
    if classification.unsplittable.is_empty() {
        None
    } else {
        Some(
            classification
                .unsplittable
                .iter()
                .map(|fragment| fragment.predicate.to_string())
                .collect::<Vec<_>>()
                .join(" AND "),
        )
    }
}

fn phase_name(kind: PhaseKind) -> &'static str {
    match kind {
        PhaseKind::PartitionPruning => "Partition pruning",

        PhaseKind::DataSkipping => "Data skipping (min/max statistics)",
    }
}

fn confidence_label(confidence: Confidence) -> &'static str {
    match confidence {
        Confidence::Exact => "exact",

        Confidence::Conservative => "conservative",

        Confidence::Incomplete => "incomplete",
    }
}

fn pruning_pct(input: usize, output: usize) -> f64 {
    if input == 0 {
        return 0.0;
    }

    let dropped = input.saturating_sub(output);

    dropped as f64 / input as f64 * 100.0
}

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
