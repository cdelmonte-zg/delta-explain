use serde_json::{Value, json};

use super::diagnostics::{explanation_message, explanation_suggestion, warning_message};

use crate::v2::analysis::model::{Confidence, PhaseKind, PredicateClassification};
use crate::v2::analysis::predicate::Pred;
use crate::v2::diagnostics::{Explanation, Warning};
use crate::v2::error::Result;
use crate::v2::gates::{AssertionResult, GateOutcome};
use crate::v2::report::Report;

pub const SCHEMA_VERSION: &str = "0.4.0";

pub(super) fn render(
    report: &Report,
    gates: &GateOutcome,
    elapsed_ms: u128,
    explain_why: bool,
) -> Result<String> {
    let total_files = report.table.total_files;

    let final_files = report
        .predicate
        .as_ref()
        .and_then(|predicate| predicate.phases.last().map(|phase| phase.output_count))
        .unwrap_or(total_files);

    let stats_present = report.table.files_with_stats;

    let stats_pct = if total_files == 0 {
        0.0
    } else {
        stats_present as f64 / total_files as f64 * 100.0
    };

    let analysis = report.predicate.as_ref().map(|predicate| {
        let classification = &predicate.classification;

        json!({
            "partition_safe":
                predicate_conjunction(
                    &classification
                        .partition_safe
                ),

            "partition_exact":
                predicate_conjunction(
                    &classification
                        .partition_exact
                ),

            "stats_safe":
                predicate_conjunction(
                    &classification
                        .stats_safe
                ),

            "unsplittable":
                unsplittable_conjunction(
                    classification
                ),

            "confidence":
                confidence_label(
                    predicate.confidence
                ),

            "notes":
                analysis_notes(
                    report
                ),
        })
    });

    let phases = report
        .predicate
        .as_ref()
        .map(|predicate| {
            predicate
                .phases
                .iter()
                .map(|phase| {
                    json!({
                        "name":
                            phase_name(
                                phase.kind
                            ),

                        "confidence":
                            confidence_label(
                                phase.confidence
                            ),

                        "predicate":
                            phase_predicate(
                                phase.kind,
                                &predicate
                                    .classification,
                            ),

                        "input_files":
                            phase.input_count,

                        "output_files":
                            phase.output_count,

                        "pruned_files":
                            phase
                                .input_count
                                .saturating_sub(
                                    phase
                                        .output_count
                                ),

                        "pruning_pct":
                            pruning_pct(
                                phase
                                    .input_count,
                                phase
                                    .output_count,
                            ),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let features = &report.table.features;

    let mut output = json!({
        "schema_version":
            SCHEMA_VERSION,

        "tool_version":
            env!("CARGO_PKG_VERSION"),

        "elapsed_ms":
            elapsed_ms,

        "table":
            &report.table.path,

        "version":
            report.table.version,

        "predicate":
            report
                .predicate
                .as_ref()
                .map(|predicate| {
                    &predicate.input
                }),

        "total_files":
            total_files,

        "final_files":
            final_files,

        "total_pruning_pct":
            pruning_pct(
                total_files,
                final_files,
            ),

        "analysis":
            analysis,

        "table_features": {
            "deletion_vectors": {
                "enabled":
                    features
                        .deletion_vectors_enabled,

                "files_with_deletion_vectors":
                    features
                        .files_with_deletion_vectors,
            },

            "column_mapping_mode":
                &features
                    .column_mapping_mode,

            "clustering_columns":
                &features
                    .clustering_columns,

            "in_commit_timestamps":
                features
                    .in_commit_timestamps,

            "unrecognized_writer_features":
                &features
                    .unrecognized_writer_features,

            "notes":
                table_notes(
                    report
                ),
        },

        "stats": {
            "mode":
                stats_mode(
                    stats_present,
                    total_files,
                ),

            "files_with_stats":
                stats_present,

            "total_files":
                total_files,

            "pct":
                stats_pct,
        },

        "phases":
            phases,

        "assertions":
            assertions(
                gates
            ),

        "result":
            gates
                .overall
                .map(|status| {
                    status.as_str()
                }),
    });

    if explain_why {
        output["explain"] = json!(
            report
                .explanations
                .iter()
                .map(explanation)
                .collect::<Vec<_>>()
        );
    }

    Ok(serde_json::to_string_pretty(&output)?)
}

fn assertions(gates: &GateOutcome) -> Vec<Value> {
    gates
        .assertions
        .iter()
        .map(|assertion| match assertion {
            AssertionResult::MinPruning {
                threshold,
                actual,
                status,
            } => {
                json!({
                    "name":
                        "min_pruning",

                    "threshold":
                        threshold,

                    "actual":
                        actual,

                    "result":
                        status.as_str(),
                })
            }

            AssertionResult::StatsComplete {
                missing_files,
                status,
            } => {
                json!({
                    "name":
                        "stats_complete",

                    "missing_count":
                        missing_files.len(),

                    "result":
                        status.as_str(),
                })
            }
        })
        .collect()
}

fn analysis_notes(report: &Report) -> Vec<Value> {
    report
        .warnings
        .iter()
        .filter(|warning| {
            matches!(
                warning,
                Warning::UnsupportedExpression { .. }
                    | Warning::UnsplittableOr { .. }
                    | Warning::PartitionEvaluationGap { .. }
            )
        })
        .map(|warning| {
            json!({
                "code":
                    analysis_warning_code(
                        warning
                    ),

                "message":
                    warning_message(
                        warning
                    ),
            })
        })
        .collect()
}

fn table_notes(report: &Report) -> Vec<Value> {
    report
        .warnings
        .iter()
        .filter(|warning| {
            matches!(
                warning,
                Warning::DeletionVectors { .. }
                    | Warning::ColumnMapping { .. }
                    | Warning::LiquidClustering { .. }
                    | Warning::UnrecognizedTableFeature { .. }
            )
        })
        .map(|warning| {
            json!({
                "code":
                    warning.code(),

                "message":
                    warning_message(
                        warning
                    ),
            })
        })
        .collect()
}

fn analysis_warning_code(warning: &Warning) -> &'static str {
    match warning {
        Warning::PartitionEvaluationGap { .. } => "PARTITION_EVAL_GAP",

        _ => warning.code(),
    }
}

fn explanation(explanation: &Explanation) -> Value {
    json!({
        "code":
            explanation.code(),

        "severity":
            explanation_severity(
                explanation
            ),

        "message":
            explanation_message(
                explanation
            ),

        "suggestion":
            explanation_suggestion(
                explanation
            ),
    })
}

fn explanation_severity(explanation: &Explanation) -> &'static str {
    match explanation {
        Explanation::UnsupportedFragment { .. } => "info",

        _ => "warning",
    }
}

fn predicate_conjunction(predicates: &[Pred]) -> Option<String> {
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
        return None;
    }

    Some(
        classification
            .unsplittable
            .iter()
            .map(|fragment| fragment.predicate.to_string())
            .collect::<Vec<_>>()
            .join(" AND "),
    )
}

fn phase_predicate(kind: PhaseKind, classification: &PredicateClassification) -> String {
    match kind {
        PhaseKind::PartitionPruning => classification
            .partition_safe
            .iter()
            .chain(classification.partition_exact.iter())
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" AND "),

        PhaseKind::DataSkipping => classification
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
            .join(" AND "),
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

fn stats_mode(present: usize, total: usize) -> &'static str {
    if total == 0 || present == 0 {
        "absent"
    } else if present == total {
        "exact"
    } else {
        "partial"
    }
}

fn pruning_pct(input: usize, output: usize) -> f64 {
    if input == 0 {
        return 0.0;
    }

    let dropped = input.saturating_sub(output);

    dropped as f64 / input as f64 * 100.0
}
