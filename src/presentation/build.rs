use super::diagnostics::{
    explanation_message, explanation_severity, explanation_suggestion, warning_message,
};
use super::files;
use super::labels::{confidence_label, phase_name};
use super::model::{
    AnalysisView, AssertionView, DiagnosticScope, ExplanationView, PhaseView, Presentation,
    PresentationOptions, StatsView, StatusView, TableFeaturesView, TableView, WarningView,
};

use crate::analysis::model::{
    Confidence, PhaseKind, PredicateClassification, UnsplittableHandling,
};
use crate::analysis::predicate::Pred;
use crate::diagnostics::{Explanation, Warning};
use crate::gates::{AssertionResult, GateOutcome, GateStatus};
use crate::metadata::scan::BaselineScan;
use crate::report::Report;

pub fn build(
    report: &Report,
    gates: &GateOutcome,
    baseline: &BaselineScan,
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

    let files = files::build(report, baseline, options);

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

        files,

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

        severity: explanation_severity(explanation),

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

fn pruning_pct(input: usize, output: usize) -> f64 {
    if input == 0 {
        return 0.0;
    }

    let dropped = input.saturating_sub(output);

    dropped as f64 / input as f64 * 100.0
}
