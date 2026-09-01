use std::fmt::Write;

use crate::v2::analysis::model::{
    Confidence, PhaseKind, PredicateClassification, UnsplittableHandling,
};
use crate::v2::diagnostics::{Explanation, Warning};
use crate::v2::gates::{AssertionResult, GateOutcome, GateStatus};

use crate::v2::report::{PredicateReport, Report};

pub fn gate_failures(outcome: &GateOutcome) -> Vec<String> {
    outcome
        .assertions
        .iter()
        .filter_map(assertion_failure)
        .collect()
}

fn assertion_failure(assertion: &AssertionResult) -> Option<String> {
    if assertion.status() != GateStatus::Fail {
        return None;
    }

    match assertion {
        AssertionResult::MinPruning {
            threshold, actual, ..
        } => Some(format!(
            "ASSERTION FAILED: total pruning \
             {actual:.1}% is below threshold \
             {threshold:.1}%"
        )),

        AssertionResult::StatsComplete { missing_files, .. } => {
            let mut message = format!(
                "ASSERTION FAILED: {} file(s) missing statistics:",
                missing_files.len()
            );

            for path in missing_files {
                message.push_str(&format!("\n  {path}"));
            }

            Some(message)
        }
    }
}

pub fn text(report: &Report, explain_why: bool) -> String {
    let mut out = String::new();

    writeln!(out, "Delta table: {}", report.table.path).unwrap();

    writeln!(out, "Version:     {}", report.table.version).unwrap();

    if let Some(predicate) = &report.predicate {
        writeln!(out, "Predicate:   {}", predicate.input).unwrap();

        write_predicate_analysis(&mut out, predicate);

        write_phases(&mut out, report, predicate);
    } else {
        writeln!(out).unwrap();

        writeln!(out, "Files in snapshot: {}", report.table.total_files).unwrap();
    }

    write_warnings(&mut out, report);

    if explain_why {
        write_explanations(&mut out, report);
    }

    out
}

fn write_predicate_analysis(out: &mut String, predicate: &PredicateReport) {
    writeln!(out).unwrap();

    writeln!(out, "Predicate Analysis:").unwrap();

    writeln!(
        out,
        "  partition-safe: {}",
        conjunction(&predicate.classification.partition_safe)
    )
    .unwrap();

    if !predicate.classification.partition_exact.is_empty() {
        writeln!(
            out,
            "  partition-exact: {}",
            conjunction(&predicate.classification.partition_exact)
        )
        .unwrap();
    }

    writeln!(
        out,
        "  stats-safe:     {}",
        conjunction(&predicate.classification.stats_safe)
    )
    .unwrap();

    writeln!(
        out,
        "  unsplittable:   {}",
        unsplittable_display(&predicate.classification)
    )
    .unwrap();

    writeln!(
        out,
        "  confidence:     {}",
        confidence_label(predicate.confidence)
    )
    .unwrap();
}

fn write_phases(out: &mut String, report: &Report, predicate: &PredicateReport) {
    writeln!(out).unwrap();

    writeln!(out, "Files in snapshot: {}", report.table.total_files).unwrap();

    for (index, phase) in predicate.phases.iter().enumerate() {
        let dropped = phase.input_count.saturating_sub(phase.output_count);

        let pct = pruning_pct(phase.input_count, phase.output_count);

        writeln!(out).unwrap();

        writeln!(
            out,
            "Phase {}: {} [{}]",
            index + 1,
            phase_name(phase.kind),
            confidence_label(phase.confidence),
        )
        .unwrap();

        writeln!(
            out,
            "  predicate:       {}",
            phase_predicate(phase.kind, &predicate.classification,)
        )
        .unwrap();

        writeln!(
            out,
            "  files remaining: {}  (-{}, {:.0}% pruned)",
            phase.output_count, dropped, pct,
        )
        .unwrap();
    }

    if predicate.phases.len() > 1 {
        if let Some(last) = predicate.phases.last() {
            writeln!(out).unwrap();

            writeln!(
                out,
                "Total reduction: {} -> {} files ({:.0}% pruned)",
                report.table.total_files,
                last.output_count,
                pruning_pct(report.table.total_files, last.output_count,),
            )
            .unwrap();
        }
    }
}

fn write_warnings(out: &mut String, report: &Report) {
    if report.warnings.is_empty() {
        return;
    }

    writeln!(out).unwrap();

    writeln!(out, "Warnings!").unwrap();

    for warning in &report.warnings {
        writeln!(out, "[{}]: {}", warning.code(), warning_message(warning),).unwrap();
    }
}

fn warning_message(warning: &Warning) -> String {
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

fn write_explanations(out: &mut String, report: &Report) {
    writeln!(out).unwrap();

    writeln!(out, "Why:").unwrap();

    if report.predicate.is_none() {
        writeln!(
            out,
            "  No predicate given (pass --where to diagnose pruning)."
        )
        .unwrap();

        return;
    }

    if report.explanations.is_empty() {
        writeln!(out, "  No pruning issues found.").unwrap();

        return;
    }

    for explanation in &report.explanations {
        writeln!(
            out,
            "  [{}] {}",
            explanation.code(),
            explanation_message(explanation),
        )
        .unwrap();

        if let Some(suggestion) = explanation_suggestion(explanation) {
            writeln!(out, "    -> {suggestion}").unwrap();
        }
    }
}

fn explanation_message(explanation: &Explanation) -> String {
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

fn explanation_suggestion(explanation: &Explanation) -> Option<String> {
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

fn phase_predicate(kind: PhaseKind, classification: &PredicateClassification) -> String {
    match kind {
        PhaseKind::PartitionPruning => {
            let predicates = classification
                .partition_safe
                .iter()
                .chain(classification.partition_exact.iter())
                .map(ToString::to_string)
                .collect::<Vec<_>>();

            display_conjunction(&predicates)
        }

        PhaseKind::DataSkipping => data_skipping_predicate(classification),
    }
}

fn data_skipping_predicate(classification: &PredicateClassification) -> String {
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

    let stripped = classification.stripped_count();

    let scanned = display_conjunction(&scanned);

    match (stripped, scanned.as_str()) {
        (0, _) => scanned,

        (1, "-") => "(1 unsupported fragment, keeps all files)".to_string(),

        (n, "-") => {
            format!("({n} unsupported fragments, keep all files)")
        }

        (1, scanned) => {
            format!("{scanned}  (+1 unsupported fragment, keeps all files)")
        }

        (n, scanned) => {
            format!("{scanned}  (+{n} unsupported fragments, keep all files)")
        }
    }
}

fn conjunction(predicates: &[crate::v2::analysis::predicate::Pred]) -> String {
    let rendered = predicates
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    display_conjunction(&rendered)
}

fn display_conjunction(predicates: &[String]) -> String {
    if predicates.is_empty() {
        "-".to_string()
    } else {
        predicates.join(" AND ")
    }
}

fn unsplittable_display(classification: &PredicateClassification) -> String {
    let rendered = classification
        .unsplittable
        .iter()
        .map(|fragment| fragment.predicate.to_string())
        .collect::<Vec<_>>();

    display_conjunction(&rendered)
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
