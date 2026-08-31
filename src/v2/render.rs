use std::fmt::Write;

use crate::v2::analysis::model::{
    Confidence, PhaseKind, PredicateClassification, UnsplittableHandling,
};
use crate::v2::diagnostics::Diagnostic;
use crate::v2::report::{PredicateReport, Report};

pub fn text(report: &Report) -> String {
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

    write_diagnostics(&mut out, report);

    out
}

fn write_diagnostics(out: &mut String, report: &Report) {
    if report.diagnostics.is_empty() {
        return;
    }

    writeln!(out).unwrap();
    writeln!(out, "Warnings!").unwrap();

    for diagnostic in &report.diagnostics {
        writeln!(
            out,
            "[{}]: {}",
            diagnostic.code(),
            diagnostic_message(diagnostic),
        )
        .unwrap();
    }
}

fn diagnostic_message(diagnostic: &Diagnostic) -> String {
    match diagnostic {
        Diagnostic::UnsupportedExpression { predicate, reasons } => {
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

        Diagnostic::UnsplittableOr { .. } => "Mixed expression across partition and \
             non-partition columns; cannot separate \
             safely, routed as unsplittable"
            .to_string(),

        Diagnostic::PartitionEvaluationGap { count } => {
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
    }
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

        (n, "-") => format!("({n} unsupported fragments, keep all files)"),

        (1, scanned) => format!("{scanned}  (+1 unsupported fragment, keeps all files)"),

        (n, scanned) => format!("{scanned}  (+{n} unsupported fragments, keep all files)"),
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
