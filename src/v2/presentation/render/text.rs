use std::fmt::Write;

use crate::v2::presentation::{AssertionView, PhaseView, Presentation, StatusView};

pub(super) fn render(presentation: &Presentation) -> String {
    let mut out = String::new();

    writeln!(out, "Delta table: {}", presentation.table.path).unwrap();

    writeln!(out, "Version:     {}", presentation.table.version).unwrap();

    if let Some(predicate) = &presentation.predicate {
        writeln!(out, "Predicate:   {predicate}").unwrap();

        if let Some(analysis) = &presentation.analysis {
            write_analysis(&mut out, analysis);
        }

        write_phases(&mut out, presentation);
    } else {
        writeln!(out).unwrap();

        writeln!(out, "Files in snapshot: {}", presentation.table.total_files).unwrap();
    }

    write_warnings(&mut out, presentation);

    if let Some(explanations) = &presentation.explanations {
        write_explanations(&mut out, presentation, explanations);
    }

    out
}

pub(super) fn gate_failures(presentation: &Presentation) -> Vec<String> {
    presentation
        .assertions
        .iter()
        .filter_map(assertion_failure)
        .collect()
}

fn assertion_failure(assertion: &AssertionView) -> Option<String> {
    if assertion.status() != StatusView::Fail {
        return None;
    }

    match assertion {
        AssertionView::MinPruning {
            threshold, actual, ..
        } => Some(format!(
            "ASSERTION FAILED: total pruning \
             {actual:.1}% is below threshold \
             {threshold:.1}%"
        )),

        AssertionView::StatsComplete { missing_files, .. } => {
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

fn write_analysis(out: &mut String, analysis: &crate::v2::presentation::AnalysisView) {
    writeln!(out).unwrap();

    writeln!(out, "Predicate Analysis:").unwrap();

    writeln!(
        out,
        "  partition-safe: {}",
        display_optional(analysis.partition_safe.as_deref())
    )
    .unwrap();

    if let Some(partition_exact) = &analysis.partition_exact {
        writeln!(out, "  partition-exact: {partition_exact}").unwrap();
    }

    writeln!(
        out,
        "  stats-safe:     {}",
        display_optional(analysis.stats_safe.as_deref())
    )
    .unwrap();

    writeln!(
        out,
        "  unsplittable:   {}",
        display_optional(analysis.unsplittable.as_deref())
    )
    .unwrap();

    writeln!(out, "  confidence:     {}", analysis.confidence).unwrap();
}

fn write_phases(out: &mut String, presentation: &Presentation) {
    writeln!(out).unwrap();

    writeln!(out, "Files in snapshot: {}", presentation.table.total_files).unwrap();

    for (index, phase) in presentation.phases.iter().enumerate() {
        writeln!(out).unwrap();

        writeln!(
            out,
            "Phase {}: {} [{}]",
            index + 1,
            phase.name,
            phase.confidence,
        )
        .unwrap();

        writeln!(out, "  predicate:       {}", phase_predicate(phase)).unwrap();

        writeln!(
            out,
            "  files remaining: {}  (-{}, {:.0}% pruned)",
            phase.output_files, phase.pruned_files, phase.pruning_pct,
        )
        .unwrap();
    }

    if presentation.phases.len() > 1 {
        writeln!(out).unwrap();

        writeln!(
            out,
            "Total reduction: {} -> {} files ({:.0}% pruned)",
            presentation.table.total_files,
            presentation.final_files,
            presentation.total_pruning_pct,
        )
        .unwrap();
    }
}

fn write_warnings(out: &mut String, presentation: &Presentation) {
    if presentation.warnings.is_empty() {
        return;
    }

    writeln!(out).unwrap();

    writeln!(out, "Warnings!").unwrap();

    for warning in &presentation.warnings {
        writeln!(out, "[{}]: {}", warning.code, warning.message,).unwrap();
    }
}

fn write_explanations(
    out: &mut String,
    presentation: &Presentation,
    explanations: &[crate::v2::presentation::ExplanationView],
) {
    writeln!(out).unwrap();

    writeln!(out, "Why:").unwrap();

    if presentation.predicate.is_none() {
        writeln!(
            out,
            "  No predicate given (pass --where to diagnose pruning)."
        )
        .unwrap();

        return;
    }

    if explanations.is_empty() {
        writeln!(out, "  No pruning issues found.").unwrap();

        return;
    }

    for explanation in explanations {
        writeln!(out, "  [{}] {}", explanation.code, explanation.message,).unwrap();

        if let Some(suggestion) = &explanation.suggestion {
            writeln!(out, "    -> {suggestion}").unwrap();
        }
    }
}

fn phase_predicate(phase: &PhaseView) -> String {
    match (
        phase.conservative_fragments,
        phase.scanned_predicate.as_deref(),
    ) {
        (0, _) => phase.predicate.clone(),

        (1, Some(scanned)) => {
            format!(
                "{scanned}  \
                 (+1 unsupported fragment, keeps all files)"
            )
        }

        (n, Some(scanned)) => {
            format!(
                "{scanned}  \
                 (+{n} unsupported fragments, keep all files)"
            )
        }

        (1, None) => "(1 unsupported fragment, keeps all files)".to_string(),

        (n, None) => {
            format!("({n} unsupported fragments, keep all files)")
        }
    }
}

fn display_optional(value: Option<&str>) -> &str {
    value.unwrap_or("-")
}
