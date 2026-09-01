use std::fmt::Write;

use num_format::{Locale, ToFormattedString};

use crate::presentation::{
    AnalysisView, AssertionView, ExplanationView, FilePhaseState, FileView, FilesView, PhaseView,
    Presentation, StatusView,
};

pub(super) fn render(presentation: &Presentation) -> String {
    let mut out = String::new();

    let _ = writeln!(out, "Delta table: {}", presentation.table.path);

    let _ = writeln!(out, "Version:     {}", presentation.table.version);

    if let Some(predicate) = &presentation.predicate {
        let _ = writeln!(out, "Predicate:   {predicate}");

        if let Some(analysis) = &presentation.analysis {
            write_analysis(&mut out, analysis);
        }

        write_phases(&mut out, presentation);
    } else {
        let _ = writeln!(out);

        let _ = writeln!(
            out,
            "Files in snapshot: {}",
            fmt(presentation.table.total_files)
        );
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

fn write_analysis(out: &mut String, analysis: &AnalysisView) {
    let _ = writeln!(out);

    let _ = writeln!(out, "Predicate Analysis:");

    let _ = writeln!(
        out,
        "  partition-safe: {}",
        display_optional(analysis.partition_safe.as_deref())
    );

    if let Some(partition_exact) = &analysis.partition_exact {
        let _ = writeln!(out, "  partition-exact: {partition_exact}");
    }

    let _ = writeln!(
        out,
        "  stats-safe:     {}",
        display_optional(analysis.stats_safe.as_deref())
    );

    let _ = writeln!(
        out,
        "  unsplittable:   {}",
        display_optional(analysis.unsplittable.as_deref())
    );

    let _ = writeln!(out, "  confidence:     {}", analysis.confidence);
}

fn write_phases(out: &mut String, presentation: &Presentation) {
    let _ = writeln!(out);

    let _ = writeln!(
        out,
        "Files in snapshot: {}",
        fmt(presentation.table.total_files)
    );

    for (index, phase) in presentation.phases.iter().enumerate() {
        let _ = writeln!(out);

        let _ = writeln!(
            out,
            "Phase {}: {} [{}]",
            index + 1,
            phase.name,
            phase.confidence,
        );

        let _ = writeln!(out, "  predicate:       {}", phase_predicate(phase));

        let _ = writeln!(
            out,
            "  files remaining: {}  (-{}, {:.0}% pruned)",
            fmt(phase.output_files),
            fmt(phase.pruned_files),
            phase.pruning_pct,
        );

        if let Some(files) = &presentation.files {
            write_phase_details(out, files, index);
        }
    }

    if presentation.phases.len() > 1 {
        let _ = writeln!(out);

        let _ = writeln!(
            out,
            "Total reduction: {} -> {} files ({:.0}% pruned)",
            fmt(presentation.table.total_files),
            fmt(presentation.final_files),
            presentation.total_pruning_pct,
        );
    }
}

fn write_phase_details(out: &mut String, files: &FilesView, phase_index: usize) {
    let candidate_count = files
        .entries
        .iter()
        .filter(|file| {
            file.phase_states
                .get(phase_index)
                .is_some_and(|state| *state != FilePhaseState::NotCandidate)
        })
        .count();

    let _ = writeln!(out);

    let mut shown = 0usize;

    for file in &files.entries {
        let Some(state) = file.phase_states.get(phase_index) else {
            continue;
        };

        if *state == FilePhaseState::NotCandidate {
            continue;
        }

        if let Some(cap) = files.limit
            && shown >= cap
        {
            let remaining = candidate_count.saturating_sub(shown);

            let _ = writeln!(
                out,
                "  ... and {} more files (raise --limit to see them)",
                fmt(remaining)
            );

            break;
        }

        shown += 1;

        write_file(out, file, *state);
    }
}

fn write_file(out: &mut String, file: &FileView, state: FilePhaseState) {
    let kept = state == FilePhaseState::Kept;

    let tag = if kept { "KEPT   " } else { "DROPPED" };

    let short_path = shorten_path(&file.path);

    let size = format_size(file.size_bytes);

    let records = file
        .num_records
        .map(|records| format!("  {records} records"))
        .unwrap_or_default();

    let partitions = format_partitions(file);

    let stats = format_stats_compact(file);

    let _ = writeln!(
        out,
        "  [{tag}] {short_path}  ({size}{records}){partitions}{stats}"
    );
}

fn format_partitions(file: &FileView) -> String {
    if file.partition_values.is_empty() {
        return String::new();
    }

    let mut parts = file
        .partition_values
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();

    parts.sort();

    format!("  partition({})", parts.join(", "))
}

fn format_stats_compact(file: &FileView) -> String {
    if !file.has_stats {
        return "  [no stats]".to_string();
    }

    if file.stats.is_empty() {
        return if file.num_records.is_some() {
            String::new()
        } else {
            "  [no stats]".to_string()
        };
    }

    let mut parts = Vec::new();

    for stats in &file.stats {
        match (&stats.min, &stats.max) {
            (Some(min), Some(max)) => {
                parts.push(format!("{}: {}..{}", stats.column, min, max,));
            }

            (Some(min), None) => {
                parts.push(format!("{}: min={}", stats.column, min,));
            }

            (None, Some(max)) => {
                parts.push(format!("{}: max={}", stats.column, max,));
            }

            (None, None) => {}
        }
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!("  stats({})", parts.join(", "))
    }
}

fn write_warnings(out: &mut String, presentation: &Presentation) {
    if presentation.warnings.is_empty() {
        return;
    }

    let _ = writeln!(out);

    let _ = writeln!(out, "Warnings!");

    for warning in &presentation.warnings {
        let _ = writeln!(out, "[{}]: {}", warning.code, warning.message,);
    }
}

fn write_explanations(
    out: &mut String,
    presentation: &Presentation,
    explanations: &[ExplanationView],
) {
    let _ = writeln!(out);

    let _ = writeln!(out, "Why:");

    if presentation.predicate.is_none() {
        let _ = writeln!(
            out,
            "  No predicate given (pass --where to diagnose pruning)."
        );

        return;
    }

    if explanations.is_empty() {
        let _ = writeln!(out, "  No pruning issues found.");

        return;
    }

    for explanation in explanations {
        let _ = writeln!(out, "  [{}] {}", explanation.code, explanation.message,);

        if let Some(suggestion) = &explanation.suggestion {
            let _ = writeln!(out, "    -> {suggestion}");
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

fn shorten_path(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn fmt(value: usize) -> String {
    value.to_formatted_string(&Locale::en)
}

fn format_size(bytes: i64) -> String {
    const KB: i64 = 1024;

    const MB: i64 = 1024 * KB;

    const GB: i64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}
