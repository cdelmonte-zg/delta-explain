//! Presentation layer: text and JSON rendering of a [`PruningReport`].
//!
//! The report module owns the computed model; everything about how it is
//! shown lives here. The JSON document carries its own `schema_version`,
//! versioned independently of the tool per SemVer.

use std::collections::HashSet;

use num_format::{Locale, ToFormattedString};
use serde_json::json;

use crate::report::{PhaseResult, PruningReport};
use crate::stats::FileStats;

pub const SCHEMA_VERSION: &str = "0.2.0";

pub enum OutputFormat {
    Text,
    Json,
}

// ── Text output ─────────────────────────────────────────────────────

pub fn print_text(
    report: &PruningReport,
    verbose: bool,
    limit: Option<usize>,
    predicate: Option<&str>,
) {
    println!("Delta table: {}", report.table_path);
    println!("Version:     {}", report.version);
    if let Some(pred) = predicate {
        println!("Predicate:   {pred}");
    }

    if let Some(analysis) = &report.analysis {
        println!();
        println!("Predicate Analysis:");
        println!(
            "  partition-safe: {}",
            analysis.partition_safe.as_deref().unwrap_or("-")
        );
        println!(
            "  stats-safe:     {}",
            analysis.stats_safe.as_deref().unwrap_or("-")
        );
        println!(
            "  unsplittable:   {}",
            analysis.unsplittable.as_deref().unwrap_or("-")
        );
        println!("  confidence:     {}", analysis.confidence);
    }

    println!();
    println!("Files in snapshot: {}", fmt(report.total_files));

    if report.phases.is_empty() {
        return;
    }

    for (i, phase) in report.phases.iter().enumerate() {
        let dropped = phase.input_count.saturating_sub(phase.output_count);
        let pct = pruning_pct(phase.input_count, phase.output_count);

        println!();
        println!("Phase {}: {} [{}]", i + 1, phase.name, phase.confidence);
        println!("  predicate:       {}", phase.predicate_display);
        println!(
            "  files remaining: {}  (-{}, {:.0}% pruned)",
            fmt(phase.output_count),
            fmt(dropped),
            pct
        );

        if verbose {
            print_phase_details(report, phase, i, limit);
        }
    }

    // Summary
    if report.phases.len() > 1 {
        let final_count = report.phases.last().unwrap().output_count;
        println!();
        println!(
            "Total reduction: {} -> {} files ({:.0}% pruned)",
            fmt(report.total_files),
            fmt(final_count),
            report.total_pruning_pct(),
        );
    }

    if let Some(analysis) = &report.analysis
        && !analysis.notes.is_empty()
    {
        println!();
        println!("Warnings!");
        for note in &analysis.notes {
            println!("[{}]: {}", note.code, note.message);
        }
    }
}

fn print_phase_details(
    report: &PruningReport,
    phase: &PhaseResult,
    phase_idx: usize,
    limit: Option<usize>,
) {
    let candidates: HashSet<&str> = if phase_idx == 0 {
        report.all_files.iter().map(|f| f.path.as_str()).collect()
    } else {
        report.phases[phase_idx - 1]
            .surviving_paths
            .iter()
            .map(|s| s.as_str())
            .collect()
    };

    println!();
    let mut shown = 0usize;
    for file in &report.all_files {
        if !candidates.contains(file.path.as_str()) {
            continue;
        }
        if let Some(cap) = limit
            && shown >= cap
        {
            let remaining = candidates.len().saturating_sub(shown);
            println!(
                "  ... and {} more files (raise --limit to see them)",
                fmt(remaining)
            );
            break;
        }
        shown += 1;

        let kept = phase.surviving_paths.contains(&file.path);
        let short_path = shorten_path(&file.path);

        let partition_str = if file.partition_values.is_empty() {
            String::new()
        } else {
            let mut parts: Vec<String> = file
                .partition_values
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            parts.sort();
            format!("  partition({})", parts.join(", "))
        };

        let stats_str = match report.file_stats.get(&file.path) {
            Some(stats) => format_stats_compact(stats),
            None => "  [no stats]".into(),
        };

        let tag = if kept { "KEPT   " } else { "DROPPED" };
        let size_str = format_size(file.size);
        // The kernel's ScanFile carries num_records only when the add
        // action has JSON stats; on stats_parsed-only checkpoints it is
        // None, so fall back to the parsed stats map.
        let records_str = file
            .num_records
            .or_else(|| {
                report
                    .file_stats
                    .get(&file.path)
                    .and_then(|s| s.num_records)
            })
            .map(|n| format!("  {n} records"))
            .unwrap_or_default();

        println!("  [{tag}] {short_path}  ({size_str}{records_str}){partition_str}{stats_str}");
    }
}

/// Render a file's statistics as the compact ` stats(col: min..max, ...)`
/// suffix of the verbose line, or ` [no stats]` when the entry carries
/// nothing displayable.
fn format_stats_compact(stats: &FileStats) -> String {
    if stats.columns.is_empty() {
        return if stats.num_records.is_some() {
            String::new()
        } else {
            "  [no stats]".into()
        };
    }

    let mut parts: Vec<String> = Vec::new();
    let mut cols: Vec<_> = stats.columns.iter().collect();
    cols.sort_by_key(|(k, _)| *k);

    for (col, col_stats) in cols {
        match (&col_stats.min, &col_stats.max) {
            (Some(min), Some(max)) => parts.push(format!("{col}: {min}..{max}")),
            (Some(min), None) => parts.push(format!("{col}: min={min}")),
            (None, Some(max)) => parts.push(format!("{col}: max={max}")),
            (None, None) => {}
        }
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!("  stats({})", parts.join(", "))
    }
}

// ── JSON output ─────────────────────────────────────────────────────

pub fn print_json(
    report: &PruningReport,
    verbose: bool,
    limit: Option<usize>,
    predicate: Option<&str>,
) {
    let (stats_present, stats_total) = report.stats_coverage();
    let stats_pct = if stats_total > 0 {
        (stats_present as f64 / stats_total as f64) * 100.0
    } else {
        0.0
    };
    let stats_mode = if stats_total == 0 || stats_present == 0 {
        "absent"
    } else if stats_present == stats_total {
        "exact"
    } else {
        "partial"
    };

    let phases: Vec<serde_json::Value> = report
        .phases
        .iter()
        .map(|phase| {
            json!({
                "name": phase.name,
                "confidence": phase.confidence.to_string(),
                "predicate": phase.predicate_display,
                "input_files": phase.input_count,
                "output_files": phase.output_count,
                "pruned_files": phase.input_count.saturating_sub(phase.output_count),
                "pruning_pct": pruning_pct(phase.input_count, phase.output_count),
            })
        })
        .collect();

    let analysis_block = report.analysis.as_ref().map(|analysis| {
        json!({
            "partition_safe": analysis.partition_safe,
            "stats_safe": analysis.stats_safe,
            "unsplittable": analysis.unsplittable,
            "confidence": analysis.confidence.to_string(),
            "notes": analysis.notes.iter().map(|n| json!({
                "code": n.code,
                "message": n.message,
            })).collect::<Vec<_>>(),
        })
    });

    let result_value = match report.overall_result {
        Some(r) => json!(r.to_string()),
        None => serde_json::Value::Null,
    };

    let mut output = json!({
        "schema_version": SCHEMA_VERSION,
        "tool_version": env!("CARGO_PKG_VERSION"),
        "elapsed_ms": report.elapsed_ms,
        "table": report.table_path,
        "version": report.version,
        "predicate": predicate,
        "total_files": report.total_files,
        "final_files": report.phases.last().map(|p| p.output_count).unwrap_or(report.total_files),
        "total_pruning_pct": report.total_pruning_pct(),
        "analysis": analysis_block,
        "stats": {
            "mode": stats_mode,
            "files_with_stats": stats_present,
            "total_files": stats_total,
            "pct": stats_pct,
        },
        "phases": phases,
        "assertions": report.assertions,
        "result": result_value,
    });

    // Per-file detail rides behind --verbose, like the text listing: the
    // compact document stays byte-stable for existing consumers, and a
    // 200k-file table does not produce a 200k-element array unasked.
    if verbose {
        let cap = limit.unwrap_or(usize::MAX);
        let files: Vec<serde_json::Value> = report
            .all_files
            .iter()
            .take(cap)
            .map(|file| {
                let num_records = file.num_records.or_else(|| {
                    report
                        .file_stats
                        .get(&file.path)
                        .and_then(|s| s.num_records)
                });
                json!({
                    "path": file.path,
                    "size_bytes": file.size,
                    "partition_values": file.partition_values,
                    "num_records": num_records,
                    "has_stats": report.file_stats.contains_key(&file.path),
                    "kept": report.pruned_by(&file.path).is_none(),
                    "pruned_by": report.pruned_by(&file.path).map(|p| p.name.clone()),
                })
            })
            .collect();
        output["files_truncated"] = json!(report.all_files.len() > files.len());
        output["files"] = json!(files);
    }

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

// ── Shared formatting helpers ───────────────────────────────────────

fn shorten_path(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn fmt(n: usize) -> String {
    n.to_formatted_string(&Locale::en)
}

fn pruning_pct(input: usize, output: usize) -> f64 {
    if input == 0 {
        return 0.0;
    }
    let dropped = input.saturating_sub(output);
    (dropped as f64 / input as f64) * 100.0
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
