//! Presentation layer: text and JSON rendering of a [`PruningReport`].
//!
//! The report module owns the computed model; everything about how it is
//! shown lives here. The JSON document carries its own `schema_version`,
//! versioned independently of the tool per SemVer.

use std::collections::HashSet;
use std::io::Write;

use num_format::{Locale, ToFormattedString};
use serde_json::json;

use crate::error::Error;
use crate::report::{PhaseResult, PruningReport};
use crate::stats::FileStats;

pub const SCHEMA_VERSION: &str = "0.3.0";

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
) -> std::io::Result<()> {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    write_text(&mut out, report, verbose, limit, predicate)
}

fn write_text(
    out: &mut impl Write,
    report: &PruningReport,
    verbose: bool,
    limit: Option<usize>,
    predicate: Option<&str>,
) -> std::io::Result<()> {
    writeln!(out, "Delta table: {}", report.table_path)?;
    writeln!(out, "Version:     {}", report.version)?;
    if let Some(pred) = predicate {
        writeln!(out, "Predicate:   {pred}")?;
    }

    if let Some(analysis) = &report.analysis {
        writeln!(out)?;
        writeln!(out, "Predicate Analysis:")?;
        writeln!(
            out,
            "  partition-safe: {}",
            analysis.partition_safe.as_deref().unwrap_or("-")
        )?;
        if let Some(exact) = &analysis.partition_exact {
            writeln!(out, "  partition-exact: {exact}")?;
        }
        writeln!(
            out,
            "  stats-safe:     {}",
            analysis.stats_safe.as_deref().unwrap_or("-")
        )?;
        writeln!(
            out,
            "  unsplittable:   {}",
            analysis.unsplittable.as_deref().unwrap_or("-")
        )?;
        writeln!(out, "  confidence:     {}", analysis.confidence)?;
    }

    writeln!(out)?;
    writeln!(out, "Files in snapshot: {}", fmt(report.total_files))?;

    for (i, phase) in report.phases.iter().enumerate() {
        let dropped = phase.input_count.saturating_sub(phase.output_count);
        let pct = pruning_pct(phase.input_count, phase.output_count);

        writeln!(out)?;
        writeln!(
            out,
            "Phase {}: {} [{}]",
            i + 1,
            phase.name,
            phase.confidence
        )?;
        // A stripped fragment is applied conservatively, never scanned
        // with: showing it on the predicate line as if it contributed
        // would contradict the analysis block three lines above. Show
        // what reached the kernel, annotate the rest.
        match (phase.conservative_fragments, &phase.scan_predicate_display) {
            (0, _) => writeln!(out, "  predicate:       {}", phase.predicate_display)?,
            (1, Some(scanned)) => writeln!(
                out,
                "  predicate:       {scanned}  (+1 unsupported fragment, keeps all files)"
            )?,
            (n, Some(scanned)) => writeln!(
                out,
                "  predicate:       {scanned}  (+{n} unsupported fragments, keep all files)"
            )?,
            (1, None) => writeln!(
                out,
                "  predicate:       (1 unsupported fragment, keeps all files)"
            )?,
            (n, None) => writeln!(
                out,
                "  predicate:       ({n} unsupported fragments, keep all files)"
            )?,
        }
        writeln!(
            out,
            "  files remaining: {}  (-{}, {:.0}% pruned)",
            fmt(phase.output_count),
            fmt(dropped),
            pct
        )?;

        if verbose {
            write_phase_details(out, report, phase, i, limit)?;
        }
    }

    // Summary
    if report.phases.len() > 1
        && let Some(last_phase) = report.phases.last()
    {
        writeln!(out)?;
        writeln!(
            out,
            "Total reduction: {} -> {} files ({:.0}% pruned)",
            fmt(report.total_files),
            fmt(last_phase.output_count),
            report.total_pruning_pct(),
        )?;
    }

    let mut warnings = report.table_features.notes(report.total_files);
    if let Some(analysis) = &report.analysis {
        warnings.extend(analysis.notes.iter().cloned());
    }
    if !warnings.is_empty() {
        writeln!(out)?;
        writeln!(out, "Warnings!")?;
        for note in &warnings {
            writeln!(out, "[{}]: {}", note.code, note.message)?;
        }
    }
    Ok(())
}

fn write_phase_details(
    out: &mut impl Write,
    report: &PruningReport,
    phase: &PhaseResult,
    phase_idx: usize,
    limit: Option<usize>,
) -> std::io::Result<()> {
    let candidates: HashSet<&str> = if phase_idx == 0 {
        report.all_files.iter().map(|f| f.path.as_str()).collect()
    } else {
        report.phases[phase_idx - 1]
            .surviving_paths
            .iter()
            .map(|s| s.as_str())
            .collect()
    };

    writeln!(out)?;
    let mut shown = 0usize;
    for file in &report.all_files {
        if !candidates.contains(file.path.as_str()) {
            continue;
        }
        if let Some(cap) = limit
            && shown >= cap
        {
            let remaining = candidates.len().saturating_sub(shown);
            writeln!(
                out,
                "  ... and {} more files (raise --limit to see them)",
                fmt(remaining)
            )?;
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

        writeln!(
            out,
            "  [{tag}] {short_path}  ({size_str}{records_str}){partition_str}{stats_str}"
        )?;
    }
    Ok(())
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

/// Renders the JSON document as a string; the CLI layer owns writing it,
/// so serialization errors and output errors stay distinguishable.
pub fn render_json(
    report: &PruningReport,
    verbose: bool,
    limit: Option<usize>,
    predicate: Option<&str>,
) -> Result<String, Error> {
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
            "partition_exact": analysis.partition_exact,
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

    let tf = &report.table_features;
    let table_features = json!({
        "deletion_vectors": {
            "enabled": tf.deletion_vectors_enabled,
            "files_with_deletion_vectors": tf.files_with_deletion_vectors,
        },
        "column_mapping_mode": tf.column_mapping_mode,
        "clustering_columns": tf.clustering_columns,
        "in_commit_timestamps": tf.in_commit_timestamps,
        "unrecognized_writer_features": tf.unrecognized_writer_features,
        "notes": tf.notes(report.total_files).iter().map(|n| json!({
            "code": n.code,
            "message": n.message,
        })).collect::<Vec<_>>(),
    });

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
        "table_features": table_features,
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

    Ok(serde_json::to_string_pretty(&output)?)
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
