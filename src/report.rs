use std::collections::{HashMap, HashSet};

use num_format::{Locale, ToFormattedString};
use serde_json::json;

use crate::predicate_analyzer::{Confidence, PredicateAnalysis};
use crate::stats::FileStats;

pub const SCHEMA_VERSION: &str = "0.1.0";

pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverallResult {
    Pass,
    Fail,
}

impl std::fmt::Display for OverallResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            OverallResult::Pass => "pass",
            OverallResult::Fail => "fail",
        })
    }
}

pub struct FileInfo {
    pub path: String,
    pub size: i64,
    pub partition_values: HashMap<String, String>,
    pub num_records: Option<u64>,
}

pub struct PhaseResult {
    pub confidence: Confidence,
    pub name: String,
    pub predicate_display: String,
    pub input_count: usize,
    pub output_count: usize,
    pub surviving_paths: HashSet<String>,
}

pub struct PruningReport {
    pub analysis: Option<PredicateAnalysis>,
    pub table_path: String,
    pub version: u64,
    pub total_files: usize,
    pub all_files: Vec<FileInfo>,
    pub file_stats: HashMap<String, FileStats>,
    pub phases: Vec<PhaseResult>,
    pub elapsed_ms: u128,
    pub assertions: Vec<serde_json::Value>,
    pub overall_result: Option<OverallResult>,
}

impl PruningReport {
    pub fn total_pruning_pct(&self) -> f64 {
        let final_count = self
            .phases
            .last()
            .map(|p| p.output_count)
            .unwrap_or(self.total_files);
        if self.total_files == 0 {
            return 0.0;
        }
        let dropped = self.total_files.saturating_sub(final_count);
        (dropped as f64 / self.total_files as f64) * 100.0
    }

    pub fn stats_coverage(&self) -> (usize, usize) {
        let with_stats = self
            .all_files
            .iter()
            .filter(|f| self.file_stats.contains_key(&f.path))
            .count();
        (with_stats, self.total_files)
    }

    // ── Text output ─────────────────────────────────────────────────

    pub fn print_text(&self, verbose: bool, predicate: Option<&str>) {
        println!("Delta table: {}", self.table_path);
        println!("Version:     {}", self.version);
        if let Some(pred) = predicate {
            println!("Predicate:   {pred}");
        }

        if let Some(analysis) = &self.analysis {
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
        println!("Files in snapshot: {}", fmt(self.total_files));

        if self.phases.is_empty() {
            return;
        }

        for (i, phase) in self.phases.iter().enumerate() {
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
                self.print_phase_details(phase, i);
            }
        }

        // Summary
        if self.phases.len() > 1 {
            let final_count = self.phases.last().unwrap().output_count;
            println!();
            println!(
                "Total reduction: {} -> {} files ({:.0}% pruned)",
                fmt(self.total_files),
                fmt(final_count),
                self.total_pruning_pct(),
            );
        }

        if let Some(analysis) = &self.analysis
            && !analysis.notes.is_empty()
        {
            println!();
            println!("Warnings!");
            for note in &analysis.notes {
                println!("[{}]: {}", note.code, note.message);
            }
        }
    }

    fn print_phase_details(&self, phase: &PhaseResult, phase_idx: usize) {
        let candidates: HashSet<&str> = if phase_idx == 0 {
            self.all_files.iter().map(|f| f.path.as_str()).collect()
        } else {
            self.phases[phase_idx - 1]
                .surviving_paths
                .iter()
                .map(|s| s.as_str())
                .collect()
        };

        println!();
        for file in &self.all_files {
            if !candidates.contains(file.path.as_str()) {
                continue;
            }

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

            let stats_str = match self.file_stats.get(&file.path) {
                Some(stats) => stats.format_compact(),
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
                    self.file_stats
                        .get(&file.path)
                        .and_then(|s| s.num_records)
                })
                .map(|n| format!("  {n} records"))
                .unwrap_or_default();

            println!("  [{tag}] {short_path}  ({size_str}{records_str}){partition_str}{stats_str}");
        }
    }

    // ── JSON output ─────────────────────────────────────────────────

    pub fn print_json(&self, predicate: Option<&str>) {
        let (stats_present, stats_total) = self.stats_coverage();
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

        let phases: Vec<serde_json::Value> = self
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

        let analysis_block = self.analysis.as_ref().map(|analysis| {
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

        let result_value = match self.overall_result {
            Some(r) => json!(r.to_string()),
            None => serde_json::Value::Null,
        };

        let output = json!({
            "schema_version": SCHEMA_VERSION,
            "tool_version": env!("CARGO_PKG_VERSION"),
            "elapsed_ms": self.elapsed_ms,
            "table": self.table_path,
            "version": self.version,
            "predicate": predicate,
            "total_files": self.total_files,
            "final_files": self.phases.last().map(|p| p.output_count).unwrap_or(self.total_files),
            "total_pruning_pct": self.total_pruning_pct(),
            "analysis": analysis_block,
            "stats": {
                "mode": stats_mode,
                "files_with_stats": stats_present,
                "total_files": stats_total,
                "pct": stats_pct,
            },
            "phases": phases,
            "assertions": self.assertions,
            "result": result_value,
        });

        println!("{}", serde_json::to_string_pretty(&output).unwrap());
    }
}

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
