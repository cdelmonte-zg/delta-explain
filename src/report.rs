//! The computed pruning model: what the analysis produced, independent of
//! how it is shown. Rendering (text and JSON) lives in [`crate::render`].

use std::collections::{HashMap, HashSet};

use crate::features::TableFeatures;
use crate::predicate_analyzer::{Confidence, PredicateAnalysis};
use crate::stats::FileStats;

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
    pub has_deletion_vector: bool,
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
    pub table_features: TableFeatures,
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

    /// The first phase that dropped this file, or None if it survives the
    /// whole chain. Phases are chained (each one's survivors feed the next),
    /// so the first phase whose survivor set misses the path is the one
    /// that eliminated it.
    pub fn pruned_by(&self, path: &str) -> Option<&PhaseResult> {
        self.phases
            .iter()
            .find(|phase| !phase.surviving_paths.contains(path))
    }

    pub fn stats_coverage(&self) -> (usize, usize) {
        let with_stats = self
            .all_files
            .iter()
            .filter(|f| self.file_stats.contains_key(&f.path))
            .count();
        (with_stats, self.total_files)
    }
}
