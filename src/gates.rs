//! CI gates: evaluate the assertion flags against a report.
//!
//! Pure: the caller prints `failures` to stderr and maps `overall` to the
//! process exit code. `--min-pruning` is the hard gate on the total pruning
//! percentage; `--assert-stats` flags files whose `add` action carries no
//! statistics.

use serde_json::json;

use crate::report::{OverallResult, PruningReport};

/// The outcome of evaluating the CI gates: the JSON assertion records for the
/// report, the overall pass/fail (None when no gate was requested), and the
/// stderr lines to emit on failure.
pub struct GateOutcome {
    pub assertions: Vec<serde_json::Value>,
    pub overall: Option<OverallResult>,
    pub failures: Vec<String>,
}

/// Evaluate `--min-pruning` and `--assert-stats` against the report.
pub fn evaluate(
    report: &PruningReport,
    min_pruning: Option<f64>,
    assert_stats: bool,
) -> GateOutcome {
    let mut assertions = Vec::new();
    let mut failures = Vec::new();
    let mut any_assertion = false;
    let mut all_pass = true;

    if let Some(threshold) = min_pruning {
        any_assertion = true;
        let actual = report.total_pruning_pct();
        let pass = actual >= threshold;
        all_pass &= pass;
        if !pass {
            failures.push(format!(
                "ASSERTION FAILED: total pruning {actual:.1}% is below threshold {threshold:.1}%"
            ));
        }
        assertions.push(json!({
            "name": "min_pruning",
            "threshold": threshold,
            "actual": actual,
            "result": if pass { "pass" } else { "fail" },
        }));
    }

    if assert_stats {
        any_assertion = true;
        let missing: Vec<&str> = report
            .all_files
            .iter()
            .filter(|f| !report.file_stats.contains_key(&f.path))
            .map(|f| f.path.as_str())
            .collect();
        let pass = missing.is_empty();
        all_pass &= pass;
        if !pass {
            let mut msg = format!(
                "ASSERTION FAILED: {} file(s) missing statistics:",
                missing.len()
            );
            for path in &missing {
                msg.push_str(&format!("\n  {path}"));
            }
            failures.push(msg);
        }
        assertions.push(json!({
            "name": "stats_complete",
            "missing_count": missing.len(),
            "result": if pass { "pass" } else { "fail" },
        }));
    }

    let overall = if any_assertion {
        Some(if all_pass {
            OverallResult::Pass
        } else {
            OverallResult::Fail
        })
    } else {
        None
    };

    GateOutcome {
        assertions,
        overall,
        failures,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::predicate_analyzer::Confidence;
    use crate::report::{FileInfo, PhaseResult, PruningReport};
    use crate::stats::FileStats;

    /// A 4-file report where the last phase keeps 1 file (75% pruned) and
    /// only files "a" and "b" have stats.
    fn report() -> PruningReport {
        let files: Vec<FileInfo> = ["a", "b", "c", "d"]
            .iter()
            .map(|p| FileInfo {
                path: p.to_string(),
                size: 1,
                partition_values: HashMap::new(),
                num_records: None,
                has_deletion_vector: false,
            })
            .collect();
        let mut file_stats = HashMap::new();
        for p in ["a", "b"] {
            file_stats.insert(
                p.to_string(),
                FileStats {
                    num_records: Some(1),
                    columns: HashMap::new(),
                },
            );
        }
        PruningReport {
            analysis: None,
            table_features: Default::default(),
            table_path: "t".into(),
            version: 0,
            total_files: files.len(),
            all_files: files,
            file_stats,
            phases: vec![PhaseResult {
                confidence: Confidence::Conservative,
                name: "Data skipping (min/max statistics)".into(),
                predicate_display: "x > 1".into(),
                input_count: 4,
                output_count: 1,
                surviving_paths: ["a".to_string()].into_iter().collect(),
            }],
            elapsed_ms: 0,
            assertions: Vec::new(),
            overall_result: None,
        }
    }

    #[test]
    fn no_flags_means_no_overall_result() {
        let outcome = evaluate(&report(), None, false);
        assert!(outcome.overall.is_none());
        assert!(outcome.assertions.is_empty());
        assert!(outcome.failures.is_empty());
    }

    #[test]
    fn min_pruning_passes_below_actual_and_fails_above() {
        let pass = evaluate(&report(), Some(60.0), false);
        assert_eq!(pass.overall, Some(OverallResult::Pass));
        assert!(pass.failures.is_empty());

        let fail = evaluate(&report(), Some(90.0), false);
        assert_eq!(fail.overall, Some(OverallResult::Fail));
        assert!(fail.failures[0].contains("below threshold 90.0%"));
        assert_eq!(fail.assertions[0]["result"], "fail");
    }

    #[test]
    fn assert_stats_flags_files_without_stats() {
        let outcome = evaluate(&report(), None, true);
        assert_eq!(outcome.overall, Some(OverallResult::Fail));
        let msg = &outcome.failures[0];
        assert!(msg.contains("2 file(s) missing statistics"));
        assert!(msg.contains("\n  c") && msg.contains("\n  d"));
        assert_eq!(outcome.assertions[0]["missing_count"], 2);
    }

    #[test]
    fn one_failing_gate_fails_the_overall_result() {
        let outcome = evaluate(&report(), Some(60.0), true);
        assert_eq!(outcome.overall, Some(OverallResult::Fail));
        assert_eq!(outcome.assertions.len(), 2);
        assert_eq!(outcome.assertions[0]["result"], "pass");
        assert_eq!(outcome.assertions[1]["result"], "fail");
    }
}
