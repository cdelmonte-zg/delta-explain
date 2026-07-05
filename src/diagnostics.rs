//! `--explain-why`: a deterministic diagnostic engine over the computed
//! report. See ADR 0007. Every diagnosis is a function of data already in
//! hand (classification, stats coverage, partition columns, per-phase
//! pruning); nothing is predicted. Codes are stable once shipped.

use crate::report::PruningReport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warning,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnosis {
    pub code: String,
    pub severity: Severity,
    pub message: String,
    pub suggestion: Option<String>,
}

impl Diagnosis {
    fn new(code: &str, severity: Severity, message: String, suggestion: Option<String>) -> Self {
        Diagnosis {
            code: code.into(),
            severity,
            message,
            suggestion,
        }
    }
}

/// Diagnose why the predicate pruned as much (or as little) as it did.
/// `predicate_columns` are the dotted names the predicate touches, from the
/// normalized AST; empty when there was no `--where`.
pub fn diagnose(
    report: &PruningReport,
    partition_columns: &[String],
    predicate_columns: &[String],
) -> Vec<Diagnosis> {
    let Some(analysis) = &report.analysis else {
        return Vec::new(); // no predicate: nothing predicate-specific to say
    };
    let mut out = Vec::new();

    // The table is partitioned, but the predicate touches no partition
    // column: directory-level pruning cannot run at all.
    let touches_partition = predicate_columns
        .iter()
        .any(|c| partition_columns.contains(c));
    if !partition_columns.is_empty() && !predicate_columns.is_empty() && !touches_partition {
        out.push(Diagnosis::new(
            "NO_PARTITION_FILTER",
            Severity::Warning,
            format!(
                "The table is partitioned by {}, but the predicate filters on none \
                 of those columns, so partition pruning cannot run.",
                partition_columns.join(", ")
            ),
            Some(format!(
                "Filter on a partition column ({}) to eliminate whole directories \
                 before data skipping.",
                partition_columns.join(", ")
            )),
        ));
    }

    // Data-skipping diagnostics apply only when a stats-safe fragment
    // actually reached the min/max phase.
    if let Some(stats_frag) = &analysis.stats_safe {
        let (present, total) = report.stats_coverage();
        let stats_absent = total == 0 || present == 0;

        if stats_absent {
            out.push(Diagnosis::new(
                "STATS_ABSENT",
                Severity::Warning,
                format!(
                    "The table carries no file statistics, so data skipping cannot \
                     prune on '{stats_frag}'."
                ),
                Some(
                    "Have the writer record statistics (delta.dataSkippingNumIndexedCols \
                     covers the columns you filter on)."
                        .into(),
                ),
            ));
        } else if let Some(phase) = report
            .phases
            .iter()
            .find(|p| p.name.contains("Data skipping"))
            && phase.input_count > 0
            && phase.input_count == phase.output_count
        {
            // Stats exist and the phase ran, but it removed nothing: every
            // file's min/max range overlaps the bound (unordered column).
            out.push(Diagnosis::new(
                "WEAK_DATA_SKIPPING",
                Severity::Warning,
                format!(
                    "Data skipping eliminated no files for '{stats_frag}': every file's \
                     min/max range overlaps the predicate's bound."
                ),
                Some(
                    "The data is not ordered by that column. Sort or cluster the table \
                     by it so each file covers a narrow range."
                        .into(),
                ),
            ));
        }
    }

    // A fragment outside the pruning language kept all files; reframe the
    // classification note as advice.
    if let Some(unsplittable) = &analysis.unsplittable {
        out.push(Diagnosis::new(
            "UNSUPPORTED_FRAGMENT",
            Severity::Info,
            format!(
                "The fragment '{unsplittable}' is outside the pruning language and was \
                 applied conservatively, keeping all files."
            ),
            Some(
                "A mixed partition/data OR cannot be separated; rewrite it so the \
                 partition and data filters are independent conjuncts. Function calls, \
                 arithmetic and subqueries cannot prune at all."
                    .into(),
            ),
        ));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attribution::build_phases;
    use crate::predicate_analyzer::analyze;
    use crate::report::{FileInfo, PruningReport};
    use crate::stats::FileStats;
    use std::collections::{HashMap, HashSet};

    fn file(path: &str) -> FileInfo {
        FileInfo {
            path: path.into(),
            size: 1,
            partition_values: HashMap::new(),
            num_records: None,
            has_deletion_vector: false,
        }
    }

    fn report_with(
        predicate: &str,
        partition_columns: &[&str],
        files: Vec<FileInfo>,
        file_stats: HashMap<String, FileStats>,
        partition_survivors: Option<HashSet<String>>,
        full_survivors: Option<HashSet<String>>,
    ) -> (PruningReport, Vec<String>, Vec<String>) {
        let parts: Vec<String> = partition_columns.iter().map(|s| s.to_string()).collect();
        let pred = crate::predicate_ast::parse(predicate).unwrap().normalized();
        let analysis = analyze(predicate, &parts).unwrap();
        let total = files.len();
        let phases = build_phases(&analysis, total, partition_survivors, full_survivors);
        let report = PruningReport {
            analysis: Some(analysis),
            table_features: Default::default(),
            table_path: "t".into(),
            version: 0,
            total_files: total,
            all_files: files,
            file_stats,
            phases,
            explain: Vec::new(),
            elapsed_ms: 0,
            assertions: Vec::new(),
            overall_result: None,
        };
        (report, parts, pred.columns())
    }

    fn stats_map(paths: &[&str]) -> HashMap<String, FileStats> {
        paths
            .iter()
            .map(|p| {
                (
                    p.to_string(),
                    FileStats {
                        num_records: Some(1),
                        columns: HashMap::new(),
                    },
                )
            })
            .collect()
    }

    fn codes(d: &[Diagnosis]) -> Vec<&str> {
        d.iter().map(|x| x.code.as_str()).collect()
    }

    #[test]
    fn no_partition_filter_when_predicate_misses_partition_columns() {
        // partitioned by country, predicate only on age -> no partition pruning.
        let (r, parts, cols) = report_with(
            "age > 40",
            &["country"],
            vec![file("a"), file("b")],
            stats_map(&["a", "b"]),
            None,
            Some(["a".into(), "b".into()].into_iter().collect()),
        );
        let d = diagnose(&r, &parts, &cols);
        assert!(codes(&d).contains(&"NO_PARTITION_FILTER"));
    }

    #[test]
    fn no_such_diagnosis_when_predicate_hits_a_partition_column() {
        let (r, parts, cols) = report_with(
            "country = 'DE'",
            &["country"],
            vec![file("a"), file("b")],
            stats_map(&["a", "b"]),
            Some(["a".into()].into_iter().collect()),
            None,
        );
        let d = diagnose(&r, &parts, &cols);
        assert!(!codes(&d).contains(&"NO_PARTITION_FILTER"));
    }

    #[test]
    fn weak_data_skipping_when_stats_present_but_nothing_pruned() {
        // stats on every file, data-skipping phase keeps all of them.
        let (r, parts, cols) = report_with(
            "age > 40",
            &[],
            vec![file("a"), file("b"), file("c")],
            stats_map(&["a", "b", "c"]),
            None,
            Some(["a".into(), "b".into(), "c".into()].into_iter().collect()),
        );
        let d = diagnose(&r, &parts, &cols);
        assert!(codes(&d).contains(&"WEAK_DATA_SKIPPING"));
    }

    #[test]
    fn stats_absent_when_no_file_has_stats() {
        let (r, parts, cols) = report_with(
            "age > 40",
            &[],
            vec![file("a"), file("b")],
            HashMap::new(),
            None,
            Some(["a".into(), "b".into()].into_iter().collect()),
        );
        let d = diagnose(&r, &parts, &cols);
        assert!(codes(&d).contains(&"STATS_ABSENT"));
        assert!(!codes(&d).contains(&"WEAK_DATA_SKIPPING"));
    }

    #[test]
    fn effective_data_skipping_raises_no_diagnosis() {
        // stats present and the phase pruned (3 -> 1): nothing to complain about.
        let (r, parts, cols) = report_with(
            "age > 40",
            &[],
            vec![file("a"), file("b"), file("c")],
            stats_map(&["a", "b", "c"]),
            None,
            Some(["a".into()].into_iter().collect()),
        );
        let d = diagnose(&r, &parts, &cols);
        assert!(d.is_empty());
    }

    #[test]
    fn unsupported_fragment_becomes_advice() {
        let (r, parts, cols) = report_with(
            "country = 'DE' OR age > 40",
            &["country"],
            vec![file("a"), file("b")],
            stats_map(&["a", "b"]),
            None,
            Some(["a".into(), "b".into()].into_iter().collect()),
        );
        let d = diagnose(&r, &parts, &cols);
        assert!(codes(&d).contains(&"UNSUPPORTED_FRAGMENT"));
    }

    #[test]
    fn no_predicate_yields_no_diagnoses() {
        let report = PruningReport {
            analysis: None,
            table_features: Default::default(),
            table_path: "t".into(),
            version: 0,
            total_files: 0,
            all_files: Vec::new(),
            file_stats: HashMap::new(),
            phases: Vec::new(),
            explain: Vec::new(),
            elapsed_ms: 0,
            assertions: Vec::new(),
            overall_result: None,
        };
        assert!(diagnose(&report, &[], &[]).is_empty());
    }
}
