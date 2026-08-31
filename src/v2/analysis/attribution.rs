use super::model::{AnalysisResult, Confidence, PhaseAnalysis, PhaseKind};

pub(super) fn build(analysis: &AnalysisResult, total_files: usize) -> Vec<PhaseAnalysis> {
    let mut phases = Vec::new();
    let mut input_count = total_files;

    if let Some(survivors) = &analysis.partition.survivors {
        let output_count = survivors.len();

        phases.push(PhaseAnalysis {
            kind: PhaseKind::PartitionPruning,

            // Keep v1 attribution semantics:
            // partition pruning itself is exact.
            //
            // Runtime evaluation gaps affect the
            // overall confidence separately.
            confidence: Confidence::Exact,

            input_count,
            output_count,

            surviving_paths: survivors.clone(),
        });

        input_count = output_count;
    }

    if let Some(survivors) = &analysis.scan.survivors {
        let confidence = if analysis.classification.unsplittable.is_empty() {
            Confidence::Conservative
        } else {
            Confidence::Incomplete
        };

        phases.push(PhaseAnalysis {
            kind: PhaseKind::DataSkipping,

            confidence,

            input_count,

            output_count: survivors.len(),

            surviving_paths: survivors.clone(),
        });
    }

    phases
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::v2::analysis::model::{
        PartitionAnalysis, PredicateClassification, ScanAnalysis, UnsplittableFragment,
        UnsplittableHandling,
    };
    use crate::v2::analysis::predicate;

    fn paths(values: &[&str]) -> HashSet<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn partition_and_scan_are_chained() {
        let analysis = AnalysisResult {
            classification: PredicateClassification {
                stats_safe: vec![predicate::parse("age > 30").unwrap()],
                ..Default::default()
            },

            partition: PartitionAnalysis {
                survivors: Some(paths(&["a", "b"])),
                evaluation_gaps: 0,
            },

            scan: ScanAnalysis {
                survivors: Some(paths(&["a"])),
            },
        };

        let phases = build(&analysis, 6);

        assert_eq!(phases.len(), 2);

        assert_eq!(phases[0].kind, PhaseKind::PartitionPruning);

        assert_eq!((phases[0].input_count, phases[0].output_count,), (6, 2));

        assert_eq!(phases[1].kind, PhaseKind::DataSkipping);

        assert_eq!((phases[1].input_count, phases[1].output_count,), (2, 1));
    }

    #[test]
    fn stats_only_starts_from_baseline() {
        let analysis = AnalysisResult {
            classification: PredicateClassification {
                stats_safe: vec![predicate::parse("age > 30").unwrap()],
                ..Default::default()
            },

            partition: PartitionAnalysis {
                survivors: None,
                evaluation_gaps: 0,
            },

            scan: ScanAnalysis {
                survivors: Some(paths(&["a", "b", "c", "d"])),
            },
        };

        let phases = build(&analysis, 6);

        assert_eq!(phases.len(), 1);

        assert_eq!(phases[0].kind, PhaseKind::DataSkipping);

        assert_eq!((phases[0].input_count, phases[0].output_count,), (6, 4));
    }

    #[test]
    fn pure_partition_builds_one_exact_phase() {
        let analysis = AnalysisResult {
            classification: PredicateClassification::default(),

            partition: PartitionAnalysis {
                survivors: Some(paths(&["a", "b"])),
                evaluation_gaps: 0,
            },

            scan: ScanAnalysis { survivors: None },
        };

        let phases = build(&analysis, 6);

        assert_eq!(phases.len(), 1);

        assert_eq!(phases[0].confidence, Confidence::Exact);

        assert_eq!((phases[0].input_count, phases[0].output_count,), (6, 2));
    }

    #[test]
    fn unsplittable_scan_is_incomplete() {
        let mixed = predicate::parse("country = 'DE' OR age > 30").unwrap();

        let analysis = AnalysisResult {
            classification: PredicateClassification {
                unsplittable: vec![UnsplittableFragment {
                    predicate: mixed,
                    handling: UnsplittableHandling::Scanned,
                }],
                ..Default::default()
            },

            partition: PartitionAnalysis {
                survivors: None,
                evaluation_gaps: 0,
            },

            scan: ScanAnalysis {
                survivors: Some(paths(&["a", "b"])),
            },
        };

        let phases = build(&analysis, 6);

        assert_eq!(phases[0].confidence, Confidence::Incomplete);
    }
}
