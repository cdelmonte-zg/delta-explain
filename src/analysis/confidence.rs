use super::model::{AnalysisResult, Confidence};

/// Final confidence after applying runtime evidence to the static
/// classification ceiling.
///
/// Static classification establishes the maximum confidence allowed by the
/// predicate structure. Runtime evidence may only degrade that value.
pub(super) fn overall(analysis: &AnalysisResult) -> Confidence {
    let static_confidence = analysis.classification.confidence_ceiling();

    if analysis.partition.evaluation_gaps > 0 {
        worse(static_confidence, Confidence::Conservative)
    } else {
        static_confidence
    }
}

fn worse(left: Confidence, right: Confidence) -> Confidence {
    use Confidence::*;

    match (left, right) {
        (Incomplete, _) | (_, Incomplete) => Incomplete,

        (Conservative, _) | (_, Conservative) => Conservative,

        (Exact, Exact) => Exact,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::model::{
        PartitionAnalysis, PredicateClassification, ScanAnalysis, UnsplittableFragment,
        UnsplittableHandling,
    };
    use crate::analysis::predicate;

    fn analysis(classification: PredicateClassification, evaluation_gaps: usize) -> AnalysisResult {
        AnalysisResult {
            classification,

            partition: PartitionAnalysis {
                survivors: None,
                evaluation_gaps,
            },

            scan: ScanAnalysis { survivors: None },
        }
    }

    #[test]
    fn exact_static_analysis_remains_exact_without_runtime_gaps() {
        let result = analysis(PredicateClassification::default(), 0);

        assert_eq!(overall(&result), Confidence::Exact);
    }

    #[test]
    fn partition_evaluation_gap_downgrades_exact_to_conservative() {
        let result = analysis(PredicateClassification::default(), 1);

        assert_eq!(overall(&result), Confidence::Conservative);
    }

    #[test]
    fn runtime_gap_does_not_lower_conservative_further() {
        let result = analysis(
            PredicateClassification {
                stats_safe: vec![predicate::parse("age > 30").unwrap()],
                ..Default::default()
            },
            1,
        );

        assert_eq!(overall(&result), Confidence::Conservative);
    }

    #[test]
    fn incomplete_static_analysis_remains_incomplete() {
        let result = analysis(
            PredicateClassification {
                unsplittable: vec![UnsplittableFragment {
                    predicate: predicate::parse("country = 'DE' OR age > 30").unwrap(),

                    handling: UnsplittableHandling::Scanned,
                }],
                ..Default::default()
            },
            1,
        );

        assert_eq!(overall(&result), Confidence::Incomplete);
    }
}
