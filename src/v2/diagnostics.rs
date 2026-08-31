use crate::v2::analysis::model::{AnalysisResult, UnsplittableHandling};
use crate::v2::analysis::predicate::Pred;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Diagnostic {
    UnsupportedExpression {
        predicate: Pred,
        reasons: Vec<String>,
    },

    UnsplittableOr {
        predicate: Pred,
    },

    PartitionEvaluationGap {
        count: usize,
    },
}

impl Diagnostic {
    pub fn code(&self) -> &'static str {
        match self {
            Diagnostic::UnsupportedExpression { .. } => "UNSUPPORTED_EXPRESSION",

            Diagnostic::UnsplittableOr { .. } => "UNSPLITTABLE_OR",

            Diagnostic::PartitionEvaluationGap { .. } => "PARTITION_EVALUATION_GAP",
        }
    }
}

/// Derive diagnostics from analysis evidence.
///
/// This function does not affect pruning, confidence, or attribution.
/// It only interprets facts already produced by the analysis pipeline.
pub fn derive(analysis: &AnalysisResult) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    for fragment in &analysis.classification.unsplittable {
        match fragment.handling {
            UnsplittableHandling::Stripped => {
                let reasons = fragment
                    .predicate
                    .unsupported_reasons()
                    .into_iter()
                    .map(str::to_string)
                    .collect();

                diagnostics.push(Diagnostic::UnsupportedExpression {
                    predicate: fragment.predicate.clone(),

                    reasons,
                });
            }

            UnsplittableHandling::Scanned => {
                diagnostics.push(Diagnostic::UnsplittableOr {
                    predicate: fragment.predicate.clone(),
                });
            }
        }
    }

    if analysis.partition.evaluation_gaps > 0 {
        diagnostics.push(Diagnostic::PartitionEvaluationGap {
            count: analysis.partition.evaluation_gaps,
        });
    }

    diagnostics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::analysis::model::{
        PartitionAnalysis, PredicateClassification, ScanAnalysis, UnsplittableFragment,
    };
    use crate::v2::analysis::predicate;

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
    fn stripped_fragment_becomes_unsupported_diagnostic() {
        let predicate = predicate::parse("UPPER(name) = 'X'").unwrap();

        let result = analysis(
            PredicateClassification {
                unsplittable: vec![UnsplittableFragment {
                    predicate,
                    handling: UnsplittableHandling::Stripped,
                }],
                ..Default::default()
            },
            0,
        );

        let diagnostics = derive(&result);

        assert_eq!(diagnostics.len(), 1);

        match &diagnostics[0] {
            Diagnostic::UnsupportedExpression { predicate, reasons } => {
                assert_eq!(predicate.to_string(), "UPPER(name) = 'X'");

                assert!(!reasons.is_empty());
            }

            other => {
                panic!("unexpected diagnostic: {other:?}");
            }
        }
    }

    #[test]
    fn scanned_mixed_fragment_becomes_unsplittable_diagnostic() {
        let predicate = predicate::parse("country = 'DE' OR age > 30").unwrap();

        let result = analysis(
            PredicateClassification {
                unsplittable: vec![UnsplittableFragment {
                    predicate,
                    handling: UnsplittableHandling::Scanned,
                }],
                ..Default::default()
            },
            0,
        );

        let diagnostics = derive(&result);

        assert_eq!(diagnostics.len(), 1);

        assert_eq!(diagnostics[0].code(), "UNSPLITTABLE_OR");
    }

    #[test]
    fn partition_evaluation_gap_becomes_diagnostic() {
        let result = analysis(PredicateClassification::default(), 2);

        let diagnostics = derive(&result);

        assert_eq!(
            diagnostics,
            vec![Diagnostic::PartitionEvaluationGap { count: 2 },]
        );
    }

    #[test]
    fn clean_analysis_has_no_diagnostics() {
        let result = analysis(PredicateClassification::default(), 0);

        assert!(derive(&result).is_empty());
    }
}
