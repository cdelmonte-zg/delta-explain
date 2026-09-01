use crate::v2::analysis::model::{AnalysisResult, UnsplittableHandling};
use crate::v2::analysis::predicate::Pred;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Warning {
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

impl Warning {
    pub fn code(&self) -> &'static str {
        match self {
            Warning::UnsupportedExpression { .. } => "UNSUPPORTED_EXPRESSION",

            Warning::UnsplittableOr { .. } => "UNSPLITTABLE_OR",

            Warning::PartitionEvaluationGap { .. } => "PARTITION_EVALUATION_GAP",
        }
    }
}

pub fn derive(analysis: &AnalysisResult) -> Vec<Warning> {
    let mut warnings = Vec::new();

    for fragment in &analysis.classification.unsplittable {
        match fragment.handling {
            UnsplittableHandling::Stripped => {
                warnings.push(Warning::UnsupportedExpression {
                    predicate: fragment.predicate.clone(),

                    reasons: fragment
                        .predicate
                        .unsupported_reasons()
                        .into_iter()
                        .map(str::to_string)
                        .collect(),
                });
            }

            UnsplittableHandling::Scanned => {
                warnings.push(Warning::UnsplittableOr {
                    predicate: fragment.predicate.clone(),
                });
            }
        }
    }

    if analysis.partition.evaluation_gaps > 0 {
        warnings.push(Warning::PartitionEvaluationGap {
            count: analysis.partition.evaluation_gaps,
        });
    }

    warnings
}
