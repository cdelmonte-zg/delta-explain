use crate::v2::analysis::model::{AnalysisResult, UnsplittableHandling};
use crate::v2::analysis::predicate::Pred;
use crate::v2::metadata::features::TableFeatures;

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

    DeletionVectors {
        files_with_deletion_vectors: usize,
        total_files: usize,
    },

    ColumnMapping {
        mode: String,
    },

    LiquidClustering {
        columns: Vec<String>,
    },

    UnrecognizedTableFeature {
        features: Vec<String>,
    },
}

impl Warning {
    pub fn code(&self) -> &'static str {
        match self {
            Warning::UnsupportedExpression { .. } => "UNSUPPORTED_EXPRESSION",

            Warning::UnsplittableOr { .. } => "UNSPLITTABLE_OR",

            Warning::PartitionEvaluationGap { .. } => "PARTITION_EVALUATION_GAP",

            Warning::DeletionVectors { .. } => "DELETION_VECTORS",

            Warning::ColumnMapping { .. } => "COLUMN_MAPPING",

            Warning::LiquidClustering { .. } => "LIQUID_CLUSTERING",

            Warning::UnrecognizedTableFeature { .. } => "UNRECOGNIZED_TABLE_FEATURE",
        }
    }
}

pub struct WarningContext<'a> {
    pub analysis: Option<&'a AnalysisResult>,
    pub features: &'a TableFeatures,
    pub total_files: usize,
}

pub fn derive(context: WarningContext<'_>) -> Vec<Warning> {
    let mut warnings = Vec::new();

    if let Some(analysis) = context.analysis {
        derive_analysis_warnings(analysis, &mut warnings);
    }

    derive_table_warnings(context.features, context.total_files, &mut warnings);

    warnings
}

fn derive_analysis_warnings(analysis: &AnalysisResult, warnings: &mut Vec<Warning>) {
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
}

fn derive_table_warnings(
    features: &TableFeatures,
    total_files: usize,
    warnings: &mut Vec<Warning>,
) {
    if features.files_with_deletion_vectors > 0 {
        warnings.push(Warning::DeletionVectors {
            files_with_deletion_vectors: features.files_with_deletion_vectors,

            total_files,
        });
    }

    if let Some(mode) = &features.column_mapping_mode {
        warnings.push(Warning::ColumnMapping { mode: mode.clone() });
    }

    if let Some(columns) = &features.clustering_columns {
        warnings.push(Warning::LiquidClustering {
            columns: columns.clone(),
        });
    }

    if !features.unrecognized_writer_features.is_empty() {
        warnings.push(Warning::UnrecognizedTableFeature {
            features: features.unrecognized_writer_features.clone(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::analysis::model::{
        PartitionAnalysis, PredicateClassification, ScanAnalysis, UnsplittableFragment,
    };
    use crate::v2::analysis::predicate;

    fn features() -> TableFeatures {
        TableFeatures {
            deletion_vectors_enabled: false,
            files_with_deletion_vectors: 0,
            column_mapping_mode: None,
            clustering_columns: None,
            in_commit_timestamps: false,
            unrecognized_writer_features: Vec::new(),
        }
    }

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
    fn stripped_fragment_becomes_unsupported_warning() {
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

        let table_features = features();

        let warnings = derive(WarningContext {
            analysis: Some(&result),
            features: &table_features,
            total_files: 6,
        });

        assert_eq!(warnings[0].code(), "UNSUPPORTED_EXPRESSION");
    }

    #[test]
    fn table_features_become_warnings() {
        let table_features = TableFeatures {
            deletion_vectors_enabled: true,
            files_with_deletion_vectors: 2,
            column_mapping_mode: Some("name".into()),
            clustering_columns: Some(vec!["age".into()]),
            in_commit_timestamps: true,
            unrecognized_writer_features: vec!["futureFeature".into()],
        };

        let warnings = derive(WarningContext {
            analysis: None,
            features: &table_features,
            total_files: 6,
        });

        let codes = warnings.iter().map(Warning::code).collect::<Vec<_>>();

        assert_eq!(
            codes,
            vec![
                "DELETION_VECTORS",
                "COLUMN_MAPPING",
                "LIQUID_CLUSTERING",
                "UNRECOGNIZED_TABLE_FEATURE",
            ]
        );
    }

    #[test]
    fn enabled_but_unused_deletion_vectors_stay_silent() {
        let mut table_features = features();

        table_features.deletion_vectors_enabled = true;

        let warnings = derive(WarningContext {
            analysis: None,
            features: &table_features,
            total_files: 6,
        });

        assert!(warnings.is_empty());
    }

    #[test]
    fn in_commit_timestamps_stay_silent() {
        let mut table_features = features();

        table_features.in_commit_timestamps = true;

        let warnings = derive(WarningContext {
            analysis: None,
            features: &table_features,
            total_files: 6,
        });

        assert!(warnings.is_empty());
    }
}
