use std::collections::HashSet;

use crate::v2::analysis::model::{
    PhaseAnalysis, PhaseKind, PredicateClassification, UnsplittableHandling,
};
use crate::v2::analysis::predicate::Pred;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Explanation {
    NoPartitionFilter {
        partition_columns: Vec<String>,
    },

    StatsAbsent {
        predicate: Pred,
    },

    WeakDataSkipping {
        predicate: Pred,
    },

    UnsupportedFragment {
        predicate: Pred,
        handling: UnsplittableHandling,
    },
}

impl Explanation {
    pub fn code(&self) -> &'static str {
        match self {
            Explanation::NoPartitionFilter { .. } => "NO_PARTITION_FILTER",

            Explanation::StatsAbsent { .. } => "STATS_ABSENT",

            Explanation::WeakDataSkipping { .. } => "WEAK_DATA_SKIPPING",

            Explanation::UnsupportedFragment { .. } => "UNSUPPORTED_FRAGMENT",
        }
    }
}

/// Facts needed to explain pruning effectiveness.
///
/// This deliberately does not depend on `Report`: diagnostics stays
/// downstream of analysis but independent of report assembly.
pub struct ExplainContext<'a> {
    pub classification: &'a PredicateClassification,
    pub phases: &'a [PhaseAnalysis],
    pub partition_columns: &'a [String],
    pub total_files: usize,
    pub files_with_stats: usize,
}

pub fn derive(context: ExplainContext<'_>) -> Vec<Explanation> {
    let mut explanations = Vec::new();

    derive_partition_filter(&context, &mut explanations);

    derive_stats_explanations(&context, &mut explanations);

    derive_unsupported_fragments(&context, &mut explanations);

    explanations
}

fn derive_partition_filter(context: &ExplainContext<'_>, explanations: &mut Vec<Explanation>) {
    if context.partition_columns.is_empty() {
        return;
    }

    let predicate_columns = predicate_columns(context.classification);

    // If no represented column is known, we cannot prove that
    // the predicate misses the partition columns. This can happen
    // for opaque unsupported expressions.
    if predicate_columns.is_empty() {
        return;
    }

    let touches_partition = predicate_columns
        .iter()
        .any(|column| context.partition_columns.contains(column));

    if !touches_partition {
        explanations.push(Explanation::NoPartitionFilter {
            partition_columns: context.partition_columns.to_vec(),
        });
    }
}

fn derive_stats_explanations(context: &ExplainContext<'_>, explanations: &mut Vec<Explanation>) {
    let Some(stats_predicate) = conjunction(&context.classification.stats_safe) else {
        return;
    };

    // Empty tables have no pruning problem to diagnose.
    if context.total_files == 0 {
        return;
    }

    if context.files_with_stats == 0 {
        explanations.push(Explanation::StatsAbsent {
            predicate: stats_predicate,
        });

        return;
    }

    // Preserve v1 semantics for now:
    // WEAK_DATA_SKIPPING is only claimed when every file has
    // statistics. Partial coverage is ambiguous because files may
    // survive simply because their statistics are missing.
    if context.files_with_stats != context.total_files {
        return;
    }

    let data_skipping = context
        .phases
        .iter()
        .find(|phase| phase.kind == PhaseKind::DataSkipping);

    let Some(phase) = data_skipping else {
        return;
    };

    if phase.input_count > 0 && phase.input_count == phase.output_count {
        explanations.push(Explanation::WeakDataSkipping {
            predicate: stats_predicate,
        });
    }
}

fn derive_unsupported_fragments(context: &ExplainContext<'_>, explanations: &mut Vec<Explanation>) {
    for fragment in &context.classification.unsplittable {
        explanations.push(Explanation::UnsupportedFragment {
            predicate: fragment.predicate.clone(),
            handling: fragment.handling,
        });
    }
}

fn predicate_columns(classification: &PredicateClassification) -> HashSet<String> {
    classification
        .partition_safe
        .iter()
        .chain(classification.partition_exact.iter())
        .chain(classification.stats_safe.iter())
        .flat_map(Pred::columns)
        .chain(
            classification
                .unsplittable
                .iter()
                .flat_map(|fragment| fragment.predicate.columns()),
        )
        .collect()
}

fn conjunction(predicates: &[Pred]) -> Option<Pred> {
    match predicates {
        [] => None,

        [single] => Some(single.clone()),

        many => Some(Pred::And(many.to_vec())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::v2::analysis::model::{Confidence, PhaseAnalysis, PredicateClassification};
    use crate::v2::analysis::predicate;

    fn phase(kind: PhaseKind, input: usize, output: usize) -> PhaseAnalysis {
        PhaseAnalysis {
            kind,
            confidence: Confidence::Conservative,
            input_count: input,
            output_count: output,
            surviving_paths: HashSet::new(),
        }
    }

    #[test]
    fn missing_partition_filter_is_explained() {
        let classification = PredicateClassification {
            stats_safe: vec![predicate::parse("age > 30").unwrap()],
            ..Default::default()
        };

        let partition_columns = vec!["country".to_string()];

        let phases = vec![phase(PhaseKind::DataSkipping, 6, 4)];

        let explanations = derive(ExplainContext {
            classification: &classification,
            phases: &phases,
            partition_columns: &partition_columns,
            total_files: 6,
            files_with_stats: 6,
        });

        assert!(
            explanations
                .iter()
                .any(|explanation| { explanation.code() == "NO_PARTITION_FILTER" })
        );
    }

    #[test]
    fn partition_filter_suppresses_missing_filter_explanation() {
        let classification = PredicateClassification {
            partition_safe: vec![predicate::parse("country = 'DE'").unwrap()],
            ..Default::default()
        };

        let partition_columns = vec!["country".to_string()];

        let explanations = derive(ExplainContext {
            classification: &classification,
            phases: &[],
            partition_columns: &partition_columns,
            total_files: 6,
            files_with_stats: 6,
        });

        assert!(
            explanations
                .iter()
                .all(|explanation| { explanation.code() != "NO_PARTITION_FILTER" })
        );
    }

    #[test]
    fn absent_stats_are_explained() {
        let classification = PredicateClassification {
            stats_safe: vec![predicate::parse("age > 30").unwrap()],
            ..Default::default()
        };

        let explanations = derive(ExplainContext {
            classification: &classification,
            phases: &[],
            partition_columns: &[],
            total_files: 6,
            files_with_stats: 0,
        });

        assert!(
            explanations
                .iter()
                .any(|explanation| { explanation.code() == "STATS_ABSENT" })
        );
    }

    #[test]
    fn complete_stats_and_zero_pruning_are_explained_as_weak() {
        let classification = PredicateClassification {
            stats_safe: vec![predicate::parse("age > 30").unwrap()],
            ..Default::default()
        };

        let phases = vec![phase(PhaseKind::DataSkipping, 6, 6)];

        let explanations = derive(ExplainContext {
            classification: &classification,
            phases: &phases,
            partition_columns: &[],
            total_files: 6,
            files_with_stats: 6,
        });

        assert!(
            explanations
                .iter()
                .any(|explanation| { explanation.code() == "WEAK_DATA_SKIPPING" })
        );
    }

    #[test]
    fn effective_data_skipping_has_no_weak_explanation() {
        let classification = PredicateClassification {
            stats_safe: vec![predicate::parse("age > 30").unwrap()],
            ..Default::default()
        };

        let phases = vec![phase(PhaseKind::DataSkipping, 6, 2)];

        let explanations = derive(ExplainContext {
            classification: &classification,
            phases: &phases,
            partition_columns: &[],
            total_files: 6,
            files_with_stats: 6,
        });

        assert!(
            explanations
                .iter()
                .all(|explanation| { explanation.code() != "WEAK_DATA_SKIPPING" })
        );
    }

    #[test]
    fn partial_stats_do_not_claim_weak_data_skipping() {
        let classification = PredicateClassification {
            stats_safe: vec![predicate::parse("age > 30").unwrap()],
            ..Default::default()
        };

        let phases = vec![phase(PhaseKind::DataSkipping, 6, 6)];

        let explanations = derive(ExplainContext {
            classification: &classification,
            phases: &phases,
            partition_columns: &[],
            total_files: 6,
            files_with_stats: 3,
        });

        assert!(
            explanations
                .iter()
                .all(|explanation| { explanation.code() != "WEAK_DATA_SKIPPING" })
        );
    }

    #[test]
    fn empty_table_has_no_stats_explanation() {
        let classification = PredicateClassification {
            stats_safe: vec![predicate::parse("age > 30").unwrap()],
            ..Default::default()
        };

        let explanations = derive(ExplainContext {
            classification: &classification,
            phases: &[],
            partition_columns: &[],
            total_files: 0,
            files_with_stats: 0,
        });

        assert!(explanations.iter().all(|explanation| {
            explanation.code() != "STATS_ABSENT" && explanation.code() != "WEAK_DATA_SKIPPING"
        }));
    }

    #[test]
    fn unsplittable_fragment_gets_explanation() {
        use crate::v2::analysis::model::{UnsplittableFragment, UnsplittableHandling};

        let classification = PredicateClassification {
            unsplittable: vec![UnsplittableFragment {
                predicate: predicate::parse("country = 'DE' OR age > 30").unwrap(),

                handling: UnsplittableHandling::Scanned,
            }],

            ..Default::default()
        };

        let explanations = derive(ExplainContext {
            classification: &classification,
            phases: &[],
            partition_columns: &["country".to_string()],
            total_files: 6,
            files_with_stats: 6,
        });

        assert!(
            explanations
                .iter()
                .any(|explanation| { explanation.code() == "UNSUPPORTED_FRAGMENT" })
        );
    }
}
