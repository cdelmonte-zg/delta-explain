use crate::analysis::model::{PredicateClassification, UnsplittableFragment, UnsplittableHandling};
use crate::analysis::predicate::Pred;

/// Classify the top-level conjuncts of a normalized predicate.
///
/// Each conjunct is routed to exactly one bucket:
///
/// - partition-safe
/// - partition-exact
/// - stats-safe
/// - unsplittable
///
/// This function performs structural classification only. It does not
/// execute scans, inspect statistics coverage, emit diagnostics, or render
/// fragments.
pub fn classify(predicate: &Pred, partition_columns: &[String]) -> PredicateClassification {
    let mut result = PredicateClassification::default();

    for clause in predicate.conjuncts() {
        classify_clause(clause, partition_columns, &mut result);
    }

    result
}

fn classify_clause(
    clause: &Pred,
    partition_columns: &[String],
    result: &mut PredicateClassification,
) {
    let referenced_columns = clause.columns();

    let any_partition = referenced_columns
        .iter()
        .any(|column| partition_columns.contains(column));

    let all_partitions = !referenced_columns.is_empty()
        && referenced_columns
            .iter()
            .all(|column| partition_columns.contains(column));

    if clause.contains_unsupported() {
        classify_unsupported(clause, all_partitions, result);

        return;
    }

    if all_partitions {
        result.partition_safe.push(clause.clone());

        return;
    }

    if !any_partition {
        result.stats_safe.push(clause.clone());

        return;
    }

    // The clause spans partition and non-partition columns but is fully
    // understood by the pruning language.
    //
    // It cannot be attributed safely to either phase, but the final kernel
    // scan can still evaluate it.
    result.unsplittable.push(UnsplittableFragment {
        predicate: clause.clone(),
        handling: UnsplittableHandling::Scanned,
    });
}

fn classify_unsupported(clause: &Pred, all_partitions: bool, result: &mut PredicateClassification) {
    // An expression may be unsupported by the general kernel lowering while
    // still having completely known semantics.
    //
    // If all referenced columns are partition columns, such a fragment can
    // be evaluated exactly against literal partition values.
    //
    // LIKE patterns that survive normalization are the main example.
    if all_partitions && !clause.contains_opaque() {
        result.partition_exact.push(clause.clone());

        return;
    }

    // Otherwise the fragment cannot safely contribute to pruning.
    result.unsplittable.push(UnsplittableFragment {
        predicate: clause.clone(),
        handling: UnsplittableHandling::Stripped,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::analysis::model::{Confidence, UnsplittableHandling};
    use crate::analysis::predicate;

    fn partitions(columns: &[&str]) -> Vec<String> {
        columns.iter().map(|column| column.to_string()).collect()
    }

    fn classified(input: &str, partition_columns: &[&str]) -> PredicateClassification {
        let predicate = predicate::parse(input).unwrap().normalized();

        classify(&predicate, &partitions(partition_columns))
    }

    #[test]
    fn partition_only_is_partition_safe() {
        let result = classified("country = 'IT'", &["country"]);

        assert_eq!(result.partition_safe.len(), 1);
        assert!(result.partition_exact.is_empty());
        assert!(result.stats_safe.is_empty());
        assert!(result.unsplittable.is_empty());

        assert_eq!(result.partition_safe[0].to_string(), "country = 'IT'");

        assert_eq!(result.confidence_ceiling(), Confidence::Exact);
    }

    #[test]
    fn data_only_is_stats_safe() {
        let result = classified("price > 50", &["country"]);

        assert!(result.partition_safe.is_empty());
        assert!(result.partition_exact.is_empty());

        assert_eq!(result.stats_safe.len(), 1);

        assert!(result.unsplittable.is_empty());

        assert_eq!(result.stats_safe[0].to_string(), "price > 50");

        assert_eq!(result.confidence_ceiling(), Confidence::Conservative);
    }

    #[test]
    fn partition_and_stats_split() {
        let result = classified("country = 'IT' AND price > 50", &["country"]);

        assert_eq!(result.partition_safe.len(), 1);

        assert_eq!(result.stats_safe.len(), 1);

        assert!(result.unsplittable.is_empty());

        assert_eq!(result.partition_safe[0].to_string(), "country = 'IT'");

        assert_eq!(result.stats_safe[0].to_string(), "price > 50");
    }

    #[test]
    fn mixed_axis_or_is_scanned_unsplittable() {
        let result = classified("country = 'IT' OR price > 50", &["country"]);

        assert!(result.partition_safe.is_empty());
        assert!(result.stats_safe.is_empty());

        assert_eq!(result.unsplittable.len(), 1);

        assert_eq!(
            result.unsplittable[0].handling,
            UnsplittableHandling::Scanned
        );

        assert_eq!(result.confidence_ceiling(), Confidence::Incomplete);

        assert_eq!(result.stripped_count(), 0);
    }

    #[test]
    fn unsupported_expression_is_stripped() {
        let result = classified("country = 'IT' AND UPPER(name) = 'X'", &["country"]);

        assert_eq!(result.partition_safe.len(), 1);

        assert_eq!(result.unsplittable.len(), 1);

        assert_eq!(
            result.unsplittable[0].handling,
            UnsplittableHandling::Stripped
        );

        assert_eq!(result.stripped_count(), 1);

        assert_eq!(result.confidence_ceiling(), Confidence::Incomplete);
    }

    #[test]
    fn non_prefix_like_on_partition_column_is_partition_exact() {
        let result = classified("country LIKE '%E'", &["country"]);

        assert!(result.partition_safe.is_empty());

        assert_eq!(result.partition_exact.len(), 1);

        assert!(result.unsplittable.is_empty());

        assert_eq!(result.partition_exact[0].to_string(), "country LIKE '%E'");

        assert_eq!(result.confidence_ceiling(), Confidence::Exact);
    }

    #[test]
    fn prefix_like_on_partition_column_is_partition_safe() {
        let result = classified("country LIKE 'D%'", &["country"]);

        assert_eq!(result.partition_safe.len(), 2);

        assert_eq!(result.partition_safe[0].to_string(), "country >= 'D'");

        assert_eq!(result.partition_safe[1].to_string(), "country < 'E'");

        assert!(result.partition_exact.is_empty());
        assert!(result.stats_safe.is_empty());
        assert!(result.unsplittable.is_empty());

        assert_eq!(result.confidence_ceiling(), Confidence::Exact);
    }

    #[test]
    fn factored_partition_clause_is_visible() {
        let result = classified(
            "(country = 'DE' AND age > 20) \
                 OR \
                 (country = 'DE' AND age < 60)",
            &["country"],
        );

        assert_eq!(result.partition_safe.len(), 1);

        assert_eq!(result.partition_safe[0].to_string(), "country = 'DE'");

        assert_eq!(result.stats_safe.len(), 1);

        assert_eq!(result.stats_safe[0].to_string(), "age > 20 OR age < 60");
    }
}
