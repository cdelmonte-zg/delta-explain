mod kernel;
pub mod model;
mod partition_eval;
mod partition_pruning;
pub mod predicate;
pub mod predicate_analyzer;
mod value_coercion;

use delta_kernel::{Engine, schema::SchemaRef};

use crate::v2::error::Result;
use crate::v2::table::TableState;

use self::model::{AnalysisResult, PredicateClassification};

/// Run predicate analysis against an opened Delta table.
///
/// This is the orchestration boundary for analysis:
///
/// SQL
///   -> parse
///   -> schema-aware normalization
///   -> static classification
///   -> partition-safe kernel pruning
///
/// Additional analysis phases can be added here without exposing their
/// sequencing to the CLI.
pub fn analyze(input: &str, table: &TableState, engine: &dyn Engine) -> Result<AnalysisResult> {
    let schema = table.snapshot.schema();

    let classification = classify_predicate(input, &table.metadata.partition_columns, &schema)?;

    let partition = partition_pruning::prune(
        &classification,
        &table.metadata.baseline.files,
        table.snapshot.clone(),
        engine,
        &schema,
    )?;

    Ok(AnalysisResult {
        classification,
        partition,
    })
}

/// Parse, normalize, and classify a predicate without executing any scans.
///
/// Kept private because callers should normally use `analyze`; this helper
/// exists to keep the static phase independently testable.
fn classify_predicate(
    input: &str,
    partition_columns: &[String],
    schema: &SchemaRef,
) -> Result<PredicateClassification> {
    let predicate =
        predicate::parse(input)?.normalized_with(|col| kernel::column_is_string(col, schema));

    Ok(predicate_analyzer::classify(&predicate, partition_columns))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};

    use super::*;
    use crate::v2::analysis::model::Confidence;

    fn partitions(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }

    fn test_schema() -> SchemaRef {
        Arc::new(
            StructType::try_new([
                StructField::nullable("country", DataType::STRING),
                StructField::nullable("age", DataType::INTEGER),
                StructField::nullable("name", DataType::STRING),
            ])
            .unwrap(),
        )
    }

    #[test]
    fn analyzes_partition_and_stats_predicate() {
        let schema = test_schema();

        let result = classify_predicate(
            "country = 'DE' AND age > 30",
            &partitions(&["country"]),
            &schema,
        )
        .unwrap();

        assert_eq!(result.partition_safe.len(), 1);

        assert_eq!(result.partition_safe[0].to_string(), "country = 'DE'");

        assert_eq!(result.stats_safe.len(), 1);

        assert_eq!(result.stats_safe[0].to_string(), "age > 30");

        assert!(result.partition_exact.is_empty());

        assert!(result.unsplittable.is_empty());

        assert_eq!(result.confidence_ceiling(), Confidence::Conservative);
    }

    #[test]
    fn normalization_happens_before_classification() {
        let schema = test_schema();

        let result = classify_predicate(
            "(country = 'DE' AND age > 20) \
                 OR (country = 'DE' AND age < 60)",
            &partitions(&["country"]),
            &schema,
        )
        .unwrap();

        assert_eq!(result.partition_safe.len(), 1);

        assert_eq!(result.partition_safe[0].to_string(), "country = 'DE'");

        assert_eq!(result.stats_safe.len(), 1);

        assert_eq!(result.stats_safe[0].to_string(), "age > 20 OR age < 60");

        assert!(result.unsplittable.is_empty());

        assert_eq!(result.confidence_ceiling(), Confidence::Conservative);
    }

    #[test]
    fn prefix_like_on_string_column_is_rewritten_before_classification() {
        let schema = test_schema();

        let result =
            classify_predicate("country LIKE 'DE%'", &partitions(&["country"]), &schema).unwrap();

        assert_eq!(result.partition_safe.len(), 2);

        assert_eq!(result.partition_safe[0].to_string(), "country >= 'DE'");

        assert_eq!(result.partition_safe[1].to_string(), "country < 'DF'");

        assert!(result.partition_exact.is_empty());

        assert!(result.stats_safe.is_empty());

        assert!(result.unsplittable.is_empty());

        assert_eq!(result.confidence_ceiling(), Confidence::Exact);
    }

    #[test]
    fn prefix_like_on_non_string_column_is_not_rewritten() {
        let schema = test_schema();

        let result =
            classify_predicate("age LIKE '3%'", &partitions(&["country"]), &schema).unwrap();

        assert!(result.partition_safe.is_empty());

        assert!(result.partition_exact.is_empty());

        assert!(result.stats_safe.is_empty());

        assert_eq!(result.unsplittable.len(), 1);

        assert_eq!(
            result.unsplittable[0].predicate.to_string(),
            "age LIKE '3%'"
        );

        assert_eq!(result.confidence_ceiling(), Confidence::Incomplete);
    }

    #[test]
    fn non_prefix_like_on_partition_column_is_partition_exact() {
        let schema = test_schema();

        let result =
            classify_predicate("country LIKE '%E'", &partitions(&["country"]), &schema).unwrap();

        assert!(result.partition_safe.is_empty());

        assert_eq!(result.partition_exact.len(), 1);

        assert_eq!(result.partition_exact[0].to_string(), "country LIKE '%E'");

        assert!(result.stats_safe.is_empty());

        assert!(result.unsplittable.is_empty());

        assert_eq!(result.confidence_ceiling(), Confidence::Exact);
    }
}
