mod attribution;
mod confidence;
mod kernel;
pub mod model;
mod partition_eval;
mod partition_pruning;
pub mod predicate;
pub mod predicate_analyzer;
mod scan_pruning;
pub(crate) mod stats_coverage;
mod value_coercion;

use delta_kernel::{Engine, schema::SchemaRef};

use crate::error::Result;
use crate::instrumentation::Instrumentation;
use crate::table::TableState;

use self::model::{AnalysisResult, Confidence, PhaseAnalysis};
use self::predicate::Pred;

/// Run predicate analysis against an opened Delta table.
///
/// This is the orchestration boundary for analysis:
///
/// SQL
///   -> parse
///   -> schema-aware normalization
///   -> static classification
///   -> partition pruning
///   -> general data-skipping scan
///
/// Instrumentation is emitted only at semantic phase boundaries. Parsing,
/// normalization, classification, and pruning helpers remain independent
/// from any concrete diagnostic output.
pub fn analyze(
    input: &str,
    table: &TableState,
    engine: &dyn Engine,
    instrumentation: &mut dyn Instrumentation,
) -> Result<AnalysisResult> {
    let schema = table.snapshot.schema();

    let (parsed, predicate) = parse_and_normalize(input, &schema)?;

    instrumentation.predicate_parsed(&parsed, &parsed)?;

    instrumentation.predicate_normalized(&predicate, &predicate)?;

    let classification =
        predicate_analyzer::classify(&predicate, &table.metadata.partition_columns);

    instrumentation.classification_completed(&classification)?;

    let partition = partition_pruning::prune(
        &classification,
        &table.metadata.baseline.files,
        table.snapshot.clone(),
        engine,
        &schema,
        instrumentation,
    )?;

    let scan = scan_pruning::prune(
        &classification,
        &partition,
        &table.metadata.baseline.files,
        table.snapshot.clone(),
        engine,
        &schema,
        instrumentation,
    )?;

    instrumentation.survivor_sets_computed(
        table.metadata.baseline.files.len(),
        partition.survivors.as_ref().map(|files| files.len()),
        scan.survivors.as_ref().map(|files| files.len()),
    )?;

    Ok(AnalysisResult {
        classification,
        partition,
        scan,
    })
}

/// Parse and schema-normalize the predicate while retaining both forms.
///
/// Keeping this as a separate pure helper lets the orchestration layer emit
/// instrumentation for both representations without making the parser or
/// normalization code aware of instrumentation.
fn parse_and_normalize(input: &str, schema: &SchemaRef) -> Result<(Pred, Pred)> {
    let parsed = predicate::parse(input)?;

    let normalized = parsed
        .clone()
        .normalized_with(|column| kernel::column_is_string(column, schema));

    Ok((parsed, normalized))
}

pub fn confidence(result: &AnalysisResult) -> Confidence {
    confidence::overall(result)
}

pub fn phases(result: &AnalysisResult, total_files: usize) -> Vec<PhaseAnalysis> {
    attribution::build(result, total_files)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};

    use self::model::PredicateClassification;
    use super::*;
    use crate::analysis::model::Confidence;

    fn classify_predicate(
        input: &str,
        partition_columns: &[String],
        schema: &SchemaRef,
    ) -> Result<PredicateClassification> {
        let (_, predicate) = parse_and_normalize(input, schema)?;

        Ok(predicate_analyzer::classify(&predicate, partition_columns))
    }

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
