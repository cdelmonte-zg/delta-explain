use std::collections::{BTreeMap, BTreeSet};

use super::model::{
    ColumnStatsCoverage, PartitionAnalysis, PredicateClassification, StatsRequirement,
};
use super::predicate::{Literal, Pred};
use crate::metadata::scan::BaselineScan;
use crate::metadata::stats::FileStats;
use delta_kernel::schema::{DataType, MetadataValue, Schema, StructField, StructType};

const PHYSICAL_NAME_KEY: &str = "delta.columnMapping.physicalName";

type RequirementsByColumns = BTreeMap<Vec<String>, BTreeSet<StatsRequirement>>;

pub(crate) fn compute(
    classification: &PredicateClassification,
    partition: &PartitionAnalysis,
    baseline: &BaselineScan,
    schema: &Schema,
) -> Option<Vec<ColumnStatsCoverage>> {
    let mut requirements = BTreeMap::new();

    for fragment in &classification.stats_safe {
        collect_stats_requirements(fragment, &mut requirements);
    }

    let is_candidate = |path: &str| {
        partition
            .survivors
            .as_ref()
            .is_none_or(|survivors| survivors.contains(path))
    };

    // we take also the files without stats
    let candidate_files = baseline
        .files
        .iter()
        .filter(|file| is_candidate(&file.path))
        .count();

    let mut coverage = Vec::new();

    for (logical_path, requirements) in requirements {
        let logical_column = logical_path.join(".");
        let physical_column = resolve_physical_path(schema, logical_path.as_slice())?;

        for requirement in requirements {
            let covered_files = baseline
                .stats
                .iter()
                .filter(|entry| {
                    let (path, file_stats) = *entry;

                    is_candidate(path.as_str())
                        && has_required_stats(file_stats, &physical_column, requirement)
                })
                .count();

            coverage.push(ColumnStatsCoverage {
                column: logical_column.clone(),
                requirement,
                candidate_files,
                covered_files,
            });
        }
    }

    Some(coverage)
}

fn collect_stats_requirements(predicate: &Pred, requirements: &mut RequirementsByColumns) {
    match predicate {
        Pred::And(parts) | Pred::Or(parts) => {
            for part in parts {
                collect_stats_requirements(part, requirements);
            }
        }

        Pred::Not(inner) => {
            collect_stats_requirements(inner, requirements);
        }

        Pred::Cmp { col, .. } | Pred::In { col, .. } | Pred::Between { col, .. } => {
            requirements
                .entry(col.0.clone())
                .or_default()
                .insert(StatsRequirement::MinMax);
        }

        Pred::IsNull { col, negated } => {
            let requirement = if *negated {
                StatsRequirement::NullCountAndNumRecords
            } else {
                StatsRequirement::NullCount
            };

            requirements
                .entry(col.0.clone())
                .or_default()
                .insert(requirement);
        }

        Pred::Distinct { col, lit, negated } => {
            let column_requirements = requirements.entry(col.0.clone()).or_default();

            match lit {
                Literal::Null => {
                    let requirement = if *negated {
                        StatsRequirement::NullCount
                    } else {
                        StatsRequirement::NullCountAndNumRecords
                    };

                    column_requirements.insert(requirement);
                }

                _ => {
                    column_requirements.insert(StatsRequirement::MinMax);

                    let null_requirement = if *negated {
                        StatsRequirement::NullCountAndNumRecords
                    } else {
                        StatsRequirement::NullCount
                    };

                    column_requirements.insert(null_requirement);
                }
            }
        }

        Pred::BoolCol(col) => {
            requirements
                .entry(col.0.clone())
                .or_default()
                .insert(StatsRequirement::MinMax);
        }

        Pred::Like { .. } | Pred::Unsupported { .. } => {}
    }
}

fn has_required_stats(file_stats: &FileStats, column: &str, requirement: StatsRequirement) -> bool {
    let Some(column_stats) = file_stats.columns.get(column) else {
        return false;
    };

    match requirement {
        StatsRequirement::MinMax => column_stats.min.is_some() && column_stats.max.is_some(),

        StatsRequirement::NullCount => column_stats.null_count.is_some(),

        StatsRequirement::NullCountAndNumRecords => {
            column_stats.null_count.is_some() && file_stats.num_records.is_some()
        }
    }
}

fn resolve_physical_path(schema: &StructType, logical_path: &[String]) -> Option<String> {
    if logical_path.is_empty() {
        return None;
    }

    let mut current_struct = schema;
    let mut physical_parts: Vec<String> = Vec::with_capacity(logical_path.len());

    for (index, logical_name) in logical_path.iter().enumerate() {
        let logical_field = find_logical_field(current_struct, logical_name)?;

        let has_next_part = index + 1 < logical_path.len();

        // if s.a.x, then a should be a struct, if not there is an anomaly in the logical path
        // and we return Option<None>
        if has_next_part {
            current_struct = match &logical_field.data_type {
                DataType::Struct(nested) => nested.as_ref(),
                _ => return None,
            };
        }

        physical_parts.push(physical_name(logical_field).to_owned());
    }

    Some(physical_parts.join("."))
}

fn find_logical_field<'a>(schema: &'a StructType, logical_name: &str) -> Option<&'a StructField> {
    schema.fields().find(|field| field.name == logical_name)
}

fn physical_name(field: &StructField) -> &str {
    match field.metadata.get(PHYSICAL_NAME_KEY) {
        Some(MetadataValue::String(name)) => name.as_str(),
        _ => field.name.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;

    use crate::analysis::model::{
        ColumnStatsCoverage, PartitionAnalysis, PredicateClassification, StatsRequirement,
    };
    use crate::analysis::predicate;
    use crate::metadata::scan::{BaselineScan, FileInfo};
    use crate::metadata::stats::{ColumnStats, FileStats};
    use rstest::{fixture, rstest};

    fn mapped_field(logical_name: &str, physical_name: &str, data_type: DataType) -> StructField {
        StructField::new(logical_name, data_type, true).with_metadata([(
            PHYSICAL_NAME_KEY.to_string(),
            MetadataValue::String(physical_name.to_string()),
        )])
    }

    #[fixture]
    fn mapped_schema() -> StructType {
        let nested = StructType::try_new(vec![mapped_field("a", "physical_a", DataType::INTEGER)])
            .expect("nested test schema should be valid");

        StructType::try_new(vec![
            mapped_field("age", "physical_age", DataType::INTEGER),
            StructField::new("plain", DataType::STRING, true),
            mapped_field("s", "physical_s", DataType::Struct(Box::new(nested))),
        ])
        .expect("test schema should be valid")
    }

    #[rstest]
    #[case::mapped_top_level(
        &["age".to_string()],
        Some("physical_age")
    )]
    #[case::unmapped_top_level(
        &["plain".to_string()],
        Some("plain")
    )]
    #[case::mapped_nested(
        &["s".to_string(), "a".to_string()],
        Some("physical_s.physical_a")
    )]
    #[case::unknown_field(
        &["missing".to_string()],
        None
    )]
    #[case::primitive_used_as_prefix(
        &["age".to_string(), "something".to_string()],
        None
    )]
    #[case::empty_path(
        &[],
        None
    )]
    fn resolves_physical_paths(
        mapped_schema: StructType,
        #[case] logical_path: &[String],
        #[case] expected: Option<&str>,
    ) {
        assert_eq!(
            resolve_physical_path(&mapped_schema, logical_path).as_deref(),
            expected
        );
    }

    #[rstest]
    fn compute_uses_physical_path_and_reports_logical_column(mapped_schema: StructType) {
        let baseline = baseline_with_age_column("physical_age");
        let partition = no_partition_pruning();

        let actual = compute(
            &classification("age > 30"),
            &partition,
            &baseline,
            &mapped_schema,
        );

        assert_eq!(
            actual,
            Some(vec![expected_coverage(
                "age",
                StatsRequirement::MinMax,
                3,
                1,
            )]),
        );
    }

    #[test]
    fn returns_none_when_a_column_path_cannot_be_resolved() {
        let baseline = baseline();
        let partition = no_partition_pruning();
        let schema = coverage_schema();

        let actual = compute(
            &classification("missing > 10"),
            &partition,
            &baseline,
            &schema,
        );

        assert_eq!(actual, None);
    }

    #[test]
    fn returns_some_empty_when_there_are_no_stats_requirements() {
        let baseline = baseline();
        let partition = no_partition_pruning();
        let schema = coverage_schema();
        let classification = PredicateClassification::default();

        let actual = compute(&classification, &partition, &baseline, &schema);

        assert_eq!(actual, Some(Vec::new()));
    }

    fn file(path: &str) -> FileInfo {
        FileInfo {
            path: path.to_string(),
            size: 100,
            partition_values: HashMap::new(),
            num_records: None,
            has_deletion_vector: false,
        }
    }

    fn expected_coverage(
        column: &str,
        requirement: StatsRequirement,
        candidate_files: usize,
        covered_files: usize,
    ) -> ColumnStatsCoverage {
        ColumnStatsCoverage {
            column: column.to_string(),
            requirement,
            candidate_files,
            covered_files,
        }
    }

    fn baseline_with_age_column(column_name: &str) -> BaselineScan {
        let stats = HashMap::from([
            (
                "a".to_string(),
                FileStats {
                    num_records: Some(10),
                    columns: HashMap::from([(
                        column_name.to_string(),
                        ColumnStats {
                            min: Some("10".to_string()),
                            max: Some("20".to_string()),
                            null_count: Some(0),
                        },
                    )]),
                },
            ),
            (
                "b".to_string(),
                FileStats {
                    num_records: None,
                    columns: HashMap::from([(
                        column_name.to_string(),
                        ColumnStats {
                            min: None,
                            max: None,
                            null_count: Some(10),
                        },
                    )]),
                },
            ),
        ]);

        BaselineScan {
            files: vec![file("a"), file("b"), file("c")],
            stats,
        }
    }

    fn baseline() -> BaselineScan {
        baseline_with_age_column("age")
    }

    fn classification(sql: &str) -> PredicateClassification {
        PredicateClassification {
            stats_safe: vec![predicate::parse(sql).unwrap()],
            ..Default::default()
        }
    }

    fn no_partition_pruning() -> PartitionAnalysis {
        PartitionAnalysis {
            survivors: None,
            evaluation_gaps: 0,
        }
    }

    fn coverage_schema() -> StructType {
        StructType::try_new(vec![
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("tail", DataType::INTEGER, true),
            StructField::new("active", DataType::BOOLEAN, true),
        ])
        .expect("coverage test schema should be valid")
    }

    #[test]
    fn coverage_depends_on_the_required_stat_kind() {
        let baseline = baseline();
        let partition = no_partition_pruning();

        let cases = [
            (
                "age > 30",
                vec![expected_coverage("age", StatsRequirement::MinMax, 3, 1)],
            ),
            (
                "age IS NULL",
                vec![expected_coverage("age", StatsRequirement::NullCount, 3, 2)],
            ),
            (
                "age IS NOT NULL",
                vec![expected_coverage(
                    "age",
                    StatsRequirement::NullCountAndNumRecords,
                    3,
                    1,
                )],
            ),
            (
                "tail > 500",
                vec![expected_coverage("tail", StatsRequirement::MinMax, 3, 0)],
            ),
            (
                "age IS DISTINCT FROM NULL",
                vec![expected_coverage(
                    "age",
                    StatsRequirement::NullCountAndNumRecords,
                    3,
                    1,
                )],
            ),
            (
                "age IS NOT DISTINCT FROM NULL",
                vec![expected_coverage("age", StatsRequirement::NullCount, 3, 2)],
            ),
            (
                "age IS DISTINCT FROM 30",
                vec![
                    expected_coverage("age", StatsRequirement::MinMax, 3, 1),
                    expected_coverage("age", StatsRequirement::NullCount, 3, 2),
                ],
            ),
            (
                "age IS NOT DISTINCT FROM 30",
                vec![
                    expected_coverage("age", StatsRequirement::MinMax, 3, 1),
                    expected_coverage("age", StatsRequirement::NullCountAndNumRecords, 3, 1),
                ],
            ),
            (
                "active",
                vec![expected_coverage("active", StatsRequirement::MinMax, 3, 0)],
            ),
            (
                "age > 10 AND age < 50",
                vec![expected_coverage("age", StatsRequirement::MinMax, 3, 1)],
            ),
        ];

        let schema = coverage_schema();

        for (sql, expected) in cases {
            let actual = compute(&classification(sql), &partition, &baseline, &schema);

            assert_eq!(actual, Some(expected), "unexpected coverage for {sql}",);
        }
    }

    #[test]
    fn partition_survivors_define_the_candidate_set() {
        let baseline = baseline();

        let partition = PartitionAnalysis {
            survivors: Some(HashSet::from(["a".to_string(), "c".to_string()])),
            evaluation_gaps: 0,
        };

        let actual = compute(
            &classification("age > 30"),
            &partition,
            &baseline,
            &coverage_schema(),
        );

        assert_eq!(
            actual,
            Some(vec![expected_coverage(
                "age",
                StatsRequirement::MinMax,
                2,
                1,
            )]),
        );
    }
}
