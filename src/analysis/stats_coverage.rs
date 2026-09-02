use std::collections::{BTreeMap, BTreeSet};

use super::model::{
    ColumnStatsCoverage, PartitionAnalysis, PredicateClassification, StatsRequirement,
};
use super::predicate::{Literal, Pred};
use crate::metadata::scan::BaselineScan;
use crate::metadata::stats::FileStats;

pub(crate) fn derive(
    classification: &PredicateClassification,
    partition: &PartitionAnalysis,
    baseline: &BaselineScan,
) -> Vec<ColumnStatsCoverage> {
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

    for (column, requirements) in requirements {
        for requirement in requirements {
            let covered_files = baseline
                .stats
                .iter()
                .filter(|entry| {
                    let (path, file_stats) = *entry;

                    is_candidate(path.as_str())
                        && has_required_stats(file_stats, &column, requirement)
                })
                .count();

            coverage.push(ColumnStatsCoverage {
                column: column.clone(),
                requirement,
                candidate_files,
                covered_files,
            });
        }
    }

    coverage
}

fn collect_stats_requirements(
    predicate: &Pred,
    requirements: &mut BTreeMap<String, BTreeSet<StatsRequirement>>,
) {
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
                .entry(col.dotted())
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
                .entry(col.dotted())
                .or_default()
                .insert(requirement);
        }

        Pred::Distinct { col, lit, negated } => {
            let column_requirements = requirements.entry(col.dotted()).or_default();

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
                .entry(col.dotted())
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

    fn baseline() -> BaselineScan {
        let stats = HashMap::from([
            (
                "a".to_string(),
                FileStats {
                    num_records: Some(10),
                    columns: HashMap::from([(
                        "age".to_string(),
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
                        "age".to_string(),
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

        for (sql, expected) in cases {
            let actual = derive(&classification(sql), &partition, &baseline);

            assert_eq!(actual, expected, "unexpected coverage for {sql}",);
        }
    }

    #[test]
    fn partition_survivors_define_the_candidate_set() {
        let baseline = baseline();

        let partition = PartitionAnalysis {
            survivors: Some(HashSet::from(["a".to_string(), "c".to_string()])),
            evaluation_gaps: 0,
        };

        let actual = derive(&classification("age > 30"), &partition, &baseline);

        assert_eq!(
            actual,
            vec![expected_coverage("age", StatsRequirement::MinMax, 2, 1,)],
        );
    }
}
