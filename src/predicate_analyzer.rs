use crate::error::Error;
use crate::predicate_ast::{self, Pred};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence {
    Exact,
    Conservative,
    Incomplete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalysisNote {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicateAnalysis {
    pub partition_safe: Option<String>,
    pub stats_safe: Option<String>,
    pub unsplittable: Option<String>,
    pub confidence: Confidence,
    pub notes: Vec<AnalysisNote>,
}

/// The analysis plus the partition-safe subtree, kept as an AST so the
/// caller can lower it for the partition-only scan without re-parsing
/// the rendered fragment string.
#[derive(Debug, Clone)]
pub struct Classified {
    pub analysis: PredicateAnalysis,
    pub partition_pred: Option<Pred>,
}

pub fn analyze(input: &str, partition_columns: &[String]) -> Result<PredicateAnalysis, Error> {
    let pred = predicate_ast::parse(input)?.normalized();
    Ok(classify(&pred, partition_columns).analysis)
}

/// Split the predicate's top-level conjuncts into the three buckets:
/// partition_safe (prunes at directory level), stats_safe (prunes on
/// min/max file statistics), unsplittable (cannot be attributed to a
/// single phase, routed conservatively).
pub fn classify(pred: &Pred, partition_columns: &[String]) -> Classified {
    let mut partition_frags: Vec<&Pred> = Vec::new();
    let mut stats_frags: Vec<&Pred> = Vec::new();
    let mut unsplittable_frags: Vec<&Pred> = Vec::new();
    let mut notes: Vec<AnalysisNote> = Vec::new();

    for clause in pred.conjuncts() {
        if clause.contains_unsupported() {
            unsplittable_frags.push(clause);
            let reasons = clause.unsupported_reasons().join("; ");
            notes.push(AnalysisNote {
                code: "UNSUPPORTED_EXPRESSION".into(),
                message: format!(
                    "{reasons}; the fragment '{clause}' cannot contribute to \
                     pruning and is applied conservatively (keeps all files)"
                ),
            });
            continue;
        }

        let refs = clause.columns();
        let any_partition = refs.iter().any(|r| partition_columns.contains(r));
        let all_partitions = !refs.is_empty() && refs.iter().all(|r| partition_columns.contains(r));

        if all_partitions {
            partition_frags.push(clause);
        } else if !any_partition {
            stats_frags.push(clause);
        } else {
            unsplittable_frags.push(clause);
            notes.push(AnalysisNote {
                code: "UNSPLITTABLE_OR".into(),
                message: "Mixed expression across partition and non-partition \
                        columns; cannot separate safely, routed as unsplittable"
                    .into(),
            });
        }
    }

    let partition_pred = match partition_frags.as_slice() {
        [] => None,
        [single] => Some((*single).clone()),
        many => Some(Pred::And(many.iter().map(|p| (*p).clone()).collect())),
    };

    let partition_safe = join_opt(&partition_frags);
    let stats_safe = join_opt(&stats_frags);
    let unsplittable = join_opt(&unsplittable_frags);

    let confidence = if unsplittable.is_some() {
        Confidence::Incomplete
    } else if stats_safe.is_some() {
        Confidence::Conservative
    } else {
        Confidence::Exact
    };

    Classified {
        analysis: PredicateAnalysis {
            partition_safe,
            stats_safe,
            unsplittable,
            confidence,
            notes,
        },
        partition_pred,
    }
}

impl std::fmt::Display for Confidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Confidence::Exact => "exact",
            Confidence::Conservative => "conservative",
            Confidence::Incomplete => "incomplete",
        };
        f.write_str(s)
    }
}

fn join_opt(frags: &[&Pred]) -> Option<String> {
    if frags.is_empty() {
        None
    } else {
        Some(
            frags
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(" AND "),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn partition_only_is_exact() {
        let r = analyze("country = 'IT'", &parts(&["country"])).unwrap();

        assert_eq!(r.partition_safe.as_deref(), Some("country = 'IT'"));
        assert_eq!(r.stats_safe, None);
        assert_eq!(r.unsplittable, None);
        assert_eq!(r.confidence, Confidence::Exact);
        assert!(r.notes.is_empty());
    }

    #[test]
    fn stats_only_is_conservative() {
        let r = analyze("price > 50", &parts(&["country"])).unwrap();

        assert_eq!(r.partition_safe, None);
        assert_eq!(r.stats_safe.as_deref(), Some("price > 50"));
        assert_eq!(r.unsplittable, None);
        assert_eq!(r.confidence, Confidence::Conservative);
        assert!(r.notes.is_empty());
    }

    #[test]
    fn partition_and_stats_splits_into_both_buckets() {
        let r = analyze("country = 'IT' AND price > 50", &parts(&["country"])).unwrap();

        assert_eq!(r.partition_safe.as_deref(), Some("country = 'IT'"));
        assert_eq!(r.stats_safe.as_deref(), Some("price > 50"));
        assert_eq!(r.unsplittable, None);
        assert_eq!(r.confidence, Confidence::Conservative);
        assert!(r.notes.is_empty());
    }

    #[test]
    fn mixed_or_is_unsplittable_and_incomplete() {
        let r = analyze("country = 'IT' OR price > 50", &parts(&["country"])).unwrap();

        assert_eq!(r.partition_safe, None);
        assert_eq!(r.stats_safe, None);
        assert!(r.unsplittable.is_some());
        assert_eq!(r.confidence, Confidence::Incomplete);
        assert_eq!(r.notes.len(), 1);
        assert_eq!(r.notes[0].code, "UNSPLITTABLE_OR");
    }

    #[test]
    fn composite_predicate_populates_all_three_buckets() {
        let r = analyze(
            "country = 'IT' AND (country = 'DE' OR price > 50) AND price < 100",
            &parts(&["country"]),
        )
        .unwrap();

        assert_eq!(r.partition_safe.as_deref(), Some("country = 'IT'"));
        assert_eq!(r.stats_safe.as_deref(), Some("price < 100"));
        assert!(r.unsplittable.is_some());
        assert_eq!(r.confidence, Confidence::Incomplete);
        assert_eq!(r.notes.len(), 1);
        assert_eq!(r.notes[0].code, "UNSPLITTABLE_OR");
    }

    #[test]
    fn unsupported_fragment_routes_unsplittable_with_note() {
        let r = analyze("country = 'IT' AND UPPER(name) = 'X'", &parts(&["country"])).unwrap();

        assert_eq!(r.partition_safe.as_deref(), Some("country = 'IT'"));
        assert_eq!(r.stats_safe, None);
        assert_eq!(r.unsplittable.as_deref(), Some("UPPER(name) = 'X'"));
        assert_eq!(r.confidence, Confidence::Incomplete);
        assert_eq!(r.notes.len(), 1);
        assert_eq!(r.notes[0].code, "UNSUPPORTED_EXPRESSION");
    }

    #[test]
    fn multiple_partition_fragments_keep_an_emittable_subtree() {
        let pred = predicate_ast::parse("country = 'IT' AND region = 'EU' AND price > 5").unwrap();
        let c = classify(&pred, &parts(&["country", "region"]));

        assert_eq!(
            c.analysis.partition_safe.as_deref(),
            Some("country = 'IT' AND region = 'EU'")
        );
        let partition_pred = c.partition_pred.expect("partition subtree");
        assert_eq!(
            partition_pred.to_string(),
            "country = 'IT' AND region = 'EU'"
        );
    }

    #[test]
    fn prefix_like_on_a_partition_column_classifies_exact() {
        let r = analyze("country LIKE 'D%'", &parts(&["country"])).unwrap();

        assert_eq!(
            r.partition_safe.as_deref(),
            Some("country >= 'D' AND country < 'E'")
        );
        assert_eq!(r.stats_safe, None);
        assert_eq!(r.unsplittable, None);
        assert_eq!(r.confidence, Confidence::Exact);
        assert!(r.notes.is_empty());
    }

    #[test]
    fn prefix_like_on_a_data_column_classifies_stats_safe() {
        let r = analyze("name LIKE 'Ha%' AND country = 'IT'", &parts(&["country"])).unwrap();

        assert_eq!(r.partition_safe.as_deref(), Some("country = 'IT'"));
        assert_eq!(
            r.stats_safe.as_deref(),
            Some("name >= 'Ha' AND name < 'Hb'")
        );
        assert_eq!(r.unsplittable, None);
        assert_eq!(r.confidence, Confidence::Conservative);
    }

    #[test]
    fn non_prefix_like_routes_unsplittable_with_note() {
        let r = analyze("name LIKE '%son'", &parts(&["country"])).unwrap();

        assert_eq!(r.unsplittable.as_deref(), Some("name LIKE '%son'"));
        assert_eq!(r.confidence, Confidence::Incomplete);
        assert_eq!(r.notes.len(), 1);
        assert_eq!(r.notes[0].code, "UNSUPPORTED_EXPRESSION");
        assert!(r.notes[0].message.contains("LIKE"));
    }

    #[test]
    fn invalid_sql_returns_error() {
        assert!(analyze("((", &parts(&[])).is_err());
    }
}
