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
    /// Fragments outside the kernel's language whose columns are all
    /// partition columns and whose semantics are fully known: evaluated
    /// per file against the literal partition values instead of degrading.
    pub partition_exact: Option<String>,
    pub stats_safe: Option<String>,
    pub unsplittable: Option<String>,
    /// The `unsplittable` fragments that still reach the final kernel scan
    /// (mixed-axis but fully lowerable, e.g. a mixed OR): their
    /// attribution is lost, their pruning is not. Feeds the text rendering
    /// of the phase line; not part of the JSON document.
    pub unsplittable_scanned: Option<String>,
    /// How many top-level conjuncts are stripped from every scan (they
    /// contain unsupported subtrees) and thus applied conservatively.
    /// Feeds the text rendering; not part of the JSON document.
    pub stripped_count: usize,
    pub confidence: Confidence,
    pub notes: Vec<AnalysisNote>,
}

/// The analysis plus the classified subtrees, kept as ASTs so the caller
/// can lower the partition-safe one for the partition-only scan and hand
/// the partition-exact one to the evaluator, without re-parsing the
/// rendered fragment strings.
#[derive(Debug, Clone)]
pub struct Classified {
    pub analysis: PredicateAnalysis,
    pub partition_pred: Option<Pred>,
    pub partition_exact_pred: Option<Pred>,
}

pub fn analyze(input: &str, partition_columns: &[String]) -> Result<PredicateAnalysis, Error> {
    let pred = predicate_ast::parse(input)?.normalized();
    Ok(classify(&pred, partition_columns).analysis)
}

/// Split the predicate's top-level conjuncts into the four buckets:
/// partition_safe (prunes at directory level), partition_exact (outside
/// the kernel's language but evaluable against partition literals),
/// stats_safe (prunes on min/max file statistics), unsplittable (cannot
/// be attributed to a single phase, routed conservatively).
pub fn classify(pred: &Pred, partition_columns: &[String]) -> Classified {
    let mut partition_frags: Vec<&Pred> = Vec::new();
    let mut exact_frags: Vec<&Pred> = Vec::new();
    let mut stats_frags: Vec<&Pred> = Vec::new();
    let mut unsplittable_frags: Vec<&Pred> = Vec::new();
    let mut scanned_unsplittable_frags: Vec<&Pred> = Vec::new();
    let mut stripped_count = 0usize;
    let mut notes: Vec<AnalysisNote> = Vec::new();

    for clause in pred.conjuncts() {
        let refs = clause.columns();
        let any_partition = refs.iter().any(|r| partition_columns.contains(r));
        let all_partitions = !refs.is_empty() && refs.iter().all(|r| partition_columns.contains(r));

        if clause.contains_unsupported() {
            // Outside the kernel's language. When the semantics are fully
            // known (no opaque subtree) and every column is a partition
            // column, the fragment is evaluated exactly against the
            // literal partition values; otherwise it degrades.
            if all_partitions && !clause.contains_opaque() {
                exact_frags.push(clause);
                continue;
            }
            unsplittable_frags.push(clause);
            stripped_count += 1;
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

        if all_partitions {
            partition_frags.push(clause);
        } else if !any_partition {
            stats_frags.push(clause);
        } else {
            unsplittable_frags.push(clause);
            // Mixed-axis but fully lowerable: the final scan honors it,
            // only the per-phase attribution is lost.
            scanned_unsplittable_frags.push(clause);
            notes.push(AnalysisNote {
                code: "UNSPLITTABLE_OR".into(),
                message: "Mixed expression across partition and non-partition \
                        columns; cannot separate safely, routed as unsplittable"
                    .into(),
            });
        }
    }

    let subtree = |frags: &[&Pred]| match frags {
        [] => None,
        [single] => Some((*single).clone()),
        many => Some(Pred::And(many.iter().map(|p| (*p).clone()).collect())),
    };
    let partition_pred = subtree(&partition_frags);
    let partition_exact_pred = subtree(&exact_frags);

    let partition_safe = join_opt(&partition_frags);
    let partition_exact = join_opt(&exact_frags);
    let stats_safe = join_opt(&stats_frags);
    let unsplittable = join_opt(&unsplittable_frags);
    let unsplittable_scanned = join_opt(&scanned_unsplittable_frags);

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
            partition_exact,
            stats_safe,
            unsplittable,
            unsplittable_scanned,
            stripped_count,
            confidence,
            notes,
        },
        partition_pred,
        partition_exact_pred,
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
    fn non_prefix_like_on_partition_columns_routes_partition_exact() {
        let r = analyze("country LIKE '%E'", &parts(&["country"])).unwrap();

        assert_eq!(r.partition_exact.as_deref(), Some("country LIKE '%E'"));
        assert_eq!(r.partition_safe, None);
        assert_eq!(r.unsplittable, None);
        assert_eq!(r.confidence, Confidence::Exact);
        assert!(r.notes.is_empty());
    }

    #[test]
    fn partition_exact_splits_alongside_the_other_buckets() {
        let r = analyze(
            "country NOT LIKE 'D%' AND region = 'EU' AND age > 40",
            &parts(&["country", "region"]),
        )
        .unwrap();

        assert_eq!(r.partition_exact.as_deref(), Some("country NOT LIKE 'D%'"));
        assert_eq!(r.partition_safe.as_deref(), Some("region = 'EU'"));
        assert_eq!(r.stats_safe.as_deref(), Some("age > 40"));
        assert_eq!(r.unsplittable, None);
        assert_eq!(r.confidence, Confidence::Conservative);
    }

    // ── Combinatorial classification: OR/NOT across the two axes ────

    #[test]
    fn all_partition_or_mixing_like_and_equality_routes_partition_exact_whole() {
        let r = analyze("country LIKE '%E' OR country = 'IT'", &parts(&["country"])).unwrap();

        assert_eq!(
            r.partition_exact.as_deref(),
            Some("country LIKE '%E' OR country = 'IT'")
        );
        assert_eq!(r.partition_safe, None);
        assert_eq!(r.unsplittable, None);
        assert_eq!(r.confidence, Confidence::Exact);
    }

    #[test]
    fn mixed_axis_or_with_like_stays_unsplittable() {
        let r = analyze("country LIKE '%E' OR age > 40", &parts(&["country"])).unwrap();

        assert_eq!(r.partition_exact, None);
        assert!(r.unsplittable.is_some());
        assert_eq!(r.confidence, Confidence::Incomplete);
    }

    #[test]
    fn not_over_mixed_and_becomes_the_unsplittable_or() {
        let r = analyze("NOT (country LIKE '%E' AND age > 40)", &parts(&["country"])).unwrap();

        assert_eq!(
            r.unsplittable.as_deref(),
            Some("country NOT LIKE '%E' OR age <= 40")
        );
        assert_eq!(r.confidence, Confidence::Incomplete);
    }

    #[test]
    fn not_over_partition_only_junction_splits_across_both_partition_routes() {
        let r = analyze(
            "NOT (country LIKE '%E' OR country = 'IT')",
            &parts(&["country"]),
        )
        .unwrap();

        assert_eq!(r.partition_safe.as_deref(), Some("country <> 'IT'"));
        assert_eq!(r.partition_exact.as_deref(), Some("country NOT LIKE '%E'"));
        assert_eq!(r.unsplittable, None);
        assert_eq!(r.confidence, Confidence::Exact);
    }

    #[test]
    fn opaque_fragments_never_route_partition_exact() {
        let r = analyze("UPPER(country) = 'DE'", &parts(&["country"])).unwrap();

        assert_eq!(r.partition_exact, None);
        assert!(r.unsplittable.is_some());
        assert_eq!(r.confidence, Confidence::Incomplete);
        assert_eq!(r.notes[0].code, "UNSUPPORTED_EXPRESSION");
    }

    #[test]
    fn classify_exposes_the_partition_exact_subtree() {
        let pred = predicate_ast::parse("country LIKE '%E' AND country NOT LIKE 'X%'")
            .unwrap()
            .normalized();
        let c = classify(&pred, &parts(&["country"]));

        let exact = c.partition_exact_pred.expect("exact subtree");
        assert_eq!(
            exact.to_string(),
            "country LIKE '%E' AND country NOT LIKE 'X%'"
        );
        assert!(c.partition_pred.is_none());
    }

    #[test]
    fn invalid_sql_returns_error() {
        assert!(analyze("((", &parts(&[])).is_err());
    }
}
