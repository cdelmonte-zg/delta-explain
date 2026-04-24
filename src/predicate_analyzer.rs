use sqlparser::ast::{BinaryOperator, Expr as SqlExpr};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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

pub fn analyze(input: &str, partition_columns: &[String]) -> Result<PredicateAnalysis, String> {
    // 1. Parse sql
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(input)
        .map_err(|e| format!("Parse error: {e}"))?;
    let sql_expr = parser
        .parse_expr()
        .map_err(|e| format!("Parse error: {e}"))?;

    let mut partition_frags: Vec<String> = Vec::new();
    let mut stats_frags: Vec<String> = Vec::new();
    let mut unsplittable_frags: Vec<String> = Vec::new();
    let mut notes: Vec<AnalysisNote> = Vec::new();

    for clause in flatten_and(&sql_expr) {
        let refs = collect_column_refs(clause);
        let frag = clause.to_string();

        let any_partition = refs.iter().any(|r| partition_columns.contains(r));

        let all_partitions = !refs.is_empty() && refs.iter().all(|r| partition_columns.contains(r));

        if all_partitions {
            partition_frags.push(frag);
        } else if !any_partition {
            stats_frags.push(frag);
        } else {
            unsplittable_frags.push(frag);

            notes.push(AnalysisNote {
                code: "UNSPLITTABLE_OR".into(),
                message: "Mixed expression across partition and non-partition \
                        columns; cannot separate safely, routed as unsplittable"
                    .into(),
            });
        }
    }

    let partition_safe = join_opt(partition_frags);
    let stats_safe = join_opt(stats_frags);
    let unsplittable = join_opt(unsplittable_frags);

    let confidence = if unsplittable.is_some() {
        Confidence::Incomplete
    } else if stats_safe.is_some() {
        Confidence::Conservative
    } else {
        Confidence::Exact
    };

    Ok(PredicateAnalysis {
        partition_safe,
        stats_safe,
        unsplittable,
        confidence,
        notes,
    })
}

fn join_opt(frags: Vec<String>) -> Option<String> {
    if frags.is_empty() {
        None
    } else {
        Some(frags.join(" AND "))
    }
}

/// Flatten top-level ANDs into a list of clauses.
fn flatten_and(expr: &SqlExpr) -> Vec<&SqlExpr> {
    match expr {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut v = flatten_and(left);
            v.extend(flatten_and(right));
            v
        }
        SqlExpr::Nested(inner) => flatten_and(inner),
        _ => vec![expr],
    }
}

/// Collect all column name references from an expression.
fn collect_column_refs(expr: &SqlExpr) -> Vec<String> {
    let mut refs = Vec::new();
    collect_refs_inner(expr, &mut refs);
    refs
}

fn collect_refs_inner(expr: &SqlExpr, refs: &mut Vec<String>) {
    match expr {
        SqlExpr::Identifier(ident) => refs.push(ident.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => {
            refs.push(
                parts
                    .iter()
                    .map(|p| &p.value)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("."),
            );
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_refs_inner(left, refs);
            collect_refs_inner(right, refs);
        }
        SqlExpr::UnaryOp { expr, .. } => collect_refs_inner(expr, refs),
        SqlExpr::Nested(inner) => collect_refs_inner(inner, refs),
        SqlExpr::IsNull(e) | SqlExpr::IsNotNull(e) => collect_refs_inner(e, refs),
        SqlExpr::InList { expr, list, .. } => {
            collect_refs_inner(expr, refs);
            for item in list {
                collect_refs_inner(item, refs);
            }
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            collect_refs_inner(expr, refs);
            collect_refs_inner(low, refs);
            collect_refs_inner(high, refs);
        }
        _ => {}
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
    fn invalid_sql_returns_error() {
        assert!(analyze("((", &parts(&[])).is_err());
    }
}
