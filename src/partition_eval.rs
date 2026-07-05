//! Exact evaluation of predicate fragments against a file's literal
//! partition values: the third interpreter over the owned AST, next to
//! classification (`predicate_analyzer`) and kernel lowering
//! (`kernel_bridge`). See ADR 0006.
//!
//! A partition-only fragment is constant across every row of a file, and a
//! row is selected only when the predicate is TRUE. Evaluation therefore
//! distinguishes four outcomes: `True` keeps the file, `False` and `Null`
//! drop it *exactly* (SQL's NULL excludes the row as surely as FALSE), and
//! `Unknown` - the evaluator's own ignorance, e.g. an unparseable partition
//! value - keeps the file conservatively. Conflating `Null` with `Unknown`
//! would either over-keep (understating exact pruning) or over-drop
//! (breaking the survivor-set guarantee); keeping them apart is what lets
//! the phase stay honest.

use std::cmp::Ordering;
use std::collections::HashMap;

use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{DataType, PrimitiveType, SchemaRef};

use crate::kernel_bridge::{
    parse_date_days, parse_decimal, parse_timestamp_micros, parse_timestamp_ntz_micros,
};
use crate::predicate_ast::{CmpOp, ColRef, Literal, Pred};

/// SQL three-valued truth plus the evaluator's own ignorance.
///
/// `Null` is a *semantic* value (the fragment evaluates to SQL NULL on
/// this file's partition values); `Unknown` means the evaluator could not
/// decide. The distinction is load-bearing: `Null` drops a file exactly,
/// `Unknown` must keep it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truth {
    True,
    False,
    Null,
    Unknown,
}

impl Truth {
    fn from_bool(b: bool) -> Truth {
        if b { Truth::True } else { Truth::False }
    }

    // The junction tables are Kleene's, extended with `Unknown` in the
    // direction that is exact for the keep decision: under AND a `Null`
    // absorbs an `Unknown` (the result is FALSE or NULL either way, and
    // both drop), while under OR the same pair must stay `Unknown` (the
    // result could be TRUE or NULL, which decide differently).
    fn and(self, other: Truth) -> Truth {
        match (self, other) {
            (Truth::False, _) | (_, Truth::False) => Truth::False,
            (Truth::Null, _) | (_, Truth::Null) => Truth::Null,
            (Truth::Unknown, _) | (_, Truth::Unknown) => Truth::Unknown,
            (Truth::True, Truth::True) => Truth::True,
        }
    }

    fn or(self, other: Truth) -> Truth {
        match (self, other) {
            (Truth::True, _) | (_, Truth::True) => Truth::True,
            (Truth::Unknown, _) | (_, Truth::Unknown) => Truth::Unknown,
            (Truth::Null, _) | (_, Truth::Null) => Truth::Null,
            (Truth::False, Truth::False) => Truth::False,
        }
    }

    fn not(self) -> Truth {
        match self {
            Truth::True => Truth::False,
            Truth::False => Truth::True,
            Truth::Null => Truth::Null,
            Truth::Unknown => Truth::Unknown,
        }
    }
}

/// Evaluate a predicate fragment against one file's partition values.
///
/// `values` is the kernel's materialized map: a partition column whose
/// value is NULL is *absent* from the map (the kernel skips null map
/// entries), and an empty string is a genuine empty string.
pub fn eval(pred: &Pred, values: &HashMap<String, String>, schema: &SchemaRef) -> Truth {
    match pred {
        Pred::And(parts) => parts
            .iter()
            .fold(Truth::True, |acc, p| acc.and(eval(p, values, schema))),
        Pred::Or(parts) => parts
            .iter()
            .fold(Truth::False, |acc, p| acc.or(eval(p, values, schema))),
        Pred::Not(inner) => eval(inner, values, schema).not(),
        Pred::Cmp { col, op, lit } => cmp_leaf(col, *op, lit, values, schema),
        Pred::In { col, list, negated } => {
            let t = in_leaf(col, list, values, schema);
            if *negated { t.not() } else { t }
        }
        Pred::Between {
            col,
            low,
            high,
            negated,
        } => {
            let t = cmp_leaf(col, CmpOp::Ge, low, values, schema).and(cmp_leaf(
                col,
                CmpOp::Le,
                high,
                values,
                schema,
            ));
            if *negated { t.not() } else { t }
        }
        Pred::IsNull { col, negated } => {
            // Two-valued: presence in the map is the whole question
            // (IS NOT NULL is true exactly when the key is present).
            Truth::from_bool(values.contains_key(&col.dotted()) == *negated)
        }
        Pred::Distinct { col, lit, negated } => distinct_leaf(col, lit, *negated, values, schema),
        Pred::BoolCol(col) => match values.get(&col.dotted()).map(String::as_str) {
            None => Truth::Null,
            Some("true") => Truth::True,
            Some("false") => Truth::False,
            Some(_) => Truth::Unknown,
        },
        Pred::Like {
            col,
            pattern,
            negated,
        } => {
            let t = match values.get(&col.dotted()) {
                None => Truth::Null,
                Some(text) => Truth::from_bool(like_match(text, pattern)),
            };
            if *negated { t.not() } else { t }
        }
        // Semantics unknown by definition; classification never routes an
        // opaque fragment here, but the interpreter stays total.
        Pred::Unsupported { .. } => Truth::Unknown,
    }
}

/// True when the file must be kept for this fragment: it evaluates to
/// TRUE, or the evaluator cannot decide. FALSE and NULL both drop: the
/// fragment is constant across the file's rows, and a row is selected
/// only when the predicate is TRUE.
pub fn keeps(pred: &Pred, values: &HashMap<String, String>, schema: &SchemaRef) -> bool {
    !matches!(eval(pred, values, schema), Truth::False | Truth::Null)
}

// ── Leaves ──────────────────────────────────────────────────────────

fn cmp_leaf(
    col: &ColRef,
    op: CmpOp,
    lit: &Literal,
    values: &HashMap<String, String>,
    schema: &SchemaRef,
) -> Truth {
    if matches!(lit, Literal::Null) {
        // `col <op> NULL` is NULL for every comparison operator.
        return Truth::Null;
    }
    let Some(raw) = values.get(&col.dotted()) else {
        return Truth::Null;
    };
    let Some(ord) = compare(raw, lit, col, schema) else {
        return Truth::Unknown;
    };
    Truth::from_bool(match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
    })
}

fn in_leaf(
    col: &ColRef,
    list: &[Literal],
    values: &HashMap<String, String>,
    schema: &SchemaRef,
) -> Truth {
    let Some(raw) = values.get(&col.dotted()) else {
        return Truth::Null;
    };
    let mut saw_null = false;
    let mut saw_unknown = false;
    for item in list {
        if matches!(item, Literal::Null) {
            saw_null = true;
            continue;
        }
        match compare(raw, item, col, schema) {
            Some(Ordering::Equal) => return Truth::True,
            Some(_) => {}
            None => saw_unknown = true,
        }
    }
    // No match: an unparseable item could still have been the match, and a
    // NULL item turns the whole IN into NULL (x = NULL is never TRUE).
    if saw_unknown {
        Truth::Unknown
    } else if saw_null {
        Truth::Null
    } else {
        Truth::False
    }
}

fn distinct_leaf(
    col: &ColRef,
    lit: &Literal,
    negated: bool,
    values: &HashMap<String, String>,
    schema: &SchemaRef,
) -> Truth {
    // IS [NOT] DISTINCT FROM is two-valued: NULL is an ordinary value.
    let distinct = match (values.get(&col.dotted()), matches!(lit, Literal::Null)) {
        (None, true) => false,
        (None, false) | (Some(_), true) => true,
        (Some(raw), false) => match compare(raw, lit, col, schema) {
            Some(ord) => ord != Ordering::Equal,
            None => return Truth::Unknown,
        },
    };
    Truth::from_bool(distinct != negated)
}

// ── Typed comparison ────────────────────────────────────────────────

/// A partition value and a literal, both coerced to the column's declared
/// type. Coercion failure on either side means the evaluator cannot rank
/// them and the caller degrades to `Unknown`.
enum Typed {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    /// Days since the epoch (DATE) or microseconds since the epoch
    /// (TIMESTAMP / TIMESTAMP_NTZ); the column type disambiguates and both
    /// sides always coerce through the same one.
    Temporal(i64),
    /// Unscaled decimal digits; both sides scale to the column's type.
    Decimal(i128),
}

fn compare(raw: &str, lit: &Literal, col: &ColRef, schema: &SchemaRef) -> Option<Ordering> {
    let dt = schema.field(col.dotted())?.data_type().clone();
    let a = typed_partition_value(raw, &dt)?;
    let b = typed_literal(lit, &dt)?;
    match (a, b) {
        (Typed::Str(x), Typed::Str(y)) => Some(x.cmp(&y)),
        (Typed::Int(x), Typed::Int(y)) => Some(x.cmp(&y)),
        (Typed::Float(x), Typed::Float(y)) => x.partial_cmp(&y),
        (Typed::Bool(x), Typed::Bool(y)) => Some(x.cmp(&y)),
        (Typed::Temporal(x), Typed::Temporal(y)) => Some(x.cmp(&y)),
        (Typed::Decimal(x), Typed::Decimal(y)) => Some(x.cmp(&y)),
        // Both sides coerce through the same column type; a variant
        // mismatch is a coercion bug surfacing, not a comparable pair.
        _ => None,
    }
}

/// Parse the serialized partition value (Delta stores them as strings in
/// the log) at the column's declared type.
fn typed_partition_value(raw: &str, dt: &DataType) -> Option<Typed> {
    let DataType::Primitive(p) = dt else {
        return None;
    };
    // Exhaustive on purpose, same rationale as kernel_bridge: a new kernel
    // primitive type must be routed here consciously.
    match p {
        PrimitiveType::String => Some(Typed::Str(raw.to_string())),
        PrimitiveType::Integer
        | PrimitiveType::Long
        | PrimitiveType::Short
        | PrimitiveType::Byte => raw.parse::<i64>().ok().map(Typed::Int),
        PrimitiveType::Float | PrimitiveType::Double => raw.parse::<f64>().ok().map(Typed::Float),
        PrimitiveType::Boolean => match raw {
            "true" => Some(Typed::Bool(true)),
            "false" => Some(Typed::Bool(false)),
            _ => None,
        },
        PrimitiveType::Date => parse_date_days(raw).ok().map(|d| Typed::Temporal(d.into())),
        PrimitiveType::Timestamp => parse_timestamp_micros(raw).ok().map(Typed::Temporal),
        PrimitiveType::TimestampNtz => parse_timestamp_ntz_micros(raw).ok().map(Typed::Temporal),
        PrimitiveType::Decimal(d) => match parse_decimal(raw, d) {
            Ok(Scalar::Decimal(v)) => Some(Typed::Decimal(v.bits())),
            _ => None,
        },
        PrimitiveType::Binary => None,
    }
}

/// Coerce the predicate literal at the column's declared type, mirroring
/// the schema-driven coercion the kernel bridge applies at emission.
fn typed_literal(lit: &Literal, dt: &DataType) -> Option<Typed> {
    let DataType::Primitive(p) = dt else {
        return None;
    };
    match (lit, p) {
        (Literal::Str(s), PrimitiveType::String) => Some(Typed::Str(s.clone())),
        (
            Literal::Number(s),
            PrimitiveType::Integer
            | PrimitiveType::Long
            | PrimitiveType::Short
            | PrimitiveType::Byte,
        ) => s.parse::<i64>().ok().map(Typed::Int),
        (Literal::Number(s), PrimitiveType::Float | PrimitiveType::Double) => {
            s.parse::<f64>().ok().map(Typed::Float)
        }
        (Literal::Number(s), PrimitiveType::Decimal(d)) => match parse_decimal(s, d) {
            Ok(Scalar::Decimal(v)) => Some(Typed::Decimal(v.bits())),
            _ => None,
        },
        (Literal::Bool(b), PrimitiveType::Boolean) => Some(Typed::Bool(*b)),
        // A quoted string against a temporal column is a date or timestamp
        // in disguise, exactly as at kernel emission.
        (Literal::Str(s) | Literal::Date(s), PrimitiveType::Date) => {
            parse_date_days(s).ok().map(|d| Typed::Temporal(d.into()))
        }
        (Literal::Str(s) | Literal::Date(s) | Literal::Timestamp(s), PrimitiveType::Timestamp) => {
            parse_timestamp_micros(s).ok().map(Typed::Temporal)
        }
        (
            Literal::Str(s) | Literal::Date(s) | Literal::Timestamp(s),
            PrimitiveType::TimestampNtz,
        ) => parse_timestamp_ntz_micros(s).ok().map(Typed::Temporal),
        _ => None,
    }
}

// ── LIKE matching ───────────────────────────────────────────────────

/// SQL LIKE over the full pattern language: `%` matches any run of
/// characters (including empty), `_` exactly one. Escape sequences never
/// reach here (the converter routes `ESCAPE` to `Unsupported`). Classic
/// iterative backtracking on the last `%`.
fn like_match(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let (mut ti, mut pi) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && p[pi] == '%' {
            star = Some((pi, ti));
            pi += 1;
        } else if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate_ast::parse;
    use delta_kernel::schema::{StructField, StructType};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(
            StructType::try_new(vec![
                StructField::nullable("country", DataType::Primitive(PrimitiveType::String)),
                StructField::nullable("year", DataType::Primitive(PrimitiveType::Integer)),
                StructField::nullable("active", DataType::Primitive(PrimitiveType::Boolean)),
                StructField::nullable("day", DataType::Primitive(PrimitiveType::Date)),
            ])
            .unwrap(),
        )
    }

    fn vals(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn ev(input: &str, pairs: &[(&str, &str)]) -> Truth {
        let pred = parse(input).unwrap().normalized();
        eval(&pred, &vals(pairs), &schema())
    }

    #[test]
    fn like_matcher_covers_the_pattern_language() {
        assert!(like_match("Anderson", "%son"));
        assert!(like_match("Anderson", "A%"));
        assert!(like_match("Anderson", "%der%"));
        assert!(like_match("Anderson", "A%n"));
        assert!(like_match("DE", "D_"));
        assert!(like_match("abc", "abc"));
        assert!(like_match("", "%"));
        assert!(like_match("", ""));
        assert!(like_match("a%b", "a%b")); // literal % in text matched by wildcard
        assert!(!like_match("Anderson", "son%"));
        assert!(!like_match("DE", "D__"));
        assert!(!like_match("abc", ""));
        assert!(!like_match("", "_"));
        // pathological backtracking still terminates and answers correctly
        assert!(like_match(
            &"a".repeat(50),
            "%a%a%a%a%b%".replace('b', "a").as_str()
        ));
        assert!(!like_match(&"a".repeat(50), "%a%a%b"));
    }

    #[test]
    fn junction_tables_are_exact_for_the_keep_decision() {
        use Truth::*;
        assert_eq!(Null.and(Unknown), Null); // FALSE or NULL either way: drops
        assert_eq!(Null.or(Unknown), Unknown); // TRUE or NULL: must keep
        assert_eq!(False.and(Unknown), False);
        assert_eq!(True.or(Unknown), True);
        assert_eq!(True.and(Unknown), Unknown);
        assert_eq!(False.or(Unknown), Unknown);
        assert_eq!(Null.not(), Null);
        assert_eq!(Unknown.not(), Unknown);
    }

    #[test]
    fn non_prefix_like_evaluates_exactly() {
        assert_eq!(ev("country LIKE '%E'", &[("country", "DE")]), Truth::True);
        assert_eq!(ev("country LIKE '%E'", &[("country", "US")]), Truth::False);
        assert_eq!(
            ev("country NOT LIKE '%E'", &[("country", "US")]),
            Truth::True
        );
    }

    #[test]
    fn null_partition_value_is_sql_null_not_ignorance() {
        // absent key = NULL: LIKE, comparisons and IN are all NULL...
        assert_eq!(ev("country LIKE '%E'", &[]), Truth::Null);
        assert_eq!(ev("country NOT LIKE '%E'", &[]), Truth::Null);
        assert_eq!(ev("year > 2020", &[]), Truth::Null);
        assert_eq!(ev("country IN ('DE', 'IT')", &[]), Truth::Null);
        // ...while the null checks and DISTINCT are two-valued
        assert_eq!(ev("country IS NULL", &[]), Truth::True);
        assert_eq!(ev("country IS NOT NULL", &[]), Truth::False);
        assert_eq!(ev("country IS DISTINCT FROM 'DE'", &[]), Truth::True);
        assert_eq!(ev("country IS NOT DISTINCT FROM 'DE'", &[]), Truth::False);
    }

    #[test]
    fn comparisons_follow_the_column_type_not_string_order() {
        // "9" > "10" lexicographically; the integer type must win
        assert_eq!(ev("year > 9", &[("year", "10")]), Truth::True);
        assert_eq!(ev("year <= 9", &[("year", "10")]), Truth::False);
        assert_eq!(
            ev("day > '2026-01-31'", &[("day", "2026-02-01")]),
            Truth::True
        );
        assert_eq!(ev("country > 'D'", &[("country", "DE")]), Truth::True);
    }

    #[test]
    fn in_list_handles_null_items_per_sql() {
        assert_eq!(
            ev("country IN ('DE', 'IT')", &[("country", "DE")]),
            Truth::True
        );
        assert_eq!(
            ev("country IN ('IT', 'US')", &[("country", "DE")]),
            Truth::False
        );
        // no match + NULL in the list -> NULL, and NOT IN of NULL is NULL
        assert_eq!(
            ev("country IN ('IT', NULL)", &[("country", "DE")]),
            Truth::Null
        );
        assert_eq!(
            ev("country NOT IN ('IT', NULL)", &[("country", "DE")]),
            Truth::Null
        );
        // a match wins even with NULL alongside
        assert_eq!(
            ev("country IN ('DE', NULL)", &[("country", "DE")]),
            Truth::True
        );
    }

    #[test]
    fn unparseable_values_degrade_to_unknown_never_to_a_verdict() {
        assert_eq!(
            ev("year > 2020", &[("year", "not-a-number")]),
            Truth::Unknown
        );
        assert_eq!(ev("active", &[("active", "maybe")]), Truth::Unknown);
        // Unknown under NOT stays Unknown: no accidental verdict
        assert_eq!(
            ev("NOT year > 2020", &[("year", "not-a-number")]),
            Truth::Unknown
        );
    }

    #[test]
    fn junctions_combine_leaves_with_sql_semantics() {
        assert_eq!(
            ev(
                "country LIKE '%E' AND year > 2020",
                &[("country", "DE"), ("year", "2026")]
            ),
            Truth::True
        );
        // NULL AND FALSE is FALSE (definitive), NULL AND TRUE is NULL
        assert_eq!(
            ev("country LIKE '%E' AND year > 2020", &[("year", "2000")]),
            Truth::False
        );
        assert_eq!(
            ev("country LIKE '%E' AND year > 2020", &[("year", "2026")]),
            Truth::Null
        );
        assert_eq!(
            ev("country LIKE '%E' OR year > 2020", &[("year", "2026")]),
            Truth::True
        );
    }

    #[test]
    fn between_and_boolcol_evaluate() {
        assert_eq!(
            ev("year BETWEEN 2020 AND 2030", &[("year", "2026")]),
            Truth::True
        );
        assert_eq!(
            ev("year NOT BETWEEN 2020 AND 2030", &[("year", "2026")]),
            Truth::False
        );
        assert_eq!(ev("active", &[("active", "true")]), Truth::True);
        assert_eq!(ev("NOT active", &[("active", "true")]), Truth::False);
        assert_eq!(ev("active", &[]), Truth::Null);
    }

    #[test]
    fn empty_string_is_a_value_not_null() {
        assert_eq!(ev("country = ''", &[("country", "")]), Truth::True);
        assert_eq!(ev("country IS NULL", &[("country", "")]), Truth::False);
        assert_eq!(ev("country LIKE '%'", &[("country", "")]), Truth::True);
    }

    #[test]
    fn keeps_drops_false_and_null_keeps_true_and_unknown() {
        let pred = parse("country LIKE '%E'").unwrap().normalized();
        let s = schema();
        assert!(keeps(&pred, &vals(&[("country", "DE")]), &s));
        assert!(!keeps(&pred, &vals(&[("country", "US")]), &s));
        assert!(!keeps(&pred, &vals(&[]), &s)); // NULL drops exactly
        let opaque = parse("UPPER(country) = 'DE'").unwrap();
        assert!(keeps(&opaque, &vals(&[]), &s)); // ignorance keeps
    }
}
