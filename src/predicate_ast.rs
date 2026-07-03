//! The owned predicate AST: the language of pruning, not the language of SQL.
//!
//! The vocabulary mirrors what delta-kernel's scan planner can actually use
//! (column-op-literal comparisons, junctions, null checks), plus `In` and
//! `Between` kept as sugar for display fidelity. Everything outside that
//! vocabulary is compressed into an [`Pred::Unsupported`] leaf at the
//! sqlparser boundary, once, with a reason; downstream interpreters
//! (classification in `predicate_analyzer`, kernel emission in
//! `kernel_bridge`) never see raw SQL nodes.

use std::fmt;

use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, UnaryOperator, Value as SqlValue};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    /// The operator that preserves meaning when the operands swap sides,
    /// so `5 < x` normalizes to `x > 5`.
    fn flipped(self) -> Self {
        match self {
            CmpOp::Eq => CmpOp::Eq,
            CmpOp::Ne => CmpOp::Ne,
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Ge => CmpOp::Le,
        }
    }

    /// The logical complement under SQL three-valued logic: `NOT (x < 5)`
    /// and `x >= 5` are both NULL when x is NULL, so the rewrite is safe.
    fn negated(self) -> Self {
        match self {
            CmpOp::Eq => CmpOp::Ne,
            CmpOp::Ne => CmpOp::Eq,
            CmpOp::Lt => CmpOp::Ge,
            CmpOp::Le => CmpOp::Gt,
            CmpOp::Gt => CmpOp::Le,
            CmpOp::Ge => CmpOp::Lt,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColRef(pub Vec<String>);

impl ColRef {
    pub fn dotted(&self) -> String {
        self.0.join(".")
    }
}

/// Literals stay lexical: the number keeps the text it was written as, and
/// typed coercion happens only at kernel emission, where the schema lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    Number(String),
    Str(String),
    Bool(bool),
    Null,
    Date(String),
    Timestamp(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pred {
    And(Vec<Pred>),
    Or(Vec<Pred>),
    Not(Box<Pred>),
    Cmp {
        col: ColRef,
        op: CmpOp,
        lit: Literal,
    },
    In {
        col: ColRef,
        list: Vec<Literal>,
        negated: bool,
    },
    Between {
        col: ColRef,
        low: Literal,
        high: Literal,
        negated: bool,
    },
    IsNull {
        col: ColRef,
        negated: bool,
    },
    /// A bare boolean column used as a predicate: `WHERE is_active`.
    BoolCol(ColRef),
    /// Anything sqlparser accepted but the pruning language cannot express.
    /// `raw` is the original SQL fragment, `reason` explains why.
    Unsupported {
        raw: String,
        reason: String,
    },
}

impl Pred {
    fn and2(a: Pred, b: Pred) -> Pred {
        let mut v = match a {
            Pred::And(v) => v,
            other => vec![other],
        };
        match b {
            Pred::And(w) => v.extend(w),
            other => v.push(other),
        }
        Pred::And(v)
    }

    fn or2(a: Pred, b: Pred) -> Pred {
        let mut v = match a {
            Pred::Or(v) => v,
            other => vec![other],
        };
        match b {
            Pred::Or(w) => v.extend(w),
            other => v.push(other),
        }
        Pred::Or(v)
    }

    pub fn contains_unsupported(&self) -> bool {
        match self {
            Pred::And(v) | Pred::Or(v) => v.iter().any(Pred::contains_unsupported),
            Pred::Not(p) => p.contains_unsupported(),
            Pred::Unsupported { .. } => true,
            Pred::Cmp { .. }
            | Pred::In { .. }
            | Pred::Between { .. }
            | Pred::IsNull { .. }
            | Pred::BoolCol(_) => false,
        }
    }

    /// Dotted names of every column the predicate touches.
    pub fn columns(&self) -> Vec<String> {
        let mut cols = Vec::new();
        self.collect_columns(&mut cols);
        cols
    }

    fn collect_columns(&self, cols: &mut Vec<String>) {
        match self {
            Pred::And(v) | Pred::Or(v) => {
                for p in v {
                    p.collect_columns(cols);
                }
            }
            Pred::Not(p) => p.collect_columns(cols),
            Pred::Cmp { col, .. }
            | Pred::In { col, .. }
            | Pred::Between { col, .. }
            | Pred::IsNull { col, .. }
            | Pred::BoolCol(col) => cols.push(col.dotted()),
            Pred::Unsupported { .. } => {}
        }
    }

    /// The top-level conjuncts: the units the analyzer classifies.
    pub fn conjuncts(&self) -> Vec<&Pred> {
        match self {
            Pred::And(v) => v.iter().collect(),
            other => vec![other],
        }
    }

    /// The reasons attached to every `Unsupported` leaf, in tree order.
    pub fn unsupported_reasons(&self) -> Vec<&str> {
        let mut reasons = Vec::new();
        self.collect_reasons(&mut reasons);
        reasons
    }

    fn collect_reasons<'a>(&'a self, reasons: &mut Vec<&'a str>) {
        match self {
            Pred::And(v) | Pred::Or(v) => {
                for p in v {
                    p.collect_reasons(reasons);
                }
            }
            Pred::Not(p) => p.collect_reasons(reasons),
            Pred::Unsupported { reason, .. } => reasons.push(reason),
            Pred::Cmp { .. }
            | Pred::In { .. }
            | Pred::Between { .. }
            | Pred::IsNull { .. }
            | Pred::BoolCol(_) => {}
        }
    }
}

// ── Normalization: semantics-preserving rewrites ────────────────────

impl Pred {
    /// Normalize the predicate: push negations down to the leaves
    /// (De Morgan), then factor conjuncts common to every OR branch out
    /// of the OR. Both rewrites preserve SQL three-valued semantics, so
    /// the kernel's survivor set cannot change; what changes is how much
    /// of the predicate the classifier can attribute to a single phase.
    pub fn normalized(self) -> Pred {
        factor_or(push_down_not(self, false))
    }

    /// The predicate with every `Unsupported` subtree removed
    /// conservatively: a dropped constraint can only keep more files,
    /// never prune one. Under AND the siblings survive; an OR (or NOT)
    /// touching an unsupported leaf is dropped whole, because its truth
    /// value cannot be bounded. `None` means nothing is left to scan with.
    pub fn without_unsupported(&self) -> Option<Pred> {
        match self {
            Pred::And(v) => {
                let kept: Vec<Pred> = v.iter().filter_map(Pred::without_unsupported).collect();
                match kept.len() {
                    0 => None,
                    _ => Some(and_flat(kept)),
                }
            }
            Pred::Or(_) | Pred::Not(_) => {
                if self.contains_unsupported() {
                    None
                } else {
                    Some(self.clone())
                }
            }
            Pred::Unsupported { .. } => None,
            Pred::Cmp { .. }
            | Pred::In { .. }
            | Pred::Between { .. }
            | Pred::IsNull { .. }
            | Pred::BoolCol(_) => Some(self.clone()),
        }
    }
}

/// Smart constructor: flatten nested ANDs, unwrap the single-element case.
fn and_flat(parts: Vec<Pred>) -> Pred {
    let mut out = Vec::with_capacity(parts.len());
    for p in parts {
        match p {
            Pred::And(v) => out.extend(v),
            other => out.push(other),
        }
    }
    match out.len() {
        1 => match out.pop() {
            Some(single) => single,
            None => Pred::And(out),
        },
        _ => Pred::And(out),
    }
}

/// Smart constructor: flatten nested ORs, unwrap the single-element case.
fn or_flat(parts: Vec<Pred>) -> Pred {
    let mut out = Vec::with_capacity(parts.len());
    for p in parts {
        match p {
            Pred::Or(v) => out.extend(v),
            other => out.push(other),
        }
    }
    match out.len() {
        1 => match out.pop() {
            Some(single) => single,
            None => Pred::Or(out),
        },
        _ => Pred::Or(out),
    }
}

/// Push negations toward the leaves. `neg` is the parity accumulated from
/// enclosing NOTs. Comparisons complement their operator; IN, BETWEEN and
/// IS NULL toggle their negated flag; a bare boolean column keeps an
/// explicit NOT (rewriting it to `col = false` would change its meaning
/// on NULL). An unsupported leaf under NOT stays wrapped: its truth value
/// is unknown, so its complement is too.
fn push_down_not(pred: Pred, neg: bool) -> Pred {
    match pred {
        Pred::And(v) => {
            let parts: Vec<Pred> = v.into_iter().map(|p| push_down_not(p, neg)).collect();
            if neg { or_flat(parts) } else { and_flat(parts) }
        }
        Pred::Or(v) => {
            let parts: Vec<Pred> = v.into_iter().map(|p| push_down_not(p, neg)).collect();
            if neg { and_flat(parts) } else { or_flat(parts) }
        }
        Pred::Not(inner) => push_down_not(*inner, !neg),
        Pred::Cmp { col, op, lit } => Pred::Cmp {
            col,
            op: if neg { op.negated() } else { op },
            lit,
        },
        Pred::In { col, list, negated } => Pred::In {
            col,
            list,
            negated: negated != neg,
        },
        Pred::Between {
            col,
            low,
            high,
            negated,
        } => Pred::Between {
            col,
            low,
            high,
            negated: negated != neg,
        },
        Pred::IsNull { col, negated } => Pred::IsNull {
            col,
            negated: negated != neg,
        },
        leaf @ (Pred::BoolCol(_) | Pred::Unsupported { .. }) => {
            if neg {
                Pred::Not(Box::new(leaf))
            } else {
                leaf
            }
        }
    }
}

/// Factor conjuncts common to every OR branch out of the OR:
/// `(a AND x) OR (a AND y)` becomes `a AND (x OR y)` by the distributive
/// law. When some branch is exactly the common part, the OR collapses to
/// it (`a OR (a AND x)` is `a`). This is what turns a repeated partition
/// filter inside an OR into a partition-safe top-level conjunct.
fn factor_or(pred: Pred) -> Pred {
    match pred {
        Pred::And(v) => and_flat(v.into_iter().map(factor_or).collect()),
        Pred::Or(v) => {
            let children: Vec<Pred> = v.into_iter().map(factor_or).collect();
            let branch_sets: Vec<Vec<Pred>> = children
                .into_iter()
                .map(|c| match c {
                    Pred::And(parts) => parts,
                    other => vec![other],
                })
                .collect();

            let common: Vec<Pred> = match branch_sets.first() {
                Some(first) => first
                    .iter()
                    .filter(|item| branch_sets[1..].iter().all(|set| set.contains(item)))
                    .cloned()
                    .collect(),
                None => Vec::new(),
            };

            if common.is_empty() {
                return or_flat(branch_sets.into_iter().map(and_flat).collect::<Vec<_>>());
            }

            let mut remainders: Vec<Pred> = Vec::with_capacity(branch_sets.len());
            for set in branch_sets {
                let mut rest = set;
                for item in &common {
                    if let Some(pos) = rest.iter().position(|r| r == item) {
                        rest.remove(pos);
                    }
                }
                if rest.is_empty() {
                    // This branch is exactly the common part: TRUE absorbs
                    // the OR, the whole disjunction reduces to the factor.
                    return and_flat(common);
                }
                remainders.push(and_flat(rest));
            }

            let mut out = common;
            out.push(or_flat(remainders));
            and_flat(out)
        }
        Pred::Not(inner) => Pred::Not(Box::new(factor_or(*inner))),
        leaf => leaf,
    }
}

// ── Parsing: SQL string -> owned AST ────────────────────────────────

/// Parse a SQL WHERE-clause expression into the owned predicate AST.
///
/// Only malformed SQL errors here. Constructs that parse but fall outside
/// the pruning language become [`Pred::Unsupported`] leaves; whether that
/// is fatal is the consumer's decision, not the parser's.
pub fn parse(input: &str) -> Result<Pred, Error> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(input)
        .map_err(|e| Error::Predicate(format!("Parse error: {e}")))?;
    let sql_expr = parser
        .parse_expr()
        .map_err(|e| Error::Predicate(format!("Parse error: {e}")))?;
    Ok(convert(&sql_expr))
}

fn unsupported(expr: &SqlExpr, reason: String) -> Pred {
    Pred::Unsupported {
        raw: expr.to_string(),
        reason,
    }
}

fn convert(expr: &SqlExpr) -> Pred {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Pred::and2(convert(left), convert(right)),
            BinaryOperator::Or => Pred::or2(convert(left), convert(right)),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                let cmp = match op {
                    BinaryOperator::Eq => CmpOp::Eq,
                    BinaryOperator::NotEq => CmpOp::Ne,
                    BinaryOperator::Lt => CmpOp::Lt,
                    BinaryOperator::LtEq => CmpOp::Le,
                    BinaryOperator::Gt => CmpOp::Gt,
                    BinaryOperator::GtEq => CmpOp::Ge,
                    _ => return unsupported(expr, format!("Unsupported binary operator: {op}")),
                };
                convert_comparison(expr, left, cmp, right)
            }
            other => unsupported(expr, format!("Unsupported binary operator: {other}")),
        },

        SqlExpr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => Pred::Not(Box::new(convert(inner))),

        SqlExpr::IsNull(inner) => convert_null_check(expr, inner, false),
        SqlExpr::IsNotNull(inner) => convert_null_check(expr, inner, true),

        SqlExpr::InList {
            expr: lhs,
            list,
            negated,
        } => {
            if list.is_empty() {
                return unsupported(expr, "Empty IN list".into());
            }
            let col = match operand(lhs) {
                Ok(Operand::Col(c)) => c,
                Ok(Operand::Lit(_)) => {
                    return unsupported(expr, format!("IN requires a column, got: {lhs}"));
                }
                Err(reason) => return unsupported(expr, reason),
            };
            let mut items = Vec::with_capacity(list.len());
            for item in list {
                match operand(item) {
                    Ok(Operand::Lit(l)) => items.push(l),
                    Ok(Operand::Col(_)) => {
                        return unsupported(
                            expr,
                            format!("IN list items must be literals, got: {item}"),
                        );
                    }
                    Err(reason) => return unsupported(expr, reason),
                }
            }
            Pred::In {
                col,
                list: items,
                negated: *negated,
            }
        }

        SqlExpr::Between {
            expr: lhs,
            negated,
            low,
            high,
        } => {
            let col = match operand(lhs) {
                Ok(Operand::Col(c)) => c,
                Ok(Operand::Lit(_)) => {
                    return unsupported(expr, format!("BETWEEN requires a column, got: {lhs}"));
                }
                Err(reason) => return unsupported(expr, reason),
            };
            let (lo, hi) = match (operand(low), operand(high)) {
                (Ok(Operand::Lit(lo)), Ok(Operand::Lit(hi))) => (lo, hi),
                (Ok(Operand::Col(_)), _) | (_, Ok(Operand::Col(_))) => {
                    return unsupported(expr, "BETWEEN bounds must be literals".into());
                }
                (Err(reason), _) | (_, Err(reason)) => return unsupported(expr, reason),
            };
            Pred::Between {
                col,
                low: lo,
                high: hi,
                negated: *negated,
            }
        }

        SqlExpr::Nested(inner) => convert(inner),

        SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => match operand(expr) {
            Ok(Operand::Col(c)) => Pred::BoolCol(c),
            _ => unsupported(expr, format!("Unsupported expression: {expr}")),
        },

        other => unsupported(other, format!("Unsupported expression: {other}")),
    }
}

fn convert_comparison(whole: &SqlExpr, left: &SqlExpr, op: CmpOp, right: &SqlExpr) -> Pred {
    match (operand(left), operand(right)) {
        (Ok(Operand::Col(col)), Ok(Operand::Lit(lit))) => Pred::Cmp { col, op, lit },
        (Ok(Operand::Lit(lit)), Ok(Operand::Col(col))) => Pred::Cmp {
            col,
            op: op.flipped(),
            lit,
        },
        (Ok(Operand::Col(_)), Ok(Operand::Col(_))) => unsupported(
            whole,
            "Column-to-column comparisons cannot use file statistics".into(),
        ),
        (Ok(Operand::Lit(_)), Ok(Operand::Lit(_))) => unsupported(
            whole,
            "Comparison between two literals references no column".into(),
        ),
        (Err(reason), _) | (_, Err(reason)) => unsupported(whole, reason),
    }
}

fn convert_null_check(whole: &SqlExpr, inner: &SqlExpr, negated: bool) -> Pred {
    match operand(inner) {
        Ok(Operand::Col(col)) => Pred::IsNull { col, negated },
        Ok(Operand::Lit(_)) => {
            unsupported(whole, format!("IS NULL requires a column, got: {inner}"))
        }
        Err(reason) => unsupported(whole, reason),
    }
}

enum Operand {
    Col(ColRef),
    Lit(Literal),
}

/// Extract a comparison operand. `Err` carries the reason the expression
/// falls outside the pruning language; the caller wraps it in `Unsupported`.
fn operand(expr: &SqlExpr) -> Result<Operand, String> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(Operand::Col(ColRef(vec![ident.value.clone()]))),
        SqlExpr::CompoundIdentifier(parts) => Ok(Operand::Col(ColRef(
            parts.iter().map(|p| p.value.clone()).collect(),
        ))),
        SqlExpr::Value(v) => literal_from_value(&v.value).map(Operand::Lit),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => match operand(inner)? {
            Operand::Lit(Literal::Number(s)) => {
                let negated = match s.strip_prefix('-') {
                    Some(rest) => rest.to_string(),
                    None => format!("-{s}"),
                };
                Ok(Operand::Lit(Literal::Number(negated)))
            }
            _ => Err("Unary minus only supported on numeric literals".into()),
        },
        SqlExpr::Nested(inner) => operand(inner),
        SqlExpr::TypedString(ts) => {
            use sqlparser::ast::DataType as SqlType;
            let Some(text) = ts.value.clone().into_string() else {
                return Err(format!("Unsupported typed literal: {ts}"));
            };
            match &ts.data_type {
                SqlType::Date => Ok(Operand::Lit(Literal::Date(text))),
                SqlType::Timestamp(_, _) => Ok(Operand::Lit(Literal::Timestamp(text))),
                other => Err(format!("Unsupported typed literal type: {other}")),
            }
        }
        other => Err(format!("Unsupported expression: {other}")),
    }
}

fn literal_from_value(val: &SqlValue) -> Result<Literal, String> {
    match val {
        SqlValue::Number(s, _) => Ok(Literal::Number(s.clone())),
        SqlValue::SingleQuotedString(s) => Ok(Literal::Str(s.clone())),
        SqlValue::Boolean(b) => Ok(Literal::Bool(*b)),
        SqlValue::Null => Ok(Literal::Null),
        other => Err(format!("Unsupported literal: {other}")),
    }
}

// ── Display: SQL-shaped rendering for reports and fragments ─────────

impl fmt::Display for CmpOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            CmpOp::Eq => "=",
            CmpOp::Ne => "<>",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        };
        f.write_str(s)
    }
}

impl fmt::Display for ColRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.dotted())
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Number(s) => f.write_str(s),
            Literal::Str(s) => write!(f, "'{}'", s.replace('\'', "''")),
            Literal::Bool(b) => write!(f, "{b}"),
            Literal::Null => f.write_str("NULL"),
            Literal::Date(s) => write!(f, "DATE '{}'", s.replace('\'', "''")),
            Literal::Timestamp(s) => write!(f, "TIMESTAMP '{}'", s.replace('\'', "''")),
        }
    }
}

impl Pred {
    /// True when the node needs parentheses as the child of a junction or NOT.
    fn is_composite(&self) -> bool {
        matches!(self, Pred::And(_) | Pred::Or(_))
    }
}

fn write_junction(f: &mut fmt::Formatter<'_>, parts: &[Pred], sep: &str) -> fmt::Result {
    for (i, p) in parts.iter().enumerate() {
        if i > 0 {
            f.write_str(sep)?;
        }
        if p.is_composite() {
            write!(f, "({p})")?;
        } else {
            write!(f, "{p}")?;
        }
    }
    Ok(())
}

impl fmt::Display for Pred {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Pred::And(v) => write_junction(f, v, " AND "),
            Pred::Or(v) => write_junction(f, v, " OR "),
            Pred::Not(p) => {
                if p.is_composite() {
                    write!(f, "NOT ({p})")
                } else {
                    write!(f, "NOT {p}")
                }
            }
            Pred::Cmp { col, op, lit } => write!(f, "{col} {op} {lit}"),
            Pred::In { col, list, negated } => {
                let not = if *negated { "NOT " } else { "" };
                write!(f, "{col} {not}IN (")?;
                for (i, item) in list.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{item}")?;
                }
                f.write_str(")")
            }
            Pred::Between {
                col,
                low,
                high,
                negated,
            } => {
                let not = if *negated { "NOT " } else { "" };
                write!(f, "{col} {not}BETWEEN {low} AND {high}")
            }
            Pred::IsNull { col, negated } => {
                let not = if *negated { "NOT " } else { "" };
                write!(f, "{col} IS {not}NULL")
            }
            Pred::BoolCol(col) => write!(f, "{col}"),
            Pred::Unsupported { raw, .. } => f.write_str(raw),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(input: &str) -> Pred {
        parse(input).unwrap()
    }

    #[test]
    fn comparison_roundtrips_through_display() {
        assert_eq!(p("country = 'DE'").to_string(), "country = 'DE'");
        assert_eq!(p("age > 30").to_string(), "age > 30");
        assert_eq!(p("age != 30").to_string(), "age <> 30");
        assert_eq!(p("profile.age >= 40").to_string(), "profile.age >= 40");
    }

    #[test]
    fn flipped_comparison_normalizes_column_to_the_left() {
        assert_eq!(p("30 < age").to_string(), "age > 30");
        assert_eq!(p("'DE' = country").to_string(), "country = 'DE'");
    }

    #[test]
    fn and_chains_flatten() {
        let pred = p("a = 1 AND b = 2 AND c = 3");
        match &pred {
            Pred::And(v) => assert_eq!(v.len(), 3),
            other => panic!("expected And, got {other:?}"),
        }
        assert_eq!(pred.to_string(), "a = 1 AND b = 2 AND c = 3");
    }

    #[test]
    fn nested_parens_unwrap() {
        assert_eq!(
            p("((((age > 40)))) AND (country = 'DE')").to_string(),
            "age > 40 AND country = 'DE'"
        );
    }

    #[test]
    fn or_inside_and_keeps_parens_in_display() {
        assert_eq!(
            p("(a = 1 OR b = 2) AND c = 3").to_string(),
            "(a = 1 OR b = 2) AND c = 3"
        );
    }

    #[test]
    fn in_and_between_render_as_written() {
        assert_eq!(
            p("country IN ('DE', 'IT')").to_string(),
            "country IN ('DE', 'IT')"
        );
        assert_eq!(
            p("country NOT IN ('US')").to_string(),
            "country NOT IN ('US')"
        );
        assert_eq!(
            p("age BETWEEN 40 AND 60").to_string(),
            "age BETWEEN 40 AND 60"
        );
    }

    #[test]
    fn unary_minus_folds_into_the_literal() {
        assert_eq!(p("delta > -5").to_string(), "delta > -5");
        assert_eq!(p("delta > - -5").to_string(), "delta > 5");
    }

    #[test]
    fn function_call_becomes_unsupported() {
        let pred = p("UPPER(country) = 'DE'");
        assert!(pred.contains_unsupported());
        match pred {
            Pred::Unsupported { raw, reason } => {
                assert_eq!(raw, "UPPER(country) = 'DE'");
                assert!(reason.contains("Unsupported expression"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn arithmetic_becomes_unsupported() {
        let pred = p("price * 2 > 100");
        assert!(pred.contains_unsupported());
    }

    #[test]
    fn like_becomes_unsupported() {
        assert!(p("name LIKE '%Hans%'").contains_unsupported());
    }

    #[test]
    fn subquery_becomes_unsupported() {
        assert!(p("age IN (SELECT 1)").contains_unsupported());
    }

    #[test]
    fn column_to_column_becomes_unsupported() {
        let pred = p("start_ts < end_ts");
        match pred {
            Pred::Unsupported { reason, .. } => {
                assert!(reason.contains("Column-to-column"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_fragment_under_and_does_not_poison_siblings() {
        let pred = p("country = 'DE' AND UPPER(name) = 'X'");
        let conjuncts = pred.conjuncts();
        assert_eq!(conjuncts.len(), 2);
        assert!(!conjuncts[0].contains_unsupported());
        assert!(conjuncts[1].contains_unsupported());
    }

    #[test]
    fn malformed_sql_is_a_parse_error() {
        assert!(parse("((").is_err());
        assert!(parse("").is_err());
    }

    #[test]
    fn columns_are_collected_dotted() {
        assert_eq!(
            p("country = 'DE' AND profile.geo.zip = '10115'").columns(),
            vec!["country", "profile.geo.zip"]
        );
    }

    fn norm(input: &str) -> String {
        p(input).normalized().to_string()
    }

    #[test]
    fn negation_pushes_through_comparisons() {
        assert_eq!(norm("NOT age > 30"), "age <= 30");
        assert_eq!(norm("NOT age = 30"), "age <> 30");
        assert_eq!(norm("NOT NOT age = 30"), "age = 30");
    }

    #[test]
    fn negation_pushes_through_junctions_by_de_morgan() {
        assert_eq!(
            norm("NOT (country = 'DE' OR age > 30)"),
            "country <> 'DE' AND age <= 30"
        );
        assert_eq!(
            norm("NOT (country = 'DE' AND age > 30)"),
            "country <> 'DE' OR age <= 30"
        );
    }

    #[test]
    fn negation_toggles_sugar_flags() {
        assert_eq!(norm("NOT country IN ('DE')"), "country NOT IN ('DE')");
        assert_eq!(
            norm("NOT age BETWEEN 30 AND 40"),
            "age NOT BETWEEN 30 AND 40"
        );
        assert_eq!(norm("NOT age IS NULL"), "age IS NOT NULL");
        assert_eq!(norm("NOT age IS NOT NULL"), "age IS NULL");
    }

    #[test]
    fn negation_stays_on_boolean_columns_and_unsupported() {
        assert_eq!(norm("NOT is_active"), "NOT is_active");
        assert_eq!(norm("NOT UPPER(name) = 'X'"), "NOT UPPER(name) = 'X'");
    }

    #[test]
    fn or_factoring_extracts_the_common_conjunct() {
        assert_eq!(
            norm("(country = 'DE' AND age > 30) OR (country = 'DE' AND score > 90)"),
            "country = 'DE' AND (age > 30 OR score > 90)"
        );
    }

    #[test]
    fn or_factoring_collapses_an_absorbed_branch() {
        assert_eq!(
            norm("country = 'DE' OR (country = 'DE' AND age > 30)"),
            "country = 'DE'"
        );
    }

    #[test]
    fn or_factoring_leaves_disjoint_branches_alone() {
        assert_eq!(
            norm("country = 'DE' OR age > 30"),
            "country = 'DE' OR age > 30"
        );
    }

    #[test]
    fn without_unsupported_keeps_and_siblings() {
        let pred = p("country = 'DE' AND UPPER(name) = 'X'");
        let clean = pred.without_unsupported().expect("sibling survives");
        assert_eq!(clean.to_string(), "country = 'DE'");
    }

    #[test]
    fn without_unsupported_drops_a_poisoned_or_whole() {
        let pred = p("age > 30 AND (country = 'DE' OR UPPER(name) = 'X')");
        let clean = pred.without_unsupported().expect("first conjunct survives");
        assert_eq!(clean.to_string(), "age > 30");
    }

    #[test]
    fn without_unsupported_is_none_when_nothing_remains() {
        assert!(p("UPPER(name) = 'X'").without_unsupported().is_none());
        assert!(
            p("UPPER(name) = 'X' OR country = 'DE'")
                .without_unsupported()
                .is_none()
        );
    }

    #[test]
    fn unsupported_reasons_surface_in_tree_order() {
        let pred = p("UPPER(a) = 'X' AND b LIKE 'y%'");
        let reasons = pred.unsupported_reasons();
        assert_eq!(reasons.len(), 2);
        assert!(reasons[0].contains("Unsupported expression"));
    }
}
