//! The owned predicate AST: the language of pruning, not the language of SQL.
//!
//! The vocabulary mirrors what the downstream interpreters can consume:
//! what delta-kernel's scan planner uses directly (column-op-literal
//! comparisons, junctions, null checks, with `In` and `Between` kept as
//! sugar for display fidelity), plus `Like`, which has no kernel lowering
//! but rewrites to a comparison range in its string-prefix shape and stays
//! evaluable against partition literals in every other. Anything else is
//! compressed into an [`Pred::Unsupported`] leaf at the sqlparser
//! boundary, once, with a reason; the interpreters (classification in
//! `predicate_analyzer`, kernel emission in `kernel_bridge`,
//! partition-literal evaluation in `partition_eval`) never see raw SQL
//! nodes.

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
    /// Null-safe comparison: `col IS [NOT] DISTINCT FROM lit`. Two-valued,
    /// never NULL, so its negation is exact. The kernel evaluates it over
    /// partition values; file statistics cannot prune on it.
    Distinct {
        col: ColRef,
        lit: Literal,
        negated: bool,
    },
    /// A bare boolean column used as a predicate: `WHERE is_active`.
    BoolCol(ColRef),
    /// `col [NOT] LIKE 'pattern'`, kept structural so normalization can
    /// rewrite the literal-prefix shape (on string columns) into a
    /// lexicographic range. A Like that survives normalization has no
    /// kernel lowering: when its columns are all partition columns the
    /// partition evaluator consumes it exactly; anywhere else it degrades
    /// like an [`Pred::Unsupported`] leaf.
    Like {
        col: ColRef,
        pattern: String,
        negated: bool,
    },
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

    /// True when the tree contains a node the kernel cannot lower: an
    /// [`Pred::Unsupported`] leaf, or a `Like` that survived normalization
    /// unrewritten. Narrower interpreters may still consume the latter;
    /// see [`Pred::contains_opaque`] for the distinction.
    pub fn contains_unsupported(&self) -> bool {
        match self {
            Pred::And(v) | Pred::Or(v) => v.iter().any(Pred::contains_unsupported),
            Pred::Not(p) => p.contains_unsupported(),
            // A Like at this point survived normalization (or was never
            // normalized): no rewrite applies, so the kernel cannot
            // consume it.
            Pred::Unsupported { .. } | Pred::Like { .. } => true,
            Pred::Cmp { .. }
            | Pred::In { .. }
            | Pred::Between { .. }
            | Pred::IsNull { .. }
            | Pred::Distinct { .. }
            | Pred::BoolCol(_) => false,
        }
    }

    /// True when the tree contains a subtree whose *semantics* are unknown
    /// (an [`Pred::Unsupported`] leaf). Narrower than
    /// [`Pred::contains_unsupported`]: a surviving `Like` has no kernel
    /// lowering but its meaning is fully known, so an interpreter with its
    /// own evaluation (the partition-literal evaluator) can still consume
    /// the fragment. An opaque subtree can never be evaluated by anyone.
    pub fn contains_opaque(&self) -> bool {
        match self {
            Pred::And(v) | Pred::Or(v) => v.iter().any(Pred::contains_opaque),
            Pred::Not(p) => p.contains_opaque(),
            Pred::Unsupported { .. } => true,
            Pred::Cmp { .. }
            | Pred::In { .. }
            | Pred::Between { .. }
            | Pred::IsNull { .. }
            | Pred::Distinct { .. }
            | Pred::Like { .. }
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
            | Pred::Distinct { col, .. }
            | Pred::Like { col, .. }
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
            Pred::Like { .. } => reasons.push(
                "this LIKE cannot prune here: only a non-negated literal-prefix pattern \
                 on a string column rewrites to a comparison range, and only a fragment \
                 referencing partition columns alone evaluates against partition values",
            ),
            Pred::Cmp { .. }
            | Pred::In { .. }
            | Pred::Between { .. }
            | Pred::IsNull { .. }
            | Pred::Distinct { .. }
            | Pred::BoolCol(_) => {}
        }
    }
}

// ── Normalization: semantics-preserving rewrites ────────────────────

impl Pred {
    /// Normalize the predicate: push negations down to the leaves
    /// (De Morgan), rewrite prefix-shaped LIKE patterns into comparison
    /// ranges, then factor conjuncts common to every OR branch out of the
    /// OR. All three rewrites preserve SQL three-valued semantics, so the
    /// kernel's survivor set cannot change; what changes is how much of
    /// the predicate the classifier can attribute to a single phase.
    ///
    /// Assumes every LIKE column is string-typed; when a schema is in
    /// hand, use [`Pred::normalized_with`] instead.
    pub fn normalized(self) -> Pred {
        self.normalized_with(|_| true)
    }

    /// [`Pred::normalized`] with schema knowledge: `is_string_col` gates
    /// the prefix-LIKE range rewrite. The lexicographic equivalence only
    /// holds on string columns; on any other type SQL's LIKE matches the
    /// value *cast to a string*, which a comparison range cannot express,
    /// so the Like node passes through for the downstream interpreters
    /// to handle (exactly, on partition literals; conservatively
    /// otherwise).
    pub fn normalized_with<F>(self, is_string_col: F) -> Pred
    where
        F: Fn(&ColRef) -> bool,
    {
        factor_or(rewrite_prefix_like(
            push_down_not(self, false),
            &is_string_col,
        ))
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
            Pred::Unsupported { .. } | Pred::Like { .. } => None,
            Pred::Cmp { .. }
            | Pred::In { .. }
            | Pred::Between { .. }
            | Pred::IsNull { .. }
            | Pred::Distinct { .. }
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
        Pred::Distinct { col, lit, negated } => Pred::Distinct {
            col,
            lit,
            negated: negated != neg,
        },
        // LIKE is NULL exactly when the column is NULL, so NOT toggles it
        // as cleanly as it complements a comparison operator.
        Pred::Like {
            col,
            pattern,
            negated,
        } => Pred::Like {
            col,
            pattern,
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

/// The shape of a LIKE pattern, as far as the range rewrite cares.
enum LikeShape {
    /// No wildcards at all: LIKE is plain equality on the pattern text.
    Exact(String),
    /// A non-empty literal chunk followed only by `%` wildcards.
    Prefix(String),
    /// Anything else: leading or embedded wildcards, any `_`.
    Other,
}

fn like_shape(pattern: &str) -> LikeShape {
    match pattern.find(['%', '_']) {
        None => LikeShape::Exact(pattern.to_string()),
        Some(0) => LikeShape::Other,
        Some(i) if pattern[i..].chars().all(|c| c == '%') => {
            LikeShape::Prefix(pattern[..i].to_string())
        }
        Some(_) => LikeShape::Other,
    }
}

/// Rewrite prefix-shaped LIKE leaves into the pruning language: a
/// wildcard-free pattern becomes plain equality, and `col LIKE 'p%'`
/// becomes the lexicographic range `col >= 'p' AND col < succ(p)`.
///
/// Both rewrites are exact under three-valued semantics (LIKE and the
/// comparisons are all NULL exactly when the column is NULL) over binary
/// code-point order, which is the order partition values and min/max
/// string statistics compare with - and only on string columns, which is
/// why `is_string` gates the leaf. Negated LIKE, non-string columns, and
/// every other pattern shape pass through untouched and degrade (or
/// evaluate) downstream.
fn rewrite_prefix_like<F>(pred: Pred, is_string: &F) -> Pred
where
    F: Fn(&ColRef) -> bool,
{
    match pred {
        Pred::And(v) => and_flat(
            v.into_iter()
                .map(|p| rewrite_prefix_like(p, is_string))
                .collect(),
        ),
        Pred::Or(v) => or_flat(
            v.into_iter()
                .map(|p| rewrite_prefix_like(p, is_string))
                .collect(),
        ),
        Pred::Not(inner) => Pred::Not(Box::new(rewrite_prefix_like(*inner, is_string))),
        Pred::Like {
            col,
            pattern,
            negated: false,
        } if is_string(&col) => match like_shape(&pattern) {
            LikeShape::Exact(text) => Pred::Cmp {
                col,
                op: CmpOp::Eq,
                lit: Literal::Str(text),
            },
            LikeShape::Prefix(prefix) => {
                let lower = Pred::Cmp {
                    col: col.clone(),
                    op: CmpOp::Ge,
                    lit: Literal::Str(prefix.clone()),
                };
                match prefix_successor(&prefix) {
                    Some(upper) => Pred::And(vec![
                        lower,
                        Pred::Cmp {
                            col,
                            op: CmpOp::Lt,
                            lit: Literal::Str(upper),
                        },
                    ]),
                    // A prefix made of maximal code points has no
                    // successor, but every string >= it starts with it,
                    // so the lower bound alone is already exact.
                    None => lower,
                }
            }
            LikeShape::Other => Pred::Like {
                col,
                pattern,
                negated: false,
            },
        },
        leaf => leaf,
    }
}

/// The least string greater than every string that starts with `prefix`,
/// in code-point order: bump the last character to its successor, carrying
/// over characters that have none. `None` when every character is
/// `char::MAX` (no such string exists).
fn prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        if let Some(next) = char_successor(last) {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// The next Unicode scalar value after `c`, skipping the surrogate gap.
fn char_successor(c: char) -> Option<char> {
    let mut u = c as u32 + 1;
    while u <= char::MAX as u32 {
        if let Some(next) = char::from_u32(u) {
            return Some(next);
        }
        u += 1;
    }
    None
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

        SqlExpr::IsDistinctFrom(a, b) => convert_distinct(expr, a, b, false),
        SqlExpr::IsNotDistinctFrom(a, b) => convert_distinct(expr, a, b, true),

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

        SqlExpr::Like {
            negated,
            any,
            expr: lhs,
            pattern,
            escape_char,
        } => {
            if *any {
                return unsupported(expr, "LIKE ANY is not supported".into());
            }
            if escape_char.is_some() {
                return unsupported(expr, "LIKE with an ESCAPE clause is not supported".into());
            }
            let col = match operand(lhs) {
                Ok(Operand::Col(c)) => c,
                Ok(Operand::Lit(_)) => {
                    return unsupported(expr, format!("LIKE requires a column, got: {lhs}"));
                }
                Err(reason) => return unsupported(expr, reason),
            };
            match operand(pattern) {
                Ok(Operand::Lit(Literal::Str(s))) => Pred::Like {
                    col,
                    pattern: s,
                    negated: *negated,
                },
                Ok(_) => unsupported(
                    expr,
                    format!("LIKE pattern must be a string literal, got: {pattern}"),
                ),
                Err(reason) => unsupported(expr, reason),
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

fn convert_distinct(whole: &SqlExpr, a: &SqlExpr, b: &SqlExpr, negated: bool) -> Pred {
    // DISTINCT is symmetric, so the literal-first form needs no operator flip.
    match (operand(a), operand(b)) {
        (Ok(Operand::Col(col)), Ok(Operand::Lit(lit)))
        | (Ok(Operand::Lit(lit)), Ok(Operand::Col(col))) => Pred::Distinct { col, lit, negated },
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
            Pred::Distinct { col, lit, negated } => {
                let not = if *negated { "NOT " } else { "" };
                write!(f, "{col} IS {not}DISTINCT FROM {lit}")
            }
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
            Pred::Like {
                col,
                pattern,
                negated,
            } => {
                let not = if *negated { "NOT " } else { "" };
                write!(f, "{col} {not}LIKE '{}'", pattern.replace('\'', "''"))
            }
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
    fn non_prefix_like_counts_as_unsupported() {
        assert!(p("name LIKE '%Hans%'").contains_unsupported());
        assert!(p("name LIKE '%Hans%'").normalized().contains_unsupported());
        assert!(!p("name LIKE 'Hans%'").normalized().contains_unsupported());
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
    fn distinct_parses_and_negation_toggles_exactly() {
        assert_eq!(
            p("country IS DISTINCT FROM 'DE'").to_string(),
            "country IS DISTINCT FROM 'DE'"
        );
        assert_eq!(
            p("'DE' IS DISTINCT FROM country").to_string(),
            "country IS DISTINCT FROM 'DE'"
        );
        assert_eq!(
            norm("NOT country IS DISTINCT FROM 'DE'"),
            "country IS NOT DISTINCT FROM 'DE'"
        );
        assert_eq!(
            norm("NOT country IS NOT DISTINCT FROM 'DE'"),
            "country IS DISTINCT FROM 'DE'"
        );
    }

    #[test]
    fn unsupported_reasons_surface_in_tree_order() {
        let pred = p("UPPER(a) = 'X' AND b LIKE 'y%'");
        let reasons = pred.unsupported_reasons();
        assert_eq!(reasons.len(), 2);
        assert!(reasons[0].contains("Unsupported expression"));
    }

    // ── Edge cases: literals ────────────────────────────────────────

    #[test]
    fn string_literals_roundtrip_escaping_and_emptiness() {
        assert_eq!(p("name = 'It''s'").to_string(), "name = 'It''s'");
        assert_eq!(p("name = ''").to_string(), "name = ''");
        assert_eq!(p("name = 'a''b''c'").to_string(), "name = 'a''b''c'");
    }

    #[test]
    fn numeric_literals_stay_lexical() {
        // The AST never reinterprets the text: scientific notation, leading
        // zeros, and high precision survive until schema-driven coercion.
        assert_eq!(p("age = 1e5").to_string(), "age = 1e5");
        assert_eq!(p("age = 007").to_string(), "age = 007");
        assert_eq!(
            p("score = 3.14159265358979").to_string(),
            "score = 3.14159265358979"
        );
    }

    #[test]
    fn boolean_literal_comparison_is_supported_bare_boolean_is_not() {
        assert!(!p("active = TRUE").contains_unsupported());
        assert_eq!(p("active = TRUE").to_string(), "active = true");
        // A bare TRUE/FALSE references no column: nothing to prune on.
        assert!(p("TRUE").contains_unsupported());
    }

    #[test]
    fn exotic_literals_become_unsupported() {
        assert!(p("x = 0x1F").contains_unsupported()); // hex
        assert!(p("ts = INTERVAL '7' DAY").contains_unsupported());
    }

    #[test]
    fn typed_temporal_literals_roundtrip() {
        assert_eq!(
            p("d = DATE '2026-07-01'").to_string(),
            "d = DATE '2026-07-01'"
        );
        assert_eq!(
            p("ts = TIMESTAMP '2026-07-01 10:00:00'").to_string(),
            "ts = TIMESTAMP '2026-07-01 10:00:00'"
        );
    }

    #[test]
    fn literal_only_comparison_becomes_unsupported() {
        let pred = p("5 = 5");
        match pred {
            Pred::Unsupported { reason, .. } => assert!(reason.contains("no column")),
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn unary_minus_on_a_string_becomes_unsupported() {
        let pred = p("name = -'x'");
        match pred {
            Pred::Unsupported { reason, .. } => {
                assert!(reason.contains("numeric literals"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    // ── Edge cases: identifiers ─────────────────────────────────────

    #[test]
    fn quoted_identifiers_parse_and_lose_their_quotes() {
        assert_eq!(p("\"country\" = 'DE'").to_string(), "country = 'DE'");
    }

    #[test]
    fn quoted_identifier_containing_a_dot_is_a_single_column_part() {
        // "my.col" is one column whose name contains a dot, not a nested
        // path; the dotted rendering is ambiguous with a real path, but the
        // column list carries it as written and classification matches it
        // against partition_columns verbatim.
        let pred = p("\"my.col\" = 5");
        assert_eq!(pred.columns(), vec!["my.col"]);
        match &pred {
            Pred::Cmp { col, .. } => assert_eq!(col.0, vec!["my.col".to_string()]),
            other => panic!("expected Cmp, got {other:?}"),
        }
    }

    // ── Edge cases: structure ───────────────────────────────────────

    #[test]
    fn empty_in_list_is_a_parse_error_not_a_conversion_gap() {
        // sqlparser rejects `IN ()` before our converter ever sees it; the
        // converter's own empty-list guard is defense in depth.
        assert!(parse("age IN ()").is_err());
    }

    #[test]
    fn direct_not_in_and_not_between_carry_the_negated_flag() {
        assert_eq!(
            p("country NOT IN ('US')").to_string(),
            "country NOT IN ('US')"
        );
        assert_eq!(
            p("age NOT BETWEEN 1 AND 2").to_string(),
            "age NOT BETWEEN 1 AND 2"
        );
    }

    #[test]
    fn or_chains_flatten_like_and_chains() {
        let pred = p("a = 1 OR (b = 2 OR c = 3)");
        match &pred {
            Pred::Or(v) => assert_eq!(v.len(), 3),
            other => panic!("expected Or, got {other:?}"),
        }
        assert_eq!(pred.to_string(), "a = 1 OR b = 2 OR c = 3");
    }

    #[test]
    fn raw_not_over_a_junction_displays_with_parens() {
        // Before normalization the NOT is still on the junction; the display
        // must parenthesize or the meaning changes.
        assert_eq!(
            p("NOT (a = 1 AND b = 2)").to_string(),
            "NOT (a = 1 AND b = 2)"
        );
    }

    #[test]
    fn null_checks_work_on_nested_columns() {
        assert_eq!(
            p("profile.geo.zip IS NOT NULL").to_string(),
            "profile.geo.zip IS NOT NULL"
        );
        assert_eq!(
            norm("NOT profile.geo.zip IS NOT NULL"),
            "profile.geo.zip IS NULL"
        );
    }

    #[test]
    fn pathological_nesting_is_a_clean_parse_error() {
        // sqlparser's recursion limit turns deep nesting into an error
        // instead of a stack overflow; this test pins that protection.
        let deep = format!("{}age > 1{}", "(".repeat(200), ")".repeat(200));
        let err = parse(&deep);
        assert!(err.is_err());
        let msg = match err {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error"),
        };
        assert!(msg.contains("recursion"), "unexpected message: {msg}");
    }

    #[test]
    fn moderate_nesting_is_fine() {
        let deep = format!("{}age > 1{}", "(".repeat(30), ")".repeat(30));
        assert_eq!(p(&deep).to_string(), "age > 1");
    }

    // ── Edge cases: rewrites ────────────────────────────────────────

    #[test]
    fn factoring_with_duplicate_conjuncts_stays_equivalent() {
        // The duplicated factor is removed once per occurrence; the result
        // keeps the redundant copy rather than risking a semantic change.
        assert_eq!(
            norm("(a = 1 AND a = 1 AND x > 2) OR (a = 1 AND y > 3)"),
            "a = 1 AND a = 1 AND (x > 2 OR y > 3)"
        );
    }

    #[test]
    fn factoring_recurses_into_nested_junctions() {
        // The inner OR factors first, which lets the outer AND flatten
        // around it.
        assert_eq!(
            norm("c = 9 AND ((a = 1 AND x > 2) OR (a = 1 AND y > 3))"),
            "c = 9 AND a = 1 AND (x > 2 OR y > 3)"
        );
    }

    #[test]
    fn double_negation_over_sugar_cancels_exactly() {
        assert_eq!(norm("NOT NOT country IN ('DE')"), "country IN ('DE')");
        assert_eq!(norm("NOT NOT age BETWEEN 1 AND 2"), "age BETWEEN 1 AND 2");
    }

    // ── LIKE: parsing, display, and the prefix rewrite ──────────────

    #[test]
    fn like_parses_structurally_and_roundtrips_through_display() {
        assert_eq!(p("name LIKE '%son'").to_string(), "name LIKE '%son'");
        assert_eq!(
            p("name NOT LIKE '%son'").to_string(),
            "name NOT LIKE '%son'"
        );
        assert_eq!(p("name LIKE 'D''s%'").to_string(), "name LIKE 'D''s%'");
    }

    #[test]
    fn prefix_like_rewrites_to_a_lexicographic_range() {
        assert_eq!(
            norm("country LIKE 'D%'"),
            "country >= 'D' AND country < 'E'"
        );
        assert_eq!(norm("name LIKE 'ab%'"), "name >= 'ab' AND name < 'ac'");
        // a trailing run of % is one wildcard
        assert_eq!(norm("name LIKE 'ab%%'"), "name >= 'ab' AND name < 'ac'");
        // the rewritten range flattens into the surrounding AND
        assert_eq!(
            norm("country LIKE 'D%' AND age > 40"),
            "country >= 'D' AND country < 'E' AND age > 40"
        );
    }

    #[test]
    fn wildcard_free_like_rewrites_to_equality() {
        assert_eq!(norm("name LIKE 'abc'"), "name = 'abc'");
        assert_eq!(norm("name LIKE ''"), "name = ''");
    }

    #[test]
    fn non_prefix_patterns_do_not_rewrite() {
        assert_eq!(norm("name LIKE '%son'"), "name LIKE '%son'");
        assert_eq!(norm("name LIKE 'a%b'"), "name LIKE 'a%b'");
        assert_eq!(norm("name LIKE 'a_'"), "name LIKE 'a_'");
        assert_eq!(norm("name LIKE '_bc'"), "name LIKE '_bc'");
        assert_eq!(norm("name LIKE '%'"), "name LIKE '%'");
    }

    #[test]
    fn negated_like_does_not_rewrite_but_double_negation_does() {
        assert_eq!(norm("name NOT LIKE 'D%'"), "name NOT LIKE 'D%'");
        assert_eq!(norm("NOT name LIKE 'D%'"), "name NOT LIKE 'D%'");
        assert_eq!(norm("NOT name NOT LIKE 'D%'"), "name >= 'D' AND name < 'E'");
    }

    #[test]
    fn prefix_successor_carries_over_maximal_code_points() {
        // a prefix ending in char::MAX carries into the previous character
        assert_eq!(
            norm(&format!("c LIKE 'a{}%'", '\u{10FFFF}')),
            format!("c >= 'a{}' AND c < 'b'", '\u{10FFFF}')
        );
        // a prefix of only maximal code points keeps just the lower bound
        assert_eq!(
            norm(&format!("c LIKE '{}%'", '\u{10FFFF}')),
            format!("c >= '{}'", '\u{10FFFF}')
        );
        // the successor skips the surrogate gap
        assert_eq!(
            norm(&format!("c LIKE '{}%'", '\u{D7FF}')),
            format!("c >= '{}' AND c < '{}'", '\u{D7FF}', '\u{E000}')
        );
    }

    #[test]
    fn prefix_rewrite_is_gated_on_string_columns() {
        // The lexicographic range only means LIKE on strings; a non-string
        // column keeps its structural Like for downstream interpreters.
        assert_eq!(
            p("year LIKE '20%'").normalized_with(|_| false).to_string(),
            "year LIKE '20%'"
        );
        assert_eq!(
            p("year LIKE '1999'").normalized_with(|_| false).to_string(),
            "year LIKE '1999'"
        );
        assert_eq!(
            p("year LIKE '20%'").normalized_with(|_| true).to_string(),
            "year >= '20' AND year < '21'"
        );
    }

    #[test]
    fn like_variants_outside_the_vocabulary_become_unsupported() {
        let escaped = p("name LIKE 'D!%x' ESCAPE '!'");
        match &escaped {
            Pred::Unsupported { reason, .. } => assert!(reason.contains("ESCAPE")),
            other => panic!("expected Unsupported, got {other:?}"),
        }
        assert!(p("name LIKE country").contains_unsupported());
        assert!(p("'D' LIKE 'D%'").contains_unsupported());
        assert!(p("name ILIKE 'd%'").contains_unsupported());
    }

    #[test]
    fn mixed_precedence_without_parens_follows_sql_rules() {
        // AND binds tighter than OR: a OR (b AND c).
        let pred = p("a = 1 OR b = 2 AND c = 3");
        match &pred {
            Pred::Or(v) => {
                assert_eq!(v.len(), 2);
                assert!(matches!(v[1], Pred::And(_)));
            }
            other => panic!("expected Or, got {other:?}"),
        }
    }
}
