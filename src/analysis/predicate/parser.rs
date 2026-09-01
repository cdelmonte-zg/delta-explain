use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, UnaryOperator, Value as SqlValue};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::Error;

use super::model::{CmpOp, ColRef, Literal, Pred};

/// Parse a SQL WHERE-clause expression into delta-explain's owned
/// predicate model.
///
/// Only malformed SQL produces an error here. Expressions that are valid
/// SQL but cannot be represented by the pruning language are converted
/// into [`Pred::Unsupported`].
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

                    _ => {
                        return unsupported(expr, format!("Unsupported binary operator: {op}"));
                    }
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

        SqlExpr::IsDistinctFrom(left, right) => convert_distinct(expr, left, right, false),

        SqlExpr::IsNotDistinctFrom(left, right) => convert_distinct(expr, left, right, true),

        SqlExpr::InList {
            expr: lhs,
            list,
            negated,
        } => convert_in_list(expr, lhs, list, *negated),

        SqlExpr::Between {
            expr: lhs,
            negated,
            low,
            high,
        } => convert_between(expr, lhs, low, high, *negated),

        SqlExpr::Like {
            negated,
            any,
            expr: lhs,
            pattern,
            escape_char,
        } => convert_like(expr, lhs, pattern, *negated, *any, escape_char.is_some()),

        SqlExpr::Nested(inner) => convert(inner),

        SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => match operand(expr) {
            Ok(Operand::Col(col)) => Pred::BoolCol(col),

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

fn convert_distinct(whole: &SqlExpr, left: &SqlExpr, right: &SqlExpr, negated: bool) -> Pred {
    // DISTINCT is symmetric, so a literal-first expression needs no
    // operator reversal.
    match (operand(left), operand(right)) {
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

fn convert_in_list(whole: &SqlExpr, lhs: &SqlExpr, list: &[SqlExpr], negated: bool) -> Pred {
    if list.is_empty() {
        return unsupported(whole, "Empty IN list".into());
    }

    let col = match operand(lhs) {
        Ok(Operand::Col(col)) => col,

        Ok(Operand::Lit(_)) => {
            return unsupported(whole, format!("IN requires a column, got: {lhs}"));
        }

        Err(reason) => {
            return unsupported(whole, reason);
        }
    };

    let mut items = Vec::with_capacity(list.len());

    for item in list {
        match operand(item) {
            Ok(Operand::Lit(lit)) => {
                items.push(lit);
            }

            Ok(Operand::Col(_)) => {
                return unsupported(
                    whole,
                    format!("IN list items must be literals, got: {item}"),
                );
            }

            Err(reason) => {
                return unsupported(whole, reason);
            }
        }
    }

    Pred::In {
        col,
        list: items,
        negated,
    }
}

fn convert_between(
    whole: &SqlExpr,
    lhs: &SqlExpr,
    low: &SqlExpr,
    high: &SqlExpr,
    negated: bool,
) -> Pred {
    let col = match operand(lhs) {
        Ok(Operand::Col(col)) => col,

        Ok(Operand::Lit(_)) => {
            return unsupported(whole, format!("BETWEEN requires a column, got: {lhs}"));
        }

        Err(reason) => {
            return unsupported(whole, reason);
        }
    };

    let (low, high) = match (operand(low), operand(high)) {
        (Ok(Operand::Lit(low)), Ok(Operand::Lit(high))) => (low, high),

        (Ok(Operand::Col(_)), _) | (_, Ok(Operand::Col(_))) => {
            return unsupported(whole, "BETWEEN bounds must be literals".into());
        }

        (Err(reason), _) | (_, Err(reason)) => {
            return unsupported(whole, reason);
        }
    };

    Pred::Between {
        col,
        low,
        high,
        negated,
    }
}

fn convert_like(
    whole: &SqlExpr,
    lhs: &SqlExpr,
    pattern: &SqlExpr,
    negated: bool,
    any: bool,
    has_escape: bool,
) -> Pred {
    if any {
        return unsupported(whole, "LIKE ANY is not supported".into());
    }

    if has_escape {
        return unsupported(whole, "LIKE with an ESCAPE clause is not supported".into());
    }

    let col = match operand(lhs) {
        Ok(Operand::Col(col)) => col,

        Ok(Operand::Lit(_)) => {
            return unsupported(whole, format!("LIKE requires a column, got: {lhs}"));
        }

        Err(reason) => {
            return unsupported(whole, reason);
        }
    };

    match operand(pattern) {
        Ok(Operand::Lit(Literal::Str(pattern))) => Pred::Like {
            col,
            pattern,
            negated,
        },

        Ok(_) => unsupported(
            whole,
            format!("LIKE pattern must be a string literal, got: {pattern}"),
        ),

        Err(reason) => unsupported(whole, reason),
    }
}

enum Operand {
    Col(ColRef),
    Lit(Literal),
}

/// Convert a SQL expression that appears as an operand into either a
/// column reference or a literal.
///
/// An error means the expression falls outside delta-explain's pruning
/// language. The caller turns that reason into [`Pred::Unsupported`].
fn operand(expr: &SqlExpr) -> Result<Operand, String> {
    match expr {
        SqlExpr::Identifier(identifier) => Ok(Operand::Col(ColRef(vec![identifier.value.clone()]))),

        SqlExpr::CompoundIdentifier(parts) => Ok(Operand::Col(ColRef(
            parts.iter().map(|part| part.value.clone()).collect(),
        ))),

        SqlExpr::Value(value) => literal_from_value(&value.value).map(Operand::Lit),

        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => match operand(inner)? {
            Operand::Lit(Literal::Number(number)) => {
                let negated = match number.strip_prefix('-') {
                    Some(rest) => rest.to_string(),

                    None => {
                        format!("-{number}")
                    }
                };

                Ok(Operand::Lit(Literal::Number(negated)))
            }

            _ => Err("Unary minus only supported on numeric literals".into()),
        },

        SqlExpr::Nested(inner) => operand(inner),

        SqlExpr::TypedString(typed) => {
            use sqlparser::ast::DataType as SqlType;

            let Some(text) = typed.value.clone().into_string() else {
                return Err(format!("Unsupported typed literal: {typed}"));
            };

            match &typed.data_type {
                SqlType::Date => Ok(Operand::Lit(Literal::Date(text))),

                SqlType::Timestamp(_, _) => Ok(Operand::Lit(Literal::Timestamp(text))),

                other => Err(format!("Unsupported typed literal type: {other}")),
            }
        }

        other => Err(format!("Unsupported expression: {other}")),
    }
}

fn literal_from_value(value: &SqlValue) -> Result<Literal, String> {
    match value {
        SqlValue::Number(number, _) => Ok(Literal::Number(number.clone())),

        SqlValue::SingleQuotedString(value) => Ok(Literal::Str(value.clone())),

        SqlValue::Boolean(value) => Ok(Literal::Bool(*value)),

        SqlValue::Null => Ok(Literal::Null),

        other => Err(format!("Unsupported literal: {other}")),
    }
}

fn unsupported(expr: &SqlExpr, reason: String) -> Pred {
    Pred::Unsupported {
        raw: expr.to_string(),
        reason,
    }
}
