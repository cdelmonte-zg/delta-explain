use delta_kernel::expressions::{Expression, Predicate, Scalar};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType, SchemaRef};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, TypedString, UnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::Error;

/// Parse a SQL WHERE-clause expression into a delta-kernel Predicate.
///
/// Supports: comparisons, AND, OR, NOT, IN (...), BETWEEN, IS [NOT] NULL,
/// parentheses, nested column references (a.b), and standard SQL literals.
/// The conversion helpers below carry plain `String` errors; this boundary
/// wraps them into [`Error::Predicate`].
pub fn parse_predicate(input: &str, schema: &SchemaRef) -> Result<Predicate, Error> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(input)
        .map_err(|e| Error::Predicate(format!("Parse error: {e}")))?;
    let sql_expr = parser
        .parse_expr()
        .map_err(|e| Error::Predicate(format!("Parse error: {e}")))?;

    convert_to_predicate(&sql_expr, schema).map_err(Error::Predicate)
}

// ── SQL AST -> kernel Predicate ─────────────────────────────────────

fn convert_to_predicate(expr: &SqlExpr, schema: &SchemaRef) -> Result<Predicate, String> {
    match expr {
        // AND / OR
        SqlExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let l = convert_to_predicate(left, schema)?;
                let r = convert_to_predicate(right, schema)?;
                Ok(Predicate::and(l, r))
            }
            BinaryOperator::Or => {
                let l = convert_to_predicate(left, schema)?;
                let r = convert_to_predicate(right, schema)?;
                Ok(Predicate::or(l, r))
            }
            // Comparison operators
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                let (l, r) = convert_comparison_pair(left, right, schema)?;
                match op {
                    BinaryOperator::Eq => Ok(l.eq(r)),
                    BinaryOperator::NotEq => Ok(l.ne(r)),
                    BinaryOperator::Lt => Ok(l.lt(r)),
                    BinaryOperator::LtEq => Ok(l.le(r)),
                    BinaryOperator::Gt => Ok(l.gt(r)),
                    BinaryOperator::GtEq => Ok(l.ge(r)),
                    _ => unreachable!(),
                }
            }
            other => Err(format!("Unsupported binary operator: {other}")),
        },

        // NOT
        SqlExpr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => {
            let inner = convert_to_predicate(expr, schema)?;
            Ok(Predicate::not(inner))
        }

        // IS NULL / IS NOT NULL
        SqlExpr::IsNull(expr) => {
            let e = convert_to_expression(expr, schema)?;
            Ok(e.is_null())
        }
        SqlExpr::IsNotNull(expr) => {
            let e = convert_to_expression(expr, schema)?;
            Ok(e.is_not_null())
        }

        // IN (val1, val2, ...)  ->  col = val1 OR col = val2 OR ...
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            if list.is_empty() {
                return Err("Empty IN list".into());
            }
            let col_type = resolve_column_type(expr, schema);
            let col = convert_to_expression(expr, schema)?;
            let preds: Vec<Predicate> = list
                .iter()
                .map(|item| {
                    let val = convert_typed_expression(item, schema, col_type.as_ref())?;
                    Ok(col.clone().eq(val))
                })
                .collect::<Result<Vec<_>, String>>()?;

            let combined = if preds.len() == 1 {
                preds.into_iter().next().unwrap()
            } else {
                Predicate::or_from(preds)
            };

            if *negated {
                Ok(Predicate::not(combined))
            } else {
                Ok(combined)
            }
        }

        // BETWEEN low AND high  ->  col >= low AND col <= high
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let col_type = resolve_column_type(expr, schema);
            let col = convert_to_expression(expr, schema)?;
            let lo = convert_typed_expression(low, schema, col_type.as_ref())?;
            let hi = convert_typed_expression(high, schema, col_type.as_ref())?;
            let between = Predicate::and(col.clone().ge(lo), col.le(hi));
            if *negated {
                Ok(Predicate::not(between))
            } else {
                Ok(between)
            }
        }

        // Nested parens
        SqlExpr::Nested(inner) => convert_to_predicate(inner, schema),

        // Boolean column reference
        SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
            let col = convert_to_expression(expr, schema)?;
            Ok(Predicate::from_expr(col))
        }

        other => Err(format!("Unsupported expression: {other}")),
    }
}

// ── Type-aware conversion helpers ────────────────────────────────────

/// For a comparison like `col > 42`, resolve the column type from one side
/// and use it to coerce the literal on the other side.
fn convert_comparison_pair(
    left: &SqlExpr,
    right: &SqlExpr,
    schema: &SchemaRef,
) -> Result<(Expression, Expression), String> {
    let left_type = resolve_column_type(left, schema);
    let right_type = resolve_column_type(right, schema);
    let hint = left_type.or(right_type);

    let l = convert_typed_expression(left, schema, hint.as_ref())?;
    let r = convert_typed_expression(right, schema, hint.as_ref())?;
    Ok((l, r))
}

/// Try to determine the DataType of a column reference in the schema.
fn resolve_column_type(expr: &SqlExpr, schema: &SchemaRef) -> Option<DataType> {
    match expr {
        SqlExpr::Identifier(ident) => schema.field(&ident.value).map(|f| f.data_type().clone()),
        SqlExpr::CompoundIdentifier(parts) => {
            // Walk the dotted path (profile.score) through struct fields so the
            // literal on the other side coerces to the leaf type. Without this,
            // a nested double compared to an integer literal aborts the scan
            // ("Invalid comparison operation: Float64 > Int32").
            let mut current = schema.field(&parts[0].value)?.data_type().clone();
            for part in &parts[1..] {
                let DataType::Struct(st) = &current else {
                    return None;
                };
                current = st.field(&part.value)?.data_type().clone();
            }
            Some(current)
        }
        SqlExpr::Nested(inner) => resolve_column_type(inner, schema),
        _ => None,
    }
}

/// Convert an expression, using a type hint to coerce numeric literals.
fn convert_typed_expression(
    expr: &SqlExpr,
    schema: &SchemaRef,
    type_hint: Option<&DataType>,
) -> Result<Expression, String> {
    match expr {
        SqlExpr::Value(val_with_span) => convert_literal(&val_with_span.value, type_hint),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => {
            let e = convert_typed_expression(inner, schema, type_hint)?;
            negate_literal(e)
        }
        SqlExpr::Nested(inner) => convert_typed_expression(inner, schema, type_hint),
        SqlExpr::TypedString(ts) => convert_typed_string(ts, type_hint),
        _ => convert_to_expression(expr, schema),
    }
}

/// Convert a typed literal like `DATE '2026-07-01'` or `TIMESTAMP '...'`.
/// The column's type hint wins when present (it is the type the kernel will
/// compare against); the declared SQL type only picks the parser otherwise.
fn convert_typed_string(
    ts: &TypedString,
    type_hint: Option<&DataType>,
) -> Result<Expression, String> {
    use sqlparser::ast::DataType as SqlType;
    let Some(text) = ts.value.clone().into_string() else {
        return Err(format!("Unsupported typed literal: {ts}"));
    };
    if let Some(dt) = type_hint
        && let Some(expr) = coerce_string_literal(&text, dt)?
    {
        return Ok(expr);
    }
    match &ts.data_type {
        SqlType::Date => Ok(Expression::literal(Scalar::Date(parse_date_days(&text)?))),
        SqlType::Timestamp(_, _) => Ok(Expression::literal(Scalar::Timestamp(
            parse_timestamp_micros(&text)?,
        ))),
        other => Err(format!("Unsupported typed literal type: {other}")),
    }
}

fn negate_literal(expr: Expression) -> Result<Expression, String> {
    match expr {
        Expression::Literal(scalar) => {
            use delta_kernel::expressions::Scalar;
            match scalar {
                Scalar::Integer(v) => Ok(Expression::literal(-v)),
                Scalar::Long(v) => Ok(Expression::literal(-v)),
                Scalar::Short(v) => Ok(Expression::literal(-v)),
                Scalar::Byte(v) => Ok(Expression::literal(-v)),
                Scalar::Float(v) => Ok(Expression::literal(-v)),
                Scalar::Double(v) => Ok(Expression::literal(-v)),
                _ => Err(format!("Cannot negate: {scalar:?}")),
            }
        }
        _ => Err("Unary minus only supported on literals".into()),
    }
}

// ── SQL AST -> kernel Expression ────────────────────────────────────

fn convert_to_expression(expr: &SqlExpr, _schema: &SchemaRef) -> Result<Expression, String> {
    match expr {
        // Simple column: age, country
        SqlExpr::Identifier(ident) => Ok(Expression::column([ident.value.clone()])),

        // Dotted column: payload.age
        SqlExpr::CompoundIdentifier(parts) => {
            let names: Vec<String> = parts.iter().map(|p| p.value.clone()).collect();
            Ok(Expression::column(names))
        }

        // Literal values (no type hint when used outside a comparison)
        SqlExpr::Value(val_with_span) => convert_literal(&val_with_span.value, None),

        // Nested parens
        SqlExpr::Nested(inner) => convert_to_expression(inner, _schema),

        // Unary minus: -42
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let inner = convert_to_expression(expr, _schema)?;
            negate_literal(inner)
        }

        other => Err(format!("Unsupported expression: {other}")),
    }
}

fn convert_literal(val: &SqlValue, type_hint: Option<&DataType>) -> Result<Expression, String> {
    match val {
        SqlValue::Number(s, _) => {
            // If we have a type hint from the column, use it to parse the right type
            if let Some(DataType::Primitive(p)) = type_hint {
                match p {
                    PrimitiveType::Double => {
                        let v: f64 = s
                            .parse()
                            .map_err(|e| format!("Invalid double '{s}': {e}"))?;
                        return Ok(Expression::literal(v));
                    }
                    PrimitiveType::Float => {
                        let v: f32 = s.parse().map_err(|e| format!("Invalid float '{s}': {e}"))?;
                        return Ok(Expression::literal(v));
                    }
                    PrimitiveType::Long => {
                        let v: i64 = s.parse().map_err(|e| format!("Invalid long '{s}': {e}"))?;
                        return Ok(Expression::literal(v));
                    }
                    PrimitiveType::Short => {
                        let v: i16 = s.parse().map_err(|e| format!("Invalid short '{s}': {e}"))?;
                        return Ok(Expression::literal(v));
                    }
                    PrimitiveType::Byte => {
                        let v: i8 = s.parse().map_err(|e| format!("Invalid byte '{s}': {e}"))?;
                        return Ok(Expression::literal(v));
                    }
                    PrimitiveType::Decimal(dt) => {
                        return Ok(Expression::literal(parse_decimal(s, dt)?));
                    }
                    _ => {}
                }
            }
            // Default: try i32, then i64, then f64
            if let Ok(v) = s.parse::<i32>() {
                Ok(Expression::literal(v))
            } else if let Ok(v) = s.parse::<i64>() {
                Ok(Expression::literal(v))
            } else if let Ok(v) = s.parse::<f64>() {
                Ok(Expression::literal(v))
            } else {
                Err(format!("Cannot parse number: '{s}'"))
            }
        }
        SqlValue::SingleQuotedString(s) => {
            // A quoted literal compared to a temporal column is a date or
            // timestamp in disguise: coerce it, or the kernel sees a type
            // mismatch and conservatively keeps every file.
            if let Some(dt) = type_hint
                && let Some(expr) = coerce_string_literal(s, dt)?
            {
                return Ok(expr);
            }
            Ok(Expression::literal(s.clone()))
        }
        SqlValue::Boolean(b) => Ok(Expression::literal(*b)),
        SqlValue::Null => Ok(Expression::null_literal(DataType::STRING)),
        other => Err(format!("Unsupported literal: {other}")),
    }
}

/// Coerce a quoted string literal to the column's temporal type, if it has
/// one. Returns `Ok(None)` when the hint is not temporal, so the caller keeps
/// the plain string literal.
fn coerce_string_literal(text: &str, hint: &DataType) -> Result<Option<Expression>, String> {
    let DataType::Primitive(p) = hint else {
        return Ok(None);
    };
    match p {
        PrimitiveType::Date => Ok(Some(Expression::literal(Scalar::Date(parse_date_days(
            text,
        )?)))),
        PrimitiveType::Timestamp => Ok(Some(Expression::literal(Scalar::Timestamp(
            parse_timestamp_micros(text)?,
        )))),
        PrimitiveType::TimestampNtz => Ok(Some(Expression::literal(Scalar::TimestampNtz(
            parse_timestamp_micros(text)?,
        )))),
        _ => Ok(None),
    }
}

/// Days since the Unix epoch for a `YYYY-MM-DD` string.
fn parse_date_days(text: &str) -> Result<i32, String> {
    const EPOCH_DAYS_FROM_CE: i32 = 719_163; // 1970-01-01 in chrono's day count
    let d = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|e| format!("Invalid date '{text}' (expected YYYY-MM-DD): {e}"))?;
    Ok(chrono::Datelike::num_days_from_ce(&d) - EPOCH_DAYS_FROM_CE)
}

/// Microseconds since the Unix epoch for a timestamp string. Accepts RFC 3339
/// (with offset), `YYYY-MM-DD[ T]HH:MM:SS[.ffffff]` treated as UTC, and a bare
/// date as midnight UTC. Delta TIMESTAMP is UTC-normalized; TIMESTAMP_NTZ
/// reuses the same wall-clock parse.
fn parse_timestamp_micros(text: &str) -> Result<i64, String> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(text) {
        return Ok(dt.timestamp_micros());
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, fmt) {
            return Ok(ndt.and_utc().timestamp_micros());
        }
    }
    if let Ok(days) = parse_date_days(text) {
        return Ok(i64::from(days) * 86_400_000_000);
    }
    Err(format!(
        "Invalid timestamp '{text}': expected YYYY-MM-DD[ HH:MM:SS[.ffffff]][+HH:MM]"
    ))
}

/// Parse a numeric literal into a decimal scaled to the column's type.
/// The literal must fit the column scale exactly; silently rounding a
/// predicate bound would change its meaning.
fn parse_decimal(text: &str, dt: &DecimalType) -> Result<Scalar, String> {
    let (int_part, frac_part) = match text.split_once('.') {
        Some((i, f)) => (i, f),
        None => (text, ""),
    };
    if frac_part.len() > dt.scale() as usize {
        return Err(format!(
            "Decimal literal '{text}' has more fractional digits than the column scale {}",
            dt.scale()
        ));
    }
    let mut digits = String::with_capacity(int_part.len() + dt.scale() as usize);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in frac_part.len()..dt.scale() as usize {
        digits.push('0');
    }
    let bits: i128 = digits
        .parse()
        .map_err(|e| format!("Invalid decimal '{text}': {e}"))?;
    delta_kernel::expressions::DecimalData::try_new(bits, *dt)
        .map(Scalar::Decimal)
        .map_err(|e| format!("Decimal literal '{text}' does not fit the column type: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_days_since_epoch() {
        assert_eq!(parse_date_days("1970-01-01"), Ok(0));
        assert_eq!(parse_date_days("1970-01-02"), Ok(1));
        assert_eq!(parse_date_days("1969-12-31"), Ok(-1));
        assert_eq!(parse_date_days("2026-07-01"), Ok(20_635));
        assert!(parse_date_days("2026-13-01").is_err());
        assert!(parse_date_days("not-a-date").is_err());
    }

    #[test]
    fn timestamp_micros_forms() {
        assert_eq!(parse_timestamp_micros("1970-01-01 00:00:01"), Ok(1_000_000));
        // an explicit offset normalizes to UTC
        assert_eq!(parse_timestamp_micros("1970-01-01T01:00:00+01:00"), Ok(0));
        // fractional seconds
        assert_eq!(parse_timestamp_micros("1970-01-01 00:00:00.000042"), Ok(42));
        // bare date is midnight UTC
        assert_eq!(parse_timestamp_micros("1970-01-02"), Ok(86_400_000_000));
        assert!(parse_timestamp_micros("teatime").is_err());
    }

    #[test]
    fn decimal_scaling() {
        let dt = DecimalType::try_new(9, 2).unwrap();
        let as_bits = |s: &str| match parse_decimal(s, &dt) {
            Ok(Scalar::Decimal(d)) => d.bits(),
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(as_bits("100.50"), 10_050);
        assert_eq!(as_bits("100.5"), 10_050);
        assert_eq!(as_bits("100"), 10_000);
        assert!(parse_decimal("1.234", &dt).is_err()); // scale overflow
        assert!(parse_decimal("abc", &dt).is_err());
    }
}
