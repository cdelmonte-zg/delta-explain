use delta_kernel::expressions::{Expression, Predicate};
use delta_kernel::schema::{DataType, SchemaRef};
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, UnaryOperator, Value as SqlValue};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Parse a SQL WHERE-clause expression into a delta-kernel Predicate.
///
/// Supports: comparisons, AND, OR, NOT, IN (...), BETWEEN, IS [NOT] NULL,
/// parentheses, nested column references (a.b), and standard SQL literals.
pub fn parse_predicate(input: &str, schema: &SchemaRef) -> Result<Predicate, String> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(input)
        .map_err(|e| format!("Parse error: {e}"))?;
    let sql_expr = parser
        .parse_expr()
        .map_err(|e| format!("Parse error: {e}"))?;

    convert_to_predicate(&sql_expr, schema)
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
        SqlExpr::CompoundIdentifier(parts) if parts.len() == 1 => {
            schema.field(&parts[0].value).map(|f| f.data_type().clone())
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
        _ => convert_to_expression(expr, schema),
    }
}

fn negate_literal(expr: Expression) -> Result<Expression, String> {
    match expr {
        Expression::Literal(scalar) => {
            use delta_kernel::expressions::Scalar;
            match scalar {
                Scalar::Integer(v) => Ok(Expression::literal(-v)),
                Scalar::Long(v) => Ok(Expression::literal(-v)),
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
            if let Some(dt) = type_hint {
                if *dt == DataType::DOUBLE {
                    let v: f64 = s
                        .parse()
                        .map_err(|e| format!("Invalid double '{s}': {e}"))?;
                    return Ok(Expression::literal(v));
                } else if *dt == DataType::FLOAT {
                    let v: f32 = s.parse().map_err(|e| format!("Invalid float '{s}': {e}"))?;
                    return Ok(Expression::literal(v));
                } else if *dt == DataType::LONG {
                    let v: i64 = s.parse().map_err(|e| format!("Invalid long '{s}': {e}"))?;
                    return Ok(Expression::literal(v));
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
        SqlValue::SingleQuotedString(s) => Ok(Expression::literal(s.clone())),
        SqlValue::Boolean(b) => Ok(Expression::literal(*b)),
        SqlValue::Null => Ok(Expression::null_literal(DataType::STRING)),
        other => Err(format!("Unsupported literal: {other}")),
    }
}
