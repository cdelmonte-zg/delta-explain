use delta_kernel::expressions::{DecimalData, Expression, Scalar};
use delta_kernel::schema::{DataType, PrimitiveType};

use crate::v2::analysis::predicate::Literal;

use crate::v2::analysis::value_coercion::{
    parse_date_days, parse_decimal_bits, parse_timestamp_micros, parse_timestamp_ntz_micros,
};

/// Convert an owned SQL literal into a typed kernel expression.
///
/// `type_hint` is normally the type of the column being compared against.
/// It is used to avoid mismatched kernel comparisons such as:
///
/// `Float64 > Int32`
///
/// and to coerce quoted temporal literals to DATE/TIMESTAMP values.
pub(super) fn literal_expr(
    literal: &Literal,
    type_hint: Option<&DataType>,
) -> Result<Expression, String> {
    match literal {
        Literal::Number(value) => number_literal(value, type_hint),

        Literal::Str(value) => {
            // A quoted literal compared to a temporal column is a date or
            // timestamp in disguise. Coerce it so the kernel compares
            // values of the same type.
            if let Some(data_type) = type_hint
                && let Some(expr) = coerce_string_literal(value, data_type)?
            {
                return Ok(expr);
            }

            Ok(Expression::literal(value.clone()))
        }

        Literal::Bool(value) => Ok(Expression::literal(*value)),

        // NULL must carry the type of the column it is compared with.
        //
        // STRING is only the fallback when no type information is
        // available.
        Literal::Null => Ok(Expression::null_literal(
            type_hint.cloned().unwrap_or(DataType::STRING),
        )),

        // The column type wins when available. For example, a SQL DATE
        // literal compared to TIMESTAMP should be emitted as the column
        // type expected by the kernel.
        Literal::Date(value) => {
            if let Some(data_type) = type_hint
                && let Some(expr) = coerce_string_literal(value, data_type)?
            {
                return Ok(expr);
            }

            Ok(Expression::literal(Scalar::Date(parse_date_days(value)?)))
        }

        Literal::Timestamp(value) => {
            if let Some(data_type) = type_hint
                && let Some(expr) = coerce_string_literal(value, data_type)?
            {
                return Ok(expr);
            }

            Ok(Expression::literal(Scalar::Timestamp(
                parse_timestamp_micros(value)?,
            )))
        }
    }
}

fn number_literal(value: &str, type_hint: Option<&DataType>) -> Result<Expression, String> {
    if let Some(DataType::Primitive(primitive)) = type_hint {
        // Exhaustive intentionally.
        //
        // If delta-kernel adds a primitive type, compilation should force
        // us to decide explicitly how numeric literals interact with it.
        match primitive {
            PrimitiveType::Double => {
                let parsed: f64 = value
                    .parse()
                    .map_err(|e| format!("Invalid double '{value}': {e}"))?;

                return Ok(Expression::literal(parsed));
            }

            PrimitiveType::Float => {
                let parsed: f32 = value
                    .parse()
                    .map_err(|e| format!("Invalid float '{value}': {e}"))?;

                return Ok(Expression::literal(parsed));
            }

            PrimitiveType::Long => {
                let parsed: i64 = value
                    .parse()
                    .map_err(|e| format!("Invalid long '{value}': {e}"))?;

                return Ok(Expression::literal(parsed));
            }

            PrimitiveType::Short => {
                let parsed: i16 = value
                    .parse()
                    .map_err(|e| format!("Invalid short '{value}': {e}"))?;

                return Ok(Expression::literal(parsed));
            }

            PrimitiveType::Byte => {
                let parsed: i8 = value
                    .parse()
                    .map_err(|e| format!("Invalid byte '{value}': {e}"))?;

                return Ok(Expression::literal(parsed));
            }

            PrimitiveType::Decimal(decimal_type) => {
                let bits = parse_decimal_bits(value, decimal_type)?;

                let decimal = DecimalData::try_new(bits, *decimal_type).map_err(|e| {
                    format!(
                        "Decimal literal '{value}' does not \
                 fit the column type: {e}"
                    )
                })?;

                return Ok(Expression::literal(Scalar::Decimal(decimal)));
            }

            // INTEGER naturally follows the i32-first fallback below.
            //
            // The remaining primitive types do not define a better
            // interpretation of a numeric SQL literal.
            PrimitiveType::Integer
            | PrimitiveType::String
            | PrimitiveType::Boolean
            | PrimitiveType::Binary
            | PrimitiveType::Date
            | PrimitiveType::Timestamp
            | PrimitiveType::TimestampNtz
            | PrimitiveType::Void
            | PrimitiveType::IntervalYearMonth
            | PrimitiveType::IntervalDayTime => {}
        }
    }

    // Default lexical inference:
    //
    // i32 → i64 → f64
    if let Ok(parsed) = value.parse::<i32>() {
        Ok(Expression::literal(parsed))
    } else if let Ok(parsed) = value.parse::<i64>() {
        Ok(Expression::literal(parsed))
    } else if let Ok(parsed) = value.parse::<f64>() {
        Ok(Expression::literal(parsed))
    } else {
        Err(format!("Cannot parse number: '{value}'"))
    }
}

/// Coerce a quoted SQL string into the temporal type declared by the
/// compared column.
///
/// Returns `Ok(None)` for non-temporal types so the caller can preserve the
/// normal string literal.
fn coerce_string_literal(text: &str, type_hint: &DataType) -> Result<Option<Expression>, String> {
    let DataType::Primitive(primitive) = type_hint else {
        return Ok(None);
    };

    match primitive {
        PrimitiveType::Date => Ok(Some(Expression::literal(Scalar::Date(parse_date_days(
            text,
        )?)))),

        PrimitiveType::Timestamp => Ok(Some(Expression::literal(Scalar::Timestamp(
            parse_timestamp_micros(text)?,
        )))),

        PrimitiveType::TimestampNtz => Ok(Some(Expression::literal(Scalar::TimestampNtz(
            parse_timestamp_ntz_micros(text)?,
        )))),

        PrimitiveType::String
        | PrimitiveType::Long
        | PrimitiveType::Integer
        | PrimitiveType::Short
        | PrimitiveType::Byte
        | PrimitiveType::Float
        | PrimitiveType::Double
        | PrimitiveType::Boolean
        | PrimitiveType::Binary
        | PrimitiveType::Decimal(_)
        | PrimitiveType::Void
        | PrimitiveType::IntervalYearMonth
        | PrimitiveType::IntervalDayTime => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use delta_kernel::schema::DecimalType;

    use super::*;

    #[test]
    fn null_literal_takes_column_type() {
        let integer_type = DataType::Primitive(PrimitiveType::Integer);

        match literal_expr(&Literal::Null, Some(&integer_type)) {
            Ok(Expression::Literal(Scalar::Null(data_type))) => {
                assert_eq!(data_type, integer_type);
            }

            other => {
                panic!("unexpected: {other:?}");
            }
        }

        match literal_expr(&Literal::Null, None) {
            Ok(Expression::Literal(Scalar::Null(data_type))) => {
                assert_eq!(data_type, DataType::STRING);
            }

            other => {
                panic!("unexpected: {other:?}");
            }
        }
    }

    #[test]
    fn integer_hint_emits_int32_literal() {
        let integer_type = DataType::Primitive(PrimitiveType::Integer);

        match literal_expr(&Literal::Number("30".into()), Some(&integer_type)) {
            Ok(Expression::Literal(Scalar::Integer(value))) => {
                assert_eq!(value, 30);
            }

            other => {
                panic!("unexpected: {other:?}");
            }
        }
    }

    #[test]
    fn long_hint_emits_int64_literal() {
        let long_type = DataType::Primitive(PrimitiveType::Long);

        match literal_expr(&Literal::Number("30".into()), Some(&long_type)) {
            Ok(Expression::Literal(Scalar::Long(value))) => {
                assert_eq!(value, 30);
            }

            other => {
                panic!("unexpected: {other:?}");
            }
        }
    }

    #[test]
    fn quoted_date_is_coerced_for_date_column() {
        let date_type = DataType::Primitive(PrimitiveType::Date);

        match literal_expr(&Literal::Str("1970-01-02".into()), Some(&date_type)) {
            Ok(Expression::Literal(Scalar::Date(days))) => {
                assert_eq!(days, 1);
            }

            other => {
                panic!("unexpected: {other:?}");
            }
        }
    }

    #[test]
    fn decimal_literal_is_materialized_for_decimal_column() {
        let decimal_type = DecimalType::try_new(9, 2).unwrap();

        let data_type = DataType::Primitive(PrimitiveType::Decimal(decimal_type));

        match literal_expr(&Literal::Number("100.50".into()), Some(&data_type)) {
            Ok(Expression::Literal(Scalar::Decimal(decimal))) => {
                assert_eq!(decimal.bits(), 10_050);
            }

            other => {
                panic!("unexpected: {other:?}");
            }
        }
    }
}
