use delta_kernel::expressions::{DecimalData, Expression, Scalar};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType};

use crate::v2::analysis::predicate::Literal;

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

/// Parse `YYYY-MM-DD` into Delta's DATE representation:
/// days since 1970-01-01.
fn parse_date_days(text: &str) -> Result<i32, String> {
    const EPOCH_DAYS_FROM_CE: i32 = 719_163;

    let date = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|e| {
        format!(
            "Invalid date '{text}' \
                 (expected YYYY-MM-DD): {e}"
        )
    })?;

    Ok(chrono::Datelike::num_days_from_ce(&date) - EPOCH_DAYS_FROM_CE)
}

/// Parse a Delta TIMESTAMP into microseconds since the Unix epoch.
///
/// Explicit offsets are normalized to UTC. Naive timestamps are interpreted
/// as UTC. A bare date means midnight UTC.
fn parse_timestamp_micros(text: &str) -> Result<i64, String> {
    if let Ok(date_time) = chrono::DateTime::parse_from_rfc3339(text) {
        return Ok(date_time.timestamp_micros());
    }

    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(date_time) = chrono::NaiveDateTime::parse_from_str(text, format) {
            return Ok(date_time.and_utc().timestamp_micros());
        }
    }

    if let Ok(days) = parse_date_days(text) {
        return Ok(i64::from(days) * 86_400_000_000);
    }

    Err(format!(
        "Invalid timestamp '{text}': expected \
         YYYY-MM-DD[ HH:MM:SS[.ffffff]][+HH:MM]"
    ))
}

/// Parse a Delta TIMESTAMP_NTZ into wall-clock microseconds.
///
/// TIMESTAMP_NTZ is timezone-naive, therefore explicit offsets are rejected.
fn parse_timestamp_ntz_micros(text: &str) -> Result<i64, String> {
    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(date_time) = chrono::NaiveDateTime::parse_from_str(text, format) {
            return Ok(date_time.and_utc().timestamp_micros());
        }
    }

    if let Ok(days) = parse_date_days(text) {
        return Ok(i64::from(days) * 86_400_000_000);
    }

    if chrono::DateTime::parse_from_rfc3339(text).is_ok() {
        return Err(format!(
            "Timestamp '{text}' carries an offset, \
             but TIMESTAMP_NTZ is timezone-naive: \
             drop the offset, or compare against a \
             TIMESTAMP column"
        ));
    }

    Err(format!(
        "Invalid timestamp '{text}': expected \
         YYYY-MM-DD[ HH:MM:SS[.ffffff]] \
         (no offset)"
    ))
}

/// Parse a lexical decimal literal into its unscaled integer representation.
///
/// Example:
///
/// DECIMAL(9, 2)
/// "100.50" -> 10050
///
/// The value must fit the declared scale exactly. No rounding is performed,
/// because rounding a predicate bound would change its semantics.
///
/// Precision validation remains the responsibility of the consumer that
/// materializes the final typed decimal value.
fn parse_decimal_bits(text: &str, decimal_type: &DecimalType) -> Result<i128, String> {
    let (integer_part, fractional_part) = match text.split_once('.') {
        Some((integer, fractional)) => (integer, fractional),

        None => (text, ""),
    };

    if fractional_part.len() > decimal_type.scale() as usize {
        return Err(format!(
            "Decimal literal '{text}' has more \
             fractional digits than the column \
             scale {}",
            decimal_type.scale()
        ));
    }

    let mut digits = String::with_capacity(integer_part.len() + decimal_type.scale() as usize);

    digits.push_str(integer_part);
    digits.push_str(fractional_part);

    for _ in fractional_part.len()..decimal_type.scale() as usize {
        digits.push('0');
    }

    digits
        .parse::<i128>()
        .map_err(|e| format!("Invalid decimal '{text}': {e}"))
}

#[cfg(test)]
mod tests {
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

        assert_eq!(parse_timestamp_micros("1970-01-01T01:00:00+01:00"), Ok(0));

        assert_eq!(parse_timestamp_micros("1970-01-01 00:00:00.000042"), Ok(42));

        assert_eq!(parse_timestamp_micros("1970-01-02"), Ok(86_400_000_000));

        assert!(parse_timestamp_micros("teatime").is_err());
    }

    #[test]
    fn ntz_is_wall_clock_and_rejects_offsets() {
        assert_eq!(
            parse_timestamp_ntz_micros("1970-01-01 01:00:00"),
            Ok(3_600_000_000)
        );

        let error = parse_timestamp_ntz_micros("1970-01-01T01:00:00+01:00");

        assert!(error.is_err_and(|message| { message.contains("timezone-naive") }));
    }

    #[test]
    fn decimal_scaling() {
        let decimal_type = DecimalType::try_new(9, 2).unwrap();

        assert_eq!(parse_decimal_bits("100.50", &decimal_type,), Ok(10_050));

        assert_eq!(parse_decimal_bits("100.5", &decimal_type,), Ok(10_050));

        assert_eq!(parse_decimal_bits("100", &decimal_type,), Ok(10_000));

        assert!(parse_decimal_bits("1.234", &decimal_type,).is_err());

        assert!(parse_decimal_bits("abc", &decimal_type,).is_err());
    }

    #[test]
    fn negative_decimal_scales_correctly() {
        let decimal_type = DecimalType::try_new(9, 2).unwrap();

        assert_eq!(parse_decimal_bits("-100.50", &decimal_type,), Ok(-10_050));
    }
}
