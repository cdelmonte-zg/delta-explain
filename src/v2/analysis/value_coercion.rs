use delta_kernel::schema::DecimalType;

/// Parse `YYYY-MM-DD` into Delta's DATE representation:
/// days since 1970-01-01.
pub(super) fn parse_date_days(text: &str) -> Result<i32, String> {
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
pub(super) fn parse_timestamp_micros(text: &str) -> Result<i64, String> {
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
pub(super) fn parse_timestamp_ntz_micros(text: &str) -> Result<i64, String> {
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
pub(super) fn parse_decimal_bits(text: &str, decimal_type: &DecimalType) -> Result<i128, String> {
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
