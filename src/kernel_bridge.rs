//! The only module that names delta-kernel's expression vocabulary.
//!
//! Two directions live here. The runtime direction lowers the owned AST
//! (`predicate_ast`) into `delta_kernel::expressions::Predicate`, coercing
//! lexical literals to the column types the schema declares. The sentinel
//! direction maps each kernel operator to a [`Capability`] tier through
//! exhaustive matches with no catch-all arm: when a kernel upgrade widens
//! an operator enum, these matches stop compiling and force a deliberate
//! decision instead of a silent gap.

use delta_kernel::expressions::{
    BinaryPredicateOp, Expression, JunctionPredicateOp, Predicate, Scalar, UnaryPredicateOp,
};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType, SchemaRef};

use crate::error::Error;
use crate::predicate_ast::{CmpOp, ColRef, Literal, Pred};

// ── Capability tiers ────────────────────────────────────────────────

/// How far the kernel can take a construct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// The data-skipping evaluator prunes on it via min/max stats.
    SkippingNative,
    /// The kernel represents and evaluates it (e.g. over partition values),
    /// but file statistics cannot prune on it.
    LanguageOnly,
}

pub fn binary_predicate_capability(op: &BinaryPredicateOp) -> Capability {
    match op {
        BinaryPredicateOp::Equal | BinaryPredicateOp::LessThan | BinaryPredicateOp::GreaterThan => {
            Capability::SkippingNative
        }
        BinaryPredicateOp::Distinct => Capability::LanguageOnly,
        // The kernel's binary IN gets no skipping; we expand SQL IN lists
        // into OR-of-equalities at emission, which does.
        BinaryPredicateOp::In => Capability::LanguageOnly,
    }
}

pub fn unary_predicate_capability(op: &UnaryPredicateOp) -> Capability {
    match op {
        UnaryPredicateOp::IsNull => Capability::SkippingNative,
    }
}

pub fn junction_predicate_capability(op: &JunctionPredicateOp) -> Capability {
    match op {
        JunctionPredicateOp::And | JunctionPredicateOp::Or => Capability::SkippingNative,
    }
}

// ── Emission: owned AST -> kernel Predicate ─────────────────────────

/// Lower the owned AST into a kernel predicate the scan builder can execute.
///
/// [`Pred::Unsupported`] leaves are fatal here: the caller decides whether
/// to emit a predicate containing one, not this function.
pub fn emit_predicate(pred: &Pred, schema: &SchemaRef) -> Result<Predicate, Error> {
    emit(pred, schema).map_err(Error::Predicate)
}

fn emit(pred: &Pred, schema: &SchemaRef) -> Result<Predicate, String> {
    match pred {
        Pred::And(parts) => {
            let emitted: Vec<Predicate> = parts
                .iter()
                .map(|p| emit(p, schema))
                .collect::<Result<_, _>>()?;
            Ok(Predicate::and_from(emitted))
        }
        Pred::Or(parts) => {
            let emitted: Vec<Predicate> = parts
                .iter()
                .map(|p| emit(p, schema))
                .collect::<Result<_, _>>()?;
            Ok(Predicate::or_from(emitted))
        }
        Pred::Not(inner) => Ok(Predicate::not(emit(inner, schema)?)),
        Pred::Cmp { col, op, lit } => {
            let hint = resolve_column_type(col, schema);
            let c = column_expr(col);
            let l = literal_expr(lit, hint.as_ref())?;
            Ok(match op {
                CmpOp::Eq => c.eq(l),
                CmpOp::Ne => c.ne(l),
                CmpOp::Lt => c.lt(l),
                CmpOp::Le => c.le(l),
                CmpOp::Gt => c.gt(l),
                CmpOp::Ge => c.ge(l),
            })
        }
        Pred::In { col, list, negated } => {
            let hint = resolve_column_type(col, schema);
            let c = column_expr(col);
            let mut preds = Vec::with_capacity(list.len());
            for item in list {
                let val = literal_expr(item, hint.as_ref())?;
                preds.push(c.clone().eq(val));
            }
            let combined = if preds.len() == 1 {
                match preds.pop() {
                    Some(p) => p,
                    None => return Err("Empty IN list".into()),
                }
            } else {
                Predicate::or_from(preds)
            };
            Ok(if *negated {
                Predicate::not(combined)
            } else {
                combined
            })
        }
        Pred::Between {
            col,
            low,
            high,
            negated,
        } => {
            let hint = resolve_column_type(col, schema);
            let c = column_expr(col);
            let lo = literal_expr(low, hint.as_ref())?;
            let hi = literal_expr(high, hint.as_ref())?;
            let between = Predicate::and(c.clone().ge(lo), c.le(hi));
            Ok(if *negated {
                Predicate::not(between)
            } else {
                between
            })
        }
        Pred::IsNull { col, negated } => {
            let c = column_expr(col);
            Ok(if *negated {
                c.is_not_null()
            } else {
                c.is_null()
            })
        }
        Pred::Distinct { col, lit, negated } => {
            let hint = resolve_column_type(col, schema);
            let c = column_expr(col);
            let l = literal_expr(lit, hint.as_ref())?;
            let distinct = Predicate::distinct(c, l);
            // DISTINCT is two-valued, so NOT(distinct) is exact, not merely
            // conservative.
            Ok(if *negated {
                Predicate::not(distinct)
            } else {
                distinct
            })
        }
        Pred::BoolCol(col) => Ok(Predicate::from_expr(column_expr(col))),
        // A Like reaching emission survived normalization unrewritten: the
        // kernel has no LIKE, so the caller must strip it (or route it to
        // the partition evaluator), same as Unsupported.
        Pred::Like { .. } => Err("LIKE has no kernel predicate form; only a literal-prefix \
             pattern on a string column rewrites to a comparison range during normalization"
            .into()),
        Pred::Unsupported { reason, .. } => Err(reason.clone()),
    }
}

fn column_expr(col: &ColRef) -> Expression {
    Expression::column(col.0.clone())
}

/// True when the column resolves to a STRING leaf in the schema. Gates
/// the prefix-LIKE range rewrite: the lexicographic equivalence holds on
/// no other type. Unknown columns answer false, which safely leaves the
/// Like unrewritten to degrade downstream.
pub fn column_is_string(col: &ColRef, schema: &SchemaRef) -> bool {
    matches!(
        resolve_column_type(col, schema),
        Some(DataType::Primitive(PrimitiveType::String))
    )
}

/// Walk the dotted path (profile.geo.zip) through struct fields so the
/// literal on the other side coerces to the leaf type. Without this, a
/// nested double compared to an integer literal aborts the scan
/// ("Invalid comparison operation: Float64 > Int32").
fn resolve_column_type(col: &ColRef, schema: &SchemaRef) -> Option<DataType> {
    let mut parts = col.0.iter();
    let mut current = schema.field(parts.next()?)?.data_type().clone();
    for part in parts {
        let DataType::Struct(st) = &current else {
            return None;
        };
        current = st.field(part)?.data_type().clone();
    }
    Some(current)
}

// ── Lexical literal -> typed kernel scalar ──────────────────────────

fn literal_expr(lit: &Literal, type_hint: Option<&DataType>) -> Result<Expression, String> {
    match lit {
        Literal::Number(s) => number_literal(s, type_hint),
        Literal::Str(s) => {
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
        Literal::Bool(b) => Ok(Expression::literal(*b)),
        // A NULL literal takes the column's type so the kernel compares
        // like with like (int vs null-of-int, not int vs null-of-string);
        // STRING is only the fallback when no column is in sight.
        Literal::Null => Ok(Expression::null_literal(
            type_hint.cloned().unwrap_or(DataType::STRING),
        )),
        // The column's type hint wins when present (it is the type the kernel
        // will compare against); the declared SQL type only picks the parser
        // otherwise.
        Literal::Date(text) => {
            if let Some(dt) = type_hint
                && let Some(expr) = coerce_string_literal(text, dt)?
            {
                return Ok(expr);
            }
            Ok(Expression::literal(Scalar::Date(parse_date_days(text)?)))
        }
        Literal::Timestamp(text) => {
            if let Some(dt) = type_hint
                && let Some(expr) = coerce_string_literal(text, dt)?
            {
                return Ok(expr);
            }
            Ok(Expression::literal(Scalar::Timestamp(
                parse_timestamp_micros(text)?,
            )))
        }
    }
}

fn number_literal(s: &str, type_hint: Option<&DataType>) -> Result<Expression, String> {
    if let Some(DataType::Primitive(p)) = type_hint {
        // Exhaustive on purpose: a new kernel primitive type must be routed
        // here consciously, not swallowed by a catch-all.
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
            // Integer takes the default i32-first path below; a numeric
            // literal against these types has no better parse than the
            // default widening chain.
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

/// Days since the Unix epoch for a `YYYY-MM-DD` string.
pub(crate) fn parse_date_days(text: &str) -> Result<i32, String> {
    const EPOCH_DAYS_FROM_CE: i32 = 719_163; // 1970-01-01 in chrono's day count
    let d = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|e| format!("Invalid date '{text}' (expected YYYY-MM-DD): {e}"))?;
    Ok(chrono::Datelike::num_days_from_ce(&d) - EPOCH_DAYS_FROM_CE)
}

/// Microseconds since the Unix epoch for a TIMESTAMP string. Accepts RFC 3339
/// (with offset, normalized to UTC), `YYYY-MM-DD[ T]HH:MM:SS[.ffffff]` treated
/// as UTC, and a bare date as midnight UTC: Delta TIMESTAMP is UTC-normalized.
pub(crate) fn parse_timestamp_micros(text: &str) -> Result<i64, String> {
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

/// Wall-clock microseconds for a TIMESTAMP_NTZ string. The column is
/// timezone-naive, so `2026-07-01 09:00:00` means nine o'clock as written,
/// wherever it was written; an explicit offset would silently shift the
/// value, so it is rejected instead of normalized.
pub(crate) fn parse_timestamp_ntz_micros(text: &str) -> Result<i64, String> {
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
    if chrono::DateTime::parse_from_rfc3339(text).is_ok() {
        return Err(format!(
            "Timestamp '{text}' carries an offset, but TIMESTAMP_NTZ is timezone-naive: drop the offset, or compare against a TIMESTAMP column"
        ));
    }
    Err(format!(
        "Invalid timestamp '{text}': expected YYYY-MM-DD[ HH:MM:SS[.ffffff]] (no offset)"
    ))
}

/// Parse a numeric literal into a decimal scaled to the column's type.
/// The literal must fit the column scale exactly; silently rounding a
/// predicate bound would change its meaning.
pub(crate) fn parse_decimal(text: &str, dt: &DecimalType) -> Result<Scalar, String> {
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
    fn capability_table_matches_kernel_0_24() {
        // The golden view of what the kernel prunes on vs merely represents.
        // A kernel bump that widens an op enum breaks the sentinel matches
        // above at compile time; this test documents the expected tiers.
        assert_eq!(
            binary_predicate_capability(&BinaryPredicateOp::Equal),
            Capability::SkippingNative
        );
        assert_eq!(
            binary_predicate_capability(&BinaryPredicateOp::LessThan),
            Capability::SkippingNative
        );
        assert_eq!(
            binary_predicate_capability(&BinaryPredicateOp::GreaterThan),
            Capability::SkippingNative
        );
        assert_eq!(
            binary_predicate_capability(&BinaryPredicateOp::Distinct),
            Capability::LanguageOnly
        );
        assert_eq!(
            binary_predicate_capability(&BinaryPredicateOp::In),
            Capability::LanguageOnly
        );
        assert_eq!(
            unary_predicate_capability(&UnaryPredicateOp::IsNull),
            Capability::SkippingNative
        );
        assert_eq!(
            junction_predicate_capability(&JunctionPredicateOp::And),
            Capability::SkippingNative
        );
        assert_eq!(
            junction_predicate_capability(&JunctionPredicateOp::Or),
            Capability::SkippingNative
        );
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
        // an explicit offset normalizes to UTC
        assert_eq!(parse_timestamp_micros("1970-01-01T01:00:00+01:00"), Ok(0));
        // fractional seconds
        assert_eq!(parse_timestamp_micros("1970-01-01 00:00:00.000042"), Ok(42));
        // bare date is midnight UTC
        assert_eq!(parse_timestamp_micros("1970-01-02"), Ok(86_400_000_000));
        assert!(parse_timestamp_micros("teatime").is_err());
    }

    #[test]
    fn ntz_is_wall_clock_and_rejects_offsets() {
        // same digits as the TIMESTAMP parse when naive...
        assert_eq!(
            parse_timestamp_ntz_micros("1970-01-01 01:00:00"),
            Ok(3_600_000_000)
        );
        // ...but an explicit offset is ambiguous for a naive column
        let err = parse_timestamp_ntz_micros("1970-01-01T01:00:00+01:00");
        assert!(err.is_err_and(|e| e.contains("timezone-naive")));
    }

    #[test]
    fn negative_decimal_scales_correctly() {
        let dt = DecimalType::try_new(9, 2).unwrap();
        match parse_decimal("-100.50", &dt) {
            Ok(Scalar::Decimal(d)) => assert_eq!(d.bits(), -10_050),
            other => panic!("unexpected: {other:?}"),
        }
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

    #[test]
    fn null_literal_takes_the_column_type() {
        let int_hint = DataType::Primitive(PrimitiveType::Integer);
        match literal_expr(&Literal::Null, Some(&int_hint)) {
            Ok(Expression::Literal(Scalar::Null(dt))) => assert_eq!(dt, int_hint),
            other => panic!("unexpected: {other:?}"),
        }
        match literal_expr(&Literal::Null, None) {
            Ok(Expression::Literal(Scalar::Null(dt))) => assert_eq!(dt, DataType::STRING),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unsupported_leaf_is_fatal_at_emission() {
        use crate::predicate_ast::parse;
        let schema = delta_kernel::schema::StructType::try_new(vec![
            delta_kernel::schema::StructField::nullable(
                "age",
                DataType::Primitive(PrimitiveType::Integer),
            ),
        ])
        .unwrap();
        let schema = std::sync::Arc::new(schema);
        let pred = parse("UPPER(name) = 'X'").unwrap();
        let err = emit_predicate(&pred, &schema);
        assert!(err.is_err());
    }

    #[test]
    fn unrewritten_like_is_fatal_at_emission() {
        use crate::predicate_ast::parse;
        let schema = delta_kernel::schema::StructType::try_new(vec![
            delta_kernel::schema::StructField::nullable(
                "name",
                DataType::Primitive(PrimitiveType::String),
            ),
        ])
        .unwrap();
        let schema = std::sync::Arc::new(schema);
        let pred = parse("name LIKE '%son'").unwrap().normalized();
        let err = emit_predicate(&pred, &schema);
        assert!(err.is_err_and(|e| e.to_string().contains("LIKE")));
    }
}
