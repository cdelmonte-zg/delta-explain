use std::cmp::Ordering;
use std::collections::HashMap;

use delta_kernel::schema::{DataType, PrimitiveType, SchemaRef};

use super::predicate::{CmpOp, ColRef, Literal, Pred};
use super::value_coercion::{
    parse_date_days, parse_decimal_bits, parse_timestamp_micros, parse_timestamp_ntz_micros,
};

/// SQL three-valued truth plus evaluator ignorance.
///
/// `Null` is a semantic SQL result.
/// `Unknown` means that the evaluator could not decide safely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Truth {
    True,
    False,
    Null,
    Unknown,
}

impl Truth {
    fn from_bool(value: bool) -> Self {
        if value { Self::True } else { Self::False }
    }

    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,

            (Self::Null, _) | (_, Self::Null) => Self::Null,

            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,

            (Self::True, Self::True) => Self::True,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,

            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,

            (Self::Null, _) | (_, Self::Null) => Self::Null,

            (Self::False, Self::False) => Self::False,
        }
    }

    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Null => Self::Null,
            Self::Unknown => Self::Unknown,
        }
    }
}

/// Evaluate a predicate against the literal partition values of one file.
///
/// An absent partition value represents SQL NULL.
pub(super) fn eval(pred: &Pred, values: &HashMap<String, String>, schema: &SchemaRef) -> Truth {
    match pred {
        Pred::And(parts) => parts
            .iter()
            .fold(Truth::True, |acc, part| acc.and(eval(part, values, schema))),

        Pred::Or(parts) => parts
            .iter()
            .fold(Truth::False, |acc, part| acc.or(eval(part, values, schema))),

        Pred::Not(inner) => eval(inner, values, schema).not(),

        Pred::Cmp { col, op, lit } => cmp_leaf(col, *op, lit, values, schema),

        Pred::In { col, list, negated } => {
            let result = in_leaf(col, list, values, schema);

            if *negated { result.not() } else { result }
        }

        Pred::Between {
            col,
            low,
            high,
            negated,
        } => {
            let result = cmp_leaf(col, CmpOp::Ge, low, values, schema).and(cmp_leaf(
                col,
                CmpOp::Le,
                high,
                values,
                schema,
            ));

            if *negated { result.not() } else { result }
        }

        Pred::IsNull { col, negated } => {
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
            let result = match values.get(&col.dotted()) {
                None => Truth::Null,

                Some(text) if like_matchable(col, schema) => {
                    Truth::from_bool(like_match(text, pattern))
                }

                Some(_) => Truth::Unknown,
            };

            if *negated { result.not() } else { result }
        }

        Pred::Unsupported { .. } => Truth::Unknown,
    }
}

fn cmp_leaf(
    col: &ColRef,
    op: CmpOp,
    lit: &Literal,
    values: &HashMap<String, String>,
    schema: &SchemaRef,
) -> Truth {
    if matches!(lit, Literal::Null) {
        return Truth::Null;
    }

    let Some(raw) = values.get(&col.dotted()) else {
        return Truth::Null;
    };

    let Some(ordering) = compare(raw, lit, col, schema) else {
        return Truth::Unknown;
    };

    Truth::from_bool(match op {
        CmpOp::Eq => ordering == Ordering::Equal,

        CmpOp::Ne => ordering != Ordering::Equal,

        CmpOp::Lt => ordering == Ordering::Less,

        CmpOp::Le => ordering != Ordering::Greater,

        CmpOp::Gt => ordering == Ordering::Greater,

        CmpOp::Ge => ordering != Ordering::Less,
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

    for literal in list {
        if matches!(literal, Literal::Null) {
            saw_null = true;
            continue;
        }

        match compare(raw, literal, col, schema) {
            Some(Ordering::Equal) => {
                return Truth::True;
            }

            Some(_) => {}

            None => {
                saw_unknown = true;
            }
        }
    }

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
    let distinct = match (values.get(&col.dotted()), matches!(lit, Literal::Null)) {
        (None, true) => false,

        (None, false) | (Some(_), true) => true,

        (Some(raw), false) => match compare(raw, lit, col, schema) {
            Some(ordering) => ordering != Ordering::Equal,

            None => {
                return Truth::Unknown;
            }
        },
    };

    Truth::from_bool(distinct != negated)
}

enum TypedValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Temporal(i64),
    Decimal(i128),
}

fn compare(raw: &str, lit: &Literal, col: &ColRef, schema: &SchemaRef) -> Option<Ordering> {
    let data_type = schema.field(col.dotted())?.data_type().clone();

    let left = partition_value(raw, &data_type)?;

    let right = literal_value(lit, &data_type)?;

    match (left, right) {
        (TypedValue::String(left), TypedValue::String(right)) => Some(left.cmp(&right)),

        (TypedValue::Integer(left), TypedValue::Integer(right)) => Some(left.cmp(&right)),

        (TypedValue::Float(left), TypedValue::Float(right)) => left.partial_cmp(&right),

        (TypedValue::Boolean(left), TypedValue::Boolean(right)) => Some(left.cmp(&right)),

        (TypedValue::Temporal(left), TypedValue::Temporal(right)) => Some(left.cmp(&right)),

        (TypedValue::Decimal(left), TypedValue::Decimal(right)) => Some(left.cmp(&right)),

        _ => None,
    }
}

fn partition_value(raw: &str, data_type: &DataType) -> Option<TypedValue> {
    let DataType::Primitive(primitive) = data_type else {
        return None;
    };

    match primitive {
        PrimitiveType::String => Some(TypedValue::String(raw.to_string())),

        PrimitiveType::Integer
        | PrimitiveType::Long
        | PrimitiveType::Short
        | PrimitiveType::Byte => raw.parse::<i64>().ok().map(TypedValue::Integer),

        PrimitiveType::Float | PrimitiveType::Double => {
            raw.parse::<f64>().ok().map(TypedValue::Float)
        }

        PrimitiveType::Boolean => match raw {
            "true" => Some(TypedValue::Boolean(true)),

            "false" => Some(TypedValue::Boolean(false)),

            _ => None,
        },

        PrimitiveType::Date => parse_date_days(raw)
            .ok()
            .map(|value| TypedValue::Temporal(i64::from(value))),

        PrimitiveType::Timestamp => parse_timestamp_micros(raw).ok().map(TypedValue::Temporal),

        PrimitiveType::TimestampNtz => parse_timestamp_ntz_micros(raw)
            .ok()
            .map(TypedValue::Temporal),

        PrimitiveType::Decimal(decimal_type) => parse_decimal_bits(raw, decimal_type)
            .ok()
            .map(TypedValue::Decimal),

        PrimitiveType::Binary
        | PrimitiveType::Void
        | PrimitiveType::IntervalYearMonth
        | PrimitiveType::IntervalDayTime => None,
    }
}

fn literal_value(literal: &Literal, data_type: &DataType) -> Option<TypedValue> {
    let DataType::Primitive(primitive) = data_type else {
        return None;
    };

    match (literal, primitive) {
        (Literal::Str(value), PrimitiveType::String) => Some(TypedValue::String(value.clone())),

        (
            Literal::Number(value),
            PrimitiveType::Integer
            | PrimitiveType::Long
            | PrimitiveType::Short
            | PrimitiveType::Byte,
        ) => value.parse::<i64>().ok().map(TypedValue::Integer),

        (Literal::Number(value), PrimitiveType::Float | PrimitiveType::Double) => {
            value.parse::<f64>().ok().map(TypedValue::Float)
        }

        (Literal::Number(value), PrimitiveType::Decimal(decimal_type)) => {
            parse_decimal_bits(value, decimal_type)
                .ok()
                .map(TypedValue::Decimal)
        }

        (Literal::Bool(value), PrimitiveType::Boolean) => Some(TypedValue::Boolean(*value)),

        (Literal::Str(value) | Literal::Date(value), PrimitiveType::Date) => parse_date_days(value)
            .ok()
            .map(|value| TypedValue::Temporal(i64::from(value))),

        (
            Literal::Str(value) | Literal::Date(value) | Literal::Timestamp(value),
            PrimitiveType::Timestamp,
        ) => parse_timestamp_micros(value).ok().map(TypedValue::Temporal),

        (
            Literal::Str(value) | Literal::Date(value) | Literal::Timestamp(value),
            PrimitiveType::TimestampNtz,
        ) => parse_timestamp_ntz_micros(value)
            .ok()
            .map(TypedValue::Temporal),

        _ => None,
    }
}

fn like_matchable(col: &ColRef, schema: &SchemaRef) -> bool {
    let Some(field) = schema.field(col.dotted()) else {
        return false;
    };

    matches!(
        field.data_type(),
        DataType::Primitive(
            PrimitiveType::String
                | PrimitiveType::Integer
                | PrimitiveType::Long
                | PrimitiveType::Short
                | PrimitiveType::Byte
                | PrimitiveType::Date
                | PrimitiveType::Boolean
        )
    )
}

fn like_match(text: &str, pattern: &str) -> bool {
    let text: Vec<char> = text.chars().collect();

    let pattern: Vec<char> = pattern.chars().collect();

    let (mut text_index, mut pattern_index) = (0usize, 0usize);

    let mut wildcard: Option<(usize, usize)> = None;

    while text_index < text.len() {
        if pattern_index < pattern.len() && pattern[pattern_index] == '%' {
            wildcard = Some((pattern_index, text_index));

            pattern_index += 1;
        } else if pattern_index < pattern.len()
            && (pattern[pattern_index] == '_' || pattern[pattern_index] == text[text_index])
        {
            text_index += 1;
            pattern_index += 1;
        } else if let Some((wildcard_index, wildcard_text_index)) = wildcard {
            pattern_index = wildcard_index + 1;

            text_index = wildcard_text_index + 1;

            wildcard = Some((wildcard_index, wildcard_text_index + 1));
        } else {
            return false;
        }
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == '%' {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};

    use super::*;
    use crate::v2::analysis::predicate;

    fn schema() -> SchemaRef {
        Arc::new(
            StructType::try_new([
                StructField::nullable("country", DataType::STRING),
                StructField::nullable("year", DataType::INTEGER),
                StructField::nullable("active", DataType::BOOLEAN),
                StructField::nullable("day", DataType::DATE),
            ])
            .unwrap(),
        )
    }

    fn values(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn evaluate(input: &str, pairs: &[(&str, &str)]) -> Truth {
        let pred = predicate::parse(input).unwrap().normalized_with(|_| false);

        eval(&pred, &values(pairs), &schema())
    }

    #[test]
    fn non_prefix_like_is_evaluated_exactly() {
        assert_eq!(
            evaluate("country LIKE '%E'", &[("country", "DE")],),
            Truth::True
        );

        assert_eq!(
            evaluate("country LIKE '%E'", &[("country", "US")],),
            Truth::False
        );
    }

    #[test]
    fn absent_partition_value_is_sql_null() {
        assert_eq!(evaluate("country LIKE '%E'", &[],), Truth::Null);

        assert_eq!(evaluate("country IS NULL", &[],), Truth::True);

        assert_eq!(evaluate("country IS NOT NULL", &[],), Truth::False);
    }

    #[test]
    fn comparisons_follow_column_type() {
        assert_eq!(evaluate("year > 9", &[("year", "10")],), Truth::True);
    }

    #[test]
    fn unknown_is_distinct_from_null() {
        assert_eq!(Truth::Null.and(Truth::Unknown), Truth::Null);

        assert_eq!(Truth::Null.or(Truth::Unknown), Truth::Unknown);
    }
}
