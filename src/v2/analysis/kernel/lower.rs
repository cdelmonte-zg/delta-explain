use delta_kernel::expressions::{Expression, Predicate};
use delta_kernel::schema::SchemaRef;

use crate::v2::analysis::predicate::{CmpOp, ColRef, Pred};
use crate::v2::error::Error;

use super::literal::literal_expr;
use super::schema::resolve_column_type;

/// Lower an owned predicate AST into delta-kernel's predicate language.
///
/// This module only performs structural lowering.
///
/// Type resolution is delegated to `schema`.
/// Literal coercion is delegated to `literal`.
///
/// Unsupported predicates are errors here. The analysis layer is
/// responsible for deciding which fragments are safe to send to the
/// kernel before calling this function.
pub(in crate::v2::analysis) fn lower(
    predicate: &Pred,
    schema: &SchemaRef,
) -> Result<Predicate, Error> {
    lower_inner(predicate, schema).map_err(Error::Predicate)
}

fn lower_inner(predicate: &Pred, schema: &SchemaRef) -> Result<Predicate, String> {
    match predicate {
        Pred::And(parts) => {
            let predicates = parts
                .iter()
                .map(|part| lower_inner(part, schema))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Predicate::and_from(predicates))
        }

        Pred::Or(parts) => {
            let predicates = parts
                .iter()
                .map(|part| lower_inner(part, schema))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Predicate::or_from(predicates))
        }

        Pred::Not(inner) => Ok(Predicate::not(lower_inner(inner, schema)?)),

        Pred::Cmp { col, op, lit } => {
            let type_hint = resolve_column_type(col, schema);

            let column = column_expr(col);

            let literal = literal_expr(lit, type_hint.as_ref())?;

            Ok(match op {
                CmpOp::Eq => column.eq(literal),

                CmpOp::Ne => column.ne(literal),

                CmpOp::Lt => column.lt(literal),

                CmpOp::Le => column.le(literal),

                CmpOp::Gt => column.gt(literal),

                CmpOp::Ge => column.ge(literal),
            })
        }

        Pred::In { col, list, negated } => {
            let type_hint = resolve_column_type(col, schema);

            let column = column_expr(col);

            let mut predicates = Vec::with_capacity(list.len());

            for item in list {
                let value = literal_expr(item, type_hint.as_ref())?;

                // Expand IN into OR-of-equalities.
                //
                // This gives the kernel's skipping path individual
                // equality predicates instead of relying on its binary
                // IN representation.
                predicates.push(column.clone().eq(value));
            }

            let combined = match predicates.pop() {
                None => {
                    return Err("Empty IN list".into());
                }

                Some(single) if predicates.is_empty() => single,

                Some(last) => {
                    predicates.push(last);
                    Predicate::or_from(predicates)
                }
            };

            if *negated {
                Ok(Predicate::not(combined))
            } else {
                Ok(combined)
            }
        }

        Pred::Between {
            col,
            low,
            high,
            negated,
        } => {
            let type_hint = resolve_column_type(col, schema);

            let column = column_expr(col);

            let low = literal_expr(low, type_hint.as_ref())?;

            let high = literal_expr(high, type_hint.as_ref())?;

            let between = Predicate::and(column.clone().ge(low), column.le(high));

            if *negated {
                Ok(Predicate::not(between))
            } else {
                Ok(between)
            }
        }

        Pred::IsNull { col, negated } => {
            let column = column_expr(col);

            if *negated {
                Ok(column.is_not_null())
            } else {
                Ok(column.is_null())
            }
        }

        Pred::Distinct { col, lit, negated } => {
            let type_hint = resolve_column_type(col, schema);

            let column = column_expr(col);

            let literal = literal_expr(lit, type_hint.as_ref())?;

            let distinct = Predicate::distinct(column, literal);

            // DISTINCT is two-valued. Negating it is therefore exact.
            if *negated {
                Ok(Predicate::not(distinct))
            } else {
                Ok(distinct)
            }
        }

        Pred::BoolCol(col) => Ok(Predicate::from_expr(column_expr(col))),

        // A LIKE that reaches this layer survived normalization.
        //
        // delta-kernel has no LIKE predicate form. Such a fragment must
        // either have been routed to partition-exact evaluation or stripped
        // before kernel lowering.
        Pred::Like { .. } => Err("LIKE has no kernel predicate form; \
                 only a literal-prefix pattern on a \
                 string column can be rewritten to a \
                 comparison range during normalization"
            .into()),

        Pred::Unsupported { reason, .. } => Err(reason.clone()),
    }
}

fn column_expr(column: &ColRef) -> Expression {
    Expression::column(column.0.clone())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};

    use super::*;

    use crate::v2::analysis::predicate;

    fn test_schema() -> SchemaRef {
        Arc::new(
            StructType::try_new([
                StructField::nullable("country", DataType::STRING),
                StructField::nullable("age", DataType::INTEGER),
                StructField::nullable("score", DataType::DOUBLE),
                StructField::nullable("active", DataType::BOOLEAN),
                StructField::nullable("event_date", DataType::DATE),
                StructField::nullable("event_ts", DataType::TIMESTAMP),
            ])
            .unwrap(),
        )
    }

    fn parsed(input: &str) -> Pred {
        predicate::parse(input).unwrap().normalized_with(|col| {
            let schema = test_schema();

            super::super::schema::column_is_string(col, &schema)
        })
    }

    #[test]
    fn lowers_simple_comparison() {
        let schema = test_schema();

        let predicate = parsed("age > 30");

        assert!(lower(&predicate, &schema,).is_ok());
    }

    #[test]
    fn lowers_typed_double_comparison() {
        let schema = test_schema();

        let predicate = parsed("score > 30");

        assert!(lower(&predicate, &schema,).is_ok());
    }

    #[test]
    fn lowers_and_predicate() {
        let schema = test_schema();

        let predicate = parsed(
            "country = 'DE' \
                 AND age > 30",
        );

        assert!(lower(&predicate, &schema,).is_ok());
    }

    #[test]
    fn lowers_or_predicate() {
        let schema = test_schema();

        let predicate = parsed(
            "country = 'DE' \
                 OR country = 'IT'",
        );

        assert!(lower(&predicate, &schema,).is_ok());
    }

    #[test]
    fn lowers_in_as_kernel_predicate() {
        let schema = test_schema();

        let predicate = parsed("country IN ('DE', 'IT')");

        assert!(lower(&predicate, &schema,).is_ok());
    }

    #[test]
    fn lowers_between() {
        let schema = test_schema();

        let predicate = parsed("age BETWEEN 20 AND 40");

        assert!(lower(&predicate, &schema,).is_ok());
    }

    #[test]
    fn lowers_is_null() {
        let schema = test_schema();

        let predicate = parsed("age IS NULL");

        assert!(lower(&predicate, &schema,).is_ok());
    }

    #[test]
    fn lowers_boolean_column() {
        let schema = test_schema();

        let predicate = parsed("active");

        assert!(lower(&predicate, &schema,).is_ok());
    }

    #[test]
    fn lowers_quoted_date_using_column_type() {
        let schema = test_schema();

        let predicate = parsed("event_date = '2026-08-31'");

        assert!(lower(&predicate, &schema,).is_ok());
    }

    #[test]
    fn prefix_like_is_lowerable_after_normalization() {
        let schema = test_schema();

        let predicate = parsed("country LIKE 'DE%'");

        assert_eq!(predicate.to_string(), "country >= 'DE' AND country < 'DF'");

        assert!(lower(&predicate, &schema,).is_ok());
    }

    #[test]
    fn unrewritten_like_is_rejected() {
        let schema = test_schema();

        let predicate = parsed("country LIKE '%DE'");

        let error = lower(&predicate, &schema);

        assert!(error.is_err());

        assert!(error.unwrap_err().to_string().contains("LIKE"));
    }

    #[test]
    fn unsupported_expression_is_rejected() {
        let schema = test_schema();

        let predicate = parsed("UPPER(country) = 'DE'");

        assert!(lower(&predicate, &schema,).is_err());
    }
}
