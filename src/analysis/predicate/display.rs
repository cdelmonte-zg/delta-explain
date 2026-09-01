use std::fmt;

use super::model::{CmpOp, ColRef, Literal, Pred};

impl fmt::Display for CmpOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            CmpOp::Eq => "=",
            CmpOp::Ne => "<>",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        };

        f.write_str(value)
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
            Literal::Number(value) => f.write_str(value),

            Literal::Str(value) => {
                write!(f, "'{}'", value.replace('\'', "''"),)
            }

            Literal::Bool(value) => {
                write!(f, "{value}")
            }

            Literal::Null => f.write_str("NULL"),

            Literal::Date(value) => {
                write!(f, "DATE '{}'", value.replace('\'', "''"),)
            }

            Literal::Timestamp(value) => {
                write!(f, "TIMESTAMP '{}'", value.replace('\'', "''"),)
            }
        }
    }
}

impl Pred {
    fn is_composite(&self) -> bool {
        matches!(self, Pred::And(_) | Pred::Or(_))
    }
}

fn write_junction(f: &mut fmt::Formatter<'_>, parts: &[Pred], separator: &str) -> fmt::Result {
    for (index, predicate) in parts.iter().enumerate() {
        if index > 0 {
            f.write_str(separator)?;
        }

        if predicate.is_composite() {
            write!(f, "({predicate})")?;
        } else {
            write!(f, "{predicate}")?;
        }
    }

    Ok(())
}

impl fmt::Display for Pred {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Pred::And(parts) => write_junction(f, parts, " AND "),

            Pred::Or(parts) => write_junction(f, parts, " OR "),

            Pred::Not(inner) => {
                if inner.is_composite() {
                    write!(f, "NOT ({inner})",)
                } else {
                    write!(f, "NOT {inner}",)
                }
            }

            Pred::Cmp { col, op, lit } => {
                write!(f, "{col} {op} {lit}",)
            }

            Pred::In { col, list, negated } => {
                let not = if *negated { "NOT " } else { "" };

                write!(f, "{col} {not}IN (",)?;

                for (index, item) in list.iter().enumerate() {
                    if index > 0 {
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

                write!(f, "{col} {not}BETWEEN {low} AND {high}",)
            }

            Pred::IsNull { col, negated } => {
                let not = if *negated { "NOT " } else { "" };

                write!(f, "{col} IS {not}NULL",)
            }

            Pred::Distinct { col, lit, negated } => {
                let not = if *negated { "NOT " } else { "" };

                write!(f, "{col} IS {not}DISTINCT FROM {lit}",)
            }

            Pred::BoolCol(col) => {
                write!(f, "{col}")
            }

            Pred::Like {
                col,
                pattern,
                negated,
            } => {
                let not = if *negated { "NOT " } else { "" };

                write!(f, "{col} {not}LIKE '{}'", pattern.replace('\'', "''"),)
            }

            Pred::Unsupported { raw, .. } => f.write_str(raw),
        }
    }
}
