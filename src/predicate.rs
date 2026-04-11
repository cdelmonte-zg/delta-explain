use delta_kernel::expressions::{Expression, Predicate};
use delta_kernel::schema::{DataType, SchemaRef};

/// Parse a simple predicate string like "age > 30 AND country = 'DE'"
/// into a kernel Predicate. Supports AND-joined binary comparisons.
/// Operators: =, !=, <, <=, >, >=
pub fn parse_predicate(input: &str, schema: &SchemaRef) -> Result<Predicate, String> {
    let clauses: Vec<&str> = split_on_and(input);
    if clauses.is_empty() {
        return Err("Empty predicate".into());
    }

    let preds: Vec<Predicate> = clauses
        .into_iter()
        .map(|clause| parse_comparison(clause.trim(), schema))
        .collect::<Result<Vec<_>, _>>()?;

    if preds.len() == 1 {
        Ok(preds.into_iter().next().unwrap())
    } else {
        Ok(Predicate::and_from(preds))
    }
}

/// Split on " AND " (case-insensitive)
fn split_on_and(input: &str) -> Vec<&str> {
    let upper = input.to_uppercase();
    let mut result = Vec::new();
    let mut start = 0;

    while let Some(pos) = upper[start..].find(" AND ") {
        result.push(&input[start..start + pos]);
        start = start + pos + 5; // len(" AND ")
    }
    result.push(&input[start..]);
    result
}

/// Parse a single comparison like "age > 30" or "country = 'DE'"
fn parse_comparison(clause: &str, schema: &SchemaRef) -> Result<Predicate, String> {
    // Try operators from longest to shortest to avoid matching '<' before '<='
    let ops = ["!=", "<=", ">=", "=", "<", ">"];

    for op in ops {
        if let Some(idx) = clause.find(op) {
            let col_name = clause[..idx].trim();
            let value_str = clause[idx + op.len()..].trim();

            let field = schema
                .field(col_name)
                .ok_or_else(|| format!("Unknown column: '{col_name}'"))?;

            let col = Expression::column([col_name]);
            let lit = parse_literal(value_str, field.data_type())?;

            return match op {
                "=" => Ok(col.eq(lit)),
                "!=" => Ok(col.ne(lit)),
                "<" => Ok(col.lt(lit)),
                "<=" => Ok(col.le(lit)),
                ">" => Ok(col.gt(lit)),
                ">=" => Ok(col.ge(lit)),
                _ => unreachable!(),
            };
        }
    }

    Err(format!("Cannot parse comparison: '{clause}'"))
}

fn parse_literal(s: &str, data_type: &DataType) -> Result<Expression, String> {
    let s = s.trim();
    if *data_type == DataType::STRING {
        let unquoted = s
            .strip_prefix('\'')
            .and_then(|s| s.strip_suffix('\''))
            .or_else(|| s.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
            .ok_or_else(|| format!("String literal must be quoted: '{s}'"))?;
        Ok(Expression::literal(unquoted.to_string()))
    } else if *data_type == DataType::INTEGER {
        let v: i32 = s.parse().map_err(|e| format!("Invalid integer '{s}': {e}"))?;
        Ok(Expression::literal(v))
    } else if *data_type == DataType::LONG {
        let v: i64 = s.parse().map_err(|e| format!("Invalid long '{s}': {e}"))?;
        Ok(Expression::literal(v))
    } else if *data_type == DataType::FLOAT {
        let v: f32 = s.parse().map_err(|e| format!("Invalid float '{s}': {e}"))?;
        Ok(Expression::literal(v))
    } else if *data_type == DataType::DOUBLE {
        let v: f64 = s.parse().map_err(|e| format!("Invalid double '{s}': {e}"))?;
        Ok(Expression::literal(v))
    } else if *data_type == DataType::BOOLEAN {
        let v: bool = s.parse().map_err(|e| format!("Invalid boolean '{s}': {e}"))?;
        Ok(Expression::literal(v))
    } else {
        Err(format!("Unsupported data type for literal: {data_type:?}"))
    }
}

/// Classify each clause as either partition-only or data-skipping.
/// Returns (partition_preds, stats_preds).
pub fn split_predicate(
    input: &str,
    schema: &SchemaRef,
    partition_columns: &[String],
) -> Result<(Vec<Predicate>, Vec<Predicate>), String> {
    let clauses = split_on_and(input);
    let mut partition_preds = Vec::new();
    let mut stats_preds = Vec::new();

    for clause in clauses {
        let clause = clause.trim();
        let pred = parse_comparison(clause, schema)?;

        // Figure out which column this clause references
        let col_name = extract_column_name(clause)?;
        if partition_columns.contains(&col_name) {
            partition_preds.push(pred);
        } else {
            stats_preds.push(pred);
        }
    }

    Ok((partition_preds, stats_preds))
}

pub fn extract_clauses(pred_str: &str, keep: impl Fn(&str) -> bool) -> String {
    let upper = pred_str.to_uppercase();
    let mut parts = Vec::new();
    let mut start = 0;

    let mut indices = Vec::new();
    let mut s = 0;
    while let Some(pos) = upper[s..].find(" AND ") {
        indices.push(s + pos);
        s = s + pos + 5;
    }

    let mut clauses = Vec::new();
    for &idx in &indices {
        clauses.push(&pred_str[start..idx]);
        start = idx + 5;
    }
    clauses.push(&pred_str[start..]);

    let ops = ["!=", "<=", ">=", "=", "<", ">"];
    for clause in clauses {
        let clause = clause.trim();
        for op in ops {
            if let Some(idx) = clause.find(op) {
                let col = clause[..idx].trim();
                if keep(col) {
                    parts.push(clause.to_string());
                }
                break;
            }
        }
    }

    parts.join(" AND ")
}

fn extract_column_name(clause: &str) -> Result<String, String> {
    let ops = ["!=", "<=", ">=", "=", "<", ">"];
    for op in ops {
        if let Some(idx) = clause.find(op) {
            return Ok(clause[..idx].trim().to_string());
        }
    }
    Err(format!("Cannot extract column name from: '{clause}'"))
}
