use super::model::{CmpOp, ColRef, Literal, Pred};

impl Pred {
    /// Normalize the predicate using semantics-preserving rewrites.
    ///
    /// This:
    ///
    /// - pushes NOT down to leaves,
    /// - rewrites prefix-shaped LIKE expressions into comparison ranges,
    /// - factors conjuncts common to every OR branch.
    ///
    /// This variant assumes LIKE columns are strings. When schema
    /// information is available, use [`Pred::normalized_with`].
    pub fn normalized(self) -> Pred {
        self.normalized_with(|_| true)
    }

    /// Normalize the predicate with schema knowledge.
    ///
    /// `is_string_col` controls whether LIKE may be rewritten into a
    /// lexicographic comparison range. That transformation is valid only
    /// for string columns.
    pub fn normalized_with<F>(self, is_string_col: F) -> Pred
    where
        F: Fn(&ColRef) -> bool,
    {
        factor_or(rewrite_prefix_like(
            push_down_not(self, false),
            &is_string_col,
        ))
    }

    /// Remove subtrees that cannot be consumed by the general pruning
    /// backend while preserving a conservative predicate.
    ///
    /// Dropping a constraint can only retain additional files; it must
    /// never cause additional pruning.
    ///
    /// Under AND, supported siblings survive independently.
    ///
    /// An OR or NOT containing unsupported semantics must be discarded
    /// entirely because its truth value cannot safely be bounded.
    ///
    /// `None` means no usable predicate remains.
    pub fn without_unsupported(&self) -> Option<Pred> {
        match self {
            Pred::And(parts) => {
                let kept: Vec<Pred> = parts.iter().filter_map(Pred::without_unsupported).collect();

                match kept.len() {
                    0 => None,
                    _ => Some(and_flat(kept)),
                }
            }

            Pred::Or(_) | Pred::Not(_) => {
                if self.contains_unsupported() {
                    None
                } else {
                    Some(self.clone())
                }
            }

            Pred::Unsupported { .. } | Pred::Like { .. } => None,

            Pred::Cmp { .. }
            | Pred::In { .. }
            | Pred::Between { .. }
            | Pred::IsNull { .. }
            | Pred::Distinct { .. }
            | Pred::BoolCol(_) => Some(self.clone()),
        }
    }
}

/// Flatten nested AND expressions and unwrap a single-element AND.
fn and_flat(parts: Vec<Pred>) -> Pred {
    let mut out = Vec::with_capacity(parts.len());

    for part in parts {
        match part {
            Pred::And(children) => {
                out.extend(children);
            }

            other => {
                out.push(other);
            }
        }
    }

    match out.len() {
        1 => match out.pop() {
            Some(single) => single,
            None => Pred::And(out),
        },

        _ => Pred::And(out),
    }
}

/// Flatten nested OR expressions and unwrap a single-element OR.
fn or_flat(parts: Vec<Pred>) -> Pred {
    let mut out = Vec::with_capacity(parts.len());

    for part in parts {
        match part {
            Pred::Or(children) => {
                out.extend(children);
            }

            other => {
                out.push(other);
            }
        }
    }

    match out.len() {
        1 => match out.pop() {
            Some(single) => single,
            None => Pred::Or(out),
        },

        _ => Pred::Or(out),
    }
}

/// Push NOT toward predicate leaves.
///
/// `negated` represents the parity of enclosing NOT expressions.
///
/// Comparisons complement their operator. IN, BETWEEN, IS NULL,
/// DISTINCT and LIKE toggle their `negated` flag.
///
/// Bare boolean columns and opaque unsupported expressions retain an
/// explicit [`Pred::Not`] because rewriting their value would not
/// preserve SQL NULL semantics.
fn push_down_not(pred: Pred, negated: bool) -> Pred {
    match pred {
        Pred::And(parts) => {
            let parts = parts
                .into_iter()
                .map(|part| push_down_not(part, negated))
                .collect();

            if negated {
                or_flat(parts)
            } else {
                and_flat(parts)
            }
        }

        Pred::Or(parts) => {
            let parts = parts
                .into_iter()
                .map(|part| push_down_not(part, negated))
                .collect();

            if negated {
                and_flat(parts)
            } else {
                or_flat(parts)
            }
        }

        Pred::Not(inner) => push_down_not(*inner, !negated),

        Pred::Cmp { col, op, lit } => Pred::Cmp {
            col,
            op: if negated { op.negated() } else { op },
            lit,
        },

        Pred::In {
            col,
            list,
            negated: inner_negated,
        } => Pred::In {
            col,
            list,
            negated: inner_negated != negated,
        },

        Pred::Between {
            col,
            low,
            high,
            negated: inner_negated,
        } => Pred::Between {
            col,
            low,
            high,
            negated: inner_negated != negated,
        },

        Pred::IsNull {
            col,
            negated: inner_negated,
        } => Pred::IsNull {
            col,
            negated: inner_negated != negated,
        },

        Pred::Distinct {
            col,
            lit,
            negated: inner_negated,
        } => Pred::Distinct {
            col,
            lit,
            negated: inner_negated != negated,
        },

        Pred::Like {
            col,
            pattern,
            negated: inner_negated,
        } => Pred::Like {
            col,
            pattern,
            negated: inner_negated != negated,
        },

        leaf @ (Pred::BoolCol(_) | Pred::Unsupported { .. }) => {
            if negated {
                Pred::Not(Box::new(leaf))
            } else {
                leaf
            }
        }
    }
}

enum LikeShape {
    /// LIKE contains no wildcards and is equivalent to equality.
    Exact(String),

    /// A non-empty literal prefix followed only by `%`.
    Prefix(String),

    /// Any other LIKE shape.
    Other,
}

fn like_shape(pattern: &str) -> LikeShape {
    match pattern.find(['%', '_']) {
        None => LikeShape::Exact(pattern.to_string()),

        Some(0) => LikeShape::Other,

        Some(index) if pattern[index..].chars().all(|c| c == '%') => {
            LikeShape::Prefix(pattern[..index].to_string())
        }

        Some(_) => LikeShape::Other,
    }
}

/// Rewrite LIKE expressions that can be represented exactly by the
/// pruning predicate language.
///
/// `col LIKE 'abc'` becomes:
///
/// `col = 'abc'`
///
/// `col LIKE 'abc%'` becomes:
///
/// `col >= 'abc' AND col < 'abd'`
///
/// The transformation is performed only for non-negated LIKE expressions
/// on string columns.
fn rewrite_prefix_like<F>(pred: Pred, is_string: &F) -> Pred
where
    F: Fn(&ColRef) -> bool,
{
    match pred {
        Pred::And(parts) => and_flat(
            parts
                .into_iter()
                .map(|part| rewrite_prefix_like(part, is_string))
                .collect(),
        ),

        Pred::Or(parts) => or_flat(
            parts
                .into_iter()
                .map(|part| rewrite_prefix_like(part, is_string))
                .collect(),
        ),

        Pred::Not(inner) => Pred::Not(Box::new(rewrite_prefix_like(*inner, is_string))),

        Pred::Like {
            col,
            pattern,
            negated: false,
        } if is_string(&col) => {
            match like_shape(&pattern) {
                LikeShape::Exact(text) => Pred::Cmp {
                    col,
                    op: CmpOp::Eq,
                    lit: Literal::Str(text),
                },

                LikeShape::Prefix(prefix) => {
                    let lower = Pred::Cmp {
                        col: col.clone(),
                        op: CmpOp::Ge,
                        lit: Literal::Str(prefix.clone()),
                    };

                    match prefix_successor(&prefix) {
                        Some(upper) => Pred::And(vec![
                            lower,
                            Pred::Cmp {
                                col,
                                op: CmpOp::Lt,
                                lit: Literal::Str(upper),
                            },
                        ]),

                        // If the prefix has no successor, the lower
                        // bound alone describes the remaining range.
                        None => lower,
                    }
                }

                LikeShape::Other => Pred::Like {
                    col,
                    pattern,
                    negated: false,
                },
            }
        }

        leaf => leaf,
    }
}

/// Return the least string strictly greater than every string beginning
/// with `prefix`.
fn prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();

    while let Some(last) = chars.pop() {
        if let Some(next) = char_successor(last) {
            chars.push(next);

            return Some(chars.into_iter().collect());
        }
    }

    None
}

/// Return the next Unicode scalar value after `c`, skipping invalid
/// surrogate code points.
fn char_successor(c: char) -> Option<char> {
    let mut value = c as u32 + 1;

    while value <= char::MAX as u32 {
        if let Some(next) = char::from_u32(value) {
            return Some(next);
        }

        value += 1;
    }

    None
}

/// Factor conjuncts common to every OR branch.
///
/// For example:
///
/// `(a AND x) OR (a AND y)`
///
/// becomes:
///
/// `a AND (x OR y)`
///
/// This exposes predicates such as partition conditions that otherwise
/// remain hidden inside individual OR branches.
fn factor_or(pred: Pred) -> Pred {
    match pred {
        Pred::And(parts) => and_flat(parts.into_iter().map(factor_or).collect()),

        Pred::Or(parts) => {
            let children: Vec<Pred> = parts.into_iter().map(factor_or).collect();

            let branch_sets: Vec<Vec<Pred>> = children
                .into_iter()
                .map(|child| match child {
                    Pred::And(parts) => parts,

                    other => {
                        vec![other]
                    }
                })
                .collect();

            let common: Vec<Pred> = match branch_sets.first() {
                Some(first) => first
                    .iter()
                    .filter(|item| branch_sets[1..].iter().all(|set| set.contains(item)))
                    .cloned()
                    .collect(),

                None => Vec::new(),
            };

            if common.is_empty() {
                return or_flat(branch_sets.into_iter().map(and_flat).collect());
            }

            let mut remainders = Vec::with_capacity(branch_sets.len());

            for set in branch_sets {
                let mut rest = set;

                for item in &common {
                    if let Some(position) = rest.iter().position(|candidate| candidate == item) {
                        rest.remove(position);
                    }
                }

                // One branch consists entirely of the common predicate:
                //
                // a OR (a AND x) == a
                if rest.is_empty() {
                    return and_flat(common);
                }

                remainders.push(and_flat(rest));
            }

            let mut result = common;

            result.push(or_flat(remainders));

            and_flat(result)
        }

        Pred::Not(inner) => Pred::Not(Box::new(factor_or(*inner))),

        leaf => leaf,
    }
}
