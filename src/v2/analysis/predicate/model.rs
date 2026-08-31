//! Domain model for pruning predicates.
//!
//! This module defines the owned predicate language used by delta-explain.
//! It has no dependency on sqlparser, delta-kernel, table metadata, or
//! reporting. Parsing, normalization, and rendering live in sibling modules.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    /// Return the operator that preserves meaning when operands swap sides.
    ///
    /// For example:
    ///
    /// `5 < x` becomes `x > 5`.
    pub(super) fn flipped(self) -> Self {
        match self {
            CmpOp::Eq => CmpOp::Eq,
            CmpOp::Ne => CmpOp::Ne,
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Ge => CmpOp::Le,
        }
    }

    /// Return the logical complement of this comparison operator.
    ///
    /// Under SQL three-valued logic, for example, `NOT (x < 5)` and
    /// `x >= 5` are both NULL when `x` is NULL, so this rewrite is safe.
    pub(super) fn negated(self) -> Self {
        match self {
            CmpOp::Eq => CmpOp::Ne,
            CmpOp::Ne => CmpOp::Eq,
            CmpOp::Lt => CmpOp::Ge,
            CmpOp::Le => CmpOp::Gt,
            CmpOp::Gt => CmpOp::Le,
            CmpOp::Ge => CmpOp::Lt,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColRef(pub Vec<String>);

impl ColRef {
    pub fn dotted(&self) -> String {
        self.0.join(".")
    }
}

/// Literals stay lexical.
///
/// Numeric values retain the text they were written as. Typed coercion
/// happens later, when an interpreter has access to the table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    Number(String),
    Str(String),
    Bool(bool),
    Null,
    Date(String),
    Timestamp(String),
}

/// The predicate language understood by delta-explain.
///
/// This is deliberately smaller than SQL's expression language. Anything
/// accepted by the SQL parser but outside this vocabulary is represented
/// once, at the parser boundary, as [`Pred::Unsupported`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pred {
    And(Vec<Pred>),

    Or(Vec<Pred>),

    Not(Box<Pred>),

    Cmp {
        col: ColRef,
        op: CmpOp,
        lit: Literal,
    },

    In {
        col: ColRef,
        list: Vec<Literal>,
        negated: bool,
    },

    Between {
        col: ColRef,
        low: Literal,
        high: Literal,
        negated: bool,
    },

    IsNull {
        col: ColRef,
        negated: bool,
    },

    /// Null-safe comparison:
    ///
    /// `col IS [NOT] DISTINCT FROM lit`.
    ///
    /// Unlike normal SQL equality this is two-valued and never evaluates
    /// to NULL.
    Distinct {
        col: ColRef,
        lit: Literal,
        negated: bool,
    },

    /// A bare boolean column used directly as a predicate:
    ///
    /// `WHERE is_active`
    BoolCol(ColRef),

    /// `col [NOT] LIKE 'pattern'`.
    ///
    /// LIKE remains structural because normalization may later rewrite
    /// literal-prefix forms into comparison ranges.
    Like {
        col: ColRef,
        pattern: String,
        negated: bool,
    },

    /// A syntactically valid SQL expression that cannot be represented by
    /// the pruning predicate language.
    Unsupported {
        raw: String,
        reason: String,
    },
}

impl Pred {
    /// Build an AND while flattening an immediately nested AND.
    ///
    /// Used by the parser while translating sqlparser's binary expression
    /// tree into the n-ary predicate representation.
    pub(super) fn and2(a: Pred, b: Pred) -> Pred {
        let mut parts = match a {
            Pred::And(parts) => parts,
            other => vec![other],
        };

        match b {
            Pred::And(other) => parts.extend(other),
            other => parts.push(other),
        }

        Pred::And(parts)
    }

    /// Build an OR while flattening an immediately nested OR.
    pub(super) fn or2(a: Pred, b: Pred) -> Pred {
        let mut parts = match a {
            Pred::Or(parts) => parts,
            other => vec![other],
        };

        match b {
            Pred::Or(other) => parts.extend(other),
            other => parts.push(other),
        }

        Pred::Or(parts)
    }

    /// True when the tree contains something that cannot be lowered to the
    /// general pruning backend.
    ///
    /// A surviving LIKE counts as unsupported here because its semantics
    /// are known but it has no general lowering. A narrower interpreter,
    /// such as partition-literal evaluation, may still understand it.
    pub fn contains_unsupported(&self) -> bool {
        match self {
            Pred::And(parts) | Pred::Or(parts) => parts.iter().any(Pred::contains_unsupported),

            Pred::Not(inner) => inner.contains_unsupported(),

            Pred::Unsupported { .. } | Pred::Like { .. } => true,

            Pred::Cmp { .. }
            | Pred::In { .. }
            | Pred::Between { .. }
            | Pred::IsNull { .. }
            | Pred::Distinct { .. }
            | Pred::BoolCol(_) => false,
        }
    }

    /// True when the tree contains a subtree whose semantics are unknown.
    ///
    /// This is narrower than [`Pred::contains_unsupported`]. A LIKE has
    /// known semantics even if some interpreters cannot consume it;
    /// [`Pred::Unsupported`] means delta-explain does not model the
    /// expression's semantics at all.
    pub fn contains_opaque(&self) -> bool {
        match self {
            Pred::And(parts) | Pred::Or(parts) => parts.iter().any(Pred::contains_opaque),

            Pred::Not(inner) => inner.contains_opaque(),

            Pred::Unsupported { .. } => true,

            Pred::Cmp { .. }
            | Pred::In { .. }
            | Pred::Between { .. }
            | Pred::IsNull { .. }
            | Pred::Distinct { .. }
            | Pred::Like { .. }
            | Pred::BoolCol(_) => false,
        }
    }

    /// Return the dotted names of all columns explicitly represented in the
    /// predicate tree.
    pub fn columns(&self) -> Vec<String> {
        let mut columns = Vec::new();
        self.collect_columns(&mut columns);
        columns
    }

    fn collect_columns(&self, columns: &mut Vec<String>) {
        match self {
            Pred::And(parts) | Pred::Or(parts) => {
                for predicate in parts {
                    predicate.collect_columns(columns);
                }
            }

            Pred::Not(inner) => inner.collect_columns(columns),

            Pred::Cmp { col, .. }
            | Pred::In { col, .. }
            | Pred::Between { col, .. }
            | Pred::IsNull { col, .. }
            | Pred::Distinct { col, .. }
            | Pred::Like { col, .. }
            | Pred::BoolCol(col) => {
                columns.push(col.dotted());
            }

            Pred::Unsupported { .. } => {}
        }
    }

    /// Return the top-level conjuncts of the predicate.
    ///
    /// These are the units consumed later by predicate classification.
    pub fn conjuncts(&self) -> Vec<&Pred> {
        match self {
            Pred::And(parts) => parts.iter().collect(),
            other => vec![other],
        }
    }

    /// Return the reasons attached to expressions that cannot be consumed
    /// by the general pruning path.
    pub fn unsupported_reasons(&self) -> Vec<&str> {
        let mut reasons = Vec::new();
        self.collect_unsupported_reasons(&mut reasons);
        reasons
    }

    fn collect_unsupported_reasons<'a>(&'a self, reasons: &mut Vec<&'a str>) {
        match self {
            Pred::And(parts) | Pred::Or(parts) => {
                for predicate in parts {
                    predicate.collect_unsupported_reasons(reasons);
                }
            }

            Pred::Not(inner) => {
                inner.collect_unsupported_reasons(reasons);
            }

            Pred::Unsupported { reason, .. } => {
                reasons.push(reason);
            }

            Pred::Like { .. } => {
                reasons.push(
                    "this LIKE cannot prune here: only a non-negated literal-prefix pattern \
                     on a string column rewrites to a comparison range, and only a fragment \
                     referencing partition columns alone evaluates against partition values",
                );
            }

            Pred::Cmp { .. }
            | Pred::In { .. }
            | Pred::Between { .. }
            | Pred::IsNull { .. }
            | Pred::Distinct { .. }
            | Pred::BoolCol(_) => {}
        }
    }
}
