use delta_kernel::schema::{DataType, PrimitiveType, SchemaRef};

use crate::v2::analysis::predicate::ColRef;

/// Return true when the referenced column resolves to a STRING leaf.
///
/// Unknown columns return false. This is conservative: a LIKE expression
/// whose type cannot be established remains structural instead of being
/// rewritten into a comparison range.
pub(crate) fn column_is_string(col: &ColRef, schema: &SchemaRef) -> bool {
    matches!(
        resolve_column_type(col, schema),
        Some(DataType::Primitive(PrimitiveType::String))
    )
}

/// Resolve a possibly nested column reference to its Delta data type.
///
/// For example:
///
/// `profile.geo.zip`
///
/// walks through nested struct fields until the leaf type is reached.
///
/// Returning `None` means that the column cannot be resolved against the
/// schema.
pub(super) fn resolve_column_type(col: &ColRef, schema: &SchemaRef) -> Option<DataType> {
    let mut parts = col.0.iter();

    let first = parts.next()?;

    let mut current = schema.field(first)?.data_type().clone();

    for part in parts {
        let DataType::Struct(struct_type) = &current else {
            return None;
        };

        current = struct_type.field(part)?.data_type().clone();
    }

    Some(current)
}
