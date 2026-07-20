//! Cassandra type marshalling helpers.
//!
//! Maps Cassandra `AbstractType` class names to their on-disk properties.
//! Fixed-length types (e.g. `Int32Type`) are serialized as raw bytes without
//! a length prefix, while variable-length types use a varint length prefix.
//!
//! Reference: `AbstractType.valueLengthIfFixed()` in Cassandra source.

/// Returns the fixed byte length for a Cassandra type, or `None` for
/// variable-length types that use a varint length prefix.
///
/// The type name is the fully-qualified Cassandra class name, e.g.
/// `"org.apache.cassandra.db.marshal.Int32Type"`.
pub fn value_length_if_fixed(type_name: &str) -> Option<usize> {
    // Extract the simple class name after the last dot
    let simple = type_name.rsplit('.').next().unwrap_or(type_name);
    match simple {
        "BooleanType" => Some(1),
        "ByteType" | "TinyintType" => None, // variable-length in Cassandra
        "ShortType" | "SmallintType" => None, // variable-length in Cassandra
        "Int32Type" => Some(4),
        "LongType" | "CounterColumnType" => Some(8),
        "FloatType" => Some(4),
        "DoubleType" => Some(8),
        "TimestampType" | "DateType" => Some(8),
        "TimeType" => Some(8),
        "UUIDType" | "LexicalUUIDType" | "TimeUUIDType" => Some(16),
        "EmptyType" => Some(0),
        // Variable-length types: UTF8Type, AsciiType, BytesType, DecimalType,
        // IntegerType, InetAddressType, etc.
        _ => None,
    }
}

/// The simple class name of a (possibly parametric) Cassandra type string:
/// the segment after the last `.` and before any `(`. E.g.
/// `org.apache.cassandra.db.marshal.ListType(...)` -> `ListType`.
fn simple_class_name(type_name: &str) -> &str {
    let head = type_name.split('(').next().unwrap_or(type_name);
    head.rsplit('.').next().unwrap_or(head).trim()
}

/// The top-level type arguments of a parametric Cassandra type, e.g.
/// `MapType(A,B)` -> `["A", "B"]` where `A`/`B` keep their own nesting.
/// Returns `None` if the type has no parenthesized arguments.
fn top_level_args(type_name: &str) -> Option<Vec<&str>> {
    let open = type_name.find('(')?;
    let close = type_name.rfind(')')?;
    if close <= open {
        return None;
    }
    let inner = &type_name[open + 1..close];
    let mut args = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (i, ch) in inner.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                args.push(inner[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    args.push(inner[start..].trim());
    Some(args)
}

/// True if `type_name` is a **non-frozen (multicell) collection** — the columns
/// that use Cassandra's complex-cell on-disk layout (cells-count + per-element
/// cells with paths). A bare `ListType`/`SetType`/`MapType` is multicell; a
/// `FrozenType(...)` wrapper (or any other type) is a single value cell.
pub fn is_multicell_collection(type_name: &str) -> bool {
    matches!(
        simple_class_name(type_name),
        "ListType" | "SetType" | "MapType"
    )
}

/// For a multicell collection column, the element **value** type used to
/// serialize each element cell's value (to decide fixed- vs varint-length):
/// - `list<T>` -> `T`
/// - `map<K,V>` -> `V`
/// - `set<T>` -> `T` (in practice the element cell's value is empty)
///
/// Returns `None` for a non-collection type.
pub fn collection_value_type(type_name: &str) -> Option<&str> {
    let args = top_level_args(type_name)?;
    match simple_class_name(type_name) {
        "ListType" | "SetType" => args.first().copied(),
        "MapType" => args.get(1).copied(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIST_INT: &str =
        "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)";
    const SET_TEXT: &str =
        "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)";
    const MAP_TEXT_INT: &str = "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)";
    const FROZEN_LIST_INT: &str = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))";

    #[test]
    fn bare_collections_are_multicell() {
        assert!(is_multicell_collection(LIST_INT));
        assert!(is_multicell_collection(SET_TEXT));
        assert!(is_multicell_collection(MAP_TEXT_INT));
    }

    #[test]
    fn frozen_and_scalar_are_not_multicell() {
        assert!(!is_multicell_collection(FROZEN_LIST_INT));
        assert!(!is_multicell_collection(
            "org.apache.cassandra.db.marshal.Int32Type"
        ));
        assert!(!is_multicell_collection(
            "org.apache.cassandra.db.marshal.UTF8Type"
        ));
    }

    #[test]
    fn collection_value_type_extracts_element_or_map_value() {
        assert_eq!(
            collection_value_type(LIST_INT),
            Some("org.apache.cassandra.db.marshal.Int32Type")
        );
        assert_eq!(
            collection_value_type(SET_TEXT),
            Some("org.apache.cassandra.db.marshal.UTF8Type")
        );
        // Map -> the VALUE type (second arg), not the key.
        assert_eq!(
            collection_value_type(MAP_TEXT_INT),
            Some("org.apache.cassandra.db.marshal.Int32Type")
        );
        assert_eq!(
            collection_value_type("org.apache.cassandra.db.marshal.Int32Type"),
            None
        );
    }

    #[test]
    fn collection_value_type_handles_nested_map_value() {
        // map<text, list<int>>: the value type is the whole nested ListType(...).
        let nested = "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))";
        assert_eq!(
            collection_value_type(nested),
            Some("org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)")
        );
    }

    #[test]
    fn known_fixed_length_types() {
        assert_eq!(
            value_length_if_fixed("org.apache.cassandra.db.marshal.Int32Type"),
            Some(4)
        );
        assert_eq!(
            value_length_if_fixed("org.apache.cassandra.db.marshal.LongType"),
            Some(8)
        );
        assert_eq!(
            value_length_if_fixed("org.apache.cassandra.db.marshal.UUIDType"),
            Some(16)
        );
        assert_eq!(
            value_length_if_fixed("org.apache.cassandra.db.marshal.BooleanType"),
            Some(1)
        );
    }

    #[test]
    fn known_variable_length_types() {
        assert_eq!(
            value_length_if_fixed("org.apache.cassandra.db.marshal.UTF8Type"),
            None
        );
        assert_eq!(
            value_length_if_fixed("org.apache.cassandra.db.marshal.BytesType"),
            None
        );
        assert_eq!(
            value_length_if_fixed("org.apache.cassandra.db.marshal.AsciiType"),
            None
        );
    }

    #[test]
    fn unknown_type_is_variable() {
        assert_eq!(value_length_if_fixed("com.example.CustomType"), None);
    }
}
