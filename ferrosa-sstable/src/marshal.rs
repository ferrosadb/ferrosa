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

/// Iterates the top-level type arguments of a parametric Cassandra type,
/// e.g. `MapType(A,B)` yields `"A"`, then `"B"` (each keeping its own
/// nesting), without collecting them into a `Vec`. `collection_value_type`
/// is on the row-body encoding hot path (`writer.rs` calls it once per
/// complex-column run, per row) and only ever needs the first or second
/// argument, so allocating a `Vec` of all of them — as a naive top-level
/// split would — is wasted work on every row of every complex column.
struct TopLevelArgs<'a> {
    inner: &'a str,
    pos: usize,
    done: bool,
}

impl<'a> Iterator for TopLevelArgs<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        if self.done {
            return None;
        }
        let bytes = self.inner.as_bytes();
        let start = self.pos;
        let mut depth = 0usize;
        let mut i = self.pos;
        while i < bytes.len() {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                b',' if depth == 0 => {
                    self.pos = i + 1;
                    return Some(self.inner[start..i].trim());
                }
                _ => {}
            }
            i += 1;
        }
        self.done = true;
        Some(self.inner[start..].trim())
    }
}

/// Returns `None` if `type_name` has no parenthesized arguments.
fn top_level_args(type_name: &str) -> Option<TopLevelArgs<'_>> {
    let open = type_name.find('(')?;
    let close = type_name.rfind(')')?;
    if close <= open {
        return None;
    }
    Some(TopLevelArgs {
        inner: &type_name[open + 1..close],
        pos: 0,
        done: false,
    })
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

/// True if `type_name` is a **non-frozen (multicell) UDT** (`UserType(...)`).
/// A non-frozen UDT is also a complex column: each field is a cell whose cell
/// path is a 2-byte big-endian field position. `FrozenType(UserType(..))` is a
/// single value cell.
pub fn is_nonfrozen_udt(type_name: &str) -> bool {
    simple_class_name(type_name) == "UserType"
}

/// True if `type_name` uses Cassandra's **complex** (per-element / per-field)
/// cell layout: a non-frozen collection or a non-frozen UDT.
pub fn is_multicell(type_name: &str) -> bool {
    is_multicell_collection(type_name) || is_nonfrozen_udt(type_name)
}

/// For a multicell collection column, the element **value** type used to
/// serialize each element cell's value (to decide fixed- vs varint-length):
/// - `list<T>` -> `T`
/// - `map<K,V>` -> `V`
/// - `set<T>` -> `T` (in practice the element cell's value is empty)
///
/// Returns `None` for a non-collection type.
pub fn collection_value_type(type_name: &str) -> Option<&str> {
    let mut args = top_level_args(type_name)?;
    match simple_class_name(type_name) {
        "ListType" | "SetType" => args.next(),
        "MapType" => args.nth(1),
        _ => None,
    }
}

/// For a multicell `map<K,V>` column, the key type `K` (each element cell's
/// path). `None` for any other type.
pub fn collection_key_type(type_name: &str) -> Option<&str> {
    match simple_class_name(type_name) {
        "MapType" => top_level_args(type_name)?.next(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_key_type_is_the_first_argument() {
        assert_eq!(
            collection_key_type(MAP_TEXT_INT),
            Some("org.apache.cassandra.db.marshal.UTF8Type")
        );
        assert_eq!(collection_key_type(LIST_INT), None);
        assert_eq!(collection_key_type(SET_TEXT), None);
    }

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

    const UDT: &str = "org.apache.cassandra.db.marshal.UserType(test,61646472,73:org.apache.cassandra.db.marshal.UTF8Type,7a:org.apache.cassandra.db.marshal.Int32Type)";
    const FROZEN_UDT: &str = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(test,61646472))";

    #[test]
    fn nonfrozen_udt_is_multicell_but_frozen_is_not() {
        assert!(is_nonfrozen_udt(UDT));
        assert!(is_multicell(UDT));
        assert!(!is_nonfrozen_udt(FROZEN_UDT));
        assert!(!is_multicell(FROZEN_UDT));
        // A UDT is not a collection.
        assert!(!is_multicell_collection(UDT));
        // Collections are still multicell.
        assert!(is_multicell(LIST_INT));
        assert!(!is_multicell("org.apache.cassandra.db.marshal.Int32Type"));
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
