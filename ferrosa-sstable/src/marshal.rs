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

#[cfg(test)]
mod tests {
    use super::*;

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
