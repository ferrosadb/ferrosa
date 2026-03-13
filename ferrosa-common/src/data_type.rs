//! CQL data type descriptors.
//!
//! [`DataType`] names the CQL scalar types that can appear in column
//! definitions. It is intentionally `#[non_exhaustive]` so that new types
//! (collections, UDTs, counters) can be added in future releases without
//! breaking downstream crates.
//!
//! The display representation uses the lowercase CQL keyword, e.g.
//! `DataType::BigInt` formats as `"bigint"`.

use std::fmt;

/// A CQL scalar type descriptor.
///
/// Used in column definitions to describe what kind of data a column holds.
/// The raw bytes for a value live in [`crate::CellValue`]; `DataType` adds
/// the type layer on top.
///
/// `#[non_exhaustive]` allows adding variants (e.g. `List`, `Map`, `Udt`)
/// without breaking downstream match arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DataType {
    /// UTF-8 string (`text` / `varchar`).
    Text,
    /// 32-bit signed integer.
    Int,
    /// 64-bit signed integer.
    BigInt,
    /// 64-bit IEEE 754 floating-point.
    Double,
    /// Boolean.
    Boolean,
    /// Version 4 UUID.
    Uuid,
    /// Timestamp (milliseconds since epoch in CQL; stored as `bigint`).
    Timestamp,
    /// Arbitrary bytes.
    Blob,
}

impl DataType {
    /// Returns `true` for numeric types: [`Int`](DataType::Int),
    /// [`BigInt`](DataType::BigInt), and [`Double`](DataType::Double).
    pub fn is_numeric(&self) -> bool {
        matches!(self, DataType::Int | DataType::BigInt | DataType::Double)
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            DataType::Text => "text",
            DataType::Int => "int",
            DataType::BigInt => "bigint",
            DataType::Double => "double",
            DataType::Boolean => "boolean",
            DataType::Uuid => "uuid",
            DataType::Timestamp => "timestamp",
            DataType::Blob => "blob",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_type_display() {
        assert_eq!(DataType::Text.to_string(), "text");
        assert_eq!(DataType::BigInt.to_string(), "bigint");
        assert_eq!(DataType::Uuid.to_string(), "uuid");
        assert_eq!(DataType::Timestamp.to_string(), "timestamp");
    }

    #[test]
    fn data_type_is_numeric() {
        assert!(DataType::Int.is_numeric());
        assert!(DataType::BigInt.is_numeric());
        assert!(DataType::Double.is_numeric());
        assert!(!DataType::Text.is_numeric());
        assert!(!DataType::Uuid.is_numeric());
    }
}
