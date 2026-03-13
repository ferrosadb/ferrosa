//! CQL type system: type identifiers, value encoding, and decoding.
//!
//! The CQL native protocol assigns a 16-bit type ID to each data type.
//! Collection types (list, map, set) carry their element type IDs inline.
//! `CqlValue` is the runtime representation used throughout query execution.

/// CQL data type with protocol type ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CqlType {
    Ascii,   // 0x0001
    Bigint,  // 0x0002
    Blob,    // 0x0003
    Boolean, // 0x0004
    Counter, // 0x0005
    Decimal, // 0x0006
    Double,  // 0x0007
    Float,   // 0x0008
    Int,     // 0x0009
    // 0x000A = custom, not supported
    Timestamp, // 0x000B
    Uuid,      // 0x000C
    Varchar,   // 0x000D
    Varint,    // 0x000E
    Timeuuid,  // 0x000F
    Inet,      // 0x0010
    Date,      // 0x0011
    Time,      // 0x0012
    Smallint,  // 0x0013
    Tinyint,   // 0x0014
    // 0x0015 = duration, deferred
    // 0x0030 = UDT, deferred
    List(Box<CqlType>),              // 0x0020
    Map(Box<CqlType>, Box<CqlType>), // 0x0021
    Set(Box<CqlType>),               // 0x0022
    Tuple(Vec<CqlType>),             // 0x0031
}

impl CqlType {
    /// Returns the protocol type ID for this type.
    pub fn type_id(&self) -> u16 {
        match self {
            Self::Ascii => 0x0001,
            Self::Bigint => 0x0002,
            Self::Blob => 0x0003,
            Self::Boolean => 0x0004,
            Self::Counter => 0x0005,
            Self::Decimal => 0x0006,
            Self::Double => 0x0007,
            Self::Float => 0x0008,
            Self::Int => 0x0009,
            Self::Timestamp => 0x000B,
            Self::Uuid => 0x000C,
            Self::Varchar => 0x000D,
            Self::Varint => 0x000E,
            Self::Timeuuid => 0x000F,
            Self::Inet => 0x0010,
            Self::Date => 0x0011,
            Self::Time => 0x0012,
            Self::Smallint => 0x0013,
            Self::Tinyint => 0x0014,
            Self::List(_) => 0x0020,
            Self::Map(_, _) => 0x0021,
            Self::Set(_) => 0x0022,
            Self::Tuple(_) => 0x0031,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_id_roundtrip() {
        let types = [
            (0x0001, CqlType::Ascii),
            (0x0002, CqlType::Bigint),
            (0x0003, CqlType::Blob),
            (0x0004, CqlType::Boolean),
            (0x0005, CqlType::Counter),
            (0x0006, CqlType::Decimal),
            (0x0007, CqlType::Double),
            (0x0008, CqlType::Float),
            (0x0009, CqlType::Int),
            (0x000B, CqlType::Timestamp),
            (0x000C, CqlType::Uuid),
            (0x000D, CqlType::Varchar),
            (0x000E, CqlType::Varint),
            (0x000F, CqlType::Timeuuid),
            (0x0010, CqlType::Inet),
            (0x0011, CqlType::Date),
            (0x0012, CqlType::Time),
            (0x0013, CqlType::Smallint),
            (0x0014, CqlType::Tinyint),
            (0x0020, CqlType::List(Box::new(CqlType::Int))),
            (
                0x0021,
                CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int)),
            ),
            (0x0022, CqlType::Set(Box::new(CqlType::Uuid))),
        ];
        for &(id, ref expected_variant) in &types {
            if !matches!(
                expected_variant,
                CqlType::List(_) | CqlType::Map(_, _) | CqlType::Set(_)
            ) {
                assert_eq!(expected_variant.type_id(), id);
            }
        }
    }

    #[test]
    fn type_id_for_collections() {
        assert_eq!(CqlType::List(Box::new(CqlType::Int)).type_id(), 0x0020);
        assert_eq!(
            CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int)).type_id(),
            0x0021
        );
        assert_eq!(CqlType::Set(Box::new(CqlType::Uuid)).type_id(), 0x0022);
    }
}
