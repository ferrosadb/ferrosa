//! User-defined type (UDT) metadata.

use ferrosa_common::CqlType;
use serde::{Deserialize, Serialize};

/// Metadata for a user-defined type.
///
/// UDTs are composite types with named fields, scoped to a keyspace.
/// Fields are ordered — insertion order is preserved and determines
/// wire encoding order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserTypeMetadata {
    pub keyspace: String,
    pub name: String,
    /// Ordered list of (field_name, field_type).
    pub fields: Vec<(String, CqlType)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_type_metadata_serde_roundtrip() {
        let udt = UserTypeMetadata {
            keyspace: "ks".into(),
            name: "address".into(),
            fields: vec![
                ("street".into(), CqlType::Varchar),
                ("city".into(), CqlType::Varchar),
                ("zip".into(), CqlType::Int),
            ],
        };
        let json = serde_json::to_string(&udt).unwrap();
        let back: UserTypeMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(udt, back);
    }

    #[test]
    fn user_type_metadata_field_order_preserved() {
        let udt = UserTypeMetadata {
            keyspace: "ks".into(),
            name: "contact".into(),
            fields: vec![
                ("first".into(), CqlType::Varchar),
                ("last".into(), CqlType::Varchar),
                ("age".into(), CqlType::Int),
                ("email".into(), CqlType::Varchar),
                ("active".into(), CqlType::Boolean),
            ],
        };
        let json = serde_json::to_string(&udt).unwrap();
        let back: UserTypeMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(back.fields.len(), 5);
        assert_eq!(back.fields[0].0, "first");
        assert_eq!(back.fields[1].0, "last");
        assert_eq!(back.fields[2].0, "age");
        assert_eq!(back.fields[3].0, "email");
        assert_eq!(back.fields[4].0, "active");
    }

    #[test]
    fn user_type_metadata_empty_fields() {
        let udt = UserTypeMetadata {
            keyspace: "ks".into(),
            name: "empty_type".into(),
            fields: vec![],
        };
        let json = serde_json::to_string(&udt).unwrap();
        let back: UserTypeMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(udt, back);
        assert!(back.fields.is_empty());
    }
}
