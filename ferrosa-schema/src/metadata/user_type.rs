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
