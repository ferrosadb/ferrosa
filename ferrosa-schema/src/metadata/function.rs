//! User-defined function metadata.

use ferrosa_common::CqlType;
use serde::{Deserialize, Serialize};

/// Stable identity for a user-defined function overload.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FunctionIdentity {
    pub keyspace: String,
    pub name: String,
    pub arg_types: Vec<CqlType>,
}

/// Metadata for a user-defined function (UDF).
///
/// Functions are keyed by (keyspace, name, arg_types) to support overloading.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserFunctionMetadata {
    pub keyspace: String,
    pub name: String,
    pub arg_names: Vec<String>,
    pub arg_types: Vec<CqlType>,
    pub return_type: CqlType,
    pub called_on_null: bool,
    pub language: String,
    /// WASM binary as hex-encoded string (stored in schema, replicated via Raft).
    pub body: String,
}

impl UserFunctionMetadata {
    pub fn identity(&self) -> FunctionIdentity {
        FunctionIdentity {
            keyspace: self.keyspace.clone(),
            name: self.name.clone(),
            arg_types: self.arg_types.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn function_metadata_serde_roundtrip() {
        let meta = UserFunctionMetadata {
            keyspace: "ks".into(),
            name: "double_it".into(),
            arg_names: vec!["val".into()],
            arg_types: vec![CqlType::Int],
            return_type: CqlType::Int,
            called_on_null: true,
            language: "wasm".into(),
            body: "deadbeef".into(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: UserFunctionMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, back);
    }

    #[test]
    fn function_metadata_empty_params() {
        let meta = UserFunctionMetadata {
            keyspace: "ks".into(),
            name: "now_utc".into(),
            arg_names: vec![],
            arg_types: vec![],
            return_type: CqlType::Timestamp,
            called_on_null: true,
            language: "wasm".into(),
            body: "cafe".into(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: UserFunctionMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(back.arg_names.len(), 0);
    }

    #[test]
    fn function_identity_includes_argument_types() {
        let int_identity = FunctionIdentity {
            keyspace: "ks".into(),
            name: "normalize".into(),
            arg_types: vec![CqlType::Int],
        };
        let text_identity = FunctionIdentity {
            keyspace: "ks".into(),
            name: "normalize".into(),
            arg_types: vec![CqlType::Varchar],
        };

        assert_ne!(int_identity, text_identity);
    }

    #[test]
    fn function_metadata_reports_overload_identity() {
        let meta = UserFunctionMetadata {
            keyspace: "ks".into(),
            name: "normalize".into(),
            arg_names: vec!["val".into()],
            arg_types: vec![CqlType::Int],
            return_type: CqlType::Int,
            called_on_null: true,
            language: "wasm".into(),
            body: "deadbeef".into(),
        };

        assert_eq!(
            meta.identity(),
            FunctionIdentity {
                keyspace: "ks".into(),
                name: "normalize".into(),
                arg_types: vec![CqlType::Int],
            }
        );
    }
}
