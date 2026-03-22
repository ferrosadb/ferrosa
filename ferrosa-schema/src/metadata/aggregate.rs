//! User-defined aggregate metadata.

use ferrosa_common::{CqlType, CqlValue};
use serde::{Deserialize, Serialize};

/// Metadata for a user-defined aggregate (UDA).
///
/// Aggregates compose two UDFs: a state function called per row and an
/// optional final function called once after all rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAggregateMetadata {
    pub keyspace: String,
    pub name: String,
    pub arg_types: Vec<CqlType>,
    pub state_func: String,
    pub state_type: CqlType,
    pub final_func: Option<String>,
    pub init_cond: Option<CqlValue>,
    pub return_type: CqlType,
    /// Optional WASM binary for monolithic UDA component (high-performance path).
    /// When present, the component exports init/accumulate/finalize/merge/serialize-state.
    /// When absent, the classic sfunc/final_func composition path is used.
    pub wasm_body: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_metadata_serde_roundtrip() {
        let meta = UserAggregateMetadata {
            keyspace: "ks".into(),
            name: "my_avg".into(),
            arg_types: vec![CqlType::Int],
            state_func: "avg_state".into(),
            state_type: CqlType::Tuple(vec![CqlType::Bigint, CqlType::Int]),
            final_func: Some("avg_final".into()),
            init_cond: None,
            return_type: CqlType::Double,
            wasm_body: None,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: UserAggregateMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, back);
    }

    #[test]
    fn aggregate_metadata_no_final_func() {
        let meta = UserAggregateMetadata {
            keyspace: "ks".into(),
            name: "my_sum".into(),
            arg_types: vec![CqlType::Int],
            state_func: "sum_state".into(),
            state_type: CqlType::Bigint,
            final_func: None,
            init_cond: Some(CqlValue::Bigint(0)),
            return_type: CqlType::Bigint,
            wasm_body: None,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: UserAggregateMetadata = serde_json::from_str(&json).unwrap();
        assert!(back.final_func.is_none());
        assert!(back.init_cond.is_some());
    }
}
