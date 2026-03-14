//! Index metadata for secondary indexes.

use ferrosa_index::{FilterPredicate, IndexType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metadata describing a secondary index on a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// Keyspace the indexed table belongs to.
    pub keyspace: String,
    /// Table the index is built on.
    pub table: String,
    /// Name of the index.
    pub name: String,
    /// Type of index (BTree, Hash, Composite, etc.).
    pub index_type: IndexType,
    /// Columns targeted by this index.
    pub target_columns: Vec<String>,
    /// Optional filter predicate for partial indexes.
    pub filter_predicate: Option<FilterPredicate>,
    /// Additional index options (implementation-specific).
    pub options: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_index::FilterOp;

    #[test]
    fn index_metadata_serde_roundtrip() {
        let meta = IndexMetadata {
            keyspace: "ks".into(),
            table: "tbl".into(),
            name: "idx_email".into(),
            index_type: IndexType::BTree,
            target_columns: vec!["email".into()],
            filter_predicate: None,
            options: HashMap::new(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta.name, back.name);
        assert_eq!(meta.target_columns, back.target_columns);
    }

    #[test]
    fn index_metadata_with_filter() {
        let meta = IndexMetadata {
            keyspace: "ks".into(),
            table: "tbl".into(),
            name: "idx_active".into(),
            index_type: IndexType::Filtered {
                predicate: FilterPredicate {
                    column: "status".into(),
                    op: FilterOp::Eq,
                    value: b"active".to_vec(),
                },
                inner: Box::new(IndexType::BTree),
            },
            target_columns: vec!["email".into()],
            filter_predicate: Some(FilterPredicate {
                column: "status".into(),
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            }),
            options: HashMap::new(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert!(back.filter_predicate.is_some());
    }
}
