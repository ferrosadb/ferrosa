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

    fn make_index(name: &str, index_type: IndexType, columns: Vec<&str>) -> IndexMetadata {
        IndexMetadata {
            keyspace: "ks".into(),
            table: "tbl".into(),
            name: name.into(),
            index_type,
            target_columns: columns.into_iter().map(String::from).collect(),
            filter_predicate: None,
            options: HashMap::new(),
        }
    }

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
            index_type: IndexType::Filtered,
            target_columns: vec!["email".into()],
            filter_predicate: Some(FilterPredicate::single(2, FilterOp::Eq, b"active".to_vec())),
            options: HashMap::new(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert!(back.filter_predicate.is_some());
    }

    #[test]
    fn index_metadata_hash_roundtrip_all_fields() {
        let mut opts = HashMap::new();
        opts.insert("capacity".into(), "1024".into());
        let meta = IndexMetadata {
            keyspace: "ks".into(),
            table: "tbl".into(),
            name: "idx_hash".into(),
            index_type: IndexType::Hash,
            target_columns: vec!["user_id".into()],
            filter_predicate: None,
            options: opts,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(back.keyspace, "ks");
        assert_eq!(back.table, "tbl");
        assert_eq!(back.name, "idx_hash");
        assert!(matches!(back.index_type, IndexType::Hash));
        assert_eq!(back.target_columns, vec!["user_id"]);
        assert!(back.filter_predicate.is_none());
        assert_eq!(
            back.options.get("capacity").map(String::as_str),
            Some("1024")
        );
    }

    #[test]
    fn index_metadata_composite_roundtrip_all_fields() {
        let meta = make_index("idx_comp", IndexType::Composite, vec!["a", "b", "c"]);
        let json = serde_json::to_string(&meta).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(back.keyspace, "ks");
        assert_eq!(back.table, "tbl");
        assert_eq!(back.name, "idx_comp");
        assert!(matches!(back.index_type, IndexType::Composite));
        assert_eq!(back.target_columns, vec!["a", "b", "c"]);
        assert!(back.filter_predicate.is_none());
        assert!(back.options.is_empty());
    }

    #[test]
    fn index_metadata_phonetic_roundtrip_all_fields() {
        let meta = make_index("idx_phonetic", IndexType::Phonetic, vec!["last_name"]);
        let json = serde_json::to_string(&meta).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(back.keyspace, "ks");
        assert_eq!(back.table, "tbl");
        assert_eq!(back.name, "idx_phonetic");
        assert!(matches!(back.index_type, IndexType::Phonetic));
        assert_eq!(back.target_columns, vec!["last_name"]);
        assert!(back.filter_predicate.is_none());
        assert!(back.options.is_empty());
    }

    #[test]
    fn index_metadata_filtered_roundtrip_all_fields() {
        let meta = IndexMetadata {
            keyspace: "ks".into(),
            table: "tbl".into(),
            name: "idx_filtered".into(),
            index_type: IndexType::Filtered,
            target_columns: vec!["status".into()],
            filter_predicate: Some(FilterPredicate::single(0, FilterOp::Eq, b"active".to_vec())),
            options: HashMap::new(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(back.keyspace, "ks");
        assert_eq!(back.table, "tbl");
        assert_eq!(back.name, "idx_filtered");
        assert!(matches!(back.index_type, IndexType::Filtered));
        assert_eq!(back.target_columns, vec!["status"]);
        let pred = back.filter_predicate.unwrap();
        assert_eq!(pred.clauses().len(), 1);
        let clause = &pred.clauses()[0];
        assert_eq!(clause.column_position, 0);
        assert!(matches!(clause.op, FilterOp::Eq));
        assert_eq!(clause.value, b"active");
    }

    /// A persisted index metadata row carrying a multi-column conjunction
    /// predicate round-trips through serde (the shape stored in
    /// `system_schema.indexes`).
    #[test]
    fn index_metadata_conjunction_roundtrip() {
        use ferrosa_index::FilterClause;
        let meta = IndexMetadata {
            keyspace: "ks".into(),
            table: "tbl".into(),
            name: "idx_conj".into(),
            index_type: IndexType::Filtered,
            target_columns: vec!["name".into()],
            filter_predicate: Some(FilterPredicate::conjunction(vec![
                FilterClause::new(1, FilterOp::Gt, vec![0, 0, 0, 21]),
                FilterClause::new(2, FilterOp::Eq, b"eng".to_vec()),
            ])),
            options: HashMap::new(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        let pred = back.filter_predicate.unwrap();
        assert_eq!(pred.clauses().len(), 2);
        assert_eq!(pred.clauses()[0].column_position, 1);
        assert!(matches!(pred.clauses()[0].op, FilterOp::Gt));
        assert_eq!(pred.clauses()[1].column_position, 2);
        assert_eq!(pred.clauses()[1].value, b"eng");
    }

    /// Backward-compatible decode: a legacy single-clause flat JSON row (the
    /// exact shape the old `FilterPredicate` serialized) still deserializes into
    /// an `IndexMetadata` with a one-clause conjunction.
    #[test]
    fn index_metadata_legacy_single_clause_predicate_decodes() {
        let legacy = r#"{
            "keyspace":"ks","table":"tbl","name":"idx_legacy",
            "index_type":"Filtered","target_columns":["status"],
            "filter_predicate":{"column_position":3,"op":"Eq","value":[97,99,116,105,118,101]},
            "options":{}
        }"#;
        let back: IndexMetadata = serde_json::from_str(legacy).unwrap();
        let pred = back.filter_predicate.unwrap();
        assert_eq!(pred.clauses().len(), 1);
        assert_eq!(pred.clauses()[0].column_position, 3);
        assert!(matches!(pred.clauses()[0].op, FilterOp::Eq));
        assert_eq!(pred.clauses()[0].value, b"active");
    }
}
