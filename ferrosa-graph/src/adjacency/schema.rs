//! Adjacency table schema definition.
//!
//! Each keyspace with graph edge tables gets a system adjacency table:
//! `system_graph_<keyspace>.adjacency`
//!
//! Schema:
//! ```sql
//! CREATE TABLE system_graph_<ks>.adjacency (
//!     vertex_id BLOB,
//!     direction TINYINT,    -- 0=OUT, 1=IN
//!     edge_label TEXT,
//!     neighbor_id BLOB,
//!     edge_table TEXT,
//!     PRIMARY KEY (vertex_id, direction, edge_label, neighbor_id)
//! );
//! ```

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use uuid::Uuid;

use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
use ferrosa_schema::metadata::table::{TableFlag, TableMetadata, TableParams};

/// Direction constants for adjacency entries.
pub const DIRECTION_OUT: u8 = 0;
pub const DIRECTION_IN: u8 = 1;

/// Returns the system keyspace name for a user keyspace's adjacency data.
pub fn adjacency_keyspace_name(user_keyspace: &str) -> String {
    format!("system_graph_{user_keyspace}")
}

/// Build the TableMetadata for the adjacency table in a given keyspace.
pub fn adjacency_table_metadata(user_keyspace: &str) -> TableMetadata {
    let ks = adjacency_keyspace_name(user_keyspace);

    let mut columns = IndexMap::new();

    columns.insert(
        "vertex_id".to_string(),
        ColumnMetadata {
            name: "vertex_id".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 0,
            column_type: "blob".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );
    columns.insert(
        "direction".to_string(),
        ColumnMetadata {
            name: "direction".to_string(),
            kind: ColumnKind::Clustering,
            position: 0,
            column_type: "tinyint".to_string(),
            clustering_order: ClusteringOrder::Asc,
            mask: None,
        },
    );
    columns.insert(
        "edge_label".to_string(),
        ColumnMetadata {
            name: "edge_label".to_string(),
            kind: ColumnKind::Clustering,
            position: 1,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::Asc,
            mask: None,
        },
    );
    columns.insert(
        "neighbor_id".to_string(),
        ColumnMetadata {
            name: "neighbor_id".to_string(),
            kind: ColumnKind::Clustering,
            position: 2,
            column_type: "blob".to_string(),
            clustering_order: ClusteringOrder::Asc,
            mask: None,
        },
    );
    columns.insert(
        "edge_table".to_string(),
        ColumnMetadata {
            name: "edge_table".to_string(),
            kind: ColumnKind::Regular,
            position: 0,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );

    let mut flags = HashSet::new();
    flags.insert(TableFlag::Compound);

    TableMetadata {
        keyspace: ks,
        name: "adjacency".to_string(),
        id: Uuid::new_v4(),
        columns,
        partition_key: vec!["vertex_id".to_string()],
        clustering_key: vec![
            ("direction".to_string(), ClusteringOrder::Asc),
            ("edge_label".to_string(), ClusteringOrder::Asc),
            ("neighbor_id".to_string(), ClusteringOrder::Asc),
        ],
        params: TableParams::default(),
        flags,
        extensions: HashMap::new(),
        is_system: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjacency_keyspace_naming() {
        assert_eq!(adjacency_keyspace_name("social"), "system_graph_social");
        assert_eq!(adjacency_keyspace_name("my_ks"), "system_graph_my_ks");
    }

    #[test]
    fn adjacency_table_is_system() {
        let meta = adjacency_table_metadata("social");
        assert!(meta.is_system);
        assert_eq!(meta.keyspace, "system_graph_social");
        assert_eq!(meta.name, "adjacency");
    }

    #[test]
    fn adjacency_table_has_correct_columns() {
        let meta = adjacency_table_metadata("test");
        assert_eq!(meta.columns.len(), 5);
        assert!(meta.columns.contains_key("vertex_id"));
        assert!(meta.columns.contains_key("direction"));
        assert!(meta.columns.contains_key("edge_label"));
        assert!(meta.columns.contains_key("neighbor_id"));
        assert!(meta.columns.contains_key("edge_table"));
    }

    #[test]
    fn adjacency_table_primary_key() {
        let meta = adjacency_table_metadata("test");
        assert_eq!(meta.partition_key, vec!["vertex_id"]);
        assert_eq!(meta.clustering_key.len(), 3);
    }
}
