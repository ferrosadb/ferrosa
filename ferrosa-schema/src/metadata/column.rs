//! Column metadata types.

use serde::{Deserialize, Serialize};

/// Metadata for a single column within a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMetadata {
    /// Column name.
    pub name: String,
    /// Column kind (partition key, clustering, regular, static).
    pub kind: ColumnKind,
    /// Position within its kind group (e.g., 0 for first partition key column).
    pub position: i32,
    /// CQL type name (e.g., "text", "int", "frozen<map<text, text>>").
    pub column_type: String,
    /// Clustering order for clustering columns; `None` for non-clustering columns.
    pub clustering_order: ClusteringOrder,
    /// Optional dynamic data masking configuration.
    pub mask: Option<ColumnMask>,
}

/// The kind of a column within a table's schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnKind {
    /// Part of the partition key.
    PartitionKey,
    /// A clustering column that determines row ordering within a partition.
    Clustering,
    /// A regular (non-key, non-static) column.
    Regular,
    /// A static column shared across all rows in a partition.
    Static,
}

/// Clustering order for a clustering column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClusteringOrder {
    /// Ascending order (default).
    Asc,
    /// Descending order.
    Desc,
    /// Not a clustering column.
    None,
}

/// Dynamic data masking configuration for a column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMask {
    /// Masking function name (e.g., "mask_default", "mask_replace").
    pub function_name: String,
    /// Arguments passed to the masking function.
    pub arguments: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_metadata_construction() {
        let col = ColumnMetadata {
            name: "user_id".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 0,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        };

        assert_eq!(col.name, "user_id");
        assert_eq!(col.kind, ColumnKind::PartitionKey);
        assert_eq!(col.position, 0);
        assert_eq!(col.column_type, "uuid");
        assert_eq!(col.clustering_order, ClusteringOrder::None);
        assert!(col.mask.is_none());
    }

    #[test]
    fn column_metadata_with_mask() {
        let col = ColumnMetadata {
            name: "email".to_string(),
            kind: ColumnKind::Regular,
            position: 0,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: Some(ColumnMask {
                function_name: "mask_default".to_string(),
                arguments: vec![],
            }),
        };

        assert!(col.mask.is_some());
        let mask = col.mask.as_ref().unwrap();
        assert_eq!(mask.function_name, "mask_default");
        assert!(mask.arguments.is_empty());
    }

    #[test]
    fn column_mask_with_arguments() {
        let mask = ColumnMask {
            function_name: "mask_replace".to_string(),
            arguments: vec!["***".to_string(), "text".to_string()],
        };

        assert_eq!(mask.function_name, "mask_replace");
        assert_eq!(mask.arguments.len(), 2);
        assert_eq!(mask.arguments[0], "***");
    }

    #[test]
    fn column_kind_all_variants() {
        let kinds = [
            ColumnKind::PartitionKey,
            ColumnKind::Clustering,
            ColumnKind::Regular,
            ColumnKind::Static,
        ];
        // Verify all variants are distinct
        for (i, a) in kinds.iter().enumerate() {
            for (j, b) in kinds.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn clustering_order_all_variants() {
        let orders = [
            ClusteringOrder::Asc,
            ClusteringOrder::Desc,
            ClusteringOrder::None,
        ];
        for (i, a) in orders.iter().enumerate() {
            for (j, b) in orders.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn column_metadata_serde_roundtrip() {
        let col = ColumnMetadata {
            name: "ts".to_string(),
            kind: ColumnKind::Clustering,
            position: 0,
            column_type: "timestamp".to_string(),
            clustering_order: ClusteringOrder::Desc,
            mask: Some(ColumnMask {
                function_name: "mask_default".to_string(),
                arguments: vec![],
            }),
        };

        let json = serde_json::to_string(&col).expect("serialize");
        let deserialized: ColumnMetadata = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(col, deserialized);
    }

    #[test]
    fn column_kind_serde_roundtrip() {
        for kind in &[
            ColumnKind::PartitionKey,
            ColumnKind::Clustering,
            ColumnKind::Regular,
            ColumnKind::Static,
        ] {
            let json = serde_json::to_string(kind).expect("serialize");
            let deserialized: ColumnKind = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(*kind, deserialized);
        }
    }

    #[test]
    fn clustering_order_serde_roundtrip() {
        for order in &[
            ClusteringOrder::Asc,
            ClusteringOrder::Desc,
            ClusteringOrder::None,
        ] {
            let json = serde_json::to_string(order).expect("serialize");
            let deserialized: ClusteringOrder = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(*order, deserialized);
        }
    }

    #[test]
    fn column_metadata_clone_eq() {
        let col = ColumnMetadata {
            name: "val".to_string(),
            kind: ColumnKind::Regular,
            position: 2,
            column_type: "int".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        };

        let cloned = col.clone();
        assert_eq!(col, cloned);
    }

    #[test]
    fn column_kind_is_hashable() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ColumnKind::PartitionKey);
        set.insert(ColumnKind::Clustering);
        set.insert(ColumnKind::Regular);
        set.insert(ColumnKind::Static);
        assert_eq!(set.len(), 4);
    }
}
