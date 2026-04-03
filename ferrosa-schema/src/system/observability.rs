//! `system_observability` keyspace and table schemas.
//!
//! Defines three tables for the built-in observability pipeline:
//! - `spans` — distributed tracing spans
//! - `metrics` — time-bucketed metric values
//! - `slow_queries` — queries exceeding a latency threshold

use ferrosa_common::schema::{ColumnDefinition, TableSchema};

/// Cassandra marshal type constants used in column definitions.
mod types {
    pub const UUID: &str = "org.apache.cassandra.db.marshal.UUIDType";
    pub const TEXT: &str = "org.apache.cassandra.db.marshal.UTF8Type";
    pub const BIGINT: &str = "org.apache.cassandra.db.marshal.LongType";
    pub const DOUBLE: &str = "org.apache.cassandra.db.marshal.DoubleType";
    pub const MAP_TEXT_TEXT: &str = "org.apache.cassandra.db.marshal.UTF8Type";
}

/// Keyspace name for the observability tables.
pub const KEYSPACE: &str = "system_observability";

/// Returns `TableSchema` for `system_observability.spans`.
///
/// Primary key: (trace_id, start_us, span_id)
/// - Partition key: trace_id (uuid)
/// - Clustering columns: start_us (bigint), span_id (uuid)
/// - Regular columns: parent_id, node_id, name, duration_us, status, attributes
pub fn spans_table_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: "spans".to_string(),
        key_type: types::UUID.to_string(),
        clustering_columns: vec![
            ColumnDefinition {
                name: "start_us".to_string(),
                type_name: types::BIGINT.to_string(),
            },
            ColumnDefinition {
                name: "span_id".to_string(),
                type_name: types::UUID.to_string(),
            },
        ],
        static_columns: vec![],
        regular_columns: vec![
            ColumnDefinition {
                name: "parent_id".to_string(),
                type_name: types::UUID.to_string(),
            },
            ColumnDefinition {
                name: "node_id".to_string(),
                type_name: types::UUID.to_string(),
            },
            ColumnDefinition {
                name: "name".to_string(),
                type_name: types::TEXT.to_string(),
            },
            ColumnDefinition {
                name: "duration_us".to_string(),
                type_name: types::BIGINT.to_string(),
            },
            ColumnDefinition {
                name: "status".to_string(),
                type_name: types::TEXT.to_string(),
            },
            ColumnDefinition {
                name: "attributes".to_string(),
                type_name: types::MAP_TEXT_TEXT.to_string(),
            },
        ],
        extensions: Default::default(),
    }
}

/// Returns `TableSchema` for `system_observability.metrics`.
///
/// Primary key: ((node_id, metric_name), bucket)
/// - Partition key: composite (node_id uuid, metric_name text)
/// - Clustering column: bucket (bigint)
/// - Regular columns: value, labels
pub fn metrics_table_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: "metrics".to_string(),
        // Composite partition key: node_id + metric_name
        key_type: "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type)".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "bucket".to_string(),
            type_name: types::BIGINT.to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![
            ColumnDefinition {
                name: "value".to_string(),
                type_name: types::DOUBLE.to_string(),
            },
            ColumnDefinition {
                name: "labels".to_string(),
                type_name: types::MAP_TEXT_TEXT.to_string(),
            },
        ],
        extensions: Default::default(),
    }
}

/// Returns `TableSchema` for `system_observability.slow_queries`.
///
/// Primary key: (node_id, timestamp)
/// - Partition key: node_id (uuid)
/// - Clustering column: timestamp (bigint)
/// - Regular columns: duration_us, keyspace, query_text, client_addr, trace_id
pub fn slow_queries_table_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: "slow_queries".to_string(),
        key_type: types::UUID.to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "timestamp".to_string(),
            type_name: types::BIGINT.to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![
            ColumnDefinition {
                name: "duration_us".to_string(),
                type_name: types::BIGINT.to_string(),
            },
            ColumnDefinition {
                name: "keyspace".to_string(),
                type_name: types::TEXT.to_string(),
            },
            ColumnDefinition {
                name: "query_text".to_string(),
                type_name: types::TEXT.to_string(),
            },
            ColumnDefinition {
                name: "client_addr".to_string(),
                type_name: types::TEXT.to_string(),
            },
            ColumnDefinition {
                name: "trace_id".to_string(),
                type_name: types::UUID.to_string(),
            },
        ],
        extensions: Default::default(),
    }
}

/// Returns all observability table schemas for registration at bootstrap.
pub fn all_observability_table_schemas() -> Vec<TableSchema> {
    vec![
        spans_table_schema(),
        metrics_table_schema(),
        slow_queries_table_schema(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spans_schema_layout() {
        let schema = spans_table_schema();
        assert_eq!(schema.keyspace, KEYSPACE);
        assert_eq!(schema.table, "spans");
        assert_eq!(schema.clustering_columns.len(), 2);
        assert_eq!(schema.clustering_columns[0].name, "start_us");
        assert_eq!(schema.clustering_columns[1].name, "span_id");
        assert_eq!(schema.regular_columns.len(), 6);
        let col_names: Vec<&str> = schema.regular_columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"parent_id"));
        assert!(col_names.contains(&"node_id"));
        assert!(col_names.contains(&"name"));
        assert!(col_names.contains(&"duration_us"));
        assert!(col_names.contains(&"status"));
        assert!(col_names.contains(&"attributes"));
    }

    #[test]
    fn metrics_schema_layout() {
        let schema = metrics_table_schema();
        assert_eq!(schema.keyspace, KEYSPACE);
        assert_eq!(schema.table, "metrics");
        assert!(schema.key_type.contains("CompositeType"));
        assert_eq!(schema.clustering_columns.len(), 1);
        assert_eq!(schema.clustering_columns[0].name, "bucket");
        assert_eq!(schema.regular_columns.len(), 2);
    }

    #[test]
    fn slow_queries_schema_layout() {
        let schema = slow_queries_table_schema();
        assert_eq!(schema.keyspace, KEYSPACE);
        assert_eq!(schema.table, "slow_queries");
        assert_eq!(schema.clustering_columns.len(), 1);
        assert_eq!(schema.clustering_columns[0].name, "timestamp");
        assert_eq!(schema.regular_columns.len(), 5);
    }

    #[test]
    fn all_observability_table_schemas_returns_three() {
        let schemas = all_observability_table_schemas();
        assert_eq!(schemas.len(), 3);
        let names: Vec<(&str, &str)> = schemas
            .iter()
            .map(|s| (s.keyspace.as_str(), s.table.as_str()))
            .collect();
        assert!(names.contains(&(KEYSPACE, "spans")));
        assert!(names.contains(&(KEYSPACE, "metrics")));
        assert!(names.contains(&(KEYSPACE, "slow_queries")));
    }
}
