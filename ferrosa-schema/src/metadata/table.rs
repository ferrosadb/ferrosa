//! Table metadata types.

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::column::{ClusteringOrder, ColumnMetadata};

/// Full metadata for a Cassandra table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Keyspace this table belongs to.
    pub keyspace: String,
    /// Table name.
    pub name: String,
    /// Unique table identifier.
    pub id: Uuid,
    /// Columns indexed by name, preserving insertion order.
    pub columns: IndexMap<String, ColumnMetadata>,
    /// Partition key column names, in order.
    pub partition_key: Vec<String>,
    /// Clustering key column names with their sort order.
    pub clustering_key: Vec<(String, ClusteringOrder)>,
    /// Table-level configuration parameters.
    pub params: TableParams,
    /// Table flags describing the table's characteristics.
    pub flags: HashSet<TableFlag>,
    /// Opaque key-value extensions on the table (e.g., graph.type, graph.label).
    pub extensions: HashMap<String, String>,
    /// Whether this is a system-managed table (protected from user DDL).
    #[serde(default)]
    pub is_system: bool,
}

/// Flags describing table characteristics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableFlag {
    /// Table has compound primary key.
    Compound,
    /// Table contains counter columns.
    Counter,
    /// Table uses dense storage (legacy Thrift tables).
    Dense,
    /// Table uses super columns (legacy Thrift tables).
    Super,
}

/// Table-level configuration parameters matching Cassandra's table options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableParams {
    /// Target false positive probability for Bloom filters.
    pub bloom_filter_fp_chance: f64,
    /// Row and key caching configuration.
    pub caching: CachingParams,
    /// Human-readable table comment.
    pub comment: String,
    /// Compaction strategy and its options.
    pub compaction: HashMap<String, String>,
    /// Compression algorithm and its options.
    pub compression: HashMap<String, String>,
    /// Probability of verifying CRC checksums on reads.
    pub crc_check_chance: f64,
    /// Default TTL in seconds for cells (0 = no expiration).
    pub default_time_to_live: i32,
    /// Grace period in seconds before tombstones are eligible for GC.
    pub gc_grace_seconds: i32,
    /// Maximum index interval for the partition index summary.
    pub max_index_interval: i32,
    /// Minimum index interval for the partition index summary.
    pub min_index_interval: i32,
    /// Memtable flush period in milliseconds (0 = flush only when full).
    pub memtable_flush_period_in_ms: i32,
    /// Speculative retry policy.
    pub speculative_retry: String,
    /// Additional write policy for speculative writes.
    pub additional_write_policy: String,
    /// Whether change data capture is enabled.
    pub cdc: bool,
    /// Read repair strategy.
    pub read_repair: String,
    /// Whether automatic snapshots are taken before schema changes.
    pub allow_auto_snapshot: bool,
    /// Whether incremental backups are enabled.
    pub incremental_backups: bool,
}

/// Caching configuration for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachingParams {
    /// Key cache mode: "ALL" or "NONE".
    pub keys: String,
    /// Row cache mode: "ALL", "NONE", or a numeric limit.
    pub rows_per_partition: String,
}

impl Default for CachingParams {
    fn default() -> Self {
        Self {
            keys: "ALL".to_string(),
            rows_per_partition: "NONE".to_string(),
        }
    }
}

impl Default for TableParams {
    fn default() -> Self {
        Self {
            bloom_filter_fp_chance: 0.01,
            caching: CachingParams::default(),
            comment: String::new(),
            compaction: HashMap::new(),
            compression: HashMap::new(),
            crc_check_chance: 1.0,
            default_time_to_live: 0,
            gc_grace_seconds: 864_000, // 10 days
            max_index_interval: 2048,
            min_index_interval: 128,
            memtable_flush_period_in_ms: 0,
            speculative_retry: "99PERCENTILE".to_string(),
            additional_write_policy: "99PERCENTILE".to_string(),
            cdc: false,
            read_repair: "BLOCKING".to_string(),
            allow_auto_snapshot: true,
            incremental_backups: true,
        }
    }
}

/// Optional updates for a table (partial update).
///
/// `None` fields are left unchanged; `Some` fields are applied.
/// Columns can be added or dropped in the same update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableUpdates {
    /// New table parameters, if changing.
    pub params: Option<TableParams>,
    /// Columns to add to the table.
    pub add_columns: Vec<ColumnMetadata>,
    /// Column names to drop from the table.
    pub drop_columns: Vec<String>,
    /// Extensions to add or update on the table.
    pub extensions: Option<HashMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use super::super::column::ColumnKind;
    use super::*;

    #[test]
    fn table_params_defaults() {
        let params = TableParams::default();

        assert!((params.bloom_filter_fp_chance - 0.01).abs() < f64::EPSILON);
        assert_eq!(params.gc_grace_seconds, 864_000);
        assert_eq!(params.default_time_to_live, 0);
        assert!(!params.cdc);
        assert_eq!(params.speculative_retry, "99PERCENTILE");
        assert_eq!(params.additional_write_policy, "99PERCENTILE");
        assert!((params.crc_check_chance - 1.0).abs() < f64::EPSILON);
        assert_eq!(params.max_index_interval, 2048);
        assert_eq!(params.min_index_interval, 128);
        assert_eq!(params.memtable_flush_period_in_ms, 0);
        assert_eq!(params.read_repair, "BLOCKING");
        assert!(params.allow_auto_snapshot);
        assert!(params.incremental_backups);
        assert!(params.comment.is_empty());
        assert!(params.compaction.is_empty());
        assert!(params.compression.is_empty());
    }

    #[test]
    fn caching_params_defaults() {
        let caching = CachingParams::default();

        assert_eq!(caching.keys, "ALL");
        assert_eq!(caching.rows_per_partition, "NONE");
    }

    #[test]
    fn table_params_serde_roundtrip() {
        let params = TableParams::default();

        let json = serde_json::to_string(&params).expect("serialize");
        let deserialized: TableParams = serde_json::from_str(&json).expect("deserialize");

        assert!(
            (params.bloom_filter_fp_chance - deserialized.bloom_filter_fp_chance).abs()
                < f64::EPSILON
        );
        assert_eq!(params.gc_grace_seconds, deserialized.gc_grace_seconds);
        assert_eq!(
            params.default_time_to_live,
            deserialized.default_time_to_live
        );
        assert_eq!(params.cdc, deserialized.cdc);
        assert_eq!(params.speculative_retry, deserialized.speculative_retry);
        assert_eq!(params.caching.keys, deserialized.caching.keys);
        assert_eq!(
            params.caching.rows_per_partition,
            deserialized.caching.rows_per_partition
        );
    }

    #[test]
    fn table_flag_all_variants() {
        let flags: HashSet<TableFlag> = [
            TableFlag::Compound,
            TableFlag::Counter,
            TableFlag::Dense,
            TableFlag::Super,
        ]
        .into_iter()
        .collect();

        assert_eq!(flags.len(), 4);
        assert!(flags.contains(&TableFlag::Compound));
        assert!(flags.contains(&TableFlag::Counter));
        assert!(flags.contains(&TableFlag::Dense));
        assert!(flags.contains(&TableFlag::Super));
    }

    #[test]
    fn table_flag_serde_roundtrip() {
        for flag in &[
            TableFlag::Compound,
            TableFlag::Counter,
            TableFlag::Dense,
            TableFlag::Super,
        ] {
            let json = serde_json::to_string(flag).expect("serialize");
            let deserialized: TableFlag = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(*flag, deserialized);
        }
    }

    #[test]
    fn table_metadata_construction() {
        let mut columns = IndexMap::new();
        columns.insert(
            "user_id".to_string(),
            ColumnMetadata {
                name: "user_id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "ts".to_string(),
            ColumnMetadata {
                name: "ts".to_string(),
                kind: ColumnKind::Clustering,
                position: 0,
                column_type: "timestamp".to_string(),
                clustering_order: ClusteringOrder::Desc,
                mask: None,
            },
        );
        columns.insert(
            "data".to_string(),
            ColumnMetadata {
                name: "data".to_string(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );

        let mut flags = HashSet::new();
        flags.insert(TableFlag::Compound);

        let table = TableMetadata {
            keyspace: "my_ks".to_string(),
            name: "events".to_string(),
            id: Uuid::new_v4(),
            columns,
            partition_key: vec!["user_id".to_string()],
            clustering_key: vec![("ts".to_string(), ClusteringOrder::Desc)],
            params: TableParams::default(),
            flags,
            extensions: HashMap::new(),
            is_system: false,
        };

        assert_eq!(table.keyspace, "my_ks");
        assert_eq!(table.name, "events");
        assert_eq!(table.columns.len(), 3);
        assert_eq!(table.partition_key, vec!["user_id"]);
        assert_eq!(table.clustering_key.len(), 1);
        assert_eq!(table.clustering_key[0].0, "ts");
        assert_eq!(table.clustering_key[0].1, ClusteringOrder::Desc);
        assert!(table.flags.contains(&TableFlag::Compound));
    }

    #[test]
    fn table_metadata_serde_roundtrip() {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            ColumnMetadata {
                name: "id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "int".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );

        let mut flags = HashSet::new();
        flags.insert(TableFlag::Compound);

        let table = TableMetadata {
            keyspace: "ks".to_string(),
            name: "t".to_string(),
            id: Uuid::new_v4(),
            columns,
            partition_key: vec!["id".to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags,
            extensions: HashMap::new(),
            is_system: false,
        };

        let json = serde_json::to_string(&table).expect("serialize");
        let deserialized: TableMetadata = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(table.keyspace, deserialized.keyspace);
        assert_eq!(table.name, deserialized.name);
        assert_eq!(table.id, deserialized.id);
        assert_eq!(table.columns.len(), deserialized.columns.len());
        assert_eq!(table.partition_key, deserialized.partition_key);
        assert_eq!(table.clustering_key, deserialized.clustering_key);
        assert_eq!(table.flags, deserialized.flags);
    }

    #[test]
    fn table_updates_construction() {
        let updates = TableUpdates {
            params: Some(TableParams::default()),
            add_columns: vec![ColumnMetadata {
                name: "new_col".to_string(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            }],
            drop_columns: vec!["old_col".to_string()],
            extensions: None,
        };

        assert!(updates.params.is_some());
        assert_eq!(updates.add_columns.len(), 1);
        assert_eq!(updates.add_columns[0].name, "new_col");
        assert_eq!(updates.drop_columns, vec!["old_col"]);

        let empty_updates = TableUpdates {
            params: None,
            add_columns: vec![],
            drop_columns: vec![],
            extensions: None,
        };

        assert!(empty_updates.params.is_none());
        assert!(empty_updates.add_columns.is_empty());
        assert!(empty_updates.drop_columns.is_empty());
    }

    #[test]
    fn table_updates_with_extensions() {
        let mut ext = HashMap::new();
        ext.insert("graph.type".to_string(), "vertex".to_string());

        let updates = TableUpdates {
            params: None,
            add_columns: vec![],
            drop_columns: vec![],
            extensions: Some(ext),
        };

        assert!(updates.extensions.is_some());
        let ext = updates.extensions.unwrap();
        assert_eq!(ext.get("graph.type"), Some(&"vertex".to_string()));
    }

    #[test]
    fn table_metadata_extensions_default_empty() {
        let table = TableMetadata {
            keyspace: "ks".to_string(),
            name: "t".to_string(),
            id: Uuid::new_v4(),
            columns: IndexMap::new(),
            partition_key: vec![],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        };
        assert!(table.extensions.is_empty());
    }

    #[test]
    fn table_metadata_extensions_serde_roundtrip() {
        let mut extensions = HashMap::new();
        extensions.insert("graph.type".to_string(), "vertex".to_string());
        extensions.insert("graph.label".to_string(), "person".to_string());

        let table = TableMetadata {
            keyspace: "ks".to_string(),
            name: "t".to_string(),
            id: Uuid::new_v4(),
            columns: IndexMap::new(),
            partition_key: vec![],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions,
            is_system: false,
        };

        let json = serde_json::to_string(&table).expect("serialize");
        let deserialized: TableMetadata = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(table.extensions.len(), deserialized.extensions.len());
        assert_eq!(
            table.extensions.get("graph.type"),
            deserialized.extensions.get("graph.type")
        );
        assert_eq!(
            table.extensions.get("graph.label"),
            deserialized.extensions.get("graph.label")
        );
    }

    #[test]
    fn table_metadata_is_system_default_false() {
        let table = TableMetadata {
            keyspace: "ks".to_string(),
            name: "t".to_string(),
            id: Uuid::new_v4(),
            columns: IndexMap::new(),
            partition_key: vec![],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        };
        assert!(!table.is_system);

        // Verify serde(default) works: deserializing JSON without is_system yields false
        let json = r#"{"keyspace":"ks","name":"t","id":"00000000-0000-0000-0000-000000000000","columns":{},"partition_key":[],"clustering_key":[],"params":{"bloom_filter_fp_chance":0.01,"caching":{"keys":"ALL","rows_per_partition":"NONE"},"comment":"","compaction":{},"compression":{},"crc_check_chance":1.0,"default_time_to_live":0,"gc_grace_seconds":864000,"max_index_interval":2048,"min_index_interval":128,"memtable_flush_period_in_ms":0,"speculative_retry":"99PERCENTILE","additional_write_policy":"99PERCENTILE","cdc":false,"read_repair":"BLOCKING","allow_auto_snapshot":true,"incremental_backups":true},"flags":[],"extensions":{}}"#;
        let deserialized: TableMetadata = serde_json::from_str(json).expect("deserialize");
        assert!(!deserialized.is_system);
    }

    #[test]
    fn caching_params_serde_roundtrip() {
        let caching = CachingParams {
            keys: "ALL".to_string(),
            rows_per_partition: "100".to_string(),
        };

        let json = serde_json::to_string(&caching).expect("serialize");
        let deserialized: CachingParams = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(caching.keys, deserialized.keys);
        assert_eq!(caching.rows_per_partition, deserialized.rows_per_partition);
    }
}
