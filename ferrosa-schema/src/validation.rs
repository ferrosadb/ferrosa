//! DDL validation rules for table and keyspace creation.

use crate::error::SchemaError;
use crate::metadata::keyspace::KeyspaceMetadata;
use crate::metadata::table::TableMetadata;

/// Maximum length for table and keyspace names.
const MAX_NAME_LENGTH: usize = 48;

/// Returns true if every character in `name` is ASCII alphanumeric or underscore.
fn is_valid_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_LENGTH
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Validate a table definition before creation.
///
/// Checks:
/// - Table name is 1–48 chars, alphanumeric or underscore
/// - Partition key is not empty
/// - All partition key column names exist in the columns map
/// - All clustering key column names exist in the columns map
pub fn validate_table(table: &TableMetadata) -> crate::Result<()> {
    if !is_valid_identifier(&table.name) {
        return Err(SchemaError::InvalidSchema(format!(
            "table name '{}' is invalid: must be 1-{} alphanumeric/underscore characters",
            table.name, MAX_NAME_LENGTH
        )));
    }

    if table.partition_key.is_empty() {
        return Err(SchemaError::InvalidSchema(
            "table must have at least one partition key column".to_string(),
        ));
    }

    for pk_col in &table.partition_key {
        if !table.columns.contains_key(pk_col) {
            return Err(SchemaError::InvalidSchema(format!(
                "partition key column '{}' not found in columns",
                pk_col
            )));
        }
    }

    for (ck_col, _order) in &table.clustering_key {
        if !table.columns.contains_key(ck_col) {
            return Err(SchemaError::InvalidSchema(format!(
                "clustering key column '{}' not found in columns",
                ck_col
            )));
        }
    }

    Ok(())
}

/// Validate a keyspace definition before creation.
///
/// Checks:
/// - Keyspace name is 1–48 chars, alphanumeric or underscore
/// - Replication factor is >= 1 if present in options
pub fn validate_keyspace(ks: &KeyspaceMetadata) -> crate::Result<()> {
    if !is_valid_identifier(&ks.name) {
        return Err(SchemaError::InvalidSchema(format!(
            "keyspace name '{}' is invalid: must be 1-{} alphanumeric/underscore characters",
            ks.name, MAX_NAME_LENGTH
        )));
    }

    // Validate every replication factor: the cluster-wide `replication_factor`
    // and each per-datacenter factor (NetworkTopologyStrategy). `class` is the
    // strategy name, not a factor.
    for (key, value) in &ks.replication.options {
        if key == "class" {
            continue;
        }

        // Transient replication (`'<full>/<transient>'`, e.g. `'3/1'`) is a
        // disabled-by-default Cassandra feature ferrosa does not implement.
        // Accepting it would persist an option string strict CQL drivers
        // (scylla, DataStax) cannot parse during schema agreement, breaking
        // every subsequent metadata fetch against the node. Reject it loudly at
        // creation, exactly as Cassandra does when transient replication is off.
        if value.contains('/') {
            return Err(SchemaError::InvalidSchema(format!(
                "transient replication is not supported: replication option \
                 '{key}' = '{value}'; use a plain integer replication factor"
            )));
        }

        match value.parse::<i32>() {
            Ok(rf) => {
                // The cluster-wide replication_factor must be >= 1. A per-DC
                // factor of 0 is valid — it excludes that datacenter.
                if key == "replication_factor" && rf < 1 {
                    return Err(SchemaError::InvalidSchema(format!(
                        "replication_factor must be >= 1, got {rf}"
                    )));
                }
                if rf < 0 {
                    return Err(SchemaError::InvalidSchema(format!(
                        "replication factor for '{key}' must be >= 0, got {rf}"
                    )));
                }
            }
            Err(_) => {
                return Err(SchemaError::InvalidSchema(format!(
                    "replication factor for '{key}' is not a valid integer: '{value}'"
                )));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use indexmap::IndexMap;
    use uuid::Uuid;

    use crate::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
    use crate::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use crate::metadata::table::{TableMetadata, TableParams};

    use super::*;

    /// Helper: build a minimal valid table with one partition key column.
    fn valid_table() -> TableMetadata {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            ColumnMetadata {
                name: "id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::None,
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
        TableMetadata {
            keyspace: "test_ks".to_string(),
            name: "my_table".to_string(),
            id: Uuid::new_v4(),
            columns,
            partition_key: vec!["id".to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        }
    }

    /// Helper: build a minimal valid keyspace.
    fn valid_keyspace() -> KeyspaceMetadata {
        let mut options = HashMap::new();
        options.insert("replication_factor".to_string(), "3".to_string());
        KeyspaceMetadata {
            name: "test_ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options,
            },
        }
    }

    #[test]
    fn accept_valid_table() {
        let table = valid_table();
        assert!(validate_table(&table).is_ok());
    }

    #[test]
    fn accept_valid_keyspace() {
        let ks = valid_keyspace();
        assert!(validate_keyspace(&ks).is_ok());
    }

    #[test]
    fn reject_empty_partition_key() {
        let mut table = valid_table();
        table.partition_key.clear();
        let err = validate_table(&table).unwrap_err();
        assert!(
            err.to_string().contains("partition key"),
            "expected partition key error, got: {}",
            err
        );
    }

    #[test]
    fn reject_invalid_table_name_empty() {
        let mut table = valid_table();
        table.name = String::new();
        let err = validate_table(&table).unwrap_err();
        assert!(
            err.to_string().contains("invalid"),
            "expected invalid name error, got: {}",
            err
        );
    }

    #[test]
    fn reject_invalid_table_name_too_long() {
        let mut table = valid_table();
        table.name = "a".repeat(49);
        let err = validate_table(&table).unwrap_err();
        assert!(
            err.to_string().contains("invalid"),
            "expected invalid name error, got: {}",
            err
        );
    }

    #[test]
    fn reject_invalid_table_name_special_chars() {
        let mut table = valid_table();
        table.name = "my-table!".to_string();
        let err = validate_table(&table).unwrap_err();
        assert!(
            err.to_string().contains("invalid"),
            "expected invalid name error, got: {}",
            err
        );
    }

    #[test]
    fn reject_zero_replication_factor() {
        let mut ks = valid_keyspace();
        ks.replication
            .options
            .insert("replication_factor".to_string(), "0".to_string());
        let err = validate_keyspace(&ks).unwrap_err();
        assert!(
            err.to_string().contains("replication_factor"),
            "expected RF error, got: {}",
            err
        );
    }

    #[test]
    fn reject_negative_replication_factor() {
        let mut ks = valid_keyspace();
        ks.replication
            .options
            .insert("replication_factor".to_string(), "-1".to_string());
        let err = validate_keyspace(&ks).unwrap_err();
        assert!(
            err.to_string().contains("replication_factor"),
            "expected RF error, got: {}",
            err
        );
    }

    /// Transient replication (`'<full>/<transient>'`, e.g. `'3/1'`) is a
    /// disabled-by-default Cassandra feature ferrosa does not implement.
    /// Accepting it would persist an option string strict CQL drivers cannot
    /// parse during schema agreement, corrupting every metadata fetch. It must
    /// be rejected at creation, as Cassandra does by default.
    #[test]
    fn reject_transient_replication_per_dc() {
        let mut ks = valid_keyspace();
        ks.replication.strategy = "NetworkTopologyStrategy".to_string();
        ks.replication.options.clear();
        ks.replication
            .options
            .insert("DC1".to_string(), "3/1".to_string());
        let err = validate_keyspace(&ks).unwrap_err();
        assert!(
            err.to_string().contains("transient replication"),
            "expected transient-replication rejection, got: {}",
            err
        );
    }

    #[test]
    fn reject_transient_replication_factor() {
        let mut ks = valid_keyspace();
        ks.replication
            .options
            .insert("replication_factor".to_string(), "3/1".to_string());
        let err = validate_keyspace(&ks).unwrap_err();
        assert!(
            err.to_string().contains("transient replication"),
            "expected transient-replication rejection, got: {}",
            err
        );
    }

    #[test]
    fn reject_non_integer_dc_factor() {
        let mut ks = valid_keyspace();
        ks.replication.strategy = "NetworkTopologyStrategy".to_string();
        ks.replication.options.clear();
        ks.replication
            .options
            .insert("DC1".to_string(), "three".to_string());
        let err = validate_keyspace(&ks).unwrap_err();
        assert!(
            err.to_string().contains("DC1"),
            "expected invalid-factor error naming the DC, got: {}",
            err
        );
    }

    /// A per-DC factor of 0 is valid in NetworkTopologyStrategy — it excludes
    /// that datacenter (see `autoexpand_exclude_dc.cql`). Only the cluster-wide
    /// `replication_factor` must be >= 1.
    #[test]
    fn accept_per_dc_zero_replication_factor() {
        let mut ks = valid_keyspace();
        ks.replication.strategy = "NetworkTopologyStrategy".to_string();
        ks.replication.options.clear();
        ks.replication
            .options
            .insert("replication_factor".to_string(), "3".to_string());
        ks.replication
            .options
            .insert("DC2".to_string(), "0".to_string());
        assert!(
            validate_keyspace(&ks).is_ok(),
            "per-DC RF of 0 (exclude DC) must be accepted"
        );
    }

    #[test]
    fn reject_missing_partition_key_column() {
        let mut table = valid_table();
        table.partition_key = vec!["nonexistent".to_string()];
        let err = validate_table(&table).unwrap_err();
        assert!(
            err.to_string().contains("nonexistent"),
            "expected missing column error, got: {}",
            err
        );
    }

    #[test]
    fn reject_missing_clustering_key_column() {
        let mut table = valid_table();
        table.clustering_key = vec![("missing_col".to_string(), ClusteringOrder::Asc)];
        let err = validate_table(&table).unwrap_err();
        assert!(
            err.to_string().contains("missing_col"),
            "expected missing column error, got: {}",
            err
        );
    }

    #[test]
    fn accept_keyspace_without_replication_factor() {
        let ks = KeyspaceMetadata {
            name: "local_ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "LocalStrategy".to_string(),
                options: HashMap::new(),
            },
        };
        assert!(validate_keyspace(&ks).is_ok());
    }

    #[test]
    fn reject_invalid_keyspace_name() {
        let mut ks = valid_keyspace();
        ks.name = "bad.name".to_string();
        let err = validate_keyspace(&ks).unwrap_err();
        assert!(
            err.to_string().contains("invalid"),
            "expected invalid name error, got: {}",
            err
        );
    }

    #[test]
    fn accept_table_name_with_underscores() {
        let mut table = valid_table();
        table.name = "my_cool_table_123".to_string();
        assert!(validate_table(&table).is_ok());
    }

    #[test]
    fn accept_table_name_at_max_length() {
        let mut table = valid_table();
        table.name = "a".repeat(48);
        assert!(validate_table(&table).is_ok());
    }
}
