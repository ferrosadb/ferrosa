//! Keyspace metadata types.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Metadata for a Cassandra keyspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyspaceMetadata {
    /// Keyspace name.
    pub name: String,
    /// Whether durable writes are enabled for this keyspace.
    pub durable_writes: bool,
    /// Replication strategy and options.
    pub replication: ReplicationParams,
}

/// Replication strategy and its options for a keyspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationParams {
    /// Strategy class name (e.g., "SimpleStrategy", "NetworkTopologyStrategy").
    pub strategy: String,
    /// Strategy-specific options (e.g., "replication_factor" -> "3").
    pub options: HashMap<String, String>,
}

/// Optional updates for a keyspace (partial update).
///
/// `None` fields are left unchanged; `Some` fields are applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyspaceUpdates {
    /// New replication parameters, if changing.
    pub replication: Option<ReplicationParams>,
    /// New durable_writes setting, if changing.
    pub durable_writes: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyspace_metadata_construction() {
        let mut options = HashMap::new();
        options.insert("replication_factor".to_string(), "3".to_string());

        let ks = KeyspaceMetadata {
            name: "my_keyspace".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options,
            },
        };

        assert_eq!(ks.name, "my_keyspace");
        assert!(ks.durable_writes);
        assert_eq!(ks.replication.strategy, "SimpleStrategy");
        assert_eq!(
            ks.replication.options.get("replication_factor"),
            Some(&"3".to_string())
        );
    }

    #[test]
    fn replication_params_network_topology() {
        let mut options = HashMap::new();
        options.insert("dc1".to_string(), "3".to_string());
        options.insert("dc2".to_string(), "2".to_string());

        let params = ReplicationParams {
            strategy: "NetworkTopologyStrategy".to_string(),
            options,
        };

        assert_eq!(params.strategy, "NetworkTopologyStrategy");
        assert_eq!(params.options.len(), 2);
        assert_eq!(params.options.get("dc1"), Some(&"3".to_string()));
        assert_eq!(params.options.get("dc2"), Some(&"2".to_string()));
    }

    #[test]
    fn keyspace_metadata_serde_roundtrip() {
        let mut options = HashMap::new();
        options.insert("replication_factor".to_string(), "3".to_string());

        let ks = KeyspaceMetadata {
            name: "test_ks".to_string(),
            durable_writes: false,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options,
            },
        };

        let json = serde_json::to_string(&ks).expect("serialize");
        let deserialized: KeyspaceMetadata = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(ks, deserialized);
    }

    #[test]
    fn replication_params_serde_roundtrip() {
        let mut options = HashMap::new();
        options.insert("dc1".to_string(), "3".to_string());

        let params = ReplicationParams {
            strategy: "NetworkTopologyStrategy".to_string(),
            options,
        };

        let json = serde_json::to_string(&params).expect("serialize");
        let deserialized: ReplicationParams = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(params, deserialized);
    }

    #[test]
    fn keyspace_updates_construction() {
        let updates = KeyspaceUpdates {
            replication: Some(ReplicationParams {
                strategy: "NetworkTopologyStrategy".to_string(),
                options: {
                    let mut opts = HashMap::new();
                    opts.insert("dc1".to_string(), "3".to_string());
                    opts
                },
            }),
            durable_writes: None,
        };

        assert!(updates.replication.is_some());
        assert!(updates.durable_writes.is_none());

        let updates_dw_only = KeyspaceUpdates {
            replication: None,
            durable_writes: Some(false),
        };

        assert!(updates_dw_only.replication.is_none());
        assert_eq!(updates_dw_only.durable_writes, Some(false));
    }

    #[test]
    fn keyspace_metadata_clone_eq() {
        let mut options = HashMap::new();
        options.insert("replication_factor".to_string(), "1".to_string());

        let ks = KeyspaceMetadata {
            name: "ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options,
            },
        };

        let cloned = ks.clone();
        assert_eq!(ks, cloned);
    }
}
