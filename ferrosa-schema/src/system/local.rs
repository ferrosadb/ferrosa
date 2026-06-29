//! `system.local` query support.
//!
//! Provides `NodeConfig` for node configuration and `LocalInfo` as the
//! result of `query_local()`.

use std::net::{IpAddr, Ipv4Addr};

use uuid::Uuid;

use crate::registry::Schema;
use crate::system::TopologyView;

/// Node configuration with sensible defaults for a Ferrosa instance.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Cluster name reported in system.local.
    pub cluster_name: String,
    /// Data center this node belongs to.
    pub data_center: String,
    /// Rack within the data center.
    pub rack: String,
    /// CQL native transport port.
    pub rpc_port: u16,
    /// Unique identifier for this node.
    pub host_id: Uuid,
    /// Address this node listens on for inter-node communication.
    pub listen_address: IpAddr,
    /// Port this node listens on for inter-node communication.
    pub listen_port: u16,
    /// Address to broadcast to other nodes for inter-node communication.
    pub broadcast_address: IpAddr,
    /// Port to broadcast to other nodes for inter-node communication.
    pub broadcast_port: u16,
    /// Address for CQL native transport connections.
    pub rpc_address: IpAddr,
    /// Address for CQL native transport connections used by internal clients.
    pub internal_rpc_address: IpAddr,
    /// Port for CQL native transport connections used by internal clients.
    pub internal_rpc_port: u16,
    /// Token ranges assigned to this node.
    pub tokens: Vec<String>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            cluster_name: "ferrosa".to_string(),
            // Cassandra 5.0 / DataStax driver convention.  See
            // ferrosa-nosqlbench/docs/initial-gaps-found.md (Gap 3).
            data_center: "datacenter1".to_string(),
            rack: "rack1".to_string(),
            rpc_port: 9042,
            host_id: Uuid::new_v4(),
            listen_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            listen_port: 7000,
            broadcast_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            broadcast_port: 7000,
            rpc_address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            internal_rpc_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            internal_rpc_port: 9042,
            // At least one token is required for drivers (Java DataStax, etc.)
            // to consider this node operational for query routing.
            tokens: vec!["0".to_string()],
        }
    }
}

/// Result of querying `system.local`.
#[derive(Debug, Clone)]
pub struct LocalInfo {
    /// Always "local".
    pub key: String,
    /// Cluster name from NodeConfig.
    pub cluster_name: String,
    /// Data center from NodeConfig.
    pub data_center: String,
    /// Rack from NodeConfig.
    pub rack: String,
    /// Unique host identifier from NodeConfig.
    pub host_id: Uuid,
    /// Partitioner class name (always Murmur3).
    pub partitioner: String,
    /// Native protocol version.
    pub native_protocol_version: String,
    /// CQL version.
    pub cql_version: String,
    /// Release version string.
    pub release_version: String,
    /// Current schema version from the snapshot.
    pub schema_version: Uuid,
    /// CQL native transport port.
    pub rpc_port: u16,
    /// Address this node listens on for inter-node communication.
    pub listen_address: IpAddr,
    /// Port this node listens on for inter-node communication.
    pub listen_port: u16,
    /// Address broadcast to other nodes for inter-node communication.
    pub broadcast_address: IpAddr,
    /// Port broadcast to other nodes for inter-node communication.
    pub broadcast_port: u16,
    /// Address for CQL native transport connections.
    pub rpc_address: IpAddr,
    /// Token ranges assigned to this node.
    pub tokens: Vec<String>,
    /// Bootstrap state (always "COMPLETED").
    pub bootstrapped: String,
}

/// Query `system.local` for this node's information.
///
/// Reads schema version from the current snapshot and combines it
/// with the node configuration to produce a `LocalInfo`.
///
/// Defaults to native protocol v4 so existing callers/tests remain
/// backward-compatible.
pub fn query_local(schema: &Schema, node_config: &NodeConfig) -> LocalInfo {
    query_local_with_view(schema, node_config, TopologyView::Public, 4)
}

/// Query `system.local` for this node's information using the requested
/// topology view and negotiated native protocol version.
///
/// `protocol_version` is used to populate `native_protocol_version` so drivers
/// (e.g. DataStax Java driver) see a value consistent with the connection
/// they opened.
pub fn query_local_with_view(
    schema: &Schema,
    node_config: &NodeConfig,
    view: TopologyView,
    protocol_version: u8,
) -> LocalInfo {
    let snap = schema.snapshot();
    let (rpc_address, rpc_port) = match view {
        TopologyView::Public => (node_config.rpc_address, node_config.rpc_port),
        TopologyView::Internal => (
            node_config.internal_rpc_address,
            node_config.internal_rpc_port,
        ),
    };
    LocalInfo {
        key: "local".to_string(),
        cluster_name: node_config.cluster_name.clone(),
        data_center: node_config.data_center.clone(),
        rack: node_config.rack.clone(),
        host_id: node_config.host_id,
        partitioner: "org.apache.cassandra.dht.Murmur3Partitioner".to_string(),
        native_protocol_version: protocol_version.to_string(),
        cql_version: "3.4.7".to_string(),
        release_version: crate::system::RELEASE_VERSION.to_string(),
        schema_version: snap.version,
        rpc_port,
        listen_address: node_config.listen_address,
        listen_port: node_config.listen_port,
        broadcast_address: node_config.broadcast_address,
        broadcast_port: node_config.broadcast_port,
        rpc_address,
        tokens: node_config.tokens.clone(),
        bootstrapped: "COMPLETED".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::TestAuditSink;
    use crate::auth::password::{PasswordHasher, PasswordPolicy};
    use crate::auth::rate_limit::RateLimitConfig;
    use crate::registry::{AuthMethod, Schema, SchemaConfig};
    use crate::secrets::EnvSecretsProvider;
    use crate::startup::DeploymentMode;

    fn test_schema() -> Schema {
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap()
    }

    #[test]
    fn node_config_defaults() {
        let config = NodeConfig::default();
        assert_eq!(config.cluster_name, "ferrosa");
        // Cassandra 5.0 / DSE / DataStax driver convention.  Drivers default
        // `localdc=datacenter1`; reporting anything else makes the driver's
        // topology probe reject every contact point.  See
        // ferrosa-nosqlbench/docs/initial-gaps-found.md (Gap 3).
        assert_eq!(config.data_center, "datacenter1");
        assert_eq!(config.rack, "rack1");
        assert_eq!(config.rpc_port, 9042);
    }

    #[test]
    fn query_local_returns_correct_fields() {
        let schema = test_schema();
        let node_config = NodeConfig::default();
        let info = query_local(&schema, &node_config);
        assert_eq!(info.key, "local");
        assert_eq!(info.cluster_name, "ferrosa");
        assert_eq!(
            info.partitioner,
            "org.apache.cassandra.dht.Murmur3Partitioner"
        );
        assert_eq!(info.native_protocol_version, "4");
        assert_eq!(info.schema_version, schema.snapshot().version);
    }

    /// BUG-020: release_version must use the shared RELEASE_VERSION constant.
    #[test]
    fn query_local_release_version_uses_constant() {
        let schema = test_schema();
        let node_config = NodeConfig::default();
        let info = query_local(&schema, &node_config);
        assert_eq!(
            info.release_version,
            crate::system::RELEASE_VERSION,
            "system.local release_version must match the shared constant"
        );
    }

    /// BUG-021: native_protocol_version must reflect the negotiated protocol
    /// version so drivers (DataStax Java driver) trust the connection.
    #[test]
    fn query_local_with_view_reports_negotiated_protocol_version() {
        let schema = test_schema();
        let node_config = NodeConfig::default();
        let info = query_local_with_view(&schema, &node_config, TopologyView::Public, 5);
        assert_eq!(info.native_protocol_version, "5");
    }

    #[test]
    fn query_local_internal_view_uses_internal_rpc_endpoint() {
        let schema = test_schema();
        let node_config = NodeConfig {
            rpc_address: "127.0.0.1".parse().unwrap(),
            rpc_port: 19042,
            internal_rpc_address: "10.89.1.48".parse().unwrap(),
            internal_rpc_port: 9042,
            ..NodeConfig::default()
        };

        let info = query_local_with_view(&schema, &node_config, TopologyView::Internal, 5);

        assert_eq!(
            info.rpc_address,
            "10.89.1.48".parse::<std::net::IpAddr>().unwrap()
        );
        assert_eq!(info.rpc_port, 9042);
        assert_eq!(info.native_protocol_version, "5");
    }
}
