//! `system.peers_v2` query support.
//!
//! Provides `PeerInfo` for peer node metadata, a `ClusterState` trait
//! for retrieving peer information, and `query_peers()`.

use std::net::IpAddr;

use uuid::Uuid;

use crate::registry::Schema;
use crate::system::TopologyView;

/// Information about a peer node in the cluster.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Peer's broadcast address.
    pub peer: IpAddr,
    /// Peer's broadcast port.
    pub peer_port: u16,
    /// Data center the peer belongs to.
    pub data_center: String,
    /// Rack within the data center.
    pub rack: String,
    /// Unique identifier for the peer node.
    pub host_id: Uuid,
    /// Preferred IP for inter-node communication, if different from peer.
    pub preferred_ip: Option<IpAddr>,
    /// Preferred port for inter-node communication, if different from peer_port.
    pub preferred_port: Option<u16>,
    /// Address for CQL native transport connections.
    pub native_address: IpAddr,
    /// Port for CQL native transport connections.
    pub native_port: u16,
    /// Address for CQL native transport connections used by internal clients.
    pub internal_native_address: IpAddr,
    /// Port for CQL native transport connections used by internal clients.
    pub internal_native_port: u16,
    /// Schema version the peer is running.
    pub schema_version: Uuid,
    /// Token ranges owned by this peer.
    pub tokens: Vec<String>,
    /// Release version the peer is running.
    pub release_version: String,
}

/// Trait for retrieving cluster peer information.
///
/// Implementations provide the list of known peers in the cluster.
/// The local node is never included in the peer list.
pub trait ClusterState: Send + Sync {
    /// Returns information about all known peer nodes.
    fn peers(&self) -> Vec<PeerInfo>;
}

/// Query `system.peers_v2` for peer node information.
///
/// Delegates to the `ClusterState` implementation and updates
/// each peer's `schema_version` from the current schema snapshot.
pub fn query_peers(schema: &Schema, cluster: &dyn ClusterState) -> Vec<PeerInfo> {
    query_peers_with_view(schema, cluster, TopologyView::Public)
}

/// Query `system.peers_v2` for peer node information using the requested
/// topology view.
pub fn query_peers_with_view(
    schema: &Schema,
    cluster: &dyn ClusterState,
    view: TopologyView,
) -> Vec<PeerInfo> {
    let snap = schema.snapshot();
    let mut peers = cluster.peers();
    for peer in &mut peers {
        // All nodes apply the same leader-generated schema_version from
        // Raft, so the local snapshot version matches what peers report.
        peer.schema_version = snap.version;
        if matches!(view, TopologyView::Internal) {
            peer.native_address = peer.internal_native_address;
            peer.native_port = peer.internal_native_port;
        }
    }
    peers
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

    struct EmptyCluster;
    impl ClusterState for EmptyCluster {
        fn peers(&self) -> Vec<PeerInfo> {
            vec![]
        }
    }

    #[test]
    fn single_node_returns_empty_peers() {
        let schema = test_schema();
        let peers = query_peers(&schema, &EmptyCluster);
        assert!(peers.is_empty());
    }

    #[test]
    fn peers_schema_version_updated_from_snapshot() {
        let schema = test_schema();
        let snap_version = schema.snapshot().version;

        struct OnePeerCluster;
        impl ClusterState for OnePeerCluster {
            fn peers(&self) -> Vec<PeerInfo> {
                vec![PeerInfo {
                    peer: "192.168.1.2".parse().unwrap(),
                    peer_port: 7000,
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    host_id: Uuid::new_v4(),
                    preferred_ip: None,
                    preferred_port: None,
                    native_address: "192.168.1.2".parse().unwrap(),
                    native_port: 9042,
                    internal_native_address: "10.0.0.2".parse().unwrap(),
                    internal_native_port: 9042,
                    schema_version: Uuid::nil(), // will be overwritten
                    tokens: vec!["-9223372036854775808".to_string()],
                    release_version: crate::system::RELEASE_VERSION.to_string(),
                }]
            }
        }

        let peers = query_peers(&schema, &OnePeerCluster);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].schema_version, snap_version);
        assert_eq!(peers[0].data_center, "dc1");
    }

    #[test]
    fn query_peers_internal_view_uses_internal_native_endpoint() {
        let schema = test_schema();

        struct OnePeerCluster;
        impl ClusterState for OnePeerCluster {
            fn peers(&self) -> Vec<PeerInfo> {
                vec![PeerInfo {
                    peer: "192.168.1.2".parse().unwrap(),
                    peer_port: 7000,
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    host_id: Uuid::new_v4(),
                    preferred_ip: None,
                    preferred_port: None,
                    native_address: "127.0.0.1".parse().unwrap(),
                    native_port: 19043,
                    internal_native_address: "10.89.1.44".parse().unwrap(),
                    internal_native_port: 9042,
                    schema_version: Uuid::nil(),
                    tokens: vec!["-9223372036854775808".to_string()],
                    release_version: crate::system::RELEASE_VERSION.to_string(),
                }]
            }
        }

        let peers = query_peers_with_view(&schema, &OnePeerCluster, TopologyView::Internal);
        assert_eq!(peers.len(), 1);
        assert_eq!(
            peers[0].native_address,
            "10.89.1.44".parse::<std::net::IpAddr>().unwrap()
        );
        assert_eq!(peers[0].native_port, 9042);
    }
}
