//! `system.peers_v2` virtual table.
//!
//! Exposes Cassandra 4.x-compatible peer topology through the virtual-table
//! registry/encoding path while reusing the same topology policy as the legacy
//! inline `system.peers` handler.

use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_cluster::ClusterStateHolder;
use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::{
    query_peers_with_view, NodeConfig, RowPredicate, Schema, SubscriptionMode, VirtualColumnDef,
    VirtualRow, VirtualTable, WireType,
};

use crate::topology::ClientTopologyPolicy;

/// Column index of `tokens` in `system.peers_v2`.
const TOKENS_COL: usize = 9;

fn make_columns() -> Vec<VirtualColumnDef> {
    vec![
        VirtualColumnDef {
            name: "peer".to_owned(),
            data_type: DataType::Inet,
        },
        VirtualColumnDef {
            name: "peer_port".to_owned(),
            data_type: DataType::Int,
        },
        VirtualColumnDef {
            name: "data_center".to_owned(),
            data_type: DataType::Text,
        },
        VirtualColumnDef {
            name: "rack".to_owned(),
            data_type: DataType::Text,
        },
        VirtualColumnDef {
            name: "host_id".to_owned(),
            data_type: DataType::Uuid,
        },
        VirtualColumnDef {
            name: "native_address".to_owned(),
            data_type: DataType::Inet,
        },
        VirtualColumnDef {
            name: "native_port".to_owned(),
            data_type: DataType::Int,
        },
        VirtualColumnDef {
            name: "schema_version".to_owned(),
            data_type: DataType::Uuid,
        },
        VirtualColumnDef {
            name: "release_version".to_owned(),
            data_type: DataType::Text,
        },
        VirtualColumnDef {
            name: "tokens".to_owned(),
            // Scalar fallback only; wire_type_for exposes this as set<text>.
            data_type: DataType::Text,
        },
    ]
}

fn encode_inet(ip: IpAddr) -> Vec<u8> {
    match ip {
        IpAddr::V4(v4) => v4.octets().to_vec(),
        IpAddr::V6(v6) => v6.octets().to_vec(),
    }
}

fn loopback_client_ip(client_address: &str) -> Option<IpAddr> {
    if let Ok(addr) = client_address.parse::<std::net::SocketAddr>() {
        return addr.ip().is_loopback().then_some(addr.ip());
    }
    if let Ok(ip) = client_address.parse::<IpAddr>() {
        return ip.is_loopback().then_some(ip);
    }
    None
}

fn harmonize_loopback_family(advertised: IpAddr, client_loopback_ip: Option<IpAddr>) -> IpAddr {
    match client_loopback_ip {
        Some(client_ip) if advertised.is_loopback() && client_ip.is_loopback() => client_ip,
        _ => advertised,
    }
}

/// Virtual table for Cassandra 4.x `system.peers_v2`.
pub struct PeersV2Table {
    node_config: Arc<NodeConfig>,
    schema: Arc<Schema>,
    topology_policy: ClientTopologyPolicy,
    cluster_state: Arc<ArcSwap<ClusterStateHolder>>,
    client_address: String,
    columns: Vec<VirtualColumnDef>,
}

impl PeersV2Table {
    /// Create a table instance for a request/client address.
    pub fn new(
        node_config: Arc<NodeConfig>,
        schema: Arc<Schema>,
        topology_policy: ClientTopologyPolicy,
        cluster_state: Arc<ArcSwap<ClusterStateHolder>>,
        client_address: impl Into<String>,
    ) -> Self {
        Self {
            node_config,
            schema,
            topology_policy,
            cluster_state,
            client_address: client_address.into(),
            columns: make_columns(),
        }
    }
}

impl VirtualTable for PeersV2Table {
    fn name(&self) -> &str {
        "peers_v2"
    }

    fn keyspace(&self) -> &str {
        "system"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0, 1]
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }

    fn wire_type_for(&self, col_idx: usize) -> Option<WireType> {
        (col_idx == TOKENS_COL).then_some(WireType::SetText)
    }

    fn read(&self, predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let mut rows = Vec::new();
        self.visit_rows(predicate, &mut |row| rows.push(row));
        rows
    }

    fn visit_rows(&self, _predicate: Option<&RowPredicate>, visit: &mut dyn FnMut(VirtualRow)) {
        tracing::debug!(
            "PeersV2Table::visit_rows start client={}",
            self.client_address
        );
        let local_addresses = [
            self.node_config.internal_rpc_address,
            self.node_config.listen_address,
            self.node_config.broadcast_address,
        ];
        let client_loopback_ip = loopback_client_ip(&self.client_address);
        let topology_view = self
            .topology_policy
            .topology_view_for_client_with_locals(&self.client_address, &local_addresses);
        tracing::debug!("PeersV2Table::visit_rows topology_view={:?}", topology_view);
        let mut peers = query_peers_with_view(
            &self.schema,
            self.cluster_state.load().as_ref(),
            topology_view,
        );
        tracing::debug!("PeersV2Table::visit_rows peers={}", peers.len());

        for peer in &mut peers {
            peer.native_address =
                harmonize_loopback_family(peer.native_address, client_loopback_ip);
            visit(VirtualRow {
                cells: vec![
                    CellValue::live(encode_inet(peer.peer), 0),
                    CellValue::live((peer.peer_port as i32).to_be_bytes().to_vec(), 0),
                    CellValue::live(peer.data_center.as_bytes().to_vec(), 0),
                    CellValue::live(peer.rack.as_bytes().to_vec(), 0),
                    CellValue::live(peer.host_id.as_bytes().to_vec(), 0),
                    CellValue::live(encode_inet(peer.native_address), 0),
                    CellValue::live((peer.native_port as i32).to_be_bytes().to_vec(), 0),
                    CellValue::live(peer.schema_version.as_bytes().to_vec(), 0),
                    CellValue::live(peer.release_version.as_bytes().to_vec(), 0),
                    CellValue::live(VirtualColumnDef::encode_list_text(&peer.tokens), 0),
                ],
            });
        }
        tracing::debug!(
            "PeersV2Table::visit_rows done client={}",
            self.client_address
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_cluster::raft::{NodeInfo, NodeState};
    use ferrosa_cluster::ring::TokenRing;
    use ferrosa_schema::audit::TestAuditSink;
    use ferrosa_schema::auth::password::{PasswordHasher, PasswordPolicy};
    use ferrosa_schema::auth::rate_limit::RateLimitConfig;
    use ferrosa_schema::registry::{AuthMethod, SchemaConfig};
    use ferrosa_schema::secrets::EnvSecretsProvider;
    use ferrosa_schema::startup::DeploymentMode;
    use ferrosa_schema::system::RELEASE_VERSION;
    use uuid::Uuid;

    fn test_schema() -> Arc<Schema> {
        Arc::new(
            Schema::new(SchemaConfig {
                hasher: PasswordHasher::default(),
                password_policy: PasswordPolicy::permissive(),
                auth_method: AuthMethod::Password,
                rate_limit: RateLimitConfig::default(),
                audit_sink: Box::new(TestAuditSink::new()),
                secrets: Box::new(EnvSecretsProvider),
                mode: DeploymentMode::Development,
            })
            .unwrap(),
        )
    }

    fn table_with_peer(client_address: &str, policy: ClientTopologyPolicy) -> PeersV2Table {
        let schema = test_schema();
        let local_id = 1_u64;
        let peer_id = 2_u64;
        let mut ring = TokenRing::new();
        ring.add_node(
            local_id,
            NodeInfo {
                host_id: Uuid::new_v4(),
                addr: "10.89.1.48:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: Some("127.0.0.1:19042".to_string()),
            },
        );
        ring.add_node(
            peer_id,
            NodeInfo {
                host_id: Uuid::from_u128(0x12345678123456781234567812345678),
                addr: "10.89.1.49:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: Some("127.0.0.1:19043".to_string()),
            },
        );
        let cluster_state = Arc::new(ArcSwap::from_pointee(ClusterStateHolder::Cluster(
            ferrosa_cluster::RaftClusterState::new(Arc::new(ArcSwap::from_pointee(ring)), local_id),
        )));
        PeersV2Table::new(
            Arc::new(NodeConfig::default()),
            schema,
            policy,
            cluster_state,
            client_address,
        )
    }

    #[test]
    fn peers_v2_table_metadata_matches_cassandra4_shape() {
        let table = table_with_peer("127.0.0.1:50000", ClientTopologyPolicy::default());
        assert_eq!(table.keyspace(), "system");
        assert_eq!(table.name(), "peers_v2");
        assert_eq!(table.primary_key_columns(), &[0, 1]);
        let names: Vec<_> = table.columns().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "peer",
                "peer_port",
                "data_center",
                "rack",
                "host_id",
                "native_address",
                "native_port",
                "schema_version",
                "release_version",
                "tokens",
            ]
        );
        let types: Vec<_> = table.columns().iter().map(|c| c.data_type).collect();
        assert_eq!(
            types,
            vec![
                DataType::Inet,
                DataType::Int,
                DataType::Text,
                DataType::Text,
                DataType::Uuid,
                DataType::Inet,
                DataType::Int,
                DataType::Uuid,
                DataType::Text,
                DataType::Text,
            ]
        );
        assert_eq!(table.wire_type_for(TOKENS_COL), Some(WireType::SetText));
    }

    #[test]
    fn peers_v2_row_contains_peer_topology() {
        let table = table_with_peer("127.0.0.1:50000", ClientTopologyPolicy::default());
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        let cells = &rows[0].cells;
        assert_eq!(cells.len(), 10);
        assert_eq!(cells[0].value.as_deref().unwrap(), &[10, 89, 1, 49]);
        assert_eq!(
            cells[1].value.as_deref().unwrap(),
            &(7000_i32.to_be_bytes())
        );
        assert_eq!(cells[2].value.as_deref().unwrap(), b"dc1");
        assert_eq!(cells[3].value.as_deref().unwrap(), b"rack1");
        assert_eq!(cells[5].value.as_deref().unwrap(), &[127, 0, 0, 1]);
        assert_eq!(
            cells[6].value.as_deref().unwrap(),
            &(19043_i32.to_be_bytes())
        );
        assert_eq!(
            cells[8].value.as_deref().unwrap(),
            RELEASE_VERSION.as_bytes()
        );
        assert!(cells[9].value.as_deref().unwrap().len() >= 4);
    }

    #[test]
    fn internal_client_sees_internal_native_endpoint() {
        let table = table_with_peer(
            "10.89.1.60:50000",
            ClientTopologyPolicy::from_csv("10.89.0.0/16").unwrap(),
        );
        let rows = table.read(None);
        let cells = &rows[0].cells;
        assert_eq!(cells[5].value.as_deref().unwrap(), &[10, 89, 1, 49]);
        assert_eq!(
            cells[6].value.as_deref().unwrap(),
            &(9042_i32.to_be_bytes())
        );
    }
}
