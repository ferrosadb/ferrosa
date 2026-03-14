//! DDL coordination for pair mode.
//!
//! Provides `DdlOperation` (the serializable DDL enum), `DdlCoordinator`
//! (routes DDL to primary authority), and RPC handlers for forwarding and
//! schema sync.

use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_schema::metadata::keyspace::KeyspaceMetadata;
use ferrosa_schema::metadata::table::TableMetadata;
use ferrosa_schema::{is_system_keyspace, Schema, SchemaSnapshot};
use ferrosa_storage::engine::StorageEngine;

use crate::error::{ClusterError, Result};
use crate::pair::PairRole;

// ---------------------------------------------------------------------------
// DdlOperation
// ---------------------------------------------------------------------------

/// A single DDL operation that can be forwarded and replicated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DdlOperation {
    CreateKeyspace(KeyspaceMetadata),
    DropKeyspace(String),
    CreateTable(Box<TableMetadata>),
    DropTable { keyspace: String, table: String },
}

impl DdlOperation {
    /// Serialize to JSON bytes.
    pub fn to_bytes(&self) -> Result<Bytes> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|e| ClusterError::Internal(format!("DdlOperation serialize: {e}")))
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| ClusterError::Internal(format!("DdlOperation deserialize: {e}")))
    }
}

// ---------------------------------------------------------------------------
// DdlCoordinator
// ---------------------------------------------------------------------------

/// Coordinates DDL in pair mode.
///
/// Primary: applies DDL locally, then replicates to secondary.
/// Secondary: forwards to primary (which applies + replicates back).
pub struct DdlCoordinator {
    role: Arc<ArcSwap<PairRole>>,
    peer_host_id: Uuid,
    schema: Arc<Schema>,
    engine: Arc<StorageEngine>,
    peer_manager: Arc<PeerManager>,
}

impl DdlCoordinator {
    pub fn new(
        role: Arc<ArcSwap<PairRole>>,
        peer_host_id: Uuid,
        schema: Arc<Schema>,
        engine: Arc<StorageEngine>,
        peer_manager: Arc<PeerManager>,
    ) -> Self {
        Self {
            role,
            peer_host_id,
            schema,
            engine,
            peer_manager,
        }
    }

    /// Route a DDL operation based on current role.
    pub async fn coordinate_ddl(&self, op: DdlOperation) -> Result<()> {
        match **self.role.load() {
            PairRole::Primary => {
                self.apply_ddl_locally(&op)?;
                self.replicate_ddl(&op).await?;
                Ok(())
            }
            PairRole::Secondary => self.forward_ddl(&op).await,
        }
    }

    /// Apply a DDL operation to the local schema and storage engine.
    pub(crate) fn apply_ddl_locally(&self, op: &DdlOperation) -> Result<()> {
        match op {
            DdlOperation::CreateKeyspace(ks) => {
                self.schema
                    .create_keyspace_internal(ks.clone())
                    .map_err(|e| ClusterError::Internal(format!("create_keyspace: {e}")))?;
            }
            DdlOperation::DropKeyspace(name) => {
                self.schema
                    .drop_keyspace_internal(name)
                    .map_err(|e| ClusterError::Internal(format!("drop_keyspace: {e}")))?;
            }
            DdlOperation::CreateTable(table) => {
                self.schema
                    .create_table_internal(*table.clone())
                    .map_err(|e| ClusterError::Internal(format!("create_table: {e}")))?;
                let storage_schema = table.to_storage_schema();
                self.engine
                    .register_table(storage_schema)
                    .map_err(ClusterError::Storage)?;
            }
            DdlOperation::DropTable { keyspace, table } => {
                self.schema
                    .drop_table_internal(keyspace, table)
                    .map_err(|e| ClusterError::Internal(format!("drop_table: {e}")))?;
            }
        }
        Ok(())
    }

    /// Send a DDL operation to the peer (as primary replicating to secondary)
    /// and wait for ACK.
    pub(crate) async fn replicate_ddl(&self, op: &DdlOperation) -> Result<()> {
        let body = op.to_bytes()?;
        let resp = self
            .peer_manager
            .send(self.peer_host_id, Message::PairDdlForward(body), Lane::Data)
            .await
            .map_err(ClusterError::Net)?;

        match resp {
            Message::PairDdlAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairDdlAck, got {:?}",
                other.msg_type()
            ))),
        }
    }

    /// Forward a DDL operation to the primary (as secondary) and wait for ACK.
    async fn forward_ddl(&self, op: &DdlOperation) -> Result<()> {
        let body = op.to_bytes()?;
        let resp = self
            .peer_manager
            .send(self.peer_host_id, Message::PairDdlForward(body), Lane::Data)
            .await
            .map_err(ClusterError::Net)?;

        match resp {
            Message::PairDdlAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairDdlAck, got {:?}",
                other.msg_type()
            ))),
        }
    }

    /// Get current role.
    pub fn role(&self) -> PairRole {
        **self.role.load()
    }
}

// ---------------------------------------------------------------------------
// PairDdlForwardHandler
// ---------------------------------------------------------------------------

/// Handles incoming `PairDdlForward` messages.
///
/// Primary: applies locally + replicates to secondary, then ACKs.
/// Secondary: applies locally, then ACKs (no further replication).
pub struct PairDdlForwardHandler {
    role: Arc<ArcSwap<PairRole>>,
    coordinator: Arc<DdlCoordinator>,
}

impl PairDdlForwardHandler {
    pub fn new(role: Arc<ArcSwap<PairRole>>, coordinator: Arc<DdlCoordinator>) -> Self {
        Self { role, coordinator }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairDdlForwardHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::PairDdlForward(b) => b,
            _ => return None,
        };

        let op = match DdlOperation::from_bytes(&body) {
            Ok(op) => op,
            Err(e) => {
                tracing::error!("failed to decode PairDdlForward: {e}");
                return None;
            }
        };

        let result = match **self.role.load() {
            PairRole::Primary => {
                // Forwarded DDL from secondary: apply + replicate back
                if let Err(e) = self.coordinator.apply_ddl_locally(&op) {
                    tracing::error!("failed to apply forwarded DDL: {e}");
                    return None;
                }
                self.coordinator.replicate_ddl(&op).await
            }
            PairRole::Secondary => {
                // Replicated DDL from primary: apply locally only
                self.coordinator.apply_ddl_locally(&op)
            }
        };

        match result {
            Ok(()) => Some(Message::PairDdlAck(Bytes::new())),
            Err(e) => {
                tracing::error!("PairDdlForward handler failed: {e}");
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PairSchemaSyncHandler
// ---------------------------------------------------------------------------

/// Handles incoming `PairSchemaSync` messages during catch-up.
///
/// Deserializes the `SchemaSnapshot`, registers all non-system tables with the
/// storage engine, then applies the snapshot to the schema registry.
pub struct PairSchemaSyncHandler {
    schema: Arc<Schema>,
    engine: Arc<StorageEngine>,
}

impl PairSchemaSyncHandler {
    pub fn new(schema: Arc<Schema>, engine: Arc<StorageEngine>) -> Self {
        Self { schema, engine }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairSchemaSyncHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::PairSchemaSync(b) => b,
            _ => return None,
        };

        let snapshot: SchemaSnapshot = match serde_json::from_slice(&body) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to decode PairSchemaSync: {e}");
                return None;
            }
        };

        // Register each non-system table with the storage engine.
        for ((keyspace, _table_name), table) in &snapshot.tables {
            if is_system_keyspace(keyspace) {
                continue;
            }
            let storage_schema = table.to_storage_schema();
            if let Err(e) = self.engine.register_table(storage_schema) {
                tracing::error!("failed to register table during schema sync: {e}");
                return None;
            }
        }

        // Apply snapshot to schema registry.
        if let Err(e) = self.schema.apply_snapshot(snapshot) {
            tracing::error!("failed to apply schema snapshot: {e}");
            return None;
        }

        Some(Message::PairDdlAck(Bytes::new()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};

    fn test_keyspace() -> KeyspaceMetadata {
        let mut opts = HashMap::new();
        opts.insert("replication_factor".to_string(), "1".to_string());
        KeyspaceMetadata {
            name: "test_ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: opts,
            },
        }
    }

    fn test_table() -> TableMetadata {
        use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
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
        TableMetadata {
            keyspace: "test_ks".to_string(),
            name: "test_tbl".to_string(),
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

    #[test]
    fn ddl_operation_create_keyspace_roundtrip() {
        let op = DdlOperation::CreateKeyspace(test_keyspace());
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::CreateKeyspace(ks) => assert_eq!(ks.name, "test_ks"),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_drop_keyspace_roundtrip() {
        let op = DdlOperation::DropKeyspace("test_ks".to_string());
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::DropKeyspace(name) => assert_eq!(name, "test_ks"),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_create_table_roundtrip() {
        let op = DdlOperation::CreateTable(Box::new(test_table()));
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::CreateTable(t) => {
                assert_eq!(t.keyspace, "test_ks");
                assert_eq!(t.name, "test_tbl");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_drop_table_roundtrip() {
        let op = DdlOperation::DropTable {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        match decoded {
            DdlOperation::DropTable { keyspace, table } => {
                assert_eq!(keyspace, "test_ks");
                assert_eq!(table, "test_tbl");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn ddl_operation_from_bytes_invalid_json_returns_error() {
        let result = DdlOperation::from_bytes(b"not valid json at all!!!");
        assert!(result.is_err(), "expected error for invalid JSON");
        let err = result.unwrap_err();
        assert!(
            matches!(err, ClusterError::Internal(_)),
            "expected ClusterError::Internal, got {err:?}"
        );
    }
}
