//! Cluster coordinator -- fans out writes and reads to replicas
//! with tunable consistency level enforcement.

pub mod batch;
pub mod cl_routing;
pub mod fulltext_stream;
pub mod metrics;
pub mod range_read_stream;
pub mod read;
pub mod stream_consumer;
pub mod stream_frame_router;
pub mod stream_producer;
pub mod stream_request_handler;
#[cfg(test)]
pub mod streaming_e2e_tests;
pub mod truncate;
pub mod write;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;

use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_net::stream_router::StreamRouter;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use crate::consistency::ConsistencyLevel;
use crate::coordinator::metrics::ReadRepairMetrics;
use crate::hints::HintStore;
use crate::pair::coordinator::decode_mutation;
use crate::raft::state_machine::RaftState;
use crate::ring::TokenRing;

/// Default maximum concurrent in-flight writes. Historically this fixed cap was
/// the primary guard against write load starving Raft heartbeat processing on a
/// shared runtime. Raft is now isolated at the source (dedicated OS thread via
/// `spawn_raft_lane_actor` + a `ferrosa_sched` consensus reservation), so this
/// cap is belt-and-suspenders — overridable via `FERROSA_WRITE_CONCURRENCY_LIMIT`
/// to investigate the real write ceiling and eventually scale with node
/// capacity (see the capacity-aware follow-up).
const DEFAULT_WRITE_CONCURRENCY_LIMIT: usize = 128;

/// Resolve the write-concurrency limit from an already-read env value, falling
/// back to [`DEFAULT_WRITE_CONCURRENCY_LIMIT`]. Pure (takes the env value as an
/// argument) so it is unit-testable without mutating the process environment.
pub(crate) fn parse_write_concurrency_limit(env_val: Option<String>) -> usize {
    env_val
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_WRITE_CONCURRENCY_LIMIT)
}

fn write_concurrency_limit() -> usize {
    parse_write_concurrency_limit(std::env::var("FERROSA_WRITE_CONCURRENCY_LIMIT").ok())
}
const STREAM_REQUEST_ID_EPOCH_BIT: u32 = 1 << 31;
const STREAM_REQUEST_ID_RANDOM_MASK: u32 = (1 << 30) - 1;

/// Coordinates writes and reads across replicas in cluster mode.
pub struct ClusterCoordinator {
    pub(crate) ring: Arc<ArcSwap<TokenRing>>,
    pub(crate) peer_manager: Arc<PeerManager>,
    pub(crate) local_node_id: u64,
    pub(crate) storage: Arc<StorageEngine>,
    pub(crate) default_rf: usize,
    pub(crate) default_cl: ConsistencyLevel,
    /// Optional hint store — when `Some`, failed remote replicas receive hints
    /// after a successful quorum write.  When `None` (e.g. in unit tests),
    /// hint storage is skipped.
    pub(crate) hint_store: Option<Arc<HintStore>>,
    /// Read repair metrics (attempted/succeeded/failed counters).
    pub repair_metrics: Arc<ReadRepairMetrics>,
    /// Bounded sink for async anti-entropy repair requests fired by the read
    /// path when it serves a read around a corrupt local SSTable. The repair
    /// scheduler drains this to refill the affected token range from a healthy
    /// replica (LOCKED DESIGN: serve now, repair in the background).
    pub(crate) anti_entropy_repair_queue: Arc<crate::coordinator::read::AntiEntropyRepairQueue>,
    /// Optional snapshot of Raft state for index-aware replica selection.
    pub(crate) raft_state: Option<Arc<ArcSwap<RaftState>>>,
    /// Bounded semaphore limiting concurrent in-flight writes. Prevents bulk
    /// CQL inserts from saturating the tokio runtime and starving Raft.
    pub(crate) write_semaphore: Arc<tokio::sync::Semaphore>,
    /// ADR-020 streaming range-read dispatch table. Shared with the
    /// `StreamFrameRouter` registered in `HandlerRegistry` — the
    /// router decodes the leading `request_id` of every inbound
    /// `RangeReadStream*` frame and pushes it onto the receiver this
    /// coordinator registered before sending the request.
    pub(crate) stream_router: Arc<StreamRouter>,
    /// Monotonic counter for generating per-call `request_id`s in
    /// streaming range reads. Seeded into a randomized high range so
    /// late chunks from a pre-restart coordinator cannot collide with
    /// freshly registered low request IDs after restart.
    pub(crate) next_request_id: Arc<AtomicU32>,
    /// When `true`, `coordinate_range_read_limited_rows` delegates to
    /// the ADR-020 streaming path instead of the legacy single-shot
    /// `RangeReadRequest` RPC. Toggled by
    /// `FERROSA_BULK_STREAMING_RANGE_READ=0` at process start opts
    /// into the legacy single-shot range RPC for mixed-version
    /// rolling upgrades. Streaming is the default because the legacy
    /// path applies a hard partition cap and cannot serve complete
    /// full-scan semantics.
    pub(crate) streaming_range_reads: bool,
    /// When `true` (the default), no-`LIMIT` `fts_match` queries use the
    /// streaming fulltext protocol (t_4ae47a9f) instead of the legacy
    /// single-message `FulltextSearchRequest` union whose O(matches)
    /// materialization OOM-killed replicas (t_8fc24ce2).
    /// `FERROSA_BULK_STREAMING_FULLTEXT=0` opts into the legacy path for
    /// mixed-version rolling upgrades.
    pub(crate) streaming_fulltext: bool,
}

fn streaming_range_reads_enabled(value: Option<&str>) -> bool {
    !matches!(
        value.map(str::trim),
        Some("0") | Some("false") | Some("FALSE") | Some("off") | Some("OFF")
    )
}

fn stream_request_id_seed(random: u32, local_node_id: u64) -> u32 {
    let node_mix = (local_node_id as u32) ^ ((local_node_id >> 32) as u32);
    STREAM_REQUEST_ID_EPOCH_BIT | ((random ^ node_mix) & STREAM_REQUEST_ID_RANDOM_MASK)
}

fn fresh_stream_request_id_seed(local_node_id: u64) -> u32 {
    let bytes = *uuid::Uuid::new_v4().as_bytes();
    let random = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        ^ u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]])
        ^ u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]])
        ^ u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    stream_request_id_seed(random, local_node_id)
}

impl ClusterCoordinator {
    pub fn new(
        ring: Arc<ArcSwap<TokenRing>>,
        peer_manager: Arc<PeerManager>,
        local_node_id: u64,
        storage: Arc<StorageEngine>,
        default_rf: usize,
        default_cl: ConsistencyLevel,
    ) -> Self {
        let streaming_env = std::env::var("FERROSA_BULK_STREAMING_RANGE_READ").ok();
        let streaming_range_reads = streaming_range_reads_enabled(streaming_env.as_deref());
        let streaming_fulltext_env = std::env::var("FERROSA_BULK_STREAMING_FULLTEXT").ok();
        let streaming_fulltext = streaming_range_reads_enabled(streaming_fulltext_env.as_deref());
        if !streaming_fulltext {
            tracing::warn!(
                "coordinator: legacy single-message fulltext search enabled via \
                 FERROSA_BULK_STREAMING_FULLTEXT=0; broad no-LIMIT fts_match queries \
                 materialize the match set on every replica (t_8fc24ce2 OOM shape)"
            );
        }
        if streaming_range_reads {
            tracing::info!(
                configured = streaming_env.as_deref().unwrap_or("default"),
                "coordinator: ADR-020 streaming range reads enabled"
            );
        } else {
            tracing::warn!(
                "coordinator: legacy capped range reads enabled via FERROSA_BULK_STREAMING_RANGE_READ=0; uncapped full scans will fail rather than return partial results"
            );
        }
        Self {
            ring,
            peer_manager,
            local_node_id,
            storage,
            default_rf,
            default_cl,
            hint_store: None,
            repair_metrics: Arc::new(ReadRepairMetrics::new()),
            anti_entropy_repair_queue: Arc::new(
                crate::coordinator::read::AntiEntropyRepairQueue::default(),
            ),
            raft_state: None,
            write_semaphore: Arc::new(tokio::sync::Semaphore::new(write_concurrency_limit())),
            stream_router: Arc::new(StreamRouter::new()),
            next_request_id: Arc::new(AtomicU32::new(fresh_stream_request_id_seed(local_node_id))),
            streaming_range_reads,
            streaming_fulltext,
        }
    }

    /// Borrow the shared `StreamRouter` so the cluster bootstrap can
    /// build a `StreamFrameRouter` against the same routing table.
    pub fn stream_router(&self) -> Arc<StreamRouter> {
        self.stream_router.clone()
    }

    /// Pull a fresh `request_id` for a streaming range read.
    pub(crate) fn next_stream_request_id(&self) -> u32 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Attach a hint store to this coordinator.
    pub fn with_hint_store(mut self, hint_store: Arc<HintStore>) -> Self {
        self.hint_store = Some(hint_store);
        self
    }

    /// Return the data center of this coordinator's local node.
    ///
    /// Looks up `local_node_id` in the current token ring snapshot.
    /// Returns `None` if the local node is not (yet) registered.
    pub fn local_dc(&self) -> Option<String> {
        let ring = self.ring.load();
        ring.get_node(self.local_node_id)
            .map(|info| info.data_center.clone())
    }

    /// Resolve the replica **host ids** that own `token` under the keyspace
    /// `strategy` from the current ring snapshot. This is the cluster-mode source
    /// for an Accord transaction's participant set (ADR-021): the coordinator
    /// owns the ring, so replica placement stays here instead of leaking into the
    /// CQL/Postgres front-ends.
    pub fn replica_host_ids_for(
        &self,
        token: crate::raft::Token,
        strategy: &crate::ring::strategy::ReplicationStrategy,
    ) -> Vec<uuid::Uuid> {
        self.ring
            .load()
            .replica_host_ids_for_strategy(token, strategy)
    }

    /// Attach a Raft state snapshot for index-aware replica selection.
    pub fn with_raft_state(mut self, state: Arc<ArcSwap<RaftState>>) -> Self {
        self.raft_state = Some(state);
        self
    }
}

// ---------------------------------------------------------------------------
// MutationForwardHandler — receives MutationForward RPCs from coordinators
// ---------------------------------------------------------------------------

/// RPC handler that receives `MutationForward` messages from coordinators
/// and applies them to local storage.
pub struct MutationForwardHandler {
    storage: Arc<StorageEngine>,
}

impl MutationForwardHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl RpcHandler for MutationForwardHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::MutationForward(b) => b,
            _ => return None,
        };
        let mutation = decode_mutation(&body).ok()?;
        let row_count = mutation.rows.len();
        metrics::inc_inbound_mutation_forward(row_count);
        let table_id = TableId::new(&mutation.keyspace, &mutation.table);
        let key = mutation.key;
        let timestamp = mutation.timestamp;
        for row in mutation.rows {
            if let Err(e) = self.storage.write(&table_id, &key, row, timestamp) {
                metrics::inc_inbound_mutation_failure();
                // CRITICAL: Do NOT return MutationAck when the write fails.
                // Returning ACK here would make the coordinator count this as a
                // successful replica write, but the data is NOT stored. This is
                // a phantom ACK that violates consistency guarantees.
                //
                // Common failure causes:
                // - Schema propagation lag: table not yet registered via Raft
                // - Storage errors: disk full, I/O failure
                //
                // By returning None (no response), the coordinator's send()
                // will timeout, treating this as a failed replica. The coordinator
                // will then store a hint for later replay.
                tracing::warn!(
                    %e,
                    table = %table_id,
                    "MutationForward write failed — not sending ACK"
                );
                return None;
            }
        }
        Some(Message::MutationAck(Bytes::new()))
    }
}

// ---------------------------------------------------------------------------
// TruncateForwardHandler — receives TruncateForward RPCs from coordinators
// ---------------------------------------------------------------------------

/// RPC handler that receives `TruncateForward` messages from coordinators
/// and truncates the specified table on local storage.
pub struct TruncateForwardHandler {
    storage: Arc<StorageEngine>,
}

impl TruncateForwardHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl RpcHandler for TruncateForwardHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::TruncateForward(b) => b,
            _ => return None,
        };
        let table_id = match decode_truncate_payload(&body) {
            Some(t) => t,
            None => {
                tracing::warn!("TruncateForward: failed to decode payload");
                return None;
            }
        };
        if let Err(e) = self.storage.truncate(&table_id) {
            tracing::warn!(%e, table = %table_id, "TruncateForward failed — not sending ACK");
            return None;
        }
        Some(Message::TruncateAck(Bytes::new()))
    }
}

/// Encode a truncate payload: `[u16 ks_len][ks_bytes][u16 table_len][table_bytes]`
pub fn encode_truncate_payload(table_id: &TableId) -> Bytes {
    let size = 2 + table_id.keyspace.len() + 2 + table_id.table.len();
    let mut buf = Vec::with_capacity(size);
    buf.extend_from_slice(&(table_id.keyspace.len() as u16).to_be_bytes());
    buf.extend_from_slice(table_id.keyspace.as_bytes());
    buf.extend_from_slice(&(table_id.table.len() as u16).to_be_bytes());
    buf.extend_from_slice(table_id.table.as_bytes());
    Bytes::from(buf)
}

/// Decode a truncate payload back to a `TableId`.
fn decode_truncate_payload(body: &[u8]) -> Option<TableId> {
    if body.len() < 4 {
        return None;
    }
    let mut cursor = body;
    let ks_len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
    cursor = &cursor[2..];
    if cursor.len() < ks_len + 2 {
        return None;
    }
    let keyspace = std::str::from_utf8(&cursor[..ks_len]).ok()?.to_string();
    cursor = &cursor[ks_len..];
    let tbl_len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
    cursor = &cursor[2..];
    if cursor.len() < tbl_len {
        return None;
    }
    let table = std::str::from_utf8(&cursor[..tbl_len]).ok()?.to_string();
    Some(TableId::new(&keyspace, &table))
}

// ---------------------------------------------------------------------------
// RepairWriteHandler — receives RepairWrite RPCs from coordinators
// ---------------------------------------------------------------------------

/// RPC handler that receives `RepairWrite` messages from coordinators
/// performing inline read repair. Applies the mutation directly to local
/// storage without forwarding to other replicas (prevents cascade).
pub struct RepairWriteHandler {
    storage: Arc<StorageEngine>,
    metrics: Arc<metrics::ReadRepairMetrics>,
}

impl RepairWriteHandler {
    pub fn new(storage: Arc<StorageEngine>, metrics: Arc<metrics::ReadRepairMetrics>) -> Self {
        Self { storage, metrics }
    }
}

#[async_trait::async_trait]
impl RpcHandler for RepairWriteHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::RepairWrite(b) => b,
            _ => return None,
        };
        let mutation = match decode_mutation(&body) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("RepairWriteHandler: failed to decode mutation: {e}");
                self.metrics.inc_failed();
                return None;
            }
        };
        let table_id = TableId::new(&mutation.keyspace, &mutation.table);
        let key = mutation.key;
        let timestamp = mutation.timestamp;
        for row in mutation.rows {
            if let Err(e) = self.storage.write(&table_id, &key, row, timestamp) {
                tracing::warn!("RepairWriteHandler: storage write failed: {e}");
                self.metrics.inc_failed();
                return None;
            }
        }
        self.metrics.inc_succeeded();
        // Fire-and-forget: no response.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ClusterError;
    use crate::pair::coordinator::encode_mutation;

    #[test]
    fn write_concurrency_limit_parses_override_else_defaults() {
        assert_eq!(parse_write_concurrency_limit(Some("512".to_string())), 512);
        assert_eq!(
            parse_write_concurrency_limit(Some("  4096 ".to_string())),
            4096
        );
        // invalid / sub-1 / absent -> default
        assert_eq!(
            parse_write_concurrency_limit(None),
            DEFAULT_WRITE_CONCURRENCY_LIMIT
        );
        assert_eq!(
            parse_write_concurrency_limit(Some("0".to_string())),
            DEFAULT_WRITE_CONCURRENCY_LIMIT
        );
        assert_eq!(
            parse_write_concurrency_limit(Some("garbage".to_string())),
            DEFAULT_WRITE_CONCURRENCY_LIMIT
        );
    }
    use crate::raft::{NodeInfo, NodeState};
    use crate::ring::TokenRing;
    use ferrosa_common::key::DecoratedKey;
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::{CellValue, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, Mutation, StorageEngineConfig};
    use uuid::Uuid;

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            write_verify: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn make_node(addr: &str) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        }
    }

    fn test_key() -> DecoratedKey {
        DecoratedKey {
            token: Token(42),
            key: PartitionKey::new(vec![1, 2, 3]),
        }
    }

    fn test_row() -> Row {
        Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }
    }

    fn register_test_table(storage: &StorageEngine) {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        storage.register_table(schema).unwrap();
    }

    #[test]
    fn streaming_range_reads_are_enabled_by_default() {
        assert!(streaming_range_reads_enabled(None));
    }

    #[test]
    fn streaming_range_reads_allow_explicit_legacy_opt_out() {
        for value in ["0", "false", "FALSE", "off", "OFF"] {
            assert!(
                !streaming_range_reads_enabled(Some(value)),
                "{value} must disable streaming range reads"
            );
        }
    }

    #[test]
    fn streaming_range_reads_accept_explicit_enable_values() {
        for value in ["1", "true", "TRUE", "on", "anything-else"] {
            assert!(
                streaming_range_reads_enabled(Some(value)),
                "{value} must keep streaming range reads enabled"
            );
        }
    }

    #[test]
    fn fresh_coordinator_does_not_reuse_low_stream_request_ids_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(TokenRing::new())),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            1,
            storage,
            1,
            ConsistencyLevel::One,
        );

        let request_id = coordinator.next_stream_request_id();
        assert_ne!(
            request_id, 1,
            "fresh coordinators must not restart streaming range-read request IDs at 1; \
             late internode chunks from a pre-restart stream are otherwise indistinguishable \
             from the first frames of a new stream"
        );
    }

    #[test]
    fn stream_request_id_seed_uses_restart_epoch_with_wrap_headroom() {
        let upper_bound = STREAM_REQUEST_ID_EPOCH_BIT + (1 << 30);

        for random in [0, 1, 42, u32::MAX] {
            let seed = stream_request_id_seed(random, 1);
            assert!(
                (STREAM_REQUEST_ID_EPOCH_BIT..upper_bound).contains(&seed),
                "seed {seed} must stay in the high restart-epoch range with at least 2^30 request IDs of headroom"
            );
        }
    }

    #[test]
    fn coordinator_has_repair_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());

        let local_node_id = 1u64;
        let ring = TokenRing::new();

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage,
            1,
            ConsistencyLevel::One,
        );

        // Metrics should start at zero.
        let text = coordinator.repair_metrics.to_prometheus_text();
        assert!(text.contains("ferrosa_read_repairs_attempted_total 0"));
    }

    #[tokio::test]
    async fn coordinate_write_local_replica_writes_to_storage() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            // PeerManager won't be used since we're the local replica
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage.clone(),
            1, // RF=1
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();

        coordinator
            .coordinate_write(&table_id, &key, row, 1000)
            .await
            .unwrap();

        // Verify the write landed in storage
        let result = storage.read(&table_id, &key).unwrap();
        assert!(result.is_some(), "local write should be readable");
    }

    #[tokio::test]
    async fn coordinate_write_unavailable_when_too_few_replicas() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let ring = TokenRing::new(); // empty ring, no replicas

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage.clone(),
            3, // RF=3
            ConsistencyLevel::Quorum,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();

        let result = coordinator
            .coordinate_write(&table_id, &key, row, 1000)
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            ClusterError::Unavailable {
                required, alive, ..
            } => {
                assert_eq!(required, 2); // QUORUM of 3 = 2
                assert_eq!(alive, 0);
            }
            other => panic!("expected Unavailable, got: {other}"),
        }
    }

    #[tokio::test]
    async fn coordinate_read_local_replica_reads_from_storage() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        // Write directly to storage first
        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();
        storage.write(&table_id, &key, row.clone(), 1000).unwrap();

        // Read via coordinator
        let result = coordinator.coordinate_read(&table_id, &key).await.unwrap();
        assert!(result.is_some(), "should read back written data");
        let rows = result.unwrap();
        assert!(!rows.is_empty());
    }

    #[tokio::test]
    async fn mutation_forward_handler_applies_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let handler = MutationForwardHandler::new(storage.clone());

        let mutation = Mutation {
            mutation_id: [0x90u8; 16],
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key: test_key(),
            rows: vec![test_row()],
            timestamp: 1000,
        };
        let body = encode_mutation(&mutation);
        let msg = Message::MutationForward(body);

        let peer_id = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let response = handler.handle(peer_id, msg).await;

        assert!(matches!(response, Some(Message::MutationAck(_))));

        // Verify the mutation was applied
        let table_id = TableId::new("test_ks", "test_tbl");
        let result = storage.read(&table_id, &test_key()).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn truncate_payload_encode_decode_roundtrip() {
        let table_id = TableId::new("my_ks", "my_table");
        let encoded = encode_truncate_payload(&table_id);
        let decoded = decode_truncate_payload(&encoded).unwrap();
        assert_eq!(decoded.keyspace, "my_ks");
        assert_eq!(decoded.table, "my_table");
    }

    #[test]
    fn truncate_payload_decode_rejects_truncated() {
        assert!(decode_truncate_payload(&[]).is_none());
        assert!(decode_truncate_payload(&[0, 5, b'h', b'e']).is_none());
    }

    #[tokio::test]
    async fn truncate_forward_handler_clears_data() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        // Write some data first
        let table_id = TableId::new("test_ks", "test_tbl");
        storage
            .write(&table_id, &test_key(), test_row(), 1000)
            .unwrap();
        assert!(storage.read(&table_id, &test_key()).unwrap().is_some());

        // Send TruncateForward
        let handler = TruncateForwardHandler::new(storage.clone());
        let payload = encode_truncate_payload(&table_id);
        let msg = Message::TruncateForward(payload);
        let peer_id = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let response = handler.handle(peer_id, msg).await;

        assert!(matches!(response, Some(Message::TruncateAck(_))));
        assert!(
            storage.read(&table_id, &test_key()).unwrap().is_none(),
            "data should be cleared after truncate"
        );
    }

    #[tokio::test]
    async fn truncate_forward_handler_no_ack_on_missing_table() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        // Do NOT register the table

        let handler = TruncateForwardHandler::new(storage.clone());
        let table_id = TableId::new("nonexistent_ks", "nonexistent_tbl");
        let payload = encode_truncate_payload(&table_id);
        let msg = Message::TruncateForward(payload);
        let peer_id = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let response = handler.handle(peer_id, msg).await;

        assert!(
            response.is_none(),
            "should not ACK truncate of missing table"
        );
    }

    #[tokio::test]
    async fn coordinate_truncate_clears_local_data() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        // Write data
        let table_id = TableId::new("test_ks", "test_tbl");
        storage
            .write(&table_id, &test_key(), test_row(), 1000)
            .unwrap();
        assert!(storage.read(&table_id, &test_key()).unwrap().is_some());

        // Truncate via coordinator (single-node cluster, no remotes)
        coordinator.coordinate_truncate(&table_id).await.unwrap();

        assert!(
            storage.read(&table_id, &test_key()).unwrap().is_none(),
            "local data should be cleared after coordinate_truncate"
        );
    }

    /// No-op listener for tests that don't care about peer events.
    struct NoopListener;
    impl ferrosa_net::peer::PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _peer: PeerId) {}
        fn on_peer_disconnected(&self, _peer: PeerId) {}
        fn on_peer_suspected(&self, _peer: PeerId) {}
        fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
        fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
    }

    /// MutationForwardHandler must NOT return ACK when the write fails.
    /// Returning ACK on failure is a phantom acknowledgement that makes
    /// the coordinator believe the replica has the data when it doesn't.
    #[tokio::test]
    async fn mutation_forward_handler_no_ack_on_write_failure() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        // Deliberately do NOT register the test table — simulates schema
        // propagation lag where the table doesn't exist on this node yet.

        let handler = MutationForwardHandler::new(storage.clone());

        let mutation = Mutation {
            mutation_id: [0x91u8; 16],
            keyspace: "nonexistent_ks".to_string(),
            table: "nonexistent_tbl".to_string(),
            key: test_key(),
            rows: vec![test_row()],
            timestamp: 1000,
        };
        let body = encode_mutation(&mutation);
        let msg = Message::MutationForward(body);

        let peer_id = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let response = handler.handle(peer_id, msg).await;

        // The handler must NOT return MutationAck when the write fails.
        // Returning None causes the coordinator's send() to timeout,
        // which correctly counts as a failed replica.
        assert!(
            response.is_none(),
            "MutationForwardHandler must NOT return ACK when write fails — \
             this would be a phantom ACK causing the coordinator to think the \
             replica has the data when it doesn't"
        );
    }

    #[test]
    fn local_dc_returns_coordinator_datacenter() {
        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        let mut node = make_node("10.0.0.1:7000");
        node.data_center = "us-east-1".to_string();
        ring.add_node(local_node_id, node);
        ring.assign_tokens(local_node_id, &[0]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            test_storage(tempfile::tempdir().unwrap().path()),
            1,
            ConsistencyLevel::One,
        );

        assert_eq!(coordinator.local_dc(), Some("us-east-1".to_string()));
    }

    #[tokio::test]
    async fn repair_write_handler_applies_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let metrics = Arc::new(super::metrics::ReadRepairMetrics::new());
        let handler = super::RepairWriteHandler::new(storage.clone(), metrics.clone());

        let mutation = Mutation {
            mutation_id: [0x91u8; 16],
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key: test_key(),
            rows: vec![test_row()],
            timestamp: 1000,
        };
        let body = encode_mutation(&mutation);
        let msg = Message::RepairWrite(body);

        let peer_id = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let response = handler.handle(peer_id, msg).await;

        // Fire-and-forget: no response expected.
        assert!(response.is_none(), "RepairWrite is fire-and-forget");

        // Verify the mutation was applied to storage.
        let table_id = TableId::new("test_ks", "test_tbl");
        let result = storage.read(&table_id, &test_key()).unwrap();
        assert!(result.is_some(), "repair write should land in storage");

        // Metrics should show one success.
        assert_eq!(
            metrics
                .read_repairs_succeeded
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn repair_write_handler_ignores_wrong_message_type() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let metrics = Arc::new(super::metrics::ReadRepairMetrics::new());
        let handler = super::RepairWriteHandler::new(storage, metrics);

        let peer_id = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let msg = Message::Ping {
            nonce: 42,
            sent_at: 0,
        };
        let response = handler.handle(peer_id, msg).await;
        assert!(response.is_none());
    }

    #[tokio::test]
    async fn repair_write_handler_corrupt_body_increments_failed() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let metrics = Arc::new(super::metrics::ReadRepairMetrics::new());
        let handler = super::RepairWriteHandler::new(storage, metrics.clone());

        let peer_id = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let msg = Message::RepairWrite(Bytes::from_static(b"garbage"));
        let response = handler.handle(peer_id, msg).await;
        assert!(response.is_none());

        assert_eq!(
            metrics
                .read_repairs_failed
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn coordinator_write_read_creates_spans() {
        use std::sync::atomic::AtomicU64;

        struct SpanCollector {
            names: Arc<std::sync::Mutex<Vec<String>>>,
            next_id: AtomicU64,
        }

        impl tracing::Subscriber for SpanCollector {
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                self.names
                    .lock()
                    .unwrap()
                    .push(span.metadata().name().to_string());
                let id = self
                    .next_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                tracing::span::Id::from_u64(id)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn event(&self, _: &tracing::Event<'_>) {}
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
        }

        let shared_names: Arc<std::sync::Mutex<Vec<String>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        let _guard = tracing::subscriber::set_default(SpanCollector {
            names: Arc::clone(&shared_names),
            next_id: AtomicU64::new(0),
        });

        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage,
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();

        coordinator
            .coordinate_write(&table_id, &key, row, 1000)
            .await
            .unwrap();
        coordinator.coordinate_read(&table_id, &key).await.unwrap();

        let recorded = shared_names.lock().unwrap();
        assert!(
            recorded.iter().any(|n| n == "cluster.write"),
            "expected 'cluster.write' span, got: {:?}",
            *recorded
        );
        assert!(
            recorded.iter().any(|n| n == "cluster.read"),
            "expected 'cluster.read' span, got: {:?}",
            *recorded
        );
    }
}
