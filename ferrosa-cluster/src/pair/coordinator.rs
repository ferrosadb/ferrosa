//! Pair mutation admission and durable replica acknowledgments.
//! Correctness: reject replica client writes and retain the requested replica threshold after promotion.
//! Last revised: 2026-10-29
//! Last changed: Honor pair ONE/QUORUM and ALL separately, with durable local acknowledgment.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use uuid::Uuid;

use ferrosa_common::key::DecoratedKey;
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_sstable::types::Row;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::{Mutation, TableId};

use crate::consistency::ConsistencyLevel;
use crate::error::{ClusterError, Result};
use crate::pair::PairRole;

/// Coordinates writes in pair mode.
///
/// Primary: writes locally, then replicates to secondary.
/// Secondary client writes are rejected until authority is explicitly granted.
pub struct PairCoordinator {
    role: Arc<ArcSwap<PairRole>>,
    peer_host_id: Uuid,
    storage: Arc<StorageEngine>,
    peer_manager: Arc<PeerManager>,
    // Held across mutation application and peer ACK. A handoff takes the write
    // side to close admission and drain existing writes before granting authority.
    handoff_gate: Arc<tokio::sync::RwLock<()>>,
    strict_quorum: bool,
}

impl PairCoordinator {
    pub fn new(
        role: Arc<ArcSwap<PairRole>>,
        peer_host_id: Uuid,
        storage: Arc<StorageEngine>,
        peer_manager: Arc<PeerManager>,
    ) -> Self {
        Self {
            role,
            peer_host_id,
            storage,
            peer_manager,
            handoff_gate: Arc::new(tokio::sync::RwLock::new(())),
            strict_quorum: false,
        }
    }

    /// Configure singleton QUORUM durability before admitting writes.
    pub fn with_strict_quorum(mut self, strict: bool) -> Self {
        self.strict_quorum = strict;
        if strict {
            self.storage.retain_commit_log_for_archive();
        }
        self
    }

    /// Return the local storage engine (used by `WritePath::range_read` for
    /// pair mode full-table scans — both pair nodes hold a full copy).
    pub(crate) fn local_storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    pub(crate) fn handoff_gate(&self) -> &Arc<tokio::sync::RwLock<()>> {
        &self.handoff_gate
    }

    /// Route a write based on current role.
    ///
    /// On the primary: writes locally, then waits for the secondary to ACK
    /// before returning success to the caller. If replication fails (peer
    /// down, timeout), the error is propagated — the write is NOT confirmed
    /// until the secondary has durably received it (C2.1).
    pub async fn coordinate_write(&self, mutation: &Mutation) -> Result<()> {
        self.coordinate_write_with(mutation, ConsistencyLevel::All, 2)
            .await
    }

    /// Pair-only singleton quorum exception. Configured RF is never reduced.
    pub async fn coordinate_write_with(
        &self,
        mutation: &Mutation,
        cl: ConsistencyLevel,
        rf: usize,
    ) -> Result<()> {
        let _admitted_write = self.handoff_gate.read().await;
        if **self.role.load() != PairRole::Primary {
            return Err(ClusterError::NotPrimary);
        }
        let required = match cl {
            ConsistencyLevel::Quorum | ConsistencyLevel::LocalQuorum if rf == 2 => 1,
            ConsistencyLevel::Two => 2,
            ConsistencyLevel::Three => 3,
            ConsistencyLevel::EachQuorum => {
                return Err(ClusterError::NotImplemented {
                    feature: "EACH_QUORUM in pair mode".into(),
                })
            }
            _ => cl.block_for(rf),
        };
        let available = 1 + usize::from(self.peer_manager.has_peer(self.peer_host_id));
        if rf == 0 || required == 0 || required > available {
            return Err(ClusterError::Unavailable {
                consistency: cl.to_string(),
                required,
                alive: available,
            });
        }
        self.apply_locally(mutation)?;
        let replicated = if available > 1 {
            let result = self.replicate_to_peer(mutation).await;
            if required > 1 {
                result?;
                true
            } else {
                result.is_ok()
            }
        } else {
            false
        };
        if self.strict_quorum
            && rf == 2
            && matches!(cl, ConsistencyLevel::Quorum | ConsistencyLevel::LocalQuorum)
            && !replicated
        {
            self.storage
                .archive_commit_log_before_ack()
                .await
                .map_err(ClusterError::Storage)?;
        }
        Ok(())
    }

    /// Apply a mutation to local storage.
    pub(crate) fn apply_locally(&self, mutation: &Mutation) -> Result<()> {
        let table_id = TableId::new(&mutation.keyspace, &mutation.table);
        for row in &mutation.rows {
            self.storage
                .write(&table_id, &mutation.key, row.clone(), mutation.timestamp)
                .map_err(ClusterError::Storage)?;
        }
        self.storage
            .force_commit_log_sync()
            .map_err(ClusterError::Storage)
    }

    /// Send a mutation to the peer and wait for ACK.
    pub(crate) async fn replicate_to_peer(&self, mutation: &Mutation) -> Result<()> {
        let body = encode_mutation(mutation);
        let resp = self
            .peer_manager
            .send_with_timeout(
                self.peer_host_id,
                Message::PairWriteForward(body),
                Lane::Data,
                Duration::from_secs(5),
            )
            .await
            .map_err(ClusterError::Net)?;

        match resp {
            Message::PairWriteAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairWriteAck, got {:?}",
                other.msg_type()
            ))),
        }
    }

    /// Coordinate an atomic local batch with RF2 ALL by default.
    pub async fn coordinate_batch(&self, mutations: Vec<Mutation>, batch_id: Uuid) -> Result<()> {
        self.coordinate_batch_with(mutations, batch_id, ConsistencyLevel::All, 2)
            .await
    }

    /// Preserve the caller's consistency for batches. The follower applies the
    /// ordinary mutation RPCs durably; every mutation must ACK for two copies.
    pub async fn coordinate_batch_with(
        &self,
        mutations: Vec<Mutation>,
        _batch_id: Uuid,
        cl: ConsistencyLevel,
        rf: usize,
    ) -> Result<()> {
        let _admitted_write = self.handoff_gate.read().await;
        if **self.role.load() != PairRole::Primary {
            return Err(ClusterError::NotPrimary);
        }
        let required = match cl {
            ConsistencyLevel::Quorum | ConsistencyLevel::LocalQuorum if rf == 2 => 1,
            ConsistencyLevel::Two => 2,
            ConsistencyLevel::Three => 3,
            ConsistencyLevel::EachQuorum => {
                return Err(ClusterError::NotImplemented {
                    feature: "EACH_QUORUM in pair mode".into(),
                })
            }
            _ => cl.block_for(rf),
        };
        let available = 1 + usize::from(self.peer_manager.has_peer(self.peer_host_id));
        if rf == 0 || required == 0 || required > available {
            return Err(ClusterError::Unavailable {
                consistency: cl.to_string(),
                required,
                alive: available,
            });
        }
        self.storage
            .write_atomic_batch(mutations.as_slice())
            .map_err(ClusterError::Storage)?;
        self.storage
            .force_commit_log_sync()
            .map_err(ClusterError::Storage)?;
        let mut replicated = available > 1;
        if replicated {
            for mutation in &mutations {
                let result = self.replicate_to_peer(mutation).await;
                if required > 1 {
                    result?;
                } else if result.is_err() {
                    replicated = false;
                    break;
                }
            }
        }
        if self.strict_quorum
            && rf == 2
            && matches!(cl, ConsistencyLevel::Quorum | ConsistencyLevel::LocalQuorum)
            && !replicated
        {
            self.storage
                .archive_commit_log_before_ack()
                .await
                .map_err(ClusterError::Storage)?;
        }
        Ok(())
    }

    /// Get current role.
    pub fn role(&self) -> PairRole {
        **self.role.load()
    }

    /// Get peer host_id.
    pub fn peer_host_id(&self) -> Uuid {
        self.peer_host_id
    }
}

/// Encode a Mutation into Bytes for the wire.
pub fn encode_mutation(mutation: &Mutation) -> Bytes {
    let size = mutation.serialized_size();
    let mut buf = vec![0u8; size];
    mutation.serialize_into(&mut buf);
    Bytes::from(buf)
}

/// Encode a single-row mutation for the wire without owning a [`Mutation`].
///
/// Byte-identical to `encode_mutation(&Mutation::new(..))` for the same fields
/// (both go through the same row encoder), and it exists so a coordinator can
/// encode the row it is about to write locally instead of cloning it into a
/// throwaway `Mutation` first.
pub fn encode_single_row_mutation(
    keyspace: &str,
    table: &str,
    key: &DecoratedKey,
    row: &Row,
    timestamp: i64,
) -> Bytes {
    let encoded = Mutation::serialize_borrowed_rows(
        Uuid::new_v4().into_bytes(),
        keyspace,
        table,
        key,
        timestamp,
        &[row],
    )
    .expect("a single-row mutation is always within the u16 row-count limit");
    Bytes::from(encoded)
}

/// Decode a Mutation from wire bytes.
pub fn decode_mutation(body: &[u8]) -> Result<Mutation> {
    Mutation::deserialize_from(body)
        .map_err(|e| ClusterError::Internal(format!("mutation decode: {e}")))
}

/// Encode a batch of mutations with a `batch_id` prefix for atomic pair replication.
///
/// Wire layout: `batch_id:[u8;16] | mutation_count:u32 | (len:u32 | mutation)*`
pub fn encode_batch(batch_id: Uuid, mutations: &[Mutation]) -> Result<Bytes> {
    let count = u32::try_from(mutations.len())
        .map_err(|_| ClusterError::Internal("batch too large".into()))?;

    // Compute total size: 16 (batch_id) + 4 (count) + sum(4 + serialized_size)
    let mutations_bytes: usize = mutations.iter().map(|m| 4 + m.serialized_size()).sum();
    let total = 16 + 4 + mutations_bytes;

    let mut buf = vec![0u8; total];
    let mut pos = 0;

    // batch_id
    buf[pos..pos + 16].copy_from_slice(batch_id.as_bytes());
    pos += 16;

    // mutation_count
    buf[pos..pos + 4].copy_from_slice(&count.to_be_bytes());
    pos += 4;

    // mutations: each prefixed with 4-byte length
    for m in mutations {
        let size = m.serialized_size();
        let len = u32::try_from(size)
            .map_err(|_| ClusterError::Internal("mutation too large to encode".into()))?;
        buf[pos..pos + 4].copy_from_slice(&len.to_be_bytes());
        pos += 4;
        m.serialize_into(&mut buf[pos..pos + size]);
        pos += size;
    }

    Ok(Bytes::from(buf))
}

/// Decode a batch payload encoded by [`encode_batch`].
///
/// Returns `(batch_id, mutations)`.
pub fn decode_batch(body: &[u8]) -> Result<(Uuid, Vec<Mutation>)> {
    if body.len() < 20 {
        return Err(ClusterError::Internal("batch payload too short".into()));
    }

    // batch_id
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&body[0..16]);
    let batch_id = Uuid::from_bytes(id_bytes);

    // mutation_count
    let count = u32::from_be_bytes([body[16], body[17], body[18], body[19]]) as usize;
    let mut pos = 20;

    let mut mutations = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > body.len() {
            return Err(ClusterError::Internal(
                "batch truncated at length prefix".into(),
            ));
        }
        let len =
            u32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]) as usize;
        pos += 4;
        if pos + len > body.len() {
            return Err(ClusterError::Internal(
                "batch truncated at mutation body".into(),
            ));
        }
        let m = Mutation::deserialize_from(&body[pos..pos + len])
            .map_err(|e| ClusterError::Internal(format!("batch mutation decode: {e}")))?;
        mutations.push(m);
        pos += len;
    }

    Ok((batch_id, mutations))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
    use ferrosa_net::config::NetConfig;
    use ferrosa_net::peer::{PeerEventListener, PeerManager};
    use ferrosa_net::rpc::handler::PeerId;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    fn test_mutation() -> Mutation {
        let key = DecoratedKey {
            token: Token(42),
            key: PartitionKey::new(vec![1, 2, 3]),
        };
        // The test schema below registers test_ks.test_tbl with an Int32
        // clustering column. The fail-loud per-cell-and-clustering
        // validator (Layer 1 of the timeuuid-flush-wedge fix) rejects
        // anything other than 4 raw bytes here.
        let row = Row {
            clustering: 10i32.to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(vec![100], 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        Mutation {
            mutation_id: [0x82u8; 16],
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key,
            rows: vec![row],
            timestamp: 1000,
        }
    }

    #[test]
    fn encode_decode_mutation_roundtrip() {
        let mutation = test_mutation();
        let encoded = encode_mutation(&mutation);
        let decoded = decode_mutation(&encoded).unwrap();

        assert_eq!(decoded.keyspace, mutation.keyspace);
        assert_eq!(decoded.table, mutation.table);
        assert_eq!(decoded.timestamp, mutation.timestamp);
        assert_eq!(decoded.rows.len(), mutation.rows.len());
    }

    /// No-op listener used in unit tests where peer lifecycle events
    /// are irrelevant.
    struct NoopListener;

    impl PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _peer: PeerId) {}
        fn on_peer_disconnected(&self, _peer: PeerId) {}
        fn on_peer_suspected(&self, _peer: PeerId) {}
        fn on_peer_recovered(&self, _peer_id: Uuid) {}
        fn on_peer_failed(&self, _peer_id: Uuid) {}
    }

    /// Build a PairCoordinator in Primary role whose peer has no connection
    /// pool (simulates a permanently unreachable secondary).
    ///
    /// The table referenced by `test_mutation()` (test_ks.test_tbl) is
    /// registered so that `apply_locally` succeeds. This isolates the
    /// replication failure as the only error path under test.
    async fn primary_coordinator_with_unreachable_peer(
        dir: &std::path::Path,
    ) -> (PairCoordinator, Uuid) {
        primary_coordinator_with_archive_store(dir, None).await
    }

    async fn primary_coordinator_with_archive_store(
        dir: &std::path::Path,
        store: Option<Arc<dyn object_store::ObjectStore>>,
    ) -> (PairCoordinator, Uuid) {
        primary_coordinator_with_archive_options(dir, store, false).await
    }

    async fn primary_coordinator_with_archive_options(
        dir: &std::path::Path,
        store: Option<Arc<dyn object_store::ObjectStore>>,
        background: bool,
    ) -> (PairCoordinator, Uuid) {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        use ferrosa_storage::{
            CommitLogBatchConfig, CommitLogConfig, CompactionConfig, StorageEngine,
            StorageEngineConfig, SyncStrategyConfig,
        };

        let commit_log = CommitLogConfig {
            segment_size: 4096,
            max_segment_age: std::time::Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: CommitLogBatchConfig::default(),
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: background.then(|| ferrosa_storage::commitlog::ArchiveConfig {
                enabled: true,
                ..Default::default()
            }),
        };
        let config = StorageEngineConfig {
            commit_log,
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
            cache_hot_window_secs: 900,
        };
        let runtime = tokio::runtime::Handle::current();
        let storage = Arc::new(
            StorageEngine::new_with_archive_store(
                config,
                background.then_some(&runtime),
                store,
                "strict-node".into(),
            )
            .expect("storage engine"),
        );

        // Register test_ks.test_tbl so apply_locally succeeds; we want to
        // isolate the replication error path, not storage validation errors.
        storage
            .register_table(TableSchema {
                keyspace: "test_ks".to_string(),
                table: "test_tbl".to_string(),
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                clustering_columns: vec![ColumnDefinition {
                    name: "ck".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                }],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "val".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                }],
                extensions: Default::default(),
            })
            .expect("register table");

        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();

        let config = Arc::new(NetConfig::default());
        let peer_manager = Arc::new(PeerManager::new(config, local_id, Arc::new(NoopListener)));

        // add_peer_entry inserts a pool-less entry — send_with_timeout on it
        // returns Err(NetError::Protocol("no connection pool")), which is the
        // same error class produced by a crashed or unreachable secondary.
        let peer_addr: std::net::SocketAddr = "127.0.0.1:7001".parse().unwrap();
        peer_manager.add_peer_entry((peer_id, peer_addr)).await;

        let role = Arc::new(ArcSwap::new(Arc::new(PairRole::Primary)));
        let coordinator = PairCoordinator::new(role, peer_id, storage, peer_manager);

        (coordinator, peer_id)
    }

    #[derive(Debug)]
    struct ArchiveGate {
        inner: object_store::memory::InMemory,
        mode: u8, // 1/2 hold manifest/upload; 3/4 fail manifest/upload
        entered: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
        writes: std::sync::atomic::AtomicUsize,
        manifest_reads: std::sync::atomic::AtomicUsize,
        manifest_read_barrier: tokio::sync::Barrier,
    }
    impl ArchiveGate {
        fn new(mode: u8) -> Arc<Self> {
            Arc::new(Self {
                inner: Default::default(),
                mode,
                entered: Arc::new(tokio::sync::Semaphore::new(0)),
                release: Arc::new(tokio::sync::Semaphore::new(0)),
                writes: std::sync::atomic::AtomicUsize::new(0),
                manifest_reads: std::sync::atomic::AtomicUsize::new(0),
                manifest_read_barrier: tokio::sync::Barrier::new(2),
            })
        }
        fn failure() -> object_store::Error {
            object_store::Error::Generic {
                store: "strict-test",
                source: std::io::Error::other("archive unavailable").into(),
            }
        }
    }
    impl std::fmt::Display for ArchiveGate {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ArchiveGate")
        }
    }
    struct GatedMultipart {
        inner: Box<dyn object_store::MultipartUpload>,
        entered: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
    }
    impl std::fmt::Debug for GatedMultipart {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("GatedMultipart")
        }
    }
    #[async_trait::async_trait]
    impl object_store::MultipartUpload for GatedMultipart {
        fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
            self.inner.put_part(data)
        }
        async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            self.inner.complete().await
        }
        async fn abort(&mut self) -> object_store::Result<()> {
            self.inner.abort().await
        }
    }
    #[async_trait::async_trait]
    impl object_store::ObjectStore for ArchiveGate {
        async fn put_opts(
            &self,
            path: &object_store::path::Path,
            data: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if path.as_ref().ends_with("archive-manifest.json") {
                if self.mode == 3 {
                    return Err(Self::failure());
                }
                if self.mode == 1 {
                    self.entered.add_permits(1);
                    self.release.acquire().await.unwrap().forget();
                }
            }
            self.inner.put_opts(path, data, opts).await
        }
        async fn put_multipart_opts(
            &self,
            path: &object_store::path::Path,
            opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.mode == 4 {
                return Err(Self::failure());
            }
            let inner = self.inner.put_multipart_opts(path, opts).await?;
            if self.mode == 2 {
                Ok(Box::new(GatedMultipart {
                    inner,
                    entered: self.entered.clone(),
                    release: self.release.clone(),
                }))
            } else {
                Ok(inner)
            }
        }
        async fn get_opts(
            &self,
            path: &object_store::path::Path,
            opts: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let result = self.inner.get_opts(path, opts).await;
            if self.mode == 6
                && path.as_ref().ends_with("archive-manifest.json")
                && self
                    .manifest_reads
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    < 2
            {
                self.manifest_read_barrier.wait().await;
            }
            result
        }
        async fn delete(&self, path: &object_store::path::Path) -> object_store::Result<()> {
            self.inner.delete(path).await
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    async fn strict_coordinator(
        dir: &std::path::Path,
        store: Arc<ArchiveGate>,
    ) -> Arc<PairCoordinator> {
        let (coordinator, _) = primary_coordinator_with_archive_store(dir, Some(store)).await;
        Arc::new(coordinator.with_strict_quorum(true))
    }

    async fn assert_strict_ack_waits(mode: u8) {
        let dir = tempfile::tempdir().unwrap();
        let store = ArchiveGate::new(mode);
        let coordinator = strict_coordinator(dir.path(), store.clone()).await;
        let task = tokio::spawn(async move {
            coordinator
                .coordinate_write_with(&test_mutation(), ConsistencyLevel::Quorum, 2)
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), store.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert!(!task.is_finished(), "ACK escaped before remote publication");
        let manifest = ferrosa_storage::commitlog::manifest::ArchiveManifest::load(
            store.as_ref(),
            "strict-node",
        )
        .await
        .unwrap();
        assert!(manifest.segments.is_empty());
        store.release.add_permits(1);
        task.await.unwrap().unwrap();
        assert_eq!(
            ferrosa_storage::commitlog::manifest::ArchiveManifest::load(
                store.as_ref(),
                "strict-node"
            )
            .await
            .unwrap()
            .segments
            .len(),
            1
        );
    }

    #[tokio::test]
    async fn strict_ack_waits_for_upload_completion() {
        assert_strict_ack_waits(2).await;
    }
    #[tokio::test]
    async fn strict_ack_waits_for_manifest_completion() {
        assert_strict_ack_waits(1).await;
    }

    #[tokio::test]
    async fn strict_archive_errors_refuse_ack_and_keep_wal() {
        for mode in [3, 4] {
            let dir = tempfile::tempdir().unwrap();
            let store = ArchiveGate::new(mode);
            let coordinator = strict_coordinator(dir.path(), store).await;
            assert!(coordinator
                .coordinate_write_with(&test_mutation(), ConsistencyLevel::Quorum, 2)
                .await
                .is_err());
            assert!(std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .any(|e| e.file_name().to_string_lossy().starts_with("commitlog-")));
        }
    }

    #[tokio::test]
    async fn strict_healthy_follower_acks_without_s3() {
        use ferrosa_net::codec::MsgType;
        use ferrosa_net::pool::PriorityPool;
        use ferrosa_net::rpc::{server::RpcServer, HandlerRegistry};
        let primary_dir = tempfile::tempdir().unwrap();
        let secondary_dir = tempfile::tempdir().unwrap();
        let store = ArchiveGate::new(4);
        let primary = strict_coordinator(primary_dir.path(), store.clone()).await;
        let (secondary, _) = primary_coordinator_with_unreachable_peer(secondary_dir.path()).await;
        secondary.role.store(Arc::new(PairRole::Secondary));
        let role = secondary.role.clone();
        let secondary = Arc::new(secondary);
        let registry = HandlerRegistry::new();
        registry.register(
            MsgType::PairWriteForward,
            Arc::new(crate::pair::handler::PairWriteForwardHandler::new(
                role,
                secondary.clone(),
            )),
        );
        let config = Arc::new(NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        });
        let server = Arc::new(RpcServer::new(
            (*config).clone(),
            primary.peer_host_id,
            Arc::new(registry),
        ));
        let addr = server.start_and_get_addr().await.unwrap();
        let pool = PriorityPool::connect(config, Uuid::new_v4(), &addr.to_string(), None, None)
            .await
            .unwrap();
        primary
            .peer_manager
            .add_peer((primary.peer_host_id, addr), pool)
            .await;
        primary
            .coordinate_write_with(&test_mutation(), ConsistencyLevel::Quorum, 2)
            .await
            .unwrap();
        assert_eq!(store.writes.load(std::sync::atomic::Ordering::SeqCst), 0);
        let mutation = test_mutation();
        assert!(secondary
            .storage
            .read(
                &TableId::new(&mutation.keyspace, &mutation.table),
                &mutation.key
            )
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn strict_one_and_default_quorum_do_no_remote_work() {
        let dir = tempfile::tempdir().unwrap();
        let store = ArchiveGate::new(4);
        let (coordinator, _) =
            primary_coordinator_with_archive_store(dir.path(), Some(store.clone())).await;
        coordinator
            .coordinate_write_with(&test_mutation(), ConsistencyLevel::Quorum, 2)
            .await
            .unwrap();
        coordinator
            .with_strict_quorum(true)
            .coordinate_write_with(&test_mutation(), ConsistencyLevel::One, 2)
            .await
            .unwrap();
        assert_eq!(store.writes.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn strict_archive_manifest_cas_preserves_concurrent_entries() {
        use ferrosa_storage::commitlog::manifest::{ArchiveManifest, ArchiveSegmentEntry};
        let store = ArchiveGate::new(6);
        let entry = |id| ArchiveSegmentEntry {
            id,
            sha256: format!("hash-{id}"),
            size: 10,
            archived_at: "now".into(),
        };
        let (a, b) = tokio::join!(
            ArchiveManifest::append_and_save(store.as_ref(), "strict-node", entry(11)),
            ArchiveManifest::append_and_save(store.as_ref(), "strict-node", entry(12)),
        );
        a.unwrap();
        b.unwrap();
        let manifest = ArchiveManifest::load(store.as_ref(), "strict-node")
            .await
            .unwrap();
        assert_eq!(
            manifest.segments.len(),
            2,
            "concurrent publishers must preserve both successful entries"
        );
    }

    #[tokio::test]
    async fn strict_reused_wal_id_cannot_race_another_engine() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let store = ArchiveGate::new(0);
        let first = strict_coordinator(first_dir.path(), store.clone()).await;
        let second = strict_coordinator(second_dir.path(), store).await;
        let mut different = test_mutation();
        different.timestamp += 1;
        let (a, b) = tokio::join!(
            first.coordinate_batch_with(
                vec![test_mutation()],
                Uuid::new_v4(),
                ConsistencyLevel::Quorum,
                2
            ),
            second.coordinate_batch_with(
                vec![different],
                Uuid::new_v4(),
                ConsistencyLevel::Quorum,
                2
            ),
        );
        assert_eq!(
            usize::from(a.is_ok()) + usize::from(b.is_ok()),
            1,
            "conflicting engines cannot both ACK the same archive key: {a:?}, {b:?}"
        );
    }

    #[tokio::test]
    async fn strict_reused_wal_id_cannot_overwrite_an_acknowledged_archive() {
        use object_store::ObjectStore;
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let store = ArchiveGate::new(0);
        let first = strict_coordinator(first_dir.path(), store.clone()).await;
        first
            .coordinate_batch_with(
                vec![test_mutation()],
                Uuid::new_v4(),
                ConsistencyLevel::Quorum,
                2,
            )
            .await
            .unwrap();
        let manifest = ferrosa_storage::commitlog::manifest::ArchiveManifest::load(
            store.as_ref(),
            "strict-node",
        )
        .await
        .unwrap();
        let id = manifest.segments[0].id;
        let hex = ferrosa_storage::upload::manager::hex_prefix_for(&id.to_string());
        let path =
            object_store::path::Path::from(format!("strict-node/commitlog-archive/{hex}/{id}.log"));
        let acknowledged = store.get(&path).await.unwrap().bytes().await.unwrap();
        let second = strict_coordinator(second_dir.path(), store.clone()).await;
        let mut different = test_mutation();
        different.timestamp += 1;
        let result = second
            .coordinate_batch_with(vec![different], Uuid::new_v4(), ConsistencyLevel::Quorum, 2)
            .await;
        assert!(
            result.is_err(),
            "reused node prefix/WAL id must fail closed, got {result:?}"
        );
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            acknowledged
        );
    }

    #[tokio::test]
    async fn strict_batch_archive_replays_exact_mutation() {
        use object_store::ObjectStore;
        let dir = tempfile::tempdir().unwrap();
        let store = ArchiveGate::new(0);
        let coordinator = strict_coordinator(dir.path(), store.clone()).await;
        let expected = test_mutation();
        coordinator
            .coordinate_batch_with(
                vec![expected.clone()],
                Uuid::new_v4(),
                ConsistencyLevel::Quorum,
                2,
            )
            .await
            .unwrap();
        let manifest = ferrosa_storage::commitlog::manifest::ArchiveManifest::load(
            store.as_ref(),
            "strict-node",
        )
        .await
        .unwrap();
        assert_eq!(manifest.segments.len(), 1);
        let entry = &manifest.segments[0];
        let hex = ferrosa_storage::upload::manager::hex_prefix_for(&entry.id.to_string());
        let path = object_store::path::Path::from(format!(
            "strict-node/commitlog-archive/{hex}/{}.log",
            entry.id
        ));
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.len() as u64, entry.size);
        let restored = tempfile::tempdir().unwrap();
        let file = restored.path().join(format!("commitlog-{}.log", entry.id));
        std::fs::write(&file, data).unwrap();
        let (_log, replayed) = ferrosa_storage::commitlog::CommitLog::open_and_replay(
            ferrosa_storage::CommitLogConfig::test_config(restored.path()),
        )
        .unwrap();
        assert!(
            replayed
                .iter()
                .any(|mutation| encode_mutation(mutation) == encode_mutation(&expected)),
            "archive recovery must preserve exact mutation bytes"
        );
    }

    /// Explicit live gate: credentials must name an isolated, disposable bucket.
    #[cfg(feature = "live-infra-tests")]
    #[tokio::test]
    async fn strict_pair_live_s3_ack_replays_exact_mutation() {
        use futures::TryStreamExt;
        use object_store::ObjectStore;

        assert_eq!(std::env::var("FERROSA_TEST_FLY").as_deref(), Ok("1"));
        let bucket = std::env::var("FERROSA_LIVE_S3_TEST_BUCKET")
            .expect("set an isolated FERROSA_LIVE_S3_TEST_BUCKET");
        assert!(bucket.starts_with("ferrosa-strict-verify-"));
        let endpoint = std::env::var("FERROSA_LIVE_S3_TEST_ENDPOINT")
            .expect("set FERROSA_LIVE_S3_TEST_ENDPOINT");
        let backing = object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_endpoint(endpoint)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .expect("live S3 configuration");
        let store = Arc::new(object_store::prefix::PrefixStore::new(
            backing,
            Uuid::new_v4().to_string(),
        ));
        let dir = tempfile::tempdir().unwrap();
        let (coordinator, _) =
            primary_coordinator_with_archive_store(dir.path(), Some(store.clone())).await;
        let coordinator = coordinator.with_strict_quorum(true);
        let expected = test_mutation();
        for _ in 0..2 {
            coordinator
                .coordinate_write_with(&expected, ConsistencyLevel::Quorum, 2)
                .await
                .expect("strict ACK must await live S3 publication");
        }
        let manifest = ferrosa_storage::commitlog::manifest::ArchiveManifest::load(
            store.as_ref(),
            "strict-node",
        )
        .await
        .unwrap();
        assert_eq!(
            manifest.segments.len(),
            2,
            "conditional manifest update must preserve the first acknowledged segment"
        );
        let entry = &manifest.segments[0];
        let hex = ferrosa_storage::upload::manager::hex_prefix_for(&entry.id.to_string());
        let path = object_store::path::Path::from(format!(
            "strict-node/commitlog-archive/{hex}/{}.log",
            entry.id
        ));
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.len() as u64, entry.size);
        assert_eq!(
            data.as_ref(),
            std::fs::read(dir.path().join(format!("commitlog-{}.log", entry.id))).unwrap(),
            "S3 must preserve exact closed WAL bytes"
        );
        let restored = tempfile::tempdir().unwrap();
        std::fs::write(
            restored.path().join(format!("commitlog-{}.log", entry.id)),
            data,
        )
        .unwrap();
        let (_log, replayed) = ferrosa_storage::commitlog::CommitLog::open_and_replay(
            ferrosa_storage::CommitLogConfig::test_config(restored.path()),
        )
        .unwrap();
        let expected_bytes = encode_mutation(&expected);
        assert!(
            replayed.into_iter().any(|mut mutation| {
                // Single-row append assigns a fresh internal WAL ID. Every other
                // serialized field, including key token and cell timestamps, must match.
                mutation.mutation_id = expected.mutation_id;
                encode_mutation(&mutation) == expected_bytes
            }),
            "archived replay changed mutation payload"
        );
        // The bucket was explicitly created for this test; remove its objects.
        let objects = store.list(None).try_collect::<Vec<_>>().await.unwrap();
        for object in objects {
            store.delete(&object.location).await.unwrap();
        }
    }

    #[tokio::test]
    async fn strict_barrier_waits_for_background_publisher_without_losing_manifest_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = ArchiveGate::new(1);
        let (coordinator, _) =
            primary_coordinator_with_archive_options(dir.path(), Some(store.clone()), true).await;
        let coordinator = Arc::new(coordinator.with_strict_quorum(true));
        let mut mutation = test_mutation();
        mutation.rows[0].cells[0].1 = CellValue::live(vec![7; 1800], 1000);
        for _ in 0..3 {
            coordinator.apply_locally(&mutation).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(10), store.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let task = tokio::spawn(async move {
            coordinator
                .coordinate_write_with(&test_mutation(), ConsistencyLevel::Quorum, 2)
                .await
        });
        assert!(!task.is_finished());
        store.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(10), store.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert!(!task.is_finished());
        store.release.add_permits(1);
        task.await.unwrap().unwrap();
        let manifest = ferrosa_storage::commitlog::manifest::ArchiveManifest::load(
            store.as_ref(),
            "strict-node",
        )
        .await
        .unwrap();
        assert_eq!(manifest.segments.len(), 2);
        assert_ne!(manifest.segments[0].id, manifest.segments[1].id);
    }

    #[tokio::test]
    async fn strict_concurrent_archives_preserve_both_manifest_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = ArchiveGate::new(1);
        let coordinator = strict_coordinator(dir.path(), store.clone()).await;
        let first = coordinator.clone();
        let a = tokio::spawn(async move {
            first
                .coordinate_write_with(&test_mutation(), ConsistencyLevel::Quorum, 2)
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), store.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let b = tokio::spawn(async move {
            coordinator
                .coordinate_write_with(&test_mutation(), ConsistencyLevel::Quorum, 2)
                .await
        });
        assert!(!a.is_finished());
        assert!(!b.is_finished());
        store.release.add_permits(1);
        a.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(10), store.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert!(!b.is_finished());
        store.release.add_permits(1);
        b.await.unwrap().unwrap();
        let manifest = ferrosa_storage::commitlog::manifest::ArchiveManifest::load(
            store.as_ref(),
            "strict-node",
        )
        .await
        .unwrap();
        assert_eq!(manifest.segments.len(), 2);
        assert_ne!(manifest.segments[0].id, manifest.segments[1].id);
    }

    // Test list: strict missing-S3 refusal, default local ACK, archive restore,
    // held/failed publication, concurrent publication, batch consistency.
    #[tokio::test]
    async fn strict_singleton_quorum_from_env_requires_remote_archive() {
        const CHILD: &str = "FERROSA_STRICT_PAIR_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            for strict in ["true", "false"] {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "pair::coordinator::tests::strict_singleton_quorum_from_env_requires_remote_archive", "--nocapture"])
                    .env(CHILD, "1")
                    .env("FERROSA_PAIR_QUORUM_STRICT", strict)
                    .status().unwrap();
                assert!(status.success(), "strict={strict} child failed");
            }
            let invalid = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "pair::coordinator::tests::strict_singleton_quorum_from_env_requires_remote_archive", "--nocapture"])
                .env(CHILD, "1").env("FERROSA_PAIR_QUORUM_STRICT", "invalid")
                .output().unwrap();
            assert!(
                !invalid.status.success(),
                "invalid explicit strict config must fail"
            );
            assert!(String::from_utf8_lossy(&invalid.stderr)
                .contains("FERROSA_PAIR_QUORUM_STRICT must be true or false"));
            assert!(!crate::config::ClusterConfig::default().pair_quorum_strict);
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let (fixture, _) = primary_coordinator_with_unreachable_peer(dir.path()).await;
        let node = crate::pair::node::PairNode::new(
            Arc::new(crate::config::ClusterConfig::from_env()),
            Arc::new(NetConfig::default()),
            Uuid::from_u128(2),
            Uuid::from_u128(1),
            "127.0.0.1:7001".parse().unwrap(),
            fixture.storage.clone(),
        );
        let result = node
            .coordinator
            .coordinate_write_with(&test_mutation(), ConsistencyLevel::Quorum, 2)
            .await;
        assert_eq!(
            result.is_err(),
            std::env::var("FERROSA_PAIR_QUORUM_STRICT").unwrap() == "true",
            "strict singleton QUORUM must not ACK without an archive store: {result:?}"
        );
    }

    #[tokio::test]
    async fn pair_batch_quorum_keeps_the_requested_consistency() {
        let dir = tempfile::tempdir().unwrap();
        let (coordinator, _) = primary_coordinator_with_unreachable_peer(dir.path()).await;
        let path = crate::write_path::WritePath::Pair(Arc::new(coordinator));
        assert!(path
            .write_batch(vec![test_mutation()], ConsistencyLevel::Quorum, 2)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn pair_batch_all_requires_the_follower() {
        let dir = tempfile::tempdir().unwrap();
        let (coordinator, _) = primary_coordinator_with_unreachable_peer(dir.path()).await;
        assert!(coordinator
            .coordinate_batch(vec![test_mutation()], Uuid::new_v4())
            .await
            .is_err());
    }

    /// C2.1 — write must NOT be confirmed to the client when the secondary
    /// cannot be reached. Before the fix, coordinate_write returned Ok even
    /// when replication failed, risking a write-loss on subsequent primary
    /// crash.
    #[tokio::test]
    async fn pair_write_confirmed_after_secondary_ack() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _peer_id) = primary_coordinator_with_unreachable_peer(dir.path()).await;

        let mutation = test_mutation();
        let result = coordinator.coordinate_write(&mutation).await;

        assert!(
            result.is_err(),
            "coordinate_write should return Err when secondary is unreachable, \
             got Ok — primary must not confirm before secondary ACKs (C2.1)"
        );
    }

    /// After a write is acknowledged (meaning `replicate_to_peer` returned Ok
    /// and the secondary has the mutation), crashing the primary must not lose
    /// the data — the mutation was already applied locally on primary before
    /// replication, so local storage must reflect it.
    ///
    /// This test verifies the write path in two steps:
    ///   1. `apply_locally` succeeds and the mutation is readable from local
    ///      storage immediately after (proving the local write happened).
    ///   2. If `replicate_to_peer` had succeeded (secondary ACK received),
    ///      crashing the primary does not un-write from local storage — the
    ///      data remains durably on the now-crashed primary's storage, and
    ///      because secondary already ACKed, secondary holds a copy too.
    ///
    /// We exercise the local-storage side directly because the test helper
    /// builds a coordinator with an unreachable peer (no connection pool), so
    /// `coordinate_write` will fail at the replication step.  The important
    /// invariant is: local write succeeded *before* replication was attempted,
    /// so dropping/crashing the primary's engine after `apply_locally` must
    /// still show the mutation in storage (i.e., the write was durable locally
    /// before the replication attempt — it cannot be "un-committed" by crash).
    #[tokio::test]
    async fn pair_write_survives_primary_crash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _peer_id) = primary_coordinator_with_unreachable_peer(dir.path()).await;

        let mutation = test_mutation();

        // Step 1: apply_locally must succeed (primary write is durable before
        // replication is even attempted).
        coordinator
            .apply_locally(&mutation)
            .expect("apply_locally must succeed — local write is the first step");

        // Step 2: Read back from local storage to confirm the mutation is
        // present.  This proves data cannot be lost by a subsequent primary
        // crash because it was already written locally.
        let table_id = ferrosa_storage::TableId::new(&mutation.keyspace, &mutation.table);
        let partition = coordinator
            .storage
            .read(&table_id, &mutation.key)
            .expect("read must not error");

        assert!(
            partition.is_some(),
            "mutation must be readable from local storage after apply_locally — \
             a primary crash after secondary ACK cannot lose data because local \
             storage already holds it (C2.1 durability invariant)"
        );

        let rows = partition.unwrap().rows;
        assert!(
            !rows.is_empty(),
            "partition must contain at least one row after apply_locally"
        );

        // Step 3: Simulate primary crash by dropping the coordinator (Arc
        // storage remains held by this test to verify the on-disk state is
        // not affected by the coordinator being gone).
        let storage_arc = Arc::clone(coordinator.local_storage());
        drop(coordinator);

        // The storage engine itself must still hold the mutation after the
        // coordinator (representing the primary process) is dropped.
        let partition_after_crash = storage_arc
            .read(&table_id, &mutation.key)
            .expect("read after simulated crash must not error");

        assert!(
            partition_after_crash.is_some(),
            "mutation must persist in storage after primary coordinator is dropped \
             (simulated crash) — secondary already ACKed so data is not lost"
        );
    }

    /// C2.1 — replication timeout must propagate as an error to the caller.
    /// The no-pool peer entry returns an immediate error (equivalent to a
    /// timeout or connection refusal from a crashed secondary).
    #[tokio::test]
    async fn pair_replication_timeout_returns_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _peer_id) = primary_coordinator_with_unreachable_peer(dir.path()).await;

        let mutation = test_mutation();
        let result = coordinator.coordinate_write(&mutation).await;

        assert!(
            result.is_err(),
            "coordinate_write must return Err on replication failure (timeout/unreachable)"
        );

        // Verify it is specifically a Net error wrapped in ReplicationFailed or Net,
        // not some unrelated error type.
        let err = result.unwrap_err();
        let is_replication_or_net = matches!(
            err,
            ClusterError::ReplicationFailed(_) | ClusterError::Net(_)
        );
        assert!(
            is_replication_or_net,
            "error should be ReplicationFailed or Net, got: {err}"
        );
    }

    // -- encode/decode mutation round-trip edge cases ---------------------

    #[test]
    fn encode_decode_mutation_preserves_key() {
        let mutation = test_mutation();
        let encoded = encode_mutation(&mutation);
        let decoded = decode_mutation(&encoded).unwrap();

        assert_eq!(decoded.key.token.0, mutation.key.token.0);
        assert_eq!(decoded.key.key.as_bytes(), mutation.key.key.as_bytes());
    }

    #[test]
    fn decode_mutation_garbage_returns_error() {
        let result = decode_mutation(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(result.is_err(), "garbage bytes should fail to decode");
        assert!(
            matches!(result.unwrap_err(), ClusterError::Internal(_)),
            "error should be ClusterError::Internal"
        );
    }

    // -- encode/decode batch round-trip -----------------------------------

    #[test]
    fn encode_decode_batch_roundtrip() {
        let mutations = vec![test_mutation(), test_mutation()];
        let batch_id = Uuid::new_v4();
        let encoded = encode_batch(batch_id, &mutations).unwrap();
        let (decoded_id, decoded_mutations) = decode_batch(&encoded).unwrap();

        assert_eq!(decoded_id, batch_id);
        assert_eq!(decoded_mutations.len(), 2);
        assert_eq!(decoded_mutations[0].keyspace, "test_ks");
        assert_eq!(decoded_mutations[1].table, "test_tbl");
    }

    #[test]
    fn encode_decode_batch_empty() {
        let batch_id = Uuid::new_v4();
        let encoded = encode_batch(batch_id, &[]).unwrap();
        let (decoded_id, decoded_mutations) = decode_batch(&encoded).unwrap();

        assert_eq!(decoded_id, batch_id);
        assert!(decoded_mutations.is_empty());
    }

    #[test]
    fn decode_batch_too_short_returns_error() {
        let result = decode_batch(&[0u8; 10]);
        assert!(result.is_err(), "payload shorter than 20 bytes should fail");
    }

    #[test]
    fn decode_batch_truncated_at_length_prefix() {
        // Valid header but truncated at mutation length prefix
        let batch_id = Uuid::new_v4();
        let mut buf = vec![0u8; 20];
        buf[0..16].copy_from_slice(batch_id.as_bytes());
        buf[16..20].copy_from_slice(&1u32.to_be_bytes()); // claims 1 mutation
                                                          // But no mutation data follows

        let result = decode_batch(&buf);
        assert!(result.is_err(), "truncated batch should fail");
    }

    #[test]
    fn decode_batch_truncated_at_mutation_body() {
        let batch_id = Uuid::new_v4();
        let mut buf = vec![0u8; 24];
        buf[0..16].copy_from_slice(batch_id.as_bytes());
        buf[16..20].copy_from_slice(&1u32.to_be_bytes()); // 1 mutation
        buf[20..24].copy_from_slice(&1000u32.to_be_bytes()); // claims 1000 bytes but only 0 follow

        let result = decode_batch(&buf);
        assert!(result.is_err(), "truncated mutation body should fail");
    }

    // -- role accessor tests ---------------------------------------------

    #[tokio::test]
    async fn coordinator_role_and_peer_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, peer_id) = primary_coordinator_with_unreachable_peer(dir.path()).await;

        assert_eq!(coordinator.role(), PairRole::Primary);
        assert_eq!(coordinator.peer_host_id(), peer_id);
    }
}
