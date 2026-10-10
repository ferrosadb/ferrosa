//! `AccordTransactionCommitter` — the cluster-side implementation of the
//! [`TransactionCommitter`] seam (ADR-021, increment 2b).
//!
//! [`TransactionCommitter`]: ferrosa_storage::accord::TransactionCommitter
//!
//! CQL/Postgres `BEGIN`/`COMMIT` buffer DML into a write-set and call
//! [`commit`](TransactionCommitter::commit); this routes the whole write-set
//! through one multi-key Accord transaction:
//!
//! 1. **resolve replicas per key** — via the injected `resolve` closure, which in
//!    production wraps `WritePath::accord_replicas_for_key` (token-aware, RF-correct,
//!    #185) keyed by each write's keyspace replication;
//! 2. **per-shard quorum** — `AccordCoordinatorDriver::new_multi` builds a per-key
//!    `ParticipantSet` and drives PreAccept(V2)/Commit/Apply under per-shard quorum;
//! 3. **unconditional apply** — a general transaction has no `IF`, so it runs in
//!    [`ReadPredicate::Always`] mode (no read-vote; #190).

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use ferrosa_common::accord::HybridLogicalClock;
use ferrosa_storage::accord::{CommitError, CommitOutcome, TransactionCommitter, TransactionWrite};

use crate::accord::apply::StorageApplier;
use crate::accord::coordinator::{AccordCoordinatorDriver, AccordDriverError};
use crate::accord::transport::AccordTransport;
use crate::accord::wire::ReadPredicate;

/// Resolves the replica host ids that own `key` in `keyspace`. `None` means the
/// key cannot be placed (e.g. not in cluster mode, or unknown keyspace) — the
/// commit fails loud rather than guessing.
pub type ReplicaResolver = Arc<dyn Fn(&str, &[u8]) -> Option<Vec<Uuid>> + Send + Sync>;

/// Replicates a table-level tombstone (`TRUNCATE`) marker to **every node that
/// serves the table**, requiring every one to acknowledge
/// (`ConsistencyLevel::All`).
///
/// Accord orders a transaction by per-shard **quorum**, but a table tombstone is
/// ONE reserved partition key whose write Accord would route to that key's RF
/// replica set — a proper subset of the ring whenever `RF < node count`. A
/// quorum on that subset leaves the nodes outside it serving the truncated rows
/// (resurrection), and Accord's quorum can also decide without a node that is
/// down and repair it later. A table tombstone must be held by every serving
/// node NOW, so this seam is deliberately NOT Accord's quorum path: it fans the
/// marker to the whole serving set and fails loud on any missing acknowledgement.
#[async_trait]
pub trait AllServingMarkerWriter: Send + Sync {
    /// Write the table-tombstone `mutation_bytes` (a serialized single-partition
    /// `Mutation`) to every node serving the table. `Err` if ANY node does not
    /// acknowledge — never a silent degrade to quorum.
    async fn write_marker_to_all_serving_nodes(&self, mutation_bytes: &[u8]) -> Result<(), String>;
}

/// Cluster-side [`TransactionCommitter`]: commits a buffered multi-key write-set
/// as one unconditional Accord transaction.
pub struct AccordTransactionCommitter {
    /// This (coordinator) node's id — derived from its host UUID like the driver.
    node_id: u64,
    clock: Arc<HybridLogicalClock>,
    /// Internode transport (a `PeerManager` in production).
    transport: Arc<dyn AccordTransport>,
    /// Applier for the coordinator's own replica (its self-send is unreachable).
    applier: Arc<dyn StorageApplier>,
    /// Per-key replica resolution (wraps `WritePath` + schema in production).
    resolve: ReplicaResolver,
    /// The coordinator node's own Accord state machine. When the coordinator is
    /// itself a replica for a transaction's keys (the common case), the driver
    /// processes its OWN PreAccept against this state — casting a real vote with
    /// real deps — instead of dialing itself over the network (a node is never in
    /// its own peer map, so that self-send fails "unknown peer"). `None` falls
    /// back to remote votes only (used by tests with an external coordinator).
    local_accord_state: Option<crate::accord::handlers::AccordState>,
    /// Production creates the committer before cluster formation publishes the
    /// local state, so retain the slot and load it when each transaction runs.
    local_accord_state_slot: Option<crate::accord::handlers::AccordStateSlot>,
    /// Replicates a table-level tombstone to every serving node (see
    /// [`AllServingMarkerWriter`]). `None` refuses such a commit loudly rather
    /// than routing it through per-key Accord, which would reach only the
    /// reserved key's RF replica set.
    marker_writer: Option<Arc<dyn AllServingMarkerWriter>>,
}

impl AccordTransactionCommitter {
    pub fn new(
        node_id: u64,
        clock: Arc<HybridLogicalClock>,
        transport: Arc<dyn AccordTransport>,
        applier: Arc<dyn StorageApplier>,
        resolve: ReplicaResolver,
    ) -> Self {
        Self {
            node_id,
            clock,
            transport,
            applier,
            resolve,
            local_accord_state: None,
            local_accord_state_slot: None,
            marker_writer: None,
        }
    }

    /// Wire the all-serving-node writer used for table-level tombstones
    /// (`TRUNCATE`). Production passes a writer over the live `WritePath`; left
    /// unwired, a commit that contains a tombstone is refused loudly.
    pub fn with_marker_writer(mut self, writer: Arc<dyn AllServingMarkerWriter>) -> Self {
        self.marker_writer = Some(writer);
        self
    }

    /// Wire the coordinator node's own Accord state machine so a
    /// replica-coordinator votes on its own PreAccept locally (real deps) instead
    /// of an unreachable self-send. Production passes the node's shared
    /// `AccordState`; without it, an RF=1 (sole-replica) transaction cannot reach
    /// quorum.
    pub fn with_local_accord_state(mut self, state: crate::accord::handlers::AccordState) -> Self {
        self.local_accord_state = Some(state);
        self
    }

    /// Wire the coordinator node's own Accord state from a shared
    /// [`AccordStateSlot`](crate::accord::handlers::AccordStateSlot). The
    /// controller fills the slot with this node's live `AccordState` during
    /// cluster formation; because the session layer constructs the committer
    /// before formation, the slot is loaded when each transaction runs. An
    /// empty slot leaves the committer on remote-only votes — correct when the
    /// node is not itself a replica.
    pub fn with_local_accord_state_slot(
        mut self,
        slot: &crate::accord::handlers::AccordStateSlot,
    ) -> Self {
        self.local_accord_state_slot = Some(slot.clone());
        self
    }

    /// Replicate each table-level tombstone to EVERY node that serves the table,
    /// requiring all of them to acknowledge (`ConsistencyLevel::All`).
    ///
    /// Fail loud: an unwired writer, or any node that does not acknowledge,
    /// refuses the commit — an unconfirmed truncate would leave that node
    /// serving the truncated rows (resurrection). There is deliberately no
    /// quorum fallback and no hint: a truncate is not eventually consistent.
    async fn replicate_tombstones_all_nodes(
        &self,
        tombstones: &[TransactionWrite],
    ) -> Result<(), CommitError> {
        let writer = self.marker_writer.as_ref().ok_or_else(|| CommitError {
            reason: "cluster committer has no all-serving marker writer; refusing to route a \
                     TRUNCATE tombstone through per-key Accord (it would reach only the \
                     reserved key's RF replica set, resurrecting the rows on every other node)"
                .to_string(),
        })?;
        for write in tombstones {
            writer
                .write_marker_to_all_serving_nodes(&write.mutation)
                .await
                .map_err(|reason| CommitError {
                    reason: format!(
                        "TRUNCATE was not acknowledged by every serving node; refusing rather \
                         than truncating a subset: {reason}"
                    ),
                })?;
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionCommitter for AccordTransactionCommitter {
    fn register_postgres_mvcc_observer(
        &self,
        observer: Arc<dyn ferrosa_storage::accord::PostgresMvccApplyObserver>,
    ) -> Result<(), CommitError> {
        self.applier
            .register_postgres_mvcc_observer(observer.clone())
            .map_err(|reason| CommitError { reason })?;
        if let Some(slot) = &self.local_accord_state_slot {
            slot.register_postgres_mvcc_observer(observer.clone())
                .map_err(|reason| CommitError { reason })?;
        } else if let Some(state) = &self.local_accord_state {
            state
                .lock()
                .register_postgres_mvcc_observer(observer)
                .map_err(|reason| CommitError { reason })?;
        }
        Ok(())
    }

    async fn commit(&self, writes: Vec<TransactionWrite>) -> Result<CommitOutcome, CommitError> {
        // BEGIN; COMMIT; with no DML is a no-op — never drive Accord for nothing.
        if writes.is_empty() {
            return Ok(CommitOutcome::Committed);
        }

        drive_accord(self, writes, ReadPredicate::Always, None)
            .await
            .map_err(|error| CommitError {
                reason: error.to_string(),
            })?;
        Ok(CommitOutcome::Committed)
    }

    async fn begin_postgres_snapshot(
        &self,
        keyspace: &str,
    ) -> Result<ferrosa_common::accord::Timestamp, CommitError> {
        use ferrosa_storage::accord::conflict_index::POSTGRES_TRANSACTION_BARRIER_KEY;

        drive_accord(
            self,
            vec![TransactionWrite {
                keyspace: keyspace.to_string(),
                key: POSTGRES_TRANSACTION_BARRIER_KEY.to_vec(),
                mutation: Vec::new(),
            }],
            ReadPredicate::SnapshotBarrier,
            None,
        )
        .await
        .map_err(|error| CommitError {
            reason: error.to_string(),
        })
    }

    async fn validate_postgres_snapshot(
        &self,
        keyspace: &str,
        snapshot: ferrosa_common::accord::Timestamp,
    ) -> Result<bool, CommitError> {
        use ferrosa_storage::accord::conflict_index::POSTGRES_TRANSACTION_BARRIER_KEY;

        match drive_accord(
            self,
            vec![TransactionWrite {
                keyspace: keyspace.to_string(),
                key: POSTGRES_TRANSACTION_BARRIER_KEY.to_vec(),
                mutation: Vec::new(),
            }],
            ReadPredicate::SnapshotBarrier,
            Some(snapshot),
        )
        .await
        {
            Ok(_) => Ok(true),
            Err(AccordDriverError::SnapshotStale) => Ok(false),
            Err(error) => Err(CommitError {
                reason: error.to_string(),
            }),
        }
    }

    async fn commit_postgres(
        &self,
        keyspace: &str,
        mut writes: Vec<TransactionWrite>,
        _tables: Vec<String>,
        snapshot: ferrosa_common::accord::Timestamp,
    ) -> Result<CommitOutcome, CommitError> {
        use ferrosa_storage::accord::conflict_index::{
            POSTGRES_TRANSACTION_BARRIER_KEY, POSTGRES_TRANSACTION_MARKER_KEY,
        };
        use ferrosa_storage::table_tombstone;

        // A table-level tombstone (TRUNCATE) is ONE reserved partition key. The
        // per-key Accord commit below would route it to that key's RF replica
        // set — a proper subset of the ring whenever RF < node count — leaving
        // the nodes outside it serving the truncated rows (resurrection). So a
        // tombstone is split out and replicated to EVERY serving node at CL=ALL
        // instead, failing loud if any node does not acknowledge. The reserved
        // key's placement never decides a truncate's scope again.
        let tombstone_key = table_tombstone::table_tombstone_key();
        let (tombstones, ordinary): (Vec<TransactionWrite>, Vec<TransactionWrite>) = writes
            .into_iter()
            .partition(|w| w.key == tombstone_key.key.as_bytes());
        if !tombstones.is_empty() {
            self.replicate_tombstones_all_nodes(&tombstones).await?;
        }
        writes = ordinary;

        // The barrier key orders all BEGIN/COMMIT barriers. A second commit-only
        // marker advances only for data transaction completions, so a BEGIN after
        // our snapshot doesn't invalidate it. Empty mutation bytes participate in
        // Accord conflicts and Apply finalization, but the storage applier drops
        // them before applying data mutations.
        writes.push(TransactionWrite {
            keyspace: keyspace.to_string(),
            key: POSTGRES_TRANSACTION_BARRIER_KEY.to_vec(),
            mutation: Vec::new(),
        });
        writes.push(TransactionWrite {
            keyspace: keyspace.to_string(),
            key: POSTGRES_TRANSACTION_MARKER_KEY.to_vec(),
            mutation: Vec::new(),
        });

        match drive_accord(self, writes, ReadPredicate::Always, Some(snapshot)).await {
            Ok(_) => Ok(CommitOutcome::Committed),
            Err(AccordDriverError::SnapshotStale) => Ok(CommitOutcome::Aborted {
                reason: "PostgreSQL transaction snapshot is stale".to_string(),
            }),
            Err(error) => Err(CommitError {
                reason: error.to_string(),
            }),
        }
    }
}

async fn drive_accord(
    committer: &AccordTransactionCommitter,
    mut writes: Vec<TransactionWrite>,
    predicate: ReadPredicate,
    snapshot_ts: Option<ferrosa_common::accord::Timestamp>,
) -> Result<ferrosa_common::accord::Timestamp, AccordDriverError> {
    if writes.is_empty() {
        return Err(AccordDriverError::Network(
            "Accord transaction requires at least one conflict key".to_string(),
        ));
    }

    // 1. Resolve each key's replicas; fail loud on an unplaceable key (never
    //    commit a write to a guessed/empty replica set).
    let profile = std::env::var_os("FERROSA_PG_COMMIT_PROFILE").is_some();
    let t_resolve = profile.then(std::time::Instant::now);
    let mut replica_union: BTreeSet<Uuid> = BTreeSet::new();
    let mut per_key: HashMap<Vec<u8>, Vec<Uuid>> = HashMap::new();
    for w in &writes {
        let replicas = (committer.resolve)(&w.keyspace, &w.key).ok_or_else(|| {
            AccordDriverError::Network(format!(
                "no replicas resolved for a key in keyspace '{}' (cluster mode required)",
                w.keyspace
            ))
        })?;
        if replicas.is_empty() {
            return Err(AccordDriverError::Network(format!(
                "empty replica set for a key in keyspace '{}'",
                w.keyspace
            )));
        }
        for r in &replicas {
            replica_union.insert(*r);
        }
        per_key.insert(w.key.clone(), replicas);
    }
    let replica_ids: Vec<Uuid> = replica_union.into_iter().collect();
    let resolve_ns = t_resolve.map(|t| t.elapsed().as_nanos() as u64);

    // 2. Build the write-set + the per-key participant resolver for the driver.
    let t_write_set = profile.then(std::time::Instant::now);
    // Stage the payloads in local temp storage when the write-set is large enough to
    // be the commit's memory problem, so the coordinator never holds ~1.1M encoded
    // mutations resident. The KEYS stay in memory — Accord orders conflicts on them
    // and the per-shard participant set is derived from them — while the payload
    // bulk moves to a temp file and is read back on demand at Apply. Small
    // write-sets stay wholly resident (no filesystem touch).
    let payload_bytes: u64 = writes.iter().map(|w| w.mutation.len() as u64).sum();
    let write_blobs =
        if ferrosa_storage::write_set_spill::WriteSetSpill::should_stage(payload_bytes) {
            let reservation = ferrosa_storage::write_set_spill::reserve_write_set_stage()
                .map_err(|e| AccordDriverError::Codec(format!("write-set spill: {e}")))?;
            let mut blobs: Vec<Vec<u8>> = writes
                .iter_mut()
                .map(|write| std::mem::take(&mut write.mutation))
                .collect();
            let staged =
                ferrosa_storage::write_set_spill::WriteSetSpill::stage(reservation, &mut blobs)
                    .map_err(|e| AccordDriverError::Codec(format!("write-set spill: {e}")))?;
            tracing::info!(
                entries = staged.len(),
                payload_bytes = staged.bytes(),
                resident_index_bytes = staged.resident_index_bytes(),
                "accord: staged the write-set payloads in local temp storage"
            );
            Some(Arc::new(staged))
        } else {
            None
        };
    let write_set: Vec<(Vec<u8>, Vec<u8>)> =
        writes.into_iter().map(|w| (w.key, w.mutation)).collect();
    let write_set_len_hint = write_set.len();
    let per_key = Arc::new(per_key);
    let pk = per_key.clone();
    let participant_resolver =
        move |k: &[u8]| -> Vec<Uuid> { pk.get(k).cloned().unwrap_or_default() };

    // 3. Drive one unconditional multi-key Accord transaction.
    let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
        committer.node_id,
        replica_ids,
        committer.transport.clone(),
        false,
        &committer.clock,
        write_set,
    )
    .with_per_key_replicas(Arc::new(participant_resolver))
    .with_local_applier(committer.applier.clone())
    .with_read_predicate(predicate);
    if let Some(blobs) = write_blobs {
        driver = driver.with_spilled_write_set(blobs);
    }
    if let Some(snapshot_ts) = snapshot_ts {
        driver = driver.with_postgres_snapshot(snapshot_ts);
    }

    // When the coordinator is itself a replica, let it vote on its own
    // PreAccept locally rather than dial itself (unreachable in its own peer
    // map). Without this a sole-replica transaction never reaches quorum.
    let local_accord_state = committer.local_accord_state.clone().or_else(|| {
        committer
            .local_accord_state_slot
            .as_ref()
            .and_then(|slot| slot.load_full())
    });
    if let Some(state) = local_accord_state {
        driver = driver.with_local_accord_state(state);
    }

    let write_set_ns = t_write_set.map(|t| t.elapsed().as_nanos() as u64);
    let write_set_len = write_set_len_hint;
    let t_run = profile.then(std::time::Instant::now);
    let result = match driver.run_transaction().await {
        Ok((timestamp, _)) => Ok(timestamp),
        // A general transaction is unconditional (Always mode), so a condition
        // abort should not arise — map it cleanly if it ever does.
        // Quorum/network/codec failures: the commit did not reach a decision —
        // surface as Err so the front-end never acks an uncommitted transaction.
        Err(e) => Err(e),
    };
    if let (Some(t_run), Some(resolve_ns), Some(write_set_ns)) = (t_run, resolve_ns, write_set_ns) {
        tracing::info!(
            keys = write_set_len,
            resolve_ms = resolve_ns as f64 / 1_000_000.0,
            write_set_ms = write_set_ns as f64 / 1_000_000.0,
            driver_ms = t_run.elapsed().as_millis() as u64,
            "drive_accord attribution"
        );
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use ferrosa_net::error::NetError;
    use ferrosa_net::message::Message;

    use crate::accord::apply::{ApplyError, ApplyMutation};

    /// Records every applied mutation's data, so a test can assert which keys' writes landed.
    struct RecordingApplier {
        applied: Mutex<Vec<Vec<u8>>>,
        registered_observers: Mutex<usize>,
    }
    impl RecordingApplier {
        fn new() -> Self {
            Self {
                applied: Mutex::new(Vec::new()),
                registered_observers: Mutex::new(0),
            }
        }
        fn applied_data(&self) -> Vec<Vec<u8>> {
            self.applied.lock().expect("applier mutex").clone()
        }

        fn registered_observer_count(&self) -> usize {
            *self
                .registered_observers
                .lock()
                .expect("observer count mutex")
        }
    }
    impl StorageApplier for RecordingApplier {
        fn register_postgres_mvcc_observer(
            &self,
            _observer: Arc<dyn ferrosa_storage::accord::PostgresMvccApplyObserver>,
        ) -> Result<(), String> {
            *self
                .registered_observers
                .lock()
                .expect("observer count mutex") += 1;
            Ok(())
        }

        fn apply(
            &self,
            _txn_id: ferrosa_common::accord::TxnId,
            mutation: ApplyMutation,
        ) -> Result<(), ApplyError> {
            self.applied
                .lock()
                .expect("applier mutex")
                .push(mutation.data);
            Ok(())
        }
    }

    struct TestPostgresObserver;

    impl ferrosa_storage::accord::PostgresMvccApplyObserver for TestPostgresObserver {
        fn prepare_postgres_apply(
            &self,
            _txn_id: ferrosa_common::accord::TxnId,
            _t: ferrosa_common::accord::Timestamp,
            _metadata: &[Vec<u8>],
        ) -> Result<(), String> {
            Ok(())
        }

        fn on_postgres_apply(
            &self,
            _txn_id: ferrosa_common::accord::TxnId,
            _t: ferrosa_common::accord::Timestamp,
            _metadata: &[Vec<u8>],
        ) -> Result<(), String> {
            Ok(())
        }
    }

    /// Transport with no reachable peers — used when the coordinator is the sole
    /// replica (every send is a self-send the driver never makes).
    struct NoPeersTransport;
    #[async_trait]
    impl AccordTransport for NoPeersTransport {
        async fn send(
            &self,
            _host: Uuid,
            _msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            Err(NetError::Timeout("no peers".into()))
        }
    }

    fn write(ks: &str, key: &[u8], mutation: &[u8]) -> TransactionWrite {
        TransactionWrite {
            keyspace: ks.to_string(),
            key: key.to_vec(),
            mutation: mutation.to_vec(),
        }
    }

    fn committer_with(host: Uuid, applier: Arc<RecordingApplier>) -> AccordTransactionCommitter {
        let node_id = u64::from_be_bytes(host.as_bytes()[..8].try_into().expect("uuid 16 bytes"));
        let clock = Arc::new(HybridLogicalClock::new(node_id, 0));
        // Sole replica = the coordinator itself, for every key.
        let resolve: ReplicaResolver = Arc::new(move |_ks: &str, _key: &[u8]| Some(vec![host]));
        AccordTransactionCommitter::new(
            node_id,
            clock,
            Arc::new(NoPeersTransport),
            applier,
            resolve,
        )
    }

    #[tokio::test]
    async fn empty_write_set_commits_without_driving_accord() {
        let applier = Arc::new(RecordingApplier::new());
        let committer = committer_with(Uuid::from_u128(1), applier.clone());

        let outcome = committer.commit(Vec::new()).await.expect("empty commit");

        assert_eq!(outcome, CommitOutcome::Committed);
        assert!(
            applier.applied_data().is_empty(),
            "an empty transaction must not apply anything"
        );
    }

    fn node_id_of(u: Uuid) -> u64 {
        u64::from_be_bytes(u.as_bytes()[..8].try_into().expect("uuid 16 bytes"))
    }

    /// One real cluster node: its state-machine handler + recording applier.
    fn make_node(
        seed: u128,
    ) -> (
        Uuid,
        Arc<crate::accord::handlers::AccordHandler>,
        Arc<RecordingApplier>,
    ) {
        use crate::accord::handlers::{AccordHandler, AccordState};
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;
        let host_id = Uuid::from_u128(seed);
        let nid = node_id_of(host_id);
        let applier = Arc::new(RecordingApplier::new());
        let sm =
            AccordStateMachine::with_applier(nid, Arc::new(MockSyncWriter::new()), applier.clone());
        let state: AccordState = Arc::new(parking_lot::Mutex::new(sm));
        (host_id, Arc::new(AccordHandler::new(state, nid)), applier)
    }

    /// Routes each Accord message to the addressed node's real handler.
    struct RoutingTransport {
        nodes: HashMap<Uuid, Arc<crate::accord::handlers::AccordHandler>>,
    }
    #[async_trait]
    impl AccordTransport for RoutingTransport {
        async fn send(
            &self,
            host: Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
            let handler = self
                .nodes
                .get(&host)
                .ok_or_else(|| NetError::Timeout("unknown peer".into()))?;
            let peer: PeerId = (host, "127.0.0.1:0".parse().expect("addr"));
            handler
                .handle(peer, msg)
                .await
                .ok_or_else(|| NetError::Timeout("no response".into()))
        }
    }

    #[tokio::test]
    async fn commits_multi_key_write_set_across_shards_and_applies_every_key() {
        // Two shards (one RF=1 node each), external coordinator. The committer
        // resolves key_a → shard A, key_b → shard B and drives ONE unconditional
        // Accord transaction; each shard must apply its own key — the committer
        // turns a buffered write-set into a real cross-shard commit end to end.
        let (ha, handler_a, applier_a) = make_node(0xA);
        let (hb, handler_b, applier_b) = make_node(0xB);
        let mut nodes = HashMap::new();
        nodes.insert(ha, handler_a);
        nodes.insert(hb, handler_b);
        let transport: Arc<dyn AccordTransport> = Arc::new(RoutingTransport { nodes });

        let coord_id = 999_999u64; // external coordinator (matches no replica)
        let clock = Arc::new(HybridLogicalClock::new(coord_id, 0));
        let resolve: ReplicaResolver = Arc::new(move |_ks: &str, key: &[u8]| {
            if key == b"acct_a" {
                Some(vec![ha])
            } else {
                Some(vec![hb])
            }
        });
        let committer = AccordTransactionCommitter::new(
            coord_id,
            clock,
            transport,
            Arc::new(RecordingApplier::new()), // coordinator is not a replica
            resolve,
        );

        let writes = vec![
            write("ks", b"acct_a", b"row_a"),
            write("ks", b"acct_b", b"row_b"),
        ];
        let outcome = committer.commit(writes).await.expect("commit");

        assert_eq!(outcome, CommitOutcome::Committed);
        assert_eq!(
            applier_a.applied_data(),
            vec![b"row_a".to_vec()],
            "shard A must apply its key"
        );
        assert_eq!(
            applier_b.applied_data(),
            vec![b"row_b".to_vec()],
            "shard B must apply its key"
        );
    }

    #[tokio::test]
    async fn commits_single_key_when_coordinator_is_sole_replica() {
        // The request-serving node IS the (sole) replica for the key — the normal
        // production case. Its own PreAccept must be voted locally, never sent to
        // itself (a node is not in its own peer map), so the transaction reaches
        // quorum and applies. This reproduces the deployed "Accord quorum
        // unavailable" failure at the committer boundary.
        use crate::accord::handlers::AccordState;
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let host = Uuid::from_u128(0xC0);
        let node_id = node_id_of(host);
        let clock = Arc::new(HybridLogicalClock::new(node_id, 0));

        // The coordinator's own Accord state machine. It drives BOTH the local
        // self-vote AND its own Commit → Apply (a node is never in its own peer
        // map, so its self-addressed Apply RPC is unreachable). The SM's apply
        // engine is the recording applier, so the coordinator's write is applied —
        // and the SM advances to Applied — through the same path a remote replica
        // takes.
        let applier = Arc::new(RecordingApplier::new());
        let local_state: AccordState =
            Arc::new(parking_lot::Mutex::new(AccordStateMachine::with_applier(
                node_id,
                Arc::new(MockSyncWriter::new()),
                applier.clone(),
            )));

        // Sole replica = the coordinator; NoPeersTransport => self is unreachable
        // over the network, exactly like the production PeerManager.
        let resolve: ReplicaResolver = Arc::new(move |_ks: &str, _key: &[u8]| Some(vec![host]));
        let committer = AccordTransactionCommitter::new(
            node_id,
            clock,
            Arc::new(NoPeersTransport),
            applier.clone(),
            resolve,
        )
        .with_local_accord_state(local_state);

        let outcome = committer
            .commit(vec![write("ks", b"k", b"v")])
            .await
            .expect("sole-replica commit must succeed");

        assert_eq!(outcome, CommitOutcome::Committed);
        assert_eq!(
            applier.applied_data(),
            vec![b"v".to_vec()],
            "the coordinator (sole replica) must apply its own write"
        );
    }

    /// END-TO-END rebind (t_813caf39): a `BEGIN..COMMIT` list-append cell flagged
    /// with the transient rebind bit must persist a path bound to the AGREED
    /// Accord execution timestamp — not the coordinator materialize clock baked in
    /// at `build_transaction_write`. This proves the 0x4000 flag survives the whole
    /// committer → PreAccept/Commit/Apply → `EngineStorageApplier` path against a
    /// REAL storage engine, closing the gap between the isolated apply unit test
    /// (which proves the applier rebinds) and the live commit path (which must
    /// deliver a flagged, un-mangled payload to that applier).
    #[tokio::test]
    async fn sole_replica_commit_rebinds_flagged_list_path_end_to_end() {
        use crate::accord::apply::EngineStorageApplier;
        use crate::accord::handlers::AccordState;
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        use ferrosa_common::{accord_list_cell_path, list_path_element_seq};
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;
        use ferrosa_storage::{
            Mutation, StorageEngine, StorageEngineConfig, TableId, CELL_REBIND_LIST_PATH_FLAG,
        };

        // Real engine + a table to persist the row into.
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap(),
        );
        engine
            .register_table(TableSchema {
                keyspace: "ks".to_string(),
                table: "t".to_string(),
                key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "v".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                }],
                extensions: Default::default(),
            })
            .unwrap();
        let engine_applier: Arc<dyn StorageApplier> =
            Arc::new(EngineStorageApplier::new(engine.clone()));

        // A flagged list-append cell whose path carries an OBVIOUSLY WRONG
        // coordinator-clock time (t.time = 1). If the rebind is live end-to-end,
        // apply overwrites it with the agreed execution ts (~now); if a wiring gap
        // drops the flag, this ancient path survives.
        let element_seq = 0u16;
        let coord_path = accord_list_cell_path(
            &ferrosa_common::accord::Timestamp {
                epoch: 0,
                time: 1,
                seq: 0,
                node: 0,
            },
            element_seq,
        );
        let key = DecoratedKey::new(PartitionKey::new(vec![0, 0, 0, 5]));
        let row = Row {
            clustering: vec![],
            cells: vec![(
                CELL_REBIND_LIST_PATH_FLAG, // storage col 0, flagged for rebind
                CellValue::live(b"L".to_vec(), 1).with_path(coord_path.clone()),
            )],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        };
        let m = Mutation::new("ks".into(), "t".into(), key.clone(), vec![row], 1);
        let mut buf = vec![0u8; m.serialized_size()];
        m.serialize_into(&mut buf);

        // Sole-replica committer wired to the REAL engine applier on both the
        // committer boundary and the coordinator's own state machine.
        let host = Uuid::from_u128(0xD0);
        let node_id = node_id_of(host);
        let clock = Arc::new(HybridLogicalClock::new(node_id, 0));
        let local_state: AccordState =
            Arc::new(parking_lot::Mutex::new(AccordStateMachine::with_applier(
                node_id,
                Arc::new(MockSyncWriter::new()),
                engine_applier.clone(),
            )));
        let resolve: ReplicaResolver = Arc::new(move |_ks: &str, _key: &[u8]| Some(vec![host]));
        let committer = AccordTransactionCommitter::new(
            node_id,
            clock,
            Arc::new(NoPeersTransport),
            engine_applier.clone(),
            resolve,
        )
        .with_local_accord_state(local_state);

        let outcome = committer
            .commit(vec![write("ks", &[0, 0, 0, 5], &buf)])
            .await
            .expect("sole-replica list-append commit must succeed");
        assert_eq!(outcome, CommitOutcome::Committed);

        // Read back the persisted cell and inspect its path.
        let partition = engine
            .read(&TableId::new("ks", "t"), &key)
            .unwrap()
            .expect("the committed row must be persisted");
        let (idx, cell) = partition.rows[0]
            .cells
            .iter()
            .find(|(_, c)| c.path.is_some())
            .expect("the persisted list cell must have a path");

        assert_eq!(
            *idx & 0xC000,
            0,
            "stored column index carries no transient flag"
        );
        assert_ne!(
            cell.path.as_deref(),
            Some(coord_path.as_slice()),
            "REBIND NOT ACTIVE end-to-end: the coordinator-clock path (t.time=1) survived the \
             commit path — the 0x4000 flag was dropped between build_transaction_write and apply"
        );
        assert_eq!(
            list_path_element_seq(cell.path.as_deref().unwrap()),
            Some(element_seq),
            "the rebound path preserves the element_seq"
        );
        assert_ne!(
            cell.timestamp, 1,
            "the cell timestamp is restamped to the agreed execution ts, confirming apply ran"
        );
    }

    #[tokio::test]
    async fn sole_replica_commit_uses_local_state_from_populated_slot() {
        // The session layer wires the committer from an AccordStateSlot the
        // controller fills at formation. A populated slot must enable the
        // coordinator's local self-vote exactly as with_local_accord_state does,
        // so an RF=1 (sole-replica) transaction commits.
        use crate::accord::handlers::{empty_accord_state_slot, publish_accord_state, AccordState};
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let host = Uuid::from_u128(0xC1);
        let node_id = node_id_of(host);
        let clock = Arc::new(HybridLogicalClock::new(node_id, 0));

        let applier = Arc::new(RecordingApplier::new());
        let slot = empty_accord_state_slot();
        // SM apply engine = the recording applier, so the coordinator's own
        // Commit → Apply (driven through the SM) is observable.
        let state: AccordState =
            Arc::new(parking_lot::Mutex::new(AccordStateMachine::with_applier(
                node_id,
                Arc::new(MockSyncWriter::new()),
                applier.clone(),
            )));
        let resolve: ReplicaResolver = Arc::new(move |_ks: &str, _key: &[u8]| Some(vec![host]));
        let committer = AccordTransactionCommitter::new(
            node_id,
            clock,
            Arc::new(NoPeersTransport),
            applier.clone(),
            resolve,
        )
        .with_local_accord_state_slot(&slot);
        // Cluster formation publishes this state after the session has already
        // constructed its long-lived committer.
        publish_accord_state(&slot, state).expect("publish local state");

        let outcome = committer
            .commit(vec![write("ks", b"k", b"v")])
            .await
            .expect("populated slot must enable the sole-replica commit");

        assert_eq!(outcome, CommitOutcome::Committed);
        assert_eq!(
            applier.applied_data(),
            vec![b"v".to_vec()],
            "the coordinator must apply its own write when the slot is populated"
        );
    }

    #[tokio::test]
    async fn postgres_mvcc_observer_reaches_state_published_after_committer_setup() {
        use crate::accord::handlers::{empty_accord_state_slot, publish_accord_state, AccordState};
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let host = Uuid::from_u128(0xC3);
        let node_id = node_id_of(host);
        let coordinator_applier = Arc::new(RecordingApplier::new());
        let replica_applier = Arc::new(RecordingApplier::new());
        let slot = empty_accord_state_slot();
        let resolve: ReplicaResolver = Arc::new(move |_ks: &str, _key: &[u8]| Some(vec![host]));
        let committer = AccordTransactionCommitter::new(
            node_id,
            Arc::new(HybridLogicalClock::new(node_id, 0)),
            Arc::new(NoPeersTransport),
            coordinator_applier,
            resolve,
        )
        .with_local_accord_state_slot(&slot);
        committer
            .register_postgres_mvcc_observer(Arc::new(TestPostgresObserver))
            .expect("register PostgreSQL MVCC observer before formation");

        let state: AccordState =
            Arc::new(parking_lot::Mutex::new(AccordStateMachine::with_applier(
                node_id,
                Arc::new(MockSyncWriter::new()),
                replica_applier.clone(),
            )));
        publish_accord_state(&slot, state).expect("publish state with registered observer");

        assert_eq!(
            replica_applier.registered_observer_count(),
            1,
            "publishing replica state must install observers before it handles remote Apply"
        );

        committer
            .commit(vec![write("ks", b"k", b"v")])
            .await
            .expect("commit after the local Accord state is published");

        assert_eq!(
            replica_applier.registered_observer_count(),
            1,
            "the published replica's storage applier must receive the PostgreSQL observer"
        );
    }

    #[tokio::test]
    async fn coordinator_marks_its_own_committed_write_applied_in_its_state_machine() {
        // The read-visibility fix: a coordinator that commits a write must drive its
        // OWN state machine to Applied — not just persist to storage. Otherwise a
        // later linearizable (SERIAL) read served by this node dep-waits forever on
        // a conflict that is durably written but never marked Applied in the SM.
        //
        // After a sole-replica commit of key `k`, the SM must report NO unapplied
        // conflicts for `k` at any later timestamp (the committed txn is Applied).
        use crate::accord::handlers::AccordState;
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let host = Uuid::from_u128(0xC2);
        let node_id = node_id_of(host);
        let clock = Arc::new(HybridLogicalClock::new(node_id, 0));
        let applier = Arc::new(RecordingApplier::new());
        let local_state: AccordState =
            Arc::new(parking_lot::Mutex::new(AccordStateMachine::with_applier(
                node_id,
                Arc::new(MockSyncWriter::new()),
                applier.clone(),
            )));
        let resolve: ReplicaResolver = Arc::new(move |_ks: &str, _key: &[u8]| Some(vec![host]));
        let committer = AccordTransactionCommitter::new(
            node_id,
            clock.clone(),
            Arc::new(NoPeersTransport),
            applier,
            resolve,
        )
        .with_local_accord_state(local_state.clone());

        committer
            .commit(vec![write("ks", b"k", b"v")])
            .await
            .expect("sole-replica commit must succeed");

        let later_t = clock.now();
        let pending = local_state
            .lock()
            .unapplied_conflicts_before(b"k", &later_t);
        assert!(
            pending.is_empty(),
            "coordinator's own committed write must be Applied in its SM (no pending \
             conflicts), else a linearizable read served by this node dep-waits forever; \
             pending={pending:?}"
        );
    }

    #[tokio::test]
    async fn sole_replica_commit_fails_without_local_state_slot() {
        // The empty slot (standalone / pre-formation) leaves no local self-vote:
        // the sole replica is self, which NoPeersTransport cannot reach — exactly
        // the deployed "Accord quorum unavailable" failure. This proves the slot
        // is the causal factor, not incidental to the setup.
        use crate::accord::handlers::empty_accord_state_slot;

        let host = Uuid::from_u128(0xC2);
        let node_id = node_id_of(host);
        let clock = Arc::new(HybridLogicalClock::new(node_id, 0));

        let slot = empty_accord_state_slot();
        let applier = Arc::new(RecordingApplier::new());
        let resolve: ReplicaResolver = Arc::new(move |_ks: &str, _key: &[u8]| Some(vec![host]));
        let committer = AccordTransactionCommitter::new(
            node_id,
            clock,
            Arc::new(NoPeersTransport),
            applier.clone(),
            resolve,
        )
        .with_local_accord_state_slot(&slot);

        let err = committer
            .commit(vec![write("ks", b"k", b"v")])
            .await
            .expect_err("an empty slot must not reach quorum for a self-only replica");
        let reason = err.reason.to_lowercase();
        assert!(
            reason.contains("quorum") || reason.contains("unavailable"),
            "expected a quorum-unavailable failure, got: {}",
            err.reason
        );
        assert!(
            applier.applied_data().is_empty(),
            "nothing must be applied when the commit cannot reach a decision"
        );
    }

    #[tokio::test]
    async fn unplaceable_key_fails_loud() {
        // A key the resolver cannot place must abort the commit with an error —
        // never silently commit to a guessed/empty replica set.
        let applier = Arc::new(RecordingApplier::new());
        let node_id = 7u64;
        let clock = Arc::new(HybridLogicalClock::new(node_id, 0));
        let resolve: ReplicaResolver = Arc::new(|_ks: &str, _key: &[u8]| None);
        let committer = AccordTransactionCommitter::new(
            node_id,
            clock,
            Arc::new(NoPeersTransport),
            applier,
            resolve,
        );

        let err = committer
            .commit(vec![write("ks", b"k", b"v")])
            .await
            .expect_err("unplaceable key must fail loud");
        assert!(err.reason.contains("no replicas"), "got: {}", err.reason);
    }

    /// Records the marker bytes handed to the all-serving writer, and can be told
    /// to fail (a serving node that did not acknowledge).
    struct MarkerRecorder {
        seen: Mutex<Vec<Vec<u8>>>,
        result: Result<(), String>,
    }

    impl MarkerRecorder {
        fn ok() -> Self {
            Self {
                seen: Mutex::new(Vec::new()),
                result: Ok(()),
            }
        }

        fn failing(reason: &str) -> Self {
            Self {
                seen: Mutex::new(Vec::new()),
                result: Err(reason.to_string()),
            }
        }

        fn seen(&self) -> Vec<Vec<u8>> {
            self.seen.lock().expect("marker recorder mutex").clone()
        }
    }

    #[async_trait]
    impl AllServingMarkerWriter for MarkerRecorder {
        async fn write_marker_to_all_serving_nodes(
            &self,
            mutation_bytes: &[u8],
        ) -> Result<(), String> {
            self.seen
                .lock()
                .expect("marker recorder mutex")
                .push(mutation_bytes.to_vec());
            self.result.clone()
        }
    }

    fn tombstone_write() -> TransactionWrite {
        TransactionWrite {
            keyspace: "ks".to_string(),
            key: ferrosa_storage::table_tombstone::table_tombstone_key()
                .key
                .as_bytes()
                .to_vec(),
            mutation: b"marker-bytes".to_vec(),
        }
    }

    fn snapshot() -> ferrosa_common::accord::Timestamp {
        ferrosa_common::accord::Timestamp {
            epoch: 0,
            time: 1,
            seq: 0,
            node: 0,
        }
    }

    #[tokio::test]
    async fn table_tombstone_is_routed_to_all_serving_nodes_not_per_key_accord() {
        // A tombstone (TRUNCATE) must NOT travel the per-key Accord path: that
        // routes it by its token to the reserved key's RF replica set and misses
        // the rest of the ring (a node outside the set keeps serving the truncated
        // rows). It must go to the all-serving marker writer instead, whatever the
        // resolver would have answered for its token.
        let recorder = Arc::new(MarkerRecorder::ok());
        let resolver_keys: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let seen = resolver_keys.clone();
        let resolve: ReplicaResolver = Arc::new(move |_ks: &str, key: &[u8]| {
            seen.lock().expect("resolver mutex").push(key.to_vec());
            // An RF-subset that does NOT include every node.
            Some(vec![Uuid::from_u128(0xAA)])
        });
        let committer = AccordTransactionCommitter::new(
            7u64,
            Arc::new(HybridLogicalClock::new(7, 0)),
            Arc::new(NoPeersTransport),
            Arc::new(RecordingApplier::new()),
            resolve,
        )
        .with_marker_writer(recorder.clone());

        // Only the tombstone is in the write-set; the barrier keys still drive
        // Accord and (with no peers) fail, but the tombstone fan-out has already
        // run. The assertion is on the ROUTING, which is decided before Accord.
        let _ = committer
            .commit_postgres("ks", vec![tombstone_write()], vec![], snapshot())
            .await;

        assert_eq!(
            recorder.seen(),
            vec![b"marker-bytes".to_vec()],
            "the tombstone must be replicated to every serving node"
        );
        let keys = resolver_keys.lock().expect("resolver mutex").clone();
        assert!(
            !keys.contains(&tombstone_write().key),
            "the tombstone must NOT be routed by its token to the per-key RF replica set"
        );
    }

    #[tokio::test]
    async fn table_tombstone_commit_fails_loud_when_a_serving_node_does_not_acknowledge() {
        // An unacknowledged replica must REFUSE the truncate loudly — never a
        // silent degrade to quorum. An unconfirmed truncate is a lie.
        let recorder = Arc::new(MarkerRecorder::failing("node C did not acknowledge"));
        let resolve: ReplicaResolver =
            Arc::new(|_ks: &str, _key: &[u8]| Some(vec![Uuid::from_u128(0xAA)]));
        let committer = AccordTransactionCommitter::new(
            7u64,
            Arc::new(HybridLogicalClock::new(7, 0)),
            Arc::new(NoPeersTransport),
            Arc::new(RecordingApplier::new()),
            resolve,
        )
        .with_marker_writer(recorder);

        let err = committer
            .commit_postgres("ks", vec![tombstone_write()], vec![], snapshot())
            .await
            .expect_err("a truncate that not every serving node acknowledged must be refused");
        let reason = err.reason.to_lowercase();
        assert!(
            reason.contains("every serving node") || reason.contains("subset"),
            "the refusal must say the truncate did not reach every node, got: {}",
            err.reason
        );
    }

    #[tokio::test]
    async fn table_tombstone_commit_refuses_without_an_all_serving_writer() {
        // No all-serving writer wired: refuse the tombstone commit loudly rather
        // than let it fall through to per-key Accord (the RF-subset scope hole).
        let committer = committer_with(Uuid::from_u128(0xAA), Arc::new(RecordingApplier::new()));

        let err = committer
            .commit_postgres("ks", vec![tombstone_write()], vec![], snapshot())
            .await
            .expect_err("a tombstone with no all-serving writer must be refused");
        assert!(
            err.reason.to_lowercase().contains("all-serving"),
            "got: {}",
            err.reason
        );
    }
}
