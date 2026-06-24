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
        }
    }
}

#[async_trait]
impl TransactionCommitter for AccordTransactionCommitter {
    async fn commit(&self, writes: Vec<TransactionWrite>) -> Result<CommitOutcome, CommitError> {
        // BEGIN; COMMIT; with no DML is a no-op — never drive Accord for nothing.
        if writes.is_empty() {
            return Ok(CommitOutcome::Committed);
        }

        // 1. Resolve each key's replicas; fail loud on an unplaceable key (never
        //    commit a write to a guessed/empty replica set).
        let mut replica_union: BTreeSet<Uuid> = BTreeSet::new();
        let mut per_key: HashMap<Vec<u8>, Vec<Uuid>> = HashMap::new();
        for w in &writes {
            let replicas = (self.resolve)(&w.keyspace, &w.key).ok_or_else(|| CommitError {
                reason: format!(
                    "no replicas resolved for a key in keyspace '{}' (cluster mode required)",
                    w.keyspace
                ),
            })?;
            if replicas.is_empty() {
                return Err(CommitError {
                    reason: format!("empty replica set for a key in keyspace '{}'", w.keyspace),
                });
            }
            for r in &replicas {
                replica_union.insert(*r);
            }
            per_key.insert(w.key.clone(), replicas);
        }
        let replica_ids: Vec<Uuid> = replica_union.into_iter().collect();

        // 2. Build the write-set + the per-key participant resolver for the driver.
        let write_set: Vec<(Vec<u8>, Vec<u8>)> =
            writes.into_iter().map(|w| (w.key, w.mutation)).collect();
        let per_key = Arc::new(per_key);
        let pk = per_key.clone();
        let participant_resolver =
            move |k: &[u8]| -> Vec<Uuid> { pk.get(k).cloned().unwrap_or_default() };

        // 3. Drive one unconditional multi-key Accord transaction.
        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            self.node_id,
            replica_ids,
            self.transport.clone(),
            false,
            &self.clock,
            write_set,
        )
        .with_per_key_replicas(Arc::new(participant_resolver))
        .with_local_applier(self.applier.clone())
        .with_read_predicate(ReadPredicate::Always);

        match driver.run_transaction().await {
            Ok(_) => Ok(CommitOutcome::Committed),
            // A general transaction is unconditional (Always mode), so a condition
            // abort should not arise — map it cleanly if it ever does.
            Err(AccordDriverError::ConditionNotMet { .. }) => Ok(CommitOutcome::Aborted {
                reason: "transaction condition not met".to_string(),
            }),
            // Quorum/network/codec failures: the commit did not reach a decision —
            // surface as Err so the front-end never acks an uncommitted transaction.
            Err(e) => Err(CommitError {
                reason: e.to_string(),
            }),
        }
    }
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
    }
    impl RecordingApplier {
        fn new() -> Self {
            Self {
                applied: Mutex::new(Vec::new()),
            }
        }
        fn applied_data(&self) -> Vec<Vec<u8>> {
            self.applied.lock().expect("applier mutex").clone()
        }
    }
    impl StorageApplier for RecordingApplier {
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
}
