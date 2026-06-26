//! Driver-level cross-shard atomicity e2e (t_afa3ee).
//!
//! Drives the REAL [`AccordCoordinatorDriver::new_multi`] over a routing
//! transport that delivers each protocol message to a real per-node
//! [`AccordHandler`]/[`AccordStateMachine`] and returns the real response. This
//! exercises the production multi-key path — PreAccept (the `AccordPreAcceptV2`
//! dependency union), per-shard quorum Commit, and per-replica `AccordApplyV2`
//! atomic apply — across genuinely distinct shards, which the `MockTransport`
//! unit tests (canned acks) and the `CrossShardCoordinator` oracle do not.
//!
//! Atomicity model (Accord, not 2PC): cross-shard atomicity is enforced at
//! **commit** — a shard that cannot reach its commit quorum aborts the whole
//! transaction, so no shard applies. Once committed, every shard applies the
//! transaction (recovery guarantees it). The tests assert both the commit-all →
//! apply-all path and the shard-down → abort-all (no partial apply) path.

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use uuid::Uuid;

    use crate::accord::wire::ReadPredicate;

    use ferrosa_common::accord::{HybridLogicalClock, TxnId};
    use ferrosa_net::codec::Lane;
    use ferrosa_net::error::NetError;
    use ferrosa_net::message::Message;
    use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;

    use crate::accord::apply::{ApplyError, ApplyMutation, StorageApplier};
    use crate::accord::coordinator::AccordCoordinatorDriver;
    use crate::accord::handlers::{AccordHandler, AccordState};
    use crate::accord::state_machine::AccordStateMachine;
    use crate::accord::transport::AccordTransport;

    /// The driver derives a node's `u64` id from the first 8 bytes of its UUID;
    /// mirror that so a node recognises messages addressed to it.
    fn node_id_of(u: Uuid) -> u64 {
        u64::from_be_bytes(u.as_bytes()[..8].try_into().expect("uuid is 16 bytes"))
    }

    /// Records the `data` bytes of every mutation applied on this node, so a test
    /// can assert which keys' writes landed (and that a failed txn landed none).
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
        fn apply(&self, _txn_id: TxnId, mutation: ApplyMutation) -> Result<(), ApplyError> {
            self.applied
                .lock()
                .expect("applier mutex")
                .push(mutation.data);
            Ok(())
        }
    }

    /// One node of the cluster: its shared state, RPC handler, recording applier.
    struct Node {
        host_id: Uuid,
        handler: Arc<AccordHandler>,
        applier: Arc<RecordingApplier>,
    }

    fn make_node(seed: u128) -> Node {
        let host_id = Uuid::from_u128(seed);
        let nid = node_id_of(host_id);
        let applier = Arc::new(RecordingApplier::new());
        let writer = Arc::new(MockSyncWriter::new());
        let sm = AccordStateMachine::with_applier(nid, writer, applier.clone());
        let state: AccordState = Arc::new(parking_lot::Mutex::new(sm));
        let handler = Arc::new(AccordHandler::new(state, nid));
        Node {
            host_id,
            handler,
            applier,
        }
    }

    /// Routes each Accord message to the addressed node's real handler and returns
    /// the real response. `down` host ids drop every message (a node that cannot
    /// participate), so the shard it backs cannot reach quorum.
    struct RoutingTransport {
        nodes: HashMap<Uuid, Arc<AccordHandler>>,
        down: HashSet<Uuid>,
        /// Number of `AccordRead` (read-vote) messages routed — lets a test prove
        /// the read-vote phase ran (or, for an unconditional txn, was skipped).
        read_votes: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl AccordTransport for RoutingTransport {
        async fn send(
            &self,
            host_id: Uuid,
            msg: Message,
            _lane: Lane,
        ) -> ferrosa_net::error::Result<Message> {
            if matches!(msg, Message::AccordRead(_)) {
                self.read_votes.fetch_add(1, Ordering::Relaxed);
            }
            if self.down.contains(&host_id) {
                return Err(NetError::Timeout("node down".into()));
            }
            let handler = self
                .nodes
                .get(&host_id)
                .ok_or_else(|| NetError::Timeout("unknown peer".into()))?;
            let peer: PeerId = (host_id, "127.0.0.1:0".parse().expect("addr"));
            handler
                .handle(peer, msg)
                .await
                .ok_or_else(|| NetError::Timeout("no response".into()))
        }
    }

    /// Build a 2-shard cluster (one RF=1 node per shard) + an external coordinator
    /// driver for a 2-key cross-shard transaction. `down` names host ids whose
    /// node drops all messages. Returns (driver, nodeA, nodeB).
    #[allow(clippy::too_many_arguments)]
    fn cross_shard_transfer(
        key_a: &[u8],
        mut_a: &[u8],
        key_b: &[u8],
        mut_b: &[u8],
        down: HashSet<Uuid>,
        predicate: ReadPredicate,
    ) -> (
        AccordCoordinatorDriver,
        Arc<RecordingApplier>,
        Arc<RecordingApplier>,
        Arc<AtomicUsize>,
    ) {
        let node_a = make_node(0xA);
        let node_b = make_node(0xB);
        let (ha, hb) = (node_a.host_id, node_b.host_id);

        let mut nodes = HashMap::new();
        nodes.insert(node_a.host_id, node_a.handler.clone());
        nodes.insert(node_b.host_id, node_b.handler.clone());
        let read_votes = Arc::new(AtomicUsize::new(0));
        let transport: Arc<dyn AccordTransport> = Arc::new(RoutingTransport {
            nodes,
            down,
            read_votes: read_votes.clone(),
        });

        // External coordinator (its id matches no replica → no self-loopback; every
        // message goes over the routing transport to a real handler).
        let coord_id = 999_999u64;
        let clock = HybridLogicalClock::new(coord_id, 0);
        let write_set = vec![
            (key_a.to_vec(), mut_a.to_vec()),
            (key_b.to_vec(), mut_b.to_vec()),
        ];

        // Per-key replica resolution: key_a lives on shard A (node_a), key_b on
        // shard B (node_b) — genuinely distinct shards.
        let ka = key_a.to_vec();
        let resolver = move |k: &[u8]| -> Vec<Uuid> {
            if k == ka.as_slice() {
                vec![ha]
            } else {
                vec![hb]
            }
        };

        let driver = AccordCoordinatorDriver::new_multi_with_transport(
            coord_id,
            vec![ha, hb],
            transport,
            false,
            &clock,
            write_set,
        )
        .with_per_key_replicas(Arc::new(resolver))
        .with_read_predicate(predicate);

        (driver, node_a.applier, node_b.applier, read_votes)
    }

    /// Happy path: a 2-key transaction across two shards commits, and BOTH shards
    /// apply their key's mutation — proving the real multi-key driver (PreAcceptV2
    /// union + per-shard quorum + per-replica AccordApplyV2) executes atomically
    /// across genuinely distinct shards.
    #[tokio::test]
    async fn cross_shard_txn_commits_and_applies_on_both_shards() {
        let (mut driver, applier_a, applier_b, _reads) = cross_shard_transfer(
            b"acct_a",
            b"row_a",
            b"acct_b",
            b"row_b",
            HashSet::new(),
            ReadPredicate::NotExists,
        );

        let result = driver.run_transaction().await;
        assert!(
            result.is_ok(),
            "cross-shard txn must commit when both shards are up: {result:?}"
        );

        assert_eq!(
            applier_a.applied_data(),
            vec![b"row_a".to_vec()],
            "shard A must apply exactly its own key's mutation"
        );
        assert_eq!(
            applier_b.applied_data(),
            vec![b"row_b".to_vec()],
            "shard B must apply exactly its own key's mutation"
        );
    }

    /// Unconditional transaction (`ReadPredicate::Always`): a plain `BEGIN/COMMIT`
    /// has no `IF` to evaluate, so the driver must SKIP the read-vote phase
    /// entirely and apply on every shard. This is the unlock for general
    /// multi-key SQL transactions (2a), which `NotExists`/`ReadRow` cannot serve.
    #[tokio::test]
    async fn unconditional_txn_skips_read_vote_and_applies_on_both_shards() {
        let (mut driver, applier_a, applier_b, read_votes) = cross_shard_transfer(
            b"acct_a",
            b"row_a",
            b"acct_b",
            b"row_b",
            HashSet::new(),
            ReadPredicate::Always,
        );

        let result = driver.run_transaction().await;
        assert!(
            result.is_ok(),
            "unconditional cross-shard txn must commit: {result:?}"
        );
        assert_eq!(
            read_votes.load(Ordering::Relaxed),
            0,
            "an unconditional (Always) txn must send NO AccordRead — the read-vote \
             phase is skipped"
        );
        assert_eq!(applier_a.applied_data(), vec![b"row_a".to_vec()]);
        assert_eq!(applier_b.applied_data(), vec![b"row_b".to_vec()]);
    }

    /// Abort-all: when one shard's only replica is down, the transaction cannot
    /// reach that shard's quorum and aborts — NEITHER shard applies (no partial
    /// cross-shard write).
    #[tokio::test]
    async fn cross_shard_txn_with_one_shard_down_applies_nothing() {
        let node_b_host = Uuid::from_u128(0xB);
        let down = HashSet::from([node_b_host]);
        let (mut driver, applier_a, applier_b, _reads) = cross_shard_transfer(
            b"acct_a",
            b"row_a",
            b"acct_b",
            b"row_b",
            down,
            ReadPredicate::NotExists,
        );

        let result = driver.run_transaction().await;
        assert!(
            result.is_err(),
            "txn must abort when shard B's only replica is down: {result:?}"
        );

        assert!(
            applier_a.applied_data().is_empty(),
            "shard A must NOT apply when the whole txn aborts (no partial cross-shard write)"
        );
        assert!(
            applier_b.applied_data().is_empty(),
            "shard B (down) must not apply"
        );
    }
}
