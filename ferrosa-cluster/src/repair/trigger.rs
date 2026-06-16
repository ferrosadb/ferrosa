//! Cluster-backed [`RepairTrigger`] — the quarantine → anti-entropy-refill wire.
//!
//! When the storage-side `SelfHealController` quarantines a corrupt SSTable
//! generation, the rows in that generation are gone locally. The controller
//! calls a [`storage RepairTrigger`](ferrosa_storage::self_heal::RepairTrigger)
//! (a *port*, since storage sits below the cluster layer) to schedule a prompt,
//! targeted refill of exactly the quarantined token ranges from a healthy
//! replica — rather than waiting for the next full anti-entropy cycle.
//!
//! [`ClusterRepairTrigger`] is the real, cluster-layer implementation of that
//! port (FMEA #10). On `request_refill(table, ranges)` it, **per range**:
//!
//! 1. Verifies a **healthy** replica peer for that `(table, range)` via the same
//!    [`RepairProbe`] the [`ClusterRepairView`](super::cluster_view) posture gate
//!    uses — a peer that returns a non-empty Merkle digest holds readable,
//!    non-corrupt data we can refill from.
//! 2. **Immediately enqueues** (async, background) one targeted repair
//!    [`SessionExecutor::run_session`] over exactly that range against the
//!    verified-healthy peer — the storage controller never blocks on the refill
//!    (LOCKED DESIGN: serve now / quarantine now, repair in the background).
//!
//! Every scheduled refill bumps an observable counter
//! (`anti_entropy_refills_scheduled_total`); a range with no verified-healthy
//! peer bumps `anti_entropy_refills_no_source_total` and logs loudly — the
//! periodic repair cycle is the backstop (design Q3 / FMEA #10). Corruption is
//! therefore never silently dropped from observability.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ferrosa_net::task_pool::TaskPool;
use ferrosa_storage::self_heal::{RepairTrigger, TableKey};
use ferrosa_storage::TableId;

use super::cluster_view::{ClusterTopology, ProbeOutcome, RepairProbe};
use super::coordinator::SessionExecutor;

/// Resolves the repair [`SessionExecutor`] for the **current** ring.
///
/// The executor is rebuilt per refill rather than captured once: the production
/// executor ([`LocalRepairExecutor`](super::executor::LocalRepairExecutor)) is
/// constructed against a fixed per-peer remote map, which goes stale as ring
/// membership changes. Resolving it lazily (the same way the manual repair
/// endpoint and the scheduler do via `build_repair_executor`) keeps the trigger
/// pointed at live peers. `None` means the node is not ring-ready — the refill
/// is skipped loudly and the periodic cycle is the backstop (FMEA #10).
pub trait ExecutorProvider: Send + Sync {
    /// Build a session executor for the current ring, or `None` if not ready.
    fn current_executor(&self) -> Option<Arc<dyn SessionExecutor>>;
}

impl<F> ExecutorProvider for F
where
    F: Fn() -> Option<Arc<dyn SessionExecutor>> + Send + Sync,
{
    fn current_executor(&self) -> Option<Arc<dyn SessionExecutor>> {
        self()
    }
}

// ---------------------------------------------------------------------------
// Metrics (FMEA #7/#10 — a silent refill is a bug). Process-global counters in
// the crate's existing free-function style (see `coordinator/metrics.rs`).
// ---------------------------------------------------------------------------

static REFILLS_SCHEDULED: AtomicU64 = AtomicU64::new(0);
static REFILLS_NO_SOURCE: AtomicU64 = AtomicU64::new(0);

/// Targeted anti-entropy refills scheduled after a successful quarantine
/// (one per `(table, range)` for which a verified-healthy replica was found).
pub fn anti_entropy_refills_scheduled_total() -> u64 {
    REFILLS_SCHEDULED.load(Ordering::Relaxed)
}

/// Quarantined ranges for which **no** verified-healthy replica could be found
/// to refill from. Non-zero means quarantined data is awaiting the periodic
/// repair backstop — should alert.
pub fn anti_entropy_refills_no_source_total() -> u64 {
    REFILLS_NO_SOURCE.load(Ordering::Relaxed)
}

/// Render the `ferrosa_anti_entropy_refill_*` counters in Prometheus format.
pub fn render_prometheus() -> String {
    format!(
        "# HELP ferrosa_anti_entropy_refills_scheduled_total Targeted refills scheduled from a healthy replica after quarantine.\n\
         # TYPE ferrosa_anti_entropy_refills_scheduled_total counter\n\
         ferrosa_anti_entropy_refills_scheduled_total {}\n\
         # HELP ferrosa_anti_entropy_refills_no_source_total Quarantined ranges with no verified-healthy replica to refill from.\n\
         # TYPE ferrosa_anti_entropy_refills_no_source_total counter\n\
         ferrosa_anti_entropy_refills_no_source_total {}\n",
        REFILLS_SCHEDULED.load(Ordering::Relaxed),
        REFILLS_NO_SOURCE.load(Ordering::Relaxed),
    )
}

// ---------------------------------------------------------------------------
// ClusterRepairTrigger
// ---------------------------------------------------------------------------

/// Cluster-layer [`RepairTrigger`]: schedules a targeted anti-entropy refill of
/// quarantined ranges from a verified-healthy replica.
///
/// Holds the three ports it needs, all trait objects so the wiring is fully
/// unit-testable with no live ring / RPC:
/// - [`ClusterTopology`] — the table's replica peers (`(ring_id, host_id)`),
/// - [`RepairProbe`] — verifies a peer holds a non-empty, non-corrupt copy,
/// - [`ExecutorProvider`] — resolves the live [`SessionExecutor`] that runs one
///   targeted Merkle-diff-then-stream session.
///
/// On a real node the binary supplies the same `RingTopology` + `RpcRepairProbe`
/// the posture gate uses and a provider that rebuilds the production session
/// executor against the current ring; tests inject mocks.
pub struct ClusterRepairTrigger {
    topology: Arc<dyn ClusterTopology>,
    probe: Arc<dyn RepairProbe>,
    executor: Arc<dyn ExecutorProvider>,
}

impl ClusterRepairTrigger {
    /// Construct from the topology, probe, and executor-provider ports.
    pub fn new(
        topology: Arc<dyn ClusterTopology>,
        probe: Arc<dyn RepairProbe>,
        executor: Arc<dyn ExecutorProvider>,
    ) -> Self {
        Self {
            topology,
            probe,
            executor,
        }
    }

    /// Find the first verified-healthy replica peer for `(table, range)`, probing
    /// the table's replica peers in the topology's deterministic order. Returns
    /// the peer's `(ring_id, host_id)` on the first non-empty digest; `None` when
    /// no reachable peer verifies a non-empty copy (FMEA #1 / single-node).
    fn healthy_peer_for(&self, table: &TableId, range: (i64, i64)) -> Option<(u64, uuid::Uuid)> {
        let key = TableKey::new(table.keyspace(), table.table());
        let peers = self.topology.replica_peers(&key);
        for (ring_id, host_id) in peers {
            match self.probe.probe_digest(host_id, table, range) {
                ProbeOutcome::HealthyNonEmpty => return Some((ring_id, host_id)),
                ProbeOutcome::Empty | ProbeOutcome::Unreachable => continue,
            }
        }
        None
    }

    /// Schedule (immediately, in the background) one targeted repair session for
    /// `(table, range)` against `peer_ring_id`. Non-blocking: the controller's
    /// quarantine tick must never wait on the refill.
    ///
    /// The executor is resolved from the [`ExecutorProvider`] *inside* the
    /// spawned task so it reflects current ring membership and the resolve
    /// (which may touch the ring snapshot) never runs on the controller tick. A
    /// `None` executor (node not ring-ready) is logged loudly — the periodic
    /// cycle is the backstop (FMEA #10).
    fn schedule_session(&self, table: &TableId, range: (i64, i64), peer_ring_id: u64) {
        let provider = self.executor.clone();
        let table = table.clone();
        let (start, end) = range;
        TaskPool::current("anti-entropy-refill").spawn(async move {
            let Some(executor) = provider.current_executor() else {
                tracing::warn!(
                    keyspace = table.keyspace(),
                    table = table.table(),
                    range_start = start,
                    range_end = end,
                    peer = peer_ring_id,
                    "anti-entropy refill: node not ring-ready; targeted refill skipped — \
                     periodic repair is the backstop (FMEA #10)"
                );
                return;
            };
            match executor.run_session(&table, start, end, peer_ring_id).await {
                Ok(stats) => {
                    tracing::info!(
                        keyspace = table.keyspace(),
                        table = table.table(),
                        range_start = start,
                        range_end = end,
                        peer = peer_ring_id,
                        partitions_streamed_in = stats.partitions_streamed_in,
                        "anti-entropy refill: quarantined range repaired from healthy replica"
                    );
                }
                Err(e) => {
                    // Loud: the session failed; the periodic cycle is the backstop.
                    tracing::warn!(
                        keyspace = table.keyspace(),
                        table = table.table(),
                        range_start = start,
                        range_end = end,
                        peer = peer_ring_id,
                        error = %e,
                        "anti-entropy refill: targeted session FAILED; periodic repair is the backstop"
                    );
                }
            }
        });
    }
}

impl RepairTrigger for ClusterRepairTrigger {
    fn request_refill(&self, table: &TableId, ranges: &[(i64, i64)]) {
        for &range in ranges {
            match self.healthy_peer_for(table, range) {
                Some((ring_id, host_id)) => {
                    REFILLS_SCHEDULED.fetch_add(1, Ordering::Relaxed);
                    tracing::info!(
                        keyspace = table.keyspace(),
                        table = table.table(),
                        range_start = range.0,
                        range_end = range.1,
                        peer = ring_id,
                        %host_id,
                        "anti-entropy refill: verified healthy replica; scheduling targeted refill"
                    );
                    self.schedule_session(table, range, ring_id);
                }
                None => {
                    REFILLS_NO_SOURCE.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        keyspace = table.keyspace(),
                        table = table.table(),
                        range_start = range.0,
                        range_end = range.1,
                        "anti-entropy refill: NO verified-healthy replica for quarantined range; \
                         periodic repair cycle is the backstop (FMEA #10)"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::super::cluster_view::{ClusterTopology, RepairProbe};
    use super::super::coordinator::{SessionExecutor, SessionStats};

    /// Topology with a fixed replica-peer list, in the order the trigger probes.
    struct MockTopology {
        peers: Vec<(u64, uuid::Uuid)>,
    }
    impl ClusterTopology for MockTopology {
        fn this_node_id(&self) -> u64 {
            1
        }
        fn owners(&self, _t: &TableKey) -> Option<Vec<u64>> {
            Some(self.peers.iter().map(|(n, _)| *n).collect())
        }
        fn replica_peers(&self, _t: &TableKey) -> Vec<(u64, uuid::Uuid)> {
            self.peers.clone()
        }
    }

    /// Probe returning a per-peer canned outcome, recording the probe order.
    struct MockProbe {
        outcomes: HashMap<uuid::Uuid, ProbeOutcome>,
        probed: Mutex<Vec<uuid::Uuid>>,
    }
    impl MockProbe {
        fn new(outcomes: Vec<(uuid::Uuid, ProbeOutcome)>) -> Self {
            Self {
                outcomes: outcomes.into_iter().collect(),
                probed: Mutex::new(Vec::new()),
            }
        }
    }
    impl RepairProbe for MockProbe {
        fn probe_digest(&self, peer: uuid::Uuid, _t: &TableId, _r: (i64, i64)) -> ProbeOutcome {
            self.probed.lock().unwrap().push(peer);
            self.outcomes
                .get(&peer)
                .copied()
                .unwrap_or(ProbeOutcome::Unreachable)
        }
    }

    /// One repair session invocation: `(table, range_start, range_end, peer)`.
    type SessionCall = (TableId, i64, i64, u64);

    /// Executor that records every `run_session` it is asked to run.
    #[derive(Default)]
    struct RecordingExecutor {
        calls: Mutex<Vec<SessionCall>>,
    }
    #[async_trait]
    impl SessionExecutor for RecordingExecutor {
        async fn run_session(
            &self,
            table: &TableId,
            range_start: i64,
            range_end: i64,
            peer: u64,
        ) -> Result<SessionStats, String> {
            self.calls
                .lock()
                .unwrap()
                .push((table.clone(), range_start, range_end, peer));
            Ok(SessionStats::default())
        }
    }

    /// Build an [`ExecutorProvider`] that always resolves to `exec` — the
    /// healthy, ring-ready case the production closure hits once the node is
    /// placed in the ring.
    fn provider_for(exec: &Arc<RecordingExecutor>) -> Arc<dyn ExecutorProvider> {
        let exec = exec.clone();
        Arc::new(move || Some(exec.clone() as Arc<dyn SessionExecutor>))
    }

    /// Wait until the recording executor has observed `n` calls, or time out.
    /// The session runs on a spawned task, so the assertion must let it land
    /// (bounded — never an unbounded spin).
    async fn wait_for_calls(exec: &Arc<RecordingExecutor>, n: usize) -> Vec<SessionCall> {
        for _ in 0..200 {
            {
                let calls = exec.calls.lock().unwrap();
                if calls.len() >= n {
                    return calls.clone();
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        exec.calls.lock().unwrap().clone()
    }

    /// RED: a quarantine refill request must enqueue exactly one targeted repair
    /// session for the right `(table, range)` against a **verified-healthy**
    /// replica peer — the one whose probe returns `HealthyNonEmpty`.
    #[tokio::test]
    async fn request_refill_schedules_targeted_session_against_healthy_replica() {
        let empty_peer = (2u64, uuid::Uuid::from_u128(0xAAAA));
        let healthy_peer = (3u64, uuid::Uuid::from_u128(0xBBBB));

        let topology = Arc::new(MockTopology {
            // Probe order: empty peer first (skipped), then the healthy one.
            peers: vec![empty_peer, healthy_peer],
        });
        let probe = Arc::new(MockProbe::new(vec![
            (empty_peer.1, ProbeOutcome::Empty),
            (healthy_peer.1, ProbeOutcome::HealthyNonEmpty),
        ]));
        let exec = Arc::new(RecordingExecutor::default());

        let trigger = ClusterRepairTrigger::new(topology, probe, provider_for(&exec));

        let table = TableId::new("ks", "t");
        let range = (100i64, 200i64);
        let before = anti_entropy_refills_scheduled_total();

        trigger.request_refill(&table, &[range]);

        let calls = wait_for_calls(&exec, 1).await;
        assert_eq!(
            calls.len(),
            1,
            "exactly one targeted refill session must be scheduled"
        );
        assert_eq!(
            calls[0],
            (table.clone(), range.0, range.1, healthy_peer.0),
            "session must target the right table+range against the verified-healthy peer"
        );
        assert!(
            anti_entropy_refills_scheduled_total() > before,
            "the scheduled-refill metric must increment (corruption self-heal is observable)"
        );
    }

    /// WIRING (FMEA #10): when a verified-healthy replica IS found but the
    /// [`ExecutorProvider`] resolves to `None` (the node is not ring-ready), no
    /// session runs — the executor is resolved lazily from the provider, and an
    /// unready node must not fabricate a session against a peer it can't reach.
    /// This pins the production seam: `main.rs` supplies a provider that returns
    /// `None` until the node is placed in the ring, exactly like this.
    #[tokio::test]
    async fn request_refill_skips_session_when_executor_provider_not_ready() {
        let healthy_peer = (3u64, uuid::Uuid::from_u128(0xBEEF));
        let topology = Arc::new(MockTopology {
            peers: vec![healthy_peer],
        });
        let probe = Arc::new(MockProbe::new(vec![(
            healthy_peer.1,
            ProbeOutcome::HealthyNonEmpty,
        )]));
        // Provider is not ring-ready: always None.
        let not_ready: Arc<dyn ExecutorProvider> =
            Arc::new(|| -> Option<Arc<dyn SessionExecutor>> { None });
        let exec = Arc::new(RecordingExecutor::default());

        let trigger = ClusterRepairTrigger::new(topology, probe, not_ready);

        // A healthy peer is found, so the scheduled-refill metric DOES bump and a
        // task is spawned — but inside the task the provider yields no executor,
        // so no run_session ever lands on the recorder.
        trigger.request_refill(&TableId::new("ks", "t"), &[(0, 100)]);

        let calls = wait_for_calls(&exec, 1).await;
        assert!(
            calls.is_empty(),
            "no session may run when the executor provider is not ring-ready"
        );
    }

    /// No verified-healthy replica (all peers empty/unreachable) → no session is
    /// scheduled and the no-source metric increments (FMEA #1: never refill from
    /// a non-source; FMEA #10: the periodic cycle is the backstop).
    #[tokio::test]
    async fn request_refill_with_no_healthy_replica_schedules_nothing_and_counts_no_source() {
        let peer_a = (2u64, uuid::Uuid::from_u128(0xCCCC));
        let peer_b = (3u64, uuid::Uuid::from_u128(0xDDDD));

        let topology = Arc::new(MockTopology {
            peers: vec![peer_a, peer_b],
        });
        let probe = Arc::new(MockProbe::new(vec![
            (peer_a.1, ProbeOutcome::Empty),
            (peer_b.1, ProbeOutcome::Unreachable),
        ]));
        let exec = Arc::new(RecordingExecutor::default());

        let trigger = ClusterRepairTrigger::new(topology, probe, provider_for(&exec));

        let before_no_source = anti_entropy_refills_no_source_total();
        trigger.request_refill(&TableId::new("ks", "t"), &[(0, 50)]);

        // Give any (erroneously) spawned session a chance to land, then assert none did.
        let calls = wait_for_calls(&exec, 1).await;
        assert!(
            calls.is_empty(),
            "no session may be scheduled when no replica is verified healthy"
        );
        assert!(
            anti_entropy_refills_no_source_total() > before_no_source,
            "the no-source metric must increment so the operator sees un-refilled corruption"
        );
    }
}
