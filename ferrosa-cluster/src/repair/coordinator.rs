//! Anti-entropy repair scheduler — drives [`RepairSession`] runs across every
//! `(table, owned-range, peer)` triple and reports per-session results.
//!
//! ## Design
//!
//! The coordinator is parameterised by a [`SessionExecutor`] trait so the
//! scheduling logic can be unit-tested in isolation from the RPC + streaming
//! machinery that actually runs a session against a remote peer.
//!
//! For a given (ring, local_node, rf, table):
//!
//! 1. Enumerate every token range the local node is a replica of, using the
//!    ring's vnode topology.
//! 2. For each range, look up the peer replicas via [`repair_participants`].
//!    Skip self.
//! 3. For each `(range, peer)` pair, invoke the executor. Cap concurrency
//!    via a tokio semaphore.
//! 4. Collect per-session results and return.
//!
//! The actual RPC + streaming impl of `SessionExecutor` lives in a follow-up
//! PR alongside the wire-protocol additions.

use async_trait::async_trait;
use std::sync::Arc;

use ferrosa_storage::TableId;

use crate::ring::TokenRing;

/// Statistics returned by a single repair session between two replicas.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionStats {
    /// Partitions streamed from the remote peer to the local node.
    pub partitions_streamed_in: u64,
    /// Partitions streamed from the local node to the remote peer.
    pub partitions_streamed_out: u64,
    /// Partition keys with identical timestamps and divergent content —
    /// surfaced to the operator, NOT auto-resolved.
    pub timestamp_ties: u64,
}

/// A `(table, range, peer)` triple scheduled for repair plus the
/// [`SessionStats`] (or error) the executor returned.
#[derive(Debug)]
pub struct SessionResult {
    pub table: TableId,
    pub range_start: i64,
    pub range_end: i64,
    pub peer: u64,
    pub result: Result<SessionStats, String>,
}

/// Executes a single repair session between the local node and one peer
/// over one token range of one table.
///
/// The production implementation runs the Merkle exchange + partition diff
/// + streaming over the internode RPC channel. Tests inject mocks that
/// record their inputs without actually doing any I/O.
#[async_trait]
pub trait SessionExecutor: Send + Sync {
    async fn run_session(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
        peer: u64,
    ) -> Result<SessionStats, String>;
}

/// Coordinator that fans repair sessions out across every (range, peer)
/// pair for a given table.
pub struct RepairCoordinator {
    /// Max concurrent sessions in flight. Bounded so a node with many
    /// vnodes × many peers doesn't open hundreds of simultaneous streams.
    pub max_concurrent_sessions: usize,
}

impl Default for RepairCoordinator {
    fn default() -> Self {
        Self {
            // 4 matches Cassandra's per-node default. With the
            // `read_token_range` primitive wiring (only materialises
            // partitions in the requested token sub-range, not a full
            // key-ordered prefix), the per-session working set is bounded
            // by leaf cardinality — typically << 1 partition per leaf at
            // TREE_DEPTH=15 — so 4 concurrent sessions fit comfortably in
            // the 2 GB fmem container.
            max_concurrent_sessions: 4,
        }
    }
}

impl RepairCoordinator {
    /// Repair `table` across every owned token range against every peer
    /// participant. Returns one [`SessionResult`] per `(range, peer)`
    /// invocation, in arbitrary order.
    pub async fn repair_table(
        &self,
        executor: Arc<dyn SessionExecutor>,
        ring: &TokenRing,
        local_node_id: u64,
        rf: usize,
        table: &TableId,
    ) -> Vec<SessionResult> {
        // Build the work list synchronously. For each (range, peer) we
        // need: range bounds, peer id. Skip ranges this node doesn't
        // replicate, and skip self when looking up peers.
        let owned_ranges = owned_token_ranges(ring, local_node_id, rf);
        let mut tasks: Vec<(i64, i64, u64)> = Vec::new();
        for (range_start, range_end) in owned_ranges {
            // Pick a representative token in the range to ask the ring
            // who replicates it. All tokens in a single vnode range
            // share the same replica set by construction.
            let peers = super::repair_participants(ring, range_start, rf);
            for peer in peers {
                if peer == local_node_id {
                    continue;
                }
                tasks.push((range_start, range_end, peer));
            }
        }

        // Bound parallelism with a semaphore.
        let sem = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent_sessions));
        let mut handles = Vec::with_capacity(tasks.len());
        for (range_start, range_end, peer) in tasks {
            let table = table.clone();
            let executor = executor.clone();
            let sem = sem.clone();
            handles.push(tokio::spawn(async move {
                // OwnedSemaphorePermit: held for the body of run_session,
                // released on drop. Bound on the runtime queue, not on
                // throughput of any single session.
                let _permit = sem.acquire_owned().await.expect("semaphore not closed");
                let result = executor
                    .run_session(&table, range_start, range_end, peer)
                    .await;
                SessionResult {
                    table,
                    range_start,
                    range_end,
                    peer,
                    result,
                }
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for h in handles {
            // A panicking session is reported as an error rather than
            // aborting the whole repair run.
            match h.await {
                Ok(r) => results.push(r),
                Err(e) => {
                    // We can't reconstruct the (range, peer) from a panic,
                    // so use sentinel values. Tests treat this as
                    // unexpected; production logs it.
                    results.push(SessionResult {
                        table: table.clone(),
                        range_start: 0,
                        range_end: 0,
                        peer: 0,
                        result: Err(format!("session task panicked: {e}")),
                    });
                }
            }
        }
        results
    }
}

/// Token ranges this node is a replica of, as `[start, end)` pairs over the
/// vnode partitioning. Excludes ranges where the local node isn't in the
/// replica set for `rf`. For the wrap-around segment (last token → first
/// token via i64::MIN/MAX), we emit TWO ranges so callers don't have to
/// special-case it.
fn owned_token_ranges(ring: &TokenRing, local_node_id: u64, rf: usize) -> Vec<(i64, i64)> {
    let mut all_tokens: Vec<i64> = ring
        .node_ids()
        .iter()
        .flat_map(|&n| ring.tokens_for_node(n))
        .collect();
    all_tokens.sort_unstable();
    all_tokens.dedup();
    if all_tokens.is_empty() {
        return vec![];
    }

    let mut ranges = Vec::new();
    for (i, &start) in all_tokens.iter().enumerate() {
        let end_token = all_tokens.get(i + 1).copied().unwrap_or(i64::MAX); // wrap segment continues to i64::MAX...
                                                                            // A range belongs to the local node if local_node_id is in the
                                                                            // replica set of the range's start token.
        if ring.replicas(start, rf).contains(&local_node_id) {
            ranges.push((start, end_token));
        }
    }
    // ... and from i64::MIN to the smallest token, if that wrap segment's
    // replicas include local.
    let first = all_tokens[0];
    if first != i64::MIN && ring.replicas(i64::MIN, rf).contains(&local_node_id) {
        ranges.push((i64::MIN, first));
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{NodeInfo, NodeState};
    use std::sync::Mutex;
    use uuid::Uuid;

    fn node_with(addr: &str, state: NodeState) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state,
            cql_broadcast: None,
        }
    }

    /// Mock executor that records every invocation and returns the
    /// configured stats. Lets tests assert on the (range, peer) work
    /// the coordinator scheduled without doing any real I/O.
    struct MockExecutor {
        calls: Mutex<Vec<(TableId, i64, i64, u64)>>,
        next_stats: SessionStats,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                next_stats: SessionStats::default(),
            }
        }
    }

    #[async_trait]
    impl SessionExecutor for MockExecutor {
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
            Ok(self.next_stats.clone())
        }
    }

    fn three_node_ring() -> TokenRing {
        let mut ring = TokenRing::new();
        ring.add_node(1, node_with("10.0.0.1:7000", NodeState::Normal));
        ring.add_node(2, node_with("10.0.0.2:7000", NodeState::Normal));
        ring.add_node(3, node_with("10.0.0.3:7000", NodeState::Normal));
        ring.assign_tokens(1, &[100, 400, 700]);
        ring.assign_tokens(2, &[200, 500, 800]);
        ring.assign_tokens(3, &[300, 600, 900]);
        ring
    }

    #[tokio::test]
    async fn repair_table_schedules_one_session_per_peer_per_owned_range() {
        let ring = three_node_ring();
        let table = TableId::new("ks", "tbl");
        let exec = Arc::new(MockExecutor::new());
        let coord = RepairCoordinator {
            max_concurrent_sessions: 4,
        };
        // RF=3 on a 3-node ring: every range is replicated by all 3 nodes.
        let results = coord.repair_table(exec.clone(), &ring, 1, 3, &table).await;
        // Expect: 9 vnode ranges × 2 peers = 18 sessions (plus the wrap
        // segment if local owns it — which it does at RF=3).
        let calls = exec.calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            results.len(),
            "every session must record exactly one mock call"
        );
        assert!(
            results.iter().all(|r| r.result.is_ok()),
            "mock executor returns Ok for every call"
        );
        // Every peer recorded is in {2, 3} — never the local node.
        let peers: std::collections::BTreeSet<u64> = calls.iter().map(|(_, _, _, p)| *p).collect();
        assert_eq!(peers, [2u64, 3].iter().copied().collect());
        // At least one session per peer.
        assert!(calls.iter().any(|(_, _, _, p)| *p == 2));
        assert!(calls.iter().any(|(_, _, _, p)| *p == 3));
    }

    #[tokio::test]
    async fn repair_table_skips_self() {
        let ring = three_node_ring();
        let table = TableId::new("ks", "tbl");
        let exec = Arc::new(MockExecutor::new());
        let coord = RepairCoordinator::default();
        coord.repair_table(exec.clone(), &ring, 2, 3, &table).await;
        let calls = exec.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|(_, _, _, p)| *p == 2),
            "local node 2 must never be its own peer"
        );
    }

    #[tokio::test]
    async fn repair_table_empty_ring_returns_no_results() {
        let ring = TokenRing::new();
        let table = TableId::new("ks", "tbl");
        let exec = Arc::new(MockExecutor::new());
        let coord = RepairCoordinator::default();
        let results = coord.repair_table(exec.clone(), &ring, 1, 3, &table).await;
        assert!(results.is_empty());
        assert!(exec.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn repair_table_concurrency_cap_caps_in_flight() {
        // Custom executor that holds the OwnedSemaphorePermit while sleeping,
        // letting us observe that more than max_concurrent_sessions don't
        // run simultaneously.
        struct ConcurrencyTracker {
            in_flight: Arc<std::sync::atomic::AtomicUsize>,
            peak: Arc<std::sync::atomic::AtomicUsize>,
        }
        #[async_trait]
        impl SessionExecutor for ConcurrencyTracker {
            async fn run_session(
                &self,
                _table: &TableId,
                _: i64,
                _: i64,
                _: u64,
            ) -> Result<SessionStats, String> {
                let n = self
                    .in_flight
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                self.peak.fetch_max(n, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                self.in_flight
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                Ok(SessionStats::default())
            }
        }
        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let exec: Arc<dyn SessionExecutor> = Arc::new(ConcurrencyTracker {
            in_flight: in_flight.clone(),
            peak: peak.clone(),
        });
        let coord = RepairCoordinator {
            max_concurrent_sessions: 2,
        };
        let ring = three_node_ring();
        let table = TableId::new("ks", "tbl");
        coord.repair_table(exec, &ring, 1, 3, &table).await;
        let observed_peak = peak.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            observed_peak <= 2,
            "peak concurrency was {observed_peak}, must be <= 2"
        );
        assert!(
            observed_peak >= 2,
            "with 18 sessions and cap=2 we should hit the cap at some point; saw peak={observed_peak}"
        );
    }
}
