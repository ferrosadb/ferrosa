//! Dependency wait graph with cycle detection and deadlock breaking.
//!
//! In the Accord protocol, a transaction that is committed but not yet applied
//! must wait for all of its dependencies to be applied first. This module
//! implements a waits-for graph that tracks these dependency relationships and
//! detects cycles (deadlocks) by traversing the graph with DFS.
//!
//! # Deadlock breaking
//!
//! When a cycle is detected, the transaction with the **highest** `t0`
//! (coordinator-proposed timestamp) is aborted. This is deterministic: every
//! replica will make the same choice for the same cycle.
//!
//! # Timeout
//!
//! If a transaction waits longer than 10 seconds for a dependency, it is
//! aborted with a timeout error.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use ferrosa_common::accord::TxnId;

/// Default dependency wait timeout: 10 seconds.
const DEP_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// DepWaitError
// ---------------------------------------------------------------------------

/// Errors that can occur during dependency waiting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepWaitError {
    /// A cycle was detected in the waits-for graph.
    CycleDetected {
        /// The transactions forming the cycle, in order.
        cycle: Vec<TxnId>,
    },
    /// The transaction with the highest t0 in a cycle was aborted.
    Aborted {
        /// The transaction that was aborted.
        txn_id: TxnId,
        /// Reason for the abort.
        reason: String,
    },
    /// The wait timed out after the configured duration.
    Timeout {
        /// The transaction that timed out.
        txn_id: TxnId,
        /// How long the transaction waited.
        waited: Duration,
    },
}

impl std::fmt::Display for DepWaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DepWaitError::CycleDetected { cycle } => {
                write!(f, "dependency cycle detected: {} transactions", cycle.len())
            }
            DepWaitError::Aborted { txn_id, reason } => {
                write!(f, "transaction t0={} aborted: {}", txn_id.0.time, reason)
            }
            DepWaitError::Timeout { txn_id, waited } => {
                write!(
                    f,
                    "transaction t0={} timed out after {:?}",
                    txn_id.0.time, waited
                )
            }
        }
    }
}

impl std::error::Error for DepWaitError {}

// ---------------------------------------------------------------------------
// DepWaitGraph
// ---------------------------------------------------------------------------

/// Waits-for graph for Accord dependency ordering.
///
/// Tracks which transactions are waiting on which dependencies, detects
/// cycles, and breaks deadlocks by aborting the transaction with the
/// highest `t0` in the cycle.
pub struct DepWaitGraph {
    /// Forward edges: waiter -> set of txns it is waiting on.
    waiting_on: HashMap<TxnId, HashSet<TxnId>>,
    /// Reverse edges: dep -> set of txns waiting on it.
    waited_by: HashMap<TxnId, HashSet<TxnId>>,
    /// Registration timestamps for timeout tracking.
    wait_start: HashMap<TxnId, Instant>,
    /// Applied transactions mapped to the time they were marked applied.
    /// Entries are pruned by [`prune()`](Self::prune) to bound memory.
    applied: HashMap<TxnId, Instant>,
    /// Aborted transactions mapped to the time they were aborted.
    /// Entries are pruned by [`prune()`](Self::prune) to bound memory.
    aborted: HashMap<TxnId, Instant>,
    /// Configurable timeout duration.
    timeout: Duration,
}

impl DepWaitGraph {
    /// Create a new empty wait graph with the default 10-second timeout.
    pub fn new() -> Self {
        Self {
            waiting_on: HashMap::new(),
            waited_by: HashMap::new(),
            wait_start: HashMap::new(),
            applied: HashMap::new(),
            aborted: HashMap::new(),
            timeout: DEP_WAIT_TIMEOUT,
        }
    }

    /// Create a new wait graph with a custom timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            waiting_on: HashMap::new(),
            waited_by: HashMap::new(),
            wait_start: HashMap::new(),
            applied: HashMap::new(),
            aborted: HashMap::new(),
            timeout,
        }
    }

    /// Register a dependency wait: `waiter` is waiting for `dep` to be applied.
    ///
    /// Returns `Ok(())` if the wait was registered (or `dep` is already applied),
    /// or `Err(DepWaitError::CycleDetected)` if adding this edge would create a cycle.
    ///
    /// If `dep` is already applied, this returns `Ok(())` immediately without
    /// registering an edge.
    pub fn register_wait(&mut self, waiter: TxnId, dep: TxnId) -> Result<(), DepWaitError> {
        // If the dependency is already applied, nothing to wait for.
        if self.applied.contains_key(&dep) {
            return Ok(());
        }

        // Tentatively add the edge, then check for cycles.
        self.waiting_on.entry(waiter).or_default().insert(dep);
        self.waited_by.entry(dep).or_default().insert(waiter);

        // Record start time if this is the first wait for this txn.
        self.wait_start.entry(waiter).or_insert_with(Instant::now);

        // Check for cycles starting from waiter.
        if let Some(cycle) = self.detect_cycle(waiter) {
            // Remove the edge we just added -- the caller will handle the abort.
            // (We leave the graph in a clean state.)
            if let Some(deps) = self.waiting_on.get_mut(&waiter) {
                deps.remove(&dep);
                if deps.is_empty() {
                    self.waiting_on.remove(&waiter);
                }
            }
            if let Some(waiters) = self.waited_by.get_mut(&dep) {
                waiters.remove(&waiter);
                if waiters.is_empty() {
                    self.waited_by.remove(&dep);
                }
            }

            return Err(DepWaitError::CycleDetected { cycle });
        }

        Ok(())
    }

    /// Break a cycle by aborting the transaction with the highest `t0`.
    ///
    /// Returns the `TxnId` of the aborted transaction and removes all its
    /// edges from the graph.
    pub fn break_cycle(&mut self, cycle: &[TxnId]) -> Result<TxnId, DepWaitError> {
        assert!(!cycle.is_empty(), "cycle must not be empty");

        // Find the transaction with the highest t0.
        let victim = cycle
            .iter()
            .max_by_key(|txn_id| txn_id.0)
            .copied()
            .expect("cycle is non-empty");

        // Remove victim from the graph.
        self.remove_txn(victim);
        self.aborted.insert(victim, Instant::now());

        Ok(victim)
    }

    /// Mark a transaction as applied (completed).
    ///
    /// Returns the set of transactions that were waiting on this dep and
    /// are now unblocked (have no remaining dependencies).
    pub fn mark_applied(&mut self, txn_id: TxnId) -> Vec<TxnId> {
        self.applied.insert(txn_id, Instant::now());

        let mut woken = Vec::new();

        // Remove txn_id from all waiters' dependency sets.
        if let Some(waiters) = self.waited_by.remove(&txn_id) {
            for waiter in &waiters {
                if let Some(deps) = self.waiting_on.get_mut(waiter) {
                    deps.remove(&txn_id);
                    if deps.is_empty() {
                        woken.push(*waiter);
                    }
                }
            }
            // Clean up empty entries.
            for w in &woken {
                self.waiting_on.remove(w);
                self.wait_start.remove(w);
            }
        }

        // Also remove the txn itself if it was waiting on anything.
        self.waiting_on.remove(&txn_id);
        self.wait_start.remove(&txn_id);

        woken
    }

    /// Check if a transaction has been applied.
    pub fn is_applied(&self, txn_id: &TxnId) -> bool {
        self.applied.contains_key(txn_id)
    }

    /// Check if a transaction has been aborted.
    pub fn is_aborted(&self, txn_id: &TxnId) -> bool {
        self.aborted.contains_key(txn_id)
    }

    /// Returns the number of entries in the applied set.
    pub fn applied_count(&self) -> usize {
        self.applied.len()
    }

    /// Returns the number of entries in the aborted set.
    pub fn aborted_count(&self) -> usize {
        self.aborted.len()
    }

    /// Evicts entries from `applied` and `aborted` that are older than `max_age`.
    ///
    /// Returns the total number of entries removed.
    ///
    /// Call this periodically from the maintenance loop (e.g., every 60 seconds
    /// with `max_age = 10 * timeout`) to prevent the sets from growing
    /// without bound under sustained write traffic.
    pub fn prune(&mut self, max_age: Duration) -> usize {
        let now = Instant::now();
        let before = self.applied.len() + self.aborted.len();
        self.applied
            .retain(|_, inserted_at| now.duration_since(*inserted_at) < max_age);
        self.aborted
            .retain(|_, inserted_at| now.duration_since(*inserted_at) < max_age);
        let after = self.applied.len() + self.aborted.len();
        before - after
    }

    /// Check if a transaction is currently waiting.
    pub fn is_waiting(&self, txn_id: &TxnId) -> bool {
        self.waiting_on.contains_key(txn_id)
    }

    /// Get the set of transactions that `txn_id` is waiting on.
    pub fn deps_of(&self, txn_id: &TxnId) -> Option<&HashSet<TxnId>> {
        self.waiting_on.get(txn_id)
    }

    /// Check for timed-out waiters.
    ///
    /// Returns the list of transactions that have exceeded the timeout.
    pub fn check_timeouts(&self) -> Vec<TxnId> {
        let now = Instant::now();
        self.wait_start
            .iter()
            .filter(|(txn_id, start)| {
                now.duration_since(**start) >= self.timeout && self.waiting_on.contains_key(txn_id)
            })
            .map(|(txn_id, _)| *txn_id)
            .collect()
    }

    /// Abort a transaction due to timeout.
    ///
    /// Returns a `DepWaitError::Timeout` with the elapsed duration.
    pub fn abort_timeout(&mut self, txn_id: TxnId) -> DepWaitError {
        let waited = self
            .wait_start
            .get(&txn_id)
            .map(|start| Instant::now().duration_since(*start))
            .unwrap_or(self.timeout);

        self.remove_txn(txn_id);
        self.aborted.insert(txn_id, Instant::now());

        DepWaitError::Timeout { txn_id, waited }
    }

    /// Return the number of transactions currently waiting.
    pub fn waiting_count(&self) -> usize {
        self.waiting_on.len()
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Detect a cycle reachable from `start` using DFS.
    ///
    /// Returns `Some(cycle)` if a cycle is found, where `cycle` contains
    /// the transactions in the cycle in traversal order.
    fn detect_cycle(&self, start: TxnId) -> Option<Vec<TxnId>> {
        let mut visited = HashSet::new();
        let mut path = Vec::new();
        let mut path_set = HashSet::new();

        self.dfs_cycle(start, &mut visited, &mut path, &mut path_set)
    }

    /// DFS cycle detection helper.
    ///
    /// Returns `Some(cycle)` if we revisit a node already on the current path.
    fn dfs_cycle(
        &self,
        node: TxnId,
        visited: &mut HashSet<TxnId>,
        path: &mut Vec<TxnId>,
        path_set: &mut HashSet<TxnId>,
    ) -> Option<Vec<TxnId>> {
        if path_set.contains(&node) {
            // Found a cycle. Extract the cycle portion from path.
            let cycle_start = path.iter().position(|n| *n == node).unwrap();
            let cycle: Vec<TxnId> = path[cycle_start..].to_vec();
            return Some(cycle);
        }

        if visited.contains(&node) {
            return None;
        }

        visited.insert(node);
        path.push(node);
        path_set.insert(node);

        if let Some(deps) = self.waiting_on.get(&node) {
            // Sort for deterministic traversal order.
            let mut deps_sorted: Vec<TxnId> = deps.iter().copied().collect();
            deps_sorted.sort();
            for dep in deps_sorted {
                if let Some(cycle) = self.dfs_cycle(dep, visited, path, path_set) {
                    return Some(cycle);
                }
            }
        }

        path.pop();
        path_set.remove(&node);
        None
    }

    /// Remove a transaction from the graph entirely.
    fn remove_txn(&mut self, txn_id: TxnId) {
        // Remove forward edges (txn_id -> deps).
        if let Some(deps) = self.waiting_on.remove(&txn_id) {
            for dep in &deps {
                if let Some(waiters) = self.waited_by.get_mut(dep) {
                    waiters.remove(&txn_id);
                    if waiters.is_empty() {
                        self.waited_by.remove(dep);
                    }
                }
            }
        }

        // Remove reverse edges (others -> txn_id).
        if let Some(waiters) = self.waited_by.remove(&txn_id) {
            for waiter in &waiters {
                if let Some(deps) = self.waiting_on.get_mut(waiter) {
                    deps.remove(&txn_id);
                    // Don't remove the waiter entry here -- they may have other deps.
                }
            }
        }

        self.wait_start.remove(&txn_id);
    }
}

impl Default for DepWaitGraph {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::accord::Timestamp;

    /// Helper: create a TxnId with a given time value for easy ordering.
    fn txn(time: u64) -> TxnId {
        TxnId(Timestamp {
            epoch: 0,
            time,
            seq: 0,
            node: 1,
        })
    }

    // -----------------------------------------------------------------------
    // Test 1: dep_wait_simple_chain
    // -----------------------------------------------------------------------

    #[test]
    fn dep_wait_simple_chain() {
        // A waits for B. B completes. A wakes.
        let mut graph = DepWaitGraph::new();

        let a = txn(100);
        let b = txn(200);

        // A registers a wait on B.
        graph.register_wait(a, b).expect("no cycle");

        // A should be waiting.
        assert!(graph.is_waiting(&a), "A must be waiting");
        assert!(!graph.is_waiting(&b), "B must not be waiting");

        // B completes (applied).
        let woken = graph.mark_applied(b);

        // A should be woken.
        assert_eq!(woken, vec![a], "A must be woken when B is applied");
        assert!(!graph.is_waiting(&a), "A must no longer be waiting");
        assert!(graph.is_applied(&b), "B must be marked as applied");
    }

    // -----------------------------------------------------------------------
    // Test 2: dep_wait_transitive
    // -----------------------------------------------------------------------

    #[test]
    fn dep_wait_transitive() {
        // A waits for B, B waits for C. C completes, then B completes.
        // Both wake in order.
        let mut graph = DepWaitGraph::new();

        let a = txn(100);
        let b = txn(200);
        let c = txn(300);

        // A waits for B.
        graph.register_wait(a, b).expect("no cycle");
        // B waits for C.
        graph.register_wait(b, c).expect("no cycle");

        assert!(graph.is_waiting(&a));
        assert!(graph.is_waiting(&b));

        // C completes -> B wakes.
        let woken_from_c = graph.mark_applied(c);
        assert_eq!(woken_from_c, vec![b], "B must wake when C is applied");
        assert!(graph.is_waiting(&a), "A must still be waiting (on B)");

        // B completes -> A wakes.
        let woken_from_b = graph.mark_applied(b);
        assert_eq!(woken_from_b, vec![a], "A must wake when B is applied");
        assert!(!graph.is_waiting(&a), "A must no longer be waiting");
    }

    // -----------------------------------------------------------------------
    // Test 3: dep_wait_deadlock_detection
    // -----------------------------------------------------------------------

    #[test]
    fn dep_wait_deadlock_detection() {
        // A -> B -> A cycle detected.
        let mut graph = DepWaitGraph::new();

        let a = txn(100);
        let b = txn(200);

        // A waits for B.
        graph.register_wait(a, b).expect("no cycle yet");

        // B waits for A -> cycle!
        let result = graph.register_wait(b, a);
        assert!(result.is_err(), "cycle must be detected");

        match result.unwrap_err() {
            DepWaitError::CycleDetected { cycle } => {
                // The cycle must contain both A and B.
                assert!(
                    cycle.contains(&a) && cycle.contains(&b),
                    "cycle must contain both A ({:?}) and B ({:?}), got {:?}",
                    a,
                    b,
                    cycle,
                );
            }
            other => panic!("expected CycleDetected, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Test 4: dep_wait_cycle_break
    // -----------------------------------------------------------------------

    #[test]
    fn dep_wait_cycle_break() {
        // Cycle A -> B -> A, broken by aborting highest t0 (B, t0=200).
        let mut graph = DepWaitGraph::new();

        let a = txn(100); // lower t0
        let b = txn(200); // higher t0

        // A waits for B.
        graph.register_wait(a, b).expect("no cycle yet");

        // B waits for A -> cycle detected.
        let result = graph.register_wait(b, a);
        assert!(result.is_err());

        let cycle = match result.unwrap_err() {
            DepWaitError::CycleDetected { cycle } => cycle,
            other => panic!("expected CycleDetected, got {:?}", other),
        };

        // Break the cycle: victim must be B (highest t0 = 200).
        let victim = graph.break_cycle(&cycle).expect("break_cycle must succeed");
        assert_eq!(
            victim, b,
            "victim must be B (highest t0); got t0={}",
            victim.0.time,
        );
        assert!(graph.is_aborted(&b), "B must be marked as aborted");

        // A should no longer be blocked (B was removed from graph).
        // A was waiting on B, and B was removed, so A's dep set is cleared.
        // But since we rolled back B->A before returning the cycle error,
        // only A->B exists. After break_cycle removes B, A is unblocked.
        assert!(
            !graph.is_waiting(&a) || graph.deps_of(&a).is_none_or(|d| d.is_empty()),
            "A must not be blocked after cycle break",
        );
    }

    // -----------------------------------------------------------------------
    // Test 5: dep_wait_timeout
    // -----------------------------------------------------------------------

    #[test]
    fn dep_wait_timeout() {
        // Use a very short timeout to test timeout behavior.
        let mut graph = DepWaitGraph::with_timeout(Duration::from_millis(1));

        let a = txn(100);
        let b = txn(200);

        graph.register_wait(a, b).expect("no cycle");

        // Wait for the timeout to elapse.
        std::thread::sleep(Duration::from_millis(5));

        // Check timeouts.
        let timed_out = graph.check_timeouts();
        assert!(
            timed_out.contains(&a),
            "A must be in the timed-out set; got {:?}",
            timed_out,
        );

        // Abort the timed-out transaction.
        let err = graph.abort_timeout(a);
        match err {
            DepWaitError::Timeout { txn_id, waited } => {
                assert_eq!(txn_id, a, "timed out txn must be A");
                assert!(
                    waited >= Duration::from_millis(1),
                    "waited duration must be >= 1ms, got {:?}",
                    waited,
                );
            }
            other => panic!("expected Timeout, got {:?}", other),
        }

        assert!(graph.is_aborted(&a), "A must be marked as aborted");
        assert!(
            !graph.is_waiting(&a),
            "A must not be waiting after timeout abort"
        );
    }

    // -----------------------------------------------------------------------
    // Test 6: dep_wait_already_applied
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Test: applied/aborted set size accessors
    // -----------------------------------------------------------------------

    #[test]
    fn applied_count_tracks_mark_applied() {
        let mut graph = DepWaitGraph::new();
        assert_eq!(graph.applied_count(), 0);
        graph.mark_applied(txn(1));
        graph.mark_applied(txn(2));
        assert_eq!(graph.applied_count(), 2);
    }

    #[test]
    fn aborted_count_tracks_break_cycle() {
        let mut graph = DepWaitGraph::new();
        assert_eq!(graph.aborted_count(), 0);

        let a = txn(100);
        let b = txn(200);
        graph.register_wait(a, b).unwrap();
        let result = graph.register_wait(b, a);
        let cycle = match result.unwrap_err() {
            DepWaitError::CycleDetected { cycle } => cycle,
            other => panic!("expected CycleDetected, got {:?}", other),
        };
        graph.break_cycle(&cycle).unwrap();
        assert_eq!(graph.aborted_count(), 1);
    }

    // -----------------------------------------------------------------------
    // Test: prune evicts stale applied entries
    // -----------------------------------------------------------------------

    #[test]
    fn prune_removes_old_applied_entries() {
        let mut graph = DepWaitGraph::new();
        graph.mark_applied(txn(1));
        graph.mark_applied(txn(2));
        assert_eq!(graph.applied_count(), 2);

        // Zero duration: every entry is "older than max_age", so all are pruned.
        let pruned = graph.prune(Duration::ZERO);
        assert_eq!(pruned, 2);
        assert_eq!(graph.applied_count(), 0);
        assert!(!graph.is_applied(&txn(1)));
        assert!(!graph.is_applied(&txn(2)));
    }

    #[test]
    fn prune_keeps_recent_applied_entries() {
        let mut graph = DepWaitGraph::new();
        graph.mark_applied(txn(1));
        graph.mark_applied(txn(2));

        // Very long max_age: nothing is old enough to prune.
        let pruned = graph.prune(Duration::from_secs(3600));
        assert_eq!(pruned, 0);
        assert_eq!(graph.applied_count(), 2);
        assert!(graph.is_applied(&txn(1)));
        assert!(graph.is_applied(&txn(2)));
    }

    #[test]
    fn prune_removes_old_aborted_entries() {
        let mut graph = DepWaitGraph::new();

        let a = txn(100);
        let b = txn(200);
        graph.register_wait(a, b).unwrap();
        let result = graph.register_wait(b, a);
        let cycle = match result.unwrap_err() {
            DepWaitError::CycleDetected { cycle } => cycle,
            other => panic!("expected cycle, got {:?}", other),
        };
        graph.break_cycle(&cycle).unwrap();
        assert_eq!(graph.aborted_count(), 1);

        let pruned = graph.prune(Duration::ZERO);
        assert!(pruned >= 1, "at least one aborted entry must be pruned");
        assert_eq!(graph.aborted_count(), 0);
    }

    #[test]
    fn applied_set_bounded_by_pruning() {
        let mut graph = DepWaitGraph::new();

        for i in 0..1000u64 {
            graph.mark_applied(txn(i));
        }
        assert_eq!(graph.applied_count(), 1000);

        let pruned = graph.prune(Duration::ZERO);
        assert_eq!(pruned, 1000);
        assert_eq!(graph.applied_count(), 0);
    }

    #[test]
    fn dep_wait_already_applied() {
        // Wait on an already-applied txn returns immediately.
        let mut graph = DepWaitGraph::new();

        let a = txn(100);
        let b = txn(200);

        // B is already applied.
        graph.mark_applied(b);
        assert!(graph.is_applied(&b));

        // A tries to wait on B -> returns Ok immediately, no edge added.
        graph.register_wait(a, b).expect("must not error");

        // A must NOT be in the waiting set (no edge was added).
        assert!(
            !graph.is_waiting(&a),
            "A must not be waiting on an already-applied dep",
        );

        assert_eq!(graph.waiting_count(), 0, "graph must have no waiters",);
    }

    // -----------------------------------------------------------------------
    // Test 7: dep_wait_concurrent_wakeup
    // -----------------------------------------------------------------------

    #[test]
    fn dep_wait_concurrent_wakeup() {
        // Multiple waiters (A, B, C) all wait on D. D completes -> all wake.
        let mut graph = DepWaitGraph::new();

        let a = txn(100);
        let b = txn(200);
        let c = txn(300);
        let d = txn(400);

        graph.register_wait(a, d).expect("no cycle");
        graph.register_wait(b, d).expect("no cycle");
        graph.register_wait(c, d).expect("no cycle");

        assert_eq!(
            graph.waiting_count(),
            3,
            "three transactions must be waiting"
        );

        // D completes -> all three wake.
        let mut woken = graph.mark_applied(d);
        woken.sort(); // Sort for deterministic comparison.

        let mut expected = vec![a, b, c];
        expected.sort();

        assert_eq!(woken, expected, "all three waiters must be woken",);

        assert_eq!(
            graph.waiting_count(),
            0,
            "no transactions should be waiting after D is applied",
        );
    }
}
