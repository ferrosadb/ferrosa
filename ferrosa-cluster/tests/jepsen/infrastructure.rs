//! Jepsen-style test infrastructure.
//!
//! Provides a [`JepsenCluster`] built on top of the Accord [`TestCluster`],
//! adding nemesis control, history recording, and CQL client abstractions.

use ferrosa_cluster::accord::test_cluster::{TestCluster, TestMessage, TestMessagePayload};
use ferrosa_common::accord::{Timestamp, TxnId, TxnPhase};
use std::collections::{HashMap, HashSet};

// ---------------------------------------------------------------------------
// Operation types for history recording
// ---------------------------------------------------------------------------

/// A single operation in the test history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operation {
    /// Wall-clock timestamp when the operation was invoked.
    pub invoke_time_ns: u64,
    /// Wall-clock timestamp when the operation completed (0 if pending).
    pub complete_time_ns: u64,
    /// Which client issued the operation.
    pub client_id: u64,
    /// The type of operation.
    pub op_type: OpType,
    /// The result of the operation (None if pending or failed).
    pub result: Option<OpResult>,
}

/// Type of operation in the history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpType {
    /// Read the register value.
    Read,
    /// Write a value to the register.
    Write(i64),
}

/// Result of an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpResult {
    /// Successful read returning a value (None = not yet written).
    ReadOk(Option<i64>),
    /// Successful write.
    WriteOk,
    /// Operation failed (e.g., timeout, partition).
    Error(String),
}

// ---------------------------------------------------------------------------
// HistoryRecorder
// ---------------------------------------------------------------------------

/// Records all operations with wall-clock timestamps for linearizability checking.
pub struct HistoryRecorder {
    /// All recorded operations in chronological order.
    history: Vec<Operation>,
    /// Monotonic clock counter (simulated wall-clock for determinism).
    clock_ns: u64,
}

impl HistoryRecorder {
    /// Create a new recorder starting at time 0.
    pub fn new() -> Self {
        Self {
            history: Vec::new(),
            clock_ns: 0,
        }
    }

    /// Advance the simulated clock by `delta_ns` nanoseconds.
    pub fn advance_clock(&mut self, delta_ns: u64) {
        self.clock_ns = self.clock_ns.saturating_add(delta_ns);
    }

    /// Current simulated time.
    pub fn now(&self) -> u64 {
        self.clock_ns
    }

    /// Record the invocation of an operation. Returns the index in the history.
    pub fn invoke(&mut self, client_id: u64, op_type: OpType) -> usize {
        let idx = self.history.len();
        self.history.push(Operation {
            invoke_time_ns: self.clock_ns,
            complete_time_ns: 0,
            client_id,
            op_type,
            result: None,
        });
        idx
    }

    /// Record the completion of an operation.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds or the operation already completed.
    pub fn complete(&mut self, index: usize, result: OpResult) {
        assert!(index < self.history.len(), "operation index out of bounds");
        assert!(
            self.history[index].result.is_none(),
            "operation {} already completed",
            index
        );
        self.history[index].complete_time_ns = self.clock_ns;
        self.history[index].result = Some(result);
    }

    /// Return all recorded operations.
    pub fn history(&self) -> &[Operation] {
        &self.history
    }

    /// Return only completed operations.
    pub fn completed_ops(&self) -> Vec<&Operation> {
        self.history
            .iter()
            .filter(|op| op.result.is_some())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// NemesisType
// ---------------------------------------------------------------------------

/// Types of nemesis faults that can be injected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NemesisType {
    /// Network partition: drop all messages between two sets of nodes.
    Partition {
        /// Nodes in partition A.
        side_a: Vec<u64>,
        /// Nodes in partition B.
        side_b: Vec<u64>,
    },
    /// Kill a node: stop processing all messages to/from it.
    Kill { node_id: u64 },
    /// Slow network: delay messages by a number of delivery cycles.
    Slow { node_id: u64, delay_cycles: usize },
    /// Clock skew: inject an offset into a node's HLC timestamps.
    ClockSkew {
        node_id: u64,
        /// Offset in microseconds (can be negative for backward skew).
        offset_us: i64,
    },
    /// Pause a node: freeze processing, buffer messages, resume later.
    Pause { node_id: u64 },
}

// ---------------------------------------------------------------------------
// NemesisController
// ---------------------------------------------------------------------------

/// Controls fault injection into a [`JepsenCluster`].
///
/// Nemesis effects are applied by filtering or modifying messages during
/// delivery, keeping the underlying TestCluster deterministic.
pub struct NemesisController {
    /// Active partitions: (side_a, side_b) pairs.
    partitions: Vec<(HashSet<u64>, HashSet<u64>)>,
    /// Killed nodes: messages to/from these nodes are dropped.
    killed_nodes: HashSet<u64>,
    /// Paused nodes: messages are buffered, not delivered.
    paused_nodes: HashSet<u64>,
    /// Buffered messages for paused nodes.
    pause_buffer: Vec<TestMessage>,
    /// Slow nodes: (node_id -> remaining delay cycles for pending messages).
    slow_nodes: HashMap<u64, usize>,
    /// Clock skew offsets per node (microseconds).
    clock_offsets: HashMap<u64, i64>,
}

impl NemesisController {
    pub fn new() -> Self {
        Self {
            partitions: Vec::new(),
            killed_nodes: HashSet::new(),
            paused_nodes: HashSet::new(),
            pause_buffer: Vec::new(),
            slow_nodes: HashMap::new(),
            clock_offsets: HashMap::new(),
        }
    }

    /// Activate a nemesis fault.
    pub fn inject(&mut self, nemesis: NemesisType) {
        match nemesis {
            NemesisType::Partition { side_a, side_b } => {
                let a: HashSet<u64> = side_a.into_iter().collect();
                let b: HashSet<u64> = side_b.into_iter().collect();
                self.partitions.push((a, b));
            }
            NemesisType::Kill { node_id } => {
                self.killed_nodes.insert(node_id);
            }
            NemesisType::Slow {
                node_id,
                delay_cycles,
            } => {
                self.slow_nodes.insert(node_id, delay_cycles);
            }
            NemesisType::ClockSkew { node_id, offset_us } => {
                self.clock_offsets.insert(node_id, offset_us);
            }
            NemesisType::Pause { node_id } => {
                self.paused_nodes.insert(node_id);
            }
        }
    }

    /// Heal all nemesis faults and return any buffered messages.
    pub fn heal_all(&mut self) -> Vec<TestMessage> {
        self.partitions.clear();
        self.killed_nodes.clear();
        self.paused_nodes.clear();
        self.slow_nodes.clear();
        self.clock_offsets.clear();
        std::mem::take(&mut self.pause_buffer)
    }

    /// Heal only network partitions.
    pub fn heal_partitions(&mut self) {
        self.partitions.clear();
    }

    /// Resume a paused node and return its buffered messages.
    pub fn resume_node(&mut self, node_id: u64) -> Vec<TestMessage> {
        self.paused_nodes.remove(&node_id);
        let (buffered, remaining): (Vec<_>, Vec<_>) = self
            .pause_buffer
            .drain(..)
            .partition(|m| m.dst == node_id || m.src == node_id);
        self.pause_buffer = remaining;
        buffered
    }

    /// Check whether a message should be dropped by the current nemesis state.
    pub fn should_drop(&self, msg: &TestMessage) -> bool {
        // Killed node: drop all messages to/from it.
        if self.killed_nodes.contains(&msg.src) || self.killed_nodes.contains(&msg.dst) {
            return true;
        }

        // Partition: drop messages crossing the partition boundary.
        for (side_a, side_b) in &self.partitions {
            let src_in_a = side_a.contains(&msg.src);
            let src_in_b = side_b.contains(&msg.src);
            let dst_in_a = side_a.contains(&msg.dst);
            let dst_in_b = side_b.contains(&msg.dst);

            // Drop if src is in one side and dst is in the other.
            if (src_in_a && dst_in_b) || (src_in_b && dst_in_a) {
                return true;
            }
        }

        false
    }

    /// Check whether a message should be buffered (paused node).
    pub fn should_buffer(&self, msg: &TestMessage) -> bool {
        self.paused_nodes.contains(&msg.dst)
    }

    /// Buffer a message for a paused node.
    pub fn buffer_message(&mut self, msg: TestMessage) {
        self.pause_buffer.push(msg);
    }

    /// Check whether a message should be delayed (slow network).
    /// Returns true if the destination node has remaining delay cycles.
    pub fn should_delay(&mut self, msg: &TestMessage) -> bool {
        if let Some(cycles) = self.slow_nodes.get_mut(&msg.dst) {
            if *cycles > 0 {
                *cycles -= 1;
                return true;
            }
        }
        false
    }

    /// Get the clock offset for a node in microseconds.
    pub fn clock_offset_us(&self, node_id: u64) -> i64 {
        self.clock_offsets.get(&node_id).copied().unwrap_or(0)
    }

    /// Returns true if any nemesis is active.
    pub fn is_active(&self) -> bool {
        !self.partitions.is_empty()
            || !self.killed_nodes.is_empty()
            || !self.paused_nodes.is_empty()
            || !self.slow_nodes.is_empty()
            || !self.clock_offsets.is_empty()
    }
}

// ---------------------------------------------------------------------------
// CqlClient — CQL client wrapper for test operations
// ---------------------------------------------------------------------------

/// A simulated CQL client that reads and writes a single register value
/// through the Accord protocol.
///
/// Each client has a unique ID and targets a specific node in the cluster.
/// Operations go through the full PreAccept -> Commit path.
pub struct CqlClient {
    /// Unique client identifier.
    pub client_id: u64,
    /// The node this client sends requests to.
    pub target_node: u64,
    /// Monotonic sequence for generating unique timestamps.
    next_seq: u64,
}

impl CqlClient {
    pub fn new(client_id: u64, target_node: u64) -> Self {
        Self {
            client_id,
            target_node,
            next_seq: 1,
        }
    }

    /// Generate a unique timestamp for this client.
    fn next_timestamp(&mut self, clock_offset_us: i64) -> Timestamp {
        let base = self.next_seq * 1000;
        let adjusted = if clock_offset_us >= 0 {
            base.saturating_add(clock_offset_us as u64)
        } else {
            base.saturating_sub(clock_offset_us.unsigned_abs())
        };
        self.next_seq += 1;
        Timestamp {
            epoch: 0,
            time: adjusted,
            seq: 0,
            node: self.target_node,
        }
    }

    /// Create a write operation: sends PreAccept messages to all replicas.
    pub fn write_preaccept(
        &mut self,
        key: &[u8],
        cluster_size: u64,
        clock_offset_us: i64,
    ) -> (TxnId, Vec<TestMessage>) {
        let t0 = self.next_timestamp(clock_offset_us);
        let txn_id = TxnId::new(self.target_node, t0);

        let messages: Vec<TestMessage> = (1..=cluster_size)
            .filter(|&dst| dst != self.target_node)
            .map(|dst| TestMessage {
                src: self.target_node,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id,
                    t0,
                    key: key.to_vec(),
                },
            })
            .collect();

        (txn_id, messages)
    }

    /// Create commit messages for all replicas.
    pub fn commit_messages(
        &self,
        txn_id: TxnId,
        t0: Timestamp,
        t: Timestamp,
        deps: Vec<TxnId>,
        cluster_size: u64,
    ) -> Vec<TestMessage> {
        (1..=cluster_size)
            .map(|dst| TestMessage {
                src: self.target_node,
                dst,
                payload: TestMessagePayload::Commit {
                    txn_id,
                    t0,
                    t,
                    deps: deps.clone(),
                },
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// JepsenCluster
// ---------------------------------------------------------------------------

/// A Jepsen-style test cluster combining TestCluster, NemesisController,
/// HistoryRecorder, and CQL clients.
pub struct JepsenCluster {
    /// The underlying Accord protocol test cluster.
    pub cluster: TestCluster,
    /// Nemesis fault injection controller.
    pub nemesis: NemesisController,
    /// Operation history recorder.
    pub recorder: HistoryRecorder,
    /// Register value per node (simulates local storage).
    pub register: HashMap<u64, i64>,
    /// Number of nodes in the cluster.
    pub node_count: u64,
    /// Messages deferred due to slow network.
    deferred_messages: Vec<TestMessage>,
}

impl JepsenCluster {
    /// Provision a new Jepsen cluster with `n` nodes.
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0.
    pub fn new(n: usize) -> Self {
        assert!(n > 0, "cluster must have at least one node");
        Self {
            cluster: TestCluster::new(n),
            nemesis: NemesisController::new(),
            recorder: HistoryRecorder::new(),
            register: HashMap::new(),
            node_count: n as u64,
            deferred_messages: Vec::new(),
        }
    }

    /// Send a message through the nemesis filter.
    ///
    /// Messages may be dropped (partition/kill), buffered (pause), or
    /// deferred (slow network) based on active nemesis state.
    pub fn send_filtered(&mut self, msg: TestMessage) {
        if self.nemesis.should_drop(&msg) {
            return; // Message lost to nemesis.
        }
        if self.nemesis.should_buffer(&msg) {
            self.nemesis.buffer_message(msg);
            return;
        }
        if self.nemesis.should_delay(&msg) {
            self.deferred_messages.push(msg);
            return;
        }
        self.cluster.send(msg);
    }

    /// Deliver the next message (respecting nemesis filters on responses).
    ///
    /// Returns the responses that were actually delivered (not filtered).
    pub fn deliver_next_filtered(&mut self) -> Vec<TestMessage> {
        if self.cluster.pending_count() == 0 {
            return vec![];
        }

        let responses = self.cluster.deliver_next();
        let mut delivered = Vec::new();

        for resp in responses {
            if self.nemesis.should_drop(&resp) {
                continue;
            }
            if self.nemesis.should_buffer(&resp) {
                self.nemesis.buffer_message(resp);
                continue;
            }
            delivered.push(resp);
        }

        delivered
    }

    /// Drain all pending messages through the nemesis filter.
    ///
    /// Uses a bounded iteration limit.
    pub fn drain_filtered(&mut self) {
        const MAX_ITERATIONS: usize = 10_000;
        let mut iterations = 0;
        while self.cluster.pending_count() > 0 {
            assert!(
                iterations < MAX_ITERATIONS,
                "drain_filtered exceeded {} iterations",
                MAX_ITERATIONS
            );
            self.deliver_next_filtered();
            iterations += 1;
        }
    }

    /// Flush deferred (slow-network) messages back into the cluster.
    #[allow(dead_code)] // Part of the nemesis API, used by future slow-network tests.
    pub fn flush_deferred(&mut self) {
        let messages = std::mem::take(&mut self.deferred_messages);
        for msg in messages {
            self.cluster.send(msg);
        }
    }

    /// Execute a full write operation: PreAccept -> collect responses -> Commit.
    ///
    /// Returns the TxnId and whether the write was committed (may fail under nemesis).
    pub fn execute_write(
        &mut self,
        client: &mut CqlClient,
        key: &[u8],
        value: i64,
    ) -> (TxnId, bool) {
        let clock_offset = self.nemesis.clock_offset_us(client.target_node);
        let (txn_id, preaccepts) = client.write_preaccept(key, self.node_count, clock_offset);

        // Record the write invocation.
        let op_idx = self.recorder.invoke(client.client_id, OpType::Write(value));

        // Send PreAccept to all replicas (through nemesis filter).
        for msg in preaccepts {
            self.send_filtered(msg);
        }

        // Also handle PreAccept on the coordinator's own replica.
        let coordinator_replica = self.cluster.replica_mut(client.target_node);
        let t0 = txn_id.0;
        let local_msg = TestMessage {
            src: client.target_node,
            dst: client.target_node,
            payload: TestMessagePayload::PreAccept {
                txn_id,
                t0,
                key: key.to_vec(),
            },
        };
        let local_responses = coordinator_replica.handle(&local_msg);
        // Local responses are just for our bookkeeping, not sent on wire.
        let _ = local_responses;

        // Deliver all pending PreAcceptOK responses.
        self.drain_filtered();

        // Collect PreAcceptOK responses from the coordinator's perspective.
        // In our model, after drain, replicas have processed the PreAccept
        // and their states reflect the result.
        let mut agreed_t = t0;
        let mut all_deps = Vec::new();
        let mut ok_count = 1u64; // Count the coordinator's own vote.

        for replica in &self.cluster.replicas {
            if replica.node_id == client.target_node {
                continue;
            }
            if let Some(state) = replica.txn_states.get(&txn_id) {
                ok_count += 1;
                if state.t > agreed_t {
                    agreed_t = state.t;
                }
                for dep in &state.deps {
                    all_deps.push(*dep);
                }
            }
        }

        // Need a quorum (majority) to commit.
        let quorum = (self.node_count / 2) + 1;
        if ok_count < quorum {
            // Not enough responses — write fails.
            self.recorder.complete(
                op_idx,
                OpResult::Error("insufficient quorum for PreAccept".to_string()),
            );
            return (txn_id, false);
        }

        // Deduplicate deps.
        let deps: Vec<TxnId> = {
            let set: HashSet<TxnId> = all_deps.into_iter().collect();
            set.into_iter().collect()
        };

        // Commit to all replicas.
        let commits = client.commit_messages(txn_id, t0, agreed_t, deps, self.node_count);
        for msg in commits {
            self.send_filtered(msg);
        }
        self.drain_filtered();

        // Update register value.
        // In a real system, the value is determined by execution order.
        // Here we track per-node registers: a committed write updates all
        // non-killed/non-paused nodes.
        for node_id in 1..=self.node_count {
            if !self.nemesis.killed_nodes.contains(&node_id)
                && !self.nemesis.paused_nodes.contains(&node_id)
            {
                if let Some(state) = self.cluster.replica(node_id).txn_states.get(&txn_id) {
                    if state.phase == TxnPhase::Committed {
                        self.register.insert(node_id, value);
                    }
                }
            }
        }

        self.recorder.complete(op_idx, OpResult::WriteOk);
        self.recorder.advance_clock(1000); // 1us per operation
        (txn_id, true)
    }

    /// Execute a linearizable read operation.
    ///
    /// A linearizable read requires quorum reachability: the reading node must
    /// be able to communicate with a majority of nodes to confirm it has the
    /// latest committed value. Under a partition, minority-side reads fail.
    ///
    /// Returns the read value (or None if never written / node unreachable).
    pub fn execute_read(&mut self, client: &CqlClient) -> Option<i64> {
        let op_idx = self.recorder.invoke(client.client_id, OpType::Read);

        // Check if the node itself is reachable.
        if self.nemesis.killed_nodes.contains(&client.target_node)
            || self.nemesis.paused_nodes.contains(&client.target_node)
        {
            self.recorder
                .complete(op_idx, OpResult::Error("node unreachable".to_string()));
            return None;
        }

        // Quorum check: can this node reach a majority?
        // Count how many nodes this node can communicate with (including itself).
        let mut reachable = 1u64; // The node itself.
        let dummy_msg_to = |dst: u64| TestMessage {
            src: client.target_node,
            dst,
            payload: TestMessagePayload::PreAccept {
                txn_id: TxnId::new(0, Timestamp::synthetic(0)),
                t0: Timestamp::synthetic(0),
                key: b"probe".to_vec(),
            },
        };

        for node_id in 1..=self.node_count {
            if node_id == client.target_node {
                continue;
            }
            if self.nemesis.killed_nodes.contains(&node_id)
                || self.nemesis.paused_nodes.contains(&node_id)
            {
                continue;
            }
            // Check if the partition blocks communication.
            let probe = dummy_msg_to(node_id);
            if !self.nemesis.should_drop(&probe) {
                reachable += 1;
            }
        }

        let quorum = (self.node_count / 2) + 1;
        if reachable < quorum {
            self.recorder.complete(
                op_idx,
                OpResult::Error("cannot reach quorum for linearizable read".to_string()),
            );
            return None;
        }

        let value = self.register.get(&client.target_node).copied();
        self.recorder.complete(op_idx, OpResult::ReadOk(value));
        self.recorder.advance_clock(1000);
        value
    }
}

// ---------------------------------------------------------------------------
// Linearizability checker
// ---------------------------------------------------------------------------

/// A simple sequential consistency checker for single-register histories.
///
/// Verifies that there exists a total order of operations consistent with
/// real-time ordering where every read returns the value of the most recent
/// preceding write.
pub struct LinearizabilityChecker;

impl LinearizabilityChecker {
    /// Check whether the given history is linearizable for a single register.
    ///
    /// Algorithm: extract all successful writes and reads, sort by completion
    /// time, and verify each read saw either the last committed write or a
    /// concurrent write.
    ///
    /// Returns `Ok(())` if linearizable, `Err(violation)` otherwise.
    pub fn check(history: &[Operation]) -> Result<(), LinearizabilityViolation> {
        // Extract successful operations only.
        let mut writes: Vec<(u64, i64)> = Vec::new(); // (complete_time, value)
        let mut reads: Vec<(u64, u64, Option<i64>)> = Vec::new(); // (invoke, complete, value)

        for op in history {
            match (&op.op_type, &op.result) {
                (OpType::Write(val), Some(OpResult::WriteOk)) => {
                    writes.push((op.complete_time_ns, *val));
                }
                (OpType::Read, Some(OpResult::ReadOk(val))) => {
                    reads.push((op.invoke_time_ns, op.complete_time_ns, *val));
                }
                _ => {} // Skip failed operations.
            }
        }

        // Sort writes by completion time.
        writes.sort_by_key(|&(t, _)| t);

        // For each read, verify it could have seen a valid write.
        for (read_invoke, read_complete, read_value) in &reads {
            // The read could have been linearized at any point in
            // [read_invoke, read_complete]. Find which writes were
            // visible at any point in that window.
            //
            // A write is visible if it completed before or during the read window.
            // The most recent such write determines the expected value.

            // Find the last write that completed at or before read_complete.
            let last_write_before = writes
                .iter()
                .rev()
                .find(|(wt, _)| *wt <= *read_complete)
                .map(|(_, v)| *v);

            // Also consider writes that are concurrent (overlapping with the read).
            let concurrent_writes: Vec<i64> = writes
                .iter()
                .filter(|(wt, _)| *wt >= *read_invoke && *wt <= *read_complete)
                .map(|(_, v)| *v)
                .collect();

            match read_value {
                None => {
                    // Read returned None — valid only if no write completed before
                    // the read completed. (The register was never written.)
                    if last_write_before.is_some() && concurrent_writes.is_empty() {
                        // There was a write that definitely happened before this read
                        // started, but the read saw None. Check if the write completed
                        // before the read was invoked.
                        let definite_write_before =
                            writes.iter().rev().find(|(wt, _)| *wt < *read_invoke);
                        if definite_write_before.is_some() {
                            return Err(LinearizabilityViolation {
                                read_invoke: *read_invoke,
                                read_complete: *read_complete,
                                read_value: *read_value,
                                expected_candidates: vec![last_write_before],
                            });
                        }
                    }
                }
                Some(rv) => {
                    // The read returned a value. It must match either:
                    // 1. The last write that completed before the read completed, OR
                    // 2. A concurrent write.
                    let mut valid = false;

                    if let Some(lw) = last_write_before {
                        if lw == *rv {
                            valid = true;
                        }
                    }
                    if !valid {
                        for cw in &concurrent_writes {
                            if *cw == *rv {
                                valid = true;
                                break;
                            }
                        }
                    }
                    // Also check: was any write with this value committed at all?
                    if !valid {
                        let any_write_with_value = writes.iter().any(|(_, v)| *v == *rv);
                        if !any_write_with_value {
                            return Err(LinearizabilityViolation {
                                read_invoke: *read_invoke,
                                read_complete: *read_complete,
                                read_value: *read_value,
                                expected_candidates: vec![last_write_before],
                            });
                        }
                        // The value exists but isn't the most recent or concurrent.
                        // This is a stale read — not linearizable.
                        return Err(LinearizabilityViolation {
                            read_invoke: *read_invoke,
                            read_complete: *read_complete,
                            read_value: *read_value,
                            expected_candidates: vec![last_write_before],
                        });
                    }
                }
            }
        }

        Ok(())
    }
}

/// A linearizability violation: a read returned an unexpected value.
#[derive(Debug)]
pub struct LinearizabilityViolation {
    /// When the read was invoked.
    pub read_invoke: u64,
    /// When the read completed.
    pub read_complete: u64,
    /// What the read returned.
    pub read_value: Option<i64>,
    /// What values would have been acceptable.
    pub expected_candidates: Vec<Option<i64>>,
}

impl std::fmt::Display for LinearizabilityViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "linearizability violation: read at [{}, {}] returned {:?}, \
             expected one of {:?}",
            self.read_invoke, self.read_complete, self.read_value, self.expected_candidates
        )
    }
}

impl std::error::Error for LinearizabilityViolation {}

// ---------------------------------------------------------------------------
// Tests (A5.1: 8 tests)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // A5.1-1: jepsen_cluster_provisioning
    #[test]
    fn jepsen_cluster_provisioning() {
        let cluster = JepsenCluster::new(3);

        // Verify 3 nodes were provisioned.
        assert_eq!(cluster.node_count, 3);
        assert_eq!(cluster.cluster.replicas.len(), 3);

        // Verify node IDs are 1, 2, 3.
        for (i, replica) in cluster.cluster.replicas.iter().enumerate() {
            assert_eq!(replica.node_id, (i + 1) as u64);
        }

        // Verify nemesis is not active.
        assert!(!cluster.nemesis.is_active());

        // Verify history is empty.
        assert!(cluster.recorder.history().is_empty());

        // Verify register is empty.
        assert!(cluster.register.is_empty());
    }

    // A5.1-2: jepsen_cql_client
    #[test]
    fn jepsen_cql_client() {
        let mut client = CqlClient::new(1, 1);

        // Client generates PreAccept messages for a 3-node cluster.
        let (txn_id, messages) = client.write_preaccept(b"key1", 3, 0);

        // TxnId should be stamped with client's target node.
        assert_eq!(txn_id.0.node, 1);

        // Should generate messages to nodes 2 and 3 (not self).
        assert_eq!(messages.len(), 2);
        assert!(messages.iter().all(|m| m.src == 1));
        let dsts: HashSet<u64> = messages.iter().map(|m| m.dst).collect();
        assert!(dsts.contains(&2));
        assert!(dsts.contains(&3));

        // All messages should be PreAccept.
        for msg in &messages {
            match &msg.payload {
                TestMessagePayload::PreAccept { key, .. } => {
                    assert_eq!(key, b"key1");
                }
                other => panic!("expected PreAccept, got {:?}", other),
            }
        }

        // Second call should generate a different timestamp.
        let (txn_id2, _) = client.write_preaccept(b"key1", 3, 0);
        assert_ne!(txn_id, txn_id2);
    }

    // A5.1-3: jepsen_nemesis_partition
    #[test]
    fn jepsen_nemesis_partition() {
        let mut nemesis = NemesisController::new();

        // Partition: {1} vs {2, 3}.
        nemesis.inject(NemesisType::Partition {
            side_a: vec![1],
            side_b: vec![2, 3],
        });

        assert!(nemesis.is_active());

        // Messages within the same side should not be dropped.
        let msg_2_to_3 = TestMessage {
            src: 2,
            dst: 3,
            payload: TestMessagePayload::PreAccept {
                txn_id: TxnId::new(2, Timestamp::synthetic(100)),
                t0: Timestamp::synthetic(100),
                key: b"k".to_vec(),
            },
        };
        assert!(!nemesis.should_drop(&msg_2_to_3));

        // Messages crossing the partition should be dropped.
        let msg_1_to_2 = TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::PreAccept {
                txn_id: TxnId::new(1, Timestamp::synthetic(200)),
                t0: Timestamp::synthetic(200),
                key: b"k".to_vec(),
            },
        };
        assert!(nemesis.should_drop(&msg_1_to_2));

        let msg_3_to_1 = TestMessage {
            src: 3,
            dst: 1,
            payload: TestMessagePayload::PreAccept {
                txn_id: TxnId::new(3, Timestamp::synthetic(300)),
                t0: Timestamp::synthetic(300),
                key: b"k".to_vec(),
            },
        };
        assert!(nemesis.should_drop(&msg_3_to_1));

        // Heal partitions.
        nemesis.heal_partitions();
        assert!(!nemesis.should_drop(&msg_1_to_2));
    }

    // A5.1-4: jepsen_nemesis_kill
    #[test]
    fn jepsen_nemesis_kill() {
        let mut nemesis = NemesisController::new();

        nemesis.inject(NemesisType::Kill { node_id: 2 });
        assert!(nemesis.is_active());

        // Messages to killed node are dropped.
        let msg_to_2 = TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::PreAccept {
                txn_id: TxnId::new(1, Timestamp::synthetic(100)),
                t0: Timestamp::synthetic(100),
                key: b"k".to_vec(),
            },
        };
        assert!(nemesis.should_drop(&msg_to_2));

        // Messages from killed node are dropped.
        let msg_from_2 = TestMessage {
            src: 2,
            dst: 1,
            payload: TestMessagePayload::PreAccept {
                txn_id: TxnId::new(2, Timestamp::synthetic(200)),
                t0: Timestamp::synthetic(200),
                key: b"k".to_vec(),
            },
        };
        assert!(nemesis.should_drop(&msg_from_2));

        // Messages between live nodes are not dropped.
        let msg_1_to_3 = TestMessage {
            src: 1,
            dst: 3,
            payload: TestMessagePayload::PreAccept {
                txn_id: TxnId::new(1, Timestamp::synthetic(300)),
                t0: Timestamp::synthetic(300),
                key: b"k".to_vec(),
            },
        };
        assert!(!nemesis.should_drop(&msg_1_to_3));

        // Heal all.
        nemesis.heal_all();
        assert!(!nemesis.should_drop(&msg_to_2));
    }

    // A5.1-5: jepsen_nemesis_slow
    #[test]
    fn jepsen_nemesis_slow() {
        let mut nemesis = NemesisController::new();

        // Slow node 2 by 3 delivery cycles.
        nemesis.inject(NemesisType::Slow {
            node_id: 2,
            delay_cycles: 3,
        });
        assert!(nemesis.is_active());

        let msg = TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::PreAccept {
                txn_id: TxnId::new(1, Timestamp::synthetic(100)),
                t0: Timestamp::synthetic(100),
                key: b"k".to_vec(),
            },
        };

        // First 3 messages should be delayed.
        assert!(nemesis.should_delay(&msg));
        assert!(nemesis.should_delay(&msg));
        assert!(nemesis.should_delay(&msg));

        // After 3 cycles, no more delay.
        assert!(!nemesis.should_delay(&msg));
    }

    // A5.1-6: jepsen_nemesis_clock_skew
    #[test]
    fn jepsen_nemesis_clock_skew() {
        let mut nemesis = NemesisController::new();

        // No offset by default.
        assert_eq!(nemesis.clock_offset_us(1), 0);
        assert_eq!(nemesis.clock_offset_us(2), 0);

        // Inject +500ms skew on node 2.
        nemesis.inject(NemesisType::ClockSkew {
            node_id: 2,
            offset_us: 500_000,
        });
        assert!(nemesis.is_active());

        assert_eq!(nemesis.clock_offset_us(1), 0); // Node 1 unaffected.
        assert_eq!(nemesis.clock_offset_us(2), 500_000); // Node 2 skewed.

        // Inject negative skew on node 3.
        nemesis.inject(NemesisType::ClockSkew {
            node_id: 3,
            offset_us: -200_000,
        });
        assert_eq!(nemesis.clock_offset_us(3), -200_000);

        // CqlClient respects clock offset.
        let mut client = CqlClient::new(1, 2);
        let (txn1, _) = client.write_preaccept(b"k", 3, 0);
        let (txn2, _) = client.write_preaccept(b"k", 3, 500_000);
        // txn2 should have a higher timestamp due to the positive offset.
        assert!(txn2.0.time > txn1.0.time);
    }

    // A5.1-7: jepsen_nemesis_pause
    #[test]
    fn jepsen_nemesis_pause() {
        let mut nemesis = NemesisController::new();

        nemesis.inject(NemesisType::Pause { node_id: 2 });
        assert!(nemesis.is_active());

        let msg = TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::PreAccept {
                txn_id: TxnId::new(1, Timestamp::synthetic(100)),
                t0: Timestamp::synthetic(100),
                key: b"k".to_vec(),
            },
        };

        // Messages to paused node should be buffered.
        assert!(nemesis.should_buffer(&msg));
        assert!(!nemesis.should_drop(&msg)); // Not dropped, just buffered.

        nemesis.buffer_message(msg.clone());

        // Messages to other nodes should not be buffered.
        let msg_to_3 = TestMessage {
            src: 1,
            dst: 3,
            payload: TestMessagePayload::PreAccept {
                txn_id: TxnId::new(1, Timestamp::synthetic(200)),
                t0: Timestamp::synthetic(200),
                key: b"k".to_vec(),
            },
        };
        assert!(!nemesis.should_buffer(&msg_to_3));

        // Resume node 2 — should return buffered messages.
        let buffered = nemesis.resume_node(2);
        assert_eq!(buffered.len(), 1);
        assert_eq!(buffered[0].dst, 2);

        // No longer paused.
        assert!(!nemesis.should_buffer(&msg));
    }

    // A5.1-8: jepsen_history_recording
    #[test]
    fn jepsen_history_recording() {
        let mut recorder = HistoryRecorder::new();

        assert_eq!(recorder.now(), 0);

        // Record a write.
        let w_idx = recorder.invoke(1, OpType::Write(42));
        assert_eq!(w_idx, 0);
        assert_eq!(recorder.history()[0].invoke_time_ns, 0);
        assert!(recorder.history()[0].result.is_none());

        recorder.advance_clock(1000);
        recorder.complete(w_idx, OpResult::WriteOk);
        assert_eq!(recorder.history()[0].complete_time_ns, 1000);
        assert_eq!(recorder.history()[0].result, Some(OpResult::WriteOk));

        // Record a read.
        recorder.advance_clock(500);
        let r_idx = recorder.invoke(2, OpType::Read);
        assert_eq!(r_idx, 1);
        assert_eq!(recorder.history()[1].invoke_time_ns, 1500);

        recorder.advance_clock(200);
        recorder.complete(r_idx, OpResult::ReadOk(Some(42)));
        assert_eq!(recorder.history()[1].complete_time_ns, 1700);

        // Record a failed operation.
        recorder.advance_clock(300);
        let f_idx = recorder.invoke(1, OpType::Write(99));
        recorder.advance_clock(100);
        recorder.complete(f_idx, OpResult::Error("timeout".to_string()));

        // Check completed ops.
        let completed = recorder.completed_ops();
        assert_eq!(completed.len(), 3);

        // Check total history.
        assert_eq!(recorder.history().len(), 3);
    }
}
