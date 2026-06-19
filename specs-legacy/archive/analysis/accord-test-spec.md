# Test Specification — Accord Distributed Transactions

> Last updated: 2026-03-21
> Status: Draft

## Design Philosophy

The 24-step EPaxos correctness test is the capstone. It exercises the interaction
between PreAccept, Accept, Recover, and ballot management across multiple replicas
with carefully ordered message delivery. If any component below it is wrong, the
24-step test either won't compile, won't pass, or will pass vacuously (testing the
wrong thing).

This specification builds a test pyramid that makes the 24-step test's success
*predictable* — not lucky. Each layer proves a property that the layer above depends on.

```
                    ┌─────────────────────────┐
                    │  24-Step EPaxos Test     │  ← capstone
                    │  (system correctness)    │
                    └────────────┬────────────┘
                   ┌─────────────┴─────────────┐
                   │  Protocol Scenario Tests   │  ← composed handlers
                   │  (multi-handler sequences) │
                   └─────────────┬─────────────┘
              ┌──────────────────┴──────────────────┐
              │  Handler Contract Tests              │  ← each handler in isolation
              │  (PreAccept, Accept, Commit, Recover)│
              └──────────────────┬──────────────────┘
         ┌───────────────────────┴───────────────────────┐
         │  State Machine Invariant Tests                 │  ← TxnState transitions
         │  (phase ordering, ballot rules, dep-set rules) │
         └───────────────────────┬───────────────────────┘
    ┌────────────────────────────┴────────────────────────────┐
    │  Data Structure Unit Tests                               │  ← types in isolation
    │  (Timestamp, Ballot, ConflictIndex, HLC, ReorderBuffer)  │
    └──────────────────────────────────────────────────────────┘
```

---

## Layer 1: Data Structure Unit Tests

These tests verify that each type has the mathematical properties the protocol
requires. No protocol logic, no message passing. Pure data structure behavior.

### 1.1 Timestamp

The protocol's correctness depends on Timestamp being a total order consistent
with the field layout. If `PartialOrd` disagrees with manual field comparison,
every conflict check is wrong.

| Test | What It Proves | How |
|------|---------------|-----|
| `timestamp_total_order_all_fields` | `Ord` is consistent across all 4 fields | Generate pairs where each field individually differs. Assert `a.cmp(&b)` matches the expected `epoch > time > seq > node` priority. Cover: same epoch different time, same time different seq, same seq different node. |
| `timestamp_eq_requires_all_fields` | Equality requires all 4 fields match | Two timestamps with same (epoch, time, seq) but different node must be `!=`. Two timestamps with all fields identical must be `==`. |
| `timestamp_bump_past_strictly_greater` | `bump_past` always returns `t > other` | For 1000 random timestamp pairs, `a.bump_past(&b, node)` must satisfy `result > b`. Edge cases: `seq` at `u32::MAX` (saturating), `time` at `u64::MAX`. |
| `timestamp_bump_past_preserves_epoch` | `bump_past` does not change epoch | Bumping across epochs would violate electorate scoping. Assert `result.epoch == other.epoch`. |
| `timestamp_new_seq_zero` | `Timestamp::new()` always starts with `seq=0` | Constructor invariant. If seq starts nonzero, timestamp space is wasted. |
| `timestamp_uniqueness_same_nanosecond` | Two timestamps from same node in same ns differ | Call `Timestamp::new(epoch, same_time, node)` twice with seq increment. Assert `t1 != t2`. |
| `timestamp_derive_hash_consistent_with_eq` | Hash is consistent with Eq | Required for use as HashMap key. Two equal timestamps must have equal hashes. Two unequal timestamps should (usually) have different hashes. |

**Why this matters for the 24-step test:** Steps 3, 7, 11, 13 all compare timestamps
to determine ordering. If `Ord` is wrong, the wrong transaction "wins" and the
test passes or fails for the wrong reason.

### 1.2 Ballot Types

The 24-step test's entire point is that `AcceptedBallot` and `PromisedBallot`
must be distinct. These tests prove the type system enforces this.

| Test | What It Proves | How |
|------|---------------|-----|
| `ballot_accepted_and_promised_are_distinct_types` | Compiler rejects mixing | Write a function `fn select_best(responses: &[RecoverOK]) -> AcceptedBallot` that returns the max `accepted_ballot`. Attempt to pass a `PromisedBallot` value — this must be a compile error. Implement as a `trybuild` or `compile_fail` test. |
| `ballot_accepted_ord` | `AcceptedBallot` has correct total order | Compare ballots with same and different inner values. Assert ordering matches inner `BallotNumber` ordering. |
| `ballot_promised_ord` | `PromisedBallot` has correct total order | Same as above for `PromisedBallot`. |
| `ballot_number_monotonic_generation` | `fresh_ballot()` always increases | Call `fresh_ballot()` 1000 times. Each result must be strictly greater than the previous. |
| `ballot_zero_is_initial` | Ballot 0 means "no ballot seen/accepted" | `BallotNumber::default()` returns 0. All generated ballots are > 0. |
| `ballot_nack_returns_promised` | NACK carries `PromisedBallot`, not `AcceptedBallot` | Type-check that the NACK message struct contains `PromisedBallot`. If someone puts `AcceptedBallot` in NACK, the Recover handler will misinterpret it. |

**Why this matters for the 24-step test:** Step 16 is where the bug manifests.
`p3` updates `max_ballot_seen=4` but does NOT re-vote. With a single field,
`accepted_ballot` is corrupted to 4. With distinct types, this is impossible
to write incorrectly — the compiler catches it.

### 1.3 TxnState Invariants

TxnState is the per-replica, per-transaction record. Its invariants are the
foundation of protocol correctness.

| Test | What It Proves | How |
|------|---------------|-----|
| `txnstate_accepted_leq_promised` | `accepted_ballot <= max_ballot_seen` always holds | Create TxnState. Call every mutation method in random order (set_pre_accepted, set_accepted, join_ballot, etc.) for 10,000 iterations. Assert invariant after each mutation. |
| `txnstate_phase_mutual_exclusion` | Only one phase flag is true at a time | Set `pre_accepted=true`. Then set `accepted=true`. Assert `pre_accepted` is now `false`. Repeat for all transitions: pre_accepted→accepted→committed→applied. |
| `txnstate_phase_ordering` | Phases only advance forward | Attempt `committed→pre_accepted` transition. Must be rejected (return error or no-op). Attempt `applied→accepted`. Same. Only forward transitions allowed. |
| `txnstate_join_ballot_updates_promised_only` | `join_ballot(b)` updates `max_ballot_seen` but NOT `accepted_ballot` | Create TxnState with `accepted_ballot=2`. Call `join_ballot(4)`. Assert `max_ballot_seen==4` AND `accepted_ballot==2` (unchanged). This is the exact invariant the 24-step test exercises at step 16. |
| `txnstate_accept_updates_both` | `accept(ballot, t, deps)` updates both `accepted_ballot` AND `max_ballot_seen` | Create TxnState. Call `accept(ballot=3, ...)`. Assert both fields are updated. `max_ballot_seen >= accepted_ballot`. |
| `txnstate_deps_union_on_preaccept` | PreAccept unions deps from conflict index | Start with empty deps. PreAccept with conflicting txn γ. Assert `γ ∈ deps`. |
| `txnstate_deps_filter_preaccept_uses_t0` | PreAccept dep filter uses `t0`, not `t` | Two txns: γ with `t0_γ=5`, τ with `t0_τ=10`. ConflictIndex reports γ as conflicting. PreAccept for τ should include γ in deps (because `t0_γ < t0_τ`). |
| `txnstate_deps_filter_accept_uses_t` | Accept dep filter uses `t`, not `t0` | Same setup but now τ has been bumped: `t0_τ=10`, `t_τ=15`. Accept should include γ if `t0_γ < t_τ` (i.e., `5 < 15`). This is a different filter than PreAccept. |
| `txnstate_default_ballots_are_zero` | Fresh TxnState has both ballots at 0 | Constructor invariant. Recovery logic checks "has any ballot been accepted" by testing `accepted_ballot > 0`. |

**Why this matters for the 24-step test:** The test exercises `join_ballot` (step 16)
and `accept` (steps 7, 13) in sequence. If `join_ballot` incorrectly touches
`accepted_ballot`, step 21's selection logic picks the wrong value.

### 1.4 ConflictIndex

| Test | What It Proves | How |
|------|---------------|-----|
| `conflict_index_single_key_register_lookup` | O(1) register and lookup for single-key writes | Register txn T1 on key K. `max_conflicting_timestamp(K)` returns `T1.t0`. Register T2 on K with higher t0. Returns `T2.t0`. |
| `conflict_index_single_key_no_false_positives` | Non-overlapping keys don't conflict | Register T1 on key A. `max_conflicting_timestamp(B)` returns `None`. |
| `conflict_index_range_overlap_detection` | Range operations detect overlapping ranges | Register T1 on range `[100, 200]`. Query with range `[150, 250]`. Must detect conflict. Query with range `[201, 300]`. Must NOT detect conflict. |
| `conflict_index_deps_before_t0_filter` | `deps_before_t0` returns only txns with `t0_γ < t0_τ` | Register T1 (t0=5), T2 (t0=10), T3 (t0=15). Query `deps_before_t0(t0=12)`. Must return `{T1, T2}` but NOT T3. |
| `conflict_index_deps_before_t_filter` | `deps_before_t` uses `t` not `t0` | Register T1 (t0=5), T2 (t0=10). Query `deps_before_t(t=8)`. Must return `{T1}` but NOT T2 (because `t0_T2=10 > t=8`). |
| `conflict_index_remove_after_applied` | Applied txns are removed from index | Register T1. Remove T1. `max_conflicting_timestamp` returns `None`. |
| `conflict_index_bounded_capacity` | Hard cap enforced; evicts only fully-applied entries | Fill to capacity with registered txns. Register one more. Must evict an applied entry, NOT a pre_accepted or committed entry. If no applied entries exist, return `Overloaded`. |
| `conflict_index_concurrent_single_threaded` | Single-threaded per shard — no races | Verify that `ConflictIndex` is `!Sync` or that all access goes through a single-threaded shard executor. Two concurrent `register` calls on the same shard must be sequenced. |
| `conflict_index_indexed_writes_projection` | `indexed_writes` tracks column projections | Register T1 writing column C with value V. `inflight_writing(C, V)` returns T1. Register T2 writing column C with value W. `inflight_writing(C, V)` still returns only T1. |

**Why this matters for the 24-step test:** Steps 3 and 12 involve PreAccept handlers
computing dep sets via the ConflictIndex. If `deps_before_t0` is wrong, the dep
sets in the test don't match expectations, and the linearizability assertion at
step 24 checks the wrong thing.

### 1.5 HybridLogicalClock

| Test | What It Proves | How |
|------|---------------|-----|
| `hlc_monotonic_forward` | HLC never returns a value <= previous | Call `hlc.now()` 10,000 times. Each must be strictly greater than the previous, even if wall clock doesn't advance (logical component increments). |
| `hlc_advances_with_wall_clock` | Physical component tracks real time | Call `hlc.now()`, sleep 1ms, call `hlc.now()`. Second result's `time` field must be >= first + ~1ms. |
| `hlc_merge_advances_past_remote` | `hlc.merge(remote_ts)` advances local HLC past remote | Set local HLC to time=100. Merge with remote time=200. Next `hlc.now()` must return time >= 200. |
| `hlc_merge_rejects_excessive_drift` | Remote timestamp > `MAX_CLOCK_DRIFT` rejected | Local HLC at time=100. Attempt merge with time=100+MAX_CLOCK_DRIFT+1. Must return error. Local HLC must NOT advance. |
| `hlc_wall_clock_regression_detected` | Backward jump in `SystemTime::now()` detected | Mock clock that returns time=100, then time=50. HLC must detect this, log error, and continue using logical increment (not regress). |
| `hlc_seq_increments_within_same_ns` | Multiple timestamps in same nanosecond use seq | Mock clock that always returns the same time. Three calls to `hlc.now()` must produce timestamps with seq=0, seq=1, seq=2. |

### 1.6 ReorderBuffer

| Test | What It Proves | How |
|------|---------------|-----|
| `reorder_buffer_delivers_in_t0_order` | Messages delivered in timestamp order, not arrival order | Enqueue PreAccept with t0=10 (arrives first), then PreAccept with t0=5 (arrives second). First delivery must be t0=5. |
| `reorder_buffer_deadline_formula` | Deadline matches spec §7.1 | Set SkewMax=10ms, Latency(C,P)=2ms, max(Latency(C',P))=5ms. Deadline for t0 at wall_clock=100 must be `100 + 10 + 5 - 2 = 113`. |
| `reorder_buffer_releases_after_deadline` | Message held until deadline, then released | Enqueue message. Assert not delivered before deadline. Advance mock clock past deadline. Assert delivered. |
| `reorder_buffer_overflow_backpressure` | Overflow returns error, not message loss | Fill to capacity. Enqueue one more. Must return `Err(Overloaded)`, not silently drop. |
| `reorder_buffer_empty_after_drain` | All messages eventually delivered | Enqueue 100 messages. Advance clock far enough. All 100 must be delivered. Buffer empty. |

---

## Layer 2: State Machine Invariant Tests

These tests verify TxnState transitions in the context of the protocol state machine,
but without network or message serialization. They use a `TestReplica` that wraps
TxnState and exposes handler methods.

### 2.1 Phase Transition Properties

| Test | What It Proves | How |
|------|---------------|-----|
| `sm_preaccept_sets_pre_accepted` | PreAccept handler sets phase flag correctly | Create fresh TxnState. Call `handle_preaccept(t0, payload)`. Assert `pre_accepted==true`, all other flags false. |
| `sm_accept_clears_preaccept` | Accept clears pre_accepted, sets accepted | Pre-accept a txn. Then `handle_accept(ballot, t, deps)`. Assert `pre_accepted==false`, `accepted==true`. |
| `sm_commit_clears_accepted` | Commit clears accepted, sets committed | Accept a txn. Then `handle_commit(t, deps)`. Assert `accepted==false`, `committed==true`. |
| `sm_apply_clears_committed` | Apply clears committed, sets applied | Commit a txn. Then `handle_apply(t, deps, result)`. Assert `committed==false`, `applied==true`. |
| `sm_idempotent_preaccept` | Duplicate PreAccept returns same response | PreAccept txn T1. PreAccept T1 again with same params. Must return same (t, deps), not create a second entry. |
| `sm_idempotent_commit` | Duplicate Commit is no-op | Commit txn T1. Commit T1 again. No error, no state change. |
| `sm_reject_preaccept_after_accept` | PreAccept rejected if already accepted | Accept txn T1 at ballot 2. Send PreAccept for T1 at ballot 0. Must be rejected (NACK or ignored). |
| `sm_reject_accept_lower_ballot` | Accept with lower ballot rejected | Accept txn T1 at ballot 3. Send Accept for T1 at ballot 2. Must return NACK with `max_ballot_seen=3`. |

### 2.2 Ballot Management (Critical Path to 24-Step)

These tests directly verify the properties that the 24-step test depends on.
Each test name maps to a specific step in the 24-step sequence.

| Test | What It Proves | Step Coverage | How |
|------|---------------|--------------|-----|
| `sm_preaccept_ballot_zero` | Initial PreAccept uses ballot 0 | Steps 1-2 | PreAccept a txn. Assert `max_ballot_seen==0`, `accepted_ballot==0`. |
| `sm_recover_updates_promised_only` | Recover(ballot) updates `max_ballot_seen` but not `accepted_ballot` | **Step 16** | Pre-accept at ballot 0. Then `handle_recover(ballot=4)`. Assert `max_ballot_seen==4` AND `accepted_ballot==0`. This is THE critical invariant. |
| `sm_accept_updates_accepted_ballot` | Accept updates `accepted_ballot` | Steps 7, 13 | Pre-accept at ballot 0. Then `handle_accept(ballot=2, t, deps)`. Assert `accepted_ballot==2`. |
| `sm_recover_after_accept_preserves_accepted` | Recover after Accept preserves `accepted_ballot` | Steps 16-17 | Accept at ballot 2 (so `accepted_ballot=2`). Then Recover at ballot 4 (only updates `max_ballot_seen`). Then Recover at ballot 5. Replica reports `accepted_ballot=2` in RecoverOK, NOT 4 or 5. |
| `sm_recovery_selection_uses_accepted_ballot` | Recovery coordinator selects by `max(accepted_ballot)` | **Step 21** | Collect RecoverOK from 3 replicas: p1 (accepted_ballot=0), p2 (accepted_ballot=3), p3 (accepted_ballot=2). Recovery must select p2's value (ballot 3 is highest), NOT p3's despite p3 having higher `max_ballot_seen`. |
| `sm_nack_carries_promised_ballot` | NACK response contains `max_ballot_seen` | Steps 4-6 | Replica has `max_ballot_seen=5`. Receive Accept with ballot=3. NACK must carry ballot 5 so the sender knows to use a higher ballot. |
| `sm_higher_ballot_preempts_lower` | A higher ballot always preempts | Steps 8-10 | Accept at ballot 2. Then Recover at ballot 3. Replica must honor ballot 3 (update `max_ballot_seen=3`). Subsequent Accept at ballot 2 is NACKed. |

### 2.3 Dependency Set Correctness

| Test | What It Proves | How |
|------|---------------|-----|
| `sm_preaccept_deps_from_conflict_index` | PreAccept computes deps from ConflictIndex | Register conflicting txn γ (t0=5) in ConflictIndex. PreAccept τ (t0=10). τ's deps must contain γ. |
| `sm_preaccept_deps_use_t0_not_t` | PreAccept dep filter is `t0_γ < t0_τ` | Register γ (t0=5, bumped t=15) and δ (t0=12). PreAccept τ (t0=10). Deps must contain γ (t0_γ=5 < 10) but NOT δ (t0_δ=12 > 10). Even though γ's t=15 > t0_τ=10, the dep filter uses t0. |
| `sm_accept_deps_use_t_not_t0` | Accept dep filter is `t0_γ < t_τ` | Same setup but now τ has been bumped to t=15. Accept for τ at t=15 must include δ (t0_δ=12 < 15) in deps. This is the key difference from PreAccept. |
| `sm_deps_union_across_quorum` | Coordinator unions deps from all PreAcceptOK responses | Replica A reports deps={γ}. Replica B reports deps={δ}. Coordinator's final deps must be {γ, δ}. |
| `sm_deps_superset_is_safe` | Extra deps are safe (unnecessary waits, not incorrect) | Include an extra dep ε that doesn't conflict. Assert the transaction still commits correctly — it just waits for ε unnecessarily. No correctness violation. |
| `sm_deps_missing_is_unsafe` | Missing dep causes serializability violation | Two conflicting txns T1 and T2. T2 commits with deps={} (T1 missing). T2 executes before T1. Read of T2's result does not reflect T1's write. Assert this is detected as a violation. |

### 2.4 Persist-Before-Reply

| Test | What It Proves | How |
|------|---------------|-----|
| `sm_preaccept_persists_before_reply` | AccordPreAccepted written to protocol log before PreAcceptOK sent | Instrument protocol log with a counter. Call `handle_preaccept`. Assert log write count incremented BEFORE the response is constructed. Use a mock log that records call ordering. |
| `sm_accept_persists_before_reply` | AccordAccepted written before AcceptOK | Same pattern. |
| `sm_apply_persists_before_flag` | AccordApplied + data written to main log before `applied=true` | Instrument main commit log. Call `handle_apply`. Assert log write before flag set. If the process crashes between write and flag, recovery replays the write. |
| `sm_crash_between_persist_and_flag` | Crash after persist, before flag, is recoverable | Write AccordApplied to log. Crash (don't set flag). Replay log. TxnState reconstructed with `applied=true`. No duplicate application. |

---

## Layer 3: Handler Contract Tests

These tests exercise individual protocol handlers end-to-end, including message
serialization, ConflictIndex interaction, and protocol log writes. Each handler
is tested with a `TestReplica` that has a real ConflictIndex and mock network.

### 3.1 PreAccept Handler

| Test | Scenario | Expected Behavior |
|------|----------|-------------------|
| `preaccept_no_conflict_fast_path` | No conflicting txns in ConflictIndex | Returns `t == t0` (agrees with coordinator's proposed timestamp). Deps may be empty. |
| `preaccept_conflict_bumps_timestamp` | Conflicting txn with higher t0 in index | Returns `t > t0` (proposes higher timestamp). `t = max_conflict.bump_past()`. |
| `preaccept_registers_in_conflict_index` | After PreAccept, txn appears in index | Call PreAccept. Then `conflict_index.max_conflicting_timestamp(same_key)` returns this txn's t0. |
| `preaccept_nack_if_higher_ballot_seen` | `max_ballot_seen > 0` for this txn_id | Returns NACK with the higher ballot. Does not re-process. |
| `preaccept_idempotent_on_duplicate` | Same PreAccept received twice | Second call returns same response. ConflictIndex has one entry, not two. |
| `preaccept_epoch_mismatch` | Replica at epoch 2, PreAccept from epoch 1 | Returns PreAcceptOK with `t.epoch=2` to signal coordinator should fetch new config. |

### 3.2 Accept Handler

| Test | Scenario | Expected Behavior |
|------|----------|-------------------|
| `accept_normal` | ballot >= max_ballot_seen | Updates `accepted_ballot`, `t`, `deps`. Returns AcceptOK. |
| `accept_nack_lower_ballot` | ballot < max_ballot_seen | Returns NACK. State unchanged. |
| `accept_after_preaccept` | PreAccept then Accept for same txn | Clears pre_accepted, sets accepted. Deps recomputed with `t` filter (not `t0`). |
| `accept_skipped_preaccept` | Accept arrives without prior PreAccept | Valid — replica may have missed PreAccept. Accept should work. |
| `accept_after_commit` | Accept for already-committed txn | Ignored. Already past this phase. |

### 3.3 Commit Handler

| Test | Scenario | Expected Behavior |
|------|----------|-------------------|
| `commit_sets_final_timestamp` | Normal Commit | `t` is set to committed timestamp. `committed=true`. Wakes dep-waiters. |
| `commit_idempotent` | Duplicate Commit | No-op. No error. |
| `commit_wakes_dep_waiters` | Txn T2 waiting on T1 to commit | T1 commits. T2's dep-wait resolves. T2 can proceed to Execute. |

### 3.4 Recover Handler

| Test | Scenario | Expected Behavior |
|------|----------|-------------------|
| `recover_updates_promised_not_accepted` | Recover(ballot=4) on replica with accepted_ballot=2 | `max_ballot_seen=4`. `accepted_ballot` unchanged at 2. RecoverOK reports `accepted_ballot=2`. |
| `recover_nack_lower_ballot` | Recover(ballot=2) but max_ballot_seen=3 | Returns NACK(3). |
| `recover_runs_preaccept_if_unseen` | Recover for txn this replica never saw | Runs PreAccept handler locally first, then responds. |
| `recover_reports_superseding_txns` | Conflicting txn with higher t0 accepted | Reports superseding set in RecoverOK. |
| `recover_reports_waiting_txns` | Conflicting txn with lower t0 but higher t | Reports wait set. Recovery coordinator must await these before deciding. |

---

## Layer 4: Protocol Scenario Tests

These tests compose multiple handlers across multiple simulated replicas.
They use a `TestCluster` of 3+ `TestReplica` instances with a deterministic
message scheduler (no real network, no tokio, no timers).

### 4.1 Deterministic Message Scheduler

```rust
struct TestCluster {
    replicas: Vec<TestReplica>,
    pending_messages: VecDeque<(NodeId, NodeId, Message)>,
}

impl TestCluster {
    /// Enqueue a message from src to dst. Not delivered until deliver() called.
    fn send(&mut self, src: NodeId, dst: NodeId, msg: Message);

    /// Deliver the next message in FIFO order to its destination.
    /// Returns the response message(s) generated.
    fn deliver_next(&mut self) -> Vec<(NodeId, NodeId, Message)>;

    /// Deliver a specific message (by index) — for out-of-order scenarios.
    fn deliver_at(&mut self, index: usize) -> Vec<(NodeId, NodeId, Message)>;

    /// Deliver all pending messages until quiescent.
    fn drain(&mut self);

    /// Drop a message (simulate network partition).
    fn drop_at(&mut self, index: usize);

    /// Assert all replicas agree on committed state for a txn.
    fn assert_consistent(&self, txn_id: &TxnId);
}
```

This scheduler is the foundation of all scenario tests. It makes message ordering
deterministic and explicit — exactly what the 24-step test requires.

### 4.2 Happy Path Scenarios

| Test | Setup | Sequence | Assertion |
|------|-------|----------|-----------|
| `scenario_fast_path_no_conflict` | 3 replicas, 1 txn, no conflicts | Coordinator sends PreAccept to all 3. All respond with t==t0. Coordinator sends Commit. | All 3 replicas have `committed==true` with same (t, deps). |
| `scenario_fast_path_with_leaseholder` | 3 replicas, coordinator is leaseholder | Coordinator does local conflict check (no network). Sends PreAccept to 2 others. Both agree. | Commit in 1 RTT. Coordinator applied locally before sending Apply. |
| `scenario_slow_path_conflict` | 3 replicas, 1 txn, 1 replica has conflict | Coordinator sends PreAccept. 2 agree on t0, 1 proposes higher t. Not enough for fast quorum. | Coordinator sends Accept with `t=max(proposals)`. Then Commit. 2 RTTs total. |
| `scenario_two_concurrent_no_conflict` | 3 replicas, 2 txns on different keys | Both txns run PreAccept concurrently. No conflicts. | Both commit via fast path. Neither appears in the other's deps. |
| `scenario_two_concurrent_same_key` | 3 replicas, 2 txns on same key | T1 starts first (lower t0). T2 sees T1 in ConflictIndex. | T2's deps contain T1. T2 executes after T1 applies. Both commit. |

### 4.3 Failure and Recovery Scenarios

| Test | Setup | Sequence | Assertion |
|------|-------|----------|-----------|
| `scenario_coordinator_crash_after_preaccept` | 3 replicas, coordinator crashes after sending PreAccept | Deliver PreAccept to 2 replicas. Coordinator "crashes" (stops processing). Recovery triggered. | Recovery coordinator completes the protocol. Txn commits with correct (t, deps). |
| `scenario_coordinator_crash_after_accept` | Coordinator crashes after sending Accept to 1 replica | 1 replica has `accepted_ballot=1`. Others have only PreAccept state. Recovery triggered. | Recovery finds accepted state, re-proposes with higher ballot. Same value committed. |
| `scenario_coordinator_crash_after_commit` | Coordinator crashes after Commit reaches 1 replica | 1 replica committed, 2 not. Recovery triggered. | Recovery finds committed state, broadcasts Commit to remaining replicas. |
| `scenario_recovery_with_no_accepted_state` | Recovery triggered but no replica accepted | All replicas only have PreAccept state. No accepted or committed state anywhere. | Recovery uses safe timestamp determination (§5 lines 18-25). Begins Accept round. |
| `scenario_recovery_with_superseding_txn` | Recovery triggered, but a conflicting txn has been accepted with higher t | Superseding set is non-empty. | Recovery bumps t to be above the superseding txn. Accepts with new t. |
| `scenario_recovery_with_wait_set` | Recovery triggered, conflicting txn is accepted but not committed | Wait set is non-empty. | Recovery waits for conflicting txn to commit, then restarts recovery. |

### 4.4 Multi-Recovery Scenarios (Building to 24-Step)

These tests exercise specific sub-sequences of the 24-step test to isolate
each mechanism. If any of these fail, the 24-step test will also fail, and
these simpler tests make debugging easier.

| Test | Sub-Sequence | What It Isolates |
|------|-------------|-----------------|
| `scenario_two_recoveries_same_txn` | Steps 4-7, then steps 8-13 | Two different nodes each initiate recovery for the same txn with increasing ballots. Second recovery must see first's accepted state. |
| `scenario_recover_after_accept_different_ballot` | Steps 7, 15-16 | Replica accepted at ballot 2. Recovery at ballot 4 arrives. Replica updates `max_ballot_seen=4` but `accepted_ballot` stays at 2. RecoverOK reports `accepted_ballot=2`. |
| `scenario_recover_selects_highest_accepted_ballot` | Steps 21 | Recovery coordinator collects RecoverOK from replicas with different `accepted_ballot` values. Must select the value from the replica with the highest `accepted_ballot`. |
| `scenario_duplicate_recover_is_idempotent` | Step 20 | Same Recover message delivered twice. Second delivery produces same RecoverOK. No state corruption. |
| `scenario_three_recoveries_escalating_ballots` | Steps 4-7, 8-13, 15-21 | Three recovery attempts with ballots 2, 3, 4+5. Each must correctly preserve `accepted_ballot` from its own Accept round while advancing `max_ballot_seen`. Final recovery selects the right value. |

---

## Layer 5: The 24-Step EPaxos Correctness Test

This is the capstone test. It encodes the exact counter-example from Sutra (2019).
Every layer below must pass for this test to be meaningful.

### 5.1 Test Infrastructure

```rust
#[test]
fn epaxos_24_step_linearizability() {
    // 3 replicas
    let mut cluster = TestCluster::new(3); // p1, p2, p3

    // 2 conflicting transactions on the same key range
    let c1 = TxnId::new(/* ... */);
    let c2 = TxnId::new(/* ... */);
    // c1 and c2 write to the same partition key

    // All clocks and timers are synthetic — deterministic, not wall-clock
    // No tokio runtime, no async, no randomness
    // ...
}
```

### 5.2 Step-by-Step Verification

The test doesn't just check the final assertion. It checks intermediate state
after EVERY step to catch bugs early and produce clear diagnostics.

| Step | Action | Intermediate Assertion |
|------|--------|----------------------|
| 1 | p3 sends PreAccept(c1) to {p1, p2, p3} | 3 pending PreAccept messages in scheduler |
| 2 | p1 sends PreAccept(c2) to {p1, p2, p3} | 3 more pending messages (6 total) |
| 3 | Deliver PreAccept(c2) at p3 | p3 sees c1 in ConflictIndex. p3 replies PreAcceptOK(c2, deps={c1}). Assert deps contains c1. |
| 4 | p3 sends Recover(ballot=2, c2) to {p2, p3} | Recovery initiated. 2 Recover messages pending. |
| 5 | Deliver Recover at p2 | p2 promises ballot=2. `p2.max_ballot_seen[c2]==2`. RecoverOK sent. |
| 6 | Deliver Recover at p3 | p3 promises ballot=2. `p3.max_ballot_seen[c2]==2`. RecoverOK includes deps={c1}, `accepted_ballot==0`. |
| 7 | p3 (recovery coord) collects → sends Accept(ballot=2, c2, deps={c1}) to {p2, p3} | Assert: p3 chose deps={c1} because no accepted state existed. `accepted_ballot==0` for both respondents. |
| 8 | p2 sends Recover(ballot=3, c2) to {p1, p2} | Second recovery attempt with higher ballot. |
| 9 | Deliver Recover(ballot=3) at p1 | p1 promises ballot=3. `p1.max_ballot_seen[c2]==3`. |
| 10 | Deliver Recover(ballot=3) at p2 | p2 promises ballot=3. `p2.max_ballot_seen[c2]==3`. |
| 11 | p2 (recovery coord) collects → sees no accepted state → sends Accept(ballot=3, c2, deps={}) | **Assert: p2 sees `accepted_ballot==0` from both p1 and p2.** p1 never saw c1, so deps={}. |
| 12 | Deliver PreAcceptOK(c2) at p1 | p1 responds with deps={} (p1 never saw c1). |
| 13 | Deliver Accept(ballot=3, deps={}) at {p1, p2} | p1 and p2 accept. `p1.accepted_ballot[c2]==3`. `p2.accepted_ballot[c2]==3`. Deps={}. |
| 14 | p1 sends AcceptOK(ballot=3) | Acknowledgment. |
| 15 | p1 sends Recover(ballot=4, c2) to {p1, p3} | Third recovery. |
| 16 | Deliver Recover(ballot=4) at p3 | **THE CRITICAL STEP.** p3 updates `max_ballot_seen[c2]=4`. **Assert: `p3.accepted_ballot[c2]` is still 2** (from step 7). With the bug: `accepted_ballot` would be corrupted to 4. |
| 17 | p1 sends Recover(ballot=5, c2) to {p1, p3} | Fourth recovery. |
| 18 | Deliver Recover(ballot=5) at p3 | `p3.max_ballot_seen=5`. `p3.accepted_ballot` still 2. |
| 19 | Deliver Recover(ballot=5) at p1 | `p1.max_ballot_seen=5`. `p1.accepted_ballot=3` (from step 13). |
| 20 | Deliver duplicate Recover(ballot=5) at p1 | Idempotent. Same RecoverOK. No state change. |
| 21 | p1 (recovery coord) finalizes | **Collects: p3.accepted_ballot=2, p1.accepted_ballot=3. Selects p1 (ballot 3 > ballot 2). Picks deps={}.** With bug: p3.accepted_ballot=4 > p1.accepted_ballot=3, picks deps={c1}. WRONG. |
| 22 | p3 replies AcceptOK(ballot=5) | Acceptance of recovery proposal. |
| 23 | p2 commits dep(c2)={} | From step 13 recovery. |
| 24 | p1 commits dep(c2)={} or dep(c2)={c1} | **With correct implementation: deps={}.** With bug: deps={c1}. |

### 5.3 Final Assertions

```rust
// PRIMARY ASSERTION: all replicas agree on committed deps
assert_eq!(
    cluster.replica(0).committed_deps(&c2),
    cluster.replica(1).committed_deps(&c2),
    "p1 and p2 must agree on committed deps for c2"
);

// SECONDARY: linearizability check
// If deps disagree, one replica executes c1 before c2 and the other
// executes c2 before c1. This is a linearizability violation.
assert!(
    cluster.linearizability_check(&[c1, c2]),
    "execution order must be linearizable"
);

// DIAGNOSTIC: print ballot history if assertion fails
// This makes debugging much easier than a bare assertion failure.
for (i, replica) in cluster.replicas.iter().enumerate() {
    let state = replica.txn_state(&c2);
    eprintln!(
        "p{}: accepted_ballot={:?}, max_ballot_seen={:?}, deps={:?}",
        i + 1,
        state.accepted_ballot,
        state.max_ballot_seen,
        state.deps,
    );
}
```

### 5.4 Mutation Testing Targets

The 24-step test should also be run with intentional mutations to verify it
catches real bugs (not just passing vacuously):

| Mutation | Expected Result |
|----------|----------------|
| Merge `accepted_ballot` and `max_ballot_seen` into a single field | Test FAILS at step 21 (wrong value selected) |
| Use `max_ballot_seen` instead of `accepted_ballot` in recovery selection | Test FAILS at step 21 |
| Skip `join_ballot` in Recover handler (don't update `max_ballot_seen`) | Test FAILS at step 16 (NACK not generated for lower ballots) |
| Use `t0` filter in Accept dep computation instead of `t` | May not fail this specific test but fails dep-filter tests in Layer 2 |
| Remove idempotency check in Recover handler | Test FAILS at step 20 (duplicate creates extra state) |

---

## Layer 6: Property-Based Tests

Supplement the deterministic tests with randomized exploration of the state space.

### 6.1 Ballot Invariant Fuzzing

```rust
#[test]
fn proptest_ballot_invariant_never_violated() {
    // Generate random sequences of:
    //   PreAccept, Accept(ballot), Recover(ballot), Commit
    // Applied to a single TxnState.
    // After each operation, assert:
    //   accepted_ballot <= max_ballot_seen
    proptest!(|(ops in vec(arb_txn_op(), 1..100))| {
        let mut state = TxnState::new(txn_id, t0);
        for op in ops {
            state.apply(op);
            prop_assert!(state.accepted_ballot <= state.max_ballot_seen);
        }
    });
}
```

### 6.2 Recovery Consistency

```rust
#[test]
fn proptest_recovery_always_selects_same_value() {
    // Generate a random cluster of 3-5 replicas.
    // Run a random sequence of PreAccept/Accept/Recover messages.
    // Trigger recovery from two different nodes simultaneously.
    // Both recovery coordinators must select the same (t, deps).
    proptest!(|(
        n_replicas in 3..=5usize,
        ops in vec(arb_protocol_op(), 5..50),
    )| {
        let mut cluster = TestCluster::new(n_replicas);
        for op in &ops {
            cluster.apply(op);
        }
        let recovery1 = cluster.recover_from(0, &txn_id);
        let recovery2 = cluster.recover_from(1, &txn_id);
        prop_assert_eq!(recovery1.t, recovery2.t);
        prop_assert_eq!(recovery1.deps, recovery2.deps);
    });
}
```

### 6.3 Dependency Completeness

```rust
#[test]
fn proptest_conflicting_txns_always_in_deps() {
    // Generate N conflicting transactions on the same key.
    // Run them through PreAccept on 3 replicas with random ordering.
    // For any two committed txns γ and τ where t_γ < t_τ:
    //   assert γ ∈ deps(τ)
    proptest!(|(
        n_txns in 2..10usize,
        delivery_order in vec(arb_delivery_order(), 1..100),
    )| {
        let mut cluster = TestCluster::new(3);
        let txns = generate_conflicting_txns(n_txns);
        for delivery in &delivery_order {
            cluster.deliver(delivery);
        }
        cluster.drain();
        for (gamma, tau) in committed_pairs(&cluster, &txns) {
            if gamma.t < tau.t {
                prop_assert!(tau.deps.contains(&gamma.txn_id));
            }
        }
    });
}
```

### 6.4 Timestamp Uniqueness Under Pressure

```rust
#[test]
fn proptest_no_duplicate_timestamps() {
    // Generate timestamps from 3 HLC instances (simulating 3 nodes)
    // running concurrently with random merges between them.
    // Assert no two timestamps are ever equal.
    proptest!(|(
        ops in vec(arb_hlc_op(), 100..1000),
    )| {
        let mut hlcs = [HLC::new(1), HLC::new(2), HLC::new(3)];
        let mut seen = HashSet::new();
        for op in &ops {
            let ts = match op {
                HlcOp::Tick(node) => hlcs[*node].now(),
                HlcOp::Merge(src, dst) => {
                    let remote = hlcs[*src].now();
                    hlcs[*dst].merge(remote);
                    hlcs[*dst].now()
                }
            };
            prop_assert!(seen.insert(ts), "duplicate timestamp: {:?}", ts);
        }
    });
}
```

---

## Test Execution Order

Tests should be run in dependency order. CI should fail fast at the lowest
layer that breaks.

```
cargo test --lib timestamp_       # Layer 1.1
cargo test --lib ballot_          # Layer 1.2
cargo test --lib txnstate_        # Layer 1.3
cargo test --lib conflict_index_  # Layer 1.4
cargo test --lib hlc_             # Layer 1.5
cargo test --lib reorder_buffer_  # Layer 1.6
cargo test --lib sm_              # Layer 2
cargo test --lib preaccept_       # Layer 3.1
cargo test --lib accept_          # Layer 3.2
cargo test --lib commit_          # Layer 3.3
cargo test --lib recover_         # Layer 3.4
cargo test --lib scenario_        # Layer 4
cargo test --lib epaxos_24_step   # Layer 5
cargo test --lib proptest_        # Layer 6
```

If Layer 1.2 (`ballot_accepted_and_promised_are_distinct_types`) fails, there
is no point running Layers 2-5. The 24-step test will fail for the same reason,
and the Layer 1 failure is easier to diagnose.

## Test Count Summary

| Layer | Tests | Purpose |
|-------|-------|---------|
| 1. Data Structures | 38 | Types have correct mathematical properties |
| 2. State Machine | 18 | Transitions preserve invariants |
| 3. Handler Contracts | 20 | Each handler correct in isolation |
| 4. Protocol Scenarios | 16 | Multi-handler sequences produce correct outcomes |
| 5. 24-Step Capstone | 1 (24 intermediate assertions) | Known counter-example caught |
| 6. Property-Based | 4 | Randomized state space exploration |
| **Total** | **97** | |
