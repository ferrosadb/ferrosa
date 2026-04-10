# Test Specification — Accord Infrastructure

> Last updated: 2026-03-21
> Status: Draft
> Companion to: accord-test-spec.md

## Design Philosophy

The main test spec (accord-test-spec.md) builds a pyramid from data structures
to the 24-step capstone. This companion spec covers the *plumbing* that
supports the protocol: the dual-log architecture, fsync ordering guarantees,
the Accord write gate, heartbeat-based clock skew measurement, protocol
message serialization, the TestCluster harness itself, and the fast quorum
size formula.

These infrastructure tests are not in the pyramid's critical path — the
24-step test does not directly depend on them — but any failure here means the
system is unsafe in production. A correct protocol implementation that does not
fsync before replying, or that leaks protocol log entries to S3, or that allows
non-transactional writes to bypass Accord, is a correctly-wrong system.

```
┌─────────────────────────────────────────────────────────┐
│  Infrastructure Tests (this spec)                       │
│                                                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────┐  │
│  │ Dual-Log │ │ Fsync    │ │ Write    │ │ Heartbeat │  │
│  │ (S1.5)   │ │ (S1.6)   │ │ Gate     │ │ RTT/Skew  │  │
│  │          │ │          │ │ (S1.8)   │ │ (S2.1-2)  │  │
│  └──────────┘ └──────────┘ └──────────┘ └───────────┘  │
│  ┌──────────┐ ┌──────────────────────┐ ┌───────────┐   │
│  │ Protocol │ │ TestCluster Harness  │ │ Fast      │   │
│  │ Messages │ │ (S1.9 prerequisite)  │ │ Quorum    │   │
│  │ (S2.8)   │ │                      │ │ (S3.2)    │   │
│  └──────────┘ └──────────────────────┘ └───────────┘   │
└─────────────────────────────────────────────────────────┘
```

---

## 1. Dual-Log Architecture (S1.5)

Accord protocol phases (PreAccepted, Accepted, Committed) are written to a
local-only *protocol log*. Only `AccordApplied` entries (with data mutations)
are written to the main commit log and uploaded to S3. This separation exists
because protocol log entries are ephemeral coordination state — replaying them
from S3 during recovery is meaningless (and expensive). The protocol log uses
smaller segments and aggressive GC because its entries are short-lived.

| Test | What It Proves | How |
|------|---------------|-----|
| `protocol_log_not_uploaded` | Protocol log segments never enter the S3 upload pipeline | Mock the S3 upload manager with a recording wrapper. Write 10 protocol entries (mix of AccordPreAccepted, AccordAccepted, AccordCommitted) to the protocol log. Assert: upload queue is empty after each write and after segment rotation. Then write 1 AccordApplied + data mutation to the main commit log. Assert: upload queue has exactly 1 segment. Assert: the uploaded segment contains only main-log entries. |
| `protocol_log_gc_after_applied` | Protocol log entries for a txn are GC'd once AccordApplied is written to main log | Write PreAccepted, Accepted, Committed entries for txn T1 to the protocol log. Write Applied for T1 to the main log. Write PreAccepted for txn T2 (still in-flight) to the protocol log. Trigger GC. Assert: T1's protocol entries are removed. Assert: T2's PreAccepted entry is NOT removed. Assert: protocol log segment file size decreased. |
| `accord_commitlog_roundtrip` | All 4 Accord entry types serialize and deserialize correctly through both logs | For each entry type (AccordPreAccepted, AccordAccepted, AccordCommitted, AccordApplied): serialize to bytes, deserialize back, assert all fields match (txn_id, ballot, timestamp, deps, result_bytes). Edge cases: empty deps (0 entries), max-size deps (256 entries), empty result bytes (0 bytes), large result bytes (1MB). Assert: CRC validation passes for each entry. |
| `protocol_log_segment_size` | Protocol log uses smaller segments than main log | Configure protocol log segment size = 8MB, main log segment size = 32MB. Write protocol entries until first segment rotation. Assert: rotation occurs at ~8MB (within 1 entry of boundary). Write main log entries until first segment rotation. Assert: rotation occurs at ~32MB. Assert: both sizes are independently configurable. |
| `protocol_log_replay_on_startup` | Protocol log replay reconstructs in-flight TxnState | Write PreAccepted entry for txn T1 (t0=100, deps={}). Write Accepted entry for T1 (ballot=2, t=105, deps={T0}). Drop all in-memory state (simulate process restart). Replay protocol log. Assert: TxnState for T1 is reconstructed with phase=accepted, accepted_ballot=2, max_ballot_seen=2, t=105, deps={T0}. Assert: ConflictIndex re-populated with T1's key. |
| `protocol_log_and_main_log_replay_order` | Protocol log replayed BEFORE main log; Applied entries suppress in-flight reconstruction | Write PreAccepted + Accepted for txn T1 to protocol log. Write Applied for T1 to main log. Write PreAccepted + Accepted for txn T2 to protocol log (T2 has no Applied entry). Simulate restart. Replay protocol log first, then main log. Assert: T1 does NOT appear as in-flight (Applied entry marks it done). Assert: T2 DOES appear as in-flight with phase=accepted. |
| `protocol_log_corrupt_entry_skipped` | Corrupted protocol log entry is skipped during replay; node still starts | Write PreAccepted for T1, Accepted for T2, Committed for T3 to protocol log. Corrupt the bytes of T2's entry (flip CRC). Replay protocol log. Assert: T1 reconstructed correctly. Assert: T2 skipped (not in TxnState map). Assert: T3 reconstructed correctly. Assert: error-level log message emitted mentioning T2's corrupted entry. Assert: node startup succeeds. |

**Why this matters:** Without log separation, protocol entries inflate S3 storage
by 3-4x (three phases per transaction). Without GC, the protocol log grows
without bound for long-running nodes. Without correct replay ordering, a node
that restarts mid-transaction either loses state or double-applies.

---

## 2. Fsync-Before-Ack (S1.6)

FM5 (RPN 150): if the commit log write is not fsynced before the protocol reply
is sent, a crash after reply but before flush loses the vote. The recovering
coordinator believes this replica voted, but the replica has no record. This
violates the quorum intersection property.

| Test | What It Proves | How |
|------|---------------|-----|
| `fsync_before_ack_ordering` | Fsync completes before protocol reply is constructed | Instrument the commit log writer with an ordering tracker (a `Vec<&str>` that records `"write"`, `"fsync"`, `"reply"` in call order). Call `handle_preaccept`. Assert sequence is exactly `["write", "fsync", "reply"]`. Use a mock writer that appends to the tracker on each operation. |
| `fsync_before_ack_accept` | Same ordering guarantee for Accept handler | Same instrumented mock. Call `handle_accept(ballot, t, deps)`. Assert sequence is `["write", "fsync", "reply"]`. Assert: the written entry is AccordAccepted with correct ballot. |
| `fsync_before_ack_apply` | Same ordering for Apply handler; `applied=true` flag set AFTER fsync | Same instrumented mock. Call `handle_apply(t, deps, result)`. Assert sequence is `["write", "fsync", "set_applied_flag", "reply"]`. The `applied=true` flag must not be observable by any reader between `write` and `fsync`. |
| `fsync_failure_prevents_reply` | IO error on fsync suppresses the protocol reply | Configure mock writer to return `Err(io::Error)` from fsync. Call `handle_preaccept`. Assert: no PreAcceptOK is produced (return value is `None` or error variant). Assert: TxnState for this txn has `pre_accepted==false` (state not advanced). Assert: error logged at error level with the IO error details. |
| `fsync_latency_does_not_block_other_shards` | Fsync on one shard does not block processing on another | Create two shard executors (shard 0 and shard 1). Configure shard 0's mock writer with a 100ms fsync delay. Send PreAccept to both shards concurrently. Assert: shard 1's PreAcceptOK is produced before shard 0's fsync completes. Assert: shard 0's PreAcceptOK is produced after its fsync. Measure: shard 1 latency < 5ms, shard 0 latency >= 100ms. |

**Why this matters:** The entire quorum-based safety argument assumes that a vote,
once acknowledged, is durable. If fsync races with the reply, a power failure
between reply and flush creates a phantom vote that no replica can reconstruct.

---

## 3. Accord Write Gate (S1.8)

FM16 (RPN 250 — highest in the FMEA): a non-transactional CQL write that
modifies a key with an in-flight Accord transaction can violate serializability.
The write gate ensures that all writes to conflicted keys are routed through
Accord or blocked.

| Test | What It Proves | How |
|------|---------------|-----|
| `non_transactional_write_accord_gate` | Non-transactional write to a key with in-flight Accord txn is intercepted | Register an in-flight Accord txn on key K in ConflictIndex (phase=pre_accepted). Send a non-transactional `INSERT INTO t (pk) VALUES (K)` through the CQL router. Assert: the write is either (a) routed through Accord as a fire-and-forget transaction, or (b) blocked until the Accord txn reaches Applied. Assert: the write does NOT bypass ConflictIndex. Assert: ConflictIndex was consulted (mock records the lookup). |
| `write_gate_no_conflict_passes_through` | Non-transactional write to unconflicted key has no Accord overhead | Ensure ConflictIndex has no entries for key L. Send `INSERT INTO t (pk) VALUES (L)` through the CQL router. Assert: write proceeds through the normal (non-Accord) write path. Assert: no AccordCoordinator invoked. Assert: latency overhead < 1us beyond normal ConflictIndex lookup. |
| `write_gate_concurrent_check` | Concurrent non-transactional write appears in Accord txn's dep set | Start an Accord txn T1 on key K (PreAccept in progress). Concurrently send a non-transactional INSERT to key K. Assert: the non-transactional write is routed through Accord as txn T2. Assert: T2 appears in T1's dep set (or T1 in T2's dep set, depending on t0 ordering). Assert: both transactions eventually commit with consistent dep sets across replicas. |
| `write_gate_after_accord_applied` | Completed Accord txn does not block subsequent non-transactional writes | Register Accord txn T1 on key K. Advance T1 through all phases to Applied. Remove T1 from ConflictIndex. Send non-transactional INSERT to key K. Assert: write proceeds through normal path without Accord routing. Assert: ConflictIndex lookup returns None for key K. |

**Why this matters:** This is the highest-RPN failure mode in the entire FMEA.
A non-transactional write that bypasses Accord creates a serializability hole
that no amount of correct protocol implementation can close. The gate is the
only line of defense.

---

## 4. Heartbeat RTT and SkewMax (S2.1, S2.2, S2.9)

The ReorderBuffer's deadline formula (spec section 7.1) depends on accurate RTT and
clock skew measurements. These measurements come from the heartbeat protocol
extension. If RTT or SkewMax is wrong, the ReorderBuffer either holds messages
too long (latency) or releases them too early (reordering).

| Test | What It Proves | How |
|------|---------------|-----|
| `heartbeat_rtt_tracking` | Per-peer RTT measured from heartbeat round-trips | Send a Ping with `sent_at=100` to peer B. Receive Pong with `recv_at` filled by B. Compute RTT = `local_recv_time - sent_at`. Assert: RTT stored in per-peer metrics for B. Send 10 more pings with varying delays. Assert: sliding window contains all 11 samples. Assert: `rtt_estimate(B)` returns a value within the range of observed RTTs. |
| `per_peer_latency_p99` | P99 latency computed correctly from sliding window | Send 100 pings to peer B with RTTs drawn from: 99 values uniformly in [1ms, 50ms], one outlier at 500ms. Compute P99 from the sliding window. Assert: P99 is approximately 50ms (the 99th percentile of the non-outlier distribution), NOT 500ms. Assert: P50 is approximately 25ms. |
| `skew_max_measurement` | SkewMax converges to the P99.9 of observed clock offsets across the cluster | Three nodes (A, B, C) exchange heartbeats. A's clock is 5ms ahead of B. B's clock is 3ms ahead of C. Run 1000 heartbeat rounds. Assert: SkewMax converges to approximately 5ms (the maximum pairwise offset at P99.9). Assert: SkewMax is >= 5ms (conservative, not underestimated). |
| `skew_outlier_rejection` | Unstable node's measurements excluded from SkewMax | Four nodes (A, B, C, D). A, B, C have stable clocks (offsets within 5ms). D sends heartbeats with wildly varying `sent_at` timestamps (jumping by +/-100ms between heartbeats). Assert: D's measurements are flagged as outliers. Assert: SkewMax is computed from A, B, C only (~5ms). Assert: SkewMax does NOT inflate to 100ms due to D. |
| `skew_hard_ceiling` | SkewMax never exceeds the hard ceiling regardless of input | Set hard ceiling to 2s (default). Simulate a node with broken NTP reporting 10s clock offset. Run heartbeat rounds. Assert: SkewMax is capped at 2s. Assert: the capped value is logged at warning level. Assert: the node with 10s offset is flagged for operator attention. |
| `reject_future_timestamp_preaccept` | Replica rejects PreAccept with timestamp beyond MAX_CLOCK_DRIFT | Set local HLC to time=1000. Set MAX_CLOCK_DRIFT to 100. Receive PreAccept with `t0.time = 1101` (exceeds 1000 + 100). Assert: PreAccept is rejected (returns error, not PreAcceptOK). Assert: local HLC is NOT advanced past 1000. Assert: no ConflictIndex entry created for this txn. Assert: rejection logged with the offending timestamp and drift delta. |
| `accept_past_timestamp_preaccept` | PreAccept with past timestamp accepted normally | Set local HLC to time=1000. Receive PreAccept with `t0.time = 500` (in the past). Assert: PreAccept is accepted. Assert: PreAcceptOK returned with `t >= 1000` (timestamp bumped to at least local time). Assert: ConflictIndex entry created. Past timestamps are safe — only future timestamps violate the drift invariant. |

**Why this matters:** The ReorderBuffer formula is `deadline = wall_clock + SkewMax +
max(Latency(C',P)) - Latency(C,P)`. If SkewMax is underestimated, messages are
released before all replicas have had a chance to deliver competing PreAccepts,
and the dep set is incomplete. If overestimated, every PreAccept incurs
unnecessary latency equal to the overestimate.

---

## 5. Accord Protocol Messages (S2.8)

Eleven new message types must be added to ferrosa-net for the Accord protocol.
These tests verify wire-level correctness: serialization roundtrips, unique
type codes, size bounds, and graceful handling of unknown types.

| Test | What It Proves | How |
|------|---------------|-----|
| `accord_message_roundtrip` | All 11 message types serialize and deserialize without data loss | For each of PreAccept, PreAcceptOK, Accept, AcceptOK, Commit, Read, ReadOK, Apply, ApplyOK, Recover, RecoverOK: construct a message with representative field values. Serialize to bytes. Deserialize from bytes. Assert all fields match the original. Cover edge cases per message type: empty deps (0 entries), large dep sets (256 entries), empty result bytes (ReadOK, ApplyOK), large result bytes (1MB payload), maximum TxnId values (`u64::MAX` epoch, time, seq), ballot at `u64::MAX`. |
| `accord_message_type_codes_unique` | No two message types share the same wire code | Collect the `MsgType` enum variant discriminant for all 11 Accord message types. Assert: all 11 codes are distinct. Collect all existing (non-Accord) message type codes from ferrosa-net. Assert: no Accord code collides with any existing code. This prevents silent misrouting where an Accord message is interpreted as a gossip message (or vice versa). |
| `accord_message_size_bounded` | Messages with maximum-sized payloads stay within 1MB | Construct a PreAccept message with `MAX_KEYS_PER_TXN` (128) keys, each key at maximum size (64KB partition key). Construct a Commit message with `MAX_DEPS` (256) dep entries. Serialize each. Assert: serialized size does not exceed 1MB. If it does, assert that the serializer returns `Err(MessageTooLarge)` rather than producing an oversized buffer. |
| `accord_message_unknown_type_rejected` | Unknown MsgType code returns a deserialization error, not a panic | Construct a raw byte buffer with a valid message header but an unrecognized MsgType code (e.g., 0xFF). Attempt deserialization. Assert: returns `Err(UnknownMessageType(0xFF))`. Assert: no panic. Assert: the network connection is not closed (the error is per-message, not per-connection). The handler logs the unknown type and continues processing subsequent messages on the same connection. |

**Why this matters:** Protocol messages are the wire contract between nodes.
A serialization bug means nodes disagree on what was voted. A type code
collision means messages are silently misrouted. An unbounded message means
a single large transaction can OOM the receiver.

---

## 6. Test Harness: TestCluster (S1.9 Prerequisite)

The `TestCluster` with deterministic message scheduling is assumed
infrastructure for the 24-step test (Layer 5) and all Layer 4 scenario tests.
It must be tested independently — if the harness has bugs, every test that
uses it is unreliable.

The harness provides: deterministic message delivery order, out-of-order
delivery for partition simulation, message dropping, quiescent drain, and
cross-replica consistency assertions. No tokio runtime, no real network, no
wall clocks.

| Test | What It Proves | How |
|------|---------------|-----|
| `test_cluster_deterministic_delivery` | Messages delivered in FIFO order by default | Create a TestCluster with 3 replicas. Enqueue 3 messages: M1 (A->B), M2 (B->C), M3 (A->C). Call `deliver_next()` three times. Assert: M1 delivered first, M2 second, M3 third. Assert: each delivery returns the response messages generated by the destination replica. |
| `test_cluster_out_of_order_delivery` | `deliver_at(index)` delivers a specific message out of order | Enqueue 3 messages: M1, M2, M3. Call `deliver_at(2)` to deliver M3 first. Assert: M3 processed by destination. Assert: M1 and M2 still in pending queue. Call `deliver_next()`. Assert: M1 delivered (original FIFO position). |
| `test_cluster_drop_message` | `drop_at(index)` removes a message without delivery | Enqueue 3 messages: M1, M2, M3. Call `drop_at(1)` to remove M2. Assert: pending queue contains only M1 and M3. Call `drain()`. Assert: only M1 and M3 were delivered. Assert: M2's destination replica never received M2. |
| `test_cluster_drain` | `drain()` delivers all messages until quiescent | Enqueue 10 messages that generate response messages (e.g., PreAccept messages that produce PreAcceptOK responses, which in turn generate Commit messages). Call `drain()`. Assert: all messages processed, including transitively generated responses. Assert: pending queue is empty. Assert: no infinite loop (drain terminates). |
| `test_cluster_assert_consistent` | `assert_consistent` passes for agreement, panics for disagreement | Two replicas both commit txn T1 with same (t=100, deps={T0}). Call `assert_consistent(&t1)`. Assert: no panic. Change replica 1's deps for T1 to {T0, T2}. Call `assert_consistent(&t1)`. Assert: panics with a message containing both replicas' state (t, deps, accepted_ballot, max_ballot_seen) for diagnostic purposes. |
| `test_cluster_no_tokio` | TestCluster operates without async runtime | Assert: `TestCluster::new(3)` does not call `tokio::runtime::Runtime::new()` or `tokio::runtime::Handle::current()`. All clocks are synthetic (`MockClock` with manual `advance(duration)` method). All message delivery is synchronous. Assert: a test using TestCluster completes in < 1ms wall-clock time (no real sleeps, no real network). |

**Why this matters:** The 24-step test's correctness depends entirely on
deterministic message ordering. If `deliver_at(2)` actually delivers message 1,
or if `drain()` silently drops messages, the 24-step test could pass or fail
for reasons unrelated to the protocol implementation. Testing the harness
separately makes harness bugs distinguishable from protocol bugs.

---

## 7. Fast Quorum Size Formula

The fast quorum formula from Spec section 3.2 determines how many replicas must
agree on (t, deps) for the coordinator to take the fast path (1 RTT). This is
pure arithmetic — no I/O, no state — but a wrong formula means the protocol
either takes the slow path unnecessarily (performance) or accepts too few
agreeing replicas (safety).

The formula: `fast_quorum_size = ceil((E + f_fast + 1) / 2)` where `E` is the
electorate size (= RF for single-DC) and `f_fast` is the number of fast-path
failures tolerated.

The slow quorum formula: `slow_quorum_size = f_slow + 1` where `f_slow` is the
number of slow-path failures tolerated.

| Test | What It Proves | How |
|------|---------------|-----|
| `fast_quorum_size_rf3_f0` | RF=3, f_fast=0: fast quorum = 2 | `fast_quorum_size(electorate=3, f_fast=0)` = `ceil((3+0+1)/2)` = `ceil(2.0)` = 2. Assert: returns 2. This is the common case: 3-node cluster, no fast-path fault tolerance. 2 of 3 agreeing replicas suffice for fast path. |
| `fast_quorum_size_rf5_f1` | RF=5, f_fast=1: fast quorum = 4 | `fast_quorum_size(electorate=5, f_fast=1)` = `ceil((5+1+1)/2)` = `ceil(3.5)` = 4. Assert: returns 4. With one allowed fast-path failure, 4 of 5 must agree. |
| `fast_quorum_size_rf3_f1` | RF=3, f_fast=1: fast quorum = 3 (unanimous) | `fast_quorum_size(electorate=3, f_fast=1)` = `ceil((3+1+1)/2)` = `ceil(2.5)` = 3. Assert: returns 3. Tolerating one fast-path failure with RF=3 requires unanimity — all 3 replicas must agree. |
| `fast_quorum_size_rf1` | RF=1, f_fast=0: fast quorum = 1 | `fast_quorum_size(electorate=1, f_fast=0)` = `ceil((1+0+1)/2)` = `ceil(1.0)` = 1. Assert: returns 1. Single-node cluster: the coordinator is the only voter. |
| `slow_quorum_size` | Slow quorum = f_slow + 1 for various configurations | Assert: `slow_quorum_size(f_slow=1)` = 2 (RF=3 with 1 fault tolerance). Assert: `slow_quorum_size(f_slow=2)` = 3 (RF=5 with 2 fault tolerance). Assert: `slow_quorum_size(f_slow=0)` = 1. The slow quorum is always strictly smaller than the fast quorum (for f_fast > 0), which is why the slow path exists as a fallback. |

**Why this matters:** If `fast_quorum_size` returns 1 instead of 2 for RF=3,
the coordinator accepts a single replica's (t, deps) as final. A second replica
with a different dep set will commit with different deps — exactly the bug the
24-step test is designed to catch, but caused by arithmetic rather than ballot
management.

---

## Test Execution Order

Infrastructure tests should run before the protocol tests in the main spec.
They validate the scaffolding that protocol tests depend on.

```
cargo test --lib test_cluster_      # Section 6 (harness)
cargo test --lib protocol_log_      # Section 1 (dual-log)
cargo test --lib accord_commitlog_  # Section 1 (serialization)
cargo test --lib fsync_             # Section 2 (durability)
cargo test --lib non_transactional_ # Section 3 (write gate)
cargo test --lib write_gate_        # Section 3 (write gate)
cargo test --lib heartbeat_rtt_     # Section 4 (RTT)
cargo test --lib per_peer_          # Section 4 (latency)
cargo test --lib skew_              # Section 4 (SkewMax)
cargo test --lib reject_future_     # Section 4 (drift)
cargo test --lib accept_past_       # Section 4 (drift)
cargo test --lib accord_message_    # Section 5 (protocol messages)
cargo test --lib fast_quorum_       # Section 7 (formula)
cargo test --lib slow_quorum_       # Section 7 (formula)
```

If the TestCluster harness tests fail, skip all Layer 4 and Layer 5 tests in
the main spec — they use the harness and their results are meaningless if the
harness is broken.

## Test Count Summary

| Section | Tests | Purpose |
|---------|-------|---------|
| 1. Dual-Log Architecture | 7 | Protocol log isolation, GC, replay, corruption handling |
| 2. Fsync-Before-Ack | 5 | Durability ordering, failure handling, shard independence |
| 3. Accord Write Gate | 4 | Non-transactional write interception, conflict routing |
| 4. Heartbeat RTT and SkewMax | 7 | Clock skew measurement, outlier rejection, drift enforcement |
| 5. Accord Protocol Messages | 4 | Wire-level serialization, type safety, size bounds |
| 6. TestCluster Harness | 6 | Deterministic scheduling, drop/reorder, consistency assertions |
| 7. Fast Quorum Size Formula | 5 | Quorum arithmetic correctness |
| **Total** | **38** | |
