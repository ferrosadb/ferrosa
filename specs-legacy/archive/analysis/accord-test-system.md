# Test Specification — Accord System-Level Tests

> Last updated: 2026-03-21
> Status: Draft
> Companion to: accord-test-spec.md (pyramid layers 1-6), accord-test-infrastructure.md (plumbing), accord-test-integration.md (S3 wiring)

## Design Philosophy

The companion specs build a test pyramid from data structure unit tests to the
24-step EPaxos capstone, then add infrastructure plumbing and integration
wiring. This spec covers the *system-level* tests that sit above the pyramid:
Jepsen correctness verification, chaos fault injection, performance benchmarks,
observability instrumentation, and the ExclusiveSyncPoint/DurabilityService
lifecycle that has no project plan task.

These tests require a running multi-node Ferrosa cluster (3+ nodes) and cannot
be run in the `TestCluster` deterministic scheduler. They validate emergent
properties that only appear under real concurrency, real network delays, and
real failure injection.

```
                     ┌───────────────────────────────────┐
                     │  System-Level Tests (this spec)   │
                     │                                   │
                     │  Jepsen correctness               │
                     │  Chaos fault injection             │
                     │  Performance benchmarks            │
                     │  ExclusiveSyncPoint/DurabilityService │
                     │  Observability and metrics         │
                     └────────────────┬──────────────────┘
                                      │
           ┌──────────────────────────┼──────────────────────────┐
           │                          │                          │
┌──────────┴──────────┐  ┌───────────┴──────────┐  ┌───────────┴──────────┐
│  accord-test-spec   │  │  accord-test-infra   │  │  accord-test-integ   │
│  (layers 1-6)       │  │  (plumbing)          │  │  (S3 wiring)         │
│  unit → 24-step     │  │  dual-log, fsync,    │  │  fire-and-forget,    │
│  capstone           │  │  write gate, msgs    │  │  dep-wait, CQL route │
└─────────────────────┘  └──────────────────────┘  └──────────────────────┘
```

### Prerequisite Relationship

Every test in this spec depends on the Jepsen test infrastructure (section 1).
No system-level test can run until `jepsen_cluster_provisioning` passes. The
infrastructure section itself has **no corresponding project plan task** and
needs one before Phase 1 gate (S4.5).

### Section Numbering

Section numbers in parentheses (e.g., S4.5, S5.6) reference tasks in the
Accord project plan (`specs/accord-project-plan.md`). Sections marked
"no project plan task" require a new task to be added before implementation.

---

## 1. Jepsen Test Infrastructure

**Project plan gap:** No task covers Jepsen infrastructure. The project plan
references `jepsen_register_linearizability` (S4.5) and `jepsen_bank_atomicity`
(S5.6) as phase gates but does not include any task for the cluster provisioning,
Clojure client, nemesis operations, or history recording that those tests depend
on. A new sprint task (suggest S4.0 or a dedicated pre-S4 infrastructure sprint)
is required.

### 1.1 Cluster Provisioning

| Test | What It Proves | How |
|------|---------------|-----|
| `jepsen_cluster_provisioning` | A 3-node Ferrosa cluster can be stood up programmatically, with CQL reachable on all nodes and Raft quorum formed. This is the prerequisite for every Jepsen test. | Use Docker Compose (local) or Terraform (EC2) to provision 3 Ferrosa containers/instances. Each node configured with `--seeds` pointing to the other two, `--cluster-name jepsen-test`, and `--mode cluster`. Wait for: (1) all 3 nodes report `UP` in `system.cluster_peers`, (2) Raft leader elected (check via `ferrosa-ctl cluster status`), (3) CQL port (9042) accepting connections on all 3 nodes, (4) a test keyspace can be created with `RF=3`. Timeout: 60s. If any condition is not met, dump logs from all 3 nodes and fail with diagnostics. |

### 1.2 Clojure Client

| Test | What It Proves | How |
|------|---------------|-----|
| `jepsen_clojure_client` | A Clojure client can execute all CQL operations required by Jepsen workloads against Ferrosa's CQL v5 protocol. | Implement a Jepsen client class extending `jepsen.client/Client`. Operations: `INSERT INTO ... VALUES (?)`, `SELECT ... WHERE pk = ?`, `BEGIN TRANSACTION ... COMMIT`, and `SET CONSISTENCY <level>` (connection-level). Test the client against the provisioned cluster: (1) insert a row, (2) read it back and assert equality, (3) execute a 2-statement transaction that reads and writes, (4) set consistency to QUORUM and verify via tracing that the coordinator contacts the expected number of replicas. Client must handle `Overloaded`, `Unavailable`, and `WriteTimeout` errors by returning `:fail` or `:info` (not crashing). |

### 1.3 Nemesis Operations

| Test | What It Proves | How |
|------|---------------|-----|
| `jepsen_nemesis_partition` | Network partitions isolate a minority node from the cluster, and the majority continues serving requests. | Nemesis: `iptables -A INPUT -s <minority_ip> -j DROP` + `iptables -A OUTPUT -d <minority_ip> -j DROP` on majority nodes. Duration: 30s. Verify: (1) majority nodes can still execute CQL writes at QUORUM, (2) minority node's CQL connections return `Unavailable`, (3) after iptables rules are removed, minority node rejoins and reads return consistent data. Repeat 3 times to verify deterministic heal behavior. |
| `jepsen_nemesis_kill` | SIGKILL of a minority node does not cause data loss or cluster hang. Recovery completes within bounded time. | Nemesis: `kill -9 <ferrosa_pid>` on one of 3 nodes. Restart the process within 5s. Verify: (1) remaining 2 nodes continue serving QUORUM writes during the outage, (2) killed node recovers from commit log on restart, (3) after restart, killed node's data matches the other two nodes (read all rows and compare). |
| `jepsen_nemesis_slow` | Network jitter does not cause correctness violations; only latency increases. | Nemesis: `tc qdisc add dev eth0 root netem delay 100ms 50ms distribution normal` on 20% of internode links (randomly selected). Duration: 30s. Verify: (1) all CQL operations eventually complete (no indefinite hangs), (2) no Accord protocol errors in logs beyond expected timeouts, (3) `SkewMax` metric increases but stays below the hard ceiling (2s). |
| `jepsen_nemesis_clock_skew` | Clock skew within SkewMax does not violate linearizability. Skew beyond SkewMax is detected and rejected. | Nemesis: `chronyc makestep` to inject +/-5ms skew on random nodes. Verify: (1) HLC monotonicity maintained on all nodes (query `system_observability.host_metrics`), (2) no `MAX_CLOCK_DRIFT` violations logged at the 5ms level, (3) if skew is increased to 2x SkewMax (separate test run), the affected node enters degraded mode and stops accepting new transactions. |
| `jepsen_nemesis_pause` | SIGSTOP (process freeze) does not cause split brain or data corruption. | Nemesis: `kill -STOP <ferrosa_pid>` on one random node for 10s, then `kill -CONT`. Verify: (1) failure detector marks the paused node as suspected within the heartbeat timeout (5s), (2) in-flight transactions coordinated by the paused node are recovered by peers, (3) after SIGCONT, the node resumes and converges with the cluster, (4) no duplicate applies (check memtable entry counts for test rows). |

### 1.4 History Recording

| Test | What It Proves | How |
|------|---------------|-----|
| `jepsen_history_recording` | All client operations are recorded in Jepsen history format with invoke/response pairs, enabling offline linearizability checking. | Run a 30s workload with 3 concurrent clients performing random reads and writes. After the run, verify: (1) history file exists and is valid Clojure data, (2) every `:invoke` has a matching `:ok`, `:fail`, or `:info` response, (3) no orphaned invokes (client crashed mid-operation), (4) Knossos can parse the history file without errors (even if the linearizability check itself is not run). History includes: `{:type :invoke, :f :write, :value 42, :process 0, :time <ns>}` tuples. |

---

## 2. Jepsen: Register (S4.5 -- Phase 1 Gate)

The register workload is the simplest linearizability test: concurrent reads and
writes to a single row. If Accord cannot pass this, nothing else matters. This
is the Phase 1 gate.

| Test | What It Proves | How |
|------|---------------|-----|
| `jepsen_register_linearizability` | Single-key Accord transactions are linearizable under node failure. Every read returns the most recently committed write. | Workload: 5 concurrent clients, each performing random reads (`SELECT value FROM test.register WHERE key = 1`) and writes (`INSERT INTO test.register (key, value) VALUES (1, ?)` with random integers). Duration: 120 seconds. Nemesis: `kill` (minority node killed, restarted within 5s). Checker: Knossos linearizability checker on the recorded history. Pass criterion: zero violations found. Knossos reports `{:valid? true}`. |
| `jepsen_register_with_partition` | Linearizability is maintained across network partitions. After partition heals, all nodes converge to the same state. | Same workload as `jepsen_register_linearizability` (5 clients, 120s, random reads/writes to single row). Nemesis: `partition` (minority network partition via iptables, 30s duration, 2 partition events during the run). Checker: Knossos. Pass criterion: linearizability checker finds zero violations. Post-run convergence check: after all partitions heal, read the register from all 3 nodes individually and assert all return the same value. |
| `jepsen_register_with_clock_skew` | Accord's SkewMax measurement and ReorderBuffer correctly handle clock skew. No ordering violations occur despite skewed clocks. | Same workload (5 clients, 120s). Nemesis: `clock-skew` (+/-5ms via `chronyc makestep` on random nodes, applied every 15s). Checker: Knossos. Pass criterion: linearizability maintained. Additional assertion: `metrics_skew_max_ns` gauge on all nodes remains below the hard ceiling. No `MAX_CLOCK_DRIFT` rejection errors in logs. |

---

## 3. Jepsen: Bank (S5.6 -- Phase 2 Gate)

The bank workload tests multi-key transaction atomicity: transfers between
accounts must preserve a global invariant. This is the Phase 2 gate.

| Test | What It Proves | How |
|------|---------------|-----|
| `jepsen_bank_atomicity` | Multi-key Accord transactions are atomic. The total balance across all accounts is invariant under concurrent transfers and failures. | Setup: `CREATE TABLE test.accounts (id int PRIMARY KEY, balance int)`. Insert 100 accounts, each with balance 1000. Total = 100,000. Workload: 10 concurrent clients performing random transfers: `BEGIN TRANSACTION; SELECT balance FROM test.accounts WHERE id = ?; SELECT balance FROM test.accounts WHERE id = ?; UPDATE test.accounts SET balance = balance - ? WHERE id = ?; UPDATE test.accounts SET balance = balance + ? WHERE id = ?; COMMIT`. Transfer amount: random 1-100. Concurrent readers: 3 clients summing all balances via `SELECT SUM(balance) FROM test.accounts` (outside transactions, at QUORUM). Duration: 120 seconds. Nemesis: `partition` + `kill` (alternating, 30s intervals). Pass criterion: every balance-sum read returns exactly 100,000. If any read returns a different total, atomicity is violated. Post-run: sum all balances from each node individually, assert all equal 100,000. |
| `jepsen_bank_no_negative_balance` | Transaction reads within `BEGIN TRANSACTION` see consistent state. No account goes negative when the application checks balances before transferring. | Same setup (100 accounts, balance 1000 each). Workload: 10 concurrent clients performing conditional transfers: `BEGIN TRANSACTION; SELECT balance FROM test.accounts WHERE id = ?; -- if balance >= amount: UPDATE balance - amount; UPDATE counterparty + amount; COMMIT; -- else: ROLLBACK`. Amount: random 1-100. Nemesis: `partition` + `kill`. Duration: 120 seconds. Pass criterion: after the run, `SELECT MIN(balance) FROM test.accounts` returns a value >= 0 on all nodes. No account balance is ever negative. This tests that reads within a transaction reflect the transaction's isolation level. |

---

## 4. Jepsen: Write-Skew (S5.7 -- Phase 2 Gate)

Write-skew is the canonical anomaly that snapshot isolation allows but strict
serializability prevents. If Accord permits write-skew, its isolation guarantee
is weaker than claimed.

| Test | What It Proves | How |
|------|---------------|-----|
| `jepsen_write_skew` | Accord provides strict serializable isolation: concurrent transactions on shared state cannot both commit based on a stale read. No lost updates. | Setup: `CREATE TABLE test.counter (id int PRIMARY KEY, value int)`. Insert `(1, 10)`. Workload: 2 concurrent clients repeatedly executing: `BEGIN TRANSACTION; SELECT value FROM test.counter WHERE id = 1; -- decrement by 1: UPDATE test.counter SET value = <read_value - 1> WHERE id = 1; COMMIT`. Under strict serializability, if T1 reads 10 and writes 9, T2 must read 9 (not 10) and write 8. Final value after both = 8. Under snapshot isolation (write-skew), both read 10, both write 9. Final value = 9 (lost update). Duration: 120 seconds with many such decrement rounds. Nemesis: `partition` + `kill`. Pass criterion: for every pair of concurrent decrements, the final value reflects both operations. Track all committed writes and verify the counter's final value equals `initial - count(committed_decrements)`. No lost updates detected. |

---

## 5. Jepsen: Long-Fork (S7.7)

The long-fork (G2) anomaly occurs in systems with weaker isolation: two
transactions both observe the initial state and commit non-conflicting writes,
creating a fork in causal history that violates serializability.

| Test | What It Proves | How |
|------|---------------|-----|
| `jepsen_long_fork` | Accord prevents the G2 (anti-dependency cycle) anomaly. No long-fork interpretation exists in the committed history. | Setup: `CREATE TABLE test.state (key text PRIMARY KEY, value int)`. Insert keys A and B with initial values. Workload: 2 concurrent transaction types. T1: `BEGIN TRANSACTION; SELECT * FROM test.state WHERE key = 'A'; SELECT * FROM test.state WHERE key = 'B'; INSERT INTO test.state (key, value) VALUES ('C', ?); COMMIT`. T2: `BEGIN TRANSACTION; SELECT * FROM test.state WHERE key = 'A'; SELECT * FROM test.state WHERE key = 'B'; INSERT INTO test.state (key, value) VALUES ('D', ?); COMMIT`. The long-fork anomaly: both T1 and T2 observe the initial state of A and B, and both commit writes to distinct keys (C and D). Under serializability, at least one must observe the other's pre-condition check on A/B after the other has modified state. Duration: 120 seconds. Nemesis: `partition` + `kill`. Checker: Knossos or Elle (Jepsen's G2 checker). Pass criterion: no long-fork interpretation found. |

---

## 6. Jepsen: Monotonic Reads (S7.7)

Monotonic reads require that once a client observes a value, it never
subsequently observes an older value for the same key.

| Test | What It Proves | How |
|------|---------------|-----|
| `jepsen_monotonic_reads` | A single client's read sequence is monotonically non-decreasing in Accord timestamp order. No time travel: a value once observed is never replaced by an older value. | Setup: `CREATE TABLE test.mono (key int PRIMARY KEY, value int, ts bigint)`. Key = 1. Workload: 1 dedicated reader client issuing `SELECT value, ts FROM test.mono WHERE key = 1` in a tight loop. 4 concurrent writer clients issuing `INSERT INTO test.mono (key, value, ts) VALUES (1, ?, ?)` with monotonically increasing values and timestamps from the client's HLC. Duration: 120 seconds. Nemesis: `partition` + `kill`. Pass criterion: the reader's observed sequence of `(value, ts)` pairs is monotonically non-decreasing by `ts`. If the reader ever observes `ts_n > ts_{n+1}`, monotonic reads are violated. Record all observations and verify post-run. |

---

## 7. Jepsen: Full Nemesis (S7.7 -- Phase 4 Gate)

The full nemesis suite is the ultimate stress test: all workloads running
simultaneously with all nemesis operations active. This is the Phase 4 gate.

| Test | What It Proves | How |
|------|---------------|-----|
| `jepsen_full_nemesis_suite` | Accord maintains all correctness properties simultaneously under maximum adversarial conditions. If this passes, the protocol implementation is correct under all tested failure modes. | Run ALL Jepsen workloads concurrently on the same 3-node cluster: register (5 clients), bank (10 clients), long-fork (2 clients), monotonic reads (5 clients), write-skew (2 clients). Each workload operates on its own keyspace to avoid cross-workload interference. All nemesis operations active simultaneously: `partition` (30s events every 90s), `kill` (minority kill every 60s, restart within 5s), `slow` (100ms jitter on 20% of links, toggled every 45s), `clock-skew` (+/-5ms every 15s), `pause` (10s SIGSTOP every 120s). Duration: 300 seconds. Pass criterion: ALL checkers pass independently. Knossos reports `{:valid? true}` for register. Bank total is exactly 100,000. No long-fork found. Monotonic reads sequence is non-decreasing. No write-skew detected. Post-run: all nodes converge to consistent state after nemesis stops (60s convergence window). |

---

## 8. Chaos: Minority Kill (S7.8 -- Phase 4 Gate)

Chaos tests differ from Jepsen tests in that they focus on recovery mechanics
and timing rather than abstract correctness properties. These tests verify that
the recovery protocol completes within bounded time and that no commits are
lost.

| Test | What It Proves | How |
|------|---------------|-----|
| `chaos_minority_kill_no_lost_commits` | Killing a minority node during active Accord transactions causes zero data loss. All committed transactions are durable. All in-flight transactions either commit or abort cleanly. | Setup: 3-node cluster, test keyspace with RF=3. Start 100 concurrent Accord transactions (each writing to a unique key): `INSERT INTO test.chaos (key, value) VALUES (?, ?)`. After 50 transactions have entered PreAccept (verified by monitoring `metrics_accord_txn_in_flight`), `kill -9` one node (minority). Allow remaining 2 nodes to process all 100 transactions. Restart killed node after 5s. Wait for recovery to complete (monitor `metrics_accord_recovery_in_progress` dropping to 0). Assertions: (1) all 100 transactions have a terminal state (committed or aborted, never stuck), (2) every committed transaction's value is readable from all 3 nodes after convergence, (3) zero data loss -- the count of committed values readable from the cluster equals the count of client-acknowledged commits, (4) no duplicate applies (each key has exactly 1 value, not 2). |
| `chaos_minority_kill_recovery_time` | Recovery of in-flight transactions completes within a bounded time proportional to the failure detector timeout. The system does not hang. | Same setup as above (3 nodes, 100 concurrent transactions, kill 1 after 50 PreAccepts). Measure: (1) time from `kill -9` to failure detector marking the node as suspected (`metrics_accord_recovery_in_progress` first increment), (2) time from suspicion to all stuck transactions completing (metric drops to 0). Assertions: (1) failure detection occurs within 2x the heartbeat timeout (default 5s, so within 10s), (2) recovery of all stuck transactions completes within 2x the failure detector timeout after detection (so within 20s total from kill), (3) no transaction is in recovery for more than 30s (the hard cap from ProgressLog exponential backoff). |

---

## 9. Performance Benchmarks (S4.6, S7.9)

Performance benchmarks establish baselines and detect regressions. Each
benchmark runs on a 3-node cluster with RF=3 and no nemesis operations.
Results are recorded in JSON for cross-run comparison.

### 9.1 Single-Key Benchmarks

| Test | What It Proves | How |
|------|---------------|-----|
| `perf_single_key_write_p50` | Accord single-key write latency is within acceptable overhead of the current QUORUM write path. | Benchmark: 10,000 sequential single-key INSERTs via CQL: `INSERT INTO test.perf (key, value) VALUES (?, ?)` with unique keys. Measure P50 latency (client-observed, including network). Compare against a baseline run with the same cluster configuration but Accord disabled (tunable CL QUORUM writes). Assertion: P50 with Accord <= 1.15x P50 without Accord (within +15%). Record both values in `benchmark-results.json`. |
| `perf_single_key_write_p99` | Accord tail latency is bounded. The ReorderBuffer and protocol overhead do not cause excessive tail latency spikes. | Same benchmark as above (10,000 INSERTs). Measure P99 latency. Assertion: P99 with Accord <= 1.25x P99 without Accord (within +25%). P99 must also be < 50ms absolute (hard cap for single-key writes on a local cluster). |
| `perf_single_key_read_p50` | Linearizable reads via Accord are no slower than (and potentially faster than) digest-based QUORUM reads, because Accord eliminates the digest read round trip in the no-conflict case. | Benchmark: 10,000 sequential single-key SELECTs via CQL: `SELECT value FROM test.perf WHERE key = ?`. Pre-populate 10,000 rows. Measure P50. Compare against QUORUM baseline. Assertion: P50 with Accord <= 0.90x P50 without Accord (improvement expected, within -10%). If reads are slower, this flags a performance issue in the linearizable read path's ConflictIndex lookup. |

### 9.2 Multi-Key Benchmarks

| Test | What It Proves | How |
|------|---------------|-----|
| `perf_multi_key_txn_p50` | Multi-key transactions (the slow path) have bounded overhead relative to single-key writes. | Benchmark: 1,000 two-partition `BEGIN TRANSACTION` blocks. Each transaction reads from partition A and writes to partition B: `BEGIN TRANSACTION; SELECT value FROM test.perf WHERE key = ?; UPDATE test.perf SET value = ? WHERE key = ?; COMMIT`. Measure P50 latency. Assertion: P50 < 2x single-key write P50 (the slow path adds at most 1 extra RTT). |

### 9.3 Internal Component Benchmarks

| Test | What It Proves | How |
|------|---------------|-----|
| `perf_conflict_index_lookup_p99` | ConflictIndex lookup is sub-microsecond, not a bottleneck on the write path. | Benchmark: populate a ConflictIndex with 500 entries (mix of single-key and range). Perform 100,000 `max_conflicting_timestamp()` calls with random keys (50% hitting existing entries, 50% misses). Measure P99 latency per call. Assertion: P99 < 50us. This is a micro-benchmark run in-process (no network), measuring only the data structure performance. |
| `perf_reorder_buffer_overhead_p99` | The ReorderBuffer's deadline wait does not add excessive latency to the commit path. | Benchmark: configure SkewMax = 10ms, network latency = 2ms. Enqueue 10,000 messages with timestamps spread across a 100ms window. Measure the P99 of (delivery_time - enqueue_time - expected_deadline_wait). Assertion: P99 overhead (above the expected deadline) < 5ms. This isolates the TimerWheel's scheduling jitter from the intentional deadline wait. |

### 9.4 Regression Gate

| Test | What It Proves | How |
|------|---------------|-----|
| `perf_regression_suite` | No performance regression between builds. Acts as a CI gate for performance-sensitive changes. | Run all benchmarks above (single-key write P50/P99, single-key read P50, multi-key txn P50, conflict index P99, reorder buffer P99). Record results in `benchmark-results.json` with timestamp, git SHA, and all measurements. Compare against the previous run's results (loaded from the same file or a baseline artifact). Flag any measurement that regresses beyond its threshold: write P50 > +15%, write P99 > +25%, read P50 > -10% (i.e., reads got slower), multi-key P50 > 2x single-key, conflict index P99 > 50us, reorder buffer P99 > 5ms. If any threshold is violated, the test fails with a report showing the regression. |

---

## 10. ExclusiveSyncPoint and DurabilityService

**Project plan gap:** The spec (`accord.md` task S4.9) and FMEA (FM11) reference
ExclusiveSyncPoint for GC coordination, and the threat model (AT22, A10) calls
out DurabilityService as a component. However, **no project plan task** covers
the ExclusiveSyncPoint/DurabilityService lifecycle: when it triggers, what it
GC's, how it interacts with the sidecar file and protocol log, or what happens
when it stalls. This gap must be closed with a new task (suggest S4.11 or a
dedicated GC sprint).

### 10.1 What ExclusiveSyncPoint Does

After an Accord transaction reaches `Applied` on ALL shards, three things can
be garbage-collected:

1. **Protocol log entries** (PreAccepted, Accepted, Committed for that TxnId)
2. **Sidecar file entries** (the `.accord` companion file written alongside the
   SSTable at flush time, per S4.9)
3. **ConflictIndex entries** (already removed at Apply time per S2.4, but the
   ExclusiveSyncPoint confirms it is safe to do so across shards)

The DurabilityService is a periodic background task that scans for transactions
where all shards have reached Applied, then triggers ExclusiveSyncPoint for
those transactions.

### 10.2 ExclusiveSyncPoint Tests

| Test | What It Proves | How |
|------|---------------|-----|
| `exclusive_sync_point_all_shards_applied` | When all shards have Applied a transaction, ExclusiveSyncPoint triggers and enables GC of protocol log and sidecar entries. | Setup: 3-node cluster, transaction T1 that touches shards on all 3 nodes. Drive T1 through the full protocol to Applied on all 3 shards. Trigger ExclusiveSyncPoint (either by waiting for DurabilityService or calling it manually). Assertions: (1) T1's protocol log entries (PreAccepted, Accepted, Committed) are eligible for GC -- after the next protocol log GC cycle, they are removed, (2) T1's entry in the `.accord` sidecar file is marked for deletion -- after the next sidecar GC cycle, the entry is removed, (3) T1's ConflictIndex entry was already removed at Apply time (S2.4) -- verify it is still absent, (4) T1's data in the memtable/SSTable is NOT affected (only metadata is GC'd). |
| `exclusive_sync_point_partial_applied` | ExclusiveSyncPoint does NOT trigger for a transaction that has not reached Applied on all shards. Premature GC is prevented. | Setup: 3-node cluster, transaction T1. T1 is Applied on shard A (node 1) and shard B (node 2), but shard C (node 3) is slow -- T1 is still in Committed state on shard C (e.g., dep-wait on another transaction). Run DurabilityService scan. Assertions: (1) ExclusiveSyncPoint does NOT trigger for T1, (2) T1's protocol log entries are retained on all nodes, (3) T1's sidecar file entries are retained, (4) no GC occurs for T1's metadata. After T1 finally reaches Applied on shard C, re-run DurabilityService. Assert: ExclusiveSyncPoint now triggers for T1, and GC proceeds. |
| `exclusive_sync_point_mixed_batch` | ExclusiveSyncPoint correctly handles a mix of fully-applied and partially-applied transactions in the same scan. | Setup: 5 transactions. T1, T2, T3 are Applied on all shards. T4 is Applied on 2/3 shards. T5 is still in Committed state. Run DurabilityService. Assertions: (1) ExclusiveSyncPoint triggers for T1, T2, T3 -- their metadata is GC-eligible, (2) T4 and T5 are NOT GC-eligible, (3) after GC, protocol log size has decreased, (4) T4 and T5's entries are intact. |

### 10.3 DurabilityService Tests

| Test | What It Proves | How |
|------|---------------|-----|
| `durability_service_periodic` | DurabilityService runs on a configurable interval and triggers ExclusiveSyncPoint for eligible transactions without operator intervention. | Configure DurabilityService with interval = 5s (shortened for testing; default 60s). Insert 10 transactions, drive all to Applied on all shards. Wait for 2 DurabilityService cycles (10s). Assertions: (1) ExclusiveSyncPoint triggered at least once for the 10 transactions, (2) protocol log entries for the 10 transactions are GC'd, (3) DurabilityService ran at least twice (verify via metrics counter `durability_service_runs_total`). Change interval to 2s. Verify the next cycle runs within 3s. |
| `durability_service_under_load` | DurabilityService continues to run even under high transaction throughput. GC is not starved by the write path. | Start a sustained write workload: 100 transactions/second for 30 seconds (3,000 total). DurabilityService interval = 10s. Assertions: (1) DurabilityService ran at least 2 times during the 30s window (not starved), (2) protocol log size does not grow unboundedly -- after the workload ends and 2 more DurabilityService cycles pass, protocol log size has decreased, (3) `metrics_protocol_log_size_bytes` gauge shows a sawtooth pattern (growing during writes, shrinking after GC). |
| `durability_service_health_check` | DurabilityService self-monitors and alerts when GC is stalling. Operators are warned before the protocol log or ConflictIndex grows dangerously large. | Simulate a GC stall: configure DurabilityService to run but prevent ExclusiveSyncPoint from completing (e.g., inject an error in the all-shards-applied check). Run for 90s. Assertions: (1) after 60s without a successful ExclusiveSyncPoint, a WARN-level log message is emitted, (2) after 300s (simulated via clock acceleration) without success, an ERROR-level log message is emitted, (3) `durability_service_stall_detected` metric is incremented, (4) the health check does not crash the DurabilityService -- it continues retrying. After the injected error is removed, verify the next cycle succeeds and the stall metric stops incrementing. |
| `durability_service_startup_catch_up` | After a node restart, DurabilityService catches up on GC that was missed during downtime. | Insert 50 transactions, all Applied on all shards. Restart one node (simulate downtime). After restart, wait for DurabilityService first cycle. Assertions: (1) all 50 transactions' metadata is GC-eligible on the restarted node, (2) GC completes within 2 DurabilityService cycles after restart, (3) protocol log size on the restarted node matches the other nodes after catch-up. |

---

## 11. Observability and Metrics

**Project plan gap:** The threat model references several metrics (AT19: SkewMax
alarm, AT21: recovery gauge, AT22: ConflictIndex health check) and the FMEA
assumes their existence (FM8: ReorderBuffer overflow detection, FM13: deadlock
counter). However, no project plan task covers implementing or testing these
metrics. A new task (suggest S3.9 or a metrics sprint) is required.

All metrics below are Prometheus-format, exposed on the `/metrics` HTTP endpoint.
Each test verifies that the metric exists, is correctly instrumented, and has
the expected behavior under controlled conditions.

### 11.1 Transaction Lifecycle Metrics

| Test | What It Proves | How |
|------|---------------|-----|
| `metrics_accord_txn_in_flight` | The in-flight transaction gauge accurately tracks active Accord transactions per shard. Operators can monitor transaction backlog. | Start 10 concurrent Accord transactions (do not commit yet -- hold them in PreAccept). Scrape `/metrics`. Assert: `ferrosa_accord_txn_in_flight` gauge >= 10 (may be slightly higher due to internal transactions). Commit all 10. Scrape again. Assert: gauge decreased by 10. Verify: gauge is incremented at PreAccept, decremented at Apply or abort. Never goes negative. |
| `metrics_accord_recovery_in_progress` | The recovery gauge tracks active recovery operations. Sustained high values indicate cluster health issues. | Kill a coordinator mid-transaction (after PreAccept, before Commit). Wait for recovery to trigger. Scrape `/metrics`. Assert: `ferrosa_accord_recovery_in_progress` gauge >= 1 on the recovery coordinator node. After recovery completes, scrape again. Assert: gauge returns to 0. Verify: gauge incremented when RecoveryCoordinator starts, decremented when recovery reaches terminal state (Applied or Invalidated). Alarm threshold: assert that a sustained value > 100 would trigger an alert rule (verify the alert rule exists in the metrics exposition, not the alerting infrastructure). |
| `metrics_accord_fast_path_ratio` | The fast-path ratio metric enables operators to understand contention levels. High contention drives the ratio down. | Run 100 single-key writes to unique keys (no contention). Scrape `/metrics`. Compute ratio: `ferrosa_accord_fast_path_commits / ferrosa_accord_total_commits`. Assert: ratio > 0.95 (> 95% fast path under no contention). Run 100 writes to the SAME key (high contention). Scrape again. Assert: ratio decreased (some transactions forced to slow path). Both counters are monotonically increasing. |

### 11.2 Component Health Metrics

| Test | What It Proves | How |
|------|---------------|-----|
| `metrics_conflict_index_size` | The ConflictIndex size gauge enables capacity monitoring. Operators can detect approaching limits before they cause `Overloaded` errors. | Start 50 Accord transactions (hold in PreAccept, do not Apply). Scrape `/metrics`. Assert: `ferrosa_accord_conflict_index_size` gauge >= 50 per shard. Apply all 50 (triggering removal from ConflictIndex). Scrape again. Assert: gauge decreased by 50. Alarm threshold verification: assert that the metric definition includes a comment or annotation indicating the alarm threshold is > 80% of hard cap (default hard cap = 100K, so alarm at > 80K). |
| `metrics_reorder_buffer_depth` | The ReorderBuffer depth gauge enables capacity monitoring. Sustained high values indicate clock skew or slow consumers. | Enqueue 20 messages into the ReorderBuffer (with deadlines in the future). Scrape `/metrics`. Assert: `ferrosa_accord_reorder_buffer_depth` gauge >= 20. Advance the clock past all deadlines (deliver all messages). Scrape again. Assert: gauge returns to 0. Alarm threshold: > 80% of configured capacity. |
| `metrics_skew_max_ns` | The SkewMax gauge exposes the current measured clock skew. Operators can detect clock drift before it causes correctness issues. | On a 3-node cluster with synchronized clocks, scrape `/metrics`. Assert: `ferrosa_accord_skew_max_ns` is < 50ms (clocks are well-synchronized). Inject 5ms clock skew on one node via `chronyc makestep`. Wait for 2 heartbeat cycles. Scrape again. Assert: SkewMax increased (reflects the injected skew). Alarm threshold: > 200ms. |
| `metrics_protocol_log_size_bytes` | The protocol log size gauge enables GC monitoring. Large values indicate DurabilityService may be stalled. | Write 100 Accord transactions through the full protocol. Scrape `/metrics`. Assert: `ferrosa_accord_protocol_log_size_bytes` > 0 (entries exist). Trigger DurabilityService GC (all transactions are Applied). Scrape again. Assert: gauge decreased. Alarm threshold: > 1GB (GC may be stalled). |

### 11.3 Anomaly Detection Metrics

| Test | What It Proves | How |
|------|---------------|-----|
| `metrics_dep_wait_duration_p99` | The dep-wait duration histogram enables latency monitoring. High dep-wait times indicate contention or stuck dependencies. | Create a dependency chain: T1 -> T2. T2 must wait for T1 to Apply. Delay T1's Apply by 50ms (inject artificial delay). T2's dep-wait duration should be ~50ms. Scrape `/metrics`. Assert: `ferrosa_accord_dep_wait_duration_seconds` histogram has observations in the 50ms bucket. Under normal operation (no artificial delays), P99 should be < 100ms. Alarm threshold: P99 > 100ms. |
| `metrics_deadlock_detected` | The deadlock counter tracks detected and broken dependency cycles. Any nonzero value is operationally significant. | Create a circular dependency: T1 depends on T2, T2 depends on T1. Wait for deadlock detection to trigger. Scrape `/metrics`. Assert: `ferrosa_accord_deadlock_detected_total` counter >= 1. Verify: counter increments by exactly 1 per detected cycle (not per involved transaction). This counter should be 0 under normal operation -- any nonzero value warrants investigation. |
