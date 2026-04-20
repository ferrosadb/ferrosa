# Accord Coverage Review

> Generated: 2026-04-18
> Branch: plan/gap-closure-sprint
> Scope: A1-A7 sprints — all Accord-related source, specs, and CI

---

## 1. Feature Inventory

All 27 inventoried features are present in code.

| Feature | File : First Significant Line |
|---------|-------------------------------|
| **PreAccept handler** | `ferrosa-cluster/src/accord/handlers.rs:107` |
| **Accept handler** | `ferrosa-cluster/src/accord/handlers.rs:138` |
| **Commit handler** | `ferrosa-cluster/src/accord/handlers.rs:163` |
| **Apply handler** | `ferrosa-cluster/src/accord/handlers.rs:179` |
| **Recover handler** | `ferrosa-cluster/src/accord/handlers.rs:195` |
| **AccordStateMachine** | `ferrosa-cluster/src/accord/state_machine.rs:72` |
| **AccordCoordinator (fast/slow path)** | `ferrosa-cluster/src/accord/coordinator.rs:140` |
| **LWT (INSERT IF NOT EXISTS, UPDATE/DELETE IF)** | `ferrosa-cql/src/accord_router.rs:103` |
| **BEGIN TRANSACTION / COMMIT / ROLLBACK** | `ferrosa-cql/src/accord_router.rs:1` (routing), `ferrosa-cql/src/transaction_keys.rs` |
| **Cross-shard conflict detection** | `ferrosa-cluster/src/accord/cross_shard.rs:240` |
| **CrossShardCoordinator** | `ferrosa-cluster/src/accord/cross_shard.rs:77` |
| **Crash recovery (sidecar replay)** | `ferrosa-storage/src/accord/crash_recovery.rs:68` |
| **Electorate reconfiguration (4-gate join)** | `ferrosa-cluster/src/accord/electorate.rs:135` |
| **EpochDrain (transition drain)** | `ferrosa-cluster/src/accord/epoch_drain.rs:81` |
| **HLC Timestamp** | `ferrosa-common/src/accord.rs:28` |
| **TxnId** | `ferrosa-common/src/accord.rs:84` |
| **BallotNumber / AcceptedBallot / PromisedBallot** | `ferrosa-common/src/accord.rs:111–148` |
| **ConflictIndex** | `ferrosa-storage/src/accord/conflict_index.rs:74` |
| **ProtocolLog (durable WAL)** | `ferrosa-storage/src/accord/protocol_log.rs:21` |
| **SyncWriter (fsync-before-ack)** | `ferrosa-storage/src/accord/sync_writer.rs:50` |
| **WriteGate (DDL drain-and-block)** | `ferrosa-storage/src/accord/write_gate.rs:15` |
| **read_2i (5-layer transactional 2i merge)** | `ferrosa-storage/src/accord/read_2i.rs:117` |
| **Sidecar files (.accord format)** | `ferrosa-storage/src/accord/sidecar.rs:59` |
| **DepWaitGraph (cycle detection)** | `ferrosa-cluster/src/accord/dep_wait.rs:86` |
| **ReorderBuffer** | `ferrosa-cluster/src/accord/reorder_buffer.rs:63` |
| **Jepsen bank harness** | `ferrosa-cluster/src/accord/jepsen_bank.rs:1` |
| **Accord internode messages (11 types)** | `ferrosa-net/src/accord_messages.rs:8` |

Additional modules confirmed present (not individually listed in specs but all implemented):

- `accord/metrics.rs` — 9 Prometheus counters/histograms
- `accord/leaseholder.rs` — linearizable local reads
- `accord/linearizable_read.rs` — leaseholder fast-read manager
- `accord/two_phase_ddl.rs` — DDL coordination under Accord
- `accord/ddl_drain.rs` — DdlDrainGuard
- `accord/durability.rs` — ExclusiveSyncPoint + DurabilityService
- `accord/uda_integration.rs` — WASM UDA execution in transactions
- `accord/perf.rs` / `accord/perf_regression.rs` — performance baselines
- `accord/epoch.rs` — EpochTracker
- `accord/recovery.rs` / `accord/recovery_scenarios.rs` — RecoveryCoordinator + 11 scenarios
- `accord/chaos_minority_kill.rs` — minority-partition chaos test
- `accord/jepsen_nemesis.rs` — NemesisController
- `accord/proptests.rs` — 4 property-based consensus invariant tests
- `accord/test_cluster.rs` — deterministic TestCluster harness

**Note on MemIndex:** `accord.md` describes "MemIndex" as a BTreeMap-backed Accord-specific component. In the codebase it lives in `ferrosa-storage/src/memtable/mem_index.rs`, not under the `accord/` subtree. The `read_2i` module references it as `LayerId::MemIndex`. The spec description is accurate to the behavior; the module boundary is slightly misrepresented (it is a general memtable component, not an Accord-only one).

---

## 2. Spec Coverage Matrix

| Feature | Spec Coverage | Status |
|---------|--------------|--------|
| PreAccept / Accept / Commit / Apply / Recover | `accord.md` §Protocol Phases; `accord-test-spec.md` Layer 3 | Current |
| AccordStateMachine | `accord.md` §AccordStateMachine; `accord-test-spec.md` Layer 4 | Current |
| AccordCoordinator fast/slow path | `accord.md` §AccordCoordinator; `project-plan-accord.md` A3 | Current |
| LWT (INSERT IF NOT EXISTS / IF conditions) | `accord.md` §LWT Statements; `accord-test-integration.md` | Current |
| BEGIN TRANSACTION / COMMIT / ROLLBACK | `accord.md` §Multi-Statement Transactions; `project-plan-accord.md` A6 | Current |
| Cross-shard conflict detection | `accord.md` §CrossShard; `accord-test-multikey-electorate.md` | Current |
| Crash recovery (sidecar replay) | `accord.md` §Sidecar Files; `accord-test-memindex-2i.md` §2 | Current |
| Electorate reconfiguration | `accord.md` §Electorate Reconfiguration; `accord-test-multikey-electorate.md` | Current |
| HLC Timestamp + TxnId + Ballot types | `accord.md` §Core Components; `accord-test-spec.md` Layer 1 | Current |
| ConflictIndex | `accord.md` §ConflictIndex; `fmea-accord.md` FM3/FM11 | Current |
| ProtocolLog durable WAL | `accord.md` §ProtocolLog | Current |
| SyncWriter (fsync-before-ack) | `accord.md` §SyncWriter; `fmea-accord.md` FM5 | Current |
| WriteGate (DDL drain-and-block) | `accord.md` §WriteGate; `project-plan-accord.md` A1 | Current |
| read_2i (5-layer 2i merge) | `accord.md` A7 table; `accord-test-memindex-2i.md` §3 | Current |
| DepWaitGraph (cycle detection) | `accord.md` §DepWaitGraph; `fmea-accord.md` FM13 | Current |
| ReorderBuffer | `accord.md` §ReorderBuffer; `fmea-accord.md` FM8 | Current |
| Jepsen bank harness | `accord.md` §Testing; `project-plan-accord.md` A6 | Current |
| Accord internode messages | `accord.md` §Protocol Phases; `accord-test-spec.md` Layer 3 | Current |
| AccordMetrics (9 metrics) | `accord.md` §Observability (table lists all 9) | Current |
| MemIndex (transactional 2i) | `accord.md` §MemIndex; `accord-test-memindex-2i.md` §1 | **Minor gap: spec places MemIndex in Accord subtree; actual code is `ferrosa-storage/src/memtable/mem_index.rs`** |
| FMEA (all 21 failure modes) | `fmea-accord.md` — all marked "Implemented" | Current |
| Threat model (STRIDE) | `threat-model-accord.md` | Current |
| DSM coupling analysis | `dsm-accord.md` | Current |
| metrics.rs | `accord.md` §Observability | No dedicated spec section beyond the table — acceptable for a thin monitoring wrapper |
| perf.rs / perf_regression.rs | `project-plan-accord.md` A7 (performance regression suite listed) | Current |
| uda_integration.rs (18 tests) | `accord-test-udf-integration.md`; `accord.md` §UDF/UDA | Current |
| two_phase_ddl.rs | `accord.md` A7 table; `accord-test-multikey-electorate.md` | Current |

**Spec-to-code mismatches found:** 1 minor, 1 quantitative.

1. **MemIndex location** (minor): `accord.md` implies MemIndex is an Accord-layer component. It is a general memtable module reused by Accord's read_2i path. The spec is behaviorally accurate but architecturally imprecise.

2. **State machine test count** (minor): `accord.md` states "39 unit tests." Actual count is 41. No functional gap, but the spec is stale by 2 tests. Similarly the project plan cites "~50" coordinator tests vs. the measured 14 — the project plan test-count table uses approximated bucket totals, not direct counts.

3. **2,808 total test claim**: The project plan documents this as "combined with existing test suite (~2,300), total reaches ~2,808." This is a derived aggregate of the whole workspace. The directly attributable Accord unit tests across all accord/ subdirectories measure **282**, plus 26 Accord-related integration tests = ~308 tests in Accord-specific files. The broader 2,808 figure encompasses all crates and is not independently verifiable at this commit without running `cargo test --workspace 2>&1 | grep "test result"`. The spec's framing is accurate but should not be cited as "2,808 Accord tests."

---

## 3. Test Coverage

### 3.1 Unit Tests (in-source, always run)

| Suite | File | Count | CI run |
|-------|------|--------|--------|
| AccordStateMachine | `ferrosa-cluster/src/accord/state_machine.rs` | 41 | Every PR |
| AccordCoordinator | `ferrosa-cluster/src/accord/coordinator.rs` | 14 | Every PR |
| RecoveryCoordinator | `ferrosa-cluster/src/accord/recovery.rs` | 7 | Every PR |
| RecoveryScenarios (11 scenarios) | `ferrosa-cluster/src/accord/recovery_scenarios.rs` | 11 | Every PR |
| CrossShard | `ferrosa-cluster/src/accord/cross_shard.rs` | 10 | Every PR |
| DepWaitGraph | `ferrosa-cluster/src/accord/dep_wait.rs` | 13 | Every PR |
| Electorate | `ferrosa-cluster/src/accord/electorate.rs` | 6 | Every PR |
| DdlDrain | `ferrosa-cluster/src/accord/ddl_drain.rs` | 4 | Every PR |
| Leaseholder | `ferrosa-cluster/src/accord/leaseholder.rs` | 5 | Every PR |
| DurabilityService | `ferrosa-cluster/src/accord/durability.rs` | 7 | Every PR |
| ReorderBuffer | `ferrosa-cluster/src/accord/reorder_buffer.rs` | 5 | Every PR |
| LinearizableRead | `ferrosa-cluster/src/accord/linearizable_read.rs` | 4 | Every PR |
| EpochDrain | `ferrosa-cluster/src/accord/epoch_drain.rs` | 3 | Every PR |
| TwoPhaseDdl | `ferrosa-cluster/src/accord/two_phase_ddl.rs` | 4 | Every PR |
| UDA integration (18 tests) | `ferrosa-cluster/src/accord/uda_integration.rs` | 15 | Every PR |
| Jepsen bank / write-skew | `ferrosa-cluster/src/accord/jepsen_bank.rs` | 3 | Every PR |
| Jepsen nemesis (in-source) | `ferrosa-cluster/src/accord/jepsen_nemesis.rs` | 3 | Every PR |
| Chaos minority kill | `ferrosa-cluster/src/accord/chaos_minority_kill.rs` | 2 | Every PR |
| Property-based tests | `ferrosa-cluster/src/accord/proptests.rs` | 4 | Every PR (nightly: 50k cases) |
| TestCluster harness | `ferrosa-cluster/src/accord/test_cluster.rs` | 6 | Every PR |
| Clock / ClockValidation | `ferrosa-cluster/src/accord/clock.rs` + `clock_validation.rs` | 8 | Every PR |
| Epoch / EpochTracker | `ferrosa-cluster/src/accord/epoch.rs` | 3 | Every PR |
| AccordMetrics | `ferrosa-cluster/src/accord/metrics.rs` | 9 | Every PR |
| Perf / PerfRegression | `ferrosa-cluster/src/accord/perf.rs` + `perf_regression.rs` | 7 | Every PR |
| ConflictIndex | `ferrosa-storage/src/accord/conflict_index.rs` | 11 | Every PR |
| CrashRecovery | `ferrosa-storage/src/accord/crash_recovery.rs` | 4 | Every PR |
| ProtocolLog | `ferrosa-storage/src/accord/protocol_log.rs` | 5 | Every PR |
| Read2i | `ferrosa-storage/src/accord/read_2i.rs` | 8 | Every PR |
| SyncWriter | `ferrosa-storage/src/accord/sync_writer.rs` | 6 | Every PR |
| Sidecar | `ferrosa-storage/src/accord/sidecar.rs` | 6 | Every PR |
| WriteGate | `ferrosa-storage/src/accord/write_gate.rs` | 4 | Every PR |
| Entries (serialization) | `ferrosa-storage/src/accord/entries.rs` | 2 | Every PR |
| OversizedEntry | `ferrosa-storage/src/accord/oversized_entry.rs` | 3 | Every PR |
| HLC / Timestamp / Ballot | `ferrosa-common/src/accord.rs` | 28 | Every PR |
| AccordMessages | `ferrosa-net/src/accord_messages.rs` | 5 | Every PR |
| AccordRouter (LWT) | `ferrosa-cql/src/accord_router.rs` | 15 | Every PR |
| **Accord unit total** | | **282** | Every PR |

### 3.2 Integration Tests

| Suite | File | Count | CI run |
|-------|------|--------|--------|
| EPaxos 24-step correctness | `ferrosa-cluster/tests/epaxos_correctness.rs` | 2 | Every PR |
| Cluster correctness (batch_atomicity, recovery_coordinator, clock_skew, pause_resume) | `ferrosa-cluster/tests/correctness.rs` | 10 | **5 of 10 skipped on PR CI** (see §3.3) |
| Accord nemesis (packet_reorder, lwt_batch_atomicity) | `ferrosa-cluster/tests/accord_nemesis.rs` | 3 | **2 of 3 skipped on PR CI** |
| Storage correctness (dep_wait_ordering, batch_atomicity) | `ferrosa-storage/tests/correctness.rs` | 5 | **2 of 5 skipped on PR CI** |
| Jepsen nemesis correctness | `ferrosa-jepsen/tests/nemesis_correctness.rs` | 6 | **crate excluded from PR CI** |
| **Integration total** | | **26** | |

### 3.3 CI Coverage Gaps

**PR CI (`ci.yml`):**

```
cargo test --workspace --exclude ferrosa-jepsen --exclude ferrosa-loadgen
    -- --skip batch_atomicity
       --skip pause_resume
       --skip recovery_coordinator
       --skip dep_wait_ordering
       --skip packet_reorder_linearizability
       --skip lwt_batch_atomicity_all
       --skip clock_skew_large_preaccept
```

Nine Accord-related test names are explicitly skipped on every PR build:

| Skipped test | File |
|---|---|
| `batch_atomicity_kill_coordinator` | `ferrosa-cluster/tests/correctness.rs` + `ferrosa-storage/tests/correctness.rs` |
| `pause_resume_state_convergence` | `ferrosa-cluster/tests/correctness.rs` |
| `recovery_coordinator_activation` | `ferrosa-cluster/tests/correctness.rs` |
| `recovery_coordinator_resolves_inflight` | `ferrosa-cluster/tests/correctness.rs` |
| `dep_wait_ordering_under_partition` | `ferrosa-storage/tests/correctness.rs` |
| `packet_reorder_linearizability` | `ferrosa-cluster/tests/accord_nemesis.rs` |
| `lwt_batch_atomicity_all_nemeses` | `ferrosa-cluster/tests/accord_nemesis.rs` |
| `clock_skew_large_preaccept_rejection` | `ferrosa-cluster/tests/correctness.rs` |

The `--skip` patterns are substring-matched, so `--skip recovery_coordinator` also suppresses `recovery_coordinator_elect_picks_lowest_id` (a deterministic non-flaky test).

**Nightly CI (`nightly-fuzz.yml`):**

The nightly job runs `cargo test --workspace --exclude ferrosa-jepsen` with the same `--skip` list (minus `binary_`, `concurrent_write`, etc.) but with `PROPTEST_CASES=50000`. This gives the property tests meaningful fuzzing coverage. However, `ferrosa-jepsen` remains excluded — the Firecracker-based nemesis tests in `ferrosa-jepsen/tests/nemesis_correctness.rs` never run in any automated CI.

---

## 4. Gaps

### P0

**G0-1: `ferrosa-jepsen` permanently excluded from all CI.**
`ci.yml` and `nightly-fuzz.yml` both `--exclude ferrosa-jepsen`. The 6 tests in `ferrosa-jepsen/tests/nemesis_correctness.rs` — including `packet_reorder_linearizability` and `lwt_batch_atomicity_all_nemeses` (which duplicate nemesis scenarios already in the excluded-by-name lists in `ferrosa-cluster/tests/accord_nemesis.rs`) — never run automatically. This is the gap the user flagged. If the Jepsen tests require Firecracker infra that CI cannot provide, the in-`ferrosa-cluster` duplicates that exercise the same logic via `TestCluster` should run; those are also currently skipped by name. The result is that no LWT atomicity-under-nemesis test runs on any automated schedule.

### P1

**G1-1: Eight Accord integration tests permanently skipped on PR CI with no documented rationale.**
`batch_atomicity`, `pause_resume`, `recovery_coordinator_activation`, `recovery_coordinator_resolves_inflight`, `dep_wait_ordering`, `packet_reorder_linearizability`, `lwt_batch_atomicity_all`, `clock_skew_large_preaccept` cover the most critical correctness properties (linearizability under partition, LWT atomicity under crash, dep-wait ordering, clock-skew rejection). They are present in `ferrosa-cluster/tests/` — no Firecracker dependency — yet are skipped. No comment in `ci.yml` explains why. This is the primary correctness gap: these tests exist, they exercise known-risky code paths, and they are never run.

**G1-2: `--skip recovery_coordinator` over-matches and suppresses `recovery_coordinator_elect_picks_lowest_id`.**
That function is synchronous and has no known flakiness; it should run on every PR. The over-broad skip pattern should be tightened to `recovery_coordinator_activation` and `recovery_coordinator_resolves_inflight`.

### P2

**G2-1: Spec test-count figures are inaccurate.**
`accord.md` states "39 unit tests" for `AccordStateMachine` (actual: 41). `project-plan-accord.md` table shows "~50" for AccordCoordinator (actual: 14) and "~40" for RecoveryCoordinator (actual: 7+11=18). The project plan explicitly uses approximations ("~") but the spec (`accord.md`) uses hard numbers. Neither represents a correctness problem, but the spec drift will cause confusion in future coverage reviews.

**G2-2: MemIndex architectural description in `accord.md` is imprecise.**
The spec describes MemIndex as an Accord-layer component; it is a general memtable module (`ferrosa-storage/src/memtable/mem_index.rs`) that Accord's read_2i path consumes via `LayerId::MemIndex`. No behavioral gap, but the spec's module boundary description should be corrected.

---

## 5. Recommendations

1. **Unskip `recovery_coordinator_elect_picks_lowest_id` immediately.** It is synchronous, deterministic, and tests the ballot-leader-election path that prevents recovery split-brain (FM4 in FMEA). Removing the over-broad `--skip recovery_coordinator` and replacing it with precise names fixes this in one line.

2. **Run the eight skipped integration tests on nightly.** Move `batch_atomicity`, `pause_resume`, `recovery_coordinator_activation`, `recovery_coordinator_resolves_inflight`, `dep_wait_ordering`, `clock_skew_large_preaccept` from the nightly skip list. These tests use `TestCluster` (in-process, no Firecracker) and have no infrastructure dependency. If they are flaky, fix the flakiness rather than skipping — they cover P0 failure modes.

3. **Either gate `ferrosa-jepsen` on an infra env-var or schedule it in nightly.** Per the test policy, tests requiring Firecracker must `panic!` with setup instructions when `FERROSA_TEST_FIRECRACKER` is unset. Apply that pattern to the Jepsen crate so it can be included in `cargo test --workspace` without requiring the full VM fleet, and the crate exclusion can be removed.

4. **Correct the two spec inaccuracies (G2-1, G2-2).** Update `accord.md` to use actual test counts (41 for state_machine) and correct the MemIndex module path to `ferrosa-storage/src/memtable/mem_index.rs`. This is a documentation-only change, under 10 lines.

5. **Add a comment to `ci.yml` for each remaining skip.** Every `--skip` entry should have an adjacent inline comment citing the reason (e.g., `# requires real Firecracker network`) or a link to a tracking issue. Undocumented skips accumulate silently; in a codebase with a zero-`#[ignore]` policy, `--skip` in CI is the equivalent escape hatch and needs the same discipline.

---

## Sprint A1-A7 Deliverable Verification

| Sprint | Claimed Deliverable | Code Present | Tests Present | Spec Present |
|--------|---------------------|:---:|:---:|:---:|
| A1 | Timestamp / TxnId / Ballot | Yes | Yes (28) | Yes |
| A1 | ConflictIndex | Yes | Yes (11) | Yes |
| A1 | ProtocolLog | Yes | Yes (5) | Yes |
| A1 | SyncWriter | Yes | Yes (6) | Yes |
| A1 | WriteGate | Yes | Yes (4) | Yes |
| A2 | ReorderBuffer | Yes | Yes (5) | Yes |
| A2 | RecoveryCoordinator | Yes | Yes (18) | Yes |
| A2 | TestCluster harness | Yes | Yes (6) | Yes |
| A2 | 24-step EPaxos test | Yes | Yes (2 tests) | Yes |
| A2 | Accord internode messages | Yes | Yes (5) | Yes |
| A3 | AccordStateMachine | Yes | Yes (41) | Yes |
| A3 | AccordCoordinator | Yes | Yes (14) | Yes |
| A3 | CQL Router → Accord | Yes | Yes (15) | Yes |
| A3 | LWT (IF NOT EXISTS / IF conditions) | Yes | Yes | Yes |
| A3 | Batch CAS | Yes | Yes | Yes |
| A3 | DepWaitGraph + cycle detection | Yes | Yes (13) | Yes |
| A3 | DdlDrain | Yes | Yes (4) | Yes |
| A4 | MemIndex (transactional 2i) | Yes (in memtable/) | Yes (8 via read_2i) | Yes (path imprecise) |
| A4 | Leaseholder | Yes | Yes (5) | Yes |
| A4 | LinearizableRead | Yes | Yes (4) | Yes |
| A5 | Jepsen TestCluster + NemesisController | Yes | Yes (3) | Yes |
| A5 | HistoryRecorder / LinearizabilityChecker | Yes | Yes | Yes |
| A5 | Crash recovery + sidecar files | Yes | Yes (4+6) | Yes |
| A5 | DurabilityService / ExclusiveSyncPoint | Yes | Yes (7) | Yes |
| A6 | BEGIN TRANSACTION / COMMIT / ROLLBACK | Yes | Yes | Yes |
| A6 | Cross-shard execution | Yes | Yes (10) | Yes |
| A6 | Jepsen bank + write-skew | Yes | Yes (3) | Yes |
| A6 | AccordMetrics (9 metrics) | Yes | Yes (9) | Yes |
| A7 | read_2i 5-layer merge | Yes | Yes (8) | Yes |
| A7 | Electorate reconfiguration (4-gate) | Yes | Yes (6) | Yes |
| A7 | EpochDrain | Yes | Yes (3) | Yes |
| A7 | Two-phase DDL | Yes | Yes (4) | Yes |
| A7 | Full Jepsen nemesis suite | Yes | Yes (3+6) | Yes — but 6 never run in CI |
| A7 | Chaos minority kill | Yes | Yes (2) | Yes |
| A7 | Performance regression suite | Yes | Yes (7) | Yes |
| A7 | UDF/UDA integration | Yes | Yes (15) | Yes |

**All 36 A1-A7 deliverables have matching code and spec. 34 of 36 have tests that run on at least nightly CI. 2 (the full Jepsen nemesis suite) are never run automatically.**
