# Jepsen Prep — Project Plan

> Test coverage gaps, FP-patterns lock review, and Jepsen readiness
> Last updated: 2026-03-30

## Summary

Three workstreams to prepare for the Jepsen correctness test run:
1. **ctl/binary test gaps** — bring test coverage up for CLI and binary startup paths
2. **FP-patterns lock review** — evaluate all locks/mutexes for functional alternatives
3. **Jepsen prep** — add unit tests that don't need infrastructure, ready the harness

---

## Workstream 1: Lock/Mutex FP Review

### Inventory (43 total synchronization primitives)

| Category | Count | Status |
|----------|-------|--------|
| ArcSwap (lock-free) | 9 | Already functional — no changes needed |
| Atomics (AtomicU64/Bool/Usize) | 12+ | Already lock-free — no changes needed |
| parking_lot::Mutex (brief) | 12 | All brief critical sections — acceptable |
| std::sync::Mutex (brief) | 8 | All brief — acceptable |
| tokio::sync::RwLock (async) | 6 | Appropriate for async contexts |
| std::sync::RwLock | 6 | Appropriate where used |

### Verdict: No critical changes needed

The codebase already follows FP best practices for a database:
- **Read path is lock-free**: TableStore, Schema, CommitLog active segment, ModeController all use ArcSwap
- **Write path uses brief locks**: Flush guard, sharded memtable (64 independent RwLocks), schema write_lock
- **No locks held across .await**: Verified across all crates
- **No deadlock risk**: Single-lock acquisition pattern throughout
- **No static mut**: Zero instances found

### Potential improvements (backlog, not blocking)

| ID | Location | Current | Proposed | Priority |
|----|----------|---------|----------|----------|
| FP-001 | `ShardedBTreeMemtable` (sharded.rs) | 64 `RwLock<BTreeMap>` | Lock-free skiplist (already feature-gated) | P3 |
| FP-002 | `LocalCache` (cache.rs) | `Mutex<HashMap>` | `ArcSwap<im::HashMap>` for lock-free reads | P4 |
| FP-003 | `ModeController` peers/nodes (controller.rs) | `Mutex<Vec/BTreeSet>` | `ArcSwap` clone-on-write | P4 |
| FP-004 | `IpConnectionTracker` (server.rs) | `RwLock<HashMap>` | `dashmap::DashMap` sharded | P4 |

---

## Workstream 2: ctl/binary Test Gaps

### ferrosa-ctl: 15 commands parsed, 0 executed in tests

| Command | Parsing | Execution | Gap |
|---------|---------|-----------|-----|
| status | Tested | **Untested** | Need CQL integration test |
| connections | Tested | **Untested** | Need CQL integration test |
| queries | Tested | **Untested** | Need CQL integration test |
| storage | Tested | **Untested** | Need CQL integration test |
| topology | Tested | **Untested** | Need HTTP API test |
| peers | Tested | **Untested** | Need HTTP API test |
| monitor | Tested | **Untested** | TUI — manual test only |
| add-node | Tested | **Untested** | Need HTTP API test |
| decommission | Tested | **Untested** | Need HTTP API test |
| ring | Tested | **Untested** | Need HTTP API test |
| rebalance | Tested | **Untested** | Need HTTP API test |
| snapshot create | Tested | **Stub** | Not implemented |
| snapshot list | Tested | **Stub** | Not implemented |
| snapshot delete | Tested | **Stub** | Not implemented |
| restore | Tested | **Stub** | Not implemented |

### ferrosa binary: 2 of 14 startup steps tested

| Step | Component | Tested? |
|------|-----------|---------|
| Config loading | `load_config()`, `config_val()` | Yes (10 tests) |
| CQL server boot | smoke.rs | Yes (basic DML) |
| Schema persistence | `persist_schema_locally()` | Yes (2 tests) |
| Host ID load/gen | — | **No** |
| StorageEngine init | — | **No** |
| S3 bootstrap | `bootstrap_from_s3()` | **No** |
| Mode controller (standalone/pair/cluster) | — | **No** |
| Web console (24 endpoints) | — | **No** (routing only) |
| Graph engine + Bolt | — | **No** |
| Seed connections | — | **No** |
| Maintenance loop | — | **No** |
| Graceful shutdown | — | **No** |

### ferrosa-jepsen: 120 tests, but only 1 unit test without infra

Untested logic that can be unit-tested:
- Workload operation generation (`src/workload/`)
- History/operation serialization (`src/history.rs`)
- Checker correctness logic (`src/checker/`)
- Nemesis scheduling and sequencing (`src/chaos/`)
- Config validation and test plan expansion (`src/config.rs`)

---

## Sprint Plan

### Sprint 1: Binary & Web API Tests (Priority 1)

| ID | Task | Size | Success Criteria | Tests |
|----|------|------|-----------------|-------|
| BT-001 | Web API endpoint smoke tests | M | All 12 GET endpoints return 200 with valid JSON | `web_api_endpoints_return_json` |
| BT-002 | Web cluster API tests | M | promote/switchover/add-node/decommission return correct status | `web_cluster_api_operations` |
| BT-003 | Prometheus /metrics endpoint test | S | Returns text/plain with ferrosa_ prefix metrics | `prometheus_metrics_endpoint` |
| BT-004 | Standalone mode startup/shutdown test | M | Boot, serve CQL, shutdown cleanly with schema persisted | `standalone_startup_shutdown` |
| BT-005 | Config loading edge cases | S | Missing env vars, invalid TOML, conflicting options | `config_edge_cases` |

### Sprint 2: ctl Integration Tests (Priority 1)

| ID | Task | Size | Success Criteria | Tests |
|----|------|------|-----------------|-------|
| CT-001 | ctl status/connections/queries against live CQL | M | Boot server, run ctl commands, verify output | `ctl_cql_commands_integration` |
| CT-002 | ctl add-node/decommission against web API | M | Boot server, call ctl cluster commands, verify HTTP | `ctl_cluster_commands_integration` |
| CT-003 | ctl error handling (connection refused, timeout) | S | Verify graceful error messages | `ctl_error_handling` |
| CT-004 | ctl storage/topology against web API | S | Verify JSON response parsing | `ctl_storage_topology` |

### Sprint 3: Jepsen Unit Tests (Priority 2)

| ID | Task | Size | Success Criteria | Tests |
|----|------|------|-----------------|-------|
| JP-001 | Workload operation generation unit tests | M | All workload types generate valid operations | `workload_generation_*` |
| JP-002 | Checker correctness logic unit tests | M | Linearizability checker passes/fails on known histories | `checker_linearizability_*` |
| JP-003 | Nemesis scheduling unit tests | S | Clock skew, partition, kill nemeses produce valid schedules | `nemesis_scheduling_*` |
| JP-004 | Config expansion unit tests | S | Topology × concurrency × workload matrix resolves correctly | `config_expansion_*` |
| JP-005 | Jepsen smoke script | S | `scripts/jepsen-smoke.sh` documented and executable | Manual |
| JP-006 | Jepsen run procedure doc | S | `specs/jepsen-run-procedure.md` with step-by-step | Doc |

### Sprint 4: FP Improvements (Priority 3 — Backlog)

| ID | Task | Size | Success Criteria | Tests |
|----|------|------|-----------------|-------|
| FP-001 | Lock-free skiplist memtable | L | Feature-gated skiplist passes all memtable tests | Existing memtable tests |
| FP-002 | ArcSwap LocalCache | S | Cache reads lock-free, writes clone-on-swap | `cache_arcswap_reads` |
| FP-003 | ArcSwap ModeController peers | S | Peer list reads lock-free | Existing controller tests |
| FP-004 | DashMap IpConnectionTracker | S | Concurrent connection tracking without RwLock | `ip_tracker_concurrent` |

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Web API tests need running server | High | Medium | Use same pattern as smoke.rs (boot in test) |
| ctl tests need CQL + HTTP | High | Medium | Start full binary in test, connect from ctl |
| Jepsen unit tests miss real failure modes | Medium | Low | These supplement, not replace, the full Jepsen run |
| FP changes introduce regressions | Low | High | Run full test suite after each change |

## Dependencies

```
Sprint 1 (BT-*) ─── no dependencies, start immediately
Sprint 2 (CT-*) ─── depends on BT-001 (web API must work)
Sprint 3 (JP-*) ─── no dependencies, can parallel with Sprint 1-2
Sprint 4 (FP-*) ─── backlog, after Jepsen run
```
