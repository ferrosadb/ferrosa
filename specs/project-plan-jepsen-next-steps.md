# Jepsen Testing — Next Steps

> Remaining work after completing all non-infrastructure test sprints.
> Last updated: 2026-04-01

## Completed Work

| Sprint | Description | Status | Tests Added |
|--------|-------------|--------|-------------|
| JP-001–004 | Jepsen unit tests (config, history, workloads, checkers, chaos) | **Complete** | 53 tests |
| BT-001–005 | Binary & Web API tests | **Complete** | 4 smoke tests + existing coverage |
| CT-001–004 | ctl integration tests (live CQL commands, error handling) | **Complete** | 8 integration tests |
| Checker parsing | Knossos + Elle output parsing edge cases | **Complete** | 18 tests |
| UCS Compaction | All 13 work packets (WP-001 through WP-013) | **Complete** | Full coverage |
| FP Review | Lock/mutex functional patterns review | **Complete** | No critical changes needed |

## Remaining Work (All Infrastructure-Dependent)

### C4: Jepsen Correctness Runs
- **Blocker:** `FERROSA_TEST_CLUSTER_NODES` — needs a live 3-node cluster
- **Tests:** 6 infra-dependent tests that panic with setup instructions
- **How to run:**
  ```bash
  cd tests/
  docker compose -f docker-compose.cluster.yml up -d
  export FERROSA_TEST_CONTAINERS=1
  export FERROSA_TEST_CLUSTER_NODES="172.28.0.2:9042,172.28.0.3:9042,172.28.0.4:9042"
  cargo test -p ferrosa-jepsen -- --nocapture
  ```

### C6: Accord Failure Injection
- **Blocker:** `FERROSA_TEST_FIRECRACKER` — needs Firecracker VMs with SSH access
- **Tests:** 4 live-cluster correctness tests under fault injection
- **How to run:**
  ```bash
  ./scripts/lima-fc-setup.sh
  export FERROSA_TEST_FIRECRACKER=1
  ./scripts/lima-fc-cluster-up.sh
  cargo test -p ferrosa-cluster --test correctness -- --nocapture
  ```

### C7: Compaction S3
- **Blocker:** `FERROSA_TEST_CONTAINERS` — needs Docker/Podman + MinIO
- **Tests:** 2 container tests for S3-backed compaction
- **How to run:**
  ```bash
  export FERROSA_TEST_CONTAINERS=1
  cargo test -p ferrosa-storage -- compaction_s3 --nocapture
  ```

### C8: CQL Driver Compatibility
- **Blocker:** T-032 (all-drivers Jepsen standard tier) not yet started
- **Tests:** Full driver compatibility across Rust, Python, Java, Go, Node.js drivers
- **Depends on:** C4 passing (Jepsen infrastructure working)

## Quick Start: Jepsen Smoke Test

Easiest path to validate infrastructure and run a minimal correctness check:

```bash
./scripts/jepsen-smoke.sh
```

This starts a 3-node Docker cluster, runs unit tests, runs smoke tier, and cleans up. See `specs/jepsen-run-procedure.md` for the full step-by-step guide.

## Backlog (Post-Jepsen)

| ID | Task | Size | Description |
|----|------|------|-------------|
| FP-001 | Lock-free skiplist memtable | L | Replace 64 `RwLock<BTreeMap>` in ShardedBTreeMemtable |
| FP-002 | ArcSwap LocalCache | S | Lock-free reads for cache via `ArcSwap<im::HashMap>` |
| FP-003 | ArcSwap ModeController peers | S | Clone-on-write for peer/node lists |
| FP-004 | DashMap IpConnectionTracker | S | Sharded concurrent map for connection tracking |

## Phase Gates (Not Yet Passing)

- `ferrosa-jepsen run --tier standard` — needs C4/C6/C8 complete
- Cassandra 5.1 reader CI gate — tests pass locally, not gating CI yet
- All six driver smoke tests — C8 partially complete
