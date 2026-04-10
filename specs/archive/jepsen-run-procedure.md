# Jepsen Test Run Procedure

> Step-by-step guide for running the full Jepsen correctness test suite.

## Prerequisites

- Docker or Podman installed (`podman` preferred on macOS via Podman Desktop)
- ferrosa built: `cargo build --release`
- MinIO container for S3 (included in docker-compose)
- ~30 minutes for full run, ~5 minutes for smoke

## Quick Smoke Test

```bash
./scripts/jepsen-smoke.sh
```

Starts a 3-node cluster, runs unit tests, runs smoke tier, cleans up.

## Full Correctness Run

### Step 1: Start Infrastructure

```bash
cd tests/
docker compose -f docker-compose.cluster.yml up -d

# Verify all nodes are up
docker compose -f docker-compose.cluster.yml ps
```

Wait for all 3 nodes to show "healthy" or check CQL port:
```bash
docker exec ferrosa-node1 sh -c "echo > /dev/tcp/localhost/9042"
```

### Step 2: Set Environment

```bash
export FERROSA_TEST_CONTAINERS=1
export FERROSA_TEST_CLUSTER_NODES="172.28.0.2:9042,172.28.0.3:9042,172.28.0.4:9042"
```

### Step 3: Run Tests by Phase

**Phase 1 — No fault injection (baseline):**
```bash
cargo test -p ferrosa-jepsen -- --nocapture 2>&1 | tee jepsen-phase1.log
```

**Phase 2 — With nemeses (clock skew, partitions, kills):**
```bash
cargo test -p ferrosa-jepsen --test smoke_tier -- --nocapture 2>&1 | tee jepsen-phase2.log
```

**C4 — Accord transaction verification:**
```bash
cargo test -p ferrosa-cluster --test correctness -- --nocapture 2>&1 | tee jepsen-c4.log
```

**C8 — Driver compatibility:**
```bash
cargo test -p ferrosa --test smoke -- --nocapture 2>&1 | tee jepsen-c8.log
```

### Step 4: Verify Results

```bash
grep "test result" jepsen-*.log
```

Expected: all `ok`, zero failures.

### Step 5: Cleanup

```bash
cd tests/
docker compose -f docker-compose.cluster.yml down -v
```

## Firecracker VMs (Alternative)

For production-grade isolation with real network partitions:

```bash
# Setup Firecracker VMs
./scripts/lima-fc-setup.sh

# Start 3-node cluster
export FERROSA_TEST_FIRECRACKER=1
./scripts/lima-fc-cluster-up.sh

# Run tests
cargo test -p ferrosa-jepsen -- --nocapture
cargo test -p ferrosa-cluster --test correctness -- --nocapture
```

## Interpreting Results

| Result | Meaning |
|--------|---------|
| All pass | Linearizability holds under failure injection |
| Checker failure | Stale read or lost write detected — investigate history |
| Timeout | Cluster didn't recover in time — check nemesis heal |
| Panic (infra) | Test infrastructure issue, not a ferrosa bug |

## Test Matrix

| Tier | Nemeses | Workloads | Topologies |
|------|---------|-----------|------------|
| Smoke | noop | register, bank | T1 (3-node) |
| Phase 1 | noop, partition-halves, kill-minority, clock-skew-small | register, bank, set | T1 |
| Phase 2 | All 15 | register, bank, set, CAS | T1, T2 (5-node) |
| Full | All 25+ (incl WAN, composed) | All | T1, T2, T3 (multi-DC) |
