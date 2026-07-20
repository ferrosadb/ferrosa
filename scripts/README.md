# Test Cluster Scripts

Scripts for bringing up a 3-node Ferrosa test cluster and running cluster-dependent tests.

## Quick Start

```bash
# Run all cluster tests (local Podman, ports 30042-30044)
scripts/test-with-cluster.sh

# Run all cluster tests in CI (Docker, default ports 9042-9044)
scripts/test-with-cluster.sh --ci
```

## Scripts

### Static I/O Audit

These read-only scanners create a review queue for redundant byte copies,
materialized streams, and filesystem/page-cache boundaries. They do not make a
performance claim; classify each result by its full production call path before
changing it.

```bash
scripts/audit-io-copy-candidates.sh
scripts/audit-page-cache-boundaries.sh
```

Add `--include-tests` when test and benchmark code is relevant, or pass a
different Ferrosa checkout path as the final argument.

### `test-cluster-up.sh` — Local (Podman)

Brings up a 3-node cluster via `podman compose` on ports **30042–30044** (CQL) and
**30000/30001** (RustFS S3). Uses project name `ferrosa-test-w1` so containers are
isolated from the live ferrosa-memory cluster.

```bash
# Bring up, wait for healthy, print env vars, tear down on Ctrl-C
scripts/test-cluster-up.sh

# Bring up and keep running (no auto-teardown)
scripts/test-cluster-up.sh --keep

# Source the env vars into your shell
source <(scripts/test-cluster-up.sh --keep)
```

### `test-cluster-up-ci.sh` — CI (Docker)

Brings up a 3-node cluster via `docker compose` on the default ports (**9042–9044**).
CI runners are ephemeral and isolated, so no port offset is needed.

```bash
# Bring up and capture the env var
export FERROSA_TEST_CLUSTER_NODES=$(scripts/test-cluster-up-ci.sh | grep FERROSA | cut -d= -f2-)
```

### `test-cluster-down.sh` — Tear Down

Tears down the test cluster. Detects Podman or Docker automatically.

```bash
scripts/test-cluster-down.sh        # local Podman
scripts/test-cluster-down.sh --ci   # CI Docker
```

### `test-with-cluster.sh` — End-to-End Wrapper

Brings up the cluster, runs `cargo test -- --ignored`, tears down.

```bash
scripts/test-with-cluster.sh              # Podman
scripts/test-with-cluster.sh --ci         # Docker
scripts/test-with-cluster.sh -- --test-threads=1  # extra cargo flags
```

## Port Ranges

| Cluster | Purpose | Port Range |
|---------|---------|------------|
| Local Podman test (ferrosa-test-w1) | Test cluster | 30000–30099 |
| CI Docker (ferrosa-test-ci) | Test cluster | 9000–9044 |
| Live fmem cluster | DO NOT TOUCH | 19000–19092, 17000–18200 |

The Podman override file `tests/docker-compose.cluster.podman.yml` remaps ports to
the 30000–30099 range when used alongside `tests/docker-compose.cluster.yml`.

## Running Cluster Tests Manually

```bash
# 1. Start the cluster
scripts/test-cluster-up.sh --keep &
export FERROSA_TEST_CLUSTER_NODES=127.0.0.1:30042,127.0.0.1:30043,127.0.0.1:30044

# 2. Run the ignored tests
cargo test -p ferrosa-cluster -- --ignored

# 3. Tear down
scripts/test-cluster-down.sh
```

## Compose Files

| File | Purpose |
|------|---------|
| `tests/docker-compose.cluster.yml` | Base cluster definition (trio/quint profiles) |
| `tests/docker-compose.cluster.podman.yml` | Port overrides for local Podman (30000–30099) |
| `tests/cluster/docker-compose.cql-integration.yml` | CQL integration tests |
