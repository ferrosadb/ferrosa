# Testing Infrastructure Coverage

> Last updated: 2026-04-18
> Scope: ferrosa-jepsen, ferrosa-loadgen, .github/workflows/

---

## 1. Feature Inventory

### ferrosa-jepsen

**Purpose:** Jepsen-style distributed correctness verification for Accord transactions and CQL LWT patterns on real multi-node clusters.

**Architecture:** Three-layer — Rust orchestrator binary, chaos plane (tc/netem, Firecracker, Fly.io), workload generators.

**Workloads** (18 total in phase1 registry):
- `register` — single-key read/write linearizability (Read, Write, CAS, SerialRead)
- `bank` — multi-account balance transfer; invariant: `SUM(balances) == initial_total`
- `lwt-1` through `lwt-16` — all 16 LWT patterns under concurrent contention (INSERT IF NOT EXISTS, CAS, DELETE IF EXISTS, BATCH, TTL interactions, static columns, SERIAL reads, BEGIN TRANSACTION)

**Nemeses:**

| Registry | Count | Examples |
|----------|-------|---------|
| Phase 1 | 4 | noop, partition-halves, kill-minority, clock-skew-small |
| Phase 2 | 15 | + partition-ring, partition-one, slow-network, jitter-network, packet-loss, kill-majority, pause-node, clock-skew-large, clock-strobe, disk-slow, disk-fail |
| Full | 25+ | + 5 WAN (dc-partition, dc-slow, dc-asymmetric, dc-flap, dc-lossy) + 5 composed (partition+kill, slow+clock, dc-partition+kill, dc-slow+disk, everything) |

**Topology progression:** T1 (3-node, Firecracker), T2 (5-node, Firecracker), T3 (3+3 dual-DC, Fly.io), T4 (3+3+3 tri-DC, Fly.io).

**Invariant checkers (three independent layers):**
1. Custom Rust linearizability checker — WGL backtracking, O(n log n) in practice, SEARCH_LIMIT=100,000 nodes; returns per-key `CheckResult` with minimal counterexample.
2. Knossos (Jepsen/Clojure) — NP-complete linearizability proof; output parser exists but subprocess invocation deferred.
3. Elle (Jepsen) — strict serializability anomaly detection (G0–G2); output parser exists but subprocess invocation deferred.
4. Domain invariants — bank sum, counter monotonicity, set growth, register correctness.

**Execution tiers:**

| Tier | Duration | Scope | Infrastructure |
|------|----------|-------|---------------|
| Smoke | ~5 min | T1, 3 nemeses, 16 LWT, Rust driver, low concurrency | Firecracker |
| Standard | ~45 min | T1+T2, all nemeses, all 6 drivers, low+medium | Firecracker |
| Full | ~4 hours | All 4 topologies, 21 nemeses, all drivers, 3 concurrency levels | Firecracker + Fly.io |
| Endurance | 24 hours | T4 tri-DC, continuous random nemesis | Fly.io |

**Driver matrix (per spec, not yet all implemented):** Python (cassandra-driver), Go (gocql), Node.js (datastax), Java (java-driver), C# (CassandraCSharpDriver), Rust (cdrs-tokio).

**Unit test coverage within the crate (no infrastructure required):**
- WorkloadRegistry, nemesis registry, history recorder, linearizability checker (all variants): 53+ pure unit tests.
- All phase1 workloads run against MockCqlSession.
- Checker tested with known-linearizable and known-non-linearizable histories including CAS, concurrent writes, phantom reads, backward reads, multi-key partial failure.

**Remaining infrastructure-gated tests:**
- C4: 6 live-cluster tests requiring `FERROSA_TEST_CLUSTER_NODES` or `FERROSA_TEST_FIRECRACKER`.
- C6: 4 Firecracker fault-injection tests requiring `FERROSA_TEST_FIRECRACKER=1`.
- C8: Full 6-driver compatibility suite (depends on C4 passing first).

---

### ferrosa-loadgen

**Purpose:** UCS compaction stress, integrity verification, and S3 load testing for the storage engine.

**Load profiles (5 built-in):**

| Profile | Read/Write | Duration | Focus |
|---------|-----------|---------|-------|
| `read_heavy` | 90/10 | 60s | Read-path merging across memtable + SSTables |
| `balanced` | 50/50 | 60s | Balanced stress on all paths |
| `write_heavy` | 10/90 | 120s | Compaction and S3 upload pressure |
| `delete_update_heavy` | 10/90 | 300s | Tombstone and overwrite pressure |
| `compaction_stress` | 20/80 | 600s | Maximum compaction cycle generation |

**Components:**
- `GroundTruth` — thread-safe key→value tracker for integrity verification.
- `IntegrityVerifier` — reads back all written keys after load, compares against ground truth.
- `StatsCollector` — ops/sec, p50/p99/p999 latency, bytes written/read.
- `ResourceMonitor` — memory/CPU tracking with leak detection verdict.
- `TuiDashboard` — live terminal dashboard (crossterm).
- `orchestrator::run_load_test` / `run_load_test_with_tui` — entry points for embedding in test or binary.

**Gating:** All non-unit tests require `FERROSA_TEST_CONTAINERS=1` (S3 integration via MinIO) or `FERROSA_TEST_LOADGEN=1` (binary smoke). Unit tests (profile ratios, generator correctness, stats, integrity logic) run without infrastructure.

---

### GitHub Actions Workflows

| Workflow | Trigger | What it runs |
|---------|---------|-------------|
| `ci.yml` | Every PR + push to main | fmt, clippy, `--workspace --exclude ferrosa-jepsen --exclude ferrosa-loadgen` (with additional `--skip` for 11 named infra-gated tests), musl static build, example CQL scripts against Docker single-node + RustFS, rustdoc |
| `nightly-fuzz.yml` | Daily 03:00 UTC + manual | 45-min proptest session (`--workspace --exclude ferrosa-jepsen`), `PROPTEST_CASES=50000`, Docker pair-mode smoke test; auto-PRs regression files |
| `cluster-data-loss.yml` | Push to main (storage/cluster/cql/sstable paths) + manual | 3-node Docker cluster, `tests/cluster/test_data_loss_reproduction.py` (cassandra-driver + pytest) |
| `driver-tests.yml` | Daily 03:30 UTC + manual | All 6 language driver smoke tests via `tests/drivers/run-all.sh`; `continue-on-error: true` (C8 not yet complete) |
| `release.yml` | Tag push `v*` | `cargo test --workspace` (note: no `--exclude`, runs all including jepsen/loadgen), clippy, glibc + musl + macOS ARM64 builds, Debian package, GitHub release |
| `docs-examples.yml` | Push/PR to main if `examples/**` changed | AsciiDoc → HTML generation only, no Rust tests |
| `docs.yml` | Every PR + push to main | `cargo doc --no-deps --workspace` |

**Key observations from workflow analysis:**
- `ci.yml` uses `--lib --tests` flag: only lib and integration test targets; doc tests excluded from PR CI.
- `nightly-fuzz.yml` excludes `ferrosa-jepsen` but does NOT exclude `ferrosa-loadgen`. However, loadgen's infra-gated tests panic without `FERROSA_TEST_CONTAINERS=1`, and nightly does not set that env var — so those tests panic and the suite reports failure for loadgen infra tests.
- `release.yml` runs `cargo test --workspace` with no exclusions — includes both jepsen and loadgen. Release tests are expected to fail for infra-gated cases unless infrastructure env vars are set (which they are not in the release runner).
- No sanitizer runs (ASAN/MSAN/TSAN) in any workflow.
- No LCOV coverage gate in any workflow.
- `FERROSA_TEST_CONTAINERS=1` is never set in any GitHub Actions workflow.
- `FERROSA_TEST_FIRECRACKER=1` is never set in any GitHub Actions workflow.

---

## 2. Spec Coverage Matrix

| Capability | Documented? | Source |
|-----------|-------------|--------|
| Workload registry (18 workloads) | Yes | `specs/jepsen-e2e-test-plan.md`, `specs/testing.md` §Suite 7 |
| All 16 LWT patterns + invariants | Yes | `specs/jepsen-e2e-test-plan.md` §LWT Workload Specification |
| Nemesis catalogue (25+ nemeses) | Yes | `specs/jepsen-e2e-test-plan.md` §Nemesis Matrix |
| Topology progression T1–T4 | Yes | `specs/jepsen-e2e-test-plan.md` §Topology Progression |
| Three-checker verification strategy | Yes | `specs/jepsen-e2e-test-plan.md` §Linearizability Verification |
| Execution tiers (Smoke/Standard/Full/Endurance) | Yes | `specs/jepsen-e2e-test-plan.md` §Execution Tiers |
| Driver matrix (6 languages) | Yes | `specs/jepsen-e2e-test-plan.md` §Driver Matrix |
| loadgen profiles | Yes | `specs/ucs-load-test-architecture.md` (referenced) |
| loadgen integrity verification | Partial | `specs/testing.md` §Suite 1 covers integrity concept; no dedicated loadgen spec section |
| Infrastructure env vars (FERROSA_TEST_*) | Yes | `CLAUDE.md` §Test Policy, `specs/archive/project-plans/project-plan-jepsen-next-steps.md` |
| CI workflow coverage (what runs where) | No | Not documented — this file fills that gap |
| Knossos/Elle integration status (deferred) | Partial | Code stubs exist; no spec notes the deferred status |
| Release workflow jepsen/loadgen inclusion | No | Undocumented; release.yml runs --workspace without exclusions |

---

## 3. CI Matrix

Legend: **✓** tested in this workflow / **excl** explicitly excluded / **skip** test exists but skipped via `--skip` name / **no-infra** test exists but panics without env vars / **build** build-only, no test / **—** not applicable

| Crate | ci.yml | nightly-fuzz.yml | cluster-data-loss.yml | driver-tests.yml | release.yml |
|-------|--------|------------------|-----------------------|------------------|-------------|
| ferrosa-common | ✓ | ✓ | — | — | ✓ |
| ferrosa-sstable | ✓ | ✓ | — | — | ✓ |
| ferrosa-storage | ✓ (some skip) | ✓ (some skip) | ✓ (via cluster test) | — | ✓ |
| ferrosa-schema | ✓ | ✓ | — | — | ✓ |
| ferrosa-cql | ✓ (some skip) | ✓ (some skip) | ✓ (via cluster test) | ✓ | ✓ |
| ferrosa-cluster | ✓ (some skip) | ✓ (some skip) | ✓ (via cluster test) | — | ✓ |
| ferrosa-graph | ✓ | ✓ | — | — | ✓ |
| ferrosa-sparql | ✓ | ✓ | — | — | ✓ |
| ferrosa-udf | ✓ | ✓ | — | — | ✓ |
| ferrosa-index | ✓ | ✓ | — | — | ✓ |
| ferrosa-index-builder | ✓ | ✓ | — | — | ✓ |
| ferrosa-net | ✓ | ✓ | — | — | ✓ |
| ferrosa-worker | ✓ | ✓ | — | — | ✓ |
| ferrosa (binary) | build+examples | build | build | ✓ | ✓ |
| ferrosa-ctl | build+musl | build | — | — | ✓ |
| ferrosa-jepsen | **excl** | **excl** | — | — | no-infra |
| ferrosa-loadgen | **excl** | no-infra (panic) | — | — | no-infra |

**Skipped tests in ci.yml + nightly-fuzz.yml (named via `--skip`):**
- `batch_atomicity`, `pause_resume`, `recovery_coordinator` — Accord multi-node tests
- `cassandra_reads_compacted`, `compaction_end_to_end_pipeline` — storage compaction E2E
- `dep_wait_ordering`, `disk_fail_no_phantom` — storage/cluster infra tests
- `packet_reorder_linearizability`, `lwt_batch_atomicity_all`, `clock_skew_large_preaccept` — cluster/Accord chaos
- `binary_*` — binary smoke tests (ci.yml only)
- `concurrent_write`, `many_flushes`, `flush_2000`, `single_writer`, `write_flush_compact` — storage load tests (ci.yml only)

**Matrix completeness:** 17 crates × 5 workflows = 85 cells. Filled (definitive answer): 77/85. Unknown/ambiguous: 8 (primarily release.yml no-infra behaviour for jepsen/loadgen).

---

## 4. Gaps

### P0: ferrosa-loadgen excluded from ci.yml but not from nightly-fuzz.yml

`nightly-fuzz.yml` uses `--workspace --exclude ferrosa-jepsen` but does not exclude `ferrosa-loadgen`. The loadgen integration tests gate on `FERROSA_TEST_CONTAINERS=1`, which nightly does not set. Result: nightly runs loadgen's infra-gated tests, they `panic!` with setup instructions, and the nightly suite fails on those tests (masked by `continue-on-error` not being set on the fuzz job). The nightly failure log will contain panics, and `grep 'test result: FAILED'` will fire, creating spurious regression PRs or false-alarm failures. **Fix: add `--exclude ferrosa-loadgen` to nightly-fuzz.yml, matching ci.yml.**

### P0: release.yml runs `cargo test --workspace` with no exclusions

The release workflow includes both `ferrosa-jepsen` and `ferrosa-loadgen` with no `--exclude`. These crates' infra-gated tests will `panic!` on GitHub-hosted runners since `FERROSA_TEST_FIRECRACKER`, `FERROSA_TEST_CLUSTER_NODES`, and `FERROSA_TEST_CONTAINERS` are never set. A release build will fail the test step for every tagged release. **Fix: add `--exclude ferrosa-jepsen --exclude ferrosa-loadgen` to the release test step, matching the pattern from ci.yml.**

### P1: No container integration tests run in any automated workflow

`FERROSA_TEST_CONTAINERS=1` is never set in any workflow. This means the S3-backed compaction tests (`ferrosa-loadgen/tests/ucs_load_s3_test.rs`) and any other MinIO-backed tests (C7 sprint) never run automatically. The cluster-data-loss workflow comes closest — it spins up Docker — but it only runs the Python data-loss reproduction test, not the Rust container tests. **Gap: C7 compaction S3 tests are perpetually blocked unless a developer runs them locally.**

### P1: ferrosa-jepsen excluded from every per-PR and nightly workflow

The smoke tier of `ferrosa-jepsen` (5 minutes, Firecracker, T1 only, 3 nemeses, Rust driver) requires `FERROSA_TEST_FIRECRACKER=1` and appropriate VM provisioning. No workflow provisions Firecracker VMs. The 53+ pure unit tests within `ferrosa-jepsen` (workload registry, nemesis registry, linearizability checker, history mechanics) run fine without infrastructure, but they are excluded from both ci.yml and nightly-fuzz.yml entirely. These unit tests could run on every PR with zero infrastructure cost.

### P2: No sanitizer runs (ASAN/MSAN/TSAN)

No workflow runs the workspace under address, memory, or thread sanitizers. Given that `ferrosa-storage` uses `unsafe` for NVMe pinning and `ferrosa-sstable` has a no-panic reader resilience requirement, TSAN would be particularly valuable for detecting data races in concurrent compaction. Sanitizer runs are typically 3–5× slower but can run nightly without impacting PR latency.

---

## 5. Recommendations

**R1 (P0): Fix nightly-fuzz.yml to exclude ferrosa-loadgen.**
Add `--exclude ferrosa-loadgen` to the `cargo test` invocation in `nightly-fuzz.yml`. This matches ci.yml's exclusion pattern and prevents spurious panic-based failures in the nightly fuzz session.

**R2 (P0): Fix release.yml to exclude infra-gated crates from test step.**
Add `--exclude ferrosa-jepsen --exclude ferrosa-loadgen` to `cargo test --workspace` in `release.yml` (line 30). This prevents every tagged release from failing its test step. Add a comment explaining why these crates are excluded and how to run them manually.

**R3 (P1): Run ferrosa-jepsen unit tests on every PR.**
The 53+ pure unit tests in `ferrosa-jepsen` (linearizability checker, workload registry, nemesis registry, history recorder, config) require no infrastructure. Change ci.yml to use `--exclude ferrosa-jepsen` only for the `--skip`-style exclusions, or add a separate `cargo test -p ferrosa-jepsen --lib` step that runs without infrastructure env vars. Zero cost, immediate benefit: the checker itself gets PR-level regression protection.

**R4 (P1): Add a container integration job to the nightly workflow.**
Extend `nightly-fuzz.yml` with a `container-integration` job that sets `FERROSA_TEST_CONTAINERS=1`, starts MinIO via Docker Compose, and runs `cargo test -p ferrosa-loadgen -p ferrosa-storage -- compaction_s3`. This is the only automated path to exercising S3-backed compaction, and it requires no Firecracker infrastructure.

**R5 (P2): Add a loadgen smoke profile to nightly.**
Define a `LoadProfile::smoke()` (10s duration, 100 key space, 2 writers, 2 readers, no S3) that runs against a local in-process `StorageEngine` with a temp directory — no containers, no S3. Add `cargo test -p ferrosa-loadgen smoke` to nightly. This gives the compaction and integrity-verification code continuous exercise without external dependencies, and validates the ground-truth tracker and stats collection don't regress.
