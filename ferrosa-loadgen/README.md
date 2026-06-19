# ferrosa-loadgen

> UCS-compaction load/stress generator for Ferrosa: drives a property-based
> read/write/update/delete workload against the storage engine (in-process) or a
> live cluster (over CQL), tracks a ground-truth oracle, and asserts zero data
> loss / zero corruption — with OS-level resource-leak detection and a live TUI.

## What this crate is

A **testing/tooling crate** — both a library and the `ferrosa-loadgen` binary. It
exists to produce sustained, realistic load that exercises the full
write → flush → compact → S3 pipeline (the Unified Compaction Strategy, UCS) and
to *prove* correctness under that load by comparing every read against an
in-memory ground-truth tracker. It is the load engine behind the UCS
end-to-end and burn-in tests.

It is a leaf of the dependency graph: **nothing depends on it**.

## What's implemented

- **Two run modes** (`src/main.rs`):
  - **In-process** — constructs a `StorageEngine` directly (optionally S3-backed)
    and drives it with native worker threads, no network.
  - **Cluster** — `--node host:port[,…]` connects over CQL (`ferrosa-cql`
    client), creates `load_test.data`, distributes workers round-robin across
    nodes, and polls server-side metrics from each node's web API
    (`/api/storage`).
- **Five load profiles** (`src/profile.rs`): `read_heavy`, `balanced`,
  `write_heavy`, `delete_update_heavy`, `compaction_stress` — each a fixed set of
  read/write/update/delete ratios, key-space size, worker counts, flush/cache
  thresholds, and fan factor. `--duration` / `--cache-max-bytes` override.
- **Compaction soak** (`--compaction-soak`): instead of a load test, runs
  `ferrosa_storage::compaction::validator::soak` for N reproducible iterations
  (per `--soak-seed`), compacting deterministic corpora and diffing each against
  the oracle. Exits non-zero on mismatch.
- **Ground-truth oracle** (`src/ground_truth.rs`): 64-shard,
  last-write-wins, timestamp-ordered map of expected key → value/tombstone.
  Records writes/deletes and classifies each read as Match / Mismatch / Missing /
  NotYetWritten.
- **Integrity verification** (`src/integrity.rs`): full-table scan at end of run;
  every ground-truth key must read back its latest value (or None if deleted).
- **Resource-leak detection** (`src/resource_monitor.rs`): samples fds, RSS/VSZ,
  TCP/unix sockets, threads, commit-log segments, SSTable count, tmp files;
  flags monotonic growth as a probable leak and **aborts** before hard limits
  (FD ulimit fraction, absolute RSS growth) are hit.
- **Stats** (`src/stats.rs`): HDR-histogram latency percentiles (p50/95/99/100),
  atomic throughput counters, periodic snapshots, final `LoadStats` report.
- **Live TUI** (`src/tui.rs`, `--tui`): ratatui dashboard — throughput sparkline,
  latency, storage metrics, leak warnings; `q`/`Ctrl-C` to stop, `p` to pause.

## How it works

The orchestrator (`src/orchestrator.rs`) spawns `num_writers + num_readers`
scoped threads. Each picks an op by profile ratio (`choose_op`), a random key in
the key space, and a random value; writes/deletes/reads go through the
`StorageEngine` and are mirrored into the `GroundTruth` oracle. The main thread
runs a 500 ms loop: periodic `flush`, `poll_compactions`, stats snapshot,
resource sampling (abort on leak), and TUI render. At the end it does a final
flush and a full integrity scan. The cluster path (`src/cluster.rs`) mirrors this
shape over a CQL client with server-side metrics polled from the web API.

## CLI (key entry points)

| Flag | Effect |
|------|--------|
| `--profile <name>` | one of the five profiles (default `balanced`) |
| `--duration <secs>` | override profile duration |
| `--node <addr[,…]>` | cluster mode over CQL (else in-process) |
| `--tui` | live dashboard |
| `--list-profiles` | print profiles and exit |
| `--compaction-soak` / `--soak-seed` / `--soak-iterations` | run the compaction validator soak instead of a load test |
| `--data-dir`, `--s3-endpoint`/`--s3-bucket`/`--s3-access-key`/`--s3-secret-key`, `--cache-max-bytes`, `--cleanup` | in-process engine config |

**Library exports** (`src/lib.rs`): `LoadProfile`, `GroundTruth`,
`IntegrityVerifier`/`IntegrityReport`, `ResourceMonitor`/`ResourceSummary`,
`LoadStats`/`StatsCollector`/`StatsSnapshot`, `run_load_test`,
`run_load_test_with_tui`.

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-storage`** (with `compaction-validator` feature) — the
  `StorageEngine` it drives in-process, compaction polling/metrics, and the
  compaction soak validator.
- **`ferrosa-common`** — `CellValue`, `DecoratedKey`, `PartitionKey`,
  `TableSchema`/`ColumnDefinition` for the load-test row shape.
- **`ferrosa-cql`** — the `CqlClient` used in cluster mode.
- **`ferrosa-sstable`** — `Row`, `LivenessInfo`, `DeletionTime` for building the
  in-process write rows.
- **`ferrosa-schema`** — schema types for the test table.

External: `clap`, `tokio`, `ratatui`/`crossterm`, `hdrhistogram`, `parking_lot`,
`rand`, `reqwest`, `serde_json`, `uuid`, `hex`, `libc`. Dev: `proptest`,
`tempfile`.

**Called by**: **NONE** — this is a tooling binary; no crate depends on it.

## Tests

- **Unit tests** in-crate (`#[test]` across `src/`, incl. one `proptest!` block in
  `generator.rs`): profile invariants, ground-truth LWW/concurrency correctness,
  stats, resource-monitor leak logic, op-distribution.
- **Integration tests** (`tests/`): `ucs_load_test.rs` (in-process E2E across
  profiles), `ucs_load_s3_test.rs` (live cluster + RustFS S3, gated on
  `FERROSA_TEST_CONTAINERS=1`), `binary_smoke.rs` (spawns the binary, gated on
  `FERROSA_TEST_LOADGEN=1`).

## Specs

- [Architecture overview](specs/overview.md) — module map, run modes, data flow
- [Roadmap](specs/roadmap.md) — Now / Next / Later
