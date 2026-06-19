---
crate: ferrosa-loadgen
status: implemented
last_updated: 2026-06-19
executive_summary: >
  UCS-compaction load/stress generator for Ferrosa. Drives a property-based
  read/write/update/delete workload against the storage engine in-process (or a
  live cluster over CQL), mirrors every mutation into a sharded ground-truth
  oracle, and asserts zero data loss / zero corruption via a final integrity
  scan — plus OS-level resource-leak detection, HDR-histogram latency stats, a
  compaction-validator soak mode, and a live ratatui TUI. A tooling binary: no
  other crate depends on it.
---

# ferrosa-loadgen — Architecture Overview

## Purpose & boundary

`ferrosa-loadgen` is the **load engine and correctness oracle** for Ferrosa's
Unified Compaction Strategy (UCS). Its job is to generate sustained, realistic
mixed workloads that exercise the full write → flush → compact → S3 pipeline, and
to *prove* the engine loses no data and corrupts nothing under that pressure by
comparing every read against an in-memory ground truth.

Its boundary is narrow and one-directional: it *consumes* the storage engine and
CQL client; it owns no engine internals. It is a **leaf** of the dependency graph
— nothing in the workspace depends on it.

## Module map

| Module | Responsibility |
|--------|----------------|
| `main` (`src/main.rs`) | CLI (`clap`), profile selection, in-process vs cluster dispatch, compaction-soak entry, exit codes |
| `profile` (`src/profile.rs`) | `LoadProfile` — the five built-in workload mixes and their knobs |
| `generator` (`src/generator.rs`) | op selection (`choose_op`), key/value generation, RSS sampling; one `proptest!` block |
| `orchestrator` (`src/orchestrator.rs`) | in-process run loop: spawn workers, 500 ms flush/compact/stats/leak/TUI tick, final integrity scan |
| `cluster` (`src/cluster.rs`) | cluster run loop over a CQL client; server metrics polled from each node's web API |
| `ground_truth` (`src/ground_truth.rs`) | 64-shard, last-write-wins oracle; classifies reads (Match/Mismatch/Missing/NotYetWritten) |
| `integrity` (`src/integrity.rs`) | full-table verification scan → `IntegrityReport` |
| `resource_monitor` (`src/resource_monitor.rs`) | OS + engine resource sampling, monotonic-growth leak detection, hard-limit abort |
| `stats` (`src/stats.rs`) | HDR-histogram latency percentiles, atomic counters, snapshots, final `LoadStats` |
| `tui` (`src/tui.rs`) | live ratatui dashboard driven by the orchestrator's snapshot loop |

## Run modes

- **In-process** (default): `main` builds a `StorageEngineConfig` (optional
  `ObjectStoreConfig` for S3) and a `StorageEngine`, then calls
  `run_load_test[_with_tui]`. Workers call `engine.write`/`engine.read` directly.
- **Cluster** (`--node`): connects over CQL (`CqlClient`), creates and truncates
  `load_test.data`, distributes workers round-robin across nodes, and polls
  server-side storage metrics (SSTables, S3 objects, pending compactions) from
  the web API at the inferred port (CQL `9042+N` → web `9090+N`).
- **Compaction soak** (`--compaction-soak`): bypasses the load loop entirely and
  runs `ferrosa_storage::compaction::validator::soak::run` for N reproducible
  iterations from `--soak-seed`, diffing compacted corpora against the oracle.

## Data flow (in-process)

1. **Setup** — register `load_test.data` schema; create `GroundTruth`,
   `StatsCollector`, `ResourceMonitor`.
2. **Workers** — `num_writers + num_readers` scoped threads. Each loop: pick op by
   ratio (`choose_op`), random key in `0..key_space_size`, random value; route to
   `engine.write` (live row), `engine.write` (tombstone row), or `engine.read`;
   mirror the result into `GroundTruth` and record latency in `StatsCollector`.
3. **Main tick (500 ms)** — `flush`, `discard_completed_commit_log_segments`,
   `poll_compactions` (on a current-thread tokio runtime), take a stats snapshot,
   sample resources (abort on leak verdict), render the TUI frame; stop when
   `duration` elapses, target bytes reached, or the user quits.
4. **Finalize** — final flush, full `IntegrityVerifier::verify_all` scan, assemble
   `LoadStats`. The binary exits non-zero on missing keys / mismatches (1) or an
   abort reason (2).

## Key invariants

1. **Ground truth is last-write-wins by timestamp.** A write only updates the
   oracle entry when its timestamp `>=` the stored one; deletes set a tombstone
   flag. This must mirror the engine's LWW so the integrity scan is meaningful.
2. **Resource growth must terminate the run, not the machine.** The monitor aborts
   at a fraction of the FD ulimit or after a bounded absolute RSS growth, after a
   warmup window — fail loud rather than OOM the host.
3. **Compaction must be polled every tick.** Without `poll_compactions`, SSTables
   accumulate unboundedly and RSS grows until abort; the tick is load-bearing.
4. **Soak corpora are reproducible per seed.** A given `--soak-seed` /
   `--soak-iterations` pair must produce identical corpora so failures replay.

## Position in the dependency graph

Leaf binary. **Calls** `ferrosa-storage` (with `compaction-validator`),
`ferrosa-common`, `ferrosa-cql`, `ferrosa-sstable`, `ferrosa-schema`.
**Called by** nothing. See the [root crate index](../../specs/crates.md) for the
full graph.
