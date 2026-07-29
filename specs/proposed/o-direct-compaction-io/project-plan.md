---
title: "O_DIRECT compaction/rebuild reader — project plan"
status: proposed
component: storage-io
task: t_bed7ed0f
executive_summary: >
  Five workstreams gated on measurement. Sprint 0 (GATE, do first) establishes a
  compaction-plus-hot-read baseline on a MULTI-NODE fly cluster of 2-core
  `performance` instances (Linux + local NVMe) to prove page-cache eviction is a
  material problem before any feature code is written — if the baseline shows no
  material foreground degradation, the feature is NOT built. Sprint 1 adds the safe
  IoMode + DirectReadAt with alignment probe + buffered fallback and the correctness
  suite (byte-identical to FileReadAt). Sprint 2 adds bounded buffers + metrics and
  extends the baseline harness into a direct-vs-buffered comparison. Sprint 3 wires
  it into the compaction/rebuild pool (separate from foreground) behind a default-off
  flag. Promotion to on requires the comparison to show reduced foreground
  refaults/tail latency with no compaction-throughput regression.
last_revised: 2026-07-21
---

# `O_DIRECT` compaction/rebuild reader — project plan

Dependency: **Sprint 0 gates everything.** S1 -> S2 -> S3; prereq dispositions run
in parallel with S0/S1.

## Sprint 0 — Baseline measurement GATE (do first; may end the project)

**Outcome:** a decision-grade report answering "does compaction's page-cache
eviction materially hurt the foreground hot set on our target hardware?"

- **Environment (pinned by owner):** a multi-node fly cluster on **2-core
  `performance` instances** (Linux, local NVMe), RF=3. Reuse the
  `deploy/fly-accord-elle` harness pattern (build+push, machine run, self-teardown
  trap).
- **Workload:** write + force a compaction-heavy load (large SSTable inputs → UCS
  compaction) while a concurrent foreground **hot read set** issues random point
  reads over a working set sized to exceed... no — sized to *fit* in page cache when
  idle but be evicted by the compaction scan. Hold the foreground read set steady;
  trigger compaction; measure the delta.
- **Metrics (buffered baseline only, no feature yet):** foreground point-read p50/p99
  latency (idle vs during-compaction), page faults/refaults, memory + I/O PSI,
  device read latency, compaction throughput. Optionally constrain node memory
  (`--vm-memory`) to force the cache-pressure regime.
- **Gate / Definition of done:** report shows whether foreground p99 + refaults
  degrade materially during compaction. **PROCEED** to S1 only if yes; otherwise
  disposition `t_bed7ed0f` as "not warranted on this hardware" with the evidence and
  stop.

## Sprint 1 — Safe seam + DirectReadAt (default-off, correctness)

- **S1.1** `IoMode { Buffered, Direct }` + `DirectIoConfig` (default-off) in
  `ferrosa-sstable/src/io.rs`.
- **S1.2** `io::direct` Linux adapter (audited `unsafe`): `open(O_DIRECT|O_RDONLY)`,
  `statx(STATX_DIOALIGN)`, aligned bounce-buffer allocation, raw `pread`; non-Linux =
  always-buffered stub.
- **S1.3** `DirectReadAt: ReadAt` — aligned-superset read + head/tail slice + EOF
  clamp + checksum on reassembled data + per-file buffered fallback on unproven
  alignment / EINVAL.
- **S1.4** `open_scan_reader(path, mode, cfg) -> DirectReadAt`.
- **Gate (FMEA F1-F7):** proptest `direct_read_matches_buffered_for_arbitrary_offset_len`;
  `direct_short_read_at_eof_matches_buffered`; `unproven_alignment_falls_back_to_buffered`;
  `unsupported_fs_open_falls_back`; `non_linux_uses_buffered`; clippy `-D warnings`;
  Miri on the copy/alignment logic with a mock fd where feasible.

## Sprint 2 — Bounds + metrics + comparison harness

- **S2.1** Bounded `max_buffer_bytes` per reader + bounded compaction pool; fail-loud
  on exceed (FMEA F8, F11). Test `aligned_buffer_residency_is_bounded`.
- **S2.2** Metrics: buffered/direct bytes, fallback + error counts, tail-fallback
  count, readahead depth (plus reuse of the S0 foreground-latency/PSI/refault/device
  metrics).
- **S2.3** Extend the Sprint 0 fly harness into an A/B: same workload, `Buffered` vs
  `Direct`, same 2-core `performance` NVMe nodes; emit the comparison report.

## Sprint 3 — Wire in + promotion

- **S3.1** Compaction executor + index-rebuild reader use a **separate**
  direct-capable pool via `open_scan_reader`; foreground path + shared foreground
  pool provably untouched (FMEA F9). Test `foreground_reads_never_use_direct` +
  `direct_and_buffered_never_share_a_datadb_range`.
- **S3.2** Byte-identical output proof: a compaction with `Direct` inputs produces
  identical SSTables to `Buffered`; existing crash-recovery suite runs with direct
  inputs (FMEA F1, F13).
- **S3.3** Cancellation + RAII (FMEA F10) `cancel_mid_scan_releases_resources`.
- **Promotion gate (FMEA F12):** enable-by-default ONLY if the S2.3 comparison shows
  reduced foreground refaults/p99 with no compaction-throughput regression on the
  2-core `performance` NVMe baseline. Until then: default-off, docs describe the
  flag + when to turn it on.

## Prereq dispositions (parallel with S0/S1)

Resolve/close before S3 lands, since they share the copy/residency surface:
`t_6edd8c1c` (bounded remote index staging), `t_74c3c7a3` (direct nested CQL
encoding), `t_74cdda11` (CachedReadAt policy), `t_63c878ed` (compaction
compression-batch residency).

## Definition of done

- Sprint 0 baseline report exists and justifies (or cancels) the work.
- Feature ships **default-off**; buffered behavior byte-identical when off or on an
  unsupported platform.
- Foreground reads + shared foreground pool untouched; no mixed access to a range.
- FMEA RPN>=high items have green tests; `unsafe` confined + audited.
- Promotion is backed by the 2-core `performance` NVMe A/B comparison, not inspection.
