---
title: "ADR: Opt-in Linux O_DIRECT for compaction and rebuild I/O"
status: proposed
component: storage-io
task: t_bed7ed0f
source_pr: https://github.com/ferrosadb/ferrosa/pull/279
executive_summary: >
  Compaction and index-rebuild stream whole SSTable Data.db files through the OS
  page cache, evicting the foreground hot working set and inflating read-path tail
  latency + refaults under a bounded cgroup. Decision: add a DEFAULT-OFF, Linux-only
  O_DIRECT reader for compaction/rebuild only, exposed behind a safe internal
  `IoMode`, with all unsafe/platform-specific opening + aligned allocation isolated
  in a small audited Linux adapter. Alignment is probed via STATX_DIOALIGN; any
  unproven alignment or unsupported filesystem/platform falls back to the existing
  buffered `FileReadAt`. Direct and buffered access to an overlapping Data.db range
  are never mixed, and the foreground reader pool is never reused. The feature stays
  off until a Linux NVMe cgroup-limited comparison proves page-cache eviction is a
  material problem and the direct path wins. Rejected: global O_DIRECT, mmap, and
  posix_fadvise(DONTNEED) as the primary mechanism.
last_revised: 2026-07-21
---

# ADR: Opt-in Linux `O_DIRECT` for compaction and rebuild I/O

## Status

Proposed. Default-off, gated behind a measurement promotion criterion.

## Context

- **Issue (`t_bed7ed0f`, from the PR #279 "I/O Page-Cache and Copy Audit"):**
  compaction reads every input `Data.db` sequentially and index rebuild re-reads
  whole bodies. These are buffered reads through the OS page cache
  (`ferrosa-sstable::io::FileReadAt`, pooled as
  `compaction/executor.rs::SharedReaderPool<FileReadAt>`). A large sequential
  compaction scan pulls cold SSTable pages into the page cache and evicts the
  foreground random-read hot set, degrading serving-path tail latency and driving
  refaults — especially under the intentional 2 GiB cgroup cap the fmem-dev cluster
  runs with.
- **Current behavior:** all reads (foreground point/range reads *and* compaction/
  rebuild scans) go through the same buffered `ReadAt` implementations and share a
  reader pool abstraction. There is no `IoMode`, no direct I/O, and no
  page-cache-avoidance control anywhere in `ferrosa-storage`/`ferrosa-sstable`.
- **Stated preference (task body):** a default-off, Linux-only compaction/rebuild
  reader that uses `O_DIRECT` to bypass the page cache for the scan, isolates all
  `unsafe`/platform code in a small audited adapter, and falls back to buffered I/O
  whenever alignment cannot be proven — never changing foreground reads and never
  reusing the shared foreground reader pool.

## Decision

Add an **opt-in, default-off, Linux-only `O_DIRECT` read path for compaction and
index-rebuild scans only**, with these load-bearing controls:

1. **Safe seam:** a `ReadAt`-implementing direct reader selected by a safe
   `IoMode { Buffered, Direct }` enum. All `unsafe` (`libc::open(O_DIRECT)`,
   aligned allocation, `pread` on the raw fd) and platform `#[cfg(target_os =
   "linux")]` code lives in one small, audited adapter module; the rest of the
   codebase only sees the safe `ReadAt` trait.
2. **Prove alignment or fall back:** query `STATX_DIOALIGN` (Linux ≥ 6.1) for the
   required memory/offset alignment. Require proven address, length, **and** offset
   alignment before issuing a direct read; otherwise transparently use the existing
   buffered `FileReadAt`. Unsupported filesystems (EINVAL on `O_DIRECT` open) and
   macOS/older Linux always take the buffered fallback.
3. **Bounce-buffer the unaligned edges:** since `ReadAt::read_at(buf, offset)` is
   called with arbitrary offset/length, the direct reader reads into bounded,
   aligned internal buffers over the aligned superset range and copies the requested
   slice out, handling the unaligned head/tail explicitly. (The goal is
   page-cache-eviction avoidance, not eliminating the userspace copy.)
4. **No mixing, no foreground reuse:** direct, buffered, and mmap access to an
   overlapping `Data.db` byte range are never combined. The direct reader is used by
   a **separate** compaction/rebuild reader pool; the shared foreground reader pool
   is never reused for direct I/O and foreground reads stay buffered.
5. **Measurement-gated promotion:** the feature ships behind a config flag that
   stays off until a comparison on the **owner-pinned baseline environment — a
   multi-node fly cluster of 2-core `performance` instances (Linux + local NVMe,
   RF=3)** — demonstrates the direct path reduces foreground refaults/tail latency
   without a compaction-throughput regression. A *buffered-only* baseline on that
   same environment must first establish that page-cache eviction is a material
   problem at all (Sprint 0 gate); if it is not, the feature is not built.

## Alternatives considered and rejected

| Alternative | Why rejected |
|---|---|
| **Global `O_DIRECT`** (foreground reads too) | Foreground random reads *benefit* from the page cache; direct I/O would add alignment cost + bounce copies to the serving path. Out of scope; explicitly excluded. |
| **`posix_fadvise(POSIX_FADV_DONTNEED)` after the scan** | Cheaper and portable, but reactive — pages are still admitted (evicting the hot set) before being dropped, and `DONTNEED` is advisory/racy under concurrent mmap. Keep as a possible complementary follow-up, not the primary mechanism. |
| **`mmap` + `MADV_DONTNEED`/`MADV_SEQUENTIAL`** | Still populates the page cache; mmap read faults are hard to bound and cancel; conflicts with the "never mmap an overlapping range" constraint. |
| **A brand-new async io_uring reader** | Larger surface, new failure modes, and unnecessary for a sequential scan; `O_DIRECT` + `pread` on a blocking pool is sufficient and auditable. |

## Consequences

- **Positive:** compaction/rebuild stop evicting the foreground hot set on Linux/NVMe;
  bounded-memory, cancellable scan reads; a reusable `IoMode` seam for future
  page-cache-sensitive batch paths.
- **Cost/risk:** one small `unsafe` adapter (audited, Miri where feasible);
  correctness surface around alignment, short reads, unaligned tails, and checksums;
  a second reader pool to bound. All gated behind default-off + the measurement.
- **Required controls (carried into the FMEA):** never mix access modes on a range;
  bound aligned-buffer residency; verify checksums on the reassembled (not partial)
  data; survive crash/restart mid-compaction (direct reads are read-only, so no new
  durability surface); fail loud + fall back on any alignment/filesystem rejection.

## Prerequisites (disposition before implementing)

Resolve or explicitly disposition the sibling PR #279 audit tasks first, since they
share the copy/residency surface: bounded remote index staging (`t_6edd8c1c`),
direct nested CQL encoding (`t_74c3c7a3`), `CachedReadAt` policy (`t_74cdda11`),
compaction compression-batch residency (`t_63c878ed`).
