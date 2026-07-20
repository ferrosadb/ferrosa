# I/O Page-Cache and Copy Audit

> Date: 2026-07-18
> Status: Source audit; no I/O behavior has changed.
> Scope: SSTable reads and compaction, object-store staging, local page caches,
> and CQL value/result encoding.

## Decision Pending

The first direct-I/O target should be an opt-in Linux compaction/rebuild mode,
not all `Data.db` reads. This needs an owner decision before implementation.

The current reader pool deliberately shares readers between foreground queries
and compaction. Linux warns against mixing direct and buffered I/O to the same
file or overlapping range, so a direct compaction reader cannot simply be
added to the existing shared reader path. Random point reads should remain
buffered until Ferrosa has an explicit bounded user-space data-page cache.

## Method

Run the checked-in static inventories first:

```bash
scripts/audit-io-copy-candidates.sh
scripts/audit-page-cache-boundaries.sh
```

The scripts produce a review queue, not a verdict. Each finding below was
traced through production source and classified by workload. The audit uses
the following external background:

- [Direct I/O for Cassandra compaction](https://lightfoot.dev/direct-i-o-for-cassandra-compaction-cutting-p99-read-latency-by-5x/)
- [Zero-Copy Pages in Rust](https://redixhumayun.github.io/databases/2026/04/14/zero-copy-pages-in-rust.html)
- [Linux `open(2)` O_DIRECT documentation](https://man7.org/linux/man-pages/man2/open.2.html)

## Verified Findings

### A1: Compaction data I/O is fully buffered

**Priority: High.** `FileReadAt` maps `Partitions.db` and `Rows.db`, and uses
`pread` for `Data.db` ([`io.rs`](../../ferrosa-sstable/src/io.rs)). Compaction
opens every input through this type
([`executor.rs`](../../ferrosa-storage/src/compaction/executor.rs)); it writes
`Data.raw`, rereads it to produce compressed `Data.db`, and writes the output
through normal file APIs ([`writer.rs`](../../ferrosa-sstable/src/writer.rs)).
There is no `O_DIRECT`, cache-advice, or explicit application-owned data buffer
policy today.

The k-way merge retains a reader for every input until the task completes. The
engine-wide reader pool is intentionally a soft cap when all pooled readers are
in use, so high-fan-in work can retain many mapped-index working sets at once.
That amplifies page-cache pressure during background work.

This is the closest Ferrosa analogue to the Cassandra compaction case: input
and output are sequential background work that can displace foreground pages.
The merge itself is already streaming by partition, so this is a page-cache and
per-chunk-copy concern, not a whole-SSTable materialization regression.

**Do next:** establish a constrained-load baseline before changing behavior;
then prototype a Linux-only, feature-gated compaction/rebuild reader with
aligned bounded buffers, explicit readahead, tail handling, checksums, and a
buffered fallback. Do not reuse the foreground reader pool for direct-mode
files or mix modes on a single `Data.db` range.

### A2: `CachedReadAt` is a passthrough, not a block cache

**Priority: High.** Its documentation promises a bounded block cache for hot
index components, but it stores only the inner reader and length, discards its
configured capacity, and delegates every read
([`io.rs`](../../ferrosa-sstable/src/io.rs)). This means Ferrosa has no
application-owned block cache beneath that abstraction. The mapped index files
still use the kernel page cache; `mmap` is not a page-cache bypass.

**Do next:** either implement a strictly byte-bounded, immutable block cache
with a safe owned-buffer API, or rename/remove the abstraction so callers do
not assume a cache exists. This must be measured against the existing
decompressed-chunk cache before adding another cache layer.

### A3: Remote SSTable range reads copy the payload twice

**Priority: Medium.** The range hook is typed as `Result<Option<Vec<u8>>>` and
copies the returned vector into the caller buffer
([`io.rs`](../../ferrosa-sstable/src/io.rs)). Its object-store implementation
turns the response `Bytes` into `Vec<u8>` first
([`engine.rs`](../../ferrosa-storage/src/engine.rs)). An evicted remote range
therefore pays `Bytes -> Vec -> caller buffer`.

**Do next:** change the hook to fill the caller's mutable slice and return the
byte count, or return `Bytes` and copy directly once. Preserve the existing
short-read, corruption, rehydration, and error semantics. This is a narrow,
safe-Rust copy-elision candidate independent of direct I/O.

### A4: Remote index building materializes before enforcing its budget

**Priority: High.** The index builder calls `GetResult::bytes()` for every
SSTable component, only then increments and checks the temporary-disk budget,
and writes the fully materialized body to disk
([`worker.rs`](../../ferrosa-index-builder/src/worker.rs)). A single component
can exceed the intended budget in memory before the job is rejected. Generated
sidecars are likewise written to disk and then fully reread for upload.

**Do next:** reserve from object metadata before transfer and stream download
to the staging file in bounded chunks. Upload sidecars through the existing
multipart-style bounded file path. This background work is a strong candidate
for page-cache reduction after the streaming conversion, but not for a global
direct-I/O default.

### A5: Compaction's compressed-output path has temporary duplicate residency

**Priority: Medium, measure first.** File-backed compaction writes `Data.raw`,
then rereads bounded batches, holds raw chunks while parallel compression
collects compressed chunks, and writes `Data.db`
([`writer.rs`](../../ferrosa-sstable/src/writer.rs)). That creates raw-file
page-cache pressure plus bounded raw/compressed heap copies. Output
verification intentionally performs one more streaming read after promotion
([`executor.rs`](../../ferrosa-storage/src/compaction/executor.rs)).

**Do next:** keep the correctness verification. Measure batch residency and
writeback first; if it is material, make compression batches byte-bounded and
release consumed raw buffers promptly. Evaluate cache-release advice only as a
control experiment; it cannot prevent the initial cache pollution.

### A6: CQL nested values are encoded into intermediate vectors

**Priority: Medium.** The CQL result encoder special-cases primitive values,
but falls back to `encode_value() -> Vec<u8>` and copies that vector into the
response `BytesMut` for collections, tuples, maps, and UDTs
([`result.rs`](../../ferrosa-cql/src/result.rs)). The shared row codec repeats
that recursive allocate-and-copy pattern for nested values
([`codec.rs`](../../ferrosa-row-bridge/src/codec.rs)).

**Do next:** add a recursive `encode_value_into(&mut BytesMut)` and exact
length helper, with `encode_value()` retained as a compatibility wrapper for
callers that require ownership. This removes intermediate payload copies while
staying entirely safe Rust.

## Intentional or Deferred Copies

- The raw CQL result fast path copies storage cell bytes once into the owned
  response body. That avoids decode/re-encode work and is appropriate until
  the protocol supports safe vectored `Bytes` segments.
- Multipart SSTable upload copies each bounded reusable part into an owned
  payload. That is a deliberate lifetime trade-off, not a whole-file
  materialization. Replacing it with fresh owned buffers may trade copying for
  allocation; benchmark it after higher-value work.
- `ObjectRangePageStore` uses files as a bounded local cache. Cache hits use
  the page cache by design. It also lacks per-key single-flight, so concurrent
  misses may duplicate range GETs. This is a cache-policy design decision, not
  evidence that every page-cache hit is harmful.
- `mmap` is currently limited to immutable index components. It avoids a
  syscall path but still faults through the kernel page cache; do not label it
  direct I/O.

## Measurement Gates

Ferrosa already exports compaction byte totals and phase durations, plus
process I/O and memory data. Before a direct-I/O change, add or collect:

1. Foreground read p50, p95, p99, and p99.9 while compaction runs at a fixed
   input/output rate.
2. Per-process minor/major faults; cgroup `memory.stat` refault and reclaim
   counters; memory and I/O PSI; writeback/reclaim activity.
3. Device queue depth, read/write bandwidth, I/O latency, CPU cycles, and
   memcpy/cache-miss profiles.
4. Compaction mode, buffered/direct bytes, aligned-tail fallback count,
   readahead depth, and direct-I/O fallback/error counters.
5. Correctness tests for short reads, unaligned tails, checksums, cancellation,
   retry, crash/restart, filesystem rejection, and macOS/unsupported-Linux
   fallback.

Use a Linux NVMe test node with a cgroup memory limit and a foreground hot set
that fits in available cache. Compare buffered and direct modes with identical
compaction inputs and concurrency. A workload whose hot set never fits in
memory cannot validate the eviction-protection hypothesis.

## Safe-Rust Direct-I/O Boundary

If the owner approves the compaction-only experiment, expose a safe internal
`IoMode` and a reader that accepts only invariant-preserving aligned buffers.
The public storage API must not expose raw file descriptors, alignment
obligations, or unsafe pointers. Any unavoidable platform-specific opening or
allocation belongs in a small audited Linux adapter with a buffered fallback.

`O_DIRECT` is not a portable semantic guarantee: alignment and support vary by
filesystem, misalignment can fail or silently fall back to buffering, and it
must not be mixed with buffered or mapped access to the same range. Query
`STATX_DIOALIGN` where available; default to buffered I/O whenever capability
or alignment cannot be proved.

## Sequenced Plan

1. Land the scanners and this audit; add baseline observability and a
   compaction-plus-hot-read benchmark.
2. Eliminate A3 and A6 with focused unit/property tests; stream A4 staging and
   sidecar upload with budget tests.
3. Measure A2 and A5 under representative workload; decide whether a bounded
   application cache or batching refinement is justified.
4. Only then prototype a Linux compaction/rebuild direct-I/O mode behind a
   default-off feature/config gate, with the measurement and correctness gates
   above required for promotion.
