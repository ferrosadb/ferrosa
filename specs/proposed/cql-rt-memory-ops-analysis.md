# Memory-operations cost analysis (`cql-rt`, PR #541 profile)

> Last updated: 2026-10-08
> Status: Analytical — no code changed
> Basis: `pr537_full.svg` (3-node cluster, CQL 512 threads, 1525.2 s CPU total)
> Companion: `cql-rt-copy-analysis.md` (the clone/Arc analysis this extends)

## Headline

**Memory-management work is ~16.7 % of all CPU** (allocator ∪ memcpy, disjoint), an
order of magnitude more than the ~1.5 % of application deep-copies that PR #541
targeted. If clones were the last round, this is the next.

| Bucket | Self-cost | Nature |
|---|---|---|
| Allocator (`malloc`/`realloc`/`free`, jemalloc + Rust `alloc`) | **7.84 %** | churn |
| `memcpy` / `memmove` / `memset` | **8.85 %** | bytes moved |
| **Disjoint memory-management total** | **16.7 %** | allocator ∪ memcpy (verified 0-frame overlap) |
| — of which: kernel page fault + page alloc + `clear_page` | (3.79 %) | **nested inside the two above — do NOT add** |

The allocator and memcpy sets are **disjoint** (692 vs 147 frames, zero overlap), so
16.7 % is not double-counted. The kernel 3.79 % is *layered inside* those frames
(children of allocator/memcpy calls), so it is a consequence of the churn and must
not be added again — the memory-related total is 16.7 %, not 20.5 %.

## Correction to the earlier number (important)

The first spec reported **memcpy 8.20 %** by summing `self` over frames whose name is
exactly `"memcpy"` — but on Linux the same function is symbolised in several ways.
The honest figures:

- frames named exactly `"memcpy"`: 125, self **8.18 %**, inclusive 14.65 %
- plus `memmove` 0.31 %, `memset` 0.33 % → **8.85 %**
- the single largest node is 4.54 % *inclusive* (1.37 % self) — my earlier spec
  quoted that 4.54 % as if it were a self-cost. It is not.

The 4.54 % node also contains **2.71 % of `asm_exc_page_fault`** as children: that
"memcpy" is partly **memory being paged in on demand**. Some of what looked like copy
cost is page-fault cost caused by allocating fresh pages.

## The kernel signal — a page-fault storm

| Kernel frame | Self-cost |
|---|---|
| page fault (`asm_exc_page_fault` → `do_page_fault` → `handle_mm_fault`) | **2.10 %** |
| page allocator (`get_page_from_freelist`, `__alloc_pages`, `rmqueue`) | **1.04 %** |
| `clear_page*` (zeroing freshly-mapped pages) | **0.66 %** |
| total | **3.79 %** |

Where the faults sit (coarse caller):

| Caller | Self-cost |
|---|---|
| inside `memcpy` | 0.88 % |
| TLS / wire buffers | 0.17 % |
| storage write / commitlog | 0.16 % |
| sstable / compaction | 0.14 % |

**Page faults + page-alloc + zeroing = 3.79 % is the signature of growing buffers and
short-lived allocations hitting fresh mmap regions.** `clear_page` at 0.66 % is the
kernel zeroing pages the process never reused — the classic symptom of
grow-from-zero / realloc-churn, not of steady-state reuse.

## The smoking gun: response buffers grow from zero

`ferrosa-cql/src/result.rs` builds **every** response body with `BytesMut::new()`
(zero capacity) → `put_i32` → metadata → rows:

- `result.rs:29, 57, 102, 143, 183, 234` — all `BytesMut::new()`
- the two hot SELECT paths (`router.rs:5411`, `router.rs:5479`) call
  `encode_rows_raw_with_writer`, which starts at `BytesMut::new()` (line 183)

A `BytesMut` growing from 0 doubles its capacity as it fills, and every growth step is
a **realloc = alloc + memcpy + free**. For a paged result of tens of KB that is
~10–15 growth steps per response, each memcpy'ing everything written so far. This is
the mechanical explanation for both the realloc share (1.97 %) and a large part of the
memcpy share, and it directly causes the page-fault/clear_page signal above.

**The row count is known before encoding.** `encode_rows_raw_with_writer` emits
metadata, reserves 4 bytes for the row count, then streams rows — so a capacity hint
(rows × avg-cell-bytes, or `column_count × rows × k`) removes the geometric growth
entirely. Same for the paged variant.

## Allocator cost attributed to callers

| Nearest ferrosa caller | Allocator self-cost |
|---|---|
| `ferrosa_cql` | **3.55 %** |
| `ferrosa_storage` | 1.20 % |
| `ferrosa_cluster` | 1.14 % |
| `ferrosa_sstable` | 0.70 % |
| `bytes` crate | 0.36 % |
| `rustls` | 0.36 % |

Split by kind: realloc/grow **1.97 %**, fresh alloc **1.17 %**, free 0.15 %.

Top exact sites:

| Site | Self-cost |
|---|---|
| `router::route_prepared_insert_fast` | 0.70 % |
| `handle_connection` harness | 0.48 % |
| `connection::build_request_context` | 0.47 % |
| `<Handle>::spawn::handle_connection` | 0.44 % |
| `coordinator::coordinate_write_with` | 0.31 % |
| `SkipListMemtable::put` | 0.27 % |
| `rustls DeframerVecBuffer::read` | 0.26 % |
| `connection::cql_value_to_term` | 0.25 % |
| `row_bridge::build_decorated_key` | 0.23 % |
| `row_bridge::codec::encode_value` | 0.21 % |
| `bytes_mut::BytesMut::reserve_inner` | 0.18 % |

## Targets, ranked by expected value / risk

1. **Size the response buffer** (`result.rs:183`, `:143`) from the known row count /
   column count instead of `BytesMut::new()`. Removes geometric realloc +
   repeated memcpy + the fresh-page churn for every response. Low risk, mechanical.
2. **`build_request_context` (0.47 % allocator + 1.11 % self).** Confirmed here
   independently: it allocates a default `AuthContext` (two `String`s) and
   `peer.to_string()` **per request**. The client address is connection-invariant —
   format once per connection.
3. **Per-response scratch reuse.** `bytes_mut::shared_v_drop` 0.18 % and
   `reserve_inner` 0.18 % are `BytesMut` lifecycle; a per-connection reusable
   response buffer (cleared, not reallocated) removes the alloc/free pair per request.
4. **`route_prepared_insert_fast` (0.70 % allocator).** The `pk_vals` / `ck_vals` /
   `regular_cells` / `pending_collections` Vecs are built per insert with no capacity
   hint; and `cql_value_to_term` 0.25 % + `raw_bytes_to_term` 0.19 % is the
   bound-value `Term` allocation (same ~0.46 % memcpy noted in the companion spec).
5. **`rustls DeframerVecBuffer::read` 0.26 %** — the TLS read buffer growing. Check
   whether tokio-rustls reuses a connection buffer or grows one per record.

## Regression report: #541 vs #537 at t512

**Verdict: the regression is real, it is mine, and my first explanation for it was
wrong.** Two separate things had to be untangled.

### 1. The sharded BTree memtable is NOT the live backend

The first diagnosis blamed the per-element `RwLock` + `BTreeMap::range` seek I added
to `ShardedRangeIter`. That cannot be the t512 regression, because **that code is not
in the profile at all**:

```
ShardedBTreeMemtable   0 frames      seek_shard     0 frames
ShardedRangeIter       0 frames      shard_data     0 frames
BTreeMap               5 frames, 0.181 %
```

`ferrosa-storage/Cargo.toml` has `default = ["skiplist-memtable"]`, and the comment
there says it outright: *"The production write path should avoid the sharded BTree
memtable's parking_lot::RwLock hot path by default."* `store.rs:1281` constructs
`Arc::new(SkipListMemtable::new())`. So `sharded.rs` is only live under
`--no-default-features` / Miri. My `range_iter` rewrite (commit `573ced85`) touched a
structure the shipped binary never instantiates. **It also cannot be the regression**
— and equally, it never delivered its memory win in production either.

The live backend is **`SkipListMemtable`** — `SkipMap<DecoratedKey, RwLock<Arc<Partition>>>`.

### 2. The live mechanism: refcount inflation → `Arc::make_mut` copy-on-write

**Correction (second pass).** The first explanation blamed "lock tenure" — the claim
that `range_iter` holds each partition's value `read()` guard across the consumer's
walk. That is **wrong**, and the code says so:

```rust
.map(|entry| Arc::clone(&entry.value().read()))   // skiplist.rs:195
```

The `read()` guard is a **temporary inside the `map` closure**: acquired, the `Arc`
cloned, **released** — per element. `snapshot()` did exactly the same per-element
acquire/release (`(**entry.value().read()).clone()`). So per-entry lock acquisition
AND tenure are both unchanged. Readers never held a lock across a scan, and readers
blocking readers is not the issue.

What actually changed is which of two things the guard's critical section produces:

| | `snapshot()` (pre-#541) | `range_iter` (#541) |
|---|---|---|
| guard | temporary per element | temporary per element — **same** |
| what is cloned | `Partition` (deep) → owned `Vec<Partition>` | `Arc<Partition>` → handed to the consumer |
| **strong count after the guard drops** | **1** — nothing holds it | **2** — the consumer holds it |
| writer cost when a `put` collides | in place — 3 allocations | **1210 allocations for a 400-row partition** |

`SkipListMemtable::put` merges through `Arc::make_mut(&mut guard)`, which is
**copy-on-write**: at refcount > 1 it deep-clones the whole partition before
mutating. `snapshot()` deep-cloned on the *reader's* side and left the count at 1,
so it never charged the writer. `range_iter` hands the consumer a refcounted clone,
so **every write landing while a scan holds partitions pays an O(rows-in-partition)
clone** — the O(N^2) fill pathology `e440b60f` removed, i.e. the fix for this exact
regression. This is why the t512 throughput drops while CPU per request falls: the
writer is doing allocator/drop work on a slower path rather than useful work.

**Pinned, not inferred.** `ferrosa-storage/tests/memtable_read_does_not_block_writes.rs`
holds a clone of the partition's `Arc`, issues one write, and asserts the *staleness*
that copy-on-write produces: the holder keeps the pre-write image while the memtable
owns the post-write one. The inverse (no holder → the write is visible immediately)
is the negative control. Asserted as behaviour rather than an allocation count,
because a process-wide counter is not usable across parallel tests — an earlier
draft measured it that way and was flaky (12/12 parallel failures).

An earlier probe also recorded 3 allocations without a holder vs 1,210 with one for
a 400-row partition. Those absolute numbers are indicative only: a per-write count
includes amortized `Vec` growth, so it is not a stable assertion (the same write
costs 3–167 in the 5-crate run depending on concurrent load). The *ratio* — orders
of magnitude, growing with the partition — is the signal.

`filter()` + per-entry bound comparisons and the `start`/`end` clones are real but
secondary — a few comparisons against an O(rows) clone.

### 3. The fulltext build scans the writable tier on the query path

**Correction.** Pre-#541 already walked the table twice
(`git show origin/main:ferrosa-storage/src/store.rs` → `add_partitions(guard.active.snapshot())`
then `for flushing in guard.flushing...`), so the double walk is **pre-existing, not
introduced here**. Two things about it are still wrong:

1. It scans the **active** memtable — the tier writers are filling — on a query
   path, so the read/write overlap above is at its worst here.
2. The index is rebuilt **from scratch on every query**: `FullTextIndexBuilder::new()`
   → analyze every row of active + flushing → `build()` → search → drop. There is no
   cache (`fti_cache`/`LazyLock` in `store.rs`: nothing). A query against an N-row
   memtable re-analyzes N rows, and `analyze()` allocates per row
   (`to_lowercase()` + 1 `String` per token + `Vec` + `HashMap` + per-term key clones).

(3) is the dominant cost on this path and needs cache invalidation or an incremental
memtable index — out of scope here, recorded as follow-up.

### 4. What the fix must NOT be

- **Do not revert to `snapshot()`** — it deep-clones the entire table to serve a
  mostly-empty check (the late-writer drains replay nothing on a healthy cluster),
  and materializing on a read path breaks the streaming rule.
- **Do not "fix" `sharded.rs`** — it is not live (see §1). The rewrite there is inert.
- **Do not use `ArcSwap<Partition>` on the value.** This was already implemented and
  reverted (`e440b60f`): `arc_swap` has no `DerefMut`, so publishing a changed value
  requires building a new one → O(rows) per write → O(N^2) fill, 2,109,500 allocations
  for 1000 writes, 52% wall time in `drop_slow`, ~95% CPU for 18 minutes with no CQL
  listener. The earlier draft of this section proposed it; that was wrong.

### 5. The fix that landed (this PR)

Read paths must **borrow** rather than hand out a refcounted `Arc`:

- **`Memtable::for_each_partition(start, end, &mut FnMut(&Partition))`** — borrows each
  partition for the callback and never clones the `Arc`, so the memtable stays the sole
  owner and concurrent writes keep merging in place (I-2). The read guard IS held across
  the callback, which is sound (readers don't block readers) and required: without it
  `Arc::make_mut` would mutate the partition under a reader. Callbacks must be short.
- **`Memtable::for_each_partition_cloned(..)`** — for callbacks that do real work per
  row (the fulltext build). Deep-clones under the guard, **releases it**, then calls back.
  A writer waits for one memcpy, not for the whole analysis (I-5). Deliberately
  `(**guard).clone()`, never `guard.clone()`: the latter resolves through `Deref` to
  `Arc::clone` and would silently re-introduce the refcount inflation this exists to
  remove. A `debug_assert_eq!(strong_count, 1)` pins it.
- **`Memtable::write_epoch()`** — O(1) "did anything land after the snapshot?", so the
  late-writer drains skip their whole-table walk on a healthy cluster. `UNTRACKED_WRITE_EPOCH`
  means "rescan", so an implementation that forgets to track it stays safe (I-1).
- **All three read-only sites** moved off `range_iter` (both late-writer drains + the
  fulltext build), and the fulltext build now reuses hoisted scratch
  (`RowScratch` + `Analyzer::analyze_into` + `add_document_with_tf` +
  `FullTextIndexBuilder::with_capacity`), removing the per-row allocations.

Why not "just make `range_iter` lazy-but-bulk-drain" as originally suggested: that
restores the deep clone of every partition on every scan — a CPU-for-memory trade in
the wrong direction, and it violates the no-materialization rule.

### 6. Invariants pinned by this PR

- **I-1 no data loss** — the late-writer drain still catches every write after the
  snapshot; `write_epoch` may only skip the walk when provably unchanged.
- **I-2 no refcount inflation on read paths** — a borrowed scan leaves the memtable the
  sole owner, so concurrent writes merge in place.
- **I-3 complete and ordered** — visits match `snapshot` on count, order and content.
- **I-4 no materialization** — one partition live at a time, never the table.
- **I-5 bounded blocking** — the cloned visit releases the guard before the callback.
- **I-6 identical index** — the reused-scratch fulltext build is byte-identical to the
  old per-row-allocating path.
- **I-8 no per-row allocation growth** — the fulltext build's per-row cost does not
  grow with how much is already indexed.

Tests: `ferrosa-storage/tests/memtable_read_does_not_block_writes.rs` (8 tests, incl.
two negative controls that fail if the guard loses its teeth),
`ferrosa-storage/tests/fulltext_scratch_equivalence.rs` (4 tests),
and the staleness pin inside `memtable_read_does_not_block_writes.rs` is the
measurement behind §2.

### 8. Driver Smoke flake (CI, merge commit) — investigated, not ours

`Driver Smoke (ubuntu-latest)` failed once on the merge commit `74ab6c10`, then
**passed on rerun of the identical commit** — intermittent, not deterministic.

The failure was in the range-scan producer:

```
storage: invalid data: range scan producer failed: task 15694 panicked with
message "scheduler blocking task must not be cancelled: JoinError::Panic(Id(15695),
"A Tokio 1.x context was found, but IO is disabled. Call `enable_io` on the runtime
builder to enable IO.")"
```

Cause: `ferrosa-sched/src/lib.rs:650 scan_carrier()` builds the scan-carrier runtime
with `.enable_time()` and **no `.enable_io()`** (pre-existing on `origin/main`, not
changed here). Any code reached from a scan producer that touches Tokio IO panics
with exactly that message. The panic is surfaced as a fail-loud
`range scan producer failed` error rather than a silent empty result, so the
diagnosis is in the error text, not in a missing row.

Why this is not this PR's change: the diff against `origin/main` has no hunks below
`store.rs:5272` and touches neither `ferrosa-sched` nor the producer at
`store.rs:2295-2360`. Recorded here because it is a real latent bug and it will
recur; the fix is to give the carrier `enable_io()` (or ensure nothing on a scan
producer path does Tokio IO), which is a scheduler change with its own review.

### 9. Follow-ups (not in this PR)

- **Same defect class, same-class sweep DONE.** `store.rs` had four more read-only
  scans of the **active** memtable through `range_iter(..)` — `read_token_range_once`,
  the token-range vector producer, `walk_token_range_for_digest`, and the
  vector-search source builder. All converted to the borrowed scan with a fresh
  `Arc::new(p.clone())` for the consumer, so the memtable keeps sole ownership.
  They were on `origin/main` already (pre-existing, not from #541) and are cold in
  the write profile (`read_token_range` 0 frames, `walk_token_range` 0.075 %,
  digest 0.182 %), so that sweep is ownership correctness rather than a benchmark
  win.
  **Key scoping rule:** only the **active** tier carries this tax. Sealed/flushing
  memtables accept no writes, so consumers may hold their `Arc`s freely — do not
  "fix" those.
  `for_each_partition` now takes `-> bool` (return `false` to stop): the borrowed
  scan walks the whole table otherwise, so converting a bounded `range_iter` loop
  into it would turn an O(matches) read into an O(table) walk *and* an O(table)
  lock sweep.
- **The transient memtable FTI is rebuilt per query.** Cache it (invalidate on write /
  memtable rotation) or build it incrementally. This is the dominant cost on the
  fulltext path — larger than everything above.
- **`ArcSwap`-style lock-free reads** are *not* the answer (see §4), but a memtable
  that publishes immutable partitions (build a new `Partition`, then swap) would let
  reads be lock-free *and* keep writes O(1) — the opposite of the reverted design. Worth
  evaluating separately, with the O(N^2) write test as the gate.
- **`analyze()` still allocates a `String` per token.** Removing it needs `Cow<'_, str>`
  tokens; measured only after the per-query rebuild is addressed.


## Design: reusable response buffer (proposal)

Answering: *"can't we pre-allocate a buffer and reuse it, since it's streaming — and
align it?"*

**Reusable pool: yes. Single shared buffer: no — that would corrupt responses.**

### Why "it's streaming" does not make one buffer safe

The CQL connection is **not** a single serial producer. In `connection.rs`:

- `dispatches_concurrently(opcode)` = `Prepare | Execute | Batch` (line 979) —
  these are handed to `task_pool.spawn(..)` (line 608) so the loop can immediately
  read the next frame. `Query` stays inline (pinned by test at line 3528).
- Each spawned task sends its result over
  `mpsc::channel::<SpawnedResponse>(max_in_flight.max(64))` (line 399–400) to the
  connection's writer task, which calls `apply_handle_result` (line 472).
- Concurrency is bounded by `max_in_flight_per_connection`
  (**default 128**, `server.rs:58`; semaphore at `connection.rs:390`).

So up to **128 responses are being built at once per connection**, each needing its
own bytes until it is written. A single reused buffer would interleave two
in-flight responses into one allocation and emit corrupted frames. The correct
shape is a **pool**, sized by the in-flight budget.

### The output side is already allocation-stable

`CqlCodec::encode(&mut self, item, dst: &mut BytesMut)` (frame.rs:494) writes into
`Framed`'s **persistent write buffer**, which tokio-util reuses for the life of the
connection. So there is no per-response allocation on the socket path — the churn is
entirely the **intermediate body buffer** that `result.rs` builds and then hands to
the encoder (which copies it into `dst`).

### Shape

```rust
struct ResponseBufferPool { // one per connection, lives beside the resp channel
    free: Vec<BytesMut>,
    max_buffers: usize,      // = peak concurrent in-flight, NOT max_in_flight
}
const RETAIN_CAP: usize = 32 * 1024; // buffers larger than this are NOT retained
```

- `acquire()` → pop a buffer, `clear()`d, capacity retained; else
  `BytesMut::with_capacity(estimate)`.
- `release(buf)` → keep only if `buf.capacity() <= RETAIN_CAP` **and**
  `free.len() < max_buffers`; otherwise drop back to the allocator.

### Bounded-memory math (this is why the cap is not optional)

`BytesMut::clear()` **keeps capacity**. Retention per connection is

```
retained ≤ min(observed_peak_concurrency, max_buffers) × RETAIN_CAP
```

Two things make this safer than it first looks, and one thing makes it dangerous:

- **Safer:** the pool can never retain more than the buffers that were *already
  concurrently live* — today those bytes exist too (allocated per response, freed on
  drop); pooling makes the peak persistent rather than the peak larger. Net new
  memory is the gap between peak-concurrent-live and what is now retained.
- **Safer:** observed peak concurrency on this workload is small (~8), not 128.
- **Dangerous:** with `max_buffers = max_in_flight` (128) and **no** `RETAIN_CAP`, one
  large response on each of 512 connections retains 512 × 128 × capacity. Unbounded
  in capacity.

So `RETAIN_CAP` is the guard. There *is* a row cap on one scan shape —
`safe_partition_key_filter_row_limit` (router.rs:575) — but **no global cap on a
response body**, and `V5_MAX_PAYLOAD` (128 KiB) bounds *envelope slicing* for v5, not
total response size (oversize bodies are split across envelopes, frame.rs:842, 1499).
A normal large SELECT still produces a body far above any retained-buffer size, so
without `RETAIN_CAP` one such response pins memory for the life of every connection
that served it.

| Worst case | Retained |
|---|---|
| 512 conns × 128 buffers × 64 KiB, uncapped | **4 GiB** — unacceptable |
| 512 conns × 8 × 32 KiB (capped) | 128 MiB |
| realistic: peak concurrency ~8, most bodies < 4 KiB | **single-digit MiB** |

### Order of work — sizing beats reuse

| Change | Saves |
|---|---|
| `BytesMut::with_capacity(estimate)` from the known row/column count | **~10–15 realloc + memcpy rounds per response** (the big win) |
| Pooled reuse | the remaining **1 alloc + 1 free per response** |

Do the sizing first — it is strictly safe (no sharing, no retention question) and is
worth more. Reuse on top is then a smaller, riskier increment.

### Alignment: skip it (evidence, not preference)

The body buffer is written with `put_u8` / `put_u32` / `put_slice` and is then
**memcpy'd by the encoder into `Framed`'s write buffer**. Nothing DMA-reads it, no
SIMD kernel consumes it, and it holds no atomics. Alignment pays when a buffer is an
`O_DIRECT` DMA target, a SIMD source, or a contended atomic line — this is none of
those, so an aligned allocation would add API surface (`aligned_alloc`, custom
allocator, `Layout`) for ~0 measurable gain.

Where alignment **is** load-bearing in this stack is the SSTable `O_DIRECT` path —
`ferrosa-sstable/src/dio_align.rs` (+ `direct.rs`, `pump.rs`, `writer.rs`) already
handle it. That is the right place for alignment work; the CQL response buffer is not.

The one real (small) alignment effect here is **false sharing**: pooled buffers
belonging to different connections can land on one cache line, and each connection is
driven from a different worker core. Padding each pool slab to 64 B avoids it. That is
a line of code and defensible — but it is a ~0.x % effect, not the reason to pool.

### Invariants to pin before any of this lands

- **I-1 wire identity** — response bytes are bit-identical to today's path, with and
  without pooling (reuse the existing staging-writer oracle in `frame.rs` tests).
- **I-2 bounded retention** — per-connection retained bytes ≤ `entries × RETAIN_CAP`,
  asserted by a test, and no buffer above `RETAIN_CAP` is ever retained.
- **I-3 concurrency safety** — two in-flight `Execute` responses never share a
  buffer; a test that dispatches concurrent EXECUTEs and asserts every response is
  well-formed and stream-ids are not crossed.
- **I-4 no unbounded growth on a large response** — serve one oversized result, then
  assert the connection's retained capacity is unchanged.
- **I-5 alignment is wire-neutral** — padding the slab does not change any emitted
  byte (cheap regression guard if the padding is added).

## Method note (so the numbers are reproducible)

Allocations and copies are **many small frames**, not one hot frame. Ranking by
*inclusive* percentage hides them (they are spread); ranking by **self** requires the
tree rebuilt first (parent links), which is what makes `self = n − Σchildren` correct.
Summing `self` over a name-pattern set is the only way to see this class — and the
pattern must include the aliases Linux emits (`memcpy`/`__memcpy`/`memmove`/`memset`,
`malloc`/`mallocx`/`rallocx`/`_rjem_*`). A single-frame read understates it by ~2×.

## memcpy attributed to callers (required vs avoidable)

| Caller | Self-cost | Required? |
|---|---|---|
| `ferrosa_cql` | **3.82 %** | mixed — see below |
| `ferrosa_sstable` (compaction / serialise) | 2.18 % | **required** — encoding to disk |
| `ferrosa_storage` (commitlog / memtable) | 1.08 % | **required** — durable append |
| TLS / crypto | 0.67 % | **required** — record framing |
| `ferrosa_cluster` (coordinator) | 0.55 % | mostly required (wire/bincode) |
| `bytes` crate | 0.02 % | — |
| unsymbolised | 0.23 % | — |

So of the 8.85 %: **~4.5 % is required I/O serialisation** (sstable + commitlog + TLS),
and **~3.8 % is inside `ferrosa_cql`** — the region that is partly avoidable.

The `ferrosa_cql` 3.82 % breakdown:

| Site | Self-cost | Read |
|---|---|---|
| `handle_connection` (direct) | 1.37 % | the unsymbolised node; 2.7 % of its children are **page faults**, so part of this is memory being faulted in, not copying |
| `tokio::raw::poll::<handle_connection>` | 1.25 % | response write path (TLS + socket) |
| `raw_bytes_to_term` | 0.17 % | bound-value `Term` copy — same as the companion spec's 0.46 % |
| `route_prepared_insert_fast` | 0.16 % | insert key/row assembly |
| `strip_wrapper` | 0.14 % | **already fixed in PR #541** (was the `format!` alloc) |

The two `handle_connection`-anchored rows (2.62 % combined) are the unsymbolised
region: musl's `memcpy` asm has no CFI, so `--call-graph fp` cannot name the callee.
They are **not** the same thing as the codec, and 2.7 % of the first node's children
are page faults — i.e. some of that number is memory being paged in, which ties back
to the grow-from-zero buffers above.

## Open questions

- [ ] What is the *steady-state* allocator share vs the burst share? This run is a
           512-thread write load; the realloc:alloc ratio (1.97 : 1.17) suggests
           growth dominates, which points at buffers rather than per-row structs.
- [ ] Is `clear_page` 0.66 % transient (ramp-up) or steady? A longer capture that
           discards the first N seconds answers this — if it is ramp-only, target #3
           matters less than #1.
- [ ] Attribute the two `handle_connection` memcpy rows (2.62 %) — needs an
           instruction-level capture, not another frame-pointer run.
