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

### 2. The lock site the user identified is real: `parking_lot::RwLock` at 2.43 %

Per-scan cost of **my** change (commit `654eae6d`), on the live path. The three
read-only scans moved from `snapshot()` to `range_iter`:

| | `snapshot()` (before) | `range_iter` (mine) |
|---|---|---|
| iteration | `map.iter()` | `map.iter()` (.iter().filter()) |
| clone | **deep-clone every partition** (`(**entry.value().read()).clone()`) → `.collect::<Vec<Partition>>()` → **all value `read()`s dropped when the Vec is built** | `Arc::clone(&entry.value().read())` — **value `read()` held for the whole scan** |

Same per-entry **read-lock acquisition**, same traversal. The difference is
**tenure and shape**:

- `snapshot()` **drains to an owned `Vec`** — the lock traffic is a short burst inside
  one call, then the scan proceeds lock-free over the copy.
- `range_iter()` is **lazy** — the `read()` guard is held *while the consumer walks*,
  which on the read path is *interleaved with writer `put`s*. So a reader now holds
  partition value locks across far more wall-clock time, against 512 writer threads
  that write-lock the same per-partition `RwLock` (SkipMap `insert` is lock-free, but
  the value merge is a write lock).

Add: `filter()` on every entry (two bound comparisons) and `start`/`end` **clones**
(`skiplist.rs:184-185`) — the pre-change path filtered inside the collect.

That is a **contention-window** regression, not an extra-lock-per-element one, and it
is on exactly the hot path 512 writers touch. It also explains the p99 jump: writer
p99 rises when locks are held longer by readers.

### 3. The fulltext build walks the table TWICE

`store.rs:9307-9311` (introduced by my change):

```rust
for partition in guard.active.range_iter(None, None)     { add_partition(&partition); }
for flushing in guard.flushing.iter() {
    for partition in flushing.range_iter(None, None)     { add_partition(&partition); }
}
```

Both walks happen **while `guard` (the whole-store guard) is held**, and the outer
`guard.active` / `guard.flushing` accessors are themselves lock acquisitions. The old
code snapshotted into owned data and could drop locks before building. So the hold
window here is longer than before in the worst place.

### 4. What the fix must NOT be

- **Do not revert to `snapshot()`** — it deep-clones the entire table to serve a
  mostly-empty check (the late-writer drains replay nothing on a healthy cluster). The
  user's rule stands: read paths must not materialize the table.
- **Do not "fix" `sharded.rs`** — it is not live. My rewrite there is inert; the honest
  move is to keep it (it is a correct lazy iterator with tests) but stop citing it as
  the regression cause, and stop counting its memory win as a production gain.

### 5. The lock-free direction (user's point, and it is the real lever)

`ferrosa-storage/Cargo.toml` says the skiplist "uses crossbeam's concurrent map plus
per-partition CAS updates instead". It does **not** — the value is a
`parking_lot::RwLock<Arc<Partition>>` (2.43 % self in the profile), and `arc_swap`
is already in the dependency tree (31 frames, 2.08 %). Two credible shapes:

1. **`ArcSwap<Partition>` per value** instead of `RwLock<Arc<Partition>>`: read locks
   disappear entirely (`load()` is a lock-free atomic load), so a scan holds **zero
   locks**. The write merge becomes `rcu`-style load → modify → `store`, retried on
   CAS failure. This is the standard read-optimized/copy-on-write memtable shape and
   directly removes the 2.43 % and the contention window above.
2. **Drop `range_iter` as the read-path scan** for the late-writer drains: those
   checks are *identity comparisons of already-known keys* — they do not need to walk
   the table at all. Ask the bounded question (does key X exist, and is it later than
   the seal?) via point lookups instead of a full unordered scan.

Both preserve streaming (no materialization) and both are invariant-testable: value
visibility must obey the same rules, and no row may be lost or reordered.


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
