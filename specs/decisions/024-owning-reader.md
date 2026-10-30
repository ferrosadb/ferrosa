# ADR-024: The merger owns its sources, so the `&'static` goes away

Status: partially implemented — see *Blocker found while implementing*
Date: 2026-10-30
Context: `docs/unsafe-ledger.md` audit item 1 — `store.rs`, the fabricated `'static`

## The site

```rust
let view_ref: &'static StoreView = unsafe { &*Arc::as_ptr(&view) };
let readers_ref: &'static [Arc<SSTableReader<R>>] = unsafe { &*(&*readers as *const _) };
let mappings_ref: &'static [ColumnOrdinalMapping] = unsafe { &*(&*mappings as *const _) };
let wanted_ref: Option<&'static [u16]> = wanted.as_deref().map(|w| unsafe { &*(w as *const _) });
```

`OwnedMerger<R>` is a self-referential struct: `merger: RangeMerger<'static, R>` borrows the boxed
slices declared after it. Moving the struct moves the `Box`/`Arc` pointers, not their allocations, and
the struct drops `merger` first, so the borrows are in fact valid for exactly as long as they are used
— Miri under Tree Borrows agrees for all three `owned_merger_*` tests.

That is evidence, and it is not the question the ledger asks. The fabricated `'static` is a promise to
the borrow checker that the compiler cannot check, and the promise outlives the reasoning that
justified it: nothing in the type system stops a future edit from handing `view_ref` to something that
outlives the `Arc`, or from dropping a field in a different order.

## Decision

Make the merge path own what it reads, and delete the borrows that need the cast.

1. `SSTableReader`'s read-path fields (`data`, `compression_info`, `crc_table`, the two chunk caches)
   become `Arc`-shared, so an iterator can clone them out. **Done.**
2. `ReadSource<R>`, `ScanData<R>`, `ChunkedCompressedData<R>`, `ChunkedUncompressedData<R>` own `Arc`
   clones instead of borrowing. **Done.**
3. `PartitionIter<R>` owns `Arc<SSTableReader<R>>`; `partitions_iter`/`scan_iter` take
   `self: &Arc<Self>`, because an `&self` method cannot hand out an owning handle to itself. **Done.**
   This is a public API change for callers holding a bare reader; every caller in `ferrosa-sstable`
   and `ferrosa-storage` now holds the `Arc` it already clones from the reader pool.
4. `MergeSource`/`RangeMerger` drop their lifetime parameter; `runs_arena` becomes owned data and its
   `Drop` impl disappears. **Pending — this is what deletes `readers_ref` and `runs_arena`.**
5. `store.rs` keeps only what it still needs to own. **Pending.**

## Blocker found while implementing: the readers are only half of it

`OwnedMerger::open` also builds the memtable sources from the store view:

```rust
let active_iter = view_ref.active.range_iter(start.as_ref(), end.as_ref());
let flushing_iters: Vec<_> = view_ref.flushing.iter()
    .map(|sealed| sealed.memtable.range_iter(start.as_ref(), end.as_ref())).collect();
```

Those iterators borrow the memtable, not the reader:

- `fn range_iter<'a>(&'a self, ..) -> Box<dyn Iterator<Item = Arc<Partition>> + Send + 'a>`
  (`memtable/mod.rs:718`), and both implementations honour that lifetime:
- skiplist: `self.map.iter()` — a crossbeam-skiplist iterator borrowing the map (`skiplist.rs:190`);
- sharded: `ShardedRangeIter::new(self, ..)`, holding `&'a self` and re-seeking under short-lived read
  locks (`sharded.rs:303`).

So the merge sources are `'a` for two independent reasons, and one of them is a property of the data
structure: there is no owning iterator over a borrowed skiplist that does not either pin the list with
`unsafe` (moving the unsafe rather than removing it) or snapshot the range into memory, which is
exactly what the streaming scan was built to avoid (`O(partitions_in_range)` pointers per scan).

**Consequence:** the `view_ref` cast cannot be removed by this ADR, and the ordering above is wrong —
steps 4 and 5 reduce the casts from four to one, not to zero. Removing the last one needs a decision
about memtable iteration, which is a different and larger change:

1. an immutable-per-generation memtable backed by an `Arc`'d sorted arena, so `range_iter` is an owning
   iterator over `Arc<[Arc<Partition>]>` — safe by construction, at the cost of insert amplification
   or generation merging;
2. a pinned arena per skiplist generation — still `unsafe`, merely relocated;
3. keep the borrowed view and take the narrower win: the merger owns its SSTable sources and
   projections, which deletes `readers_ref`, `mappings_ref`, `wanted_ref` and `runs_arena`, leaving one
   cast in the file with the memtable contract written down and tested.

Option 3 is a real reduction and does not block on the memtable question. Option 1 is the only one that
removes the last cast, and it should be its own ADR.

## Cost

One `Arc` clone per source per merger, where the code previously passed a borrow, plus one
`Arc<SSTableReader>` per iterator. Both are noise next to the I/O a merger performs, but the merge path
is hot during compaction and range scans, so it must be measured rather than asserted.

## What this does not fix

Holding `Arc<SSTableReader<R>>` keeps *that* reader alive; it does not stop `StoreView` from being
replaced. That is fine — replacement swaps the `Arc` — but it is also why the view cast is a separate
problem, not a corollary of this one.

## Verification

- `owned_merger_*` tests under `MIRIFLAGS=-Zmiri-tree-borrows`: they exercise move-after-borrow,
  unwind-from-panic and use-after-projection. They passed before this work with the cast in place, and
  must keep passing with it gone.
- Ledger: `store.rs` 4 -> 1 (option 3) or 4 -> 0 (option 1); `range_merger.rs` 3 -> 0. Rows move to
  `audited` with the Miri flags quoted.
- Benchmark the merge path before and after; a measurable regression is a reason to optimise the
  owning version, not to keep the cast.
