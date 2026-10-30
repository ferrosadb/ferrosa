# ADR-024: The merger owns its readers, so the `&'static` goes away

Status: proposed
Date: 2026-10-30
Context: `docs/unsafe-ledger.md` audit item 1 — `store.rs:2122`, the fabricated `'static`

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
outlives the `Arc`, or from dropping a field in a different order. Removing the borrow removes the
class.

## Why it cannot be removed narrowly

`RangeMerger` borrows its sources for *iteration*, not just for construction:

- `MergeSource::SsTable { iter: PartitionIter<'a, R>, mapping: Option<&'a ColumnOrdinalMapping>, .. }`
- `PartitionIter<'a, R> { sst: &'a SSTableReader<R>, source: ReadSource<'a, R> }`
- `ReadSource<'a, R>` wraps four data holders that each borrow the reader (`ScanData`, `ChunkedCompressedData`,
  `ChunkedUncompressedData`, `Raw(&'a R)`).

So the lifetime is threaded from the storage crate down into `ferrosa-sstable`'s reader. There is no
one-line fix at the call site, and the `runs_arena` leaked `*const [Vec<Arc<SSTableReader<R>>>]` in
`range_merger.rs` (with its manual `Drop`) is the same problem one level down.

## Decision

Make the reader path own its handles, and delete the lifetime.

1. `PartitionIter<R>` holds `Arc<SSTableReader<R>>` instead of `&'a SSTableReader<R>`; `ReadSource<R>`
   becomes an owning enum over `Arc<Mmap>` / `Arc<R>` (the mmap sites are already `Arc`-shared where
   they are mapped, and `FileReadAt` already has an `Arc<Mmap>` variant to copy from).
2. `MergeSource<R>` holds `Arc<[Arc<SSTableReader<R>>]>` for the run's readers and
   `Option<Arc<ColumnOrdinalMapping>>` for the projection, so a merger can be moved freely.
3. `RangeMerger<R>` loses its lifetime parameter; `runs_arena` becomes
   `Option<Box<[Vec<Arc<SSTableReader<R>>>]>>` and its `Drop` impl disappears.
4. `LiveCount<'a>` holds `Arc<TableSchema>`.
5. `OwnedMerger` keeps only its owned fields; all four casts and the `'static` annotations go.

Blast radius is `ferrosa-sstable` (reader, `ReadSource`, `PartitionIter`) and `ferrosa-storage`
(`range_merger`, `store`) — 13 `RangeMerger<` and 20 `MergeSource` mentions. It is a designed change,
not a drive-by, and it lands as its own PR.

## Cost

One `Arc` clone per source per merger, where the code previously passed a borrow. Both `Arc::clone`
and `Box` allocation are noise next to the I/O the merger performs, but it must be measured rather
than asserted: the merge path is hot during compaction and range scans.

## What this does not fix

The `Arc<SSTableReader<R>>` clone keeps the reader alive; it does not stop `StoreView` from being
replaced. That is fine — replacement swaps the `Arc`, and the merger holds the old one.

## Verification

- `owned_merger_*` tests under `MIRIFLAGS=-Zmiri-tree-borrows`: they exercise move-after-borrow,
  unwind-from-panic and use-after-projection, and must keep passing with no `unsafe` in the file.
- Ledger: `store.rs` 4 -> 0 and `range_merger.rs` 3 -> 0. Both rows move to `audited` with the Miri
  flags quoted.
- Benchmark the merge path before and after; a measurable regression is a reason to optimise the
  owning version, not to keep the cast.
