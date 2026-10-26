# Performance patterns — the write path

Rules for a hot path in ferrosa. Each one exists because a measured or structural cost
was attributed to it; each has a testable form so a reviewer can check it without
re-deriving the reasoning.

## P1 — Input is immutable; `mut` only on state you own

Data arriving from the network or disk is never mutated. `mut` is legitimate on:

- scratch/reused buffers you own (`out.clear(); fill(&mut out)`) — note this is a direct
  trade against P4: zero allocation *requires* a mutable reused buffer, so reuse wins;
- accumulators, cursors-as-value, `&mut self` on engines/state machines;
- in-place sort/compaction of data you own.

`mut` on a decoded input value is a defect. The signature is the API: `&mut Row` on the
*incoming* row invites rewriting the caller's data.

```rust
// wrong: merge mutates the input it was handed
fn merge(existing: &mut Row, incoming: &mut Row)

// right: read the input, write owned state, take cells by move
fn merge(existing: &mut Row, incoming: Row)
```

**Test:** a value assertion that the input is byte-identical after the call.

## P2 — Move, do not clone

A clone at a lifetime boundary means ownership was never threaded. If one consumer needs
it longer, **move it in**. If several do, hand out **handles** (`Bytes`, `Arc<str>`,
`Arc<[T]>`) — a refcount bump, not a payload copy.

The distinction that matters is not `clone()` the method but *copying bytes vs bumping a
counter*. `bytes.clone()` at a boundary is fine; `Mutation::clone()` re-allocates two
`String`s, the key, every row and every cell.

**Test (the one that cannot pass for a copy):** hold two consumers' values alive and
compare the inner pointer (`ptr::eq` / `Arc::as_ptr`) — same address proves sharing.

## P3 — One buffer in, one buffer out; slices in between

The request payload is one `Bytes`. Rows, clusterings and cells are **ranges into it**,
not owned `Vec<u8>`s. The write out appends borrowed slices into a reused record buffer.
Fan-out hands each replica a `Bytes` clone.

Target: **allocations per write are flat in row count.** 1 → 100 → 10,000 rows costs the
same number of allocations, differing only in bytes.

**Test:** a 10k-row write uses the same record-buffer capacity as a 1k-row write.

## P4 — Reuse buffers across calls, and prove it

One buffer per statement, cleared and refilled, owned by the caller:

```rust
let mut derived = Vec::with_capacity(expected);
for msg in stream {
    derived.clear();
    derive_into(&msg, &mut derived);   // fills in place
    for item in derived.drain(..) { apply(item); }   // move out, keep capacity
}
```

Prefer an out-parameter (`out: &mut Vec<T>`) over returning `Vec<T>` for anything called
per write. A returned `Vec` forces one allocation per call that no callee can avoid.

**Test:** capacity after N writes equals capacity after the first; assert capacity, not an
allocation counter.

## P5 — Cursors are values, not mutable state

```rust
#[derive(Clone, Copy)]
struct SliceCursor<'a, T> { slice: &'a [T], index: usize }
```

`Copy`, no `T: Clone` bound, `next(self) -> Self`. Prefer `next_item(self) -> Option<(Self, &'a T)>`
so termination is in the type — a *saturating* `next()` that stalls at the last element
makes `while get().is_some() { next() }` an infinite loop.

Do **not** use a linked zipper (`Cons(T, Arc<...>)`) for linear data: it costs one `Arc`
allocation per step and needs `T: Clone`. For tree-shaped navigation a real zipper is
justified, but only with `unzip()` — a zipper that cannot rebuild the structure cannot edit.

## P6 — Many small objects: arena, not one allocation each

For per-row collections (`Vec<Cell>`), allocate one arena chunk per write and hand out
`(offset, len)` indices. For the dominant small case use `SmallVec`. Only reach for `Box`/`Arc`
per item when the item genuinely outlives the arena.

## P7 — Counters are atomics, not locks

`AtomicU64` + `fetch_add(Relaxed)` keeps the counter off the `&mut` path and off the lock.
A `Mutex<u64>` on a hot path is worse than a `mut` binding: it hides the contract and
serialises the thing you were measuring.

## P8 — tokio `mpsc` correctness (we keep it; do not hand-roll a ring)

Non-negotiables when using `tokio::sync::mpsc`:

1. **Bounded**, with capacity from config — never `unbounded_channel` on a write path.
2. **Never hold a lock or borrow across `.send().await` / `.recv().await`.**
3. **Close is explicit**: dropping the last `Sender` closes; the receiver must treat
   `None` as shutdown, and any blocked producer must be woken (tokio does this).
4. **Backpressure has a named policy**: block, or drop with a counted/logged event — never
   silently drop. If full means drop, the drop must increment a counter a test can see.
5. **Steady state allocates ~1 block per 32 messages**, not per message. If a profile shows
   per-message allocation, the bug is in the code around it (e.g. cloning per send).

## Irreducible floors — do not chase these

- Unbounded output cannot live in a fixed buffer: stream or spill, do not "optimise" it.
- A decoder manufacturing owned values has nothing to reuse: needs a view/borrowed decode.
- Parked/retried writes must own their bytes (see `ParkedWriteSet`).
- Sort/aggregate are O(n)/O(groups) by definition.
- The request buffer itself: one per request.
