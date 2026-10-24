# HANDOFF — write-path performance programme

**Read this first.** It is the entry point for any session picking up the write-path
performance work. Detail lives in [PATTERNS.md](PATTERNS.md) and [AUDIT.md](AUDIT.md);
this file carries the state, the ordering, and the traps that are expensive to rediscover.

## Where the work stands

| item | state |
|---|---|
| Pattern set (P1–P8) | committed — `docs/performance/PATTERNS.md` |
| Audit + crate briefs (classes A–F) | committed — `docs/performance/AUDIT.md` |
| Observer drain reuses one buffer (`derive_into`) | **landed** — commit `f79f4d63e` |
| Schema-on-write for graph labels (vertices) | **landed, local only** — commit `8b005153a`, not pushed |
| Relationship MERGE (implicit keys) | **blocked** — issue #610, write and read key derivation must change together |
| Class F (observer drops lose index entries) | **not started — do this first** |
| Storage payload model (`TableRef`/`Bytes`/`RowView`) | **not started — blocking for the rest** |

Branch: `feat/graph-schema-on-write` in worktree `.wt-schema-on-write`, rebased onto
`origin/main`. Nothing from this programme is pushed.

## Next action, precisely

**Correctness first (Phase 1: C1–C4 in AUDIT.md § Order of work), then performance
(Phase 2: T1–T5).** A perf win that leaves data unreachable is worthless.

**C1 first**, because it is a correctness bug and because the Arc payload that fixes its cost
is the same payload model the perf work needs.

Red test: register an async observer with **capacity 1**, do not drain, write N edges, then
drain and assert **every edge is traversable** — or that the keyspace is marked dirty and
the reconcile repairs it. Today: N−1 drops, no marker, N−1 untraversable edges.

Fix, both halves:
1. refcounted payload so `engine.rs:9338` (`mutation.clone()` in `try_send`) is a pointer;
2. **backpressure, never drop** (owner decision, final). Index entries must never be
   discarded — a dropped entry means committed data with no path to it. Preferred shape:
   inline fallback, running the drain's derivation synchronously on the writer when the
   queue is full. `blocking_send` only where the caller is provably not a runtime worker;
   never an unbounded channel.

Then brief 1 in AUDIT.md (storage data model), then cluster, graph, front ends.

## Decisions already made — do not reopen

- **Keep `tokio::sync::mpsc`.** Do not hand-roll a ring buffer. The two hand-rolled
  versions reviewed here needed `MaybeUninit` slots, `ptr::write`/`ptr::read` pairing,
  waker lifecycle, and had two real bugs each (unsynchronized waker cell; wake-while-
  holding-lock deadlock; missing `Consumer::drop` close). Its steady-state cost is ~1 block
  per 32 messages, not per message. Revisit only if a profile shows transport ≥ a few
  percent of write self-cost, and then use `rtrb` + `AtomicWaker` rather than own code.
- **No linked zipper for linear data.** `Cons(T, Arc<..>)` costs one `Arc` allocation per
  step and needs `T: Clone`. Use `SliceCursor` (`Copy`, no bound, `next(self) -> Self`) and
  `next_item(self) -> Option<(Self, &'a T)>` — a *saturating* `next()` that stalls at the
  last element turns `while get().is_some() { next() }` into an infinite loop (verified).
- **Stable over nightly.** `impl !Sync` buys nothing here; `PhantomData<Cell<()>>` gives the
  same auto-`!Sync` for zero bytes and no feature gate.
- **`mut` policy:** fine on owned state/scratch/cursors/accumulators; never on data from
  socket or disk. A reused buffer is a deliberate trade: zero allocations *requires* a
  mutable owned buffer.

## Traps — already paid for, do not pay again

1. **Do not clone at a lifetime boundary.** Move it in (one consumer) or hand out
   `Bytes`/`Arc` handles (several). A deep `clone()` there is the smell.
2. **Do not synthesise merge keys inside `build_schema_aware_merge_shape`.** It sets
   `used_schema_layout = true`, which skips `infer_schema_aware_merge_shape` — the read that
   resolves a *bound* endpoint's real key. Result: a duplicate endpoint vertex
   (`bolt_write_gaps::merge_after_match_is_idempotent`). Truncating the key on the write side
   alone also breaks traversal, because the read path still derives 32 bytes
   (`exists_pattern_*`, `concurrent_merge_*`). Both defects recorded on issue #610.
3. **Do not build amd64 with qemu.** `rustc` segfaults under qemu-user
   (`uncaught target signal 11`), 0 crates compiled. Get x86 binaries from CI: the
   `ferrosa-node-image` artifact of the *Build node image* job; `podman load` mislabels it
   arm64 because the manifest carries no platform — extract the binary and `COPY` it over an
   explicit `--platform linux/amd64` base instead.
4. **All performance measurement happens on Fly**, never local containers. Local loopback is
   for correctness only — and note that the local binary answers `SELECT 1` in **66–100 µs**
   while the same binary over Fly 6PN took 46 ms before the Nagle fix; that contrast *was*
   the diagnosis.
5. **Per-statement latency on the bench rig cannot compare engines.** The runner is
   co-located with pg18, so pg18 pays ~0.33 ms of path while other nodes pay ~1.3–2.7 ms.
   Use throughput, or normalise by measured RTT.
6. **A CPU flame graph cannot find a wait.** The 46 ms stall produced zero samples because
   the thread was blocked in `write()`. Off-CPU or an A/B is the instrument.

## Gates

```
cargo test -p <crate>            # never bare pytest/cargo test at workspace root for a slice
cargo clippy -p <crate> --all-targets -- -D warnings
cargo fmt -p <crate>
```

`ferrosa-graph` full suite was **608 passed / 0 failed** at `8b005153a`;
`ferrosa-storage` lib **1925 passed / 0 failed** at `f79f4d63e`.

## Open questions for the owner

1. Class F policy: **settled — backpressure only.** Dropping a derived index entry is
   silent data loss (committed data with no path to it) and is not permitted.
2. `FOREACH` implicitly creates labels (Neo4j-consistent). Confirm, and keep
   `foreach_with_a_create_creates_the_label` as the contract.
3. Secondary index for graph labels: `CREATE INDEX … FOR (n:Label) ON (n.pk)` fails with
   `CypherSyntaxError`, so every benchmark "point read" is a label scan (136 ms vs Neo4j's
   2 ms at 20k nodes). Bigger than any allocation win, and unowned.
