# Unsafe policy

The invariant, the permitted reasons, the evidence a site must carry, and the gate that enforces it.

**The invariant:** there is no `unsafe` in Ferrosa except where the platform or the language gives
no safe alternative, and never for performance.

`unsafe` is the part of this codebase that no proof and no model covers — the Lean corpus reasons
about a specified machine, Aeneas cannot translate `unsafe` at all, and Miri only sees the paths a
test happens to execute. What remains is hand review, and hand review of an unenumerated set does not
happen. So the set is enumerated, every entry carries a reason, and CI fails when the enumeration and
the source disagree. This document is the rule; `scripts/ci-gates/guard-unsafe.py` is the enforcement;
`scripts/ci-gates/unsafe-allow.toml` is the enumeration.

`docs/unsafe-ledger.md` remains the per-site audit queue — which invariants have been checked by hand,
and by what. This document is the admission rule: *may* a site exist at all, and under what kind.

## 1. Permitted kinds

A site is permitted only if it falls in one of these classes. The class is a property of the reason,
not of the keyword: `unsafe { libc::getrlimit(..) }` is `syscall`, while
`unsafe { slice::get_unchecked(i) }` is `perf` even though both are one `unsafe` block.

| Kind | Meaning | In this tree |
| --- | --- | --- |
| `ffi` | Calling into a C library or a crate's own `unsafe` API, where the ABI is the only interface. | jemalloc `mallctl`, `rquickjs` raw values and `Ctx`/`Value::from_raw` |
| `syscall` | A libc syscall or syscall wrapper with no safe Rust equivalent: `fcntl`, `sysconf`, `getrlimit`, `posix_fadvise`, `pwritev`, `statx`, `setenv`/`unsetenv`. | `F_FULLFSYNC`, `F_NOCACHE`, `SYS_statx`, `pwritev` scatter-gather |
| `ioctl` | `ioctl` specifically, where the request takes a struct the kernel fills in. | (none today) |
| `mmap` | A memory mapping, whose obligation is that nothing truncates or rewrites the file while it is mapped. | `memmap2::Mmap` on SSTables, sidecars, staged write sets |
| `allocator` | A `GlobalAlloc` implementation, which is an `unsafe trait` with `unsafe fn` methods by ABI. | `CountingJemalloc`, the `#[cfg(test)]` allocation probe |
| `layout` | `repr(C)`/`repr(packed)` structs crossing an ABI border, and the alignment/zeroing plumbing they need. | `KernelStatx`, `mem::zeroed()` for kernel out-params |
| `sync-primitive` | A concurrency primitive std genuinely does not provide. **The entry must name the primitive and say why std has no equivalent** — "std has no safe disjoint-mutable-ranges-from-a-shared-allocation primitive" is a reason; "we needed a lock-free buffer" is not. | (none today) |

Nothing else is permitted.

## 2. Findings, not categories

Two of the classes the inventory reports are **not** categories a site may live in. Both are findings,
and both fail the gate:

- **`perf`** — the site exists to go faster. This is the prohibited class. It does not matter how much
  faster: "it was slower" is not a justification, and a benchmark showing the safe version is 3%
  slower is an accepted regression, not an argument. The answer to a `perf` site is a safe rewrite,
  or a decision by the operator, recorded here, that the safe version's cost is unacceptable — which
  is still a finding, not an entry.
- **`unknown`** — the site's reason is not a platform mandate. This covers "no comment at all" and
  also the lifetime-convenience family: `&'static` laundering from `Arc::as_ptr`, `Box::leak` plus
  `&*ptr` self-referential structs, `unsafe impl Send` where std's safe primitives would do. Those
  move no memory and buy no speed, and they are *still not permitted*: the invariant has exactly one
  reason, and convenience is not it. The safe patterns are known and are listed with each finding
  below.

A site that moves no memory — a `&'static str`, an `Arc` cast, a lifetime extension — is never
reported as `perf`. The finding is the construct (a self-referential owner that cannot be expressed
safely), not the keyword.

## 3. What counts as evidence

A `// SAFETY:` comment is a **claim**. Nobody has verified it by reading it. The gate can check that
one exists and that it says something about a mechanism; it cannot check that the mechanism is true.

A site is **audited** — the only state in which it belongs in `docs/unsafe-ledger.md` with status
`audited` — when two things exist:

1. **A mechanism sentence.** Not the operation, the reason it holds:
   - not evidence: *"getrlimit is a standard POSIX call."*
   - evidence: *"`getrlimit` writes `rlimit` only when it returns 0; the `assume_init` is inside the
     `rc == 0` arm, so the out-param is never read uninitialised."*
   - not evidence: *"the pointer is valid."*
   - evidence: *"`self.mmap` is declared before `self.slice` and neither is moved, so the slice cannot
     outlive the mapping."*
2. **A test that fails when the mechanism stops holding.** The test must be *mutation-checked*: break
   the mechanism deliberately (move the `assume_init` outside the `rc == 0` arm; make the mmap file
   truncatable; drop the owner before the borrower) and watch the test go red. A test that passes both
   ways is not evidence, it is decoration.

The gate enforces (1) weakly and mechanically — see §6 — and cannot enforce (2) at all. (2) is the
reviewer's job, and a site without it is `audited: false` no matter how confident the comment sounds.

## 4. Review rule

Two changes are **reviewed changes, never side effects**:

- **Removing a `#![forbid(unsafe_code)]`** from a crate. The seal is the ratchet; a crate that needs
  `unsafe` again is a design change and gets its own review, with the reason.
- **Adding a `kind = "perf"` entry to the allowlist.** The gate rejects `perf` entries outright, so
  this is a change to the gate as well as to the allowlist. It is not a way to unblock a PR.

Correspondingly, deleting an allowlist entry in the same commit as the conversion that removes the site
is routine and needs no separate review — that direction always shrinks the set.

## 5. Inventory

Measured on `main` at `de3eeb2c`, by `scripts/ci-gates/guard-unsafe.py` scanning `*/src/**/*.rs` and
excluding `#[cfg(test)]` bodies — inline modules, `mod x;` declarations resolved transitively, and
`#[cfg(test)] impl`/`fn`, which the older scanner in `tests/ci/test_unsafe_ledger.py` did not
recognise.

**59 production sites in 16 files across 6 crates**, plus one test-only site in `ferrosa-schema` that
the ledger previously counted as production (60). That site is `env::remove_var` inside a
`#[cfg(test)] impl Schema`, so the corrected count is 59; the ledger and the scanner were fixed
together.

That was the measurement on `main`. On `policy/unsafe-discipline` (2026-11-05) the count is **64
production sites in 16 files across 6 crates**, and the difference is the two conversions described in
§6: the commit log's buffer lost its ten `perf` sites and gained sixteen `sync-primitive` ones (a
smaller, range-scoped primitive where there were ten whole-buffer derefs), and `buffer_bytes` lost its
`unsafe` entirely; `ferrosa-sstable`'s `KernelStatx::zeroed` lost its `unsafe` the same way. `perf` is
now zero tree-wide.

| File | Sites | Kinds |
| --- | --- | --- |
| `ferrosa-storage/src/commitlog/segment.rs` | 17 | `sync-primitive` 16, `syscall` 1 |
| `ferrosa-udf/src/asc.rs` | 10 | `unknown` 5, `ffi` 5 |
| `ferrosa/src/main.rs` | 5 | `allocator` 5 |
| `ferrosa-sstable/src/pump.rs` | 5 | `syscall` 3, `unknown` 2 |
| `ferrosa/src/jemalloc_tuning.rs` | 4 | `ffi` 4 |
| `ferrosa-storage/src/store.rs` | 4 | `unknown` 4 |
| `ferrosa-cql/src/prometheus.rs` | 3 | `syscall` 3 |
| `ferrosa-sstable/src/direct.rs` | 3 | `syscall` 3 |
| `ferrosa-storage/src/range_merger.rs` | 3 | `unknown` 3 |
| `ferrosa-loadgen/src/resource_monitor.rs` | 2 | `syscall` 2 |
| `ferrosa-sstable/src/dio_align.rs` | 1 | `syscall` 1 |
| `ferrosa-storage/src/flush.rs` | 2 | `mmap` 1, `syscall` 1 |
| `ferrosa-storage/src/write_set_spill.rs` | 2 | `mmap` 2 |
| `ferrosa-sstable/src/io.rs` | 1 | `mmap` 1 |
| `ferrosa-storage/src/index/sidecar.rs` | 1 | `mmap` 1 |
| `ferrosa-storage/src/write_set_stage.rs` | 1 | `mmap` 1 |

Totals by kind:

| Kind | Sites |
| --- | --- |
| `sync-primitive` | 16 |
| `syscall` | 14 |
| `unknown` | 14 |
| `ffi` | 9 |
| `mmap` | 6 |
| `allocator` | 5 |
| `layout` | 0 |
| `perf` | 0 |
| `ioctl` | 0 |
| **Total** | **64** |

Fourteen of the 64 are findings, all `unknown` — the lifetime-convenience family in §6. Every one is
recorded in `scripts/ci-gates/unsafe-allow.toml` under `[[debt]]` with the safe alternative that
removes it. The ten `perf` findings that were here are gone: they were the commit log's buffer, and
§6 records what replaced them.

## 6. Findings

Ranked by blast radius — how much of the system depends on the site being reachable and hot.

### `perf` — prohibited, and now zero

The ten `perf` findings were all in `ferrosa-storage/src/commitlog/segment.rs`: the segment buffer was
an `UnsafeCell<Vec<u8>>` entered as a whole, `&mut *self.buffer.get()`, by every writer. That is the
commit log's append path, so it could not be fixed by adding a lock without a decision; it was fixed by
replacing the buffer with a primitive that only ever hands out the range a writer owns.

**What the code was, and why it was undefined behaviour.** The CAS on `position` gives each writer an
exclusive byte range `[off, off+total_size)`. That is true of the *bytes*. It was not true of the
*references*: `&mut *self.buffer.get()` produced a `&mut Vec<u8>` covering the **entire** buffer, so two
concurrent writers held two overlapping `&mut` to one allocation. Disjoint byte ranges do not make
overlapping `&mut` sound under Stacked or Tree Borrows, and Miri reports it the moment a test drives two
writers (`Data race detected between (1) retag write … and (2) retag write of type std::vec::Vec<u8>` at
the `&mut *self.buffer.get()` line).

**What replaced it.** `SegmentBuffer` is a small type in the same file with three `unsafe fn`s and one
rule: a reference always covers exactly one reservation. `range_mut(off, len)` builds
`slice::from_raw_parts_mut(base.add(off), len)` for the bytes the caller owns; `prefix(end)` builds the
`&[u8]` a flusher can legally read; `release()` frees the allocation under the same lock every flusher
takes. Alongside them, the length became atomic:

- `written: AtomicUsize` is published **`Release`** by a writer after the last byte of its range, and
  loaded **`Acquire`** by a flusher. The pair, rather than `SeqCst`, is what the guarantee needs: the
  flusher must see the bytes if it sees the end offset, and nothing else is shared. A flush now writes
  `[0, min(snapshot, written))`, so a reservation whose bytes do not exist yet is never published and
  never advances the watermark past itself.
- `len: AtomicUsize` carries the allocated length, so `buffer_bytes()` no longer reads the `Vec` header
  while writers hold `&mut` into the same allocation. That site had no `unsafe` left to allow.

The one writer that reaches *backwards* — the marker link patch, which writes eight bytes below the
flush watermark — now takes `file_handle`, the same lock a flusher holds while it reads. That is the
only new lock in the path, and it costs nothing on append: `allocate_and_link_sync_marker` already held
`marker_link`, and concurrent `force_sync` callers still fsync in parallel.

**Evidence.** Two tests, both in the module:

- `concurrent_writers_take_disjoint_ranges_and_lose_no_bytes` — eight writers, all eight reservations
  taken behind a barrier before any write begins, then compared byte-for-byte against the same writes
  done one at a time. It is RED on the parent commit under Miri and green after, and the on-disk half of
  the assertion runs on a real target (Miri has no `fdatasync`).
- `a_flush_between_a_reservation_and_its_write_must_not_publish_unwritten_bytes` — a flush between
  `allocate()` and `write_entry()` used to write zeros over the range and then move the watermark past
  it, so the entry never reached disk. RED in plain `cargo test` before the atomic length, green after.

**The follow-up this does not do.** Per-writer shards would let each writer own its buffer outright and
remove the raw pointer and both `unsafe impl`s with it. That redesigns allocation, the flush watermark
and the marker chain together, so it is named in `SegmentBuffer`'s documentation rather than folded in
here.

### `unknown` — not permitted

| # | Site | Construct | Safe alternative |
| --- | --- | --- | --- |
| 5 | `store.rs:2122,2124,2126,2129` | `&'static` laundering: `&*Arc::as_ptr(&view)` and three `&*(&*x as *const [T])` reborrows, so a paused scan's `OwnedMerger` owns its sources and lends `'static`. | Make `RangeMerger` own its inputs (`Arc<StoreView>`, `Box<[Arc<SSTableReader<R>>]>`) instead of borrowing them, or wrap the pair in `self_cell`/`ouroboros`, where the lifetime is derived and checked rather than asserted. |
| 6 | `range_merger.rs:1450,1565,1577` | `Box::leak` + `&*runs_ptr` to extend a `Box<[...]>` to `'a`, a manual `unsafe impl Send`, and `Box::from_raw` in `Drop`. | Store the `Box<[Vec<..>]>` in the struct and hand out indices, not references; or `Arc<[Vec<..>]>`, which is `Send + Sync` for free and removes the manual `Send` and the `Drop` impl. |
| 7 | `pump.rs:448` | `slice::from_raw_parts(self.ptr, self.len)` — a raw pointer walk over buffers owned by the flusher batch. | Make `PumpBuffer<'a>` hold `&'a [u8]` and let `as_slice` be `self.bytes`. The batch already outlives the call; the raw pointer only exists because the struct erased the borrow. |
| 8 | `pump.rs:529` | `unsafe impl Send for FileSink`, justified by "the `Vec`'s own allocation moves, not the data the stale pointers pointed to". | Goes away with 7: once `FileSink` holds no stale raw pointers, `Send` is automatic. |
| 9 | `udf/asc.rs:234,235` | `unsafe impl Send + Sync for CtxPtr(NonNull<JSContext>)` — declared so wasmtime's host-closure bound is met, while the type's own comment says single-threaded use only. The impl contradicts the documented usage. | Keep the context in a `thread_local!` keyed by the calling store; nothing `Send`-required then holds the pointer. |
| 10 | `udf/asc.rs:252,253` | `unsafe impl Send + Sync for SendActive(Rc<ActiveCtx>)` — laundering an `Rc` across the same bound. std has `Arc` and `Mutex`, so this is convenience, not a missing primitive. | `thread_local!` for the active-context cell, reached by key from the host closure. |
| 11 | `udf/asc.rs:299` | `&mut *active` reinterpreted as `&mut StoreContextMut<'static, ShimData>`. | Same thread-local as 10; the push/pop discipline becomes a scoped borrow instead of a `'static` assertion. |

Sites 7–11 are ranked below 5–6 because they are on the SSTable-flush and UDF-compile paths, not the
per-append path.

### Structural symptoms attached to otherwise-permitted sites

These are `syscall`/`ffi`/`layout` sites whose *kind* is permitted but whose shape is removable. They
are listed so the follow-up does not treat "the kind is allowed" as "the site is fine":

- **`mem::zeroed()` out-parameters** — `loadgen/src/resource_monitor.rs:442,468`. `ferrosa-sstable`'s
  `dio_align.rs:129` no longer has one: the kernel's `statx` buffer is a field-by-field zero literal, so
  the compiler proves it instead of an `unsafe` assertion. `resource_monitor.rs:442` (`getrlimit`) now
  uses `MaybeUninit` and reads it only inside the `== 0` arm. `resource_monitor.rs:468` (`task_info`,
  macOS-only) is the remaining one: it cannot be compiled on a Linux host, so it is left for a change
  made and tested on a Mac rather than edited blind.
- **`unsafe fn` that should be a safe fn around one block** — `ferrosa/src/jemalloc_tuning.rs:68`
  makes `mallctl_write_ssize` `unsafe fn`, which forces every caller to add its own `unsafe` block
  (`:109`). Nine sites become four.
- **`assume_init` that is already correct** — `ferrosa-cql/src/prometheus.rs:380,383` is the pattern
  the other two should copy: the `assume_init` is inside the `rc == 0` arm.

## 7. Sealed crates

A crate with zero production sites carries `#![forbid(unsafe_code)]`, so a later addition is a compile
error instead of a code-review omission. An existing `forbid` is never removed as a side effect of
anything (§4).

Sealed (21): `ferrosa-cdc`, `ferrosa-cluster`, `ferrosa-common`, `ferrosa-ctl`, `ferrosa-flight`,
`ferrosa-graph`, `ferrosa-index`, `ferrosa-index-builder`, `ferrosa-jsonb`, `ferrosa-net`,
`ferrosa-row-bridge`, `ferrosa-sched`, `ferrosa-session`, `ferrosa-sim`, `ferrosa-sparql`,
`ferrosa-sql`, `ferrosa-view`, `ferrosa-worker`, `red-ledger`, `xtask`, `tools/alloc-count`.

Sealing `red-ledger` and `xtask` on this branch is new: both had zero `unsafe` anywhere in `src/`.
Every other sealed crate already carried the attribute; none was removed.

Not sealed, and why:

- `ferrosa-storage` (30), `ferrosa-sstable` (10), `ferrosa-udf` (10), `ferrosa` (9), `ferrosa-cql` (3),
  `ferrosa-loadgen` (2) — at least one production site (§5).
- **`ferrosa-postgres`, `ferrosa-jepsen`, `ferrosa-schema` have zero production sites and still cannot
  be sealed.** `#![forbid(unsafe_code)]` covers `#[cfg(test)]` code too, and each reaches for the
  platform from a test: an allocation-counting `GlobalAlloc` in `ferrosa-postgres/src/mvcc.rs`,
  `env::set_var` in `ferrosa-jepsen/src/test_env.rs`, `env::remove_var` in
  `ferrosa-schema/src/registry.rs`. Each is one test-helper away from being sealable, and that is the
  cheapest remaining ratchet in the tree. `ferrosa-schema`'s site is the one the ledger used to count
  as production.
- `ferrosa-jsonb-conformance` is a separate workspace with no `src/` — only `tests/` — so there is no
  crate root to seal.

## 8. The gate

```
python3 scripts/ci-gates/guard-unsafe.py            # exit 0 clean, 1 violation, 2 cannot scan
```

CI runs it in the `unsafe-allowlist` job, alongside its own unit tests. The allowlist has two tables:
`[[allow]]` for permitted sites, and `[[debt]]` for the recorded findings — `perf` and `unknown` sites
that exist today, each with the `fix` that removes it. Keeping them apart is what makes the rule
enforceable while the tree still contains findings: a `perf` site cannot be admitted as *permitted*, it
can only be admitted as *debt*, and adding a debt entry is a reviewed change (§4).

It fails on:

| Failure | Meaning |
| --- | --- |
| `unlisted` | `unsafe` present in production code with neither an `[[allow]]` nor a `[[debt]]` entry. |
| `stale` | An entry whose site no longer exists — rot in the other direction. Delete the entry in the commit that removes the site. |
| `duplicate` | More entries than sites for one `(file, item)`. |
| `perf` / `unknown` in `[[allow]]` | The entry's kind is a finding, not a permitted kind. Move it to `[[debt]]`. |
| `not-a-finding` | A `[[debt]]` entry whose kind is a permitted one — a permitted site belongs in `[[allow]]`. |
| `missing-field` | An entry without `file`, `item`, `kind`, `reason`, `owner`, `since`; a `[[debt]]` entry also needs `fix`. |
| `no-safety-comment` | A permitted site with no `// SAFETY:` comment above it. |
| `weak-safety-comment` | The comment exists but names no mechanism. Heuristic: at least 40 characters and at least one term from a mechanism vocabulary (`because`, `only if`, `guarded`, `initializes`, `outlives`, `immutable`, `renamed`, `checked`, `exclusive`, `declared before`, `never`, `for the duration`, `owns`, `scoped`, `cannot`, `lifetime`, `synchronized`, `atomic`, `valid`, …). A comment that only restates the call fails. The gate prints the vocabulary; it is a floor, not a substitute for §3's test. |
| `sealed` | A production site in a crate carrying `#![forbid(unsafe_code)]`. |

`item` is the enclosing `fn`/`impl` name with its signature and generics stripped, not a line number,
so entries survive a parameter being added.
`[[debt]]` entries are exempt from the comment check on purpose: a finding is a site whose justification
is absent or invalid, so requiring the comment it lacks would make the two classes contradict each
other. The debt entry's own `reason` and `fix` stand in its place.

Every class above has a RED test in `tests/ci/test_guard_unsafe.py`, and each test was mutation-checked:
the corresponding check was disabled and the test was confirmed to fail.

## 9. Adding a site

1. Write the mechanism sentence in a `// SAFETY:` comment above the block, in the shape §3 asks for.
2. Add an `[[allow]]` entry with `file`, `item`, `kind`, `reason`, `owner`, `since`.
3. If the kind would be `perf` or `unknown`, stop — the gate and the reviewer will both reject it as a
   permitted site. Rewrite the construct safely, or bring it to the operator with the benchmark. If it
   must land anyway, it goes in `[[debt]]` with a `fix`, and that is a reviewed change (§4).
4. If the crate is sealed, do not remove the seal. That is §4.
5. When a site is converted, delete its entry in the **same commit**. `stale` is a failure, because a
   stale allowlist is how this kind of gate rots.
