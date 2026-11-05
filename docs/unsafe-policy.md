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
excluding `#[cfg(test)]` modules — inline and `mod x;` declarations, transitively.

**60 sites in 17 files across 7 crates.** One of the 60 (`ferrosa-schema/src/registry.rs`, an
`env::remove_var` inside a `#[cfg(test)] impl`) sits in a test-only `impl` block that the older
scanner in `tests/ci/test_unsafe_ledger.py` does not recognise, because it only looks for
`#[cfg(test)] mod`. The true production count is **59**; the inventory slice of this branch corrects
the ledger and the scanner together.

| File | Sites | Kinds |
| --- | --- | --- |
| `ferrosa-storage/src/commitlog/segment.rs` | 11 | `perf` 10, `syscall` 1 |
| `ferrosa-udf/src/asc.rs` | 10 | `ffi` 7, `unknown` 3 |
| `ferrosa/src/main.rs` | 5 | `allocator` 5 |
| `ferrosa-sstable/src/pump.rs` | 5 | `syscall` 3, `unknown` 2 |
| `ferrosa/src/jemalloc_tuning.rs` | 4 | `ffi` 4 |
| `ferrosa-storage/src/store.rs` | 4 | `unknown` 4 |
| `ferrosa-cql/src/prometheus.rs` | 3 | `syscall` 3 |
| `ferrosa-sstable/src/direct.rs` | 3 | `syscall` 3 |
| `ferrosa-storage/src/range_merger.rs` | 3 | `unknown` 3 |
| `ferrosa-loadgen/src/resource_monitor.rs` | 2 | `syscall` 2 |
| `ferrosa-sstable/src/dio_align.rs` | 2 | `layout` 1, `syscall` 1 |
| `ferrosa-storage/src/flush.rs` | 2 | `mmap` 1, `syscall` 1 |
| `ferrosa-storage/src/write_set_spill.rs` | 2 | `mmap` 2 |
| `ferrosa-schema/src/registry.rs` | 1 | `syscall` 1 (test-only) |
| `ferrosa-sstable/src/io.rs` | 1 | `mmap` 1 |
| `ferrosa-storage/src/index/sidecar.rs` | 1 | `mmap` 1 |
| `ferrosa-storage/src/write_set_stage.rs` | 1 | `mmap` 1 |

Totals by kind:

| Kind | Sites |
| --- | --- |
| `syscall` | 15 |
| `unknown` | 12 |
| `ffi` | 11 |
| `perf` | 10 |
| `mmap` | 6 |
| `allocator` | 5 |
| `layout` | 1 |
| `sync-primitive` | 0 |
| `ioctl` | 0 |
| **Total** | **60** |

## 6. Findings

Ranked by blast radius — how much of the system depends on the site being reachable and hot.

### `perf` — prohibited

| # | Site | Why it is `perf` | Safe alternative |
| --- | --- | --- | --- |
| 1 | `commitlog/segment.rs:562,608,648,724,1205` — `&mut *self.buffer.get()` | The commit log is on every write. A `Mutex` around the append buffer is the safe form; these derefs exist to let concurrent writers append without one. | `Mutex<Vec<u8>>` around allocation and copy; or shard the buffer per writer so each shard is owned, not interior-mutable. **Also unsound today** — see below. |
| 2 | `commitlog/segment.rs:807,977` — `&*self.buffer.get()` | Same buffer, read side, for the flush. | Read through the same `Mutex`/shard ownership; `wait_for_writers()` already orders it, so the lock is not on the hot path. |
| 3 | `commitlog/segment.rs:1171` — `unsafe { &*self.buffer.get() }.len()` | Reads one `usize` to avoid taking a lock. | `AtomicUsize` updated on append and release. Trivially safe, and the cheapest fix in this list. |
| 4 | `commitlog/segment.rs:393,394` — `unsafe impl Send/Sync for Segment` | The enabling declaration for the buffer: it exists so a `Segment` can be shared across writer threads without a lock. | Falls out for free once the buffer is a `Mutex`/owned shards. |

**The claim at `segment.rs:562` does not hold.** The SAFETY comment says the CAS allocation gives the
writer exclusive ownership of `[off, off+total_size)`. That is true of the *bytes*, and it is not what
the code does: `&mut *self.buffer.get()` produces a `&mut Vec<u8>` covering the **entire** buffer, so
two concurrent writers hold two overlapping `&mut` to the same allocation. Disjoint byte ranges do not
make overlapping `&mut` sound under Stacked or Tree Borrows, and Miri reports it as such the moment a
test drives two writers. This is the highest-priority finding on this branch and the first crate the
follow-up should take.

The minimal sound fix when the lock-free design is kept: never form a `&mut Vec<u8>`. Keep the
`UnsafeCell`, take the base pointer once, and scope the reference to the owned range —
`slice::from_raw_parts_mut(base.add(off), total_size)` — which is the standard journaling pattern and
is what the comment already claims. That is still `unsafe`, so it is still a `perf` finding against
the invariant; it just stops being UB.

### `unknown` — not permitted

| # | Site | Construct | Safe alternative |
| --- | --- | --- | --- |
| 5 | `store.rs:2122,2124,2126,2129` | `&'static` laundering: `&*Arc::as_ptr(&view)` and three `&*(&*x as *const [T])` reborrows, so a paused scan's `OwnedMerger` owns its sources and lends `'static`. | Make `RangeMerger` own its inputs (`Arc<StoreView>`, `Box<[Arc<SSTableReader<R>>]>`) instead of borrowing them, or wrap the pair in `self_cell`/`ouroboros`, where the lifetime is derived and checked rather than asserted. |
| 6 | `range_merger.rs:1450,1565,1577` | `Box::leak` + `&*runs_ptr` to extend a `Box<[...]>` to `'a`, a manual `unsafe impl Send`, and `Box::from_raw` in `Drop`. | Store the `Box<[Vec<..>]>` in the struct and hand out indices, not references; or `Arc<[Vec<..>]>`, which is `Send + Sync` for free and removes the manual `Send` and the `Drop` impl. |
| 7 | `pump.rs:448` | `slice::from_raw_parts(self.ptr, self.len)` — a raw pointer walk over buffers owned by the flusher batch. | Make `PumpBuffer<'a>` hold `&'a [u8]` and let `as_slice` be `self.bytes`. The batch already outlives the call; the raw pointer only exists because the struct erased the borrow. |
| 8 | `pump.rs:529` | `unsafe impl Send for FileSink`, justified by "the `Vec`'s own allocation moves, not the data the stale pointers pointed to". | Goes away with 7: once `FileSink` holds no stale raw pointers, `Send` is automatic. |
| 9 | `udf/asc.rs:252,253` | `unsafe impl Send + Sync for SendActive(Rc<ActiveCtx>)` — laundering an `Rc` across wasmtime's `Send + Sync` host-callback bound. | Keep the active-context cell in a `thread_local!` and reach it from the host closure by key; nothing then crosses a thread boundary and no impl is needed. |
| 10 | `udf/asc.rs:299` | `&mut *active` reinterpreted as `&mut StoreContextMut<'static, ShimData>`. | Same thread-local as 9; the push/pop discipline becomes a scoped borrow instead of a `'static` assertion. |

Sites 7–10 are ranked below 5–6 because they are on the SSTable-flush and UDF-compile paths, not the
per-append path.

### Structural symptoms attached to otherwise-permitted sites

These are `syscall`/`ffi`/`layout` sites whose *kind* is permitted but whose shape is removable. They
are listed so the follow-up does not treat "the kind is allowed" as "the site is fine":

- **`mem::zeroed()` out-parameters** — `loadgen/src/resource_monitor.rs:438,461` and
  `sstable/src/dio_align.rs:129`. Prefer `MaybeUninit`, scope it so the return code is checked before
  `assume_init`. At `resource_monitor.rs:438` and `:461` the SAFETY comment is *"getrlimit is a
  standard POSIX call"* / *"task_info is a standard mach system call"* — that names the operation, not
  the mechanism, and would fail §3.
- **`unsafe fn` that should be a safe fn around one block** — `ferrosa/src/jemalloc_tuning.rs:68`
  makes `mallctl_write_ssize` `unsafe fn`, which forces every caller to add its own `unsafe` block
  (`:109`). Nine sites become four.
- **`assume_init` that is already correct** — `ferrosa-cql/src/prometheus.rs:380,383` is the pattern
  the other two should copy: the `assume_init` is inside the `rc == 0` arm.

## 7. Sealed crates

A crate with zero production sites carries `#![forbid(unsafe_code)]`, so a later addition is a compile
error instead of a code-review omission. An existing `forbid` is never removed as a side effect of
anything (§4).

Sealed (24): `ferrosa-cdc`, `ferrosa-cluster`, `ferrosa-common`, `ferrosa-ctl`, `ferrosa-flight`,
`ferrosa-graph`, `ferrosa-index`, `ferrosa-index-builder`, `ferrosa-jsonb`, `ferrosa-jsonb-conformance`,
`ferrosa-net`, `ferrosa-postgres`, `ferrosa-row-bridge`, `ferrosa-sched`, `ferrosa-session`,
`ferrosa-sim`, `ferrosa-sparql`, `ferrosa-sql`, `ferrosa-view`, `ferrosa-worker`, `red-ledger`,
`xtask`, `tools/alloc-count`, `ferrosa-jepsen`.

Not sealed, and why:

- `ferrosa-storage`, `ferrosa-sstable`, `ferrosa-udf`, `ferrosa-cql`, `ferrosa-loadgen`, `ferrosa` —
  at least one production site (§5).
- `ferrosa-schema` — its only site is `registry.rs:2078`, inside `#[cfg(test)] impl Schema`.
  `#![forbid(unsafe_code)]` covers test code too, so the crate cannot be sealed until that line is
  gone; the fix is to stop reading the environment in a test helper, not to allow the unsafe.

## 8. The gate

```
python3 scripts/ci-gates/guard-unsafe.py            # exit 0 clean, 1 violation, 2 cannot scan
```

CI runs it in the `unsafe-allowlist` job, alongside its own unit tests. It fails on:

| Failure | Meaning |
| --- | --- |
| `unlisted` | `unsafe` present in production code with no `[[allow]]` entry. |
| `stale` | An entry whose site no longer exists — rot in the other direction. Delete the entry in the commit that removes the site. |
| `perf` / `unknown` | The entry's kind is a finding, not a category. |
| `missing-field` | An entry without `file`, `item`, `kind`, `reason`, `owner`, `since`. |
| `no-safety-comment` | A listed site with no `// SAFETY:` comment above it. |
| `weak-safety-comment` | The comment exists but names no mechanism. Heuristic: it must contain one of a small set of mechanism words (`because`, `only if`, `guarded`, `initializes`, `outlives`, `immutable`, `renamed`, `checked`, `exclusive`, `declared before`, `never`) **and** be at least 40 characters. Short or purely descriptive comments fail. |
| `sealed` | A production site in a crate carrying `#![forbid(unsafe_code)]`. |

`item` is the enclosing `fn`/`impl` header, not a line number, so entries survive code moving.

## 9. Adding a site

1. Write the mechanism sentence in a `// SAFETY:` comment above the block, in the shape §3 asks for.
2. Add a `[[allow]]` entry with `file`, `item`, `kind`, `reason`, `owner`, `since`.
3. If `kind` is `perf` or `unknown`, stop — the gate and the reviewer will both reject it. Rewrite the
   construct safely, or bring it to the operator with the benchmark.
4. If the crate is sealed, do not remove the seal. That is §4.
