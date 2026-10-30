# Unsafe ledger

Every `unsafe` block, `unsafe fn`, `unsafe impl` and `unsafe extern` in Ferrosa's production code is
listed here, with the invariant it is relying on and whether that invariant has been audited by hand.
`unsafe` is the one part of the codebase that no proof, no model and no test can cover for us: the
Lean corpus reasons about a specified machine, Aeneas explicitly cannot translate `unsafe`, and Miri
only sees the paths a test happens to execute. So the only defence is that the set is small, known,
and reviewed.

**The ledger is enforced.** `tests/ci/test_unsafe_ledger.py` scans `*/src/**/*.rs` for unsafe
constructs outside `#[cfg(test)]` modules and fails the build when the per-file counts drift from the
table below. Adding an `unsafe` site requires adding a row — the count is the ratchet.

## Policy

1. **No new `unsafe` without a ledger row.** The CI check fails on any count increase.
2. **Every site carries a `// SAFETY:` comment naming the invariant**, not the operation. "It's fine,
   the pointer is valid" is not an invariant; "the mapping outlives every slice derived from it
   because `self.map` is declared before `self.slice` and neither is moved" is.
3. **Prefer removing the need.** Four of the seven mmap sites exist to read a file that an
   `Arc<Vec<u8>>` would also read; mmap buys latency, at the cost of a SIGBUS contract. Each one has to
   justify itself against the safe alternative.
4. **`unsafe` lives in the smallest possible surface.** A crate that can be sealed is sealed:
   `#![forbid(unsafe_code)]` goes on every crate with no unsafe in `src/`, so a later addition is a
   compile error rather than a code review someone skips.
5. **Test-only unsafe is not exempt from review, only from this count.** It is enumerated separately
   below, because a test that mutates the environment unsoundly can also produce a false green.

## Inventory

| Scope | Sites |
| --- | --- |
| Production code (`src/`, outside `#[cfg(test)]`) | **65** |
| `#[cfg(test)]` modules inside `src/` | 65 |
| `tests/` and `examples/` | 299 |
| WASM example bindings (`examples/**/src/`) | 58 |

Production sites by crate:

| File | Sites | Category | Invariant this site relies on | Status |
| --- | --- | --- | --- | --- |
| `ferrosa-storage/src/commitlog/segment.rs` | 11 | `UnsafeCell` shared buffer, manual `Send`/`Sync` | one writer at a time by construction; buffer is not aliased across the FFI boundary | queued |
| `ferrosa-udf/src/asc.rs` | 10 | FFI to the JS engine (`rquickjs`) | `CtxPtr`/`SendActive` outlive every use; raw handles come from a live context | queued |
| `ferrosa-sstable/src/direct.rs` | 8 | custom aligned allocator, `O_DIRECT`/`F_NOCACHE` fcntl | `alloc`/`dealloc` receive the same `Layout`; the fd is valid for the call | queued |
| `ferrosa-sstable/src/pump.rs` | 5 | `preadv`/`pwritev` scatter-gather, raw slice | `iov_base`/`iov_len` describe the same allocation for the whole call | queued |
| `ferrosa/src/main.rs` | 5 | `GlobalAlloc` implementation over jemalloc | every method forwards the layout unchanged; no double free | queued |
| `ferrosa/src/jemalloc_tuning.rs` | 4 | `mallctl` FFI | the `name` is NUL-terminated and the out-param matches the type jemalloc expects | queued |
| `ferrosa-storage/src/store.rs` | 4 | `&'static` lifetime laundering from `Arc::as_ptr` | the `Arc` guard outlives every reference derived from it | queued |
| `ferrosa-cql/src/prometheus.rs` | 3 | `libc::getrlimit`/`sysconf` | the return code is checked before the out-param is read | queued |
| `ferrosa-storage/src/range_merger.rs` | 3 | self-referential borrow across an await | the owner is pinned and outlives the stream | queued |
| `ferrosa-loadgen/src/resource_monitor.rs` | 2 | `libc` resource query | return code checked | queued |
| `ferrosa-sstable/src/dio_align.rs` | 2 | `mem::zeroed`, `posix_memalign`-style alignment | the zeroed type has no invalid all-zero bit pattern | queued |
| `ferrosa-storage/src/write_set_spill.rs` | 2 | `memmap2::Mmap` | the file is never truncated or written while mapped | audited |
| `ferrosa-storage/src/flush.rs` | 2 | `memmap2::Mmap`, `posix_fadvise` | same, plus the fd is valid for the call | mapping audited, `fadvise` queued |
| `ferrosa-schema/src/registry.rs` | 1 | `libc` | return code checked | queued |
| `ferrosa-sstable/src/io.rs` | 1 | `memmap2::Mmap` | the file is never truncated or written while mapped | audited |
| `ferrosa-storage/src/index/sidecar.rs` | 1 | `memmap2::Mmap` | same | audited |
| `ferrosa-storage/src/write_set_stage.rs` | 1 | `memmap2::Mmap` | same | audited |

**Five of the seven mappings are audited; nothing else is.** The table is the queue, not a
certificate; `Status` moves to `audited` only when a reviewer has checked the invariant against the
call sites and recorded what they checked — the three artifacts for each are in *The mapping
contract* below. 33 of the 188 sites in `src/` carry a `SAFETY:` comment today, so most rows also need
the comment written before they can be audited at all.

## The mapping contract

`memmap2::Mmap::map` is `unsafe` for exactly one reason: if the file shrinks underneath the mapping,
touching the missing pages faults (`SIGBUS`). Everything else about a mapping is safe. So the whole
obligation is one sentence — **nothing truncates or rewrites a mapped file in place** — and it is
discharged three ways:

The sites split into two mechanisms, and conflating them would be the error:

1. **Published files: written to a temp name, `rename`d into place.** Sidecars (`sidecar.rs:231`) and
   SSTable components are never written in place. `rename` swaps the directory entry and leaves the
   old inode intact, so a reader holding the previous version keeps reading the previous bytes. Test:
   `rewriting_a_sidecar_leaves_a_mapped_reader_intact` (`index/sidecar.rs`).
2. **Private files: written, closed, then mapped, with no surviving writable handle.** A spilled write
   set lives in a fresh directory from `reserve_write_set_stage`; the writer is dropped and the file
   reopened read-only *before* `Mmap::map`, and no other handle to it exists. There is no `rename` in
   these two files, so the guarantee is exclusivity rather than atomic publication. Tests:
   `mmap_entry_reads_identical_bytes_to_the_owned_mutation_path` and
   `mmap_entry_is_a_stable_slice_of_the_mapping` (`write_set_spill.rs`).
3. **Unlink only after the last reader drops.** GC and compaction delete whole files, which removes
   the name but not the inode while it is mapped. Test:
   `a_mapped_index_survives_replacement_and_removal` (`sstable/io.rs`) replaces a mapped component by
   rename and then deletes it; the open reader returns the original bytes after both.
4. **No in-place truncation anywhere.** Checked against every `set_len` and `truncate(true)` call site
   in `ferrosa-storage` and `ferrosa-sstable`: `engine.rs` (4), `schema_snapshot.rs`, `store.rs`,
   `eviction_audit.rs` (`CURRENT` pointer), `count.rs`, `upload/download.rs` (download target). Every
   one sizes a file that is never mapped — none names a component, sidecar or spill file.

**Residual, not covered by any of the above:** another process truncating one of these files. Nothing
in Rust can prevent that, and the failure is a fault rather than an error — the honest mitigation is
that these are files only this server writes.

**Why they were not removed.** Each has a safe replacement — read the bytes into a `Vec` — and each
loses the property the mapping exists for: the kernel can evict mapped pages under memory pressure,
whereas a `Vec` is resident until dropped. On a table with many open components that is unbounded RSS,
so removal here trades a contract that holds by construction for a memory regression. The sites that
*were* removable for free — the hand-rolled aligned allocator, which is neither reclaimable nor
cheap — are removed instead (`direct.rs`, 8 -> 3).

## Audit order

Not all 65 sites are equally dangerous. Ranked by the consequences of getting the invariant wrong:

1. **`store.rs:2122` — `&'static StoreView` from `Arc::as_ptr`.** A fabricated `'static` is the exact
   class of bug that produces a use-after-free the borrow checker cannot see. The question to answer:
   what keeps the `Arc` alive for the whole life of `view_ref`, and can that ever be a read guard that
   is released while a reference is still held?
2. **The seven `memmap2::Mmap` sites.** A mapping is UB if the file is truncated underneath it (SIGBUS
   at minimum, torn data at worst). Every one of these needs the truncation contract written down and
   checked against the rename/GC paths, which is the same question ADR-022 asks about `fnf` segment
   headers.
3. **`segment.rs` — `UnsafeCell` buffer with hand-written `Send`/`Sync`.** `unsafe impl Send`/`Sync` is
   an assertion that the type's interior mutability is properly synchronised; if it is not, the failure
   is a data race, which is the one bug class the compiler otherwise rules out.
4. **`direct.rs` — the aligned allocator.** `alloc`/`dealloc` with mismatched layouts is heap
   corruption; the only defence is that both call sites derive the layout the same way.
5. Everything else — `libc` calls with checked return codes, `GlobalAlloc` forwarding, `mallctl`.

## Sealed crates

These crates have no `unsafe` in `src/` and carry `#![forbid(unsafe_code)]`, so any addition is a
compile error:

`ferrosa-cdc`, `ferrosa-cluster`, `ferrosa-common`, `ferrosa-ctl`, `ferrosa-flight`, `ferrosa-graph`,
`ferrosa-index`, `ferrosa-index-builder`, `ferrosa-jsonb`, `ferrosa-jsonb-conformance`, `ferrosa-net`,
`ferrosa-row-bridge`, `ferrosa-sched`, `ferrosa-session`, `ferrosa-sim`, `ferrosa-sparql`,
`ferrosa-sql`, `ferrosa-view`, `ferrosa-worker`.

Not sealed, and why: `ferrosa-storage`, `ferrosa-sstable`, `ferrosa-udf`, `ferrosa-cql`,
`ferrosa-schema`, `ferrosa-loadgen`, `ferrosa-jepsen`, and the `ferrosa` binary all contain at least one
production site. Tooling crates (`fuzz`, `xtask`, `sstable-compat`) are unsealed by omission rather
than by argument.

## Adding a site

1. Can it be removed? Check the safe alternative first — most `libc` sites have one, and it is usually
   slower by microseconds and correct by construction.
2. If not, write the `// SAFETY:` comment as an invariant.
3. Add or bump the row in the table.
4. If the crate is sealed, removing `#![forbid(unsafe_code)]` is a separate, reviewed change — that is
   the point of sealing it.
