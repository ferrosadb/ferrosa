# Proving the unsafe: what the IR can and cannot give us

Companion to [`unsafe-ledger.md`](unsafe-ledger.md). That file says *what we have*; this one says *what
each item needs to be discharged, and with what*.

## The short answer

**Aeneas cannot do it, and no available tool proves `unsafe` for all inputs.** Aeneas documents
`unsafe`, interior mutability, raw pointers, atomics, threads and FFI as unsupported — its whole
argument is that the borrow checker already ruled those hazards out, which is exactly what `unsafe`
undoes. The separation-logic extension that would lift this is in progress upstream, and
RefinedRust/RustBelt are research prototypes outside this toolchain.

What is reachable is a three-layer answer, and for most sites the honest ranking is *redesign first,
prove second*:

| Layer | Tool | What it establishes |
| --- | --- | --- |
| **Specification** | this repo's Lean corpus (`Ferrosa/Unsafe/Invariants.lean`) | the invariant, and that the *safe wrapper* is correct assuming it holds |
| **Discharge** | Kani (CBMC over the real MIR, bounded) | the invariant for all inputs up to a bound — raw pointers, allocation layouts, uninitialised memory |
| **Evidence** | Miri in CI | no UB on executed paths; a green run is never a proof |
| **Design** | restructuring | the invariant becomes a type, and there is nothing left to prove |

The reason Kani is the right discharge tool and not a substitute for Aeneas: it consumes the *same*
MIR rustc produces for the shipped build. It is not a translation with a fidelity gap — the gap is
boundedness, which is arguable, rather than semantics, which is not.

## What is already done

`specs/lean4/Ferrosa/Unsafe/Invariants.lean` — 10 theorems, all machine-checked, one per invariant
cluster. Each states the property the `unsafe` site asserts and the consequence for the wrapper:

| Cluster | Theorem | What it settles |
| --- | --- | --- |
| `commitlog/segment.rs` (11) | `two_mutable_borrows_cannot_coexist` | the `UnsafeCell` discipline admits no aliasing mutable borrow |
| | `a_shared_borrow_racing_a_mutation_is_the_promised_race` | what `unsafe impl Sync` actually denies |
| `store.rs` (4) | `a_reference_used_past_its_owner_is_invalid` | quantified over arbitrary lifetimes, so not a claim about one call site |
| | `a_static_lifetime_does_not_extend_the_region` | `&'static` is sound only while the owner lives — the audit question, stated |
| mmap (7) | `a_truncation_below_a_mapping_makes_the_tail_unreadable` | the invariant is a property of the *rename/GC* path, not of the mapping |
| | `a_slice_outliving_its_mapping_is_a_dangling_read` | slices must not outlive the `Mmap` |
| `direct.rs` (8) | `a_block_freed_with_a_different_layout_corrupts_the_heap` | layout agreement is the whole obligation |
| libc (6) | `a_failed_syscall_has_no_out_parameter` | why `assume_init` needs the return-code branch |
| markers (10) | `overlapping_writers_are_the_race` | what `Send`/`Sync` claims about access intervals |
| `asc.rs` (10) | `a_handle_after_its_context_is_dropped_is_dangling` | handle validity is tied to context liveness |

The remaining gap is not "what is the invariant" — it is "does this code establish it". That is the
part Kani answers for a bound, and the part review answers today.

## Per-cluster: prove it, or delete it

Ranked by `unsafe` sites removed per unit of work. The first two are not proofs; they are the reason
most of this list should shrink rather than be verified.

### 1. `store.rs:2122` — delete the `&'static` (4 sites)

```rust
let view_ref: &'static StoreView = unsafe { &*Arc::as_ptr(&view) };
```

Nothing here needs `'static`. A borrowed struct holding the `Arc` alongside the reference makes the
lifetime a type-level fact, and then the three sibling raw casts (`&*readers`, `&*mappings`,
`&*wanted`) are unnecessary because the `Arc` can be cloned into the struct. This is the highest-risk
site in the codebase *and* the easiest to remove. Kani would need a harness reasoning about drop
order, which is the weakest thing it does; deleting the site is stronger.

### 2. `direct.rs` — the aligned allocator (8 sites)

The obligation is `alloc`/`dealloc` layout agreement. Two routes: prove it with a Kani harness over
`size ∈ [1, 4096] × align ∈ {512, 4096}`, or replace the hand-rolled buffer with a `Vec<u8>` plus
`align_offset` on a `#[repr(align)]` wrapper. If `O_DIRECT` needs the pointer alignment the allocator
provides, a Kani harness is the answer and the bound is easy to justify: the allocator is called with
two layouts in the whole engine.

### 3. mmap — move the obligation into safe code (7 sites)

The mapping is sound iff the file is never truncated beneath it. That is a property of the rename and
GC paths, all safe code, so it can be modelled in Lean and checked by Kani without touching `unsafe` —
this is the cheapest open item in the plan, and it is also the one that has an operational answer:
`fnf` already renames rather than truncates (ADR-022), so the contract may already hold and only needs
stating. What cannot be proved by any tool: SIGBUS itself is an OS contract. The mapping must also
hold its own fd and be created with the file open and unlinked, or the contract is unenforceable.

### 4. `segment.rs` — the `UnsafeCell` buffer (11 sites)

`unsafe impl Send`/`Sync` on a type with `UnsafeCell` is the assertion the discipline theorem checks.
Kani has no concurrency model, so it cannot discharge this; Loom can test interleavings to a bound, and
`&mut`-only access would make the marker impls unnecessary. The real question for the audit is
narrow: which methods take the buffer through `&self` rather than `&mut self`? Those are the ones
`Sync` has to be justified for.

### 5. libc out-parameters (6 sites)

`prometheus.rs` does `getrlimit` then `assume_init`. The fix is a wrapper —
`fn syscall_out<T>(f: impl FnOnce(*mut T) -> c_int) -> io::Result<T>` — so the uninitialised read cannot
be written at all. Kani cannot model libc here (extern), and Miri will not reach the failure path
without fault injection.

### 6. `asc.rs` — FFI handles (10 sites)

`CtxPtr`/`SendActive` carry hand-written `Send`/`Sync`, and `Ctx::from_raw` is called on a pointer
stored as `&'static`. Every one of these is a lifetime in disguise: `PhantomData<&'a Ctx>` plus holding
the context guard makes the invariant a type. rquickjs is not going to be verified; the goal is that
our side of the boundary has no obligations it cannot express.

## What no tool will ever discharge

These stay in the ledger as trusted, and should say so explicitly:

- **SIGBUS on a truncated mapping** — an OS contract, not a code property.
- **`O_DIRECT` alignment and `F_NOCACHE`/`posix_fadvise` semantics** — kernel contracts; a wrong
  constant is silent corruption the verifier cannot see.
- **jemalloc's `mallctl` ABI** — the name strings and out-param types are an external ABI.
- **The allocator's layout agreement across a `GlobalAlloc` boundary** — provable for our calls,
  trusted for jemalloc's side.

## CI shape when this lands

1. `cargo kani` job, one harness per `Unsafe` ledger row, each naming its bound in a comment.
2. A drift check, like `Witnesses.lean`: every `.met` theorem in `Ferrosa/Unsafe/Invariants.lean`
   must have a harness of the same name — the same mechanism that keeps the Lean witnesses honest,
   applied to the discharge.
3. Miri on the unsafe-touching crates' test suite, nightly, non-blocking, with a tracked failure list.

Until 1 exists, the trusted base for `unsafe` is: this specification, the ledger, and hand audit —
and the ledger says zero files audited, which is the honest state.
