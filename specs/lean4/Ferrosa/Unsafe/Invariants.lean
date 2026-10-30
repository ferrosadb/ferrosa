/-
  The trusted base under the `unsafe` surface
  ===========================================

  Aeneas cannot translate `unsafe`, because its whole argument is that the borrow checker already ruled
  out aliasing, use-after-free and data races, so proving the `unsafe` surface needs a different story:
  Kani over MIR, which is the only reachable tool that reasons about raw pointers, allocation layouts
  and uninitialised memory in the code as written, and is bounded, so it proves each property only up
  to a bound; Miri, which detects UB on executed paths and is evidence rather than proof; and this
  specification. Each unsafe cluster gets one invariant, one abstract object, and the theorems showing
  the safe wrapper refines that object assuming the invariant holds, which turns the audit into one
  precise question — does the code establish the invariant? — instead of "is this unsafe OK?".
  `docs/unsafe-proof-plan.md` maps every site in `docs/unsafe-ledger.md` to one of these predicates and
  to the tool that can discharge it. Three rows are red, and they are the trusted base's whole gap: no
  Kani integration exists, nothing prevents a truncate-while-mapped, and no separation-logic route to
  `unsafe` is available in this toolchain, so review covers what the tools cannot.
-/
import Ferrosa.Core
import Ferrosa.Ledger

namespace Ferrosa.Unsafe

open Ferrosa Ferrosa.Ledger

/-! ## 1. `commitlog/segment.rs` — `UnsafeCell` buffer with hand-written `Send`/`Sync`

The buffer is reached through `unsafe { &mut *self.buffer.get() }`. That is sound only under a
borrowing discipline the compiler is not checking for us: shared borrows may coexist, a mutable borrow
may not coexist with anything. -/

inductive Buf where
  | free
  | shared (readers : Nat)
  | mutable
  deriving DecidableEq, Repr

def borrowShared : Buf → Option Buf
  | .free => some (.shared 1)
  | .shared n => some (.shared (n + 1))
  | .mutable => none

def borrowMut : Buf → Option Buf
  | .free => some .mutable
  | .shared _ => none
  | .mutable => none

/-- **No two mutable borrows of the same cell.** The second `unsafe { &mut *cell.get() }` fails the
    discipline rather than silently aliasing. -/
theorem two_mutable_borrows_cannot_coexist : (borrowMut .free).bind borrowMut = none := by decide

/-- A mutable borrow excludes readers, and readers exclude a mutable borrow. -/
theorem a_mutable_borrow_excludes_readers :
    borrowMut (.shared 3) = none ∧ borrowShared .mutable = none := by decide

/-- Shared borrows stack, which is what `&self` methods need. -/
theorem shared_borrows_stack (n : Nat) : borrowShared (.shared n) = some (.shared (n + 1)) := by rfl

/-- **`unsafe impl Sync` is justified only if mutation requires exclusivity.** A shared borrow held on
    one thread while another mutates is the data race the marker impl promises cannot happen. -/
theorem a_shared_borrow_racing_a_mutation_is_the_promised_race :
    (borrowShared .mutable).isSome = false ∧ (borrowMut (.shared 1)).isSome = false := by decide

/-! ## 2. `store.rs` — `&'static StoreView` from `Arc::as_ptr`

Fabricating `'static` is sound exactly when nothing frees the pointee during the derived lifetime.
This is the same obligation as any lifetime extension, and it is the one place in the codebase where a
mistake produces a use-after-free that no test reliably catches. -/

structure Life where
  birth : Nat
  death : Option Nat
  deriving DecidableEq, Repr

def aliveAt (l : Life) (t : Nat) : Bool :=
  decide (l.birth ≤ t) && (match l.death with | none => true | some d => decide (t < d))

/-- A reference is valid at `t` only while its owner is alive. -/
def usableAt (owner ref : Life) (t : Nat) : Bool := aliveAt ref t && aliveAt owner t

/-- **Using a reference after its owner is gone is the violation.** Stated over arbitrary lives so it
    is not a claim about one configuration. -/
theorem a_reference_used_past_its_owner_is_invalid (owner ref : Life) (t : Nat)
    (hdead : aliveAt owner t = false) : usableAt owner ref t = false := by
  simp [usableAt, hdead]

/-- Extending a lifetime does not extend the region: `&'static T` from a shorter-lived owner is sound
    only if no use occurs after the owner dies. This is the predicate the audit has to establish for
    `store.rs:2122`. -/
theorem a_static_lifetime_does_not_extend_the_region :
    usableAt ⟨0, some 5⟩ ⟨0, none⟩ 6 = false ∧ usableAt ⟨0, some 5⟩ ⟨0, none⟩ 4 = true := by decide

/-! ## 3. `memmap2::Mmap` (7 sites) — the truncation contract

A mapping is valid for `[0, mapped)` only while the file is at least that long. Truncation is the one
event that turns a valid read into SIGBUS. Crucially, that is a property of the *storage engine's*
rename and GC paths, which are safe code — so this obligation can be discharged on code Aeneas can
see, and the `unsafe` site inherits it. -/

def readable (mapped fileLen offset : Nat) : Bool := decide (offset < mapped && offset < fileLen)

/-- **Truncating a mapped file below an offset makes that offset unreadable**, so the mapping's
    soundness is a claim about a different subsystem: nothing may truncate while a mapping exists. -/
theorem a_truncation_below_a_mapping_makes_the_tail_unreadable :
    readable 4096 8192 4000 = true ∧ readable 4096 1024 4000 = false := by decide

/-- And the mapping must outlive its own slices: dropping the `Mmap` while a slice derived from it is
    live is a dangling read, exactly like case 2. -/
theorem a_slice_outliving_its_mapping_is_a_dangling_read :
    usableAt ⟨0, none⟩ ⟨0, none⟩ 10 = true ∧ usableAt ⟨0, some 1⟩ ⟨0, none⟩ 10 = false := by decide

/-! ## 4. `sstable/direct.rs` — the aligned allocator

`alloc(layout)` and `dealloc(ptr, layout)` must receive the same layout. A mismatch is heap
corruption, and it is invisible: the failure surfaces in an unrelated allocation. -/

structure Block where
  size : Nat
  align : Nat
  deriving DecidableEq, Repr

def deallocMatches (allocated freed : Block) : Bool := allocated == freed

/-- The round trip is balanced. -/
theorem a_block_freed_with_its_own_layout_is_balanced :
    deallocMatches ⟨4096, 512⟩ ⟨4096, 512⟩ = true := by decide

/-- **A layout mismatch is heap corruption, not a leak**: freeing with a larger size hands the
    allocator a block boundary it will reuse for something else. -/
theorem a_block_freed_with_a_different_layout_corrupts_the_heap :
    deallocMatches ⟨4096, 512⟩ ⟨8192, 512⟩ = false := by decide

/-! ## 5. `libc` sites — out-parameters are only valid after success

`prometheus.rs` reads `limit.assume_init()` after `getrlimit`. That is sound only when the call
succeeded; on failure the out-parameter is uninitialised and `assume_init` is UB. -/

inductive Syscall where
  | ok (value : Nat)
  | failed (errno : Nat)
  deriving DecidableEq, Repr

def outParam : Syscall → Option Nat
  | .ok v => some v
  | .failed _ => none

/-- **A failed syscall has no out-parameter.** Reading one is undefined, which is why every site must
    branch on the return code before touching the buffer. -/
theorem a_failed_syscall_has_no_out_parameter : outParam (.failed 22) = none := by rfl

/-- An out-parameter is readable exactly when the call succeeded, so branching on the return code
    before touching the buffer is what makes the read defined rather than undefined. -/
theorem checking_the_return_code_is_what_makes_the_read_defined :
    (outParam (.ok 1024)).isSome = true ∧ (outParam (.failed 22)).isSome = false := by decide

/-! ## 6. `unsafe impl Send`/`Sync` (10 across the ledger) — the marker is a claim about access

A marker impl asserts that concurrent access is safe. That holds when writes are exclusive, which for
a `&mut`-based API means the access intervals of writers never overlap. -/

structure Access where
  thread : Nat
  start : Nat
  stop : Nat
  writes : Bool
  deriving DecidableEq, Repr

def disjoint (a b : Access) : Bool := decide (a.stop ≤ b.start ∨ b.stop ≤ a.start)

def safeConcurrently (a b : Access) : Bool :=
  disjoint a b || (!a.writes && !b.writes)

/-- Two writers on overlapping intervals are the race the marker impl denies. -/
theorem overlapping_writers_are_the_race :
    safeConcurrently ⟨0, 0, 10, true⟩ ⟨1, 5, 15, true⟩ = false := by decide

/-- Two readers may overlap, which is what makes `&self` concurrency possible at all. -/
theorem overlapping_readers_are_fine :
    safeConcurrently ⟨0, 0, 10, false⟩ ⟨1, 5, 15, false⟩ = true := by decide

/-- A writer disjoint from a reader is fine. -/
theorem a_disjoint_writer_and_reader_are_fine :
    safeConcurrently ⟨0, 0, 10, true⟩ ⟨1, 10, 20, false⟩ = true := by decide

/-! ## 7. `ferrosa-udf/src/asc.rs` — FFI handles tied to a live context

`Ctx::from_raw` and `Value::from_raw` are sound only while the context they came from is alive. A
handle carried past a dropped context is the classic FFI dangling use. -/

structure Ctx where
  id : Nat
  live : Bool
  deriving DecidableEq, Repr

def handleValid (handle ctx : Nat) (c : Ctx) : Bool := decide (handle = ctx) && c.live

/-- A handle from a live context is usable. -/
theorem a_handle_from_a_live_context_is_usable : handleValid 7 7 ⟨7, true⟩ = true := by decide

/-- **A handle used after its context is dropped is dangling.** This is what `CtxPtr`'s `Send`/`Sync`
    and the `&'static` store context in `asc.rs` must rule out. -/
theorem a_handle_after_its_context_is_dropped_is_dangling :
    handleValid 7 7 ⟨7, false⟩ = false := by decide

def ledger : List SpecItem := [
  ⟨"the UnsafeCell buffer admits one mutable borrow and any number of shared, never both",
   .met "Unsafe.two_mutable_borrows_cannot_coexist"⟩,
  ⟨"a shared borrow racing a mutation is the data race `unsafe impl Sync` denies",
   .met "Unsafe.a_shared_borrow_racing_a_mutation_is_the_promised_race"⟩,
  ⟨"shared borrows stack, which is what the `&self` readers of the buffer need",
   .met "Unsafe.shared_borrows_stack"⟩,
  ⟨"using a reference after its owner is freed is invalid, whatever the declared lifetime",
   .met "Unsafe.a_reference_used_past_its_owner_is_invalid"⟩,
  ⟨"a `&'static` does not extend the region: soundness is 'no use after the owner dies'",
   .met "Unsafe.a_static_lifetime_does_not_extend_the_region"⟩,
  ⟨"truncating a mapped file below an offset makes that offset unreadable",
   .met "Unsafe.a_truncation_below_a_mapping_makes_the_tail_unreadable"⟩,
  ⟨"a slice outliving its mapping is a dangling read",
   .met "Unsafe.a_slice_outliving_its_mapping_is_a_dangling_read"⟩,
  ⟨"a block freed with its own layout is balanced, and a mismatch corrupts the heap",
   .met "Unsafe.a_block_freed_with_a_different_layout_corrupts_the_heap"⟩,
  ⟨"a failed syscall has no out-parameter, so reading one is undefined",
   .met "Unsafe.a_failed_syscall_has_no_out_parameter"⟩,
  ⟨"overlapping writers are the race a marker impl promises cannot happen",
   .met "Unsafe.overlapping_writers_are_the_race"⟩,
  ⟨"a handle used after its context is dropped is dangling",
   .met "Unsafe.a_handle_after_its_context_is_dropped_is_dangling"⟩,
  ⟨"every `unsafe` site in `docs/unsafe-ledger.md` is discharged by a Kani harness, not by review",
   .failing
     "not implemented: no Kani integration exists. Kani is the only reachable tool that reasons about raw pointers, allocation layouts and uninitialised memory in actual MIR, but it is bounded — a green harness proves the property to a bound, and the bound has to be argued in the harness comment. This is the gap that review currently covers"
     "Unsafe.a_block_freed_with_a_different_layout_corrupts_the_heap"⟩,
  ⟨"the truncation contract is enforced in the safe rename/GC paths, where it can be proved",
   .failing
     "not established: the invariant is stated and the seven mmap sites inherit it, but nothing prevents a truncate-while-mapped. It lives in safe code, so it is reachable by model or by Kani without touching `unsafe` — this is the cheapest of the open items"
     "Unsafe.a_truncation_below_a_mapping_makes_the_tail_unreadable"⟩,
  ⟨"the Aeneas/separation-logic route to proving `unsafe` is not available yet",
   .failing
     "not available: Aeneas documents `unsafe` and concurrency as unsupported pending an ongoing separation-logic extension. RefinedRust and RustBelt are research prototypes outside this toolchain. Until that lands, the trusted base for `unsafe` is: this specification, Kani to a bound, Miri on executed paths, and hand audit"
     "Unsafe.a_reference_used_past_its_owner_is_invalid"⟩
]

/-- Three reds, and between them they are the trusted base's whole gap: no Kani integration, the
    truncation contract stated but not enforced, and no separation-logic route to `unsafe`. -/
theorem unsafe_has_three_red_items : redCount ledger = 3 := by decide

def main : IO Unit :=
  printLedger "the trusted base under the unsafe surface" ledger

end Ferrosa.Unsafe
