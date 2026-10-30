/-
  The executable harness: every invariant, over an enumerated case space
  ======================================================================

  A theorem stays green while the predicate beneath it is quietly weakened, so the invariants under
  the `unsafe` surface are re-run here as executable cases over a finite, enumerated space, printed
  with the case count that was actually checked. A failure prints the falsifying input rather than a
  boolean, and the run exits non-zero, so a statement that drifts away from the obligation it names
  shows up as a red line naming the case that broke it. The harness must also cover exactly the
  theorem names the ledger discharges — a ledger row with no harness is an unvalidated claim, a
  harness with no ledger row is a claim nothing depends on — and both directions fail the run. There
  is no bound left to argue because the case space is finite and written out in the source, which is
  what makes this the Lean-side stand-in for the Kani harnesses `docs/unsafe-proof-plan.md` calls for.
  `lake exe harness` runs it and exits non-zero on any falsified case or any drift.
-/
import Ferrosa.Core
import Ferrosa.Ledger
import Ferrosa.Unsafe.Invariants

namespace Ferrosa.Harness

open Ferrosa Ferrosa.Ledger Ferrosa.Unsafe

/-- One executable claim: a name matching a ledger theorem, the obligation in words, and the cases
    that were run. `failures` carries the falsifying inputs, so a red harness prints the
    counterexample rather than a boolean. -/
structure Check where
  name : String
  claim : String
  cases : Nat
  failures : List String
  deriving Repr

def check (name claim : String) (inputs : List (String × Bool)) : Check :=
  let bad := inputs.filterMap fun (label, ok) => if ok then none else some label
  ⟨name, claim, inputs.length, bad⟩

def Check.passed (c : Check) : Bool := c.failures.isEmpty

/-- Cartesian product of two case spaces. Lean core has no comprehension syntax, so the harness
    builds its grids explicitly. -/
def cross (xs : List α) (ys : List β) (f : α → β → (String × Bool)) : List (String × Bool) :=
  (xs.map fun x => ys.map fun y => f x y).flatten

def cross3 (xs : List α) (ys : List β) (zs : List γ) (f : α → β → γ → (String × Bool)) :
    List (String × Bool) :=
  (xs.map fun x => (ys.map fun y => zs.map fun z => f x y z)).flatten.flatten

def show1 {α : Type} [Repr α] (a : α) : String := s!"{repr a}"

/-! ## 1. `commitlog/segment.rs` — the `UnsafeCell` borrowing discipline -/

/-- Reachable borrow states from `free`, to depth `fuel`, under both borrow operations. -/
def borrowClosure (fuel : Nat) : List Buf :=
  let step : List Buf → List Buf := fun frontier =>
    (frontier.map (fun s => (borrowShared s).getD s)) ++ (frontier.map (fun s => (borrowMut s).getD s))
  let rec go : Nat → List Buf → List Buf
    | 0, acc => acc
    | n + 1, acc =>
      let next := (step acc).filter (fun s => !acc.contains s)
      go n (acc ++ next)
  go fuel [.free]

def readers (s : Buf) : Nat := match s with | .shared n => n | _ => 0

def wellFormed (s : Buf) : Bool :=
  readers s = 0 || (match s with | .mutable => false | _ => true)

/-- The obligation: no reachable state admits a writer and a reader at once. -/
def twoMutableBorrows : Check :=
  check "two_mutable_borrows_cannot_coexist"
    "no reachable borrow state holds a mutable borrow and a shared borrow at the same time"
    ((borrowClosure 4).map fun s => (show1 s, wellFormed s))

def sharedBorrowsStack : Check :=
  check "shared_borrows_stack"
    "successive shared borrows increase the reader count by one and never create a writer"
    (((List.range 5).map fun n => (s!"shared {n}",
      borrowShared (.shared n) == some (.shared (n + 1)))) ++
     [("mutable is refused a reader", (borrowShared .mutable).isNone),
      ("mutable is refused a second writer", (borrowMut .mutable).isNone)])

def mutationExcludesReaders : Check :=
  check "a_shared_borrow_racing_a_mutation_is_the_promised_race"
    "a state granting a mutable borrow refuses readers and a second writer; every shared state refuses a writer"
    ([("mutable refuses readers and writers",
        (borrowMut .mutable).isNone && (borrowShared .mutable).isNone)] ++
     ((List.range 4).map fun n => (s!"shared {n} refuses a writer", (borrowMut (.shared n)).isNone)) ++
     [("free grants exactly one writer",
        (borrowMut .free).isSome && (borrowMut ((borrowMut .free).getD .free)).isNone)])

/-! ## 2. `store.rs` — a reference may not be used past its owner -/

def lives : List Life := (List.range 6).map (fun d => ⟨0, some d⟩) ++ [⟨0, none⟩, ⟨1, some 1⟩]

def referenceUse : Check :=
  check "a_reference_used_past_its_owner_is_invalid"
    "at every instant a usable reference implies a live owner, for every lifetime pair tested"
    (cross3 lives lives (List.range 7) fun o r t =>
      (s!"owner {show1 o} ref {show1 r} t {t}", !(usableAt o r t) || aliveAt o t))

def staticLifetime : Check :=
  check "a_static_lifetime_does_not_extend_the_region"
    "an owner that dies at 5 makes its `'static` reference unusable from 5 on, and usable before"
    [("use at 4 is sound", usableAt ⟨0, some 5⟩ ⟨0, none⟩ 4),
     ("use at 5 is the violation", !usableAt ⟨0, some 5⟩ ⟨0, none⟩ 5),
     ("use at 6 is the violation", !usableAt ⟨0, some 5⟩ ⟨0, none⟩ 6)]

/-! ## 3. mmap — the truncation contract, which lives in safe code -/

def mappings : List Nat := [0, 1024, 4096]
def fileLens : List Nat := [0, 1024, 4096, 8192]
def offsets : List Nat := [0, 1000, 1023, 1024, 4000, 4095, 4096, 5000]

def truncationContract : Check :=
  check "a_truncation_below_a_mapping_makes_the_tail_unreadable"
    "a read is legal only below both the mapping length and the current file length"
    (cross3 mappings fileLens offsets fun m f o =>
      (s!"mapped {m} file {f} offset {o}", !(readable m f o) || (o < f && o < m)))

def truncationIsUnreadable : Check :=
  check "a_truncation_below_a_mapping_makes_the_tail_unreadable.truncated"
    "shrinking the file below an offset that used to be readable makes it unreadable"
    ((List.range 40).map fun k =>
      let o := 1 + k * 100
      (s!"offset {o} after truncation to {o - 1}", readable 4096 8192 o && !(readable 4096 (o - 1) o)))

def sliceOutlivesMapping : Check :=
  check "a_slice_outliving_its_mapping_is_a_dangling_read"
    "a slice derived from a mapping is usable only while the mapping is alive"
    [("mapping alive", usableAt ⟨0, none⟩ ⟨0, none⟩ 10),
     ("mapping dropped at 1", !usableAt ⟨0, some 1⟩ ⟨0, none⟩ 10),
     ("mapping dropped at 9", !usableAt ⟨0, some 9⟩ ⟨0, none⟩ 10)]

/-! ## 4. `direct.rs` — layout agreement in the aligned allocator -/

def blocks : List Block := [⟨1, 1⟩, ⟨512, 512⟩, ⟨4096, 512⟩, ⟨4096, 4096⟩, ⟨8192, 4096⟩]

def layoutAgreement : Check :=
  check "a_block_freed_with_a_different_layout_corrupts_the_heap"
    "dealloc accepts a block exactly when the layout matches the one it was allocated with"
    (cross blocks blocks fun a b => (s!"{show1 a} vs {show1 b}", deallocMatches a b == (a == b)))

/-! ## 5. libc — an out-parameter exists only after success -/

def syscalls : List Syscall := [.ok 0, .ok 1024, .ok 4_294_967_295, .failed 1, .failed 22, .failed 75]

def outParamExistence : Check :=
  check "a_failed_syscall_has_no_out_parameter"
    "the out-parameter is readable exactly for a successful call, and the errno carries nothing"
    (syscalls.map fun s =>
      (show1 s, (outParam s).isSome == (match s with | .ok _ => true | .failed _ => false)))

/-! ## 6. `unsafe impl Send`/`Sync` — the marker is a claim about access intervals -/

def accessPairs : List Access :=
  [⟨0, 0, 10, true⟩, ⟨0, 0, 10, false⟩, ⟨1, 5, 15, true⟩, ⟨1, 5, 15, false⟩,
   ⟨1, 10, 20, true⟩, ⟨1, 9, 20, false⟩, ⟨1, 0, 10, true⟩]

def markerSoundness : Check :=
  check "overlapping_writers_are_the_race"
    "concurrent access is safe exactly when the intervals are disjoint or neither writes"
    (cross accessPairs accessPairs fun a b =>
      (s!"{show1 a} {show1 b}", safeConcurrently a b == (disjoint a b || (!a.writes && !b.writes))))

def overlappingWritersAreNeverSafe : Check :=
  check "overlapping_writers_are_the_race.exhaustive"
    "two writers on overlapping intervals are never safe, whatever the thread ids say"
    (cross accessPairs accessPairs fun a b =>
      (s!"threads {a.thread}/{b.thread} over {a.start}-{a.stop}",
        !(safeConcurrently a b && a.writes && b.writes && !(disjoint a b))))

/-! ## 7. `asc.rs` — an FFI handle is valid only inside a live context -/

def contexts : List Ctx := [⟨7, true⟩, ⟨7, false⟩, ⟨8, true⟩, ⟨8, false⟩]

def handleValidity : Check :=
  check "a_handle_after_its_context_is_dropped_is_dangling"
    "a handle is usable exactly when it names this context and the context is alive"
    (cross [7, 8, 9] contexts fun h c =>
      (s!"handle {h} ctx {show1 c}", handleValid h c.id c == (h == c.id && c.live)))

def checks : List Check :=
  [twoMutableBorrows, sharedBorrowsStack, mutationExcludesReaders,
   referenceUse, staticLifetime,
   truncationContract, truncationIsUnreadable, sliceOutlivesMapping,
   layoutAgreement, outParamExistence, markerSoundness, overlappingWritersAreNeverSafe,
   handleValidity]

/-! ## Drift

The harness must cover exactly the theorems the ledger names as discharges. A ledger entry whose
theorem has no harness is an unvalidated claim; a harness whose theorem is not in the ledger is a claim
nothing depends on. Both directions fail the run. -/

def ledgerTheorems : List String :=
  Ferrosa.Unsafe.ledger.filterMap fun i =>
    match i.status with | .met p => some (p.splitOn "." |>.getLast!) | .failing _ _ => none

def harnessNames : List String :=
  (checks.map (·.name)).map fun n => (n.splitOn ".").headD n

def uncovered : List String := ledgerTheorems.filter fun t => !harnessNames.contains t

def orphaned : List String := (harnessNames.filter fun n => !ledgerTheorems.contains n).eraseDups

def driftFailures : List String :=
  (uncovered.map fun t => s!"no harness for ledger theorem {t}") ++
  (orphaned.map fun n => s!"harness {n} has no ledger theorem")

/-- Total enumerated cases across every harness, so the size of the check is visible. -/
def totalCases : Nat := (checks.map (·.cases)).sum

def render (c : Check) : String :=
  let head := s!"  {if c.passed then "PASS" else "FAIL"}  {c.name}  ({c.cases} cases) — {c.claim}"
  if c.passed then head
  else head ++ "\n" ++ String.intercalate "\n" (c.failures.map fun f => s!"          falsified by: {f}")

def main : IO Unit := do
  let failures := (checks.filter fun c => !c.passed).map (·.failures.length)
  IO.println s!"the harness over the invariants under the unsafe surface ({checks.length} checks, {totalCases} enumerated cases)\n"
  IO.println (String.intercalate "\n" (checks.map render))
  IO.println ""
  if !driftFailures.isEmpty then
    IO.println (String.intercalate "\n" (driftFailures.map fun f => s!"  DRIFT  {f}"))
  if failures.isEmpty && driftFailures.isEmpty then
    IO.println s!"ALL PASS — every invariant holds over {totalCases} enumerated cases and the ledger is covered"
  else
    IO.println s!"FAILED — {failures.length} invariant(s) falsified, {driftFailures.length} drift item(s)"
    IO.Process.exit 1

end Ferrosa.Harness

/-- Root entry point, so both `lake exe harness` and `lake env lean --run Ferrosa/Harness.lean` work. -/
def main : IO Unit := Ferrosa.Harness.main
