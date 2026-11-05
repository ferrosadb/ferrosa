/-
  Ownership: one payload, N handles
  ================================

  The operator's rule is four words — moves, no clones — and the half that is easy to get wrong is
  the second one. A value has one owner at a time, so passing it on is a move: the sender ends the
  exchange holding nothing it can use. But several places legitimately need to refer to the same
  thing — a peer send to *k* replicas, a snapshot served to every follower, a row an observer and a
  memtable both need — and the answer there is not *k* copies of the value. It is one payload with
  *k* refcounted handles on it.

  This module is the machine for that distinction. A heap holds payloads; a payload carries a
  logical value, a version, a byte length, and the list of holders referring to it. A handle is one
  `(payload, holder)` pair, so the number of handles on a payload is the length of its holder list
  and the stored reference count has to equal it. Nothing here knows what a row is; the payload is a
  length and an identity, which is what the ownership question depends on.

  Four transitions, and the guards are the theorem:

  * `attach` adds a handle — the refcount rises, the payload does not move;
  * `handOff` moves a handle from one holder to another — the refcount does not change at all,
    because a move neither creates nor destroys a reference, and the sender's handle is gone;
  * `release` removes a handle — at zero handles the payload leaves the heap and its id is recorded;
  * `mutate` rewrites a payload **only** at refcount one. Above one it is not a transition but a
    refused attempt, which is the model's whole statement of shared immutability. The code's
    `Arc::make_mut` is `cow`: it publishes a new version and leaves the shared one alone.

  The invariant is carried over every reachable state, not over a chosen trace. `Op` is the
  alphabet, `runAll` runs a trace from the empty heap, `inv_*` shows each operation preserves the
  invariant, and `reachable_inv` is the induction over the trace that produced the state. The runs
  under "Traces" exist to show the arithmetic — a fan-out to four peers, a send whose peer fails, a
  pair of handles released in either order — not to stand in for the proofs.
-/
import Ferrosa.Ledger

namespace Ferrosa.Ownership

open Ferrosa.Ledger

/-! ## Payloads, handles, heap

A payload is identified by `id` and carries the logical `value` it is a copy of, the `version` of
that value, its byte length, and the holders that refer to it. `Kind.value` entries are the resident
copy of a value; `Kind.artifact` entries are derived buffers — an encode buffer is one of these, and
counting it as a second copy of the value is the error the split exists to prevent.
-/

/-- What a payload is: the resident copy of a value, or a derived artifact such as an encode buffer. -/
inductive Kind where
  /-- The resident payload of a logical value. -/
  | value
  /-- A derived buffer produced from a payload: an encode buffer, a serialised frame. -/
  | artifact
  deriving BEq, DecidableEq, Repr

/-- A payload in the heap: its identity, the value it carries and which version of it, its byte
    length, and the holders referring to it. -/
structure Payload where
  /-- Heap-unique identity, allocated from `Heap.hnext` and never reused. -/
  id : Nat
  /-- Byte length of the payload. This is what residence costs. -/
  len : Nat
  /-- The logical value this payload carries. Two payloads with the same `value` are two copies. -/
  value : Nat
  /-- Which version of that value. Copy-on-write publishes a higher version; it does not edit this
      one. -/
  version : Nat
  /-- Resident value copy or derived artifact. -/
  kind : Kind
  /-- The stored reference count. `RefsOk` says it equals `holders.length` for every payload. -/
  refs : Nat
  /-- The holders referring to this payload, one entry per handle. -/
  holders : List Nat
  deriving BEq, Repr

/-- Bytes charged for one handle: the `Arc` word and the holder slot it costs to name a payload.
    A fan-out's memory is the payload plus this, once per consumer — never the payload again. -/
def handleMeta : Nat := 24

/-- The heap: every payload, the identity allocator, and the ids whose last handle was released. -/
structure Heap where
  /-- Resident payloads, in allocation order. -/
  payloads : List Payload := []
  /-- Next unused payload id. Monotone, so a freed id is never handed out again. -/
  hnext : Nat := 0
  /-- Ids whose last handle has been released, most recent first. -/
  freed : List Nat := []

/-- The heap with nothing in it. -/
def empty : Heap := {}

/-! ## Reading a heap

The accessors below are primitive recursions rather than `List.filter`, because the theorems are
inductions over the payload list and each step has to see the head.
-/

/-- Reference count stored on payload `p`, or `0` when `p` is not resident. -/
def refsOf (p : Nat) : List Payload → Nat
  | [] => 0
  | x :: xs => if x.id == p then x.refs else refsOf p xs

/-- Number of handles naming payload `p`. -/
def handlesOf (p : Nat) : List Payload → Nat
  | [] => 0
  | x :: xs => (if x.id == p then x.holders.length else 0) + handlesOf p xs

/-- Whether `holder` holds a handle on payload `p`. -/
def heldBy (p holder : Nat) : List Payload → Bool
  | [] => false
  | x :: xs => (x.id == p && x.holders.contains holder) || heldBy p holder xs

/-- Payload `p`'s byte length, or `0` when it is not resident. -/
def lenOf (p : Nat) : List Payload → Nat
  | [] => 0
  | x :: xs => if x.id == p then x.len else lenOf p xs

/-- Payload `p`'s version, or `0` when it is not resident. -/
def versionOf (p : Nat) : List Payload → Nat
  | [] => 0
  | x :: xs => if x.id == p then x.version else versionOf p xs

/-- Total bytes held by payloads. -/
def bytesOf : List Payload → Nat
  | [] => 0
  | x :: xs => x.len + bytesOf xs

/-- Total handle metadata, one charge per handle on every payload. -/
def metaOf : List Payload → Nat
  | [] => 0
  | x :: xs => x.holders.length * handleMeta + metaOf xs

/-- Resident bytes: payload bytes plus handle metadata. -/
def residentBytes (ps : List Payload) : Nat := bytesOf ps + metaOf ps

/-- Payloads carrying value `v` of a given kind. `countOf v .value` is the residency of `v`. -/
def countOf (v : Nat) (k : Kind) : List Payload → Nat
  | [] => 0
  | x :: xs => (if x.value == v && x.kind == k then 1 else 0) + countOf v k xs

namespace Heap

/-- Payloads carrying value `v` as a resident value copy. -/
def valueCopies (h : Heap) (v : Nat) : Nat := countOf v .value h.payloads

/-- Artifacts derived from value `v`. -/
def artifacts (h : Heap) (v : Nat) : Nat := countOf v .artifact h.payloads

/-- Handles naming payload `p`. -/
def handles (h : Heap) (p : Nat) : Nat := handlesOf p h.payloads

/-- Stored reference count of payload `p`. -/
def refs (h : Heap) (p : Nat) : Nat := refsOf p h.payloads

/-- Byte length of payload `p`. -/
def len (h : Heap) (p : Nat) : Nat := lenOf p h.payloads

/-- Version of payload `p`. -/
def version (h : Heap) (p : Nat) : Nat := versionOf p h.payloads

/-- Whether `holder` has a handle on payload `p`. -/
def holds (h : Heap) (holder p : Nat) : Bool := heldBy p holder h.payloads

/-- Whether payload `p` is resident. -/
def live (h : Heap) (p : Nat) : Bool := h.refs p != 0

/-- Resident bytes. -/
def bytes (h : Heap) : Nat := residentBytes h.payloads

/-- The logical value a payload carries, or `0`. -/
def valueOf (h : Heap) (p : Nat) : Nat :=
  ((h.payloads.find? (fun x => x.id == p)).map (·.value)).getD 0

end Heap

/-! ## Rewrites of the payload list

Each in-place transition is one `List.map` with a named function, so the proofs use `List.mem_map`
and `List.map_congr_left` instead of induction over ad-hoc matches. Every one of them carries its
guard inside the function as well as at the API, so a rewrite that would break the count simply does
nothing: `detachFn` only decrements when the holder is really there.
-/

/-- Add one handle: the reference count and the holder list move together. -/
def attachFn (p holder : Nat) (x : Payload) : Payload :=
  if x.id == p then { x with refs := x.refs + 1, holders := x.holders ++ [holder] } else x

/-- Remove one occurrence of `holder` from a holder list. -/
def removeOne (holder : Nat) : List Nat → List Nat
  | [] => []
  | y :: ys => if y == holder then ys else y :: removeOne holder ys

/-- Remove one handle, but only one that is there: the count and the holder list move together or
    not at all. -/
def detachFn (p holder : Nat) (x : Payload) : Payload :=
  if x.id == p && x.holders.contains holder then
    { x with refs := x.refs - 1, holders := removeOne holder x.holders }
  else x

/-- Rewrite a payload's length. Only ever applied at reference count one. -/
def rewriteFn (p len' : Nat) (x : Payload) : Payload :=
  if x.id == p then { x with len := len' } else x

/-- Replace a holder name in a holder list, in place. -/
def replaceHolder (src dst : Nat) : List Nat → List Nat
  | [] => []
  | y :: ys => if y == src then dst :: ys else y :: replaceHolder src dst ys

/-- The move: one holder name becomes another, and the count is untouched. -/
def renameFn (p src dst : Nat) (x : Payload) : Payload :=
  if x.id == p then { x with holders := replaceHolder src dst x.holders } else x

/-- Drop a payload entirely: its last handle has been released. -/
def removePayload (p : Nat) : List Payload → List Payload
  | [] => []
  | x :: xs => if x.id == p then removePayload p xs else x :: removePayload p xs

/-- Whether a payload is the resident value copy of `(v, ver)`. An artifact is a derivation of a
    value, not a second copy of it, so it is not counted here. -/
def isValueKey (v ver : Nat) (x : Payload) : Bool := decide (x.value = v ∧ x.version = ver ∧ x.kind = .value)

/-- The payload just allocated for `(v, ver)` is that key's resident copy. -/
theorem isValueKey_self (pid len v ver holder : Nat) :
    isValueKey v ver (⟨pid, len, v, ver, Kind.value, 1, [holder]⟩ : Payload) = true := by
  unfold isValueKey
  simp

/-- A payload allocated for a different key is not that key's resident copy. -/
theorem isValueKey_other (pid len v ver holder v' ver' : Nat)
    (hne : ¬ (v = v' ∧ ver = ver')) :
    isValueKey v' ver' (⟨pid, len, v, ver, Kind.value, 1, [holder]⟩ : Payload) = false := by
  unfold isValueKey
  simp only [Payload.value, Payload.version, Payload.kind]
  by_cases hv : v = v'
  · have hvv : ver ≠ ver' := fun hc => hne ⟨hv, hc⟩
    simp [hv, hvv]
  · simp [hv]

/-- An artifact is never a value copy, whatever key it is compared against. -/
theorem isValueKey_artifact (pid len v ver holder v' ver' : Nat) :
    isValueKey v' ver' (⟨pid, len, v, ver, Kind.artifact, 1, [holder]⟩ : Payload) = false := by
  unfold isValueKey
  simp

/-- How many resident value copies carry `(v, ver)`. Single residence says this is at most one. -/
def keyCount (v ver : Nat) : List Payload → Nat
  | [] => 0
  | x :: xs => (if isValueKey v ver x then 1 else 0) + keyCount v ver xs

/-- A rewrite that leaves identity, value, version and kind alone — every in-place transition. -/
structure SameIdentity (f : Payload → Payload) : Prop where
  /-- The payload keeps its id. -/
  id : ∀ x, (f x).id = x.id
  /-- The payload keeps its logical value. -/
  value : ∀ x, (f x).value = x.value
  /-- The payload keeps its version. -/
  version : ∀ x, (f x).version = x.version
  /-- The payload keeps its kind. -/
  kind : ∀ x, (f x).kind = x.kind

/-! ## The invariant

Two clauses, and each is a thing a refcount bug breaks:

* `RefsOk` — the stored count is the number of handles. A count that drifts from the holder list is
  a leak (too high) or a use-after-free (too low), and this clause rules out both.
* `Residence` — at most one resident **value** payload per `(value, version)`. Stated over the heap
  rather than over one code path, which is what makes it an audit of the whole tree instead of a
  property of one function.

Release and id-freedom are separate theorems rather than invariant clauses, because they are claims
about one transition each (`release_records_the_id`, `a_released_handle_cannot_be_released_again`,
`alloc_mints_the_next_id`), and folding them in would hide the step that has to be checked.
-/

/-- Every payload's stored count equals its number of handles. -/
def RefsOk (ps : List Payload) : Prop := ∀ x ∈ ps, x.refs = x.holders.length

/-- At most one resident value copy per `(value, version)`. -/
def Residence (ps : List Payload) : Prop := ∀ v ver, keyCount v ver ps ≤ 1

/-- The heap invariant. -/
structure Inv (h : Heap) : Prop where
  /-- Count equals handles, everywhere. -/
  refs : RefsOk h.payloads
  /-- One value payload per `(value, version)`. -/
  residence : Residence h.payloads

/-! ## Transitions

Each is a function returning `Option`: `none` is "this is not a transition", which is how a refused
operation is represented rather than as a state change with a flag. The guards are part of the
machine, not comments on it.
-/

namespace Heap

/-- Allocate a payload and one handle on it. The id comes from `hnext`, which the allocation raises,
    so the same id is never minted twice. -/
def alloc (h : Heap) (v ver len holder : Nat) (k : Kind) : Heap :=
  { payloads := h.payloads ++ [⟨h.hnext, len, v, ver, k, 1, [holder]⟩],
    hnext := h.hnext + 1,
    freed := h.freed }

/-- Share a value with `holder`: a new handle, no new payload. Refused when the holder already has
    one here, because a second identical handle is a second release. -/
def attach (h : Heap) (p holder : Nat) : Option Heap :=
  if h.live p && !h.holds holder p then
    some { h with payloads := h.payloads.map (attachFn p holder) }
  else none

/-- Share with `n` further consumers. -/
def attachMany (h : Heap) (p first : Nat) : Nat → Option Heap
  | 0 => some h
  | n + 1 => match h.attach p (first + n) with
    | none => none
    | some h' => h'.attachMany p first n

/-- **The move.** The sender's handle becomes the receiver's. The reference count does not change:
    a move neither creates nor destroys a reference, which is exactly why it cannot leak. -/
def handOff (h : Heap) (src dst p : Nat) : Option Heap :=
  if h.holds src p && !h.holds dst p then
    some { h with payloads := h.payloads.map (renameFn p src dst) }
  else none

/-- Release one handle. At zero handles the payload leaves the heap and its id is recorded. -/
def release (h : Heap) (holder p : Nat) : Option Heap :=
  if h.holds holder p then
    if h.handles p <= 1 then
      some { h with payloads := removePayload p h.payloads, freed := p :: h.freed }
    else
      some { h with payloads := h.payloads.map (detachFn p holder) }
  else none

/-- **Mutate in place, uniquely.** Refused above one reference: this is shared immutability as a
    guard rather than as a convention. -/
def mutate (h : Heap) (p len' : Nat) : Option Heap :=
  if h.refs p == 1 then
    some { h with payloads := h.payloads.map (rewriteFn p len') }
  else none

/-- **Copy-on-write.** A shared payload is forked: the writer's handle leaves the shared payload, a
    payload at `newVer` is published with the writer's handle on it, and every existing holder keeps
    the old version untouched. `newVer` has to be a version nothing resident holds yet, which makes
    the fork a *new* version rather than a second copy of the same one — the difference between the
    sanctioned duplication and the violation. -/
def cow (h : Heap) (holder p newVer : Nat) : Option Heap :=
  if h.holds holder p && 1 < h.refs p && keyCount (h.valueOf p) newVer h.payloads == 0 then
    let fresh : Payload := ⟨h.hnext, h.len p, h.valueOf p, newVer, .value, 1, [holder]⟩
    some { payloads := h.payloads.map (detachFn p holder) ++ [fresh],
           hnext := h.hnext + 1, freed := h.freed }
  else none

/-- **A peer send.** One encode produces one artifact, and every peer gets a handle on it. The
    payload of the value is not touched, and the artifact is allocated once however many peers there
    are — the `k`-independence obligation. -/
def send (h : Heap) (p : Nat) (encodedLen : Nat) (peers : List Nat) : Option Heap :=
  if h.live p && !peers.isEmpty then
    let h1 := h.alloc (h.valueOf p) (h.version p) encodedLen 0 .artifact
    h1.attachMany (h1.hnext - 1) 1 peers.length
  else none

end Heap

/-! ## List lemmas

These are the inductions the preservation proofs stand on: what a `map` does to a count, what a
`filter` does to a key count, and how the holder list shrinks by exactly one. Each is stated over an
arbitrary payload list so it applies to any heap, and each is proved by the same recipe — a case
split on the id the rewrite tests, then arithmetic.
-/

/-- Each in-place rewrite keeps the `(value, version, kind)` identity of every payload. -/
theorem keyCount_map (f : Payload → Payload) (hf : SameIdentity f) (v ver : Nat)
    (ps : List Payload) : keyCount v ver (ps.map f) = keyCount v ver ps := by
  induction ps with
  | nil => rfl
  | cons x xs ih =>
    have h1 : (f x).value = x.value := hf.value x
    have h2 : (f x).version = x.version := hf.version x
    have h3 : (f x).kind = x.kind := hf.kind x
    simp [keyCount, isValueKey, h1, h2, h3, ih]

/-- Appending one payload adds exactly its own contribution to a key count. -/
theorem keyCount_append_one (v ver : Nat) (ps : List Payload) (x : Payload) :
    keyCount v ver (ps ++ [x]) =
      keyCount v ver ps + (if isValueKey v ver x then 1 else 0) := by
  induction ps with
  | nil => simp [keyCount]
  | cons y ys ih => simp [keyCount, ih]; omega

/-- Adding one payload cannot lower any key count. -/
theorem keyCount_cons_le (v ver : Nat) (x : Payload) (xs : List Payload) :
    keyCount v ver xs ≤ keyCount v ver (x :: xs) := by
  simp only [keyCount]
  split <;> omega

/-- Dropping a payload cannot raise any key count. -/
theorem keyCount_removePayload_le (v ver p : Nat) (ps : List Payload) :
    keyCount v ver (removePayload p ps) ≤ keyCount v ver ps := by
  induction ps with
  | nil => simp [removePayload]
  | cons x xs ih =>
    by_cases h : (x.id == p) = true
    · have hstep : removePayload p (x :: xs) = removePayload p xs := by
        simp only [removePayload]
        rw [if_pos h]
      rw [hstep]
      exact Nat.le_trans ih (keyCount_cons_le v ver x xs)
    · have hstep : removePayload p (x :: xs) = x :: removePayload p xs := by
        simp only [removePayload]
        rw [if_neg h]
      rw [hstep]
      simp only [keyCount]
      exact Nat.add_le_add_left ih _

/-- Removing one holder name shortens a holder list by exactly one. -/
theorem length_removeOne (holder : Nat) (l : List Nat) (hmem : holder ∈ l) :
    (removeOne holder l).length + 1 = l.length := by
  induction l with
  | nil => simp at hmem
  | cons y ys ih =>
    by_cases h : (y == holder) = true
    · simp only [removeOne]
      rw [if_pos h]
      simp
    · have hyne : y ≠ holder := by
        intro heq
        rw [heq] at h
        simp at h
      have hmem' : holder ∈ ys := by
        rcases List.mem_cons.mp hmem with heq | hin
        · exact absurd heq.symm hyne
        · exact hin
      have hih := ih hmem'
      simp only [removeOne]
      rw [if_neg h]
      simp only [List.length_cons]
      omega

/-- Renaming a holder keeps the number of handles. -/
theorem length_replaceHolder (src dst : Nat) (l : List Nat) :
    (replaceHolder src dst l).length = l.length := by
  induction l with
  | nil => rfl
  | cons y ys ih =>
    by_cases h : (y == src) = true
    · simp only [replaceHolder]
      rw [if_pos h]
      simp
    · simp only [replaceHolder]
      rw [if_neg h]
      simp only [List.length_cons]
      omega

/-- After a rename the old name is no longer a holder, provided the holder list had no duplicate —
    which is what one handle per holder means. -/
theorem not_mem_replaceHolder (src dst : Nat) (l : List Nat) (hne : src ≠ dst)
    (hnd : l.Nodup) : src ∉ replaceHolder src dst l := by
  induction l with
  | nil => simp [replaceHolder]
  | cons y ys ih =>
    simp only [List.nodup_cons] at hnd
    obtain ⟨hnotmem, hnd'⟩ := hnd
    by_cases h : (y == src) = true
    · have hy : y = src := by simpa using h
      simp only [replaceHolder]
      rw [if_pos h]
      simp only [List.mem_cons]
      intro hmem
      rcases hmem with heq | hin
      · exact hne heq
      · exact absurd hin (by rw [← hy]; exact hnotmem)
    · simp only [replaceHolder]
      rw [if_neg h]
      simp only [List.mem_cons]
      intro hmem
      rcases hmem with heq | hin
      · have hy : (y == src) = true := by rw [← heq]; simp
        exact absurd hy h
      · exact ih hnd' hin

/-! ## The invariant is preserved by every step

Each transition is one lemma, and each lemma is the same shape: unfold the guard, take the branch that
produced the state, and show the two clauses land on the other side unchanged. `attach` raises a count
and a holder list together; `detach` lowers both; `rename` and `rewrite` change neither; `remove`
drops a payload from all three; and `cow` adds a payload whose key nothing resident carried.
-/

/-- `attach` adds a holder, it does not touch payload bytes. -/
theorem lenOf_map_attach (p holder q : Nat) (ps : List Payload) :
    lenOf q (ps.map (attachFn p holder)) = lenOf q ps := by
  induction ps with
  | nil => rfl
  | cons x xs ih =>
    have hid : (attachFn p holder x).id = x.id := by simp only [attachFn]; split <;> rfl
    have hlen : (attachFn p holder x).len = x.len := by simp only [attachFn]; split <;> rfl
    by_cases hq : (x.id == q) = true
    · simp only [List.map_cons, lenOf]
      rw [hid, hlen, if_pos hq]
      rw [if_pos hq]
    · simp only [List.map_cons, lenOf]
      rw [hid, if_neg hq, if_neg hq]
      exact ih

/-- `detach` adds nothing and removes nothing but a holder name. -/
theorem lenOf_map_detach (p holder q : Nat) (ps : List Payload) :
    lenOf q (ps.map (detachFn p holder)) = lenOf q ps := by
  induction ps with
  | nil => rfl
  | cons x xs ih =>
    have hid : (detachFn p holder x).id = x.id := by simp only [detachFn]; split <;> rfl
    have hlen : (detachFn p holder x).len = x.len := by simp only [detachFn]; split <;> rfl
    by_cases hq : (x.id == q) = true
    · simp only [List.map_cons, lenOf]
      rw [hid, hlen, if_pos hq]
      rw [if_pos hq]
    · simp only [List.map_cons, lenOf]
      rw [hid, if_neg hq, if_neg hq]
      exact ih

/-- A length rewrite leaves every other payload's count alone. -/
theorem refsOf_rewriteFn (p len' q : Nat) (ps : List Payload) :
    refsOf q (ps.map (rewriteFn p len')) = refsOf q ps := by
  induction ps with
  | nil => rfl
  | cons x xs ih =>
    have hid : (rewriteFn p len' x).id = x.id := by simp only [rewriteFn]; split <;> rfl
    have hrf : (rewriteFn p len' x).refs = x.refs := by simp only [rewriteFn]; split <;> rfl
    by_cases hq : (x.id == q) = true
    · simp only [List.map_cons, refsOf]
      rw [hid, hrf, if_pos hq]
      rw [if_pos hq]
    · simp only [List.map_cons, refsOf]
      rw [hid, if_neg hq, if_neg hq]
      exact ih

/-- A length rewrite of a resident payload sets that length. -/
theorem lenOf_map_rewrite_self (p len' : Nat) (ps : List Payload) (h : refsOf p ps = 1) :
    lenOf p (ps.map (rewriteFn p len')) = len' := by
  induction ps with
  | nil => exact absurd h (by simp [refsOf])
  | cons x xs ih =>
    have hid : (rewriteFn p len' x).id = x.id := by simp only [rewriteFn]; split <;> rfl
    by_cases hp : (x.id == p) = true
    · have hx : x.id = p := by simpa using hp
      simp only [List.map_cons, lenOf]
      rw [hid, hx, if_pos (by simp : (p == p) = true)]
      simp only [rewriteFn, hp]
      simp
    · have hx : refsOf p xs = 1 := by
        simp only [refsOf, if_neg hp] at h
        exact h
      simp only [List.map_cons, lenOf]
      rw [hid, if_neg hp]
      exact ih hx

/-- A freed payload has no handles left. -/
theorem handlesOf_removePayload_self (p : Nat) (ps : List Payload) :
    handlesOf p (removePayload p ps) = 0 := by
  induction ps with
  | nil => rfl
  | cons x xs ih =>
    by_cases h : (x.id == p) = true
    · have hstep : removePayload p (x :: xs) = removePayload p xs := by
        simp only [removePayload]; rw [if_pos h]
      rw [hstep]
      exact ih
    · have hstep : removePayload p (x :: xs) = x :: removePayload p xs := by
        simp only [removePayload]; rw [if_neg h]
      rw [hstep]
      simp only [handlesOf, if_neg h]
      simpa using ih

/-- A freed payload has no count left. -/
theorem refsOf_removePayload_self (p : Nat) (ps : List Payload) :
    refsOf p (removePayload p ps) = 0 := by
  induction ps with
  | nil => rfl
  | cons x xs ih =>
    by_cases h : (x.id == p) = true
    · have hstep : removePayload p (x :: xs) = removePayload p xs := by
        simp only [removePayload]; rw [if_pos h]
      rw [hstep]
      exact ih
    · have hstep : removePayload p (x :: xs) = x :: removePayload p xs := by
        simp only [removePayload]; rw [if_neg h]
      rw [hstep]
      simp only [refsOf, if_neg h]
      exact ih

/-- After a rename no payload carries the old holder name, so nobody can act through it. -/
theorem heldBy_rename_fn (p src dst : Nat) (ps : List Payload)
    (hnd : ∀ x ∈ ps, x.holders.Nodup) (hne : src ≠ dst) :
    heldBy p src (ps.map (renameFn p src dst)) = false := by
  induction ps with
  | nil => rfl
  | cons x xs ih =>
    have hx : x.holders.Nodup := hnd x (List.mem_cons_self ..)
    have hxs : ∀ y ∈ xs, y.holders.Nodup := fun y hy => hnd y (List.mem_cons_of_mem x hy)
    simp only [List.map_cons, heldBy]
    rw [ih hxs, Bool.or_false]
    by_cases hid : (x.id == p) = true
    · have hnot : src ∉ replaceHolder src dst x.holders :=
        not_mem_replaceHolder src dst x.holders hne hx
      have hcont : (replaceHolder src dst x.holders).contains src = false := by
        by_cases hc : (replaceHolder src dst x.holders).contains src = true
        · exact absurd (List.contains_iff_mem.mp hc) hnot
        · simpa using hc
      simp only [renameFn]
      rw [if_pos hid]
      simp only [hcont, Bool.and_false]
    · simp only [renameFn]
      rw [if_neg hid]
      simp only [hid, Bool.false_and]

/-- `attach` keeps the count equal to the handles it has just added. -/
theorem refsOk_attach (p holder : Nat) (ps : List Payload) (h : RefsOk ps) :
    RefsOk (ps.map (attachFn p holder)) := by
  intro y hy
  rw [List.mem_map] at hy
  obtain ⟨x, hx, rfl⟩ := hy
  have hx' := h x hx
  simp only [attachFn]
  split
  · simp only [List.length_append, List.length_singleton]
    omega
  · exact hx'

/-- `detach` keeps the count equal to the handles left, and touches nothing it was not asked to. -/
theorem refsOk_detach (p holder : Nat) (ps : List Payload) (h : RefsOk ps) :
    RefsOk (ps.map (detachFn p holder)) := by
  intro y hy
  rw [List.mem_map] at hy
  obtain ⟨x, hx, rfl⟩ := hy
  have hx' := h x hx
  simp only [detachFn]
  split
  · rename_i hg
    have hparts : (x.id == p) = true ∧ x.holders.contains holder = true := by
      simpa [Bool.and_eq_true] using hg
    have hmem : holder ∈ x.holders := List.contains_iff_mem.mp hparts.2
    have hlen := length_removeOne holder x.holders hmem
    simp only [Payload.refs, Payload.holders]
    omega
  · exact hx'

/-- A rename leaves both the count and the number of holders alone. -/
theorem refsOk_rename (p src dst : Nat) (ps : List Payload) (h : RefsOk ps) :
    RefsOk (ps.map (renameFn p src dst)) := by
  intro y hy
  rw [List.mem_map] at hy
  obtain ⟨x, hx, rfl⟩ := hy
  have hx' := h x hx
  simp only [renameFn]
  split
  · simp only [length_replaceHolder]
    exact hx'
  · exact hx'

/-- A length rewrite keeps the count and the holder list as they were. -/
theorem refsOk_rewrite (p len' : Nat) (ps : List Payload) (h : RefsOk ps) :
    RefsOk (ps.map (rewriteFn p len')) := by
  intro y hy
  rw [List.mem_map] at hy
  obtain ⟨x, hx, rfl⟩ := hy
  have hx' := h x hx
  simp only [rewriteFn]
  split
  · exact hx'
  · exact hx'

/-- Dropping a payload keeps the remaining ones consistent. -/
theorem refsOk_removePayload (p : Nat) (ps : List Payload) (h : RefsOk ps) :
    RefsOk (removePayload p ps) := by
  induction ps with
  | nil => intro y hy; simp [removePayload] at hy
  | cons x xs ih =>
    have hxs : RefsOk xs := fun y hy => h y (List.mem_cons_of_mem x hy)
    by_cases hdrop : (x.id == p) = true
    · have hstep : removePayload p (x :: xs) = removePayload p xs := by
        simp only [removePayload]
        rw [if_pos hdrop]
      rw [hstep]
      exact ih hxs
    · have hstep : removePayload p (x :: xs) = x :: removePayload p xs := by
        simp only [removePayload]
        rw [if_neg hdrop]
      rw [hstep]
      intro y hy
      rcases List.mem_cons.mp hy with heq | hin
      · rw [heq]; exact h x (List.mem_cons_self ..)
      · exact ih hxs y hin

/-- The identity clauses the in-place rewrites need. -/
theorem sameIdentity_attach (p holder : Nat) : SameIdentity (attachFn p holder) where
  id := by intro x; simp only [attachFn]; split <;> rfl
  value := by intro x; simp only [attachFn]; split <;> rfl
  version := by intro x; simp only [attachFn]; split <;> rfl
  kind := by intro x; simp only [attachFn]; split <;> rfl

/-- `detach` is an in-place rewrite, so it keeps the key. -/
theorem sameIdentity_detach (p holder : Nat) : SameIdentity (detachFn p holder) where
  id := by intro x; simp only [detachFn]; split <;> rfl
  value := by intro x; simp only [detachFn]; split <;> rfl
  version := by intro x; simp only [detachFn]; split <;> rfl
  kind := by intro x; simp only [detachFn]; split <;> rfl

/-- `rewrite` is an in-place rewrite, so it keeps the key. -/
theorem sameIdentity_rewrite (p len' : Nat) : SameIdentity (rewriteFn p len') where
  id := by intro x; simp only [rewriteFn]; split <;> rfl
  value := by intro x; simp only [rewriteFn]; split <;> rfl
  version := by intro x; simp only [rewriteFn]; split <;> rfl
  kind := by intro x; simp only [rewriteFn]; split <;> rfl

/-- `rename` is an in-place rewrite, so it keeps the key. -/
theorem sameIdentity_rename (p src dst : Nat) : SameIdentity (renameFn p src dst) where
  id := by intro x; simp only [renameFn]; split <;> rfl
  value := by intro x; simp only [renameFn]; split <;> rfl
  version := by intro x; simp only [renameFn]; split <;> rfl
  kind := by intro x; simp only [renameFn]; split <;> rfl

/-- In-place steps keep single residence. -/
theorem residence_map (f : Payload → Payload) (hf : SameIdentity f) (ps : List Payload)
    (h : Residence ps) : Residence (ps.map f) := by
  intro v ver
  rw [keyCount_map f hf v ver]
  exact h v ver

/-! ## Reachable states

`Op` is the alphabet of what can happen to a heap; `runAll` runs a trace; `Reachable` is membership
in the image of `runAll` from the empty heap.
-/

/-- One step: allocate a value, share it, move it, release it, mutate it, fork it, send it. -/
inductive Op where
  /-- Allocate the resident copy of value `v`, version `ver`, of `len` bytes, held by `holder`. -/
  | alloc (v ver len holder : Nat)
  /-- Share payload `p` to `holder`. -/
  | attach (p holder : Nat)
  /-- Move payload `p` from `src` to `dst`. -/
  | handOff (src dst p : Nat)
  /-- Release `holder`'s handle on payload `p`. -/
  | release (holder p : Nat)
  /-- Attempt an in-place rewrite of payload `p` to `len` bytes. -/
  | mutate (p len : Nat)
  /-- Fork payload `p` to `holder` at version `newVer`. -/
  | cowOp (holder p newVer : Nat)
  /-- Encode payload `p` once and send it to `peers`. -/
  | sendOp (p encodedLen : Nat) (peers : List Nat)

/-- Run one operation. `none` means the operation was refused, which is a real outcome: a mutation
    attempted while a payload is shared does not happen, and the heap is unchanged. -/
def Op.run : Op → Heap → Option Heap
  | .alloc v ver len holder, h =>
    if keyCount v ver h.payloads == 0 then some (h.alloc v ver len holder .value) else none
  | .attach p holder, h => h.attach p holder
  | .handOff src dst p, h => h.handOff src dst p
  | .release holder p, h => h.release holder p
  | .mutate p len, h => h.mutate p len
  | .cowOp holder p newVer, h => h.cow holder p newVer
  | .sendOp p encodedLen peers, h => h.send p encodedLen peers

/-- Run a trace. -/
def runAll (ops : List Op) (h : Heap) : Option Heap :=
  match ops with
  | [] => some h
  | op :: rest => match op.run h with
    | none => none
    | some h' => runAll rest h'

/-- A state is reachable when some trace from the empty heap produces it. -/
def Reachable (h : Heap) : Prop := ∃ ops, runAll ops empty = some h

/-- Allocating a fresh key keeps single residence: the new payload is the first of its key, because
    the guard says nothing resident carries it. -/
theorem residence_alloc (ps : List Payload) (pid v ver len holder : Nat) (h : Residence ps)
    (hfresh : keyCount v ver ps = 0) :
    Residence (ps ++ [⟨pid, len, v, ver, .value, 1, [holder]⟩]) := by
  intro v' ver'
  rw [keyCount_append_one]
  by_cases hk : v' = v ∧ ver' = ver
  · rw [hk.1, hk.2, isValueKey_self, hfresh]
    simp
  · have hne : ¬ (v = v' ∧ ver = ver') := by
      intro hc
      exact hk ⟨hc.1.symm, hc.2.symm⟩
    rw [isValueKey_other pid len v ver holder v' ver' hne]
    simpa using h v' ver'

/-- Allocation preserves the invariant. -/
theorem inv_alloc (h : Heap) (v ver len holder : Nat) (hi : Inv h)
    (hfresh : keyCount v ver h.payloads = 0) : Inv (h.alloc v ver len holder .value) := by
  have hres := residence_alloc h.payloads h.hnext v ver len holder hi.residence hfresh
  refine ⟨?_, ?_⟩
  · intro x hx
    simp only [Heap.alloc] at hx
    rcases List.mem_append.mp hx with hin | hin
    · exact hi.refs x hin
    · simp only [List.mem_singleton] at hin
      subst hin
      simp
  · simpa only [Heap.alloc] using hres

/-- Allocating an *artifact* cannot disturb single residence: an artifact is a derivation of a value,
    not a copy of it, so it is not the resident value payload of any key. -/
theorem inv_alloc_artifact (h : Heap) (v ver len holder : Nat) (hi : Inv h) :
    Inv (h.alloc v ver len holder .artifact) := by
  refine ⟨?_, ?_⟩
  · intro x hx
    simp only [Heap.alloc] at hx
    rcases List.mem_append.mp hx with hin | hin
    · exact hi.refs x hin
    · simp only [List.mem_singleton] at hin
      subst hin
      simp
  · intro v' ver'
    simp only [Heap.alloc]
    rw [keyCount_append_one, isValueKey_artifact]
    simpa using hi.residence v' ver'

/-- Sharing preserves the invariant. -/
theorem inv_attach (h h' : Heap) (p holder : Nat) (hi : Inv h) (hx : h.attach p holder = some h') :
    Inv h' := by
  simp only [Heap.attach] at hx
  split at hx
  · simp only [Option.some.injEq] at hx
    subst hx
    exact ⟨refsOk_attach p holder h.payloads hi.refs,
      residence_map _ (sameIdentity_attach p holder) _ hi.residence⟩
  · simp at hx

/-- A move preserves the invariant. -/
theorem inv_handOff (h h' : Heap) (src dst p : Nat) (hi : Inv h)
    (hx : h.handOff src dst p = some h') : Inv h' := by
  simp only [Heap.handOff] at hx
  split at hx
  · simp only [Option.some.injEq] at hx
    subst hx
    exact ⟨refsOk_rename p src dst h.payloads hi.refs,
      residence_map _ (sameIdentity_rename p src dst) _ hi.residence⟩
  · simp at hx

/-- A release preserves the invariant, in both branches: one payload leaves, or one handle does. -/
theorem inv_release (h h' : Heap) (holder p : Nat) (hi : Inv h)
    (hx : h.release holder p = some h') : Inv h' := by
  simp only [Heap.release] at hx
  split at hx
  · split at hx
    · simp only [Option.some.injEq] at hx
      subst hx
      refine ⟨refsOk_removePayload p h.payloads hi.refs, ?_⟩
      intro v ver
      exact Nat.le_trans (keyCount_removePayload_le v ver p h.payloads) (hi.residence v ver)
    · simp only [Option.some.injEq] at hx
      subst hx
      exact ⟨refsOk_detach p holder h.payloads hi.refs,
        residence_map _ (sameIdentity_detach p holder) _ hi.residence⟩
  · simp at hx

/-- An in-place mutation preserves the invariant: it changes a length, not a key or a count. -/
theorem inv_mutate (h h' : Heap) (p len' : Nat) (hi : Inv h) (hx : h.mutate p len' = some h') :
    Inv h' := by
  simp only [Heap.mutate] at hx
  split at hx
  · simp only [Option.some.injEq] at hx
    subst hx
    exact ⟨refsOk_rewrite p len' h.payloads hi.refs,
      residence_map _ (sameIdentity_rewrite p len') _ hi.residence⟩
  · simp at hx

/-- Copy-on-write preserves the invariant: the fork is a key nothing resident carried. -/
theorem inv_cow (h h' : Heap) (holder p newVer : Nat) (hi : Inv h)
    (hx : h.cow holder p newVer = some h') : Inv h' := by
  simp only [Heap.cow] at hx
  split at hx
  · rename_i hg
    simp only [Option.some.injEq] at hx
    subst hx
    refine ⟨?_, ?_⟩
    · intro y hy
      rcases List.mem_append.mp hy with hin | hin
      · exact refsOk_detach p holder h.payloads hi.refs y hin
      · simp only [List.mem_singleton] at hin
        subst hin
        simp
    · intro v' ver'
      rw [keyCount_append_one]
      have hguard : keyCount (h.valueOf p) newVer h.payloads = 0 := by
        have hparts : h.holds holder p = true ∧ decide (1 < h.refs p) = true
            ∧ (keyCount (h.valueOf p) newVer h.payloads == 0) = true := by
          simpa [Bool.and_eq_true, and_assoc] using hg
        simpa using hparts.2.2
      by_cases hk : v' = h.valueOf p ∧ ver' = newVer
      · rw [hk.1, hk.2, isValueKey_self]
        have hzero : keyCount (h.valueOf p) newVer
            (h.payloads.map (detachFn p holder)) = 0 := by
          rw [keyCount_map _ (sameIdentity_detach p holder)]
          exact hguard
        rw [hzero]
        simp
      · have hne : ¬ (h.valueOf p = v' ∧ newVer = ver') := by
          intro hc
          exact hk ⟨hc.1.symm, hc.2.symm⟩
        rw [isValueKey_other h.hnext (h.len p) (h.valueOf p) newVer holder v' ver' hne]
        have hsame : keyCount v' ver' (h.payloads.map (detachFn p holder))
            = keyCount v' ver' h.payloads := keyCount_map _ (sameIdentity_detach p holder) v' ver' _
        rw [hsame]
        simpa using hi.residence v' ver'
  · simp at hx

/-- Sharing with many consumers is just repeated sharing, so it preserves the invariant too. -/
theorem inv_attachMany (h h' : Heap) (p first n : Nat) (hi : Inv h)
    (hx : h.attachMany p first n = some h') : Inv h' := by
  induction n generalizing h with
  | zero =>
    simp only [Heap.attachMany, Option.some.injEq] at hx
    subst hx
    exact hi
  | succ k ih =>
    simp only [Heap.attachMany] at hx
    split at hx
    · simp at hx
    · rename_i hmid ha
      exact ih hmid (inv_attach h hmid p _ hi ha) hx

/-- A peer send preserves the invariant: one artifact is allocated (which is not a value payload) and
    then one handle per peer is attached to it. -/
theorem inv_send (h h' : Heap) (p encodedLen : Nat) (peers : List Nat) (hi : Inv h)
    (hx : h.send p encodedLen peers = some h') : Inv h' := by
  simp only [Heap.send] at hx
  split at hx
  · exact inv_attachMany _ h' _ _ _ (inv_alloc_artifact h _ _ _ _ hi) hx
  · simp at hx

/-- The invariant holds at every reachable state. This is the induction over the trace: each step of
    the trace is one of the preservation lemmas above. -/
theorem runAll_inv (ops : List Op) (h h' : Heap) (hx : runAll ops h = some h') (hi : Inv h) :
    Inv h' := by
  induction ops generalizing h with
  | nil =>
    simp only [runAll, Option.some.injEq] at hx
    subst hx
    exact hi
  | cons op rest ih =>
    simp only [runAll] at hx
    split at hx
    · simp at hx
    · rename_i hmid hrun
      have hstep : Inv hmid := by
        cases op with
        | alloc v ver len holder =>
          simp only [Op.run] at hrun
          split at hrun
          · rename_i hfresh
            simp only [Option.some.injEq] at hrun
            subst hrun
            exact inv_alloc h v ver len holder hi (by simpa using hfresh)
          · simp at hrun
        | attach p holder => exact inv_attach h hmid p holder hi hrun
        | handOff src dst p => exact inv_handOff h hmid src dst p hi hrun
        | release holder p => exact inv_release h hmid holder p hi hrun
        | mutate p len => exact inv_mutate h hmid p len hi hrun
        | cowOp holder p newVer => exact inv_cow h hmid holder p newVer hi hrun
        | sendOp p encodedLen peers =>
          simp only [Op.run] at hrun
          exact inv_send h hmid p encodedLen peers hi hrun
      exact ih hmid hx hstep

/-- The invariant at every reachable state, as a corollary of the trace induction. -/
theorem reachable_inv (h : Heap) (hr : Reachable h) : Inv h := by
  obtain ⟨ops, hops⟩ := hr
  have hempty : Inv empty := by
    refine ⟨?_, ?_⟩
    · intro x hx; simp [empty] at hx
    · intro v ver; simp [empty, keyCount]
  exact runAll_inv ops empty h hops hempty

/-- **The stored count is the number of handles, at every reachable state.** -/
theorem reachable_count_is_the_handles (h : Heap) (hr : Reachable h) : RefsOk h.payloads :=
  (reachable_inv h hr).refs

/-- **One resident value payload per `(value, version)`, at every reachable state.** -/
theorem reachable_one_residence_per_version (h : Heap) (hr : Reachable h) :
    Residence h.payloads := (reachable_inv h hr).residence

/-! ## The obligations

Each obligation is a theorem about the machine, and the ledger in `Ferrosa/Ownership/Obligations`
names it. The counted forms are stated over an arbitrary number of consumers where the arithmetic is
general, and as checked runs where it is a particular configuration.
-/

/-- One payload cons, one step of the count. -/
theorem refsOf_cons (p : Nat) (y : Payload) (ys : List Payload) :
    refsOf p (y :: ys) = if y.id == p then y.refs else refsOf p ys := rfl

/-- One payload cons, one step of the handle count. -/
theorem handlesOf_cons (p : Nat) (y : Payload) (ys : List Payload) :
    handlesOf p (y :: ys) = (if y.id == p then y.holders.length else 0) + handlesOf p ys := rfl

/-- The id a rewrite carries out. -/
theorem attachFn_id (p holder : Nat) (x : Payload) : (attachFn p holder x).id = x.id := by
  simp only [attachFn]; split <;> rfl

/-- The count an attach leaves on a payload it matched. -/
theorem attachFn_refs (p holder : Nat) (x : Payload) (h : (x.id == p) = true) :
    (attachFn p holder x).refs = x.refs + 1 := by
  simp [attachFn, h]

/-- Attaching raises the stored count by exactly one, for a payload that is there. -/
theorem refsOf_attachFn_self (p holder : Nat) (ps : List Payload) (hr : refsOf p ps ≠ 0) :
    refsOf p (ps.map (attachFn p holder)) = refsOf p ps + 1 := by
  induction ps with
  | nil => exact absurd rfl hr
  | cons x xs ih =>
    rw [List.map_cons, refsOf_cons, refsOf_cons]
    by_cases h : (x.id == p) = true
    · rw [attachFn_id, if_pos h, attachFn_refs p holder x h, if_pos h]
    · have hr' : refsOf p xs ≠ 0 := by
        intro hz
        apply hr
        rw [refsOf_cons, if_neg h, hz]
      simp only [attachFn_id, if_neg h]
      exact ih hr'

/-- **Sharing raises the count and leaves the payload alone.** This is the `Arc::clone` shape: one
    more handle, one more in the count, no payload bytes touched. -/
theorem attach_raises_the_reference_count (h h' : Heap) (p holder : Nat) (hi : RefsOk h.payloads)
    (hx : h.attach p holder = some h') :
    h'.refs p = h.refs p + 1 ∧ h'.len p = h.len p := by
  simp only [Heap.attach] at hx
  split at hx
  · rename_i hg
    have hparts : h.live p = true ∧ (h.holds holder p = false) := by
      simpa [Bool.and_eq_true, Bool.not_eq_true] using hg
    have hr : h.refs p ≠ 0 := by
      have hl : h.live p = true := hparts.1
      simp only [Heap.live, bne_iff_ne] at hl
      exact hl
    simp only [Option.some.injEq] at hx
    subst hx
    exact ⟨refsOf_attachFn_self p holder h.payloads hr, lenOf_map_attach p holder p h.payloads⟩
  · simp at hx

/-- A resident payload's handle count is positive, by the invariant: the count is its handle list. -/
theorem handles_pos (ps : List Payload) (p : Nat) (h : RefsOk ps) (hr : refsOf p ps ≠ 0) :
    0 < handlesOf p ps := by
  induction ps with
  | nil => exact absurd rfl hr
  | cons x xs ih =>
    by_cases hid : (x.id == p) = true
    · have hx := h x (List.mem_cons_self ..)
      have hpos : 0 < x.holders.length := by
        have hz : x.refs ≠ 0 := by
          have he : refsOf p (x :: xs) = x.refs := by simp only [refsOf, if_pos hid]
          rw [he] at hr; exact hr
        omega
      have hgoal : handlesOf p (x :: xs) = x.holders.length + handlesOf p xs := by
        simp only [handlesOf, if_pos hid]
      rw [hgoal]
      exact Nat.lt_of_lt_of_le hpos (Nat.le_add_right _ _)
    · have hr' : refsOf p xs ≠ 0 := by
        have he : refsOf p (x :: xs) = refsOf p xs := by simp only [refsOf, if_neg hid]
        rw [he] at hr; exact hr
      have hxs : RefsOk xs := fun y hy => h y (List.mem_cons_of_mem x hy)
      have hgoal : handlesOf p (x :: xs) = handlesOf p xs := by
        simp only [handlesOf, if_neg hid, Nat.zero_add]
      rw [hgoal]
      exact ih hxs hr'

/-- A move keeps every count exactly where it was. -/
theorem refsOf_renameFn (p src dst : Nat) (ps : List Payload) :
    refsOf p (ps.map (renameFn p src dst)) = refsOf p ps := by
  induction ps with
  | nil => rfl
  | cons x xs ih =>
    rw [List.map_cons, refsOf_cons, refsOf_cons]
    have hid : (renameFn p src dst x).id = x.id := by simp only [renameFn]; split <;> rfl
    rw [hid]
    by_cases h : (x.id == p) = true
    · have hrf : (renameFn p src dst x).refs = x.refs := by simp [renameFn, h]
      rw [if_pos h, hrf, if_pos h]
    · rw [if_neg h, if_neg h, ih]

/-- **After a move the sender holds nothing.** The handle list is the whole of "can be used": there
    is no second name for the value, so a use after the move has nothing to refer to. -/
theorem hand_off_leaves_the_sender_without_a_handle (h h' : Heap) (src dst p : Nat)
    (hnd : ∀ x ∈ h.payloads, x.holders.Nodup) (hne : src ≠ dst)
    (hx : h.handOff src dst p = some h') : h'.holds src p = false := by
  simp only [Heap.handOff] at hx
  split at hx
  · simp only [Option.some.injEq] at hx
    subst hx
    exact heldBy_rename_fn p src dst h.payloads hnd hne
  · simp at hx

/-- **A released payload is off the heap, with no handle left.** -/
theorem release_at_one_frees_the_payload (h h' : Heap) (holder p : Nat)
    (hx : h.release holder p = some h') (hone : h.handles p = 1) :
    h'.refs p = 0 ∧ h'.handles p = 0 ∧ p ∈ h'.freed := by
  simp only [Heap.release] at hx
  split at hx
  · have hle : h.handles p <= 1 := by omega
    rw [if_pos hle] at hx
    simp only [Option.some.injEq] at hx
    subst hx
    exact ⟨refsOf_removePayload_self p h.payloads, handlesOf_removePayload_self p h.payloads,
      List.mem_cons_self ..⟩
  · simp at hx

/-- **A handle that is gone cannot be released a second time**, so the release at zero happens once:
    the transition needs the handle it is releasing. -/
theorem release_of_an_unheld_payload_is_not_a_step (h : Heap) (holder p : Nat)
    (hnh : h.holds holder p = false) : h.release holder p = none := by
  simp only [Heap.release]
  rw [if_neg (by simpa using hnh)]

/-- Releasing never mints an id, so nothing can be resurrected through the allocator. -/
theorem release_keeps_the_allocator (h h' : Heap) (holder p : Nat)
    (hx : h.release holder p = some h') : h'.hnext = h.hnext := by
  simp only [Heap.release] at hx
  split at hx
  · split at hx
    · simp only [Option.some.injEq] at hx
      subst hx
      rfl
    · simp only [Option.some.injEq] at hx
      subst hx
      rfl
  · simp at hx

/-- Allocation always mints the next id, so an id is never handed out twice. -/
theorem alloc_mints_the_next_id (h : Heap) (v ver len holder : Nat) (k : Kind) :
    (h.alloc v ver len holder k).hnext = h.hnext + 1 := rfl

/-- **A shared payload cannot be rewritten in place.** Above one reference the mutation is not a
    transition: the machine has no state in which it happened. -/
theorem mutation_is_refused_above_one_reference (h : Heap) (p len' : Nat) (hshr : 1 < h.refs p) :
    h.mutate p len' = none := by
  have hne : ¬ ((h.refs p == 1) = true) := by
    intro hc
    have hone : h.refs p = 1 := by simpa using hc
    omega
  simp only [Heap.mutate]
  rw [if_neg hne]

/-- Under unique ownership the mutation happens, and it changes the length and nothing else. -/
theorem mutate_under_unique_ownership (h h' : Heap) (p len' : Nat)
    (hx : h.mutate p len' = some h') : h'.len p = len' ∧ h'.refs p = h.refs p := by
  simp only [Heap.mutate] at hx
  split at hx
  · rename_i hg
    have hone : h.refs p = 1 := by simpa using hg
    simp only [Option.some.injEq] at hx
    subst hx
    exact ⟨lenOf_map_rewrite_self p len' h.payloads hone, refsOf_rewriteFn p len' p h.payloads⟩
  · simp at hx

/-- **Copy-on-write publishes a new payload instead of editing the shared one.** The fork is an
    addition to the heap, and the payload every existing holder refers to is untouched — which is
    what `Arc::make_mut` does at reference count above one. -/
theorem cow_publishes_a_new_payload (h h' : Heap) (holder p newVer : Nat)
    (hx : h.cow holder p newVer = some h') :
    h'.payloads.length = h.payloads.length + 1 := by
  simp only [Heap.cow] at hx
  split at hx
  · simp only [Option.some.injEq] at hx
    subst hx
    simp only [List.length_append, List.length_singleton, List.length_map]
  · simp at hx

/-- **One payload plus `n` handles is what a fan-out costs.** The payload term does not grow with the
    number of consumers; the handle term does, and it is metadata. -/
theorem one_payload_and_n_handles_cost (len n : Nat) :
    len + (n + 1) * handleMeta = (len + n * handleMeta) + handleMeta := by
  rw [Nat.add_mul, Nat.one_mul]
  omega

/-- **Sharing beats copying for any fan-out of two or more**: `k` handles on one payload cost less
    than `k` copies of it, for a payload larger than a handle. -/
theorem sharing_beats_copying (k : Nat) (hk : 2 ≤ k) :
    handleMeta * k < 1000 * k := by
  have : handleMeta < 1000 := by decide
  exact Nat.mul_lt_mul_of_pos_right this (by omega)

/-! ### The counted forms, as runs

The arithmetic above is general; these check the shapes the ledger cites, on the configuration the
code has: a 1 000-byte payload, a 64-byte encode, and three peers.
-/

/-- One payload shared to three consumers. -/
def traceFanOut : List Op := [.alloc 7 0 1000 0, .attach 0 1, .attach 0 2, .attach 0 3]

/-- The same value sent to three peers. -/
def traceSend : List Op := [.alloc 7 0 1000 0, .sendOp 0 64 [10, 11, 12]]

/-- A send whose first peer never acknowledges: its handle is released. -/
def traceSendFailure : List Op := traceSend ++ [.release 1 1]

/-- Two handles released in one order. -/
def traceDropsAB : List Op := [.alloc 7 0 1000 0, .attach 0 1, .release 1 0, .release 0 0]

/-- The same two released in the other order. -/
def traceDropsBA : List Op := [.alloc 7 0 1000 0, .attach 0 1, .release 0 0, .release 1 0]

/-- A move from the producer to the first consumer. -/
def traceMove : List Op := [.alloc 7 0 1000 0, .handOff 0 1 0]

/-- An in-place rewrite attempted while a second handle exists. -/
def traceMutateShared : List Op := [.alloc 7 0 1000 0, .attach 0 1, .mutate 0 10]

/-- An in-place rewrite with one handle. -/
def traceMutateUnique : List Op := [.alloc 7 0 1000 0, .mutate 0 10]

/-- A fork of a shared payload. -/
def traceCow : List Op := [.alloc 7 0 1000 0, .attach 0 1, .cowOp 1 0 1]

/-- **Residency is one payload and N handles.** Three consumers: one value copy, four handles (the
    producer's and three consumers'), and 1 096 bytes — the payload plus four handles' metadata. -/
theorem a_fan_out_to_three_consumers_is_one_payload_and_four_handles :
    (runAll traceFanOut empty).map (fun h => (h.valueCopies 7, h.handles 0, h.bytes))
      = some (1, 4, 1000 + 4 * handleMeta) := by decide

/-- **A send encodes once.** Three peers, one artifact. -/
theorem a_send_to_three_peers_allocates_one_artifact :
    (runAll traceSend empty).map (fun h => h.artifacts 7) = some 1 := by decide

/-- **A send adds one handle per peer and touches the value not at all.** -/
theorem a_send_to_three_peers_holds_four_handles_on_one_artifact :
    (runAll traceSend empty).map (fun h => (h.handles 1, h.handles 0, h.valueCopies 7))
      = some (4, 1, 1) := by decide

/-- **The memory a send costs over the value is the encode buffer, once.** -/
theorem a_send_costs_the_encode_buffer_and_one_handle_per_peer :
    (runAll traceSend empty).map (fun h => h.bytes)
      = some (1000 + 64 + 5 * handleMeta) := by decide

/-- **A peer that fails costs exactly its own handle.** Releasing the failed peer's handle leaves the
    other two peers, the buffer, and the value untouched. -/
theorem a_failed_peer_send_releases_one_handle :
    (runAll traceSendFailure empty).map (fun h => (h.handles 1, h.valueCopies 7, h.refs 0))
      = some (3, 1, 1) := by decide

/-- **Drop order does not change the end state.** Two handles, released either way round, leave the
    same heap: no payload, the id recorded once, no count. -/
theorem drops_in_any_order_end_the_same_way :
    (runAll traceDropsAB empty).map (fun h => (h.payloads.length, h.freed, h.refs 0))
      = (runAll traceDropsBA empty).map (fun h => (h.payloads.length, h.freed, h.refs 0)) := by
  decide

/-- **A move leaves the sender with nothing and the receiver with the only handle.** -/
theorem a_move_leaves_the_sender_with_nothing :
    (runAll traceMove empty).map (fun h => (h.holds 0 0, h.holds 1 0, h.handles 0, h.refs 0))
      = some (false, true, 1, 1) := by decide

/-- **A mutation attempted while a payload is shared is not a transition**, so the run stops rather
    than editing the shared bytes. -/
theorem a_mutation_of_a_shared_payload_is_refused :
    runAll traceMutateShared empty = none := by decide

/-- Under unique ownership the same mutation is a step. -/
theorem a_mutation_under_unique_ownership_happens :
    (runAll traceMutateUnique empty).map (fun h => h.len 0) = some 10 := by decide

/-- **A fork publishes a second version and leaves the readers on the first.** -/
theorem a_fork_publishes_a_new_version :
    (runAll traceCow empty).map (fun h => (h.valueCopies 7, h.len 0, h.version 1, h.handles 0))
      = some (2, 1000, 1, 1) := by decide

end Ferrosa.Ownership
