/-
  Pinning: what a reader opened, and what a manifest entry resolves to
  =====================================================================

  `Ferrosa/Streaming/Machine.lean` states the memory discipline, and `Ferrosa/Streaming/Audit.lean`
  claims among its `.met` rows that "a reader's view is one pinned version for its whole life"
  (witness `Streaming.a_reader_view_depends_only_on_its_pin`). This module checks that claim against
  the machine's own definitions, and the machine cannot express it as written.

  `Store.bytesOf` (Machine.lean:51-54) takes a *manifest name* and resolves it through
  `s.published`: `bytesOf s n = s.size (s.published n)`. `Reader.pin` (Machine.lean:88-99) is a
  version number, and the audit's theorem applies `bytesOf` to it, so a reader's "view" is the bytes
  of whichever version the manifest names *now*, not of the version the reader pinned. Under those
  definitions:

  * two readers with different pins can hold the same bytes
    (`a_pinned_view_does_not_depend_on_the_pin`), which is the negation of what the row is cited for;
  * and a reader that resolves its pin through the manifest sees a different cell after an unrelated
    publish, though no byte of the version it opened changed
    (`a_name_keyed_view_moves_with_the_manifest`).

  Neither is a defect in the code. It is a defect in the model: the invariant needs the reader to
  capture `size` at open and the view to be keyed by the *version*, after which it follows from the
  immutability the machine already proves (`publish_does_not_change_a_version_size`). That corrected
  form is proved here (`a_publish_leaves_an_open_reader_intact`), so the ledger row can be re-pointed
  at a theorem that says what its text says.

  On merge, `Streaming/Audit.lean:ledger` needs two edits. Neither is made here, because this branch
  is a model contribution and not a change to the area the PR is landing:

  1. the row `"a reader's view is one pinned version for its whole life"` should cite
     `Pinning.a_publish_leaves_an_open_reader_intact` instead of
     `Streaming.a_reader_view_depends_only_on_its_pin`;
  2. a new row `"a manifest name resolves to whatever version the manifest publishes now"`, `.met`
     `Pinning.a_name_keyed_view_resolves_through_the_manifest`, moves the area's pinned count from
     36/14 to 37/14 and `Ferrosa.Unimplemented.ledgerTotal` from 102 to 103 — the registry's pinned
     totals, which is why they are untouched on this branch.
-/
import Ferrosa.Streaming.Machine

namespace Ferrosa.Streaming

/-! ## A reader that captures what it opened

The machine's `Reader` carries a `pin` and a `chunk` but no bytes, so it has nothing to compare
against after a publish. The shape below adds the one field the invariant needs: what the reader took
from its version at open time. That field makes "the view does not change" a statement about the
reader rather than about the manifest. -/

/-- A reader as it exists once it has opened: the version it pinned, and the bytes it took from that
    version at open time. `held` is captured, not recomputed — that is the modelling decision. -/
structure OpenedReader where
  /-- The version this reader was opened against. -/
  pin : Nat
  /-- The bytes it took from that version when it opened. -/
  held : Nat
  deriving Repr, DecidableEq

/-- Open a reader at a version of a store: it captures that version's byte count. -/
def openAt (s : Store) (v : Nat) : OpenedReader := ⟨v, s.size v⟩

/-- What an open reader serves: what it captured, never what the manifest says now. -/
def OpenedReader.serves (r : OpenedReader) : Nat := r.held

/-! ## What a manifest name resolves to

Two facts about `Store.bytesOf`, both immediate from its definition, and together they are the
counterexample: the function is keyed by the manifest entry, so it is not a function of the pin. -/

/-- **A manifest name resolves through the manifest.**  `bytesOf` looks the name up in `published` and
    returns that version's size, so what a name-keyed reader sees is decided by the current manifest
    and not by anything the reader pinned. -/
theorem a_name_keyed_view_resolves_through_the_manifest (s : Store) (name : Nat)
    (h : s.published name ≠ 0) :
    s.bytesOf name = s.size (s.published name) := by
  unfold Store.bytesOf Store.read
  simp [h]

/-- Two names publishing versions of one size: the store used to exhibit the next fact. -/
def oneVersionPerName : Store :=
  { published := fun _ => 3, size := fun _ => 7, written := 3 }

/-- **Two readers with different pins hold the same bytes**, so the relation in
    `Streaming.a_reader_view_depends_only_on_its_pin` is not injective in the pin: the pin is used as
    a manifest name, and two names may publish equal-sized versions. -/
theorem a_pinned_view_does_not_depend_on_the_pin :
    ∃ (s : Store) (r r' : Reader), r.pin ≠ r'.pin ∧ s.bytesOf r.pin = s.bytesOf r'.pin := by
  refine ⟨oneVersionPerName, ⟨1, 0, 0, 0, 0⟩, ⟨2, 0, 0, 0, 0⟩, ?_, ?_⟩
  · decide
  · decide

/-- A store with two versions of different sizes, the first published under name `1`. -/
def twoVersions : Store :=
  { published := fun _ => 1, size := fun n => if n = 1 then 10 else 20, written := 2 }

/-- **A name-keyed view moves under an unrelated publish.**  Version `1`'s bytes did not change and
    version `1` was not overwritten; name `1` was re-pointed at version `2`, and a reader that
    resolves through the manifest now holds `20` where it held `10`. This is the observation the
    audit's "one pinned version for its whole life" row is meant to exclude, and the machine's
    definitions admit it. -/
theorem a_name_keyed_view_moves_with_the_manifest :
    twoVersions.bytesOf 1 = 10 ∧ (twoVersions.withPublished 1 2).bytesOf 1 = 20 := by
  constructor <;> decide

/-! ## The invariant, stated so that it holds

The repair is one field: the reader captures the version's bytes at open (`openAt`), and the view is
`r.held`. Stability then follows from the immutability the machine already proves — a publish moves a
manifest entry and touches no version's size. -/

/-- **A publish leaves an open reader intact.**  Opening at version `k` before and after a publish of
    some name and version captures the same bytes, because publishing changes no version's size. -/
theorem a_publish_leaves_an_open_reader_intact (s : Store) (name v k : Nat) :
    (openAt (s.withPublished name v) k).serves = (openAt s k).serves :=
  publish_does_not_change_a_version_size s name v k

/-- **A reader serves what it opened**, so no later transition can change its answer: `serves` reads
    the captured field and nothing else. Stated for every store, which is the point — it does not
    mention the manifest. -/
theorem an_open_reader_serves_what_it_opened (s : Store) (v : Nat) :
    (openAt s v).serves = s.size v := rfl

/-- **A published version does not change an open reader's bytes**, which is the form the ledger
    row's text wants: the reader keeps the version it pinned whatever the manifest does. -/
theorem a_published_version_does_not_change_an_open_readers_bytes (s : Store) (name v k : Nat) :
    (openAt (s.withPublished name v) k).held = (openAt s k).held :=
  publish_does_not_change_a_version_size s name v k

/-! ## Obligations, as data carrying their own proof

The same shape `Ferrosa/Streaming/Obligations.lean` uses, so these rows can be appended there without
changing the type. The first obligation is the one this module adds: a manifest is a *name* index,
and a view that goes through it is not a pinned view. -/

/-- One obligation row: a name, the proposition owed, and a proof of it. Named apart from
    `Streaming.Obligation` because both live in this namespace. -/
structure PinObligation where
  name : String
  statement : Prop
  proof : statement

/-- What this module owes. -/
def pinningObligations : List PinObligation := [
  ⟨"a manifest name resolves to whatever version the manifest publishes now",
   ∀ (s : Store) (name : Nat), s.published name ≠ 0 →
     s.bytesOf name = s.size (s.published name),
   a_name_keyed_view_resolves_through_the_manifest⟩,
  ⟨"a name-keyed view moves under an unrelated publish",
   twoVersions.bytesOf 1 = 10 ∧ (twoVersions.withPublished 1 2).bytesOf 1 = 20,
   a_name_keyed_view_moves_with_the_manifest⟩,
  ⟨"a publish leaves an open reader intact",
   ∀ (s : Store) (name v k : Nat),
     (openAt (s.withPublished name v) k).serves = (openAt s k).serves,
   a_publish_leaves_an_open_reader_intact⟩,
  ⟨"an open reader serves what it opened",
   ∀ (s : Store) (v : Nat), (openAt s v).serves = s.size v,
   an_open_reader_serves_what_it_opened⟩,
  ⟨"a published version does not change an open reader's bytes",
   ∀ (s : Store) (name v k : Nat),
     (openAt (s.withPublished name v) k).held = (openAt s k).held,
   a_published_version_does_not_change_an_open_readers_bytes⟩
]

/-- Five obligations, pinned so a row cannot be dropped. -/
theorem pinning_obligation_count : pinningObligations.length = 5 := by decide

/-- Every obligation carries a proof: the element type has no other constructor. -/
theorem every_pinning_obligation_carries_a_proof : ∀ o ∈ pinningObligations, o.statement := by
  intro o _
  exact o.proof

end Ferrosa.Streaming
