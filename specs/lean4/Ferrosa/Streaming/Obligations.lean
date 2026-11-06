/-
  The obligations, as data that cannot exist without its proof
  ===========================================================

  `Ferrosa/Streaming/Machine.lean` proves thirty-odd facts about the model. This module states the ones
  the operator asked for as a list, and it does so in the only form that cannot rot: an `Obligation`
  carries a `statement : Prop` and a `proof : statement`, so a row can be constructed only by exhibiting
  the proof. There is no "status" field to set to `met`, and no way to add a row for a claim nobody has
  discharged.

  That is deliberately different from an area ledger. The ledger is the corpus's claim list, and a claim
  there may be red because the CODE does not meet it. These obligations are about the machine: every
  one of them is proved, and the audit module is what records which code paths do and do not correspond.
  Keeping the two apart is what stops "the model is proved" from reading as "the code conforms".
-/
import Ferrosa.Streaming.Machine

namespace Ferrosa.Streaming

/-- One obligation: a name, the proposition owed, and a proof of it. The proof field is not optional, so
    an obligation with no discharge cannot be written down. -/
structure Obligation where
  /-- The claim, in the words the audit refers to it by. -/
  name : String
  /-- What is owed, as a proposition. -/
  statement : Prop
  /-- The discharge. -/
  proof : statement

/-- The obligations of the memory machine: bounded reads, early abandon, immutability, atomic transform,
    no in-place edit, stream failure, the permit, the queue, and the spill. -/
def obligations : List Obligation := [
  ⟨"a read changes nothing in the store",
   ∀ (s : Store) (name : Nat), run s (.read name) = .ok s,
   a_read_changes_nothing⟩,
  ⟨"an abandoned reader changes nothing in the store",
   ∀ (s : Store), run s .abandon = .ok s,
   an_abandon_changes_nothing⟩,
  ⟨"a spill changes nothing durable",
   ∀ (s : Store) (bytes : Nat), run s (.spill bytes) = .ok s,
   a_spill_changes_nothing⟩,
  ⟨"a streaming reader's resident set does not depend on the result size",
   ∀ (r : Reader), r.resident = r.chunk + r.fixed,
   a_streaming_reader_holds_the_same_bytes_for_any_result⟩,
  ⟨"no constant bounds a materialising reader",
   ∀ (c width : Nat), 0 < width → ∃ rows, c < materialisedResident rows width,
   no_constant_bounds_a_materialising_reader⟩,
  ⟨"a reader's view depends only on its pin",
   ∀ (s : Store) (r r' : Reader), r.pin = r'.pin → s.bytesOf r.pin = s.bytesOf r'.pin,
   a_reader_view_depends_only_on_its_pin⟩,
  ⟨"abandoning a reader leaves no work queued",
   ∀ (r : Reader), r.abandon.remaining = 0,
   abandoning_a_reader_leaves_no_work⟩,
  ⟨"abandoning a reader releases its chunk",
   ∀ (r : Reader), r.releasedResident = r.fixed,
   abandoning_releases_the_chunk⟩,
  ⟨"a failed transform leaves the published manifest readable",
   ∀ (s : Store) (t : Transform) (name : Nat), t.permitted s = false →
     run s (.transform t name) = .refused s
       "transform refused: output not fresh, input unwritten, or bytes unverified",
   a_failed_transform_leaves_the_manifest⟩,
  ⟨"a successful transform publishes exactly one manifest entry",
   ∀ (s : Store) (t : Transform) (name : Nat), t.permitted s = true →
     run s (.transform t name) = .ok { (s.withPublished name t.out) with written := t.out },
   a_successful_transform_publishes_one_entry⟩,
  ⟨"a transform whose output is not fresh is refused",
   ∀ (s : Store) (t : Transform), s.written ≥ t.out → t.permitted s = false,
   a_stale_output_is_refused⟩,
  ⟨"a transform whose bytes are unverified is refused",
   ∀ (s : Store) (t : Transform), t.verified = false → t.permitted s = false,
   an_unverified_transform_is_refused⟩,
  ⟨"an in-place edit changes what a pinned reader sees",
   ∀ (s : Store) (v old new : Nat), s.size v = old → old ≠ new →
     (s.editInPlace v new).size v ≠ s.size v,
   a_pinned_view_changes_under_an_in_place_edit⟩,
  ⟨"a publish moves one manifest entry and no other",
   ∀ (s : Store) (name v other : Nat), other ≠ name →
     (s.withPublished name v).published other = s.published other,
   publish_moves_one_manifest_entry⟩,
  ⟨"a publish does not change the size of any version",
   ∀ (s : Store) (name v k : Nat), (s.withPublished name v).size k = s.size k,
   publish_does_not_change_a_version_size⟩,
  ⟨"at most the permit limit holds a permit",
   ∀ (p : Permits), p.held ≤ p.limit → p.acquire.held ≤ p.limit,
   at_most_the_limit_hold_a_permit⟩,
  ⟨"a refusal is not a grant",
   ∀ (p : Permits), ¬ p.held < p.limit → p.acquire.isGranted = false,
   a_refusal_is_not_a_grant⟩,
  ⟨"a bounded queue never exceeds its capacity",
   ∀ (q : Queue), q.length ≤ q.capacity → q.push.length ≤ q.capacity,
   a_bounded_queue_stays_within_its_capacity⟩,
  ⟨"an unbounded queue is the leak",
   ∀ (produced retired : Nat), retired ≤ produced →
     unboundedOccupancy produced retired + retired = produced,
   an_unbounded_queue_is_the_leak⟩,
  ⟨"a spilling operator's resident set is bounded by its threshold",
   ∀ (o : Operator) (rows width : Nat), o.hasSpill = true → o.spills = true →
     o.resident rows width ≤ o.bound,
   a_spilling_operator_is_bounded⟩,
  ⟨"a spill path nothing calls is not a bound",
   ∀ (o : Operator) (rows width : Nat), o.spills = false →
     o.resident rows width = rows * width,
   a_spill_nothing_calls_is_not_a_bound⟩,
  ⟨"at the bound the next step is a spill",
   ∀ (bound resident : Nat), bound ≤ resident → stepAt resident bound = .spill,
   at_the_bound_the_next_step_is_a_spill⟩,
  ⟨"continuing in memory past the bound breaks it",
   ∀ (o : Operator) (rows width : Nat), o.bound < rows * width →
     o.bound < continuedInMemory rows width,
   continuing_in_memory_breaks_the_bound⟩,
  ⟨"a collected result defeats the spill bound",
   ∀ (rows bound : Nat), bound < rows → bound < collectedResult rows,
   a_collected_result_defeats_the_bound⟩,
  ⟨"a spill without disk admission fails midway",
   ∀ (free need : Nat), free < need → spillAdmitted free need = false,
   an_unadmitted_spill_fails_midway⟩,
  ⟨"a spill file is not durable data and is not visible",
   spillRun.inStore = false ∧ spillRun.visible = false,
   a_spill_is_not_durable_data⟩,
  ⟨"a crash mid-spill leaves nothing a reader can see",
   spillRun.visible = false,
   a_crash_mid_spill_is_invisible⟩,
  ⟨"reconciliation clears an orphan rather than adopting it",
   spillRun.adopted = false,
   reconciliation_clears_orphans⟩,
  ⟨"a silent in-memory fallback after a spill failure exceeds the bound",
   ∀ (rows width bound : Nat), bound < rows * width →
     bound < SpillPolicy.resident .fallbackToRam rows width bound,
   a_silent_fallback_exceeds_the_bound⟩,
  ⟨"a reordering stage with no spill is proportional to its input",
   ∀ (rows width bound : Nat), reorderingStageResident rows width false bound = rows * width,
   a_reordering_stage_without_spill_is_proportional⟩
]

/-- The names of the obligations, in list order. -/
def obligationNames : List String := obligations.map (·.name)

/-- Thirty obligations, pinned so a discharge cannot be dropped from the list without this failing. -/
theorem obligation_count : obligations.length = 30 := by decide

/-- **Every obligation in the list carries a proof.**  Not a corollary of a status field: the list's
    element type has no constructor without one, so this theorem records the type's shape rather than a
    property that could have been violated. -/
theorem every_obligation_carries_a_proof : ∀ o ∈ obligations, o.statement := by
  intro o _
  exact o.proof

/-- Obligation names are distinct, so an audit row cannot refer to two obligations by one name. -/
theorem obligation_names_are_distinct : obligationNames.Nodup := by decide

end Ferrosa.Streaming
