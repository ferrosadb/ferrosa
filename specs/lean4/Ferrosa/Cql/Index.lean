/-
  Ferrosa formal specification — CQL front end, part 7: secondary indexes.
  =====================================================================

  A secondary index is a *derived* view, and it is maintained by a background build
  over SSTables rather than inside the row write (`ferrosa-storage/src/index/`:
  `scheduler`, `tracker`, `remote_backend`).  That makes index reads subject to a
  readiness question the row path does not have, and the coordinator's answer is
  `select_index_ready_replicas` (`ferrosa-cluster/src/coordinator/read.rs:51`), whose
  own doc comment says:

      "Reorder replicas so that nodes with [`IndexNodeStatus::Ready`] ... come
       first. ... Returns all replicas (never filters)."

  So the function is a **preference**, not a guarantee, and this file says exactly
  that in machine-checked form:

    * `reorder_never_filters` — the selection has the same members as the replica
      set, so a replica building the index is still eligible to serve the query;
    * `ready_replicas_come_first` — the preference is real, just not a filter;
    * `preference_is_not_completeness` — there are replica sets where the selection
      returns exactly the input and nothing is ready, which is the "all building"
      case the Rust test `select_index_ready_replicas_all_building_returns_all`
      pins down;
    * `indexed_read_can_be_stale` — a read served this way can miss a row that a
      preceding committed write put there, and that history is not serializable.

  Index *entries*, once built, are keyed by `(value, primary key)`, so maintaining
  them is a set operation — commutative and idempotent (`index_entries_commute`,
  reusing the collections model), which is why index maintenance is not itself a
  lost-update hazard.  The hazard is the lag, not the merge.
-/
import Ferrosa.Cql.Collections
import Ferrosa.Accord.Serializable

set_option linter.deprecated false
set_option linter.unusedSimpArgs false

namespace Ferrosa.Cql.Index

open Ferrosa
open Ferrosa.Accord

abbrev Node := Nat

/-- The readiness of one replica for one index, mirroring `IndexNodeStatus`. -/
inductive IdxStatus where
  | ready
  | building
  | unknown
  deriving DecidableEq, Repr

/-- Prefer replicas whose index build has finished; keep everyone else in the
    original order.  This is `select_index_ready_replicas` transcribed. -/
def selectIndexReady (replicas : List Node) (st : Node → IdxStatus) : List Node :=
  replicas.filter (fun r => st r == .ready) ++ replicas.filter (fun r => !(st r == .ready))

/-! ## What the reordering does and does not promise -/

/-- **The selection never filters.**  Stated as membership, which is what "never
    filters" means: a replica is in the selection exactly when it is in the input,
    including when it is still building the index. -/
theorem reorder_never_filters (replicas : List Node) (st : Node → IdxStatus) (r : Node) :
    r ∈ selectIndexReady replicas st ↔ r ∈ replicas := by
  unfold selectIndexReady
  simp only [List.mem_append, List.mem_filter, beq_iff_eq]
  constructor
  · rintro (h | h) <;> exact h.left
  · intro hmem
    by_cases hr : st r = .ready
    · exact Or.inl ⟨hmem, hr⟩
    · exact Or.inr ⟨hmem, by simp [beq_iff_eq, hr]⟩

/-- The first segment really is the ready set — the preference is not a fiction. -/
theorem first_segment_is_ready (replicas : List Node) (st : Node → IdxStatus) (r : Node)
    (h : r ∈ replicas.filter (fun r => st r == .ready)) : st r = .ready := by
  simp only [List.mem_filter, beq_iff_eq] at h
  exact h.2

/-- **The all-building case**, mirroring the Rust test of the same name: when nobody
    has finished the build, the selection returns the input unchanged. -/
theorem all_building_returns_all :
    selectIndexReady [0, 1] (fun _ => .building) = [0, 1] := by decide

/-- A query result is complete for an index only if every replica that serves it has
    finished building that index. -/
def Complete (serving : List Node) (st : Node → IdxStatus) : Bool :=
  serving.all (fun r => st r == .ready)

/-- **Preference is not completeness.**  There is a replica set for which the
    selection returns the input unchanged and nothing is ready: the reordering has
    done all it can do, and the caller has no signal that the answer may be
    incomplete. -/
theorem preference_is_not_completeness :
    ∃ (replicas : List Node) (st : Node → IdxStatus),
      selectIndexReady replicas st = replicas ∧ Complete replicas st = false :=
  ⟨[0, 1], fun _ => .building, all_building_returns_all, by decide⟩

/-- The positive direction: if every replica is ready, the selection is complete —
    so the `Ready` fast path is sound, and the gap is confined to the degraded case. -/
theorem all_ready_is_complete (replicas : List Node) :
    Complete (selectIndexReady replicas (fun _ => .ready)) (fun _ => .ready) = true := by
  unfold Complete selectIndexReady
  simp [List.filter]

/-! ## The history this admits -/

/-- A row is committed, and then an indexed read on a replica whose index is still
    building does not find it.  `k = 0` was written (`append 0 7`) and the read that
    should match the index entry returns `[]`. -/
def hIndexedStale : History := ⟨[
  invEv 0 1 [.append 0 7] 0,
  okEv 0 1 [.append 0 7] [] 0 10,
  invEv 1 2 [.read 0] 20,
  okEv 1 2 [.read 0] [[]] 20 30]⟩

/-- **Not serializable.**  A read that follows a committed write and does not see it
    has no serial explanation — the indexed read cannot be placed after the write it
    should have observed.  Index readiness is therefore a *correctness* premise for
    indexed reads, not a performance hint. -/
theorem indexed_read_can_be_stale : strictlySerializableB hIndexedStale = false := by
  native_decide

/-- The same history with the row visible is serializable, so the checker is
    discriminating rather than rejecting everything. -/
def hIndexedFresh : History := ⟨[
  invEv 0 1 [.append 0 7] 0,
  okEv 0 1 [.append 0 7] [] 0 10,
  invEv 1 2 [.read 0] 20,
  okEv 1 2 [.read 0] [[7]] 20 30]⟩

theorem indexed_read_when_fresh_is_serializable : strictlySerializableB hIndexedFresh = true := by
  native_decide

/-! ## Index entries themselves merge safely -/

/-- An index partition holds a set of `(value, primary key)` entries, so maintaining
    it is `setAdd`, not an append: two concurrent inserts of different entries
    commute and neither is lost (`setAdd_comm`, `setAdd_idem`).  The index's problem
    is lag, not merge. -/
theorem index_entries_commute (s : Collections.SetVal) (a b : Val) :
    Collections.setAdd (Collections.setAdd s a) b
      = Collections.setAdd (Collections.setAdd s b) a :=
  Collections.setAdd_comm s a b

/-- A **removal** is not a union: it needs to know what to remove, which is a read
    of the old row.  So deleting an index entry — the update path, where the row's
    old value is stale — is a read-modify-write again. -/
def setRemove (s : Collections.SetVal) (v : Val) : Collections.SetVal :=
  fun w => s w && !(w == v)

theorem setRemove_not_comm_with_add :
    ∃ (s : Collections.SetVal) (a b : Val),
      setRemove (Collections.setAdd s a) b ≠ Collections.setAdd (setRemove s b) a := by
  refine ⟨Collections.setEmpty, 1, 1, ?_⟩
  intro h
  have h1 := congrFun h 1
  simp [setRemove, Collections.setAdd, Collections.setEmpty] at h1

end Ferrosa.Cql.Index
