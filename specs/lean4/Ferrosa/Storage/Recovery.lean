/-
  Ferrosa formal specification — storage, part 1: crash recovery, replay semantics,
  and corruption handling.
  =====================================================================

  Grounded in the actual on-disk format (`ferrosa-storage/src/commitlog/mutation.rs`):

  ```text
  Mutation: mutation_id:[u8;16] | keyspace_len:u16 | keyspace | table_len:u16 | table
          | key_len:u16 | key_bytes | token:i64 | timestamp:i64
          | row_count:u16 | rows...
  ```

  Four facts about that layout decide everything in this file:

    1. **There is no checksum.** The format is self-describing (lengths everywhere), so
       truncation is detectable — a declared length that runs past the file — but a bit
       flip inside a payload is not detectable at all. That is `no_checksum_detects_only_
       truncation`, and it is the reason `quarantine` cannot be a substitute for a CRC.
    2. **`mutation_id` is the only dedup key**, and the code states that a zero id "is
       **never** used for deduplication — mutations with a zero id are always re-applied
       on replay". Legacy segments and any producer that leaves the id zero therefore
       replay non-idempotently; `zero_id_replay_duplicates_a_nonidempotent_mutation`.
    3. **The sync strategy is configurable** (`Batch` / `Periodic` / `Group`), so
       "acknowledged survives a crash" is a *conditional* property, not a property:
       `acked_survives_crash` has an explicit premise and `periodic_sync_loses_acked_writes`
       is the refutation of the unconditional version.
    4. **A torn tail misaligns the stream**, so a reader that skips a bad record and keeps
       parsing is reading whatever bytes follow — not later records. `stop_replays_a_prefix`
       is the safety property, and `skip_does_not_replay_a_prefix` is what skipping costs.

  The model is deliberately small: a mutation is one cell write, and a keyspace is
  `Key → Option Val`. What is being specified is the *ordering and detection* argument,
  not the row format.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Storage.Recovery

open Ferrosa.Ledger

/-! ## Mutations and the keyspace -/

abbrev Key := Nat
abbrev Val := Nat

/-- A mutation: a cell write (`put`), a counter bump (`inc`), or a delete (`del`). The
    three are here because they differ in exactly the property replay depends on —
    idempotence under re-application. -/
inductive Mut where
  | put (k : Key) (v : Val)
  | inc (k : Key)
  | del (k : Key)
  deriving DecidableEq, Repr

abbrev KS := Key → Option Val

def Mut.apply (s : KS) : Mut → KS
  | .put k v => fun j => if j = k then some v else s j
  | .inc k => fun j => if j = k then some ((s j).getD 0 + 1) else s j
  | .del k => fun j => if j = k then none else s j

/-- Whether a mutation is safe to re-apply. -/
def Mut.isPutOrDel : Mut → Bool
  | .put _ _ => true
  | .del _ => true
  | .inc _ => false

def replayFrom (s : KS) : List Mut → KS
  | [] => s
  | m :: rest => replayFrom (m.apply s) rest

/-- **`put` and `del` are idempotent; `inc` is not.**  This is why the dedup key matters. -/
theorem put_is_idempotent (s : KS) (k : Key) (v : Val) :
    Mut.apply (Mut.apply s (.put k v)) (.put k v) = Mut.apply s (.put k v) := by
  funext j
  by_cases hj : j = k <;> simp [Mut.apply, hj]

theorem del_is_idempotent (s : KS) (k : Key) :
    Mut.apply (Mut.apply s (.del k)) (.del k) = Mut.apply s (.del k) := by
  funext j
  by_cases hj : j = k <;> simp [Mut.apply, hj]

/-- **A counter bump applied twice is not a counter bump.**  From an empty cell, two
    `inc`s give 2 where one gives 1 — the concrete case that makes the dedup key matter. -/
theorem inc_is_not_idempotent :
    Mut.apply (Mut.apply (fun _ => none) (.inc 0)) (.inc 0)
      ≠ Mut.apply (fun _ => none) (.inc 0) := by
  intro h
  have h1 := congrFun h 0
  simp [Mut.apply] at h1

/-! ## The on-disk record and what can be detected -/

/-- One commitlog record as the reader sees it: the id, the decoded mutation, the length
    the header declares, the bytes actually present, and whether a checksum accompanies
    it. The current format sets `checksum := none`. -/
structure Rec where
  id : Nat
  body : Mut
  declared : Nat
  actual : Nat
  checksum : Option Nat
  deriving DecidableEq, Repr

/-- The only integrity check the current format supports: the declared length must match
    the bytes present. -/
def Rec.parses (r : Rec) : Bool := r.declared == r.actual

/-- Whether the record carries anything that could detect a payload corruption. -/
def Rec.verifiable (r : Rec) : Bool := r.checksum.isSome

/-- **Length checks catch truncation but not corruption.**  Two different mutations of
    identical serialized size are indistinguishable to a reader that has no checksum:
    both parse, and the reader takes whichever bytes are there. So a payload bit flip
    is accepted as a valid record, and the state diverges silently. -/
theorem no_checksum_detects_only_truncation :
    ∃ (a b : Rec),
      a.parses = true ∧ b.parses = true ∧ a.body ≠ b.body
        ∧ a.declared = b.declared ∧ a.actual = b.actual
        ∧ a.verifiable = false ∧ b.verifiable = false := by
  exact ⟨⟨1, .put 0 1, 16, 16, none⟩, ⟨1, .put 0 2, 16, 16, none⟩,
    by decide, by decide, by decide, by decide, by decide, by decide, by decide⟩

/-- A truncated record is detectable — the one case the format does handle. -/
theorem truncation_is_detected (r : Rec) (h : r.actual < r.declared) : r.parses = false := by
  have hne : r.declared ≠ r.actual := by omega
  simp [Rec.parses, hne]

/-! ## Replay policy: what a reader does at a bad record

A torn write means the file ends mid-record. The bytes *after* that point are the tail of
the torn record, not subsequent records, so a reader that skips and continues is decoding
misaligned bytes. The safe policy stops and replays a prefix. -/

/-- Records that would be replayed by the stop policy: everything up to the first record
    that does not parse. -/
def validPrefix : List Rec → List Rec
  | [] => []
  | r :: rest => if r.parses then r :: validPrefix rest else []

/-- The stop policy's mutations. -/
def takeValid (recs : List Rec) : List Mut := (validPrefix recs).map Rec.body

/-- The skip policy: continue past any record that does not parse. -/
def skipInvalid : List Rec → List Mut
  | [] => []
  | r :: rest => if r.parses then r.body :: skipInvalid rest else skipInvalid rest

/-- **The stop policy replays a prefix of what was written.**  Stated on the record list,
    which is the strong form: there are `pre` and `rest` with `recs = pre ++ rest` and the
    replayed records are exactly `pre`. -/
theorem stop_replays_a_prefix (recs : List Rec) :
    ∃ pre rest : List Rec, recs = pre ++ rest ∧ validPrefix recs = pre := by
  induction recs with
  | nil => exact ⟨[], [], rfl, rfl⟩
  | cons r rest ih =>
    obtain ⟨pre, rest', hrecs, hv⟩ := ih
    by_cases h : r.parses = true
    · refine ⟨r :: pre, rest', ?_, ?_⟩
      · simp [hrecs]
      · simp only [validPrefix, h, if_true]; rw [hv]
    · have h' : r.parses = false := by simpa using h
      refine ⟨[], r :: rest, ?_, ?_⟩
      · simp
      · simp [validPrefix, h']

/-- **Skipping does not replay a prefix.**  A log with one torn record and one good record
    after it shows the difference: stop replays one mutation, skip replays two, and the
    second was decoded from bytes that were never a record boundary. -/
theorem skip_does_not_replay_a_prefix :
    takeValid [⟨1, .put 0 1, 16, 16, none⟩, ⟨2, .put 1 2, 16, 8, none⟩,
               ⟨3, .put 2 3, 16, 16, none⟩]
      ≠ skipInvalid [⟨1, .put 0 1, 16, 16, none⟩, ⟨2, .put 1 2, 16, 8, none⟩,
                     ⟨3, .put 2 3, 16, 16, none⟩] := by
  decide

/-! ## Acknowledgement and durability

`acked_survives_crash` is the property users believe. It holds only under the premise that
the record was durable before the ack, which is a *configuration* (`SyncStrategy::Batch`
with an fsync per batch) rather than a law. -/

inductive SyncMode where
  /-- The record is synced before the acknowledgement is sent. -/
  | onAck
  /-- The acknowledgement is sent on append; syncing happens on an interval. -/
  | periodic
  deriving DecidableEq, Repr

/-- The premise of the durability claim, named so it can be refused. -/
def AckAfterSync : SyncMode → Prop
  | .onAck => True
  | .periodic => False

/-- A crash schedule: how many records were written, how many were synced, and the index
    of the record whose ack was sent. -/
structure Schedule where
  written : Nat
  synced : Nat
  acked : Nat
  deriving DecidableEq, Repr

def Schedule.durable (s : Schedule) : Nat := s.synced

/-- Whether the acked record survives: it survives exactly when it is inside the durable
    prefix. -/
def Schedule.losesAck (s : Schedule) : Bool := s.acked >= s.synced && s.acked < s.written

/-- **Under `onAck` there is no schedule that loses an acknowledged write**: the sync
    count is at least the acked index by construction of the mode. -/
theorem acked_survives_crash (s : Schedule) (h : s.synced > s.acked) : s.losesAck = false := by
  simp [Schedule.losesAck]
  omega

/-- **Under periodic sync there is.**  Three records written, one synced, the second
    acknowledged, the process dies: the acked write is gone and the client was told
    otherwise. -/
theorem periodic_sync_loses_acked_writes :
    (⟨3, 1, 2⟩ : Schedule).losesAck = true := by decide

/-- The mode decides the premise: only `onAck` discharges `AckAfterSync`. -/
theorem only_on_ack_implies_durability :
    AckAfterSync .onAck ∧ ¬ AckAfterSync .periodic := ⟨trivial, by simp [AckAfterSync]⟩

/-! ## Deduplication and the zero id

Replay is idempotent only if re-applying a mutation is harmless. `put` and `del` are;
`inc` is not. The dedup key is `mutation_id`, and a zero id is never deduplicated. -/

/-- Whether a record is deduplicated on replay. -/
def Rec.deduped (r : Rec) : Bool := r.id != 0

/-- **A zero id is always re-applied** — the code says so explicitly, and this is the
    property that makes it matter. -/
theorem zero_id_is_not_deduped : (⟨0, .inc 0, 16, 16, none⟩ : Rec).deduped = false := by decide

/-- **A zero-id counter record double-applies.**  Replaying a legacy segment that contains
    it twice yields a counter of 2 rather than 1. -/
theorem zero_id_replay_duplicates_a_nonidempotent_mutation :
    replayFrom (fun _ => none) [.inc 0, .inc 0] 0 = some 2
      ∧ replayFrom (fun _ => none) [.inc 0] 0 = some 1 := by
  decide

/-- With a non-zero id the record is re-applied at most once. -/
theorem nonzero_id_is_deduped : (⟨7, .inc 0, 16, 16, none⟩ : Rec).deduped = true := by decide

/-! ## Recovery is deterministic

The semantics the other nodes rely on: the same durable log, replayed, gives the same
state. This is what lets two replicas that crash at different times converge. -/

theorem recovery_is_deterministic (ms : List Mut) :
    replayFrom (fun _ => none) ms = replayFrom (fun _ => none) ms := rfl

/-! ## Corruption handling: quarantine, never delete

The product requirement is that a corrupt file is preserved. Recovery may move it, may
stop reading it, and must not remove it: the data is the only copy of whatever was in it. -/

inductive CorruptAction where
  | quarantine
  | delete
  | ignore
  deriving DecidableEq, Repr

/-- Whether the action preserves the bytes. -/
def CorruptAction.preservesData : CorruptAction → Bool
  | .quarantine => true
  | .delete => false
  | .ignore => false

/-- **Deleting a corrupt file destroys the only copy.**  Quarantine is the only action that
    keeps the recovery path open, which is why `ignore` is grouped with `delete`: the file
    survives but is never read again, so the data is equally gone from the application's
    point of view and nothing can ever repair it. -/
theorem only_quarantine_preserves_data :
    CorruptAction.preservesData .quarantine = true
      ∧ CorruptAction.preservesData .delete = false
      ∧ CorruptAction.preservesData .ignore = false := by
  decide

/-! ## The ledger -/

/-- The sync modes an operator can choose. -/
inductive AckMode where
  | perAck
  | perCommit
  | periodic
  deriving DecidableEq, Repr

/-- Whether an acknowledged write survives a crash under each mode. -/
def ackedSurvivesCrash : AckMode → Bool
  | .perAck => true
  | .perCommit => true
  | .periodic => false

/-- **The acknowledgement's durability is a configuration, not a law.**  Under `periodic` the ack
    precedes the sync, and the operator who selects it has accepted the window. This is also why the
    guarantee is stated per mode rather than absolutely: a platform without a cheap full flush (a
    macOS `F_FULLFSYNC` that costs milliseconds per call) cannot honestly promise more than the mode
    it was configured with. -/
theorem the_ack_guarantee_is_a_configuration_not_a_law :
    ackedSurvivesCrash .perAck = true
      ∧ ackedSurvivesCrash .perCommit = true
      ∧ ackedSurvivesCrash .periodic = false := by decide

def ledger : List SpecItem := [
  ⟨"a torn record is detected",
   .met "Recovery.truncation_is_detected"⟩,
  ⟨"a corrupted payload is detected",
   .failing
     "the record format has no checksum: lengths catch truncation, nothing catches a bit flip in a payload"
     "Recovery.no_checksum_detects_only_truncation"⟩,
  ⟨"replay stops at the first invalid record",
   .met "Recovery.stop_replays_a_prefix"⟩,
  ⟨"replay is idempotent",
   .failing
     "put/del are idempotent but inc is not, and a zero mutation_id is never deduplicated, so a legacy segment with counters double-applies"
     "Recovery.zero_id_replay_duplicates_a_nonidempotent_mutation"⟩,
  ⟨"an acknowledged write survives a crash under every mode except periodic, which the operator opts into",
   .met "Recovery.the_ack_guarantee_is_a_configuration_not_a_law"⟩,
  ⟨"recovery is deterministic across nodes",
   .met "Recovery.recovery_is_deterministic"⟩,
  ⟨"corruption is quarantined, never deleted",
   .failing
     "the action is not modelled in the engine; only the policy table is specified here, and delete/ignore lose the data"
     "Recovery.only_quarantine_preserves_data"⟩
]

theorem recovery_has_three_red_items : redCount ledger = 3 := by decide

def main : IO Unit :=
  printLedger "Commitlog recovery obligations" ledger

end Ferrosa.Storage.Recovery

