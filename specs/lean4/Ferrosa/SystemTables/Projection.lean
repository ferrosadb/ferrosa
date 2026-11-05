/-
  The projection layer: a durable cell must equal the state that wrote it
  ======================================================================

  Every Raft system-table mutation is projected into storage as a row, and a restart rebuilds the
  in-memory state by reading those rows back.  The operator's invariant is that the two agree:
  **for every cell, the persisted row converges to the in-memory post-state of the applied log
  prefix, whatever timestamps the individual mutations carry.**

  The projection is a last-write-wins store with no version of its own.  `grant_to_row` stamps a
  permission row with `now_micros()` (`ferrosa-schema/src/system/persistence.rs:417-432`) and
  `PermissionRevoked` stamps its tombstone the same way
  (`ferrosa-cluster/src/system_table_writer.rs:164-181`); the row carries no sequence, generation or
  log index, so the only ordering information in durable state is the wall clock.  Reads then apply
  two different tie rules:

  `merge_rows_into` / `cell_supersedes` (`ferrosa-storage/src/merge.rs:137-190`) keep the higher
    timestamp, and on a tie a live cell beats a tombstone and the lexicographically greater value
    wins.  This is the read and compaction path.  The payload is `serde_json` of a `Vec` built from
    a `HashSet` (`persistence.rs:421-423`), so its element order is the hash iteration order and
    which payload is greater is not a function of the permission sets.

  `merge_into_existing_row` / `reconcile_ref` (`ferrosa-storage/src/memtable/sharded.rs:520-560`,
    `ferrosa-common/src/complex_cell.rs:63-77`) keep the higher timestamp, and on a tie a tombstone
    beats a live cell.  This is the memtable write path.

  And the reload path is not last-write-wins at all: `load_role_permissions` skips every row whose
  merged row-level deletion is not live (`ferrosa-cluster/src/system_table_loader.rs:333-335`), a
  live write never clears that deletion (`merge_rows_into` keeps the maximum), and only a later
  compaction purge drops the marker (`ferrosa-storage/src/compaction/purge.rs:119-153`).

  This module is deliberately not permission-specific.  A `Write` is one projected mutation, a
  `Cell` is one (partition, clustering) key, and the policy parameter is the merge rule the
  projection might have had:

  `timestampOnly` — the code's rule: timestamp, then the code's tie-break.

  `timestampSeq`  — timestamp, then a monotone per-cell sequence (the smallest fix for a tie).

  `seqFirst`      — per-cell sequence first, timestamp second (the durable fix for any clock).

  Visibility is a second parameter because the code has a second defect: `absorbing` is the loader's
  present rule (any row tombstone hides the cell forever), `suppressing` is timestamp-ordered
  deletion suppression (a tombstone hides only the writes it is newer than).

  A *history* is the cell's write list, newest first, which is also how the row's versions stack.
  `run` applies a history and `Cell.restart` reloads it, so `Converges` is exactly "the row after a
  restart equals the state before it".  Disproofs are single histories whose fields reduce under
  `decide`; the positive statements are proved over every history.
-/
import Ferrosa.Ledger

namespace Ferrosa.SystemTables

open Ferrosa.Ledger

/-! ## Cells, writes and the two policy parameters

A write is the pair the projection actually stores: the payload and the microsecond it was assigned.
The two inductives under it are the choices the code did not make, kept explicit so a theorem can say
which choice a property needs rather than blaming the code in general. -/

/-- A cell value: the canonical, ordered form of a permission set (the JSON byte array).  Only the
    order the merge compares is modelled, which is why a list of ids is enough. -/
abbrev Val := List Nat

/-- A row timestamp, in microseconds.  The code has no other version field. -/
abbrev Ts := Int

/-- One projected mutation of one cell.  `val = none` is a tombstone: `PermissionRevoked` writes a
    row-level deletion, so a revoke and a grant are the same two fields with a different payload. -/
structure Write where
  val : Option Val
  ts : Ts
  deriving DecidableEq, Repr

/-- A live write: `GrantUpdated`, carrying the complete post-state of the cell. -/
def live (v : Val) (ts : Ts) : Write := ⟨some v, ts⟩

/-- A tombstone: `PermissionRevoked`, and (for a whole role) `RoleDropped`. -/
def tomb (ts : Ts) : Write := ⟨none, ts⟩

/-- Whether a write carries no value. -/
def Write.isTomb (w : Write) : Bool := w.val.isNone

/-- Byte-wise comparison of two serialized payloads, as `cell_supersedes` does.  The payload's
    element order is the hash iteration order of the permission set
    (`ferrosa-schema/src/system/persistence.rs:421-423`), so the greater side is not determined by
    the sets alone. -/
def lexLt : Val → Val → Bool
  | [], [] => false
  | [], _ :: _ => true
  | _ :: _, [] => false
  | a :: as, b :: bs => if a = b then lexLt as bs else decide (a < b)

/-- The merge rule the projection might have had. -/
inductive Policy where
  /-- Timestamp only, with the code's tie-break: a live cell beats a tombstone, then the greater
      value wins (`cell_supersedes`).  No sequence is carried, so two writes in one microsecond are
      ordered by their payload. -/
  | timestampOnly
  /-- Timestamp first, a monotone per-cell sequence on an equal timestamp.  The later write wins a
      tie, but a later write with a *smaller* timestamp still loses. -/
  | timestampSeq
  /-- Per-cell sequence first, timestamp second: the newest write to a cell always wins. -/
  | seqFirst
  deriving DecidableEq, Repr

/-- What the reload path does with a row-level deletion. -/
inductive Visibility where
  /-- The loader as written: any non-live row deletion hides the cell, whatever the cells say
      (`system_table_loader.rs:333-335`, `merge_rows_into` keeps the maximum deletion). -/
  | absorbing
  /-- Timestamp-ordered deletion suppression: a tombstone only hides the writes it is newer than. -/
  | suppressing
  deriving DecidableEq, Repr

/-! ## The merge itself

`resolve` walks a cell's writes from newest to oldest.  The newest write is the candidate and an
older one displaces it only when the policy says so; the sequence number is therefore the position
in the history, which is what a per-cell sequence column would encode. -/

/-- Whether the **older** of two writes displaces the newer one under `p`. -/
def olderBeats (p : Policy) (newer older : Write) : Bool :=
  match p with
  | .timestampOnly =>
      if _ : older.ts ≠ newer.ts then decide (older.ts > newer.ts)
      else if _ : older.val.isSome ≠ newer.val.isSome then older.val.isSome
      else lexLt (newer.val.getD []) (older.val.getD [])
  | .timestampSeq => decide (older.ts > newer.ts)
  | .seqFirst => false

/-- Fold a cell's write history (newest first) to its winner. -/
def resolve (p : Policy) : Write → List Write → Write
  | cur, [] => cur
  | cur, w :: rest => resolve p (if olderBeats p cur w then w else cur) rest

/-- The winning write of a non-empty history. -/
def winningWrite (p : Policy) : List Write → Option Write
  | [] => none
  | w :: older => some (resolve p w older)

/-- What a restart reads back for one cell.  `absorbing` is the code: a row with any tombstone in
    its history is invisible.  `suppressing` lets the merge rule decide. -/
def reload (p : Policy) (vis : Visibility) (ws : List Write) : Option Val :=
  match vis with
  | .absorbing =>
      if ws.any Write.isTomb then none
      else
        match winningWrite p ws with
        | none => none
        | some w => w.val
  | .suppressing =>
      match winningWrite p ws with
      | none => none
      | some w => w.val

/-! ## The cell machine

`Cell.write` is one projected mutation: the in-memory value becomes the write's payload (the caller
derives the payload from the post-state) and the write joins the durable history.  `Cell.restart`
throws the in-memory value away and reloads it from the history, which is what a cold start does. -/

/-- One cell: its in-memory value and its durable write history, newest first. -/
structure Cell where
  mem : Option Val
  log : List Write

/-- A cell that has never been written. -/
def Cell.initial : Cell := ⟨none, []⟩

/-- Apply one projected mutation. -/
def Cell.write (c : Cell) (w : Write) : Cell := ⟨w.val, w :: c.log⟩

/-- Restart: discard the in-memory value and reload it from durable rows. -/
def Cell.restart (p : Policy) (vis : Visibility) (c : Cell) : Cell :=
  ⟨reload p vis c.log, c.log⟩

/-- Apply a history, newest first: the oldest write runs first, then each newer one. -/
def run : List Write → Cell
  | [] => Cell.initial
  | w :: rest => (run rest).write w

/-- The operator's invariant, for one merge rule and one visibility rule: after any history, a
    restart reads back exactly the value the last write left in memory. -/
def Converges (p : Policy) (vis : Visibility) : Prop :=
  ∀ ws : List Write, (Cell.restart p vis (run ws)).mem = (run ws).mem

/-- The history a cell actually has is exactly its log. -/
theorem run_log : ∀ ws : List Write, (run ws).log = ws
  | [] => rfl
  | w :: rest => by
      simp [run, Cell.write, run_log rest]

/-- The in-memory value after a history is the newest write's payload. -/
theorem run_mem : ∀ ws : List Write, (run ws).mem = ws.head?.bind Write.val
  | [] => rfl
  | _ :: _ => rfl

/-! ## Sequence first: the newest write wins, for any clock

The history's first element is the newest write, and `olderBeats .seqFirst` is `false` by
construction — a per-cell sequence compared before the timestamp never lets an older write displace
a newer one.  Convergence is then immediate and needs no clock assumption. -/

/-- A payload never beats itself under the byte comparison. -/
theorem lexLt_irrefl (v : Val) : lexLt v v = false := by
  induction v with
  | nil => rfl
  | cons a as ih => simp [lexLt, ih]

/-- If no older write beats the newest one, the newest one is the winner. -/
theorem resolve_keeps_of_no_beat (p : Policy) (cur : Write) (older : List Write)
    (h : ∀ w ∈ older, olderBeats p cur w = false) : resolve p cur older = cur := by
  induction older with
  | nil => rfl
  | cons w rest ih =>
      have hw : olderBeats p cur w = false := h w (by simp)
      have hrest : ∀ x ∈ rest, olderBeats p cur x = false :=
        fun x hx => h x (by simp only [List.mem_cons]; exact Or.inr hx)
      simp [resolve, hw]
      exact ih hrest

/-- Under `seqFirst` an older write never displaces the newest one. -/
theorem resolve_seqFirst (cur : Write) (older : List Write) :
    resolve .seqFirst cur older = cur :=
  resolve_keeps_of_no_beat .seqFirst cur older (fun _ _ => rfl)

/-- A restart of a `seqFirst` / `suppressing` cell returns the newest write's payload. -/
theorem reload_seqFirst_suppressing (w : Write) (older : List Write) :
    reload .seqFirst .suppressing (w :: older) = w.val := by
  simp [reload, winningWrite, resolve_seqFirst]

/-- **CONVERGENCE UNDER THE DURABLE POLICY.** A per-cell sequence compared before the timestamp,
    with a tombstone that only hides what it is newer than, makes the reloaded value the newest
    write's payload for every history — no assumption about the wall clock. -/
theorem converges_seqFirst_suppressing : Converges .seqFirst .suppressing := by
  intro ws
  cases ws with
  | nil => rfl
  | cons w rest =>
      show reload .seqFirst .suppressing (run (w :: rest)).log = (run (w :: rest)).mem
      rw [run_log, run_mem]
      exact reload_seqFirst_suppressing w rest

/-- **NO RESURRECTION UNDER THE DURABLE POLICY.** Whatever the newest write leaves out is absent
    after a restart, so a permission a later write removed cannot come back from an older one. -/
theorem seqFirst_suppressing_never_resurrects (w : Write) (older : List Write) (p : Nat) :
    p ∉ (w.val.getD []) → p ∉ (reload .seqFirst .suppressing (w :: older)).getD [] := by
  rw [reload_seqFirst_suppressing]
  exact id

/-! ## Timestamp first: a tie is not a tie-break for the log

With no sequence, the projection orders two writes by their payload when their timestamps agree, and
the payload's element order is the set's hash order rather than the log's.  One reachable
serialization makes the earlier, larger set win the tie, undoing the removal. -/

/-- Two live writes to one cell in one microsecond: the newer revoke leaves `[0]`, the older grant
    serialized as `[2, 0]`.  The older payload compares greater, so a restart resurrects `2`. -/
def tieLog : List Write := [live [0] 5, live [2, 0] 5]

/-- **TIE-SAFETY FAILS UNDER THE CODE'S RULE.** The later write is the one the caller committed, and
    the reloaded value is the earlier one. -/
theorem tsOnly_tie_breaks_toward_the_older_write :
    reload .timestampOnly .absorbing tieLog = some [2, 0] := by
  decide

/-- **CONVERGENCE FAILS UNDER THE CODE'S RULE.** The same history: memory holds `[0]` after the
    revoke, a restart reads `[2, 0]`. -/
theorem tsOnly_tie_fails_convergence :
    reload .timestampOnly .absorbing tieLog = some [2, 0] ∧
    (run tieLog).mem = some [0] := by
  decide

/-- **NO RESURRECTION FAILS.** Element `2` was removed by the later write and is present after the
    restart, which is the production symptom: a role loses a permission, restarts, and has it back. -/
theorem tsOnly_tie_resurrects_a_revoked_permission :
    (run tieLog).mem = some [0] ∧
    reload .timestampOnly .absorbing tieLog = some [2, 0] ∧
    2 ∈ (reload .timestampOnly .absorbing tieLog).getD [] := by
  decide

/-- **A SEQUENCE FIXES THE TIE.** `timestampSeq` never lets an equal-timestamp write displace the
    newer one, so the same history reloads as the later value. -/
theorem timestampSeq_tie_keeps_the_later_write :
    reload .timestampSeq .suppressing tieLog = some [0] ∧
    (∀ (cur : Write) (older : List Write), (∀ w ∈ older, w.ts ≤ cur.ts) →
      resolve .timestampSeq cur older = cur) := by
  refine ⟨by decide, ?_⟩
  intro cur older h
  exact resolve_keeps_of_no_beat .timestampSeq cur older
    (fun w hw => by
      unfold olderBeats
      have hgt : ¬ (w.ts > cur.ts) := Int.not_lt.mpr (h w hw)
      simp [hgt])

/-- **A SEQUENCE IS NOT ENOUGH FOR A CLOCK THAT GOES BACKWARDS.** The timestamp is still compared
    first, so a later write carrying an earlier microsecond loses.  This history has one write per
    apply and still diverges. -/
def clockBackLog : List Write := [live [0] 5, live [1] 10]

/-- **CONVERGENCE FAILS UNDER TIMESTAMP-THEN-SEQUENCE.** Memory `[0]`, reload `[1]`. -/
theorem timestampSeq_needs_a_monotone_clock :
    reload .timestampSeq .suppressing clockBackLog = some [1] ∧
    (run clockBackLog).mem = some [0] := by
  decide

/-! ## The code's own rule, and the convention it rests on

With distinct, increasing timestamps and no tombstone in the cell, `timestampOnly` does keep the
newest write, which is the case the apply path produces in the common run.  Both hypotheses are
load-bearing: the tie above violates the first, and a revoke violates the second. -/

/-- Under `timestampOnly`, an older write whose timestamp is strictly smaller never displaces the
    newer one, so increasing stamps reproduce the caller's order. -/
theorem resolve_timestampOnly_of_strictly_older (cur : Write) (older : List Write)
    (h : ∀ w ∈ older, w.ts < cur.ts) : resolve .timestampOnly cur older = cur :=
  resolve_keeps_of_no_beat .timestampOnly cur older
    (fun w hw => by
      unfold olderBeats
      have hlt := h w hw
      have hne : ¬ (w.ts = cur.ts) := Int.ne_of_lt hlt
      have hgt : ¬ (w.ts > cur.ts) := Int.not_lt.mpr (Int.le_of_lt hlt)
      simp [hne, hgt])

/-- **THE CODE CONVERGES ONLY UNDER TWO CALLER ASSUMPTIONS.** Strictly increasing per-cell
    timestamps and no tombstone in the cell: then the reload returns the newest payload.  Neither is
    checked by the projection; the first is the wall clock's and the second is the caller's choice
    of mutation. -/
theorem tsOnly_converges_when_the_clock_is_strictly_increasing
    (w : Write) (older : List Write)
    (hts : ∀ o ∈ older, o.ts < w.ts)
    (hno : (w :: older).any Write.isTomb = false) :
    reload .timestampOnly .absorbing (w :: older) = w.val := by
  simp [reload, hno, winningWrite, resolve_timestampOnly_of_strictly_older w older hts]

/-! ## The loader's row tombstone is absorbing

`load_role_permissions` does not compare a deletion against the cells it shadows; it drops the whole
row whenever the merged deletion is not live, and no later live write clears that deletion.  A
re-grant after a revoke-all is therefore invisible to the next boot until a compaction purge removes
the marker, which needs `gc_grace` to have elapsed. -/

/-- A revoke-all, then a re-grant at a strictly later timestamp: memory `[0, 1]`, durable `none`. -/
def burialLog : List Write := [live [0, 1] 30, tomb 20, live [0] 10]

/-- **NO RE-GRANT SURVIVES THE LOADER'S TOMBSTONE.** Even the strongest merge rule cannot help,
    because the visibility rule discards the row before the merge is consulted. -/
theorem absorbing_tombstone_buries_a_later_grant :
    reload .seqFirst .absorbing burialLog = none ∧
    (run burialLog).mem = some [0, 1] := by
  decide

/-- **NO MERGE RULE SAVES AN ABSORBING TOMBSTONE.** For every policy the reload is empty while the
    state is not, so convergence needs the visibility rule changed as well as the ordering. -/
theorem no_policy_survives_absorption (p : Policy) :
    reload p .absorbing burialLog ≠ (run burialLog).mem := by
  cases p <;> decide

/-! ## Independence, replay, and the apply-path convention

Three properties that do not depend on the tie-break at all: a write touches only its own cell, a
replayed row is not a new version, and the "one mutation per cell per apply" rule the apply arms
happen to satisfy.  The third is where the convention lives, and the theorem below shows it is not
sufficient on its own. -/

/-- A table of cells, keyed as the projection keys rows: one entry per (partition, clustering). -/
abbrev Table := Nat → List Write

/-- Write one cell.  The row is keyed by the cell, so this touches no other key. -/
def Table.write (t : Table) (k : Nat) (w : Write) : Table :=
  fun k' => if k' = k then w :: t k' else t k'

/-- **NO COLLATERAL.** A write to one cell leaves every other cell's durable history, and therefore
    its reloaded value, unchanged.  This mirrors the writer's per-resource clustering
    (`ferrosa-cluster/src/system_table_writer.rs:159-181`). -/
theorem no_collateral (p : Policy) (vis : Visibility) (t : Table) (k k' : Nat) (w : Write)
    (h : k' ≠ k) :
    reload p vis ((t.write k w) k') = reload p vis (t k') := by
  simp [Table.write, h]

/-- An equal-timestamp write never displaces its own copy. -/
theorem olderBeats_self (p : Policy) (w : Write) : olderBeats p w w = false := by
  cases p with
  | timestampOnly =>
      unfold olderBeats
      have hne : ¬ (w.ts ≠ w.ts) := by simp
      have hne2 : ¬ (w.val.isSome ≠ w.val.isSome) := by simp
      simp [hne, hne2, lexLt_irrefl]
  | timestampSeq =>
      unfold olderBeats
      have hgt : ¬ (w.ts > w.ts) := Int.lt_irrefl w.ts
      simp [hgt]
  | seqFirst => rfl

/-- A duplicated newest write is not a competing version. -/
theorem resolve_duplicate (p : Policy) (w : Write) (rest : List Write) :
    resolve p w (w :: rest) = resolve p w rest := by
  simp [resolve, olderBeats_self]

/-- A duplicated newest write does not change whether the cell carries a tombstone. -/
theorem any_tomb_duplicate (w : Write) (rest : List Write) :
    (w :: w :: rest).any Write.isTomb = (w :: rest).any Write.isTomb := by
  cases h : w.isTomb <;> simp [List.any_cons, h]

/-- **REPLAY IDEMPOTENCY.** Re-projecting the row that is already the newest version is a no-op on
    the durable state, under every policy and both visibility rules: a crash that replays the last
    mutation does not change what the next restart reads. -/
theorem replay_same_row_is_a_noop (p : Policy) (vis : Visibility) (w : Write) (rest : List Write) :
    reload p vis (w :: w :: rest) = reload p vis (w :: rest) := by
  cases vis with
  | absorbing => simp [reload, winningWrite, resolve_duplicate]
  | suppressing => simp [reload, winningWrite, resolve_duplicate]

/-- One apply's writes for one cell. -/
abbrev Apply := List Write

/-- The apply path's convention: at most one mutation for a cell per apply.  The grant and revoke
    arms satisfy it (`state_machine.rs:1883-1947`), but nothing in the projection checks it. -/
def OnePerApply (t : List Apply) : Prop := ∀ a ∈ t, a.length ≤ 1

/-- Flatten a trace of applies (newest first) into the cell's write history. -/
def flatten (t : List Apply) : List Write := t.flatten

/-- **THE CONVENTION IS NECESSARY BUT NOT SUFFICIENT.** A trace with one write per apply still fails
    to converge when the clock hands the later apply a smaller timestamp, so "one mutation per cell
    per apply" cannot be the reason the code is safe. -/
theorem one_per_apply_is_insufficient :
    ∃ t : List Apply, OnePerApply t ∧
      reload .timestampOnly .absorbing (flatten t) ≠ (run (flatten t)).mem := by
  refine ⟨[[live [0] 5], [live [1] 10]], ?_, ?_⟩
  · intro a ha
    simp only [List.mem_cons, List.not_mem_nil, or_false] at ha
    rcases ha with rfl | rfl <;> simp
  · simp [flatten]
    decide

/-! ## The two tie rules are not the same rule

The memtable applies `reconcile_ref` (`ferrosa-common/src/complex_cell.rs:63-77`), the read and
compaction merge applies `cell_supersedes` (`ferrosa-storage/src/merge.rs:177-190`), and they choose
opposite winners on an equal-timestamp live/tombstone pair.  Which one answers therefore depends on
whether the row is still in the memtable or has reached an SSTable. -/

/-- The memtable's tie rule: on an equal timestamp the **tombstone** wins. -/
def memtableBeats (newer older : Write) : Bool :=
  if _ : older.ts ≠ newer.ts then decide (older.ts > newer.ts)
  else if _ : newer.val.isSome ≠ older.val.isSome then !older.val.isSome
  else lexLt (newer.val.getD []) (older.val.getD [])

/-- **THE SAME PAIR, TWO WINNERS.** An older live write and a newer tombstone at one microsecond:
    the read merge lets the live cell win (a value beats a tombstone), the memtable lets the
    tombstone win. -/
theorem two_tie_rules_disagree :
    olderBeats .timestampOnly (tomb 5) (live [0] 5) = true ∧
    memtableBeats (tomb 5) (live [0] 5) = false := by
  decide

/-! ## The ledger

Twelve obligations on the projection layer: convergence under each policy, tie-safety, resurrection,
the two independent properties (no collateral, replay idempotency), the loader's tombstone, the
apply-path convention, and the disagreement between the two merge rules.  Seven are unmet, each
naming the single history that refutes it.

The verdict the table records is that the operator's invariant is a real requirement, not a quirk of
grants, and that the code meets it only by convention: timestamp-only ordering plus a caller that
keeps one write per cell per apply, never tombstones a cell it will write again, and happens to hand
out increasing microseconds.  The weakest policy that makes every row hold for any timestamps is
`seqFirst` with `suppressing` visibility: a monotone per-cell sequence compared before the timestamp,
and a tombstone that hides only the writes it is newer than.
-/

def ledger : List SpecItem := [
  ⟨"a cell's persisted row converges to its in-memory value under the code's merge rule",
   .failing
     "there is no sequence or generation column: grant_to_row stamps now_micros() (ferrosa-schema/src/system/persistence.rs:417-432) and PermissionRevoked does the same (ferrosa-cluster/src/system_table_writer.rs:164-181), while cell_supersedes orders an equal timestamp by the greater payload (ferrosa-storage/src/merge.rs:177-190), and that payload is serde_json of a HashSet-derived Vec (persistence.rs:421-423), so its order is the hash order. Two live writes to one cell in one microsecond can therefore resolve to the earlier write, so a restart reads back a permission set the state machine no longer holds. One apply emits at most one write per cell (state_machine.rs:1883-1947), but two applies can share a microsecond and the projection has nothing else to order them by"
     "SystemTables.tsOnly_tie_fails_convergence"⟩,
  ⟨"a timestamp with a per-cell sequence converges for every clock",
   .failing
     "timestampSeq still compares the timestamp first, so a later write whose now_micros() is smaller than the write it supersedes loses to it. SystemTime is not monotone (an NTP step, a suspended VM, two nodes' clocks), and nothing on the projection path derives the timestamp from the Raft log index, so a later apply can carry an earlier timestamp and a restart then restores the superseded value"
     "SystemTables.timestampSeq_needs_a_monotone_clock"⟩,
  ⟨"a per-cell sequence compared before the timestamp converges for every clock",
   .met "SystemTables.converges_seqFirst_suppressing"⟩,
  ⟨"two writes to one cell with equal timestamps: the later one wins",
   .failing
     "with no sequence the tie is broken by the serialized payload (ferrosa-storage/src/merge.rs:177-190), not by which write the log applied later. That payload is serde_json of a HashSet-derived Vec (ferrosa-schema/src/system/persistence.rs:421-423), so its element order is the hash iteration order: a removal can lose to the set it shortened, and which side wins is not a function of the permission sets at all. The fix's structural rule (one mutation per cell per apply, state_machine.rs:1883-1947) removes the intra-apply pair but not a tie between two applies, and it does not make the order the log's"
     "SystemTables.tsOnly_tie_breaks_toward_the_older_write"⟩,
  ⟨"a per-cell sequence makes the later write win an equal timestamp",
   .met "SystemTables.timestampSeq_tie_keeps_the_later_write"⟩,
  ⟨"a revoked permission is not resurrected by an equal-timestamp write",
   .failing
     "the later revoke writes the remaining set and the earlier grant wrote the larger set; at an equal timestamp cell_supersedes keeps the greater payload (ferrosa-storage/src/merge.rs:177-190), which can be the earlier one because the payload's order is the set's hash order (ferrosa-schema/src/system/persistence.rs:421-423), so the removed permission is present again after a restart and the role's authorization answers change with nothing logged and nothing failing"
     "SystemTables.tsOnly_tie_resurrects_a_revoked_permission"⟩,
  ⟨"a revoked permission is not resurrected under a per-cell sequence",
   .met "SystemTables.seqFirst_suppressing_never_resurrects"⟩,
  ⟨"writing one cell leaves every other cell unchanged",
   .met "SystemTables.no_collateral"⟩,
  ⟨"replaying the same row is a no-op on the durable state",
   .met "SystemTables.replay_same_row_is_a_noop"⟩,
  ⟨"a cell re-granted after a full revoke is visible again",
   .failing
     "PermissionRevoked writes a row-level deletion (ferrosa-cluster/src/system_table_writer.rs:164-181) and load_role_permissions skips every row whose merged deletion is not live (ferrosa-cluster/src/system_table_loader.rs:333-335). merge_rows_into keeps the maximum deletion and merge_into_existing_row only raises it (ferrosa-storage/src/merge.rs:137-143, ferrosa-storage/src/memtable/sharded.rs:549), so a later live write never clears the marker; only a compaction purge after gc_grace drops it (ferrosa-storage/src/compaction/purge.rs:119-153). A revoke followed by a grant inside that window is lost on the next boot, and the direct DDL path writes the same tombstone for a partial revoke while its in-memory schema keeps the remainder (ferrosa-cluster/src/ddl_path.rs:444-467), so single-node mode loses the remaining permissions the same way"
     "SystemTables.absorbing_tombstone_buries_a_later_grant"⟩,
  ⟨"convergence does not rest on a caller convention",
   .failing
     "the apply arms happen to emit one mutation per cell (state_machine.rs:1883-1947), which is a property of three match arms, not of the projection: system_table_writer.rs has no per-cell guard or version, and REVOKE emits one Raft operation per permission (ferrosa-cql/src/router.rs:14140-14167), so one statement writes the same cell in consecutive applies. A new mutation kind that emits two writes for a cell re-opens the tie, and the convention does not help a clock that goes backwards"
     "SystemTables.one_per_apply_is_insufficient"⟩,
  ⟨"the write path and the read path agree on an equal-timestamp tie",
   .failing
     "the memtable applies reconcile_ref, where a tombstone wins an equal-timestamp tie (ferrosa-common/src/complex_cell.rs:63-77, called from ferrosa-storage/src/memtable/sharded.rs:556-585), while the read and compaction merge applies cell_supersedes, where a live cell wins (ferrosa-storage/src/merge.rs:137-190). The same pair of versions resolves to opposite winners depending on whether the row is still in the memtable or has reached an SSTable, so the durable outcome also depends on flush timing"
     "SystemTables.two_tie_rules_disagree"⟩
]

/-- Twelve obligations on the projection, seven of them unmet. -/
theorem system_table_projection_has_twelve_items_and_seven_red :
    ledger.length = 12 ∧ redCount ledger = 7 := by
  decide

def main : IO Unit :=
  printLedger "the system-table projection" ledger

/-! ## The axiom audit

Every disproof is a finite computation, so it reports no axioms at all; the positive theorems use
`simp` and `omega` over an arbitrary history, which report `propext` and `Quot.sound`.  None reports
`sorryAx`.
-/

#print axioms run_log
#print axioms run_mem
#print axioms lexLt_irrefl
#print axioms resolve_keeps_of_no_beat
#print axioms resolve_seqFirst
#print axioms reload_seqFirst_suppressing
#print axioms converges_seqFirst_suppressing
#print axioms seqFirst_suppressing_never_resurrects
#print axioms tsOnly_tie_breaks_toward_the_older_write
#print axioms tsOnly_tie_fails_convergence
#print axioms tsOnly_tie_resurrects_a_revoked_permission
#print axioms timestampSeq_tie_keeps_the_later_write
#print axioms timestampSeq_needs_a_monotone_clock
#print axioms resolve_timestampOnly_of_strictly_older
#print axioms tsOnly_converges_when_the_clock_is_strictly_increasing
#print axioms absorbing_tombstone_buries_a_later_grant
#print axioms no_policy_survives_absorption
#print axioms no_collateral
#print axioms olderBeats_self
#print axioms resolve_duplicate
#print axioms any_tomb_duplicate
#print axioms replay_same_row_is_a_noop
#print axioms one_per_apply_is_insufficient
#print axioms two_tie_rules_disagree
#print axioms system_table_projection_has_twelve_items_and_seven_red

end Ferrosa.SystemTables
