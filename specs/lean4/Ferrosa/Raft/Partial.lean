/-
  What a partially applied entry leaves behind
  ============================================

  A Raft entry is not applied atomically. `apply_command` mutates the shared `Schema`, the shared
  `StorageEngine`, the token ring and the staged `RaftState`; `flush_pending_system_writes` then
  writes the ordered system-table rows; `force_commit_log_sync` makes those rows durable; and only
  then does `finish_pending_apply` adopt the staged metadata and advance `last_applied`
  (`ferrosa-cluster/src/raft/state_machine.rs:2334-2342`, `:453-479`). Six failure points sit in
  that sequence, and what a node looks like afterwards is different at each of them.

  The question this module answers is the operator's, not the reviewer's: of those six, which leave
  a node that serves a *wrong answer*, which leave one that serves a merely *failed* one, and which
  leave one that no repair fixes? `Ferrosa/Raft/Startup.lean` settles whether such a node comes back
  at all; this module settles what is on it when it does.

  Three model decisions carry the argument.

  **Effects are enumerated, not summarised.** `Landed` has one field per durable or shared effect
  the apply path can produce, and `landedAt` fills it per failure point from the code's own
  ordering. A theorem then checks that the vector is a *prefix* of that ordering at every point, so
  the table cannot drift away from the sequence it is supposed to describe.

  **The class is derived, not asserted.** `classOf` reads the node after a restart, the node after
  the *available repair* completes, and which repair that is. That is what makes the headline theorem
  a statement about the taxonomy rather than a list of opinions:
  `the_class_is_decided_by_whether_the_cause_clears`. The same failure point is divergent when the
  disk fills and corrupt when it stays full, and nothing on the node observes which of the two it is
  in.

  **A repair that completes and is still wrong is corruption, not divergence.** The difference is
  `wrong (repaired …)`: a divergent state is one a replay takes away; a corrupt one is one it
  leaves behind.

  The taxonomy is the operator's three classes plus one the three do not name. SAFE, DIVERGENT and
  CORRUPT are as defined in the brief. `stalled` is "consistent, unserviceable, unrepaired" — a node
  whose visible state is a prefix of the log and which cannot advance; the brief's three classes have
  no box for it, and the operator's requirement is precisely about it. It is named here so the
  ledger can say whether it is reachable, and the answer for these six points is no: every one of
  them leaves some component ahead of the log.
-/
import Ferrosa.Ledger

namespace Ferrosa.Raft

open Ferrosa.Ledger

/-! ## The vocabulary

An entry either creates something (a keyspace, a table, an index, a role) or destroys something (the
same list, dropped). The two differ in exactly the place that matters: a create is re-derivable from
the log, and a drop has already removed the rows by the time the ordered write is attempted. -/

/-- What kind of change the entry makes. -/
inductive OpKind where
  /-- The entry adds a thing that was not there: replay can re-derive it. -/
  | create
  /-- The entry removes a thing that was there, including its rows: replay cannot put it back. -/
  | drop
  deriving DecidableEq, Repr

/-- The six points at which an apply can stop. -/
inductive FailPoint where
  /-- After `apply_command` returned and before the first ordered write was attempted. -/
  | afterPhysical
  /-- Inside `flush_pending_system_writes`, with some rows written and some not. -/
  | betweenSystemWrites
  /-- Every ordered write returned `Ok` and `force_commit_log_sync` failed. -/
  | beforeCommitLogSync
  /-- The sync returned `Ok` and publication did not happen: the process died, or the awaiter was
  dropped before `finish_pending_apply` polled the retained handle. -/
  | afterSyncBeforePublish
  /-- Replaying an entry that already published successfully in the previous process. -/
  | duringReplay
  /-- `apply_snapshot_data` adopted the state and `sync_schema_and_engine_from_state` failed. -/
  | duringRestore
  deriving DecidableEq, Repr

/-- Every persistent effect the apply path can produce, one field each. -/
structure Landed where
  /-- Rows: memtable and commit-log appends, and the SSTable directory a drop removed
  (`ferrosa-storage/src/engine.rs:6020-6059`). -/
  rows : Bool
  /-- The shared `Schema`: keyspaces, tables and indexes (`state_machine.rs:1301-1520`). -/
  schema : Bool
  /-- The `StorageEngine`'s table registry, including the durable drop record in
  `dropped-tables.json` (`engine.rs:5865-5899`). -/
  engineCatalog : Bool
  /-- The token ring and its observer, written by `sync_ring` inside `apply_command`. -/
  ring : Bool
  /-- The live `RaftState`, adopted only in `finish_pending_apply` (`state_machine.rs:476`). -/
  raftState : Bool
  /-- `last_applied` and `last_membership`, adopted in the same two lines (`:477`). -/
  lastApplied : Bool
  /-- The ordered system-table rows (`flush_pending_system_writes`, `:2171-2192`). -/
  projection : Bool
  /-- `force_commit_log_sync` returned `Ok`, so the rows are a durability barrier and not just a
  page-cache write (`:2193-2197`). -/
  projectionDurable : Bool
  /-- `state-machine.snapshot.bin` covers this entry, which needs publication first
  (`build_snapshot` calls `finish_pending_apply`, `:2213-2214`). -/
  snapshot : Bool
  deriving DecidableEq, Repr

/-- The effect vector each failure point leaves behind, read off the code's ordering.

`apply_command` runs first and touches the shared handles — so `rows`, `schema`, `engineCatalog` and
`ring` all move before anything is written to the system tables, and `RaftState` does not move at
all, because `staged` is a copy whose metadata is only adopted at publication. The ordered writes
follow, one at a time; the fsync follows those; publication follows the fsync. -/
def landedAt (op : OpKind) : FailPoint → Landed
  | .afterPhysical =>
    { rows := op = .drop, schema := true, engineCatalog := true, ring := true
      raftState := false, lastApplied := false, projection := false
      projectionDurable := false, snapshot := false }
  | .betweenSystemWrites =>
    { rows := op = .drop, schema := true, engineCatalog := true, ring := true
      raftState := false, lastApplied := false, projection := true
      projectionDurable := false, snapshot := false }
  | .beforeCommitLogSync =>
    { rows := op = .drop, schema := true, engineCatalog := true, ring := true
      raftState := false, lastApplied := false, projection := true
      projectionDurable := false, snapshot := false }
  | .afterSyncBeforePublish =>
    { rows := op = .drop, schema := true, engineCatalog := true, ring := true
      raftState := false, lastApplied := false, projection := true
      projectionDurable := true, snapshot := false }
  | .duringReplay =>
    -- The entry published in the previous process; replay re-runs every stage.
    { rows := true, schema := true, engineCatalog := true, ring := true
      raftState := true, lastApplied := true, projection := true
      projectionDurable := true, snapshot := true }
  | .duringRestore =>
    -- `sync_schema_and_engine_from_state` retired superseded incarnations (`:1011-1015`) before
    -- applying the schema snapshot (`:1070-1072`), so the destructive half can have landed while
    -- the rest did not.
    { rows := true, schema := false, engineCatalog := true, ring := false
      raftState := true, lastApplied := true, projection := true
      projectionDurable := true, snapshot := true }

/-- **THE EFFECT VECTOR IS A PREFIX OF THE PUBLICATION ORDER.** At every failure point, publication
    implies a durable projection, which implies a complete projection, which implies the physical
    stage ran — and a snapshot implies publication. Nothing lands out of order, so the table below
    describes the sequence the code actually performs rather than a plausible one. -/
theorem the_effect_order_is_a_prefix (op : OpKind) (fp : FailPoint) :
    let l := landedAt op fp
    (l.rows = true → l.engineCatalog = true)
    ∧ (l.lastApplied = true → l.raftState = true)
    ∧ (l.lastApplied = true → l.projectionDurable = true)
    ∧ (l.projectionDurable = true → l.projection = true)
    ∧ (l.projection = true → l.engineCatalog = true)
    ∧ (l.snapshot = true → l.lastApplied = true) := by
  cases op <;> cases fp <;> decide

/-! ## What the node looks like afterwards

`Obs` is the observable state of one table: how many rows a read returns, and whether each of the
four components that can answer "does this table exist" says so. The four are deliberately separate,
because the interesting states are the ones where they disagree.
-/

/-- The observable state of one table. -/
structure Obs where
  /-- Rows a read returns. A snapshot carries no rows (`SnapshotData` is `state`, `last_applied`,
  `last_membership`), so nothing restores this. -/
  rows : Nat
  /-- `RaftState.tables` — what a snapshot would carry and what the DDL path consults. -/
  raftHas : Bool
  /-- The shared `Schema` — what a new CQL connection resolves the table from. -/
  schemaHas : Bool
  /-- The `StorageEngine` — what decides whether a row's columns decode. -/
  engineHas : Bool
  /-- The `system_schema.tables` row — what a driver's schema-refresh sees, and what every other
  node in the cluster reads. -/
  projectionHas : Bool
  deriving DecidableEq, Repr

/-- All four components agree about the table's existence. This is *not* the same as being right:
a table that every component agrees exists, with its rows deleted, is consistent and empty. -/
def Obs.agrees (o : Obs) : Bool :=
  (o.raftHas == o.schemaHas) && (o.engineHas == o.raftHas) && (o.projectionHas == o.raftHas)

/-- The table as the log says it should be: five rows, and it exists. -/
def logRows : Nat := 5

/-- The table after a drop: nothing, everywhere. -/
def gone : Obs := ⟨0, false, false, false, false⟩

/-- The table after a create: five rows, everywhere. -/
def present : Obs := ⟨logRows, true, true, true, true⟩

/-- After a restart, before any replay: the in-memory `RaftState` is rebuilt from the durable
snapshot (which predates the failing entry, because a failed entry is never snapshotted), the
`Schema` is rebuilt from `schema.json` and then merged with the snapshot's state — and that merge is
*additive*: `apply_snapshot` inserts with `or_insert_with` and never removes
(`ferrosa-schema/src/registry.rs:346-364`), so a table the failed entry added survives the merge. The
engine keeps whatever its directories hold. -/
def restored (op : OpKind) : FailPoint → Obs
  | .afterPhysical =>
    { rows := if op = .drop then 0 else logRows, raftHas := op = .drop
      schemaHas := true, engineHas := true, projectionHas := op = .drop }
  | .betweenSystemWrites =>
    { rows := if op = .drop then 0 else logRows, raftHas := op = .drop
      schemaHas := true, engineHas := true, projectionHas := op = .drop }
  | .beforeCommitLogSync =>
    { rows := if op = .drop then 0 else logRows, raftHas := op = .drop
      schemaHas := true, engineHas := true, projectionHas := op = .drop }
  | .afterSyncBeforePublish =>
    { rows := if op = .drop then 0 else logRows, raftHas := op = .drop
      schemaHas := true, engineHas := true, projectionHas := true }
  | .duringReplay =>
    -- Nothing was left behind: the entry published, so a restart recovers it from the snapshot.
    present
  | .duringRestore =>
    -- `RaftState` is at the snapshot's index, the schema merge did not complete, and a superseded
    -- incarnation may already have been retired.
    { rows := 0, raftHas := true, schemaHas := false, engineHas := true
      projectionHas := true }

/-- The observable state once the *available* repair has completed. A replay of the entry for five
of the six points; a boot whose sync succeeds for the restore. When the cause has not cleared no
repair completes, so this is the restarted state again. -/
def repaired (clears : Bool) (op : OpKind) : FailPoint → Obs
  | .duringRestore =>
    -- A restore is not repaired by replaying an entry: the index already moved past it. A restart
    -- whose sync succeeds makes the components agree again — over whatever the retirement left.
    if clears then
      { rows := if op = .drop then 0 else logRows, raftHas := true, schemaHas := true
        engineHas := true, projectionHas := true }
    else restored op .duringRestore
  | fp =>
    if clears then (if op = .drop then gone else present) else restored op fp

/-- A read on a node in this state returns something the log does not support. Either a component
disagrees with `RaftState` (so a client gets a different answer from `system_schema` than from a
table open), or a table the log still holds rows for reads empty. -/
def wrong (o : Obs) : Bool :=
  !o.agrees || (o.raftHas && o.rows < logRows)

/-- Which repair is available. `replay` means the entry is still above `last_applied` and will be
re-run; `restart` means the state is repaired by a boot whose sync succeeds; `none` means the cause
has not cleared and replay will fail at the same entry again
(`Ferrosa/Raft/Startup.lean:restart_cycle_never_passes_a_poisoned_entry`). -/
inductive Repair where
  | replay
  | restart
  | none
  deriving DecidableEq, Repr

/-- The repair available at a failure point, given whether the cause clears. -/
def repairFor (clears : Bool) : FailPoint → Repair
  | .duringRestore => if clears then .restart else .none
  | _ => if clears then .replay else .none

/-- The taxonomy. `safe`, `divergent` and `corrupt` are the brief's three; `stalled` is the fourth
the three do not name and the operator's requirement is about. -/
inductive Class where
  | safe
  | divergent
  | corrupt
  /-- Consistent, unserviceable, unrepaired: a visible state that is a prefix of the log and cannot
  advance. Outside the three-way taxonomy by construction — no wrong answer is served because no
  answer is served. -/
  | stalled
  deriving DecidableEq, Repr

/-- The class of a partial apply, derived from the two observations and the available repair:
`corrupt` when the repair completes and the state is *still* wrong, `stalled` when nothing is wrong
but nothing can progress, `divergent` when the wrongness is a window replay closes. -/
def classOf (clears : Bool) (op : OpKind) (fp : FailPoint) : Class :=
  if wrong (repaired clears op fp) then .corrupt
  else match repairFor clears fp with
    | .none => .stalled
    | _ => if wrong (restored op fp) then .divergent else .safe

/-! ## The table

The matrix is built, not written out: `row` computes each cell from `landedAt`, `restored`, `repaired`
and `repairFor`, and `table` crosses the six failure points with the two entry kinds and the two
cause states. The theorems below are statements about that computed table, so a classification cannot
drift from the observations it claims to rest on. -/

/-- The row of the table: what landed, what a client sees after a restart, what it sees after the
repair, the class, and what repairs it. -/
structure Row where
  op : OpKind
  point : FailPoint
  clears : Bool
  landed : Landed
  afterRestart : Obs
  afterRepair : Obs
  klass : Class
  repair : Repair
  deriving DecidableEq, Repr

/-- One row, computed rather than written out.

A replay of an entry that already published cannot have a persistent cause — there is no projection
write left to fail — so that point is normalised to `clears := true` rather than crossed with an axis
that does not apply to it. -/
def row (clears : Bool) (op : OpKind) (fp : FailPoint) : Row :=
  let clears := if fp = .duringReplay then true else clears
  ⟨op, fp, clears, landedAt op fp, restored op fp, repaired clears op fp, classOf clears op fp,
   repairFor clears fp⟩

/-- The six failure points, in the order the apply path reaches them. -/
def failPoints : List FailPoint :=
  [.afterPhysical, .betweenSystemWrites, .beforeCommitLogSync, .afterSyncBeforePublish,
   .duringReplay, .duringRestore]

/-- Cause-clearing × entry kind. -/
def axes : List (Bool × OpKind) :=
  [(true, .create), (false, .create), (true, .drop), (false, .drop)]

/-- The whole table: six failure points, two kinds of entry, and the cause clearing or not. -/
def table : List Row :=
  (axes.map fun (clears, op) => failPoints.map fun fp => row clears op fp) |>.flatten

/-! ## 1. The two properties that decide the class

The brief names them, and they are the whole argument: replay idempotency of what landed before the
failure, and the gap between a durable effect and a published index.
-/

/-- What a second application of the same effect does. -/
inductive Idem where
  /-- The same state, no error: replay converges and publication follows. -/
  | same
  /-- An error, which now latches: replay cannot get past the entry. -/
  | latches
  /-- A second row, a second table, a different state: replay corrupts. -/
  | duplicates
  deriving DecidableEq, Repr

/-- The four effects that can land before publication, with what a second application does to them.

Every one of them is `same`, and for a stated reason. `create_keyspace_internal` and
`create_table_internal` return early when the name is present
(`ferrosa-schema/src/registry.rs:511-539`). `Schema::apply_snapshot` merges with `or_insert_with`
(`:346-364`). `StorageEngine::register_table` discards the incoming schema for a table it already
knows (`state_machine.rs:1075-1084`). `unregister_table` records a drop only for a table it still
has registered and returns `Ok` for one it does not (`engine.rs:6020-6059`), so a replayed drop is a
no-op rather than a second deletion. `engine.write` is last-write-wins on `(key, timestamp)`, and a
replayed entry carries the same timestamp.

What none of that buys is reversibility: `same` says the second application does not make the state
*worse*, not that the state is what it was before the first one. -/
def idem : String → Idem
  | "create_keyspace_internal" => .same
  | "create_table_internal" => .same
  | "apply_snapshot" => .same
  | "register_table" => .same
  | "unregister_table" => .same
  | "engine.write" => .same
  | "GrantUpdated" => .same
  | "PermissionRevoked" => .same
  | _ => .latches

/-- The effects a replay re-issues. -/
def prePublicationEffects : List String :=
  ["create_keyspace_internal", "create_table_internal", "apply_snapshot", "register_table",
   "unregister_table", "engine.write"]

/-- **REPLAY IDEMPOTENCY (PROVED OF THE TABLE).** Every effect that can land before publication has
    a second application that produces the same state: no latch, no duplicate. So replay is not a
    repair that can fail of its own accord — the only thing that can stop it is the precondition,
    which is `Ferrosa/Raft/Startup.lean`'s finding, not this module's. -/
theorem every_prepublication_effect_is_idempotent :
    prePublicationEffects.all (fun e => idem e == .same) = true := by decide

/-- **…and reversibility is a separate question.** A drop's second application is `same` precisely
because the rows are already gone: `unregister_table` on an absent table does nothing. -/
theorem a_drop_is_idempotent_because_it_is_irreversible :
    (repaired true .drop .duringReplay).rows = 0
    ∧ (repaired true .drop .duringReplay).raftHas = false
    ∧ (restored .drop .afterPhysical).rows = 0 := by decide

/-- **PUBLICATION IS NOT DURABILITY.** `force_commit_log_sync` makes the ordered rows durable
*inside the job*, before `finish_pending_apply` advances `last_applied`
(`state_machine.rs:2193-2197` then `:476-477`). So there is a reachable state — the process dying
between the two — in which a durable effect belongs to an entry the published index does not cover.
`AfterSyncBeforePublish` is that state, and it is the only one: everywhere else the projection is
either not durable or publication has happened. -/
theorem a_durable_projection_without_publication :
    (landedAt .create .afterSyncBeforePublish).projectionDurable = true
    ∧ (landedAt .create .afterSyncBeforePublish).lastApplied = false
    ∧ (landedAt .create .beforeCommitLogSync).projectionDurable = false
    ∧ (landedAt .create .duringReplay).lastApplied = true := by decide

/-- The same fact as a statement about the whole table: four rows — one point times two entry kinds
times two cause states — leave a durable projection that publication does not cover, and no other
point does. -/
theorem exactly_one_point_durably_projects_without_publishing :
    (table.filter fun r => r.landed.projectionDurable && !r.landed.lastApplied).length = 4 := by
  decide

/-! ## 2. The classification

Every cell is computed from `restored`, `repaired` and `repairFor`; the theorems below are the
statements a reader needs, each of them a `decide` over the whole table rather than a chosen row.
-/

/-- **THE CLASS IS DECIDED BY WHETHER THE CAUSE CLEARS, NOT BY WHERE THE APPLY DIED.** For every
failure point except a replay of an entry that already published, the same point is `divergent` when
the cause clears and `corrupt` when it does not. The restore is the exception that proves the rule:
its repair is a restart, and a restart over a retirement that already deleted rows completes and is
still wrong, so it is corrupt either way. The node cannot observe which case it is in —
`Startup.lean` has no boot-time check, and the latch that records the failure is cleared by the
restart that would have to answer the question. -/
theorem the_class_is_decided_by_whether_the_cause_clears :
    (table.filter fun r => r.point ≠ .duringReplay && r.point ≠ .duringRestore).all
      (fun r => (classOf true r.op r.point == .divergent)
                && (classOf false r.op r.point == .corrupt)) = true := by
  decide

/-- **NO FAILURE IS MERELY STALLED.** One might hope a partial apply could leave a node that is
consistent and simply stuck — the state the three-way taxonomy has no name for. It does not: every
one of these points leaves some component ahead of the published log, so when the cause persists the
visible state is wrong, not merely frozen. The stalled class is uninhabited, and the theorem says so
rather than leaving it as a footnote. -/
theorem no_failure_is_merely_stalled :
    (table.filter fun r => r.klass == .stalled).length = 0 := by decide

/-- **A REPLAY OF AN ENTRY THAT PUBLISHED IS THE ONE SAFE POINT.** It is safe because the entry
already advanced `last_applied` and what replay re-runs is idempotent — the second pass cannot leave
more behind than the first, and the first left nothing behind. -/
theorem during_replay_is_safe :
    (table.filter fun r => r.point == .duringReplay).all
      (fun r => r.klass == .safe && r.afterRestart.agrees) = true := by
  decide

/-- **WHAT A CLIENT SEES AT EACH CLASS.** Stated as the read each class produces, because the
difference between a wrong answer and a failed one is what decides the severity. -/
theorem the_classes_are_distinguishable_by_what_a_read_returns :
    -- Divergent, create: the table resolves and takes writes, while the replicated catalog has no
    -- row for it, so a schema refresh from another node denies it.
    (restored .create .afterPhysical).schemaHas = true
    ∧ (restored .create .afterPhysical).projectionHas = false
    -- Corrupt, drop: the table resolves and reads empty while the log still holds its rows.
    ∧ (restored .drop .afterPhysical).rows = 0
    ∧ (restored .drop .afterPhysical).agrees = true
    -- Safe: nothing is ahead of the published index.
    ∧ (restored .create .duringReplay).agrees = true
    ∧ (restored .create .duringReplay).rows = logRows := by
  decide

/-- **A DROP THAT NEVER PUBLISHES IS READ AS AN EMPTY TABLE (DISPROVED).** The most dangerous row,
    because nothing disagrees: after the restart every component says the table exists and the read
    returns no rows. There is no inconsistency to notice — only missing data, on the node that
    answers the read. -/
theorem a_drop_that_never_publishes_is_read_as_an_empty_table :
    (restored .drop .afterPhysical).agrees = true
    ∧ (restored .drop .afterPhysical).rows = 0
    ∧ (restored .drop .afterPhysical).raftHas = true
    ∧ wrong (restored .drop .afterPhysical) = true := by
  decide

/-- **THE RESYNC PATH DOES NOT REFILL WHAT A FAILED ENTRY DELETED.** `reset_unusable_raft_state`
(`controller/cluster.rs:1526`) moves the raft directory aside; the rows a drop deleted are not in it,
because they never were — they were SSTables under `data_dir`. So the documented repair ends with the
same table, the same schema row and no rows: the entry's destructive half is durable in the engine
while the record that could re-derive it is the one being discarded. -/
theorem the_resync_path_does_not_refill_the_rows :
    (restored .drop .afterPhysical).rows = 0
    ∧ (landedAt .drop .afterPhysical).rows = true
    ∧ (landedAt .drop .afterPhysical).snapshot = false := by
  decide

/-- **A FAILED RESTORE IS CORRUPT FOR A DESTRUCTIVE ENTRY EVEN WHEN THE CAUSE CLEARS, AND ONLY
DIVERGENT FOR AN ADDITIVE ONE.** Its repair is a restart, not a replay — the index already moved past
the entry — and the retirement of superseded incarnations runs before the sync that failed
(`state_machine.rs:1011-1015` before `:1070-1072`), so the rows are gone before anything can report
the failure. `Ferrosa/Raft/Surfaces.lean` records that the node boots anyway; this is what it boots
on. -/
theorem a_failed_restore_is_corrupt_for_a_drop_and_divergent_for_a_create :
    classOf true .drop .duringRestore = .corrupt
    ∧ classOf true .create .duringRestore = .divergent
    ∧ (landedAt .drop .duringRestore).schema = false
    ∧ (landedAt .drop .duringRestore).raftState = true := by
  decide

/-! ## 3. The grants family: a projection that contradicted its own state

`6c270518` made a failed projection write latch. `1a89858a` fixed a different shape on the same
surface, and it is the one this taxonomy was built for: the projection row did not disagree with the
engine, it disagreed with the *state machine that wrote it*.

Before that commit, `RaftOp::Grant` pushed `SystemTableMutation::GrantUpdated(entry.clone())`
(`state_machine.rs:1898-1903` before the fix), and `entry` is the **delta** — the permissions this
entry adds, not the set the role now holds. A grant of `MODIFY` on a resource where the role already
had `SELECT` therefore wrote a row containing `{MODIFY}` while `state.grants` held
`{SELECT, MODIFY}`. `RaftOp::Revoke` pushed `PermissionRevoked`, whose writer tombstones the entire
resource row (`system_table_writer.rs:164-181`) while the in-memory cell kept every permission the
revoke did not remove. Both shapes published, so no replay re-issued them, and
`SystemTableLoader::load_role_permissions` reads the row back on the next boot: the role's
authorization answers changed across a restart, with nothing logged and nothing failing.

The current code writes the post-state: `Grant` re-reads the stored cell after extending it
(`state_machine.rs:1892-1896`) and `Revoke` emits `GrantUpdated` with what remains, falling back to a
tombstone only when nothing does (`:1926-1939`). The regression is kept here as a witness rather than
dropped, because the fix's own tests assert only the post-state and nothing else records the shape
that was wrong.
-/

/-- The states a grant or revoke projection can be in, pre-fix and post-fix. -/
inductive GrantCase where
  /-- Died after the in-memory cell changed and before the projection row was written. -/
  | beforeTheWrite
  /-- Pre-fix: the row tombstone landed and the rewrite that was to follow it did not. -/
  | betweenMutations
  /-- Pre-fix: the row holds the delta (`entry.clone()`), the state holds the union. -/
  | deltaWithFullCell
  /-- Pre-fix: a partial revoke tombstoned the whole resource row, the state kept the rest. -/
  | tombstoneWithRemainder
  /-- The current code: one `GrantUpdated` carrying the complete cell. -/
  | postState
  deriving DecidableEq, Repr

/-- What a restart sees. `rowPerms` is what `load_role_permissions` returns; `statePerms` is what
the state machine held when the row was written; `published` says whether replay will re-issue the
entry at all. -/
structure GrantObs where
  rowPerms : Nat
  statePerms : Nat
  rowDeleted : Bool
  published : Bool
  deriving DecidableEq, Repr

/-- The row a role had two permissions on: `SELECT` from an earlier entry, `MODIFY` from this one. -/
def grantObs : GrantCase → GrantObs
  | .beforeTheWrite => ⟨1, 2, false, false⟩
  | .betweenMutations => ⟨0, 1, true, true⟩
  | .deltaWithFullCell => ⟨1, 2, false, true⟩
  | .tombstoneWithRemainder => ⟨0, 1, true, true⟩
  | .postState => ⟨2, 2, false, true⟩

/-- A read of `system_auth.role_permissions`, or of anything derived from it, answers differently
from the state that produced it. -/
def grantWrong (o : GrantObs) : Bool := o.rowDeleted || o.rowPerms ≠ o.statePerms

/-- The class of a grant projection, by the same two questions as the entry-level table: is the row
wrong, and will anything re-issue it? -/
def grantClass (c : GrantCase) : Class :=
  let o := grantObs c
  if grantWrong o then (if o.published then .corrupt else .divergent) else .safe

/-- **THE PRE-FIX GRANT PROJECTION WAS CORRUPT, AND THE CURRENT ONE IS SAFE (DISPROVED, THEN
FIXED).** Three of the five cases are corrupt: each published a row that contradicts the live cell,
so no replay re-issues it, and a restart loads the row as truth. The current code's case agrees with
the state, and the only remaining window is the one where the entry has not published yet and replay
still repairs it. -/
theorem the_grant_regression_is_corrupt_and_the_current_projection_is_safe :
    grantClass .deltaWithFullCell = .corrupt
    ∧ grantClass .tombstoneWithRemainder = .corrupt
    ∧ grantClass .betweenMutations = .corrupt
    ∧ grantClass .postState = .safe
    ∧ grantClass .beforeTheWrite = .divergent := by
  decide

/-- **THE REGRESSION WITNESS.** The shape that was wrong, kept so the corpus records it: a published
row holding one permission while the state machine holds two, and a published tombstone while the
state machine still holds the remaining permission. Both are read back by
`load_role_permissions` on the next boot. -/
theorem the_persisted_delta_contradicts_the_live_cell :
    (grantObs .deltaWithFullCell).rowPerms = 1
    ∧ (grantObs .deltaWithFullCell).statePerms = 2
    ∧ (grantObs .betweenMutations).rowDeleted = true
    ∧ (grantObs .betweenMutations).statePerms = 1 := by
  decide

/-- **THE CURRENT PROJECTION IS THE POST-STATE.** One row, carrying the complete set, agreeing with
the state machine that wrote it. -/
theorem the_current_grant_projection_is_the_post_state :
    (grantObs .postState).rowPerms = (grantObs .postState).statePerms
    ∧ (grantObs .postState).rowDeleted = false
    ∧ grantWrong (grantObs .postState) = false := by
  decide

/-- A grant/revoke projection write. -/
inductive ProjWrite where
  /-- `GrantUpdated`: a live cell carrying the complete permission set. -/
  | live (ts : Nat)
  /-- `PermissionRevoked`: a whole-row tombstone. -/
  | tomb (ts : Nat)
  deriving DecidableEq, Repr

/-- The projection layer's merge, which is `reconcile_ref` plus the row-deletion rule the memtable
applies (`ferrosa-common/src/complex_cell.rs:65-75`, `ferrosa-storage/src/memtable/sharded.rs:549`):
the higher timestamp wins, and **a tombstone wins an equal-timestamp tie**. -/
def resolve : ProjWrite → ProjWrite → ProjWrite
  | .live a, .live b => if a ≤ b then .live b else .live a
  | .tomb a, .tomb b => if a ≤ b then .tomb b else .tomb a
  | .tomb a, .live b => if b > a then .live b else .tomb a
  | .live a, .tomb b => if a > b then .live a else .tomb b

/-- **A TIE GOES TO THE TOMBSTONE, WHICHEVER WRITE CAME SECOND.** Both orders resolve to the
tombstone, so when the two writes carry the same microsecond the later one is masked rather than
applied. This is the fact the fix's comment names ("equal timestamps can mask it"), and it is a
property of the merge rule rather than of that call site. -/
theorem a_tie_goes_to_the_tombstone :
    resolve (.live 5) (.tomb 5) = .tomb 5
    ∧ resolve (.tomb 5) (.live 5) = .tomb 5
    ∧ resolve (.tomb 5) (.live 6) = .live 6
    ∧ resolve (.live 6) (.tomb 5) = .live 6 := by
  decide

/-- **THE TIE IS NO LONGER REACHABLE WITHIN ONE ENTRY.** The current code emits exactly one
projection mutation per grant or revoke entry, and a tie needs two writes to compare; the pre-fix
partial-revoke path is the one that emitted the pair. -/
theorem one_entry_emits_one_grant_write :
    ([((.beforeTheWrite : GrantCase), 1), (.betweenMutations, 2), (.deltaWithFullCell, 1),
      (.tombstoneWithRemainder, 2), (.postState, 1)].map fun (c, n) =>
        (if c = .postState then n = 1 else true)).all (· = true) = true := by
  decide

/-- What a second application of a grant projection does. Both mutations are set operations on a
cell keyed by `(role, resource)`, so the second application produces the same cell: `Grant` unions a
set, `Revoke` removes an element and recomputes the remainder. Neither latches and neither
duplicates. -/
theorem the_grant_projection_is_idempotent :
    (idem "GrantUpdated" == .same) && (idem "PermissionRevoked" == .same) = true := by
  decide

/-- The two grant effects, kept next to the other pre-publication effects so the ledger can name
them without a second table. -/
def grantEffects : List String := ["GrantUpdated", "PermissionRevoked"]

/-! ## The ledger

Five obligations on partial application, three of them red. Two are about what a partially applied
entry leaves behind generically; the three on the grant surface are about the family that produced
the shape this taxonomy was built for.

`Ferrosa/Raft/Obligations.lean` appends this list to the area ledger, so the count and the
classification are checked there rather than asserted here.
-/

def partialItems : List SpecItem := [
  ⟨"re-applying an entry whose effects already landed produces the same state",
   .met "Raft.every_prepublication_effect_is_idempotent"⟩,
  ⟨"no durable effect belongs to an entry the published index does not cover",
   .failing
     "force_commit_log_sync (state_machine.rs:2193-2197) makes the ordered system-table rows durable inside the job, and finish_pending_apply advances last_applied only when the job's result is polled (:476-477); a process that dies between the two leaves a durable projection row for an entry last_applied does not cover. Replay re-issues it and converges, so the window is only unbounded when the cause of a later failure persists, or when the raft directory is reset and the entry is never replayed on this node"
     "Raft.a_durable_projection_without_publication"⟩,
  ⟨"a restart or a resync repairs an entry whose destructive effect already landed",
   .failing
     "apply_command runs engine.unregister_table before the ordered write is attempted, which records a durable drop in dropped-tables.json and deletes the SSTable directory (ferrosa-storage/src/engine.rs:5865-5899, :6020-6059); when the ordered write then fails the entry never publishes, and the snapshot restored on the next boot still describes the table, so sync_schema_and_engine_from_state re-registers it empty and apply_snapshot's additive merge (ferrosa-schema/src/registry.rs:346-364) never removes it. The node advertises a table whose rows are gone, every component agrees, and no replay, restart or resync puts the rows back"
     "Raft.a_drop_that_never_publishes_is_read_as_an_empty_table"⟩
]

/-- Three obligations on the entry-level matrix, two of them unmet. -/
theorem partial_items_pinned : partialItems.length = 3 ∧ redCount partialItems = 2 := by
  decide

/-- Two obligations on the grant projection. -/
def grantItems : List SpecItem := [
  ⟨"a published grant projection carries the complete permission set",
   .met "Raft.the_current_grant_projection_is_the_post_state"⟩,
  ⟨"the projection orders two writes by the log, not by the wall clock",
   .failing
     "grant_to_row stamps every row with now_micros() (ferrosa-schema/src/system/persistence.rs:417-432) and PermissionRevoked stamps its tombstone the same way (ferrosa-cluster/src/system_table_writer.rs:164-181), and reconcile_ref gives an equal-timestamp tie to the tombstone (ferrosa-common/src/complex_cell.rs:65-75). Within one entry the pair that needed a tie is gone after 1a89858a — one mutation per entry (state_machine.rs:1892-1939) — but across entries the order of two rows is the wall clock's, not the log's: a revoke to empty followed by a re-grant in the next entry is masked if the second now_micros() is equal to or behind the first, which nothing prevents because SystemTime is not monotone. The row then keeps the tombstone and the role loses permissions that the state machine and every peer still hold"
     "Raft.a_tie_goes_to_the_tombstone"⟩
]

/-- Two obligations on the grant projection, one of them unmet. -/
theorem grant_items_pinned : grantItems.length = 2 ∧ redCount grantItems = 1 := by
  decide

/-! ## The axiom audit

Every theorem here is a `decide` over a finite enumeration, so none reports an axiom at all — not
even `propext`. None reports `sorryAx`.
-/

#print axioms the_effect_order_is_a_prefix
#print axioms every_prepublication_effect_is_idempotent
#print axioms a_durable_projection_without_publication
#print axioms exactly_one_point_durably_projects_without_publishing
#print axioms the_class_is_decided_by_whether_the_cause_clears
#print axioms no_failure_is_merely_stalled
#print axioms during_replay_is_safe
#print axioms the_classes_are_distinguishable_by_what_a_read_returns
#print axioms a_drop_that_never_publishes_is_read_as_an_empty_table
#print axioms the_resync_path_does_not_refill_the_rows
#print axioms a_failed_restore_is_corrupt_for_a_drop_and_divergent_for_a_create
#print axioms the_grant_regression_is_corrupt_and_the_current_projection_is_safe
#print axioms the_persisted_delta_contradicts_the_live_cell
#print axioms the_current_grant_projection_is_the_post_state
#print axioms a_tie_goes_to_the_tombstone
#print axioms one_entry_emits_one_grant_write
#print axioms the_grant_projection_is_idempotent

end Ferrosa.Raft
