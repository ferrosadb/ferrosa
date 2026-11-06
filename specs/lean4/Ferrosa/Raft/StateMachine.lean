/-
  The apply fence: an entry becomes applied only after its work has run
  =====================================================================

  A committed Raft entry changes the schema and the system tables, and `last_applied` is the index
  a reader is told has taken effect. This module is the machine that change is argued against: the
  published index, the staged metadata copy, the effects that copy shares with the live machine, the
  ordered system-table write and the durable prefix behind it, the single in-flight job and its two
  stages, and the latch that stops everything after a failure.

  Two things surprise a reader. The staged copy is not isolated: it clones the metadata but shares
  `schema`, `engine` and `system_writer`, so an apply that is cancelled or panics leaves physical
  effects visible while the published index still points at the entry before it. And publication
  guarantees the ordered write was *attempted*, never that it landed.

  The code is async and the extractor rejects `block_on`, so this model is hand-written. It models
  ferrosa's own behaviour, which is why it lives in this corpus rather than beside the extractor.
-/

namespace Ferrosa.Raft

/-! ## What is modelled, and at what abstraction

Entries are identified by log index (`Nat`, 1-based). `last_applied` is `Option<LogId>`; the model
carries its index in `applied`, with `0` playing the role of `None`. Entry indexes are strictly
increasing and never 0, so this is order-isomorphic on every reachable state, which is all the five
obligations use.

The one in-flight job (`PendingApply` plus its `JoinHandle`) is represented by the `pending*` group
of fields rather than a nested structure, because every transition writes whole fields. The group is
only meaningful while `pending = some k`; `beginApply` initialises it, `finish` clears `pending`, and
nothing else reads it.

* `committed`  — highest index present in the committed log (drives `propose`).
* `applied`    — `last_applied.index`.
* `metaState`  — indices present in the live `RaftState`. This is the *isolated* part of
  `staged_apply`: `state` is cloned, and the staged copy's state is adopted wholesale by
  `finish_pending_apply`. It therefore moves atomically with `applied`.
* `external`   — indices whose *physical* transition is visible through the shared handles:
  `schema` (`Arc<Schema>`), `engine` (`Arc<StorageEngine>`), `system_writer`
  (`SystemTableWriter` over the same engine). `staged_apply` clones these `Arc`s, so this part of
  the staged copy shares the live machine.
* `attempted`  — indices whose ordered system-table write has *returned* (success or error).
  `flush_pending_system_writes` swallows a write error (`warn!`/`error!`) and returns, so
  "returned" is the barrier the job's completion actually provides.
* `written`    — durable system-table rows, as a prefix watermark: the largest index `j` such that
  the rows for every entry `1..j` are durable (the write returned `Ok`). A failed write leaves the
  watermark behind, so it never claims a row that is not there.
* `pending*`   — `pending_apply`: the in-flight index, the worker job's two stages
  (`apply_command`, then `flush_pending_system_writes`), and `dead` for a panicked job whose
  `JoinHandle` a trait call has not yet polled.
* `failed`     — `apply_failure`.

The two policy parameters are the point of the model:

* `PublishPolicy.early` — the pre-PR shape: `last_applied = Some(entry.log_id)` at the top of the
  apply loop, before `apply_command` ran.
* `PublishPolicy.late`  — the fenced shape: `last_applied` is set in `finish_pending_apply` only
  after the job's `JoinHandle` resolves.

* `Staging.shared`   — the code as written: the staged copy shares `schema`/`engine`/`system_writer`.
* `Staging.isolated` — a hypothetical in which the staged copy owns those handles too, so a
  cancelled apply leaves no trace anywhere until publication.
-/

/-- Where `last_applied` is advanced relative to the physical transition and the ordered
system-table write. -/
inductive PublishPolicy where
  /-- `last_applied := entry.log_id` at the top of the apply loop (the pre-PR shape). -/
  | early
  /-- `last_applied := pending.log_id` in `finish_pending_apply` (the fenced shape). -/
  | late
  deriving DecidableEq, Repr

/-- Whether this policy publishes `last_applied` before the physical transition. -/
def PublishPolicy.isEarly : PublishPolicy → Bool
  | .early => true
  | .late => false

/-- How much of the staged copy is isolated from the live machine. -/
inductive Staging where
  /-- `RaftState` cloned, `schema`/`engine`/`system_writer` shared `Arc`s (the code as written). -/
  | shared
  /-- Hypothetical: the staged copy owns its external handles as well. -/
  | isolated
  deriving DecidableEq, Repr

/-- One event of the machine. -/
inductive Ev where
  /-- A raft entry is committed (nothing in the state machine changes yet). -/
  | propose
  /-- An apply job for `applied + 1` is spawned and `pending_apply` is set. -/
  | beginApply
  /-- The job completed the physical command transition. -/
  | physical
  /-- The job's ordered system-table writes all returned `Ok`. -/
  | systemWrite
  /-- The job's ordered system-table writes returned an error, which is logged and dropped. This is
  the **pre-`6c270518` shape**, kept because the regression trace below is still a trace of this
  machine. -/
  | systemFail
  /-- The job's ordered system-table write returned an error and the job propagated it:
  `flush_pending_system_writes` is fallible (`state_machine.rs:2171-2199`), so `finish_pending_apply`
  latches and the entry does not publish (`:454-464`). This is the shipped shape. -/
  | systemFailFatal
  /-- The awaiter is dropped. `pending_apply` (and so the job) is retained. -/
  | cancel
  /-- `finish_pending_apply`: poll the retained handle and publish, or latch on failure. -/
  | finish
  /-- The worker job panicked. -/
  | panic
  /-- The process is lost. The persisted record does not change. -/
  | crash
  /-- A new process rebuilds in-memory state from the snapshot at `applied`. -/
  | restart
  deriving DecidableEq, Repr

/-- The whole machine. -/
structure St where
  committed : Nat
  applied : Nat
  metaState : Nat
  external : Nat
  attempted : Nat
  /-- Durable system-table rows, as a prefix: the largest index `j` such that the rows
  for every entry `1..j` are durable in the engine. A failed write stops the prefix
  from advancing even if a later write succeeds, which is what replay needs to reason
  about. -/
  written : Nat
  /-- `pending_apply.map (·.log_id.index)`. -/
  pending : Option Nat
  /-- `staged.apply_command` returned. -/
  pendingPhysical : Bool
  /-- `staged.flush_pending_system_writes` returned (whether or not a write failed). -/
  pendingSystemWrites : Bool
  /-- The job panicked; the retained handle will report a `JoinError`. -/
  pendingDead : Bool
  /-- The staged copy's `RaftState` watermark, adopted at publication. -/
  pendingStaged : Nat
  failed : Bool
  deriving DecidableEq, Repr

/-- No entries, no apply, nothing latched. -/
def St.initial : St where
  committed := 0
  applied := 0
  metaState := 0
  external := 0
  attempted := 0
  written := 0
  pending := none
  pendingPhysical := false
  pendingSystemWrites := false
  pendingDead := false
  pendingStaged := 0
  failed := false

/-- One step. Events the code cannot perform are no-ops: `beginApply` is guarded because every
trait entry point reaps `pending_apply` and refuses after a failure, so a second job can never be
spawned over an unreaped one. -/
def step (pol : PublishPolicy) (stg : Staging) (s : St) (e : Ev) : St :=
  match e with
  | .propose => { s with committed := s.committed + 1 }
  | .beginApply =>
    if s.pending = none ∧ s.failed = false ∧ s.applied + 1 ≤ s.committed then
      { s with
        applied := if pol.isEarly then s.applied + 1 else s.applied
        pending := some (s.applied + 1)
        pendingPhysical := false
        pendingSystemWrites := false
        pendingDead := false
        pendingStaged := s.metaState }
    else s
  | .physical =>
    match s.pending with
    | none => s
    | some k =>
      if s.pendingPhysical then s
      else
        { s with
          external := match stg with | .shared => k | .isolated => s.external
          pendingPhysical := true
          pendingStaged := k }
  | .systemWrite =>
    match s.pending with
    | none => s
    | some k =>
      if s.pendingPhysical = true ∧ s.pendingSystemWrites = false then
        { s with
          attempted := k
          written := if s.written + 1 = k then k else s.written
          pendingSystemWrites := true }
      else s
  | .systemFail =>
    match s.pending with
    | none => s
    | some k =>
      if s.pendingPhysical = true ∧ s.pendingSystemWrites = false then
        { s with attempted := k, pendingSystemWrites := true }
      else s
  | .systemFailFatal =>
    match s.pending with
    | none => s
    | some k =>
      if s.pendingPhysical = true ∧ s.pendingSystemWrites = false then
        { s with attempted := k, pending := none, failed := true }
      else s
  | .cancel => s
  | .finish =>
    match s.pending with
    | none => s
    | some k =>
      if s.pendingDead = true then { s with failed := true, pending := none }
      else if s.pendingPhysical = true ∧ s.pendingSystemWrites = true then
        { s with
          applied := k
          metaState := s.pendingStaged
          external := match stg with | .shared => s.external | .isolated => k
          pending := none }
      else s
  | .panic =>
    match s.pending with
    | none => s
    | some _ => { s with pendingDead := true }
  | .crash => s
  | .restart =>
    { s with pending := none, failed := false, metaState := s.applied, external := s.applied }

/-- Run a trace. -/
def run (pol : PublishPolicy) (stg : Staging) (s : St) (tr : List Ev) : St :=
  tr.foldl (step pol stg) s

end Ferrosa.Raft
