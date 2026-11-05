/-
  The rest of the surface: snapshot, purge and the two restart paths
  =================================================================

  The apply fence is one durability boundary. This module is the others: what happens between the
  published index, the snapshot a builder produces, the file that snapshot is written to, the
  watermark the log store is allowed to purge to, and the two paths a process can take when it
  starts again. It exists because the fence found one bug of a shape — a durable effect that did
  not happen, with publication advancing anyway — and the question this module answers is what else
  of that shape is on the surface.

  Three of the obligations here fail, and all three are that shape. The snapshot slot is guarded
  against going backwards and the file it writes to is not, so a builder taken before a snapshot
  install overwrites the durable snapshot with an older one while the purge watermark still says the
  newer entries are expendable. A snapshot that cannot be restored is logged and booted on anyway,
  because `recover_from_persisted_snapshot` has already adopted the snapshot's state by the time the
  sync that would make it true fails. And a local engine or schema effect that fails during an apply
  is reported into the response, which nothing replays, so the only thing that repairs it is a
  restart.

  Four of them hold, and two of those are why the failures above are loud rather than silent: the
  purge ceiling is the durable watermark and not the snapshot file, and the classifier refuses to
  start a node whose applied index is behind its purge point. That pair is what turns "the snapshot
  file went backwards" into "this node cannot start" instead of "this node replayed from a hole".

  The Rust is async and the extractor rejects `block_on`, so this model is hand-written like
  `Ferrosa/Raft/StateMachine.lean` and for the same reason: it models ferrosa's own behaviour.
-/
import Ferrosa.Ledger
import Ferrosa.Raft.StateMachine

namespace Ferrosa.Raft

open Ferrosa.Ledger

/-! ## What is modelled, and at what abstraction

Every field is an index watermark, and `0` is "nothing". Nothing here needs a set, because every
question is "which index has this reached", and the one relation that is not an inequality is the
latch, which is a `Bool`.

* `applied`  — `last_applied.index`. In memory only: it is not persisted except inside a snapshot,
  which is why a crash resets it and a replay does not see the previous process's answer.
* `stateW`   — the watermark `RaftState` itself reflects. It moves with `applied` on every path that
  adopts a completed apply, so the two are equal on every reachable state and are carried
  separately only so the theorems can say so.
* `shared`   — what the shared `Arc<Schema>` and the `Arc<StorageEngine>` reflect. The apply path
  advances it one entry at a time, and *only* when the local effect for that entry succeeded; a
  snapshot install or a restart re-syncs it wholesale from `RaftState`.
* `slot`     — `current_snapshot`'s `last_log_id.index`: the snapshot replication is served.
* `disk`     — the `last_log_id.index` of `state-machine.snapshot.bin`.
* `durable`  — the `durable_applied` watermark. Monotone: `mark_durably_applied` is a `fetch_max`.
  It is raised only after `persist_snapshot_to_disk` returns `Ok`.
* `purged`   — the log store's `last_purged_log_id.index`.
* `builder`  — an outstanding `get_snapshot_builder` result, at the index it was taken from.
* `latched`  — `apply_failure.is_some()`.
* `booted`   — whether this process is running at all. `crash` clears it; the two boot events set it
  to the classifier's verdict, which is the only thing that decides it.

`purged` advancing to `durable` in one step bakes in `local_state::purge_ceiling`, which clamps a
purge request to the durable watermark and is proved on the Rust side by
`no_permitted_purge_can_strand_this_node`. This module does not re-prove that clamp; it assumes it
and proves what the rest of the system does around it. That assumption is the one place the model is
weaker than the code, and it is stated rather than glossed.
-/

/-- One event of the snapshot, purge and restart surface. -/
inductive BEv where
  /-- A committed entry applies cleanly: the state, the shared schema and the engine all advance. -/
  | apply
  /-- A committed entry applies, but a local engine or schema effect for it failed. The failure is
  reported as `RaftResponse::Error`, which nothing replays, and the entry publishes anyway. -/
  | applyLocalFail
  /-- `get_snapshot_builder`: a point-in-time copy is taken at the published index. -/
  | beginBuild
  /-- `build_snapshot` on a builder taken earlier, with the file write and the watermark succeeding.
  The slot is guarded against going backwards; the file write is not. -/
  | finishBuild
  /-- The same build, with `persist_snapshot_to_disk` failing: the slot is already published, the
  file and the watermark are not touched, and `build_snapshot` returns an error. -/
  | finishBuildNoPersist
  /-- `install_snapshot` from the leader, `n` entries ahead of the published index. -/
  | install (n : Nat)
  /-- The log store purges to its ceiling, which is the durable watermark. -/
  | purge
  /-- The process is lost. Every in-memory field is gone; the file, the watermark and the log are
  not. -/
  | crash
  /-- Restart on the persisted snapshot: the state and the consumers of it are restored together. -/
  | bootFromDisk
  /-- Restart on the persisted snapshot where `sync_schema_and_engine_from_state` failed: the state
  is adopted, the shared schema and engine are not, and the error is logged rather than raised. -/
  | bootSkippingTheSync
  deriving DecidableEq, Repr

/-- The whole boundary surface. -/
structure Boundary where
  applied : Nat
  stateW : Nat
  shared : Nat
  slot : Nat
  disk : Nat
  durable : Nat
  purged : Nat
  builder : Option Nat
  latched : Bool
  booted : Bool
  deriving DecidableEq, Repr

/-- Nothing published, nothing built, nothing durable, not running. -/
def Boundary.initial : Boundary where
  applied := 0
  stateW := 0
  shared := 0
  slot := 0
  disk := 0
  durable := 0
  purged := 0
  builder := none
  latched := false
  booted := false

/-- One step.

The two facts to read carefully are in `finishBuild` and `bootSkippingTheSync`, because they are
where the code and the intent part company. `finishBuild` guards the slot with a `max`, mirroring
`publish_snapshot`'s refusal to replace a newer snapshot, and then assigns the file unconditionally,
mirroring `state_machine.rs:2225` where the return value of the guard is discarded. The two boot
events both classify, and they disagree only about whether the consumers were restored. -/
def bstep (s : Boundary) (e : BEv) : Boundary :=
  match e with
  | .apply =>
    if s.latched then s
    else { s with
           applied := s.applied + 1
           stateW := s.applied + 1
           shared := s.shared + 1 }
  | .applyLocalFail =>
    if s.latched then s
    else { s with
           applied := s.applied + 1
           stateW := s.applied + 1 }
  | .beginBuild =>
    if s.latched then s else { s with builder := some s.applied }
  | .finishBuild =>
    match s.builder with
    | none => s
    | some b =>
      if s.latched then s
      else { s with
             slot := if b ≥ s.slot then b else s.slot
             disk := b
             durable := max s.durable b
             builder := none }
  | .finishBuildNoPersist =>
    match s.builder with
    | none => s
    | some b =>
      if s.latched then s
      else { s with
             slot := if b ≥ s.slot then b else s.slot
             builder := none }
  | .install n =>
    if s.latched ∨ n = 0 then s
    else { s with
           applied := s.applied + n
           stateW := s.applied + n
           shared := s.applied + n
           slot := max s.slot (s.applied + n)
           disk := s.applied + n
           durable := max s.durable (s.applied + n) }
  | .purge => { s with purged := max s.purged s.durable }
  | .crash =>
    { s with
      applied := 0
      stateW := 0
      shared := 0
      slot := 0
      builder := none
      latched := false
      booted := false }
  | .bootFromDisk =>
    if s.disk = 0 ∨ s.latched then s
    else { s with
           applied := s.disk
           stateW := s.disk
           shared := s.disk
           booted := decide (s.disk ≥ s.purged) }
  | .bootSkippingTheSync =>
    if s.disk = 0 ∨ s.latched then s
    else { s with
           applied := s.disk
           stateW := s.disk
           booted := decide (s.disk ≥ s.purged) }

/-- Run a trace. -/
def brun (s : Boundary) (tr : List BEv) : Boundary := tr.foldl bstep s

/-- The apply path alone: `true` is an entry whose local effects landed, `false` is one that
reported a failure. Used to state what the apply path can and cannot repair. -/
def applyTrace : List Bool → List BEv
  | [] => []
  | b :: bs => (if b then BEv.apply else BEv.applyLocalFail) :: applyTrace bs

/-- Five entries published cleanly, a builder taken at 5, a leader snapshot installed at 10, and the
slow builder finishing afterwards. The builder is stale by construction. -/
def staleBuilderTrace : List BEv :=
  [.apply, .apply, .apply, .apply, .apply, .beginBuild, .install 5, .finishBuild]

/-! ## 1. The snapshot slot never moves backwards

`publish_snapshot` refuses a `meta.last_log_id` below the one the slot holds, and every path that
writes the slot goes through it: a build, a membership refresh, and a snapshot install. Only a crash
clears it, because the process that held it is gone. This is the guard that works, and it is stated
first because the finding below is that its sibling file has no equivalent.
-/

/-- **THE SLOT DOES NOT REGRESS.** No event other than `crash` lowers the published snapshot index:
a build at a stale index leaves the slot where it is, an install takes the maximum, and everything
else leaves both fields alone. -/
theorem slot_never_regresses (s : Boundary) (e : BEv) (he : e ≠ .crash) :
    (bstep s e).slot ≥ s.slot := by
  cases e <;> simp only [bstep] <;> try (repeat' split) <;> simp_all <;> omega

/-- **A build at a stale index leaves the slot alone**, which is T-F1-2's assertion and the reason
the slot is not the defect. -/
theorem stale_build_keeps_the_newer_slot :
    (brun Boundary.initial staleBuilderTrace).slot = 10 := by
  decide

/-! ## 2. The file the slot guards has no such guard

`build_snapshot` calls `publish_snapshot` for the slot and then persists to `snapshot_path`
unconditionally — the boolean `publish_snapshot` returns is discarded (`state_machine.rs:2218`), and
the write at `:2225` is not conditioned on it. A builder taken before an install therefore writes
its older snapshot over the installed one, while `mark_durably_applied` stays at the newer index
because it is a `fetch_max`. The slot still serves the newer snapshot; the disk now holds the older
one; the watermark tells the log store the newer entries may be dropped.
-/

/-- **THE DURABLE SNAPSHOT REGRESSES (DISPROVED).** A builder taken at index 5, an install at 10, and
the builder finishing afterwards: the slot and the watermark both stay at 10, and the file goes back
to 5. The trace is the one the Rust test `snapshot_builder_does_not_overwrite_newer_installed_snapshot`
already exercises, with a `snapshot_path` configured — which that test does not do, because it builds
on `FerrosStateMachine::new()`, whose `snapshot_path` is `None`. -/
theorem stale_builder_overwrites_the_durable_snapshot :
    (brun Boundary.initial staleBuilderTrace).disk = 5
    ∧ (brun Boundary.initial staleBuilderTrace).slot = 10
    ∧ (brun Boundary.initial staleBuilderTrace).durable = 10
    ∧ (brun Boundary.initial staleBuilderTrace).applied = 10 := by
  decide

/-- **…and the node then refuses to start.** Purging to the watermark and crashing leaves
`applied = 5 < purged = 10`: the two durable facts disagree and `classify_local_raft_state` returns
`StrandedBehindPurge`, so `controller/cluster.rs:1526` moves the whole raft directory aside and
rebuilds the node from a leader snapshot. The log it throws away is the only reason the node could
have started at all. -/
theorem a_stale_build_strands_the_node :
    (brun Boundary.initial (staleBuilderTrace ++ [.purge, .crash, .bootFromDisk])).disk = 5
    ∧ (brun Boundary.initial (staleBuilderTrace ++ [.purge, .crash, .bootFromDisk])).purged = 10
    ∧ (brun Boundary.initial (staleBuilderTrace ++ [.purge, .crash, .bootFromDisk])).applied = 5
    ∧ (brun Boundary.initial (staleBuilderTrace ++ [.purge, .crash, .bootFromDisk])).booted = false := by
  decide

/-! ## 3. A snapshot that cannot be restored is booted on anyway

`apply_snapshot_data` adopts the state and the published index (`state_machine.rs:838-839`) and only
then calls `sync_schema_and_engine_from_state` (`:841-844`), whose failure is returned as an error.
`recover_from_persisted_snapshot` propagates it, and its one production caller —
`controller/cluster.rs:1459-1465` — logs a warning and carries on. The process starts with
`RaftState` at the snapshot's index and a shared `Schema` and `StorageEngine` that never saw it.

That is not a transient: the apply path advances `shared` by the entries it applies, so the gap is
only closed by a later `sync_schema_and_engine_from_state`, which runs on a snapshot install and not
on an apply. A node in this state serves a table list from `RaftState` that its engine cannot
resolve, and the engine is what decides how a row's columns are decoded.

The destructive half is worse than the stale half. `sync_schema_and_engine_from_state` retires
superseded table incarnations (`:1001-1005`) before it applies the schema snapshot (`:1061`), so a
failure after the retirement has deleted physical data and returned an error that the caller
discards.
-/

/-- **A FAILED RESTORE BOOTS WITH AN UNSYNCED SCHEMA (DISPROVED).** The snapshot at 5 is adopted by
the state and by nothing else, the classifier's verdict is "usable" because 5 is not behind the purge
point, and the process runs. -/
theorem a_failed_restore_boots_with_a_stale_schema :
    (brun Boundary.initial
        [.apply, .apply, .apply, .apply, .apply, .beginBuild, .finishBuild,
         .crash, .bootSkippingTheSync]).disk = 5
    ∧ (brun Boundary.initial
        [.apply, .apply, .apply, .apply, .apply, .beginBuild, .finishBuild,
         .crash, .bootSkippingTheSync]).stateW = 5
    ∧ (brun Boundary.initial
        [.apply, .apply, .apply, .apply, .apply, .beginBuild, .finishBuild,
         .crash, .bootSkippingTheSync]).shared = 0
    ∧ (brun Boundary.initial
        [.apply, .apply, .apply, .apply, .apply, .beginBuild, .finishBuild,
         .crash, .bootSkippingTheSync]).booted = true := by
  decide

/-- **The two restore paths agree about everything except the consumers.** This is what makes the
finding specific: the classifier's verdict is identical, so the defect is not in the classification
but in the decision to start after a restore that did not complete. -/
theorem restore_paths_differ_only_in_the_consumers :
    (brun Boundary.initial
        [.apply, .apply, .beginBuild, .finishBuild, .crash, .bootFromDisk]).booted
      = (brun Boundary.initial
        [.apply, .apply, .beginBuild, .finishBuild, .crash, .bootSkippingTheSync]).booted
    ∧ (brun Boundary.initial
        [.apply, .apply, .beginBuild, .finishBuild, .crash, .bootFromDisk]).shared = 2
    ∧ (brun Boundary.initial
        [.apply, .apply, .beginBuild, .finishBuild, .crash, .bootSkippingTheSync]).shared = 0 := by
  decide

/-! ## 4. The purge ceiling is the watermark, not the file

This is the design that makes the two findings loud. `log_store.rs:1325` clamps every purge to
`durable_applied`, and `mark_durably_applied` is a `fetch_max` that is only raised after
`persist_snapshot_to_disk` returns `Ok`, so the ceiling can never exceed what was once durable —
even when the file later regresses underneath it. The consequence is exactly the classification in
section 5: the node stops instead of replaying from a hole.
-/

/-- **One step never lets the purge point past the durable watermark.** The purge sets it to their
maximum, and nothing else raises it. -/
theorem purge_bounded_step (s : Boundary) (e : BEv) (h : s.purged ≤ s.durable) :
    (bstep s e).purged ≤ (bstep s e).durable := by
  cases e <;> simp only [bstep] <;> try (repeat' split) <;> simp_all <;> omega

/-- **THE LOG IS NEVER PURGED PAST THE DURABLE SNAPSHOT.** Over every trace, the purge point is at
most the durable watermark, including after a build whose persist failed (where the slot leads the
disk) and after a stale build (where the file lags it). -/
theorem purge_bounded_by_durable : ∀ (tr : List BEv) (s : Boundary),
    s.purged ≤ s.durable → (brun s tr).purged ≤ (brun s tr).durable
  | [], _, h => h
  | e :: es, s, h => purge_bounded_by_durable es (bstep s e) (purge_bounded_step s e h)

/-- **A build that cannot persist still does not authorise a purge.** The slot advances — that is the
D1 ordering, and it is intentional, because replication must be able to serve a snapshot covering
anything that may be purged — and the watermark does not, so the ceiling holds. -/
theorem an_unpersisted_build_authorises_no_purge :
    (brun Boundary.initial
        [.apply, .apply, .beginBuild, .finishBuildNoPersist, .purge]).slot = 2
    ∧ (brun Boundary.initial
        [.apply, .apply, .beginBuild, .finishBuildNoPersist, .purge]).disk = 0
    ∧ (brun Boundary.initial
        [.apply, .apply, .beginBuild, .finishBuildNoPersist, .purge]).durable = 0
    ∧ (brun Boundary.initial
        [.apply, .apply, .beginBuild, .finishBuildNoPersist, .purge]).purged = 0 := by
  decide

/-! ## 5. A node that cannot reconstruct its applied state refuses to start

`classify_local_raft_state` is the guard that replaced the 2026-08-20 silence: it reports a node
whose `last_applied` is behind `last_purged` as stranded and never as usable, and the caller moves
the raft directory aside rather than clamping forward. The model carries it because the two findings
above would both be silent — an undetectable replay from a hole — without it.
-/

/-- **A NODE WHOSE SNAPSHOT DOES NOT COVER ITS PURGE POINT DOES NOT START.** Both boot events take
their verdict from the same comparison, so neither can start a node whose durable state does not
reach its purge point. This is `classify_local_raft_state`'s `StrandedBehindPurge`, in the one place
that decides whether a process runs. -/
theorem boot_requires_the_snapshot_to_cover_the_purge_point (s : Boundary) (hl : s.latched = false)
    (h : s.disk ≠ 0) (hlt : s.disk < s.purged) :
    (bstep s .bootFromDisk).booted = false
    ∧ (bstep s .bootSkippingTheSync).booted = false := by
  have hf : ¬ (s.disk ≥ s.purged) := by omega
  constructor <;> simp [bstep, hl, h, hf]

/-- **A refused boot is exactly the stranding case.** The boundary — applied exactly at the purge
point — start, one index behind it does not. -/
theorem purged_past_applied_is_refused :
    (bstep { Boundary.initial with disk := 10, purged := 10 } .bootFromDisk).booted = true
    ∧ (bstep { Boundary.initial with disk := 9, purged := 10 } .bootFromDisk).booted = false
    ∧ (bstep { Boundary.initial with disk := 9, purged := 10 } .bootSkippingTheSync).booted = false := by
  decide

/-! ## 6. A failed local effect is repaired by a restart and by nothing else

A `register_table`, an `unregister_table` or a `create_keyspace_internal` that fails during an apply
pushes an `ApplyError`, and `apply_command` folds the collected errors into `RaftResponse::Error`
(`state_machine.rs:2142-2155`). The entry does not fail: `last_applied` advances, the state adopts
the mutation, and the response is a value openraft carries to the caller, which is not durable and
not replayed. Its comment at `:2142-2144` says callers "can detect engine/schema/system_writer
failures and decide whether to retry" — but a caller cannot retry a committed entry, and the only
thing that re-runs the effect is a replay from a snapshot behind it, which happens on a restart or an
install (`:1435-1444` for the registration, which is unconditional and so re-runs on replay, and
`install_snapshot` at `:2382` for the wholesale re-sync).

The test `apply_command_propagates_engine_register_failure` (`:5148`) pins the intent in place: the
response names the failure, `apply_failure` stays `None`, and "committed failed intent remains for
replay". So this is a deliberate design, and the obligation it fails is the one that design assumes:
that replay is a repair path an operator can rely on without taking the node down.
-/

/-- **A FAILED LOCAL EFFECT LEAVES A GAP THE APPLY PATH DOES NOT CLOSE (DISPROVED).** One entry whose
engine registration failed leaves the published index one ahead of what the engine reflects, and
every subsequent clean apply moves both by one, so the gap never closes. -/
theorem a_failed_local_effect_leaves_a_permanent_gap :
    (brun Boundary.initial [.apply, .apply, .applyLocalFail]).applied = 3
    ∧ (brun Boundary.initial [.apply, .apply, .applyLocalFail]).shared = 2
    ∧ (brun (bstep (bstep Boundary.initial .apply) .applyLocalFail)
        (applyTrace [true, true])).applied = 4
    ∧ (brun (bstep (bstep Boundary.initial .apply) .applyLocalFail)
        (applyTrace [true, true])).shared = 3 := by
  decide

/-- **The gap is closed by a full re-sync, not by applying more entries.** This is the positive half
of the same statement: `install_snapshot` re-runs `sync_schema_and_engine_from_state` over the whole
state, so the only repair is a wholesale one. -/
theorem only_a_resync_closes_the_gap :
    (brun Boundary.initial [.apply, .apply, .applyLocalFail, .apply]).shared = 3
    ∧ (brun Boundary.initial [.apply, .apply, .applyLocalFail, .install 3]).shared = 6 := by
  decide

/-! ## 7. A latched machine publishes nothing

Every trait entry point calls `finish_pending_apply` first, and that returns the latch's error before
doing anything else (`state_machine.rs:436-443`). `get_snapshot_builder` cannot return an error, so it
logs and returns a builder anyway (`:2341-2346`) — the one place the latch is not raised at a caller.
The builder it returns retains `apply_failure` (`:2364`) and `build_snapshot` re-checks it (`:2190`),
so the build fails and no index is published. The suspicion that this is a hole is settled: it is a
guard bypass with no reachable effect.
-/

/-- **A LATCHED MACHINE MOVES NO WATERMARK.** While the latch is set, no event other than `crash`
changes the slot, the file, the watermark or the published index, because every one of them is behind
a `finish_pending_apply` that refuses. -/
theorem latched_machine_publishes_nothing (s : Boundary) (e : BEv) (hl : s.latched = true)
    (he : e ≠ .crash) :
    (bstep s e).slot = s.slot
    ∧ (bstep s e).disk = s.disk
    ∧ (bstep s e).durable = s.durable
    ∧ (bstep s e).applied = s.applied := by
  cases e <;> simp only [bstep, hl] <;> try (repeat' split) <;> simp_all

/-- **A builder that exists at all inherits the latch and can build nothing.** The builder is a copy
of the machine, including `apply_failure`, and `build_snapshot` re-checks it, so a build on a builder
taken while latched publishes no slot, no file and no watermark. -/
theorem a_builder_from_a_latched_machine_yields_nothing :
    (bstep { Boundary.initial with latched := true } .beginBuild).builder = none
    ∧ (brun { Boundary.initial with latched := true, builder := some 4 } [.finishBuild]).slot = 0
    ∧ (brun { Boundary.initial with latched := true, builder := some 4 } [.finishBuild]).disk = 0
    ∧ (brun { Boundary.initial with latched := true, builder := some 4 } [.finishBuild]).durable = 0 := by
  decide

/-! ## The ledger

Seven obligations, three of them red, and each names the theorem that settles it. The three red ones
are one shape: a durable effect that did not happen — a file written from a stale builder, a state
adopted without its consumers, a local effect reported into a response nobody replays — with the
published index moving on regardless. `Ferrosa/Raft/Obligations.lean` appends this list to the area's
ledger, so the count and the classification are checked there rather than asserted here.
-/

def surfaceItems : List SpecItem := [
  ⟨"the published snapshot index never moves backwards",
   .met "Raft.slot_never_regresses"⟩,
  ⟨"the durable snapshot on disk never moves backwards",
   .failing
     "build_snapshot discards publish_snapshot's refusal (state_machine.rs:2218) and writes snapshot_path unconditionally (:2225), so a builder taken before an install persists its older snapshot over the installed one while mark_durably_applied stays at the newer index; the slot keeps the newer snapshot so replication is unaffected, and the log store is free to purge to it, so the node boots as StrandedBehindPurge and discards its raft directory"
     "Raft.stale_builder_overwrites_the_durable_snapshot"⟩,
  ⟨"a snapshot is restored as a whole or the node does not boot on it",
   .failing
     "apply_snapshot_data adopts the state and the published index (state_machine.rs:838-839) before sync_schema_and_engine_from_state (:841-844) can fail, and controller/cluster.rs:1459-1465 downgrades that failure to a warning, so the process runs with RaftState at the snapshot index and a shared Schema and StorageEngine that never saw it; nothing on the apply path closes the gap, and the retirement of superseded incarnations (:1001-1005) happens before the sync that can still fail"
     "Raft.a_failed_restore_boots_with_a_stale_schema"⟩,
  ⟨"a failed local side effect is repaired without a restart",
   .failing
     "an engine or schema effect that fails during apply becomes RaftResponse::Error (state_machine.rs:2142-2155), which is not durable and not replayed, while the entry publishes and the state adopts the mutation; the effect is re-attempted only by a replay from a snapshot behind it — register_table is unconditional at :1435-1444, so it re-runs on replay — and a replay needs a restart or an install, so until then the node advertises a table its engine cannot serve"
     "Raft.a_failed_local_effect_leaves_a_permanent_gap"⟩,
  ⟨"the log is purged only as far as a durable snapshot covers",
   .met "Raft.purge_bounded_by_durable"⟩,
  ⟨"a node that cannot reconstruct its applied state refuses to start",
   .met "Raft.boot_requires_the_snapshot_to_cover_the_purge_point"⟩,
  ⟨"the snapshot builder inherits the apply latch",
   .met "Raft.latched_machine_publishes_nothing"⟩
]

/-- Seven obligations on this surface, three of them unmet, pinned here so that closing one or adding
a claim forces this table to move in the same change. -/
theorem surface_items_pinned : surfaceItems.length = 7 ∧ redCount surfaceItems = 3 := by
  decide

/-! ## The axiom audit

Each proved obligation is checked here for what it rests on. `omega` introduces nothing, the
trace-level inductions report `propext` and `Quot.sound`, and the counterexample traces, being finite
computations, report no axioms at all. None of them reports `sorryAx`.
-/

#print axioms slot_never_regresses
#print axioms stale_build_keeps_the_newer_slot
#print axioms stale_builder_overwrites_the_durable_snapshot
#print axioms a_stale_build_strands_the_node
#print axioms a_failed_restore_boots_with_a_stale_schema
#print axioms restore_paths_differ_only_in_the_consumers
#print axioms purge_bounded_step
#print axioms purge_bounded_by_durable
#print axioms an_unpersisted_build_authorises_no_purge
#print axioms boot_requires_the_snapshot_to_cover_the_purge_point
#print axioms purged_past_applied_is_refused
#print axioms a_failed_local_effect_leaves_a_permanent_gap
#print axioms only_a_resync_closes_the_gap
#print axioms latched_machine_publishes_nothing
#print axioms a_builder_from_a_latched_machine_yields_nothing

end Ferrosa.Raft
