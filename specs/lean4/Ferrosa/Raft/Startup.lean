/-
  Startup, replay, and whether these failures can wedge a node
  ============================================================

  The operator's requirement is one sentence: "we can't have a wedged node because of these
  failures." The apply fence in `Ferrosa/Raft/StateMachine.lean` and the snapshot surface in
  `Ferrosa/Raft/Surfaces.lean` each found a durable effect that did not happen while publication
  advanced. This module asks the question those two leave open: when such a failure lands, what does
  the node do on the *next* boot, and is there any state in which it comes up, reports itself
  healthy, and cannot make progress?

  Three facts about the Rust shape the answer, and all three are easy to miss.

  **The latch is a single in-memory field.** `apply_failure: Option<String>`
  (`ferrosa-cluster/src/raft/state_machine.rs:375`) is set by `finish_pending_apply` (`:436-443`,
  `:454-474`) and is not part of `SnapshotData`, which carries `state`, `last_applied` and
  `last_membership` only. Startup constructs a fresh machine with `apply_failure: None`
  (`:580`, `:766`) at `controller/cluster.rs:1449-1453`. A restart therefore *always* clears the
  latch, and nothing at boot asks whether the failure that set it is still true.

  **A failed apply is loud, not silent.** The staged job's error reaches openraft as
  `Notify::StateMachine { result: Err }`; `raft_core.rs:1415` propagates it with `?` into `Fatal`,
  the core stops, `metrics.running_state` becomes `Err`, and the consensus-metrics poller calls
  `health.fail("openraft-fatal", …)` (`raft/consensus_metrics.rs:91-103`), which closes
  `is_cql_ready` (`controller/mod.rs:959-964`). So a latched node refuses traffic rather than
  serving a lie — which is why the apply-latch family is *not* a silent wedge, and why the same
  family is still a wedge: nothing repairs it and every restart reproduces it.

  **Two boot paths do not verify the state they claim.** `recover_from_persisted_snapshot` adopts
  `state` and `last_applied` (`state_machine.rs:848-849`) before `sync_schema_and_engine_from_state`
  (`:851-854`) can fail, and `controller/cluster.rs:1459-1465` downgrades that error to a warning
  and boots. The classifier is then asked about the *adopted* index, not the synced one
  (`:1485-1489`, reading `last_applied_index` at `:609-611`). And when the snapshot cannot be read
  at all, `last_applied` stays `None`, the classifier reports `NeedsPurgePointBaseline`
  (`raft/local_state.rs:93-96`), and `recover_from_purge_point` (`state_machine.rs:621-632`) sets
  `last_applied` to the purge point *without touching `state`* — a node that reports the whole log
  applied while holding an empty `RaftState`, and which therefore never replays the entries below
  the purge point and is never sent a snapshot for them.

  The model is hand-written for the same reason as its two siblings: the Rust is async and the
  extractor rejects `block_on`. Every field is an index watermark or a `Bool`, so the theorems are
  `decide` over a finite trace or `omega` over a step. What a partial apply *leaves behind* is
  `Ferrosa/Raft/Partial.lean`; this module is about whether the node comes back at all.
-/
import Ferrosa.Ledger
import Ferrosa.Raft.StateMachine

namespace Ferrosa.Raft

open Ferrosa.Ledger

/-! ## What is modelled, and at what abstraction

* `applied`   — `last_applied.index`; `0` plays the role of `None`, as in `StateMachine.lean`
  (entries are 1-based, so the encoding is order-isomorphic on reachable states). In memory only:
  a restart resets it and `boot` re-derives it from `disk`.
* `synced`    — the watermark the shared `Schema` and `StorageEngine` actually reflect. It moves
  with `applied` on the apply path (`state_machine.rs:476-479` publishes the staged metadata, whose
  schema and engine work already ran) and on a completed restore, and it does not move when the
  restore's sync fails or when `recover_from_purge_point` sets the index alone.
* `disk`      — the index in `state-machine.snapshot.bin`; `0` means no usable file. A file that
  exists but does not decode is `disk = 0` with the log still purged: that is the
  `NeedsPurgePointBaseline` case, and modelling it as "no snapshot" is what makes it visible.
* `purged`    — the log store's `last_purged_log_id`, which also bounds what replay can still read.
* `logEnd`    — the committed log's last index, i.e. what replay has to reach.
* `hole`      — the log's internal gap, if any (`log_store.rs:1019-1045`). It dominates the pair,
  as `classify_local_raft_state` does (`local_state.rs:84-86`), because both take the same reset
  path (`cluster.rs:1493-1526`).
* `latched`   — `apply_failure.is_some()`.
* `poison`    — the projection precondition is false *now*: `check_write_admission` refuses
  (`ferrosa-storage/src/engine.rs:3354-3372`, reached from every `engine.write` at `:10109`) or the
  commit-log fsync fails (`state_machine.rs:2196`). Nothing but `heal` clears it, so a trace that
  never heals models a persistent precondition: a full disk, a table that cannot be created, an IO
  error that does not go away.
* `rows`      — rows of the one table the next entry drops, as a read returns them. This is the only
  field a snapshot cannot restore: `SnapshotData` carries no row data, so nothing in the recovery
  path can raise it again.
* `snapHasTable` — whether the durable snapshot still describes that table. It predates the drop by
  construction, because an entry that failed its projection was never published and so was never
  snapshotted.
* `advertised`— whether `RaftState` advertises that table. `boot` derives it from `disk` and
  `snapHasTable`, which is what makes the lost-catalog case expressible.
* `serving`   — the node admits CQL data operations: `consensus_health.is_healthy()` and a known
  leader. The latch closes it in the same step; in the code that is the sm-worker exit → openraft
  `Fatal` → the consensus-metrics poller, a bounded delay rather than a may-forever, which is the
  one place this model is *kinder* than the code.
* `up`, `refused` — a process is running, or the classifier refused to start one.
-/

/-! ## The boot classification

`classify_local_raft_state` reads three durable facts and answers with one of four verdicts. The
model spells the same four, with the hole first for the reason `local_state.rs` gives: a hole makes
the log unreplayable however healthy the applied/purged pair looks.
-/

/-- What this node's persisted state can be used for, mirroring `LocalRaftState`. -/
inductive Verdict where
  /-- The log and state machine agree; start openraft on them. -/
  | usable
  /-- `last_applied` was lost but a purge point survives; replay must start there. -/
  | needsBaseline (purgePoint : Nat)
  /-- `last_applied` is behind `last_purged`: the entries between exist nowhere on this node. -/
  | stranded (applied purged : Nat)
  /-- A block of entries above the purge point is absent, so openraft cannot replay past it. -/
  | hole (lo hi : Nat)
  deriving DecidableEq, Repr

/-- The classifier. `purged = 0` is `None`, `applied = 0` is `None`, and a hole dominates. -/
def classify (hole : Option (Nat × Nat)) (applied purged : Nat) : Verdict :=
  match hole with
  | some (lo, hi) => .hole lo hi
  | none =>
    if purged = 0 then .usable
    else if applied = 0 then .needsBaseline purged
    else if applied < purged then .stranded applied purged
    else .usable

/-- A verdict that permits a process: `usable`, or a baseline the caller can recover from. The two
    others are refused. -/
def Verdict.admits : Verdict → Bool
  | .usable => true
  | .needsBaseline _ => true
  | .stranded _ _ => false
  | .hole _ _ => false

/-- **The classifier's one guarantee, restated.** A node whose durable state does not reach its
    purge point is `stranded`, never `usable`, and one with a hole in its log is `hole` — the two
    verdicts `cluster.rs:1493-1508` turns into a reset. -/
theorem classify_names_the_two_unusable_shapes :
    classify none 5 10 = .stranded 5 10
    ∧ classify none 0 10 = .needsBaseline 10
    ∧ classify none 10 10 = .usable
    ∧ classify (some (7, 9)) 10 10 = .hole 7 9 := by
  decide

/-! ## The machine

One structure holds every watermark the boot, replay and restart paths can move, and `sstep` is the
event that moves it. The events are the code's own operations, not an abstraction of them: `boot` is
the sequence at `controller/cluster.rs:1449-1516`, `restart` is the process loss that clears every
in-memory field, and `projectionFail` is the error branch `6c270518` added at
`state_machine.rs:454-464`. -/

/-- Everything startup, replay and restart can change. -/
structure Boot where
  /-- `last_applied.index`, in memory; `0` is `None`. -/
  applied : Nat
  /-- What the shared `Schema` and `StorageEngine` reflect. -/
  synced : Nat
  /-- The index in `state-machine.snapshot.bin`; `0` is no usable snapshot. -/
  disk : Nat
  /-- The log store's `last_purged_log_id`. -/
  purged : Nat
  /-- The committed log's last index. -/
  logEnd : Nat
  /-- The log's internal hole, if any. -/
  hole : Option (Nat × Nat)
  /-- `apply_failure.is_some()`. -/
  latched : Bool
  /-- The projection precondition is false now. -/
  poison : Bool
  /-- Rows of the table the next entry drops, as a read returns them. -/
  rows : Nat
  /-- The durable snapshot still describes that table. -/
  snapHasTable : Bool
  /-- `RaftState` advertises that table. -/
  advertised : Bool
  /-- The node admits CQL data operations. -/
  serving : Bool
  /-- A process is running. -/
  up : Bool
  /-- The classifier refused to start one. -/
  refused : Bool
  deriving DecidableEq, Repr

/-- Nothing has happened: no snapshot, no log, no process. -/
def Boot.initial : Boot where
  applied := 0
  synced := 0
  disk := 0
  purged := 0
  logEnd := 0
  hole := none
  latched := false
  poison := false
  rows := 0
  snapHasTable := false
  advertised := false
  serving := false
  up := false
  refused := false

/-- One event. -/
inductive SEv where
  /-- Start a process: restore the snapshot, sync the consumers, classify, decide. -/
  | boot
  /-- The same, with `sync_schema_and_engine_from_state` failing: `cluster.rs:1459-1465` warns and
  boots, so `synced` stays where it was while `applied` moves. -/
  | bootUnsynced
  /-- The next committed entry publishes: its ordered projection write returned `Ok`. Refused while
  the projection precondition is false, which is exactly what `6c270518` changed. -/
  | publish
  /-- The physical stage of the next entry lands. For a drop that means `unregister_table`
  (`engine.rs:5865-5899`) recorded a durable drop in `dropped-tables.json` and deleted the SSTable
  directory (`:6020-6059`) — before the ordered write is attempted. -/
  | applyDestructive
  /-- The ordered projection write for that entry returned an error: the job fails, the entry is
  unpublished, and the latch closes. -/
  | projectionFail
  /-- The projection precondition clears (space is freed, the table can be created). -/
  | heal
  /-- The process is lost and a new one starts. Nothing in memory survives; the file, the log and
  the rows on disk do. -/
  | restart
  deriving DecidableEq, Repr

/-- A fresh process: memory is empty, durable state is not. `apply_failure` is not in `SnapshotData`
and the constructor sets it to `None`, so the latch cannot survive this event. -/
def restartTo (s : Boot) : Boot :=
  { s with
    applied := 0
    synced := 0
    advertised := false
    latched := false
    serving := false
    up := false
    refused := false }

/-- Boot: construct a machine, restore the snapshot, sync the consumers, classify, decide.

`syncOk` is whether `sync_schema_and_engine_from_state` returned `Ok`; when it did not, `applied`
still takes the snapshot's index because `apply_snapshot_data` adopted it at
`state_machine.rs:848-849` before the sync at `:851` could fail. -/
def bootStep (s : Boot) (syncOk : Bool) : Boot :=
  let r := restartTo s
  let applied := if r.disk = 0 then 0 else r.disk
  let advertised := r.disk ≠ 0 && r.snapHasTable
  let synced := if syncOk then applied else 0
  let base := { r with applied := applied, synced := synced, advertised := advertised }
  match classify base.hole base.applied base.purged with
  | .usable => { base with up := true, serving := true }
  | .needsBaseline _ =>
    -- `recover_from_purge_point`: the index is adopted, the state is not touched.
    { base with up := true, serving := true, applied := base.purged }
  | .stranded _ _ => { base with refused := true }
  | .hole _ _ => { base with refused := true }

/-- One step. Events the code cannot perform are no-ops, and the two that need a precondition
(`publish` needs a projection that will succeed, `projectionFail` needs one that will not) are
guarded rather than left to the trace's discretion. -/
def sstep (s : Boot) (e : SEv) : Boot :=
  match e with
  | .boot => if s.up then s else bootStep s true
  | .bootUnsynced => if s.up then s else bootStep s false
  | .publish =>
    if s.up && !s.latched && !s.poison && s.applied < s.logEnd then
      { s with applied := s.applied + 1, synced := s.synced + 1 }
    else s
  | .applyDestructive => if s.up && !s.latched then { s with rows := 0 } else s
  | .projectionFail =>
    if s.up && !s.latched && s.poison then { s with latched := true, serving := false } else s
  | .heal => { s with poison := false }
  | .restart => restartTo s

/-- Run a trace. -/
def srun (s : Boot) (tr : List SEv) : Boot := tr.foldl sstep s

/-- `srun` splits over a concatenation, so a trace can be argued in pieces. -/
theorem srun_srun (s : Boot) (t₁ t₂ : List SEv) : srun s (t₁ ++ t₂) = srun (srun s t₁) t₂ := by
  simp [srun, List.foldl_append]

/-- `n` publishes. -/
def publishes (n : Nat) : List SEv := List.replicate n .publish

/-- One restart cycle of a node whose next entry cannot write its projection: die, boot, run the
physical stage, fail the projection. -/
def cycle : List SEv := [.restart, .boot, .applyDestructive, .projectionFail]

/-- `n` restart cycles. -/
def cycles (n : Nat) : List SEv := List.flatten (List.replicate n cycle)

/-! ## 1. What the classifier settles, and the one thing it cannot see

The strand family and the hole family are *repaired*: `cluster.rs:1518-1526` moves the raft
directory aside and rebuilds from the leader, which is a documented, reachable resync path. That is
requirement (c) of the operator's sentence, and it holds for both shapes.

What the classifier cannot see is the state behind the numbers. It compares `last_applied` against
`last_purged`; neither value says whether the `RaftState` at that index exists. Section 3 gives
both ways that goes wrong.
-/

/-- **A node only runs on a verdict that admits it.** Over every combination of snapshot index and
    purge point in a grid that includes the stranded, baseline and healthy cases, a boot that starts
    a process is a boot whose classification admitted it — so no shape the reset path exists for can
    become a serving node by accident of ordering. -/
theorem no_refused_verdict_starts_a_process :
    ([(0, 0), (0, 4), (3, 4), (4, 4), (4, 3), (5, 4), (10, 10)].map fun (d, p) =>
      let s := { Boot.initial with disk := d, purged := p }
      let out := sstep s .boot
      if out.up then Verdict.admits (classify none out.applied p) else true).all (· = true)
    = true := by
  decide

/-- **The boundary is the purge point.** Applied exactly at it runs; one index behind it does not,
    and the whole raft directory is reset rather than clamped forward — `local_state.rs`'s
    `stranded_is_never_reported_as_usable`, carried into the decision that starts a process. -/
theorem only_a_covered_apply_may_run :
    (sstep { Boot.initial with disk := 10, purged := 10, applied := 10 } .boot).up = true
    ∧ (sstep { Boot.initial with disk := 9, purged := 10, applied := 9 } .boot).up = false
    ∧ (sstep { Boot.initial with disk := 9, purged := 10, applied := 9 } .boot).refused = true
    ∧ (sstep { Boot.initial with hole := some (7, 9), disk := 10, purged := 10 } .boot).up
        = false := by
  decide

/-! ## 2. The latch is loud, and it is not durable

`6c270518` made every failed projection write latch the machine and return an error, so the set of
inputs that latch grew. Two things about that are settled here, and one is not.

Settled: a latched node does not serve. The error reaches openraft, the core goes fatal, and the
consensus-metrics poller closes health — so no operator is told the node is fine while it is not.

Not settled: the latch is a field on an in-memory structure. A restart clears it; nothing else
does, and nothing at boot checks whether the reason for it is still true.
-/

/-- **A latched node never serves.** Over every trace: if the machine holds a failure, it is not
admitted to traffic. `projectionFail` sets both fields together and no event sets the latch while
leaving the node serving, so the two cannot come apart. In the code this is the sm-worker exit at
`raft_core.rs:1415` → `Fatal` → `consensus_metrics.rs:91-103` → `is_cql_ready`
(`controller/mod.rs:959-964`). -/
theorem latch_preserved (s : Boot) (h : s.latched = true → s.serving = false) (e : SEv) :
    (sstep s e).latched = true → (sstep s e).serving = false := by
  intro hL
  cases e with
  | restart => simp [sstep, restartTo] at hL ⊢
  | boot =>
    by_cases hu : s.up
    · simp only [sstep, if_pos hu] at hL ⊢
      exact h hL
    · simp only [sstep, if_neg hu, bootStep, restartTo] at hL ⊢
      split at hL
      all_goals simp_all
  | bootUnsynced =>
    by_cases hu : s.up
    · simp only [sstep, if_pos hu] at hL ⊢
      exact h hL
    · simp only [sstep, if_neg hu, bootStep, restartTo] at hL ⊢
      split at hL
      all_goals simp_all
  | publish =>
    by_cases hg : s.up && !s.latched && !s.poison && s.applied < s.logEnd
    · simp only [sstep, if_pos hg] at hL ⊢
      exact h hL
    · simp only [sstep, if_neg hg] at hL ⊢
      exact h hL
  | applyDestructive =>
    by_cases hg : s.up && !s.latched
    · simp only [sstep, if_pos hg] at hL ⊢
      exact h hL
    · simp only [sstep, if_neg hg] at hL ⊢
      exact h hL
  | projectionFail =>
    by_cases hg : s.up && !s.latched && s.poison
    · simp only [sstep, if_pos hg]
    · simp only [sstep, if_neg hg] at hL ⊢
      exact h hL
  | heal =>
    simp only [sstep] at hL ⊢
    exact h hL

/-- **A latched node never serves, over every trace.** The coupling is an invariant the machine keeps:
`latch_preserved` is the one-step case and this is its induction. It is stated with the coupling as a
hypothesis rather than proved from nothing, because a state that already holds both fields is not one
the machine can produce. -/
theorem a_latched_node_never_serves (s : Boot) (h0 : s.latched = true → s.serving = false)
    (tr : List SEv) : (srun s tr).latched = true → (srun s tr).serving = false := by
  induction tr generalizing s with
  | nil => exact h0
  | cons e es ih => exact fun h => ih (sstep s e) (latch_preserved s h0 e) h

/-- **THE LATCH DOES NOT SURVIVE A RESTART.** `restart` is the only event that clears
`apply_failure`, and there is no event that persists it: `SnapshotData` carries `state`,
`last_applied` and `last_membership`, and the constructor at `state_machine.rs:580` sets the field
to `None`. So the latch describes one process, not one node. -/
theorem restart_clears_the_latch (s : Boot) : (sstep s .restart).latched = false := rfl

/-- **A NODE CAN LIVELOCK THROUGH RESTART CYCLES (DISPROVED).** With a persistent projection
failure, every restart ends exactly where the first one did: `applied` stays at the recovered
snapshot index however many times the node is restarted, because each cycle re-runs the same entry,
fails it again, and clears the latch that would have recorded the failure. This is the statement
over every start index, every log end and every table state in the grid — the grid is the model's
whole parameter space once `poison` is true and the boot is admitted, so "it depends" is not doing
any work here. -/
theorem restart_cycle_never_passes_a_poisoned_entry :
    ([(0, 0, 0, true), (0, 0, 4, true), (3, 0, 4, true), (3, 2, 4, true), (3, 0, 4, false),
      (4, 4, 5, true)].map fun (d, p, l, sh) =>
      let s : Boot :=
        { Boot.initial with
          disk := d, purged := p, logEnd := l, rows := 5, snapHasTable := sh
          poison := true, up := true, serving := true }
      let one := srun s (cycles 1)
      let three := srun s (cycles 3)
      three.applied == one.applied && three.latched == one.latched
        && three.rows == one.rows && three.disk == one.disk).all (· = true) = true := by
  decide

/-- The same claim as a trace of the field a reader cares about, on the reachable state: a node at
snapshot 3 with four committed entries and a full disk republishes nothing after three restarts. -/
theorem three_restarts_end_where_one_does :
    let s : Boot :=
      { Boot.initial with
        disk := 3, logEnd := 4, rows := 5, snapHasTable := true
        poison := true, up := true, serving := true }
    (srun s (cycles 3)).applied = 3
    ∧ (srun s (cycles 3)).latched = true
    ∧ (srun s (cycles 3)).serving = false
    ∧ (srun s (cycles 3)).rows = 0 := by
  decide

/-! ## 3. Two boot paths that claim more than they can honour

Both are reachable, both are silent in the sense that matters — the node runs, `/readyz` opens, and
nothing retries or repairs — and both have the same root: startup never compares the *state* it
holds against the *index* it reports.
-/

/-- **A FAILED RESTORE BOOTS A HEALTHY NODE IT CANNOT HONOUR (DISPROVED).** `apply_snapshot_data`
adopts `data.state` and `meta.last_log_id` (`state_machine.rs:848-849`) and only then syncs
(`:851-854`). `cluster.rs:1459-1465` logs the failure and boots. The result is a node whose
`RaftState` is at index 4 and whose schema and engine never saw it, and the classifier — asked
about `last_applied`, which is already 4 — reports `Usable`, because 4 is not behind the purge
point. -/
theorem failed_restore_boots_a_healthy_node :
    let s := srun
      { Boot.initial with
        disk := 4, logEnd := 5, snapHasTable := true, rows := 3, up := true }
      [.restart, .bootUnsynced]
    s.up = true ∧ s.serving = true ∧ s.applied = 4 ∧ s.synced = 0 := by
  decide

/-- **AN UNREADABLE SNAPSHOT SKIPS THE WHOLE PURGED LOG (DISPROVED).** Ten entries were applied and
purged; the snapshot file that covered them no longer decodes, so `recover_from_persisted_snapshot`
returns `Err` (`state_machine.rs:1173-1177`), `cluster.rs:1459-1465` warns, and `last_applied` is
`None`. The classifier reports `NeedsPurgePointBaseline`, and `recover_from_purge_point`
(`:621-632`) sets `last_applied` to the purge point without restoring any state. The node then runs
with an empty catalog, publishes entry 11 onwards, is never sent a snapshot for 1..10, and reports
`applied = 10` — so the pair the classifier checks agrees while the state behind it is gone. -/
theorem an_unreadable_snapshot_skips_the_purged_log :
    let s := srun
      { Boot.initial with
        purged := 10, logEnd := 12, snapHasTable := true, rows := 5 }
      [.boot, .publish]
    s.up = true ∧ s.serving = true ∧ s.applied = 11 ∧ s.advertised = false := by
  decide

/-- The same trace's second half: the two entries above the purge point are applied, and the ten
below it are not, because there is nothing left to read them from. -/
theorem the_purged_entries_are_never_replayed :
    let s := srun
      { Boot.initial with purged := 10, logEnd := 12 }
      [.boot, .publish, .publish]
    s.applied = 12 ∧ s.synced = 2 ∧ s.advertised = false := by
  decide

/-! ## 4. Replay progress

Replay from `last_applied` terminates: every publish strictly reduces `logEnd - applied`, and no
event but a restart raises it. It also re-runs exactly the entries whose projection did not land,
because publication — not the physical stage — is what advances `last_applied`
(`state_machine.rs:476-477`). What a replay then *re-encounters* is `Ferrosa/Raft/Partial.lean`.
-/

/-- One publish, with its guard discharged. -/
theorem publish_step (s : Boot) (hu : s.up = true) (hl : s.latched = false)
    (hp : s.poison = false) (hlt : s.applied < s.logEnd) :
    sstep s .publish = { s with applied := s.applied + 1, synced := s.synced + 1 } := by
  obtain ⟨a, y, d, p, l, h', lt, po, r, sh, ad, se, u, rf⟩ := s
  simp only at hu hl hp hlt
  subst hu hl hp
  simp [sstep, hlt]

/-- **REPLAY REACHES THE LOG END (PROVED).** From a node that can publish, `logEnd - applied`
publishes leave nothing to replay: publication is not optional, and a node that never latches
finishes the log. -/
theorem replay_reaches_the_log_end (s : Boot) (n : Nat)
    (h : s.applied + n = s.logEnd) (hup : s.up = true) (hl : s.latched = false)
    (hp : s.poison = false) :
    (srun s (publishes n)).applied = s.logEnd := by
  induction n generalizing s with
  | zero =>
    simp only [publishes, List.replicate_zero, srun, List.foldl_nil]
    omega
  | succ k ih =>
    have hlt : s.applied < s.logEnd := by omega
    rw [publishes, List.replicate_succ]
    show (srun (sstep s .publish) (List.replicate k .publish)).applied = s.logEnd
    rw [publish_step s hup hl hp hlt]
    exact ih { s with applied := s.applied + 1, synced := s.synced + 1 }
      (by simp; omega) hup hl hp

/-- **REPLAY RE-RUNS THE ENTRY WHOSE PROJECTION DID NOT LAND (PROVED).** Publication is withheld
when the ordered write fails (`state_machine.rs:454-464` returns before `:476`), so the entry is
still above `last_applied` and still below `logEnd`, which is exactly the range replay re-reads. -/
theorem a_withheld_entry_is_still_in_the_replay_range (s : Boot) (h : s.applied < s.logEnd) :
    s.applied + 1 ≤ s.logEnd := by omega

/-! ## 5. A bounded cross-check of the same invariant

The theorem above is an induction. The two below re-run the same predicates over every trace of a
small alphabet to depth four, so a step whose guard was mistyped fails here even if the induction
still closes. The bound is stated rather than implied, and the alphabet deliberately omits `heal`
and `bootUnsynced`: it is checking the latch/readiness coupling, not the whole machine.
-/

/-- Every trace over `{boot, publish, applyDestructive, projectionFail, restart}` to depth `n`,
built breadth-first so the depth is the list's own bound rather than a recursion argument. -/
def smallTraces : Nat → List (List SEv)
  | 0 => [[]]
  | n + 1 =>
    let alphabet : List SEv := [.boot, .publish, .applyDestructive, .projectionFail, .restart]
    let prev := smallTraces n
    prev ++ List.flatten (prev.map fun t => alphabet.map fun e => t ++ [e])

/-- **A latched node is never a serving node, over every small trace.** -/
theorem no_small_trace_is_both_latched_and_serving :
    (smallTraces 3).all (fun tr =>
      !((srun Boot.initial tr).latched && (srun Boot.initial tr).serving)) = true := by
  decide

/-- **The classifier's refusal is never undone by replay.** No small trace turns a refused boot into
a running node, because `restart` is the only event that clears `refused` and it clears `up` in the
same step. -/
theorem refusal_is_not_undone_by_replay :
    (smallTraces 3).all (fun tr =>
      !((srun Boot.initial tr).refused && (srun Boot.initial tr).up)) = true := by
  decide

/-! ## The ledger

Five obligations on startup and replay, three of them red. The two that hold are what replay
guarantees when the projection can be written; the three that do not are the families that make a
node stop — two silently, because startup reports an index it cannot honour, and one loudly, because
the latch closes the node instead of repairing it.

`Ferrosa/Raft/Obligations.lean` appends this list to the area ledger, so the count and the
classification are checked there rather than asserted here.
-/

/-- A startup inconsistency must be repaired in place or quarantined explicitly; serving and refusing
    to boot are no longer discharges. `Ferrosa/Raft/Quarantine.lean` models the design. -/
def startupItems : List SpecItem := [
  SpecItem.mk "a startup inconsistency is repaired in place, serves consistent state, or stays quarantined naming the cause" (Status.failing
     "two boot paths report an index the node cannot honour. apply_snapshot_data_from adopts state and last_applied (ferrosa-cluster/src/raft/state_machine.rs:959-960) before sync_schema_and_engine_from_state (:1100-1180) can fail, and controller/cluster.rs:1467-1472 logs that failure and boots, so the classifier - asked about the already-adopted last_applied (:1492-1496) - answers Usable; and when the snapshot does not decode at all, last_applied stays None, recover_from_purge_point (:713-724) sets it to the purge point without restoring state, and the node publishes above the purge point forever with an empty RaftState. Both run, both open readiness, and neither has a retry or a repair: /readyz (ferrosa/src/web/readiness.rs:191-372) and is_cql_ready (ferrosa-cluster/src/controller/mod.rs:994-999) never compare the state the node would answer from against the index it reports. The third branch is not a discharge either: the path that returns without openraft (controller/cluster.rs:1588-1594, then :1439-1445) leaves the node up and permanently outside the cluster (Raft.the_refusal_path_is_terminal). What the operator now requires is quarantine-and-repair: refuse traffic, stay a member, pull the state from a peer, and verify it before serving again - modelled in Ferrosa/Raft/Quarantine.lean, where Raft.a_sufficient_peer_repairs_the_node is the proof that it works and Raft.the_code_answers_from_state_it_does_not_have is the code that does not have it"
     "Raft.an_unreadable_snapshot_skips_the_purged_log"),
  SpecItem.mk "the apply latch survives a restart" (Status.failing
     "apply_failure is a plain Option<String> on the in-memory state machine (ferrosa-cluster/src/raft/state_machine.rs:435), absent from SnapshotData (:390-400), and the constructor sets it to None (:670), so every restart clears it: the latch describes a process, not a node, and nothing at boot asks whether the condition that set it is still true. Under quarantine the same requirement is stronger and still unmet - the quarantine reason and any repair position must survive a restart too, and neither has a durable field (Raft.a_restart_forgets_the_repair_position)"
     "Raft.restart_clears_the_latch"),
  SpecItem.mk "a restart cannot repeat a persistent projection failure without progress" (Status.failing
     "a persistent precondition - a disk below its write reserve refusing engine.write via check_write_admission (ferrosa-storage/src/engine.rs:3354-3372, :10109), or a commit-log fsync that keeps failing (ferrosa-cluster/src/raft/state_machine.rs:2196) - re-fails the same entry on every boot. The cycle restart, boot, physical, projectionFail is a fixed point, so applied never passes the entry and the node alternates between openraft Fatal and replaying the same failure; 6c270518 widened this set by making every WarnReplay site fatal as well. Repairing in place would discharge it - Raft.three_repair_cycles_pass_the_poisoned_entry proves the entry is passed once the node adopts a peer's state instead of re-running its own - but the code still has no such path and no repair position to resume from (Raft.the_code_never_passes_the_poisoned_entry)"
     "Raft.restart_cycle_never_passes_a_poisoned_entry"),
  SpecItem.mk "replay re-runs every entry whose ordered projection write did not land" (Status.met "Raft.a_withheld_entry_is_still_in_the_replay_range"),
  SpecItem.mk "replay of the committed log terminates" (Status.met "Raft.replay_reaches_the_log_end")
]

/-- Five obligations on startup and replay, three of them unmet, pinned here so closing one or
adding a claim forces this table to move in the same change. -/
theorem startup_items_pinned : startupItems.length = 5 ∧ redCount startupItems = 3 := by
  decide

/-! ## The axiom audit

The trace-level theorems are finite computations and report no axioms; the inductions report
`propext` and `Quot.sound`, which `decide` and `omega` introduce. None reports `sorryAx`.
-/

#print axioms a_latched_node_never_serves
#print axioms restart_clears_the_latch
#print axioms restart_cycle_never_passes_a_poisoned_entry
#print axioms three_restarts_end_where_one_does
#print axioms failed_restore_boots_a_healthy_node
#print axioms an_unreadable_snapshot_skips_the_purged_log
#print axioms publish_step
#print axioms replay_reaches_the_log_end
#print axioms no_small_trace_is_both_latched_and_serving

end Ferrosa.Raft
