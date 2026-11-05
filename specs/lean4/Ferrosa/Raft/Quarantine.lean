/-
  Quarantine and online repair: the discharge for a startup inconsistency
  ======================================================================

  `Ferrosa/Raft/Startup.lean` settled that a node which discovers an inconsistency at boot does one
  of three things today: it serves as if healthy (`failed_restore_boots_a_healthy_node`,
  `an_unreadable_snapshot_skips_the_purged_log`), it stops loudly and forever (`apply_failure` →
  openraft `Fatal` → `consensus_health.fail`), or it throws its raft directory aside and rebuilds
  from the leader (`controller/cluster.rs:1531-1552`). The operator has since ruled out the third
  shape's failure branch — "refusing to boot" is not a discharge — and ruled out the first
  entirely. The requirement is **repair and quarantine**: a node that cannot honour its state stops
  answering, stays a member, repairs from a peer while it runs, and resumes only after checking that
  the state it holds is the state it claims.

  This module models that lifecycle and asks the operator's six questions of it. It is a *design*
  model with the current code's behaviour carried as a second policy in the same machine
  (`Policy.serveAnyway`), so every obligation is settled twice: a theorem about the design, and a
  trace showing what the code does instead. The second half is what an implementer acts on.

  ## What the code has today, and what it does not

  **There is no node-level quarantine.** `quarantin*` appears over a thousand times in the tree and
  every occurrence is SSTable- or range-level: `ferrosa-cluster/src/repair/trigger.rs:1-20` wires
  "quarantine → anti-entropy refill" for a corrupt table, `ferrosa/src/main.rs:1798` quarantines a
  schema file. Nothing in `ModeController` or `FerrosStateMachine` can say "this node's state is
  inconsistent; refuse traffic and repair".

  **Readiness has no inconsistency term.** `ModeController::is_cql_ready`
  (`controller/mod.rs:994-999`) consults `consensus_health`, `write_path`, `ddl_path` and
  `declared_topology_is_ready` — four things, none of which is "the state I would answer from
  matches the index I report". `/readyz` (`ferrosa/src/web/readiness.rs:191-372`) adds supervised
  tasks, failed listeners, set-aside mutations, vector-index rebuilds and Raft leader liveness. A
  node holding `last_applied = 10` and an empty `RaftState` passes every one of them.

  **The snapshot builder has no consistency check.** `build_snapshot`
  (`raft/state_machine.rs:2528-2583`) serialises `self.state` at `self.last_applied` and then calls
  `mark_durably_applied` (`:2576`), which is what lets the log store purge up to that index
  (`log_store.rs:1325-1355`). The only thing that can stop it is `apply_failure`, through
  `finish_pending_apply` (`:527-529`) — so the *unsynced but unlatched* node is exactly the one that
  snapshots its unhonoured state and then purges the entries that could have repaired it.

  **The repair channel is closed by the latch.** `install_snapshot` (`:2801-2837`) begins with
  `finish_pending_apply` (`:2804`), which returns the latched error and nothing else once
  `apply_failure` is set (`:527-529`). The node that most needs a snapshot is the one that cannot
  install it.

  **Durable state is replaced, not merged.** `apply_snapshot_data_from` assigns
  `self.state = data.state` (`:959`) and `last_applied = meta.last_log_id` (`:960`). The
  `absent`/`retired` guards (`:947-952`, `:997-1016`) refuse a snapshot that *drops* a table with
  local artifacts, and `45a6af1d` refuses a *persisted* startup snapshot that cannot establish
  replacement order (`:1138-1146`). Neither merges: a table present on both sides is taken wholesale
  from the snapshot, so a locally durable row newer than the peer's index is dropped with no record.
  A repair merge by timestamp is no way out either — the projection has no per-cell generation
  (`Ferrosa/SystemTables/Projection.lean:13-24`), so it inherits `tsOnly_tie_fails_convergence`.

  **Voting has no state-machine term at all.** `RaftVoteHandler` (`raft/handlers.rs:1014-1019`)
  forwards straight to `raft.vote(req)`. A node whose projection diverged from its applied index —
  the failed-restore shape — answers votes and is counted toward quorum.

  **The failure state is not durable and not repairable.** `apply_failure` is a plain field on the
  in-memory machine (`:435`), absent from `SnapshotData` (`:390-400`), set to `None` by the
  constructor (`:670`). `ConsensusHealth` (`consensus_health.rs:97-125`) has `fail`, `is_healthy`,
  `failure` and `failure_count` and no reset, so a failure is process-lifetime: the only way back is
  a restart, and a restart re-derives the same failure from the same durable facts.

  ## What is modelled, and at what abstraction

  Every field is an index watermark, a small count or a `Bool`, so a trace reduces under `decide` and
  the inductions close on the step function.

  * `phase`      — `healthy`, `quarantined r`, `repairing src`, `verifying src c`.
  * `applied`    — `last_applied.index`; `synced` — what the `Schema`, the `StorageEngine` and
                   `RaftState` actually reflect. `consistent` is `synced = applied`.
  * `delivered`  — the highest index whose *content* has been received from a peer.
  * `claim`      — the highest index a peer has merely *said* this node is at.
  * `peerIndex`  — the highest index a reachable peer can actually supply; `0` means none can.
  * `purgePoint` — the log store's `last_purged_log_id`, which is also the floor a repair must cover.
  * `newerRows`  — locally durable rows above what the peer can supply; `pendingRows` — those rows
                   once preserved, awaiting an explicit decision.
  * `disk`, `restoreOk` — the persisted snapshot index, and whether restoring it synced the consumers.
  * `latched`    — `apply_failure.is_some()`: the code's response to a failed apply, and the gate
                   `install_snapshot` sits behind.
  * `serving`, `reportsReady`, `snapshots`, `votes`, `counted` — the five surfaces a quarantined node
                   must keep shut: client reads and writes, the readiness/health endpoint, the
                   snapshot builder, its own vote, and its presence in a quorum count.
-/
import Ferrosa.Ledger
import Ferrosa.Raft.Startup
import Ferrosa.SystemTables.Projection

namespace Ferrosa.Raft

open Ferrosa.Ledger

/-! ## 1. The states, and what a repair source is

`Quarantined` carries a reason because the reason is what an operator and a peer both need: a node
that says "unreadable snapshot at 10, purge point 10" can be repaired, one that says "unhealthy"
cannot be triaged. `Repairing` carries the source because the two sources have different sufficiency
conditions — a snapshot at index `n` covers everything below `n` by itself, a log replay needs the
peer's prefix to start at `last_applied + 1` — and `Node.need` is the one bound that covers both.
-/

/-- Why a node will not answer. The shapes `Startup.lean` and `local_state.rs:27-58` name, carried
    as the reason a quarantined node reports. -/
inductive QReason where
  /-- `recover_from_persisted_snapshot` returned `Err`: the file exists and does not decode. -/
  | snapshotUnreadable
  /-- The snapshot covers `covers`, and the purge point is `shortOf`: the entries between exist
      nowhere on this node. -/
  | snapshotShort (covers shortOf : Nat)
  /-- `RaftState` reflects `synced` and `last_applied` says `applied`. -/
  | projectionBehind (applied synced : Nat)
  /-- `apply_failure` names an index above `last_applied`: a persistent local failure. -/
  | latchedAt (idx : Nat)
  deriving DecidableEq, Repr

/-- Where a repair comes from. -/
inductive Source where
  /-- A peer's snapshot at index `idx`; it covers everything below `idx` by itself. -/
  | peerSnapshot (idx : Nat)
  /-- A peer's log from `from_`; it needs every index from there to the commit point. -/
  | peerLog (from_ : Nat)
  deriving DecidableEq, Repr

/-- The lifecycle. `Verifying` carries the index the peer claims as well as the source, because the
    claim and the received content are two different facts and only the second may be adopted. -/
inductive Phase where
  | healthy
  | quarantined (r : QReason)
  | repairing (src : Source)
  | verifying (src : Source) (claimed : Nat)
  deriving DecidableEq, Repr

/-- Which machine is being modelled. `quarantine` is the design; `serveAnyway` is the code as it
    stands — no quarantine state, gates never closed for a silent boot path, a latch for the loud
    one; `trustClaims` is the tempting wrong design, included so that the false exit is a trace
    rather than an argument. -/
inductive Policy where
  | quarantine
  | serveAnyway
  | trustClaims
  deriving DecidableEq, Repr

/-! ## 2. The node

One structure holds every field the three policies can move, so a disproof and a proof are traces of
the same machine and differ only in the policy argument.
-/

/-- Everything the lifecycle can change. -/
structure Node where
  /-- The lifecycle state. -/
  phase : Phase
  /-- A process is running. A quarantined node is `up`: it is a member, not a corpse. -/
  up : Bool
  /-- `last_applied.index`. -/
  applied : Nat
  /-- What the `Schema`, the `StorageEngine` and `RaftState` reflect. -/
  synced : Nat
  /-- The highest index whose content this node has received from a peer. -/
  delivered : Nat
  /-- The highest index a peer has merely claimed. -/
  claim : Nat
  /-- The highest index a reachable peer can actually supply; `0` means none can. -/
  peerIndex : Nat
  /-- The log store's `last_purged_log_id`. -/
  purgePoint : Nat
  /-- Locally durable rows above what the peer can supply. -/
  newerRows : Nat
  /-- Those rows once preserved, awaiting an explicit decision. -/
  pendingRows : Nat
  /-- The index in `state-machine.snapshot.bin`; `0` means no usable file. -/
  disk : Nat
  /-- `sync_schema_and_engine_from_state` returned `Ok` during the restore. -/
  restoreOk : Bool
  /-- `apply_failure.is_some()`. -/
  latched : Bool
  /-- Client reads and writes are admitted. -/
  serving : Bool
  /-- `/readyz` and `/health` answer ready. -/
  reportsReady : Bool
  /-- `get_snapshot_builder` may build. -/
  snapshots : Bool
  /-- This node may answer a vote request. -/
  votes : Bool
  /-- This node is counted toward a quorum. -/
  counted : Bool
  deriving DecidableEq, Repr

/-- Nothing has happened: no state, no process, healthy by vacuity. -/
def Node.initial : Node where
  phase := .healthy
  up := false
  applied := 0
  synced := 0
  delivered := 0
  claim := 0
  peerIndex := 0
  purgePoint := 0
  newerRows := 0
  pendingRows := 0
  disk := 0
  restoreOk := true
  latched := false
  serving := false
  reportsReady := false
  snapshots := false
  votes := false
  counted := false

/-- The consistency this whole module is about: the state a read would be answered from is the state
    the published index claims. -/
def Node.consistent (s : Node) : Bool := s.synced == s.applied

/-- Whether the node is not healthy. Derived from `phase`, so it cannot drift from it. -/
def Node.inQ (s : Node) : Bool :=
  match s.phase with
  | .healthy => false
  | _ => true

/-- Every surface a quarantined node must keep shut, as one predicate: client traffic, the readiness
    endpoint, the snapshot builder, this node's vote, and its count toward quorum. -/
def Node.closed (s : Node) : Bool :=
  !s.serving && !s.reportsReady && !s.snapshots && !s.votes && !s.counted

/-- The soundness predicate: an open client surface implies consistency. Stated as one expression so
    the step lemma is a single `simp` rather than one implication per surface. -/
def Node.safe (s : Node) : Bool := (!s.serving && !s.reportsReady) || s.consistent

/-- This node's vote and its presence in a quorum count, as one predicate. -/
def Node.uncounted (s : Node) : Bool := !s.votes && !s.counted

/-- What a client read is answered from. `none` is a refusal, and a refusal is an answer a client can
    act on; `some k` is a claim that the value is the state at index `k`. -/
def Node.answer (s : Node) : Option Nat :=
  if s.serving then some s.synced else none

/-- What `/readyz` would say about the state index. -/
def Node.status (s : Node) : Option Nat :=
  if s.reportsReady then some s.synced else none

/-- The floor a repair must cover: the state this node actually holds (`synced`), and the purge point
    below which nothing exists on this node at all. `applied` is deliberately **not** part of it — an
    `applied` index above `synced` is a claim the node cannot honour, and repairing exactly that claim
    is the point of the module. A log prefix below `synced` is no use (the node already holds that
    state) and a snapshot below the purge point cannot be checked against the log, so one bound
    covers both sources. -/
def Node.need (s : Node) : Nat := max s.synced s.purgePoint

/-- Whether a reachable peer can supply enough. -/
def Node.sufficient (s : Node) : Bool := s.peerIndex ≥ s.need

/-! ## 3. The events, and the three policies

The event alphabet is the adversarial scheduler the operator asked for: `wait` is arbitrary delay,
`deliver`/`claimCaughtUp` are a peer that may send state or only a message, duplicates and reorderings
are absorbed because `deliver` and `claimCaughtUp` are `max`es, and `restart` is process loss at any
point. `deliver` is additionally gated on `idx ≤ peerIndex`, because the network cannot deliver state
no peer holds.
-/

/-- One event. -/
inductive QEv where
  /-- Boot or verification discovers an inconsistency, with its reason. -/
  | detect (r : QReason)
  /-- Ask a peer for state. -/
  | request
  /-- A peer supplies the content of index `idx` (a snapshot, or a log prefix ending there). -/
  | deliver (idx : Nat)
  /-- A peer asserts this node is at `idx`, supplying no state. -/
  | claimCaughtUp (idx : Nat)
  /-- Check the received content against the peer's, and adopt it if it is sufficient. -/
  | verify
  /-- Build a snapshot at `applied`. -/
  | buildSnapshot
  /-- A vote request arrives. -/
  | vote
  /-- A client read or write arrives. -/
  | traffic
  /-- Arbitrary delay. -/
  | wait
  /-- The process is lost; durable state survives. -/
  | restart
  deriving DecidableEq, Repr

/-- Enter quarantine: every surface shuts in the same step, which is the whole of soundness. -/
def quarantineTo (s : Node) (r : QReason) : Node :=
  { s with
    phase := .quarantined r
    serving := false
    reportsReady := false
    snapshots := false
    votes := false
    counted := false }

/-- Adopt the state at `n` and open the surfaces, because they now agree. This is the design's exit:
    the applied index and what the consumers reflect move together. -/
def healthyAt (s : Node) (n : Nat) : Node :=
  { s with
    phase := .healthy
    applied := n
    synced := n
    serving := s.up
    reportsReady := s.up
    snapshots := s.up
    votes := s.up
    counted := s.up }

/-- Adopt the state at `n` and change no gate: the code's `apply_snapshot_data`, which assigns
    `self.state` and `last_applied` and leaves readiness to whatever else was already answering. -/
def adoptState (s : Node) (n : Nat) : Node :=
  { s with phase := .healthy, applied := n, synced := n }

/-- Adopt an *index* at `n` and leave `synced` where it was, reopening the surfaces as if the state
    had come with it: `recover_from_purge_point` (`state_machine.rs:713-724`) plus the classifier's
    `Usable`, which sets `last_applied` without touching `state` and lets the node serve again. The
    node then reports the index as applied while holding the state it had. -/
def adoptClaimed (s : Node) (n : Nat) : Node :=
  { s with
    phase := .healthy
    applied := n
    serving := s.up
    reportsReady := s.up
    snapshots := s.up
    votes := s.up
    counted := s.up }

/-- One step.

The design and the code differ in exactly four events, and each difference is one of the obligations
below: `detect` (quarantine, or keep answering), `request` (a latch that closes the repair channel),
`verify` (adopt only received content, and keep the newer local rows) and `restart` (re-enter
quarantine rather than report healthy). -/
def qstep (pol : Policy) (s : Node) (e : QEv) : Node :=
  match e with
  | .detect r =>
    match pol with
    | .serveAnyway =>
      -- The code has no quarantine state. A failed apply latches and closes health loudly; every
      -- boot-path inconsistency is adopted and the node keeps answering.
      match r with
      | .latchedAt _ =>
        { s with phase := .healthy, latched := true, serving := false, reportsReady := false }
      | .snapshotShort _ shortOf =>
        -- `NeedsPurgePointBaseline` → `recover_from_purge_point`, then `Usable`: the index is
        -- adopted, the state is not, and the classifier's own pair now agrees.
        { s with phase := .healthy, applied := max s.applied shortOf }
      | _ => { s with phase := .healthy }
    | _ => quarantineTo s r
  | .request =>
    match s.phase with
    | .quarantined _ =>
      if s.latched then
        -- `install_snapshot` begins with `finish_pending_apply` (:2804), which returns the latched
        -- error (:527-529). The latch is the reason the repair cannot start.
        s
      else
        { s with
          phase :=
            .repairing
              (if s.sufficient then .peerSnapshot s.peerIndex else .peerLog (s.applied + 1)) }
    | _ => s
  | .deliver idx =>
    match pol with
    | .serveAnyway =>
      -- openraft may call `install_snapshot` at any time, so no local phase gates it; the latch
      -- does, through `finish_pending_apply`.
      if s.latched then s
      else { s with delivered := max s.delivered idx, phase := .verifying (.peerSnapshot idx) idx }
    | _ =>
      if idx ≤ s.peerIndex then
        match s.phase with
        | .repairing src => { s with delivered := max s.delivered idx, phase := .verifying src idx }
        | .verifying src c => { s with delivered := max s.delivered idx, phase := .verifying src c }
        | _ => s
      else s
  | .claimCaughtUp idx =>
    -- A message with no state behind it. It is recorded and it changes nothing else, which is the
    -- difference between `claim` and `delivered`.
    { s with claim := max s.claim idx }
  | .verify =>
    match pol with
    | .serveAnyway =>
      if s.latched then s
      else if s.delivered ≥ s.need then
        -- `self.state = data.state` (:959): the state at `applied` is replaced, and nothing records
        -- what the replaced state held.
        { adoptState s s.delivered with pendingRows := 0 }
      else s
    | .trustClaims =>
      -- The tempting wrong design: the peer's word is the evidence, so an index is adopted with
      -- nothing behind it.
      match s.phase with
      | .verifying _ _ =>
        if s.claim ≥ s.need then adoptClaimed s s.claim
        else s
      | .repairing _ =>
        if s.claim ≥ s.need then adoptClaimed s s.claim
        else s
      | _ => s
    | .quarantine =>
      match s.phase with
      | .repairing _ =>
        if s.delivered ≥ s.need then
          { healthyAt s s.delivered with pendingRows := s.newerRows }
        else s
      | .verifying _ _ =>
        if s.delivered ≥ s.need then
          { healthyAt s s.delivered with pendingRows := s.newerRows }
        else s
      | _ => s
  | .buildSnapshot =>
    if s.snapshots && !s.latched then
      -- `persist_snapshot_to_disk` at `last_applied`, then `mark_durably_applied` (:2576), which lets
      -- the log purge to that index. `restoreOk` records the consequence: a snapshot that decodes is
      -- a snapshot whose restore succeeds, whatever it holds.
      { s with
        purgePoint := max s.purgePoint s.applied
        disk := s.applied
        restoreOk := true }
    else s
  | .vote =>
    match s.phase with
    | .healthy => { s with votes := s.up, counted := s.up }
    | _ => s
  | .traffic => s
  | .wait => s
  | .restart =>
    let applied := s.disk
    let synced := if s.restoreOk then s.disk else 0
    let ok := s.restoreOk
    match pol with
    | .quarantine =>
      -- Deterministic: if the restore did not sync, the node comes back quarantined, never healthy
      -- on the strength of a partial repair.
      { s with
        applied := applied
        synced := synced
        phase := if ok then .healthy else .quarantined (.projectionBehind applied synced)
        delivered := 0
        claim := 0
        latched := false
        serving := ok && s.up
        reportsReady := ok && s.up
        snapshots := ok && s.up
        votes := ok && s.up
        counted := ok && s.up }
    | _ =>
      -- The code: there is no quarantine, so the node returns to answering whatever the restore did.
      { s with
        applied := applied
        synced := synced
        phase := .healthy
        delivered := 0
        claim := 0
        latched := false
        serving := true
        reportsReady := true
        snapshots := true
        votes := true
        counted := true }

/-- Run a trace. -/
def qrun (pol : Policy) (s : Node) (tr : List QEv) : Node := tr.foldl (qstep pol) s

/-- `qrun` splits over a concatenation. -/
theorem qrun_qrun (pol : Policy) (s : Node) (t₁ t₂ : List QEv) :
    qrun pol s (t₁ ++ t₂) = qrun pol (qrun pol s t₁) t₂ := by
  simp [qrun, List.foldl_append]

/-- The trace a repairing node runs once a peer can supply: ask, receive, check. -/
def repairTrace (idx : Nat) : List QEv := [.request, .deliver idx, .verify]

/-- `n` delays. -/
def stalled (n : Nat) : List QEv := List.replicate n .wait

/-- `n` messages claiming the node is caught up. -/
def claims (n : Nat) : List QEv := List.replicate n (.claimCaughtUp 0)

/-- A state in which no peer can help: nothing above, no usable snapshot, the builder shut, and the
    purge point equal to what the node holds. Every conjunct is preserved by every design event,
    which is what makes "no peer, no exit" an induction rather than a case. -/
def Node.NoPeer (s : Node) : Prop :=
  s.peerIndex = 0 ∧ s.restoreOk = false ∧ s.disk = 10 ∧ s.applied = 10
    ∧ s.purgePoint = 10 ∧ s.delivered = 0 ∧ s.snapshots = false ∧ s.up = true

/-! ## 4. OBLIGATION 1 — quarantine soundness

No client-visible answer is derived from inconsistent state. "Client-visible" is three surfaces here,
because the code has three and all three are client-visible: the CQL data path (`answer`), the
readiness endpoint (`status`), and the snapshot builder — client-visible in the only sense that
matters, since it publishes this node's state as the cluster's truth and then permits the log to be
purged against it.

The theorem is an induction, not a definition: `serving`, `reportsReady` and `snapshots` are
independent fields of the same structure the code's policy uses, so a step that opened one while
`synced ≠ applied` would be a counterexample rather than a type error.
-/

/-- **One step of the design keeps an open surface honest.** `detect` shuts both client surfaces in
    the same step; `restart` re-derives them from the restore; `verify` opens them only after
    assigning `synced := applied`; every other event leaves both the gates and the watermarks alone. -/
theorem safe_step (s : Node) (h : s.safe = true) (e : QEv) :
    (qstep .quarantine s e).safe = true := by
  cases e <;> simp only [qstep] <;> (try (repeat' split)) <;>
    simp_all [Node.safe, Node.consistent, quarantineTo, healthyAt]

/-- **The invariant holds of every reachable state**, by induction on the trace. -/
theorem safe_run (s : Node) (h : s.safe = true) (tr : List QEv) :
    (qrun .quarantine s tr).safe = true := by
  induction tr generalizing s with
  | nil => simpa [qrun] using h
  | cons e es ih => exact ih (qstep .quarantine s e) (safe_step s h e)

/-- **QUARANTINE SOUNDNESS (design, proved).** Over every trace, from a node whose open surfaces imply
    consistency, no client read and no readiness answer is produced from state that disagrees with
    the published index. This is the property `ModeController::is_cql_ready`
    (`controller/mod.rs:994-999`) and `/readyz` (`ferrosa/src/web/readiness.rs:191-372`) do not have,
    because neither of them reads `synced` at all. -/
theorem no_answer_from_inconsistent_state (s : Node) (h : s.safe = true) (tr : List QEv) :
    ((qrun .quarantine s tr).answer.isSome = true → (qrun .quarantine s tr).consistent = true)
    ∧ ((qrun .quarantine s tr).status.isSome = true → (qrun .quarantine s tr).consistent = true) := by
  have hs := safe_run s h tr
  constructor
  · intro hx
    have hserv : (qrun .quarantine s tr).serving = true := by simpa [Node.answer] using hx
    simpa [Node.safe, hserv] using hs
  · intro hx
    have hr : (qrun .quarantine s tr).reportsReady = true := by simpa [Node.status] using hx
    simpa [Node.safe, hr] using hs

/-- **A QUARANTINED NODE KEEPS EVERY SURFACE SHUT (design, proved).** Over every trace, a node in
    quarantine serves nothing, reports nothing ready, builds no snapshot, casts no vote and is
    counted in no quorum. Being shut is what soundness is: there is no path from an inconsistent
    state to an answer. -/
theorem a_quarantined_node_keeps_every_surface_shut (s : Node)
    (h : s.inQ = true → s.closed = true) (tr : List QEv) :
    (qrun .quarantine s tr).inQ = true → (qrun .quarantine s tr).closed = true := by
  induction tr generalizing s with
  | nil => exact h
  | cons e es ih =>
    intro hq
    refine ih (qstep .quarantine s e) ?_ hq
    intro hq'
    cases e <;> simp only [qstep] at hq' ⊢ <;> (try (repeat' split)) <;>
      simp_all [Node.inQ, Node.closed, quarantineTo, healthyAt]

/-- **THE CODE ANSWERS FROM STATE IT DOES NOT HAVE (DISPROVED).** `apply_snapshot_data_from` adopts
    `last_applied` from the snapshot (`state_machine.rs:960`) and `controller/cluster.rs:1467-1472`
    downgrades a failed `sync_schema_and_engine_from_state` to a warning and boots, so the node comes
    up with `last_applied = 4` and a `RaftState` the schema and engine never saw. Nothing in
    `is_cql_ready` or `/readyz` reads the difference, so the read is admitted and answered at an
    index whose state does not exist. -/
theorem the_code_answers_from_state_it_does_not_have :
    let s : Node := { Node.initial with disk := 4, restoreOk := false, up := true, peerIndex := 4 }
    let out := qrun .serveAnyway s [.restart, .traffic]
    out.answer = some 0 ∧ out.synced = 0 ∧ out.applied = 4 ∧ out.consistent = false
      ∧ out.reportsReady = true := by
  decide

/-- **A QUARANTINED NODE CANNOT BE SNAPSHOTTED INTO CONSISTENCY (design, proved).** The builder is
    shut, so no snapshot is published at an index the node cannot honour and the purge point does not
    advance. The log entries below `applied` are still there to replay, which is what a repair needs. -/
theorem a_quarantined_node_cannot_be_snapshotted_into_consistency :
    let s : Node :=
      { Node.initial with
        phase := .quarantined (.projectionBehind 4 0)
        up := true
        applied := 4
        synced := 0
        peerIndex := 4
        disk := 4
        restoreOk := false
        snapshots := false }
    let out := qrun .quarantine s [.buildSnapshot, .restart]
    out.purgePoint = 0 ∧ out.disk = 4 ∧ out.synced = 0 ∧ out.consistent = false := by
  decide

/-- **THE CODE'S SNAPSHOT BUILDER MAKES A NODE CONSISTENT BY FIAT (DISPROVED).** The same boot, then
    one `get_snapshot_builder` round. `build_snapshot` (`state_machine.rs:2528-2583`) serialises
    `self.state` at `self.last_applied` with no consistency check, persists it and calls
    `mark_durably_applied` (`:2576`), which is what the log store's purge guard trusts
    (`log_store.rs:1325-1355`). The next boot restores that file successfully — it decodes, and it
    says the state is what it is — so the node reports `applied = synced = 4` with an empty
    `RaftState`, and the entries that could have rebuilt it are now purgable. -/
theorem the_snapshot_builder_makes_a_node_consistent_by_fiat :
    let s : Node := { Node.initial with disk := 4, restoreOk := false, up := true, peerIndex := 4 }
    let out := qrun .serveAnyway s [.restart, .buildSnapshot, .restart]
    out.consistent = true ∧ out.synced = 4 ∧ out.applied = 4 ∧ out.purgePoint = 4
      ∧ out.serving = true := by
  decide

/-! ## 5. OBLIGATION 2 — no false exit

Quarantine is left only after verifying consistency, not after receiving a message that claims it.
The two events are separate fields on the structure (`claim` and `delivered`) because in the code they
are separate facts and only one of them is consulted anywhere: the classifier compares `last_applied`
against `last_purged` (`local_state.rs:88-102`) — two numbers — and `recover_from_purge_point`
(`state_machine.rs:713-724`) adopts the purge point as `last_applied` with no state behind it. That
adoption *is* an exit on a claim about an index.
-/

/-- **A CLAIM DOES NOT EXIT QUARANTINE (design, proved).** Any number of `claimCaughtUp` messages at
    any index leaves the phase where it was: the claim is recorded and nothing else moves. -/
theorem claims_keep_the_phase (s : Node) (n : Nat) :
    (qrun .quarantine s (claims n)).phase = s.phase := by
  induction n generalizing s with
  | zero => rfl
  | succ k ih =>
    have h : claims (k + 1) = QEv.claimCaughtUp 0 :: claims k := by
      simp [claims, List.replicate_succ]
    rw [h]
    show (qrun .quarantine (qstep .quarantine s (QEv.claimCaughtUp 0)) (claims k)).phase = s.phase
    rw [ih (qstep .quarantine s (QEv.claimCaughtUp 0))]
    simp [qstep]

/-- **A CLAIM DOES NOT EXIT QUARANTINE.** The ledger's statement: from a quarantined state, any
    number of claims leaves the reason and the phase intact. -/
theorem a_claim_does_not_exit_quarantine (s : Node) (r : QReason)
    (hp : s.phase = .quarantined r) (n : Nat) :
    (qrun .quarantine s (claims n)).phase = .quarantined r := by
  rw [claims_keep_the_phase, hp]

/-- **A POLICY THAT TRUSTS THE MESSAGE EXITS WITHOUT THE STATE (DISPROVED).** The trace the design
    exists to rule out: the peer says "you are caught up at 10", no state arrives, and the node
    adopts index 10 with `synced` still at 0 and reopens its surfaces. It is the shape
    `recover_from_purge_point` takes (`state_machine.rs:713-724`): the purge point is treated as
    evidence of state. -/
theorem a_trusting_policy_exits_on_a_claim :
    let s : Node :=
      { Node.initial with
        phase := .quarantined (.snapshotUnreadable)
        up := true
        applied := 10
        synced := 0
        purgePoint := 10
        peerIndex := 10 }
    let out := qrun .trustClaims s [.request, .claimCaughtUp 10, .verify]
    out.phase = .healthy ∧ out.applied = 10 ∧ out.synced = 0 ∧ out.serving = true
      ∧ out.consistent = false := by
  decide

/-- **THE CODE'S EXIT IS AN INDEX, NOT A STATE (DISPROVED).** With the snapshot unreadable
    (`state_machine.rs:1358-1385` returns `Err`), `last_applied` stays `None`, the classifier answers
    `NeedsPurgePointBaseline` (`local_state.rs:93-96`), and `recover_from_purge_point` (`:713-724`)
    sets `last_applied` to the purge point *without touching `state`*. The node is then `Usable` by
    the classifier's own pair and serves with an empty `RaftState`: the ten entries below the purge
    point are never replayed, because replay starts at the index the node now claims. -/
theorem the_code_exits_on_the_purge_point_without_the_state :
    let s : Node :=
      { Node.initial with
        phase := .quarantined (.snapshotShort 0 10)
        up := true
        applied := 0
        synced := 0
        purgePoint := 10
        peerIndex := 10
        disk := 0
        restoreOk := false
        serving := true
        reportsReady := true }
    let out := qrun .serveAnyway s [.detect (.snapshotShort 0 10)]
    out.phase = .healthy ∧ out.applied = 10 ∧ out.synced = 0 ∧ out.consistent = false
      ∧ out.serving = true := by
  decide

/-! ## 6. OBLIGATION 3 — repair liveness

If a peer with sufficient state exists and the network eventually delivers, the node reaches healthy.
"Sufficient" is stated once, in `Node.need`: a peer must reach `max applied purgePoint`. The converse
matters as much: when no peer can supply it, the node stays quarantined **and says so**. It does not
exit, and it does not crash.
-/

/-- **REPAIR LIVENESS (design, proved).** From a quarantined node that is up, with a peer that can
    reach `need` and is not itself latched, `request → deliver → verify` reaches healthy with the
    state and the index agreeing, and every surface reopened. -/
theorem a_sufficient_peer_repairs_the_node :
    let s : Node :=
      { Node.initial with
        phase := .quarantined (.snapshotShort 0 10)
        up := true
        applied := 10
        synced := 0
        purgePoint := 10
        peerIndex := 12 }
    let out := qrun .quarantine s (repairTrace 12)
    out.phase = .healthy ∧ out.applied = 12 ∧ out.synced = 12 ∧ out.serving = true
      ∧ out.snapshots = true ∧ out.votes = true ∧ out.consistent = true := by
  decide

/-- A run of waits changes nothing. -/
theorem stalled_is_a_noop (pol : Policy) (s : Node) (n : Nat) :
    qrun pol s (stalled n) = s := by
  induction n generalizing s with
  | zero => rfl
  | succ k ih =>
    show qrun pol (qstep pol s .wait) (stalled k) = s
    rw [ih (qstep pol s .wait)]
    rfl

/-- **DELAY AND CLAIMS TOGETHER DO NOT EXIT IT EITHER.** The same statement with `n` waits interleaved
    before the claims: the scheduler's delay buys nothing and the peer's message buys nothing. -/
theorem a_claim_with_delay_does_not_exit_quarantine (s : Node) (r : QReason)
    (hp : s.phase = .quarantined r) (n : Nat) :
    (qrun .quarantine s (stalled n ++ claims n)).phase = .quarantined r := by
  rw [qrun_qrun, stalled_is_a_noop, claims_keep_the_phase, hp]

/-- **DELAY DOES NOT PREVENT IT (design, proved).** Any number of waits inserted before the delivery:
    the node still reaches healthy once the real content arrives. Duplication and reordering are
    absorbed by `deliver` being a `max`, so this is the scheduler's whole freedom over the delivery
    itself. -/
theorem delay_does_not_prevent_repair (n : Nat) :
    let s : Node :=
      { Node.initial with
        phase := .quarantined (.snapshotShort 0 10)
        up := true
        applied := 10
        synced := 0
        purgePoint := 10
        peerIndex := 12 }
    (qrun .quarantine s (stalled n ++ repairTrace 12)).phase = .healthy := by
  dsimp only
  rw [qrun_qrun, stalled_is_a_noop]
  decide

/-- **NO PEER, NO EXIT (design, proved).** One step of the design, from a state where nothing can
    supply the missing state: the node is still not healthy and the state is still one no peer can
    help. -/
theorem no_peer_step (s : Node) (h : s.NoPeer) (hp : s.phase ≠ .healthy) (e : QEv) :
    (qstep .quarantine s e).phase ≠ .healthy ∧ (qstep .quarantine s e).NoPeer := by
  obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8⟩ := h
  cases e <;> simp only [qstep] <;> (try (repeat' split)) <;>
    refine ⟨?_, ?_⟩ <;> simp_all [Node.NoPeer, Node.need, quarantineTo, healthyAt] <;> try omega

/-- **NO PEER KEEPS THE NODE QUARANTINED FOREVER, AND IT STAYS UP (design, proved).** No trace of
    events reaches healthy, and the process is still running at the end. The quarantine is explicit
    and permanent — one of the three acceptable discharges; silently serving is not, and neither is
    exiting. -/
theorem no_peer_keeps_the_node_quarantined_forever (s : Node) (h : s.NoPeer)
    (hp : s.phase ≠ .healthy) (tr : List QEv) :
    (qrun .quarantine s tr).phase ≠ .healthy ∧ (qrun .quarantine s tr).up = true := by
  induction tr generalizing s with
  | nil => exact ⟨hp, h.2.2.2.2.2.2.2⟩
  | cons e es ih =>
    obtain ⟨hph, hno⟩ := no_peer_step s h hp e
    exact ih (qstep .quarantine s e) hno hph

/-- **THE LATCH CLOSES THE ONLY REPAIR CHANNEL (DISPROVED).** A persistent local failure sets
    `apply_failure` (`state_machine.rs:435`, `:968`), and `install_snapshot` begins with
    `finish_pending_apply` (`:2804`), which returns the latched error for every call (`:527-529`).
    The node that most needs a snapshot is the one that cannot install it: the repair path is behind
    the state the repair would fix. -/
theorem a_latched_node_cannot_install_the_snapshot_that_would_fix_it :
    let s : Node :=
      { Node.initial with
        phase := .quarantined (.latchedAt 11)
        up := true
        applied := 10
        synced := 10
        peerIndex := 12
        disk := 10 }
    let out := qrun .serveAnyway s [.detect (.latchedAt 11), .request, .deliver 12, .verify]
    out.latched = true ∧ out.serving = false ∧ out.applied = 10 ∧ out.delivered = 0 := by
  decide

/-! ## 7. OBLIGATION 4 — no lost durable data

Repair must not silently discard locally durable rows that are newer than the peer's state. The code
replaces `self.state` wholesale (`:959`) and takes `last_applied` from the snapshot (`:960`); the
refusals it does have are about *dropping a table* (`:947-952`, `:997-1016`) and about a *persisted*
startup snapshot that cannot establish replacement order (`:1138-1146`), not about a table the
snapshot also carries. A row written locally at index 12 survives as an SSTable while the metadata
that describes it is replaced by the peer's state at 10, and nothing records that it exists.

The design's answer is not "merge by timestamp": `Ferrosa/SystemTables/Projection.lean:13-24` settles
that the projection has no per-cell generation, and `tsOnly_tie_fails_convergence` is what a merge
therefore inherits. The design's answer is the one the operator asked for — preserve, and quarantine
explicitly.
-/

/-- **A REPAIR KEEPS THE NEWER ROWS (design, proved).** The rows above the peer's index are carried
    into `pendingRows` by the same step that adopts the state, so they are recorded rather than
    dropped: the node leaves quarantine with an explicit debt, not with a silent loss. -/
theorem a_repair_preserves_the_rows_above_the_peer :
    let s : Node :=
      { Node.initial with
        phase := .quarantined (.snapshotShort 0 10)
        up := true
        applied := 10
        synced := 0
        purgePoint := 10
        peerIndex := 10
        newerRows := 2 }
    let out := qrun .quarantine s (repairTrace 10)
    out.phase = .healthy ∧ out.pendingRows = 2 ∧ out.consistent = true := by
  decide

/-- **THE CODE DROPS THEM AND RECORDS NOTHING (DISPROVED).** The same repair on the code's policy: the
    state at 10 is adopted, the local rows above it are not carried anywhere, and `pendingRows = 0`
    is the model of `self.state = data.state` leaving no trace of what it replaced. -/
theorem a_repair_that_replaces_state_drops_the_newer_rows :
    let s : Node :=
      { Node.initial with
        phase := .quarantined (.projectionBehind 12 0)
        up := true
        applied := 12
        synced := 0
        purgePoint := 10
        peerIndex := 10
        newerRows := 2
        serving := false }
    let out := qrun .serveAnyway s [.detect (.projectionBehind 12 0), .deliver 10, .verify]
    out.applied = 10 ∧ out.pendingRows = 0 ∧ out.newerRows = 2 := by
  decide

/-- **A REPAIR MERGE THAT TRUSTS TIMESTAMPS INHERITS THE PROJECTION'S TIE DEFECT.** The two writes a
    repair merge would compare for one cell — the peer's, and the local newer one — carry the same
    microsecond, and the projection model's own counterexample is exactly that pair: the reload keeps
    the older payload. So "just merge the rows" is not a discharge; the projection needs a per-cell
    generation first (`Ferrosa/SystemTables/Projection.lean:238-242`,
    `tsOnly_tie_fails_convergence`). -/
theorem a_repair_merge_by_timestamp_keeps_the_older_cell :
    Ferrosa.SystemTables.reload Ferrosa.SystemTables.Policy.timestampOnly
      Ferrosa.SystemTables.Visibility.absorbing Ferrosa.SystemTables.tieLog = some [2, 0] :=
  Ferrosa.SystemTables.tsOnly_tie_breaks_toward_the_older_write

/-! ## 8. OBLIGATION 5 — online membership

A quarantined node stays a cluster member — it must, or it cannot be repaired — but it must not be
able to corrupt anything through membership: its own vote cannot help elect a leader that would then
serve its state, and its presence must not let a quorum form on bad state.

The distinction the model keeps is between *being a member* (`up`, receiving replication and
deliveries) and *being counted* (`votes`, `counted`). The code has only the first: `RaftVoteHandler`
(`raft/handlers.rs:1014-1019`) forwards to `raft.vote(req)` with no state-machine term at all.
-/

/-- **A QUARANTINED NODE'S VOTE IS NOT COUNTED (design, proved).** One step from a node in quarantine:
    the vote and the quorum count stay shut. `vote` opens them only in the `.healthy` phase, which is
    why this is an implication on `inQ` rather than a plain invariant. -/
theorem uncounted_step (s : Node) (h : s.inQ = true → s.uncounted = true) (e : QEv) :
    (qstep .quarantine s e).inQ = true → (qstep .quarantine s e).uncounted = true := by
  intro hq
  cases e <;> simp only [qstep] at hq ⊢ <;> (try (repeat' split)) <;>
    simp_all [Node.uncounted, Node.inQ, quarantineTo, healthyAt]

/-- **A QUARANTINED NODE IS A MEMBER THAT IS NOT COUNTED (design, proved).** From any quarantined
    state, no trace makes it vote or be counted. This is what lets a quarantined node be repaired
    without being removed from the cluster, and it is the half the code is missing:
    `RaftVoteHandler` (`raft/handlers.rs:1014-1019`) has no such term. -/
theorem a_quarantined_node_stays_a_member_and_is_not_counted (s : Node)
    (h : s.inQ = true → s.uncounted = true) (tr : List QEv) :
    (qrun .quarantine s tr).inQ = true → (qrun .quarantine s tr).uncounted = true := by
  induction tr generalizing s with
  | nil => exact h
  | cons e es ih =>
    intro hq
    exact ih (qstep .quarantine s e) (uncounted_step s h e) hq

/-- **A DIVERGENT NODE STILL VOTES (DISPROVED).** The projection diverged at boot and nothing closes
    the vote path: a vote request arriving afterwards is answered and counted, so the node can be part
    of the quorum that elects a leader while holding a state machine it cannot honour. -/
theorem a_divergent_node_still_votes :
    let s : Node := { Node.initial with disk := 4, restoreOk := false, up := true, peerIndex := 4 }
    let out := qrun .serveAnyway s [.restart, .vote]
    out.synced = 0 ∧ out.applied = 4 ∧ out.consistent = false ∧ out.votes = true
      ∧ out.counted = true := by
  decide

/-! ## 9. OBLIGATION 6 — progress

`Startup.lean`'s `restart_cycle_never_passes_a_poisoned_entry` is the wedge: a persistent local
failure re-fails the same entry on every boot, so `applied` never moves. Under the design the node
does not need to pass the entry locally at all — it adopts a state that already covers it — and a
restart mid-repair re-enters quarantine rather than reporting healthy on a partial repair.

There is a gap between design and code that the model makes explicit rather than papering over:
`delivered` is a field, and nothing in the code persists one. `SnapshotData` (`:390-400`) carries
`state`, `last_applied` and `last_membership`. `a_restart_mid_repair_resumes` is therefore a
statement about the design *with* a durable cursor, and `a_restart_forgets_the_repair_position`
records that the code has none.
-/

/-- **A RESTART MID-REPAIR RE-ENTERS QUARANTINE AND RESUMES (design, proved).** The process dies
    after the inconsistent boot and before any state arrives; the new process reads the same durable
    facts, quarantines for the same reason, and the repair it then runs still reaches healthy. It is
    never healthy on the strength of the partial attempt. -/
theorem a_restart_mid_repair_resumes :
    let s : Node :=
      { Node.initial with
        disk := 4
        restoreOk := false
        up := true
        purgePoint := 4
        peerIndex := 6 }
    let booted := qrun .quarantine s [.restart]
    let afterDeath := qrun .quarantine booted [.restart]
    afterDeath.phase ≠ .healthy
      ∧ (qrun .quarantine afterDeath (repairTrace 6)).phase = .healthy
      ∧ (qrun .quarantine afterDeath (repairTrace 6)).applied = 6 := by
  decide

/-- **THE WEDGE IS DISPROVED UNDER THE DESIGN.** The entry that keeps failing its local projection is
    at 5, `applied` is 4, and the precondition never clears — `Startup.lean`'s exact shape. A
    restart-and-repair cycle reaches a state that covers the entry, because the repair adopts the
    peer's index rather than re-running the entry; three cycles end in the same place, so the node is
    not oscillating, it is repaired. -/
theorem three_repair_cycles_pass_the_poisoned_entry :
    let s : Node :=
      { Node.initial with
        phase := .quarantined (.latchedAt 5)
        up := true
        applied := 4
        synced := 4
        purgePoint := 0
        peerIndex := 6
        disk := 4 }
    let cycle : List QEv := [.restart, .detect (.latchedAt 5), .request, .deliver 6, .verify]
    let one := qrun .quarantine s cycle
    let three := qrun .quarantine s (cycle ++ cycle ++ cycle)
    one.phase = .healthy ∧ one.applied = 6 ∧ three.phase = .healthy ∧ three.applied = 6
      ∧ three.synced = 6 := by
  decide

/-- **THE CODE STILL WEDGES (DISPROVED).** The same state on the code's policy: the restart clears the
    latch (`apply_failure` is not in `SnapshotData`, `state_machine.rs:670`), the failure re-latches,
    and the snapshot cannot be installed because `install_snapshot` is behind the latch. Three cycles
    end where one did, at `applied = 4`. -/
theorem the_code_never_passes_the_poisoned_entry :
    let s : Node :=
      { Node.initial with
        phase := .quarantined (.latchedAt 5)
        up := true
        applied := 4
        synced := 4
        purgePoint := 0
        peerIndex := 6
        disk := 4 }
    let cycle : List QEv := [.restart, .detect (.latchedAt 5), .request, .deliver 6, .verify]
    let one := qrun .serveAnyway s cycle
    let three := qrun .serveAnyway s (cycle ++ cycle ++ cycle)
    one.applied = 4 ∧ three.applied = 4 ∧ three.latched = true ∧ three.serving = false := by
  decide

/-- **THE REPAIR POSITION IS NOT DURABLE (DISPROVED, and the gap the design cannot close by itself).**
    A restart zeroes `delivered` and `claim`, because neither is in `SnapshotData`
    (`state_machine.rs:390-400`) and the constructor starts from `None` (`:670`). The design can
    re-derive the position from the peer, which is why the liveness theorems above hold, but until a
    repair marker exists on disk a node that restarts while a large snapshot is in flight restarts
    the transfer from the beginning. -/
theorem a_restart_forgets_the_repair_position :
    let s : Node :=
      { Node.initial with
        phase := .repairing (.peerSnapshot 6)
        up := true
        applied := 4
        synced := 4
        delivered := 6
        disk := 4
        restoreOk := true }
    (qrun .quarantine s [.restart]).delivered = 0 := by
  decide

/-! ## 10. The third branch of the old trichotomy is not a discharge

`Startup.lean`'s ledger said a startup failure "serves, refuses naming the cause, or names a resync
path", and treated the refusal branch as acceptable. The operator has ruled that out. The model's
`refused` field stands for the branches that return without starting openraft
(`controller/cluster.rs:1439-1445`, `:1588-1594`); this theorem shows that from a refused boot nothing
in the event alphabet brings the node up, because the durable facts it classifies on never change.
-/

/-- **THE REFUSAL PATH IS TERMINAL (DISPROVED as a discharge).** Three boots and two restarts from a
    stranded state: `up` is false at every point, because `bootStep` re-classifies the same durable
    facts (`disk = 9`, `purged = 10`, no hole) to `stranded` each time. `refusal_is_not_undone_by_replay`
    in `Startup.lean` covers the small-trace class; this is the trace in the field a caller reads. -/
theorem the_refusal_path_is_terminal :
    (srun { Boot.initial with disk := 9, purged := 10 }
        [.boot, .restart, .boot, .restart, .boot]).up = false
    ∧ (srun { Boot.initial with disk := 9, purged := 10 }
        [.boot, .restart, .boot, .restart, .boot]).refused = true := by
  decide

/-! ## The ledger

Six obligations, each settled twice: once as a property of the design (`met`, proved above) and once
as the gap between that design and the code (`failing`, with the trace that shows the code doing the
other thing). The design rows are named `design: …` and the code rows `code: …`, so the distinction
is in the ledger and not only in this comment.

`Ferrosa/Raft/Obligations.lean` appends this list to the area ledger, and `Ferrosa/Unimplemented.lean`
requires every red here to be classified there, so a code gap cannot be dropped without the
bookkeeping breaking.
-/

def quarantineItems : List SpecItem := [
  SpecItem.mk "design: a quarantined node answers no read, no readiness probe and no snapshot request" (Status.met "Raft.no_answer_from_inconsistent_state"),
  SpecItem.mk "code: a node whose restore failed answers reads and reports ready on state it does not have" (Status.failing
     "apply_snapshot_data_from adopts last_applied from the snapshot (ferrosa-cluster/src/raft/state_machine.rs:960) and a failing sync_schema_and_engine_from_state rolls back the state but not the boot (ferrosa-cluster/src/controller/cluster.rs:1467-1472 logs and boots), so the node runs with last_applied=4 and a RaftState the schema and engine never saw. ModeController::is_cql_ready (ferrosa-cluster/src/controller/mod.rs:994-999) checks consensus health, write path, DDL path and declared topology, and /readyz (ferrosa/src/web/readiness.rs:191-372) checks supervised tasks, listeners, set-aside mutations and Raft liveness: neither reads the difference between synced and applied. There is no quarantine state anywhere in the process - every quarantin* in the tree is SSTable- or range-level (ferrosa-cluster/src/repair/trigger.rs:1-20). The snapshot builder is worse than passive: build_snapshot (ferrosa-cluster/src/raft/state_machine.rs:2528-2583) publishes the unhonoured state at last_applied and mark_durably_applied (:2576) then lets the log purge to it, so the node becomes consistent by fiat and the entries that could have repaired it are gone"
     "Raft.the_code_answers_from_state_it_does_not_have"),
  SpecItem.mk "design: quarantine is left only after the state is verified, never on a claim" (Status.met "Raft.a_claim_does_not_exit_quarantine"),
  SpecItem.mk "code: the node exits an inconsistent state on an index it was told, not on state it holds" (Status.failing
     "the classifier compares two numbers, last_applied and last_purged (ferrosa-cluster/src/raft/local_state.rs:88-102), and recover_from_purge_point (ferrosa-cluster/src/raft/state_machine.rs:713-724) sets last_applied to the purge point without touching state, so the node reports the whole log applied while holding an empty RaftState and never replays the entries below it. That is an exit on a claim about an index: nothing in the boot path compares RaftState against the entries it says it applied. Raft.a_trusting_policy_exits_on_a_claim is the same trace with the claim made in words, which is the shape a repair driven by a peer saying 'you are caught up' would take"
     "Raft.the_code_exits_on_the_purge_point_without_the_state"),
  SpecItem.mk "design: a peer with sufficient state repairs a quarantined node while it runs" (Status.met "Raft.a_sufficient_peer_repairs_the_node"),
  SpecItem.mk "code: a latched node cannot install the snapshot that would repair it, and never becomes healthy again" (Status.failing
     "install_snapshot (ferrosa-cluster/src/raft/state_machine.rs:2801-2837) begins with finish_pending_apply (:2804), which returns the apply_failure error for every call while the latch is set (:527-529), so the repair path sits behind the state it would repair; and ConsensusHealth (ferrosa-cluster/src/consensus_health.rs:97-125) has fail/is_healthy/failure/failure_count and no reset, so a failure is process-lifetime and only a restart clears it - the restart that re-derives the same failure from the same durable facts. A node with no reachable peer has nowhere to say so either: nothing between /readyz and the operator distinguishes 'waiting for a peer' from 'failed', so the third acceptable discharge (quarantine forever, with the reason) is not expressible today"
     "Raft.a_latched_node_cannot_install_the_snapshot_that_would_fix_it"),
  SpecItem.mk "design: a repair preserves the locally durable rows that are newer than the peer's state" (Status.met "Raft.a_repair_preserves_the_rows_above_the_peer"),
  SpecItem.mk "code: a repair replaces local state wholesale and records nothing it discarded" (Status.failing
     "apply_snapshot_data_from assigns self.state = data.state (ferrosa-cluster/src/raft/state_machine.rs:959) and last_applied = meta.last_log_id (:960); the guards it does have refuse a snapshot that drops a table carrying local artifacts (:947-952, :997-1016) and, since 45a6af1d, a persisted startup snapshot that cannot prove replacement order (:1138-1146) - none of them merges, or records a row for, a table the snapshot also carries. A locally durable row above the peer's index survives as an SSTable while the metadata describing it is replaced. A repair merge by timestamp is not a way out: the projection has no per-cell generation (specs/lean4/Ferrosa/SystemTables/Projection.lean:13-24) and Raft.a_repair_merge_by_timestamp_keeps_the_older_cell is that model's tsOnly_tie_breaks_toward_the_older_write applied to the pair a repair would compare"
     "Raft.a_repair_that_replaces_state_drops_the_newer_rows"),
  SpecItem.mk "design: a quarantined node stays a member and is counted in no quorum" (Status.met "Raft.a_quarantined_node_stays_a_member_and_is_not_counted"),
  SpecItem.mk "code: a node whose projection diverged still votes and is counted toward quorum" (Status.failing
     "RaftVoteHandler (ferrosa-cluster/src/raft/handlers.rs:1014-1019) forwards a VoteRequest straight to raft.vote(req) with no state-machine or health term, so a node that booted with last_applied ahead of its RaftState (ferrosa-cluster/src/raft/state_machine.rs:960, ferrosa-cluster/src/controller/cluster.rs:1467-1472) answers votes and is counted - it can be part of the quorum that elects a leader while holding a state machine it cannot honour. Only the openraft-fatal path closes anything, and it closes the node rather than the vote"
     "Raft.a_divergent_node_still_votes"),
  SpecItem.mk "design: a restart during repair re-enters quarantine and the node still makes progress" (Status.met "Raft.three_repair_cycles_pass_the_poisoned_entry"),
  SpecItem.mk "code: a restart replays the same failure and the repair position is not durable" (Status.failing
     "apply_failure is a field on the in-memory machine (ferrosa-cluster/src/raft/state_machine.rs:435), absent from SnapshotData (:390-400) and set to None by the constructor (:670), so a restart clears it and the next boot re-derives the same failure from the same durable facts (Raft.restart_cycle_never_passes_a_poisoned_entry); no durable repair cursor exists either, so a restart during a snapshot transfer starts it again (Raft.a_restart_forgets_the_repair_position). The reset path that does make progress (ferrosa-cluster/src/controller/cluster.rs:1531-1552) does it by moving the raft directory aside and taking a fresh snapshot, which discards the log rather than repairing from it, and when the reset itself fails the node returns without raft (ferrosa-cluster/src/controller/cluster.rs:1588-1594, 'needs operator repair') - the refusal the operator has ruled out as a discharge (Raft.the_refusal_path_is_terminal)"
     "Raft.the_code_never_passes_the_poisoned_entry")
]

/-- Twelve obligations, six of them design properties that hold and six of them gaps in the code. -/
theorem quarantine_items_pinned : quarantineItems.length = 12 ∧ redCount quarantineItems = 6 := by
  decide

/-! ## The axiom audit

The trace-level theorems are finite computations over `decide` and report no axioms; the inductions
report `propext` and `Quot.sound`, and two of them also `Classical.choice`, which `simp_all` pulls in
when it discharges a `≠` on an inductive. None reports `sorryAx`.
-/

#print axioms no_answer_from_inconsistent_state
#print axioms a_quarantined_node_keeps_every_surface_shut
#print axioms the_code_answers_from_state_it_does_not_have
#print axioms a_quarantined_node_cannot_be_snapshotted_into_consistency
#print axioms the_snapshot_builder_makes_a_node_consistent_by_fiat
#print axioms a_claim_does_not_exit_quarantine
#print axioms a_claim_with_delay_does_not_exit_quarantine
#print axioms a_trusting_policy_exits_on_a_claim
#print axioms the_code_exits_on_the_purge_point_without_the_state
#print axioms a_sufficient_peer_repairs_the_node
#print axioms delay_does_not_prevent_repair
#print axioms no_peer_keeps_the_node_quarantined_forever
#print axioms a_latched_node_cannot_install_the_snapshot_that_would_fix_it
#print axioms a_repair_preserves_the_rows_above_the_peer
#print axioms a_repair_that_replaces_state_drops_the_newer_rows
#print axioms a_repair_merge_by_timestamp_keeps_the_older_cell
#print axioms a_quarantined_node_stays_a_member_and_is_not_counted
#print axioms a_divergent_node_still_votes
#print axioms a_restart_mid_repair_resumes
#print axioms three_repair_cycles_pass_the_poisoned_entry
#print axioms the_code_never_passes_the_poisoned_entry
#print axioms a_restart_forgets_the_repair_position
#print axioms the_refusal_path_is_terminal

end Ferrosa.Raft
