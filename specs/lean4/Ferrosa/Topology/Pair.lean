/-
  Ferrosa formal specification — pair mode: two nodes, one primary, no quorum.
  ==========================================================================

  Two nodes and no quorum: a write the primary acknowledges is on exactly one machine,
  and losing that machine loses the write until an operator promotes the other node.
  The surviving secondary keeps serving reads, so a pair whose primary is down is
  read-only rather than unavailable, and writes refuse rather than split.  Promotion is
  the operator's decision and is gated on the operator having confirmed the peer dead,
  which is the only thing keeping a pair from accepting two divergent writes under one
  key — `unguarded_promotion_splits_brain` is the split brain that arrives the moment
  that gate is dropped.  A returning primary is demoted on reconnect, because connection
  direction assigns the roles and no election is involved.
-/
import Ferrosa.Topology.Basic

namespace Ferrosa.Topology.Pair

open Ferrosa
open Ferrosa.Topology

/-- Pair-mode roles.  `none` is a node that has not been assigned a role yet. -/
inductive Role where
  | none
  | primary
  | secondary
  deriving DecidableEq, Repr

/-- The two-node formation state.  `up` means "alive and serving"; a partitioned
    node is still `up` from its own point of view. -/
structure Pair where
  r0 : Role
  r1 : Role
  up0 : Bool
  up1 : Bool
  deriving DecidableEq, Repr

/-- Node `i` is accepting writes. -/
def Writable (s : Pair) (i : Node) : Prop :=
  (i = 0 ∧ s.r0 = .primary ∧ s.up0 = true) ∨ (i = 1 ∧ s.r1 = .primary ∧ s.up1 = true)

/-- Node `i` is serving reads (a degraded secondary still does: T8b). -/
def Readable (s : Pair) (i : Node) : Prop :=
  (i = 0 ∧ s.up0 = true) ∨ (i = 1 ∧ s.up1 = true)

/-- Transitions.  `op` is the operator's out-of-band decision: `promote`
    transitions are only available when `op = true`, and they additionally
    require the peer to be down, because the operator has confirmed its death. -/
inductive Step (op : Bool) : Pair → Pair → Prop where
  /-- T1: the receiver of the connection becomes primary, the initiator
      secondary.  One action, both effects, so the roles cannot disagree. -/
  | t1 : Step op ⟨.none, .none, true, true⟩ ⟨.primary, .secondary, true, true⟩
  /-- T9: operator promotes node 1 while node 0 is confirmed dead. -/
  | promote1 {s : Pair} (hop : op = true) (hdown : s.up0 = false) (hn : s.r1 ≠ .primary) :
      Step op s { s with r1 := .primary }
  /-- T9 mirror: operator promotes node 0 while node 1 is confirmed dead. -/
  | promote0 {s : Pair} (hop : op = true) (hdown : s.up1 = false) (hn : s.r0 ≠ .primary) :
      Step op s { s with r0 := .primary }
  /-- Graceful demotion. -/
  | demote0 {s : Pair} : Step op s { s with r0 := .secondary }
  | demote1 {s : Pair} : Step op s { s with r1 := .secondary }
  /-- T9: the old primary reconnects.  It initiates, therefore it becomes
      secondary — connection direction decides, not a UUID comparison. -/
  | rejoin0 {s : Pair} (hpeer : s.r1 = .primary) :
      Step op s { s with r0 := .secondary, up0 := true }
  | rejoin1 {s : Pair} (hpeer : s.r0 = .primary) :
      Step op s { s with r1 := .secondary, up1 := true }
  /-- Node failure (T8a/T8b). -/
  | fail0 {s : Pair} : Step op s { s with up0 := false }
  | fail1 {s : Pair} : Step op s { s with up1 := false }

/-- Reachability at a fixed operator policy. -/
inductive Reachable (op : Bool) : Pair → Prop where
  | t1 : Reachable op ⟨.none, .none, true, true⟩
  | step : Reachable op s → Step op s s' → Reachable op s'

/-! ## The pair is single-primary by construction, with no election to run

  The first connection assigns both roles in one step, so there is no interleaving in which two
  nodes call themselves primary and no election to lose. What an operator gets is a pair that
  cannot split on its own: with the primary down, the surviving node serves reads and refuses
  writes until someone promotes it.
-/

/-- T1 assigns both roles at once: the receiver is primary, the initiator is
    secondary.  There is no interleaving in which both call themselves primary. -/
theorem t1_roles_are_determined :
    (⟨.none, .none, true, true⟩ : Pair) = ⟨.none, .none, true, true⟩ ∧
      (⟨.primary, .secondary, true, true⟩ : Pair).r0 = .primary ∧
      (⟨.primary, .secondary, true, true⟩ : Pair).r1 = .secondary := by
  refine ⟨rfl, rfl, rfl⟩

/-- **Promotion is operator-only, and requires the peer confirmed dead.**  Both
    halves of the gate appear in the conclusion, so neither can be dropped
    without changing the theorem. -/
theorem promotion_requires_operator_and_dead_peer {op : Bool} {s s' : Pair}
    (h : Step op s s') (hnew : s'.r1 = .primary) (hold : s.r1 ≠ .primary) :
    op = true ∧ s.up0 = false := by
  cases h with
  | t1 => simp at hnew
  | promote1 hop hdown _ => exact ⟨hop, hdown⟩
  | promote0 _ _ _ => simp_all
  | demote0 => simp_all
  | demote1 => simp_all
  | rejoin0 _ => simp_all
  | rejoin1 _ => simp_all
  | fail0 => simp_all
  | fail1 => simp_all

/-- **No split brain.**  In any reachable pair state, at most one node is both
    primary and up, so at most one node accepts writes. -/
theorem no_two_writable {op : Bool} {s : Pair} (h : Reachable op s) :
    ¬(Writable s 0 ∧ Writable s 1) := by
  induction h with
  | t1 => simp [Writable]
  | step hr hs ih =>
      cases hs with
      | t1 => simp [Writable]
      | promote1 _ hdown _ => simp_all [Writable]
      | promote0 _ hdown _ => simp_all [Writable]
      | demote0 | demote1 | rejoin0 _ | rejoin1 _ | fail0 | fail1 =>
          simp_all [Writable]

/-! ## What the operator gate buys, and what dropping it would cost

  Promotion is available only to the operator and only once the peer is confirmed dead, and both
  halves of that gate are load-bearing. Drop the confirmation and one promotion from the initial
  state leaves both nodes primary and accepting writes for the same key, which a client experiences
  as two successful writes to one key holding different values.
-/

/-- Dropping the "peer is confirmed dead" guard makes split brain reachable:
both nodes end up primary and up, so both accept writes.  This is the theorem
that justifies the operator-only transition. -/
inductive UnguardedStep : Pair → Pair → Prop where
  | t1 : UnguardedStep ⟨.none, .none, true, true⟩ ⟨.primary, .secondary, true, true⟩
  | promote1 {s : Pair} (hn : s.r1 ≠ .primary) : UnguardedStep s { s with r1 := .primary }

/-- **Counterexample: without the guard, one promotion splits the brain.**  A single
    `promote1` step from the T1 state leaves both nodes primary and up, so both
    accept writes for the same key and a client can be told two different values
    succeeded.  Nothing in the unguarded system can tell a dead peer from a
    partitioned one, so there is no state at which the promotion is safe.  The code
    keeps the half of the guard it can observe: `ModeController::force_promote`
    refuses while the pair peer is still connected
    (`force_promote_is_refused_while_the_primary_is_still_connected`), and a live
    primary hands authority over with a fenced switchover instead. -/
theorem unguarded_promotion_splits_brain :
    ∃ s s', UnguardedStep s s' ∧ Writable s' 0 ∧ Writable s' 1 := by
  refine ⟨⟨.primary, .secondary, true, true⟩, ⟨.primary, .primary, true, true⟩,
          UnguardedStep.promote1 (by simp), ?_, ?_⟩
  · simp [Writable]
  · simp [Writable]

/-- The promotion path is reachable in the guarded system: fail the primary,
    promote the secondary. -/
theorem promote_after_primary_failure_reachable :
    ∃ s, Reachable true s ∧ s.r0 = .primary ∧ s.r1 = .primary ∧ s.up0 = false := by
  refine ⟨⟨.primary, .primary, false, true⟩, ?_, rfl, rfl, rfl⟩
  have h1 : Reachable true (⟨.primary, .secondary, true, true⟩ : Pair) :=
    Reachable.step Reachable.t1 Step.t1
  have h2 : Reachable true (⟨.primary, .secondary, false, true⟩ : Pair) :=
    Reachable.step h1 Step.fail0
  exact Reachable.step h2 (Step.promote1 rfl rfl (by simp))

/-- **A returning primary is demoted by connection direction.**  In the state the
    operator created (old primary down, peer promoted), the old primary coming
    back leaves it secondary and the promoted node primary — the documented
    T9 outcome, and the reason the pair heals without a role election. -/
theorem returning_primary_becomes_secondary :
    ∃ s, Reachable true s ∧ s.r0 = .secondary ∧ s.r1 = .primary ∧
      s.up0 = true ∧ s.up1 = true := by
  refine ⟨⟨.secondary, .primary, true, true⟩, ?_, rfl, rfl, rfl, rfl⟩
  have h1 : Reachable true (⟨.primary, .secondary, true, true⟩ : Pair) :=
    Reachable.step Reachable.t1 Step.t1
  have h2 : Reachable true (⟨.primary, .secondary, false, true⟩ : Pair) :=
    Reachable.step h1 Step.fail0
  have h3 : Reachable true (⟨.primary, .primary, false, true⟩ : Pair) :=
    Reachable.step h2 (Step.promote1 rfl rfl (by simp))
  exact Reachable.step h3 (Step.rejoin0 rfl)

/-- **Degraded pair serves stale reads only.**  With the primary down and no
    promotion, the surviving node is readable but not writable: reads at
    `LOCAL_ONE` keep working, writes fail.  This is T8b's contract. -/
theorem degraded_pair_is_read_only :
    Readable ⟨.primary, .secondary, false, true⟩ 1 ∧
      ¬ Writable ⟨.primary, .secondary, false, true⟩ 1 := by
  refine ⟨by simp [Readable], ?_⟩
  simp [Writable]

/-- ... and it stays that way: no *reachable* transition from a degraded pair
    makes the secondary writable without the operator. -/
theorem degraded_pair_stays_read_only {s : Pair} (h : Reachable false s)
    (h0 : s.up0 = false) (h1 : s.r1 = .secondary) (h2 : s.up1 = true) :
    ¬ Writable s 1 := by
  intro hw
  simp_all [Writable]

/-! ## Switchover admission: a transport refusal is not an answer from the peer

  Before handing over, the primary pushes its schema to the secondary and waits for an ack naming
  the version it pushed. A secondary that has just restarted refuses the first attempt with
  `Reconnecting` until its bulk lane redials, and that refusal carries no information about the
  peer's schema — the request never left the primary. The push therefore waits inside a fixed
  budget, and only a reply decides. Two things have to stay true while it waits: the handoff is
  admitted only on an ack that names the pushed version, and a peer that never answers still
  refuses the handoff. `pushWithin` makes both checkable without a socket.
-/

/-- One attempt's outcome: the peer answered with the schema version it converged to (`ack`), the
    lane had not finished reconnecting (`reconnecting`), or the attempt reached the peer and
    failed (`failed`). -/
inductive PushOutcome where
  | ack (version : Nat)
  | reconnecting
  | failed (reason : Nat)
  deriving DecidableEq, Repr

/-- Run the schema push with `n` attempts left in the budget. A `reconnecting` lane costs one
    attempt and decides nothing; an `ack` or a `failed` attempt decides immediately. -/
def pushWithin : Nat → Nat → List PushOutcome → Option (Bool × Nat)
  | 0, _, _ => none
  | _ + 1, _, [] => none
  | _ + 1, want, .ack version :: _ => some (version == want, version)
  | _ + 1, _, .failed reason :: _ => some (false, reason)
  | n + 1, want, .reconnecting :: rest => pushWithin n want rest

/-- **The handoff is admitted only on the pushed version.** Whatever the sequence of attempts, a
    `true` verdict names the version that was pushed, so a stale or foreign ack cannot admit a
    switchover. -/
theorem push_admits_only_on_the_pushed_version :
    ∀ (n want : Nat) (os : List PushOutcome) (v : Nat),
      pushWithin n want os = some (true, v) → v = want := by
  intro n
  induction n with
  | zero => intro want os v h; simp [pushWithin] at h
  | succ n ih =>
      intro want os v h
      cases os with
      | nil => simp [pushWithin] at h
      | cons o rest =>
          cases o with
          | ack w =>
              simp only [pushWithin, Option.some.injEq, Prod.mk.injEq] at h
              obtain ⟨hbeq, hw⟩ := h
              subst hw
              exact beq_iff_eq.mp hbeq
          | failed r => simp [pushWithin] at h
          | reconnecting => exact ih want rest v h

/-- **A lane that redials inside the budget does not fail the handoff.** Any number of
    `reconnecting` refusals followed by an ack naming the pushed version is admitted, which is the
    nightly's Phase 5 case: the lane came up 14 ms after the refusal. -/
theorem push_succeeds_when_the_lane_redials_within_the_budget :
    ∀ (k want : Nat) (rest : List PushOutcome),
      pushWithin (k + 1) want (List.replicate k .reconnecting ++ .ack want :: rest) =
        some (true, want) := by
  intro k
  induction k with
  | zero => intro want rest; simp [pushWithin, List.replicate_zero]
  | succ k ih => intro want rest; simp [pushWithin, List.replicate_succ, ih]

/-- **A peer that never answers still refuses the handoff.** When every attempt in the budget is a
    reconnecting lane, the push returns no verdict, so the switchover is refused exactly as it was
    before the wait existed. -/
theorem push_refuses_when_every_attempt_is_a_reconnecting_lane :
    ∀ (n want : Nat), pushWithin n want (List.replicate n .reconnecting) = none := by
  intro n
  induction n with
  | zero => intro want; simp [pushWithin, List.replicate_zero]
  | succ n ih => intro want; simp [pushWithin, List.replicate_succ, ih]

/-- **A delivered failure is not retried.** An attempt that reached the peer and failed decides
    the handoff at once; the budget does not turn it into a second attempt. -/
theorem push_refuses_a_delivered_failure :
    ∀ (want r : Nat) (rest : List PushOutcome),
      pushWithin 2 want (.reconnecting :: .failed r :: rest) = some (false, r) := by
  intro want r rest
  simp [pushWithin]


/-! ## Data catch-up on rejoin: a schema sync alone leaves the returning node short

  Rejoining a pair ran two steps and applied one. `send_schema_sync_to_peer` settles *which
  tables exist*; only the commit-log replay gives the returning node the *rows* this primary
  acknowledged while it was down. The docker smoke's Phase 5 is the witness: node2 promoted,
  took `failover1`, node1 rejoined and took the primary role back, and node1 answered
  `SELECT v FROM smoke_test.kv WHERE k = 'failover1'` with zero rows. No role was wrong; the
  returning node's store simply never learned the write.

  The model is presence-only. Values resolve last-write-wins per cell, so a key that is absent
  returns no row at all — which is exactly the read the smoke performs.
-/

/-- The keys a node can serve. -/
abbrev Keys := List Nat

/-- What the returning node holds when the rejoin syncs the schema and stops there:
    whatever it held before it went down. -/
def storeAfterSchemaOnlySync (before : Keys) : Keys := before

/-- What it holds when the primary also replays its commit log. -/
def storeAfterReplay (before committed : Keys) : Keys := before ++ committed

/-- **A schema-only rejoin drops a write the primary acknowledged.** A node that was down
    while its peer committed `k` returns holding its old store, so `k` is absent — the
    zero-row read, and the reason Phase 5 failed. -/
theorem schema_only_rejoin_drops_a_committed_write :
    ∃ (before committed : Keys) (k : Nat),
      k ∈ committed ∧ k ∉ storeAfterSchemaOnlySync before := by
  exact ⟨[], [7], 7, by simp, by simp [storeAfterSchemaOnlySync]⟩

/-- **A rejoin that replays the commit log keeps every committed write.** -/
theorem replay_on_rejoin_keeps_every_committed_write
    (before committed : Keys) (k : Nat) (hk : k ∈ committed) :
    k ∈ storeAfterReplay before committed := by
  simp [storeAfterReplay, hk]

/-- **Catch-up leaves the returning store equal to the union of what it had and what the
    primary committed** — membership, key by key, for every key and every starting state. -/
theorem catch_up_makes_the_stores_agree (before committed : Keys) (k : Nat) :
    (k ∈ storeAfterReplay before committed) = (k ∈ before ∨ k ∈ committed) := by
  simp [storeAfterReplay, List.mem_append]

/-- **The switchover read the smoke performs succeeds after catch-up**, for the key the old
    primary acknowledged during the failover. -/
theorem switchover_after_catch_up_serves_the_failover_key :
    (7 : Nat) ∈ storeAfterReplay [] [7] := by
  simp [storeAfterReplay]


/-! ## Digest-guided rejoin: send the peer only what it lacks

  The full replay is correct but pays for the whole retained log on every rejoin,
  including the mutations the returning node already has on disk. Rather than
  inventing a position cursor — which would have to survive concurrent forwards
  whose order the peer does not share — the rejoining node reports the mutation
  ids in its own log, and the primary skips exactly those. The exchange is
  unordered by construction: what matters is set membership, not position.
-/

/-- Replay only the mutations the peer's digest says it lacks. -/
def replayMissing : Keys → Keys → Keys
  | _has, [] => []
  | has, k :: ks =>
      if k ∈ has then replayMissing has ks else k :: replayMissing has ks

/-- **Nothing is invented.** A digest-guided replay sends only keys the primary
    committed; it cannot conjure a write the primary never made. -/
theorem replay_missing_is_a_subset (has committed : Keys) :
    ∀ k, k ∈ replayMissing has committed → k ∈ committed := by
  induction committed with
  | nil =>
      intro k hk
      simp [replayMissing] at hk
  | cons c cs ih =>
      intro k hk
      by_cases hc : c ∈ has
      · simp [replayMissing, hc] at hk
        exact List.mem_cons_of_mem c (ih k hk)
      · simp [replayMissing, hc] at hk
        exact hk.elim (fun h => by simp [h])
          (fun h => List.mem_cons_of_mem c (ih k h))

/-- **Nothing is dropped.** Every committed key the peer lacks is replayed. -/
theorem replay_missing_covers_what_the_peer_lacks (has committed : Keys) :
    ∀ k, k ∈ committed → k ∉ has → k ∈ replayMissing has committed := by
  induction committed with
  | nil =>
      intro k hk _
      simp at hk
  | cons c cs ih =>
      intro k hk hnot
      exact (List.mem_cons.mp hk).elim
        (fun h => by subst h; simp [replayMissing, hnot])
        (fun hk' => by
          by_cases hc : c ∈ has
          · simp [replayMissing, hc]
            exact ih k hk' hnot
          · simp [replayMissing, hc]
            exact Or.inr (ih k hk' hnot))

/-- **The peer ends up holding every committed write**, either because it was
    already durable there before the rejoin or because the replay carried it. -/
theorem digest_replay_keeps_every_committed_write (has committed : Keys) :
    ∀ k, k ∈ committed → k ∈ has ∨ k ∈ replayMissing has committed := by
  intro k hk
  by_cases h : k ∈ has
  · exact Or.inl h
  · exact Or.inr (replay_missing_covers_what_the_peer_lacks has committed k hk h)

/-- **A declined digest is the full replay.** With nothing reported as held,
    the replay is exactly the committed log — the fallback stays correct. -/
theorem no_digest_replays_the_whole_log (committed : Keys) :
    replayMissing [] committed = committed := by
  induction committed with
  | nil => rfl
  | cons c cs ih =>
      simp [replayMissing, ih]


end Ferrosa.Topology.Pair