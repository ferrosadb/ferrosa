/-
  The apply fence, settled: three obligations proved, one red, one fixed
  ======================================================================

  A committed entry is either published as applied with its ordered system-table write behind it, or
  left unpublished and replayable, and the published index never moves backwards or skips an entry.
  Those three hold over every trace of the machine in `Ferrosa/Raft/StateMachine.lean`, for both
  publish policies and both stagings, so sharing the staged copy cannot break them.

  The fourth obligation used to be red: publication waited for the ordered system-table write to be
  *attempted*, not to succeed, so a failed write published an entry whose row was missing. `6c270518`
  made `flush_pending_system_writes` fallible and propagated into the latch, and the machine gained a
  `.systemFailFatal` event for that shape; `sysfail_blocks_publication` is the same trace the old
  disproof used, ending with nothing published. The pre-fix trace is kept as
  `sysfail_publishes_without_row`, because a trace of a machine that no longer ships is the clearest
  statement of what was wrong.

  The fifth obligation does not hold, and the staged copy is why: it shares `schema`, `engine` and
  `system_writer` with the live machine, so physical effects are visible before the published index
  moves. Only the metadata transition is genuinely isolated.

  The area continues in four siblings, and this file's ledger appends all of them so one area still
  prints and counts as one list. `Ferrosa/Raft/Surfaces.lean` audits the snapshot, the purge ceiling
  and the two restart paths. `Ferrosa/Raft/Startup.lean` asks what the node does on the next boot —
  whether a failure state repairs, refuses, or resyncs — and finds that the apply latch is in-memory
  and every restart replays the same failure. `Ferrosa/Raft/Partial.lean` enumerates what a partially
  applied entry leaves behind and classifies it: safe, divergent, corrupt, or stalled.
  `Ferrosa/Raft/Quarantine.lean` models the discharge the operator settled on — refuse traffic, stay a
  member, repair from a peer, verify before serving — and records, for each of its six obligations,
  the design property that holds and the code path that does not have it yet. Refusing to boot is no
  longer one of the accepted answers, so `Startup.lean`'s trichotomy row was restated.

  The proved statements quantify over every trace and are closed by induction on it. A disproof is a
  single trace whose fields reduce under `decide`, which is all a counterexample needs.
-/
import Ferrosa.Ledger
import Ferrosa.Raft.StateMachine
import Ferrosa.Raft.Surfaces
import Ferrosa.Raft.Startup
import Ferrosa.Raft.Partial
import Ferrosa.Raft.Quarantine

namespace Ferrosa.Raft

open Ferrosa.Ledger

/-! ## The reachable-state invariant

Only the fenced publish policy is treated this way; the pre-PR policy exists for the comparison and
needs none of it. `Invariant` is what `finish_pending_apply` establishes: the pending slot names the
next index, its two stages are recorded in order, and the published index never leads the write
attempt.
-/

structure Invariant (s : St) : Prop where
  /-- `pending_apply` always holds the entry after `last_applied`. -/
  pendingNext : ∀ k, s.pending = some k → k = s.applied + 1
  /-- A job whose ordered write has returned recorded that index as `attempted`. -/
  pendingAttempted : ∀ k, s.pending = some k → s.pendingSystemWrites = true → s.attempted = k
  /-- Publication never leads the write attempt. -/
  appliedLeAttempted : s.applied ≤ s.attempted
  /-- ... and the write attempt never runs more than one index ahead. -/
  attemptedBound : s.attempted ≤ s.applied + 1
  /-- A latched machine holds no job. -/
  latchClean : s.failed = true → s.pending = none

/-- **The initial state satisfies the invariant.** Nothing has been committed, no job is in flight,
    no write has been attempted and no failure is latched, so all five fields hold vacuously. -/
theorem inv_initial : Invariant St.initial where
  pendingNext := by simp [St.initial]
  pendingAttempted := by simp [St.initial]
  appliedLeAttempted := by simp [St.initial]
  attemptedBound := by simp [St.initial]
  latchClean := by simp [St.initial]

/-! ### Single-step preservation, one field at a time

Each field of the invariant is preserved by every event on its own, so preservation is five small
lemmas rather than one case split over ten events and five fields. Each lemma splits on the event
and on whether a job is in flight, which is the only thing that changes how an event is read.
-/

/-- **The pending slot still names the next index after any step.** `beginApply` opens it one past
    the published index; `finish` publishes exactly it and closes the slot; every other event
    leaves both fields alone or is refused while a job is outstanding. -/
theorem inv_pendingNext_step (stg : Staging) (s : St) (e : Ev)
    (hnext : ∀ k, s.pending = some k → k = s.applied + 1) :
    ∀ k, (step .late stg s e).pending = some k → k = (step .late stg s e).applied + 1 := by
  intro k hk
  rcases hp : s.pending with _ | n
  · cases e <;> simp only [PublishPolicy.isEarly, step, hp] at hk ⊢ <;> (try (repeat' split)) <;> simp_all
  · have hn := hnext n hp
    cases e <;> simp only [PublishPolicy.isEarly, step, hp] at hk ⊢ <;> (try (repeat' split)) <;> simp_all <;> (try (repeat' split)) <;> omega

/-- **A returned ordered write is recorded against the pending index**, whether it succeeded or
    failed: both stages set `attempted`, so the record never names an entry whose write did not
    return. -/
theorem inv_pendingAttempted_step (stg : Staging) (s : St) (e : Ev)
    (hatt : ∀ k, s.pending = some k → s.pendingSystemWrites = true → s.attempted = k) :
    ∀ k, (step .late stg s e).pending = some k → (step .late stg s e).pendingSystemWrites = true
      → (step .late stg s e).attempted = k := by
  intro k hk hw
  rcases hp : s.pending with _ | n
  · cases e <;> simp only [PublishPolicy.isEarly, step, hp] at hk hw ⊢ <;> (try (repeat' split)) <;> simp_all
  · cases e <;> simp only [PublishPolicy.isEarly, step, hp] at hk hw ⊢ <;>
      (try (repeat' split)) <;> simp_all <;> (try (repeat' split)) <;> omega

/-- **Publication never leads the write attempt.** The two facts above are exactly what this needs:
    the published index is either unchanged or set to the pending index, and that index is the one
    the write attempt records. -/
theorem inv_appliedLeAttempted_step (stg : Staging) (s : St) (e : Ev)
    (hnext : ∀ k, s.pending = some k → k = s.applied + 1)
    (hatt : ∀ k, s.pending = some k → s.pendingSystemWrites = true → s.attempted = k)
    (hle : s.applied ≤ s.attempted) :
    (step .late stg s e).applied ≤ (step .late stg s e).attempted := by
  rcases hp : s.pending with _ | n
  · cases e <;> simp only [PublishPolicy.isEarly, step, hp] at * <;> (try (repeat' split)) <;> simp_all <;> omega
  · have hn := hnext n hp
    cases e <;> simp only [PublishPolicy.isEarly, step, hp] at * <;> (try (repeat' split)) <;> simp_all <;> omega

/-- **The write attempt never runs more than one entry ahead of publication**, because the pending
    slot holds the next index and nothing else can raise it. -/
theorem inv_attemptedBound_step (stg : Staging) (s : St) (e : Ev)
    (hnext : ∀ k, s.pending = some k → k = s.applied + 1)
    (hbound : s.attempted ≤ s.applied + 1) :
    (step .late stg s e).attempted ≤ (step .late stg s e).applied + 1 := by
  rcases hp : s.pending with _ | n
  · cases e <;> simp only [PublishPolicy.isEarly, step, hp] at * <;> (try (repeat' split)) <;> simp_all <;> omega
  · have hn := hnext n hp
    cases e <;> simp only [PublishPolicy.isEarly, step, hp] at * <;> (try (repeat' split)) <;> simp_all <;> omega

/-- **A latched machine holds no job.** The latch is only ever set together with taking the pending
    handle, so no work is left that could move state behind the error. -/
theorem inv_latchClean_step (stg : Staging) (s : St) (e : Ev)
    (hlatch : s.failed = true → s.pending = none) :
    (step .late stg s e).failed = true → (step .late stg s e).pending = none := by
  intro hf
  cases e <;> simp only [PublishPolicy.isEarly, step] at hf ⊢ <;> (try (repeat' split)) <;> simp_all

/-- **Every event preserves the invariant**, including `cancel` (a no-op), `panic`, `crash` and
    `restart`. This is the one lemma the trace-level theorems below are allowed to assume. -/
theorem inv_step (stg : Staging) (s : St) (e : Ev) (h : Invariant s) : Invariant (step .late stg s e) where
  pendingNext := inv_pendingNext_step stg s e h.pendingNext
  pendingAttempted := inv_pendingAttempted_step stg s e h.pendingAttempted
  appliedLeAttempted := inv_appliedLeAttempted_step stg s e h.pendingNext h.pendingAttempted h.appliedLeAttempted
  attemptedBound := inv_attemptedBound_step stg s e h.pendingNext h.attemptedBound
  latchClean := inv_latchClean_step stg s e h.latchClean

/-- **The invariant holds of every state a trace can reach** from a state that satisfies it, by
    induction on the trace. -/
theorem inv_run (stg : Staging) : ∀ (tr : List Ev) (s : St), Invariant s → Invariant (run .late stg s tr)
  | [], _s, h => h
  | e :: es, s, h => inv_run stg es (step .late stg s e) (inv_step stg s e h)

/-! ## 1. Monotonicity: the published index never moves backwards

Replay and restart are the two events that could have moved the index backwards, and neither does:
`restart` rebuilds in-memory state from the snapshot at the published index, and replay is modelled
as starting at the next entry. The claim holds for both publish policies and both stagings, so
nothing this PR changes can introduce a regression here.
-/

/-- **One step never lowers the published index**, provided the pending entry is not behind it —
    which the invariant gives, and which also holds for the pre-PR policy, where the pending index
    is the already-published one. -/
theorem applied_monotone_step (pol : PublishPolicy) (stg : Staging) (s : St) (e : Ev)
    (h : ∀ k, s.pending = some k → s.applied ≤ k) :
    s.applied ≤ (step pol stg s e).applied := by
  rcases hp : s.pending with _ | n
  · cases e <;> simp only [PublishPolicy.isEarly, step, hp] at * <;> (try (repeat' split)) <;> simp_all <;> (try (repeat' split)) <;> omega
  · have hn := h n hp
    cases e <;> simp only [PublishPolicy.isEarly, step, hp] at * <;> (try (repeat' split)) <;> simp_all <;> (try (repeat' split)) <;> omega

/-- **The pending-entry condition survives a step**, so monotonicity can be chained along a trace. -/
theorem pending_ge_step (pol : PublishPolicy) (stg : Staging) (s : St) (e : Ev)
    (h : ∀ k, s.pending = some k → s.applied ≤ k) :
    ∀ k, (step pol stg s e).pending = some k → (step pol stg s e).applied ≤ k := by
  intro k hk
  rcases hp : s.pending with _ | n
  · cases e <;> simp only [PublishPolicy.isEarly, step, hp] at hk ⊢ <;> (try (repeat' split)) <;> simp_all <;> (try (repeat' split)) <;> omega
  · have hn := h n hp
    cases e <;> simp only [PublishPolicy.isEarly, step, hp] at hk ⊢ <;> (try (repeat' split)) <;> simp_all

/-- **MONOTONICITY (proved).** The published index is non-decreasing over every trace, for both
    publish policies and both stagings — including `restart`, which rebuilds in-memory state but
    does not move the published index. -/
theorem applied_monotone (pol : PublishPolicy) (stg : Staging) :
    ∀ (tr : List Ev) (s : St), (∀ k, s.pending = some k → s.applied ≤ k)
      → s.applied ≤ (run pol stg s tr).applied
  | [], _s, _ => Nat.le_refl _
  | e :: es, s, h =>
    Nat.le_trans (applied_monotone_step pol stg s e h)
      (applied_monotone pol stg es (step pol stg s e) (pending_ge_step pol stg s e h))

/-- **Monotonicity from an empty machine**, for the fenced publish policy: no trace of events can
    move the published index backwards. -/
theorem applied_monotone_late (stg : Staging) (tr : List Ev) :
    St.initial.applied ≤ (run .late stg St.initial tr).applied :=
  applied_monotone .late stg tr St.initial (by simp [St.initial])

/-! ## 2. Gaplessness: publication advances to the pending index only

An index is published only by `finish_pending_apply`, only for an entry whose job has returned, and
only to the next index. A reader therefore never sees a hole and never sees an index ahead of the
entry that earned it.
-/

/-- **No event advances the published index by more than one**, so a trace cannot skip an entry. -/
theorem applied_advances_by_one (stg : Staging) (s : St) (e : Ev)
    (hnext : ∀ k, s.pending = some k → k = s.applied + 1) :
    (step .late stg s e).applied ≤ s.applied + 1 := by
  rcases hp : s.pending with _ | n
  · cases e <;> simp only [PublishPolicy.isEarly, step, hp] at * <;> (try (repeat' split)) <;> simp_all
  · have hn := hnext n hp
    cases e <;> simp only [PublishPolicy.isEarly, step, hp] at * <;> (try (repeat' split)) <;> simp_all <;> (try (repeat' split)) <;> omega

/-- **GAPLESSNESS, part 1 (proved).** Under the fenced publish policy the only event that can
    advance the published index is `finish_pending_apply`. -/
theorem late_publish_only_at_finish (stg : Staging) (s : St) (e : Ev)
    (h : (step .late stg s e).applied ≠ s.applied) : e = .finish := by
  cases e <;> simp only [PublishPolicy.isEarly, step] at h <;> (try (repeat' split)) <;>
    first | rfl | (exfalso; exact h (by (repeat' split) <;> rfl))

/-- **GAPLESSNESS, part 2 (proved).** A publication that advances the index does so only for a
    retained job whose physical transition *and* ordered system-table write have both returned.
    Nothing else can publish. -/
theorem publish_requires_completed_job (stg : Staging) (s : St)
    (h : (step .late stg s .finish).applied ≠ s.applied) :
    ∃ k, s.pending = some k ∧ s.pendingPhysical = true ∧ s.pendingSystemWrites = true := by
  by_cases hpk : ∃ k, s.pending = some k ∧ s.pendingPhysical = true ∧ s.pendingSystemWrites = true
  · exact hpk
  · exfalso
    apply h
    simp only [step]
    rcases hp : s.pending with _ | n
    · simp
    · by_cases hd : s.pendingDead = true
      · simp [hd]
      · by_cases hc2 : s.pendingPhysical = true ∧ s.pendingSystemWrites = true
        · exact absurd ⟨n, hp, hc2⟩ hpk
        · simp [hd, hc2]

/-- **GAPLESSNESS, part 3 (proved).** The pending slot always names the next index, so publication
    advances the published index to exactly one past it and skips no entry. -/
theorem publish_advances_to_pending_index (stg : Staging) (s : St) (h : Invariant s)
    (hadv : (step .late stg s .finish).applied ≠ s.applied) :
    (step .late stg s .finish).applied = s.applied + 1 := by
  obtain ⟨k, hk, _, _⟩ := publish_requires_completed_job stg s hadv
  have hn := h.pendingNext k hk
  simp only [step, hk] at hadv ⊢
  by_cases hd : s.pendingDead = true
  · simp [hd] at hadv
  · by_cases hc : s.pendingPhysical = true ∧ s.pendingSystemWrites = true
    · simp [hd, hc]
      omega
    · simp [hd, hc] at hadv

/-! ## 3. Latch-closedness: after a failure nothing moves

A panicked apply latches the machine. Every trait entry point returns the latched error from then
on, and no event other than a restart changes a field of the state machine — the committed log may
still advance, because that is raft's log and not the state machine.
-/

/-- **LATCH-CLOSEDNESS (proved).** With the failure latch set and no pending job — which the code's
    own control flow guarantees, since the latch is only ever set together with taking the job — no
    event other than `restart` changes any state-machine field. -/
theorem latch_closed (pol : PublishPolicy) (stg : Staging) (s : St) (e : Ev)
    (hfailed : s.failed = true) (hpending : s.pending = none) (he : e ≠ .restart) :
    (step pol stg s e).applied = s.applied
    ∧ (step pol stg s e).metaState = s.metaState
    ∧ (step pol stg s e).external = s.external
    ∧ (step pol stg s e).attempted = s.attempted
    ∧ (step pol stg s e).written = s.written
    ∧ (step pol stg s e).pending = s.pending
    ∧ (step pol stg s e).failed = true := by
  cases e <;> simp only [PublishPolicy.isEarly, step, hpending] <;> (try (repeat' split)) <;> simp_all

/-- **The latch is never set while a job is retained**, so a latched machine has no work left that
    could move state behind the error. -/
theorem latch_clears_pending (stg : Staging) (s : St) (e : Ev)
    (hlatch : s.failed = true → s.pending = none) :
    (step .late stg s e).failed = true → (step .late stg s e).pending = none :=
  inv_latchClean_step stg s e hlatch

/-! ## 4. Replay safety: what publication promises about the row

Split into what the code can and cannot deliver.

* `publish_after_write_attempt` — **proved unconditionally.** The published index never advances past
  an index whose ordered system-table write has not *returned*. This is the ordering guarantee the
  fence is about, and it survives cancellation because the job is retained.
* `rows_present_when_writes_succeed` — **proved conditionally.** If no ordered write ever returned an
  error, every published index's row is durable.
* `sysfail_publishes_without_row` — **disproved** for the unconditional reading. A failed ordered
  write is logged and swallowed, so the job completes and the entry publishes with the durable
  prefix behind it; replay then starts past it.
-/

/-- **Every published index has had its ordered system-table write return**, so publication can
    never run ahead of the attempt. -/
theorem publish_after_write_attempt (stg : Staging) (tr : List Ev) :
    ∀ i, 1 ≤ i → i ≤ (run .late stg St.initial tr).applied
      → i ≤ (run .late stg St.initial tr).attempted := by
  intro i hi hle
  have h := (inv_run stg tr St.initial inv_initial).appliedLeAttempted
  omega

/-- The durable prefix keeps up with the write attempt as long as no ordered write fails. -/
structure WrittenInv (s : St) : Prop where
  /-- Every returned write is durable. -/
  writtenEqAttempted : s.written = s.attempted

/-- **Nothing has been written and nothing attempted**, so the durable prefix and the attempt agree
    on the empty machine. -/
theorem written_inv_initial : WrittenInv St.initial where
  writtenEqAttempted := by simp [St.initial]

/-- **A successful ordered write raises the durable prefix and the attempt together.** A failing
    write is the only event that separates them, which is why this lemma excludes it — in both of its
    shapes, since `6c270518` an error that is propagated leaves the attempt recorded and the prefix
    behind, exactly as an error that is swallowed does. -/
theorem written_inv_step (stg : Staging) (s : St) (e : Ev) (h : WrittenInv s)
    (hnext : ∀ k, s.pending = some k → k = s.applied + 1)
    (hge : s.applied ≤ s.attempted) (hb : s.attempted ≤ s.applied + 1)
    (he : e ≠ .systemFail) (hef : e ≠ .systemFailFatal) : WrittenInv (step .late stg s e) where
  writtenEqAttempted := by
    have hw := h.writtenEqAttempted
    rcases hp : s.pending with _ | n
    · cases e <;> simp only [PublishPolicy.isEarly, step, hp] at * <;>
        (try (repeat' split)) <;> simp_all <;> (try (repeat' split)) <;> omega
    · have hn := hnext n hp
      cases e <;> simp only [PublishPolicy.isEarly, step, hp] at * <;>
        (try (repeat' split)) <;> simp_all <;> (try (repeat' split)) <;> omega

/-- **A trace in which no ordered write fails keeps the durable prefix level with the attempt**, by
    induction on the trace. -/
theorem written_inv_run (stg : Staging) :
    ∀ (tr : List Ev) (s : St), Invariant s → WrittenInv s → (∀ e, e ∈ tr → e ≠ .systemFail)
      → (∀ e, e ∈ tr → e ≠ .systemFailFatal) → WrittenInv (run .late stg s tr)
  | [], s, _, h, _, _ => h
  | e :: es, s, hinv, h, hno, hnoF =>
    written_inv_run stg es (step .late stg s e) (inv_step stg s e hinv)
      (written_inv_step stg s e h hinv.pendingNext hinv.appliedLeAttempted
        hinv.attemptedBound (hno e (by simp)) (hnoF e (by simp)))
      (fun e' he' => hno e' (by simp [he'])) (fun e' he' => hnoF e' (by simp [he']))

/-- **REPLAY SAFETY, durability half (proved, conditional).** If no ordered system-table write in
    the trace returned an error, the published index never runs past the durable system-table
    prefix: every published index has its row in the engine. -/
theorem rows_present_when_writes_succeed (stg : Staging) (tr : List Ev)
    (hno : ∀ e, e ∈ tr → e ≠ .systemFail) (hnoF : ∀ e, e ∈ tr → e ≠ .systemFailFatal) :
    (run .late stg St.initial tr).applied ≤ (run .late stg St.initial tr).written := by
  have hinv := (inv_run stg tr St.initial inv_initial).appliedLeAttempted
  have hwi := written_inv_run stg tr St.initial inv_initial written_inv_initial hno hnoF
  have hw := hwi.writtenEqAttempted
  omega

/-- **REPLAY SAFETY, durability half — DISPROVED (the pre-`6c270518` shape, kept as the regression
    trace).** A failed ordered system-table write is logged and swallowed, so the job completes,
    publication advances, the durable prefix stays behind, and a restart does not re-issue the write
    because replay starts at the next index. No cancellation is involved. This is a trace of the
    machine's `.systemFail` event, which is what the code did before `6c270518`; the shipped shape is
    the `.systemFailFatal` trace below. -/
theorem sysfail_publishes_without_row :
    (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFail, .finish]).applied = 1
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFail, .finish]).attempted = 1
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFail, .finish]).written = 0
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFail, .finish]).failed = false
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFail, .finish, .restart]).written = 0 := by
  decide

/-- **THE FIX, AT THE TRACE THE OLD DISPROOF USED: A FAILED PROJECTION PUBLISHES NOTHING
    (PROVED).** With the job propagating the error (`state_machine.rs:2334-2342` → `:454-464`) the
    entry does not advance `last_applied`, the durable prefix does not advance either, and the
    machine latches — so the same four events that used to publish row-less now publish nothing, and
    a restart replays the entry instead of starting after it. -/
theorem sysfail_blocks_publication :
    (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFailFatal]).applied = 0
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFailFatal]).attempted = 1
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFailFatal]).written = 0
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFailFatal]).failed = true
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFailFatal]).pending = none
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFailFatal, .finish]).applied = 0
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .systemFailFatal, .restart, .beginApply, .physical,
         .systemWrite, .finish]).applied = 1 := by
  decide

/-- **…and nothing republishes it before a restart.** The latch closes every entry point, so over any
    trace that does not restart the process the published index is frozen where the failure left it.
    The restart is the one event that clears the latch, which is `Ferrosa/Raft/Startup.lean`'s
    finding, not a hole in this one. -/
theorem a_latched_entry_is_not_republished (stg : Staging) (tr : List Ev)
    (hne : ∀ e, e ∈ tr → e ≠ .restart) (s : St) (hf : s.failed = true) (hp : s.pending = none) :
    (run .late stg s tr).applied = s.applied := by
  induction tr generalizing s with
  | nil => rfl
  | cons e es ih =>
    obtain ⟨h1, h2, h3, h4, h5, h6, h7⟩ := latch_closed .late stg s e hf hp (hne e (by simp))
    rw [show run .late stg s (e :: es) = run .late stg (step .late stg s e) es from rfl]
    rw [ih (fun e' he' => hne e' (by simp [he'])) (step .late stg s e) h7 (h6.trans hp)]
    exact h1

/-- **Cancellation is not a state change.** `finish_pending_apply` polls the retained handle, so
    dropping the awaiter leaves both the handle and the job in place. -/
theorem cancel_is_noop (pol : PublishPolicy) (stg : Staging) (s : St) :
    step pol stg s .cancel = s := rfl

/-- **A cancelled apply is reaped and publishes with its ordered write.** The write ran inside the
    retained job, so the next trait call finishes the entry rather than starting it again. -/
theorem late_cancel_still_publishes :
    (run .late .shared St.initial
        [.propose, .beginApply, .cancel, .physical, .systemWrite, .finish]).applied = 1
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .cancel, .physical, .systemWrite, .finish]).written = 1
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .cancel, .physical, .systemWrite, .finish]).pending = none := by
  decide

/-- **The pre-PR shape (DISPROVED).** Publishing the index at the top of the apply loop means a
    cancellation during the separately awaited system-table flush leaves the entry published with
    its write never attempted — and no pending job left to finish it. -/
theorem early_cancel_publishes_before_write :
    (run .early .shared St.initial [.propose, .beginApply, .physical, .cancel]).applied = 1
    ∧ (run .early .shared St.initial [.propose, .beginApply, .physical, .cancel]).attempted = 0
    ∧ (run .early .shared St.initial [.propose, .beginApply, .physical, .cancel]).written = 0
    ∧ (run .early .shared St.initial
        [.propose, .beginApply, .physical, .cancel, .restart]).written = 0 := by
  decide

/-! ## 5. Aliasing: the staged copy is not isolated

`staged_apply()` clones the metadata, but `schema`, `engine`, `ring` and `system_writer` are `Arc`
clones of the live machine's handles, and `apply_command` mutates the shared `Schema` and
`StorageEngine` while it runs on the blocking thread — before publication. The model records that as
`external`, separately from the isolated `metaState`.

Monotonicity, gaplessness, latch-closedness and the write-attempt ordering are all proved with the
staging universally quantified, so sharing cannot break them. What sharing costs is the strong form
of obligation 4 — "no state in which part of the entry is visible" — which fails for `shared` and
holds only for the `isolated` hypothesis.
-/

/-- **ALIASING (disproved for the code as written).** One step of the job exposes entry 1 through
    the shared schema and engine while the published index and the live metadata still say nothing
    was applied. Replace `physical` with `cancel` in the trace and the window stays open until the
    next trait call arrives to reap the job. -/
theorem shared_exposes_effects_before_publication :
    (run .late .shared St.initial [.propose, .beginApply, .physical]).external = 1
    ∧ (run .late .shared St.initial [.propose, .beginApply, .physical]).metaState = 0
    ∧ (run .late .shared St.initial [.propose, .beginApply, .physical]).applied = 0 := by
  decide

/-- **The same trace with owned handles touches nothing until publication**, which is what sharing
    gives up. -/
theorem isolated_hides_effects_until_publication :
    (run .late .isolated St.initial [.propose, .beginApply, .physical]).external = 0
    ∧ (run .late .isolated St.initial
        [.propose, .beginApply, .physical, .systemWrite, .finish]).external = 1 := by
  decide

/-- **What sharing does not cost.** The metadata transition and the published index move together,
    because publication adopts the completed metadata and sets the index in the same poll — there is
    no await between them — and the fence makes every entry point await the job first. The staged
    metadata is genuinely isolated; only the shared handles are not. -/
theorem metadata_published_with_index (stg : Staging) :
    (run .late stg St.initial
        [.propose, .beginApply, .physical, .systemWrite, .finish]).metaState = 1
    ∧ (run .late stg St.initial
        [.propose, .beginApply, .physical, .systemWrite, .finish]).applied = 1
    ∧ (run .late stg St.initial
        [.propose, .beginApply, .physical, .systemWrite]).metaState = 0 := by
  cases stg <;> decide

/-- **ALIASING under a panicking job.** The physical transition has already touched the shared
    handles when the job dies, so the shared effects are ahead of the published index, the latch
    closes the machine, and the entry is left published-but-not-applied. Recovery is replay after
    restart, which rebuilds in-memory state from the snapshot at the published index; the partially
    written system rows are the only residue, and whether replay converges on re-writing them depends
    on idempotency of the system-table writer and the engine, which this model does not cover. -/
theorem panic_after_physical_latches :
    (run .late .shared St.initial [.propose, .beginApply, .physical, .panic, .finish]).failed = true
    ∧ (run .late .shared St.initial [.propose, .beginApply, .physical, .panic, .finish]).applied = 0
    ∧ (run .late .shared St.initial [.propose, .beginApply, .physical, .panic, .finish]).external = 1
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .panic, .finish, .restart]).failed = false
    ∧ (run .late .shared St.initial
        [.propose, .beginApply, .physical, .panic, .finish, .restart]).external = 0 := by
  decide

/-! ## The ledger

Twenty-two obligations, each with the theorem that settles it: twelve the fence and its snapshot
surface, five on startup and replay, and five on what a partial apply leaves behind. Ten are red,
each naming the single trace or table that refutes it.

Reading this table answers what a client may assume about a schema change once raft reports the entry
applied, where that assumption stops (`Surfaces.lean`), what a node does on the next boot when the
assumption failed (`Startup.lean`), and what it holds when it comes back (`Partial.lean`).
-/

def ledger : List SpecItem := [
  ⟨"the published index never moves backwards",
   .met "Raft.applied_monotone"⟩,
  ⟨"publication advances the index to the pending entry and skips none",
   .met "Raft.publish_advances_to_pending_index"⟩,
  ⟨"once an apply has failed, nothing that follows changes the state machine",
   .met "Raft.latch_closed"⟩,
  ⟨"an entry is published only when its ordered system-table write landed",
   .met "Raft.sysfail_blocks_publication"⟩,
  ⟨"the staged copy leaves no trace until it publishes",
   .failing
     "staged_apply clones the metadata but shares the schema, engine and system-writer handles, so the physical transition is visible on the blocking thread while the published index still points at the previous entry"
     "Raft.shared_exposes_effects_before_publication"⟩
] ++ surfaceItems ++ startupItems ++ partialItems ++ grantItems ++ quarantineItems

/-- The area records thirty-four obligations of which sixteen are unmet — the fence's one, the snapshot
    surface's three, startup's three, partial application's two, the grant projection's one and the
    quarantine design's six — and both numbers are pinned here: closing one, or adding a claim, forces
    this table to be updated in the same change. The quarantine list is six `design:` rows that hold
    and six `code:` rows that do not, so its six reds are the gap between the target lifecycle and the
    process as it boots today. -/
theorem raft_has_thirty_four_items_and_sixteen_red : ledger.length = 34 ∧ redCount ledger = 16 := by
  decide

def main : IO Unit :=
  printLedger "the raft apply fence" ledger

/-! ## The axiom audit

Each proved obligation is checked here for what it rests on. The trace-level proofs report `propext`
and `Quot.sound`, which `decide` and `omega` introduce; the counterexample traces, being finite
computations, report no axioms at all. None of them reports `sorryAx`.
-/

#print axioms applied_monotone
#print axioms applied_monotone_late
#print axioms applied_advances_by_one
#print axioms late_publish_only_at_finish
#print axioms publish_requires_completed_job
#print axioms publish_advances_to_pending_index
#print axioms latch_closed
#print axioms publish_after_write_attempt
#print axioms rows_present_when_writes_succeed
#print axioms sysfail_publishes_without_row
#print axioms early_cancel_publishes_before_write
#print axioms shared_exposes_effects_before_publication
#print axioms panic_after_physical_latches

end Ferrosa.Raft
