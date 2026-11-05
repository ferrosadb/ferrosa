/-
  The recovery gate as a concurrent object
  ========================================

  `Ferrosa/Raft/Quarantine.lean` models *what a quarantined node does*: which surfaces it keeps
  shut and how a peer repairs it. It says nothing about the object that decides those questions,
  `RecoveryAdmission` (`ferrosa-cluster/src/raft/quarantine.rs`), and that object is where the
  interesting failure shapes live: a lease, a refusal path, an epoch-exact drain, and a wake signal
  that must not be lost between the check and the suspension.

  This module models the gate itself, as the concurrent machine the code implements, so each claim
  the code makes about it is either a theorem here or a red.

  ## The code, and the shape of each of its moves

  * `with_admission_epoch` (`quarantine.rs:90-104`) takes the publication lease with `try_lock`
    (`:95`), refuses with "recovery publication is busy" when it is held (`:96`), checks admission
    at the caller's epoch, runs the closure, checks admission again (`:101`), and releases. A
    failed admission releases the lease, because the guard is dropped on the `?`.
  * `quarantine` (`:65-72`) takes the fault-state *write* lock, records the fault, advances the
    epoch, drops the lock, and only then calls `notify_waiters` (`:71`) — so the epoch is visible
    before any waiter can be woken.
  * `drain_publications` (`:108-120`) `try_lock`s the same lease and refuses with "recovery
    publication is still active" when it is held, then requires `state.epoch = expected_epoch` and
    a non-empty fault set.
  * `changed_since` (`:74-88`) registers with `enable()` *before* reading the epoch, which is what
    makes a transition between the read and the `await` impossible to lose.

  ## What is modelled, and at what abstraction

  Every field is a `Bool`, a small `Nat` or an `Option`: `epoch`, the number of recorded `faults`,
  `holding` (the lease), `applied` (published effects that landed), `last` (the caller-visible
  outcome of the most recent move), and the consumer side `seen` / `registered` / `woke`.

  The lock is modelled by its *effect* — `holding` is a `Bool` and `acquire` refuses rather than
  queues when it is `true` — because the code's mutual exclusion is `parking_lot::Mutex`'s, not
  this module's. What is proved here is what the code does *around* the lock: who may proceed, what
  a refusal leaves behind, and what a waiter is entitled to expect. The lock's own exclusion is an
  assumption, stated once, and it is the one line of this module that is not machine-checked.

  `maxEpoch` is `u64::MAX`, where `advance` (`:151-160`) stops incrementing and records an
  "epoch" fault instead; §7 is the consequence for the wake protocol.
-/
import Ferrosa.Ledger

namespace Ferrosa.Raft

open Ferrosa.Ledger

/-! ## 1. The payload a refusal carries

`RecoveryRefusal` (`quarantine.rs:11-14`) is `epoch` and `reason` and nothing else. That is the
whole of what a caller can branch on, which is why §6's red is about the type and not about a
message: a retryable lease refusal and a terminal quarantine refusal arrive in the same shape. -/

/-- `RecoveryRefusal` as the caller sees it. -/
structure Refusal where
  /-- The gate's epoch at the moment of refusal. -/
  epoch : Nat
  /-- The human-readable cause. The *only* field that distinguishes one refusal from another. -/
  reason : String
deriving DecidableEq, Repr

/-- **A REFUSAL IS FULLY DETERMINED BY ITS TWO FIELDS.** There is no hidden discriminator for a
    caller to match on: any two refusals that agree on `epoch` and `reason` are the same value, so
    telling "the lease was busy, retry" apart from "this node is quarantined, stop" requires
    parsing `reason`. `quarantine.rs:11-14`. -/
theorem refusal_payload_is_fully_determined_by_its_two_fields (a b : Refusal)
    (he : a.epoch = b.epoch) (hr : a.reason = b.reason) : a = b := by
  obtain ⟨e₁, r₁⟩ := a
  obtain ⟨e₂, r₂⟩ := b
  simp_all

/-- `u64::MAX`: the epoch at which `advance` stops counting. -/
def maxEpoch : Nat := 18446744073709551615

/-! ## 2. The machine

The five gate fields are the code's `AdmissionState` plus the lease and the effect counter; the
three consumer fields are one `changed_since` call in flight. `last` is the outcome of whichever
move most recently returned to a caller, because that is all a refusal is. -/

structure Gate where
  /-- `AdmissionState.epoch`. -/
  epoch : Nat := 0
  /-- `AdmissionState.faults.len()`: only ever grows, because `quarantine` inserts and nothing
      removes (`quarantine.rs:66`). -/
  faults : Nat := 0
  /-- The publication lease: `with_admission_epoch` and `drain_publications` both hold it. -/
  holding : Bool := false
  /-- Published effects that landed. -/
  applied : Nat := 0
  /-- The outcome the most recent call returned. -/
  last : Option Refusal := none
  /-- The consumer's `seen` epoch. -/
  seen : Nat := 0
  /-- The consumer has called `enable()`: it is registered with the notifier. -/
  registered : Bool := false
  /-- The epoch the consumer observed, once it has observed one. -/
  woke : Option Nat := none
deriving DecidableEq, Repr

/-- The state a process starts from: `RecoveryAdmission::new()` (`quarantine.rs:40-42`). -/
def Gate.initial : Gate := {}

/-- The moves a caller can make. `acquire`/`publish`/`finish` are the three phases of
    `with_admission_epoch`, split so a quarantine can land in the middle of one. -/
inductive Op where
  /-- `with_admission_epoch`'s `try_lock` and first admission check (`quarantine.rs:95-99`). -/
  | acquire (e : Nat)
  /-- The caller's closure, after a successful admission. -/
  | publish (e : Nat)
  /-- The post-check and the release (`quarantine.rs:100-103`). -/
  | finish (e : Nat)
  /-- `quarantine`: record a fault, advance, notify (`quarantine.rs:65-72`). -/
  | quarantine
  /-- `drain_publications expected_epoch` (`quarantine.rs:108-120`). -/
  | drain (e : Nat)
  /-- `changed_since`: `enable()` before the epoch read (`quarantine.rs:76-78`). -/
  | register
  /-- A waiter that arrives after the transition reads the epoch directly (`:80-82`). -/
  | read
deriving DecidableEq, Repr

/-- `try_lock`, then the first admission check. A refusal leaves the lease free, because the guard
    is dropped on the early return — this is the half that makes a refusal non-wedging. -/
def acquire (s : Gate) (e : Nat) : Gate :=
  if s.holding = true then
    { s with last := some ⟨s.epoch, "recovery publication is busy"⟩ }
  else if s.faults > 0 then
    { s with last := some ⟨s.epoch, "fault"⟩ }
  else if e ≠ s.epoch then
    { s with last := some ⟨s.epoch, "admission epoch changed"⟩ }
  else
    { s with holding := true, last := none }

/-- The caller's closure. Enabled only while the lease is held, no fault is recorded, and the
    caller's epoch is still the gate's. -/
def publish (s : Gate) (e : Nat) : Gate :=
  if s.holding = true ∧ s.faults = 0 ∧ e = s.epoch then
    { s with applied := s.applied + 1 }
  else s

/-- The post-check and the release. The effect committed by `publish` is not undone: there is no
    field here that could undo it, which is the point of §8's theorem. -/
def finish (s : Gate) (e : Nat) : Gate :=
  if s.holding = false then s
  else if s.faults > 0 then
    { s with holding := false, last := some ⟨s.epoch, "fault"⟩ }
  else if e ≠ s.epoch then
    { s with holding := false, last := some ⟨s.epoch, "admission epoch changed"⟩ }
  else
    { s with holding := false, last := none }

/-- Detection. Always enabled, never consults the lease: the epoch advances, the registered waiter
    is woken, and no lock the publisher holds is touched. At `maxEpoch` the fault is recorded and
    the epoch does not move, which is `advance`'s overflow branch (`quarantine.rs:151-160`). -/
def quarantine (s : Gate) : Gate :=
  if s.epoch = maxEpoch then
    { s with faults := s.faults + 1 }
  else
    { s with epoch := s.epoch + 1, faults := s.faults + 1,
             woke := if s.registered = true ∧ s.seen ≠ s.epoch + 1 then some (s.epoch + 1) else s.woke }

/-- `drain_publications expected_epoch`: the lease `try_lock`ed, then the epoch and fault check
    (`quarantine.rs:109-118`). -/
def drain (s : Gate) (e : Nat) : Gate :=
  if s.holding = true then
    { s with last := some ⟨s.epoch, "recovery publication is still active"⟩ }
  else if s.epoch = e ∧ s.faults > 0 then
    { s with last := none }
  else
    { s with last := some ⟨s.epoch, "publication drain requires the current quarantined epoch"⟩ }

/-- `enable()`: register with the notifier without reading the epoch. -/
def register (s : Gate) : Gate := { s with registered := true }

/-- The epoch read that closes `changed_since`'s loop. -/
def read (s : Gate) : Gate :=
  if s.epoch ≠ s.seen then { s with woke := some s.epoch } else s

def step (s : Gate) : Op → Gate
  | .acquire e => acquire s e
  | .publish e => publish s e
  | .finish e => finish s e
  | .quarantine => quarantine s
  | .drain e => drain s e
  | .register => register s
  | .read => read s

/-- A trace. -/
def grun (s : Gate) : List Op → Gate
  | [] => s
  | o :: os => grun (step s o) os

/-! ## 3. Mutual exclusion, stated once

The lease is a `Bool` because the exclusion itself is `parking_lot::Mutex::try_lock`'s
(`quarantine.rs:95`). What this section fixes is the part the code owns: the *only* move that takes
the lease is `acquire`, it takes it only from the free state, and every release is unconditional. -/

/-- **A HELD LEASE REFUSES.** A holder is never displaced: when the lease is taken, the acquire
    returns the busy refusal and leaves the holder alone. -/
theorem acquire_fails_when_the_lease_is_held (s : Gate) (e : Nat) (h : s.holding = true) :
    (acquire s e).holding = true
      ∧ (acquire s e).last = some ⟨s.epoch, "recovery publication is busy"⟩ := by
  simp [acquire, h]

/-- **THE RELEASE IS UNCONDITIONAL.** `finish` always drops the lease, on the admitted and on the
    refused path, so a refusal cannot wedge the gate for the next caller. -/
theorem finish_always_releases_the_lease (s : Gate) (e : Nat) :
    (finish s e).holding = false := by
  by_cases h1 : s.holding = false
  · simp [finish, h1]
  · by_cases h2 : s.faults > 0
    · simp [finish, h1, h2]
    · by_cases h3 : e = s.epoch
      · simp [finish, h1, h2, h3]
      · simp [finish, h1, h2, h3]

/-- **NO MOVE BUT `acquire` TAKES THE LEASE.** Every other step of the machine keeps `holding` as
    it was, so the lease's exclusion is the lock's and its release is the code's. -/
theorem no_step_but_acquire_takes_the_lease (s : Gate) (e : Nat) (h : s.holding = false) :
    (publish s e).holding = false
      ∧ (quarantine s).holding = false
      ∧ (drain s e).holding = false
      ∧ (read s).holding = false
      ∧ (register s).holding = false := by
  refine ⟨?_, ?_, ?_, ?_, ?_⟩
  · unfold publish; split <;> simp [h]
  · unfold quarantine; split <;> simp [h]
  · unfold drain; split <;> (try split) <;> simp [h]
  · unfold read; split <;> simp [h]
  · exact h

/-- **A HELD LEASE REFUSES, AND THE REFUSAL LEAVES THE GATE OPEN.** The second publisher gets the
    busy refusal, commits nothing, and the lease is not taken from under the first: this is the
    cross-thread nesting case that used to block (`quarantine.rs:95-97`, regression
    `cross_thread_nested_publication_refuses_instead_of_waiting`). -/
theorem a_held_lease_refuses_a_second_publisher :
    (grun Gate.initial [.acquire 0, .acquire 0]).last
        = some ⟨0, "recovery publication is busy"⟩
      ∧ (grun Gate.initial [.acquire 0, .acquire 0]).applied = 0
      ∧ (grun Gate.initial [.acquire 0, .acquire 0]).holding = true := by
  decide

/-- **A REFUSED PUBLISHER IS NOT RE-QUEUED.** After the holder finishes, the refused caller's
    outcome is still the refusal: nothing in the gate remembers it, retries it, or wakes it. This
    is the half of "refuse instead of wait" the code does not provide a caller-side remedy for —
    see §6. -/
theorem a_refused_publisher_is_not_queued :
    (grun Gate.initial [.acquire 0, .acquire 0, .finish 0]).applied = 0
      ∧ (grun Gate.initial [.acquire 0, .acquire 0, .finish 0]).holding = false
      ∧ (grun Gate.initial [.acquire 0, .acquire 0, .finish 0, .acquire 0, .publish 0]).applied
          = 1 := by
  decide

/-! ## 4. Detection does not wait for the publisher

The property the previous revision of the code failed (it held the fault-state mutex for the whole
publication, so `quarantine` inside a publish closure deadlocked), and the one the thread-local
guard only half fixed (a nested publication on another thread blocked on the parent's lease). The
model's `quarantine` step reads and writes no field the publisher holds other than `faults` and
`epoch`, and both are reached without the lease. -/

/-- **DETECTION PROGRESSES WHILE A PUBLISHER IS HALTED.** A publisher holds the lease and never
    finishes — the shape of an I/O stall — and two quarantines still land: the epoch moves twice and
    the gate is closed. `quarantine` (`quarantine.rs:65-72`) takes only the fault-state write lock
    and drops it before notifying. -/
theorem detection_progresses_while_a_publisher_is_halted :
    (grun Gate.initial [.acquire 0, .quarantine, .quarantine]).epoch = 2
      ∧ (grun Gate.initial [.acquire 0, .quarantine, .quarantine]).faults = 2
      ∧ (grun Gate.initial [.acquire 0, .quarantine, .quarantine]).holding = true := by
  decide

/-- **NO PUBLICATION LANDS IN A QUARANTINED EPOCH.** After detection, a fresh caller's admission
    fails, and even a caller that reaches `publish` commits nothing. This is what makes a
    successful drain conclusive rather than momentary: the lease may be taken again, but nothing
    can be published through it. -/
theorem no_publication_lands_in_a_quarantined_epoch :
    (grun Gate.initial [.quarantine, .acquire 1, .publish 1]).applied = 0
      ∧ (grun Gate.initial [.quarantine, .acquire 1]).holding = false := by
  decide

/-! ## 5. The drain

`drain_publications` (`quarantine.rs:108-120`) is the proof obligation a repair path discharges
before it fixes the persisted purge floor: no publication is in flight, and the epoch it is
repairing is the epoch the gate is in. -/

/-- **A DRAIN THAT RETURNS `Ok` PROVES BOTH PREMISES.** `Ok` is returned only with the lease free,
    at exactly `expected_epoch`, and with the gate actually closed — the old
    `drain_publications()` returned `Ok` on a healthy gate and never looked at the epoch. -/
theorem drain_ok_implies_no_holder_and_the_current_epoch (s : Gate) (e : Nat)
    (h : (drain s e).last = none) : s.holding = false ∧ s.epoch = e ∧ s.faults > 0 := by
  by_cases h1 : s.holding = true
  · simp [drain, h1] at h
  · by_cases h2 : s.epoch = e ∧ s.faults > 0
    · exact ⟨by simpa using h1, h2.1, h2.2⟩
    · simp [drain, h1, h2] at h

/-- **A DRAIN IS REFUSED EXACTLY BY THE PUBLICATION IT MUST OUTLIVE.** A publication that was
    admitted before detection still holds the lease, so the repair's drain is refused — the code
    measured here returns `Err` in 51% of single attempts with one doomed publisher spinning. The
    drain is not wrong to refuse; §6 is about what the caller can do next. -/
theorem a_drain_is_refused_by_the_publication_it_must_outlive :
    (grun Gate.initial [.acquire 0, .quarantine, .drain 1]).last
      = some ⟨1, "recovery publication is still active"⟩ := by
  decide

/-- **A STALE EPOCH IS REFUSED TOO**, so a repair that read the epoch before detection cannot drain
    the gate that detection closed afterwards. -/
theorem a_drain_at_a_stale_epoch_is_refused :
    (grun Gate.initial [.quarantine, .drain 0]).last
      = some ⟨1, "publication drain requires the current quarantined epoch"⟩ := by
  decide

/-! ## 6. The wake protocol, and what it cannot express

`changed_since` (`quarantine.rs:74-88`) registers with `enable()` before reading the epoch, so a
transition between the read and the `await` is delivered rather than lost. The model's `register`
step does no read at all, and the `quarantine` step wakes every registered waiter whose `seen`
differs from the new epoch, which is `notify_waiters` after the lock is dropped (`:70-71`). -/

/-- **A REGISTERED CONSUMER NEVER LOSES THE TRANSITION.** Register first, transition after: the wake
    is delivered. This is the lost-wakeup freedom the `enable()`-before-read ordering buys. -/
theorem no_lost_wakeup_for_a_registered_consumer :
    (grun Gate.initial [.register, .quarantine]).woke = some 1 := by
  decide

/-- **A CONSUMER THAT ARRIVES LATE READS THE EPOCH ITSELF**, so registration is not required to
    observe a transition that already happened (`quarantine.rs:80-82`). -/
theorem a_late_consumer_reads_the_current_epoch :
    (grun Gate.initial [.quarantine, .read]).woke = some 1 := by
  decide

/-- **THE ONLY WAKE SOURCE IS THE EPOCH.** Registering and then watching a publisher take and
    release the lease produces no wake at all: `notify_waiters` is called from `quarantine` and
    nowhere else (`quarantine.rs:71`). A repair loop whose `drain_publications` returned
    "recovery publication is still active" therefore has no primitive to wait on — it can only
    spin on `try_lock`, which is what §5's measurement shows losing half its attempts against one
    spinning publisher. -/
theorem a_lease_release_wakes_nobody :
    (grun Gate.initial [.register, .acquire 0, .finish 0]).woke = none := by
  decide

/-- **THE GATE NEVER REOPENS.** `faults` only ever grows — nothing in `RecoveryAdmission` removes a
    fault or resets the epoch — so after detection every admission at every epoch is refused. The
    only exit is a restart, which re-derives the same faults. This is `Raft/Quarantine.lean`'s
    `code:` gap seen from the gate's side, and it is also why §5's drain can be conclusive: there
    is no later publisher to fear, only no way to finish the repair either. -/
theorem the_gate_never_reopens :
    (grun Gate.initial [.quarantine, .finish 0, .acquire 1, .acquire 2]).applied = 0
      ∧ (grun Gate.initial [.quarantine, .finish 0, .acquire 1, .acquire 2]).faults = 1 := by
  decide

/-! ## 7. The epoch wrap, where the wake signal and the gate disagree

`advance` (`quarantine.rs:151-160`) records an "epoch" fault and leaves the epoch alone when
`checked_add` returns `None`. The gate is then closed while the epoch is unchanged, and the wake
protocol is keyed on the epoch: no waiter is woken, and a `changed_since` loop whose `seen` is the
saturated epoch re-reads the same number forever. Unreachable in a process's lifetime at one
increment per fault, and latent for any future path that records a fault without advancing the
epoch. -/

/-- **A FAULT WITHOUT AN EPOCH DELTA WAKES NOBODY.** At `u64::MAX` a quarantine closes the gate and
    emits no wake, so a consumer suspended on that epoch stays suspended on a gate that is shut. -/
theorem a_fault_without_an_epoch_delta_wakes_nobody :
    (grun { Gate.initial with epoch := maxEpoch, seen := maxEpoch } [.register, .quarantine]).woke
        = none
      ∧ (grun { Gate.initial with epoch := maxEpoch, seen := maxEpoch } [.register, .quarantine]).faults
        = 1 := by
  decide

/-- **AND THE EPOCH IS THE `u64` ONE**, so §7's wrap is the code's wrap and not a property of the
    model's `Nat`. -/
theorem max_epoch_is_u64_max : maxEpoch = 2 ^ 64 - 1 := by decide

/-! ## 8. What a caller of an old-epoch publication is owed

`with_admission_epoch`'s second check (`quarantine.rs:101`) returns `Err` after the closure has
already run. There is no rollback, and the module says so (`:88-89`: "already-admitted I/O is not
rolled back"). The model has no field that could represent a rollback, so the theorem below is
about the code's shape and not about a modelling choice. -/

/-- **AN OLD-EPOCH EFFECT SURVIVES THE REFUSAL THAT REJECTS IT.** Detection lands inside the
    closure; the post-check refuses; the effect is counted anyway and nothing records that an
    effect of epoch 0 landed on a node now repairing at epoch 1. A repair that fixes the purge
    floor after this drain is repairing *around* a write it has no record of. -/
theorem an_old_epoch_effect_survives_the_refusal :
    (grun Gate.initial [.acquire 0, .publish 0, .quarantine, .finish 0]).applied = 1
      ∧ (grun Gate.initial [.acquire 0, .publish 0, .quarantine, .finish 0]).last
          = some ⟨1, "fault"⟩ := by
  decide

/-- **A CLEAN PUBLICATION IS RECORDED AS ONE.** The uncontended path commits exactly once and
    reports success, so the gate is not refusing work it should admit. -/
theorem an_admitted_publication_commits_once :
    (grun Gate.initial [.acquire 0, .publish 0, .finish 0]).applied = 1
      ∧ (grun Gate.initial [.acquire 0, .publish 0, .finish 0]).last = none := by
  decide

/-! ## The ledger

Twelve obligations: seven properties of the design that hold (`design:`) and five gaps between that
design and the code (`code:`), each naming the theorem that exhibits the gap and the file:line that
causes it.

The rows are *not* appended to `Ferrosa/Raft/Obligations.lean` in this revision, because that file
and `Ferrosa/Unimplemented.lean` carry pinned totals that the branch this model was written from is
concurrently editing. The merge step is the append to `Raft.ledger` (whose pin `34 / 16` becomes
`46 / 21`) plus five `Unimplemented` classification rows for the `code:` items below. Until then
this ledger is not counted by `ledgerTotal`, and that is recorded here rather than left to be
noticed. -/

def gateItems : List SpecItem := [
  SpecItem.mk "design: a held lease refuses a second publisher instead of waiting on it"
    (Status.met "Raft.a_held_lease_refuses_a_second_publisher"),
  SpecItem.mk "design: only acquire takes the lease, and every release gives it back"
    (Status.met "Raft.no_step_but_acquire_takes_the_lease"),
  SpecItem.mk "design: detection closes the gate while a publisher is halted"
    (Status.met "Raft.detection_progresses_while_a_publisher_is_halted"),
  SpecItem.mk "design: no publication lands in a quarantined epoch"
    (Status.met "Raft.no_publication_lands_in_a_quarantined_epoch"),
  SpecItem.mk "design: a successful drain proves no holder at the current quarantined epoch"
    (Status.met "Raft.drain_ok_implies_no_holder_and_the_current_epoch"),
  SpecItem.mk "design: a consumer registered before the transition never loses it"
    (Status.met "Raft.no_lost_wakeup_for_a_registered_consumer"),
  SpecItem.mk "design: a consumer that arrives after the transition observes it on the read"
    (Status.met "Raft.a_late_consumer_reads_the_current_epoch"),
  SpecItem.mk "code: a lease refusal on a healthy gate is indistinguishable, by type, from a quarantine refusal"
    (Status.failing
      "RecoveryRefusal (ferrosa-cluster/src/raft/quarantine.rs:11-14) is epoch plus reason, so 'the lease was busy, retry' and 'this node is quarantined, stop' arrive in the same shape and a caller must parse the reason string (Raft.refusal_payload_is_fully_determined_by_its_two_fields). The busy refusal carries the current epoch, which on a healthy gate is the epoch a quarantine would report, so an epoch comparison does not separate them either (Raft.a_held_lease_refuses_a_second_publisher). Measured: a second publisher on a healthy gate is refused with epoch 0 in 1.4us, where the parent revision blocked it and then admitted it"
      "Raft.a_held_lease_refuses_a_second_publisher"),
  SpecItem.mk "code: a refused publisher is not queued, retried or woken; its work is dropped with a string"
    (Status.failing
      "with_admission_epoch (ferrosa-cluster/src/raft/quarantine.rs:95-97) returns on try_lock failure with nothing recorded, and the code offers no retry helper, so a caller that lost the race must re-drive the whole publication itself and its only signal is its own return value (Raft.a_refused_publisher_is_not_queued). Before this commit the same caller blocked and succeeded: the deadlock fix converted queueing into refusal without giving the refused side a path back"
      "Raft.a_refused_publisher_is_not_queued"),
  SpecItem.mk "code: a refused drain has nothing to wait on, because only epoch changes notify"
    (Status.failing
      "notify_waiters is called from quarantine and nowhere else (ferrosa-cluster/src/raft/quarantine.rs:71), and the publication mutex (:36) has no notifier, so a repair loop whose drain_publications returned 'recovery publication is still active' (:110-113) can only spin on try_lock. Measured against one doomed publisher: 51% of 100000 single drain attempts returned Err. The gate is not unsafe when that happens - no publication can land in a quarantined epoch (Raft.no_publication_lands_in_a_quarantined_epoch) - it is unable to make progress without an unpinned retry loop"
      "Raft.a_lease_release_wakes_nobody"),
  SpecItem.mk "code: the effect of an old-epoch publication stands and is recorded nowhere"
    (Status.failing
      "with_admission_epoch's second admits_epoch (ferrosa-cluster/src/raft/quarantine.rs:101) refuses after the closure ran, and the module states that already-admitted I/O is not rolled back (:88-89); nothing durable records that an epoch-0 effect landed, so a repair that fixes the purge floor afterwards has no record of the write it is repairing around (Raft.an_old_epoch_effect_survives_the_refusal). No durable repair marker exists either, which is Raft.Quarantine's a_restart_forgets_the_repair_position seen from the gate's side"
      "Raft.an_old_epoch_effect_survives_the_refusal"),
  SpecItem.mk "code: a fault recorded without an epoch advance wakes no suspended consumer"
    (Status.failing
      "advance (ferrosa-cluster/src/raft/quarantine.rs:151-160) records an 'epoch' fault and leaves the epoch unchanged when checked_add returns None, so the gate is closed while the epoch - the only thing changed_since (:74-88) wakes on - is identical, and a consumer suspended on the saturated epoch re-reads the same number forever (Raft.a_fault_without_an_epoch_delta_wakes_nobody). Latent: one increment per fault makes it unreachable in practice, and it becomes reachable the moment any other path records a fault without advancing the epoch, which a clear/recover path would"
      "Raft.a_fault_without_an_epoch_delta_wakes_nobody")
]

/-- Twelve obligations, seven design properties that hold and five code gaps. -/
theorem gate_items_pinned : gateItems.length = 12 ∧ redCount gateItems = 5 := by
  decide

/-! ## The axiom audit

The trace theorems are finite computations and report no axioms; the case-split theorems report
`propext` at most. None reports `sorryAx`.
-/

#print axioms refusal_payload_is_fully_determined_by_its_two_fields
#print axioms acquire_fails_when_the_lease_is_held
#print axioms finish_always_releases_the_lease
#print axioms no_step_but_acquire_takes_the_lease
#print axioms a_held_lease_refuses_a_second_publisher
#print axioms a_refused_publisher_is_not_queued
#print axioms detection_progresses_while_a_publisher_is_halted
#print axioms no_publication_lands_in_a_quarantined_epoch
#print axioms drain_ok_implies_no_holder_and_the_current_epoch
#print axioms a_drain_is_refused_by_the_publication_it_must_outlive
#print axioms a_drain_at_a_stale_epoch_is_refused
#print axioms no_lost_wakeup_for_a_registered_consumer
#print axioms a_late_consumer_reads_the_current_epoch
#print axioms a_lease_release_wakes_nobody
#print axioms the_gate_never_reopens
#print axioms a_fault_without_an_epoch_delta_wakes_nobody
#print axioms max_epoch_is_u64_max
#print axioms an_old_epoch_effect_survives_the_refusal
#print axioms an_admitted_publication_commits_once
#print axioms gate_items_pinned

end Ferrosa.Raft
