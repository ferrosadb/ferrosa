/-
  The recovery admission gate: a lease, two notifies, and the waiters that outlive them
  ====================================================================================

  `Ferrosa/Raft/Quarantine.lean` models the *lifecycle* a quarantined node walks: phase, synced
  index, the five surfaces it must keep shut. It says nothing about the concurrency primitive that
  lifecycle stands on. `ferrosa-cluster/src/raft/quarantine.rs` is that primitive: an epoch, a fault
  map, a `parking_lot::Mutex<()>` lease meaning "a publication is in flight", and two
  `tokio::sync::Notify` channels.

  `cc8806fd` is the first commit that treats this primitive as an interface. It types the refusals
  (`Busy`, `Quarantined`, `EpochChanged`, `InvalidDrain`), adds a `PublicationLease` whose `Drop`
  unlocks before it wakes, adds `drain_publications_async` that awaits a release instead of
  refusing, and makes `changed_since` return on a terminal fault even when the epoch did not move.
  This module models the primitive and asks what those changes actually buy.

  Five fields are the whole state, and each is one thing the code holds:

  * `epoch`      -- `AdmissionState.epoch`.
  * `faults`     -- the fault-map keys. The terminal marker is not a caller's group: `advance`
                    inserts it when `checked_add` fails, so a caller can occupy the same key.
  * `holder`     -- `some t` iff task `t` holds the `publication` lease.
  * `registered` -- tasks that called `Notified::enable` and are suspended.
  * `notified`   -- the tasks the last `notify_waiters` woke.

  The distinction the module turns on: `drain_publications` is a *function* of the state that
  always answers, while `drain_publications_async` is the only path that suspends. Whether a gate
  refuses or waits is therefore a property of the primitive per entry point, and the two entry
  points differ.
-/

namespace Ferrosa.Admission

/-! ## The refusal kinds, and the state

Five fields, four kinds. `Kind` is the code's `RecoveryRefusalKind`; `Gate` is `RecoveryAdmission` with the two `Notify` sets made explicit. -/

/-- What a gate operation answers. `busy` is the retryable one; the other three are terminal until
    the state changes for a reason the caller cannot supply itself. -/
inductive Kind where
  | ok
  | busy
  | quarantined
  | epochChanged
  | invalidDrain
  deriving DecidableEq, Repr

/-- The gate. `holder` is the publication lease: at most one task, ever. -/
structure Gate where
  epoch : Nat
  faults : List String
  holder : Option Nat
  registered : List Nat
  notified : List Nat
  deriving DecidableEq, Repr

/-- A fresh gate: epoch 0, no faults, no lease, no waiters (`RecoveryAdmission::default`). -/
def Gate.initial : Gate := ⟨0, [], none, [], []⟩

/-- The terminal marker `advance` writes when the epoch cannot advance. Written `s!"epoch"` so this
    file needs no escaped quote; it is the same `&str` the code inserts. -/
def terminalGroup : String := s!"epoch"

def Gate.terminal (g : Gate) : Bool := g.faults.contains terminalGroup

/-! ## The three results, as the code computes them

Three results, and the difference between them is the module: `drainSync` and `changedSince` are total functions of the state, `drain_publications_async` is the only one that suspends. -/

/-- `Self::check` (quarantine.rs:206-222). Faults are tested **before** the epoch, so a quarantined
    gate answers `quarantined` even to a stale epoch. -/
def Gate.admitsEpoch (g : Gate) (e : Nat) : Kind :=
  if g.faults ≠ [] then .quarantined
  else if e ≠ g.epoch then .epochChanged
  else .ok

/-- `check_drain` (quarantine.rs:172-185). The epoch is tested **before** the fault map, the reverse
    of `admitsEpoch`; a stale drain is `epochChanged`, not `invalidDrain`. -/
def Gate.drainCheck (g : Gate) (e : Nat) : Kind :=
  if g.epoch ≠ e then .epochChanged
  else if g.faults = [] then .invalidDrain
  else .ok

/-- `drain_publications` (quarantine.rs:132-137): check, take the lease, check again. Total in the
    state: it never suspends, and a held lease is answered `busy`. -/
def Gate.drainSync (g : Gate) (e : Nat) : Kind :=
  if g.drainCheck e ≠ .ok then g.drainCheck e
  else if g.holder.isSome then .busy else .ok

/-- `changed_since` (quarantine.rs:97-114). It returns the **stored** epoch, and returns it when the
    epoch differs from `seen` *or* the terminal fault is present. `none` is the suspending branch. -/
def Gate.changedSince (g : Gate) (seen : Nat) : Option Nat :=
  if g.epoch ≠ seen ∨ g.terminal then some g.epoch else none

/-! ## The steps

Four steps: the two lease moves, the registration, and a fault. Nothing else touches the state, which is what makes the induction over `run` the whole reachability argument. -/

inductive Ev where
  | acquire (t : Nat)
  | release (t : Nat)
  | register (t : Nat)
  | fault (group : String)
  deriving DecidableEq, Repr

/-- `acquire` is `try_lock` succeeding; `release` is `PublicationLease::drop`, which unlocks and
    then notifies every registered waiter; `register` is `Notified::enable`, which happens *before*
    the caller checks anything; `fault` is `quarantine`, which takes the transition write lock and
    never the publication lease. -/
def Gate.step (g : Gate) : Ev → Gate
  | .acquire t => if g.holder.isNone then { g with holder := some t } else g
  | .release t =>
    if g.holder = some t then { g with holder := none, notified := g.registered, registered := [] }
    else g
  | .register t => if g.registered.contains t then g else { g with registered := g.registered ++ [t] }
  | .fault grp => if g.faults.contains grp then g else { g with faults := g.faults ++ [grp] }

def Gate.run (g : Gate) (tr : List Ev) : Gate := tr.foldl Gate.step g

/-! ## What `cc8806fd` buys, stated as theorems

The first four are the interface `cc8806fd` builds and they hold. They are stated over an arbitrary state, not over the initial one, because a gate is reached by steps no single caller sees. -/

/-- The lease is exclusive: a drain that answers `ok` was not concurrent with a publisher. This is
    the property `try_lock` exists for, and it is why the drain is a function rather than a
    suspension point: a held lease is a `busy` answer, not a wait. -/
theorem a_successful_drain_holds_the_lease_alone (g : Gate) (e : Nat)
    (h : g.drainSync e = .ok) : g.holder = none := by
  by_cases hc : g.drainCheck e = .ok
  · cases hh : g.holder with
    | none => rfl
    | some v =>
      have hb : g.drainSync e = .busy := by simp [Gate.drainSync, hc, hh]
      rw [hb] at h
      exact absurd h (by simp)
  · have hs : g.drainSync e = g.drainCheck e := by simp [Gate.drainSync, hc]
    rw [hs] at h
    exact absurd h hc

/-- A held lease on a current, quarantined epoch is answered `busy` -- the retryable refusal -- and
    not one of the terminal kinds. Before `cc8806fd` all four were one string, so a caller could not
    tell "try again" from "this node is inconsistent". -/
theorem a_busy_gate_refuses_retryably (g : Gate) (e : Nat) (t : Nat)
    (hh : g.holder = some t) (hd : g.drainCheck e = .ok) : g.drainSync e = .busy := by
  simp [Gate.drainSync, hd, hh]

/-- A release wakes every waiter that had registered before it. This is the fix `cc8806fd` lands for
    the reported shape: `PublicationLease::drop` unlocks first (`drop(self.guard.take())`) and only
    then calls `notify_waiters`, so a woken waiter cannot observe the lease still held by the
    dropper. -/
theorem a_release_wakes_every_registered_waiter (g : Gate) (t w : Nat)
    (hh : g.holder = some t) (hr : w ∈ g.registered) :
    w ∈ (g.step (.release t)).notified := by
  simp [Gate.step, hh]
  exact hr

/-- Register-before-check is what makes the wakeup unlosable: a waiter that enabled its `Notified`
    and then observed `busy` is in `registered`, so the very release it waits for notifies it.
    `notify_waiters` stores no permit, so this ordering is the whole guarantee. -/
theorem a_waiter_that_registers_before_checking_is_woken (g : Gate) (t w : Nat)
    (hh : g.holder = some t) :
    w ∈ ((g.step (.register w)).step (.release t)).notified := by
  have hhold : (g.step (.register w)).holder = some t := by
    simp only [Gate.step]
    split <;> exact hh
  have hmem : w ∈ (g.step (.register w)).registered := by
    simp only [Gate.step]
    split
    · next hc => simpa using hc
    · simp
  exact a_release_wakes_every_registered_waiter _ _ _ hhold hmem

/-! ## Three things the commit does not buy

A refusal kind that is wrong is worse than a string that is vague, because a caller branches on it. These three are where `cc8806fd`'s own interface is the thing at fault. -/

/-- **`changed_since` can return the number it was given.** On the terminal fault the epoch cannot
    advance, so the function returns `some seen`. A caller written as
    `loop { seen <- changed_since seen; ... }` -- the shape the doc comment invites -- spins without
    progress at 100% CPU, forever, because every call returns immediately with the same value.

    The theorem is the counterexample, not the complaint: the value returned is not a signal that
    anything moved. `cc8806fd` documents "callers must recheck admission after every wake"; the gate
    offers no way to distinguish "woke with a new epoch" from "woke without one" other than a
    second call to `admits`. -/
theorem changed_since_can_return_its_argument :
    ∃ g : Gate, ∃ seen : Nat, g.changedSince seen = some seen := by
  refine ⟨⟨7, [terminalGroup], none, [], []⟩, 7, ?_⟩
  simp [Gate.changedSince, Gate.terminal]

/-- The stronger form: the returning call is not evidence of progress, and the gate it returns from
    is closed. A loop whose only termination condition is "the epoch moved" does not terminate. -/
theorem a_returning_wake_is_not_progress :
    ∃ g : Gate, ∃ seen : Nat,
      g.changedSince seen = some seen ∧ g.admitsEpoch seen ≠ .ok := by
  refine ⟨⟨7, [terminalGroup], none, [], []⟩, 7, ?_, ?_⟩
  · simp [Gate.changedSince, Gate.terminal]
  · simp [Gate.admitsEpoch]

/-- **Only the holder frees the lease.** `drain_publications_async` suspends until
    `publication_released` fires, and that channel is fed only by `PublicationLease::drop` -- i.e.
    only by the task holding the lease. An awaiting drain that is itself the holder therefore has no
    step that can make it return: the synchronous sibling refuses `busy` in exactly this state, and
    the async sibling waits. `cc8806fd`'s doc comment on `drain_publications` still says "Busy
    publication is refused, never waited upon", which is now true of one entry point and false of
    the other, and nothing guards the async path against the holder awaiting itself. -/
theorem only_the_holder_frees_the_lease (g : Gate) (t u : Nat)
    (hg : g.holder = some t) (h : (g.step (.release u)).holder = none) : u = t := by
  by_cases hu : g.holder = some u
  · rw [hg] at hu
    exact (Option.some.inj hu).symm
  · simp [Gate.step, hu] at h
    rw [hg] at h
    cases h

/-- The stall, stated as the absence of progress: from a state where `t` holds the lease, the
    synchronous drain answers `busy`, never `ok`, so a task that suspends on `t`'s release and is
    itself `t` waits for a step only it can take. -/
theorem an_awaiting_holder_has_no_progress (g : Gate) (t : Nat) (hh : g.holder = some t)
    (e : Nat) : g.drainSync e ≠ .ok := by
  intro h
  have hnone := a_successful_drain_holds_the_lease_alone g e h
  rw [hh] at hnone
  cases hnone

/-- **A refused publication may already have run.** `with_admission_epoch` checks the epoch, takes
    the lease, checks again, runs `publish`, then checks a third time -- and the third check reads
    the state *after* the closure. A fault landing during the closure therefore refuses a
    publication whose effect is already durable, and `EpochChanged` is what a retrying caller sees.
    The commit's own note covers the I/O ("already-admitted I/O is not rolled back"); it does not
    cover the closure, and the refusal kind tells the caller to retry. -/
def Gate.publishThen (before after : Gate) (e : Nat) : Kind :=
  match before.admitsEpoch e with
  | .ok => match before.holder with
          | some _ => .busy
          | none => after.admitsEpoch e
  | k => k

/-- The refusal a caller can retry on is `Quarantined` here, not `EpochChanged`: `check` tests the fault map before the epoch, and a fault is what an epoch bump always leaves behind. The effect the closure produced is already durable. -/
theorem a_refused_publication_may_already_have_run :
    ∃ before after : Gate, ∃ e : Nat,
      before.admitsEpoch e = .ok ∧ after.admitsEpoch e = .quarantined ∧
      Gate.publishThen before after e = .quarantined := by
  refine ⟨⟨0, [], none, [], []⟩, ⟨1, [s!"projection"], none, [], []⟩, 0, ?_, ?_, ?_⟩
  · simp [Gate.admitsEpoch]
  · simp [Gate.admitsEpoch]
  · decide

/-! ## What the tree has, with file and line

Four rows are the discipline working and five are open. Each names the code the row is about, so a row cannot be relabelled without the reader being able to check it. -/

structure Site where
  what : String
  site : String
  open_ : Bool

def sites : List Site := [
  ⟨"`drain_publications` checks the epoch, takes the lease, and checks again, so a drain cannot straddle a publication",
   "ferrosa-cluster/src/raft/quarantine.rs:132-137", false⟩,
  ⟨"`PublicationLease::drop` unlocks before `notify_waiters`, so a woken waiter never sees the lease still held",
   "ferrosa-cluster/src/raft/quarantine.rs:21-28", false⟩,
  ⟨"`drain_publications_async` registers its `Notified` before checking, so the release it waits for cannot be lost",
   "ferrosa-cluster/src/raft/quarantine.rs:141-156", false⟩,
  ⟨"`quarantine` notifies `publication_released` as well as `epoch_changed`, so a drain blocked on a lease is woken to re-check and answer `epochChanged`",
   "ferrosa-cluster/src/raft/quarantine.rs:92", false⟩,
  ⟨"`changed_since` returns on the terminal fault with the epoch unchanged, so its return value is not a progress signal",
   "ferrosa-cluster/src/raft/quarantine.rs:105-113", true⟩,
  ⟨"`drain_publications_async` awaits its own lease's release with no self-holder guard, where the synchronous sibling refuses `busy`",
   "ferrosa-cluster/src/raft/quarantine.rs:141-156", true⟩,
  ⟨"the third `admits_epoch` runs after `publish`, so a fault during the closure refuses a publication that already ran",
   "ferrosa-cluster/src/raft/quarantine.rs:118-129", true⟩,
  ⟨"`check` reports the alphabetically first fault key, so the synthetic terminal entry can displace the caller's group reason in `reason()` and `Display`",
   "ferrosa-cluster/src/raft/quarantine.rs:207-211", true⟩,
  ⟨"no in-tree consumer calls `admits`, `admits_epoch`, `with_admission_epoch`, `changed_since` or either drain: `RecoveryAdmission` is constructed and stored, never consulted",
   "ferrosa-cluster/src/controller/mod.rs:205", true⟩
]

def openSites : List Site := sites.filter (·.open_)

/-- Nine rows, pinned, so a site cannot be dropped from the audit silently. -/
theorem site_count : sites.length = 9 := by decide

/-- Five of the nine are open at `cc8806fd`. -/
theorem open_site_count : openSites.length = 5 := by decide

/-- Every row the filter kept is an open one, so the count is not the only guard: the rows themselves cannot be mislabelled once `sites` is fixed. -/
theorem an_open_site_is_open : ∀ s ∈ openSites, s.open_ = true := by
  intro s hs
  simp [openSites, sites] at hs
  rcases hs with h | h | h | h | h <;> subst h <;> rfl

end Ferrosa.Admission
