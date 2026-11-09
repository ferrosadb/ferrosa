/-
  Ferrosa formal specification — storage: the commit log's stall clock.
  ======================================================================

  A periodic commit log acknowledges a write before it is fsynced, so the window of
  acknowledged-but-not-durable writes has to be bounded. `SyncHealth`
  (`ferrosa-storage/src/commitlog/sync.rs`) bounds it with a stall clock: the instant the
  oldest unsynced write was registered. While that write is older than the stall deadline,
  `admit` refuses new durable writes with `CommitLogNotDurable`.

  A sync is accounted with a *ticket*: the highest write sequence registered when the
  attempt starts. When the flush succeeds, everything up to the ticket is durable; if no
  write arrived during the flush the clock is cleared, otherwise it restarts at the ticket's
  instant, which is no later than any write the flush did not cover.

  The bug this models. `CommitLog::force_sync` flushed the segment but never told the
  health record, so after a successful forced sync the data was durable and the clock still
  ran: writes kept being refused "past the stall deadline" until the sync thread's own next
  timer (`force_old_keeps_refusing`). The fix accounts a forced sync with the same ticket
  protocol as the thread (`force_new_is_a_sync`), so a completed sync of either kind clears
  the clock (`completed_sync_clears_the_clock`, `forced_sync_admits`), and a failed forced
  sync leaves it exactly as it was (`failed_force_leaves_the_clock`).

  A forced sync can race the thread's, so an older ticket may complete after a newer one.
  Applying it would move the clock back to an instant whose writes are already durable;
  the fix ignores a superseded ticket (`superseded_ticket_changes_nothing`).

  Scope. Time is a natural number of nanoseconds; the deadline is positive. The `dead` and
  `failing` states are not modelled: neither a forced sync nor this fix changes them.
-/

namespace Ferrosa.Storage.SyncHealth

/-! ## The health record and its events

The record keeps the sequence of the last registered write, the sequence known durable, and
the stall clock: the registration instant of the oldest unsynced write, or nothing when every
write is durable. A write sets the clock only when it is unset, so the oldest instant stands.
-/

/-- What the commit log knows about durability. -/
structure Health where
  written : Nat
  durable : Nat
  since : Option Nat
  deriving Repr, DecidableEq

/-- A write registered at `now`: the next sequence, and the clock if it was unset. -/
def write (h : Health) (now : Nat) : Health :=
  { h with written := h.written + 1, since := h.since.orElse (fun _ => some now) }

/-- A sync attempt's ticket: the sequence it covers and the instant it was read. -/
structure Ticket where
  target : Nat
  at_ : Nat
  deriving Repr, DecidableEq

/-- The ticket a sync attempt reads before it flushes. -/
def begin (h : Health) (now : Nat) : Ticket := ⟨h.written, now⟩

/-- A flush covering `t` succeeded. A ticket older than what is already durable changes
nothing; otherwise everything up to the ticket is durable, and the clock is cleared when no
write arrived since the ticket, or restarted at the ticket's instant when one did. -/
def syncOk (h : Health) (t : Ticket) : Health :=
  if t.target < h.durable then h
  else
    { h with
      durable := t.target,
      since := if h.written = t.target then none else some t.at_ }

/-- The sync thread's fsync, accounted with its ticket. -/
def threadSync (h : Health) (t : Ticket) : Health := syncOk h t

/-- The shipped `force_sync`: the data was flushed, the record was not touched. -/
def forceOld (h : Health) : Health := h

/-- The fixed `force_sync` when its flush succeeds: the thread's own accounting. -/
def forceNewOk (h : Health) (t : Ticket) : Health := syncOk h t

/-- The fixed `force_sync` when its flush fails: nothing became durable. -/
def forceNewFailed (h : Health) : Health := h

/-- `admit` refuses: an unsynced write has waited at least `deadline`. -/
def refuses (h : Health) (now deadline : Nat) : Prop :=
  ∃ t, h.since = some t ∧ deadline ≤ now - t

/-- `refuses`, as a computation: the clock is set and the deadline has passed. -/
def refusesB (h : Health) (now deadline : Nat) : Bool :=
  match h.since with
  | none => false
  | some t => decide (deadline ≤ now - t)

/-- The computation decides exactly the refusal predicate, so `decide` can evaluate a trace. -/
theorem refusesB_iff (h : Health) (now deadline : Nat) :
    refusesB h now deadline = true ↔ refuses h now deadline := by
  unfold refusesB refuses
  cases hs : h.since with
  | none => simp
  | some t => simp

instance (h : Health) (now deadline : Nat) : Decidable (refuses h now deadline) :=
  decidable_of_iff _ (refusesB_iff h now deadline)

/-! ## The bug: a forced sync left the clock running

One write is registered at time 0 and forced to disk at time 10; with a 5 ns deadline the
shipped accounting still refuses at time 10, although the write is durable.
-/

/-- The stalled state: one write at time 0, nothing synced. -/
def stalled : Health := write ⟨0, 0, none⟩ 0

/-- After the shipped forced sync the record still refuses, though the data is on disk. -/
theorem force_old_keeps_refusing : refuses (forceOld stalled) 10 5 := by
  decide

/-! ## The fix: a completed sync of either kind clears the clock

A forced sync is accounted exactly as the thread's fsync, so whatever is proved about one is
proved about the other. When no write lands during the flush, the clock is cleared and no
deadline refuses.
-/

/-- The fixed forced sync is the thread's sync. -/
theorem force_new_is_a_sync (h : Health) (t : Ticket) : forceNewOk h t = threadSync h t := rfl

/-- A completed sync whose ticket was read with nothing written since, and which is not
superseded, clears the stall clock. -/
theorem completed_sync_clears_the_clock (h : Health) (now : Nat) (hd : h.durable ≤ h.written) :
    (syncOk h (begin h now)).since = none := by
  simp [syncOk, begin, Nat.not_lt.mpr hd]

/-- The same for the thread's fsync. -/
theorem thread_sync_clears_the_clock (h : Health) (now : Nat) (hd : h.durable ≤ h.written) :
    (threadSync h (begin h now)).since = none :=
  completed_sync_clears_the_clock h now hd

/-- And for the fixed forced sync: after it, no deadline refuses. -/
theorem forced_sync_admits (h : Health) (now later deadline : Nat) (hd : h.durable ≤ h.written) :
    ¬ refuses (forceNewOk h (begin h now)) later deadline := by
  intro ⟨t, ht, _⟩
  rw [force_new_is_a_sync, thread_sync_clears_the_clock h now hd] at ht
  cases ht

/-- On the trace of the bug, the fixed forced sync admits. -/
theorem force_new_admits_the_stalled_write : ¬ refuses (forceNewOk stalled (begin stalled 10)) 10 5 :=
  forced_sync_admits stalled 10 10 5 (by decide)

/-! ## A failed forced sync, and a superseded ticket

A forced sync whose flush fails leaves the record unchanged: the writes it covered keep
ageing toward the deadline and are refused once they pass it. A ticket older than what is
already durable changes nothing, so a sync that loses a race cannot move the clock back.
-/

/-- A failed forced sync leaves the clock exactly as it was. -/
theorem failed_force_leaves_the_clock (h : Health) : (forceNewFailed h).since = h.since := rfl

/-- So a state that refuses still refuses after a failed forced sync. -/
theorem failed_force_still_refuses (h : Health) (now deadline : Nat) :
    refuses h now deadline → refuses (forceNewFailed h) now deadline := id

/-- An older ticket completing after a newer one changes nothing. -/
theorem superseded_ticket_changes_nothing (h : Health) (t : Ticket) (older : t.target < h.durable) :
    syncOk h t = h := by
  simp [syncOk, older]

/-- The race, concretely: the thread reads a ticket, a write lands, a forced sync covers
both, and the thread's older ticket then completes; the clock stays cleared. -/
theorem a_lost_race_keeps_the_clock_cleared :
    let h0 := write ⟨0, 0, none⟩ 0
    let old := begin h0 1
    let h1 := write h0 2
    let h2 := forceNewOk h1 (begin h1 3)
    (threadSync h2 old).since = none := by
  decide

end Ferrosa.Storage.SyncHealth
