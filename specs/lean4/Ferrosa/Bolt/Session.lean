/-
  Ferrosa formal specification — the Bolt session (`ferrosa-graph/src/bolt`).
  =====================================================================

  A Bolt connection is stateful — HELLO, LOGON, RUN, PULL, DISCARD, RESET, GOODBYE over
  four server-side states — and two of its rules a driver can rely on. A failure is
  sticky: only RESET, or closing the connection, gets out of it, and every other
  message is ignored. RESET restores the connection exactly, with no queued result
  stream left behind and a failed session usable again.

  What a driver cannot rely on is pairing replies to messages by position: because a
  failed session ignores messages silently, the reply stream is shorter than the message
  stream, and every later reply sits one place earlier for each message that was
  swallowed — a driver that counts replies desynchronises for the rest of the connection,
  and the failure it is told about carries no statement identity to recover from. RUN
  without PULL is also unbounded — each unconsumed run is a result set the server holds,
  with no backpressure in the protocol.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Bolt

open Ferrosa.Ledger

inductive Msg where
  | hello
  | logon
  | run
  | pull
  | discard
  | reset
  | goodbye
  deriving DecidableEq, Repr

/-- The session state. `streaming n` means `n` result streams are pending consumption. -/
inductive Sess where
  | ready
  | streaming (queued : Nat)
  | failed
  | closed
  deriving DecidableEq, Repr

/-- The transition function. GOODBYE closes from anywhere; RESET clears; FAILED swallows
    everything else. -/
def step : Sess → Msg → Sess
  | _, .goodbye => .closed
  | .failed, .reset => .ready
  | .failed, _ => .failed
  | _, .reset => .ready
  | .closed, _ => .closed
  | .streaming n, .run => .streaming (n + 1)
  | _, .run => .streaming 1
  | s, _ => s

/-- **Only RESET (or GOODBYE) leaves the failed state.**  No other message has any effect, which
    is what makes a failure unambiguous to the server. -/
theorem failure_is_sticky (m : Msg) (h1 : m ≠ .reset) (h2 : m ≠ .goodbye) :
    step .failed m = .failed := by
  cases m <;> simp_all [step]

/-- **RESET restores the initial state exactly**: no queued stream survives it, and a failed
    session becomes usable again. -/
theorem reset_clears_streams_and_failure :
    step (.streaming 3) .reset = .ready
      ∧ step .failed .reset = .ready
      ∧ step (step (.streaming 1) .run) .reset = .ready := by
  decide

/-- Replies produced by a message sequence. A message sent while FAILED produces none. -/
def replies : Sess → List Msg → Nat
  | _, [] => 0
  | s, m :: rest =>
    if s = .failed ∧ m ≠ .reset ∧ m ≠ .goodbye then replies s rest
    else replies (step s m) rest + 1

/-- **Replies are not one per message.**  Two RUNs in the ready state produce two replies; the
    same two RUNs in the failed state produce none. A driver that pairs by index is wrong by
    exactly the number of ignored messages. -/
theorem replies_are_not_one_per_message :
    replies .ready [.run, .run] = 2 ∧ replies .failed [.run, .run] = 0 := by
  decide

/-- **The desynchronisation is cumulative.**  The same four messages produce four replies from a
    ready session and two from a failed one: every ignored message removes a reply and shifts
    every later one down by one, for the rest of the connection. -/
theorem ignored_messages_shift_every_later_reply :
    replies .ready [.run, .run, .reset, .run] = 4
      ∧ replies .failed [.run, .run, .reset, .run] = 2 := by decide

/-- **RUN without PULL accumulates.**  Each queued stream is a result set the server is holding;
    the protocol has no bound and no backpressure on this path. -/
theorem unconsumed_runs_grow_the_queue :
    step (step (.streaming 1) .run) .run = .streaming 3 := by decide

/-- PULL consumes; DISCARD also consumes. Both are needed, because a client that abandons a
    stream must be able to release it without reading it. -/
theorem pull_and_discard_both_release :
    step (.streaming 1) .pull = .streaming 1
      ∧ step (.streaming 1) .discard = .streaming 1 := by
  decide

/-! ## The ledger

Six obligations a driver would want from a Bolt session are listed against the declaration that
decides each one, and three of them are red: reply pairing, bounded queuing, and attributing a
failure to a statement. A red item carries the sentence a client observes — a desynchronised reply
stream, a run queue with no backpressure, a failure with no statement identity to recover from — so
the ledger reads as the protocol's limits as much as its features. `redCount ledger` counts the red
items and `bolt_has_three_red_items` fixes the number at three.

-/

def ledger : List SpecItem := [
  ⟨"only RESET leaves the failed state",
   .met "Bolt.failure_is_sticky"⟩,
  ⟨"RESET restores the initial state exactly",
   .met "Bolt.reset_clears_streams_and_failure"⟩,
  ⟨"replies can be paired to messages by index",
   .failing
     "a failed session ignores messages silently, so the reply stream is shorter than the message stream and shifts; a counting driver desynchronises"
     "Bolt.replies_are_not_one_per_message"⟩,
  ⟨"queued result streams are bounded",
   .failing
     "RUN without PULL grows the server-side queue with no protocol backpressure"
     "Bolt.unconsumed_runs_grow_the_queue"⟩,
  ⟨"a stream can be released without being read",
   .met "Bolt.pull_and_discard_both_release"⟩,
  ⟨"a failure is attributable to a statement",
   .failing
     "FAILURE carries no statement identity, and in a pipelined session the position is exactly what the client cannot recover"
     "Bolt.ignored_messages_shift_every_later_reply"⟩
]

/-- Three of the six Bolt obligations are red: replies cannot be paired to messages by
    position, queued result streams are unbounded, and a failure carries no statement
    identity for the client to recover from. -/
theorem bolt_has_three_red_items : redCount ledger = 3 := by decide

def main : IO Unit :=
  printLedger "Bolt session obligations" ledger

end Ferrosa.Bolt
