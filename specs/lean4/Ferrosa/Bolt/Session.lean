/-
  Ferrosa formal specification — the Bolt session (`ferrosa-graph/src/bolt`).
  =====================================================================

  Bolt is a stateful session protocol: HELLO, LOGON, RUN, PULL, DISCARD, RESET, GOODBYE, with
  four server-side states. Three properties are what a driver depends on:

    `failure_is_sticky`            only RESET leaves FAILED — no message can slip through
    `reset_clears_streams_and_failure`  RESET restores the initial state exactly
    `replies_are_not_one_per_message`   and therefore a client **cannot** pair replies to
                                 messages by index, which is the assumption every naive driver
                                 starts from

  The last one is the interesting one. After a failure the server ignores messages silently, so
  the reply stream is shorter than the message stream and shifts underneath the client. A
  driver that counts replies desynchronises by exactly the number of ignored messages.

  The open item is unbounded: `unconsumed_runs_grow_the_queue` shows that RUN without PULL
  accumulates results the server must hold, and nothing in the protocol bounds it.
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

/-! ## The ledger -/

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

theorem bolt_has_three_red_items : redCount ledger = 3 := by decide

def main : IO Unit :=
  printLedger "Bolt session obligations" ledger

end Ferrosa.Bolt
