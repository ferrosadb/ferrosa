/-
  Ferrosa formal specification — Jepsen's checker models, ported.
  ============================================================

  Source: `jepsen-io/jepsen`, `jepsen/test/jepsen/checker_test.clj`. That file tests the built-in
  models (`queue`, `total-queue`, `set`, `unique-ids`, `counter`, `set-full`) rather than a storage
  system, so every assertion in it is a statement about *checker semantics* — exactly what a machine
  check can hold us to.

  What is ported here is the semantics and the verdicts, not the Clojure:

  * A history is a list of operations, each with an outcome and a real-time interval.
  * The checker searches for a serial order that respects real time and replays to the observed
    results. Three outcome kinds are load-bearing:
      - `ok v`     — took effect, and the observation must match;
      - `fail`/`info` — *may or may not* have taken effect, with an unconstrained observation;
      - `pending`  — never completed, so it may take effect anywhere after its invocation.
    Collapsing `fail`/`info`/`pending` into "no effect" is the classic porting mistake and it makes
    the checker unsound in the permissive direction; the cases below pin the distinction.
  * Each `deftest` assertion becomes a theorem. `native_decide` is used only over these finite
    histories.

  Not ported, and listed in the ledger: the `set` model's multi-value reads and `set-full`,
  `unique-ids`, the generators, the nemeses, and the stats/plot assertions (which are about reporting,
  not about correctness).
-/
import Ferrosa.Core
import Ferrosa.Ledger
import Ferrosa.Accord.Serializable

namespace Ferrosa.Testing.Models

open Ferrosa Ferrosa.Ledger Ferrosa.Accord

/-! ## Histories -/

/-- What the client observed. -/
inductive Outcome where
  /-- The operation completed and returned this value. -/
  | ok (v : Nat)
  /-- The operation is known to have failed — it may or may not have taken effect. -/
  | fail
  /-- The operation's fate is unknown — it may or may not have taken effect. -/
  | info
  /-- The operation never completed. -/
  | pending
  deriving DecidableEq, Repr

/-- One operation, with its real-time interval. `res = 0` means it never completed. -/
structure Op (α : Type) where
  op : α
  outcome : Outcome
  inv : Nat
  res : Nat
  deriving DecidableEq, Repr

def completedBefore {α : Type} (a b : Op α) : Bool :=
  a.res != 0 && a.res < b.inv

/-- Real time: nothing later in the order may have completed before something earlier was invoked. -/
def rtChain {α : Type} : List (Op α) → Bool
  | [] => true
  | o :: rest => rest.all (fun b => !completedBefore b o) && rtChain rest

/-- Replay a candidate order, exploring every branch an indeterminate operation allows. -/
def runAll {α σ : Type} (step : σ → α → Outcome → List (σ × Bool)) (s : σ) :
    List (Op α) → Bool
  | [] => true
  | o :: rest => (step s o.op o.outcome).any (fun p => p.2 && runAll step p.1 rest)

/-- **Linearizable**: some order of the operations respects real time and replays to every observation.
    This is Jepsen's `valid?`. -/
def linearizableB {α σ : Type} (step : σ → α → Outcome → List (σ × Bool)) (init : σ)
    (h : List (Op α)) : Bool :=
  (perms h).any (fun ord => rtChain ord && runAll step init ord)

/-! ## The counter model -/

inductive CtrOp where
  | add (n : Nat)
  | read
  deriving DecidableEq, Repr

/-- A counter: `add` increments, `read` returns the current value. -/
def ctrStep (s : Nat) : CtrOp → Outcome → List (Nat × Bool)
  | .add n, .ok _ => [(s + n, true)]
  | .add n, .fail => [(s, true), (s + n, true)]
  | .add n, .info => [(s, true), (s + n, true)]
  | .add n, .pending => [(s, true), (s + n, true)]
  | .read, .ok v => [(s, s == v)]
  | .read, .fail => [(s, true)]
  | .read, .info => [(s, true)]
  | .read, .pending => [(s, true)]

def counterLinearizable (h : List (Op CtrOp)) : Bool := linearizableB ctrStep 0 h

def cRead (p v i : Nat) : Op CtrOp := ⟨.read, .ok v, i, i + 1⟩
def cAdd (p n : Nat) (oc : Outcome) (i : Nat) : Op CtrOp := ⟨.add n, oc, i, i + 1⟩

/-- Empty history. -/
theorem counter_empty_is_valid : counterLinearizable [] = true := by native_decide

/-- `counter-test` "initial read": a fresh counter reads 0. -/
theorem counter_initial_read_is_zero : counterLinearizable [cRead 0 0 1] = true := by native_decide

/-- `counter-test` "initial read" negative: the same history reading 1 is not linearizable. -/
theorem counter_cannot_start_at_one : counterLinearizable [cRead 0 1 1] = false := by native_decide

/-- `counter-test` "ignore failed ops": a failed `add` need not have taken effect, so a later read of 0
    is legal. Treating `fail` as "took effect" would reject this history. -/
theorem a_failed_add_need_not_have_taken_effect :
    counterLinearizable [cAdd 0 1 .fail 1, cRead 0 0 2] = true := by native_decide

/-- `counter-test` "incomplete history": an unfinished `add 1` may take effect between the two reads,
    0 then 1. Treating `pending` as "no effect" would reject this history. -/
theorem an_incomplete_add_may_take_effect_between_reads :
    counterLinearizable
      [cAdd 0 1 .pending 1, cRead 1 0 2, cRead 1 1 3] = true := by native_decide

/-- And it need not: the same two reads are legal at 0 and 0. -/
theorem an_incomplete_add_need_not_take_effect :
    counterLinearizable
      [cAdd 0 1 .pending 1, cRead 1 0 2, cRead 1 0 3] = true := by native_decide

/-- An acknowledged `add` must be visible to a read that starts after it completes. -/
theorem an_acknowledged_add_is_visible_to_a_later_read :
    counterLinearizable [cAdd 0 2 (.ok 0) 1, cRead 1 2 3] = true
      ∧ counterLinearizable [cAdd 0 2 (.ok 0) 1, cRead 1 0 3] = false := by native_decide

/-! ## The unordered queue model -/

inductive QOp where
  | enqueue (v : Nat)
  | dequeue (v : Nat)
  deriving DecidableEq, Repr

/-- An unordered queue: an enqueue appends, a dequeue may take any value present. -/
def queueStep (q : List Nat) : QOp → Outcome → List (List Nat × Bool)
  | .enqueue v, .ok _ => [(q ++ [v], true)]
  | .enqueue v, .fail => [(q, true), (q ++ [v], true)]
  | .enqueue v, .info => [(q, true), (q ++ [v], true)]
  | .enqueue v, .pending => [(q, true), (q ++ [v], true)]
  | .dequeue v, .ok _ => if q.contains v then [(q.erase v, true)] else []
  | .dequeue _, .fail => [(q, true)]
  | .dequeue _, .info => [(q, true)]
  | .dequeue _, .pending => [(q, true)]

def queueLinearizable (h : List (Op QOp)) : Bool := linearizableB queueStep [] h

def qInv (o : QOp) (i : Nat) : Op QOp := ⟨o, .pending, i, 0⟩
def qOk (o : QOp) (i : Nat) : Op QOp := ⟨o, .ok 0, i, i + 1⟩

/-- `queue-test` "empty". -/
theorem queue_empty_is_valid : queueLinearizable [] = true := by native_decide

/-- `queue-test` "Possible enqueue but no dequeue". -/
theorem a_possible_enqueue_alone_is_valid :
    queueLinearizable [qInv (.enqueue 1) 1] = true := by native_decide

/-- `queue-test` "Definite enqueue but no dequeue". -/
theorem a_definite_enqueue_alone_is_valid :
    queueLinearizable [qOk (.enqueue 1) 1] = true := by native_decide

/-- `queue-test` "concurrent enqueue/dequeue": a dequeue can return a value enqueued concurrently. -/
theorem a_dequeue_may_observe_a_concurrent_enqueue :
    queueLinearizable
      [⟨.dequeue 1, .pending, 1, 0⟩, ⟨.enqueue 1, .pending, 2, 0⟩,
       ⟨.dequeue 1, .ok 1, 3, 4⟩] = true := by native_decide

/-- `queue-test` "dequeue but no enqueue": a dequeue of a value never enqueued is invalid. -/
theorem a_dequeue_without_an_enqueue_is_invalid :
    queueLinearizable [⟨.dequeue 1, .ok 1, 1, 2⟩] = false := by native_decide

/-- A value can be dequeued at most as many times as it was enqueued. -/
theorem a_queue_cannot_hand_out_more_than_it_was_given :
    queueLinearizable
      [qOk (.enqueue 1) 1, ⟨.dequeue 1, .ok 1, 2, 3⟩, ⟨.dequeue 1, .ok 1, 4, 5⟩] = false := by native_decide

/-! ## The total (FIFO) queue model -/

/-- A total queue: an enqueue appends, a dequeue must take the head. -/
def totalQueueStep (q : List Nat) : QOp → Outcome → List (List Nat × Bool)
  | .enqueue v, .ok _ => [(q ++ [v], true)]
  | .enqueue v, .fail => [(q, true), (q ++ [v], true)]
  | .enqueue v, .info => [(q, true), (q ++ [v], true)]
  | .enqueue v, .pending => [(q, true), (q ++ [v], true)]
  | .dequeue v, .ok _ =>
      match q with
      | [] => []
      | x :: rest => if x == v then [(rest, true)] else []
  | .dequeue _, .fail => [(q, true)]
  | .dequeue _, .info => [(q, true)]
  | .dequeue _, .pending => [(q, true)]

def totalQueueLinearizable (h : List (Op QOp)) : Bool := linearizableB totalQueueStep [] h

/-- `total-queue-test` "empty". -/
theorem total_queue_empty_is_valid : totalQueueLinearizable [] = true := by native_decide

/-- `total-queue-test` "sane": enqueue 1 and 2, dequeue 1 then 2. -/
theorem total_queue_dequeues_in_fifo_order :
    totalQueueLinearizable
      [qInv (.enqueue 1) 1, qInv (.enqueue 2) 2, qOk (.enqueue 2) 3,
       ⟨.dequeue 1, .ok 1, 4, 5⟩, ⟨.dequeue 2, .ok 2, 6, 7⟩] = true := by native_decide

/-- The same history with the dequeue order reversed is not FIFO. -/
theorem total_queue_rejects_out_of_order_dequeues :
    totalQueueLinearizable
      [⟨.enqueue 1, .ok 0, 1, 2⟩, ⟨.enqueue 2, .ok 0, 3, 4⟩,
       ⟨.dequeue 2, .ok 2, 5, 6⟩, ⟨.dequeue 1, .ok 1, 7, 8⟩] = false := by native_decide

/-- The same history is legal when the enqueues overlap, because then either enqueue order is
    possible and the FIFO model can fit the observations either way. This is why the *generator*, not
    just the model, decides what a history can prove. -/
theorem overlapping_enqueues_leave_the_fifo_order_free :
    totalQueueLinearizable
      [⟨.enqueue 1, .ok 0, 1, 3⟩, ⟨.enqueue 2, .ok 0, 2, 4⟩,
       ⟨.dequeue 2, .ok 2, 5, 6⟩, ⟨.dequeue 1, .ok 1, 7, 8⟩] = true := by native_decide

/-- **The two queue models disagree**, which is why the choice of model is part of the test: the same
    history is legal for an unordered queue and illegal for a total one. -/
theorem the_queue_models_differ_on_the_same_history :
    queueLinearizable [qOk (.enqueue 1) 1, qOk (.enqueue 2) 3, ⟨.dequeue 2, .ok 2, 5, 6⟩] = true
      ∧ totalQueueLinearizable
          [qOk (.enqueue 1) 1, qOk (.enqueue 2) 3, ⟨.dequeue 2, .ok 2, 5, 6⟩] = false := by native_decide

def ledger : List SpecItem := [
  ⟨"a failed operation need not have taken effect",
   .met "Models.a_failed_add_need_not_have_taken_effect"⟩,
  ⟨"an incomplete operation may take effect between two later reads",
   .met "Models.an_incomplete_add_may_take_effect_between_reads"⟩,
  ⟨"and it need not, so both readings of the same history are legal",
   .met "Models.an_incomplete_add_need_not_take_effect"⟩,
  ⟨"an acknowledged write is visible to a read that starts after it completes",
   .met "Models.an_acknowledged_add_is_visible_to_a_later_read"⟩,
  ⟨"a value the model cannot produce is rejected",
   .met "Models.counter_cannot_start_at_one"⟩,
  ⟨"a dequeue with no matching enqueue is rejected",
   .met "Models.a_dequeue_without_an_enqueue_is_invalid"⟩,
  ⟨"a queue cannot hand out more copies than it was given",
   .met "Models.a_queue_cannot_hand_out_more_than_it_was_given"⟩,
  ⟨"the total queue rejects out-of-order dequeues",
   .met "Models.total_queue_rejects_out_of_order_dequeues"⟩,
  ⟨"the ordered and unordered queue models disagree on the same history",
   .met "Models.the_queue_models_differ_on_the_same_history"⟩,
  ⟨"the set model is ported, including its multi-value reads",
   .failing
     "not ported: a set read observes a whole set, so the observation type is not a single Nat. Modelling it needs a set-valued observation and a multiset comparison, which is a different checker shape from the register-shaped ones here"
     "Models.the_queue_models_differ_on_the_same_history"⟩,
  ⟨"the set-full and unique-ids models are ported",
   .failing
     "not ported: set-full counts recovered/acknowledged/ok attempts, and unique-ids checks that every requested id is present; both need the multiset bookkeeping in checker_test.clj, not just a verdict"
     "Models.the_queue_models_differ_on_the_same_history"⟩,
  ⟨"Jepsen's generators, nemeses and stats assertions are ported",
   .failing
     "out of scope by decision: generators produce histories, nemeses inject faults, and the stats/plot tests assert reporting. None of them are correctness predicates on a history, so porting them would not strengthen the checker"
     "Models.the_queue_models_differ_on_the_same_history"⟩
]

theorem models_has_three_red_items : redCount ledger = 3 := by decide

def main : IO Unit :=
  printLedger "Jepsen checker models (ported from checker_test.clj)" ledger

end Ferrosa.Testing.Models
