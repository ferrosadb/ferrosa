/-
  Ferrosa formal specification — Accord, part 1: transactions, histories,
  serializability.
  ================================================================

  Sources:
    * `research/SPEC-accord-transactions.md` §3.2.1 (Timestamp),
      §4.4 (Execute: Read set evaluated against local storage),
     §4.5 (Apply: idempotent, dep-gated),
    * `ferrosa-jepsen/src/history.rs` (Op / OpResult / Operation),
    * `ferrosa-jepsen/elle-checker/src/ferrosa/elle_check.clj`
      (the claimed model is `:strict-serializable` over `list-append`).

  The correctness statement is the one Jepsen actually checks, made explicit:

    a history is *strictly serializable* when there is a total order of the
    committed transactions such that (a) replaying them in that order from the
    empty database reproduces every observed read result, and (b) the order
    respects real time — if A completed before B was invoked, A precedes B.

  A transaction is modelled as an ordered list of ops (`read k`, `append k v`)
  executed atomically against the keyspace, so "full transaction semantics"
  includes intra-transaction read-your-writes: an `append` earlier in the same
  transaction is visible to a later `read` in that transaction.
-/
namespace Ferrosa.Accord

abbrev Node := Nat
abbrev TxnId := Nat
abbrev Key := Nat
abbrev Val := Nat

/-! ## §3.2.1 Timestamps -/

/-- The four-field commit timestamp.  Field order is significant: it *is* the
    comparison order (`epoch`, then `time`, then `seq`, then `node`). -/
structure Ts where
  epoch : Nat
  time : Nat
  seq : Nat
  node : Node
  deriving DecidableEq, Repr

namespace Ts

/-- Lexicographic timestamp order, as §3.2.1's `Ord` derive defines it. -/
def lt (a b : Ts) : Prop :=
  a.epoch < b.epoch ∨
    (a.epoch = b.epoch ∧
      (a.time < b.time ∨
        (a.time = b.time ∧
          (a.seq < b.seq ∨ (a.seq = b.seq ∧ a.node < b.node)))))

/-- `bump_past`: the replica's counter-proposal in PreAccept/Accept (§3.2.1,
    §4.1).  Strictly greater than the timestamp it bumps past. -/
def bumpPast (other : Ts) (node : Node) : Ts :=
  { other with seq := other.seq + 1, node := node }

theorem bumpPast_gt_seq (other : Ts) (node : Node) : other.seq < (bumpPast other node).seq := by
  simp [bumpPast]

theorem bumpPast_gt (other : Ts) (node : Node) : lt other (bumpPast other node) := by
  refine Or.inr ⟨rfl, Or.inr ⟨rfl, Or.inl ?_⟩⟩
  simp [bumpPast]

/-- Two transactions proposed by *different* nodes in the same nanosecond still
    differ, because `node` is the last field of the order (§6.2). -/
theorem distinct_nodes_ordered (e t s n₁ n₂ : Nat) (h : n₁ ≠ n₂) :
    lt ⟨e, t, s, n₁⟩ ⟨e, t, s, n₂⟩ ∨ lt ⟨e, t, s, n₂⟩ ⟨e, t, s, n₁⟩ := by
  rcases Nat.lt_or_gt_of_ne h with hlt | hgt
  · exact Or.inl (Or.inr ⟨rfl, Or.inr ⟨rfl, Or.inr ⟨rfl, hlt⟩⟩⟩)
  · exact Or.inr (Or.inr ⟨rfl, Or.inr ⟨rfl, Or.inr ⟨rfl, hgt⟩⟩⟩)

end Ts

/-! ## Transactions and their ops -/

/-- One operation inside a transaction (the `list-append` op set of Elle's
    check, plus the intra-transaction order the real transaction has). -/
inductive Op where
  | read (k : Key)
  | append (k : Key) (v : Val)
  deriving DecidableEq, Repr

def Op.key : Op → Key
  | .read k => k
  | .append k _ => k

/-- A transaction: an ordered op list.  Read results are *not* stored here —
    they are what the history observed, and the checker compares against them. -/
structure Txn where
  id : TxnId
  ops : List Op
  deriving DecidableEq, Repr

def Txn.keys (t : Txn) : List Key := t.ops.map Op.key

def Txn.appends (t : Txn) : List (Key × Val) :=
  t.ops.filterMap (fun op => match op with | .append k v => some (k, v) | _ => none)

def Txn.readKeys (t : Txn) : List Key :=
  t.ops.filterMap (fun op => match op with | .read k => some k | _ => none)

/-- Two transactions conflict when they touch a common key. -/
def Txn.conflicts (a b : Txn) : Bool :=
  a.keys.any (fun k => b.keys.contains k)

/-! ## The keyspace -/

/-- A database: one append-list per key. -/
abbrev State := Key → List Val

def State.empty : State := fun _ => []

/-- Execute one op, returning the new state and (for a read) its result. -/
def State.step (s : State) : Op → State × Option (List Val)
  | .read k => (s, some (s k))
  | .append k v => (fun j => if j = k then s j ++ [v] else s j, none)

/-- Run a whole op list, keeping only the state. -/
def State.runOps (s : State) : List Op → State
  | [] => s
  | op :: rest => (s.step op).1.runOps rest

/-- Run a whole op list, keeping only the read results, in order. -/
def State.readsOf (s : State) : List Op → List (List Val)
  | [] => []
  | op :: rest =>
      match s.step op with
      | (s1, some r) => r :: s1.readsOf rest
      | (s1, none) => s1.readsOf rest

/-- The combined evaluator used by the protocol model. -/
def State.evalOps (s : State) (ops : List Op) : State × List (List Val) :=
  (s.runOps ops, s.readsOf ops)

/-! ### Composition lemmas

These carry the whole serializability argument: running `a` then `b` is running
`a ++ b`. -/
theorem State.runOps_append (s : State) (a b : List Op) :
    (s.runOps a).runOps b = s.runOps (a ++ b) := by
  induction a generalizing s with
  | nil => rfl
  | cons op rest ih => simp [State.runOps, ih]

/-- A read-only transaction cannot change the state. -/
theorem State.runOps_read_only (s : State) (ops : List Op)
    (h : ∀ op ∈ ops, ∃ k, op = .read k) : s.runOps ops = s := by
  induction ops generalizing s with
  | nil => rfl
  | cons op rest ih =>
      obtain ⟨k, hk⟩ := h op (by simp)
      subst hk
      simp [State.runOps, State.step, ih _ (fun o ho => h o (by simp [ho]))]

/-! ### Appends are what move the state -/

/-- Appending `v` to `k` makes `v` the last element of that key's list. -/
theorem State.runOps_append_visible (s : State) (ops : List Op) (k : Key) (v : Val)
    (h : ops = [.append k v]) : ((s.runOps ops) k).getLast? = some v := by
  subst h
  simp [State.runOps, State.step]

/-! ## Histories, in the shape the Jepsen driver records -/

inductive EvKind where
  | invoke
  | ok
  | info
  | fail
  deriving DecidableEq, Repr

/-- One event of a history.  `reads` is empty for `invoke`; for `ok`/`info` it is
    the observed result of each `read` op in `ops`, in order. -/
structure Ev where
  proc : Node
  kind : EvKind
  txn : TxnId
  ops : List Op
  reads : List (List Val)
  invokeUs : Nat
  completeUs : Nat
  deriving DecidableEq, Repr

structure History where
  evs : List Ev
  deriving DecidableEq, Repr

/-- A committed transaction, as the serial order sees it. -/
structure TxnRec where
  id : TxnId
  ops : List Op
  reads : List (List Val)
  invokeUs : Nat
  completeUs : Nat

def TxnRec.toTxn (r : TxnRec) : Txn := ⟨r.id, r.ops⟩

/-- The committed transactions of a history, first `ok` event per transaction
    winning.  `info` and `fail` transactions are *not* part of the serial order:
    an unknown outcome may or may not have executed, and an aborted one must not
    have written. -/
def History.committed (h : History) : List TxnRec :=
  -- accumulated in reverse (prepend, not append) and flipped once: on a
  -- 2400-transaction history the append-per-event version is quadratic in
  -- list copies and does not finish
  ((h.evs.filter (fun e => e.kind == .ok)).foldl
    (fun acc e =>
      if acc.any (fun r => r.id == e.txn) then acc
      else ⟨e.txn, e.ops, e.reads, e.invokeUs, e.completeUs⟩ :: acc)
    []).reverse

end Ferrosa.Accord
