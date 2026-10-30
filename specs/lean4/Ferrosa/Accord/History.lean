/-
  Ferrosa formal specification — Accord, part 1: transactions, histories,
  serializability.
  ==========================================================================

  A history is what a client saw, in the shape the Jepsen driver records it, and whether
  it is serializable is decided against the transactions it names rather than against the
  order the events arrived in.  A transaction is an ordered list of `read` and `append` ops
  executed atomically against a per-key append list, so intra-transaction read-your-writes
  fall out of the model instead of needing a separate rule.  Two fields carry more weight
  than they look: the four-field commit timestamp's field order *is* its comparison order,
  which is what breaks ties between two nodes proposing in the same nanosecond; and the
  committed set of a history takes the first `:ok` event per transaction, because an
  `:info` transaction may or may not have executed and a `:fail` one must not have written.
-/
namespace Ferrosa.Accord

abbrev Node := Nat
abbrev TxnId := Nat
abbrev Key := Nat
abbrev Val := Nat

/-! ## Timestamps, and the order their fields are compared in -/

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

/-- A counter-proposal's `seq` field is strictly larger than the one it bumps past, so
    the replica cannot re-propose the number it has already rejected. -/
theorem bumpPast_gt_seq (other : Ts) (node : Node) : other.seq < (bumpPast other node).seq := by
  simp [bumpPast]

/-- The bumped timestamp is strictly later than the one it bumps past, which is what
    makes a counter-proposal supersede the proposal it saw rather than tie with it. -/
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

/-! ## Transactions, the ops inside them, and when two of them conflict -/

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

/-! ## The keyspace an op list is executed against -/

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

/-! ### Composition lemmas, which carry the serializability argument

Running `a` then `b` is running `a ++ b`, and that is the step the whole argument takes
when it treats a transaction's ops as one block. -/

/-- Running op list `a` and then op list `b` leaves the same keyspace as running the
    concatenation `a ++ b` — the composition lemma the serializability argument rests
    on, since it is what lets a transaction's ops be replayed as one block. -/
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
