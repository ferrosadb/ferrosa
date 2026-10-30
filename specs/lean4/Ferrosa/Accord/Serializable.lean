/-
  Ferrosa formal specification — Accord, part 2: serializability, executable.
  ===========================================================================

  Serializability here is a search, not an opinion: a history is serializable when some
  total order of its committed transactions replays from the empty database and reproduces
  every read result the client observed, and strictly serializable when that order also
  respects real time.  Both predicates are decidable, so a history a run actually produced
  can be replayed and judged, and the five hand-written histories at the end of the module
  are the small cases that name what the checker accepts and what it rejects — a stale
  read, a phantom value, an transaction that failed to see its own append.  The search is
  factorial in the number of transactions, which suits checking a Jepsen history of tens of
  transactions; the protocol proofs never search, they exhibit the order.
-/
import Ferrosa.Accord.History

namespace Ferrosa.Accord

/-! ## Indexing a transaction list by position

  A candidate order is judged by position: which transaction came first, and whether the clock
  agrees with that ranking. Indexing the list once with its positions supplies the comparison the
  real-time condition needs, without re-scanning the order for each pair.
-/

def idxFrom : Nat → List TxnId → List (Nat × TxnId)
  | _, [] => []
  | i, x :: xs => (i, x) :: idxFrom (i + 1) xs

def findTxn (txns : List TxnRec) (id : TxnId) : Option TxnRec :=
  txns.find? (fun r => r.id == id)

/-! ## Replaying a candidate order, and rejecting a history that names a transaction it never ran

  A candidate order is replayed from the empty database and every transaction's observed read
  results must match what the replay produces; nothing weaker than exact reproduction counts. A
  history naming a transaction it never ran is judged not serializable rather than left undecided,
  so the checker answers for every input it is given.
-/

/-- Replay `order` from the empty database; every transaction's observed read
    results must match.  `false` on a dangling id, which keeps the checker
    total (a history that names a transaction it never ran is not serializable). -/
def orderOkFrom (txns : List TxnRec) (s : State) : List TxnId → Bool
  | [] => true
  | id :: rest =>
      match findTxn txns id with
      | none => false
      | some r =>
          let (s', rs) := s.evalOps r.ops
          (rs == r.reads) && orderOkFrom txns s' rest

def orderOk (txns : List TxnRec) (order : List TxnId) : Bool :=
  orderOkFrom txns State.empty order

/-- The serial order must use each committed transaction exactly once. -/
def coversAll (txns : List TxnRec) (order : List TxnId) : Bool :=
  (order.eraseDups.length == order.length) &&
    (txns.all (fun r => order.contains r.id) && order.all (fun id => (findTxn txns id).isSome))

def serializableBy (txns : List TxnRec) (order : List TxnId) : Bool :=
  coversAll txns order && orderOk txns order

/-! ## Real time, as the condition that separates strict from plain

  For a transaction placed earlier in the order, no later one may have completed before it was
  invoked, and that is the one condition separating strict serializability from serializability. A
  client sees the violation as a value that travels backwards in wall-clock time: an order exists,
  it is not the order the client lived through.
-/

/-- The strict-serializability side condition: for `a` earlier than `b` in the
    order, `b` must not have completed before `a` was invoked. -/
def realTimeOk (txns : List TxnRec) (order : List TxnId) : Bool :=
  (idxFrom 0 order).all (fun ib =>
    (idxFrom 0 order).all (fun ja =>
      if ja.1 < ib.1 then
        match findTxn txns ja.2, findTxn txns ib.2 with
        | some a, some b => !(Nat.ble b.completeUs a.invokeUs)
        | _, _ => true
      else true))

def strictlySerializableBy (txns : List TxnRec) (order : List TxnId) : Bool :=
  serializableBy txns order && realTimeOk txns order

/-! ## Search over orders (the Jepsen fallback)

  The search enumerates every permutation of the committed transactions and accepts the history if
  any one replays correctly, which is the Jepsen driver's own fallback rather than a proof
  strategy. It is factorial in the number of transactions, so it suits a history of tens of them;
  the protocol arguments elsewhere exhibit the order instead of searching for it.
-/

def inserts {α : Type} (x : α) : List α → List (List α)
  | [] => [[x]]
  | y :: ys => (x :: y :: ys) :: (inserts x ys).map (fun l => y :: l)

def perms {α : Type} : List α → List (List α)
  | [] => [[]]
  | x :: xs => (perms xs).flatMap (inserts x)

def serializableB (h : History) : Bool :=
  let txns := h.committed
  (perms (txns.map TxnRec.id)).any (fun order => serializableBy txns order)

def strictlySerializableB (h : History) : Bool :=
  let txns := h.committed
  (perms (txns.map TxnRec.id)).any (fun order => strictlySerializableBy txns order)

/-! ## The same statements as `Prop`s, for the proofs

  The Boolean checkers answer whether one given order works, and these two predicates say that some
  order does, which is the form the safety proofs consume. Strictness implies serializability
  through the same witness, and an empty history satisfies both.
-/

def Serializable (h : History) : Prop :=
  ∃ order, serializableBy h.committed order = true

def StrictlySerializable (h : History) : Prop :=
  ∃ order, strictlySerializableBy h.committed order = true

/-- **Strict implies serializable** — the order that witnesses strictness also
    witnesses serializability. -/
theorem strict_implies_serializable {h : History} (hs : StrictlySerializable h) :
    Serializable h := by
  obtain ⟨order, horder⟩ := hs
  refine ⟨order, ?_⟩
  unfold strictlySerializableBy at horder
  rw [Bool.and_eq_true] at horder
  exact horder.1

/-- An empty history is trivially both. -/
theorem empty_strictly_serializable : StrictlySerializable ⟨[]⟩ := by
  refine ⟨[], ?_⟩
  native_decide

/-! ## Hand-written histories

These are small enough to run, and each names the property it demonstrates.
`okEv` is the `:ok` event of a completed transaction; `invEv` an `:invoke`. -/

def okEv (p : Node) (id : TxnId) (ops : List Op) (reads : List (List Val))
    (inv comp : Nat) : Ev := ⟨p, .ok, id, ops, reads, inv, comp⟩

def invEv (p : Node) (id : TxnId) (ops : List Op) (inv : Nat) : Ev :=
  ⟨p, .invoke, id, ops, [], inv, 0⟩

/-- `write 7 to k0`, observed by a later read. -/
def hFreshRead : History := ⟨[
  invEv 0 1 [.append 0 7] 0,
  okEv 0 1 [.append 0 7] [] 0 10,
  invEv 1 2 [.read 0] 20,
  okEv 1 2 [.read 0] [[7]] 20 30]⟩

/-- The same write, but the read observed `[]` *after* the write completed:
    serializable (order the read first) but not strictly serializable. -/
def hStaleRead : History := ⟨[
  invEv 0 1 [.append 0 7] 0,
  okEv 0 1 [.append 0 7] [] 0 10,
  invEv 1 2 [.read 0] 20,
  okEv 1 2 [.read 0] [[]] 20 30]⟩

/-- A read of a value no transaction ever wrote: not serializable at all. -/
def hPhantom : History := ⟨[
  invEv 0 1 [.read 0] 0,
  okEv 0 1 [.read 0] [[9]] 0 10]⟩

/-- Intra-transaction read-your-writes: the append precedes the read *inside* the
    transaction, so the read must see it.  This is the "full transaction
    semantics" case that a per-op model gets wrong. -/
def hReadYourWrites : History := ⟨[
  invEv 0 1 [.append 0 1, .read 0] 0,
  okEv 0 1 [.append 0 1, .read 0] [[1]] 0 10]⟩

/-- ... and the same transaction observed *not* seeing its own append, which the
    atomic-transaction model rejects. -/
def hReadYourWritesBroken : History := ⟨[
  invEv 0 1 [.append 0 1, .read 0] 0,
  okEv 0 1 [.append 0 1, .read 0] [[]] 0 10]⟩

/-! ### Five small histories, each naming the property it demonstrates

  Each history is small enough to decide and is named for what it demonstrates: a fresh read, a
  stale read that is serializable but not strict, a value nothing ever wrote, and a transaction that
  once observed its own append and once did not. The red cases are the shapes a run takes when a
  guarantee is broken.
-/

/-- A write followed by a read that observes it is strictly serializable: the order
    that runs the write first reproduces the read and respects the clock. -/
theorem hFreshRead_strict : strictlySerializableB hFreshRead = true := by native_decide

/-- The same history in the `Prop` form, witnessed by the order that runs the write
    before the read. -/
theorem hFreshRead_serial : Serializable hFreshRead := ⟨[1, 2], by native_decide⟩

/-- **Serializable but not strictly serializable** — one theorem, two facts. -/
theorem hStaleRead_serial_but_not_strict :
    serializableB hStaleRead = true ∧ strictlySerializableB hStaleRead = false := by
  native_decide

/-- **Red: a read of a value nothing ever wrote is not serializable in any order.**
    The transaction returns `9` from a key no transaction appended to, so no replay
    from the empty database reproduces it — the shape a phantom or a lost write takes
    when it reaches a client. -/
theorem hPhantom_not_serializable : serializableB hPhantom = false := by native_decide

/-- A transaction whose read observes its own earlier append is strictly serializable:
    the checker applies the transaction's ops in order, so read-your-writes is part of
    atomicity rather than a separate guarantee. -/
theorem hReadYourWrites_ok : strictlySerializableB hReadYourWrites = true := by native_decide

/-- **Red: and the same transaction observed not seeing its own append is rejected.**
    An atomic transaction cannot read a state that excludes its own write, so the
    history has no serial order at all. -/
theorem hReadYourWritesBroken_rejected :
    serializableB hReadYourWritesBroken = false := by native_decide

end Ferrosa.Accord
