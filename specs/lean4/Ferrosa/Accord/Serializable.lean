/-
  Ferrosa formal specification — Accord, part 2: serializability, executable.
  ===========================================================================

  Two predicates, matching what Jepsen/Elle actually check:

  * `Serializable`      — some total order of the committed transactions replays
                          to exactly the observed read results;
  * `StrictlySerializable` — that order also respects real time.

  Both are decidable by search over permutations, so a history can be *run*
  through the definition with `#eval`.  `permutations` is factorial, so the
  executable form is intended for the sizes Jepsen histories are checked at
  (tens of transactions with a fixed order supplied, or a handful when
  searching); the protocol proofs never search — they exhibit the order.
-/
import Ferrosa.Accord.History

namespace Ferrosa.Accord

/-! ## Lists of transactions, indexed -/

def idxFrom : Nat → List TxnId → List (Nat × TxnId)
  | _, [] => []
  | i, x :: xs => (i, x) :: idxFrom (i + 1) xs

def findTxn (txns : List TxnRec) (id : TxnId) : Option TxnRec :=
  txns.find? (fun r => r.id == id)

/-! ## Replaying an order -/

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

/-! ## Real time -/

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

/-! ## Search over orders (the Jepsen fallback) -/

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

/-! ## The same statements as `Prop`s, for the proofs -/

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

/-! ### The demos -/

theorem hFreshRead_strict : strictlySerializableB hFreshRead = true := by native_decide
theorem hFreshRead_serial : Serializable hFreshRead := ⟨[1, 2], by native_decide⟩

/-- **Serializable but not strictly serializable** — one theorem, two facts. -/
theorem hStaleRead_serial_but_not_strict :
    serializableB hStaleRead = true ∧ strictlySerializableB hStaleRead = false := by
  native_decide

theorem hPhantom_not_serializable : serializableB hPhantom = false := by native_decide

theorem hReadYourWrites_ok : strictlySerializableB hReadYourWrites = true := by native_decide

theorem hReadYourWritesBroken_rejected :
    serializableB hReadYourWritesBroken = false := by native_decide

end Ferrosa.Accord
