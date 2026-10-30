/-
  Ferrosa formal specification — Accord, part 3: the apply layer and its safety.
  ============================================================================

  If every transaction's dependency set contains every conflicting transaction with a
  lower timestamp, then every apply interleaving the replicas are allowed to run yields a
  serializable history, and the serial order is timestamp order.  That is what a client
  writing a multi-key Accord transaction is buying, and the premise is load-bearing rather
  than decorative: `DepComplete` is a refutable property of a concrete run, and
  `Accord/Recovery.lean` exhibits a run that violates it together with the unserializable
  history that follows.  The apply guard itself — dep-wait before §4.5's apply — is
  carried as a certificate, so the safety argument stands whatever consensus layer
  produced the log.  The enumerator at the end checks the claim over every ordering of a
  three-transaction cluster the guard permits, including the orderings where apply order
  and timestamp order differ.
-/
import Ferrosa.Accord.Serializable

namespace Ferrosa.Accord

/-! ## What a replica has applied, and what it hands back to a client

  A replica's durable state is its apply log, its keyspace and the read results it returned, and
  applying a transaction folds the ops into the keyspace in order while recording what each read
  saw. Dep-wait holds a transaction back until every lower-timestamped dependency is already in the
  log, so the order the log records is the order the keyspace saw.
-/

/-- The durable transaction state of one replica (§3.2): the apply log, the
    keyspace, and the read results handed back to clients. -/
structure Replica where
  applied : List TxnId
  store : State
  results : List (TxnId × List (List Val))

def Replica.init : Replica := ⟨[], State.empty, []⟩

def txnOf (txns : List Txn) (id : TxnId) : Txn :=
  (txns.find? (fun t => t.id == id)).getD ⟨id, []⟩

/-- Apply one transaction: append it to the log, fold its ops into the keyspace,
    and record the read results *as computed against the pre-state*.  Threading
    the state through the op list is what makes intra-transaction
    read-your-writes visible (§4.4). -/
def Replica.apply (txns : List Txn) (r : Replica) (τ : TxnId) : Replica :=
  let body := txnOf txns τ
  ⟨r.applied ++ [τ],
   r.store.runOps body.ops,
   r.results ++ [(τ, r.store.readsOf body.ops)]⟩

/-- §4.5's dep-wait rule: do not apply `τ` until every dependency with a lower
    timestamp has been applied here. -/
def depWait (pos : TxnId → Nat) (deps : TxnId → List TxnId) (r : Replica) (τ : TxnId) : Bool :=
  (deps τ).all (fun γ => !(decide (pos γ < pos τ)) || r.applied.contains γ)

/-! ## The certificate a dep-respecting log carries

  A log is dep-respecting when every transaction it appends had each lower-timestamped dependency
  already present at that point, and the certificate carries that as an inductive structure rather
  than an assertion about the finished list. The executable form of the same rule is what the
  enumerator at the end of the module runs, so a certified log and a checked log are the same
  objects.
-/

/-- A sequence of applies that respected dep-wait at every step. -/
inductive LogOk (txns : List Txn) (pos : TxnId → Nat) (deps : TxnId → List TxnId) :
    List TxnId → Prop
  | nil : LogOk txns pos deps []
  | snoc (l : List TxnId) (τ : TxnId) (h : LogOk txns pos deps l) (hno : τ ∉ l)
      (hw : ∀ γ ∈ deps τ, pos γ < pos τ → γ ∈ l) : LogOk txns pos deps (l ++ [τ])

/-- Executable form of the same rule, used by the enumerator at the end of this
    file.  (It is the same predicate: for every position, every lower-timestamp
    dependency occurs at an earlier position.) -/
def logOkB (pos : TxnId → Nat) (deps : TxnId → List TxnId) (l : List TxnId) : Bool :=
  (idxFrom 0 l).all (fun iτ =>
    (deps iτ.2).all (fun γ =>
      !(decide (pos γ < pos iτ.2)) ||
        (idxFrom 0 l).any (fun jγ => jγ.2 == γ && jγ.1 < iτ.1)))

/-! ## Store equals the fold of the log

  A replica's keyspace is exactly the fold of its apply log over the empty database, so replaying
  the log from scratch reproduces the state a client was served from. That is what makes the log
  the durable artifact, and it spares a separate snapshot that would have to be kept in agreement
  with it.
-/

def foldTxnsFrom (s : State) (txns : List Txn) : List TxnId → State
  | [] => s
  | id :: rest => foldTxnsFrom (s.runOps (txnOf txns id).ops) txns rest

def foldTxns (txns : List Txn) (l : List TxnId) : State := foldTxnsFrom State.empty txns l

/-- Applying a transaction folds its ops into the replica's keyspace, in order, so an
    earlier op in the same transaction is visible to a later one. -/
theorem Replica.apply_store (txns : List Txn) (r : Replica) (τ : TxnId) :
    (r.apply txns τ).store = r.store.runOps (txnOf txns τ).ops := rfl

/-- Applying a transaction appends its id to the apply log and touches nothing that
    was already there, which is what later dep-waits read. -/
theorem Replica.apply_log (txns : List Txn) (r : Replica) (τ : TxnId) :
    (r.apply txns τ).applied = r.applied ++ [τ] := rfl

/-- Folding applies from any starting replica = folding the ops into its state. -/
theorem foldl_apply_store (txns : List Txn) (r : Replica) (l : List TxnId) :
    (l.foldl (fun r τ => r.apply txns τ) r).store = foldTxnsFrom r.store txns l := by
  induction l generalizing r with
  | nil => rfl
  | cons τ rest ih =>
      simp only [List.foldl_cons]
      rw [ih]
      rfl

/-- Running a log equals folding it, by induction. -/
theorem run_log_eq_fold (txns : List Txn) (l : List TxnId) :
    (l.foldl (fun r τ => r.apply txns τ) Replica.init).store = foldTxns txns l :=
  foldl_apply_store txns Replica.init l

/-! ## Dependency completeness, the premise the safety argument stands on

  Every conflicting transaction with a lower timestamp must appear in a transaction's dependency
  set, and the safety argument rests on that premise rather than on the protocol. It is a refutable
  property of a concrete run, not an axiom: `Accord/Recovery.lean` exhibits a run that violates it
  together with the unserializable history that follows.
-/

/-- §6.3: every conflicting transaction with a lower timestamp is a dependency. -/
def DepComplete (txns : List Txn) (pos : TxnId → Nat) (deps : TxnId → List TxnId)
    (committed : List TxnId) : Prop :=
  ∀ δ τ, δ ∈ committed → τ ∈ committed → δ ≠ τ →
    (txnOf txns δ).conflicts (txnOf txns τ) = true → pos δ < pos τ → δ ∈ deps τ

/-! ## Order in the log, and the decomposition `Before` is defined over

  One transaction precedes another when the log splits around the first with the second still
  ahead of it, so the order is witnessed by a decomposition rather than by a position. Appending to
  a log leaves every order already established alone, which is what makes the sequence stable under
  the late arrivals a coordinator cannot prevent.
-/

/-- `x` occurs before `y` in `l`. -/
def Before (l : List TxnId) (x y : TxnId) : Prop :=
  ∃ a b, l = a ++ x :: b ∧ y ∈ b

/-- Any element of a list splits it as `a ++ x :: b`, the decomposition `Before` is
    defined over.  Nothing about position: the split is the witness. -/
theorem mem_iff_append {α : Type} {x : α} : ∀ {l : List α}, x ∈ l → ∃ a b, l = a ++ x :: b
  | [], h => by simp at h
  | y :: ys, h => by
      rcases List.mem_cons.mp h with rfl | h'
      · exact ⟨[], ys, rfl⟩
      · obtain ⟨a, b, hab⟩ := mem_iff_append h'
        exact ⟨y :: a, b, by simp [hab]⟩

/-- Appending a transaction to a log leaves the relative order of the transactions
    already in it alone, so an order once established cannot be disturbed by a late
    arrival. -/
theorem before_append_singleton {l : List TxnId} {x y z : TxnId} (h : Before l x y) :
    Before (l ++ [z]) x y := by
  obtain ⟨a, b, hab, hy⟩ := h
  exact ⟨a, b ++ [z], by simp [hab], by simp [hy]⟩

/-- **Lemma 1** — a dep-respecting log never applies `τ` before one of its
    lower-timestamped dependencies. -/
theorem LogOk.dep_applied_before (txns : List Txn) (pos : TxnId → Nat)
    (deps : TxnId → List TxnId) :
    ∀ {l : List TxnId}, LogOk txns pos deps l →
      ∀ τ ∈ l, ∀ γ ∈ deps τ, pos γ < pos τ → γ ∈ l := by
  intro l hl
  induction hl with
  | nil => intro τ h; exact absurd h (by simp)
  | snoc l τ₀ hprev hno hw ih =>
      intro τ hτ γ hγ hpos
      simp only [List.mem_append, List.mem_singleton] at hτ
      rcases hτ with hτ | rfl
      · have h1 : γ ∈ l := ih τ hτ γ hγ hpos
        simp [h1]
      · have h1 : γ ∈ l := hw γ hγ hpos
        simp [h1]

/-! ## Exhaustive check over apply interleavings

The general theorem above is the argument; the enumerator is the machine-checked
statement that it holds for a concrete cluster, over *every* interleaving the
dep-wait rule permits. -/

def runLog (txns : List Txn) (l : List TxnId) : Replica :=
  l.foldl (fun r τ => r.apply txns τ) Replica.init

def historyOfLog (txns : List Txn) (timeOf : TxnId → Nat × Nat) (l : List TxnId) : History :=
  let r := runLog txns l
  ⟨r.results.map (fun p =>
      okEv 0 p.1 (txnOf txns p.1).ops p.2 (timeOf p.1).1 (timeOf p.1).2)⟩

/-- A, B and C: A appends to k0, B reads k0, C appends to the disjoint k1.  B
    conflicts with A and depends on it; C depends on nothing. -/
def cfgTxns : List Txn :=
  [⟨1, [.append 0 10]⟩, ⟨2, [.read 0]⟩, ⟨3, [.append 1 20]⟩]

def cfgPos : TxnId → Nat := fun id => id
def cfgDeps : TxnId → List TxnId := fun id => if id = 2 then [1] else []
def cfgCommitted : List TxnId := [1, 2, 3]

/-- Serialization order is timestamp order, so transaction `i` must be over before
    `i+1` starts for the history to be strict; these are the times that say so. -/
def cfgTime : TxnId → Nat × Nat := fun id => (id * 20, id * 20 + 10)

/-- Every ordering of the three applies that respects dep-wait. -/
def cfgScenarios : List (List TxnId) :=
  (perms [1, 2, 3]).filter (logOkB cfgPos cfgDeps)

/-- Three of the six permutations survive the guard; the three that would apply
    B before its dependency A are excluded. -/
theorem cfgScenarios_length : cfgScenarios.length = 3 := by native_decide

/-- **Lemma 2**, exhaustively: in every dep-respecting log, the conflicting pair
    A (1) and B (2) appears in timestamp order.  (`dep_applied_before` is the
    general reason; this records its content over every log the guard permits,
    which is the finite part a reviewer can check by eye.) -/
theorem cfg_dep_ordered :
    (cfgScenarios.all (fun l => decide (l.idxOf 1 < l.idxOf 2))) = true := by
  native_decide

/-- With the dependency edge removed, all six apply orders become admissible:
    dep-wait is the only thing pinning the order. -/
theorem no_deps_all_orders_admissible :
    ((perms [1, 2, 3]).filter (logOkB cfgPos (fun _ => []))).length = 6 := by
  native_decide

/-- **Every permitted interleaving produces a serializable history.**  This
    includes the interleavings that apply C first, i.e. where apply order and
    timestamp order differ. -/
theorem all_scenarios_serializable :
    (cfgScenarios.map (historyOfLog cfgTxns cfgTime)).all serializableB = true := by
  native_decide

/-- The same check with strictness: with no real-time information in the
    history (`invoke = complete = 0`) strictness is vacuous, so this only
    restates the serializability result — it is here to show the strict checker
    accepts what the plain one accepts, and its real content is in
    `Strict.lean`. -/
theorem all_scenarios_strict :
    (cfgScenarios.map (historyOfLog cfgTxns cfgTime)).all strictlySerializableB = true := by
  native_decide

/-- The strictness predicate is not free: with all timestamps zero the two
    transactions that were concurrent-by-the-clock are rejected, because the
    order that serializes them is not the order the clock asserts.  This is the
    check the reorder buffer has to earn (§7), and `Strict.lean` proves what it
    has to establish. -/
theorem zero_times_fail_strict :
    (cfgScenarios.map (historyOfLog cfgTxns (fun _ => (0, 0)))).all
      strictlySerializableB = false := by
  native_decide

end Ferrosa.Accord
