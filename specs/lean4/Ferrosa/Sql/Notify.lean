/-
  Ferrosa formal specification — LISTEN / NOTIFY: at most once, only if committed, in commit order.
  =================================================================================================

  Decision D13 (`specs/todo/postgres-frontend/decisions.md`) makes `pg_notify` / `NOTIFY` real:
  a notification raised inside a transaction is queued with it, and delivered to listening
  sessions only when that transaction commits. PostgreSQL's own contract
  (`doc/src/sgml/ref/notify.sgml`) is the specification:

  * a rolled-back transaction notifies nobody;
  * notifications are delivered in the order their transactions committed;
  * within one transaction, a repeated `(channel, payload)` pair is delivered once.

  The implementation (`ferrosa-postgres/src/notify.rs`) holds the hub's publish lock across a
  notifying transaction's commit and its publication, exactly as PostgreSQL holds
  `NotifyQueueLock`, so a commit and the delivery of its batch are one step. This model takes
  that step as atomic and proves the three properties over every event sequence.

  What is not modelled: channel subscription is a filter over the delivered sequence (each
  listener sees the subsequence on its channels, which inherits all three properties), and
  delivery to sessions on other nodes (the front end refuses `LISTEN` in a cluster — see
  `ferrosa-postgres/README.md`).
-/
set_option linter.deprecated false

namespace Ferrosa.Sql.Notify

abbrev Txn := Nat

structure Note where
  channel : String
  payload : String
  deriving DecidableEq, Repr

inductive Event where
  /-- `pg_notify` / `NOTIFY` inside transaction `t`. -/
  | notify (t : Txn) (n : Note)
  | commit (t : Txn)
  | abort (t : Txn)

structure State where
  /-- Each open transaction's queued notifications, duplicates collapsed. -/
  pending : Txn → List Note
  /-- Transactions that have ended (committed or aborted). -/
  closed : List Txn
  /-- What the hub has delivered, in delivery order. -/
  delivered : List (Txn × Note)

def State.init : State := ⟨fun _ => [], [], []⟩

/-- PostgreSQL collapses a repeated `(channel, payload)` within one transaction. -/
def addNote (ns : List Note) (n : Note) : List Note := if n ∈ ns then ns else ns ++ [n]

def clear (p : Txn → List Note) (t : Txn) : Txn → List Note := fun u => if u = t then [] else p u

/-- The transaction's batch, tagged with its id. -/
def batch (s : State) (t : Txn) : List (Txn × Note) := (s.pending t).map (fun n => (t, n))

def step (s : State) : Event → State
  | .notify t n =>
      if t ∈ s.closed then s
      else { s with pending := fun u => if u = t then addNote (s.pending t) n else s.pending u }
  | .commit t =>
      if t ∈ s.closed then s
      else ⟨clear s.pending t, t :: s.closed, s.delivered ++ batch s t⟩
  | .abort t =>
      if t ∈ s.closed then s
      else ⟨clear s.pending t, t :: s.closed, s.delivered⟩

def run (es : List Event) : State := es.foldl step State.init

/-! ## Deliveries only come from commits

The invariant every reachable state keeps: queues hold no duplicates, deliveries hold no
duplicates, every delivery belongs to a closed transaction, and a closed transaction has nothing
pending. Its first consequence is at-most-once delivery.
-/

/-- The invariant every reachable state keeps. -/
structure Inv (s : State) : Prop where
  pending_nodup : ∀ t, (s.pending t).Nodup
  delivered_nodup : s.delivered.Nodup
  delivered_closed : ∀ p ∈ s.delivered, p.1 ∈ s.closed
  closed_empty : ∀ t ∈ s.closed, s.pending t = []

/-- Queuing a notification keeps a transaction's queue free of duplicates: a pair already queued
    is not added again. -/
theorem addNote_nodup (ns : List Note) (n : Note) (h : ns.Nodup) : (addNote ns n).Nodup := by
  unfold addNote
  split
  · exact h
  · rename_i hn
    rw [List.nodup_append]
    refine ⟨h, List.nodup_cons.mpr ⟨List.not_mem_nil, List.nodup_nil⟩, ?_⟩
    intro a ha b hb hab
    simp only [List.mem_singleton] at hb
    subst hb; subst hab
    exact hn ha

/-- A transaction's batch, tagged with its id, has no duplicates when its queue has none. -/
theorem batch_nodup (s : State) (t : Txn) (h : (s.pending t).Nodup) : (batch s t).Nodup := by
  unfold batch
  refine List.Pairwise.map _ ?_ h
  intro a b hab heq
  simp only [Prod.mk.injEq] at heq
  exact hab heq.2

/-- The empty starting state satisfies the invariant: nothing is pending, closed or delivered. -/
theorem init_inv : Inv State.init :=
  ⟨fun _ => List.nodup_nil, List.nodup_nil, fun _ h => by simp [State.init] at h,
    fun _ h => by simp [State.init] at h⟩

/-- Every event preserves the invariant. Queues stay duplicate-free, a commit appends only its
    own batch (which cannot collide with anything delivered before, since its transaction was
    still open), and every delivery belongs to a transaction that has ended. -/
theorem step_inv (s : State) (e : Event) (hs : Inv s) : Inv (step s e) := by
  cases e with
  | notify t n =>
    simp only [step]
    split
    · exact hs
    · rename_i hopen
      refine ⟨?_, hs.delivered_nodup, hs.delivered_closed, ?_⟩
      · intro u; dsimp only
        split
        · exact addNote_nodup _ _ (hs.pending_nodup t)
        · exact hs.pending_nodup u
      · intro u hu; dsimp only
        have : u ≠ t := fun e => hopen (e ▸ hu)
        simp only [this, ite_false]
        exact hs.closed_empty u hu
  | commit t =>
    simp only [step]
    split
    · exact hs
    · rename_i hopen
      refine ⟨?_, ?_, ?_, ?_⟩
      · intro u; dsimp only [clear]; split
        · exact List.nodup_nil
        · exact hs.pending_nodup u
      · rw [List.nodup_append]
        refine ⟨hs.delivered_nodup, batch_nodup s t (hs.pending_nodup t), ?_⟩
        intro a ha b hb hab
        have hac := hs.delivered_closed a ha
        unfold batch at hb
        rw [List.mem_map] at hb
        obtain ⟨n, _, hbn⟩ := hb
        subst hab
        rw [← hbn] at hac
        exact hopen hac
      · intro p hp
        rw [List.mem_append] at hp
        cases hp with
        | inl h => exact List.mem_cons_of_mem t (hs.delivered_closed p h)
        | inr h =>
          unfold batch at h
          rw [List.mem_map] at h
          obtain ⟨n, _, hpn⟩ := h
          rw [← hpn]
          exact List.mem_cons_self
      · intro u hu; dsimp only [clear] at hu ⊢
        split
        · rfl
        · rename_i hne
          rw [List.mem_cons] at hu
          cases hu with
          | inl h => exact absurd h hne
          | inr h => exact hs.closed_empty u h
  | abort t =>
    simp only [step]
    split
    · exact hs
    · refine ⟨?_, hs.delivered_nodup, ?_, ?_⟩
      · intro u; dsimp only [clear]; split
        · exact List.nodup_nil
        · exact hs.pending_nodup u
      · intro p hp
        exact List.mem_cons_of_mem t (hs.delivered_closed p hp)
      · intro u hu; dsimp only [clear] at hu ⊢
        split
        · rfl
        · rename_i hne
          rw [List.mem_cons] at hu
          cases hu with
          | inl h => exact absurd h hne
          | inr h => exact hs.closed_empty u h

/-- Folding any sequence of events over a state that satisfies the invariant yields a state that
    satisfies it. -/
theorem foldl_inv (es : List Event) (s : State) (hs : Inv s) : Inv (es.foldl step s) := by
  induction es generalizing s with
  | nil => exact hs
  | cons e rest ih => exact ih (step s e) (step_inv s e hs)

/-- Every state reachable from the start by any event sequence satisfies the invariant. -/
theorem run_inv (es : List Event) : Inv (run es) := foldl_inv es State.init init_inv

/-- **At most once.** No notification of any transaction is ever delivered twice. -/
theorem delivered_at_most_once (es : List Event) : (run es).delivered.Nodup :=
  (run_inv es).delivered_nodup

/-! ## Only committed transactions deliver

A delivery tagged `t` exists only if a `commit t` event occurred. In particular a transaction
that aborted (and never committed) delivers nothing. -/

/-- Starting from a state with nothing delivered, every notification delivered after a sequence
    of events is tagged with a transaction whose `commit` event is in that sequence. This is the
    general form behind `delivered_only_if_committed`. -/
theorem delivered_only_on_commit_foldl (es : List Event) (s : State)
    (hs : ∀ p ∈ s.delivered, False) :
    ∀ p ∈ (es.foldl step s).delivered, Event.commit p.1 ∈ es := by
  -- Strengthen: every delivery is either in the start state or from a commit in `es`.
  suffices h : ∀ (es : List Event) (s : State) p, p ∈ (es.foldl step s).delivered →
      p ∈ s.delivered ∨ Event.commit p.1 ∈ es by
    intro p hp
    cases h es s p hp with
    | inl h0 => exact (hs p h0).elim
    | inr h1 => exact h1
  intro es
  induction es with
  | nil => intro s p hp; exact Or.inl hp
  | cons e rest ih =>
    intro s p hp
    cases ih (step s e) p hp with
    | inr h => exact Or.inr (List.mem_cons_of_mem e h)
    | inl h =>
      cases e with
      | notify t n =>
        simp only [step] at h; split at h <;> exact Or.inl h
      | abort t =>
        simp only [step] at h; split at h <;> exact Or.inl h
      | commit t =>
        simp only [step] at h
        split at h
        · exact Or.inl h
        · rw [List.mem_append] at h
          cases h with
          | inl h => exact Or.inl h
          | inr h =>
            unfold batch at h
            rw [List.mem_map] at h
            obtain ⟨n, _, hpn⟩ := h
            subst hpn
            exact Or.inr List.mem_cons_self

/-- **Only if committed.** Every delivered notification belongs to a transaction that
    committed. -/
theorem delivered_only_if_committed (es : List Event) :
    ∀ p ∈ (run es).delivered, Event.commit p.1 ∈ es :=
  delivered_only_on_commit_foldl es State.init (fun _ h => by simp [State.init] at h)

/-- **A rolled-back transaction notifies nobody.** -/
theorem rolled_back_delivers_nothing (es : List Event) (t : Txn)
    (hnc : Event.commit t ∉ es) : ∀ p ∈ (run es).delivered, p.1 ≠ t := by
  intro p hp heq
  exact hnc (heq ▸ delivered_only_if_committed es p hp)

/-! ## Commit order

Delivery is append-only: whatever has been delivered stays a prefix of everything delivered
later, and a commit appends its whole batch at the end. So if `t₁` commits before `t₂`, every
notification of `t₁` precedes every notification of `t₂`. -/

/-- One event never withdraws or reorders a delivery: what was delivered before the event is a
    prefix of what is delivered after it. -/
theorem step_extends (s : State) (e : Event) : s.delivered <+: (step s e).delivered := by
  cases e with
  | notify t n => simp only [step]; split <;> exact List.prefix_refl _
  | abort t => simp only [step]; split <;> exact List.prefix_refl _
  | commit t => simp only [step]; split
                · exact List.prefix_refl _
                · exact List.prefix_append _ _

/-- Any sequence of events only extends the delivered sequence: what was delivered at the start
    stays a prefix of what is delivered at the end. -/
theorem foldl_extends (es : List Event) (s : State) : s.delivered <+: (es.foldl step s).delivered := by
  induction es generalizing s with
  | nil => exact List.prefix_refl _
  | cons e rest ih => exact List.IsPrefix.trans (step_extends s e) (ih (step s e))

/-- **Append-only.** Nothing delivered is ever withdrawn or reordered by later events. -/
theorem delivery_is_append_only (es more : List Event) :
    (run es).delivered <+: (run (es ++ more)).delivered := by
  unfold run
  rw [List.foldl_append]
  exact foldl_extends more _

/-- **A commit delivers its batch at the end.** Committing an open transaction appends exactly
    its queued notifications — after everything committed before it. -/
theorem commit_appends_its_batch (es : List Event) (t : Txn) (hopen : t ∉ (run es).closed) :
    (run (es ++ [.commit t])).delivered = (run es).delivered ++ batch (run es) t := by
  unfold run at hopen ⊢
  rw [List.foldl_append]
  simp only [List.foldl_cons, List.foldl_nil, step]
  rw [if_neg hopen]

/-- **Commit order.** If `t₁`'s commit comes first, everything `t₁` delivered precedes
    everything `t₂`'s later commit delivers. -/
theorem commit_order (es mid : List Event) (t₁ t₂ : Txn)
    (h₁ : t₁ ∉ (run es).closed) (h₂ : t₂ ∉ (run (es ++ [.commit t₁] ++ mid)).closed) :
    ∃ before,
      (run (es ++ [.commit t₁] ++ mid ++ [.commit t₂])).delivered =
        before ++ batch (run (es ++ [.commit t₁] ++ mid)) t₂ ∧
      (run es).delivered ++ batch (run es) t₁ <+: before := by
  refine ⟨(run (es ++ [.commit t₁] ++ mid)).delivered, ?_, ?_⟩
  · exact commit_appends_its_batch _ t₂ h₂
  · rw [← commit_appends_its_batch es t₁ h₁]
    exact delivery_is_append_only _ mid

/-! ## Duplicates within a transaction collapse

Within one transaction PostgreSQL delivers a repeated `(channel, payload)` pair once. The queue
collapses the repeat when it is raised, so the batch a commit delivers already holds each pair
once.
-/

/-- Queuing a `(channel, payload)` pair that the transaction has already queued leaves its queue
    unchanged, so the pair is delivered once, as PostgreSQL specifies. -/
theorem repeated_note_is_queued_once (ns : List Note) (n : Note) (h : n ∈ ns) :
    addNote ns n = ns := by
  unfold addNote; simp [h]

end Ferrosa.Sql.Notify
