/-
  Ferrosa formal specification — `LOCK TABLE` modes (found by the live Oban probe).
  ================================================================================

  Ecto takes `LOCK TABLE schema_migrations IN SHARE UPDATE EXCLUSIVE MODE` in the
  migration transaction and then inserts the version row from another connection.
  Ferrosa's first `LOCK` took writer ownership of the whole table in every mode, so
  that INSERT waited for the lock holder, which waited for the INSERT: every Ecto
  migration run deadlocked. In PostgreSQL the INSERT's ROW EXCLUSIVE does not
  conflict with SHARE UPDATE EXCLUSIVE.

  The implementation now: every mode takes one table-lock slot (so a `LOCK`
  conflicts with every other `LOCK` of the table), and only SHARE and stronger
  also take the table's writer stripes (so they, and only they, block writers).

  * `impl_blocks_writers_iff_pg` — a mode blocks plain DML exactly when
    PostgreSQL's matrix says it conflicts with ROW EXCLUSIVE.
  * `sue_admits_writers` — the deadlock is gone.
  * `impl_never_weaker` — between two `LOCK`s the implementation conflicts
    whenever PostgreSQL does (it may conflict more, never less), so mutual
    exclusion PostgreSQL guarantees still holds.
-/

namespace Ferrosa.Sql.TableLock

inductive Mode where
  | accessShare | rowShare | rowExclusive | shareUpdateExclusive
  | share | shareRowExclusive | exclusive | accessExclusive
  deriving DecidableEq, Repr

open Mode

/-- PostgreSQL's table-lock conflict matrix (documentation, "Table-Level Locks"). -/
def pgConflict : Mode → Mode → Bool
  | accessShare, b => b == accessExclusive
  | rowShare, b => b == exclusive || b == accessExclusive
  | rowExclusive, b => b == share || b == shareRowExclusive || b == exclusive || b == accessExclusive
  | shareUpdateExclusive, b =>
      b == shareUpdateExclusive || b == share || b == shareRowExclusive || b == exclusive ||
        b == accessExclusive
  | share, b =>
      b == rowExclusive || b == shareUpdateExclusive || b == shareRowExclusive || b == exclusive ||
        b == accessExclusive
  | shareRowExclusive, b => b != accessShare && b != rowShare
  | exclusive, b => b != accessShare
  | accessExclusive, _ => true

/-- Whether a mode takes the table's writer stripes (blocks plain DML). -/
def takesStripes : Mode → Bool
  | share | shareRowExclusive | exclusive | accessExclusive => true
  | _ => false

/-- Two `LOCK` statements: both take the table-lock slot, so they always conflict. -/
def implLockConflict (_ _ : Mode) : Bool := true

/-- Plain DML (ROW EXCLUSIVE) waits exactly for a holder that took the stripes. -/
def implBlocksWriters (m : Mode) : Bool := takesStripes m

/-- A `LOCK TABLE` mode blocks plain DML in Ferrosa exactly when PostgreSQL's matrix says it
    conflicts with ROW EXCLUSIVE, for all eight modes. -/
theorem impl_blocks_writers_iff_pg (m : Mode) :
    implBlocksWriters m = pgConflict m rowExclusive := by
  cases m <;> rfl

/-- SHARE UPDATE EXCLUSIVE, the mode Ecto's migrator takes, does not block writers, so the
    version INSERT from the second connection proceeds. -/
theorem sue_admits_writers : implBlocksWriters shareUpdateExclusive = false := rfl

/-- Between two `LOCK TABLE` statements the implementation conflicts whenever PostgreSQL does.
    It may conflict more often, never less, so every mutual exclusion PostgreSQL guarantees
    between table locks still holds. -/
theorem impl_never_weaker (a b : Mode) (_h : pgConflict a b = true) :
    implLockConflict a b = true := rfl

/-- The matrix is symmetric, as PostgreSQL documents it. -/
theorem pg_symmetric (a b : Mode) : pgConflict a b = pgConflict b a := by
  cases a <;> cases b <;> rfl

/-! ## The deadlock itself, as a wait-for graph

  A deadlock is a cycle in the wait-for graph: `a` waits for `b` when `a` needs
  something `b` holds. Two kinds of edge exist:

  * **registry edges** — waits inside ferrosa's writer-ownership registry, which
    refuses a request whose wait would close a cycle (`40P01`);
  * **application edges** — waits the database cannot see. Ecto's migrator holds
    the lock on connection 1 and waits, in Elixir, for connection 2's version
    INSERT to finish. No detector inside the database can see that edge.

  So the registry's detection alone cannot rule out the Ecto hang: a cycle that
  runs through an application edge is invisible to it. The fix must keep the
  registry from adding the edge back, and these theorems show it does.
-/

abbrev Txn := Nat
abbrev Graph := List (Txn × Txn)

/-- `Path g a b`: a non-empty chain of wait edges from `a` to `b`. -/
inductive Path (g : Graph) : Txn → Txn → Prop where
  | edge {a b} : (a, b) ∈ g → Path g a b
  | step {a b c} : (a, b) ∈ g → Path g b c → Path g a c

def Acyclic (g : Graph) : Prop := ∀ t, ¬ Path g t t

/-- Reflexive-transitive reachability. -/
def Reach (g : Graph) (a b : Txn) : Prop := a = b ∨ Path g a b

/-- Wait-for paths compose: a path from `a` to `b` followed by a path from `b` to `c` is a path
    from `a` to `c`. -/
theorem Path.trans {g : Graph} {a b c : Txn} (h1 : Path g a b) (h2 : Path g b c) : Path g a c := by
  induction h1 with
  | edge h => exact Path.step h h2
  | step h _ ih => exact Path.step h (ih h2)

/-- A wait-for path in a graph is still a path in any graph that contains all of its edges:
    adding waits never removes a cycle. -/
theorem Path.mono {g g' : Graph} (hsub : ∀ e, e ∈ g → e ∈ g') {a b : Txn} (h : Path g a b) :
    Path g' a b := by
  induction h with
  | edge h => exact Path.edge (hsub _ h)
  | step h _ ih => exact Path.step (hsub _ h) ih

/-- A path in `g` with the edge `(a, b)` added either avoids it, or passes through
    it and so yields reachability `x ⇝ a` and `b ⇝ y` in the old graph. -/
theorem path_add_edge {g : Graph} {a b x y : Txn} (h : Path ((a, b) :: g) x y) :
    Path g x y ∨ (Reach g x a ∧ Reach g b y) := by
  induction h with
  | @edge u v he =>
    rcases List.mem_cons.mp he with heq | hmem
    · cases heq; exact Or.inr ⟨Or.inl rfl, Or.inl rfl⟩
    · exact Or.inl (Path.edge hmem)
  | @step u v w he _ ih =>
    rcases List.mem_cons.mp he with heq | hmem
    · cases heq
      rcases ih with hp | ⟨_, hby⟩
      · exact Or.inr ⟨Or.inl rfl, Or.inr hp⟩
      · exact Or.inr ⟨Or.inl rfl, hby⟩
    · rcases ih with hp | ⟨hva, hby⟩
      · exact Or.inl (Path.step hmem hp)
      · refine Or.inr ⟨?_, hby⟩
        rcases hva with rfl | hp
        · exact Or.inr (Path.edge hmem)
        · exact Or.inr (Path.step hmem hp)

/-- **The registry's rule keeps it deadlock-free.** Adding the wait `a → b` only when
    `b` cannot already reach `a` (otherwise the request fails with `40P01`)
    preserves acyclicity. -/
theorem guarded_wait_preserves_acyclic (g : Graph) (a b : Txn) (hg : Acyclic g)
    (hguard : ¬ Reach g b a) : Acyclic ((a, b) :: g) := by
  intro t ht
  rcases path_add_edge ht with hp | ⟨hta, hbt⟩
  · exact hg t hp
  · apply hguard
    rcases hbt with rfl | hbt
    · exact hta
    · rcases hta with rfl | hta
      · exact Or.inr hbt
      · exact Or.inr (Path.trans hbt hta)

/-! ### Ecto's migration, transaction 1 = the migrator, 2 = its version INSERT

Ecto's migration as a wait-for graph. Transaction 1 is the migrator, which holds the table lock
and waits in the application for transaction 2, the version INSERT on another connection.
Whether transaction 2 waits for transaction 1 inside the database depends only on whether the
held mode blocks writers, so that one bit decides whether the schedule has a cycle.
-/

/-- The edges of Ecto's schedule. The migrator (1) waits, in the application, for
    the INSERT (2). The INSERT waits in the registry for the lock holder exactly
    when the held mode blocks writers. -/
def ectoGraph (blocks : Mode → Bool) (m : Mode) : Graph :=
  (1, 2) :: (if blocks m then [(2, 1)] else [])

/-- The first implementation: every mode took the writer stripes. -/
def oldBlocksWriters (_ : Mode) : Bool := true

/-- **The bug, reproduced:** under the old implementation Ecto's schedule has a cycle, a
    deadlock that runs through the application edge, so no detector saw it. -/
theorem old_ecto_deadlocks : ¬ Acyclic (ectoGraph oldBlocksWriters shareUpdateExclusive) := by
  intro h
  apply h 1
  have e12 : ((1 : Txn), (2 : Txn)) ∈ ectoGraph oldBlocksWriters shareUpdateExclusive := by
    simp [ectoGraph, oldBlocksWriters]
  have e21 : ((2 : Txn), (1 : Txn)) ∈ ectoGraph oldBlocksWriters shareUpdateExclusive := by
    simp [ectoGraph, oldBlocksWriters]
  exact Path.step e12 (Path.edge e21)

/-- In the graph whose only edge is the migrator waiting for the INSERT, nothing leads out of
    the INSERT, so no path starts there. -/
theorem path_from_two_absurd {a : Txn} : ¬ Path [(1, 2)] 2 a := by
  intro h
  cases h with
  | edge he => simp at he
  | step he _ => simp at he

/-- **The deadlock is gone:** under the implementation the same schedule is acyclic for
    SHARE UPDATE EXCLUSIVE, and for every mode PostgreSQL lets writers through. -/
theorem ecto_schedule_acyclic (m : Mode) (h : pgConflict m rowExclusive = false) :
    Acyclic (ectoGraph implBlocksWriters m) := by
  have hb : implBlocksWriters m = false := by rw [impl_blocks_writers_iff_pg]; exact h
  intro t ht
  simp only [ectoGraph, hb] at ht
  cases ht with
  | edge he =>
    rcases List.mem_cons.mp he with heq | hmem
    · cases heq
    · simp at hmem
  | step he hp =>
    simp at he
    obtain ⟨rfl, rfl⟩ := he
    exact path_from_two_absurd hp

/-- Ecto's migration schedule under SHARE UPDATE EXCLUSIVE has no cycle in the implementation:
    the deadlock every Ecto migration hit is gone. -/
theorem ecto_migration_no_deadlock :
    Acyclic (ectoGraph implBlocksWriters shareUpdateExclusive) :=
  ecto_schedule_acyclic shareUpdateExclusive rfl

/-- And where PostgreSQL itself blocks writers (SHARE and stronger), the schedule is a
    deadlock in PostgreSQL too: the implementation adds no deadlock PostgreSQL lacks. -/
theorem impl_deadlocks_only_where_pg_does (m : Mode) :
    (¬ Acyclic (ectoGraph implBlocksWriters m)) → pgConflict m rowExclusive = true := by
  intro hcyc
  cases hm : pgConflict m rowExclusive with
  | true => rfl
  | false => exact absurd (ecto_schedule_acyclic m hm) hcyc

end Ferrosa.Sql.TableLock
