/-
  Ferrosa formal specification — transactional DDL on the PostgreSQL front end.
  ============================================================================

  Ecto runs every migration inside `BEGIN … COMMIT`, so a `CREATE TABLE`, an `ALTER TABLE`
  and a `CREATE INDEX` arrive inside a transaction block and must behave as PostgreSQL's
  transactional DDL does from the client's side: a statement later in the block sees the
  schema the earlier ones produced, `ROLLBACK` leaves the schema untouched, and `COMMIT`
  applies the whole batch, in order.

  Ferrosa replicates a schema change as one Raft entry per operation
  (`ferrosa_cluster::ddl_path::DdlPath`), so the batch is buffered on the session and
  replayed at `COMMIT`. A replay is a sequence of independent schema changes: it is
  atomic only when every step succeeds. When step `k` fails (the schema-change path is
  unavailable, or a concurrent session changed the schema the batch was planned
  against), steps `1 … k-1` have been applied and cannot be withdrawn. What this module
  pins is that the outcome is never a *silent* partial application: the report names
  exactly the prefix that reached the schema, the step that failed, and the steps that
  never ran — and the schema the client is left with is exactly that prefix applied.

  Source: `ferrosa-postgres/src/ddl_txn.rs` (`DdlBatch`, `commit_ddl_batch`).
-/

namespace Ferrosa.Sql.Ddl

variable {S Op : Type}

/-! ## The buffered view

A schema is any type `S`, and a DDL statement is a partial function on it: `apply o s` is the
schema after `o`, or `none` when `o` is refused against `s` (the table already exists, the column is
missing). A statement inside the block is planned against the *view*: the committed schema with the
statements buffered so far applied in order. A statement the view refuses is refused to the client and
never enters the buffer.

-/

/-- Apply a batch in order, failing as soon as one step is refused. -/
def applyAll (apply : Op → S → Option S) : List Op → S → Option S
  | [], s => some s
  | o :: os, s => (apply o s).bind (applyAll apply os)

/-- Applying a batch extended by one statement is applying the batch, then the statement. -/
theorem applyAll_append_one (apply : Op → S → Option S) (t : List Op) (o : Op) (s : S) :
    applyAll apply (t ++ [o]) s = (applyAll apply t s).bind (apply o) := by
  induction t generalizing s with
  | nil => simp [applyAll]
  | cons x xs ih =>
    simp only [List.cons_append, applyAll]
    cases apply x s with
    | none => rfl
    | some s' => simp [Option.bind, ih]

/-- Buffer one statement: it is accepted only when it applies on top of the current view, and then
    the buffer grows by appending it. -/
def buffer (apply : Op → S → Option S) (committed : S) (t : List Op) (o : Op) :
    Option (List Op) :=
  match applyAll apply t committed with
  | none => none
  | some v => (apply o v).map (fun _ => t ++ [o])

/-- **A statement inside the block sees the buffered schema.**  After a statement is buffered, the
    view the next statement is planned against is the previous view with that statement applied. -/
theorem buffered_statement_is_visible (apply : Op → S → Option S) (committed : S)
    (t t' : List Op) (o : Op) (v : S) (hview : applyAll apply t committed = some v)
    (hbuf : buffer apply committed t o = some t') :
    applyAll apply t' committed = apply o v := by
  cases happ : apply o v with
  | none => simp [buffer, hview, happ] at hbuf
  | some v' =>
    simp only [buffer, hview, happ, Option.map, Option.some.injEq] at hbuf
    subst hbuf
    rw [applyAll_append_one, hview]
    simp [Option.bind, happ]

/-- A buffered batch always has a view: buffering never admits a statement the view refuses. -/
theorem buffer_keeps_a_view (apply : Op → S → Option S) (committed : S) (t t' : List Op) (o : Op)
    (hbuf : buffer apply committed t o = some t') :
    ∃ v, applyAll apply t' committed = some v := by
  cases hview : applyAll apply t committed with
  | none => simp [buffer, hview] at hbuf
  | some v =>
    cases happ : apply o v with
    | none => simp [buffer, hview, happ] at hbuf
    | some v' =>
      refine ⟨v', ?_⟩
      rw [buffered_statement_is_visible apply committed t t' o v hview hbuf, happ]

/-! ## Rollback applies nothing

`ROLLBACK` (or a failed statement followed by `COMMIT`, which PostgreSQL treats as a rollback) drops
the buffer. The committed schema is the schema before `BEGIN`.

-/

/-- The schema after `ROLLBACK`. The buffer is discarded unread. -/
def rollback (committed : S) (_buffered : List Op) : S := committed

/-- **Rollback applies none of the batch.** -/
theorem rollback_applies_nothing (committed : S) (t : List Op) :
    rollback committed t = committed := rfl

/-! ## Commit replays the batch and reports exactly what it applied

At `COMMIT` the batch is replayed in order through the schema-change path, modelled by `exec`. The
replay either applies every step, or stops at the first failure and reports the prefix it applied, the
step that failed and the rest. `exec` may fail where the plan did not (the path is unavailable, or a
concurrent change invalidated the plan), which is why the result has a failure case at all.

-/

/-- What a `COMMIT` of a DDL batch reports. -/
inductive CommitResult (S Op : Type) where
  /-- Every step applied; the resulting schema. -/
  | committed (s : S)
  /-- `applied` reached the schema, `failed` was refused, `notApplied` never ran; `s` is the
      schema the client is left with. -/
  | failedAt (applied : List Op) (failed : Op) (notApplied : List Op) (s : S)
  deriving DecidableEq, Repr

/-- Replay a batch in order, stopping at the first refused step. -/
def commit (exec : Op → S → Option S) : List Op → S → CommitResult S Op
  | [], s => .committed s
  | o :: os, s =>
    match exec o s with
    | none => .failedAt [] o os s
    | some s' =>
      match commit exec os s' with
      | .committed s'' => .committed s''
      | .failedAt a f n st => .failedAt (o :: a) f n st

/-- **Commit applies the whole batch, in order.**  A commit that reports success left exactly the
    schema the batch applied in order produces. -/
theorem commit_success_is_the_whole_batch (exec : Op → S → Option S) (t : List Op) (s s' : S)
    (h : commit exec t s = .committed s') : applyAll exec t s = some s' := by
  induction t generalizing s with
  | nil => simp [commit] at h; simp [applyAll, h]
  | cons o os ih =>
    cases he : exec o s with
    | none => simp [commit, he] at h
    | some s1 =>
      cases hc : commit exec os s1 with
      | committed s2 =>
        simp only [commit, he, hc, CommitResult.committed.injEq] at h
        subst h
        simp [applyAll, he, Option.bind, ih s1 hc]
      | failedAt a f n st => simp [commit, he, hc] at h

/-- And conversely: when every step succeeds, the commit reports success with that schema. -/
theorem whole_batch_commits (exec : Op → S → Option S) (t : List Op) (s s' : S)
    (h : applyAll exec t s = some s') : commit exec t s = .committed s' := by
  induction t generalizing s with
  | nil => simp [applyAll] at h; simp [commit, h]
  | cons o os ih =>
    simp only [applyAll] at h
    cases he : exec o s with
    | none => rw [he] at h; simp at h
    | some s1 =>
      rw [he] at h
      simp only [Option.bind] at h
      simp [commit, he, ih s1 h]

/-- **When the plan still holds, commit reproduces the view.**  If the schema did not change since
    the statements were planned (`exec` agrees with the planner) then the commit reports success with
    exactly the schema the statements inside the block saw. -/
theorem commit_reproduces_the_view (apply : Op → S → Option S) (t : List Op) (committed v : S)
    (hview : applyAll apply t committed = some v) : commit apply t committed = .committed v :=
  whole_batch_commits apply t committed v hview

/-- **No silent partial application.**  A failed commit reports a split of the batch into the steps
    that were applied, the one that failed and the ones that never ran; the schema it reports is
    exactly the applied prefix on top of the committed schema, and the failed step really was
    refused there. -/
theorem failed_commit_reports_the_exact_prefix (exec : Op → S → Option S) (t : List Op)
    (s st : S) (a n : List Op) (f : Op) (h : commit exec t s = .failedAt a f n st) :
    t = a ++ f :: n ∧ applyAll exec a s = some st ∧ exec f st = none := by
  induction t generalizing s a with
  | nil => simp [commit] at h
  | cons o os ih =>
    cases he : exec o s with
    | none =>
      simp only [commit, he, CommitResult.failedAt.injEq] at h
      obtain ⟨ha, hf, hn, hs⟩ := h
      subst ha hf hn hs
      exact ⟨rfl, rfl, he⟩
    | some s1 =>
      cases hc : commit exec os s1 with
      | committed s2 => simp [commit, he, hc] at h
      | failedAt a' f' n' st' =>
        simp only [commit, he, hc, CommitResult.failedAt.injEq] at h
        obtain ⟨ha, hf, hn, hs⟩ := h
        subst ha hf hn hs
        obtain ⟨h1, h2, h3⟩ := ih s1 a' hc
        refine ⟨by simp [h1], ?_, h3⟩
        simp [applyAll, he, Option.bind, h2]

/-- A failure on the first step applies nothing: the reported schema is the committed one. -/
theorem failure_on_the_first_step_applies_nothing (exec : Op → S → Option S) (o : Op)
    (os : List Op) (s : S) (h : exec o s = none) :
    commit exec (o :: os) s = .failedAt [] o os s := by
  simp [commit, h]

/-! ## A concrete case: the third of five statements fails

Five statements, the third refused at replay. The report says two were applied, names the third, and
lists the last two as never run; the schema holds exactly the first two.

-/

/-- A toy schema: the list of applied statement numbers. Statement `3` is refused. -/
def toyExec (o : Nat) (s : List Nat) : Option (List Nat) :=
  if o = 3 then none else some (s ++ [o])

/-- **The third of five fails: two applied, reported as two.** -/
theorem third_of_five_fails_loudly :
    commit toyExec [1, 2, 3, 4, 5] [] = .failedAt [1, 2] 3 [4, 5] [1, 2] := by
  decide

end Ferrosa.Sql.Ddl
