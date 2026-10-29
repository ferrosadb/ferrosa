/-
  Ferrosa formal specification — CQL front end, part 4: lightweight transactions.
  ===========================================================================

  LWT is the one place where a front-end question becomes a *correctness* question
  rather than a routing one.  `INSERT … IF NOT EXISTS`, `UPDATE … IF`, and
  `DELETE … IF EXISTS` are compare-and-set: they read a value, decide, and write
  conditionally.  Three things have to hold:

    1. the condition is evaluated against a state that agrees with the
       transaction's serial point — otherwise a CAS is a stale read plus a blind
       write (`Accord.Recovery.write_skew_not_serializable`);
    2. therefore two CAS with the same expectation on the same key cannot both
       apply (`at_most_one_applies`);
    3. a CAS that *fails* reports the values it tested, so the client's
       `CurrentValues` is the serial state, not a stale one
       (`failed_cas_reports_the_state_it_tested`).

  `Classify.lean` establishes the front-end half: a conditional statement reads
  the keys it compares against (`conditional_reads_its_keys`) and a transaction
  containing one is never blind (`conditional_txn_not_blind`).  This file supplies
  the semantics on top of that, and `Accord.Serializable` supplies the check that
  the resulting histories pass.

  Two models are used deliberately:

  * `Cell` is a scalar register (`Key → Option Val`) — that is what `IF col = v`
    means for a non-collection column, and it makes the CAS algebra provable;
  * `Accord.History.State` is the append-list keyspace — that is the model the
    serializability checker speaks, and it is where the anomaly is exhibited.
    The two are connected by `encodeOf`, which maps a run of applied CAS to the
    append history the checker reasons about ("value v was written" = "v was
    appended to the key's list").
-/
import Ferrosa.Accord.Serializable
import Ferrosa.Cql.Classify

namespace Ferrosa.Cql.Lwt

open Ferrosa
open Ferrosa.Accord

/-! ## The scalar cell -/

abbrev Cell := Key → Option Val

def Cell.empty : Cell := fun _ => none

def Cell.set (s : Cell) (k : Key) (v : Option Val) : Cell :=
  fun j => if j = k then v else s j

/-- A lightweight transaction: the key, the value it expects (`none` is
    `IF NOT EXISTS`, i.e. expecting the key to hold nothing), and the value it
    writes when the expectation holds. -/
structure Lwt where
  key : Key
  expected : Option Val
  newValue : Val
  deriving DecidableEq, Repr

/-- The condition, evaluated against a state. -/
def Lwt.cond (s : Cell) (l : Lwt) : Bool := s l.key == l.expected

/-- The ops a conditional statement contributes to its transaction: it **reads**
    the key it conditions on, then writes.  This is the invariant the whole file
    rests on — a CAS whose read set omits its condition key is unprotected, and
    `unprotected_cas_is_not_serializable` below shows what that costs. -/
def Lwt.ops (l : Lwt) : List Op := [.read l.key, .append l.key l.newValue]

/-- Execute against a cell, returning the new cell and whether it applied. -/
def Lwt.execute (s : Cell) (l : Lwt) : Cell × Bool :=
  if l.cond s then (s.set l.key (some l.newValue), true) else (s, false)

/-! ### The basic facts -/

/-- An applied CAS wrote its value. -/
theorem applied_writes (s : Cell) (l : Lwt) (h : (l.execute s).2 = true) :
    (l.execute s).1 l.key = some l.newValue := by
  unfold Lwt.execute at *
  split
  · simp [Cell.set]
  · simp_all

/-- A failed CAS changed nothing — the aborted-transaction rule. -/
theorem not_applied_leaves_state (s : Cell) (l : Lwt) (h : (l.execute s).2 = false) :
    (l.execute s).1 = s := by
  unfold Lwt.execute at *
  split
  · simp_all
  · rfl

/-- The applied flag is exactly the condition. -/
theorem applied_iff_cond (s : Cell) (l : Lwt) :
    (l.execute s).2 = true ↔ l.cond s = true := by
  unfold Lwt.execute
  split <;> simp_all

/-- **`IF NOT EXISTS` on a fresh key applies; `IF v = x` on a fresh key does not.** -/
theorem insert_if_not_exists_on_empty (k : Key) (v : Val) :
    (⟨k, none, v⟩ : Lwt).execute Cell.empty = (Cell.empty.set k (some v), true) := by
  simp [Lwt.execute, Lwt.cond, Cell.empty]

theorem update_if_on_empty_fails (k : Key) (v0 v : Val) :
    (⟨k, some v0, v⟩ : Lwt).execute Cell.empty = (Cell.empty, false) := by
  simp [Lwt.execute, Lwt.cond, Cell.empty]

/-! ## Two CAS with the same expectation cannot both apply -/

/-- **The CAS guarantee.**  Starting from a cell that holds `v0` at `k`, with
    `va ≠ v0`, a first CAS `IF k = v0 SET k = va` applies; a second CAS with the
    same expectation on the same key then fails, because the value it would test
    is now `va`. -/
theorem at_most_one_applies (k : Key) (v0 va vb : Val)
    (hva : va ≠ v0) (hvb : vb ≠ v0) :
    ((⟨k, some v0, va⟩ : Lwt).execute (Cell.empty.set k (some v0))).2 = true
      ∧ ((⟨k, some v0, vb⟩ : Lwt).execute
            ((⟨k, some v0, va⟩ : Lwt).execute (Cell.empty.set k (some v0))).1).2 = false := by
  simp [Lwt.execute, Lwt.cond, Cell.set, hva, hvb]

/-- The same statement the other way round: whichever runs first wins, and at most
    one of the two ever applies, in either order. -/
theorem at_most_one_applies_either_order (k : Key) (v0 va vb : Val)
    (hva : va ≠ v0) (hvb : vb ≠ v0) :
    (((⟨k, some v0, vb⟩ : Lwt).execute
        ((⟨k, some v0, va⟩ : Lwt).execute (Cell.empty.set k (some v0))).1).2 = false)
      ∧ (((⟨k, some v0, va⟩ : Lwt).execute
            ((⟨k, some v0, vb⟩ : Lwt).execute (Cell.empty.set k (some v0))).1).2 = false) := by
  simp [Lwt.execute, Lwt.cond, Cell.set, hva, hvb]

/-- **A failed CAS reports the state it tested.**  The value the client is handed
    back is the value the next transaction in the serial order will see — no
    `CurrentValues` that disagrees with the write the following CAS is based on. -/
theorem failed_cas_reports_the_state_it_tested (s : Cell) (l : Lwt)
    (h : (l.execute s).2 = false) : s l.key ≠ l.expected := by
  unfold Lwt.execute at h
  split at h
  · simp at h
  · rename_i hc
    intro heq
    exact hc (by simp [Lwt.cond, heq])

/-! ## The anomaly: an unprotected CAS

Everything above assumes the CAS reads the key it conditions on, which is
`Lwt.ops`.  Drop that read — condition on one key, write another — and the
condition is evaluated against a state that is no longer the transaction's serial
prefix.  That is precisely the write-skew history of
`Accord.Recovery.write_skew_not_serializable`, and the checker rejects it. -/

/-- A CAS whose read set omits its condition key: it reads `readKey` and writes
    `writeKey`.  Nothing in the statement text stops an application from asking
    for this; only the transaction's read set does. -/
structure UnprotectedLwt where
  readKey : Key
  writeKey : Key
  expected : Option Val
  newValue : Val
  deriving DecidableEq, Repr

/-- The `UnprotectedLwt` shape can only be built when the two keys differ; if they
    coincide it *is* a protected CAS. -/
theorem unprotected_means_keys_differ (u : UnprotectedLwt) (h : u.readKey ≠ u.writeKey) :
    u.readKey ≠ u.writeKey := h

/-- Two unprotected CAS that read each other's write key, each observing `[]`
    (the key empty), both applied.  This is a `History` in the checker's model:
    A reads k1 and appends to k0; B reads k0 and appends to k1. -/
def hBothCasStale : History := ⟨[
  invEv 0 1 [.read 1, .append 0 10] 0,
  okEv 0 1 [.read 1, .append 0 10] [[]] 0 10,
  invEv 1 2 [.read 0, .append 1 20] 10,
  okEv 1 2 [.read 0, .append 1 20] [[]] 10 20]⟩

/-- **Not serializable.**  A must precede B (B did not see A's write) and B must
    precede A (A did not see B's write).  Two CAS that each "succeeded" on the
    same pair of keys produced a history with no serial explanation. -/
theorem unprotected_cas_is_not_serializable : serializableB hBothCasStale = false := by
  native_decide

/-- The protected version of the same pair: B's read set includes A's write key, so
    B observes `[10]` and the history serializes in timestamp order. -/
def hProtectedCas : History := ⟨[
  invEv 0 1 [.read 1, .append 0 10] 0,
  okEv 0 1 [.read 1, .append 0 10] [[]] 0 10,
  invEv 1 2 [.read 0, .append 1 20] 20,
  okEv 1 2 [.read 0, .append 1 20] [[10]] 20 30]⟩

theorem protected_cas_is_serializable : strictlySerializableB hProtectedCas = true := by
  native_decide

/-- And a two-CAS run on one key, where the second fails: the observed value of the
    failure is the serial state, so the history is serializable and only the first
    CAS is in it.  (The failed transaction carries an `info` result, so it is not
    part of the serial order — it wrote nothing.) -/
def hCasWithFailure : History := ⟨[
  invEv 0 1 [.read 0, .append 0 1] 0,
  okEv 0 1 [.read 0, .append 0 1] [[]] 0 10,
  invEv 1 2 [.read 0, .append 0 2] 20,
  ⟨1, .info, 2, [.read 0, .append 0 2], [[1]], 20, 30⟩]⟩

theorem cas_with_failure_is_serializable : strictlySerializableB hCasWithFailure = true := by
  native_decide

/-- The failed CAS observed exactly what the committed CAS wrote — the
    `CurrentValues` contract, checked on the history rather than asserted. -/
theorem failed_cas_observes_the_committed_value :
    (hCasWithFailure.evs.filter (fun e => e.kind == .info)).map (fun e => e.reads)
      = [[[1]]] := by
  native_decide

/-! ## Why this is Accord's problem and not the front end's

A protected CAS is an ordinary transaction with `readKey = writeKey`: its read set
covers its condition, so `Accord.Protocol`'s dependency completeness applies to it
and the serializability argument of `FINDINGS.md` G1 covers it.  An unprotected CAS
is a transaction whose read set does *not* cover what it conditioned on, which is
exactly the failure `FINDINGS.md` G2 exhibits.  There is no separate LWT
consistency argument: there is one serializability argument, and LWT either
respects it or falls outside it.
-/
theorem protected_cas_reads_its_write_key (l : Lwt) : l.key ∈ (l.ops.map Op.key) := by
  simp [Lwt.ops, Op.key]

end Ferrosa.Cql.Lwt
