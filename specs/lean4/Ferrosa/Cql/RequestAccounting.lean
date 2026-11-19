/-
  Ferrosa formal specification — CQL request accounting.
  =====================================================

  Every request the server accepts has exactly one outcome, and the exported
  counters must account for all of them. The property that matters operationally
  is narrower than "the counters are right": it is that a request the server
  REFUSES is still counted.

  A refused request is the easy one to lose. It never enters the in-flight set,
  it never runs a `RequestTimer`, and it produces no result — the only trace is
  an `ERROR(Overloaded)` frame the client sees. The server-side series
  `ferrosa_cql_requests_total{outcome="error"}` is fed by timers, so a shedding
  path that merely replies leaves that series at zero.

  Observed at scale (easy-db-lab, 3 nodes, `KeyValue`, --rate 20000 --threads 16,
  2026-11-19): the client counted 35,339 failed operations while all three nodes
  exported `outcome="error" 0`, `net_data_lane_rejected_total 0`,
  `write_admission_rejected_total{reason="hard_memtable"} 0` and
  `memtable_backpressure 0`. Every diagnostic an operator would consult said the
  cluster was healthy; the client said a third of its work failed. The reason is
  modelled here: shedding was not accounted for.

  The fix gives a shed request an explicit outcome, so the invariant below holds
  for the refusal arm as well as the executed arm.
-/

namespace Ferrosa.Cql.RequestAccounting

/-- How a request ended. `Shed` is a refusal at admission: no work ran, the
client received `ERROR(Overloaded)`. -/
inductive Outcome where
  | ok
  | failed
  | shed
  | cancelled
  deriving DecidableEq, Repr

/-- The exported counter a given outcome increments. The point of the model: the
mapping is TOTAL, so no outcome can exist without a counter to land in. -/
def counterFor : Outcome → String
  | .ok => "ok"
  | .failed => "error"
  | .shed => "error"
  | .cancelled => "cancelled"

/-- A request as accounted: its outcome, and whether accounting recorded it. -/
structure Accounted where
  outcome : Outcome
  recorded : Bool
  deriving DecidableEq, Repr

/-- The invariant an operator relies on: every request that ended is recorded. -/
def AllAccounted (rs : List Accounted) : Prop := ∀ r ∈ rs, r.recorded = true

/-- The shipped behaviour: a timer records `ok`/`failed`/`cancelled`, and the
shed arm replies without recording anything. -/
def accountOld (outcome : Outcome) (recorded : Bool) : Accounted :=
  match outcome with
  | .shed => ⟨.shed, false⟩
  | _ => ⟨outcome, recorded⟩

/-- The fix: a shed request is recorded like any other. -/
def accountNew (outcome : Outcome) (recorded : Bool) : Accounted :=
  ⟨outcome, recorded⟩

/-- A run in which the server shed one request and executed one. -/
def runOld : List Accounted := [accountOld .ok true, accountOld .shed true]

/-- The same run under the fix. -/
def runNew : List Accounted := [accountNew .ok true, accountNew .shed true]

/-- The shipped accounting loses the shed request: it is not recorded. `decide`
refutes the invariant on that trace, which is the counter reading zero while the
client saw failures. -/
theorem old_loses_the_shed_request : ¬ AllAccounted runOld := by
  intro h
  have := h ⟨.shed, false⟩ (by decide)
  exact absurd this (by decide)

/-- Under the fix the same run accounts for every request. -/
theorem new_accounts_for_the_shed_request : AllAccounted runNew := by
  intro r hr
  simp only [runNew, List.mem_cons, List.not_mem_nil, or_false] at hr
  rcases hr with h | h <;> subst h <;> rfl

/-- General form: whatever the outcomes, the fix records what it is given. -/
theorem new_is_total (outcome : Outcome) : (accountNew outcome true).recorded = true := rfl

/-- And a shed request lands in the error series, so an operator consulting the
error counter sees load shedding rather than reading the node as healthy. -/
theorem shed_lands_in_the_error_series : counterFor .shed = "error" := rfl

end Ferrosa.Cql.RequestAccounting
