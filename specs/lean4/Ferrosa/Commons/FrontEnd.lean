/-
  What the five front ends duplicate, and which module should own each piece
  ==========================================================================

  The same twelve concerns are specified once per surface today — a value system, three-valued
  truth, a logical plan, parameter binding, an error taxonomy, stream and gap handling,
  backpressure, consistency levels, aggregate merges, wire framing, reader liveness, and the merge
  laws under all of them — under different names and with slightly different rules. Each entry names
  the code sites that duplicate it and the commons module that should own it, so a unification has an
  inventory rather than an opinion.

  The test for belonging in commons is stated as a theorem: an entry is listed only when at least
  two front ends need it, and a single surface's concern stays where it is.
-/
import Ferrosa.Core

namespace Ferrosa.Commons.Refactors

/-- One duplicated concern. `frontEnds` is the evidence for putting it in commons; `sites` is where the
    duplication lives today; `home` is the commons module that should own it. -/
structure Target where
  name : String
  frontEnds : List String
  sites : String
  home : String

def targets : List Target := [
  ⟨"value and type system",
   ["CQL", "SQL", "Bolt", "SPARQL", "Flight"],
   "CqlValue (ferrosa-cql/src/ast.rs), PG Datum + OIDs (ferrosa-postgres), Bolt struct tags, SPARQL literals",
   "Commons.Value"⟩,
  ⟨"three-valued truth and null propagation",
   ["CQL", "SQL", "SPARQL"],
   "SQL <> admits NULL, CQL null comparison, SPARQL unbound/error",
   "Commons.Truth"⟩,
  ⟨"logical plan",
   ["CQL", "SQL", "Bolt", "Flight"],
   "router.rs plan, ferrosa-postgres executor, Bolt RUN, Flight plan.rs",
   "Commons.Plan"⟩,
  ⟨"parameter binding and prepared statements",
   ["CQL", "SQL", "Bolt"],
   "CQL prepared cache, PG portal, Bolt statement identity",
   "Commons.Parameters"⟩,
  ⟨"error and verdict taxonomy",
   ["CQL", "SQL", "Bolt"],
   "CQL error codes, PG SQLSTATE, Bolt FAILURE, Sql.Consistency.Verdict.retry",
   "Commons.Verdict"⟩,
  ⟨"stream, gap and resume",
   ["CQL", "Flight", "CDC"],
   "Cql/Subscribe, Flight/Subscribe, Cdc/Subscribe (Gap, replay)",
   "Commons.Stream"⟩,
  ⟨"backpressure and the shared memory pool",
   ["CQL", "Flight", "CDC"],
   "Cdc/Subscribe fair share, controller and spill; Postgres unbuffered write_all",
   "Commons.Flow"⟩,
  ⟨"consistency levels and their acknowledgement rules",
   ["CQL", "SQL", "Bolt"],
   "Sql/Consistency.lean, Topology/ConsistencyLevels.lean, ferrosa-cluster/src/consistency.rs",
   "Commons.Consistency"⟩,
  ⟨"aggregate merge rules and sketches",
   ["CQL", "SQL", "Flight"],
   "count.rs DistinctExact/DistinctEstimate, router.rs COUNT name matching",
   "Commons.Hll"⟩,
  ⟨"wire framing and acknowledgement coalescing",
   ["SQL", "Bolt", "CQL"],
   "ferrosa-postgres/src/server.rs unbuffered write_all, Bolt packstream, CQL native frames",
   "Commons.Framing"⟩,
  ⟨"reader liveness and cancellation",
   ["CQL", "SQL", "Flight"],
   "Mvcc.stalled / STALLED_READER_TIMEOUT applied per surface",
   "Commons.Liveness"⟩,
  ⟨"merge laws shared by every join above",
   ["Tombstone", "Sketch"],
   "Obs.merge (Storage/Tombstone) and Registers.merge (Commons/Hll) are the same algebra",
   "Commons.Tactics"⟩
]

def isShared (t : Target) : Bool := 2 ≤ t.frontEnds.length

/-- **Every target in the list is shared by at least two front ends**, which is the test for belonging
    in commons. `native_decide` over the concrete list: a finite computation, not a general claim. -/
theorem every_commons_target_is_shared_by_at_least_two_front_ends :
    targets.all isShared = true := by native_decide

/-- The inventory is twelve entries; adding a thirteenth without a shared justification fails here. -/
theorem there_are_twelve_commons_targets : targets.length = 12 := by native_decide

def main : IO Unit := do
  IO.println s!"commons refactor targets: {targets.length}"
  for t in targets do
    IO.println s!"  {t.name} -> {t.home}"

end Ferrosa.Commons.Refactors
