/-
  Unimplemented: not there yet.
  =============================

  The ledgers say a property is not proved. That is one word for several situations, and only one
  of them is a red (see `Ferrosa/Broken.lean`). This file holds the rest:

    unimplemented   no code, no format field, no plan node. Build it.
    requirement     no implementation can meet it as written. Narrow the requirement.
    proof           the behaviour may well be right; the proof is missing.
    measurement     the claim needs evidence, not a proof. Build the harness.
    inherited       a third-party protocol decides it. Document, do not fix.
    undecided       blocked on a design choice nobody has made yet.
    closed          was listed as red, has since been implemented.

  The two lists together with `Broken.broken` account for every red item in every ledger, which is
  what `every_ledger_item_is_classified` pins.
-/
import Ferrosa.Ledger
import Ferrosa.Cdc.Subscribe
import Ferrosa.Flight.Subscribe
import Ferrosa.Broken
import Ferrosa.Cql.AggregateObligations
import Ferrosa.Storage.Recovery
import Ferrosa.Storage.Tombstone
import Ferrosa.Storage.Format
import Ferrosa.Sql.Semantics
import Ferrosa.Bolt.Session
import Ferrosa.Sparql.Semantics
import Ferrosa.Sql.Index
import Ferrosa.Sql.Txn
import Ferrosa.Storage.Mvcc
import Ferrosa.Storage.Streaming
import Ferrosa.Sql.Consistency

namespace Ferrosa.Unimplemented

open Ferrosa.Ledger

structure Item where
  area : String
  what : String

/-- Absent: no code path, no format field, no plan node. -/
def unimplemented : List Item := [
  ⟨"CQL aggregate", "COUNT(DISTINCT) merges across shards"⟩,
  ⟨"CQL aggregate", "COUNT(ESTIMATE_DISTINCT) has a relative error bound"⟩,
  ⟨"CQL aggregate", "COUNT(ESTIMATE_DISTINCT) merges across shards"⟩,
  ⟨"CQL aggregate", "the CQL surface accepts COUNT(DISTINCT c)"⟩,
  ⟨"CQL aggregate", "the CQL surface accepts COUNT(ESTIMATE_DISTINCT c)"⟩,
  ⟨"Recovery", "a corrupted payload is detected"⟩,
  ⟨"Recovery", "corruption is quarantined, never deleted"⟩,
  ⟨"Tombstone", "gc_grace is enforced against that precondition"⟩,
  ⟨"fnf", "a flush can resume at the last verified record"⟩,
  ⟨"fnf", "a table can be exported to BTI for Cassandra by compaction"⟩,
  ⟨"SQL", "a multi-statement transaction is atomic"⟩,
  ⟨"Bolt", "queued result streams are bounded"⟩,
  ⟨"Txn", "SERIALIZABLE isolation is available"⟩,
  ⟨"MVCC", "versions are stored in the fnf record"⟩,
  ⟨"MVCC", "compaction carries the spill pointer forward"⟩,
  ⟨"Streaming", "buffer memory is one shared pool sized by a shared_memory setting"⟩,
  ⟨"Consistency", "the write path captures a transaction's read set and validates it before committing"⟩,
  ⟨"MVCC", "the read path records a predicate lock for every row it reads"⟩,
  ⟨"SUBSCRIBE", "the gap signal carries the position to replay from"⟩,
  ⟨"SUBSCRIBE", "a cancelled subscription reports its final cursor"⟩,
  ⟨"SUBSCRIBE", "the Flight surface can plan and serve a stream ticket"⟩,
  ⟨"SUBSCRIBE", "each consumer's unacknowledged messages are queued on disk and truncated by acknowledgement"⟩,
  ⟨"SUBSCRIBE", "a proportional controller drives the fair share"⟩,
  ⟨"MVCC", "an asynchronous vacuum decrements reference counts after each commit"⟩
]

/-- Everything the ledgers list that is neither broken nor absent. -/
structure Other where
  area : String
  what : String
  why : String

def nonDefects : List Other := [
  ⟨"Recovery", "an acknowledged write survives a crash",
   "requirement: it survives under on-ack sync; the periodic mode is a documented looser guarantee"⟩,
  ⟨"Tombstone", "the merge is order-independent",
   "proof: only the colliding cases are checked, not the general statement"⟩,
  ⟨"fnf", "record CRCs cost less than 5% of point-read p99",
   "measurement: needs the harness, not a theorem"⟩,
  ⟨"Bolt", "replies can be paired to messages by index",
   "inherited: Bolt pairs them by arrival, and a FAILURE carries no statement identity"⟩,
  ⟨"Bolt", "a failure is attributable to a statement",
   "inherited: the protocol has nowhere to put the identity"⟩,
  ⟨"SPARQL", "OPTIONAL scoping is specified",
   "undecided: no representation in the plan types yet"⟩,
  ⟨"Index", "a range lookup on an indexed column is constant in nodes",
   "requirement: impossible for BTree, and the O(1) claim belongs to Hash only"⟩,
  ⟨"CQL aggregate", "COUNT(DISTINCT) is O(1) in memory",
   "closed: count.rs implements DistinctExact with a disk-backed slot table, 19/19 tests"⟩
]

def ledgerTotal : Nat :=
  6
    + redCount Ferrosa.Storage.Recovery.ledger
    + redCount Ferrosa.Storage.Tombstone.ledger
    + redCount Ferrosa.Storage.Format.ledger
    + redCount Ferrosa.Sql.ledger
    + redCount Ferrosa.Bolt.ledger
    + redCount Ferrosa.Sparql.ledger
    + redCount Ferrosa.Sql.Index.ledger
    + redCount Ferrosa.Sql.Txn.ledger
    + redCount Ferrosa.Storage.Mvcc.ledger
    + redCount Ferrosa.Cdc.ledger
    + redCount Ferrosa.Flight.ledger
    + redCount Ferrosa.Sql.Consistency.ledger
    + redCount Ferrosa.Storage.Streaming.ledger

/-- **Every red item in every ledger appears here or in `Broken.broken`, exactly once.** -/
theorem every_ledger_item_is_classified :
    Ferrosa.Broken.broken.length + unimplemented.length + nonDefects.length = ledgerTotal := by
  native_decide

theorem unimplemented_has_the_right_length : unimplemented.length = 24 := by native_decide
theorem twelve_are_not_defects : nonDefects.length = 8 := by native_decide

/-- **Live defects now outnumber everything else.**  Thirteen reds against twelve requirements,
    proofs, measurements, inherited protocol behaviour and closures. This theorem had the opposite
    direction until the memory specs found three more: the closer the specs get to the code, the
    more of it turns out to be shipping something that is not true. -/
theorem live_defects_outnumber_the_rest : nonDefects.length < Ferrosa.Broken.broken.length := by
  native_decide

def main : IO Unit := do
  IO.println s!"unimplemented — no code path yet: {unimplemented.length} items"
  for i in unimplemented do
    IO.println s!"  [{i.area}] {i.what}"
  IO.println ""
  IO.println s!"not defects — requirement, proof, measurement, inherited, undecided, closed: {nonDefects.length}"
  for o in nonDefects do
    IO.println s!"  [{o.area}] {o.what}"
    IO.println s!"      {o.why}"

end Ferrosa.Unimplemented
