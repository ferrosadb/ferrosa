/-
  Broken: the implementation exists and it is wrong.
  ==================================================

  These are the reds. Not "we have not built it" — built, shipping, and producing a wrong answer
  or a wrong claim today. Every one has a witness theorem in its area's ledger, and every one has a
  place in the code that can be pointed at.

  The distinction orders the work: an unimplemented feature can be scheduled against a milestone, a
  broken one is already telling someone something false.

  Every row carries the area ledger it classifies (`ledger`, a module key registered in
  `Ferrosa/Unimplemented.lean`) and the exact `SpecItem.name` it classifies (`item`). `area` and
  `what` are prose for the reader; `ledger` and `item` are what the machine checks.
-/
import Ferrosa.Ledger

namespace Ferrosa.Broken

/-- A red item: where it lives, what it promises, where it breaks that promise, and — so the
    accounting can be checked rather than asserted — which ledger row it is. -/
structure Item where
  area : String
  what : String
  site : String
  /-- Module key of the area ledger this row classifies, e.g. `"Ferrosa.Storage.Recovery"`. -/
  ledger : String
  /-- The exact `SpecItem.name` in that ledger. -/
  item : String

def broken : List Item := [
  ⟨"Recovery", "replay is idempotent",
   "commitlog/mutation.rs: a zero mutation_id is always re-applied, so a non-idempotent mutation duplicates",
   "Ferrosa.Storage.Recovery", "replay is idempotent"⟩,
  ⟨"Tombstone", "a local deletion time is representable for every TTL",
   "the field is i32, so it wraps at 2038-01-19 and a long TTL expires immediately",
   "Ferrosa.Storage.Tombstone", "a local deletion time is representable for every valid TTL"⟩,
  ⟨"SQL", "three-valued logic is preserved",
   "NOT (b = 5) is translated to b <> 5, which admits the NULL rows SQL's UNKNOWN excludes",
   "Ferrosa.Sql.Semantics", "three-valued logic is preserved"⟩,
  ⟨"SQL", "aggregates follow the empty-set rules",
   "SUM over no rows returns 0 where SQL requires NULL",
   "Ferrosa.Sql.Semantics", "aggregates follow SQL's empty-set rules"⟩,
  ⟨"SQL", "the commit decision is durable before the acknowledgement",
   "COMMIT reports success with no durable decision recorded anywhere",
   "Ferrosa.Sql.Txn", "the commit decision is durable before the acknowledgement"⟩,
  ⟨"SPARQL", "`p+` returns each pair once",
   "path expansion emits one row per path, so a two-node cycle gives three rows for two pairs",
   "Ferrosa.Sparql.Semantics", "`p+` returns each pair once"⟩,
  ⟨"SPARQL", "RDF-star terms are distinct from IRIs in the store",
   "Term.quoted 1 2 3 and Term.iri 1002003 hash to the same key",
   "Ferrosa.Sparql.Semantics", "RDF-star terms are distinct from IRIs in the store"⟩,
  ⟨"Index", "index and table cannot diverge",
   "index maintenance sits outside the atomic boundary, so a crash between the two leaves them disagreeing",
   "Ferrosa.Sql.Index", "index and table cannot diverge"⟩,
  ⟨"Consistency", "the coordinator enforces the per-datacenter requirement",
   "write.rs:753 compares a total ack count, so five acks in the primary and none in the secondary passes QUORUM",
   "Ferrosa.Sql.Consistency", "the coordinator enforces the per-datacenter requirement for every write, not a total"⟩,
  ⟨"Streaming", "a page bound in rows is a bound in memory",
   "DEFAULT_SCAN_PAGE_SIZE is 5000 rows: 5 MB of narrow rows and 5 GB of 1 MiB rows on the same coordinator",
   "Ferrosa.Storage.Streaming", "the scan's page bound is a bound in bytes"⟩,
  ⟨"Streaming", "the scan never materialises a page of rows before encoding",
   "encode_rows_with_writer visits one row at a time, but all_rows accumulates a whole page before it is called",
   "Ferrosa.Storage.Streaming", "the row buffer is never built as Vec<Vec<Option<CqlValue>>> on the scan path"⟩,
  ⟨"Streaming", "the reader streams partitions to the caller",
   "read_partitions_limited returns Vec<Partition>, so the page is already materialised when the caller sees it",
   "Ferrosa.Storage.Streaming", "the reader hands partitions to the caller instead of a vector of them"⟩,
  ⟨"Streaming", "a decoded chunk is written into a caller-supplied buffer",
   "compression.rs decompress ignores its uncompressed-length argument and returns a fresh Vec per chunk: one allocation and one copy for every byte read",
   "Ferrosa.Storage.Streaming", "a decoded chunk is written into a caller-supplied buffer rather than returned as a new Vec"⟩,
  ⟨"Streaming", "a slow client cannot grow the coordinator's memory",
   "the response is encoded into one BytesMut and written afterwards, so a slow client parks the whole page in the coordinator instead of applying backpressure",
   "Ferrosa.Storage.Streaming", "a slow client cannot grow the coordinator's memory"⟩,
  ⟨"Streaming", "acks are coalesced so a high-latency pipe is not ack-starved",
   "TCP_NODELAY exists only as c38add89, which is not an ancestor of main, and it batches nothing: replies are still several small writes, so main pays ~46 ms per statement over a real network",
   "Ferrosa.Storage.Streaming", "acks are coalesced so a high-latency pipe is not ack-starved"⟩
]

theorem broken_has_fifteen_items : broken.length = 15 := by native_decide

/-- The surfaces are where the damage concentrates: five of the thirteen are SQL or SPARQL, which are
    the two places a client is most likely to be told something confidently wrong. -/
theorem the_surfaces_carry_five : (broken.filter fun i => i.area == "SQL").length
    + (broken.filter fun i => i.area == "SPARQL").length = 5 := by native_decide

def main : IO Unit := do
  IO.println s!"broken — built, shipping, wrong: {broken.length} items"
  for i in broken do
    IO.println s!"  [{i.area}] {i.what}"
    IO.println s!"      {i.site}"

end Ferrosa.Broken
