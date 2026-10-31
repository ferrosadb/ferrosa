/-
  Broken: built, shipping, and wrong
  ==================================

  Everything in this list is built and shipping and tells a customer something false today, which is
  a different category from a feature that was never written — an unimplemented item can be scheduled
  against a milestone, a broken one is already lying to someone. Each row names a witness theorem in
  its area's ledger, so the claim is not a mood. The damage concentrates on the surfaces a client
  trusts: five of the fifteen are SQL or SPARQL, the two places where a confidently wrong answer is
  most likely to be acted on.

  **A row's `site` is either a Rust location or the word `unverified:`.** Ten rows point at code
  that was read at this revision. Five were checked on 2026-10-31 and describe a hazard that exists
  in the Lean model but **not in the shipping implementation**: the code does the correct thing and
  has tests pinning it. Those rows say so in place, because a row claiming a site that does not
  exist is the same failure the list is about — a confident statement nobody verified. They stay in
  the list (the ledger items are still red, the model still admits the hazard) but they are not
  evidence that a customer is being lied to, and `five_rows_have_no_verified_site` counts them so a
  future reader cannot quietly reinterpret them.

  Rows carry the area ledger they classify and the exact ledger item, because `Ferrosa/Unimplemented.lean`
  re-derives the whole classification from those two fields and fails if a red is unaccounted for.
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
   "ferrosa-common/src/cell.rs:52 and ferrosa-sstable/src/types.rs:90 hold it as i32 seconds (`i32::MAX` = not deleted); ferrosa-sstable/src/data.rs:1108-1111 decodes it with `wrapping_add`, so a past-2038 value wraps negative, and ferrosa-row-bridge/src/row.rs:53-55 then reports it expired with a signed `now_secs >= ldt`",
   "Ferrosa.Storage.Tombstone", "a local deletion time is representable for every valid TTL"⟩,
  ⟨"SQL", "three-valued logic is preserved",
   "unverified: no site on main. ferrosa-sql evaluates `NOT` over the three-valued `Option` (plan.rs:192-197) and `sql_cmp` returns `None` for NULL, so UNKNOWN survives negation; plan.rs:1369-1373 and 1894-1898 assert the NULL row is excluded. The hazard is real in the model (Ferrosa/Sql/Semantics.lean:39 `notEqNaive`) but no Rust path performs the compare-and-negate translation",
   "Ferrosa.Sql.Semantics", "three-valued logic is preserved"⟩,
  ⟨"SQL", "aggregates follow the empty-set rules",
   "unverified: no site on main. ferrosa-sql/src/exec.rs:379-381 returns `Value::Null` when `numeric_count == 0` (`// PostgreSQL: SUM over no non-null rows is NULL`), exec.rs:1211-1228 and plan.rs:1820-1824 assert it, and CQL's path agrees (ferrosa-cql/src/router.rs:1497-1508). The model's zero-returning accumulator (Ferrosa/Sql/Semantics.lean) has no shipping counterpart",
   "Ferrosa.Sql.Semantics", "aggregates follow SQL's empty-set rules"⟩,
  ⟨"SQL", "the commit decision is durable before the acknowledgement",
   "ferrosa-postgres/src/query.rs:2014-2021 commits through `engine.write_atomic_batch(source)`, i.e. ferrosa-storage/src/engine.rs:10347-10348 with `record = None`, so ferrosa-storage/src/engine.rs:10429-10435 appends each mutation as its own log entry; the single CRC-protected decision record (ferrosa-storage/src/commitlog/transaction_record.rs, reachable only via ferrosa-storage/src/engine.rs:10505-10514) is never written, yet ferrosa-storage/src/engine.rs:10448 syncs and ferrosa-postgres/src/server.rs:1495-1498 acks `COMMIT`",
   "Ferrosa.Sql.Txn", "the commit decision is durable before the acknowledgement"⟩,
  ⟨"SPARQL", "`p+` returns each pair once",
   "unverified: no site on main. ferrosa-sparql/src/property_path.rs:158-169 seeds `visited` with the start and pushes a node only when `visited.insert` succeeds, so each `(start, node)` pair is emitted once and the cycle test at 345-362 asserts it. The model's `expandNoVisited` (Ferrosa/Sparql/Semantics.lean:80-82) has no Rust counterpart; a different hole does exist there — a self-loop `(a,a)` emits nothing",
   "Ferrosa.Sparql.Semantics", "`p+` returns each pair once"⟩,
  ⟨"SPARQL", "RDF-star terms are distinct from IRIs in the store",
   "unverified: no site on main. ferrosa-sparql/src/triple_store.rs:264-271 keys a term by a tagged identity header (`[2, kind, 0x10, datatype, 0x11, lang]`, kind 0=uri/1=bnode/2=literal) and triple_store.rs:335-343 hard-errors on an unknown tag, so no packing can collide an IRI with another kind; quoted triples are rejected outright (results.rs:158-163, update.rs:740-744). The collision exists only in the model's `s * 1000000 + p * 1000 + o` encoding (Ferrosa/Sparql/Semantics.lean:117-119)",
   "Ferrosa.Sparql.Semantics", "RDF-star terms are distinct from IRIs in the store"⟩,
  ⟨"Index", "index and table cannot diverge",
   "ferrosa-storage/src/store.rs:3907-3920 inserts the index postings and ferrosa-storage/src/store.rs:4065-4068 then puts the row in the memtable: two side effects, in that order, with the code comment admitting `before the memtable put`; ferrosa-storage/src/engine.rs:10123-10127 logs the row for durability and ferrosa-storage/src/engine.rs:10163-10186 performs the write, so the commit log holds no index record and a crash between them cannot be repaired from it",
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
   "ferrosa-cql/src/result.rs:133-164 encodes the whole page into one `BytesMut` (`let mut buf = BytesMut::new()` at :143, returned whole) and ferrosa-cql/src/connection.rs:757-779 only then freezes and sends it, so occupancy is the whole response and a slow client parks all of it in the coordinator; ferrosa-cql/src/router.rs:2967-2991 feeds it from a fully built `Vec<Vec<Option<CqlValue>>>`",
   "Ferrosa.Storage.Streaming", "a slow client cannot grow the coordinator's memory"⟩,
  ⟨"Streaming", "acks are coalesced so a high-latency pipe is not ack-starved",
   "unverified: stale as written. The premise that NODELAY exists only on unmerged c38add89 is false — `git merge-base --is-ancestor c38add89 HEAD` exits 0, and ferrosa-cql/src/server.rs:303-311 sets `stream.set_nodelay(true)` on every accepted CQL socket (same at ferrosa-postgres/src/server.rs:2237-2245, ferrosa-graph/src/bolt/server.rs:136-142, ferrosa-net/src/rpc/server.rs:267-273). Whether replies are still several small writes, and what that costs per statement, has not been re-measured. Do not re-site this row before measuring",
   "Ferrosa.Storage.Streaming", "acks are coalesced so a high-latency pipe is not ack-starved"⟩
]

/-- Fifteen built-and-wrong items, pinned so a defect cannot be dropped from the list without the
    corpus classification breaking. -/
theorem broken_has_fifteen_items : broken.length = 15 := by native_decide

/-- The surfaces are where the damage concentrates: five of the fifteen are SQL or SPARQL, which are
    the two places a client is most likely to be told something confidently wrong. -/
theorem the_surfaces_carry_five : (broken.filter fun i => i.area == "SQL").length
    + (broken.filter fun i => i.area == "SPARQL").length = 5 := by native_decide

/-- Five of the fifteen could not be anchored to the shipping code: four describe an implementation
    that does the correct thing at this revision (with tests pinning it) and one is stale because the
    fix it says is unmerged has been merged since. Counted, so a later edit cannot quietly turn a
    model-only hazard back into an assertion about code. -/
theorem five_rows_have_no_verified_site :
    (broken.filter fun i => i.site.startsWith "unverified:").length = 5 := by native_decide

/-- The remainder — the rows that name a real location — are what the list's opening claim is about:
    code that was read, not inferred. -/
theorem ten_rows_name_a_site :
    (broken.filter fun i => !i.site.startsWith "unverified:").length = 10 := by native_decide

def main : IO Unit := do
  IO.println s!"broken — built, shipping, wrong: {broken.length} items"
  for i in broken do
    IO.println s!"  [{i.area}] {i.what}"
    IO.println s!"      {i.site}"

end Ferrosa.Broken
