/-
  Broken: built, shipping, and wrong
  ==================================

  Everything in this list is built and shipping and tells a customer something false today, which is
  a different category from a feature that was never written — an unimplemented item can be scheduled
  against a milestone, a broken one is already lying to someone. Each row names a witness theorem in
  its area's ledger, so the claim is not a mood. The damage concentrates on the surfaces a client
  trusts: five of the sixteen are SQL or SPARQL, the two places where a confidently wrong answer is
  most likely to be acted on.

  **A row's `site` is either a Rust location or the word `unverified:`.** Eleven rows point at code
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
  /-- What is wrong, in words a product reader can follow: what a user would see and when. A row
      whose site is `unverified:` opens with "Model only", because the code is correct there. -/
  plain : String
  /-- The engineering evidence: the code read at this revision, by file and line. -/
  site : String
  /-- Module key of the area ledger this row classifies, e.g. `"Ferrosa.Storage.Recovery"`. -/
  ledger : String
  /-- The exact `SpecItem.name` in that ledger. -/
  item : String

def broken : List Item := [
  ⟨"Recovery", "replay is idempotent",
   "Replaying the commit log after a crash can apply the same write twice. A write recorded with no mutation id is always replayed, so an increment or a list append can be doubled by a restart.",
   "commitlog/mutation.rs: a zero mutation_id is always re-applied, so a non-idempotent mutation duplicates",
   "Ferrosa.Storage.Recovery", "replay is idempotent"⟩,
  ⟨"Tombstone", "a local deletion time is representable for every TTL",
   "A deletion time later than January 2038 does not fit the 32-bit field that stores it. It wraps to a negative number and the row is then treated as already expired, so data with a long TTL can disappear early.",
   "ferrosa-common/src/cell.rs:52 and ferrosa-sstable/src/types.rs:90 hold it as i32 seconds (`i32::MAX` = not deleted); ferrosa-sstable/src/data.rs:1108-1111 decodes it with `wrapping_add`, so a past-2038 value wraps negative, and ferrosa-row-bridge/src/row.rs:53-55 then reports it expired with a signed `now_secs >= ldt`",
   "Ferrosa.Storage.Tombstone", "a local deletion time is representable for every valid TTL"⟩,
  ⟨"SQL", "three-valued logic is preserved",
   "Model only: the SQL surface handles NULL correctly here. NOT applied to an unknown comparison stays unknown, so a NULL row is excluded, and tests pin it. Only the model's simplified comparison gets this wrong.",
   "unverified: no site on main. ferrosa-sql evaluates `NOT` over the three-valued `Option` (plan.rs:192-197) and `sql_cmp` returns `None` for NULL, so UNKNOWN survives negation; plan.rs:1369-1373 and 1894-1898 assert the NULL row is excluded. The hazard is real in the model (Ferrosa/Sql/Semantics.lean:39 `notEqNaive`) but no Rust path performs the compare-and-negate translation",
   "Ferrosa.Sql.Semantics", "three-valued logic is preserved"⟩,
  ⟨"SQL", "aggregates follow the empty-set rules",
   "Model only: SUM over no rows returns NULL, as PostgreSQL does, on both the SQL and CQL surfaces, and tests pin it. Only the model's accumulator returns zero.",
   "unverified: no site on main. ferrosa-sql/src/exec.rs:379-381 returns `Value::Null` when `numeric_count == 0` (`// PostgreSQL: SUM over no non-null rows is NULL`), exec.rs:1211-1228 and plan.rs:1820-1824 assert it, and CQL's path agrees (ferrosa-cql/src/router.rs:1497-1508). The model's zero-returning accumulator (Ferrosa/Sql/Semantics.lean) has no shipping counterpart",
   "Ferrosa.Sql.Semantics", "aggregates follow SQL's empty-set rules"⟩,
  ⟨"SQL", "the commit decision is durable before the acknowledgement",
   "A COMMIT over the Postgres protocol logs each of the transaction's writes as a separate entry and never writes the single commit record that makes a transaction all-or-nothing. A crash while it is being logged can leave some of its writes applied and others not.",
   "ferrosa-postgres/src/query.rs:2014-2021 commits through `engine.write_atomic_batch(source)`, i.e. ferrosa-storage/src/engine.rs:10347-10348 with `record = None`, so ferrosa-storage/src/engine.rs:10429-10435 appends each mutation as its own log entry; the single CRC-protected decision record (ferrosa-storage/src/commitlog/transaction_record.rs, reachable only via ferrosa-storage/src/engine.rs:10505-10514) is never written, yet ferrosa-storage/src/engine.rs:10448 syncs and ferrosa-postgres/src/server.rs:1495-1498 acks `COMMIT`",
   "Ferrosa.Sql.Txn", "the commit decision is durable before the acknowledgement"⟩,
  ⟨"SPARQL", "`p+` returns each pair once",
   "Model only: a one-or-more property path returns each pair once, and a test with a cycle pins it. A different defect does exist here: a node linked to itself returns nothing for that path.",
   "unverified: no site on main. ferrosa-sparql/src/property_path.rs:158-169 seeds `visited` with the start and pushes a node only when `visited.insert` succeeds, so each `(start, node)` pair is emitted once and the cycle test at 345-362 asserts it. The model's `expandNoVisited` (Ferrosa/Sparql/Semantics.lean:80-82) has no Rust counterpart; a different hole does exist there — a self-loop `(a,a)` emits nothing",
   "Ferrosa.Sparql.Semantics", "`p+` returns each pair once"⟩,
  ⟨"SPARQL", "RDF-star terms are distinct from IRIs in the store",
   "Model only: the store keeps IRIs, blank nodes and literals apart by an explicit tag, and refuses quoted triples outright. The collision exists only in the model's numeric encoding of terms.",
   "unverified: no site on main. ferrosa-sparql/src/triple_store.rs:264-271 keys a term by a tagged identity header (`[2, kind, 0x10, datatype, 0x11, lang]`, kind 0=uri/1=bnode/2=literal) and triple_store.rs:335-343 hard-errors on an unknown tag, so no packing can collide an IRI with another kind; quoted triples are rejected outright (results.rs:158-163, update.rs:740-744). The collision exists only in the model's `s * 1000000 + p * 1000 + o` encoding (Ferrosa/Sparql/Semantics.lean:117-119)",
   "Ferrosa.Sparql.Semantics", "RDF-star terms are distinct from IRIs in the store"⟩,
  ⟨"Index", "index and table cannot diverge",
   "A secondary index is updated before its row is written, and only the row is logged. A crash between the two leaves an index entry for a row that does not exist, and recovery cannot repair it from the log.",
   "ferrosa-storage/src/store.rs:3907-3920 inserts the index postings and ferrosa-storage/src/store.rs:4065-4068 then puts the row in the memtable: two side effects, in that order, with the code comment admitting `before the memtable put`; ferrosa-storage/src/engine.rs:10123-10127 logs the row for durability and ferrosa-storage/src/engine.rs:10163-10186 performs the write, so the commit log holds no index record and a crash between them cannot be repaired from it",
   "Ferrosa.Sql.Index", "index and table cannot diverge"⟩,
  ⟨"Consistency", "the coordinator enforces the per-datacenter requirement",
   "A QUORUM write counts acknowledgements across all datacenters together, so a write acknowledged only in the primary datacenter succeeds while the secondary holds none of it.",
   "write.rs:753 compares a total ack count, so five acks in the primary and none in the secondary passes QUORUM",
   "Ferrosa.Sql.Consistency", "the coordinator enforces the per-datacenter requirement for every write, not a total"⟩,
  ⟨"Streaming", "a page bound in rows is a bound in memory",
   "Scans are paged by row count, 5,000 rows, not by size. The memory one page needs ranges from about 5 MB for narrow rows to about 5 GB for 1 MiB rows on the same coordinator.",
   "DEFAULT_SCAN_PAGE_SIZE is 5000 rows: 5 MB of narrow rows and 5 GB of 1 MiB rows on the same coordinator",
   "Ferrosa.Storage.Streaming", "the scan's page bound is a bound in bytes"⟩,
  ⟨"Streaming", "the scan never materialises a page of rows before encoding",
   "A scan gathers a whole page of rows in memory before it encodes any of them, so its peak memory is a full page rather than one row.",
   "encode_rows_with_writer visits one row at a time, but all_rows accumulates a whole page before it is called",
   "Ferrosa.Storage.Streaming", "the row buffer is never built as Vec<Vec<Option<CqlValue>>> on the scan path"⟩,
  ⟨"Streaming", "the reader streams partitions to the caller",
   "The partition reader hands back a finished list, so a whole page is held in memory before the caller sees its first partition.",
   "read_partitions_limited returns Vec<Partition>, so the page is already materialised when the caller sees it",
   "Ferrosa.Storage.Streaming", "the reader hands partitions to the caller instead of a vector of them"⟩,
  ⟨"Streaming", "a decoded chunk is written into a caller-supplied buffer",
   "Every compressed chunk is decompressed into a newly allocated buffer, so each byte read costs an extra allocation and an extra copy.",
   "compression.rs decompress ignores its uncompressed-length argument and returns a fresh Vec per chunk: one allocation and one copy for every byte read",
   "Ferrosa.Storage.Streaming", "a decoded chunk is written into a caller-supplied buffer rather than returned as a new Vec"⟩,
  ⟨"Streaming", "a slow client cannot grow the coordinator's memory",
   "A query's whole response page is built in memory before any of it is sent, so a slow client keeps all of it held on the coordinator until it has finished reading.",
   "ferrosa-cql/src/result.rs:133-164 encodes the whole page into one `BytesMut` (`let mut buf = BytesMut::new()` at :143, returned whole) and ferrosa-cql/src/connection.rs:757-779 only then freezes and sends it, so occupancy is the whole response and a slow client parks all of it in the coordinator; ferrosa-cql/src/router.rs:2967-2991 feeds it from a fully built `Vec<Vec<Option<CqlValue>>>`",
   "Ferrosa.Storage.Streaming", "a slow client cannot grow the coordinator's memory"⟩,
  ⟨"Streaming", "acks are coalesced so a high-latency pipe is not ack-starved",
   "Model only: this row is stale. The socket setting it says is missing, TCP_NODELAY, is set on every client and internode socket. Whether replies still go out as several small writes, and what that costs, has not been measured.",
   "unverified: stale as written. The premise that NODELAY exists only on unmerged c38add89 is false — `git merge-base --is-ancestor c38add89 HEAD` exits 0, and ferrosa-cql/src/server.rs:303-311 sets `stream.set_nodelay(true)` on every accepted CQL socket (same at ferrosa-postgres/src/server.rs:2237-2245, ferrosa-graph/src/bolt/server.rs:136-142, ferrosa-net/src/rpc/server.rs:267-273). Whether replies are still several small writes, and what that costs per statement, has not been re-measured. Do not re-site this row before measuring",
   "Ferrosa.Storage.Streaming", "acks are coalesced so a high-latency pipe is not ack-starved"⟩,
  ⟨"Raft", "an entry is published only when its ordered system-table write landed",
   "A schema change is reported as applied even when the write that makes it durable failed. The failure is logged and dropped, the applied index advances, and a restart replays from after that entry, so the system-table row can stay missing.",
   "ferrosa-cluster/src/raft/state_machine.rs:2166-2174 logs a failed ordered write and returns (the Error level at :2171 is a log line, not a latch), so the job completes and :466-467 adopt the completed metadata and advance last_applied; replay then starts at the next index, so the write is never re-issued",
   "Ferrosa.Raft.Obligations", "an entry is published only when its ordered system-table write landed"⟩
]

/-- Sixteen built-and-wrong items, pinned so a defect cannot be dropped from the list without the
    corpus classification breaking. -/
theorem broken_has_sixteen_items : broken.length = 16 := by native_decide

/-- The surfaces are where the damage concentrates: five of the sixteen are SQL or SPARQL, which are
    the two places a client is most likely to be told something confidently wrong. -/
theorem the_surfaces_carry_five : (broken.filter fun i => i.area == "SQL").length
    + (broken.filter fun i => i.area == "SPARQL").length = 5 := by native_decide

/-- Five of the sixteen could not be anchored to the shipping code: four describe an implementation
    that does the correct thing at this revision (with tests pinning it) and one is stale because the
    fix it says is unmerged has been merged since. Counted, so a later edit cannot quietly turn a
    model-only hazard back into an assertion about code. -/
theorem five_rows_have_no_verified_site :
    (broken.filter fun i => i.site.startsWith "unverified:").length = 5 := by native_decide

/-- The remainder — the rows that name a real location — are what the list's opening claim is about:
    code that was read, not inferred. -/
theorem eleven_rows_name_a_site :
    (broken.filter fun i => !i.site.startsWith "unverified:").length = 11 := by native_decide

/-- What a reader of the specification is shown for a row: the sentence that says what is wrong. -/
def readerText (i : Item) : String := i.plain

/-- Text a product reader can follow without the source tree open: no code spans, no file paths or
    line numbers, no Rust paths, and short enough to read as a sentence or two. -/
def readable (s : String) : Bool :=
  !s.isEmpty && s.length ≤ 320 &&
    !(["`", ".rs", "::", "src/"].any fun t => (s.splitOn t).length > 1)

/-- **Every row says what is wrong in words a product reader can follow.** The engineering evidence
    stays in `site`; this is the sentence the specification shows instead of it. -/
theorem every_row_reads_as_prose : broken.all (fun i => readable (readerText i)) = true := by
  native_decide

/-- **A reader is told when a row is about the model, not the shipping code.** Exactly the rows whose
    site is `unverified:` open their plain text with "Model only", so the product-facing sentence can
    never present a model-only hazard as a defect a customer meets. Written after the rows, so it is a
    regression guard: removing the prefix from any one row, or adding it to a verified one, fails it. -/
theorem model_only_rows_say_so :
    broken.all (fun i => i.site.startsWith "unverified:" == i.plain.startsWith "Model only") = true := by
  native_decide

def main : IO Unit := do
  IO.println s!"broken — built, shipping, wrong: {broken.length} items"
  for i in broken do
    IO.println s!"  [{i.area}] {i.what}"
    IO.println s!"      {i.plain}"
    IO.println s!"      evidence: {i.site}"

end Ferrosa.Broken
