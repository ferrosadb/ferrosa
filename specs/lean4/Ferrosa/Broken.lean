/-
  Broken: built, shipping, and wrong
  ==================================

  Everything in this list is built and shipping and tells a customer something false today, which is
  a different category from a feature that was never written — an unimplemented item can be scheduled
  against a milestone, a broken one is already lying to someone. Each row names a witness theorem in
  its area's ledger, so the claim is not a mood. The damage concentrates on the surfaces a client
  trusts: five of the thirty-one are SQL or SPARQL, the two places where a confidently wrong answer is
  most likely to be acted on.

  **A row's `site` is either a Rust location or the word `unverified:`.** Twenty-one rows point at code
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
  ⟨"Raft", "a startup inconsistency is repaired in place, serves consistent state, or stays quarantined naming the cause",
   "When the saved snapshot cannot be restored, or cannot be read at all, the node logs a warning and starts anyway, reporting an applied index it cannot back with real schema or table state. It looks healthy, answers questions from state that is not there, and nothing retries or repairs it.",
   "ferrosa-cluster/src/raft/state_machine.rs:959-960 adopt state and last_applied before sync_schema_and_engine_from_state (:1100-1180) can fail, and ferrosa-cluster/src/controller/cluster.rs:1467-1472 logs that error and boots; the classifier at :1492-1496 is asked about the already-adopted index, so it answers Usable. When the file does not decode at all, last_applied stays None and recover_from_purge_point (state_machine.rs:713-724) sets it to the purge point without restoring state. Neither ferrosa-cluster/src/controller/mod.rs:994-999 nor ferrosa/src/web/readiness.rs:191-372 compares synced against applied; the branch that returns without openraft (controller/cluster.rs:1588-1594) leaves the node up and permanently out of the cluster",
   "Ferrosa.Raft.Obligations", "a startup inconsistency is repaired in place, serves consistent state, or stays quarantined naming the cause"⟩,
  ⟨"Raft", "code: a node whose restore failed answers reads and reports ready on state it does not have",
   "A node whose saved state fails to restore starts anyway. It reports an applied index it cannot back with real table state, answers queries from data that is not there, and reports itself ready.",
   "ferrosa-cluster/src/raft/state_machine.rs:960 adopts last_applied while :1100-1180 can still fail; ferrosa-cluster/src/controller/cluster.rs:1467-1472 logs and boots; ferrosa-cluster/src/controller/mod.rs:994-999 checks consensus health, write path, DDL path and declared topology, and ferrosa/src/web/readiness.rs:191-372 checks supervised tasks, listeners, set-aside mutations and Raft liveness - neither reads synced; build_snapshot at state_machine.rs:2528-2583 then publishes that unhonoured state and mark_durably_applied (:2576) lets the log purge to it",
   "Ferrosa.Raft.Obligations", "code: a node whose restore failed answers reads and reports ready on state it does not have"⟩,
  ⟨"Raft", "code: the node exits an inconsistent state on an index it was told, not on state it holds",
   "A node that cannot read its saved snapshot adopts the log position it had already discarded. It then claims the whole log is applied while holding no catalog, and never replays the entries it skipped.",
   "ferrosa-cluster/src/raft/local_state.rs:88-102 compares last_applied against last_purged, two numbers, and ferrosa-cluster/src/raft/state_machine.rs:713-724 sets last_applied to the purge point without touching state; nothing in the boot path compares RaftState against the entries it claims to have applied",
   "Ferrosa.Raft.Obligations", "code: the node exits an inconsistent state on an index it was told, not on state it holds"⟩,
  ⟨"Raft", "code: a latched node cannot install the snapshot that would repair it, and never becomes healthy again",
   "When a node has already failed to apply a change, the one message that could repair it is refused before it is read. The node can never recover without a restart, and the restart reproduces the failure.",
   "ferrosa-cluster/src/raft/state_machine.rs:2801-2837 has install_snapshot begin with finish_pending_apply (:2804), which returns the apply_failure error for every call while the latch is set (:527-529); ferrosa-cluster/src/consensus_health.rs:97-125 has fail/is_healthy/failure/failure_count and no reset, so the failure is process-lifetime",
   "Ferrosa.Raft.Obligations", "code: a latched node cannot install the snapshot that would repair it, and never becomes healthy again"⟩,
  ⟨"Raft", "code: a repair replaces local state wholesale and records nothing it discarded",
   "Repairing a node from a peer replaces its local state wholesale. Rows written locally that are newer than the peer's copy are dropped, and no record says they existed.",
   "ferrosa-cluster/src/raft/state_machine.rs:959 assigns self.state = data.state and :960 takes last_applied from the snapshot; the guards at :947-952 and :997-1016 refuse only a snapshot that drops a table with local artifacts, and :1138-1146 refuses only a persisted startup snapshot with no replacement order - none merges or records a row for a table the snapshot also carries",
   "Ferrosa.Raft.Obligations", "code: a repair replaces local state wholesale and records nothing it discarded"⟩,
  ⟨"Raft", "code: a node whose projection diverged still votes and is counted toward quorum",
   "A node whose local state disagrees with the index it reports still answers vote requests and counts toward quorum, so it can help elect a leader while unable to honour the state it holds.",
   "ferrosa-cluster/src/raft/handlers.rs:1014-1019 forwards a VoteRequest straight to raft.vote with no state-machine or health term, so a node that booted with last_applied ahead of its RaftState (ferrosa-cluster/src/raft/state_machine.rs:960, ferrosa-cluster/src/controller/cluster.rs:1467-1472) votes and is counted",
   "Ferrosa.Raft.Obligations", "code: a node whose projection diverged still votes and is counted toward quorum"⟩,
  ⟨"Raft", "code: a restart replays the same failure and the repair position is not durable",
   "The reason a node stopped applying changes lives only in memory, and no repair position is written down. Every restart replays the same failure and any repair in flight starts over.",
   "ferrosa-cluster/src/raft/state_machine.rs:435 is a plain field, absent from SnapshotData (:390-400) and set to None by the constructor (:670), and ferrosa-cluster/src/controller/cluster.rs:1531-1552 repairs only by moving the raft directory aside and taking a fresh snapshot; when that reset fails the node returns without raft (:1588-1594)",
   "Ferrosa.Raft.Obligations", "code: a restart replays the same failure and the repair position is not durable"⟩,
  ⟨"Raft", "the apply latch survives a restart",
   "The flag that stops a node applying changes lives only in memory, so a restart always clears it, and nothing at startup asks whether the reason for the failure is still true.",
   "ferrosa-cluster/src/raft/state_machine.rs:375 is a plain field, absent from SnapshotData (state, last_applied, last_membership), and the constructor at :580 sets it to None; controller/cluster.rs:1449-1453 builds a fresh machine on every boot",
   "Ferrosa.Raft.Obligations", "the apply latch survives a restart"⟩,
  ⟨"Raft", "a restart cannot repeat a persistent projection failure without progress",
   "If the cause of a failed schema write does not clear - a disk below its reserve, a table that cannot be created - the node restarts clean, fails the same entry, and stops again. Repeatedly, with no progress, and with nothing on the node able to tell that apart from a transient fault.",
   "6c270518 made every WarnReplay system-write site fatal (ferrosa-cluster/src/raft/state_machine.rs:2171-2199 returning into :454-464), widening the set of inputs that latch; the precondition is check_write_admission (ferrosa-storage/src/engine.rs:3354-3372, reached from engine.write at :10109) or the fsync at state_machine.rs:2196. Ferrosa/Raft/Startup.lean:restart_cycle_never_passes_a_poisoned_entry is the trace",
   "Ferrosa.Raft.Obligations", "a restart cannot repeat a persistent projection failure without progress"⟩,
  ⟨"Raft", "no durable effect belongs to an entry the published index does not cover",
   "A row can reach the disk before the index it belongs to is published. Playback normally reissues it, but the window is unbounded when the cause of a later failure persists, and it is the raft directory that holds the only record the row could be explained by.",
   "ferrosa-cluster/src/raft/state_machine.rs:2193-2197 forces the commit log to disk inside the job, and finish_pending_apply advances last_applied only when the job's result is polled at :476-477; a death between the two leaves the row durable and unpublished. controller/cluster.rs:1526 then moves the whole raft directory aside, taking the entry with it",
   "Ferrosa.Raft.Obligations", "no durable effect belongs to an entry the published index does not cover"⟩,
  ⟨"Raft", "a restart or a resync repairs an entry whose destructive effect already landed",
   "Dropping a table deletes its rows before the bookkeeping row is written. If that row then fails, the drop is never recorded as applied, and after a restart the node advertises the table again: empty, with every component agreeing, and nothing left that could put the rows back.",
   "apply_command calls engine.unregister_table inside the staged job (ferrosa-cluster/src/raft/state_machine.rs:2334-2342 -> ferrosa-storage/src/engine.rs:5865-5899, :6020-6059), which records a durable drop in dropped-tables.json and deletes the SSTable directory, before flush_pending_system_writes at :2171 can fail; the snapshot restored on the next boot still describes the table and apply_snapshot merges additively (ferrosa-schema/src/registry.rs:346-364)",
   "Ferrosa.Raft.Obligations", "a restart or a resync repairs an entry whose destructive effect already landed"⟩,
  ⟨"Raft", "the projection orders two writes by the log, not by the wall clock",
   "Rows written to the permission and catalog tables are ordered by the wall clock rather than by the log. Two writes inside the same microsecond resolve to a delete, so a grant that follows a revoke can be masked and a role can lose permissions that it, and every peer, still holds.",
   "1a89858a removed the tombstone-then-rewrite pair within one entry (ferrosa-cluster/src/raft/state_machine.rs:1892-1939), but grant_to_row still stamps rows with now_micros() (ferrosa-schema/src/system/persistence.rs:417-432) and PermissionRevoked stamps its tombstone the same way (ferrosa-cluster/src/system_table_writer.rs:164-181); reconcile_ref gives an equal-timestamp tie to the tombstone (ferrosa-common/src/complex_cell.rs:65-75) and SystemTime is not monotone",
   "Ferrosa.Raft.Obligations", "the projection orders two writes by the log, not by the wall clock"⟩,
  ⟨"Raft", "the durable snapshot on disk never moves backwards",
   "A node that finishes building an old snapshot after it has installed a newer one writes the old one over the newer one on disk. The two durable facts then disagree, so the node refuses to start, throws away its raft log and has to be rebuilt from a peer.",
   "ferrosa-cluster/src/raft/state_machine.rs:2218 discards the boolean publish_snapshot returns when it refuses to replace a newer snapshot, and :2225 then persists snapshot_path unconditionally, so a builder taken at index 5 that finishes after install_snapshot at 10 writes 5 over 10 while mark_durably_applied (:2229) stays at 10 because it is a fetch_max; the guard is only tested with snapshot_path None (the test at :4501 builds on FerrosStateMachine::new())",
   "Ferrosa.Raft.Obligations", "the durable snapshot on disk never moves backwards"⟩,
  ⟨"Raft", "a snapshot is restored as a whole or the node does not boot on it",
   "If restoring the saved snapshot fails part-way, the node logs a warning and starts anyway, with the raft state at the snapshot's index and the schema it serves from unchanged. Tables the cluster believes exist cannot be resolved on that node until it is restarted.",
   "ferrosa-cluster/src/raft/state_machine.rs:838-839 adopt data.state and meta.last_log_id before sync_schema_and_engine_from_state can fail at :841-844, and ferrosa-cluster/src/controller/cluster.rs:1459-1465 downgrades recover_from_persisted_snapshot's error to tracing::warn! and boots; the retirement of superseded table incarnations at :1001-1005 runs before the apply_snapshot at :1061 that can still fail",
   "Ferrosa.Raft.Obligations", "a snapshot is restored as a whole or the node does not boot on it"⟩,
  ⟨"Raft", "a failed local side effect is repaired without a restart",
   "When registering a table with the local storage engine fails during an apply, the cluster keeps the table and only that node cannot serve it, and nothing retries until it restarts. A client that retries sees a table that exists everywhere except there.",
   "ferrosa-cluster/src/raft/state_machine.rs:1412 inserts the table into state and :1436-1443 pushes ApplyError::EngineRegisterTable when register_table fails, and :2142-2155 folds the collected errors into RaftResponse::Error, a value nothing replays, while the staged state publishes at :466-467; the registration at :1435-1444 is unconditional and so re-runs on replay, but a replay needs a restart or an install (install_snapshot at :2382), and the design is pinned in place by apply_command_propagates_engine_register_failure (:5148, 'committed failed intent remains for replay')",
   "Ferrosa.Raft.Obligations", "a failed local side effect is repaired without a restart"⟩,
  ⟨"Projection", "a persisted cell converges to the in-memory post-state",
   "Two permission changes can land in the same microsecond. The stored row is then ordered by which value sorts higher rather than by which change came second, so a restart can read back a permission set the cluster already replaced.",
   "grant_to_row stamps rows with now_micros() (ferrosa-schema/src/system/persistence.rs:417-432) and PermissionRevoked does the same (ferrosa-cluster/src/system_table_writer.rs:164-181); cell_supersedes keeps the higher timestamp and, on a tie, the lexicographically greater payload (ferrosa-storage/src/merge.rs:137-190), and the payload is serde_json of a HashSet-derived Vec (persistence.rs:421-423), so two live writes to one cell in one microsecond resolve to whichever payload sorts higher, not to the later write; load_role_permissions reads it back at boot (ferrosa-cluster/src/system_table_loader.rs:320-371)",
   "Ferrosa.SystemTables.Projection",
   "a cell's persisted row converges to its in-memory value under the code's merge rule"⟩,
  ⟨"Projection", "the projection orders two writes by the log, not by the wall clock",
   "Stored rows are ordered by the machine's wall clock rather than by the order the cluster applied them. If the clock steps backwards, a restart restores the value a later change had already superseded.",
   "no row carries a sequence, generation or log index; the only version is the cell timestamp set by now_micros() at projection time (ferrosa-schema/src/system/persistence.rs:417-432), so a later apply whose SystemTime is smaller than the write it supersedes loses under cell_supersedes (ferrosa-storage/src/merge.rs:177-183)",
   "Ferrosa.SystemTables.Projection",
   "a timestamp with a per-cell sequence converges for every clock"⟩,
  ⟨"Projection", "a later write wins an equal timestamp",
   "When two writes carry the same timestamp the stored row keeps the larger value rather than the later one. Removing a permission can leave a value that sorts below the set it shortened, so the removal can lose even though it was applied later.",
   "cell_supersedes compares (timestamp, has_value, value_bytes, local_deletion_time) (ferrosa-storage/src/merge.rs:177-190); with the timestamp equal the payload decides, so the lexicographically smaller later row loses. That payload is serde_json of a HashSet-derived Vec (ferrosa-schema/src/system/persistence.rs:421-423), so its order is the hash iteration order and which side is greater is not a function of the permission sets. The apply arms emit one mutation per cell (ferrosa-cluster/src/raft/state_machine.rs:1883-1947) but two applies can share a microsecond",
   "Ferrosa.SystemTables.Projection",
   "two writes to one cell with equal timestamps: the later one wins"⟩,
  ⟨"Projection", "a revoked permission is not resurrected by a restart",
   "A permission removed by a later change can be present again after a restart. The earlier row can carry a value that sorts higher and win the tie, so a role has access the cluster removed.",
   "the later revoke writes the remaining set and the earlier grant wrote the larger set; at an equal timestamp cell_supersedes keeps the greater payload (ferrosa-storage/src/merge.rs:177-190), which can be the earlier one because the payload's order is the set's hash order (ferrosa-schema/src/system/persistence.rs:421-423), and load_role_permissions replays it on the next boot (ferrosa-cluster/src/system_table_loader.rs:320-371)",
   "Ferrosa.SystemTables.Projection",
   "a revoked permission is not resurrected by an equal-timestamp write"⟩,
  ⟨"Projection", "a cell re-granted after a full revoke is visible again",
   "Removing the last permission on a resource writes a deletion marker that a later grant never clears. Until compaction purges it, a restart reads the resource as having no permissions at all.",
   "PermissionRevoked writes a row-level deletion (ferrosa-cluster/src/system_table_writer.rs:164-181); load_role_permissions skips any row whose merged deletion is not live (ferrosa-cluster/src/system_table_loader.rs:333-335); merge_rows_into keeps the maximum deletion and merge_into_existing_row only raises it (ferrosa-storage/src/merge.rs:137-143, ferrosa-storage/src/memtable/sharded.rs:549), so a later grant never clears the marker until compaction purges it after gc_grace (ferrosa-storage/src/compaction/purge.rs:119-153); ferrosa-cluster/src/ddl_path.rs:444-467 writes the same tombstone for a partial revoke in direct mode",
   "Ferrosa.SystemTables.Projection",
   "a cell re-granted after a full revoke is visible again"⟩,
  ⟨"Projection", "convergence does not rest on a caller convention",
   "The stored rows have no sequence of their own, so correctness rests on the caller writing each cell once per apply and on the clock moving forward. A new mutation that writes a cell twice breaks that with nothing detecting it.",
   "the apply arms happen to emit one mutation per cell (ferrosa-cluster/src/raft/state_machine.rs:1883-1947) but system_table_writer.rs holds no per-cell guard or version, and a REVOKE emits one Raft operation per permission (ferrosa-cql/src/router.rs:14123-14167), so one statement writes the same cell in consecutive applies",
   "Ferrosa.SystemTables.Projection",
   "convergence does not rest on a caller convention"⟩,
  ⟨"Projection", "the write path and the read path resolve a tie the same way",
   "The in-memory and on-disk merge paths choose opposite winners when timestamps are equal. What a restart reads can depend on whether the row had already been flushed.",
   "reconcile_ref gives an equal-timestamp tie to the tombstone (ferrosa-common/src/complex_cell.rs:63-77, called from ferrosa-storage/src/memtable/sharded.rs:556-585) while cell_supersedes gives it to the live value (ferrosa-storage/src/merge.rs:177-190); the same pair of versions resolves to opposite winners in the memtable and on the read/compaction merge",
   "Ferrosa.SystemTables.Projection",
   "the write path and the read path agree on an equal-timestamp tie"⟩,
  ⟨"Streaming", "no durable artifact changes under a reader",
   "The commit log rewrites eight bytes of a segment that is already on disk. A reader tailing that segment can see the same position change from end-of-log to a pointer at the next section, and a replay that read the old value stops early.",
   "repair_marker_in_place pwrites the previous sync marker with write_all_at (ferrosa-storage/src/commitlog/segment.rs:226-247) whenever the marker sits below the flush watermark, reached at :898 and :931; marker_patch_to_rewrite (:672-684) is the predicate that selects an already-durable range",
   "Ferrosa.Streaming.Audit", "no durable artifact changes under a reader"⟩,
  ⟨"Streaming", "an operator that spills does not re-collect its result",
   "A graph ORDER BY sorts through a disk-backed spill and then loads every sorted row back into one vector. Peak memory is the result set, so a ten-row answer still holds every candidate row.",
   "SpillSortSink::finish_rows allocates Vec::with_capacity(rows_pushed) and drains the merged stream into it (ferrosa-graph/src/executor/spill.rs:235-250), while the streaming alternative finish_stream (:268) is what Bolt, leapfrog and expand use",
   "Ferrosa.Streaming.Audit", "an operator that spills does not re-collect its result"⟩,
  ⟨"Streaming", "a spilling sort's input is not already a vector of every row",
   "Variable-length path expansion collects one row for every path before it sorts anything. The spill bounds the sort's own buffer and leaves the input list untouched, so the query still holds every path it found.",
   "rows.push(row) per expanded path at ferrosa-graph/src/executor/varpath.rs:437, then spill_order_by at :445 drains that vector into the sink; when the sink declines, sort_rows at :450 sorts the same vector in memory",
   "Ferrosa.Streaming.Audit", "a spilling sort's input is not already a vector of every row"⟩,
  ⟨"Streaming", "a system-table load does not hold the whole table",
   "The boot loader reads system tables one partition at a time and keeps them all. A cluster with a large role or grant table needs memory proportional to that table before it can serve a query.",
   "SystemTableLoader::walk_all_partitions streams the merged view (ferrosa-cluster/src/system_table_loader.rs:78-85), but load_roles pushes every partition (:172) and load_role_permissions pushes every grant (:370); the capped read this replaced was removed because it dropped the tail",
   "Ferrosa.Streaming.Audit", "a system-table load does not hold the whole table"⟩,
  ⟨"Streaming", "a snapshot restore does not read the whole artifact into memory",
   "Restoring the saved snapshot reads the whole file and then decodes a second copy of it. A node with a large catalog needs both in memory before it can apply either.",
   "recover_from_persisted_snapshot does std::fs::read(&path) and bincode::deserialize (ferrosa-cluster/src/raft/state_machine.rs:1376-1379) before apply_snapshot_data_from, with no streaming decode or chunked apply",
   "Ferrosa.Streaming.Audit", "a snapshot restore does not read the whole artifact into memory"⟩,
  ⟨"Streaming", "a snapshot build does not copy its payload once per publisher",
   "Building a snapshot copies the cluster state, encodes it, and then copies the encoded bytes again for the publish slot and again for the disk write. Three copies of the same state are live at once.",
   "build_snapshot clones state (ferrosa-cluster/src/raft/state_machine.rs:2531-2534), encodes it at :2546, then calls publish_snapshot with bytes.clone() at :2567 and clones again for the blocking disk job at :2571",
   "Ferrosa.Streaming.Audit", "a snapshot build does not copy its payload once per publisher"⟩,
  ⟨"Streaming", "the spill threshold is the process bound, not each operator's share of it",
   "Each query sizes its spill threshold at half the process memory budget. Two such queries hold the whole budget and a third has nothing left, so a bound that looks like half is really twice the budget.",
   "SpillCtx::default() is constructed per call (ferrosa-sql/src/plan.rs:255, ferrosa-postgres/src/result_stream.rs:211) at process_spill_threshold_bytes, which is half the detected budget (ferrosa-storage/src/spill_budget.rs:211-238) with no divisor for concurrency",
   "Ferrosa.Streaming.Audit", "the spill threshold is the process bound, not each operator's share of it"⟩,
  ⟨"Streaming", "an ORDER BY that cannot spill refuses instead of sorting in memory",
   "An ORDER BY whose sort term is not a projected column silently sorts in memory instead of refusing. The query answers correctly and holds every row, and nothing reports that the spill was skipped.",
   "SpillSortSink::try_new returns None when a term is not a projected column (ferrosa-graph/src/executor/spill.rs:197-207) and spill_order_by returns false (:305-312), after which varpath sorts the accumulated vector in memory (ferrosa-graph/src/executor/varpath.rs:450)",
   "Ferrosa.Streaming.Audit", "an ORDER BY that cannot spill refuses instead of sorting in memory"⟩,
  ⟨"Streaming", "blocking work is queued against a capacity",
   "A snapshot's bytes are handed to a blocking pool whose queue has no limit. If the pool is busy the payload waits in that queue, holding its memory, and nothing bounds how many wait.",
   "TaskPool::spawn_blocking forwards to tokio's blocking pool (ferrosa-common/src/task_pool.rs:60-68), whose queue is unbounded, and the closure owns a full snapshot byte vector cloned at ferrosa-cluster/src/raft/state_machine.rs:2571",
   "Ferrosa.Streaming.Audit", "blocking work is queued against a capacity"⟩,
  ⟨"Codec", "every path hands the pipeline one canonical value type",
   "The value a query path produces is the on-disk format's own type, defined inside the storage-format crate. A change to how data is stored therefore changes the type every front end passes around, so the change cannot be confined to the file-format code.",
   "ferrosa-sstable/src/types.rs:1-14 declares the type format-specific and format-shaped; 540 references to it from 101 shipping files in 12 crates (ferrosa-storage 236, ferrosa-cluster 161, ferrosa-cql 58, ferrosa-postgres 27, ferrosa-graph 23, and seven smaller holders), and 210 direct CellValue constructions in 41 shipping files bypass the one builder at ferrosa-row-bridge/src/row.rs:804",
   "Ferrosa.Codec.Audit", "every path hands the pipeline one canonical value type"⟩,
  ⟨"Codec", "on-disk layout knowledge is confined to the codec module",
   "The names of the files a stored table is made of are written out again in four other components instead of being taken from the storage-format crate that defines them. Renaming or replacing a file layout is therefore a change in four components rather than in one.",
   "437 literal component filenames in 21 shipping files (ferrosa-storage 365, ferrosa-cluster 43, ferrosa-ctl 22, ferrosa-index-builder 7) while the codec exports them as toc::DATA and siblings (ferrosa-sstable/src/toc.rs:16-37); ferrosa-storage/src/flush.rs:123-129 additionally re-derives the Statistics header's local-deletion-time width and sentinel",
   "Ferrosa.Codec.Audit", "on-disk layout knowledge is confined to the codec module"⟩,
  ⟨"Codec", "the canonical form is a function of the request, not of the writer's clock",
   "A delete, and a value written with an expiry, record the time they were processed rather than a time the write carries. Running the same statement twice therefore stores two different values, and a retry can decide which of two writes wins.",
   "ferrosa-row-bridge/src/row.rs:815-817 stamps a null's local deletion time from SystemTime::now() and :826-830 computes an expiry as now + ttl; ferrosa-storage/src/merge.rs:177-190 compares that field on the equal-timestamp path, so which of two writes wins can differ between two executions of one statement",
   "Ferrosa.Codec.Audit", "the canonical form is a function of the request, not of the writer's clock"⟩,
  ⟨"Codec", "a deletion time is computed the same, and width-safe, on every path",
   "One internal writer computes an expiry by dividing a microsecond timestamp straight into a signed 32-bit second count with no bound, while two other paths clamp the same quantity. Past 2038 a cell written by the first reads as already expired.",
   "ferrosa-cluster/src/telemetry/writer.rs:131 casts (ts / 1_000_000) as i32 with no clamp; ferrosa-graph/src/executor/expand.rs:6082 and ferrosa-graph/src/adjacency/reconcile.rs:866 clamp the same quantity into u32",
   "Ferrosa.Codec.Audit", "a deletion time is computed the same, and width-safe, on every path"⟩,
  ⟨"Codec", "an artifact records which format version wrote it",
   "A stored table does not record which format wrote it. A reader decides how to parse one from which files happen to be present, so a later format that reuses a file name is read as the current one instead of being refused.",
   "ferrosa-sstable/src/reader.rs opens a table from component handles and checks no version; ferrosa-sstable/src/statistics.rs:147-172 has no version field; ferrosa-sstable/src/toc.rs:16-37 lists filenames only",
   "Ferrosa.Codec.Audit", "an artifact records which format version wrote it"⟩,
  ⟨"Ownership", "a replication send copies the mutation once instead of encoding it and moving it on",
   "A replicated write is duplicated in full before it is sent. The copy exists because the local apply needs to own the data while the send still needs it, but the send only reads it to encode, so the copy could be an encode followed by a move.",
   "ferrosa-cluster/src/pair/coordinator.rs:122 clones the Mutation for apply_locally_async while replicate_to_peer at :181 only borrows it to encode; ferrosa-cluster/src/pair/handler.rs:47 clones again on the primary's forward path",
   "Ferrosa.Ownership.Obligations", "a replication send does not copy the mutation it is about to encode"⟩,
  ⟨"Ownership", "a snapshot is handed to each follower as a shared payload",
   "Every follower that asks for the current snapshot is given its own copy of it. A node serving several lagging followers holds the whole snapshot once per follower, so catching up one more node costs one more full copy on the leader.",
   "ferrosa-cluster/src/raft/state_machine.rs:599 returns a cloned Vec<u8> from the published slot, and :2855 hands that to every get_current_snapshot caller",
   "Ferrosa.Ownership.Obligations", "a snapshot is served to every follower through a handle rather than a copy"⟩,
  ⟨"Ownership", "building a snapshot does not duplicate the state it encodes",
   "A snapshot build duplicates the whole catalogue state and then serialises the duplicate. The serialiser only reads it, so the copy costs a second full catalogue for the duration of every snapshot.",
   "ferrosa-cluster/src/raft/state_machine.rs:2541 clones RaftState into SnapshotData although encode_snapshot takes a reference, and :248 clones the bootstrap map a second time",
   "Ferrosa.Ownership.Obligations", "a snapshot build does not duplicate the state it is about to encode"⟩,
  ⟨"Ownership", "the encoded snapshot is held once during a build",
   "While a snapshot is being written, the encoded bytes are held three times at once: the original, the copy published for replication, and the copy the disk write owns. Peak memory for a build is three times the snapshot.",
   "ferrosa-cluster/src/raft/state_machine.rs:2567 clones the bytes into the published slot and :2571 clones them again for the blocking disk write, while the original is moved into the returned cursor",
   "Ferrosa.Ownership.Obligations", "the encoded snapshot is not resident three times during a build"⟩,
  ⟨"Ownership", "a write handed to an observer does not duplicate its row",
   "When an observer is registered, every write duplicates the row before storing it so that the observer and the memtable can both read it. The duplication is per write, on the hot path, and it copies the row's metadata as well as its cells.",
   "ferrosa-storage/src/engine.rs:10203 builds the observer Mutation from vec![row.clone()] and then moves the row into the memtable; the batch path does the same at :10325",
   "Ferrosa.Ownership.Obligations", "a write that hands a row to an observer does not give the observer a second payload"⟩,
  ⟨"Ownership", "an observer's derived writes are moved, not copied",
   "Observer-generated writes are duplicated row by row into the target table even though the write they came from is finished with afterwards. Every derived row is copied once more than it needs to be.",
   "ferrosa-storage/src/engine.rs:9651 and :9698 loop over &dm.rows and clone each row into the target memtable, while dm is owned and dropped at the end of the loop",
   "Ferrosa.Ownership.Obligations", "an observer drain moves rows out of the mutation it owns"⟩
]

/-- Fifty-seven built-and-wrong items, pinned so a defect cannot be dropped from the list without the
corpus classification breaking. The sysfail row left for `Unimplemented.discharged` when `6c270518`
closed it; six rows joined when startup, replay and the grant projection were audited, seven when
the system-table projection layer was modelled, nine when the memory-discipline machine was audited
against the read and transform paths, five when the canonical-form and codec-seam audit found that
the pipeline's value type and the file layout both live in four crates it should not, and six when
the ownership machine was audited against the peer send, snapshot and observer paths. -/
theorem broken_has_fifty_seven_items : broken.length = 57 := by native_decide

/-- The surfaces are where the damage concentrates: five of the thirty-one are SQL or SPARQL, which
    are the two places where a client is most likely to be told something confidently wrong. -/
theorem the_surfaces_carry_five : (broken.filter fun i => i.area == "SQL").length
    + (broken.filter fun i => i.area == "SPARQL").length = 5 := by native_decide

/-- Five of the thirty-one could not be anchored to the shipping code: four describe an
    implementation that does the correct thing at this revision (with tests pinning it) and one is
    stale because the fix it says is unmerged has been merged since. Counted, so a later edit cannot
    quietly turn a model-only hazard back into an assertion about code. -/
theorem five_rows_have_no_verified_site :
    (broken.filter fun i => i.site.startsWith "unverified:").length = 5 := by native_decide

/-- The remainder - the rows that name a real location — are what the list's opening claim is about:
    code that was read, not inferred. -/
theorem fifty_two_rows_name_a_site :
    (broken.filter fun i => !i.site.startsWith "unverified:").length = 52 := by native_decide

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
