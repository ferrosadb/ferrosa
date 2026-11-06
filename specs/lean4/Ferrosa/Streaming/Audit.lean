/-
  The audit: which code paths bound their memory, and which only say they do
  ==========================================================================

  The machine says what a bounded read is. This module says which sites in the tree are one, and the
  answer is not two-valued. A site is `streaming` when it is bounded by construction, `serialized` when
  it is bounded because only a fixed number run at once, `spilling` when it is bounded because it writes
  runs to disk, `boundedByConfig` when it is bounded only because a setting happens to be small,
  `unbounded` when it is bounded by nothing, and `unresolved` when there is no code path to inspect at
  all. The middle three are where the incidents live: a knob that can be raised, a spill that a caller
  can defeat, a limit that only applies to one surface.

  Every row names a file and line read at this revision. A row is not a verdict on the module that holds
  it — `ferrosa-sstable` is streaming in three places and unbounded in one — and the two `unbounded` rows
  that sit behind a spill implementation record that fact separately, because a spill that exists and is
  not reached is not a bound.

  The ledger below is the area's claim list: the obligations the machine discharges, and the code gaps
  as reds. A red is not an opinion about the site; each one names the theorem that exhibits the shape
  and a location that can be opened.
-/
import Ferrosa.Ledger
import Ferrosa.Streaming.Obligations

namespace Ferrosa.Streaming

open Ferrosa.Ledger

/-! ## The classes, and why the middle ones are the point

Six classes, not two. `boundedByConfig` and `spilling` both hold "bounded" in a comment and both stop
holding it under a condition a reviewer has to check: the knob, and whether the spill is reached with
the result still streaming. `unresolved` is here because a site nobody has looked at is not a pass.
-/

/-- How a site's resident memory is bounded. -/
inductive Class where
  /-- Bounded by construction: one chunk or one partition at a time. -/
  | streaming
  /-- Bounded because a permit admits only a fixed number at once. -/
  | serialized
  /-- Bounded because the operator spills past a threshold. `reachable` says whether the executor
      actually calls the spill for this shape. -/
  | spilling (reachable : Bool)
  /-- Bounded only because a setting is small. `knob` names the setting. -/
  | boundedByConfig (knob : String)
  /-- Bounded by nothing in the dataset size. `spillExists` records whether a spill implementation sits
      on the same path, because that is the difference between "no fix started" and "a fix that does not
      bind". -/
  | unbounded (spillExists : Bool)
  /-- No code path to inspect: the guarantee is not made anywhere, so it is not met. -/
  | unresolved
  deriving Repr, DecidableEq

/-- One audited site: the layer, the claim it would have to meet, and where it was read. -/
structure Site where
  /-- The crate or layer, as the report groups them. -/
  layer : String
  /-- What the site is claimed to do about memory. -/
  what : String
  /-- Which class it is in. -/
  klass : Class
  /-- `file:line`, as read at this revision. -/
  site : String
  /-- The engineering evidence: what the code does, in one sentence a reviewer can check. -/
  note : String

/-- The sites read for this audit. -/
def sites : List Site := [
  ⟨"commit log", "the segment reader allocates one entry and no more", .streaming,
   "ferrosa-storage/src/commitlog/reader.rs:182",
   "`next_entry_bounded` reads the 8-byte header, checks the size CRC, rejects a payload above the caller's maximum before allocating, and reads exactly that payload."⟩,
  ⟨"sstable", "a compressed chunk decodes into the caller's buffer", .streaming,
   "ferrosa-sstable/src/reader.rs:256",
   "`decompress_into(payload, chunk)` writes into a buffer resized to the index's `chunk_length`, with the LZ4 declared length and the stored CRC checked first."⟩,
  ⟨"sstable", "skipping a cell does not read its value", .streaming,
   "ferrosa-sstable/src/data.rs:2029",
   "`read_cell_skip` advances `pos` past each field and checks the length against `MAX_VALUE_LEN`, allocating nothing."⟩,
  ⟨"compaction", "the merge holds one row per input, not per output", .streaming,
   "ferrosa-storage/src/compaction/stream.rs:585",
   "`compact` sizes its source and merge vectors from `readers.len()` and copies one candidate row at a time through a caller buffer."⟩,
  ⟨"snapshot", "a snapshot is published by write, sync, rename, sync", .streaming,
   "ferrosa-cluster/src/raft/state_machine.rs:1315",
   "`persist_snapshot_to_disk` writes a staging file, fsyncs it, renames it over the path, then fsyncs the directory; a prior snapshot is validated before it is skipped."⟩,
  ⟨"system tables", "the system-table walk visits one partition at a time", .streaming,
   "ferrosa-cluster/src/system_table_loader.rs:78",
   "`walk_all_partitions` delegates to `walk_token_range`, which streams the merged memtable and SSTable view."⟩,
  ⟨"cql", "a result cursor holds a node-wide permit or is refused", .serialized,
   "ferrosa-cql/src/result_cursor.rs:513",
   "`admit` CAS-increments the open count under `max_open` (default 256), sweeps expired cursors first, and returns Overloaded rather than queueing."⟩,
  ⟨"postgres", "a suspended portal holds a connection and node slot or is refused", .serialized,
   "ferrosa-postgres/src/portal_limits.rs:221",
   "64 suspended portals per connection and 2048 per node by default, refused with SQLSTATE 53000 and counted in a refusal metric."⟩,
  ⟨"compaction", "concurrent compactions are capped", .serialized,
   "ferrosa-storage/src/compaction/executor.rs:141",
   "`FERROSA_MAX_CONCURRENT_COMPACTIONS` defaults to an auto-tune over CPU count and memory budget, clamped to at least one."⟩,
  ⟨"spill", "an ORDER BY sort spills at a byte threshold and merges with bounded fan-in", .spilling true,
   "ferrosa-storage/src/external_sort.rs:283",
   "`push` spills the buffer at `threshold_bytes`, `finish` cascade-merges to `MERGE_FANIN`, and every I/O error is propagated with `?`."⟩,
  ⟨"sql", "sort, aggregate, dedup and join all sort through the spilling sorter", .spilling true,
   "ferrosa-sql/src/exec.rs:115",
   "`SpillSort::new` at exec.rs:115, 558, 610, 766 and 788, so the blocking operators have no in-memory path left."⟩,
  ⟨"graph", "a spilled ORDER BY can be consumed as a stream", .spilling true,
   "ferrosa-graph/src/executor/spill.rs:268",
   "`finish_stream` moves the reservation into a row stream; Bolt (bolt/server.rs:958), leapfrog (leapfrog.rs:324) and expand (expand.rs:1591) use it."⟩,
  ⟨"commit log", "replay reads one segment at a time", .boundedByConfig "commit log segment size",
   "ferrosa-storage/src/commitlog/mod.rs:879",
   "`read_all` loads one segment per loop iteration and drops it before the next; raising the segment size raises the resident set one-for-one."⟩,
  ⟨"commit log", "each open segment owns one preallocated buffer", .boundedByConfig "commit log segment size",
   "ferrosa-storage/src/commitlog/segment.rs:279",
   "`buffer: UnsafeCell<Vec<u8>>` is sized to the segment capacity; the resident set is that size times the number of open segments."⟩,
  ⟨"sstable", "a single cell is capped at 256 MiB", .boundedByConfig "MAX_VALUE_LEN (compile-time)",
   "ferrosa-sstable/src/data.rs:37",
   "`MAX_VALUE_LEN` bounds a value read and skipped at 256 MiB; it is a constant, so raising it is a recompile rather than a setting."⟩,
  ⟨"flush", "a flush holds the whole memtable as one sorted vector", .boundedByConfig "memtable flush threshold",
   "ferrosa-storage/src/flush.rs:243",
   "`split_sorted_partitions_into_shards` takes `Vec<Partition>` and the caller sorts it, so a flush is resident at the memtable's size, times flush pool width."⟩,
  ⟨"storage", "a full-range scan copies the in-range active memtable", .boundedByConfig "memtable flush threshold",
   "ferrosa-storage/src/store.rs:7606",
   "`mem_active.push(Arc::new(p.clone()))` clones every in-range partition per scan; the bound is the memtable, multiplied by concurrent scans because this path holds no permit."⟩,
  ⟨"storage", "a token-range read holds at most its limit", .boundedByConfig "the query's LIMIT",
   "ferrosa-storage/src/store.rs:7436",
   "`while matched.len() < limit` stops the scan at the caller's cap, so the resident set is the limit and not the range."⟩,
  ⟨"cql", "a scan page is 5000 rows", .boundedByConfig "FERROSA_CQL_DEFAULT_PAGE_SIZE",
   "ferrosa-cql/src/paging.rs:256",
   "`DEFAULT_SCAN_PAGE_SIZE` is a row count, so the bytes it holds move with row width: about 5 MB narrow and 5 GB at 1 MiB per row."⟩,
  ⟨"graph", "a spilled ORDER BY collects its result back into one vector", .unbounded true,
   "ferrosa-graph/src/executor/spill.rs:242",
   "`finish_rows` allocates `Vec::with_capacity(rows_pushed)` and drains the merged stream into it, so peak memory is the result however well the sort spilled."⟩,
  ⟨"graph", "path expansion does not hold every path before the sort", .unbounded true,
   "ferrosa-graph/src/executor/varpath.rs:437",
   "`rows.push(row)` accumulates one row per expanded path, then `spill_order_by` drains that vector; the spill bounds the sort buffer, not the input."⟩,
  ⟨"sql", "the spill threshold is the process bound", .unbounded true,
   "ferrosa-sql/src/spill.rs:414",
   "`SpillCtx::default()` is built per call (plan.rs:255, postgres/result_stream.rs:211) at `process_spill_threshold_bytes()`, which is half the detected budget with no divisor for concurrency."⟩,
  ⟨"system tables", "the role and grant loaders do not hold the whole table", .unbounded false,
   "ferrosa-cluster/src/system_table_loader.rs:172",
   "`roles.push` (172) and `grants.push` (370) accumulate every partition the streaming walk visits; the cap that used to limit this was removed because it dropped the tail."⟩,
  ⟨"snapshot", "a snapshot build does not clone its payload per publisher", .unbounded false,
   "ferrosa-cluster/src/raft/state_machine.rs:2531",
   "`state.clone()` then `encode_snapshot` then `bytes.clone()` for the publish slot and again for the disk job: three copies of cluster state live at once."⟩,
  ⟨"snapshot", "a restore does not read the whole snapshot into memory", .unbounded false,
   "ferrosa-cluster/src/raft/state_machine.rs:1376",
   "`std::fs::read(&path)` then `bincode::deserialize` holds the file and the decoded value together; nothing streams the apply."⟩,
  ⟨"sstable", "the vector-returning decoder is off the serving path", .unbounded false,
   "ferrosa-sstable/src/compression.rs:271",
   "`decompress` ignores its declared length and returns a fresh Vec; on the read path it is replaced by `decompress_into`, and its remaining callers are writer read-back (writer.rs:4230) and tests."⟩,
  ⟨"permits", "blocking work is queued against a capacity", .unbounded false,
   "ferrosa-common/src/task_pool.rs:60",
   "`spawn_blocking` submits to tokio's blocking pool, whose queue is unbounded; a queued closure keeps its owned payload, e.g. the snapshot bytes cloned at state_machine.rs:2571."⟩,
  ⟨"spill", "a spill reserves disk space before it writes", .unresolved,
   "ferrosa-storage/src/engine.rs:5305",
   "`reserve_query_temp_table` creates the directory and returns a guard; the only free-space check is `check_write_admission` (engine.rs:3354), which no spill path calls."⟩,
  ⟨"spill", "spill files land on a filesystem that is not memory", .unresolved,
   "ferrosa-sql/src/spill.rs:466",
   "`default_temp_root` is `$FERROSA_SQL_TEMP_DIR` or `std::env::temp_dir()/ferrosa-sql`; nothing checks whether that path is tmpfs."⟩,
  ⟨"spill", "orphaned spill directories are reconciled at startup", .unresolved,
   "ferrosa-sql/src/spill.rs:475",
   "`sweep_orphans_once` covers only the SQL root; the engine's own reservations under `<data_dir>/tmp_order_by_sort` and `tmp_distinct` rely on `Drop` and are never swept."⟩,
  ⟨"permits", "a node admits memory-bearing work through one permit", .unresolved,
   "ferrosa-cql/src/connection.rs:391",
   "There are three independent limits (connection.rs:391, result_cursor.rs:513, portal_limits.rs:221) and no shared pool; Raft's `physical_apply.rs:45` counts active jobs without limiting them."⟩,
  ⟨"snapshot", "a PITR rewrap publishes a new version atomically", .unresolved,
   "ferrosa-storage/src/engine.rs:26579",
   "No rewrap operator exists in the tree: the only PITR path is `run_pitr_replay_e2e`, a replay test, so there is no transform to inspect."⟩
]

/-- Thirty-two sites were read. -/
theorem site_count : sites.length = 32 := by decide

/-- **Every site names a file and line.**  A row without a location is a mood, not an audit finding, so
    each `site` field has to contain a `file.rs:line`. -/
theorem every_site_names_a_location :
    sites.all (fun s => (s.site.splitOn ".rs:").length > 1) = true := by native_decide

/-- **Every config-bounded site names its knob.**  "Bounded by configuration" is only useful if the
    setting can be found and raised, so the class carries the name and this check enforces it. -/
theorem every_config_bound_names_its_knob :
    sites.all (fun s => match s.klass with
      | .boundedByConfig knob => !knob.isEmpty
      | _ => true) = true := by native_decide

/-- Six sites are bounded by construction. -/
theorem streaming_sites : (sites.filter fun s => s.klass == .streaming).length = 6 := by decide

/-- Three are bounded by a permit. -/
theorem serialized_sites : (sites.filter fun s => match s.klass with
    | .serialized => true | _ => false).length = 3 := by decide

/-- Three are bounded because they spill. -/
theorem spilling_sites : (sites.filter fun s => match s.klass with
    | .spilling _ => true | _ => false).length = 3 := by decide

/-- Seven are bounded only by a setting. -/
theorem config_bounded_sites : (sites.filter fun s => match s.klass with
    | .boundedByConfig _ => true | _ => false).length = 7 := by decide

/-- Eight are bounded by nothing in the dataset size. -/
theorem unbounded_sites : (sites.filter fun s => match s.klass with
    | .unbounded _ => true | _ => false).length = 8 := by decide

/-- Five have no code path at all, and are counted as unresolved rather than passed. -/
theorem unresolved_sites : (sites.filter fun s => s.klass == .unresolved).length = 5 := by decide

/-- **Three of the eight unbounded sites sit behind a spill implementation.**  That is the distinction the
    audit exists to draw: the fix is written, and the path still holds everything, because the spill is
    defeated by a collected result, an already-materialised input, or a per-operator threshold. Counted so
    a future "spilling" label cannot be read as a bound. -/
theorem three_unbounded_sites_have_a_spill :
    (sites.filter fun s => match s.klass with | .unbounded true => true | _ => false).length = 3 := by
  decide

/-- The classes partition the sites: no row is in two, and the counts sum to the list. -/
theorem classes_partition_the_sites :
    (sites.filter (fun s => s.klass == .streaming)).length
      + (sites.filter fun s => match s.klass with | .serialized => true | _ => false).length
      + (sites.filter fun s => match s.klass with | .spilling _ => true | _ => false).length
      + (sites.filter fun s => match s.klass with | .boundedByConfig _ => true | _ => false).length
      + (sites.filter fun s => match s.klass with | .unbounded _ => true | _ => false).length
      + (sites.filter fun s => s.klass == .unresolved).length = 32 := by decide

/-- The layers the audit covers, so the table can be printed by layer. -/
def layers : List String := ["cql", "commit log", "compaction", "flush", "graph", "permits",
  "postgres", "sstable", "snapshot", "spill", "sql", "storage", "system tables"]

/-- The covered layers are distinct. -/
theorem layers_are_distinct : layers.Nodup := by decide

/-! ## The ledger

Twenty-two obligations the machine discharges and fourteen code gaps. The gaps are of two kinds and
they are counted separately: nine are wrong now — a published artifact edited in place, a spill whose
result is collected, a loader that holds its table — and five are unresolved, where the code makes no
such guarantee at all, which is not the same as meeting it.
-/

/-- The area's claims: the obligations, then the sites that do not meet them. -/
def ledger : List SpecItem := [
  ⟨"a read path's resident set is independent of the result size",
   .met "Streaming.a_streaming_reader_holds_the_same_bytes_for_any_result"⟩,
  ⟨"no constant bounds a read that materialises its result",
   .met "Streaming.no_constant_bounds_a_materialising_reader"⟩,
  ⟨"a reader's view is one pinned version for its whole life",
   .met "Streaming.a_reader_view_depends_only_on_its_pin"⟩,
  ⟨"abandoning a reader leaves no work queued and no side effect",
   .met "Streaming.abandoning_a_reader_leaves_no_work"⟩,
  ⟨"a read changes nothing durable",
   .met "Streaming.a_read_changes_nothing"⟩,
  ⟨"publishing a version disturbs no other manifest entry",
   .met "Streaming.publish_moves_one_manifest_entry"⟩,
  ⟨"a transform's output is fresh, so no published artifact is overwritten",
   .met "Streaming.a_stale_output_is_refused"⟩,
  ⟨"a transform's bytes are verified before publication, so no unverified tail is published",
   .met "Streaming.an_unverified_transform_is_refused"⟩,
  ⟨"a failed transform leaves the previously published version readable",
   .met "Streaming.a_failed_transform_leaves_the_manifest"⟩,
  ⟨"a successful transform publishes in one transition",
   .met "Streaming.a_successful_transform_publishes_one_entry"⟩,
  ⟨"an in-place edit would change what a pinned reader sees, which is why it is refused",
   .met "Streaming.a_pinned_view_changes_under_an_in_place_edit"⟩,
  ⟨"at most the permit limit holds a permit at once",
   .met "Streaming.at_most_the_limit_hold_a_permit"⟩,
  ⟨"a refusal is reported as a refusal, never as a completed request",
   .met "Streaming.a_refusal_is_not_a_grant"⟩,
  ⟨"a bounded queue never exceeds its capacity",
   .met "Streaming.a_bounded_queue_stays_within_its_capacity"⟩,
  ⟨"an unbounded queue is a leak with a queue in front of it",
   .met "Streaming.an_unbounded_queue_is_the_leak"⟩,
  ⟨"a spilling operator's resident set is its threshold, not its input",
   .met "Streaming.a_spilling_operator_is_bounded"⟩,
  ⟨"a spill implementation that no executor calls is not a bound",
   .met "Streaming.a_spill_nothing_calls_is_not_a_bound"⟩,
  ⟨"at the threshold the operator's next step is a spill, not a larger buffer",
   .met "Streaming.at_the_bound_the_next_step_is_a_spill"⟩,
  ⟨"a spill whose result is collected does not bound peak memory",
   .met "Streaming.a_collected_result_defeats_the_bound"⟩,
  ⟨"a spill without disk admission fails midway, after the work is done",
   .met "Streaming.an_unadmitted_spill_fails_midway"⟩,
  ⟨"a spill file is invisible to readers and is cleared rather than adopted",
   .met "Streaming.reconciliation_clears_orphans"⟩,
  ⟨"a silent in-memory fallback after a spill failure exceeds the bound",
   .met "Streaming.a_silent_fallback_exceeds_the_bound"⟩,
  ⟨"no durable artifact changes under a reader",
   .failing
     "the commit-log sync marker is rewritten in place: `repair_marker_in_place` pwrites 8 bytes below the flush watermark into a region the previous flush already made durable (segment.rs:231-247, reached at :898 and :931), so a reader of the open segment can see the same offset change from EOF to the next section"
     "Streaming.a_pinned_view_changes_under_an_in_place_edit"⟩,
  ⟨"an operator that spills does not re-collect its result",
   .failing
     "the graph ORDER BY spills correctly and then collects every merged row back with `Vec::with_capacity(rows_pushed)` (graph/executor/spill.rs:242), so peak memory is the result set and the temp-sort threshold bounds nothing"
     "Streaming.a_collected_result_defeats_the_bound"⟩,
  ⟨"a spilling sort's input is not already a vector of every row",
   .failing
     "variable-length path expansion pushes one row per path into `rows` (graph/executor/varpath.rs:437) and only then sorts, spilling or not (varpath.rs:445-451); the spill bounds the sort buffer, not the input"
     "Streaming.a_reordering_stage_without_spill_is_proportional"⟩,
  ⟨"a system-table load does not hold the whole table",
   .failing
     "the loader walks partitions one at a time and pushes every one: `roles.push` at system_table_loader.rs:172 and `grants.push` at :370, which is unbounded in the number of roles and grants"
     "Streaming.no_constant_bounds_a_materialising_reader"⟩,
  ⟨"a snapshot restore does not read the whole artifact into memory",
   .failing
     "`recover_from_persisted_snapshot` does `std::fs::read(&path)` and then `bincode::deserialize` (raft/state_machine.rs:1376), holding the file and the decoded value together with no streaming apply"
     "Streaming.no_constant_bounds_a_materialising_reader"⟩,
  ⟨"a snapshot build does not copy its payload once per publisher",
   .failing
     "`build_snapshot` clones the whole state (state_machine.rs:2533), encodes it (2546), then clones the bytes for the publish slot (2567) and again for the disk job (2571): three copies of cluster state live at once"
     "Streaming.no_constant_bounds_a_materialising_reader"⟩,
  ⟨"the spill threshold is the process bound, not each operator's share of it",
   .failing
     "`SpillCtx::default()` is built per call (sql/plan.rs:255, postgres/result_stream.rs:211) at `process_spill_threshold_bytes()`, which is half the detected budget (storage/spill_budget.rs:232) with no divisor for concurrency, so three operators hold one and a half budgets"
     "Streaming.per_operator_thresholds_multiply"⟩,
  ⟨"an ORDER BY that cannot spill refuses instead of sorting in memory",
   .failing
     "the graph sink returns None when an order term is not a projected column (graph/executor/spill.rs:200-202) and the caller then sorts the whole accumulated vector in memory (varpath.rs:450), so an eligibility check silently widens the bound"
     "Streaming.a_spill_nothing_calls_is_not_a_bound"⟩,
  ⟨"blocking work is queued against a capacity",
   .failing
     "`spawn_blocking` submits to tokio's blocking pool (common/task_pool.rs:60) whose queue has no capacity, and a queued closure keeps its owned payload — the snapshot bytes cloned at state_machine.rs:2571 sit in that queue"
     "Streaming.an_unbounded_queue_is_the_leak"⟩,
  ⟨"a spill reserves disk space before it writes",
   .failing
     "no such guarantee: `reserve_query_temp_table` (storage/engine.rs:5305) creates the directory and returns a guard, and the only free-space check in the engine is `check_write_admission` (:3354) on the write path, so a spill discovers ENOSPC inside the merge"
     "Streaming.an_unadmitted_spill_fails_midway"⟩,
  ⟨"a node admits memory-bearing work through one permit",
   .failing
     "no such guarantee: there are three independent limits (cql/connection.rs:391, cql/result_cursor.rs:513, postgres/portal_limits.rs:221) and no shared pool, and raft's `physical_apply.rs:45 admit` counts active jobs without limiting them, so the node's bound is their sum"
     "Streaming.pool_limits_add"⟩,
  ⟨"orphaned spill directories are reconciled at startup",
   .failing
     "no such guarantee for the engine's roots: `sweep_orphans_once` (sql/spill.rs:475) covers only `$FERROSA_SQL_TEMP_DIR`, and reservations under `<data_dir>/tmp_order_by_sort` and `tmp_distinct` rely on `Drop`, so a crash leaves them until the disk fills"
     "Streaming.reconciliation_clears_orphans"⟩,
  ⟨"spill files land on a filesystem that is not memory",
   .failing
     "no such guarantee: `default_temp_root` is the system temp directory (sql/spill.rs:466-471) and nothing checks whether it is tmpfs, so on a node with a memory-backed /tmp the spill threshold is a RAM threshold"
     "Streaming.a_spill_to_tmpfs_stays_in_memory"⟩,
  ⟨"a PITR rewrap publishes a new version atomically",
   .failing
     "no such operator exists: the only PITR path in the tree is `run_pitr_replay_e2e` (storage/engine.rs:26579), a replay test, so there is no transform to hold to the precondition"
     "Streaming.a_successful_transform_publishes_one_entry"⟩
]

/-- The area records thirty-six obligations of which fourteen are unmet, and both numbers are pinned:
    adding a claim, or closing one, forces the ledger to be updated in the same change. -/
theorem streaming_has_thirty_six_items_and_fourteen_red : ledger.length = 36 ∧ redCount ledger = 14 := by
  decide

def main : IO Unit :=
  printLedger "memory discipline (no materialisation, streaming, no mutation except transforming)" ledger

end Ferrosa.Streaming
