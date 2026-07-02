# Streaming range reads — remove the server-side result cap

**Status:** proposed (design locked; implementation in `feat/stream-range-reads-no-cap`).
**Steps 1–2 and Step 5 (final) landed.**

## Progress — consume-path bounded-streaming refactor (`t_ee98faa0` + `t_3fc6be3c`)

**Landed.** The last Vec-accumulating consume path — `consume_range_stream`'s
`StreamConsumeOutcome.partitions` accumulation, drained by
`coordinate_range_read_stream_limited_rows`'s `all_partitions.extend(...)` +
`dedup_by_token` — is removed. Peak was `O(result)`; at the intentional 2 GiB
node cap this OOM-killed the coordinator on the live `fts_match` content scan and
multi-page projected scans.

- **Bounded streaming consumer** (`ferrosa-cluster/src/coordinator/stream_consumer.rs`):
  new `PartitionSink` trait + `consume_range_stream_into(..)` MOVE each decoded
  partition into the sink one at a time and drop it, so the consumer's resident
  set is `O(chunk)`, not `O(result)`. `ChannelPartitionSink` forwards through a
  bounded `mpsc` (back-pressure). The legacy `consume_range_stream` (Vec) is now a
  thin wrapper over the streaming consumer with an accumulating `VecPartitionSink`,
  kept only for point-bounded callers / tests. Straggler draining and the
  Done/heartbeat/truncated semantics are unchanged.
- **Limited-rows coordinator rewired** (`range_read_stream.rs`,
  `coordinate_range_read_stream_limited_rows`): no longer reads a local Vec + runs
  the accumulating `consume_range_stream` + extends + `dedup_by_token`. It drinks
  the already-bounded, token-deduped, `<= k`-row-fragment N-way merge stream
  (`coordinate_range_read_stream_all_with_projection`, the same path uncapped
  `SELECT *` uses) and folds consecutive same-key fragments into at most `limit`
  WHOLE partitions. Peak resident set is `O(k + caller's own limit)` — never the
  whole table. `row_limit` is applied by the stream. Dead helpers
  `read_local_range_stream_limited_rows` and `dedup_by_token` removed.
- **Memory-bound test flipped RED→GREEN**
  (`ferrosa-cluster/tests/replica_scan_serialization_memory_bound.rs`): the
  consumer test now drives the REAL producer frames into
  `consume_range_stream_into` under a two-phase measurement (produce OUTSIDE the
  window, measure ONLY the consume phase) that isolates the consumer's resident
  set from producer/storage allocation noise. Result: consumer peak
  ~284 KiB @ N=750 vs ~291 KiB @ N=12 000 — **ratio 1.02**, flat and independent
  of N, far under the 32 MiB in-flight budget, with counts == N (no data loss).
  Reverting the consumer to accumulate a Vec makes it RED (peak → `O(result)`).

## Progress — `fts_match` arm coordinator accumulation bounded (`t_ee98faa0`)

**Landed.** The `where_has_fts_match` arm in `ferrosa-cql/src/router.rs` — the
exact query shape that OOM-killed the live cluster
(`… context_snippet = fts_match(<terms>)` over a large `entity_store` via
ferrosa-memory `hybrid_search`) — no longer materializes every matching row
before applying LIMIT. Previously it point-read each matched partition, appended
every surviving row into an unbounded `fts_rows` Vec, and applied `take(limit)`
only AFTER the loop: peak = O(matched rows × row size). (The consume-path fix
above did NOT cover this arm — it point-reads via `write_path.read`, not the
range-scan stream.) ferrosa-memory's `hybrid_search` DOES send a LIMIT
(`LIMIT {k}` with k = `source_limit` clamp ≤ 50 for entity content;
≤ 512 entity-name / ≤ 4096 BM25-segment candidates), so the live OOM was pure
coordinator-side pre-LIMIT accumulation.

- **LIMIT present** — the fetch loop stops point-reading as soon as `limit` rows
  SURVIVE the post-filter (`fts_rows.truncate(limit)` + break; rows dropped by
  non-fts predicates don't count and keep the loop reading). Peak ≈ limit rows +
  one partition; remaining matched partitions are never consulted.
- **No LIMIT** — the full result is legitimate, so MEMORY is bounded, not the
  result: the arm builds one page per response (client `page_size`, or
  `default_scan_page_size` when the client sends none — same policy as the
  unbounded-`SELECT *` streaming fix) and returns a partition-granular
  `PagingState` continuation (resume strictly after the last fully-delivered
  partition key). Pages may exceed `page_size` by the tail of one wide partition
  (`page_size` is a hint per the CQL spec). Peak ≈ one page + one partition.
- **Determinism** — matched partitions are visited in sorted partition-key-byte
  order. CQL promises no ordering for this arm, but the previous
  `HashSet` iteration order silently decided WHICH rows a LIMIT kept
  (observed: `[1, 4, 5, 3, 2, 7, 6]`); the sort makes LIMIT early-exit and page
  cursors reproducible.
- **Semantics preserved** — row retention by FULL doc key (t_da51e20c), the
  legacy-doc-key skip, and post-filter predicates are unchanged; results are
  never truncated server-side (bounded only by the query's own LIMIT). The
  previously-`#[ignore]`d `fts_match_with_pk_predicates_returns_matches`
  (t_8686dd3c) now passes (fixed by the row-granular doc keys) and was
  un-ignored.
- **Known bounded residual** — the FTS hit-set (`matched`) is O(matches) SMALL
  doc keys (keys, not rows), accepted for now.
- **Tests** (`router.rs`, RED→GREEN via a test-only per-thread partition-read
  counter): `fts_match_limit_stops_partition_reads_at_limit` (10 matches,
  LIMIT 3 → exactly 3 reads), `fts_match_limit_post_filtered_rows_do_not_count_toward_limit`
  (dropped rows trigger further reads, exact LIMIT), `fts_match_no_limit_pages_bound_accumulation`
  (pages 3/3/1, union = all rows, no dups), `fts_match_no_limit_returns_all_matching_rows`
  (no server-side truncation).

**Deferred consumers still materializing** (bounded by the caller's own
`limit`/page — not the OOM path, but not yet streaming): the CQL `PartitionKeyLookup`
fallback and empty-index fallback (`router.rs` `range_read_with`), and the SPARQL /
graph executors' `write_path.range_read(..)` callers, still collect the streamed
partitions into a `Vec` at the WritePath boundary. These call `range_read_stream_all_with`
under the hood (bounded producer) but materialize the whole result; convert them
to page-by-page streaming consumers in a follow-up.


**Step 5 (landed).** The last accumulating shape — an arbitrary unbounded
`ORDER BY` (no `LIMIT`) global sort — no longer fail-loud-refuses past the
`DEFAULT_RANGE_READ_LIMIT` probe cap. It now streams the uncapped scan
(`range_read_stream_all_with`) through a **spilling external merge sort** and
returns the fully, correctly ordered result. Bounded memory (the spill
threshold), complete + correctly ordered, disk I/O for large sorts, no cap other
than the query's own `LIMIT`.

- **RAM budget + spill threshold** (`ferrosa-storage/src/spill_budget.rs`):
  detects the process budget (cgroup v2 `memory.max` → cgroup v1
  `memory.limit_in_bytes` → `/proc/meminfo` `MemTotal`; `"max"`/near-`i64::MAX`
  sentinels → unlimited → system total; unknown host → 1 GiB floor). Detection is
  injectable (`BudgetSources`) and cached once; the threshold defaults to **50%**
  of the budget, tunable via `FERROSA_RANGE_SPILL_THRESHOLD_PCT` (default 50) or
  the absolute `FERROSA_RANGE_SPILL_THRESHOLD_BYTES` override (read fresh per call
  so operators/tests can retune without a restart).
- **External merge sort** (`ferrosa-storage/src/external_sort.rs`): rows are
  MOVED (never cloned) into an accumulation buffer; when the buffer crosses the
  threshold it is sorted and spilled to a run file (length-prefixed serde_json)
  and cleared; `finish()` **cascade-merges** runs in fixed fan-in
  (`MERGE_FANIN = 64`) passes down to `<= MERGE_FANIN` runs, then does a bounded
  k-way merge (min-heap holding one row per run). Peak working set is
  `O(MERGE_FANIN)` + one buffer — **independent of the row count**. Every spill/
  merge I/O error propagates (fail loud); a truncated run is a hard error, never a
  silent early stop. Runs live under the `TempSortTableReservation` dir and are
  cleaned up on drop.
- **Router wiring** (`ferrosa-cql/src/router.rs`): the arbitrary-unbounded-ORDER-BY
  arm streams the uncapped scan through `sort_rows_from_partition_stream_spilling`
  (build rows + predicate-filter per partition → push into `ExternalSorter`), then
  the generic in-memory sort site is skipped (`order_by_already_sorted`).
  `DISTINCT`/aggregate/function-projection keep their `range_read_limited_rows_checked`
  fail-loud cap (unchanged).

**Steps 1–2 landed.**

**Step 2 (landed).** The `DEFAULT_RANGE_READ_LIMIT` (10_000) **result cap** is removed
for the O(1)-streamable shapes; each is bounded ONLY by the query's own `LIMIT`:

- **Scalar aggregates** (`SUM`/`MIN`/`MAX`/`AVG`) over a full ALLOW FILTERING scan now
  fold through an O(1) streaming accumulator (`router.rs` `stream_builtin_aggregates` +
  `StreamingAggAcc`) over the uncapped `range_read_stream_all_with` — exact over the whole
  table, no `all_rows` materialization, move-only. Previously `SELECT SUM(v) FROM t` (no
  LIMIT) was **refused entirely** ("unbounded full-table materialization is disabled").
  (`COUNT(*)` keeps its existing dedicated streaming/metadata path.)
- **User `LIMIT N`** larger than the storage Vec-materialization OOM guard now streams
  (`range_read_stream_all_with().take(N)`) instead of the Vec path — the router `scan_bound`
  arm. `range_read_limited_rows` / `coordinate_range_read_stream_limited_rows` no longer
  re-clamp the caller's bound to 10_000 (`write_path.rs`, `range_read_stream.rs`).
- **`SELECT DISTINCT <partition key>`** and simple **`WHERE … ALLOW FILTERING`** scans were
  already uncapped (step 1 projected stream / paged-filter path); step-2 tests lock this in.

**Spilled (step 5, landed):** an unbounded `ORDER BY` (no `LIMIT`) global sort now streams
the uncapped scan through the spilling external merge sort (above) instead of the
truncation-detecting `range_read_limited_rows_checked` cap. `GROUP BY` is not
yet parsed in the CQL AST, so high-cardinality group state is N/A (no cap to remove;
add per-group spill when `GROUP BY` lands — deferred). The legacy non-streaming
coordinated RPC (`FERROSA_BULK_STREAMING_RANGE_READ=0`, a documented degraded opt-out) and
the raft `RangeReadHandler` keep their per-replica cap intentionally.

Step-2 tests (all RED→GREEN or bounded-guard): `router.rs`
`sum_aggregate_is_exact_past_10k` (RED→GREEN), `user_limit_above_10k_returns_all_requested_rows`
(RED→GREEN), `streaming_aggregates_are_correct_with_where_and_nulls`,
`allow_filtering_scan_returns_all_rows_past_10k`,
`distinct_partition_key_returns_all_rows_past_10k_non_projected`; the step-2
bounded guard `order_by_no_limit_stays_bounded_past_10k` was replaced in step 5 by
`order_by_no_limit_sorts_all_rows_past_10k` (spill, all rows) +
`order_by_spills_and_stays_correct` (randomized vs in-memory reference); cluster
`range_scan_streaming_memory_bound.rs` covers the memory-bound + >10k data-completeness at
the coordinated `range_read_stream_all_with` layer.

**Premise corrected during step 1.** The projected arm does **not** clamp at 10k —
`range_read_projected` iterates uncapped (`cap = usize::MAX`). Its real defect was on the
**cluster** path: it read only the local node (`coordinator.storage`) and returned
**silently partial** results. Step 1 (commit `34576b1e`) routes it through coordinated
streaming (`range_read_projected_stream_all_with`), fixing that and removing a full
`Vec<Partition>` materialization. The actual `DEFAULT_RANGE_READ_LIMIT` clamps live in
`range_read_limited_rows` (`write_path.rs:737/796`) and the coordinator/raft range-read
paths — used by the **non-projected** complex arm (`range_read_limited_rows_checked`,
fail-loud at `router.rs ~4695`) and simple coordinated reads. **Those are the cap sites to
remove in steps 2+.** (#234, a server-side fail-loud guard, was rejected as the wrong fix.)

Two follow-ups from step 1: (a) `range_read_projected` is now orphaned (no real callers;
survives only as `pub`) — delete it once streaming is complete; (b) a cluster projected
scan **with** a `scan_bound` (LIMIT/page_size) now fail-louds (`partition_limit`
unimplemented for the coordinated projected stream) instead of returning local-only
partial — implement coordinated projected partition-limit if that shape needs cluster
support.

## Principle

A query's result is bounded **only** by the `LIMIT` it specifies (or, for ANN, the
query's top-`K`). There is **no server-side result cap**. The 10 000-row
`DEFAULT_RANGE_READ_LIMIT` result cap is removed.

Safety comes from bounding **memory**, not result count:

- Stream every range read through **bounded-capacity buffers** (`VecDeque` ring buffers,
  move semantics — no per-row `clone`/`copy`; rows move from reader → buffer → page).
- Back-pressure the producer when the buffer is full; the client pulls the next page via
  the existing CQL `paging_state` contract. In-flight memory ≈ one page + the bounded
  pipeline buffer, independent of total result size.
- For shapes that cannot be computed in O(1) streaming state, **spill to disk** via the
  existing temp-sort infra (`TempSortTableReservation`, `engine.rs:311/2577`;
  `spill_batch`, `store.rs:3438`). Bounded memory, complete result, disk I/O for huge sorts.

## Per-shape plan

| Shape | Today | Target |
|-------|-------|--------|
| Simple `WHERE … ALLOW FILTERING` | already streams/pages | **done** — paged-filter stream; step-2 test locks uncapped >10k |
| `COUNT(*)` | streams (#230) | unchanged |
| Other aggregates (`SUM`/`MIN`/`MAX`/`AVG`) | ~~capped at 10k~~ **was fail-loud refused** | **done (step 2)** — O(1) streaming accumulator (`stream_builtin_aggregates`) |
| User `LIMIT N` (> OOM guard) | ~~clamped to 10k (Vec)~~ | **done (step 2)** — `range_read_stream_all_with().take(N)` |
| `DISTINCT <partition key>` | capped at 10k | **done (step 1)** — token-ordered scan visits each partition once, no dedup buffer |
| Projected scan (subset of columns) | `range_read_projected(.., scan_bound)` capped | **done (step 1)** — `range_read_projected_stream_all_with` (`write_path.rs:619`) |
| `GROUP BY` (high cardinality) | N/A — not parsed in the AST yet | add per-group accumulator + spill when `GROUP BY` lands (deferred; not step 5) |
| `ORDER BY` (no `LIMIT`, arbitrary) | ~~capped + fail-loud~~ | **done (step 5)** — spilling external merge sort (`ExternalSorter` + cascade k-way merge); complete, correctly ordered, memory bounded by the spill threshold, no cap |
| `ORDER BY … LIMIT N` / clustering-order | bounded | bounded top-N heap (size N), no spill |
| ANN | top-`K` | bounded heap of size `K` (query-specified) |

## Cap-removal sites (all clamp/`default = DEFAULT_RANGE_READ_LIMIT`)

- `ferrosa-cluster/src/write_path.rs:27` — the const (re-purpose to a *page/buffer* default, not a result cap), `:737`, `:796` clamps.
- `ferrosa-cluster/src/coordinator/read.rs:1271` (default), `:1302` (clamp).
- `ferrosa-cluster/src/coordinator/range_read_stream.rs:1628` (clamp).
- `ferrosa-cluster/src/raft/handlers.rs:1050` (default), `:1098` (clamp).
- `ferrosa-cql/src/router.rs:4708` (passes the cap into the complex/projected arm); convert the arm at `~4673-4693` to the `stream_all`/spill variants.

Each clamp becomes either (a) removed where the path already pages, or (b) a *buffer/page
size* (memory bound), never a result-count cap.

## Test plan (TDD, RED first)

1. `SELECT *` / projected / `DISTINCT` over **>10k** partitions returns **every** row (no cap, no error).
2. Peak RSS during a large scan is independent of result size (bounded-buffer property; mirror `range_scan_streaming_memory_bound.rs`).
3. `ORDER BY` with no `LIMIT` over >10k rows returns a fully, correctly ordered result via spill.
4. `GROUP BY` high-cardinality returns all groups; group-state spills past the budget.
5. Aggregates (`SUM`/`AVG`) over >10k rows are exact (not over a 10k window).
6. No `clone` of row payloads on the hot path (move-only); buffers are fixed-capacity `VecDeque`.

## Memory budget + spill threshold (implemented in step 5)

Implemented as `ferrosa-storage/src/spill_budget.rs` (see Step 5 above). The
design below is realized: detection order cgroup v2 → cgroup v1 → system total →
1 GiB floor, injectable + cached; threshold 50% default with `_PCT`/`_BYTES`
overrides read fresh per call.

- **Detect the RAM budget**: cgroup v2 `memory.max` (then cgroup v1
  `memory.limit_in_bytes`), falling back to system total (`sysinfo`/`/proc/meminfo`).
  Cache it at startup.
- **Spill threshold = 50% of that budget by default, tunable.**
  `FERROSA_RANGE_SPILL_THRESHOLD_PCT` (default `50`), with an absolute
  `FERROSA_RANGE_SPILL_THRESHOLD_BYTES` override. When an `ORDER BY`/`GROUP BY`
  accumulation crosses the threshold, spill to the temp-sort table.
- **Buffer/page capacity is tunable**, derived from the same budget (a small fraction,
  e.g. `FERROSA_RANGE_READ_PAGE_BYTES`), so the streaming pipeline's in-flight memory is a
  bounded slice of the budget independent of result size.

The 50% default is a starting assumption to be tuned against soak results.
