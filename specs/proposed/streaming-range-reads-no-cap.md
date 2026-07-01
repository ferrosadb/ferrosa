# Streaming range reads — remove the server-side result cap

**Status:** proposed (design locked; implementation in `feat/stream-range-reads-no-cap`).
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

**Still bounded (fail-loud) until step 5 (spill-to-disk):** an unbounded `ORDER BY` (no
`LIMIT`) global sort keeps the truncation-detecting cap
(`range_read_limited_rows_checked`, still using `DEFAULT_RANGE_READ_LIMIT` as its probe
bound) rather than materializing the whole table for an in-memory sort. `GROUP BY` is not
yet parsed in the CQL AST, so high-cardinality group state is N/A (no cap to remove;
add per-group spill when `GROUP BY` lands, with step 5). The legacy non-streaming
coordinated RPC (`FERROSA_BULK_STREAMING_RANGE_READ=0`, a documented degraded opt-out) and
the raft `RangeReadHandler` keep their per-replica cap intentionally.

Step-2 tests (all RED→GREEN or bounded-guard): `router.rs`
`sum_aggregate_is_exact_past_10k` (RED→GREEN), `user_limit_above_10k_returns_all_requested_rows`
(RED→GREEN), `streaming_aggregates_are_correct_with_where_and_nulls`,
`allow_filtering_scan_returns_all_rows_past_10k`,
`distinct_partition_key_returns_all_rows_past_10k_non_projected`,
`order_by_no_limit_stays_bounded_past_10k` (bounded guard); cluster
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
| `GROUP BY` (high cardinality) | N/A — not parsed in the AST yet | add per-group accumulator + spill when `GROUP BY` lands (step 5) |
| `ORDER BY` (no `LIMIT`, arbitrary) | capped + fail-loud | **still fail-loud-bounded** until step 5 spillable temp-sort |
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

## Memory budget + spill threshold (decided)

There is no configured-RAM reader yet (only per-feature env budgets like
`FERROSA_RRD_RING_MEMORY_BUDGET_BYTES`). Add one:

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
