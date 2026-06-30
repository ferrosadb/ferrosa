# Streaming range reads — remove the server-side result cap

**Status:** proposed (design locked; implementation in `feat/stream-range-reads-no-cap`).
**Fixes a live failure seam.** #232's fail-loud guard did **not** reliably land, and the
**projected** complex arm (`range_read_projected`, `router.rs:4681`) truncates at 10k with
**no check at all** — silently returning wrong results for projected `DISTINCT`/complex
scans past 10k. (#234, which tried to bolt fail-loud onto that arm, was rejected: a
server-side limit is the wrong fix.) Removing the cap + streaming eliminates truncation
entirely — there is no cap to truncate at. The non-projected arm's existing
`range_read_limited_rows_checked` probe is treated as untrustworthy, not a safety net.

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
| Simple `WHERE … ALLOW FILTERING` | already streams/pages | unchanged; drop any residual clamp |
| `COUNT(*)` | streams (#230) | unchanged |
| Other aggregates (`SUM`/`MIN`/`MAX`/`AVG`) | capped at 10k | O(1) streaming accumulator |
| `DISTINCT <partition key>` | capped at 10k | stream: a token-ordered scan visits each partition once, so distinct partition keys emit with no dedup buffer (`router.rs:3723`) |
| Projected scan (subset of columns) | `range_read_projected(.., scan_bound)` capped | `range_read_projected_stream_all_with` (`write_path.rs:619`) |
| `GROUP BY` (high cardinality) | capped | per-group accumulator; spill group state past the memory budget |
| `ORDER BY` (no `LIMIT`, arbitrary) | capped + fail-loud | spillable temp-sort (already classified at `router.rs:112-204`) |
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
