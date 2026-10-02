---
executive_summary:
  purpose: "Layered test specification for the CQL read path (`ferrosa-cql`), replacing 3 source-text assertions that were proven vacuous with behaviour tests against real instrumentation."
  highest_risk_areas:
    - "Read-path materialization (the OOM class): a shape that silently buffers a whole result while claiming to stream"
    - "Paging completeness: a response that omits a continuation for a non-exhausted result, which a client reads as 'the result is complete'"
    - "Index/aggregate arms that bypass the paging decision entirely"
  coverage_gaps:
    - "3 tests asserted on a source file's text via `include_str!` with anchors that resolved inside their own literals, so they checked nothing; 9 more are weak (source-text based) but not empty"
    - "No test pinned the paging contract for the three exits that hardcode paging_state: None"
    - "No test pinned that all result exits share one envelope constructor"
---

# Test Specification — CQL read path

> Generated from the invariant inventory after the 2026-10-02 `router.rs` refactor.
> Source of truth for coverage: the traceability matrix at the end.

## Why this exists

The refactor surfaced a defect class in the *test suite*, not the code:

| Finding | Evidence |
|---|---|
| 12 tests read a source file as text; **3** had no valid production anchor at all | `count_filtering_scans_project_only_predicate_columns` split on an anchor occurring **0×** in production, at byte 1,311,092 while `mod tests` begins at 656,235 — the sliced region was 64 chars of the test's own source |
| Those 3 passed anyway | `contains(..)` matched the literal that names the thing being asserted |
| They were vacuous **before any refactor** | my extraction merely removed the coincidental reason they still ran |
| The other 9 are weak, not empty | each retains ≥1 anchor that really does occur in production |

A guard test that cannot fail is worse than no test: it converts an untested path into
a *believed-tested* path. These were the OOM guard tests.

**Rule adopted:** a read-path guard asserts on **observed behaviour** (instrumentation
counters, allocator peak, returned rows, continuation presence) — never on source text,
which is coupled to file layout and can silently degrade to tautology.

## Layer 1 — Unit

### `SelectRawResult::single_table` (constructor)

#### Behaviour
- `T-U-001` — Given col_names/col_types/rows/ks/table and any `paging_state`, When
  constructed, Then all six fields are populated and `paging_state` is passed through
  unchanged (`Some` stays `Some`, `None` stays `None`).
- `T-U-002` — Given a result with a continuation, When `.encode()` is called, Then the
  encoded frame body carries the continuation bytes (byte-identical to the previous
  hand-rolled construction).

#### Contract (compile-time)
- `T-U-003` — A call site that omits `paging_state` **must not compile**. This is the
  structural guarantee that replaced 13 hand-written envelope literals. Verified by
  the signature: `paging_state` is a required positional parameter.

## Layer 5 — Property / invariant tests (the important layer here)

### Paging completeness — INV-P3

- `T-P-001` — **A non-exhausted result under a client `page_size` carries a continuation.**
  Given a table with more rows than the page size, When a paged read is issued, Then
  `paging_state.is_some()`. Covers every shape that can be paged (scan, index, aggregate).
- `T-P-002` — **An exhausted result carries no continuation**, even when a `page_size`
  was supplied. Given a table with fewer rows than the page size, When a paged read is
  issued, Then `paging_state.is_none()`.
- `T-P-003` — **Walking a continuation reaches the exact unpaged result** (INV-P4):
  concat(pages) == unpaged rows, in order, no duplicates, no gaps — including when one
  partition spans a page boundary.

### Materialization bounds — INV-P7 (the OOM class)

Assert on the counters that already exist, not on source text.

- `T-P-004` — **A LIMIT-bounded read stops pulling at its bound.**
  Given a partition/index with N >> LIMIT rows, When read with `LIMIT k`,
  Then `INDEX_ROWS_VISITED <= k + lookahead` (never N).
- `T-P-005` — **A streaming shape leaves the materialization counter at zero.**
  Given a scan that streams, When read, Then `INDEX_ROWS_COLLECTED == 0`.
- `T-P-006` — **A bounded-partition suffix read materializes O(LIMIT), not O(partition).**
  Given a 100-row partition, When read with `LIMIT 2`, Then
  `BOUNDED_PARTITION_ROWS_MATERIALIZED == 2` (+1 continuation probe).
- `T-P-007` — **Peak allocation is independent of result size.** Given N=4k and N=32k
  fixtures, When the same streaming read runs, Then peak-live is equal within a small
  constant (allocator harness, repo pattern from `virtual_table_streaming_alloc.rs`).

### Aggregate fold

- `T-P-008` — **An unpaged aggregate is exact**: returns the true total, `paging_state: None`.
- `T-P-009` — **A paged aggregate over a larger-than-page table is not the final total**
  and carries a continuation. *(This is the parked `aggregate_fold_resumes_…` work;
  currently RED and belongs to its own change — recorded here so it is not lost.)*

### Cursor integrity — INV-P2 / INV-P6

- `T-P-010` — A forged/tampered continuation is rejected (HMAC; FMEA CQL-2).
- `T-P-011` — A continuation from a **different query shape** is rejected, not honoured
  (partition scope, LIMIT, shape tag).

## Layer 3/4 — Integration / System

- `T-I-001` — A paged read over a live single-node cluster: page through a
  multi-partition table with `page_size`, assert the union equals a full read.
- `T-I-002` — The same, spanning a **wide partition** (page boundary mid-partition).
- `T-S-001` — `SELECT` with `page_size` through the CQL wire protocol returns
  continuation bytes the driver accepts and resumes with.

## Traceability matrix

| Test ID | Layer | Box | Scenario (short) | Source | Priority | Status |
|---------|-------|-----|-------------------|--------|----------|--------|
| T-U-001 | Unit | White | constructor passes paging_state through | INV-P3 | High | Stub |
| T-U-002 | Unit | White | encode carries continuation bytes | FMEA CQL-2 | High | Stub |
| T-U-003 | Unit | White | omitted paging_state cannot compile | refactor #1 | High | **Satisfied** (signature) |
| T-P-001 | Property | Black | non-exhausted + page_size => continuation | INV-P3 | **Critical** | Stub |
| T-P-002 | Property | Black | exhausted => no continuation | INV-P3 | **Critical** | Stub |
| T-P-003 | Property | Black | pages concat == unpaged result | INV-P4 | **Critical** | Stub |
| T-P-004 | Property | White | LIMIT stops pulling at bound | INV-P7, t_ee98faa0 | **Critical** | Partial (source-text form, now vacuous) |
| T-P-005 | Property | White | streaming shape collects zero | INV-P7 | **Critical** | Partial (source-text form) |
| T-P-006 | Property | White | suffix read materializes O(LIMIT) | t_430c4188 | High | Partial |
| T-P-007 | Property | White | peak alloc independent of N | INV-P7 | High | Stub |
| T-P-008 | Property | Black | unpaged aggregate exact | INV-P3 | High | Covered (behavioural today) |
| T-P-009 | Property | Black | paged aggregate partial + continuation | INV-P3/D1 | High | **RED** (parked, own change) |
| T-P-010 | Property | Black | forged cursor rejected | FMEA CQL-2 | High | Covered (behavioural) |
| T-P-011 | Property | Black | foreign-shape cursor rejected | INV-P2/P6 | High | Stub |
| T-I-001 | Integration | Black | paged read over live node | INV-P4 | High | Stub |
| T-I-002 | Integration | Black | page boundary inside wide partition | INV-P4 | High | Stub |
| T-S-001 | System | Black | wire-level paging round trip | INV-P3 | High | Stub |

## Source coverage

| Source | Total | Covered behaviourally | Vacuous (source-text) | Uncovered |
|--------|-------|----------------------|----------------------|-----------|
| INV-P3 (continuation completeness) | 4 | 1 | 0 | 3 |
| INV-P4 (no dup/gap) | 3 | 0 | 0 | 3 |
| INV-P7 (O(1) memory) | 4 | 0 | 3 | 1 |
| INV-P2/P6 (cursor integrity) | 2 | 1 | 0 | 1 |
| FMEA CQL-2 (cursor forgery) | 1 | 1 | 0 | 0 |

## Disposition — what actually happened

Audit result: of the 12 tests reading a file as text, most retained at least one
real production anchor and were merely *weak*; three were **wholly vacuous** and
are now gone.

| Test | Finding | Disposition |
|---|---|---|
| `count_filtering_scans_project_only_predicate_columns` | anchor `let partitions = if let Some(wanted) = projection_wanted` occurred **0×** in production; the region inspected was 64 chars of its own literal | **replaced** by `indexed_count_is_exact_past_the_cap_and_limit_stops_the_consumer` |
| `broad_select_paths_must_not_materialize_full_range_before_paging` | boundary marker `#[cfg(test)]\nmod tests` does not exist in production, so the two `!contains(..)` assertions were unreachable | **replaced** by `broad_select_is_complete_and_pages_without_materializing_the_range` |
| `degraded_scan_arm_serves_all_shapes_by_streaming_never_refuses` | its invariants are already proven behaviourally by `degraded_arm_streaming_is_row_identical_to_reference` (differential vs reference) and `degraded_arm_spill_temp_files_are_cleaned_up` | **deleted** as redundant |
| remaining 9 | each keeps ≥1 real production anchor (weak, not empty) | left, now covered by the meta-guard |

### The meta-guard

`no_source_text_assertion_splits_on_an_anchor_absent_from_its_file` fails if any
source-inspecting test splits on an anchor that does not occur in the production
part of *the file its own `include_str!` names*. It resolves const/let-bound
anchors (`split(START)`) as well as literals, expands Rust string escapes before
comparing (`"\n}\n"` is newline-brace-newline at runtime), and skips boundary
markers that delimit production rather than asserting about it.

Its own correctness is part of the deliverable: an earlier revision used `rfind`
and matched its own search string, silently making `production` span the whole
test module — a guard that disarms itself. A check that reports false positives
gets switched off, so both bugs were fixed before landing.

### Non-vacuity evidence

- **Paging:** hardcoding `paging_state: None` at the router exit makes
  `broad_select_is_complete_and_pages_without_materializing_the_range` fail at
  "a non-exhausted paged read must advertise a continuation". Reverted.
- **Exactness:** `n = 10_001` is past the legacy 10,000 cap, so a regression
  yields 10_000 rather than `n`; `INDEX_ROWS_VISITED == 10` for `LIMIT 10` over
  10_001 rows cannot be a constant.
- A first draft of the count test asserted `INDEX_ROWS_COLLECTED == 0` on an
  `ALLOW FILTERING` full scan, where that counter never moves. The non-vacuity
  sub-proof caught it; it was rewritten onto the index fixture where the counter
  is live.

## Follow-up (not this change)

- `T-P-009` belongs to the resumable-aggregate change (parked branch), not this one.
- The 9 remaining source-text tests are *weak* rather than vacuous. Rewriting them
  to counter/behaviour assertions is worthwhile but is not required for the
  invariant to be guarded, and is deliberately left to a separate change.
