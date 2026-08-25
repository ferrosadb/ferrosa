---
crate: ferrosa-cql
doc: fmea
last_updated: 2026-08-09
---

# ferrosa-cql — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (each 1–10,
higher = worse). This crate is the entire client path, so most severities are
high. Entries below reflect gaps found in the code, not hypotheticals.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| CQL-1 | LWT routed to Accord in standalone/pair mode where `peer_manager`/`accord_clock` are absent | `INSERT ... IF NOT EXISTS` / `IF <cond>` return `ServerError` ("not yet implemented") instead of executing | 8 | 5 | 2 | 80 | **Designed fail-loud** (p0-03): `route()` returns a clear error rather than a silent non-linearizable local path. Full coordinator driver tracked on `fix/p0-03b-accord-network`. Real functional gap, but observable. |
| CQL-2 | `paging_state` cursor is opaque but **unsigned** — `PagingState::encode/decode` is a plain length-prefixed pk+ck+flag with no HMAC | A client can forge/tamper a paging token to resume at an arbitrary key, bypassing the original query's partition scope (cross-partition read / IDOR-style) | 7 | 4 | 7 | 196 → 24 | **Implemented.** `encode` appends `HMAC-SHA256(process_key, payload)`; `decode` verifies it constant-time (`Mac::verify_slice`) before parsing, rejecting any forged/tampered cursor. Key from `FERROSA_PAGING_HMAC_KEY` (64 hex) or a random per-process key (multi-node clusters must share it). Tests: `tampered_paging_state_is_rejected`, `unsigned_forged_paging_state_is_rejected`. |
| CQL-3 | Encoder divergence vs `ferrosa-postgres` if the re-exported row codec were ever forked into this crate | Rows written via one front-end read back wrong/invisible via the other (silent corruption) | 10 | 1 | 6 | 60 | **Structural**: codec is the single re-export from `ferrosa-row-bridge` (D10); no second encoder exists here. Reinforced by the PG differential oracle. |
| CQL-4 | Permission check omitted on a new `route_*` handler | Unauthorized DML/DDL succeeds (privilege escalation) | 9 | 2 | 5 | 90 | M8 convention: every `route_*` calls `Schema::check_permission`. Risk is a *new* handler forgetting it — not enforced by the type system. Add a lint/test that every route is permission-gated. |
| CQL-5 | `router.rs` is a 22.4k-LoC monolith (`route()` + ~60 `route_*` handlers in one file) | Hard to review; high chance a change to one handler regresses another; reviewers miss missing checks (feeds CQL-4) | 6 | 6 | 6 | 216 | **Open structural debt.** Split DML / DDL / role / type-function handlers into submodules. Test density (253 tests) partially compensates detection. |
| CQL-6 | Subscription poll re-runs the inner SELECT on an interval with no global cap on rows/work per tick | A broad subscribed SELECT over a large table re-scans every interval, amplifying load (DoS-by-subscription) | 6 | 3 | 6 | 108 | Per-connection `max_subscriptions` bounds count; per-tick scan cost is not bounded. Add a per-subscription row/byte budget and backpressure. |
| CQL-7 | `COMPACT` statement parses and routes but is not implemented | `COMPACT` returns "not supported / not implemented" error | 3 | 3 | 2 | 18 | **Designed**: explicit error message; manual SSTable compaction is intentionally not exposed. Low severity (UCS runs automatically). |
| CQL-8 | Auth flag drift — historically `FERROSA_AUTH_ENABLED` (storage) and `FERROSA_AUTH_DISABLED` (CQL) could disagree, so the server sent `AUTHENTICATE` while storage accepted everything, breaking standard drivers | Drivers fail to connect, or connect with mismatched auth expectations | 7 | 2 | 4 | 56 | **Fixed**: `resolve_auth_disabled()` makes storage `auth_enabled` the single source of truth; the env override is deprecated and logs a warning. Covered by unit tests. |
| CQL-9 | Server-side `now()` minting a non-16-byte TimeUUID | TimeUUID-clustered tables wedge at memtable flush (data-loss-class bug) | 9 | 1 | 4 | 36 | **Fixed + documented invariant**: `eval_now()` always returns 16 bytes; guards the prior flush-wedge bug. |
| CQL-10 | In-flight semaphore (default 128) or per-IP cap rejecting legitimate bursts as `Overloaded` | Spurious `Overloaded` errors under legitimate load spikes | 4 | 3 | 3 | 36 | Tunable via `ServerConfig`; designed backpressure that fails loud rather than queueing unboundedly. |
| CQL-11 | DataStax Java driver `DROP KEYSPACE` times out during schema-agreement on v5 | Duplicate retained-event replay after index DDL launched overlapping driver metadata refreshes while the control connection was being replaced, so the following DROP could miss a stable control connection. | 5 | 1 | 1 | 5 | **Fixed:** the schema-event forwarder is the sole retained-event replay path, so a reconnecting control connection receives the event once (`register_replays_retained_table_schema_event_once`). The DataStax Java v5 smoke now keeps `DROP KEYSPACE` enabled and passes 38/38 checks; ten repeated full-suite runs also passed. |
| CQL-12 | Out-of-range `timestamp` value written verbatim (integer literal or bound 8-byte value) | An i64 outside `chrono`'s representable millisecond range lands in a `timestamp` cell; on read the driver's date decode crashes, breaking `SELECT *` for the whole partition (forensically observed as `days=-1917935064`) | 9 | 2 | 6 | 108 → 18 | **Fixed (Bug C)**: `bridge::validate_timestamp_ms` rejects any timestamp cell outside `[TIMESTAMP_MIN_MS, TIMESTAMP_MAX_MS]` (chrono `MIN_UTC`/`MAX_UTC` millis) at the write boundary for integer-literal, string, and bound-blob paths. Read robustness: `cell_to_cql_value` fails loud (`ServerError` with the offending millis) on already-corrupt on-disk timestamps instead of emitting an undecodable value. Tests: `term_out_of_range_integer_timestamp_is_rejected`, `bound_out_of_range_timestamp_blob_is_rejected`, `timestamp_bounds_match_chrono`, `cell_to_cql_value_rejects_corrupt_timestamp`. |
| CQL-13 | The generic keyed scalar planner selects a full-text/vector/geo index for `column = value` | A valid keyed equality read can consult an incompatible index map and return an empty result; selection was iteration-order dependent when phonetic and full-text indexes shared a column | 8 | 3 | 5 | 120 → 16 | **Fixed:** scalar plans admit only B-tree, hash, composite, phonetic, and filtered indexes. Full-text/vector/geo remain on dedicated query branches. Regression: `keyed_equality_does_not_consult_fulltext_index`. |
| CQL-14 | PREPARE counted `LIMIT ?` but tried to resolve every marker as a table column | PREPARE returned `Invalid` with one fewer variable specification, preventing strict drivers from preparing an otherwise valid parameterized scan | 6 | 3 | 3 | 54 → 6 | **Fixed:** `connection::analyze_prepared_columns` emits the Cassandra-style synthetic `[limit] : int` spec after WHERE/ANN markers instead of schema-resolving it. Wire regression: `prepare_select_with_where_and_limit_bind_markers_reports_col_count_two`. |
| CQL-15 | Paged `SELECT DISTINCT` over a composite partition key bounded the upstream scan by physical partitions/rows, then applied DISTINCT and an offset cursor afterward | A driver page could under-fill and advance past an unseen logical partition; three partitions could return only two distinct rows across the traversal | 8 | 3 | 5 | 120 → 16 | **Fixed:** complete partition-key DISTINCT projections use a projected partition stream that emits one row per partition and resumes from the last emitted partition key. Direct, prepared-plan, multi-clustering-row, and partial-prefix-refusal coverage: `select_distinct_composite_partition_key_enumerates_partitions`. |

## Top risks to act on

1. **CQL-5 (RPN 216)** — the 22.4k-LoC `router.rs` monolith. It is the dominant
   *detection* risk: missing permission checks (CQL-4) and other regressions hide
   in a file too large to review as a unit. Decompose into per-statement-family
   submodules.
2. **CQL-2 (RPN 196)** — the unsigned `paging_state` cursor. It is opaque to
   clients by convention only; nothing prevents a crafted token from resuming at
   an attacker-chosen key. Sign or scope-bind the cursor.
3. **CQL-6 (RPN 108) / CQL-1 (RPN 80)** — unbounded per-tick subscription scans,
   and the standalone LWT-on-Accord functional gap (already fail-loud).

## Detection assets

- ~945 in-crate tests (router 253, parser 158, bridge 121).
- Integration tests: `tests/{handshake, auth_integration, auth_warn_mode,
  bolt_transaction_state, cassandra_cql_examples}.rs` plus the ignored
  live-cluster `tests/fts_live_cluster.rs` guard for coordinator-independent
  native `fts_match` results on a 3-node cluster.
- Postgres differential oracle (in `ferrosa-postgres`) guards the shared codec.
- Per-opcode CQL metrics + Prometheus endpoint surface error/overload rates.
