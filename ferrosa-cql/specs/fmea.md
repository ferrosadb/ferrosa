---
crate: ferrosa-cql
doc: fmea
last_updated: 2026-06-21
---

# ferrosa-cql — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (each 1–10,
higher = worse). This crate is the entire client path, so most severities are
high. Entries below reflect gaps found in the code, not hypotheticals.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| CQL-1 | LWT routed to Accord in standalone/pair mode where `peer_manager`/`accord_clock` are absent | `INSERT ... IF NOT EXISTS` / `IF <cond>` return `ServerError` ("not yet implemented") instead of executing | 8 | 5 | 2 | 80 | **Designed fail-loud** (p0-03): `route()` returns a clear error rather than a silent non-linearizable local path. Full coordinator driver tracked on `fix/p0-03b-accord-network`. Real functional gap, but observable. |
| CQL-2 | `paging_state` cursor is opaque but **unsigned** — `PagingState::encode/decode` is a plain length-prefixed pk+ck+flag with no HMAC | A client can forge/tamper a paging token to resume at an arbitrary key, bypassing the original query's partition scope (cross-partition read / IDOR-style) | 7 | 4 | 7 | 196 | **Open gap.** Decode validates lengths but not authenticity. Sign the cursor with a per-server key, or bind it to the prepared/query id. See roadmap. |
| CQL-3 | Encoder divergence vs `ferrosa-postgres` if the re-exported row codec were ever forked into this crate | Rows written via one front-end read back wrong/invisible via the other (silent corruption) | 10 | 1 | 6 | 60 | **Structural**: codec is the single re-export from `ferrosa-row-bridge` (D10); no second encoder exists here. Reinforced by the PG differential oracle. |
| CQL-4 | Permission check omitted on a new `route_*` handler | Unauthorized DML/DDL succeeds (privilege escalation) | 9 | 2 | 5 | 90 | M8 convention: every `route_*` calls `Schema::check_permission`. Risk is a *new* handler forgetting it — not enforced by the type system. Add a lint/test that every route is permission-gated. |
| CQL-5 | `router.rs` is a 22.4k-LoC monolith (`route()` + ~60 `route_*` handlers in one file) | Hard to review; high chance a change to one handler regresses another; reviewers miss missing checks (feeds CQL-4) | 6 | 6 | 6 | 216 | **Open structural debt.** Split DML / DDL / role / type-function handlers into submodules. Test density (253 tests) partially compensates detection. |
| CQL-6 | Subscription poll re-runs the inner SELECT on an interval with no global cap on rows/work per tick | A broad subscribed SELECT over a large table re-scans every interval, amplifying load (DoS-by-subscription) | 6 | 3 | 6 | 108 | Per-connection `max_subscriptions` bounds count; per-tick scan cost is not bounded. Add a per-subscription row/byte budget and backpressure. |
| CQL-7 | `COMPACT` statement parses and routes but is not implemented | `COMPACT` returns "not supported / not implemented" error | 3 | 3 | 2 | 18 | **Designed**: explicit error message; manual SSTable compaction is intentionally not exposed. Low severity (UCS runs automatically). |
| CQL-8 | Auth flag drift — historically `FERROSA_AUTH_ENABLED` (storage) and `FERROSA_AUTH_DISABLED` (CQL) could disagree, so the server sent `AUTHENTICATE` while storage accepted everything, breaking standard drivers | Drivers fail to connect, or connect with mismatched auth expectations | 7 | 2 | 4 | 56 | **Fixed**: `resolve_auth_disabled()` makes storage `auth_enabled` the single source of truth; the env override is deprecated and logs a warning. Covered by unit tests. |
| CQL-9 | Server-side `now()` minting a non-16-byte TimeUUID | TimeUUID-clustered tables wedge at memtable flush (data-loss-class bug) | 9 | 1 | 4 | 36 | **Fixed + documented invariant**: `eval_now()` always returns 16 bytes; guards the prior flush-wedge bug. |
| CQL-10 | In-flight semaphore (default 128) or per-IP cap rejecting legitimate bursts as `Overloaded` | Spurious `Overloaded` errors under legitimate load spikes | 4 | 3 | 3 | 36 | Tunable via `ServerConfig`; designed backpressure that fails loud rather than queueing unboundedly. |

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
