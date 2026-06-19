# Ops Coverage — ferrosa binary, ferrosa-worker, ferrosa-ctl

> Generated: 2026-04-18
> Scope: `ferrosa/` (main binary crate), `ferrosa-worker/`, `ferrosa-ctl/`
> Source: live code inspection — `ferrosa/src/web/`, `ferrosa-ctl/src/`, `ferrosa-worker/src/`

---

## 1. Feature Inventory

### 1.1 Web API Endpoints (port 9090)

Mounted in `ferrosa/src/web/mod.rs::build_router`. Auth middleware (`auth_middleware`) wraps all `/api/*` routes. `/metrics` and the static fallback (`/`) are public.

#### Core API (`/api/*`) — `web/api.rs::routes()`

| Method | Path | Handler | Notes |
|--------|------|---------|-------|
| GET | `/api/connections` | `get_connections` | Alias also at `/api/storage` |
| GET | `/api/storage_stats` | `get_storage_stats` | Primary path |
| GET | `/api/storage` | `get_storage_stats` | Alias |
| GET | `/api/active_queries` | `get_active_queries` | Alias also at `/api/queries` |
| GET | `/api/queries` | `get_active_queries` | Alias |
| GET | `/api/tables` | `list_tables` | Lists all virtual tables in registry |

#### Cluster API (`/api/cluster/*`) — `web/api.rs::cluster_routes()`

| Method | Path | Handler | Notes |
|--------|------|---------|-------|
| GET | `/api/cluster/status` | `cluster_status` | Returns Raft node state |
| POST | `/api/cluster/promote` | `cluster_promote` | Promotes candidate to leader |
| POST | `/api/cluster/switchover` | `cluster_switchover` | Leader stepdown with election |
| POST | `/api/cluster/add-node` | `add_node_handler` | Pre-approves a node by host ID |
| POST | `/api/cluster/decommission` | `decommission_handler` | Decommissions a node |
| GET | `/api/cluster/ring` | `ring_handler` | Token ring distribution |
| POST | `/api/cluster/rebalance` | `rebalance_handler` | Rebalances token assignment |

#### Snapshot / PITR API (`/api/*`) — `web/snapshots.rs::snapshot_routes()`

| Method | Path | Handler | Notes |
|--------|------|---------|-------|
| GET | `/api/snapshots` | `list_snapshots` | Lists all snapshots |
| POST | `/api/snapshots` | `create_snapshot` | Creates a named snapshot with optional TTL |
| DELETE | `/api/snapshots/{name}` | `delete_snapshot` | Deletes a named snapshot |
| GET | `/api/archive_status` | `get_archive_status` | Commit log archiving lag, S3 state |
| POST | `/api/restore/preflight` | `restore_preflight` | Validates a restore target, no side effects |
| POST | `/api/restore` | `trigger_restore` | Executes restore, optionally to a timestamp |

#### Observability API (`/api/observability/*`) — `web/observability.rs::routes()`

| Method | Path | Handler | Notes |
|--------|------|---------|-------|
| GET | `/api/observability/cql` | `get_cql_stats` | CQL query latency, error counts |
| GET | `/api/observability/alerts` | `get_alerts` | Alert evaluator state |
| GET | `/api/observability/query_fingerprints` | `get_query_fingerprints` | Top-10k fingerprints |
| GET | `/api/observability/table_access` | `get_table_access` | Per-table read/write counts |
| GET | `/api/observability/full_scan_reasons` | `get_full_scan_reasons` | Full-partition scan detection |
| GET | `/api/observability/billing` | `get_billing` | Per-client billing counters |

#### Debug API (`/api/debug/*`) — `web/debug.rs::debug_routes()`

| Method | Path | Handler | Notes |
|--------|------|---------|-------|
| GET | `/api/debug/flamechart` | `flamechart_handler` | On-demand SVG flame chart; requires `FERROSA_DEBUG_AUTH_TOKEN` Bearer header |
| POST | `/api/debug/force-compact` | `force_compact_handler` | Triggers manual compaction |

#### WebSocket + Prometheus

| Method | Path | Handler | Notes |
|--------|------|---------|-------|
| GET | `/api/ws` | `ws_handler` | Virtual table subscription; supports subscribe/unsubscribe messages |
| GET | `/metrics` | `get_metrics` | Prometheus text exposition; **public**, no auth |

**Total: 25 HTTP endpoints + 1 WebSocket endpoint = 26 surfaces**

---

### 1.2 `ferrosa-ctl` Commands

| Command | Sub-action | Backing endpoint | Notes |
|---------|-----------|-----------------|-------|
| `status` | — | CQL `system_observability.connections` | Summary + connection count |
| `connections [--sort]` | — | CQL `system_observability.connections` | Tabular display |
| `queries [--long-running]` | — | CQL `system_observability.active_queries` | Sorted by elapsed time |
| `storage` | — | CQL `system_observability.*` | Storage stats |
| `topology` | — | CQL `system.peers` | Token ring table |
| `peers` | — | CQL `system.peers` | Peer node list |
| `monitor [--panel]` | — | CQL (polling) | TUI dashboard (T24); panels: connections, queries, storage |
| `add-node <host_id>` | — | `POST /api/cluster/add-node` | HTTP web API |
| `decommission [host_id]` | — | `POST /api/cluster/decommission` | HTTP web API |
| `ring` | — | `GET /api/cluster/ring` | HTTP web API |
| `rebalance` | — | `POST /api/cluster/rebalance` | HTTP web API |
| `snapshot` | `create <name> [--ttl-hours]` | `POST /api/snapshots` | HTTP web API |
| `snapshot` | `list` | `GET /api/snapshots` | HTTP web API |
| `snapshot` | `delete <name>` | `DELETE /api/snapshots/{name}` | HTTP web API |
| `restore <name> [--point-in-time] [--force]` | — | `POST /api/restore` | HTTP web API; PITR via timestamp |

**Total: 15 ctl commands (3 snapshot sub-actions counted separately)**

---

### 1.3 `ferrosa-worker` Tasks

| Task type | Description |
|-----------|-------------|
| `IndexBuild` | Builds secondary index for SSTables from S3; reads SSTable data, writes sidecar files to output S3 prefix |

Worker is a standalone binary — no HTTP surface. Receives `TaskDescriptor` JSON on stdin and writes `TaskResult` on stdout.

---

## 2. Spec Coverage Matrix

| Component / Endpoint group | Spec document | Coverage |
|----------------------------|---------------|----------|
| Observability API (`/api/observability/*`) | `specs/observability-architecture.md`, `specs/observability-fmea.md`, `specs/observability-threat-model.md` | Full — design, FMEA, and threat model all reference these routes by path |
| Flamechart endpoint (`/api/debug/flamechart`) | `specs/observability-architecture.md` (§Layer 9), `specs/observability-threat-model.md` (OBS-T1, OBS-T2) | Partial — architecture and threat model cover the endpoint; **no dedicated API reference** |
| Debug force-compact (`POST /api/debug/force-compact`) | None | **Not documented** |
| Snapshot / PITR API (`/api/snapshots`, `/api/restore`) | `specs/pitr.md`, `specs/project-plan-pitr.md`, `specs/analysis/pitr-fmea.md` | Partial — storage-layer PITR is documented; web API surface is not itemised per-endpoint |
| Cluster management API (`/api/cluster/*`) | `specs/cluster-formation-architecture.md`, `specs/cluster-formation-state-machine.md` | Partial — formation protocol covered; `promote`/`switchover`/`rebalance` endpoints absent from specs |
| Core metrics API (`/api/connections`, `/api/storage_stats`, etc.) | `specs/status.md`, `specs/components.md` | Minimal — listed in component env-var table; no per-route contract |
| WebSocket (`/api/ws`) | None | **Not documented** |
| Prometheus `/metrics` | `specs/status.md` (one line) | Minimal |
| Auth middleware (`FERROSA_AUTH_DISABLED` semantics) | `specs/threat-model.md`, `specs/decisions/design-cql-role-auth-rollout.md` | Covered as a threat/decision; **no API reference describing behaviour when enabled vs disabled** |
| `ferrosa-ctl` commands | `specs/status.md` (one-line mention), `specs/components.md` | Minimal |
| `ferrosa-worker` | `specs/remote-index-build-backend.md` | Covered for the index-build task type |

---

## 3. Test Coverage

### 3.1 `ferrosa/tests/`

| File | Tests | What is exercised |
|------|-------|-------------------|
| `tests/web_ws.rs` | 2 | WebSocket subscribe/unsubscribe happy path; unknown-table error path. Does **not** use the real `ws.rs` handler — clones the logic inline because the crate is binary-only. |
| `tests/smoke.rs` | 10 | CQL server lifecycle (start, DDL, DML, concurrent sessions, system tables, index DDL). Does **not** exercise any web endpoint. |

### 3.2 `ferrosa/src/web/` (inline `mod tests`)

| Module | Tests | Notes |
|--------|-------|-------|
| `api.rs` | 27 | Virtual table JSON serialisation, cluster route responses, ring/rebalance handler logic |
| `snapshots.rs` | 14 | Snapshot create/list/delete/restore preflight serialisation; no live HTTP round-trip |
| `debug.rs` | 27 | Flamechart handler, auth token check, IP whitelist parsing, `collect_span_samples` timing |
| `observability.rs` | 1 | Single smoke test for route registration |
| `static_files.rs` | 14 | MIME detection, 404 fallback, embedded asset serving |
| `ws.rs` | 26 | Full subscribe/unsubscribe/error/close-frame coverage against a real in-process socket |
| `auth.rs` | 0 | **Zero tests in the inline `mod tests` block** (live agent fixing auth.rs) |

### 3.3 `ferrosa-ctl/tests/`

| File | Tests | What is exercised |
|------|-------|-------------------|
| `tests/integration.rs` | See below | Boots CQL server, registers virtual tables, runs queries issued by ctl commands |

Confirmed test functions cover: `connections` table structure, `state` column values, `active_queries` schema, storage stats query, URL construction for all web API calls (`add-node`, `decommission`, `ring`, `rebalance`, `snapshot create/list/delete`, `restore` with and without `--point-in-time`). No live HTTP server is started — the URL-construction and body-serialisation paths are unit-tested; the actual HTTP dispatch is not integration-tested.

### 3.4 `ferrosa-worker/` (inline)

| Module | Tests |
|--------|-------|
| `main.rs` | 3 — stdin/stdout task dispatch, JSON deserialisation of `TaskDescriptor` |

---

## 4. Gaps

### P0 — Critical

**GAP-1: `FERROSA_AUTH_DISABLED` bypasses all `/api/*` auth — no test guards against regression.**

`auth_middleware` in `web/auth.rs` unconditionally calls `next.run(req).await` when `state.auth_disabled` is true (line 46). The `docker-compose.yml`, CI workflow, and all cluster examples set this flag to `"true"`. A live agent is currently rewriting `auth.rs`. The `mod tests` block in `auth.rs` contains **zero tests** (confirmed by digest). There is no integration test that boots the web server with auth enabled and verifies that an unauthenticated request to `/api/snapshots` or `/api/cluster/promote` is rejected with 401. Once auth is re-enabled, this gap is a direct regression surface for privilege escalation (see `specs/threat-model.md` T-S2).

**GAP-2: `POST /api/restore` is called by `ferrosa-ctl restore` but has no integration test that issues a real HTTP request.**

`ferrosa-ctl::commands::restore` builds the POST body and sends it via `reqwest`. The web handler `trigger_restore` exists in `snapshots.rs` and has 14 inline unit tests — but all test body serialisation in isolation. No test boots both the web server and a ctl client and exercises the full path from `ferrosa-ctl restore` to `trigger_restore` to `StorageEngine::restore_from_snapshot`. The PITR `--point-in-time` flag is especially untested end-to-end: the timestamp is passed through the body but the handler's timestamp-to-log-sequence-number lookup is not exercised.

### P1 — High

**GAP-3: Flamechart endpoint is undocumented in the API reference and has a separate, non-standard auth mechanism.**

`GET /api/debug/flamechart` is documented in `specs/observability-architecture.md` as a design decision and in `specs/observability-threat-model.md` as a risk, but it does not appear in any API reference that operators would consult. The endpoint uses a completely separate auth scheme (`Authorization: Bearer <FERROSA_DEBUG_AUTH_TOKEN>`) that is independent of the Basic-auth middleware. This means:

1. The endpoint is accessible with any auth middleware state — even with `FERROSA_AUTH_DISABLED=false`, the Bearer token check runs from `check_auth()` inside the handler, not the middleware stack.
2. When `FERROSA_DEBUG_AUTH_TOKEN` is not set, the endpoint returns 403 Forbidden — but this behavior is not documented, so operators who want to enable profiling have no spec to follow.
3. The `check_ip_whitelist` call passes `None` for `remote_ip` unconditionally — the IP whitelist guard designed in the threat model is inert in the current implementation.

**GAP-4: `ferrosa-ctl` web-API commands (`add-node`, `decommission`, `ring`, `rebalance`) have no live HTTP integration test.**

URL construction and body serialisation are covered by unit tests in `ferrosa-ctl/tests/integration.rs` and `commands.rs::mod tests`. However, no test starts the axum web server, sends a real HTTP request, and validates the response JSON. The ctl command success path (printing the response) and error path (non-2xx → `Err`) are only tested by inspecting static strings.

### P2 — Medium

**GAP-5: `POST /api/debug/force-compact` has no spec document and minimal handler body.**

The handler exists (`force_compact_handler`, `debug.rs` line 70–76) and is 7 lines. It is not referenced in any spec. Its auth model defers to the same inline `check_auth` (Bearer token) as the flamechart, but the handler body is not shown in the digest output — it may be a stub.

**GAP-6: `ferrosa-worker` has only 3 tests and no spec for failure modes.**

The worker process receives JSON on stdin and is the only binary with no network surface. Its failure modes (malformed JSON, S3 read error, index build crash) are not covered by an FMEA or threat model, and the 3 existing tests only exercise the happy-path deserialisation.

**GAP-7: WebSocket endpoint (`/api/ws`) has no spec document.**

The `ws.rs` module is well-tested (26 internal tests + 2 integration tests in `web_ws.rs`) but there is no spec describing the JSON protocol, supported message types (`subscribe`, `unsubscribe`), error semantics, or subscription lifecycle. Client developers have only the source code as a reference.

---

## 5. Recommendations

**R1 (P0): Write auth middleware tests before the live auth.rs patch lands.**
Add at minimum: (a) a test that boots the axum router with `auth_disabled: false` and verifies a 401 response with no `Authorization` header; (b) a test with a valid superuser credential returns 200; (c) a test with a non-admin role returns 403. These should live in `web/auth.rs::mod tests`, which currently has zero tests.

**R2 (P0): Add an end-to-end restore integration test.**
Create a test in `ferrosa/tests/` (or a new `ferrosa-ctl/tests/restore_e2e.rs`) that: starts the web server with a real `WebAppState`, calls `POST /api/snapshots` to create a snapshot, then calls `POST /api/restore/preflight` to validate, then `POST /api/restore` to trigger — asserting both the HTTP status codes and the downstream `StorageEngine` state. The `--point-in-time` path must be covered separately.

**R3 (P1): Write a one-page API reference for the debug endpoints.**
Add `specs/coverage/debug-api.md` (or append to `specs/observability-architecture.md`) with: the full URL, the `FERROSA_DEBUG_AUTH_TOKEN` setup steps, the `seconds` parameter range, the rate limit (1 concurrent), the `image/svg+xml` response contract, and the note that `check_ip_whitelist` is currently inert (passing `None` as the remote IP). This closes the operator-facing documentation gap for both `flamechart` and `force-compact`.

**R4 (P1): Wire `check_ip_whitelist` to the real remote IP.**
`flamechart_handler` calls `check_ip_whitelist(None)` — the whitelist guard is never exercised because `remote_ip` is always `None`. Either pass `ConnectInfo<SocketAddr>` as an extractor argument (axum supports this), or remove the check until it can be wired correctly. As-is, the designed mitigation (OBS-T1, threat model) is inert.

**R5 (P2): Add a `ferrosa-worker` FMEA covering S3 and index-build failure modes.**
The worker has no retry logic, no circuit breaker, and exits on any error — this is acceptable (fail loud), but the failure modes (malformed task descriptor, S3 permission denied, index build OOM) should be enumerated so operators know what to expect in the task queue when a worker exits non-zero.
