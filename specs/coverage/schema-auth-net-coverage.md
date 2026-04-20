---
type: coverage-review
scope: ferrosa-schema, ferrosa-common (excl. accord.rs), ferrosa-net (excl. accord_messages.rs)
created: 2026-04-18
status: current
---

# Coverage Review — Schema, Auth, and Internode Networking

## 1. Feature Inventory

### ferrosa-schema (27 source files)

| Feature | File | Key symbols |
|---|---|---|
| Schema registry / SchemaSnapshot | `src/registry.rs` L38, L124 | `Schema`, `SchemaSnapshot`, `SchemaConfig` |
| Keyspace DDL | `src/registry.rs` L779–L865 | `create_keyspace`, `alter_keyspace`, `drop_keyspace` |
| Table DDL | `src/registry.rs` L874–L1037 | `create_table`, `alter_table`, `drop_table` |
| Index DDL | `src/registry.rs` L1045–L1116 | `create_index`, `drop_index` |
| UDT DDL | `src/registry.rs` L1346–L1472 | `create_type_internal`, `alter_type_add_field`, `alter_type_rename_field` |
| UDF/aggregate DDL | `src/registry.rs` L1452–L1595 | `create_function_internal`, `create_aggregate_internal` |
| Role management | `src/registry.rs` L1125–L1340 | `create_role`, `alter_role`, `drop_role`, `grant`, `revoke` |
| Role / permission metadata | `src/auth/role.rs`, `src/auth/permission.rs` | `RoleMetadata`, `AuthContext`, `Permission`, `Resource`, `GrantEntry` |
| Password hashing | `src/auth/password.rs` | `PasswordHasher` (bcrypt + argon2id), `PasswordPolicy`, `verify_password_any` |
| Rate limiting | `src/auth/rate_limit.rs` | `AuthRateLimiter`, `RateLimitConfig`, exponential back-off |
| Bootstrap / default roles | `src/auth/bootstrap.rs` L59 | `seed_default_roles`, `admin_password_is_default` |
| Authenticate | `src/registry.rs` L564–L633 | `Schema::authenticate` (rate-check, verify, auto-rehash) |
| Permission check | `src/auth/permission.rs` L109 | `check_permission`, `has_permission_recursive`, `resource_matches` |
| Audit event model | `src/audit/event.rs` | `AuditEvent`, `AuditEventKind` (auth + DDL events) |
| AuditSink trait | `src/audit/mod.rs` L14 | `AuditSink`, `TestAuditSink`, `CompositeSink` |
| LogAuditSink (production default) | `src/audit/log_sink.rs` | `LogAuditSink` — structured JSON via `tracing` |
| SystemTableAuditSink (in-memory ring buffer) | `src/audit/table_sink.rs` | `SystemTableAuditSink`, `AuditLogEntry` |
| system_schema virtual tables | `src/system/schema_tables.rs`, `src/system/index_tables.rs`, `src/system/type_tables.rs`, `src/system/aggregate_tables.rs`, `src/system/function_tables.rs` | `query_keyspaces`, `query_tables`, `query_columns`, VirtualTable impls |
| system_auth tables | `src/system/auth_tables.rs` | `query_roles`, `query_role_members`, `query_role_permissions`, `query_audit_log` |
| system_observability schemas | `src/system/observability.rs` | `spans_table_schema`, `metrics_table_schema`, `slow_queries_table_schema` |
| system.local / system.peers | `src/system/local.rs`, `src/system/peers.rs` | `LocalInfo`, `PeerInfo`, `ClusterState` trait |
| system_schema persistence encoding | `src/system/persistence.rs` | `SystemTableMutation`, `keyspace_to_row`, `table_to_rows`, `role_to_row`, `grant_to_row` |
| Virtual table framework | `src/virtual_table.rs`, `src/virtual_registry.rs` | `VirtualTable` trait, `VirtualTableRegistry` |
| SecretsProvider | `src/secrets/mod.rs`, `src/secrets/env.rs` | `SecretsProvider` trait, `EnvSecretsProvider` |
| Production mode checks | `src/startup.rs` | `DeploymentMode`, `ProductionViolation`, `validate_production_requirements` |
| Schema validation | `src/validation.rs` | `validate_table`, `validate_keyspace` |
| CQL↔storage type conversion | `src/convert.rs` | `cql_to_marshal_type`, `to_storage_schema` |
| Error types | `src/error.rs` | `SchemaError` (22 variants) |

**Total: 27 source features tracked.**

---

### ferrosa-common (10 source files, excl. accord.rs)

| Feature | File | Key symbols |
|---|---|---|
| Token / Murmur3 partitioner | `src/token.rs`, `src/murmur3.rs` | `Token`, `hash3_x64_128` |
| PartitionKey / DecoratedKey | `src/key.rs` | `PartitionKey`, `DecoratedKey` |
| CellValue / tombstone / TTL / expiry | `src/cell.rs` | `CellValue`, `NO_TIMESTAMP`, `NO_TTL`, `NO_DELETION_TIME` |
| CQL type system | `src/cql_type.rs` | `CqlType`, `CqlValue`, ordered comparison |
| Storage schema re-export | `src/schema.rs` | `TableSchema`, `ColumnDefinition`, `PinConfig` |
| Data type (marshal) | `src/data_type.rs` | `DataType` |
| Error types | `src/error.rs` | `Error`, `Result` |
| Proptest generators | `src/test_generators.rs` | `arb_cell_value`, `arb_decorated_key` |

**Total: 8 features tracked (accord.rs excluded per scope).**

---

### ferrosa-net (13 source files, excl. accord_messages.rs)

| Feature | File | Key symbols |
|---|---|---|
| Frame codec (44-byte header, lanes, flags) | `src/codec.rs` | `InternodeCodec`, `FrameHeader`, `Lane` enum, `MsgType`, `TraceContext` |
| Message types | `src/message.rs` | `Message` enum (~30 variants), `encode`/`decode` |
| Lane actor (cancel-safe, actor pattern) | `src/lane_actor.rs` | `LaneHandle`, `lane_actor_loop`, `LaneCommand` |
| Raft lane on dedicated OS thread | `src/lane_actor.rs` L246 | `spawn_raft_lane_actor` — `std::thread::Builder`, single-threaded Tokio runtime |
| Priority pool (Control/Data/Raft lanes) | `src/pool.rs` | `PriorityPool`, per-lane `LaneHandle`, TLS connector wiring |
| RPC client | `src/rpc/client.rs` | `RpcClient`, `send`, `fire`, `connect_with_tls`, `BandwidthMetrics` |
| RPC server (accept loop, graceful drain) | `src/rpc/server.rs` | `RpcServer`, `start_and_get_addr`, `shutdown(drain_timeout)`, `CancellationToken` |
| RPC handler / registry | `src/rpc/handler.rs` | `RpcHandler` trait, `HandlerRegistry`, `PingHandler` |
| Peer manager / heartbeat loop | `src/peer.rs` | `PeerManager`, `run_heartbeat_loop`, `PeerEventListener` trait |
| HMAC-SHA256 handshake (PSK) | `src/handshake.rs` | `compute_auth_token`, `initiate_handshake`, `accept_handshake` |
| Reconnect / exponential back-off | `src/reconnect.rs` | `ExponentialBackoff`, `LaneState`, `MAX_RECONNECT_ATTEMPTS=12` |
| TLS (rustls) | `src/tls.rs` | `build_tls_acceptor`, `build_tls_connector`, cert+key loading |
| Clock skew tracker | `src/skew.rs` | `SkewTracker`, `record_heartbeat`, `is_outlier`, RTT P99 |
| Discovery / seed peers | `src/discovery/seeds.rs` | `SeedDiscovery`, `parse`, `from_config` |
| Error types | `src/error.rs` | `NetError` (9 variants) |
| Net config / env parsing | `src/config.rs` | `NetConfig`, `from_env` (TLS paths, PSK, bind addr) |

**Total: 16 features tracked.**

---

## 2. Spec Coverage Matrix

| ADR / Spec | Covered by code | Notes |
|---|---|---|
| ADR-003 Raft for metadata | Raft lane on dedicated OS thread (`spawn_raft_lane_actor`) | ferrosa-cluster owns Raft state machine; net provides the transport lane |
| ADR-004 Layered SSTable | Not in scope for these crates | — |
| ADR-005 Rust-native crates | Structural — all three crates are clean Rust | No Java FFI |
| ADR-006 Auth-first schema | Fully implemented: every mutating registry method requires `&AuthContext`; no auth-bypass paths | `FERROSA_AUTH_DISABLED=true` in docker-compose bypasses at a higher layer (design doc `design-cql-role-auth-rollout.md` Sprint A) |
| ADR-007 Configurable password hashing | `PasswordHasher` (Bcrypt/Argon2id), auto-upgrade on login, `verify_password_any` | No `vault`/`aws-sm` providers yet (ADR-009 scope) |
| ADR-008 Audit-first schema | `LogAuditSink` is production default; every registry mutating path calls `emit_audit`; `TestAuditSink` validates coverage | `SystemTableAuditSink` is implemented but NOT wired in production (see Gaps §4) |
| ADR-009 Pluggable secrets | `SecretsProvider` trait + `EnvSecretsProvider` shipped; Vault/AWS/file providers deferred | Consequence acknowledged in ADR |
| ADR-010 Production mode | `validate_production_requirements` checks S3-HTTP, default password, env secrets, password policy; CQL TLS and internode TLS checks are explicit stubs (`startup.rs:138`) | Stubs are commented as awaiting those crates |
| Cancel-safety conventions | Lane actor uses `reserve()+send()`, Raft lane on dedicated thread, `CancellationToken` in RpcServer; `PeerManager` send methods carry `# Cancel Safety` doc annotations | 24 public async fns; ~18 carry cancel-safety annotations (≈75%) |
| Observability architecture | `system_observability` table schemas defined (`spans_table_schema`, `metrics_table_schema`, `slow_queries_table_schema`); `FerrosaTelemetryLayer` integration is upstream in `ferrosa` crate | ferrosa-schema owns schema definitions only |

---

## 3. Test Coverage

### ferrosa-schema

| Test file | Tests | What is covered |
|---|---|---|
| `tests/integration.rs` | 8 | Full DDL workflow, permission denial, rate-limit lockout, audit event count (8 ops → 8 events), index create/drop idempotency |
| `tests/auth_integration.rs` | 4 | Bootstrap with/without `FERROSA_SUPERUSER_PASSWORD`, production mode violations, hash filtering (superuser sees hashes, non-superuser does not) |
| `tests/property_tests.rs` | 4 (proptest) | SchemaSnapshot serde roundtrip, superuser always authorized, bcrypt hash-verify |
| Unit tests (in-source) | ~16 | `startup.rs` production checks, `auth_tables.rs` audit_log access control, `table_sink.rs` ring-buffer eviction, `permission.rs` inheritance |

**Not tested:** `alter_table`, `alter_keyspace`, `UDT alter_type_rename_field`, argon2id path end-to-end, `CompositeSink`, virtual table `read()` with non-trivial predicates, `system.local`/`system.peers` query helpers.

### ferrosa-common

| Module | Tests |
|---|---|
| `murmur3.rs` | 7 (incl. Java compatibility vector) |
| `key.rs` | 6 (DecoratedKey ordering, PartitionKey comparison) |
| `cell.rs` | 4 (live/expiring/tombstone round-trips) |
| `cql_type.rs` | 5 (CqlValue ordering) |
| `schema.rs` | 5 (TableSchema helpers) |
| `token.rs` | 5 |
| `error.rs` | 3 |
| `data_type.rs` | 2 |

**Not tested:** `test_generators.rs` proptest strategies (used by other crates, not directly tested here), `serde_helpers` module.

### ferrosa-net

| Test file / module | Tests | What is covered |
|---|---|---|
| `tests/integration.rs` | 3 (async) | Two-peer handshake + message exchange, PSK auth accepted, PSK mismatch rejected |
| `src/codec.rs` | 14 | Frame encode/decode, flag bits, lane routing |
| `src/message.rs` | 18 | All major message type encode/decode roundtrips |
| `src/lane_actor.rs` | 3 | Lane send, fire-and-forget, status query |
| `src/skew.rs` | 7 | RTT sampling, outlier detection, percentile math |
| `src/handshake.rs` | 2 | HMAC token compute+verify roundtrip |
| `src/reconnect.rs` | 3 | Exponential back-off timing, cap, reset |
| `src/discovery/seeds.rs` | 4 | Seed parse, from_config |
| `src/pool.rs` | 1 | Three-lane connect (no TLS path tested) |

**Not tested:** TLS path through `PriorityPool` (no test certs provisioned), `RpcServer` + `RpcClient` unit tests (0 tests in those modules; covered only by integration tests), `PeerManager` heartbeat loop under simulated peer failure, bandwidth metrics accumulation, graceful drain under live traffic.

---

## 4. Gaps

### P0 — Correctness / Security Gaps

**G-P0-1: `SystemTableAuditSink` implemented but never wired in production**

`ferrosa/src/main.rs` L378 and L1259 both construct `SchemaConfig` with `Box::new(ferrosa_schema::LogAuditSink)`. `SystemTableAuditSink` is implemented, tested in isolation, and exported — but not connected to the production binary. The consequence: `system_auth.audit_log` CQL queries (`SELECT * FROM system_auth.audit_log`) will always return empty results in a running cluster.

- ADR-008 states the default sink is `LogAuditSink` (log-to-tracing), which is correct and intentional per the ADR text.
- However, `query_audit_log` in `src/system/auth_tables.rs` takes a `&SystemTableAuditSink` parameter — meaning any CQL path that exposes this table must thread an explicit sink reference. There is no `Schema`-level method to query the audit log through the configured `AuditSink`. If an operator runs `SELECT * FROM system_auth.audit_log` they will reach dead code or an empty ring buffer.
- **Threat-model impact (T-R1):** An attacker can perform repeated authentication attempts, CREATE/DROP operations, or permission escalations and leave no queryable audit trail at the CQL layer.

**Recommendation:** Either (a) expose `query_audit_log` via a method on `Schema` that returns an error if the configured sink is not a `SystemTableAuditSink`, with a clear error message, or (b) route both sinks simultaneously via `CompositeSink` and document that `system_auth.audit_log` requires `FERROSA_AUDIT_SINK=table` — and add a `ProductionViolation` when production mode is active without a table sink.

---

**G-P0-2: CQL-layer authentication bypass still active**

Per `design-cql-role-auth-rollout.md` §1, `FERROSA_AUTH_DISABLED=true` is set in the deployed `docker-compose.yml`. The CQL server accepts any `STARTUP` frame without credentials. The schema-side primitives (`authenticate`, `check_permission`, `grant`, `revoke`) are complete and correct, but they are unreachable from the network boundary.

- No CQL `AUTHENTICATE` response is emitted when `auth_disabled=true`.
- No seed-role bootstrap runs at cluster startup (Sprint A item 1 in the design doc).
- `query_audit_log` returns an empty ring buffer for the reason in G-P0-1.

This is a known, documented gap (Sprint A of the design doc), but it is the highest-priority open item for production readiness. It is listed here because it means the entire auth enforcement layer in `ferrosa-schema` is effectively unexercised in integration at the network boundary.

---

### P1 — Functional Gaps

**G-P1-1: Production mode TLS checks are stubs**

`ferrosa-schema/src/startup.rs:138` contains the comment:
```
// CQL TLS and internode TLS checks are stubs (added when those crates land)
```
The `ProductionViolation` enum includes `CqlTlsNotConfigured`, `CqlMtlsNotConfigured`, `InternodeTlsNotConfigured`, `InternodeMtlsNotConfigured`, but `validate_production_requirements` never emits them. A node running `FERROSA_MODE=production` without TLS configured will not fail startup.

- `ferrosa-net` has TLS infrastructure (`src/tls.rs`, `NetConfig` fields `tls_cert_path`/`tls_key_path`/`require_tls`) but the startup validation does not query `NetConfig`.
- ADR-010 explicitly acknowledges this is incremental but names it as a consequence to close when CQL and internode crates land. Both have landed — the hook has not been wired.

---

**G-P1-2: `cancel-safety` doc annotation coverage is incomplete**

The convention (`specs/cancel-safety-conventions.md`) requires all public `async fn` to include a `# Cancel Safety` section. `ferrosa-net` has 24 public async functions; approximately 6 are missing the annotation (notably: `PriorityPool::connect`, `PriorityPool::all_lanes_resolved`, `PeerManager::add_peer`, `PeerManager::add_peer_entry`, `PeerManager::record_heartbeat`, `PeerManager::remove_peer`).

This is not a runtime bug but is a documentation/review gap that could lead to cancel-unsafe callers.

---

### P2 — Quality / Coverage Gaps

**G-P2-1: No TLS integration test in ferrosa-net**

`tests/integration.rs` has three tests — PSK accepted, PSK mismatch, and plain two-peer exchange. None tests the TLS code path (`src/tls.rs`, `connect_with_tls`). The TLS builder functions are exercised only at compile time. A regression in certificate loading or SNI matching would not be caught by CI.

**G-P2-2: `alter_table`, UDT mutation, and argon2id paths lack integration tests**

- `alter_table` (adding/dropping columns, changing table params) has no integration test despite having non-trivial merge logic in `src/registry.rs` L922–L990.
- `alter_type_rename_field` and `alter_type_add_field` have no test at all.
- The argon2id branch of `PasswordHasher` is exercised only by property tests with bcrypt; no test sets `PasswordHasher::Argon2id` and authenticates through `Schema::authenticate` end-to-end (including the auto-rehash path).

---

## 5. Recommendations

**R1 (P0): Resolve the `SystemTableAuditSink` dead-end**

Add a `CompositeSink` configuration path or a `Schema::query_audit_log(&self, auth: &AuthContext)` method that dispatches to the configured sink if it is a table sink, and returns a documented error otherwise. Wire `ProductionViolation::AuditSinkNotQueryable` when production mode runs with `LogAuditSink` only. This closes the T-R1 threat gap without requiring operators to choose between auditability and log-forwarding.

**R2 (P0): Complete Sprint A of `design-cql-role-auth-rollout.md`**

The highest-leverage change: wire the CQL `AUTHENTICATE` / `AUTH_RESPONSE` frames through to `Schema::authenticate`, remove `FERROSA_AUTH_DISABLED` from `docker-compose.yml`, and add the cluster-startup seed-role bootstrap. All schema-side primitives are done; this is plumbing work only.

**R3 (P1): Wire TLS status into `validate_production_requirements`**

Add a `NetConfigSummary { has_tls_cert: bool, require_tls: bool }` field to `ProductionCheckConfig` (or call into `ferrosa-net` directly) so the four TLS `ProductionViolation` variants are actually emitted. Close the stub comment at `startup.rs:138`.

**R4 (P1): Add cancel-safety annotations to the six uncovered async fns**

Document `PriorityPool::connect`, `all_lanes_resolved`, and the four `PeerManager` methods per the convention. `connect` and `add_peer` are cancel-unsafe (they commit state before the future resolves) and should say so explicitly.

**R5 (P2): Add ferrosa-net TLS integration test and schema `alter_table` test**

Generate a self-signed cert in the test fixture (using `rcgen` or a pre-generated test cert committed to the repo) and add a fourth integration test that connects `PriorityPool` with TLS. Add an `alter_table` integration test that adds a column, verifies it in the snapshot, and drops it — exercising the merge logic that is otherwise untested.
