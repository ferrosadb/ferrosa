# Wire Up Observability: Virtual Tables + Auth + WebSocket Push

## Context

The ferrosa web console (`/api/*` endpoints) returns empty arrays because virtual tables are never registered at startup. The dashboard also lacks authentication and uses polling-based REST. This plan wires up the virtual tables, adds Basic auth requiring Admin/Operator role, and replaces polling with WebSocket push driven by each table's `SubscriptionMode`.

## Files to Modify

| File | Action |
|------|--------|
| `ferrosa/Cargo.toml` | Add `base64 = "0.22"`, `serde = "1"` (with derive), enable axum `ws` feature |
| `ferrosa/src/main.rs` | Register virtual tables, pass schema to web server |
| `ferrosa/src/web/mod.rs` | New `WebAppState`, `FromRef` impls, add auth layer + ws route |
| `ferrosa/src/web/auth.rs` | **New** — Basic auth middleware with role check |
| `ferrosa/src/web/ws.rs` | **New** — WebSocket handler with subscription-driven push |
| `ferrosa/src/web/api.rs` | Make `virtual_table_to_json` `pub(crate)` |

## Step 1: Wire Up Virtual Table Registration

**`ferrosa/src/main.rs`** — After SharedState is constructed (line 174), before the web server starts (line 180):

```rust
// Register virtual tables into the registry
let vt_registry = Arc::new(ferrosa_schema::VirtualTableRegistry::new());
vt_registry.register(Arc::new(
    ferrosa_cql::virtual_tables::ConnectionsTable::new(
        shared_state.connection_tracker.clone(),
    ),
));
vt_registry.register(Arc::new(
    ferrosa_cql::virtual_tables::ActiveQueriesTable::new(
        shared_state.query_tracker.clone(),
    ),
));
```

Clone the `Arc`s from `shared_state` after construction — no need to restructure existing code.

## Step 2: Auth Middleware

**`ferrosa/src/web/auth.rs`** (new file) — Follow the pattern from `ferrosa-graph/src/http.rs:107-190`:

1. Extract `Authorization: Basic <base64>` header
2. Decode, split on `:`, call `Schema::authenticate(username, password)`
3. **Role check**: superuser passes; otherwise walk `member_of` chain checking for "admin" or "operator" role names
4. When `auth_disabled` is true, skip all checks
5. Return `401` with `WWW-Authenticate: Basic realm="ferrosa"` on failure, `403` if authenticated but wrong role

## Step 3: Refactor Web State

**`ferrosa/src/web/mod.rs`** — Unify state:

```rust
#[derive(Clone)]
pub struct WebAppState {
    pub registry: Arc<VirtualTableRegistry>,
    pub mode_controller: Arc<ModeController>,
    pub schema: Arc<Schema>,
    pub auth_disabled: bool,
}
```

Implement `FromRef<WebAppState>` for `Arc<VirtualTableRegistry>` and `Arc<ModeController>` so existing handler signatures in `api.rs` continue to work unchanged.

Update `build_router` and `start_web_server` to accept `WebAppState`. Apply auth middleware as a layer:

```rust
Router::new()
    .nest("/api", api::routes(...))
    .nest("/api/cluster", api::cluster_routes(...))
    .route("/api/ws", get(ws::ws_handler))
    .fallback(static_files::static_handler)
    .layer(middleware::from_fn_with_state(state.clone(), auth::auth_middleware))
    .with_state(state)
```

Update `start_web_server` signature; pass `schema.clone()` from main.rs. Read `FERROSA_AUTH_DISABLED` in `WebConfig::from_env()`.

## Step 4: WebSocket Push

**`ferrosa/src/web/ws.rs`** (new file):

**Protocol:**

```
Client -> Server:  {"type": "subscribe", "table": "connections"}
Client -> Server:  {"type": "unsubscribe", "table": "connections"}
Server -> Client:  {"type": "data", "table": "connections", "rows": [...]}
Server -> Client:  {"type": "error", "message": "unknown table: foo"}
```

**Implementation:**

- `ws_handler` accepts `WebSocketUpgrade` + `State<WebAppState>`
- After upgrade, split socket into sender/receiver
- Use `mpsc` channel to funnel outbound messages to sender task
- On `subscribe`: look up table in registry, spawn poll task at interval from `subscription_mode()`:
  - `Pollable` → 2s default
  - `DemandDriven { default_interval }` → use that interval (500ms for active_queries)
  - `None` → send one snapshot, don't loop
- On `unsubscribe`: cancel the poll task via `JoinHandle::abort()`
- Reuse `api::virtual_table_to_json()` (make it `pub(crate)`) for serialization

**Dependencies:** `axum = { version = "0.8", features = ["ws"] }`, `futures` for `SinkExt`/`StreamExt` on split socket.

## Step 5: Cargo.toml Updates

```toml
axum = { version = "0.8", features = ["ws"] }
base64 = "0.22"
serde = { version = "1", features = ["derive"] }
futures = "0.3"
```

## Verification

1. **Virtual tables**: `cargo test -p ferrosa` — existing tests pass
2. **Build**: `cargo build -p ferrosa` compiles
3. **Docker rebuild**: Rebuild UAT cluster in ferrosa-test, verify:
   - `curl http://localhost:9190/api/tables` returns `["connections","active_queries"]`
   - `curl http://localhost:9190/api/connections` returns 401 (when auth enabled)
   - `curl -u cassandra:cassandra http://localhost:9190/api/connections` returns connection data
4. **WebSocket**: Connect with `websocat` or similar, send subscribe, observe periodic pushes

## Changes Already Applied (partial, from prior session)

- `ferrosa/Cargo.toml`: axum ws feature, base64, futures, serde added
- `ferrosa/src/web/api.rs`: `virtual_table_to_json` changed to `pub(crate)`

These should be verified against the actual module structure before continuing.
