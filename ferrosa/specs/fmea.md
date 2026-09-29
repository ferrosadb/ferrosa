---
crate: ferrosa
doc: fmea
last_updated: 2026-08-27
---

# ferrosa — FMEA / Known Issues

Failure modes are ranked by **RPN = Severity × Occurrence × Detection** (1–10
each; higher = worse). As the composition root, this crate's failures are
*startup, ordering, lifecycle, and configuration* faults — the subsystem crates
own their own internal FMEAs.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| FE-1 | **Partial-boot: a non-critical listener fails to bind but the process keeps running.** Graph/Bolt/SPARQL/Flight servers are `spawn`-ed and a bind error is only logged (`tracing::error!`), never propagated. | Node looks "up" (CQL serving) but a front-end the operator expects is silently absent. No health signal distinguishes "disabled" from "failed to bind". | 7 | 4 | 7 | 196 | **Open gap.** Bind failures log loudly but the process does not crash or surface them in `/readyz`. Add a startup readiness aggregate that fails loud when an *enabled* listener didn't bind. |
| FE-2 | **Startup ordering regression.** A future edit registers a listener or replay step before its dependency (engine before CdcBus, mutation replay before table registration, SessionCore before PeerManager). | Recovery breaks silently — e.g. "table not registered: system_schema.*" during Raft replay drops persistence; or LWT returns ServerError because peer_manager was None. | 9 | 2 | 6 | 108 | The order is encoded only as imperative `main` code + comments. Mitigated by integration/cluster suites; **no in-crate guard test asserts the order.** |
| FE-3 | **Auth kill-switch resolves open in production.** Storage default is `auth_enabled=false`; if neither `FERROSA_AUTH_ENABLED` nor `[cql].auth_enabled` is set, `resolve_auth_disabled` returns `auth_disabled=true` for CQL **and** the web/graph/Bolt/SPARQL consoles (which inherit the same flag). | An operator who forgets the switch ships an unauthenticated cluster *and* an open admin console (`/admin/*`, cluster promote/decommission) — but drivers connect, so it looks fine. | 9 | 3 | 5 | 135 | Documented as "single source of truth" + a 5-minute default-password WARN when auth *is* on. **Gap:** the default is permissive and silent; consider failing loud (or defaulting closed) in `DeploymentMode::Production`. |
| FE-4 | **Flight signing key ephemeral when unset.** With `--features flight` and no `FERROSA_FLIGHT_SIGNING_KEY`, a random per-process key is used. | Bearer tokens don't survive a restart and don't validate across nodes; clients see sudden auth failures after any bounce. | 5 | 4 | 4 | 80 | Loud WARN at startup naming the env var; functional but fragile. Make it fail-loud (refuse to start) when `DeploymentMode::Production`. |
| FE-5 | **host_id silently regenerated.** A corrupt/empty/missing `host_id` file regenerates a new UUID; if this node was a cluster member, its identity (and ring position) is lost. | New identity ⇒ orphaned data, ring imbalance, ghost membership. | 8 | 2 | 4 | 64 | **Mitigated (BUG-008):** `classify_host_id_state` emits a distinct ERROR/WARN per case naming the path + bad content + new id. Detection now depends on log review, not silence. |
| FE-6 | **Internode bind on the wrong port.** Default is `17000`; a config that re-introduces `7000` collides with macOS ControlCenter (BUG-001) or another service. | Opaque `EADDRINUSE` crash, or peers can't connect. | 6 | 2 | 3 | 36 | Default moved to 17000 with an explicit code comment + a `ferrosa-net` guard test (`default_bind_port_is_not_7000`). Invalid TOML bind is logged and ignored, never fatal. |
| FE-7 | **Shutdown timeout drops un-flushed data.** The 30 s graceful drain (`mode_controller.shutdown` → internode drain → memtable flush → schema persist) can exceed the window under heavy flush/S3 load. | Memtables not flushed ⇒ data loss on restart for anything not yet in an SSTable or commit log. | 8 | 2 | 5 | 80 | SIGTERM is handled (not just SIGINT) so container stops drain; timeout logs "shutdown timed out after 30s". **Gap:** the timeout is fixed and the partial-flush case is not surfaced as a metric. |
| FE-8 | **S3 bootstrap / index-UDT-UDF replay failures are non-fatal.** On cold start, schema bootstrap and the `system_schema.*` re-registration steps log a WARN and continue on error. | A node can come up "fresh" or missing secondary indexes / UDTs / UDFs that exist in S3, serving incomplete schema without crashing. | 7 | 3 | 6 | 126 | Each step logs the failure with table/keyspace context; designed as best-effort with local→S3→fresh priority. **Gap:** no readiness gate distinguishes "fresh by design" from "bootstrap failed". |
| FE-9 | **Maintenance-loop sub-task panics swallowed.** Flush/S3-sync run on detached `std::thread`s; the schema-sync path `continue`s on flush failure to avoid persisting schema ahead of data. | A persistently failing flush silently stops schema persistence; a panicked sync thread is logged but the loop continues. | 6 | 3 | 6 | 108 | Failures are logged per tick; the "skip schema persist if flush failed" guard is correct fail-loud-ish behavior. **Gap:** no escalation/metric if a tick keeps failing. |
| FE-10 | **TOML internode broadcast omitted from peer handshake.** `[internode].broadcast` updated `broadcast_addr` but left `internode_broadcast=None`; same-host nodes using distinct internode ports therefore could not advertise their canonical reverse-dial endpoint. | The cluster seed substitutes its own port for inbound peers, routes multiple host IDs back to itself, and reports successful invites while joiners remain in pair mode. | 9 | 4 | 7 | 252 | **Fixed in code, live re-verification pending (2026-08-07):** `apply_internode_toml_overrides` preserves the exact TOML value after validation. Regression `apply_internode_toml_overrides_sets_other_fields` asserts both resolved and advertised forms. The cluster-side reverse-address selection is tracked as CL-16. |
| FE-11 | **Two incompatible writers race on `schema.json`.** The composition root wrote a full registry object while storage flush wrote a table-only array; whichever ran last selected the next boot's apparent schema. | A healthy node can restart with an empty/partial CQL registry, hide intact SSTables, and overwrite the only good local evidence. | 10 | 3 | 8 | 240 → 10 | **Fixed:** startup validates the bounded, discriminated registry document before opening storage. `SchemaSnapshotStore` exclusively publishes it with a file lock, verified staging, fsync/rename/directory-fsync, quarantine, and three generations. Storage-only metadata moved to `storage-schema.json`; parse/apply/register errors now return from startup rather than warn-and-continue. |
| FE-12 | **A background client listener fails to bind and the node still probes ready.** Postgres, SPARQL, graph HTTP and Bolt each logged one ERROR and carried on. | An orchestrator or operator sees a healthy node with a client port missing; clients get connection refused with no signal on the node. | 7 | 5 | 8 | 280 → 35 | **Mitigated:** every background listener records itself in `ListenerStatus`; a failure makes `/readyz` return 503 `waiting_for: listeners` naming the listener and reason, and `ferrosa_listener_up{listener}` goes to 0. A failure is deliberately not fatal (a SPARQL port clash should not take down a database that is serving CQL). Residual: graph HTTP, Bolt and SPARQL learn of a bind failure only when their server task returns, so a listener that dies silently without returning is not detected. Tests: `listener_status::tests::*`, `readyz_is_not_ready_while_a_client_listener_has_failed`, `metrics_export_the_health_of_the_background_listeners`. |
| FE-13 | **Unusable configuration values are dropped silently.** `FERROSA_CQL_BROADCAST` fell back to `127.0.0.1`; `NetConfig::from_env` ignored unparseable values, so `FERROSA_INTERNODE_REQUIRE_TLS=ture` silently disabled the TLS requirement. | Drivers told to connect to loopback; a node that was meant to refuse plaintext internode traffic accepts it. | 8 | 4 | 8 | 256 → 32 | **Fixed:** `parse_cql_broadcast` returns an error naming the value; `NetConfig::from_lookup` reports a `ConfigIssue` for every rejected value and `from_env_checked` makes typos fatal at startup (seed and broadcast hostnames that may not resolve yet warn only). Tests: `cql_broadcast::tests::an_unusable_value_is_an_error_that_names_it`, `ferrosa-net` `config::tests::*`. |

| FE-T300 | **Startup outside standalone with jsonb columns in the persisted schema (T-300, D24, D15a).** Seeds configured or a former cluster member, with jsonb tables restored. | A node joins peers with jsonb and no capability ledger. | 9 | 2 | 2 | 36 | **Fixed:** `ModeController::check_startup_jsonb` runs right after controller construction; `main` logs FATAL naming the tables and returns the error (non-zero exit). No flag bypasses it. Test: `startup_with_jsonb_outside_standalone_is_refused_naming_tables` (ferrosa-cluster). |

## Top risks to act on

1. **FE-10 (RPN 252) — TOML internode advertisement.** The focused regression is
   green, but the launchd cluster must be rebuilt once with this binary to prove
   all three nodes leave pair mode and recover one Raft group.
2. **FE-1 (RPN 196) — partial boot.** A failed bind of an *enabled* front-end
   should fail loud (crash or fail `/readyz`), not log-and-continue. Today a node
   can serve CQL while its Postgres/Flight/graph listener never came up, with no
   health distinction from "disabled."
3. **FE-3 (RPN 135) — auth resolves open by default.** The permissive default is
   silent; in production mode the composition root should default closed or
   refuse to start without an explicit auth decision, since the same flag also
   opens the admin REST surface.

## Detection assets

- In-crate unit tests for every config resolver (env→TOML→default), `host_id`
  classification, hinted-handoff dir, schema persist/load, web auth bypass.
- `/readyz` (un-authenticated) and `/metrics` on the web console.
- Per-step structured WARN/ERROR logs across bootstrap, replay, and shutdown.
- `ferrosa-net` `default_bind_port_is_not_7000` guard (FE-6).
- `apply_internode_toml_overrides_sets_other_fields` pins TOML broadcast
  propagation into the handshake advertisement (FE-10).

### Pump wiring acceptance (T-045)

The CQL wiring acceptance test writes compressed and plain tables through a real client/server, includes a row larger than a pump segment, flushes and compacts, tears down the first runtime, and verifies all rows through a reopened engine and fresh server.

Compaction stop scope is validated before registry access; unknown/partial scope
cannot fall through to node-wide cancellation. The route is POST-only and remains
behind the shared admin/operator authentication middleware. Tests exercise real
queued compactions on two tables and verify selected cancellation preserves inputs.
