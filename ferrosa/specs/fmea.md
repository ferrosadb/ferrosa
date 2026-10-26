---
crate: ferrosa
doc: fmea
last_updated: 2026-10-11
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
| FE-3 | **Auth kill-switch resolves open in production.** Storage default is `auth_enabled=false`; if neither `FERROSA_AUTH_ENABLED` nor `[cql].auth_enabled` is set, `resolve_auth_disabled` returns `auth_disabled=true` for CQL **and** the web/graph/Bolt/SPARQL consoles (which inherit the same flag). | An operator who forgets the switch ships an unauthenticated cluster *and* an open admin console (`/admin/*`, cluster promote/decommission) — but drivers connect, so it looks fine. | 9 | 3 | 5 | 135 | Documented as "single source of truth" + a 5-minute default-password WARN when auth *is* on. **Gap:** the default is permissive and silent; consider failing loud (or defaulting closed) in `DeploymentMode::Production`. As of ADR-024 the PostgreSQL front-end no longer hides its half of this: with `auth_enabled=false` it stays bound and refuses every login by name (FATAL 28000, "no roles are configured on this node …") and logs the same once at startup, so an operator who forgot the switch cannot mistake the box for a working database. |
| FE-4 | **Flight signing key ephemeral when unset.** With `--features flight` and no `FERROSA_FLIGHT_SIGNING_KEY`, a random per-process key is used. | Bearer tokens don't survive a restart and don't validate across nodes; clients see sudden auth failures after any bounce. | 5 | 4 | 4 | 80 | Loud WARN at startup naming the env var; functional but fragile. Make it fail-loud (refuse to start) when `DeploymentMode::Production`. |
| FE-5 | **host_id silently regenerated.** A corrupt/empty/missing `host_id` file regenerates a new UUID; if this node was a cluster member, its identity (and ring position) is lost. | New identity ⇒ orphaned data, ring imbalance, ghost membership. | 8 | 2 | 4 | 64 | **Mitigated (BUG-008):** `classify_host_id_state` emits a distinct ERROR/WARN per case naming the path + bad content + new id. Detection now depends on log review, not silence. |
| FE-6 | **Internode bind on the wrong port.** Default is `17000`; a config that re-introduces `7000` collides with macOS ControlCenter (BUG-001) or another service. | Opaque `EADDRINUSE` crash, or peers can't connect. | 6 | 2 | 3 | 36 | Default moved to 17000 with an explicit code comment + a `ferrosa-net` guard test (`default_bind_port_is_not_7000`). Invalid TOML bind used to be logged and ignored; it is now **fatal** (`apply_internode_toml_overrides` returns an `Err` naming the value, and `main` exits non-zero), matching the env path (`from_env_checked`). |
| FE-7 | **Shutdown timeout drops un-flushed data.** The 30 s graceful drain (`mode_controller.shutdown` → internode drain → memtable flush → schema persist) can exceed the window under heavy flush/S3 load. | Memtables not flushed ⇒ data loss on restart for anything not yet in an SSTable or commit log. | 8 | 2 | 5 | 80 | SIGTERM is handled (not just SIGINT) so container stops drain; timeout logs "shutdown timed out after 30s". **Gap:** the timeout is fixed and the partial-flush case is not surfaced as a metric. |
| FE-8 | **S3 bootstrap / index-UDT-UDF replay failures are non-fatal.** On cold start, schema bootstrap and the `system_schema.*` re-registration steps log a WARN and continue on error. | A node can come up "fresh" or missing secondary indexes / UDTs / UDFs that exist in S3, serving incomplete schema without crashing. | 7 | 3 | 6 | 126 | Each step logs the failure with table/keyspace context; designed as best-effort with local→S3→fresh priority. A **partial** S3 bootstrap is now loud and reported as *failed* (FE-18): `bootstrap_from_s3` names the tables that failed, returns an error, and never registers a table whose data did not come down, so the caller does not persist the schema and a later restart retries. **Gap:** no readiness gate distinguishes "fresh by design" from "bootstrap failed". |
| FE-9 | **The flusher or the maintenance loop dies or hangs and the node stays up.** Flushes ran on detached threads; a panic was one `flush task panicked` ERROR line, a hung flush blocked every arm of the loop (`rx.await` had no deadline), and the loop's own JoinHandle was dropped. 2026-10-02 node2: a `writer.rs:1965` panic, then no flush, compaction or schema persist for a day while `/readyz` said 200 (t_7681b32b). | Memtables and commit-log retention grow without bound; reads are served; nothing reports it. | 9 | 3 | 9 | 243 → 27 | **Mitigated:** `supervisor.rs` + `maintenance.rs`. Each flush is a thread behind `catch_unwind`, waited on for at most `FERROSA_FLUSH_STALL_DEADLINE_SECS` (300); a panic is restarted on the next tick, a hang is a stall (one per deadline) and no longer wedges the loop; the loop is restarted if it panics or returns. `/readyz` 503 `background_tasks` and `ferrosa_supervised_task_up` = 0 while impaired; ERROR lines on the edges. Panics and stalls past the intensity (3 per hour by default) sync the commit log and abort with a FATAL line. A flush that returns an error is impaired but does not escalate. S3 sync threads now log a spawn or runtime-build failure instead of discarding it. The flush restart window is shared across loop incarnations (it used to reset when the loop restarted; `the_flush_restart_window_survives_a_maintenance_loop_restart`). Tests: `supervisor::tests::*`, `readyz_is_not_ready_while_a_supervised_task_is_impaired`, `get_metrics_returns_prometheus_text`; storage `engine::flush_panic_recovery::*`. |
| FE-10 | **TOML internode broadcast omitted from peer handshake.** `[internode].broadcast` updated `broadcast_addr` but left `internode_broadcast=None`; same-host nodes using distinct internode ports therefore could not advertise their canonical reverse-dial endpoint. | The cluster seed substitutes its own port for inbound peers, routes multiple host IDs back to itself, and reports successful invites while joiners remain in pair mode. | 9 | 4 | 7 | 252 | **Fixed in code, live re-verification pending (2026-08-07):** `apply_internode_toml_overrides` preserves the exact TOML value after validation. Regression `apply_internode_toml_overrides_sets_other_fields` asserts both resolved and advertised forms. The cluster-side reverse-address selection is tracked as CL-16. |
| FE-11 | **Two incompatible writers race on `schema.json`.** The composition root wrote a full registry object while storage flush wrote a table-only array; whichever ran last selected the next boot's apparent schema. | A healthy node can restart with an empty/partial CQL registry, hide intact SSTables, and overwrite the only good local evidence. | 10 | 3 | 8 | 240 → 10 | **Fixed:** startup validates the bounded, discriminated registry document before opening storage. `SchemaSnapshotStore` exclusively publishes it with a file lock, verified staging, fsync/rename/directory-fsync, quarantine, and three generations. Storage-only metadata moved to `storage-schema.json`; parse/apply/register errors now return from startup rather than warn-and-continue. |
| FE-12 | **A background client listener fails to bind and the node still probes ready.** Postgres, SPARQL, graph HTTP and Bolt each logged one ERROR and carried on. | An orchestrator or operator sees a healthy node with a client port missing; clients get connection refused with no signal on the node. | 7 | 5 | 8 | 280 → 35 | **Mitigated:** every background listener records itself in `ListenerStatus`; a failure makes `/readyz` return 503 `waiting_for: listeners` naming the listener and reason, and `ferrosa_listener_up{listener}` goes to 0. A failure is deliberately not fatal (a SPARQL port clash should not take down a database that is serving CQL). Residual: graph HTTP, Bolt and SPARQL learn of a bind failure only when their server task returns, so a listener that dies silently without returning is not detected. Tests: `listener_status::tests::*`, `readyz_is_not_ready_while_a_client_listener_has_failed`, `metrics_export_the_health_of_the_background_listeners`. |
| FE-13 | **Unusable configuration values are dropped silently.** `FERROSA_CQL_BROADCAST` fell back to `127.0.0.1`; `NetConfig::from_env` ignored unparseable values, so `FERROSA_INTERNODE_REQUIRE_TLS=ture` silently disabled the TLS requirement. | Drivers told to connect to loopback; a node that was meant to refuse plaintext internode traffic accepts it. | 8 | 4 | 8 | 256 → 32 | **Fixed:** `parse_cql_broadcast` returns an error naming the value; `NetConfig::from_lookup` reports a `ConfigIssue` for every rejected value and `from_env_checked` makes typos fatal at startup (seed and broadcast hostnames that may not resolve yet warn only). Tests: `cql_broadcast::tests::an_unusable_value_is_an_error_that_names_it`, `ferrosa-net` `config::tests::*`. |
| FE-14 (t_88479cda) | **The commit log's fsync thread dies or wedges and the node keeps acknowledging writes.** Nothing watched `commitlog-periodic-sync`; its panic was a stderr line. | Writes acknowledged as durable but never fsynced; lost on power failure or crash. | 10 | 2 | 9 | 180 → 20 | **Mitigated:** the commit log refuses writes on its own (storage ST-71). `CommitLogSyncSupervisor` samples the thread's health every 100 ms: a death is a panic failure and the thread is restarted (it fsyncs what the dead one left first); each elapsed `FERROSA_COMMITLOG_SYNC_STALL_DEADLINE_MS` (2000) is a stall; past the intensity the node syncs the commit log and aborts. `/readyz` 503 and `ferrosa_supervised_task_up{task="commitlog_sync"}` = 0 while dead, failing or stalled. A stall's CAUSE is now named (ST-86): the stall log line carries `cause=<StallCause>` and `/metrics` counts `ferrosa_commitlog_sync_stalls_total{cause}`, so `no_attempt_issued` (the thread did not run) is distinguishable from `device_slow` (an fsync was in flight the whole time) — the two want opposite remedies. The watcher runs under `supervise`. Tests: `supervisor::tests::*commit_log*`, `a_stall_with_no_fsync_in_flight_is_logged_and_counted_as_the_thread`, `a_stall_with_an_fsync_in_flight_is_logged_and_counted_as_the_device`, `the_stall_cause_is_counted_once_per_episode_not_once_per_deadline`, `the_supervisor_restarts_a_real_dead_commit_log_sync_thread`. |
| FE-15 | **The maintenance loop hangs without panicking.** `poll_compactions` and commit-log GC run inline in the loop; an await that never completes or a blocking call that never returns stopped every arm, and `supervise` sees only panics and returns. | No flush, compaction, GC or schema persist; `/readyz` 200. | 9 | 2 | 9 | 162 → 27 | **Mitigated:** the loop beats a `Heartbeat` (one atomic) every iteration; `MaintenanceWatchdog`, on its own OS thread, counts a `maintenance_loop` stall per elapsed `FERROSA_MAINTENANCE_STALL_DEADLINE_SECS` (default twice the flush stall deadline, 600 s) of silence, reports it on `/readyz` and the metrics, and escalates (commit-log sync, abort) past the intensity; a hung future cannot be restarted in place. Tests: `a_hung_maintenance_loop_is_a_stall_per_deadline_and_escalates`, `a_maintenance_loop_that_beats_again_recovers`. Residual: the deadline must exceed the slowest legitimate arm; an arm that is slow but alive for longer is reported as hung. |
| FE-16 (t_396d4c80) | **Two self-heal controllers ran.** `main` spawned one after the PeerManager (quarantine gated on peer liveness) and a second with the repair wiring (gated on a peer proven to hold a good copy). | Both scanned every table and acted on the same corrupt generations; the first could quarantine a possibly-only copy when a peer was merely reachable (storage self-heal FMEA #1). | 8 | 3 | 7 | 168 → 16 | **Fixed:** only the verified-replica controller is spawned; `LivePeerHealth` is gone. Guard: `tests::main_spawns_exactly_one_self_heal_controller` (counts spawn sites in `main.rs`; red on the previous source, which had two). |

| FE-17 | **The PostgreSQL default keyspace `public` was sized from the `[internode] seed` list alone.** The PG front end answers every `SELECT` from this node's LOCAL `TableStore` (`storage_provider::produce_scan` never fans a range read across the ring), so `public` must be replicated to every node a scan is served from; `main` derived its factor from seeds only (`seeds + 1`). | Any node with no configured seed — the seed node of a star topology, and every node of the live `maas-loadtest` cluster — created `public` at **RF=1**: one owner per partition, so a wire `SELECT` served by a non-owner silently returned a fraction of the committed rows. The reported `INSERT 0 2` / one-row SELECT looked like data loss; both rows were durably present and the write was complete — the read was not. | 9 | 4 | 9 | 324 → 108 | **Fixed:** `postgres_default_keyspace_replication_factor_for_cluster` = `max(seeds + 1, FERROSA_EXPECTED_CLUSTER_SIZE, committed cluster size)`, never below 1. An `public` already created too small cannot be repaired in place (no `ALTER KEYSPACE`), so `undersized_guard_sweep` re-verifies it on a bounded schedule and logs one actionable ERROR per distinct condition — never a per-tick flood, never a latch; it clears once the keyspace is re-created. Tests: `postgres_default_keyspace_covers_a_cluster_formed_without_configured_seeds`, `undersized_guard_reports_once_per_distinct_condition_and_clears_on_heal`, and the real-process wire red `ferrosa/tests/pg_public_keyspace_rf_process.rs` (PRE-change: 1 of 5 rows; FIXED: 5 of 5). **Read path closed (same session):** the cross-node fan-out read is no longer unimplemented — every `SELECT` opens `WritePath::range_read_stream_all_with` at `ConsistencyLevel::Quorum` through the `ferrosa-postgres::ClusterRead` seam (`cluster_read.rs`, `ClusterRangeRead`), so a scan reads the ring rather than only local storage and **fails loud** when the level cannot be met. Process red: `ferrosa/tests/pg_select_cluster_read_process.rs` (a `SELECT` from each of three nodes returns every committed row by value; an owner stopped makes it complete-or-loud). Because reads honour a CL, the replication factor can be a **quorum** (`floor(N/2)+1`) rather than the whole cluster — `W + R > RF` holds. **Residual:** an operator who sets neither a seed list nor `FERROSA_EXPECTED_CLUSTER_SIZE` still creates `public` at RF=1 on first boot; the boot guard names it loudly. |

| FE-18 (t_boot_lane) | **Boot path gated consensus on the object store.** `main` awaited `validate_object_store_access`, `probe_s3_cas`, `replay_pending_uploads` and the cold-start `bootstrap_from_s3` *before* binding the internode listener, with `FERROSA_S3_REQUEST_TIMEOUT_SECS` defaulting to 900 s. | A restarting node is off the internode lane for up to that window; a brief leaderless spell cannot re-form quorum. Observed 2026-10-10: `deploy-rolling4.sh` restarted node2 (the leader) first and the cluster was unusable ~35 min while each node sat behind its boot object-store calls. | 10 | 3 | 3 | 90 | **Fixed:** the internode listener binds before the object-store work, and the `ModeController` boot-state consensus gate (`defer_consensus_until_boot_state` / `mark_boot_state_loaded`) keeps Raft from starting until that work completes, re-driving any transition a peer requested while the gate was closed. Consensus ordering is unchanged; only *reachability* is decoupled. Real-process red: `ferrosa/tests/boot_object_store_decouple.rs` (black-holed S3; PRE-change: listener never binds within 30 s — measured >180 s off the lane; FIXED: ~2.4 s, 5/5). Unit: `boot_gate_defers_cluster_transition_until_state_loaded` plus its positive control `cluster_transition_proceeds_when_boot_gate_is_open`. **Residual:** the production security gate now runs *after* the internode listener binds (a production-failing node binds the lane, then exits without serving or joining); `FERROSA_S3_REQUIRED=true` still fails fast before the listener, by operator opt-in. |
| FE-19 | **A restarted Raft node probes ready on a leader it has not heard from.** `/readyz` treated `raft.current_leader().is_some()` as ready; openraft derives it from the persisted committed vote, so a node names a leader before any contact. A former leader restarted after the others elected a new term named ITSELF and answered ready. | Control-cluster roll 2026-10-23: a node answered `{"ready":true}` then 503 `raft_leader` three seconds later (its election timeout), and the nightly engine roll stopped and rolled back. An orchestrator routing on readiness sends traffic to a node with no live leader. | 6 | 4 | 7 | 168 → 24 | **Fixed:** Forming/Cluster readiness requires live evidence (`ModeController::raft_leader_liveness`): as leader, `millis_since_quorum_ack` within the election timeout maximum; as follower, an AppendEntries accepted in this process from the named leader at the current term within that window (`raft::leader_contact`, fed by `RaftAppendHandler`). The 503 `detail` names which case failed. Residual: the leader's quorum-ack age is as fresh as openraft's last metrics flush, which a leader runs every heartbeat tick. Tests: `raft::leader_contact::tests::*`, `raft_leader_liveness_after_restart::restarted_node_names_its_persisted_leader_but_is_not_live_until_contacted` (restarts a real Raft from disk), `single_voter_leader_is_live`, `append_handler_records_contact_only_for_accepted_requests`. |

| FE-T300 | **Startup refusal of jsonb columns outside standalone (T-300, D24, D15a) — REMOVED.** The jsonb capability gate refused a node that joined peers with jsonb tables restored. | — (obsolete: the gate is gone) | 9 | 2 | 2 | 36 | **REMOVED (obsolete), not disabled.** The jsonb capability gate (T-300/D24/D32) was REMOVED from the source tree entirely: jsonb is now a first-class data type. The engine ALREADY replicates graph materialization over Raft, so a column type is not special — a gate that could REFUSE a data type was itself the defect. jsonb works standalone, paired and clustered with no flag, env var, config key or operator action, and is never turned off again. The only surviving jsonb constraint is the placement rule (T-154a, D3, D21): jsonb is still refused in partition/clustering key columns (any depth), as a set element, as a map key, and as a vector element. The former negative test is renamed `jsonb_ddl_is_permitted_in_every_deployment_mode`. With this removal the startup-refusal residual noted under FE-18 is closed. |

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
- **CQL-T467ci-01:** `GET /api/cluster/status` reports `ddl_path` (`direct`, `pair`,
  `cluster`, `forming`, `unavailable`) beside `mode`; `mode` leads it during the
  transition to cluster. Pinned by `api_cluster_status_reports_the_live_ddl_path`.
- `ferrosa-net` `default_bind_port_is_not_7000` guard (FE-6).
- `apply_internode_toml_overrides_sets_other_fields` pins TOML broadcast
  propagation into the handshake advertisement (FE-10).

### Pump wiring acceptance (T-045)

The CQL wiring acceptance test writes compressed and plain tables through a real client/server, includes a row larger than a pump segment, flushes and compacts, tears down the first runtime, and verifies all rows through a reopened engine and fresh server.

Compaction stop scope is validated before registry access; unknown/partial scope
cannot fall through to node-wide cancellation. The route is POST-only and remains
behind the shared admin/operator authentication middleware. Tests exercise real
queued compactions on two tables and verify selected cancellation preserves inputs.
| FE-OUT-F3 (outage 2026-10-10) | **A node exited with no final log line.** The log writer is `tracing_appender::non_blocking`; `process::exit` (16 sites) and the supervisor's `abort()` skip the guard that drains it, a panic went to stderr only, and SIGTERM was not named. | Operators could not tell a crash from a kill; node2 died at 16:55 with nothing in its log. | 7 | 5 | 9 | 315 → 35 | **Fixed:** `lifecycle.rs` (see README). Tests: `tests/last_line.rs` runs the binary: `panic_on_any_thread_writes_a_final_log_line`, `fatal_exit_flushes_and_logs_reason` (all 20,000 queued lines must survive the exit with the writer slowed to 100 us/line; `WorkerGuard::drop` alone kept 5,166 in isolation and ~209 under full-workspace load, so the exit counts both ends of the writer and waits up to 30 s, logging any shortfall), `sigterm_logs_the_signal_name` (`received SIGTERM`, last line `ferrosa exiting code=0 reason=shutdown after SIGTERM`); each red on origin/main and red again with its guard removed. Residual: SIGKILL and OOM kills still leave no line (nothing can), and a panic hook cannot help a process killed while the hook runs. |

### Resolved test-harness bind failures from descriptor inheritance (2026-10-23)

`kernel_ephemeral_floor` launched macOS `sysctl` for every allocation. A helper
process could temporarily retain a sibling thread's listener across its close,
so `a_port_handed_out_is_never_probed_by_a_sibling_allocation` reported a bind
collision even with its allocator scan removed. Five unchanged runs and five
query-only diagnosis controls failed. The helper now caches the host range and
initializes it before `port_is_free` creates a transient listener. The listener
squatter fixture also joins the existing fork-serialization group: its
probe/drop/rebind setup cannot overlap the process-spawning guards.

The new fresh-process query observer uses the real platform `sysctl` and checks
both floor/port values and query counts. Restoring the uncached reader fails the
original bind guard and query guard in five of five controls; removing pre-probe
initialization fails the observer's initialization assertion. No exclusion
assertions, retries, or timeout budgets were relaxed. This repair is test support
only; application startup and listener behavior are unchanged. See Forge
`t_f961615a`; the broader pre-taken-slice fixture issue remains on `t_97fb1f1c`.

### Resolved collision fixture escaping its process lease (2026-10-23)

The PostgreSQL transaction collision fixture scanned the entire shared band with
process-local reservations, ignoring other processes' slice leases. CI run
39842236334 overlapped it with a committed-row replica restart; that replica
failed to rebind CQL port 20016. Logs do not identify the socket owner, but an
exact-source two-process control reproduced the mechanism: the global selector
held the owner's unbound port 20000 and prevented its rebind. Allocating through
the collision fixture's own allocator preserved the owner rebind. The fixture
and the occupied-listener guard now share `hold_collision_port`; the deliberate
first SPARQL collision and retry/readiness assertions remain exercised. The new
allocator-boundary guard fails with the global selector and passes with this
fix. This is independent of the macOS descriptor-inheritance repair above.

### Docker recovery smoke readiness (2026-10-23)

Nightly run 39922605666's cluster-mode checks succeeded before the survivor's
peer RPC lanes finished reconnecting; both recovery MutationForward calls
failed and the one-shot QUORUM write received only its local response. Phase 10
now establishes replication readiness with a bounded QUORUM read of an existing
row before asserting the one-shot write and cross-replica data recovery. A failed
read cannot satisfy the value predicate even if its diagnostic contains that
value. Script-level controls cover transient and permanent unavailability;
the actual container run remains a separate integration check.

The kernel-query observer and its `port_is_free` import share the macOS target
guard. Keeping that import unconditional after moving the occupied-listener
fixture to the shared helper left it unused on Linux and failed strict Clippy.
The exact-source Linux lint guard reproduces the failure with the unconditional
import. The full all-features test mirror requires Linux because its real-file
`STATX_DIOALIGN` probe deliberately rejects other platforms.

## DDL publication readiness race (`t_9a8b88de`)

A node may know its live Raft leader before the asynchronous transition
publishes Cluster DDL. Reporting ready in that interval allowed a successful
local CREATE that never reached the other DC. `/readyz` now requires the
published Cluster DDL path as well as live leadership, retaining existing
`raft_leader` diagnostics when Raft itself is not live. A real elected Raft
fixture checks Direct and Forming return 503 and the same node returns 200 only
after Cluster publication. Cluster transition/admission is repaired in
`ferrosa-cluster`; this does not repair previously divergent schema.

The smoke harness also requires `ddl_path == "cluster"` inside its existing
cluster-formation wait, so its one-shot CREATE cannot race publication.
Behavioral shell regressions cover delayed and permanently missing publication
without extending the wait or retrying writes.

PostgreSQL is registered as starting before the web readiness endpoint is
exposed. It becomes up only after its background TCP bind succeeds; pending or
failed binds keep readiness false. This closes the startup window where an
empty listener-status map could briefly make an occupied-port node look ready.
