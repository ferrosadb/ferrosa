# ferrosa-cluster

> The distribution layer: Raft metadata consensus (openraft), tunable-consistency
> read/write coordination, the cluster-formation state machine, anti-entropy
> repair, hinted handoff, and Accord strict-serializable transactions.

## What this crate is

`ferrosa-cluster` turns a set of single-node `ferrosa-storage` engines into a
distributed database. It owns four largely independent subsystems that share a
token ring and a Raft-replicated metadata state machine:

1. **Raft metadata consensus** (`raft/`) — schema/DDL, membership, token
   assignments, and cluster config are replicated through openraft 0.9 (a pinned
   ferrosa fork with a CheckQuorum extension; a PreVote gate also exists in the
   fork but is **disabled by default** — its network transport is unimplemented,
   see below). The state machine is in-memory `RaftState`; the log is sled-backed.
2. **Read/write coordination** (`coordinator/`) — fans writes and reads out to
   replicas with tunable CQL consistency-level (CL) enforcement, write
   backpressure, read repair, batchlog, and range/streaming reads.
3. **Cluster formation** (`controller/`, `mode.rs`, `ring/`, `pair/`) — the
   `ModeController` state machine drives `Standalone → Pair → Forming → Cluster`,
   the token ring computes replicas (SimpleStrategy / NetworkTopologyStrategy),
   and pair mode gives two-node durability before a Raft quorum exists.
4. **Anti-entropy & repair** (`repair/`, `hints/`) — Merkle-tree repair sessions,
   an automatic repair scheduler, hinted handoff for transiently-down replicas,
   and a quarantine → refill trigger from the storage self-heal path.

It also implements **Accord** (`accord/`), an EPaxos-family protocol for
strict-serializable multi-key / cross-shard transactions and LWT.
During Apply, remote replica fanout runs concurrently with the coordinator's
local dependency wait. The coordinator reports success only after its local
transaction reaches `Applied` and every participating shard reaches Apply
quorum, so a locally parked dependency does not delay propagation or become an
early acknowledgement.

The Apply fan-out is **memory-bounded**: `apply_fanout_bounded` keeps one replica's
`AccordApplyV2` payload resident at a time — build, serialize, drop, send, peer by
peer — rather than materializing every replica's payload simultaneously. The
per-shard quorum accounting is unchanged; only the peak memory is. This matters for
large transactions, where the payloads are the coordinator-only term that tips a
node: a ~1.1M-row commit measured ~4.5 GB peak on the coordinator against ~2.75 GB
on each replica before the bound. See FMEA `CL-50`.

> **Correctness-evidence honesty.** The Accord and Raft subsystems have extensive
> *in-crate, deterministic* tests (state-machine, recovery, property, and
> simulated-nemesis). There is **no external/public Jepsen run yet** — the
> `ferrosa-jepsen` end-to-end harness is an approved-but-unbuilt standalone crate
> ([specs/todo/jepsen-e2e-test-plan.md](../specs/todo/jepsen-e2e-test-plan.md)).
> Treat consensus/transaction correctness as *tested*, not *proven in the wild*.
> See [specs/fmea.md](specs/fmea.md).

## What's implemented

### Raft metadata consensus (`raft/`)
- openraft 0.9 with features `serde`, `storage-v2`, `loosen-follower-log-revert`,
  plus a pinned fork adding **PreVote** (`raft_enable_pre_vote`) and
  **CheckQuorum** (`raft_check_quorum_ratio`, default 0.75) per ADR-012.
  - **PreVote defaults OFF** (`raft_enable_pre_vote = false`). ADR-012 intended
    it on, but the fork's tick election path hard-gates `elect()` behind a
    pre-vote round while `FerrosRaftNetwork` (`raft/network.rs`) never overrides
    `pre_vote` — the default trait impl returns an "unimplemented" error counted
    as a NO vote, making a pre-vote quorum structurally impossible and freezing
    any multi-voter formation at term 1. Leaving it on only subtracts liveness.
    Re-enable only after the pre-vote transport lands (forge t_b0aac0d3 /
    t_32cb5ad3). Regression guard: `candidate_re_campaigns_while_peers_are_down`
    in `tests/cluster_formation.rs`. Spec:
    [`specs/implemented/bug-cluster-formation-pre-vote-election-stall.md`](../specs/implemented/bug-cluster-formation-pre-vote-election-stall.md).
- `FerrosStateMachine` / `RaftState` — keyspaces, tables, roles/grants, indexes,
  types, UDFs/UDAs, members, token map, per-node index status, cluster config.
  `DropTable` apply removes the table's index entries from `RaftState` and, via
  `engine.unregister_table`, cascades tombstones over the dropped table's
  `system_schema.indexes` registrations (t_ae06e925). `DropIndex` apply now
  also calls `engine.drop_index`, so live memtable/vector index state, sidecar
  read guards, and `IndexStateTracker` entries are removed on the applying node
  immediately. `CreateIndex` apply is now symmetric with it: it calls
  `ddl_path::build_replicated_index`, so a `CREATE INDEX` that reaches this node
  through the log — rather than through a CQL session — is BUILT here and not
  merely recorded. It previously registered the index in `Schema` and
  `system_schema.indexes` and wired nothing, which left every node but the DDL
  coordinator holding an index the planner selects and the engine cannot answer
  (CL-18). The same call is made by `apply_ddl_locally` (pair) and by the shared
  `ddl_path` apply, and all three delegate to
  `StorageEngine::register_index_in_engine` — the resolver the restart reload
  uses — so the wiring cannot drift between the paths again.
- `DdlPath::kind()` / `ModeController::ddl_path_kind()` name the live DDL path
  (`direct`, `pair`, `cluster`, `forming`, `unavailable`). The mode reads
  `cluster` from the start of `transition_to_cluster`, but DDL stays `direct`
  until Raft has a leader, so readiness probes wait on `ddl_path == "cluster"`
  (exposed on `/api/cluster/status`; CQL-T467ci-01).
  Snapshot install no longer treats `previous - next` as authority to unregister
  local tables. A table that is absent from an incoming snapshot is considered
  ambiguous until snapshots carry explicit identity-scoped drop markers; if the
  local engine still has SSTables or persisted `system_schema.indexes` rows for
  such a table, install fails loud before `engine.unregister_table` can tombstone
  indexes or remove SSTables. Plain `RaftOp::DropTable` still uses the normal
  destructive cleanup path.
- `SystemTableLoader` reconstructs durable schema/auth state during cold
  start. Persisted `system_auth.roles` rows replace fresh-process bootstrap
  roles before missing seed roles are created, so rotated hashes survive both
  graceful and commit-log recovery paths.
- `SledLogStore` — sled-backed log + meta trees, legacy-format migration, log
  inspection/reset tooling.
- `election_guard.rs` — `run_election_guard` watchdog (P0-17/P0-19): a burst
  detector and a 30 s rolling-window detector that call `elect(false)` to suppress
  divergence-driven election storms for 60 s; `ELECTION_STORM_TERM_JUMPS_TOTAL`.
- `consensus_metrics.rs` — Prometheus surface for Raft liveness:
  `ferrosa_raft_current_term` / `_is_leader` / `_has_leader` (fed by
  `run_consensus_metrics_poller`, a per-cluster-node task that derives leadership
  from `current_leader()` — the reliable `/readyz` source — not the raw
  `metrics().state` snapshot which read 0 for the leader) plus
  `_election_storm_term_jumps_total`. Rendered onto the main binary's
  `/metrics`. Kept separate from `election_guard` per ADR-012 so the metric home
  and its poller outlive the guard. The scan-storm regression (t_88223ad0 / T0.6)
  asserts the storm counter stays 0 under full-table `ALLOW FILTERING` load.
- `snapshot_pusher.rs` — leader-side sweep (P0-20) that triggers snapshot +
  heartbeat to lagging followers; `INSTALLSNAPSHOT_PUSHES_TOTAL`.
- `snapshot_transport.rs` — snapshots travel on `Lane::Bulk` (not `Lane::Raft`),
  3 MiB chunks, 60 s per-chunk timeout.
- `multi_dc_apply.rs` / `group_id.rs` — per-DC Raft groups (UUID-v5 group ids),
  HLC reorder buffer + applied-txn ledger for cross-DC Accord apply (ADR-015).
- `raft_forward.rs` — forward client writes / membership updates to the leader.

### Coordination (`coordinator/`, `consistency.rs`, `write_path.rs`)
- `ConsistencyLevel` — full CQL CL set incl. `Serial`/`LocalSerial`; `block_for` /
  `block_for_dc` quorum math, wire + string codecs.
- `write_path.rs` — the front-end-facing replica-placement boundary (ADR-021):
  `replicas_for_key(token, strategy)` and `accord_replicas_for_key(key,
  replication)` resolve a key's RF replica host ids from the ring in cluster
  mode (`None` outside it, so the caller keeps its local/all-peers fallback;
  `Err` on an unparseable strategy). The CQL/Postgres LWT/Accord paths pass raw
  key bytes + keyspace replication and never touch the partitioner or ring — the
  CQL LWT path (`route_lwt_via_accord`) uses this for token-aware, RF-correct
  participant sets instead of replicating every key to every connected peer.
- `coordinator/write.rs` — replica fan-out with `cl.block_for(rf)` ack threshold,
  NTS / `LOCAL_QUORUM` / `EACH_QUORUM` per-DC variants, hinted handoff for failed
  replicas, post-quorum hint drain, lazy mutation encoding.
- `coordinator/read.rs` — two-phase digest reads, inline read repair on digest
  mismatch (fail-loud `ReadTimeout` rather than serve stale), corrupt-SSTable
  failover feeding the bounded `AntiEntropyRepairQueue` (cap 1024) — *serve now,
  repair in background* (LOCKED DESIGN). Unbounded single-partition reads use
  the `Bulk` internode lane for every remote page, while LIMIT-bounded and exact
  clustering-row reads retain `Data`; this prevents a wide-partition scan from
  queuing ahead of small writes on the latency-sensitive lane (t_82052066).
  Also hosts the index scatter-gathers:
  `coordinate_index_read` (secondary index) and `coordinate_fulltext_search`,
  plus the KEYED index read `coordinate_index_read_in_partition` (t_430c4188):
  `WHERE <full pk> AND <indexed_col> = ?` contacts ONLY the partition's replicas
  (ring placement under the keyspace strategy), each running
  `read_by_index_in_partition` locally — never a global scatter-gather. The
  coordinator stops after the requested consistency level has enough successful
  responses (t_2f174c97), so CL ONE does not inherit a slow peer's three-second
  Bulk timeout; quorum levels still wait for their required successes.
  (`fts_match` — fans out to every node's local FTI and unions/de-dupes the
  matching keys, since full-text hits span all token ranges; BUG-F-007). Legacy
  scatter-gathers fail when any required node fails; returning a local-only union
  as a successful no-hit search would hide missing data. The
  query-derived `LIMIT k` is pushed down to every replica
  (`FulltextSearchRequestPayload.limit`, t_ee98faa0 layer 2) so each holds a
  bounded top-k working set and the union is at most `replicas x k` keys;
  `limit: None` (no-LIMIT statement) requests the complete match set — never a
  server-side cap.
- `coordinator/cl_routing.rs` — W8.4 learner-aware routing (voter-only quorums,
  leader-only serial, cross-DC Accord routing).
- `coordinator/batch.rs` — 3-phase logged batchlog (write → fan out → delete only
  on full success) with replay task; `DEFAULT_BATCH_CONCURRENCY = 32`.
- `coordinator/fulltext_stream.rs` — streaming fulltext search (t_4ae47a9f),
  the `fts_match` twin of the ADR-020 range-read stream: the producer walks the
  local FTI via `fulltext_search_each` on a blocking thread and fires bounded
  `FulltextSearchStreamChunk` key batches (≤ 4096 keys) on `Lane::Bulk` with
  heartbeats + Cancel; `coordinate_fulltext_search_stream` fans out to every
  node, N-way merges over bounded channels, and dedups into one `seen` set —
  the only O(distinct matches) allocation left in the path (scores and extra
  copies are gone; the pre-fix union OOM-killed replicas, t_8fc24ce2). Any
  replica failure fails the stream loudly — no silent partial union (stricter
  than the legacy degrading path). `WritePath::fulltext_search_stream` is the
  mode-dispatching entry; `FERROSA_BULK_STREAMING_FULLTEXT=0` falls back to
  the legacy single-message union for mixed-version upgrades.
- `coordinator/range_read_stream.rs` — every merge source pull goes through
  `next_fragment_bounded` (CL-19), so a silent source — including the LOCAL
  engine stream, which has no `clean_end_guarded_stream` watchdog of its own —
  surfaces as a loud, retryable error naming the merge and its budget instead of
  parking the scan until the caller's own timeout fires. Both merge paths are
  covered: the fragment merge (`FragmentCursor::ensure_peeked`) and the
  whole-partition merge used by `LIMIT N` + partition-key reads, which bypasses
  the cursor entirely.
- `coordinator/{range_read_stream,stream_*}.rs` — ADR-020 streaming range reads
  and projected streaming scans; the old Vec-returning
  `WritePath::range_read_projected` wrapper has been removed, so projected
  scans use `range_read_projected_stream_all_*` directly (default; legacy capped
  path behind `FERROSA_BULK_STREAMING_RANGE_READ=0`).
  `DEFAULT_RANGE_READ_LIMIT` — the old 10_000-row *result* cap — is gone; the
  value survives as `LEGACY_RANGE_READ_REPLICA_WINDOW`, a **resource** bound on
  one legacy single-shot range RPC message, never a result bound.
  `range_read_limited_rows` and `coordinate_range_read_stream_limited_rows`
  honor the caller's own bound (a user `LIMIT N`) uncapped, and
  `range_read_limited_rows_checked` now probes exactly one partition past the
  caller's own bound instead of a hard cap; no query path selects it. The window
  is never applied as a result cap on a streaming `*_stream_all_*` scan
  (spec: `../ferrosa/specs/proposed/streaming-range-reads-no-cap.md`).
  The same bounded Bulk frames now carry global secondary-index walks, in row
  order — `(partition key, clustering)` — from an optional cursor carried in
  the request's `start_key` + `start_clustering` (resume strictly after that
  row). `coordinate_index_read_stream` merges the node streams in row order
  (`merge_index_streams_in_row_order`) and drops a row several replicas return
  by comparing it with the previous row: one head per node, no set of rows
  seen (t_50c8bc7d). CQL global index scans use
  `WritePath::index_read_stream`. A replica that does not declare the index
  refuses the walk (truncated `Done` → `TruncatedReplica`) instead of
  contributing zero rows, so a tenant-wide read through a partition-key index
  either unions every node's slice or fails (t_50c8bc7d); callers with a scan
  alternative check `WritePath::declares_index_locally` first.
  The consume path is **bounded memory**: `stream_consumer::PartitionSink` +
  `consume_range_stream_into` MOVE each decoded partition into a sink one at a
  time (resident set `O(chunk)`), and `coordinate_range_read_stream_limited_rows`
  drinks the token-deduped N-way merge stream and folds `<= limit` whole
  partitions — never accumulating `O(result)` (the `t_ee98faa0` / `t_3fc6be3c`
  OOM). The legacy `Vec`-accumulating `consume_range_stream` is a thin wrapper
  (`VecPartitionSink`) kept for point-bounded callers / the e2e tests.
  **Stream lifecycle (t_dc729b1d / t_3fc6be3c):** `StreamFrameRouter` ties chunk
  seq-state to route liveness — a frame with no seq-state AND no registered
  route is a terminal straggler (request_ids are monotonic, never reused, and
  the route is always registered before the request fires) and drops silently
  instead of fabricating fresh `expected=0` state (the phantom
  `expected_seq=0 observed_seq=5` gap-close per abandoned page). A genuine gap
  or reorder on a LIVE route still closes the route loudly, exactly once
  (`route_closures()` counter — non-zero in steady state means real chunk
  loss). When a consumer abandons a coordinated stream mid-flight (every paged
  read does, on every page but the last), the per-replica forwarder task fires
  `RangeReadStreamCancel` (info-logged) so the remote producer stops between
  batches instead of streaming the remaining table onto `Lane::Bulk` for
  nobody; the producer-side handler is registered for the Cancel MsgType. The
  N-way merge also races its core loop against `out_tx.closed()` so abandonment
  aborts even while the merge is parked awaiting a stalled source; pinned by
  `range_read_stream::tests::nway_merge_consumer_drop_aborts_when_parked_on_stalled_source`.
  Harness: `tests/range_scan_multi_replica_paging.rs` (real 3-node loopback,
  RF=3/CL=ALL, counter-asserted).
  **Flow control (t_a0f922a3):** internode range streams are WINDOWED — each
  `RangeReadStreamRequest` carries `max_chunks` (`STREAM_WINDOW_CHUNKS = 16`,
  provably < the 32-slot route buffer); the producer stops at the window and
  reports a `(partition_key, clustering)` resume position in its Done, and the
  coordinator's `WindowedReplicaForwarder` fires the continuation only after
  the consumer drains the window. Without this, any scan larger than the
  buffer overflowed the route (`ChannelFull` → fail-loud close → retryable
  ReadTimeout that drivers retry forever — the live 15k-partition paged-scan
  "stall"). Heartbeats route lossily (`StreamRouter::route_lossy`) so a
  keep-alive can never close a healthy mid-window route. Paged scans resume
  WITHIN a partition end-to-end: `write_path::ScanResume { key, clustering }`
  ships the cursor's clustering position to every producer
  (`start_clustering` on the wire; `resume_filtered_stream` locally), so a
  wide partition spanning pages never re-streams its delivered prefix.
  **Resume-filter fail-loud (t_a0f922a3):** `resume_filtered_stream` drops rows
  `<= resume_ck` as the already-delivered prefix, which is correct only if the
  fragment stream is monotonically ascending in raw clustering. A legacy /
  mis-sorted SSTable can emit a wide partition as two concatenated ascending
  runs, whose second run restarts *below* `resume_ck` and would be silently
  dropped — under-delivering the page with no error. The wrapper now tracks the
  delivered clustering for the resume partition and **errors** (`Storage /
  InvalidData`, "compact this table") on any regression instead of returning a
  silent partial. It is inert on healthy (monotonic) data, adds O(1) state, and
  does no buffering. The permanent fix is at rest: compaction rewrites such
  SSTables in sorted, byte-comparable order (`ferrosa-storage` legacy-format
  rewrite).
  **Wire note:** the request/Done payload field additions are a bincode wire
  change — upgrade all nodes together (mixed versions fail decode loudly).
- **Write backpressure**: `WRITE_CONCURRENCY_LIMIT = 128` semaphore prevents bulk
  CQL inserts from starving Raft heartbeats on the tokio runtime.

### Formation (`controller/`, `mode.rs`, `ring/`, `pair/`, `rebalance.rs`)
- `DeploymentMode` + `ModeController` — the `Standalone → Pair → Forming →
  Cluster` state machine with degraded states; `ClusterStateHolder` dispatches
  `SingleNode` / `Pair` / `Raft` cluster state.
- `controller/bootstrap/` — 8-phase formation pipeline (DeliverInvites →
  EstablishPools → CreateRaft → WaitLeader → ReplaySchema → BootstrapStream →
  Promote → DrainQueue). ReplaySchema re-sends local user schema to the
  current leader in bounded rounds (`replay_schema::after_round`): a forward
  that fails during an election is retried, and the outage is logged once
  when it starts and once when it recovers or gives up (FMEA CL-46).
- `controller/peer_events.rs` — inbound handshakes prefer the peer's advertised
  internode endpoint when creating reverse pools and tracking invite targets;
  the observed IP plus local port is only a compatibility fallback. This keeps
  same-host clusters with per-node ports from routing every host ID to the seed.
- `ring/` — `TokenRing` (BTreeMap), SimpleStrategy + NetworkTopologyStrategy
  replica selection with rack diversity, learner `owns_tokens` handling.
- `controller/membership.rs` — `MembershipChanger` mutates the four membership
  stores atomically (state machine, openraft voters, network node-map, peers);
  add/remove voter, learner-only join, promote/demote, DC swap drain.
- `controller/cluster_rejoin.rs` — P0-21 rejoin after formation timeout;
  `CLUSTER_REJOIN_ATTEMPTS_TOTAL` / `_FAILURES_TOTAL`.
- `pair/` — two-node primary/secondary coordination (deterministic host-ID
  ordering, independent of which TCP direction wins), switchover, catch-up.
  Schema catch-up (`PairSchemaSyncHandler`) is applied only by a receiver: a
  primary refuses a peer's snapshot. A receiver converges to the primary's
  keyspaces and tables (inserting, replacing a stale definition, dropping what
  the primary dropped), verifies, adopts the primary's schema version and acks
  with it (CL-33). Switchover refuses until the peer confirms that version and
  no rejoin data replay is running or failed (`CatchUpGate`, CL-34).
  `ModeController::choose_pair_role` gives `Primary` to the **lowest** host_id
  (`local_host_id <= peer_host_id`). A node with no `FERROSA_HOST_ID` generates
  a random UUID on first boot, so any deployment that needs a predictable
  primary must pin the ids — every `docker-compose*.yml` in this repo does, and
  `ferrosa`'s `pair_primary_is_deterministic` test enforces it. An unpromoted
  secondary refuses client CQL entirely (`is_cql_ready()` is false) while still
  reporting healthy on `/readyz`, so an unpinned stack can hand you a node that
  looks up but serves nothing.
- `rebalance.rs` — token-skew rebalancing with data streaming.
- `raft/handlers.rs` `RowWire`/`CellValueWire` — the bincode row format of
  coordinator reads, anti-entropy repair, range-read streaming, digests and
  bootstrap/decommission/rebalance streaming. A complex (non-frozen collection)
  cell carries its path behind leading tag `2`; simple cells keep the legacy
  bytes. A node older than this format refuses a complex cell with a decode
  error rather than flattening it, and the row-stream receiver fails a session
  whose payload does not decode (FMEA CL-32).
- `controller/jsonb_gate.rs` (T-300, D24) — while any table holds jsonb, a
  standalone node may not move to Pair, Forming or Cluster: the transition entry
  points and `try_transition_mode` refuse, naming the tables and the D15a
  ledger. `check_startup_jsonb` lets `main` fail startup for a non-standalone or
  seeded node whose schema holds jsonb. The controller's mode cell is shared
  with `Schema`, so the schema's DDL gate always sees the live mode. No flag
  bypasses it; T-154b replaces it with the ledger.

### Repair & hints (`repair/`, `hints/`)
- `repair/merkle.rs` — depth-15 Merkle trees (32 768 leaves), content-aware
  partition hashing, divergent-leaf detection.
- `repair/{coordinator,executor}.rs` — Merkle-then-stream sessions with bounded
  fetch/apply chunks; deterministic single-initiator selection (no thundering
  herd); timestamp ties surfaced, never auto-resolved (Aphyr-safe).
- Every path that moves a partition between nodes carries the whole partition:
  rows, static row and partition deletion (P0-3, CL-43). Row streaming encodes
  through `StreamedMutation::from_partition` (legacy `Vec<RowWire>` bytes when
  there is nothing else, otherwise the versioned envelope old nodes refuse
  with a decode error); repair apply, `RepairApplyHandler`, the stream receiver
  and local read repair call `StorageEngine::apply_partition`; read repair
  encodes `read_repair_body` from borrowed rows (deletion and static marker
  rows) and applies the same decoded body locally.
- `repair/scheduler.rs` — `AutoRepairScheduler` / `AutoRepairConfig` (24 h default
  interval, round-robin tables), Prometheus metrics.
- `repair/trigger.rs` — quarantine → anti-entropy refill: a corrupt SSTable
  quarantined in storage schedules a targeted refill from a verified-healthy peer.
- `hints/` — per-peer on-disk hint segments (CRC32, crash-recoverable),
  byte-budget backpressure (no silent loss → `needs_repair` + ERROR), FIFO
  at-least-once delivery as `MutationForward`. **No time-based TTL** (budget cap).
- `repair/coordinator.rs::ring_health` / `RingHealth` / `ring_data_scatter_risk`
  — pure ring-membership health derived from node states. A member that is not
  `Normal` (`Joining` / `Leaving` / `Decommissioned` / `Learner`) is excluded
  from `TokenRing::replicas()`, so its tokens are served by other nodes. These
  make that degraded state reportable; `GET /api/cluster/ring` returns
  `ring_healthy`, `non_normal_members` and `data_scatter_risk` (CL-29).
- `repair/coordinator.rs::promote_joining_members` — pure Promote-phase planner:
  one `SetNodeState{Normal}` per `Joining` member WHOSE BOOTSTRAP IS RECORDED
  (`RaftOp::RecordBootstrapComplete`, CL-41), and nothing for
  `Leaving` / `Decommissioned` / `Learner` (operator intent and the ADR-014
  learner state are never reversed). `joiners_awaiting_bootstrap` names the
  unrecorded joiners, which stay `Joining`. The controller runs it on the
  recovered-topology path (CL-29); a joiner found unrecorded there reruns its
  own bootstrap.
- `controller/data_movement.rs` — no replica-ownership change without verified
  data movement (CL-40, CL-41):
  - `decommission_verified`: `Leaving` → stream every partition of every range
    the leaving node REPLICATES (per keyspace strategy, `DecommissionPlan`) to
    each new owner, each batch verified by the receiver → only then
    `LeaveNode`. Any read error, stream rejection, short apply or range left
    with no replica aborts; the node stays `Leaving` with its data
    (`DECOMMISSION_ABORTS`). `ModeController::initiate_decommission` runs it and
    only for the local node.
  - `bootstrap_verified`: one Merkle anti-entropy session per (table, range the
    joiner will replicate, current replica), all must succeed, then
    `RecordBootstrapComplete`, then `Normal` (`BOOTSTRAP_ABORTS`).
  - The row-stream `StreamEnd` reply carries a `StreamEndAck` verdict
    (`Applied { applied }` / `Rejected { reason }`); `StreamSender::send_stream`
    returns the verified applied count and fails on a rejection or a pre-upgrade
    peer's bare `ok`.
- `ModeController::downgrade_to_pair(Some(peer))` — operator downgrade, with
  friction by design (t_ad872ac7): accepted only after a node was taken down,
  i.e. every ring member other than this node and the named, connected peer is
  down, and only if those downed members are a Raft-voter MINORITY. Two-phase
  and fenced (t_47bbeb66, `controller/dissolution.rs`): the peer durably
  records `dissolved-into-pair.json` and stops its Raft group first
  (`PairDissolve`), then this node does the same and installs the pair path.
  A node holding the marker never starts Raft, restarts `Standalone` rather
  than as a returning member, and admits only its partner as a peer, so the
  members taken down can never re-form the old cluster (CL-42, CL-45).

### Accord transactions (`accord/`)
- `coordinator.rs` / `state_machine.rs` — PreAccept → {fast path | Accept} →
  Commit → [read-vote] → Apply, fast/slow quorum math, HLC timestamps + `TxnId`.
  Accept is dependency-monotonic: replicas retain dependencies seen locally
  after PreAccept and return that effective set in AcceptOK; the coordinator
  unions the accepted quorum's dependencies before Commit. This prevents a
  delayed Accept from dropping a conflict discovered during the first round.
  **The conflict index is a hard floor on the largest decidable transaction.**
  A PreAccept registers the transaction under EVERY key in its write-set and is
  all-or-nothing: if any `ConflictIndex::register` fails (capacity) the replica
  rolls back the partial registration and answers with no vote. The capacity
  therefore caps the size of any single transaction the cluster can decide, so
  it MUST cover every write-set the front end admits. It resolves, cached
  process-wide, from `FERROSA_ACCORD_CONFLICT_INDEX_CAPACITY`, else from the
  PostgreSQL front end's own `FERROSA_POSTGRES_MAX_TXN_WRITES`, floored at the
  historical 100 000 (`state_machine::resolve_conflict_index_capacity`). Before
  this the capacity was a fixed 100 000 that no setting could raise: a
  ~1,000,112-key transactional `COPY` (`pgbench -i`, admitted because
  `FERROSA_POSTGRES_MAX_TXN_WRITES=3000000`) was refused by all three replicas —
  `key_count=1000112 e=conflict index at capacity`, zero votes — and surfaced as
  an opaque "Accord quorum unavailable" on a healthy cluster (CL-49). The driver
  also refuses an oversized write-set **by name**
  (`AccordDriverError::WriteSetExceedsCapacity`), before the protocol registers
  anything, instead of letting it fail as a mystery quorum error.
  The read-vote phase is the LWT `IF`-condition gate: every conditional
  statement, `INSERT IF NOT EXISTS` included, sends `ReadClusteringRow`, the
  replicas read only that table's row at `t` (`StorageReader::
  read_clustering_row_at`, t_5504f601), and the coordinator gates on it.
  `ReadRow` (whole partition) is still answered for older coordinators; the
  new variant is appended last, so existing wire tags are unchanged. A replica
  that cannot decode a read-vote now abstains with an error log; one that
  predates this change echoes the request instead, which the coordinator
  logs as a malformed vote and does not count, so the round fails with
  QuorumUnavailable rather than deciding on it. `NotExists` is
  retired: it was answered from the conflict index (partition-key bytes, no
  table), so replicas and the coordinator now refuse it and the transaction
  fails loud (t_7a0acbc8, t_fe2426bb). An unconditional write at SERIAL
  consistency sends `Always`; a general multi-key
  SQL transaction uses `ReadPredicate::Always`, which **skips the read-vote
  entirely** and always applies after commit (there is no `IF` to evaluate). The
  vote collector stops once F+1 matching votes decide the predicate, without
  waiting for an unavailable minority. PostgreSQL snapshot barriers use the
  committed slow quorum plus a local dependency/apply check and do not fan out
  remote read-votes. The replica apply path is **dep-ordered**: `handle_apply`
  routes every real write
  through `apply.rs`'s `DepWaitApplier`, so a mutation persists only once all of
  its dependencies have applied locally (otherwise it parks and the cascade
  applies it in order). An explicit no-write Apply also resolves an absent local
  dependency and cascades parked dependents; this handles merged dependencies
  learned from another replica without falsely acknowledging real writes. A
  transaction waits only for dependencies that execute **before** it: its
  dependency set is computed from `t0`, so a dependency committed with a later
  `t` is waived (at apply time, or when that dependency's commit lands). Without
  this, two transactions whose PreAccepts crossed each waited on the other until
  the dependency wait failed both, with every replica live (FMEA CL-28). A
  dependency cycle among parked transactions is refused loudly, never dropped.
  The wait is **bounded and operator-tunable**: `FERROSA_ACCORD_TXN_TIMEOUT_SECS`
  (config `[accord] txn_timeout_secs`), defaulting to
  `epoch_drain::DEFAULT_TXN_TIMEOUT` (10 s, single-sourced so the drain that must
  exceed it cannot drift) and read on the hot path through a lock-free atomic —
  no state lock, no blocking-pool hop, no allocation per RPC. When the bound
  expires the coordinator **abandons** the transaction: it finalizes it as a
  no-write — rolled back, never applied, which also releases every successor
  parked behind it — and returns `AccordDriverError::TxnAbandoned`. A stuck apply
  therefore cannot poison a key permanently, and the client is told the
  transaction did not commit and may retry: the PostgreSQL front end maps the
  `abandoned:` reason to SQLSTATE 40001 (serialization failure) and the CQL
  router to a retryable server error.
  A PreAccept for a transaction the replica already knows is decided is
  refused (`SmResponse::AlreadyDecided`, the empty `PreAcceptOK` on the wire)
  and registers nothing; otherwise a PreAccept queued behind a no-write
  finalize, or delayed past `prune_applied`, would register a conflict nothing
  ever clears (FMEA CL-36). The record is `finalized.rs`'s `FinalizedTxns`:
  exact tombstones plus a monotone floor, bounded by a 60 s retention horizon
  (advanced by `prune_applied` from the shared HLC), a 250 000-id cap that
  evicts into the floor, and a restart floor set when the HLC is wired, since
  the replica does not replay its Accord log.
  `accord/quorum_availability.rs` drives real replicas through the real committer
  to pin the criterion: a live quorum commits while one replica is paused, and a
  lost quorum fails promptly naming the quorum.
- `recovery.rs` — Paxos-style recovery selecting by highest `accepted_ballot`.
- `transaction_commit.rs` — `AccordTransactionCommitter`: the cluster-side
  implementation of `ferrosa_storage`'s `TransactionCommitter` seam (ADR-021). CQL/
  Postgres `BEGIN`/`COMMIT` buffer DML and call it; it resolves replicas per key
  (injected resolver wrapping `WritePath::accord_replicas_for_key` + schema in prod),
  then drives ONE unconditional (`ReadPredicate::Always`) multi-key Accord
  transaction via `new_multi`, mapping the outcome to `Committed`/`Aborted`/`Err`
  (fail-loud — never acks an uncommitted txn). When the coordinator is itself a
  replica for the txn's keys (the common case), it votes its OWN PreAccept
  **locally** against the node's live `AccordState` — a node is never in its own
  peer map, so a self-send would fail "unknown peer" and a sole-replica (RF=1)
  txn would never reach quorum. That state is wired via `with_local_accord_state`
  or, from the session layer, `with_local_accord_state_slot(&AccordStateSlot)`:
  `handlers::publish_accord_state` fills the slot at cluster formation with the
  SAME `AccordState` the node's `AccordHandler` serves, so the coordinator's
  self-vote and its remote peers agree on dependencies.
  **Table tombstones (`TRUNCATE`).** A `TRUNCATE` is ONE reserved-partition
  mutation. Routing it through the per-key Accord path above would place it by
  that key's token on the key's RF replica set — a proper subset of the ring when
  `RF < node count` — so the nodes outside it would keep serving the truncated
  rows. `commit_postgres` therefore splits table-tombstone writes out of the
  Accord write-set and replicates them through the `AllServingMarkerWriter` seam
  (`WritePathAllServingMarkerWriter` over the live `WritePath` in production),
  which fans the marker to **every node serving the table** at
  `ConsistencyLevel::All`: `ClusterCoordinator::coordinate_all_serving_write` uses
  the whole ring as the target set (`WritePath::all_serving_host_ids`) and requires
  **every** target to acknowledge — no quorum shortcut, no hint fallback, because
  `CL=ALL` alone only filters the replica slice it is handed (that one key's RF
  set). An unwired writer, or any node that does not ack, **refuses the commit
  loudly** — an unconfirmed truncate would resurrect the rows on the node that
  missed it.
- `apply.rs` — `DepWaitApplier` (dep-wait + `StorageApplier` seam) +
  `EngineStorageApplier`/`EngineStorageReader` (real persistence and linearizable
  read-at-`t`). **Multi-key (Phase 2/3):** `DepWaitApplier::try_apply_writeset`
  parks a transaction's WHOLE write-set and applies every key on resolve;
  `StorageApplier::apply_writeset` commits all of a txn's partitions in ONE atomic
  `apply_batch` (all-or-nothing — a failure on any key persists none); idempotency
  is keyed by `(txn_id, partition_key, t)` so writes 2..N of one transaction are
  never deduped/dropped.
- `wire.rs` — bincode payloads for each protocol message. **Multi-key:**
  `WriteSetEntry` + `ApplyV2Payload` back the additive `AccordPreAcceptV2`/
  `AccordApplyV2` wire codes; `AccordCoordinatorDriver::new_multi(write_set)` is
  the multi-key constructor (`new` is the one-entry degenerate case). Multi-key
  *execution is wired*: `run_transaction` drops the old fail-loud guard, builds a
  per-shard participant (`ParticipantSet::from_per_key` via the
  `with_per_key_replicas` resolver) and fans a per-replica `AccordApplyV2`
  (scoped to each replica's owned keys) out under per-shard quorum. Conflict
  ordering unions dependencies across ALL keys: PreAccept fans `AccordPreAcceptV2`
  (every key) so each replica registers the txn under all its keys and returns the
  dep union, serializing transactions that overlap on a non-first key (t_276e12).
- `dep_wait.rs` — waits-for graph with deterministic cycle-breaking.
- `cross_shard.rs` / `cross_dc_adapter.rs` — multi-shard atomicity, cross-DC glue.
- `electorate.rs` / `epoch.rs` — JoinElectorate membership gates, epoch tracking.
- `durability.rs`, `leaseholder.rs`, `linearizable_read.rs`, `two_phase_ddl.rs`.
- In-crate Jepsen-style tests: `jepsen_bank.rs`, `jepsen_nemesis.rs`,
  `recovery_scenarios.rs`, `proptests.rs` — all on the deterministic `TestCluster`.

- **Peer identity is verified on connect.** `PeerManager::ensure_peer` rejects a
  connection whose handshake host_id differs from the id the ring said lives at
  that address (`peer identity mismatch`), so a stale or looped-back address can
  never be pooled under another node's id. `PeerFireSink` (range-stream
  replies) also refuses to stream to the local host_id with a specific error
  instead of a bare "unknown peer" (t_b78e8e9a).
- **Streaming range reads serve the local replica locally.** A ring entry that
  carries this node's own host_id (under any node id) is the local replica, not
  a remote: `range_read_remotes` (and the fulltext fan-out) drop it, so the
  local-engine stream answers it and a node started alone or with peers down
  still serves CL ONE/LOCAL_ONE. `spawn_replica_fragment_stream` errors rather
  than firing to the local host_id.
- **Auto-repair never claims convergence it did not observe.**
  `classify_repair_outcome` maps a table's session tallies to a `RepairOutcome`:
  `Converged` needs at least one successful session, no failures and no
  divergence. All-failed logs ERROR (table + failure count), partial failure
  WARNs, and zero sessions WARNs that agreement was not verified. Failed and
  empty cycles increment `ferrosa_auto_repair_tables_failed_total` /
  `ferrosa_auto_repair_tables_no_sessions_total`, are not counted as repaired,
  and are readable via `AutoRepairScheduler::last_outcome`; the round-robin
  cursor retries them.

### Accord cell timestamps (t_277e2bf9, t_cf637b6e)

`accord_cell_timestamp(t)` is `t.time / 1000`: the HLC is nanoseconds, cell
timestamps are microseconds. It is the write stamp, the read-at-`t` bound and
the read-vote stamp. Rows an older build stamped in nanoseconds are normalised
at decode (see ferrosa-storage), which is what keeps CAS on them working; the
two changes ship together. `agreed_row` compares read votes in canonical form
(decoded and re-encoded), so replicas on different builds agree on the same
row during a rolling upgrade (t_b986c335). `tests/accord_legacy_ns_compat.rs`
runs real two-node LWTs over data the old build wrote.

## Dependencies

**Calls** (ferrosa crates this depends on):
`ferrosa-cdc`, `ferrosa-common`, `ferrosa-index`, `ferrosa-net`,
`ferrosa-schema`, `ferrosa-sstable`, `ferrosa-storage`.

**Called by** (crates that depend on this):
`ferrosa`, `ferrosa-cql`, `ferrosa-ctl`, `ferrosa-flight`, `ferrosa-graph`,
`ferrosa-session`, `ferrosa-sparql`.

External: `openraft` (pinned fork), `sled`, `tokio`, `arc-swap`, `dashmap`,
`parking_lot`, `bincode`, `crc32fast`, `uuid`, `serde`, `tracing`.

## Tests

~1050 test functions across the crate (in-module `#[cfg(test)]` + `tests/`).
Notable integration suites: `failure_mode_matrix` (44), `raft_election_storm`
(36), `leader_snapshot_push` (31), `accord_lwt_concurrent` (21),
`accord_nemesis` (15), `correctness` (11), `cluster_formation` (10),
`joining_node_health` (9 — ring membership health + Promote-phase planning;
pins that a stuck `Joining` member is reported unhealthy and repaired, while
`Leaving` / `Decommissioned` / `Learner` are never promoted, CL-29). All run on
deterministic in-process harnesses unless gated behind `live-infra-tests`.

Range-scan memory boundedness is guarded by two allocator-tracking suites:
`range_scan_streaming_memory_bound` (the coordinator **Stream** API is O(1) in N)
and `replica_scan_serialization_memory_bound` (drives the REAL wire serialization
+ `consume_range_stream_into`; a two-phase measurement isolates the consumer's
resident set from producer/storage noise and asserts it is `O(chunk)`,
INDEPENDENT of N — the `t_3fc6be3c`/`t_ee98faa0` bounded-consume proof — plus the
producer/backpressure bounds). The gated multi-node live confirmation is `fly_stream_scan_live` (feature
`live-infra-tests` + `FERROSA_TEST_FLY=1`), which drives
`deploy/fly-stream-scan/`; it panics loudly on missing infra rather than passing.

Replicated `CREATE INDEX` (FM-70 / CL-18, `t_1f2741a0`) is guarded end to end by
`replicated_create_index` (three in-process Raft voters, each with its own
`StorageEngine` + `Schema` via `TestCluster::with_voters_and_engines`; the DDL is
proposed once on the leader and every node must build the index and answer an
indexed read) and, on a real cluster, `replicated_jsonb_index_live` (feature
`live-infra-tests` + `FERROSA_TEST_CLUSTER_NODES`, panics when unset).

The Accord `ReorderBuffer` drain is guarded without a wall-clock bound.
`accord::perf_regression::perf_regression_suite` measures the 1000-message drain
RELATIVE to a same-run CPU reference loop (the load-independent form of the
absolute `< 10 ms` it used to assert — the absolute form ejected a docs-only PR
at 52.8 ms, `forge t_430e21f7`, and the nightly fuzz lane at 56.7 ms on
2026-09-30 while the dedicated perf job passed in the same workflow). The
deterministic half lives in `tests/reorder_buffer_drain_budget.rs`: it counts
allocations under a `#[global_allocator]` hook (the drain must make ONE output
allocation regardless of message count — measured 1 allocation / 32 bytes per
message, constant from 1000 to 4000 messages) and asserts the per-message drain
cost stays linear. On an idle box the drain is ~0.16 ms for 1000 messages, and
the drain/reference-loop ratio holds at 0.72–0.80 from 0 to 576 competing
threads on an 18-core host, which is why the ratio form does not flake. The
structural invariants (completeness, `t0` order, arrival order within equal
`t0`, contiguous ready prefix, capacity/`len`) are pinned in
`src/accord/reorder_buffer.rs`, including a differential test against a flat
reference model.

The multi-node `TestCluster` harness (`tests/common/raft_harness.rs`) runs
openraft with short timers (50 ms heartbeat, 200–400 ms election). To keep
election convergence deterministic when `cargo test` runs many runtime-heavy test
binaries in parallel, the harness holds one of `K = ceil(cores/4)`
**cross-process** slots (an `fs2` advisory file lock) for each cluster's lifetime,
bounding aggregate raft-worker oversubscription. Leader-dependent setup uses
`require_leader(timeout)`, which fails loud at the real precondition rather than
panicking later in `leader_node()`.

### Write admission

`WritePath::write` and logged `write_batch` await the local storage engine's
per-table pressure gate before dispatching direct, pair, or cluster writes.
The gate uses async notification/deadline waiting only in the soft-pressure
zone; the storage write keeps its synchronous hard-limit check as the final
guard. Batch admission is preflighted before any logged mutation is applied.

## Specs

- [Architecture overview](specs/overview.md) — subsystem map, invariants, position
- [Data flow](specs/data-flow.md) — tunable-CL write + Accord transaction diagrams
- [FMEA / known issues](specs/fmea.md) — ranked failure modes + real evidence gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later

Related reference specs: `specs/reference/cluster-formation-architecture.md`,
`specs/reference/anti-entropy-repair-architecture.md`,
`specs/decisions/015-multi-dc-raft-per-dc-accord.md`,
`specs/todo/jepsen-e2e-test-plan.md`.
