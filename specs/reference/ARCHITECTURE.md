# Architecture

A one-page tour of the ferrosa codebase. New contributor goal: know where to put a change after ten minutes.

Ferrosa is a developer-preview Rust reimplementation of an Apache-Cassandra-shaped database with S3 as the durable substrate and local NVMe as a cache. It is a Cargo workspace of 18 crates; the `ferrosa` binary composes CQL, Cypher (Bolt + HTTP), SPARQL, and a Prometheus/JSON observability surface in one process. Treat this as an internal architecture map, not a production-readiness claim.

This document describes how the pieces fit together and the invariants that hold the system up. It does not teach CQL or Cassandra — it describes where ferrosa differs from defaults and where in the tree to find each piece.

## Layers and crates

The workspace (see [`Cargo.toml`](../Cargo.toml)) groups into five layers. Lower layers never depend on higher layers.

**Foundation.** [`ferrosa-common`](../ferrosa-common/) is a leaf crate of shared types: `Token` (i64, Murmur3Partitioner-compatible), `PartitionKey`, `DecoratedKey`, `CellValue`, error types, and the Accord primitives `TxnId`/`Ballot`/`accord::Timestamp` (HLC). **Invariant:** CQL-level types (text, collections, UDTs) do *not* live here — they live in `ferrosa-cql` so that `ferrosa-common` stays a dependency of everyone without pulling in protocol concerns.

**Storage.** [`ferrosa-sstable`](../ferrosa-sstable/) reads and writes Cassandra BTI SSTables over an abstract `ReadAt` trait (file impl here, S3 impl in `ferrosa-storage`). [`ferrosa-index`](../ferrosa-index/) is a pluggable index framework — BTree/Hash/Composite/Phonetic/Filtered/Vector (HNSW + IVFFlat)/FullText — with its own `IndexFactory`/`IndexBuilder`/`IndexReader` trait set. [`ferrosa-storage`](../ferrosa-storage/) owns the memtable (64-shard BTreeMap or skiplist behind a feature flag), commit log, STCS/UCS compaction, cache, S3 write-behind via the `object_store` crate, sidecar index persistence, PITR (`archiver.rs`, `snapshot.rs`, `restore.rs`), and the Accord durability helpers (`accord/sync_writer.rs`, `accord/write_gate.rs`, `accord/reorder_buffer.rs`). [`ferrosa-schema`](../ferrosa-schema/) owns `Schema` (an `ArcSwap<SchemaSnapshot>` for lock-free reads), RBAC, audit, virtual-table registry, and the `system_schema.*` / `system_auth.*` definitions.

**Consensus and transport.** [`ferrosa-net`](../ferrosa-net/) is a standalone crate (no `ferrosa-common` dependency) implementing the 12-byte-header internode frame, 26 message types, PSK-HMAC handshake, three-lane `PriorityPool` (Raft/Data/Bulk TCP lanes per peer), and `HandlerRegistry` with the `LazyRaft` pattern that lets us register Raft handlers *before* Raft itself is initialized. [`ferrosa-cluster`](../ferrosa-cluster/) sits on top: openraft-based metadata consensus (schema + topology), the `ModeController` state machine (Standalone → Pair → Forming → Cluster), token ring, tunable-CL coordinator (`coordinator/write.rs`, `coordinator/read.rs`), hinted handoff, and Accord transaction components (`accord/state_machine.rs`, `accord/coordinator.rs`, recovery, electorate, and test harness code). Public Jepsen evidence is still tracked as verification work.

**Query and client.** [`ferrosa-cql`](../ferrosa-cql/) is the CQL v4/v5 server — framing, lexer, recursive-descent parser with depth/collection caps, router, prepared-statement cache (moka W-TinyLFU), SASL PLAIN auth, `ScanPlan` planner, and the `ConnectionTracker`/`QueryTracker` that back `system_observability`. [`ferrosa-graph`](../ferrosa-graph/) is the Cypher engine: parser, logical+physical planner, hop-by-hop expand executor, `AdjacencyIndexObserver` hooked into the storage write path, Bolt v5 server, HTTP endpoint with Basic auth and TLS. [`ferrosa-sparql`](../ferrosa-sparql/) is a SPARQL 1.1 endpoint (Query + Update, RDF*, property paths) built on spargebra/oxrdf that translates to `rdf_triples` CQL rows. [`ferrosa-udf`](../ferrosa-udf/) runs user-defined functions as WASM components under Wasmtime with fuel-capped sandboxing.

**Additional client surfaces.** [`ferrosa-postgres`](../ferrosa-postgres/) is a PostgreSQL frontend/backend wire-protocol (v3) server (port 5432, developer preview) with SCRAM-SHA-256 auth and both the simple and extended (Parse/Bind/Describe/Execute) query protocols. It parses and plans SQL via the bespoke [`ferrosa-sql`](../ferrosa-sql/) engine (decision D3 — no DataFusion/Arrow) and supports `SELECT`, single-row `INSERT`/`UPDATE`/`DELETE`, and transaction protocol state. Its writes go through [`ferrosa-row-bridge`](../ferrosa-row-bridge/) — the **single canonical CQL row codec** (decision D10) that `ferrosa-cql` also re-exports — so a row written over Postgres decodes byte-identically over CQL; `ferrosa-postgres` deliberately does **not** depend on `ferrosa-cql`. The front-end is cross-checked against a real PostgreSQL 16 by a differential oracle in CI ([postgres-differential-oracle](../implemented/postgres-differential-oracle.md)). [`ferrosa-flight`](../ferrosa-flight/) is an Apache Arrow Flight gRPC endpoint: a client places a CQL `SELECT` in the Flight ticket, `ferrosa-cql` executes it, and `convert.rs` streams the result back as Arrow record batches; `Handshake` issues a signed bearer token (decision D4) that every other RPC requires. [`ferrosa-cdc`](../ferrosa-cdc/) is a bounded change-data-capture bus (leaf on `ferrosa-common`) carrying `WrittenOnNode` (commit-log apply) and `CommittedToCluster` (Accord/quorum) event streams that back CQL `SUBSCRIBE` and the Flight CDC channel; producers in `ferrosa-storage` and `ferrosa-cluster` convert mutations to `CdcEvent` at the tap. [`ferrosa-view`](../ferrosa-view/) holds conflict-free materialized-view primitives (`ViewMetadata`, `validate_view_def`, `compute_view_delta`) pending engine wiring.

**Operations.** [`ferrosa-ctl`](../ferrosa-ctl/) is a CLI + ratatui TUI that connects over CQL. [`ferrosa-worker`](../ferrosa-worker/) is the shared background-task registry. [`ferrosa-jepsen`](../ferrosa-jepsen/) and [`ferrosa-loadgen`](../ferrosa-loadgen/) are test harnesses (Firecracker-based Jepsen, UCS compaction stress). [`ferrosa-index-builder`](../ferrosa-index-builder/) is a standalone HTTP service that offloads secondary-index construction from the engine; the engine talks to it when `FERROSA_INDEX_BACKEND=remote`.

**Binary.** [`ferrosa`](../ferrosa/) is the composition root. Startup order in [`ferrosa/src/main.rs`](../ferrosa/src/main.rs) is: tracing → host_id → `StorageEngine::new` → `Schema::new` → `ModeController::new` → `PeerManager` → RPC server on :7000 → CQL on :9042 → optional PostgreSQL wire on :5432 → optional Arrow Flight gRPC → optional graph HTTP :7474 + Bolt :7687 → SPARQL :8080 → web console + cluster REST API on :9090.

## Write path (single CQL INSERT)

1. Client frame lands in [`ferrosa-cql/src/server.rs`](../ferrosa-cql/src/server.rs); an `IpSlotGuard` registers the per-IP slot.
2. `frame.rs` → `parser.rs` → `router.rs`. The router intercepts virtual-keyspace SELECTs before touching storage (see [`router.rs` virtual-table fast path](../ferrosa-cql/src/router.rs)).
3. If the statement is a regular INSERT/UPDATE/DELETE, the router hands off to `ferrosa-cluster::WritePath` ([`write_path.rs`](../ferrosa-cluster/src/write_path.rs)). The `WritePath` enum (`Direct`/`Pair`/`Cluster`/`Unavailable`) makes the routing an explicit atomic choice that matches the current `ModeController` state.
4. In Cluster mode, `ClusterCoordinator` ([`coordinator/write.rs`](../ferrosa-cluster/src/coordinator/write.rs)) fans out to replicas chosen by `TokenRing` and blocks for `ConsistencyLevel::blockFor(CL)` ACKs.
5. On each replica, the mutation hits `StorageEngine::write()` which (a) appends to the commit log segment via CAS on an `AtomicUsize` offset (no lock held during serialization), (b) writes into the active 64-shard memtable, and (c) fires the `WriteObserver` chain (`AdjacencyIndexObserver`, `SubscriptionObserver`, Accord sidecar writers). The commit-log entry format is self-describing — see [`ferrosa-storage/src/commitlog/mutation.rs`](../ferrosa-storage/src/commitlog/mutation.rs).
6. On memtable pressure, `arc-swap` atomically replaces the active memtable with a fresh one. The old memtable flushes through `SSTableWriter` (see invariants below) to local NVMe. `UploadManager` ([`ferrosa-storage/src/upload/manager.rs`](../ferrosa-storage/src/upload/manager.rs)) pushes the resulting component files to S3 over a bounded mpsc channel with exponential backoff.
7. If the table is NVMe-pinned (`pin_config.is_pinned()`), step 6 *skips* S3 and pins the SSTable in `LocalCache` — this is the one place where "S3 is the source of truth" is intentionally violated.

LWTs and `BEGIN TRANSACTION` divert at step 3 into `AccordCoordinator` ([`ferrosa-cluster/src/accord/coordinator.rs`](../ferrosa-cluster/src/accord/coordinator.rs)) — PreAccept → (fast path 3/4 quorum) Commit → Execute, or PreAccept → Accept → Commit → Execute on the slow path — before the write reaches `SyncWriter` and the normal storage engine.

## Read path (single CQL SELECT)

1. Frame → parse → router, same as writes. Router checks `state.schema.virtual_tables().get(ks, table)` first; hits skip the storage engine entirely.
2. `planner.rs` produces a `ScanPlan`: `PrimaryKey` (partition-bound), `SingleIndex`, `IndexIntersection` (merge RowPositions from multiple indexes before fetch), or `FullScan`.
3. `WritePath::pk_read()` routes through `ClusterCoordinator::read` which picks replicas, favoring the local one, and enforces CL.
4. On a replica, `StorageEngine::read` calls `merge::read_range()` ([`ferrosa-storage/src/merge.rs`](../ferrosa-storage/src/merge.rs)) which merges the active memtable, any flushing memtable, and every relevant SSTable (last-write-wins, with row-level tombstone merge). SSTables are accessed through `Arc` refcounts, so in-flight reads pin the set against compaction.
5. SSTable component access: Bloom filter → Partitions.db trie → Rows.db trie → Data.db. Cache misses in `LocalCache` fall through to S3 via the `ReadAt` S3 implementation in `ferrosa-storage`; the object is warmed into the cache before being returned.
6. For indexed reads, per-SSTable sidecar files (`{generation}-<kind>-{idx}.db`) provide `(value → RowPosition)` entries; a missing sidecar triggers per-SSTable fallback to full scan and schedules a rebuild via `IndexBuildScheduler`.

## Consensus split

Raft (openraft, [`ferrosa-cluster/src/raft/`](../ferrosa-cluster/src/raft/)) is the *metadata* log: schema changes, topology (node join/leave, token assignment), and cluster config. Every node applies the same `RaftCommand` sequence to a deterministic `FerrosStateMachine` backed by `sled`. DDL and node lifecycle are never gossip-based — they are Raft-applied-index compared.

Accord ([`ferrosa-cluster/src/accord/`](../ferrosa-cluster/src/accord/)) is the *transaction* protocol: serializable LWT and multi-statement `BEGIN TRANSACTION` with no dedicated coordinator (any node may coordinate). `ConflictIndex` tracks key-range conflicts; `DepWaitGraph` orders execution; `RecoveryCoordinator` handles the 11 interrupted-transaction scenarios; `DdlDrain` forces Accord to quiesce before a Raft-committed DDL applies.

## Invariants, and where they are enforced

**Storage durability.** S3 is the source of truth; local NVMe is cache. Exceptions must be explicit: NVMe-pinned tables (`pin_config.is_pinned()`) skip upload; this is the only sanctioned deviation. Enforced at the flush/compaction pin check in [`ferrosa-storage/src/engine.rs`](../ferrosa-storage/src/engine.rs) and the compaction state machine described in [`specs/data-flow.md`](data-flow.md).

**SSTableWriter produces readable SSTables or fails loud.** Landed 2026-04-19 after a `tool_usage_log` corruption incident. Two gates in [`ferrosa-sstable/src/writer.rs`](../ferrosa-sstable/src/writer.rs):
- **Gate A — clustering-shape validation** (`validate_clustering_shape`, called from `add_partition` around line 184). Rejects any row whose clustering bytes do not match the `SerializationHeader`'s declared fixed-length clustering columns *before* a single byte hits Data.db.
- **Gate B — defensive self-readback** (`verify_output_readable`, called from `finish` around line 294, gated by `WriteOptions.verify_output` which defaults to `true`). Reopens the freshly-built components through the full reader pipeline and asserts that the trie, Data.db, and partition counts agree.

Both gates exist specifically so that a writer-side schema mismatch crashes the flush with a typed `InvalidFormat` error rather than producing a pair of files that opens but blows up mid-iteration with `read_exact_at: wanted N, got M`.

**Schema convergence is Raft-applied-index, not UUIDs.** `Schema` is never mutated in place — it is `ArcSwap<SchemaSnapshot>`. A DDL is applied only when `FerrosStateMachine::apply` commits the `RaftCommand`. Gossip UUIDs are maintained for driver compatibility but are not the source of truth ([`ferrosa-schema/src/registry.rs`](../ferrosa-schema/src/registry.rs)).

**Graph engine owns its backing tables.** The graph engine stores topology in ordinary CQL tables marked with `graph.type = "edge"` / `graph.type = "node"` extensions (see the extension lookups in [`ferrosa-graph/src/engine.rs:104`](../ferrosa-graph/src/engine.rs) and `:242-267`) and maintains a system-managed adjacency keyspace `system_graph_<user_keyspace>` ([`ferrosa-graph/src/adjacency/schema.rs:32`](../ferrosa-graph/src/adjacency/schema.rs)). **Invariant:** only `ferrosa-graph` writes to `graph.type`-tagged tables and to `system_graph_*` keyspaces, and it does so only through the Cypher path. Arbitrary CQL writes that bypass Cypher will desynchronize the adjacency index from the user tables; the `AdjacencyIndexObserver` ([`ferrosa-graph/src/adjacency/observer.rs`](../ferrosa-graph/src/adjacency/observer.rs)) is how writes stay consistent, and the background reconciler ([`adjacency/reconcile.rs`](../ferrosa-graph/src/adjacency/reconcile.rs)) is a fallback, not a license to write directly. T6 extension validation in `ferrosa-schema` and T7 system-table protection enforce this at DDL time.

**Commit-log entries are self-describing; SSTable rows are delta-encoded.** Do not try to reuse one format for the other. Commit-log framing lives in [`ferrosa-storage/src/commitlog/mutation.rs`](../ferrosa-storage/src/commitlog/mutation.rs). SSTable row serialization against a `SerializationHeader` lives in [`ferrosa-sstable/src/data.rs`](../ferrosa-sstable/src/data.rs).

**Manifest is etag-CAS, not leader-owned.** Every `manifest.json` update is a conditional PUT via `object_store::PutMode::Update` ([`ferrosa-storage/src/manifest.rs`](../ferrosa-storage/src/manifest.rs)). Two nodes compacting into the same manifest will have one fail and retry.

**Raft handlers register via `LazyRaft` before Raft initializes.** `HandlerRegistry` supports runtime-dynamic registration precisely so the formation code in [`ferrosa-cluster/src/raft/handlers.rs`](../ferrosa-cluster/src/raft/handlers.rs) can wire the `RaftAppendHandler`/`RaftVoteHandler`/`RaftSnapshotHandler` through a watch channel before `openraft::Raft::new` is called. Violating this order causes the handler-registration race that the `LazyRaft` pattern was introduced to close.

## Auth surfaces

Several independent entry points, each with its own auth stack, plus a single kill switch.

- **CQL :9042** — SASL PLAIN in [`ferrosa-cql/src/auth.rs`](../ferrosa-cql/src/auth.rs) → `Schema::authenticate()` (bcrypt/argon2id) → RBAC `check_permission()`. Connection state machine `AwaitingStartup → Authenticating → Ready`, max 3 attempts per connection, rate-limited per IP, RAII `IpSlotGuard` for slot release.
- **Web + cluster REST API :9090** — Basic auth middleware in [`ferrosa/src/web/auth.rs`](../ferrosa/src/web/auth.rs); routes for snapshots/restore/promote/switchover/archive_status and the JSON observability feed in [`ferrosa/src/web/api.rs`](../ferrosa/src/web/api.rs). The middleware short-circuits on `state.auth_disabled` ([`auth.rs:46`](../ferrosa/src/web/auth.rs)).
- **Graph HTTP :7474 and Bolt :7687** — Basic auth (T2) in `ferrosa-graph/src/http.rs`, per-hop authorization checks at the logical planner (T3), TLS enforced (T11), `RequestBodyLimitLayer` and `CatchPanicLayer` wrapping the Axum router.
- **SPARQL :8080** — gated by `FERROSA_SPARQL_ENABLED`; auth is currently the same Basic-auth pattern as the graph endpoint.
- **PostgreSQL wire :5432** — SCRAM-SHA-256 in `ferrosa-postgres` (`handshake`/`scram`) validating the same `Schema` roles as CQL; honors the auth kill switch. Developer preview.
- **Arrow Flight gRPC** — `Handshake` validates CQL credentials and returns a signed (HMAC) bearer token with a TTL (decision D4); every other RPC requires `authorization: Bearer <token>` and derives its `AuthContext` from the verified claims — no anonymous access.

**`FERROSA_AUTH_DISABLED` flattens the CQL and web surfaces simultaneously.** It is read once in [`ferrosa/src/main.rs:532`](../ferrosa/src/main.rs) and propagated into both `CqlConfig` and `WebConfig`. When true: CQL accepts any (or no) credentials and the web auth middleware passes through. It is intended only for local dev and CI — docker-compose and the CI workflow set it; production startup validation in [`ferrosa-schema/src/startup.rs`](../ferrosa-schema/src/startup.rs) refuses to start with `DeploymentMode::Production` and auth disabled.

**Current gaps** (tracked in `specs/in-process/`): the graph and SPARQL endpoints do not currently honor `FERROSA_AUTH_DISABLED` (they require their own Basic-auth header even in dev); the internode PSK is not rotatable at runtime; Bolt has only password auth, no Kerberos.

## Cluster formation (one paragraph)

Nodes transition Standalone → Pair → Forming → Cluster automatically as peers connect, driven by `ModeController` ([`ferrosa-cluster/src/controller/mod.rs`](../ferrosa-cluster/src/controller/mod.rs)). There is no mode flag. Pair mode is a write-forwarding two-node configuration with a switchover protocol; Forming mode blocks DDL while schema converges through Raft and bootstrap streams run bidirectionally; Cluster mode enables Accord and tunable CL. The CQL broadcast address for `system.peers.native_address` is resolved in a three-tier fallback (ring → PeerManager → internode fallback), which is the hook for Docker port-mapping and NAT. See [`specs/cluster-formation-architecture.md`](cluster-formation-architecture.md) for the full state machine; [`specs/data-flow.md`](data-flow.md) has the sequence diagram.

## Observability, briefly

The `system_observability` keyspace is *code-backed* — every table is a `dyn VirtualTable` registered in a lock-free `ArcSwap` registry ([`ferrosa-schema/src/virtual_registry.rs`](../ferrosa-schema/src/virtual_registry.rs)). The router intercepts SELECTs on these keyspaces before the storage engine. `ferrosa-cql` exposes `connections` and `active_queries`; `ferrosa-storage` exposes `storage_stats` and `secondary_indexes`. The `/metrics` endpoint ([`ferrosa-cql/src/prometheus.rs`](../ferrosa-cql/src/prometheus.rs)) renders the same tables as Prometheus text exposition — text columns become labels, numeric columns become `ferrosa_<table>_<column>`. The web dashboard and `ferrosa-ctl monitor` both consume these tables; the CLI goes over CQL, the dashboard over JSON. Distributed tracing writes spans directly into `system_observability.spans` via `StorageEngine::write_observability()` to avoid a CQL feedback loop ([`specs/observability-architecture.md`](observability-architecture.md)).

## Where to put things

- A new CQL statement → lexer tokens + `ast.rs` variant + `parser.rs` arm + `router.rs` dispatch + executor in the appropriate subsystem.
- A new index type → implement `IndexFactory`/`IndexBuilder`/`IndexReader` in `ferrosa-index`, register in the factory, add the `USING '<name>'` string, add a sidecar emitter in `ferrosa-storage/src/index/`.
- A new internode RPC → add a `MsgType` in `ferrosa-net/src/codec.rs`, a `Message` variant in `message.rs`, an RPC handler, register through `HandlerRegistry` (use `LazyRaft` if Raft-adjacent).
- A new virtual table → implement `VirtualTable` in the owning crate, register with `VirtualTableRegistry` at startup, add a row to the `system_observability` documentation.
- A new storage-side effect on writes → implement `WriteObserver` and pick `ObserverMode::Sync` or `Async`. Async observers run over a bounded mpsc and drop on backpressure by design (T9).
- Anything touching on-disk format → add a reader round-trip test *and* keep `WriteOptions.verify_output = true` enabled for the duration of the change.

## Related specs

- [overview.md](overview.md) — design principles, decision table, AWS lock-in flags
- [components.md](components.md) — per-crate module inventory and build order
- [data-flow.md](data-flow.md) — full sequence diagrams for writes, reads, Accord, PITR
- [cluster-formation-architecture.md](cluster-formation-architecture.md) — state machine, ClusterInvite, degraded modes
- [observability-architecture.md](observability-architecture.md) — tracing layer, schema, sampling
