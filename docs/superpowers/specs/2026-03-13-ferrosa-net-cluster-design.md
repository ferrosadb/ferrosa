# ferrosa-net and ferrosa-cluster Design

> Last updated: 2026-03-13
> Status: Draft

## Goal

Add distributed operation to Ferrosa via two new crates: **ferrosa-net** (internode
transport and RPC service layer) and **ferrosa-cluster** (Raft consensus, token ring,
coordinator pattern, and replication). Support three deployment modes — Standalone,
Pair, and Cluster — with progressive formation from a single node to a full cluster.

## Architecture

ferrosa-net is a "smart net" crate owning transport, RPC dispatch, service discovery,
and failure detection. ferrosa-cluster is a "focused cluster" crate owning Raft
consensus, token ring, consistency levels, and coordination. ferrosa-cluster depends
on ferrosa-net for all communication with peers.

**Crate dependency graph (additions in bold):**

```text
ferrosa-common
├── ferrosa-sstable
│   └── ferrosa-storage
│       ├── ferrosa-schema
│       │   ├── ferrosa-cql
│       │   ├── ferrosa-graph
│       │   └── **ferrosa-cluster**
│       └── **ferrosa-cluster**
└── ferrosa-ctl

**ferrosa-net** (no dependency on ferrosa-common)
└── **ferrosa-cluster**
```

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Transport | TCP + rustls; QUIC behind feature flag | TCP is proven; QUIC can slot in later |
| Discovery | Static seeds (CLI + env) + optional DNS | Seeds for bootstrap, DNS for k8s/ECS |
| RPC model | Request-response + streaming + fire-and-forget | Covers coordination, bulk transfer, and heartbeats |
| Connection pool | Priority lanes (raft, data, bulk) | Prevents bulk transfers from starving Raft heartbeats |
| Crate split | Smart Net + Focused Cluster | RPC and failure detection live close to connections; consensus and coordination stay focused |
| Raft scope | Schema + topology + token transitions + cluster config | Single source of truth for all cluster-wide state |
| Deployment modes | Standalone / Pair / Cluster (progressive) | Organic growth from 1 node to full cluster |
| Pair writes | Secondary forwards to primary transparently | Client doesn't need to know which node is primary |
| Pair failover | Manual operator promotion only | No auto-promotion prevents split brain with 2 nodes |
| Serialization | Custom binary (length-prefixed, big-endian) | No protobuf dependency; matches CQL codec pattern |
| Phase 1 auth | PSK (HMAC-SHA256), mTLS in Phase 2 | Authentication from day one; cluster name alone is not a secret |
| Node join | Operator approval required in production | Prevents unauthorized nodes from receiving replicated data |

## Related ADRs

- [ADR-002](../../../specs/decisions/002-cql-only-compat.md) — Own internode protocol
- [ADR-003](../../../specs/decisions/003-raft-metadata.md) — Raft for metadata, tunable CL
- [ADR-010](../../../specs/decisions/010-production-mode.md) — mTLS required in production

> **Note:** `specs/components.md` needs updating when this spec is implemented:
> add `ferrosa-cluster → ferrosa-schema` and `ferrosa-cluster → ferrosa-net` edges,
> and remove the `ferrosa-net → ferrosa-common` edge (ferrosa-net is standalone).

---

## Part 1: ferrosa-net

### Responsibilities

ferrosa-net owns:

- Wire protocol (framing, codec, handshake)
- TLS (rustls, mTLS in production mode)
- Priority-lane connection pool (raft, data, bulk)
- RPC service layer — typed request handlers registered by ferrosa-cluster
- Service discovery (static seeds + optional DNS)
- Failure detection (heartbeat pings on raft lane, configurable timeout)
- Peer lifecycle events (connected, disconnected, suspected-dead)

ferrosa-net does NOT own:

- What to do when a peer dies (ferrosa-cluster decides)
- Raft message handling logic (ferrosa-cluster registers handlers)
- Token ring, replica selection, consistency levels
- Schema or storage

### Wire Protocol

Binary frame format, 12-byte header:

| Field | Size | Description |
|-------|------|-------------|
| version | 1 byte | Protocol version (starts at 1) |
| flags | 1 byte | Compression, stream, fire-and-forget |
| lane | 1 byte | 0=raft, 1=data, 2=bulk |
| msg_type | 1 byte | RPC message type enum |
| stream_id | 4 bytes | Request/response correlation (u32) |
| length | 4 bytes | Body length (u32 big-endian, max 256 MiB) |

Codec rejects frames with `length > MAX_FRAME_BODY_SIZE` (default 256 MiB) before
allocating, and closes the connection with `ProtocolError` for unknown `msg_type`
or invalid `lane` values (must be 0-2).

**Flag bits:**

| Bit | Meaning |
|-----|---------|
| 0 | Compressed (LZ4 or Snappy) |
| 1 | Stream-start (first frame in a streaming sequence) |
| 2 | Stream-end (last frame in a streaming sequence) |
| 3 | Fire-and-forget (no response expected) |

### Message Types

Initial message type set:

| Category | Messages |
|----------|----------|
| Raft | `RaftAppendEntries`, `RaftAppendResponse`, `RaftVote`, `RaftVoteResponse`, `RaftInstallSnapshot` |
| Data | `MutationForward`, `MutationAck`, `ReadRequest`, `ReadResponse` |
| Streaming | `StreamStart`, `StreamChunk`, `StreamEnd` |
| Lifecycle | `Handshake`, `HandshakeAck`, `Ping`, `Pong` |
| Pair | `PairWriteForward`, `PairWriteAck`, `PairCatchUp`, `PairCatchUpResponse`, `RoleSwap` |
| Observability | Reuses request-response for internode observability queries |

### Message Body Serialization

Message bodies use hand-rolled big-endian binary encoding, matching the CQL codec
pattern already used in ferrosa-cql. Each message type defines its own layout:

- **Integers:** big-endian (u16, u32, u64, i64)
- **Strings:** 2-byte BE length prefix + UTF-8 bytes
- **Byte buffers:** 4-byte BE length prefix + raw bytes
- **UUIDs:** 16 bytes, big-endian
- **Lists/maps:** 4-byte BE element count + repeated elements

This avoids adding a serialization framework dependency (protobuf, bincode,
postcard) and keeps the wire format consistent with CQL's approach. Each message
type implements `encode(&self, buf: &mut BytesMut)` and
`decode(buf: &mut Bytes) -> Result<Self>` methods.

### Handshake

On connect, nodes exchange `Handshake` containing:

- `cluster_name` — must match; reject immediately on mismatch
- `host_id` — sender's UUID
- `protocol_version` — negotiate to lowest common version
- `supported_compression` — negotiate compression algorithm
- `auth_token` — HMAC-SHA256 of `cluster_name + host_id + nonce` using a
  pre-shared key (`FERROSA_INTERNODE_PSK`). Provides authentication in Phase 1
  before mTLS is implemented. In Phase 2+, mTLS replaces this; PSK becomes
  optional (defense-in-depth).

Connections that don't complete handshake within `FERROSA_HANDSHAKE_TIMEOUT_SECS`
(default 5) are closed.

### Connection Pool

`PriorityPool` maintains 3 TCP connections per peer:

| Lane | Purpose | Timeout |
|------|---------|---------|
| Raft | Consensus messages (small, latency-critical) | 1s |
| Data | Mutation forwarding, read requests | 10s |
| Bulk | SSTable streaming, bootstrap, snapshot transfer | 60s |

Each connection is multiplexed via stream IDs (max `MAX_STREAMS_PER_LANE`,
default 128 concurrent). Lane assignment is determined by the `lane` field in
the frame header.

`RpcServer` limits total inbound connections to `FERROSA_MAX_INTERNODE_CONNECTIONS`
(default 100). Each peer needs only 3 connections, so 100 supports ~33 peers with
headroom. New connections beyond the limit are rejected after handshake with an
`Overloaded` error.

### Failure Detection

- Heartbeat `Ping` sent on raft lane every 500ms (configurable via
  `FERROSA_HEARTBEAT_INTERVAL_MS`)
- Peer marked suspected-dead after 3 missed heartbeats (1.5s default, configurable
  via `FERROSA_HEARTBEAT_TIMEOUT_MS`)
- ferrosa-net emits `on_peer_suspected` event to registered `PeerEventListener`
- ferrosa-cluster decides what to do (mark hints, trigger failover prompt, etc.)

### Service Discovery

**Static seeds:** `--seed <addr>` CLI arg (repeatable) or `FERROSA_SEED` env var
(comma-separated). CLI takes precedence if both provided. No seed = standalone.

**DNS discovery:** Optional `FERROSA_DNS_DISCOVERY` resolves a DNS name to peer
addresses. Useful for Kubernetes headless services or AWS Cloud Map. Seeds are
always the fallback.

### Key Abstractions

```rust
/// Register typed message handlers.
trait RpcHandler: Send + Sync {
    /// Handle a message from a peer. Returns None for fire-and-forget.
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message>;
}

/// Subscribe to peer lifecycle events.
trait PeerEventListener: Send + Sync {
    fn on_peer_connected(&self, peer: PeerId);
    fn on_peer_disconnected(&self, peer: PeerId);
    fn on_peer_suspected(&self, peer: PeerId);
}

/// Identifies a peer.
type PeerId = (Uuid, SocketAddr);
```

`RpcServer` — listens, accepts connections, dispatches incoming messages to
registered handlers by `msg_type`.

`RpcClient` — maintains connection pool to a peer, sends typed requests, returns
responses (or fires-and-forgets).

`PeerManager` — holds `RpcClient` per known peer, emits lifecycle events, runs
failure detection loop.

### Module Structure

```text
ferrosa-net/src/
├── lib.rs
├── codec.rs            // InternodeCodec (Encoder/Decoder), frame header
├── message.rs          // Message enum, serialization/deserialization
├── handshake.rs        // Connection handshake protocol
├── tls.rs              // rustls config, mTLS certificate loading
├── pool.rs             // PriorityPool: 3 lanes per peer
├── rpc/
│   ├── mod.rs          // RpcServer, RpcClient types
│   ├── server.rs       // Listen, accept, dispatch to handlers
│   ├── client.rs       // Send request, await response, fire-and-forget
│   └── handler.rs      // Handler trait, handler registry
├── discovery/
│   ├── mod.rs          // Discovery trait
│   ├── seeds.rs        // Static seed list from CLI/env
│   └── dns.rs          // DNS-based discovery (optional)
├── peer.rs             // PeerManager: peer lifecycle, failure detection
└── config.rs           // NetConfig
```

### Dependencies

- `tokio`, `tokio-util` — async runtime, codec
- `tokio-rustls` — TLS
- `bytes` — zero-copy buffers
- `lz4_flex`, `snap` — frame compression (shared with ferrosa-cql)
- `tracing` — structured logging
- `arc-swap` — lock-free peer list updates
- `uuid` — PeerId

ferrosa-net has no dependency on ferrosa-common — it is a standalone transport
library. `PeerId` uses `uuid` directly, not common types like `Token` or
`PartitionKey`.

---

## Part 2: ferrosa-cluster

### Responsibilities

ferrosa-cluster owns:

- Raft consensus via `openraft` (single cluster-wide group)
- Raft state machine: schema, topology, token assignments, cluster config
- `ClusterState` trait implementation (replaces `SingleNodeClusterState`)
- Token ring: replica placement for `SimpleStrategy` and `NetworkTopologyStrategy`
- Coordinator pattern: route mutations/reads to correct replicas
- Consistency level enforcement (ONE, QUORUM, ALL, LOCAL_QUORUM, etc.)
- Write replication: forward mutations to replicas, collect ACKs
- Read coordination: fan out to replicas, merge results, optional read repair
- Node lifecycle: join, leave, decommission, bootstrap
- Hinted handoff: store-and-forward for unavailable replicas
- Schema distribution: DDL through Raft, applied at consistent log index
- Pair mode protocol: write forwarding, catch-up, switchover

### Deployment Modes

Three modes with progressive formation:

| Mode | Nodes | Consensus | Schema | Data replication |
|------|-------|-----------|--------|-----------------|
| `Standalone` | 1 | None | Local only | None |
| `Pair` | 2 | None — synchronous write-both | Primary pushes DDL, secondary ACKs | Primary writes both, ACKs after both confirm |
| `Cluster` | 3+ | Raft (openraft) | Raft-replicated | Tunable CL |

**Progressive formation:**

1. First node starts with no seed → Standalone
1. Second node joins (via `--seed`) → both transition to Pair
1. Third node joins → all three transition to Cluster (Raft group forms)
1. Subsequent nodes join existing Raft group

Mode is inferred from node count. `FERROSA_CLUSTER_MODE` can be set explicitly to
prevent transitions (e.g., `pair` rejects a third node). Mode transition is a
one-way ratchet: Standalone → Pair → Cluster.

### Pair Mode

**Roles:** One primary, one secondary. Client connects to either node transparently.

**Writes:** If client connects to secondary, secondary forwards write to primary
invisibly. Primary writes locally, then synchronously replicates to secondary
before ACK to client.

**Reads:** Both nodes serve reads from local storage. Secondary reads may be
slightly stale (bounded by replication lag).

**DDL:** Primary only. Pushes schema changes to secondary synchronously.

**Replication lag tracking:**

- Primary tracks last mutation sequence ACKed by secondary
- Exposed via `system_observability.replication_lag` virtual table:
  `last_ack_seq`, `last_ack_time`, `lag_ms`, `pending_mutations`
- ferrosa-ctl `monitor` TUI gets a replication panel

**Secondary catch-up protocol:**

The catch-up protocol uses **commit log segment:offset positions** as sequence
numbers. Each mutation in the commit log has a unique position defined by
`(segment_id: u64, offset: u32)`. Secondary tracks the last position it
successfully applied.

- On reconnect, secondary sends `PairCatchUp { last_segment_id, last_offset }`
  to primary
- Primary locates that position in its commit log and replays all mutations
  from that point forward via the data lane
- If the segment has been recycled (compacted away), primary responds with
  `PairCatchUpResponse::FullBootstrapRequired` and secondary falls back to
  S3 snapshot restore + delta catch-up

**Switchover (planned, operator-initiated):**

1. Operator sends switchover command via ferrosa-ctl (or admin RPC)
1. Primary stops accepting new writes, drains in-flight writes
1. Primary confirms secondary is fully caught up (replication lag = 0)
1. Primary sends `RoleSwap` message to secondary
1. Secondary promotes to primary, begins accepting writes
1. Old primary demotes to secondary
1. Brief unavailability window (in-flight drain only, no data movement)

**Failover (unplanned):**

- Secondary detects primary unreachable (heartbeat timeout)
- Secondary does NOT auto-promote — operator must explicitly promote via ferrosa-ctl
- This prevents split brain: with only 2 nodes, there is no way to distinguish
  "network partition" from "node is dead"
- Primary going down: secondary serves stale reads, writes unavailable until
  operator promotes
- Secondary going down: primary continues reads and writes (unreplicated)
- Network partition: both stay in current roles, operator investigates

**Primary election on first pair formation:**

- Higher `host_id` wins (deterministic, no consensus needed)

### Raft State Machine (Cluster Mode)

Single Raft group managing four categories of state:

```rust
// All maps use BTreeMap (not HashMap) to ensure deterministic iteration
// order across nodes. apply() must be purely deterministic: no wall-clock
// timestamps, no random values, no HashMap. All values come from the
// Raft command content or Raft log index.
struct RaftState {
    // 1. Schema
    schema_version: Uuid,
    keyspaces: BTreeMap<String, KeyspaceMetadata>,
    tables: BTreeMap<(String, String), TableMetadata>,
    roles: BTreeMap<String, RoleMetadata>,
    grants: BTreeMap<String, Vec<GrantEntry>>,

    // 2. Topology
    members: BTreeMap<Uuid, NodeInfo>,   // host_id → node info

    // 3. Token assignments
    token_map: BTreeMap<Token, Uuid>,   // token → owning host_id
    pending_ranges: Vec<RangeTransfer>, // in-flight token moves

    // 4. Cluster config
    config: ClusterConfig,              // CL defaults, compaction settings, etc.
}
```

**Raft commands:**

| Category | Commands |
|----------|----------|
| Schema | `CreateKeyspace`, `AlterKeyspace`, `DropKeyspace`, `CreateTable`, `AlterTable`, `DropTable` |
| Auth | `CreateRole`, `AlterRole`, `DropRole`, `Grant`, `Revoke` |
| Topology | `JoinNode`, `LeaveNode`, `MoveToken` |
| Config | `UpdateConfig` |

**Apply flow:** When a Raft command is committed, every node applies it to both
the Raft state machine AND the local `Schema` (via `ArcSwap` swap). CQL queries
never touch Raft directly — they read lock-free schema snapshots.

**Snapshots:** Full `RaftState` serialized to S3. New nodes bootstrap by
downloading latest snapshot + replaying recent log entries.

**Raft group size:** 3-5 voter nodes. Additional nodes join as learners (receive
log entries but don't vote).

### Consistency Levels

| Level | `blockFor(RF)` | Scope |
|-------|---------------|-------|
| `ONE` | 1 | Any replica |
| `TWO` | 2 | Any 2 replicas |
| `THREE` | 3 | Any 3 replicas |
| `QUORUM` | (RF/2)+1 | Any quorum |
| `ALL` | RF | All replicas |
| `LOCAL_ONE` | 1 | Same DC only |
| `LOCAL_QUORUM` | (DC_RF/2)+1 | Same DC only |
| `EACH_QUORUM` | (DC_RF/2)+1 per DC | Every DC meets local quorum |

**CL behavior per deployment mode:**

| Mode | Behavior |
|------|----------|
| Standalone | CL ignored — all operations are local |
| Pair | Writes always go to both (effective ALL). Reads always local (effective ONE). CL accepted but not meaningful. |
| Cluster | Full CL enforcement |

### Coordinator Pattern (Cluster Mode)

1. CQL server receives query
1. Router computes partition token from partition key
1. Coordinator looks up replica set from token ring
1. **Write:** forward mutation to `blockFor(CL)` replicas via ferrosa-net data
   lane, wait for ACKs, respond to client. Async hinted handoff for unavailable
   replicas.
1. **Read:** send read request to `blockFor(CL)` replicas, take fastest response,
   optionally trigger read repair if digests differ.
1. If not enough replicas available to satisfy CL, return `Unavailable` error
   immediately (fail-fast, no timeout wait).

The node that receives the CQL connection acts as coordinator. Token-aware routing
is a client driver optimization, not a server concern.

**Backpressure:** Coordinator tracks in-flight requests per peer. When a peer's
data lane is saturated (at `MAX_STREAMS_PER_LANE`), coordinator returns
`Overloaded` to the CQL client (fail-fast) rather than queuing unboundedly.
Write/read timeout errors (`WriteTimeout`, `ReadTimeout`) always surface to the
client — the coordinator never silently downgrades CL.

### Token Ring

- `i64` token space (Murmur3, Cassandra-compatible)
- `BTreeMap<Token, Uuid>` for O(log n) lookup of owning node
- Virtual nodes: each node owns `FERROSA_NUM_TOKENS` ranges (default 256)
- `SimpleStrategy`: RF replicas starting at token position, walking ring clockwise
- `NetworkTopologyStrategy`: RF replicas per data center, skipping same-rack nodes

### Node Lifecycle

**Join (cluster mode):**

1. New node contacts seed via ferrosa-net (must pass handshake: PSK or mTLS)
1. If `FERROSA_AUTO_JOIN=false` (production default), Raft leader rejects join
   unless the node's `host_id` was pre-approved via
   `ferrosa-ctl add-node <host_id>`. In development mode (`FERROSA_AUTO_JOIN=true`),
   any authenticated node can join.
1. Seed's Raft leader assigns tokens (proposes `JoinNode`)
1. New node appears in token ring as "joining"
1. Bootstrap: stream from existing owners via bulk lane, or fetch from S3
1. Once complete, transition to "live" (Raft commit)

**Fast bootstrap from S3:**

- New node restores from S3 snapshot of SSTables + manifest before connecting
- On connect, reports manifest state (which SSTables, latest commit log position)
- Cluster sends only the delta (mutations since that point)

**Leave (graceful decommission):**

1. Operator sends decommission command via ferrosa-ctl
1. Raft leader proposes `LeaveNode`, reassigns tokens
1. Leaving node streams data to new owners
1. Once transferred, node removed from topology (Raft commit)

### Hinted Handoff

When a replica is unavailable during a write:

- Coordinator stores the mutation as a "hint" in `FERROSA_HINTED_HANDOFF_DIR`
- Capped at `FERROSA_HINTED_HANDOFF_MAX_MB` per peer (default 1 GB). When cap
  is reached, oldest hints are dropped and the peer is flagged for full repair
  on reconnection.
- Hints stored in FIFO order with the original mutation timestamp. Replayed in
  write-order using the original timestamp (not replay time) to preserve
  correct tombstone and TTL semantics.
- When the peer reconnects (via ferrosa-net `on_peer_connected`), hints are
  replayed via data lane
- Hints are deleted after successful replay

### Module Structure

```text
ferrosa-cluster/src/
├── lib.rs
├── config.rs               // ClusterConfig
├── state.rs                // ClusterState trait impl
├── consistency.rs          // ConsistencyLevel enum, blockFor()
├── token_ring.rs           // TokenRing, replica placement
├── coordinator/
│   ├── mod.rs
│   ├── write.rs            // Forward mutation, wait for CL ACKs
│   ├── read.rs             // Fan out reads, merge, read repair
│   └── batch.rs            // Batch coordination
├── raft/
│   ├── mod.rs
│   ├── state_machine.rs    // RaftState: schema + topology + tokens + config
│   ├── log_store.rs        // Raft log persistence
│   ├── network.rs          // openraft NetworkFactory via ferrosa-net
│   └── snapshot.rs         // Snapshot to/from S3
├── pair/
│   ├── mod.rs
│   ├── primary.rs          // Primary role: accept writes, replicate
│   ├── secondary.rs        // Secondary role: forward writes, serve reads
│   ├── catchup.rs          // Catch-up protocol (seq-based replay)
│   └── switchover.rs       // Operator-initiated role swap
├── replication.rs          // WriteObserver impl, async mutation forwarding
├── repair/
│   ├── mod.rs
│   ├── read_repair.rs      // Opportunistic read repair
│   └── hinted_handoff.rs   // Store hints, replay on reconnect
├── lifecycle/
│   ├── mod.rs
│   ├── join.rs             // Node join, token assignment, bootstrap
│   ├── leave.rs            // Decommission, token handoff
│   └── bootstrap.rs        // Stream from peers or S3
└── schema.rs               // DDL through Raft
```

### Dependencies

- `ferrosa-net` — RPC, peer management
- `ferrosa-common` — `Token`, `PartitionKey`, `DecoratedKey`
- `ferrosa-schema` — `Schema`, `ClusterState` trait, `NodeConfig`, `PeerInfo`,
  `ReplicationParams`
- `ferrosa-storage` — `StorageEngine`, `WriteObserver`
- `openraft` — Raft consensus
- `tokio` — async runtime
- `arc-swap` — lock-free token ring and cluster state reads
- `serde`, `serde_json` — Raft state machine serialization
- `tracing` — structured logging

---

## Part 3: Integration with Existing Crates

### ferrosa-cql changes (minimal)

- Router gains a `coordinate()` path: if the query's partition token maps to a
  remote node, forward via ferrosa-cluster coordinator instead of local storage
- `SharedState.cluster_state` switches from `SingleNodeClusterState` to
  ferrosa-cluster's implementation
- CL from QUERY frame parameters passed to coordinator

### ferrosa-schema changes

- DDL mutations route through Raft in cluster/pair mode instead of applying
  directly
- `Schema` gains `apply_raft_command()` — applies pre-validated DDL without
  re-checking auth (auth checked on proposing node)
- `ClusterState` trait may gain a `replication_lag()` method for the observability
  virtual table

### ferrosa-storage changes

- Core engine unchanged. ferrosa-cluster implements `WriteObserver` (async mode)
  to intercept mutations for replication forwarding.
- Commit log gains a method to replay from a given sequence number (needed for
  pair mode catch-up). Partially implemented in storage-replay worktree.

### ferrosa (binary) changes

- Startup reads `FERROSA_CLUSTER_MODE` and `--seed` args
- No seed → Standalone (current behavior, no ferrosa-net/cluster)
- Seed provided → contact seed, negotiate mode (Pair or Cluster)
- Constructs appropriate cluster implementation and injects into `SharedState`

---

## Part 4: Configuration

### ferrosa-net environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FERROSA_SEED` | (none) | Comma-separated seed addresses |
| `FERROSA_INTERNODE_BIND` | `0.0.0.0:7000` | Internode listen address |
| `FERROSA_INTERNODE_BROADCAST` | (same as bind) | Address advertised to peers |
| `FERROSA_INTERNODE_TLS_CERT` | (none) | TLS certificate path |
| `FERROSA_INTERNODE_TLS_KEY` | (none) | TLS private key path |
| `FERROSA_INTERNODE_TLS_CA` | (none) | CA cert for mTLS peer verification |
| `FERROSA_DNS_DISCOVERY` | (none) | DNS name for peer discovery |
| `FERROSA_HEARTBEAT_INTERVAL_MS` | `500` | Heartbeat ping interval |
| `FERROSA_HEARTBEAT_TIMEOUT_MS` | `1500` | Peer suspected-dead threshold |
| `FERROSA_INTERNODE_PSK` | (none) | Pre-shared key for handshake auth (Phase 1) |
| `FERROSA_MAX_INTERNODE_CONNECTIONS` | `100` | Max inbound internode connections |
| `FERROSA_HANDSHAKE_TIMEOUT_SECS` | `5` | Close connections that don't complete handshake |
| `FERROSA_MAX_FRAME_BODY_SIZE` | `268435456` | Max internode frame body (256 MiB) |
| `FERROSA_MAX_STREAMS_PER_LANE` | `128` | Max concurrent streams per connection lane |

### ferrosa-cluster environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FERROSA_CLUSTER_MODE` | (auto) | Force mode: `standalone`, `pair`, `cluster` |
| `FERROSA_CLUSTER_NAME` | `ferrosa` | Cluster name (must match across nodes) |
| `FERROSA_DATA_CENTER` | `dc1` | Node's data center |
| `FERROSA_RACK` | `rack1` | Node's rack |
| `FERROSA_NUM_TOKENS` | `256` | Virtual nodes per node |
| `FERROSA_DEFAULT_CL` | `QUORUM` | Default consistency level |
| `FERROSA_RAFT_ELECTION_TIMEOUT_MS` | `1000` | Raft election timeout |
| `FERROSA_RAFT_SNAPSHOT_INTERVAL` | `10000` | Log entries between snapshots |
| `FERROSA_HINTED_HANDOFF_DIR` | `{data_dir}/hints` | Hint storage directory |
| `FERROSA_HINTED_HANDOFF_MAX_MB` | `1024` | Max hint storage per peer |
| `FERROSA_AUTO_JOIN` | `false` | Allow unapproved nodes to join (true for dev) |

### CLI args

```text
ferrosa --seed 10.0.1.5:7000 [--seed 10.0.1.6:7000]
```

`--seed` is repeatable. CLI takes precedence over `FERROSA_SEED` env var.

### Production mode (ADR-010)

When `FERROSA_MODE=production`, startup requires `FERROSA_INTERNODE_TLS_CERT` and
`FERROSA_INTERNODE_TLS_KEY`. Fails closed otherwise.

---

## Part 5: Error Handling

### ferrosa-net errors

| Error | Behavior |
|-------|----------|
| `ConnectionRefused` | Queued for retry with exponential backoff |
| `HandshakeFailed` | Cluster name mismatch, version incompatible, or TLS rejected |
| `Timeout` | Per-lane timeout (raft=1s, data=10s, bulk=60s) |
| `PeerSuspected` | Heartbeat timeout, event emitted to ferrosa-cluster |
| `ProtocolError` | Corrupt frame, unknown type → close connection |

### ferrosa-cluster errors (surfaced to CQL client)

| Error | Meaning |
|-------|---------|
| `Unavailable { cl, required, alive }` | Not enough replicas to satisfy CL (fail-fast) |
| `WriteTimeout { cl, received, required }` | Replicas alive but not enough ACKed in time |
| `ReadTimeout { cl, received, required, data_present }` | Same for reads |
| `ReadFailure` / `WriteFailure` | Replica returned an error |
| `PairWriteUnavailable` | Pair mode, primary down, operator must promote |

These map to CQL protocol error codes — existing drivers handle them with retry
policies and fallback.

### Raft failures

- **Leader election timeout:** DDL and topology changes temporarily unavailable.
  Data reads/writes continue if replicas are reachable.
- **Log compaction behind:** Snapshot shipped to lagging node via S3.
- **Voter lost (below quorum):** DDL blocked until quorum restored. Data
  operations continue at achievable CL.

---

## Part 6: Testing

### Unit and Integration Tests

**ferrosa-net:**

- Codec encode/decode round-trips, corrupt frames, compression
- Handshake version negotiation, cluster name mismatch rejection
- Integration tests with loopback connections (two tokio tasks as peers)
- Priority lane: verify raft lane not blocked by bulk transfers
- TLS tests with self-signed certs
- Failure detection: suspected-dead after missed heartbeats

**ferrosa-cluster:**

- Token ring: replica placement for SimpleStrategy and NetworkTopologyStrategy
- Consistency level: `blockFor()` across RF and DC configurations
- Raft state machine: apply commands, verify state, snapshot/restore round-trip
- Coordinator with mock ferrosa-net: write/read fan-out, CL enforcement, timeouts
- Pair mode: write forwarding, switchover, catch-up protocol
- Mode transition: standalone → pair → cluster progression
- Integration: 3-node on localhost, DDL through Raft, QUORUM write, node join/leave

**Property tests (proptest):**

- Token ring: any token maps to exactly RF replicas on distinct nodes
- Consistency level: `blockFor` never exceeds RF, quorum > RF/2
- Raft state machine: apply + snapshot + restore = same state

### Docker-Based End-to-End Testing

**Containers:**

- `rustfs` — S3-compatible object storage
  ([rustfs](https://github.com/rustfs/rustfs)). One instance shared by all nodes.
- `ferrosa-node-{1,2,3,...}` — Ferrosa instances, each with own local volume.
- `docker-compose.yml` in `tests/cluster/` orchestrating everything.

**Compose profiles:**

| Profile | Containers | Tests |
|---------|-----------|-------|
| `standalone` | 1 ferrosa + rustfs | Single-node with S3 backend |
| `pair` | 2 ferrosa + rustfs | Pair formation, write forwarding, read from secondary, switchover, failover |
| `cluster` | 3 ferrosa + rustfs | Raft formation, DDL replication, QUORUM writes, node join/leave |
| `progression` | 1→2→3 ferrosa + rustfs | Standalone → Pair → Cluster growth |

**Test scripts in `tests/cluster/scripts/`:**

- `test-switchover.sh` — trigger switchover via ferrosa-ctl, verify writes on new
  primary
- `test-failover.sh` — kill primary container, verify reads on secondary, operator
  promote, verify writes resume
- `test-partition.sh` — `docker network disconnect` to simulate network split,
  verify no split brain

**Dockerfile:**

- Multi-stage build: compile ferrosa, copy into minimal runtime image
- Configurable via `FERROSA_*` env vars
- `--seed` passed as container command arg

**Firecracker / fly.io portability:**

- Same container image works on Firecracker VMs
- Swap rustfs for fly.io Tigris object storage via `FERROSA_S3_ENDPOINT` and
  `FERROSA_S3_BUCKET`
- `object_store` crate (already in ferrosa-storage) supports any S3-compatible
  endpoint

---

## Phasing

This design will be implemented across multiple sub-projects:

1. **ferrosa-net Phase 1:** Codec (with frame size limits), handshake (with PSK
   auth), connection pool (with stream limits), RPC server/client (with
   connection limits), failure detection. No TLS yet (development mode).
1. **ferrosa-cluster Phase 1 (Pair mode):** Primary/secondary roles, write
   forwarding, synchronous replication, catch-up, switchover. No Raft.
1. **ferrosa-net Phase 2:** TLS/mTLS, DNS discovery, compression.
1. **ferrosa-cluster Phase 2 (Cluster mode):** Raft via openraft, token ring,
   consistency levels, coordinator pattern.
1. **ferrosa-cluster Phase 3:** Hinted handoff, read repair, node lifecycle
   (join/leave/bootstrap).
1. **Docker testing infrastructure:** Dockerfile, compose profiles, test scripts.
1. **Production hardening:** Production mode validation, monitoring, operational
   tooling.

## Open Questions

- [ ] QUIC transport: evaluate `quinn` crate for Phase 2+ (behind feature flag)
- [ ] Anti-entropy repair: Merkle tree comparison for periodic background validation
- [ ] Accord protocol: distributed transactions (deferred per ADR-003)
- [ ] HLC / TrueTime-like: cross-DC clock synchronization for conflict resolution
