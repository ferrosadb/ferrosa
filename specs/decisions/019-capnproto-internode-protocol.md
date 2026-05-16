# ADR-019: CapnProto Internode Protocol Envelope

> Date: 2026-05-14
> Status: Proposed
> Scope: `ferrosa-net` internode wire contract and the typed schemas that
> will replace ad-hoc frame bodies over time.
> Non-goal: This ADR does not implement `.capnp` schemas, code generation,
> handler rewrites, or dependency changes.

## Context

`ferrosa-net` currently uses a custom 44-byte frame header and a hand-written
`Message` enum:

- `ferrosa-net/src/codec.rs` defines `FrameHeader` as
  `version:u8 | flags:u8 | lane:u8 | msg_type:u8 | stream_id:u32 |
  length:u32 | trace_context:32 bytes`.
- `ferrosa-net/src/message.rs` mixes a few typed bodies (`Handshake`,
  `HandshakeAck`, `Ping`, `Pong`, `ClusterInvite`, `PairCatchUp`,
  `RoleSwap`, `BootstrapComplete`) with many opaque byte payloads
  interpreted by `ferrosa-cluster`.
- `ferrosa-net/src/handshake.rs` accepts exactly protocol version `1` and
  negotiates only compression. It already has a backwards-compatible trailing
  optional `cql_broadcast` field, but there is no general feature negotiation
  or schema-version negotiation.
- Request/response correlation is the frame `stream_id:u32`; fire-and-forget
  is a header flag. There is no protocol-level error frame: missing handlers
  return `None`, callers time out, and error semantics are local to each
  payload family.
- Cluster recovery and formation now depend on more message families:
  cluster invite/rejoin, membership forwarding, Raft recovery traffic,
  bootstrap stream control, row/SSTable streaming, data read/write fan-out,
  pair catch-up, batchlog replay, index coordination, and Accord.

CapnProto is a good fit for the next wire generation because capnproto-rust
provides `capnp` runtime support plus `capnpc` build integration, generated
`Reader<'a>` / `Builder<'a>` APIs, tagged unions, and documented protocol
versioning rules. The upstream compatibility rule that matters most here:
new fields, enumerants, methods, and types may be added compatibly when their
ordinals are greater than all previous members; symbolic names can change if
ordinals/type IDs stay stable.

## Decision

Introduce a CapnProto message body contract under the existing `ferrosa-net`
connection, using the current TCP/TLS connection manager and priority lanes.
The migration will be envelope-first:

1. Keep the existing fixed frame long enough to negotiate support and route
   legacy peers.
2. Add a CapnProto `Envelope` as the body for a new message type family.
3. Move existing message families into typed CapnProto payload structs behind
   the envelope union.
4. Retire opaque `Bytes` payloads only after mixed-version tests prove both
   directions of rolling upgrade.

This ADR chooses a schema-first plan, not code. Implementation should add
`.capnp` files and codegen in a later card.

## Goals

- Stable envelope with explicit protocol version, feature flags, sender and
  recipient identity, correlation IDs, deadline and tracing metadata.
- Typed error frames instead of timeout-only failures for known failures.
- Typed cluster invite, rejoin, recovery, and bootstrap stream/control frames.
- Backwards-compatible migration from the current `MsgType`/opaque-bytes
  protocol.
- CapnProto evolvability discipline: never reuse ordinals; append-only fields
  and enum values; unknown fields preserved by readers where possible.
- Bounded decoding and streaming: no whole-cluster, whole-table, or whole-file
  materialization before paging/iteration.

## Non-goals

- Do not switch to CapnProto RPC/object-capability transport in this slice.
  Ferrosa will use CapnProto for message bodies first.
- Do not remove the existing `RpcClient`, `PeerManager`, `PriorityPool`, TLS,
  lanes, or handler registry in the first implementation slice.
- Do not make Cassandra internode wire compatibility a goal. Ferrosa's client
  CQL wire compatibility is separate from its own internode protocol.

## Compatibility model

### Protocol versions

Use three version numbers with distinct meanings:

| Field | Width | Meaning |
|---|---:|---|
| `transportVersion` | `UInt16` | Frame/envelope contract. Major incompatible changes bump this. |
| `minSupportedTransportVersion` | `UInt16` | Lowest peer transport this node can speak. |
| `schemaVersion` | `UInt16` | CapnProto schema generation for this payload family. Additive changes normally keep this stable. |

Version negotiation rules:

1. A peer advertises `transportVersion`, `minSupportedTransportVersion`, and
   a feature bitset in `hello`.
2. The receiver chooses the highest mutually supported transport version:
   `min(local.current, remote.current)` if it is at least both peers' minimums.
3. If there is no overlap, reply with `helloReject` / `error` carrying
   `UNSUPPORTED_VERSION`, local min/current, and a safe reason string.
4. Schema-family feature bits gate any non-additive or behavior-changing
   field. Additive CapnProto fields with defaults do not require a transport
   version bump, but still need tests with older readers.

### Feature flags

Feature flags are stable `UInt64` bit positions. Bits are never reused.
The first word is enough for the initial migration; add `featureWords` later
only by appending a field.

Initial feature allocation:

| Bit | Name | Meaning |
|---:|---|---|
| 0 | `CAPNP_ENVELOPE` | Peer can receive `Envelope` bodies. |
| 1 | `CAPNP_TYPED_ERRORS` | Peer understands `ErrorFrame`. |
| 2 | `CAPNP_CLUSTER_INVITE_V2` | Peer understands typed invite/rejoin payloads. |
| 3 | `CAPNP_BOOTSTRAP_CONTROL` | Peer understands bootstrap stream control messages. |
| 4 | `CAPNP_SSTABLE_STREAM_V2` | Peer understands chunked SSTable descriptors/chunks/acks. |
| 5 | `CAPNP_MEMBERSHIP_OP_V2` | Peer understands typed membership forwarding. |
| 6 | `CAPNP_ACCORD_V2` | Peer understands typed Accord message wrappers. |
| 7 | `CAPNP_BATCHLOG_V2` | Peer understands typed batchlog write/delete/replay. |

Unknown feature bits must be ignored unless the payload's `requiredFeatures`
contains a bit the receiver lacks; in that case the receiver returns
`UNSUPPORTED_FEATURE`.

## Envelope contract

Every CapnProto message body starts with `Envelope`. It carries routing and
compatibility metadata; the payload union carries the family-specific body.

```capnp
@0xf36f6f73615f0190;

struct Envelope {
  magic @0 :UInt32;                  # ASCII-ish FER1 marker, guards wrong body.
  transportVersion @1 :UInt16;
  minSupportedTransportVersion @2 :UInt16;
  schemaVersion @3 :UInt16;
  messageFamily @4 :MessageFamily;
  messageKind @5 :UInt16;            # Family-local kind, append-only.
  flags @6 :UInt32;                  # request/response/error/stream bits.
  requiredFeatures @7 :UInt64;
  optionalFeatures @8 :UInt64;

  sender @9 :NodeIdentity;
  recipient @10 :NodeIdentity;       # empty UUID allowed for broadcast/invite.
  clusterId @11 :Data;               # 16-byte UUID or future cluster ID.
  epoch @12 :UInt64;                 # formation/membership epoch, 0 if N/A.

  correlationId @13 :Data;           # 16 bytes, unique per request chain.
  causationId @14 :Data;             # optional parent correlation ID.
  streamId @15 :UInt64;              # replaces u32 stream_id once negotiated.
  sequence @16 :UInt64;              # chunk/control sequence within stream.
  deadlineUnixNanos @17 :UInt64;     # 0 means no explicit deadline.

  traceId @18 :Data;                 # 16 bytes, mirrors existing trace field.
  spanId @19 :Data;                  # 8 bytes.
  traceFlags @20 :UInt64;

  payload :union {
    hello @21 :Hello;
    helloAck @22 :HelloAck;
    error @23 :ErrorFrame;
    health @24 :HealthFrame;
    cluster @25 :ClusterControl;
    recovery @26 :RecoveryControl;
    bootstrap @27 :BootstrapControl;
    stream @28 :StreamFrame;
    raft @29 :RaftFrame;
    membership @30 :MembershipFrame;
    data @31 :DataFrame;
    pair @32 :PairFrame;
    batchlog @33 :BatchlogFrame;
    index @34 :IndexFrame;
    accord @35 :AccordFrame;
  }
}

enum MessageFamily {
  lifecycle @0;
  clusterControl @1;
  recovery @2;
  bootstrap @3;
  stream @4;
  raft @5;
  membership @6;
  data @7;
  pair @8;
  batchlog @9;
  index @10;
  accord @11;
}
```

### Envelope flags

`flags` is a stable bitset:

| Bit | Name | Meaning |
|---:|---|---|
| 0 | `REQUEST` | Expects a response unless `FIRE_AND_FORGET` is set. |
| 1 | `RESPONSE` | Correlates to a prior request. |
| 2 | `ERROR` | Payload must be `ErrorFrame`. |
| 3 | `FIRE_AND_FORGET` | No response slot should be registered. |
| 4 | `STREAM_START` | First frame for a stream. |
| 5 | `STREAM_CHUNK` | Carries stream data. |
| 6 | `STREAM_END` | Last frame for a stream. |
| 7 | `RETRYABLE` | Sender may retry idempotently. |
| 8 | `COMPRESSED` | Payload/chunk uses negotiated compression. |

### Node identity

```capnp
struct NodeIdentity {
  hostId @0 :Data;             # 16-byte UUID; required after hello.
  nodeId @1 :UInt64;           # `uuid_to_node_id(hostId)` when known.
  address @2 :Text;            # internode address advertised by sender.
  cqlBroadcast @3 :Text;       # optional; empty string means absent.
  dataCenter @4 :Text;
  rack @5 :Text;
  certificateFingerprint @6 :Data; # optional mTLS identity binding.
}
```

Identity rules:

- `hostId` is the durable Ferrosa node identity and must match the authenticated
  TLS/PSK identity once those features are enabled.
- `nodeId` is only a derived routing hint; receivers recompute it from
  `hostId` for consensus-critical paths.
- `address` is never authorization. It is routing metadata and can be stale.
- `certificateFingerprint` is optional during PSK/dev mode but required by the
  future production-mode mTLS gate.

### Correlation IDs

- `correlationId` is a 128-bit opaque ID generated by the request originator.
- All response, error, retry, and stream-control frames for that logical
  operation use the same `correlationId`.
- `streamId` is a per-connection numeric convenience, not the durable identity.
  It may be remapped on reconnect; `correlationId` survives reconnect/retry.
- `causationId` links child messages to a parent request, e.g. bootstrap control
  causing multiple SSTable stream subflows.

## Lifecycle and negotiation frames

```capnp
struct Hello {
  clusterName @0 :Text;
  clusterId @1 :Data;
  transportVersion @2 :UInt16;
  minSupportedTransportVersion @3 :UInt16;
  featureBits @4 :UInt64;
  supportedCompression @5 :List(Compression);
  authToken @6 :Data;       # existing PSK HMAC token; redact in logs.
  nonce @7 :UInt64;
  node @8 :NodeIdentity;
}

struct HelloAck {
  accepted @0 :Bool;
  chosenTransportVersion @1 :UInt16;
  chosenFeatureBits @2 :UInt64;
  chosenCompression @3 :Compression;
  node @4 :NodeIdentity;
  reason @5 :Text;
}

enum Compression { none @0; lz4 @1; snappy @2; zstd @3; }

struct HealthFrame {
  pingNonce @0 :UInt64;
  sentAtUnixNanos @1 :UInt64;
  pingRecvAtUnixNanos @2 :UInt64;
  nodeState @3 :NodeLifecycleState;
}
```

Mapping from existing messages:

| Existing message | CapnProto payload |
|---|---|
| `Handshake` | `Envelope.hello` |
| `HandshakeAck` | `Envelope.helloAck` |
| `Ping` / `Pong` | `Envelope.health` with request/response flags |

## Error frames

```capnp
struct ErrorFrame {
  code @0 :ErrorCode;
  retryable @1 :Bool;
  safeMessage @2 :Text;
  detailCode @3 :Text;       # stable machine-readable string.
  failedFamily @4 :MessageFamily;
  failedKind @5 :UInt16;
  failedCorrelationId @6 :Data;
  leaderHint @7 :NodeIdentity;
  minSupportedTransportVersion @8 :UInt16;
  maxSupportedTransportVersion @9 :UInt16;
  missingFeatures @10 :UInt64;
}

enum ErrorCode {
  ok @0;
  malformedFrame @1;
  unsupportedVersion @2;
  unsupportedFeature @3;
  unauthenticated @4;
  unauthorized @5;
  clusterMismatch @6;
  unknownMessage @7;
  noHandler @8;
  timeout @9;
  overloaded @10;
  notLeader @11;
  staleEpoch @12;
  conflict @13;
  retryRequired @14;
  fullBootstrapRequired @15;
  internal @16;
}
```

Error-frame rules:

- Known rejections must return `ErrorFrame` instead of silently dropping the
  request. `noHandler` replaces the current warn-and-timeout path when both
  peers support `CAPNP_TYPED_ERRORS`.
- `safeMessage` must be log-safe. Secrets, auth tokens, and credentials are
  never included; redact as `[REDACTED]` if the source text may contain them.
- `leaderHint` is populated for membership/Raft forwarding failures when known.
- `retryable=true` requires an idempotence contract in the payload family.

## Cluster invite, rejoin, and recovery frames

```capnp
struct ClusterControl {
  op :union {
    invite @0 :ClusterInvite;
    inviteAck @1 :ClusterInviteAck;
    rejoinRequest @2 :RejoinRequest;
    rejoinPlan @3 :RejoinPlan;
    formationEpochBump @4 :FormationEpochBump;
  }
}

struct ClusterInvite {
  initiator @0 :NodeIdentity;
  peers @1 :List(NodeIdentity);
  formationEpoch @2 :UInt64;
  expiresAtUnixNanos @3 :UInt64;
  inviteId @4 :Data;          # 16-byte UUID; dedupe/replay guard.
}

struct ClusterInviteAck {
  host @0 :NodeIdentity;
  inviteId @1 :Data;
  accepted @2 :Bool;
  reason @3 :Text;
}

struct RejoinRequest {
  host @0 :NodeIdentity;
  lastKnownMembershipEpoch @1 :UInt64;
  lastAppliedRaftIndex @2 :UInt64;
  localGeneration @3 :UInt64;
  wantsBootstrapPlan @4 :Bool;
}

struct RejoinPlan {
  membershipEpoch @0 :UInt64;
  state @1 :NodeLifecycleState;
  requiredBootstrap @2 :Bool;
  bootstrapPlanId @3 :Data;
  peers @4 :List(NodeIdentity);
  reason @5 :Text;
}

struct FormationEpochBump {
  previous @0 :UInt64;
  next @1 :UInt64;
  reason @2 :Text;
}

enum NodeLifecycleState {
  unknown @0;
  joining @1;
  normal @2;
  leaving @3;
  left @4;
  degraded @5;
}
```

Requirements:

- `ClusterInvite` replaces the current `initiator:Uuid + peers:Vec<(Uuid,
  SocketAddr)>` body with identity, epoch, expiry, and dedupe ID.
- Receivers reject invites with stale `formationEpoch`, expired `expiresAt`, or
  a mismatched cluster identity.
- Rejoin requests are explicit; reconnecting peers no longer rely solely on
  opportunistic `ClusterInvite` replay to escape pair mode.
- Invite/rejoin must remain on the Data lane until Raft handlers are guaranteed
  present on all peers.

## Recovery control frames

Recovery frames are separate from ordinary cluster invite/rejoin so a node can
recover after it was already part of the cluster but lost local state, lost Raft
logs, or needs a bootstrap plan after stale generation detection.

```capnp
struct RecoveryControl {
  op :union {
    request @0 :RecoveryRequest;
    plan @1 :RecoveryPlan;
    progress @2 :RecoveryProgress;
    complete @3 :RecoveryComplete;
  }
}

struct RecoveryRequest {
  host @0 :NodeIdentity;
  reason @1 :RecoveryReason;
  lastMembershipEpoch @2 :UInt64;
  lastAppliedRaftIndex @3 :UInt64;
  lastDurableCommitLogSegment @4 :UInt64;
  localGeneration @5 :UInt64;
}

struct RecoveryPlan {
  planId @0 :Data;
  membershipEpoch @1 :UInt64;
  action @2 :RecoveryAction;
  bootstrapPlanId @3 :Data;
  leader @4 :NodeIdentity;
  safeMessage @5 :Text;
}

struct RecoveryProgress {
  planId @0 :Data;
  phase @1 :Text;
  completedUnits @2 :UInt64;
  totalUnits @3 :UInt64;
}

struct RecoveryComplete {
  planId @0 :Data;
  host @1 :NodeIdentity;
  finalMembershipEpoch @2 :UInt64;
  finalAppliedRaftIndex @3 :UInt64;
}

enum RecoveryReason {
  unknown @0;
  peerReconnected @1;
  staleGeneration @2;
  lostLocalState @3;
  raftLogGap @4;
  streamRetry @5;
}

enum RecoveryAction {
  noOp @0;
  refreshMetadata @1;
  replayRaft @2;
  runBootstrap @3;
  fullBootstrapRequired @4;
}
```

Recovery requirements:

- A node with stale generation or Raft log gaps receives an explicit
  `RecoveryPlan` instead of relying on timeouts or repeated `ClusterInvite`.
- `planId` is reused by any required bootstrap stream/control frames so progress
  and errors stay correlated.
- Recovery messages must be idempotent: a duplicate `RecoveryRequest` for the
  same host/generation returns the same active plan or a completed result.

## Bootstrap stream and control frames

```capnp
struct BootstrapControl {
  op :union {
    begin @0 :BootstrapBegin;
    tablePlan @1 :BootstrapTablePlan;
    ready @2 :BootstrapReady;
    complete @3 :BootstrapComplete;
    abort @4 :BootstrapAbort;
    ack @5 :BootstrapAck;
  }
}

struct BootstrapBegin {
  planId @0 :Data;
  membershipEpoch @1 :UInt64;
  source @2 :NodeIdentity;
  target @3 :NodeIdentity;
  totalTables @4 :UInt32;
}

struct BootstrapTablePlan {
  planId @0 :Data;
  keyspace @1 :Text;
  table @2 :Text;
  tokenRanges @3 :List(TokenRange);
  transferMode @4 :TransferMode;
  rowFallbackLimit @5 :UInt32;
  expectedBytes @6 :UInt64;
}

struct BootstrapReady {
  planId @0 :Data;
  node @1 :NodeIdentity;
  acceptedTables @2 :UInt32;
}

struct BootstrapComplete {
  planId @0 :Data;
  node @1 :NodeIdentity;
  streamedBytes @2 :UInt64;
  completedTables @3 :UInt32;
}

struct BootstrapAbort {
  planId @0 :Data;
  code @1 :ErrorCode;
  reason @2 :Text;
  retryable @3 :Bool;
}

struct BootstrapAck {
  planId @0 :Data;
  sequence @1 :UInt64;
  receivedBytes @2 :UInt64;
}

struct TokenRange { start @0 :Int64; end @1 :Int64; }
enum TransferMode { sstableBulk @0; boundedRows @1; retryRequired @2; }
```

This mirrors the current bootstrap planner contract:

- SSTable-backed tables use `sstableBulk` first.
- Row fallback is only allowed for bounded small-table fallback and carries the
  explicit `rowFallbackLimit`.
- Failed SSTable bulk transfer returns `retryRequired` / `BootstrapAbort` and
  must not fall back to unbounded row materialization.

## Data streaming frames

```capnp
struct StreamFrame {
  op :union {
    start @0 :StreamStart;
    chunk @1 :StreamChunk;
    ack @2 :StreamAck;
    end @3 :StreamEnd;
  }
}

struct StreamStart {
  streamKind @0 :StreamKind;
  streamId @1 :Data;          # 16-byte stream UUID.
  planId @2 :Data;
  keyspace @3 :Text;
  table @4 :Text;
  totalBytes @5 :UInt64;
  chunkSize @6 :UInt32;
  checksum @7 :Data;
}

struct StreamChunk {
  streamId @0 :Data;
  sequence @1 :UInt64;
  offset @2 :UInt64;
  data @3 :Data;
  checksum @4 :Data;
}

struct StreamAck {
  streamId @0 :Data;
  highestContiguousSequence @1 :UInt64;
  receivedBytes @2 :UInt64;
  windowCreditBytes @3 :UInt64;
}

struct StreamEnd {
  streamId @0 :Data;
  finalSequence @1 :UInt64;
  totalBytes @2 :UInt64;
  checksum @3 :Data;
}

enum StreamKind { rowMutation @0; sstableFile @1; raftSnapshot @2; hintReplay @3; }
```

Backpressure requirement: a sender may not exceed advertised
`windowCreditBytes`, and receivers must write chunks through bounded sinks.
No implementation may buffer a full SSTable or full bootstrap table in memory
before forwarding chunks.

## Raft and membership frames

```capnp
struct RaftFrame {
  op :union {
    appendEntries @0 :Data;
    appendResponse @1 :Data;
    vote @2 :Data;
    voteResponse @3 :Data;
    installSnapshot @4 :Data;
  }
  raftGroupId @5 :Data;
  term @6 :UInt64;
}

struct MembershipFrame {
  op :union {
    addVoter @0 :MembershipAddVoter;
    removeVoter @1 :MembershipRemoveVoter;
    updateMetadata @2 :MembershipUpdateMetadata;
    promoteLearner @3 :MembershipPromoteLearner;
    approveNode @4 :MembershipApproveNode;
    result @5 :MembershipResult;
  }
  operationId @6 :Data;
  membershipEpoch @7 :UInt64;
}
```

Initial Raft payloads may remain opaque `Data` because openraft serialization is
owned by `ferrosa-cluster`; the envelope still adds group ID, term, identity,
correlation, and typed errors. Membership operations should be typed early
because ADR-013 requires whole-operation forwarding, idempotence, and leader
hints.

## Data, pair, batchlog, index, and Accord frames

These families may migrate after lifecycle/bootstrap/membership:

- `DataFrame`: mutation forward/ack, read/range read request/response, repair
  write, truncate, index scatter-gather. Include consistency level, table ID,
  token range, original client timestamp, and idempotence key where available.
- `PairFrame`: write forward/ack, catch-up request/response, role swap,
  schema sync, DDL forward/ack, batch forward/ack. Pair catch-up must carry
  validated segment/offset and support `fullBootstrapRequired` errors.
- `BatchlogFrame`: write/delete/replay with original mutation timestamps and
  replay ordering metadata.
- `IndexFrame`: build request/complete and secondary-index read request/response
  with index ID/build ID.
- `AccordFrame`: pre-accept/accept/commit/read/apply/recover wrappers with
  `TxnId`, HLC, coordinator, ballot/epoch where available; opaque protocol
  payload is allowed until Accord structs are stable.

## Mapping from current `MsgType`

| Current range | Existing family | Envelope family |
|---|---|---|
| `0x01..0x06` | lifecycle + cluster invite | `lifecycle`, `clusterControl` |
| `0x10..0x14` | Raft | `raft` |
| `0x20..0x28` | data read/write/repair/truncate | `data` |
| `0x30..0x35` | row/SSTable streaming | `stream`, `bootstrap` |
| `0x40..0x49` | pair mode | `pair` |
| `0x50..0x52` | batchlog | `batchlog` |
| `0x60..0x63` | index coordination/read | `index` |
| `0x70..0x7a` | Accord | `accord` |
| `0x80..0x83` | bootstrap complete + membership forward | `bootstrap`, `membership` |

Reserve the existing numeric `MsgType` table for legacy routing during rolling
upgrade. New CapnProto bodies should use a small set of new legacy-discriminant
escape hatches, for example `CapnpEnvelopeControl`, `CapnpEnvelopeData`, and
`CapnpEnvelopeBulk`, so old peers reject them cleanly as unknown instead of
mis-decoding a body as an older message.

## Migration strategy

### Phase 0 — schema and generated-code plumbing only

- Add `ferrosa-net/schemas/internode.capnp` and `build.rs` with `capnpc`.
- Add generated-code module boundaries but do not route production traffic.
- Add golden decode/encode tests for `Envelope`, `Hello`, `ErrorFrame`,
  `ClusterInvite`, and `BootstrapControl`.

### Phase 1 — dual handshake

- Extend current `Handshake` with optional trailing advertised feature bits and
  min/current transport version, preserving old decoder compatibility.
- If both peers advertise `CAPNP_ENVELOPE`, they may send CapnProto envelope
  bodies after `HelloAck`; otherwise use current `Message` encoding.
- Keep existing PSK/mTLS checks before accepting feature negotiation.

### Phase 2 — typed errors and lifecycle/control

- Route missing-handler, unsupported-version, unsupported-feature, stale-epoch,
  not-leader, and overload failures through `ErrorFrame` when negotiated.
- Migrate Ping/Pong and ClusterInvite/Ack first because they are small and
  heavily exercised in formation/rejoin tests.

### Phase 3 — bootstrap control and streaming

- Migrate `BootstrapComplete`, bootstrap plan/control, and SSTable stream
  start/chunk/ack/end.
- Prove bounded-memory behavior with large synthetic stream tests and windowed
  chunk acknowledgements.

### Phase 4 — membership forwarding and recovery

- Migrate `ClusterMembershipForward/Ack` from opaque bytes to typed
  `MembershipFrame` and typed result/error.
- Add rejoin request/plan and stale-epoch handling.

### Phase 5 — remaining opaque families

- Migrate data, pair, batchlog, index, Accord, and Raft wrappers in that order.
- Preserve opaque subpayloads for unstable internals until their structs settle.

### Phase 6 — retire legacy

- After one release where every supported rolling-upgrade pair negotiates
  `CAPNP_ENVELOPE`, flip the default to CapnProto and keep legacy behind a
  temporary compatibility flag.
- Remove the hand-written legacy bodies only after a later release explicitly
  drops mixed-version support.

## TDD and verification plan

No production implementation belongs in this ADR card. The implementation card
must follow these RED/GREEN checks:

1. **Schema golden tests**
   - RED: add tests that construct canonical `Envelope`/`Hello`/`ErrorFrame`
     bytes from fixtures and fail because the schema module does not exist.
   - GREEN: add `.capnp` schema and `capnpc` plumbing only.

2. **Negotiation compatibility**
   - RED: old-format `Handshake` without feature trailing fields still decodes;
     new-format handshake advertises min/current versions and features; peers
     with no overlapping version return `UNSUPPORTED_VERSION`.
   - GREEN: implement optional trailing decode and negotiation helper.

3. **Typed error instead of timeout**
   - RED: when `CAPNP_TYPED_ERRORS` is negotiated, dispatching an unregistered
     message returns `ErrorFrame{code=noHandler}` with the original
     `correlationId`; current behavior times out.
   - GREEN: add error-frame response path.

4. **Cluster invite v2**
   - RED: invite with stale `formationEpoch` or expired `expiresAt` returns
     `staleEpoch`/`retryRequired` and does not trigger transition.
   - GREEN: typed invite handler.

5. **Bootstrap stream bounds**
   - RED: a synthetic multi-GB declared SSTable stream sends chunks through a
     bounded window and never accumulates full payload bytes in memory.
   - GREEN: stream start/chunk/ack/end implementation.

6. **Mixed-version rolling upgrade**
   - RED/GREEN matrix:
     - old initiator -> new acceptor uses legacy path;
     - new initiator -> old acceptor falls back to legacy;
     - new -> new negotiates CapnProto;
     - unsupported future version receives typed or legacy safe rejection.

7. **Fuzz/property tests**
   - malformed envelope never panics;
   - unknown enum values/features return explicit errors or are ignored per
     rule;
   - every request response/error preserves `correlationId`;
   - large declared lengths are rejected before allocation.

8. **Security/logging tests**
   - auth tokens, PSKs, TLS material, and credential-looking payloads are never
     printed in `safeMessage`; tests assert redaction as `[REDACTED]`.

Suggested commands for implementation cards:

```bash
cargo test -p ferrosa-net envelope -- --nocapture
cargo test -p ferrosa-net handshake -- --nocapture
cargo test -p ferrosa-net cluster_invite -- --nocapture
cargo test -p ferrosa-cluster bootstrap_stream -- --nocapture
cargo fmt --all -- --check
cargo clippy -p ferrosa-net --lib --tests -- -D warnings
cargo clippy -p ferrosa-cluster --lib --tests -- -D warnings
```

## Acceptance criteria for this ADR

- [x] Defines envelope fields.
- [x] Defines protocol version negotiation.
- [x] Defines feature flags.
- [x] Defines node identity.
- [x] Defines correlation/request IDs.
- [x] Defines error frames.
- [x] Defines cluster invite/rejoin/recovery messages.
- [x] Defines bootstrap stream/control messages.
- [x] Defines backwards-compatible migration strategy.
- [x] Includes a TDD test plan for implementation cards.

## References

- `ferrosa-net/src/codec.rs`
- `ferrosa-net/src/message.rs`
- `ferrosa-net/src/handshake.rs`
- `ferrosa-net/src/rpc/handler.rs`
- `ferrosa-net/src/rpc/client.rs`
- `ferrosa-cluster/src/controller/cluster.rs`
- `ferrosa-cluster/src/controller/bootstrap/bootstrap_stream.rs`
- `specs/cluster-formation-architecture.md`
- `specs/threat-model-net-cluster.md`
- `specs/decisions/013-membership-change-protocol.md`
- `specs/decisions/015-multi-dc-raft-per-dc-accord.md`
- capnproto-rust: https://github.com/capnproto/capnproto-rust
- CapnProto language evolution rules: https://capnproto.org/language.html#evolving-your-protocol
