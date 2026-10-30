@0xf36f6f73615f0190;

# ADR-019 envelope/common fields plus cluster invite/rejoin and recovery
# controller families. Append-only: do not reuse ordinals.

struct Envelope {
  magic @0 :UInt32;
  transportVersion @1 :UInt16;
  minSupportedTransportVersion @2 :UInt16;
  schemaVersion @3 :UInt16;
  messageFamily @4 :MessageFamily;
  messageKind @5 :UInt16;
  flags @6 :UInt32;
  requiredFeatures @7 :UInt64;
  optionalFeatures @8 :UInt64;

  sender @9 :NodeIdentity;
  recipient @10 :NodeIdentity;
  clusterId @11 :Data;
  epoch @12 :UInt64;

  correlationId @13 :Data;
  causationId @14 :Data;
  streamId @15 :UInt64;
  sequence @16 :UInt64;
  deadlineUnixNanos @17 :UInt64;

  traceId @18 :Data;
  spanId @19 :Data;
  traceFlags @20 :UInt64;

  payload :union {
    hello @21 :Hello;
    helloAck @22 :HelloAck;
    error @23 :ErrorFrame;
    health @24 :HealthFrame;
    cluster @25 :ClusterControl;
    recovery @26 :RecoveryControl;
    legacy @27 :LegacyPayload;
    bootstrap @28 :BootstrapControl;
    stream @29 :StreamControl;
    # Accord consensus family (ADR-019). Append-only: new ordinal only.
    accord @30 :AccordControl;
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

struct NodeIdentity {
  hostId @0 :Data;
  nodeId @1 :UInt64;
  address @2 :Text;
  cqlBroadcast @3 :Text;
  dataCenter @4 :Text;
  rack @5 :Text;
  certificateFingerprint @6 :Data;
}

struct Hello {
  clusterName @0 :Text;
  clusterId @1 :Data;
  transportVersion @2 :UInt16;
  minSupportedTransportVersion @3 :UInt16;
  featureBits @4 :UInt64;
  supportedCompression @5 :List(Compression);
  authToken @6 :Data;
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

enum Compression {
  none @0;
  lz4 @1;
  snappy @2;
  zstd @3;
}

struct HealthFrame {
  pingNonce @0 :UInt64;
  sentAtUnixNanos @1 :UInt64;
  pingRecvAtUnixNanos @2 :UInt64;
  nodeState @3 :NodeLifecycleState;
}

struct ErrorFrame {
  code @0 :ErrorCode;
  retryable @1 :Bool;
  safeMessage @2 :Text;
  detailCode @3 :Text;
  failedFamily @4 :MessageFamily;
  failedKind @5 :UInt16;
  failedCorrelationId @6 :Data;
  leaderHint @7 :NodeIdentity;
  minSupportedTransportVersion @8 :UInt16;
  maxSupportedTransportVersion @9 :UInt16;
  missingFeatures @10 :UInt64;
}

struct LegacyPayload {
  msgType @0 :UInt16;
  body @1 :Data;
}

struct BootstrapControl {
  op :union {
    plan @0 :BootstrapPlan;
    progress @1 :BootstrapProgress;
    complete @2 :BootstrapComplete;
    error @3 :BootstrapError;
  }
}

struct BootstrapPlan {
  planId @0 :Data;
  tableId @1 :Text;
  streamPlan @2 :BootstrapStreamPlan;
}

struct BootstrapStreamPlan {
  mode :union {
    sstableBulk @0 :SstableBulkPlan;
    boundedRows @1 :BoundedRowsPlan;
    retryRequired @2 :RetryRequiredPlan;
  }
}

struct SstableBulkPlan {
  sstableDirCount @0 :UInt32;
}

struct BoundedRowsPlan {
  rowFallbackLimit @0 :UInt32;
}

struct RetryRequiredPlan {}

struct BootstrapProgress {
  planId @0 :Data;
  completedChunks @1 :UInt64;
  totalChunks @2 :UInt64;
  bytesStreamed @3 :UInt64;
}

struct BootstrapComplete {
  planId @0 :Data;
  host @1 :NodeIdentity;
  bytesStreamed @2 :UInt64;
}

struct BootstrapError {
  planId @0 :Data;
  failedPlan @1 :BootstrapStreamPlan;
  retryable @2 :Bool;
  safeMessage @3 :Text;
}

struct StreamControl {
  op :union {
    start @0 :StreamStart;
    chunk @1 :StreamChunk;
    end @2 :StreamEnd;
  }
}

enum StreamKind {
  unknown @0;
  sstable @1;
  rowFallback @2;
}

struct StreamStart {
  planId @0 :Data;
  kind @1 :StreamKind;
  totalChunks @2 :UInt64;
  maxChunkBytes @3 :UInt32;
}

struct StreamChunkMetadata {
  planId @0 :Data;
  kind @1 :StreamKind;
  chunkIndex @2 :UInt64;
  byteOffset @3 :UInt64;
  payloadBytes @4 :UInt32;
  crc32c @5 :UInt32;
  isLast @6 :Bool;
}

struct StreamChunk {
  metadata @0 :StreamChunkMetadata;
  data @1 :Data;
}

struct StreamEnd {
  planId @0 :Data;
  kind @1 :StreamKind;
  chunksSent @2 :UInt64;
  bytesSent @3 :UInt64;
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
  inviteId @4 :Data;
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

enum NodeLifecycleState {
  unknown @0;
  joining @1;
  normal @2;
  leaving @3;
  left @4;
  degraded @5;
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

# ---------------------------------------------------------------------------
# Accord consensus family (ADR-019). Append-only: these are NEW types; new
# ordinals only. The struct field shapes mirror `ferrosa-cluster`'s
# `accord::wire` bincode payloads 1:1, so a capnp-encoded Accord frame decodes
# to exactly what the bincode frame decoded to.
#
# The Apply family (`applyV2` and its single-key degenerate `apply`) is the
# BULK DATA PATH — a transactional COPY's whole write-set rides in one
# `AccordApplyV2`. The rest carry Accord's identity/dependency bookkeeping.
# ---------------------------------------------------------------------------

struct AccordTxnId {
  # ferrosa_common::accord::Timestamp — Accord's total-order execution stamp.
  epoch @0 :UInt64;
  time @1 :UInt64;
  seq @2 :UInt32;
  node @3 :UInt64;
}

struct AccordWriteSetEntry {
  # One write in a multi-key transaction's write-set (wire.rs `WriteSetEntry`).
  key @0 :Data;
  mutation @1 :Data;
}

struct AccordApplyV2 {
  # wire.rs `ApplyV2Payload`: the multi-key Apply request. The whole write-set
  # travels here, which is the heaviest object on the data path.
  txnId @0 :AccordTxnId;
  writes @1 :List(AccordWriteSetEntry);
}

struct AccordApplyV2Region {
  # The region-REFERENCE multi-key Apply: the write-set payload bytes travel as ONE
  # contiguous REGION appended AFTER this capnp message (see ferrosa-cluster README,
  # "The region-REFERENCE Apply wire"). This header carries only the index.
  txnId @0 :AccordTxnId;
  # Number of entries; `offsets`/`lengths` are parallel lists of this length.
  entryCount @1 :UInt32;
  # Byte offset of entry i within the (UNCOMPRESSED) region, and its byte length.
  offsets @2 :List(UInt64);
  lengths @3 :List(UInt32);
  # Region codec tag: 0=none, 1=lz4, 2=snappy, 3=zstd. When non-zero the appended
  # region is a blocked, length-prefixed compression of the same bytes.
  compression @4 :UInt8;
  # Uncompressed region length (the decoder sizes its output buffer from this).
  uncompressedLen @5 :UInt64;
  # Block size the region was compressed in (0 = whole region as one block).
  blockBytes @6 :UInt32;
}

struct AccordApply {
  # wire.rs `ApplyPayload`: the single-key Apply (degenerate one-entry case).
  txnId @0 :AccordTxnId;
  resultData @1 :Data;
}

struct AccordApplyOk {
  # wire.rs `ApplyOkPayload`.
  txnId @0 :AccordTxnId;
  from @1 :UInt64;
}

struct AccordPreAccept {
  # wire.rs `PreAcceptPayload`.
  txnId @0 :AccordTxnId;
  t0 @1 :AccordTxnId;
  key @2 :Data;
  ballot @3 :UInt64;
  epoch @4 :UInt64;
}

struct AccordPreAcceptV2 {
  # wire.rs `PreAcceptV2Payload`. `snapshotTs` is optional (has_*).
  txnId @0 :AccordTxnId;
  t0 @1 :AccordTxnId;
  keys @2 :List(Data);
  ballot @3 :UInt64;
  epoch @4 :UInt64;
  snapshotTs @5 :AccordTxnId;
}

struct AccordPreAcceptOk {
  # wire.rs `PreAcceptOkPayload`.
  from @0 :UInt64;
  t @1 :AccordTxnId;
  deps @2 :List(AccordTxnId);
  snapshotStale @3 :Bool;
}

struct AccordAccept {
  # wire.rs `AcceptPayload`.
  txnId @0 :AccordTxnId;
  t0 @1 :AccordTxnId;
  t @2 :AccordTxnId;
  deps @3 :List(AccordTxnId);
  ballot @4 :UInt64;
}

struct AccordAcceptOk {
  # wire.rs `AcceptOkPayload` (the current shape; a pre-dependency coordinator
  # sent a body with `deps` absent, which decodes here as an empty list).
  txnId @0 :AccordTxnId;
  deps @1 :List(AccordTxnId);
}

struct AccordCommit {
  # wire.rs `CommitPayload`.
  txnId @0 :AccordTxnId;
  t0 @1 :AccordTxnId;
  t @2 :AccordTxnId;
  deps @3 :List(AccordTxnId);
}

struct AccordCommitOk {
  # wire.rs `CommitOkPayload`.
  txnId @0 :AccordTxnId;
  from @1 :UInt64;
}

struct AccordRecover {
  # wire.rs `RecoverPayload`.
  txnId @0 :AccordTxnId;
  t0 @1 :AccordTxnId;
  ballot @2 :UInt64;
}

struct AccordRead {
  # wire.rs `ReadVotePayload` — the linearizable read-vote request.
  txnId @0 :AccordTxnId;
  t @1 :AccordTxnId;
  key @2 :Data;
  predicate @3 :AccordReadPredicate;
}

struct AccordReadPredicate {
  # wire.rs `ReadPredicate`.
  op :union {
    notExists @0 :Void;
    readRow @1 :AccordReadRow;
    snapshotBarrier @2 :Void;
    always @3 :Void;
    readClusteringRow @4 :AccordReadClusteringRow;
  }
}

struct AccordReadRow {
  keyspace @0 :Text;
  table @1 :Text;
}

struct AccordReadClusteringRow {
  keyspace @0 :Text;
  table @1 :Text;
  clustering @2 :Data;
}

struct AccordReadOk {
  # wire.rs `ReadVoteOkPayload` — the read-vote response.
  txnId @0 :AccordTxnId;
  from @1 :UInt64;
  conditionHolds @2 :Bool;
  currentRow @3 :Data;
}

struct AccordControl {
  op :union {
    # Apply family — the bulk data path.
    applyV2 @0 :AccordApplyV2;
    apply @1 :AccordApply;
    applyOk @2 :AccordApplyOk;
    # Consensus control / bookkeeping.
    preAccept @3 :AccordPreAccept;
    preAcceptV2 @4 :AccordPreAcceptV2;
    preAcceptOk @5 :AccordPreAcceptOk;
    accept @6 :AccordAccept;
    acceptOk @7 :AccordAcceptOk;
    commit @8 :AccordCommit;
    commitOk @9 :AccordCommitOk;
    recover @10 :AccordRecover;
    # `AccordRecoverOK` carries no payload (wire.rs defines no RecoverOk struct).
    recoverOk @11 :Void;
    # Linearizable read-vote.
    read @12 :AccordRead;
    readOk @13 :AccordReadOk;
    # Region-REFERENCE Apply (the region is appended after this capnp message).
    applyV2Region @14 :AccordApplyV2Region;
  }
}

# ---------------------------------------------------------------------------
# PostgreSQL MVCC row-version metadata (ferrosa-postgres `RowChange`).
#
# This is the binary Cap'n Proto replacement for the JSON row-version blob that
# used to travel inside a PostgreSQL commit's Accord write-set: `RowChange`
# (the before/after row images plus the SQL key and partition key for one row
# version) was encoded with `serde_json` and re-decoded with `serde_json` on
# replica apply. It rides INSIDE the storage mutation's envelope (it is not an
# envelope payload of its own); the `PgMvccRowChanges` root carries one
# partition's `Vec<RowChange>`.
#
# Append-only: these are NEW types at new ordinals; no existing ordinal is
# reused and no existing field is renumbered.
# ---------------------------------------------------------------------------

struct PgMvccTextOrNull {
  # One `text[]` element, which may be NULL.
  present @0 :Bool;
  value @1 :Text;
}

struct PgMvccNumeric {
  # `Value::Numeric`: arbitrary-precision `unscaled` as little-endian
  # two's-complement bytes (`BigInt::to_signed_bytes_le`) with the decimal scale.
  unscaled @0 :Data;
  scale @1 :Int32;
}

struct PgMvccInet {
  # `Value::Inet`: an IPv4 (4 octets) or IPv6 (16 octets) address.
  isV6 @0 :Bool;
  octets @1 :Data;
}

struct PgMvccValue {
  # `ferrosa_sql::Value` — one union member per variant, so the decoder keeps
  # the derived `Eq`/`Hash` value semantics (floats by bit pattern, numeric by
  # its exact normalized unscaled/scale pair).
  op :union {
    nullValue @0 :Void;
    intValue @1 :Int64;
    textValue @2 :Text;
    boolValue @3 :Bool;
    # `OrderedFloat<f64>` by bit pattern, so NaN and signed zero round-trip.
    floatValue @4 :UInt64;
    uuidValue @5 :Data;
    byteaValue @6 :Data;
    timestampValue @7 :Int64;
    dateValue @8 :Int32;
    timeValue @9 :Int64;
    inetValue @10 :PgMvccInet;
    numericValue @11 :PgMvccNumeric;
    # Validated jsonb canonical cell bytes (`JsonbValue::as_bytes`).
    jsonbValue @12 :Data;
    jsonPathValue @13 :Text;
    textArrayValue @14 :List(PgMvccTextOrNull);
  }
}

struct PgMvccRow {
  # `ferrosa_sql::Row`.
  values @0 :List(PgMvccValue);
}

struct PgMvccRowChange {
  # `ferrosa-postgres` `RowChange`: the row-version metadata for one SQL key.
  table @0 :Text;
  key @1 :List(PgMvccValue);
  partitionKey @2 :Data;
  # Optional before/after images (`hasBefore()`/`hasAfter()`).
  before @3 :PgMvccRow;
  after @4 :PgMvccRow;
  # Authoritative TableMetadata.id. Missing legacy identity is refused by new readers.
  tableId @5 :Data;
}

struct PgMvccRowChanges {
  # One partition's `Vec<RowChange>`.
  changes @0 :List(PgMvccRowChange);
  table @1 :Text;
  tableId @2 :Data;
}

# ---------------------------------------------------------------------------
# Forwarded Raft schema/membership command family (CAP_RAFT_SCHEMA_CAPNP).
#
# A non-leader node forwards a `RaftCommand` to the elected leader inside
# `ferrosa-net`'s `Message::ClusterMembershipForward` on `Lane::Data`. The body
# is today a bare bincode blob (ferrosa-cluster/src/raft_forward.rs). This
# family frames the operations whose payload is STRUCTURED SCHEMA METADATA —
# the DDL subset plus the node-membership ops the same path carries — as Cap'n
# Proto, so the leader decodes into the SAME owned structures the bincode frame
# produced (ferrosa-schema `TableMetadata`/`KeyspaceMetadata`,
# ferrosa-cluster `NodeInfo`/`NodeState`/`DataMovementEvidence`).
#
# Codec: `ferrosa_cluster::raft_forward_codec`. Bincode is a SHIPPED wire
# contract — `ferrosa-cluster/src/raft/mod.rs` pins `RaftOp`'s bincode variant
# tags by test (`raft_op_variant_tag_stability`), and a peer on the previous
# build sends bincode only — so the capnp frame is ADDED ALONGSIDE it and the
# receiver DUAL-READS. The two are told apart by a versioned magic prefix on the
# wire, a fixed `[magic:u32 BE][version:u8]` header, because a Cap'n Proto
# message's segment framing conceals a leading struct field, so the receiver
# cannot tell a capnp frame from a bincode blob by peeking the payload.
# `magic`/`version` repeat the marker INSIDE the struct, mirroring the Accord
# family's `Envelope.magic`, and are validated on decode. An unreadable or
# unknown frame is refused by name; there is no silent fallback to a
# half-parsed command.
#
# The remaining admin variants (role/grant/index/type/function/aggregate,
# `UpdateConfig`, `LeaveNode`, `AssignTokens`, `AccordApply`) are not framed:
# those commands keep the bincode body, and the absence of the wire magic is
# what says so.
#
# Append-only: these are NEW types at new ordinals; no existing ordinal is
# reused and no existing field is renumbered.
# ---------------------------------------------------------------------------

struct FwdKvPair {
  key @0 :Text;
  value @1 :Text;
}

struct FwdOptionalText {
  # `Option<String>`: `present` distinguishes `Some("")` from `None`.
  present @0 :Bool;
  value @1 :Text;
}

struct FwdOptionalBool {
  # `Option<bool>`: `present` distinguishes `Some(false)` from `None`.
  present @0 :Bool;
  value @1 :Bool;
}

struct FwdStringMap {
  # `HashMap<String, String>`. A NULL pointer (absent field) is `None`; a
  # present struct with an empty `entries` list is `Some(empty)`.
  entries @0 :List(FwdKvPair);
}

struct FwdReplicationParams {
  # ferrosa-schema `ReplicationParams`.
  strategy @0 :Text;
  options @1 :List(FwdKvPair);
}

struct FwdKeyspaceMetadata {
  # ferrosa-schema `KeyspaceMetadata`.
  name @0 :Text;
  durableWrites @1 :Bool;
  replication @2 :FwdReplicationParams;
}

struct FwdAlterKeyspace {
  # `RaftOp::AlterKeyspace { name, updates }` (`KeyspaceUpdates`).
  name @0 :Text;
  # Present (`hasReplication()`) is `Some(ReplicationParams)`; absent is `None`.
  replication @1 :FwdReplicationParams;
  durableWrites @2 :FwdOptionalBool;
}

enum FwdColumnKind {
  partitionKey @0;
  clustering @1;
  regular @2;
  static @3;
}

enum FwdClusteringOrder {
  asc @0;
  desc @1;
  none @2;
}

struct FwdColumnMask {
  # ferrosa-schema `ColumnMask`.
  functionName @0 :Text;
  arguments @1 :List(Text);
}

struct FwdColumnMetadata {
  # ferrosa-schema `ColumnMetadata`.
  name @0 :Text;
  kind @1 :FwdColumnKind;
  position @2 :Int32;
  columnType @3 :Text;
  clusteringOrder @4 :FwdClusteringOrder;
  # Present (`hasMask()`) is `Some(ColumnMask)`; absent is `None`.
  mask @5 :FwdColumnMask;
}

struct FwdCachingParams {
  # ferrosa-schema `CachingParams`.
  keys @0 :Text;
  rowsPerPartition @1 :Text;
}

struct FwdTableParams {
  # ferrosa-schema `TableParams`.
  bloomFilterFpChance @0 :Float64;
  caching @1 :FwdCachingParams;
  comment @2 :Text;
  compaction @3 :List(FwdKvPair);
  compression @4 :List(FwdKvPair);
  crcCheckChance @5 :Float64;
  defaultTimeToLive @6 :Int32;
  gcGraceSeconds @7 :Int32;
  maxIndexInterval @8 :Int32;
  minIndexInterval @9 :Int32;
  memtableFlushPeriodInMs @10 :Int32;
  speculativeRetry @11 :Text;
  additionalWritePolicy @12 :Text;
  cdc @13 :Bool;
  readRepair @14 :Text;
  allowAutoSnapshot @15 :Bool;
  incrementalBackups @16 :Bool;
}

enum FwdTableFlag {
  compound @0;
  counter @1;
  dense @2;
  super @3;
}

struct FwdClusteringKey {
  # One entry of `TableMetadata::clustering_key` (`(String, ClusteringOrder)`).
  name @0 :Text;
  order @1 :FwdClusteringOrder;
}

struct FwdTableUpdates {
  # ferrosa-schema `TableUpdates`.
  # Present (`hasParams()`) is `Some(TableParams)`; absent is `None`.
  params @0 :FwdTableParams;
  addColumns @1 :List(FwdColumnMetadata);
  dropColumns @2 :List(Text);
  # Present (`hasExtensions()`) is `Some(HashMap)`; absent is `None`.
  extensions @3 :FwdStringMap;
}

struct FwdAlterTable {
  # `RaftOp::AlterTable { keyspace, table, updates }`.
  keyspace @0 :Text;
  table @1 :Text;
  updates @2 :FwdTableUpdates;
}

struct FwdDropTable {
  # `RaftOp::DropTable { keyspace, table }`.
  keyspace @0 :Text;
  table @1 :Text;
}

struct FwdTableMetadata {
  # ferrosa-schema `TableMetadata`.
  keyspace @0 :Text;
  name @1 :Text;
  # 16 raw `Uuid` bytes (`TableMetadata::id`).
  id @2 :Data;
  columns @3 :List(FwdColumnMetadata);
  partitionKey @4 :List(Text);
  clusteringKey @5 :List(FwdClusteringKey);
  params @6 :FwdTableParams;
  flags @7 :List(FwdTableFlag);
  extensions @8 :List(FwdKvPair);
  isSystem @9 :Bool;
}

struct FwdNodeState {
  # ferrosa-cluster `NodeState`.
  state :union {
    joining @0 :Void;
    normal @1 :Void;
    leaving @2 :Void;
    decommissioned @3 :Void;
    # `Learner { owns_tokens }`.
    learner @4 :Bool;
  }
}

struct FwdNodeInfo {
  # ferrosa-cluster `NodeInfo`.
  # 16 raw `Uuid` bytes (`NodeInfo::host_id`).
  hostId @0 :Data;
  addr @1 :Text;
  dataCenter @2 :Text;
  rack @3 :Text;
  state @4 :FwdNodeState;
  # `Option<String>`: `present` distinguishes `Some("")` from `None`.
  cqlBroadcast @5 :FwdOptionalText;
}

struct FwdSetNodeState {
  # `RaftOp::SetNodeState { node_id, state }`.
  nodeId @0 :UInt64;
  state @1 :FwdNodeState;
}

struct FwdDataMovementEvidence {
  # ferrosa-cluster `DataMovementEvidence`.
  sessions @0 :UInt64;
  ranges @1 :UInt64;
  partitions @2 :UInt64;
}

struct FwdRecordBootstrapComplete {
  # `RaftOp::RecordBootstrapComplete { node_id, evidence }`.
  nodeId @0 :UInt64;
  evidence @1 :FwdDataMovementEvidence;
}

struct ForwardedRaftCommand {
  # The root of a forwarded `RaftCommand` frame. See the family banner above.
  # The versioned magic repeats here so the capnp frame is self-describing even
  # if the wire prefix were stripped; `raft_forward_codec` validates both.
  magic @0 :UInt32;
  version @1 :UInt16;
  # 16 raw `Uuid` bytes (`RaftCommand::schema_version`).
  schemaVersion @2 :Data;
  op :union {
    createKeyspace @3 :FwdKeyspaceMetadata;
    dropKeyspace @4 :Text;
    createTable @5 :FwdTableMetadata;
    dropTable @6 :FwdDropTable;
    alterKeyspace @7 :FwdAlterKeyspace;
    alterTable @8 :FwdAlterTable;
    joinNode @9 :FwdNodeInfo;
    updateNodeInfo @10 :FwdNodeInfo;
    approveNode @11 :Data;
    setNodeState @12 :FwdSetNodeState;
    recordBootstrapComplete @13 :FwdRecordBootstrapComplete;
  }
}
