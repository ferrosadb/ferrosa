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
  # Byte offset of entry i within the appended region, and its byte length.
  offsets @2 :List(UInt64);
  lengths @3 :List(UInt32);
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
}

struct PgMvccRowChanges {
  # One partition's `Vec<RowChange>`.
  changes @0 :List(PgMvccRowChange);
}
