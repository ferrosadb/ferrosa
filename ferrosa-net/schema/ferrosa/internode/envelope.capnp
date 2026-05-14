@0xf36f6f73615f0190;

# Minimal first slice of ADR-019: stable envelope/common fields plus one
# representative cluster-control family. Append-only: do not reuse ordinals.

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

enum NodeLifecycleState {
  unknown @0;
  joining @1;
  normal @2;
  leaving @3;
  left @4;
  degraded @5;
}
