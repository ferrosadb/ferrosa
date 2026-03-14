# ferrosa-net Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the ferrosa-net crate — internode wire protocol, PSK-authenticated handshake, priority-lane connection pool, RPC server/client, failure detection, and static seed discovery.

**Architecture:** ferrosa-net is a standalone transport library with no dependency on ferrosa-common. It implements a 12-byte binary frame protocol with 3 priority lanes (raft, data, bulk), multiplexed via stream IDs. Connections are authenticated via PSK (HMAC-SHA256) during handshake. The RPC layer lets ferrosa-cluster register typed message handlers. A heartbeat-based failure detector emits peer lifecycle events.

**Tech Stack:** Rust, tokio, tokio-util (codec), bytes, uuid, hmac + sha2 (PSK auth), lz4_flex + snap (compression), arc-swap (lock-free peer list), tracing

**Spec:** `docs/superpowers/specs/2026-03-13-ferrosa-net-cluster-design.md` (Part 1)

**Threat Model:** `specs/threat-model-net-cluster.md` (T1–T6, T20)

---

## File Map

### New crate: ferrosa-net

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `ferrosa-net/Cargo.toml` | Crate manifest with workspace conventions |
| Create | `ferrosa-net/src/lib.rs` | Module declarations, public re-exports |
| Create | `ferrosa-net/src/error.rs` | `NetError` enum |
| Create | `ferrosa-net/src/config.rs` | `NetConfig` struct with env/CLI defaults |
| Create | `ferrosa-net/src/codec.rs` | `FrameHeader`, `Lane`, `InternodeCodec` (Encoder/Decoder) |
| Create | `ferrosa-net/src/message.rs` | `Message` enum, per-type binary encode/decode |
| Create | `ferrosa-net/src/handshake.rs` | Handshake/HandshakeAck protocol, PSK validation |
| Create | `ferrosa-net/src/pool.rs` | `PriorityPool`: 3 TCP connections per peer, stream management |
| Create | `ferrosa-net/src/rpc/mod.rs` | Re-exports for RPC module |
| Create | `ferrosa-net/src/rpc/handler.rs` | `RpcHandler` trait, `HandlerRegistry` |
| Create | `ferrosa-net/src/rpc/server.rs` | `RpcServer`: listen, accept, handshake, dispatch |
| Create | `ferrosa-net/src/rpc/client.rs` | `RpcClient`: connect, send, await response |
| Create | `ferrosa-net/src/discovery/mod.rs` | `Discovery` trait, re-exports |
| Create | `ferrosa-net/src/discovery/seeds.rs` | `SeedDiscovery`: parse from CLI/env |
| Create | `ferrosa-net/src/peer.rs` | `PeerManager`: lifecycle events, failure detection |

### Workspace modifications

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `Cargo.toml` (workspace root) | Add `ferrosa-net` to workspace members |

---

## Chunk 1: Foundation (Tasks 1–3)

### Task 1: Crate scaffold, error types, and config

**Files:**

- Create: `ferrosa-net/Cargo.toml`
- Create: `ferrosa-net/src/lib.rs`
- Create: `ferrosa-net/src/error.rs`
- Create: `ferrosa-net/src/config.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create Cargo.toml and add to workspace**

```toml
# ferrosa-net/Cargo.toml
[package]
name = "ferrosa-net"
description = "Internode transport, RPC service, and failure detection for Ferrosa"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true

[dependencies]
arc-swap = "1.7"
async-trait = "0.1"
bytes = "1"
futures = "0.3"
hmac = "0.12"
lz4_flex = "0.11"
rand = "0.8"
sha2 = "0.10"
snap = "1"
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
tracing = "0.1"
uuid = { version = "1", features = ["v4"] }

[dev-dependencies]
proptest = "1"
tokio = { version = "1", features = ["full", "test-util"] }
```

Add `"ferrosa-net"` to the workspace members list in the root `Cargo.toml`.

- [ ] **Step 2: Create lib.rs with initial modules**

Only declare modules that exist at this point. Each subsequent task adds its module
declaration when creating the file.

```rust
// ferrosa-net/src/lib.rs
//! Internode transport, RPC service, and failure detection for Ferrosa.
//!
//! ferrosa-net owns the wire protocol, connection pool, and peer lifecycle.
//! It is a standalone transport library with no dependency on ferrosa-common.
//! ferrosa-cluster registers message handlers and reacts to peer events.

pub mod config;
pub mod error;
```

> **Note for later tasks:** Each task that creates a new module must add its
> `pub mod <name>;` declaration to `lib.rs`. Task 2 adds `codec`, Task 3 adds
> `message`, Task 4 adds `handshake`, Task 5 adds `rpc`, Task 7 adds nothing
> (client is inside rpc/), Task 8 adds `pool`, Task 9 adds `discovery`,
> Task 10 adds `peer`.

- [ ] **Step 3: Create error.rs**

```rust
// ferrosa-net/src/error.rs
use std::fmt;

/// Errors produced by the ferrosa-net transport layer.
#[derive(Debug)]
pub enum NetError {
    /// Frame body exceeds MAX_FRAME_BODY_SIZE.
    FrameTooLarge { size: u32, max: u32 },
    /// Unknown message type byte.
    UnknownMessageType(u8),
    /// Invalid lane value (must be 0–2).
    InvalidLane(u8),
    /// Handshake failed (cluster name mismatch, PSK invalid, etc.).
    HandshakeFailed(String),
    /// Connection timed out (per-lane or handshake).
    Timeout(String),
    /// Peer suspected dead (heartbeat timeout).
    PeerSuspected(uuid::Uuid),
    /// Protocol violation (corrupt frame, unexpected state).
    Protocol(String),
    /// Maximum connections reached.
    Overloaded,
    /// I/O error from the transport layer.
    Io(std::io::Error),
}

impl fmt::Display for NetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge { size, max } => {
                write!(f, "frame body too large: {size} bytes (max {max})")
            }
            Self::UnknownMessageType(t) => write!(f, "unknown message type: 0x{t:02x}"),
            Self::InvalidLane(l) => write!(f, "invalid lane: {l} (expected 0-2)"),
            Self::HandshakeFailed(msg) => write!(f, "handshake failed: {msg}"),
            Self::Timeout(msg) => write!(f, "timeout: {msg}"),
            Self::PeerSuspected(id) => write!(f, "peer suspected dead: {id}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::Overloaded => write!(f, "max internode connections reached"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for NetError {}

impl From<std::io::Error> for NetError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, NetError>;
```

- [ ] **Step 4: Create config.rs**

```rust
// ferrosa-net/src/config.rs
use std::net::SocketAddr;
use std::time::Duration;

/// Configuration for the ferrosa-net transport layer.
/// All values can be overridden via environment variables.
#[derive(Debug, Clone)]
pub struct NetConfig {
    /// Address to bind the internode listener.
    pub bind_addr: SocketAddr,
    /// Address advertised to peers (defaults to bind_addr).
    pub broadcast_addr: SocketAddr,
    /// Seed addresses for bootstrap (from --seed CLI or FERROSA_SEED env).
    pub seeds: Vec<SocketAddr>,
    /// Cluster name — must match across all nodes.
    pub cluster_name: String,
    /// Pre-shared key for handshake authentication (Phase 1).
    pub psk: Option<String>,
    /// Heartbeat ping interval.
    pub heartbeat_interval: Duration,
    /// Peer suspected-dead after this duration without heartbeat.
    pub heartbeat_timeout: Duration,
    /// Max inbound internode connections (T5 mitigation).
    pub max_connections: usize,
    /// Max time to complete handshake before closing connection (T5).
    pub handshake_timeout: Duration,
    /// Max frame body size in bytes (T3 mitigation).
    pub max_frame_body_size: u32,
    /// Max concurrent streams per connection lane (T15).
    pub max_streams_per_lane: usize,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:7000".parse().unwrap(),
            broadcast_addr: "127.0.0.1:7000".parse().unwrap(),
            seeds: Vec::new(),
            cluster_name: "ferrosa".to_string(),
            psk: None,
            heartbeat_interval: Duration::from_millis(500),
            heartbeat_timeout: Duration::from_millis(1500),
            max_connections: 100,
            handshake_timeout: Duration::from_secs(5),
            max_frame_body_size: 256 * 1024 * 1024, // 256 MiB
            max_streams_per_lane: 128,
        }
    }
}

impl NetConfig {
    /// Build config from environment variables, with defaults.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Ok(v) = std::env::var("FERROSA_INTERNODE_BIND") {
            if let Ok(addr) = v.parse() {
                cfg.bind_addr = addr;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_INTERNODE_BROADCAST") {
            if let Ok(addr) = v.parse() {
                cfg.broadcast_addr = addr;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_SEED") {
            cfg.seeds = v
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
        }
        if let Ok(v) = std::env::var("FERROSA_CLUSTER_NAME") {
            cfg.cluster_name = v;
        }
        if let Ok(v) = std::env::var("FERROSA_INTERNODE_PSK") {
            cfg.psk = Some(v);
        }
        if let Ok(v) = std::env::var("FERROSA_HEARTBEAT_INTERVAL_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                cfg.heartbeat_interval = Duration::from_millis(ms);
            }
        }
        if let Ok(v) = std::env::var("FERROSA_HEARTBEAT_TIMEOUT_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                cfg.heartbeat_timeout = Duration::from_millis(ms);
            }
        }
        if let Ok(v) = std::env::var("FERROSA_MAX_INTERNODE_CONNECTIONS") {
            if let Ok(n) = v.parse() {
                cfg.max_connections = n;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_HANDSHAKE_TIMEOUT_SECS") {
            if let Ok(s) = v.parse::<u64>() {
                cfg.handshake_timeout = Duration::from_secs(s);
            }
        }
        if let Ok(v) = std::env::var("FERROSA_MAX_FRAME_BODY_SIZE") {
            if let Ok(n) = v.parse() {
                cfg.max_frame_body_size = n;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_MAX_STREAMS_PER_LANE") {
            if let Ok(n) = v.parse() {
                cfg.max_streams_per_lane = n;
            }
        }

        cfg
    }
}
```

- [ ] **Step 5: Write tests for config and error**

```rust
// In config.rs #[cfg(test)] mod tests
#[test]
fn default_config_values() {
    let cfg = NetConfig::default();
    assert_eq!(cfg.bind_addr, "0.0.0.0:7000".parse().unwrap());
    assert_eq!(cfg.cluster_name, "ferrosa");
    assert!(cfg.psk.is_none());
    assert_eq!(cfg.max_connections, 100);
    assert_eq!(cfg.max_frame_body_size, 256 * 1024 * 1024);
    assert_eq!(cfg.max_streams_per_lane, 128);
    assert_eq!(cfg.heartbeat_interval, Duration::from_millis(500));
    assert_eq!(cfg.heartbeat_timeout, Duration::from_millis(1500));
    assert_eq!(cfg.handshake_timeout, Duration::from_secs(5));
}

// In error.rs #[cfg(test)] mod tests
#[test]
fn error_display_messages() {
    let e = NetError::FrameTooLarge { size: 1000, max: 500 };
    assert!(e.to_string().contains("1000"));
    assert!(e.to_string().contains("500"));

    let e = NetError::InvalidLane(5);
    assert!(e.to_string().contains("5"));

    let e = NetError::Overloaded;
    assert!(e.to_string().contains("max internode connections"));
}

#[test]
fn io_error_conversion() {
    let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
    let net_err: NetError = io_err.into();
    assert!(matches!(net_err, NetError::Io(_)));
}
```

- [ ] **Step 6: Verify build and tests pass**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass

- [ ] **Step 7: Commit**

```bash
git add ferrosa-net/ Cargo.toml
git commit -m "feat(net): scaffold ferrosa-net crate with error types and config"
```

---

### Task 2: Wire protocol codec (FrameHeader, Lane, InternodeCodec)

**Files:**

- Create: `ferrosa-net/src/codec.rs`

**Reference:** Spec Part 1 → Wire Protocol. Follow the `CqlCodec` pattern from `ferrosa-cql/src/frame.rs`.

- [ ] **Step 1: Write failing tests for Lane and FrameHeader**

```rust
// codec.rs #[cfg(test)] mod tests

#[test]
fn lane_from_u8_valid() {
    assert_eq!(Lane::try_from(0).unwrap(), Lane::Raft);
    assert_eq!(Lane::try_from(1).unwrap(), Lane::Data);
    assert_eq!(Lane::try_from(2).unwrap(), Lane::Bulk);
}

#[test]
fn lane_from_u8_invalid() {
    assert!(Lane::try_from(3).is_err());
    assert!(Lane::try_from(255).is_err());
}

#[test]
fn frame_header_encode_decode_roundtrip() {
    let header = FrameHeader {
        version: 1,
        flags: 0,
        lane: Lane::Data,
        msg_type: MsgType::Ping,
        stream_id: 42,
        length: 1024,
    };
    let mut buf = BytesMut::with_capacity(HEADER_SIZE);
    header.encode(&mut buf);
    assert_eq!(buf.len(), HEADER_SIZE);

    let decoded = FrameHeader::decode(&buf).unwrap();
    assert_eq!(decoded.version, 1);
    assert_eq!(decoded.lane, Lane::Data);
    assert_eq!(decoded.msg_type, MsgType::Ping);
    assert_eq!(decoded.stream_id, 42);
    assert_eq!(decoded.length, 1024);
}
```

- [ ] **Step 2: Implement Lane, MsgType, and FrameHeader**

```rust
// ferrosa-net/src/codec.rs
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::{NetError, Result};

/// Header size in bytes: version(1) + flags(1) + lane(1) + msg_type(1)
/// + stream_id(4) + length(4) = 12.
pub const HEADER_SIZE: usize = 12;

/// Flag bits.
pub const FLAG_COMPRESSED: u8 = 0x01;
pub const FLAG_STREAM_START: u8 = 0x02;
pub const FLAG_STREAM_END: u8 = 0x04;
pub const FLAG_FIRE_AND_FORGET: u8 = 0x08;

/// Priority lane for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Lane {
    Raft = 0,
    Data = 1,
    Bulk = 2,
}

impl TryFrom<u8> for Lane {
    type Error = NetError;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Raft),
            1 => Ok(Self::Data),
            2 => Ok(Self::Bulk),
            _ => Err(NetError::InvalidLane(value)),
        }
    }
}

/// Message type discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MsgType {
    // Lifecycle
    Handshake = 0x01,
    HandshakeAck = 0x02,
    Ping = 0x03,
    Pong = 0x04,
    // Raft
    RaftAppendEntries = 0x10,
    RaftAppendResponse = 0x11,
    RaftVote = 0x12,
    RaftVoteResponse = 0x13,
    RaftInstallSnapshot = 0x14,
    // Data
    MutationForward = 0x20,
    MutationAck = 0x21,
    ReadRequest = 0x22,
    ReadResponse = 0x23,
    // Streaming
    StreamStart = 0x30,
    StreamChunk = 0x31,
    StreamEnd = 0x32,
    // Pair
    PairWriteForward = 0x40,
    PairWriteAck = 0x41,
    PairCatchUp = 0x42,
    PairCatchUpResponse = 0x43,
    RoleSwap = 0x44,
}

impl TryFrom<u8> for MsgType {
    type Error = NetError;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x01 => Ok(Self::Handshake),
            0x02 => Ok(Self::HandshakeAck),
            0x03 => Ok(Self::Ping),
            0x04 => Ok(Self::Pong),
            0x10 => Ok(Self::RaftAppendEntries),
            0x11 => Ok(Self::RaftAppendResponse),
            0x12 => Ok(Self::RaftVote),
            0x13 => Ok(Self::RaftVoteResponse),
            0x14 => Ok(Self::RaftInstallSnapshot),
            0x20 => Ok(Self::MutationForward),
            0x21 => Ok(Self::MutationAck),
            0x22 => Ok(Self::ReadRequest),
            0x23 => Ok(Self::ReadResponse),
            0x30 => Ok(Self::StreamStart),
            0x31 => Ok(Self::StreamChunk),
            0x32 => Ok(Self::StreamEnd),
            0x40 => Ok(Self::PairWriteForward),
            0x41 => Ok(Self::PairWriteAck),
            0x42 => Ok(Self::PairCatchUp),
            0x43 => Ok(Self::PairCatchUpResponse),
            0x44 => Ok(Self::RoleSwap),
            _ => Err(NetError::UnknownMessageType(value)),
        }
    }
}

/// 12-byte wire frame header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u8,
    pub flags: u8,
    pub lane: Lane,
    pub msg_type: MsgType,
    pub stream_id: u32,
    pub length: u32,
}

impl FrameHeader {
    /// Create a new frame header with version=1 and flags=0.
    pub fn new(msg_type: MsgType, lane: Lane, stream_id: u32, length: u32) -> Self {
        Self { version: 1, flags: 0, lane, msg_type, stream_id, length }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.flags);
        buf.put_u8(self.lane as u8);
        buf.put_u8(self.msg_type as u8);
        buf.put_u32(self.stream_id);
        buf.put_u32(self.length);
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(NetError::Protocol("header too short".into()));
        }
        Ok(Self {
            version: buf[0],
            flags: buf[1],
            lane: Lane::try_from(buf[2])?,
            msg_type: MsgType::try_from(buf[3])?,
            stream_id: u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]),
            length: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
        })
    }
}

/// A raw internode frame: header + body bytes.
#[derive(Debug, Clone)]
pub struct Frame {
    pub header: FrameHeader,
    pub body: bytes::Bytes,
}
```

- [ ] **Step 3: Write failing tests for InternodeCodec**

```rust
#[test]
fn codec_decode_complete_frame() {
    let mut codec = InternodeCodec::new(256 * 1024 * 1024);
    let mut buf = BytesMut::new();
    let header = FrameHeader {
        version: 1,
        flags: 0,
        lane: Lane::Raft,
        msg_type: MsgType::Ping,
        stream_id: 1,
        length: 4,
    };
    header.encode(&mut buf);
    buf.put_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

    let frame = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame.header.msg_type, MsgType::Ping);
    assert_eq!(frame.body.len(), 4);
}

#[test]
fn codec_decode_incomplete_header() {
    let mut codec = InternodeCodec::new(256 * 1024 * 1024);
    let mut buf = BytesMut::from(&[0x01, 0x00, 0x00][..]);
    assert!(codec.decode(&mut buf).unwrap().is_none());
}

#[test]
fn codec_decode_incomplete_body() {
    let mut codec = InternodeCodec::new(256 * 1024 * 1024);
    let mut buf = BytesMut::new();
    let header = FrameHeader {
        version: 1, flags: 0, lane: Lane::Raft,
        msg_type: MsgType::Ping, stream_id: 0, length: 100,
    };
    header.encode(&mut buf);
    buf.put_slice(&[0u8; 50]); // only 50 of 100 bytes
    assert!(codec.decode(&mut buf).unwrap().is_none());
}

#[test]
fn codec_reject_oversized_frame() {
    let mut codec = InternodeCodec::new(1024);
    let mut buf = BytesMut::new();
    let header = FrameHeader {
        version: 1, flags: 0, lane: Lane::Data,
        msg_type: MsgType::Ping, stream_id: 0, length: 2048,
    };
    header.encode(&mut buf);
    let err = codec.decode(&mut buf).unwrap_err();
    assert!(matches!(err, NetError::FrameTooLarge { .. }));
}

#[test]
fn codec_encode_decode_roundtrip() {
    let mut codec = InternodeCodec::new(256 * 1024 * 1024);
    let frame = Frame {
        header: FrameHeader {
            version: 1, flags: 0, lane: Lane::Bulk,
            msg_type: MsgType::StreamChunk, stream_id: 99,
            length: 0, // set by encoder
        },
        body: bytes::Bytes::from_static(b"hello world"),
    };
    let mut buf = BytesMut::new();
    codec.encode(frame.clone(), &mut buf).unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.header.lane, Lane::Bulk);
    assert_eq!(decoded.header.msg_type, MsgType::StreamChunk);
    assert_eq!(decoded.header.stream_id, 99);
    assert_eq!(decoded.body, bytes::Bytes::from_static(b"hello world"));
}
```

- [ ] **Step 4: Implement InternodeCodec**

```rust
// In codec.rs, after Frame definition

/// Codec for encoding/decoding internode frames on a TCP stream.
pub struct InternodeCodec {
    max_frame_body_size: u32,
}

impl InternodeCodec {
    pub fn new(max_frame_body_size: u32) -> Self {
        Self { max_frame_body_size }
    }
}

impl Decoder for InternodeCodec {
    type Item = Frame;
    type Error = NetError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }
        let header = FrameHeader::decode(&src[..HEADER_SIZE])?;
        if header.length > self.max_frame_body_size {
            return Err(NetError::FrameTooLarge {
                size: header.length,
                max: self.max_frame_body_size,
            });
        }
        let total = HEADER_SIZE + header.length as usize;
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }
        src.advance(HEADER_SIZE);
        let body = src.split_to(header.length as usize).freeze();
        Ok(Some(Frame { header, body }))
    }
}

impl Encoder<Frame> for InternodeCodec {
    type Error = NetError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<()> {
        let mut header = item.header;
        header.length = item.body.len() as u32;
        dst.reserve(HEADER_SIZE + item.body.len());
        header.encode(dst);
        dst.put_slice(&item.body);
        Ok(())
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add ferrosa-net/src/codec.rs
git commit -m "feat(net): add wire protocol codec with 12-byte frame header"
```

---

### Task 3: Message types and serialization

**Files:**

- Create: `ferrosa-net/src/message.rs`

**Reference:** Spec Part 1 → Message Types, Message Body Serialization. Each message type uses hand-rolled big-endian binary encoding.

- [ ] **Step 1: Write failing tests for message encode/decode**

```rust
// message.rs #[cfg(test)] mod tests

#[test]
fn ping_roundtrip() {
    let msg = Message::Ping { nonce: 42 };
    let mut buf = BytesMut::new();
    msg.encode(&mut buf);
    let decoded = Message::decode(MsgType::Ping, &mut buf.freeze()).unwrap();
    assert_eq!(decoded, Message::Ping { nonce: 42 });
}

#[test]
fn handshake_roundtrip() {
    let msg = Message::Handshake {
        cluster_name: "ferrosa".to_string(),
        host_id: Uuid::new_v4(),
        protocol_version: 1,
        supported_compression: vec![0, 1], // none + lz4
        auth_token: vec![0xAB; 32],
    };
    let mut buf = BytesMut::new();
    msg.encode(&mut buf);
    let decoded = Message::decode(MsgType::Handshake, &mut buf.freeze()).unwrap();
    assert_eq!(decoded, msg);
}

#[test]
fn pair_catch_up_roundtrip() {
    let msg = Message::PairCatchUp {
        last_segment_id: 17,
        last_offset: 4096,
    };
    let mut buf = BytesMut::new();
    msg.encode(&mut buf);
    let decoded = Message::decode(MsgType::PairCatchUp, &mut buf.freeze()).unwrap();
    assert_eq!(decoded, msg);
}
```

- [ ] **Step 2: Implement Message enum with encode/decode**

The `Message` enum has one variant per `MsgType`. Phase 1 implements the lifecycle
and pair message bodies fully. Raft and data messages carry opaque `Bytes` payloads
that ferrosa-cluster will interpret — ferrosa-net just transports them.

```rust
// ferrosa-net/src/message.rs
use bytes::{Buf, BufMut, Bytes, BytesMut};
use uuid::Uuid;

use crate::codec::MsgType;
use crate::error::{NetError, Result};

/// Internode message. Each variant corresponds to a MsgType.
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    // Lifecycle
    Handshake {
        cluster_name: String,
        host_id: Uuid,
        protocol_version: u8,
        supported_compression: Vec<u8>, // 0=none, 1=lz4, 2=snappy
        auth_token: Vec<u8>,
    },
    HandshakeAck {
        host_id: Uuid,
        protocol_version: u8,
        chosen_compression: u8, // selected from initiator's supported list
        accepted: bool,
        reason: String,
    },
    Ping { nonce: u64 },
    Pong { nonce: u64 },

    // Raft — opaque payloads, ferrosa-cluster interprets
    RaftAppendEntries(Bytes),
    RaftAppendResponse(Bytes),
    RaftVote(Bytes),
    RaftVoteResponse(Bytes),
    RaftInstallSnapshot(Bytes),

    // Data — opaque payloads
    MutationForward(Bytes),
    MutationAck(Bytes),
    ReadRequest(Bytes),
    ReadResponse(Bytes),

    // Streaming — opaque payloads
    StreamStart(Bytes),
    StreamChunk(Bytes),
    StreamEnd(Bytes),

    // Pair mode
    PairWriteForward(Bytes),
    PairWriteAck(Bytes),
    PairCatchUp {
        last_segment_id: u64,
        last_offset: u32,
    },
    PairCatchUpResponse(Bytes),
    RoleSwap {
        new_primary: Uuid,
        new_secondary: Uuid,
    },
}
```

Implement `encode(&self, buf: &mut BytesMut)` and
`decode(msg_type: MsgType, body: &mut Bytes) -> Result<Self>`.

**Encoding helpers** (private functions in message.rs):

```rust
fn put_string(buf: &mut BytesMut, s: &str) {
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
}

fn get_string(buf: &mut Bytes) -> Result<String> {
    if buf.remaining() < 2 {
        return Err(NetError::Protocol("truncated string length".into()));
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(NetError::Protocol("truncated string body".into()));
    }
    let bytes = buf.split_to(len);
    String::from_utf8(bytes.to_vec())
        .map_err(|_| NetError::Protocol("invalid UTF-8 in string".into()))
}

fn put_uuid(buf: &mut BytesMut, id: &Uuid) {
    buf.put_slice(id.as_bytes());
}

fn get_uuid(buf: &mut Bytes) -> Result<Uuid> {
    if buf.remaining() < 16 {
        return Err(NetError::Protocol("truncated UUID".into()));
    }
    let mut bytes = [0u8; 16];
    buf.copy_to_slice(&mut bytes);
    Ok(Uuid::from_bytes(bytes))
}

fn put_bytes(buf: &mut BytesMut, data: &[u8]) {
    buf.put_u32(data.len() as u32);
    buf.put_slice(data);
}

fn get_bytes(buf: &mut Bytes) -> Result<Vec<u8>> {
    if buf.remaining() < 4 {
        return Err(NetError::Protocol("truncated bytes length".into()));
    }
    let len = buf.get_u32() as usize;
    if buf.remaining() < len {
        return Err(NetError::Protocol("truncated bytes body".into()));
    }
    Ok(buf.split_to(len).to_vec())
}
```

**`msg_type()` method** — returns the `MsgType` discriminant for a message:

```rust
impl Message {
    pub fn msg_type(&self) -> MsgType {
        match self {
            Self::Handshake { .. } => MsgType::Handshake,
            Self::HandshakeAck { .. } => MsgType::HandshakeAck,
            Self::Ping { .. } => MsgType::Ping,
            Self::Pong { .. } => MsgType::Pong,
            Self::RaftAppendEntries(_) => MsgType::RaftAppendEntries,
            Self::RaftAppendResponse(_) => MsgType::RaftAppendResponse,
            Self::RaftVote(_) => MsgType::RaftVote,
            Self::RaftVoteResponse(_) => MsgType::RaftVoteResponse,
            Self::RaftInstallSnapshot(_) => MsgType::RaftInstallSnapshot,
            Self::MutationForward(_) => MsgType::MutationForward,
            Self::MutationAck(_) => MsgType::MutationAck,
            Self::ReadRequest(_) => MsgType::ReadRequest,
            Self::ReadResponse(_) => MsgType::ReadResponse,
            Self::StreamStart(_) => MsgType::StreamStart,
            Self::StreamChunk(_) => MsgType::StreamChunk,
            Self::StreamEnd(_) => MsgType::StreamEnd,
            Self::PairWriteForward(_) => MsgType::PairWriteForward,
            Self::PairWriteAck(_) => MsgType::PairWriteAck,
            Self::PairCatchUp { .. } => MsgType::PairCatchUp,
            Self::PairCatchUpResponse(_) => MsgType::PairCatchUpResponse,
            Self::RoleSwap { .. } => MsgType::RoleSwap,
        }
    }

    /// Encode message body to bytes. Does NOT include the frame header.
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::Handshake { cluster_name, host_id, protocol_version, supported_compression, auth_token } => {
                put_string(buf, cluster_name);
                put_uuid(buf, host_id);
                buf.put_u8(*protocol_version);
                buf.put_u8(supported_compression.len() as u8);
                buf.put_slice(supported_compression);
                put_bytes(buf, auth_token);
            }
            Self::HandshakeAck { host_id, protocol_version, chosen_compression, accepted, reason } => {
                put_uuid(buf, host_id);
                buf.put_u8(*protocol_version);
                buf.put_u8(*chosen_compression);
                buf.put_u8(if *accepted { 1 } else { 0 });
                put_string(buf, reason);
            }
            Self::Ping { nonce } | Self::Pong { nonce } => buf.put_u64(*nonce),
            Self::PairCatchUp { last_segment_id, last_offset } => {
                buf.put_u64(*last_segment_id);
                buf.put_u32(*last_offset);
            }
            Self::RoleSwap { new_primary, new_secondary } => {
                put_uuid(buf, new_primary);
                put_uuid(buf, new_secondary);
            }
            // Opaque payload variants — copy body directly, no additional framing
            Self::RaftAppendEntries(b) | Self::RaftAppendResponse(b)
            | Self::RaftVote(b) | Self::RaftVoteResponse(b)
            | Self::RaftInstallSnapshot(b)
            | Self::MutationForward(b) | Self::MutationAck(b)
            | Self::ReadRequest(b) | Self::ReadResponse(b)
            | Self::StreamStart(b) | Self::StreamChunk(b) | Self::StreamEnd(b)
            | Self::PairWriteForward(b) | Self::PairWriteAck(b)
            | Self::PairCatchUpResponse(b) => buf.put_slice(b),
        }
    }

    /// Decode message body from bytes given the message type from the frame header.
    pub fn decode(msg_type: MsgType, body: &mut Bytes) -> Result<Self> {
        Ok(match msg_type {
            MsgType::Handshake => {
                let cluster_name = get_string(body)?;
                let host_id = get_uuid(body)?;
                if body.remaining() < 1 { return Err(NetError::Protocol("truncated handshake".into())); }
                let protocol_version = body.get_u8();
                if body.remaining() < 1 { return Err(NetError::Protocol("truncated compression list".into())); }
                let comp_len = body.get_u8() as usize;
                if body.remaining() < comp_len { return Err(NetError::Protocol("truncated compression".into())); }
                let supported_compression = body.split_to(comp_len).to_vec();
                let auth_token = get_bytes(body)?;
                Self::Handshake { cluster_name, host_id, protocol_version, supported_compression, auth_token }
            }
            MsgType::HandshakeAck => {
                let host_id = get_uuid(body)?;
                if body.remaining() < 3 { return Err(NetError::Protocol("truncated handshake ack".into())); }
                let protocol_version = body.get_u8();
                let chosen_compression = body.get_u8();
                let accepted = body.get_u8() != 0;
                let reason = get_string(body)?;
                Self::HandshakeAck { host_id, protocol_version, chosen_compression, accepted, reason }
            }
            MsgType::Ping => {
                if body.remaining() < 8 { return Err(NetError::Protocol("truncated ping".into())); }
                Self::Ping { nonce: body.get_u64() }
            }
            MsgType::Pong => {
                if body.remaining() < 8 { return Err(NetError::Protocol("truncated pong".into())); }
                Self::Pong { nonce: body.get_u64() }
            }
            MsgType::PairCatchUp => {
                if body.remaining() < 12 { return Err(NetError::Protocol("truncated pair catch up".into())); }
                Self::PairCatchUp { last_segment_id: body.get_u64(), last_offset: body.get_u32() }
            }
            MsgType::RoleSwap => {
                let new_primary = get_uuid(body)?;
                let new_secondary = get_uuid(body)?;
                Self::RoleSwap { new_primary, new_secondary }
            }
            // Opaque payload variants — take remaining bytes as payload
            MsgType::RaftAppendEntries => Self::RaftAppendEntries(body.split_to(body.remaining())),
            MsgType::RaftAppendResponse => Self::RaftAppendResponse(body.split_to(body.remaining())),
            MsgType::RaftVote => Self::RaftVote(body.split_to(body.remaining())),
            MsgType::RaftVoteResponse => Self::RaftVoteResponse(body.split_to(body.remaining())),
            MsgType::RaftInstallSnapshot => Self::RaftInstallSnapshot(body.split_to(body.remaining())),
            MsgType::MutationForward => Self::MutationForward(body.split_to(body.remaining())),
            MsgType::MutationAck => Self::MutationAck(body.split_to(body.remaining())),
            MsgType::ReadRequest => Self::ReadRequest(body.split_to(body.remaining())),
            MsgType::ReadResponse => Self::ReadResponse(body.split_to(body.remaining())),
            MsgType::StreamStart => Self::StreamStart(body.split_to(body.remaining())),
            MsgType::StreamChunk => Self::StreamChunk(body.split_to(body.remaining())),
            MsgType::StreamEnd => Self::StreamEnd(body.split_to(body.remaining())),
            MsgType::PairWriteForward => Self::PairWriteForward(body.split_to(body.remaining())),
            MsgType::PairWriteAck => Self::PairWriteAck(body.split_to(body.remaining())),
            MsgType::PairCatchUpResponse => Self::PairCatchUpResponse(body.split_to(body.remaining())),
        })
    }
}
```

- [ ] **Step 3: Add proptest fuzzing for decode safety**

```rust
// In message.rs #[cfg(test)]
use proptest::prelude::*;

proptest! {
    #[test]
    fn decode_never_panics(data in proptest::collection::vec(any::<u8>(), 0..512)) {
        let bytes = Bytes::from(data);
        // Try decoding as each message type — should return Ok or Err, never panic
        for msg_type_byte in 0x01..=0x44u8 {
            if let Ok(msg_type) = MsgType::try_from(msg_type_byte) {
                let _ = Message::decode(msg_type, &mut bytes.clone());
            }
        }
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass (including proptest with default iterations)

- [ ] **Step 5: Commit**

```bash
git add ferrosa-net/src/message.rs
git commit -m "feat(net): add message types with binary serialization and proptest fuzz"
```

---

## Chunk 2: Handshake and RPC (Tasks 4–6)

### Task 4: Handshake protocol

**Files:**

- Create: `ferrosa-net/src/handshake.rs`

**Reference:** Spec Part 1 → Handshake. PSK auth uses HMAC-SHA256 of
`cluster_name + host_id + nonce`. Threat model T4 requires PSK from Phase 1.

- [ ] **Step 1: Write failing tests**

```rust
// handshake.rs #[cfg(test)] mod tests
use super::*;
use tokio::io::duplex;
use tokio_util::codec::Framed;
use crate::codec::InternodeCodec;
use crate::config::NetConfig;

fn test_config(cluster: &str, psk: Option<&str>) -> NetConfig {
    let mut config = NetConfig::default();
    config.cluster_name = cluster.to_string();
    config.psk = psk.map(|s| s.to_string());
    config
}

#[tokio::test]
async fn handshake_success_with_psk() {
    let (client_io, server_io) = duplex(8192);
    let config = test_config("ferrosa", Some("secret"));
    let client_id = Uuid::new_v4();
    let server_id = Uuid::new_v4();

    let client_fut = initiate_handshake(
        &mut Framed::new(client_io, InternodeCodec::new(config.max_frame_body_size)),
        &config, client_id,
    );
    let server_fut = accept_handshake(
        &mut Framed::new(server_io, InternodeCodec::new(config.max_frame_body_size)),
        &config, server_id,
    );

    let (client_res, server_res) = tokio::join!(client_fut, server_fut);
    assert_eq!(client_res.unwrap(), server_id);
    assert_eq!(server_res.unwrap(), client_id);
}

#[tokio::test]
async fn handshake_rejects_cluster_name_mismatch() {
    let (client_io, server_io) = duplex(8192);
    let client_config = test_config("ferrosa", None);
    let server_config = test_config("other", None);

    let client_fut = initiate_handshake(
        &mut Framed::new(client_io, InternodeCodec::new(client_config.max_frame_body_size)),
        &client_config, Uuid::new_v4(),
    );
    let server_fut = accept_handshake(
        &mut Framed::new(server_io, InternodeCodec::new(server_config.max_frame_body_size)),
        &server_config, Uuid::new_v4(),
    );

    let (client_res, _server_res) = tokio::join!(client_fut, server_fut);
    assert!(matches!(client_res, Err(NetError::HandshakeFailed(_))));
}

#[tokio::test]
async fn handshake_rejects_bad_psk() {
    let (client_io, server_io) = duplex(8192);
    let client_config = test_config("ferrosa", Some("secret1"));
    let server_config = test_config("ferrosa", Some("secret2"));

    let client_fut = initiate_handshake(
        &mut Framed::new(client_io, InternodeCodec::new(client_config.max_frame_body_size)),
        &client_config, Uuid::new_v4(),
    );
    let server_fut = accept_handshake(
        &mut Framed::new(server_io, InternodeCodec::new(server_config.max_frame_body_size)),
        &server_config, Uuid::new_v4(),
    );

    let (client_res, _server_res) = tokio::join!(client_fut, server_fut);
    assert!(matches!(client_res, Err(NetError::HandshakeFailed(_))));
}

#[tokio::test]
async fn handshake_succeeds_without_psk() {
    let (client_io, server_io) = duplex(8192);
    let config = test_config("ferrosa", None);
    let client_id = Uuid::new_v4();
    let server_id = Uuid::new_v4();

    let client_fut = initiate_handshake(
        &mut Framed::new(client_io, InternodeCodec::new(config.max_frame_body_size)),
        &config, client_id,
    );
    let server_fut = accept_handshake(
        &mut Framed::new(server_io, InternodeCodec::new(config.max_frame_body_size)),
        &config, server_id,
    );

    let (client_res, server_res) = tokio::join!(client_fut, server_fut);
    assert_eq!(client_res.unwrap(), server_id);
    assert_eq!(server_res.unwrap(), client_id);
}

#[tokio::test]
async fn handshake_rejects_protocol_version_mismatch() {
    // Spec requires lowest-common-version negotiation. If versions are
    // incompatible (e.g., server only supports v2+, client sends v1),
    // the handshake should fail.
    // For Phase 1 both sides use PROTOCOL_VERSION=1, so this test verifies
    // the rejection path by having the acceptor check min_version > offered.
    // Implementation detail: accept_handshake rejects if offered version < 1.
    let (client_io, server_io) = duplex(8192);
    let config = test_config("ferrosa", None);

    // Manually send a Handshake with protocol_version=0 (unsupported)
    let mut client_framed = Framed::new(client_io, InternodeCodec::new(config.max_frame_body_size));
    let bad_handshake = Message::Handshake {
        cluster_name: "ferrosa".to_string(),
        host_id: Uuid::new_v4(),
        protocol_version: 0,
        supported_compression: vec![0],
        auth_token: vec![],
    };
    // Send raw and verify acceptor rejects
    use futures::SinkExt;
    let mut body = BytesMut::new();
    bad_handshake.encode(&mut body);
    let frame = Frame { header: FrameHeader::new(MsgType::Handshake, Lane::Raft, 0, body.len() as u32), body: body.freeze() };
    client_framed.send(frame).await.unwrap();

    let server_fut = accept_handshake(
        &mut Framed::new(server_io, InternodeCodec::new(config.max_frame_body_size)),
        &config, Uuid::new_v4(),
    );
    assert!(matches!(server_fut.await, Err(NetError::HandshakeFailed(_))));
}

#[test]
fn compute_auth_token_deterministic() {
    let host_id = Uuid::new_v4();
    let token1 = compute_auth_token("ferrosa", &host_id, 42, "secret");
    let token2 = compute_auth_token("ferrosa", &host_id, 42, "secret");
    assert_eq!(token1, token2);
}

#[test]
fn compute_auth_token_differs_with_different_nonce() {
    let host_id = Uuid::new_v4();
    let token1 = compute_auth_token("ferrosa", &host_id, 1, "secret");
    let token2 = compute_auth_token("ferrosa", &host_id, 2, "secret");
    assert_ne!(token1, token2);
}
```

- [ ] **Step 2: Implement handshake module**

Key functions:

```rust
// ferrosa-net/src/handshake.rs
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Compute PSK auth token: HMAC-SHA256(key=psk, data=cluster_name|host_id|nonce).
pub fn compute_auth_token(
    cluster_name: &str,
    host_id: &Uuid,
    nonce: u64,
    psk: &str,
) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(psk.as_bytes())
        .expect("HMAC accepts any key length");
    mac.update(cluster_name.as_bytes());
    mac.update(host_id.as_bytes());
    mac.update(&nonce.to_be_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Verify a received auth token.
pub fn verify_auth_token(
    cluster_name: &str,
    host_id: &Uuid,
    nonce: u64,
    psk: &str,
    token: &[u8],
) -> bool {
    let expected = compute_auth_token(cluster_name, host_id, nonce, psk);
    // Constant-time comparison
    expected.len() == token.len()
        && expected.iter().zip(token).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

/// Current protocol version. Both sides negotiate to the lower value.
pub const PROTOCOL_VERSION: u8 = 1;

/// Run initiator side of handshake over a framed connection.
pub async fn initiate_handshake<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    framed: &mut Framed<T, InternodeCodec>,
    config: &NetConfig,
    local_host_id: Uuid,
) -> Result<Uuid> {
    use futures::{SinkExt, StreamExt};

    // Build nonce for PSK auth. The auth_token is [nonce (8 bytes) || HMAC (32 bytes)]
    // so the acceptor can recover the nonce and verify the HMAC.
    let nonce: u64 = rand::random();
    let auth_token = match &config.psk {
        Some(psk) => {
            let hmac = compute_auth_token(&config.cluster_name, &local_host_id, nonce, psk);
            let mut token = nonce.to_be_bytes().to_vec();
            token.extend_from_slice(&hmac);
            token
        }
        None => vec![],
    };

    // Send Handshake
    let handshake = Message::Handshake {
        cluster_name: config.cluster_name.clone(),
        host_id: local_host_id,
        protocol_version: PROTOCOL_VERSION,
        supported_compression: vec![0], // Phase 1: no compression
        auth_token,
    };
    let mut body = BytesMut::new();
    handshake.encode(&mut body);
    let frame = Frame {
        header: FrameHeader::new(MsgType::Handshake, Lane::Raft, 0, body.len() as u32),
        body: body.freeze(),
    };
    framed.send(frame).await.map_err(|e| NetError::Io(e))?;

    // Await HandshakeAck
    let ack_frame = framed.next().await
        .ok_or_else(|| NetError::HandshakeFailed("connection closed".into()))?
        .map_err(|e| NetError::Io(e))?;
    let ack = Message::decode(ack_frame.header.msg_type, &mut ack_frame.body.clone())?;

    match ack {
        Message::HandshakeAck { host_id, accepted, reason, .. } => {
            if accepted {
                Ok(host_id)
            } else {
                Err(NetError::HandshakeFailed(reason))
            }
        }
        _ => Err(NetError::Protocol("expected HandshakeAck".into())),
    }
}

/// Run acceptor side of handshake over a framed connection.
pub async fn accept_handshake<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    framed: &mut Framed<T, InternodeCodec>,
    config: &NetConfig,
    local_host_id: Uuid,
) -> Result<Uuid> {
    use futures::{SinkExt, StreamExt};

    // Await Handshake
    let hs_frame = framed.next().await
        .ok_or_else(|| NetError::HandshakeFailed("connection closed".into()))?
        .map_err(|e| NetError::Io(e))?;
    let hs = Message::decode(hs_frame.header.msg_type, &mut hs_frame.body.clone())?;

    let (peer_host_id, peer_cluster, peer_version, peer_token) = match hs {
        Message::Handshake { cluster_name, host_id, protocol_version, auth_token, .. } => {
            (host_id, cluster_name, protocol_version, auth_token)
        }
        _ => return Err(NetError::Protocol("expected Handshake".into())),
    };

    // Validate cluster name
    if peer_cluster != config.cluster_name {
        let reason = format!("cluster mismatch: expected '{}', got '{}'", config.cluster_name, peer_cluster);
        send_handshake_ack(framed, local_host_id, false, &reason).await?;
        return Err(NetError::HandshakeFailed(reason));
    }

    // Validate protocol version (reject version 0 or versions we don't support)
    if peer_version < 1 {
        let reason = format!("unsupported protocol version: {}", peer_version);
        send_handshake_ack(framed, local_host_id, false, &reason).await?;
        return Err(NetError::HandshakeFailed(reason));
    }

    // Validate PSK if configured
    if let Some(psk) = &config.psk {
        // Reconstruct expected token — initiator used their host_id + a nonce
        // For Phase 1, we verify the token is a valid HMAC. The nonce is embedded
        // in the HMAC input, so we verify by recomputing with all possible nonces.
        // Simpler approach: the nonce is sent alongside (prepended to auth_token).
        // Actually, the initiator's nonce must be transmitted. We prepend it as the
        // first 8 bytes of auth_token.
        if peer_token.len() < 8 {
            let reason = "auth token too short".to_string();
            send_handshake_ack(framed, local_host_id, false, &reason).await?;
            return Err(NetError::HandshakeFailed(reason));
        }
        let nonce = u64::from_be_bytes(peer_token[..8].try_into().unwrap());
        if !verify_auth_token(&config.cluster_name, &peer_host_id, nonce, psk, &peer_token[8..]) {
            let reason = "PSK authentication failed".to_string();
            send_handshake_ack(framed, local_host_id, false, &reason).await?;
            return Err(NetError::HandshakeFailed(reason));
        }
    }

    // Accept
    send_handshake_ack(framed, local_host_id, true, "").await?;
    Ok(peer_host_id)
}

/// Helper to send a HandshakeAck frame.
async fn send_handshake_ack<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    framed: &mut Framed<T, InternodeCodec>,
    host_id: Uuid,
    accepted: bool,
    reason: &str,
) -> Result<()> {
    use futures::SinkExt;
    let ack = Message::HandshakeAck {
        host_id,
        protocol_version: PROTOCOL_VERSION,
        chosen_compression: 0, // Phase 1: no compression
        accepted,
        reason: reason.to_string(),
    };
    let mut body = BytesMut::new();
    ack.encode(&mut body);
    let frame = Frame {
        header: FrameHeader::new(MsgType::HandshakeAck, Lane::Raft, 0, body.len() as u32),
        body: body.freeze(),
    };
    framed.send(frame).await.map_err(|e| NetError::Io(e))
}
```

Both functions are wrapped in `tokio::time::timeout(config.handshake_timeout, ...)`
at the call site (T5 mitigation) — see Task 6 (RpcServer) and Task 7 (RpcClient).

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add ferrosa-net/src/handshake.rs
git commit -m "feat(net): add PSK-authenticated handshake protocol"
```

---

### Task 5: RPC handler trait and registry

**Files:**

- Create: `ferrosa-net/src/rpc/mod.rs`
- Create: `ferrosa-net/src/rpc/handler.rs`

**Reference:** Spec Part 1 → Key Abstractions.

- [ ] **Step 1: Write failing tests**

```rust
// handler.rs #[cfg(test)] mod tests
use super::*;
use crate::codec::MsgType;
use crate::message::Message;

struct EchoPingHandler;

#[async_trait::async_trait]
impl RpcHandler for EchoPingHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        match msg {
            Message::Ping { nonce } => Some(Message::Pong { nonce }),
            _ => None,
        }
    }
}

#[tokio::test]
async fn registry_dispatches_to_registered_handler() {
    let mut registry = HandlerRegistry::new();
    registry.register(MsgType::Ping, Arc::new(EchoPingHandler));

    let peer_id = (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
    let msg = Message::Ping { nonce: 42 };
    let response = registry.dispatch(peer_id, MsgType::Ping, msg).await;
    assert!(matches!(response, Some(Message::Pong { nonce: 42 })));
}

#[tokio::test]
async fn registry_returns_none_for_unregistered_type() {
    let registry = HandlerRegistry::new();
    let peer_id = (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
    let msg = Message::Ping { nonce: 1 };
    let response = registry.dispatch(peer_id, MsgType::Ping, msg).await;
    assert!(response.is_none());
}
```

- [ ] **Step 2: Implement RpcHandler trait and HandlerRegistry**

```rust
// ferrosa-net/src/rpc/handler.rs
use std::collections::HashMap;
use std::sync::Arc;

use crate::codec::MsgType;
use crate::message::Message;

/// Peer identifier: (host_id, socket_addr).
pub type PeerId = (uuid::Uuid, std::net::SocketAddr);

/// Trait for handling incoming RPC messages.
#[async_trait::async_trait]
pub trait RpcHandler: Send + Sync {
    /// Handle a message from a peer. Returns None for fire-and-forget.
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message>;
}

/// Registry mapping MsgType → handler.
pub struct HandlerRegistry {
    handlers: HashMap<MsgType, Arc<dyn RpcHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self { handlers: HashMap::new() }
    }

    pub fn register(&mut self, msg_type: MsgType, handler: Arc<dyn RpcHandler>) {
        self.handlers.insert(msg_type, handler);
    }

    pub async fn dispatch(
        &self,
        from: PeerId,
        msg_type: MsgType,
        msg: Message,
    ) -> Option<Message> {
        match self.handlers.get(&msg_type) {
            Some(handler) => handler.handle(from, msg).await,
            None => {
                tracing::warn!(?msg_type, "no handler registered");
                None
            }
        }
    }
}
```

Note: `async-trait` is already in `Cargo.toml` from Task 1. Add `Hash` derive
to `MsgType` in codec.rs (needed for `HashMap<MsgType, _>` in `HandlerRegistry`).

- [ ] **Step 3: Create rpc/mod.rs**

Only declare `handler` for now. Task 6 adds `pub mod server;` and Task 7
adds `pub mod client;`.

```rust
// ferrosa-net/src/rpc/mod.rs
pub mod handler;
// pub mod server; — added in Task 6
// pub mod client; — added in Task 7

pub use handler::{HandlerRegistry, PeerId, RpcHandler};
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add ferrosa-net/src/rpc/
git commit -m "feat(net): add RpcHandler trait and HandlerRegistry"
```

---

### Task 6: RPC server

**Files:**

- Create: `ferrosa-net/src/rpc/server.rs`

**Reference:** Spec Part 1 → Key Abstractions (RpcServer). Threat model T5 requires
connection limits and handshake timeout.

- [ ] **Step 1: Write failing tests**

Add `pub mod server;` to `rpc/mod.rs` first.

```rust
// server.rs #[cfg(test)] mod tests
use super::*;
use crate::codec::{Lane, MsgType};
use crate::config::NetConfig;
use crate::message::Message;
use crate::handshake::initiate_handshake;
use crate::rpc::handler::{HandlerRegistry, RpcHandler, PeerId};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

struct EchoPingHandler;

#[async_trait::async_trait]
impl RpcHandler for EchoPingHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        match msg {
            Message::Ping { nonce } => Some(Message::Pong { nonce }),
            _ => None,
        }
    }
}

#[tokio::test]
async fn server_accepts_connection_and_completes_handshake() {
    let mut config = NetConfig::default();
    config.bind_addr = "127.0.0.1:0".parse().unwrap();
    let server_id = uuid::Uuid::new_v4();
    let registry = HandlerRegistry::new();
    let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

    let addr = server.start_and_get_addr().await.unwrap();
    let client_id = uuid::Uuid::new_v4();
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut framed = Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
    let peer = initiate_handshake(&mut framed, &config, client_id).await.unwrap();
    assert_eq!(peer, server_id);
}

#[tokio::test]
async fn server_rejects_when_max_connections_reached() {
    let mut config = NetConfig::default();
    config.bind_addr = "127.0.0.1:0".parse().unwrap();
    config.max_connections = 1;
    let server_id = uuid::Uuid::new_v4();
    let registry = HandlerRegistry::new();
    let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

    let addr = server.start_and_get_addr().await.unwrap();

    // First connection succeeds
    let stream1 = TcpStream::connect(addr).await.unwrap();
    let mut framed1 = Framed::new(stream1, InternodeCodec::new(config.max_frame_body_size));
    initiate_handshake(&mut framed1, &config, uuid::Uuid::new_v4()).await.unwrap();

    // Second connection: server accepts TCP, performs handshake, then sends Overloaded
    let stream2 = TcpStream::connect(addr).await.unwrap();
    let mut framed2 = Framed::new(stream2, InternodeCodec::new(config.max_frame_body_size));
    let result = initiate_handshake(&mut framed2, &config, uuid::Uuid::new_v4()).await;
    assert!(matches!(result, Err(NetError::HandshakeFailed(_))));
}

#[tokio::test]
async fn server_dispatches_message_to_handler() {
    let mut config = NetConfig::default();
    config.bind_addr = "127.0.0.1:0".parse().unwrap();
    let server_id = uuid::Uuid::new_v4();
    let mut registry = HandlerRegistry::new();
    registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
    let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

    let addr = server.start_and_get_addr().await.unwrap();
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut framed = Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
    initiate_handshake(&mut framed, &config, uuid::Uuid::new_v4()).await.unwrap();

    // Send Ping
    use futures::{SinkExt, StreamExt};
    let ping = Message::Ping { nonce: 42 };
    let mut body = BytesMut::new();
    ping.encode(&mut body);
    let frame = Frame {
        header: FrameHeader::new(MsgType::Ping, Lane::Raft, 1, body.len() as u32),
        body: body.freeze(),
    };
    framed.send(frame).await.unwrap();

    // Receive Pong
    let resp_frame = framed.next().await.unwrap().unwrap();
    let resp = Message::decode(resp_frame.header.msg_type, &mut resp_frame.body.clone()).unwrap();
    assert!(matches!(resp, Message::Pong { nonce: 42 }));
}
```

- [ ] **Step 2: Implement RpcServer**

```rust
// ferrosa-net/src/rpc/server.rs
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

use crate::codec::{Frame, FrameHeader, InternodeCodec};
use crate::config::NetConfig;
use crate::error::NetError;
use crate::handshake::accept_handshake;
use crate::message::Message;
use crate::rpc::handler::HandlerRegistry;

pub struct RpcServer {
    config: Arc<NetConfig>,
    local_host_id: uuid::Uuid,
    registry: Arc<HandlerRegistry>,
    active_connections: Arc<AtomicUsize>,
    /// Set after binding — the actual address (useful when bind_addr uses port 0).
    bound_addr: tokio::sync::watch::Sender<Option<std::net::SocketAddr>>,
    bound_addr_rx: tokio::sync::watch::Receiver<Option<std::net::SocketAddr>>,
}

impl RpcServer {
    pub fn new(
        config: NetConfig,
        local_host_id: uuid::Uuid,
        registry: HandlerRegistry,
    ) -> Self {
        let (bound_addr, bound_addr_rx) = tokio::sync::watch::channel(None);
        Self {
            config: Arc::new(config),
            local_host_id,
            registry: Arc::new(registry),
            active_connections: Arc::new(AtomicUsize::new(0)),
            bound_addr,
            bound_addr_rx,
        }
    }

    /// Bind, store the bound address, then spawn the accept loop in background.
    /// Returns the actual bound address (resolves port 0).
    pub async fn start_and_get_addr(self: &Arc<Self>) -> crate::error::Result<std::net::SocketAddr> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        let addr = listener.local_addr()?;
        let _ = self.bound_addr.send(Some(addr));
        tracing::info!(%addr, "internode server listening");
        let server = self.clone();
        tokio::spawn(async move { server.accept_loop(listener).await });
        Ok(addr)
    }

    async fn accept_loop(self: Arc<Self>, listener: TcpListener) {
        loop {
            let (stream, peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => { tracing::error!(error = %e, "accept error"); continue; }
            };

            let current = self.active_connections.load(Ordering::Relaxed);
            if current >= self.config.max_connections {
                tracing::warn!(%peer_addr, "rejecting: max connections reached");
                // Per spec: accept TCP, read the Handshake, then reject with
                // HandshakeAck { accepted: false, reason: "overloaded" } so the
                // client gets a meaningful error instead of a silent drop.
                let config = self.config.clone();
                let host_id = self.local_host_id;
                tokio::spawn(async move {
                    let mut framed = Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
                    // Read the client's Handshake (we need to consume it)
                    if let Some(Ok(_frame)) = framed.next().await {
                        // Send rejection HandshakeAck
                        let ack = Message::HandshakeAck {
                            host_id,
                            protocol_version: crate::handshake::PROTOCOL_VERSION,
                            chosen_compression: 0,
                            accepted: false,
                            reason: "overloaded".to_string(),
                        };
                        let mut body = bytes::BytesMut::new();
                        ack.encode(&mut body);
                        let frame = Frame {
                            header: FrameHeader::new(
                                crate::codec::MsgType::HandshakeAck,
                                crate::codec::Lane::Raft,
                                0,
                                body.len() as u32,
                            ),
                            body: body.freeze(),
                        };
                        let _ = framed.send(frame).await;
                    }
                });
                continue;
            }

            self.active_connections.fetch_add(1, Ordering::Relaxed);
            let server = self.clone();
            tokio::spawn(async move {
                if let Err(e) = server.handle_connection(stream, peer_addr).await {
                    tracing::error!(%peer_addr, error = %e, "connection error");
                }
                server.active_connections.fetch_sub(1, Ordering::Relaxed);
            });
        }
    }

    async fn handle_connection(
        &self,
        stream: tokio::net::TcpStream,
        peer_addr: std::net::SocketAddr,
    ) -> crate::error::Result<()> {
        let mut framed = Framed::new(stream, InternodeCodec::new(
            self.config.max_frame_body_size,
        ));

        // Handshake with timeout (T5)
        let peer_host_id = tokio::time::timeout(
            self.config.handshake_timeout,
            accept_handshake(&mut framed, &self.config, self.local_host_id),
        )
        .await
        .map_err(|_| NetError::Timeout("handshake".into()))??;

        let peer_id = (peer_host_id, peer_addr);
        tracing::info!(?peer_id, "peer connected");

        // Message dispatch loop: read frames, decode, dispatch to registry,
        // send response (if any) back on the same connection.
        while let Some(frame_result) = framed.next().await {
            let frame = frame_result.map_err(|e| NetError::Io(e))?;
            let msg_type = frame.header.msg_type;
            let stream_id = frame.header.stream_id;
            let msg = Message::decode(msg_type, &mut frame.body.clone())?;

            if let Some(response) = self.registry.dispatch(peer_id, msg_type, msg).await {
                let mut body = bytes::BytesMut::new();
                response.encode(&mut body);
                let resp_frame = Frame {
                    header: FrameHeader::new(
                        response.msg_type(),
                        frame.header.lane,
                        stream_id, // echo stream_id for correlation
                        body.len() as u32,
                    ),
                    body: body.freeze(),
                };
                framed.send(resp_frame).await.map_err(|e| NetError::Io(e))?;
            }
            // Fire-and-forget messages (no response) — just continue the loop
        }

        tracing::info!(?peer_id, "peer disconnected");
        Ok(())
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add ferrosa-net/src/rpc/server.rs
git commit -m "feat(net): add RPC server with connection limits and handshake"
```

---

## Chunk 3: Client, Pool, and Discovery (Tasks 7–9)

### Task 7: RPC client

**Files:**

- Create: `ferrosa-net/src/rpc/client.rs`

**Pre-condition:** Add `pub mod client;` to `rpc/mod.rs`. The `Lane` type is
re-exported from `crate::codec` (defined in Task 2).

- [ ] **Step 1: Write failing tests**

```rust
// client.rs #[cfg(test)] mod tests
use super::*;
use crate::codec::{Lane, MsgType};
use crate::config::NetConfig;
use crate::message::Message;
use crate::rpc::handler::{HandlerRegistry, RpcHandler, PeerId};
use crate::rpc::server::RpcServer;
use std::sync::Arc;

struct EchoPingHandler;

#[async_trait::async_trait]
impl RpcHandler for EchoPingHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        match msg {
            Message::Ping { nonce } => Some(Message::Pong { nonce }),
            _ => None,
        }
    }
}

/// Helper: start a test server with EchoPingHandler, return its address.
async fn start_echo_server(config: &NetConfig) -> std::net::SocketAddr {
    let mut registry = HandlerRegistry::new();
    registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
    let server = Arc::new(RpcServer::new(config.clone(), uuid::Uuid::new_v4(), registry));
    server.start_and_get_addr().await.unwrap()
}

#[tokio::test]
async fn client_connects_and_handshakes() {
    let mut config = NetConfig::default();
    config.bind_addr = "127.0.0.1:0".parse().unwrap();
    let addr = start_echo_server(&config).await;

    let client = RpcClient::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
        .await
        .unwrap();
    assert_eq!(client.peer_addr(), addr);
}

#[tokio::test]
async fn client_send_and_receive() {
    let mut config = NetConfig::default();
    config.bind_addr = "127.0.0.1:0".parse().unwrap();
    let addr = start_echo_server(&config).await;

    let client = RpcClient::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
        .await
        .unwrap();
    let resp = client.send(Message::Ping { nonce: 99 }, Lane::Raft).await.unwrap();
    assert!(matches!(resp, Message::Pong { nonce: 99 }));
}

#[tokio::test]
async fn client_fire_and_forget() {
    let mut config = NetConfig::default();
    config.bind_addr = "127.0.0.1:0".parse().unwrap();
    let addr = start_echo_server(&config).await;

    let client = RpcClient::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
        .await
        .unwrap();
    // fire() should complete without error even though there's no response
    client.fire(Message::Ping { nonce: 1 }, Lane::Data).await.unwrap();
}

#[tokio::test]
async fn client_timeout_on_no_response() {
    // Server with no handlers registered — accepts but never responds
    let mut config = NetConfig::default();
    config.bind_addr = "127.0.0.1:0".parse().unwrap();
    let registry = HandlerRegistry::new(); // empty — no handlers
    let server = Arc::new(RpcServer::new(config.clone(), uuid::Uuid::new_v4(), registry));
    let addr = server.start_and_get_addr().await.unwrap();

    let client = RpcClient::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
        .await
        .unwrap();
    // Send on Raft lane (1s timeout) — should timeout since no handler responds
    let result = client.send(Message::Ping { nonce: 1 }, Lane::Raft).await;
    assert!(matches!(result, Err(NetError::Timeout(_))));
}
```

- [ ] **Step 2: Implement RpcClient**

```rust
// ferrosa-net/src/rpc/client.rs
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::codec::Framed;

use crate::codec::{Frame, FrameHeader, InternodeCodec, Lane};
use crate::config::NetConfig;
use crate::error::{NetError, Result};
use crate::handshake::initiate_handshake;
use crate::message::Message;

pub struct RpcClient {
    config: Arc<NetConfig>,
    peer_addr: std::net::SocketAddr,
    /// Pending responses keyed by stream_id.
    pending: Arc<Mutex<HashMap<u32, oneshot::Sender<Message>>>>,
    /// Sender half for outgoing frames.
    tx: mpsc::Sender<Frame>,
    next_stream_id: Arc<AtomicU32>,
}

impl RpcClient {
    pub fn peer_addr(&self) -> std::net::SocketAddr {
        self.peer_addr
    }

    /// Connect to peer, perform handshake, start read/write loops.
    pub async fn connect(
        config: Arc<NetConfig>,
        local_host_id: uuid::Uuid,
        peer_addr: std::net::SocketAddr,
    ) -> Result<Self> {
        let stream = TcpStream::connect(peer_addr).await?;
        let mut framed = Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));

        // Handshake with timeout (T5)
        let _peer_host_id = tokio::time::timeout(
            config.handshake_timeout,
            initiate_handshake(&mut framed, &config, local_host_id),
        )
        .await
        .map_err(|_| NetError::Timeout("handshake".into()))??;

        let pending: Arc<Mutex<HashMap<u32, oneshot::Sender<Message>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (tx, mut rx) = mpsc::channel::<Frame>(256);

        let (mut sink, mut stream) = framed.split();

        // Write loop: forward frames from tx channel to the connection
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                if sink.send(frame).await.is_err() {
                    break;
                }
            }
        });

        // Read loop: route incoming frames to pending oneshot receivers
        let pending_clone = pending.clone();
        tokio::spawn(async move {
            while let Some(Ok(frame)) = stream.next().await {
                let stream_id = frame.header.stream_id;
                if let Ok(msg) = Message::decode(frame.header.msg_type, &mut frame.body.clone()) {
                    let mut map = pending_clone.lock().await;
                    if let Some(sender) = map.remove(&stream_id) {
                        let _ = sender.send(msg);
                    }
                }
            }
        });

        Ok(Self {
            config,
            peer_addr,
            pending,
            tx,
            next_stream_id: Arc::new(AtomicU32::new(1)),
        })
    }

    /// Send a request and await response. Respects per-lane timeout.
    pub async fn send(&self, msg: Message, lane: Lane) -> Result<Message> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let (resp_tx, resp_rx) = oneshot::channel();

        // Register pending response
        self.pending.lock().await.insert(stream_id, resp_tx);

        // Encode and send frame
        let mut body = BytesMut::new();
        msg.encode(&mut body);
        let frame = Frame {
            header: FrameHeader::new(msg.msg_type(), lane, stream_id, body.len() as u32),
            body: body.freeze(),
        };
        self.tx.send(frame).await
            .map_err(|_| NetError::Protocol("connection closed".into()))?;

        // Await response with per-lane timeout
        tokio::time::timeout(lane.timeout(), resp_rx)
            .await
            .map_err(|_| NetError::Timeout(format!("{:?} lane timeout", lane)))?
            .map_err(|_| NetError::Protocol("response channel dropped".into()))
    }

    /// Send fire-and-forget (no response expected).
    pub async fn fire(&self, msg: Message, lane: Lane) -> Result<()> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let mut body = BytesMut::new();
        msg.encode(&mut body);
        let mut header = FrameHeader::new(msg.msg_type(), lane, stream_id, body.len() as u32);
        header.flags |= crate::codec::FLAG_FIRE_AND_FORGET;
        let frame = Frame { header, body: body.freeze() };
        self.tx.send(frame).await
            .map_err(|_| NetError::Protocol("connection closed".into()))
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add ferrosa-net/src/rpc/client.rs
git commit -m "feat(net): add RPC client with request-response and fire-and-forget"
```

---

### Task 8: Priority-lane connection pool

**Files:**

- Create: `ferrosa-net/src/pool.rs`

**Reference:** Spec Part 1 → Connection Pool. 3 TCP connections per peer, one per lane.

- [ ] **Step 1: Write failing tests**

```rust
// pool.rs #[cfg(test)] mod tests
use super::*;
use crate::codec::{Lane, MsgType};
use crate::config::NetConfig;
use crate::message::Message;
use crate::rpc::handler::{HandlerRegistry, RpcHandler, PeerId};
use crate::rpc::server::RpcServer;
use std::sync::Arc;
use std::time::Duration;

struct EchoPingHandler;

#[async_trait::async_trait]
impl RpcHandler for EchoPingHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        match msg {
            Message::Ping { nonce } => Some(Message::Pong { nonce }),
            _ => None,
        }
    }
}

#[tokio::test]
async fn pool_connects_and_sends_on_each_lane() {
    let mut config = NetConfig::default();
    config.bind_addr = "127.0.0.1:0".parse().unwrap();
    let mut registry = HandlerRegistry::new();
    registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
    let server = Arc::new(RpcServer::new(config.clone(), uuid::Uuid::new_v4(), registry));
    let addr = server.start_and_get_addr().await.unwrap();

    let pool = PriorityPool::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
        .await
        .unwrap();

    // Send on each lane — all should succeed
    for lane in [Lane::Raft, Lane::Data, Lane::Bulk] {
        let resp = pool.send(Message::Ping { nonce: 1 }, lane).await.unwrap();
        assert!(matches!(resp, Message::Pong { nonce: 1 }));
    }
}

#[test]
fn lane_timeout_values() {
    assert_eq!(Lane::Raft.timeout(), Duration::from_secs(1));
    assert_eq!(Lane::Data.timeout(), Duration::from_secs(10));
    assert_eq!(Lane::Bulk.timeout(), Duration::from_secs(60));
}
```

- [ ] **Step 2: Add `pub mod pool;` to `lib.rs` and implement PriorityPool**

```rust
// ferrosa-net/src/pool.rs
use std::sync::Arc;
use std::net::SocketAddr;

use uuid::Uuid;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::error::Result;
use crate::message::Message;
use crate::rpc::client::RpcClient;

/// Maintains 3 connections to a peer — one per priority lane.
pub struct PriorityPool {
    raft: RpcClient,
    data: RpcClient,
    bulk: RpcClient,
}

impl PriorityPool {
    /// Open 3 TCP connections to the peer (one per lane).
    /// All 3 must succeed; if any fails, the entire connect fails.
    pub async fn connect(
        config: Arc<NetConfig>,
        local_host_id: Uuid,
        peer_addr: SocketAddr,
    ) -> Result<Self> {
        let raft = RpcClient::connect(config.clone(), local_host_id, peer_addr).await?;
        let data = RpcClient::connect(config.clone(), local_host_id, peer_addr).await?;
        let bulk = RpcClient::connect(config, local_host_id, peer_addr).await?;
        Ok(Self { raft, data, bulk })
    }

    pub fn client(&self, lane: Lane) -> &RpcClient {
        match lane {
            Lane::Raft => &self.raft,
            Lane::Data => &self.data,
            Lane::Bulk => &self.bulk,
        }
    }

    pub async fn send(&self, msg: Message, lane: Lane) -> Result<Message> {
        self.client(lane).send(msg, lane).await
    }

    pub async fn fire(&self, msg: Message, lane: Lane) -> Result<()> {
        self.client(lane).fire(msg, lane).await
    }
}
```

Add `timeout()` method to `Lane` in codec.rs:

```rust
impl Lane {
    pub fn timeout(&self) -> Duration {
        match self {
            Self::Raft => Duration::from_secs(1),
            Self::Data => Duration::from_secs(10),
            Self::Bulk => Duration::from_secs(60),
        }
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add ferrosa-net/src/pool.rs ferrosa-net/src/codec.rs ferrosa-net/src/lib.rs
git commit -m "feat(net): add priority-lane connection pool (raft, data, bulk)"
```

---

### Task 9: Static seed discovery

**Files:**

- Create: `ferrosa-net/src/discovery/mod.rs`
- Create: `ferrosa-net/src/discovery/seeds.rs`

> **Scope note:** DNS-based discovery (`dns.rs` in the spec's module structure) is
> deferred to Phase 2 (net). Phase 1 uses static seed lists only.

- [ ] **Step 1: Write failing tests**

```rust
// seeds.rs #[cfg(test)] mod tests
use super::*;
use std::net::SocketAddr;

#[test]
fn parse_seeds_from_comma_separated() {
    let seeds = SeedDiscovery::parse("10.0.1.5:7000,10.0.1.6:7000");
    assert_eq!(seeds.len(), 2);
    assert_eq!(seeds[0], "10.0.1.5:7000".parse().unwrap());
}

#[test]
fn parse_seeds_empty_string() {
    let seeds = SeedDiscovery::parse("");
    assert!(seeds.is_empty());
}

#[test]
fn parse_seeds_trims_whitespace() {
    let seeds = SeedDiscovery::parse(" 10.0.1.5:7000 , 10.0.1.6:7000 ");
    assert_eq!(seeds.len(), 2);
}

#[test]
fn parse_seeds_skips_invalid() {
    let seeds = SeedDiscovery::parse("10.0.1.5:7000,not-an-addr,10.0.1.6:7000");
    assert_eq!(seeds.len(), 2); // skips "not-an-addr"
}
```

- [ ] **Step 2: Add `pub mod discovery;` to `lib.rs` and implement discovery module**

```rust
// ferrosa-net/src/discovery/mod.rs
pub mod seeds;
pub use seeds::SeedDiscovery;

use std::net::SocketAddr;

/// Trait for peer discovery mechanisms.
pub trait Discovery: Send + Sync {
    /// Return the current set of known peer addresses.
    fn peers(&self) -> Vec<SocketAddr>;
}

// ferrosa-net/src/discovery/seeds.rs
use std::net::SocketAddr;
use super::Discovery;

/// Static seed list parsed from CLI args or FERROSA_SEED env var.
pub struct SeedDiscovery {
    seeds: Vec<SocketAddr>,
}

impl SeedDiscovery {
    pub fn new(seeds: Vec<SocketAddr>) -> Self {
        Self { seeds }
    }

    pub fn parse(input: &str) -> Vec<SocketAddr> {
        input
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.parse().ok())
            .collect()
    }

    pub fn from_config(config: &crate::config::NetConfig) -> Self {
        Self::new(config.seeds.clone())
    }
}

impl Discovery for SeedDiscovery {
    fn peers(&self) -> Vec<SocketAddr> {
        self.seeds.clone()
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add ferrosa-net/src/discovery/ ferrosa-net/src/lib.rs
git commit -m "feat(net): add static seed discovery"
```

---

## Chunk 4: Failure Detection and PeerManager (Tasks 10–11)

### Task 10: Failure detection (heartbeat)

**Files:**

- Create: `ferrosa-net/src/peer.rs`

**Reference:** Spec Part 1 → Failure Detection. Heartbeat Ping on raft lane every
500ms. Suspected-dead after 3 missed heartbeats. Threat model T6.

- [ ] **Step 1: Write failing tests**

Uses `#[tokio::test(start_paused = true)]` for deterministic time control.
`tokio::time::advance()` lets us simulate heartbeat timeouts without wall-clock delays.

```rust
// peer.rs #[cfg(test)] mod tests
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct TestListener {
    connected_count: AtomicUsize,
    suspected_count: AtomicUsize,
    disconnected_count: AtomicUsize,
}

impl TestListener {
    fn new() -> Self {
        Self {
            connected_count: AtomicUsize::new(0),
            suspected_count: AtomicUsize::new(0),
            disconnected_count: AtomicUsize::new(0),
        }
    }
}

impl PeerEventListener for TestListener {
    fn on_peer_connected(&self, _peer: PeerId) {
        self.connected_count.fetch_add(1, Ordering::Relaxed);
    }
    fn on_peer_disconnected(&self, _peer: PeerId) {
        self.disconnected_count.fetch_add(1, Ordering::Relaxed);
    }
    fn on_peer_suspected(&self, _peer: PeerId) {
        self.suspected_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn peer_event_listener_receives_connected() {
    let config = Arc::new(NetConfig::default());
    let listener = Arc::new(TestListener::new());
    let pm = PeerManager::new(config, uuid::Uuid::new_v4(), listener.clone());

    let peer_id = (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
    // Use a mock pool — for unit testing we only test the PeerManager logic,
    // not actual TCP connections. Create a helper: PeerManager::add_peer_state.
    pm.add_peer_entry(peer_id).await;

    assert_eq!(listener.connected_count.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn peer_event_listener_receives_suspected() {
    let mut config = NetConfig::default();
    config.heartbeat_interval = Duration::from_millis(100);
    config.heartbeat_timeout = Duration::from_millis(300); // 3x interval
    let config = Arc::new(config);
    let listener = Arc::new(TestListener::new());
    let pm = Arc::new(PeerManager::new(config, uuid::Uuid::new_v4(), listener.clone()));

    let peer_id = (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
    pm.add_peer_entry(peer_id).await;

    // Start heartbeat loop in background
    let pm_clone = pm.clone();
    tokio::spawn(async move { pm_clone.run_heartbeat_loop().await });

    // Advance time past 3 missed heartbeats without calling record_heartbeat
    tokio::time::advance(Duration::from_millis(350)).await;
    tokio::task::yield_now().await;

    assert!(listener.suspected_count.load(Ordering::Relaxed) >= 1);
}

#[tokio::test(start_paused = true)]
async fn heartbeat_keeps_peer_alive() {
    let mut config = NetConfig::default();
    config.heartbeat_interval = Duration::from_millis(100);
    config.heartbeat_timeout = Duration::from_millis(300);
    let config = Arc::new(config);
    let listener = Arc::new(TestListener::new());
    let pm = Arc::new(PeerManager::new(config, uuid::Uuid::new_v4(), listener.clone()));

    let host_id = uuid::Uuid::new_v4();
    let peer_id = (host_id, "127.0.0.1:7000".parse().unwrap());
    pm.add_peer_entry(peer_id).await;

    let pm_clone = pm.clone();
    tokio::spawn(async move { pm_clone.run_heartbeat_loop().await });

    // Simulate responding to heartbeats: record heartbeat every 90ms for 600ms
    for _ in 0..6 {
        tokio::time::advance(Duration::from_millis(90)).await;
        pm.record_heartbeat(host_id).await;
    }

    // Peer should NOT be suspected — heartbeats kept it alive
    assert_eq!(listener.suspected_count.load(Ordering::Relaxed), 0);
}
```

- [ ] **Step 2: Implement PeerEventListener and PeerManager**

```rust
// ferrosa-net/src/peer.rs
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::message::Message;
use crate::pool::PriorityPool;
use crate::rpc::handler::PeerId;

/// Subscribe to peer lifecycle events.
pub trait PeerEventListener: Send + Sync {
    fn on_peer_connected(&self, peer: PeerId);
    fn on_peer_disconnected(&self, peer: PeerId);
    fn on_peer_suspected(&self, peer: PeerId);
}

/// Manages all peer connections and runs failure detection.
pub struct PeerManager {
    config: Arc<NetConfig>,
    local_host_id: uuid::Uuid,
    peers: RwLock<HashMap<uuid::Uuid, PeerState>>,
    listener: Arc<dyn PeerEventListener>,
}

struct PeerState {
    pool: Option<PriorityPool>, // None for unit-test entries (add_peer_entry)
    peer_id: PeerId,
    last_heartbeat: tokio::time::Instant,
    missed_heartbeats: u32, // T6: suspected after 3 consecutive misses
}

impl PeerManager {
    pub fn new(
        config: Arc<NetConfig>,
        local_host_id: uuid::Uuid,
        listener: Arc<dyn PeerEventListener>,
    ) -> Self {
        Self {
            config,
            local_host_id,
            peers: RwLock::new(HashMap::new()),
            listener,
        }
    }

    /// Add a connected peer with a real connection pool.
    pub async fn add_peer(&self, peer_id: PeerId, pool: PriorityPool) {
        let (host_id, _addr) = peer_id;
        let state = PeerState {
            pool: Some(pool),
            peer_id,
            last_heartbeat: tokio::time::Instant::now(),
            missed_heartbeats: 0,
        };
        self.peers.write().await.insert(host_id, state);
        self.listener.on_peer_connected(peer_id);
    }

    /// Add a peer entry without a connection pool (for unit testing).
    pub async fn add_peer_entry(&self, peer_id: PeerId) {
        let (host_id, _addr) = peer_id;
        let state = PeerState {
            pool: None,
            peer_id,
            last_heartbeat: tokio::time::Instant::now(),
            missed_heartbeats: 0,
        };
        self.peers.write().await.insert(host_id, state);
        self.listener.on_peer_connected(peer_id);
    }

    /// Send a message to a peer on the specified lane.
    pub async fn send(
        &self,
        host_id: uuid::Uuid,
        msg: Message,
        lane: Lane,
    ) -> crate::error::Result<Message> {
        let peers = self.peers.read().await;
        let state = peers.get(&host_id)
            .ok_or_else(|| crate::error::NetError::Protocol(
                format!("unknown peer: {host_id}")
            ))?;
        match &state.pool {
            Some(pool) => pool.send(msg, lane).await,
            None => Err(crate::error::NetError::Protocol("no connection pool".into())),
        }
    }

    /// Start heartbeat loop. Sends Ping on raft lane at configured interval.
    /// Marks peers as suspected if no Pong received within timeout (3 missed
    /// heartbeats as per spec T6).
    pub async fn run_heartbeat_loop(&self) {
        let mut interval = tokio::time::interval(self.config.heartbeat_interval);
        loop {
            interval.tick().await;

            let mut peers = self.peers.write().await;
            let mut suspected = Vec::new();

            for (host_id, state) in peers.iter_mut() {
                let elapsed = state.last_heartbeat.elapsed();
                if elapsed >= self.config.heartbeat_timeout {
                    state.missed_heartbeats += 1;
                    if state.missed_heartbeats >= 3 {
                        tracing::warn!(%host_id, "peer suspected dead: {} missed heartbeats",
                            state.missed_heartbeats);
                        suspected.push(state.peer_id);
                    }
                } else {
                    state.missed_heartbeats = 0;
                }

                // Send Ping via raft lane (fire-and-forget; Pong handled by record_heartbeat)
                if let Some(pool) = &state.pool {
                    let nonce = rand::random();
                    let _ = pool.fire(Message::Ping { nonce }, Lane::Raft).await;
                }
            }

            // Notify listener outside the write lock iteration
            drop(peers);
            for peer_id in suspected {
                self.listener.on_peer_suspected(peer_id);
            }
        }
    }

    /// Called when Pong received — update last_heartbeat timestamp and reset counter.
    pub async fn record_heartbeat(&self, host_id: uuid::Uuid) {
        let mut peers = self.peers.write().await;
        if let Some(state) = peers.get_mut(&host_id) {
            state.last_heartbeat = tokio::time::Instant::now();
            state.missed_heartbeats = 0;
        }
    }
}
```

> **Note:** `rand` and `async-trait` are already in `Cargo.toml` from Task 1.
> Add `pub mod peer;` to `lib.rs`.

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add ferrosa-net/src/peer.rs
git commit -m "feat(net): add PeerManager with heartbeat failure detection"
```

---

### Task 11: Integration test — two peers end-to-end

**Files:**

- Create: `ferrosa-net/tests/integration.rs`

This is the capstone test verifying the full stack works: server + client +
handshake + message dispatch + heartbeat.

- [ ] **Step 1: Write integration test**

```rust
// ferrosa-net/tests/integration.rs
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ferrosa_net::codec::{Lane, MsgType};
use ferrosa_net::config::NetConfig;
use ferrosa_net::message::Message;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::{HandlerRegistry, RpcHandler};
use ferrosa_net::rpc::server::RpcServer;

struct EchoPingHandler;

#[async_trait::async_trait]
impl RpcHandler for EchoPingHandler {
    async fn handle(
        &self,
        _from: ferrosa_net::rpc::PeerId,
        msg: Message,
    ) -> Option<Message> {
        match msg {
            Message::Ping { nonce } => Some(Message::Pong { nonce }),
            _ => None,
        }
    }
}

struct TestListener {
    connected: AtomicBool,
}

impl PeerEventListener for TestListener {
    fn on_peer_connected(&self, _peer: ferrosa_net::rpc::PeerId) {
        self.connected.store(true, Ordering::Relaxed);
    }
    fn on_peer_disconnected(&self, _peer: ferrosa_net::rpc::PeerId) {}
    fn on_peer_suspected(&self, _peer: ferrosa_net::rpc::PeerId) {}
}

#[tokio::test]
async fn two_peers_handshake_and_exchange_messages() {
    let mut config = NetConfig::default();
    config.bind_addr = "127.0.0.1:0".parse().unwrap();

    let server_id = uuid::Uuid::new_v4();
    let mut registry = HandlerRegistry::new();
    registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
    let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

    let addr = server.start_and_get_addr().await.unwrap();

    // Connect client via PriorityPool (tests all 3 lanes)
    let client_id = uuid::Uuid::new_v4();
    let listener = Arc::new(TestListener { connected: AtomicBool::new(false) });
    let pm = Arc::new(PeerManager::new(
        Arc::new(config.clone()), client_id, listener.clone(),
    ));

    let pool = ferrosa_net::pool::PriorityPool::connect(
        Arc::new(config), client_id, addr,
    ).await.unwrap();

    let peer_id = (server_id, addr);
    pm.add_peer(peer_id, pool).await;
    assert!(listener.connected.load(Ordering::Relaxed));

    // Send Ping on data lane, receive Pong
    let resp = pm.send(server_id, Message::Ping { nonce: 42 }, Lane::Data).await.unwrap();
    assert!(matches!(resp, Message::Pong { nonce: 42 }));
}

#[tokio::test]
async fn two_peers_with_psk_authentication() {
    let mut config = NetConfig::default();
    config.bind_addr = "127.0.0.1:0".parse().unwrap();
    config.psk = Some("test-secret".to_string());

    let mut registry = HandlerRegistry::new();
    registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
    let server = Arc::new(RpcServer::new(config.clone(), uuid::Uuid::new_v4(), registry));
    let addr = server.start_and_get_addr().await.unwrap();

    // Client with same PSK should connect successfully
    let client = ferrosa_net::rpc::client::RpcClient::connect(
        Arc::new(config), uuid::Uuid::new_v4(), addr,
    ).await.unwrap();

    let resp = client.send(Message::Ping { nonce: 7 }, Lane::Raft).await.unwrap();
    assert!(matches!(resp, Message::Pong { nonce: 7 }));
}

#[tokio::test]
async fn psk_mismatch_rejects_connection() {
    let mut server_config = NetConfig::default();
    server_config.bind_addr = "127.0.0.1:0".parse().unwrap();
    server_config.psk = Some("secret-a".to_string());

    let registry = HandlerRegistry::new();
    let server = Arc::new(RpcServer::new(server_config.clone(), uuid::Uuid::new_v4(), registry));
    let addr = server.start_and_get_addr().await.unwrap();

    // Client with different PSK — handshake should fail on the CLIENT side
    // with a HandshakeFailed error (server sends HandshakeAck with accepted=false)
    let mut client_config = server_config.clone();
    client_config.psk = Some("secret-b".to_string());
    let result = ferrosa_net::rpc::client::RpcClient::connect(
        Arc::new(client_config), uuid::Uuid::new_v4(), addr,
    ).await;
    assert!(matches!(result, Err(ferrosa_net::error::NetError::HandshakeFailed(_))));
}
```

- [ ] **Step 2: Run integration tests**

Run: `cargo test -p ferrosa-net --test integration`
Expected: All tests pass

- [ ] **Step 3: Run full test suite**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass (unit + integration)

- [ ] **Step 4: Run clippy and fmt**

Run: `cargo clippy -p ferrosa-net --all-targets && cargo fmt -p ferrosa-net --check`
Expected: No warnings, formatting clean

- [ ] **Step 5: Commit**

```bash
git add ferrosa-net/tests/
git commit -m "test(net): add integration tests for two-peer handshake and messaging"
```

---

## Summary

| Task | Component | Tests | Key Threat Mitigations |
|------|-----------|-------|----------------------|
| 1 | Scaffold + error + config | 5 | T3 (frame size), T5 (connection limits), T15 (stream limits) |
| 2 | Wire protocol codec | 7 | T3 (oversized frame rejection, invalid lane/type) |
| 3 | Message serialization | 5+ proptest | T3 (decode never panics) |
| 4 | Handshake protocol | 6 | T4 (PSK auth), T5 (handshake timeout) |
| 5 | RPC handler + registry | 2 | — |
| 6 | RPC server | 3 | T5 (max connections) |
| 7 | RPC client | 4 | Per-lane timeouts |
| 8 | Priority-lane pool | 3 | Lane isolation |
| 9 | Static seed discovery | 4 | — |
| 10 | Failure detection | 3 | T6 (heartbeat + stream_id correlation) |
| 11 | Integration tests | 3 | End-to-end verification |

**Total:** 11 tasks, ~45 tests, all threat mitigations from the threat model for ferrosa-net Phase 1 (T3, T4, T5, T6) baked into the implementation from the start.
