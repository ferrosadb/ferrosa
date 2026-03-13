# ferrosa-cql Implementation Plan — Part A: Protocol + Types

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development
> (if subagents available) or superpowers:executing-plans to implement this plan.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the CQL native protocol v5 frame layer, type system, error types,
and TCP server with SASL PLAIN authentication — enough to accept connections
from a real CQL driver and complete the auth handshake.

**Architecture:** Tokio multi-threaded runtime with per-connection tasks. Frame
codec uses `tokio-util`'s `Encoder`/`Decoder` on `Framed<TcpStream>`. Lock-free
schema access via `ArcSwap<SchemaSnapshot>`. Auth delegates to
`ferrosa-schema::Schema::authenticate()`.

**Tech Stack:** Rust, tokio, tokio-util, bytes, arc-swap, uuid, num-bigint, phf,
md-5, ferrosa-common, ferrosa-schema

**Spec:** `docs/superpowers/specs/2026-03-12-ferrosa-cql-design.md`

**Prerequisite:** This plan assumes the working branch includes `ferrosa-schema`
and `ferrosa-storage` from main. Before starting, rebase or merge main into your
feature branch so these crates are available:

```bash
git fetch origin
git merge origin/main
```

---

## Chunk 1: Crate Setup + Frame Layer

### Task 1: Create the ferrosa-cql crate

**Files:**

- Create: `ferrosa-cql/Cargo.toml`
- Create: `ferrosa-cql/src/lib.rs`
- Modify: `Cargo.toml` (workspace members)

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
name = "ferrosa-cql"
description = "CQL native protocol v5 for Ferrosa"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true

[dependencies]
ferrosa-common = { path = "../ferrosa-common" }
ferrosa-schema = { path = "../ferrosa-schema" }
ferrosa-storage = { path = "../ferrosa-storage" }
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
bytes = "1"
arc-swap = "1.7"
uuid = { version = "1", features = ["v4"] }
num-bigint = "0.4"
phf = { version = "0.11", features = ["macros"] }
md-5 = "0.10"
tracing = "0.1"

[dev-dependencies]
proptest = "1"
tempfile = "3"
tokio = { version = "1", features = ["full", "test-util"] }
```

- [ ] **Step 2: Create src/lib.rs stub**

```rust
//! CQL native protocol v5 for Ferrosa.
//!
//! This crate implements the binary framing, type system, parser, query
//! routing, and TCP server for CQL protocol v5 — the client-facing
//! interface to the Ferrosa database.
//!
//! # Architecture
//!
//! Each TCP connection gets its own Tokio task. The task owns a
//! `Framed<TcpStream, CqlCodec>` for zero-copy frame encoding/decoding,
//! an `AuthContext` after authentication, and a reference to the shared
//! `ArcSwap<SchemaSnapshot>` for lock-free schema reads.
//!
//! All hot paths are lock-free: schema lookups use `ArcSwap::load()`,
//! prepared statement cache uses `moka` (W-TinyLFU), and storage
//! access goes through `Arc<StorageEngine>`.

pub mod error;
pub mod frame;
```

- [ ] **Step 3: Create src/error.rs stub**

Tasks 2-3 need `CqlError` to compile (Opcode `TryFrom` and codec `Error` type).
Create a minimal stub that will be expanded in Task 4.

```rust
//! CQL error types.
//!
//! Minimal stub — expanded with full error codes, Display, and
//! encode_body in Task 4.

use std::fmt;
use std::io;

/// CQL protocol error.
#[derive(Debug, Clone)]
pub enum CqlError {
    /// Protocol-level error (malformed frame, wrong version, bad opcode).
    Protocol(String),
    /// I/O error wrapper.
    Io(String),
}

impl fmt::Display for CqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(msg) => write!(f, "{msg}"),
            Self::Io(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for CqlError {}

impl From<io::Error> for CqlError {
    fn from(e: io::Error) -> Self {
        Self::Io(e.to_string())
    }
}
```

- [ ] **Step 4: Add ferrosa-cql to workspace members**

In the root `Cargo.toml`, add `"ferrosa-cql"` to the `members` list:

```toml
[workspace]
resolver = "2"
members = [
    "ferrosa-common",
    "ferrosa-cql",
    "ferrosa-schema",
    "ferrosa-sstable",
    "ferrosa-storage",
]
```

- [ ] **Step 5: Verify the crate compiles**

Run: `cargo build -p ferrosa-cql`
Expected: SUCCESS (lib.rs, error.rs stub, and empty frame.rs all exist)

- [ ] **Step 6: Commit**

```bash
git add ferrosa-cql/Cargo.toml ferrosa-cql/src/lib.rs ferrosa-cql/src/error.rs Cargo.toml Cargo.lock
git commit -m "feat(cql): scaffold ferrosa-cql crate with dependencies and error stub"
```

---

### Task 2: Opcode enum and frame header types

**Files:**

- Create: `ferrosa-cql/src/frame.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing tests for Opcode**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opcode_from_u8_roundtrip() {
        for &(byte, expected) in &[
            (0x00, Opcode::Error),
            (0x01, Opcode::Startup),
            (0x02, Opcode::Ready),
            (0x03, Opcode::Authenticate),
            (0x05, Opcode::Options),
            (0x06, Opcode::Supported),
            (0x07, Opcode::Query),
            (0x08, Opcode::Result),
            (0x09, Opcode::Prepare),
            (0x0A, Opcode::Execute),
            (0x0B, Opcode::Register),
            (0x0C, Opcode::Event),
            (0x0D, Opcode::Batch),
            (0x0E, Opcode::AuthChallenge),
            (0x0F, Opcode::AuthResponse),
            (0x10, Opcode::AuthSuccess),
        ] {
            let op = Opcode::try_from(byte).unwrap();
            assert_eq!(op, expected);
            assert_eq!(op as u8, byte);
        }
    }

    #[test]
    fn opcode_from_invalid_byte() {
        assert!(Opcode::try_from(0x04).is_err()); // deprecated CREDENTIALS
        assert!(Opcode::try_from(0xFF).is_err());
    }

    #[test]
    fn frame_header_size_is_9() {
        assert_eq!(HEADER_SIZE, 9);
    }

    #[test]
    fn frame_header_encode_decode_roundtrip() {
        let header = FrameHeader {
            version: VERSION_RESPONSE,
            flags: 0,
            stream_id: 42,
            opcode: Opcode::Result,
            length: 1024,
        };
        let mut buf = BytesMut::with_capacity(HEADER_SIZE);
        header.encode(&mut buf);
        assert_eq!(buf.len(), HEADER_SIZE);

        let decoded = FrameHeader::decode(&buf).unwrap();
        assert_eq!(decoded.version, header.version);
        assert_eq!(decoded.flags, header.flags);
        assert_eq!(decoded.stream_id, header.stream_id);
        assert_eq!(decoded.opcode, header.opcode);
        assert_eq!(decoded.length, header.length);
    }

    #[test]
    fn frame_header_negative_stream_id() {
        let header = FrameHeader {
            version: VERSION_REQUEST,
            flags: 0,
            stream_id: -1,
            opcode: Opcode::Query,
            length: 0,
        };
        let mut buf = BytesMut::with_capacity(HEADER_SIZE);
        header.encode(&mut buf);
        let decoded = FrameHeader::decode(&buf).unwrap();
        assert_eq!(decoded.stream_id, -1);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-cql -- frame`
Expected: FAIL — `Opcode` and `FrameHeader` not defined

- [ ] **Step 3: Implement Opcode enum and FrameHeader**

```rust
//! CQL native protocol v5 frame encoding and decoding.
//!
//! Each frame has a 9-byte header (version, flags, stream ID, opcode,
//! length) followed by a body of `length` bytes.

use bytes::{Buf, BufMut, BytesMut};

use crate::error::CqlError;

/// CQL native protocol v5 version byte for requests.
pub const VERSION_REQUEST: u8 = 0x05;
/// CQL native protocol v5 version byte for responses.
pub const VERSION_RESPONSE: u8 = 0x85;
/// Size of the frame header in bytes.
pub const HEADER_SIZE: usize = 9;
/// Default maximum frame body size: 256 MiB.
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 256 * 1024 * 1024;

/// CQL protocol opcodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    // 0x04 = CREDENTIALS, deprecated since v2
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
}

impl TryFrom<u8> for Opcode {
    type Error = CqlError;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Error),
            0x01 => Ok(Self::Startup),
            0x02 => Ok(Self::Ready),
            0x03 => Ok(Self::Authenticate),
            0x05 => Ok(Self::Options),
            0x06 => Ok(Self::Supported),
            0x07 => Ok(Self::Query),
            0x08 => Ok(Self::Result),
            0x09 => Ok(Self::Prepare),
            0x0A => Ok(Self::Execute),
            0x0B => Ok(Self::Register),
            0x0C => Ok(Self::Event),
            0x0D => Ok(Self::Batch),
            0x0E => Ok(Self::AuthChallenge),
            0x0F => Ok(Self::AuthResponse),
            0x10 => Ok(Self::AuthSuccess),
            _ => Err(CqlError::Protocol(format!(
                "unknown opcode: 0x{value:02X}"
            ))),
        }
    }
}

/// A parsed CQL frame header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u8,
    pub flags: u8,
    pub stream_id: i16,
    pub opcode: Opcode,
    pub length: u32,
}

impl FrameHeader {
    /// Encode this header into `buf`. Appends exactly 9 bytes.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.flags);
        buf.put_i16(self.stream_id);
        buf.put_u8(self.opcode as u8);
        buf.put_u32(self.length);
    }

    /// Decode a header from the first 9 bytes of `buf`.
    pub fn decode(buf: &[u8]) -> std::result::Result<Self, CqlError> {
        if buf.len() < HEADER_SIZE {
            return Err(CqlError::Protocol(format!(
                "header too short: {} bytes",
                buf.len()
            )));
        }
        let mut cursor = &buf[..HEADER_SIZE];
        let version = cursor.get_u8();
        let flags = cursor.get_u8();
        let stream_id = cursor.get_i16();
        let opcode = Opcode::try_from(cursor.get_u8())?;
        let length = cursor.get_u32();
        Ok(Self {
            version,
            flags,
            stream_id,
            opcode,
            length,
        })
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-cql -- frame`
Expected: PASS (all 4 tests)

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/frame.rs
git commit -m "feat(cql): add Opcode enum and FrameHeader encode/decode"
```

---

### Task 3: CqlCodec (tokio Encoder/Decoder)

**Files:**

- Modify: `ferrosa-cql/src/frame.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing tests for CqlCodec**

```rust
#[test]
fn codec_decode_complete_frame() {
    let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
    let mut buf = BytesMut::new();

    // Build a STARTUP frame: version=0x05, flags=0, stream=1, opcode=0x01, body=empty
    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 1,
        opcode: Opcode::Startup,
        length: 0,
    };
    header.encode(&mut buf);

    let frame = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame.header.opcode, Opcode::Startup);
    assert_eq!(frame.header.stream_id, 1);
    assert!(frame.body.is_empty());
}

#[test]
fn codec_decode_incomplete_header() {
    let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
    let mut buf = BytesMut::from(&[0x05, 0x00, 0x00][..]);
    // Only 3 bytes — not enough for 9-byte header
    assert!(codec.decode(&mut buf).unwrap().is_none());
}

#[test]
fn codec_decode_incomplete_body() {
    let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
    let mut buf = BytesMut::new();
    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Query,
        length: 100,
    };
    header.encode(&mut buf);
    buf.put_slice(&[0u8; 50]); // Only 50 of 100 body bytes
    assert!(codec.decode(&mut buf).unwrap().is_none());
}

#[test]
fn codec_reject_oversized_frame() {
    let mut codec = CqlCodec::new(1024); // 1KB max
    let mut buf = BytesMut::new();
    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Query,
        length: 2048,
    };
    header.encode(&mut buf);
    assert!(codec.decode(&mut buf).is_err());
}

#[test]
fn codec_encode_frame() {
    let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
    let frame = CqlFrame {
        header: FrameHeader {
            version: VERSION_RESPONSE,
            flags: 0,
            stream_id: 5,
            opcode: Opcode::Result,
            length: 0, // will be set by encoder
        },
        body: BytesMut::from(&b"hello"[..]).freeze(),
    };
    let mut buf = BytesMut::new();
    codec.encode(frame, &mut buf).unwrap();

    // Should have 9-byte header + 5-byte body
    assert_eq!(buf.len(), 14);
    let decoded_header = FrameHeader::decode(&buf).unwrap();
    assert_eq!(decoded_header.length, 5);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-cql -- frame`
Expected: FAIL — `CqlCodec` and `CqlFrame` not defined

- [ ] **Step 3: Implement CqlCodec**

Add to `frame.rs`:

```rust
use bytes::Bytes;
use tokio_util::codec::{Decoder, Encoder};

/// A complete CQL frame: header + body.
#[derive(Debug, Clone)]
pub struct CqlFrame {
    pub header: FrameHeader,
    pub body: Bytes,
}

/// Tokio codec for CQL v5 frame encoding/decoding.
///
/// Implements length-delimited framing with a configurable max frame size.
/// The decoder accumulates bytes until a complete frame (header + body) is
/// available, then returns a `CqlFrame`.
pub struct CqlCodec {
    max_frame_size: u32,
}

impl CqlCodec {
    pub fn new(max_frame_size: u32) -> Self {
        Self { max_frame_size }
    }
}

impl Decoder for CqlCodec {
    type Item = CqlFrame;
    type Error = CqlError;

    fn decode(&mut self, src: &mut BytesMut) -> std::result::Result<Option<Self::Item>, Self::Error> {
        // Need at least the header.
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }

        // Peek at the header without consuming.
        let header = FrameHeader::decode(&src[..HEADER_SIZE])?;

        if header.length > self.max_frame_size {
            return Err(CqlError::Protocol(format!(
                "frame body too large: {} bytes (max {})",
                header.length, self.max_frame_size
            )));
        }

        let total = HEADER_SIZE + header.length as usize;
        if src.len() < total {
            // Reserve space for the rest of the frame.
            src.reserve(total - src.len());
            return Ok(None);
        }

        // Consume header + body.
        src.advance(HEADER_SIZE);
        let body = src.split_to(header.length as usize).freeze();

        Ok(Some(CqlFrame { header, body }))
    }
}

impl Encoder<CqlFrame> for CqlCodec {
    type Error = CqlError;

    fn encode(&mut self, item: CqlFrame, dst: &mut BytesMut) -> std::result::Result<(), Self::Error> {
        let mut header = item.header;
        header.length = item.body.len() as u32;
        dst.reserve(HEADER_SIZE + item.body.len());
        header.encode(dst);
        dst.put(item.body);
        Ok(())
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-cql -- frame`
Expected: PASS (all 9 tests)

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/frame.rs
git commit -m "feat(cql): add CqlCodec with Encoder/Decoder for frame I/O"
```

---

### Task 4: CqlError enum (replace stub with full implementation)

**Files:**

- Modify: `ferrosa-cql/src/error.rs` (replace the stub from Task 1)
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing tests for CqlError**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_values() {
        assert_eq!(CqlError::ServerError("".into()).error_code(), 0x0000);
        assert_eq!(CqlError::Protocol("".into()).error_code(), 0x000A);
        assert_eq!(CqlError::BadCredentials.error_code(), 0x0100);
        assert_eq!(CqlError::Unavailable.error_code(), 0x1000);
        assert_eq!(CqlError::Overloaded.error_code(), 0x1100);
        assert_eq!(CqlError::SyntaxError("".into()).error_code(), 0x2000);
        assert_eq!(CqlError::Unauthorized("".into()).error_code(), 0x2100);
        assert_eq!(CqlError::Invalid("".into()).error_code(), 0x2200);
        assert_eq!(CqlError::ConfigError("".into()).error_code(), 0x2300);
        assert_eq!(
            CqlError::AlreadyExists {
                keyspace: "ks".into(),
                table: "t".into()
            }
            .error_code(),
            0x2400
        );
        assert_eq!(CqlError::Unprepared([0u8; 16]).error_code(), 0x2500);
    }

    #[test]
    fn encode_error_frame_body() {
        let err = CqlError::SyntaxError("bad query".into());
        let body = err.encode_body();
        // 4-byte error code + 2-byte string length + "bad query" (9 bytes)
        assert_eq!(&body[..4], &0x2000u32.to_be_bytes());
        let str_len = u16::from_be_bytes([body[4], body[5]]) as usize;
        let msg = std::str::from_utf8(&body[6..6 + str_len]).unwrap();
        assert_eq!(msg, "bad query");
    }

    #[test]
    fn from_schema_error_keyspace_exists() {
        let schema_err =
            ferrosa_schema::SchemaError::KeyspaceExists("ks1".into());
        let cql_err: CqlError = schema_err.into();
        assert_eq!(cql_err.error_code(), 0x2400);
    }

    #[test]
    fn from_schema_error_permission_denied() {
        let schema_err = ferrosa_schema::SchemaError::PermissionDenied {
            role: "user".into(),
            permission: ferrosa_schema::Permission::Select,
            resource: ferrosa_schema::Resource::AllKeyspaces,
        };
        let cql_err: CqlError = schema_err.into();
        assert_eq!(cql_err.error_code(), 0x2100);
    }

    #[test]
    fn error_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<CqlError>();
        assert_sync::<CqlError>();
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-cql -- error`
Expected: FAIL — `CqlError` not defined

- [ ] **Step 3: Implement CqlError**

```rust
//! CQL protocol error types.
//!
//! Each variant maps to a CQL error code from the native protocol spec.
//! `encode_body()` produces the wire-format error response body.

use bytes::{BufMut, BytesMut};

/// CQL protocol error with structured data for each error code.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CqlError {
    /// 0x0000 — unexpected internal failure.
    ServerError(String),
    /// 0x000A — malformed frame, wrong version, bad opcode.
    Protocol(String),
    /// 0x0100 — authentication rejected.
    BadCredentials,
    /// 0x1000 — not enough replicas.
    Unavailable,
    /// 0x1100 — server backpressure.
    Overloaded,
    /// 0x2000 — CQL syntax error.
    SyntaxError(String),
    /// 0x2100 — insufficient permissions.
    Unauthorized(String),
    /// 0x2200 — valid syntax but semantic error.
    Invalid(String),
    /// 0x2300 — invalid DDL configuration.
    ConfigError(String),
    /// 0x2400 — object already exists.
    AlreadyExists { keyspace: String, table: String },
    /// 0x2500 — unknown prepared statement ID.
    Unprepared([u8; 16]),
}

impl CqlError {
    /// Returns the CQL error code for this error.
    pub fn error_code(&self) -> u32 {
        match self {
            Self::ServerError(_) => 0x0000,
            Self::Protocol(_) => 0x000A,
            Self::BadCredentials => 0x0100,
            Self::Unavailable => 0x1000,
            Self::Overloaded => 0x1100,
            Self::SyntaxError(_) => 0x2000,
            Self::Unauthorized(_) => 0x2100,
            Self::Invalid(_) => 0x2200,
            Self::ConfigError(_) => 0x2300,
            Self::AlreadyExists { .. } => 0x2400,
            Self::Unprepared(_) => 0x2500,
        }
    }

    /// Encode the error body for a CQL ERROR response frame.
    ///
    /// Format: `[int error_code][string message][extra fields...]`
    pub fn encode_body(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u32(self.error_code());

        let msg = self.to_string();
        buf.put_u16(msg.len() as u16);
        buf.put_slice(msg.as_bytes());

        // Extra fields for specific error types.
        match self {
            Self::AlreadyExists { keyspace, table } => {
                buf.put_u16(keyspace.len() as u16);
                buf.put_slice(keyspace.as_bytes());
                buf.put_u16(table.len() as u16);
                buf.put_slice(table.as_bytes());
            }
            Self::Unprepared(id) => {
                buf.put_u16(16);
                buf.put_slice(id);
            }
            _ => {}
        }

        buf
    }
}

impl std::fmt::Display for CqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServerError(msg) => write!(f, "server error: {msg}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::BadCredentials => write!(f, "bad credentials"),
            Self::Unavailable => write!(f, "unavailable"),
            Self::Overloaded => write!(f, "overloaded"),
            Self::SyntaxError(msg) => write!(f, "{msg}"),
            Self::Unauthorized(msg) => write!(f, "unauthorized: {msg}"),
            Self::Invalid(msg) => write!(f, "{msg}"),
            Self::ConfigError(msg) => write!(f, "config error: {msg}"),
            Self::AlreadyExists { keyspace, table } => {
                if table.is_empty() {
                    write!(f, "keyspace already exists: {keyspace}")
                } else {
                    write!(f, "table already exists: {keyspace}.{table}")
                }
            }
            Self::Unprepared(id) => {
                write!(f, "unprepared: ")?;
                for b in id {
                    write!(f, "{b:02x}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for CqlError {}

impl From<ferrosa_schema::SchemaError> for CqlError {
    fn from(err: ferrosa_schema::SchemaError) -> Self {
        use ferrosa_schema::SchemaError;
        match err {
            SchemaError::KeyspaceExists(ks) => Self::AlreadyExists {
                keyspace: ks,
                table: String::new(),
            },
            SchemaError::TableExists(ks, t) => Self::AlreadyExists {
                keyspace: ks,
                table: t,
            },
            SchemaError::KeyspaceNotFound(ks) => {
                Self::Invalid(format!("keyspace not found: {ks}"))
            }
            SchemaError::TableNotFound(ks, t) => {
                Self::Invalid(format!("table not found: {ks}.{t}"))
            }
            SchemaError::RoleExists(r) => Self::Invalid(format!("role already exists: {r}")),
            SchemaError::RoleNotFound(r) => Self::Invalid(format!("role not found: {r}")),
            SchemaError::AuthenticationFailed => Self::BadCredentials,
            SchemaError::AuthenticationThrottled => Self::BadCredentials,
            SchemaError::PermissionDenied { role, permission, resource } => {
                Self::Unauthorized(format!("{role} lacks {permission} on {resource}"))
            }
            SchemaError::SystemKeyspaceProtected(ks) => {
                Self::Invalid(format!("cannot modify system keyspace: {ks}"))
            }
            SchemaError::PasswordTooWeak { violations } => {
                Self::Invalid(format!("password too weak: {}", violations.join(", ")))
            }
            SchemaError::RoleCycleDetected(r) => {
                Self::Invalid(format!("role cycle detected involving: {r}"))
            }
            SchemaError::InvalidSchema(msg) => Self::ConfigError(msg),
            _ => {
                tracing::warn!("unmapped schema error variant: {err}");
                Self::ServerError(format!("schema error: {err}"))
            }
        }
    }
}

impl From<ferrosa_common::Error> for CqlError {
    fn from(err: ferrosa_common::Error) -> Self {
        Self::ServerError(format!("storage error: {err}"))
    }
}

impl From<std::io::Error> for CqlError {
    fn from(err: std::io::Error) -> Self {
        Self::ServerError(format!("I/O error: {err}"))
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-cql -- error`
Expected: PASS (all 5 tests)

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/error.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add CqlError enum with protocol error codes and From impls"
```

---

## Chunk 2: CQL Type System

### Task 5: CqlType enum (type IDs)

**Files:**

- Create: `ferrosa-cql/src/types.rs`
- Modify: `ferrosa-cql/src/lib.rs` (add `pub mod types;`)
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing tests for CqlType**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_id_roundtrip() {
        let types = [
            (0x0001, CqlType::Ascii),
            (0x0002, CqlType::Bigint),
            (0x0003, CqlType::Blob),
            (0x0004, CqlType::Boolean),
            (0x0005, CqlType::Counter),
            (0x0006, CqlType::Decimal),
            (0x0007, CqlType::Double),
            (0x0008, CqlType::Float),
            (0x0009, CqlType::Int),
            (0x000B, CqlType::Timestamp),
            (0x000C, CqlType::Uuid),
            (0x000D, CqlType::Varchar),
            (0x000E, CqlType::Varint),
            (0x000F, CqlType::Timeuuid),
            (0x0010, CqlType::Inet),
            (0x0011, CqlType::Date),
            (0x0012, CqlType::Time),
            (0x0013, CqlType::Smallint),
            (0x0014, CqlType::Tinyint),
            (0x0020, CqlType::List(Box::new(CqlType::Int))),
            (0x0021, CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int))),
            (0x0022, CqlType::Set(Box::new(CqlType::Uuid))),
        ];
        for &(id, ref expected_variant) in &types {
            // For non-collection types, verify the base ID
            if !matches!(expected_variant, CqlType::List(_) | CqlType::Map(_, _) | CqlType::Set(_)) {
                assert_eq!(expected_variant.type_id(), id);
            }
        }
    }

    #[test]
    fn type_id_for_collections() {
        assert_eq!(CqlType::List(Box::new(CqlType::Int)).type_id(), 0x0020);
        assert_eq!(
            CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int)).type_id(),
            0x0021
        );
        assert_eq!(CqlType::Set(Box::new(CqlType::Uuid)).type_id(), 0x0022);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-cql -- types`
Expected: FAIL — `CqlType` not defined

- [ ] **Step 3: Implement CqlType**

```rust
//! CQL type system: type identifiers, value encoding, and decoding.
//!
//! The CQL native protocol assigns a 16-bit type ID to each data type.
//! Collection types (list, map, set) carry their element type IDs inline.
//! `CqlValue` is the runtime representation used throughout query execution.

/// CQL data type with protocol type ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CqlType {
    Ascii,      // 0x0001
    Bigint,     // 0x0002
    Blob,       // 0x0003
    Boolean,    // 0x0004
    Counter,    // 0x0005
    Decimal,    // 0x0006
    Double,     // 0x0007
    Float,      // 0x0008
    Int,        // 0x0009
    // 0x000A = custom, not supported
    Timestamp,  // 0x000B
    Uuid,       // 0x000C
    Varchar,    // 0x000D
    Varint,     // 0x000E
    Timeuuid,   // 0x000F
    Inet,       // 0x0010
    Date,       // 0x0011
    Time,       // 0x0012
    Smallint,   // 0x0013
    Tinyint,    // 0x0014
    // 0x0015 = duration, deferred
    // 0x0030 = UDT, deferred
    List(Box<CqlType>),              // 0x0020
    Map(Box<CqlType>, Box<CqlType>), // 0x0021
    Set(Box<CqlType>),               // 0x0022
    Tuple(Vec<CqlType>),             // 0x0031
}

impl CqlType {
    /// Returns the protocol type ID for this type.
    pub fn type_id(&self) -> u16 {
        match self {
            Self::Ascii => 0x0001,
            Self::Bigint => 0x0002,
            Self::Blob => 0x0003,
            Self::Boolean => 0x0004,
            Self::Counter => 0x0005,
            Self::Decimal => 0x0006,
            Self::Double => 0x0007,
            Self::Float => 0x0008,
            Self::Int => 0x0009,
            Self::Timestamp => 0x000B,
            Self::Uuid => 0x000C,
            Self::Varchar => 0x000D,
            Self::Varint => 0x000E,
            Self::Timeuuid => 0x000F,
            Self::Inet => 0x0010,
            Self::Date => 0x0011,
            Self::Time => 0x0012,
            Self::Smallint => 0x0013,
            Self::Tinyint => 0x0014,
            Self::List(_) => 0x0020,
            Self::Map(_, _) => 0x0021,
            Self::Set(_) => 0x0022,
            Self::Tuple(_) => 0x0031,
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-cql -- types`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/types.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add CqlType enum with protocol type IDs"
```

---

### Task 6: CqlValue enum — scalar encode/decode

**Files:**

- Modify: `ferrosa-cql/src/types.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing tests for CqlValue scalar encode/decode**

```rust
#[test]
fn encode_decode_int() {
    let val = CqlValue::Int(42);
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Int, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_bigint() {
    let val = CqlValue::Bigint(i64::MAX);
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Bigint, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_text() {
    let val = CqlValue::Text("hello world".to_string());
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Varchar, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_boolean_true() {
    let val = CqlValue::Boolean(true);
    let bytes = val.encode_value();
    assert_eq!(bytes, vec![1]);
    let decoded = CqlValue::decode_value(&CqlType::Boolean, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_boolean_false() {
    let val = CqlValue::Boolean(false);
    let bytes = val.encode_value();
    assert_eq!(bytes, vec![0]);
}

#[test]
fn encode_decode_float() {
    let val = CqlValue::Float(3.14f32.to_bits());
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Float, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_double() {
    let val = CqlValue::Double(std::f64::consts::PI.to_bits());
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Double, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_uuid() {
    let id = uuid::Uuid::new_v4();
    let val = CqlValue::Uuid(id);
    let bytes = val.encode_value();
    assert_eq!(bytes.len(), 16);
    let decoded = CqlValue::decode_value(&CqlType::Uuid, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_blob() {
    let val = CqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Blob, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_inet_v4() {
    let val = CqlValue::Inet(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)));
    let bytes = val.encode_value();
    assert_eq!(bytes.len(), 4);
    let decoded = CqlValue::decode_value(&CqlType::Inet, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_inet_v6() {
    let val = CqlValue::Inet(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST));
    let bytes = val.encode_value();
    assert_eq!(bytes.len(), 16);
    let decoded = CqlValue::decode_value(&CqlType::Inet, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_smallint() {
    let val = CqlValue::Smallint(-1234);
    let bytes = val.encode_value();
    assert_eq!(bytes.len(), 2);
    let decoded = CqlValue::decode_value(&CqlType::Smallint, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_tinyint() {
    let val = CqlValue::Tinyint(-42);
    let bytes = val.encode_value();
    assert_eq!(bytes.len(), 1);
    let decoded = CqlValue::decode_value(&CqlType::Tinyint, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_date() {
    let val = CqlValue::Date(19000); // some day
    let bytes = val.encode_value();
    assert_eq!(bytes.len(), 4);
    let decoded = CqlValue::decode_value(&CqlType::Date, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_time() {
    let nanos: i64 = 12 * 3_600_000_000_000 + 30 * 60_000_000_000; // 12:30:00
    let val = CqlValue::Time(nanos);
    let bytes = val.encode_value();
    assert_eq!(bytes.len(), 8);
    let decoded = CqlValue::decode_value(&CqlType::Time, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_timestamp() {
    let val = CqlValue::Timestamp(1710000000000); // some unix millis
    let bytes = val.encode_value();
    assert_eq!(bytes.len(), 8);
    let decoded = CqlValue::decode_value(&CqlType::Timestamp, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_varint() {
    use num_bigint::BigInt;
    let val = CqlValue::Varint(BigInt::from(123456789i64));
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Varint, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_varint_negative() {
    use num_bigint::BigInt;
    let val = CqlValue::Varint(BigInt::from(-1i64));
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Varint, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_decimal() {
    use num_bigint::BigInt;
    let val = CqlValue::Decimal {
        scale: 2,
        unscaled: BigInt::from(12345i64),
    };
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Decimal, &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_ascii() {
    let val = CqlValue::Ascii("hello".into());
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Ascii, &bytes).unwrap();
    assert_eq!(decoded, val);
    // Verify it returns Ascii, not Text
    assert!(matches!(decoded, CqlValue::Ascii(_)));
}

#[test]
fn encode_decode_counter() {
    let val = CqlValue::Counter(42);
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Counter, &bytes).unwrap();
    assert_eq!(decoded, val);
    // Verify it returns Counter, not Bigint
    assert!(matches!(decoded, CqlValue::Counter(_)));
}

#[test]
fn encode_decode_timeuuid() {
    let val = CqlValue::Timeuuid(uuid::Uuid::new_v4());
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(&CqlType::Timeuuid, &bytes).unwrap();
    assert_eq!(decoded, val);
    // Verify it returns Timeuuid, not Uuid
    assert!(matches!(decoded, CqlValue::Timeuuid(_)));
}

#[test]
fn float_ord_uses_total_ordering() {
    // -1.0 should sort before 1.0
    let neg = CqlValue::Float((-1.0f32).to_bits());
    let pos = CqlValue::Float(1.0f32.to_bits());
    assert!(neg < pos);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-cql -- types`
Expected: FAIL — `CqlValue` not defined

- [ ] **Step 3: Implement CqlValue enum and scalar encode/decode**

Add to `types.rs`:

```rust
use std::net::IpAddr;

use num_bigint::BigInt;

use crate::error::CqlError;

/// A CQL value at runtime.
///
/// Covers all scalar and collection types. Float/Double store raw bits
/// as u32/u64 so `Eq` can be derived. `Ord` is implemented manually
/// using `f32::total_cmp`/`f64::total_cmp` for IEEE 754 total ordering.
///
/// Note: `Null` is signaled out-of-band via the CQL wire protocol
/// length prefix (-1). `encode_value` for `Null` returns an empty vec;
/// callers are responsible for writing the -1 length prefix when encoding
/// a null cell. `decode_value` is never called for null (the caller
/// checks the length prefix first).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CqlValue {
    Null,
    Ascii(String),
    Bigint(i64),
    Blob(Vec<u8>),
    Boolean(bool),
    Counter(i64),
    Decimal { scale: i32, unscaled: BigInt },
    Double(u64),  // f64 bits for Eq/Ord
    Float(u32),   // f32 bits for Eq/Ord
    Int(i32),
    Timestamp(i64),
    Uuid(uuid::Uuid),
    Text(String),   // varchar
    Varint(BigInt),
    Timeuuid(uuid::Uuid),
    Inet(IpAddr),
    Date(u32),
    Time(i64),
    Smallint(i16),
    Tinyint(i8),
    // Collections added in Task 7
}

impl PartialOrd for CqlValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CqlValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        // Compare by discriminant first for cross-type ordering
        let d_self = std::mem::discriminant(self);
        let d_other = std::mem::discriminant(other);
        if d_self != d_other {
            // Use discriminant index for stable cross-type ordering.
            // This is a fallback — in practice, CQL comparisons are
            // always same-type within a column.
            return self.discriminant_index().cmp(&other.discriminant_index());
        }
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Ascii(a), Self::Ascii(b)) | (Self::Text(a), Self::Text(b)) => a.cmp(b),
            (Self::Bigint(a), Self::Bigint(b))
            | (Self::Counter(a), Self::Counter(b))
            | (Self::Timestamp(a), Self::Timestamp(b))
            | (Self::Time(a), Self::Time(b)) => a.cmp(b),
            (Self::Int(a), Self::Int(b)) => a.cmp(b),
            (Self::Smallint(a), Self::Smallint(b)) => a.cmp(b),
            (Self::Tinyint(a), Self::Tinyint(b)) => a.cmp(b),
            (Self::Boolean(a), Self::Boolean(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => {
                f32::from_bits(*a).total_cmp(&f32::from_bits(*b))
            }
            (Self::Double(a), Self::Double(b)) => {
                f64::from_bits(*a).total_cmp(&f64::from_bits(*b))
            }
            (Self::Blob(a), Self::Blob(b)) => a.cmp(b),
            (Self::Uuid(a), Self::Uuid(b))
            | (Self::Timeuuid(a), Self::Timeuuid(b)) => a.cmp(b),
            (Self::Inet(a), Self::Inet(b)) => a.to_string().cmp(&b.to_string()),
            (Self::Date(a), Self::Date(b)) => a.cmp(b),
            (Self::Varint(a), Self::Varint(b)) => a.cmp(b),
            (Self::Decimal { scale: sa, unscaled: ua },
             Self::Decimal { scale: sb, unscaled: ub }) => {
                sa.cmp(sb).then_with(|| ua.cmp(ub))
            }
            _ => Ordering::Equal, // same discriminant, unreachable
        }
    }
}

impl CqlValue {
    /// Discriminant index for cross-type ordering.
    fn discriminant_index(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Ascii(_) => 1,
            Self::Bigint(_) => 2,
            Self::Blob(_) => 3,
            Self::Boolean(_) => 4,
            Self::Counter(_) => 5,
            Self::Decimal { .. } => 6,
            Self::Double(_) => 7,
            Self::Float(_) => 8,
            Self::Int(_) => 9,
            Self::Timestamp(_) => 10,
            Self::Uuid(_) => 11,
            Self::Text(_) => 12,
            Self::Varint(_) => 13,
            Self::Timeuuid(_) => 14,
            Self::Inet(_) => 15,
            Self::Date(_) => 16,
            Self::Time(_) => 17,
            Self::Smallint(_) => 18,
            Self::Tinyint(_) => 19,
            // Collections use 20+ (added in Task 7)
            #[allow(unreachable_patterns)]
            _ => 255,
        }
    }
}

impl CqlValue {
    /// Encode this value as CQL wire-format bytes (no length prefix).
    pub fn encode_value(&self) -> Vec<u8> {
        match self {
            Self::Ascii(s) | Self::Text(s) => s.as_bytes().to_vec(),
            Self::Bigint(n) | Self::Counter(n) | Self::Timestamp(n) => n.to_be_bytes().to_vec(),
            Self::Blob(b) => b.clone(),
            Self::Boolean(b) => vec![if *b { 1 } else { 0 }],
            Self::Decimal { scale, unscaled } => {
                let mut buf = scale.to_be_bytes().to_vec();
                buf.extend_from_slice(&unscaled.to_signed_bytes_be());
                buf
            }
            Self::Double(bits) => bits.to_be_bytes().to_vec(),
            Self::Float(bits) => bits.to_be_bytes().to_vec(),
            Self::Int(n) => n.to_be_bytes().to_vec(),
            Self::Uuid(u) | Self::Timeuuid(u) => u.as_bytes().to_vec(),
            Self::Varint(n) => n.to_signed_bytes_be(),
            Self::Inet(ip) => match ip {
                IpAddr::V4(v4) => v4.octets().to_vec(),
                IpAddr::V6(v6) => v6.octets().to_vec(),
            },
            Self::Date(d) => d.to_be_bytes().to_vec(),
            Self::Time(t) => t.to_be_bytes().to_vec(),
            Self::Smallint(n) => n.to_be_bytes().to_vec(),
            Self::Tinyint(n) => n.to_be_bytes().to_vec(),
            Self::Null => vec![],
        }
    }

    /// Decode a value from CQL wire-format bytes given its type.
    pub fn decode_value(cql_type: &CqlType, bytes: &[u8]) -> Result<Self, CqlError> {
        match cql_type {
            CqlType::Ascii => Ok(Self::Ascii(
                String::from_utf8(bytes.to_vec())
                    .map_err(|e| CqlError::Invalid(format!("invalid ASCII: {e}")))?,
            )),
            CqlType::Varchar => Ok(Self::Text(
                String::from_utf8(bytes.to_vec())
                    .map_err(|e| CqlError::Invalid(format!("invalid UTF-8: {e}")))?,
            )),
            CqlType::Bigint => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("bigint requires 8 bytes".into()))?;
                Ok(Self::Bigint(i64::from_be_bytes(arr)))
            }
            CqlType::Counter => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("counter requires 8 bytes".into()))?;
                Ok(Self::Counter(i64::from_be_bytes(arr)))
            }
            CqlType::Blob => Ok(Self::Blob(bytes.to_vec())),
            CqlType::Boolean => {
                if bytes.len() != 1 {
                    return Err(CqlError::Invalid("boolean requires 1 byte".into()));
                }
                Ok(Self::Boolean(bytes[0] != 0))
            }
            CqlType::Double => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("double requires 8 bytes".into()))?;
                Ok(Self::Double(u64::from_be_bytes(arr)))
            }
            CqlType::Float => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("float requires 4 bytes".into()))?;
                Ok(Self::Float(u32::from_be_bytes(arr)))
            }
            CqlType::Int => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("int requires 4 bytes".into()))?;
                Ok(Self::Int(i32::from_be_bytes(arr)))
            }
            CqlType::Timestamp => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("timestamp requires 8 bytes".into()))?;
                Ok(Self::Timestamp(i64::from_be_bytes(arr)))
            }
            CqlType::Uuid => {
                if bytes.len() != 16 {
                    return Err(CqlError::Invalid("uuid requires 16 bytes".into()));
                }
                Ok(Self::Uuid(uuid::Uuid::from_slice(bytes).map_err(|e| {
                    CqlError::Invalid(format!("invalid uuid: {e}"))
                })?))
            }
            CqlType::Timeuuid => {
                if bytes.len() != 16 {
                    return Err(CqlError::Invalid("timeuuid requires 16 bytes".into()));
                }
                Ok(Self::Timeuuid(uuid::Uuid::from_slice(bytes).map_err(
                    |e| CqlError::Invalid(format!("invalid timeuuid: {e}")),
                )?))
            }
            CqlType::Inet => match bytes.len() {
                4 => {
                    let arr: [u8; 4] = bytes.try_into().unwrap();
                    Ok(Self::Inet(IpAddr::V4(arr.into())))
                }
                16 => {
                    let arr: [u8; 16] = bytes.try_into().unwrap();
                    Ok(Self::Inet(IpAddr::V6(arr.into())))
                }
                n => Err(CqlError::Invalid(format!(
                    "inet requires 4 or 16 bytes, got {n}"
                ))),
            },
            CqlType::Date => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("date requires 4 bytes".into()))?;
                Ok(Self::Date(u32::from_be_bytes(arr)))
            }
            CqlType::Time => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("time requires 8 bytes".into()))?;
                Ok(Self::Time(i64::from_be_bytes(arr)))
            }
            CqlType::Smallint => {
                let arr: [u8; 2] = bytes
                    .try_into()
                    .map_err(|_| CqlError::Invalid("smallint requires 2 bytes".into()))?;
                Ok(Self::Smallint(i16::from_be_bytes(arr)))
            }
            CqlType::Tinyint => {
                if bytes.len() != 1 {
                    return Err(CqlError::Invalid("tinyint requires 1 byte".into()));
                }
                Ok(Self::Tinyint(bytes[0] as i8))
            }
            CqlType::Varint => {
                Ok(Self::Varint(BigInt::from_signed_bytes_be(bytes)))
            }
            CqlType::Decimal => {
                if bytes.len() < 4 {
                    return Err(CqlError::Invalid("decimal requires at least 4 bytes".into()));
                }
                let scale = i32::from_be_bytes(bytes[..4].try_into().unwrap());
                let unscaled = BigInt::from_signed_bytes_be(&bytes[4..]);
                Ok(Self::Decimal { scale, unscaled })
            }
            _ => Err(CqlError::Invalid(format!(
                "unsupported type for decode: {:?}",
                cql_type
            ))),
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-cql -- types`
Expected: PASS (all scalar encode/decode tests)

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/types.rs
git commit -m "feat(cql): add CqlValue enum with scalar encode/decode for all CQL types"
```

---

### Task 7: CqlValue collection encode/decode

**Files:**

- Modify: `ferrosa-cql/src/types.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing tests for collection encode/decode**

```rust
#[test]
fn encode_decode_list_of_ints() {
    let val = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)]);
    let bytes = val.encode_value();
    let decoded =
        CqlValue::decode_value(&CqlType::List(Box::new(CqlType::Int)), &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_empty_list() {
    let val = CqlValue::List(vec![]);
    let bytes = val.encode_value();
    let decoded =
        CqlValue::decode_value(&CqlType::List(Box::new(CqlType::Int)), &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_set_of_text() {
    let val = CqlValue::Set(vec![
        CqlValue::Text("a".into()),
        CqlValue::Text("b".into()),
    ]);
    let bytes = val.encode_value();
    let decoded =
        CqlValue::decode_value(&CqlType::Set(Box::new(CqlType::Varchar)), &bytes).unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_map_text_to_int() {
    let val = CqlValue::Map(vec![
        (CqlValue::Text("x".into()), CqlValue::Int(10)),
        (CqlValue::Text("y".into()), CqlValue::Int(20)),
    ]);
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(
        &CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int)),
        &bytes,
    )
    .unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_nested_list_of_maps() {
    let inner = CqlValue::Map(vec![(CqlValue::Text("k".into()), CqlValue::Int(1))]);
    let val = CqlValue::List(vec![inner]);
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(
        &CqlType::List(Box::new(CqlType::Map(
            Box::new(CqlType::Varchar),
            Box::new(CqlType::Int),
        ))),
        &bytes,
    )
    .unwrap();
    assert_eq!(decoded, val);
}

#[test]
fn encode_decode_tuple() {
    let val = CqlValue::Tuple(vec![
        Some(CqlValue::Int(42)),
        None, // null element
        Some(CqlValue::Text("hello".into())),
    ]);
    let bytes = val.encode_value();
    let decoded = CqlValue::decode_value(
        &CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar, CqlType::Varchar]),
        &bytes,
    )
    .unwrap();
    assert_eq!(decoded, val);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-cql -- types::tests::encode_decode_list`
Expected: FAIL — `CqlValue::List` etc. not defined

- [ ] **Step 3: Add collection variants and encode/decode**

Add variants to `CqlValue`:

```rust
/// Ordered list of values.
List(Vec<CqlValue>),
/// Set of values. Uses Vec (not BTreeSet) to preserve exact wire order
/// without re-sorting. The CQL protocol sends sets pre-sorted and
/// pre-deduplicated. The bridge layer converts to BTreeSet if needed.
Set(Vec<CqlValue>),
/// Map of key-value pairs. Uses Vec (not BTreeMap) to preserve wire
/// order. Same rationale as Set.
Map(Vec<(CqlValue, CqlValue)>),
/// Tuple — fixed number of typed elements, some potentially null.
Tuple(Vec<Option<CqlValue>>),
```

Add to `encode_value`:

```rust
Self::List(items) | Self::Set(items) => {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(items.len() as i32).to_be_bytes());
    for item in items {
        let encoded = item.encode_value();
        buf.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
        buf.extend_from_slice(&encoded);
    }
    buf
}
Self::Map(entries) => {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(entries.len() as i32).to_be_bytes());
    for (k, v) in entries {
        let ek = k.encode_value();
        buf.extend_from_slice(&(ek.len() as i32).to_be_bytes());
        buf.extend_from_slice(&ek);
        let ev = v.encode_value();
        buf.extend_from_slice(&(ev.len() as i32).to_be_bytes());
        buf.extend_from_slice(&ev);
    }
    buf
}
Self::Tuple(elements) => {
    let mut buf = Vec::new();
    for elem in elements {
        match elem {
            Some(val) => {
                let encoded = val.encode_value();
                buf.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
                buf.extend_from_slice(&encoded);
            }
            None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    buf
}
```

Add to `decode_value`:

```rust
CqlType::List(elem_type) => {
    let (count, mut pos) = read_collection_header(bytes)?;
    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (val, next_pos) = read_collection_element(bytes, pos, elem_type)?;
        items.push(val);
        pos = next_pos;
    }
    Ok(Self::List(items))
}
CqlType::Set(elem_type) => {
    let (count, mut pos) = read_collection_header(bytes)?;
    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (val, next_pos) = read_collection_element(bytes, pos, elem_type)?;
        items.push(val);
        pos = next_pos;
    }
    Ok(Self::Set(items))
}
CqlType::Map(key_type, val_type) => {
    let (count, mut pos) = read_collection_header(bytes)?;
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (k, kpos) = read_collection_element(bytes, pos, key_type)?;
        let (v, vpos) = read_collection_element(bytes, kpos, val_type)?;
        entries.push((k, v));
        pos = vpos;
    }
    Ok(Self::Map(entries))
}
CqlType::Tuple(elem_types) => {
    let mut elements = Vec::with_capacity(elem_types.len());
    let mut pos = 0;
    for et in elem_types {
        if pos + 4 > bytes.len() {
            return Err(CqlError::Invalid("tuple truncated".into()));
        }
        let len = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap());
        pos += 4;
        if len < 0 {
            elements.push(None);
        } else {
            let end = pos + len as usize;
            if end > bytes.len() {
                return Err(CqlError::Invalid("tuple element truncated".into()));
            }
            elements.push(Some(CqlValue::decode_value(et, &bytes[pos..end])?));
            pos = end;
        }
    }
    Ok(Self::Tuple(elements))
}
```

Helper functions:

```rust
/// Read the 4-byte element count from a collection header.
fn read_collection_header(bytes: &[u8]) -> Result<(i32, usize), CqlError> {
    if bytes.len() < 4 {
        return Err(CqlError::Invalid("collection too short for count".into()));
    }
    let count = i32::from_be_bytes(bytes[..4].try_into().unwrap());
    if count < 0 {
        return Err(CqlError::Invalid("negative collection count".into()));
    }
    Ok((count, 4))
}

/// Read one length-prefixed element from a collection at `pos`.
fn read_collection_element(
    bytes: &[u8],
    pos: usize,
    elem_type: &CqlType,
) -> Result<(CqlValue, usize), CqlError> {
    if pos + 4 > bytes.len() {
        return Err(CqlError::Invalid("collection truncated at element length".into()));
    }
    let len = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap());
    if len < 0 {
        return Ok((CqlValue::Null, pos + 4));
    }
    let len = len as usize;
    let end = pos + 4 + len;
    if end > bytes.len() {
        return Err(CqlError::Invalid("collection truncated at element data".into()));
    }
    let val = CqlValue::decode_value(elem_type, &bytes[pos + 4..end])?;
    Ok((val, end))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-cql -- types`
Expected: PASS (all tests including collections)

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/types.rs
git commit -m "feat(cql): add collection encode/decode for List, Set, Map with nesting"
```

---

### Task 8: CqlValue property tests

**Files:**

- Modify: `ferrosa-cql/src/types.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write proptest for scalar round-trip**

```rust
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn arb_scalar_value() -> impl Strategy<Value = (CqlType, CqlValue)> {
        prop_oneof![
            any::<i32>().prop_map(|n| (CqlType::Int, CqlValue::Int(n))),
            any::<i64>().prop_map(|n| (CqlType::Bigint, CqlValue::Bigint(n))),
            any::<i64>().prop_map(|n| (CqlType::Counter, CqlValue::Counter(n))),
            any::<i16>().prop_map(|n| (CqlType::Smallint, CqlValue::Smallint(n))),
            any::<i8>().prop_map(|n| (CqlType::Tinyint, CqlValue::Tinyint(n))),
            any::<bool>().prop_map(|b| (CqlType::Boolean, CqlValue::Boolean(b))),
            any::<u32>().prop_map(|n| (CqlType::Float, CqlValue::Float(n))),
            any::<u64>().prop_map(|n| (CqlType::Double, CqlValue::Double(n))),
            any::<u32>().prop_map(|n| (CqlType::Date, CqlValue::Date(n))),
            any::<i64>().prop_map(|n| (CqlType::Time, CqlValue::Time(n))),
            any::<i64>().prop_map(|n| (CqlType::Timestamp, CqlValue::Timestamp(n))),
            "[ -~]{0,100}".prop_map(|s| (CqlType::Varchar, CqlValue::Text(s))),
            "[ -~]{0,100}".prop_map(|s| (CqlType::Ascii, CqlValue::Ascii(s))),
            prop::collection::vec(any::<u8>(), 0..100)
                .prop_map(|b| (CqlType::Blob, CqlValue::Blob(b))),
            prop::array::uniform16(any::<u8>())
                .prop_map(|b| (CqlType::Uuid, CqlValue::Uuid(uuid::Uuid::from_bytes(b)))),
            prop::array::uniform16(any::<u8>())
                .prop_map(|b| (CqlType::Timeuuid, CqlValue::Timeuuid(uuid::Uuid::from_bytes(b)))),
            (0..4u8).prop_map(|v| {
                let ip: IpAddr = if v < 2 {
                    IpAddr::V4(std::net::Ipv4Addr::new(v, v, v, v))
                } else {
                    IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
                };
                (CqlType::Inet, CqlValue::Inet(ip))
            }),
            any::<i64>().prop_map(|n| {
                use num_bigint::BigInt;
                (CqlType::Varint, CqlValue::Varint(BigInt::from(n)))
            }),
        ]
    }

    proptest! {
        #[test]
        fn scalar_roundtrip((cql_type, value) in arb_scalar_value()) {
            let encoded = value.encode_value();
            let decoded = CqlValue::decode_value(&cql_type, &encoded).unwrap();
            prop_assert_eq!(decoded, value);
        }
    }
}
```

- [ ] **Step 2: Run proptests**

Run: `cargo test -p ferrosa-cql -- proptests`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add ferrosa-cql/src/types.rs
git commit -m "test(cql): add proptest for CqlValue scalar round-trip"
```

---

## Chunk 3: Auth + Server

### Task 9: Auth handshake module

**Files:**

- Create: `ferrosa-cql/src/auth.rs`
- Modify: `ferrosa-cql/src/lib.rs` (add `pub mod auth;`)
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing tests for SASL PLAIN parsing**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sasl_plain_valid() {
        // SASL PLAIN format: \0username\0password
        let payload = b"\0cassandra\0cassandra";
        let (user, pass) = parse_sasl_plain(payload).unwrap();
        assert_eq!(user, "cassandra");
        assert_eq!(pass, "cassandra");
    }

    #[test]
    fn parse_sasl_plain_with_authzid() {
        // SASL PLAIN: authzid\0username\0password (authzid ignored)
        let payload = b"ignored\0user\0pass";
        let (user, pass) = parse_sasl_plain(payload).unwrap();
        assert_eq!(user, "user");
        assert_eq!(pass, "pass");
    }

    #[test]
    fn parse_sasl_plain_empty_password() {
        let payload = b"\0user\0";
        let (user, pass) = parse_sasl_plain(payload).unwrap();
        assert_eq!(user, "user");
        assert_eq!(pass, "");
    }

    #[test]
    fn parse_sasl_plain_no_null() {
        let payload = b"no nulls here";
        assert!(parse_sasl_plain(payload).is_err());
    }

    #[test]
    fn authenticator_class_name() {
        assert_eq!(
            AUTHENTICATOR_CLASS,
            "org.apache.cassandra.auth.PasswordAuthenticator"
        );
    }

    #[test]
    fn encode_authenticate_body() {
        let body = encode_authenticate_response();
        // [2-byte string length][authenticator class string]
        let len = u16::from_be_bytes([body[0], body[1]]) as usize;
        let class = std::str::from_utf8(&body[2..2 + len]).unwrap();
        assert_eq!(class, AUTHENTICATOR_CLASS);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-cql -- auth`
Expected: FAIL — module not defined

- [ ] **Step 3: Implement auth module**

```rust
//! SASL PLAIN authentication for CQL connections.
//!
//! The auth flow: server sends AUTHENTICATE with the authenticator class
//! name, client responds with AUTH_RESPONSE containing a SASL PLAIN
//! payload (`\0username\0password`), server validates via
//! `ferrosa-schema::Schema::authenticate()`.

use bytes::BufMut;

use crate::error::CqlError;

/// The authenticator class name sent to drivers.
pub const AUTHENTICATOR_CLASS: &str =
    "org.apache.cassandra.auth.PasswordAuthenticator";

/// Maximum auth attempts before closing the connection.
pub const MAX_AUTH_ATTEMPTS: u32 = 3;

/// Parse a SASL PLAIN payload into (username, password).
///
/// SASL PLAIN format: `[authzid]\0<username>\0<password>`
/// The authzid (authorization identity) is ignored.
pub fn parse_sasl_plain(payload: &[u8]) -> Result<(&str, &str), CqlError> {
    // Find the two null separators.
    let mut nulls = payload
        .iter()
        .enumerate()
        .filter(|(_, &b)| b == 0)
        .map(|(i, _)| i);

    let first = nulls
        .next()
        .ok_or_else(|| CqlError::Protocol("SASL PLAIN: missing null separator".into()))?;
    let second = nulls
        .next()
        .ok_or_else(|| CqlError::Protocol("SASL PLAIN: missing second null separator".into()))?;

    let username = std::str::from_utf8(&payload[first + 1..second])
        .map_err(|e| CqlError::Protocol(format!("SASL PLAIN: invalid UTF-8 username: {e}")))?;
    let password = std::str::from_utf8(&payload[second + 1..])
        .map_err(|e| CqlError::Protocol(format!("SASL PLAIN: invalid UTF-8 password: {e}")))?;

    Ok((username, password))
}

/// Encode the body of an AUTHENTICATE response frame.
///
/// Format: `[string authenticator_class]`
pub fn encode_authenticate_response() -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + AUTHENTICATOR_CLASS.len());
    buf.put_u16(AUTHENTICATOR_CLASS.len() as u16);
    buf.extend_from_slice(AUTHENTICATOR_CLASS.as_bytes());
    buf
}

/// Encode the body of an AUTH_SUCCESS response frame (empty token).
pub fn encode_auth_success() -> Vec<u8> {
    // [int length][-1 for null token]
    (-1i32).to_be_bytes().to_vec()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-cql -- auth`
Expected: PASS (all 6 tests)

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/auth.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add SASL PLAIN auth parsing and AUTHENTICATE response encoding"
```

---

### Task 10: TCP server skeleton

**Files:**

- Create: `ferrosa-cql/src/server.rs`
- Create: `ferrosa-cql/src/connection.rs`
- Modify: `ferrosa-cql/src/lib.rs`
- Test: inline `#[cfg(test)]` in `server.rs`

- [ ] **Step 1: Write failing test for server startup**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;
    use crate::frame::{FrameHeader, Opcode, HEADER_SIZE};

    #[tokio::test]
    async fn server_accepts_connection() {
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 10,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_in_flight_per_connection: 128,
            auth_disabled: false,
        };
        let server = CqlServer::new(config);
        let addr = server.start_background().await.unwrap();

        // Connect and verify we can establish TCP
        let _stream = TcpStream::connect(addr).await.unwrap();
    }

    #[tokio::test]
    async fn server_rejects_over_limit_with_overloaded() {
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 1,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_in_flight_per_connection: 128,
            auth_disabled: false,
        };
        let server = CqlServer::new(config);
        let addr = server.start_background().await.unwrap();

        // First connection succeeds
        let _conn1 = TcpStream::connect(addr).await.unwrap();
        // Give the accept loop time to register
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Second connection should get ERROR(Overloaded)
        let mut conn2 = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut buf = vec![0u8; 256];
        let n = conn2.read(&mut buf).await.unwrap();
        // Should receive a frame with Opcode::Error
        assert!(n >= HEADER_SIZE);
        let header = FrameHeader::decode(&buf[..HEADER_SIZE]).unwrap();
        assert_eq!(header.opcode, Opcode::Error);
        // Error code should be 0x1100 (Overloaded)
        let error_code = i32::from_be_bytes(buf[HEADER_SIZE..HEADER_SIZE + 4].try_into().unwrap());
        assert_eq!(error_code, 0x1100);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-cql -- server`
Expected: FAIL — `CqlServer` not defined

- [ ] **Step 3: Implement server.rs**

```rust
//! CQL TCP server: accepts connections and spawns per-connection tasks.
//!
//! `CqlServer` binds to a TCP address and spawns a Tokio task per
//! incoming connection. Each task runs the connection lifecycle
//! (auth handshake → query loop) defined in `connection.rs`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::SinkExt;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tracing::{info, warn};

use crate::error::CqlError;
use crate::frame::{
    CqlCodec, CqlFrame, FrameHeader, Opcode, DEFAULT_MAX_FRAME_SIZE, VERSION_RESPONSE,
};

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
    pub max_frame_size: u32,
    /// Max concurrent in-flight requests per connection (default 128).
    /// TODO: Enforce in connection handler (Part B) — reject with ERROR(Overloaded).
    pub max_in_flight_per_connection: usize,
    /// If true, skip auth (STARTUP returns READY directly).
    pub auth_disabled: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9042".parse().unwrap(),
            max_connections: 1024,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_in_flight_per_connection: 128,
            auth_disabled: false,
        }
    }
}

/// CQL protocol server.
pub struct CqlServer {
    config: ServerConfig,
    active_connections: Arc<AtomicUsize>,
}

impl CqlServer {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Start the server in the background. Returns the bound address.
    pub async fn start_background(&self) -> Result<SocketAddr, CqlError> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        let addr = listener.local_addr()?;
        let max_connections = self.config.max_connections;
        let max_frame_size = self.config.max_frame_size;
        let auth_disabled = self.config.auth_disabled;
        let active = self.active_connections.clone();

        info!("CQL server listening on {addr}");

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let current = active.fetch_add(1, Ordering::Relaxed);
                        if current >= max_connections {
                            active.fetch_sub(1, Ordering::Relaxed);
                            warn!("connection limit reached, rejecting {peer}");
                            // Send ERROR(Overloaded) per spec before closing
                            let codec = CqlCodec::new(max_frame_size);
                            let mut framed = Framed::new(stream, codec);
                            let err = CqlError::Overloaded;
                            let body = err.encode_body().freeze();
                            let frame = CqlFrame {
                                header: FrameHeader {
                                    version: VERSION_RESPONSE,
                                    flags: 0,
                                    stream_id: -1,
                                    opcode: Opcode::Error,
                                    length: 0,
                                },
                                body,
                            };
                            let _ = framed.send(frame).await;
                            continue;
                        }
                        let active = active.clone();
                        let auth_disabled = auth_disabled;
                        tokio::spawn(async move {
                            crate::connection::handle_connection(
                                stream,
                                peer,
                                max_frame_size,
                                auth_disabled,
                            )
                            .await;
                            active.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                    Err(e) => {
                        warn!("accept error: {e}");
                    }
                }
            }
        });

        Ok(addr)
    }

    /// Returns the number of active connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }
}
```

- [ ] **Step 4: Implement connection.rs stub**

```rust
//! Per-connection CQL protocol handler.
//!
//! Each connection runs as an independent Tokio task. It reads frames
//! via `CqlCodec`, performs the auth handshake, then enters the query
//! loop. All state (auth context, current keyspace) is connection-local.

use std::net::SocketAddr;

use tokio::net::TcpStream;
use tracing::debug;

/// Handle a single CQL connection.
///
/// This is the entry point for each connection task. It sets up the
/// codec, runs the auth handshake, and enters the query loop.
/// Part A implements only the STARTUP → AUTHENTICATE → AUTH_SUCCESS
/// handshake. The query loop is added in Part B.
pub async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    _max_frame_size: u32,
    _auth_disabled: bool,
) {
    debug!("new connection from {peer}");

    // Part A: stub — just accept and close.
    // Part B will add the full protocol handler.
    drop(stream);

    debug!("connection from {peer} closed");
}
```

- [ ] **Step 5: Update lib.rs**

Add `pub mod server;` and `pub mod connection;`.

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p ferrosa-cql -- server`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add ferrosa-cql/src/server.rs ferrosa-cql/src/connection.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add CqlServer TCP listener with connection limit and per-connection tasks"
```

---

### Task 11: Connection handshake (STARTUP → AUTHENTICATE → AUTH_SUCCESS)

**Files:**

- Modify: `ferrosa-cql/src/connection.rs`
- Test: integration test in `ferrosa-cql/tests/handshake.rs`

- [ ] **Step 1: Write failing integration test**

Create `ferrosa-cql/tests/handshake.rs`:

```rust
//! Integration test: CQL v5 auth handshake.

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use ferrosa_cql::frame::*;
use ferrosa_cql::server::{CqlServer, ServerConfig};

fn encode_startup_frame() -> BytesMut {
    // STARTUP body: [string map] with CQL_VERSION=3.0.0
    let mut body = BytesMut::new();
    body.put_u16(1); // 1 entry
    let key = b"CQL_VERSION";
    body.put_u16(key.len() as u16);
    body.put_slice(key);
    let val = b"3.0.0";
    body.put_u16(val.len() as u16);
    body.put_slice(val);

    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Startup,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    buf
}

fn encode_auth_response(username: &str, password: &str) -> BytesMut {
    // AUTH_RESPONSE body: [bytes] containing SASL PLAIN payload
    let sasl = format!("\0{username}\0{password}");
    let sasl_bytes = sasl.as_bytes();

    let mut body = BytesMut::new();
    body.put_i32(sasl_bytes.len() as i32);
    body.put_slice(sasl_bytes);

    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::AuthResponse,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    buf
}

/// Helper: send a STARTUP frame.
async fn send_startup(stream: &mut TcpStream) {
    let buf = encode_startup_frame();
    stream.write_all(&buf).await.unwrap();
}

/// Helper: read a single response frame header + body.
struct RawFrame {
    opcode: Opcode,
    body: Vec<u8>,
}

async fn read_frame(stream: &mut TcpStream) -> RawFrame {
    let mut hdr_buf = vec![0u8; HEADER_SIZE];
    stream.read_exact(&mut hdr_buf).await.unwrap();
    let header = FrameHeader::decode(&hdr_buf).unwrap();
    let mut body = vec![0u8; header.length as usize];
    if !body.is_empty() {
        stream.read_exact(&mut body).await.unwrap();
    }
    RawFrame {
        opcode: header.opcode,
        body,
    }
}

/// Helper: send a raw frame with given opcode and body.
async fn send_raw_frame(stream: &mut TcpStream, opcode: Opcode, body: &[u8]) {
    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 0,
        opcode,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(body);
    stream.write_all(&buf).await.unwrap();
}

#[tokio::test]
async fn startup_then_authenticate_then_auth_success() {
    let config = ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        max_in_flight_per_connection: 128,
        auth_disabled: false,
    };
    let server = CqlServer::new(config);
    let addr = server.start_background().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send STARTUP
    let startup = encode_startup_frame();
    stream.write_all(&startup).await.unwrap();

    // Read AUTHENTICATE response
    let mut resp = vec![0u8; 256];
    let n = stream.read(&mut resp).await.unwrap();
    assert!(n >= HEADER_SIZE);
    let header = FrameHeader::decode(&resp[..HEADER_SIZE]).unwrap();
    assert_eq!(header.opcode, Opcode::Authenticate);

    // Send AUTH_RESPONSE with valid credentials
    let auth = encode_auth_response("cassandra", "cassandra");
    stream.write_all(&auth).await.unwrap();

    // Read AUTH_SUCCESS
    let n = stream.read(&mut resp).await.unwrap();
    assert!(n >= HEADER_SIZE);
    let header = FrameHeader::decode(&resp[..HEADER_SIZE]).unwrap();
    assert_eq!(header.opcode, Opcode::AuthSuccess);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-cql --test handshake`
Expected: FAIL — connection stub doesn't send AUTHENTICATE

- [ ] **Step 3: Implement connection handshake**

Update `connection.rs` to use the codec and implement the handshake:

```rust
use std::net::SocketAddr;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, warn};

use crate::auth;
use crate::error::CqlError;
use crate::frame::*;

/// Handle a single CQL connection.
pub async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    max_frame_size: u32,
    auth_disabled: bool,
) {
    debug!("new connection from {peer}");

    let codec = CqlCodec::new(max_frame_size);
    let mut framed = Framed::new(stream, codec);

    // Phase 1: Wait for STARTUP (handle OPTIONS first)
    let startup = match framed.next().await {
        Some(Ok(frame)) if frame.header.opcode == Opcode::Startup => frame,
        Some(Ok(frame)) => {
            warn!("expected STARTUP, got {:?} from {peer}", frame.header.opcode);
            let _ = send_error(&mut framed, 0, CqlError::Protocol(
                "expected STARTUP as first message".into(),
            )).await;
            return;
        }
        Some(Err(e)) => {
            warn!("frame error from {peer}: {e}");
            return;
        }
        None => {
            debug!("connection from {peer} closed before STARTUP");
            return;
        }
    };

    // Phase 2: Dev mode bypass — if auth disabled, send READY and skip auth
    if auth_disabled {
        if send_frame(
            &mut framed,
            startup.header.stream_id,
            Opcode::Ready,
            Bytes::new(),
        )
        .await
        .is_err()
        {
            return;
        }
        debug!("connection from {peer} ready (auth disabled)");
        // Skip to query loop
    } else {

    // Phase 3: Send AUTHENTICATE
    let auth_body = auth::encode_authenticate_response();
    if send_frame(
        &mut framed,
        startup.header.stream_id,
        Opcode::Authenticate,
        Bytes::from(auth_body),
    )
    .await
    .is_err()
    {
        return;
    }

    // Phase 3: Wait for AUTH_RESPONSE, validate credentials
    let mut attempts = 0u32;
    loop {
        let frame = match framed.next().await {
            Some(Ok(frame)) if frame.header.opcode == Opcode::AuthResponse => frame,
            Some(Ok(frame)) => {
                warn!("expected AUTH_RESPONSE, got {:?} from {peer}", frame.header.opcode);
                let _ = send_error(&mut framed, 0, CqlError::Protocol(
                    "expected AUTH_RESPONSE".into(),
                )).await;
                return;
            }
            _ => return,
        };

        // Parse SASL PLAIN from the AUTH_RESPONSE body.
        // Body format: [int length][bytes payload]
        let body = &frame.body;
        if body.len() < 4 {
            let _ = send_error(&mut framed, frame.header.stream_id, CqlError::Protocol(
                "AUTH_RESPONSE body too short".into(),
            )).await;
            return;
        }
        let payload_len = i32::from_be_bytes(body[..4].try_into().unwrap());
        if payload_len < 0 || body.len() < 4 + payload_len as usize {
            let _ = send_error(&mut framed, frame.header.stream_id, CqlError::Protocol(
                "AUTH_RESPONSE: invalid payload length".into(),
            )).await;
            return;
        }
        let payload = &body[4..4 + payload_len as usize];

        match auth::parse_sasl_plain(payload) {
            Ok((_username, _password)) => {
                // TODO Part B: validate via ferrosa-schema::Schema::authenticate()
                // For Part A, accept any credentials.
                let success_body = auth::encode_auth_success();
                if send_frame(
                    &mut framed,
                    frame.header.stream_id,
                    Opcode::AuthSuccess,
                    Bytes::from(success_body),
                )
                .await
                .is_err()
                {
                    return;
                }
                break;
            }
            Err(_) => {
                attempts += 1;
                if attempts >= auth::MAX_AUTH_ATTEMPTS {
                    let _ = send_error(&mut framed, frame.header.stream_id, CqlError::BadCredentials).await;
                    return;
                }
                let _ = send_error(&mut framed, frame.header.stream_id, CqlError::BadCredentials).await;
            }
        }
    }

    debug!("connection from {peer} authenticated");

    } // end else (auth not disabled)

    // Phase 4: Query loop (Part B)
    // For now, handle OPTIONS and return errors for everything else.
    while let Some(result) = framed.next().await {
        match result {
            Ok(frame) if frame.header.opcode == Opcode::Options => {
                let supported_body = encode_supported_options();
                let _ = send_frame(
                    &mut framed,
                    frame.header.stream_id,
                    Opcode::Supported,
                    Bytes::from(supported_body),
                )
                .await;
            }
            Ok(frame) => {
                debug!("received {:?} from {peer} (not yet handled)", frame.header.opcode);
                let _ = send_error(
                    &mut framed,
                    frame.header.stream_id,
                    CqlError::ServerError("query handling not yet implemented".into()),
                )
                .await;
            }
            Err(e) => {
                warn!("frame error from {peer}: {e}");
                break;
            }
        }
    }

    debug!("connection from {peer} closed");
}

/// Send a CQL frame.
async fn send_frame(
    framed: &mut Framed<TcpStream, CqlCodec>,
    stream_id: i16,
    opcode: Opcode,
    body: Bytes,
) -> Result<(), CqlError> {
    let frame = CqlFrame {
        header: FrameHeader {
            version: VERSION_RESPONSE,
            flags: 0,
            stream_id,
            opcode,
            length: 0, // set by encoder
        },
        body,
    };
    framed.send(frame).await.map_err(|e| {
        CqlError::ServerError(format!("send error: {e}"))
    })
}

/// Send a CQL ERROR response.
async fn send_error(
    framed: &mut Framed<TcpStream, CqlCodec>,
    stream_id: i16,
    error: CqlError,
) -> Result<(), CqlError> {
    let body = error.encode_body();
    send_frame(framed, stream_id, Opcode::Error, body.freeze()).await
}

/// Encode the SUPPORTED response body.
///
/// Returns a string multimap with CQL_VERSION and COMPRESSION options.
fn encode_supported_options() -> Vec<u8> {
    let mut buf = Vec::new();
    // [short] number of keys
    buf.extend_from_slice(&2u16.to_be_bytes());

    // Key 1: CQL_VERSION
    let key = b"CQL_VERSION";
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(key);
    // [short] number of values for this key
    buf.extend_from_slice(&1u16.to_be_bytes());
    let val = b"3.4.7";
    buf.extend_from_slice(&(val.len() as u16).to_be_bytes());
    buf.extend_from_slice(val);

    // Key 2: COMPRESSION (empty — compression deferred)
    let key = b"COMPRESSION";
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&0u16.to_be_bytes()); // no compression options

    buf
}
```

Add `futures = "0.3"` to `Cargo.toml` dependencies.

- [ ] **Step 4: Run integration test to verify it passes**

Run: `cargo test -p ferrosa-cql --test handshake`
Expected: PASS

- [ ] **Step 5: Write auth failure tests**

Add to `handshake.rs`:

```rust
#[tokio::test]
async fn malformed_sasl_payload_returns_bad_credentials() {
    let config = ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        max_in_flight_per_connection: 128,
        auth_disabled: false,
    };
    let server = CqlServer::new(config);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send STARTUP
    send_startup(&mut stream).await;

    // Read AUTHENTICATE
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    // Send AUTH_RESPONSE with malformed SASL payload (no null delimiters)
    let bad_payload = b"not-valid-sasl";
    let mut body = Vec::new();
    body.extend_from_slice(&(bad_payload.len() as i32).to_be_bytes());
    body.extend_from_slice(bad_payload);
    send_raw_frame(&mut stream, Opcode::AuthResponse, &body).await;

    // Should receive ERROR(Bad Credentials)
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Error);
    let error_code = i32::from_be_bytes(resp.body[..4].try_into().unwrap());
    assert_eq!(error_code, 0x0100); // Bad Credentials
}

#[tokio::test]
async fn three_failed_auth_attempts_closes_connection() {
    let config = ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        max_in_flight_per_connection: 128,
        auth_disabled: false,
    };
    let server = CqlServer::new(config);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send STARTUP
    send_startup(&mut stream).await;

    // Read AUTHENTICATE
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    // Send 3 malformed AUTH_RESPONSEs
    let bad_payload = b"not-valid-sasl";
    for _ in 0..3 {
        let mut body = Vec::new();
        body.extend_from_slice(&(bad_payload.len() as i32).to_be_bytes());
        body.extend_from_slice(bad_payload);
        send_raw_frame(&mut stream, Opcode::AuthResponse, &body).await;

        // Read ERROR(Bad Credentials) each time
        let resp = read_frame(&mut stream).await;
        assert_eq!(resp.opcode, Opcode::Error);
    }

    // Connection should be closed now — next read returns 0 bytes
    let mut buf = vec![0u8; 64];
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "connection should be closed after 3 auth failures");
}

#[tokio::test]
async fn auth_disabled_startup_returns_ready() {
    let config = ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        max_in_flight_per_connection: 128,
        auth_disabled: true, // dev mode
    };
    let server = CqlServer::new(config);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send STARTUP
    send_startup(&mut stream).await;

    // Should receive READY (not AUTHENTICATE)
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Ready);
}
```

- [ ] **Step 6: Run auth failure tests**

Run: `cargo test -p ferrosa-cql --test handshake`
Expected: PASS (all tests including auth failure and dev mode)

- [ ] **Step 7: Run all crate tests**

Run: `cargo test -p ferrosa-cql`
Expected: PASS (all unit + integration tests)

- [ ] **Step 8: Commit**

```bash
git add ferrosa-cql/src/connection.rs ferrosa-cql/tests/handshake.rs ferrosa-cql/Cargo.toml
git commit -m "feat(cql): implement STARTUP → AUTHENTICATE → AUTH_SUCCESS handshake with dev mode bypass"
```

---

### Task 12: OPTIONS/SUPPORTED handshake

**Files:**

- Modify: `ferrosa-cql/src/connection.rs`
- Test: add to `ferrosa-cql/tests/handshake.rs`

- [ ] **Step 1: Write failing test for OPTIONS**

Add to `handshake.rs`:

```rust
#[tokio::test]
async fn options_returns_supported() {
    let config = ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        max_in_flight_per_connection: 128,
        auth_disabled: false,
    };
    let server = CqlServer::new(config);
    let addr = server.start_background().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send OPTIONS (no body)
    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Options,
        length: 0,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    stream.write_all(&buf).await.unwrap();

    // Read SUPPORTED response
    let mut resp = vec![0u8; 256];
    let n = stream.read(&mut resp).await.unwrap();
    assert!(n >= HEADER_SIZE);
    let resp_header = FrameHeader::decode(&resp[..HEADER_SIZE]).unwrap();
    assert_eq!(resp_header.opcode, Opcode::Supported);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-cql --test handshake -- options`
Expected: FAIL — OPTIONS not handled

- [ ] **Step 3: Handle OPTIONS before STARTUP**

In `connection.rs`, modify Phase 1 to handle OPTIONS before STARTUP in a loop:

```rust
// Phase 1: Wait for STARTUP (handle OPTIONS first)
let startup = loop {
    match framed.next().await {
        Some(Ok(frame)) if frame.header.opcode == Opcode::Options => {
            // Respond with SUPPORTED
            let supported_body = encode_supported_options();
            if send_frame(
                &mut framed,
                frame.header.stream_id,
                Opcode::Supported,
                Bytes::from(supported_body),
            )
            .await
            .is_err()
            {
                return;
            }
            continue;
        }
        Some(Ok(frame)) if frame.header.opcode == Opcode::Startup => break frame,
        Some(Ok(frame)) => {
            warn!("expected STARTUP or OPTIONS, got {:?}", frame.header.opcode);
            let _ = send_error(&mut framed, 0, CqlError::Protocol(
                "expected STARTUP or OPTIONS".into(),
            )).await;
            return;
        }
        Some(Err(e)) => {
            warn!("frame error from {peer}: {e}");
            return;
        }
        None => return,
    }
};
```

Note: `encode_supported_options()` was already added to `connection.rs` in Task 11.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p ferrosa-cql --test handshake`
Expected: PASS

- [ ] **Step 5: Write post-auth OPTIONS test**

Add to `handshake.rs`:

```rust
#[tokio::test]
async fn options_works_after_auth() {
    let config = ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        max_in_flight_per_connection: 128,
        auth_disabled: true, // use dev mode for simplicity
    };
    let server = CqlServer::new(config);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Complete handshake (dev mode: STARTUP → READY)
    send_startup(&mut stream).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Ready);

    // Send OPTIONS after auth
    send_raw_frame(&mut stream, Opcode::Options, &[]).await;

    // Should still get SUPPORTED
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Supported);
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p ferrosa-cql --test handshake`
Expected: PASS (all tests)

- [ ] **Step 7: Commit**

```bash
git add ferrosa-cql/src/connection.rs ferrosa-cql/tests/handshake.rs
git commit -m "feat(cql): handle OPTIONS/SUPPORTED in pre-auth and post-auth phases"
```

---

### Task 13: Final Part A verification

**Files:** None (verification only)

- [ ] **Step 1: Run full test suite**

Run: `cargo test -p ferrosa-cql`
Expected: PASS (all tests)

- [ ] **Step 2: Run clippy**

Run: `cargo clippy -p ferrosa-cql --all-targets -- -D warnings`
Expected: No warnings

- [ ] **Step 3: Check formatting**

Run: `cargo fmt -p ferrosa-cql --check`
Expected: No formatting issues

- [ ] **Step 4: Generate docs**

Run: `cargo doc -p ferrosa-cql --no-deps`
Expected: No warnings

- [ ] **Step 5: Commit any fixes**

If clippy or fmt found issues, fix and commit:

```bash
git add ferrosa-cql/
git commit -m "fix(cql): address clippy and formatting issues"
```
