# CQL Protocol Specification

> Last updated: 2026-03-12
> Status: Approved

## Overview

`ferrosa-cql` implements CQL native protocol v5 — the client-facing interface to Ferrosa. It handles TCP connections, binary protocol framing, CQL parsing, query execution, prepared statement caching, and SASL PLAIN authentication.

All hot paths are lock-free. The system parallelizes across all cores via Tokio's multi-threaded runtime.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Concurrency | Lock-free via `ArcSwap` + `moka` | No contention on hot paths; see [ADR-006](decisions/006-cql-architecture.md) |
| Parser | Hand-written recursive descent | CQL is LL(2); no backtracking needed; see [ADR-006](decisions/006-cql-architecture.md) |
| Prepared cache | `moka` W-TinyLFU | Lock-free reads, frequency+recency eviction |
| `ALLOW FILTERING` | Rejected (returns Invalid error) | Full-scan support deferred to secondary index work |
| Auth | SASL PLAIN only | Standard CQL driver expectation; pluggable trait for future |

## Dependencies

```
ferrosa-cql
├── ferrosa-common   (Token, DecoratedKey, CellValue, Error)
├── ferrosa-schema   (Schema, auth, permissions)
├── ferrosa-storage  (StorageEngine reads/writes)
├── tokio            (async runtime, TCP, task spawning)
├── tokio-util       (Codec/Framed for protocol framing)
├── bytes            (zero-copy byte buffers)
├── arc-swap         (lock-free schema snapshot access)
├── moka             (lock-free prepared statement cache)
├── phf              (compile-time perfect hash for keywords)
├── md-5             (MD5 for prepared statement IDs)
├── uuid             (UUID type support)
└── num-bigint       (varint/decimal support)
```

## Architecture

```mermaid
graph TB
    subgraph "ferrosa-cql"
        subgraph "Transport"
            Server[TCP Server]
            Conn[Connection Task]
            Codec[CqlCodec<br/>Encoder/Decoder]
        end

        subgraph "Protocol"
            Frame[Frame Layer<br/>9-byte header]
            Auth[SASL PLAIN Auth]
            Types[CQL Type System<br/>CqlValue enum]
        end

        subgraph "Query Engine"
            Lexer[Lexer<br/>zero-alloc tokenizer]
            Parser[Recursive Descent<br/>Parser]
            Router[Query Router]
            Prepared[Prepared Cache<br/>moka W-TinyLFU]
        end

        subgraph "Bridge"
            Bridge[CqlValue ↔ CellValue<br/>Key serialization]
        end
    end

    Server --> Conn
    Conn --> Codec --> Frame
    Conn --> Auth
    Conn --> Router
    Router --> Lexer --> Parser
    Router --> Prepared
    Router --> Bridge

    Bridge --> Schema[ferrosa-schema]
    Bridge --> Storage[ferrosa-storage]
```

## Frame Layer

### Binary Framing (CQL v5)

9-byte header per frame:

| Field | Size | Description |
|-------|------|-------------|
| version | 1 byte | Protocol version (`0x05` request, `0x85` response) |
| flags | 1 byte | Compression, tracing, custom payload, warning |
| stream ID | 2 bytes | Multiplexing identifier (big-endian i16) |
| opcode | 1 byte | Operation type |
| length | 4 bytes | Body length (big-endian u32) |

### Opcodes

| Opcode | Value | Direction | Purpose |
|--------|-------|-----------|---------|
| ERROR | 0x00 | Response | Error with code + message |
| STARTUP | 0x01 | Request | Initiate connection |
| READY | 0x02 | Response | Server ready (no auth needed) |
| AUTHENTICATE | 0x03 | Response | Auth required |
| OPTIONS | 0x05 | Request | Query supported options |
| SUPPORTED | 0x06 | Response | Supported options response |
| QUERY | 0x07 | Request | Execute CQL query |
| RESULT | 0x08 | Response | Query result |
| PREPARE | 0x09 | Request | Prepare a statement |
| EXECUTE | 0x0A | Request | Execute prepared statement |
| REGISTER | 0x0B | Request | Register for events |
| EVENT | 0x0C | Response | Event notification |
| BATCH | 0x0D | Request | Batch of statements |
| AUTH_CHALLENGE | 0x0E | Response | Auth challenge |
| AUTH_RESPONSE | 0x0F | Request | Auth response |
| AUTH_SUCCESS | 0x10 | Response | Auth success |

### Implementation

- **Zero-copy**: `bytes::BytesMut` for read buffers
- **Tokio codec pattern**: `Encoder`/`Decoder` traits on `Framed<TcpStream, CqlCodec>`
- **Multiplexing**: stream IDs allow concurrent in-flight requests per connection
- **Max frame size**: configurable, default 256 MiB
- **Frame compression**: LZ4/Snappy negotiated during STARTUP. Deferred — flag parsed but compression not applied.

## CQL Type System

Single `CqlValue` enum covering all CQL types with `encode`/`decode` methods.

### Type Mapping

| CQL Type | Wire Format | Rust Type |
|----------|------------|-----------|
| `ascii`, `text`, `varchar` | UTF-8 bytes | `String` |
| `int` | 4-byte big-endian | `i32` |
| `bigint`, `counter` | 8-byte big-endian | `i64` |
| `smallint` | 2-byte big-endian | `i16` |
| `tinyint` | 1 byte | `i8` |
| `float` | 4-byte IEEE 754 | `f32` |
| `double` | 8-byte IEEE 754 | `f64` |
| `boolean` | 1 byte (0/1) | `bool` |
| `blob` | raw bytes | `Vec<u8>` / `Bytes` |
| `uuid`, `timeuuid` | 16 bytes | `uuid::Uuid` |
| `timestamp` | 8-byte millis since epoch | `i64` |
| `inet` | 4 or 16 bytes | `std::net::IpAddr` |
| `varint` | variable-length signed | `num_bigint::BigInt` |
| `decimal` | varint scale + varint unscaled | `(i32, BigInt)` |
| `list<T>` | `[n][element]*n` | `Vec<CqlValue>` |
| `set<T>` | `[n][element]*n` | `Vec<CqlValue>` |
| `map<K,V>` | `[n][key,val]*n` | `Vec<(CqlValue, CqlValue)>` |
| `tuple` | concatenated elements | `Vec<Option<CqlValue>>` |
| `frozen<UDT>` | concatenated named fields | `Vec<(String, Option<CqlValue>)>` |

### CqlValue / CellValue Bridge

`CqlValue` (protocol-facing) converts to/from `ferrosa-common::CellValue` (storage-facing):

- **Cell values**: CQL wire format bytes (big-endian), avoiding a second serialization format
- **Partition keys**: Composite key format — `[2-byte length][value bytes][0x00 terminator]` per component
- **Clustering keys**: Byte-comparable encoding from `ferrosa-sstable::byte_comparable`
- **Null handling**: CQL null (length = -1) maps to `CellValue::Empty`

## Parser

### Pipeline

```
Input: &str → Lexer (Token stream) → Parser (AST) → Statement enum
```

- **Lexer**: Single-pass, zero-allocation tokenizer, `Token<'input>` borrows from source. Keywords via `phf` perfect-hash map.
- **Parser**: One function per grammar rule. LL(2) — no backtracking. Returns `Result<Statement>` with span info.
- **AST**: `Statement` enum with variants: `Select`, `Insert`, `Update`, `Delete`, `CreateKeyspace`, `CreateTable`, `AlterTable`, `DropTable`, `Use`, `Batch`, etc.

## Query Routing

```
Statement → DDL        → ferrosa-schema::Schema
          → DML reads  → ferrosa-storage::StorageEngine::read()
          → DML writes → ferrosa-storage::StorageEngine::write()
          → USE        → connection-local state
          → PREPARE    → parse + validate + cache
          → EXECUTE    → lookup cached plan, bind, re-enter router
```

## Authentication

SASL PLAIN flow:

1. STARTUP → AUTHENTICATE (`org.apache.cassandra.auth.PasswordAuthenticator`)
1. Client sends AUTH_RESPONSE: `\0<username>\0<password>`
1. Server validates via `ferrosa-schema::Schema::authenticate()`
1. Success: AUTH_SUCCESS; failure: ERROR(Bad Credentials)
1. Max 3 auth attempts per connection

In development mode with no auth configured, STARTUP returns READY directly.

## Prepared Statement Cache

- **Cache**: `moka` W-TinyLFU, weight-based capacity (default 10 MiB)
- **ID**: MD5 of query string (protocol convention)
- **Schema invalidation**: background sweep on schema snapshot update
- **No TTL**: statements live until evicted by size pressure or schema change

## Error Codes

| Code | Name | When |
|------|------|------|
| `0x0000` | Server Error | Unexpected internal failure |
| `0x000A` | Protocol Error | Malformed frame, wrong version |
| `0x0100` | Bad Credentials | AUTH_RESPONSE rejected |
| `0x1000` | Unavailable | Not enough replicas |
| `0x1100` | Overloaded | Server backpressure |
| `0x2000` | Syntax Error | Parser failed |
| `0x2100` | Unauthorized | Permission denied |
| `0x2200` | Invalid | Semantic error (unknown table, type mismatch) |
| `0x2300` | Config Error | Invalid DDL |
| `0x2400` | Already Exists | CREATE without IF NOT EXISTS |
| `0x2500` | Unprepared | EXECUTE with unknown prepared ID |

## Crate Structure

```
ferrosa-cql/
├── Cargo.toml
└── src/
    ├── lib.rs           # Public API: CqlServer, start()
    ├── frame.rs         # Frame header, CqlCodec, opcodes
    ├── types.rs         # CqlValue enum, encode/decode, type IDs
    ├── bridge.rs        # CqlValue ↔ CellValue conversion
    ├── lexer.rs         # Zero-alloc tokenizer, keyword map
    ├── parser.rs        # Recursive descent parser
    ├── ast.rs           # Statement enum, AST nodes
    ├── router.rs        # Query routing
    ├── prepared.rs      # PreparedPlan, moka cache
    ├── auth.rs          # SASL PLAIN handshake
    ├── error.rs         # CqlError enum, error codes
    ├── server.rs        # TCP listener, backpressure
    └── connection.rs    # Per-connection task
```

## Implementation Parts

- **Part A**: Protocol + Types (frame layer, CQL type system, TCP server with auth)
- **Part B**: Parser + Execution (hand-written recursive descent, query routing)
- **Part C**: Prepared statements + System queries (moka cache, system keyspace routing)
- **Part D**: Threat model + security hardening (STRIDE analysis)

## Testing Strategy

- **Frame codec**: round-trip for every opcode, truncated/oversized frames
- **Type system**: encode/decode for every CQL type, nested collections, nulls
- **Parser**: one test per statement type, bind markers, nested types
- **Property tests**: `CqlValue` round-trip, frame decode safety, parser safety
- **Integration**: in-memory server with test harness sending raw frames
- **No mocks**: real schema and storage objects

## Related Specs

- [Overview](overview.md) — system overview
- [Components](components.md) — crate architecture
- [ADR-006](decisions/006-cql-architecture.md) — CQL architectural decisions
