# ferrosa-cql Design Spec

> Last updated: 2026-03-12
> Status: Approved

## Overview

ferrosa-cql implements the CQL native protocol v5, providing the client-facing interface to Ferrosa. It handles TCP connections, binary protocol framing, CQL parsing, query execution, and prepared statement caching. All hot paths are lock-free and the system parallelizes across all cores via Tokio's multi-threaded runtime.

## Implementation Parts

- **Part A**: Protocol + Types (frame layer, CQL type system, TCP server with auth)
- **Part B**: Parser + Execution (hand-written recursive descent, query routing to schema/storage)
- **Part C**: Prepared statements + System queries (moka cache, system keyspace routing, USE/DESCRIBE)
- **Part D**: Threat model + security hardening (STRIDE analysis, fix findings)

## Architecture

### Concurrency Model

- **Tokio multi-threaded runtime**: per-connection tasks, work-stealing across all cores
- **Lock-free shared state**:
  - `ArcSwap<SchemaSnapshot>` for schema lookups (consistent with ferrosa-schema pattern)
  - `moka` concurrent cache for prepared statements (W-TinyLFU eviction, lock-free reads)
  - `Arc<StorageEngine>` for storage reads/writes (engine handles its own concurrency)
- **No Mutex on hot path**: every query executes without contention
- **Per-connection isolation**: each Tokio task owns its `AuthContext`, current keyspace, and codec state

### Connection Lifecycle

```
TCP accept
  → Tokio task spawned per connection
  → Framed<TcpStream, CqlCodec> for zero-copy framing
  → STARTUP handshake (version negotiation)
  → AUTH handshake (AUTHENTICATE → AUTH_RESPONSE → AUTH_SUCCESS)
  → Query loop: read frame → parse → validate → execute → encode response
  → Connection close / error → task cleanup
```

## Frame Layer

### Binary Framing (CQL v5)

9-byte header per frame:

| Field | Size | Description |
|-------|------|-------------|
| version | 1 byte | Protocol version (0x05 request, 0x85 response) |
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
| _CREDENTIALS_ | _0x04_ | _—_ | _Deprecated since v2, reserved_ |
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
- **Max frame size**: configurable, default 256 MiB (Cassandra default)
- **Frame compression**: CQL v5 supports LZ4 and Snappy for frame bodies, negotiated during STARTUP/SUPPORTED exchange. Deferred — initial implementation sends uncompressed frames. The flag is parsed but compression/decompression is not applied.

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
| `date` | 4-byte unsigned (days since epoch) | `u32` |
| `time` | 8-byte nanoseconds since midnight | `i64` |
| `duration` | 3 varints (months, days, nanos) | deferred |
| `inet` | 4 or 16 bytes | `std::net::IpAddr` |
| `varint` | variable-length signed | `num_bigint::BigInt` |
| `decimal` | varint scale + varint unscaled | `(i32, BigInt)` |
| `list<T>` | `[n][element]*n` | `Vec<CqlValue>` |
| `set<T>` | `[n][element]*n` | `Vec<CqlValue>` (wire-order; bridge converts) |
| `map<K,V>` | `[n][key,val]*n` | `Vec<(CqlValue, CqlValue)>` (wire-order; bridge converts) |
| `tuple` | concatenated elements | `Vec<Option<CqlValue>>` |
| `frozen<UDT>` | concatenated named fields | `Vec<(String, Option<CqlValue>)>` |

### Design

- Type IDs from protocol spec (0x0001=ascii through 0x0030=UDT)
- Collections decode recursively (e.g., `list<frozen<map<text, int>>>` works naturally)
- `CqlValue` implements `Ord` using CQL's type-specific ordering (numeric for ints, lexicographic for strings); sets and maps are homogeneous (elements share a declared type from the column definition)
- Allocation-minimal: borrow `&[u8]` where possible, only allocate for owned values

### CqlValue / CellValue Bridge

`CqlValue` (protocol-facing) converts to/from `ferrosa-common::CellValue` (storage-facing). A dedicated `bridge` module owns this conversion:

- **Cell values**: `CqlValue` serializes to `CellValue.value` as CQL wire format bytes (big-endian, same as protocol encoding). This avoids a second serialization format and means SSTable readers can decode values directly.
- **Partition keys**: Composite partition keys serialize using Cassandra's composite key format — each component is `[2-byte length][value bytes][0x00 terminator]`, concatenated. Single-component keys use raw value bytes. This is the format `ferrosa-common::PartitionKey` expects.
- **Clustering keys**: Serialized using the byte-comparable encoding from `ferrosa-sstable::byte_comparable` to preserve sort order in the storage layer.
- **Null handling**: CQL null (length = -1 in protocol) maps to `CellValue::Empty` in storage.

## Parser — Hand-Written Recursive Descent

### Pipeline

```
Input: &str
  → Lexer (tokenizer): produces Token stream
  → Parser: consumes tokens, builds AST nodes
Output: Statement enum
```

### Lexer

- Single-pass, zero-allocation tokenizer yielding `Token<'input>` borrowing from source
- Token types: keywords, identifiers, string literals, integers, floats, operators, bind markers
- Keywords case-insensitive via `phf` perfect-hash map (compile-time generated)
- String literals: single-quoted `'text'` with `''` escape, `$$dollar-quoted$$` for blobs

### Parser

One function per grammar rule:

- `parse_statement()` dispatches on first keyword to `parse_select()`, `parse_insert()`, `parse_update()`, `parse_delete()`, `parse_create_*()`, `parse_alter_*()`, `parse_drop_*()`, `parse_use()`, `parse_grant()`, `parse_revoke()`, `parse_batch()`
- Each returns `Result<Statement>` with span information for error reporting
- `parse_where_clause()` → list of `Relation` (column op value)
- `parse_select_clause()` → `Vec<Selector>` (column, function call, `*`, `COUNT(*)`)
- `parse_column_type()` → recursive for nested generic types
- Bind markers (`?`, `:name`) produce `BindMarker` AST nodes

### AST

- `Statement` enum with variants: `Select`, `Insert`, `Update`, `Delete`, `CreateKeyspace`, `CreateTable`, `AlterTable`, `DropTable`, `CreateRole`, `Grant`, `Revoke`, `Use`, `Batch`, etc.
- Each variant holds only the parsed fields
- No heap allocation for simple queries where possible

### Performance

- Single pass, O(n) in query length
- No backtracking — CQL grammar is LL(2) (at most 2-token lookahead for `CREATE TABLE` vs `CREATE KEYSPACE`)
- Pure function, no shared state — each connection parses independently

## Query Execution and Routing

### Router

```
Statement (AST)
  → DDL (CREATE/ALTER/DROP/GRANT/REVOKE)  →  ferrosa-schema::Schema
  → DML reads (SELECT)                    →  ferrosa-storage::StorageEngine::read()
  → DML writes (INSERT/UPDATE/DELETE)     →  ferrosa-storage::StorageEngine::write()
  → USE                                   →  connection-local state
  → BATCH (single-partition)              →  atomic apply via StorageEngine
  → BATCH (cross-partition, unlogged)     →  best-effort individual writes
  → BATCH (cross-partition, logged)       →  deferred (returns Unsupported error)
  → System queries (system_schema.*)      →  ferrosa-schema::snapshot()
  → PREPARE                               →  parse + validate + cache
  → EXECUTE                               →  lookup cached plan, bind, re-enter router
```

### Execution Context (per-connection, lock-free)

- `AuthContext`, current keyspace, references to shared state
- Schema via `ArcSwap<SchemaSnapshot>` — lock-free load
- Prepared cache via `moka` — lock-free get/insert
- Storage via `Arc<StorageEngine>`

### Query Validation

- Resolve keyspace (explicit `ks.table` or connection default)
- Validate column names, types, partition key completeness
- SELECT: verify WHERE covers partition key; `ALLOW FILTERING` is rejected with an Invalid error in the initial implementation (full-scan support deferred to secondary index work)
- **Consistency level**: parsed from query parameters and validated, but execution assumes single-node (effectively CL=ONE). Multi-node CL enforcement is handled by ferrosa-cluster integration.

### Result Encoding

- DML reads → RESULT(Rows) with column specs
- DDL → RESULT(Schema_change)
- Writes → RESULT(Void)
- Errors → ERROR frame with appropriate code

No query optimizer in initial implementation.

## Authentication

### SASL PLAIN Flow

- STARTUP response returns AUTHENTICATE with authenticator name `org.apache.cassandra.auth.PasswordAuthenticator` (standard string drivers expect)
- Client sends AUTH_RESPONSE containing SASL PLAIN payload: `\0<username>\0<password>` (null-delimited)
- Server validates via `ferrosa-schema::Schema::authenticate(username, password)`
- Success: AUTH_SUCCESS with empty token; failure: ERROR(Bad Credentials)
- Max auth attempts per connection: 3, then connection is closed

### Auth-Disabled Mode

- In `DeploymentMode::Development` with no auth configured, STARTUP returns READY directly (skip AUTHENTICATE)
- Production mode always requires auth

## Connection Limits and Backpressure

- **Max connections**: configurable, default 1024 per server; new connections beyond limit receive ERROR(Overloaded) and are closed
- **Max in-flight per connection**: bounded by stream ID space (i16, 32K); server-side configurable limit (default 128) — requests beyond limit receive ERROR(Overloaded) on that stream
- **Memory pressure**: if Tokio task queue depth exceeds threshold, new requests get ERROR(Overloaded)
- **Storage backpressure**: `StorageEngine` can return a backpressure signal; CQL layer propagates as ERROR(Overloaded)

## Prepared Statement Cache

### Flow

```
PREPARE "SELECT * FROM ks.t WHERE id = ?"
  → parse → validate → compute MD5 → store in moka cache → return ID + metadata

EXECUTE (prepared_id, [bound_values])
  → moka lookup → bind values → route to execution
```

### PreparedPlan

- `id: [u8; 16]` — MD5 of query string (protocol convention)
- `result_metadata_id: [u8; 16]` — MD5 of result column specs
- `statement: Statement` — pre-parsed AST
- `keyspace: String` — resolved at prepare time
- `bound_columns: Vec<ColumnSpec>` — types for bind markers
- `result_columns: Vec<ColumnSpec>` — result set column specs
- `partition_key_indices: Vec<usize>` — bind markers forming partition key

### Cache Design (moka)

- **Lock-free**: W-TinyLFU eviction (frequency + recency aware, Caffeine-equivalent)
- **Weight-based capacity**: default `max(configured_limit, 10 MiB)`, configurable in server options
- **Weigher**: estimated byte size of each `PreparedPlan` (AST + metadata)
- **Oversized statements**: statements exceeding cache capacity are still preparable and returned to the driver, but bypass the cache (not cached, re-parsed on each EXECUTE)
- **Schema invalidation**: background sweep on schema snapshot update, remove entries for affected keyspace/table
- **Eviction metrics**: counter + periodic log warning when evictions occur
- **No TTL**: statements live until evicted by size pressure or schema change

### Result Metadata Caching (v5)

- EXECUTE can set `SKIP_METADATA` flag if driver has result metadata
- Server tracks `result_metadata_id` — sends `METADATA_CHANGED` flag on schema change so driver re-prepares

## Error Handling

### Error Codes

| Code | Name | When |
|------|------|------|
| `0x0000` | Server Error | Unexpected internal failure |
| `0x000A` | Protocol Error | Malformed frame, wrong version, bad opcode |
| `0x0100` | Bad Credentials | AUTH_RESPONSE rejected |
| `0x1000` | Unavailable | Not enough replicas |
| `0x1100` | Overloaded | Server backpressure |
| `0x2000` | Syntax Error | Parser failed |
| `0x2100` | Unauthorized | Permission denied |
| `0x2200` | Invalid | Semantic error (unknown table, type mismatch) |
| `0x2300` | Config Error | Invalid DDL |
| `0x2400` | Already Exists | CREATE without IF NOT EXISTS on existing object |
| `0x2500` | Unprepared | EXECUTE with unknown prepared ID |

### Error Propagation

- Internal `CqlError` enum with variants carrying structured data
- `From<SchemaError>` and `From<StorageError>` conversions
- Parser errors include byte offset and surrounding snippet
- Connection-level: malformed frames get ERROR response + connection kept alive; auth failures close after max attempts; panics caught at task boundary

## Testing Strategy

### Unit Tests

- **Frame codec**: round-trip for every opcode, truncated/oversized frames, version mismatch
- **Type system**: encode/decode for every CQL type, nested collections, nulls, boundary values
- **Lexer**: all syntax variants, edge cases (dollar-quoted, unicode, case insensitivity)
- **Parser**: one test per statement type, one per expected error, bind markers, nested types
- **Error encoding**: every error code produces spec-compliant bytes

### Integration Tests

- In-memory server (schema + storage, no TCP) with test harness sending raw frames
- Full auth handshake flow
- Query lifecycle: prepare → execute → verify result rows
- Schema DDL → INSERT → SELECT → data round-trip
- Error paths: nonexistent table, type mismatch, unauthorized
- Connection multiplexing: interleaved stream IDs

### Property Tests (proptest)

- `CqlValue` round-trip: arbitrary value → encode → decode → assert equal
- Frame decode safety: arbitrary bytes → never panics
- Parser safety: random strings → valid AST or clean error, never panics

### No Mocks

Schema and storage layers are lightweight enough to instantiate in-memory. Real objects, real behavior.

## Crate Structure

```
ferrosa-cql/
├── Cargo.toml
└── src/
    ├── lib.rs              # Public API: CqlServer, start()
    ├── frame.rs            # Frame header, CqlCodec (Encoder/Decoder), opcodes
    ├── types.rs            # CqlValue enum, encode/decode, type IDs
    ├── bridge.rs           # CqlValue <-> CellValue conversion, key serialization
    ├── lexer.rs            # Zero-alloc tokenizer, Token<'input>, keyword map
    ├── parser.rs           # Recursive descent parser, one fn per grammar rule
    ├── ast.rs              # Statement enum, all AST node types
    ├── router.rs           # Query routing: DDL→schema, DML→storage, system→snapshot
    ├── prepared.rs         # PreparedPlan, moka cache, schema invalidation
    ├── auth.rs             # SASL PLAIN handshake, auth state machine
    ├── error.rs            # CqlError enum, error code encoding, From impls
    ├── server.rs           # TCP listener, connection accept loop, backpressure
    └── connection.rs       # Per-connection task: codec + auth + query loop
```

## Key Dependencies

| Crate | Purpose |
|-------|---------|
| `tokio` | Async runtime, TCP, task spawning |
| `bytes` | Zero-copy byte buffers |
| `tokio-util` | Codec/Framed for protocol framing |
| `arc-swap` | Lock-free schema snapshot access |
| `moka` | Lock-free prepared statement cache (W-TinyLFU) |
| `phf` | Compile-time perfect hash for keyword lookup |
| `md-5` | MD5 hashing for prepared statement IDs |
| `uuid` | UUID type support |
| `num-bigint` | Varint/decimal support |
| `ferrosa-schema` | Schema operations, auth, permissions |
| `ferrosa-storage` | Storage engine reads/writes |
| `ferrosa-common` | Shared types (CellValue, Token, etc.) |
