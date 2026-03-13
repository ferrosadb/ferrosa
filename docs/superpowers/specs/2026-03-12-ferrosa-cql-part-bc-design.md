# ferrosa-cql Parts B+C Design

> Date: 2026-03-12
> Status: Draft
> Crate: ferrosa-cql
> Depends on: ferrosa-common, ferrosa-schema, ferrosa-storage, ferrosa-sstable

## Overview

Complete the CQL protocol layer by implementing Parts B (parser + query
execution) and C (prepared statements + system queries). After this work,
`cqlsh` and standard CQL drivers connect to Ferrosa and run queries
end-to-end against a single-node storage engine.

Part A (done) provides: CQL v5 framing (`CqlCodec`), full type system
(`CqlValue`/`CqlType` with encode/decode for all types including collections),
TCP server with connection limit, SASL PLAIN auth helpers, error types with
wire encoding, and `SchemaError` → `CqlError` conversion.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Parser | Hand-written recursive descent | CQL is LL(2); no backtracking needed. `phf` keyword lookup already in deps. Graph crate set the pattern. |
| Bridge copies | Owned types, one allocation per value | Fixed-size types are register values (zero alloc). String/blob need alloc regardless (UTF-8 validation, ownership). Lifetime infection through router return types not worth it. If profiling shows allocation is hot, targeted `Bytes` refcounting is a surgical fix. |
| Prepared cache | `moka` sync::Cache, W-TinyLFU | Lock-free reads on the EXECUTE hot path. Weight-based eviction (default 10 MiB). |
| moka issue policy | Contribute upstream | If we find bugs or missing features in moka during testing, create patches and submit upstream PRs rather than forking. |
| SELECT scope | Single-partition reads only | WHERE must fully specify partition key with `=`. Range scans and multi-partition queries are follow-on. Returns `CqlError::Invalid` otherwise. |
| Unsupported DDL | Parse-time rejection | CREATE INDEX/VIEW/FUNCTION/AGGREGATE/TRIGGER/TYPE return `SyntaxError("not yet supported")`. Aligns with schema Chunks C-F. |

## New Files

All in `ferrosa-cql/src/`:

| File | Purpose |
|------|---------|
| `lexer.rs` | Single-pass zero-allocation tokenizer. `Token<'input>` borrows from source. Keywords via `phf` perfect-hash map. |
| `parser.rs` | Recursive descent parser. One function per grammar rule. LL(2) lookahead. Returns `Result<Statement, CqlError>`. |
| `ast.rs` | `Statement` enum and all AST node types. |
| `bridge.rs` | `CqlValue` ↔ `CellValue` conversion, partition key serialization, clustering key encoding. Stateless pure functions. |
| `result.rs` | RESULT frame body encoding (Rows, Void, Prepared, SetKeyspace, SchemaChange). Encode only, no decode. |
| `router.rs` | Query dispatch: AST → Schema / StorageEngine / system queries. |
| `prepared.rs` | `PreparedCache` wrapping moka, `PreparedPlan` struct, schema invalidation sweep. |

## Modified Files

| File | Change |
|------|--------|
| `Cargo.toml` | Add `moka = { version = "0.12", features = ["sync"] }`, `ferrosa-sstable = { path = "../ferrosa-sstable" }` (for `Row`, `Partition`, `DeletionTime`, `LivenessInfo` — not re-exported by ferrosa-storage), and `indexmap = "2"` (for `IndexMap` in bridge) |
| `lib.rs` | Add module declarations for new files |
| `connection.rs` | Replace stub with full protocol handler |
| `server.rs` | Add `SharedState` struct, pass to connection tasks |

## Component Details

### 1. Lexer

Single-pass tokenizer producing `Token<'input>` that borrows from the source
string. Token types:

- Keywords (resolved via `phf` map): SELECT, INSERT, UPDATE, DELETE, CREATE,
  ALTER, DROP, FROM, WHERE, AND, OR, IN, SET, INTO, VALUES, IF, EXISTS, NOT,
  PRIMARY, KEY, TABLE, KEYSPACE, ROLE, GRANT, REVOKE, ON, TO, OF, USE, BATCH,
  BEGIN, APPLY, UNLOGGED, COUNTER, LOGGED, TRUNCATE, ORDER, BY, ASC, DESC,
  LIMIT, ALLOW, FILTERING, WITH, REPLICATION, DURABLE_WRITES, PASSWORD,
  SUPERUSER, LOGIN, NOSUPERUSER, NOLOGIN, TRUE, FALSE, NULL, INT, BIGINT,
  TEXT, VARCHAR, BLOB, BOOLEAN, FLOAT, DOUBLE, TIMESTAMP, UUID, TIMEUUID,
  INET, COUNTER (type), VARINT, DECIMAL, DATE, TIME, SMALLINT, TINYINT,
  LIST, MAP, SET (type), TUPLE, FROZEN, STATIC, CLUSTERING, COMPACT, STORAGE,
  ASCII, TTL, WRITETIME, TOKEN, IF_NOT_EXISTS (synthetic), IF_EXISTS (synthetic)
- Identifiers: unquoted (`[a-zA-Z_][a-zA-Z0-9_]*`) and quoted (`"..."`)
- Literals: string (`'...'`), integer, float, UUID, hex blob (`0x...`), boolean, null
- Bind markers: `?` (positional) and `:name` (named)
- Operators: `=`, `<`, `>`, `<=`, `>=`, `!=`, `+`, `-`, `(`, `)`, `,`, `.`, `;`, `[`, `]`, `{`, `}`, `:`
- Whitespace and comments (`--` line, `/* */` block) are skipped

### 2. Parser

One function per grammar rule. LL(2) — peek at current and next token, no
backtracking. Returns `Result<Statement, CqlError::SyntaxError>`.

#### AST Types

```rust
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateKeyspace(CreateKeyspaceStatement),
    AlterKeyspace(AlterKeyspaceStatement),
    DropKeyspace(DropKeyspaceStatement),
    CreateTable(CreateTableStatement),
    AlterTable(AlterTableStatement),
    DropTable(DropTableStatement),
    CreateRole(CreateRoleStatement),
    AlterRole(AlterRoleStatement),
    DropRole(DropRoleStatement),
    Grant(GrantStatement),
    Revoke(RevokeStatement),
    Use(UseStatement),
    Batch(BatchStatement),
    Truncate(TruncateStatement),
}
```

**SelectStatement fields**: keyspace, table, columns (list or `Star`),
where_clauses, order_by, limit, allow_filtering.

**InsertStatement fields**: keyspace, table, columns, values
(`Vec<Term>`), if_not_exists, using_timestamp, using_ttl.

**UpdateStatement fields**: keyspace, table, assignments
(`Vec<(String, Term)>`), where_clauses, if_exists,
using_timestamp, using_ttl.

**DeleteStatement fields**: keyspace, table, columns (list or all),
where_clauses, if_exists, using_timestamp.

**BatchStatement fields**: batch_type (`Logged | Unlogged | Counter`),
statements (`Vec<Statement>` — only Insert/Update/Delete allowed),
using_timestamp.

**CreateTableStatement fields**: keyspace, name, columns
(`Vec<(String, CqlTypeName)>`), partition_key (`Vec<String>`),
clustering_key (`Vec<(String, ClusteringOrder)>`),
if_not_exists, table_options.

**WhereClause**: `{ column: String, op: ComparisonOp, value: Term }`
where `ComparisonOp` is `Eq | Lt | Gt | Le | Ge | In | Ne`.
`Ne` is only relevant for IF conditions in LWT (out of scope), but
included for parse completeness — the router rejects it with
`CqlError::Invalid("!= not supported in WHERE clause")`.

**Term** uses parser-level literal types, not `CqlValue`, because the
parser doesn't know the target column type at parse time:

```rust
pub enum Term {
    StringLiteral(String),
    IntegerLiteral(i64),
    FloatLiteral(f64),
    UuidLiteral(Uuid),
    BlobLiteral(Vec<u8>),
    BoolLiteral(bool),
    Null,
    BindMarker(Option<String>),   // ? or :name
    InList(Vec<Term>),
    ListLiteral(Vec<Term>),       // [a, b, c]
    MapLiteral(Vec<(Term, Term)>),// {k: v, ...}
    SetLiteral(Vec<Term>),        // {a, b, c}
    TupleLiteral(Vec<Term>),      // (a, b, c)
}
```

The bridge converts `Term` to `CqlValue` using the target column's `CqlType`
from `TableMetadata`. For example, `IntegerLiteral(42)` becomes
`CqlValue::Int(42)` or `CqlValue::Bigint(42)` depending on the column type.
This means type coercion and validation happen at the bridge layer, not the
parser.

DDL for keyspaces, roles, grant/revoke: fields match the corresponding
`ferrosa-schema` API parameters directly.

**Unsupported statements**: CREATE INDEX, CREATE VIEW, CREATE FUNCTION,
CREATE AGGREGATE, CREATE TRIGGER, CREATE TYPE → return
`CqlError::SyntaxError("not yet supported: <statement>")`.

### 3. Bridge

Stateless module of pure functions in `bridge.rs`. Converts between
protocol-level types (`CqlValue`, `Statement`) and storage-level types
(`DecoratedKey`, `Row`, `Partition`).

**Key function signatures:**

```rust
/// Build a DecoratedKey from parsed partition key values.
pub fn build_decorated_key(
    pk_values: &[CqlValue],
    pk_types: &[CqlType],
) -> Result<DecoratedKey, CqlError>;

/// Build a storage Row from parsed column values.
/// column_map maps column names to (column_index, CqlType).
pub fn build_row(
    columns: &[String],
    values: &[CqlValue],
    column_map: &IndexMap<String, (u16, CqlType)>,
    clustering_columns: &[(String, CqlType)],
    timestamp: i64,
    ttl: Option<i32>,
) -> Result<Row, CqlError>;

/// Build a tombstone Row for DELETE.
pub fn build_delete_row(
    columns: Option<&[String]>,
    column_map: &IndexMap<String, (u16, CqlType)>,
    clustering_values: &[CqlValue],
    clustering_types: &[CqlType],
    timestamp: i64,
) -> Result<Row, CqlError>;

/// Convert a Partition from storage into result rows.
/// Returns (column_names, column_types, rows) for the result encoder.
pub fn partition_to_rows(
    partition: &Partition,
    table: &TableMetadata,
    selected_columns: &[String],  // or all columns if SELECT *
) -> Result<(Vec<String>, Vec<CqlType>, Vec<Vec<Option<CqlValue>>>), CqlError>;

/// Convert a parser-level Term to a typed CqlValue using the target column type.
/// IntegerLiteral(42) + CqlType::Int → CqlValue::Int(42), etc.
pub fn term_to_cql_value(term: &Term, target_type: &CqlType) -> Result<CqlValue, CqlError>;

/// Parse a CQL type string (from ColumnMetadata.column_type) into CqlType.
pub fn parse_cql_type(s: &str) -> Result<CqlType, CqlError>;
```

**Write path** (INSERT/UPDATE → `StorageEngine::write()`):

1. Resolve table from schema snapshot → `TableMetadata`
1. Extract partition key column names and types from `TableMetadata`
1. `build_decorated_key()`: encode each PK value via `CqlValue::encode_value()`,
   compose into `PartitionKey` (single-column: raw bytes; multi-column:
   `[2-byte len][value bytes][0x00]` per component), compute token via
   `DecoratedKey::new(partition_key)` which calls murmur3 internally
1. `build_row()`: for each column value: if null, produce
   `CellValue::tombstone(timestamp, now_seconds)`. Otherwise call
   `CqlValue::encode_value()` → `Vec<u8>`, wrap in
   `CellValue::live(bytes, timestamp)` (or `CellValue::expiring()` if
   USING TTL). Map column names to `u16` indices
   via `TableMetadata.columns` position. Encode clustering columns via
   length-prefixed concatenation (see clustering key encoding below).
   Null values in clustering skip write. Construct `Row { clustering, cells,
   deletion: DeletionTime::LIVE, primary_key_liveness:
   LivenessInfo::with_timestamp(timestamp) }`
1. Construct `TableId::new(&keyspace, &table)` and call
   `engine.write(&table_id, &key, row, timestamp)`

**Delete path** (DELETE → `StorageEngine::write()`):

- **Partition-level delete** (no WHERE on clustering): write with
  `Row { deletion: DeletionTime { marked_for_delete_at: timestamp, .. }, .. }`
- **Row-level delete** (WHERE specifies clustering key): same, with
  clustering bytes set and `deletion` populated
- **Column-level delete** (DELETE specific columns): write `CellValue::tombstone(timestamp, now_seconds)` for each named column

**Type note**: `DeletionTime.local_deletion_time` is `u32`,
`CellValue.local_deletion_time` is `i32`. Bridge casts via `as u32`/`as i32`
as needed (both represent seconds since epoch; the value range is identical
in practice).

**Read path** (`StorageEngine::read()` → result rows):

1. `engine.read(&table_id, &key)` → `Option<Partition>`
1. `partition_to_rows()`: iterate `partition.rows`, for each row:
   - Decode clustering bytes back to column values by reading
     length-prefixed segments (reverse of the encoding in `build_row`)
   - For each selected column, find `(u16, CellValue)` in `row.cells`
     by column index. Call `CqlValue::decode_value(&cql_type,
     &cell_value.value.as_deref().unwrap())`. Tombstones
     (`cell_value.value == None`) → `None` (null in result).
   - Handle `partition.static_row` if the table has static columns
1. Return column names, types, and row data for the result encoder

**CQL type resolution** — `parse_cql_type()`:

`ColumnMetadata.column_type` is a string like `"text"`, `"int"`,
`"frozen<map<text, int>>"`. The bridge provides `parse_cql_type()` which
parses these strings into `CqlType`. This is also used by the parser for
`CqlTypeName` in CREATE TABLE → `CqlType` conversion.

**Partition key serialization**:

- Single-column: `CqlValue::encode_value()` → `PartitionKey::new(bytes)`
- Multi-column composite: `[2-byte len][value bytes][0x00]` per component,
  concatenated → `PartitionKey::new(bytes)`
- Token computed by `DecoratedKey::new(partition_key)` (calls murmur3
  internally, no direct hash call needed)

**Clustering key encoding**: The bridge implements its own clustering key
serialization in `bridge.rs`. For each clustering column value, encode via
`CqlValue::encode_value()` and concatenate with 2-byte length prefixes
(matching Cassandra's `ClusteringPrefix` format). This produces the
`Vec<u8>` for `Row.clustering`. Note: `ferrosa_sstable::byte_comparable`
encodes `DecoratedKey` (token + partition key) — it does NOT handle
clustering keys. The `ferrosa-sstable` dependency is needed for `Row`,
`Partition`, `DeletionTime`, and `LivenessInfo` types (which are defined
there and re-exported through `ferrosa-storage`).

### 4. Result Encoder

Encoding functions in `result.rs` that produce `Bytes` for RESULT frame
bodies. No decoding (we only emit results, never parse them).

**Result kinds:**

| Kind | Code | Encoding |
|------|------|----------|
| Void | 0x0001 | `[int 0x0001]` |
| Rows | 0x0002 | `[int 0x0002][metadata][int rows_count][rows...]` |
| SetKeyspace | 0x0003 | `[int 0x0003][string keyspace]` |
| Prepared | 0x0004 | `[int 0x0004][short id_len][bytes id][result_metadata][bound_metadata]` |
| SchemaChange | 0x0005 | `[int 0x0005][string change_type][string target][string options...]` |

**Rows metadata**: `[int flags][int columns_count][column_specs...]` where
each column spec is `[string ks][string table][string name][short type_id][type_params]`.
The `NO_METADATA` flag (0x0004) skips column specs for prepared execute.

**Row data**: `[int cell_length][bytes cell_value]` per column, `-1` for null.

### 5. Router

`router.rs` — central dispatch from parsed AST to subsystems.

**Shared state** (held as `Arc`, same instance for all connections):

```rust
pub struct SharedState {
    pub engine: Arc<StorageEngine>,
    pub schema: Arc<Schema>,
    pub node_config: Arc<NodeConfig>,
    pub cluster_state: Arc<dyn ClusterState>,
    pub prepared_cache: Arc<PreparedCache>,
}
```

No separate `ArcSwap<SchemaSnapshot>` — `schema.snapshot()` already returns
`Arc<SchemaSnapshot>` via its internal `ArcSwap`, so callers just call
`state.schema.snapshot()` when they need a consistent view.

`NodeConfig` (from `ferrosa_schema::system::local`) is needed for
`system.local` queries. `cluster_state` implements `ClusterState` trait
(from `ferrosa_schema::system::peers`) for `system.peers` queries. For
single-node mode, the router defines `SingleNodeClusterState` that returns
an empty vec from `peers()`. This gets replaced by a real implementation
when ferrosa-cluster is built.

**Per-request context**:

```rust
pub struct RequestContext<'a> {
    pub auth: &'a AuthContext,
    pub current_keyspace: &'a Option<String>,
}
```

**Dispatch rules:**

| Statement | Target | Result Kind |
|-----------|--------|-------------|
| Select on `system.local` | `query_local(&schema, &node_config)` → `LocalInfo` → rows | Rows |
| Select on `system.peers` / `system.peers_v2` | `query_peers(&schema, &cluster_state)` → `Vec<PeerInfo>` → rows | Rows |
| Select on `system_schema.keyspaces` | `query_keyspaces(&snap)` → `Vec<KeyspaceRow>` → rows | Rows |
| Select on `system_schema.tables` | `query_tables(&snap)` → `Vec<TableRow>` → rows | Rows |
| Select on `system_schema.columns` | `query_columns(&snap)` → `Vec<ColumnRow>` → rows | Rows |
| Select on `system_auth.roles` | `query_roles(&snap, &auth)` → `Vec<RoleRow>` → rows | Rows |
| Select on `system_auth.role_members` | `query_role_members(&snap)` → `Vec<RoleMemberRow>` → rows | Rows |
| Select on `system_auth.role_permissions` | `query_role_permissions(&snap)` → `Vec<RolePermissionRow>` → rows | Rows |
| Select on user table | `StorageEngine::read()` → bridge → encode rows | Rows |
| Insert / Update / Delete | bridge → `StorageEngine::write()` | Void |
| CreateKeyspace / AlterKeyspace / DropKeyspace | `Schema::create_keyspace()` etc. | SchemaChange |
| CreateTable | `Schema::create_table()` + convert `TableMetadata` → `TableSchema` + `engine.register_table()` | SchemaChange |
| AlterTable / DropTable | `Schema::alter_table()` / `drop_table()` etc. | SchemaChange |
| CreateRole / AlterRole / DropRole | `Schema::create_role(role, password, auth)` etc. | Void |
| Grant / Revoke | `Schema::grant()` / `revoke()` | Void |
| Use | set connection-local keyspace | SetKeyspace |
| Batch | iterate statements, same dispatch | Void |
| Truncate | flush table (proper truncate is follow-on) | Void |

**CREATE TABLE registration**: After `Schema::create_table()` succeeds, the
router calls `table_metadata.to_storage_schema()` (from
`ferrosa_schema::convert`) which handles the full conversion from
`TableMetadata` (CQL-level type names like `"text"`) to
`ferrosa_common::TableSchema` (Cassandra marshal class names like
`org.apache.cassandra.db.marshal.UTF8Type`). This includes composite key
handling, column sorting by position, and collection type mapping. Then call
`engine.register_table(table_schema)` so the storage engine creates the
table's directory and memtable. `TableId` is constructed via
`TableId::new(&keyspace, &table)` — this is used for all `engine.read()`
and `engine.write()` calls.

**System query rows**: Each `query_*()` function returns typed structs
(`LocalInfo`, `PeerInfo`, `KeyspaceRow`, `TableRow`, `ColumnRow`). The
router converts these to result rows by mapping struct fields to
`Vec<Option<CqlValue>>` matching the CQL column spec for each system table.
This conversion lives in the router (not the bridge) since it's system-table
specific and not reusable.

**Table resolution**: Explicit keyspace (`ks.table`) takes precedence.
Otherwise use connection's current keyspace. If neither, return
`CqlError::Invalid("no keyspace specified")`.

**WHERE enforcement**: WHERE must fully specify the partition key with `=`
(all components for composite keys). If not, return
`CqlError::Invalid("partition key must be fully specified")`.
`ALLOW FILTERING` parsed but returns `CqlError::Invalid("ALLOW FILTERING is not supported")`.

### 6. Prepared Statement Cache

`prepared.rs` — wraps `moka::sync::Cache`.

```rust
pub struct PreparedPlan {
    pub id: [u8; 16],                       // MD5 of query string
    pub query: String,                       // original query text
    pub statement: Statement,                // parsed AST
    pub keyspace: Option<String>,            // keyspace at prepare time
    pub result_metadata: ResultMetadata,     // column specs for result set
    pub bound_metadata: BoundMetadata,       // bind marker types and names
}

pub struct PreparedCache {
    cache: moka::sync::Cache<[u8; 16], Arc<PreparedPlan>>,
}
```

**PREPARE flow**: Parse query → resolve table from schema → extract column
types → compute MD5 → store `Arc<PreparedPlan>` in cache → return Prepared
result with ID + metadata.

**EXECUTE flow**: Look up ID. Miss → `CqlError::Unprepared(id)`. Hit →
bind provided values to markers, dispatch through router.

**Schema invalidation**: On schema version change (compare UUIDs from
`SchemaSnapshot`), background sweep evicts entries whose referenced tables
no longer exist or whose column definitions changed. The sweep is async
and non-blocking — a stale hit during the window returns slightly outdated
metadata (drivers handle re-preparing).

**Capacity**: Weight-based, default 10 MiB. Each entry's weight is
`query.len() + size_of::<Statement>() + metadata sizes`.

**moka policy**: If we encounter bugs or missing features in moka during
testing, we create patches and submit upstream PRs rather than forking or
working around.

### 7. Connection Handler

Replaces the stub in `connection.rs` with a full protocol handler.

**Connection lifecycle:**

```
Accept TCP → Framed<TcpStream, CqlCodec>
  → STARTUP/OPTIONS phase
  → Auth phase (if enabled): AUTHENTICATE → AUTH_RESPONSE → AUTH_SUCCESS
  → Ready phase: process requests until disconnect
```

**Per-connection state:**

```rust
struct ConnectionState {
    auth_context: Option<AuthContext>,
    current_keyspace: Option<String>,
    auth_attempts: u32,
}
```

**Request dispatch by opcode:**

| Opcode | Handler |
|--------|---------|
| Startup | Validate `CQL_VERSION` in body. Auth disabled → READY. Auth enabled → AUTHENTICATE. |
| Options | Return SUPPORTED (`CQL_VERSION: 3.4.7`, `COMPRESSION: []`) |
| AuthResponse | Parse SASL PLAIN → `Schema::authenticate()`. Success → AUTH_SUCCESS. Failure → ERROR(BadCredentials), close after 3 attempts. |
| Query | Parse query string + params from body. Route through router. Encode result. |
| Prepare | Parse query from body. Build `PreparedPlan`, cache. Return Prepared result. |
| Execute | Read prepared ID + bound values. Look up plan, bind, route. |
| Batch | Read batch type + statements. Parse or look up prepared for each. Route. First failure aborts. |
| Register | Parse event types. Store on connection. Return READY. Event push is follow-on. |

**Stream ID**: Every response carries the request's stream ID. Enables
driver-side multiplexing.

**Error handling**: `CqlError` from any layer → ERROR frame on the same
stream ID. Connection stays open. Only protocol-level errors (bad version,
malformed frame) close the connection.

**Server changes**: `CqlServer` gains a `SharedState` field. Construction
takes `Arc<StorageEngine>`, `Arc<Schema>`, etc. Each spawned connection
task receives a clone of `SharedState`.

## Deferred Items

| Item | Tracked In | Notes |
|------|-----------|-------|
| Frame compression (LZ4/Snappy) | CQL spec | Flag parsed but not applied |
| EVENT push notifications | Connection handler | Register accepted, events not pushed |
| `max_in_flight_per_connection` enforcement | `server.rs:25` TODO | Accept but don't enforce limit |
| `ALLOW FILTERING` / full table scan | Router | Returns Invalid error |
| Range scans / multi-partition SELECT | Router | Requires read_range iterator work |
| Logged batch atomicity | Router | Logged batches execute as unlogged (no coordinator log) |
| CREATE INDEX / VIEW / FUNCTION / etc. | Parser | Returns SyntaxError("not yet supported") |
| UDT support | Parser + types | Deferred to schema Chunk C |
| Query tracing | Connection handler | Tracing flag parsed but not acted on |

## Testing Strategy

| Layer | Approach | Examples |
|-------|----------|---------|
| Lexer | Unit tests, pure input → tokens | Keywords, identifiers, string/number/uuid literals, operators, unterminated string, unicode |
| Parser | Unit tests, pure string → AST | One test per statement type, bind markers, composite primary keys, malformed queries → SyntaxError |
| Bridge | Unit tests against known byte patterns | Int/text/blob round-trip, composite partition key, null handling, clustering key encoding |
| Result encoder | Unit tests, verify wire format | Rows with known types, Void, Prepared metadata, SchemaChange |
| Router | Integration with real Schema + StorageEngine (in-memory) | INSERT→SELECT, CREATE KEYSPACE→CREATE TABLE→INSERT→SELECT, USE changes default, auth-gated DDL |
| Prepared | Unit tests with moka cache | Prepare→execute round-trip, unprepared ID error, schema invalidation evicts stale |
| Connection | Integration with raw TCP frames | Full handshake, auth flow, QUERY→RESULT, bad version rejected, stream ID preserved |
| Property tests | proptest | Parser safety (arbitrary strings don't panic), CqlValue bridge round-trip |

No mocks — real Schema and StorageEngine with in-memory flush targets and
temp directories, following the pattern established by ferrosa-storage tests.

## Dependencies

| Crate | Version | Purpose | Notes |
|-------|---------|---------|-------|
| `moka` | 0.12 | Prepared statement cache | `sync` feature. Lock-free reads. W-TinyLFU eviction. |
| `indexmap` | 2 | Ordered maps in bridge | Used for column ordering in bridge functions. |
| `ferrosa-sstable` | path | `Row`, `Partition`, `DeletionTime`, `LivenessInfo` types | Not re-exported by `ferrosa-storage`. Direct dep needed. |

All other dependencies (`tokio`, `tokio-util`, `bytes`, `futures`,
`arc-swap`, `uuid`, `num-bigint`, `phf`, `md-5`, `tracing`) are already
in Cargo.toml from Part A.

## Related Specs

- [CQL Architecture Spec](../../../specs/cql.md) — protocol spec and architecture
- [ADR-006](../../../specs/decisions/006-cql-architecture.md) — CQL architectural decisions
- [CQL Design (Part A)](2026-03-12-ferrosa-cql-design.md) — Part A implementation design
- [Storage Spec](../../../specs/storage.md) — StorageEngine API
- [Schema Design](2026-03-12-ferrosa-schema-design.md) — Schema registry API
