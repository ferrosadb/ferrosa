# ferrosa-common

> The leaf crate: the shared low-level type vocabulary (`Token`,
> `DecoratedKey`, `CellValue`, `CqlType`/`CqlValue`, `Error`) that every other
> Ferrosa crate builds on.

## What this crate is

`ferrosa-common` is the **bottom of the dependency graph**. It depends on no
other Ferrosa crate and is depended on by essentially every other crate in the
workspace. It owns the small set of types that the storage engine, the CQL/PG
front-ends, the cluster layer, and the index/UDF crates all have to agree on:
the hash-ring `Token` and `DecoratedKey`, the storage `CellValue`, the CQL type
model (`CqlType`/`CqlValue`), the workspace-wide `Error`/`Result`, the Accord
HLC/timestamp/ballot types, and the Cassandra-compatible Murmur3 hash.

Several of these types live here specifically to **break dependency cycles**:
`CqlType`/`CqlValue` were moved out of `ferrosa-cql` so `ferrosa-udf` (and
others below `ferrosa-cql`) can reference them without pulling in the large CQL
crate; `TableSchema` lives here so storage and schema can share it without a
cycle through `ferrosa-sstable`. Wire-format CQL encode/decode does **not** live
here — it lives in `ferrosa-cql` / `ferrosa-row-bridge`.

## What's implemented

- **Hash ring** — `Token` (`i64` newtype over Murmur3 `h1`), `Token::from_key`,
  `Token::MIN`/`MAX`; `murmur3::hash3_x64_128` (Cassandra-bit-compatible,
  including the deliberate tail sign-extension bug).
- **Keys** — `PartitionKey` (raw bytes) and `DecoratedKey` (key + cached token,
  ordered by token then key bytes, with `filter_hash` for Bloom double-hashing).
- **Storage cell** — `CellValue` with live / expiring / tombstone constructors
  and `is_live`/`is_tombstone`/`is_expiring`; sentinels `NO_TIMESTAMP`,
  `NO_TTL`, `NO_DELETION_TIME`.
- **CQL type model** — `DataType` (scalar descriptor, `#[non_exhaustive]`),
  `CqlType` (full type tree incl. List/Map/Set/Tuple/Udt/Vector, with protocol
  `type_id()`), and `CqlValue` (runtime value with manual IEEE-754-total `Ord`).
- **Errors** — `Error` (`#[non_exhaustive]`) + `Result`; notable typed variant
  `Error::CorruptSstable { gen, min_token, max_token }` with `corrupt_sstable()`
  / `corrupt_sstable_range()` for failover + targeted repair, and
  `is_backpressure()` for overload classification.
- **Accord primitives** — `Timestamp`, `TxnId`, `BallotNumber` /
  `AcceptedBallot` / `PromisedBallot` (type-safe role separation),
  `HybridLogicalClock` (lock-free, drift-rejecting `merge`), `BallotGenerator`,
  `TxnPhase` / `TxnState`.
- **Schema** — `TableSchema`, `ColumnDefinition`, `PinConfig` (NVMe pinning from
  table extensions), plus fail-loud helpers `fixed_width_for_marshal_type`,
  `validate_cell_bytes`, `validate_clustering_shape`, and the
  `legacy_storage_column_order_warning` detector.
- **Geometry** — `Geometry` (Point + single-ring Polygon), `marshal_wkb` /
  `parse_wkb` (fail-loud on unknown byte-order, unsupported type, trailing
  bytes, antimeridian crossing).
- **Task spawning** — `TaskPool`: an explicit spawn target wrapping an optional
  dedicated `tokio::runtime::Runtime`, with a documented `current()` fallback to
  ambient `tokio::spawn`.
- **Test generators** — behind the `test-generators` feature: proptest
  strategies (`arb_cell_value`, `arb_cell`, `arb_partition_key`,
  `arb_decorated_key`) shared across crates.

## How it works

One module per concern; all are pure data + small methods with no I/O except the
HLC reading the system clock:

- **`token`** / **`murmur3`** — the ring position and the hash that produces it.
- **`key`** — `PartitionKey` and `DecoratedKey` (token cached at construction).
- **`cell`** — `CellValue` state machine (live / expiring / tombstone).
- **`data_type`** / **`cql_type`** — the type descriptors and runtime values.
- **`error`** — workspace `Error`/`Result`, including the typed `CorruptSstable`
  repair signal.
- **`accord`** — Accord timestamps, ballots, HLC, and per-txn state.
- **`schema`** — `TableSchema` and the marshal-type / clustering validators.
- **`geometry`** — WKB marshal/parse for the supported geometry subset.
- **`task_pool`** — runtime-aware spawn helper.

The crate root (`lib.rs`) re-exports the headline types so downstream code
writes `ferrosa_common::{DecoratedKey, CqlValue, Error}` rather than reaching
into modules.

## Public API (key entry points)

| Area | Types / functions |
|------|-------------------|
| Ring | `Token`, `Token::from_key`, `murmur3::hash3_x64_128` |
| Keys | `PartitionKey`, `DecoratedKey`, `DecoratedKey::filter_hash` |
| Cells | `CellValue::{live,expiring,tombstone,is_live,is_tombstone,is_expiring}` |
| Types | `DataType`, `CqlType::type_id`, `CqlValue` |
| Errors | `Error`, `Result`, `Error::{corrupt_sstable,corrupt_sstable_range,is_backpressure}` |
| Accord | `Timestamp`, `TxnId`, `BallotNumber`/`AcceptedBallot`/`PromisedBallot`, `HybridLogicalClock`, `BallotGenerator`, `TxnPhase`, `TxnState` |
| Schema | `TableSchema`, `ColumnDefinition`, `PinConfig`, `fixed_width_for_marshal_type`, `validate_cell_bytes`, `validate_clustering_shape` |
| Geometry | `Geometry`, `marshal_wkb`, `parse_wkb` |
| Spawning | `TaskPool` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **NONE** — `ferrosa-common` is the **leaf crate**. It depends on no other
  Ferrosa crate, by design: it sits at the bottom of the graph so the cycle-prone
  shared types (`CqlType`/`CqlValue`, `TableSchema`) can be reused without
  pulling in `ferrosa-cql`. External deps only: `num-bigint`, `serde`, `uuid`,
  `tokio` (rt), and `proptest` (optional, `test-generators`).

**Called by** (crates that depend on this — essentially every crate):

- **`ferrosa`** — binary; uses the shared error/key/value model throughout.
- **`ferrosa-cdc`** — change-data types built on `CellValue`/`CqlValue`.
- **`ferrosa-cluster`** — ring placement via `Token`/`DecoratedKey`, Accord types.
- **`ferrosa-cql`** — `CqlType`/`CqlValue` (re-exported), `Error` mapping.
- **`ferrosa-ctl`** — CLI/TUI consumes the shared types for display.
- **`ferrosa-flight`** — Arrow Flight endpoint maps `CqlValue`/`CqlType`.
- **`ferrosa-graph`** — property-graph values over `CqlValue`.
- **`ferrosa-index`** — indexes keyed by `DecoratedKey`/`CqlValue`.
- **`ferrosa-index-builder`** — standalone builder shares key/value model.
- **`ferrosa-loadgen`** — generates rows using the shared cell/value types.
- **`ferrosa-net`** — internode framing of keys/tokens; `TaskPool` for runtimes.
- **`ferrosa-postgres`** — PG front-end reuses `CqlValue`/`Error`.
- **`ferrosa-row-bridge`** — encodes/decodes `CqlValue`/`CellValue`/`DecoratedKey`.
- **`ferrosa-schema`** — extends `TableSchema`/`ColumnDefinition`.
- **`ferrosa-session`** — session state over the shared error/value types.
- **`ferrosa-sparql`** — SPARQL results mapped to `CqlValue`.
- **`ferrosa-sstable`** — reads/writes `CellValue` and `DecoratedKey`; uses
  `Error::CorruptSstable`.
- **`ferrosa-storage`** — memtable/compaction keyed on `DecoratedKey`; raises
  `CorruptSstable`; spawns via `TaskPool`.
- **`ferrosa-udf`** — `CqlType`/`CqlValue` without a `ferrosa-cql` dependency.
- **`ferrosa-worker`** — background tasks via `TaskPool`.

## Tests

In-crate unit tests are healthy and co-located with each module: **95 `#[test]`
functions** across the crate (accord 28, schema 18, geometry 17, murmur3 7, key
6, token 5, cql_type 5, cell 4, error 3, data_type 2). Murmur3 is covered by
characterization vectors generated from Cassandra source for bit-exact
compatibility. Gaps and the highest-risk areas (HLC clock `expect`, geometry
subset) are tracked in [specs/fmea.md](specs/fmea.md) and
[specs/roadmap.md](specs/roadmap.md).

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, data flow
- [FMEA / known issues](specs/fmea.md) — failure modes + gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
