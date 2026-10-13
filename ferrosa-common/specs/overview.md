---
crate: ferrosa-common
status: implemented
last_updated: 2026-06-19
executive_summary: >
  The leaf crate at the bottom of the Ferrosa dependency graph. It owns the
  shared low-level type vocabulary — Token/DecoratedKey, CellValue, the full
  CqlType/CqlValue model, the workspace Error/Result, Accord HLC/timestamp/ballot
  types, Cassandra-compatible Murmur3, TableSchema, and a runtime-aware TaskPool.
  Several types live here specifically to break dependency cycles (CqlType/CqlValue
  out of ferrosa-cql; TableSchema out of ferrosa-sstable). It depends on no other
  Ferrosa crate and is depended on by essentially every one of them.
---

# ferrosa-common — Architecture Overview

## Purpose & boundary

`ferrosa-common` is the **single shared type vocabulary** for the workspace. Its
boundary is deliberately minimal: pure data types plus small, total methods, with
no I/O anywhere except `HybridLogicalClock` reading the system clock. It knows
nothing about wire framing, query planning, storage layout, transport, or
consensus orchestration — those live in the crates above it.

It exists because many crates must agree **byte-for-byte and bit-for-bit** on a
handful of primitives: the hash-ring `Token`/`DecoratedKey`, the storage
`CellValue`, the CQL type model, and the workspace `Error`. Placing them at the
bottom of the graph lets cycle-prone types be shared without an upward
dependency — `CqlType`/`CqlValue` were moved out of `ferrosa-cql` so
`ferrosa-udf` can use them without the large CQL crate, and `TableSchema` lives
here so storage and schema share it without a cycle through `ferrosa-sstable`.

## Module map

| Module | LoC | Responsibility |
|--------|-----|----------------|
| `accord` (`src/accord.rs`) | 1177 | Accord `Timestamp`, `TxnId`, ballot newtypes, `HybridLogicalClock` (lock-free, drift-checked `merge`), `BallotGenerator`, `TxnPhase`/`TxnState` |
| `cancel` (`src/cancel.rs`) | ~260 | `CancelToken`/`CancelReason`/`Cancelled` (T-021): a `Relaxed`-atomic flag plus a crossbeam `closed()` channel a `select!` blocks on without polling |
| `schema` (`src/schema.rs`) | 637 | `TableSchema`, `ColumnDefinition`, `PinConfig`; fail-loud `fixed_width_for_marshal_type` / `validate_cell_bytes` / `validate_clustering_shape`; legacy column-order detector |
| `geometry` (`src/geometry.rs`) | 436 | `Geometry` (Point + single-ring Polygon), WKB `marshal_wkb` / `parse_wkb` |
| `cql_type` (`src/cql_type.rs`) | 334 | `CqlType` (full type tree + `type_id`), `CqlValue` (runtime value, IEEE-754-total `Ord`), bigint serde |
| `murmur3` (`src/murmur3.rs`) | 283 | Cassandra-bit-compatible `hash3_x64_128` (preserves the tail sign-extension bug) |
| `key` (`src/key.rs`) | 176 | `PartitionKey`, `DecoratedKey` (cached token, token-then-bytes order, `filter_hash`) |
| `error` (`src/error.rs`) | 168 | `Error` / `Result`; typed `CorruptSstable` repair signal; `is_backpressure` |
| `cell` (`src/cell.rs`) | 151 | `CellValue` live/expiring/tombstone + sentinels |
| `data_type` (`src/data_type.rs`) | 89 | `DataType` scalar descriptor (exhaustive) |
| `token` (`src/token.rs`) | 79 | `Token` newtype + `from_key` |
| `timeuuid` (`src/timeuuid.rs`) | 187 | v1 TimeUUID byte layout (`v1_timeuuid`) for the synthetic `_sys_ck_` key of a PK-less Postgres table, plus `SYNTHETIC_KEY_COLUMN` / `is_reserved_column_name` guarding the reserved `_sys_` prefix. No clock of its own: callers pass `(time, clock_seq, node)` in, so uniqueness stays the caller's guarantee |
| `task_pool` (`src/task_pool.rs`) | 71 | `TaskPool` runtime-aware spawn helper |
| `test_generators` (`src/test_generators.rs`) | ~140 | proptest strategies (feature `test-generators`) for cells/keys and shrink-friendly generated DDL/snapshot table identities, drop markers, and index declarations |
| `lib` (`src/lib.rs`) | 39 | module declarations + headline re-exports |

## Data flow / role

`ferrosa-common` is not a pipeline; it is the **shared alphabet** the pipelines
speak. Two representative roles:

**Placement.** A partition key's raw bytes →
`Token::from_key` (`murmur3::hash3_x64_128` `h1`) → `DecoratedKey { token, key }`.
The cluster layer uses the token to pick owning nodes; the storage/SSTable layer
uses the same `DecoratedKey` ordering (token, then key bytes) for on-disk
position; `filter_hash` feeds Bloom double-hashing. One hash, computed once,
agreed on everywhere.

**Typed failure.** When a read can't be resolved because an SSTable is corrupt,
`ferrosa-storage` raises `Error::CorruptSstable { gen, min_token, max_token }`.
The read coordinator calls `corrupt_sstable_range()` (never string-matches the
message) to fail over to another replica and target anti-entropy repair at
exactly the corrupt token range.

```mermaid
flowchart TD
    PK["PartitionKey (bytes)"] --> M["murmur3::hash3_x64_128"]
    M --> TOK["Token (h1)"]
    TOK --> DK["DecoratedKey { token, key }"]
    DK --> CL["ferrosa-cluster: node placement"]
    DK --> SS["ferrosa-sstable: on-disk order + Bloom (h1,h2)"]
    CT["CqlType / CqlValue"] --> CQL["ferrosa-cql / ferrosa-postgres / ferrosa-udf"]
    ERR["Error::CorruptSstable {gen,min,max}"] --> RC["read coordinator: failover + targeted repair"]
```

## Key invariants

1. **Leaf crate — no Ferrosa dependency.** `ferrosa-common` depends on no other
   workspace crate. This is structural: it is what lets `CqlType`/`CqlValue` and
   `TableSchema` be shared without cycles. Adding a Ferrosa dependency here would
   re-introduce the cycle the move was meant to break.
2. **Murmur3 is bit-identical to Cassandra.** `hash3_x64_128` must reproduce
   `org.apache.cassandra.utils.MurmurHash.hash3_x64_128` exactly, **including**
   the asymmetric tail handling (sign-extended tail bytes vs. zero-extended
   body). Characterization vectors enforce this; changing it re-tokenizes every
   key and silently corrupts placement.
3. **`DecoratedKey` orders by token, then key bytes.** Matches Cassandra's
   `DecoratedKey.compareTo`; both SSTable order and ring ownership depend on it.
4. **`CqlValue` has a total order across all variants.** Float/Double/Vector use
   `total_cmp` and a fixed cross-type discriminant index, so `Ord` is total even
   for NaN — required wherever values are used as sorted keys.
5. **`CorruptSstable` is a typed signal, never string-matched.** The repair range
   is read via `corrupt_sstable_range()`.
6. **`#[non_exhaustive]` on `Error` only.** `DataType` dropped it (jsonb M3) so
   downstream matches are exhaustive and the compiler flags every site when a
   variant is added. `CqlValue::cmp` and its storage/schema callers have no
   wildcard arms either.

## Position in the dependency graph

The bottom. Depends on no Ferrosa crate (external only: `num-bigint`, `serde`,
`uuid`, `tokio`, `crossbeam-channel` (T-021), optional `proptest`). Depended on by essentially every other
crate in the workspace — see the [README dependency list](../README.md#dependencies)
and the [root crate index](../../specs/crates.md) for the full graph.

Cancellation uses a one-slot Crossbeam channel solely for disconnection. No
payload is sent or consumed; every cloned receiver observes closure, including
clones created after cancellation. Fixed capacity avoids zero-channel select
packet allocations in storage pump waits (T-081).

## jsonb (T-150)

`CqlType::Jsonb` and `CqlValue::Jsonb(ferrosa_jsonb::JsonbValue)` (T-150). The value is validated and never raw bytes; `Ord`/`Eq`/`Hash` delegate to `JsonbValue` (D18, D2a) and `discriminant_index` gives it index 27. The name registry has `jsonb` with the POC marshal class `org.apache.cassandra.db.marshal.JsonbType`; `custom_class_type` resolves only that class (D20) and `check_jsonb_nesting` rejects `set<jsonb>`, `map<jsonb, _>` and `vector<jsonb>` at any depth (D21). `jsonb_canonical_text` prints a value. New edge: `ferrosa-common` -> `ferrosa-jsonb` (leaf; `guard-arrow-free.sh` passes).
