# ferrosa-row-bridge

> The single canonical CQL row codec + `Partition`→row decomposition, shared
> byte-for-byte by both query front-ends (`ferrosa-cql` and `ferrosa-postgres`).

## What this crate is

A small, dependency-light crate that owns the **storage-row encode/decode** and
the **`Partition` → result-row decomposition** that the CQL and Postgres
front-ends *must* agree on exactly. The logic originally lived inside
`ferrosa-cql`; it was extracted here (decision **D10**) so `ferrosa-postgres`
can reuse the *identical* encoder/decoder **without** depending on the ~54k-LOC
`ferrosa-cql` crate. Duplicating this logic would risk silently-divergent row
ordering — the top FMEA risk for the SQL front-end — so it lives here once.

## What's implemented

- **CQL wire codec** — `encode_value` / `decode_value` for the scalar CQL types
  (int family, text/ascii, bool, float/double, uuid/timeuuid, blob, timestamp,
  date, time, inet, decimal, varint).
- **CQL type-name parser** — `parse_cql_type` / `parse_cql_type_in_keyspace`.
- **Write-direction row assembly** — `build_decorated_key` (single + composite
  partition keys), `build_row`, `build_delete_row`, `encode_clustering`.
- **Read-direction decomposition** — `partition_to_rows`,
  `partition_to_rows_with_storage_mapping`, `partition_to_rows_with_clustering`,
  `write_partition_raw_rows_with_storage_mapping`, plus `decode_pk` /
  `decode_clustering` and the liveness helpers `cell_is_live` / `ldt_is_expired`.
- **`RowBridgeError`** — a minimal stand-in error; `ferrosa-cql` provides
  `From<RowBridgeError> for CqlError` at its re-export boundary.

## How it works

Two modules:

- **`codec`** (`src/codec.rs`) — value ↔ CQL wire bytes + the type-name parser.
- **`row`** (`src/row.rs`) — partition/clustering key decoders, the read-path
  decomposition (tombstone/TTL skipping, storage-order → table-order mapping),
  and the write-path row builders.

`ferrosa-cql` re-exports these at their original public paths
(`ferrosa_cql::types::{encode_value, decode_value}`,
`ferrosa_cql::bridge::{build_decorated_key, build_row, …}`), so its internal
callers are unaffected. `ferrosa-postgres` calls them directly.

## Public API (key entry points)

| Area | Functions |
|------|-----------|
| Codec | `encode_value`, `decode_value`, `parse_cql_type`, `parse_cql_type_in_keyspace` |
| Write rows | `build_decorated_key`, `build_row`, `build_delete_row`, `encode_clustering` |
| Read rows | `partition_to_rows[_with_storage_mapping|_with_clustering]`, `decode_pk`, `decode_clustering` |
| Liveness | `cell_is_live`, `ldt_is_expired` |
| Error | `RowBridgeError` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-common`** — `CqlValue`, `CqlType`, `DecoratedKey`, `PartitionKey`,
  `CellValue` (the shared type model it encodes/decodes).
- **`ferrosa-sstable`** — `Partition`, `Row`, `LivenessInfo`, `DeletionTime` (the
  storage row shapes it builds and decomposes).
- **`ferrosa-schema`** — keyspace/type resolution for `parse_cql_type_in_keyspace`.

External: `num-bigint`, `uuid`, `tracing`. **Never** depends on `ferrosa-cql`.

**Called by** (crates that depend on this):

- **`ferrosa-cql`** — re-exports the codec + decomposition at their original paths.
- **`ferrosa-postgres`** — reuses the exact write/read codec for its DML + reads.

## Tests

The canonical codec/row unit tests currently live in `ferrosa-cql`'s `bridge`
module (they were not moved with the functions). In-crate test coverage is a
tracked gap — see [specs/fmea.md](specs/fmea.md) and [specs/roadmap.md](specs/roadmap.md).

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, data flow
- [FMEA / known issues](specs/fmea.md) — failure modes + gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
