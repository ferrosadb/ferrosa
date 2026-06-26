---
crate: ferrosa-row-bridge
status: implemented
last_updated: 2026-06-19
executive_summary: >
  The canonical CQL row codec and Partition→row decomposition, extracted from
  ferrosa-cql (decision D10) so the CQL and Postgres front-ends share one
  byte-identical encoder/decoder. A divergent copy is the top SQL-front-end FMEA
  risk, so this logic lives here exactly once.
---

# ferrosa-row-bridge — Architecture Overview

## Purpose & boundary

`ferrosa-row-bridge` is the **single source of truth** for translating between
the in-memory CQL value model and the on-disk/wire storage-row representation.
Its boundary is deliberately narrow: it knows about `CqlValue`/`CqlType`
(`ferrosa-common`) and the SSTable `Partition`/`Row` shapes (`ferrosa-sstable`),
and nothing about either front-end's protocol framing, planning, or transport.

It exists because **two** front-ends (`ferrosa-cql`, `ferrosa-postgres`) must
produce and consume *byte-identical* storage rows. Decision **D10**: extract the
shared logic into a leaf-ish crate that `ferrosa-postgres` can depend on without
pulling in the large `ferrosa-cql` crate.

## Module map

| Module | Responsibility |
|--------|----------------|
| `codec` (`src/codec.rs`, ~690 LoC) | `encode_value`/`decode_value` (CQL wire bytes), `parse_cql_type[_in_keyspace]` |
| `row` (`src/row.rs`, ~489 LoC) | partition/clustering key decode, `Partition`→row decomposition, write-path row builders |
| `lib` (`src/lib.rs`) | `RowBridgeError`, public re-exports |

## Data flow

**Write path** (front-end → storage): a parsed value → `encode_value` → cell
bytes; partition-key values → `build_decorated_key` (single bare; composite
length-prefixed `[2-byte len][bytes][0x00]`); regular cells + clustering →
`build_row` / `build_delete_row`. The result is a storage `Row` the engine
persists. `ferrosa-cql` and `ferrosa-postgres` call the *same* functions, so a
row written via Postgres decodes identically over CQL.

**Read path** (storage → front-end): a storage `Partition` →
`partition_to_rows_with_storage_mapping` → `Vec<Vec<Option<CqlValue>>>`,
applying tombstone + TTL-expiry skipping and the storage-column-order →
table-column-order mapping. Variants pair rows with raw clustering bytes (for
paging cursors) or emit raw byte slices.

## Key invariants

1. **Byte-identical encoding across front-ends.** `encode_value` and the row
   builders must be the only encoder; both front-ends route through them.
2. **Cells sorted by storage column index.** `build_row` sorts cells by index —
   the SSTable reader reads them in index order, so out-of-order cells corrupt
   reads (100% data loss for that row). Enforced in `build_row`.
3. **NULL is a tombstone, not an empty cell.** An explicit `Null` value emits a
   cell tombstone so reads return NULL, not `""`/`0`.
4. **No dependency on `ferrosa-cql`.** Enforced structurally (it would create a
   cycle: `ferrosa-cql` → `ferrosa-row-bridge`).

## Position in the dependency graph

Leaf-adjacent: depends only on `ferrosa-common`, `ferrosa-sstable`,
`ferrosa-schema`. Depended on by `ferrosa-cql` and `ferrosa-postgres`. See the
[root crate index](../../specs/crates.md) for the full graph.
