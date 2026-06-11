# Dogfood `system_schema` — Cassandra-style schema-as-data

**Status:** Proposed (design, grounded). Not implemented.
**Author:** blueprint, 2026-06-10.
**Prompted by:** the index-subsystem refactor's Phase 0b (index reload-survival). Today a
node restart drops every secondary index because the storage engine persists only
`Vec<TableSchema>` to `schema.json` (no indexes), and `load_local_schema_if_present`
re-registers tables with `register_table_inner(schema, vec![])`. The chosen fix is the
Cassandra-faithful one: make `system_schema.*` **real, persisted tables** (durable as
SSTables, rebuilt at boot), starting with `system_schema.indexes`.

## How Cassandra does it (reference)

Cassandra stores schema **as data dogfooded through its own storage engine** — the
`system_schema` keyspace (`keyspaces`, `tables`, `columns`, `indexes`, `types`,
`functions`, `aggregates`, `views`, `triggers`, `dropped_columns`). These are ordinary
tables → SSTables on disk; the schema is itself queryable CQL data. Cluster propagation
is via schema-version digests + pulled mutations (≤4.x) or a Raft-style log (5.0 / CEP-21
TCM). No JSON file.

## Where ferrosa is today (verified)

- **Authoritative cluster schema = Raft.** DDL → `ferrosa-cluster/src/ddl_path.rs`
  (`create_index_internal`) → Raft `RaftOp::CreateIndex` applied in
  `ferrosa-cluster/src/raft/state_machine.rs:1023` → updates the **in-memory** Registry.
- **Registry** (`ferrosa-schema/src/registry.rs:124` `Schema` over `ArcSwap<SchemaSnapshot>`,
  l.38) holds `indexes: HashMap<(ks,tbl,name), IndexMetadata>` (l.52). In-memory only.
- **`system_schema.indexes` is VIRTUAL** — `SystemSchemaIndexesTable::read()`
  (`ferrosa-schema/src/system/index_tables.rs:18`) walks `snap.indexes` on every SELECT;
  registered as a virtual table at `registry.rs:233`; the CQL router falls through to the
  virtual-table path (`ferrosa-cql/src/router.rs` ~2348).
- **Local durability = `schema.json`** (`engine.rs:2430 persist_schema_locally`) — writes
  `Vec<TableSchema>`, **no indexes**. Reload `engine.rs:2454/2471` → `register_table_inner(.., vec![])`.
- **Precedent: `system_auth.*` are already dogfooded** as real SSTables. DDL applies
  `SystemTableWriter::new(engine).apply(SystemTableMutation::RoleCreated/GrantUpdated/…)`
  (`ferrosa-cluster/src/ddl_path.rs:240-263`; writer in
  `ferrosa-cluster/src/system_table_writer.rs:31`). Persisted set in
  `ferrosa-schema/src/system/persistence.rs:482 all_system_table_schemas`. Row encoders like
  `keyspace_to_row`/`table_to_rows` (persistence.rs:153/200) and column-index constants
  (`*_COL_*`) are the template. **`registry.rs:3040 apply_snapshot_restores_indexes` already
  proves the Registry can be rebuilt from a snapshot incl. indexes.**

So this is *not* a from-scratch rewrite — it extends the existing `system_auth` dogfooding
pattern to `system_schema.indexes` (and, later, the rest of `system_schema`).

## Design — dogfood `system_schema.indexes` first

Cassandra shape: PK `keyspace_name`, clustering `(table_name, index_name)`, regular
`kind`, `options` (+ ferrosa adds `target` and the index type so reload is lossless).

1. **Persisted table schema.** Add `system_schema.indexes` to
   `persistence.rs:all_system_table_schemas` with columns
   `(keyspace_name PK, table_name+index_name clustering, kind, target, options)` and the
   `INDEXES_COL_*` position constants (mirror `COLUMNS_COL_*`). It then registers as a real
   table at boot (`engine.register_system_tables`, engine.rs:706).
2. **Mutation + row encoder** (`ferrosa-schema/src/system/persistence.rs`): add
   `SystemTableMutation::IndexCreated(IndexMetadata)` and `IndexDropped { keyspace, table, name }`
   (l.84), and `index_to_rows(&IndexMetadata) -> SystemRow` mirroring `table_to_rows`
   (composite clustering `[u16 len][table][u16 len][index_name]`, cells = kind/target/options,
   tombstone for dropped). Unit-test the encode round-trip.
3. **DDL wiring** (`ferrosa-cluster/src/system_table_writer.rs` apply() + the three DDL apply
   sites: `ddl_path.rs:CreateIndex`, `pair/ddl.rs:331`, `raft/state_machine.rs:1023`): after
   `schema.create_index_internal(idx)`, also `SystemTableWriter::apply(IndexCreated(idx))` —
   exactly like `Grant`/`Revoke`. Adding the enum variants forces handling in `apply()`
   (compiler-enforced exhaustiveness).
4. **Startup reconstruction** (the one step beyond auth): on boot, read
   `system_schema.indexes` rows → for each, rebuild the Registry **and** call
   `engine.add_index(name, column_position, index_type)` so the index is live (memtable
   indexing + the eager/backfill build pipeline) and correctly typed. Resolve
   `target` column name → `column_position` via the table schema. This replaces the
   `register_table_inner(.., vec![])` gap and composes with the Phase 0 type-threading
   (`store.index_type_for`). Handles the chicken-and-egg the same way auth does: system tables
   are registered (engine.rs:706) before user-schema restore.
5. **Query path.** Switch `system_schema.indexes` SELECT from the virtual table to the normal
   storage read (retire `SystemSchemaIndexesTable`); the Registry remains the in-memory cache,
   now rebuilt from storage at boot.

## Phased TDD plan

1. **Encoder + mutation (additive).** `IndexCreated`/`IndexDropped` + `index_to_rows` +
   `INDEXES_COL_*` + the persisted schema. RED: encode an `IndexMetadata` → assert the row's
   PK/clustering/cells; decode round-trip. No behavior change until wired.
2. **DDL writes the row.** Wire the three apply sites; RED: after `CREATE INDEX`,
   `SELECT * FROM system_schema.indexes` (still virtual) AND a direct read of the stored table
   both show the index; the stored row survives a flush.
3. **Startup reconstruction.** RED: create index → flush → drop+rebuild the engine (reload) →
   the index is registered (queryable, typed via `index_type_for`, and a build job carries the
   real type). This is the headline reload-survival test.
4. **Retire the virtual table.** Switch the query path to storage; assert
   `system_schema.indexes` is served from the stored table and parity with the old virtual rows.

## Broader follow-on (full Cassandra model)

Extend the same pattern to the rest: make `system_schema.types/functions/aggregates/views`
stored (today hardcoded/empty in the router), and make `keyspaces/tables/columns` fully
storage-served rather than computed-from-Registry. End state: `system_schema` is entirely
schema-as-data, `schema.json` is retired (or kept only as a legacy import path), and the
Registry is a pure in-memory cache rebuilt from `system_schema` SSTables at boot — matching
Cassandra. This is a larger effort and can land table-by-table after indexes.

## Critical files

- Schema/encoders: `ferrosa-schema/src/system/persistence.rs` (mutation enum l.84, encoders
  l.150+, `all_system_table_schemas` l.482, `*_COL_*` constants), `ferrosa-schema/src/metadata/index.rs`
  (`IndexMetadata`), `ferrosa-schema/src/system/index_tables.rs` (virtual table to retire),
  `ferrosa-schema/src/registry.rs` (`apply_snapshot_restores_indexes` l.3040).
- Writer + DDL: `ferrosa-cluster/src/system_table_writer.rs`, `ferrosa-cluster/src/ddl_path.rs`,
  `ferrosa-cluster/src/pair/ddl.rs`, `ferrosa-cluster/src/raft/state_machine.rs`.
- Startup + reload: `ferrosa-storage/src/engine.rs` (`register_system_tables` l.706,
  `load_local_schema_if_present` l.2454, `register_table_inner` l.2311, `add_index` +
  `store.index_type_for`), `ferrosa/src/main.rs` boot sequence (l.630-825).
- Query path: `ferrosa-cql/src/router.rs` (system_schema dispatch ~1705-2358).

## Risks / notes

- **Bootstrap ordering** (chicken-and-egg): `system_schema.indexes` must be a registered table
  before its rows are read at boot — solved exactly as auth/keyspaces are (system tables
  registered at engine.rs:706 before user-schema restore).
- **Two sources during migration:** until step 4, the Registry (virtual) and the stored table
  coexist; keep them consistent (DDL writes both) and cut over atomically in step 4.
- **Column-position resolution:** the stored row carries the `target` column *name*; reload
  resolves it to a position via the table schema (the Phase 0 `add_index(col_pos, type)` API).
- **Composes with the index refactor:** this is Phase 0b. Phases 1-4 (build/read dispatch,
  validation, geospatial) sit on top and need indexes to be live + typed after restart — which
  this provides.
- **Scope:** indexes-first is the unblocking slice; the full `system_schema` dogfood is a
  larger, table-by-table follow-on.
