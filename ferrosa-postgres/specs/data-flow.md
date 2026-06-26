---
crate: ferrosa-postgres
doc: data-flow
last_updated: 2026-06-19
---

# ferrosa-postgres — Data Flow

How one `SELECT` and one `INSERT` travel from the Postgres wire, through
`parse → execute → storage`, and back. Both paths share the canonical
`ferrosa-row-bridge` codec (decision D10), so Postgres-written rows are
byte-identical to CQL.

## SELECT (read path)

A simple `Q` query for `SELECT ... FROM t [JOIN ...] WHERE ...`. Tables are
materialized from the async storage scan up front, then the sync engine runs.

```mermaid
sequenceDiagram
    autonumber
    participant Drv as Postgres driver
    participant Srv as server::query_loop
    participant Q as query::execute_query
    participant SP as storage_provider::load_table
    participant Eng as ferrosa-storage StorageEngine
    participant RB as ferrosa-row-bridge
    participant SQL as ferrosa-sql::execute

    Drv->>Srv: Query 'Q' (SELECT ...)
    Srv->>Q: execute_query(engine, schema, sql)
    Q->>Q: parse_statement(sql) (err =&gt; 42601)
    Q->>SP: load_catalog: load_table per FROM/JOIN
    Note over SP: R15 guard — schema metadata decides<br/>existence (missing =&gt; 42P01, not empty)
    SP->>Eng: range_iter(table_id) (async stream of Partition)
    Eng-->>SP: Partition*
    SP->>RB: partition_to_rows_with_storage_mapping
    RB-->>SP: Vec&lt;Vec&lt;Option&lt;CqlValue&gt;&gt;&gt;
    SP->>SP: cql_to_value per cell =&gt; InMemoryTable
    SP-->>Q: MapCatalog (sync-scannable snapshot)
    Q->>SQL: execute(select, catalog, params)
    SQL-->>Q: QueryResult (columns + rows)
    Q->>Q: render_result =&gt; RowDescription + DataRow* + CommandComplete "SELECT n"
    Q-->>Srv: Vec&lt;BackendMessage&gt;
    Srv->>Drv: RowDescription, DataRow*, CommandComplete, ReadyForQuery
```

Notes:

- The async `range_iter` stream is fully drained **before** the sync operators
  run — the engine is synchronous and must never `block_on` the runtime.
- Column order follows the table's declared (DDL) order via the shared bridge,
  matching the CQL `route_select` read path exactly.
- On any failure exactly one `ErrorResponse` is emitted (`42601` parse, `42P01`
  undefined table, `58000` storage, `42703`/`42702` column) — never a fake empty
  result.

## INSERT (write path)

A single-row `INSERT INTO t (cols...) VALUES (literals...)`. Values are resolved
to `CqlValue` by the target column's CQL type, then encoded with the SAME
`ferrosa-row-bridge` builders the engine and CQL decode.

```mermaid
sequenceDiagram
    autonumber
    participant Drv as Postgres driver
    participant Srv as server::query_loop
    participant Q as query::execute_insert
    participant Sch as ferrosa-schema (TableMetadata)
    participant RB as ferrosa-row-bridge
    participant Eng as ferrosa-storage StorageEngine

    Drv->>Srv: Query 'Q' (INSERT ...)
    Srv->>Q: execute_query =&gt; execute_insert
    Q->>Sch: snapshot().tables.get(ks, table) (missing =&gt; 42P01)
    Q->>Q: per column: parse_cql_type_in_keyspace + value_to_cql
    Note over Q: type mismatch =&gt; 42804<br/>out of range =&gt; 22003<br/>missing key col =&gt; 23502<br/>$N param =&gt; 0A000 (preview gap)
    Q->>RB: build_decorated_key(pk_values)
    Q->>RB: build_row(regular_cells, ck_values, ts)
    RB-->>Q: storage Row (cells sorted by storage index)
    Q->>Eng: write_atomic_batch([Mutation])
    Eng-->>Q: Ok (or 58000 on write error)
    Q-->>Srv: CommandComplete "INSERT 0 1"
    Srv->>Drv: CommandComplete, ReadyForQuery
```

Notes:

- `UPDATE` and `DELETE` follow the same shape: a full-primary-key equality
  `WHERE` identifies the row; `UPDATE` writes regular/static cells (blind
  upsert), `DELETE` writes a row-level tombstone (`build_delete_row`). Both
  report `1` because the Cassandra-style write has no match count.
- Autocommit (no open transaction) applies immediately via `write_atomic_batch`,
  as drawn above. Inside a `BEGIN`/`COMMIT` block the write is instead BUFFERED
  as a `TransactionWrite` (`apply_or_buffer`) and the whole write-set is applied
  atomically through the Accord `TransactionCommitter` on `COMMIT` (`commit_txn`);
  `ROLLBACK` discards the buffer (never applied). See [fmea.md](fmea.md) PG-1.
- The encoder is the single canonical `ferrosa-row-bridge` codec, so the row is
  byte-identical whether written via Postgres or CQL (no second encoder, D10).
