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

A simple `Q` query for `SELECT ... FROM t [JOIN ...] WHERE ...`. Storage rows
flow through a bounded provider into the synchronous executor, whose output is
streamed to the socket in bounded batches with backpressure (t_f348ba0b).

```mermaid
sequenceDiagram
    autonumber
    participant Drv as Postgres driver
    participant Srv as server::query_loop
    participant Q as query::execute_query_streaming
    participant SP as storage_provider::load_table
    participant Eng as ferrosa-storage StorageEngine
    participant RB as ferrosa-row-bridge
    participant RS as result_stream (blocking thread)
    participant SQL as ferrosa-sql::execute_streaming

    Drv->>Srv: Query 'Q' (SELECT ...)
    Srv->>Q: execute_query_streaming(engine, schema, sql, sink)
    Q->>Q: parse_statement(sql) (err =&gt; 42601)
    Q->>SP: load_catalog: load_table per FROM/JOIN
    Note over SP: R15 guard — schema metadata decides<br/>existence (missing =&gt; 42P01, not empty)
    SP->>Eng: range_iter(table_id) (async stream of Partition)
    Eng-->>SP: Partition*
    SP->>RB: partition_to_rows_with_storage_mapping
    RB-->>SP: one partition's decoded rows
    SP->>SP: bounded channel (64 rows) + live MVCC root + emitted keys
    SP-->>Q: MapCatalog (re-scannable streaming provider)
    Q->>RS: open_stream(select, catalog, params)
    RS->>SQL: execute_streaming(.., ChannelSink)
    SQL-->>RS: columns, then rows in batches (bounded channel)
    RS-->>Q: ResultStream (columns known)
    Q->>Drv: RowDescription
    loop per batch, until done or max_rows
        RS-->>Q: batch of rows
        Q->>Drv: DataRow* (socket write = backpressure)
    end
    Q-->>Srv: tail: CommandComplete, PortalSuspended, or ErrorResponse
    Srv->>Drv: tail, ReadyForQuery
```

Notes:

- The storage provider reads one partition at a time and applies backpressure
  through the bounded channel. The result side is a second bounded channel of
  row batches; the socket write is the backpressure. Memory is O(batch) whatever
  the result size. Extended-protocol `Execute` with `max_rows` stops after that
  many rows with `PortalSuspended` and parks the running query on the portal;
  the next `Execute` continues from the next row.
- A failure after rows were sent yields an `ErrorResponse` after those rows,
  never a `CommandComplete`.
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
- Autocommit commits through PostgreSQL MVCC, which records before/after row
  images around `write_atomic_batch`. Inside a `BEGIN`/`COMMIT` block, the write
  is buffered as a PostgreSQL-owned `PgWrite`; `COMMIT` submits the PG mutation
  batch, snapshot, and read/write table set through Accord when configured;
  replica apply stages and publishes row-version metadata for active snapshots.
  `ROLLBACK` discards buffered writes. Jepsen and fault/atomic-visibility
  verification remain outstanding; see [fmea.md](fmea.md) PG-11.
- The encoder is the single canonical `ferrosa-row-bridge` codec, so the row is
  byte-identical whether written via Postgres or CQL (no second encoder, D10).

## Direct READ COMMITTED statement and commit

1. Default/explicit RC and implicit statements take fresh committed views and
   overlay their own staged writes. SERIALIZABLE retains its BEGIN view.
2. UPDATE/DELETE select candidates through MVCC point/scan reads or a proven
   stable declared-key index. Each initially matching candidate waits for its
   table-UUID/partition hash stripe, then rechecks the winner and full predicate.
3. Mutations retain table UUIDs and spill through TxnWriteSet. Local FIFO requests
   record predecessor dependencies; cycle-closing requesters fail with `40P01`.
   Default lock timeout is disabled. Admission above 16,384 identities or 262,144
   held/pending references fails `53200`; errors discard the transaction's writes
   and ownership. Pending guards reclaim cancelled queued or granted reservations.
4. COMMIT validates original incarnation/lease boundaries, acquires remaining
   write scopes, and uses a fresh RC commit view (original for SERIALIZABLE).
   Ownership lives outside the consumed write source until atomic apply and MVCC
   publication both finish. Late bulk writers retain conservative conflict checks.
5. COMMIT/ROLLBACK, failure, disconnect or dropped execution futures release
   ownership. While execution is blocked, native EOF/Terminate/CancelRequest
   detection remains t_b254fd86. No statement or whole transaction is replayed.
6. Original BEGIN lifetime (600 seconds by default) survives RC refresh. Expiry
   wakes waiting acquisitions and releases idle owners without another client
   frame, reporting `40001`. The reaper never drops leases; pre-apply validation
   rejects expired work, while irreversible apply/publication retain ownership.

The local stripe domain has no remote coordinator; distributed RC remains a
separate requirement. SQL transaction-clock values do not change across statements.
