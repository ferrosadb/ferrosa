//! Module: the write half of a general `UPDATE` / `DELETE` (lane W2).
//!
//! Correctness: a statement is executed as one read and a stream of keyed writes.
//!
//! 1. **Read.** `ferrosa_sql::open_dml_cursor` runs a general `SELECT` over the
//!    target joined with the `FROM` list and the CTEs. It runs under the
//!    statement's snapshot with the transaction's own writes overlaid, exactly as a
//!    `SELECT` does. It yields one row per target row: the storage key, each `SET`
//!    value, the `RETURNING` values over the new row, and the old row.
//! 2. **Write.** Each row becomes one keyed write through the ordinary write path
//!    ([`crate::query::apply_or_buffer_silent`]). So transactions, Accord, writer
//!    ownership and foreign keys behave as for any other write
//!    (`specs/lean4/Ferrosa/Sql/Dml.lean`, `read_then_write_eq_declarative`).
//!
//! A transaction's read set records every relation the read touched
//! (`DmlStmt::read_select`, see `server::track_select_reads`).
//!
//! **READ COMMITTED (decision D16).** The writer takes ownership of the row, then
//! re-reads it. If the row is no longer the one the snapshot read, the statement
//! fails with 40001 rather than re-evaluating its `WHERE` against the new version.
//! So two claimers of one row can never both write it (`rc_no_double_claim`).
//!
//! **Bounded memory.** Rows stream from the read; writes go to the transaction's
//! spilling write-set. `RETURNING` rows wait in a spilling replay buffer until
//! every write succeeded, so a client never sees a row of a statement that then
//! failed partway through its writes. A failure is an `ErrorResponse` and never a
//! command tag. No step falls back silently.
//!
//! Last revised: 2026-11-08

use std::collections::HashMap;

use ferrosa_common::{CqlType, CqlValue};
use ferrosa_schema::{ColumnKind, SchemaSnapshot, TableMetadata};
use ferrosa_sql::spill::{ReplayBuffer, SeqRow};
use ferrosa_sql::{
    Column, ColumnType, DmlAction, DmlLayout, DmlSpec, DmlStmt, Projection, Row, SpillCtx,
    Value as SqlValue,
};
use ferrosa_storage::Mutation;

use crate::messages::BackendMessage;
use crate::mvcc::{MvccCommitError, PgWrite};
use crate::query::{
    error_response, exec_error_response, row_description_fields, value_to_cql,
    write_error_response, ReadEnv, ReplySink, ReturningOpts,
};
use crate::result_stream::{open_cursor_stream, ResultStream, RESULT_BATCH_ROWS};
use crate::txn_write_set::TxnWriteSet;

/// The SQLSTATE and message of a READ COMMITTED write over a row that moved (D16).
const MOVED: (&str, &str) = (
    "40001",
    "could not serialize access due to concurrent update",
);

/// The table a statement writes, resolved once.
struct Target<'a> {
    meta: &'a TableMetadata,
    ks: String,
    table: String,
}

impl<'a> Target<'a> {
    fn resolve(
        snap: &'a SchemaSnapshot,
        default_schema: &str,
        dml: &DmlStmt,
    ) -> Result<Self, BackendMessage> {
        let ks = dml
            .target
            .schema
            .clone()
            .unwrap_or_else(|| default_schema.to_string());
        let table = dml.target.table.clone();
        let meta = snap
            .tables
            .get(&(ks.clone(), table.clone()))
            .ok_or_else(|| {
                error_response(
                    "42P01",
                    &format!("relation \"{ks}.{table}\" does not exist"),
                )
            })?;
        Ok(Self { meta, ks, table })
    }

    /// The storage key, partition columns then clustering columns.
    fn key_columns(&self) -> Vec<String> {
        self.meta
            .partition_key
            .iter()
            .cloned()
            .chain(
                self.meta
                    .clustering_key
                    .iter()
                    .map(|(name, _)| name.clone()),
            )
            .collect()
    }

    fn cql_type(&self, env: &ReadEnv<'_>, column: &str) -> Result<CqlType, BackendMessage> {
        let meta = self.meta.columns.get(column).ok_or_else(|| {
            error_response(
                "42703",
                &format!(
                    "column \"{column}\" of relation \"{}\" does not exist",
                    self.table
                ),
            )
        })?;
        ferrosa_row_bridge::parse_cql_type_in_keyspace(&meta.column_type, &self.ks, env.schema)
            .map_err(|e| error_response("42704", &e.to_string()))
    }

    fn to_cql(
        &self,
        env: &ReadEnv<'_>,
        column: &str,
        value: &SqlValue,
    ) -> Result<CqlValue, BackendMessage> {
        value_to_cql(value, &self.cql_type(env, column)?, env.jsonb_limits)
    }
}

/// One row of the read, split by the layout. Values are moved out, not copied.
struct ReadRow {
    keys: Vec<SqlValue>,
    sets: Vec<SqlValue>,
    returning: Vec<SqlValue>,
    old: Vec<SqlValue>,
}

impl ReadRow {
    fn split(row: Row, layout: &DmlLayout) -> Result<Self, BackendMessage> {
        let want = layout.old_start() + layout.old.len();
        if row.0.len() != want {
            return Err(error_response(
                "XX000",
                &format!(
                    "DML read row has {} values, layout wants {want}",
                    row.0.len()
                ),
            ));
        }
        let mut values = row.0.into_iter();
        let mut take = |n: usize| values.by_ref().take(n).collect::<Vec<_>>();
        Ok(Self {
            keys: take(layout.keys.len()),
            sets: take(layout.sets.len()),
            returning: take(layout.returning.len()),
            old: take(layout.old.len()),
        })
    }
}

/// How the statement's output is shaped and where its rows came from.
pub(crate) struct DmlRun<'a> {
    pub(crate) env: ReadEnv<'a>,
    pub(crate) params: Vec<SqlValue>,
    pub(crate) returning: ReturningOpts<'a>,
}

/// Execute a general `UPDATE`/`DELETE`. In an open transaction (`txn`) the writes
/// are buffered and commit with it. Without one, they commit here as one atomic
/// batch. `RETURNING` rows go to `out`. The returned messages close the reply.
///
/// # Errors
///
/// An I/O error from `out`: the client went away.
pub(crate) async fn execute_general_dml<O: ReplySink>(
    run: DmlRun<'_>,
    dml: &DmlStmt,
    txn: Option<&mut TxnWriteSet>,
    out: &mut O,
) -> std::io::Result<Vec<BackendMessage>> {
    let snap = run.env.schema.snapshot();
    let target = match Target::resolve(&snap, run.env.default_schema, dml) {
        Ok(target) => target,
        Err(error) => return Ok(vec![error]),
    };
    let standalone = txn.is_none();
    let mut local = TxnWriteSet::default();
    let set = match txn {
        Some(set) => set,
        None => &mut local,
    };
    let written = match read_and_write(&run, dml, &target, &snap, set).await {
        Ok(written) => written,
        Err(error) => return Ok(vec![error]),
    };
    if standalone {
        if let Err(error) = commit_standalone(&run.env, local) {
            return Ok(vec![error]);
        }
    }
    emit(out, &run.returning, dml, written).await
}

/// What the write phase produced: the row count and the buffered `RETURNING`.
struct Written {
    count: usize,
    layout: DmlLayout,
    returning: Option<Returned>,
}

/// `RETURNING` rows, spilled past the threshold, with the directory they spill to.
struct Returned {
    rows: ReplayBuffer,
    _reservation: ferrosa_storage::TempSortTableReservation,
}

async fn read_and_write(
    run: &DmlRun<'_>,
    dml: &DmlStmt,
    target: &Target<'_>,
    snap: &SchemaSnapshot,
    set: &mut TxnWriteSet,
) -> Result<Written, BackendMessage> {
    let spec = DmlSpec {
        key_columns: target.key_columns(),
        old_row: true,
    };
    let pending = set
        .pending_writes()
        .map_err(|e| error_response("58000", &format!("write-set could not be read back: {e}")))?;
    let (mut stream, layout) = open_read(run, dml, &spec, &pending).await?;
    drop(pending);
    let mut returning = match dml.returning {
        Some(_) => Some(returned_buffer()?),
        None => None,
    };
    let writer = Writer {
        env: &run.env,
        target,
        snap,
        layout: &layout,
        update: matches!(dml.action, DmlAction::Update(_)),
    };
    let mut count = 0usize;
    while let Some(row) = stream.next_row().await? {
        let row = ReadRow::split(row, &layout)?;
        let ReadRow {
            keys,
            sets,
            returning: values,
            old,
        } = row;
        writer.write(set, keys, sets, old).await?;
        if let Some(buf) = returning.as_mut() {
            buf.rows
                .push(SeqRow::new(count as u64, Row(values)))
                .map_err(|e| error_response("58000", &format!("RETURNING spill: {e}")))?;
        }
        count += 1;
    }
    Ok(Written {
        count,
        layout,
        returning,
    })
}

fn returned_buffer() -> Result<Returned, BackendMessage> {
    const LABEL: &str = "dml_returning";
    let ctx = SpillCtx::default();
    let reservation = ctx
        .reserve(LABEL)
        .map_err(|e| error_response("58000", &format!("RETURNING spill: {e}")))?;
    let rows = ReplayBuffer::new(reservation.path(), &ctx, LABEL);
    Ok(Returned {
        rows,
        _reservation: reservation,
    })
}

/// Load every relation the statement reads and open its read on a blocking thread.
async fn open_read(
    run: &DmlRun<'_>,
    dml: &DmlStmt,
    spec: &DmlSpec,
    pending: &[PgWrite],
) -> Result<(ResultStream, DmlLayout), BackendMessage> {
    let env = &run.env;
    let read = dml.read_select();
    let (catalog, failure) = crate::query::load_catalog_with_mvcc(
        env.engine,
        env.schema,
        &read,
        env.default_schema,
        env.mvcc,
        env.snapshot,
        Some(pending),
        env.cluster_read.reader(),
        Some((&run.params, env.jsonb_limits)),
    )
    .await?;
    let layout = ferrosa_sql::describe_dml(dml, spec, &catalog, env.default_schema)
        .map_err(|e| exec_error_response(&e))?;
    let (stmt, spec) = (dml.clone(), spec.clone());
    let (schema, params) = (env.default_schema.to_string(), run.params.clone());
    let stream = open_cursor_stream(catalog, failure, false, move |catalog| {
        let ctx = SpillCtx::default();
        ferrosa_sql::open_dml_cursor(&stmt, &spec, catalog, &schema, &params, &ctx)
            .map(|(cursor, _)| cursor)
    })
    .await?;
    Ok((stream, layout))
}

/// Writes one read row by its key.
struct Writer<'a> {
    env: &'a ReadEnv<'a>,
    target: &'a Target<'a>,
    snap: &'a SchemaSnapshot,
    layout: &'a DmlLayout,
    update: bool,
}

impl Writer<'_> {
    async fn write(
        &self,
        set: &mut TxnWriteSet,
        keys: Vec<SqlValue>,
        sets: Vec<SqlValue>,
        old: Vec<SqlValue>,
    ) -> Result<(), BackendMessage> {
        let (pk, ck) = self.storage_key(&keys)?;
        let meta = self.target.meta;
        let owned = crate::query::own_row(self.env.mvcc, Some(&mut *set), meta.id, &pk).await?;
        if owned {
            self.recheck(set, &pk, &ck, &old)?;
        }
        let mutation = if self.update {
            self.update_mutation(&pk, &ck, sets)?
        } else {
            self.delete_mutation(&pk, &ck, old)?
        };
        crate::query::apply_or_buffer_silent(
            self.env.engine,
            self.env.schema,
            self.env.mvcc,
            Some(set),
            PgWrite(mutation, meta.id),
        )
        .await
        .map_err(first_error)
    }

    /// The row's partition and clustering values, in key order.
    fn storage_key(
        &self,
        keys: &[SqlValue],
    ) -> Result<(Vec<CqlValue>, Vec<CqlValue>), BackendMessage> {
        let pk_len = self.target.meta.partition_key.len();
        if keys.len() != self.layout.keys.len() || keys.len() < pk_len {
            return Err(error_response(
                "XX000",
                "DML read row has a malformed storage key",
            ));
        }
        let mut cql = Vec::with_capacity(keys.len());
        for (column, value) in self.layout.keys.iter().zip(keys) {
            if value.is_null() {
                return Err(error_response(
                    "XX000",
                    &format!("storage key column \"{}\" read as NULL", column.name),
                ));
            }
            cql.push(self.target.to_cql(self.env, &column.name, value)?);
        }
        let ck = cql.split_off(pk_len);
        Ok((cql, ck))
    }

    /// D16: the owned row must still be the one the read saw.
    fn recheck(
        &self,
        set: &mut TxnWriteSet,
        pk: &[CqlValue],
        ck: &[CqlValue],
        old: &[SqlValue],
    ) -> Result<(), BackendMessage> {
        let current = crate::query::read_current_row_for_update(
            self.env.engine,
            self.env.schema,
            &self.target.ks,
            &self.target.table,
            pk,
            ck,
            Some(set),
        )?;
        match current {
            Some(row) if row.0.as_slice() == old => Ok(()),
            _ => Err(error_response(MOVED.0, MOVED.1)),
        }
    }

    fn update_mutation(
        &self,
        pk: &[CqlValue],
        ck: &[CqlValue],
        sets: Vec<SqlValue>,
    ) -> Result<Mutation, BackendMessage> {
        let meta = self.target.meta;
        let mut cells: Vec<(u16, CqlValue)> = Vec::with_capacity(sets.len());
        let mut assigned: HashMap<String, CqlValue> = HashMap::with_capacity(sets.len());
        for (column, value) in self.layout.sets.iter().zip(sets) {
            let name = column.name.as_str();
            let kind = meta.columns.get(name).map(|c| c.kind);
            if !matches!(kind, Some(ColumnKind::Regular | ColumnKind::Static)) {
                return Err(error_response(
                    "0A000",
                    &format!("cannot UPDATE key column \"{name}\" in SET"),
                ));
            }
            let index = meta.storage_column_index(name).ok_or_else(|| {
                error_response(
                    "42703",
                    &format!("column \"{name}\" not found in storage schema"),
                )
            })?;
            let cql = self.target.to_cql(self.env, name, &value)?;
            assigned.insert(name.to_string(), cql.clone());
            cells.push((index, cql));
        }
        crate::pg_fk::check_child_row(self.env.engine, self.env.schema, meta, &assigned)
            .map_err(|(code, message)| error_response(&code, &message))?;
        let timestamp = crate::query::dml_timestamp();
        let key = ferrosa_row_bridge::build_decorated_key(pk, &[])
            .map_err(|e| error_response("22000", &e.to_string()))?;
        let row = ferrosa_row_bridge::build_row(&cells, ck, timestamp, None);
        Ok(Mutation::new(
            self.target.ks.clone(),
            self.target.table.clone(),
            key,
            vec![row],
            timestamp,
        ))
    }

    fn delete_mutation(
        &self,
        pk: &[CqlValue],
        ck: &[CqlValue],
        old: Vec<SqlValue>,
    ) -> Result<Mutation, BackendMessage> {
        let mut values: HashMap<String, CqlValue> = HashMap::with_capacity(old.len());
        for (column, value) in self.layout.old.iter().zip(old) {
            let cql = self.target.to_cql(self.env, &column.name, &value)?;
            values.insert(column.name.clone(), cql);
        }
        crate::pg_fk::check_parent_row(self.env.engine, self.snap, self.target.meta, &values)
            .map_err(|(code, message)| error_response(&code, &message))?;
        crate::query::build_delete_mutation(&self.target.ks, &self.target.table, pk, ck)
    }
}

/// The write path reports a list; the statement reports its first error.
fn first_error(mut messages: Vec<BackendMessage>) -> BackendMessage {
    if messages.is_empty() {
        return error_response("XX000", "a write failed without an error message");
    }
    messages.swap_remove(0)
}

/// No transaction: commit the statement's writes as one atomic batch.
fn commit_standalone(env: &ReadEnv<'_>, set: TxnWriteSet) -> Result<(), BackendMessage> {
    if set.is_empty() {
        return Ok(());
    }
    let staged = set.into_staged().map_err(|e| write_error_response(&e))?;
    let Some(mvcc) = env.mvcc else {
        return env
            .engine
            .write_atomic_batch(staged)
            .map_err(|e| write_error_response(&e));
    };
    let committed = crate::query::commit_mutations(
        env.engine,
        env.schema,
        mvcc,
        &mvcc.snapshot(),
        &std::collections::HashSet::new(),
        staged,
    );
    match committed {
        Ok(_) => Ok(()),
        Err(MvccCommitError::SerializationFailure) => Err(error_response(
            "40001",
            "could not serialize PostgreSQL transaction",
        )),
        Err(MvccCommitError::Storage(error)) => Err(write_error_response(&error)),
        Err(error) => Err(error_response("58000", &format!("write failed: {error:?}"))),
    }
}

/// `RETURNING *` hides ferrosa's reserved columns, as `SELECT *` does.
fn visible_returning(dml: &DmlStmt, columns: &[Column]) -> Vec<usize> {
    let star = matches!(dml.returning, Some(Projection::Star));
    columns
        .iter()
        .enumerate()
        .filter(|(_, c)| !(star && ferrosa_common::timeuuid::is_reserved_column_name(&c.name)))
        .map(|(i, _)| i)
        .collect()
}

/// Send the `RETURNING` rows (after every write succeeded) and the command tag.
async fn emit<O: ReplySink>(
    out: &mut O,
    opts: &ReturningOpts<'_>,
    dml: &DmlStmt,
    written: Written,
) -> std::io::Result<Vec<BackendMessage>> {
    let tag = vec![BackendMessage::CommandComplete {
        tag: format!("{} {}", dml.verb(), written.count),
    }];
    let Some(mut returned) = written.returning else {
        return Ok(tag);
    };
    let visible = visible_returning(dml, &written.layout.returning);
    let columns: Vec<Column> = visible
        .iter()
        .map(|&i| written.layout.returning[i].clone())
        .collect();
    if opts.with_row_description {
        let fields = row_description_fields(&columns, opts.result_formats);
        out.send(vec![BackendMessage::RowDescription { fields }])
            .await?;
    }
    let types: Vec<ColumnType> = columns.iter().map(|c| c.ty).collect();
    let mut rows = match returned.rows.replay() {
        Ok(rows) => rows,
        Err(e) => {
            return Ok(vec![error_response(
                "58000",
                &format!("RETURNING replay: {e}"),
            )])
        }
    };
    loop {
        let mut batch = Vec::with_capacity(RESULT_BATCH_ROWS);
        for item in rows.by_ref().take(RESULT_BATCH_ROWS) {
            match item {
                Ok(seq_row) => batch.push(project(seq_row.row, &visible)),
                Err(e) => {
                    return Ok(vec![error_response(
                        "58000",
                        &format!("RETURNING replay: {e}"),
                    )])
                }
            }
        }
        if batch.is_empty() {
            return Ok(tag);
        }
        let (_, encode_error) = out
            .send_rows(batch.into_iter(), &types, opts.result_formats)
            .await?;
        if let Some(error) = encode_error {
            return Ok(vec![error]);
        }
    }
}

/// Keep the `visible` values of a row, moving them.
fn project(row: Row, visible: &[usize]) -> Row {
    if visible.len() == row.0.len() {
        return row;
    }
    let mut slots: Vec<Option<SqlValue>> = row.0.into_iter().map(Some).collect();
    Row(visible.iter().filter_map(|&i| slots[i].take()).collect())
}

/// `Describe` of a prepared general `UPDATE`/`DELETE`: each parameter's type OID
/// (the client's declared OID where non-zero, else the inferred one) and the
/// `RETURNING` columns (`None` without `RETURNING`). Reads only schemas.
pub(crate) async fn describe(
    ctx: &crate::server::QueryContext,
    dml: &DmlStmt,
    declared: &[i32],
) -> Result<(Vec<i32>, Option<Vec<Column>>), BackendMessage> {
    let snap = ctx.schema.snapshot();
    let target = Target::resolve(&snap, &ctx.default_schema, dml)?;
    let spec = DmlSpec {
        key_columns: target.key_columns(),
        old_row: true,
    };
    let read = dml.read_select();
    let (catalog, _scans) =
        crate::query::load_catalog(&ctx.engine, &ctx.schema, &read, &ctx.default_schema).await?;
    let describe_error = |e: ferrosa_sql::ExecError| crate::extended::describe_exec_error(&e);
    let layout = ferrosa_sql::describe_dml(dml, &spec, &catalog, &ctx.default_schema)
        .map_err(describe_error)?;
    let inferred = ferrosa_sql::infer_dml_param_types(dml, &spec, &catalog, &ctx.default_schema)
        .map_err(describe_error)?;
    let oids = inferred
        .iter()
        .enumerate()
        .map(|(i, ty)| match declared.get(i).copied() {
            Some(oid) if oid != 0 => oid,
            _ => crate::query::column_type_oid(*ty),
        })
        .collect();
    let returning = dml.returning.as_ref().map(|_| {
        visible_returning(dml, &layout.returning)
            .into_iter()
            .map(|i| layout.returning[i].clone())
            .collect()
    });
    Ok((oids, returning))
}
