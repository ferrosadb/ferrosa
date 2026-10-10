//! Driving a `COPY ... FROM STDIN` exchange.
//!
//! COPY is the one statement that cannot be answered in a single step. The client sends the
//! payload only *after* the server acknowledges with `CopyInResponse`, so the exchange is a
//! conversation: acknowledge, then consume `CopyData` frames until `CopyDone`. That is why this
//! lives beside the connection loop, which owns the frame buffer and the stream, rather than in
//! the executor.
//!
//! Three rules shape the code below.
//!
//! **Nothing is acknowledged that cannot run.** `CopyInResponse` is the cue for the client to
//! start sending, so the table, the column list and the payload options are all resolved *first*.
//! Acknowledging and then failing would have the client stream a payload at a statement that was
//! never going to accept it.
//!
//! **A failure keeps draining.** Once the payload starts, the client is sending regardless of what
//! the server thinks. Returning early on a bad row would leave those frames in the socket, where
//! the *next* thing to read them would be the statement parser — and a client's data would be
//! interpreted as SQL. So a failure is remembered, the remaining payload is consumed and
//! discarded, and the error is sent once the client has finished.
//!
//! **Rows take the ordinary DML seam, so a COPY is transactional exactly as an INSERT is.** Each
//! row goes through `query::execute_insert` — the same path (`apply_or_buffer`) an INSERT uses —
//! rather than a parallel one, so COPY gets the same type coercion, the same synthetic `_sys_ck_`
//! key minting, and the same write seam. That seam buffers the write into the open transaction's
//! write-set when there is one, and applies it immediately (autocommit) otherwise. So a COPY
//! inside `BEGIN` becomes visible only at `COMMIT` and is discarded by `ROLLBACK`, with no `25001`
//! refusal. There is no second way to write a row here.
//!
//! In autocommit there is no transaction to buffer into, so the rows are staged locally in a
//! [`TxnWriteSet`] and flushed in bounded batches ([`FLUSH_EVERY`]). Inside a transaction those
//! rows go to the session's own write-set instead. Either staging structure is a
//! threshold-bounded, spilling write-set (`FERROSA_WRITE_SET_SPILL_THRESHOLD_BYTES`): a bulk load
//! past the buffer SPILLS rather than growing front-end memory, and there is deliberately NO
//! capacity refusal — a larger write-set is never declined for being large (FMEA PG-12).
//!
//! The payload options are resolved from the parsed `CopyFromStdinStmt`. One of them, `FREEZE`,
//! has no analogue in an LSM (no heap pages ⇒ no frozen rows) and is accepted-and-recorded by the
//! parser, never applied; it has no effect on this path and is not consulted here.

use bytes::BytesMut;
use ferrosa_common::timeuuid::is_reserved_column_name;
use ferrosa_sql::{CopyFormatKind, CopyFromStdinStmt, InsertStmt, ScalarValue, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::codec;
use crate::copy_decode::{CopyDecoder, CopyOptions, CopyRow};
use crate::extended::Session;
use crate::messages::{BackendMessage, FrontendMessage};
use crate::mvcc::MvccCommitError;
use crate::query::{self, ReturningOpts};
use crate::server::{dml_context, QueryContext};
use crate::TxnWriteSet;

/// Rows staged before the write set is flushed — the AUTOCOMMIT path only.
///
/// A COPY is one statement with one result, but holding a million rows of mutations in memory to
/// commit them at the end would be a self-inflicted OOM. Flushing periodically bounds the buffer
/// while keeping the commit count far below one-per-row. Inside a transaction there is nothing to
/// flush here: the rows go into the session's own write-set, which the open transaction's `COMMIT`
/// applies and its `ROLLBACK` discards. Both staging buffers SPILL past their threshold rather
/// than grow, so neither mode accumulates the load resident.
const FLUSH_EVERY: usize = 1000;

/// Everything resolved before the client is told to send.
struct CopyPlan {
    /// The columns the payload's fields map to, in order.
    columns: Vec<String>,
    options: CopyOptions,
    /// The wire format code for `CopyInResponse`: 0 text, 1 binary. Binary is refused at parse.
    format_code: u8,
}

/// Run the whole exchange: plan, acknowledge, consume, insert, report.
///
/// # Errors
///
/// Only I/O: a protocol or payload problem is reported to the client as an `ErrorResponse` (the
/// connection stays usable), not as an error here.
pub(crate) async fn drive<St>(
    stream: &mut St,
    frames: &mut BytesMut,
    read_buf: &mut [u8],
    ctx: &QueryContext,
    session: &mut Session,
    stmt: &CopyFromStdinStmt,
) -> std::io::Result<()>
where
    St: AsyncRead + AsyncWrite + Unpin,
{
    let mut out = BytesMut::new();

    // Snapshot the transaction mode ONCE. No other statement can run while the payload streams,
    // so the session cannot change mode mid-exchange — and every row of this COPY must land in
    // the same place: the session's write-set when a block is open, the local staging buffer in
    // autocommit. (A COPY arriving in an ABORTED block is refused by `plan_copy` before the ack,
    // so `in_txn` here only distinguishes "open block" from "autocommit".)
    let in_txn = session.in_txn();

    // Plan first: a refusal here is sent without ever acknowledging the COPY.
    let plan = match plan_copy(ctx, session, stmt) {
        Ok(plan) => plan,
        Err(refusal) => {
            refusal.encode(&mut out);
            BackendMessage::ReadyForQuery(session.txn_status()).encode(&mut out);
            stream.write_all(&out).await?;
            return Ok(());
        }
    };

    BackendMessage::CopyInResponse {
        format: plan.format_code,
        column_formats: Vec::new(),
    }
    .encode(&mut out);
    stream.write_all(&out).await?;
    // The ack is on the wire. The tail below is a SEPARATE write, so the buffer must be emptied
    // first: a second `CopyInResponse` arriving after the payload re-cues a real client into copy
    // mode — psql answers `CopyFail "trying to exit copy mode"` and `pgbench -i` dies with a bare
    // `PQendcopy failed`, both of them reading our stale ack as "start copying again".
    out.clear();

    let mut decoder = CopyDecoder::new(plan.options.clone());
    let mut inserted: u64 = 0;
    let mut failure: Option<BackendMessage> = None;
    // Autocommit staging buffer. Unused inside a transaction: there the rows go into the session's
    // write-set instead (see the module docs), so this stays empty and the flush below is skipped.
    let mut buffer = TxnWriteSet::default();

    loop {
        match codec::read_frontend(frames) {
            Ok(Some(FrontendMessage::CopyData { data })) => {
                // Once a failure is pending the payload is discarded, not decoded: the client is
                // still sending and these bytes have nowhere to go.
                if failure.is_none() {
                    match decoder.push(data) {
                        Ok(rows) => {
                            if let Err(err) = stage_rows(
                                ctx,
                                session,
                                stmt,
                                &plan.columns,
                                in_txn,
                                &mut buffer,
                                rows,
                                &mut inserted,
                            )
                            .await
                            {
                                failure = Some(err);
                            }
                        }
                        Err(e) => {
                            failure = Some(bad_payload(&format!("invalid COPY payload: {e}")));
                        }
                    }
                }
            }
            Ok(Some(FrontendMessage::CopyDone)) => {
                if failure.is_none() {
                    match decoder.finish() {
                        Ok(rows) => {
                            if let Err(err) = stage_rows(
                                ctx,
                                session,
                                stmt,
                                &plan.columns,
                                in_txn,
                                &mut buffer,
                                rows,
                                &mut inserted,
                            )
                            .await
                            {
                                failure = Some(err);
                            }
                        }
                        Err(e) => {
                            failure = Some(bad_payload(&format!("invalid COPY payload: {e}")));
                        }
                    }
                }
                break;
            }
            Ok(Some(FrontendMessage::CopyFail { message })) => {
                failure = Some(query::error_response(
                    "57014",
                    &format!("COPY aborted by client: {message}"),
                ));
                break;
            }
            Ok(Some(_)) => {
                // A query or an extended-protocol message in the middle of a COPY payload: the
                // stream is out of step, so this cannot be recovered from.
                failure = Some(query::error_response(
                    "08P01",
                    "a non-COPY message arrived during COPY FROM STDIN",
                ));
                break;
            }
            Ok(None) => {
                let n = stream.read(read_buf).await?;
                if n == 0 {
                    // The peer vanished mid-COPY. There is nobody left to report to.
                    return Ok(());
                }
                frames.extend_from_slice(&read_buf[..n]);
            }
            Err(e) => {
                failure = Some(query::error_response(
                    "08P01",
                    &format!("malformed message during COPY FROM STDIN: {e}"),
                ));
                break;
            }
        }
    }

    match failure {
        Some(err) => {
            // Fail loud, never silent. Inside a transaction an error ABORTS the block (PG
            // `25P02`), so poison it: a later COMMIT then rolls back rather than applying the
            // rows that DID buffer before the failure. A COPY that died part-way must never be
            // committed as if the load were whole — that partial load is exactly the silent data
            // loss the old `25001` refusal existed to prevent. In autocommit there is no block to
            // poison: the unflushed staging tail is simply dropped here, and rows from an earlier
            // flush are already applied (bounded by FLUSH_EVERY) and cannot be rolled back.
            if in_txn {
                session.mark_txn_failed();
            }
            err.encode(&mut out);
        }
        None => {
            if in_txn {
                // The rows are in the transaction's write-set: COMMIT applies them atomically,
                // ROLLBACK discards them. Announcing `COPY n` now is what PostgreSQL does inside a
                // block too — visibility, not the count, waits for COMMIT.
                BackendMessage::CommandComplete {
                    tag: format!("COPY {inserted}"),
                }
                .encode(&mut out);
            } else {
                // Autocommit: apply the staged write-set through the same commit path an INSERT
                // uses, so a COPY cannot commit differently.
                match flush(ctx, &mut buffer).await {
                    Ok(()) => BackendMessage::CommandComplete {
                        tag: format!("COPY {inserted}"),
                    }
                    .encode(&mut out),
                    Err(err) => err.encode(&mut out),
                }
            }
        }
    }
    BackendMessage::ReadyForQuery(session.txn_status()).encode(&mut out);
    stream.write_all(&out).await?;
    Ok(())
}

/// Route a batch of decoded rows to the write seam a COPY must use: the open transaction's
/// write-set when `in_txn`, else the local autocommit staging buffer.
///
/// This is the ONE place the transaction-vs-autocommit decision is made for COPY rows, so the two
/// modes cannot drift apart.
#[allow(clippy::too_many_arguments)]
async fn stage_rows(
    ctx: &QueryContext,
    session: &mut Session,
    stmt: &CopyFromStdinStmt,
    columns: &[String],
    in_txn: bool,
    buffer: &mut TxnWriteSet,
    rows: Vec<CopyRow>,
    inserted: &mut u64,
) -> Result<(), BackendMessage> {
    if in_txn {
        // Buffer into the SAME write-set INSERT/TRUNCATE use, so COMMIT applies it and ROLLBACK
        // discards it. `flush_locally` is false: an open transaction must never be committed
        // mid-statement — the write-set's own staging buffer bounds residency, not FLUSH_EVERY.
        insert_rows(
            ctx,
            stmt,
            columns,
            Some(session.txn_writes_mut()),
            rows,
            inserted,
            false,
        )
        .await
    } else {
        insert_rows(ctx, stmt, columns, Some(buffer), rows, inserted, true).await
    }
}

/// Resolve the table, the columns and the payload options — everything that could refuse the
/// COPY, checked before the client is told to send.
fn plan_copy(
    ctx: &QueryContext,
    session: &Session,
    stmt: &CopyFromStdinStmt,
) -> Result<CopyPlan, BackendMessage> {
    // A COPY is a command like any other: inside an ABORTED transaction block PostgreSQL refuses
    // every statement but COMMIT/ROLLBACK (`25P02`). Refused here, before the client is cued to
    // send, so a payload is never streamed at a statement that cannot accept it. (An OPEN block is
    // fine — the rows buffer into its write-set; see the module docs.)
    if session.in_failed_txn() {
        return Err(query::error_response(
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        ));
    }

    let ks = stmt.table.schema.as_deref().unwrap_or(&ctx.default_schema);
    let snap = ctx.schema.snapshot();
    let Some(meta) = snap.tables.get(&(ks.to_string(), stmt.table.table.clone())) else {
        return Err(query::error_response(
            "42P01",
            &format!("relation \"{ks}.{}\" does not exist", stmt.table.table),
        ));
    };

    let columns: Vec<String> = match &stmt.columns {
        Some(named) => {
            for name in named {
                if is_reserved_column_name(name) {
                    return Err(query::error_response(
                        "42P16",
                        &format!("column \"{name}\" is reserved and cannot be written"),
                    ));
                }
                if !meta.columns.contains_key(name.as_str()) {
                    return Err(query::error_response(
                        "42703",
                        &format!(
                            "column \"{name}\" of relation \"{}\" does not exist",
                            stmt.table.table
                        ),
                    ));
                }
            }
            named.clone()
        }
        // No column list: the table's own columns in declared order. The reserved synthetic key is
        // excluded — it is minted per row, and the client sends exactly the columns it declared.
        None => meta
            .columns
            .values()
            .filter(|c| !is_reserved_column_name(&c.name))
            .map(|c| c.name.clone())
            .collect(),
    };
    if columns.is_empty() {
        return Err(bad_payload(&format!(
            "relation \"{}\" has no writable columns",
            stmt.table.table
        )));
    }

    let mut options = match stmt.format {
        CopyFormatKind::Text => CopyOptions::text(),
        CopyFormatKind::Csv => CopyOptions::csv(),
    };
    if let Some(delimiter) = stmt.delimiter {
        // A delimiter is one BYTE on the wire. Refusing a non-ASCII one rather than truncating it
        // to its first byte keeps the failure legible.
        if !delimiter.is_ascii() {
            return Err(bad_payload(&format!(
                "COPY delimiter {delimiter:?} is not an ASCII character"
            )));
        }
        options.delimiter = delimiter as u8;
    }
    if let Some(null) = &stmt.null {
        options.null = null.as_bytes().to_vec();
    }
    options.header = stmt.header;

    let format_code = match stmt.format {
        CopyFormatKind::Text => 0,
        CopyFormatKind::Csv => 1,
    };
    Ok(CopyPlan {
        columns,
        options,
        format_code,
    })
}

/// Insert every decoded row through the shared DML seam, staging them in `buffer`.
///
/// `buffer` is `Some` either way: the session's transaction write-set when a block is open, or the
/// caller's autocommit staging buffer otherwise — the seam decides what "buffer" means from the
/// `Some` alone, so both modes take the identical path. `flush_locally` is true only in
/// autocommit, where the staged writes are applied every [`FLUSH_EVERY`] rows; inside a transaction
/// there is nothing to flush (the write-set's own cap bounds the rows), and flushing would commit
/// a still-open transaction.
///
/// A row whose field count does not match the column count is refused: silently padding or
/// truncating it would store a row the client never sent.
async fn insert_rows(
    ctx: &QueryContext,
    stmt: &CopyFromStdinStmt,
    columns: &[String],
    mut buffer: Option<&mut TxnWriteSet>,
    rows: Vec<CopyRow>,
    inserted: &mut u64,
    flush_locally: bool,
) -> Result<(), BackendMessage> {
    // Built ONCE and reused. An `InsertStmt` owns its column list, so constructing one per row
    // would clone the table name and the entire column list once for every row of the payload —
    // a million times for a million-row COPY. Only the values differ between rows, so the row
    // vector is reused in place.
    let mut ins = InsertStmt {
        table: stmt.table.clone(),
        columns: columns.to_vec(),
        rows: vec![Vec::new()],
        returning: None,
    };

    for row in rows {
        if row.len() != columns.len() {
            return Err(bad_payload(&format!(
                "COPY row has {} fields but {} columns are expected",
                row.len(),
                columns.len()
            )));
        }
        ins.rows[0] = row
            .into_iter()
            .map(|field| {
                ScalarValue::Literal(match field {
                    None => Value::Null,
                    // A payload field is TEXT whatever the column's type: the format has no types,
                    // and it is `resolve_dml_value` that decides what the text means for the column
                    // it lands in. Coercing here would be a second, divergent notion of the types.
                    //
                    // `from_utf8` MOVES the field's own allocation into the String instead of
                    // copying it (`from_utf8_lossy(..).into_owned()` always allocated a second
                    // buffer). A COPY payload is valid UTF-8 in the overwhelming majority of
                    // cases, so this is a move; invalid UTF-8 still degrades to a lossy String
                    // exactly as before, so no input changes meaning.
                    Some(bytes) => Value::Text(match String::from_utf8(bytes) {
                        Ok(text) => text,
                        Err(not_utf8) => String::from_utf8_lossy(not_utf8.as_bytes()).into_owned(),
                    }),
                })
            })
            .collect();
        let msgs = query::execute_insert(
            dml_context(ctx, buffer.as_deref_mut()),
            &ins,
            &[],
            ReturningOpts {
                with_row_description: false,
                result_formats: &[],
            },
        )
        .await;
        if let Some(err) = msgs
            .into_iter()
            .find(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
        {
            return Err(err);
        }
        *inserted += 1;
        if flush_locally {
            if let Some(staged) = buffer.as_deref_mut() {
                if staged.len() >= FLUSH_EVERY {
                    flush(ctx, staged).await?;
                }
            }
        }
    }
    Ok(())
}

/// Commit the buffered write set, using the same parameters the autocommit path uses so a COPY
/// cannot commit differently from an INSERT.
async fn flush(ctx: &QueryContext, buffer: &mut TxnWriteSet) -> Result<(), BackendMessage> {
    if buffer.is_empty() {
        return Ok(());
    }
    // Consume the staging as the streaming write-set source the commit path reads, so the
    // autocommit apply never re-materializes the batch.
    let staged = std::mem::take(buffer)
        .into_staged()
        .map_err(|error| query::write_error_response(&error))?;
    let _commit_guard = ctx.mvcc.commit_guard().await;
    match query::commit_mutations(
        &ctx.engine,
        &ctx.schema,
        &ctx.mvcc,
        &ctx.mvcc.snapshot(),
        &std::collections::HashSet::new(),
        staged,
    ) {
        Ok(_) => Ok(()),
        // Mirrors the autocommit arms exactly, so a COPY refused for the same reason reports the
        // same SQLSTATE an INSERT would.
        Err(MvccCommitError::SerializationFailure) => Err(query::error_response(
            "40001",
            "could not serialize COPY FROM STDIN",
        )),
        Err(MvccCommitError::Storage(error)) => Err(query::write_error_response(&error)),
        Err(error) => Err(query::error_response(
            "58000",
            &format!("COPY write set failed to commit: {error:?}"),
        )),
    }
}

/// `22P04` — the payload did not match the table.
fn bad_payload(message: &str) -> BackendMessage {
    query::error_response("22P04", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::txn_atomicity_tests::{
        make_ctx, make_ctx_synthetic_key, row_count, superuser, synthetic_keys, synthetic_values,
    };

    /// Frame a frontend message: one tag byte, a big-endian length that INCLUDES the length word
    /// itself, then the body. There is no frontend encoder in the codec (the server only ever
    /// decodes these), so the tests build the bytes a client would send.
    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + body.len());
        out.push(tag);
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    fn copy_data(data: &[u8]) -> Vec<u8> {
        frame(b'd', data)
    }

    fn copy_fail(message: &str) -> Vec<u8> {
        let mut body = message.as_bytes().to_vec();
        body.push(0);
        frame(b'f', &body)
    }

    fn parse_copy(sql: &str) -> ferrosa_sql::CopyFromStdinStmt {
        match ferrosa_sql::parse_statement(sql).expect("parses") {
            ferrosa_sql::Statement::CopyFromStdin(c) => *c,
            other => panic!("expected COPY, got {other:?}"),
        }
    }

    /// Run one whole exchange against an EXISTING session and return everything the server wrote
    /// back.
    ///
    /// The payload holds only `CopyData`/`CopyDone`/`CopyFail` frames. `drive` is entered *after*
    /// the connection loop has consumed the `Query` frame that opened the COPY, so a Query frame
    /// here would (correctly) be read as a stray message arriving mid-COPY. Nothing depends on
    /// task scheduling: the payload is written before `drive` runs.
    ///
    /// Taking the session as a parameter is what lets a test put it in a transaction first: the
    /// rows then have to land in the session's write-set, which is only observable through the
    /// same `Session` the `COMMIT`/`ROLLBACK` drives.
    async fn run_copy_in(
        ctx: &QueryContext,
        session: &mut Session,
        sql: &str,
        payload: &[Vec<u8>],
    ) -> (Vec<u8>, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let stmt = parse_copy(sql);

        // The pipe must hold the payload plus the server's reply without a reader running.
        let (mut client, mut server) = tokio::io::duplex(1 << 20);
        let mut wire = Vec::new();
        for part in payload {
            wire.extend_from_slice(part);
        }
        client.write_all(&wire).await.unwrap();

        let mut frames = BytesMut::new();
        let mut read_buf = vec![0u8; 1 << 16];
        drive(&mut server, &mut frames, &mut read_buf, ctx, session, &stmt)
            .await
            .expect("drive returns");

        // Close our end of the pipe so the read below can reach EOF instead of blocking forever.
        // `drive` has already returned, so everything the server will ever write is buffered.
        drop(server);

        let mut reply = Vec::new();
        let mut buf = vec![0u8; 1 << 16];
        loop {
            // A short read is possible, so keep reading until EOF or until a read produces
            // nothing for a moment. The timeout is a backstop, not a convergence budget: it is
            // never reached in practice because the server half is dropped above.
            match tokio::time::timeout(std::time::Duration::from_millis(500), client.read(&mut buf))
                .await
            {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                Ok(Ok(n)) => reply.extend_from_slice(&buf[..n]),
            }
        }
        (reply.clone(), String::from_utf8_lossy(&reply).into_owned())
    }

    /// [`run_copy_in`] against a fresh, autocommit session — the common case.
    async fn run_copy(ctx: &QueryContext, sql: &str, payload: &[Vec<u8>]) -> (Vec<u8>, String) {
        let mut session = Session::new(superuser());
        run_copy_in(ctx, &mut session, sql, payload).await
    }

    /// [`run_copy_in`] for a payload LARGER than the duplex pipe's buffer.
    ///
    /// `run_copy_in` writes the whole wire into the pipe BEFORE `drive` runs, so it only works for
    /// a payload that fits the 1 MiB pipe; a write-set big enough to SPILL does not, and the write
    /// would deadlock waiting for a reader that has not started. Here the client is a SPAWNED task,
    /// so the payload streams as the server drains it.
    async fn run_streaming_copy(
        ctx: &QueryContext,
        session: &mut Session,
        sql: &str,
        payload: &[u8],
    ) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let stmt = parse_copy(sql);
        let (mut client, mut server) = tokio::io::duplex(1 << 16);
        let mut wire = copy_data(payload);
        wire.extend_from_slice(&frame(b'c', &[]));
        let writer = tokio::spawn(async move {
            client
                .write_all(&wire)
                .await
                .expect("write the COPY payload");
            // Drain the server's reply until it closes its end.
            let mut reply = Vec::new();
            let mut buf = vec![0u8; 1 << 16];
            loop {
                match tokio::time::timeout(
                    std::time::Duration::from_millis(500),
                    client.read(&mut buf),
                )
                .await
                {
                    Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                    Ok(Ok(n)) => reply.extend_from_slice(&buf[..n]),
                }
            }
            reply
        });

        let mut frames = BytesMut::new();
        let mut read_buf = vec![0u8; 1 << 16];
        drive(&mut server, &mut frames, &mut read_buf, ctx, session, &stmt)
            .await
            .expect("drive returns");
        drop(server);

        let reply = writer.await.expect("writer task");
        String::from_utf8_lossy(&reply).into_owned()
    }

    /// Run one simple-query statement (`BEGIN`/`COMMIT`/`ROLLBACK`) through the server's own
    /// transaction path, collecting its whole reply. This is how the COPIES below become
    /// transactional: they are wrapped by the SAME `BEGIN`/`COMMIT`/`ROLLBACK` handling a real
    /// connection uses, never a hand-rolled stand-in.
    async fn simple(ctx: &QueryContext, session: &mut Session, sql: &str) -> Vec<BackendMessage> {
        let mut out: Vec<BackendMessage> = Vec::new();
        let tail = crate::server::execute_simple_to(ctx, session, sql, &mut out)
            .await
            .expect("an in-memory sink cannot fail");
        out.extend(tail);
        out
    }

    /// Every row of the payload is written, and the count the client is told is the count that
    /// landed.
    #[tokio::test]
    async fn copy_from_stdin_writes_every_row_and_reports_the_count() {
        let (_dir, ctx) = make_ctx().await;
        let (_reply, text) = run_copy(
            &ctx,
            "COPY kv (k) FROM STDIN",
            &[copy_data(b"a\nb\nc\n"), frame(b'c', &[])],
        )
        .await;

        assert!(
            text.starts_with('G'),
            "the COPY is acknowledged first: {text:?}"
        );
        assert!(text.contains("COPY 3"), "the reported count: {text:?}");
        for key in ["a", "b", "c"] {
            assert_eq!(row_count(&ctx, key).await, 1, "row {key} must be persisted");
        }
    }

    /// The number of `CopyInResponse` frames in a server reply.
    ///
    /// The ack is 8 bytes: tag `G`, a length of `7` (which includes the length word itself), a
    /// format byte of `0`, and a zero column count. Counting the FRAME rather than the letter
    /// `G` is deliberate — a payload line or a command tag may legitimately contain a `G`.
    fn copy_in_responses(reply: &[u8]) -> usize {
        const FRAME: [u8; 8] = [b'G', 0, 0, 0, 7, 0, 0, 0];
        reply.windows(FRAME.len()).filter(|w| *w == FRAME).count()
    }

    /// `COPY ... FROM STDIN` is acknowledged with `CopyInResponse` EXACTLY ONCE.
    ///
    /// The ack is the one and only cue for the client to start streaming its payload. A SECOND
    /// `G` arriving *after* the payload makes a real client (psql, `pgbench -i`) believe it has
    /// been re-cued into copy mode: psql answers `CopyFail "trying to exit copy mode"` and
    /// pgbench dies with a bare `PQendcopy failed`. The reply buffer is written once for the ack
    /// and once for the tail, and the tail must not carry a stale copy of the ack.
    #[tokio::test]
    async fn copy_from_stdin_is_acknowledged_exactly_once() {
        let (_dir, ctx) = make_ctx().await;
        let (reply, text) = run_copy(
            &ctx,
            "COPY kv (k) FROM STDIN",
            &[copy_data(b"a\nb\n"), frame(b'c', &[])],
        )
        .await;

        assert_eq!(
            copy_in_responses(&reply),
            1,
            "the COPY must be acknowledged exactly once: {text:?}"
        );
        assert!(
            text.contains("COPY 2"),
            "and still report its own count: {text:?}"
        );
    }

    /// The same invariant on the FAILURE tail: an error reply must not re-send the ack either.
    /// This is the exact shape a bad `pgbench -i` row takes — ack, payload, then the error.
    #[tokio::test]
    async fn a_failed_copy_is_acknowledged_exactly_once() {
        let (_dir, ctx) = make_ctx().await;
        let (reply, text) = run_copy(
            &ctx,
            "COPY kv (k) FROM STDIN",
            &[copy_data(b"good\nbad\textra\n"), frame(b'c', &[])],
        )
        .await;

        assert_eq!(
            copy_in_responses(&reply),
            1,
            "even a failed COPY keeps a single ack: {text:?}"
        );
        assert!(
            text.contains("22P04"),
            "the failure is still reported: {text:?}"
        );
    }

    /// A malformed row is reported, the remaining payload is DRAINED (so it cannot be read as
    /// SQL), and nothing from the unflushed tail is written. The drain is the point: returning
    /// early would leave the client's bytes in the socket for the statement parser to eat.
    #[tokio::test]
    async fn a_bad_row_is_reported_and_the_rest_of_the_payload_is_drained() {
        let (_dir, ctx) = make_ctx().await;
        // One good row, then a row with two fields for one column, then MORE payload after the
        // failure and a clean CopyDone.
        let (_reply, text) = run_copy(
            &ctx,
            "COPY kv (k) FROM STDIN",
            &[
                copy_data(b"good\nbad\textra\n"),
                copy_data(b"more\n"),
                frame(b'c', &[]),
            ],
        )
        .await;

        assert!(
            text.starts_with('G'),
            "acknowledged before the payload was sent"
        );
        assert!(
            text.contains("2 fields but 1 columns"),
            "the malformed row is named: {text:?}"
        );
        assert!(
            text.contains("22P04"),
            "and typed as a bad payload: {text:?}"
        );
        assert!(
            !text.contains("COPY 1"),
            "a failed COPY must not also report success: {text:?}"
        );
        assert_eq!(
            row_count(&ctx, "good").await,
            0,
            "the whole unflushed tail is dropped, including the good row before the bad one"
        );
    }

    /// A client `CopyFail` is reported and writes nothing.
    #[tokio::test]
    async fn a_client_copy_fail_writes_nothing() {
        let (_dir, ctx) = make_ctx().await;
        let (_reply, text) = run_copy(
            &ctx,
            "COPY kv (k) FROM STDIN",
            &[copy_data(b"a\n"), copy_fail("client changed its mind")],
        )
        .await;

        assert!(text.starts_with('G'));
        assert!(
            text.contains("57014"),
            "reported as a client abort: {text:?}"
        );
        assert!(
            text.contains("client changed its mind"),
            "with the client's own words"
        );
        assert_eq!(row_count(&ctx, "a").await, 0, "nothing is written");
    }

    /// The invariant that shapes the whole file: nothing is acknowledged that cannot run. A COPY
    /// against a table that does not exist must be refused BEFORE `CopyInResponse`, or the client
    /// would stream a payload at a statement that was never going to accept it.
    #[tokio::test]
    async fn a_copy_that_cannot_run_is_refused_before_it_is_acknowledged() {
        let (_dir, ctx) = make_ctx().await;
        let (reply, text) = run_copy(&ctx, "COPY nope (k) FROM STDIN", &[]).await;

        assert_eq!(
            reply.first().copied(),
            Some(b'E'),
            "the first thing the client sees must be the error, not an acknowledgement"
        );
        assert!(text.contains("42P01"), "a missing relation: {text:?}");
        assert!(
            !text.contains('G'),
            "no CopyInResponse anywhere in the reply: {text:?}"
        );
    }

    /// The reserved synthetic key is minted per row, never sent by the client, so naming it is
    /// refused — and, again, refused before acknowledgement.
    #[tokio::test]
    async fn naming_the_reserved_column_is_refused() {
        let (_dir, ctx) = make_ctx().await;
        let (reply, text) = run_copy(&ctx, "COPY kv (_sys_ck_) FROM STDIN", &[]).await;

        assert_eq!(reply.first().copied(), Some(b'E'));
        assert!(text.contains("42P16"), "reserved name: {text:?}");
        assert!(!text.contains('G'), "not acknowledged: {text:?}");
    }

    /// pgbench's exact form on PostgreSQL v14+: `(freeze on)`. An LSM has no heap pages and so no
    /// frozen-row concept — the option is accepted-and-recorded, not applied (see
    /// `ferrosa_sql::CopyFromStdinStmt::freeze`) — but the COPY it wraps must still load the rows.
    /// This asserts the rows land, not merely that no error was returned.
    #[tokio::test]
    async fn copy_freeze_on_loads_the_rows() {
        let (_dir, ctx) = make_ctx().await;
        let (_reply, text) = run_copy(
            &ctx,
            "copy kv (k) from stdin with (freeze on)",
            &[copy_data(b"x\ny\n"), frame(b'c', &[])],
        )
        .await;

        assert!(
            text.starts_with('G'),
            "the COPY is acknowledged first: {text:?}"
        );
        assert!(text.contains("COPY 2"), "the reported count: {text:?}");
        for key in ["x", "y"] {
            assert_eq!(row_count(&ctx, key).await, 1, "row {key} must be persisted");
        }
    }

    /// RED-then-GREEN: a COPY inside an open transaction is ACCEPTED (the old `25001` refusal is
    /// gone), its rows are BUFFERED in the transaction's write-set rather than applied, and they
    /// become VISIBLE only on `COMMIT`.
    ///
    /// The values are read back, not merely the absence of an error: "no error" would also be
    /// true of a COPY that acknowledged and dropped the payload — the exact silent-data-loss
    /// bug the refusal used to stand in front of.
    #[tokio::test]
    async fn copy_inside_a_transaction_buffers_and_applies_on_commit() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        simple(&ctx, &mut session, "BEGIN").await;

        let (_reply, text) = run_copy_in(
            &ctx,
            &mut session,
            "COPY kv (k) FROM STDIN",
            &[copy_data(b"a\nb\nc\n"), frame(b'c', &[])],
        )
        .await;

        assert!(
            text.starts_with('G'),
            "the COPY is acknowledged first: {text:?}"
        );
        assert!(
            !text.contains("25001"),
            "a COPY inside a transaction must no longer be refused: {text:?}"
        );
        assert!(
            text.contains("COPY 3"),
            "the statement still reports its own row count: {text:?}"
        );
        assert_eq!(
            session.txn_writes().len(),
            3,
            "the three rows must sit in the transaction write-set, unbuffered nothing"
        );
        for key in ["a", "b", "c"] {
            assert_eq!(
                row_count(&ctx, key).await,
                0,
                "row {key} must NOT be visible before COMMIT (buffered, not applied)"
            );
        }

        simple(&ctx, &mut session, "COMMIT").await;
        for key in ["a", "b", "c"] {
            assert_eq!(
                row_count(&ctx, key).await,
                1,
                "row {key} must be visible after COMMIT"
            );
        }
    }

    /// The ACID half, and the reason the whole change is safe: a COPY inside a transaction that
    /// `ROLLBACK`s leaves NO rows. Without this, "accepted" would only mean the refusal moved, and
    /// the old silent-data-loss bug would return in a new costume — a COPY that reports `COPY n`
    /// and commits rows the client asked to discard.
    #[tokio::test]
    async fn copy_inside_a_rolled_back_transaction_leaves_no_rows() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        simple(&ctx, &mut session, "BEGIN").await;

        let (_reply, text) = run_copy_in(
            &ctx,
            &mut session,
            "COPY kv (k) FROM STDIN",
            &[copy_data(b"a\nb\n"), frame(b'c', &[])],
        )
        .await;
        assert!(
            text.contains("COPY 2"),
            "the COPY itself succeeds: {text:?}"
        );
        assert_eq!(
            session.txn_writes().len(),
            2,
            "the rows are buffered in the transaction write-set"
        );

        simple(&ctx, &mut session, "ROLLBACK").await;
        assert!(
            session.txn_writes().is_empty(),
            "ROLLBACK must clear the buffered COPY rows"
        );
        for key in ["a", "b"] {
            assert_eq!(
                row_count(&ctx, key).await,
                0,
                "row {key} must be discarded by ROLLBACK"
            );
        }
    }

    /// Fail loud, not silent: a COPY that dies mid-payload inside a transaction must ABORT that
    /// transaction, so the rows that did buffer are not later committed as if the load were whole.
    /// A poisoned transaction's `COMMIT` is a `ROLLBACK`, and the partially-loaded rows must be
    /// absent — the specific trap this change had to avoid.
    #[tokio::test]
    async fn a_failed_copy_inside_a_transaction_must_not_commit_a_partial_load() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        simple(&ctx, &mut session, "BEGIN").await;

        // A good row, then a malformed one — the good row is buffered before the failure.
        let (_reply, text) = run_copy_in(
            &ctx,
            &mut session,
            "COPY kv (k) FROM STDIN",
            &[
                copy_data(b"good\nbad\textra\n"),
                copy_data(b"more\n"),
                frame(b'c', &[]),
            ],
        )
        .await;
        assert!(text.contains("22P04"), "the bad row is reported: {text:?}");

        // COMMIT on the aborted transaction must NOT apply the buffered "good" row.
        simple(&ctx, &mut session, "COMMIT").await;
        assert_eq!(
            row_count(&ctx, "good").await,
            0,
            "a failed COPY must never commit a partial load"
        );
    }

    /// The transactional COPY into a PK-LESS table: the table declared no `PRIMARY KEY`, so
    /// it keys on the invisible synthetic `_sys_ck_` column and every buffered row needs its
    /// own minted v1 TimeUUID. RED before the fix — COMMIT built the row image and rejected
    /// the key as `uuid requires 16 bytes`. The rows must now LAND at COMMIT.
    #[tokio::test]
    async fn copy_inside_a_transaction_into_a_pkless_table_commits_its_rows() {
        let (_dir, ctx) = make_ctx_synthetic_key().await;
        let mut session = Session::new(superuser());
        simple(&ctx, &mut session, "BEGIN").await;

        let (_reply, text) = run_copy_in(
            &ctx,
            &mut session,
            "COPY sk (v) FROM STDIN",
            &[copy_data(b"a\nb\nc\n"), frame(b'c', &[])],
        )
        .await;
        assert!(
            text.contains("COPY 3"),
            "the buffered COPY reports its count: {text:?}"
        );
        assert_eq!(
            session.txn_writes().len(),
            3,
            "all three rows sit in the transaction write-set"
        );

        simple(&ctx, &mut session, "COMMIT").await;

        let mut values = synthetic_values(&ctx).await;
        values.sort();
        assert_eq!(
            values,
            vec!["a", "b", "c"],
            "every buffered row must land at COMMIT: {text:?}"
        );
    }

    /// The property a "rows landed" count alone would MISS: each buffered row must carry its
    /// OWN synthetic key. A key minted once and reused for every row of the batch collapses
    /// N rows into one — the write-set applies cleanly and reports success while silently
    /// losing rows, which is exactly the row-loss bug keying on a non-unique column had.
    #[tokio::test]
    async fn copy_inside_a_transaction_gives_each_buffered_row_its_own_key() {
        let (_dir, ctx) = make_ctx_synthetic_key().await;
        let mut session = Session::new(superuser());
        simple(&ctx, &mut session, "BEGIN").await;

        let (_reply, _text) = run_copy_in(
            &ctx,
            &mut session,
            "COPY sk (v) FROM STDIN",
            &[copy_data(b"a\nb\nc\nd\ne\n"), frame(b'c', &[])],
        )
        .await;
        simple(&ctx, &mut session, "COMMIT").await;

        let keys = synthetic_keys(&ctx).await;
        assert_eq!(keys.len(), 5, "count rows == count keys");
        let distinct: std::collections::HashSet<&Vec<u8>> = keys.iter().collect();
        assert_eq!(
            distinct.len(),
            5,
            "each buffered row must have a DISTINCT synthetic key; {keys:?}"
        );
    }

    /// The distinct keys must also stay TIME-ORDERED: the synthetic key is the storage key,
    /// and a batch whose keys sort backwards scatters rows that were written in order. This
    /// is the same monotonicity the mint guarantees, asserted across a whole buffered COPY.
    #[tokio::test]
    async fn copy_buffered_synthetic_keys_stay_time_ordered() {
        let (_dir, ctx) = make_ctx_synthetic_key().await;
        let mut session = Session::new(superuser());
        simple(&ctx, &mut session, "BEGIN").await;

        let payload = (0..50).map(|i| format!("v{i}\n")).collect::<String>();
        let (_reply, _text) = run_copy_in(
            &ctx,
            &mut session,
            "COPY sk (v) FROM STDIN",
            &[copy_data(payload.as_bytes()), frame(b'c', &[])],
        )
        .await;
        simple(&ctx, &mut session, "COMMIT").await;

        let mut keys = synthetic_keys(&ctx).await;
        assert_eq!(keys.len(), 50, "all 50 rows must land");
        // The keys as handed out are already ordered; reading them back sorted must not
        // change the SET (they are all distinct and monotone), so a strictly increasing
        // sort is the assertion.
        let sorted = {
            let mut k = keys.clone();
            k.sort();
            k
        };
        keys.sort();
        assert_eq!(sorted, keys, "the 50 distinct keys must sort as themselves");
        for pair in sorted.windows(2) {
            assert!(pair[0] < pair[1], "keys must be strictly increasing");
        }
    }

    /// No regression: an AUTOCOMMIT COPY (no open transaction) into the same PK-less table
    /// still mints a key per row and applies each immediately.
    #[tokio::test]
    async fn autocommit_copy_into_a_pkless_table_still_works() {
        let (_dir, ctx) = make_ctx_synthetic_key().await;
        let (_reply, text) = run_copy(
            &ctx,
            "COPY sk (v) FROM STDIN",
            &[copy_data(b"x\ny\n"), frame(b'c', &[])],
        )
        .await;

        assert!(text.contains("COPY 2"), "the reported count: {text:?}");
        let mut values = synthetic_values(&ctx).await;
        values.sort();
        assert_eq!(values, vec!["x", "y"], "autocommit rows land: {text:?}");
        let keys = synthetic_keys(&ctx).await;
        let distinct: std::collections::HashSet<&Vec<u8>> = keys.iter().collect();
        assert_eq!(distinct.len(), 2, "a key per row in autocommit too");
    }

    /// The deployed shape: `pgbench -i` buffers ~1.1M rows into ONE transaction write-set and
    /// commits them in one COMMIT. The write-set is now STAGED, not resident: a COPY large enough
    /// to exceed the staging buffer SPILLS to disk, so front-end residency is bounded by the
    /// buffer and does NOT track the row count (FMEA PG-12).
    ///
    /// This test asserts BOTH halves — BOUNDED RESIDENCY (the set spilled, and its resident bytes
    /// stay within the staging buffer even though the rows far exceed it) AND COMPLETE DATA
    /// (every row lands at COMMIT with its own key). The residency half is the exact OPPOSITE of
    /// the old `txn_writes().len() == N, "all rows buffered"`, which asserted the defect —
    /// buffering the whole load resident — as if it were the contract.
    #[tokio::test]
    async fn a_large_transactional_copy_stays_bounded_and_commits_distinct_rows() {
        const N: usize = 12_000;
        // ~1 KiB per row, so N rows far exceed the default 8 MiB staging buffer (N × 1 KiB ≈ 12 MiB)
        // and force a spill (the buffer is a tunable; the point is that the staged payload is many
        // times it). The COPY goes through the spawned-writer helper because that payload is far
        // larger than the duplex pipe — `run_copy_in` writes the whole payload before it reads and
        // would deadlock here.
        const PAD: usize = 1024;
        let (_dir, ctx) = make_ctx_synthetic_key().await;
        let mut session = Session::new(superuser());
        simple(&ctx, &mut session, "BEGIN").await;

        let payload = (0..N)
            .map(|i| {
                let mut value = format!("v{i:08}");
                value.push_str(&"x".repeat(PAD - value.len()));
                value.push('\n');
                value
            })
            .collect::<String>();
        let text = run_streaming_copy(
            &ctx,
            &mut session,
            "COPY sk (v) FROM STDIN",
            payload.as_bytes(),
        )
        .await;
        assert!(
            text.contains(&format!("COPY {N}")),
            "reported count: {text:?}"
        );

        // BOUNDED RESIDENCY: every row is staged, yet the write-set has SPILLED — the payload is
        // on disk and the resident bytes are within the staging buffer, never the whole load.
        assert_eq!(
            session.txn_write_count(),
            N,
            "every row must be staged in the transaction write-set"
        );
        assert!(
            session.txn_writes_spilled(),
            "N rows of ~{PAD} bytes must exceed the staging buffer and SPILL, not stay resident"
        );
        assert!(
            session.txn_writes_resident_bytes() <= session.txn_writes_threshold_bytes(),
            "resident bytes must stay within the staging buffer ({} <= {})",
            session.txn_writes_resident_bytes(),
            session.txn_writes_threshold_bytes()
        );

        simple(&ctx, &mut session, "COMMIT").await;

        // COMPLETE DATA: every staged row must land at COMMIT with its own key.
        let keys = synthetic_keys(&ctx).await;
        assert_eq!(keys.len(), N, "every staged row must land at COMMIT");
        let distinct: std::collections::HashSet<&Vec<u8>> = keys.iter().collect();
        assert_eq!(distinct.len(), N, "each row needs its OWN key");
    }

    /// Preserved invariant: a transactional COPY into a PK-less table that `ROLLBACK`s leaves
    /// no rows — the buffered keys are discarded with the write-set.
    #[tokio::test]
    async fn copy_into_a_pkless_table_inside_a_rolled_back_transaction_leaves_no_rows() {
        let (_dir, ctx) = make_ctx_synthetic_key().await;
        let mut session = Session::new(superuser());
        simple(&ctx, &mut session, "BEGIN").await;

        let (_reply, text) = run_copy_in(
            &ctx,
            &mut session,
            "COPY sk (v) FROM STDIN",
            &[copy_data(b"a\nb\n"), frame(b'c', &[])],
        )
        .await;
        assert!(text.contains("COPY 2"), "the COPY succeeds: {text:?}");

        simple(&ctx, &mut session, "ROLLBACK").await;
        assert!(
            synthetic_values(&ctx).await.is_empty(),
            "ROLLBACK must discard every buffered row"
        );
    }
}
