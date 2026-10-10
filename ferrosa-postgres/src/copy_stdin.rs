//! Driving a `COPY ... FROM STDIN` exchange.
//!
//! COPY is the one statement that cannot be answered in a single step. The client sends the
//! payload only *after* the server acknowledges with `CopyInResponse`, so the exchange is a
//! conversation: acknowledge, then consume `CopyData` frames until `CopyDone`. That is why this
//! lives beside the connection loop, which owns the frame buffer and the stream, rather than in
//! the executor.
//!
//! Two rules shape the code below.
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
//! Rows are inserted through the ordinary DML path (`query::execute_insert`) rather than a
//! parallel one, so COPY gets the same type coercion, the same synthetic `_sys_ck_` key minting,
//! and — because the write set is flushed with the very parameters the autocommit path uses —
//! the same commit. There is no second way to write a row here.
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
use crate::PgWrite;

/// Rows buffered before the write set is flushed.
///
/// A COPY is one statement with one result, but holding a million rows of mutations in memory to
/// commit them at the end would be a self-inflicted OOM. Flushing periodically bounds the buffer
/// while keeping the commit count far below one-per-row. (The session's own write-set limit does
/// not apply: this is not a client transaction, and COPY is atomic per flush, not per statement.)
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

    let mut decoder = CopyDecoder::new(plan.options.clone());
    let mut inserted: u64 = 0;
    let mut failure: Option<BackendMessage> = None;
    let mut buffer: Vec<PgWrite> = Vec::new();

    loop {
        match codec::read_frontend(frames) {
            Ok(Some(FrontendMessage::CopyData { data })) => {
                // Once a failure is pending the payload is discarded, not decoded: the client is
                // still sending and these bytes have nowhere to go.
                if failure.is_none() {
                    match decoder.push(data) {
                        Ok(rows) => {
                            if let Err(err) = insert_rows(
                                ctx,
                                stmt,
                                &plan.columns,
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
                            if let Err(err) = insert_rows(
                                ctx,
                                stmt,
                                &plan.columns,
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
        // The buffered rows are deliberately NOT committed on failure: the write set is dropped,
        // so a COPY that failed part-way leaves nothing behind from the unflushed tail. Rows from
        // an earlier flush are already committed and cannot be rolled back — which is why an
        // unflushed tail is the common case (FLUSH_EVERY rows) and a failure is reported, never
        // swallowed.
        Some(err) => err.encode(&mut out),
        None => match flush(ctx, &mut buffer).await {
            Ok(()) => BackendMessage::CommandComplete {
                tag: format!("COPY {inserted}"),
            }
            .encode(&mut out),
            Err(err) => err.encode(&mut out),
        },
    }
    BackendMessage::ReadyForQuery(session.txn_status()).encode(&mut out);
    stream.write_all(&out).await?;
    Ok(())
}

/// Resolve the table, the columns and the payload options — everything that could refuse the
/// COPY, checked before the client is told to send.
fn plan_copy(
    ctx: &QueryContext,
    session: &Session,
    stmt: &CopyFromStdinStmt,
) -> Result<CopyPlan, BackendMessage> {
    if session.in_txn() {
        // Nothing is inserted on a COPY inside an explicit transaction, so allowing it would
        // silently drop the payload.
        return Err(query::error_response(
            "25001",
            "COPY FROM STDIN cannot run inside a transaction block",
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

/// Insert every decoded row, buffering the writes and flushing every [`FLUSH_EVERY`].
///
/// A row whose field count does not match the column count is refused: silently padding or
/// truncating it would store a row the client never sent.
async fn insert_rows(
    ctx: &QueryContext,
    stmt: &CopyFromStdinStmt,
    columns: &[String],
    buffer: &mut Vec<PgWrite>,
    rows: Vec<CopyRow>,
    inserted: &mut u64,
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
            dml_context(ctx, Some(buffer)),
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
        if buffer.len() >= FLUSH_EVERY {
            flush(ctx, buffer).await?;
        }
    }
    Ok(())
}

/// Commit the buffered write set, using the same parameters the autocommit path uses so a COPY
/// cannot commit differently from an INSERT.
async fn flush(ctx: &QueryContext, buffer: &mut Vec<PgWrite>) -> Result<(), BackendMessage> {
    if buffer.is_empty() {
        return Ok(());
    }
    let mutations = buffer.drain(..).map(|w| w.0).collect::<Vec<_>>();
    let _commit_guard = ctx.mvcc.commit_guard().await;
    match query::commit_mutations(
        &ctx.engine,
        &ctx.schema,
        &ctx.mvcc,
        &ctx.mvcc.snapshot(),
        &std::collections::HashSet::new(),
        mutations,
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
    use crate::server::txn_atomicity_tests::{make_ctx, row_count, superuser};

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

    /// Run one whole exchange and return everything the server wrote back.
    ///
    /// The payload holds only `CopyData`/`CopyDone`/`CopyFail` frames. `drive` is entered *after*
    /// the connection loop has consumed the `Query` frame that opened the COPY, so a Query frame
    /// here would (correctly) be read as a stray message arriving mid-COPY. Nothing depends on
    /// task scheduling: the payload is written before `drive` runs.
    async fn run_copy(ctx: &QueryContext, sql: &str, payload: &[Vec<u8>]) -> (Vec<u8>, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let stmt = parse_copy(sql);

        // The pipe must hold the payload plus the server's reply without a reader running.
        let (mut client, mut server) = tokio::io::duplex(1 << 20);
        let mut wire = Vec::new();
        for part in payload {
            wire.extend_from_slice(part);
        }
        client.write_all(&wire).await.unwrap();

        let mut session = crate::extended::Session::new(superuser());
        let mut frames = BytesMut::new();
        let mut read_buf = vec![0u8; 1 << 16];
        drive(
            &mut server,
            &mut frames,
            &mut read_buf,
            ctx,
            &mut session,
            &stmt,
        )
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

    /// Every row of the payload is written, and the count the client is told is the count that
    /// landed. This is the assertion the multi-row INSERT could not make.
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
}
