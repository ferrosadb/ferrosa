//! The PL/pgSQL host interface, served from the calling session (D13).
//!
//! A runtime (the interpreter, or a compiled component) is synchronous; the session is async.
//! The runtime runs on a blocking thread with a [`ChannelHost`] whose every call is a request
//! to this task, which serves it against the session — the same transaction, the same
//! snapshot, the same write-set, the same authorization — and answers. So whatever a `DO`
//! block or a trigger function writes commits or rolls back with the statement that ran it.
//!
//! Results stream: `query` opens the executor's row stream and `fetch` pulls at most `max`
//! rows from it; nothing gathers a whole result. Open cursors are bounded
//! ([`MAX_OPEN_CURSORS`]) and all of a run's cursors are dropped when it ends.

use std::collections::HashMap;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Arc;

use ferrosa_plpgsql::{CursorId, Host, SqlError, Value};
use ferrosa_sql::{ColumnType, Row, Statement, Value as SqlValue};

use crate::extended::Session;
use crate::messages::BackendMessage;
use crate::result_stream::ResultStream;
use crate::server::QueryContext;

/// Cursors one run may hold open at once (`54000` beyond).
pub(crate) const MAX_OPEN_CURSORS: usize = 16;

type Reply<T> = SyncSender<Result<T, SqlError>>;

/// One host call, sent from the runtime's thread to the session's task.
pub(crate) enum Request {
    Query {
        sql: String,
        params: Vec<Value>,
        reply: Reply<CursorId>,
    },
    Fetch {
        cursor: CursorId,
        max: u32,
        reply: Reply<Vec<Vec<Value>>>,
    },
    Close {
        cursor: CursorId,
    },
    Execute {
        sql: String,
        params: Vec<Value>,
        reply: Reply<u64>,
    },
    Notify {
        channel: String,
        payload: String,
        reply: Reply<()>,
    },
}

/// The runtime's side: each call becomes a [`Request`] and blocks for its answer.
pub(crate) struct ChannelHost {
    tx: tokio::sync::mpsc::Sender<Request>,
}

fn gone() -> SqlError {
    SqlError::new(
        "57014",
        "the statement running this PL/pgSQL code was cancelled",
    )
}

impl ChannelHost {
    fn call<T>(&self, make: impl FnOnce(Reply<T>) -> Request) -> Result<T, SqlError> {
        let (reply, answer) = sync_channel(1);
        self.tx.blocking_send(make(reply)).map_err(|_| gone())?;
        answer.recv().map_err(|_| gone())?
    }
}

impl Host for ChannelHost {
    fn query(&mut self, sql: &str, params: &[Value]) -> Result<CursorId, SqlError> {
        self.call(|reply| Request::Query {
            sql: sql.to_string(), // crosses to the session's task, which needs its own copy
            params: params.to_vec(), // Arc-backed values: shares the text
            reply,
        })
    }

    fn fetch(&mut self, cursor: CursorId, max: u32) -> Result<Vec<Vec<Value>>, SqlError> {
        self.call(|reply| Request::Fetch { cursor, max, reply })
    }

    fn close(&mut self, cursor: CursorId) {
        if self.tx.blocking_send(Request::Close { cursor }).is_err() {
            tracing::debug!("PL/pgSQL cursor close after the session task ended");
        }
    }

    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, SqlError> {
        self.call(|reply| Request::Execute {
            sql: sql.to_string(), // crosses to the session's task, which needs its own copy
            params: params.to_vec(), // Arc-backed values: shares the text
            reply,
        })
    }

    fn notify(&mut self, channel: &str, payload: &str) -> Result<(), SqlError> {
        self.call(|reply| Request::Notify {
            channel: channel.to_string(), // crosses threads
            payload: payload.to_string(), // crosses threads
            reply,
        })
    }
}

/// Run `work` on a blocking thread against a [`ChannelHost`] served from `session`.
pub(crate) async fn run<T, F>(
    ctx: &QueryContext,
    session: &mut Session,
    depth: usize,
    work: F,
) -> Result<T, SqlError>
where
    T: Send + 'static,
    F: FnOnce(ChannelHost) -> Result<T, SqlError> + Send + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let worker = tokio::task::spawn_blocking(move || work(ChannelHost { tx }));
    let mut served = Served {
        ctx,
        cursors: HashMap::new(),
        next: 0,
        depth,
    };
    while let Some(request) = rx.recv().await {
        served.serve(session, request).await;
    }
    drop(served);
    worker.await.map_err(|e| {
        tracing::error!(%e, "PL/pgSQL runtime thread failed");
        SqlError::new("XX000", format!("PL/pgSQL runtime thread failed: {e}"))
    })?
}

enum Cursor {
    Stream(ResultStream),
    Rows {
        rows: std::vec::IntoIter<Row>,
        types: Vec<ColumnType>,
    },
}

struct Served<'c> {
    ctx: &'c QueryContext,
    cursors: HashMap<CursorId, Cursor>,
    next: CursorId,
    /// Nesting depth of PL/pgSQL runs (a trigger fired by a trigger's own write).
    depth: usize,
}

/// A front-end error response as a host error.
pub(crate) fn sql_error(message: &BackendMessage) -> SqlError {
    let BackendMessage::ErrorResponse { fields } = message else {
        return SqlError::new("XX000", "expected an error response");
    };
    let field = |code: u8| {
        fields
            .iter()
            .find(|(c, _)| *c == code)
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    };
    SqlError::new(field(b'C'), field(b'M'))
}

/// The first error among a statement's messages, if any.
pub(crate) fn first_error(messages: &[BackendMessage]) -> Option<SqlError> {
    messages
        .iter()
        .find(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
        .map(sql_error)
}

/// The affected-row count in a command tag (`INSERT 0 3` → 3).
fn tag_count(messages: &[BackendMessage]) -> u64 {
    messages
        .iter()
        .rev()
        .find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => tag.rsplit(' ').next()?.parse().ok(),
            _ => None,
        })
        .unwrap_or(0)
}

/// Decode host values as bound parameters (text form, by type OID).
fn decode_params(ctx: &QueryContext, params: Vec<Value>) -> Result<Vec<SqlValue>, SqlError> {
    params
        .into_iter()
        .map(|v| {
            let oid = i32::try_from(v.type_oid).unwrap_or(0);
            crate::query::decode_param_checked(
                0,
                oid,
                v.text.as_deref().map(str::as_bytes),
                &ctx.jsonb_limits,
            )
            .map_err(|e| SqlError::new(e.sqlstate, e.message))
        })
        .collect()
}

/// A result row as host values.
pub(crate) fn row_values(row: Row, types: &[ColumnType]) -> Result<Vec<Value>, SqlError> {
    row.0
        .into_iter()
        .zip(types)
        .map(|(value, ty)| {
            let text = crate::query::value_text(&value)
                .map_err(|e| sql_error(&crate::query::encode_error_response(&e)))?;
            let oid = u32::try_from(crate::query::column_type_oid(*ty)).unwrap_or(0);
            Ok(Value {
                type_oid: oid,
                text: text.map(Arc::from),
            })
        })
        .collect()
}

impl Served<'_> {
    async fn serve(&mut self, session: &mut Session, request: Request) {
        match request {
            Request::Query { sql, params, reply } => {
                let result = self.query(session, &sql, params).await;
                answer(reply, result);
            }
            Request::Fetch { cursor, max, reply } => {
                let result = self.fetch(cursor, max).await;
                answer(reply, result);
            }
            Request::Close { cursor } => {
                self.cursors.remove(&cursor);
            }
            Request::Execute { sql, params, reply } => {
                let result = Box::pin(self.execute(session, sql, params)).await;
                answer(reply, result);
            }
            Request::Notify {
                channel,
                payload,
                reply,
            } => {
                let result = super::notify(self.ctx, session, &channel, &payload)
                    .await
                    .map_err(|e| sql_error(&e));
                answer(reply, result);
            }
        }
    }

    async fn query(
        &mut self,
        session: &mut Session,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<CursorId, SqlError> {
        if self.cursors.len() >= MAX_OPEN_CURSORS {
            return Err(SqlError::new(
                "54000",
                format!("PL/pgSQL holds more than {MAX_OPEN_CURSORS} open queries"),
            ));
        }
        let statement = crate::catalog::parse_pg(sql)
            .map_err(|e| SqlError::new(crate::query::parse_error_sqlstate(&e), e.to_string()))?;
        authorize(self.ctx, session, &statement)?;
        let params = decode_params(self.ctx, params)?;
        let cursor = match statement {
            Statement::Select(select) => Cursor::Stream(
                crate::server::open_session_select(self.ctx, session, *select, params)
                    .await
                    .map_err(|e| sql_error(&e))?,
            ),
            Statement::SelectExprs(mut items) => {
                for item in &mut items {
                    bind_scalar(&mut item.value, &params)?;
                }
                let result = crate::server::scalar_select(self.ctx, session, &items)
                    .await
                    .map_err(|e| sql_error(&e))?;
                Cursor::Rows {
                    types: result.columns.iter().map(|c| c.ty).collect(),
                    rows: result.rows.into_iter(),
                }
            }
            _ => return Err(SqlError::new("42601", "a PL/pgSQL query must be a SELECT")),
        };
        self.next += 1;
        self.cursors.insert(self.next, cursor);
        Ok(self.next)
    }

    async fn fetch(&mut self, cursor: CursorId, max: u32) -> Result<Vec<Vec<Value>>, SqlError> {
        let Some(open) = self.cursors.get_mut(&cursor) else {
            return Err(SqlError::new(
                "34000",
                format!("cursor {cursor} is not open"),
            ));
        };
        let max = usize::try_from(max).unwrap_or(usize::MAX);
        let mut batch = Vec::with_capacity(max.min(ferrosa_plpgsql::FETCH_BATCH as usize));
        match open {
            Cursor::Stream(stream) => {
                let types: Vec<ColumnType> = stream.columns().iter().map(|c| c.ty).collect();
                while batch.len() < max {
                    match stream.next_row().await.map_err(|e| sql_error(&e))? {
                        Some(row) => batch.push(row_values(row, &types)?),
                        None => break,
                    }
                }
            }
            Cursor::Rows { rows, types } => {
                for row in rows.by_ref().take(max) {
                    batch.push(row_values(row, types)?);
                }
            }
        }
        Ok(batch)
    }

    async fn execute(
        &mut self,
        session: &mut Session,
        sql: String,
        params: Vec<Value>,
    ) -> Result<u64, SqlError> {
        if let Some(parsed) = ferrosa_plpgsql::utility::parse_utility(&sql) {
            let statement = parsed.map_err(SqlError::from)?;
            let messages = super::execute_nested(self.ctx, session, statement, self.depth).await;
            return match first_error(&messages) {
                Some(error) => Err(error),
                None => Ok(0),
            };
        }
        let statement = crate::catalog::parse_pg(&sql)
            .map_err(|e| SqlError::new(crate::query::parse_error_sqlstate(&e), e.to_string()))?;
        authorize(self.ctx, session, &statement)?;
        let params = decode_params(self.ctx, params)?;
        let messages = crate::server::execute_in_session(
            self.ctx, session, &sql, statement, params, self.depth,
        )
        .await;
        match first_error(&messages) {
            Some(error) => Err(error),
            None => Ok(tag_count(&messages)),
        }
    }
}

/// Bind `$n` in a no-`FROM` expression select to its value: the legacy scalar grammar the
/// engine keeps for literal-only selects takes no parameters itself, so the host binds them
/// here (this is substitution of a bound value, not evaluation).
fn bind_scalar(value: &mut ferrosa_sql::ScalarValue, params: &[SqlValue]) -> Result<(), SqlError> {
    use ferrosa_sql::ScalarValue;
    match value {
        ScalarValue::Param(n) => {
            let bound = params.get(n.wrapping_sub(1)).ok_or_else(|| {
                SqlError::new("08P01", format!("no value bound for parameter ${n}"))
            })?;
            *value = ScalarValue::Literal(bound.clone()); // a placeholder may appear more than once
        }
        ScalarValue::Concat { left, right } => {
            bind_scalar(left, params)?;
            bind_scalar(right, params)?;
        }
        ScalarValue::Cast { value, .. } => bind_scalar(value, params)?,
        ScalarValue::Literal(_) | ScalarValue::Func(_) | ScalarValue::Subquery(_) => {}
    }
    Ok(())
}

fn authorize(ctx: &QueryContext, session: &Session, statement: &Statement) -> Result<(), SqlError> {
    crate::authz::authorize(
        &ctx.schema,
        session.auth(),
        &crate::authz::statement_permissions(statement, &ctx.default_schema),
    )
    .map_err(|e| sql_error(&e))
}

fn answer<T>(reply: Reply<T>, result: Result<T, SqlError>) {
    if reply.send(result).is_err() {
        tracing::debug!("PL/pgSQL runtime ended before its answer was delivered");
    }
}
