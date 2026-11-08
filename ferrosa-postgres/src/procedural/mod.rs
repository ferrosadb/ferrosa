//! The front end's procedural lane (decision D13): `LISTEN`/`UNLISTEN`/`NOTIFY`/`pg_notify`,
//! `DO` blocks, PL/pgSQL functions and triggers.
//!
//! Statements are recognised by [`ferrosa_plpgsql::utility::parse_utility`] before the SQL
//! engine sees them; [`execute`] runs them against the session.

pub mod functions;
mod host;
pub(crate) mod triggers;

use std::sync::Arc;

use ferrosa_plpgsql::utility::{NotifyArg, Utility};
use ferrosa_sql::Value as SqlValue;

use crate::extended::Session;
use crate::messages::{BackendMessage, FieldDescription};
use crate::notify::{ListenAction, NotifyHub, TxnNotes};
use crate::query::error_response;
use crate::server::QueryContext;

pub use functions::FunctionBackend;

/// The node-wide procedural state every session shares: the notification hub, how PL/pgSQL
/// functions run on this node, and the compiled functions by source hash.
pub struct Procedural {
    pub(crate) hub: Arc<NotifyHub>,
    pub(crate) backend: FunctionBackend,
    pub(crate) functions: functions::FunctionCache,
}

impl Procedural {
    /// Procedural state running functions with `backend`. The choice is logged.
    pub fn new(backend: FunctionBackend) -> Self {
        match &backend {
            FunctionBackend::Compiled(_) => tracing::info!("PL/pgSQL functions run compiled (D13)"),
            FunctionBackend::Interpreted => {
                tracing::warn!("PL/pgSQL functions run on the interpreter (test/dev backend)")
            }
            FunctionBackend::Unavailable(reason) => {
                tracing::warn!(%reason, "PL/pgSQL functions are unavailable on this node")
            }
        }
        Self {
            hub: Arc::new(NotifyHub::default()),
            backend,
            functions: functions::FunctionCache::default(),
        }
    }

    /// The interpreter backend, for tests.
    pub fn interpreted() -> Self {
        Self::new(FunctionBackend::Interpreted)
    }
}

/// No function backend: `CREATE FUNCTION` and trigger firing refuse with `0A000`. LISTEN/NOTIFY
/// and `DO` (always interpreted) work.
impl Default for Procedural {
    fn default() -> Self {
        Self::new(FunctionBackend::Unavailable(
            "no PL/pgSQL function backend is configured".into(),
        ))
    }
}

/// How a statement arrived: the simple protocol leads a result with its `RowDescription`;
/// the extended protocol sent it at `Describe`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Protocol {
    Simple,
    Extended,
}

/// `pg_notify`'s result column (`void`, OID 2278).
pub(crate) fn pg_notify_field() -> FieldDescription {
    FieldDescription {
        name: "pg_notify".to_string(),
        type_oid: 2278,
        type_size: 4,
        format_code: 0,
    }
}

/// The `ParameterDescription` OIDs for a prepared procedural statement: `pg_notify`'s `$n`
/// arguments are `text` unless the client declared otherwise.
pub(crate) fn param_oids(statement: &Utility, declared: &[i32]) -> Vec<i32> {
    let count = match statement {
        Utility::PgNotify { channel, payload } => [channel, payload]
            .iter()
            .filter_map(|a| match a {
                NotifyArg::Param(n) => Some(*n),
                NotifyArg::Literal(_) => None,
            })
            .max()
            .unwrap_or(0),
        _ => 0,
    };
    (0..count)
        .map(|i| match declared.get(i) {
            Some(&oid) if oid != 0 => oid,
            _ => 25,
        })
        .collect()
}

/// The result shape of a prepared procedural statement.
pub(crate) fn describe(statement: &Utility) -> BackendMessage {
    match statement {
        Utility::PgNotify { .. } => BackendMessage::RowDescription {
            fields: vec![pg_notify_field()],
        },
        _ => BackendMessage::NoData,
    }
}

/// Run a recognised statement. `params` are the bound values (empty on the simple path).
/// An error inside a transaction block poisons it, as for every other statement.
pub(crate) async fn execute(
    ctx: &QueryContext,
    session: &mut Session,
    statement: Utility,
    params: &[SqlValue],
    protocol: Protocol,
) -> Vec<BackendMessage> {
    if session.in_failed_txn() {
        return vec![error_response(
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        )];
    }
    let messages = dispatch(ctx, session, statement, params, protocol).await;
    let failed = messages
        .iter()
        .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
    if failed && session.in_txn() {
        session.mark_txn_failed();
    }
    messages
}

async fn dispatch(
    ctx: &QueryContext,
    session: &mut Session,
    statement: Utility,
    params: &[SqlValue],
    protocol: Protocol,
) -> Vec<BackendMessage> {
    let result = match statement {
        Utility::Listen(channel) => listen(ctx, session, ListenAction::Listen(Arc::from(channel)))
            .await
            .map(|()| tag("LISTEN")),
        Utility::Unlisten(channel) => {
            listen(ctx, session, ListenAction::Unlisten(channel.map(Arc::from)))
                .await
                .map(|()| tag("UNLISTEN"))
        }
        Utility::Notify { channel, payload } => notify(ctx, session, &channel, &payload)
            .await
            .map(|()| tag("NOTIFY")),
        Utility::PgNotify { channel, payload } => {
            pg_notify(ctx, session, &channel, &payload, params, protocol).await
        }
        Utility::Do { body } => do_block(ctx, session, &body).await,
        Utility::CreateFunction(f) => functions::create(ctx, session, f).await,
        Utility::DropFunction {
            if_exists,
            name,
            arg_types,
        } => functions::drop(ctx, session, if_exists, name, arg_types).await,
        Utility::CreateTrigger(t) => triggers::create(ctx, session, t).await,
        Utility::DropTrigger {
            if_exists,
            name,
            table,
        } => triggers::drop(ctx, session, if_exists, name, table).await,
    };
    result.unwrap_or_else(|error| vec![error])
}

/// A statement run from inside PL/pgSQL (through the host's `execute`): `DO`, `LISTEN` and
/// `UNLISTEN` make no sense there and are refused; the rest run as from a client.
pub(crate) async fn execute_nested(
    ctx: &QueryContext,
    session: &mut Session,
    statement: Utility,
    _depth: usize,
) -> Vec<BackendMessage> {
    if matches!(
        statement,
        Utility::Do { .. } | Utility::Listen(_) | Utility::Unlisten(_)
    ) {
        return vec![error_response(
            "0A000",
            &format!(
                "{} cannot be run from inside PL/pgSQL",
                statement_name(&statement)
            ),
        )];
    }
    Box::pin(dispatch(ctx, session, statement, &[], Protocol::Extended)).await
}

/// A PL/pgSQL parse refusal as an error response.
pub(crate) fn pl_error(error: ferrosa_plpgsql::PlError) -> BackendMessage {
    error_response(error.sqlstate, &error.message)
}

/// A host or runtime error as an error response.
pub(crate) fn runtime_error(error: &ferrosa_plpgsql::SqlError) -> BackendMessage {
    error_response(&error.sqlstate, &error.message)
}

/// `DO $$ … $$`: parse, then interpret against the session. Outside a transaction block the
/// block is its own transaction: everything it did commits together, or nothing does.
async fn do_block(
    ctx: &QueryContext,
    session: &mut Session,
    body: &str,
) -> Result<Vec<BackendMessage>, BackendMessage> {
    let program = ferrosa_plpgsql::parse_do_block(body).map_err(pl_error)?;
    let implicit = !session.in_txn();
    if implicit {
        crate::server::begin_implicit_transaction(ctx, session).await?;
    }
    let ran = host::run(ctx, session, 0, move |mut host| {
        ferrosa_plpgsql::interpret(&program, ferrosa_plpgsql::Call::default(), &mut host)
    })
    .await;
    if let Err(error) = ran {
        if implicit {
            session.end_txn();
        }
        return Err(runtime_error(&error));
    }
    if implicit {
        let committed = crate::server::commit_txn(ctx, session).await;
        if let Some(error) = committed
            .into_iter()
            .find(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
        {
            return Err(error);
        }
    }
    Ok(tag("DO"))
}

fn statement_name(statement: &Utility) -> &'static str {
    match statement {
        Utility::Do { .. } => "DO",
        Utility::CreateFunction(_) => "CREATE FUNCTION",
        Utility::DropFunction { .. } => "DROP FUNCTION",
        Utility::CreateTrigger(_) => "CREATE TRIGGER",
        Utility::DropTrigger { .. } => "DROP TRIGGER",
        Utility::Listen(_) => "LISTEN",
        Utility::Unlisten(_) => "UNLISTEN",
        Utility::Notify { .. } => "NOTIFY",
        Utility::PgNotify { .. } => "pg_notify",
    }
}

fn tag(tag: &str) -> Vec<BackendMessage> {
    vec![BackendMessage::CommandComplete {
        tag: tag.to_string(),
    }]
}

/// `LISTEN` / `UNLISTEN`: at commit inside a transaction block, at once otherwise.
///
/// A cluster node refuses `LISTEN`: delivery between nodes is not implemented, and a session
/// that subscribed here would silently miss every notification committed on another node.
async fn listen(
    ctx: &QueryContext,
    session: &mut Session,
    action: ListenAction,
) -> Result<(), BackendMessage> {
    if matches!(action, ListenAction::Listen(_)) && ctx.accord.committer().is_some() {
        return Err(error_response(
            "0A000",
            "LISTEN is not supported on a cluster node: notifications are delivered only to \
             sessions on the node where the transaction commits, and cross-node delivery is \
             not implemented",
        ));
    }
    if session.in_txn() {
        session.txn_notes_mut().listen(action);
        return Ok(());
    }
    let pid = session.notify_pid(&ctx.procedural.hub);
    let guard = ctx.procedural.hub.lock().await;
    guard.apply_listens(session.listener_mut(), pid, vec![action]);
    Ok(())
}

/// `NOTIFY` / `pg_notify`: queued in the open transaction, or — outside one — its own
/// transaction, published at once.
pub(crate) async fn notify(
    ctx: &QueryContext,
    session: &mut Session,
    channel: &str,
    payload: &str,
) -> Result<(), BackendMessage> {
    if session.in_txn() {
        return session.txn_notes_mut().notify(channel, payload);
    }
    let mut notes = TxnNotes::default();
    notes.notify(channel, payload)?;
    let pid = session.notify_pid(&ctx.procedural.hub);
    let guard = ctx.procedural.hub.lock().await;
    guard.reserve(&notes)?;
    guard.publish(pid, notes);
    Ok(())
}

async fn pg_notify(
    ctx: &QueryContext,
    session: &mut Session,
    channel: &NotifyArg,
    payload: &NotifyArg,
    params: &[SqlValue],
    protocol: Protocol,
) -> Result<Vec<BackendMessage>, BackendMessage> {
    let channel = notify_arg(channel, params)?.unwrap_or_default();
    let payload = notify_arg(payload, params)?.unwrap_or_default();
    notify(ctx, session, &channel, &payload).await?;
    let mut out = Vec::with_capacity(3);
    if protocol == Protocol::Simple {
        out.push(BackendMessage::RowDescription {
            fields: vec![pg_notify_field()],
        });
    }
    out.push(BackendMessage::DataRow {
        columns: vec![Some(Vec::new())],
    });
    out.push(BackendMessage::CommandComplete {
        tag: "SELECT 1".to_string(),
    });
    Ok(out)
}

/// A `pg_notify` argument's text (`None` for NULL, which PostgreSQL treats as empty).
fn notify_arg(arg: &NotifyArg, params: &[SqlValue]) -> Result<Option<String>, BackendMessage> {
    let value = match arg {
        NotifyArg::Literal(text) => return Ok(Some(text.clone())), // owned copy: the parsed statement is reused by a prepared portal
        NotifyArg::Param(n) => params.get(n.wrapping_sub(1)).ok_or_else(|| {
            error_response("08P01", &format!("no value bound for parameter ${n}"))
        })?,
    };
    crate::query::value_text(value).map_err(|error| crate::query::encode_error_response(&error))
}

/// A notifying transaction's publication, prepared before its commit: the hub's publish lock
/// is held from [`prepare_publication`] until [`Publication::finish`], so the commit and the
/// delivery of its notifications are one step (commit order), and the listeners' queues were
/// checked for room before anything was applied.
pub(crate) struct Publication {
    guard: crate::notify::PublishGuard,
    pid: i32,
    notes: TxnNotes,
    listens: Vec<ListenAction>,
}

/// Take the publish lock and reserve room for `notes`.
///
/// # Errors
///
/// `54000` when a listener's queue lacks room; the caller ends the transaction uncommitted.
pub(crate) async fn prepare_publication(
    ctx: &QueryContext,
    session: &mut Session,
    notes: TxnNotes,
) -> Result<Publication, BackendMessage> {
    let pid = session.notify_pid(&ctx.procedural.hub);
    let guard = ctx.procedural.hub.lock().await;
    let (notes, listens) = notes.into_parts();
    guard.reserve(&notes)?;
    Ok(Publication {
        guard,
        pid,
        notes,
        listens,
    })
}

impl Publication {
    /// After the commit: publish and apply LISTENs if `commit_messages` say it committed;
    /// otherwise drop everything (nothing is delivered for an uncommitted transaction).
    pub(crate) fn finish(self, session: &mut Session, commit_messages: &[BackendMessage]) {
        let committed = matches!(
            commit_messages.first(),
            Some(BackendMessage::CommandComplete { tag }) if tag == "COMMIT"
        );
        if !committed {
            return;
        }
        let Publication {
            guard,
            pid,
            notes,
            listens,
        } = self;
        guard.publish(pid, notes);
        guard.apply_listens(session.listener_mut(), pid, listens);
    }
}
