//! AFTER ROW triggers (D13): `CREATE TRIGGER … AFTER INSERT | UPDATE [OF col] … FOR EACH ROW
//! EXECUTE PROCEDURE f()` and `DROP TRIGGER [IF EXISTS] … ON t`.
//!
//! **Storage.** A trigger belongs to its table, as in PostgreSQL (`pg_trigger.tgrelid`): the
//! table's triggers are the JSON list under the table extension [`TRIGGERS_KEY`], written
//! through the same `ALTER TABLE` schema-change path as any table option, so they replicate
//! with the table and go away with it.
//!
//! **Firing.** The DML executor records one [`RowEvent`] per row it writes to a table that has
//! triggers (`NEW`, `OLD`, the `SET` columns). After the statement — before control returns
//! to the client and before any commit — [`fire`] runs each matching trigger's function once
//! per event, in row order and then trigger-name order, with `NEW`, `OLD` and `TG_OP` bound,
//! through the host interface of the same session: its writes join the same transaction and
//! roll back with it (`specs/lean4/Ferrosa/Sql/Trigger.lean`). A trigger function's own
//! writes may fire triggers in turn, to a depth of [`MAX_TRIGGER_DEPTH`].

use std::collections::HashMap;
use std::sync::Arc;

use ferrosa_plpgsql::utility::{CreateTrigger, QualifiedName, TriggerEvent};
use ferrosa_plpgsql::{Program, TriggerCall, Value};
use ferrosa_schema::{TableMetadata, TableUpdates};
use ferrosa_sql::Value as SqlValue;
use serde::{Deserialize, Serialize};

use crate::extended::Session;
use crate::messages::BackendMessage;
use crate::query::error_response;
use crate::server::QueryContext;

/// The table extension holding a table's triggers.
pub(crate) const TRIGGERS_KEY: &str = "pg.triggers";
/// Row events one statement may queue (`54000` beyond): the trigger queue is bounded.
pub(crate) const MAX_ROW_EVENTS: usize = 100_000;
/// Nested trigger firing (a trigger's write firing a trigger), PostgreSQL's
/// `max_stack_depth` analogue (`54001` beyond). Each level's future is boxed, but polling
/// still nests one call chain per level, so the bound is set below what a 2 MiB tokio worker
/// stack holds in a debug build (`a_trigger_that_fires_itself_stops_at_the_depth_bound` runs
/// on a 2 MiB test thread; 16 overflowed it).
pub(crate) const MAX_TRIGGER_DEPTH: usize = 8;

/// A stored trigger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredTrigger {
    pub(crate) name: String,
    /// `"INSERT"` or `"UPDATE"`, with the `OF` columns for an UPDATE (empty: any update).
    pub(crate) events: Vec<(String, Vec<String>)>,
    pub(crate) function_schema: String,
    pub(crate) function_name: String,
}

/// What happened to a row.
#[derive(Debug)]
pub(crate) enum RowOp {
    Insert,
    Update { set_columns: Vec<String> },
}

/// One written row of a table with triggers.
#[derive(Debug)]
pub(crate) struct RowEvent {
    pub(crate) keyspace: String,
    pub(crate) table: String,
    pub(crate) op: RowOp,
    /// `NEW`: the row's values by column (a column absent here is NULL).
    pub(crate) new: HashMap<String, SqlValue>,
    /// `OLD`: the row before an UPDATE.
    pub(crate) old: Option<HashMap<String, SqlValue>>,
}

/// The statement's trigger queue.
#[derive(Debug, Default)]
pub(crate) struct RowEvents {
    events: Vec<RowEvent>,
}

/// The shared handle the DML executor records through.
/// A `Mutex` (not a `RefCell`) because the DML futures holding it must be `Send`; it is locked
/// only for one push, never across an await.
pub(crate) type EventSink<'a> = Option<&'a std::sync::Mutex<RowEvents>>;

impl RowEvents {
    /// Queue an event.
    ///
    /// # Errors
    ///
    /// `54000` past [`MAX_ROW_EVENTS`].
    pub(crate) fn push(&mut self, event: RowEvent) -> Result<(), BackendMessage> {
        if self.events.len() >= MAX_ROW_EVENTS {
            return Err(error_response(
                "54000",
                &format!("a statement queued more than {MAX_ROW_EVENTS} trigger events"),
            ));
        }
        self.events.push(event);
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Record an event when `sink` is set and the table has triggers.
pub(crate) fn record(
    sink: EventSink<'_>,
    meta: &TableMetadata,
    make: impl FnOnce() -> RowEvent,
) -> Result<(), BackendMessage> {
    match sink {
        Some(sink) if has_triggers(meta) => match sink.lock() {
            Ok(mut events) => events.push(make()),
            Err(_) => Err(error_response("XX000", "trigger event queue lock poisoned")),
        },
        _ => Ok(()),
    }
}

/// Whether the table has any trigger.
pub(crate) fn has_triggers(meta: &TableMetadata) -> bool {
    meta.extensions
        .get(TRIGGERS_KEY)
        .is_some_and(|json| json.trim() != "[]")
}

/// The table's triggers, by name.
pub(crate) fn stored(meta: &TableMetadata) -> Result<Vec<StoredTrigger>, BackendMessage> {
    let Some(json) = meta.extensions.get(TRIGGERS_KEY) else {
        return Ok(Vec::new());
    };
    let mut triggers: Vec<StoredTrigger> = serde_json::from_str(json).map_err(|e| {
        tracing::error!(%e, table = %meta.name, "stored trigger list is unreadable");
        error_response(
            "XX001",
            &format!("stored triggers of \"{}\" are unreadable: {e}", meta.name),
        )
    })?;
    triggers.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(triggers)
}

fn table_meta(
    snapshot: &ferrosa_schema::SchemaSnapshot,
    keyspace: &str,
    table: &str,
) -> Result<TableMetadata, BackendMessage> {
    snapshot
        .tables
        .get(&(keyspace.to_string(), table.to_string()))
        .cloned() // copied out of the shared schema snapshot
        .ok_or_else(|| {
            error_response(
                "42P01",
                &format!("relation \"{keyspace}.{table}\" does not exist"),
            )
        })
}

/// The first trigger (name, table) whose function is `(keyspace, name)`.
pub(crate) fn dependent_trigger(
    snapshot: &ferrosa_schema::SchemaSnapshot,
    keyspace: &str,
    function: &str,
) -> Result<Option<(String, String)>, BackendMessage> {
    for meta in snapshot.tables.values() {
        for trigger in stored(meta)? {
            if trigger.function_schema == keyspace && trigger.function_name == function {
                return Ok(Some((trigger.name, meta.name.clone()))); // copied out of the snapshot
            }
        }
    }
    Ok(None)
}

fn save(triggers: &[StoredTrigger]) -> Result<TableUpdates, BackendMessage> {
    let json = serde_json::to_string(triggers)
        .map_err(|e| error_response("XX000", &format!("encoding triggers: {e}")))?;
    Ok(TableUpdates {
        params: None,
        add_columns: Vec::new(),
        drop_columns: Vec::new(),
        extensions: Some(HashMap::from([(TRIGGERS_KEY.to_string(), json)])),
    })
}

fn authorize_alter(
    ctx: &QueryContext,
    session: &Session,
    keyspace: &str,
    table: &str,
) -> Result<(), BackendMessage> {
    crate::authz::authorize(
        &ctx.schema,
        session.auth(),
        &[(
            ferrosa_schema::auth::permission::Permission::Alter,
            ferrosa_schema::auth::permission::Resource::Table(
                keyspace.to_string(),
                table.to_string(),
            ),
        )],
    )
}

/// `CREATE TRIGGER`.
pub(crate) async fn create(
    ctx: &QueryContext,
    session: &mut Session,
    statement: CreateTrigger,
) -> Result<Vec<BackendMessage>, BackendMessage> {
    super::ddl_available(ctx, "CREATE TRIGGER")?;
    let view = super::ddl_view(ctx, session);
    let CreateTrigger {
        name,
        events,
        table,
        function,
    } = statement;
    let keyspace = super::functions::resolve_schema(ctx, &table);
    authorize_alter(ctx, session, &keyspace, &table.name)?;
    let meta = table_meta(&view, &keyspace, &table.name)?;
    let function_schema = super::functions::resolve_schema(ctx, &function);
    // The function must exist, be PL/pgSQL, take no arguments and return trigger.
    super::functions::load_trigger_function(ctx, &view, &function_schema, &function.name).await?;
    let mut triggers = stored(&meta)?;
    if triggers.iter().any(|t| t.name == name) {
        return Err(error_response(
            "42710",
            &format!(
                "trigger \"{name}\" for relation \"{}\" already exists",
                table.name
            ),
        ));
    }
    for event in &events {
        if let TriggerEvent::Update { columns } = event {
            if let Some(missing) = columns
                .iter()
                .find(|c| !meta.columns.contains_key(c.as_str()))
            {
                return Err(error_response(
                    "42703",
                    &format!(
                        "column \"{missing}\" of relation \"{}\" does not exist",
                        table.name
                    ),
                ));
            }
        }
    }
    triggers.push(StoredTrigger {
        name,
        events: events.into_iter().map(stored_event).collect(),
        function_schema,
        function_name: function.name,
    });
    let updates = save(&triggers)?;
    let op = super::DdlOp::AlterTable {
        keyspace,
        table: table.name,
        updates,
    };
    super::apply_ddl(ctx, session, vec![op]).await?;
    Ok(vec![BackendMessage::CommandComplete {
        tag: "CREATE TRIGGER".into(),
    }])
}

fn stored_event(event: TriggerEvent) -> (String, Vec<String>) {
    match event {
        TriggerEvent::Insert => ("INSERT".into(), Vec::new()),
        TriggerEvent::Update { columns } => ("UPDATE".into(), columns),
    }
}

/// `DROP TRIGGER [IF EXISTS] name ON table`.
pub(crate) async fn drop(
    ctx: &QueryContext,
    session: &mut Session,
    if_exists: bool,
    name: String,
    table: QualifiedName,
) -> Result<Vec<BackendMessage>, BackendMessage> {
    super::ddl_available(ctx, "DROP TRIGGER")?;
    let view = super::ddl_view(ctx, session);
    let keyspace = super::functions::resolve_schema(ctx, &table);
    authorize_alter(ctx, session, &keyspace, &table.name)?;
    let done = || {
        Ok(vec![BackendMessage::CommandComplete {
            tag: "DROP TRIGGER".into(),
        }])
    };
    let meta = match table_meta(&view, &keyspace, &table.name) {
        Ok(meta) => meta,
        Err(_) if if_exists => return done(),
        Err(error) => return Err(error),
    };
    let mut triggers = stored(&meta)?;
    let before = triggers.len();
    triggers.retain(|t| t.name != name);
    if triggers.len() == before {
        if if_exists {
            return done();
        }
        return Err(error_response(
            "42704",
            &format!(
                "trigger \"{name}\" for table \"{}\" does not exist",
                table.name
            ),
        ));
    }
    let updates = save(&triggers)?;
    let op = super::DdlOp::AlterTable {
        keyspace,
        table: table.name,
        updates,
    };
    super::apply_ddl(ctx, session, vec![op]).await?;
    done()
}

impl StoredTrigger {
    /// Whether this trigger fires for `op` (PostgreSQL: `UPDATE OF` fires when one of its
    /// columns is a target of the `SET` list).
    fn fires_for(&self, op: &RowOp) -> bool {
        self.events.iter().any(|(kind, columns)| match op {
            RowOp::Insert => kind == "INSERT",
            RowOp::Update { set_columns } => {
                kind == "UPDATE"
                    && (columns.is_empty() || columns.iter().any(|c| set_columns.contains(c)))
            }
        })
    }
}

/// Fire the queued events' triggers, in the calling session's transaction.
pub(crate) async fn fire(
    ctx: &QueryContext,
    session: &mut Session,
    events: RowEvents,
    depth: usize,
) -> Result<(), BackendMessage> {
    if events.is_empty() {
        return Ok(());
    }
    if depth >= MAX_TRIGGER_DEPTH {
        return Err(error_response(
            "54001",
            &format!("trigger nesting exceeded {MAX_TRIGGER_DEPTH} levels"),
        ));
    }
    let committed = ctx.schema.snapshot();
    for event in events.events {
        let meta = table_meta(&committed, &event.keyspace, &event.table)?;
        for trigger in stored(&meta)?.iter().filter(|t| t.fires_for(&event.op)) {
            let loaded = super::functions::load_trigger_function(
                ctx,
                &committed,
                &trigger.function_schema,
                &trigger.function_name,
            )
            .await?;
            let call = trigger_call(ctx, &meta, &event, &loaded.program)?;
            // Boxed: a trigger's write can fire a trigger, and each level must live on the
            // heap, not the stack, so the depth bound (not a stack overflow) ends a cascade.
            Box::pin(super::functions::call(
                ctx,
                session,
                &loaded,
                call,
                depth + 1,
            ))
            .await
            .map_err(|e| super::runtime_error(&e))?;
        }
    }
    Ok(())
}

/// The trigger call for an event: `TG_OP` and the referenced `NEW`/`OLD` fields, each with the
/// column's real type OID (so comparisons in the function see the column's type).
fn trigger_call(
    ctx: &QueryContext,
    meta: &TableMetadata,
    event: &RowEvent,
    program: &Program,
) -> Result<ferrosa_plpgsql::Call, BackendMessage> {
    let op = match event.op {
        RowOp::Insert => "INSERT",
        RowOp::Update { .. } => "UPDATE",
    };
    let new_row = Some(fields(ctx, meta, &event.new, &program.new_fields(), "new")?);
    let old_row = match &event.old {
        Some(old) => Some(fields(ctx, meta, old, &program.old_fields(), "old")?),
        None => None,
    };
    Ok(ferrosa_plpgsql::Call {
        args: Vec::new(),
        trigger: Some(TriggerCall {
            op: op.into(),
            new_row,
            old_row,
        }),
    })
}

fn fields(
    ctx: &QueryContext,
    meta: &TableMetadata,
    row: &HashMap<String, SqlValue>,
    names: &[&str],
    record: &str,
) -> Result<Vec<Value>, BackendMessage> {
    names
        .iter()
        .map(|name| {
            let Some(column) = meta.columns.get(*name) else {
                return Err(error_response(
                    "42703",
                    &format!("record \"{record}\" has no field \"{name}\""),
                ));
            };
            let pg = crate::pg_types::pg_type_of_column(
                &column.column_type,
                &meta.keyspace,
                &ctx.schema,
            )
            .map_err(|e| error_response("42704", &e.to_string()))?;
            let text = match row.get(*name) {
                Some(value) => crate::query::value_text(value)
                    .map_err(|e| crate::query::encode_error_response(&e))?,
                None => None,
            };
            Ok(Value {
                type_oid: pg.oid,
                text: text.map(Arc::from),
            })
        })
        .collect()
}
