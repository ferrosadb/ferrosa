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
/// Estimated bytes of row events one statement may queue (`54000` beyond). The queue is
/// bounded by memory, not by a row count: 256 MiB holds the 100 000 events of the former
/// count bound at a typical eight-column row with both images, and a statement past it is
/// refused and rolled back, never truncated.
pub(crate) const TRIGGER_QUEUE_BYTES: usize = 256 * 1024 * 1024;
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

/// A row image by column name. The names are shared with the statement's [`ColumnNames`]
/// (or moved in from the writer), so building an image copies no strings.
pub(crate) type RowImage = HashMap<Arc<str>, SqlValue>;

/// A table's column names, shared by every image one statement builds.
pub(crate) type ColumnNames = std::collections::HashSet<Arc<str>>;

/// The column names of `meta` when it has triggers, built once per statement; empty when it
/// has none, since no image is then built.
pub(crate) fn column_names(meta: &TableMetadata) -> ColumnNames {
    if !has_triggers(meta) {
        return ColumnNames::new();
    }
    meta.columns
        .keys()
        .map(|name| Arc::<str>::from(name.as_str()))
        .collect()
}

/// `name` as the statement's shared column name.
///
/// # Errors
///
/// `XX000` when `name` is not a column of the table: an image naming a column the table
/// does not have is an engine fault, not a value to bind.
pub(crate) fn shared_name(names: &ColumnNames, name: &str) -> Result<Arc<str>, BackendMessage> {
    names.get(name).map(Arc::clone).ok_or_else(|| {
        error_response(
            "XX000",
            &format!("trigger image names \"{name}\", which is not a column of the table"),
        )
    })
}

/// An owned row's image, its values moved and its names shared.
///
/// # Errors
///
/// As [`shared_name`].
pub(crate) fn shared_image(
    names: &ColumnNames,
    row: HashMap<String, SqlValue>,
) -> Result<RowImage, BackendMessage> {
    let mut image = RowImage::with_capacity(row.len());
    for (name, value) in row {
        image.insert(shared_name(names, &name)?, value);
    }
    Ok(image)
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
    pub(crate) new: RowImage,
    /// `OLD`: the row before an UPDATE.
    pub(crate) old: Option<RowImage>,
}

impl RowEvent {
    /// Estimated resident bytes of this event: its inline size, its strings and both images.
    /// A shared name or value is counted in every event that holds it, so the estimate errs
    /// high, toward refusing early rather than holding more than the budget.
    pub(crate) fn estimated_bytes(&self) -> usize {
        let set_columns = match &self.op {
            RowOp::Insert => 0,
            RowOp::Update { set_columns } => set_columns
                .iter()
                .map(|c| std::mem::size_of::<String>() + c.len())
                .sum(),
        };
        std::mem::size_of::<Self>()
            + self.keyspace.len()
            + self.table.len()
            + set_columns
            + image_bytes(&self.new)
            + self.old.as_ref().map_or(0, image_bytes)
    }
}

/// Estimated resident bytes of an image: its table slots and each entry's heap payload.
fn image_bytes(image: &RowImage) -> usize {
    image.capacity() * std::mem::size_of::<(Arc<str>, SqlValue)>()
        + image
            .iter()
            .map(|(name, value)| name.len() + crate::mvcc::value_heap_bytes(value))
            .sum::<usize>()
}

/// The statement's trigger queue.
#[derive(Debug, Default)]
pub(crate) struct RowEvents {
    events: Vec<RowEvent>,
    /// The queued events' estimated bytes, held at or below [`TRIGGER_QUEUE_BYTES`].
    bytes: usize,
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
    /// `54000` when the event would take the queue past [`TRIGGER_QUEUE_BYTES`]; the event
    /// is not queued and the statement fails, so no trigger event is ever silently dropped.
    pub(crate) fn push(&mut self, event: RowEvent) -> Result<(), BackendMessage> {
        let bytes = self.bytes.saturating_add(event.estimated_bytes());
        if bytes > TRIGGER_QUEUE_BYTES {
            return Err(error_response(
                "54000",
                &format!(
                    "a statement's AFTER ROW trigger events exceeded {TRIGGER_QUEUE_BYTES} bytes \
                     after {} events",
                    self.events.len()
                ),
            ));
        }
        self.bytes = bytes;
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
    make: impl FnOnce() -> Result<RowEvent, BackendMessage>,
) -> Result<(), BackendMessage> {
    match sink {
        Some(sink) if has_triggers(meta) => match sink.lock() {
            Ok(mut events) => events.push(make()?),
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
    row: &RowImage,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sqlstate(message: &BackendMessage) -> Option<&str> {
        match message {
            BackendMessage::ErrorResponse { fields } => fields
                .iter()
                .find(|(code, _)| *code == b'C')
                .map(|(_, state)| state.as_str()),
            _ => None,
        }
    }

    /// An UPDATE event of a typical eight-column row: four ints and four short texts,
    /// with both images bound.
    fn typical_update(names: &ColumnNames) -> RowEvent {
        let image: RowImage = names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let value = if i % 2 == 0 {
                    SqlValue::Int(i as i64)
                } else {
                    SqlValue::Text(format!("value-{i:04}").into())
                };
                (Arc::clone(name), value)
            })
            .collect();
        RowEvent {
            keyspace: "public".into(),
            table: "jobs".into(),
            op: RowOp::Update {
                set_columns: vec!["state".into()],
            },
            new: image.clone(),
            old: Some(image),
        }
    }

    fn eight_columns() -> ColumnNames {
        [
            "id", "state", "queue", "worker", "attempt", "args", "errors", "tags",
        ]
        .into_iter()
        .map(Arc::<str>::from)
        .collect()
    }

    #[test]
    fn the_byte_budget_holds_what_the_old_event_count_held_at_typical_rows() {
        let names = eight_columns();
        let one = typical_update(&names).estimated_bytes();
        assert!(one > 0);
        assert!(
            one * 100_000 <= TRIGGER_QUEUE_BYTES,
            "100000 typical events ({one} bytes each) must fit in {TRIGGER_QUEUE_BYTES}"
        );
    }

    #[test]
    fn crossing_the_byte_budget_is_refused_with_54000_and_nothing_is_dropped() {
        let big = SqlValue::Text("x".repeat(1 << 20).into());
        let make = || RowEvent {
            keyspace: "public".into(),
            table: "blobs".into(),
            op: RowOp::Insert,
            new: [(Arc::<str>::from("body"), big.clone())]
                .into_iter()
                .collect(),
            old: None,
        };
        let per_event = make().estimated_bytes();
        let fits = TRIGGER_QUEUE_BYTES / per_event;
        let mut events = RowEvents::default();
        for _ in 0..fits {
            events.push(make()).expect("within the budget");
        }
        let refused = events.push(make()).expect_err("past the budget");
        assert_eq!(sqlstate(&refused), Some("54000"));
        // Refusal, not truncation: every accepted event is still queued, and the refused
        // one was not.
        assert_eq!(events.events.len(), fits);
        assert!(events.bytes <= TRIGGER_QUEUE_BYTES);
    }
}
