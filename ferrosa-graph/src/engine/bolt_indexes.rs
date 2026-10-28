//! First-access scalar-index setup, shared by every Bolt connection to an engine.
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use ferrosa_cluster::write_path::WritePath;
use ferrosa_index::IndexType;
use ferrosa_schema::metadata::index::IndexMetadata;
use ferrosa_schema::{ColumnKind, Schema, SchemaSnapshot};
use ferrosa_storage::{StorageEngine, TableId};
use futures::StreamExt;
use tokio::sync::Mutex;

use super::GraphSchemaCoordinator;
use crate::error::{GraphError, Result};

#[derive(Clone)]
struct Ready {
    // Weak keeps allocation identity unique without retaining old schema maps.
    snapshot: Weak<SchemaSnapshot>,
    path: Weak<WritePath>,
}

pub(super) struct BoltIndexSetup {
    ready: ArcSwap<HashMap<String, HashMap<String, Ready>>>,
    pub(super) gate: Mutex<()>,
    #[cfg(test)]
    pub(super) initializations: std::sync::atomic::AtomicUsize,
}

pub(super) struct Context<'a> {
    pub(super) schema: &'a Arc<Schema>,
    pub(super) storage: &'a Arc<StorageEngine>,
    pub(super) path: Arc<WritePath>,
    pub(super) coordinator: &'a dyn GraphSchemaCoordinator,
    pub(super) timeout: Duration,
}

impl BoltIndexSetup {
    pub(super) fn new() -> Self {
        Self {
            ready: ArcSwap::from_pointee(HashMap::new()),
            gate: Mutex::new(()),
            #[cfg(test)]
            initializations: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn is_ready(
        &self,
        keyspace: &str,
        table: &str,
        snapshot: &Arc<SchemaSnapshot>,
        path: &Arc<WritePath>,
    ) -> bool {
        self.ready
            .load()
            .get(keyspace)
            .and_then(|tables| tables.get(table))
            .is_some_and(|ready| {
                ready.snapshot.as_ptr() == Arc::as_ptr(snapshot)
                    && ready.path.as_ptr() == Arc::as_ptr(path)
            })
    }

    pub(super) async fn ensure(
        &self,
        keyspace: &str,
        table_name: &str,
        context: Context<'_>,
    ) -> Result<()> {
        let Context {
            schema,
            storage,
            path,
            coordinator,
            timeout,
        } = context;
        if self.is_ready(keyspace, table_name, &schema.snapshot(), &path) {
            return Ok(());
        }
        tokio::time::timeout(timeout, async {
            let _gate = self.gate.lock().await;
            if self.is_ready(keyspace, table_name, &schema.snapshot(), &path) {
                return Ok(());
            }
            #[cfg(test)]
            self.initializations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            loop {
                let snapshot = schema.snapshot();
                let mut pending = Vec::new();
                for table in snapshot.tables.values().filter(|table|
                    table.keyspace == keyspace && table.name == table_name && !table.is_system &&
                    schema.virtual_tables().get(keyspace, table_name).is_none() &&
                    table.extensions.get("graph.type").is_some_and(|kind| kind == "vertex" || kind == "edge")) {
                    for column in table.columns.values().filter(|column|
                        matches!(column.kind, ColumnKind::Regular | ColumnKind::Static) && scalar(&column.column_type)) {
                        let existing = snapshot.indexes.values().find(|index|
                            index.keyspace == keyspace && index.table == table.name &&
                            index.target_columns.as_slice() == [column.name.as_str()] &&
                            index.filter_predicate.is_none() &&
                            matches!(index.index_type, IndexType::BTree | IndexType::Hash));
                        let index = existing.cloned().unwrap_or_else(|| IndexMetadata {
                            keyspace: keyspace.into(), table: table.name.clone(),
                            name: format!("__bolt_{}_{}_idx", table.name, column.name),
                            index_type: IndexType::BTree,
                            target_columns: vec![column.name.clone()],
                            filter_predicate: None, options: HashMap::new(),
                        });
                        if existing.is_none() {
                            coordinator.apply_create_index(index.clone()).await?;
                        }
                        let id = TableId::new(keyspace, &table.name);
                        if !path.declares_index_locally(&id, &index.name) {
                            let engine = Arc::clone(storage);
                            let definition = index.clone();
                            let partition_key = table.partition_key.clone();
                            tokio::task::spawn_blocking(move ||
                                ferrosa_cluster::ddl_path::build_replicated_index(
                                    &engine, &definition, &partition_key, "Bolt first access"))
                                .await.map_err(|error| GraphError::Validation(format!("Bolt index setup task: {error}")))?
                                .map_err(|error| GraphError::Validation(format!("Bolt index setup: {error}")))?;
                        }
                        if !path.index_is_current(&id, &index.name) {
                            pending.push(index.name);
                        } else if matches!(&*path, WritePath::Cluster(_)) {
                            // The merge initializes every source before yielding
                            // its first row. A bounded empty-key probe therefore
                            // observes stale remote indexes as well as the local
                            // state, without draining any posting list.
                            let ready = match path.index_read_stream(
                                &id, &index.name, &ferrosa_index::IndexKey(Vec::new()), None).await {
                                Ok(mut stream) => !matches!(stream.next().await, Some(Err(_))),
                                Err(_) => false,
                            };
                            if !ready { pending.push(index.name); }
                        }
                    }
                }
                // Every index was derived from this snapshot. Our own DDL, or a
                // racing schema change, requires another pass before publishing.
                if pending.is_empty() && Arc::ptr_eq(&schema.snapshot(), &snapshot) {
                    let mut ready = (**self.ready.load()).clone();
                    ready.entry(keyspace.into()).or_default().insert(table_name.into(), Ready { snapshot: Arc::downgrade(&snapshot), path: Arc::downgrade(&path) });
                    self.ready.store(Arc::new(ready));
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.map_err(|_| GraphError::Validation(format!(
            "Bolt scalar indexes for keyspace '{keyspace}' are not current after {timeout:?}; retry after backfill completes")))?
    }
}

fn scalar(column_type: &str) -> bool {
    matches!(
        column_type,
        "ascii"
            | "bigint"
            | "blob"
            | "boolean"
            | "counter"
            | "date"
            | "decimal"
            | "double"
            | "duration"
            | "float"
            | "inet"
            | "int"
            | "smallint"
            | "text"
            | "time"
            | "timestamp"
            | "timeuuid"
            | "tinyint"
            | "uuid"
            | "varchar"
            | "varint"
    )
}
