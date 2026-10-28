//! Count liveness without constructing payload rows or sending them over a channel.
//! Physical keys reconcile with the same live MVCC registry as ordinary scans.
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use super::{decoded_row_values, read_consistency, ScanContext, ScanFailure, Value};
use crate::mvcc::MvccTableView;

#[derive(Default)]
struct CountState {
    seen: HashSet<Vec<Value>>,
    replacements: u64,
    error: Option<String>,
}

impl CountState {
    /// `true` lets the backend count the physical row. A snapshot/own-write
    /// replacement is counted here instead, including a now-deleted partition.
    fn observe(
        &mut self,
        ctx: &ScanContext,
        view: Option<&MvccTableView>,
        failure: &ScanFailure,
        key: &ferrosa_common::DecoratedKey,
    ) -> Result<bool, String> {
        let index = ctx.pk_idx[0];
        let value = ferrosa_row_bridge::decode_value(&ctx.col_types[index], key.key.as_bytes())
            .map_err(|error| format!("count key decode failed: {error}"))?;
        let key = decoded_row_values(vec![Some(value)])?;
        let historical = view
            .map(|view| view.row(&key))
            .transpose()
            .map_err(|_| {
                failure.expired();
                "PostgreSQL transaction snapshot expired".to_string()
            })?
            .flatten();
        let replacement = ctx.overlay.get(&key).or(historical.as_deref());
        let first_visit = self.seen.insert(key);
        if let Some(row) = replacement {
            if first_visit && row.is_some() {
                self.replacements += 1;
            }
            Ok(false)
        } else {
            // A fragmented physical partition may visit the predicate again;
            // the backend itself counts its disjoint live rows only once.
            Ok(true)
        }
    }
}

fn ensure_active(view: Option<&MvccTableView>, failure: &ScanFailure) -> Result<(), String> {
    if let Some(view) = view {
        view.ensure_active().map_err(|_| {
            failure.expired();
            "PostgreSQL transaction snapshot expired".to_string()
        })?;
    }
    Ok(())
}

pub(super) async fn metadata_count(
    ctx: &Arc<ScanContext>,
    view: Option<&MvccTableView>,
    failure: &ScanFailure,
) -> Result<Option<u64>, String> {
    ensure_active(view, failure)?;
    let state = Mutex::new(CountState::default());
    let tracking = view.is_some() || !ctx.overlay.is_empty();
    let matches = |key: &ferrosa_common::DecoratedKey| {
        if ferrosa_storage::table_tombstone::is_table_tombstone_key(key) {
            return false;
        }
        if !tracking {
            return true;
        }
        let mut state = state.lock().expect("count callback state poisoned");
        if state.error.is_some() {
            return false;
        }
        match state.observe(ctx, view, failure, key) {
            Ok(keep) => keep,
            Err(error) => {
                state.error = Some(error);
                false
            }
        }
    };
    let physical = match &ctx.cluster_read {
        Some(reader) => {
            reader
                .count_matching(
                    &ctx.table_id,
                    read_consistency()?,
                    &ctx.replication,
                    &matches,
                )
                .await?
        }
        None => Some(
            ctx.engine
                .count_live_single_row_range_matching(&ctx.table_id, None, None, &matches)
                .map_err(|error| error.to_string())?,
        ),
    };
    let Some(physical) = physical else {
        return Ok(None);
    };
    let mut state = state
        .into_inner()
        .map_err(|_| "count callback state poisoned")?;
    if let Some(error) = state.error {
        return Err(error);
    }
    // Keys absent from the physical pass may still have visible before-images
    // or staged inserts. Keys visited earlier must never be counted twice,
    // even if their first before-image was registered after that visit.
    if let Some(view) = view {
        let rows = view.rows().map_err(|_| {
            failure.expired();
            "PostgreSQL transaction snapshot expired"
        })?;
        for image in rows {
            let (key, row) = image.map_err(|_| {
                failure.expired();
                "PostgreSQL transaction snapshot expired"
            })?;
            if !state.seen.contains(key.as_ref())
                && !ctx.overlay.contains_key(key.as_ref())
                && row.is_some()
            {
                state.replacements += 1;
            }
        }
    }
    for (key, row) in ctx.overlay.iter() {
        if !state.seen.contains(key) && row.is_some() {
            state.replacements += 1;
        }
    }
    ensure_active(view, failure)?;
    physical
        .checked_add(state.replacements)
        .map(Some)
        .ok_or_else(|| "COUNT exceeds unsigned range".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_read::{PartitionStream, RangeRead};
    use crate::mvcc::{MvccManager, RowChange};
    use ferrosa_cluster::consistency::ConsistencyLevel;
    use ferrosa_cluster::ring::strategy::ReplicationStrategy;
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::{CqlType, DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row as StoredRow};
    use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn stored(deleted: bool) -> StoredRow {
        StoredRow {
            clustering: vec![],
            cells: if deleted {
                vec![]
            } else {
                vec![(0, CellValue::live(b"v".to_vec(), 1000))]
            },
            deletion: if deleted {
                DeletionTime::new(3000, 3)
            } else {
                DeletionTime::LIVE
            },
            primary_key_liveness: if deleted {
                LivenessInfo::NONE
            } else {
                LivenessInfo::with_timestamp(1000)
            },
        }
    }
    fn key(name: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(name.as_bytes().to_vec()))
    }
    fn logical(name: &str) -> super::super::Row {
        super::super::Row(vec![Value::Text(name.into()), Value::Text("v".into())])
    }
    fn fixture() -> (tempfile::TempDir, Arc<ScanContext>) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap(),
        );
        let table = TableId::new("count_snapshot", "rows");
        engine
            .register_table(TableSchema {
                keyspace: table.keyspace.clone(),
                table: table.table.clone(),
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "v".into(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                }],
                extensions: Default::default(),
            })
            .unwrap();
        for name in ["old", "other"] {
            engine
                .write(&table, &key(name), stored(false), 1000)
                .unwrap();
        }
        let ctx = Arc::new(ScanContext {
            engine,
            table_id: table,
            col_names: vec!["k".into(), "v".into()],
            col_types: vec![CqlType::Varchar, CqlType::Varchar],
            pk_idx: vec![0],
            ck_idx: vec![],
            storage_to_table: vec![1],
            overlay: Default::default(),
            cluster_read: None,
            replication: ReplicationStrategy::Simple {
                replication_factor: 1,
            },
            point: None,
            metadata_countable: true,
        });
        (dir, ctx)
    }
    type Hook = Box<dyn FnOnce() + Send>;
    struct Reader {
        rows: Vec<Partition>,
        mode: u8,
        before: Mutex<Option<Hook>>,
        after: Mutex<Option<Hook>>,
        counts: AtomicUsize,
        scans: AtomicUsize,
    }
    impl Reader {
        fn new(ctx: &ScanContext, mode: u8) -> Self {
            Self {
                rows: vec![ctx
                    .engine
                    .read(&ctx.table_id, &key("old"))
                    .unwrap()
                    .unwrap()],
                mode,
                before: Mutex::new(None),
                after: Mutex::new(None),
                counts: AtomicUsize::new(0),
                scans: AtomicUsize::new(0),
            }
        }
    }
    #[async_trait::async_trait]
    impl RangeRead for Reader {
        async fn count_matching(
            &self,
            _table: &TableId,
            cl: ConsistencyLevel,
            strategy: &ReplicationStrategy,
            matches: &(dyn for<'key> Fn(&'key DecoratedKey) -> bool + Sync),
        ) -> Result<Option<u64>, String> {
            self.counts.fetch_add(1, Ordering::Relaxed);
            assert_eq!(cl, read_consistency().unwrap());
            assert_eq!(strategy.replication_factor(), 1);
            if self.mode == 1 {
                return Ok(None);
            }
            if self.mode == 2 {
                return Err("count quorum unavailable".into());
            }
            if let Some(hook) = self.before.lock().unwrap().take() {
                hook();
            }
            let n = self
                .rows
                .iter()
                .filter(|p| matches(&p.key))
                .map(|p| p.rows.len() as u64)
                .sum();
            if let Some(hook) = self.after.lock().unwrap().take() {
                hook();
            }
            Ok(Some(n))
        }
        async fn stream_all(
            &self,
            _table: &TableId,
            _cl: ConsistencyLevel,
            _strategy: &ReplicationStrategy,
        ) -> Result<PartitionStream, String> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            Ok(Box::pin(futures::stream::iter(
                self.rows.clone().into_iter().map(Ok),
            )))
        }
    }

    #[tokio::test]
    async fn metadata_count_own_overlay_replaces_deletes_and_adds_once() {
        let (_dir, mut ctx) = fixture();
        Arc::get_mut(&mut ctx).unwrap().overlay = Arc::new(
            [
                (vec![Value::Text("old".into())], None),
                (vec![Value::Text("new".into())], Some(logical("new"))),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(
            metadata_count(&ctx, None, &ScanFailure::default())
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn metadata_count_snapshot_delete_before_or_after_visit_is_counted_once() {
        for before in [true, false] {
            let (_dir, mut ctx) = fixture();
            let manager = Arc::new(MvccManager::default());
            let snapshot = manager.snapshot();
            let id = uuid::Uuid::new_v4();
            let view = manager.table_view(&snapshot, "count_snapshot.rows", id);
            let reader = Arc::new(Reader::new(&ctx, 0));
            let engine = ctx.engine.clone();
            let table = ctx.table_id.clone();
            let writer = manager.clone();
            let hook: Hook = Box::new(move || {
                writer
                    .commit(
                        &writer.snapshot(),
                        &HashSet::new(),
                        vec![RowChange {
                            table: "count_snapshot.rows".into(),
                            table_id: id,
                            key: vec![Value::Text("old".into())],
                            partition_key: b"old".to_vec(),
                            before: Some(logical("old")),
                            after: None,
                        }],
                        || engine.write(&table, &key("old"), stored(true), 3000),
                    )
                    .unwrap();
            });
            if before {
                *reader.before.lock().unwrap() = Some(hook);
            } else {
                *reader.after.lock().unwrap() = Some(hook);
            }
            Arc::get_mut(&mut ctx).unwrap().cluster_read = Some(reader.clone());
            assert_eq!(
                metadata_count(&ctx, Some(&view), &ScanFailure::default())
                    .await
                    .unwrap(),
                Some(1),
                "before={before}"
            );
            assert_eq!(reader.scans.load(Ordering::Relaxed), 0);
            // The remote fixture has one row while local storage has two: never
            // substitute the local count for the authoritative cluster response.
            assert_eq!(reader.counts.load(Ordering::Relaxed), 1);
        }
    }

    #[tokio::test]
    async fn metadata_count_expiry_after_physical_pass_refuses_partial_result() {
        let (_dir, mut ctx) = fixture();
        let manager = Arc::new(MvccManager::default());
        let snapshot = manager.snapshot();
        let view = manager.table_view(&snapshot, "count_snapshot.rows", uuid::Uuid::new_v4());
        let reader = Arc::new(Reader::new(&ctx, 0));
        *reader.after.lock().unwrap() = Some(Box::new(move || {
            manager.expire_all_snapshots_for_test();
        }));
        Arc::get_mut(&mut ctx).unwrap().cluster_read = Some(reader.clone());
        let failure = ScanFailure::default();
        assert!(metadata_count(&ctx, Some(&view), &failure).await.is_err());
        assert_eq!(failure.take_with_code().unwrap().0, "40001");
        assert_eq!(reader.scans.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_count_remote_unsupported_falls_back_but_errors_do_not() {
        for mode in [1, 2] {
            let (_dir, mut ctx) = fixture();
            let reader = Arc::new(Reader::new(&ctx, mode));
            Arc::get_mut(&mut ctx).unwrap().cluster_read = Some(reader.clone());
            let table = super::super::StreamingTable {
                schema: super::super::rel_schema_for(&ctx.col_names, &ctx.col_types),
                ctx,
                handle: tokio::runtime::Handle::current(),
                failure: ScanFailure::default(),
                scan_buffer_rows: 4,
                mvcc_view: None,
            };
            let catalog = ferrosa_sql::MapCatalog::new().with_table(
                "count_snapshot",
                "rows",
                Arc::new(table),
            );
            let ferrosa_sql::Statement::Select(stmt) =
                ferrosa_sql::parse_statement("SELECT COUNT(*) FROM rows").unwrap()
            else {
                panic!("SELECT")
            };
            let result = tokio::task::spawn_blocking(move || {
                ferrosa_sql::execute(&stmt, &catalog, "count_snapshot", &[])
            })
            .await
            .unwrap();
            if mode == 1 {
                assert_eq!(
                    result.unwrap().rows,
                    vec![super::super::Row(vec![Value::Int(1)])]
                );
                assert_eq!(reader.scans.load(Ordering::Relaxed), 1);
            } else {
                assert!(
                    matches!(result, Err(ferrosa_sql::ExecError::Source(message)) if message.contains("quorum unavailable"))
                );
                assert_eq!(reader.scans.load(Ordering::Relaxed), 0);
            }
            assert_eq!(reader.counts.load(Ordering::Relaxed), 1);
        }
    }
}
