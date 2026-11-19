//! The write path into [`StorageEngine`]: single-row writes, batches, and observer-derived mutations.

use super::*;

impl StorageEngine {
    /// Writes a derived mutation (produced by an observer) to the commit log
    /// and the target table's memtable.
    ///
    /// Mirrors the pattern in `dispatch_sync_observers`. Errors are logged at
    /// ERROR level and the write is skipped; derived mutation failures must
    /// not crash the engine.
    ///
    /// The derived rows are shared with the memtable, never copied: a caller
    /// that also hands the same mutation to async observers (the timeseries
    /// materializer) keeps one allocation per row.
    pub(super) fn apply_derived_mutation(&self, dm: &Mutation) {
        if let Err(e) = self.check_write_admission() {
            tracing::error!(
                %e,
                keyspace = %dm.keyspace,
                table = %dm.table,
                "observer drain: local disk reserve exhausted; derived mutation dropped"
            );
            return;
        }
        if let Err(e) = self.commit_log.append(dm) {
            tracing::error!(
                %e,
                keyspace = %dm.keyspace,
                table = %dm.table,
                "observer drain: commit log append failed for derived mutation"
            );
            return;
        }
        let dtid = TableId::new(&dm.keyspace, &dm.table);
        let tables = self.tables.load();
        if let Some(state) = tables.get(&dtid) {
            for row in &dm.rows {
                if let Err(e) = state.store.write(&dm.key, Arc::clone(row)) {
                    tracing::error!(
                        %e,
                        keyspace = %dm.keyspace,
                        table = %dm.table,
                        "observer drain: memtable write failed for derived mutation"
                    );
                }
            }
        } else {
            tracing::error!(
                keyspace = %dm.keyspace,
                table = %dm.table,
                "observer drain: target table not registered — derived mutation dropped; \
                 register the adjacency table with StorageEngine before registering the observer"
            );
        }
    }

    pub fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Arc<Row>,
        timestamp: i64,
    ) -> ferrosa_common::Result<()> {
        let total_start = Instant::now();
        // Per-write span emitted at DEBUG level so it is free at the
        // default INFO subscriber filter — every span allocation was
        // measurable on a 50k ops/s workload.
        let _span = tracing::debug_span!(
            "storage.write",
            table = %table_id,
        )
        .entered();
        let phase_start = Instant::now();
        if let Err(e) = self.check_write_admission() {
            crate::metrics::observe_write_phase(
                crate::metrics::WritePhase::AdmissionDisk,
                phase_start.elapsed(),
            );
            crate::metrics::observe_write_phase(
                crate::metrics::WritePhase::Total,
                total_start.elapsed(),
            );
            crate::metrics::inc_write_failure_reason(
                crate::metrics::WriteFailureReason::DiskReserve,
            );
            return Err(e);
        }
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::AdmissionDisk,
            phase_start.elapsed(),
        );

        let phase_start = Instant::now();
        {
            let tables = self.tables.load();
            let state = match tables.get(table_id) {
                Some(state) => state,
                None => {
                    crate::metrics::observe_write_phase(
                        crate::metrics::WritePhase::AdmissionMemtable,
                        phase_start.elapsed(),
                    );
                    crate::metrics::observe_write_phase(
                        crate::metrics::WritePhase::Total,
                        total_start.elapsed(),
                    );
                    crate::metrics::inc_write_failure_reason(
                        crate::metrics::WriteFailureReason::TableMissing,
                    );
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "table not registered: {table_id}"
                    )));
                }
            };
            if let Err(e) = self.check_memtable_write_admission(table_id, state) {
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::AdmissionMemtable,
                    phase_start.elapsed(),
                );
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::Total,
                    total_start.elapsed(),
                );
                crate::metrics::inc_write_failure_reason(
                    crate::metrics::WriteFailureReason::MemtableBackpressure,
                );
                return Err(e);
            }
        }
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::AdmissionMemtable,
            phase_start.elapsed(),
        );

        // 1. Append to commit log for durability.
        let phase_start = Instant::now();
        let cl_pos = match self
            .commit_log
            .append_single_row(table_id, key, &row, timestamp)
        {
            Ok(pos) => pos,
            Err(e) => {
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::CommitLogAppend,
                    phase_start.elapsed(),
                );
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::Total,
                    total_start.elapsed(),
                );
                crate::metrics::inc_write_failure_reason(
                    crate::metrics::WriteFailureReason::CommitLogAppend,
                );
                return Err(e);
            }
        };
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::CommitLogAppend,
            phase_start.elapsed(),
        );
        let observer_mutation = if self.has_observers_for_table(table_id) {
            // The observer shares the row the memtable stores (one `Arc`,
            // no copy of its cells).
            Some(Mutation::new(
                table_id.keyspace.clone(),
                table_id.table.clone(),
                key.clone(),
                vec![Arc::clone(&row)],
                timestamp,
            ))
        } else {
            None
        };

        // 2. Write to the table's memtable and track commit log position.
        let phase_start = Instant::now();
        {
            let tables = self.tables.load();
            let state = match tables.get(table_id) {
                Some(state) => state,
                None => {
                    crate::metrics::observe_write_phase(
                        crate::metrics::WritePhase::MemtableWrite,
                        phase_start.elapsed(),
                    );
                    crate::metrics::observe_write_phase(
                        crate::metrics::WritePhase::Total,
                        total_start.elapsed(),
                    );
                    crate::metrics::inc_write_failure_reason(
                        crate::metrics::WriteFailureReason::TableMissing,
                    );
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "table not registered: {table_id}"
                    )));
                }
            };
            if let Err(e) = state.store.write(key, row) {
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::MemtableWrite,
                    phase_start.elapsed(),
                );
                crate::metrics::observe_write_phase(
                    crate::metrics::WritePhase::Total,
                    total_start.elapsed(),
                );
                crate::metrics::inc_write_failure_reason(
                    crate::metrics::WriteFailureReason::MemtableWrite,
                );
                return Err(e);
            }
            state.last_commit_log_position.store(Arc::new(Some(cl_pos)));
            // Set the first-unflushed timestamp via CAS — succeeds at most once
            // per memtable epoch, no contention after the first write.
            if state
                .first_unflushed_write_at_nanos
                .load(std::sync::atomic::Ordering::Relaxed)
                == 0
            {
                let now = now_nanos_since_reference();
                let _ = state.first_unflushed_write_at_nanos.compare_exchange(
                    0,
                    now,
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            self.request_flush_if_needed(state);
        }
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::MemtableWrite,
            phase_start.elapsed(),
        );

        // 3. Notify observers after successful commit log + memtable write.
        let phase_start = Instant::now();
        if let Some(mutation) = observer_mutation {
            self.dispatch_sync_observers(table_id, &mutation);
            self.dispatch_async_observers(table_id, || mutation);
        }
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::Observers,
            phase_start.elapsed(),
        );
        crate::metrics::observe_write_phase(
            crate::metrics::WritePhase::Total,
            total_start.elapsed(),
        );
        crate::metrics::inc_write_total();

        Ok(())
    }

    /// Apply a batch of mixed ops atomically and durably (single fsync group).
    ///
    /// All-or-nothing: every op lowers to a [`Mutation`] and the whole set is
    /// applied through [`Self::write_atomic_batch`], which preflights every
    /// target table **before** appending any commit-log record. On any
    /// preflight / append failure **none** of the ops are applied and an `Err`
    /// is returned (spec URS-QEC-X02, fail-loud per X01). Ops touching the same
    /// `(keyspace, table, key)` are applied in `ops` order.
    ///
    /// An empty batch is `Ok(())`.
    pub fn apply_batch(&self, ops: Vec<BatchOp>) -> ferrosa_common::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mutations: Vec<Mutation> = ops.into_iter().map(BatchOp::into_mutation).collect();
        let record = crate::commitlog::transaction_record::encode(
            &mutations,
            self.commit_log.max_entry_size(),
        )?;
        self.write_atomic_batch_inner(mutations, Some(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::{CellValue, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    /// RED for p0-oom-audit clone-on-row-data at `apply_derived_mutation`:
    /// each derived row used to be deep-copied into the memtable. The derived
    /// mutation now shares its rows with the memtable, so the stored row is
    /// the mutation's own allocation (pointer identity).
    #[test]
    fn apply_derived_mutation_stores_the_derived_row_without_copying_it() {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
        engine
            .register_table(TableSchema {
                keyspace: "derived_ks".to_string(),
                table: "target".to_string(),
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "v".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                }],
                extensions: Default::default(),
            })
            .unwrap();
        let key = DecoratedKey::new(PartitionKey::new(b"k".to_vec()));
        let row = Arc::new(Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"derived".to_vec(), 11))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(11),
        });
        let derived = Mutation::new(
            "derived_ks".to_string(),
            "target".to_string(),
            key.clone(),
            vec![Arc::clone(&row)],
            11,
        );

        engine.apply_derived_mutation(&derived);

        let stored = engine
            .read(&TableId::new("derived_ks", "target"), &key)
            .unwrap()
            .expect("the derived row was applied");
        assert_eq!(stored.rows.len(), 1);
        assert!(
            Arc::ptr_eq(&stored.rows[0], &row),
            "the memtable must hold the derived mutation's row, not a copy of it"
        );
    }
}
