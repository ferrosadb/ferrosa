//! The write path into [`TableStore`]: admission, index maintenance and the memtable put.

use super::*;

impl<F: FlushTarget> TableStore<F> {
    /// Write a row into the active memtable and update secondary indexes.
    ///
    /// Loads the current view atomically, then delegates to the memtable's
    /// `put`. After the memtable write, each declared secondary index is
    /// updated by extracting the indexed column value from the row cells.
    ///
    /// The whole write — index postings and memtable put — runs against ONE
    /// loaded view, admitted through that view's memtable gate, and posts
    /// under the catalog and schema bound to that memtable. A flush publishes
    /// the next memtable before it seals this one, and waits for the writers
    /// inside before it snapshots, so a write never posts into one memtable's
    /// indexes and puts its row into another, never lands in a frozen
    /// memtable after its snapshot, and never posts under a catalog its
    /// memtable's flush will not write sidecars for. A writer that meets a
    /// sealed gate reloads the view and writes to the new memtable; no writer
    /// ever waits for a flush.
    ///
    /// Legacy nanosecond timestamps are normalised to microseconds before the
    /// row reaches the indexes or the memtable (t_cf637b6e), so no producer can
    /// put one where a comparison would see it.
    pub fn write(&self, key: &DecoratedKey, mut row: Row) -> Result<()> {
        if self.retired.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(ferrosa_common::Error::InvalidData(
                "write refused: store is retired".into(),
            ));
        }
        normalise_legacy_ns_row(&mut row);
        for _ in 0..MAX_SEALED_MEMTABLE_RETRIES {
            let guard = self.view.load();
            if let Some(_admitted) = guard.indexes.gate.try_enter() {
                // Retirement may close admission after the first check. An
                // earlier writer that already passed this check is counted by
                // the same gate that retire_checked seals and drains.
                if self.retired.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err(ferrosa_common::Error::InvalidData(
                        "write refused: store is retired".into(),
                    ));
                }
                return self.write_admitted(&guard, key, row);
            }
            // Sealed: a flush already published the next memtable.
            std::thread::yield_now();
        }
        tracing::error!(
            attempts = MAX_SEALED_MEMTABLE_RETRIES,
            "store: every memtable this write loaded was already sealed; the write is refused"
        );
        Err(ferrosa_common::Error::InvalidData(format!(
            "write refused: met a sealed memtable {MAX_SEALED_MEMTABLE_RETRIES} times in a row"
        )))
    }

    /// [`Self::write`] once it holds an admission to `guard`'s memtable.
    ///
    /// A whole-value (path-less, live) cell on a non-frozen collection column
    /// must parse as a collection, or the write is refused before anything
    /// sees it: a complex-framed flush or compaction later expands that cell
    /// into elements, and a value that cannot be expanded would fail every
    /// flush of the table. Every write producer (CQL, mixed-version forwards,
    /// hints, batchlog, graph writes, commit-log replay) passes through here.
    /// It is checked against the schema bound to the admitted memtable, the
    /// same schema its flush writes under, so a racing `ALTER` cannot pass a
    /// cell under one schema and flush it under another.
    pub(super) fn write_admitted(
        &self,
        guard: &StoreView,
        key: &DecoratedKey,
        row: Row,
    ) -> Result<()> {
        crate::memtable::validate_legacy_collection_blobs(&row, &guard.indexes.schema)?;
        let catalog = Arc::clone(&guard.indexes.catalog);

        // Secondary index maintenance: extract indexed column values and insert
        // before the memtable put (which consumes the row reference via move).
        if !catalog.indexed_columns.is_empty() {
            for (index_name, col_pos) in &catalog.indexed_columns {
                if let Some(index) = guard.indexes.get(index_name) {
                    insert_scalar_memtable_index_row(
                        index,
                        index_name,
                        *col_pos,
                        catalog.index_type_for(index_name),
                        catalog.index_filter_predicates.get(index_name),
                        key.key.as_bytes(),
                        &row,
                    );
                }
            }
        }

        // Partition-key index maintenance: a partition-key column's value is
        // not a cell either, so it is extracted from the row's composite
        // partition-key bytes at the declared component.
        if !catalog.indexed_partition_key_columns.is_empty() {
            let total = self.partition_key_column_count();
            let components = ferrosa_row_bridge::decode_pk(key, total);
            for (index_name, component) in &catalog.indexed_partition_key_columns {
                let Some(value) = components.get(*component) else {
                    tracing::warn!(
                        index_name,
                        component,
                        total,
                        "store: partition-key index component missing from key bytes"
                    );
                    continue;
                };
                let index_type = catalog.index_type_for(index_name);
                match crate::index::scheduler::encode_index_key(index_type, value) {
                    Ok(Some(index_key)) => {
                        // One posting per PARTITION (t_c5bccc65): every row of
                        // the partition shares this value, so the posting names
                        // the partition and the read streams its rows. A later
                        // write to the same partition posts the same position,
                        // which the index ignores.
                        let row_pos = RowPosition {
                            partition_key: key.key.as_bytes().to_vec(),
                            clustering_key: Vec::new(),
                        };
                        match guard.indexes.get(index_name) {
                            Some(idx) => idx.insert(index_key, row_pos),
                            // Declared but absent from the live map: the write
                            // is not indexed and a later read of this index
                            // would report a short answer as a complete one.
                            None => tracing::error!(
                                index_name,
                                "store: declared partition-key index is missing from the live index \
                                 map; this write is NOT indexed and reads of this index \
                                 will be incomplete"
                            ),
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(
                            index_name,
                            %e,
                            "store: skipping partition-key index entry; key encoding failed"
                        );
                    }
                }
            }
        }

        // Clustering-column index maintenance (t_430c4188): a clustering
        // column's value is not a cell, so it is extracted from the row's
        // composite clustering-key bytes at the declared component.
        if !catalog.indexed_clustering_columns.is_empty() && !row.clustering.is_empty() {
            let total = self.clustering_column_count();
            let components = ferrosa_row_bridge::decode_clustering(&row.clustering, total);
            for (index_name, component) in &catalog.indexed_clustering_columns {
                let Some(value) = components.get(*component) else {
                    tracing::warn!(
                        index_name,
                        component,
                        total,
                        "store: clustering index component missing from row clustering bytes"
                    );
                    continue;
                };
                let index_type = catalog.index_type_for(index_name);
                match crate::index::scheduler::encode_index_key(index_type, value) {
                    Ok(Some(index_key)) => {
                        let row_pos = RowPosition {
                            partition_key: key.key.as_bytes().to_vec(),
                            clustering_key: row.clustering.clone(),
                        };
                        match guard.indexes.get(index_name) {
                            Some(idx) => idx.insert(index_key, row_pos),
                            // Declared but absent from the live map: the write
                            // is not indexed and a later read of this index
                            // would report a short answer as a complete one.
                            None => tracing::error!(
                                index_name,
                                "store: declared clustering index is missing from the live index \
                                 map; this write is NOT indexed and reads of this index \
                                 will be incomplete"
                            ),
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(
                            index_name,
                            %e,
                            "store: skipping clustering memtable index entry; key encoding failed"
                        );
                    }
                }
            }
        }

        // Vector index maintenance: extract vector column values and insert into
        // the in-memory HNSW/brute-force index for the active memtable.
        // Row byte offset is unknown until the SSTable is written; we use 0 as a
        // placeholder. The drain→HNSW build at flush time re-inserts with
        // the final on-disk offset. For now, the memtable search is by position
        // within the memtable (ordering only), not absolute file offset.
        if !catalog.vector_index_configs.is_empty() {
            for cfg in &catalog.vector_index_configs {
                if let Some(cell) = row
                    .cells
                    .iter()
                    .find(|(idx, _)| *idx as usize == cfg.column_position)
                {
                    if let Some(ref value) = cell.1.value {
                        if let Ok(vector) = ferrosa_index::bytes_to_f32s::<Arc<[f32]>>(value) {
                            // Use a sequential position based on current index size
                            // (placeholder offset; not a true file offset).
                            let pos = ferrosa_index::vector::RowPosition::new(
                                guard
                                    .vector_indexes
                                    .get(&cfg.index_name)
                                    .map(|vi| vi.len() as u64)
                                    .unwrap_or(0),
                            );
                            if let Some(vi) = guard.vector_indexes.get(&cfg.index_name) {
                                vi.insert_with_scope(
                                    pos,
                                    vector,
                                    Some(Arc::from(key.key.as_bytes())),
                                );
                            }
                        }
                        // If bytes_to_vec_f32 fails, the cell contains non-vector
                        // data — skip silently (the schema enforces the type).
                    }
                }
            }
        }

        // The caller holds an admission to this memtable's gate, so the flush
        // that freezes it has not snapshotted it yet: the row lands in the
        // memtable its postings were added for, under that memtable's schema.
        guard.active.put(key, row, &guard.indexes.schema)
    }
}
