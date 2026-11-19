//! Secondary-index reads on [`StorageEngine`]: they resolve the table and delegate to its store.

use super::*;

impl StorageEngine {
    /// Visit a secondary-index result one partition at a time, in row order.
    /// The callback may stop the scan, allowing paging and cancellation to
    /// reach the index reader without first building a `Vec` of all postings
    /// or partitions. See [`Self::read_by_index_each_after`].
    pub fn read_by_index_each(
        &self,
        table_id: &TableId,
        index_name: &str,
        key: &ferrosa_index::IndexKey,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> ferrosa_common::Result<()> {
        self.read_by_index_each_after(table_id, index_name, key, None, visitor)
    }

    /// Whether ANY row of `table_id` carries an `index_name` posting for `key_bytes`.
    ///
    /// A thin, `ferrosa-index`-free convenience over [`Self::read_by_index_each`] for a
    /// caller that holds only the encoded key bytes (the Postgres front end's foreign-key
    /// probe does). It stops at the first match, so the cost is one index point lookup,
    /// never O(result) or a scan. Errors propagate from `read_by_index_each`, which refuses
    /// loudly when the index is undeclared or not current — a probe never reports "no such
    /// parent" for an index it could not actually consult.
    pub fn read_by_index_exists(
        &self,
        table_id: &TableId,
        index_name: &str,
        key_bytes: &[u8],
    ) -> ferrosa_common::Result<bool> {
        let key = ferrosa_index::IndexKey(key_bytes.to_vec());
        let mut found = false;
        self.read_by_index_each(table_id, index_name, &key, &mut |_| {
            found = true;
            std::ops::ControlFlow::Break(())
        })?;
        Ok(found)
    }

    /// Visit every partition a secondary-index result names, by RAW encoded key bytes.
    ///
    /// The multi-partition sibling of [`Self::read_by_index_exists`]: the same
    /// `ferrosa-index`-free convenience for a caller that holds only the encoded value
    /// bytes (a front end that must enumerate the rows an indexed value reaches, not
    /// merely test for one). The visitor runs in row order and may stop early. A missing
    /// or stale index is refused loudly by [`Self::read_by_index_each_after`] — it never
    /// answers "no rows" for an index it could not consult.
    pub fn read_by_index_each_bytes(
        &self,
        table_id: &TableId,
        index_name: &str,
        key_bytes: &[u8],
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> ferrosa_common::Result<()> {
        let key = ferrosa_index::IndexKey(key_bytes.to_vec());
        self.read_by_index_each(table_id, index_name, &key, visitor)
    }

    /// Visit a secondary-index result in row order — `(partition key,
    /// clustering)` — strictly after `after` when given, so a page can resume
    /// where the previous one stopped. O(posting sources) memory; see
    /// `TableStore::read_by_index_each_after`.
    pub fn read_by_index_each_after(
        &self,
        table_id: &TableId,
        index_name: &str,
        key: &ferrosa_index::IndexKey,
        after: Option<&ferrosa_index::RowPosition>,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> ferrosa_common::Result<()> {
        if !self.index_is_current(table_id, index_name) {
            return Err(ferrosa_common::Error::InvalidData(format!(
                "secondary index '{index_name}' on {table_id} is not current; refusing to return \
                 incomplete results while index backfill is pending or failed"
            )));
        }
        let state = self.require_table(table_id)?;
        state.note_foreground_read();
        state
            .store
            .read_by_index_each_after(index_name, key, after, visitor)
    }

    /// Open a bounded asynchronous stream over a secondary-index result.
    /// The index walk runs on a blocking worker and hands partitions through
    /// a bounded channel; the full global result is never materialized.
    pub fn read_by_index_stream(
        self: &Arc<Self>,
        table_id: &TableId,
        index_name: &str,
        key: &ferrosa_index::IndexKey,
    ) -> std::pin::Pin<
        Box<dyn futures::stream::Stream<Item = ferrosa_common::Result<Partition>> + Send>,
    > {
        self.read_by_index_stream_after(table_id, index_name, key, None)
    }

    /// [`Self::read_by_index_stream`] resumed strictly after `after`: the
    /// stream yields rows in row order, so the next page of an index read is
    /// the stream opened after the last row the previous page delivered.
    pub fn read_by_index_stream_after(
        self: &Arc<Self>,
        table_id: &TableId,
        index_name: &str,
        key: &ferrosa_index::IndexKey,
        after: Option<ferrosa_index::RowPosition>,
    ) -> std::pin::Pin<
        Box<dyn futures::stream::Stream<Item = ferrosa_common::Result<Partition>> + Send>,
    > {
        let table_id = table_id.clone();
        let index_name = index_name.to_string();
        let key = key.clone();
        let engine = Arc::clone(self);
        let label = format!("{table_id} index {index_name}");
        spawn_index_stream(label, move |visit| {
            engine.read_by_index_each_after(&table_id, &index_name, &key, after.as_ref(), visit)
        })
    }

    /// Whether `key` names so large a share of `table_id` that a scan is
    /// cheaper than serving it through `index_name`. See
    /// [`TableStore::index_key_is_unselective`]. An unregistered table is
    /// `false`: the index path that follows reports it loudly.
    pub fn index_key_is_unselective(
        &self,
        table_id: &TableId,
        index_name: &str,
        key: &ferrosa_index::IndexKey,
    ) -> ferrosa_common::Result<bool> {
        match self.table_state(table_id) {
            Some(state) => state.store.index_key_is_unselective(index_name, key),
            None => Ok(false),
        }
    }

    /// Evaluate a canonical row against the named local index declaration,
    /// using the same catalog, key encoding and predicate rules as postings.
    pub fn index_row_matches(
        &self,
        table_id: &TableId,
        index_name: &str,
        query: &ferrosa_index::IndexKey,
        key: &DecoratedKey,
        row: &Row,
    ) -> ferrosa_common::Result<bool> {
        self.require_table(table_id)?
            .store
            .index_row_matches(index_name, query, key, row)
    }

    /// Visit the rows of ONE partition matching a secondary-index key
    /// (t_430c4188), in clustering order, strictly after the clustering
    /// `after` when given. Delegates to
    /// [`TableStore::read_by_index_in_partition_each_after`]: O(posting
    /// sources) memory, one point read per row as it is reached, and the
    /// visitor's `Break` stops the walk.
    ///
    /// This fails loud on an unregistered table: the keyed CQL path only
    /// reaches here after schema validation, so a missing table is an internal
    /// inconsistency, not an empty result.
    pub fn read_by_index_in_partition_each_after(
        &self,
        table_id: &TableId,
        index_name: &str,
        key: &ferrosa_index::IndexKey,
        partition_key: &[u8],
        after: Option<&[u8]>,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> ferrosa_common::Result<()> {
        let Some(state) = self.table_state(table_id) else {
            return Err(ferrosa_common::Error::InvalidData(format!(
                "read_by_index_in_partition: table {table_id:?} not registered"
            )));
        };
        state.note_foreground_read();
        state.store.read_by_index_in_partition_each_after(
            index_name,
            key,
            partition_key,
            after,
            visitor,
        )
    }

    /// [`Self::read_by_index_in_partition_each_after`] as a bounded
    /// asynchronous stream: the walk runs on a blocking worker and hands rows
    /// through a bounded channel, so a slow consumer stalls the walk and a
    /// dropped one stops it.
    pub fn read_by_index_in_partition_stream_after(
        self: &Arc<Self>,
        table_id: &TableId,
        index_name: &str,
        key: &ferrosa_index::IndexKey,
        partition_key: &[u8],
        after: Option<Vec<u8>>,
    ) -> std::pin::Pin<
        Box<dyn futures::stream::Stream<Item = ferrosa_common::Result<Partition>> + Send>,
    > {
        let table_id = table_id.clone();
        let index_name = index_name.to_string();
        let key = key.clone();
        let partition_key = partition_key.to_vec();
        let engine = Arc::clone(self);
        let label = format!("{table_id} keyed index {index_name}");
        spawn_index_stream(label, move |visit| {
            engine.read_by_index_in_partition_each_after(
                &table_id,
                &index_name,
                &key,
                &partition_key,
                after.as_deref(),
                visit,
            )
        })
    }

    /// Visit the rows a geo (cell-id) secondary index files under the
    /// inclusive `[start, end]` cell-id `ranges`, each once. Delegates to
    /// [`TableStore::read_by_index_cell_ranges_each`]: lazy over the memtable
    /// index and every sidecar, O(sources + ranges) memory, and the
    /// visitor's `Break` stops the walk. An unregistered table is refused:
    /// the geo CQL path resolves the table first, so answering "no points"
    /// for it would hide an inconsistency.
    pub fn read_by_index_cell_ranges_each(
        &self,
        table_id: &TableId,
        index_name: &str,
        ranges: &[(u64, u64)],
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> ferrosa_common::Result<()> {
        let Some(state) = self.table_state(table_id) else {
            return Err(ferrosa_common::Error::InvalidData(format!(
                "read_by_index_cell_ranges: table {table_id:?} not registered"
            )));
        };
        state.note_foreground_read();
        state
            .store
            .read_by_index_cell_ranges_each(index_name, ranges, visitor)
    }
}
