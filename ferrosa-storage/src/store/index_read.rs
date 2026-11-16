//! Secondary-index reads on [`TableStore`]: keyed, in-partition and geo consults, and their quarantine guard.

use super::*;

impl<F: FlushTarget> TableStore<F> {
    /// Refuse an index consult while a generation overlapping it is
    /// quarantined. `token` narrows the check to one partition's token; `None`
    /// means the consult spans every token, so ANY quarantined generation in
    /// `view` counts.
    ///
    /// An index's postings come from sidecars, which are not the SSTables the
    /// rows live in. A generation whose objects are lost can leave its
    /// sidecar readable (or absent) and the postings pointing at rows the
    /// store can no longer read: the consult then reports zero or fewer rows
    /// as success. Refuse it the way a stale index is refused (FMEA ST-56).
    fn refuse_index_read_over_quarantine(
        &self,
        view: &StoreView,
        token: Option<i64>,
        op: &'static str,
    ) -> Result<()> {
        if self.quarantined_sstables.is_empty() && self.index_unavailable_sstables.is_empty() {
            return Ok(());
        }
        let overlapping = view.sstables.iter().find(|desc| {
            (self.is_sstable_quarantined(&desc.gen)
                || self.index_unavailable_sstables.contains(&desc.gen))
                && token.is_none_or(|t| t >= desc.min_token && t <= desc.max_token)
        });
        let Some(desc) = overlapping else {
            return Ok(());
        };
        tracing::debug!(
            op,
            gen = %desc.gen,
            "index read refused: a quarantined SSTable overlaps it, so its result could be short"
        );
        Err(ferrosa_common::Error::corrupt_sstable(
            desc.gen.clone(),
            desc.min_token,
            desc.max_token,
        ))
    }

    /// Visit every row matching a secondary-index key, in row order. See
    /// [`Self::read_by_index_each_after`].
    pub fn read_by_index_each(
        &self,
        index_name: &str,
        key: &IndexKey,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> Result<()> {
        self.read_by_index_each_after(index_name, key, None, visitor)
    }

    /// Visit the rows matching a secondary-index key in row order —
    /// `(partition key, clustering)` bytes — strictly after `after` when
    /// given. The visitor owns back-pressure: [`std::ops::ControlFlow::Break`]
    /// ends the walk once a page is full, and the caller resumes it with the
    /// last row it kept as `after`.
    ///
    /// Memory is O(posting sources), never O(result): every source (the
    /// active memtable index and each SSTable sidecar) holds its postings in
    /// row order, so one `OrderedPostings` merge yields them in order, and a
    /// row held by two sources is dropped by comparing it with the previous
    /// row rather than by remembering every row seen (t_50c8bc7d). A row
    /// posting is point-read as it is reached; a partition posting (pk, []),
    /// which a partition-key index writes, streams the partition's rows in
    /// bounded chunks instead of one point read per row (t_c5bccc65).
    pub fn read_by_index_each_after(
        &self,
        index_name: &str,
        key: &IndexKey,
        after: Option<&RowPosition>,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> Result<()> {
        // An index this node does not declare cannot answer, and must not
        // answer "nothing". The planner selects indexes from the CQL schema,
        // so a consult arrives here only when schema and engine disagree — a
        // restart that failed to re-register the index, or a DROP racing the
        // read. An empty answer from here is unioned by the coordinator into
        // a short result that reads as the rows not existing (t_50c8bc7d:
        // 101,848 entities read as an empty database). Refusing is still
        // never stale: a dropped index's sidecars are not consulted.
        if !self.secondary_index_declared(index_name) {
            return Err(ferrosa_common::Error::InvalidData(format!(
                "secondary index '{index_name}' is not declared on this node's \
                 table, so it cannot answer; refusing to report its absence as \
                 zero matching rows"
            )));
        }
        let Some(lookup_key) = self.encode_index_lookup_key(index_name, key)? else {
            return Ok(());
        };

        let guard = self.view.load();
        self.refuse_index_read_over_quarantine(&guard, None, "read_by_index_each_after")?;
        let memtables = memtable_posting_lists(&guard, index_name, &lookup_key);
        // Seek to the cursor's PARTITION, inclusive: a partition posting
        // (pk, []) sorts before every row of pk, and a page that stopped
        // inside pk must reopen it. Rows at or before the cursor are dropped
        // as they are reached.
        let from = after.map(|cursor| RowPosition {
            partition_key: cursor.partition_key.clone(),
            clustering_key: Vec::new(),
        });
        let sources =
            index_posting_sources(&guard, &memtables, index_name, &lookup_key, from.as_ref());

        // The partition last streamed whole: a row posting for it (a sidecar
        // written before partition postings) names a row already delivered.
        let mut streamed_partition: Option<&[u8]> = None;
        for position in OrderedPostings::new(sources) {
            if streamed_partition == Some(position.partition_key) {
                continue;
            }
            let resume = after
                .filter(|cursor| cursor.partition_key.as_slice() == position.partition_key)
                .map(|cursor| cursor.clustering_key.as_slice());
            let flow = if position.clustering_key.is_empty() {
                streamed_partition = Some(position.partition_key);
                self.stream_partition_rows(position.partition_key, resume, visitor)?
            } else if resume.is_some_and(|cursor| position.clustering_key <= cursor) {
                continue;
            } else {
                self.visit_indexed_row(position, visitor)?
            };
            if flow.is_break() {
                break;
            }
        }
        Ok(())
    }

    /// Whether an index key names so large a share of the table that one
    /// sequential scan beats a point read per match.
    ///
    /// Serving an index key point-reads every row it names, and each point
    /// read probes every SSTable. A tenant index over a single-tenant table
    /// names the whole table: ferrosa-memory's edge count point-read 193,181
    /// rows through `co_occurs_with`'s tenant index in 87 s (2026-09-29),
    /// where a scan takes well under a second. The postings are counted only
    /// up to the threshold, so a selective key costs a handful of posting
    /// reads, never a walk of the whole index.
    pub fn index_key_is_unselective(&self, index_name: &str, key: &IndexKey) -> Result<bool> {
        if !self.secondary_index_declared(index_name) {
            // The read path refuses an undeclared index loudly; not here.
            return Ok(false);
        }
        let Some(lookup_key) = self.encode_index_lookup_key(index_name, key)? else {
            return Ok(false);
        };
        let guard = self.view.load();
        let partitions = guard
            .sstables
            .iter()
            .map(|sstable| sstable.partition_count)
            .sum::<u64>()
            .saturating_add(guard.active.partition_count() as u64)
            .saturating_add(
                guard
                    .flushing
                    .iter()
                    .map(|sealed| sealed.memtable.partition_count() as u64)
                    .sum::<u64>(),
            );
        let threshold = usize::try_from(partitions / UNSELECTIVE_INDEX_SHARE_DENOMINATOR)
            .unwrap_or(usize::MAX)
            .max(UNSELECTIVE_INDEX_MIN_MATCHES);
        let memtables = memtable_posting_lists(&guard, index_name, &lookup_key);
        let sources = index_posting_sources(&guard, &memtables, index_name, &lookup_key, None);
        Ok(OrderedPostings::new(sources).take(threshold).count() >= threshold)
    }

    /// Point-read the one row a row posting names and hand it to `visitor`.
    /// A posting that outlived its row (deleted since it was indexed) yields
    /// nothing.
    fn visit_indexed_row(
        &self,
        position: RowPositionRef<'_>,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> Result<std::ops::ControlFlow<()>> {
        #[cfg(test)]
        INDEX_POINT_READS.with(|count| count.set(count.get() + 1));
        let decorated = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
            position.partition_key.to_vec(),
        ));
        let row = self.read_clustering_row(&decorated, position.clustering_key)?;
        Ok(match row {
            Some(partition) => visitor(partition),
            None => std::ops::ControlFlow::Continue(()),
        })
    }

    /// Encode a raw query term into the index's native key space. A phonetic
    /// index is keyed by the phonetic *code* of the term (both the memtable
    /// index and the flushed sidecar store codes), so `WHERE name = 'Jon'`
    /// must look up Jon's code, not its bytes; BTree/Hash/Composite are
    /// identity-encoded. `None` for a term that encodes to nothing (e.g.
    /// empty or non-UTF-8 text on a phonetic index): it cannot match any
    /// stored entry.
    fn encode_index_lookup_key(
        &self,
        index_name: &str,
        key: &IndexKey,
    ) -> Result<Option<IndexKey>> {
        let index_type = self.index_type_for(index_name);
        crate::index::scheduler::encode_index_key(index_type, &key.0).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "secondary index '{index_name}' read key encoding failed: {e}"
            ))
        })
    }

    /// Re-evaluate a candidate against the current scalar index declaration.
    /// Used after canonical row reconciliation as well as local posting lookup.
    /// The caller supplies a deletion-reconciled row; no data scan occurs here.
    pub fn index_row_matches(
        &self,
        index_name: &str,
        query: &IndexKey,
        key: &DecoratedKey,
        row: &Row,
    ) -> Result<bool> {
        let catalog = self.catalog();
        let (family, position, kind, predicate) =
            catalog.scalar_definition(index_name).ok_or_else(|| {
                ferrosa_common::Error::InvalidData(format!(
                    "index {index_name} has no scalar row predicate"
                ))
            })?;
        if matches!(kind, IndexType::Vector | IndexType::FullText) {
            return Err(ferrosa_common::Error::InvalidData(format!(
                "{kind} cannot verify scalar index rows"
            )));
        }
        let schema = self.schema.load();
        if !schema.clustering_columns.is_empty() && row.clustering.is_empty() {
            return Err(ferrosa_common::Error::InvalidData(
                "empty clustered index rows are unsupported by the current writer contract".into(),
            ));
        }

        if family == 0 && position < schema.static_columns.len()
            || predicate.is_some_and(|p| {
                p.clauses
                    .iter()
                    .any(|clause| clause.column_position < schema.static_columns.len())
            })
        {
            return Err(ferrosa_common::Error::InvalidData(
                "static-column index predicates require a canonical static-row read".into(),
            ));
        }
        if kind == IndexType::Filtered && predicate.is_none() {
            return Err(ferrosa_common::Error::InvalidData(format!(
                "filtered index {index_name} has no predicate"
            )));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .min(i32::MAX as u64) as i32;
        let cell_value = |position: usize| {
            row.cells
                .iter()
                .find(|(ordinal, _)| usize::from(*ordinal) == position)
                .filter(|(_, cell)| {
                    cell.timestamp >= row.deletion.marked_for_delete_at
                        && ferrosa_row_bridge::cell_is_live(cell, now)
                })
                .and_then(|(_, cell)| cell.value.as_deref())
        };
        if predicate
            .is_some_and(|predicate| !ferrosa_index::evaluate_predicate_row(predicate, cell_value))
        {
            return Ok(false);
        }
        let encode_matches = |value: &[u8]| match kind {
            // These index families use identity encoding. Equality borrows both
            // sides; allocating two full cell copies here adds no information.
            IndexType::BTree | IndexType::Hash | IndexType::Composite | IndexType::Filtered => {
                Ok(value == query.0.as_slice())
            }
            IndexType::Phonetic | IndexType::Geo => {
                let encoded_query = crate::index::scheduler::encode_index_key(kind, &query.0)
                    .map_err(ferrosa_common::Error::InvalidData)?;
                let Some(encoded_query) = encoded_query else {
                    return Ok(false);
                };
                crate::index::scheduler::encode_index_key(kind, value)
                    .map(|encoded| encoded.as_ref() == Some(&encoded_query))
                    .map_err(ferrosa_common::Error::InvalidData)
            }
            IndexType::Vector | IndexType::FullText => Err(ferrosa_common::Error::InvalidData(
                "non-scalar index does not support scalar equality".into(),
            )),
        };
        match family {
            0 => {
                if row
                    .cells
                    .iter()
                    .any(|(ordinal, cell)| usize::from(*ordinal) == position && cell.path.is_some())
                {
                    return Err(ferrosa_common::Error::InvalidData(
                        "complex-cell scalar index predicates are unsupported".into(),
                    ));
                }
                cell_value(position).map_or(Ok(false), encode_matches)
            }
            1 | 2 => {
                let live_key = row.primary_key_liveness.timestamp != i64::MIN
                    && row.primary_key_liveness.timestamp >= row.deletion.marked_for_delete_at
                    && !(row.primary_key_liveness.has_ttl()
                        && ferrosa_row_bridge::ldt_is_expired(
                            row.primary_key_liveness.local_deletion_time,
                            now,
                        ));
                if !live_key
                    && !row
                        .cells
                        .iter()
                        .any(|(ordinal, _)| cell_value(usize::from(*ordinal)).is_some())
                {
                    return Ok(false);
                }
                let components = if family == 1 {
                    ferrosa_row_bridge::decode_clustering(
                        &row.clustering,
                        schema.clustering_columns.len(),
                    )
                } else {
                    ferrosa_row_bridge::decode_pk(key, self.partition_key_column_count())
                };
                let value = components.get(position).ok_or_else(|| {
                    ferrosa_common::Error::InvalidData(format!(
                        "index {index_name} key component {position} is missing"
                    ))
                })?;
                encode_matches(value)
            }
            _ => Err(ferrosa_common::Error::InvalidData(
                "unknown scalar index key family".into(),
            )),
        }
    }

    /// Query by secondary index restricted to ONE partition (t_430c4188):
    /// looks up the index key in the memtable index and all SSTable sidecar
    /// indexes, keeps only the postings whose `RowPosition.partition_key`
    /// equals `partition_key`, and point-reads exactly those rows.
    ///
    /// This serves the fully-keyed CQL shape `WHERE <full partition key> AND
    /// <indexed_col> = ?`: work and memory are O(rows matching the value in
    /// the partition), never O(partition rows). Postings from other partitions
    /// are dropped per batch BEFORE retention, so a value that is hot globally
    /// cannot blow the `INDEX_RESULT_CAP` bound (or memory) for a keyed query
    /// that matches only a few rows in its partition. Postings retained after
    /// keying are still capped by `INDEX_RESULT_CAP` — the same fail-loud
    /// bound as the geo candidate consult, never a silent truncation.
    ///
    /// The secondary index is keyed globally by value (not by `(partition,
    /// value)`), so this consult still walks the per-node postings list for
    /// the value; only the retained set is partition-scoped. Staleness
    /// semantics match the global streaming consult — both read the same
    /// memtable index + sidecar layers.
    pub fn read_by_index_in_partition(
        &self,
        index_name: &str,
        key: &IndexKey,
        partition_key: &[u8],
    ) -> Result<Vec<Partition>> {
        if !self.secondary_index_declared(index_name) {
            return Ok(Vec::new());
        }

        let guard = self.view.load();
        let partition_token = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
            partition_key.to_vec(),
        ))
        .token
        .0;
        self.refuse_index_read_over_quarantine(
            &guard,
            Some(partition_token),
            "read_by_index_in_partition",
        )?;

        // Same per-type key encoding as `read_by_index` (see there for why a
        // phonetic index must be probed by code, not raw bytes).
        let index_type = self.index_type_for(index_name);
        let lookup_key = match crate::index::scheduler::encode_index_key(index_type, &key.0) {
            Ok(Some(encoded)) => encoded,
            Ok(None) => return Ok(Vec::new()),
            Err(e) => {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "secondary index '{index_name}' read key encoding failed: {e}"
                )));
            }
        };
        let query_key = key;
        let key = &lookup_key;

        let mut positions: Vec<RowPosition> = Vec::new();
        let mut append_in_partition = |batch: Vec<RowPosition>| -> Result<()> {
            for pos in batch {
                if pos.partition_key != partition_key {
                    continue;
                }
                if positions.len() >= INDEX_RESULT_CAP {
                    return Err(ferrosa_common::Error::InvalidFormat(format!(
                        "secondary index query exceeded {} row limit; \
                         use ALLOW FILTERING for unbounded scans",
                        INDEX_RESULT_CAP
                    )));
                }
                positions.push(pos);
            }
            Ok(())
        };

        // 1. Memtable indexes: the active memtable's and the flushing one's.
        for idx in memtable_indexes_named(&guard, index_name) {
            append_in_partition(idx.lookup(key))?;
        }

        // 2. SSTable sidecar indexes (same lenient per-sidecar error handling
        // as `read_by_index`: a missing/failed sidecar contributes nothing).
        for sidecar in guard.sidecar_indexes.iter() {
            if let Some(reader) = sidecar.get(index_name) {
                let results = reader.lookup(key).map_err(|e| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "secondary index '{index_name}' read failed: {e}"
                    ))
                })?;
                append_in_partition(results)?;
            }
        }

        // 3. Deduplicate by clustering key (the partition key is fixed).
        let mut seen = std::collections::HashSet::new();
        positions.retain(|p| seen.insert(p.clustering_key.clone()));

        // 4. Point-read exactly the matching rows — never the whole partition.
        let dk = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
            partition_key.to_vec(),
        ));
        let partition_wide = self.partition_key_index_def(index_name).is_some();
        let clustered = self.clustering_column_count() > 0;
        let mut partitions = Vec::new();
        for pos in &positions {
            if clustered && pos.clustering_key.is_empty() && !partition_wide {
                return Err(ferrosa_common::Error::InvalidData(
                    "empty clustered index posting is unsupported; refusing a partition-wide fallback".into(),
                ));
            }
            let read = if partition_wide {
                self.read(&dk)
            } else {
                self.read_clustering_row(&dk, &pos.clustering_key)
            };
            let partition = read?;
            if let Some(mut partition) = partition {
                let mut matching = Vec::with_capacity(partition.rows.len());
                for row in partition.rows.drain(..) {
                    if self.index_row_matches(index_name, query_key, &dk, &row)? {
                        matching.push(row);
                    }
                }
                partition.rows = matching;
                if !partition.rows.is_empty() {
                    partitions.push(partition);
                }
            }
        }

        Ok(partitions)
    }

    /// Query a geo (cell-id) secondary index by a set of `[start, end]` cell-id
    /// ranges, returning the matching base-table partitions.
    ///
    /// Unlike a keyed secondary-index lookup — which does a point lookup on an
    /// exact key — a geo index is keyed by an 8-byte big-endian
    /// space-filling-curve cell id, so a spatial query maps to a small bounded
    /// set of contiguous cell-id ranges (produced by `ferrosa_index::geo::cover_*`).
    /// Each `(start, end)` is an **inclusive** range of `u64` cell ids; this
    /// scans the ordered memtable index and every SSTable sidecar for entries
    /// whose big-endian key falls inside any range, deduplicates by
    /// `(partition_key, clustering_key)`, and fetches the rows.
    ///
    /// The same fail-loud `INDEX_RESULT_CAP` bound as the keyed consult applies:
    /// the candidate set is never silently truncated — exceeding the cap returns
    /// an error suggesting `ALLOW FILTERING`. The geo cover ranges are already
    /// bounded (<= a few thousand cells), so the candidate count is bounded by
    /// the data that actually lives in those cells. Refinement with exact
    /// distance / containment is the caller's responsibility (the cover is an
    /// over-approximation).
    pub fn read_by_index_cell_ranges(
        &self,
        index_name: &str,
        ranges: &[(u64, u64)],
    ) -> Result<Vec<Partition>> {
        if !self.secondary_index_declared(index_name) {
            return Ok(Vec::new());
        }

        let guard = self.view.load();
        self.refuse_index_read_over_quarantine(&guard, None, "read_by_index_cell_ranges")?;

        let mut positions: Vec<RowPosition> = Vec::new();
        let mut append_positions = |batch: Vec<RowPosition>| -> Result<()> {
            if positions.len().saturating_add(batch.len()) > INDEX_RESULT_CAP {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "geo index query exceeded {} row limit; \
                     use ALLOW FILTERING for unbounded scans",
                    INDEX_RESULT_CAP
                )));
            }
            positions.extend(batch);
            Ok(())
        };

        // Encode each (start, end) cell id to its big-endian key bounds once.
        let key_ranges: Vec<(IndexKey, IndexKey)> = ranges
            .iter()
            .map(|(start, end)| {
                (
                    IndexKey(start.to_be_bytes().to_vec()),
                    IndexKey(end.to_be_bytes().to_vec()),
                )
            })
            .collect();

        // 1. Memtable indexes (active and flushing): ordered range scan per range.
        for idx in memtable_indexes_named(&guard, index_name) {
            for (start_key, end_key) in &key_ranges {
                append_positions(idx.range(start_key, end_key))?;
            }
        }

        // 2. SSTable sidecar indexes: ordered range scan per range.
        //
        // A failing range read is REPORTED, not skipped. This used to be
        // `if let Ok(results) = ...` with no else, so an unreadable sidecar —
        // a short read, a truncated file, a decode failure — contributed no
        // positions and the query returned the rows it could find as though
        // they were all of them. A caller cannot tell that from a genuinely
        // smaller result, which is how unreadable data gets reported as absent
        // data.
        for sidecar in guard.sidecar_indexes.iter() {
            if let Some(reader) = sidecar.get(index_name) {
                for (start_key, end_key) in &key_ranges {
                    let results = reader.range(start_key, end_key).map_err(|e| {
                        ferrosa_common::Error::InvalidFormat(format!(
                            "geo index '{index_name}' range read failed: {e}"
                        ))
                    })?;
                    append_positions(results)?;
                }
            }
        }

        // 3. Deduplicate by (partition_key, clustering_key). A point may appear
        //    in overlapping ranges and across memtable + sidecars.
        let mut seen = std::collections::HashSet::new();
        positions.retain(|p| seen.insert((p.partition_key.clone(), p.clustering_key.clone())));

        // 4. Fetch base-table rows, carrying the clustering key so wide clustered
        //    tables do not materialize the whole partition per hit.
        let mut partitions = Vec::new();
        for pos in &positions {
            let dk = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
                pos.partition_key.clone(),
            ));
            let read = if pos.clustering_key.is_empty() {
                self.read(&dk)
            } else {
                self.read_clustering_row(&dk, &pos.clustering_key)
            };
            let partition = read?;
            if let Some(partition) = partition {
                partitions.push(partition);
            }
        }

        Ok(partitions)
    }
}
