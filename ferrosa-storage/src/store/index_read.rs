//! Secondary-index reads on [`TableStore`]: keyed, in-partition and geo consults, and their quarantine guard.

use super::*;

/// What a row's indexed value is checked against: a query term, encoded the
/// way the index encodes values before comparing, or a key already in the
/// index's key space (a posting's own key).
#[derive(Clone, Copy)]
enum IndexProbe<'a> {
    Term(&'a [u8]),
    Encoded(&'a [u8]),
}

impl<'a> IndexProbe<'a> {
    /// The probe's bytes. Identity-encoded families compare them as they are.
    fn bytes(self) -> &'a [u8] {
        match self {
            Self::Term(bytes) | Self::Encoded(bytes) => bytes,
        }
    }
}

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
        self.index_row_has_key(index_name, IndexProbe::Term(&query.0), key, row)
    }

    /// Whether `row`'s current indexed value encodes to `encoded` — the key
    /// space postings are filed under. [`Self::index_row_matches`] encodes a
    /// query term and asks this; a posting walk asks it with the posting's
    /// own key, which is how a posting that outlived its value (the row was
    /// updated to a value filed under another key) is told from a live one.
    fn index_row_has_key(
        &self,
        index_name: &str,
        probe: IndexProbe<'_>,
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
                Ok(value == probe.bytes())
            }
            IndexType::Phonetic | IndexType::Geo => {
                let encoded_query = match probe {
                    IndexProbe::Encoded(encoded) => std::borrow::Cow::Borrowed(encoded),
                    IndexProbe::Term(term) => {
                        match crate::index::scheduler::encode_index_key(kind, term)
                            .map_err(ferrosa_common::Error::InvalidData)?
                        {
                            Some(encoded) => std::borrow::Cow::Owned(encoded.0),
                            None => return Ok(false),
                        }
                    }
                };
                crate::index::scheduler::encode_index_key(kind, value)
                    .map(|value_key| {
                        value_key.is_some_and(|value_key| value_key.0 == *encoded_query)
                    })
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

    /// Visit the rows of ONE partition that match a secondary-index key
    /// (t_430c4188), in clustering order, strictly after the clustering
    /// `after` when given. Serves the fully keyed CQL shape `WHERE <full
    /// partition key> AND <indexed_col> = ?`.
    ///
    /// Every source's postings are seeked to the partition and merged in row
    /// order (the private `OrderedPostings` merge); the walk ends at the first posting of
    /// another partition. Each row posting is point-read as it is reached and
    /// re-checked against the index (a posting can outlive its value), so
    /// memory is O(posting sources) however many rows match, and the
    /// visitor's [`std::ops::ControlFlow::Break`] stops the walk at once.
    /// A partition-key index files one partition posting `(pk, [])`, which
    /// streams the partition's rows in bounded chunks.
    ///
    /// The index is keyed by value, not by `(partition, value)`, so the seek
    /// is a binary search per source; postings of other partitions are never
    /// walked.
    pub fn read_by_index_in_partition_each_after(
        &self,
        index_name: &str,
        key: &IndexKey,
        partition_key: &[u8],
        after: Option<&[u8]>,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> Result<()> {
        if !self.secondary_index_declared(index_name) {
            return Ok(());
        }
        let guard = self.view.load();
        let decorated = DecoratedKey::new(ferrosa_common::key::PartitionKey::new(
            partition_key.to_vec(),
        ));
        self.refuse_index_read_over_quarantine(
            &guard,
            Some(decorated.token.0),
            "read_by_index_in_partition_each_after",
        )?;
        let Some(lookup_key) = self.encode_index_lookup_key(index_name, key)? else {
            return Ok(());
        };
        let memtables = memtable_posting_lists(&guard, index_name, &lookup_key);
        let partition_wide = self.partition_key_index_def(index_name).is_some();
        // Seek every source straight to the cursor (inclusive; the cursor
        // row itself is dropped below), so a resumed window never re-walks
        // the postings it already delivered. A partition-key index files its
        // one posting at `(pk, [])`, which is where its seek must land.
        let seek = RowPosition {
            partition_key: partition_key.to_vec(),
            clustering_key: match after {
                Some(cursor) if !partition_wide => cursor.to_vec(),
                _ => Vec::new(),
            },
        };
        let sources =
            index_posting_sources(&guard, &memtables, index_name, &lookup_key, Some(&seek));
        let mut postings = OrderedPostings::new(sources)
            .take_while(|position| position.partition_key == partition_key);

        let mut failure = None;
        let mut checked = |candidate: Partition| {
            self.visit_matching_rows(candidate, &mut failure, visitor, |store, dk, row| {
                store.index_row_has_key(index_name, IndexProbe::Term(&key.0), dk, row)
            })
        };
        if partition_wide {
            // A partition-key index names whole partitions: one posting means
            // every row of this partition matches.
            if postings.next().is_some() {
                // `Break` means the visitor (or a failed check) stopped the
                // walk; either way the partition was the whole walk.
                let _stopped: std::ops::ControlFlow<()> =
                    self.stream_partition_rows(partition_key, after, &mut checked)?;
            }
            return failure.map_or(Ok(()), Err);
        }
        let clustered = self.clustering_column_count() > 0;
        for position in postings {
            if clustered && position.clustering_key.is_empty() {
                return Err(ferrosa_common::Error::InvalidData(
                    "empty clustered index posting is unsupported; refusing a partition-wide fallback".into(),
                ));
            }
            if after.is_some_and(|cursor| position.clustering_key <= cursor) {
                continue;
            }
            let flow = self.visit_indexed_row(position, &mut checked)?;
            if flow.is_break() {
                break;
            }
        }
        failure.map_or(Ok(()), Err)
    }

    /// Hand `visitor` the rows of `candidate` that `matches` accepts, as one
    /// partition; nothing when none do. An error from `matches` stops the
    /// walk and is parked in `failure` for the caller to return.
    fn visit_matching_rows(
        &self,
        mut candidate: Partition,
        failure: &mut Option<ferrosa_common::Error>,
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
        mut matches: impl FnMut(&Self, &DecoratedKey, &Row) -> Result<bool>,
    ) -> std::ops::ControlFlow<()> {
        let mut verdicts = Ok(());
        let key = candidate.key.clone();
        candidate
            .rows
            .retain(|row| match (&verdicts, matches(self, &key, row)) {
                (Ok(()), Ok(keep)) => keep,
                (Ok(()), Err(error)) => {
                    verdicts = Err(error);
                    false
                }
                (Err(_), _) => false,
            });
        if let Err(error) = verdicts {
            *failure = Some(error);
            return std::ops::ControlFlow::Break(());
        }
        if candidate.rows.is_empty() {
            return std::ops::ControlFlow::Continue(());
        }
        visitor(candidate)
    }

    /// Visit the rows a geo (cell-id) secondary index files under any of the
    /// inclusive `[start, end]` cell-id `ranges`, each once.
    ///
    /// A geo index keys each point by its own 64-bit cell id, so one cover
    /// range spans many keys and its postings come in `(key, row)` order,
    /// not row order. The walk therefore:
    /// 1. normalizes the ranges (sorted, overlaps merged), so every posting
    ///    falls in at most one and the ranges are walked in key order;
    /// 2. merges each range's sources (the memtables' pinned posting lists
    ///    and every sidecar's entries, both lazy) in `(key, row)` order,
    ///    advancing every source that holds the smallest head together, so a
    ///    posting two sources share is read once;
    /// 3. point-reads the row and keeps it only when its CURRENT value still
    ///    encodes to the posting's cell. A point that moved left its old
    ///    posting behind; that posting names a row now filed under another
    ///    cell, which is either visited through its own posting or lies
    ///    outside the ranges. So each row is visited at most once, with no
    ///    set of the rows seen.
    ///
    /// Memory is O(sources + ranges); the visitor's `Break` stops the walk.
    /// Refinement by exact distance / containment is the caller's: the cover
    /// over-approximates.
    pub fn read_by_index_cell_ranges_each(
        &self,
        index_name: &str,
        ranges: &[(u64, u64)],
        visitor: &mut dyn FnMut(Partition) -> std::ops::ControlFlow<()>,
    ) -> Result<()> {
        if !self.secondary_index_declared(index_name) {
            return Ok(());
        }
        let guard = self.view.load();
        self.refuse_index_read_over_quarantine(&guard, None, "read_by_index_cell_ranges_each")?;
        let memtables = memtable_indexes_named(&guard, index_name);
        let sidecars: Vec<&SidecarReader> = guard
            .sidecar_indexes
            .iter()
            .filter_map(|sidecar| sidecar.get(index_name))
            .collect();
        let mut head = GeoHead::default();
        let mut failure = None;
        for (start, end) in normalize_cell_ranges(ranges) {
            let start = IndexKey(start.to_be_bytes().to_vec());
            let end = IndexKey(end.to_be_bytes().to_vec());
            let mut cursors: Vec<GeoCursor<'_>> =
                memtables
                    .iter()
                    .map(|index| GeoCursor::memtable(index.posting_lists_in_range(&start, &end)))
                    .chain(sidecars.iter().map(|reader| {
                        GeoCursor::sidecar(reader.entries_in_range(&start.0, &end.0))
                    }))
                    .collect();
            while head.take_smallest(&mut cursors) {
                let position = RowPositionRef {
                    partition_key: &head.partition_key,
                    clustering_key: &head.clustering_key,
                };
                let cell = head.key.as_slice();
                let mut checked = |candidate: Partition| {
                    self.visit_matching_rows(candidate, &mut failure, visitor, |store, dk, row| {
                        store.index_row_has_key(index_name, IndexProbe::Encoded(cell), dk, row)
                    })
                };
                let flow =
                    if position.clustering_key.is_empty() && self.clustering_column_count() > 0 {
                        self.stream_partition_rows(position.partition_key, None, &mut checked)?
                    } else {
                        self.visit_indexed_row(position, &mut checked)?
                    };
                if flow.is_break() {
                    return failure.map_or(Ok(()), Err);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

/// Sort `ranges` and merge the ones that overlap or touch, so the result is
/// ascending and disjoint. Memory is O(ranges) — the cover, never the data.
fn normalize_cell_ranges(ranges: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut sorted: Vec<(u64, u64)> = ranges
        .iter()
        .copied()
        .filter(|(start, end)| start <= end)
        .collect();
    sorted.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(sorted.len());
    for (start, end) in sorted {
        match merged.last_mut() {
            Some(last) if start <= last.1.saturating_add(1) => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }
    merged
}

/// One lazy `(key, row)`-ordered source of geo postings within one range.
enum GeoCursor<'a> {
    /// A memtable's keys in the range, each a pinned posting list, and the
    /// position reached in the current one.
    Memtable {
        lists: crate::memtable::index::PostingListRange,
        current: Option<crate::memtable::index::PostingList>,
        at: usize,
    },
    /// A sidecar's entries in the range, borrowed from its mapping.
    Sidecar {
        entries: Box<dyn Iterator<Item = (&'a [u8], RowPositionRef<'a>)> + 'a>,
        current: Option<(&'a [u8], RowPositionRef<'a>)>,
    },
}

impl<'a> GeoCursor<'a> {
    fn memtable(mut lists: crate::memtable::index::PostingListRange) -> Self {
        // A posting list is never empty (an insert is the only way to make
        // one), but skip any that were rather than trust it.
        let current = lists.find(|list| !list.as_slice().is_empty());
        Self::Memtable {
            lists,
            current,
            at: 0,
        }
    }

    fn sidecar(entries: impl Iterator<Item = (&'a [u8], RowPositionRef<'a>)> + 'a) -> Self {
        let mut entries: Box<dyn Iterator<Item = (&'a [u8], RowPositionRef<'a>)> + 'a> =
            Box::new(entries);
        let current = entries.next();
        Self::Sidecar { entries, current }
    }

    /// The cursor's current posting, or `None` once exhausted.
    fn head(&self) -> Option<(&[u8], RowPositionRef<'_>)> {
        match self {
            Self::Memtable { current, at, .. } => current.as_ref().map(|list| {
                (
                    list.key().0.as_slice(),
                    RowPositionRef::from(&list.as_slice()[*at]),
                )
            }),
            Self::Sidecar { current, .. } => *current,
        }
    }

    /// Step past the current posting.
    fn advance(&mut self) {
        match self {
            Self::Memtable { lists, current, at } => {
                let Some(list) = current else {
                    return;
                };
                *at += 1;
                if *at >= list.as_slice().len() {
                    *current = lists.find(|list| !list.as_slice().is_empty());
                    *at = 0;
                }
            }
            Self::Sidecar { entries, current } => {
                let previous = *current;
                *current = entries.next();
                debug_assert!(
                    current.is_none() || *current > previous,
                    "sidecar range out of (key, row) order"
                );
            }
        }
    }
}

/// The smallest posting across a range's cursors, copied into buffers that
/// are reused for every posting (so the walk allocates once, not per row).
#[derive(Default)]
struct GeoHead {
    key: Vec<u8>,
    partition_key: Vec<u8>,
    clustering_key: Vec<u8>,
}

impl GeoHead {
    /// Copy the smallest head among `cursors` into `self` and advance every
    /// cursor that holds it, so a posting present in several sources is
    /// taken once. `false` when every cursor is exhausted. Each call
    /// advances at least one cursor, so the walk is bounded by the postings.
    fn take_smallest(&mut self, cursors: &mut [GeoCursor<'_>]) -> bool {
        let smallest = cursors.iter().filter_map(GeoCursor::head).min();
        let Some((key, position)) = smallest else {
            return false;
        };
        self.key.clear();
        self.key.extend_from_slice(key);
        self.partition_key.clear();
        self.partition_key.extend_from_slice(position.partition_key);
        self.clustering_key.clear();
        self.clustering_key
            .extend_from_slice(position.clustering_key);
        let taken = (
            self.key.as_slice(),
            RowPositionRef {
                partition_key: &self.partition_key,
                clustering_key: &self.clustering_key,
            },
        );
        for cursor in cursors.iter_mut() {
            if cursor.head() == Some(taken) {
                cursor.advance();
            }
        }
        true
    }
}
