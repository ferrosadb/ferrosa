//! Apply a whole [`Partition`] received from another replica.
//!
//! Row streaming (bootstrap, decommission, rebalance), Merkle repair and read
//! repair all move partitions between nodes. A partition is more than its
//! clustered rows: it also has a partition-level deletion and, on a table with
//! static columns, a static row. Writing `partition.rows` alone drops both,
//! and a dropped partition deletion RESURRECTS every older row the receiver
//! still holds (P0-3).
//!
//! The write path speaks rows, so the whole partition is expressed as rows:
//!
//! - the partition deletion as the partition-tombstone marker (empty
//!   clustering, no cells, non-LIVE deletion), which the memtable has always
//!   lifted into [`Partition::deletion`];
//! - the static row as the static-row marker (empty clustering plus static
//!   cells on a clustered table), which the memtable lifts into
//!   [`Partition::static_row`];
//! - the clustered rows unchanged.
//!
//! Because these are ordinary rows they travel through the commit log and
//! through the existing `Mutation` wire format unchanged.

use ferrosa_common::{DecoratedKey, Error, Result};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

use crate::engine::StorageEngine;
use crate::TableId;

/// Newest timestamp anywhere in `row`: liveness, cells, row deletion.
pub fn row_write_timestamp(row: &Row) -> i64 {
    row.cells
        .iter()
        .map(|(_, c)| c.timestamp)
        .chain([
            row.primary_key_liveness.timestamp,
            row.deletion.marked_for_delete_at,
        ])
        .max()
        .unwrap_or(i64::MIN)
}

/// Newest timestamp anywhere in `partition`, including its partition deletion
/// and static row. `i64::MIN` when it holds nothing.
pub fn partition_write_timestamp(partition: &Partition) -> i64 {
    partition
        .rows
        .iter()
        .chain(partition.static_row.as_ref())
        .map(row_write_timestamp)
        .chain([partition.deletion.marked_for_delete_at])
        .max()
        .unwrap_or(i64::MIN)
}

/// The partition-tombstone marker row for `deletion`, or `None` when LIVE.
pub fn deletion_marker(deletion: DeletionTime) -> Option<Row> {
    (deletion != DeletionTime::LIVE).then(|| Row {
        clustering: Vec::new(),
        cells: Vec::new(),
        deletion,
        primary_key_liveness: LivenessInfo::NONE,
    })
}

/// Check that `static_row` can travel as the static-row marker and return it
/// as that marker: a valid static row (empty clustering, LIVE deletion, no
/// liveness) IS its own marker, so no copy is made. `None` when there is no
/// static row or it has no cells.
///
/// Fails rather than drop state it cannot represent: a static row carrying a
/// clustering, a row deletion or a liveness has no row-write form.
pub fn static_row_marker<'a>(
    key: &DecoratedKey,
    static_row: Option<&'a Row>,
) -> Result<Option<&'a Row>> {
    let Some(row) = static_row else {
        return Ok(None);
    };
    if !row.clustering.is_empty()
        || row.deletion != DeletionTime::LIVE
        || row.primary_key_liveness.has_timestamp()
    {
        return Err(Error::InvalidData(format!(
            "partition key={:?}: static row carries clustering {:02x?}, row deletion {:?} or \
             liveness {:?}, which no row write can represent; refusing to drop it",
            String::from_utf8_lossy(key.key.as_bytes()),
            row.clustering,
            row.deletion,
            row.primary_key_liveness
        )));
    }
    Ok((!row.cells.is_empty()).then_some(row))
}

/// The partition-level state of a partition (its deletion and static row) as
/// the at most two marker rows the write path accepts, in apply order. The
/// static row is moved, not copied: it is its own marker
/// ([`static_row_marker`]).
pub fn partition_state_rows(
    key: &DecoratedKey,
    deletion: DeletionTime,
    static_row: Option<Row>,
) -> Result<impl Iterator<Item = Row>> {
    let keep_static = static_row_marker(key, static_row.as_ref())?.is_some();
    let static_marker = static_row.filter(|_| keep_static);
    Ok(deletion_marker(deletion).into_iter().chain(static_marker))
}

/// Every piece of `partition`'s state as rows, in apply order: partition
/// deletion, static row, clustered rows. Consumes the partition; nothing is
/// copied.
pub fn into_partition_rows(partition: Partition) -> Result<impl Iterator<Item = Row>> {
    let Partition {
        key,
        deletion,
        static_row,
        rows,
    } = partition;
    Ok(partition_state_rows(&key, deletion, static_row)?.chain(rows))
}

impl StorageEngine {
    /// Apply a whole partition received from another replica: its partition
    /// deletion, its static row and its clustered rows, each merged with
    /// last-write-wins against what this node already holds.
    ///
    /// Returns the number of rows written. Stops at the first failed write
    /// and returns its error; earlier writes stay applied (they are idempotent
    /// LWW merges, so a retry of the whole partition is safe).
    pub fn apply_partition(&self, table_id: &TableId, partition: Partition) -> Result<usize> {
        let Partition {
            key,
            deletion,
            static_row,
            rows,
        } = partition;
        self.apply_partition_parts(table_id, &key, deletion, static_row, rows)
    }

    /// [`Self::apply_partition`] from a partition's parts, with the clustered
    /// rows pulled from `rows` one at a time, so a decoder can feed them
    /// straight from the wire without building a second row vector.
    pub fn apply_partition_parts(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        deletion: DeletionTime,
        static_row: Option<Row>,
        rows: impl IntoIterator<Item = Row>,
    ) -> Result<usize> {
        if static_row.is_some() {
            let schema = self
                .table_schema(table_id)
                .ok_or_else(|| Error::InvalidFormat(format!("table not registered: {table_id}")))?;
            if schema.clustering_columns.is_empty() {
                return Err(Error::InvalidData(format!(
                    "{table_id}: partition key={:?} has a static row but the table has no \
                     clustering columns, so it cannot hold static columns",
                    String::from_utf8_lossy(key.key.as_bytes())
                )));
            }
        }
        self.apply_partition_rows(
            table_id,
            key,
            partition_state_rows(key, deletion, static_row)?.chain(rows),
        )
    }

    /// Write rows already in partition-state form (from
    /// [`into_partition_rows`] or a read-repair `Mutation`), each at its own
    /// newest timestamp. Returns how many were written.
    pub fn apply_partition_rows(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        rows: impl IntoIterator<Item = Row>,
    ) -> Result<usize> {
        let mut written = 0usize;
        for row in rows {
            let ts = row_write_timestamp(&row);
            self.write(table_id, key, row, ts)?;
            written += 1;
        }
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};

    fn cell_row(ck: Vec<u8>, idx: u16, ts: i64) -> Row {
        Row {
            clustering: ck,
            cells: vec![(idx, CellValue::live(b"v".to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        }
    }

    fn partition(deletion: DeletionTime, static_row: Option<Row>) -> Partition {
        Partition {
            key: DecoratedKey::new(PartitionKey::new(b"k".to_vec())),
            deletion,
            static_row,
            rows: vec![cell_row(vec![0, 0, 0, 1], 1, 30)],
        }
    }

    #[test]
    fn rows_carry_deletion_then_static_then_clustered() {
        let p = partition(DeletionTime::new(10, 99), Some(cell_row(Vec::new(), 0, 20)));
        let rows: Vec<Row> = into_partition_rows(p.clone()).unwrap().collect();
        assert_eq!(rows.len(), 3);
        assert!(rows[0].clustering.is_empty() && rows[0].cells.is_empty());
        assert_eq!(rows[0].deletion, DeletionTime::new(10, 99));
        assert!(
            rows[1].clustering.is_empty(),
            "static marker has empty clustering"
        );
        assert_eq!(rows[1].cells, p.static_row.as_ref().unwrap().cells);
        assert_eq!(rows[2], p.rows[0]);
        assert_eq!(partition_write_timestamp(&p), 30);
    }

    #[test]
    fn live_partition_without_static_is_just_its_rows() {
        let p = partition(DeletionTime::LIVE, None);
        assert_eq!(
            into_partition_rows(p.clone()).unwrap().collect::<Vec<_>>(),
            p.rows
        );
    }

    #[test]
    fn static_row_with_row_deletion_is_refused_not_dropped() {
        let mut s = cell_row(Vec::new(), 0, 20);
        s.deletion = DeletionTime::new(5, 5);
        let Err(err) = into_partition_rows(partition(DeletionTime::LIVE, Some(s))) else {
            panic!("a static row with a row deletion must be refused");
        };
        assert!(err.to_string().contains("static row"), "{err}");
    }

    #[test]
    fn static_row_with_clustering_is_refused_not_dropped() {
        let s = cell_row(vec![1, 2], 0, 20);
        let Err(err) = into_partition_rows(partition(DeletionTime::LIVE, Some(s))) else {
            panic!("a static row with clustering bytes has no marker form and must be refused");
        };
        assert!(err.to_string().contains("static row"), "{err}");
    }
}
