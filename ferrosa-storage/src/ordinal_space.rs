//! Cell ordinals at the SSTable boundary (t_65661473).
//!
//! Above the boundary, one ordinal space is the source of truth: the FLAT
//! space `TableSchema` defines, statics at `0..static_columns.len()` and
//! regulars after them. CQL, the commit log, the memtable, every storage read
//! and every reader of a read use it, and CQL writes a static cell inside the
//! clustered row it was written with.
//!
//! An SSTable numbers its static columns and its regular columns separately,
//! each from 0 (`SerializationHeader::static_columns` / `regular_columns`), and
//! holds static cells only in the partition's static row. The conversion
//! happens in exactly two places:
//!
//! - writing a memtable to an SSTable: [`flat_into_sstable_space`], in place,
//!   before the flush builds its header;
//! - reading an SSTable: `range_merger::ColumnOrdinalMapping::for_header`
//!   targets the flat space.
//!
//! SSTable-to-SSTable rewrites (compaction, spill runs) stay in SSTable space
//! (`ColumnOrdinalMapping::for_rewrite`).

use ferrosa_common::{Error, Result};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

/// Convert a memtable partition from flat ordinals to SSTable ordinals, in
/// place: static cells found in clustered rows are merged (cell
/// last-write-wins) into the partition's static row, and regular ordinals drop
/// by `static_count`. Nothing is copied.
///
/// Fails, rather than write a cell to the wrong column, when a static-row cell
/// is not a static ordinal or when the table declares no statics but the
/// partition holds a static row with cells.
pub fn flat_into_sstable_space(partition: &mut Partition, static_count: usize) -> Result<()> {
    let key = || String::from_utf8_lossy(partition_key_bytes(partition)).into_owned();
    if let Some(static_row) = &partition.static_row {
        if let Some((idx, _)) = static_row
            .cells
            .iter()
            .find(|(idx, _)| usize::from(*idx) >= static_count)
        {
            return Err(Error::InvalidData(format!(
                "partition key={:?}: static-row cell index {idx} is not one of the table's \
                 {static_count} static column(s)",
                key()
            )));
        }
    }
    if static_count == 0 {
        return Ok(());
    }
    let shift = u16::try_from(static_count).map_err(|_| {
        Error::InvalidData(format!(
            "{static_count} static columns exceed the u16 ordinal space"
        ))
    })?;
    let mut lifted: Vec<(u16, ferrosa_common::CellValue)> = Vec::new();
    for row in &mut partition.rows {
        if !row.cells.iter().any(|(idx, _)| *idx < shift) {
            for (idx, _) in &mut row.cells {
                *idx -= shift;
            }
            continue;
        }
        let cells = std::mem::take(&mut row.cells);
        row.cells.reserve(cells.len());
        for (idx, cell) in cells {
            if idx < shift {
                lifted.push((idx, cell));
            } else {
                row.cells.push((idx - shift, cell));
            }
        }
    }
    if lifted.is_empty() {
        return Ok(());
    }
    let static_row = partition.static_row.get_or_insert_with(|| Row {
        clustering: Vec::new(),
        cells: Vec::new(),
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::NONE,
    });
    for (idx, cell) in lifted {
        merge_cell(&mut static_row.cells, idx, cell);
    }
    Ok(())
}

fn partition_key_bytes(partition: &Partition) -> &[u8] {
    partition.key.key.as_bytes()
}

/// Merge one cell into `cells` (sorted by `(ordinal, path)`) with the storage
/// engine's cell last-write-wins.
fn merge_cell(
    cells: &mut Vec<(u16, ferrosa_common::CellValue)>,
    idx: u16,
    cell: ferrosa_common::CellValue,
) {
    let pos = cells.binary_search_by(|(i, c)| (*i, &c.path).cmp(&(idx, &cell.path)));
    match pos {
        Ok(at) => cells[at].1 = ferrosa_common::reconcile(&cells[at].1, &cell),
        Err(at) => cells.insert(at, (idx, cell)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};

    fn row(ck: u8, cells: Vec<(u16, CellValue)>) -> Row {
        Row {
            clustering: vec![ck],
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        }
    }

    fn partition(static_row: Option<Row>, rows: Vec<Row>) -> Partition {
        Partition {
            key: DecoratedKey::new(PartitionKey::new(b"k".to_vec())),
            deletion: DeletionTime::LIVE,
            static_row,
            rows,
        }
    }

    #[test]
    fn statics_lift_newest_wins_and_regulars_shift() {
        let mut p = partition(
            None,
            vec![
                row(
                    1,
                    vec![
                        (0, CellValue::live(b"old".to_vec(), 10)),
                        (1, CellValue::live(b"a".to_vec(), 10)),
                    ],
                ),
                row(
                    2,
                    vec![
                        (0, CellValue::live(b"new".to_vec(), 20)),
                        (2, CellValue::live(b"b".to_vec(), 20)),
                    ],
                ),
            ],
        );
        flat_into_sstable_space(&mut p, 1).unwrap();
        let s = p.static_row.expect("static cells lifted");
        assert_eq!(s.cells, vec![(0, CellValue::live(b"new".to_vec(), 20))]);
        assert_eq!(
            p.rows[0].cells,
            vec![(0, CellValue::live(b"a".to_vec(), 10))]
        );
        assert_eq!(
            p.rows[1].cells,
            vec![(1, CellValue::live(b"b".to_vec(), 20))]
        );
    }

    #[test]
    fn no_statics_is_untouched() {
        let rows = vec![row(1, vec![(0, CellValue::live(b"a".to_vec(), 1))])];
        let mut p = partition(None, rows.clone());
        flat_into_sstable_space(&mut p, 0).unwrap();
        assert_eq!(p.rows, rows);
        assert!(p.static_row.is_none());
    }

    #[test]
    fn a_static_row_cell_outside_the_statics_is_refused() {
        let s = Row {
            clustering: Vec::new(),
            cells: vec![(1, CellValue::live(b"x".to_vec(), 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        };
        let mut p = partition(Some(s), Vec::new());
        let err = flat_into_sstable_space(&mut p, 1).unwrap_err();
        assert!(err.to_string().contains("static-row cell index 1"), "{err}");
    }
}
