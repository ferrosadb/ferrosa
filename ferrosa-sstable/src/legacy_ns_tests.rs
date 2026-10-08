//! SSTables written before t_cf637b6e hold Accord timestamps in NANOSECONDS.
//! Every decode site must hand back microseconds, and the reader's header must
//! bound the normalised cells. One test per decode path, so removing any one
//! normalisation call turns exactly that path's test red.

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, LEGACY_NS_THRESHOLD};

use crate::reader::{SSTableComponents, SSTableReader};
use crate::statistics::SerializationHeader;
use crate::types::{DeletionTime, LivenessInfo, Partition, Row};
use crate::writer::{SSTableWriter, WriteOptions};

/// A wall-clock microsecond instant, and the nanosecond stamp the pre-fix
/// Accord apply wrote for it.
const T0_US: i64 = 1_791_437_153_001_234;
fn ns(micros: i64) -> i64 {
    micros * 1_000 + 789
}

const SET_TYPE: &str =
    "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)";

fn header(min: i64, max: i64, complex: bool) -> SerializationHeader {
    SerializationHeader {
        min_timestamp: min,
        min_local_deletion_time: 1_700_000_000,
        min_ttl: 0,
        max_timestamp: max,
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".to_string()],
        static_columns: vec![],
        regular_columns: vec![(
            b"v".to_vec(),
            if complex {
                SET_TYPE.to_string()
            } else {
                "org.apache.cassandra.db.marshal.UTF8Type".to_string()
            },
        )],
        complex_collections: complex,
    }
}

fn key() -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(b"k".to_vec()))
}

const CK: [u8; 4] = [0, 0, 0, 1];
const LDT: u32 = 1_700_000_100;

/// The legacy partition every simple-column test reads: a partition deletion,
/// one row with liveness, a row deletion and a cell carrying its own stamp,
/// all in nanoseconds.
fn legacy_partition() -> Partition {
    Partition {
        key: key(),
        deletion: DeletionTime::new(ns(T0_US - 10), LDT),
        static_row: None,
        rows: vec![Row {
            clustering: CK.to_vec(),
            cells: vec![(0, CellValue::live(b"lwt".to_vec(), ns(T0_US + 1)))],
            deletion: DeletionTime::new(ns(T0_US - 5), LDT),
            primary_key_liveness: LivenessInfo::with_timestamp(ns(T0_US)),
        }],
    }
}

fn write(header: &SerializationHeader, partitions: &[Partition]) -> crate::writer::SSTableOutput {
    let mut writer = SSTableWriter::new(WriteOptions::default(), header.clone());
    for p in partitions {
        writer.add_partition(p).unwrap();
    }
    writer.finish().unwrap()
}

fn open(output: &crate::writer::SSTableOutput) -> SSTableReader<&[u8]> {
    SSTableReader::open(SSTableComponents {
        data: output.data.as_slice(),
        partitions: output.partitions.as_slice(),
        rows: output.rows.as_slice(),
        filter: output.filter.clone(),
        compression_info: output.compression_info.clone(),
        statistics: output.statistics.clone(),
    })
    .unwrap()
}

fn legacy_reader_output() -> crate::writer::SSTableOutput {
    write(
        &header(ns(T0_US - 5), ns(T0_US + 1), false),
        &[legacy_partition()],
    )
}

/// Assert `row` carries the legacy partition's stamps in microseconds.
fn assert_row_micros(row: &Row, path: &str) {
    assert_eq!(
        row.primary_key_liveness.timestamp, T0_US,
        "{path}: liveness stamp"
    );
    assert_eq!(
        row.deletion.marked_for_delete_at,
        T0_US - 5,
        "{path}: row deletion stamp"
    );
    assert_eq!(
        row.deletion.local_deletion_time, LDT,
        "{path}: ldt untouched"
    );
}

#[test]
fn read_partition_normalises_every_stamp() {
    let output = legacy_reader_output();
    let reader = open(&output);
    let p = reader.get_partition(&key()).unwrap().expect("partition");
    assert_eq!(
        p.deletion.marked_for_delete_at,
        T0_US - 10,
        "partition deletion stamp"
    );
    assert_row_micros(&p.rows[0], "read_row");
    assert_eq!(p.rows[0].cells[0].1.timestamp, T0_US + 1, "cell stamp");
}

#[test]
fn read_partition_metadata_normalises_row_stamps() {
    let output = legacy_reader_output();
    let reader = open(&output);
    let mut iter = reader.partitions_iter().unwrap();
    let p = iter.next_partition_metadata().unwrap().expect("partition");
    assert_row_micros(&p.rows[0], "read_row_metadata");
}

#[test]
fn read_partition_projected_normalises_row_and_cell_stamps() {
    let output = legacy_reader_output();
    let reader = open(&output);
    let mut iter = reader.partitions_iter().unwrap();
    let p = iter
        .next_partition_projected(&[0])
        .unwrap()
        .expect("partition");
    assert_row_micros(&p.rows[0], "read_row_projected");
    assert_eq!(p.rows[0].cells[0].1.timestamp, T0_US + 1, "projected cell");
}

/// A complex (collection) column's deletion is delta-encoded separately.
#[test]
fn a_complex_column_deletion_is_normalised() {
    let row = Row {
        clustering: CK.to_vec(),
        cells: vec![
            (0, CellValue::tombstone(ns(T0_US - 3), LDT as i32)),
            (
                0,
                CellValue {
                    path: Some(7i32.to_be_bytes().to_vec()),
                    ..CellValue::live(Vec::new(), ns(T0_US))
                },
            ),
        ],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ns(T0_US)),
    };
    let output = write(
        &header(ns(T0_US - 3), ns(T0_US), true),
        &[Partition {
            key: key(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row],
        }],
    );
    let reader = open(&output);
    let p = reader.get_partition(&key()).unwrap().expect("partition");
    let cells = &p.rows[0].cells;
    let deletion = cells
        .iter()
        .find(|(_, c)| c.path.is_none() && c.value.is_none())
        .expect("the collection tombstone survives the round trip");
    assert_eq!(deletion.1.timestamp, T0_US - 3, "complex deletion stamp");
    let element = cells.iter().find(|(_, c)| c.path.is_some()).unwrap();
    assert_eq!(element.1.timestamp, T0_US, "element stamp");
}

#[test]
fn the_reader_counts_the_legacy_stamps_it_holds() {
    let output = legacy_reader_output();
    // Partition deletion, liveness, row deletion, cell.
    assert_eq!(open(&output).count_legacy_ns_timestamps().unwrap(), 4);
    let micros = write(
        &header(T0_US, T0_US, false),
        &[Partition {
            key: key(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: CK.to_vec(),
                cells: vec![(0, CellValue::live(b"v".to_vec(), T0_US))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(T0_US),
            }],
        }],
    );
    assert_eq!(open(&micros).count_legacy_ns_timestamps().unwrap(), 0);
}

// ---------------------------------------------------------------------------
// Header bounds.
// ---------------------------------------------------------------------------

#[test]
fn a_nanosecond_only_file_reports_exact_microsecond_bounds() {
    let output = legacy_reader_output();
    let reader = open(&output);
    assert_eq!(reader.stored_header().min_timestamp, ns(T0_US - 5));
    assert_eq!(reader.header().min_timestamp, T0_US - 5);
    assert_eq!(reader.header().max_timestamp, T0_US + 1);
    assert!(reader.may_hold_legacy_ns_timestamps());
}

/// A mixed file: a legacy cell older in real time than a micros cell. The
/// stored minimum is the micros cell; the normalised legacy cell sits below
/// it, so the reader's bounds must widen to cover it.
#[test]
fn a_mixed_file_reports_bounds_covering_every_normalised_cell() {
    let row = |ck: u8, ts: i64| Row {
        clustering: vec![0, 0, 0, ck],
        cells: vec![(0, CellValue::live(b"v".to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    };
    let output = write(
        &header(T0_US + 1_000_000, ns(T0_US), false),
        &[Partition {
            key: key(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row(1, ns(T0_US)), row(2, T0_US + 1_000_000)],
        }],
    );
    let reader = open(&output);
    let p = reader.get_partition(&key()).unwrap().unwrap();
    let min_cell = p
        .rows
        .iter()
        .flat_map(|r| r.cells.iter().map(|(_, c)| c.timestamp))
        .min()
        .unwrap();
    assert_eq!(min_cell, T0_US);
    assert!(
        reader.header().min_timestamp <= min_cell,
        "header min {} must bound the normalised cell {min_cell}",
        reader.header().min_timestamp
    );
    assert!(reader.header().max_timestamp >= T0_US + 1_000_000);
    assert!(reader.stored_header().min_timestamp < LEGACY_NS_THRESHOLD);
}

#[test]
fn a_microsecond_file_reports_its_stored_bounds() {
    let output = write(
        &header(T0_US, T0_US + 5, false),
        &[Partition {
            key: key(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: CK.to_vec(),
                cells: vec![(0, CellValue::live(b"v".to_vec(), T0_US + 5))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(T0_US),
            }],
        }],
    );
    let reader = open(&output);
    assert_eq!(reader.header().min_timestamp, T0_US);
    assert_eq!(reader.header().max_timestamp, T0_US + 5);
    assert!(!reader.may_hold_legacy_ns_timestamps());
}

// ---------------------------------------------------------------------------
// Test 14: sentinels round-trip unchanged.
// ---------------------------------------------------------------------------

#[test]
fn sentinels_round_trip_unchanged() {
    let output = write(
        &header(T0_US, T0_US, false),
        &[Partition {
            key: key(),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: CK.to_vec(),
                cells: vec![(0, CellValue::live(b"v".to_vec(), T0_US))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::NONE,
            }],
        }],
    );
    let reader = open(&output);
    let p = reader.get_partition(&key()).unwrap().unwrap();
    assert_eq!(p.deletion, DeletionTime::LIVE);
    assert_eq!(p.rows[0].deletion, DeletionTime::LIVE);
    assert_eq!(p.rows[0].primary_key_liveness, LivenessInfo::NONE);
    assert_eq!(p.rows[0].cells[0].1.timestamp, T0_US);
    assert_eq!(reader.count_legacy_ns_timestamps().unwrap(), 0);
}
