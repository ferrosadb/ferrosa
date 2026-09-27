//! T-037: row-body size-then-write invariants.
//!
//! RE1 (SizeCounter == bytes written) and RE4 (out-of-order complex input
//! still serializes correctly) live here. RE2 (golden byte identity) is
//! `oracle_golden_reproduction` / `oracle_file_backed_matches_in_memory` in
//! `tests/oracle.rs` — this packet does not duplicate the golden corpus, it
//! just keeps those tests green. RE3 (zero allocations) is
//! `tests/row_encode_alloc.rs`, which needs its own `#[global_allocator]`
//! and so cannot share a test binary with anything else.

mod support;

use proptest::prelude::*;

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};

use support::generators::{arb_case, SizeProfile};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// RE1: for arbitrary rows (all flag combos, statics, TTLs, deletions,
    /// simple + complex columns, 0-5 clustering columns) the row body's
    /// `SizeCounter` total must equal the bytes actually written.
    /// `encode_row_body`'s `debug_assert_eq!` (`ferrosa-sstable/src/writer.rs`)
    /// enforces this on every row it encodes — `cargo test`'s dev profile
    /// keeps debug assertions on, so driving `add_partition` across the
    /// generator's full input space here fails loudly the moment the two
    /// passes ever disagree, instead of relying on some other test to
    /// happen to exercise the mismatching shape.
    #[test]
    fn row_encode_size_counter_matches_written(case in arb_case(SizeProfile::property())) {
        let header = case.schema.header();
        let mut writer = SSTableWriter::new(case.options.clone(), header);
        for partition in &case.partitions {
            writer
                .add_partition(partition)
                .expect("add_partition must not fail for generated input");
        }
        writer.finish().expect("finish must not fail for generated input");
    }
}

const SET_INT: &str =
    "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)";

fn header_with_complex_set() -> SerializationHeader {
    SerializationHeader {
        min_timestamp: 0,
        min_local_deletion_time: 0,
        min_ttl: 0,
        max_timestamp: i64::MAX,
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".to_string()],
        static_columns: Vec::new(),
        regular_columns: vec![(b"s".to_vec(), SET_INT.to_string())],
        complex_collections: true,
    }
}

/// Writes a single partition with one row carrying exactly `cells` and
/// returns the resulting Data.db bytes.
fn write_single_row(cells: Vec<(u16, CellValue)>) -> Vec<u8> {
    let header = header_with_complex_set();
    let mut writer = SSTableWriter::new(WriteOptions::default(), header);
    let partition = Partition {
        key: DecoratedKey::new(PartitionKey::from(b"k".as_slice())),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: 0i32.to_be_bytes().to_vec(),
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }],
    };
    writer.add_partition(&partition).expect("add_partition");
    writer.finish().expect("finish").data
}

/// RE4: element cells for a complex column that arrive OUT of cell-path
/// order must still produce exactly the same bytes as the same cells
/// already in order. `encode_row_body`'s reusable index scratch
/// (`complex_order_scratch`) sorts every complex-column run unconditionally
/// rather than trusting the input or merely `debug_assert`-ing it — see the
/// evidence trail in `ferrosa-sstable/README.md` "Row encoding" for why a
/// debug-only assertion would not be safe here.
#[test]
fn row_encode_out_of_order_complex_cells_match_sorted_output() {
    let element = |path: i32| {
        (
            0u16,
            CellValue::live(Vec::new(), 1000).with_path(path.to_be_bytes().to_vec()),
        )
    };

    let sorted = vec![element(1), element(2), element(3), element(4)];
    let mut shuffled = sorted.clone();
    shuffled.swap(0, 3);
    shuffled.swap(1, 2);
    assert_ne!(
        sorted
            .iter()
            .map(|(_, c)| c.path.clone())
            .collect::<Vec<_>>(),
        shuffled
            .iter()
            .map(|(_, c)| c.path.clone())
            .collect::<Vec<_>>(),
        "the shuffle must actually change the order, or this test proves nothing"
    );

    let sorted_bytes = write_single_row(sorted);
    let shuffled_bytes = write_single_row(shuffled);

    assert_eq!(
        sorted_bytes, shuffled_bytes,
        "out-of-order complex cell input must still serialize in cell-path order"
    );
}
