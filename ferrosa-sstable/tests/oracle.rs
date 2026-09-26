//! T-035: writer test oracle.
//!
//! Freezes today's `SSTableWriter` output as a byte-exact oracle, ahead of
//! later packets (T-037, T-038) rewriting how it produces `Data.db`. See
//! `ferrosa-suite/specs/sstable-write-pump/compiled-project-plan.md` (T-035)
//! and `test-specification.md` (Seams, L2 P1) for the design this
//! implements, and `tests/support/legacy_writer.rs` for why this drives the
//! writer's public API rather than a verbatim copy of its private internals.

mod support;

use proptest::prelude::*;

use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};

use support::generators::{arb_case, SizeProfile};
use support::golden::{build_case, read_case_from_disk, write_options, CASES};
use support::legacy_writer::{assert_components_identical, legacy_write, legacy_write_file_backed};

/// Golden reproduction: today's writer, driven with each case's recorded
/// seed/options, reproduces the checked-in `tests/golden/<case>/` bytes
/// exactly. This is the actual oracle later packets must keep passing.
#[test]
fn oracle_golden_reproduction() {
    assert!(!CASES.is_empty(), "golden corpus must not be empty");
    for spec in CASES {
        let (header, partitions) = build_case(spec);
        let options = write_options(spec);

        let fresh = if spec.file_backed {
            let dir =
                tempfile::tempdir().unwrap_or_else(|e| panic!("case {}: tempdir: {e}", spec.name));
            let raw_data_path = dir.path().join("raw-data.tmp");
            legacy_write_file_backed(
                &partitions,
                &header,
                options,
                dir.path().join("staging"),
                raw_data_path,
            )
            .unwrap_or_else(|e| panic!("case {}: file-backed write failed: {e}", spec.name))
        } else {
            legacy_write(&partitions, &header, options)
                .unwrap_or_else(|e| panic!("case {}: in-memory write failed: {e}", spec.name))
        };

        let has_compression = spec.compression.is_some();
        let golden = read_case_from_disk(spec.name, has_compression);
        assert_components_identical(&fresh, &golden, &format!("golden case '{}'", spec.name));
    }
}

/// Golden files, read back through the real `SSTableReader`, report the
/// partition count and per-partition row counts that were written.
#[test]
fn oracle_golden_reads_back_through_reader() {
    assert!(!CASES.is_empty(), "golden corpus must not be empty");
    for spec in CASES {
        let (_header, partitions) = build_case(spec);
        let expected_partition_count = partitions.len();
        let expected_row_counts: Vec<usize> = partitions.iter().map(|p| p.rows.len()).collect();

        let has_compression = spec.compression.is_some();
        let disk = read_case_from_disk(spec.name, has_compression);
        let components = SSTableComponents {
            data: disk.data,
            partitions: disk.partitions,
            rows: disk.rows,
            filter: disk.filter,
            compression_info: disk.compression_info,
            statistics: disk.statistics,
        };
        let reader = SSTableReader::open(components)
            .unwrap_or_else(|e| panic!("case {}: reader open failed: {e}", spec.name));

        assert_eq!(
            reader.key_count() as usize,
            expected_partition_count,
            "case {}: partition count mismatch",
            spec.name
        );

        let mut iter = reader
            .partitions_iter()
            .unwrap_or_else(|e| panic!("case {}: partitions_iter failed: {e}", spec.name));
        let mut actual_row_counts = Vec::new();
        while let Some(partition) = iter
            .next_partition()
            .unwrap_or_else(|e| panic!("case {}: next_partition failed: {e}", spec.name))
        {
            actual_row_counts.push(partition.rows.len());
        }
        assert_eq!(
            actual_row_counts, expected_row_counts,
            "case {}: per-partition row counts mismatch",
            spec.name
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// The file-backed writer path (`new_file_backed` + `finish_to_directory`)
    /// and the in-memory path (`new` + `finish`) must produce byte-identical
    /// components for the same input, across the generator's full space:
    /// 0-5 clustering columns, static rows, deletions, TTLs, simple and
    /// complex columns, empty values, and row bodies up to ~256 KiB.
    #[test]
    fn oracle_file_backed_matches_in_memory(case in arb_case(SizeProfile::property())) {
        let header = case.schema.header();

        let in_memory = legacy_write(&case.partitions, &header, case.options.clone())
            .expect("in-memory writer must succeed for generated input");

        let dir = tempfile::tempdir().expect("tempdir");
        let raw_data_path = dir.path().join("raw-data.tmp");
        let file_backed = legacy_write_file_backed(
            &case.partitions,
            &header,
            case.options.clone(),
            dir.path().join("staging"),
            raw_data_path,
        )
        .expect("file-backed writer must succeed for generated input");

        assert_components_identical(&in_memory, &file_backed, "in-memory vs file-backed");
    }
}
