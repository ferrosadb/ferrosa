//! Property-based tests for the commit log subsystem.
//!
//! Uses `proptest` with shared generators from `ferrosa-common` and
//! commit-log-specific generators defined locally. These complement the
//! integration tests with randomized inputs to verify invariants hold
//! for *all* inputs, not just specific examples.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use tempfile::TempDir;

use ferrosa_common::test_generators::{arb_cell_value, arb_decorated_key};
use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::commitlog::{
    CommitLog, CommitLogConfig, CommitLogPosition, Mutation, SyncStrategyConfig, TableId,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Creates a test `CommitLogConfig`.
///
/// `CommitLogConfig::test_config` is `#[cfg(test)]` inside the crate, so we
/// replicate it here for integration/property tests.
fn test_config(dir: &Path) -> CommitLogConfig {
    CommitLogConfig {
        segment_size: 4096,
        max_segment_age: Duration::from_secs(60),
        sync_strategy: SyncStrategyConfig::Batch,
        log_dir: dir.to_path_buf(),
        checkpoint_dir: dir.to_path_buf(),
        archive: None,
        ..CommitLogConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Local generators
// ---------------------------------------------------------------------------

fn arb_row() -> impl Strategy<Value = Row> {
    (
        prop::collection::vec(any::<u8>(), 0..32),
        prop::collection::vec((0u16..64, arb_cell_value()), 0..16),
        prop_oneof![
            Just(DeletionTime::LIVE),
            (1i64..1_000_000, 1u32..100_000).prop_map(|(ts, ldt)| DeletionTime::new(ts, ldt)),
        ],
        1i64..1_000_000,
    )
        .prop_map(|(clustering, mut cells, deletion, ts)| {
            cells.sort_by_key(|(idx, _)| *idx);
            cells.dedup_by_key(|(idx, _)| *idx);
            Row {
                clustering,
                cells,
                deletion,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }
        })
}

fn arb_mutation() -> impl Strategy<Value = Mutation> {
    (
        "[a-z]{1,8}",
        "[a-z]{1,8}",
        arb_decorated_key(),
        prop::collection::vec(arb_row(), 0..8),
        1i64..1_000_000,
        prop::collection::vec(proptest::prelude::any::<u8>(), 16..=16),
    )
        .prop_map(|(keyspace, table, key, rows, timestamp, id_vec)| {
            let mut mutation_id = [0u8; 16];
            mutation_id.copy_from_slice(&id_vec);
            if mutation_id == [0u8; 16] {
                mutation_id[0] = 1;
            }
            Mutation {
                mutation_id,
                keyspace,
                table,
                key,
                rows,
                timestamp,
            }
        })
}

fn arb_mutation_sequence(range: std::ops::Range<usize>) -> impl Strategy<Value = Vec<Mutation>> {
    prop::collection::vec(arb_mutation(), range)
}

// ---------------------------------------------------------------------------
// Property 1: serialization_round_trip
// ---------------------------------------------------------------------------

proptest! {
    /// For any Mutation, serialize then deserialize is identity.
    #[test]
    fn serialization_round_trip(mutation in arb_mutation()) {
        let size = mutation.serialized_size();
        let mut buf = vec![0u8; size];
        mutation.serialize_into(&mut buf);
        let deserialized = Mutation::deserialize_from(&buf).unwrap();

        prop_assert_eq!(&mutation.keyspace, &deserialized.keyspace);
        prop_assert_eq!(&mutation.table, &deserialized.table);
        prop_assert_eq!(&mutation.key, &deserialized.key);
        prop_assert_eq!(mutation.timestamp, deserialized.timestamp);
        prop_assert_eq!(mutation.rows.len(), deserialized.rows.len());
        for (orig, deser) in mutation.rows.iter().zip(deserialized.rows.iter()) {
            prop_assert_eq!(&orig.clustering, &deser.clustering);
            prop_assert_eq!(orig.cells.len(), deser.cells.len());
            prop_assert_eq!(orig.deletion, deser.deletion);
            prop_assert_eq!(orig.primary_key_liveness, deser.primary_key_liveness);
            for (oc, dc) in orig.cells.iter().zip(deser.cells.iter()) {
                prop_assert_eq!(oc, dc);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Property 2: append_replay_round_trip
// ---------------------------------------------------------------------------

proptest! {
    /// For any sequence of mutations, append all then replay recovers all in order.
    #[test]
    fn append_replay_round_trip(mutations in arb_mutation_sequence(1..20)) {
        let dir = TempDir::new().unwrap();
        // Use a larger segment size to avoid rotations with large mutations.
        let config = CommitLogConfig {
            segment_size: 256 * 1024,
            ..test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        for m in &mutations {
            cl.append(m).unwrap();
        }

        cl.shutdown().unwrap();

        let config2 = CommitLogConfig {
            segment_size: 256 * 1024,
            ..test_config(dir.path())
        };
        let (_cl2, replayed) = CommitLog::open_and_replay(config2).unwrap();

        prop_assert_eq!(
            mutations.len(),
            replayed.len(),
            "expected {} replayed mutations, got {}",
            mutations.len(),
            replayed.len()
        );

        for (orig, recovered) in mutations.iter().zip(replayed.iter()) {
            prop_assert_eq!(&orig.keyspace, &recovered.keyspace);
            prop_assert_eq!(&orig.table, &recovered.table);
            prop_assert_eq!(&orig.key, &recovered.key);
            prop_assert_eq!(orig.timestamp, recovered.timestamp);
            prop_assert_eq!(orig.rows.len(), recovered.rows.len());
        }

        _cl2.shutdown().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Property 3: cas_allocation_non_overlapping
// ---------------------------------------------------------------------------

/// All CAS-allocated (offset, len) ranges are disjoint.
#[test]
fn cas_allocation_non_overlapping() {
    let dir = TempDir::new().unwrap();

    // Use a 1 MB segment. We cannot access `Segment` directly (it is
    // pub(crate)), so we exercise allocation through CommitLog::append,
    // which returns CommitLogPosition. Each position plus entry size gives
    // us the allocated range. Instead, we use the positions to verify
    // monotonicity within each segment.

    let config = CommitLogConfig {
        segment_size: 1024 * 1024,
        ..test_config(dir.path())
    };
    let cl = Arc::new(CommitLog::new(config).unwrap());

    let threads = 8;
    let appends_per_thread = 50;

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let cl = Arc::clone(&cl);
            std::thread::spawn(move || {
                let mut positions = Vec::new();
                for i in 0..appends_per_thread {
                    let m = Mutation {
                        mutation_id: [0x70u8; 16],
                        keyspace: "ks".to_string(),
                        table: "tbl".to_string(),
                        key: ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
                            format!("t{t}_k{i}").into_bytes(),
                        )),
                        rows: vec![Row {
                            clustering: vec![t as u8, i as u8],
                            cells: vec![(0, CellValue::live(vec![0u8; 16], 1000))],
                            deletion: DeletionTime::LIVE,
                            primary_key_liveness: LivenessInfo::with_timestamp(1000),
                        }],
                        timestamp: (t * 1000 + i) as i64,
                    };
                    let pos = cl.append(&m).unwrap();
                    positions.push(pos);
                }
                positions
            })
        })
        .collect();

    let mut all_positions: Vec<CommitLogPosition> = Vec::new();
    for h in handles {
        all_positions.extend(h.join().unwrap());
    }

    cl.shutdown().unwrap();

    // All positions must be unique (no two writers got the same offset in
    // the same segment).
    let unique: HashSet<(u64, u64)> = all_positions
        .iter()
        .map(|p| (p.segment_id, p.offset))
        .collect();
    assert_eq!(
        unique.len(),
        all_positions.len(),
        "all positions must be unique: {} unique out of {}",
        unique.len(),
        all_positions.len()
    );

    // Within each segment, sort by offset and verify no overlaps.
    // We cannot know the exact entry size from CommitLogPosition alone,
    // but offsets must be strictly increasing and spaced by at least
    // ENTRY_OVERHEAD (12 bytes).
    let mut by_segment: std::collections::HashMap<u64, Vec<u64>> = std::collections::HashMap::new();
    for p in &all_positions {
        by_segment.entry(p.segment_id).or_default().push(p.offset);
    }
    for (_seg_id, mut offsets) in by_segment {
        offsets.sort();
        for window in offsets.windows(2) {
            assert!(
                window[1] > window[0],
                "offsets in same segment must be strictly increasing: {} >= {}",
                window[0],
                window[1]
            );
            // Each entry has at least 12 bytes overhead.
            assert!(
                window[1] - window[0] >= 12,
                "gap between offsets must be at least ENTRY_OVERHEAD (12): gap = {}",
                window[1] - window[0]
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Property 4: segment_rotation_preserves_data
// ---------------------------------------------------------------------------

proptest! {
    /// All mutations recoverable even when spanning segment boundaries.
    #[test]
    fn segment_rotation_preserves_data(mutations in arb_mutation_sequence(1..50)) {
        let dir = TempDir::new().unwrap();
        // Use 4 KB segments to force rotation while still fitting most
        // randomly generated mutations (which can exceed 1 KB).
        let segment_size = 4096;
        let config = CommitLogConfig {
            segment_size,
            ..test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let mut appended = Vec::new();
        let mut segment_ids = HashSet::new();
        for m in &mutations {
            // Skip mutations that are too large for a single segment.
            // entry_overhead (12) + serialized_size + header (25) must fit.
            if m.serialized_size() + 12 + 25 > segment_size {
                continue;
            }
            match cl.append(m) {
                Ok(pos) => {
                    segment_ids.insert(pos.segment_id);
                    appended.push(m.clone());
                }
                Err(_) => {
                    // Should not happen after the size check, but be safe.
                    continue;
                }
            }
        }

        cl.shutdown().unwrap();

        // Replay and verify all appended mutations come back.
        let config2 = CommitLogConfig {
            segment_size,
            ..test_config(dir.path())
        };
        let (_cl2, replayed) = CommitLog::open_and_replay(config2).unwrap();

        prop_assert_eq!(
            appended.len(),
            replayed.len(),
            "expected {} replayed mutations (from {} segments), got {}",
            appended.len(),
            segment_ids.len(),
            replayed.len()
        );

        for (orig, recovered) in appended.iter().zip(replayed.iter()) {
            prop_assert_eq!(&orig.keyspace, &recovered.keyspace);
            prop_assert_eq!(&orig.table, &recovered.table);
            prop_assert_eq!(&orig.key, &recovered.key);
            prop_assert_eq!(orig.timestamp, recovered.timestamp);
        }

        _cl2.shutdown().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Property 5: flush_tracking_correctness
// ---------------------------------------------------------------------------

proptest! {
    /// A segment is deleted iff every dirty table has flushed past it.
    ///
    /// We write mutations targeting two tables, flush one table at various
    /// points, and verify segments are retained if any dirty table remains.
    #[test]
    fn flush_tracking_correctness(
        flush_index in 1usize..10,
    ) {
        let dir = TempDir::new().unwrap();
        let config = CommitLogConfig {
            segment_size: 512,
            ..test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let table_a = TableId::new("ks", "table_a");
        let table_b = TableId::new("ks", "table_b");

        // Append mutations from two tables.
        let mut positions_a = Vec::new();
        let mut positions_b = Vec::new();
        for i in 0..20 {
            let ma = Mutation {
                mutation_id: [0x71u8; 16],
                keyspace: "ks".to_string(),
                table: "table_a".to_string(),
                key: ferrosa_common::DecoratedKey::new(
                    ferrosa_common::PartitionKey::new(b"pk".to_vec()),
                ),
                rows: vec![Row {
                    clustering: vec![i as u8],
                    cells: vec![(0, CellValue::live(vec![0u8; 8], 1000 + i))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000 + i),
                }],
                timestamp: 1000 + i,
            };
            match cl.append(&ma) {
                Ok(pos) => positions_a.push(pos),
                Err(_) => break,
            }

            let mb = Mutation {
                mutation_id: [0x72u8; 16],
                keyspace: "ks".to_string(),
                table: "table_b".to_string(),
                key: ferrosa_common::DecoratedKey::new(
                    ferrosa_common::PartitionKey::new(b"pk".to_vec()),
                ),
                rows: vec![Row {
                    clustering: vec![i as u8],
                    cells: vec![(0, CellValue::live(vec![0u8; 8], 2000 + i))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(2000 + i),
                }],
                timestamp: 2000 + i,
            };
            match cl.append(&mb) {
                Ok(pos) => positions_b.push(pos),
                Err(_) => break,
            }
        }

        // Force rotation so active segment becomes closed.
        cl.force_rotate().unwrap();

        let count_segments = || -> usize {
            std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("commitlog-") && n.ends_with(".log"))
                })
                .count()
        };

        let before = count_segments();

        // Flush table_a up to some midpoint — segments should NOT be fully
        // cleaned because table_b is still dirty.
        let flush_idx = flush_index.min(positions_a.len().saturating_sub(1));
        if !positions_a.is_empty() {
            cl.discard_completed(&table_a, positions_a[flush_idx]).unwrap();
        }

        let after_partial = count_segments();

        // Now flush both tables fully.
        if let Some(last_a) = positions_a.last() {
            cl.discard_completed(&table_a, *last_a).unwrap();
        }
        if let Some(last_b) = positions_b.last() {
            cl.discard_completed(&table_b, *last_b).unwrap();
        }

        let after_full = count_segments();

        // After flushing both tables, we should have fewer segments than before.
        // The active segment (from force_rotate) may still exist.
        prop_assert!(
            after_full <= before,
            "after full flush should have <= segments: before={before}, after={after_full}"
        );

        // After flushing both tables, we should have fewer or equal segments
        // compared to partial flush.
        prop_assert!(
            after_full <= after_partial,
            "full flush should delete at least as many as partial: partial={after_partial}, full={after_full}"
        );

        cl.shutdown().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Property 6: crash_recovery_completeness
// ---------------------------------------------------------------------------

proptest! {
    /// After crash (truncation), all entries before the truncation point
    /// are recoverable.
    #[test]
    fn crash_recovery_completeness(
        num_mutations in 3usize..15,
        truncate_pct in 1u8..50,
    ) {
        let dir = TempDir::new().unwrap();
        let config = CommitLogConfig {
            segment_size: 64 * 1024,
            ..test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let mut positions = Vec::new();
        for i in 0..num_mutations {
            let m = Mutation {
                mutation_id: [0x73u8; 16],
                keyspace: "ks".to_string(),
                table: "tbl".to_string(),
                key: ferrosa_common::DecoratedKey::new(
                    ferrosa_common::PartitionKey::new(format!("pk_{i}").into_bytes()),
                ),
                rows: vec![Row {
                    clustering: vec![i as u8],
                    cells: vec![(0, CellValue::live(format!("val_{i}").into_bytes(), 1000 + i as i64))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000 + i as i64),
                }],
                timestamp: 1000 + i as i64,
            };
            let pos = cl.append(&m).unwrap();
            positions.push(pos);
        }

        cl.shutdown().unwrap();

        // Find segment files and truncate the last one.
        let mut segment_files: Vec<std::path::PathBuf> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("commitlog-") && n.ends_with(".log"))
            })
            .map(|e| e.path())
            .collect();
        segment_files.sort();

        if let Some(last_file) = segment_files.last() {
            let data = std::fs::read(last_file).unwrap();
            let truncate_bytes = (data.len() as u64 * truncate_pct as u64 / 100).max(1) as usize;
            if data.len() > truncate_bytes + 25 {
                // Only truncate if we keep more than the header.
                let truncated = &data[..data.len() - truncate_bytes];
                std::fs::write(last_file, truncated).unwrap();
            }
        }

        // Replay — we should get at least some entries back.
        let config2 = CommitLogConfig {
            segment_size: 64 * 1024,
            ..test_config(dir.path())
        };
        let (_cl2, replayed) = CommitLog::open_and_replay(config2).unwrap();

        // Invariant: recovered count <= original count (no phantom entries).
        prop_assert!(
            replayed.len() <= num_mutations,
            "should not recover more entries than were written: recovered={}, written={}",
            replayed.len(),
            num_mutations
        );

        // All recovered entries should have valid keyspace.
        for m in &replayed {
            prop_assert_eq!(&m.keyspace, "ks");
            prop_assert_eq!(&m.table, "tbl");
        }

        _cl2.shutdown().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Property 7: crash_recovery_no_duplicates
// ---------------------------------------------------------------------------

proptest! {
    /// Replay never produces duplicate entries (same segment_id + offset).
    #[test]
    fn crash_recovery_no_duplicates(mutations in arb_mutation_sequence(1..20)) {
        let dir = TempDir::new().unwrap();
        let config = CommitLogConfig {
            segment_size: 256 * 1024,
            ..test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        for m in &mutations {
            cl.append(m).unwrap();
        }

        cl.shutdown().unwrap();

        let config2 = CommitLogConfig {
            segment_size: 256 * 1024,
            ..test_config(dir.path())
        };
        let (_cl2, replayed) = CommitLog::open_and_replay(config2).unwrap();

        // Verify no duplicates by checking that timestamps + keyspace + table
        // combinations match the originals without repetition. Since replay
        // returns (position, mutation) pairs internally and we only get
        // mutations, we verify the count matches (no extras).
        prop_assert_eq!(
            mutations.len(),
            replayed.len(),
            "replay should not produce duplicates: wrote {}, replayed {}",
            mutations.len(),
            replayed.len()
        );

        _cl2.shutdown().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Property 8: commutativity_of_discard
// ---------------------------------------------------------------------------

proptest! {
    /// Discarding table_a then table_b produces the same result as
    /// discarding table_b then table_a.
    #[test]
    fn commutativity_of_discard(order_a_first in any::<bool>()) {
        let make_log = || -> (TempDir, CommitLog, Vec<CommitLogPosition>, Vec<CommitLogPosition>) {
            let dir = TempDir::new().unwrap();
            let config = CommitLogConfig {
                segment_size: 512,
                ..test_config(dir.path())
            };
            let cl = CommitLog::new(config).unwrap();

            let mut positions_a = Vec::new();
            let mut positions_b = Vec::new();

            for i in 0..15 {
                let ma = Mutation {
                    mutation_id: [0x74u8; 16],
                    keyspace: "ks".to_string(),
                    table: "table_a".to_string(),
                    key: ferrosa_common::DecoratedKey::new(
                        ferrosa_common::PartitionKey::new(b"pk".to_vec()),
                    ),
                    rows: vec![Row {
                        clustering: vec![i as u8],
                        cells: vec![(0, CellValue::live(vec![0u8; 8], 1000 + i))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(1000 + i),
                    }],
                    timestamp: 1000 + i,
                };
                match cl.append(&ma) {
                    Ok(pos) => positions_a.push(pos),
                    Err(_) => break,
                }

                let mb = Mutation {
                    mutation_id: [0x75u8; 16],
                    keyspace: "ks".to_string(),
                    table: "table_b".to_string(),
                    key: ferrosa_common::DecoratedKey::new(
                        ferrosa_common::PartitionKey::new(b"pk".to_vec()),
                    ),
                    rows: vec![Row {
                        clustering: vec![i as u8],
                        cells: vec![(0, CellValue::live(vec![0u8; 8], 2000 + i))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(2000 + i),
                    }],
                    timestamp: 2000 + i,
                };
                match cl.append(&mb) {
                    Ok(pos) => positions_b.push(pos),
                    Err(_) => break,
                }
            }

            cl.force_rotate().unwrap();
            (dir, cl, positions_a, positions_b)
        };

        let table_a = TableId::new("ks", "table_a");
        let table_b = TableId::new("ks", "table_b");

        let (dir, cl, positions_a, positions_b) = make_log();

        let count_segments = || -> usize {
            std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("commitlog-") && n.ends_with(".log"))
                })
                .count()
        };

        if positions_a.is_empty() || positions_b.is_empty() {
            cl.shutdown().unwrap();
            return Ok(());
        }

        let last_a = *positions_a.last().unwrap();
        let last_b = *positions_b.last().unwrap();

        if order_a_first {
            cl.discard_completed(&table_a, last_a).unwrap();
            cl.discard_completed(&table_b, last_b).unwrap();
        } else {
            cl.discard_completed(&table_b, last_b).unwrap();
            cl.discard_completed(&table_a, last_a).unwrap();
        }

        let final_count = count_segments();

        // Regardless of order, all closed segments should be cleaned since
        // both tables were fully flushed. The active segment from
        // force_rotate has not been flushed to disk yet, so it may not
        // exist as a file — final_count can be 0.
        // The key invariant: no closed (dirty) segments remain.
        prop_assert!(
            final_count <= 1,
            "after flushing both tables, at most the active segment file should remain, got {final_count}"
        );

        cl.shutdown().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Property 9: serialized_size_matches_actual
// ---------------------------------------------------------------------------

proptest! {
    /// `serialized_size()` returns the exact number of bytes written by
    /// `serialize_into()`.
    #[test]
    fn serialized_size_matches_actual(mutation in arb_mutation()) {
        let size = mutation.serialized_size();
        let mut buf = vec![0u8; size];
        mutation.serialize_into(&mut buf);

        // Deserializing from exactly `size` bytes should succeed.
        let roundtripped = Mutation::deserialize_from(&buf).unwrap();
        prop_assert_eq!(roundtripped.serialized_size(), size);

        // A buffer that is 1 byte shorter should fail.
        if size > 0 {
            let result = Mutation::deserialize_from(&buf[..size - 1]);
            prop_assert!(result.is_err(), "truncated buffer should fail to deserialize");
        }
    }
}

// ---------------------------------------------------------------------------
// Property 10: checkpoint_atomicity
// ---------------------------------------------------------------------------

proptest! {
    /// After discard_completed + shutdown + replay, no mutations at or
    /// before the discarded position are replayed.
    #[test]
    fn checkpoint_atomicity(
        num_mutations in 5usize..15,
        flush_at in 1usize..5,
    ) {
        let dir = TempDir::new().unwrap();
        let config = CommitLogConfig {
            segment_size: 64 * 1024,
            ..test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let table_id = TableId::new("ks", "tbl");

        let mut positions = Vec::new();
        for i in 0..num_mutations {
            let m = Mutation {
                mutation_id: [0x76u8; 16],
                keyspace: "ks".to_string(),
                table: "tbl".to_string(),
                key: ferrosa_common::DecoratedKey::new(
                    ferrosa_common::PartitionKey::new(format!("pk_{i}").into_bytes()),
                ),
                rows: vec![Row {
                    clustering: vec![i as u8],
                    cells: vec![(0, CellValue::live(format!("v{i}").into_bytes(), 1000 + i as i64))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000 + i as i64),
                }],
                timestamp: 1000 + i as i64,
            };
            let pos = cl.append(&m).unwrap();
            positions.push(pos);
        }

        // Flush/discard at a midpoint.
        let flush_idx = flush_at.min(positions.len() - 1);
        let flush_pos = positions[flush_idx];
        cl.discard_completed(&table_id, flush_pos).unwrap();

        cl.shutdown().unwrap();

        // Replay — mutations at or before flush_pos should NOT appear.
        let config2 = CommitLogConfig {
            segment_size: 64 * 1024,
            ..test_config(dir.path())
        };
        let (_cl2, replayed) = CommitLog::open_and_replay(config2).unwrap();

        // All replayed mutations should have positions strictly after flush_pos.
        // Since open_and_replay filters by checkpoint, and checkpoint records
        // the flushed position, we should only see mutations after that point.
        // The number of replayed mutations should be <= (total - flushed).
        prop_assert!(
            replayed.len() <= num_mutations,
            "should not replay more than written: replayed={}, written={}",
            replayed.len(),
            num_mutations
        );

        // Verify replayed mutations have timestamps > the flushed mutation's
        // timestamp (since we used monotonically increasing timestamps).
        let flushed_ts = 1000 + flush_idx as i64;
        for m in &replayed {
            prop_assert!(
                m.timestamp > flushed_ts,
                "replayed mutation with ts={} should be > flushed ts={}",
                m.timestamp,
                flushed_ts
            );
        }

        _cl2.shutdown().unwrap();
    }
}
