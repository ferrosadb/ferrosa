//! Integration tests for the commit log subsystem.
//!
//! Each test creates a `TempDir`, builds a `CommitLogConfig`, and exercises
//! the full lifecycle: append mutations, shutdown, replay, and verify recovery.

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use ferrosa_storage::commitlog::{
    CommitLog, CommitLogConfig, Mutation, SyncStrategyConfig, TableId,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Creates a test `CommitLogConfig` with 4 KB segments and Batch sync.
///
/// `CommitLogConfig::test_config` is `#[cfg(test)]` inside the crate, so it
/// is not visible to integration tests. We replicate it here.
fn test_config(dir: &Path) -> CommitLogConfig {
    CommitLogConfig {
        segment_size: 4096,
        max_segment_age: Duration::from_secs(60),
        sync_strategy: SyncStrategyConfig::Batch,
        log_dir: dir.to_path_buf(),
        checkpoint_dir: dir.to_path_buf(),
        archive: None,
    }
}

/// Creates a simple mutation for testing.
fn make_mutation(ks: &str, table: &str, key: &[u8], value: &[u8], ts: i64) -> Mutation {
    Mutation {
        keyspace: ks.to_string(),
        table: table.to_string(),
        key: DecoratedKey::new(PartitionKey::new(key.to_vec())),
        rows: vec![Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(value.to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }],
        timestamp: ts,
    }
}

/// Counts segment files (matching `commitlog-*.log`) in a directory.
fn count_segment_files(dir: &Path) -> usize {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("commitlog-") && n.ends_with(".log"))
        })
        .count()
}

// ---------------------------------------------------------------------------
// Test 1: append_replay_round_trip
// ---------------------------------------------------------------------------

#[test]
fn append_replay_round_trip() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path());
    let cl = CommitLog::new(config).unwrap();

    // Write 10 mutations.
    for i in 0..10 {
        let m = make_mutation(
            "test_ks",
            "test_table",
            format!("pk_{i}").as_bytes(),
            format!("val_{i}").as_bytes(),
            1000 + i,
        );
        cl.append(&m).unwrap();
    }

    cl.shutdown().unwrap();

    // Replay and verify all 10 come back.
    let config2 = test_config(dir.path());
    let (_cl2, replayed) = CommitLog::open_and_replay(config2).unwrap();

    assert_eq!(
        replayed.len(),
        10,
        "expected 10 replayed mutations, got {}",
        replayed.len()
    );

    // Verify each mutation's keyspace.
    for m in &replayed {
        assert_eq!(m.keyspace, "test_ks");
        assert_eq!(m.table, "test_table");
    }

    _cl2.shutdown().unwrap();
}

// ---------------------------------------------------------------------------
// Test 2: concurrent_appends_no_data_loss
// ---------------------------------------------------------------------------

#[test]
fn concurrent_appends_no_data_loss() {
    let dir = TempDir::new().unwrap();
    // Use a larger segment to avoid too many rotations with 800 mutations.
    let config = CommitLogConfig {
        segment_size: 256 * 1024, // 256 KB
        ..test_config(dir.path())
    };
    let cl = Arc::new(CommitLog::new(config).unwrap());

    let num_threads = 8;
    let mutations_per_thread = 100;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let cl = Arc::clone(&cl);
            thread::spawn(move || {
                for i in 0..mutations_per_thread {
                    let m = make_mutation(
                        "test_ks",
                        "test_table",
                        format!("t{t}_pk_{i}").as_bytes(),
                        format!("t{t}_val_{i}").as_bytes(),
                        (t * 1000 + i) as i64,
                    );
                    cl.append(&m).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    cl.shutdown().unwrap();

    // Replay and verify all 800 mutations are present.
    let config2 = CommitLogConfig {
        segment_size: 256 * 1024,
        ..test_config(dir.path())
    };
    let (_cl2, replayed) = CommitLog::open_and_replay(config2).unwrap();

    assert_eq!(
        replayed.len(),
        num_threads * mutations_per_thread,
        "expected {} replayed mutations, got {}",
        num_threads * mutations_per_thread,
        replayed.len()
    );

    _cl2.shutdown().unwrap();
}

// ---------------------------------------------------------------------------
// Test 3: flush_tracking_cleans_segments
// ---------------------------------------------------------------------------

#[test]
fn flush_tracking_cleans_segments() {
    let dir = TempDir::new().unwrap();
    // Small segments to force rotation.
    let config = CommitLogConfig {
        segment_size: 512,
        ..test_config(dir.path())
    };
    let cl = CommitLog::new(config).unwrap();

    let table_id = TableId::new("test_ks", "test_table");

    // Append mutations until we've rotated a few times.
    let mut last_pos = None;
    for i in 0..20 {
        let m = make_mutation("test_ks", "test_table", b"pk", b"val", 1000 + i);
        match cl.append(&m) {
            Ok(pos) => last_pos = Some(pos),
            Err(_) => break,
        }
    }
    let last_pos = last_pos.expect("should have appended at least one mutation");

    // Force rotation so the active segment becomes closed.
    cl.force_rotate().unwrap();

    let before = count_segment_files(dir.path());
    assert!(
        before >= 2,
        "need at least 2 segment files for this test, got {before}"
    );

    // Discard all mutations for this table past the last position.
    cl.discard_completed(&table_id, last_pos).unwrap();

    let after = count_segment_files(dir.path());
    assert!(
        after < before,
        "discard should have deleted some segments: before={before}, after={after}"
    );

    cl.shutdown().unwrap();
}

// ---------------------------------------------------------------------------
// Test 4: segment_rotation_on_size
// ---------------------------------------------------------------------------

#[test]
fn segment_rotation_on_size() {
    let dir = TempDir::new().unwrap();
    // 4 KB segments — each mutation is ~100+ bytes, so we need many to fill 3.
    let config = test_config(dir.path());
    let cl = CommitLog::new(config).unwrap();

    let mut segment_ids = HashSet::new();
    for i in 0..100 {
        let m = make_mutation("test_ks", "test_table", b"pk", b"val", 1000 + i);
        match cl.append(&m) {
            Ok(pos) => {
                segment_ids.insert(pos.segment_id);
            }
            Err(_) => break,
        }
    }

    assert!(
        segment_ids.len() >= 3,
        "expected at least 3 distinct segments, got {}",
        segment_ids.len()
    );

    // Force a final flush so files are on disk.
    cl.shutdown().unwrap();

    // Verify at least 3 segment files exist (active + closed).
    // Note: closed segments were flushed during rotation. The active segment
    // is flushed on shutdown.
    let file_count = count_segment_files(dir.path());
    assert!(
        file_count >= 3,
        "expected at least 3 segment files, got {file_count}"
    );
}

// ---------------------------------------------------------------------------
// Test 5: crash_mid_entry
// ---------------------------------------------------------------------------

#[test]
fn crash_mid_entry() {
    let dir = TempDir::new().unwrap();
    // Use a larger segment to keep all entries in a single segment for simplicity.
    let config = CommitLogConfig {
        segment_size: 32 * 1024,
        ..test_config(dir.path())
    };
    let cl = CommitLog::new(config).unwrap();

    // Write 5 entries and record positions.
    let mut positions = Vec::new();
    for i in 0..5 {
        let m = make_mutation(
            "test_ks",
            "test_table",
            format!("pk_{i}").as_bytes(),
            format!("val_{i}").as_bytes(),
            1000 + i,
        );
        let pos = cl.append(&m).unwrap();
        positions.push(pos);
    }

    cl.shutdown().unwrap();

    // Find the segment file and truncate it to simulate a crash mid-entry.
    // We truncate to a point that cuts through the last entry, preserving
    // the first 4 complete entries.
    let segment_files: Vec<_> = fs::read_dir(dir.path())
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

    assert!(
        !segment_files.is_empty(),
        "should have at least one segment file"
    );

    for seg_path in &segment_files {
        let data = fs::read(seg_path).unwrap();
        let original_len = data.len();

        // Truncate by removing the last 20 bytes, which should corrupt the
        // last entry (each entry is ~100+ bytes, so 20 bytes is partial).
        if original_len > 20 {
            let truncated = &data[..original_len - 20];
            fs::write(seg_path, truncated).unwrap();
        }
    }

    // Replay: should recover entries before the truncation point.
    let config2 = CommitLogConfig {
        segment_size: 32 * 1024,
        ..test_config(dir.path())
    };
    let (_cl2, replayed) = CommitLog::open_and_replay(config2).unwrap();

    // We wrote 5 entries. Truncating the last ~20 bytes should lose at most
    // the last entry. We should get at least 4 back.
    assert!(
        replayed.len() >= 4,
        "expected at least 4 entries after truncation, got {}",
        replayed.len()
    );
    assert!(
        replayed.len() < 5,
        "expected fewer than 5 entries after truncation, got {}",
        replayed.len()
    );

    _cl2.shutdown().unwrap();
}

// ---------------------------------------------------------------------------
// Test 6: checkpoint_survives_restart
// ---------------------------------------------------------------------------

#[test]
fn checkpoint_survives_restart() {
    let dir = TempDir::new().unwrap();
    let config = CommitLogConfig {
        segment_size: 32 * 1024,
        ..test_config(dir.path())
    };
    let cl = CommitLog::new(config).unwrap();

    // Write 10 mutations across two tables.
    for i in 0..5 {
        let m = make_mutation(
            "test_ks",
            "table_a",
            format!("pk_{i}").as_bytes(),
            b"val",
            1000 + i,
        );
        cl.append(&m).unwrap();
    }
    let mut last_pos_b = None;
    for i in 0..5 {
        let m = make_mutation(
            "test_ks",
            "table_b",
            format!("pk_{i}").as_bytes(),
            b"val",
            2000 + i,
        );
        let pos = cl.append(&m).unwrap();
        last_pos_b = Some(pos);
    }

    // Discard table_b (creates a checkpoint).
    let table_b = TableId::new("test_ks", "table_b");
    cl.discard_completed(&table_b, last_pos_b.unwrap()).unwrap();

    cl.shutdown().unwrap();

    // Create a new CommitLog via open_and_replay.
    let config2 = CommitLogConfig {
        segment_size: 32 * 1024,
        ..test_config(dir.path())
    };
    let (_cl2, replayed) = CommitLog::open_and_replay(config2).unwrap();

    // Only table_a mutations should be replayed (table_b was checkpointed).
    for m in &replayed {
        assert_eq!(
            m.table, "table_a",
            "expected only table_a mutations after checkpoint, got table '{}'",
            m.table
        );
    }
    assert_eq!(
        replayed.len(),
        5,
        "expected 5 table_a mutations, got {}",
        replayed.len()
    );

    _cl2.shutdown().unwrap();
}

// ---------------------------------------------------------------------------
// Test 7: multiple_tables_independent_flush
// ---------------------------------------------------------------------------

#[test]
fn multiple_tables_independent_flush() {
    let dir = TempDir::new().unwrap();
    // Small segments to force rotation.
    let config = CommitLogConfig {
        segment_size: 512,
        ..test_config(dir.path())
    };
    let cl = CommitLog::new(config).unwrap();

    let table_a = TableId::new("ks", "table_a");
    let table_b = TableId::new("ks", "table_b");

    // Append mutations from two different tables.
    let mut last_pos_a = None;
    let mut last_pos_b = None;
    for i in 0..10 {
        let ma = make_mutation("ks", "table_a", b"pk", b"val_a", 1000 + i);
        match cl.append(&ma) {
            Ok(pos) => last_pos_a = Some(pos),
            Err(_) => break,
        }
        let mb = make_mutation("ks", "table_b", b"pk", b"val_b", 2000 + i);
        match cl.append(&mb) {
            Ok(pos) => last_pos_b = Some(pos),
            Err(_) => break,
        }
    }

    // Force rotation so segments are closed.
    cl.force_rotate().unwrap();

    let before = count_segment_files(dir.path());

    // Discard only table_a — segments should stay because table_b is still dirty.
    cl.discard_completed(&table_a, last_pos_a.unwrap()).unwrap();

    let after_a = count_segment_files(dir.path());
    // Segments with table_b data should still exist.
    // (Some segments might only have table_a data and be deleted, but segments
    // with both tables should remain.)

    // Now discard table_b — all remaining closed segments should be deleted.
    cl.discard_completed(&table_b, last_pos_b.unwrap()).unwrap();

    let after_both = count_segment_files(dir.path());

    // After discarding both tables, closed segments should be deleted.
    // Only the new active segment (from force_rotate) should remain or fewer.
    assert!(
        after_both <= after_a,
        "discarding both tables should delete at least as many segments: after_a={after_a}, after_both={after_both}"
    );
    assert!(
        after_both < before,
        "discarding both tables should delete some segments: before={before}, after_both={after_both}"
    );

    cl.shutdown().unwrap();
}

// ---------------------------------------------------------------------------
// Test 8: batch_sync_strategy
// ---------------------------------------------------------------------------

#[test]
fn batch_sync_strategy() {
    let dir = TempDir::new().unwrap();
    let config = test_config(dir.path()); // Uses SyncStrategyConfig::Batch

    let cl = CommitLog::new(config).unwrap();

    let m = make_mutation("test_ks", "test_table", b"pk1", b"value1", 42_000);
    cl.append(&m).unwrap();

    // With Batch sync, data should be fsynced immediately after each write.
    // Verify that the segment file exists and contains data beyond the header.
    let segment_files: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("commitlog-") && n.ends_with(".log"))
        })
        .collect();

    assert!(
        !segment_files.is_empty(),
        "segment file should exist after append with Batch sync"
    );

    let file_size = fs::metadata(segment_files[0].path()).unwrap().len();
    // The header is 17 bytes + 8-byte sync marker = 25 bytes.
    // With one entry, the file should be larger than 25 bytes.
    assert!(
        file_size > 25,
        "segment file should contain data beyond the header, got {file_size} bytes"
    );

    cl.shutdown().unwrap();
}
