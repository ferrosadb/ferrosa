//! End-to-end integration tests for the secondary index lifecycle.
//!
//! Exercises the full path from [`IndexStateTracker`] registration through
//! [`IndexBuildScheduler`] processing and back to status verification.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrosa_index::IndexType;
use ferrosa_storage::index::{
    BuildPriority, IndexBuildJob, IndexBuildScheduler, IndexStateTracker, IndexStatus,
};

/// Full lifecycle: register -> mark pending -> submit build -> verify current.
#[test]
fn index_build_lifecycle() {
    // 1. Create IndexStateTracker.
    let tracker = Arc::new(IndexStateTracker::new());

    // 2. Create IndexBuildScheduler with 1 worker.
    let scheduler = IndexBuildScheduler::new(1, Arc::clone(&tracker));

    // 3. Register an index.
    tracker.register_index("test_ks", "users", "idx_email");
    let state = tracker.get_state("test_ks", "users", "idx_email").unwrap();
    assert!(
        matches!(state.status, IndexStatus::Current),
        "newly registered index should be Current"
    );

    // 4. Mark an SSTable as pending.
    tracker.mark_pending("test_ks", "users", "idx_email", "sst-001", 2048);

    // 5. Verify status is Stale.
    let state = tracker.get_state("test_ks", "users", "idx_email").unwrap();
    assert!(
        matches!(state.status, IndexStatus::Stale { .. }),
        "index with pending SSTable should be Stale, got {:?}",
        state.status
    );

    // 6. Submit a build job.
    scheduler
        .submit(IndexBuildJob {
            sstable_id: "sst-001".to_string(),
            index_name: "idx_email".to_string(),
            index_type: IndexType::BTree,
            table: ("test_ks".to_string(), "users".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            clustering_source: None,
            filter_predicate: None,
        })
        .expect("submit should succeed");

    // 7. Wait for build to complete (poll tracker).
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let state = tracker.get_state("test_ks", "users", "idx_email").unwrap();
        if matches!(state.status, IndexStatus::Current) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for index to become Current"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // 8. Verify status transitions to Current.
    let state = tracker.get_state("test_ks", "users", "idx_email").unwrap();
    assert!(matches!(state.status, IndexStatus::Current));
    assert!(state.indexed_sstables.contains("sst-001"));
    assert!(state.pending_sstables.is_empty());
    assert_eq!(state.total_builds, 1);

    // 9. Shutdown scheduler.
    scheduler.shutdown();
}

/// Multiple indexes tracked independently — staleness of one does not affect
/// the other.
#[test]
fn multiple_indexes_with_independent_staleness() {
    let tracker = Arc::new(IndexStateTracker::new());
    let scheduler = IndexBuildScheduler::new(2, Arc::clone(&tracker));

    // Register two indexes on different tables.
    tracker.register_index("ks", "tbl_a", "idx_a");
    tracker.register_index("ks", "tbl_b", "idx_b");

    // Mark SSTable pending only for idx_a.
    tracker.mark_pending("ks", "tbl_a", "idx_a", "sst-100", 512);

    let state_a = tracker.get_state("ks", "tbl_a", "idx_a").unwrap();
    let state_b = tracker.get_state("ks", "tbl_b", "idx_b").unwrap();

    assert!(
        matches!(state_a.status, IndexStatus::Stale { .. }),
        "idx_a should be Stale"
    );
    assert!(
        matches!(state_b.status, IndexStatus::Current),
        "idx_b should still be Current"
    );

    // Build idx_a via the scheduler.
    scheduler
        .submit(IndexBuildJob {
            sstable_id: "sst-100".to_string(),
            index_name: "idx_a".to_string(),
            index_type: IndexType::Hash,
            table: ("ks".to_string(), "tbl_a".to_string()),
            priority: BuildPriority::Normal,
            enqueued_at: Instant::now(),
            column_position: 0,
            clustering_source: None,
            filter_predicate: None,
        })
        .unwrap();

    // Wait for idx_a to complete.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let state = tracker.get_state("ks", "tbl_a", "idx_a").unwrap();
        if matches!(state.status, IndexStatus::Current) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for idx_a to become Current"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Both should now be Current.
    let state_a = tracker.get_state("ks", "tbl_a", "idx_a").unwrap();
    let state_b = tracker.get_state("ks", "tbl_b", "idx_b").unwrap();
    assert!(matches!(state_a.status, IndexStatus::Current));
    assert!(matches!(state_b.status, IndexStatus::Current));

    // Now make idx_b stale — idx_a stays Current.
    tracker.mark_pending("ks", "tbl_b", "idx_b", "sst-200", 1024);
    let state_a = tracker.get_state("ks", "tbl_a", "idx_a").unwrap();
    let state_b = tracker.get_state("ks", "tbl_b", "idx_b").unwrap();
    assert!(
        matches!(state_a.status, IndexStatus::Current),
        "idx_a should remain Current"
    );
    assert!(
        matches!(state_b.status, IndexStatus::Stale { .. }),
        "idx_b should be Stale"
    );

    scheduler.shutdown();
}

/// Dropping an index removes it entirely from the tracker.
#[test]
fn drop_index_removes_from_tracker() {
    let tracker = Arc::new(IndexStateTracker::new());

    tracker.register_index("ks", "tbl", "idx_to_drop");
    tracker.mark_pending("ks", "tbl", "idx_to_drop", "sst-001", 256);

    // Verify it exists.
    assert!(tracker.get_state("ks", "tbl", "idx_to_drop").is_some());
    assert_eq!(tracker.all_states().len(), 1);

    // Drop it.
    let removed = tracker.remove_index("ks", "tbl", "idx_to_drop");
    assert!(removed, "remove_index should return true");

    // Verify it's gone.
    assert!(tracker.get_state("ks", "tbl", "idx_to_drop").is_none());
    assert!(tracker.all_states().is_empty());

    // Dropping again returns false.
    let removed = tracker.remove_index("ks", "tbl", "idx_to_drop");
    assert!(
        !removed,
        "remove_index for absent index should return false"
    );
}
