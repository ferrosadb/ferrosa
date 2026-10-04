//! Idle expiry and disconnect each release everything a suspended portal
//! held: its node slot, the blocking threads, the storage scan's pool slot,
//! and its spilled sort runs.
//!
//! One test in its own binary: it points the query temp directory and the
//! spill threshold at this test through the environment, before any query.

#[path = "common/blocking_pool.rs"]
mod blocking_pool;
#[path = "common/pg_server.rs"]
mod pg_server;

use std::path::Path;
use std::time::Duration;

use blocking_pool::{eventually, on_bounded_runtime, settle_blocking_pool, MAX_BLOCKING, SETTLE};
use ferrosa_postgres::PortalLimits;
use pg_server::{connect, start_server_with};
use tokio_postgres::error::SqlState;

const ROWS: usize = 3_000;

/// Short enough to wait out, long enough that the test can observe the portal
/// suspended first.
const IDLE_TIMEOUT: Duration = Duration::from_millis(1_500);

/// Entries under the query temp root: one directory per live spill
/// reservation.
fn spill_entries(root: &Path) -> usize {
    match std::fs::read_dir(root) {
        Ok(entries) => entries.count(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => panic!("cannot list the spill root {}: {error}", root.display()),
    }
}

/// Suspend two portals on `client` after one row each: a sort that spilled,
/// and a plain scan whose storage producer is stopped mid-table.
async fn suspend_two(
    tx: &tokio_postgres::Transaction<'_>,
) -> (tokio_postgres::Portal, tokio_postgres::Portal) {
    let sorted = tx
        .prepare("SELECT id FROM t ORDER BY id")
        .await
        .expect("prepare");
    let plain = tx.prepare("SELECT id FROM t").await.expect("prepare");
    let a = tx.bind(&sorted, &[]).await.expect("bind");
    let b = tx.bind(&plain, &[]).await.expect("bind");
    assert_eq!(tx.query_portal(&a, 1).await.expect("a suspends").len(), 1);
    assert_eq!(tx.query_portal(&b, 1).await.expect("b suspends").len(), 1);
    (a, b)
}

#[test]
fn idle_expiry_and_disconnect_release_slots_threads_and_spill_files() {
    let temp = tempfile::tempdir().expect("tempdir");
    let spill_root = temp.path().join("spill");
    // Before any query: every sort here spills, under a root this test owns.
    std::env::set_var("FERROSA_SQL_TEMP_DIR", &spill_root);
    std::env::set_var("FERROSA_RANGE_SPILL_THRESHOLD_BYTES", "256");

    on_bounded_runtime(async {
        let pool = ferrosa_sched::init_global_pool(ferrosa_sched::Reservation::new(17, 1));
        let limits = PortalLimits {
            idle_timeout: IDLE_TIMEOUT,
            ..PortalLimits::default()
        };
        let server = start_server_with(ROWS, limits).await;

        // ── Idle expiry ────────────────────────────────────────────────────
        let mut client = connect(server.port).await;
        let tx = client.transaction().await.expect("begin");
        let (sorted, _plain) = suspend_two(&tx).await;
        assert_eq!(server.portals.suspended(), 2);
        assert!(
            spill_entries(&spill_root) > 0,
            "premise: the suspended sort holds spilled runs"
        );

        let expired = eventually(IDLE_TIMEOUT + SETTLE, || server.portals.suspended() == 0).await;
        assert!(
            expired,
            "{} portals still suspended long after the idle timeout",
            server.portals.suspended()
        );
        assert!(
            eventually(SETTLE, || spill_entries(&spill_root) == 0).await,
            "an expired portal left {} spill reservations behind",
            spill_entries(&spill_root)
        );
        assert!(
            eventually(SETTLE, || pool.active() == 0).await,
            "an expired portal's scan still holds {} pool slots",
            pool.active()
        );
        assert_eq!(settle_blocking_pool().await, MAX_BLOCKING);

        let error = tx
            .query_portal(&sorted, 1)
            .await
            .expect_err("an expired portal must not run again");
        assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED));
        let message = error
            .as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_default();
        assert!(
            message.contains("FERROSA_POSTGRES_SUSPENDED_PORTAL_IDLE_TIMEOUT_MS"),
            "{message}"
        );
        drop(tx);
        drop(client);

        // ── Disconnect ─────────────────────────────────────────────────────
        let mut client = connect(server.port).await;
        {
            let tx = client.transaction().await.expect("begin");
            let _held = suspend_two(&tx).await;
            assert_eq!(server.portals.suspended(), 2);
            assert!(
                spill_entries(&spill_root) > 0,
                "premise: spilled runs exist"
            );
            // Leak the transaction guard so dropping the client is a bare
            // disconnect, not a ROLLBACK.
            std::mem::forget(tx);
        }
        drop(client);
        assert!(
            eventually(SETTLE, || server.portals.suspended() == 0).await,
            "a disconnect left {} portals suspended",
            server.portals.suspended()
        );
        assert!(
            eventually(SETTLE, || spill_entries(&spill_root) == 0).await,
            "a disconnect left {} spill reservations behind",
            spill_entries(&spill_root)
        );
        assert!(
            eventually(SETTLE, || pool.active() == 0).await,
            "a disconnected portal's scan still holds pool slots"
        );
        assert_eq!(settle_blocking_pool().await, MAX_BLOCKING);
    });
}
