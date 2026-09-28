//! Module: per-partition serialization of conditional writes on a standalone node.
//! Correctness: two conditional statements on one partition never interleave
//! their read/evaluate/write steps; the racing-update test applies exactly one.
//! Last revised: 2026-09-28
//! Last changed: New; standalone `UPDATE/DELETE/INSERT ... IF` evaluate under this lock.
//!
//! In cluster mode a conditional statement is decided by Accord
//! (`router::route_lwt_via_accord`). A standalone node has no peers and no
//! consensus round, so the same read-evaluate-write sequence is made atomic by
//! serializing it per partition inside the process that owns the storage
//! engine. The lock table is process-wide because one process owns one engine;
//! two engines in one test process only share stripes (extra contention, never
//! a missed exclusion).
//!
//! Only conditional writes take the lock. Mixing conditional and
//! unconditional writes on the same row is not linearizable in Cassandra
//! either, so unconditional writes stay lock-free.

use std::hash::{Hash, Hasher};
use std::sync::OnceLock;

use ferrosa_common::DecoratedKey;
use ferrosa_storage::TableId;
use tokio::sync::{Mutex, MutexGuard};

/// Number of lock stripes. Power of two; a collision only adds contention.
const STRIPE_COUNT: usize = 1024;

static STRIPES: OnceLock<Box<[Mutex<()>]>> = OnceLock::new();

fn stripes() -> &'static [Mutex<()>] {
    STRIPES.get_or_init(|| (0..STRIPE_COUNT).map(|_| Mutex::new(())).collect())
}

/// Index of the stripe guarding `(table, key)`.
fn stripe_index(table: &TableId, key: &DecoratedKey) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    table.keyspace.hash(&mut h);
    table.table.hash(&mut h);
    key.key.as_bytes().hash(&mut h);
    (h.finish() as usize) % STRIPE_COUNT
}

/// Serialize conditional writes on one partition.
///
/// A `tokio` mutex is required (not `std`/`parking_lot`) because the guard is
/// held across the async storage read and write. Dropping the returned future
/// before completion releases nothing it did not acquire (cancel safe).
/// Callers hold a single partition lock at a time, so no lock ordering exists
/// to deadlock on.
pub async fn lock_partition(table: &TableId, key: &DecoratedKey) -> MutexGuard<'static, ()> {
    stripes()[stripe_index(table, key)].lock().await
}

#[cfg(test)]
mod tests {
    //! Standalone (non-cluster) conditional-write evidence for t_cd5142b5.

    use std::sync::Arc;

    use ferrosa_cluster::consistency::ConsistencyLevel;
    use ferrosa_schema::AuthContext;

    use crate::error::CqlError;
    use crate::router::{route, route_transactional, RequestContext, RouteResult, SharedState};

    fn ctx<'a>(auth: &'a AuthContext, ks: &'a Option<String>) -> RequestContext<'a> {
        RequestContext {
            auth,
            current_keyspace: ks,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
            protocol_version: 4,
        }
    }

    fn auth() -> AuthContext {
        AuthContext {
            role: "cassandra".into(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    async fn run(state: &SharedState, cql: &str) -> Result<Vec<u8>, CqlError> {
        let auth = auth();
        let ks = Some("lwt_ks".to_string());
        let stmt = crate::parser::parse(cql)?;
        match route(state, &ctx(&auth, &ks), stmt).await? {
            RouteResult::Result(buf) => Ok(buf.to_vec()),
            _ => Err(CqlError::ServerError("unexpected route result".into())),
        }
    }

    async fn setup() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let state = crate::test_util::standalone_for_test(dir.path());
        let auth = auth();
        let none = None;
        for cql in [
            "CREATE KEYSPACE lwt_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            "CREATE TABLE lwt_ks.t (k int PRIMARY KEY, v text, n int)",
        ] {
            let stmt = crate::parser::parse(cql).expect("parse ddl");
            route(&state, &ctx(&auth, &none), stmt).await.expect("ddl");
        }
        run(
            &state,
            "INSERT INTO lwt_ks.t (k, v, n) VALUES (1, 'orig', 10)",
        )
        .await
        .expect("seed");
        (state, dir)
    }

    /// Decoded first row of a Rows result: column names and raw cell bytes.
    struct Decoded {
        names: Vec<String>,
        cells: Vec<Option<Vec<u8>>>,
        rows: usize,
    }

    fn be_u16(b: &[u8], off: usize) -> usize {
        u16::from_be_bytes([b[off], b[off + 1]]) as usize
    }

    fn be_i32(b: &[u8], off: usize) -> i32 {
        i32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
    }

    fn decode(buf: &[u8]) -> Decoded {
        assert_eq!(be_i32(buf, 0), 2, "expected a Rows result");
        let col_count = be_i32(buf, 8) as usize;
        let mut off = 12;
        off += 2 + be_u16(buf, off);
        off += 2 + be_u16(buf, off);
        let mut names = Vec::new();
        for _ in 0..col_count {
            let n = be_u16(buf, off);
            names.push(String::from_utf8_lossy(&buf[off + 2..off + 2 + n]).into_owned());
            off += 2 + n + 2;
        }
        let rows = be_i32(buf, off) as usize;
        off += 4;
        let mut cells = Vec::new();
        for _ in 0..(rows.min(1) * col_count) {
            let len = be_i32(buf, off);
            off += 4;
            if len < 0 {
                cells.push(None);
            } else {
                cells.push(Some(buf[off..off + len as usize].to_vec()));
                off += len as usize;
            }
        }
        Decoded { names, cells, rows }
    }

    fn applied(d: &Decoded) -> bool {
        assert_eq!(d.names[0], "[applied]");
        d.cells[0].as_deref() == Some(&[1u8][..])
    }

    fn cell_text(d: &Decoded, col: &str) -> Option<String> {
        let i = d.names.iter().position(|n| n == col).expect("column");
        d.cells[i]
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).into_owned())
    }

    async fn current_v(state: &SharedState) -> Option<String> {
        let d = decode(
            &run(state, "SELECT v FROM lwt_ks.t WHERE k = 1")
                .await
                .expect("select"),
        );
        if d.rows == 0 {
            return None;
        }
        cell_text(&d, "v")
    }

    #[tokio::test]
    async fn update_if_false_condition_does_not_write_and_returns_current_values() {
        let (state, _d) = setup().await;
        let r = run(
            &state,
            "UPDATE lwt_ks.t SET v = 'new' WHERE k = 1 IF v = 'WRONG'",
        )
        .await
        .expect("route");
        let d = decode(&r);
        assert!(!applied(&d));
        assert_eq!(cell_text(&d, "v").as_deref(), Some("orig"));
        assert_eq!(current_v(&state).await.as_deref(), Some("orig"));
    }

    #[tokio::test]
    async fn update_if_true_condition_writes_and_returns_applied() {
        let (state, _d) = setup().await;
        let r = run(
            &state,
            "UPDATE lwt_ks.t SET v = 'new' WHERE k = 1 IF v = 'orig'",
        )
        .await
        .expect("route");
        assert!(applied(&decode(&r)));
        assert_eq!(current_v(&state).await.as_deref(), Some("new"));
    }

    #[tokio::test]
    async fn update_if_exists_on_missing_row_does_not_write() {
        let (state, _d) = setup().await;
        let r = run(&state, "UPDATE lwt_ks.t SET v = 'x' WHERE k = 99 IF EXISTS")
            .await
            .expect("route");
        assert!(!applied(&decode(&r)));
        let sel = decode(
            &run(&state, "SELECT v FROM lwt_ks.t WHERE k = 99")
                .await
                .unwrap(),
        );
        assert_eq!(sel.rows, 0, "IF EXISTS on a missing row must not create it");
    }

    #[tokio::test]
    async fn delete_if_false_condition_does_not_delete() {
        let (state, _d) = setup().await;
        let r = run(&state, "DELETE FROM lwt_ks.t WHERE k = 1 IF v = 'WRONG'")
            .await
            .expect("route");
        let d = decode(&r);
        assert!(!applied(&d));
        assert_eq!(cell_text(&d, "v").as_deref(), Some("orig"));
        assert_eq!(current_v(&state).await.as_deref(), Some("orig"));
    }

    #[tokio::test]
    async fn delete_if_true_condition_deletes_and_returns_applied() {
        let (state, _d) = setup().await;
        let r = run(&state, "DELETE FROM lwt_ks.t WHERE k = 1 IF v = 'orig'")
            .await
            .expect("route");
        assert!(applied(&decode(&r)));
        assert_eq!(current_v(&state).await, None);
    }

    #[tokio::test]
    async fn delete_if_exists_on_missing_row_is_not_applied() {
        let (state, _d) = setup().await;
        let r = run(&state, "DELETE FROM lwt_ks.t WHERE k = 99 IF EXISTS")
            .await
            .expect("route");
        assert!(!applied(&decode(&r)));
    }

    #[tokio::test]
    async fn transaction_staged_conditional_is_rejected_loudly() {
        let (state, _d) = setup().await;
        let auth = auth();
        let ks = Some("lwt_ks".to_string());
        let now = std::time::Instant::now();
        let mut shim = None;
        let begin = crate::parser::parse("BEGIN TRANSACTION").expect("parse");
        route_transactional(&state, &ctx(&auth, &ks), &begin, &mut shim, now)
            .await
            .expect("txn stmt")
            .expect("begin");
        let upd = crate::parser::parse("UPDATE lwt_ks.t SET v = 'x' WHERE k = 1 IF v = 'orig'")
            .expect("parse");
        let err = route_transactional(&state, &ctx(&auth, &ks), &upd, &mut shim, now)
            .await
            .expect("staged in txn")
            .err()
            .expect("conditional in a transaction must be rejected");
        assert!(
            matches!(err, CqlError::ConditionalUnsupported { .. }),
            "got {err:?}"
        );
        assert_eq!(current_v(&state).await.as_deref(), Some("orig"));
    }

    #[tokio::test]
    async fn logged_batch_with_condition_is_rejected_and_writes_nothing() {
        let (state, _d) = setup().await;
        let err = run(
            &state,
            "BEGIN BATCH \
             INSERT INTO lwt_ks.t (k, v, n) VALUES (2, 'b', 1); \
             UPDATE lwt_ks.t SET v = 'x' WHERE k = 1 IF v = 'WRONG'; \
             APPLY BATCH",
        )
        .await
        .expect_err("conditional batch must be rejected");
        assert!(
            matches!(err, CqlError::ConditionalUnsupported { .. }),
            "got {err:?}"
        );
        assert_eq!(current_v(&state).await.as_deref(), Some("orig"));
        let sel = decode(
            &run(&state, "SELECT v FROM lwt_ks.t WHERE k = 2")
                .await
                .unwrap(),
        );
        assert_eq!(sel.rows, 0, "no statement of a rejected batch may apply");
    }

    #[tokio::test]
    async fn unlogged_batch_with_condition_is_rejected() {
        let (state, _d) = setup().await;
        let err = run(
            &state,
            "BEGIN UNLOGGED BATCH \
             DELETE FROM lwt_ks.t WHERE k = 1 IF EXISTS; \
             APPLY BATCH",
        )
        .await
        .expect_err("conditional batch must be rejected");
        assert!(matches!(err, CqlError::ConditionalUnsupported { .. }));
        assert_eq!(current_v(&state).await.as_deref(), Some("orig"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn racing_conditional_updates_apply_exactly_once() {
        for round in 0..20 {
            let (state, _d) = setup().await;
            let mut tasks = Vec::new();
            for i in 0..8 {
                let state = Arc::clone(&state);
                tasks.push(tokio::spawn(async move {
                    let cql = format!("UPDATE lwt_ks.t SET v = 'w{i}' WHERE k = 1 IF v = 'orig'");
                    decode(&run(&state, &cql).await.expect("route"))
                }));
            }
            let mut wins = 0;
            for t in tasks {
                if applied(&t.await.expect("join")) {
                    wins += 1;
                }
            }
            assert_eq!(wins, 1, "round {round}: exactly one racer may apply");
        }
    }
}
