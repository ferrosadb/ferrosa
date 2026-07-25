//! Module: Move the synchronous relational executor off the async runtime.
//! Correctness: Correct when every CPU-bound `ferrosa_sql` call runs on a
//!   blocking thread, so no async worker is occupied for the duration of a
//!   sort/join and connection keepalives stay responsive under query load.
//! Last revised: 2026-07-25
//! Last changed: Created — `execute_offloaded` wraps `ferrosa_sql::execute`
//!   (t_d3b2dec1).
//!
//! # Why
//!
//! `ferrosa_sql::execute` is fully synchronous and CPU-bound: it runs scan,
//! filter, sort, hash-aggregate and hash-join over a materialized row set. It
//! used to be called directly inside the async query handlers, so one
//! PG-wire `SELECT` over a large table occupied an async worker thread for the
//! whole sort/join.
//!
//! That is the failure mode PR #131 fixed on the CQL keepalive path: sync work
//! inline on an async worker starves keepalives and raft heartbeats. The rule
//! that came out of it — any sync storage/CPU work must be offloaded — applies
//! here for the same reason.
//!
//! # Why `spawn_blocking` and not the scheduler pool
//!
//! `ferrosa_sched`'s `submit_scan` is built for *chunked streaming producers*:
//! it hands the caller a `ScanSlot` to yield against and wires channel-close
//! cancellation. A relational `execute` is a single opaque call with no yield
//! points, so it has nothing to do with a `ScanSlot`.
//!
//! `spawn_blocking` is the right primitive, and it is not unbounded: the data
//! and background runtimes set explicit `max_blocking_threads` ceilings
//! (`ferrosa/src/runtime.rs`, t_88223ad0) precisely so blocking work cannot
//! oversubscribe the cores and starve consensus.

use ferrosa_sql::{Catalog, ExecError, QueryResult, SelectStmt, Value};

/// Run the synchronous relational executor on a blocking thread.
///
/// Takes every input by value because the closure must be `'static`: the
/// statement and catalog are moved onto the blocking thread rather than
/// borrowed across the await. `MapCatalog`'s tables are
/// `Arc<dyn TableProvider + Send + Sync>`, so the move is a refcount bump, not
/// a copy of the row data.
///
/// # Errors
///
/// Propagates the executor's [`ExecError`] unchanged.
///
/// # Panics
///
/// A panic inside the executor is re-raised on the caller with its original
/// payload rather than being flattened into a SQL error. `ExecError`'s variants
/// are all user-facing SQL conditions (`NoSuchTable`, `NotGrouped`, ...); a
/// panic is an engine bug, and dressing it up as a query error would hide a
/// defect behind a message the client cannot act on. Re-raising keeps the
/// original message and backtrace, and tokio isolates the failure to this
/// connection's task.
///
/// `spawn_blocking` work cannot be cancelled once it has started, so a panic is
/// the only way this join fails.
pub(crate) async fn execute_offloaded<C>(
    stmt: SelectStmt,
    catalog: C,
    default_schema: String,
    params: Vec<Value>,
) -> Result<QueryResult, ExecError>
where
    C: Catalog + Send + 'static,
{
    match tokio::task::spawn_blocking(move || {
        ferrosa_sql::execute(&stmt, &catalog, &default_schema, &params)
    })
    .await
    {
        Ok(result) => result,
        Err(join_err) => std::panic::resume_unwind(join_err.into_panic()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use ferrosa_sql::{SharedTable, Statement};

    /// A catalog that records the thread its `resolve` ran on.
    ///
    /// `Catalog::resolve` is called from inside `ferrosa_sql::execute`, so the
    /// thread it observes IS the thread the synchronous executor ran on. That
    /// makes the offload directly observable instead of inferred. The recorder
    /// is shared via `Arc` so it stays readable after the catalog is moved onto
    /// the blocking thread.
    struct ThreadRecordingCatalog(std::sync::Arc<Mutex<Option<std::thread::ThreadId>>>);

    impl Catalog for ThreadRecordingCatalog {
        fn resolve(&self, _schema: &str, _table: &str) -> Option<SharedTable> {
            *self.0.lock().expect("recorder lock") = Some(std::thread::current().id());
            None // absent table: resolve still ran, which is what we observe
        }
    }

    fn select_stmt(sql: &str) -> SelectStmt {
        match ferrosa_sql::parse_statement(sql).expect("statement parses") {
            Statement::Select(s) => *s,
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    /// The synchronous executor must NOT run on the caller's async worker.
    ///
    /// This is the whole point of the module: an inline `execute` pins a runtime
    /// worker for the duration of a sort/join, which is how the PR #131
    /// keepalive starvation happened. One worker thread makes an inline call
    /// unmistakable — it would run on the very thread awaiting it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn executor_does_not_run_on_the_async_worker() {
        let caller = std::thread::current().id();
        let recorder = std::sync::Arc::new(Mutex::new(None));

        let _ = execute_offloaded(
            select_stmt("SELECT * FROM t"),
            ThreadRecordingCatalog(recorder.clone()),
            "public".to_string(),
            Vec::new(),
        )
        .await;

        let ran_on = recorder
            .lock()
            .expect("recorder lock")
            .expect("the executor called Catalog::resolve");
        assert_ne!(
            caller, ran_on,
            "the synchronous executor ran on the async worker; it must be offloaded"
        );
    }

    /// The offload must not change what the query returns.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn offloading_preserves_the_executor_result() {
        let stmt = select_stmt("SELECT * FROM missing");
        let catalog = ferrosa_sql::MapCatalog::new();

        let direct = ferrosa_sql::execute(&stmt, &catalog, "public", &[]);
        let offloaded = execute_offloaded(
            select_stmt("SELECT * FROM missing"),
            ferrosa_sql::MapCatalog::new(),
            "public".to_string(),
            Vec::new(),
        )
        .await;

        assert_eq!(
            direct, offloaded,
            "offloading must be behavior-preserving, including the error case"
        );
    }
}
