//! Module: Stream query results from the executor to the wire, O(batch) not
//! O(result).
//! Correctness: Correct when (1) no more than a bounded window of rows is ever
//! resident between the executor and the socket, whatever the result size;
//! (2) every row the executor yields is delivered exactly once, in order, across
//! any number of `Execute` calls on a suspended portal; (3) a failure after
//! rows were sent surfaces as an `ErrorResponse`, never as a `CommandComplete`;
//! and (4) a query waiting for its client — a portal suspended by `max_rows`, a
//! socket that is not draining — holds no thread.
//! Last revised: 2026-10-03
//! Last changed: Pull-driven. The executor is an owned `RowCursor` pulled a
//!   batch at a time on a blocking thread, so a waiting query parks no thread
//!   (missing-guards entry 8, FMEA PG-Tf348ba0b).
//!
//! # Shape
//!
//! The query is opened as a [`ferrosa_sql::RowCursor`]: the whole operator
//! pipeline, owned and `Send`, with nothing pulled yet. Pulling may block (on
//! storage, on spilled runs), so a pull runs on a blocking thread — a *fetch*
//! of at most [`RESULT_BATCH_ROWS`] rows that returns the cursor with the rows.
//! The async side encodes a batch to `DataRow`s and writes it to the socket.
//!
//! As soon as one batch arrives the next fetch starts, so the executor works
//! while the socket write is in flight. A fetch never waits on the client: it
//! ends when it has a batch, or the result ends. Between fetches the cursor is
//! just a value, so neither a suspended portal nor a client that stopped
//! reading holds a thread. Memory is bounded by
//!
//! - one batch being encoded or sent,
//! - one prefetched batch, and
//! - for a suspended portal, the unsent remainder of a batch.
//!
//! Dropping the [`ResultStream`] (portal `Close`, disconnect, session end)
//! drops the cursor, and with it the scans and their storage producers; an
//! in-flight fetch sees the cancel flag at its next row and stops.
//!
//! # Below the executor
//!
//! A storage scan the cursor reads from is fed by a producer on the scan pool.
//! When the cursor is not pulled, that producer gives back its pool slot and
//! its thread at once (it never waits for room on a blocking thread, which
//! the executor here needs), and resumes from its position when rows are
//! wanted again (`ferrosa-storage`, FMEA PG-Tf348ba0b).

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ferrosa_sql::{Catalog, Column, ColumnType, ExecError, Row, RowCursor, SelectStmt, SpillCtx};
use tokio::task::JoinHandle;

use crate::messages::BackendMessage;
use crate::query::{
    check_scan_failure, encode_data_row, encode_error_response, error_response,
    exec_error_response, ReplySink,
};
use crate::storage_provider::ScanFailure;

/// Rows one fetch pulls from the executor: the unit of work a blocking thread
/// does per turn, and the unit handed to the socket.
pub(crate) const RESULT_BATCH_ROWS: usize = 16;

/// How a fetch left the cursor.
enum FetchEnd {
    /// The batch is full; the cursor has (or may have) more.
    More(RowCursor),
    /// The result ended after this batch.
    Done,
    /// The pipeline failed after this batch; what was sent is incomplete.
    Failed(ExecError),
    /// The stream was dropped while the fetch ran; nobody reads this.
    Cancelled,
}

/// One fetch's output: the rows, then how the cursor ended.
struct Fetched {
    rows: Vec<Row>,
    end: FetchEnd,
}

/// Body of one fetch: pull up to [`RESULT_BATCH_ROWS`] rows. Runs on a
/// blocking thread and waits only on the executor's own inputs, never on the
/// client, so it always ends in bounded time.
fn fetch(mut cursor: RowCursor, cancel: &AtomicBool) -> Fetched {
    let mut rows = Vec::with_capacity(RESULT_BATCH_ROWS);
    while rows.len() < RESULT_BATCH_ROWS {
        if cancel.load(Ordering::Acquire) {
            return Fetched {
                rows,
                end: FetchEnd::Cancelled,
            };
        }
        match cursor.next_row() {
            Some(Ok(row)) => rows.push(row),
            Some(Err(error)) => {
                return Fetched {
                    rows,
                    end: FetchEnd::Failed(error),
                }
            }
            None => {
                return Fetched {
                    rows,
                    end: FetchEnd::Done,
                }
            }
        }
    }
    Fetched {
        rows,
        end: FetchEnd::More(cursor),
    }
}

/// Where the executor is, between and during fetches.
enum Exec {
    /// A fetch is running, or finished and waiting to be collected.
    Fetching(JoinHandle<Fetched>),
    /// The result ended; nothing is left to pull.
    Done,
    /// The pipeline failed; reported once the rows before it are sent.
    Failed(ExecError),
    /// The stream gave up on the query (an encode failure, or drop); any
    /// further pull is an internal error, never an end of result.
    Abandoned,
}

/// How one [`ResultStream::pump`] call ended.
#[derive(Debug)]
pub(crate) enum PumpEnd {
    /// Every row was delivered; `rows` counts those sent by this call.
    Complete { rows: usize },
    /// The row limit was reached with rows possibly remaining.
    Suspended,
    /// The query failed after (possibly) sending rows.
    Failed(BackendMessage),
}

impl PumpEnd {
    /// The messages that close this call's reply.
    pub(crate) fn into_messages(self) -> Vec<BackendMessage> {
        match self {
            PumpEnd::Complete { rows } => vec![BackendMessage::CommandComplete {
                tag: format!("SELECT {rows}"),
            }],
            PumpEnd::Suspended => vec![BackendMessage::PortalSuspended],
            PumpEnd::Failed(error) => vec![error],
        }
    }
}

/// What [`ResultStream::next_batch`] found.
enum Batch {
    Rows,
    End,
    Failed(BackendMessage),
}

/// A running query whose rows are pulled on demand.
pub(crate) struct ResultStream {
    columns: Vec<Column>,
    exec: Exec,
    /// Unsent remainder of the last batch received.
    buffered: VecDeque<Row>,
    failure: ScanFailure,
    /// Set on drop; an in-flight fetch stops at its next row.
    cancel: Arc<AtomicBool>,
}

/// Resolve and open `stmt` on a blocking thread, so a resolution error is
/// reported before any output, and start fetching its first batch.
///
/// # Errors
///
/// The `ErrorResponse` to send when the query cannot start.
///
/// # Panics
///
/// Re-raises a panic from the executor thread with its original payload, as
/// `offload` did: a panic is an engine bug, not a SQL condition.
pub(crate) async fn open_stream<C>(
    stmt: SelectStmt,
    catalog: C,
    failure: ScanFailure,
    default_schema: String,
    params: Vec<ferrosa_sql::Value>,
) -> Result<ResultStream, BackendMessage>
where
    C: Catalog + Send + 'static,
{
    let opened = tokio::task::spawn_blocking(move || {
        ferrosa_sql::open_cursor(
            &stmt,
            &catalog,
            &default_schema,
            &params,
            &SpillCtx::default(),
        )
    })
    .await;
    let cursor = match opened {
        Ok(Ok(cursor)) => cursor,
        Ok(Err(error)) => {
            return Err(check_scan_failure(&failure).unwrap_or_else(|| exec_error_response(&error)))
        }
        Err(join_error) => return Err(executor_died(join_error)),
    };
    let cancel = Arc::new(AtomicBool::new(false));
    Ok(ResultStream {
        columns: cursor.columns().to_vec(),
        exec: Exec::Fetching(spawn_fetch(cursor, &cancel)),
        buffered: VecDeque::new(),
        failure,
        cancel,
    })
}

/// Pull the next batch from `cursor` on a blocking thread.
fn spawn_fetch(cursor: RowCursor, cancel: &Arc<AtomicBool>) -> JoinHandle<Fetched> {
    let cancel = Arc::clone(cancel);
    tokio::task::spawn_blocking(move || fetch(cursor, &cancel))
}

/// A fetch's blocking task did not return: re-raise a panic, report anything
/// else (runtime shutdown) as an internal error.
fn executor_died(join_error: tokio::task::JoinError) -> BackendMessage {
    if join_error.is_panic() {
        std::panic::resume_unwind(join_error.into_panic());
    }
    error_response(
        "XX000",
        &format!("internal error: query executor was cancelled: {join_error}"),
    )
}

impl ResultStream {
    /// The output columns, known before the first row.
    pub(crate) fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Wait for the in-flight fetch and buffer its rows, starting the next
    /// fetch at once if the result goes on. Rows are reported before the end
    /// or failure that followed them.
    async fn next_batch(&mut self) -> Batch {
        loop {
            let handle = match &mut self.exec {
                Exec::Fetching(handle) => handle,
                Exec::Done => {
                    return match check_scan_failure(&self.failure) {
                        // A scan that died closed its channel, so the executor
                        // finished "successfully" over a short row set. Never
                        // report that as done.
                        Some(error) => Batch::Failed(error),
                        None => Batch::End,
                    };
                }
                // A recorded storage failure outranks an executor error
                // computed from the truncated scan beneath it.
                Exec::Failed(error) => {
                    return Batch::Failed(
                        check_scan_failure(&self.failure)
                            .unwrap_or_else(|| exec_error_response(error)),
                    )
                }
                Exec::Abandoned => {
                    return Batch::Failed(error_response(
                        "XX000",
                        "internal error: the query was abandoned and has no more rows",
                    ))
                }
            };
            let fetched = match handle.await {
                Ok(fetched) => fetched,
                Err(join_error) => {
                    self.exec = Exec::Done;
                    return Batch::Failed(executor_died(join_error));
                }
            };
            self.exec = match fetched.end {
                FetchEnd::More(cursor) => Exec::Fetching(spawn_fetch(cursor, &self.cancel)),
                FetchEnd::Done => Exec::Done,
                FetchEnd::Failed(error) => Exec::Failed(error),
                FetchEnd::Cancelled => Exec::Abandoned,
            };
            if !fetched.rows.is_empty() {
                self.buffered.extend(fetched.rows);
                return Batch::Rows;
            }
        }
    }

    /// Encode and send up to `limit` rows (all of them when `None`), in order.
    ///
    /// Rows go to `out` a batch at a time, so nothing beyond one batch is held
    /// here. A `Suspended` end leaves the stream positioned exactly after the
    /// last row sent; calling `pump` again continues from there.
    ///
    /// # Errors
    ///
    /// An I/O error from `out` (the client went away). The caller drops the
    /// stream, which stops the executor.
    pub(crate) async fn pump<O: ReplySink>(
        &mut self,
        limit: Option<usize>,
        formats: &[i16],
        out: &mut O,
    ) -> io::Result<PumpEnd> {
        let types: Vec<ColumnType> = self.columns.iter().map(|c| c.ty).collect();
        let mut sent = 0usize;
        loop {
            let room = limit.map_or(usize::MAX, |n| n.saturating_sub(sent));
            if room == 0 {
                return Ok(PumpEnd::Suspended);
            }
            if self.buffered.is_empty() {
                match self.next_batch().await {
                    Batch::Rows => {}
                    Batch::End => return Ok(PumpEnd::Complete { rows: sent }),
                    Batch::Failed(error) => return Ok(PumpEnd::Failed(error)),
                }
            }
            let take = room.min(self.buffered.len());
            let (messages, encode_error) = self.encode_next(take, &types, formats);
            sent += messages.len();
            out.send(messages).await?;
            if let Some(error) = encode_error {
                self.abandon();
                return Ok(PumpEnd::Failed(error));
            }
        }
    }

    /// Encode the next `take` buffered rows, stopping at the first that cannot
    /// be encoded. Rows before it are still returned so they reach the client
    /// ahead of the error, as they would have from PostgreSQL.
    fn encode_next(
        &mut self,
        take: usize,
        types: &[ColumnType],
        formats: &[i16],
    ) -> (Vec<BackendMessage>, Option<BackendMessage>) {
        let mut messages = Vec::with_capacity(take);
        for row in self.buffered.drain(..take) {
            match encode_data_row(&row, types, formats) {
                Ok(message) => messages.push(message),
                Err(error) => return (messages, Some(encode_error_response(&error))),
            }
        }
        (messages, None)
    }

    /// Stop the executor. An in-flight fetch stops at its next row and drops
    /// the cursor (its scans and spill files) when it returns; a finished one
    /// is dropped with its handle.
    fn abandon(&mut self) {
        self.cancel.store(true, Ordering::Release);
        self.exec = Exec::Abandoned;
        self.buffered.clear();
    }
}

impl Drop for ResultStream {
    fn drop(&mut self) {
        self.abandon();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use ferrosa_sql::{RelSchema, TableProvider, Value};

    /// `n` int rows; when `poison_at` is set, that row carries a numeric, which
    /// has no binary encoding and so fails at encode time. Counts rows produced.
    struct Numbers {
        schema: RelSchema,
        n: i64,
        poison_at: Option<i64>,
        produced: Arc<AtomicUsize>,
    }

    impl TableProvider for Numbers {
        fn schema(&self) -> &RelSchema {
            &self.schema
        }

        fn scan(&self) -> Box<dyn Iterator<Item = Row> + Send> {
            let produced = Arc::clone(&self.produced);
            let poison_at = self.poison_at;
            Box::new((0..self.n).map(move |i| {
                produced.fetch_add(1, Ordering::SeqCst);
                if poison_at == Some(i) {
                    Row::new(vec![Value::numeric(1.into(), 0)])
                } else {
                    Row::new(vec![Value::Int(i)])
                }
            }))
        }
    }

    fn select(sql: &str) -> SelectStmt {
        match ferrosa_sql::parse_statement(sql).expect("parses") {
            ferrosa_sql::Statement::Select(s) => *s,
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    async fn open(n: i64, poison_at: Option<i64>) -> (ResultStream, Arc<AtomicUsize>) {
        let produced = Arc::new(AtomicUsize::new(0));
        let table = Numbers {
            schema: RelSchema::new(vec![Column::new("id", ColumnType::Int)]),
            n,
            poison_at,
            produced: Arc::clone(&produced),
        };
        let catalog = ferrosa_sql::MapCatalog::new().with_table("public", "t", Arc::new(table));
        let stream = open_stream(
            select("SELECT id FROM t"),
            catalog,
            ScanFailure::default(),
            "public".to_string(),
            Vec::new(),
        )
        .await
        .expect("stream opens");
        (stream, produced)
    }

    fn text_ids(messages: &[BackendMessage]) -> Vec<i64> {
        messages
            .iter()
            .map(|m| match m {
                BackendMessage::DataRow { columns } => {
                    let text = columns[0].as_ref().expect("non-null");
                    String::from_utf8_lossy(text).parse().expect("int text")
                }
                other => panic!("expected DataRow, got {other:?}"),
            })
            .collect()
    }

    fn binary_ids(messages: &[BackendMessage]) -> Vec<i64> {
        messages
            .iter()
            .map(|m| match m {
                BackendMessage::DataRow { columns } => {
                    let bytes = columns[0].as_ref().expect("non-null");
                    i64::from(i32::from_be_bytes(bytes[..4].try_into().expect("int4")))
                }
                other => panic!("expected DataRow, got {other:?}"),
            })
            .collect()
    }

    /// A catalog that records the thread its `resolve` ran on. `resolve` is
    /// called from inside the executor, so that thread IS the executor's.
    struct ThreadRecordingCatalog(Arc<std::sync::Mutex<Option<std::thread::ThreadId>>>);

    impl Catalog for ThreadRecordingCatalog {
        fn resolve(&self, _schema: &str, _table: &str) -> Option<ferrosa_sql::SharedTable> {
            *self.0.lock().expect("recorder lock") = Some(std::thread::current().id());
            None // absent table: resolve still ran, which is what we observe
        }
    }

    /// The synchronous executor must not run on the caller's async worker, or
    /// one large query pins a runtime worker and starves keepalives (PR #131,
    /// t_d3b2dec1). One worker makes an inline call unmistakable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn executor_does_not_run_on_the_async_worker() {
        let caller = std::thread::current().id();
        let recorder = Arc::new(std::sync::Mutex::new(None));
        let opened = open_stream(
            select("SELECT id FROM missing"),
            ThreadRecordingCatalog(Arc::clone(&recorder)),
            ScanFailure::default(),
            "public".to_string(),
            Vec::new(),
        )
        .await;
        assert!(opened.is_err(), "the table is absent, so the query fails");
        let ran_on = recorder
            .lock()
            .expect("recorder lock")
            .expect("the executor called Catalog::resolve");
        assert_ne!(caller, ran_on, "the executor ran on the async worker");
    }

    /// A limit suspends, and the next call resumes with no gap or duplicate,
    /// including when the limit splits a batch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn suspension_resumes_exactly_where_it_stopped() {
        let (mut stream, _) = open(100, None).await;
        let mut seen = Vec::new();
        // 7 does not divide RESULT_BATCH_ROWS, so batches are split mid-way.
        loop {
            let mut out: Vec<BackendMessage> = Vec::new();
            let end = stream.pump(Some(7), &[], &mut out).await.expect("pump");
            seen.extend(text_ids(&out));
            match end {
                PumpEnd::Suspended => {}
                PumpEnd::Complete { rows } => {
                    assert_eq!(rows, 100 % 7, "the last call reports only its own rows");
                    break;
                }
                PumpEnd::Failed(e) => panic!("unexpected failure {e:?}"),
            }
        }
        assert_eq!(seen, (0..100).collect::<Vec<_>>());
    }

    /// A suspended stream holds no blocking thread. With a blocking pool of
    /// one thread, a stream parked in a send would leave nothing for anyone
    /// else; here another blocking task runs while several streams sit
    /// suspended mid-result.
    #[test]
    fn a_suspended_stream_holds_no_blocking_thread() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let outcome = rt.block_on(async {
            let mut suspended = Vec::new();
            // Opening the next stream needs a blocking thread too, so a stream
            // that kept one stalls the loop: every step has a deadline.
            let step = Duration::from_secs(10);
            for _ in 0..4 {
                let Ok((mut stream, _)) = tokio::time::timeout(step, open(100_000, None)).await
                else {
                    return Err(format!(
                        "opening a stream stalled beside {} suspended streams",
                        suspended.len()
                    ));
                };
                let mut out: Vec<BackendMessage> = Vec::new();
                let end = stream.pump(Some(1), &[], &mut out).await.expect("pump");
                assert!(matches!(end, PumpEnd::Suspended), "got {end:?}");
                suspended.push(stream);
            }
            let probe = tokio::task::spawn_blocking(|| std::thread::current().id());
            match tokio::time::timeout(step, probe).await {
                Ok(_) => Ok(()),
                Err(_) => Err(format!(
                    "a blocking task could not run beside {} suspended streams",
                    suspended.len()
                )),
            }
        });
        // A thread parked forever must fail the test, not hang its teardown.
        rt.shutdown_timeout(Duration::from_secs(1));
        if let Err(message) = outcome {
            panic!("{message}");
        }
    }

    /// Dropping the stream (portal Close, disconnect) stops the executor: the
    /// scan is not run to the end of a table nobody is reading.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_the_stream_stops_the_producer() {
        let (stream, produced) = open(10_000_000, None).await;
        drop(stream);
        let mut last = usize::MAX;
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let now = produced.load(Ordering::SeqCst);
            if now == last {
                break;
            }
            last = now;
        }
        let stopped_at = produced.load(Ordering::SeqCst);
        assert!(
            stopped_at < 1_000,
            "the producer ran {stopped_at} rows after its consumer was dropped"
        );
    }

    /// A failure after rows were sent yields those rows, then an error, and
    /// never a completion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_mid_stream_error_follows_the_rows_it_interrupts() {
        let (mut stream, _) = open(50, Some(20)).await;
        let mut out: Vec<BackendMessage> = Vec::new();
        // Binary format: the numeric at row 20 cannot be encoded.
        let end = stream.pump(None, &[1], &mut out).await.expect("pump");
        assert_eq!(binary_ids(&out), (0..20).collect::<Vec<_>>());
        match end {
            PumpEnd::Failed(BackendMessage::ErrorResponse { .. }) => {}
            other => panic!("expected a trailing ErrorResponse, got {other:?}"),
        }
    }
}
