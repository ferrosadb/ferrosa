//! Module: Stream query results from the executor to the wire, O(batch) not
//! O(result).
//! Correctness: Correct when (1) no more than a bounded window of rows is ever
//! resident between the executor and the socket, whatever the result size;
//! (2) every row the executor yields is delivered exactly once, in order, across
//! any number of `Execute` calls on a suspended portal; and (3) a failure after
//! rows were sent surfaces as an `ErrorResponse`, never as a `CommandComplete`.
//! Last revised: 2026-09-28
//! Last changed: Created — replaces the materialize-then-render path
//!   (t_f348ba0b, FMEA PG-Tf348ba0b).
//!
//! # Shape
//!
//! `ferrosa_sql::execute_streaming` is synchronous and pull-based, so it runs on
//! a blocking thread (as `offload` did for the collecting call) and pushes its
//! output through a **bounded** `tokio::sync::mpsc` channel of row *batches*.
//! The async side pulls a batch at a time, encodes it to `DataRow`s and writes
//! them to the socket. Three things bound memory:
//!
//! - the producer holds at most one batch while it fills it,
//! - the channel holds [`RESULT_CHANNEL_BATCHES`] batches, and
//! - a suspended portal holds at most the unsent remainder of one batch.
//!
//! Backpressure is the channel: while the socket (or a suspended portal) is not
//! draining, the producer blocks in `blocking_send` and the pipeline does not
//! advance. Dropping the [`ResultStream`] (portal `Close`, disconnect, session
//! end) closes the receiver; the producer's next send fails and it stops, which
//! drops the scan and, through it, the storage producer.
//!
//! # Cost of suspension
//!
//! A suspended portal parks one `spawn_blocking` thread in `blocking_send`. The
//! runtimes cap blocking threads (`max_blocking_threads`), so many concurrently
//! suspended portals consume that budget until they are closed or the
//! transaction ends. This is the price of not re-running the query per `Execute`
//! and is recorded in FMEA PG-Tf348ba0b.

use std::collections::VecDeque;
use std::io;
use std::ops::ControlFlow;

use ferrosa_sql::{Catalog, Column, ColumnType, ExecError, Row, RowSink, SelectStmt, SpillCtx};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::messages::BackendMessage;
use crate::query::{
    check_scan_failure, encode_data_row, error_response, exec_error_response, ReplySink,
};
use crate::storage_provider::ScanFailure;

/// Rows per batch handed from the executor thread to the async side.
pub(crate) const RESULT_BATCH_ROWS: usize = 16;

/// Batches the channel holds; with [`RESULT_BATCH_ROWS`] this is the whole
/// in-flight window between executor and socket.
pub(crate) const RESULT_CHANNEL_BATCHES: usize = 2;

/// One message from the executor thread to the async consumer.
enum Item {
    Columns(Vec<Column>),
    Rows(Vec<Row>),
    /// The pipeline ran to completion.
    Done,
    /// The pipeline failed; rows already sent are an incomplete result.
    Failed(ExecError),
}

/// [`RowSink`] that batches rows into a bounded channel.
struct ChannelSink {
    tx: mpsc::Sender<Item>,
    batch: Vec<Row>,
}

impl ChannelSink {
    fn new(tx: mpsc::Sender<Item>) -> Self {
        Self {
            tx,
            batch: Vec::with_capacity(RESULT_BATCH_ROWS),
        }
    }

    /// Send one item, blocking while the channel is full. `Break` means the
    /// consumer dropped the stream.
    fn send(&self, item: Item) -> ControlFlow<()> {
        match self.tx.blocking_send(item) {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
    }

    fn flush(&mut self) -> ControlFlow<()> {
        if self.batch.is_empty() {
            return ControlFlow::Continue(());
        }
        let full = std::mem::replace(&mut self.batch, Vec::with_capacity(RESULT_BATCH_ROWS));
        self.send(Item::Rows(full))
    }

    /// Flush the partial batch, then send the terminal item.
    fn finish(&mut self, end: Item) -> ControlFlow<()> {
        self.flush()?;
        self.send(end)
    }
}

impl RowSink for ChannelSink {
    fn columns(&mut self, columns: &[Column]) -> ControlFlow<()> {
        self.send(Item::Columns(columns.to_vec()))
    }

    fn row(&mut self, row: Row) -> ControlFlow<()> {
        self.batch.push(row);
        if self.batch.len() >= RESULT_BATCH_ROWS {
            self.flush()
        } else {
            ControlFlow::Continue(())
        }
    }
}

/// Body of the blocking producer thread.
fn run_producer<C: Catalog>(
    stmt: &SelectStmt,
    catalog: &C,
    default_schema: &str,
    params: &[ferrosa_sql::Value],
    tx: mpsc::Sender<Item>,
) {
    let mut sink = ChannelSink::new(tx);
    let outcome = ferrosa_sql::execute_streaming(
        stmt,
        catalog,
        default_schema,
        params,
        &SpillCtx::default(),
        &mut sink,
    );
    let end = match outcome {
        Ok(()) => Item::Done,
        Err(error) => Item::Failed(error),
    };
    if sink.finish(end).is_break() {
        // Designed and observable: the consumer closed the portal or the
        // connection went away, so nobody is left to read the rest.
        tracing::debug!("result consumer went away before the stream ended");
    }
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
    rx: mpsc::Receiver<Item>,
    producer: JoinHandle<()>,
    /// Unsent remainder of the last batch received.
    buffered: VecDeque<Row>,
    failure: ScanFailure,
}

/// Start `stmt` on a blocking thread and wait for its column metadata, so a
/// resolution error is reported before any output.
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
    let (tx, rx) = mpsc::channel(RESULT_CHANNEL_BATCHES);
    let producer = tokio::task::spawn_blocking(move || {
        run_producer(&stmt, &catalog, &default_schema, &params, tx);
    });
    let mut stream = ResultStream {
        columns: Vec::new(),
        rx,
        producer,
        buffered: VecDeque::new(),
        failure,
    };
    match stream.rx.recv().await {
        Some(Item::Columns(columns)) => {
            stream.columns = columns;
            Ok(stream)
        }
        Some(Item::Failed(error)) => Err(stream.failure_or(&error)),
        Some(Item::Rows(_) | Item::Done) => Err(error_response(
            "XX000",
            "internal error: query stream began without column metadata",
        )),
        None => Err(stream.producer_died().await),
    }
}

impl ResultStream {
    /// The output columns, known before the first row.
    pub(crate) fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// A recorded storage failure outranks an executor error computed from the
    /// truncated scan beneath it.
    fn failure_or(&self, error: &ExecError) -> BackendMessage {
        check_scan_failure(&self.failure).unwrap_or_else(|| exec_error_response(error))
    }

    /// The channel closed with no terminal item: the executor thread died.
    async fn producer_died(&mut self) -> BackendMessage {
        match (&mut self.producer).await {
            Err(join_error) if join_error.is_panic() => {
                std::panic::resume_unwind(join_error.into_panic())
            }
            Err(join_error) => error_response(
                "XX000",
                &format!("internal error: query executor was cancelled: {join_error}"),
            ),
            Ok(()) => error_response(
                "XX000",
                "internal error: query stream ended without completing",
            ),
        }
    }

    async fn next_batch(&mut self) -> Batch {
        match self.rx.recv().await {
            Some(Item::Rows(rows)) => {
                self.buffered.extend(rows);
                Batch::Rows
            }
            Some(Item::Done) => match check_scan_failure(&self.failure) {
                // A scan that died closed its channel, so the executor finished
                // "successfully" over a short row set. Never report that as done.
                Some(error) => Batch::Failed(error),
                None => Batch::End,
            },
            Some(Item::Failed(error)) => Batch::Failed(self.failure_or(&error)),
            Some(Item::Columns(_)) => Batch::Failed(error_response(
                "XX000",
                "internal error: query stream repeated its column metadata",
            )),
            None => Batch::Failed(self.producer_died().await),
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
                Err(error) => return (messages, Some(error_response("22003", &error))),
            }
        }
        (messages, None)
    }

    /// Stop the executor: close the receiver so its next send fails.
    fn abandon(&mut self) {
        self.rx.close();
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

        fn scan(&self) -> Box<dyn Iterator<Item = Row> + '_> {
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
