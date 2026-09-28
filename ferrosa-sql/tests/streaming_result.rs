//! Streaming result delivery (forge t_f348ba0b, FMEA SQL-Tf348ba0b).
//!
//! The executor's operators already stream and spill (t_50d99192, proven in
//! `spill_operators.rs`). The last materialization was the boundary: the final
//! rows were collected into `QueryResult.rows` before the caller saw the first
//! one. `execute_streaming` hands each row to a [`RowSink`] as the pipeline
//! yields it, so a caller that forwards rows (the Postgres wire front end) holds
//! O(batch) rather than O(result).
//!
//! What these tests pin, none of which a collecting API can satisfy:
//!
//! 1. A plain scan never runs ahead of its consumer: the number of rows the
//!    provider has produced but the sink has not yet received stays constant
//!    whatever the table size.
//! 2. A blocking operator (ORDER BY) over an input far above the spill threshold
//!    still delivers every row, with peak resident rows an order of magnitude
//!    below the row count.
//! 3. A sink that stops early stops the scan: the pipeline is pulled lazily, so
//!    a client that closes a portal does not pay for the rest of the table.

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ferrosa_sql::spill::{DirReserver, SpillCtx};
use ferrosa_sql::{
    execute_streaming, parse_statement, Column, ColumnType, MapCatalog, RelSchema, Row, RowSink,
    SelectStmt, Statement, TableProvider, Value,
};

/// A table that yields `n` single-int rows and counts how many it has produced.
struct CountingTable {
    schema: RelSchema,
    n: i64,
    produced: Arc<AtomicUsize>,
}

impl TableProvider for CountingTable {
    fn schema(&self) -> &RelSchema {
        &self.schema
    }

    fn scan(&self) -> Box<dyn Iterator<Item = Row> + '_> {
        let produced = Arc::clone(&self.produced);
        // Descending so an ORDER BY has real work to do.
        Box::new((0..self.n).rev().map(move |i| {
            produced.fetch_add(1, Ordering::SeqCst);
            Row::new(vec![Value::Int(i)])
        }))
    }
}

fn catalog(n: i64, produced: &Arc<AtomicUsize>) -> MapCatalog {
    let schema = RelSchema::new(vec![Column::new("id", ColumnType::Int)]);
    MapCatalog::new().with_table(
        "public",
        "t",
        Arc::new(CountingTable {
            schema,
            n,
            produced: Arc::clone(produced),
        }),
    )
}

fn select(sql: &str) -> SelectStmt {
    match parse_statement(sql).expect("parses") {
        Statement::Select(s) => *s,
        other => panic!("expected SELECT, got {other:?}"),
    }
}

/// Records, per delivered row, how far the provider has run ahead of the sink.
struct LeadSink {
    produced: Arc<AtomicUsize>,
    received: usize,
    max_lead: usize,
    stop_after: Option<usize>,
    saw_columns: bool,
}

impl LeadSink {
    fn new(produced: &Arc<AtomicUsize>, stop_after: Option<usize>) -> Self {
        Self {
            produced: Arc::clone(produced),
            received: 0,
            max_lead: 0,
            stop_after,
            saw_columns: false,
        }
    }
}

impl RowSink for LeadSink {
    fn columns(&mut self, columns: &[Column]) -> ControlFlow<()> {
        assert_eq!(columns.len(), 1, "columns arrive before any row");
        assert_eq!(self.received, 0, "columns arrive before any row");
        self.saw_columns = true;
        ControlFlow::Continue(())
    }

    fn row(&mut self, _row: Row) -> ControlFlow<()> {
        self.received += 1;
        let lead = self.produced.load(Ordering::SeqCst) - self.received;
        self.max_lead = self.max_lead.max(lead);
        match self.stop_after {
            Some(n) if self.received >= n => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        }
    }
}

#[test]
fn scan_never_runs_ahead_of_its_consumer() {
    const N: i64 = 5_000;
    let produced = Arc::new(AtomicUsize::new(0));
    let cat = catalog(N, &produced);
    let mut sink = LeadSink::new(&produced, None);

    execute_streaming(
        &select("SELECT id FROM t"),
        &cat,
        "public",
        &[],
        &SpillCtx::default(),
        &mut sink,
    )
    .expect("query runs");

    assert!(sink.saw_columns);
    assert_eq!(sink.received as i64, N, "every row is delivered");
    assert!(
        sink.max_lead <= 2,
        "the pipeline buffered {} rows ahead of the sink; a streaming boundary holds O(1)",
        sink.max_lead
    );
}

#[test]
fn order_by_over_a_spilling_input_delivers_every_row_within_the_budget() {
    const N: i64 = 4_000;
    let dir = tempfile::tempdir().expect("tempdir");
    let ctx = SpillCtx::new(Arc::new(DirReserver::new(dir.path())), 256);
    let produced = Arc::new(AtomicUsize::new(0));
    let cat = catalog(N, &produced);
    let mut sink = LeadSink::new(&produced, None);

    execute_streaming(
        &select("SELECT id FROM t ORDER BY id"),
        &cat,
        "public",
        &[],
        &ctx,
        &mut sink,
    )
    .expect("query runs");

    assert_eq!(sink.received as i64, N, "spilling must not truncate");
    assert!(ctx.stats().spilled(), "the tiny budget must force a spill");
    assert!(
        ctx.stats().max_resident_rows() < (N as usize) / 10,
        "peak resident rows {} must stay an order of magnitude below {N}",
        ctx.stats().max_resident_rows()
    );
}

#[test]
fn a_sink_that_stops_early_stops_the_scan() {
    const N: i64 = 5_000;
    let produced = Arc::new(AtomicUsize::new(0));
    let cat = catalog(N, &produced);
    let mut sink = LeadSink::new(&produced, Some(10));

    execute_streaming(
        &select("SELECT id FROM t"),
        &cat,
        "public",
        &[],
        &SpillCtx::default(),
        &mut sink,
    )
    .expect("an early stop is not an error");

    assert_eq!(sink.received, 10);
    assert!(
        produced.load(Ordering::SeqCst) <= 12,
        "the scan produced {} rows for a consumer that took 10",
        produced.load(Ordering::SeqCst)
    );
}
