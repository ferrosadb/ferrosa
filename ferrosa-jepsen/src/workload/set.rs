//! Module: the `set` workload — unique integers inserted across buckets, a mandatory final read,
//! and a verdict from the durability book.
//! Correctness: correct when every invoked INSERT lands in `attempted`, a success in
//! `acknowledged`, a timeout or post-send error in `indeterminate`, a definite refusal in neither;
//! when the final read pages every bucket, holding one page at a time; and when a bucket that
//! cannot be read is a run failure ("final read failed: bucket N"), never a verdict.
//! Last revised: 2026-10-31
//! Last changed: New module (packet JE-5, decisions D7–D9 of `specs/jepsen-evidence`).
//!
//! Schema: `jepsen.set (bucket int, v bigint, PRIMARY KEY (bucket, v))`. A run draws a random
//! nonce at construction and writes values `nonce << 32 | n` for `n = 0, 1, …`, so its values are
//! unique and the final read is scoped to this run's range: rows a previous run left in the table
//! are outside it and can neither be recovered nor mistaken for a fabrication.
//!
//! The book is derived from the recorded history, not kept beside it: every INSERT is an
//! `Op::Write { key: "set-<bucket>", value }`, and its result encodes the outcome. A definite
//! refusal is recorded as `OpResult::Err` whose message starts with [`DEFINITE_FAILURE`]; every
//! other error, and a timeout, is indeterminate. The conservative reading of an unrecognised error
//! is indeterminate: it may excuse a recovered value, but it can never hide a loss
//! (`an_indeterminate_write_never_hides_a_loss`), and calling a write that did land "definite"
//! would convict the database of a fabrication it did not commit.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};

use crate::checker::book::{Book, Multiset};
use crate::checker::profile::{ConsistencyProfile, Level};
use crate::history::{History, HistoryRecorder, Op, OpResult};

use super::{CqlSession, DurabilityVerdict, Workload};

/// The table the workload writes and reads back.
pub const TABLE: &str = "jepsen.set";
/// History key prefix; the bucket number follows it.
const KEY_PREFIX: &str = "set-";
/// Message prefix that marks a recorded INSERT error as a definite refusal.
pub const DEFINITE_FAILURE: &str = "definite failure: ";
/// First retry delay of a failed final-read page; it doubles per failure in the same bucket.
const FINAL_READ_BACKOFF: Duration = Duration::from_millis(250);
/// Values per run: the low 32 bits of a value are the run's counter.
const VALUES_PER_RUN: u64 = 1 << 32;

/// The workload's parameters (decisions table of `specs/jepsen-evidence`). Each is read the way
/// [`ConsistencyProfile::from_vars`] reads its variables: unset takes the default, and a value
/// that does not parse is refused naming the variable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetParams {
    /// `SET_BUCKETS`: partitions the values are spread across.
    pub buckets: u32,
    /// `FINAL_READ_SETTLE` (seconds): wait between healing and the final read.
    pub settle: Duration,
    /// `FINAL_READ_PAGE`: rows per final-read page.
    pub page: u32,
    /// `FINAL_READ_RETRIES`: failed page reads tolerated per bucket before the run fails.
    pub retries: u32,
}

impl Default for SetParams {
    fn default() -> Self {
        Self {
            buckets: 16,
            settle: Duration::from_secs(10),
            page: 1000,
            retries: 5,
        }
    }
}

impl SetParams {
    pub fn from_env() -> Result<Self, String> {
        Self::from_vars(|key| std::env::var(key).ok())
    }

    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let mut p = SetParams::default();
        if let Some(n) = parse_u32(&var, "SET_BUCKETS", 1)? {
            p.buckets = n;
        }
        if let Some(secs) = parse_u32(&var, "FINAL_READ_SETTLE", 0)? {
            p.settle = Duration::from_secs(u64::from(secs));
        }
        if let Some(n) = parse_u32(&var, "FINAL_READ_PAGE", 1)? {
            p.page = n;
        }
        if let Some(n) = parse_u32(&var, "FINAL_READ_RETRIES", 0)? {
            p.retries = n;
        }
        Ok(p)
    }
}

/// `Ok(None)` when unset, `Ok(Some(n))` when set to an integer `>= min`, else an error naming `key`.
fn parse_u32(
    var: &impl Fn(&str) -> Option<String>,
    key: &str,
    min: u32,
) -> Result<Option<u32>, String> {
    let Some(raw) = var(key) else {
        return Ok(None);
    };
    match raw.trim().parse::<u32>() {
        Ok(n) if n >= min => Ok(Some(n)),
        _ => Err(format!("{key}={raw:?} is not an integer >= {min}")),
    }
}

/// How an INSERT ended, from the client's point of view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The database acknowledged the write.
    Acknowledged,
    /// The write timed out or failed after being sent; it may or may not have applied.
    Indeterminate,
    /// The database refused the write before applying it.
    DefiniteFailure,
}

/// Classify a failed INSERT. Only a refusal the protocol defines as "not applied" is definite:
/// an invalid or unauthorised statement, `Unavailable` (the coordinator saw too few replicas and
/// did not start the write), `Overloaded`, `IsBootstrapping`, a rate limit, and errors the driver
/// raises before anything is sent (no connection, an empty plan, a serialisation failure).
/// Everything else — a write timeout or failure, a server error, a broken connection, a client
/// timeout, or an error this function does not recognise — is indeterminate.
pub fn classify_write_error(err: &anyhow::Error) -> WriteOutcome {
    match err.downcast_ref::<ExecutionError>() {
        Some(e) if refused_before_applying(e) => WriteOutcome::DefiniteFailure,
        _ => WriteOutcome::Indeterminate,
    }
}

/// Whether the driver or the coordinator guarantees `e` left the write unapplied.
fn refused_before_applying(e: &ExecutionError) -> bool {
    match e {
        ExecutionError::BadQuery(_)
        | ExecutionError::EmptyPlan
        | ExecutionError::PrepareError(_)
        | ExecutionError::ConnectionPoolError(_) => true,
        ExecutionError::LastAttemptError(attempt) => match attempt {
            RequestAttemptError::SerializationError(_)
            | RequestAttemptError::CqlRequestSerialization(_)
            | RequestAttemptError::UnableToAllocStreamId => true,
            RequestAttemptError::DbError(db, _) => matches!(
                db,
                DbError::SyntaxError
                    | DbError::Invalid
                    | DbError::AuthenticationError
                    | DbError::Unauthorized
                    | DbError::ConfigError
                    | DbError::Unavailable { .. }
                    | DbError::Overloaded
                    | DbError::IsBootstrapping
                    | DbError::RateLimitReached { .. }
                    | DbError::Unprepared { .. }
            ),
            _ => false,
        },
        _ => false,
    }
}

/// The history result that records a failed INSERT's `outcome`.
fn recorded_error(outcome: WriteOutcome, err: &anyhow::Error) -> OpResult {
    match outcome {
        WriteOutcome::DefiniteFailure => OpResult::Err(format!("{DEFINITE_FAILURE}{err:#}")),
        WriteOutcome::Acknowledged | WriteOutcome::Indeterminate => {
            OpResult::Err(format!("{err:#}"))
        }
    }
}

/// The book's write side, from the recorded history: `attempted`, `acknowledged` and
/// `indeterminate`. `recovered` is left empty for the final read to fill. A set INSERT recorded
/// with a result no INSERT can produce is refused rather than guessed at.
pub fn book_from_history(history: &History) -> Result<Book> {
    let mut book = Book::default();
    let set_writes = history.operations.iter().filter_map(|op| match &op.op {
        Op::Write { key, value } if key.starts_with(KEY_PREFIX) => Some((*value, &op.result)),
        _ => None,
    });
    for (value, result) in set_writes {
        book.attempted.add(value);
        match result {
            OpResult::Ok => book.acknowledged.add(value),
            OpResult::Err(m) if m.starts_with(DEFINITE_FAILURE) => {}
            OpResult::Err(_) | OpResult::Timeout => book.indeterminate.add(value),
            other => bail!("set INSERT of {value} recorded an impossible result {other:?}"),
        }
    }
    Ok(book)
}

/// One page of a bucket's values.
#[async_trait]
pub trait BucketReader: Send + Sync {
    /// Up to `limit` values of `bucket`, strictly greater than `after`, in ascending order.
    async fn read_page(&self, bucket: u32, after: i64, limit: u32) -> Result<Vec<i64>>;
}

/// The final read's scope and pacing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPlan {
    pub buckets: u32,
    pub page: u32,
    pub retries: u32,
    /// This run's values: `[low, high)`.
    pub low: i64,
    pub high: i64,
    /// First retry delay; doubles per failure within a bucket.
    pub backoff: Duration,
}

/// Read every bucket into `recovered`, one page at a time (D8). A bucket whose page reads fail
/// more than `plan.retries` times is a run failure naming the bucket.
pub async fn read_all_buckets(
    reader: &dyn BucketReader,
    plan: &ReadPlan,
    recovered: &mut Multiset,
) -> Result<()> {
    for bucket in 0..plan.buckets {
        read_bucket(reader, plan, bucket, recovered)
            .await
            .with_context(|| format!("final read failed: bucket {bucket}"))?;
    }
    Ok(())
}

/// Page through one bucket. Terminates: every accepted page strictly raises `after` within the
/// finite range `[low, high)`, and failures are capped at `plan.retries + 1`.
async fn read_bucket(
    reader: &dyn BucketReader,
    plan: &ReadPlan,
    bucket: u32,
    recovered: &mut Multiset,
) -> Result<()> {
    let mut after = plan.low - 1;
    let mut failures: u32 = 0;
    loop {
        let page = match reader.read_page(bucket, after, plan.page).await {
            Ok(page) => page,
            Err(e) if failures < plan.retries => {
                failures += 1;
                let delay = plan.backoff.saturating_mul(1 << (failures - 1).min(16));
                tracing::warn!(
                    bucket,
                    attempt = failures,
                    retries = plan.retries,
                    delay_ms = delay.as_millis() as u64,
                    error = %format!("{e:#}"),
                    "set: final-read page failed; retrying"
                );
                tokio::time::sleep(delay).await;
                continue;
            }
            Err(e) => {
                return Err(e.context(format!(
                    "page after {after} still failing after {} retries",
                    plan.retries
                )))
            }
        };
        if page.len() > plan.page as usize {
            bail!(
                "page returned {} rows, more than the {} asked for",
                page.len(),
                plan.page
            );
        }
        let full = page.len() == plan.page as usize;
        for v in page {
            if v <= after || v >= plan.high {
                bail!(
                    "page returned {v}, outside ({after}, {}): the read is not paging this run's \
                     values in order",
                    plan.high
                );
            }
            recovered.add(v);
            after = v;
        }
        if !full {
            return Ok(());
        }
    }
}

/// The final read and the verdict: the write side from `history`, `recovered` from `reader`,
/// then [`Book::verdict`] under `profile`.
pub async fn verdict_after_final_read(
    reader: &dyn BucketReader,
    plan: &ReadPlan,
    history: &History,
    profile: &ConsistencyProfile,
) -> Result<DurabilityVerdict> {
    let mut book = book_from_history(history)?;
    read_all_buckets(reader, plan, &mut book.recovered).await?;
    Ok(DurabilityVerdict {
        summary: book.summary(),
        assurance: book.verdict(profile),
    })
}

/// [`BucketReader`] over CQL: a clustering-range query per page, at the profile's read level.
pub struct CqlBucketReader<'a> {
    pub session: &'a dyn CqlSession,
    pub level: Level,
    /// Exclusive upper bound of this run's values.
    pub high: i64,
}

#[async_trait]
impl BucketReader for CqlBucketReader<'_> {
    async fn read_page(&self, bucket: u32, after: i64, limit: u32) -> Result<Vec<i64>> {
        let query = format!(
            "SELECT v FROM {TABLE} WHERE bucket = {bucket} AND v > {after} AND v < {high} \
             LIMIT {limit}",
            high = self.high
        );
        let rows = self.session.execute_at(&query, self.level).await?;
        let mut values = Vec::with_capacity(rows.len());
        for row in &rows {
            let (_, raw) = row
                .first()
                .with_context(|| format!("final read row has no column: {query}"))?;
            let v = raw
                .parse::<i64>()
                .with_context(|| format!("final read value {raw:?} is not a bigint: {query}"))?;
            values.push(v);
        }
        Ok(values)
    }
}

/// Inserts unique integers across `SET_BUCKETS` partitions, then reads them all back.
pub struct SetWorkload {
    /// High bits of every value this instance writes (`1..2^30`, so values stay positive).
    nonce: i64,
}

impl Default for SetWorkload {
    fn default() -> Self {
        Self::new()
    }
}

impl SetWorkload {
    pub fn new() -> Self {
        Self::with_nonce(i64::from(rand::random::<u32>() % ((1 << 30) - 1)) + 1)
    }

    pub fn with_nonce(nonce: i64) -> Self {
        assert!(
            (1..(1 << 30)).contains(&nonce),
            "set nonce {nonce} must be in 1..2^30"
        );
        Self { nonce }
    }

    /// This run's values: `[low, high)`.
    pub fn range(&self) -> (i64, i64) {
        (self.nonce << 32, (self.nonce + 1) << 32)
    }
}

#[async_trait]
impl Workload for SetWorkload {
    fn name(&self) -> &str {
        "set"
    }

    /// The history is a set of unique-value inserts, not a register: durability is judged by the
    /// book and the final read.
    fn register_linearizable(&self) -> bool {
        false
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session
            .execute(
                "CREATE KEYSPACE IF NOT EXISTS jepsen \
                 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
            )
            .await?;
        session
            .execute(&format!(
                "CREATE TABLE IF NOT EXISTS {TABLE} \
                 (bucket int, v bigint, PRIMARY KEY (bucket, v))"
            ))
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let params = SetParams::from_env().map_err(anyhow::Error::msg)?;
        let profile = ConsistencyProfile::from_env().map_err(anyhow::Error::msg)?;
        let (low, _) = self.range();
        let start = Instant::now();
        // Pacing floor, as in `bank`: a fault can make every INSERT fail instantly, and without a
        // floor the loop would append failed ops at CPU speed. It also bounds the book, whose
        // size is the number of attempted writes.
        const MIN_ITER: Duration = Duration::from_millis(1);
        let mut n: u64 = 0;
        while start.elapsed() < duration {
            tokio::time::sleep(MIN_ITER).await;
            if n >= VALUES_PER_RUN {
                bail!(
                    "set workload exhausted its {VALUES_PER_RUN} values for nonce {}",
                    self.nonce
                );
            }
            let bucket = n % u64::from(params.buckets);
            let value = low + i64::try_from(n).context("set counter exceeds i64")?;
            n += 1;
            recorder.invoke(Op::Write {
                key: format!("{KEY_PREFIX}{bucket}"),
                value,
            });
            let insert = format!("INSERT INTO {TABLE} (bucket, v) VALUES ({bucket}, {value})");
            let result = match session.execute_at(&insert, profile.write_level).await {
                Ok(_) => OpResult::Ok,
                Err(e) => recorded_error(classify_write_error(&e), &e),
            };
            recorder.complete(result);
        }
        Ok(())
    }

    /// The book is the check; there is no separate history invariant.
    fn check_invariant(&self, _history: &History) -> Result<()> {
        Ok(())
    }

    async fn final_read(
        &self,
        session: &dyn CqlSession,
        history: &History,
        profile: &ConsistencyProfile,
    ) -> Result<Option<DurabilityVerdict>> {
        let params = SetParams::from_env().map_err(anyhow::Error::msg)?;
        let (low, high) = self.range();
        tracing::info!(
            settle_secs = params.settle.as_secs(),
            buckets = params.buckets,
            page = params.page,
            "set: faults healed; settling before the final read"
        );
        tokio::time::sleep(params.settle).await;
        let plan = ReadPlan {
            buckets: params.buckets,
            page: params.page,
            retries: params.retries,
            low,
            high,
            backoff: FINAL_READ_BACKOFF,
        };
        let reader = CqlBucketReader {
            session,
            level: profile.read_level,
            high,
        };
        let verdict = verdict_after_final_read(&reader, &plan, history, profile).await?;
        tracing::info!(
            assurance = verdict.assurance.as_str(),
            attempted = verdict.summary.attempted,
            acknowledged = verdict.summary.acknowledged,
            indeterminate = verdict.summary.indeterminate,
            recovered = verdict.summary.recovered,
            lost = verdict.summary.lost,
            fabricated = verdict.summary.fabricated,
            recovered_indeterminate = verdict.summary.recovered_indeterminate,
            "set: final read complete"
        );
        Ok(Some(verdict))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU32, Ordering};

    use scylla::errors::WriteType;
    use scylla::statement::Consistency;

    use super::*;
    use crate::checker::profile::{Assurance, NotTested};
    use crate::history::Operation;

    const LOW: i64 = 7 << 32;

    fn plan(buckets: u32, page: u32, retries: u32) -> ReadPlan {
        ReadPlan {
            buckets,
            page,
            retries,
            low: LOW,
            high: LOW + (1 << 32),
            backoff: Duration::ZERO,
        }
    }

    fn quorum() -> ConsistencyProfile {
        ConsistencyProfile::default()
    }

    fn write(bucket: u32, value: i64, result: OpResult) -> Operation {
        Operation {
            client_id: "set".into(),
            invoke_us: 1,
            complete_us: 2,
            op: Op::Write {
                key: format!("{KEY_PREFIX}{bucket}"),
                value,
            },
            result,
        }
    }

    fn acked(bucket: u32, value: i64) -> Operation {
        write(bucket, value, OpResult::Ok)
    }

    fn timed_out(bucket: u32, value: i64) -> Operation {
        write(bucket, value, OpResult::Timeout)
    }

    fn refused(bucket: u32, value: i64) -> Operation {
        let e = anyhow::anyhow!("Unavailable");
        write(
            bucket,
            value,
            recorded_error(WriteOutcome::DefiniteFailure, &e),
        )
    }

    /// A table in memory: each bucket's values, served in ascending pages. Asserts the reader
    /// never asks for more than the plan's page.
    struct Table {
        buckets: BTreeMap<u32, Vec<i64>>,
        page: u32,
        /// Bucket whose reads fail, and how many times before they succeed (`u32::MAX`: always).
        failing: Option<(u32, u32)>,
        failures: AtomicU32,
    }

    impl Table {
        fn new(page: u32, rows: &[(u32, i64)]) -> Self {
            let mut buckets: BTreeMap<u32, Vec<i64>> = BTreeMap::new();
            for (b, v) in rows {
                buckets.entry(*b).or_default().push(*v);
            }
            for vs in buckets.values_mut() {
                vs.sort_unstable();
            }
            Self {
                buckets,
                page,
                failing: None,
                failures: AtomicU32::new(0),
            }
        }

        fn failing(mut self, bucket: u32, times: u32) -> Self {
            self.failing = Some((bucket, times));
            self
        }
    }

    #[async_trait]
    impl BucketReader for Table {
        async fn read_page(&self, bucket: u32, after: i64, limit: u32) -> Result<Vec<i64>> {
            assert_eq!(
                limit, self.page,
                "the reader must page at the plan's page size"
            );
            if let Some((b, times)) = self.failing {
                if b == bucket && self.failures.load(Ordering::SeqCst) < times {
                    self.failures.fetch_add(1, Ordering::SeqCst);
                    bail!("read timeout on bucket {bucket}");
                }
            }
            Ok(self
                .buckets
                .get(&bucket)
                .map(|vs| {
                    vs.iter()
                        .copied()
                        .filter(|v| *v > after)
                        .take(limit as usize)
                        .collect()
                })
                .unwrap_or_default())
        }
    }

    fn history(ops: Vec<Operation>) -> History {
        History { operations: ops }
    }

    #[tokio::test]
    async fn a_loss_and_a_fabrication_are_reported_separately() {
        // 1 and 2 acknowledged; 3 refused definitively. The read returns 1 and 3.
        let h = history(vec![
            acked(0, LOW + 1),
            acked(1, LOW + 2),
            refused(0, LOW + 3),
        ]);
        let table = Table::new(10, &[(0, LOW + 1), (0, LOW + 3)]);
        let v = verdict_after_final_read(&table, &plan(2, 10, 0), &h, &quorum())
            .await
            .expect("final read succeeds");
        assert_eq!(v.summary.lost, 1);
        assert_eq!(v.summary.lost_sample, vec![LOW + 2]);
        assert_eq!(v.summary.fabricated, 1);
        assert_eq!(v.summary.fabricated_sample, vec![LOW + 3]);
        assert_eq!(v.assurance, Assurance::Refuted);
        let msg = v.summary.anomaly_message().expect("anomalous");
        assert!(
            msg.contains("lost 1") && msg.contains("fabricated 1"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn a_timed_out_write_that_landed_is_not_fabricated() {
        let h = history(vec![acked(0, LOW + 1), timed_out(1, LOW + 2)]);
        let table = Table::new(10, &[(0, LOW + 1), (1, LOW + 2)]);
        let v = verdict_after_final_read(&table, &plan(2, 10, 0), &h, &quorum())
            .await
            .expect("final read succeeds");
        assert_eq!(v.summary.fabricated, 0);
        assert_eq!(v.summary.recovered_indeterminate, 1);
        assert_eq!(v.summary.indeterminate, 1);
        assert_eq!(v.assurance, Assurance::Confirmed);
    }

    #[tokio::test]
    async fn an_unreadable_bucket_is_a_run_failure_not_a_verdict() {
        let h = history(vec![acked(3, LOW + 1)]);
        let table = Table::new(10, &[(3, LOW + 1)]).failing(3, u32::MAX);
        let err = verdict_after_final_read(&table, &plan(4, 10, 2), &h, &quorum())
            .await
            .expect_err("an unreadable bucket must fail the run");
        assert!(
            err.to_string().contains("final read failed: bucket 3"),
            "{err:#}"
        );
        // The first read and `retries` retries, then the run fails.
        assert_eq!(table.failures.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_run_without_acknowledged_writes_is_not_tested() {
        let h = history(vec![timed_out(0, LOW + 1)]);
        let table = Table::new(10, &[]);
        let v = verdict_after_final_read(&table, &plan(2, 10, 0), &h, &quorum())
            .await
            .expect("final read succeeds");
        assert_eq!(
            v.assurance,
            Assurance::NotTested(NotTested::NoAcknowledgedWrites)
        );
        assert_eq!(v.summary.attempted, 1);
        assert_eq!(
            v.summary.indeterminate, 1,
            "a timeout is not an acknowledgement"
        );
    }

    /// The read pages through a bucket larger than one page and recovers every value once.
    #[tokio::test]
    async fn the_final_read_pages_through_every_bucket() {
        let rows: Vec<(u32, i64)> = (0..25).map(|i| (i % 3, LOW + i64::from(i))).collect();
        let ops = rows.iter().map(|(b, v)| acked(*b, *v)).collect();
        let table = Table::new(4, &rows);
        let v = verdict_after_final_read(&table, &plan(3, 4, 0), &history(ops), &quorum())
            .await
            .expect("final read succeeds");
        assert_eq!((v.summary.recovered, v.summary.lost), (25, 0));
        assert_eq!(v.assurance, Assurance::Confirmed);
    }

    /// A transient failure is retried and the bucket still read in full.
    #[tokio::test]
    async fn a_page_that_fails_within_the_retry_budget_is_retried() {
        let h = history(vec![acked(1, LOW + 1)]);
        let table = Table::new(10, &[(1, LOW + 1)]).failing(1, 2);
        let v = verdict_after_final_read(&table, &plan(2, 10, 2), &h, &quorum())
            .await
            .expect("two failures fit a budget of two retries");
        assert_eq!(v.summary.recovered, 1);
        assert_eq!(v.assurance, Assurance::Confirmed);
    }

    /// A reader that ignores the bound it was given and returns the same values every time.
    struct IgnoresBounds(Vec<i64>);

    #[async_trait]
    impl BucketReader for IgnoresBounds {
        async fn read_page(&self, _bucket: u32, _after: i64, _limit: u32) -> Result<Vec<i64>> {
            Ok(self.0.clone())
        }
    }

    /// A page that goes backwards, leaves the run's range, or overfills is a reader defect, not
    /// data: it would otherwise loop forever or count a value twice.
    #[tokio::test]
    async fn a_page_that_breaks_the_paging_contract_is_refused() {
        let h = history(vec![acked(0, LOW + 1)]);
        for (rows, why) in [
            (vec![LOW - 5], "below the range"),
            (vec![LOW + (1 << 32)], "at the range's exclusive end"),
            (vec![LOW + 2, LOW + 1], "descending"),
            (vec![LOW + 1, LOW + 2, LOW + 3], "more rows than asked for"),
        ] {
            let err = verdict_after_final_read(&IgnoresBounds(rows), &plan(1, 2, 0), &h, &quorum())
                .await
                .expect_err(why);
            assert!(
                err.to_string().contains("final read failed: bucket 0"),
                "{why}: {err:#}"
            );
        }
        // A full page whose repeat is not above the last value: paging must not loop on it.
        let err = verdict_after_final_read(
            &IgnoresBounds(vec![LOW + 1, LOW + 2]),
            &plan(1, 2, 0),
            &h,
            &quorum(),
        )
        .await
        .expect_err("a repeated page is refused, not re-read forever");
        assert!(format!("{err:#}").contains("outside"), "{err:#}");
    }

    #[test]
    fn a_set_insert_with_an_impossible_result_is_refused() {
        let h = history(vec![write(0, LOW, OpResult::Value(Some(1)))]);
        assert!(book_from_history(&h).is_err());
    }

    fn db(e: DbError) -> anyhow::Error {
        let exec = ExecutionError::LastAttemptError(RequestAttemptError::DbError(e, "x".into()));
        anyhow::Error::new(exec).context("executing query `INSERT …`")
    }

    #[test]
    fn only_a_not_applied_refusal_is_definite() {
        let unavailable = db(DbError::Unavailable {
            consistency: Consistency::Quorum,
            required: 2,
            alive: 1,
        });
        assert_eq!(
            classify_write_error(&unavailable),
            WriteOutcome::DefiniteFailure
        );
        assert_eq!(
            classify_write_error(&db(DbError::Overloaded)),
            WriteOutcome::DefiniteFailure
        );
        let write_timeout = db(DbError::WriteTimeout {
            consistency: Consistency::Quorum,
            received: 1,
            required: 2,
            write_type: WriteType::Simple,
        });
        assert_eq!(
            classify_write_error(&write_timeout),
            WriteOutcome::Indeterminate
        );
        assert_eq!(
            classify_write_error(&db(DbError::ServerError)),
            WriteOutcome::Indeterminate
        );
        let client_timeout =
            anyhow::Error::new(ExecutionError::RequestTimeout(Duration::from_secs(12)));
        assert_eq!(
            classify_write_error(&client_timeout),
            WriteOutcome::Indeterminate
        );
        assert_eq!(
            classify_write_error(&anyhow::anyhow!("something else")),
            WriteOutcome::Indeterminate
        );
    }

    #[test]
    fn a_definite_refusal_is_recorded_so_the_book_can_see_it() {
        let h = history(vec![
            acked(0, LOW + 1),
            timed_out(0, LOW + 2),
            refused(0, LOW + 3),
        ]);
        let b = book_from_history(&h).expect("book");
        assert_eq!(b.attempted.to_vec(), vec![LOW + 1, LOW + 2, LOW + 3]);
        assert_eq!(b.acknowledged.to_vec(), vec![LOW + 1]);
        assert_eq!(b.indeterminate.to_vec(), vec![LOW + 2]);
    }

    fn vars<'a>(set: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            set.iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn unset_parameters_take_their_defaults() {
        assert_eq!(SetParams::from_vars(vars(&[])), Ok(SetParams::default()));
        let d = SetParams::default();
        assert_eq!(
            (d.buckets, d.settle, d.page, d.retries),
            (16, Duration::from_secs(10), 1000, 5)
        );
    }

    #[test]
    fn declared_parameters_are_read() {
        let p = SetParams::from_vars(vars(&[
            ("SET_BUCKETS", "4"),
            ("FINAL_READ_SETTLE", "0"),
            ("FINAL_READ_PAGE", "50"),
            ("FINAL_READ_RETRIES", "0"),
        ]));
        assert_eq!(
            p,
            Ok(SetParams {
                buckets: 4,
                settle: Duration::ZERO,
                page: 50,
                retries: 0
            })
        );
    }

    #[test]
    fn a_parameter_that_does_not_parse_is_refused() {
        for (key, value) in [
            ("SET_BUCKETS", "0"),
            ("SET_BUCKETS", "sixteen"),
            ("FINAL_READ_SETTLE", "10s"),
            ("FINAL_READ_PAGE", "0"),
            ("FINAL_READ_RETRIES", "-1"),
        ] {
            let result = SetParams::from_vars(vars(&[(key, value)]));
            assert!(
                matches!(&result, Err(e) if e.contains(key)),
                "{key}={value:?} must be refused naming the variable: {result:?}"
            );
        }
    }

    /// A session that records the statements it was asked to run, at which level, and answers
    /// SELECTs with fixed rows.
    struct Recording {
        seen: std::sync::Mutex<Vec<(String, Level)>>,
        rows: Vec<Vec<(String, String)>>,
    }

    #[async_trait]
    impl CqlSession for Recording {
        async fn execute(&self, query: &str) -> Result<Vec<Vec<(String, String)>>> {
            bail!("unexpected execute without a level: {query}")
        }

        async fn execute_at(
            &self,
            query: &str,
            level: Level,
        ) -> Result<Vec<Vec<(String, String)>>> {
            self.seen
                .lock()
                .expect("test lock")
                .push((query.to_string(), level));
            Ok(self.rows.clone())
        }
    }

    #[tokio::test]
    async fn the_cql_reader_pages_by_clustering_range_at_the_read_level() {
        let session = Recording {
            seen: std::sync::Mutex::new(Vec::new()),
            rows: vec![vec![("v".into(), (LOW + 9).to_string())]],
        };
        let reader = CqlBucketReader {
            session: &session,
            level: Level::All,
            high: LOW + (1 << 32),
        };
        let page = reader.read_page(5, LOW + 3, 100).await.expect("page");
        assert_eq!(page, vec![LOW + 9]);
        let seen = session.seen.lock().expect("test lock");
        assert_eq!(
            seen.as_slice(),
            &[(
                format!(
                    "SELECT v FROM jepsen.set WHERE bucket = 5 AND v > {} AND v < {} LIMIT 100",
                    LOW + 3,
                    LOW + (1 << 32)
                ),
                Level::All
            )]
        );
    }
}
