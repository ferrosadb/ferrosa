//! Module: Client request rate, outcome and latency for the CQL data opcodes.
//! Correctness: Correct when every finished request is counted exactly once under
//!   its kind and outcome (ok, error, or cancelled when the task is dropped before
//!   it finishes), the latency histogram is cumulative with a `+Inf` bucket equal to
//!   the count, the in-flight gauge returns to zero however a request ends, and every
//!   series is rendered even before its first sample so dashboards see stable names.
//! Last revised: 2026-09-26
//! Last changed: New module — `/metrics` had no request-rate or request-latency
//!   series, so a dashboard could not show client load or tail latency.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::frame::Opcode;

/// The CQL opcodes that carry client work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind {
    Query,
    Prepare,
    Execute,
    Batch,
}

impl RequestKind {
    const ALL: [RequestKind; 4] = [Self::Query, Self::Prepare, Self::Execute, Self::Batch];

    /// The kind for a data opcode; `None` for handshake and control opcodes.
    pub fn from_opcode(opcode: Opcode) -> Option<Self> {
        match opcode {
            Opcode::Query => Some(Self::Query),
            Opcode::Prepare => Some(Self::Prepare),
            Opcode::Execute => Some(Self::Execute),
            Opcode::Batch => Some(Self::Batch),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Prepare => "prepare",
            Self::Execute => "execute",
            Self::Batch => "batch",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// How a request ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Answered with a result.
    Ok,
    /// Answered with a CQL error.
    Error,
    /// Dropped before it finished (client gone, task aborted).
    Cancelled,
}

impl Outcome {
    const ALL: [Outcome; 3] = [Self::Ok, Self::Error, Self::Cancelled];

    fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// Upper bounds, in seconds, of the latency histogram buckets.
pub const BUCKETS_SECONDS: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

struct KindStats {
    outcomes: [AtomicU64; 3],
    /// Non-cumulative: an observation lands in the first bucket whose bound holds it.
    buckets: [AtomicU64; BUCKETS_SECONDS.len()],
    sum_micros: AtomicU64,
    count: AtomicU64,
    in_flight: AtomicI64,
}

impl KindStats {
    const fn new() -> Self {
        Self {
            outcomes: [const { AtomicU64::new(0) }; 3],
            buckets: [const { AtomicU64::new(0) }; BUCKETS_SECONDS.len()],
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
            in_flight: AtomicI64::new(0),
        }
    }
}

/// Request counters and a latency histogram per [`RequestKind`].
pub struct RequestMetrics {
    kinds: [KindStats; 4],
}

impl RequestMetrics {
    pub const fn new() -> Self {
        Self {
            kinds: [
                KindStats::new(),
                KindStats::new(),
                KindStats::new(),
                KindStats::new(),
            ],
        }
    }

    /// Begin timing a request. Call [`RequestTimer::finish`] when it ends; dropping the
    /// timer without finishing records the request as cancelled.
    pub fn start(&self, kind: RequestKind) -> RequestTimer<'_> {
        self.kinds[kind.index()]
            .in_flight
            .fetch_add(1, Ordering::Relaxed);
        RequestTimer {
            metrics: self,
            kind,
            started: Instant::now(),
            finished: false,
        }
    }

    /// Record one finished request (outcome, latency). Does not touch the in-flight
    /// gauge; [`RequestTimer`] pairs that with [`Self::start`].
    pub fn record(&self, kind: RequestKind, outcome: Outcome, elapsed: Duration) {
        let stats = &self.kinds[kind.index()];
        stats.outcomes[outcome.index()].fetch_add(1, Ordering::Relaxed);
        let seconds = elapsed.as_secs_f64();
        // An observation past the last finite bound is only in `+Inf` (the count).
        if let Some(bucket) = BUCKETS_SECONDS.iter().position(|bound| seconds <= *bound) {
            stats.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        }
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        stats.sum_micros.fetch_add(micros, Ordering::Relaxed);
        stats.count.fetch_add(1, Ordering::Relaxed);
    }

    fn end(&self, kind: RequestKind, outcome: Outcome, elapsed: Duration) {
        self.record(kind, outcome, elapsed);
        self.kinds[kind.index()]
            .in_flight
            .fetch_sub(1, Ordering::Relaxed);
    }

    /// Prometheus text for every kind and outcome.
    pub fn render_prometheus(&self, out: &mut String) {
        use std::fmt::Write;
        // Writing into a String cannot fail, so the `write!` results are ignored.
        out.push_str(
            "# HELP ferrosa_cql_requests_total CQL data requests finished, by kind and outcome (ok, error, cancelled).\n\
             # TYPE ferrosa_cql_requests_total counter\n",
        );
        for kind in RequestKind::ALL {
            for outcome in Outcome::ALL {
                let _ = writeln!(
                    out,
                    "ferrosa_cql_requests_total{{kind=\"{}\",outcome=\"{}\"}} {}",
                    kind.label(),
                    outcome.label(),
                    self.kinds[kind.index()].outcomes[outcome.index()].load(Ordering::Relaxed)
                );
            }
        }
        out.push_str(
            "# HELP ferrosa_cql_request_duration_seconds CQL data request latency in seconds, by kind.\n\
             # TYPE ferrosa_cql_request_duration_seconds histogram\n",
        );
        for kind in RequestKind::ALL {
            let stats = &self.kinds[kind.index()];
            let mut cumulative = 0u64;
            for (bound, bucket) in BUCKETS_SECONDS.iter().zip(&stats.buckets) {
                cumulative += bucket.load(Ordering::Relaxed);
                let _ = writeln!(
                    out,
                    "ferrosa_cql_request_duration_seconds_bucket{{kind=\"{}\",le=\"{bound}\"}} {cumulative}",
                    kind.label()
                );
            }
            let count = stats.count.load(Ordering::Relaxed);
            let sum = stats.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
            let _ = writeln!(
                out,
                "ferrosa_cql_request_duration_seconds_bucket{{kind=\"{}\",le=\"+Inf\"}} {count}",
                kind.label()
            );
            let _ = writeln!(
                out,
                "ferrosa_cql_request_duration_seconds_sum{{kind=\"{}\"}} {sum}",
                kind.label()
            );
            let _ = writeln!(
                out,
                "ferrosa_cql_request_duration_seconds_count{{kind=\"{}\"}} {count}",
                kind.label()
            );
        }
        out.push_str(
            "# HELP ferrosa_cql_requests_in_flight CQL data requests currently being handled, by kind.\n\
             # TYPE ferrosa_cql_requests_in_flight gauge\n",
        );
        for kind in RequestKind::ALL {
            let _ = writeln!(
                out,
                "ferrosa_cql_requests_in_flight{{kind=\"{}\"}} {}",
                kind.label(),
                self.kinds[kind.index()].in_flight.load(Ordering::Relaxed)
            );
        }
    }
}

impl Default for RequestMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Times one request; see [`RequestMetrics::start`].
pub struct RequestTimer<'a> {
    metrics: &'a RequestMetrics,
    kind: RequestKind,
    started: Instant,
    finished: bool,
}

impl RequestTimer<'_> {
    /// The request ended with `outcome`.
    pub fn finish(mut self, outcome: Outcome) {
        self.finished = true;
        self.metrics.end(self.kind, outcome, self.started.elapsed());
    }
}

impl Drop for RequestTimer<'_> {
    fn drop(&mut self) {
        // A request whose task was dropped mid-flight (the client went away, the
        // connection task was aborted) is counted, not lost, and no longer in flight.
        if !self.finished {
            self.metrics
                .end(self.kind, Outcome::Cancelled, self.started.elapsed());
        }
    }
}

static GLOBAL: RequestMetrics = RequestMetrics::new();

/// Begin timing a request in the process-wide metrics.
pub fn start(kind: RequestKind) -> RequestTimer<'static> {
    GLOBAL.start(kind)
}

/// Append the process-wide request metrics to `out`.
pub fn render_prometheus(out: &mut String) {
    GLOBAL.render_prometheus(out);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(m: &RequestMetrics) -> String {
        let mut out = String::new();
        m.render_prometheus(&mut out);
        out
    }

    fn sample(text: &str, series: &str) -> Option<f64> {
        text.lines().find_map(|l| {
            l.strip_prefix(series)?
                .strip_prefix(' ')?
                .trim()
                .parse()
                .ok()
        })
    }

    #[test]
    fn data_opcodes_map_to_a_kind_and_control_opcodes_do_not() {
        assert_eq!(
            RequestKind::from_opcode(Opcode::Query),
            Some(RequestKind::Query)
        );
        assert_eq!(
            RequestKind::from_opcode(Opcode::Prepare),
            Some(RequestKind::Prepare)
        );
        assert_eq!(
            RequestKind::from_opcode(Opcode::Execute),
            Some(RequestKind::Execute)
        );
        assert_eq!(
            RequestKind::from_opcode(Opcode::Batch),
            Some(RequestKind::Batch)
        );
        assert_eq!(RequestKind::from_opcode(Opcode::Options), None);
        assert_eq!(RequestKind::from_opcode(Opcode::Startup), None);
    }

    #[test]
    fn every_series_exists_before_the_first_request() {
        let text = render(&RequestMetrics::new());
        for family in [
            "ferrosa_cql_requests_total counter",
            "ferrosa_cql_request_duration_seconds histogram",
            "ferrosa_cql_requests_in_flight gauge",
        ] {
            assert!(
                text.contains(&format!("# TYPE {family}")),
                "{family}\n{text}"
            );
        }
        for kind in ["query", "prepare", "execute", "batch"] {
            for outcome in ["ok", "error", "cancelled"] {
                let series =
                    format!("ferrosa_cql_requests_total{{kind=\"{kind}\",outcome=\"{outcome}\"}}");
                assert_eq!(sample(&text, &series), Some(0.0), "{series}");
            }
            let count = format!("ferrosa_cql_request_duration_seconds_count{{kind=\"{kind}\"}}");
            assert_eq!(sample(&text, &count), Some(0.0), "{count}");
        }
    }

    #[test]
    fn outcomes_are_counted_per_kind() {
        let m = RequestMetrics::new();
        m.record(RequestKind::Query, Outcome::Ok, Duration::from_millis(3));
        m.record(RequestKind::Query, Outcome::Ok, Duration::from_millis(3));
        m.record(
            RequestKind::Query,
            Outcome::Error,
            Duration::from_millis(30),
        );
        m.record(RequestKind::Batch, Outcome::Ok, Duration::from_millis(1));
        let text = render(&m);
        let get = |series: &str| sample(&text, series).unwrap_or(f64::NAN);
        assert_eq!(
            get("ferrosa_cql_requests_total{kind=\"query\",outcome=\"ok\"}"),
            2.0
        );
        assert_eq!(
            get("ferrosa_cql_requests_total{kind=\"query\",outcome=\"error\"}"),
            1.0
        );
        assert_eq!(
            get("ferrosa_cql_requests_total{kind=\"batch\",outcome=\"ok\"}"),
            1.0
        );
        assert_eq!(
            get("ferrosa_cql_requests_total{kind=\"execute\",outcome=\"ok\"}"),
            0.0
        );
    }

    #[test]
    fn the_histogram_is_cumulative_with_sum_count_and_an_inf_bucket() {
        let m = RequestMetrics::new();
        for ms in [3, 30, 3000] {
            m.record(RequestKind::Query, Outcome::Ok, Duration::from_millis(ms));
        }
        m.record(RequestKind::Query, Outcome::Ok, Duration::from_secs(60));
        let text = render(&m);
        let bucket = |le: &str| {
            sample(
                &text,
                &format!(
                    "ferrosa_cql_request_duration_seconds_bucket{{kind=\"query\",le=\"{le}\"}}"
                ),
            )
            .unwrap_or(f64::NAN)
        };
        assert_eq!(bucket("0.0025"), 0.0);
        assert_eq!(bucket("0.005"), 1.0, "3 ms");
        assert_eq!(bucket("0.05"), 2.0, "+30 ms");
        assert_eq!(bucket("5"), 3.0, "+3 s");
        assert_eq!(bucket("10"), 3.0, "60 s is past every finite bucket");
        assert_eq!(bucket("+Inf"), 4.0);
        let count = sample(
            &text,
            "ferrosa_cql_request_duration_seconds_count{kind=\"query\"}",
        );
        assert_eq!(count, Some(4.0));
        let sum = sample(
            &text,
            "ferrosa_cql_request_duration_seconds_sum{kind=\"query\"}",
        )
        .unwrap();
        assert!((sum - 63.033).abs() < 1e-6, "sum {sum}");
    }

    #[test]
    fn a_timer_tracks_in_flight_and_records_its_outcome() {
        let m = RequestMetrics::new();
        let timer = m.start(RequestKind::Execute);
        let while_running = render(&m);
        assert_eq!(
            sample(
                &while_running,
                "ferrosa_cql_requests_in_flight{kind=\"execute\"}"
            ),
            Some(1.0)
        );
        timer.finish(Outcome::Error);
        let after = render(&m);
        assert_eq!(
            sample(&after, "ferrosa_cql_requests_in_flight{kind=\"execute\"}"),
            Some(0.0)
        );
        assert_eq!(
            sample(
                &after,
                "ferrosa_cql_requests_total{kind=\"execute\",outcome=\"error\"}"
            ),
            Some(1.0)
        );
    }

    #[test]
    fn a_timer_dropped_without_finishing_counts_as_cancelled_and_releases_in_flight() {
        let m = RequestMetrics::new();
        drop(m.start(RequestKind::Query));
        let text = render(&m);
        assert_eq!(
            sample(&text, "ferrosa_cql_requests_in_flight{kind=\"query\"}"),
            Some(0.0)
        );
        assert_eq!(
            sample(
                &text,
                "ferrosa_cql_requests_total{kind=\"query\",outcome=\"cancelled\"}"
            ),
            Some(1.0)
        );
        assert_eq!(
            sample(
                &text,
                "ferrosa_cql_request_duration_seconds_count{kind=\"query\"}"
            ),
            Some(1.0)
        );
    }
}
