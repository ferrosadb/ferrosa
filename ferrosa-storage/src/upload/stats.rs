//! Optional object-store statistics (`FERROSA_S3_STATS=1`).
//!
//! Tuning the object keys and table sizes needs numbers the store does not
//! give back: how many requests of each kind, how many bytes, how slow, how
//! often 429, and which table and component the bytes belong to. Off by
//! default; when off, no wrapper is installed and the hooks in the download
//! path cost one `OnceLock` read.
//!
//! [`StatsStore`] wraps the innermost store (below the concurrency limit and
//! the 429 retry layer), so it sees every real request, each retry as its own
//! request, and latency that excludes client-side pacing. Per table and
//! component the key layout `prefix/<hex>/<table>/<gen>/<gen>-<component>`
//! is parsed; label cardinality is bounded by [`tracked_label_pairs`].
//!
//! Exposed through Prometheus ([`render_prometheus`]) and the virtual tables
//! `system_observability.object_store_ops` and `object_store_objects`.

use std::collections::HashMap;
use std::fmt::{self, Write as _};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result, UploadPart,
};

/// Default for [`tracked_label_pairs`], used when `FERROSA_S3_STATS_MAX_KEYS` is
/// unset, blank or unparseable.
pub const DEFAULT_TRACKED_LABEL_PAIRS: usize = 4096;

/// Distinct (table, component) label pairs tracked, from
/// `FERROSA_S3_STATS_MAX_KEYS` (default [`DEFAULT_TRACKED_LABEL_PAIRS`]). Further
/// keys fold into `("-", "overflow")` and the first overflow is logged, so no
/// observation is dropped silently.
///
/// This bounds METRIC LABEL CARDINALITY, not query results: a read of
/// `system_observability.object_store_stats` returns every tracked row and
/// truncates nothing. Unbounded label cardinality is the failure it prevents.
/// Read once and cached, so a mid-run change does not resize the map.
pub fn tracked_label_pairs() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        match std::env::var("FERROSA_S3_STATS_MAX_KEYS") {
            Ok(raw) if !raw.trim().is_empty() => match raw.trim().parse::<usize>() {
                Ok(n) if n > 0 => n,
                // Fail loud-ish: stats are optional, so a bad value must not
                // stop the node, but it must not be silently ignored either.
                _ => {
                    tracing::warn!(
                        value = %raw,
                        default = DEFAULT_TRACKED_LABEL_PAIRS,
                        "FERROSA_S3_STATS_MAX_KEYS must be a positive integer; using the default"
                    );
                    DEFAULT_TRACKED_LABEL_PAIRS
                }
            },
            _ => DEFAULT_TRACKED_LABEL_PAIRS,
        }
    })
}

/// Request latency bucket upper bounds, milliseconds.
pub const LATENCY_BUCKETS_MS: [u64; 12] = [
    5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000,
];
/// Object size bucket upper bounds, bytes (64 KiB .. 1 GiB).
pub const SIZE_BUCKETS_BYTES: [u64; 6] = [
    64 * 1024,
    1024 * 1024,
    16 * 1024 * 1024,
    64 * 1024 * 1024,
    256 * 1024 * 1024,
    1024 * 1024 * 1024,
];

/// Parse a `FERROSA_S3_STATS`-style boolean. Absent or empty is off; a value
/// that is neither a recognised true nor false is rejected naming `name`.
pub fn parse_flag(name: &str, value: Option<&str>) -> ferrosa_common::Result<bool> {
    let Some(raw) = value.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(false);
    };
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => Ok(true),
        "0" | "false" | "off" | "no" => Ok(false),
        _ => Err(ferrosa_common::Error::InvalidFormat(format!(
            "{name}={raw:?} is not a boolean (use 1 or 0)"
        ))),
    }
}

/// Object-store operation kinds, one counter set each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Get,
    GetRange,
    Put,
    Multipart,
    Delete,
    List,
    Head,
    Other,
}

const OPS: [Op; 8] = [
    Op::Get,
    Op::GetRange,
    Op::Put,
    Op::Multipart,
    Op::Delete,
    Op::List,
    Op::Head,
    Op::Other,
];

impl Op {
    fn idx(self) -> usize {
        self as usize
    }

    /// Label used in metrics and the virtual table.
    pub fn label(self) -> &'static str {
        match self {
            Op::Get => "get",
            Op::GetRange => "get_range",
            Op::Put => "put",
            Op::Multipart => "multipart",
            Op::Delete => "delete",
            Op::List => "list",
            Op::Head => "head",
            Op::Other => "other",
        }
    }
}

/// Fixed-bucket histogram of atomics.
#[derive(Debug)]
struct Histogram<const N: usize> {
    bounds: &'static [u64; N],
    buckets: [AtomicU64; N],
    overflow: AtomicU64,
    sum: AtomicU64,
}

impl<const N: usize> Histogram<N> {
    fn new(bounds: &'static [u64; N]) -> Self {
        Self {
            bounds,
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            overflow: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    fn observe(&self, value: u64) {
        self.sum.fetch_add(value, Ordering::Relaxed);
        match self.bounds.iter().position(|bound| value <= *bound) {
            Some(i) => self.buckets[i].fetch_add(1, Ordering::Relaxed),
            None => self.overflow.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Per-bucket (non-cumulative) counts; the last entry is the overflow.
    fn counts(&self) -> Vec<u64> {
        let mut counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();
        counts.push(self.overflow.load(Ordering::Relaxed));
        counts
    }

    fn count(&self) -> u64 {
        self.counts().iter().sum()
    }

    /// Upper bound of the bucket holding quantile `q` (0..=1); `None` when
    /// empty. The overflow bucket reports the largest finite bound.
    fn quantile_upper(&self, q: f64) -> Option<u64> {
        let counts = self.counts();
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return None;
        }
        let rank = ((total as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, c) in counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Some(self.bounds[i.min(N - 1)]);
            }
        }
        Some(self.bounds[N - 1])
    }
}

#[derive(Debug)]
struct OpCounters {
    requests: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
    rate_limited: AtomicU64,
    retries: AtomicU64,
    latency_ms: Histogram<12>,
}

impl OpCounters {
    fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            rate_limited: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            latency_ms: Histogram::new(&LATENCY_BUCKETS_MS),
        }
    }
}

/// Counters for one (table, component).
#[derive(Debug)]
struct ObjectCounters {
    get_whole: AtomicU64,
    get_ranged: AtomicU64,
    put_requests: AtomicU64,
    bytes_fetched: AtomicU64,
    bytes_requested: AtomicU64,
    bytes_put: AtomicU64,
    size: Histogram<6>,
    size_max: AtomicU64,
    downloads: AtomicU64,
    download_bytes: AtomicU64,
    download_micros: AtomicU64,
    ranged_downloads: AtomicU64,
    part_retries: AtomicU64,
}

impl ObjectCounters {
    fn new() -> Self {
        Self {
            get_whole: AtomicU64::new(0),
            get_ranged: AtomicU64::new(0),
            put_requests: AtomicU64::new(0),
            bytes_fetched: AtomicU64::new(0),
            bytes_requested: AtomicU64::new(0),
            bytes_put: AtomicU64::new(0),
            size: Histogram::new(&SIZE_BUCKETS_BYTES),
            size_max: AtomicU64::new(0),
            downloads: AtomicU64::new(0),
            download_bytes: AtomicU64::new(0),
            download_micros: AtomicU64::new(0),
            ranged_downloads: AtomicU64::new(0),
            part_retries: AtomicU64::new(0),
        }
    }

    fn observe_size(&self, size: u64) {
        self.size.observe(size);
        self.size_max.fetch_max(size, Ordering::Relaxed);
    }
}

/// A (table, component) pair; `table` is `keyspace.table`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey {
    pub table: String,
    pub component: String,
}

/// Split `prefix/<hex>/<table>/<gen>/<gen>-<component>`. Anything else (the
/// manifest, pending logs, probes) is `("-", "other")`.
pub fn parse_object_key(path: &Path) -> ObjectKey {
    let parts: Vec<_> = path.parts().map(|p| p.as_ref().to_owned()).collect();
    if let [.., table, generation, file] = parts.as_slice() {
        if let Some(component) = file
            .strip_prefix(generation.as_str())
            .and_then(|rest| rest.strip_prefix('-'))
        {
            return ObjectKey {
                table: table.clone(),
                component: component.to_owned(),
            };
        }
    }
    ObjectKey {
        table: "-".into(),
        component: "other".into(),
    }
}

/// Name, help text and accessor of one per-operation Prometheus series.
type OpSeries = (&'static str, &'static str, fn(&OpSnapshot) -> u64);
/// Name, help text and accessor of one per-object Prometheus series.
type ObjectSeries = (&'static str, &'static str, fn(&ObjectSnapshot) -> f64);

/// All counters. Cheap to share; every update is atomic.
#[derive(Debug)]
pub struct ObjectStoreStats {
    ops: [OpCounters; 8],
    objects: Mutex<HashMap<ObjectKey, Arc<ObjectCounters>>>,
}

impl Default for ObjectStoreStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of one operation kind.
#[derive(Debug, Clone, PartialEq)]
pub struct OpSnapshot {
    pub op: &'static str,
    pub requests: u64,
    pub bytes: u64,
    pub errors: u64,
    pub rate_limited: u64,
    pub retries: u64,
    pub p50_ms: Option<u64>,
    pub p99_ms: Option<u64>,
    pub total_ms: f64,
}

/// Snapshot of one (table, component).
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectSnapshot {
    pub key: ObjectKey,
    pub get_whole: u64,
    pub get_ranged: u64,
    pub put_requests: u64,
    pub bytes_fetched: u64,
    pub bytes_requested: u64,
    pub bytes_put: u64,
    pub objects_seen: u64,
    pub size_max: u64,
    pub size_sum: u64,
    pub downloads: u64,
    pub ranged_downloads: u64,
    pub download_bytes: u64,
    pub download_secs: f64,
    pub part_retries: u64,
}

impl ObjectSnapshot {
    /// Mean download throughput over completed downloads, MB/s (1e6 bytes).
    pub fn download_mb_per_sec(&self) -> f64 {
        if self.download_secs <= 0.0 {
            return 0.0;
        }
        self.download_bytes as f64 / 1e6 / self.download_secs
    }

    /// Bytes fetched per byte the callers asked for; 1.0 when nothing was
    /// requested yet. Above 1.0 means re-fetches or over-reads.
    pub fn read_amplification(&self) -> f64 {
        if self.bytes_requested == 0 {
            return 1.0;
        }
        self.bytes_fetched as f64 / self.bytes_requested as f64
    }
}

impl ObjectStoreStats {
    pub fn new() -> Self {
        Self {
            ops: std::array::from_fn(|_| OpCounters::new()),
            objects: Mutex::new(HashMap::new()),
        }
    }

    fn counters_for(&self, path: &Path) -> Arc<ObjectCounters> {
        let key = parse_object_key(path);
        let mut map = self
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(found) = map.get(&key) {
            return Arc::clone(found);
        }
        let limit = tracked_label_pairs();
        let key = if map.len() >= limit {
            let overflow = ObjectKey {
                table: "-".into(),
                component: "overflow".into(),
            };
            if !map.contains_key(&overflow) {
                tracing::warn!(
                    limit,
                    "object-store stats: too many distinct (table, component) keys; \
                     further keys are folded into component=overflow"
                );
            }
            overflow
        } else {
            key
        };
        Arc::clone(
            map.entry(key)
                .or_insert_with(|| Arc::new(ObjectCounters::new())),
        )
    }

    /// Record one finished request.
    fn finish(&self, op: Op, started: Instant, bytes: u64, error: Option<&object_store::Error>) {
        let c = &self.ops[op.idx()];
        c.requests.fetch_add(1, Ordering::Relaxed);
        c.bytes.fetch_add(bytes, Ordering::Relaxed);
        c.latency_ms.observe(started.elapsed().as_millis() as u64);
        if let Some(e) = error {
            c.errors.fetch_add(1, Ordering::Relaxed);
            if super::throttle::is_rate_limited(e) {
                c.rate_limited.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// A 429 retry performed by the throttling layer.
    pub fn record_retry(&self, op: &str) {
        let op = match op {
            "get" => Op::Get,
            "get_range" | "get_ranges" => Op::GetRange,
            "put" => Op::Put,
            "put_multipart" => Op::Multipart,
            "delete" => Op::Delete,
            "list" => Op::List,
            "head" => Op::Head,
            _ => Op::Other,
        };
        self.ops[op.idx()].retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Bytes the caller asked for, when it knows (a whole-object download
    /// asks for the object's size).
    pub fn record_requested(&self, path: &Path, bytes: u64) {
        self.counters_for(path)
            .bytes_requested
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// One finished object download.
    pub fn record_download(&self, record: &DownloadRecord<'_>) {
        let c = self.counters_for(record.path);
        c.downloads.fetch_add(1, Ordering::Relaxed);
        if record.ranged {
            c.ranged_downloads.fetch_add(1, Ordering::Relaxed);
        }
        c.download_bytes
            .fetch_add(record.object_bytes, Ordering::Relaxed);
        c.download_micros
            .fetch_add(record.elapsed.as_micros() as u64, Ordering::Relaxed);
        c.part_retries
            .fetch_add(u64::from(record.part_retries), Ordering::Relaxed);
        c.bytes_requested
            .fetch_add(record.object_bytes, Ordering::Relaxed);
    }

    /// Per-operation snapshots, in a fixed order.
    pub fn op_snapshots(&self) -> Vec<OpSnapshot> {
        OPS.iter()
            .map(|op| {
                let c = &self.ops[op.idx()];
                OpSnapshot {
                    op: op.label(),
                    requests: c.requests.load(Ordering::Relaxed),
                    bytes: c.bytes.load(Ordering::Relaxed),
                    errors: c.errors.load(Ordering::Relaxed),
                    rate_limited: c.rate_limited.load(Ordering::Relaxed),
                    retries: c.retries.load(Ordering::Relaxed),
                    p50_ms: c.latency_ms.quantile_upper(0.5),
                    p99_ms: c.latency_ms.quantile_upper(0.99),
                    total_ms: c.latency_ms.sum.load(Ordering::Relaxed) as f64,
                }
            })
            .collect()
    }

    /// Per-(table, component) snapshots, sorted by key.
    pub fn object_snapshots(&self) -> Vec<ObjectSnapshot> {
        let map = self
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut out: Vec<_> = map
            .iter()
            .map(|(key, c)| ObjectSnapshot {
                key: key.clone(),
                get_whole: c.get_whole.load(Ordering::Relaxed),
                get_ranged: c.get_ranged.load(Ordering::Relaxed),
                put_requests: c.put_requests.load(Ordering::Relaxed),
                bytes_fetched: c.bytes_fetched.load(Ordering::Relaxed),
                bytes_requested: c.bytes_requested.load(Ordering::Relaxed),
                bytes_put: c.bytes_put.load(Ordering::Relaxed),
                objects_seen: c.size.count(),
                size_max: c.size_max.load(Ordering::Relaxed),
                size_sum: c.size.sum.load(Ordering::Relaxed),
                downloads: c.downloads.load(Ordering::Relaxed),
                ranged_downloads: c.ranged_downloads.load(Ordering::Relaxed),
                download_bytes: c.download_bytes.load(Ordering::Relaxed),
                download_secs: c.download_micros.load(Ordering::Relaxed) as f64 / 1e6,
                part_retries: c.part_retries.load(Ordering::Relaxed),
            })
            .collect();
        out.sort_by(|a, b| a.key.cmp(&b.key));
        out
    }

    fn size_histograms(&self) -> Vec<(ObjectKey, Vec<u64>, u64)> {
        let map = self
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut out: Vec<_> = map
            .iter()
            .map(|(k, c)| {
                (
                    k.clone(),
                    c.size.counts(),
                    c.size.sum.load(Ordering::Relaxed),
                )
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Prometheus text for everything recorded.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        self.render_ops(&mut out);
        self.render_objects(&mut out);
        out
    }

    fn render_ops(&self, out: &mut String) {
        let ops = self.op_snapshots();
        let series: [OpSeries; 5] = [
            (
                "requests_total",
                "Object-store requests by operation (each 429 retry counts).",
                |s| s.requests,
            ),
            (
                "request_bytes_total",
                "Bytes moved by object-store requests by operation.",
                |s| s.bytes,
            ),
            (
                "request_errors_total",
                "Object-store requests that returned an error.",
                |s| s.errors,
            ),
            (
                "rate_limited_total",
                "Object-store requests answered 429.",
                |s| s.rate_limited,
            ),
            (
                "retries_total",
                "429 retries performed by the throttling layer.",
                |s| s.retries,
            ),
        ];
        for (name, help, get) in series {
            let _ = writeln!(out, "# HELP ferrosa_s3_{name} {help}");
            let _ = writeln!(out, "# TYPE ferrosa_s3_{name} counter");
            for s in &ops {
                let _ = writeln!(out, "ferrosa_s3_{name}{{op=\"{}\"}} {}", s.op, get(s));
            }
        }
        let _ = writeln!(
            out,
            "# HELP ferrosa_s3_request_duration_seconds Object-store request latency (time to response headers for GETs)."
        );
        let _ = writeln!(out, "# TYPE ferrosa_s3_request_duration_seconds histogram");
        for op in OPS {
            let c = &self.ops[op.idx()];
            let counts = c.latency_ms.counts();
            let mut cumulative = 0;
            for (i, bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
                cumulative += counts[i];
                let _ = writeln!(
                    out,
                    "ferrosa_s3_request_duration_seconds_bucket{{op=\"{}\",le=\"{}\"}} {cumulative}",
                    op.label(),
                    *bound as f64 / 1000.0
                );
            }
            cumulative += counts[LATENCY_BUCKETS_MS.len()];
            let _ = writeln!(
                out,
                "ferrosa_s3_request_duration_seconds_bucket{{op=\"{}\",le=\"+Inf\"}} {cumulative}\n\
                 ferrosa_s3_request_duration_seconds_sum{{op=\"{}\"}} {:.3}\n\
                 ferrosa_s3_request_duration_seconds_count{{op=\"{}\"}} {cumulative}",
                op.label(),
                op.label(),
                c.latency_ms.sum.load(Ordering::Relaxed) as f64 / 1000.0,
                op.label()
            );
        }
    }

    fn render_objects(&self, out: &mut String) {
        let snaps = self.object_snapshots();
        let series: [ObjectSeries; 7] = [
            (
                "object_bytes_fetched_total",
                "Bytes returned by GETs per table and component.",
                |s| s.bytes_fetched as f64,
            ),
            (
                "object_bytes_requested_total",
                "Bytes callers asked for per table and component (read amplification denominator).",
                |s| s.bytes_requested as f64,
            ),
            (
                "object_bytes_put_total",
                "Bytes uploaded per table and component.",
                |s| s.bytes_put as f64,
            ),
            (
                "object_downloads_total",
                "Completed whole-object downloads per table and component.",
                |s| s.downloads as f64,
            ),
            (
                "object_ranged_downloads_total",
                "Completed downloads that used parallel ranged parts.",
                |s| s.ranged_downloads as f64,
            ),
            (
                "object_download_seconds_total",
                "Wall time of completed downloads per table and component.",
                |s| s.download_secs,
            ),
            (
                "object_download_part_retries_total",
                "Ranged download parts retried per table and component.",
                |s| s.part_retries as f64,
            ),
        ];
        for (name, help, get) in series {
            let _ = writeln!(out, "# HELP ferrosa_s3_{name} {help}");
            let _ = writeln!(out, "# TYPE ferrosa_s3_{name} counter");
            for s in &snaps {
                let _ = writeln!(out, "ferrosa_s3_{name}{{{}}} {}", labels(&s.key), get(s));
            }
        }
        let _ = writeln!(
            out,
            "# HELP ferrosa_s3_object_requests_total GET requests per table and component by kind."
        );
        let _ = writeln!(out, "# TYPE ferrosa_s3_object_requests_total counter");
        for s in &snaps {
            let l = labels(&s.key);
            let _ = writeln!(
                out,
                "ferrosa_s3_object_requests_total{{{l},kind=\"whole\"}} {}",
                s.get_whole
            );
            let _ = writeln!(
                out,
                "ferrosa_s3_object_requests_total{{{l},kind=\"ranged\"}} {}",
                s.get_ranged
            );
            let _ = writeln!(
                out,
                "ferrosa_s3_object_requests_total{{{l},kind=\"put\"}} {}",
                s.put_requests
            );
        }
        let _ = writeln!(out, "# HELP ferrosa_s3_object_size_bytes Sizes of objects seen by GETs per table and component.");
        let _ = writeln!(out, "# TYPE ferrosa_s3_object_size_bytes histogram");
        for (key, counts, sum) in self.size_histograms() {
            let l = labels(&key);
            let mut cumulative = 0;
            for (i, bound) in SIZE_BUCKETS_BYTES.iter().enumerate() {
                cumulative += counts[i];
                let _ = writeln!(
                    out,
                    "ferrosa_s3_object_size_bytes_bucket{{{l},le=\"{bound}\"}} {cumulative}"
                );
            }
            cumulative += counts[SIZE_BUCKETS_BYTES.len()];
            let _ = writeln!(
                out,
                "ferrosa_s3_object_size_bytes_bucket{{{l},le=\"+Inf\"}} {cumulative}\n\
                 ferrosa_s3_object_size_bytes_sum{{{l}}} {sum}\n\
                 ferrosa_s3_object_size_bytes_count{{{l}}} {cumulative}"
            );
        }
    }
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn labels(key: &ObjectKey) -> String {
    format!(
        "table=\"{}\",component=\"{}\"",
        escape(&key.table),
        escape(&key.component)
    )
}

/// A finished object download, for [`record_download`].
pub struct DownloadRecord<'a> {
    pub path: &'a Path,
    pub object_bytes: u64,
    pub elapsed: Duration,
    pub parts: u32,
    pub ranged: bool,
    pub part_retries: u32,
}

static GLOBAL: OnceLock<Arc<ObjectStoreStats>> = OnceLock::new();

/// The process-wide stats, when enabled.
pub fn global() -> Option<&'static Arc<ObjectStoreStats>> {
    GLOBAL.get()
}

/// Enable stats for the process (idempotent) and return the shared handle.
pub fn enable() -> Arc<ObjectStoreStats> {
    Arc::clone(GLOBAL.get_or_init(|| Arc::new(ObjectStoreStats::new())))
}

/// [`enable`] when `FERROSA_S3_STATS` is set true.
pub fn enabled_from_env() -> ferrosa_common::Result<Option<Arc<ObjectStoreStats>>> {
    let on = parse_flag(
        "FERROSA_S3_STATS",
        std::env::var("FERROSA_S3_STATS").ok().as_deref(),
    )?;
    Ok(on.then(enable))
}

/// Record a finished download in the global stats, if enabled.
pub fn record_download(record: &DownloadRecord<'_>) {
    if let Some(stats) = global() {
        stats.record_download(record);
    }
}

/// Record a 429 retry in the global stats, if enabled.
pub fn record_retry(op: &str) {
    if let Some(stats) = global() {
        stats.record_retry(op);
    }
}

static MAX_IN_FLIGHT: AtomicU64 = AtomicU64::new(0);
static POOL_MAX_IDLE: AtomicU64 = AtomicU64::new(0);

/// Record the effective in-flight limit and pool size the store was built
/// with, so "what is the pool size on this host" is answerable from metrics.
pub fn record_pool_settings(max_in_flight: usize, pool_max_idle_per_host: usize) {
    MAX_IN_FLIGHT.store(max_in_flight as u64, Ordering::Relaxed);
    POOL_MAX_IDLE.store(pool_max_idle_per_host as u64, Ordering::Relaxed);
}

/// Prometheus gauges of the effective in-flight limit and pool size.
fn render_pool_gauges() -> String {
    let mut out = String::new();
    out.push_str(
        "# HELP ferrosa_s3_max_in_flight Effective cap on concurrent object-store requests.\n",
    );
    out.push_str("# TYPE ferrosa_s3_max_in_flight gauge\n");
    let _ = writeln!(
        out,
        "ferrosa_s3_max_in_flight {}",
        MAX_IN_FLIGHT.load(Ordering::Relaxed)
    );
    out.push_str(
        "# HELP ferrosa_s3_pool_max_idle_per_host Effective idle connections kept per host.\n",
    );
    out.push_str("# TYPE ferrosa_s3_pool_max_idle_per_host gauge\n");
    let _ = writeln!(
        out,
        "ferrosa_s3_pool_max_idle_per_host {}",
        POOL_MAX_IDLE.load(Ordering::Relaxed)
    );
    out
}

/// Prometheus text for the global stats; empty when disabled.
pub fn render_prometheus() -> String {
    let Some(stats) = global() else {
        return String::new();
    };
    let mut out = stats.render_prometheus();
    out.push_str(&render_pool_gauges());
    out
}

/// An [`ObjectStore`] that records every request into [`ObjectStoreStats`].
pub struct StatsStore {
    inner: Arc<dyn ObjectStore>,
    stats: Arc<ObjectStoreStats>,
}

impl StatsStore {
    pub fn new(inner: Arc<dyn ObjectStore>, stats: Arc<ObjectStoreStats>) -> Self {
        Self { inner, stats }
    }
}

impl fmt::Debug for StatsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatsStore").finish_non_exhaustive()
    }
}

impl fmt::Display for StatsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StatsStore({})", self.inner)
    }
}

/// Counts bytes of a multipart upload as parts complete.
#[derive(Debug)]
struct StatsUpload {
    inner: Box<dyn MultipartUpload>,
    stats: Arc<ObjectStoreStats>,
    object: Arc<ObjectCounters>,
}

#[async_trait]
impl MultipartUpload for StatsUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = data.content_length() as u64;
        let started = Instant::now();
        let stats = Arc::clone(&self.stats);
        let object = Arc::clone(&self.object);
        let part = self.inner.put_part(data);
        Box::pin(async move {
            let result = part.await;
            stats.finish(Op::Multipart, started, bytes, result.as_ref().err());
            if result.is_ok() {
                object.bytes_put.fetch_add(bytes, Ordering::Relaxed);
            }
            result
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let started = Instant::now();
        let result = self.inner.complete().await;
        self.stats
            .finish(Op::Multipart, started, 0, result.as_ref().err());
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.abort().await;
        self.stats
            .finish(Op::Multipart, started, 0, result.as_ref().err());
        result
    }
}

#[async_trait]
impl ObjectStore for StatsStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let bytes = payload.content_length() as u64;
        let object = self.stats.counters_for(location);
        let started = Instant::now();
        let result = self.inner.put_opts(location, payload, opts).await;
        self.stats
            .finish(Op::Put, started, bytes, result.as_ref().err());
        object.put_requests.fetch_add(1, Ordering::Relaxed);
        if result.is_ok() {
            object.bytes_put.fetch_add(bytes, Ordering::Relaxed);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        let object = self.stats.counters_for(location);
        let started = Instant::now();
        let result = self.inner.put_multipart_opts(location, opts).await;
        self.stats
            .finish(Op::Multipart, started, 0, result.as_ref().err());
        object.put_requests.fetch_add(1, Ordering::Relaxed);
        let inner = result?;
        Ok(Box::new(StatsUpload {
            inner,
            stats: Arc::clone(&self.stats),
            object,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let ranged = options.range.is_some();
        let op = if ranged { Op::GetRange } else { Op::Get };
        let object = self.stats.counters_for(location);
        let started = Instant::now();
        let result = self.inner.get_opts(location, options).await;
        self.stats.finish(op, started, 0, result.as_ref().err());
        let mut result = result?;
        let kind = if ranged {
            &object.get_ranged
        } else {
            &object.get_whole
        };
        kind.fetch_add(1, Ordering::Relaxed);
        object.observe_size(result.meta.size as u64);
        let delivered = result.range.end.saturating_sub(result.range.start) as u64;
        result.payload = match result.payload {
            GetResultPayload::Stream(stream) => {
                let stats = Arc::clone(&self.stats);
                GetResultPayload::Stream(
                    stream
                        .map(move |chunk| {
                            if let Ok(bytes) = &chunk {
                                let n = bytes.len() as u64;
                                object.bytes_fetched.fetch_add(n, Ordering::Relaxed);
                                stats.ops[op.idx()].bytes.fetch_add(n, Ordering::Relaxed);
                            }
                            chunk
                        })
                        .boxed(),
                )
            }
            file @ GetResultPayload::File(..) => {
                object.bytes_fetched.fetch_add(delivered, Ordering::Relaxed);
                self.stats.ops[op.idx()]
                    .bytes
                    .fetch_add(delivered, Ordering::Relaxed);
                file
            }
        };
        Ok(result)
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let requested = range.end.saturating_sub(range.start) as u64;
        let object = self.stats.counters_for(location);
        let started = Instant::now();
        let result = self.inner.get_range(location, range).await;
        let got = result.as_ref().map_or(0, |b| b.len() as u64);
        self.stats
            .finish(Op::GetRange, started, got, result.as_ref().err());
        object.get_ranged.fetch_add(1, Ordering::Relaxed);
        object
            .bytes_requested
            .fetch_add(requested, Ordering::Relaxed);
        object.bytes_fetched.fetch_add(got, Ordering::Relaxed);
        result
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let requested: u64 = ranges
            .iter()
            .map(|r| r.end.saturating_sub(r.start) as u64)
            .sum();
        let object = self.stats.counters_for(location);
        let started = Instant::now();
        let result = self.inner.get_ranges(location, ranges).await;
        let got = result
            .as_ref()
            .map_or(0, |all| all.iter().map(|b| b.len() as u64).sum());
        self.stats
            .finish(Op::GetRange, started, got, result.as_ref().err());
        object
            .get_ranged
            .fetch_add(ranges.len() as u64, Ordering::Relaxed);
        object
            .bytes_requested
            .fetch_add(requested, Ordering::Relaxed);
        object.bytes_fetched.fetch_add(got, Ordering::Relaxed);
        result
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let started = Instant::now();
        let result = self.inner.head(location).await;
        self.stats
            .finish(Op::Head, started, 0, result.as_ref().err());
        result
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.delete(location).await;
        self.stats
            .finish(Op::Delete, started, 0, result.as_ref().err());
        result
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        // A listing is a paged stream; the request is counted when it is
        // issued, and an item error counts as an error.
        let started = Instant::now();
        let stats = Arc::clone(&self.stats);
        stats.finish(Op::List, started, 0, None);
        self.inner
            .list(prefix)
            .inspect(move |item| {
                if let Err(e) = item {
                    stats.finish(Op::List, Instant::now(), 0, Some(e));
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let started = Instant::now();
        let result = self.inner.list_with_delimiter(prefix).await;
        self.stats
            .finish(Op::List, started, 0, result.as_ref().err());
        result
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.copy(from, to).await;
        self.stats
            .finish(Op::Other, started, 0, result.as_ref().err());
        result
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.copy_if_not_exists(from, to).await;
        self.stats
            .finish(Op::Other, started, 0, result.as_ref().err());
        result
    }
}

#[cfg(test)]
mod tests;
