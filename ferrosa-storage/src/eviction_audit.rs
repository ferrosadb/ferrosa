//! Durable, bounded audit trail of cache-eviction passes.
//!
//! On 2026-09-29 about 1000 SSTables were evicted and nobody could say why:
//! the reason lived only in a stdout log that had rotated away. This records,
//! for every eviction PASS that found pressure, what the evictor saw and did,
//! in files under `<data_dir>/eviction-audit/` that the engine itself bounds.
//!
//! # Record
//!
//! One JSON object per line ([`PassRecord`]): timestamps, the pass's trigger,
//! `max_bytes` / `min_bytes` / `target_free` / `projected_available`, the
//! manifest's byte claim AND the real on-disk total of the same set (the gap
//! between them is the signal, FMEA ST-64), duplicate manifest entries, how
//! many generations were evicted and their size, and the writer's pid and
//! build version.
//!
//! # Hard bound on disk
//!
//! The audit must never fill the disk: the thing recording disk-pressure
//! decisions would itself cause them. Total size is bounded BY CONSTRUCTION,
//! however often eviction fires:
//!
//! * the budget is `FERROSA_EVICTION_AUDIT_MAX_BYTES` (default 4 MiB, clamped
//!   to 16 KiB..=32 MiB);
//! * it is split into [`SEGMENTS`] equal segments after a 4 KiB reserve: one
//!   `audit.current.jsonl` and at most `SEGMENTS - 1` rotated
//!   `audit.<seq>.jsonl`;
//! * a record never lands in a segment it does not fit; a full current
//!   segment is rotated, and the oldest rotated segment is deleted BEFORE the
//!   rename, so there are never more than `SEGMENTS` segments on disk;
//! * records are capped at [`MAX_RECORD_BYTES`], far below the smallest
//!   segment.
//!
//! The default is 4 MiB against the 512 MiB default
//! `FERROSA_LOCAL_DISK_FREE_RESERVE_BYTES`: under 0.8% of the margin. The
//! ceiling, 32 MiB, is 6.3% of it. Identical consecutive passes are coalesced
//! into one record with a count instead of appended, so a steady state costs
//! one record, not one per sync.
//!
//! # Never delays or fails an eviction
//!
//! [`EvictionAudit::record_pass`] returns nothing and never panics. A write
//! failure is reported on the edges (one WARN when writes start failing, one
//! INFO when they recover) and counted in
//! `ferrosa_storage_eviction_audit_write_failures_total`. The eviction marker's
//! durability always takes priority over the audit record.
//!
//! # Optional S3 offload
//!
//! Off by default (`FERROSA_EVICTION_AUDIT_OFFLOAD=true` enables it). Rotated
//! segments are uploaded to `<prefix>/eviction-audit/<instance>/<segment>`
//! through the engine's shared, throttled object store, so
//! `FERROSA_S3_MAX_REQUESTS_PER_SECOND` / `MAX_CONCURRENT_REQUESTS` apply. It
//! runs after the eviction pass, uploads at most ONE segment per sync with a
//! timeout and no retry (the next sync is the retry), and deletes the local
//! segment only after the put is confirmed. A failed upload leaves the local
//! segment in place. The disk bound outranks offload: if uploads keep failing,
//! the ring still drops the oldest un-uploaded segment by age (logged).

use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::eviction_marker::Trigger;

/// Default total on-disk budget for the audit files.
pub const DEFAULT_MAX_BYTES: u64 = 4 * 1024 * 1024;
/// Smallest accepted budget.
pub const MIN_MAX_BYTES: u64 = 16 * 1024;
/// Largest accepted budget (6.3% of the default 512 MiB free-space reserve).
pub const MAX_MAX_BYTES: u64 = 32 * 1024 * 1024;
/// Segments in the ring: the current one plus `SEGMENTS - 1` rotated.
pub const SEGMENTS: u64 = 4;
// The ceiling stays under 7% of the default 512 MiB free-space reserve, and
// the smallest segment holds several records. Checked at compile time.
const _: () = assert!(MAX_MAX_BYTES * 100 / (512 * 1024 * 1024) < 7);
const _: () = assert!((MIN_MAX_BYTES - 4 * 1024) / SEGMENTS > 2 * MAX_RECORD_BYTES as u64);

/// Space set aside for the instance-id file and slack.
const RESERVED_BYTES: u64 = 4 * 1024;
/// Upper bound on one serialized record.
pub const MAX_RECORD_BYTES: usize = 1024;
/// Time allowed for one offload upload.
const OFFLOAD_TIMEOUT: Duration = Duration::from_secs(15);

const CURRENT: &str = "audit.current.jsonl";
const INSTANCE_FILE: &str = "instance-id";

/// One eviction pass, or several identical consecutive ones.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PassRecord {
    /// Schema version.
    pub version: u32,
    /// First pass folded into this record, ms since the Unix epoch.
    pub first_unix_ms: u64,
    /// Last pass folded into this record.
    pub last_unix_ms: u64,
    /// Passes folded into this record (>= 1).
    pub count: u64,
    /// Writer process id.
    pub pid: u32,
    /// Writer build version.
    pub build: String,
    /// Why the pass started evicting.
    pub trigger: Trigger,
    /// `local_cache_max_bytes`.
    pub max_bytes: u64,
    /// Cache floor.
    pub min_bytes: u64,
    /// Free-disk target (0 = disabled).
    pub target_free: u64,
    /// Free disk bytes projected at the end of the pass.
    pub projected_available: u64,
    /// What the manifest claims for the eligible generations.
    pub manifest_bytes: u64,
    /// What those generations occupy on disk. The evictor's decision input.
    pub disk_bytes: u64,
    /// Manifest entries skipped as duplicates.
    pub duplicate_entries: u64,
    /// Generations evicted by this pass.
    pub evicted_generations: u64,
    /// Bytes of the generations evicted by this pass.
    pub evicted_bytes: u64,
}

impl PassRecord {
    /// Whether `other` is the same decision, so the two coalesce. Timestamps,
    /// count and `projected_available` (which moves every pass) are excluded.
    pub fn same_decision(&self, other: &Self) -> bool {
        self.pid == other.pid
            && self.build == other.build
            && self.trigger == other.trigger
            && self.max_bytes == other.max_bytes
            && self.min_bytes == other.min_bytes
            && self.target_free == other.target_free
            && self.manifest_bytes == other.manifest_bytes
            && self.disk_bytes == other.disk_bytes
            && self.duplicate_entries == other.duplicate_entries
            && self.evicted_generations == other.evicted_generations
            && self.evicted_bytes == other.evicted_bytes
    }
}

/// Where the audit lives and how big it may be.
#[derive(Debug, Clone)]
pub struct AuditConfig {
    /// Directory holding the segments.
    pub dir: PathBuf,
    /// Total byte budget (clamped).
    pub max_bytes: u64,
    /// Upload rotated segments to S3.
    pub offload: bool,
}

impl AuditConfig {
    /// Config for `data_dir` from `FERROSA_EVICTION_AUDIT_MAX_BYTES` and
    /// `FERROSA_EVICTION_AUDIT_OFFLOAD`. A malformed value is logged and the
    /// default used.
    pub fn from_env(data_dir: &Path) -> Self {
        let max_bytes = match std::env::var("FERROSA_EVICTION_AUDIT_MAX_BYTES") {
            Ok(v) => v.parse::<u64>().unwrap_or_else(|e| {
                tracing::warn!(value = %v, error = %e, "FERROSA_EVICTION_AUDIT_MAX_BYTES is not a number; using the default");
                DEFAULT_MAX_BYTES
            }),
            Err(_) => DEFAULT_MAX_BYTES,
        };
        let offload = matches!(
            std::env::var("FERROSA_EVICTION_AUDIT_OFFLOAD").as_deref(),
            Ok("1" | "true" | "TRUE" | "yes")
        );
        Self::new(data_dir.join("eviction-audit"), max_bytes, offload)
    }

    /// Config with an explicit directory and budget (clamped).
    pub fn new(dir: PathBuf, max_bytes: u64, offload: bool) -> Self {
        Self {
            dir,
            max_bytes: max_bytes.clamp(MIN_MAX_BYTES, MAX_MAX_BYTES),
            offload,
        }
    }

    /// Size of one ring segment.
    pub fn segment_bytes(&self) -> u64 {
        (self.max_bytes - RESERVED_BYTES) / SEGMENTS
    }
}

/// A transition of a failure condition, for edge-only reporting.
#[derive(Debug, PartialEq, Eq)]
enum Edge {
    Started,
    Cleared,
    Unchanged,
}

fn edge(flag: &AtomicBool, failing_now: bool) -> Edge {
    match (flag.swap(failing_now, Ordering::Relaxed), failing_now) {
        (false, true) => Edge::Started,
        (true, false) => Edge::Cleared,
        _ => Edge::Unchanged,
    }
}

#[derive(Default)]
struct Inner {
    ready: bool,
    last: Option<PassRecord>,
    current_len: u64,
    last_line_start: u64,
    next_seq: u64,
}

/// The audit writer. Cheap to construct: no I/O until the first record.
pub struct EvictionAudit {
    config: AuditConfig,
    inner: Mutex<Inner>,
    write_failing: AtomicBool,
    offload_failing: AtomicBool,
    write_failure_edges: AtomicU64,
}

impl EvictionAudit {
    /// A writer for `config`.
    pub fn new(config: AuditConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(Inner::default()),
            write_failing: AtomicBool::new(false),
            offload_failing: AtomicBool::new(false),
            write_failure_edges: AtomicU64::new(0),
        }
    }

    /// The configuration in force.
    pub fn config(&self) -> &AuditConfig {
        &self.config
    }

    /// Whether audit writes are currently failing.
    pub fn is_failing(&self) -> bool {
        self.write_failing.load(Ordering::Relaxed)
    }

    /// How many times writes STARTED failing (edges, not failures).
    pub fn write_failure_edges(&self) -> u64 {
        self.write_failure_edges.load(Ordering::Relaxed)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record one eviction pass. Never fails and never panics: a write error
    /// is reported on the edges and the eviction goes on.
    pub fn record_pass(&self, record: PassRecord) {
        let result = {
            let mut inner = self.lock();
            let result = self.write(&mut inner, record.clone());
            if result.is_err() {
                // Re-learn lengths from disk on the next attempt.
                inner.ready = false;
                inner.last = None;
            }
            result
        };
        crate::metrics::observe_eviction_audit(&record, result.is_ok());
        match (edge(&self.write_failing, result.is_err()), &result) {
            (Edge::Started, Err(e)) => {
                self.write_failure_edges.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    dir = %self.config.dir.display(),
                    error = %e,
                    "eviction audit: cannot write the audit record; evictions proceed and are \\
                     still marked, but this pass and later ones are unrecorded until it recovers"
                );
            }
            (Edge::Cleared, _) => tracing::info!(
                dir = %self.config.dir.display(),
                "eviction audit: audit writes recovered"
            ),
            _ => {}
        }
    }

    fn write(&self, inner: &mut Inner, record: PassRecord) -> std::io::Result<()> {
        if !inner.ready {
            self.init(inner)?;
        }
        let segment = self.config.segment_bytes();
        let record = match inner.last.take() {
            Some(last) if last.same_decision(&record) => {
                let merged = PassRecord {
                    count: last.count + record.count,
                    last_unix_ms: record.last_unix_ms,
                    projected_available: record.projected_available,
                    ..last
                };
                let line = encode(&merged)?;
                if inner.last_line_start + line.len() as u64 <= segment {
                    return self.rewrite_tail(inner, merged, &line);
                }
                merged
            }
            _ => record,
        };
        self.append_new(inner, record, segment)
    }

    fn init(&self, inner: &mut Inner) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.config.dir)?;
        inner.current_len = match std::fs::metadata(self.config.dir.join(CURRENT)) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e),
        };
        inner.next_seq = rotated_segments(&self.config.dir)?
            .last()
            .map_or(0, |(seq, _)| seq + 1);
        inner.last = None;
        inner.last_line_start = inner.current_len;
        inner.ready = true;
        Ok(())
    }

    fn rewrite_tail(
        &self,
        inner: &mut Inner,
        merged: PassRecord,
        line: &[u8],
    ) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(self.config.dir.join(CURRENT))?;
        file.set_len(inner.last_line_start)?;
        file.seek(SeekFrom::Start(inner.last_line_start))?;
        file.write_all(line)?;
        file.sync_data()?;
        inner.current_len = inner.last_line_start + line.len() as u64;
        inner.last = Some(merged);
        Ok(())
    }

    fn append_new(
        &self,
        inner: &mut Inner,
        record: PassRecord,
        segment: u64,
    ) -> std::io::Result<()> {
        let line = encode(&record)?;
        if inner.current_len + line.len() as u64 > segment {
            self.rotate(inner)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.config.dir.join(CURRENT))?;
        file.write_all(&line)?;
        file.sync_data()?;
        inner.last_line_start = inner.current_len;
        inner.current_len += line.len() as u64;
        inner.last = Some(record);
        Ok(())
    }

    /// Retire the current segment. The oldest rotated segments are removed
    /// FIRST so the ring never holds more than [`SEGMENTS`] files.
    fn rotate(&self, inner: &mut Inner) -> std::io::Result<()> {
        let mut rotated = rotated_segments(&self.config.dir)?;
        while rotated.len() as u64 >= SEGMENTS - 1 {
            let (seq, path) = rotated.remove(0);
            std::fs::remove_file(&path)?;
            tracing::info!(
                segment = seq,
                offload = self.config.offload,
                "eviction audit: ring full, dropped the oldest segment"
            );
        }
        let current = self.config.dir.join(CURRENT);
        if inner.current_len > 0 {
            std::fs::rename(&current, rotated_path(&self.config.dir, inner.next_seq))?;
            inner.next_seq += 1;
        }
        inner.current_len = 0;
        inner.last_line_start = 0;
        inner.last = None;
        Ok(())
    }

    /// Upload the oldest rotated segment, when offload is enabled. Strictly
    /// best-effort: at most one segment, one attempt, a timeout, and the local
    /// copy goes only after the put succeeded. Failures are reported on the
    /// edges and leave the segment in place.
    pub async fn offload_rotated(&self, store: &Arc<dyn object_store::ObjectStore>, prefix: &str) {
        if !self.config.offload {
            return;
        }
        let outcome = self.offload_one(store, prefix).await;
        crate::metrics::observe_eviction_audit_offload(
            matches!(outcome, Ok(true)),
            outcome.is_err(),
        );
        match (edge(&self.offload_failing, outcome.is_err()), outcome) {
            (Edge::Started, Err(e)) => tracing::warn!(
                error = %e,
                "eviction audit: cannot offload a rotated segment to S3; the local copy is kept"
            ),
            (Edge::Cleared, _) => {
                tracing::info!("eviction audit: S3 offload recovered")
            }
            _ => {}
        }
    }

    /// `Ok(true)` uploaded and removed, `Ok(false)` nothing to do.
    async fn offload_one(
        &self,
        store: &Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) -> Result<bool, String> {
        let Some((_, path)) = rotated_segments(&self.config.dir)
            .map_err(|e| format!("cannot list segments: {e}"))?
            .into_iter()
            .next()
        else {
            return Ok(false);
        };
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            // The ring dropped it between listing and reading: nothing to do.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
        };
        let instance = self
            .instance_id()
            .map_err(|e| format!("instance id: {e}"))?;
        let key = if prefix.is_empty() {
            format!("eviction-audit/{instance}/{name}")
        } else {
            format!("{prefix}/eviction-audit/{instance}/{name}")
        };
        let location = object_store::path::Path::from(key);
        let put = store.put(&location, bytes.into());
        match tokio::time::timeout(OFFLOAD_TIMEOUT, put).await {
            Err(_) => return Err(format!("upload timed out after {OFFLOAD_TIMEOUT:?}")),
            Ok(Err(e)) => return Err(format!("upload failed: {e}")),
            Ok(Ok(_)) => {}
        }
        std::fs::remove_file(&path)
            .map_err(|e| format!("uploaded but could not remove {}: {e}", path.display()))?;
        Ok(true)
    }

    /// This data directory's stable instance id, created on first use.
    fn instance_id(&self) -> std::io::Result<String> {
        std::fs::create_dir_all(&self.config.dir)?;
        let path = self.config.dir.join(INSTANCE_FILE);
        match std::fs::read_to_string(&path) {
            Ok(id) if !id.trim().is_empty() => Ok(id.trim().to_string()),
            _ => {
                let id = uuid::Uuid::new_v4().simple().to_string();
                std::fs::write(&path, &id)?;
                Ok(id)
            }
        }
    }
}

fn encode(record: &PassRecord) -> std::io::Result<Vec<u8>> {
    let mut line = serde_json::to_vec(record).map_err(std::io::Error::other)?;
    line.push(b'\n');
    if line.len() > MAX_RECORD_BYTES {
        return Err(std::io::Error::other(format!(
            "audit record is {} bytes, over the {MAX_RECORD_BYTES} byte cap",
            line.len()
        )));
    }
    Ok(line)
}

fn rotated_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("audit.{seq:020}.jsonl"))
}

/// Rotated segments, oldest first.
fn rotated_segments(dir: &Path) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(seq) = name
            .to_str()
            .and_then(|n| n.strip_prefix("audit."))
            .and_then(|n| n.strip_suffix(".jsonl"))
            .and_then(|n| n.parse::<u64>().ok())
        else {
            continue;
        };
        out.push((seq, entry.path()));
    }
    out.sort_by_key(|(seq, _)| *seq);
    Ok(out)
}

/// Milliseconds since the Unix epoch.
pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pass with distinguishing figures `n`.
    fn pass(n: u64) -> PassRecord {
        PassRecord {
            version: 1,
            first_unix_ms: 1_000 + n,
            last_unix_ms: 1_000 + n,
            count: 1,
            pid: std::process::id(),
            build: env!("CARGO_PKG_VERSION").to_string(),
            trigger: Trigger::CacheCap,
            max_bytes: 100,
            min_bytes: 0,
            target_free: 0,
            projected_available: 5,
            manifest_bytes: 160,
            disk_bytes: 160,
            duplicate_entries: 0,
            evicted_generations: 1,
            evicted_bytes: n,
        }
    }

    fn audit(dir: &Path, max: u64, offload: bool) -> EvictionAudit {
        EvictionAudit::new(AuditConfig::new(dir.join("audit"), max, offload))
    }

    fn dir_bytes(dir: &Path) -> u64 {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().metadata().unwrap().len())
            .sum()
    }

    fn all_records(dir: &Path) -> Vec<PassRecord> {
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
            .collect();
        files.sort();
        files
            .iter()
            .flat_map(|p| {
                std::fs::read_to_string(p)
                    .unwrap()
                    .lines()
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .map(|l| serde_json::from_str(&l).expect("every line is a record"))
            .collect()
    }

    /// Far more distinct passes than fit: the disk stays under the budget,
    /// the oldest records are gone and the newest survive.
    #[test]
    fn the_audit_stays_under_its_cap_and_keeps_the_newest_records() {
        let tmp = tempfile::tempdir().unwrap();
        let cap = MIN_MAX_BYTES;
        let audit = audit(tmp.path(), cap, false);
        let dir = tmp.path().join("audit");
        let passes = 600u64;
        let mut peak = 0;
        for n in 0..passes {
            audit.record_pass(pass(n));
            peak = peak.max(dir_bytes(&dir));
        }
        let records = all_records(&dir);
        assert!(peak <= cap, "peak {peak} bytes exceeded the {cap} byte cap");
        assert!(
            (records.len() as u64) < passes / 10,
            "{} of {passes} records kept: the ring did not drop anything",
            records.len()
        );
        assert!(
            records.iter().any(|r| r.evicted_bytes == passes - 1),
            "newest survives"
        );
        assert!(
            !records.iter().any(|r| r.evicted_bytes == 0),
            "oldest was dropped"
        );
        let kept: Vec<u64> = records.iter().map(|r| r.evicted_bytes).collect();
        assert!(
            kept.windows(2).all(|w| w[0] < w[1]),
            "kept records are the contiguous newest, in order"
        );
        assert_eq!(*kept.last().unwrap(), passes - 1);
        assert_eq!(
            kept.len() as u64,
            kept.last().unwrap() - kept.first().unwrap() + 1
        );
    }

    /// Identical consecutive passes become one record with a count.
    #[test]
    fn identical_consecutive_passes_coalesce() {
        let tmp = tempfile::tempdir().unwrap();
        let audit = audit(tmp.path(), DEFAULT_MAX_BYTES, false);
        let dir = tmp.path().join("audit");
        let mut first = pass(7);
        audit.record_pass(first.clone());
        for t in 1..=4 {
            let mut again = pass(7);
            again.last_unix_ms = 5_000 + t;
            again.projected_available = 100 + t;
            audit.record_pass(again);
        }
        audit.record_pass(pass(8));

        let records = all_records(&dir);
        assert_eq!(records.len(), 2, "five identical passes plus one different");
        first.count = 5;
        first.last_unix_ms = 5_004;
        first.projected_available = 104;
        assert_eq!(records[0], first);
        assert_eq!(records[1].evicted_bytes, 8);
        assert_eq!(records[1].count, 1);
    }

    /// An unwritable audit does not panic, reports one WARN edge however many
    /// passes fail, and one recovery edge when it works again.
    #[test]
    fn a_write_failure_is_reported_once_and_recovery_once() {
        let tmp = tempfile::tempdir().unwrap();
        let audit = audit(tmp.path(), DEFAULT_MAX_BYTES, false);
        // A regular file where the audit directory must go.
        std::fs::write(tmp.path().join("audit"), b"in the way").unwrap();
        for n in 0..5 {
            audit.record_pass(pass(n));
        }
        assert!(audit.is_failing());
        assert_eq!(audit.write_failure_edges(), 1, "five failures, one edge");

        std::fs::remove_file(tmp.path().join("audit")).unwrap();
        audit.record_pass(pass(99));
        assert!(!audit.is_failing(), "recovered");
        assert_eq!(all_records(&tmp.path().join("audit")).len(), 1);
        assert_eq!(audit.write_failure_edges(), 1);
    }

    #[test]
    fn the_budget_is_clamped_and_segments_fit_a_record() {
        assert_eq!(
            AuditConfig::new("x".into(), 0, false).max_bytes,
            MIN_MAX_BYTES
        );
        assert_eq!(
            AuditConfig::new("x".into(), u64::MAX, false).max_bytes,
            MAX_MAX_BYTES
        );
        let tight = AuditConfig::new("x".into(), 0, false);
        assert!(tight.segment_bytes() > MAX_RECORD_BYTES as u64 * 2);
    }

    #[test]
    fn a_new_process_does_not_coalesce_into_an_old_one() {
        let tmp = tempfile::tempdir().unwrap();
        audit(tmp.path(), DEFAULT_MAX_BYTES, false).record_pass(pass(3));
        audit(tmp.path(), DEFAULT_MAX_BYTES, false).record_pass(pass(3));
        assert_eq!(all_records(&tmp.path().join("audit")).len(), 2);
    }

    // ---- offload ----

    /// Fill the ring so there are rotated segments, then return the audit.
    fn audit_with_rotated_segments(tmp: &Path, offload: bool) -> EvictionAudit {
        let audit = audit(tmp, MIN_MAX_BYTES, offload);
        for n in 0..40 {
            audit.record_pass(pass(n));
        }
        assert!(
            !rotated_segments(&tmp.join("audit")).unwrap().is_empty(),
            "setup: a rotated segment exists"
        );
        audit
    }

    #[derive(Debug)]
    struct RejectPuts(Arc<dyn object_store::ObjectStore>);

    impl std::fmt::Display for RejectPuts {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RejectPuts")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for RejectPuts {
        async fn put_opts(
            &self,
            _l: &object_store::path::Path,
            _p: object_store::PutPayload,
            _o: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            Err(object_store::Error::NotSupported {
                source: "simulated outage".into(),
            })
        }
        async fn put_multipart_opts(
            &self,
            _l: &object_store::path::Path,
            _o: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            Err(object_store::Error::NotSupported {
                source: "simulated outage".into(),
            })
        }
        async fn get_opts(
            &self,
            l: &object_store::path::Path,
            o: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.0.get_opts(l, o).await
        }
        async fn delete(&self, l: &object_store::path::Path) -> object_store::Result<()> {
            self.0.delete(l).await
        }
        fn list(
            &self,
            p: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>>
        {
            self.0.list(p)
        }
        async fn list_with_delimiter(
            &self,
            p: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.0.list_with_delimiter(p).await
        }
        async fn copy(
            &self,
            f: &object_store::path::Path,
            t: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.0.copy(f, t).await
        }
        async fn copy_if_not_exists(
            &self,
            f: &object_store::path::Path,
            t: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.0.copy_if_not_exists(f, t).await
        }
    }

    async fn stored_objects(
        store: &Arc<dyn object_store::ObjectStore>,
    ) -> Vec<object_store::ObjectMeta> {
        use futures::StreamExt;
        store.list(None).map(|m| m.unwrap()).collect().await
    }

    #[tokio::test]
    async fn with_offload_off_nothing_touches_the_object_store() {
        let tmp = tempfile::tempdir().unwrap();
        let audit = audit_with_rotated_segments(tmp.path(), false);
        let before = rotated_segments(&tmp.path().join("audit")).unwrap().len();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());

        audit.offload_rotated(&store, "pfx").await;

        assert!(
            stored_objects(&store).await.is_empty(),
            "no object was written"
        );
        assert_eq!(
            rotated_segments(&tmp.path().join("audit")).unwrap().len(),
            before
        );
        assert!(
            !tmp.path().join("audit").join(INSTANCE_FILE).exists(),
            "no instance id minted either"
        );
    }

    #[tokio::test]
    async fn with_offload_on_a_rotated_segment_is_uploaded_then_removed_locally() {
        let tmp = tempfile::tempdir().unwrap();
        let audit = audit_with_rotated_segments(tmp.path(), true);
        let dir = tmp.path().join("audit");
        let (_, oldest) = rotated_segments(&dir).unwrap().remove(0);
        let local = std::fs::read(&oldest).unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());

        audit.offload_rotated(&store, "pfx").await;

        let objects = stored_objects(&store).await;
        assert_eq!(objects.len(), 1, "one segment per call");
        let key = objects[0].location.to_string();
        assert!(
            key.starts_with("pfx/eviction-audit/")
                && key.ends_with(oldest.file_name().unwrap().to_str().unwrap()),
            "{key}"
        );
        let remote = store
            .get(&objects[0].location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            remote.as_ref(),
            local.as_slice(),
            "uploaded bytes match the segment"
        );
        assert!(
            !oldest.exists(),
            "local copy removed after the confirmed upload"
        );
        assert!(
            dir.join(CURRENT).exists(),
            "the current segment is never offloaded"
        );
    }

    #[tokio::test]
    async fn a_failed_upload_keeps_the_local_segment_and_reports_once() {
        let tmp = tempfile::tempdir().unwrap();
        let audit = audit_with_rotated_segments(tmp.path(), true);
        let dir = tmp.path().join("audit");
        let before: Vec<_> = rotated_segments(&dir).unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(RejectPuts(Arc::new(object_store::memory::InMemory::new())));

        audit.offload_rotated(&store, "pfx").await;
        audit.offload_rotated(&store, "pfx").await;

        assert_eq!(
            rotated_segments(&dir).unwrap(),
            before,
            "no segment was deleted"
        );
        assert!(audit.offload_failing.load(Ordering::Relaxed));
    }
}
