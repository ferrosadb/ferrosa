//! The eviction marker: `<table dir>/<gen>.evicted`.
//!
//! A marker is the only thing that tells "the uploaded-cache evictor removed
//! this generation, restore it from S3" apart from "compaction retired it,
//! leave it gone". Since the 2026-09-29 incident it also records WHY, so an
//! eviction can be explained after the log that announced it has rotated.
//!
//! # Format
//!
//! One JSON object ([`EvictionRecord`]). Every byte figure is optional because
//! the writers know different things: the evictor knows them all, a recovery
//! tool knows none. Unknown fields are ignored, so a newer writer's marker
//! still reads here.
//!
//! # Compatibility
//!
//! * An EMPTY file is a legacy marker (written before records existed). It is
//!   honoured exactly like any marker and reads as [`MarkerState::Legacy`]:
//!   "evicted, reason unknown".
//! * A file that does not parse (truncated, garbage, unreadable) is still a
//!   marker: the generation was evicted. It reads as
//!   [`MarkerState::Unreadable`] carrying the reason, and callers report it
//!   loudly. It never fails startup and is never silently ignored.
//!
//! Presence of the file is what restore keys on; the content is provenance.
//!
//! # Durability
//!
//! [`write_marker`] writes a temporary file, fsyncs it, renames it into place
//! and fsyncs the directory. The evictor calls it BEFORE it deletes any
//! component, so a crash between the two can never lose track of an evicted
//! SSTable. A crash mid-write leaves at worst a stray `.<gen>.evicted.tmp`,
//! which no discovery path matches.

use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// `source` written by the uploaded-cache evictor in the engine.
pub const SOURCE_EVICTOR: &str = "ferrosa-storage evictor";

/// Current record schema version.
pub const RECORD_VERSION: u32 = 1;

/// Why a generation was evicted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// The uploaded-SSTable cache was over `local_cache_max_bytes`.
    CacheCap,
    /// Free disk space was under the eviction target.
    FreeSpace,
    /// Both limits were exceeded at once.
    CacheCapAndFreeSpace,
    /// Not an eviction decision: a recovery tool marked a generation that an
    /// earlier eviction removed. The original reason is not known.
    Recovered,
}

impl Trigger {
    /// The trigger implied by which limits were exceeded. `None` when neither
    /// was, which means the caller should not be evicting.
    pub fn from_limits(over_cache_limit: bool, under_free_target: bool) -> Option<Self> {
        match (over_cache_limit, under_free_target) {
            (true, true) => Some(Self::CacheCapAndFreeSpace),
            (true, false) => Some(Self::CacheCap),
            (false, true) => Some(Self::FreeSpace),
            (false, false) => None,
        }
    }
}

/// The content of a non-empty marker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictionRecord {
    /// Schema version of this record ([`RECORD_VERSION`]).
    pub version: u32,
    /// Why the generation was evicted.
    pub trigger: Trigger,
    /// Who wrote the marker: [`SOURCE_EVICTOR`], or the recovery command line
    /// (`ferrosa-ctl sstable mark-evicted`). Distinguishes a real eviction
    /// from a marker written by hand or by a tool.
    pub source: String,
    /// Wall clock when the marker was written, ms since the Unix epoch.
    pub written_at_unix_ms: u64,
    /// Local size of the generation being evicted.
    #[serde(default)]
    pub generation_bytes: Option<u64>,
    /// Uploaded-cache bytes before this eviction.
    #[serde(default)]
    pub total_bytes: Option<u64>,
    /// `local_cache_max_bytes`.
    #[serde(default)]
    pub max_bytes: Option<u64>,
    /// The cache floor the evictor will not go below.
    #[serde(default)]
    pub min_bytes: Option<u64>,
    /// Free disk bytes the evictor projected at this point.
    #[serde(default)]
    pub projected_available: Option<u64>,
    /// Free disk bytes the evictor was trying to reach (0 = disabled).
    #[serde(default)]
    pub target_free: Option<u64>,
    /// Index artifacts (`.sidecar`, full-text, vector files; component names
    /// after `{gen}-`) the generation held locally when it was evicted. A
    /// restore must obtain every one of them: an index without its postings
    /// answers `Ok` with no rows. `None` means unknown (a legacy or recovered
    /// marker), so only what the object store lists can be pulled.
    #[serde(default)]
    pub index_artifacts: Option<Vec<String>>,
}

impl EvictionRecord {
    /// A record with no figures, for writers that know only who they are.
    pub fn bare(trigger: Trigger, source: impl Into<String>) -> Self {
        Self {
            version: RECORD_VERSION,
            trigger,
            source: source.into(),
            written_at_unix_ms: now_unix_ms(),
            generation_bytes: None,
            total_bytes: None,
            max_bytes: None,
            min_bytes: None,
            projected_available: None,
            target_free: None,
            index_artifacts: None,
        }
    }
}

/// What a marker file says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerState {
    /// A readable record.
    Recorded(EvictionRecord),
    /// Empty file: written before records existed. Evicted, reason unknown.
    Legacy,
    /// Present but not a valid record (truncated, garbage, unreadable). Still
    /// an eviction; carries why it could not be read.
    Unreadable(String),
}

impl MarkerState {
    /// Whether the reason for the eviction is known.
    pub fn is_reason_known(&self) -> bool {
        matches!(self, Self::Recorded(r) if r.trigger != Trigger::Recovered)
    }
}

/// Milliseconds since the Unix epoch; 0 only if the clock is before it.
fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "eviction marker: system clock is before the Unix epoch; recording 0");
            0
        })
}

/// Durably write the marker for `gen` into `table_dir`: temp file, fsync,
/// rename, fsync of the directory. Overwrites an existing marker.
pub fn write_marker(table_dir: &Path, gen: &str, record: &EvictionRecord) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(record).map_err(std::io::Error::other)?;
    let tmp = table_dir.join(format!(".{gen}.evicted.tmp"));
    let mut file = std::fs::File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, marker_path(table_dir, gen))?;
    std::fs::File::open(table_dir)?.sync_all()
}

/// Path of the marker for `gen`.
pub fn marker_path(table_dir: &Path, gen: &str) -> PathBuf {
    table_dir.join(format!("{gen}.evicted"))
}

/// Read a marker. Never fails: every outcome is a [`MarkerState`].
pub fn read_marker(path: &Path) -> MarkerState {
    match std::fs::read(path) {
        Ok(bytes) if bytes.is_empty() => MarkerState::Legacy,
        Ok(bytes) => match serde_json::from_slice::<EvictionRecord>(&bytes) {
            Ok(record) => MarkerState::Recorded(record),
            Err(e) => MarkerState::Unreadable(format!("not a valid eviction record: {e}")),
        },
        Err(e) => MarkerState::Unreadable(format!("could not read the marker: {e}")),
    }
}

/// The index artifacts the marker at `path` says the generation held when it
/// was evicted. `None` when the marker is legacy, unreadable or written by a
/// recovery tool: the expected set is then unknown.
pub fn expected_index_artifacts(path: &Path) -> Option<Vec<String>> {
    match read_marker(path) {
        MarkerState::Recorded(record) => record.index_artifacts,
        MarkerState::Legacy | MarkerState::Unreadable(_) => None,
    }
}

/// Counts of markers by provenance, for one summary line at restore.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Census {
    /// Markers with a readable evictor record.
    pub recorded: usize,
    /// Markers written by a recovery tool ([`Trigger::Recovered`]).
    pub recovered: usize,
    /// Empty legacy markers.
    pub legacy: usize,
    /// Present but unreadable markers.
    pub unreadable: usize,
}

impl Census {
    /// Tally one marker.
    pub fn add(&mut self, state: &MarkerState) {
        match state {
            MarkerState::Recorded(r) if r.trigger == Trigger::Recovered => self.recovered += 1,
            MarkerState::Recorded(_) => self.recorded += 1,
            MarkerState::Legacy => self.legacy += 1,
            MarkerState::Unreadable(_) => self.unreadable += 1,
        }
    }
}

/// Log one line summarising the provenance of every marker under
/// `sstables_dir`, at restore. Unreadable markers are a warning: the
/// generation is still restored, but someone should look at the file.
pub fn log_census(sstables_dir: &Path) {
    let mut census = Census::default();
    let mut first_unreadable: Option<(PathBuf, String)> = None;
    for table in read_dir_or_log(sstables_dir) {
        for entry in read_dir_or_log(&table.path()) {
            let name = entry.file_name();
            let Some(gen) = name.to_str().and_then(|n| n.strip_suffix(".evicted")) else {
                continue;
            };
            if gen.parse::<u64>().is_err() {
                continue;
            }
            let state = read_marker(&entry.path());
            if let MarkerState::Unreadable(why) = &state {
                first_unreadable.get_or_insert((entry.path(), why.clone()));
            }
            census.add(&state);
        }
    }
    tracing::info!(
        recorded = census.recorded,
        recovered_by_tool = census.recovered,
        legacy_reason_unknown = census.legacy,
        unreadable_reason_unknown = census.unreadable,
        "eviction markers found at restore"
    );
    if let Some((path, why)) = first_unreadable {
        tracing::warn!(
            first = %path.display(),
            error = %why,
            count = census.unreadable,
            "eviction markers that could not be read are still honoured (evicted, reason unknown)"
        );
    }
}

/// Entries of `dir`; an unreadable directory or entry is logged, not hidden.
fn read_dir_or_log(dir: &Path) -> Vec<std::fs::DirEntry> {
    let reader = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(dir = %dir.display(), error = %e, "eviction markers: cannot list directory");
            }
            return Vec::new();
        }
    };
    reader
        .filter_map(|entry| match entry {
            Ok(e) => Some(e),
            Err(e) => {
                tracing::warn!(dir = %dir.display(), error = %e, "eviction markers: unreadable directory entry");
                None
            }
        })
        .collect()
}

/// Test fixture: a complete evictor record.
#[cfg(test)]
pub(crate) fn test_record() -> EvictionRecord {
    EvictionRecord {
        version: RECORD_VERSION,
        trigger: Trigger::CacheCap,
        source: SOURCE_EVICTOR.to_string(),
        written_at_unix_ms: 1_700_000_000_000,
        generation_bytes: Some(80),
        total_bytes: Some(160),
        max_bytes: Some(100),
        min_bytes: Some(0),
        projected_available: Some(5),
        target_free: Some(10),
        index_artifacts: Some(vec!["idx_a.sidecar".to_string()]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_written_marker_reads_back_whole() {
        let dir = tempfile::tempdir().unwrap();
        let record = test_record();
        write_marker(dir.path(), "5", &record).unwrap();
        assert_eq!(
            read_marker(&marker_path(dir.path(), "5")),
            MarkerState::Recorded(record)
        );
        assert!(
            !dir.path().join(".5.evicted.tmp").exists(),
            "the temp file is renamed away"
        );
    }

    #[test]
    fn a_marker_from_a_newer_writer_with_extra_fields_still_reads() {
        let dir = tempfile::tempdir().unwrap();
        let mut value = serde_json::to_value(test_record()).unwrap();
        value["future_field"] = serde_json::json!("x");
        std::fs::write(marker_path(dir.path(), "5"), value.to_string()).unwrap();
        assert!(matches!(
            read_marker(&marker_path(dir.path(), "5")),
            MarkerState::Recorded(_)
        ));
    }

    #[test]
    fn an_empty_file_is_legacy_and_garbage_is_unreadable() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(marker_path(dir.path(), "1"), b"").unwrap();
        std::fs::write(marker_path(dir.path(), "2"), b"{\"version\":").unwrap();
        assert_eq!(
            read_marker(&marker_path(dir.path(), "1")),
            MarkerState::Legacy
        );
        assert!(matches!(
            read_marker(&marker_path(dir.path(), "2")),
            MarkerState::Unreadable(_)
        ));
        assert!(
            matches!(
                read_marker(&marker_path(dir.path(), "3")),
                MarkerState::Unreadable(m) if m.contains("could not read")
            ),
            "a missing file is reported, not mistaken for legacy"
        );
    }

    #[test]
    fn the_trigger_follows_which_limits_were_exceeded() {
        assert_eq!(Trigger::from_limits(true, false), Some(Trigger::CacheCap));
        assert_eq!(Trigger::from_limits(false, true), Some(Trigger::FreeSpace));
        assert_eq!(
            Trigger::from_limits(true, true),
            Some(Trigger::CacheCapAndFreeSpace)
        );
        assert_eq!(Trigger::from_limits(false, false), None);
    }

    #[test]
    fn only_a_real_evictor_record_has_a_known_reason() {
        assert!(MarkerState::Recorded(test_record()).is_reason_known());
        assert!(!MarkerState::Legacy.is_reason_known());
        assert!(!MarkerState::Unreadable("x".into()).is_reason_known());
        let recovered = EvictionRecord::bare(Trigger::Recovered, "ferrosa-ctl");
        assert!(!MarkerState::Recorded(recovered).is_reason_known());
    }
}
