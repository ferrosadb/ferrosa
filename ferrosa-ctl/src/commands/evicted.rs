//! `ferrosa-ctl sstable mark-evicted` — mark generations a pre-marker ferrosa
//! evicted, so the next start restores them from S3.
//!
//! This replaces `scripts/recover-evicted-sstables.py`. Mutating a node's data
//! directory belongs in the product, where it is tested, dry-run by default,
//! and refuses a live node; a script is reference material only.
//!
//! Evidence is the node's log: each
//! `s3-sync: evicted uploaded local SSTable from cache table="T" sstable="G"`
//! line names a generation the evictor removed. A generation whose files are
//! back on local disk, whose table directory is gone (dropped table), or that
//! already carries a marker is left alone. Restore itself checks the S3
//! manifest, so a marker for a generation compaction has since retired cannot
//! resurrect it.
//!
//! The marker's `source` is this command line and its trigger is
//! `recovered`, so a marker written here is never mistaken for a real
//! eviction decision.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use ferrosa_storage::eviction_marker::{self, EvictionRecord, Trigger};

type CtlError = Box<dyn std::error::Error + Send + Sync>;

/// `source` recorded in every marker this command writes.
pub const SOURCE: &str = "ferrosa-ctl sstable mark-evicted";

const NEEDLE: &[u8] = b"evicted uploaded local SSTable from cache";

/// Hard cap on log lines scanned, so a runaway file cannot hang the command.
const MAX_LOG_LINES: u64 = 50_000_000;

/// What the command would do (or did), per table.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Plan {
    /// Generations to mark, by table directory name.
    pub to_mark: BTreeMap<String, BTreeSet<String>>,
    /// Generations whose files are on local disk.
    pub already_local: usize,
    /// Generations that already carry a marker (left untouched).
    pub already_marked: usize,
    /// Generations whose table directory no longer exists.
    pub no_table_dir: usize,
    /// Distinct evictions found in the log.
    pub evictions_in_log: usize,
}

impl Plan {
    /// Total generations to mark.
    pub fn total(&self) -> usize {
        self.to_mark.values().map(BTreeSet::len).sum()
    }
}

/// Outcome of the liveness probe.
#[derive(Debug, PartialEq, Eq)]
enum Liveness {
    /// A node holds the lock on this file.
    Live(PathBuf),
    /// Every lock found was free.
    Stopped,
    /// No lock file exists to probe, so a stopped node cannot be proven.
    Unprovable,
}

/// Probe the sled lock (`<data_dir>/raft/**/db`, flock'd exclusively by a
/// running node) without creating or changing anything.
fn probe_liveness(data_dir: &Path) -> Result<Liveness, CtlError> {
    use fs2::FileExt;
    let raft = data_dir.join("raft");
    let mut candidates = vec![raft.join("db")];
    match std::fs::read_dir(&raft) {
        Ok(entries) => {
            for entry in entries {
                candidates.push(entry?.path().join("db"));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("cannot list {}: {e}", raft.display()).into()),
    }
    let mut probed = false;
    for path in candidates {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("cannot open {}: {e}", path.display()).into()),
        };
        probed = true;
        match file.try_lock_exclusive() {
            Ok(()) => file.unlock()?,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(Liveness::Live(path));
            }
            Err(e) => return Err(format!("cannot probe lock {}: {e}", path.display()).into()),
        }
    }
    Ok(if probed {
        Liveness::Stopped
    } else {
        Liveness::Unprovable
    })
}

/// Remove ANSI colour escapes (`ESC [ ... m`) from a log line.
fn strip_ansi(line: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(line.len());
    let mut i = 0;
    while i < line.len() {
        if line[i] == 0x1b && line.get(i + 1) == Some(&b'[') {
            let mut j = i + 2;
            while j < line.len() && (line[j].is_ascii_digit() || line[j] == b';') {
                j += 1;
            }
            if line.get(j) == Some(&b'm') {
                i = j + 1;
                continue;
            }
        }
        out.push(line[i]);
        i += 1;
    }
    out
}

/// `(table, generation)` from one log line, if it records an eviction.
fn parse_eviction(line: &[u8]) -> Option<(String, String)> {
    let line = strip_ansi(line);
    let at = line.windows(NEEDLE.len()).position(|w| w == NEEDLE)?;
    let rest = std::str::from_utf8(&line[at + NEEDLE.len()..]).ok()?;
    let rest = rest.trim_start().strip_prefix("table=\"")?;
    let (table, rest) = rest.split_once('"')?;
    let rest = rest.trim_start().strip_prefix("sstable=\"")?;
    let (gen, _) = rest.split_once('"')?;
    let safe_table =
        !table.is_empty() && !table.contains(['/', '\\']) && table != "." && table != "..";
    (safe_table && !gen.is_empty() && gen.bytes().all(|b| b.is_ascii_digit()))
        .then(|| (table.to_string(), gen.to_string()))
}

/// Every distinct eviction the log records. `tail_mb == 0` scans all of it.
fn evictions_in_log(log: &Path, tail_mb: u64) -> Result<BTreeSet<(String, String)>, CtlError> {
    let mut file =
        File::open(log).map_err(|e| format!("cannot open log {}: {e}", log.display()))?;
    let mut skip_partial = false;
    if tail_mb > 0 {
        let len = file.metadata()?.len();
        let start = len.saturating_sub(tail_mb.saturating_mul(1024 * 1024));
        file.seek(SeekFrom::Start(start))?;
        skip_partial = start > 0;
    }
    let mut reader = BufReader::new(file);
    let mut found = BTreeSet::new();
    let mut buf = Vec::new();
    for _ in 0..MAX_LOG_LINES {
        buf.clear();
        if reader.read_until(b'\n', &mut buf)? == 0 {
            return Ok(found);
        }
        if skip_partial {
            skip_partial = false;
            continue;
        }
        if let Some(hit) = parse_eviction(&buf) {
            found.insert(hit);
        }
    }
    Err(format!(
        "log {} exceeds {MAX_LOG_LINES} lines; use --tail-mb",
        log.display()
    )
    .into())
}

fn is_local(table_dir: &Path, gen: &str) -> bool {
    table_dir.join(format!("{gen}-Data.db")).exists()
        || table_dir.join(gen).join(format!("{gen}-Data.db")).exists()
}

/// Decide, from the log and the data directory, what to mark.
pub fn build_plan(data_dir: &Path, log: &Path, tail_mb: u64) -> Result<Plan, CtlError> {
    let sstables = data_dir.join("sstables");
    if !sstables.is_dir() {
        return Err(format!("no sstables directory at {}", sstables.display()).into());
    }
    let evictions = evictions_in_log(log, tail_mb)?;
    let mut plan = Plan {
        evictions_in_log: evictions.len(),
        ..Plan::default()
    };
    for (table, gen) in evictions {
        let table_dir = sstables.join(&table);
        if !table_dir.is_dir() {
            plan.no_table_dir += 1;
        } else if is_local(&table_dir, &gen) {
            plan.already_local += 1;
        } else if eviction_marker::marker_path(&table_dir, &gen).exists() {
            plan.already_marked += 1;
        } else {
            plan.to_mark.entry(table).or_default().insert(gen);
        }
    }
    Ok(plan)
}

/// `ferrosa-ctl sstable mark-evicted`. Dry run unless `apply`. Refuses a data
/// directory whose node holds its lock; with `apply`, also refuses one it
/// cannot prove stopped unless `assume_stopped`.
pub fn sstable_mark_evicted(
    data_dir: &Path,
    log: &Path,
    tail_mb: u64,
    apply: bool,
    assume_stopped: bool,
) -> Result<(), CtlError> {
    match probe_liveness(data_dir)? {
        Liveness::Live(lock) => {
            return Err(format!(
                "refusing: {} is locked, so a node is running on {}. Stop it first.",
                lock.display(),
                data_dir.display()
            )
            .into());
        }
        Liveness::Unprovable if apply && !assume_stopped => {
            return Err(format!(
                "refusing --apply: no lock file under {}/raft to prove the node is stopped. \
                 Confirm it is stopped and pass --assume-stopped.",
                data_dir.display()
            )
            .into());
        }
        Liveness::Unprovable => eprintln!(
            "note: no lock file under {}/raft; cannot prove the node is stopped",
            data_dir.display()
        ),
        Liveness::Stopped => {}
    }

    let plan = build_plan(data_dir, log, tail_mb)?;
    let verb = if apply { "marked" } else { "would mark" };
    let sstables = data_dir.join("sstables");
    for (table, gens) in &plan.to_mark {
        if apply {
            for gen in gens {
                let record = EvictionRecord::bare(Trigger::Recovered, SOURCE);
                eviction_marker::write_marker(&sstables.join(table), gen, &record)
                    .map_err(|e| format!("cannot write the marker for {table}/{gen}: {e}"))?;
            }
        }
        println!("{verb} {:4}  {table}", gens.len());
    }
    println!(
        "{}{verb} {} generation(s); {} already local; {} already marked; \
         {} with no table directory; {} evictions in the log",
        if apply { "" } else { "DRY RUN: " },
        plan.total(),
        plan.already_local,
        plan.already_marked,
        plan.no_table_dir,
        plan.evictions_in_log
    );
    if plan.evictions_in_log == 0 {
        return Err("no evictions found in the log: nothing to recover (wrong log?)".into());
    }
    if !apply && plan.total() > 0 {
        println!("Re-run with --apply (node must be stopped).");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_storage::eviction_marker::{read_marker, MarkerState};
    use fs2::FileExt;

    const T: &str = "agent_memory.entity_store";

    fn line(table: &str, gen: &str) -> String {
        format!(
            "\x1b[2m2026-09-29\x1b[0m \x1b[33m INFO\x1b[0m s3-sync: evicted uploaded local \
             SSTable from cache table=\"{table}\" sstable=\"{gen}\" size_bytes=5\n"
        )
    }

    /// A data dir with table `T` holding generation 3 locally, a log naming
    /// generations 1, 2 (twice), 3, plus one for a dropped table.
    fn fixture() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let table_dir = dir.path().join("sstables").join(T);
        std::fs::create_dir_all(&table_dir).unwrap();
        std::fs::write(table_dir.join("3-Data.db"), b"x").unwrap();
        let log = dir.path().join("node.log");
        let text = format!(
            "{}{}{}{}{}unrelated line\n",
            line(T, "1"),
            line(T, "2"),
            line(T, "2"),
            line(T, "3"),
            line("gone.table", "9")
        );
        std::fs::write(&log, text).unwrap();
        (dir, log)
    }

    fn marker(dir: &Path, gen: &str) -> PathBuf {
        eviction_marker::marker_path(&dir.join("sstables").join(T), gen)
    }

    #[test]
    fn the_plan_lists_only_generations_that_need_a_marker() {
        let (dir, log) = fixture();
        let plan = build_plan(dir.path(), &log, 0).unwrap();
        assert_eq!(
            plan.to_mark,
            BTreeMap::from([(
                T.to_string(),
                BTreeSet::from(["1".to_string(), "2".to_string()])
            )])
        );
        assert_eq!(plan.already_local, 1);
        assert_eq!(plan.no_table_dir, 1);
        assert_eq!(
            plan.evictions_in_log, 4,
            "the repeated line is one eviction"
        );
    }

    #[test]
    fn a_hostile_table_name_in_the_log_is_ignored() {
        assert_eq!(parse_eviction(line("../etc", "1").as_bytes()), None);
        assert_eq!(parse_eviction(line("a/b", "1").as_bytes()), None);
        assert_eq!(parse_eviction(line(T, "1x").as_bytes()), None);
        assert!(parse_eviction(line(T, "1").as_bytes()).is_some());
    }

    #[test]
    fn a_dry_run_writes_nothing() {
        let (dir, log) = fixture();
        sstable_mark_evicted(dir.path(), &log, 0, false, false).unwrap();
        assert!(!marker(dir.path(), "1").exists());
        assert!(!marker(dir.path(), "2").exists());
    }

    #[test]
    fn apply_writes_markers_naming_this_command_as_the_source() {
        let (dir, log) = fixture();
        sstable_mark_evicted(dir.path(), &log, 0, true, true).unwrap();
        for gen in ["1", "2"] {
            let MarkerState::Recorded(record) = read_marker(&marker(dir.path(), gen)) else {
                panic!("generation {gen} must carry a readable record");
            };
            assert_eq!(record.source, "ferrosa-ctl sstable mark-evicted");
            assert_eq!(record.trigger, Trigger::Recovered);
        }
        assert!(
            !marker(dir.path(), "3").exists(),
            "a local generation is not marked"
        );
    }

    #[test]
    fn apply_never_overwrites_a_real_evictor_marker() {
        let (dir, log) = fixture();
        let real = EvictionRecord::bare(Trigger::FreeSpace, eviction_marker::SOURCE_EVICTOR);
        eviction_marker::write_marker(&dir.path().join("sstables").join(T), "1", &real).unwrap();
        sstable_mark_evicted(dir.path(), &log, 0, true, true).unwrap();
        assert_eq!(
            read_marker(&marker(dir.path(), "1")),
            MarkerState::Recorded(real)
        );
    }

    #[test]
    fn a_live_data_dir_is_refused_even_for_a_dry_run() {
        let (dir, log) = fixture();
        let raft = dir.path().join("raft").join("dc1");
        std::fs::create_dir_all(&raft).unwrap();
        let held = File::create(raft.join("db")).unwrap();
        held.lock_exclusive().unwrap();

        for apply in [false, true] {
            let err = sstable_mark_evicted(dir.path(), &log, 0, apply, true).unwrap_err();
            assert!(err.to_string().contains("a node is running"), "{err}");
        }
        assert!(!marker(dir.path(), "1").exists(), "nothing written");

        held.unlock().unwrap();
        sstable_mark_evicted(dir.path(), &log, 0, true, false).unwrap();
        assert!(marker(dir.path(), "1").exists(), "stopped node: proceeds");
    }

    #[test]
    fn apply_without_a_lock_to_probe_needs_assume_stopped() {
        let (dir, log) = fixture();
        let err = sstable_mark_evicted(dir.path(), &log, 0, true, false).unwrap_err();
        assert!(err.to_string().contains("--assume-stopped"), "{err}");
        assert!(!marker(dir.path(), "1").exists());
        sstable_mark_evicted(dir.path(), &log, 0, false, false).unwrap();
    }

    #[test]
    fn a_log_with_no_evictions_is_an_error_not_a_success() {
        let (dir, _) = fixture();
        let empty = dir.path().join("empty.log");
        std::fs::write(&empty, b"nothing here\n").unwrap();
        let err = sstable_mark_evicted(dir.path(), &empty, 0, false, false).unwrap_err();
        assert!(err.to_string().contains("no evictions"), "{err}");
    }
}
