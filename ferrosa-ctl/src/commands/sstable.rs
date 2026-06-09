//! Offline SSTable analysis and recovery — `ferrosa-ctl sstable ...`.
//!
//! Unlike the rest of `ferrosa-ctl`, these subcommands take **no network
//! connection**. They operate directly on an on-disk table directory (the
//! `<data_dir>/sstables/<keyspace>.<table>/` layout) and its `quarantine/`
//! subdirectory, the same way the engine does at startup.
//!
//! The corruption verdict is delegated to the storage engine's own startup
//! smoke test, [`StorageEngine::smoke_test_generation`] — the exact function
//! the self-heal detector runs to decide what to quarantine. A ctl verdict can
//! therefore never diverge from what a node decides at boot. Component-level
//! detail (partition counts, Data.db extent, timestamps) comes from the BTI
//! [`SSTableReader`].
//!
//! All commands here are **read-only** except the recovery action, which only
//! ever moves files (never deletes) and defaults to a dry run.

use std::path::{Path, PathBuf};

use serde::Serialize;
use tabled::builder::Builder;
use tabled::settings::Style;

use ferrosa_sstable::io::FileReadAt;
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
use ferrosa_storage::engine::StorageEngine;

/// Error type shared with `main`'s unified result.
type CtlError = Box<dyn std::error::Error + Send + Sync>;

/// The component files that make up one SSTable generation on disk.
const COMPONENTS: [&str; 7] = [
    "Data.db",
    "Partitions.db",
    "Rows.db",
    "Filter.db",
    "Statistics.db",
    "CompressionInfo.db",
    "TOC.txt",
];

/// Classification of one generation within a table directory.
#[derive(Debug, Clone, Serialize)]
pub struct GenReport {
    pub gen: u64,
    /// `live` (the table dir) or `quarantine` (the `quarantine/` subdir).
    pub location: String,
    /// `OK` or `CORRUPT`, per the engine's startup smoke test.
    pub verdict: String,
    /// Exact engine failure message when `CORRUPT`; `None` when `OK`.
    pub reason: Option<String>,
    /// Partitions the index footer claims (Partitions.db), if the SSTable opens.
    pub index_partitions: Option<u64>,
    /// Partitions reachable by walking Data.db, if the SSTable opens. For a
    /// corrupt generation `reachable < index` means truncation; `reachable ==
    /// index` with a CORRUPT verdict means intra-partition parse drift.
    pub reachable_partitions: Option<u64>,
    /// Data.db length in bytes, if present.
    pub data_len: Option<u64>,
}

/// Deep single-generation report for `inspect`.
#[derive(Debug, Clone, Serialize)]
pub struct GenInspect {
    pub gen: u64,
    pub location: String,
    pub verdict: String,
    pub reason: Option<String>,
    pub index_partitions: Option<u64>,
    pub reachable_partitions: Option<u64>,
    pub data_extent_ok: Option<bool>,
    pub data_extent_error: Option<String>,
    pub min_timestamp: Option<i64>,
    pub max_timestamp: Option<i64>,
    pub components: Vec<ComponentInfo>,
    /// Operator-facing best guess at the corruption class, derived from the
    /// verdict + partition counts. `None` when the generation is healthy.
    pub likely_corruption_class: Option<String>,
}

/// One component file's presence/size.
#[derive(Debug, Clone, Serialize)]
pub struct ComponentInfo {
    pub name: String,
    pub present: bool,
    pub len: u64,
    /// True for a present-but-empty file. Only meaningful for Data.db /
    /// Partitions.db (a zero-byte Rows.db is the expected output for simple
    /// partitions and is NOT corruption).
    pub zero_byte: bool,
}

// ── scan ──────────────────────────────────────────────────────────────────────

/// Classify every generation in `table_dir` (and, when requested, its
/// `quarantine/` subdir). Pure read — never moves or mutates a file.
pub fn scan_report(table_dir: &Path, include_quarantine: bool) -> Result<Vec<GenReport>, CtlError> {
    if !table_dir.is_dir() {
        return Err(format!("not a directory: {}", table_dir.display()).into());
    }
    let mut out = Vec::new();
    classify_dir(table_dir, "live", &mut out);
    if include_quarantine {
        let q = table_dir.join("quarantine");
        if q.is_dir() {
            classify_dir(&q, "quarantine", &mut out);
        }
    }
    Ok(out)
}

/// Append a `GenReport` for every generation found directly in `dir`.
fn classify_dir(dir: &Path, location: &str, out: &mut Vec<GenReport>) {
    for gen in StorageEngine::list_generations_in_dir(dir) {
        let (verdict, reason) = match StorageEngine::smoke_test_generation(dir, gen) {
            Ok(()) => ("OK".to_string(), None),
            Err(e) => ("CORRUPT".to_string(), Some(e.to_string())),
        };
        let (index_partitions, reachable_partitions) = match open_reader(dir, gen) {
            Ok(reader) => (
                Some(reader.key_count()),
                Some(reader.walkable_partition_count()),
            ),
            Err(_) => (None, None),
        };
        out.push(GenReport {
            gen,
            location: location.to_string(),
            verdict,
            reason,
            index_partitions,
            reachable_partitions,
            data_len: component_len(dir, gen, "Data.db"),
        });
    }
}

/// `ferrosa-ctl sstable scan` entry point: classify and print.
pub fn sstable_scan(
    table_dir: &Path,
    include_quarantine: bool,
    json: bool,
) -> Result<(), CtlError> {
    let reports = scan_report(table_dir, include_quarantine)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&reports)?);
        return Ok(());
    }
    print_scan_table(&reports);
    print_scan_summary(&reports, table_dir);
    Ok(())
}

/// Render the per-generation scan results as a table.
fn print_scan_table(reports: &[GenReport]) {
    let mut builder = Builder::default();
    builder.push_record([
        "gen",
        "location",
        "verdict",
        "index_parts",
        "reachable",
        "data_len",
        "reason",
    ]);
    for r in reports {
        builder.push_record([
            r.gen.to_string(),
            r.location.clone(),
            r.verdict.clone(),
            opt_u64(r.index_partitions),
            opt_u64(r.reachable_partitions),
            opt_u64(r.data_len),
            r.reason.clone().unwrap_or_default(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::sharp());
    println!("{table}");
}

/// Print a one-line summary of OK/CORRUPT counts by location.
fn print_scan_summary(reports: &[GenReport], table_dir: &Path) {
    let total = reports.len();
    let corrupt = reports.iter().filter(|r| r.verdict == "CORRUPT").count();
    let quarantined = reports
        .iter()
        .filter(|r| r.location == "quarantine")
        .count();
    println!(
        "{}: {total} generation(s), {} OK, {corrupt} CORRUPT, {quarantined} in quarantine/",
        table_dir.display(),
        total - corrupt
    );
}

// ── inspect ───────────────────────────────────────────────────────────────────

/// Build a deep report for one generation, searching the table dir then its
/// `quarantine/` subdir. Pure read.
pub fn inspect_report(table_dir: &Path, gen: u64) -> Result<GenInspect, CtlError> {
    let (dir, location) = locate_gen(table_dir, gen).ok_or_else(|| {
        format!(
            "generation {gen} not found in {} or its quarantine/ subdir",
            table_dir.display()
        )
    })?;

    let (verdict, reason) = match StorageEngine::smoke_test_generation(&dir, gen) {
        Ok(()) => ("OK".to_string(), None),
        Err(e) => ("CORRUPT".to_string(), Some(e.to_string())),
    };

    let mut index_partitions = None;
    let mut reachable_partitions = None;
    let mut data_extent_ok = None;
    let mut data_extent_error = None;
    let mut min_timestamp = None;
    let mut max_timestamp = None;
    if let Ok(reader) = open_reader(&dir, gen) {
        index_partitions = Some(reader.key_count());
        reachable_partitions = Some(reader.walkable_partition_count());
        match reader.validate_data_extent() {
            Ok(()) => data_extent_ok = Some(true),
            Err(e) => {
                data_extent_ok = Some(false);
                data_extent_error = Some(e.to_string());
            }
        }
        min_timestamp = Some(reader.header().min_timestamp);
        max_timestamp = Some(reader.header().max_timestamp);
    }

    let components = COMPONENTS
        .iter()
        .map(|c| component_info(&dir, gen, c))
        .collect();
    let likely_corruption_class = classify_corruption(
        &verdict,
        index_partitions,
        reachable_partitions,
        data_extent_ok,
    );

    Ok(GenInspect {
        gen,
        location: location.to_string(),
        verdict,
        reason,
        index_partitions,
        reachable_partitions,
        data_extent_ok,
        data_extent_error,
        min_timestamp,
        max_timestamp,
        components,
        likely_corruption_class,
    })
}

/// `ferrosa-ctl sstable inspect` entry point.
pub fn sstable_inspect(table_dir: &Path, gen: u64, json: bool) -> Result<(), CtlError> {
    let report = inspect_report(table_dir, gen)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    print_inspect(&report);
    Ok(())
}

/// Heuristic mapping of (verdict, partition counts) → a human corruption class.
fn classify_corruption(
    verdict: &str,
    index: Option<u64>,
    reachable: Option<u64>,
    extent_ok: Option<bool>,
) -> Option<String> {
    if verdict == "OK" {
        return None;
    }
    if extent_ok == Some(false) {
        return Some("truncation: Data.db shorter than its index claims".to_string());
    }
    match (index, reachable) {
        (Some(i), Some(r)) if r < i => {
            Some("truncation: fewer partitions reachable than indexed".to_string())
        }
        (Some(i), Some(r)) if r == i => {
            Some("intra-partition parse drift (writer bitmap under-count of cells)".to_string())
        }
        _ => Some("unreadable: SSTable failed to open".to_string()),
    }
}

fn print_inspect(r: &GenInspect) {
    println!("generation {} ({})", r.gen, r.location);
    println!("  verdict:            {}", r.verdict);
    if let Some(reason) = &r.reason {
        println!("  reason:             {reason}");
    }
    if let Some(class) = &r.likely_corruption_class {
        println!("  likely corruption:  {class}");
    }
    println!("  index partitions:   {}", opt_u64(r.index_partitions));
    println!("  reachable parts:    {}", opt_u64(r.reachable_partitions));
    match (r.data_extent_ok, &r.data_extent_error) {
        (Some(true), _) => println!("  data extent:        ok"),
        (Some(false), Some(e)) => println!("  data extent:        FAIL — {e}"),
        _ => println!("  data extent:        unknown (did not open)"),
    }
    println!(
        "  timestamps:         min={} max={}",
        opt_i64(r.min_timestamp),
        opt_i64(r.max_timestamp)
    );
    println!("  components:");
    for c in &r.components {
        let flag = if c.present && c.zero_byte && (c.name == "Data.db" || c.name == "Partitions.db")
        {
            "  <-- ZERO-BYTE CRITICAL COMPONENT"
        } else {
            ""
        };
        let state = if c.present {
            format!("{} bytes", c.len)
        } else {
            "absent".to_string()
        };
        println!("    {:<20} {}{}", c.name, state, flag);
    }
}

// ── quarantine (stop-the-bleeding) ──────────────────────────────────────────────

/// Planned disposition of one live generation under `quarantine`.
#[derive(Debug, Clone, Serialize)]
pub struct QuarantineAction {
    pub gen: u64,
    /// `move` (corrupt, will be moved aside) or `already-quarantined` (a
    /// same-numbered copy already sits in `quarantine/`, so the live copy is
    /// left in place rather than overwriting it). Healthy generations are never
    /// included.
    pub action: String,
    pub reason: Option<String>,
}

/// Plan the quarantine of every CORRUPT generation in `table_dir`'s live set.
/// Healthy generations are never touched. Pure read — moves nothing.
///
/// This exists because the engine's self-heal controller runs with a
/// single-node cluster view (a documented deferred limitation), so it never
/// quarantines corrupt SSTables on a multi-node cluster — they stay live, get
/// re-pulled from S3, and are re-detected every scan cycle. This command lets an
/// operator move them aside while the node is stopped.
pub fn quarantine_plan(table_dir: &Path) -> Result<Vec<QuarantineAction>, CtlError> {
    if !table_dir.is_dir() {
        return Err(format!("not a directory: {}", table_dir.display()).into());
    }
    let q = table_dir.join("quarantine");
    let mut plan = Vec::new();
    for gen in StorageEngine::list_generations_in_dir(table_dir) {
        // Healthy generations are left live — never quarantined.
        if let Err(e) = StorageEngine::smoke_test_generation(table_dir, gen) {
            let action = if gen_present(&q, gen) {
                "already-quarantined"
            } else {
                "move"
            };
            plan.push(QuarantineAction {
                gen,
                action: action.to_string(),
                reason: Some(e.to_string()),
            });
        }
    }
    Ok(plan)
}

/// `ferrosa-ctl sstable quarantine` entry point. **Dry-run by default**: prints
/// the plan and mutates nothing. With `apply`, moves each `move` generation into
/// `quarantine/` via the engine's mover (rename — never deletes). Generations
/// already present in `quarantine/` (S3 re-download duplicates) are left in
/// place, never overwritten. The node MUST be stopped first.
pub fn sstable_quarantine(table_dir: &Path, apply: bool, json: bool) -> Result<(), CtlError> {
    let plan = quarantine_plan(table_dir)?;

    if !apply {
        if json {
            println!("{}", serde_json::to_string_pretty(&plan)?);
        } else {
            print_quarantine_plan(&plan, table_dir);
        }
        return Ok(());
    }

    let mut moved = 0usize;
    let mut failed = 0usize;
    for a in plan.iter().filter(|a| a.action == "move") {
        match StorageEngine::quarantine_corrupt_generation(table_dir, a.gen) {
            Ok(_) => moved += 1,
            Err(e) => {
                eprintln!("warning: failed to quarantine gen {}: {e}", a.gen);
                failed += 1;
            }
        }
    }
    let dups = plan
        .iter()
        .filter(|a| a.action == "already-quarantined")
        .count();
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "moved": moved,
                "failed": failed,
                "left_in_place_already_quarantined": dups,
            }))?
        );
    } else {
        println!(
            "{}: quarantined {moved} corrupt generation(s){}{}",
            table_dir.display(),
            if failed > 0 {
                format!(", {failed} failed")
            } else {
                String::new()
            },
            if dups > 0 {
                format!(", {dups} left in place (already in quarantine/)")
            } else {
                String::new()
            },
        );
    }
    Ok(())
}

fn print_quarantine_plan(plan: &[QuarantineAction], table_dir: &Path) {
    let movable = plan.iter().filter(|a| a.action == "move").count();
    let dups = plan
        .iter()
        .filter(|a| a.action == "already-quarantined")
        .count();
    if plan.is_empty() {
        println!(
            "{}: no corrupt live generations — nothing to quarantine",
            table_dir.display()
        );
        return;
    }
    let mut builder = Builder::default();
    builder.push_record(["gen", "action", "reason"]);
    for a in plan {
        builder.push_record([
            a.gen.to_string(),
            a.action.clone(),
            a.reason.clone().unwrap_or_default(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::sharp());
    println!("{table}");
    println!(
        "{}: DRY RUN — would move {movable} corrupt generation(s) to quarantine/ ({dups} already there). \
         Re-run with --apply (node must be stopped).",
        table_dir.display()
    );
}

// ── salvage (measure recoverable rows) ──────────────────────────────────────────

/// Per-generation salvage measurement: how many partitions/rows can be
/// recovered from a (possibly corrupt) generation.
#[derive(Debug, Clone, Serialize)]
pub struct GenSalvage {
    pub gen: u64,
    pub location: String,
    pub verdict: String,
    pub partitions_total: u64,
    pub partitions_complete: u64,
    pub partitions_partial: u64,
    pub partitions_failed: u64,
    pub rows_recovered: u64,
    pub static_rows_recovered: u64,
}

/// Measure recoverable rows across every generation in `table_dir` (and, when
/// requested, its `quarantine/` subdir) using the resilient salvage reader.
/// Pure read — recovers nothing to disk; only counts.
pub fn salvage_report(
    table_dir: &Path,
    include_quarantine: bool,
) -> Result<Vec<GenSalvage>, CtlError> {
    if !table_dir.is_dir() {
        return Err(format!("not a directory: {}", table_dir.display()).into());
    }
    let mut out = Vec::new();
    measure_salvage_dir(table_dir, "live", &mut out);
    if include_quarantine {
        let q = table_dir.join("quarantine");
        if q.is_dir() {
            measure_salvage_dir(&q, "quarantine", &mut out);
        }
    }
    Ok(out)
}

fn measure_salvage_dir(dir: &Path, location: &str, out: &mut Vec<GenSalvage>) {
    for gen in StorageEngine::list_generations_in_dir(dir) {
        let verdict = match StorageEngine::smoke_test_generation(dir, gen) {
            Ok(()) => "OK",
            Err(_) => "CORRUPT",
        };
        let mut entry = GenSalvage {
            gen,
            location: location.to_string(),
            verdict: verdict.to_string(),
            partitions_total: 0,
            partitions_complete: 0,
            partitions_partial: 0,
            partitions_failed: 0,
            rows_recovered: 0,
            static_rows_recovered: 0,
        };
        // salvage with a no-op sink — bounded memory, we only want the stats.
        if let Ok(reader) = open_reader(dir, gen) {
            if let Ok(stats) = reader.salvage(|_p| {}) {
                entry.partitions_total = stats.partitions_total;
                entry.partitions_complete = stats.partitions_complete;
                entry.partitions_partial = stats.partitions_partial;
                entry.partitions_failed = stats.partitions_failed;
                entry.rows_recovered = stats.rows_recovered;
                entry.static_rows_recovered = stats.static_rows_recovered;
            }
        }
        out.push(entry);
    }
}

/// `ferrosa-ctl sstable salvage` entry point (measurement mode). Reports how
/// many rows are recoverable per generation and in aggregate, separating
/// already-healthy (OK) generations from corrupt ones.
pub fn sstable_salvage(
    table_dir: &Path,
    include_quarantine: bool,
    json: bool,
) -> Result<(), CtlError> {
    let reports = salvage_report(table_dir, include_quarantine)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&reports)?);
        return Ok(());
    }
    print_salvage_table(&reports);
    print_salvage_summary(&reports, table_dir);
    Ok(())
}

fn print_salvage_table(reports: &[GenSalvage]) {
    let mut builder = Builder::default();
    builder.push_record([
        "gen", "location", "verdict", "parts", "complete", "partial", "failed", "rows",
    ]);
    for r in reports {
        builder.push_record([
            r.gen.to_string(),
            r.location.clone(),
            r.verdict.clone(),
            r.partitions_total.to_string(),
            r.partitions_complete.to_string(),
            r.partitions_partial.to_string(),
            r.partitions_failed.to_string(),
            r.rows_recovered.to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::sharp());
    println!("{table}");
}

fn print_salvage_summary(reports: &[GenSalvage], table_dir: &Path) {
    let corrupt: Vec<&GenSalvage> = reports.iter().filter(|r| r.verdict == "CORRUPT").collect();
    let corrupt_rows: u64 = corrupt.iter().map(|r| r.rows_recovered).sum();
    let ok_rows: u64 = reports
        .iter()
        .filter(|r| r.verdict == "OK")
        .map(|r| r.rows_recovered)
        .sum();
    let corrupt_partial: u64 = corrupt.iter().map(|r| r.partitions_partial).sum();
    println!(
        "{}: salvage yield — {corrupt_rows} row(s) recoverable from {} corrupt generation(s) \
         ({corrupt_partial} partition(s) partially recovered); {ok_rows} row(s) in healthy generations.",
        table_dir.display(),
        corrupt.len()
    );
}

// ── shared helpers ──────────────────────────────────────────────────────────────

/// Locate a generation: the table dir itself (flat or nested), else its
/// `quarantine/` subdir. Returns the directory to hand to the engine and a
/// location label.
fn locate_gen(table_dir: &Path, gen: u64) -> Option<(PathBuf, &'static str)> {
    if gen_present(table_dir, gen) {
        return Some((table_dir.to_path_buf(), "live"));
    }
    let q = table_dir.join("quarantine");
    if gen_present(&q, gen) {
        return Some((q, "quarantine"));
    }
    None
}

/// True if `dir` holds `gen`'s Data.db, flat or in a `<gen>/` subdir.
fn gen_present(dir: &Path, gen: u64) -> bool {
    dir.join(format!("{gen}-Data.db")).exists()
        || dir
            .join(gen.to_string())
            .join(format!("{gen}-Data.db"))
            .exists()
}

/// Resolve the directory that physically holds `gen`'s component files.
fn resolve_gen_dir(dir: &Path, gen: u64) -> Option<PathBuf> {
    if dir.join(format!("{gen}-Data.db")).exists() {
        return Some(dir.to_path_buf());
    }
    let nested = dir.join(gen.to_string());
    if nested.join(format!("{gen}-Data.db")).exists() {
        return Some(nested);
    }
    None
}

/// Open a BTI reader for `gen` in `dir`, mirroring `ferrosa-sstable-dump`.
fn open_reader(dir: &Path, gen: u64) -> Result<SSTableReader<FileReadAt>, CtlError> {
    let gdir = resolve_gen_dir(dir, gen)
        .ok_or_else(|| format!("generation {gen}: no Data.db under {}", dir.display()))?;
    let comp = |name: &str| gdir.join(format!("{gen}-{name}"));
    let data = FileReadAt::open(comp("Data.db"))?;
    let partitions = FileReadAt::open(comp("Partitions.db"))?;
    let rows = FileReadAt::open(comp("Rows.db"))?;
    let filter = std::fs::read(comp("Filter.db")).unwrap_or_default();
    let statistics = std::fs::read(comp("Statistics.db")).unwrap_or_default();
    let compression_info = std::fs::read(comp("CompressionInfo.db")).ok();
    let reader = SSTableReader::open(SSTableComponents {
        data,
        partitions,
        rows,
        filter,
        compression_info,
        statistics,
    })?;
    Ok(reader)
}

/// Length of one component file, or `None` if absent.
fn component_len(dir: &Path, gen: u64, name: &str) -> Option<u64> {
    let gdir = resolve_gen_dir(dir, gen)?;
    std::fs::metadata(gdir.join(format!("{gen}-{name}")))
        .ok()
        .map(|m| m.len())
}

/// Presence/size of one component file.
fn component_info(dir: &Path, gen: u64, name: &str) -> ComponentInfo {
    let len = component_len(dir, gen, name);
    ComponentInfo {
        name: name.to_string(),
        present: len.is_some(),
        len: len.unwrap_or(0),
        zero_byte: len == Some(0),
    }
}

fn opt_u64(v: Option<u64>) -> String {
    v.map(|n| n.to_string()).unwrap_or_else(|| "-".to_string())
}

fn opt_i64(v: Option<i64>) -> String {
    v.map(|n| n.to_string()).unwrap_or_else(|| "-".to_string())
}

// ── Tests ───────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_storage::self_heal::test_fixtures::{
        corrupt_one_generation, table_dir_with_n_generations,
    };

    #[test]
    fn scan_reports_ok_for_healthy_generations() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let reports = scan_report(&table_dir, false).unwrap();
        assert_eq!(reports.len(), 2, "two healthy generations");
        for r in &reports {
            assert_eq!(r.verdict, "OK", "healthy gen must be OK: {r:?}");
            assert_eq!(r.location, "live");
            assert_eq!(
                r.index_partitions, r.reachable_partitions,
                "healthy gen: all indexed partitions are reachable"
            );
        }
    }

    #[test]
    fn scan_flags_truncated_generation_as_corrupt() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let bad = corrupt_one_generation(&table_dir);
        let reports = scan_report(&table_dir, false).unwrap();
        let bad_report = reports
            .iter()
            .find(|r| r.gen == bad)
            .expect("bad gen present");
        assert_eq!(bad_report.verdict, "CORRUPT");
        assert!(
            bad_report.reason.is_some(),
            "a corrupt gen must carry a reason"
        );
        // The healthy sibling is untouched.
        assert!(
            reports.iter().any(|r| r.gen != bad && r.verdict == "OK"),
            "the other generation stays OK"
        );
    }

    /// Regression canary: a present-but-empty Rows.db is the EXPECTED writer
    /// output for simple partitions and must NOT be treated as corruption.
    /// If this ever flips to CORRUPT, the whole recovery model is wrong.
    #[test]
    fn scan_treats_zero_byte_rows_db_as_ok() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(1);
        let gen = StorageEngine::list_generations_in_dir(&table_dir)[0];
        let rows_db = resolve_gen_dir(&table_dir, gen)
            .unwrap()
            .join(format!("{gen}-Rows.db"));
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&rows_db)
            .unwrap();
        assert_eq!(std::fs::metadata(&rows_db).unwrap().len(), 0);

        let reports = scan_report(&table_dir, false).unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0].verdict, "OK",
            "zero-byte Rows.db must remain OK (false-positive guard)"
        );
    }

    #[test]
    fn scan_includes_quarantined_generations_when_requested() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        // Quarantine the newest generation via the engine's own mover.
        let gen = StorageEngine::list_generations_in_dir(&table_dir)[0];
        StorageEngine::quarantine_corrupt_generation(&table_dir, gen).unwrap();

        // Without the flag, the quarantined gen is not reported.
        let live_only = scan_report(&table_dir, false).unwrap();
        assert!(
            live_only.iter().all(|r| r.gen != gen),
            "quarantined gen excluded by default"
        );

        // With the flag, it appears tagged `quarantine`.
        let with_q = scan_report(&table_dir, true).unwrap();
        let q = with_q
            .iter()
            .find(|r| r.gen == gen)
            .expect("quarantined gen present with include_quarantine");
        assert_eq!(q.location, "quarantine");
    }

    #[test]
    fn inspect_finds_generation_in_quarantine() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(1);
        let gen = StorageEngine::list_generations_in_dir(&table_dir)[0];
        StorageEngine::quarantine_corrupt_generation(&table_dir, gen).unwrap();

        let report = inspect_report(&table_dir, gen).unwrap();
        assert_eq!(report.gen, gen);
        assert_eq!(report.location, "quarantine");
        assert!(
            report
                .components
                .iter()
                .any(|c| c.name == "Data.db" && c.present),
            "Data.db component reported present"
        );
    }

    #[test]
    fn inspect_classifies_truncation() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(1);
        let bad = corrupt_one_generation(&table_dir);
        let report = inspect_report(&table_dir, bad).unwrap();
        assert_eq!(report.verdict, "CORRUPT");
        assert!(
            report.likely_corruption_class.is_some(),
            "a corrupt gen gets a corruption-class hint"
        );
    }

    #[test]
    fn scan_report_errors_on_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope");
        assert!(scan_report(&missing, false).is_err());
    }

    // ── quarantine tests ─────────────────────────────────────────────────────

    #[test]
    fn quarantine_plan_targets_only_corrupt_generations() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let bad = corrupt_one_generation(&table_dir);
        let plan = quarantine_plan(&table_dir).unwrap();
        assert_eq!(plan.len(), 1, "only the corrupt gen is planned");
        assert_eq!(plan[0].gen, bad);
        assert_eq!(plan[0].action, "move");
        assert!(plan[0].reason.is_some());
    }

    #[test]
    fn quarantine_dry_run_moves_nothing() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let bad = corrupt_one_generation(&table_dir);
        let bad_data = table_dir.join(format!("{bad}-Data.db"));
        assert!(bad_data.exists(), "precondition: corrupt gen is live");

        // apply = false → dry run.
        sstable_quarantine(&table_dir, false, true).unwrap();

        assert!(
            bad_data.exists(),
            "dry run must NOT move the corrupt generation"
        );
        assert!(
            !table_dir
                .join("quarantine")
                .join(format!("{bad}-Data.db"))
                .exists(),
            "dry run must not populate quarantine/"
        );
    }

    #[test]
    fn quarantine_apply_moves_only_corrupt_and_keeps_healthy() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let bad = corrupt_one_generation(&table_dir);
        let good = StorageEngine::list_generations_in_dir(&table_dir)
            .into_iter()
            .find(|g| *g != bad)
            .expect("a healthy sibling exists");

        sstable_quarantine(&table_dir, true, true).unwrap();

        // Corrupt gen moved out of the live dir into quarantine/.
        assert!(
            !table_dir.join(format!("{bad}-Data.db")).exists(),
            "corrupt gen left the live dir"
        );
        assert!(
            table_dir
                .join("quarantine")
                .join(format!("{bad}-Data.db"))
                .exists(),
            "corrupt gen is now in quarantine/"
        );
        // Healthy gen untouched.
        assert!(
            table_dir.join(format!("{good}-Data.db")).exists(),
            "healthy gen stays live"
        );
    }

    #[test]
    fn quarantine_plan_flags_already_quarantined_duplicate() {
        // Build one corrupt gen, then place a same-numbered copy in quarantine/
        // (the S3 re-download duplicate case). The live copy must be classified
        // `already-quarantined` so apply leaves it in place rather than
        // overwriting the quarantined copy.
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(1);
        let bad = corrupt_one_generation(&table_dir);
        let q = table_dir.join("quarantine");
        std::fs::create_dir_all(&q).unwrap();
        for name in ["Data.db", "Partitions.db"] {
            let src = table_dir.join(format!("{bad}-{name}"));
            if src.exists() {
                std::fs::copy(&src, q.join(format!("{bad}-{name}"))).unwrap();
            }
        }

        let plan = quarantine_plan(&table_dir).unwrap();
        let entry = plan
            .iter()
            .find(|a| a.gen == bad)
            .expect("corrupt gen planned");
        assert_eq!(
            entry.action, "already-quarantined",
            "a gen already present in quarantine/ is not re-moved"
        );

        // apply leaves the live copy in place (no overwrite).
        sstable_quarantine(&table_dir, true, true).unwrap();
        assert!(
            table_dir.join(format!("{bad}-Data.db")).exists(),
            "already-quarantined duplicate is left in place, never overwriting"
        );
    }

    // ── salvage tests ────────────────────────────────────────────────────────

    #[test]
    fn salvage_report_recovers_all_rows_from_healthy_table() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let report = salvage_report(&table_dir, false).unwrap();
        assert_eq!(report.len(), 2);
        for r in &report {
            assert_eq!(r.verdict, "OK");
            // The fixture writes one 1-row partition per generation.
            assert_eq!(r.partitions_total, 1);
            assert_eq!(r.partitions_complete, 1);
            assert_eq!(r.partitions_partial, 0);
            assert_eq!(r.partitions_failed, 0);
            assert_eq!(r.rows_recovered, 1, "healthy gen recovers its row");
        }
    }

    #[test]
    fn salvage_report_is_resilient_to_corrupt_generation() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let bad = corrupt_one_generation(&table_dir);

        // Must not error despite the corrupt generation.
        let report = salvage_report(&table_dir, false).unwrap();
        let bad_entry = report
            .iter()
            .find(|r| r.gen == bad)
            .expect("corrupt gen present");
        assert_eq!(bad_entry.verdict, "CORRUPT");
        // The healthy sibling still recovers its row in full.
        assert!(
            report
                .iter()
                .any(|r| r.gen != bad && r.verdict == "OK" && r.rows_recovered == 1),
            "healthy sibling recovers fully alongside the corrupt gen"
        );
    }
}
