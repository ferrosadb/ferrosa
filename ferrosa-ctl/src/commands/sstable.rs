//! Offline SSTable analysis and recovery — `ferrosa-ctl sstable ...`.
//!
//! Unlike the rest of `ferrosa-ctl`, these subcommands take **no network
//! connection**. They operate directly on an on-disk table directory (the
//! `<data_dir>/sstables/<keyspace>.<table>/` layout) and its `quarantine/`
//! subdirectory, the same way the engine does at startup.
//!
//! The corruption verdict is delegated to the storage engine's own startup
//! smoke test, `StorageEngine::smoke_test_generation` — the exact function
//! the self-heal detector runs to decide what to quarantine. A ctl verdict can
//! therefore never diverge from what a node decides at boot. Component-level
//! detail (partition counts, Data.db extent, timestamps) comes from the BTI
//! `SSTableReader`.
//!
//! All commands here are **read-only** except the recovery action, which only
//! ever moves files (never deletes) and defaults to a dry run.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use tabled::builder::Builder;
use tabled::settings::Style;

use ferrosa_common::{DecoratedKey, NO_TIMESTAMP};
use ferrosa_cql::client::{CqlClient, PreparedStatement};
use ferrosa_sstable::io::FileReadAt;
use ferrosa_sstable::marshal::value_length_if_fixed;
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::Row;
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
    /// Why the reader could not be opened, when it could not. Every field
    /// the reader would have filled is `None` in that case.
    pub open_error: Option<String>,
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
    let mut open_error = None;
    match open_reader(&dir, gen) {
        Err(e) => open_error = Some(e.to_string()),
        Ok(reader) => {
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
        open_error,
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
    if let Some(e) = &r.open_error {
        println!("  open error:         {e}");
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
    /// Why the generation could not be opened or salvaged; its counts are
    /// zero (or partial) because of it, not because nothing is recoverable.
    pub error: Option<String>,
    /// Where the partition walk stopped before EOF; later partitions were
    /// not attempted, so the counts are a floor.
    pub walk_stopped: Option<String>,
    /// Why the first partially recovered partition stopped.
    pub first_partial: Option<String>,
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
            error: None,
            walk_stopped: None,
            first_partial: None,
        };
        // salvage with a no-op sink — bounded memory, we only want the stats.
        let stats = open_reader(dir, gen).and_then(|reader| Ok(reader.salvage(|_, _, _| {})?));
        match stats {
            Ok(stats) => {
                entry.partitions_total = stats.partitions_total;
                entry.partitions_complete = stats.partitions_complete;
                entry.partitions_partial = stats.partitions_partial;
                entry.partitions_failed = stats.partitions_failed;
                entry.rows_recovered = stats.rows_recovered;
                entry.static_rows_recovered = stats.static_rows_recovered;
                entry.walk_stopped = stats.walk_stopped;
                entry.first_partial = stats.first_partial;
                entry.error = stats.first_failure;
            }
            Err(e) => entry.error = Some(e.to_string()),
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
    // Zero counts for a generation that could not be measured are not "nothing
    // recoverable"; say which ones and why.
    for r in reports {
        if let Some(e) = &r.error {
            println!("gen {} ({}): {e}", r.gen, r.location);
        }
        if let Some(why) = &r.walk_stopped {
            println!("gen {} ({}): COUNTS ARE A FLOOR — {why}", r.gen, r.location);
        }
        if let Some(why) = &r.first_partial {
            println!("gen {} ({}): first partial: {why}", r.gen, r.location);
        }
    }
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

// ── find (locate rows by a byte pattern) ────────────────────────────────────────

/// One cell of a row [`find`](sstable_find) matched.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FoundCell {
    /// Column name from the generation's own serialization header, or
    /// `#<index>` when the index is outside the header.
    pub column: String,
    /// The column's marshal type from the header.
    pub column_type: String,
    /// `None` for a tombstone.
    pub value_hex: Option<String>,
    pub timestamp: i64,
    pub ttl: i32,
    pub local_deletion_time: i32,
}

/// One row [`find`](sstable_find) matched, with everything needed to tell a
/// correctly written value from bytes taken out of a neighbouring field.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FoundRow {
    pub gen: u64,
    pub location: String,
    pub partition_key_hex: String,
    pub clustering_hex: String,
    /// `i64::MIN` when the row has no primary-key liveness.
    pub liveness_timestamp: i64,
    /// Where the pattern occurred: `partition_key`, `clustering`, or a column.
    pub matched_in: Vec<String>,
    pub cells: Vec<FoundCell>,
}

fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String cannot fail.
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Parse a hex pattern, ignoring `-`, `:` and spaces so a UUID pastes as is.
fn parse_hex_pattern(text: &str) -> Result<Vec<u8>, CtlError> {
    let digits: Vec<u8> = text
        .bytes()
        .filter(|b| !matches!(b, b'-' | b':' | b' '))
        .collect();
    if digits.is_empty() || !digits.len().is_multiple_of(2) {
        return Err(format!("pattern {text:?} is not an even number of hex digits").into());
    }
    digits
        .chunks(2)
        .map(|pair| {
            let s = std::str::from_utf8(pair).map_err(|e| e.to_string())?;
            u8::from_str_radix(s, 16).map_err(|e| format!("pattern {text:?}: {e}").into())
        })
        .collect()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Whether the pattern occurs in the row's partition key, clustering or any
/// cell value. Borrow-only: the scan calls this for EVERY row, so it must not
/// allocate. Only a row that passes is rendered by [`render_row`].
fn row_matches(key_bytes: &[u8], row: &Row, pattern: &[u8]) -> bool {
    contains(key_bytes, pattern)
        || contains(&row.clustering, pattern)
        || row
            .cells
            .iter()
            .any(|(_, cell)| cell.value.as_deref().is_some_and(|v| contains(v, pattern)))
}

/// Render a row that [`row_matches`] accepted, every cell named from the
/// generation's own header.
///
/// A cell's column index counts within its own row kind: the decoder indexes
/// a static row's cells into `static_columns` and a clustered row's into
/// `regular_columns`, so `is_static` picks the list.
fn render_row(
    header: &SerializationHeader,
    key_bytes: &[u8],
    row: &Row,
    is_static: bool,
    pattern: &[u8],
    gen: u64,
    location: &str,
) -> FoundRow {
    let mut matched_in = Vec::new();
    if contains(key_bytes, pattern) {
        matched_in.push("partition_key".to_string());
    }
    if contains(&row.clustering, pattern) {
        matched_in.push("clustering".to_string());
    }
    let columns = if is_static {
        &header.static_columns
    } else {
        &header.regular_columns
    };
    let mut cells = Vec::with_capacity(row.cells.len());
    for (col_idx, cell) in &row.cells {
        let idx = *col_idx as usize;
        let (name, ty) = columns.get(idx).map_or_else(
            || (format!("#{idx}"), "?".to_string()),
            |(n, t)| (String::from_utf8_lossy(n).into_owned(), t.clone()),
        );
        if cell.value.as_deref().is_some_and(|v| contains(v, pattern)) {
            matched_in.push(name.clone());
        }
        cells.push(FoundCell {
            column: name,
            column_type: ty,
            value_hex: cell.value.as_deref().map(to_hex),
            timestamp: cell.timestamp,
            ttl: cell.ttl,
            local_deletion_time: cell.local_deletion_time,
        });
    }
    debug_assert!(!matched_in.is_empty(), "render_row called on a non-match");
    FoundRow {
        gen,
        location: location.to_string(),
        partition_key_hex: to_hex(key_bytes),
        clustering_hex: to_hex(&row.clustering),
        liveness_timestamp: row.primary_key_liveness.timestamp,
        matched_in,
        cells,
    }
}

/// One thing [`find_rows`] observed, handed to the caller's sink the moment it
/// is seen. Nothing is accumulated: a tenant-wide pattern can match tens of
/// thousands of rows, and holding them to print at the end is exactly the
/// materialization this tool must not do.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FindEvent {
    /// A matching row.
    Row(FoundRow),
    /// A generation that could not be read to the end. A corrupt generation is
    /// what this command is pointed at, so it does not abort the search, but it
    /// is never a silent gap: rows decoded before the failure were already
    /// emitted, and this event follows them.
    Unreadable(UnreadableGen),
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct UnreadableGen {
    pub gen: u64,
    pub location: String,
    /// Partitions decoded before the failure.
    pub partitions_read: u64,
    pub error: String,
}

/// Counts only; the rows themselves went to the sink.
#[derive(Debug, Clone, Copy, Serialize, Default, PartialEq)]
pub struct FindSummary {
    pub generations_scanned: u64,
    pub rows_scanned: u64,
    pub rows_matched: u64,
    pub generations_unreadable: u64,
}

/// The caller's consumer for [`FindEvent`]s. An `Err` (stdout closed, disk
/// full) aborts the whole search: losing output is not a per-generation
/// failure, and carrying on would report a search nobody saw.
pub type FindSink<'a> = dyn FnMut(FindEvent) -> Result<(), CtlError> + 'a;

/// Every row in `table_dir` (and, when asked, its `quarantine/`) whose key,
/// clustering or a cell value contains `pattern`, emitted to `sink` as found.
/// Pure read.
///
/// Streams rows, not partitions: `partitions_iter` +
/// `next_partition_header_only` + `stream_clustered_rows`, the same walk the
/// startup smoke test uses, so a wide partition is never materialized and at
/// most one decoded row is live at a time. Non-matching rows are tested by
/// borrow and never rendered.
pub fn find_rows(
    table_dir: &Path,
    pattern: &[u8],
    include_quarantine: bool,
    sink: &mut FindSink<'_>,
) -> Result<FindSummary, CtlError> {
    if !table_dir.is_dir() {
        return Err(format!("not a directory: {}", table_dir.display()).into());
    }
    if pattern.is_empty() {
        return Err("empty pattern matches everything".into());
    }
    let q = table_dir.join("quarantine");
    let quarantine = (include_quarantine && q.is_dir()).then_some((q, "quarantine"));
    let dirs = std::iter::once((table_dir.to_path_buf(), "live")).chain(quarantine);
    let mut summary = FindSummary::default();
    for (dir, location) in dirs {
        for gen in StorageEngine::list_generations_in_dir(&dir) {
            summary.generations_scanned += 1;
            let mut scan = GenScan {
                gen,
                location,
                pattern,
                sink: &mut *sink,
                summary: &mut summary,
                partitions_read: 0,
                sink_error: None,
            };
            let outcome = scan.run(&dir);
            let (partitions_read, sink_error) = (scan.partitions_read, scan.sink_error);
            if let Some(e) = sink_error {
                return Err(e);
            }
            if let Err(e) = outcome {
                summary.generations_unreadable += 1;
                sink(FindEvent::Unreadable(UnreadableGen {
                    gen,
                    location: location.to_string(),
                    partitions_read,
                    error: e.to_string(),
                }))?;
            }
        }
    }
    Ok(summary)
}

/// The walk of one generation. A sink failure inside the row callback has to
/// stop the decoder, which only understands its own error type, so the real
/// error is parked in `sink_error` and [`find_rows`] returns it instead of
/// reporting the generation as unreadable.
struct GenScan<'s, 'k> {
    gen: u64,
    location: &'static str,
    pattern: &'s [u8],
    sink: &'s mut FindSink<'k>,
    summary: &'s mut FindSummary,
    partitions_read: u64,
    sink_error: Option<CtlError>,
}

impl GenScan<'_, '_> {
    fn run(&mut self, dir: &Path) -> Result<(), CtlError> {
        let reader = Arc::new(open_reader(dir, self.gen)?);
        let header = reader.header();
        let mut iter = reader.partitions_iter()?;
        while let Some((key, _deletion, static_row)) = iter.next_partition_header_only()? {
            self.partitions_read += 1;
            let key_bytes = key.key.as_bytes();
            if let Some(row) = &static_row {
                self.offer(header, key_bytes, row, true);
            }
            iter.stream_clustered_rows(|row| {
                self.offer(header, key_bytes, row, false);
                match self.sink_error {
                    None => Ok(()),
                    Some(_) => Err(ferrosa_common::Error::InvalidFormat(
                        "find: output failed; stopping the scan".to_string(),
                    )),
                }
            })?;
            if self.sink_error.is_some() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Test one row; render and emit it only if it matches.
    fn offer(
        &mut self,
        header: &SerializationHeader,
        key_bytes: &[u8],
        row: &Row,
        is_static: bool,
    ) {
        self.summary.rows_scanned += 1;
        if self.sink_error.is_some() || !row_matches(key_bytes, row, self.pattern) {
            return;
        }
        self.summary.rows_matched += 1;
        let found = render_row(
            header,
            key_bytes,
            row,
            is_static,
            self.pattern,
            self.gen,
            self.location,
        );
        if let Err(e) = (self.sink)(FindEvent::Row(found)) {
            self.sink_error = Some(e);
        }
    }
}

/// `ferrosa-ctl sstable find` entry point. Each match is written as it is
/// found: JSON Lines (one [`FindEvent`] per line, then a `summary` line) with
/// `json`, or text otherwise. Output errors are returned, never swallowed.
pub fn sstable_find(
    table_dir: &Path,
    pattern_hex: &str,
    include_quarantine: bool,
    json: bool,
) -> Result<(), CtlError> {
    use std::io::Write as _;
    let pattern = parse_hex_pattern(pattern_hex)?;
    let mut out = std::io::BufWriter::new(std::io::stdout().lock());
    let summary = find_rows(table_dir, &pattern, include_quarantine, &mut |event| {
        if json {
            serde_json::to_writer(&mut out, &event)?;
            writeln!(out)?;
        } else {
            write_event_text(&mut out, &event)?;
        }
        Ok(())
    })?;
    if json {
        serde_json::to_writer(
            &mut out,
            &serde_json::json!({ "kind": "summary", "summary": summary }),
        )?;
        writeln!(out)?;
    } else {
        writeln!(
            out,
            "{}: {} matching row(s) of {} scanned in {} generation(s); \
             {} generation(s) could not be read to the end",
            table_dir.display(),
            summary.rows_matched,
            summary.rows_scanned,
            summary.generations_scanned,
            summary.generations_unreadable
        )?;
    }
    out.flush()?;
    Ok(())
}

fn write_event_text(out: &mut impl std::io::Write, event: &FindEvent) -> std::io::Result<()> {
    match event {
        FindEvent::Row(r) => {
            writeln!(
                out,
                "gen {} ({}) pk={} ck={} liveness_ts={} matched_in={:?}",
                r.gen,
                r.location,
                r.partition_key_hex,
                r.clustering_hex,
                r.liveness_timestamp,
                r.matched_in
            )?;
            for c in &r.cells {
                writeln!(
                    out,
                    "    {} ({}) = {} ts={} ttl={} ldt={}",
                    c.column,
                    c.column_type,
                    c.value_hex.as_deref().unwrap_or("<tombstone>"),
                    c.timestamp,
                    c.ttl,
                    c.local_deletion_time
                )?;
            }
            Ok(())
        }
        // Unreadable generations go in the same stream as the rows, in order,
        // so a reader sees which rows preceded the failure.
        FindEvent::Unreadable(u) => writeln!(
            out,
            "UNREADABLE gen {} ({}) after {} partition(s): {}",
            u.gen, u.location, u.partitions_read, u.error
        ),
    }
}

// ── reingest (recover salvaged rows through the live write path) ─────────────────

/// A table's partition-key and clustering column names, in position order,
/// read from `system_schema.columns`. Regular column names come from the
/// SSTable header (which carries them), so only the key columns — which the
/// header does NOT name — need the live schema.
#[derive(Debug, Clone, PartialEq)]
pub struct TableLayout {
    pub keyspace: String,
    pub table: String,
    pub partition_key: Vec<String>,
    pub clustering: Vec<String>,
}

/// One salvaged row reconstructed into the column names + bound value bytes for
/// a prepared INSERT, plus the write timestamp to preserve (LWW correctness).
#[derive(Debug, Clone, PartialEq)]
pub struct RowInsert {
    pub columns: Vec<String>,
    /// Raw serialized value bytes, positional with `columns`. The on-disk cell
    /// bytes are already the CQL-wire value form, so they bind directly.
    pub values: Vec<Option<Vec<u8>>>,
    /// `USING TIMESTAMP` to apply, or `None` to let the server assign.
    pub timestamp: Option<i64>,
    /// Fixed-width cells dropped for a wrong value length.
    pub dropped_cells: u32,
}

/// Why [`reconstruct_row`] declined a salvaged row. Reingest counts these by
/// kind, so a skipped row is never just a missing number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RowSkip {
    /// The key or clustering does not split into the layout's component count.
    KeyShape,
    /// A key component's length does not match its fixed-width type.
    KeyLength,
    /// A cell names a column index outside its row kind's header list: the
    /// row was misread, so none of it can be trusted.
    UnknownColumn,
    /// No live cell to recover (tombstones only, or a key-only row).
    NoLiveCells,
}

impl std::fmt::Display for RowSkip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            RowSkip::KeyShape => "key does not split into the table's key columns",
            RowSkip::KeyLength => "a key component has the wrong length for its type",
            RowSkip::UnknownColumn => "a cell names a column outside the header",
            RowSkip::NoLiveCells => "no live cell to recover",
        })
    }
}

/// Reconstruct an INSERT for one salvaged row, or say why not.
///
/// A cell's column index counts within its own row kind: the decoder indexes
/// a static row's cells into `static_columns` and a clustered row's into
/// `regular_columns`, so `is_static` picks the list. A fixed-width cell whose
/// length is wrong is dropped and counted in [`RowInsert::dropped_cells`];
/// the rest of the row is kept.
fn reconstruct_row(
    layout: &TableLayout,
    header: &SerializationHeader,
    key: &DecoratedKey,
    row: &Row,
    is_static: bool,
) -> Result<RowInsert, RowSkip> {
    let pk = ferrosa_cql::bridge::decode_pk(key, layout.partition_key.len());
    if pk.len() != layout.partition_key.len() {
        return Err(RowSkip::KeyShape);
    }
    // A static row has no clustering; its key is the partition key alone.
    let ck_columns: &[String] = if is_static { &[] } else { &layout.clustering };
    let ck = ferrosa_cql::bridge::decode_clustering(&row.clustering, ck_columns.len());
    if ck.len() != ck_columns.len() {
        return Err(RowSkip::KeyShape);
    }

    // Reject the whole row if any key component's byte-length doesn't match its
    // fixed-length type. A bad key length is drift garbage — inserting it would
    // misfile data under a wrong/partial key, the worst possible outcome.
    if !key_lengths_valid(&pk, &pk_component_types(&header.key_type))
        || !key_lengths_valid(&ck, &header.clustering_types)
    {
        return Err(RowSkip::KeyLength);
    }

    let header_columns = if is_static {
        &header.static_columns
    } else {
        &header.regular_columns
    };
    let mut columns: Vec<String> = Vec::with_capacity(pk.len() + ck.len() + row.cells.len());
    let mut values: Vec<Option<Vec<u8>>> = Vec::with_capacity(columns.capacity());

    for (name, val) in layout.partition_key.iter().zip(pk) {
        columns.push(name.clone());
        values.push(Some(val));
    }
    for (name, val) in ck_columns.iter().zip(ck) {
        columns.push(name.clone());
        values.push(Some(val));
    }

    let mut timestamp = if row.primary_key_liveness.has_timestamp() {
        Some(row.primary_key_liveness.timestamp)
    } else {
        None
    };
    let mut live_cells = 0usize;
    let mut dropped_cells = 0u32;
    for (col_idx, cell) in &row.cells {
        let Some((name_bytes, ty)) = header_columns.get(*col_idx as usize) else {
            return Err(RowSkip::UnknownColumn);
        };
        // Skip tombstones — a cell with no value is a deletion, not data.
        let Some(value) = &cell.value else { continue };
        // Drop a cell whose value byte-length doesn't match its fixed-length
        // type (drift garbage, or an empty value for a fixed column) — the
        // server would reject it. Variable-length types (text/blob/vector) pass
        // and are caught by the per-row execute if still invalid.
        if value_length_if_fixed(ty).is_some_and(|n| value.len() != n) {
            dropped_cells += 1;
            continue;
        }
        columns.push(String::from_utf8_lossy(name_bytes).into_owned());
        values.push(Some(value.to_vec()));
        live_cells += 1;
        if cell.timestamp != NO_TIMESTAMP {
            timestamp = Some(timestamp.map_or(cell.timestamp, |t| t.max(cell.timestamp)));
        }
    }

    if live_cells == 0 {
        return Err(RowSkip::NoLiveCells);
    }
    Ok(RowInsert {
        columns,
        values,
        timestamp,
        dropped_cells,
    })
}

/// True if every component whose type is fixed-length has the exact expected
/// byte-length. Variable-length components always pass.
fn key_lengths_valid(components: &[Vec<u8>], types: &[String]) -> bool {
    for (val, ty) in components.iter().zip(types.iter()) {
        if let Some(n) = value_length_if_fixed(ty) {
            if val.len() != n {
                return false;
            }
        }
    }
    true
}

/// The per-component marshal type names of a partition key. A `CompositeType(..)`
/// key splits into its inner types; a single-column key is one type.
fn pk_component_types(key_type: &str) -> Vec<String> {
    if let Some(start) = key_type.find("CompositeType(") {
        let inner = &key_type[start + "CompositeType(".len()..];
        let inner = inner.strip_suffix(')').unwrap_or(inner);
        split_top_level(inner)
    } else {
        vec![key_type.to_string()]
    }
}

/// Split a comma-separated type list at depth-0 commas so nested generic types
/// (e.g. `ReversedType(...)`) stay intact.
fn split_top_level(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                out.push(s[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        out.push(last.to_string());
    }
    out
}

/// Build the `INSERT` CQL text (with `?` markers and an optional
/// `USING TIMESTAMP ?`) for a given ordered column set. Identity-quoted to
/// tolerate reserved words / case. The timestamp marker, when present, is the
/// LAST bind position.
fn insert_cql(keyspace: &str, table: &str, columns: &[String], with_timestamp: bool) -> String {
    let cols = columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let markers = vec!["?"; columns.len()].join(", ");
    let mut cql = format!("INSERT INTO \"{keyspace}\".\"{table}\" ({cols}) VALUES ({markers})");
    if with_timestamp {
        cql.push_str(" USING TIMESTAMP ?");
    }
    cql
}

/// Parse `keyspace.table` from a table SSTable directory name.
fn parse_keyspace_table(table_dir: &Path) -> Result<(String, String), CtlError> {
    let name = table_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("cannot read table dir name from {}", table_dir.display()))?;
    let (ks, tbl) = name
        .split_once('.')
        .ok_or_else(|| format!("table dir '{name}' is not in <keyspace>.<table> form"))?;
    Ok((ks.to_string(), tbl.to_string()))
}

/// Build a [`TableLayout`] from a `system_schema.columns` query result. Pure —
/// unit-tested with a synthetic result, no live node required.
fn parse_layout_from_result(
    keyspace: &str,
    table: &str,
    res: &ferrosa_cql::client::QueryResult,
) -> Result<TableLayout, CtlError> {
    let col = |n: &str| res.column_names.iter().position(|c| c == n);
    let (ni, ki, pi) = match (col("column_name"), col("kind"), col("position")) {
        (Some(a), Some(b), Some(c)) => (a, b, c),
        _ => return Err("system_schema.columns result missing expected columns".into()),
    };
    let mut pk: Vec<(i32, String)> = Vec::new();
    let mut ck: Vec<(i32, String)> = Vec::new();
    for row in &res.rows {
        let Some(name) = row_text(row, ni) else {
            continue;
        };
        let Some(kind) = row_text(row, ki) else {
            continue;
        };
        let position = row_i32(row, pi).unwrap_or(0);
        match kind.as_str() {
            "partition_key" => pk.push((position, name)),
            "clustering" => ck.push((position, name)),
            _ => {}
        }
    }
    if pk.is_empty() {
        return Err(format!("no partition-key columns found for {keyspace}.{table}").into());
    }
    pk.sort_by_key(|(p, _)| *p);
    ck.sort_by_key(|(p, _)| *p);
    Ok(TableLayout {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        partition_key: pk.into_iter().map(|(_, n)| n).collect(),
        clustering: ck.into_iter().map(|(_, n)| n).collect(),
    })
}

fn row_text(row: &ferrosa_cql::client::ResultRow, i: usize) -> Option<String> {
    row.columns
        .get(i)
        .and_then(|o| o.as_ref())
        .map(|b| String::from_utf8_lossy(b).into_owned())
}

fn row_i32(row: &ferrosa_cql::client::ResultRow, i: usize) -> Option<i32> {
    row.columns
        .get(i)
        .and_then(|o| o.as_ref())
        .filter(|b| b.len() == 4)
        .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

/// Fetch a table's key-column layout from the live node's `system_schema.columns`.
async fn fetch_table_layout(
    client: &mut CqlClient,
    keyspace: &str,
    table: &str,
) -> Result<TableLayout, CtlError> {
    let q = format!(
        "SELECT column_name, kind, position FROM system_schema.columns \
         WHERE keyspace_name = '{keyspace}' AND table_name = '{table}'"
    );
    let res = client.query(&q).await?;
    parse_layout_from_result(keyspace, table, &res)
}

/// `ferrosa-ctl sstable reingest` entry point. Salvages rows from this table's
/// CORRUPT generations and re-inserts them through the live write path (the
/// fixed writer + proper RF replication). **Dry-run by default**: connects only
/// to read schema, reports how many rows would be re-ingested, and writes
/// nothing. With `apply`, executes prepared INSERTs preserving each row's
/// original `USING TIMESTAMP`.
pub async fn sstable_reingest(
    table_dir: &Path,
    host: SocketAddr,
    user: &str,
    password: &str,
    apply: bool,
    include_quarantine: bool,
    limit: Option<u64>,
) -> Result<(), CtlError> {
    if !table_dir.is_dir() {
        return Err(format!("not a directory: {}", table_dir.display()).into());
    }
    let (keyspace, table) = parse_keyspace_table(table_dir)?;
    let mut client = CqlClient::connect_with_credentials(host, user, password).await?;
    let layout = fetch_table_layout(&mut client, &keyspace, &table).await?;

    let mut dirs: Vec<PathBuf> = vec![table_dir.to_path_buf()];
    if include_quarantine {
        let q = table_dir.join("quarantine");
        if q.is_dir() {
            dirs.push(q);
        }
    }

    let mut tally = ReingestTally::default();
    let mut prepared = PreparedCache::default();
    'gens: for dir in &dirs {
        for gen in StorageEngine::list_generations_in_dir(dir) {
            // Only CORRUPT generations need salvage; healthy ones are already served.
            if StorageEngine::smoke_test_generation(dir, gen).is_ok() {
                continue;
            }
            tally.corrupt_gens += 1;
            let reader = match open_reader(dir, gen) {
                Ok(r) => r,
                Err(e) => {
                    tally.unsalvageable_gens += 1;
                    eprintln!(
                        "gen {gen} ({}): cannot open for salvage: {e}",
                        dir.display()
                    );
                    continue;
                }
            };
            let (mut rows, worker) = spawn_generation_salvage(reader, layout.clone());
            while let Some(item) = rows.recv().await {
                let ins = match item {
                    Ok(ins) => ins,
                    Err(skip) => {
                        *tally.skipped.entry(skip).or_default() += 1;
                        continue;
                    }
                };
                tally.reconstructed += 1;
                tally.dropped_cells += u64::from(ins.dropped_cells);
                if !apply {
                    continue;
                }
                if limit.is_some_and(|cap| tally.inserted >= cap) {
                    break;
                }
                // A single row the server rejects (e.g. a variable-length cell
                // that is still drift garbage) must not abort recovery of the
                // rest: count it, show why, keep going.
                match execute_row_insert(&mut client, &keyspace, &table, ins, &mut prepared).await {
                    Ok(()) => tally.inserted += 1,
                    Err(e) => tally.reject(gen, &e),
                }
            }
            // Closing the receiver tells a still-running salvage to stop
            // reconstructing rows nobody will read.
            drop(rows);
            match worker.await {
                Ok(Ok(stats)) => {
                    // Rows past where salvage stopped were never offered; a
                    // count that silently stops there would read as complete.
                    let reasons = [
                        &stats.walk_stopped,
                        &stats.first_partial,
                        &stats.first_failure,
                    ];
                    for why in reasons.into_iter().flatten() {
                        eprintln!("gen {gen} ({}): {why}", dir.display());
                    }
                    if stats.walk_stopped.is_some() {
                        tally.unsalvageable_gens += 1;
                    }
                }
                Ok(Err(e)) => {
                    tally.unsalvageable_gens += 1;
                    eprintln!("gen {gen} ({}): salvage stopped: {e}", dir.display());
                }
                Err(e) => return Err(format!("gen {gen}: salvage task failed: {e}").into()),
            }
            if apply && limit.is_some_and(|cap| tally.inserted >= cap) {
                break 'gens;
            }
        }
    }

    tally.print(&keyspace, &table, apply, &layout);
    if tally.unsalvageable_gens > 0 {
        return Err(format!(
            "{} of {} corrupt generation(s) could not be salvaged; the counts above are incomplete",
            tally.unsalvageable_gens, tally.corrupt_gens
        )
        .into());
    }
    Ok(())
}

/// Rows in flight between the salvage thread and the inserter. Bounds memory
/// regardless of generation size, and blocks salvage while inserts catch up.
const REINGEST_CHANNEL_ROWS: usize = 256;

/// Rejected inserts printed individually before the rest are only counted.
const REINGEST_REJECTS_SHOWN: u64 = 20;

/// Salvage one generation on a blocking thread, streaming each reconstructed
/// row (or the reason it was skipped) through a bounded channel. The salvage
/// result, including a failure partway through, comes back on the handle.
fn spawn_generation_salvage(
    reader: SSTableReader<FileReadAt>,
    layout: TableLayout,
) -> (
    tokio::sync::mpsc::Receiver<Result<RowInsert, RowSkip>>,
    tokio::task::JoinHandle<ferrosa_common::Result<ferrosa_sstable::reader::SalvageStats>>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(REINGEST_CHANNEL_ROWS);
    let worker = tokio::task::spawn_blocking(move || {
        let header = reader.header().clone();
        // Salvage hands over one recovered row at a time from a reused
        // buffer; each is reconstructed and sent, never collected.
        reader.salvage(|key, row, is_static| {
            // A send fails only when the inserter hung up (limit reached);
            // the receiver is then closed and this check skips the rest.
            if !tx.is_closed() {
                let delivered = tx
                    .blocking_send(reconstruct_row(&layout, &header, key, row, is_static))
                    .is_ok();
                debug_assert!(delivered || tx.is_closed());
            }
        })
    });
    (rx, worker)
}

/// What `reingest` did, for the closing summary.
#[derive(Default)]
struct ReingestTally {
    corrupt_gens: u64,
    unsalvageable_gens: u64,
    reconstructed: u64,
    inserted: u64,
    rejected: u64,
    dropped_cells: u64,
    skipped: std::collections::BTreeMap<RowSkip, u64>,
}

impl ReingestTally {
    fn reject(&mut self, gen: u64, e: &CtlError) {
        self.rejected += 1;
        if self.rejected <= REINGEST_REJECTS_SHOWN {
            eprintln!("gen {gen}: insert rejected: {e}");
        } else if self.rejected == REINGEST_REJECTS_SHOWN + 1 {
            eprintln!("(further rejected inserts are counted, not shown)");
        }
    }

    fn print(&self, keyspace: &str, table: &str, apply: bool, layout: &TableLayout) {
        if apply {
            println!(
                "{keyspace}.{table}: re-ingested {} row(s) from {} corrupt generation(s) via the \
                 live write path; {} rejected by the server.",
                self.inserted, self.corrupt_gens, self.rejected
            );
        } else {
            println!(
                "{keyspace}.{table}: DRY RUN — {} row(s) reconstructable from {} corrupt \
                 generation(s) would be re-ingested. PK={:?} CK={:?}. Re-run with --apply to \
                 write (data goes through the fixed writer + RF replication).",
                self.reconstructed, self.corrupt_gens, layout.partition_key, layout.clustering
            );
        }
        for (skip, n) in &self.skipped {
            println!("  skipped {n} row(s): {skip}");
        }
        if self.dropped_cells > 0 {
            println!(
                "  dropped {} fixed-width cell(s) with a wrong value length",
                self.dropped_cells
            );
        }
    }
}

/// Prepared INSERTs keyed by column list, one map per `USING TIMESTAMP`
/// variant. Looked up by `&[String]`, so a cache hit allocates nothing.
#[derive(Default)]
struct PreparedCache {
    by_columns: [std::collections::HashMap<Vec<String>, PreparedStatement>; 2],
}

/// Prepare (cached) and execute one reconstructed row's INSERT. Takes the row
/// by value so its values bind without a copy.
async fn execute_row_insert(
    client: &mut CqlClient,
    keyspace: &str,
    table: &str,
    ins: RowInsert,
    cache: &mut PreparedCache,
) -> Result<(), CtlError> {
    let with_ts = ins.timestamp.is_some();
    let statements = &mut cache.by_columns[usize::from(with_ts)];
    if !statements.contains_key(ins.columns.as_slice()) {
        let cql = insert_cql(keyspace, table, &ins.columns, with_ts);
        let stmt = client.prepare(&cql).await?;
        statements.insert(ins.columns.clone(), stmt);
    }
    let stmt = statements
        .get(ins.columns.as_slice())
        .expect("inserted immediately above when absent");

    let mut values = ins.values;
    if let Some(ts) = ins.timestamp {
        // USING TIMESTAMP ? binds a bigint (8-byte big-endian) in the final slot.
        values.push(Some(ts.to_be_bytes().to_vec()));
    }
    client.execute(stmt, &values, 1).await?;
    Ok(())
}

// ── s3-clean (remove corrupt gens from the object store) ────────────────────────

/// Delete every CORRUPT generation's objects from the object store so a cold
/// restart cannot re-download and re-detect them (the S3-reseed loop). Reuses
/// the engine's S3 config (`FERROSA_S3_*` env) and the canonical object-key
/// layout. **Dry-run by default** — `apply` performs the deletes. Only corrupt
/// generations are touched; healthy ones are never deleted.
pub async fn sstable_s3_clean(table_dir: &Path, apply: bool) -> Result<(), CtlError> {
    if !table_dir.is_dir() {
        return Err(format!("not a directory: {}", table_dir.display()).into());
    }
    let (keyspace, table) = parse_keyspace_table(table_dir)?;
    let table_id = format!("{keyspace}.{table}");

    let config = ferrosa_storage::ObjectStoreConfig::from_env()
        .map_err(|e| format!("S3 config (set FERROSA_S3_* env vars): {e}"))?;
    let store = config
        .build_object_store()
        .map_err(|e| format!("build object store: {e}"))?;

    // Corrupt generations in the live dir and the quarantine subdir.
    let mut gens: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for dir in [table_dir.to_path_buf(), table_dir.join("quarantine")] {
        if !dir.is_dir() {
            continue;
        }
        for gen in StorageEngine::list_generations_in_dir(&dir) {
            if StorageEngine::smoke_test_generation(&dir, gen).is_err() {
                gens.insert(gen);
            }
        }
    }

    let mut objects = 0usize;
    for gen in &gens {
        let hex = ferrosa_storage::upload::hex_prefix_for(&gen.to_string());
        let prefix =
            object_store::path::Path::from(format!("{}/{hex}/{table_id}/{gen}", config.prefix));
        objects += clean_generation_objects(store.as_ref(), &prefix, apply).await?;
    }

    if apply {
        println!(
            "{table_id}: deleted {objects} S3 object(s) across {} corrupt generation(s) under \
             prefix '{}/' — they will not be re-downloaded on cold restart.",
            gens.len(),
            config.prefix
        );
    } else {
        println!(
            "{table_id}: DRY RUN — would delete {objects} S3 object(s) across {} corrupt \
             generation(s) under '{}/.../{table_id}/'. Re-run with --apply.",
            gens.len(),
            config.prefix
        );
    }
    Ok(())
}

/// Count, and with `apply` delete, every object under one generation's prefix.
///
/// Streams: the listing feeds `delete_stream` directly (object_store's own
/// list-then-delete idiom, which batches deletes on S3), so no key list is
/// held whatever the generation's object count. Prefixes match by path
/// segment, so generation `12` never reaches `123`. The first list or delete
/// error stops the clean and is returned with the prefix; the count says how
/// far it got only on success.
async fn clean_generation_objects(
    store: &dyn object_store::ObjectStore,
    prefix: &object_store::path::Path,
    apply: bool,
) -> Result<usize, CtlError> {
    use futures::{StreamExt as _, TryStreamExt as _};
    let listed = store.list(Some(prefix));
    let count = if apply {
        let locations = listed.map_ok(|meta| meta.location).boxed();
        store
            .delete_stream(locations)
            .try_fold(0usize, |n, _| async move { Ok(n + 1) })
            .await
            .map_err(|e| format!("clean under {prefix}: {e}"))?
    } else {
        listed
            .try_fold(0usize, |n, _| async move { Ok(n + 1) })
            .await
            .map_err(|e| format!("list under {prefix}: {e}"))?
    };
    Ok(count)
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

/// A component whose absence is a designed state: `Ok(None)` only for
/// `NotFound`; every other I/O error is returned with the path.
fn read_optional_component(path: &Path) -> Result<Option<Vec<u8>>, CtlError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("{}: {e}", path.display()).into()),
    }
}

/// Open a BTI reader for `gen` in `dir`, mirroring `ferrosa-sstable-dump`.
fn open_reader(dir: &Path, gen: u64) -> Result<SSTableReader<FileReadAt>, CtlError> {
    let gdir = resolve_gen_dir(dir, gen)
        .ok_or_else(|| format!("generation {gen}: no Data.db under {}", dir.display()))?;
    let comp = |name: &str| gdir.join(format!("{gen}-{name}"));
    let data = FileReadAt::open(comp("Data.db"))?;
    let partitions = FileReadAt::open(comp("Partitions.db"))?;
    let rows = FileReadAt::open(comp("Rows.db"))?;
    // `SSTableReader::open` takes these three as resident buffers: they are
    // the per-generation metadata the engine also keeps in memory (a bloom
    // filter sized by partition count, the stats header, the chunk offsets),
    // bounded by the generation rather than by the rows in it. Data, Partitions
    // and Rows, which do scale with the data, are read through `FileReadAt`.
    //
    // Absence is meaningful for two of them, and only absence: no
    // CompressionInfo.db means the generation is uncompressed, and a missing
    // Filter.db degrades to "no filter" with a warning. Any other read error,
    // and a missing Statistics.db, fails the open instead of handing the
    // reader an empty buffer it would misread.
    let filter = read_optional_component(&comp("Filter.db"))?.unwrap_or_else(|| {
        eprintln!("warning: generation {gen}: no Filter.db; opening without a bloom filter");
        Vec::new()
    });
    let statistics = std::fs::read(comp("Statistics.db"))
        .map_err(|e| format!("generation {gen}: Statistics.db: {e}"))?;
    let compression_info = read_optional_component(&comp("CompressionInfo.db"))?;
    let is_compressed = compression_info.is_some();
    let mut reader = SSTableReader::open(SSTableComponents {
        data,
        partitions,
        rows,
        filter,
        compression_info,
        statistics,
    })?;

    // Digest.crc32/CRC.db, when present, so `ferrosa-ctl sstable verify` and
    // friends can actually check them instead of opening a reader that never
    // had them loaded (T-012; `publication-safety.md` M1/M5). Tolerant of
    // either being absent -- an SSTable written before T-011 has neither, and
    // must keep opening.
    let digest_path = comp("Digest.crc32");
    let crc_path = comp("CRC.db");
    ferrosa_sstable::reader::load_checksums_if_present(
        &mut reader,
        &gen.to_string(),
        Some(&digest_path),
        if is_compressed { None } else { Some(&crc_path) },
    );

    Ok(reader)
}

/// Length of one component file, or `None` if absent. A stat that fails for
/// any reason other than absence is warned about by path, so an unreadable
/// component is not reported as a missing one without a trace.
fn component_len(dir: &Path, gen: u64, name: &str) -> Option<u64> {
    let path = resolve_gen_dir(dir, gen)?.join(format!("{gen}-{name}"));
    match std::fs::metadata(&path) {
        Ok(m) => Some(m.len()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            eprintln!("warning: cannot stat {}: {e}", path.display());
            None
        }
    }
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

    /// Run `find_rows`, collecting what the sink saw (tests only: the real
    /// command writes each event out as it arrives).
    fn collect_find(
        table_dir: &Path,
        pattern: &[u8],
    ) -> (FindSummary, Vec<FoundRow>, Vec<UnreadableGen>) {
        let (mut rows, mut unreadable) = (Vec::new(), Vec::new());
        let summary = find_rows(table_dir, pattern, false, &mut |event| {
            match event {
                FindEvent::Row(r) => rows.push(r),
                FindEvent::Unreadable(u) => unreadable.push(u),
            }
            Ok(())
        })
        .unwrap();
        (summary, rows, unreadable)
    }

    #[test]
    fn find_reports_the_one_row_holding_a_value_with_its_generation() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);

        let (summary, rows, unreadable) = collect_find(&table_dir, b"v1");

        assert!(unreadable.is_empty(), "{unreadable:?}");
        assert_eq!(rows.len(), 1, "only pk1 holds v1: {rows:?}");
        assert_eq!(summary.rows_matched, 1);
        assert!(
            summary.rows_scanned > summary.rows_matched,
            "the other rows were scanned and not emitted: {summary:?}"
        );
        let row = &rows[0];
        assert_eq!(row.partition_key_hex, to_hex(b"pk1"));
        assert_eq!(row.location, "live");
        assert!(
            StorageEngine::list_generations_in_dir(&table_dir).contains(&row.gen),
            "the row names a generation that exists"
        );
        assert!(
            !row.matched_in.is_empty() && !row.matched_in.contains(&"partition_key".to_string()),
            "the match is in a cell, not the key: {:?}",
            row.matched_in
        );
        assert!(
            row.cells
                .iter()
                .any(|c| c.value_hex.as_deref() == Some(to_hex(b"v1").as_str())),
            "the matching cell is rendered: {:?}",
            row.cells
        );
    }

    /// A corrupt generation is what `find` is pointed at: it is reported, and
    /// the search carries on through the healthy ones.
    #[test]
    fn find_reports_an_unreadable_generation_and_keeps_searching() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let bad = corrupt_one_generation(&table_dir);

        let (summary, rows, unreadable) = collect_find(&table_dir, b"pk");

        assert_eq!(unreadable.len(), 1, "{unreadable:?}");
        assert_eq!(summary.generations_unreadable, 1);
        assert_eq!(unreadable[0].gen, bad);
        assert!(!unreadable[0].error.is_empty());
        assert!(
            rows.iter().any(|r| r.gen != bad),
            "the healthy generation is still searched: {rows:?}"
        );
    }

    /// Losing output is not a corrupt generation: the first sink error stops
    /// the search and is returned, and nothing more is offered to the sink.
    #[test]
    fn find_stops_and_returns_the_error_when_the_sink_fails() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let mut offered = 0u32;

        let result = find_rows(&table_dir, b"pk", false, &mut |_event| {
            offered += 1;
            Err("stdout closed".into())
        });

        let err = result.expect_err("a sink failure must fail the search");
        assert_eq!(err.to_string(), "stdout closed");
        assert_eq!(offered, 1, "no event is offered after the sink failed");
    }

    /// A Filter.db that exists but cannot be read used to become an empty
    /// filter with no message. Only absence is a designed state; an I/O
    /// error must fail the open and name the path.
    #[cfg(unix)]
    #[test]
    fn open_reader_refuses_an_unreadable_filter_component() {
        use std::os::unix::fs::PermissionsExt as _;
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(1);
        let gen = StorageEngine::list_generations_in_dir(&table_dir)[0];
        let gdir = resolve_gen_dir(&table_dir, gen).expect("fixture generation");
        let filter = gdir.join(format!("{gen}-Filter.db"));
        std::fs::set_permissions(&filter, std::fs::Permissions::from_mode(0o000)).unwrap();

        let result = open_reader(&table_dir, gen);
        std::fs::set_permissions(&filter, std::fs::Permissions::from_mode(0o644)).unwrap();

        let err = result
            .err()
            .expect("an unreadable Filter.db must fail the open");
        assert!(
            err.to_string().contains("Filter.db"),
            "the error names the component: {err}"
        );
    }

    async fn put_objects(store: &dyn object_store::ObjectStore, keys: &[&str]) {
        for k in keys {
            store
                .put(&object_store::path::Path::from(*k), b"x".to_vec().into())
                .await
                .unwrap();
        }
    }

    async fn count_under(store: &dyn object_store::ObjectStore, prefix: &str) -> usize {
        use futures::TryStreamExt as _;
        store
            .list(Some(&object_store::path::Path::from(prefix)))
            .try_fold(0usize, |n, _| async move { Ok(n + 1) })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn s3_clean_dry_run_counts_one_generation_and_deletes_nothing() {
        let store = object_store::memory::InMemory::new();
        put_objects(
            &store,
            &[
                "p/ab/ks.t/12/12-Data.db",
                "p/ab/ks.t/12/12-Index.db",
                "p/ab/ks.t/123/123-Data.db",
            ],
        )
        .await;

        let n = clean_generation_objects(&store, &"p/ab/ks.t/12".into(), false)
            .await
            .unwrap();

        assert_eq!(n, 2, "only generation 12's objects, not 123's");
        assert_eq!(
            count_under(&store, "p").await,
            3,
            "a dry run deletes nothing"
        );
    }

    #[tokio::test]
    async fn s3_clean_apply_deletes_exactly_that_generation() {
        let store = object_store::memory::InMemory::new();
        put_objects(
            &store,
            &[
                "p/ab/ks.t/12/12-Data.db",
                "p/ab/ks.t/12/12-Index.db",
                "p/ab/ks.t/123/123-Data.db",
            ],
        )
        .await;

        let n = clean_generation_objects(&store, &"p/ab/ks.t/12".into(), true)
            .await
            .unwrap();

        assert_eq!(n, 2);
        assert_eq!(count_under(&store, "p/ab/ks.t/12").await, 0);
        assert_eq!(
            count_under(&store, "p/ab/ks.t/123").await,
            1,
            "a sibling generation whose number shares the prefix survives"
        );
    }

    /// Salvage streams a generation's rows through the bounded channel and
    /// reports its own result on the handle.
    #[tokio::test]
    async fn salvage_streams_reconstructed_rows_and_reports_its_result() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(2);
        let gen = StorageEngine::list_generations_in_dir(&table_dir)[0];
        let reader = open_reader(&table_dir, gen).unwrap();
        let layout = TableLayout {
            keyspace: "test_ks".into(),
            table: "test_table".into(),
            partition_key: vec!["pk".into()],
            clustering: vec!["ck".into()],
        };

        let (mut rx, worker) = spawn_generation_salvage(reader, layout);
        let mut rows = Vec::new();
        while let Some(item) = rx.recv().await {
            rows.push(item.expect("fixture rows reconstruct"));
        }
        let stats = worker
            .await
            .unwrap()
            .expect("salvage of a healthy generation");

        assert_eq!(
            rows.len() as u64,
            stats.rows_recovered,
            "every recovered row was sent"
        );
        assert_eq!(rows.len(), 1, "one row per fixture generation");
        assert_eq!(rows[0].columns, vec!["pk", "ck", "val"]);
    }

    /// A generation that cannot be opened says why in both reports, instead of
    /// reading as zero recoverable rows or a row of dashes.
    #[cfg(unix)]
    #[test]
    fn inspect_and_salvage_report_why_a_generation_did_not_open() {
        use std::os::unix::fs::PermissionsExt as _;
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(1);
        let gen = StorageEngine::list_generations_in_dir(&table_dir)[0];
        let gdir = resolve_gen_dir(&table_dir, gen).expect("fixture generation");
        let filter = gdir.join(format!("{gen}-Filter.db"));
        std::fs::set_permissions(&filter, std::fs::Permissions::from_mode(0o000)).unwrap();

        let inspected = inspect_report(&table_dir, gen);
        let salvaged = salvage_report(&table_dir, false);
        std::fs::set_permissions(&filter, std::fs::Permissions::from_mode(0o644)).unwrap();

        let open_error = inspected
            .unwrap()
            .open_error
            .expect("inspect names the open failure");
        assert!(open_error.contains("Filter.db"), "{open_error}");
        let measured = salvaged.unwrap();
        let error = measured[0]
            .error
            .as_deref()
            .expect("salvage names the open failure");
        assert!(error.contains("Filter.db"), "{error}");
    }

    /// Each row kind indexes its cells into its own column list, so a regular
    /// row's column 0 is the first REGULAR column even when the table has
    /// static columns.
    #[test]
    fn find_names_cells_from_the_column_list_of_their_own_row_kind() {
        use ferrosa_common::{CellValue, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo};
        let utf8 = "org.apache.cassandra.db.marshal.UTF8Type".to_string();
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 0,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: utf8.clone(),
            clustering_types: vec![],
            static_columns: vec![(b"s".to_vec(), utf8.clone())],
            regular_columns: vec![(b"r".to_vec(), utf8)],
        };
        let key = DecoratedKey::new(PartitionKey::new(b"k".to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hit".to_vec(), 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        };

        let key_bytes = key.key.as_bytes();
        assert!(row_matches(key_bytes, &row, b"hit"));
        assert!(!row_matches(key_bytes, &row, b"miss"));
        let regular = render_row(&header, key_bytes, &row, false, b"hit", 1, "live");
        let static_ = render_row(&header, key_bytes, &row, true, b"hit", 1, "live");

        assert_eq!(regular.cells[0].column, "r");
        assert_eq!(regular.matched_in, vec!["r".to_string()]);
        assert_eq!(static_.cells[0].column, "s");
    }

    #[test]
    fn find_pattern_accepts_a_pasted_uuid_and_refuses_bad_hex() {
        assert_eq!(
            parse_hex_pattern("0008109a-5f8f:bf d8").unwrap(),
            vec![0x00, 0x08, 0x10, 0x9a, 0x5f, 0x8f, 0xbf, 0xd8]
        );
        assert!(parse_hex_pattern("abc").is_err(), "odd digit count");
        assert!(parse_hex_pattern("zz").is_err(), "not hex");
        assert!(
            parse_hex_pattern("--").is_err(),
            "nothing left after separators"
        );
    }

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

    /// `open_reader` must load `Digest.crc32`/`CRC.db` automatically when
    /// they exist, so `ferrosa-ctl sstable verify` can actually check them
    /// (T-012; this call site previously never loaded them).
    #[test]
    fn digest_verify_open_reader_loads_digest_when_present() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(1);
        let gen = StorageEngine::list_generations_in_dir(&table_dir)[0];
        let reader = open_reader(&table_dir, gen).unwrap();
        assert!(
            reader.digest_loaded(),
            "open_reader must load Digest.crc32 when the file exists"
        );
    }

    /// An SSTable written before T-011 has no `Digest.crc32`/`CRC.db`.
    /// `open_reader` must still open it (`publication-safety.md` M1), just
    /// without reporting checksums as loaded.
    #[test]
    fn digest_verify_open_reader_tolerates_missing_digest() {
        let (_engine, _dir, table_dir) = table_dir_with_n_generations(1);
        let gen = StorageEngine::list_generations_in_dir(&table_dir)[0];
        let gdir = resolve_gen_dir(&table_dir, gen).unwrap();
        std::fs::remove_file(gdir.join(format!("{gen}-Digest.crc32"))).unwrap();
        let _ = std::fs::remove_file(gdir.join(format!("{gen}-CRC.db")));

        let reader = open_reader(&table_dir, gen)
            .expect("an old SSTable without Digest.crc32 must still open");
        assert!(
            !reader.digest_loaded(),
            "a generation with no Digest.crc32 file must not report a loaded digest"
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

    // ── reingest reconstruction tests ────────────────────────────────────────

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::PartitionKey;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    /// Encode a composite partition key the way the storage layer does:
    /// `[u16 len][value][0x00]` per component.
    fn composite_pk(components: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        for c in components {
            out.extend_from_slice(&(c.len() as u16).to_be_bytes());
            out.extend_from_slice(c);
            out.push(0x00);
        }
        out
    }

    fn entity_store_header() -> SerializationHeader {
        SerializationHeader {
            complex_collections: false,
            min_timestamp: 0,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                org.apache.cassandra.db.marshal.UUIDType,\
                org.apache.cassandra.db.marshal.UUIDType)"
                .into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.UUIDType".into()],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"entity_name".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"embedding".to_vec(),
                    "org.apache.cassandra.db.marshal.BytesType".into(),
                ),
            ],
        }
    }

    #[test]
    fn reconstruct_row_maps_composite_pk_clustering_and_regular_cells() {
        let layout = TableLayout {
            keyspace: "agent_memory".into(),
            table: "entity_store".into(),
            partition_key: vec!["tenant_id".into(), "session_id".into()],
            clustering: vec!["entity_id".into()],
        };
        let header = entity_store_header();
        // Realistic 16-byte UUIDs for the composite PK + clustering.
        let tenant = vec![0xAA; 16];
        let session = vec![0xBB; 16];
        let entity = vec![0xCC; 16];
        let key = DecoratedKey::new(PartitionKey::new(composite_pk(&[&tenant, &session])));
        let row = Row {
            clustering: entity.clone(),
            cells: vec![
                (0, CellValue::live(b"Ada".to_vec(), 1234)),
                (1, CellValue::live(b"\x01\x02\x03".to_vec(), 1240)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1234),
        };

        let ins = reconstruct_row(&layout, &header, &key, &row, false).expect("row reconstructs");
        assert_eq!(
            ins.columns,
            vec![
                "tenant_id",
                "session_id",
                "entity_id",
                "entity_name",
                "embedding"
            ]
        );
        assert_eq!(
            ins.values,
            vec![
                Some(tenant),
                Some(session),
                Some(entity),
                Some(b"Ada".to_vec()),
                Some(b"\x01\x02\x03".to_vec()),
            ]
        );
        assert_eq!(
            ins.timestamp,
            Some(1240),
            "uses the max cell timestamp (LWW)"
        );
    }

    /// The decoder indexes a clustered row's cells into `regular_columns` and a
    /// static row's into `static_columns`. Reingest named a clustered row's
    /// cell 0 as STATIC column 0 whenever the table had static columns, so
    /// `--apply` wrote a regular value into the static column.
    #[test]
    fn reconstruct_row_names_cells_from_the_column_list_of_their_own_row_kind() {
        let utf8 = "org.apache.cassandra.db.marshal.UTF8Type".to_string();
        let layout = TableLayout {
            keyspace: "ks".into(),
            table: "t".into(),
            partition_key: vec!["k".into()],
            clustering: vec![],
        };
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 0,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: utf8.clone(),
            clustering_types: vec![],
            static_columns: vec![(b"s".to_vec(), utf8.clone())],
            regular_columns: vec![(b"r".to_vec(), utf8)],
        };
        let key = DecoratedKey::new(PartitionKey::new(b"k1".to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"v".to_vec(), 7))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        };

        let regular = reconstruct_row(&layout, &header, &key, &row, false).expect("regular row");
        let static_ = reconstruct_row(&layout, &header, &key, &row, true).expect("static row");

        assert_eq!(
            regular.columns,
            vec!["k", "r"],
            "a clustered row's cell 0 is `r`"
        );
        assert_eq!(
            static_.columns,
            vec!["k", "s"],
            "a static row's cell 0 is `s`"
        );

        let misread = Row {
            cells: vec![(1, CellValue::live(b"v".to_vec(), 7))],
            ..row
        };
        assert_eq!(
            reconstruct_row(&layout, &header, &key, &misread, false),
            Err(RowSkip::UnknownColumn),
            "a cell outside the header rejects the row, it is not silently dropped"
        );
    }

    #[test]
    fn reconstruct_row_skips_tombstone_cells_and_keyonly_rows() {
        let layout = TableLayout {
            keyspace: "agent_memory".into(),
            table: "entity_store".into(),
            partition_key: vec!["tenant_id".into(), "session_id".into()],
            clustering: vec!["entity_id".into()],
        };
        let header = entity_store_header();
        let key = DecoratedKey::new(PartitionKey::new(composite_pk(&[&[0xAA; 16], &[0xBB; 16]])));
        // Only a tombstone cell (value None) → nothing to recover.
        let row = Row {
            clustering: vec![0xCC; 16],
            cells: vec![(
                0,
                CellValue {
                    value: None,
                    timestamp: 1234,
                    ttl: 0,
                    local_deletion_time: 5,
                    path: None,
                },
            )],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1234),
        };
        assert_eq!(
            reconstruct_row(&layout, &header, &key, &row, false),
            Err(RowSkip::NoLiveCells),
            "a tombstone-only row yields no insert"
        );
    }

    #[test]
    fn reconstruct_row_rejects_bad_length_key_component() {
        let layout = TableLayout {
            keyspace: "agent_memory".into(),
            table: "entity_store".into(),
            partition_key: vec!["tenant_id".into(), "session_id".into()],
            clustering: vec!["entity_id".into()],
        };
        let header = entity_store_header(); // UUID PK + UUID clustering (16 bytes each)
                                            // 4-byte components for 16-byte UUID columns → drift garbage.
        let key = DecoratedKey::new(PartitionKey::new(composite_pk(&[b"AAAA", b"BBBB"])));
        let row = Row {
            clustering: vec![0xCC; 16],
            cells: vec![(0, CellValue::live(b"Ada".to_vec(), 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        };
        assert_eq!(
            reconstruct_row(&layout, &header, &key, &row, false),
            Err(RowSkip::KeyLength),
            "a wrong-length UUID key component is rejected (never misfiled)"
        );
    }

    #[test]
    fn reconstruct_row_skips_wrong_length_fixed_cell_but_keeps_good() {
        let layout = TableLayout {
            keyspace: "agent_memory".into(),
            table: "entity_warmth".into(),
            partition_key: vec!["tenant_id".into()],
            clustering: vec![],
        };
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 0,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UUIDType".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"label".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"score".to_vec(),
                    "org.apache.cassandra.db.marshal.DoubleType".into(),
                ),
            ],
        };
        let key = DecoratedKey::new(PartitionKey::new(vec![0xAB; 16]));
        let row = Row {
            clustering: vec![],
            cells: vec![
                (0, CellValue::live(b"hot".to_vec(), 5)), // text, any length OK
                (1, CellValue::live(vec![0u8; 3], 6)),    // double needs 8 bytes → skip
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(5),
        };
        let ins = reconstruct_row(&layout, &header, &key, &row, false).expect("good cell remains");
        assert_eq!(
            ins.columns,
            vec!["tenant_id", "label"],
            "bad double cell dropped"
        );
        assert_eq!(
            ins.values,
            vec![Some(vec![0xAB; 16]), Some(b"hot".to_vec())]
        );
        assert_eq!(ins.dropped_cells, 1, "the dropped cell is counted");
    }

    #[test]
    fn pk_component_types_parses_composite_and_single() {
        assert_eq!(
            pk_component_types(
                "org.apache.cassandra.db.marshal.CompositeType(\
                 org.apache.cassandra.db.marshal.UUIDType,\
                 org.apache.cassandra.db.marshal.Int32Type)"
            ),
            vec![
                "org.apache.cassandra.db.marshal.UUIDType",
                "org.apache.cassandra.db.marshal.Int32Type"
            ]
        );
        assert_eq!(
            pk_component_types("org.apache.cassandra.db.marshal.UUIDType"),
            vec!["org.apache.cassandra.db.marshal.UUIDType"]
        );
    }

    #[test]
    fn insert_cql_quotes_columns_and_appends_timestamp_marker() {
        let cql = insert_cql(
            "agent_memory",
            "entity_store",
            &["tenant_id".into(), "entity_name".into()],
            true,
        );
        assert_eq!(
            cql,
            "INSERT INTO \"agent_memory\".\"entity_store\" (\"tenant_id\", \"entity_name\") \
             VALUES (?, ?) USING TIMESTAMP ?"
        );
        let no_ts = insert_cql("ks", "t", &["a".into()], false);
        assert_eq!(no_ts, "INSERT INTO \"ks\".\"t\" (\"a\") VALUES (?)");
    }

    #[test]
    fn parse_keyspace_table_splits_dir_name() {
        let dir = std::path::Path::new("/var/lib/ferrosa/sstables/agent_memory.entity_store");
        let (ks, t) = parse_keyspace_table(dir).unwrap();
        assert_eq!(ks, "agent_memory");
        assert_eq!(t, "entity_store");
        assert!(parse_keyspace_table(std::path::Path::new("/x/nodotdir")).is_err());
    }

    #[test]
    fn parse_layout_from_result_orders_keys_by_position() {
        use ferrosa_cql::client::{QueryResult, ResultRow};
        let text = |s: &str| Some(s.as_bytes().to_vec());
        let int = |n: i32| Some(n.to_be_bytes().to_vec());
        // Rows deliberately out of position order; regular column included.
        let res = QueryResult {
            column_names: vec!["column_name".into(), "kind".into(), "position".into()],
            rows: vec![
                ResultRow {
                    columns: vec![text("session_id"), text("partition_key"), int(1)],
                },
                ResultRow {
                    columns: vec![text("entity_name"), text("regular"), int(-1)],
                },
                ResultRow {
                    columns: vec![text("tenant_id"), text("partition_key"), int(0)],
                },
                ResultRow {
                    columns: vec![text("entity_id"), text("clustering"), int(0)],
                },
            ],
            paging_state: None,
        };
        let layout = parse_layout_from_result("agent_memory", "entity_store", &res).unwrap();
        assert_eq!(
            layout.partition_key,
            vec!["tenant_id", "session_id"],
            "PK ordered by position"
        );
        assert_eq!(layout.clustering, vec!["entity_id"]);
    }

    #[test]
    fn parse_layout_from_result_errors_without_partition_key() {
        use ferrosa_cql::client::{QueryResult, ResultRow};
        let res = QueryResult {
            column_names: vec!["column_name".into(), "kind".into(), "position".into()],
            rows: vec![ResultRow {
                columns: vec![
                    Some(b"c".to_vec()),
                    Some(b"regular".to_vec()),
                    Some((-1i32).to_be_bytes().to_vec()),
                ],
            }],
            paging_state: None,
        };
        assert!(parse_layout_from_result("ks", "t", &res).is_err());
    }
}
