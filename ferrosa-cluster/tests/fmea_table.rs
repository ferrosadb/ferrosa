//! Guards on `specs/fmea.md`, the crate's record of what has gone wrong in this
//! cluster and what was done about it.
//!
//! They exist because of a measured failure, not a hypothetical one. `CL-73`
//! recorded the DDL-proposal bound (`t_e8abe81a`); a merge of two branches that
//! had each independently chosen the id `CL-73` for their own defect replaced the
//! row IN PLACE, and came back recording the read-set granularity instead. The id
//! survived, the defect under it did not, and nothing in the repo - no test, no
//! script, no hook - parsed this table, so a fixed defect's record was deleted in
//! silence and the crate README went on citing `CL-73` for the other defect.
//!
//! Three invariants, one test each:
//!
//! 1. An id names exactly ONE defect. A second row under an id that already has
//!    one is how a collision arrives; the known pre-existing collisions are
//!    listed in [`KNOWN_COLLISIONS`] and a NEW one is a failure.
//! 2. The id -> defect mapping in `specs/fmea-ids.txt` still holds: every id it
//!    registers is still in the table naming the same defect, and every row in the
//!    table is registered. Renumbering a row, replacing one, or adding one is
//!    allowed - it just cannot be silent. Regenerate in the same commit:
//!
//!    ```text
//!    FERROSA_FMEA_BASELINE=write cargo test -p ferrosa-cluster --test fmea_table
//!    ```
//!
//! 3. Every `CL-<n>` cited from this crate's documentation resolves to a row, so
//!    a deletion cannot leave a reader following a reference to nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// How much of a row's Failure-mode cell identifies WHICH defect it is. Long
/// enough that a rewrite of the residual further down the row does not trip it,
/// short enough that a different defect cannot match.
const SUBJECT_CHARS: usize = 120;

/// Ids that already name more than one defect in `specs/fmea.md`, with where they
/// are. They are debt, not licence: they predate this guard and are NOT fixed here
/// because resolving each one means deciding which row is canonical, which is a
/// judgement about the incidents themselves. A NEW collision fails, and an id here
/// that stops colliding must be deleted from this list (the test says so).
const KNOWN_COLLISIONS: &[(&str,
    // two colliding rows in the main table
    &str,
    // and the second row that shares it, if there is one
    &str,
)] = &[
    ("CL-17", "line 44 (keyed secondary-index reads)", "line 61 (unbounded single-partition scans)"),
    ("CL-18", "line 41 (replicated CREATE INDEX never built)", "line 42 (BYTE-IDENTICAL duplicate of line 41) and line 63"),
    ("CL-19", "line 40 (cold start did not reconstruct system_auth.roles)", "line 62 (unguarded merge source stalled the scan)"),
    ("CL-22", "line 33 (formation dialled peers at the receiver's port)", "lines 35 and 43 (snapshot install inferred drops; peer pooled under a bad identity)"),
    ("CL-23", "line 34 (QUORUM range scan read only the first remote)", "line 38 (doomed transaction held keys for the full RPC timeout)"),
    ("CL-24", "line 32 (non-replica coordinator voted on its row)", "line 36 (SAME defect text as CL-22 line 33, under a different id)"),
    ("CL-32", "line 143 (RowWire could not carry a complex cell path)", "line 148 (byte-identical duplicate of line 143)"),
    ("CL-46", "line 18 (F+1 read-vote agreement compared raw bytes)", "line 151 (schema replay said retry later and never retried)"),
    ("CL-47", "line 19 (refused DropTable reported as success)", "line 152 (Accord apply skew gauge read 1000x high)"),
    ("CL-48", "line 153 (empty-body AccordApplyOK satisfied the quorum)", "line 154 (TRUNCATE applied node-locally)"),
    ("CL-61", "line 13 (consensus start gated on boot-time object-store work)", "line 164 (apply-path idempotency structures never pruned)"),
];

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn fmea_path() -> PathBuf {
    crate_dir().join("specs/fmea.md")
}

fn baseline_path() -> PathBuf {
    crate_dir().join("specs/fmea-ids.txt")
}

struct Row {
    id: String,
    subject: String,
    line: usize,
}

fn is_fmea_id(candidate: &str) -> bool {
    candidate
        .strip_prefix("CL-")
        .is_some_and(|digits| !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()))
}

/// Every row of the table, wherever it sits in the file. Deliberately agnostic
/// about sections: the collision this guards against was two branches editing the
/// same table, and `specs/fmea.md` has grown a second block of rows further down.
fn parse_rows(text: &str) -> Vec<Row> {
    let mut rows = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if !line.starts_with("| CL-") {
            continue;
        }
        let mut cells = line.split('|');
        cells.next();
        let Some(id_cell) = cells.next() else { continue };
        let Some(id) = id_cell.split_whitespace().next() else {
            continue;
        };
        if !is_fmea_id(id) {
            continue;
        }
        rows.push(Row {
            id: id.to_string(),
            subject: cells.next().unwrap_or("").trim().to_string(),
            line: index + 1,
        });
    }
    assert!(
        rows.len() >= 70,
        "parsed only {} FMEA rows out of {}: the table's shape changed and these guards are \
         now measuring nothing",
        rows.len(),
        fmea_path().display()
    );
    rows
}

fn normalize(text: &str) -> String {
    let stripped: String = text.chars().filter(|c| !matches!(c, '*' | '`' | '_')).collect();
    stripped.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase()
}

/// The part of a row that says WHICH defect it is. Stable across a residual being
/// re-measured or a mitigation being rewritten further along the row; different the
/// moment the row is about something else.
fn subject_of(subject: &str) -> String {
    normalize(subject).chars().take(SUBJECT_CHARS).collect()
}

/// FNV-1a. A local hash so the baseline stays byte-identical across toolchains and
/// Rust versions, unlike `DefaultHasher`.
fn fingerprint(text: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn subjects_by_id(rows: &[Row]) -> BTreeMap<String, String> {
    let mut by_id: BTreeMap<String, Vec<&Row>> = BTreeMap::new();
    for row in rows {
        by_id.entry(row.id.clone()).or_default().push(row);
    }
    by_id
        .into_iter()
        .map(|(id, mut group)| {
            // A colliding id registers both rows, sorted, so a baseline entry cannot
            // depend on row order.
            let mut subjects: Vec<String> = group.drain(..).map(|row| subject_of(&row.subject)).collect();
            subjects.sort();
            (id, fingerprint(&subjects.join("\n")))
        })
        .collect()
}

fn read_baseline(text: &str) -> BTreeMap<String, (String, String)> {
    let mut entries = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.splitn(3, ' ');
        let (Some(id), Some(fp)) = (fields.next(), fields.next()) else {
            panic!("specs/fmea-ids.txt line is not `<id> <fingerprint> <subject>`: {line:?}");
        };
        entries.insert(id.to_string(), (fp.to_string(), fields.next().unwrap_or("").to_string()));
    }
    assert!(
        entries.len() >= 70,
        "specs/fmea-ids.txt registers only {} ids: an emptier baseline checks less, so this is a \
         failure and not a pass; regenerate it deliberately",
        entries.len()
    );
    entries
}

fn subject_preview(subject: &str) -> String {
    let normalized = normalize(subject);
    let preview: String = normalized.chars().take(60).collect();
    if normalized.chars().count() > 60 {
        format!("{preview}...")
    } else {
        preview
    }
}

/// Writes the baseline when asked to, so regenerating it is a command rather than
/// a hand-edit of hashes.
fn maybe_write_baseline(rows: &[Row]) {
    if std::env::var("FERROSA_FMEA_BASELINE").as_deref() != Ok("write") {
        return;
    }
    let subjects = subjects_by_id(rows);
    let previews: BTreeMap<String, String> = rows
        .iter()
        .map(|row| (row.id.clone(), subject_preview(&row.subject)))
        .collect();
    let mut text = String::from(
        "# Register of the defect each FMEA id names.\n\
         # <id> <fingerprint of the id's Failure-mode subject> <the subject, for a reader>\n\
         # Checked by ferrosa-cluster/tests/fmea_table.rs. Regenerate only deliberately, in the\n\
         # same commit as the renumbering or re-wording that made it stale:\n\
         #   FERROSA_FMEA_BASELINE=write cargo test -p ferrosa-cluster --test fmea_table\n",
    );
    for (id, fp) in subjects {
        text.push_str(&format!("{id} {fp} {}\n", previews.get(&id).cloned().unwrap_or_default()));
    }
    std::fs::write(baseline_path(), text).expect("write the baseline");
}

fn table_rows() -> (String, Vec<Row>) {
    let text = std::fs::read_to_string(fmea_path()).expect("read specs/fmea.md");
    let rows = parse_rows(&text);
    (text, rows)
}

/// Invariant 1: an id names exactly one defect.
#[test]
fn every_fmea_id_names_exactly_one_defect() {
    let (_, rows) = table_rows();
    let mut by_id: BTreeMap<&str, Vec<&Row>> = BTreeMap::new();
    for row in &rows {
        by_id.entry(row.id.as_str()).or_default().push(row);
    }

    let mut new_collisions = Vec::new();
    for (id, group) in &by_id {
        if group.len() < 2 {
            continue;
        }
        if !KNOWN_COLLISIONS.iter().any(|(known, _, _)| known == id) {
            new_collisions.push(format!(
                "  {id} is on lines {}: {}",
                group.iter().map(|row| row.line.to_string()).collect::<Vec<_>>().join(" and "),
                group
                    .iter()
                    .map(|row| format!("line {} = {:?}", row.line, subject_preview(&row.subject)))
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
        }
    }
    assert!(
        new_collisions.is_empty(),
        "specs/fmea.md now gives one id to more than one defect. Two branches that each pick the \
         same next id produce exactly this, and whichever row a later merge keeps, the other \
         defect's record is gone - that is how CL-73 lost the DDL-proposal bound. Give one of \
         them a free id instead:\n{}",
        new_collisions.join("\n")
    );

    // An id that has stopped colliding is a fix: the list must not rot.
    let stale: Vec<&str> = KNOWN_COLLISIONS
        .iter()
        .filter(|(id, _, _)| by_id.get(id).map(|group| group.len()).unwrap_or(0) < 2)
        .map(|(id, _, _)| *id)
        .collect();
    assert!(
        stale.is_empty(),
        "these ids no longer name more than one defect, so their KNOWN_COLLISIONS entries are \
         stale: {stale:?}. Delete them from the list in this file - an exemption that outlives \
         its defect hides the next collision under the same id."
    );
}

/// Invariant 2: the id -> defect mapping is unchanged, and complete.
#[test]
fn fmea_rows_keep_the_defect_their_id_recorded() {
    let (_, rows) = table_rows();
    maybe_write_baseline(&rows);
    let registered = read_baseline(
        &std::fs::read_to_string(baseline_path()).expect("read specs/fmea-ids.txt"),
    );
    let current = subjects_by_id(&rows);
    let previews: BTreeMap<&str, &str> = rows
        .iter()
        .map(|row| (row.id.as_str(), row.subject.as_str()))
        .collect();

    let mut problems = Vec::new();
    for (id, (baseline_fp, baseline_subject)) in &registered {
        match current.get(id) {
            None => problems.push(format!(
                "  {id} is registered as {baseline_subject:?} and is no longer in the table: a \
                 defect's record was removed or renumbered. Restore the row, or - if the \
                 renumbering is intended - regenerate the baseline in the same commit."
            )),
            Some(fp) if fp != baseline_fp => problems.push(format!(
                "  {id} now names a different defect. Registered: {baseline_subject:?}. Now: {:?}.",
                previews.get(id.as_str()).map(|s| subject_preview(s)).unwrap_or_default()
            )),
            Some(_) => {}
        }
    }
    for id in current.keys() {
        if !registered.contains_key(id) {
            problems.push(format!(
                "  {id} is in the table and not registered. Register it: \
                 FERROSA_FMEA_BASELINE=write cargo test -p ferrosa-cluster --test fmea_table"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "the FMEA table and specs/fmea-ids.txt disagree. This is the guard that would have caught \
         a merge replacing the CL-73 row:\n{}",
        problems.join("\n")
    );
}

/// Invariant 3: a citation resolves.
#[test]
fn fmea_references_resolve_to_a_row() {
    let (_, rows) = table_rows();
    let known: BTreeSet<&str> = rows.iter().map(|row| row.id.as_str()).collect();

    let mut docs = vec![crate_dir().join("README.md")];
    let specs = crate_dir().join("specs");
    for entry in std::fs::read_dir(&specs).expect("list specs/") {
        let path = entry.expect("specs/ entry").path();
        if path.extension().is_some_and(|ext| ext == "md") {
            docs.push(path);
        }
    }

    let mut citations = 0usize;
    let mut dangling = Vec::new();
    for path in docs {
        let text = std::fs::read_to_string(&path).expect("read a crate doc");
        for (index, line) in text.lines().enumerate() {
            for token in line.split(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '_')) {
                if !is_fmea_id(token) {
                    continue;
                }
                citations += 1;
                if !known.contains(token) {
                    dangling.push(format!("  {}:{} cites {token}, which is not a row of specs/fmea.md", path
                        .strip_prefix(crate_dir())
                        .unwrap_or(Path::new("?"))
                        .display(), index + 1));
                }
            }
        }
    }
    assert!(
        citations >= 40,
        "found only {citations} FMEA citations across this crate's docs: the scan is not reading \
         them and its silence means nothing"
    );
    assert!(
        dangling.is_empty(),
        "a crate doc cites an FMEA id that no longer exists:\n{}",
        dangling.join("\n")
    );
}
