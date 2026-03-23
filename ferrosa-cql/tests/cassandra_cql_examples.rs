//! CQL language completeness test.
//!
//! Parses every `.cql` example file from the Cassandra documentation
//! (`cassandra/doc/modules/cassandra/examples/CQL/`) and reports which
//! statements parse successfully vs. which fail. This ensures ferrosa's
//! CQL parser stays aligned with the official Cassandra CQL grammar.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Find the Cassandra CQL examples directory relative to the workspace root.
fn cql_examples_dir() -> PathBuf {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    Path::new(&manifest)
        .parent()
        .unwrap()
        .join("cassandra/doc/modules/cassandra/examples/CQL")
}

/// cqlsh-only commands that are NOT part of the CQL language spec.
const CQLSH_ONLY: &[&str] = &[
    "SOURCE",
    "CAPTURE",
    "DESCRIBE",
    "COPY",
    "SHOW",
    "TRACING",
    "EXPAND",
    "PAGING",
    "SERIAL",
    "CONSISTENCY",
    "LOGIN",
];

/// Returns true if this statement is a cqlsh-specific command, not CQL.
fn is_cqlsh_command(stmt: &str) -> bool {
    let first_word = stmt.split_whitespace().next().unwrap_or("");
    CQLSH_ONLY
        .iter()
        .any(|cmd| first_word.eq_ignore_ascii_case(cmd))
}

/// Returns true if this looks like non-CQL code (Java UDF bodies, etc).
fn is_non_cql(stmt: &str) -> bool {
    let trimmed = stmt.trim();
    // Java/JS UDF body fragments
    trimmed.starts_with("$$")
        || trimmed.starts_with("return")
        || trimmed.starts_with("if ")
        || trimmed.starts_with("state.")
        || trimmed.starts_with("udt.")
        || trimmed.starts_with("r =")
        || trimmed.starts_with("}")
        || trimmed.starts_with("*")
        || trimmed.starts_with("min(")
        // APPLY BATCH is the closing keyword, not a standalone statement
        || trimmed.eq_ignore_ascii_case("APPLY BATCH")
}

/// Split a `.cql` file into individual statements (separated by `;`).
/// Strips comments and blank lines.
fn split_statements(content: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut current = String::new();

    for line in content.lines() {
        let trimmed = line.trim();
        // Skip line comments
        if trimmed.starts_with("//") || trimmed.starts_with("--") {
            continue;
        }
        // Strip inline comments
        let line = if let Some(pos) = line.find("//") {
            &line[..pos]
        } else {
            line
        };
        current.push_str(line);
        current.push('\n');

        if line.trim_end().ends_with(';') {
            let stmt = current.trim().trim_end_matches(';').trim().to_string();
            if !stmt.is_empty() {
                stmts.push(stmt);
            }
            current.clear();
        }
    }
    // Handle trailing statement without semicolon
    let remaining = current.trim().trim_end_matches(';').trim().to_string();
    if !remaining.is_empty() {
        stmts.push(remaining);
    }
    stmts
}

#[test]
fn parse_cassandra_cql_examples() {
    let dir = cql_examples_dir();
    if !dir.exists() {
        eprintln!(
            "Skipping CQL doc test: cassandra submodule not checked out at {:?}",
            dir
        );
        return;
    }

    let mut results: BTreeMap<String, Vec<(String, Result<(), String>)>> = BTreeMap::new();
    let mut total = 0usize;
    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;

    // Collect all .cql files recursively
    let mut cql_files: Vec<PathBuf> = Vec::new();
    collect_cql_files(&dir, &mut cql_files);
    cql_files.sort();

    for path in &cql_files {
        let rel = path
            .strip_prefix(&dir)
            .unwrap()
            .to_string_lossy()
            .to_string();
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                results
                    .entry(rel.clone())
                    .or_default()
                    .push(("(read error)".into(), Err(e.to_string())));
                failed += 1;
                total += 1;
                continue;
            }
        };

        let stmts = split_statements(&content);
        let file_results = results.entry(rel.clone()).or_default();

        for stmt_text in &stmts {
            // Skip cqlsh commands and non-CQL code fragments
            if is_cqlsh_command(stmt_text) || is_non_cql(stmt_text) {
                skipped += 1;
                continue;
            }
            total += 1;
            match ferrosa_cql::parser::parse(stmt_text) {
                Ok(_) => {
                    passed += 1;
                    file_results.push((stmt_text.clone(), Ok(())));
                }
                Err(e) => {
                    failed += 1;
                    file_results.push((stmt_text.clone(), Err(format!("{e}"))));
                }
            }
        }
    }

    // Print summary
    eprintln!("\n=== CQL Doc Examples Parse Results ===");
    eprintln!("Files:      {}", cql_files.len());
    eprintln!("CQL stmts:  {total}");
    eprintln!("Parsed OK:  {passed}");
    eprintln!("Failed:     {failed}");
    eprintln!("Skipped:    {skipped} (cqlsh commands, UDF bodies)");
    eprintln!(
        "Coverage:   {:.1}%",
        if total > 0 {
            passed as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    );

    if failed > 0 {
        eprintln!("\n--- Failures ---");
        for (file, file_results) in &results {
            let failures: Vec<_> = file_results.iter().filter(|(_, r)| r.is_err()).collect();
            if !failures.is_empty() {
                eprintln!("\n  {file}:");
                for (stmt, err) in failures {
                    let preview = if stmt.len() > 80 {
                        format!("{}...", &stmt[..80])
                    } else {
                        stmt.clone()
                    };
                    eprintln!("    FAIL: {preview}");
                    eprintln!("          {}", err.as_ref().unwrap_err());
                }
            }
        }
    }

    // This test is informational — it reports coverage without failing.
    // Uncomment the assertion below to make it enforcing:
    // assert_eq!(failed, 0, "{failed} CQL statements failed to parse");
}

fn collect_cql_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_cql_files(&path, out);
            } else if path.extension().map(|e| e == "cql").unwrap_or(false) {
                out.push(path);
            }
        }
    }
}
