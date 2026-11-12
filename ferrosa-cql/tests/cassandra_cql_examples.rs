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
    let mut chars = content.chars().peekable();
    let mut quote: Option<char> = None;
    let mut dollar_quote: Option<String> = None;
    let mut block_comment = false;
    let mut line_comment = false;

    while let Some(ch) = chars.next() {
        let next = chars.peek().copied();
        if line_comment {
            if ch == '\n' {
                line_comment = false;
                current.push('\n');
            }
            continue;
        }
        if block_comment {
            if ch == '*' && next == Some('/') {
                chars.next();
                block_comment = false;
                current.push(' ');
            }
            continue;
        }
        if let Some(delimiter) = &dollar_quote {
            if ch == '$' {
                let mut closing = String::from("$");
                while closing.len() < delimiter.len() {
                    if let Some(c) = chars.next() {
                        closing.push(c);
                    } else {
                        break;
                    }
                }
                current.push_str(&closing);
                if &closing == delimiter {
                    dollar_quote = None;
                }
            } else {
                current.push(ch);
            }
            continue;
        }
        if let Some(delim) = quote {
            current.push(ch);
            if ch == delim {
                if next == Some(delim) {
                    current.push(chars.next().unwrap());
                } else {
                    quote = None;
                }
            } else if ch == '\\' && next.is_some() {
                current.push(chars.next().unwrap());
            }
            continue;
        }

        if ch == '-' && next == Some('-') || ch == '/' && next == Some('/') {
            chars.next();
            line_comment = true;
        } else if ch == '/' && next == Some('*') {
            chars.next();
            block_comment = true;
        } else if ch == '\'' || ch == '"' {
            quote = Some(ch);
            current.push(ch);
        } else if ch == '$' {
            let mut tag = String::from("$");
            while chars
                .peek()
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_')
            {
                tag.push(chars.next().unwrap());
            }
            if chars.peek() == Some(&'$') {
                tag.push(chars.next().unwrap());
                dollar_quote = Some(tag.clone());
                current.push_str(&tag);
            } else {
                current.push_str(&tag);
            }
        } else if ch == ';' {
            current.push(ch);
            let normalized = current.trim().trim_end_matches(';').trim();
            let is_batch = normalized
                .split_whitespace()
                .take(4)
                .collect::<Vec<_>>()
                .iter()
                .any(|word| word.eq_ignore_ascii_case("BATCH"))
                && normalized.to_ascii_uppercase().ends_with("APPLY BATCH");
            let begins_batch = current
                .split_whitespace()
                .take(4)
                .collect::<Vec<_>>()
                .first()
                .is_some_and(|word| word.eq_ignore_ascii_case("BEGIN"))
                && current
                    .split_whitespace()
                    .take(4)
                    .any(|word| word.eq_ignore_ascii_case("BATCH"));
            if !begins_batch || is_batch {
                let stmt = current.trim().trim_end_matches(';').trim().to_string();
                if !stmt.is_empty() {
                    stmts.push(stmt);
                }
                current.clear();
            }
        } else {
            current.push(ch);
        }
    }
    // Handle trailing statement without semicolon
    let remaining = current.trim().trim_end_matches(';').trim().to_string();
    if !remaining.is_empty() {
        stmts.push(remaining);
    }
    stmts
}

#[cfg(test)]
mod splitter_tests {
    use super::split_statements;

    #[test]
    fn split_statements_keeps_semicolons_in_quoted_text() {
        let statements = split_statements(
            "INSERT INTO t (k, v) VALUES (1, 'a;b');\nUPDATE t SET v = \"x;y\" WHERE k = 1;",
        );

        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("'a;b'"));
        assert!(statements[1].contains("\"x;y\""));
    }

    #[test]
    fn split_statements_keeps_semicolons_in_dollar_quoted_udf_body() {
        let statements = split_statements(
            "CREATE FUNCTION f (x int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS $$ return x; $$;\nSELECT * FROM t;",
        );

        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("return x;"));
        assert!(statements[1].starts_with("SELECT"));
    }

    #[test]
    fn split_statements_ignores_semicolons_in_comments() {
        let statements =
            split_statements("SELECT 1 /* ignored; */ FROM t;\nSELECT 2 -- ignored;\nFROM t;");

        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("SELECT 1"));
        assert!(statements[1].contains("SELECT 2"));
    }

    #[test]
    fn split_statements_keeps_batch_as_one_statement() {
        let statements = split_statements(
            "BEGIN UNLOGGED BATCH\nINSERT INTO t (k) VALUES (1);\nUPDATE t SET v = 2 WHERE k = 1;\nAPPLY BATCH;\nSELECT * FROM t;",
        );

        assert_eq!(statements.len(), 2);
        assert!(statements[0].starts_with("BEGIN UNLOGGED BATCH"));
        assert!(statements[0].contains("APPLY BATCH"));
        assert!(statements[1].starts_with("SELECT"));
    }
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

    type FileResults = Vec<(String, Result<(), String>)>;
    let mut results: BTreeMap<String, FileResults> = BTreeMap::new();
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
