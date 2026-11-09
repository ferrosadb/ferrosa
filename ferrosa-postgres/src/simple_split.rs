//! Module: split a simple-query string into its statements.
//!
//! A simple `Query` message may carry several statements separated by `;`. A `;`
//! inside a string (`'…'`, with `''` escapes), a quoted identifier (`"…"`), a
//! dollar-quoted body (`$tag$…$tag$`, which holds whole PL/pgSQL functions), a
//! `-- line` comment or a `/* block */` comment does not separate statements.
//! Empty statements (`;;`, a trailing `;`) are dropped, as PostgreSQL ignores them.
//! `Sql/SimpleSplit.lean` models the scanner: rejoining the pieces gives the input
//! back, and no piece is cut inside a quoted region.
//!
//! Last revised: 2026-11-08

/// The statements of `sql`, in order, each trimmed; empty ones dropped.
pub(crate) fn split_statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let (mut start, mut i) = (0usize, 0usize);
    while i < bytes.len() {
        i = match bytes[i] {
            b'\'' => skip_quoted(bytes, i, b'\''),
            b'"' => skip_quoted(bytes, i, b'"'),
            b'-' if bytes.get(i + 1) == Some(&b'-') => skip_line_comment(bytes, i),
            b'/' if bytes.get(i + 1) == Some(&b'*') => skip_block_comment(bytes, i),
            b'$' => skip_dollar(sql, i),
            b';' => {
                push(&mut out, &sql[start..i]);
                start = i + 1;
                i + 1
            }
            _ => i + 1,
        };
    }
    push(&mut out, &sql[start..]);
    out
}

fn push<'a>(out: &mut Vec<&'a str>, piece: &'a str) {
    let piece = piece.trim();
    if !piece.is_empty() {
        out.push(piece);
    }
}

/// Past the closing quote; a doubled quote is an escape. Unterminated: to the end
/// (the parser then reports the error for that one statement).
fn skip_quoted(bytes: &[u8], open: usize, quote: u8) -> usize {
    let mut i = open + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_line_comment(bytes: &[u8], start: usize) -> usize {
    bytes[start..]
        .iter()
        .position(|&b| b == b'\n')
        .map_or(bytes.len(), |p| start + p + 1)
}

fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    bytes[start + 2..]
        .windows(2)
        .position(|w| w == b"*/")
        .map_or(bytes.len(), |p| start + 2 + p + 2)
}

/// `$tag$ … $tag$` (tag may be empty); a `$` that does not open a tag (`$1`) is
/// ordinary text.
fn skip_dollar(sql: &str, start: usize) -> usize {
    let rest = &sql[start + 1..];
    let Some(end) = rest.find('$') else {
        return start + 1;
    };
    let tag = &rest[..end];
    if !tag.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        || tag.starts_with(|c: char| c.is_ascii_digit())
    {
        return start + 1;
    }
    let delim = &sql[start..start + end + 2];
    let body_start = start + delim.len();
    sql[body_start..]
        .find(delim)
        .map_or(sql.len(), |p| body_start + p + delim.len())
}

#[cfg(test)]
mod tests {
    use super::split_statements;

    #[test]
    fn splits_at_top_level_semicolons_only() {
        assert_eq!(
            split_statements("SELECT 1; SELECT 2"),
            ["SELECT 1", "SELECT 2"]
        );
        assert_eq!(
            split_statements("SELECT ';'; SELECT \"a;b\""),
            ["SELECT ';'", "SELECT \"a;b\""]
        );
        assert_eq!(
            split_statements("SELECT 'it''s; ok'"),
            ["SELECT 'it''s; ok'"]
        );
        assert_eq!(
            split_statements("SELECT 1 -- a; b\n; SELECT 2"),
            ["SELECT 1 -- a; b", "SELECT 2"]
        );
        assert_eq!(split_statements("SELECT /* ; */ 1"), ["SELECT /* ; */ 1"]);
        assert_eq!(
            split_statements("SELECT $1; SELECT 2"),
            ["SELECT $1", "SELECT 2"]
        );
    }

    #[test]
    fn a_dollar_quoted_body_is_one_statement() {
        let census = "DO $$ BEGIN IF true THEN PERFORM 1; END IF; END$$;;";
        assert_eq!(
            split_statements(census),
            ["DO $$ BEGIN IF true THEN PERFORM 1; END IF; END$$"]
        );
        let f = "CREATE FUNCTION f() RETURNS int AS $body$ BEGIN RETURN 1; END; $body$ LANGUAGE plpgsql";
        assert_eq!(split_statements(f), [f]);
    }

    #[test]
    fn empty_statements_are_dropped() {
        assert_eq!(split_statements(";; SELECT 1 ;"), ["SELECT 1"]);
        assert!(split_statements("  ").is_empty());
    }
}
