//! Module: Bound bracket nesting in SPARQL text before `spargebra` parses it.
//! Correctness: Correct when every query or update whose `(`/`{`/`[` nesting
//!   exceeds [`MAX_NESTING`] is refused before parsing, and no bracket inside a
//!   string literal, IRI or comment is counted, so text the parser skips can
//!   neither hide real nesting nor cause a false refusal.
//! Last revised: 2026-10-03
//! Last changed: Created. `spargebra`'s parser recurses per nesting level with
//!   no limit; 100,000 nested `(` overflowed a 2 MiB worker stack and aborted
//!   the process.
//!
//! The scan follows the SPARQL lexical rules that decide what the parser
//! ignores: string literals (`"`, `'`, and their long `"""`/`'''` forms, with
//! backslash escapes), IRI references (`<` followed by IRI characters up to
//! `>`; otherwise `<` is the less-than operator), and `#` comments to the end
//! of the line. A closing bracket inside any of those must not lower the
//! count, or a crafted literal could hide real nesting that follows it.

use crate::error::SparqlError;

/// Deepest bracket nesting a query or update may use. Real queries nest a
/// handful of levels; the parser spends several stack frames per level.
pub const MAX_NESTING: usize = 64;

/// Refuse `text` if its bracket nesting exceeds [`MAX_NESTING`].
///
/// # Errors
///
/// [`SparqlError::Parse`] naming the limit.
pub fn check_nesting(text: &str) -> Result<(), SparqlError> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' | b'{' | b'[' => {
                depth += 1;
                if depth > MAX_NESTING {
                    return Err(SparqlError::Parse(format!(
                        "query nests brackets deeper than {MAX_NESTING} levels"
                    )));
                }
                i += 1;
            }
            b')' | b'}' | b']' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            b'"' | b'\'' => i = skip_string(bytes, i),
            b'#' => i = skip_comment(bytes, i),
            b'<' => i = skip_iri(bytes, i),
            _ => i += 1,
        }
    }
    Ok(())
}

/// Index just past the string literal opening at `start`. An unterminated
/// literal runs to the end; the parser rejects it.
fn skip_string(bytes: &[u8], start: usize) -> usize {
    let quote = bytes[start];
    let long = bytes.get(start + 1) == Some(&quote) && bytes.get(start + 2) == Some(&quote);
    let mut i = if long { start + 3 } else { start + 1 };
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b if b == quote && !long => return i + 1,
            b if b == quote
                && bytes.get(i + 1) == Some(&quote)
                && bytes.get(i + 2) == Some(&quote) =>
            {
                return i + 3
            }
            // A short literal cannot span lines.
            b'\n' | b'\r' if !long => return i,
            _ => i += 1,
        }
    }
    bytes.len()
}

fn skip_comment(bytes: &[u8], start: usize) -> usize {
    bytes[start..]
        .iter()
        .position(|&b| b == b'\n' || b == b'\r')
        .map_or(bytes.len(), |n| start + n)
}

/// `<` starts an IRI reference only when IRI characters run to a `>`;
/// otherwise it is the less-than operator and only `<` is consumed.
fn skip_iri(bytes: &[u8], start: usize) -> usize {
    for (offset, &b) in bytes[start + 1..].iter().enumerate() {
        match b {
            b'>' => return start + 1 + offset + 1,
            b'<' | b'"' | b'{' | b'}' | b'|' | b'^' | b'`' | b'\\' => break,
            b if b <= 0x20 => break,
            _ => {}
        }
    }
    start + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(open: &str, close: &str, depth: usize) -> String {
        format!(
            "SELECT * WHERE {{ ?s ?p ?o FILTER({}1{}) }}",
            open.repeat(depth),
            close.repeat(depth)
        )
    }

    #[test]
    fn nesting_at_the_limit_is_accepted_and_beyond_it_refused() {
        // The WHERE group and FILTER add two levels.
        assert!(check_nesting(&nested("(", ")", MAX_NESTING - 2)).is_ok());
        assert!(check_nesting(&nested("(", ")", MAX_NESTING - 1)).is_err());
    }

    #[test]
    fn brackets_in_literals_iris_and_comments_are_not_counted() {
        let deep = "(".repeat(MAX_NESTING * 2);
        for text in [
            format!("SELECT * WHERE {{ ?s ?p \"{deep}\" }}"),
            format!("SELECT * WHERE {{ ?s ?p '{deep}' }}"),
            format!("SELECT * WHERE {{ ?s ?p \"\"\"{deep}\n\"\"\" }}"),
            format!("SELECT * WHERE {{ ?s ?p ?o }} # {deep}\n"),
            format!("SELECT * WHERE {{ ?s <http://x/{deep}> ?o }}"),
        ] {
            assert!(check_nesting(&text).is_ok(), "{text}");
        }
    }

    /// Closing brackets the parser skips must not cancel real nesting.
    #[test]
    fn skipped_closers_cannot_hide_real_nesting() {
        let closers = ")".repeat(MAX_NESTING * 2);
        let opens = "(".repeat(MAX_NESTING + 1);
        for prefix in [
            format!("\"{closers}\""),
            format!("'{closers}'"),
            format!("# {closers}\n"),
            format!("<http://x/{closers}>"),
            format!("\"\\\"{closers}\""),
        ] {
            let text = format!("SELECT * WHERE {{ ?s ?p {prefix} FILTER({opens}1) }}");
            assert!(check_nesting(&text).is_err(), "{text}");
        }
    }

    /// `<` as less-than does not swallow what follows.
    #[test]
    fn less_than_is_not_an_iri() {
        let text = format!(
            "SELECT * WHERE {{ ?s ?p ?o FILTER(?o < 3 && {}1{}) }}",
            "(".repeat(MAX_NESTING),
            ")".repeat(MAX_NESTING)
        );
        assert!(check_nesting(&text).is_err());
    }
}
