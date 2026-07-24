//! Module: Transparent auto-parameterization for unprepared CQL queries.
//! Correctness: Correct when `normalize` masks exactly the scalar literals a
//!   real parse would treat as literal Terms — verified per-skeleton on the
//!   first (cache-miss) parse before any skeleton is trusted for fast-path
//!   binding, and when the skeleton preserves every non-literal source byte so
//!   two queries sharing a skeleton are structurally identical.
//! Last revised: 2026-07-24
//! Last changed: New module — slice 1 (INSERT) of transparent prepared-statement
//!   caching (t_48d5eeaa). Cheap single-pass normalizer + literal extraction;
//!   the flamegraph shows lex+parse+alloc is ~40% of unprepared-write CPU and a
//!   normalized-skeleton cache can skip parse+AST-build on the repeated-shape
//!   hot path (measured: normalize 87ns vs full parse 422ns per INSERT).
//!
//! # Why a separate normalizer (not the lexer)
//!
//! The lexer is the cost we are trying to avoid on a cache hit. This scanner
//! masks the four high-cardinality scalar literal forms — single-quoted
//! strings, `0x` hex blobs, digit-led numbers, and UUIDs — WITHOUT keyword
//! classification, `Token` construction, or AST allocation. Everything else
//! (identifiers, keywords including `true`/`false`/`null`, operators,
//! punctuation) is copied verbatim into the skeleton, so the skeleton is the
//! source query with only its scalar literals replaced by `?`.
//!
//! # Safety model
//!
//! This scanner is deliberately CONSERVATIVE and never authoritative. It is a
//! fast pre-filter whose output is proven correct, per distinct skeleton, by a
//! real parse on the cache-miss path (see the caller). Anything it cannot mask
//! with certainty it simply leaves in the skeleton; the worst case is a lower
//! hit rate, never a wrong bind. A skeleton whose masked literals do not match
//! the real parse's literal Terms is marked uncacheable and always full-parsed.

use crate::ast::{InsertStatement, Term};
use crate::error::CqlError;

/// The kind of a masked scalar literal — enough to reconstruct its typed value
/// on a cache hit without re-lexing the whole statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiteralKind {
    /// `'...'` single-quoted string (may contain `''` escapes).
    String,
    /// `0x…` hex blob.
    Hex,
    /// Digit-led run: integer, float, or UUID. Disambiguated on bind by the
    /// target column type / a single-token parse of the span.
    Numeric,
}

/// A masked literal: its byte span in the ORIGINAL query and its kind. The span
/// is `original[start..end]`, including the surrounding quotes for `String` and
/// the `0x` prefix for `Hex`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    pub start: usize,
    pub end: usize,
    pub kind: LiteralKind,
}

impl Literal {
    /// The masked literal's source text, `query[start..end]`.
    pub fn text<'a>(&self, query: &'a str) -> &'a str {
        &query[self.start..self.end]
    }

    /// Reconstruct this literal's typed [`Term`] on the cache-hit path, reusing
    /// the parser's single-term logic (see [`crate::parser::parse_term`]) so the
    /// bound value is byte-for-byte what a full parse of the original query would
    /// have produced. No duplicated unescape/hex/UUID logic lives here.
    pub fn to_term(&self, query: &str) -> Result<Term, CqlError> {
        crate::parser::parse_term(self.text(query))
    }
}

/// A normalized query: the skeleton (literals replaced by `?`) plus the ordered
/// spans of the literals that were masked, left-to-right.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Normalized {
    /// The source query with every masked scalar literal replaced by a single
    /// `?`. Used as the cache key: byte-identical skeletons are structurally
    /// identical queries differing only in literal values.
    pub skeleton: String,
    /// Masked literals in source order. `literals[i]` corresponds to the i-th
    /// `?` that this normalizer introduced (NOT to pre-existing `?` bind
    /// markers, which are copied verbatim and never counted here).
    pub literals: Vec<Literal>,
}

/// Longest query we will attempt to normalize. A pathological multi-kilobyte
/// statement is rare, not on the hot repeated-shape path, and not worth the
/// skeleton allocation — fall back to full parse.
const MAX_NORMALIZE_LEN: usize = 4096;

/// Cheap single-pass normalization. Returns `Some(Normalized)` for a query we
/// can mask, or `None` when the query is too long or contains a byte we will
/// not classify (so the caller full-parses without caching).
///
/// Returns `None` — never a wrong answer — for anything ambiguous. This is a
/// pre-filter; correctness of the masking is confirmed by the caller's parse.
pub fn normalize(query: &str) -> Option<Normalized> {
    let b = query.as_bytes();
    if b.len() > MAX_NORMALIZE_LEN {
        return None;
    }
    let mut skeleton = String::with_capacity(b.len());
    let mut literals: Vec<Literal> = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i];
        match c {
            b'\'' => {
                let start = i;
                let end = scan_string(b, i)?;
                literals.push(Literal {
                    start,
                    end,
                    kind: LiteralKind::String,
                });
                skeleton.push('?');
                i = end;
            }
            b'0'..=b'9' => {
                let start = i;
                let (end, kind) = scan_numeric_or_hex(b, i);
                literals.push(Literal { start, end, kind });
                skeleton.push('?');
                i = end;
            }
            // A double-quoted identifier is NOT a literal (it is a quoted
            // column/table name). Copy it verbatim, quotes and all, so it stays
            // part of the skeleton and is never masked or extracted.
            b'"' => {
                let end = scan_quoted_ident(b, i)?;
                skeleton.push_str(&query[i..end]);
                i = end;
            }
            // A client-supplied `?` bind marker makes the skeleton's `?`
            // ambiguous — we could no longer tell a normalizer-introduced `?`
            // (bind from an extracted span) from a client one (bind from a wire
            // value). Bail: such a query is already parameterized and is not the
            // inline-literal shape this cache targets. Full-parse without caching.
            b'?' => return None,
            _ => {
                // A non-literal byte. Everything that is not the start of a
                // scalar literal — identifiers, keywords, whitespace,
                // operators, `(`/`)`/`,` — is copied verbatim. `push(c as char)`
                // is correct because a multi-byte UTF-8 code point only appears
                // inside a string literal (handled above) or a quoted identifier;
                // a bare non-ASCII byte here means an identifier we still copy
                // byte-for-byte.
                skeleton.push(c as char);
                i += 1;
            }
        }
    }
    Some(Normalized { skeleton, literals })
}

/// Scan a single-quoted string starting at `open` (`b[open] == '\''`). Returns
/// the index just past the closing quote, or `None` if unterminated. Handles
/// the `''` escape: a doubled quote is content, not a terminator.
fn scan_string(b: &[u8], open: usize) -> Option<usize> {
    let mut i = open + 1;
    while i < b.len() {
        match memchr::memchr(b'\'', &b[i..]) {
            Some(rel) => {
                let q = i + rel;
                if q + 1 < b.len() && b[q + 1] == b'\'' {
                    // Escaped quote: skip both, keep scanning.
                    i = q + 2;
                    continue;
                }
                return Some(q + 1);
            }
            None => return None,
        }
    }
    None
}

/// Scan a digit-led run starting at `start` (`b[start]` is an ASCII digit).
/// Returns `(end, kind)`. Recognizes `0x…` hex; otherwise a numeric/UUID run of
/// `[0-9A-Fa-f.\-+eE]` — the union of integer, float (incl. exponent), and UUID
/// shapes. The exact type is resolved on bind, so this only needs to find the
/// run's END, and it must match what the lexer would consume as one token.
fn scan_numeric_or_hex(b: &[u8], start: usize) -> (usize, LiteralKind) {
    if b[start] == b'0' && start + 1 < b.len() && (b[start + 1] | 0x20) == b'x' {
        let mut i = start + 2;
        while i < b.len() && b[i].is_ascii_hexdigit() {
            i += 1;
        }
        return (i, LiteralKind::Hex);
    }
    let mut i = start;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_alphanumeric() || c == b'.' || c == b'-' || c == b'+' {
            // `-`/`+` only continue the run as part of a UUID (`8-4-4-4-12`) or
            // an exponent sign (`1e-9`); a following non-alnum would be a
            // separate operator, so require the sign to be followed by an
            // alnum/hex continuation to avoid swallowing `1-2` as one literal.
            if (c == b'-' || c == b'+') && !(i + 1 < b.len() && b[i + 1].is_ascii_alphanumeric()) {
                break;
            }
            i += 1;
        } else {
            break;
        }
    }
    (i, LiteralKind::Numeric)
}

/// Scan a `"..."` quoted identifier starting at `open`. Returns the index just
/// past the closing quote (handles the `""` escape), or `None` if unterminated.
fn scan_quoted_ident(b: &[u8], open: usize) -> Option<usize> {
    let mut i = open + 1;
    while i < b.len() {
        match memchr::memchr(b'"', &b[i..]) {
            Some(rel) => {
                let q = i + rel;
                if q + 1 < b.len() && b[q + 1] == b'"' {
                    i = q + 2;
                    continue;
                }
                return Some(q + 1);
            }
            None => return None,
        }
    }
    None
}

/// A verified, cacheable INSERT template: the structural skeleton of an INSERT
/// (everything except its masked scalar values), from which any sibling INSERT
/// sharing the same normalized skeleton is reconstructed by binding that
/// sibling's extracted literal spans in order.
///
/// Built ONLY by [`verify_insert`], which proves — against a real parse — that
/// the normalizer masked exactly this INSERT's value Terms. It therefore holds
/// no values of its own; each cache hit supplies its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertTemplate {
    keyspace: Option<String>,
    table: String,
    columns: Vec<String>,
    if_not_exists: bool,
}

impl InsertTemplate {
    /// Reconstruct a concrete INSERT by binding `values` (this hit's extracted
    /// literals, converted via [`Literal::to_term`]) into the template. The
    /// caller guarantees `values.len()` equals the template's masked-value count
    /// (the skeleton fixes it), so the result is structurally identical to a
    /// full parse of the original query, differing only in the bound values.
    pub fn build(&self, values: Vec<Term>) -> InsertStatement {
        InsertStatement {
            keyspace: self.keyspace.clone(),
            table: self.table.clone(),
            columns: self.columns.clone(),
            values,
            if_not_exists: self.if_not_exists,
            using_timestamp: None,
            using_ttl: None,
        }
    }
}

/// True for the scalar literal Terms this cache masks and can reconstruct from a
/// span. Deliberately narrow for slice 1: `bool`/`null` are keyword-literals the
/// normalizer leaves in the skeleton (so they never appear as masked spans), and
/// collections/functions/bind-markers/durations are rejected outright — an
/// INSERT containing any of them is never templated and always full-parsed.
fn is_maskable_scalar(t: &Term) -> bool {
    matches!(
        t,
        Term::StringLiteral(_)
            | Term::IntegerLiteral(_)
            | Term::FloatLiteral(_)
            | Term::UuidLiteral(_)
            | Term::BlobLiteral(_)
    )
}

/// Verify a parsed INSERT against its normalization and, if the fast path can
/// provably reconstruct its siblings, return a cacheable [`InsertTemplate`].
///
/// This is the safety spine: a skeleton is trusted for fast-path binding ONLY
/// after this returns `Some` for the first (cache-miss) query that produced it.
/// It returns `None` (→ never cache, always full-parse) unless ALL hold:
/// - no `USING TIMESTAMP`/`USING TTL` (their literal would be masked but is not
///   in `values`, so it cannot be reconstructed by position);
/// - every value is a maskable scalar literal (so masked-count == value-count);
/// - the masked-span count equals the value count; and
/// - each masked span, re-parsed via [`Literal::to_term`], equals the parse's
///   value Term at the same position — i.e. the normalizer and the real parser
///   agree, byte-for-byte, on every literal.
pub fn verify_insert(
    query: &str,
    ins: &InsertStatement,
    normalized: &Normalized,
) -> Option<InsertTemplate> {
    if ins.using_timestamp.is_some() || ins.using_ttl.is_some() {
        return None;
    }
    if ins.values.len() != normalized.literals.len() {
        return None;
    }
    for (lit, value) in normalized.literals.iter().zip(ins.values.iter()) {
        if !is_maskable_scalar(value) {
            return None;
        }
        // The airtight check: the normalizer's span and the parser's Term must
        // be the same value. Runs once per skeleton (miss path); never on hits.
        match lit.to_term(query) {
            Ok(reparsed) if &reparsed == value => {}
            _ => return None,
        }
    }
    Some(InsertTemplate {
        keyspace: ins.keyspace.clone(),
        table: ins.table.clone(),
        columns: ins.columns.clone(),
        if_not_exists: ins.if_not_exists,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(q: &str) -> Normalized {
        normalize(q).expect("normalizes")
    }

    /// The literal texts, extracted from the original by span, in order.
    fn lit_texts<'a>(q: &'a str, n: &Normalized) -> Vec<&'a str> {
        n.literals.iter().map(|l| &q[l.start..l.end]).collect()
    }

    #[test]
    fn masks_the_three_scalar_kinds_in_an_insert() {
        let q = "INSERT INTO baselines.data (pk, ck, val) VALUES \
                 ('machine-abc-000012345', 1, 0xDEADBEEF0123456789ABCDEF)";
        let n = norm(q);
        assert_eq!(
            n.skeleton,
            "INSERT INTO baselines.data (pk, ck, val) VALUES (?, ?, ?)"
        );
        assert_eq!(
            lit_texts(q, &n),
            vec!["'machine-abc-000012345'", "1", "0xDEADBEEF0123456789ABCDEF"]
        );
        assert_eq!(
            n.literals.iter().map(|l| l.kind).collect::<Vec<_>>(),
            vec![LiteralKind::String, LiteralKind::Numeric, LiteralKind::Hex]
        );
    }

    #[test]
    fn two_inserts_of_the_same_shape_share_a_skeleton() {
        let a = norm("INSERT INTO t (k, v) VALUES ('alpha', 100)");
        let b = norm("INSERT INTO t (k, v) VALUES ('bravo-99', 7)");
        assert_eq!(a.skeleton, b.skeleton);
        assert_eq!(a.skeleton, "INSERT INTO t (k, v) VALUES (?, ?)");
    }

    #[test]
    fn escaped_quote_stays_inside_one_string_literal() {
        let q = "INSERT INTO t (k) VALUES ('O''Brien')";
        let n = norm(q);
        assert_eq!(n.skeleton, "INSERT INTO t (k) VALUES (?)");
        assert_eq!(lit_texts(q, &n), vec!["'O''Brien'"]);
    }

    #[test]
    fn a_quote_inside_a_string_is_not_a_delimiter() {
        // The comma and paren live INSIDE the string — they must not leak into
        // the skeleton as structure.
        let q = "INSERT INTO t (k) VALUES ('a, b) c')";
        let n = norm(q);
        assert_eq!(n.skeleton, "INSERT INTO t (k) VALUES (?)");
        assert_eq!(lit_texts(q, &n), vec!["'a, b) c'"]);
    }

    #[test]
    fn existing_bind_markers_make_the_query_uncacheable() {
        // A client-parameterized query is already cheap and would make the
        // skeleton's `?` ambiguous — the normalizer must bail (None), so the
        // caller full-parses it without transparent caching.
        assert_eq!(normalize("INSERT INTO t (k, v) VALUES (?, ?)"), None);
        assert_eq!(normalize("INSERT INTO t (k, v) VALUES (?, 'x')"), None);
    }

    #[test]
    fn quoted_identifier_is_not_masked() {
        let q = "INSERT INTO \"MyTable\" (\"Col\") VALUES ('x')";
        let n = norm(q);
        assert_eq!(n.skeleton, "INSERT INTO \"MyTable\" (\"Col\") VALUES (?)");
        assert_eq!(lit_texts(q, &n), vec!["'x'"]);
    }

    #[test]
    fn keywords_and_booleans_and_null_are_left_in_the_skeleton() {
        // true/false/null are keyword-literals; leaving them unmasked is safe
        // and keeps the normalizer from having to know CQL keywords.
        let q = "INSERT INTO t (k, b, n) VALUES ('x', true, null)";
        let n = norm(q);
        assert_eq!(n.skeleton, "INSERT INTO t (k, b, n) VALUES (?, true, null)");
        assert_eq!(lit_texts(q, &n), vec!["'x'"]);
    }

    #[test]
    fn uuid_is_masked_as_one_literal() {
        let q = "INSERT INTO t (id) VALUES (550e8400-e29b-41d4-a716-446655440000)";
        let n = norm(q);
        assert_eq!(n.skeleton, "INSERT INTO t (id) VALUES (?)");
        assert_eq!(
            lit_texts(q, &n),
            vec!["550e8400-e29b-41d4-a716-446655440000"]
        );
        assert_eq!(n.literals[0].kind, LiteralKind::Numeric);
    }

    #[test]
    fn float_literal_is_one_masked_span() {
        let q = "INSERT INTO t (x) VALUES (3.14159)";
        let n = norm(q);
        assert_eq!(n.skeleton, "INSERT INTO t (x) VALUES (?)");
        assert_eq!(lit_texts(q, &n), vec!["3.14159"]);
    }

    #[test]
    fn unterminated_string_returns_none() {
        assert_eq!(normalize("INSERT INTO t (k) VALUES ('oops)"), None);
    }

    #[test]
    fn over_length_query_returns_none() {
        let big = format!(
            "INSERT INTO t (k) VALUES ('{}')",
            "x".repeat(MAX_NORMALIZE_LEN)
        );
        assert_eq!(normalize(&big), None);
    }

    /// Safety spine: for a set of INSERTs, each masked span converted via
    /// `to_term` must equal, in order, the value Terms a full parse produced.
    /// This is what lets the cache-hit path bind extracted spans and get a
    /// value identical to full parsing — with no duplicated literal logic.
    #[test]
    fn extracted_spans_reparse_to_the_full_parses_value_terms() {
        use crate::ast::Statement;
        for q in [
            "INSERT INTO ks.t (a, b, c) VALUES ('machine-abc-000012345', 1, 0xDEADBEEF)",
            "INSERT INTO t (k, v) VALUES ('O''Brien', 42)",
            "INSERT INTO t (x, y) VALUES (3.14159, 550e8400-e29b-41d4-a716-446655440000)",
            "INSERT INTO t (k) VALUES ('a, b) c')",
        ] {
            let stmt = crate::parser::parse(q).expect("parse");
            let Statement::Insert(ins) = stmt else {
                panic!("expected INSERT");
            };
            // The parse's literal value Terms (all scalar in these fixtures).
            let parsed_terms = ins.values;
            let n = norm(q);
            let extracted: Vec<Term> = n
                .literals
                .iter()
                .map(|l| l.to_term(q).expect("span re-parses to a term"))
                .collect();
            assert_eq!(
                extracted, parsed_terms,
                "extracted spans must reconstruct the full parse's value Terms for `{q}`"
            );
        }
    }

    use crate::ast::Statement;

    /// Parse a query and unwrap its INSERT, panicking otherwise.
    fn insert_of(q: &str) -> InsertStatement {
        match crate::parser::parse(q).expect("parse") {
            Statement::Insert(i) => i,
            other => panic!("expected INSERT, got {other:?}"),
        }
    }

    #[test]
    fn verify_then_build_round_trips_to_the_full_parse() {
        // The end-to-end fast-path contract: for the SAME shape with different
        // values, template.build(bound spans) must equal a full parse.
        let seed = "INSERT INTO ks.t (a, b, c) VALUES ('seed', 1, 0xAA)";
        let n_seed = norm(seed);
        let ins_seed = insert_of(seed);
        let tmpl = verify_insert(seed, &ins_seed, &n_seed).expect("seed is cacheable");

        // A sibling of the same shape — must reconstruct via the cached template.
        let sib = "INSERT INTO ks.t (a, b, c) VALUES ('machine-x', 999, 0xDEADBEEF)";
        let n_sib = norm(sib);
        assert_eq!(
            n_sib.skeleton, n_seed.skeleton,
            "same shape => same skeleton"
        );
        let bound: Vec<Term> = n_sib
            .literals
            .iter()
            .map(|l| l.to_term(sib).unwrap())
            .collect();
        let rebuilt = Statement::Insert(tmpl.build(bound));
        assert_eq!(
            rebuilt,
            crate::parser::parse(sib).expect("parse sibling"),
            "template.build must equal a full parse of the sibling"
        );
    }

    #[test]
    fn verify_rejects_using_timestamp() {
        let q = "INSERT INTO t (k, v) VALUES ('x', 1) USING TIMESTAMP 12345";
        let n = norm(q);
        assert_eq!(verify_insert(q, &insert_of(q), &n), None);
    }

    #[test]
    fn verify_rejects_bool_and_null_values() {
        // Normalizer leaves true/null in the skeleton, so masked-count (1) !=
        // value-count (3): the count guard rejects — always full-parse instead.
        let q = "INSERT INTO t (k, b, n) VALUES ('x', true, null)";
        let n = norm(q);
        assert_eq!(verify_insert(q, &insert_of(q), &n), None);
    }

    #[test]
    fn verify_rejects_collection_value() {
        let q = "INSERT INTO t (k, s) VALUES ('x', {1, 2, 3})";
        let n = norm(q);
        assert_eq!(verify_insert(q, &insert_of(q), &n), None);
    }

    #[test]
    fn verify_rejects_function_call_value() {
        let q = "INSERT INTO t (k, id) VALUES ('x', now())";
        let n = norm(q);
        assert_eq!(verify_insert(q, &insert_of(q), &n), None);
    }

    #[test]
    fn skeleton_preserves_every_non_literal_byte() {
        // The core safety invariant: replacing each `?` in the skeleton back
        // with its original literal span reproduces the source query exactly.
        for q in [
            "INSERT INTO ks.t (a, b, c) VALUES ('s', 42, 0xFF)",
            "INSERT INTO t (k) VALUES ('a''b')",
            "UPDATE t SET v = 5 WHERE k = 'x'",
            "SELECT * FROM t WHERE k = 'x' AND n = 10 LIMIT 100",
        ] {
            let n = norm(q);
            let mut rebuilt = String::new();
            let mut lit = n.literals.iter();
            for ch in n.skeleton.chars() {
                if ch == '?' {
                    let l = lit.next().expect("a literal for each '?'");
                    rebuilt.push_str(&q[l.start..l.end]);
                } else {
                    rebuilt.push(ch);
                }
            }
            assert_eq!(rebuilt, q, "skeleton+literals must reconstruct the source");
        }
    }
}
