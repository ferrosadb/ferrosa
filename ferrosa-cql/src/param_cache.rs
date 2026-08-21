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

use crate::ast::{InsertStatement, Statement, Term};
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

    /// Reconstruct this literal's typed [`Term`] on the cache-hit path by lexing
    /// the span directly: the LEXER already decodes each scalar literal
    /// (`''`-unescape, hex/UUID/number), so mapping its single token to a `Term`
    /// reuses that exact logic with NO duplication and skips the whole parser/
    /// term-grammar layer (measurably cheaper than `parser::parse_term` on the
    /// hot path). Requires the span to be exactly ONE scalar-literal token so a
    /// mis-scanned multi-token span can never bind a truncated value; the
    /// miss-path verification checks this result equals a full parse's Term.
    pub fn to_term(&self, query: &str) -> Result<Term, CqlError> {
        use crate::lexer::{Lexer, TokenKind};
        let mut lexer = Lexer::new(self.text(query))?;
        let term = match lexer.next_token()?.kind {
            TokenKind::StringLiteral(s) => Term::StringLiteral(s),
            TokenKind::IntegerLiteral(n) => Term::IntegerLiteral(n),
            TokenKind::FloatLiteral(f) => Term::FloatLiteral(f),
            TokenKind::UuidLiteral(u) => Term::UuidLiteral(u),
            TokenKind::BlobLiteral(b) => Term::BlobLiteral(b),
            other => {
                return Err(CqlError::SyntaxError(format!(
                    "param-cache span is not a scalar literal token: {other:?}"
                )));
            }
        };
        if lexer.next_token()?.kind != TokenKind::Eof {
            return Err(CqlError::SyntaxError(
                "param-cache span holds more than one token".into(),
            ));
        }
        Ok(term)
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
        // No further quote means the string is unterminated.
        let rel = memchr::memchr(b'\'', &b[i..])?;
        let q = i + rel;
        if q + 1 < b.len() && b[q + 1] == b'\'' {
            // Escaped quote: skip both, keep scanning.
            i = q + 2;
            continue;
        }
        return Some(q + 1);
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
        // No further quote means the identifier is unterminated.
        let rel = memchr::memchr(b'"', &b[i..])?;
        let q = i + rel;
        if q + 1 < b.len() && b[q + 1] == b'"' {
            i = q + 2;
            continue;
        }
        return Some(q + 1);
    }
    None
}

/// Build a fast-INSERT SKELETON from a verified parse: the same structure with
/// every scalar value replaced by a `BindMarker(None)`. The skeleton is cached
/// behind an `Arc` and executed per hit via the router's BORROWED fast-insert
/// path (`route_prepared_insert_fast`) with the hit's decoded values — so a hit
/// materializes no owned `InsertStatement` and clones nothing but an `Arc`
/// refcount. `USING TIMESTAMP`/`TTL` are dropped (verification rejects them).
fn skeleton_of(ins: &InsertStatement) -> InsertStatement {
    InsertStatement {
        keyspace: ins.keyspace.clone(),
        table: ins.table.clone(),
        columns: ins.columns.clone(),
        values: vec![Term::BindMarker(None); ins.values.len()],
        if_not_exists: ins.if_not_exists,
        using_timestamp: None,
        using_ttl: None,
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
/// provably reconstruct its siblings, return a cacheable skeleton `InsertStatement`.
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
///
/// Returns the cacheable skeleton (bind-marker values) on success.
pub fn verify_insert(
    query: &str,
    ins: &InsertStatement,
    normalized: &Normalized,
) -> Option<InsertStatement> {
    if ins.using_timestamp.is_some() || ins.using_ttl.is_some() {
        return None;
    }
    // Require an explicit column list matching the value arity. Rejects the
    // JSON-insert / column-less shapes whose `build` reconstruction would be
    // malformed, and pins `value_count == columns.len()`.
    if ins.columns.is_empty() || ins.columns.len() != ins.values.len() {
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
    Some(skeleton_of(ins))
}

/// Default number of distinct skeletons the transparent cache retains when
/// enabled. A production write mix repeats a small set of INSERT shapes, so a
/// few thousand skeletons cover it with a bounded footprint.
const DEFAULT_MAX_SKELETONS: u64 = 4096;

/// Environment variable that opts the transparent param cache in. Absent (the
/// default) leaves it OFF and `handle_query` always full-parses.
pub const ENABLE_ENV: &str = "FERROSA_TRANSPARENT_PARAM_CACHE";

/// Pure enable check — `true` only for an explicit truthy value. Takes the env
/// value as an argument so it is testable without mutating the process
/// environment (a `set_var` race, per the rust skill).
pub fn cache_enabled(env_val: Option<String>) -> bool {
    matches!(
        env_val.as_deref().map(str::trim),
        Some("1") | Some("true") | Some("on") | Some("yes")
    )
}

/// Build the transparent cache from the environment: `Some` when [`ENABLE_ENV`]
/// is truthy, else `None` (disabled — the default). Called once at CQL server
/// construction, never per request.
pub fn from_env() -> Option<std::sync::Arc<TransparentCache>> {
    if cache_enabled(std::env::var(ENABLE_ENV).ok()) {
        Some(std::sync::Arc::new(TransparentCache::new(
            DEFAULT_MAX_SKELETONS,
        )))
    } else {
        None
    }
}

/// What a cached skeleton resolves to.
#[derive(Debug)]
enum Entry {
    /// A verified INSERT skeleton (bind-marker values), shared behind an `Arc`.
    /// A hit clones only the `Arc` and supplies its own decoded values.
    Insert(std::sync::Arc<InsertStatement>),
    /// This skeleton was parsed once and could not be safely templated
    /// (non-INSERT, USING clause, collection value, verification mismatch, …).
    /// Cached so we skip re-verifying it; the caller always full-parses.
    Uncacheable,
}

/// What [`TransparentCache::resolve`] hands back. A `FastInsert` lets the caller
/// execute the INSERT through the router's BORROWED fast path with no owned
/// `Statement` materialization; `Full` is an ordinary parsed statement (or parse
/// error) for every non-fast path.
pub enum Resolved {
    /// A cache HIT on a verified INSERT: run `route_prepared_insert_fast` with
    /// the borrowed `skeleton` (bind-marker values) and these decoded `values`.
    FastInsert {
        skeleton: std::sync::Arc<InsertStatement>,
        values: Vec<Term>,
    },
    /// A full parsed statement (miss / uncacheable / bypass / decode fallback),
    /// or the parse error. Semantically identical to calling the parser directly.
    /// Boxed because `Statement` is large: keeping the two `Resolved` variants a
    /// similar size means a hit (the small `FastInsert`) does not pay for the big
    /// one, and the single box allocation lands only on the rarer non-hit paths.
    Full(Box<Result<Statement, CqlError>>),
}

impl Resolved {
    /// Materialize to an owned `Statement` — the fallback when the borrowed fast
    /// path is unavailable (frame carries bound values, connection is mid-
    /// transaction, or the router declines), and what tests compare. For a
    /// `FastInsert` this binds the decoded values into a clone of the skeleton;
    /// the result is byte-for-byte a full parse of the original query.
    pub fn into_statement(self) -> Result<Statement, CqlError> {
        match self {
            Resolved::FastInsert { skeleton, values } => {
                let mut ins = (*skeleton).clone();
                ins.values = values;
                Ok(Statement::Insert(ins))
            }
            Resolved::Full(result) => *result,
        }
    }
}

/// Per-call outcome of [`TransparentCache::resolve`], for metrics + tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Fast path: reconstructed from a cached template, no full parse.
    Hit,
    /// First sight of this skeleton: full-parsed, then cached (as template or
    /// uncacheable).
    Miss,
    /// Known-uncacheable skeleton: full-parsed, no re-verification.
    Uncacheable,
    /// Not normalizable (bind markers, over-length, unterminated): full-parsed,
    /// never cached.
    Bypass,
}

/// Transparent auto-parameterization cache: maps a normalized query skeleton to
/// a verified skeleton `InsertStatement` so repeated inline-literal INSERTs of the same
/// shape skip lex + parse + AST allocation on every hit.
///
/// Keyed by the full skeleton STRING (not a hash) — this is a correctness-
/// critical path and a hash collision would bind one shape's values into
/// another's template. The skeleton is length-bounded (see `MAX_NORMALIZE_LEN`) and the
/// cache is size-bounded by moka.
pub struct TransparentCache {
    cache: moka::sync::Cache<String, std::sync::Arc<Entry>, ahash::RandomState>,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    uncacheable: std::sync::atomic::AtomicU64,
    bypass: std::sync::atomic::AtomicU64,
}

impl TransparentCache {
    /// Create a cache holding up to `max_entries` distinct skeletons.
    ///
    /// Uses `ahash` for the skeleton key: it hashes the ~50-byte skeleton on
    /// EVERY hit, and ahash is several times faster than moka's default SipHash
    /// there while staying DoS-resistant (seeded) — which matters because the
    /// key is derived from client-supplied query text.
    pub fn new(max_entries: u64) -> Self {
        Self {
            cache: moka::sync::Cache::builder()
                .max_capacity(max_entries)
                .build_with_hasher(ahash::RandomState::new()),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            uncacheable: std::sync::atomic::AtomicU64::new(0),
            bypass: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Resolve `query` to a [`Statement`], using the cache when it safely can
    /// and falling back to `parse` (the real parser) otherwise. The returned
    /// [`Outcome`] classifies the path taken.
    ///
    /// A cache HIT reconstructs the INSERT by binding this query's extracted
    /// literal spans into the verified template — no full parse. Every other
    /// path calls `parse`, and its result is byte-for-byte what the caller would
    /// have gotten without the cache: the cache never changes semantics, only
    /// skips work on the repeated-shape hot path.
    pub fn resolve<F>(&self, query: &str, parse: F) -> (Resolved, Outcome)
    where
        F: FnOnce(&str) -> Result<Statement, CqlError>,
    {
        use std::sync::atomic::Ordering::Relaxed;
        let Some(norm) = normalize(query) else {
            self.bypass.fetch_add(1, Relaxed);
            return (Resolved::Full(Box::new(parse(query))), Outcome::Bypass);
        };
        match self.cache.get(&norm.skeleton) {
            Some(entry) => match entry.as_ref() {
                Entry::Insert(skeleton) => {
                    match decode_values(query, &norm, skeleton.values.len()) {
                        Some(values) => {
                            self.hits.fetch_add(1, Relaxed);
                            (
                                Resolved::FastInsert {
                                    skeleton: skeleton.clone(),
                                    values,
                                },
                                Outcome::Hit,
                            )
                        }
                        // A span failed to re-lex (a malformed literal that a
                        // full parse would also reject): fall back so the client
                        // gets the real error, never a silently wrong bind.
                        None => (Resolved::Full(Box::new(parse(query))), Outcome::Bypass),
                    }
                }
                Entry::Uncacheable => {
                    self.uncacheable.fetch_add(1, Relaxed);
                    (Resolved::Full(Box::new(parse(query))), Outcome::Uncacheable)
                }
            },
            None => {
                self.misses.fetch_add(1, Relaxed);
                let parsed = parse(query);
                if let Ok(Statement::Insert(ins)) = &parsed {
                    let entry = match verify_insert(query, ins, &norm) {
                        Some(skeleton) => Entry::Insert(std::sync::Arc::new(skeleton)),
                        None => Entry::Uncacheable,
                    };
                    self.cache.insert(norm.skeleton, std::sync::Arc::new(entry));
                } else if parsed.is_ok() {
                    // A well-formed non-INSERT: remember it is uncacheable so we
                    // do not re-attempt templating it every time.
                    self.cache
                        .insert(norm.skeleton, std::sync::Arc::new(Entry::Uncacheable));
                }
                // A parse ERROR is never cached (a later identical skeleton is
                // still a parse error, and caching Err would be wrong).
                (Resolved::Full(Box::new(parsed)), Outcome::Miss)
            }
        }
    }

    /// `(hits, misses, uncacheable, bypass)` since construction.
    pub fn stats(&self) -> (u64, u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.hits.load(Relaxed),
            self.misses.load(Relaxed),
            self.uncacheable.load(Relaxed),
            self.bypass.load(Relaxed),
        )
    }

    /// Drop every cached skeleton (e.g. after a schema change that could alter
    /// how a shape resolves). Bounded, best-effort.
    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }
}

/// Decode a hit's extracted literal spans into the ordered bind values for the
/// cached skeleton. Returns `None` if a span fails to re-lex (caller falls back
/// to a full parse — real error, never a wrong bind) or the span count no longer
/// matches the skeleton's bind arity (impossible for a matching skeleton, but
/// checked defensively).
fn decode_values(query: &str, norm: &Normalized, expected: usize) -> Option<Vec<Term>> {
    if norm.literals.len() != expected {
        return None;
    }
    let mut values = Vec::with_capacity(expected);
    for lit in &norm.literals {
        values.push(lit.to_term(query).ok()?);
    }
    Some(values)
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
        // values, skeleton + decoded values must equal a full parse.
        let seed = "INSERT INTO ks.t (a, b, c) VALUES ('seed', 1, 0xAA)";
        let n_seed = norm(seed);
        let ins_seed = insert_of(seed);
        let skeleton =
            std::sync::Arc::new(verify_insert(seed, &ins_seed, &n_seed).expect("seed cacheable"));

        // A sibling of the same shape — reconstruct via the cached skeleton.
        let sib = "INSERT INTO ks.t (a, b, c) VALUES ('machine-x', 999, 0xDEADBEEF)";
        let n_sib = norm(sib);
        assert_eq!(
            n_sib.skeleton, n_seed.skeleton,
            "same shape => same skeleton"
        );
        let values = decode_values(sib, &n_sib, skeleton.values.len()).expect("decode");
        let rebuilt = Resolved::FastInsert { skeleton, values }
            .into_statement()
            .unwrap();
        assert_eq!(
            rebuilt,
            crate::parser::parse(sib).expect("parse sibling"),
            "skeleton + decoded values must equal a full parse of the sibling"
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

    /// The end-to-end cache contract: for a repeated INSERT shape, a HIT
    /// returns exactly what a full parse of that specific query would, and
    /// every path's Statement equals the real parse.
    #[test]
    fn cache_hit_reconstructs_the_exact_full_parse() {
        let cache = TransparentCache::new(64);
        let shape = [
            "INSERT INTO ks.t (a, b, c) VALUES ('first', 1, 0xAA)",
            "INSERT INTO ks.t (a, b, c) VALUES ('second-value', 22, 0xBBCC)",
            "INSERT INTO ks.t (a, b, c) VALUES ('third', 333, 0xDEADBEEF)",
        ];
        // First query: MISS (parsed + cached as template).
        let (r0, o0) = cache.resolve(shape[0], crate::parser::parse);
        assert_eq!(o0, Outcome::Miss);
        assert_eq!(
            r0.into_statement().unwrap(),
            crate::parser::parse(shape[0]).unwrap()
        );
        // Subsequent same-shape queries: HIT, and byte-identical to a full parse.
        for q in &shape[1..] {
            let (r, o) = cache.resolve(q, crate::parser::parse);
            assert_eq!(o, Outcome::Hit, "second+ sighting of a shape must hit");
            assert_eq!(
                r.into_statement().unwrap(),
                crate::parser::parse(q).unwrap(),
                "hit must equal a full parse of `{q}`"
            );
        }
        let (hits, misses, _unc, _byp) = cache.stats();
        assert_eq!((hits, misses), (2, 1));
    }

    #[test]
    fn non_insert_is_cached_uncacheable_but_still_parsed_correctly() {
        let cache = TransparentCache::new(64);
        let q = "SELECT a, b FROM t WHERE k = 'x' LIMIT 10";
        let (r1, o1) = cache.resolve(q, crate::parser::parse);
        assert_eq!(o1, Outcome::Miss);
        assert_eq!(
            r1.into_statement().unwrap(),
            crate::parser::parse(q).unwrap()
        );
        // A different SELECT of the same shape: known-uncacheable, still correct.
        let q2 = "SELECT a, b FROM t WHERE k = 'y' LIMIT 99";
        let (r2, o2) = cache.resolve(q2, crate::parser::parse);
        assert_eq!(o2, Outcome::Uncacheable);
        assert_eq!(
            r2.into_statement().unwrap(),
            crate::parser::parse(q2).unwrap()
        );
    }

    #[test]
    fn bind_marker_query_bypasses_the_cache() {
        let cache = TransparentCache::new(64);
        let q = "INSERT INTO t (k, v) VALUES (?, ?)";
        let (r, o) = cache.resolve(q, crate::parser::parse);
        assert_eq!(o, Outcome::Bypass);
        assert_eq!(
            r.into_statement().unwrap(),
            crate::parser::parse(q).unwrap()
        );
        assert_eq!(cache.stats().3, 1, "one bypass counted");
    }

    #[test]
    fn parse_error_is_returned_and_never_cached() {
        let cache = TransparentCache::new(64);
        let bad = "INSERT INTO t (k) VALUES ('x'"; // unterminated ) — parse error
        let (r, _o) = cache.resolve(bad, crate::parser::parse);
        assert!(r.into_statement().is_err(), "a parse error must propagate");
    }

    #[test]
    fn uncacheable_insert_shape_falls_back_every_time() {
        // An INSERT with a collection value can't be templated (slice 1): first
        // sight caches it Uncacheable, later same-shape queries take that path.
        // Same set CARDINALITY so both share one skeleton (differing element
        // counts would mask to different skeletons and each be its own miss).
        let cache = TransparentCache::new(64);
        let a = "INSERT INTO t (k, s) VALUES ('x', {1, 2})";
        let b = "INSERT INTO t (k, s) VALUES ('y', {3, 4})";
        let (_ra, oa) = cache.resolve(a, crate::parser::parse);
        assert_eq!(oa, Outcome::Miss);
        let (rb, ob) = cache.resolve(b, crate::parser::parse);
        assert_eq!(ob, Outcome::Uncacheable);
        assert_eq!(
            rb.into_statement().unwrap(),
            crate::parser::parse(b).unwrap()
        );
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
