//! Streaming (bounded-memory) search over an on-disk FTI sidecar file.
//!
//! [`super::reader::FullTextIndexReader::open`] deserializes the ENTIRE index
//! — every term string and every posting — into heap structures before a
//! single posting is scored. For a broad term over a large table that is
//! O(index size) per sidecar per query, which is what OOM-killed every replica
//! at once on the live `fts_match('memory')` scan (t_ee98faa0 layer 2).
//!
//! This module walks the sequential FTI byte format (see
//! [`super::builder`] for the layout) through a [`std::io::BufReader`]
//! instead:
//!
//! * [`scan_term_each`] — the non-materializing primitive: hands each match to
//!   a callback as it is decoded, holding an O(1) working set (one posting key
//!   at a time) and stopping early on [`std::ops::ControlFlow::Break`]. A caller
//!   that forwards hits into a bounded channel keeps peak memory independent of
//!   the matching-doc count even without a `LIMIT` — the shape that OOM-killed
//!   replicas on a broad `fts_match` (t_8fc24ce2).
//! * [`scan_term_top_k`] — single-term search (the live-OOM query shape), built
//!   on `scan_term_each`. Postings of the one matching term are scored as they
//!   are decoded and fed into a bounded top-k heap when the query carries a
//!   `LIMIT k`; peak additional memory is O(k), independent of the index or
//!   matching-doc count. Without a limit the complete hit set is returned
//!   (O(matches) — the result itself, nothing more).
//!
//! The term dictionary is written sorted (see `serialize_fti`), so the walk
//! early-exits as soon as it passes the target term.
//!
//! Last revised: 2026-07-15
//! Last changed: Extracted `scan_term_each` (non-materializing callback walk)
//! as the primitive underneath `scan_term_top_k`, for bounded-memory streaming.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use super::builder::{
    FTI_MAGIC, FTI_TERM_INDEX_FOOTER_LEN, FTI_TERM_INDEX_INTERVAL, FTI_TERM_INDEX_MAGIC,
    FTI_VERSION,
};
use super::reader::FtsHit;
use super::scoring::{bm25_score, Bm25Params};
use super::topk::TopK;

/// Streaming single-term search over one FTI sidecar file.
///
/// Matching semantics and BM25 scores are identical to deserializing the file
/// and running [`super::reader::FullTextIndexReader::search`] with
/// `FtsQuery::Term` — only the memory profile differs (O(k) / O(matches)
/// instead of O(index)).
///
/// # Errors
///
/// Returns `Err` on I/O failure or a malformed/truncated FTI file.
pub fn scan_term_top_k(
    path: &Path,
    term: &str,
    limit: Option<usize>,
) -> Result<Vec<FtsHit>, String> {
    if limit == Some(0) {
        return Ok(vec![]);
    }
    let mut topk = limit.map(TopK::new);
    let mut all_hits: Vec<FtsHit> = Vec::new();

    scan_term_each(path, term, |hit| {
        match topk.as_mut() {
            Some(t) => t.push_owned(hit.partition_key, hit.score),
            None => all_hits.push(hit),
        }
        std::ops::ControlFlow::Continue(())
    })?;

    Ok(match topk {
        Some(t) => t.into_hits(),
        None => {
            all_hits.sort_by(|a, b| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| a.partition_key.cmp(&b.partition_key))
            });
            all_hits
        }
    })
}

/// Streaming single-term search that hands each match to `on_hit` **as it is
/// decoded**, holding no growing result buffer of its own.
///
/// This is the non-materializing primitive underneath [`scan_term_top_k`]: the
/// walk's own working set is O(1) (one posting key at a time), so a caller that
/// forwards hits into a bounded channel keeps peak memory independent of the
/// matching-doc count — the shape that OOM-killed replicas on a broad
/// `fts_match` with no `LIMIT` (t_8fc24ce2). Matching semantics and BM25 scores
/// are identical to [`super::reader::FullTextIndexReader::search`] for a
/// `FtsQuery::Term`.
///
/// `on_hit` returns [`std::ops::ControlFlow::Break`] to stop the walk early (consumer-paced
/// backpressure — e.g. the downstream channel's receiver was dropped, or a
/// `LIMIT` is already satisfied). Because the term dictionary is written sorted,
/// the walk also early-exits as soon as it passes the target term.
///
/// # Errors
///
/// Returns `Err` on I/O failure or a malformed/truncated FTI file.
pub fn scan_term_each<F>(path: &Path, term: &str, on_hit: F) -> Result<(), String>
where
    F: FnMut(FtsHit) -> std::ops::ControlFlow<()>,
{
    let file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let file_len = file
        .metadata()
        .map_err(|e| format!("stat {}: {e}", path.display()))?
        .len();
    scan_term_each_in(BufReader::new(file), file_len, term, on_hit)
}

/// [`scan_term_each`] over any seekable source of `file_len` bytes.
pub(crate) fn scan_term_each_in<R, F>(
    mut reader: R,
    file_len: u64,
    term: &str,
    mut on_hit: F,
) -> Result<(), String>
where
    R: Read + Seek,
    F: FnMut(FtsHit) -> std::ops::ControlFlow<()>,
{
    // Trailer first: `total_doc_len` (u64 LE) is the last 8 bytes, and BM25
    // needs avgdl before the first posting is scored.
    if file_len < 8 + 9 {
        return Err(format!("FTI too short: {file_len} bytes"));
    }
    reader
        .seek(SeekFrom::End(-8))
        .map_err(|e| format!("seek trailer: {e}"))?;
    let total_doc_len = read_u64(&mut reader)?;
    reader
        .seek(SeekFrom::Start(0))
        .map_err(|e| format!("seek start: {e}"))?;

    // Header: magic(4) + version(1) + doc_count(4) + term_count(4).
    let mut magic = [0u8; 4];
    reader
        .read_exact(&mut magic)
        .map_err(|e| format!("read magic: {e}"))?;
    if &magic != FTI_MAGIC {
        return Err(format!("invalid FTI magic: {magic:?}"));
    }
    let version = read_u8(&mut reader)?;
    if version != FTI_VERSION {
        return Err(format!(
            "unsupported FTI version {version} (expected {FTI_VERSION})"
        ));
    }
    let doc_count = read_u32(&mut reader)?;
    let term_count = read_u32(&mut reader)?;

    let avgdl = if doc_count == 0 {
        0.0
    } else {
        total_doc_len as f64 / doc_count as f64
    };
    let params = Bm25Params::default();
    let target = term.as_bytes();
    let Some(walk) = locate_term(&mut reader, file_len, target, term_count)? else {
        return Ok(()); // sorts before the first term: absent.
    };
    reader
        .seek(SeekFrom::Start(walk.offset))
        .map_err(|e| format!("seek to term: {e}"))?;
    let mut term_buf: Vec<u8> = Vec::new();

    for _ in 0..walk.terms {
        let term_len = read_u16(&mut reader)? as usize;
        term_buf.clear();
        term_buf.resize(term_len, 0);
        reader
            .read_exact(&mut term_buf)
            .map_err(|e| format!("read term: {e}"))?;
        let doc_freq = read_u32(&mut reader)?;
        let posting_count = read_u32(&mut reader)?;

        match term_buf.as_slice().cmp(target) {
            std::cmp::Ordering::Less => {
                // Not our term yet — skip its postings without decoding keys.
                for _ in 0..posting_count {
                    let pk_len = read_u16(&mut reader)? as i64;
                    reader
                        .seek_relative(pk_len + 8)
                        .map_err(|e| format!("skip posting: {e}"))?;
                }
            }
            std::cmp::Ordering::Equal => {
                for _ in 0..posting_count {
                    let pk_len = read_u16(&mut reader)? as usize;
                    let mut pk = vec![0u8; pk_len];
                    reader
                        .read_exact(&mut pk)
                        .map_err(|e| format!("read posting key: {e}"))?;
                    let term_freq = read_u32(&mut reader)?;
                    let doc_len = read_u32(&mut reader)?;
                    let score = bm25_score(
                        term_freq,
                        doc_freq as u64,
                        doc_count as u64,
                        doc_len,
                        avgdl,
                        &params,
                    );
                    if on_hit(FtsHit {
                        partition_key: pk,
                        score,
                    })
                    .is_break()
                    {
                        return Ok(());
                    }
                }
                break; // dictionary is sorted; the term appears once.
            }
            std::cmp::Ordering::Greater => break, // sorted dictionary — passed it.
        }
    }

    Ok(())
}

/// Where a lookup starts walking the dictionary, and how many terms it may
/// walk from there.
struct Walk {
    offset: u64,
    terms: u32,
}

/// Header: magic(4) + version(1) + doc_count(4) + term_count(4).
const HEADER_LEN: u64 = 13;

/// Sidecars searched by walking their whole dictionary because they predate
/// the term index (see [`legacy_sidecar_walks_total`]).
static LEGACY_SIDECAR_WALKS_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static LEGACY_SIDECAR_WARNED: std::sync::Once = std::sync::Once::new();

/// How many lookups walked a whole sidecar dictionary because the sidecar
/// predates the term index. Each such sidecar costs O(index size) per query
/// term until compaction or a rebuild rewrites it.
pub fn legacy_sidecar_walks_total() -> u64 {
    LEGACY_SIDECAR_WALKS_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// Find where `target` would be in the dictionary. `None` means it sorts
/// before the first term, so it is absent. A sidecar without a term index is
/// walked from the first term, and counted and reported as such.
fn locate_term<R: Read + Seek>(
    reader: &mut R,
    file_len: u64,
    target: &[u8],
    term_count: u32,
) -> Result<Option<Walk>, String> {
    let whole = Walk {
        offset: HEADER_LEN,
        terms: term_count,
    };
    let tail = FTI_TERM_INDEX_FOOTER_LEN + 8;
    if file_len < HEADER_LEN + 8 + tail {
        // Too short to hold a footer.
        return verified_legacy(reader, file_len, term_count, whole).map(Some);
    }
    reader
        .seek(SeekFrom::End(-(tail as i64)))
        .map_err(|e| format!("seek term index footer: {e}"))?;
    let index_offset = read_u64(reader)?;
    let entry_count = read_u32(reader)?;
    let mut magic = [0u8; 8];
    reader
        .read_exact(&mut magic)
        .map_err(|e| format!("read term index magic: {e}"))?;
    if &magic != FTI_TERM_INDEX_MAGIC {
        return verified_legacy(reader, file_len, term_count, whole).map(Some);
    }
    let index_end = file_len - tail;
    if index_offset < HEADER_LEN + 8 || index_offset > index_end {
        return Err(format!(
            "FTI term index footer points at {index_offset}, outside {}..={index_end}",
            HEADER_LEN + 8
        ));
    }
    let mut section = vec![0u8; (index_end - index_offset) as usize];
    reader
        .seek(SeekFrom::Start(index_offset))
        .map_err(|e| format!("seek term index: {e}"))?;
    reader
        .read_exact(&mut section)
        .map_err(|e| format!("read term index: {e}"))?;
    let entries = parse_term_index(&section, entry_count, index_offset, term_count)?;
    // The last indexed term at or before the target; the target, if present,
    // is within the next FTI_TERM_INDEX_INTERVAL terms.
    let at = entries.partition_point(|e| e.term <= target);
    let Some(entry) = at.checked_sub(1).map(|i| &entries[i]) else {
        return Ok(None);
    };
    Ok(Some(Walk {
        offset: entry.offset,
        terms: (term_count - entry.ordinal).min(FTI_TERM_INDEX_INTERVAL as u32),
    }))
}

/// `whole`, once the file is confirmed to be the legacy layout: its
/// dictionary ends exactly at the 8-byte trailer. A file with an index that
/// lost its tail has no footer either, and walking it would score with
/// whatever bytes now sit where `total_doc_len` was.
fn verified_legacy<R: Read + Seek>(
    reader: &mut R,
    file_len: u64,
    term_count: u32,
    whole: Walk,
) -> Result<Walk, String> {
    let body_end = legacy_dictionary_end(reader, term_count)?;
    if body_end + 8 != file_len {
        return Err(format!(
            "FTI has no term index and {} bytes where its 8-byte trailer belongs: \
             truncated or corrupt",
            file_len.saturating_sub(body_end)
        ));
    }
    Ok(legacy(whole))
}

/// Walk the whole dictionary, skipping postings, and return where it ends.
fn legacy_dictionary_end<R: Read + Seek>(reader: &mut R, term_count: u32) -> Result<u64, String> {
    reader
        .seek(SeekFrom::Start(HEADER_LEN))
        .map_err(|e| format!("seek dictionary: {e}"))?;
    for _ in 0..term_count {
        let term_len = read_u16(reader)? as i64;
        reader
            .seek_relative(term_len + 4)
            .map_err(|e| format!("skip term: {e}"))?;
        let posting_count = read_u32(reader)?;
        for _ in 0..posting_count {
            let pk_len = read_u16(reader)? as i64;
            reader
                .seek_relative(pk_len + 8)
                .map_err(|e| format!("skip posting: {e}"))?;
        }
    }
    reader
        .stream_position()
        .map_err(|e| format!("dictionary end: {e}"))
}

fn legacy(whole: Walk) -> Walk {
    LEGACY_SIDECAR_WALKS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    LEGACY_SIDECAR_WARNED.call_once(|| {
        tracing::warn!(
            "full-text sidecar predates the term index: lookups walk its whole term \
             dictionary until compaction or a rebuild rewrites it; counted in \
             legacy_sidecar_walks_total"
        );
    });
    whole
}

struct IndexEntry<'a> {
    term: &'a [u8],
    ordinal: u32,
    offset: u64,
}

/// Decode the term index, checking it is exactly `entry_count` entries in
/// dictionary order that point inside the term section.
fn parse_term_index(
    section: &[u8],
    entry_count: u32,
    index_offset: u64,
    term_count: u32,
) -> Result<Vec<IndexEntry<'_>>, String> {
    let corrupt = |why: &str| format!("corrupt FTI term index: {why}");
    let mut entries: Vec<IndexEntry<'_>> = Vec::with_capacity(entry_count as usize);
    let mut rest = section;
    for _ in 0..entry_count {
        let (len, after) = rest
            .split_at_checked(2)
            .ok_or_else(|| corrupt("truncated"))?;
        let len = u16::from_le_bytes([len[0], len[1]]) as usize;
        let (term, after) = after
            .split_at_checked(len)
            .ok_or_else(|| corrupt("truncated"))?;
        let (fixed, after) = after
            .split_at_checked(12)
            .ok_or_else(|| corrupt("truncated"))?;
        let ordinal = u32::from_le_bytes(fixed[..4].try_into().expect("4 bytes"));
        let offset = u64::from_le_bytes(fixed[4..].try_into().expect("8 bytes"));
        if ordinal >= term_count || offset < HEADER_LEN || offset >= index_offset {
            return Err(corrupt("entry points outside the dictionary"));
        }
        if entries.last().is_some_and(|prev| {
            prev.term >= term || prev.ordinal >= ordinal || prev.offset >= offset
        }) {
            return Err(corrupt("entries out of order"));
        }
        entries.push(IndexEntry {
            term,
            ordinal,
            offset,
        });
        rest = after;
    }
    if !rest.is_empty() {
        return Err(corrupt("bytes left after the last entry"));
    }
    Ok(entries)
}

fn read_u8(r: &mut impl Read) -> Result<u8, String> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b).map_err(|e| format!("read u8: {e}"))?;
    Ok(b[0])
}

fn read_u16(r: &mut impl Read) -> Result<u16, String> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b).map_err(|e| format!("read u16: {e}"))?;
    Ok(u16::from_le_bytes(b))
}

fn read_u32(r: &mut impl Read) -> Result<u32, String> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b).map_err(|e| format!("read u32: {e}"))?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64(r: &mut impl Read) -> Result<u64, String> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b).map_err(|e| format!("read u64: {e}"))?;
    Ok(u64::from_le_bytes(b))
}

/// Combine per-term hit lists into conjunction (AND) hits, best-first.
///
/// A document survives only if it matched EVERY term; its score is the sum of
/// its per-term scores, which is how BM25 composes across a conjunction.
///
/// # Why this is not per-term top-k
///
/// The tempting shortcut — ask each term for its own top-k and intersect the
/// results — is wrong, and quietly so. The best conjunction hit need not be any
/// single term's best hit: a document ranked 51st for one term and 1st for
/// another can be the only document matching both, and per-term top-k drops it
/// before the intersection ever sees it. The truncation must happen AFTER the
/// intersection, which is why this takes complete per-term lists and why the
/// caller applies `limit` to the result rather than to the inputs.
///
/// Working set is the union of the supplied posting lists — the terms actually
/// queried — not the whole index.
pub fn intersect_conjunction(per_term: Vec<Vec<FtsHit>>) -> Vec<FtsHit> {
    // An empty query matches nothing. Returning "everything" here would turn a
    // degenerate query into a full scan.
    if per_term.is_empty() {
        return Vec::new();
    }

    use std::collections::HashMap;

    // Seed from the first term, collapsing any duplicate posting for the same
    // document so it cannot be counted twice and outrank a document that
    // genuinely matched more terms.
    let mut acc: HashMap<Vec<u8>, f64> = HashMap::new();
    for hit in per_term[0].iter() {
        let entry = acc.entry(hit.partition_key.clone()).or_insert(0.0);
        *entry = entry.max(hit.score);
    }

    for term_hits in per_term.iter().skip(1) {
        if term_hits.is_empty() || acc.is_empty() {
            // A conjunction with an unmatched term matches nothing.
            return Vec::new();
        }
        let mut this_term: HashMap<Vec<u8>, f64> = HashMap::new();
        for hit in term_hits {
            let entry = this_term.entry(hit.partition_key.clone()).or_insert(0.0);
            *entry = entry.max(hit.score);
        }
        acc.retain(|key, score| match this_term.get(key) {
            Some(extra) => {
                *score += extra;
                true
            }
            None => false,
        });
    }

    let mut out: Vec<FtsHit> = acc
        .into_iter()
        .map(|(partition_key, score)| FtsHit {
            partition_key,
            score,
        })
        .collect();

    // Best-first so a later top-k keeps the best documents. Equal scores
    // tie-break on the key, so the order is stable across calls and a truncated
    // or paged result does not reshuffle.
    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.partition_key.cmp(&b.partition_key))
    });
    out
}

#[cfg(test)]
mod conjunction_tests {
    use super::*;

    fn hit(key: &str, score: f64) -> FtsHit {
        FtsHit {
            partition_key: key.as_bytes().to_vec(),
            score,
        }
    }

    fn keys(hits: &[FtsHit]) -> Vec<String> {
        hits.iter()
            .map(|h| String::from_utf8(h.partition_key.clone()).unwrap())
            .collect()
    }

    /// One term: the conjunction is that term's hits.
    #[test]
    fn a_single_term_conjunction_is_that_terms_hits() {
        let out = intersect_conjunction(vec![vec![hit("a", 2.0), hit("b", 1.0)]]);
        assert_eq!(keys(&out), vec!["a", "b"]);
    }

    /// A conjunction keeps only documents that matched EVERY term.
    #[test]
    fn only_documents_matching_every_term_survive() {
        let out = intersect_conjunction(vec![
            vec![hit("a", 1.0), hit("b", 1.0), hit("c", 1.0)],
            vec![hit("b", 1.0), hit("c", 1.0)],
            vec![hit("c", 1.0)],
        ]);
        assert_eq!(keys(&out), vec!["c"]);
    }

    /// Scores add across terms, so a document matching both terms strongly
    /// outranks one that matched both weakly.
    #[test]
    fn scores_sum_across_terms() {
        let out = intersect_conjunction(vec![
            vec![hit("strong", 3.0), hit("weak", 0.5)],
            vec![hit("strong", 4.0), hit("weak", 0.25)],
        ]);
        assert_eq!(keys(&out), vec!["strong", "weak"]);
        assert!((out[0].score - 7.0).abs() < f64::EPSILON);
        assert!((out[1].score - 0.75).abs() < f64::EPSILON);
    }

    /// Results come back best-first, so a later top-k truncation keeps the
    /// best documents rather than an arbitrary slice.
    #[test]
    fn results_are_ordered_best_first() {
        let out = intersect_conjunction(vec![
            vec![hit("low", 0.1), hit("high", 9.0), hit("mid", 1.0)],
            vec![hit("low", 0.1), hit("high", 9.0), hit("mid", 1.0)],
        ]);
        assert_eq!(keys(&out), vec!["high", "mid", "low"]);
    }

    /// Equal scores tie-break on the key, so the order is deterministic and a
    /// paged or truncated result does not reshuffle between calls.
    #[test]
    fn equal_scores_tie_break_deterministically() {
        let a = intersect_conjunction(vec![vec![hit("b", 1.0), hit("a", 1.0), hit("c", 1.0)]]);
        let b = intersect_conjunction(vec![vec![hit("c", 1.0), hit("a", 1.0), hit("b", 1.0)]]);
        assert_eq!(keys(&a), keys(&b));
        assert_eq!(keys(&a), vec!["a", "b", "c"]);
    }

    /// One term matching nothing empties the conjunction — it is an AND.
    #[test]
    fn a_term_with_no_hits_empties_the_conjunction() {
        let out = intersect_conjunction(vec![
            vec![hit("a", 1.0), hit("b", 1.0)],
            vec![],
            vec![hit("a", 1.0)],
        ]);
        assert!(out.is_empty(), "got {:?}", keys(&out));
    }

    /// No terms is not "everything". An empty query matches nothing.
    #[test]
    fn no_terms_matches_nothing() {
        assert!(intersect_conjunction(vec![]).is_empty());
    }

    /// A document repeated inside ONE term's postings must not be counted
    /// twice, or it would outrank documents that genuinely matched more terms.
    #[test]
    fn a_repeated_document_within_one_term_is_not_double_counted() {
        let out = intersect_conjunction(vec![
            vec![hit("a", 2.0), hit("a", 2.0)],
            vec![hit("a", 1.0)],
        ]);
        assert_eq!(keys(&out), vec!["a"]);
        assert!(
            (out[0].score - 3.0).abs() < f64::EPSILON,
            "score was {}, expected 2.0 + 1.0 with the duplicate collapsed",
            out[0].score
        );
    }

    /// The trap this function exists to avoid, stated as a test.
    ///
    /// Per-term top-k then intersect is NOT top-k of the intersection. Here
    /// `mid` is 2nd for both terms and is the ONLY document matching both, so
    /// it is the correct top-1 conjunction hit. A per-term top-1 would have
    /// kept only `x` and `y` and returned NOTHING.
    #[test]
    fn the_best_conjunction_hit_need_not_be_any_terms_best_hit() {
        let term_a = vec![hit("x", 9.0), hit("mid", 5.0)];
        let term_b = vec![hit("y", 9.0), hit("mid", 5.0)];

        let out = intersect_conjunction(vec![term_a, term_b]);

        assert_eq!(
            keys(&out),
            vec!["mid"],
            "intersecting per-term top-1 would have dropped the only real match"
        );
        assert!((out[0].score - 10.0).abs() < f64::EPSILON);
    }
}

/// A sidecar's term dictionary is reachable by seek, not by walking it.
///
/// Live on 2026-10-07: node1's `entity_store` had 16 name-index sidecars
/// totalling 184 MB. Every query term walked every one of them from byte 0, so
/// a five-word `fts_match` read ~900 MB per node and took 18.7 s.
#[cfg(test)]
mod term_index_tests {
    use super::*;
    use std::io::Cursor;
    use std::ops::ControlFlow;

    use crate::fulltext::builder::{serialize_fti_without_term_index, FullTextIndexBuilder};
    use crate::fulltext::reader::deserialize_fti;

    /// Counts the bytes pulled from the underlying source; seeks are free.
    struct Counting<R> {
        inner: R,
        read: std::rc::Rc<std::cell::Cell<u64>>,
    }

    impl<R: Read> Read for Counting<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.inner.read(buf)?;
            self.read.set(self.read.get() + n as u64);
            Ok(n)
        }
    }

    impl<R: Seek> Seek for Counting<R> {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            self.inner.seek(pos)
        }
    }

    /// `n` distinct alphabetic words, so each document adds one term.
    fn word(i: usize) -> String {
        let mut s = String::from("t");
        let mut x = i;
        for _ in 0..4 {
            s.push((b'a' + (x % 26) as u8) as char);
            x /= 26;
        }
        s
    }

    fn sidecar(n: usize) -> Vec<u8> {
        let mut builder = FullTextIndexBuilder::new();
        for i in 0..n {
            builder.add_document(format!("pk{i:06}").into_bytes(), &word(i));
        }
        builder.finish().unwrap()
    }

    /// The terms as stored (after analysis), sorted as the dictionary is.
    fn stored_terms(bytes: &[u8]) -> Vec<String> {
        let mut terms: Vec<String> = deserialize_fti(bytes).unwrap().terms.into_keys().collect();
        terms.sort();
        terms
    }

    /// Search `bytes` for `term`, returning the hit keys and the bytes read.
    fn search(bytes: &[u8], term: &str) -> (Vec<Vec<u8>>, u64) {
        let read = std::rc::Rc::new(std::cell::Cell::new(0));
        let source = Counting {
            inner: Cursor::new(bytes.to_vec()),
            read: std::rc::Rc::clone(&read),
        };
        let mut keys = Vec::new();
        scan_term_each_in(BufReader::new(source), bytes.len() as u64, term, |hit| {
            keys.push(hit.partition_key);
            ControlFlow::Continue(())
        })
        .unwrap();
        keys.sort();
        (keys, read.get())
    }

    #[test]
    fn a_term_lookup_reads_a_bounded_slice_of_the_sidecar_not_all_of_it() {
        let bytes = sidecar(20_000);
        let terms = stored_terms(&bytes);
        // The last term is the linear walk's worst case: every earlier term
        // and posting is read to reach it.
        let last = terms.last().unwrap();
        let (keys, read) = search(&bytes, last);
        assert_eq!(keys.len(), 1, "the last term's one document is found");
        assert!(
            read * 20 < bytes.len() as u64,
            "looking up one term read {read} of {} bytes",
            bytes.len()
        );
    }

    #[test]
    fn an_indexed_lookup_finds_every_term_and_nothing_else() {
        let bytes = sidecar(1_000);
        let legacy = serialize_fti_without_term_index(&deserialize_fti(&bytes).unwrap()).unwrap();
        let terms = stored_terms(&bytes);
        for term in &terms {
            let (indexed, _) = search(&bytes, term);
            let (linear, _) = search(&legacy, term);
            assert_eq!(indexed.len(), 1, "{term} is found");
            assert_eq!(indexed, linear, "{term}: same answer as the linear walk");
        }
        // Absent terms: before the first, between two, after the last.
        for absent in ["a", &format!("{}a", terms[500]), "zzzzzz"] {
            assert!(search(&bytes, absent).0.is_empty(), "{absent} is absent");
        }
    }

    #[test]
    fn a_sidecar_written_before_the_term_index_is_still_searched() {
        let bytes = sidecar(300);
        let legacy = serialize_fti_without_term_index(&deserialize_fti(&bytes).unwrap()).unwrap();
        assert!(legacy.len() < bytes.len(), "the legacy layout has no index");
        for term in stored_terms(&legacy) {
            assert_eq!(search(&legacy, &term).0.len(), 1, "{term} is found");
        }
    }

    /// Losing the footer makes a file look like the legacy layout, with
    /// index bytes where `total_doc_len` belongs. Scoring it would be wrong
    /// without a sign; it is refused.
    #[test]
    fn an_indexed_sidecar_that_lost_its_tail_is_refused_not_scored() {
        let bytes = sidecar(300);
        let term = stored_terms(&bytes)[0].clone();
        for cut in [1, 10, 27, 28, 40] {
            let truncated = &bytes[..bytes.len() - cut];
            let result = scan_term_each_in(
                BufReader::new(Cursor::new(truncated.to_vec())),
                truncated.len() as u64,
                &term,
                |_| ControlFlow::Continue(()),
            );
            assert!(result.is_err(), "cut {cut} bytes: must be refused");
        }
    }

    /// A rollback to a build without the index still reads new sidecars: the
    /// file opens with exactly the old layout.
    #[test]
    fn a_sidecar_with_a_term_index_starts_with_the_old_layout() {
        let bytes = sidecar(300);
        let legacy = serialize_fti_without_term_index(&deserialize_fti(&bytes).unwrap()).unwrap();
        assert_eq!(&bytes[..legacy.len()], legacy.as_slice());
        assert_eq!(
            bytes[bytes.len() - 8..],
            legacy[legacy.len() - 8..],
            "the last 8 bytes are still total_doc_len"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::ControlFlow;

    use crate::fulltext::builder::FullTextIndexBuilder;
    use crate::fulltext::reader::FullTextIndexReader;

    fn write_fti(dir: &Path, docs: &[(&[u8], &str)]) -> std::path::PathBuf {
        let mut builder = FullTextIndexBuilder::new();
        for (pk, text) in docs {
            builder.add_document(pk.to_vec(), text);
        }
        let bytes = builder.finish().unwrap();
        let path = dir.join("gen1-FTI-idx.db");
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn stream_term_matches_reader_search_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let docs: Vec<(Vec<u8>, String)> = (0..200)
            .map(|i| {
                let text = if i % 3 == 0 {
                    format!("memory snippet number {i} memory")
                } else {
                    format!("unrelated filler text {i}")
                };
                (format!("pk{i:04}").into_bytes(), text)
            })
            .collect();
        let doc_refs: Vec<(&[u8], &str)> = docs
            .iter()
            .map(|(pk, t)| (pk.as_slice(), t.as_str()))
            .collect();
        let path = write_fti(dir.path(), &doc_refs);

        let reader = FullTextIndexReader::open(std::fs::read(&path).unwrap()).unwrap();
        let expected = reader.search_str("memory").unwrap();
        let streamed = scan_term_top_k(&path, "memory", None).unwrap();

        assert_eq!(streamed.len(), expected.len(), "same match set size");
        let expected_keys: std::collections::HashSet<_> =
            expected.iter().map(|h| h.partition_key.clone()).collect();
        for hit in &streamed {
            assert!(expected_keys.contains(&hit.partition_key));
            let exp = expected
                .iter()
                .find(|h| h.partition_key == hit.partition_key)
                .unwrap();
            assert!(
                (exp.score - hit.score).abs() < 1e-12,
                "scores must be identical"
            );
        }
    }

    #[test]
    fn stream_term_top_k_returns_k_best_scores() {
        let dir = tempfile::tempdir().unwrap();
        let docs: Vec<(Vec<u8>, String)> = (0..50)
            .map(|i| {
                // Increasing term frequency → strictly increasing BM25 score.
                let text = format!("{} filler", "memory ".repeat(i + 1));
                (format!("pk{i:04}").into_bytes(), text)
            })
            .collect();
        let doc_refs: Vec<(&[u8], &str)> = docs
            .iter()
            .map(|(pk, t)| (pk.as_slice(), t.as_str()))
            .collect();
        let path = write_fti(dir.path(), &doc_refs);

        let reader = FullTextIndexReader::open(std::fs::read(&path).unwrap()).unwrap();
        let full = reader.search_str("memory").unwrap();
        let top5 = scan_term_top_k(&path, "memory", Some(5)).unwrap();

        assert_eq!(top5.len(), 5);
        for (a, b) in top5.iter().zip(full.iter().take(5)) {
            assert_eq!(a.partition_key, b.partition_key, "top-k must be the best k");
            assert!((a.score - b.score).abs() < 1e-12);
        }
    }

    #[test]
    fn scan_term_each_yields_every_match_without_materializing() {
        // The callback walk must visit exactly the same hit set (keys + scores)
        // as `scan_term_top_k(.., None)`, one hit at a time, holding no growing
        // result Vec of its own.
        let dir = tempfile::tempdir().unwrap();
        let docs: Vec<(Vec<u8>, String)> = (0..120)
            .map(|i| {
                let text = if i % 2 == 0 {
                    format!("memory row {i}")
                } else {
                    format!("filler row {i}")
                };
                (format!("pk{i:04}").into_bytes(), text)
            })
            .collect();
        let doc_refs: Vec<(&[u8], &str)> = docs
            .iter()
            .map(|(pk, t)| (pk.as_slice(), t.as_str()))
            .collect();
        let path = write_fti(dir.path(), &doc_refs);

        let expected = scan_term_top_k(&path, "memory", None).unwrap();

        let mut seen: Vec<FtsHit> = Vec::new();
        scan_term_each(&path, "memory", |hit| {
            seen.push(hit);
            ControlFlow::Continue(())
        })
        .unwrap();

        assert_eq!(seen.len(), expected.len(), "same number of matches");
        let expected_keys: std::collections::HashSet<_> =
            expected.iter().map(|h| h.partition_key.clone()).collect();
        for hit in &seen {
            assert!(expected_keys.contains(&hit.partition_key));
            let exp = expected
                .iter()
                .find(|h| h.partition_key == hit.partition_key)
                .unwrap();
            assert!((exp.score - hit.score).abs() < 1e-12, "identical score");
        }
    }

    #[test]
    fn scan_term_each_early_exit_stops_the_walk() {
        // Returning `Break` after the first hit must halt the walk immediately
        // (consumer-paced backpressure): the callback is not invoked again.
        let dir = tempfile::tempdir().unwrap();
        let docs: Vec<(Vec<u8>, String)> = (0..50)
            .map(|i| (format!("pk{i:04}").into_bytes(), "memory".to_string()))
            .collect();
        let doc_refs: Vec<(&[u8], &str)> = docs
            .iter()
            .map(|(pk, t)| (pk.as_slice(), t.as_str()))
            .collect();
        let path = write_fti(dir.path(), &doc_refs);

        let mut count = 0usize;
        scan_term_each(&path, "memory", |_hit| {
            count += 1;
            ControlFlow::Break(())
        })
        .unwrap();

        assert_eq!(count, 1, "walk stops at the first Break");
    }

    #[test]
    fn scan_term_each_truncated_file_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_fti(dir.path(), &[(b"pk1", "hello world")]);
        let bytes = std::fs::read(&path).unwrap();
        let cut = dir.path().join("cut2.db");
        std::fs::write(&cut, &bytes[..bytes.len() / 2]).unwrap();
        let mut count = 0usize;
        let res = scan_term_each(&cut, "hello", |_| {
            count += 1;
            ControlFlow::Continue(())
        });
        assert!(res.is_err());
    }

    #[test]
    fn stream_term_absent_term_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_fti(dir.path(), &[(b"pk1", "hello world")]);
        assert!(scan_term_top_k(&path, "zzz", Some(10)).unwrap().is_empty());
        assert!(scan_term_top_k(&path, "aaa", None).unwrap().is_empty());
    }

    #[test]
    fn stream_term_limit_zero_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_fti(dir.path(), &[(b"pk1", "hello world")]);
        assert!(scan_term_top_k(&path, "hello", Some(0)).unwrap().is_empty());
    }

    #[test]
    fn stream_term_truncated_file_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_fti(dir.path(), &[(b"pk1", "hello world")]);
        let bytes = std::fs::read(&path).unwrap();
        let cut = dir.path().join("cut.db");
        std::fs::write(&cut, &bytes[..bytes.len() / 2]).unwrap();
        assert!(scan_term_top_k(&cut, "hello", Some(10)).is_err());
    }

    #[test]
    fn stream_term_bad_magic_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.db");
        std::fs::write(&path, b"XXXX0000000000000000").unwrap();
        assert!(scan_term_top_k(&path, "hello", Some(10)).is_err());
    }
}
