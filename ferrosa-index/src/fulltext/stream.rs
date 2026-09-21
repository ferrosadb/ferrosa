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

use super::builder::{FTI_MAGIC, FTI_VERSION};
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
pub fn scan_term_each<F>(path: &Path, term: &str, mut on_hit: F) -> Result<(), String>
where
    F: FnMut(FtsHit) -> std::ops::ControlFlow<()>,
{
    let file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let file_len = file
        .metadata()
        .map_err(|e| format!("stat {}: {e}", path.display()))?
        .len();
    let mut reader = BufReader::new(file);

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
    let mut term_buf: Vec<u8> = Vec::new();

    for _ in 0..term_count {
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
