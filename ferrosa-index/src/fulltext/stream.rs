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
//! * [`scan_term_top_k`] — single-term search (the live-OOM query shape).
//!   Postings of the one matching term are scored as they are decoded and fed
//!   into a bounded top-k heap when the query carries a `LIMIT k`; peak
//!   additional memory is O(k), independent of the index or matching-doc
//!   count. Without a limit the complete hit set is returned (O(matches) —
//!   the result itself, nothing more).
//!
//! The term dictionary is written sorted (see `serialize_fti`), so the walk
//! early-exits as soon as it passes the target term.

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
    let mut topk = limit.map(TopK::new);
    let mut all_hits: Vec<FtsHit> = Vec::new();

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
                    match topk.as_mut() {
                        Some(t) => t.push_owned(pk, score),
                        None => all_hits.push(FtsHit {
                            partition_key: pk,
                            score,
                        }),
                    }
                }
                break; // dictionary is sorted; the term appears once.
            }
            std::cmp::Ordering::Greater => break, // sorted dictionary — passed it.
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;
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
