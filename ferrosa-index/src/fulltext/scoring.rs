//! BM25 scoring for full-text index queries.
//!
//! BM25 (Best Match 25) is the industry-standard probabilistic relevance
//! ranking function used by Elasticsearch, Lucene, and most modern search
//! engines. It extends TF-IDF with document-length normalization.
//!
//! # Formula
//!
//! ```text
//! BM25(q, d) = Σ IDF(t) * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * |d| / avgdl))
//! ```
//!
//! Where:
//! - `tf` = term frequency in the document
//! - `IDF(t)` = log((N - df + 0.5) / (df + 0.5) + 1)
//! - `N` = total number of documents in the collection
//! - `df` = document frequency of the term
//! - `|d|` = length of the document field
//! - `avgdl` = average document field length
//! - `k1` = term saturation parameter (default 1.2)
//! - `b` = length normalization parameter (default 0.75)

/// Parameters that control BM25 scoring behaviour.
#[derive(Debug, Clone, PartialEq)]
pub struct Bm25Params {
    /// Term frequency saturation parameter. Higher values give more weight to
    /// repeated terms. Typical range: 1.2–2.0. Default: 1.2.
    pub k1: f64,
    /// Field-length normalization factor. 0.0 = no normalization, 1.0 = full
    /// normalization. Default: 0.75.
    pub b: f64,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// Compute the BM25 relevance score for a single term in a single document.
///
/// # Arguments
///
/// - `term_frequency` — how many times the term appears in this document's field
/// - `doc_frequency` — how many documents in the collection contain the term
/// - `total_docs` — total number of documents in the collection
/// - `field_length` — length (in tokens) of the field in this document
/// - `avg_field_length` — average field length across the collection
/// - `params` — BM25 tuning parameters
///
/// # Returns
///
/// A non-negative relevance score. Higher means more relevant.
///
/// # Panics
///
/// Does not panic. Returns 0.0 when `doc_frequency >= total_docs` (IDF would
/// be negative or zero, which BM25+ avoids via the `+1` inside the log).
pub fn bm25_score(
    term_frequency: u32,
    doc_frequency: u32,
    total_docs: u32,
    field_length: u32,
    avg_field_length: f64,
    params: &Bm25Params,
) -> f64 {
    // Guard: trivial cases that would produce meaningless scores.
    if total_docs == 0 || doc_frequency == 0 || term_frequency == 0 {
        return 0.0;
    }

    // Ensure avg_field_length is positive to avoid division by zero.
    let avgdl = if avg_field_length > 0.0 {
        avg_field_length
    } else {
        1.0
    };

    // IDF component: Lucene's smooth IDF variant (BM25+), always >= 0.
    let n = total_docs as f64;
    let df = doc_frequency as f64;
    let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();

    // TF-norm component.
    let tf = term_frequency as f64;
    let dl = field_length as f64;
    let k1 = params.k1;
    let b = params.b;

    let numerator = tf * (k1 + 1.0);
    let denominator = tf + k1 * (1.0 - b + b * dl / avgdl);

    idf * (numerator / denominator)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bm25_single_term() {
        let score = bm25_score(2, 10, 1000, 50, 100.0, &Bm25Params::default());
        assert!(score > 0.0);
    }

    #[test]
    fn bm25_multi_term_ranking() {
        let params = Bm25Params::default();
        // Doc with higher TF should score higher (same collection, same doc length).
        let score_high = bm25_score(5, 10, 1000, 50, 100.0, &params);
        let score_low = bm25_score(1, 10, 1000, 50, 100.0, &params);
        assert!(score_high > score_low);
    }

    #[test]
    fn bm25_rare_term_scores_higher() {
        let params = Bm25Params::default();
        // Rare term (df=2) should score higher than common term (df=500).
        let score_rare = bm25_score(1, 2, 1000, 50, 100.0, &params);
        let score_common = bm25_score(1, 500, 1000, 50, 100.0, &params);
        assert!(score_rare > score_common);
    }

    #[test]
    fn bm25_zero_term_frequency_returns_zero() {
        let score = bm25_score(0, 10, 1000, 50, 100.0, &Bm25Params::default());
        assert_eq!(score, 0.0);
    }

    #[test]
    fn bm25_zero_total_docs_returns_zero() {
        let score = bm25_score(1, 1, 0, 50, 100.0, &Bm25Params::default());
        assert_eq!(score, 0.0);
    }

    #[test]
    fn bm25_long_doc_scores_lower_than_short_doc() {
        let params = Bm25Params::default();
        // Same TF, but long doc should score lower after length normalization.
        let score_short = bm25_score(2, 10, 1000, 20, 100.0, &params);
        let score_long = bm25_score(2, 10, 1000, 200, 100.0, &params);
        assert!(score_short > score_long);
    }

    #[test]
    fn bm25_no_length_normalization_when_b_zero() {
        let params = Bm25Params { k1: 1.2, b: 0.0 };
        // With b=0, field length has no effect — scores must be equal.
        let score_short = bm25_score(2, 10, 1000, 20, 100.0, &params);
        let score_long = bm25_score(2, 10, 1000, 200, 100.0, &params);
        let diff = (score_short - score_long).abs();
        assert!(diff < 1e-12, "b=0 should eliminate length penalty, diff={diff}");
    }
}
