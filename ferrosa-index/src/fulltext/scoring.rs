//! BM25 relevance scoring for full-text search.
//!
//! Implements Okapi BM25 (Best Match 25), the industry-standard probabilistic
//! ranking function used by Lucene/Elasticsearch and SAI in Cassandra 5.
//!
//! ## Formula
//!
//! For a query term `q` and document `d`:
//!
//! ```text
//! score(d, q) = IDF(q) * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl/avgdl))
//! ```
//!
//! Where:
//! - `tf`    — term frequency in document
//! - `IDF`   — inverse document frequency: `ln((N - df + 0.5) / (df + 0.5) + 1)`
//! - `dl`    — document length (token count)
//! - `avgdl` — average document length across corpus
//! - `k1`    — term saturation parameter (default 1.2)
//! - `b`     — length normalization parameter (default 0.75)

/// BM25 parameters.
pub struct Bm25Params {
    /// Term saturation factor. Higher values give more weight to TF.
    pub k1: f64,
    /// Length normalization factor. 0 = no normalization, 1 = full normalization.
    pub b: f64,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// Compute BM25 score for one `(query_term, document)` pair.
///
/// # Arguments
///
/// * `tf`    — term frequency in the document.
/// * `df`    — number of documents containing the term.
/// * `n`     — total number of documents in the corpus.
/// * `dl`    — length of the document in tokens.
/// * `avgdl` — average document length across the corpus.
/// * `params` — BM25 tuning parameters (use `Default::default()` for standard values).
///
/// Returns 0.0 when `n == 0` or `df == 0` to avoid divide-by-zero.
pub fn bm25_score(tf: u32, df: u64, n: u64, dl: u32, avgdl: f64, params: &Bm25Params) -> f64 {
    assert!(
        avgdl >= 0.0,
        "avgdl must be non-negative, got {avgdl}"
    );

    // Guard against degenerate inputs before checking df <= n invariant.
    if n == 0 || df == 0 || avgdl == 0.0 {
        return 0.0;
    }

    assert!(
        df <= n,
        "df ({df}) must not exceed n ({n})"
    );

    let tf_f = tf as f64;
    let df_f = df as f64;
    let n_f = n as f64;
    let dl_f = dl as f64;

    // IDF component: smooth variant that avoids negative scores.
    let idf = ((n_f - df_f + 0.5) / (df_f + 0.5) + 1.0).ln();

    // TF normalization component.
    let tf_norm = tf_f * (params.k1 + 1.0)
        / (tf_f + params.k1 * (1.0 - params.b + params.b * dl_f / avgdl));

    idf * tf_norm
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bm25_score_zero_n_returns_zero() {
        let params = Bm25Params::default();
        let score = bm25_score(5, 2, 0, 10, 0.0, &params);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn bm25_score_zero_df_returns_zero() {
        let params = Bm25Params::default();
        let score = bm25_score(5, 0, 100, 10, 15.0, &params);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn bm25_score_higher_tf_gives_higher_score() {
        let params = Bm25Params::default();
        let low = bm25_score(1, 5, 100, 10, 10.0, &params);
        let high = bm25_score(5, 5, 100, 10, 10.0, &params);
        assert!(
            high > low,
            "score with tf=5 ({high}) should exceed score with tf=1 ({low})"
        );
    }

    #[test]
    fn bm25_score_rarer_term_scores_higher() {
        let params = Bm25Params::default();
        // Term appearing in 2/100 docs (rare) vs 50/100 (common).
        let rare = bm25_score(2, 2, 100, 10, 10.0, &params);
        let common = bm25_score(2, 50, 100, 10, 10.0, &params);
        assert!(
            rare > common,
            "rare term ({rare}) should score higher than common term ({common})"
        );
    }

    #[test]
    fn bm25_score_shorter_doc_scores_higher_same_tf() {
        let params = Bm25Params::default();
        // Same tf, same df, but shorter document.
        let short = bm25_score(2, 5, 100, 5, 10.0, &params);
        let long = bm25_score(2, 5, 100, 20, 10.0, &params);
        assert!(
            short > long,
            "shorter doc ({short}) should score higher than longer doc ({long})"
        );
    }

    #[test]
    fn bm25_score_positive_for_matching_term() {
        let params = Bm25Params::default();
        let score = bm25_score(3, 10, 1000, 15, 20.0, &params);
        assert!(score > 0.0, "BM25 score must be positive: got {score}");
    }
}
