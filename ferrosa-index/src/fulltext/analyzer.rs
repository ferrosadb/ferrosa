//! Text analyzers for full-text indexing.
//!
//! An analyzer converts raw text into a sequence of tokens suitable for
//! indexing. The pipeline is: normalize → tokenize → filter (stop words).
//!
//! Provided analyzers:
//! - [`Analyzer::Standard`]: lowercase, split on non-alphanumeric, configurable stop words.
//! - [`Analyzer::Simple`]: lowercase + split on non-alpha characters, no stop words.
//! - [`Analyzer::Keyword`]: treats entire field as one token (no tokenization).

use std::collections::{HashMap, HashSet};

// ── Analyzer ──────────────────────────────────────────────────────────────────

/// The analyzer an index was built with.
///
/// A closed set of variants rather than a trait object. Two reasons:
///
/// 1. The choice of analyzer is part of an index's identity. A query term
///    normalized differently from the documents looks up a posting list that
///    can never exist, so the build side and the query side must be handed the
///    same analyzer *kind* — a comparison a value supports and a `dyn` does not.
/// 2. Every implementation lives in this crate, so closing the set loses no
///    supported configuration, and the pipeline becomes a branch on data the
///    extraction tool can model instead of a vtable it cannot see.
///
/// [`Analyzer::Standard`] carries its stop-word set because stop words are the
/// only configurable behaviour; the other variants are parameterless.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Analyzer {
    /// Lowercase → split on non-alphanumeric → drop stop words.
    Standard { stop_words: HashSet<String> },

    /// Lowercase → split on non-alphabetic characters. No stop-word removal.
    Simple,

    /// The whole field value as a single lowercased token.
    ///
    /// Useful for exact-match fields such as email addresses or product codes.
    Keyword,
}

impl Analyzer {
    /// The default English-language analyzer.
    ///
    /// Pipeline: lowercase → split on non-alphanumeric → remove stop words.
    /// Stop words default to a small English set; override with
    /// [`Analyzer::standard_with_stop_words`].
    pub fn standard() -> Self {
        Analyzer::Standard {
            stop_words: DEFAULT_STOP_WORDS
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
        }
    }

    /// A [`Analyzer::Standard`] with a custom set of stop words.
    pub fn standard_with_stop_words(stop_words: impl IntoIterator<Item = String>) -> Self {
        Analyzer::Standard {
            stop_words: stop_words.into_iter().collect(),
        }
    }

    /// Analyze `text` and return the resulting tokens.
    pub fn analyze(&self, text: &str) -> Vec<String> {
        let mut out = Vec::new();
        self.analyze_into(text, &mut out);
        out
    }

    /// Analyze `text`, appending tokens to `out`.
    ///
    /// **Appends**: the caller is responsible for clearing `out` when it wants
    /// a fresh result. [`super::builder::FullTextIndexBuilder::add_document_with_tf`]
    /// clears on entry and reuses the allocation across rows, so a scan over N
    /// rows does not allocate N token vectors.
    pub fn analyze_into(&self, text: &str, out: &mut Vec<String>) {
        match self {
            Analyzer::Standard { stop_words } => {
                let lowered = text.to_lowercase();
                for token in lowered.split(|c: char| !c.is_alphanumeric()) {
                    if token.is_empty() || stop_words.contains(token) {
                        continue;
                    }
                    out.push(token.to_string());
                }
            }
            Analyzer::Simple => {
                let lowered = text.to_lowercase();
                for token in lowered.split(|c: char| !c.is_alphabetic()) {
                    if token.is_empty() {
                        continue;
                    }
                    out.push(token.to_string());
                }
            }
            Analyzer::Keyword => {
                let trimmed = text.trim().to_lowercase();
                if !trimmed.is_empty() {
                    out.push(trimmed);
                }
            }
        }
    }
}

impl Default for Analyzer {
    fn default() -> Self {
        Analyzer::standard()
    }
}

/// Default English stop words for [`Analyzer::Standard`].
const DEFAULT_STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "been", "but", "by", "for", "from", "has", "he",
    "in", "is", "it", "its", "of", "on", "or", "that", "the", "their", "there", "they", "this",
    "to", "was", "were", "will", "with",
];

// ── Factory ───────────────────────────────────────────────────────────────────

/// The analyzer used when no per-index analyzer option is threaded.
///
/// Both the write side ([`super::builder::FullTextIndexBuilder::new`], document
/// indexing) and the read side ([`super::reader::FullTextIndexReader`], query-
/// term normalization) construct their default analyzer here, so the two sides
/// stay in lock-step. If they diverge, a query term the analyzer would drop or
/// rewrite looks up a posting list that can never exist → a deterministic
/// false-empty result. When a per-index analyzer option is eventually persisted
/// and threaded, both sides must be handed the *same* analyzer kind.
pub fn default_analyzer() -> Analyzer {
    Analyzer::standard()
}

/// Select an analyzer from a CQL index `WITH OPTIONS` map.
///
/// Recognized option keys:
/// - `"analyzer"` → `"standard"` (default), `"simple"`, `"keyword"`
/// - `"stop_words_list"` → comma-separated list of stop words (standard only)
pub fn analyzer_from_options(options: &HashMap<String, String>) -> Analyzer {
    match options.get("analyzer").map(|s| s.as_str()) {
        Some("simple") => Analyzer::Simple,
        Some("keyword") => Analyzer::Keyword,
        _ => {
            // Standard (default) — optionally with custom stop words.
            if let Some(stops) = options.get("stop_words_list") {
                let custom: Vec<String> = stops
                    .split(',')
                    .map(|s| s.trim().to_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect();
                Analyzer::standard_with_stop_words(custom)
            } else {
                Analyzer::standard()
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// The analyzer is a value from a closed set, not a trait object.
    ///
    /// This pins the shape the extraction needs: the build side and the query
    /// side must be handed the same analyzer kind, and choosing it must not
    /// require a heap allocation or a vtable.
    #[test]
    fn analyzer_is_a_value_from_a_closed_set() {
        let keyword = Analyzer::Keyword;
        assert_eq!(
            keyword.analyze("  Hello World  "),
            vec!["hello world".to_string()]
        );

        let simple = Analyzer::Simple;
        assert_eq!(
            simple.analyze("Hello World"),
            vec!["hello".to_string(), "world".to_string()]
        );

        let standard = Analyzer::standard();
        let tokens = standard.analyze("the quick fox");
        assert!(!tokens.contains(&"the".to_string()), "stop word survives");
        assert!(tokens.contains(&"quick".to_string()));
    }

    /// Appending is the contract: the caller clears, the analyzer adds.
    #[test]
    fn analyze_into_appends_without_clearing() {
        let mut out = vec!["sentinel".to_string()];
        Analyzer::Simple.analyze_into("Hello World", &mut out);
        assert_eq!(
            out,
            vec![
                "sentinel".to_string(),
                "hello".to_string(),
                "world".to_string()
            ]
        );
    }

    #[test]
    fn standard_analyzer_lowercases_and_splits() {
        let a = Analyzer::standard();
        let tokens = a.analyze("Hello World");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
    }

    #[test]
    fn standard_analyzer_removes_stop_words() {
        let a = Analyzer::standard();
        let tokens = a.analyze("the quick brown fox");
        assert!(
            !tokens.contains(&"the".to_string()),
            "stop word 'the' should be removed"
        );
        assert!(tokens.contains(&"quick".to_string()));
        assert!(tokens.contains(&"brown".to_string()));
        assert!(tokens.contains(&"fox".to_string()));
    }

    #[test]
    fn fts_custom_stop_words() {
        let options: HashMap<String, String> = [
            ("analyzer".into(), "standard".into()),
            ("stop_words_list".into(), "rust,cargo,crate".into()),
        ]
        .into();
        let analyzer = analyzer_from_options(&options);
        let tokens = analyzer.analyze("Rust is a cargo crate system");
        // Custom stops should be removed.
        assert!(
            !tokens.contains(&"rust".to_string()),
            "custom stop 'rust' must be absent"
        );
        assert!(
            !tokens.contains(&"cargo".to_string()),
            "custom stop 'cargo' must be absent"
        );
        assert!(
            !tokens.contains(&"crate".to_string()),
            "custom stop 'crate' must be absent"
        );
        // Non-stop words must remain.
        assert!(tokens.contains(&"system".to_string()));
    }

    #[test]
    fn fts_language_analyzer() {
        // analyzer=none maps to default (standard); explicitly test keyword for no-stemming.
        let options: HashMap<String, String> = [("analyzer".into(), "keyword".into())].into();
        let analyzer = analyzer_from_options(&options);
        let tokens = analyzer.analyze("Hello World");
        // KeywordAnalysis preserves the whole string as one token.
        assert_eq!(tokens, vec!["hello world".to_string()]);
    }

    #[test]
    fn simple_analyzer_no_stop_words() {
        let a = Analyzer::Simple;
        let tokens = a.analyze("The quick brown fox");
        // "the" is not filtered by the simple analyzer.
        assert!(tokens.contains(&"the".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
    }

    #[test]
    fn keyword_analyzer_single_token() {
        let a = Analyzer::Keyword;
        let tokens = a.analyze("  Hello World  ");
        assert_eq!(tokens, vec!["hello world".to_string()]);
    }

    #[test]
    fn keyword_analyzer_empty() {
        let a = Analyzer::Keyword;
        let tokens = a.analyze("   ");
        assert!(tokens.is_empty());
    }

    #[test]
    fn analyzer_from_options_defaults_to_standard() {
        let options: HashMap<String, String> = HashMap::new();
        let a = analyzer_from_options(&options);
        let tokens = a.analyze("the quick fox");
        // Standard removes "the".
        assert!(!tokens.contains(&"the".to_string()));
    }

    #[test]
    fn analyzer_from_options_simple() {
        let options: HashMap<String, String> = [("analyzer".into(), "simple".into())].into();
        let a = analyzer_from_options(&options);
        let tokens = a.analyze("Hello World");
        assert_eq!(tokens, vec!["hello".to_string(), "world".to_string()]);
    }
}
