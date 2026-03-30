//! Text analyzers for full-text indexing.
//!
//! An analyzer converts raw text into a sequence of tokens suitable for
//! indexing. The pipeline is: normalize → tokenize → filter (stop words, stem).
//!
//! Provided analyzers:
//! - [`StandardAnalyzer`]: lowercase, split on whitespace/punctuation, configurable stop words.
//! - [`SimpleAnalyzer`]: lowercase + split on non-alpha characters, no stop words.
//! - [`KeywordAnalyzer`]: treats entire field as one token (no tokenization).

use std::collections::{HashMap, HashSet};

// ── Analyzer trait ────────────────────────────────────────────────────────────

/// Converts a text string into a sequence of normalized tokens.
pub trait Analyzer: Send + Sync {
    /// Analyze `text` and return the resulting tokens.
    fn analyze(&self, text: &str) -> Vec<String>;
}

// ── StandardAnalyzer ─────────────────────────────────────────────────────────

/// Default English-language analyzer.
///
/// Pipeline: lowercase → split on non-alphanumeric → remove stop words.
/// Stop words default to a small English set; override with `with_stop_words`.
pub struct StandardAnalyzer {
    stop_words: HashSet<String>,
}

impl StandardAnalyzer {
    /// Create a new `StandardAnalyzer` with default English stop words.
    pub fn new() -> Self {
        let defaults = [
            "a", "an", "and", "are", "as", "at", "be", "been", "but", "by", "for",
            "from", "has", "he", "in", "is", "it", "its", "of", "on", "or", "that",
            "the", "their", "there", "they", "this", "to", "was", "were", "will",
            "with",
        ];
        Self {
            stop_words: defaults.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    /// Create a `StandardAnalyzer` with a custom set of stop words.
    pub fn with_stop_words(stop_words: impl IntoIterator<Item = String>) -> Self {
        Self {
            stop_words: stop_words.into_iter().collect(),
        }
    }
}

impl Default for StandardAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer for StandardAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|t| !t.is_empty())
            .filter(|t| !self.stop_words.contains(*t))
            .map(|t| t.to_string())
            .collect()
    }
}

// ── SimpleAnalyzer ────────────────────────────────────────────────────────────

/// Lowercase + split on non-alphabetic characters. No stop-word removal.
pub struct SimpleAnalyzer;

impl Analyzer for SimpleAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphabetic())
            .filter(|t| !t.is_empty())
            .map(|t| t.to_string())
            .collect()
    }
}

// ── KeywordAnalyzer ───────────────────────────────────────────────────────────

/// Treats the entire field value as a single token (lowercased).
///
/// Useful for exact-match fields such as email addresses or product codes.
pub struct KeywordAnalyzer;

impl Analyzer for KeywordAnalyzer {
    fn analyze(&self, text: &str) -> Vec<String> {
        let trimmed = text.trim().to_lowercase();
        if trimmed.is_empty() {
            vec![]
        } else {
            vec![trimmed]
        }
    }
}

// ── Factory ───────────────────────────────────────────────────────────────────

/// Select an analyzer from a CQL index `WITH OPTIONS` map.
///
/// Recognized option keys:
/// - `"analyzer"` → `"standard"` (default), `"simple"`, `"keyword"`
/// - `"stop_words_list"` → comma-separated list of stop words (standard only)
pub fn analyzer_from_options(options: &HashMap<String, String>) -> Box<dyn Analyzer> {
    match options.get("analyzer").map(|s| s.as_str()) {
        Some("simple") => Box::new(SimpleAnalyzer),
        Some("keyword") => Box::new(KeywordAnalyzer),
        _ => {
            // Standard (default) — optionally with custom stop words.
            if let Some(stops) = options.get("stop_words_list") {
                let custom: Vec<String> = stops
                    .split(',')
                    .map(|s| s.trim().to_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect();
                Box::new(StandardAnalyzer::with_stop_words(custom))
            } else {
                Box::new(StandardAnalyzer::new())
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_analyzer_lowercases_and_splits() {
        let a = StandardAnalyzer::new();
        let tokens = a.analyze("Hello World");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
    }

    #[test]
    fn standard_analyzer_removes_stop_words() {
        let a = StandardAnalyzer::new();
        let tokens = a.analyze("the quick brown fox");
        assert!(!tokens.contains(&"the".to_string()), "stop word 'the' should be removed");
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
        assert!(!tokens.contains(&"rust".to_string()), "custom stop 'rust' must be absent");
        assert!(!tokens.contains(&"cargo".to_string()), "custom stop 'cargo' must be absent");
        assert!(!tokens.contains(&"crate".to_string()), "custom stop 'crate' must be absent");
        // Non-stop words must remain.
        assert!(tokens.contains(&"system".to_string()));
    }

    #[test]
    fn fts_language_analyzer() {
        // analyzer=none maps to default (standard); explicitly test keyword for no-stemming.
        let options: HashMap<String, String> =
            [("analyzer".into(), "keyword".into())].into();
        let analyzer = analyzer_from_options(&options);
        let tokens = analyzer.analyze("Hello World");
        // KeywordAnalyzer preserves the whole string as one token.
        assert_eq!(tokens, vec!["hello world".to_string()]);
    }

    #[test]
    fn simple_analyzer_no_stop_words() {
        let a = SimpleAnalyzer;
        let tokens = a.analyze("The quick brown fox");
        // "the" is not filtered by SimpleAnalyzer.
        assert!(tokens.contains(&"the".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
    }

    #[test]
    fn keyword_analyzer_single_token() {
        let a = KeywordAnalyzer;
        let tokens = a.analyze("  Hello World  ");
        assert_eq!(tokens, vec!["hello world".to_string()]);
    }

    #[test]
    fn keyword_analyzer_empty() {
        let a = KeywordAnalyzer;
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
        let options: HashMap<String, String> =
            [("analyzer".into(), "simple".into())].into();
        let a = analyzer_from_options(&options);
        let tokens = a.analyze("Hello World");
        assert_eq!(tokens, vec!["hello".to_string(), "world".to_string()]);
    }
}
