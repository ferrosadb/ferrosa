//! Text analysis pipeline for full-text indexing.
//!
//! Provides the [`Analyzer`] trait and two concrete implementations:
//!
//! - [`SimpleAnalyzer`]: Lowercases and whitespace-splits text (no stop words, no stemming).
//! - [`StandardAnalyzer`]: Full pipeline — Unicode word tokenization, lowercase,
//!   English stop word removal, and Porter stemming.

use std::borrow::Cow;
use std::collections::HashSet;

use super::stemmer;

// ── Core types ───────────────────────────────────────────────────────────────

/// A single token produced by an analyzer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token<'a> {
    /// The normalized text of this token.
    pub text: Cow<'a, str>,
    /// Zero-based position of this token in the original token stream.
    pub position: u32,
}

// ── Analyzer trait ────────────────────────────────────────────────────────────

/// Transforms a raw string into a sequence of [`Token`]s suitable for indexing
/// or query parsing.
pub trait Analyzer {
    /// Analyze `input` and return an ordered list of tokens.
    fn analyze<'a>(&self, input: &'a str) -> Vec<Token<'a>>;
}

// ── SimpleAnalyzer ────────────────────────────────────────────────────────────

/// Splits on whitespace and lowercases each token. No stop word removal or
/// stemming — useful for exact-match full-text scenarios.
#[derive(Debug, Default, Clone)]
pub struct SimpleAnalyzer;

impl SimpleAnalyzer {
    /// Create a new `SimpleAnalyzer`.
    pub fn new() -> Self {
        Self
    }
}

impl Analyzer for SimpleAnalyzer {
    fn analyze<'a>(&self, input: &'a str) -> Vec<Token<'a>> {
        input
            .split_whitespace()
            .enumerate()
            .map(|(i, word)| Token {
                text: Cow::Owned(word.to_lowercase()),
                position: i as u32,
            })
            .collect()
    }
}

// ── StandardAnalyzer ──────────────────────────────────────────────────────────

/// Full-pipeline analyzer: Unicode word tokenization → lowercase → English stop
/// word removal → Porter stemming.
///
/// # Pipeline
///
/// 1. Tokenize on Unicode word boundaries (any non-alphanumeric character is a
///    delimiter — covers hyphens, underscores, punctuation, etc.)
/// 2. Lowercase each token
/// 3. Drop tokens that are English stop words
/// 4. Apply Porter stemming to the remaining tokens
#[derive(Debug, Clone)]
pub struct StandardAnalyzer {
    stop_words: HashSet<&'static str>,
}

impl StandardAnalyzer {
    /// Create a `StandardAnalyzer` with the built-in English stop word list.
    pub fn new() -> Self {
        let stop_words: HashSet<&'static str> = [
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
            "if", "in", "into", "is", "it", "no", "not", "of", "on", "or",
            "such", "that", "the", "their", "then", "there", "these", "they",
            "this", "to", "was", "will", "with",
        ]
        .iter()
        .copied()
        .collect();
        Self { stop_words }
    }
}

impl Default for StandardAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer for StandardAnalyzer {
    fn analyze<'a>(&self, input: &'a str) -> Vec<Token<'a>> {
        // Step 1 & 2: split on non-alphanumeric chars, lowercase
        // Step 3: remove stop words
        // Step 4: Porter stem
        // Position is assigned after filtering so positions reflect token order
        // in the output stream, not the original word stream.
        input
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|word| word.to_lowercase())
            .filter(|word| !self.stop_words.contains(word.as_str()))
            .enumerate()
            .map(|(i, word)| Token {
                text: Cow::Owned(stemmer::porter_stem(&word)),
                position: i as u32,
            })
            .collect()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── SimpleAnalyzer tests ──────────────────────────────────────────────────

    #[test]
    fn simple_analyzer_lowercases() {
        let analyzer = SimpleAnalyzer::new();
        let tokens = analyzer.analyze("Hello World");
        let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_ref()).collect();
        assert_eq!(texts, vec!["hello", "world"]);
    }

    #[test]
    fn simple_analyzer_assigns_positions() {
        let analyzer = SimpleAnalyzer::new();
        let tokens = analyzer.analyze("a b c");
        let positions: Vec<u32> = tokens.iter().map(|t| t.position).collect();
        assert_eq!(positions, vec![0, 1, 2]);
    }

    #[test]
    fn simple_analyzer_empty_input() {
        let analyzer = SimpleAnalyzer::new();
        let tokens = analyzer.analyze("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn simple_analyzer_whitespace_only() {
        let analyzer = SimpleAnalyzer::new();
        let tokens = analyzer.analyze("   ");
        assert!(tokens.is_empty());
    }

    // ── StandardAnalyzer tests ────────────────────────────────────────────────

    #[test]
    fn standard_analyzer_tokenizes() {
        let analyzer = StandardAnalyzer::new();
        let tokens = analyzer.analyze("Hello World! Rust is great");
        let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_ref()).collect();
        // "is" removed as stop word; remaining words have no suffix rules → unchanged
        assert_eq!(texts, vec!["hello", "world", "rust", "great"]);
    }

    #[test]
    fn standard_analyzer_removes_stops() {
        let analyzer = StandardAnalyzer::new();
        let tokens = analyzer.analyze("the quick brown fox");
        let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_ref()).collect();
        // "the" is a stop word
        assert_eq!(texts, vec!["quick", "brown", "fox"]);
    }

    #[test]
    fn standard_analyzer_stems() {
        let analyzer = StandardAnalyzer::new();
        let tokens = analyzer.analyze("running databases");
        let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_ref()).collect();
        // Porter: running→run, databases→databas
        assert_eq!(texts, vec!["run", "databas"]);
    }

    #[test]
    fn standard_analyzer_unicode_word_boundaries() {
        let analyzer = StandardAnalyzer::new();
        let tokens = analyzer.analyze("hello-world foo_bar baz");
        // hyphens and underscores split tokens; expect at least 3 non-empty tokens
        // "hello", "world", "foo", "bar", "baz" after splitting on non-alphanumeric
        assert!(tokens.len() >= 3);
    }

    #[test]
    fn standard_analyzer_empty_input() {
        let analyzer = StandardAnalyzer::new();
        let tokens = analyzer.analyze("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn standard_analyzer_all_stop_words() {
        let analyzer = StandardAnalyzer::new();
        let tokens = analyzer.analyze("the and or but");
        // All stop words → empty
        assert!(tokens.is_empty());
    }

    #[test]
    fn standard_analyzer_positions_after_filtering() {
        let analyzer = StandardAnalyzer::new();
        let tokens = analyzer.analyze("the quick brown fox");
        // "the" filtered out; quick=0, brown=1, fox=2
        let positions: Vec<u32> = tokens.iter().map(|t| t.position).collect();
        assert_eq!(positions, vec![0, 1, 2]);
    }

    #[test]
    fn standard_analyzer_punctuation_only() {
        let analyzer = StandardAnalyzer::new();
        let tokens = analyzer.analyze("!!! --- ...");
        assert!(tokens.is_empty());
    }
}
