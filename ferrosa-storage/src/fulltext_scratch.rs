//! Reusable per-row scratch for the fulltext memtable index build.
//!
//! The build walks every row of the active and flushing memtables and, for each,
//! concatenates the indexed column's text, analyzes it into tokens, and folds a
//! term-frequency map. Done naively that is three fresh allocations per row (a
//! `String`, a `Vec<String>`, a `HashMap`) plus one `String` per token, on a path
//! that can run over a hundred thousand rows per query — and this path is
//! rebuilt per query, not cached.
//!
//! [`RowScratch`] holds those three buffers and **clears** them between rows
//! rather than reallocating, so the per-row cost is the token strings (which
//! must be owned to key the term map) and nothing else. Capacity is retained, so
//! the buffers converge on the widest row seen and stop growing.
//!
//! This is the "pre-allocated, reused buffer" discipline: bounded (cleared, not
//! grown per row), and it never materializes the memtable — the caller still
//! processes one partition and one row at a time.

use std::collections::HashMap;

/// Reusable buffers for one row's text, tokens and term frequencies.
///
/// Invariant: [`RowScratch::reset`] must be called before each row's text is
/// pushed. `tokens` and `tf` are cleared for you inside
/// `FullTextIndexBuilder::add_document_with_tf`; `text` is cleared by `reset`.
pub struct RowScratch {
    /// The indexed column's concatenated text for the current row.
    text: String,
    /// The current row's tokens. Owned strings because they key the term map.
    pub tokens: Vec<String>,
    /// The current row's term counts.
    pub tf: HashMap<String, u32>,
}

impl RowScratch {
    pub fn new() -> Self {
        Self {
            text: String::new(),
            tokens: Vec::new(),
            tf: HashMap::new(),
        }
    }

    /// Clear the text buffer for a new row. Capacity is retained.
    pub fn reset(&mut self) {
        self.text.clear();
    }

    /// Append one indexed cell's text, with the separating space the previous
    /// implementation pushed after each value.
    ///
    /// The space is what makes adjacent cells' words distinct; dropping it would
    /// join the last word of one cell to the first of the next and index a term
    /// that is not in the data.
    pub fn push_text(&mut self, s: &str) {
        self.text.push_str(s);
        self.text.push(' ');
    }

    /// Whether any text was accumulated for this row.
    pub fn has_text(&self) -> bool {
        !self.text.trim().is_empty()
    }

    /// The accumulated text, trimmed — matching the previous `text.trim()`
    /// before indexing. The trailing separator space is removed here rather than
    /// by not pushing it, so the concatenation is identical for every row.
    pub fn text_trimmed(&self) -> &str {
        self.text.trim()
    }
}

impl Default for RowScratch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_text_separates_cells_and_trims_to_the_same_text() {
        // Mirrors the previous per-row `String` + `push_str` + `push(' ')`.
        let mut s = RowScratch::new();
        s.push_text("hello");
        s.push_text("world");
        assert_eq!(s.text_trimmed(), "hello world");
        assert!(s.has_text());
    }

    #[test]
    fn reset_clears_text_but_keeps_capacity() {
        let mut s = RowScratch::new();
        s.push_text("some longer text to grow the buffer");
        let cap = s.text.capacity();
        s.reset();
        assert!(!s.has_text(), "reset must clear the text");
        assert_eq!(s.text.capacity(), cap, "reset must retain capacity");
    }

    #[test]
    fn whitespace_only_text_is_not_indexed() {
        let mut s = RowScratch::new();
        s.push_text("   ");
        assert!(
            !s.has_text(),
            "a whitespace-only row must not become a document"
        );
    }
}
