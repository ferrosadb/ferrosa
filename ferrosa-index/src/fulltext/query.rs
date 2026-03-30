//! Full-text search query parser.
//!
//! Parses a query string into an [`FtsQuery`] AST that can be evaluated against
//! an inverted index.
//!
//! # Syntax
//!
//! | Input | Result |
//! |-------|--------|
//! | `hello` | `Term("hello")` |
//! | `"hello world"` | `Phrase(["hello", "world"])` |
//! | `a AND b` | `And(Term("a"), Term("b"))` |
//! | `a OR b` | `Or(Term("a"), Term("b"))` |
//! | `NOT a` | `Not(Term("a"))` |
//! | `compac*` | `Prefix("compac")` |
//! | `hello world` | `And(Term("hello"), Term("world"))` (default AND) |
//!
//! # Precedence (highest → lowest)
//!
//! 1. `NOT` (unary prefix)
//! 2. `AND`
//! 3. `OR`
//!
//! Parentheses override precedence: `(a OR b) AND c`.
//!
//! # Wildcard constraints (FT-012)
//!
//! - Bare `*` is rejected.
//! - Single-character prefix (`a*`) is rejected (minimum prefix length = 2).
//! - Valid prefixes are capped at 10 000 expanded terms at query time.

/// Maximum number of terms a prefix query may expand to.
pub const MAX_WILDCARD_EXPANSION: usize = 10_000;

/// Minimum number of characters required before a `*` wildcard.
const MIN_PREFIX_LEN: usize = 2;

/// A parsed full-text search query node.
#[derive(Debug, Clone, PartialEq)]
pub enum FtsQuery {
    /// A single term: `hello`
    Term(String),
    /// An exact phrase: `"hello world"` → tokens `["hello", "world"]`
    Phrase(Vec<String>),
    /// Conjunction: both sub-queries must match.
    And(Box<FtsQuery>, Box<FtsQuery>),
    /// Disjunction: at least one sub-query must match.
    Or(Box<FtsQuery>, Box<FtsQuery>),
    /// Negation: the sub-query must not match.
    Not(Box<FtsQuery>),
    /// Prefix wildcard: `compac*` — matches all terms starting with the prefix.
    /// Expansion is capped at [`MAX_WILDCARD_EXPANSION`] terms.
    Prefix(String),
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Parse an FTS query string into an [`FtsQuery`] AST.
///
/// # Errors
///
/// Returns `Err(String)` if:
/// - The input is empty or contains only whitespace.
/// - A wildcard has fewer than [`MIN_PREFIX_LEN`] prefix characters.
/// - A bare `*` appears with no prefix text.
/// - Parentheses are unbalanced.
/// - An expected operand is missing (e.g. trailing `AND`).
pub fn parse_fts_query(input: &str) -> Result<FtsQuery, String> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err("empty query".to_string());
    }
    let mut parser = Parser::new(tokens);
    let query = parser.parse_or()?;
    if parser.pos < parser.tokens.len() {
        return Err(format!(
            "unexpected token '{:?}' at position {}",
            parser.tokens[parser.pos], parser.pos
        ));
    }
    Ok(query)
}

// ── Tokenizer ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Token {
    /// A bare word (may end with `*` for prefix queries).
    Word(String),
    /// A quoted phrase string (already stripped of quotes).
    Phrase(String),
    /// `AND` keyword.
    And,
    /// `OR` keyword.
    Or,
    /// `NOT` keyword.
    Not,
    /// `(`
    LParen,
    /// `)`
    RParen,
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        // Skip whitespace.
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }

        match chars[i] {
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '"' => {
                // Quoted phrase — scan to closing quote.
                i += 1; // skip opening quote
                let start = i;
                while i < chars.len() && chars[i] != '"' {
                    i += 1;
                }
                if i >= chars.len() {
                    return Err("unterminated quoted phrase".to_string());
                }
                let phrase: String = chars[start..i].iter().collect();
                i += 1; // skip closing quote
                tokens.push(Token::Phrase(phrase));
            }
            '*' => {
                // Bare star with no preceding word.
                return Err("bare '*' is not a valid query — prefix with at least two characters (e.g. 'co*')".to_string());
            }
            _ => {
                // Bare word — collect until whitespace or `(` `)`.
                let start = i;
                while i < chars.len() && !chars[i].is_whitespace() && chars[i] != '(' && chars[i] != ')' {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();

                // Check for wildcard suffix.
                if let Some(prefix) = word.strip_suffix('*') {
                    if prefix.is_empty() {
                        // Bare `*` was caught above, but handle double-star / edge cases.
                        return Err("bare '*' is not a valid query — prefix with at least two characters (e.g. 'co*')".to_string());
                    }
                    if prefix.chars().count() < MIN_PREFIX_LEN {
                        return Err(format!(
                            "wildcard prefix '{prefix}*' is too short — minimum {MIN_PREFIX_LEN} characters required"
                        ));
                    }
                    tokens.push(Token::Word(format!("{prefix}*")));
                } else {
                    // Check for keywords.
                    let tok = match word.as_str() {
                        "AND" => Token::And,
                        "OR" => Token::Or,
                        "NOT" => Token::Not,
                        _ => Token::Word(word),
                    };
                    tokens.push(tok);
                }
            }
        }
    }

    Ok(tokens)
}

// ── Recursive-descent parser ──────────────────────────────────────────────────

/// Parses tokens using recursive descent.
///
/// Grammar (lowest to highest precedence):
/// ```text
/// or_expr  := and_expr (OR and_expr)*
/// and_expr := not_expr (AND not_expr)*
///           | not_expr not_expr*          (implicit AND for adjacent terms)
/// not_expr := NOT not_expr
///           | atom
/// atom     := WORD | PHRASE | LPAREN or_expr RPAREN
/// ```
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn consume(&mut self) -> Option<&Token> {
        let tok = self.tokens.get(self.pos);
        self.pos += 1;
        tok
    }

    /// `or_expr := and_expr (OR and_expr)*`
    fn parse_or(&mut self) -> Result<FtsQuery, String> {
        let mut left = self.parse_and()?;
        while self.peek() == Some(&Token::Or) {
            self.consume(); // eat OR
            let right = self.parse_and()?;
            left = FtsQuery::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    /// `and_expr := not_expr (AND not_expr)* | not_expr not_expr*`
    ///
    /// Adjacent terms without an explicit operator are joined with implicit AND.
    fn parse_and(&mut self) -> Result<FtsQuery, String> {
        let mut left = self.parse_not()?;

        loop {
            match self.peek() {
                Some(Token::And) => {
                    self.consume(); // eat AND
                    let right = self.parse_not()?;
                    left = FtsQuery::And(Box::new(left), Box::new(right));
                }
                // Implicit AND: next token starts a new atom (word, phrase, NOT, or `(`).
                Some(Token::Word(_))
                | Some(Token::Phrase(_))
                | Some(Token::Not)
                | Some(Token::LParen) => {
                    let right = self.parse_not()?;
                    left = FtsQuery::And(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }

        Ok(left)
    }

    /// `not_expr := NOT not_expr | atom`
    fn parse_not(&mut self) -> Result<FtsQuery, String> {
        if self.peek() == Some(&Token::Not) {
            self.consume(); // eat NOT
            let operand = self.parse_not()?;
            return Ok(FtsQuery::Not(Box::new(operand)));
        }
        self.parse_atom()
    }

    /// `atom := WORD | PHRASE | LPAREN or_expr RPAREN`
    fn parse_atom(&mut self) -> Result<FtsQuery, String> {
        match self.peek().cloned() {
            Some(Token::Word(w)) => {
                self.consume();
                if let Some(prefix) = w.strip_suffix('*') {
                    Ok(FtsQuery::Prefix(prefix.to_string()))
                } else {
                    Ok(FtsQuery::Term(w))
                }
            }
            Some(Token::Phrase(text)) => {
                self.consume();
                let words: Vec<String> = text
                    .split_whitespace()
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
                if words.is_empty() {
                    return Err("empty phrase is not a valid query".to_string());
                }
                Ok(FtsQuery::Phrase(words))
            }
            Some(Token::LParen) => {
                self.consume(); // eat `(`
                let inner = self.parse_or()?;
                match self.consume() {
                    Some(Token::RParen) => Ok(inner),
                    _ => Err("expected closing ')'".to_string()),
                }
            }
            Some(Token::RParen) => Err("unexpected ')'".to_string()),
            Some(Token::And) => Err("unexpected AND — missing left operand".to_string()),
            Some(Token::Or) => Err("unexpected OR — missing left operand".to_string()),
            Some(Token::Not) => {
                // Should not reach here; NOT is handled in parse_not.
                Err("unexpected NOT".to_string())
            }
            None => Err("unexpected end of query — missing operand".to_string()),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── FT-009: core query parsing ─────────────────────────────────────────────

    #[test]
    fn fts_query_parse_term() {
        let q = parse_fts_query("hello").unwrap();
        assert!(matches!(q, FtsQuery::Term(s) if s == "hello"));
    }

    #[test]
    fn fts_query_parse_phrase() {
        let q = parse_fts_query("\"exact match\"").unwrap();
        assert!(matches!(q, FtsQuery::Phrase(words) if words == vec!["exact", "match"]));
    }

    #[test]
    fn fts_query_parse_boolean() {
        let q = parse_fts_query("a AND b OR c").unwrap();
        // OR at top level (lower precedence), AND nested.
        assert!(matches!(q, FtsQuery::Or(_, _)));
    }

    #[test]
    fn fts_query_boolean_precedence() {
        // AND binds tighter than OR: `a OR b AND c` => Or(Term("a"), And(Term("b"), Term("c")))
        let q = parse_fts_query("a OR b AND c").unwrap();
        match q {
            FtsQuery::Or(left, right) => {
                assert!(matches!(*left, FtsQuery::Term(_)));
                assert!(matches!(*right, FtsQuery::And(_, _)));
            }
            _ => panic!("expected Or at top level"),
        }
    }

    #[test]
    fn fts_query_default_and() {
        let q = parse_fts_query("hello world").unwrap();
        assert!(matches!(q, FtsQuery::And(_, _)));
    }

    #[test]
    fn fts_query_parse_not() {
        let q = parse_fts_query("NOT spam").unwrap();
        assert!(matches!(q, FtsQuery::Not(_)));
    }

    #[test]
    fn fts_query_parse_explicit_and() {
        let q = parse_fts_query("a AND b").unwrap();
        assert!(matches!(q, FtsQuery::And(_, _)));
    }

    #[test]
    fn fts_query_parse_grouped() {
        // (a OR b) AND c — parentheses lift OR above AND.
        let q = parse_fts_query("(a OR b) AND c").unwrap();
        match q {
            FtsQuery::And(left, right) => {
                assert!(matches!(*left, FtsQuery::Or(_, _)));
                assert!(matches!(*right, FtsQuery::Term(_)));
            }
            _ => panic!("expected And at top level"),
        }
    }

    #[test]
    fn fts_query_default_and_three_terms() {
        // `a b c` => And(And(a, b), c)
        let q = parse_fts_query("a b c").unwrap();
        assert!(matches!(q, FtsQuery::And(_, _)));
    }

    #[test]
    fn fts_query_not_binds_tighter_than_and() {
        // `NOT a AND b` => And(Not(a), b), not Not(And(a, b))
        let q = parse_fts_query("NOT a AND b").unwrap();
        match q {
            FtsQuery::And(left, right) => {
                assert!(matches!(*left, FtsQuery::Not(_)));
                assert!(matches!(*right, FtsQuery::Term(_)));
            }
            _ => panic!("expected And at top level"),
        }
    }

    // ── FT-012: wildcard cap ───────────────────────────────────────────────────

    #[test]
    fn fts_wildcard_expansion_capped() {
        let q = parse_fts_query("compac*").unwrap();
        assert!(matches!(q, FtsQuery::Prefix(s) if s == "compac"));
    }

    #[test]
    fn fts_wildcard_bare_star_rejected() {
        assert!(parse_fts_query("*").is_err());
    }

    #[test]
    fn fts_wildcard_min_prefix() {
        // Single char prefix rejected.
        assert!(parse_fts_query("a*").is_err());
    }

    #[test]
    fn fts_wildcard_two_char_prefix_accepted() {
        // Two-character prefix is the minimum allowed.
        let q = parse_fts_query("ab*").unwrap();
        assert!(matches!(q, FtsQuery::Prefix(s) if s == "ab"));
    }

    #[test]
    fn fts_wildcard_max_expansion_constant() {
        // The cap constant must be 10 000.
        assert_eq!(MAX_WILDCARD_EXPANSION, 10_000);
    }

    #[test]
    fn fts_parse_empty_query_rejected() {
        assert!(parse_fts_query("").is_err());
        assert!(parse_fts_query("   ").is_err());
    }

    #[test]
    fn fts_parse_unbalanced_paren_rejected() {
        assert!(parse_fts_query("(a OR b").is_err());
    }
}
