//! Full-text search query parsing.
//!
//! Parses a CQL `column = fts_match(query_string)` query string into an
//! [`FtsQuery`] enum. The query language supports:
//!
//! - **Phrase queries**: `"hello world"` — match documents containing the exact phrase.
//! - **Boolean AND**: `rust AND cargo` — both terms must be present.
//! - **Boolean OR**: `rust OR cargo` — either term must be present.
//! - **Term queries**: `rust` — a single token (analyzed before search).
//!
//! Parsing is case-insensitive for the `AND`/`OR` operators.

/// A parsed full-text search query.
#[derive(Debug, Clone, PartialEq)]
pub enum FtsQuery {
    /// A single analyzed term.
    Term(String),
    /// An exact phrase (sequence of tokens in order).
    Phrase(Vec<String>),
    /// Both sub-queries must match.
    And(Box<FtsQuery>, Box<FtsQuery>),
    /// Either sub-query must match.
    Or(Box<FtsQuery>, Box<FtsQuery>),
    /// A conjunction of multiple terms (from a plain multi-word query).
    MultiTerm(Vec<String>),
    /// Prefix wildcard: matches any term starting with the given prefix.
    Prefix(String),
    /// Negation: exclude documents matching the inner query.
    Not(Box<FtsQuery>),
}

/// Parse a query string into an [`FtsQuery`].
///
/// Grammar (simplified):
/// ```text
/// query     = expr (("AND" | "OR") expr)*
/// expr      = '"' phrase '"' | term
/// phrase    = word+
/// term      = [^ ]+
/// ```
///
/// # Errors
///
/// Returns `Err(String)` if the query is empty or malformed (e.g., unclosed
/// quoted phrase).
pub fn parse_fts_query(query: &str) -> Result<FtsQuery, String> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Err("query string must not be empty".into());
    }

    // Tokenize respecting quoted phrases.
    let raw_tokens = tokenize_query(trimmed)?;

    if raw_tokens.is_empty() {
        return Err("query contains no searchable tokens".into());
    }

    // Build the expression tree from raw tokens.
    build_expr(&raw_tokens)
}

// ── Internal tokenizer ────────────────────────────────────────────────────────

/// A raw token produced by the query tokenizer.
#[derive(Debug, PartialEq)]
enum RawToken {
    /// A plain word or operator keyword.
    Word(String),
    /// A quoted phrase: already split into its constituent words.
    Phrase(Vec<String>),
    /// The `AND` boolean operator.
    And,
    /// The `OR` boolean operator.
    Or,
    /// The `NOT` boolean operator.
    Not,
}

/// Tokenize a query string, respecting double-quoted phrases.
fn tokenize_query(input: &str) -> Result<Vec<RawToken>, String> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&c) = chars.peek() {
        match c {
            ' ' | '\t' => {
                chars.next();
            }
            '"' => {
                chars.next(); // consume opening quote
                let mut phrase_words = Vec::new();
                let mut word = String::new();
                let mut closed = false;
                for ch in chars.by_ref() {
                    if ch == '"' {
                        if !word.is_empty() {
                            phrase_words.push(word.to_lowercase());
                        }
                        closed = true;
                        break;
                    } else if ch == ' ' || ch == '\t' {
                        if !word.is_empty() {
                            phrase_words.push(word.to_lowercase());
                            word = String::new();
                        }
                    } else {
                        word.push(ch);
                    }
                }
                if !closed {
                    return Err("unclosed quoted phrase in query".into());
                }
                if phrase_words.is_empty() {
                    return Err("empty quoted phrase in query".into());
                }
                tokens.push(RawToken::Phrase(phrase_words));
            }
            _ => {
                // Collect a word.
                let mut word = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch == ' ' || ch == '\t' || ch == '"' {
                        break;
                    }
                    word.push(ch);
                    chars.next();
                }
                if !word.is_empty() {
                    match word.to_uppercase().as_str() {
                        "AND" => tokens.push(RawToken::And),
                        "OR" => tokens.push(RawToken::Or),
                        "NOT" => tokens.push(RawToken::Not),
                        "*" => {
                            return Err(
                                "bare wildcard '*' is not allowed; use a prefix like 'term*'"
                                    .into(),
                            )
                        }
                        _ => {
                            let lower = word.to_lowercase();
                            if lower.ends_with('*') && lower.len() > 1 {
                                // Prefix wildcard: "term*" → Prefix("term")
                                tokens.push(RawToken::Word(format!(
                                    "{}*",
                                    &lower[..lower.len() - 1]
                                )));
                            } else {
                                tokens.push(RawToken::Word(lower));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(tokens)
}

/// Build an `FtsQuery` expression tree from raw tokens.
///
/// Handles `AND`/`OR` operators with left-associativity. Plain adjacent
/// terms without operators are emitted as a `MultiTerm` (implicit AND).
fn build_expr(tokens: &[RawToken]) -> Result<FtsQuery, String> {
    // Check for boolean operators.
    // Find the rightmost OR (lowest precedence), then AND, then NOT (highest).
    let or_pos = tokens.iter().rposition(|t| t == &RawToken::Or);
    if let Some(pos) = or_pos {
        let left = build_expr(&tokens[..pos])?;
        let right = build_expr(&tokens[pos + 1..])?;
        return Ok(FtsQuery::Or(Box::new(left), Box::new(right)));
    }

    let and_pos = tokens.iter().rposition(|t| t == &RawToken::And);
    if let Some(pos) = and_pos {
        let left = build_expr(&tokens[..pos])?;
        let right = build_expr(&tokens[pos + 1..])?;
        return Ok(FtsQuery::And(Box::new(left), Box::new(right)));
    }

    // NOT is a unary prefix operator — must be at the start.
    if let Some(RawToken::Not) = tokens.first() {
        if tokens.len() < 2 {
            return Err("NOT operator requires an operand".into());
        }
        let inner = build_expr(&tokens[1..])?;
        return Ok(FtsQuery::Not(Box::new(inner)));
    }

    // No operators — collect terms/phrases.
    let mut terms = Vec::new();
    let mut phrases = Vec::new();
    for tok in tokens {
        match tok {
            RawToken::Word(w) => terms.push(w.clone()),
            RawToken::Phrase(words) => phrases.push(words.clone()),
            RawToken::And | RawToken::Or | RawToken::Not => {
                return Err("unexpected operator token in expression".into())
            }
        }
    }

    if !phrases.is_empty() && terms.is_empty() && phrases.len() == 1 {
        return Ok(FtsQuery::Phrase(phrases.remove(0)));
    }

    if !phrases.is_empty() {
        // Mix of phrases and terms — wrap in And chain.
        let mut queries: Vec<FtsQuery> = phrases.into_iter().map(FtsQuery::Phrase).collect();
        queries.extend(terms.into_iter().map(FtsQuery::Term));
        return Ok(queries
            .into_iter()
            .reduce(|a, b| FtsQuery::And(Box::new(a), Box::new(b)))
            .unwrap());
    }

    match terms.len() {
        0 => Err("empty expression in query".into()),
        1 => {
            let t = terms.remove(0);
            if t.ends_with('*') {
                Ok(FtsQuery::Prefix(t[..t.len() - 1].to_string()))
            } else {
                Ok(FtsQuery::Term(t))
            }
        }
        _ => {
            // Convert any prefix-wildcard terms in the list.
            let queries: Vec<FtsQuery> = terms
                .into_iter()
                .map(|t| {
                    if t.ends_with('*') {
                        FtsQuery::Prefix(t[..t.len() - 1].to_string())
                    } else {
                        FtsQuery::Term(t)
                    }
                })
                .collect();
            // Check if any are prefix queries — if so, wrap in And chain.
            if queries.iter().any(|q| matches!(q, FtsQuery::Prefix(_))) {
                Ok(queries
                    .into_iter()
                    .reduce(|a, b| FtsQuery::And(Box::new(a), Box::new(b)))
                    .unwrap())
            } else {
                // All plain terms — use MultiTerm.
                let terms: Vec<String> = queries
                    .into_iter()
                    .map(|q| match q {
                        FtsQuery::Term(t) => t,
                        _ => unreachable!(),
                    })
                    .collect();
                Ok(FtsQuery::MultiTerm(terms))
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_term() {
        let q = parse_fts_query("rust").unwrap();
        assert_eq!(q, FtsQuery::Term("rust".into()));
    }

    #[test]
    fn parse_multi_term_implicit_and() {
        let q = parse_fts_query("rust cargo").unwrap();
        assert_eq!(q, FtsQuery::MultiTerm(vec!["rust".into(), "cargo".into()]));
    }

    #[test]
    fn parse_explicit_and() {
        let q = parse_fts_query("rust AND cargo").unwrap();
        assert!(matches!(&q, FtsQuery::And(l, r) if
            **l == FtsQuery::Term("rust".into()) &&
            **r == FtsQuery::Term("cargo".into())
        ));
    }

    #[test]
    fn parse_explicit_or() {
        let q = parse_fts_query("rust OR go").unwrap();
        assert!(matches!(&q, FtsQuery::Or(l, r) if
            **l == FtsQuery::Term("rust".into()) &&
            **r == FtsQuery::Term("go".into())
        ));
    }

    #[test]
    fn parse_phrase_query() {
        let q = parse_fts_query("\"hello world\"").unwrap();
        assert_eq!(q, FtsQuery::Phrase(vec!["hello".into(), "world".into()]));
    }

    #[test]
    fn parse_empty_query_returns_error() {
        assert!(parse_fts_query("").is_err());
        assert!(parse_fts_query("   ").is_err());
    }

    #[test]
    fn parse_unclosed_phrase_returns_error() {
        assert!(parse_fts_query("\"hello world").is_err());
    }

    #[test]
    fn parse_mixed_case_operators() {
        // AND and OR are case-insensitive.
        let q = parse_fts_query("rust and cargo").unwrap();
        assert!(matches!(q, FtsQuery::And(_, _)));
        let q2 = parse_fts_query("rust or cargo").unwrap();
        assert!(matches!(q2, FtsQuery::Or(_, _)));
    }

    #[test]
    fn parse_prefix_query() {
        let q = parse_fts_query("rust*").unwrap();
        assert_eq!(q, FtsQuery::Prefix("rust".into()));
    }

    #[test]
    fn parse_not_query() {
        let q = parse_fts_query("NOT rust").unwrap();
        assert_eq!(q, FtsQuery::Not(Box::new(FtsQuery::Term("rust".into()))));
    }

    #[test]
    fn parse_bare_star_rejected() {
        let result = parse_fts_query("*");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("bare wildcard"));
    }

    #[test]
    fn parse_not_binds_tighter_than_and() {
        // "a AND NOT b" → And(Term("a"), Not(Term("b")))
        let q = parse_fts_query("a AND NOT b").unwrap();
        match q {
            FtsQuery::And(left, right) => {
                assert_eq!(*left, FtsQuery::Term("a".into()));
                assert_eq!(*right, FtsQuery::Not(Box::new(FtsQuery::Term("b".into()))));
            }
            _ => panic!("expected And, got {q:?}"),
        }
    }
}
