//! Tokens of a PL/pgSQL body, with the byte span each came from.
//!
//! Expressions are passed to the database as SQL text, so a token keeps its exact source
//! span: an expression is rebuilt from the source with only the variable references
//! replaced. Comments are dropped; strings and dollar-quoted strings are single tokens, so a
//! `;` or a keyword inside one is never seen by the parser.

use crate::PlError;

/// What a token is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Tok {
    /// An unquoted identifier or keyword, lowercased.
    Word(String),
    /// A `"quoted"` identifier, case kept and `""` unescaped.
    Quoted(String),
    /// A string literal (`'…'`, `E'…'`, `$tag$…$tag$`), kept as source text.
    Str,
    /// A number, kept as source text.
    Number,
    /// `$n`.
    Param(usize),
    /// An operator or punctuation (`:=`, `::`, `=`, `(`, `;`, `..`).
    Op(String),
}

/// A token and its source span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Token {
    pub(crate) tok: Tok,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

impl Token {
    /// `true` when this is the unquoted word `w` (given lowercase).
    pub(crate) fn is_word(&self, w: &str) -> bool {
        matches!(&self.tok, Tok::Word(x) if x == w)
    }

    /// `true` when this is the operator `op`.
    pub(crate) fn is_op(&self, op: &str) -> bool {
        matches!(&self.tok, Tok::Op(x) if x == op)
    }
}

/// Tokenize `src`.
///
/// # Errors
///
/// `42601` for an unterminated string, quoted identifier or comment.
pub(crate) fn tokenize(src: &str) -> Result<Vec<Token>, PlError> {
    let bytes = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if let Some(end) = skip_comment(bytes, i)? {
            i = end;
            continue;
        }
        let (tok, end) = lex_one(src, i)?;
        out.push(Token { tok, start, end });
        i = end;
    }
    Ok(out)
}

/// The end of a comment starting at `i`, if one does.
fn skip_comment(bytes: &[u8], i: usize) -> Result<Option<usize>, PlError> {
    if bytes[i..].starts_with(b"--") {
        let end = bytes[i..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(bytes.len(), |p| i + p + 1);
        return Ok(Some(end));
    }
    if !bytes[i..].starts_with(b"/*") {
        return Ok(None);
    }
    // Block comments nest in PostgreSQL.
    let mut depth = 0usize;
    let mut j = i;
    while j + 1 < bytes.len() {
        if bytes[j..].starts_with(b"/*") {
            depth += 1;
            j += 2;
        } else if bytes[j..].starts_with(b"*/") {
            depth -= 1;
            j += 2;
            if depth == 0 {
                return Ok(Some(j));
            }
        } else {
            j += 1;
        }
    }
    Err(PlError::syntax("unterminated /* comment"))
}

/// Lex the token starting at `i`; returns it and its end.
fn lex_one(src: &str, i: usize) -> Result<(Tok, usize), PlError> {
    let bytes = src.as_bytes();
    let c = bytes[i];
    if (c == b'E' || c == b'e') && bytes.get(i + 1) == Some(&b'\'') {
        return Ok((Tok::Str, string_end(bytes, i + 1, true)?));
    }
    if c == b'\'' {
        return Ok((Tok::Str, string_end(bytes, i, false)?));
    }
    if c == b'"' {
        return quoted_ident(src, i);
    }
    if c == b'$' {
        return dollar(src, i);
    }
    if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 {
        let end = scan(bytes, i, |b| {
            b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
        });
        return Ok((Tok::Word(src[i..end].to_ascii_lowercase()), end));
    }
    if c.is_ascii_digit() {
        return Ok((Tok::Number, number_end(bytes, i)));
    }
    Ok(operator(src, i))
}

fn scan(bytes: &[u8], from: usize, keep: impl Fn(u8) -> bool) -> usize {
    bytes[from..]
        .iter()
        .position(|&b| !keep(b))
        .map_or(bytes.len(), |p| from + p)
}

/// End of a `'…'` string starting at the quote `q` (`''` escapes; `\'` too in `E'…'`).
fn string_end(bytes: &[u8], q: usize, backslash: bool) -> Result<usize, PlError> {
    let mut j = q + 1;
    while j < bytes.len() {
        match bytes[j] {
            b'\\' if backslash => j += 2,
            b'\'' if bytes.get(j + 1) == Some(&b'\'') => j += 2,
            b'\'' => return Ok(j + 1),
            _ => j += 1,
        }
    }
    Err(PlError::syntax("unterminated quoted string"))
}

fn quoted_ident(src: &str, i: usize) -> Result<(Tok, usize), PlError> {
    let bytes = src.as_bytes();
    let mut j = i + 1;
    while j < bytes.len() {
        if bytes[j] == b'"' {
            if bytes.get(j + 1) == Some(&b'"') {
                j += 2;
                continue;
            }
            let name = src[i + 1..j].replace("\"\"", "\"");
            return Ok((Tok::Quoted(name), j + 1));
        }
        j += 1;
    }
    Err(PlError::syntax("unterminated quoted identifier"))
}

/// `$n` or a dollar-quoted string `$tag$…$tag$`.
fn dollar(src: &str, i: usize) -> Result<(Tok, usize), PlError> {
    let bytes = src.as_bytes();
    let digits_end = scan(bytes, i + 1, |b| b.is_ascii_digit());
    if digits_end > i + 1 {
        let n = src[i + 1..digits_end]
            .parse()
            .map_err(|_| PlError::syntax("parameter number out of range"))?;
        return Ok((Tok::Param(n), digits_end));
    }
    let tag_end = scan(bytes, i + 1, |b| b.is_ascii_alphanumeric() || b == b'_');
    if bytes.get(tag_end) != Some(&b'$') {
        return Ok((Tok::Op("$".into()), i + 1));
    }
    let tag = &src[i..=tag_end];
    let body_start = tag_end + 1;
    match src[body_start..].find(tag) {
        Some(p) => Ok((Tok::Str, body_start + p + tag.len())),
        None => Err(PlError::syntax(format!(
            "unterminated dollar-quoted string {tag}"
        ))),
    }
}

fn number_end(bytes: &[u8], i: usize) -> usize {
    let mut j = scan(bytes, i, |b| b.is_ascii_digit());
    // `1..3` is a range, not a decimal point.
    if bytes.get(j) == Some(&b'.') && bytes.get(j + 1) != Some(&b'.') {
        j = scan(bytes, j + 1, |b| b.is_ascii_digit());
    }
    if matches!(bytes.get(j), Some(b'e' | b'E')) {
        let k = if matches!(bytes.get(j + 1), Some(b'+' | b'-')) {
            j + 2
        } else {
            j + 1
        };
        if bytes.get(k).is_some_and(u8::is_ascii_digit) {
            j = scan(bytes, k, |b| b.is_ascii_digit());
        }
    }
    j
}

/// Operators: the two-character forms the parser looks for, else a run of operator
/// characters, else one punctuation character.
fn operator(src: &str, i: usize) -> (Tok, usize) {
    let bytes = src.as_bytes();
    for two in [":=", "::", "..", "<<", ">>"] {
        if bytes[i..].starts_with(two.as_bytes()) {
            return (Tok::Op(two.into()), i + 2);
        }
    }
    const OPS: &[u8] = b"+-*/<>=~!@#%^&|`?";
    if OPS.contains(&bytes[i]) {
        let end = scan(bytes, i, |b| OPS.contains(&b));
        return (Tok::Op(src[i..end].into()), end);
    }
    let len = src[i..].chars().next().map_or(1, char::len_utf8);
    (Tok::Op(src[i..i + len].into()), i + len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(src: &str) -> Vec<Tok> {
        tokenize(src)
            .expect("lexes")
            .into_iter()
            .map(|t| t.tok)
            .collect()
    }

    #[test]
    fn words_are_lowercased_and_quoted_identifiers_kept() {
        assert_eq!(
            kinds("Begin \"MiXed\"\"x\""),
            vec![Tok::Word("begin".into()), Tok::Quoted("MiXed\"x".into())]
        );
    }

    #[test]
    fn assignment_cast_and_range_are_single_operators() {
        assert_eq!(
            kinds("x:=a::text 1..3"),
            vec![
                Tok::Word("x".into()),
                Tok::Op(":=".into()),
                Tok::Word("a".into()),
                Tok::Op("::".into()),
                Tok::Word("text".into()),
                Tok::Number,
                Tok::Op("..".into()),
                Tok::Number,
            ]
        );
    }

    #[test]
    fn strings_hide_semicolons_and_comments() {
        let toks = tokenize("'a;--b' E'it\\'s' $f$ ; $f$ -- tail\n;").expect("lexes");
        assert_eq!(toks.len(), 4);
        assert!(toks[..3].iter().all(|t| t.tok == Tok::Str));
        assert!(toks[3].is_op(";"));
    }

    #[test]
    fn params_and_dollar_quotes_are_told_apart() {
        assert_eq!(kinds("$1 $$x$$"), vec![Tok::Param(1), Tok::Str]);
    }

    #[test]
    fn unterminated_forms_are_syntax_errors() {
        for src in ["'abc", "\"abc", "/* x", "$q$ abc"] {
            assert_eq!(tokenize(src).expect_err(src).sqlstate, "42601", "{src}");
        }
    }
}
