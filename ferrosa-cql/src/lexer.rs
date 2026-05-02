//! Zero-allocation CQL lexer.
//!
//! Tokenizes a CQL query string, yielding `Token<'input>` values that
//! borrow directly from the source. Keywords are recognized via a `phf`
//! perfect-hash map at compile time.

use phf::phf_map;
use uuid::Uuid;

use crate::error::CqlError;

/// Maximum query length in bytes (1 MiB). Security mitigation M1.
pub const MAX_QUERY_LENGTH: usize = 1_048_576;

/// CQL keywords recognized by the lexer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Alter,
    Drop,
    From,
    Where,
    And,
    Or,
    In,
    Set,
    Into,
    Values,
    If,
    Exists,
    Not,
    Primary,
    Key,
    Table,
    Keyspace,
    Role,
    Grant,
    Revoke,
    On,
    To,
    Of,
    Use,
    Batch,
    Begin,
    Apply,
    Unlogged,
    Counter,
    Logged,
    Truncate,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Allow,
    Filtering,
    With,
    Replication,
    DurableWrites,
    Password,
    Superuser,
    Login,
    Nosuperuser,
    Nologin,
    True,
    False,
    Null,
    Using,
    Timestamp,
    Ttl,
    Int,
    Bigint,
    Text,
    Varchar,
    Blob,
    Boolean,
    Float,
    Double,
    Uuid,
    Timeuuid,
    Inet,
    Varint,
    Decimal,
    Date,
    Time,
    Smallint,
    Tinyint,
    Ascii,
    List,
    Map,
    Tuple,
    Frozen,
    Static,
    Clustering,
    Compact,
    Storage,
    Token,
    Writetime,
    All,
    Permissions,
    Index,
    Options,
    Subscribe,
    Unsubscribe,
    Every,
    Delta,
    Type,
    Rename,
    Add,
    Function,
    Returns,
    Language,
    Called,
    Input,
    Replace,
    Aggregate,
    Sfunc,
    Stype,
    Finalfunc,
    Initcond,
    As,
    Contains,
    Explain,
    Distinct,
    Transaction,
    Commit,
    Rollback,
    Sounds,
    Like,
    /// CQL `USER` — deprecated alias for `ROLE WITH LOGIN = true`.
    User,
}

/// Compile-time keyword map. Case-insensitive lookup is done by
/// uppercasing the candidate before lookup.
static KEYWORDS: phf::Map<&'static str, Keyword> = phf_map! {
    "SELECT" => Keyword::Select,
    "INSERT" => Keyword::Insert,
    "UPDATE" => Keyword::Update,
    "DELETE" => Keyword::Delete,
    "CREATE" => Keyword::Create,
    "ALTER" => Keyword::Alter,
    "DROP" => Keyword::Drop,
    "FROM" => Keyword::From,
    "WHERE" => Keyword::Where,
    "AND" => Keyword::And,
    "OR" => Keyword::Or,
    "IN" => Keyword::In,
    "SET" => Keyword::Set,
    "INTO" => Keyword::Into,
    "VALUES" => Keyword::Values,
    "IF" => Keyword::If,
    "EXISTS" => Keyword::Exists,
    "NOT" => Keyword::Not,
    "PRIMARY" => Keyword::Primary,
    "KEY" => Keyword::Key,
    "TABLE" => Keyword::Table,
    "KEYSPACE" => Keyword::Keyspace,
    "ROLE" => Keyword::Role,
    "GRANT" => Keyword::Grant,
    "REVOKE" => Keyword::Revoke,
    "ON" => Keyword::On,
    "TO" => Keyword::To,
    "OF" => Keyword::Of,
    "USE" => Keyword::Use,
    "BATCH" => Keyword::Batch,
    "BEGIN" => Keyword::Begin,
    "APPLY" => Keyword::Apply,
    "UNLOGGED" => Keyword::Unlogged,
    "COUNTER" => Keyword::Counter,
    "LOGGED" => Keyword::Logged,
    "TRUNCATE" => Keyword::Truncate,
    "ORDER" => Keyword::Order,
    "BY" => Keyword::By,
    "ASC" => Keyword::Asc,
    "DESC" => Keyword::Desc,
    "LIMIT" => Keyword::Limit,
    "ALLOW" => Keyword::Allow,
    "FILTERING" => Keyword::Filtering,
    "WITH" => Keyword::With,
    "REPLICATION" => Keyword::Replication,
    "DURABLE_WRITES" => Keyword::DurableWrites,
    "PASSWORD" => Keyword::Password,
    "SUPERUSER" => Keyword::Superuser,
    "LOGIN" => Keyword::Login,
    "NOSUPERUSER" => Keyword::Nosuperuser,
    "NOLOGIN" => Keyword::Nologin,
    "TRUE" => Keyword::True,
    "FALSE" => Keyword::False,
    "NULL" => Keyword::Null,
    "USING" => Keyword::Using,
    "TIMESTAMP" => Keyword::Timestamp,
    "TTL" => Keyword::Ttl,
    "INT" => Keyword::Int,
    "BIGINT" => Keyword::Bigint,
    "TEXT" => Keyword::Text,
    "VARCHAR" => Keyword::Varchar,
    "BLOB" => Keyword::Blob,
    "BOOLEAN" => Keyword::Boolean,
    "FLOAT" => Keyword::Float,
    "DOUBLE" => Keyword::Double,
    "UUID" => Keyword::Uuid,
    "TIMEUUID" => Keyword::Timeuuid,
    "INET" => Keyword::Inet,
    "VARINT" => Keyword::Varint,
    "DECIMAL" => Keyword::Decimal,
    "DATE" => Keyword::Date,
    "TIME" => Keyword::Time,
    "SMALLINT" => Keyword::Smallint,
    "TINYINT" => Keyword::Tinyint,
    "ASCII" => Keyword::Ascii,
    "LIST" => Keyword::List,
    "MAP" => Keyword::Map,
    "TUPLE" => Keyword::Tuple,
    "FROZEN" => Keyword::Frozen,
    "STATIC" => Keyword::Static,
    "CLUSTERING" => Keyword::Clustering,
    "COMPACT" => Keyword::Compact,
    "STORAGE" => Keyword::Storage,
    "TOKEN" => Keyword::Token,
    "WRITETIME" => Keyword::Writetime,
    "ALL" => Keyword::All,
    "PERMISSIONS" => Keyword::Permissions,
    "INDEX" => Keyword::Index,
    "OPTIONS" => Keyword::Options,
    "SUBSCRIBE" => Keyword::Subscribe,
    "UNSUBSCRIBE" => Keyword::Unsubscribe,
    "EVERY" => Keyword::Every,
    "DELTA" => Keyword::Delta,
    "TYPE" => Keyword::Type,
    "RENAME" => Keyword::Rename,
    "ADD" => Keyword::Add,
    "FUNCTION" => Keyword::Function,
    "RETURNS" => Keyword::Returns,
    "LANGUAGE" => Keyword::Language,
    "CALLED" => Keyword::Called,
    "INPUT" => Keyword::Input,
    "REPLACE" => Keyword::Replace,
    "AGGREGATE" => Keyword::Aggregate,
    "SFUNC" => Keyword::Sfunc,
    "STYPE" => Keyword::Stype,
    "FINALFUNC" => Keyword::Finalfunc,
    "INITCOND" => Keyword::Initcond,
    "AS" => Keyword::As,
    "CONTAINS" => Keyword::Contains,
    "EXPLAIN" => Keyword::Explain,
    "DISTINCT" => Keyword::Distinct,
    "TRANSACTION" => Keyword::Transaction,
    "COMMIT" => Keyword::Commit,
    "ROLLBACK" => Keyword::Rollback,
    "SOUNDS" => Keyword::Sounds,
    "LIKE" => Keyword::Like,
    "USER" => Keyword::User,
    // `USERS`/`ROLES` are NOT reserved keywords — `SELECT * FROM users`
    // is a legal Cassandra query against a user-defined table named
    // `users`. The `LIST USERS`/`LIST ROLES` parser handles them via
    // case-insensitive ident matching after the `LIST` keyword.
};

/// Token kind produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind<'input> {
    Keyword(Keyword),
    /// Unquoted identifier — borrows from the source.
    Ident(&'input str),
    /// Quoted identifier — `"MyTable"` preserves case.
    QuotedIdent(String),
    /// String literal — `'hello'` with `''` escape.
    StringLiteral(String),
    IntegerLiteral(i64),
    FloatLiteral(f64),
    UuidLiteral(uuid::Uuid),
    /// Hex blob literal — `0xDEADBEEF`.
    BlobLiteral(Vec<u8>),
    /// `?` positional bind marker.
    QuestionMark,
    /// `:name` named bind marker.
    NamedBind(String),
    // Comparison operators
    Eq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    NotEq,
    // Arithmetic
    Plus,
    Minus,
    Star,
    // Delimiters
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    // Punctuation
    Comma,
    Dot,
    Semicolon,
    Colon,
    // End of input
    Eof,
}

/// A token with its position in the source.
#[derive(Debug, Clone, PartialEq)]
pub struct Token<'input> {
    pub kind: TokenKind<'input>,
    pub pos: usize,
}

/// Check if two token kinds match structurally. For keywords, compares
/// the specific keyword variant. For other kinds, compares discriminants.
fn kind_matches(actual: &TokenKind<'_>, expected: &TokenKind<'_>) -> bool {
    match (actual, expected) {
        (TokenKind::Keyword(a), TokenKind::Keyword(b)) => a == b,
        _ => std::mem::discriminant(actual) == std::mem::discriminant(expected),
    }
}

/// Zero-allocation lexer for CQL queries.
///
/// Yields `Token<'input>` values that borrow from the source string.
/// Maintains a byte offset cursor and supports peek/next.
#[derive(Debug)]
pub struct Lexer<'input> {
    input: &'input str,
    bytes: &'input [u8],
    pos: usize,
    peeked: Option<Token<'input>>,
}

impl<'input> Lexer<'input> {
    /// Create a new lexer over the given input.
    ///
    /// Returns `CqlError::Invalid` if the input exceeds `MAX_QUERY_LENGTH`
    /// (security mitigation M1).
    pub fn new(input: &'input str) -> Result<Self, CqlError> {
        if input.len() > MAX_QUERY_LENGTH {
            return Err(CqlError::Invalid(format!(
                "query too long: {} bytes (max {})",
                input.len(),
                MAX_QUERY_LENGTH
            )));
        }
        Ok(Self {
            input,
            bytes: input.as_bytes(),
            pos: 0,
            peeked: None,
        })
    }

    /// Peek at the next token without consuming it.
    pub fn peek(&mut self) -> Result<&Token<'input>, CqlError> {
        if self.peeked.is_none() {
            self.peeked = Some(self.advance()?);
        }
        // SAFETY: we just set peeked above if it was None
        Ok(self.peeked.as_ref().expect("peeked was just set"))
    }

    /// Consume and return the next token.
    pub fn next_token(&mut self) -> Result<Token<'input>, CqlError> {
        if let Some(tok) = self.peeked.take() {
            return Ok(tok);
        }
        self.advance()
    }

    /// Consume the next token and assert its kind matches `expected`.
    pub fn expect(&mut self, expected: &TokenKind<'_>) -> Result<Token<'input>, CqlError> {
        let tok = self.next_token()?;
        if kind_matches(&tok.kind, expected) {
            Ok(tok)
        } else {
            Err(CqlError::SyntaxError(format!(
                "expected {:?}, got {:?} at position {}",
                expected, tok.kind, tok.pos
            )))
        }
    }

    /// Peek and consume the next token if it matches `expected`.
    /// Returns `Ok(true)` if consumed, `Ok(false)` otherwise.
    pub fn eat(&mut self, expected: &TokenKind<'_>) -> Result<bool, CqlError> {
        let tok = self.peek()?;
        if kind_matches(&tok.kind, expected) {
            self.next_token()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Skip whitespace and comments (`--` line comments, `/* */` block comments).
    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while self.pos < self.bytes.len() {
                let b = self.bytes[self.pos];
                if b == b' ' || b == b'\t' || b == b'\n' || b == b'\r' {
                    self.pos += 1;
                } else {
                    break;
                }
            }

            if self.pos >= self.bytes.len() {
                return;
            }

            // Line comment: -- to end of line
            if self.pos + 1 < self.bytes.len()
                && self.bytes[self.pos] == b'-'
                && self.bytes[self.pos + 1] == b'-'
            {
                self.pos += 2;
                while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
                    self.pos += 1;
                }
                continue;
            }

            // Block comment: /* ... */
            if self.pos + 1 < self.bytes.len()
                && self.bytes[self.pos] == b'/'
                && self.bytes[self.pos + 1] == b'*'
            {
                self.pos += 2;
                loop {
                    if self.pos + 1 >= self.bytes.len() {
                        // Unterminated block comment — treat as consumed to EOF
                        self.pos = self.bytes.len();
                        return;
                    }
                    if self.bytes[self.pos] == b'*' && self.bytes[self.pos + 1] == b'/' {
                        self.pos += 2;
                        break;
                    }
                    self.pos += 1;
                }
                continue;
            }

            // No more whitespace or comments
            break;
        }
    }

    /// Advance the cursor and produce the next token.
    fn advance(&mut self) -> Result<Token<'input>, CqlError> {
        self.skip_whitespace_and_comments();

        if self.pos >= self.bytes.len() {
            return Ok(Token {
                kind: TokenKind::Eof,
                pos: self.pos,
            });
        }

        let start = self.pos;
        let b = self.bytes[self.pos];

        match b {
            // String literal
            b'\'' => self.read_string_literal(),

            // Quoted identifier
            b'"' => self.read_quoted_identifier(),

            // Number or hex blob (0x...)
            b'0' if self.pos + 1 < self.bytes.len()
                && (self.bytes[self.pos + 1] == b'x' || self.bytes[self.pos + 1] == b'X') =>
            {
                self.read_hex_blob()
            }
            b'0'..=b'9' => self.read_number(),

            // Identifier or keyword
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => Ok(self.read_identifier()),

            // Operators
            b'=' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::Eq,
                    pos: start,
                })
            }
            b'!' if self.pos + 1 < self.bytes.len() && self.bytes[self.pos + 1] == b'=' => {
                self.pos += 2;
                Ok(Token {
                    kind: TokenKind::NotEq,
                    pos: start,
                })
            }
            b'<' => {
                self.pos += 1;
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'=' {
                    self.pos += 1;
                    Ok(Token {
                        kind: TokenKind::LtEq,
                        pos: start,
                    })
                } else {
                    Ok(Token {
                        kind: TokenKind::Lt,
                        pos: start,
                    })
                }
            }
            b'>' => {
                self.pos += 1;
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'=' {
                    self.pos += 1;
                    Ok(Token {
                        kind: TokenKind::GtEq,
                        pos: start,
                    })
                } else {
                    Ok(Token {
                        kind: TokenKind::Gt,
                        pos: start,
                    })
                }
            }
            b'+' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::Plus,
                    pos: start,
                })
            }
            b'-' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::Minus,
                    pos: start,
                })
            }
            b'*' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::Star,
                    pos: start,
                })
            }

            // Delimiters
            b'(' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::LParen,
                    pos: start,
                })
            }
            b')' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::RParen,
                    pos: start,
                })
            }
            b'[' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::LBracket,
                    pos: start,
                })
            }
            b']' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::RBracket,
                    pos: start,
                })
            }
            b'{' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::LBrace,
                    pos: start,
                })
            }
            b'}' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::RBrace,
                    pos: start,
                })
            }

            // Punctuation
            b',' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::Comma,
                    pos: start,
                })
            }
            b'.' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::Dot,
                    pos: start,
                })
            }
            b';' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::Semicolon,
                    pos: start,
                })
            }

            // ? bind marker
            b'?' => {
                self.pos += 1;
                Ok(Token {
                    kind: TokenKind::QuestionMark,
                    pos: start,
                })
            }

            // : — either Colon or :name NamedBind
            b':' => {
                self.pos += 1;
                if self.pos < self.bytes.len()
                    && (self.bytes[self.pos].is_ascii_alphabetic() || self.bytes[self.pos] == b'_')
                {
                    let name_start = self.pos;
                    while self.pos < self.bytes.len()
                        && (self.bytes[self.pos].is_ascii_alphanumeric()
                            || self.bytes[self.pos] == b'_')
                    {
                        self.pos += 1;
                    }
                    let name = self.input[name_start..self.pos].to_owned();
                    Ok(Token {
                        kind: TokenKind::NamedBind(name),
                        pos: start,
                    })
                } else {
                    Ok(Token {
                        kind: TokenKind::Colon,
                        pos: start,
                    })
                }
            }

            other => Err(CqlError::SyntaxError(format!(
                "unexpected character '{}' at position {}",
                other as char, start
            ))),
        }
    }

    /// Read an identifier or keyword. Also detects UUID literals.
    fn read_identifier(&mut self) -> Token<'input> {
        let start = self.pos;
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];
            if b.is_ascii_alphanumeric() || b == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }

        let text = &self.input[start..self.pos];

        // UUID detection: if the identifier looks like the start of a UUID
        // (8 hex chars followed by a '-'), try to consume the full UUID.
        if text.len() == 8
            && text.bytes().all(|b| b.is_ascii_hexdigit())
            && self.pos < self.bytes.len()
            && self.bytes[self.pos] == b'-'
        {
            // Speculatively try to read a full UUID: 8-4-4-4-12
            let saved_pos = self.pos;
            // We already have the first 8 hex chars. Try to consume -xxxx-xxxx-xxxx-xxxxxxxxxxxx
            if let Some(uuid) = self.try_read_uuid_tail(text) {
                return Token {
                    kind: TokenKind::UuidLiteral(uuid),
                    pos: start,
                };
            }
            // Not a valid UUID — restore position
            self.pos = saved_pos;
        }

        // Case-insensitive keyword lookup
        let upper = text.to_ascii_uppercase();
        if let Some(&kw) = KEYWORDS.get(upper.as_str()) {
            Token {
                kind: TokenKind::Keyword(kw),
                pos: start,
            }
        } else {
            Token {
                kind: TokenKind::Ident(text),
                pos: start,
            }
        }
    }

    /// Try to read the tail of a UUID after the first 8 hex chars.
    /// Returns `Some(Uuid)` on success, `None` on failure.
    fn try_read_uuid_tail(&mut self, prefix: &str) -> Option<Uuid> {
        // Expected: -{4hex}-{4hex}-{4hex}-{12hex}
        let remaining_pattern: &[usize] = &[4, 4, 4, 12];

        let mut full = String::with_capacity(36);
        full.push_str(prefix);

        for &count in remaining_pattern {
            // Expect a dash
            if self.pos >= self.bytes.len() || self.bytes[self.pos] != b'-' {
                return None;
            }
            full.push('-');
            self.pos += 1;

            // Expect `count` hex digits
            let seg_start = self.pos;
            for _ in 0..count {
                if self.pos >= self.bytes.len() || !self.bytes[self.pos].is_ascii_hexdigit() {
                    return None;
                }
                self.pos += 1;
            }
            full.push_str(&self.input[seg_start..self.pos]);
        }

        // Make sure we're not in the middle of an identifier
        if self.pos < self.bytes.len()
            && (self.bytes[self.pos].is_ascii_alphanumeric() || self.bytes[self.pos] == b'_')
        {
            return None;
        }

        full.parse::<Uuid>().ok()
    }

    /// Read a number (integer or float).
    ///
    /// If digits are followed by hex alpha characters (a-fA-F), the token
    /// might be a UUID literal like `550e8400-e29b-...`. In that case we
    /// extend the read to the full alphanumeric word and attempt UUID
    /// detection, falling back to an identifier if it's not a valid UUID.
    fn read_number(&mut self) -> Result<Token<'input>, CqlError> {
        let start = self.pos;
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
            self.pos += 1;
        }

        // If the next character is a letter or underscore, this is not a
        // pure number — it could be a UUID or a hex-prefixed identifier.
        // Read the full alphanumeric word and check for UUID.
        if self.pos < self.bytes.len()
            && (self.bytes[self.pos].is_ascii_alphabetic() || self.bytes[self.pos] == b'_')
        {
            while self.pos < self.bytes.len()
                && (self.bytes[self.pos].is_ascii_alphanumeric() || self.bytes[self.pos] == b'_')
            {
                self.pos += 1;
            }
            let text = &self.input[start..self.pos];

            // UUID detection: 8 hex chars followed by '-'
            if text.len() == 8
                && text.bytes().all(|b| b.is_ascii_hexdigit())
                && self.pos < self.bytes.len()
                && self.bytes[self.pos] == b'-'
            {
                let saved_pos = self.pos;
                if let Some(uuid) = self.try_read_uuid_tail(text) {
                    return Ok(Token {
                        kind: TokenKind::UuidLiteral(uuid),
                        pos: start,
                    });
                }
                self.pos = saved_pos;
            }

            // Not a UUID — return as identifier (keyword lookup won't match
            // anything starting with a digit, so it'll be Ident).
            let text = &self.input[start..self.pos];
            let upper = text.to_ascii_uppercase();
            if let Some(&kw) = KEYWORDS.get(upper.as_str()) {
                return Ok(Token {
                    kind: TokenKind::Keyword(kw),
                    pos: start,
                });
            }
            return Ok(Token {
                kind: TokenKind::Ident(text),
                pos: start,
            });
        }

        // UUID detection for pure-digit prefixes: e.g. 11111111-1111-1111-1111-111111111111
        // The block above only handles prefixes with hex alpha chars (a-f); all-digit
        // prefixes like "11111111" fall through here because '-' is not alphabetic.
        let digit_text = &self.input[start..self.pos];
        if digit_text.len() == 8 && self.pos < self.bytes.len() && self.bytes[self.pos] == b'-' {
            let saved_pos = self.pos;
            if let Some(uuid) = self.try_read_uuid_tail(digit_text) {
                return Ok(Token {
                    kind: TokenKind::UuidLiteral(uuid),
                    pos: start,
                });
            }
            self.pos = saved_pos;
        }

        // Check for decimal point → float
        if self.pos < self.bytes.len()
            && self.bytes[self.pos] == b'.'
            && self.pos + 1 < self.bytes.len()
            && self.bytes[self.pos + 1].is_ascii_digit()
        {
            self.pos += 1; // skip dot
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            let text = &self.input[start..self.pos];
            let value: f64 = text
                .parse()
                .map_err(|_| CqlError::SyntaxError(format!("invalid float literal: {}", text)))?;
            Ok(Token {
                kind: TokenKind::FloatLiteral(value),
                pos: start,
            })
        } else {
            let text = &self.input[start..self.pos];
            if let Ok(value) = text.parse::<i64>() {
                Ok(Token {
                    kind: TokenKind::IntegerLiteral(value),
                    pos: start,
                })
            } else if let Ok(value) = text.parse::<f64>() {
                // Large integers that overflow i64 can still be valid as float/double
                // literals (e.g. proptest-generated doubles like "33092290000000000000000000000000").
                // CQL allows integer-shaped literals for float/double columns.
                Ok(Token {
                    kind: TokenKind::FloatLiteral(value),
                    pos: start,
                })
            } else {
                Err(CqlError::SyntaxError(format!(
                    "invalid numeric literal: {}",
                    text
                )))
            }
        }
    }

    /// Read a string literal: `'hello'` with `''` escape.
    fn read_string_literal(&mut self) -> Result<Token<'input>, CqlError> {
        let start = self.pos;
        self.pos += 1; // skip opening quote

        let mut s = String::new();
        loop {
            if self.pos >= self.bytes.len() {
                return Err(CqlError::SyntaxError(format!(
                    "unterminated string literal starting at position {}",
                    start
                )));
            }
            let b = self.bytes[self.pos];
            if b == b'\'' {
                self.pos += 1;
                // Check for escaped quote ('')
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'\'' {
                    s.push('\'');
                    self.pos += 1;
                } else {
                    break;
                }
            } else {
                s.push(b as char);
                self.pos += 1;
            }
        }
        Ok(Token {
            kind: TokenKind::StringLiteral(s),
            pos: start,
        })
    }

    /// Read a quoted identifier: `"MyTable"`.
    fn read_quoted_identifier(&mut self) -> Result<Token<'input>, CqlError> {
        let start = self.pos;
        self.pos += 1; // skip opening double-quote

        let mut s = String::new();
        loop {
            if self.pos >= self.bytes.len() {
                return Err(CqlError::SyntaxError(format!(
                    "unterminated quoted identifier starting at position {}",
                    start
                )));
            }
            let b = self.bytes[self.pos];
            if b == b'"' {
                self.pos += 1;
                // CQL allows "" to escape a double-quote inside a quoted identifier
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'"' {
                    s.push('"');
                    self.pos += 1;
                } else {
                    break;
                }
            } else {
                s.push(b as char);
                self.pos += 1;
            }
        }
        Ok(Token {
            kind: TokenKind::QuotedIdent(s),
            pos: start,
        })
    }

    /// Read a hex blob literal: `0xDEADBEEF`.
    fn read_hex_blob(&mut self) -> Result<Token<'input>, CqlError> {
        let start = self.pos;
        self.pos += 2; // skip 0x

        let hex_start = self.pos;
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_hexdigit() {
            self.pos += 1;
        }

        let hex_str = &self.input[hex_start..self.pos];
        if !hex_str.len().is_multiple_of(2) {
            return Err(CqlError::SyntaxError(format!(
                "hex blob literal has odd number of digits at position {}",
                start
            )));
        }

        let mut bytes = Vec::with_capacity(hex_str.len() / 2);
        let mut i = 0;
        while i < hex_str.len() {
            let hi = hex_digit_value(hex_str.as_bytes()[i]);
            let lo = hex_digit_value(hex_str.as_bytes()[i + 1]);
            bytes.push((hi << 4) | lo);
            i += 2;
        }

        Ok(Token {
            kind: TokenKind::BlobLiteral(bytes),
            pos: start,
        })
    }
}

/// Convert an ASCII hex digit to its numeric value (0–15).
/// Assumes the input is a valid hex digit (checked by `is_ascii_hexdigit`).
fn hex_digit_value(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => unreachable!("caller ensures valid hex digit"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: lex all tokens from input (excluding Eof).
    fn lex_all(input: &str) -> Vec<TokenKind<'_>> {
        let mut lexer = Lexer::new(input).expect("input within limits");
        let mut tokens = vec![];
        loop {
            let tok = lexer.next_token().expect("no lex error");
            if tok.kind == TokenKind::Eof {
                break;
            }
            tokens.push(tok.kind);
        }
        tokens
    }

    #[test]
    fn lex_select_query() {
        let tokens = lex_all("SELECT * FROM users WHERE id = 42");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Keyword(Keyword::Select),
                TokenKind::Star,
                TokenKind::Keyword(Keyword::From),
                TokenKind::Ident("users"),
                TokenKind::Keyword(Keyword::Where),
                TokenKind::Ident("id"),
                TokenKind::Eq,
                TokenKind::IntegerLiteral(42),
            ]
        );
    }

    #[test]
    fn lex_string_literal() {
        let tokens = lex_all("'hello world'");
        assert_eq!(tokens, vec![TokenKind::StringLiteral("hello world".into())]);
    }

    #[test]
    fn lex_escaped_string() {
        let tokens = lex_all("'it''s'");
        assert_eq!(tokens, vec![TokenKind::StringLiteral("it's".into())]);
    }

    #[test]
    fn lex_integer_and_float() {
        let tokens = lex_all("42 3.25 -7");
        assert_eq!(
            tokens,
            vec![
                TokenKind::IntegerLiteral(42),
                TokenKind::FloatLiteral(3.25),
                TokenKind::Minus,
                TokenKind::IntegerLiteral(7),
            ]
        );
    }

    #[test]
    fn lex_uuid() {
        let tokens = lex_all("550e8400-e29b-41d4-a716-446655440000");
        let expected_uuid: uuid::Uuid = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        assert_eq!(tokens, vec![TokenKind::UuidLiteral(expected_uuid)]);
    }

    #[test]
    fn lex_uuid_all_digits() {
        // UUIDs where the first 8 chars are all decimal digits must not be
        // parsed as integer-minus-integer.
        let tokens = lex_all("11111111-1111-1111-1111-111111111111");
        let expected: uuid::Uuid = "11111111-1111-1111-1111-111111111111".parse().unwrap();
        assert_eq!(tokens, vec![TokenKind::UuidLiteral(expected)]);
    }

    #[test]
    fn lex_hex_blob() {
        let tokens = lex_all("0xDEADBEEF");
        assert_eq!(
            tokens,
            vec![TokenKind::BlobLiteral(vec![0xDE, 0xAD, 0xBE, 0xEF])]
        );
    }

    #[test]
    fn lex_bind_markers() {
        let tokens = lex_all("? :name");
        assert_eq!(
            tokens,
            vec![TokenKind::QuestionMark, TokenKind::NamedBind("name".into()),]
        );
    }

    #[test]
    fn lex_operators() {
        let tokens = lex_all("= < > <= >= !=");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Eq,
                TokenKind::Lt,
                TokenKind::Gt,
                TokenKind::LtEq,
                TokenKind::GtEq,
                TokenKind::NotEq,
            ]
        );
    }

    #[test]
    fn lex_keywords_case_insensitive() {
        let tokens = lex_all("select SELECT Select");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Keyword(Keyword::Select),
                TokenKind::Keyword(Keyword::Select),
                TokenKind::Keyword(Keyword::Select),
            ]
        );
    }

    #[test]
    fn lex_quoted_identifier() {
        let tokens = lex_all("\"MyTable\"");
        assert_eq!(tokens, vec![TokenKind::QuotedIdent("MyTable".into())]);
    }

    #[test]
    fn lex_line_comment() {
        let tokens = lex_all("-- comment\nSELECT");
        assert_eq!(tokens, vec![TokenKind::Keyword(Keyword::Select)]);
    }

    #[test]
    fn lex_block_comment() {
        let tokens = lex_all("/* comment */SELECT");
        assert_eq!(tokens, vec![TokenKind::Keyword(Keyword::Select)]);
    }

    #[test]
    fn lex_unterminated_string_error() {
        let mut lexer = Lexer::new("'hello").unwrap();
        let err = lexer.next_token().unwrap_err();
        match err {
            CqlError::SyntaxError(msg) => assert!(msg.contains("unterminated")),
            other => panic!("expected SyntaxError, got {:?}", other),
        }
    }

    #[test]
    fn lex_empty_input() {
        let mut lexer = Lexer::new("").unwrap();
        let tok = lexer.next_token().unwrap();
        assert_eq!(tok.kind, TokenKind::Eof);
    }

    #[test]
    fn lex_query_too_long() {
        let long_input = "a".repeat(MAX_QUERY_LENGTH + 1);
        let err = Lexer::new(&long_input).unwrap_err();
        match err {
            CqlError::Invalid(msg) => assert!(msg.contains("query too long")),
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn lexer_recognizes_subscribe_keywords() {
        let tokens = lex_all("SUBSCRIBE SELECT * FROM t EVERY 5s DELTA");
        assert!(tokens
            .iter()
            .any(|t| matches!(t, TokenKind::Keyword(Keyword::Subscribe))));
        assert!(tokens
            .iter()
            .any(|t| matches!(t, TokenKind::Keyword(Keyword::Every))));
        assert!(tokens
            .iter()
            .any(|t| matches!(t, TokenKind::Keyword(Keyword::Delta))));
    }

    #[test]
    fn lexer_recognizes_unsubscribe() {
        let tokens = lex_all("UNSUBSCRIBE");
        assert!(tokens
            .iter()
            .any(|t| matches!(t, TokenKind::Keyword(Keyword::Unsubscribe))));
    }

    #[test]
    fn lexer_recognizes_type_keyword() {
        let tokens = lex_all("CREATE TYPE");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Keyword(Keyword::Create),
                TokenKind::Keyword(Keyword::Type),
            ]
        );
    }

    #[test]
    fn lexer_recognizes_type_case_insensitive() {
        let tokens = lex_all("type Type TYPE");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Keyword(Keyword::Type),
                TokenKind::Keyword(Keyword::Type),
                TokenKind::Keyword(Keyword::Type),
            ]
        );
    }

    #[test]
    fn lexer_recognizes_add_and_rename_keywords() {
        let tokens = lex_all("ADD RENAME");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Keyword(Keyword::Add),
                TokenKind::Keyword(Keyword::Rename),
            ]
        );
    }
}
