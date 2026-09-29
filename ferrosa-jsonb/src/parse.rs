//! Module: streaming RFC 8259 JSON text parser feeding `JsonbBuilder` (T-103,
//! D2, D6b, D14, FM-01, FM-03, FM-04, FM-16, JB-D5, JB-T7).
//! Correctness: correct when the order is input size, then a bracket-depth
//! pre-scan that skips strings (so a depth breach is refused before anything is
//! built), then an iterative parse (an explicit frame stack, no recursion, so
//! a 256 KiB thread stack parses depth 1000). Numbers reach `Number::parse_lexeme`
//! as lexemes, so the digit caps run before any big-integer work. Every
//! syntax, UTF-8 or surrogate fault is a typed error with a byte offset; nothing
//! is repaired. Duplicate keys resolve last-wins in the builder and the count is
//! reported once per document to a `DuplicateKeyObserver` under an edge label,
//! or fail with the object's path under `DuplicateKeyPolicy::Error`.
//! Working set: input + O(depth) frames + the builder arena, which is at most
//! `WORKING_SET_MULTIPLE` times the input length (worst case is a flat array of
//! one-byte numbers); nothing is ever reparsed.
//! Last revised: 2026-09-28
//! Last changed: T-103 initial parser.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::builder::{Encoded, JsonbBuilder};
use crate::error::JsonbError;
use crate::limits::{HardCeilings, Limits};
use crate::number::Number;

/// Documented worst-case multiple of the input length held while parsing.
pub const WORKING_SET_MULTIPLE: usize = 32;

/// Receives the duplicate-key count for a document. The adapter increments
/// `jsonb_duplicate_keys_dropped_total{edge}` and writes the edge log line
/// (D6b, FM-04). Called at most once per document, and only when count > 0.
pub trait DuplicateKeyObserver {
    fn duplicate_keys_dropped(&self, edge: &str, count: u64);
}

/// Node-wide budget of bytes in flight across concurrent parses (JB-D5).
#[derive(Debug, Clone)]
pub struct InflightBudget {
    max: usize,
    used: Arc<AtomicUsize>,
}

/// Bytes admitted by an [`InflightBudget`]; released on drop.
#[derive(Debug)]
pub struct InflightPermit {
    bytes: usize,
    used: Arc<AtomicUsize>,
}

impl InflightBudget {
    pub fn new(max: usize) -> InflightBudget {
        InflightBudget {
            max,
            used: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Admit `bytes` or refuse loudly; never blocks.
    pub fn try_acquire(&self, bytes: usize) -> Result<InflightPermit, JsonbError> {
        let max = self.max;
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |u| {
                u.checked_add(bytes).filter(|t| *t <= max)
            })
            .map_err(|_| JsonbError::InflightBudgetExceeded {
                requested: bytes,
                max,
            })?;
        Ok(InflightPermit {
            bytes,
            used: Arc::clone(&self.used),
        })
    }

    /// The budget an input of `input_len` bytes needs (see `WORKING_SET_MULTIPLE`).
    pub fn working_set(input_len: usize) -> usize {
        input_len.saturating_mul(WORKING_SET_MULTIPLE)
    }
}

impl Drop for InflightPermit {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

fn syntax(offset: usize, reason: &'static str) -> JsonbError {
    JsonbError::Syntax { offset, reason }
}

/// Parse JSON text into canonical jsonb bytes. The duplicate-key count is in
/// the result.
pub fn parse_text(input: &[u8], limits: &Limits) -> Result<Encoded, JsonbError> {
    limits.check_input_len(input.len())?;
    prescan_depth(input, limits)?;
    Parser::new(input, limits).run()
}

/// [`parse_text`], reporting dropped duplicate keys to `observer` under `edge`.
pub fn parse_text_observed(
    input: &[u8],
    limits: &Limits,
    edge: &str,
    observer: &dyn DuplicateKeyObserver,
) -> Result<Encoded, JsonbError> {
    let out = parse_text(input, limits)?;
    if out.duplicate_keys_dropped > 0 {
        observer.duplicate_keys_dropped(edge, out.duplicate_keys_dropped);
    }
    Ok(out)
}

/// Reject over-deep nesting before anything is built. Brackets inside strings
/// do not count; malformed text is left for the parser to locate.
fn prescan_depth(input: &[u8], limits: &Limits) -> Result<(), JsonbError> {
    let (mut depth, mut in_string, mut escaped) = (0u32, false, false);
    for &b in input {
        if in_string {
            match (escaped, b) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => in_string = false,
                (false, _) => {}
            }
        } else if b == b'"' {
            in_string = true;
        } else if b == b'[' || b == b'{' {
            depth = depth.saturating_add(1);
            limits.check_depth(depth)?;
            HardCeilings::CURRENT.check_depth(depth)?;
        } else if b == b']' || b == b'}' {
            depth = depth.saturating_sub(1);
        }
    }
    Ok(())
}

struct Frame {
    object: bool,
    key: Option<String>,
    /// Elements started so far (arrays).
    index: usize,
}

#[derive(PartialEq, Eq)]
enum Flow {
    /// A container opened and a value must follow.
    Opened,
    /// A complete value was consumed.
    Complete,
}

struct Parser<'a> {
    s: &'a [u8],
    pos: usize,
    builder: JsonbBuilder,
    frames: Vec<Frame>,
}

impl<'a> Parser<'a> {
    fn new(s: &'a [u8], limits: &Limits) -> Parser<'a> {
        Parser {
            s,
            pos: 0,
            builder: JsonbBuilder::new(*limits),
            frames: Vec::new(),
        }
    }

    fn peek(&self) -> Option<u8> {
        self.s.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    /// Every pass consumes at least one byte, so `len + 1` passes always
    /// suffice; the bound is a hard cap, not an expected count.
    fn run(mut self) -> Result<Encoded, JsonbError> {
        let mut finished = false;
        for _ in 0..=self.s.len() {
            if self.parse_value()? == Flow::Opened {
                continue;
            }
            if self.after_value()? {
                finished = true;
                break;
            }
        }
        if !finished {
            return Err(syntax(self.pos, "parser made no progress"));
        }
        self.skip_ws();
        if self.pos < self.s.len() {
            return Err(syntax(self.pos, "trailing characters after the document"));
        }
        self.builder.finish()
    }

    fn parse_value(&mut self) -> Result<Flow, JsonbError> {
        self.skip_ws();
        let at = self.pos;
        let Some(first) = self.peek() else {
            return Err(syntax(at, "unexpected end of input"));
        };
        if let Some(top) = self.frames.last_mut() {
            top.index += 1;
        }
        match first {
            b'{' => return self.open(true),
            b'[' => return self.open(false),
            b'"' => {
                let text = self.string()?;
                self.builder.string(&text)?;
            }
            b't' => self.literal(b"true", true)?,
            b'f' => self.literal(b"false", false)?,
            b'n' => {
                self.word(b"null")?;
                self.builder.null()?;
            }
            b'-' | b'0'..=b'9' => {
                let n = self.number()?;
                self.builder.number(n)?;
            }
            _ => return Err(syntax(at, "unexpected character")),
        }
        Ok(Flow::Complete)
    }

    fn word(&mut self, word: &[u8]) -> Result<(), JsonbError> {
        let end = self.pos + word.len();
        if self.s.get(self.pos..end) != Some(word) {
            return Err(syntax(self.pos, "invalid literal"));
        }
        self.pos = end;
        Ok(())
    }

    fn literal(&mut self, word: &[u8], value: bool) -> Result<(), JsonbError> {
        self.word(word)?;
        self.builder.boolean(value)
    }

    fn open(&mut self, object: bool) -> Result<Flow, JsonbError> {
        if object {
            self.builder.begin_object()?;
        } else {
            self.builder.begin_array()?;
        }
        self.pos += 1;
        self.frames.push(Frame {
            object,
            key: None,
            index: 0,
        });
        self.skip_ws();
        let closer = if object { b'}' } else { b']' };
        if self.peek() == Some(closer) {
            self.pos += 1;
            self.close(object)?;
            return Ok(Flow::Complete);
        }
        if object {
            self.key()?;
        }
        Ok(Flow::Opened)
    }

    fn close(&mut self, object: bool) -> Result<(), JsonbError> {
        self.frames.pop();
        let result = if object {
            self.builder.end_object()
        } else {
            self.builder.end_array()
        };
        match result {
            Err(JsonbError::DuplicateKey { .. }) => {
                Err(JsonbError::DuplicateKey { path: self.path() })
            }
            other => other,
        }
    }

    /// Path of the innermost open position: `$`, `$.a`, `$.a[2]`.
    fn path(&self) -> String {
        let mut path = String::from("$");
        for frame in &self.frames {
            match (&frame.key, frame.object) {
                (Some(k), true) => {
                    path.push('.');
                    path.push_str(k);
                }
                (_, false) => path.push_str(&format!("[{}]", frame.index.saturating_sub(1))),
                (None, true) => {}
            }
        }
        path
    }

    fn key(&mut self) -> Result<(), JsonbError> {
        self.skip_ws();
        if self.peek() != Some(b'"') {
            return Err(syntax(self.pos, "expected an object key"));
        }
        let key = self.string()?;
        self.skip_ws();
        if self.peek() != Some(b':') {
            return Err(syntax(self.pos, "expected ':' after an object key"));
        }
        self.pos += 1;
        self.builder.key(&key)?;
        if let Some(top) = self.frames.last_mut() {
            top.key = Some(key);
        }
        Ok(())
    }

    /// Consume separators and closers after a value. `true` means the
    /// document's root value is complete; `false` means a value must follow.
    fn after_value(&mut self) -> Result<bool, JsonbError> {
        for _ in 0..=self.frames.len() {
            let Some(object) = self.frames.last().map(|f| f.object) else {
                return Ok(true);
            };
            self.skip_ws();
            match (self.peek(), object) {
                (Some(b','), _) => {
                    self.pos += 1;
                    if object {
                        self.key()?;
                    }
                    return Ok(false);
                }
                (Some(b'}'), true) => {
                    self.pos += 1;
                    self.close(true)?;
                }
                (Some(b']'), false) => {
                    self.pos += 1;
                    self.close(false)?;
                }
                (None, _) => return Err(syntax(self.pos, "unexpected end of input")),
                (Some(_), _) => return Err(syntax(self.pos, "expected ',' or a closing bracket")),
            }
        }
        Ok(self.frames.is_empty())
    }

    fn digits(&mut self) -> usize {
        let start = self.pos;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        self.pos - start
    }

    fn number(&mut self) -> Result<Number, JsonbError> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        match self.peek() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                self.digits();
            }
            _ => return Err(syntax(self.pos, "expected a digit")),
        }
        if self.peek() == Some(b'.') {
            self.pos += 1;
            if self.digits() == 0 {
                return Err(syntax(self.pos, "expected a digit after '.'"));
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if self.digits() == 0 {
                return Err(syntax(self.pos, "expected a digit in the exponent"));
            }
        }
        let raw = self
            .s
            .get(start..self.pos)
            .ok_or(syntax(start, "number range out of bounds"))?;
        let lexeme = std::str::from_utf8(raw).map_err(|_| syntax(start, "number is not ASCII"))?;
        Number::parse_lexeme(lexeme).map_err(|e| match e {
            JsonbError::InvalidNumber { offset } => JsonbError::InvalidNumber {
                offset: start + offset,
            },
            other => other,
        })
    }

    /// Validate the raw run `[from, pos)` as UTF-8 and append it. A multi-byte
    /// character never contains an ASCII delimiter, so runs split cleanly.
    fn push_run(&self, from: usize, out: &mut String) -> Result<(), JsonbError> {
        let raw = self
            .s
            .get(from..self.pos)
            .ok_or(syntax(from, "string range out of bounds"))?;
        match std::str::from_utf8(raw) {
            Ok(text) => {
                out.push_str(text);
                Ok(())
            }
            Err(e) => Err(JsonbError::InvalidUtf8 {
                offset: from + e.valid_up_to(),
            }),
        }
    }

    fn string(&mut self) -> Result<String, JsonbError> {
        let start = self.pos;
        self.pos += 1;
        let mut out = String::new();
        for _ in 0..=self.s.len() {
            let run = self.pos;
            while matches!(self.peek(), Some(b) if b != b'"' && b != b'\\' && b >= 0x20) {
                self.pos += 1;
            }
            self.push_run(run, &mut out)?;
            match self.peek() {
                None => return Err(syntax(start, "unterminated string")),
                Some(b'"') => {
                    self.pos += 1;
                    return Ok(out);
                }
                Some(b'\\') => self.escape(&mut out)?,
                Some(_) => return Err(syntax(self.pos, "control character in string")),
            }
        }
        Err(syntax(start, "unterminated string"))
    }

    fn hex4(&self, at: usize) -> Option<u32> {
        let digits = self.s.get(at..at + 4)?;
        digits.iter().try_fold(0u32, |acc, d| {
            char::from(*d).to_digit(16).map(|v| acc * 16 + v)
        })
    }

    fn escape(&mut self, out: &mut String) -> Result<(), JsonbError> {
        let at = self.pos;
        let simple = match self.s.get(at + 1).copied() {
            Some(b'"') => Some('"'),
            Some(b'\\') => Some('\\'),
            Some(b'/') => Some('/'),
            Some(b'b') => Some('\u{8}'),
            Some(b'f') => Some('\u{c}'),
            Some(b'n') => Some('\n'),
            Some(b'r') => Some('\r'),
            Some(b't') => Some('\t'),
            Some(b'u') => None,
            _ => return Err(syntax(at, "invalid escape")),
        };
        if let Some(c) = simple {
            out.push(c);
            self.pos += 2;
            return Ok(());
        }
        let c = self.unicode_escape(at)?;
        out.push(c);
        Ok(())
    }

    /// `\uXXXX`, joining a surrogate pair; a lone surrogate is an error at the
    /// backslash of the offending escape.
    fn unicode_escape(&mut self, at: usize) -> Result<char, JsonbError> {
        let first = self.hex4(at + 2).ok_or(syntax(at, "invalid \\u escape"))?;
        self.pos = at + 6;
        let code = match first {
            0xD800..=0xDBFF => {
                let is_escape = self.s.get(self.pos..self.pos + 2) == Some(b"\\u");
                let low = if is_escape {
                    self.hex4(self.pos + 2)
                } else {
                    None
                };
                let Some(low @ 0xDC00..=0xDFFF) = low else {
                    return Err(syntax(at, "lone surrogate in \\u escape"));
                };
                self.pos += 6;
                0x10000 + ((first - 0xD800) << 10) + (low - 0xDC00)
            }
            0xDC00..=0xDFFF => return Err(syntax(at, "lone surrogate in \\u escape")),
            other => other,
        };
        char::from_u32(code).ok_or(syntax(at, "invalid code point in \\u escape"))
    }
}
