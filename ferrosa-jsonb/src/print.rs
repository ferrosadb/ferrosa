//! Module: the one jsonb text printer, in three frozen styles (T-105, D6, D2a,
//! D13a, D26; FM-12, FM-106, JB-D4, JB-T6).
//! Correctness: correct when `Canonical` prints keys bytewise, compact, numbers
//! plain with their scale; `PgText` is byte-identical to PostgreSQL 16 jsonb
//! output; `Normalized` is `Canonical` with trailing fractional zeros removed
//! (the D13a hash input); output is checked against a byte budget BEFORE each
//! write, so an over-budget print is a typed error; and nesting never recurses.
//! Last revised: 2026-09-28
//! Last changed: T-105 initial printer.
//!
//! The walk keeps an explicit frame stack (depth is attacker-controlled up to
//! `HARD_MAX_DEPTH`). The only per-object allocation is the `PgText` key reorder
//! (JB-T105-03): PG orders keys by byte length then bytewise, which differs from
//! the stored bytewise order, so one object's entries are sorted at a time; the
//! count is bounded by the validated cell size.
//!
//! A streaming sink may already hold a prefix when an error is returned; only
//! [`print_to_string`] guarantees no partial text escapes.

use std::fmt::{self, Write};
use std::vec;

use thiserror::Error;

use crate::error::{EncodingFault, JsonbError};
use crate::number::Number;
use crate::reader::{ArrayIter, ObjectIter, ValueKind, ValueRef};

/// Which text form to print.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextStyle {
    /// Compact, keys bytewise as stored, numbers plain with scale (D6, D2a).
    Canonical,
    /// PostgreSQL 16 jsonb text: keys by byte length then bytewise, `": "` and
    /// `", "` separators, plain numeric with scale (D26).
    PgText,
    /// `Canonical` with trailing fractional zeros stripped: `1`, `1.0` and
    /// `1.00` print alike. The D13a RDF hash input and the `Debug` text.
    Normalized,
}

/// Why a print failed. Nothing is truncated: a failure is always an error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PrintError {
    /// The text would exceed the output byte budget (JB-D4).
    #[error("jsonb text output would exceed the budget of {max} bytes")]
    BudgetExceeded { max: usize },
    /// The sink refused a write.
    #[error("the jsonb text sink refused a write")]
    Sink,
    /// The cell is malformed (a reader fault).
    #[error(transparent)]
    Jsonb(#[from] JsonbError),
}

impl From<fmt::Error> for PrintError {
    fn from(_: fmt::Error) -> Self {
        PrintError::Sink
    }
}

/// Print `root` in `style` into `out`, at most `budget` bytes. Returns the
/// number of bytes written.
pub fn print_value<W: Write>(
    root: ValueRef<'_>,
    style: TextStyle,
    budget: usize,
    out: &mut W,
) -> Result<usize, PrintError> {
    let mut printer = Printer {
        out,
        style,
        budget,
        written: 0,
        scratch: String::new(),
        stack: Vec::new(),
    };
    printer.run(root)?;
    Ok(printer.written)
}

/// Print `root` to a new `String`; an over-budget print returns the error and
/// no text.
pub fn print_to_string(
    root: ValueRef<'_>,
    style: TextStyle,
    budget: usize,
) -> Result<String, PrintError> {
    let mut text = String::new();
    print_value(root, style, budget, &mut text)?;
    Ok(text)
}

type Entry<'a> = (&'a str, ValueRef<'a>);

enum Items<'a> {
    Array(ArrayIter<'a>),
    Object(ObjectIter<'a>),
    Sorted(vec::IntoIter<Entry<'a>>),
}

struct Frame<'a> {
    items: Items<'a>,
    close: char,
    first: bool,
}

/// One member: `sep` says a separator precedes it, `key` is set in objects.
struct Member<'a> {
    sep: bool,
    key: Option<&'a str>,
    value: ValueRef<'a>,
}

impl<'a> Frame<'a> {
    fn next_member(&mut self) -> Result<Option<Member<'a>>, JsonbError> {
        let (key, value) = match &mut self.items {
            Items::Array(it) => match it.next().transpose()? {
                Some(v) => (None, v),
                None => return Ok(None),
            },
            Items::Object(it) => match it.next().transpose()? {
                Some((k, v)) => (Some(k), v),
                None => return Ok(None),
            },
            Items::Sorted(it) => match it.next() {
                Some((k, v)) => (Some(k), v),
                None => return Ok(None),
            },
        };
        let sep = !self.first;
        self.first = false;
        Ok(Some(Member { sep, key, value }))
    }
}

struct Printer<'w, 'a, W: Write> {
    out: &'w mut W,
    style: TextStyle,
    budget: usize,
    written: usize,
    scratch: String,
    stack: Vec<Frame<'a>>,
}

/// PG order (jsonb_util.c `lengthCompareJsonbStringValue`): byte length, then bytes.
fn pg_key_order(a: &Entry<'_>, b: &Entry<'_>) -> std::cmp::Ordering {
    a.0.len()
        .cmp(&b.0.len())
        .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
}

impl<'w, 'a, W: Write> Printer<'w, 'a, W> {
    fn run(&mut self, root: ValueRef<'a>) -> Result<(), PrintError> {
        self.enter(root)?;
        while let Some(frame) = self.stack.last_mut() {
            let close = frame.close;
            match frame.next_member()? {
                Some(m) => self.member(m)?,
                None => {
                    self.stack.pop();
                    self.emit_char(close)?;
                }
            }
        }
        Ok(())
    }

    fn member(&mut self, m: Member<'a>) -> Result<(), PrintError> {
        if m.sep {
            self.emit(self.sep())?;
        }
        if let Some(key) = m.key {
            self.string(key)?;
            self.emit(self.colon())?;
        }
        self.enter(m.value)
    }

    fn sep(&self) -> &'static str {
        match self.style {
            TextStyle::PgText => ", ",
            TextStyle::Canonical | TextStyle::Normalized => ",",
        }
    }

    fn colon(&self) -> &'static str {
        match self.style {
            TextStyle::PgText => ": ",
            TextStyle::Canonical | TextStyle::Normalized => ":",
        }
    }

    /// Write a scalar, or open a container and push its frame.
    fn enter(&mut self, v: ValueRef<'a>) -> Result<(), PrintError> {
        match v.kind()? {
            ValueKind::Null => self.emit("null"),
            ValueKind::Bool => self.emit(if v.as_bool()? { "true" } else { "false" }),
            ValueKind::Number => self.number(&v.as_number()?),
            ValueKind::String => self.string(v.as_str()?),
            ValueKind::Array => {
                let items = Items::Array(v.as_array()?.iter());
                self.open('[', ']', items)
            }
            ValueKind::Object => {
                let items = self.object_items(v)?;
                self.open('{', '}', items)
            }
        }
    }

    fn object_items(&self, v: ValueRef<'a>) -> Result<Items<'a>, PrintError> {
        let obj = v.as_object()?;
        if self.style != TextStyle::PgText {
            return Ok(Items::Object(obj.iter()));
        }
        let mut entries: Vec<Entry<'a>> = Vec::with_capacity(obj.len());
        for entry in obj.iter() {
            entries.push(entry?);
        }
        entries.sort_by(pg_key_order);
        Ok(Items::Sorted(entries.into_iter()))
    }

    fn open(&mut self, open: char, close: char, items: Items<'a>) -> Result<(), PrintError> {
        self.emit_char(open)?;
        self.stack.push(Frame {
            items,
            close,
            first: true,
        });
        Ok(())
    }

    fn number(&mut self, n: &Number) -> Result<(), PrintError> {
        self.scratch.clear();
        match self.style {
            TextStyle::Normalized => write!(self.scratch, "{}", n.value_normalized())?,
            TextStyle::Canonical | TextStyle::PgText => write!(self.scratch, "{n}")?,
        }
        let text = std::mem::take(&mut self.scratch);
        let result = self.emit(&text);
        self.scratch = text;
        result
    }

    /// PG `escape_json`: `\"` `\\` `\b` `\f` `\n` `\r` `\t`, other C0 controls as
    /// `\u00xx`, everything else raw UTF-8.
    fn string(&mut self, s: &str) -> Result<(), PrintError> {
        self.emit_char('"')?;
        let mut run_start = 0usize;
        for (i, c) in s.char_indices() {
            let Some(esc) = escape_of(c) else { continue };
            self.emit(slice(s, run_start, i)?)?;
            self.scratch.clear();
            esc.write_into(&mut self.scratch)?;
            let text = std::mem::take(&mut self.scratch);
            let result = self.emit(&text);
            self.scratch = text;
            result?;
            run_start = i + c.len_utf8();
        }
        self.emit(slice(s, run_start, s.len())?)?;
        self.emit_char('"')
    }

    fn emit_char(&mut self, c: char) -> Result<(), PrintError> {
        let mut buf = [0u8; 4];
        self.emit(c.encode_utf8(&mut buf))
    }

    /// The single write path: check the budget first, then write.
    fn emit(&mut self, text: &str) -> Result<(), PrintError> {
        let total = self.written.saturating_add(text.len());
        if total > self.budget {
            return Err(PrintError::BudgetExceeded { max: self.budget });
        }
        self.out.write_str(text)?;
        self.written = total;
        Ok(())
    }
}

/// `s[from..to]`, or a typed fault; both bounds are char boundaries by
/// construction, so the fault path is unreachable for a valid `&str`.
fn slice(s: &str, from: usize, to: usize) -> Result<&str, PrintError> {
    s.get(from..to)
        .ok_or(PrintError::Jsonb(JsonbError::InvalidEncoding {
            reason: EncodingFault::InvalidUtf8,
        }))
}

enum Escape {
    Short(&'static str),
    Control(u8),
}

impl Escape {
    fn write_into(&self, out: &mut String) -> fmt::Result {
        match self {
            Escape::Short(s) => out.write_str(s),
            Escape::Control(b) => write!(out, "\\u{b:04x}"),
        }
    }
}

fn escape_of(c: char) -> Option<Escape> {
    match c {
        '"' => Some(Escape::Short("\\\"")),
        '\\' => Some(Escape::Short("\\\\")),
        '\u{8}' => Some(Escape::Short("\\b")),
        '\u{c}' => Some(Escape::Short("\\f")),
        '\n' => Some(Escape::Short("\\n")),
        '\r' => Some(Escape::Short("\\r")),
        '\t' => Some(Escape::Short("\\t")),
        c if c < ' ' => u8::try_from(u32::from(c)).ok().map(Escape::Control),
        _ => None,
    }
}
