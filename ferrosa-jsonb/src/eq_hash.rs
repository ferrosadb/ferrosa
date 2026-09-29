//! Module: value `Eq`, normalized `Hash` and canonical-by-value `Debug` for
//! `JsonbValue` (T-106, D2a, FM-09, FM-11, FM-104).
//! Correctness: correct when `a == b` <=> `a.cmp(b) == Equal` <=> equal hash
//! <=> equal `Debug`. Numbers hash and print in their trailing-zero-free form,
//! so `1`, `1.0` and a padded bigdecimal `1` agree; objects walk in stored
//! (bytewise) key order, which is canonical, so insertion order never shows.
//! `Debug` is untruncated and injective: strings are quoted with escapes, every
//! container is delimited, so distinct values never print alike (DISTINCT keys
//! on this text today). Both walks are iterative. A reader fault (impossible for
//! a validated value) hashes the raw cell and prints a marker, and bumps
//! `comparison_faults`; it is never silent.
//! Last revised: 2026-09-28
//! Last changed: T-106 initial Eq, Hash and Debug.

use std::cmp::Ordering;
use std::fmt::{self, Write};
use std::hash::{Hash, Hasher};

use crate::error::JsonbError;
use crate::order::record_fault;
use crate::reader::{ValueKind, ValueRef};
use crate::value::JsonbValue;

impl PartialEq for JsonbValue {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes() || self.cmp(other) == Ordering::Equal
    }
}

impl Eq for JsonbValue {}

/// One pending item of a walk.
enum Item<'a> {
    Key(&'a str),
    Val(ValueRef<'a>),
}

/// The children of a container as walk items, in stored key order.
fn children<'a>(v: ValueRef<'a>, kind: ValueKind) -> Result<Vec<Item<'a>>, JsonbError> {
    let mut out = Vec::new();
    if kind == ValueKind::Object {
        for e in v.as_object()?.iter() {
            let (k, x) = e?;
            out.push(Item::Key(k));
            out.push(Item::Val(x));
        }
    } else {
        for e in v.as_array()?.iter() {
            out.push(Item::Val(e?));
        }
    }
    Ok(out)
}

fn hash_str<H: Hasher>(s: &str, state: &mut H) {
    state.write_usize(s.len());
    state.write(s.as_bytes());
}

/// Hash one value's head; returns its children to walk, if any.
fn hash_head<'a, H: Hasher>(v: ValueRef<'a>, state: &mut H) -> Result<Vec<Item<'a>>, JsonbError> {
    let kind = v.kind()?;
    match kind {
        ValueKind::Null => state.write_u8(0),
        ValueKind::Bool => state.write_u8(if v.as_bool()? { 2 } else { 1 }),
        ValueKind::Number => {
            state.write_u8(3);
            v.as_number()?.hash(state);
        }
        ValueKind::String => {
            state.write_u8(4);
            hash_str(v.as_str()?, state);
        }
        ValueKind::Array | ValueKind::Object => {
            let items = children(v, kind)?;
            state.write_u8(if kind == ValueKind::Array { 5 } else { 6 });
            state.write_usize(items.len());
            return Ok(items);
        }
    }
    Ok(Vec::new())
}

fn hash_walk<H: Hasher>(root: ValueRef<'_>, state: &mut H) -> Result<(), JsonbError> {
    let mut stack = vec![Item::Val(root)];
    while let Some(item) = stack.pop() {
        match item {
            Item::Key(k) => hash_str(k, state),
            Item::Val(v) => stack.extend(hash_head(v, state)?.into_iter().rev()),
        }
    }
    Ok(())
}

impl Hash for JsonbValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let walked = self.view().and_then(|cell| hash_walk(cell.root(), state));
        if walked.is_err() {
            record_fault();
            state.write(self.as_bytes());
        }
    }
}

/// A pending piece of `Debug` text.
enum Text<'a> {
    Lit(&'static str),
    Str(&'a str),
    Val(ValueRef<'a>),
}

fn open_container<'a>(
    v: ValueRef<'a>,
    kind: ValueKind,
    stack: &mut Vec<Text<'a>>,
) -> Result<&'static str, JsonbError> {
    let (open, close) = if kind == ValueKind::Array {
        ("[", "]")
    } else {
        ("{", "}")
    };
    let mut parts = Vec::new();
    let mut first = true;
    for item in children(v, kind)? {
        match item {
            Item::Key(k) => {
                if !first {
                    parts.push(Text::Lit(","));
                }
                parts.extend([Text::Str(k), Text::Lit(":")]);
            }
            Item::Val(x) => {
                if kind == ValueKind::Array && !first {
                    parts.push(Text::Lit(","));
                }
                parts.push(Text::Val(x));
            }
        }
        first = false;
    }
    stack.push(Text::Lit(close));
    stack.extend(parts.into_iter().rev());
    Ok(open)
}

fn write_val<'a, W: Write>(
    v: ValueRef<'a>,
    out: &mut W,
    stack: &mut Vec<Text<'a>>,
) -> Result<(), JsonbError> {
    let kind = v.kind()?;
    let text = match kind {
        ValueKind::Null => Some("null".to_string()),
        ValueKind::Bool => Some(v.as_bool()?.to_string()),
        ValueKind::Number => Some(v.as_number()?.value_normalized().to_string()),
        ValueKind::String => Some(format!("{:?}", v.as_str()?)),
        ValueKind::Array | ValueKind::Object => None,
    };
    let piece = match text {
        Some(t) => t,
        None => open_container(v, kind, stack)?.to_string(),
    };
    out.write_str(&piece)
        .map_err(|_| JsonbError::BuilderMisuse {
            reason: "debug sink failed",
        })
}

fn debug_walk<W: Write>(root: ValueRef<'_>, out: &mut W) -> Result<(), JsonbError> {
    let sink = |_| JsonbError::BuilderMisuse {
        reason: "debug sink failed",
    };
    let mut stack = vec![Text::Val(root)];
    while let Some(t) = stack.pop() {
        match t {
            Text::Lit(s) => out.write_str(s).map_err(sink)?,
            Text::Str(s) => write!(out, "{s:?}").map_err(sink)?,
            Text::Val(v) => write_val(v, out, &mut stack)?,
        }
    }
    Ok(())
}

impl fmt::Debug for JsonbValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Jsonb(")?;
        let walked = self.view().and_then(|cell| debug_walk(cell.root(), f));
        if let Err(e) = walked {
            record_fault();
            write!(f, "<fault: {e}>")?;
        }
        f.write_str(")")
    }
}
