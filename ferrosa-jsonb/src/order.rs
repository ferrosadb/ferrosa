//! Module: the one total order over jsonb values (T-106, D18, D18a, D2a).
//! Correctness: correct when the order is PostgreSQL's (kind rank Object >
//! Array > Boolean > Number > String > Null; objects by pair count, then by
//! (key, value) pairs in PG key order, shortest key first then bytewise; arrays
//! by length then element-wise; numbers by exact value; strings bytewise), no
//! pair is incomparable, and nothing is mapped to `Equal` by fallback. The walk
//! is iterative (an explicit work stack), so depth 1000 needs no native stack.
//! A reader fault cannot happen on a validated `JsonbValue`; if memory were
//! corrupted, `Ord` falls back to comparing the cells' bytes (still total,
//! never `Equal` for different bytes) and bumps [`comparison_faults`].
//! Last revised: 2026-09-28
//! Last changed: T-106 initial comparator.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use crate::error::JsonbError;
use crate::reader::{ValueKind, ValueRef};
use crate::value::JsonbValue;

static FAULTS: AtomicU64 = AtomicU64::new(0);

/// How many comparisons, hashes or debug prints hit a reader fault on a value
/// that was validated at construction (JB-T106-01). Nonzero means corruption:
/// export it as a metric and alert on any increase.
pub fn comparison_faults() -> u64 {
    FAULTS.load(AtomicOrdering::Relaxed)
}

pub(crate) fn record_fault() {
    FAULTS.fetch_add(1, AtomicOrdering::Relaxed);
}

/// One pending comparison on the explicit stack.
enum Work<'a> {
    Vals(ValueRef<'a>, ValueRef<'a>),
    Keys(&'a str, &'a str),
}

fn rank(kind: ValueKind) -> u8 {
    match kind {
        ValueKind::Null => 0,
        ValueKind::String => 1,
        ValueKind::Number => 2,
        ValueKind::Bool => 3,
        ValueKind::Array => 4,
        ValueKind::Object => 5,
    }
}

/// PostgreSQL key order: shortest first, then bytewise (D18a).
fn key_order(a: &str, b: &str) -> Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

fn push_array<'a>(
    a: ValueRef<'a>,
    b: ValueRef<'a>,
    stack: &mut Vec<Work<'a>>,
) -> Result<(), JsonbError> {
    let mut items = Vec::new();
    for (x, y) in a.as_array()?.iter().zip(b.as_array()?.iter()) {
        items.push(Work::Vals(x?, y?));
    }
    stack.extend(items.into_iter().rev());
    Ok(())
}

fn pg_sorted<'a>(v: ValueRef<'a>) -> Result<Vec<(&'a str, ValueRef<'a>)>, JsonbError> {
    let mut entries = Vec::new();
    for e in v.as_object()?.iter() {
        entries.push(e?);
    }
    entries.sort_by(|x, y| key_order(x.0, y.0));
    Ok(entries)
}

fn push_object<'a>(
    a: ValueRef<'a>,
    b: ValueRef<'a>,
    stack: &mut Vec<Work<'a>>,
) -> Result<(), JsonbError> {
    let (ea, eb) = (pg_sorted(a)?, pg_sorted(b)?);
    let mut items = Vec::new();
    for ((ka, va), (kb, vb)) in ea.into_iter().zip(eb) {
        items.push(Work::Keys(ka, kb));
        items.push(Work::Vals(va, vb));
    }
    stack.extend(items.into_iter().rev());
    Ok(())
}

fn container_len(v: ValueRef<'_>, kind: ValueKind) -> Result<usize, JsonbError> {
    match kind {
        ValueKind::Object => Ok(v.as_object()?.len()),
        _ => Ok(v.as_array()?.len()),
    }
}

/// Compare one pair of values; containers of equal size push their children.
fn step<'a>(
    a: ValueRef<'a>,
    b: ValueRef<'a>,
    stack: &mut Vec<Work<'a>>,
) -> Result<Ordering, JsonbError> {
    let (ka, kb) = (a.kind()?, b.kind()?);
    let by_rank = rank(ka).cmp(&rank(kb));
    if by_rank != Ordering::Equal {
        return Ok(by_rank);
    }
    match ka {
        ValueKind::Null => Ok(Ordering::Equal),
        ValueKind::Bool => Ok(a.as_bool()?.cmp(&b.as_bool()?)),
        ValueKind::Number => Ok(a.as_number()?.cmp(&b.as_number()?)),
        ValueKind::String => Ok(a.as_str()?.as_bytes().cmp(b.as_str()?.as_bytes())),
        ValueKind::Array | ValueKind::Object => {
            let by_len = container_len(a, ka)?.cmp(&container_len(b, kb)?);
            if by_len != Ordering::Equal {
                return Ok(by_len);
            }
            if ka == ValueKind::Array {
                push_array(a, b, stack)?;
            } else {
                push_object(a, b, stack)?;
            }
            Ok(Ordering::Equal)
        }
    }
}

/// Compare two values in D18 order, reporting a reader fault as an error.
pub fn compare(a: ValueRef<'_>, b: ValueRef<'_>) -> Result<Ordering, JsonbError> {
    let mut stack = vec![Work::Vals(a, b)];
    while let Some(work) = stack.pop() {
        let order = match work {
            Work::Keys(x, y) => key_order(x, y),
            Work::Vals(x, y) => step(x, y, &mut stack)?,
        };
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(Ordering::Equal)
}

impl JsonbValue {
    /// The D18 order, or the reader fault (impossible for a validated value).
    pub fn try_cmp(&self, other: &JsonbValue) -> Result<Ordering, JsonbError> {
        compare(self.view()?.root(), other.view()?.root())
    }
}

impl Ord for JsonbValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.try_cmp(other) {
            Ok(order) => order,
            Err(_) => {
                record_fault();
                self.as_bytes().cmp(other.as_bytes())
            }
        }
    }
}

impl PartialOrd for JsonbValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
