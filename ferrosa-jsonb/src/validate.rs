//! Module: the jsonb cell validator (T-104, D4, D14b, D14d, architecture 3.2 C1-C11).
//! Correctness: correct when it accepts a byte string iff that string is the
//! canonical encoding of the value it holds: known envelope, sound metadata,
//! sorted unique dictionary with every key used, monotone in-bounds contiguous
//! offsets, ascending in-range field ids, defined primitives at their smallest
//! kind, valid UTF-8, a well-formed bigdecimal within the D14a caps, depth at or
//! below the hard ceiling and the value ending at the cell's last byte. Reads
//! check `HardCeilings` only; no tunable `Limits` reaches this module (FM-17).
//! The walk is iterative with a frame stack capped at the depth ceiling (M6), and
//! no allocation is sized from a claimed count.
//! Last revised: 2026-09-28
//! Last changed: T-104 initial validator.

use crate::encode::{BIGDECIMAL_PRIMITIVE_ID, ENVELOPE};
use crate::error::{EncodingFault, JsonbError};
use crate::limits::{HardCeilings, HARD_MAX_DEPTH};
use crate::number::NumberKind;
use crate::reader::{decode_number, exact, fault, min_width, primitive_fault, str_of, Head, Meta};

/// Strings longer than this use primitive 16 (C7).
const SHORT_STRING_MAX: usize = 63;
const LARGE_COUNT: usize = 255;

/// Validate a whole cell; returns its metadata and the root value's bytes.
pub(crate) fn validate_cell(cell: &[u8]) -> Result<(Meta<'_>, &[u8]), JsonbError> {
    HardCeilings::CURRENT.check_encoded_len(cell.len())?;
    let (envelope, rest) = cell.split_first().ok_or(fault(EncodingFault::Truncated))?;
    if *envelope != ENVELOPE {
        return Err(JsonbError::UnknownEnvelope { byte: *envelope });
    }
    let meta = Meta::parse(rest)?;
    let root = rest
        .get(meta.len()..)
        .ok_or(fault(EncodingFault::Truncated))?;
    Walker::new(meta).run(root)?;
    Ok((meta, root))
}

struct Frame<'a> {
    head: Head<'a>,
    next: usize,
    prev_id: Option<usize>,
}

struct Walker<'a> {
    meta: Meta<'a>,
    used: Vec<u64>,
    stack: Vec<Frame<'a>>,
}

impl<'a> Walker<'a> {
    fn new(meta: Meta<'a>) -> Walker<'a> {
        // The dictionary count is bounded by the metadata bytes already checked.
        Walker {
            meta,
            used: vec![0; meta.count().div_ceil(64)],
            stack: Vec::new(),
        }
    }

    /// Depth-first over an explicit stack; each loop turn consumes one value.
    fn run(&mut self, root: &'a [u8]) -> Result<(), JsonbError> {
        self.enter(root)?;
        while let Some(span) = self.next_child()? {
            self.enter(span)?;
        }
        self.check_all_keys_used()
    }

    /// The next unvisited child of the innermost open container, popping
    /// finished containers.
    fn next_child(&mut self) -> Result<Option<&'a [u8]>, JsonbError> {
        while let Some(frame) = self.stack.last_mut() {
            if frame.next >= frame.head.count {
                self.stack.pop();
                continue;
            }
            let i = frame.next;
            frame.next += 1;
            if frame.head.is_object {
                let id = frame.head.id(i)?;
                mark_field(&mut self.used, &self.meta, &mut frame.prev_id, id)?;
            }
            return frame.head.child(i).map(Some);
        }
        Ok(None)
    }

    fn enter(&mut self, span: &'a [u8]) -> Result<(), JsonbError> {
        let h = span
            .first()
            .copied()
            .ok_or(fault(EncodingFault::Truncated))?;
        if h & 3 >= 2 {
            let head = Head::parse(span)?;
            check_container(&head)?;
            let depth = u32::try_from(self.stack.len() + 1).unwrap_or(u32::MAX);
            if depth > HARD_MAX_DEPTH {
                return Err(JsonbError::DepthExceeded {
                    depth,
                    max: HARD_MAX_DEPTH,
                });
            }
            self.stack.push(Frame {
                head,
                next: 0,
                prev_id: None,
            });
            Ok(())
        } else {
            check_scalar(span)
        }
    }

    /// C3: the dictionary holds exactly the keys the value uses.
    fn check_all_keys_used(&self) -> Result<(), JsonbError> {
        let used: usize = self.used.iter().map(|w| w.count_ones() as usize).sum();
        if used != self.meta.count() {
            return Err(fault(EncodingFault::NonCanonical("C3")));
        }
        Ok(())
    }
}

/// Field ids ascend strictly (so keys are sorted and unique) and are in range.
fn mark_field(
    used: &mut [u64],
    meta: &Meta<'_>,
    prev: &mut Option<usize>,
    id: usize,
) -> Result<(), JsonbError> {
    if id >= meta.count() {
        return Err(fault(EncodingFault::FieldIdOutOfRange));
    }
    if prev.is_some_and(|p| id <= p) {
        return Err(fault(EncodingFault::FieldsNotSorted));
    }
    *prev = Some(id);
    let word = used
        .get_mut(id / 64)
        .ok_or(fault(EncodingFault::FieldIdOutOfRange))?;
    *word |= 1u64 << (id % 64);
    Ok(())
}

/// C5, C6: contiguous children, minimal widths, `is_large` iff count > 255.
fn check_container(head: &Head<'_>) -> Result<(), JsonbError> {
    let rule = if head.is_object { "C5" } else { "C6" };
    let non_canonical = fault(EncodingFault::NonCanonical(rule));
    if head.offset(0)? != 0 {
        return Err(fault(EncodingFault::OffsetsNotMonotonic));
    }
    let total = head.offset(head.count)?;
    match total.cmp(&head.data_len()) {
        std::cmp::Ordering::Greater => return Err(fault(EncodingFault::OffsetOutOfBounds)),
        std::cmp::Ordering::Less => return Err(fault(EncodingFault::TrailingBytes)),
        std::cmp::Ordering::Equal => {}
    }
    if head.large != (head.count > LARGE_COUNT) || head.off_w != min_width(total) {
        return Err(non_canonical);
    }
    if head.is_object {
        let last = match head.count {
            0 => 0,
            n => head.id(n - 1)?,
        };
        if head.id_w != min_width(last) {
            return Err(non_canonical);
        }
    }
    Ok(())
}

fn expected_kind(id: u8) -> Option<NumberKind> {
    match id {
        3 => Some(NumberKind::Int8),
        4 => Some(NumberKind::Int16),
        5 => Some(NumberKind::Int32),
        6 => Some(NumberKind::Int64),
        8 => Some(NumberKind::Decimal4),
        9 => Some(NumberKind::Decimal8),
        10 => Some(NumberKind::Decimal16),
        BIGDECIMAL_PRIMITIVE_ID => Some(NumberKind::BigDecimal),
        _ => None,
    }
}

/// C7-C10: a scalar spans exactly its bytes and uses its canonical form.
fn check_scalar(span: &[u8]) -> Result<(), JsonbError> {
    let h = span
        .first()
        .copied()
        .ok_or(fault(EncodingFault::Truncated))?;
    if h & 3 == 1 {
        return str_of(span).map(|_| ());
    }
    let id = h >> 2;
    match id {
        0..=2 => exact(span, 1),
        16 => {
            let (text, long) = str_of(span)?;
            if long && text.len() <= SHORT_STRING_MAX {
                return Err(fault(EncodingFault::NonCanonical("C7")));
            }
            Ok(())
        }
        _ => match expected_kind(id) {
            Some(want) if decode_number(span)?.kind() == want => Ok(()),
            Some(_) => Err(fault(EncodingFault::NonCanonical("C9"))),
            None => Err(primitive_fault(id)),
        },
    }
}
