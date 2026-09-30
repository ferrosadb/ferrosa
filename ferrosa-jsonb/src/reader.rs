//! Module: the checked zero-copy reader over a validated jsonb cell (T-104, D4,
//! D14b, architecture 3.2).
//! Correctness: correct when every access goes through `slice::get` or checked
//! arithmetic, so a byte string that somehow escaped validation yields a typed
//! error and never a panic or out-of-bounds read; when strings and objects borrow
//! from the cell (zero copy); when `ObjectRef::get` binary-searches the sorted
//! field ids by key bytes; and when nothing recurses (callers walk with their
//! own stack over `ObjectIter` and `ArrayIter`).
//! Last revised: 2026-09-28
//! Last changed: T-104 initial reader.

use num_bigint::{BigInt, BigUint};

use crate::encode::BIGDECIMAL_PRIMITIVE_ID;
use crate::error::{EncodingFault, JsonbError};
use crate::number::Number;
use crate::validate;

/// Most bytes a canonical bigdecimal body can hold within the D14a digit caps:
/// 147 455 digits are 489 8xx bits, 61 23x bytes; the exact bound is checked on
/// the number itself, this only refuses absurd bodies before any allocation.
const BIGDECIMAL_MAX_BODY: usize = 61_500;
/// log2(10), for the digit-cap bit estimate.
const LOG2_10: f64 = std::f64::consts::LOG2_10;

pub(crate) fn fault(reason: EncodingFault) -> JsonbError {
    JsonbError::InvalidEncoding { reason }
}

/// The smallest of 1..=4 bytes that holds `v` (C4, C5).
pub(crate) fn min_width(v: usize) -> usize {
    match v {
        0..=0xFF => 1,
        0x100..=0xFFFF => 2,
        0x1_0000..=0xFF_FFFF => 3,
        _ => 4,
    }
}

/// A little-endian unsigned integer of `w` (1..=4) bytes at `at`.
pub(crate) fn read_le(bytes: &[u8], at: usize, w: usize) -> Result<usize, JsonbError> {
    let end = at
        .checked_add(w)
        .ok_or(fault(EncodingFault::OffsetOutOfBounds))?;
    let field = bytes.get(at..end).ok_or(fault(EncodingFault::Truncated))?;
    if w > 4 {
        return Err(fault(EncodingFault::OffsetOutOfBounds));
    }
    Ok(field
        .iter()
        .rev()
        .fold(0usize, |acc, b| (acc << 8) | usize::from(*b)))
}

fn first(bytes: &[u8]) -> Result<u8, JsonbError> {
    bytes
        .first()
        .copied()
        .ok_or(fault(EncodingFault::Truncated))
}

/// The fault for a primitive id that is not a jsonb scalar (C10).
pub(crate) fn primitive_fault(id: u8) -> JsonbError {
    match id {
        7 | 11..=15 | 17..=20 => fault(EncodingFault::ExcludedPrimitive(id)),
        _ => fault(EncodingFault::UnknownPrimitive(id)),
    }
}

/// The metadata section: a sorted key dictionary (C2, C3, C4).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Meta<'a> {
    bytes: &'a [u8],
    count: usize,
    w: usize,
    keys_at: usize,
}

impl<'a> Meta<'a> {
    /// Parse and validate the metadata at the start of `bytes`. The result
    /// covers exactly the metadata section; the value follows it.
    pub(crate) fn parse(bytes: &'a [u8]) -> Result<Meta<'a>, JsonbError> {
        let h = first(bytes)?;
        if h & 0x0F != 1 || h & 0x10 == 0 {
            return Err(fault(EncodingFault::BadMetadataHeader));
        }
        if h & 0x20 != 0 {
            return Err(fault(EncodingFault::NonCanonical("C2")));
        }
        let w = usize::from(h >> 6) + 1;
        let count = read_le(bytes, 1, w)?;
        let keys_at = count
            .checked_add(1)
            .and_then(|n| n.checked_mul(w))
            .and_then(|n| n.checked_add(1 + w))
            .ok_or(fault(EncodingFault::OffsetOutOfBounds))?;
        if keys_at > bytes.len() {
            return Err(fault(EncodingFault::Truncated));
        }
        let mut meta = Meta {
            bytes,
            count,
            w,
            keys_at,
        };
        let key_bytes = meta.check_tables()?;
        if w != min_width(count.max(key_bytes)) {
            return Err(fault(EncodingFault::NonCanonical("C4")));
        }
        meta.bytes = bytes
            .get(..keys_at + key_bytes)
            .ok_or(fault(EncodingFault::Truncated))?;
        Ok(meta)
    }

    /// Offsets monotone from zero, keys in bounds, valid UTF-8, strictly
    /// ascending. Returns the total key bytes. Loops `count` times, and `count`
    /// is already bounded by the bytes that hold its offset table.
    fn check_tables(&self) -> Result<usize, JsonbError> {
        if self.offset(0)? != 0 {
            return Err(fault(EncodingFault::OffsetsNotMonotonic));
        }
        let key_bytes = self.offset(self.count)?;
        let end = self
            .keys_at
            .checked_add(key_bytes)
            .ok_or(fault(EncodingFault::OffsetOutOfBounds))?;
        if end > self.bytes.len() {
            return Err(fault(EncodingFault::OffsetOutOfBounds));
        }
        let mut prev: Option<&[u8]> = None;
        for id in 0..self.count {
            let key = self.key_bytes(id)?;
            std::str::from_utf8(key).map_err(|_| fault(EncodingFault::InvalidUtf8))?;
            if prev.is_some_and(|p| p >= key) {
                return Err(fault(EncodingFault::DictionaryNotSorted));
            }
            prev = Some(key);
        }
        Ok(key_bytes)
    }

    /// Number of dictionary keys.
    pub(crate) fn count(&self) -> usize {
        self.count
    }

    /// Length of the metadata section in bytes.
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    fn offset(&self, i: usize) -> Result<usize, JsonbError> {
        let at = i
            .checked_mul(self.w)
            .and_then(|n| n.checked_add(1 + self.w))
            .ok_or(fault(EncodingFault::OffsetOutOfBounds))?;
        read_le(self.bytes, at, self.w)
    }

    fn key_bytes(&self, id: usize) -> Result<&'a [u8], JsonbError> {
        let next = id
            .checked_add(1)
            .ok_or(fault(EncodingFault::FieldIdOutOfRange))?;
        let (a, b) = (self.offset(id)?, self.offset(next)?);
        if a > b {
            return Err(fault(EncodingFault::OffsetsNotMonotonic));
        }
        let start = self.keys_at.saturating_add(a);
        let end = self.keys_at.saturating_add(b);
        self.bytes
            .get(start..end)
            .ok_or(fault(EncodingFault::OffsetOutOfBounds))
    }

    /// The key with dictionary id `id`.
    pub(crate) fn key(&self, id: usize) -> Result<&'a str, JsonbError> {
        if id >= self.count {
            return Err(fault(EncodingFault::FieldIdOutOfRange));
        }
        std::str::from_utf8(self.key_bytes(id)?).map_err(|_| fault(EncodingFault::InvalidUtf8))
    }
}

/// The header and tables of one object or array (C5, C6).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Head<'a> {
    span: &'a [u8],
    pub(crate) is_object: bool,
    pub(crate) count: usize,
    pub(crate) id_w: usize,
    pub(crate) off_w: usize,
    pub(crate) large: bool,
    ids_at: usize,
    offs_at: usize,
    data_at: usize,
}

impl<'a> Head<'a> {
    /// Parse the header of the container that spans exactly `span`.
    pub(crate) fn parse(span: &'a [u8]) -> Result<Head<'a>, JsonbError> {
        let h = first(span)?;
        let is_object = match h & 3 {
            2 => true,
            3 => false,
            _ => {
                return Err(JsonbError::WrongKind {
                    expected: "an object or array",
                })
            }
        };
        let off_w = usize::from((h >> 2) & 3) + 1;
        let (id_w, large, reserved) = if is_object {
            (usize::from((h >> 4) & 3) + 1, h & 0x40 != 0, h & 0x80 != 0)
        } else {
            (0, h & 0x10 != 0, h & 0xE0 != 0)
        };
        if reserved {
            return Err(fault(EncodingFault::NonCanonical("C5")));
        }
        let count_w = if large { 4 } else { 1 };
        let count = read_le(span, 1, count_w)?;
        let ids_at = 1 + count_w;
        let oob = fault(EncodingFault::OffsetOutOfBounds);
        let offs_at = count
            .checked_mul(id_w)
            .and_then(|n| n.checked_add(ids_at))
            .ok_or(oob.clone())?;
        let data_at = count
            .checked_add(1)
            .and_then(|n| n.checked_mul(off_w))
            .and_then(|n| n.checked_add(offs_at))
            .ok_or(oob)?;
        if data_at > span.len() {
            return Err(fault(EncodingFault::Truncated));
        }
        Ok(Head {
            span,
            is_object,
            count,
            id_w,
            off_w,
            large,
            ids_at,
            offs_at,
            data_at,
        })
    }

    /// Bytes of the child region.
    pub(crate) fn data_len(&self) -> usize {
        self.span.len().saturating_sub(self.data_at)
    }

    /// Field id of entry `i` (objects only).
    pub(crate) fn id(&self, i: usize) -> Result<usize, JsonbError> {
        if !self.is_object || i >= self.count {
            return Err(fault(EncodingFault::FieldIdOutOfRange));
        }
        let at = i
            .checked_mul(self.id_w)
            .and_then(|n| n.checked_add(self.ids_at))
            .ok_or(fault(EncodingFault::OffsetOutOfBounds))?;
        read_le(self.span, at, self.id_w)
    }

    /// Offset `i` (0..=count) into the child region.
    pub(crate) fn offset(&self, i: usize) -> Result<usize, JsonbError> {
        if i > self.count {
            return Err(fault(EncodingFault::OffsetOutOfBounds));
        }
        let at = i
            .checked_mul(self.off_w)
            .and_then(|n| n.checked_add(self.offs_at))
            .ok_or(fault(EncodingFault::OffsetOutOfBounds))?;
        read_le(self.span, at, self.off_w)
    }

    /// The bytes of child `i`, checked against the child region.
    pub(crate) fn child(&self, i: usize) -> Result<&'a [u8], JsonbError> {
        let (a, b) = (self.offset(i)?, self.offset(i + 1)?);
        if a > b {
            return Err(fault(EncodingFault::OffsetsNotMonotonic));
        }
        if b > self.data_len() {
            return Err(fault(EncodingFault::OffsetOutOfBounds));
        }
        let start = self.data_at + a;
        self.span
            .get(start..self.data_at + b)
            .ok_or(fault(EncodingFault::OffsetOutOfBounds))
    }
}

/// The kind of a jsonb value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Null,
    Bool,
    Number,
    String,
    Object,
    Array,
}

/// A validated cell: metadata plus one root value, borrowed from the bytes.
#[derive(Debug, Clone, Copy)]
pub struct JsonbRef<'a> {
    bytes: &'a [u8],
    meta: Meta<'a>,
    root: &'a [u8],
}

impl<'a> JsonbRef<'a> {
    /// Fully validate `bytes` against the fixed hard ceilings and the canonical
    /// rules C1-C11 (D4, D14b). It takes no `Limits`: lowering a tunable never
    /// hides stored data. This is the only way to obtain a reader.
    pub fn validate(bytes: &'a [u8]) -> Result<JsonbRef<'a>, JsonbError> {
        let (meta, root) = validate::validate_cell(bytes)?;
        Ok(JsonbRef { bytes, meta, root })
    }

    /// The whole cell, envelope included.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// The root value.
    pub fn root(&self) -> ValueRef<'a> {
        ValueRef {
            meta: self.meta,
            bytes: self.root,
        }
    }
}

/// One value inside a validated cell.
#[derive(Debug, Clone, Copy)]
pub struct ValueRef<'a> {
    meta: Meta<'a>,
    bytes: &'a [u8],
}

fn wrong(expected: &'static str) -> JsonbError {
    JsonbError::WrongKind { expected }
}

impl<'a> ValueRef<'a> {
    /// The kind of this value.
    pub fn kind(&self) -> Result<ValueKind, JsonbError> {
        let h = first(self.bytes)?;
        match (h & 3, h >> 2) {
            (0, 0) => Ok(ValueKind::Null),
            (0, 1 | 2) => Ok(ValueKind::Bool),
            (0, 3..=6 | 8..=10 | BIGDECIMAL_PRIMITIVE_ID) => Ok(ValueKind::Number),
            (0, 16) | (1, _) => Ok(ValueKind::String),
            (0, id) => Err(primitive_fault(id)),
            (2, _) => Ok(ValueKind::Object),
            _ => Ok(ValueKind::Array),
        }
    }

    /// The boolean value.
    pub fn as_bool(&self) -> Result<bool, JsonbError> {
        match (self.kind()?, first(self.bytes)? >> 2) {
            (ValueKind::Bool, 1) => Ok(true),
            (ValueKind::Bool, _) => Ok(false),
            _ => Err(wrong("a boolean")),
        }
    }

    /// The string value, borrowed from the cell.
    pub fn as_str(&self) -> Result<&'a str, JsonbError> {
        if self.kind()? != ValueKind::String {
            return Err(wrong("a string"));
        }
        str_of(self.bytes).map(|(s, _)| s)
    }

    /// The exact number, with its scale.
    pub fn as_number(&self) -> Result<Number, JsonbError> {
        if self.kind()? != ValueKind::Number {
            return Err(wrong("a number"));
        }
        decode_number(self.bytes)
    }

    /// The object view.
    pub fn as_object(&self) -> Result<ObjectRef<'a>, JsonbError> {
        if self.kind()? != ValueKind::Object {
            return Err(wrong("an object"));
        }
        let head = Head::parse(self.bytes)?;
        Ok(ObjectRef {
            meta: self.meta,
            head,
        })
    }

    /// The array view.
    pub fn as_array(&self) -> Result<ArrayRef<'a>, JsonbError> {
        if self.kind()? != ValueKind::Array {
            return Err(wrong("an array"));
        }
        let head = Head::parse(self.bytes)?;
        Ok(ArrayRef {
            meta: self.meta,
            head,
        })
    }
}

/// An object: entries in ascending key-byte order.
#[derive(Debug, Clone, Copy)]
pub struct ObjectRef<'a> {
    meta: Meta<'a>,
    head: Head<'a>,
}

impl<'a> ObjectRef<'a> {
    /// Number of entries.
    pub fn len(&self) -> usize {
        self.head.count
    }

    /// True when there are no entries.
    pub fn is_empty(&self) -> bool {
        self.head.count == 0
    }

    fn entry(&self, i: usize) -> Result<(&'a str, ValueRef<'a>), JsonbError> {
        let key = self.meta.key(self.head.id(i)?)?;
        let bytes = self.head.child(i)?;
        Ok((
            key,
            ValueRef {
                meta: self.meta,
                bytes,
            },
        ))
    }

    /// The value under `key`, by binary search over the sorted field ids.
    pub fn get(&self, key: &str) -> Result<Option<ValueRef<'a>>, JsonbError> {
        let (mut lo, mut hi) = (0usize, self.head.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (k, v) = self.entry(mid)?;
            match k.as_bytes().cmp(key.as_bytes()) {
                std::cmp::Ordering::Equal => return Ok(Some(v)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Ok(None)
    }

    /// Entries in key order.
    pub fn iter(&self) -> ObjectIter<'a> {
        ObjectIter {
            obj: *self,
            next: 0,
        }
    }
}

/// Iterator over an object's entries; a fault ends it after yielding the error.
#[derive(Debug, Clone)]
pub struct ObjectIter<'a> {
    obj: ObjectRef<'a>,
    next: usize,
}

impl<'a> Iterator for ObjectIter<'a> {
    type Item = Result<(&'a str, ValueRef<'a>), JsonbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.obj.head.count {
            return None;
        }
        let item = self.obj.entry(self.next);
        self.next = if item.is_err() {
            self.obj.head.count
        } else {
            self.next + 1
        };
        Some(item)
    }
}

/// An array.
#[derive(Debug, Clone, Copy)]
pub struct ArrayRef<'a> {
    meta: Meta<'a>,
    head: Head<'a>,
}

impl<'a> ArrayRef<'a> {
    /// Number of elements.
    pub fn len(&self) -> usize {
        self.head.count
    }

    /// True when there are no elements.
    pub fn is_empty(&self) -> bool {
        self.head.count == 0
    }

    /// Element `i`, or `None` past the end.
    pub fn get(&self, i: usize) -> Result<Option<ValueRef<'a>>, JsonbError> {
        if i >= self.head.count {
            return Ok(None);
        }
        let bytes = self.head.child(i)?;
        Ok(Some(ValueRef {
            meta: self.meta,
            bytes,
        }))
    }

    /// Elements in order.
    pub fn iter(&self) -> ArrayIter<'a> {
        ArrayIter {
            arr: *self,
            next: 0,
        }
    }
}

/// Iterator over an array's elements; a fault ends it after yielding the error.
#[derive(Debug, Clone)]
pub struct ArrayIter<'a> {
    arr: ArrayRef<'a>,
    next: usize,
}

impl<'a> Iterator for ArrayIter<'a> {
    type Item = Result<ValueRef<'a>, JsonbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.arr.head.count {
            return None;
        }
        let item = self.arr.get(self.next).transpose();
        self.next = if matches!(item, Some(Ok(_))) {
            self.next + 1
        } else {
            self.arr.head.count
        };
        item
    }
}

// ---- scalars ----------------------------------------------------------------

/// Require a scalar to span exactly `n` bytes.
pub(crate) fn exact(bytes: &[u8], n: usize) -> Result<(), JsonbError> {
    match bytes.len().cmp(&n) {
        std::cmp::Ordering::Less => Err(fault(EncodingFault::Truncated)),
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Greater => Err(fault(EncodingFault::TrailingBytes)),
    }
}

/// A string scalar (short or primitive 16) and whether it used the long form.
pub(crate) fn str_of(bytes: &[u8]) -> Result<(&str, bool), JsonbError> {
    let h = first(bytes)?;
    let (body_at, len, long) = match (h & 3, h >> 2) {
        (1, len) => (1usize, usize::from(len), false),
        (0, 16) => (5usize, read_le(bytes, 1, 4)?, true),
        _ => return Err(wrong("a string")),
    };
    let end = body_at
        .checked_add(len)
        .ok_or(fault(EncodingFault::OffsetOutOfBounds))?;
    if end > bytes.len() {
        return Err(fault(EncodingFault::OffsetOutOfBounds));
    }
    exact(bytes, end)?;
    let body = bytes
        .get(body_at..end)
        .ok_or(fault(EncodingFault::Truncated))?;
    let text = std::str::from_utf8(body).map_err(|_| fault(EncodingFault::InvalidUtf8))?;
    Ok((text, long))
}

/// Sign-extend a little-endian two's complement field to `i128`.
fn le_i128(field: &[u8]) -> i128 {
    let negative = field.last().is_some_and(|b| b & 0x80 != 0);
    let mut buf = if negative { [0xFFu8; 16] } else { [0u8; 16] };
    buf.iter_mut().zip(field).for_each(|(d, s)| *d = *s);
    i128::from_le_bytes(buf)
}

fn fixed_number(bytes: &[u8], id: u8) -> Result<Number, JsonbError> {
    let (width, scaled) = match id {
        3 => (1, false),
        4 => (2, false),
        5 => (4, false),
        6 => (8, false),
        8 => (4, true),
        9 => (8, true),
        _ => (16, true),
    };
    exact(bytes, 1 + usize::from(scaled) + width)?;
    let scale = if scaled {
        u16::from(
            bytes
                .get(1)
                .copied()
                .ok_or(fault(EncodingFault::Truncated))?,
        )
    } else {
        0
    };
    let body = bytes
        .get(1 + usize::from(scaled)..)
        .ok_or(fault(EncodingFault::Truncated))?;
    Ok(Number::from_i128(le_i128(body), scale))
}

/// Longest LEB128 encoding of a `u64`.
const MAX_UVARINT_BYTES: usize = 10;

/// A minimal LEB128 `u64`; returns the value and the bytes used. A varint with
/// no terminator within [`MAX_UVARINT_BYTES`] is malformed.
fn read_uvarint(b: &[u8]) -> Result<(u64, usize), JsonbError> {
    let mut v = 0u64;
    for (i, byte) in b.iter().enumerate() {
        if i >= MAX_UVARINT_BYTES {
            return Err(fault(EncodingFault::BadBigDecimal));
        }
        let low = u64::from(byte & 0x7F);
        if i == 9 && low > 1 {
            return Err(fault(EncodingFault::BadBigDecimal));
        }
        v |= low << (7 * i);
        if byte & 0x80 == 0 {
            if i > 0 && low == 0 {
                return Err(fault(EncodingFault::NonCanonical("C9")));
            }
            return Ok((v, i + 1));
        }
    }
    Err(fault(EncodingFault::BadBigDecimal))
}

/// `|u| < 10^digits`, using bit lengths and an exact compare only at the edge.
fn below_pow10(u: &BigInt, digits: usize) -> bool {
    let a = (digits as f64 * LOG2_10).floor() as u64;
    let bits = u.bits();
    if bits + 2 <= a {
        return true;
    }
    if bits >= a + 3 {
        return false;
    }
    let Ok(exp) = u32::try_from(digits) else {
        return false;
    };
    u.magnitude() < &BigUint::from(10u8).pow(exp)
}

fn bigdecimal(bytes: &[u8]) -> Result<Number, JsonbError> {
    let bad = || fault(EncodingFault::BadBigDecimal);
    let rest = bytes.get(1..).ok_or(bad())?;
    let (zz, used) = read_uvarint(rest)?;
    let scale = (zz >> 1) as i64 ^ -((zz & 1) as i64);
    let scale = u16::try_from(scale).map_err(|_| bad())?;
    let max_scale = crate::limits::HARD_MAX_DIGITS_AFTER_POINT;
    if usize::from(scale) > max_scale {
        return Err(JsonbError::DigitsAfterPointExceeded {
            digits: usize::from(scale),
            max: max_scale,
        });
    }
    let rest = rest.get(used..).ok_or(bad())?;
    let (len, used) = read_uvarint(rest)?;
    let body = rest.get(used..).ok_or(bad())?;
    let len = usize::try_from(len).map_err(|_| bad())?;
    if len == 0 || body.len() != len {
        return Err(bad());
    }
    minimal_twos_complement(body)?;
    if len > BIGDECIMAL_MAX_BODY {
        return Err(JsonbError::DigitsBeforePointExceeded {
            digits: len.saturating_mul(2),
            max: crate::limits::HARD_MAX_DIGITS_BEFORE_POINT,
        });
    }
    let unscaled = BigInt::from_signed_bytes_be(body);
    let cap = crate::limits::HARD_MAX_DIGITS_BEFORE_POINT + usize::from(scale);
    if !below_pow10(&unscaled, cap) {
        return Err(JsonbError::DigitsBeforePointExceeded {
            digits: cap + 1,
            max: crate::limits::HARD_MAX_DIGITS_BEFORE_POINT,
        });
    }
    Ok(Number::from_bigint(unscaled, scale))
}

/// A redundant leading `0x00` or `0xFF` byte makes the encoding non-minimal.
fn minimal_twos_complement(body: &[u8]) -> Result<(), JsonbError> {
    if let (Some(a), Some(b)) = (body.first(), body.get(1)) {
        if (*a == 0x00 && b & 0x80 == 0) || (*a == 0xFF && b & 0x80 != 0) {
            return Err(fault(EncodingFault::NonCanonical("C9")));
        }
    }
    Ok(())
}

/// Decode a number scalar (int8..int64, decimal4/8/16, bigdecimal) exactly.
pub(crate) fn decode_number(bytes: &[u8]) -> Result<Number, JsonbError> {
    let id = first(bytes)? >> 2;
    match id {
        3..=6 | 8..=10 => fixed_number(bytes, id),
        BIGDECIMAL_PRIMITIVE_ID => bigdecimal(bytes),
        _ => Err(wrong("a number")),
    }
}
