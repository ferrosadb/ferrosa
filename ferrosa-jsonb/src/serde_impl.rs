//! Module: serde for `JsonbValue` (T-106, FM-21, JB-T1).
//! Correctness: correct when human-readable formats carry padded standard
//! base64 of the canonical cell, other formats carry raw bytes, and
//! `Deserialize` VALIDATES the cell against the hard ceilings, so a spill record,
//! a peer message or a file never yields an unchecked value. Text longer than
//! the base64 of the largest cell is refused before it is decoded. Base64 is
//! strict: canonical padding, alphabet and unused bits.
//! Last revised: 2026-09-28
//! Last changed: T-106 initial serde.

use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::limits::HARD_MAX_ENCODED_BYTES;
use crate::value::JsonbValue;

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Longest base64 text a valid cell can have.
const MAX_TEXT: usize = HARD_MAX_ENCODED_BYTES.div_ceil(3) * 4;

fn sextet(acc: u32, shift: u32) -> char {
    let idx = usize::try_from((acc >> shift) & 63).unwrap_or(0);
    ALPHABET.get(idx).map_or('?', |b| char::from(*b))
}

fn encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let mut acc = 0u32;
        for i in 0..3 {
            acc = (acc << 8) | chunk.get(i).map_or(0, |b| u32::from(*b));
        }
        out.push(sextet(acc, 18));
        out.push(sextet(acc, 12));
        out.push(if chunk.len() > 1 { sextet(acc, 6) } else { '=' });
        out.push(if chunk.len() > 2 { sextet(acc, 0) } else { '=' });
    }
    out
}

fn value_of(c: u8) -> Option<u32> {
    let pos = ALPHABET.iter().position(|a| *a == c)?;
    u32::try_from(pos).ok()
}

/// Decode one 4-character group; `last` allows padding.
fn decode_group(group: &[u8], last: bool, out: &mut Vec<u8>) -> Result<(), &'static str> {
    let (mut acc, mut pad) = (0u32, 0usize);
    for c in group {
        acc <<= 6;
        if *c == b'=' {
            if !last {
                return Err("padding before the end");
            }
            pad += 1;
        } else if pad > 0 {
            return Err("data after padding");
        } else {
            acc |= value_of(*c).ok_or("character outside the base64 alphabet")?;
        }
    }
    let unused_mask = match pad {
        0 => 0,
        1 => 0xFF,
        2 => 0xFFFF,
        _ => return Err("too much padding"),
    };
    if acc & unused_mask != 0 {
        return Err("non-canonical base64 (nonzero unused bits)");
    }
    let bytes = [(acc >> 16) as u8, (acc >> 8) as u8, acc as u8];
    out.extend(bytes.iter().take(3 - pad));
    Ok(())
}

fn decode(text: &str) -> Result<Vec<u8>, &'static str> {
    let raw = text.as_bytes();
    if raw.len() > MAX_TEXT {
        return Err("text is longer than any valid jsonb cell");
    }
    if !raw.len().is_multiple_of(4) {
        return Err("base64 length is not a multiple of 4");
    }
    let groups = raw.len() / 4;
    let mut out = Vec::with_capacity(groups * 3);
    for (i, group) in raw.as_chunks::<4>().0.iter().enumerate() {
        decode_group(group, i + 1 == groups, &mut out)?;
    }
    Ok(out)
}

impl Serialize for JsonbValue {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.serialize_str(&encode(self.as_bytes()))
        } else {
            s.serialize_bytes(self.as_bytes())
        }
    }
}

struct CellVisitor;

impl CellVisitor {
    fn checked<E: de::Error>(bytes: Vec<u8>) -> Result<JsonbValue, E> {
        JsonbValue::from_bytes(bytes).map_err(E::custom)
    }
}

impl<'de> Visitor<'de> for CellVisitor {
    type Value = JsonbValue;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a canonical jsonb cell (base64 text or bytes)")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<JsonbValue, E> {
        Self::checked(decode(v).map_err(E::custom)?)
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<JsonbValue, E> {
        if v.len() > HARD_MAX_ENCODED_BYTES {
            return Err(E::custom("jsonb cell above the hard ceiling"));
        }
        Self::checked(v.to_vec())
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<JsonbValue, E> {
        if v.len() > HARD_MAX_ENCODED_BYTES {
            return Err(E::custom("jsonb cell above the hard ceiling"));
        }
        Self::checked(v)
    }
}

impl<'de> Deserialize<'de> for JsonbValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<JsonbValue, D::Error> {
        if d.is_human_readable() {
            d.deserialize_str(CellVisitor)
        } else {
            d.deserialize_bytes(CellVisitor)
        }
    }
}
