//! Module: array INPUT for `text[]`/`jsonb[]` columns (Oban's `tags`, `attempted_by`,
//! `errors`), on W1's array value (`ferrosa_sql::ArrayValue`; output is W1's).
//!
//! * An array column is stored as a **frozen** CQL list (`frozen<list<text>>`): one cell, so
//!   an empty array (`'{}'`) stays distinct from NULL — a non-frozen CQL list collapses an
//!   empty list to NULL.
//! * Text input is PostgreSQL's array text form, one dimension: `{a,"b c",NULL}`, with
//!   double-quoted elements and backslash escapes; an unquoted `NULL` is a NULL element. A
//!   malformed literal is `22P02`; a nested (multi-dimensional) array is `0A000`.
//! * Binary input is `array_recv`'s format (one dimension); each element is decoded by the
//!   element type's binary rule (text bytes; jsonb's `0x01` version byte then text).
//!
//! Last revised: 2026-11-08

use ferrosa_sql::{ArrayElem, ArrayValue, Value};

use crate::messages::BackendMessage;
use crate::query::error_response;

fn malformed(text: &str, why: &str) -> BackendMessage {
    error_response(
        "22P02",
        &format!("malformed array literal: \"{text}\" ({why})"),
    )
}

/// Parse a one-dimensional PostgreSQL array literal into its elements (`None` = NULL).
pub(crate) fn parse_text_array(text: &str) -> Result<Vec<Option<String>>, BackendMessage> {
    let body = text
        .trim()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| malformed(text, "it must be enclosed in braces"))?;
    let mut items = Vec::new();
    if body.trim().is_empty() {
        return Ok(items);
    }
    let mut chars = body.chars().peekable();
    // One element per iteration; bounded by the literal's length.
    for _ in 0..=body.len() {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let item = match chars.peek() {
            Some('"') => {
                chars.next();
                Some(quoted_element(&mut chars, text)?)
            }
            Some('{') => {
                return Err(error_response(
                    "0A000",
                    "multi-dimensional arrays are not supported",
                ))
            }
            _ => unquoted_element(&mut chars),
        };
        items.push(item);
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        match chars.next() {
            None => return Ok(items),
            Some(',') => {}
            Some(other) => return Err(malformed(text, &format!("unexpected '{other}'"))),
        }
    }
    Err(malformed(text, "too many elements"))
}

fn quoted_element(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    text: &str,
) -> Result<String, BackendMessage> {
    let mut out = String::new();
    loop {
        match chars.next() {
            Some('\\') => out.push(chars.next().ok_or_else(|| malformed(text, "trailing \\"))?),
            Some('"') => return Ok(out),
            Some(c) => out.push(c),
            None => return Err(malformed(text, "unterminated quoted element")),
        }
    }
}

fn unquoted_element(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<String> {
    let mut out = String::new();
    while let Some(&c) = chars.peek() {
        if c == ',' {
            break;
        }
        chars.next();
        if c == '\\' {
            if let Some(escaped) = chars.next() {
                out.push(escaped);
            }
            continue;
        }
        out.push(c);
    }
    let trimmed = out.trim_end();
    if trimmed.eq_ignore_ascii_case("NULL") {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn be_i32(bytes: &[u8], at: usize) -> Result<i32, BackendMessage> {
    bytes
        .get(at..at + 4)
        .and_then(|b| b.try_into().ok())
        .map(i32::from_be_bytes)
        .ok_or_else(|| error_response("08P01", "binary array parameter is truncated"))
}

/// Decode an `array_recv` binary array of text or jsonb elements.
pub(crate) fn decode_binary_array(bytes: &[u8]) -> Result<Value, BackendMessage> {
    let ndim = be_i32(bytes, 0)?;
    let elem_oid = be_i32(bytes, 8)?;
    let elem = match elem_oid {
        25 | 1043 => ArrayElem::Text,
        3802 => ArrayElem::Jsonb,
        other => {
            return Err(error_response(
                "0A000",
                &format!("binary arrays of element type OID {other} are not supported"),
            ))
        }
    };
    if ndim == 0 {
        return Ok(Value::Array(ArrayValue::new(elem, Vec::new())));
    }
    if ndim != 1 {
        return Err(error_response(
            "0A000",
            "multi-dimensional arrays are not supported",
        ));
    }
    let count = usize::try_from(be_i32(bytes, 12)?)
        .map_err(|_| error_response("08P01", "negative array length"))?;
    let mut at = 20; // ndim, flags, elem oid, length, lower bound
    let mut items = Vec::with_capacity(count.min(bytes.len()));
    for _ in 0..count {
        let len = be_i32(bytes, at)?;
        at += 4;
        if len < 0 {
            items.push(Value::Null);
            continue;
        }
        let len = len as usize;
        let raw = bytes
            .get(at..at + len)
            .ok_or_else(|| error_response("08P01", "binary array element is truncated"))?;
        at += len;
        let body = if elem == ArrayElem::Jsonb {
            raw.get(1..).unwrap_or(&[])
        } else {
            raw
        };
        let text = std::str::from_utf8(body)
            .map_err(|_| error_response("22021", "array element is not valid UTF-8"))?;
        items.push(Value::Text(text.to_string().into()));
    }
    Ok(Value::Array(ArrayValue::new(elem, items)))
}

/// The array element type a stored CQL list item implies (used when reading a list back).
pub(crate) fn elem_of(item: &ferrosa_common::CqlValue) -> ArrayElem {
    use ferrosa_common::CqlValue;
    match item {
        CqlValue::Int(_) | CqlValue::Smallint(_) | CqlValue::Tinyint(_) => ArrayElem::Int,
        CqlValue::Bigint(_) | CqlValue::Counter(_) => ArrayElem::BigInt,
        CqlValue::Boolean(_) => ArrayElem::Bool,
        CqlValue::Float(_) | CqlValue::Double(_) => ArrayElem::Float,
        CqlValue::Uuid(_) | CqlValue::Timeuuid(_) => ArrayElem::Uuid,
        CqlValue::Timestamp(_) => ArrayElem::Timestamp,
        CqlValue::Decimal { .. } | CqlValue::Varint(_) => ArrayElem::Numeric,
        CqlValue::Jsonb(_) => ArrayElem::Jsonb,
        _ => ArrayElem::Text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_arrays_parse_like_postgres() {
        assert_eq!(
            parse_text_array("{}").unwrap(),
            Vec::<Option<String>>::new()
        );
        assert_eq!(
            parse_text_array(r#"{a,"b c",NULL,"NULL","x\"y"}"#).unwrap(),
            vec![
                Some("a".into()),
                Some("b c".into()),
                None,
                Some("NULL".into()),
                Some("x\"y".into())
            ]
        );
        assert!(parse_text_array("{oops").is_err());
        assert!(parse_text_array("{{1},{2}}").is_err());
    }
}
