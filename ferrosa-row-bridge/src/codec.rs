//! CQL wire-format value codec and CQL type-name parser.
//!
//! Moved verbatim (behaviour-identical) from `ferrosa-cql` so both the CQL and
//! Postgres front-ends share one decode path. The only change from the original
//! is the error type: `CqlError::Invalid(msg)` -> [`RowBridgeError::invalid`].

use std::net::IpAddr;

use num_bigint::BigInt;

pub use ferrosa_common::{CqlType, CqlValue};
use ferrosa_schema::Schema;

use crate::RowBridgeError;

/// Encode a [`CqlValue`] as CQL wire-format bytes (no length prefix).
pub fn encode_value(value: &CqlValue) -> Vec<u8> {
    match value {
        CqlValue::Ascii(s) | CqlValue::Text(s) => s.as_bytes().to_vec(),
        CqlValue::Bigint(n) | CqlValue::Counter(n) | CqlValue::Timestamp(n) => {
            n.to_be_bytes().to_vec()
        }
        CqlValue::Blob(b) => b.clone(),
        CqlValue::Boolean(b) => vec![if *b { 1 } else { 0 }],
        CqlValue::Decimal { scale, unscaled } => {
            let mut buf = scale.to_be_bytes().to_vec();
            buf.extend_from_slice(&unscaled.to_signed_bytes_be());
            buf
        }
        CqlValue::Double(bits) => bits.to_be_bytes().to_vec(),
        CqlValue::Float(bits) => bits.to_be_bytes().to_vec(),
        CqlValue::Int(n) => n.to_be_bytes().to_vec(),
        CqlValue::Uuid(u) | CqlValue::Timeuuid(u) => u.as_bytes().to_vec(),
        CqlValue::Varint(n) => n.to_signed_bytes_be(),
        CqlValue::Inet(ip) => match ip {
            IpAddr::V4(v4) => v4.octets().to_vec(),
            IpAddr::V6(v6) => v6.octets().to_vec(),
        },
        CqlValue::Date(d) => d.to_be_bytes().to_vec(),
        CqlValue::Time(t) => t.to_be_bytes().to_vec(),
        CqlValue::Smallint(n) => n.to_be_bytes().to_vec(),
        CqlValue::Tinyint(n) => n.to_be_bytes().to_vec(),
        CqlValue::Duration {
            months,
            days,
            nanos,
        } => {
            let mut buf = Vec::new();
            buf.extend(encode_vint(*months as i64));
            buf.extend(encode_vint(*days as i64));
            buf.extend(encode_vint(*nanos));
            buf
        }
        CqlValue::List(items) | CqlValue::Set(items) => {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(items.len() as i32).to_be_bytes());
            for item in items {
                let encoded = encode_value(item);
                buf.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
                buf.extend_from_slice(&encoded);
            }
            buf
        }
        CqlValue::Map(entries) => {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(entries.len() as i32).to_be_bytes());
            for (k, v) in entries {
                let ek = encode_value(k);
                buf.extend_from_slice(&(ek.len() as i32).to_be_bytes());
                buf.extend_from_slice(&ek);
                let ev = encode_value(v);
                buf.extend_from_slice(&(ev.len() as i32).to_be_bytes());
                buf.extend_from_slice(&ev);
            }
            buf
        }
        CqlValue::Tuple(elements) => {
            let mut buf = Vec::new();
            for elem in elements {
                match elem {
                    Some(val) => {
                        let encoded = encode_value(val);
                        buf.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
                        buf.extend_from_slice(&encoded);
                    }
                    None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                }
            }
            buf
        }
        CqlValue::Vector(bits) => {
            let mut buf = Vec::with_capacity(bits.len() * 4);
            for b in bits {
                buf.extend_from_slice(&f32::from_bits(*b).to_be_bytes());
            }
            buf
        }
        CqlValue::Udt(fields) => {
            let mut buf = Vec::new();
            for (_name, value) in fields {
                match value {
                    Some(val) => {
                        let encoded = encode_value(val);
                        buf.extend_from_slice(&(encoded.len() as i32).to_be_bytes());
                        buf.extend_from_slice(&encoded);
                    }
                    None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                }
            }
            buf
        }
        CqlValue::Null => vec![],
    }
}

/// Decode a [`CqlValue`] from CQL wire-format bytes given its type.
pub fn decode_value(cql_type: &CqlType, bytes: &[u8]) -> Result<CqlValue, RowBridgeError> {
    match cql_type {
        CqlType::Ascii => Ok(CqlValue::Ascii(
            String::from_utf8(bytes.to_vec())
                .map_err(|e| RowBridgeError::invalid(format!("invalid ASCII: {e}")))?,
        )),
        CqlType::Varchar => Ok(CqlValue::Text(
            String::from_utf8(bytes.to_vec())
                .map_err(|e| RowBridgeError::invalid(format!("invalid UTF-8: {e}")))?,
        )),
        CqlType::Bigint => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| RowBridgeError::invalid("bigint requires 8 bytes"))?;
            Ok(CqlValue::Bigint(i64::from_be_bytes(arr)))
        }
        CqlType::Counter => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| RowBridgeError::invalid("counter requires 8 bytes"))?;
            Ok(CqlValue::Counter(i64::from_be_bytes(arr)))
        }
        CqlType::Blob => Ok(CqlValue::Blob(bytes.to_vec())),
        CqlType::Boolean => {
            if bytes.len() != 1 {
                return Err(RowBridgeError::invalid("boolean requires 1 byte"));
            }
            Ok(CqlValue::Boolean(bytes[0] != 0))
        }
        CqlType::Double => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| RowBridgeError::invalid("double requires 8 bytes"))?;
            Ok(CqlValue::Double(u64::from_be_bytes(arr)))
        }
        CqlType::Float => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| RowBridgeError::invalid("float requires 4 bytes"))?;
            Ok(CqlValue::Float(u32::from_be_bytes(arr)))
        }
        CqlType::Int => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| RowBridgeError::invalid("int requires 4 bytes"))?;
            Ok(CqlValue::Int(i32::from_be_bytes(arr)))
        }
        CqlType::Timestamp => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| RowBridgeError::invalid("timestamp requires 8 bytes"))?;
            Ok(CqlValue::Timestamp(i64::from_be_bytes(arr)))
        }
        CqlType::Uuid => {
            if bytes.len() != 16 {
                return Err(RowBridgeError::invalid("uuid requires 16 bytes"));
            }
            Ok(CqlValue::Uuid(uuid::Uuid::from_slice(bytes).map_err(
                |e| RowBridgeError::invalid(format!("invalid uuid: {e}")),
            )?))
        }
        CqlType::Timeuuid => {
            if bytes.len() != 16 {
                return Err(RowBridgeError::invalid("timeuuid requires 16 bytes"));
            }
            Ok(CqlValue::Timeuuid(uuid::Uuid::from_slice(bytes).map_err(
                |e| RowBridgeError::invalid(format!("invalid timeuuid: {e}")),
            )?))
        }
        CqlType::Inet => match bytes.len() {
            4 => {
                let arr: [u8; 4] = bytes.try_into().unwrap();
                Ok(CqlValue::Inet(IpAddr::V4(arr.into())))
            }
            16 => {
                let arr: [u8; 16] = bytes.try_into().unwrap();
                Ok(CqlValue::Inet(IpAddr::V6(arr.into())))
            }
            n => Err(RowBridgeError::invalid(format!(
                "inet requires 4 or 16 bytes, got {n}"
            ))),
        },
        CqlType::Date => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| RowBridgeError::invalid("date requires 4 bytes"))?;
            Ok(CqlValue::Date(u32::from_be_bytes(arr)))
        }
        CqlType::Time => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| RowBridgeError::invalid("time requires 8 bytes"))?;
            Ok(CqlValue::Time(i64::from_be_bytes(arr)))
        }
        CqlType::Smallint => {
            let arr: [u8; 2] = bytes
                .try_into()
                .map_err(|_| RowBridgeError::invalid("smallint requires 2 bytes"))?;
            Ok(CqlValue::Smallint(i16::from_be_bytes(arr)))
        }
        CqlType::Tinyint => {
            if bytes.len() != 1 {
                return Err(RowBridgeError::invalid("tinyint requires 1 byte"));
            }
            Ok(CqlValue::Tinyint(bytes[0] as i8))
        }
        CqlType::Duration => {
            let mut offset = 0;
            let months = decode_vint(bytes, &mut offset)? as i32;
            let days = decode_vint(bytes, &mut offset)? as i32;
            let nanos = decode_vint(bytes, &mut offset)?;
            Ok(CqlValue::Duration {
                months,
                days,
                nanos,
            })
        }
        CqlType::Varint => Ok(CqlValue::Varint(BigInt::from_signed_bytes_be(bytes))),
        CqlType::Decimal => {
            if bytes.len() < 4 {
                return Err(RowBridgeError::invalid("decimal requires at least 4 bytes"));
            }
            let scale = i32::from_be_bytes(bytes[..4].try_into().unwrap());
            let unscaled = BigInt::from_signed_bytes_be(&bytes[4..]);
            Ok(CqlValue::Decimal { scale, unscaled })
        }
        CqlType::List(elem_type) => {
            let (count, mut pos) = read_collection_header(bytes)?;
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let (val, next_pos) = read_collection_element(bytes, pos, elem_type)?;
                items.push(val);
                pos = next_pos;
            }
            Ok(CqlValue::List(items))
        }
        CqlType::Set(elem_type) => {
            let (count, mut pos) = read_collection_header(bytes)?;
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let (val, next_pos) = read_collection_element(bytes, pos, elem_type)?;
                items.push(val);
                pos = next_pos;
            }
            Ok(CqlValue::Set(items))
        }
        CqlType::Map(key_type, val_type) => {
            let (count, mut pos) = read_collection_header(bytes)?;
            let mut entries = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let (k, kpos) = read_collection_element(bytes, pos, key_type)?;
                let (v, vpos) = read_collection_element(bytes, kpos, val_type)?;
                entries.push((k, v));
                pos = vpos;
            }
            Ok(CqlValue::Map(entries))
        }
        CqlType::Tuple(elem_types) => {
            let mut elements = Vec::with_capacity(elem_types.len());
            let mut pos = 0;
            for et in elem_types {
                if pos + 4 > bytes.len() {
                    return Err(RowBridgeError::invalid("tuple truncated"));
                }
                let len = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap());
                pos += 4;
                if len < 0 {
                    elements.push(None);
                } else {
                    let end = pos + len as usize;
                    if end > bytes.len() {
                        return Err(RowBridgeError::invalid("tuple element truncated"));
                    }
                    elements.push(Some(decode_value(et, &bytes[pos..end])?));
                    pos = end;
                }
            }
            Ok(CqlValue::Tuple(elements))
        }
        CqlType::Vector(_, dim) => {
            let expected = *dim * 4;
            if bytes.len() != expected {
                return Err(RowBridgeError::invalid(format!(
                    "vector<float, {}> requires {} bytes, got {}",
                    dim,
                    expected,
                    bytes.len()
                )));
            }
            let mut bits = Vec::with_capacity(*dim);
            for i in 0..*dim {
                let offset = i * 4;
                let arr: [u8; 4] = bytes[offset..offset + 4].try_into().unwrap();
                bits.push(f32::from_be_bytes(arr).to_bits());
            }
            Ok(CqlValue::Vector(bits))
        }
        CqlType::Udt {
            fields: field_defs, ..
        } => {
            let mut result_fields = Vec::with_capacity(field_defs.len());
            let mut pos = 0;
            for (field_name, field_type) in field_defs {
                if pos >= bytes.len() {
                    // Cassandra allows shorter encodings — trailing fields are null
                    result_fields.push((field_name.clone(), None));
                    continue;
                }
                if pos + 4 > bytes.len() {
                    return Err(RowBridgeError::invalid("UDT field truncated at length"));
                }
                let len = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap());
                pos += 4;
                if len < 0 {
                    result_fields.push((field_name.clone(), None));
                } else {
                    let end = pos + len as usize;
                    if end > bytes.len() {
                        return Err(RowBridgeError::invalid("UDT field truncated at data"));
                    }
                    let val = decode_value(field_type, &bytes[pos..end])?;
                    result_fields.push((field_name.clone(), Some(val)));
                    pos = end;
                }
            }
            Ok(CqlValue::Udt(result_fields))
        }
    }
}

/// Zigzag-encode and write a signed integer as a variable-length byte sequence.
/// This is the encoding Cassandra uses for the CQL duration type (0x0015).
fn encode_vint(value: i64) -> Vec<u8> {
    let zigzag = if value >= 0 {
        (value as u64) << 1
    } else {
        ((-(value + 1)) as u64) << 1 | 1
    };
    let mut buf = Vec::new();
    let mut v = zigzag;
    loop {
        if v < 0x80 {
            buf.push(v as u8);
            break;
        }
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf
}

/// Decode a zigzag-encoded variable-length integer from `data` at `offset`.
fn decode_vint(data: &[u8], offset: &mut usize) -> Result<i64, RowBridgeError> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        if *offset >= data.len() {
            return Err(RowBridgeError::invalid("truncated vint in duration"));
        }
        let byte = data[*offset];
        *offset += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    // Zigzag decode
    Ok(((result >> 1) as i64) ^ (-((result & 1) as i64)))
}

/// Read the 4-byte element count from a collection header.
///
/// Validates that `count` does not exceed what the remaining buffer can
/// possibly hold. Each collection element needs at least a 4-byte length
/// prefix, so `count` cannot exceed `(remaining_bytes) / 4`. This prevents
/// a corrupt or malicious count from triggering a multi-gigabyte allocation.
fn read_collection_header(bytes: &[u8]) -> Result<(i32, usize), RowBridgeError> {
    if bytes.len() < 4 {
        return Err(RowBridgeError::invalid("collection too short for count"));
    }
    let count = i32::from_be_bytes(bytes[..4].try_into().unwrap());
    if count < 0 {
        return Err(RowBridgeError::invalid("negative collection count"));
    }
    let remaining = bytes.len() - 4;
    if count as usize > remaining / 4 {
        tracing::warn!(
            count,
            remaining,
            first_bytes = ?&bytes[..std::cmp::min(16, bytes.len())],
            "collection decode: count exceeds buffer — possible data corruption"
        );
        return Err(RowBridgeError::invalid(format!(
            "collection count {} exceeds buffer capacity ({} bytes remaining)",
            count, remaining
        )));
    }
    Ok((count, 4))
}

/// Read one length-prefixed element from a collection at `pos`.
fn read_collection_element(
    bytes: &[u8],
    pos: usize,
    elem_type: &CqlType,
) -> Result<(CqlValue, usize), RowBridgeError> {
    if pos + 4 > bytes.len() {
        return Err(RowBridgeError::invalid(
            "collection truncated at element length",
        ));
    }
    let len = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap());
    if len < 0 {
        return Ok((CqlValue::Null, pos + 4));
    }
    let len = len as usize;
    let end = pos + 4 + len;
    if end > bytes.len() {
        return Err(RowBridgeError::invalid(
            "collection truncated at element data",
        ));
    }
    let val = decode_value(elem_type, &bytes[pos + 4..end])?;
    Ok((val, end))
}

// ---------------------------------------------------------------------------
// CQL type-name parser
// ---------------------------------------------------------------------------

/// Parse a CQL type name string (e.g. `"int"`, `"list<text>"`, `"frozen<map<text, int>>"`)
/// into a [`CqlType`].
///
/// `frozen<...>` is treated as transparent — the inner type is returned directly,
/// since `CqlType` does not represent frozen as a distinct concept.
pub fn parse_cql_type(s: &str) -> Result<CqlType, RowBridgeError> {
    let s = s.trim();
    let mut parser = TypeParser::new(s);
    let result = parser.parse_type()?;
    let remaining = parser.remaining().trim();
    if !remaining.is_empty() {
        return Err(RowBridgeError::invalid(format!(
            "unexpected trailing characters in type: '{remaining}'"
        )));
    }
    Ok(result)
}

/// Parse a CQL type name string with schema context for UDT resolution.
///
/// Like [`parse_cql_type`] but also resolves user-defined type names by looking
/// them up in the schema registry for the given keyspace.
pub fn parse_cql_type_in_keyspace(
    s: &str,
    keyspace: &str,
    schema: &Schema,
) -> Result<CqlType, RowBridgeError> {
    let s = s.trim();
    let mut parser = TypeParser::new_with_schema(s, keyspace, schema);
    let result = parser.parse_type()?;
    let remaining = parser.remaining().trim();
    if !remaining.is_empty() {
        return Err(RowBridgeError::invalid(format!(
            "unexpected trailing characters in type: '{remaining}'"
        )));
    }
    Ok(result)
}

/// Small recursive-descent parser for CQL type strings.
struct TypeParser<'a> {
    input: &'a str,
    pos: usize,
    /// Optional schema context for resolving UDT names.
    schema_ctx: Option<(&'a str, &'a Schema)>,
}

impl<'a> TypeParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            pos: 0,
            schema_ctx: None,
        }
    }

    fn new_with_schema(input: &'a str, keyspace: &'a str, schema: &'a Schema) -> Self {
        Self {
            input,
            pos: 0,
            schema_ctx: Some((keyspace, schema)),
        }
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input.as_bytes()[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.as_bytes().get(self.pos).copied()
    }

    fn consume(&mut self, ch: u8) -> Result<(), RowBridgeError> {
        self.skip_whitespace();
        match self.peek() {
            Some(c) if c == ch => {
                self.pos += 1;
                Ok(())
            }
            _ => Err(RowBridgeError::invalid(format!(
                "expected '{}' in type at position {}",
                ch as char, self.pos
            ))),
        }
    }

    /// Read an identifier (letters, digits, underscore).
    fn read_ident(&mut self) -> Result<&'a str, RowBridgeError> {
        self.skip_whitespace();
        let start = self.pos;
        while self.pos < self.input.len() {
            let b = self.input.as_bytes()[self.pos];
            if b.is_ascii_alphanumeric() || b == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(RowBridgeError::invalid(format!(
                "expected type name at position {}",
                self.pos
            )));
        }
        Ok(&self.input[start..self.pos])
    }

    fn parse_type(&mut self) -> Result<CqlType, RowBridgeError> {
        let ident = self.read_ident()?;
        let lower = ident.to_ascii_lowercase();

        match lower.as_str() {
            "text" | "varchar" => Ok(CqlType::Varchar),
            "int" => Ok(CqlType::Int),
            "bigint" => Ok(CqlType::Bigint),
            "smallint" => Ok(CqlType::Smallint),
            "tinyint" => Ok(CqlType::Tinyint),
            "float" => Ok(CqlType::Float),
            "double" => Ok(CqlType::Double),
            "boolean" => Ok(CqlType::Boolean),
            "blob" => Ok(CqlType::Blob),
            "uuid" => Ok(CqlType::Uuid),
            "timeuuid" => Ok(CqlType::Timeuuid),
            "timestamp" => Ok(CqlType::Timestamp),
            "inet" => Ok(CqlType::Inet),
            "ascii" => Ok(CqlType::Ascii),
            "counter" => Ok(CqlType::Counter),
            "varint" => Ok(CqlType::Varint),
            "decimal" => Ok(CqlType::Decimal),
            "date" => Ok(CqlType::Date),
            "time" => Ok(CqlType::Time),
            "duration" => Ok(CqlType::Duration),
            "list" => {
                self.skip_whitespace();
                self.consume(b'<')?;
                let elem = self.parse_type()?;
                self.skip_whitespace();
                self.consume(b'>')?;
                Ok(CqlType::List(Box::new(elem)))
            }
            "set" => {
                self.skip_whitespace();
                self.consume(b'<')?;
                let elem = self.parse_type()?;
                self.skip_whitespace();
                self.consume(b'>')?;
                Ok(CqlType::Set(Box::new(elem)))
            }
            "map" => {
                self.skip_whitespace();
                self.consume(b'<')?;
                let key = self.parse_type()?;
                self.skip_whitespace();
                self.consume(b',')?;
                let val = self.parse_type()?;
                self.skip_whitespace();
                self.consume(b'>')?;
                Ok(CqlType::Map(Box::new(key), Box::new(val)))
            }
            "tuple" => {
                self.skip_whitespace();
                self.consume(b'<')?;
                let mut types = vec![self.parse_type()?];
                loop {
                    self.skip_whitespace();
                    if self.peek() == Some(b',') {
                        self.pos += 1;
                        types.push(self.parse_type()?);
                    } else {
                        break;
                    }
                }
                self.consume(b'>')?;
                Ok(CqlType::Tuple(types))
            }
            "vector" => {
                self.skip_whitespace();
                self.consume(b'<')?;
                let elem = self.parse_type()?;
                self.skip_whitespace();
                self.consume(b',')?;
                self.skip_whitespace();
                let dim_str = self.read_ident()?;
                let dim: usize = dim_str.parse().map_err(|_| {
                    RowBridgeError::invalid(format!(
                        "expected integer dimension for vector, got '{dim_str}'"
                    ))
                })?;
                self.skip_whitespace();
                self.consume(b'>')?;
                Ok(CqlType::Vector(Box::new(elem), dim))
            }
            "frozen" => {
                self.skip_whitespace();
                self.consume(b'<')?;
                let inner = self.parse_type()?;
                self.skip_whitespace();
                self.consume(b'>')?;
                Ok(inner)
            }
            _ => {
                // Try UDT lookup if schema context is available.
                // Copy context refs to avoid borrow conflict with `self.read_ident()`.
                let ctx = self.schema_ctx;
                if let Some((keyspace, schema)) = ctx {
                    // Check for fully-qualified name (ks.typename) by reading a dot
                    if self.peek() == Some(b'.') {
                        self.pos += 1;
                        let type_name = self.read_ident()?;
                        let type_name_lower = type_name.to_ascii_lowercase();
                        if let Some(udt) = schema.get_type(&lower, &type_name_lower) {
                            return Ok(CqlType::Udt {
                                keyspace: udt.keyspace,
                                name: udt.name,
                                fields: udt.fields,
                            });
                        }
                        return Err(RowBridgeError::invalid(format!(
                            "unknown CQL type: '{lower}.{type_name_lower}'"
                        )));
                    }
                    // Try unqualified name in current keyspace
                    if let Some(udt) = schema.get_type(keyspace, &lower) {
                        return Ok(CqlType::Udt {
                            keyspace: udt.keyspace,
                            name: udt.name,
                            fields: udt.fields,
                        });
                    }
                }
                Err(RowBridgeError::invalid(format!(
                    "unknown CQL type: '{lower}'"
                )))
            }
        }
    }
}
