//! Bridge between CQL protocol types and storage types.
//!
//! Stateless pure functions for converting between parser-level `Term`,
//! wire-level `CqlValue`, and storage-level `CellValue`/`DecoratedKey`/`Row`.
//!
//! All narrowing conversions are range-checked (security mitigation M5).
//! No `unwrap()` on user data (M4) — the only exception is system clock
//! in `build_delete_row`.

use std::net::IpAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use crate::ast::{CqlTypeName, Term};
use crate::error::CqlError;
use crate::types::{decode_value, encode_value, CqlType, CqlValue};
use ferrosa_schema::Schema;

// ---------------------------------------------------------------------------
// Function 1: term_to_cql_value
// ---------------------------------------------------------------------------

/// Convert a parser-level [`Term`] to a wire-level [`CqlValue`], coercing
/// according to the target column's [`CqlType`].
///
/// Narrowing integer conversions are range-checked (M5). Bind markers are
/// rejected in non-prepared queries.
pub fn term_to_cql_value(term: &Term, target: &CqlType) -> Result<CqlValue, CqlError> {
    match term {
        Term::Null => Ok(CqlValue::Null),

        Term::IntegerLiteral(n) => match target {
            CqlType::Int => {
                let v = i32::try_from(*n)
                    .map_err(|_| CqlError::Invalid("value out of range for int".into()))?;
                Ok(CqlValue::Int(v))
            }
            CqlType::Bigint => Ok(CqlValue::Bigint(*n)),
            CqlType::Smallint => {
                let v = i16::try_from(*n)
                    .map_err(|_| CqlError::Invalid("value out of range for smallint".into()))?;
                Ok(CqlValue::Smallint(v))
            }
            CqlType::Tinyint => {
                let v = i8::try_from(*n)
                    .map_err(|_| CqlError::Invalid("value out of range for tinyint".into()))?;
                Ok(CqlValue::Tinyint(v))
            }
            CqlType::Timestamp => Ok(CqlValue::Timestamp(*n)),
            CqlType::Counter => Ok(CqlValue::Counter(*n)),
            _ => Err(CqlError::Invalid(format!(
                "type mismatch: expected {}, got integer literal",
                cql_type_name(target)
            ))),
        },

        Term::FloatLiteral(f) => match target {
            CqlType::Float => {
                let f32_val = *f as f32;
                if f.is_finite() && !f32_val.is_finite() {
                    return Err(CqlError::Invalid("value out of range for float".into()));
                }
                Ok(CqlValue::Float(f32_val.to_bits()))
            }
            CqlType::Double => Ok(CqlValue::Double(f.to_bits())),
            _ => Err(CqlError::Invalid(format!(
                "type mismatch: expected {}, got float literal",
                cql_type_name(target)
            ))),
        },

        Term::StringLiteral(s) => match target {
            CqlType::Varchar => Ok(CqlValue::Text(s.clone())),
            CqlType::Ascii => Ok(CqlValue::Ascii(s.clone())),
            CqlType::Inet => {
                let addr: IpAddr = s
                    .parse()
                    .map_err(|e| CqlError::Invalid(format!("invalid inet address: {e}")))?;
                Ok(CqlValue::Inet(addr))
            }
            CqlType::Timestamp => {
                // Parse ISO 8601 timestamp: "2024-01-15T10:30:00Z" or "2024-01-15 10:30:00+0000"
                // Try common formats
                let ms = if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                    dt.timestamp_millis()
                } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                {
                    dt.and_utc().timestamp_millis()
                } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                {
                    dt.and_utc().timestamp_millis()
                } else if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis()
                } else {
                    return Err(CqlError::Invalid(format!(
                        "cannot parse timestamp from string: {s}"
                    )));
                };
                Ok(CqlValue::Timestamp(ms))
            }
            CqlType::Date => {
                // Parse ISO date: "2024-01-15"
                // CQL date is days since epoch (1970-01-01), stored as u32 with 2^31 offset
                let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|e| CqlError::Invalid(format!("cannot parse date: {e}")))?;
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = (d - epoch).num_days();
                // CQL date encoding: days since epoch + 2^31 (to make it unsigned/sortable)
                let encoded = (days + (1i64 << 31)) as u32;
                Ok(CqlValue::Date(encoded))
            }
            CqlType::Time => {
                // Parse time: "10:30:00" or "10:30:00.123456789"
                // CQL time is nanoseconds since midnight
                let t = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                    .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
                    .map_err(|e| CqlError::Invalid(format!("cannot parse time: {e}")))?;
                let nanos = t
                    .signed_duration_since(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                    .num_nanoseconds()
                    .unwrap_or(0);
                Ok(CqlValue::Time(nanos))
            }
            _ => Err(CqlError::Invalid(format!(
                "type mismatch: expected {}, got string literal",
                cql_type_name(target)
            ))),
        },

        Term::BoolLiteral(b) => match target {
            CqlType::Boolean => Ok(CqlValue::Boolean(*b)),
            _ => Err(CqlError::Invalid(format!(
                "type mismatch: expected {}, got boolean literal",
                cql_type_name(target)
            ))),
        },

        Term::UuidLiteral(u) => match target {
            CqlType::Uuid => Ok(CqlValue::Uuid(*u)),
            CqlType::Timeuuid => Ok(CqlValue::Timeuuid(*u)),
            _ => Err(CqlError::Invalid(format!(
                "type mismatch: expected {}, got uuid literal",
                cql_type_name(target)
            ))),
        },

        Term::BlobLiteral(b) => match target {
            CqlType::Blob => Ok(CqlValue::Blob(b.clone())),
            _ => Err(CqlError::Invalid(format!(
                "type mismatch: expected {}, got blob literal",
                cql_type_name(target)
            ))),
        },

        Term::ListLiteral(items) => match target {
            CqlType::List(elem_type) => {
                let converted: Result<Vec<CqlValue>, _> = items
                    .iter()
                    .map(|item| term_to_cql_value(item, elem_type))
                    .collect();
                Ok(CqlValue::List(converted?))
            }
            _ => Err(CqlError::Invalid(format!(
                "type mismatch: expected {}, got list literal",
                cql_type_name(target)
            ))),
        },

        Term::SetLiteral(items) => match target {
            CqlType::Set(elem_type) => {
                let converted: Result<Vec<CqlValue>, _> = items
                    .iter()
                    .map(|item| term_to_cql_value(item, elem_type))
                    .collect();
                Ok(CqlValue::Set(converted?))
            }
            _ => Err(CqlError::Invalid(format!(
                "type mismatch: expected {}, got set literal",
                cql_type_name(target)
            ))),
        },

        Term::MapLiteral(pairs) => match target {
            CqlType::Map(key_type, val_type) => {
                let mut converted = Vec::with_capacity(pairs.len());
                for (k, v) in pairs {
                    let ck = term_to_cql_value(k, key_type)?;
                    let cv = term_to_cql_value(v, val_type)?;
                    converted.push((ck, cv));
                }
                Ok(CqlValue::Map(converted))
            }
            CqlType::Udt {
                fields: field_defs, ..
            } => {
                // UDT literal: {field_name: value, ...}
                // Keys in the map literal must be string literals matching field names.
                let mut result = Vec::with_capacity(field_defs.len());
                for (field_name, field_type) in field_defs {
                    let value = pairs
                        .iter()
                        .find(|(k, _)| match k {
                            Term::StringLiteral(s) => s.eq_ignore_ascii_case(field_name),
                            _ => false,
                        })
                        .map(|(_, v)| term_to_cql_value(v, field_type))
                        .transpose()?;
                    result.push((field_name.clone(), value));
                }
                Ok(CqlValue::Udt(result))
            }
            _ => Err(CqlError::Invalid(format!(
                "type mismatch: expected {}, got map literal",
                cql_type_name(target)
            ))),
        },

        Term::TupleLiteral(items) => match target {
            CqlType::Tuple(types) => {
                let mut elements = Vec::with_capacity(types.len());
                for (i, ty) in types.iter().enumerate() {
                    if i < items.len() {
                        elements.push(Some(term_to_cql_value(&items[i], ty)?));
                    } else {
                        elements.push(None);
                    }
                }
                Ok(CqlValue::Tuple(elements))
            }
            _ => Err(CqlError::Invalid(format!(
                "type mismatch: expected {}, got tuple literal",
                cql_type_name(target)
            ))),
        },

        Term::BindMarker(_) => Err(CqlError::Invalid(
            "bind markers not supported in non-prepared queries".into(),
        )),

        Term::InList(_) => Err(CqlError::Invalid(
            "IN list not supported in this context".into(),
        )),

        Term::FunctionCall { name, args, .. } => {
            match name.to_lowercase().as_str() {
                "uuid" if args.is_empty() => Ok(CqlValue::Uuid(uuid::Uuid::new_v4())),
                "now" if args.is_empty() => {
                    // timeuuid-like: use current timestamp
                    Ok(CqlValue::Timestamp(chrono::Utc::now().timestamp_millis()))
                }
                "totimestamp" if args.len() == 1 => {
                    // Convert argument to timestamp
                    let inner = term_to_cql_value(&args[0], &CqlType::Timestamp)?;
                    Ok(inner)
                }
                "todate" if args.len() == 1 => {
                    let inner = term_to_cql_value(&args[0], &CqlType::Date)?;
                    Ok(inner)
                }
                _ => Err(CqlError::Invalid(format!("unknown function: {name}"))),
            }
        }
    }
}

/// Human-readable name for a CqlType (for error messages).
fn cql_type_name(t: &CqlType) -> &'static str {
    match t {
        CqlType::Ascii => "ascii",
        CqlType::Bigint => "bigint",
        CqlType::Blob => "blob",
        CqlType::Boolean => "boolean",
        CqlType::Counter => "counter",
        CqlType::Decimal => "decimal",
        CqlType::Double => "double",
        CqlType::Float => "float",
        CqlType::Int => "int",
        CqlType::Timestamp => "timestamp",
        CqlType::Uuid => "uuid",
        CqlType::Varchar => "text",
        CqlType::Varint => "varint",
        CqlType::Timeuuid => "timeuuid",
        CqlType::Inet => "inet",
        CqlType::Date => "date",
        CqlType::Time => "time",
        CqlType::Smallint => "smallint",
        CqlType::Tinyint => "tinyint",
        CqlType::Duration => "duration",
        CqlType::List(_) => "list",
        CqlType::Map(_, _) => "map",
        CqlType::Set(_) => "set",
        CqlType::Tuple(_) => "tuple",
        CqlType::Udt { .. } => "udt",
    }
}

/// CQL type display name suitable for `system_schema.types` `field_types`.
///
/// Produces lowercase CQL type names (e.g. `"text"`, `"int"`, `"list<text>"`,
/// `"map<text, int>"`, `"ks.typename"`).
pub fn cql_type_display_name(t: &CqlType) -> String {
    match t {
        CqlType::Ascii => "ascii".to_string(),
        CqlType::Bigint => "bigint".to_string(),
        CqlType::Blob => "blob".to_string(),
        CqlType::Boolean => "boolean".to_string(),
        CqlType::Counter => "counter".to_string(),
        CqlType::Decimal => "decimal".to_string(),
        CqlType::Double => "double".to_string(),
        CqlType::Float => "float".to_string(),
        CqlType::Int => "int".to_string(),
        CqlType::Timestamp => "timestamp".to_string(),
        CqlType::Uuid => "uuid".to_string(),
        CqlType::Varchar => "text".to_string(),
        CqlType::Varint => "varint".to_string(),
        CqlType::Timeuuid => "timeuuid".to_string(),
        CqlType::Inet => "inet".to_string(),
        CqlType::Date => "date".to_string(),
        CqlType::Time => "time".to_string(),
        CqlType::Smallint => "smallint".to_string(),
        CqlType::Tinyint => "tinyint".to_string(),
        CqlType::Duration => "duration".to_string(),
        CqlType::List(inner) => format!("list<{}>", cql_type_display_name(inner)),
        CqlType::Set(inner) => format!("set<{}>", cql_type_display_name(inner)),
        CqlType::Map(k, v) => {
            format!(
                "map<{}, {}>",
                cql_type_display_name(k),
                cql_type_display_name(v)
            )
        }
        CqlType::Tuple(types) => {
            let inner: Vec<String> = types.iter().map(cql_type_display_name).collect();
            format!("tuple<{}>", inner.join(", "))
        }
        CqlType::Udt { keyspace, name, .. } => format!("{keyspace}.{name}"),
    }
}

// ---------------------------------------------------------------------------
// Function 2: parse_cql_type
// ---------------------------------------------------------------------------

/// Parse a CQL type name string (e.g. `"int"`, `"list<text>"`, `"frozen<map<text, int>>"`)
/// into a [`CqlType`].
///
/// `frozen<...>` is treated as transparent — the inner type is returned directly,
/// since `CqlType` does not represent frozen as a distinct concept.
pub fn parse_cql_type(s: &str) -> Result<CqlType, CqlError> {
    let s = s.trim();
    let mut parser = TypeParser::new(s);
    let result = parser.parse_type()?;
    let remaining = parser.remaining().trim();
    if !remaining.is_empty() {
        return Err(CqlError::Invalid(format!(
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
) -> Result<CqlType, CqlError> {
    let s = s.trim();
    let mut parser = TypeParser::new_with_schema(s, keyspace, schema);
    let result = parser.parse_type()?;
    let remaining = parser.remaining().trim();
    if !remaining.is_empty() {
        return Err(CqlError::Invalid(format!(
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

    fn consume(&mut self, ch: u8) -> Result<(), CqlError> {
        self.skip_whitespace();
        match self.peek() {
            Some(c) if c == ch => {
                self.pos += 1;
                Ok(())
            }
            _ => Err(CqlError::Invalid(format!(
                "expected '{}' in type at position {}",
                ch as char, self.pos
            ))),
        }
    }

    /// Read an identifier (letters, digits, underscore).
    fn read_ident(&mut self) -> Result<&'a str, CqlError> {
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
            return Err(CqlError::Invalid(format!(
                "expected type name at position {}",
                self.pos
            )));
        }
        Ok(&self.input[start..self.pos])
    }

    fn parse_type(&mut self) -> Result<CqlType, CqlError> {
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
                        return Err(CqlError::Invalid(format!(
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
                Err(CqlError::Invalid(format!("unknown CQL type: '{lower}'")))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Function 2b: resolve_type_name
// ---------------------------------------------------------------------------

/// Resolve a [`CqlTypeName`] (from the parser AST) to a [`CqlType`], looking up
/// user-defined types in the schema registry.
///
/// Built-in types (text, int, etc.) are resolved directly. If a `Simple` name
/// doesn't match any built-in, the schema is checked for a UDT in the current
/// keyspace (or a fully-qualified `ks.typename`).
///
/// `frozen<...>` is treated as transparent — the inner type is returned directly.
pub fn resolve_type_name(
    type_name: &CqlTypeName,
    keyspace: &str,
    schema: &Schema,
) -> Result<CqlType, CqlError> {
    match type_name {
        CqlTypeName::Simple(name) => {
            // Try built-in types first via the existing TypeParser
            let lower = name.to_ascii_lowercase();
            if let Some(builtin) = resolve_builtin_type(&lower) {
                return Ok(builtin);
            }
            // Try UDT lookup — could be "other_ks.typename"
            if let Some(dot_pos) = name.find('.') {
                let ks = &name[..dot_pos];
                let tn = &name[dot_pos + 1..];
                if let Some(udt) = schema.get_type(ks, tn) {
                    return Ok(CqlType::Udt {
                        keyspace: udt.keyspace,
                        name: udt.name,
                        fields: udt.fields,
                    });
                }
            } else if let Some(udt) = schema.get_type(keyspace, name) {
                return Ok(CqlType::Udt {
                    keyspace: udt.keyspace,
                    name: udt.name,
                    fields: udt.fields,
                });
            }
            Err(CqlError::Invalid(format!("unknown type: '{name}'")))
        }
        CqlTypeName::Frozen(inner) => {
            // frozen is a storage hint, not a separate type
            resolve_type_name(inner, keyspace, schema)
        }
        CqlTypeName::List(inner) => Ok(CqlType::List(Box::new(resolve_type_name(
            inner, keyspace, schema,
        )?))),
        CqlTypeName::Set(inner) => Ok(CqlType::Set(Box::new(resolve_type_name(
            inner, keyspace, schema,
        )?))),
        CqlTypeName::Map(k, v) => Ok(CqlType::Map(
            Box::new(resolve_type_name(k, keyspace, schema)?),
            Box::new(resolve_type_name(v, keyspace, schema)?),
        )),
        CqlTypeName::Tuple(elems) => {
            let resolved: Result<Vec<_>, _> = elems
                .iter()
                .map(|e| resolve_type_name(e, keyspace, schema))
                .collect();
            Ok(CqlType::Tuple(resolved?))
        }
    }
}

/// Try to resolve a lowercase type name to a built-in CQL type.
fn resolve_builtin_type(name: &str) -> Option<CqlType> {
    match name {
        "text" | "varchar" => Some(CqlType::Varchar),
        "int" => Some(CqlType::Int),
        "bigint" => Some(CqlType::Bigint),
        "smallint" => Some(CqlType::Smallint),
        "tinyint" => Some(CqlType::Tinyint),
        "float" => Some(CqlType::Float),
        "double" => Some(CqlType::Double),
        "boolean" => Some(CqlType::Boolean),
        "blob" => Some(CqlType::Blob),
        "uuid" => Some(CqlType::Uuid),
        "timeuuid" => Some(CqlType::Timeuuid),
        "timestamp" => Some(CqlType::Timestamp),
        "inet" => Some(CqlType::Inet),
        "ascii" => Some(CqlType::Ascii),
        "counter" => Some(CqlType::Counter),
        "varint" => Some(CqlType::Varint),
        "decimal" => Some(CqlType::Decimal),
        "date" => Some(CqlType::Date),
        "time" => Some(CqlType::Time),
        "duration" => Some(CqlType::Duration),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Function 3: build_decorated_key
// ---------------------------------------------------------------------------

/// Build a [`DecoratedKey`] from partition key column values.
///
/// - Single-column PK: raw `encode_value()` bytes.
/// - Composite PK: `[2-byte len][value bytes][0x00]` per component.
pub fn build_decorated_key(
    pk_values: &[CqlValue],
    _pk_types: &[CqlType],
) -> Result<DecoratedKey, CqlError> {
    if pk_values.is_empty() {
        return Err(CqlError::Invalid(
            "partition key must have at least one column".into(),
        ));
    }

    let bytes = if pk_values.len() == 1 {
        encode_value(&pk_values[0])
    } else {
        // Composite key: [2-byte len][value bytes][0x00] per component
        let mut buf = Vec::new();
        for val in pk_values {
            let encoded = encode_value(val);
            let len = u16::try_from(encoded.len())
                .map_err(|_| CqlError::Invalid("partition key component too large".into()))?;
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(&encoded);
            buf.push(0x00);
        }
        buf
    };

    Ok(DecoratedKey::new(PartitionKey::new(bytes)))
}

// ---------------------------------------------------------------------------
// Function 4: build_row
// ---------------------------------------------------------------------------

/// Build a storage [`Row`] from column values.
///
/// - `column_values`: `(column_index, value)` pairs for non-key columns.
/// - `clustering_values`: CQL values for clustering columns.
/// - `timestamp`: write timestamp (microseconds).
/// - `ttl`: optional TTL in seconds.
pub fn build_row(
    column_values: &[(u16, CqlValue)],
    clustering_values: &[CqlValue],
    timestamp: i64,
    ttl: Option<i32>,
) -> Row {
    let clustering = encode_clustering(clustering_values);

    let cells: Vec<(u16, CellValue)> = column_values
        .iter()
        .map(|(idx, val)| {
            let encoded = encode_value(val);
            let cell = match ttl {
                Some(ttl_secs) => {
                    // System clock: the one allowed unwrap
                    let now_secs = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let local_deletion_time =
                        i32::try_from(now_secs.saturating_add(ttl_secs as u64)).unwrap_or(i32::MAX);
                    CellValue::expiring(encoded, timestamp, ttl_secs, local_deletion_time)
                }
                None => CellValue::live(encoded, timestamp),
            };
            (*idx, cell)
        })
        .collect();

    let primary_key_liveness = match ttl {
        Some(ttl_secs) => {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let local_deletion_time =
                i32::try_from(now_secs.saturating_add(ttl_secs as u64)).unwrap_or(i32::MAX);
            LivenessInfo::with_ttl(timestamp, ttl_secs, local_deletion_time)
        }
        None => LivenessInfo::with_timestamp(timestamp),
    };

    Row {
        clustering,
        cells,
        deletion: DeletionTime::LIVE,
        primary_key_liveness,
    }
}

// ---------------------------------------------------------------------------
// Function 5: build_delete_row
// ---------------------------------------------------------------------------

/// Build a storage [`Row`] representing a deletion.
///
/// - `delete_columns`: column indices to delete. If empty, this is a row-level
///   deletion (range tombstone).
/// - `clustering_values`: CQL values for clustering columns.
/// - `timestamp`: deletion timestamp (microseconds).
pub fn build_delete_row(
    delete_columns: &[u16],
    clustering_values: &[CqlValue],
    timestamp: i64,
) -> Row {
    let clustering = encode_clustering(clustering_values);

    // System clock: the one allowed unwrap
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;

    if delete_columns.is_empty() {
        // Row-level deletion
        Row {
            clustering,
            cells: vec![],
            deletion: DeletionTime::new(timestamp, now_secs),
            primary_key_liveness: LivenessInfo::NONE,
        }
    } else {
        // Column-level deletion: tombstone each specified column
        let cells: Vec<(u16, CellValue)> = delete_columns
            .iter()
            .map(|&idx| {
                // CellValue.local_deletion_time is i32, DeletionTime.local_deletion_time is u32
                let ldt = i32::try_from(now_secs).unwrap_or(i32::MAX);
                (idx, CellValue::tombstone(timestamp, ldt))
            })
            .collect();

        Row {
            clustering,
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        }
    }
}

// ---------------------------------------------------------------------------
// Function 6: partition_to_rows
// ---------------------------------------------------------------------------

/// Convert a storage `Partition` back to result rows for CQL RESULT encoding.
///
/// Each returned row is a `Vec<Option<CqlValue>>` with one entry per column
/// in `column_names`. Tombstone rows and cells are represented as `None`.
pub fn partition_to_rows(
    partition: &ferrosa_sstable::types::Partition,
    column_names: &[String],
    column_types: &[CqlType],
    pk_columns: &[usize],
    ck_columns: &[usize],
) -> Vec<Vec<Option<CqlValue>>> {
    let mut result = Vec::new();

    // Pre-decode PK values from the partition key
    let pk_values = decode_pk(&partition.key, pk_columns.len());

    for row in &partition.rows {
        // Skip tombstone rows
        if !row.deletion.is_live() {
            continue;
        }

        let mut output_row: Vec<Option<CqlValue>> = vec![None; column_names.len()];

        // Fill PK columns
        for (i, &col_idx) in pk_columns.iter().enumerate() {
            if col_idx < column_types.len() {
                if let Some(bytes) = pk_values.get(i) {
                    if let Ok(val) = decode_value(&column_types[col_idx], bytes) {
                        output_row[col_idx] = Some(val);
                    }
                }
            }
        }

        // Fill CK columns
        let ck_values = decode_clustering(&row.clustering, ck_columns.len());
        for (i, &col_idx) in ck_columns.iter().enumerate() {
            if col_idx < column_types.len() {
                if let Some(bytes) = ck_values.get(i) {
                    if let Ok(val) = decode_value(&column_types[col_idx], bytes) {
                        output_row[col_idx] = Some(val);
                    }
                }
            }
        }

        // Fill regular columns from cells
        for (col_index, cell) in &row.cells {
            let idx = *col_index as usize;
            if idx < column_types.len() {
                if cell.is_tombstone() {
                    output_row[idx] = None;
                } else if let Some(ref value_bytes) = cell.value {
                    if let Ok(val) = decode_value(&column_types[idx], value_bytes) {
                        output_row[idx] = Some(val);
                    }
                }
            }
        }

        result.push(output_row);
    }

    result
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Encode clustering key values to bytes.
///
/// Single clustering column: raw `encode_value()` bytes.
/// Multiple: `[2-byte len][value bytes]` per component (no trailing 0x00).
fn encode_clustering(values: &[CqlValue]) -> Vec<u8> {
    if values.is_empty() {
        return vec![];
    }
    if values.len() == 1 {
        return encode_value(&values[0]);
    }
    // Multi-column clustering: length-prefixed concatenation
    let mut buf = Vec::new();
    for val in values {
        let encoded = encode_value(val);
        // Use saturating cast; clustering values should never be > 64K
        let len = (encoded.len() as u16).to_be_bytes();
        buf.extend_from_slice(&len);
        buf.extend_from_slice(&encoded);
    }
    buf
}

/// Decode partition key bytes into component byte slices.
///
/// Single PK: the whole byte slice is the single component.
/// Composite PK: `[2-byte len][value bytes][0x00]` per component.
fn decode_pk(dk: &DecoratedKey, num_components: usize) -> Vec<Vec<u8>> {
    let bytes = dk.key.as_bytes();
    if num_components <= 1 {
        return vec![bytes.to_vec()];
    }
    // Composite: [2-byte len][value bytes][0x00] per component
    let mut components = Vec::with_capacity(num_components);
    let mut pos = 0;
    while pos + 2 <= bytes.len() && components.len() < num_components {
        let len = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;
        let end = pos + len;
        if end > bytes.len() {
            break;
        }
        components.push(bytes[pos..end].to_vec());
        pos = end;
        // Skip the 0x00 separator
        if pos < bytes.len() && bytes[pos] == 0x00 {
            pos += 1;
        }
    }
    components
}

/// Decode clustering key bytes into component byte slices.
///
/// Single CK: the whole byte slice is the single component.
/// Multiple: `[2-byte len][value bytes]` per component.
fn decode_clustering(bytes: &[u8], num_components: usize) -> Vec<Vec<u8>> {
    if bytes.is_empty() || num_components == 0 {
        return vec![];
    }
    if num_components == 1 {
        return vec![bytes.to_vec()];
    }
    let mut components = Vec::with_capacity(num_components);
    let mut pos = 0;
    while pos + 2 <= bytes.len() && components.len() < num_components {
        let len = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;
        let end = pos + len;
        if end > bytes.len() {
            break;
        }
        components.push(bytes[pos..end].to_vec());
        pos = end;
    }
    components
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_int_to_cql_int() {
        let val = term_to_cql_value(&Term::IntegerLiteral(42), &CqlType::Int).unwrap();
        assert_eq!(val, CqlValue::Int(42));
    }

    #[test]
    fn term_int_overflow_rejected() {
        let result = term_to_cql_value(&Term::IntegerLiteral(i64::MAX), &CqlType::Int);
        assert!(result.is_err());
    }

    #[test]
    fn term_int_to_cql_bigint() {
        let val = term_to_cql_value(&Term::IntegerLiteral(42), &CqlType::Bigint).unwrap();
        assert_eq!(val, CqlValue::Bigint(42));
    }

    #[test]
    fn term_string_to_text() {
        let val =
            term_to_cql_value(&Term::StringLiteral("hello".into()), &CqlType::Varchar).unwrap();
        assert_eq!(val, CqlValue::Text("hello".into()));
    }

    #[test]
    fn term_type_mismatch() {
        let result = term_to_cql_value(&Term::StringLiteral("hello".into()), &CqlType::Int);
        assert!(result.is_err());
    }

    #[test]
    fn term_null_any_type() {
        assert_eq!(
            term_to_cql_value(&Term::Null, &CqlType::Int).unwrap(),
            CqlValue::Null
        );
    }

    #[test]
    fn term_list_literal() {
        let term = Term::ListLiteral(vec![Term::IntegerLiteral(1), Term::IntegerLiteral(2)]);
        let val = term_to_cql_value(&term, &CqlType::List(Box::new(CqlType::Int))).unwrap();
        assert_eq!(
            val,
            CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)])
        );
    }

    #[test]
    fn term_smallint_overflow() {
        let result = term_to_cql_value(&Term::IntegerLiteral(40000), &CqlType::Smallint);
        assert!(result.is_err());
    }

    #[test]
    fn term_tinyint_overflow() {
        let result = term_to_cql_value(&Term::IntegerLiteral(200), &CqlType::Tinyint);
        assert!(result.is_err());
    }

    #[test]
    fn term_float_overflow() {
        // f64 value that is finite but out of f32 range
        let result = term_to_cql_value(&Term::FloatLiteral(f64::MAX), &CqlType::Float);
        assert!(result.is_err());
    }

    #[test]
    fn term_float_ok() {
        let val = term_to_cql_value(&Term::FloatLiteral(3.125), &CqlType::Float).unwrap();
        if let CqlValue::Float(bits) = val {
            let f = f32::from_bits(bits);
            assert!((f - 3.125f32).abs() < 1e-6);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn term_double_ok() {
        let val = term_to_cql_value(&Term::FloatLiteral(3.125), &CqlType::Double).unwrap();
        assert_eq!(val, CqlValue::Double(3.125f64.to_bits()));
    }

    #[test]
    fn term_bool_ok() {
        let val = term_to_cql_value(&Term::BoolLiteral(true), &CqlType::Boolean).unwrap();
        assert_eq!(val, CqlValue::Boolean(true));
    }

    #[test]
    fn term_uuid_ok() {
        let u = uuid::Uuid::new_v4();
        let val = term_to_cql_value(&Term::UuidLiteral(u), &CqlType::Uuid).unwrap();
        assert_eq!(val, CqlValue::Uuid(u));
    }

    #[test]
    fn term_timeuuid_ok() {
        let u = uuid::Uuid::new_v4();
        let val = term_to_cql_value(&Term::UuidLiteral(u), &CqlType::Timeuuid).unwrap();
        assert_eq!(val, CqlValue::Timeuuid(u));
    }

    #[test]
    fn term_blob_ok() {
        let val = term_to_cql_value(&Term::BlobLiteral(vec![0xDE, 0xAD]), &CqlType::Blob).unwrap();
        assert_eq!(val, CqlValue::Blob(vec![0xDE, 0xAD]));
    }

    #[test]
    fn term_inet_ok() {
        let val =
            term_to_cql_value(&Term::StringLiteral("127.0.0.1".into()), &CqlType::Inet).unwrap();
        assert_eq!(val, CqlValue::Inet("127.0.0.1".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn term_inet_invalid() {
        let result = term_to_cql_value(&Term::StringLiteral("not-an-ip".into()), &CqlType::Inet);
        assert!(result.is_err());
    }

    #[test]
    fn term_set_literal() {
        let term = Term::SetLiteral(vec![Term::IntegerLiteral(1), Term::IntegerLiteral(2)]);
        let val = term_to_cql_value(&term, &CqlType::Set(Box::new(CqlType::Int))).unwrap();
        assert_eq!(val, CqlValue::Set(vec![CqlValue::Int(1), CqlValue::Int(2)]));
    }

    #[test]
    fn term_map_literal() {
        let term = Term::MapLiteral(vec![(
            Term::StringLiteral("key".into()),
            Term::IntegerLiteral(42),
        )]);
        let val = term_to_cql_value(
            &term,
            &CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int)),
        )
        .unwrap();
        assert_eq!(
            val,
            CqlValue::Map(vec![(CqlValue::Text("key".into()), CqlValue::Int(42))])
        );
    }

    #[test]
    fn term_tuple_literal() {
        let term = Term::TupleLiteral(vec![Term::IntegerLiteral(1)]);
        let val = term_to_cql_value(&term, &CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]))
            .unwrap();
        // Second element should be None since the tuple literal only has one item
        assert_eq!(val, CqlValue::Tuple(vec![Some(CqlValue::Int(1)), None]));
    }

    #[test]
    fn term_bind_marker_rejected() {
        let result = term_to_cql_value(&Term::BindMarker(None), &CqlType::Int);
        assert!(result.is_err());
    }

    #[test]
    fn term_int_to_timestamp() {
        let val =
            term_to_cql_value(&Term::IntegerLiteral(1710000000), &CqlType::Timestamp).unwrap();
        assert_eq!(val, CqlValue::Timestamp(1710000000));
    }

    #[test]
    fn term_int_to_counter() {
        let val = term_to_cql_value(&Term::IntegerLiteral(99), &CqlType::Counter).unwrap();
        assert_eq!(val, CqlValue::Counter(99));
    }

    #[test]
    fn term_string_to_ascii() {
        let val = term_to_cql_value(&Term::StringLiteral("hello".into()), &CqlType::Ascii).unwrap();
        assert_eq!(val, CqlValue::Ascii("hello".into()));
    }

    // --- parse_cql_type tests ---

    #[test]
    fn parse_simple_type() {
        assert_eq!(parse_cql_type("text").unwrap(), CqlType::Varchar);
        assert_eq!(parse_cql_type("int").unwrap(), CqlType::Int);
    }

    #[test]
    fn parse_all_simple_types() {
        assert_eq!(parse_cql_type("varchar").unwrap(), CqlType::Varchar);
        assert_eq!(parse_cql_type("bigint").unwrap(), CqlType::Bigint);
        assert_eq!(parse_cql_type("smallint").unwrap(), CqlType::Smallint);
        assert_eq!(parse_cql_type("tinyint").unwrap(), CqlType::Tinyint);
        assert_eq!(parse_cql_type("float").unwrap(), CqlType::Float);
        assert_eq!(parse_cql_type("double").unwrap(), CqlType::Double);
        assert_eq!(parse_cql_type("boolean").unwrap(), CqlType::Boolean);
        assert_eq!(parse_cql_type("blob").unwrap(), CqlType::Blob);
        assert_eq!(parse_cql_type("uuid").unwrap(), CqlType::Uuid);
        assert_eq!(parse_cql_type("timeuuid").unwrap(), CqlType::Timeuuid);
        assert_eq!(parse_cql_type("timestamp").unwrap(), CqlType::Timestamp);
        assert_eq!(parse_cql_type("inet").unwrap(), CqlType::Inet);
        assert_eq!(parse_cql_type("ascii").unwrap(), CqlType::Ascii);
        assert_eq!(parse_cql_type("counter").unwrap(), CqlType::Counter);
        assert_eq!(parse_cql_type("varint").unwrap(), CqlType::Varint);
        assert_eq!(parse_cql_type("decimal").unwrap(), CqlType::Decimal);
        assert_eq!(parse_cql_type("date").unwrap(), CqlType::Date);
        assert_eq!(parse_cql_type("time").unwrap(), CqlType::Time);
    }

    #[test]
    fn parse_collection_type() {
        assert_eq!(
            parse_cql_type("list<int>").unwrap(),
            CqlType::List(Box::new(CqlType::Int))
        );
    }

    #[test]
    fn parse_set_type() {
        assert_eq!(
            parse_cql_type("set<text>").unwrap(),
            CqlType::Set(Box::new(CqlType::Varchar))
        );
    }

    #[test]
    fn parse_map_type() {
        assert_eq!(
            parse_cql_type("map<text, int>").unwrap(),
            CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int))
        );
    }

    #[test]
    fn parse_nested_type() {
        assert_eq!(
            parse_cql_type("map<text, list<int>>").unwrap(),
            CqlType::Map(
                Box::new(CqlType::Varchar),
                Box::new(CqlType::List(Box::new(CqlType::Int)))
            )
        );
    }

    #[test]
    fn parse_tuple_type() {
        assert_eq!(
            parse_cql_type("tuple<int, text>").unwrap(),
            CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar])
        );
    }

    #[test]
    fn parse_frozen_type() {
        assert_eq!(
            parse_cql_type("frozen<map<text, int>>").unwrap(),
            CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int))
        );
    }

    #[test]
    fn parse_frozen_list() {
        assert_eq!(
            parse_cql_type("frozen<list<int>>").unwrap(),
            CqlType::List(Box::new(CqlType::Int))
        );
    }

    #[test]
    fn parse_unknown_type_rejected() {
        assert!(parse_cql_type("nosuchtype").is_err());
    }

    #[test]
    fn parse_type_with_whitespace() {
        assert_eq!(
            parse_cql_type("  map < text , int > ").unwrap(),
            CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int))
        );
    }

    // --- build_decorated_key tests ---

    #[test]
    fn build_decorated_key_single() {
        let key = build_decorated_key(&[CqlValue::Int(42)], &[CqlType::Int]).unwrap();
        assert_eq!(key.key.as_bytes(), &42i32.to_be_bytes());
    }

    #[test]
    fn build_decorated_key_composite() {
        let key = build_decorated_key(
            &[CqlValue::Text("hello".into()), CqlValue::Int(1)],
            &[CqlType::Varchar, CqlType::Int],
        )
        .unwrap();
        let bytes = key.key.as_bytes();
        // First component: [2-byte len=5][hello][0x00]
        assert_eq!(&bytes[0..2], &5u16.to_be_bytes());
        assert_eq!(&bytes[2..7], b"hello");
        assert_eq!(bytes[7], 0x00);
    }

    #[test]
    fn build_decorated_key_composite_second_component() {
        let key = build_decorated_key(
            &[CqlValue::Text("hello".into()), CqlValue::Int(1)],
            &[CqlType::Varchar, CqlType::Int],
        )
        .unwrap();
        let bytes = key.key.as_bytes();
        // Second component starts after "hello" (5 bytes) + 2-byte len + 0x00 = offset 8
        // [2-byte len=4][00 00 00 01][0x00]
        assert_eq!(&bytes[8..10], &4u16.to_be_bytes());
        assert_eq!(&bytes[10..14], &1i32.to_be_bytes());
        assert_eq!(bytes[14], 0x00);
    }

    #[test]
    fn build_decorated_key_empty_rejected() {
        let result = build_decorated_key(&[], &[]);
        assert!(result.is_err());
    }

    // --- build_row tests ---

    #[test]
    fn build_row_simple() {
        let row = build_row(
            &[(0, CqlValue::Text("alice".into()))],
            &[], // no clustering
            1000,
            None,
        );
        assert_eq!(row.cells.len(), 1);
        assert!(row.cells[0].1.is_live());
        assert!(row.clustering.is_empty());
    }

    #[test]
    fn build_row_with_clustering() {
        let row = build_row(
            &[(0, CqlValue::Int(42))],
            &[CqlValue::Text("ck1".into())],
            2000,
            None,
        );
        assert_eq!(row.clustering, encode_value(&CqlValue::Text("ck1".into())));
        assert!(row.primary_key_liveness.has_timestamp());
        assert!(!row.primary_key_liveness.has_ttl());
    }

    #[test]
    fn build_row_with_ttl() {
        let row = build_row(&[(0, CqlValue::Int(42))], &[], 3000, Some(3600));
        assert!(row.cells[0].1.is_expiring());
        assert!(row.primary_key_liveness.has_ttl());
        assert_eq!(row.primary_key_liveness.ttl, 3600);
    }

    #[test]
    fn build_row_multi_clustering() {
        let row = build_row(
            &[(0, CqlValue::Int(1))],
            &[CqlValue::Text("a".into()), CqlValue::Int(42)],
            1000,
            None,
        );
        // Multi-column clustering: length-prefixed concatenation
        let expected = {
            let mut buf = Vec::new();
            let a_bytes = encode_value(&CqlValue::Text("a".into()));
            buf.extend_from_slice(&(a_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(&a_bytes);
            let i_bytes = encode_value(&CqlValue::Int(42));
            buf.extend_from_slice(&(i_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(&i_bytes);
            buf
        };
        assert_eq!(row.clustering, expected);
    }

    // --- build_delete_row tests ---

    #[test]
    fn build_delete_row_row_level() {
        let row = build_delete_row(&[], &[CqlValue::Int(1)], 5000);
        assert!(!row.deletion.is_live());
        assert_eq!(row.deletion.marked_for_delete_at, 5000);
        assert!(row.cells.is_empty());
    }

    #[test]
    fn build_delete_row_column_level() {
        let row = build_delete_row(&[0, 1], &[CqlValue::Int(1)], 6000);
        assert!(row.deletion.is_live());
        assert_eq!(row.cells.len(), 2);
        assert!(row.cells[0].1.is_tombstone());
        assert!(row.cells[1].1.is_tombstone());
    }

    // --- partition_to_rows tests ---

    #[test]
    fn partition_to_rows_basic() {
        use ferrosa_sstable::types::Partition;

        let pk_bytes = encode_value(&CqlValue::Int(42));
        let dk = DecoratedKey::new(PartitionKey::new(pk_bytes));

        let cell_bytes = encode_value(&CqlValue::Text("alice".into()));
        let row = Row {
            clustering: vec![],
            cells: vec![(1, CellValue::live(cell_bytes, 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };

        let partition = Partition {
            key: dk,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row],
        };

        let column_names = vec!["id".into(), "name".into()];
        let column_types = vec![CqlType::Int, CqlType::Varchar];
        let pk_columns = vec![0usize];
        let ck_columns: Vec<usize> = vec![];

        let rows = partition_to_rows(
            &partition,
            &column_names,
            &column_types,
            &pk_columns,
            &ck_columns,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some(CqlValue::Int(42)));
        assert_eq!(rows[0][1], Some(CqlValue::Text("alice".into())));
    }

    #[test]
    fn partition_to_rows_skips_tombstone() {
        use ferrosa_sstable::types::Partition;

        let pk_bytes = encode_value(&CqlValue::Int(1));
        let dk = DecoratedKey::new(PartitionKey::new(pk_bytes));

        let tombstone_row = Row {
            clustering: vec![],
            cells: vec![],
            deletion: DeletionTime::new(1000, 1700000000),
            primary_key_liveness: LivenessInfo::NONE,
        };

        let partition = Partition {
            key: dk,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![tombstone_row],
        };

        let rows = partition_to_rows(&partition, &["id".into()], &[CqlType::Int], &[0], &[]);
        assert!(rows.is_empty());
    }

    #[test]
    fn partition_to_rows_tombstone_cell() {
        use ferrosa_sstable::types::Partition;

        let pk_bytes = encode_value(&CqlValue::Int(1));
        let dk = DecoratedKey::new(PartitionKey::new(pk_bytes));

        let row = Row {
            clustering: vec![],
            cells: vec![(1, CellValue::tombstone(1000, 1700000000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };

        let partition = Partition {
            key: dk,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row],
        };

        let rows = partition_to_rows(
            &partition,
            &["id".into(), "name".into()],
            &[CqlType::Int, CqlType::Varchar],
            &[0],
            &[],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some(CqlValue::Int(1)));
        // Tombstone cell -> None
        assert_eq!(rows[0][1], None);
    }

    // --- resolve_type_name tests ---

    fn test_schema_with_keyspace(ks: &str) -> Schema {
        use ferrosa_schema::{
            AuthMethod, DeploymentMode, EnvSecretsProvider, KeyspaceMetadata, PasswordHasher,
            PasswordPolicy, RateLimitConfig, ReplicationParams, SchemaConfig, TestAuditSink,
        };
        use std::collections::HashMap;

        let schema = Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap();
        schema
            .create_keyspace_internal(KeyspaceMetadata {
                name: ks.to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
                },
            })
            .unwrap();
        schema
    }

    #[test]
    fn resolve_builtin_type_still_works() {
        let schema = test_schema_with_keyspace("ks");
        let resolved =
            resolve_type_name(&CqlTypeName::Simple("text".to_string()), "ks", &schema).unwrap();
        assert_eq!(resolved, CqlType::Varchar);
    }

    #[test]
    fn resolve_builtin_int() {
        let schema = test_schema_with_keyspace("ks");
        let resolved =
            resolve_type_name(&CqlTypeName::Simple("int".to_string()), "ks", &schema).unwrap();
        assert_eq!(resolved, CqlType::Int);
    }

    #[test]
    fn resolve_udt_type_name() {
        use ferrosa_schema::metadata::UserTypeMetadata;

        let schema = test_schema_with_keyspace("ks");
        schema
            .create_type_internal(&UserTypeMetadata {
                keyspace: "ks".to_string(),
                name: "address".to_string(),
                fields: vec![
                    ("street".to_string(), CqlType::Varchar),
                    ("zip".to_string(), CqlType::Int),
                ],
            })
            .unwrap();

        let resolved =
            resolve_type_name(&CqlTypeName::Simple("address".to_string()), "ks", &schema).unwrap();
        match resolved {
            CqlType::Udt {
                keyspace,
                name,
                fields,
            } => {
                assert_eq!(keyspace, "ks");
                assert_eq!(name, "address");
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "street");
                assert_eq!(fields[1].0, "zip");
            }
            other => panic!("expected Udt, got {:?}", other),
        }
    }

    #[test]
    fn resolve_frozen_udt() {
        use ferrosa_schema::metadata::UserTypeMetadata;

        let schema = test_schema_with_keyspace("ks");
        schema
            .create_type_internal(&UserTypeMetadata {
                keyspace: "ks".to_string(),
                name: "address".to_string(),
                fields: vec![("street".to_string(), CqlType::Varchar)],
            })
            .unwrap();

        // frozen<address> should resolve to CqlType::Udt
        let resolved = resolve_type_name(
            &CqlTypeName::Frozen(Box::new(CqlTypeName::Simple("address".to_string()))),
            "ks",
            &schema,
        )
        .unwrap();
        match resolved {
            CqlType::Udt { name, .. } => assert_eq!(name, "address"),
            other => panic!("expected Udt, got {:?}", other),
        }
    }

    #[test]
    fn resolve_qualified_udt() {
        use ferrosa_schema::metadata::UserTypeMetadata;
        use ferrosa_schema::{KeyspaceMetadata, ReplicationParams};
        use std::collections::HashMap;

        let schema = test_schema_with_keyspace("ks1");
        schema
            .create_keyspace_internal(KeyspaceMetadata {
                name: "ks2".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
                },
            })
            .unwrap();
        schema
            .create_type_internal(&UserTypeMetadata {
                keyspace: "ks2".to_string(),
                name: "phone".to_string(),
                fields: vec![("number".to_string(), CqlType::Varchar)],
            })
            .unwrap();

        // From ks1 context, resolve "ks2.phone"
        let resolved = resolve_type_name(
            &CqlTypeName::Simple("ks2.phone".to_string()),
            "ks1",
            &schema,
        )
        .unwrap();
        match resolved {
            CqlType::Udt { keyspace, name, .. } => {
                assert_eq!(keyspace, "ks2");
                assert_eq!(name, "phone");
            }
            other => panic!("expected Udt, got {:?}", other),
        }
    }

    #[test]
    fn resolve_unknown_type_rejected() {
        let schema = test_schema_with_keyspace("ks");
        let result = resolve_type_name(
            &CqlTypeName::Simple("nosuchtype".to_string()),
            "ks",
            &schema,
        );
        assert!(result.is_err());
    }

    #[test]
    fn resolve_list_of_udt() {
        use ferrosa_schema::metadata::UserTypeMetadata;

        let schema = test_schema_with_keyspace("ks");
        schema
            .create_type_internal(&UserTypeMetadata {
                keyspace: "ks".to_string(),
                name: "tag".to_string(),
                fields: vec![("label".to_string(), CqlType::Varchar)],
            })
            .unwrap();

        let resolved = resolve_type_name(
            &CqlTypeName::List(Box::new(CqlTypeName::Frozen(Box::new(
                CqlTypeName::Simple("tag".to_string()),
            )))),
            "ks",
            &schema,
        )
        .unwrap();
        match resolved {
            CqlType::List(inner) => match *inner {
                CqlType::Udt { name, .. } => assert_eq!(name, "tag"),
                other => panic!("expected Udt inside List, got {:?}", other),
            },
            other => panic!("expected List, got {:?}", other),
        }
    }

    // --- UDT literal coercion tests ---

    #[test]
    fn udt_literal_coercion() {
        let udt_type = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("city".to_string(), CqlType::Varchar),
            ],
        };
        let term = Term::MapLiteral(vec![
            (
                Term::StringLiteral("street".to_string()),
                Term::StringLiteral("123 Main".to_string()),
            ),
            (
                Term::StringLiteral("city".to_string()),
                Term::StringLiteral("Springfield".to_string()),
            ),
        ]);
        let val = term_to_cql_value(&term, &udt_type).unwrap();
        match val {
            CqlValue::Udt(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "street");
                assert_eq!(fields[0].1, Some(CqlValue::Text("123 Main".to_string())));
                assert_eq!(fields[1].0, "city");
                assert_eq!(fields[1].1, Some(CqlValue::Text("Springfield".to_string())));
            }
            other => panic!("expected Udt, got {:?}", other),
        }
    }

    #[test]
    fn udt_literal_missing_field_is_none() {
        let udt_type = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("city".to_string(), CqlType::Varchar),
            ],
        };
        // Only provide 'street', leave 'city' missing
        let term = Term::MapLiteral(vec![(
            Term::StringLiteral("street".to_string()),
            Term::StringLiteral("123 Main".to_string()),
        )]);
        let val = term_to_cql_value(&term, &udt_type).unwrap();
        match val {
            CqlValue::Udt(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].1, Some(CqlValue::Text("123 Main".to_string())));
                // Missing field should be None
                assert_eq!(fields[1].0, "city");
                assert_eq!(fields[1].1, None);
            }
            other => panic!("expected Udt, got {:?}", other),
        }
    }

    #[test]
    fn udt_literal_case_insensitive_keys() {
        let udt_type = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![("street".to_string(), CqlType::Varchar)],
        };
        // Key uses different case
        let term = Term::MapLiteral(vec![(
            Term::StringLiteral("STREET".to_string()),
            Term::StringLiteral("123 Main".to_string()),
        )]);
        let val = term_to_cql_value(&term, &udt_type).unwrap();
        match val {
            CqlValue::Udt(fields) => {
                assert_eq!(fields[0].0, "street");
                assert_eq!(fields[0].1, Some(CqlValue::Text("123 Main".to_string())));
            }
            other => panic!("expected Udt, got {:?}", other),
        }
    }

    #[test]
    fn udt_literal_nested_types() {
        let udt_type = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "person".to_string(),
            fields: vec![
                ("name".to_string(), CqlType::Varchar),
                ("age".to_string(), CqlType::Int),
            ],
        };
        let term = Term::MapLiteral(vec![
            (
                Term::StringLiteral("name".to_string()),
                Term::StringLiteral("Alice".to_string()),
            ),
            (
                Term::StringLiteral("age".to_string()),
                Term::IntegerLiteral(30),
            ),
        ]);
        let val = term_to_cql_value(&term, &udt_type).unwrap();
        match val {
            CqlValue::Udt(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].1, Some(CqlValue::Text("Alice".to_string())));
                assert_eq!(fields[1].1, Some(CqlValue::Int(30)));
            }
            other => panic!("expected Udt, got {:?}", other),
        }
    }
}
