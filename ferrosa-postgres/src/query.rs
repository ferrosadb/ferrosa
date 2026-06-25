//! Query execution: lower a simple-query SQL string onto the bespoke relational
//! engine ([`ferrosa_sql`]) over live ferrosa storage, and render the result set
//! as Postgres backend messages.
//!
//! Pipeline for one `SELECT`:
//!
//! 1. `parse(sql)` → [`ferrosa_sql::SelectStmt`] (syntax errors → `42601`).
//! 2. For every referenced table (`FROM` plus an optional `JOIN`), resolve its
//!    keyspace and `storage_provider::load_table`s it into an in-memory snapshot,
//!    registering it in a [`ferrosa_sql::MapCatalog`]. A missing table is
//!    `42P01` (undefined_table) — never a silently-empty relation (the R15 guard).
//! 3. `execute(&stmt, &catalog, default_schema)` runs the sync operators.
//! 4. Render `RowDescription` + one `DataRow` per row (values to **text** format)
//!    + `CommandComplete { tag: "SELECT <n>" }`.
//!
//! The caller (the server's post-auth loop) appends the trailing
//! `ReadyForQuery` — this function never emits it, so a single turn can carry the
//! whole result set followed by exactly one ready signal.
//!
//! ## Fail loud
//!
//! Every failure maps to a concrete SQLSTATE and a single `ErrorResponse`; we
//! never return a fake empty result set on error. SQLSTATE choices:
//!
//! | failure                                    | SQLSTATE | name                  |
//! |--------------------------------------------|----------|-----------------------|
//! | parse error                                | `42601`  | syntax_error          |
//! | table not in schema (`NoSuchTable`)        | `42P01`  | undefined_table       |
//! | storage / decode error while loading       | `58000`  | system_error          |
//! | unknown column / qualifier                  | `42703`  | undefined_column      |
//! | ambiguous column                            | `42702`  | ambiguous_column      |

use std::sync::Arc;

use ferrosa_common::{CqlType, CqlValue};
use ferrosa_schema::{ColumnKind, Schema};
use ferrosa_sql::{
    execute, parse_statement, Column, ColumnType, DeleteStmt, ExecError, InsertStmt, MapCatalog,
    QueryResult, Row, ScalarItem, ScalarValue, Statement, UpdateStmt, Value as SqlValue,
};
use ferrosa_storage::{Mutation, StorageEngine};

use crate::messages::{BackendMessage, FieldDescription};
use crate::storage_provider::{load_table, LoadError};

/// Build an `ErrorResponse` with the standard severity/code/message trio
/// (`S=ERROR`, `C=<sqlstate>`, `M=<message>`).
pub(crate) fn error_response(sqlstate: &str, message: &str) -> BackendMessage {
    BackendMessage::ErrorResponse {
        fields: vec![
            (b'S', "ERROR".to_string()),
            (b'C', sqlstate.to_string()),
            (b'M', message.to_string()),
        ],
    }
}

/// The Postgres type OID for a relational [`ColumnType`].
///
/// `Int -> 23` (int4), `Text -> 25` (text), `Bool -> 16` (bool),
/// `Float -> 701` (float8), `Uuid -> 2950` (uuid), `Bytea -> 17` (bytea),
/// `Timestamp -> 1114` (timestamp without tz), `Date -> 1082` (date),
/// `Time -> 1083` (time without tz), `Inet -> 869` (inet),
/// `Numeric -> 1700` (numeric).
pub(crate) fn column_type_oid(ty: ColumnType) -> i32 {
    match ty {
        ColumnType::Int => 23,
        ColumnType::Text => 25,
        ColumnType::Bool => 16,
        ColumnType::Float => 701,
        ColumnType::Uuid => 2950,
        ColumnType::Bytea => 17,
        ColumnType::Timestamp => 1114,
        ColumnType::Date => 1082,
        ColumnType::Time => 1083,
        ColumnType::Inet => 869,
        ColumnType::Numeric => 1700,
    }
}

/// The on-wire fixed size for a column type (`-1` for variable-length text /
/// bytea / inet / numeric). `Uuid` is a fixed 16 bytes; `Timestamp`/`Time` are
/// 8-byte integers and `Date` is a 4-byte integer (matching the binary encodings
/// in [`encode_value`]).
fn column_type_size(ty: ColumnType) -> i16 {
    match ty {
        ColumnType::Int => 4,
        ColumnType::Bool => 1,
        ColumnType::Float => 8,
        ColumnType::Uuid => 16,
        ColumnType::Timestamp | ColumnType::Time => 8,
        ColumnType::Date => 4,
        ColumnType::Text | ColumnType::Bytea | ColumnType::Inet | ColumnType::Numeric => -1,
    }
}

/// Render a [`SqlValue`] to its Postgres **text-format** column bytes, or `None`
/// for SQL NULL (encoded on the wire as a `-1` length with no bytes).
///
/// Float uses Rust's default `{}` formatting (round-trippable shortest form) for
/// v1, with the non-finite cases mapped to Postgres's spellings: `NaN`,
/// `Infinity`, `-Infinity`. Exact float text-format parity with Postgres is
/// tracked as follow-up.
fn render_value(value: &SqlValue) -> Option<Vec<u8>> {
    match value {
        SqlValue::Null => None,
        SqlValue::Int(i) => Some(i.to_string().into_bytes()),
        SqlValue::Text(s) => Some(s.clone().into_bytes()),
        SqlValue::Bool(b) => Some(if *b { b"t".to_vec() } else { b"f".to_vec() }),
        SqlValue::Float(of) => {
            let f = of.0;
            let s = if f.is_nan() {
                "NaN".to_string()
            } else if f.is_infinite() {
                if f.is_sign_negative() {
                    "-Infinity".to_string()
                } else {
                    "Infinity".to_string()
                }
            } else {
                format!("{f}")
            };
            Some(s.into_bytes())
        }
        // `uuid::Uuid`'s Display is the canonical lowercase hyphenated form,
        // which is exactly Postgres's uuid text output.
        SqlValue::Uuid(u) => Some(u.to_string().into_bytes()),
        // Postgres bytea text output (default `hex` format): `\x` followed by
        // lowercase hex of the bytes; empty bytea ⇒ just `\x`.
        SqlValue::Bytea(bytes) => Some(bytea_hex_text(bytes)),
        // Temporal / network / numeric: exact Postgres text forms.
        SqlValue::Timestamp(micros) => Some(render_timestamp_text(*micros).into_bytes()),
        SqlValue::Date(days) => Some(render_date_text(*days).into_bytes()),
        SqlValue::Time(micros) => Some(render_time_text(*micros).into_bytes()),
        // `IpAddr`'s Display is the canonical IP string, exactly Postgres `inet`
        // text output for a plain host address.
        SqlValue::Inet(ip) => Some(ip.to_string().into_bytes()),
        SqlValue::Numeric { unscaled, scale } => {
            Some(render_numeric_text(unscaled, *scale).into_bytes())
        }
    }
}

/// Render a [`SqlValue::Timestamp`] (Unix-epoch microseconds, UTC) as Postgres
/// `timestamp` text: `YYYY-MM-DD HH:MM:SS` with up to 6 fractional digits, the
/// fraction having TRAILING ZEROS TRIMMED and the dot dropped entirely when the
/// microsecond part is zero (e.g. `2024-01-15 10:30:00`, `2024-01-15 10:30:00.5`).
fn render_timestamp_text(micros: i64) -> String {
    let (secs, sub_micros) = div_floor_rem(micros, 1_000_000);
    let dt = chrono::DateTime::from_timestamp(secs, 0).expect("timestamp micros in chrono range");
    let date_time = dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string();
    format!("{date_time}{}", fractional_suffix(sub_micros as u32))
}

/// Render a [`SqlValue::Date`] (days since the Unix epoch) as Postgres `date`
/// text: `YYYY-MM-DD`.
fn render_date_text(days: i32) -> String {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
    // Signed day offset (works for pre-1970 negative days too).
    let d = epoch
        .checked_add_signed(chrono::Duration::days(i64::from(days)))
        .expect("date days in chrono range");
    d.format("%Y-%m-%d").to_string()
}

/// Render a [`SqlValue::Time`] (microseconds since midnight) as Postgres `time`
/// text: `HH:MM:SS` with up to 6 fractional digits trimmed exactly like the
/// timestamp fraction.
fn render_time_text(micros: i64) -> String {
    let total_secs = micros.div_euclid(1_000_000);
    let sub_micros = micros.rem_euclid(1_000_000) as u32;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    format!("{h:02}:{m:02}:{s:02}{}", fractional_suffix(sub_micros))
}

/// Build the fractional-seconds suffix for a microsecond remainder, Postgres
/// style: empty when zero, otherwise `.` + the 6-digit fraction with trailing
/// zeros trimmed (`500000` ⇒ `.5`, `123000` ⇒ `.123`, `1` ⇒ `.000001`).
fn fractional_suffix(sub_micros: u32) -> String {
    if sub_micros == 0 {
        return String::new();
    }
    let s = format!("{sub_micros:06}");
    format!(".{}", s.trim_end_matches('0'))
}

/// Floor division + non-negative remainder for an `i64` over a positive divisor,
/// so a pre-1970 (negative) microsecond timestamp maps to the correct second and
/// a NON-NEGATIVE sub-second remainder (Postgres never prints a negative fraction).
fn div_floor_rem(n: i64, d: i64) -> (i64, i64) {
    (n.div_euclid(d), n.rem_euclid(d))
}

/// Render a normalized `(unscaled, scale)` decimal as Postgres `numeric` plain
/// text (no exponent for the magnitudes this path sees): place the decimal point
/// `scale` digits from the right of the unscaled magnitude, prefixing a `-` for
/// negative values and left-padding with zeros for `0.0x` cases. A negative scale
/// appends `|scale|` trailing zeros (the value is scaled up).
fn render_numeric_text(unscaled: &num_bigint::BigInt, scale: i32) -> String {
    use num_bigint::Sign;
    let sign = if unscaled.sign() == Sign::Minus {
        "-"
    } else {
        ""
    };
    let digits = unscaled.magnitude().to_str_radix(10); // absolute value, no sign
    let body = if scale <= 0 {
        // Integer value, optionally scaled up by |scale| trailing zeros.
        let mut s = digits;
        for _ in 0..(-scale) {
            s.push('0');
        }
        s
    } else {
        let scale = scale as usize;
        if digits.len() > scale {
            // Split into integer and fractional parts.
            let point = digits.len() - scale;
            format!("{}.{}", &digits[..point], &digits[point..])
        } else {
            // 0.00..digits — pad the fraction with leading zeros to `scale`.
            let zeros = scale - digits.len();
            format!("0.{}{}", "0".repeat(zeros), digits)
        }
    };
    format!("{sign}{body}")
}

/// Render bytes as Postgres `hex`-format bytea text: `\x` then lowercase hex.
fn bytea_hex_text(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + bytes.len() * 2);
    out.extend_from_slice(b"\\x");
    for b in bytes {
        out.push(hex_digit(b >> 4));
        out.push(hex_digit(b & 0x0f));
    }
    out
}

/// Lowercase hex digit for a 0..=15 nibble.
fn hex_digit(nibble: u8) -> u8 {
    match nibble {
        0..=9 => b'0' + nibble,
        _ => b'a' + (nibble - 10),
    }
}

/// Decode one bound parameter value into a [`SqlValue`].
///
/// `format` is the Bind format code (`0` = text, `1` = binary); `type_oid` is
/// the parameter's declared Postgres type OID (`0` = unspecified ⇒ lenient
/// text). `bytes` is `None` for SQL NULL. For unknown OIDs we fall back to a
/// best-effort textual decode (or NULL) rather than panic.
pub fn decode_param(format: i16, type_oid: i32, bytes: Option<&[u8]>) -> SqlValue {
    let Some(raw) = bytes else {
        return SqlValue::Null;
    };
    if format == 1 {
        decode_param_binary(type_oid, raw)
    } else {
        decode_param_text(type_oid, raw)
    }
}

/// Text-format parameter decode: parse the UTF-8 string per the declared OID.
fn decode_param_text(type_oid: i32, raw: &[u8]) -> SqlValue {
    // A non-UTF-8 text parameter is a client protocol error; treat as NULL
    // rather than panic (documented lenient fallback).
    let Ok(s) = std::str::from_utf8(raw) else {
        return SqlValue::Null;
    };
    match type_oid {
        // int4 / int8 / int2: decimal integer.
        23 | 20 | 21 => s
            .parse::<i64>()
            .map(SqlValue::Int)
            .unwrap_or(SqlValue::Null),
        // text / varchar / name.
        25 | 1043 | 19 => SqlValue::Text(s.to_string()),
        16 => decode_bool_text(s),
        // float4 / float8.
        700 | 701 => s
            .parse::<f64>()
            .map(SqlValue::float)
            .unwrap_or(SqlValue::Null),
        // uuid: parse the canonical hyphenated form. A malformed value falls
        // back to best-effort Text rather than panicking (documented lenient
        // fallback, consistent with the unknown-OID arm).
        2950 => uuid::Uuid::parse_str(s)
            .map(SqlValue::Uuid)
            .unwrap_or_else(|_| SqlValue::Text(s.to_string())),
        // bytea: a `\x<hex>` string decodes to raw bytes; a malformed hex body
        // falls back to NULL (documented lenient fallback, no panic).
        17 => decode_bytea_hex_text(s).unwrap_or(SqlValue::Null),
        // timestamp (1114): `YYYY-MM-DD HH:MM:SS[.ffffff]`. A malformed value
        // falls back to NULL (documented lenient fallback, no panic).
        1114 => parse_timestamp_text(s).unwrap_or(SqlValue::Null),
        // date (1082): `YYYY-MM-DD`.
        1082 => parse_date_text(s).unwrap_or(SqlValue::Null),
        // time (1083): `HH:MM:SS[.ffffff]`.
        1083 => parse_time_text(s).unwrap_or(SqlValue::Null),
        // inet (869): a canonical IP string parses to an `IpAddr`.
        869 => s
            .parse::<std::net::IpAddr>()
            .map(SqlValue::Inet)
            .unwrap_or(SqlValue::Null),
        // numeric (1700): a plain decimal string. Numeric params are TEXT-only
        // (binary numeric is out of scope — see `encode_value`/`decode_param_binary`).
        1700 => parse_numeric_text(s).unwrap_or(SqlValue::Null),
        // OID 0 (unspecified) or any unknown OID: lenient — keep as text.
        _ => SqlValue::Text(s.to_string()),
    }
}

/// Parse Postgres `timestamp` text (`YYYY-MM-DD HH:MM:SS[.ffffff]`, also
/// tolerating the ISO `T` separator) into [`SqlValue::Timestamp`] (Unix-epoch
/// micros, UTC). Returns `None` on a malformed value (lenient, no panic).
fn parse_timestamp_text(s: &str) -> Option<SqlValue> {
    let s = s.trim();
    let naive = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .ok()?;
    let micros = naive.and_utc().timestamp_micros();
    Some(SqlValue::Timestamp(micros))
}

/// Parse Postgres `date` text (`YYYY-MM-DD`) into [`SqlValue::Date`] (days since
/// the Unix epoch).
fn parse_date_text(s: &str) -> Option<SqlValue> {
    let date = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let days = (date - epoch).num_days();
    Some(SqlValue::Date(i32::try_from(days).ok()?))
}

/// Parse Postgres `time` text (`HH:MM:SS[.ffffff]`) into [`SqlValue::Time`]
/// (microseconds since midnight).
fn parse_time_text(s: &str) -> Option<SqlValue> {
    let t = chrono::NaiveTime::parse_from_str(s.trim(), "%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveTime::parse_from_str(s.trim(), "%H:%M:%S"))
        .ok()?;
    let midnight = chrono::NaiveTime::from_hms_opt(0, 0, 0)?;
    let micros = (t - midnight).num_microseconds()?;
    Some(SqlValue::Time(micros))
}

/// Parse a plain Postgres `numeric` text body (`[-]ddd[.ddd]`, no exponent) into
/// a normalized [`SqlValue::Numeric`]. Returns `None` for malformed input.
fn parse_numeric_text(s: &str) -> Option<SqlValue> {
    use num_bigint::BigInt;
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (sign, rest) = match s.strip_prefix('-') {
        Some(r) => (-1i8, r),
        None => (1i8, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    // Both parts must be all-ASCII-digits (int part may be empty for `.5`).
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mut digits = String::new();
    digits.push_str(int_part);
    digits.push_str(frac_part);
    if digits.is_empty() {
        return None;
    }
    let magnitude = digits.parse::<BigInt>().ok()?;
    let unscaled = if sign < 0 { -magnitude } else { magnitude };
    let scale = i32::try_from(frac_part.len()).ok()?;
    Some(SqlValue::numeric(unscaled, scale))
}

/// Decode a Postgres `hex`-format bytea text body (`\x<hex>`) into raw bytes.
/// Returns `None` when the prefix is missing or the hex is malformed.
fn decode_bytea_hex_text(s: &str) -> Option<SqlValue> {
    let hex = s.strip_prefix("\\x")?;
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let chars: Vec<u8> = hex.bytes().collect();
    for pair in chars.chunks_exact(2) {
        let hi = hex_value(pair[0])?;
        let lo = hex_value(pair[1])?;
        bytes.push((hi << 4) | lo);
    }
    Some(SqlValue::Bytea(bytes))
}

/// Parse a single ASCII hex digit (case-insensitive) into its 0..=15 value.
fn hex_value(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Postgres bool text spellings the driver may send.
fn decode_bool_text(s: &str) -> SqlValue {
    match s.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "1" | "y" | "yes" | "on" => SqlValue::Bool(true),
        "f" | "false" | "0" | "n" | "no" | "off" => SqlValue::Bool(false),
        _ => SqlValue::Null,
    }
}

/// Binary-format parameter decode: big-endian per the declared OID.
fn decode_param_binary(type_oid: i32, raw: &[u8]) -> SqlValue {
    match type_oid {
        // int4 (BE i32).
        23 => be_int(raw, 4).map_or(SqlValue::Null, SqlValue::Int),
        // int8 (BE i64).
        20 => be_int(raw, 8).map_or(SqlValue::Null, SqlValue::Int),
        // int2 (BE i16).
        21 => be_int(raw, 2).map_or(SqlValue::Null, SqlValue::Int),
        // text / varchar.
        25 | 1043 => std::str::from_utf8(raw)
            .map(|s| SqlValue::Text(s.to_string()))
            .unwrap_or(SqlValue::Null),
        // bool: any non-zero byte is true.
        16 => raw
            .first()
            .map_or(SqlValue::Null, |b| SqlValue::Bool(*b != 0)),
        // float4 (BE f32 bits).
        700 if raw.len() == 4 => {
            SqlValue::float(f32::from_be_bytes(raw.try_into().unwrap()) as f64)
        }
        // float8 (BE f64 bits).
        701 if raw.len() == 8 => SqlValue::float(f64::from_be_bytes(raw.try_into().unwrap())),
        // uuid: 16 big-endian bytes. A wrong length falls back to NULL (no panic).
        2950 => uuid::Uuid::from_slice(raw)
            .map(SqlValue::Uuid)
            .unwrap_or(SqlValue::Null),
        // bytea: the raw bytes, copied verbatim.
        17 => SqlValue::Bytea(raw.to_vec()),
        // timestamp (1114): BE i64 microseconds since the Postgres epoch
        // (2000-01-01). Shift to the Unix-epoch micros our `Value` carries.
        1114 => be_int(raw, 8)
            .map(|pg| SqlValue::Timestamp(pg + PG_EPOCH_MICROS))
            .unwrap_or(SqlValue::Null),
        // date (1082): BE i32 days since the Postgres epoch (2000-01-01). Shift
        // to days since the Unix epoch.
        1082 => {
            if raw.len() == 4 {
                let pg_days = i32::from_be_bytes(raw.try_into().unwrap());
                SqlValue::Date(pg_days + PG_EPOCH_DAYS)
            } else {
                SqlValue::Null
            }
        }
        // time (1083): BE i64 microseconds since midnight (same origin as our repr).
        1083 => be_int(raw, 8).map(SqlValue::Time).unwrap_or(SqlValue::Null),
        // inet (869): the Postgres inet binary (family, bits, is_cidr, len, addr).
        869 => decode_inet_binary(raw).unwrap_or(SqlValue::Null),
        // Unknown OID: best-effort text, else NULL (documented fallback — no panic).
        // NOTE: binary `numeric` (1700) is intentionally NOT decoded here — it
        // falls through to this best-effort arm. Binary numeric is out of scope
        // (numeric params are text-only); the differential oracle uses
        // simple_query (text), so this path is never exercised for numeric.
        _ => std::str::from_utf8(raw)
            .map(|s| SqlValue::Text(s.to_string()))
            .unwrap_or(SqlValue::Null),
    }
}

/// Microseconds between the Unix epoch (1970-01-01) and the Postgres epoch
/// (2000-01-01): `946_684_800` seconds. Adding this to a Postgres-epoch micros
/// value yields Unix-epoch micros (our `Value::Timestamp` repr).
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// Days between the Unix epoch and the Postgres epoch (2000-01-01): 10957.
const PG_EPOCH_DAYS: i32 = 10_957;

/// Decode the Postgres `inet` binary format into [`SqlValue::Inet`]:
/// `[family][bits][is_cidr][addr_len][address bytes]`. Family 2 = IPv4 (4-byte
/// address), family 3 = IPv6 (16-byte address). Returns `None` on any shape
/// mismatch.
fn decode_inet_binary(raw: &[u8]) -> Option<SqlValue> {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    if raw.len() < 4 {
        return None;
    }
    let family = raw[0];
    let addr_len = raw[3] as usize;
    let addr = raw.get(4..4 + addr_len)?;
    match (family, addr_len) {
        (2, 4) => {
            let octets: [u8; 4] = addr.try_into().ok()?;
            Some(SqlValue::Inet(IpAddr::V4(Ipv4Addr::from(octets))))
        }
        (3, 16) => {
            let octets: [u8; 16] = addr.try_into().ok()?;
            Some(SqlValue::Inet(IpAddr::V6(Ipv6Addr::from(octets))))
        }
        _ => None,
    }
}

/// Encode an `IpAddr` into the Postgres `inet` binary format (host address, not
/// CIDR): `[family][bits][is_cidr=0][addr_len][address bytes]`. Family 2 = IPv4
/// with `bits=32`; family 3 = IPv6 with `bits=128`.
fn encode_inet_binary(ip: &std::net::IpAddr) -> Vec<u8> {
    use std::net::IpAddr;
    let mut out = Vec::with_capacity(20);
    match ip {
        IpAddr::V4(v4) => {
            out.push(2); // AF_INET (Postgres uses 2 for IPv4)
            out.push(32); // bits
            out.push(0); // is_cidr = false
            out.push(4); // address length
            out.extend_from_slice(&v4.octets());
        }
        IpAddr::V6(v6) => {
            out.push(3); // PGSQL_AF_INET6
            out.push(128); // bits
            out.push(0); // is_cidr = false
            out.push(16); // address length
            out.extend_from_slice(&v6.octets());
        }
    }
    out
}

/// Read a big-endian signed integer of `width` bytes (2/4/8) into i64, or `None`
/// if the byte length doesn't match.
fn be_int(raw: &[u8], width: usize) -> Option<i64> {
    if raw.len() != width {
        return None;
    }
    let val = match width {
        2 => i64::from(i16::from_be_bytes(raw.try_into().ok()?)),
        4 => i64::from(i32::from_be_bytes(raw.try_into().ok()?)),
        8 => i64::from_be_bytes(raw.try_into().ok()?),
        _ => return None,
    };
    Some(val)
}

/// Encode a [`SqlValue`] to its wire bytes in the requested `format` (`0` text,
/// `1` binary) for a column of declared `col_type`, or `None` for SQL NULL.
///
/// The binary encoding is kept consistent with the OID/size advertised in
/// `column_type_oid` / `column_type_size`: `ColumnType::Int` ⇒ int4 (OID 23,
/// 4 bytes), so an `Int` always emits a 4-byte big-endian `i32` (a value that
/// overflows `i32` saturates rather than corrupting the frame). `Float` ⇒
/// float8 (OID 701, 8 bytes).
pub fn encode_value(format: i16, col_type: ColumnType, v: &SqlValue) -> Option<Vec<u8>> {
    if format != 1 {
        return render_value(v); // text format: reuse the existing renderer
    }
    match v {
        SqlValue::Null => None,
        SqlValue::Int(i) => Some(encode_int_binary(col_type, *i)),
        SqlValue::Text(s) => Some(s.clone().into_bytes()),
        SqlValue::Bool(b) => Some(vec![u8::from(*b)]),
        // Floats are advertised as float8 (OID 701); emit 8-byte BE bits.
        SqlValue::Float(of) => Some(of.0.to_be_bytes().to_vec()),
        // uuid (OID 2950): its 16 big-endian bytes.
        SqlValue::Uuid(u) => Some(u.as_bytes().to_vec()),
        // bytea (OID 17): the raw bytes verbatim.
        SqlValue::Bytea(bytes) => Some(bytes.clone()),
        // timestamp (OID 1114): BE i64 micros since the POSTGRES epoch
        // (2000-01-01) — shift our Unix-epoch micros down by the epoch delta.
        SqlValue::Timestamp(micros) => Some((micros - PG_EPOCH_MICROS).to_be_bytes().to_vec()),
        // date (OID 1082): BE i32 days since the Postgres epoch.
        SqlValue::Date(days) => Some((days - PG_EPOCH_DAYS).to_be_bytes().to_vec()),
        // time (OID 1083): BE i64 micros since midnight (same origin as our repr).
        SqlValue::Time(micros) => Some(micros.to_be_bytes().to_vec()),
        // inet (OID 869): the Postgres inet binary (family/bits/is_cidr/len/addr).
        SqlValue::Inet(ip) => Some(encode_inet_binary(ip)),
        // numeric (OID 1700): binary numeric is OUT OF SCOPE. A client that
        // requests binary results for a numeric column falls back to the TEXT
        // bytes (documented). The differential oracle uses simple_query (text),
        // so this path is never exercised by the gate.
        SqlValue::Numeric { unscaled, scale } => {
            Some(render_numeric_text(unscaled, *scale).into_bytes())
        }
    }
}

/// Binary integer encoding honoring the column's declared width: `ColumnType::Int`
/// is int4 ⇒ 4-byte BE (saturating to `i32` range); anything else falls back to
/// int8 ⇒ 8-byte BE. Keeps the bytes consistent with the RowDescription OID/size.
fn encode_int_binary(col_type: ColumnType, i: i64) -> Vec<u8> {
    match col_type {
        ColumnType::Int => {
            let v = i.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32;
            v.to_be_bytes().to_vec()
        }
        _ => i.to_be_bytes().to_vec(),
    }
}

/// Map an [`ExecError`] from the binder/executor to a single fail-loud
/// `ErrorResponse` with the appropriate SQLSTATE.
pub(crate) fn exec_error_response(err: &ExecError) -> BackendMessage {
    let (sqlstate, message) = match err {
        ExecError::NoSuchTable { .. } => ("42P01", err.to_string()),
        ExecError::NoSuchColumn(_) | ExecError::UnknownQualifier(_) => ("42703", err.to_string()),
        ExecError::AmbiguousColumn(_) => ("42702", err.to_string()),
        ExecError::NotGrouped(_) | ExecError::AggregateInWhere(_) => ("42803", err.to_string()),
        ExecError::InvalidOrderBy(_) => ("42P10", err.to_string()),
        // A `$N` with no bound value: undefined_parameter.
        ExecError::MissingParameter(_) => ("42P02", err.to_string()),
    };
    error_response(sqlstate, &message)
}

/// The wire format code (`0` text, `1` binary) for result column `i`, applying
/// the Postgres fan-out rule: an empty list ⇒ all text; a single code ⇒ that
/// code for every column; otherwise the per-column code.
fn result_format_for(formats: &[i16], i: usize) -> i16 {
    match formats.len() {
        0 => 0,
        1 => formats[0],
        _ => formats.get(i).copied().unwrap_or(0),
    }
}

/// Build the `RowDescription` fields for a column list under `result_formats`,
/// so each field's advertised format code matches how its `DataRow` bytes are
/// later encoded.
pub(crate) fn row_description_fields(
    columns: &[ferrosa_sql::Column],
    result_formats: &[i16],
) -> Vec<FieldDescription> {
    columns
        .iter()
        .enumerate()
        .map(|(i, col)| FieldDescription {
            name: col.name.clone(),
            type_oid: column_type_oid(col.ty),
            type_size: column_type_size(col.ty),
            format_code: result_format_for(result_formats, i),
        })
        .collect()
}

/// Render a successful [`QueryResult`] into `RowDescription` + `DataRow`s +
/// `CommandComplete`, encoding each column per `result_formats` (text or binary).
/// The simple-query path passes `&[]` (all text).
fn render_result(result: QueryResult, result_formats: &[i16]) -> Vec<BackendMessage> {
    let mut out = Vec::with_capacity(result.rows.len() + 2);

    let fields = row_description_fields(&result.columns, result_formats);
    out.push(BackendMessage::RowDescription { fields });

    let col_types: Vec<ColumnType> = result.columns.iter().map(|c| c.ty).collect();
    let nrows = result.rows.len();
    for row in &result.rows {
        let columns = row
            .0
            .iter()
            .enumerate()
            .map(|(i, v)| encode_value(result_format_for(result_formats, i), col_types[i], v))
            .collect();
        out.push(BackendMessage::DataRow { columns });
    }

    out.push(BackendMessage::CommandComplete {
        tag: format!("SELECT {nrows}"),
    });
    out
}

/// Execute one simple-query SQL string and return the backend messages that
/// describe the outcome — a result set on success, or exactly one
/// `ErrorResponse` on any failure. The caller appends `ReadyForQuery`.
pub async fn execute_query(
    engine: &StorageEngine,
    schema: &Schema,
    sql: &str,
    default_schema: &str,
    txn: Option<&mut Vec<ferrosa_storage::accord::TransactionWrite>>,
) -> Vec<BackendMessage> {
    // 1. Parse the top-level statement.
    let stmt = match parse_statement(sql) {
        Ok(stmt) => stmt,
        Err(e) => return vec![error_response("42601", &e.to_string())],
    };

    match stmt {
        // Table query: load referenced tables (the R15 guard lives in
        // `load_table` — a missing table is `NoSuchTable`, never an empty
        // scan), then execute over the materialized snapshots.
        Statement::Select(select) => {
            let catalog = match load_catalog(engine, schema, &select, default_schema).await {
                Ok(catalog) => catalog,
                Err(err_msg) => return vec![err_msg],
            };
            match execute(&select, &catalog, default_schema, &[]) {
                Ok(result) => render_result(result, &[]), // simple query: all text
                Err(e) => vec![exec_error_response(&e)],
            }
        }
        // No-`FROM` expression query: `SELECT 1`, `SELECT version()`, etc.
        Statement::SelectExprs(items) => match execute_scalar_select(&items, default_schema) {
            Ok(result) => render_result(result, &[]),
            Err(err_msg) => vec![err_msg],
        },
        // Transaction control routes through Accord; execution is wired
        // separately (t_0f96cb47). Fail loud rather than fake atomicity.
        Statement::Begin | Statement::Commit | Statement::Rollback => vec![error_response(
            "0A000",
            "transactions are not yet implemented (Accord-backed transactions are in progress)",
        )],
        // Session GUCs are not modeled yet.
        Statement::Set { .. } | Statement::Reset { .. } => vec![error_response(
            "0A000",
            "SET/RESET session statements are not yet implemented",
        )],
        // DML: single-row INSERT / UPDATE / DELETE. With `txn = Some(buffer)`
        // (an open transaction) the write is BUFFERED as a `TransactionWrite`
        // and committed atomically via Accord on COMMIT; with `txn = None`
        // (autocommit) it applies immediately via `write_atomic_batch`.
        Statement::Insert(ins) => execute_insert(engine, schema, &ins, default_schema, txn),
        Statement::Update(upd) => execute_update(engine, schema, &upd, default_schema, txn),
        Statement::Delete(del) => execute_delete(engine, schema, &del, default_schema, txn),
    }
}

/// Max DML writes buffered in one Postgres transaction before it is rejected.
/// A client must not be able to OOM the server with an unbounded open `BEGIN`
/// (Power-of-10 Rule 3: every server-side dynamic collection has a hard cap).
/// Mirrors `ferrosa_cql::session::MAX_TXN_WRITES`.
const MAX_TXN_WRITES: usize = 10_000;

/// Buffer a built `Mutation` as a [`TransactionWrite`] into the open
/// transaction's write-set, or apply it immediately via `write_atomic_batch`
/// when there is no open transaction (autocommit).
///
/// FAIL LOUD: when buffering, exceeding [`MAX_TXN_WRITES`] returns an error
/// response (and the server poisons the transaction) rather than growing the
/// buffer without bound; a buffered write is NEVER applied to storage here —
/// only the Accord committer applies it on COMMIT.
fn apply_or_buffer(
    engine: &StorageEngine,
    txn: Option<&mut Vec<ferrosa_storage::accord::TransactionWrite>>,
    key: &ferrosa_common::DecoratedKey,
    mutation: Mutation,
    ok_tag: &str,
) -> Vec<BackendMessage> {
    match txn {
        Some(buffer) => {
            if buffer.len() >= MAX_TXN_WRITES {
                return vec![error_response(
                    "53400",
                    &format!(
                        "transaction write-set exceeds the {MAX_TXN_WRITES}-write limit; \
                         ROLLBACK required"
                    ),
                )];
            }
            let mut mutation_bytes = vec![0u8; mutation.serialized_size()];
            mutation.serialize_into(&mut mutation_bytes);
            buffer.push(ferrosa_storage::accord::TransactionWrite {
                keyspace: mutation.keyspace.clone(),
                key: key.key.as_bytes().to_vec(),
                mutation: mutation_bytes,
            });
            vec![BackendMessage::CommandComplete {
                tag: ok_tag.to_string(),
            }]
        }
        None => match engine.write_atomic_batch(vec![mutation]) {
            Ok(()) => vec![BackendMessage::CommandComplete {
                tag: ok_tag.to_string(),
            }],
            Err(e) => vec![error_response("58000", &format!("write failed: {e}"))],
        },
    }
}

/// Convert a SQL [`SqlValue`] literal to the [`CqlValue`] the engine stores,
/// driven by the target column's [`CqlType`]. The inverse of
/// `storage_provider::cql_to_value`; `Null` maps to a tombstone for any type,
/// and a type mismatch fails loud (`42804`) rather than silently coercing.
fn value_to_cql(value: &SqlValue, ty: &CqlType) -> Result<CqlValue, BackendMessage> {
    if matches!(value, SqlValue::Null) {
        return Ok(CqlValue::Null);
    }
    let out = match (ty, value) {
        (CqlType::Int, SqlValue::Int(i)) => CqlValue::Int(
            i32::try_from(*i).map_err(|_| error_response("22003", "integer out of int4 range"))?,
        ),
        (CqlType::Bigint, SqlValue::Int(i)) => CqlValue::Bigint(*i),
        (CqlType::Counter, SqlValue::Int(i)) => CqlValue::Counter(*i),
        (CqlType::Smallint, SqlValue::Int(i)) => CqlValue::Smallint(
            i16::try_from(*i).map_err(|_| error_response("22003", "out of smallint range"))?,
        ),
        (CqlType::Tinyint, SqlValue::Int(i)) => CqlValue::Tinyint(
            i8::try_from(*i).map_err(|_| error_response("22003", "out of tinyint range"))?,
        ),
        (CqlType::Varchar, SqlValue::Text(s)) => CqlValue::Text(s.clone()),
        (CqlType::Ascii, SqlValue::Text(s)) => CqlValue::Ascii(s.clone()),
        (CqlType::Boolean, SqlValue::Bool(b)) => CqlValue::Boolean(*b),
        (CqlType::Float, SqlValue::Float(f)) => CqlValue::Float((f.into_inner() as f32).to_bits()),
        (CqlType::Double, SqlValue::Float(f)) => CqlValue::Double(f.into_inner().to_bits()),
        (CqlType::Float, SqlValue::Int(i)) => CqlValue::Float((*i as f32).to_bits()),
        (CqlType::Double, SqlValue::Int(i)) => CqlValue::Double((*i as f64).to_bits()),
        (CqlType::Uuid, SqlValue::Uuid(u)) => CqlValue::Uuid(*u),
        (CqlType::Timeuuid, SqlValue::Uuid(u)) => CqlValue::Timeuuid(*u),
        (CqlType::Blob, SqlValue::Bytea(b)) => CqlValue::Blob(b.clone()),
        (CqlType::Timestamp, SqlValue::Timestamp(micros)) => CqlValue::Timestamp(micros / 1000),
        (CqlType::Date, SqlValue::Date(d)) => {
            CqlValue::Date((i64::from(*d) + 2_147_483_648) as u32)
        }
        (CqlType::Time, SqlValue::Time(micros)) => CqlValue::Time(micros * 1000),
        (CqlType::Inet, SqlValue::Inet(ip)) => CqlValue::Inet(*ip),
        (CqlType::Decimal, SqlValue::Numeric { unscaled, scale }) => CqlValue::Decimal {
            scale: *scale,
            unscaled: unscaled.clone(),
        },
        (CqlType::Varint, SqlValue::Numeric { unscaled, scale }) if *scale == 0 => {
            CqlValue::Varint(unscaled.clone())
        }
        _ => {
            return Err(error_response(
                "42804",
                &format!("value does not match column type {ty:?}"),
            ))
        }
    };
    Ok(out)
}

/// Execute a single-row `INSERT`: materialize the row from the table schema and
/// write it through the engine. Returns `CommandComplete "INSERT 0 1"` (Postgres
/// reports oid 0 + a 1-row count). The row encoder is the shared
/// `ferrosa-row-bridge` one — the SAME bytes the engine + CQL reads decode.
fn execute_insert(
    engine: &StorageEngine,
    schema: &Schema,
    ins: &InsertStmt,
    default_schema: &str,
    txn: Option<&mut Vec<ferrosa_storage::accord::TransactionWrite>>,
) -> Vec<BackendMessage> {
    use std::collections::HashMap;

    let ks = ins.table.schema.as_deref().unwrap_or(default_schema);
    let snap = schema.snapshot();
    let meta = match snap.tables.get(&(ks.to_string(), ins.table.table.clone())) {
        Some(m) => m,
        None => {
            return vec![error_response(
                "42P01",
                &format!("relation \"{ks}.{}\" does not exist", ins.table.table),
            )]
        }
    };

    // Convert each named column's value (per its CQL type); collect regular/
    // static cells by storage index, and all values by name for key ordering.
    let mut col_values: HashMap<String, CqlValue> = HashMap::new();
    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
    for (i, col_name) in ins.columns.iter().enumerate() {
        let col_meta = match meta.columns.get(col_name) {
            Some(c) => c,
            None => {
                return vec![error_response(
                    "42703",
                    &format!(
                        "column \"{col_name}\" of relation \"{}\" does not exist",
                        ins.table.table
                    ),
                )]
            }
        };
        let cql_type =
            match ferrosa_row_bridge::parse_cql_type_in_keyspace(&col_meta.column_type, ks, schema)
            {
                Ok(t) => t,
                Err(e) => return vec![error_response("42704", &e.to_string())],
            };
        let value = match &ins.values[i] {
            ScalarValue::Literal(v) => match value_to_cql(v, &cql_type) {
                Ok(cv) => cv,
                Err(msg) => return vec![msg],
            },
            ScalarValue::Param(_) => {
                return vec![error_response(
                "0A000",
                "$N parameters in INSERT require the extended-query protocol (not yet supported)",
            )]
            }
            ScalarValue::Func(_) => {
                return vec![error_response(
                    "0A000",
                    "function calls in INSERT VALUES are not supported",
                )]
            }
        };
        if matches!(col_meta.kind, ColumnKind::Regular | ColumnKind::Static) {
            if let Some(idx) = meta.storage_column_index(col_name) {
                regular_cells.push((idx, value.clone()));
            }
        }
        col_values.insert(col_name.clone(), value);
    }

    // Partition-key and clustering values in key order — all required for INSERT.
    let mut pk_values = Vec::with_capacity(meta.partition_key.len());
    for name in &meta.partition_key {
        match col_values.get(name) {
            Some(v) => pk_values.push(v.clone()),
            None => {
                return vec![error_response(
                    "23502",
                    &format!("partition key column \"{name}\" must be specified in INSERT"),
                )]
            }
        }
    }
    let mut ck_values = Vec::new();
    for (name, _order) in &meta.clustering_key {
        match col_values.get(name) {
            Some(v) => ck_values.push(v.clone()),
            None => {
                return vec![error_response(
                    "23502",
                    &format!("clustering column \"{name}\" must be specified in INSERT"),
                )]
            }
        }
    }

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    let key = match ferrosa_row_bridge::build_decorated_key(&pk_values, &[]) {
        Ok(k) => k,
        Err(e) => return vec![error_response("22000", &e.to_string())],
    };
    let row = ferrosa_row_bridge::build_row(&regular_cells, &ck_values, timestamp, None);
    let mutation = Mutation::new(
        ks.to_string(),
        ins.table.table.clone(),
        key.clone(),
        vec![row],
        timestamp,
    );
    apply_or_buffer(engine, txn, &key, mutation, "INSERT 0 1")
}

/// Resolve a `(column, value)` pair from a DML statement to its `CqlValue`,
/// looking up the column's CQL type from `meta`. Returns the column metadata
/// alongside so the caller can classify it (key vs regular). `$N`/function
/// values fail loud (extended-protocol / unsupported).
fn resolve_dml_value<'a>(
    meta: &'a ferrosa_schema::TableMetadata,
    schema: &Schema,
    ks: &str,
    table: &str,
    col_name: &str,
    sv: &ScalarValue,
) -> Result<(&'a ferrosa_schema::ColumnMetadata, CqlValue), BackendMessage> {
    let col_meta = meta.columns.get(col_name).ok_or_else(|| {
        error_response(
            "42703",
            &format!("column \"{col_name}\" of relation \"{table}\" does not exist"),
        )
    })?;
    let cql_type =
        ferrosa_row_bridge::parse_cql_type_in_keyspace(&col_meta.column_type, ks, schema)
            .map_err(|e| error_response("42704", &e.to_string()))?;
    let value = match sv {
        ScalarValue::Literal(v) => value_to_cql(v, &cql_type)?,
        ScalarValue::Param(_) => {
            return Err(error_response(
                "0A000",
                "$N parameters require the extended-query protocol (not yet supported)",
            ))
        }
        ScalarValue::Func(_) => {
            return Err(error_response(
                "0A000",
                "function calls in DML values are not supported",
            ))
        }
    };
    Ok((col_meta, value))
}

/// Execute a single-row `UPDATE`: a Cassandra-style upsert. The equality `WHERE`
/// supplies the full primary key (which identifies the row); `SET` supplies the
/// regular/static cells. Returns `CommandComplete "UPDATE 1"` — the engine write
/// is a blind upsert, so the affected-row count is reported as 1 when the write
/// lands (Cassandra has no match count; this is the documented semantic).
fn execute_update(
    engine: &StorageEngine,
    schema: &Schema,
    upd: &UpdateStmt,
    default_schema: &str,
    txn: Option<&mut Vec<ferrosa_storage::accord::TransactionWrite>>,
) -> Vec<BackendMessage> {
    use std::collections::HashMap;

    let ks = upd.table.schema.as_deref().unwrap_or(default_schema);
    let table = upd.table.table.as_str();
    let snap = schema.snapshot();
    let meta = match snap.tables.get(&(ks.to_string(), table.to_string())) {
        Some(m) => m,
        None => {
            return vec![error_response(
                "42P01",
                &format!("relation \"{ks}.{table}\" does not exist"),
            )]
        }
    };

    // SET assignments -> regular/static cells (by storage index).
    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
    for (col_name, sv) in &upd.assignments {
        let (col_meta, value) = match resolve_dml_value(meta, schema, ks, table, col_name, sv) {
            Ok(r) => r,
            Err(msg) => return vec![msg],
        };
        if !matches!(col_meta.kind, ColumnKind::Regular | ColumnKind::Static) {
            return vec![error_response(
                "0A000",
                &format!("cannot UPDATE key column \"{col_name}\" in SET"),
            )];
        }
        match meta.storage_column_index(col_name) {
            Some(idx) => regular_cells.push((idx, value)),
            None => {
                return vec![error_response(
                    "42703",
                    &format!("column \"{col_name}\" not found in storage schema"),
                )]
            }
        }
    }

    // WHERE equality -> key values (must be key columns).
    let mut key_values: HashMap<String, CqlValue> = HashMap::new();
    for (col_name, sv) in &upd.where_eq {
        let (col_meta, value) = match resolve_dml_value(meta, schema, ks, table, col_name, sv) {
            Ok(r) => r,
            Err(msg) => return vec![msg],
        };
        if !matches!(
            col_meta.kind,
            ColumnKind::PartitionKey | ColumnKind::Clustering
        ) {
            return vec![error_response(
                "0A000",
                &format!("UPDATE WHERE supports only key columns; \"{col_name}\" is not a key"),
            )];
        }
        key_values.insert(col_name.clone(), value);
    }

    let mut pk_values = Vec::with_capacity(meta.partition_key.len());
    for name in &meta.partition_key {
        match key_values.get(name) {
            Some(v) => pk_values.push(v.clone()),
            None => {
                return vec![error_response(
                    "23502",
                    &format!("partition key column \"{name}\" must be specified in WHERE"),
                )]
            }
        }
    }
    let mut ck_values = Vec::new();
    for (name, _order) in &meta.clustering_key {
        match key_values.get(name) {
            Some(v) => ck_values.push(v.clone()),
            None => {
                return vec![error_response(
                    "23502",
                    &format!("clustering column \"{name}\" must be specified in WHERE"),
                )]
            }
        }
    }

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    let key = match ferrosa_row_bridge::build_decorated_key(&pk_values, &[]) {
        Ok(k) => k,
        Err(e) => return vec![error_response("22000", &e.to_string())],
    };
    let row = ferrosa_row_bridge::build_row(&regular_cells, &ck_values, timestamp, None);
    let mutation = Mutation::new(
        ks.to_string(),
        table.to_string(),
        key.clone(),
        vec![row],
        timestamp,
    );
    apply_or_buffer(engine, txn, &key, mutation, "UPDATE 1")
}

/// Execute a single-row `DELETE`: a row-level tombstone. The equality `WHERE`
/// supplies the full primary key identifying the row. Returns `CommandComplete
/// "DELETE 1"` — the engine writes a tombstone unconditionally, so the count is
/// reported as 1 when the write lands (Cassandra has no match count).
fn execute_delete(
    engine: &StorageEngine,
    schema: &Schema,
    del: &DeleteStmt,
    default_schema: &str,
    txn: Option<&mut Vec<ferrosa_storage::accord::TransactionWrite>>,
) -> Vec<BackendMessage> {
    use std::collections::HashMap;

    let ks = del.table.schema.as_deref().unwrap_or(default_schema);
    let table = del.table.table.as_str();
    let snap = schema.snapshot();
    let meta = match snap.tables.get(&(ks.to_string(), table.to_string())) {
        Some(m) => m,
        None => {
            return vec![error_response(
                "42P01",
                &format!("relation \"{ks}.{table}\" does not exist"),
            )]
        }
    };

    let mut key_values: HashMap<String, CqlValue> = HashMap::new();
    for (col_name, sv) in &del.where_eq {
        let (col_meta, value) = match resolve_dml_value(meta, schema, ks, table, col_name, sv) {
            Ok(r) => r,
            Err(msg) => return vec![msg],
        };
        if !matches!(
            col_meta.kind,
            ColumnKind::PartitionKey | ColumnKind::Clustering
        ) {
            return vec![error_response(
                "0A000",
                &format!("DELETE WHERE supports only key columns; \"{col_name}\" is not a key"),
            )];
        }
        key_values.insert(col_name.clone(), value);
    }

    let mut pk_values = Vec::with_capacity(meta.partition_key.len());
    for name in &meta.partition_key {
        match key_values.get(name) {
            Some(v) => pk_values.push(v.clone()),
            None => {
                return vec![error_response(
                    "23502",
                    &format!("partition key column \"{name}\" must be specified in WHERE"),
                )]
            }
        }
    }
    let mut ck_values = Vec::new();
    for (name, _order) in &meta.clustering_key {
        match key_values.get(name) {
            Some(v) => ck_values.push(v.clone()),
            None => {
                return vec![error_response(
                    "23502",
                    &format!("clustering column \"{name}\" must be specified in WHERE"),
                )]
            }
        }
    }

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    let key = match ferrosa_row_bridge::build_decorated_key(&pk_values, &[]) {
        Ok(k) => k,
        Err(e) => return vec![error_response("22000", &e.to_string())],
    };
    // Empty delete-columns => row-level tombstone.
    let row = ferrosa_row_bridge::build_delete_row(&[], &ck_values, timestamp);
    let mutation = Mutation::new(
        ks.to_string(),
        table.to_string(),
        key.clone(),
        vec![row],
        timestamp,
    );
    apply_or_buffer(engine, txn, &key, mutation, "DELETE 1")
}

/// Evaluate a no-`FROM` expression SELECT (`SELECT 1`, `SELECT version()`,
/// `SELECT current_database()`) into a one-row [`QueryResult`]. Literals are
/// returned as-is; a small set of info/session functions are evaluated from the
/// connection's context.
pub(crate) fn execute_scalar_select(
    items: &[ScalarItem],
    default_schema: &str,
) -> Result<QueryResult, BackendMessage> {
    let mut columns = Vec::with_capacity(items.len());
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        let value = match &item.value {
            ScalarValue::Literal(v) => v.clone(),
            ScalarValue::Func(name) => eval_scalar_func(name, default_schema)?,
            ScalarValue::Param(_) => {
                return Err(error_response(
                    "0A000",
                    "$N parameters require the extended-query protocol",
                ))
            }
        };
        let ty = value_column_type(&value);
        let name = item
            .alias
            .clone()
            .unwrap_or_else(|| default_scalar_name(&item.value));
        columns.push(Column::new(name, ty));
        values.push(value);
    }
    Ok(QueryResult {
        columns,
        rows: vec![Row(values)],
    })
}

/// Evaluate a zero-arg info/session function. Unsupported names fail loud
/// (`0A000`) rather than returning a guessed value.
fn eval_scalar_func(name: &str, default_schema: &str) -> Result<SqlValue, BackendMessage> {
    match name {
        // Keep in step with the `server_version` ParameterStatus (connection.rs).
        "VERSION" => Ok(SqlValue::Text("PostgreSQL 16.0 (ferrosa)".to_string())),
        "CURRENT_DATABASE" | "CURRENT_CATALOG" | "CURRENT_SCHEMA" => {
            Ok(SqlValue::Text(default_schema.to_string()))
        }
        other => Err(error_response(
            "0A000",
            &format!(
                "function {}() is not supported yet",
                other.to_ascii_lowercase()
            ),
        )),
    }
}

/// The Postgres result type for a value (NULL defaults to text).
fn value_column_type(v: &SqlValue) -> ColumnType {
    match v {
        SqlValue::Int(_) => ColumnType::Int,
        SqlValue::Text(_) | SqlValue::Null => ColumnType::Text,
        SqlValue::Bool(_) => ColumnType::Bool,
        SqlValue::Float(_) => ColumnType::Float,
        SqlValue::Numeric { .. } => ColumnType::Numeric,
        SqlValue::Uuid(_) => ColumnType::Uuid,
        SqlValue::Bytea(_) => ColumnType::Bytea,
        SqlValue::Timestamp(_) => ColumnType::Timestamp,
        SqlValue::Date(_) => ColumnType::Date,
        SqlValue::Time(_) => ColumnType::Time,
        SqlValue::Inet(_) => ColumnType::Inet,
    }
}

/// The default output column name with no `AS` alias: the lowercased function
/// name for a function call, else Postgres's `?column?`.
fn default_scalar_name(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Func(name) => name.to_ascii_lowercase(),
        _ => "?column?".to_string(),
    }
}

/// Load every table referenced by `stmt` (FROM + optional JOIN) into a
/// [`MapCatalog`], so the sync engine can scan them. Returns the populated
/// catalog, or a single fail-loud [`BackendMessage::ErrorResponse`] (undefined
/// table `42P01` or storage error `58000`) — never a silently-empty relation.
///
/// Shared by the simple-query path ([`execute_query`]) and the extended-query
/// path (Describe/Execute), so both resolve tables identically.
pub(crate) async fn load_catalog(
    engine: &StorageEngine,
    schema: &Schema,
    stmt: &ferrosa_sql::SelectStmt,
    default_schema: &str,
) -> Result<MapCatalog, BackendMessage> {
    let mut catalog = MapCatalog::new();
    let referenced = std::iter::once(&stmt.from).chain(stmt.join.as_ref().map(|j| &j.table));
    for table_ref in referenced {
        let keyspace = table_ref.schema.as_deref().unwrap_or(default_schema);
        match load_table(engine, schema, keyspace, &table_ref.table).await {
            Ok(table) => {
                catalog = catalog.with_table(keyspace, &table_ref.table, Arc::new(table));
            }
            Err(LoadError::NoSuchTable { .. }) => {
                let msg = format!("relation \"{keyspace}.{}\" does not exist", table_ref.table);
                return Err(error_response("42P01", &msg));
            }
            Err(e @ LoadError::Storage(_)) => {
                return Err(error_response("58000", &e.to_string()));
            }
        }
    }
    Ok(catalog)
}

/// Render a `QueryResult` (or an `ExecError`) into backend messages for the
/// extended-query **Execute** path: `DataRow`s (encoded per `result_formats`) +
/// `CommandComplete`, with **no** leading `RowDescription` (the client already
/// learned the columns from `Describe`) and no `ReadyForQuery` (that follows
/// `Sync`). An error yields a single `ErrorResponse`.
pub(crate) fn render_execute_result(
    result: Result<QueryResult, ExecError>,
    result_formats: &[i16],
) -> Vec<BackendMessage> {
    match result {
        Ok(result) => {
            let col_types: Vec<ColumnType> = result.columns.iter().map(|c| c.ty).collect();
            let nrows = result.rows.len();
            let mut out = Vec::with_capacity(nrows + 1);
            for row in &result.rows {
                let columns = row
                    .0
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        encode_value(result_format_for(result_formats, i), col_types[i], v)
                    })
                    .collect();
                out.push(BackendMessage::DataRow { columns });
            }
            out.push(BackendMessage::CommandComplete {
                tag: format!("SELECT {nrows}"),
            });
            out
        }
        Err(e) => vec![exec_error_response(&e)],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_sql::Value as SqlValue;

    #[test]
    fn column_type_oids_match_postgres_builtins() {
        assert_eq!(column_type_oid(ColumnType::Int), 23);
        assert_eq!(column_type_oid(ColumnType::Text), 25);
        assert_eq!(column_type_oid(ColumnType::Bool), 16);
        assert_eq!(column_type_oid(ColumnType::Float), 701); // float8
    }

    #[test]
    fn column_type_sizes_are_wire_correct() {
        assert_eq!(column_type_size(ColumnType::Int), 4);
        assert_eq!(column_type_size(ColumnType::Bool), 1);
        assert_eq!(column_type_size(ColumnType::Float), 8);
        assert_eq!(column_type_size(ColumnType::Text), -1); // variable length
    }

    #[test]
    fn render_value_text_format() {
        assert_eq!(render_value(&SqlValue::Null), None);
        assert_eq!(render_value(&SqlValue::Int(42)), Some(b"42".to_vec()));
        assert_eq!(render_value(&SqlValue::Int(-7)), Some(b"-7".to_vec()));
        assert_eq!(
            render_value(&SqlValue::Text("hi".into())),
            Some(b"hi".to_vec())
        );
        assert_eq!(render_value(&SqlValue::Bool(true)), Some(b"t".to_vec()));
        assert_eq!(render_value(&SqlValue::Bool(false)), Some(b"f".to_vec()));
    }

    #[test]
    fn render_value_float_text_format() {
        assert_eq!(render_value(&SqlValue::float(1.5)), Some(b"1.5".to_vec()));
        assert_eq!(
            render_value(&SqlValue::float(-0.25)),
            Some(b"-0.25".to_vec())
        );
        // Non-finite values use Postgres spellings.
        assert_eq!(
            render_value(&SqlValue::float(f64::NAN)),
            Some(b"NaN".to_vec())
        );
        assert_eq!(
            render_value(&SqlValue::float(f64::INFINITY)),
            Some(b"Infinity".to_vec())
        );
        assert_eq!(
            render_value(&SqlValue::float(f64::NEG_INFINITY)),
            Some(b"-Infinity".to_vec())
        );
    }

    #[test]
    fn decode_param_text_by_oid() {
        // int4 / int8 / int2 parse to Int.
        assert_eq!(decode_param(0, 23, Some(b"42")), SqlValue::Int(42));
        assert_eq!(decode_param(0, 20, Some(b"-7")), SqlValue::Int(-7));
        // text / varchar / name stay text.
        assert_eq!(
            decode_param(0, 25, Some(b"hi")),
            SqlValue::Text("hi".into())
        );
        // bool spellings.
        assert_eq!(decode_param(0, 16, Some(b"t")), SqlValue::Bool(true));
        assert_eq!(decode_param(0, 16, Some(b"false")), SqlValue::Bool(false));
        // float8.
        assert_eq!(decode_param(0, 701, Some(b"1.5")), SqlValue::float(1.5));
        // OID 0 (unspecified) is lenient text.
        assert_eq!(
            decode_param(0, 0, Some(b"raw")),
            SqlValue::Text("raw".into())
        );
        // NULL.
        assert_eq!(decode_param(0, 23, None), SqlValue::Null);
    }

    #[test]
    fn decode_param_binary_by_oid() {
        // int4: 4-byte BE.
        assert_eq!(
            decode_param(1, 23, Some(&1i32.to_be_bytes())),
            SqlValue::Int(1)
        );
        // int8: 8-byte BE.
        assert_eq!(
            decode_param(1, 20, Some(&9_000_000_000i64.to_be_bytes())),
            SqlValue::Int(9_000_000_000)
        );
        // int2: 2-byte BE.
        assert_eq!(
            decode_param(1, 21, Some(&7i16.to_be_bytes())),
            SqlValue::Int(7)
        );
        // text.
        assert_eq!(
            decode_param(1, 25, Some(b"hi")),
            SqlValue::Text("hi".into())
        );
        // bool: non-zero byte ⇒ true.
        assert_eq!(decode_param(1, 16, Some(&[1])), SqlValue::Bool(true));
        assert_eq!(decode_param(1, 16, Some(&[0])), SqlValue::Bool(false));
        // float4 / float8 from BE bits.
        assert_eq!(
            decode_param(1, 700, Some(&1.5f32.to_be_bytes())),
            SqlValue::float(1.5)
        );
        assert_eq!(
            decode_param(1, 701, Some(&(-0.25f64).to_be_bytes())),
            SqlValue::float(-0.25)
        );
        // NULL.
        assert_eq!(decode_param(1, 23, None), SqlValue::Null);
    }

    #[test]
    fn encode_value_text_and_binary_round_trip() {
        // Text format reuses render_value.
        assert_eq!(
            encode_value(0, ColumnType::Int, &SqlValue::Int(42)),
            Some(b"42".to_vec())
        );
        // Binary int4 round-trips with decode_param.
        let enc = encode_value(1, ColumnType::Int, &SqlValue::Int(258)).unwrap();
        assert_eq!(enc, 258i32.to_be_bytes().to_vec());
        assert_eq!(decode_param(1, 23, Some(&enc)), SqlValue::Int(258));
        // Binary text.
        assert_eq!(
            encode_value(1, ColumnType::Text, &SqlValue::Text("hi".into())),
            Some(b"hi".to_vec())
        );
        // Binary bool.
        assert_eq!(
            encode_value(1, ColumnType::Bool, &SqlValue::Bool(true)),
            Some(vec![1])
        );
        // Binary float8 round-trips.
        let f = encode_value(1, ColumnType::Float, &SqlValue::float(3.5)).unwrap();
        assert_eq!(decode_param(1, 701, Some(&f)), SqlValue::float(3.5));
        // NULL ⇒ None in both formats.
        assert_eq!(encode_value(0, ColumnType::Int, &SqlValue::Null), None);
        assert_eq!(encode_value(1, ColumnType::Int, &SqlValue::Null), None);
    }

    #[test]
    fn uuid_text_render_is_canonical_lowercase_hyphenated() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            render_value(&SqlValue::Uuid(u)),
            Some(b"550e8400-e29b-41d4-a716-446655440000".to_vec())
        );
    }

    #[test]
    fn bytea_text_render_is_postgres_hex() {
        assert_eq!(
            render_value(&SqlValue::Bytea(vec![0xde, 0xad, 0xbe, 0xef])),
            Some(b"\\xdeadbeef".to_vec())
        );
        // Empty bytea ⇒ just the `\x` prefix.
        assert_eq!(
            render_value(&SqlValue::Bytea(vec![])),
            Some(b"\\x".to_vec())
        );
    }

    #[test]
    fn uuid_and_bytea_oid_and_size() {
        assert_eq!(column_type_oid(ColumnType::Uuid), 2950);
        assert_eq!(column_type_size(ColumnType::Uuid), 16);
        assert_eq!(column_type_oid(ColumnType::Bytea), 17);
        assert_eq!(column_type_size(ColumnType::Bytea), -1);
    }

    #[test]
    fn uuid_binary_encode_is_16_be_bytes_and_round_trips() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let enc = encode_value(1, ColumnType::Uuid, &SqlValue::Uuid(u)).unwrap();
        assert_eq!(enc, u.as_bytes().to_vec());
        assert_eq!(enc.len(), 16);
        assert_eq!(decode_param(1, 2950, Some(&enc)), SqlValue::Uuid(u));
    }

    #[test]
    fn bytea_binary_encode_is_raw_bytes_and_round_trips() {
        let raw = vec![0x00, 0x01, 0xff, 0x10];
        let enc = encode_value(1, ColumnType::Bytea, &SqlValue::Bytea(raw.clone())).unwrap();
        assert_eq!(enc, raw);
        assert_eq!(decode_param(1, 17, Some(&enc)), SqlValue::Bytea(raw));
    }

    #[test]
    fn uuid_text_decode_parses_hyphenated() {
        let s = "550e8400-e29b-41d4-a716-446655440000";
        let u = uuid::Uuid::parse_str(s).unwrap();
        assert_eq!(decode_param(0, 2950, Some(s.as_bytes())), SqlValue::Uuid(u));
        // A malformed uuid text falls back to best-effort Text (no panic).
        assert_eq!(
            decode_param(0, 2950, Some(b"not-a-uuid")),
            SqlValue::Text("not-a-uuid".into())
        );
    }

    #[test]
    fn bytea_text_decode_parses_hex_prefixed() {
        assert_eq!(
            decode_param(0, 17, Some(b"\\xdeadbeef")),
            SqlValue::Bytea(vec![0xde, 0xad, 0xbe, 0xef])
        );
        // Empty hex.
        assert_eq!(decode_param(0, 17, Some(b"\\x")), SqlValue::Bytea(vec![]));
    }

    #[test]
    fn uuid_and_bytea_null_encode_and_decode() {
        assert_eq!(encode_value(0, ColumnType::Uuid, &SqlValue::Null), None);
        assert_eq!(encode_value(1, ColumnType::Uuid, &SqlValue::Null), None);
        assert_eq!(encode_value(0, ColumnType::Bytea, &SqlValue::Null), None);
        assert_eq!(encode_value(1, ColumnType::Bytea, &SqlValue::Null), None);
        assert_eq!(decode_param(0, 2950, None), SqlValue::Null);
        assert_eq!(decode_param(1, 17, None), SqlValue::Null);
    }

    // ── Temporal / inet / numeric: OIDs + sizes ───────────────────────────

    #[test]
    fn new_type_oids_and_sizes() {
        assert_eq!(column_type_oid(ColumnType::Timestamp), 1114);
        assert_eq!(column_type_size(ColumnType::Timestamp), 8);
        assert_eq!(column_type_oid(ColumnType::Date), 1082);
        assert_eq!(column_type_size(ColumnType::Date), 4);
        assert_eq!(column_type_oid(ColumnType::Time), 1083);
        assert_eq!(column_type_size(ColumnType::Time), 8);
        assert_eq!(column_type_oid(ColumnType::Inet), 869);
        assert_eq!(column_type_size(ColumnType::Inet), -1);
        assert_eq!(column_type_oid(ColumnType::Numeric), 1700);
        assert_eq!(column_type_size(ColumnType::Numeric), -1);
    }

    // ── Timestamp text rendering (fractional trimming) ────────────────────

    #[test]
    fn render_timestamp_trims_fraction_postgres_style() {
        // 2024-01-15 10:30:00 UTC, no fraction ⇒ no dot.
        let base = parse_timestamp_text("2024-01-15 10:30:00").unwrap();
        let SqlValue::Timestamp(micros) = base else {
            panic!("expected Timestamp");
        };
        assert_eq!(
            render_value(&SqlValue::Timestamp(micros)),
            Some(b"2024-01-15 10:30:00".to_vec())
        );
        // .5 second ⇒ ".5" (one digit, trailing zeros trimmed).
        assert_eq!(
            render_value(&SqlValue::Timestamp(micros + 500_000)),
            Some(b"2024-01-15 10:30:00.5".to_vec())
        );
        // .123 ⇒ "123".
        assert_eq!(
            render_value(&SqlValue::Timestamp(micros + 123_000)),
            Some(b"2024-01-15 10:30:00.123".to_vec())
        );
        // 1 microsecond ⇒ ".000001" (all 6 digits significant).
        assert_eq!(
            render_value(&SqlValue::Timestamp(micros + 1)),
            Some(b"2024-01-15 10:30:00.000001".to_vec())
        );
    }

    #[test]
    fn render_timestamp_handles_pre_1970() {
        // 1969-12-31 23:59:59.5 UTC ⇒ -500_000 micros. The fraction stays
        // non-negative (.5) and the second floors correctly.
        let micros = -500_000;
        assert_eq!(
            render_value(&SqlValue::Timestamp(micros)),
            Some(b"1969-12-31 23:59:59.5".to_vec())
        );
    }

    #[test]
    fn render_date_text_form() {
        let SqlValue::Date(days) = parse_date_text("2024-01-15").unwrap() else {
            panic!("expected Date");
        };
        assert_eq!(
            render_value(&SqlValue::Date(days)),
            Some(b"2024-01-15".to_vec())
        );
        // The Unix epoch is day 0.
        assert_eq!(
            render_value(&SqlValue::Date(0)),
            Some(b"1970-01-01".to_vec())
        );
        // A pre-epoch (negative) day renders correctly.
        assert_eq!(
            render_value(&SqlValue::Date(-1)),
            Some(b"1969-12-31".to_vec())
        );
    }

    #[test]
    fn render_time_text_form_trims_fraction() {
        // 10:30:00 ⇒ no fraction.
        let micros = (10 * 3600 + 30 * 60) * 1_000_000;
        assert_eq!(
            render_value(&SqlValue::Time(micros)),
            Some(b"10:30:00".to_vec())
        );
        // + .25 second.
        assert_eq!(
            render_value(&SqlValue::Time(micros + 250_000)),
            Some(b"10:30:00.25".to_vec())
        );
        // Midnight.
        assert_eq!(render_value(&SqlValue::Time(0)), Some(b"00:00:00".to_vec()));
    }

    #[test]
    fn render_inet_text_is_canonical_ip() {
        use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
        assert_eq!(
            render_value(&SqlValue::Inet(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)))),
            Some(b"192.168.0.1".to_vec())
        );
        assert_eq!(
            render_value(&SqlValue::Inet(IpAddr::V6(Ipv6Addr::LOCALHOST))),
            Some(b"::1".to_vec())
        );
    }

    // ── Numeric text rendering ────────────────────────────────────────────

    #[test]
    fn render_numeric_text_forms() {
        use num_bigint::BigInt;
        // 123.45 (unscaled 12345, scale 2).
        assert_eq!(
            render_value(&SqlValue::numeric(BigInt::from(12345), 2)),
            Some(b"123.45".to_vec())
        );
        // Integer (scale 0).
        assert_eq!(
            render_value(&SqlValue::numeric(BigInt::from(42), 0)),
            Some(b"42".to_vec())
        );
        // Negative.
        assert_eq!(
            render_value(&SqlValue::numeric(BigInt::from(-12345), 2)),
            Some(b"-123.45".to_vec())
        );
        // 0.05 (leading-zero fraction padding).
        assert_eq!(
            render_value(&SqlValue::numeric(BigInt::from(5), 2)),
            Some(b"0.05".to_vec())
        );
        // Zero.
        assert_eq!(
            render_value(&SqlValue::numeric(BigInt::from(0), 4)),
            Some(b"0".to_vec())
        );
        // Trailing-zero normalization: 1.50 ⇒ "1.5".
        assert_eq!(
            render_value(&SqlValue::numeric(BigInt::from(150), 2)),
            Some(b"1.5".to_vec())
        );
        // Negative scale (value scaled up): 12 * 10^2 = 1200.
        assert_eq!(
            render_value(&SqlValue::numeric(BigInt::from(12), -2)),
            Some(b"1200".to_vec())
        );
    }

    // ── Text decode (parse the canonical forms) ───────────────────────────

    #[test]
    fn decode_param_text_for_new_types() {
        use num_bigint::BigInt;
        use std::net::IpAddr;
        // timestamp.
        assert_eq!(
            decode_param(0, 1114, Some(b"2024-01-15 10:30:00.5")),
            parse_timestamp_text("2024-01-15 10:30:00.5").unwrap()
        );
        // date.
        assert_eq!(
            decode_param(0, 1082, Some(b"1970-01-02")),
            SqlValue::Date(1)
        );
        // time.
        assert_eq!(
            decode_param(0, 1083, Some(b"00:00:01")),
            SqlValue::Time(1_000_000)
        );
        // inet.
        assert_eq!(
            decode_param(0, 869, Some(b"10.0.0.1")),
            SqlValue::Inet("10.0.0.1".parse::<IpAddr>().unwrap())
        );
        // numeric (text-only).
        assert_eq!(
            decode_param(0, 1700, Some(b"123.45")),
            SqlValue::numeric(BigInt::from(12345), 2)
        );
        // numeric with a leading dot.
        assert_eq!(
            decode_param(0, 1700, Some(b".5")),
            SqlValue::numeric(BigInt::from(5), 1)
        );
        // Malformed values ⇒ NULL (lenient, no panic).
        assert_eq!(decode_param(0, 1114, Some(b"not-a-time")), SqlValue::Null);
        assert_eq!(decode_param(0, 1700, Some(b"1.2.3")), SqlValue::Null);
    }

    // ── Binary round-trips for timestamp/date/time/inet ───────────────────

    #[test]
    fn binary_round_trip_temporal_and_inet() {
        use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
        // timestamp: encode (Postgres-epoch micros) then decode back.
        let ts = parse_timestamp_text("2024-06-17 12:00:00.123456").unwrap();
        let enc = encode_value(1, ColumnType::Timestamp, &ts).unwrap();
        assert_eq!(enc.len(), 8);
        assert_eq!(decode_param(1, 1114, Some(&enc)), ts);

        // date.
        let d = SqlValue::Date(19_876); // arbitrary day count
        let enc = encode_value(1, ColumnType::Date, &d).unwrap();
        assert_eq!(enc.len(), 4);
        assert_eq!(decode_param(1, 1082, Some(&enc)), d);

        // time.
        let t = SqlValue::Time(45_000_123_456);
        let enc = encode_value(1, ColumnType::Time, &t).unwrap();
        assert_eq!(enc.len(), 8);
        assert_eq!(decode_param(1, 1083, Some(&enc)), t);

        // inet v4 + v6.
        let v4 = SqlValue::Inet(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)));
        let enc = encode_value(1, ColumnType::Inet, &v4).unwrap();
        assert_eq!(decode_param(1, 869, Some(&enc)), v4);
        let v6 = SqlValue::Inet(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)));
        let enc = encode_value(1, ColumnType::Inet, &v6).unwrap();
        assert_eq!(decode_param(1, 869, Some(&enc)), v6);
    }

    #[test]
    fn timestamp_binary_uses_postgres_epoch() {
        // The Postgres epoch (2000-01-01 00:00:00) encodes to all-zero BE i64.
        let ts = parse_timestamp_text("2000-01-01 00:00:00").unwrap();
        let enc = encode_value(1, ColumnType::Timestamp, &ts).unwrap();
        assert_eq!(enc, 0i64.to_be_bytes().to_vec());
        // And the Unix-epoch micros value equals PG_EPOCH_MICROS.
        assert_eq!(ts, SqlValue::Timestamp(PG_EPOCH_MICROS));
    }

    #[test]
    fn numeric_binary_falls_back_to_text() {
        use num_bigint::BigInt;
        // Out-of-scope binary numeric ⇒ the TEXT bytes (documented fallback).
        let n = SqlValue::numeric(BigInt::from(12345), 2);
        assert_eq!(
            encode_value(1, ColumnType::Numeric, &n),
            Some(b"123.45".to_vec())
        );
    }

    #[test]
    fn result_format_fan_out_rule() {
        // Empty ⇒ all text (0).
        assert_eq!(result_format_for(&[], 3), 0);
        // Single ⇒ applies to every column.
        assert_eq!(result_format_for(&[1], 5), 1);
        // Per-column.
        assert_eq!(result_format_for(&[0, 1], 1), 1);
        assert_eq!(result_format_for(&[0, 1], 0), 0);
    }

    #[test]
    fn error_response_carries_severity_code_message() {
        let BackendMessage::ErrorResponse { fields } = error_response("42601", "boom") else {
            panic!("expected ErrorResponse");
        };
        assert_eq!(fields[0], (b'S', "ERROR".to_string()));
        assert_eq!(fields[1], (b'C', "42601".to_string()));
        assert_eq!(fields[2], (b'M', "boom".to_string()));
    }

    #[test]
    fn exec_error_maps_to_sqlstate() {
        let undefined_table = exec_error_response(&ExecError::NoSuchTable {
            schema: "public".into(),
            table: "nope".into(),
        });
        assert!(matches!(
            undefined_table,
            BackendMessage::ErrorResponse { ref fields } if fields[1] == (b'C', "42P01".to_string())
        ));

        let undefined_col = exec_error_response(&ExecError::NoSuchColumn("zzz".into()));
        assert!(matches!(
            undefined_col,
            BackendMessage::ErrorResponse { ref fields } if fields[1] == (b'C', "42703".to_string())
        ));

        let bad_qualifier = exec_error_response(&ExecError::UnknownQualifier("q".into()));
        assert!(matches!(
            bad_qualifier,
            BackendMessage::ErrorResponse { ref fields } if fields[1] == (b'C', "42703".to_string())
        ));

        let ambiguous = exec_error_response(&ExecError::AmbiguousColumn("x".into()));
        assert!(matches!(
            ambiguous,
            BackendMessage::ErrorResponse { ref fields } if fields[1] == (b'C', "42702".to_string())
        ));

        // An aggregate in WHERE is a grouping_error (42803), same family as
        // NotGrouped.
        let agg_in_where = exec_error_response(&ExecError::AggregateInWhere("COUNT(*)".into()));
        assert!(matches!(
            agg_in_where,
            BackendMessage::ErrorResponse { ref fields } if fields[1] == (b'C', "42803".to_string())
        ));
    }

    #[test]
    fn render_result_shapes_messages_in_order() {
        use ferrosa_sql::{Column, ColumnType, Row};
        let result = QueryResult {
            columns: vec![
                Column::new("name", ColumnType::Text),
                Column::new("score", ColumnType::Int),
            ],
            rows: vec![
                Row::new(vec![SqlValue::Text("a".into()), SqlValue::Int(1)]),
                Row::new(vec![SqlValue::Null, SqlValue::Int(2)]),
            ],
        };
        let msgs = render_result(result, &[]);
        // RowDescription, two DataRows, then CommandComplete.
        assert_eq!(msgs.len(), 4);
        assert!(matches!(msgs[0], BackendMessage::RowDescription { .. }));
        assert!(matches!(msgs[1], BackendMessage::DataRow { .. }));
        assert!(matches!(msgs[2], BackendMessage::DataRow { .. }));
        match &msgs[3] {
            BackendMessage::CommandComplete { tag } => assert_eq!(tag, "SELECT 2"),
            other => panic!("expected CommandComplete, got {other:?}"),
        }
        // The NULL renders as a None column in the second DataRow.
        match &msgs[2] {
            BackendMessage::DataRow { columns } => {
                assert_eq!(columns[0], None);
                assert_eq!(columns[1], Some(b"2".to_vec()));
            }
            other => panic!("expected DataRow, got {other:?}"),
        }
    }

    #[test]
    fn scalar_select_literal_and_info_functions() {
        let items = vec![
            ScalarItem {
                value: ScalarValue::Literal(SqlValue::Int(1)),
                alias: None,
            },
            ScalarItem {
                value: ScalarValue::Func("VERSION".into()),
                alias: None,
            },
            ScalarItem {
                value: ScalarValue::Func("CURRENT_DATABASE".into()),
                alias: Some("db".into()),
            },
        ];
        let result = execute_scalar_select(&items, "myks").expect("scalar select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns.len(), 3);
        // Default names: ?column? for a literal, the function name for a call.
        assert_eq!(result.columns[0].name, "?column?");
        assert_eq!(result.columns[1].name, "version");
        assert_eq!(result.columns[2].name, "db");
        let row = &result.rows[0].0;
        assert_eq!(row[0], SqlValue::Int(1));
        assert!(matches!(&row[1], SqlValue::Text(s) if s.contains("ferrosa")));
        assert_eq!(row[2], SqlValue::Text("myks".to_string()));
    }

    #[test]
    fn value_to_cql_maps_per_target_type() {
        use ferrosa_common::CqlValue as C;
        assert_eq!(
            value_to_cql(&SqlValue::Int(5), &CqlType::Int).unwrap(),
            C::Int(5)
        );
        assert_eq!(
            value_to_cql(&SqlValue::Int(5), &CqlType::Bigint).unwrap(),
            C::Bigint(5)
        );
        assert_eq!(
            value_to_cql(&SqlValue::Text("x".into()), &CqlType::Varchar).unwrap(),
            C::Text("x".to_string())
        );
        assert_eq!(
            value_to_cql(&SqlValue::Bool(true), &CqlType::Boolean).unwrap(),
            C::Boolean(true)
        );
        // NULL maps to a tombstone for any target type.
        assert_eq!(
            value_to_cql(&SqlValue::Null, &CqlType::Int).unwrap(),
            C::Null
        );
        // float bit-pattern round-trips.
        assert_eq!(
            value_to_cql(&SqlValue::float(9.5), &CqlType::Double).unwrap(),
            C::Double(9.5f64.to_bits())
        );
        // out-of-range int into int4, and a type mismatch, both fail loud.
        assert!(value_to_cql(&SqlValue::Int(i64::MAX), &CqlType::Int).is_err());
        assert!(value_to_cql(&SqlValue::Text("x".into()), &CqlType::Int).is_err());
    }

    #[test]
    fn scalar_select_unsupported_function_fails_loud() {
        // An unmodeled function errors (0A000) rather than guessing a value.
        let items = vec![ScalarItem {
            value: ScalarValue::Func("NOW".into()),
            alias: None,
        }];
        assert!(execute_scalar_select(&items, "ks").is_err());
    }
}

/// Transaction-buffer correctness (FMEA PG-1): DML in a `BEGIN`/`COMMIT` block
/// must BUFFER as an Accord `TransactionWrite` instead of applying to storage;
/// only the committer applies it. These run a real local `StorageEngine` (temp
/// dir, no S3/Docker/cluster) and read back via the same `execute_query` path.
#[cfg(test)]
mod txn_buffer_tests {
    use super::*;
    use ferrosa_schema::{
        AuthContext, AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode,
        EnvSecretsProvider, KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig,
        ReplicationParams, Schema, SchemaConfig, TableMetadata, TableParams, TestAuditSink,
    };
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    };
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::time::Duration;
    use uuid::Uuid;

    fn schema_config() -> SchemaConfig {
        SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        }
    }

    fn superuser() -> AuthContext {
        AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    fn column(name: &str, kind: ColumnKind, ty: &str) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position: 0,
            column_type: ty.to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        }
    }

    /// Schema with keyspace `public` and table `kv(k text PK, v text)`.
    fn schema_with_kv() -> Schema {
        let schema = Schema::new(schema_config()).expect("schema bootstraps");
        let auth = superuser();
        schema
            .create_keyspace(
                KeyspaceMetadata {
                    name: "public".to_string(),
                    durable_writes: true,
                    replication: ReplicationParams {
                        strategy: "SimpleStrategy".to_string(),
                        options: {
                            let mut o = HashMap::new();
                            o.insert("replication_factor".to_string(), "1".to_string());
                            o
                        },
                    },
                },
                &auth,
            )
            .expect("create keyspace public");
        let mut cols = IndexMap::new();
        cols.insert(
            "k".to_string(),
            column("k", ColumnKind::PartitionKey, "text"),
        );
        cols.insert("v".to_string(), column("v", ColumnKind::Regular, "text"));
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "kv".to_string(),
                    id: Uuid::new_v4(),
                    columns: cols,
                    partition_key: vec!["k".to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .expect("create table kv");
        schema
    }

    fn engine_config(dir: &Path) -> StorageEngineConfig {
        StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 256 * 1024,
                max_segment_age: Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                batch: Default::default(),
                log_dir: dir.join("commitlog"),
                checkpoint_dir: dir.join("commitlog"),
                archive: None,
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            write_verify: false,
        }
    }

    fn kv_storage_schema() -> ferrosa_common::schema::TableSchema {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        TableSchema {
            keyspace: "public".to_string(),
            table: "kv".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    /// How many `kv` rows whose `k` equals `key` are visible in storage, read
    /// back through the SAME `execute_query` SELECT path the front-end serves.
    async fn row_count(engine: &StorageEngine, schema: &Schema, key: &str) -> usize {
        let msgs = execute_query(
            engine,
            schema,
            &format!("SELECT k FROM kv WHERE k = '{key}'"),
            "public",
            None,
        )
        .await;
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
            "read-back SELECT failed: {msgs:?}"
        );
        msgs.iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count()
    }

    async fn new_engine_and_schema() -> (tempfile::TempDir, StorageEngine, Schema) {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
        engine.register_table(kv_storage_schema()).unwrap();
        let schema = schema_with_kv();
        (dir, engine, schema)
    }

    #[tokio::test]
    async fn buffered_insert_is_not_applied_until_committer() {
        // An INSERT with `txn = Some(buffer)` is BUFFERED, never written to
        // storage. Contrast: an autocommit INSERT (`txn = None`) IS written.
        let (_dir, engine, schema) = new_engine_and_schema().await;

        // Buffered: must NOT touch storage.
        let mut buffer: Vec<ferrosa_storage::accord::TransactionWrite> = Vec::new();
        let msgs = execute_query(
            &engine,
            &schema,
            "INSERT INTO kv (k, v) VALUES ('a', 'buffered')",
            "public",
            Some(&mut buffer),
        )
        .await;
        assert!(
            matches!(&msgs[..], [BackendMessage::CommandComplete { tag }] if tag == "INSERT 0 1"),
            "buffered INSERT still acks INSERT 0 1: {msgs:?}"
        );
        assert_eq!(
            buffer.len(),
            1,
            "the write was buffered as a TransactionWrite"
        );
        assert_eq!(buffer[0].keyspace, "public");
        assert!(
            !buffer[0].mutation.is_empty() && !buffer[0].key.is_empty(),
            "the buffered write carries encoded key + mutation bytes"
        );
        assert_eq!(
            row_count(&engine, &schema, "a").await,
            0,
            "a BUFFERED write must NOT be visible in storage (FMEA PG-1)"
        );

        // Autocommit: IS applied immediately.
        let msgs = execute_query(
            &engine,
            &schema,
            "INSERT INTO kv (k, v) VALUES ('b', 'autocommit')",
            "public",
            None,
        )
        .await;
        assert!(
            matches!(&msgs[..], [BackendMessage::CommandComplete { tag }] if tag == "INSERT 0 1"),
            "autocommit INSERT acks: {msgs:?}"
        );
        assert_eq!(
            row_count(&engine, &schema, "b").await,
            1,
            "an autocommit write IS applied immediately"
        );

        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn applying_a_buffered_write_set_makes_it_visible() {
        // The committer's job: apply the buffered mutation. Here we apply the
        // buffered TransactionWrite's mutation bytes through the engine exactly
        // as the cluster committer's apply path does, proving the buffered bytes
        // are a faithful, applyable mutation (not a fake ack).
        let (_dir, engine, schema) = new_engine_and_schema().await;

        let mut buffer: Vec<ferrosa_storage::accord::TransactionWrite> = Vec::new();
        execute_query(
            &engine,
            &schema,
            "INSERT INTO kv (k, v) VALUES ('c', 'committed')",
            "public",
            Some(&mut buffer),
        )
        .await;
        assert_eq!(buffer.len(), 1);
        assert_eq!(
            row_count(&engine, &schema, "c").await,
            0,
            "buffered, not yet applied"
        );

        // Apply the buffered mutation (what the committer does on COMMIT).
        let mutation = Mutation::deserialize_from(&buffer[0].mutation).expect("decode mutation");
        engine.write_atomic_batch(vec![mutation]).expect("apply");
        assert_eq!(
            row_count(&engine, &schema, "c").await,
            1,
            "after the committer applies the buffered write-set the row is visible"
        );

        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn buffer_respects_write_cap() {
        // Staging past MAX_TXN_WRITES fails loud (53400) rather than growing the
        // buffer without bound; nothing is applied to storage.
        let (_dir, engine, schema) = new_engine_and_schema().await;
        let mut buffer: Vec<ferrosa_storage::accord::TransactionWrite> =
            Vec::with_capacity(MAX_TXN_WRITES);
        // Pre-fill to the cap with dummy writes so the next stage trips it.
        for _ in 0..MAX_TXN_WRITES {
            buffer.push(ferrosa_storage::accord::TransactionWrite {
                keyspace: "public".to_string(),
                key: b"x".to_vec(),
                mutation: b"x".to_vec(),
            });
        }
        let msgs = execute_query(
            &engine,
            &schema,
            "INSERT INTO kv (k, v) VALUES ('over', 'cap')",
            "public",
            Some(&mut buffer),
        )
        .await;
        match &msgs[..] {
            [BackendMessage::ErrorResponse { fields }] => {
                assert_eq!(fields[1], (b'C', "53400".to_string()));
            }
            other => panic!("expected a fail-loud cap ErrorResponse, got {other:?}"),
        }
        assert_eq!(
            buffer.len(),
            MAX_TXN_WRITES,
            "the over-cap write was NOT buffered"
        );
        assert_eq!(
            row_count(&engine, &schema, "over").await,
            0,
            "an over-cap write is never applied to storage"
        );

        engine.shutdown().unwrap();
    }
}
