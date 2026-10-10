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

use ferrosa_common::timeuuid::is_synthetic_key_column;
use ferrosa_common::{CqlType, CqlValue};
use ferrosa_schema::{ColumnKind, Schema};
use ferrosa_sql::{
    parse_statement, CastTarget, Column, ColumnType, DeleteStmt, ExecError, Expr, InsertStmt,
    MapCatalog, QueryResult, Returning, Row, ScalarItem, ScalarValue, SelectStmt, Statement, Term,
    UpdateStmt, Value as SqlValue,
};
use ferrosa_storage::{Mutation, StorageEngine};

use crate::messages::{BackendMessage, FieldDescription};
use crate::mvcc::{
    MvccCommitError, MvccManager, MvccSnapshot, PgWrite, RowChange, DEFAULT_MAX_TXN_WRITES,
};
use crate::result_stream::{open_stream, ResultStream};
use crate::storage_provider::TableCodec;
use crate::storage_provider::{load_table_with_overlay, LoadError, ScanFailure, SCAN_BUFFER_ROWS};
use crate::synthetic_key::next_synthetic_key;

/// The reserved PostgreSQL schema whose relations are projected from the live
/// schema, not read from storage (see [`crate::catalog`]).
const PG_CATALOG: &str = "pg_catalog";

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

/// The Postgres type OID for a relational [`ColumnType`], from the one
/// [`crate::pg_types`] map (`Float -> 701`, `Numeric -> 1700`, ...).
pub(crate) fn column_type_oid(ty: ColumnType) -> i32 {
    // Every minted OID is below 2^31 (asserted by the pg_types tests).
    i32::try_from(crate::pg_types::for_column_type(ty).oid).unwrap_or(i32::MAX)
}

/// Render a [`SqlValue`] to its Postgres **text-format** column bytes, or `None`
/// for SQL NULL (encoded on the wire as a `-1` length with no bytes).
///
/// Float uses Rust's default `{}` formatting (round-trippable shortest form) for
/// v1, with the non-finite cases mapped to Postgres's spellings: `NaN`,
/// `Infinity`, `-Infinity`. Exact float text-format parity with Postgres is
/// tracked as follow-up.
///
/// A value with no text rendering is a typed [`EncodeError`], never `None`: a
/// `None` here would reach the client as SQL NULL.
fn render_value(value: &SqlValue) -> Result<Option<Vec<u8>>, EncodeError> {
    let text = match value {
        SqlValue::Null => return Ok(None),
        SqlValue::Int(i) => i.to_string().into_bytes(),
        SqlValue::Text(s) => s.clone().into_bytes(),
        SqlValue::Bool(b) => {
            if *b {
                b"t".to_vec()
            } else {
                b"f".to_vec()
            }
        }
        SqlValue::Float(of) => render_float_text(of.0).into_bytes(),
        // `uuid::Uuid`'s Display is the canonical lowercase hyphenated form,
        // which is exactly Postgres's uuid text output.
        SqlValue::Uuid(u) => u.to_string().into_bytes(),
        // Postgres bytea text output (default `hex` format): `\x` followed by
        // lowercase hex of the bytes; empty bytea => just `\x`.
        SqlValue::Bytea(bytes) => bytea_hex_text(bytes),
        // Temporal / network / numeric: exact Postgres text forms.
        SqlValue::Timestamp(micros) => render_timestamp_text(*micros).into_bytes(),
        SqlValue::Date(days) => render_date_text(*days).into_bytes(),
        SqlValue::Time(micros) => render_time_text(*micros).into_bytes(),
        // `IpAddr`'s Display is the canonical IP string, exactly Postgres `inet`
        // text output for a plain host address.
        SqlValue::Inet(ip) => ip.to_string().into_bytes(),
        SqlValue::Numeric { unscaled, scale } => render_numeric_text(unscaled, *scale).into_bytes(),
        // PostgreSQL jsonb text (D26), or a typed error for a corrupt cell.
        SqlValue::Jsonb(doc) => crate::jsonb_wire::render_text(doc)?,
        SqlValue::JsonPath(_) | SqlValue::TextArray(_) => {
            return Err(EncodeError::unsupported(
                "result format for jsonpath and text[] values is not supported yet (T-161b)",
            ))
        }
    };
    Ok(Some(text))
}

/// Postgres text spelling of an `f64`: shortest round-trip form, with the
/// non-finite cases as `NaN`, `Infinity`, `-Infinity`.
fn render_float_text(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        }
    } else {
        format!("{f}")
    }
}

/// A value that cannot be encoded in the requested wire format: the SQLSTATE and
/// message of the `ErrorResponse` that replaces the row. Fail loud: the value is
/// never sent as NULL or as bytes a driver could misdecode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeError {
    /// SQLSTATE for the `ErrorResponse`.
    pub sqlstate: &'static str,
    /// Human-readable cause; never contains stored data.
    pub message: String,
}

impl EncodeError {
    /// A format the value has no encoding for (`0A000`).
    fn unsupported(message: &str) -> Self {
        Self {
            sqlstate: "0A000",
            message: message.to_string(),
        }
    }
}

/// A range or width failure (`22003`, numeric_value_out_of_range): the original
/// meaning of every string-typed encode error.
impl From<String> for EncodeError {
    fn from(message: String) -> Self {
        Self {
            sqlstate: "22003",
            message,
        }
    }
}

impl From<crate::jsonb_wire::WireError> for EncodeError {
    fn from(error: crate::jsonb_wire::WireError) -> Self {
        Self {
            sqlstate: error.sqlstate,
            message: error.message,
        }
    }
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.sqlstate, self.message)
    }
}

/// The `ErrorResponse` for an [`EncodeError`], carrying its own SQLSTATE.
pub(crate) fn encode_error_response(error: &EncodeError) -> BackendMessage {
    error_response(error.sqlstate, &error.message)
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

/// A parameter-decode failure: the SQLSTATE the `ErrorResponse` carries and a
/// message. It never echoes the parameter value (it may be a credential).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParamError {
    pub sqlstate: &'static str,
    pub message: String,
}

impl ParamError {
    fn new(sqlstate: &'static str, message: String) -> Self {
        Self { sqlstate, message }
    }

    /// Text-format value that does not parse as its declared type
    /// (`invalid_text_representation`, `22P02`).
    fn text(type_oid: i32, why: &str) -> Self {
        Self::new(
            "22P02",
            format!("invalid input syntax for parameter type OID {type_oid}: {why}"),
        )
    }

    /// Binary-format value that is malformed for its declared type
    /// (`invalid_binary_representation`, `22P03`).
    fn binary(type_oid: i32, why: &str) -> Self {
        Self::new(
            "22P03",
            format!("invalid binary representation for parameter type OID {type_oid}: {why}"),
        )
    }
}

/// Parameter type OIDs the decoder maps. Anything else (except `0`,
/// unspecified) is refused rather than decoded as text.
const SUPPORTED_PARAM_OIDS: [i32; 18] = [
    16, 17, 19, 20, 21, 23, 25, 114, 700, 701, 869, 1043, 1082, 1083, 1114, 1700, 2950, 3802,
];

/// `json` (D11: stored as jsonb) and `jsonb` parameter OIDs.
const PARAM_OID_JSON: i32 = 114;
const PARAM_OID_JSONB: i32 = 3802;

/// Decode one bound parameter into a [`SqlValue`], failing loud.
///
/// `format` is the Bind format code (`0` = text, `1` = binary); `type_oid` is
/// the declared type OID (`0` = unspecified, treated as UTF-8 text). `bytes`
/// is `None` for SQL NULL. A value that does not parse, a malformed binary
/// value, and an OID with no mapping are all errors: none becomes NULL or text.
///
/// `jsonb_limits` gates jsonb (3802) and json (114) input only (D14b): those
/// parameters are parsed here into a validated [`SqlValue::Jsonb`]. Text bound
/// to a jsonb column under any other OID is parsed later, in [`value_to_cql`].
pub(crate) fn decode_param_checked(
    format: i16,
    type_oid: i32,
    bytes: Option<&[u8]>,
    jsonb_limits: &ferrosa_jsonb::Limits,
) -> Result<SqlValue, ParamError> {
    let Some(raw) = bytes else {
        return Ok(SqlValue::Null);
    };
    if !matches!(format, 0 | 1) {
        return Err(ParamError::new(
            "08P01",
            format!("unsupported parameter format code {format}"),
        ));
    }
    if type_oid != 0 && !SUPPORTED_PARAM_OIDS.contains(&type_oid) {
        return Err(ParamError::new(
            "42704",
            format!("parameter type OID {type_oid} is not supported"),
        ));
    }
    if matches!(type_oid, PARAM_OID_JSON | PARAM_OID_JSONB) {
        return decode_param_jsonb(format, type_oid, raw, jsonb_limits);
    }
    if format == 1 {
        decode_param_binary(type_oid, raw)
    } else {
        decode_param_text(type_oid, raw)
    }
}

/// jsonb / json parameter: parse under the configured limits (D14b).
///
/// Text format is the JSON text. Binary `jsonb` is a `0x01` version byte then the
/// text (`jsonb_recv`); binary `json` is the bare text (`json_recv`). The
/// [`crate::jsonb_wire`] errors carry the SQLSTATE and never echo the input.
fn decode_param_jsonb(
    format: i16,
    type_oid: i32,
    raw: &[u8],
    limits: &ferrosa_jsonb::Limits,
) -> Result<SqlValue, ParamError> {
    use crate::jsonb_wire::{parse_binary_input, parse_text_input, InputEdge};
    let parsed = if format == 1 && type_oid == PARAM_OID_JSONB {
        parse_binary_input(raw, limits)
    } else {
        parse_text_input(raw, limits, InputEdge::Parameter)
    };
    parsed
        .map(SqlValue::Jsonb)
        .map_err(|e| ParamError::new(e.sqlstate, e.message))
}

/// Text-format parameter decode: parse the UTF-8 string per the declared OID.
fn decode_param_text(type_oid: i32, raw: &[u8]) -> Result<SqlValue, ParamError> {
    let s = std::str::from_utf8(raw)
        .map_err(|_| ParamError::text(type_oid, "value is not valid UTF-8"))?;
    let bad = |why: &str| ParamError::text(type_oid, why);
    match type_oid {
        // int4 / int8 / int2: decimal integer, range-checked per width.
        23 | 20 | 21 => {
            let n = s.trim().parse::<i64>().map_err(|_| bad("not an integer"))?;
            let fits = match type_oid {
                23 => i32::try_from(n).is_ok(),
                21 => i16::try_from(n).is_ok(),
                _ => true,
            };
            if fits {
                Ok(SqlValue::Int(n))
            } else {
                Err(bad("integer out of range"))
            }
        }
        // text / varchar / name; OID 0 (unspecified) is text too.
        0 | 25 | 1043 | 19 => Ok(SqlValue::Text(s.to_string())),
        16 => decode_bool_text(s).ok_or_else(|| bad("not a boolean")),
        700 | 701 => s
            .trim()
            .parse::<f64>()
            .map(SqlValue::float)
            .map_err(|_| bad("not a floating-point number")),
        2950 => uuid::Uuid::parse_str(s)
            .map(SqlValue::Uuid)
            .map_err(|_| bad("not a uuid")),
        // bytea: a `\x<hex>` string decodes to raw bytes.
        17 => decode_bytea_hex_text(s).ok_or_else(|| bad("not a hex-format bytea")),
        1114 => parse_timestamp_text(s).ok_or_else(|| bad("not a timestamp")),
        1082 => parse_date_text(s).ok_or_else(|| bad("not a date")),
        1083 => parse_time_text(s).ok_or_else(|| bad("not a time")),
        869 => s
            .parse::<std::net::IpAddr>()
            .map(SqlValue::Inet)
            .map_err(|_| bad("not an inet address")),
        // Numeric params are TEXT-only (see `decode_param_binary`).
        1700 => parse_numeric_text(s).ok_or_else(|| bad("not a numeric")),
        // Unreachable behind the `SUPPORTED_PARAM_OIDS` gate; refuse anyway.
        other => Err(ParamError::new(
            "42704",
            format!("parameter type OID {other} is not supported"),
        )),
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

/// Largest decimal exponent accepted in numeric text (`1e5`); a larger one
/// would materialise an absurd digit string when rendered.
const MAX_NUMERIC_EXPONENT: i32 = 10_000;

/// Parse a Postgres `numeric` text body (`[-]ddd[.ddd][e[+-]dd]`) into a
/// [`SqlValue::Numeric`]. Returns `None` for malformed input or an exponent
/// beyond [`MAX_NUMERIC_EXPONENT`].
fn parse_numeric_text(s: &str) -> Option<SqlValue> {
    let s = s.trim();
    let Some((mantissa, exp)) = s.split_once(['e', 'E']) else {
        return parse_numeric_mantissa(s, 0);
    };
    let exp = exp.parse::<i32>().ok()?;
    if exp.abs() > MAX_NUMERIC_EXPONENT {
        return None;
    }
    parse_numeric_mantissa(mantissa, exp)
}

/// Parse `[-]ddd[.ddd]` and apply a decimal exponent (`value * 10^exp`).
fn parse_numeric_mantissa(s: &str, exp: i32) -> Option<SqlValue> {
    use num_bigint::BigInt;
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
    let scale = i32::try_from(frac_part.len()).ok()?.checked_sub(exp)?;
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
    // `as_chunks::<2>()` gives `&[[u8; 2]]`, so the pair is indexed without a
    // bounds check the compiler cannot elide from a slice.
    let (pairs, _remainder) = chars.as_chunks::<2>();
    for pair in pairs {
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

/// Postgres bool text spellings the driver may send; `None` if unrecognised.
fn decode_bool_text(s: &str) -> Option<SqlValue> {
    match s.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "1" | "y" | "yes" | "on" => Some(SqlValue::Bool(true)),
        "f" | "false" | "0" | "n" | "no" | "off" => Some(SqlValue::Bool(false)),
        _ => None,
    }
}

/// Binary-format parameter decode: big-endian per the declared OID. Every
/// wrong length or out-of-range value is a `22P03`, never NULL.
fn decode_param_binary(type_oid: i32, raw: &[u8]) -> Result<SqlValue, ParamError> {
    let bad = |why: &str| ParamError::binary(type_oid, why);
    let int = |width: usize| be_int(raw, width).ok_or_else(|| bad("wrong length"));
    match type_oid {
        23 => int(4).map(SqlValue::Int),
        20 => int(8).map(SqlValue::Int),
        21 => int(2).map(SqlValue::Int),
        // text / varchar / name; OID 0 (unspecified) is text too.
        0 | 25 | 1043 | 19 => std::str::from_utf8(raw)
            .map(|s| SqlValue::Text(s.to_string()))
            .map_err(|_| bad("value is not valid UTF-8")),
        // bool: exactly one byte, any non-zero byte is true.
        16 => match raw {
            [b] => Ok(SqlValue::Bool(*b != 0)),
            _ => Err(bad("wrong length")),
        },
        // float4 (BE f32 bits) / float8 (BE f64 bits).
        700 => <[u8; 4]>::try_from(raw)
            .map(|b| SqlValue::float(f64::from(f32::from_be_bytes(b))))
            .map_err(|_| bad("wrong length")),
        701 => <[u8; 8]>::try_from(raw)
            .map(|b| SqlValue::float(f64::from_be_bytes(b)))
            .map_err(|_| bad("wrong length")),
        // uuid: 16 big-endian bytes.
        2950 => uuid::Uuid::from_slice(raw)
            .map(SqlValue::Uuid)
            .map_err(|_| bad("wrong length")),
        // bytea: the raw bytes, copied verbatim.
        17 => Ok(SqlValue::Bytea(raw.to_vec())),
        // timestamp (1114): BE i64 microseconds since the Postgres epoch
        // (2000-01-01). Shift to the Unix-epoch micros our `Value` carries.
        1114 => int(8)?
            .checked_add(PG_EPOCH_MICROS)
            .map(SqlValue::Timestamp)
            .ok_or_else(|| bad("timestamp out of range")),
        // date (1082): BE i32 days since the Postgres epoch (2000-01-01).
        1082 => {
            let pg_days = i32::try_from(int(4)?).map_err(|_| bad("wrong length"))?;
            pg_days
                .checked_add(PG_EPOCH_DAYS)
                .map(SqlValue::Date)
                .ok_or_else(|| bad("date out of range"))
        }
        // time (1083): BE i64 microseconds since midnight.
        1083 => int(8).map(SqlValue::Time),
        // inet (869): the Postgres inet binary (family, bits, is_cidr, len, addr).
        869 => decode_inet_binary(raw).ok_or_else(|| bad("malformed inet")),
        // Binary numeric params are out of scope (text-only).
        1700 => Err(ParamError::new(
            "0A000",
            "binary numeric parameters are not supported".to_string(),
        )),
        // Unreachable behind the `SUPPORTED_PARAM_OIDS` gate; refuse anyway.
        other => Err(ParamError::new(
            "42704",
            format!("parameter type OID {other} is not supported"),
        )),
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
/// `column_type_oid` and `pg_types` `typlen`: `ColumnType::Int` ⇒ int4 (OID 23,
/// 4 bytes), so an `Int` always emits a 4-byte big-endian `i32`; an out-of-range
/// value returns an error rather than being truncated. `Float` ⇒
/// float8 (OID 701, 8 bytes).
pub fn encode_value(
    format: i16,
    col_type: ColumnType,
    v: &SqlValue,
) -> Result<Option<Vec<u8>>, EncodeError> {
    if format != 1 {
        return render_value(v); // text format: reuse the existing renderer
    }
    Ok(match v {
        SqlValue::Null => None,
        SqlValue::Int(i) => Some(encode_int_binary(col_type, *i)?),
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
        SqlValue::Timestamp(micros) => Some(
            micros
                .checked_sub(PG_EPOCH_MICROS)
                .ok_or_else(|| "timestamp is out of PostgreSQL binary range".to_string())?
                .to_be_bytes()
                .to_vec(),
        ),
        // date (OID 1082): BE i32 days since the Postgres epoch.
        SqlValue::Date(days) => Some(
            days.checked_sub(PG_EPOCH_DAYS)
                .ok_or_else(|| "date is out of PostgreSQL binary range".to_string())?
                .to_be_bytes()
                .to_vec(),
        ),
        // time (OID 1083): BE i64 micros since midnight (same origin as our repr).
        SqlValue::Time(micros) => Some(micros.to_be_bytes().to_vec()),
        // inet (OID 869): the Postgres inet binary (family/bits/is_cidr/len/addr).
        SqlValue::Inet(ip) => Some(encode_inet_binary(ip)),
        // Sending text bytes under binary format violates PostgreSQL's wire
        // contract. Until numeric binary encoding is implemented, fail the
        // statement instead of returning data the driver may misdecode.
        SqlValue::Numeric { .. } => {
            return Err("binary result format for numeric is not supported"
                .to_string()
                .into());
        }
        // jsonb_send: version byte 0x01 then the PostgreSQL text form (D11, D26).
        SqlValue::Jsonb(doc) => Some(crate::jsonb_wire::render_binary(doc)?),
        SqlValue::JsonPath(_) | SqlValue::TextArray(_) => {
            return Err(EncodeError::unsupported(&format!(
                "no binary codec for {col_type:?} (T-161b)"
            )));
        }
    })
}

/// Binary integer encoding honoring the column's declared width. `Int` is int4;
/// `BigInt` is int8. Keeps bytes consistent with the RowDescription OID/size.
fn encode_int_binary(col_type: ColumnType, i: i64) -> Result<Vec<u8>, String> {
    match col_type {
        ColumnType::Int => i32::try_from(i)
            .map(|value| value.to_be_bytes().to_vec())
            .map_err(|_| format!("integer {i} is outside the PostgreSQL int4 range")),
        ColumnType::BigInt => Ok(i.to_be_bytes().to_vec()),
        _ => Ok(i.to_be_bytes().to_vec()),
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
        // A blocking operator could not read or write its spilled state. This
        // is an operator failure, not bad SQL, so it maps to Class 58
        // (system_error) — and it is reported rather than being papered over
        // with a short result the client could not distinguish from a complete
        // one (forge t_50d99192).
        ExecError::Spill(_) => ("58030", err.to_string()),
        // A `::` cast the front end failed to resolve before planning. The front
        // end resolves every cast, so this is a caller that drove the engine
        // directly with an unresolved cast; refused as an unsupported feature
        // rather than compared as if the cast did nothing.
        ExecError::UnresolvedCast(_) => ("0A000", err.to_string()),
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
            type_size: crate::pg_types::for_column_type(col.ty).typlen,
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
        let columns = match row
            .0
            .iter()
            .enumerate()
            .map(|(i, v)| encode_value(result_format_for(result_formats, i), col_types[i], v))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(columns) => columns,
            Err(error) => return vec![encode_error_response(&error)],
        };
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
    engine: &Arc<StorageEngine>,
    schema: &Schema,
    sql: &str,
    default_schema: &str,
    jsonb_limits: &ferrosa_jsonb::Limits,
    txn: Option<&mut Vec<PgWrite>>,
) -> Vec<BackendMessage> {
    let env = ReadEnv {
        engine,
        schema,
        default_schema,
        mvcc: None,
        snapshot: None,
        ddl: None,
        jsonb_limits,
    };
    execute_query_with_mvcc(env, sql, txn).await
}

/// The SQLSTATE a parse failure is reported with (FMEA PG-T132a-04).
///
/// Refused DDL clauses are `0A000` (feature not supported) and a missing key is
/// too: ferrosa refuses those by name rather than accept a fake constraint.
/// Definition errors carry their own class-42 codes.
pub(crate) fn parse_error_sqlstate(error: &ferrosa_sql::ParseError) -> &'static str {
    use ferrosa_sql::ParseError;
    match error {
        ParseError::UnsupportedClause(_)
        | ParseError::MissingPrimaryKey
        | ParseError::UnsupportedAlter(_)
        | ParseError::UnsupportedCopy(_)
        | ParseError::UnsupportedStorageParameter(_)
        | ParseError::UnsupportedSelectExpr(_)
        | ParseError::UnsupportedCast(_)
        | ParseError::UnsupportedCastExpr => "0A000",
        ParseError::UnknownType(_) => "42704",
        ParseError::DuplicateColumn(_) => "42701",
        ParseError::MultiplePrimaryKeys => "42P16",
        ParseError::UnknownPrimaryKeyColumn(_) => "42703",
        // PostgreSQL's statement_too_complex, as for max_stack_depth.
        ParseError::TooDeep => "54001",
        ParseError::Unexpected { .. } | ParseError::UnexpectedEnd | ParseError::BadToken(_) => {
            "42601"
        }
    }
}

/// Collecting form of [`execute_query_streaming`]: every message, rows included,
/// gathered into one `Vec`. For tests and tools that want the whole reply as a
/// value; the server never calls it, because gathering a `SELECT`'s rows here is
/// exactly the materialization the streaming path exists to avoid.
pub(crate) async fn execute_query_with_mvcc(
    env: ReadEnv<'_>,
    sql: &str,
    txn: Option<&mut Vec<PgWrite>>,
) -> Vec<BackendMessage> {
    let mut messages: Vec<BackendMessage> = Vec::new();
    let tail = execute_query_streaming(env, sql, txn, &mut messages).await;
    match tail {
        Ok(tail) => messages.extend(tail),
        Err(error) => messages.push(error_response(
            "58000",
            &format!("in-memory reply sink failed: {error}"),
        )),
    }
    messages
}

/// Execute one simple-query SQL string. A `SELECT` streams its `RowDescription`
/// and `DataRow`s to `out` as the executor yields them; the returned messages
/// are what remains to send after that (the `CommandComplete`, or the
/// `ErrorResponse` that ends a failed query). Everything else is bounded and is
/// returned whole.
///
/// # Errors
///
/// An I/O error from `out`: the client went away mid-stream.
pub(crate) async fn execute_query_streaming<O: ReplySink>(
    env: ReadEnv<'_>,
    sql: &str,
    txn: Option<&mut Vec<PgWrite>>,
    out: &mut O,
) -> std::io::Result<Vec<BackendMessage>> {
    let stmt = match parse_statement(sql) {
        Ok(stmt) => stmt,
        Err(e) => {
            return Ok(vec![error_response(
                parse_error_sqlstate(&e),
                &e.to_string(),
            )])
        }
    };
    let Statement::Select(select) = stmt else {
        return Ok(execute_statement(env, stmt, txn).await);
    };
    // Table query: load referenced tables (the R15 guard lives in
    // `load_table` — a missing table is `NoSuchTable`, never an empty scan),
    // then stream the executor's output. Simple query: all text, no params.
    let pending = txn.as_deref().map(Vec::as_slice);
    let mut stream = match open_select_stream(env, *select, pending, Vec::new()).await {
        Ok(stream) => stream,
        Err(error) => return Ok(vec![error]),
    };
    let fields = row_description_fields(stream.columns(), &[]);
    out.send(vec![BackendMessage::RowDescription { fields }])
        .await?;
    Ok(stream.pump(None, &[], out).await?.into_messages())
}

/// What a read runs against: the storage engine and schema it resolves tables
/// in, and the MVCC snapshot it sees.
#[derive(Clone, Copy)]
pub(crate) struct ReadEnv<'a> {
    pub(crate) engine: &'a Arc<StorageEngine>,
    pub(crate) schema: &'a Schema,
    pub(crate) default_schema: &'a str,
    pub(crate) mvcc: Option<&'a MvccManager>,
    pub(crate) snapshot: Option<&'a MvccSnapshot>,
    pub(crate) ddl: Option<&'a dyn crate::ddl::DdlExecutor>,
    /// Tunable jsonb ingest limits (D14b), for text coerced into jsonb columns.
    pub(crate) jsonb_limits: &'a ferrosa_jsonb::Limits,
}

/// Load `select`'s tables and start its executor on a blocking thread, ready to
/// be pumped. The single entry point for both simple and extended `SELECT`s.
pub(crate) async fn open_select_stream(
    env: ReadEnv<'_>,
    mut select: ferrosa_sql::SelectStmt,
    pending_writes: Option<&[PgWrite]>,
    params: Vec<SqlValue>,
) -> Result<ResultStream, BackendMessage> {
    // Resolve every `::` cast to a concrete value BEFORE planning: the `::type`
    // semantics (a relation name to its `pg_class.oid`) live here, in the
    // PostgreSQL front end, not in the pure engine. A cast that cannot be resolved
    // is an error here, never a value silently left to the executor.
    resolve_casts(&mut select, &params, env.schema, env.default_schema)?;
    let (catalog, failure) = load_catalog_with_mvcc(
        env.engine,
        env.schema,
        &select,
        env.default_schema,
        env.mvcc,
        env.snapshot,
        pending_writes,
    )
    .await?;
    open_stream(
        select,
        catalog,
        failure,
        env.default_schema.to_string(),
        params,
    )
    .await
}

/// Resolve every `::` cast in a table select's `WHERE`/`HAVING` to its concrete
/// value, before the executor plans the statement.
///
/// `ferrosa-sql` parses a cast and carries its target, but owns no catalog-OID
/// scheme (that is PostgreSQL-specific). The **semantics** of each target live
/// here: `text::regclass` resolves the relation name to the `pg_class.oid` of the
/// relation it names, so `oid = $1::regclass` compares exactly as PostgreSQL
/// does. The term is REWRITTEN to a literal OID, so the pure engine compares
/// like-with-like and a cast can never be silently ignored.
///
/// # Errors
///
/// `42P01` (undefined_table) when the name resolves to no relation — never a
/// zero OID, which would silently match nothing; `42846` (cannot_coerce) when the
/// operand is not a relation name; `08P01` when a cast's `$N` has no bound value.
fn resolve_casts(
    select: &mut SelectStmt,
    params: &[SqlValue],
    schema: &Schema,
    default_schema: &str,
) -> Result<(), BackendMessage> {
    if let Some(filter) = select.filter.as_mut() {
        resolve_casts_in_expr(filter, params, schema, default_schema)?;
    }
    if let Some(having) = select.having.as_mut() {
        resolve_casts_in_expr(having, params, schema, default_schema)?;
    }
    Ok(())
}

fn resolve_casts_in_expr(
    expr: &mut Expr,
    params: &[SqlValue],
    schema: &Schema,
    default_schema: &str,
) -> Result<(), BackendMessage> {
    match expr {
        Expr::Compare { value, .. } if term_has_cast(value) => {
            // `Term::Literal` cannot carry a cast, so the term is a cast tree.
            let resolved = eval_cast_term(value, params, schema, default_schema)?;
            *value = Term::Literal(resolved);
            Ok(())
        }
        Expr::Compare { .. } => Ok(()),
        Expr::Not(inner) => resolve_casts_in_expr(inner, params, schema, default_schema),
        Expr::And(left, right) | Expr::Or(left, right) => {
            resolve_casts_in_expr(left, params, schema, default_schema)?;
            resolve_casts_in_expr(right, params, schema, default_schema)
        }
    }
}

/// Whether this term carries a `::` cast anywhere (so it must be resolved).
fn term_has_cast(term: &Term) -> bool {
    match term {
        Term::Cast { .. } => true,
        Term::Literal(_) | Term::Param(_) => false,
    }
}

/// Evaluate a comparison term that carries a cast to its concrete value,
/// substituting bound `$N` values and applying each cast in turn.
fn eval_cast_term(
    term: &Term,
    params: &[SqlValue],
    schema: &Schema,
    default_schema: &str,
) -> Result<SqlValue, BackendMessage> {
    match term {
        Term::Literal(v) => Ok(v.clone()),
        Term::Param(n) => params.get(n.wrapping_sub(1)).cloned().ok_or_else(|| {
            error_response(
                "08P01",
                &format!("bind parameter ${n} has no value (out of range)"),
            )
        }),
        Term::Cast { value, ty } => {
            let inner = eval_cast_term(value, params, schema, default_schema)?;
            apply_cast(&inner, *ty, schema, default_schema)
        }
    }
}

/// Apply a parsed cast to an already-evaluated value — the ONE place a cast
/// target's meaning is decided.
fn apply_cast(
    value: &SqlValue,
    ty: CastTarget,
    schema: &Schema,
    default_schema: &str,
) -> Result<SqlValue, BackendMessage> {
    match ty {
        CastTarget::Regclass => match value {
            // PostgreSQL: NULL cast to any type is NULL. `oid = NULL` is UNKNOWN,
            // so the row is filtered out — never a zero OID that would match.
            SqlValue::Null => Ok(SqlValue::Null),
            SqlValue::Text(name) => {
                let oid = crate::catalog::resolve_regclass(schema, default_schema, name)
                    .ok_or_else(|| {
                        error_response("42P01", &format!("relation \"{name}\" does not exist"))
                    })?;
                Ok(SqlValue::Int(i64::from(oid)))
            }
            other => Err(error_response(
                "42846",
                &format!("cannot cast type {} to regclass", value_type_name(other)),
            )),
        },
    }
}

/// The Postgres type name of a value, for a `cannot cast` message.
fn value_type_name(value: &SqlValue) -> &'static str {
    match value {
        SqlValue::Int(_) => "integer",
        SqlValue::Float(_) => "double precision",
        SqlValue::Numeric { .. } => "numeric",
        SqlValue::Bool(_) => "boolean",
        SqlValue::Text(_) => "text",
        SqlValue::Null => "unknown",
        SqlValue::Uuid(_) => "uuid",
        SqlValue::Bytea(_) => "bytea",
        SqlValue::Timestamp(_) => "timestamp",
        SqlValue::Date(_) => "date",
        SqlValue::Time(_) => "time",
        SqlValue::Inet(_) => "inet",
        SqlValue::Jsonb(_) => "jsonb",
        SqlValue::JsonPath(_) => "jsonpath",
        SqlValue::TextArray(_) => "text[]",
    }
}

/// Where a reply's messages go as they are produced. The server's sink writes
/// them to the socket, so a streaming `SELECT` never accumulates its rows;
/// `Vec<BackendMessage>` collects them for tests and tools.
pub(crate) trait ReplySink {
    /// Deliver `messages` in order. An error means the peer is gone.
    async fn send(&mut self, messages: Vec<BackendMessage>) -> std::io::Result<()>;
}

impl ReplySink for Vec<BackendMessage> {
    async fn send(&mut self, messages: Vec<BackendMessage>) -> std::io::Result<()> {
        self.extend(messages);
        Ok(())
    }
}

/// Encode one result row as a `DataRow` under the portal's result formats.
///
/// # Errors
///
/// The message to report when a value has no encoding in the requested format.
pub(crate) fn encode_data_row(
    row: &Row,
    col_types: &[ColumnType],
    result_formats: &[i16],
) -> Result<BackendMessage, EncodeError> {
    let columns = row
        .0
        .iter()
        .enumerate()
        .map(|(i, v)| encode_value(result_format_for(result_formats, i), col_types[i], v))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BackendMessage::DataRow { columns })
}

/// Every statement that is not a table `SELECT`: bounded replies, returned whole.
/// Map a failed storage write to a PostgreSQL error, distinguishing deliberate
/// backpressure from a genuine failure.
///
/// The SQLSTATE class is the contract, not the message. `53000`
/// (insufficient_resources) says "I cannot accept at this pace" and drivers,
/// poolers and retry middleware back off and retry on it. `58000` is class 58,
/// a system error external to PostgreSQL, which the same clients treat as fatal
/// and do NOT retry. Collapsing an overload into 58000 means a producer that is
/// outrunning the device is never told to slow down, so it keeps producing — the
/// server sheds load it could have had the client pace instead.
///
/// `Error::is_backpressure()` is the shared classifier (it covers
/// `Error::Overloaded` and the disk-reserve refusal), so this stays in step with
/// the CQL front end rather than re-deciding what counts as backpressure.
pub(crate) fn write_error_response(error: &ferrosa_common::Error) -> BackendMessage {
    if error.is_backpressure() {
        // `is_backpressure()` has two branches: the typed `Error::Overloaded`,
        // and a legacy STRING match on `InvalidData` (`starts_with("overloaded:")`)
        // that exists only because the error type is flattened on some paths.
        // The string branch FAILS OPEN — if `Overloaded`'s Display ever changes,
        // it silently stops matching and a refusal is reported as 58000 again,
        // with nothing to show it regressed. So say when we relied on it. The
        // CQL side already logs this; this keeps the two front ends symmetric.
        if !matches!(error, ferrosa_common::Error::Overloaded { .. }) {
            tracing::warn!(
                %error,
                "backpressure classified by the legacy string match, not by type; \
                 the carrier on this path flattens the error (see t_01fe2523)"
            );
        }
        error_response("53000", &format!("write refused: {error}"))
    } else {
        error_response("58000", &format!("write failed: {error}"))
    }
}

async fn execute_statement(
    env: ReadEnv<'_>,
    stmt: Statement,
    txn: Option<&mut Vec<PgWrite>>,
) -> Vec<BackendMessage> {
    let ReadEnv {
        engine,
        schema,
        default_schema,
        mvcc,
        ddl,
        jsonb_limits,
        ..
    } = env;
    match stmt {
        // Routed to `execute_query_streaming` before this point.
        Statement::Select(_) => vec![error_response(
            "XX000",
            "internal error: a table SELECT reached the non-streaming path",
        )],
        // No-`FROM` expression query: `SELECT 1`, `SELECT version()`,
        // `SELECT (SELECT count(*) FROM t)`, etc.
        Statement::SelectExprs(items) => {
            let ctx = ScalarReadCtx::new(env, txn.as_deref().map(Vec::as_slice));
            match execute_scalar_select(&items, ctx).await {
                Ok(result) => render_result(result, &[]),
                Err(err_msg) => vec![err_msg],
            }
        }
        // Unreachable on the server path: `server::execute_simple` intercepts
        // BEGIN/COMMIT/ROLLBACK and drives them against the session's buffered
        // PostgreSQL MVCC write-set (`server::commit_txn`), and the
        // extended protocol does the same. This arm only catches a caller that
        // reaches `dispatch` directly without session state, where there is no
        // transaction to begin or commit. Failing loud beats silently reporting
        // a COMMIT that buffered nothing.
        Statement::Begin { .. } | Statement::Commit | Statement::Rollback => vec![error_response(
            "0A000",
            "transaction control requires a session; this path has no transaction state",
        )],
        // Session GUCs are not modeled yet.
        Statement::Set { .. } | Statement::Reset { .. } => vec![error_response(
            "0A000",
            "SET/RESET session statements are not yet implemented",
        )],
        // DDL (T-132a): through the same schema-change path CQL DDL uses.
        Statement::CreateTable(create) => {
            crate::ddl::execute_create_table(
                crate::ddl::DdlEnv {
                    executor: ddl,
                    schema,
                    default_schema,
                    in_txn: txn.is_some(),
                },
                &create,
            )
            .await
        }
        // DROP TABLE: pgbench -i's reset step. Same schema-change path.
        Statement::DropTable(drop) => {
            crate::ddl::execute_drop_table(
                crate::ddl::DdlEnv {
                    executor: ddl,
                    schema,
                    default_schema,
                    in_txn: txn.is_some(),
                },
                &drop,
            )
            .await
        }
        // `COPY ... FROM STDIN` cannot be answered in one step: the payload arrives AFTER the
        // acknowledgement, as `CopyData` frames, so the connection loop drives it. Reaching here
        // means it came in over a path that cannot carry a COPY — the extended protocol, or a tool
        // calling `execute_query` directly — which is refused rather than half-done.
        Statement::CopyFromStdin(_) => vec![error_response(
            "0A000",
            "COPY FROM STDIN must be driven by the connection loop, not answered directly",
        )],
        // ALTER TABLE: the step pgbench -i runs right after creating its tables without a key.
        // Same schema-change path.
        Statement::AlterTable(alter) => {
            crate::ddl::execute_alter_table(
                crate::ddl::DdlEnv {
                    executor: ddl,
                    schema,
                    default_schema,
                    in_txn: txn.is_some(),
                },
                &alter,
            )
            .await
        }
        // TRUNCATE [TABLE] t [, ...]: a normal replicated WRITE of a table-level
        // tombstone (ferrosa_storage::table_tombstone), through the SAME seam as an
        // INSERT/UPDATE/DELETE. That makes the statement transactional — it buffers
        // in `txn`, applies atomically on COMMIT, and is discarded by ROLLBACK — and
        // it replicates through the ordinary write path, so the node-local
        // `StorageEngine::truncate` (which would empty one replica) is never used.
        Statement::Truncate(table_list) => {
            execute_truncate(
                TruncateContext {
                    engine,
                    schema,
                    default_schema,
                    mvcc,
                    txn,
                },
                &table_list,
            )
            .await
        }
        // VACUUM: flush the table's memtables down and submit compaction. In an LSM/SSTable
        // store that IS what VACUUM means — it is not a no-op, and calling it one would be a lie
        // about a maintenance operation that really does run.
        //
        // What it does NOT do, stated plainly:
        //
        //   * It does not WAIT for compaction. `force_compact_all` submits tasks and returns, so
        //     the `VACUUM` tag means "flushed and compaction submitted", not "space reclaimed".
        //     (Real VACUUM likewise returns before all cleanup is guaranteed.)
        //   * It does not promise reclamation. Whether tombstoned data is actually dropped depends
        //     on the purge policy, gated by `compaction_purge_enabled()` in ferrosa-storage; this
        //     statement neither forces purge on nor claims a specific outcome.
        //   * `force_compact_all` is the storage engine's only force entry point and it considers
        //     EVERY table, so a named `VACUUM t` flushes only `t` but submits compaction for all
        //     tables with at least two SSTables. Maintenance either way, but a real difference
        //     from PostgreSQL, where `VACUUM t` touches only `t`.
        Statement::Vacuum(vacuum) => {
            let targets: Vec<ferrosa_storage::TableId> = match &vacuum.table {
                Some(named) => {
                    let found: Vec<ferrosa_storage::TableId> = engine
                        .registered_table_ids()
                        .into_iter()
                        .filter(|id| {
                            engine
                                .table_schema(id)
                                .is_some_and(|schema| schema.table == named.table)
                        })
                        .collect();
                    if found.is_empty() {
                        return vec![error_response(
                            "42P01",
                            &format!("relation \"{}\" does not exist", named.table),
                        )];
                    }
                    found
                }
                None => engine.registered_table_ids(),
            };
            // Flush first: compaction operates on SSTables, so anything still in memory has to be
            // pushed down before there is anything to compact.
            for id in &targets {
                if let Err(error) = engine.flush(id) {
                    return vec![write_error_response(&error)];
                }
            }
            engine.force_compact_all();
            vec![BackendMessage::CommandComplete {
                tag: "VACUUM".to_string(),
            }]
        }
        // ANALYZE: likewise a deliberate successful no-op — no statistics are collected.
        Statement::Analyze(_) => vec![BackendMessage::CommandComplete {
            tag: "ANALYZE".to_string(),
        }],
        // DML: single-row INSERT / UPDATE / DELETE. The simple-query path has no
        // bound parameters (`&[]`); a `$N` in simple SQL is therefore a fail-loud
        // error (no value to bind). With `txn = Some(buffer)` (an open
        // transaction) the write is BUFFERED as a `PgWrite` and committed
        // atomically via PostgreSQL MVCC on COMMIT; with `txn = None`
        // (autocommit) the MVCC manager applies it immediately. An INSERT
        // RETURNING leads with a RowDescription on this (simple) path.
        Statement::Insert(ins) => {
            execute_insert(
                DmlContext {
                    engine,
                    mvcc,
                    schema,
                    default_schema,
                    txn,
                    jsonb_limits,
                },
                &ins,
                &[],
                ReturningOpts::simple(),
            )
            .await
        }
        Statement::Update(upd) => {
            let context = DmlContext {
                engine,
                mvcc,
                schema,
                default_schema,
                txn,
                jsonb_limits,
            };
            execute_update(context, &upd, &[]).await
        }
        Statement::Delete(del) => {
            let context = DmlContext {
                engine,
                mvcc,
                schema,
                default_schema,
                txn,
                jsonb_limits,
            };
            execute_delete(context, &del, &[]).await
        }
    }
}

/// Buffer a built `Mutation` as a PostgreSQL `PgWrite` into the open
/// transaction's write-set, or apply it immediately via the MVCC manager when
/// there is no open transaction (autocommit).
///
/// FAIL LOUD: when buffering, exceeding the configured write cap returns an error
/// response (and the server poisons the transaction) rather than growing the
/// buffer without bound; a buffered write is NEVER applied to storage here —
/// only the PostgreSQL MVCC manager applies it on COMMIT.
async fn apply_or_buffer(
    engine: &StorageEngine,
    schema: &Schema,
    mvcc: Option<&MvccManager>,
    txn: Option<&mut Vec<PgWrite>>,
    mutation: Mutation,
    ok_tag: &str,
) -> Vec<BackendMessage> {
    match apply_or_buffer_silent(engine, schema, mvcc, txn, mutation).await {
        Ok(()) => vec![BackendMessage::CommandComplete {
            tag: ok_tag.to_string(),
        }],
        Err(messages) => messages,
    }
}

/// The write half of [`apply_or_buffer`]: buffer the mutation in the open
/// transaction, or apply it immediately (autocommit). Returns `Ok(())` on
/// success and the error response(s) to send on failure — no `CommandComplete`,
/// so a caller that writes several mutations for ONE statement (TRUNCATE of a
/// table list) can emit a single tag.
async fn apply_or_buffer_silent(
    engine: &StorageEngine,
    schema: &Schema,
    mvcc: Option<&MvccManager>,
    txn: Option<&mut Vec<PgWrite>>,
    mutation: Mutation,
) -> Result<(), Vec<BackendMessage>> {
    let max_txn_writes = mvcc.map_or(DEFAULT_MAX_TXN_WRITES, MvccManager::max_txn_writes);
    match txn {
        Some(buffer) => {
            if buffer.len() >= max_txn_writes {
                return Err(vec![error_response(
                    "53400",
                    &format!(
                        "transaction write-set exceeds the {max_txn_writes}-write limit; \
                         ROLLBACK required"
                    ),
                )]);
            }
            buffer.push(PgWrite(mutation));
            Ok(())
        }
        None => match mvcc {
            Some(mvcc) => {
                let _commit_guard = mvcc.commit_guard().await;
                match commit_mutations(
                    engine,
                    schema,
                    mvcc,
                    &mvcc.snapshot(),
                    &std::collections::HashSet::new(),
                    vec![mutation],
                ) {
                    Ok(_) => Ok(()),
                    Err(MvccCommitError::SerializationFailure) => Err(vec![error_response(
                        "40001",
                        "could not serialize PostgreSQL transaction",
                    )]),
                    // Now that `Storage` carries the typed error, a commit
                    // refused for backpressure answers 53000 like the direct
                    // write path, instead of collapsing into 58000.
                    Err(MvccCommitError::Storage(error)) => Err(vec![write_error_response(&error)]),
                    Err(error) => Err(vec![error_response(
                        "58000",
                        &format!("write failed: {error:?}"),
                    )]),
                }
            }
            None => match engine.write_atomic_batch(vec![mutation]) {
                Ok(()) => Ok(()),
                Err(e) => Err(vec![write_error_response(&e)]),
            },
        },
    }
}

/// What a `TRUNCATE` runs against: the same storage engine, schema registry and
/// transaction write-set an `INSERT`/`UPDATE`/`DELETE` runs against. TRUNCATE is a
/// write now, so it shares the write seam and inherits its transactionality.
pub(crate) struct TruncateContext<'a> {
    pub(crate) engine: &'a StorageEngine,
    pub(crate) schema: &'a Schema,
    pub(crate) default_schema: &'a str,
    pub(crate) mvcc: Option<&'a MvccManager>,
    pub(crate) txn: Option<&'a mut Vec<PgWrite>>,
}

/// Execute `TRUNCATE [TABLE] a [, b, ...]`.
///
/// Each named table is resolved in its schema (defaulting to `env.default_schema`)
/// and truncated by writing a **table-level tombstone** — one reserved-partition
/// [`Mutation`] carrying [`ferrosa_storage::table_tombstone::table_tombstone_row`] —
/// through [`apply_or_buffer_silent`], the SAME seam every DML write uses. The
/// tombstone is a normal replicated write, so:
///
/// - inside a transaction it buffers and is discarded by `ROLLBACK` (no `25001`);
/// - in autocommit it applies at once and, in cluster mode, replicates through the
///   ordinary write path — never the node-local `StorageEngine::truncate`.
///
/// In cluster mode the tombstone's commit is replicated to **every node that can
/// serve the table** at `ConsistencyLevel::All` (`AccordTransactionCommitter`),
/// so a node outside the reserved key's RF replica set still learns of the
/// truncate. That is the difference between a working `TRUNCATE` and a silent
/// resurrection of the rows on the nodes the key's RF set misses.
///
/// Every named table's existence is checked BEFORE any write so a refused statement
/// changes nothing (`42P01`). Reply is one `TRUNCATE TABLE` command tag.
pub(crate) async fn execute_truncate(
    context: TruncateContext<'_>,
    stmt: &ferrosa_sql::TruncateStatement,
) -> Vec<BackendMessage> {
    let TruncateContext {
        engine,
        schema,
        default_schema,
        mvcc,
        mut txn,
    } = context;

    // Resolve and validate every name first: a missing table must not truncate the
    // tables named before it, so a refused TRUNCATE writes nothing at all.
    let mut resolved: Vec<(String, String)> = Vec::with_capacity(stmt.tables.len());
    for target in &stmt.tables {
        let keyspace = target.schema.as_deref().unwrap_or(default_schema);
        if !schema
            .snapshot()
            .tables
            .contains_key(&(keyspace.to_string(), target.table.clone()))
        {
            return vec![error_response(
                "42P01",
                &format!("relation \"{}\" does not exist", target.table),
            )];
        }
        resolved.push((keyspace.to_string(), target.table.clone()));
    }

    for (keyspace, table) in resolved {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| (d.as_micros() as i64, d.as_secs() as u32))
            .unwrap_or((0, 0));
        let (marked_for_delete_at, local_deletion_time) = now;
        let marker = ferrosa_storage::table_tombstone::table_tombstone_row(
            marked_for_delete_at,
            local_deletion_time,
        );
        let mutation = Mutation::new(
            keyspace,
            table,
            ferrosa_storage::table_tombstone::table_tombstone_key(),
            vec![marker],
            marked_for_delete_at,
        );
        if let Err(messages) =
            apply_or_buffer_silent(engine, schema, mvcc, txn.as_deref_mut(), mutation).await
        {
            return messages;
        }
    }

    vec![BackendMessage::CommandComplete {
        tag: "TRUNCATE TABLE".to_string(),
    }]
}

/// Apply a PostgreSQL write batch atomically and publish row versions only after
/// storage confirms the entire batch. This path is independent of Cassandra's
/// Cassandra Accord transaction protocol.
pub(crate) fn commit_mutations(
    engine: &StorageEngine,
    schema: &Schema,
    mvcc: &MvccManager,
    snapshot: &MvccSnapshot,
    read_tables: &std::collections::HashSet<String>,
    mutations: Vec<Mutation>,
) -> Result<u64, MvccCommitError> {
    if mutations.is_empty() {
        return Ok(mvcc.current_commit_seq());
    }
    let changes = prepare_row_changes(engine, schema, &mutations)?;
    mvcc.commit(snapshot, read_tables, || {
        // The typed error propagates: the front end must be able to ask
        // `is_backpressure()` to choose between 53000 and 58000.
        engine.write_atomic_batch(mutations)?;
        Ok(changes)
    })
}

/// Build MVCC row images before a distributed commit applies the mutations.
pub(crate) fn prepare_row_changes(
    engine: &StorageEngine,
    schema: &Schema,
    mutations: &[Mutation],
) -> Result<Vec<RowChange>, MvccCommitError> {
    if mutations.is_empty() {
        return Ok(Vec::new());
    }
    (|| {
        let mut table_rows: std::collections::HashMap<
            (String, String),
            std::collections::HashMap<Vec<SqlValue>, Option<Row>>,
        > = std::collections::HashMap::new();
        let mut before_rows: std::collections::HashMap<
            (String, String),
            std::collections::HashMap<Vec<SqlValue>, Option<Row>>,
        > = std::collections::HashMap::new();
        let mut partition_keys: std::collections::HashMap<Vec<SqlValue>, Vec<u8>> =
            std::collections::HashMap::new();
        let mut touched_tables = std::collections::HashSet::new();
        // One decode projection per table, not one per row: `read_row_image` used
        // to rebuild the declared names, parsed types and key indices on every
        // call, which a 1.1M-row commit paid 1.1M times.
        let mut codecs: std::collections::HashMap<(String, String), TableCodec> =
            std::collections::HashMap::new();
        for mutation in mutations {
            let table = (mutation.keyspace.clone(), mutation.table.clone());
            touched_tables.insert(table.clone());
            if !codecs.contains_key(&table) {
                codecs.insert(
                    table.clone(),
                    TableCodec::build(&mutation.keyspace, &mutation.table, schema)
                        .map_err(ferrosa_common::Error::InvalidData)?,
                );
            }
            let codec = codecs
                .get(&table)
                .expect("codec was just inserted for this table");
            for row in &mutation.rows {
                let before = crate::storage_provider::read_row_image(
                    engine,
                    codec,
                    mutation,
                    &row.clustering,
                )
                .map_err(|error| {
                    ferrosa_common::Error::InvalidData(format!("read before image failed: {error}"))
                })?;
                if let Some((key, image)) = before {
                    partition_keys.insert(key.clone(), mutation.key.key.as_bytes().to_vec());
                    table_rows
                        .entry(table.clone())
                        .or_default()
                        .insert(key.clone(), Some(image.clone()));
                    before_rows
                        .entry(table.clone())
                        .or_default()
                        .insert(key, Some(image));
                }
            }
        }
        for (keyspace, table) in touched_tables {
            let codec = codecs
                .get(&(keyspace.clone(), table.clone()))
                .expect("a touched table always has a codec");
            let overlay = table_rows
                .entry((keyspace.clone(), table.clone()))
                .or_default();
            crate::storage_provider::apply_pending_writes_with_partition_keys(
                engine,
                codec,
                &keyspace,
                &table,
                overlay,
                mutations.iter(),
                crate::storage_provider::ApplyOutputs {
                    partition_keys: Some(&mut partition_keys),
                    before_images: None,
                    before_cache: None,
                },
            )
            .map_err(|error| {
                ferrosa_common::Error::InvalidData(format!(
                    "build transaction row image failed: {error}"
                ))
            })?;
        }
        let mut changes = Vec::new();
        for ((keyspace, table), after_rows) in table_rows {
            let before = before_rows
                .remove(&(keyspace.clone(), table.clone()))
                .unwrap_or_default();
            for (key, after) in after_rows {
                changes.push(RowChange {
                    table: format!("{keyspace}.{table}"),
                    partition_key: partition_keys.get(&key).cloned().ok_or_else(|| {
                        ferrosa_common::Error::InvalidData(
                            "could not map PostgreSQL row version to its partition".to_string(),
                        )
                    })?,
                    before: before.get(&key).cloned().unwrap_or(None),
                    key,
                    after,
                });
            }
        }
        Ok(changes)
    })()
    .map_err(MvccCommitError::Storage)
}

/// Table identity used to key the per-table codec/overlay caches of
/// [`prepare_accord_writes`].
///
/// `Arc<str>` so a repeated lookup is a refcount bump rather than two `String`
/// heap allocations: a bulk `COPY` writes ONE table at a time, so the identity
/// changes O(tables) times across a whole write-set, never O(mutations).
type TableKey = (std::sync::Arc<str>, std::sync::Arc<str>);

/// Hands out the current table's [`TableKey`], rebuilding it only when the
/// mutation's table differs from the last one seen — allocation-free for the
/// grouped-by-table order a bulk load produces, and still correct for an
/// interleaved one (it just pays one rebuild per change).
struct TableKeyCache {
    current: Option<TableKey>,
}

impl TableKeyCache {
    fn key_for(&mut self, keyspace: &str, table: &str) -> TableKey {
        let stale = match &self.current {
            Some((ks, name)) => &**ks != keyspace || &**name != table,
            None => true,
        };
        if stale {
            self.current = Some((std::sync::Arc::from(keyspace), std::sync::Arc::from(table)));
        }
        self.current.clone().expect("the cache was just populated")
    }
}

/// Build a PostgreSQL `COMMIT`'s Accord write-set in **ONE streaming pass**,
/// without materializing whole-table row images.
///
/// The former shape called [`prepare_row_changes`], which built whole-table
/// images (`table_rows`, a duplicated `before_rows`, a global `partition_keys`)
/// and returned them as a `Vec<RowChange>` — one entry per row, each carrying its
/// before *and* after image — which the caller then re-grouped into a
/// `changes_by_partition` map. For the `pgbench -i --scale 10` shape (1.1M rows in
/// one transaction) that is four to five simultaneous full copies of the write-set
/// on the coordinator's COMMIT peak, which is what OOM-killed the node. None of
/// them is required: a `Mutation` **is** one storage partition, so its row
/// versions belong to that partition alone and can be encoded and handed to the
/// committer as the mutation is visited.
///
/// The only cross-mutation state that must survive is the per-table overlay —
/// and only because a transaction may write the same SQL key twice, in which case
/// its later write must build on its earlier one. The resident row-image count is
/// therefore O(one mutation + the overlay), never O(rows x copies).
///
/// The emitted `TransactionWrite` order matches the input mutation order, and the
/// per-partition metadata is identical to
/// `prepare_row_changes`-then-group for a write-set whose SQL keys are distinct —
/// the transactional `COPY` case, and every case except a transaction that writes
/// one key twice, where this streams each intermediate row image rather than only
/// the final one (a duplicate-key write-set is last-write-wins at the MVCC apply
/// anyway; see `record_applied_accord_commit`).
pub(crate) fn prepare_accord_writes(
    engine: &StorageEngine,
    schema: &Schema,
    mutations: Vec<Mutation>,
) -> Result<Vec<ferrosa_storage::accord::TransactionWrite>, MvccCommitError> {
    use ferrosa_storage::accord::TransactionWrite;

    let mut codecs: std::collections::HashMap<TableKey, TableCodec> =
        std::collections::HashMap::new();
    let mut overlays: std::collections::HashMap<
        TableKey,
        std::collections::HashMap<Vec<SqlValue>, Option<Row>>,
    > = std::collections::HashMap::new();
    let mut writes: Vec<TransactionWrite> = Vec::with_capacity(mutations.len());
    // Temporary attribution scaffolding (see `FERROSA_PG_COMMIT_PROFILE`).
    let profile = std::env::var_os("FERROSA_PG_COMMIT_PROFILE").is_some();
    let mut serialize_ns = 0u64;
    let mut pending_ns = 0u64;
    let mut metadata_bytes = 0usize;

    /// How many mutations' pre-transaction before-images are resolved in ONE
    /// batched storage read.
    ///
    /// A point read per row pays the storage view/schema/tombstone fixed cost once
    /// per row; batching resolves a whole window against a single store view. The
    /// bound keeps the prefetch cache — and the mutations it spans — O(chunk), not
    /// O(write-set), so the streaming shape (and its commit peak) is preserved.
    const BEFORE_IMAGE_PREFETCH_CHUNK: usize = 4096;

    // ONE serialization buffer for the whole write-set, reused per mutation
    // instead of a fresh `vec![0; n]` allocation each time (see `serialize_into`
    // below). `mutations` is CONSUMED by this function, so nothing needs the
    // per-mutation buffer to outlive its iteration.
    let mut bytes: Vec<u8> = Vec::new();
    let mut table_keys = TableKeyCache { current: None };
    let mut pending: Vec<Mutation> = mutations;
    while !pending.is_empty() {
        let take = BEFORE_IMAGE_PREFETCH_CHUNK.min(pending.len());
        let chunk: Vec<Mutation> = pending.drain(..take).collect();

        // Prebuild each distinct table's codec once, then batch-read this chunk's
        // before-images for it against a single store view.
        let mut chunk_tables: Vec<TableKey> = Vec::new();
        for mutation in &chunk {
            if ferrosa_storage::table_tombstone::is_table_tombstone_key(&mutation.key) {
                continue;
            }
            let table = table_keys.key_for(&mutation.keyspace, &mutation.table);
            if !chunk_tables.contains(&table) {
                chunk_tables.push(table);
            }
        }
        let mut before_caches: std::collections::HashMap<
            TableKey,
            std::collections::HashMap<Vec<SqlValue>, Option<Row>>,
        > = std::collections::HashMap::new();
        for table in &chunk_tables {
            let codec = match codecs.entry(table.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(TableCodec::build(&table.0, &table.1, schema).map_err(
                        |error| MvccCommitError::Storage(ferrosa_common::Error::InvalidData(error)),
                    )?)
                }
            };
            let overlay = overlays.entry(table.clone()).or_default();
            let cache = crate::storage_provider::prefetch_before_images(
                engine,
                codec,
                &table.0,
                &table.1,
                overlay,
                chunk.iter(),
            )
            .map_err(|error| MvccCommitError::Storage(ferrosa_common::Error::InvalidData(error)))?;
            if !cache.is_empty() {
                before_caches.insert(table.clone(), cache);
            }
        }

        for mutation in chunk {
            let table = table_keys.key_for(&mutation.keyspace, &mutation.table);
            // Reuse the ONE buffer: `clear` + `resize` to this mutation's exact
            // `serialized_size` writes exactly what a fresh zeroed buffer would, so a
            // previous (larger) mutation's tail can never leak into this frame.
            bytes.clear();
            bytes.resize(mutation.serialized_size(), 0);
            let serialize_started = profile.then(std::time::Instant::now);
            mutation.serialize_into(&mut bytes);
            if let Some(started) = serialize_started {
                serialize_ns += u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            }

            // A table tombstone (`TRUNCATE`) marker carries no row image; the cluster
            // committer routes it to every serving node at CL=ALL from its own bytes.
            if ferrosa_storage::table_tombstone::is_table_tombstone_key(&mutation.key) {
                writes.push(TransactionWrite {
                    keyspace: mutation.keyspace,
                    // MOVED out of the consumed mutation, never copied.
                    key: mutation.key.key.into_bytes(),
                    // The shared buffer lives on, so a tombstone takes a copy. A
                    // tombstone is ONE entry per TRUNCATE, not one per row.
                    mutation: bytes.clone(),
                });
                continue;
            }

            let codec = match codecs.entry(table.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(TableCodec::build(&table.0, &table.1, schema).map_err(
                        |error| MvccCommitError::Storage(ferrosa_common::Error::InvalidData(error)),
                    )?)
                }
            };
            let overlay = overlays.entry(table.clone()).or_default();
            // Scoped to this one mutation; dropped at the end of the iteration.
            let mut before_images: std::collections::HashMap<Vec<SqlValue>, Option<Row>> =
                std::collections::HashMap::new();
            let mut partition_keys: std::collections::HashMap<Vec<SqlValue>, Vec<u8>> =
                std::collections::HashMap::new();
            let pending_started = profile.then(std::time::Instant::now);
            crate::storage_provider::apply_pending_writes_with_partition_keys(
                engine,
                codec,
                &table.0,
                &table.1,
                overlay,
                std::iter::once(&mutation),
                crate::storage_provider::ApplyOutputs {
                    partition_keys: Some(&mut partition_keys),
                    before_images: Some(&mut before_images),
                    before_cache: before_caches.get(&table),
                },
            )
            .map_err(|error| MvccCommitError::Storage(ferrosa_common::Error::InvalidData(error)))?;
            if let Some(started) = pending_started {
                pending_ns += u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            }

            // Encode this partition's row-version metadata now, while only its own
            // images are resident, and never keep the `RowChange` list.
            //
            // The partition bytes are MOVED out of the consumed mutation instead of
            // copied with `as_bytes().to_vec()`.
            let partition_key = mutation.key.key.into_bytes();
            let mutation_bytes = if partition_keys.is_empty() {
                // The shared serialize buffer is reused, so this rare no-row path takes
                // a copy (a bulk `COPY` always carries rows, so it is not the hot path).
                bytes.clone()
            } else {
                // Each partition's SQL key and partition bytes are MOVED out of
                // `partition_keys` (dropped at the end of this iteration) rather than
                // cloned: the map already holds exactly the bytes the metadata needs.
                let changes: Vec<RowChange> = partition_keys
                    .into_iter()
                    .map(|(key, partition_bytes)| {
                        let before = before_images.get(&key).cloned().flatten();
                        let after = overlay.get(&key).cloned().flatten();
                        RowChange {
                            table: format!("{}.{}", table.0, table.1),
                            key,
                            partition_key: partition_bytes,
                            before,
                            after,
                        }
                    })
                    .collect();
                let metadata = serde_json::to_vec(&changes).map_err(|error| {
                    MvccCommitError::Storage(ferrosa_common::Error::InvalidData(format!(
                        "serialize PostgreSQL MVCC row versions: {error}"
                    )))
                })?;
                metadata_bytes += metadata.len();
                ferrosa_storage::accord::encode_postgres_mvcc_mutation(&bytes, &metadata).map_err(
                    |error| MvccCommitError::Storage(ferrosa_common::Error::InvalidData(error)),
                )?
            };

            writes.push(TransactionWrite {
                keyspace: mutation.keyspace,
                key: partition_key,
                mutation: mutation_bytes,
            });
        }
    }
    if profile {
        use std::sync::atomic::Ordering;
        tracing::info!(
            mutations = writes.len(),
            pending_ms = pending_ns as f64 / 1_000_000.0,
            serialize_ms = serialize_ns as f64 / 1_000_000.0,
            metadata_kib = metadata_bytes / 1024,
            read_image_calls = crate::storage_provider::READ_IMAGE_CALLS.load(Ordering::Relaxed),
            read_image_ms =
                crate::storage_provider::READ_IMAGE_NS.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            "prepare_accord_writes attribution"
        );
    }
    Ok(writes)
}

/// Resolve a DML scalar to a concrete [`SqlValue`], substituting bound
/// parameters from `params` (1-based, the Postgres `$N` numbering). A literal
/// passes through; a `$N` indexes `params`. FAILS LOUD (`08P01`,
/// protocol_violation) when `$N` has no bound value — never a silent default, so
/// a parameter the client failed to bind can never become NULL. Function calls
/// in DML values are unsupported (`0A000`). A `||` is concatenated the same way a
/// select-list one is (the DML value grammar never produces it today; the arm
/// keeps the semantics uniform rather than unreachable).
fn substitute_param(sv: &ScalarValue, params: &[SqlValue]) -> Result<SqlValue, BackendMessage> {
    match sv {
        ScalarValue::Literal(v) => Ok(v.clone()),
        ScalarValue::Param(n) => {
            // `$N` is 1-based; `params` is 0-based.
            params.get(n.wrapping_sub(1)).cloned().ok_or_else(|| {
                error_response(
                    "08P01",
                    &format!("bind parameter ${n} has no value (out of range)"),
                )
            })
        }
        ScalarValue::Func(_) => Err(error_response(
            "0A000",
            "function calls in DML values are not supported",
        )),
        ScalarValue::Concat { left, right } => {
            let left = substitute_param(left, params)?;
            let right = substitute_param(right, params)?;
            concat_text(&left, &right)
        }
        // The DML/`VALUES` grammar never builds a cast (`parse_scalar_value` has no
        // `::`), so this arm is unreachable today; it refuses loudly rather than
        // guessing if a future grammar reaches it.
        ScalarValue::Cast { ty, .. } => Err(error_response(
            "0A000",
            &format!("a `::{}` cast is not supported in a DML value", ty.name()),
        )),
        // The DML/`VALUES` grammar never builds a scalar subquery, so this arm is
        // unreachable today; it refuses loudly rather than guessing if a future
        // grammar reaches it (there is no read environment on this synchronous,
        // value-resolving path to run the inner query against).
        ScalarValue::Subquery(_) => Err(error_response(
            "0A000",
            "a scalar subquery in a DML value is not supported",
        )),
    }
}

/// Coerce numeric text (`[-]ddd[.ddd][e[+-]dd]`) into a [`CqlValue::Decimal`],
/// reusing [`parse_numeric_text`] — the SAME parser the numeric text-parameter
/// path and the jsonb text-input path use. A value that is not a valid numeric
/// fails loud (`22P02`, invalid_text_representation) rather than guessing one.
fn decimal_from_numeric_text(text: &str) -> Result<CqlValue, BackendMessage> {
    match parse_numeric_text(text) {
        Some(SqlValue::Numeric { unscaled, scale }) => Ok(CqlValue::Decimal { scale, unscaled }),
        _ => Err(error_response(
            "22P02",
            &format!("invalid input syntax for type numeric: \"{text}\""),
        )),
    }
}

/// Parse a TEXT value into the [`SqlValue`] the target column's arm expects.
///
/// Only the types with an unambiguous text form are handled; anything else is refused by name
/// rather than guessed at, because a silently misread value is worse than a refusal. A value that
/// has a text form but does not parse is refused `22P02`, matching PostgreSQL's
/// `invalid_text_representation` — never defaulted to zero, empty, or NULL.
fn text_to_sql_value(ty: &CqlType, text: &str) -> Result<SqlValue, BackendMessage> {
    let trimmed = text.trim();
    let bad = |what: &str| {
        Err(error_response(
            "22P02",
            &format!("invalid input syntax for type {what}: \"{text}\""),
        ))
    };
    match ty {
        CqlType::Int | CqlType::Bigint | CqlType::Counter => trimmed
            .parse::<i64>()
            .map(SqlValue::Int)
            .or_else(|_| bad("integer")),
        CqlType::Smallint => trimmed
            .parse::<i16>()
            .map(|v| SqlValue::Int(v as i64))
            .or_else(|_| bad("smallint")),
        CqlType::Tinyint => trimmed
            .parse::<i8>()
            .map(|v| SqlValue::Int(v as i64))
            .or_else(|_| bad("tinyint")),
        CqlType::Float | CqlType::Double => trimmed
            .parse::<f64>()
            .map(|v| SqlValue::Float(v.into()))
            .or_else(|_| bad("float")),
        // PostgreSQL's boolean input accepts these spellings; anything else is not a boolean.
        CqlType::Boolean => match trimmed.to_ascii_lowercase().as_str() {
            "t" | "true" | "y" | "yes" | "on" | "1" => Ok(SqlValue::Bool(true)),
            "f" | "false" | "n" | "no" | "off" | "0" => Ok(SqlValue::Bool(false)),
            _ => bad("boolean"),
        },
        CqlType::Uuid | CqlType::Timeuuid => trimmed
            .parse::<uuid::Uuid>()
            .map(SqlValue::Uuid)
            .or_else(|_| bad("uuid")),
        CqlType::Inet => trimmed
            .parse::<std::net::IpAddr>()
            .map(SqlValue::Inet)
            .or_else(|_| bad("inet")),
        // PostgreSQL's bytea text form: `\x` followed by an even number of hex digits.
        CqlType::Blob => {
            let Some(hex) = trimmed.strip_prefix("\\x") else {
                return bad("bytea");
            };
            if hex.len() % 2 != 0 {
                return bad("bytea");
            }
            let mut bytes = Vec::with_capacity(hex.len() / 2);
            for pair in hex.as_bytes().chunks(2) {
                let Ok(byte) = u8::from_str_radix(std::str::from_utf8(pair).unwrap_or(""), 16)
                else {
                    return bad("bytea");
                };
                bytes.push(byte);
            }
            Ok(SqlValue::Bytea(bytes))
        }
        // Types whose text form is NOT implemented here. Refused by name rather than guessed,
        // and deliberately not routed through the `42804` catch-all so the message says which
        // thing is missing instead of blaming the client's value.
        CqlType::Timestamp
        | CqlType::Date
        | CqlType::Time
        | CqlType::Duration
        | CqlType::Varint
        | CqlType::List(_)
        | CqlType::Map(_, _)
        | CqlType::Set(_)
        | CqlType::Tuple(_)
        | CqlType::Udt { .. }
        | CqlType::Vector(_, _)
        | CqlType::Varchar
        | CqlType::Ascii
        | CqlType::Decimal
        | CqlType::Jsonb => Err(error_response(
            "0A000",
            &format!("no text form is implemented for column type {ty:?}"),
        )),
    }
}

/// Convert a SQL [`SqlValue`] literal to the [`CqlValue`] the engine stores,
/// driven by the target column's [`CqlType`]. The inverse of
/// `storage_provider::cql_to_value`; `Null` maps to a tombstone for any type,
/// and a type mismatch fails loud (`42804`) rather than silently coercing.
///
/// `jsonb_limits` gates the jsonb arm only: a text value bound to a jsonb column
/// (an untyped literal, or a parameter declared text) is parsed and validated
/// here, as PostgreSQL coerces an unknown-typed literal (D11, D14b).
pub(crate) fn value_to_cql(
    value: &SqlValue,
    ty: &CqlType,
    jsonb_limits: &ferrosa_jsonb::Limits,
) -> Result<CqlValue, BackendMessage> {
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
        // A numeric/decimal column takes an integer literal widened exactly, at
        // scale 0 — PostgreSQL coerces an untyped integer literal to `numeric`.
        (CqlType::Decimal, SqlValue::Int(i)) => CqlValue::Decimal {
            scale: 0,
            unscaled: (*i).into(),
        },
        // A decimal literal is lowered by the SQL front-end to `f64`. Recover its
        // shortest round-trip decimal text and parse it with the SAME routine the
        // numeric text-parameter and jsonb text-input paths use, so `1.5` binds as
        // unscaled 15 / scale 1 rather than being refused.
        (CqlType::Decimal, SqlValue::Float(f)) => {
            decimal_from_numeric_text(&f.into_inner().to_string())?
        }
        // A value that arrives as TEXT — an untyped string literal, or a COPY FROM
        // STDIN payload cell — is parsed into a decimal exactly like a numeric
        // parameter. A non-numeric string fails loud (22P02), never a silent guess.
        (CqlType::Decimal, SqlValue::Text(s)) => decimal_from_numeric_text(s)?,
        (CqlType::Varint, SqlValue::Numeric { unscaled, scale }) if *scale == 0 => {
            CqlValue::Varint(unscaled.clone())
        }
        // T-160: a validated jsonb value binds to a jsonb column as its cell.
        (CqlType::Jsonb, SqlValue::Jsonb(doc)) => CqlValue::Jsonb(doc.clone()),
        // Untyped literal / text-declared parameter: parse and validate.
        (CqlType::Jsonb, SqlValue::Text(text)) => {
            use crate::jsonb_wire::{parse_text_input, InputEdge};
            let doc = parse_text_input(text.as_bytes(), jsonb_limits, InputEdge::Literal)
                .map_err(|e| error_response(e.sqlstate, &e.message))?;
            CqlValue::Jsonb(doc)
        }
        // A COPY payload carries NO TYPES: every field arrives as bytes, so the destination COLUMN
        // is the only thing that can say what a field means. PostgreSQL parses COPY input per the
        // target column's type for the same reason. Without this arm, `COPY t FROM STDIN` into any
        // non-text column fails `42804` — observed live as
        // `ERROR: value does not match column type Int`.
        //
        // The text is parsed into the SqlValue the target type already expects and the call
        // recurses, so units and conversions stay in ONE place: a text `1.5` bound to a Decimal
        // goes down the very arm the literal `1.5` takes, and cannot drift from it.
        //
        // This sits AFTER the specific text arms above (varchar/ascii/decimal/jsonb), which keep
        // their own meaning, and BEFORE the catch-all, which still refuses a NON-text value whose
        // type does not match.
        (ty, SqlValue::Text(text)) => {
            value_to_cql(&text_to_sql_value(ty, text)?, ty, jsonb_limits)?
        }
        (CqlType::Ascii, _)
        | (CqlType::Bigint, _)
        | (CqlType::Blob, _)
        | (CqlType::Boolean, _)
        | (CqlType::Counter, _)
        | (CqlType::Decimal, _)
        | (CqlType::Double, _)
        | (CqlType::Float, _)
        | (CqlType::Int, _)
        | (CqlType::Timestamp, _)
        | (CqlType::Uuid, _)
        | (CqlType::Varchar, _)
        | (CqlType::Varint, _)
        | (CqlType::Timeuuid, _)
        | (CqlType::Inet, _)
        | (CqlType::Date, _)
        | (CqlType::Time, _)
        | (CqlType::Smallint, _)
        | (CqlType::Tinyint, _)
        | (CqlType::Duration, _)
        | (CqlType::List(_), _)
        | (CqlType::Map(_, _), _)
        | (CqlType::Set(_), _)
        | (CqlType::Tuple(_), _)
        | (CqlType::Udt { .. }, _)
        | (CqlType::Jsonb, _)
        | (CqlType::Vector(_, _), _) => {
            return Err(error_response(
                "42804",
                &format!("value does not match column type {ty:?}"),
            ))
        }
    };
    Ok(out)
}

/// How an `INSERT ... RETURNING` result is rendered, bundled so `execute_insert`
/// stays within the argument limit. `with_row_description` controls whether the
/// result leads with a `RowDescription` (simple path: `true`; extended Execute:
/// `false`, the client already learned columns from `Describe`); `result_formats`
/// are the portal's per-column format codes (empty ⇒ all text).
#[derive(Clone, Copy)]
pub(crate) struct ReturningOpts<'a> {
    pub with_row_description: bool,
    pub result_formats: &'a [i16],
}

/// Shared storage and transaction state for one PostgreSQL DML statement.
pub(crate) struct DmlContext<'a> {
    pub engine: &'a StorageEngine,
    pub mvcc: Option<&'a MvccManager>,
    pub schema: &'a Schema,
    pub default_schema: &'a str,
    pub txn: Option<&'a mut Vec<PgWrite>>,
    /// Tunable jsonb ingest limits (D14b) for text bound to jsonb columns.
    pub jsonb_limits: &'a ferrosa_jsonb::Limits,
}

impl ReturningOpts<'_> {
    /// The simple-query default: lead with a `RowDescription`, all text format.
    pub(crate) fn simple() -> Self {
        Self {
            with_row_description: true,
            result_formats: &[],
        }
    }
}

/// Execute a single-row `INSERT`: materialize the row from the table schema and
/// write it through the engine. Returns `CommandComplete "INSERT 0 1"` (Postgres
/// reports oid 0 + a 1-row count). The row encoder is the shared
/// `ferrosa-row-bridge` one — the SAME bytes the engine + CQL reads decode.
///
/// `params` supplies bound `$N` values (the extended-query path); the simple
/// path passes `&[]`. `returning_opts` controls RETURNING rendering (see
/// [`ReturningOpts`]). `txn = Some(buffer)` BUFFERS the write as a
/// PostgreSQL `PgWrite` (committed atomically on COMMIT); `None` is autocommit.
///
/// `RETURNING` echoes the values just written (built in-memory from the supplied
/// column values — storage is NOT read back): exactly what Ecto needs to recover
/// a generated/echoed key after an insert. A buffered write still returns its
/// RETURNING rows now; the write commits at COMMIT.
pub(crate) async fn execute_insert(
    context: DmlContext<'_>,
    ins: &InsertStmt,
    params: &[SqlValue],
    returning_opts: ReturningOpts<'_>,
) -> Vec<BackendMessage> {
    use std::collections::HashMap;

    let DmlContext {
        engine,
        mvcc,
        schema,
        default_schema,
        txn,
        jsonb_limits,
    } = context;

    let ReturningOpts {
        with_row_description,
        result_formats,
    } = returning_opts;

    // Re-borrowed per row below: each row is its own mutation, buffered into the
    // open transaction when there is one.
    let mut txn = txn;

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

    // Multi-row INSERT is PARSED but must not be EXECUTED yet.
    //
    // The SQL layer is proven correct: this loop writes all three rows in-process,
    // via autocommit AND via the buffered write-set, and the mvcc/server multi-row
    // tests pass. On the live cluster the SAME statement reports `INSERT 0 3` and
    // writes only row 1, reproduced twice on a fresh table — so the loss is in the
    // distributed apply path, below this layer, not here.
    //
    // Until that is found, fail loud. A silent row-drop announced as `INSERT 0 3` is
    // strictly worse than an error. See the work item for the disproof of the two
    // hypotheses already ruled out (accumulator reuse; the buffered write-set).
    if ins.rows.len() != 1 {
        return vec![error_response(
            "0A000",
            &format!(
                "multi-row INSERT is parsed but not yet executed ({} rows); \
                 executing it drops rows below the SQL layer",
                ins.rows.len()
            ),
        )];
    }

    // The statement's own tag: one INSERT of N rows, not N INSERTs of one.
    let tag = format!("INSERT 0 {}", ins.rows.len());
    let mut combined_returning: Option<QueryResult> = None;
    let mut last_write: Vec<BackendMessage> = Vec::new();
    for row_index in 0..ins.rows.len() {
        // Resolve THIS row's named columns (per CQL type), substituting bound params;
        // collect regular/static cells by storage index, the CQL values by name for key
        // ordering, and the SQL values by name for RETURNING.
        //
        // These MUST be built per row. Declaring them outside the loop lets
        // `regular_cells` accumulate every earlier row's cells, so row 2 carries row
        // 1's values as well — a row-count assertion does not catch that, which is
        // exactly how the first attempt shipped a defect.
        let mut col_values: HashMap<String, CqlValue> = HashMap::new();
        let mut sql_values: HashMap<String, SqlValue> = HashMap::new();
        let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
        for (i, col_name) in ins.columns.iter().enumerate() {
            let (col_meta, sql_value, value) = match resolve_dml_value(
                meta,
                schema,
                ks,
                &ins.table.table,
                col_name,
                &ins.rows[row_index][i],
                ParamCtx {
                    params,
                    jsonb_limits,
                },
            ) {
                Ok(r) => r,
                Err(msg) => return vec![msg],
            };
            if matches!(col_meta.kind, ColumnKind::Regular | ColumnKind::Static) {
                if let Some(idx) = meta.storage_column_index(col_name) {
                    regular_cells.push((idx, value.clone()));
                }
            }
            col_values.insert(col_name.clone(), value);
            sql_values.insert(col_name.clone(), sql_value);
        }

        // Enforce every FOREIGN KEY on this table BEFORE the write. The check is a normal read
        // of the parent (a point read, or a lookup through the parent-key index), so it lands in
        // the read set a serializable COMMIT validates — a concurrent parent delete bumps the
        // parent's table epoch and fails that commit (`mvcc::validate_snapshot`). A NULL
        // referencing value satisfies MATCH SIMPLE and is skipped, as in PostgreSQL.
        if let Err((code, message)) =
            crate::pg_fk::check_child_row(engine, schema, meta, &col_values)
        {
            return vec![error_response(&code, &message)];
        }

        // Partition-key and clustering values in key order — all required for INSERT.
        let mut pk_values = Vec::with_capacity(meta.partition_key.len());
        for name in &meta.partition_key {
            match col_values.get(name) {
                Some(v) => pk_values.push(v.clone()),
                // The synthetic key is ferrosa's own: the client never supplies it, because
                // it is filtered out of `SELECT *` and of the column list it was given. Mint
                // one for this row instead of demanding a column the user cannot see.
                None if is_synthetic_key_column(name) => {
                    pk_values.push(CqlValue::Uuid(uuid::Uuid::from_bytes(next_synthetic_key())));
                }
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

        // Build the RETURNING result BEFORE the write, from the in-memory values, so
        // a missing RETURNING column fails loud without a half-applied side effect.
        let returning = match ins.returning.as_ref() {
            Some(r) => {
                match build_insert_returning(meta, schema, ks, &ins.table.table, r, &sql_values) {
                    Ok(rows) => Some(rows),
                    Err(msg) => return vec![msg],
                }
            }
            None => None,
        };

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
        // Write (or buffer into the open transaction) via the shared seam. On any
        // error — including the write-set cap — `apply_or_buffer` returns an
        // `ErrorResponse`; propagate it untouched (the RETURNING rows are never
        // emitted for a failed write). On success it returns `CommandComplete`
        // carrying `tag`, which is the STATEMENT's count (`INSERT 0 N`) rather than
        // this row's, so N rows produce one announcement of N. When RETURNING is
        // present the DataRows are accumulated across rows and rendered once, from
        // the in-memory values resolved above (no storage read-back). A buffered
        // write still returns its RETURNING rows now and commits at COMMIT.
        last_write =
            apply_or_buffer(engine, schema, mvcc, txn.as_deref_mut(), mutation, &tag).await;
        // A failed row kills the whole statement. Report it untouched rather than
        // announcing a count for rows that were not all written.
        if last_write
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
        {
            return last_write;
        }
        if let Some(result) = returning {
            match &mut combined_returning {
                Some(acc) => acc.rows.extend(result.rows),
                None => combined_returning = Some(result),
            }
        }
    }

    // ONE result for the whole statement: a single RowDescription, one DataRow per
    // inserted row, and a single `INSERT 0 N` tag — never N concatenated results.
    match combined_returning {
        Some(result) => render_dml_returning(result, with_row_description, result_formats, tag),
        None => last_write,
    }
}

/// Resolve a `RETURNING` clause for an `INSERT` into the output column set + the
/// single returned row, from the values just written (no storage read-back).
/// `RETURNING *` expands to every table column in schema order; a named column
/// not present in the statement's value list is reported as NULL (it was not
/// supplied — Cassandra has no row to read a default from). A `RETURNING` of a
/// column that does not exist in the table fails loud (`42703`).
fn build_insert_returning(
    meta: &ferrosa_schema::TableMetadata,
    schema: &Schema,
    ks: &str,
    table: &str,
    returning: &Returning,
    sql_values: &std::collections::HashMap<String, SqlValue>,
) -> Result<QueryResult, BackendMessage> {
    let names: Vec<String> = match returning {
        Returning::Star => meta.columns.keys().cloned().collect(),
        Returning::Columns(cols) => cols.clone(),
    };
    let mut columns = Vec::with_capacity(names.len());
    let mut row = Vec::with_capacity(names.len());
    for name in &names {
        let col_meta = meta.columns.get(name).ok_or_else(|| {
            error_response(
                "42703",
                &format!("column \"{name}\" of relation \"{table}\" does not exist in RETURNING"),
            )
        })?;
        let cql_type =
            ferrosa_row_bridge::parse_cql_type_in_keyspace(&col_meta.column_type, ks, schema)
                .map_err(|e| error_response("42704", &e.to_string()))?;
        columns.push(Column::new(
            name.clone(),
            crate::pg_types::pg_type_of(&cql_type).column_type,
        ));
        row.push(sql_values.get(name).cloned().unwrap_or(SqlValue::Null));
    }
    Ok(QueryResult {
        columns,
        rows: vec![Row(row)],
    })
}

/// The parameter type OIDs to advertise for a parameterized DML statement in
/// `ParameterDescription`. The COUNT is the number of distinct `$N` placeholders
/// (driven by the highest index `N` referenced), so a driver like tokio-postgres
/// — which does NOT pre-declare OIDs in `Parse` and learns the count from this
/// reply — binds the right number of parameters. Each OID is the client-declared
/// type where given (non-zero), else `0` (unspecified ⇒ decode leniently at
/// Bind). No column-from-comparison inference is attempted for DML.
///
/// Fails loud (`08P01`) on a non-contiguous placeholder set (e.g. `$1, $3` with
/// no `$2`): Postgres requires `$1..$N` dense, and a gap would silently bind the
/// wrong value.
pub(crate) fn dml_param_oids(
    placeholders: &[usize],
    declared: &[i32],
) -> Result<Vec<i32>, BackendMessage> {
    let max = placeholders.iter().copied().max().unwrap_or(0);
    if max == 0 {
        return Ok(Vec::new());
    }
    // Every index 1..=max must be present (Postgres requires dense $1..$N).
    for n in 1..=max {
        if !placeholders.contains(&n) {
            return Err(error_response(
                "08P01",
                &format!("parameter ${n} is referenced out of order or missing in $1..${max}"),
            ));
        }
    }
    Ok((1..=max)
        .map(|n| match declared.get(n - 1).copied() {
            Some(oid) if oid != 0 => oid,
            _ => 0,
        })
        .collect())
}

/// Collect the `$N` placeholder indices referenced by an `INSERT`'s VALUES.
pub(crate) fn insert_placeholders(ins: &InsertStmt) -> Vec<usize> {
    ins.rows
        .iter()
        .flatten()
        .filter_map(scalar_param_index)
        .collect()
}

/// Collect the `$N` placeholder indices referenced by an `UPDATE` (SET + WHERE).
pub(crate) fn update_placeholders(upd: &UpdateStmt) -> Vec<usize> {
    upd.assignments
        .iter()
        .chain(upd.where_eq.iter())
        .filter_map(|(_, sv)| scalar_param_index(sv))
        .collect()
}

/// Collect the `$N` placeholder indices referenced by a `DELETE`'s WHERE.
pub(crate) fn delete_placeholders(del: &DeleteStmt) -> Vec<usize> {
    del.where_eq
        .iter()
        .filter_map(|(_, sv)| scalar_param_index(sv))
        .collect()
}

/// The 1-based `$N` index of a scalar, if it is a parameter placeholder.
fn scalar_param_index(sv: &ScalarValue) -> Option<usize> {
    match sv {
        ScalarValue::Param(n) => Some(*n),
        _ => None,
    }
}

/// `(placeholder index, target column name)` pairs for an `INSERT`: each `$N` in
/// VALUES is bound to the column at the same position in the column list.
fn insert_param_targets(ins: &InsertStmt) -> Vec<(usize, &str)> {
    ins.rows
        .iter()
        .flat_map(|row| ins.columns.iter().zip(row.iter()))
        .filter_map(|(col, sv)| scalar_param_index(sv).map(|n| (n, col.as_str())))
        .collect()
}

/// `(placeholder index, target column name)` pairs for an `UPDATE`: SET and
/// WHERE `$N`s are bound to the column on the left of each `=`.
fn update_param_targets(upd: &UpdateStmt) -> Vec<(usize, &str)> {
    upd.assignments
        .iter()
        .chain(upd.where_eq.iter())
        .filter_map(|(col, sv)| scalar_param_index(sv).map(|n| (n, col.as_str())))
        .collect()
}

/// `(placeholder index, target column name)` pairs for a `DELETE` WHERE.
fn delete_param_targets(del: &DeleteStmt) -> Vec<(usize, &str)> {
    del.where_eq
        .iter()
        .filter_map(|(col, sv)| scalar_param_index(sv).map(|n| (n, col.as_str())))
        .collect()
}

/// Infer the parameter type OIDs for a parameterized DML statement by resolving
/// each `$N` against its TARGET COLUMN's type in the table schema. This lets a
/// driver (tokio-postgres) serialize bound values with a concrete type instead
/// of probing the catalog for an unspecified (`0`) OID. The client-declared OID
/// wins where it is non-zero; otherwise the column's type drives it; a target we
/// cannot resolve falls back to `0` (lenient). FAILS LOUD on a non-dense `$1..$N`
/// placeholder set (via [`dml_param_oids`]).
///
/// `targets` maps each present placeholder to its column name; `meta` is the
/// table metadata; `declared` the client-declared OIDs from Parse.
fn infer_dml_param_oids(
    targets: &[(usize, &str)],
    meta: &ferrosa_schema::TableMetadata,
    schema: &Schema,
    ks: &str,
    declared: &[i32],
) -> Result<Vec<i32>, BackendMessage> {
    let placeholders: Vec<usize> = targets.iter().map(|(n, _)| *n).collect();
    // Validate density + length and seed with declared/0 first.
    let mut oids = dml_param_oids(&placeholders, declared)?;
    // Then refine each `0` (unspecified) slot from its target column's type.
    for (n, col_name) in targets {
        let idx = n - 1;
        if oids.get(idx).copied() != Some(0) {
            continue; // a non-zero client-declared OID already won
        }
        if let Some(col_meta) = meta.columns.get(*col_name) {
            // An unresolvable column type is refused, not left as an
            // unspecified (0) parameter OID.
            let pg = crate::pg_types::pg_type_of_column(&col_meta.column_type, ks, schema)
                .map_err(|e| error_response("42704", &e.to_string()))?;
            oids[idx] = column_type_oid(pg.column_type);
        }
    }
    Ok(oids)
}

/// Resolve a DML statement's table metadata for parameter inference, or a
/// fail-loud `42P01` if the table does not exist. Returns the OIDs inferred from
/// each `$N`'s target column.
pub(crate) fn infer_insert_param_oids(
    schema: &Schema,
    ins: &InsertStmt,
    default_schema: &str,
    declared: &[i32],
) -> Result<Vec<i32>, BackendMessage> {
    let ks = ins.table.schema.as_deref().unwrap_or(default_schema);
    let snap = schema.snapshot();
    let Some(meta) = snap.tables.get(&(ks.to_string(), ins.table.table.clone())) else {
        // Defer the table-existence error to Describe/Execute; here just fall
        // back to declared/0 OIDs so the count is still correct.
        return dml_param_oids(&insert_placeholders(ins), declared);
    };
    infer_dml_param_oids(&insert_param_targets(ins), meta, schema, ks, declared)
}

/// As [`infer_insert_param_oids`] for an `UPDATE`.
pub(crate) fn infer_update_param_oids(
    schema: &Schema,
    upd: &UpdateStmt,
    default_schema: &str,
    declared: &[i32],
) -> Result<Vec<i32>, BackendMessage> {
    let ks = upd.table.schema.as_deref().unwrap_or(default_schema);
    let snap = schema.snapshot();
    let Some(meta) = snap.tables.get(&(ks.to_string(), upd.table.table.clone())) else {
        return dml_param_oids(&update_placeholders(upd), declared);
    };
    infer_dml_param_oids(&update_param_targets(upd), meta, schema, ks, declared)
}

/// As [`infer_insert_param_oids`] for a `DELETE`.
pub(crate) fn infer_delete_param_oids(
    schema: &Schema,
    del: &DeleteStmt,
    default_schema: &str,
    declared: &[i32],
) -> Result<Vec<i32>, BackendMessage> {
    let ks = del.table.schema.as_deref().unwrap_or(default_schema);
    let snap = schema.snapshot();
    let Some(meta) = snap.tables.get(&(ks.to_string(), del.table.table.clone())) else {
        return dml_param_oids(&delete_placeholders(del), declared);
    };
    infer_dml_param_oids(&delete_param_targets(del), meta, schema, ks, declared)
}

/// Resolve the output columns of an `INSERT ... RETURNING` for the extended
/// `Describe('S')` reply (a `RowDescription`). Returns `Ok(None)` when the
/// statement has no `RETURNING` (the caller emits `NoData`), or the resolved
/// column descriptors. Fails loud on an unknown table (`42P01`) or an unknown
/// RETURNING column (`42703`) — the same errors Execute would raise, surfaced at
/// Describe time so the driver learns the shape before binding.
pub(crate) fn describe_insert_returning(
    schema: &Schema,
    ins: &InsertStmt,
    default_schema: &str,
) -> Result<Option<Vec<Column>>, BackendMessage> {
    let Some(returning) = ins.returning.as_ref() else {
        return Ok(None);
    };
    let ks = ins.table.schema.as_deref().unwrap_or(default_schema);
    let snap = schema.snapshot();
    let meta = snap
        .tables
        .get(&(ks.to_string(), ins.table.table.clone()))
        .ok_or_else(|| {
            error_response(
                "42P01",
                &format!("relation \"{ks}.{}\" does not exist", ins.table.table),
            )
        })?;
    let names: Vec<String> = match returning {
        Returning::Star => meta.columns.keys().cloned().collect(),
        Returning::Columns(cols) => cols.clone(),
    };
    let mut columns = Vec::with_capacity(names.len());
    for name in &names {
        let col_meta = meta.columns.get(name).ok_or_else(|| {
            error_response(
                "42703",
                &format!(
                    "column \"{name}\" of relation \"{}\" does not exist in RETURNING",
                    ins.table.table
                ),
            )
        })?;
        let cql_type =
            ferrosa_row_bridge::parse_cql_type_in_keyspace(&col_meta.column_type, ks, schema)
                .map_err(|e| error_response("42704", &e.to_string()))?;
        columns.push(Column::new(
            name.clone(),
            crate::pg_types::pg_type_of(&cql_type).column_type,
        ));
    }
    Ok(Some(columns))
}

/// Render a DML `RETURNING` result into backend messages: an optional leading
/// `RowDescription` (simple path), the `DataRow`(s) encoded per `result_formats`
/// (the Postgres fan-out rule — empty ⇒ all text), then `CommandComplete` with
/// the supplied `tag`. The extended Execute path passes `with_row_description =
/// false` (the client learned the columns from `Describe`) AND the portal's
/// chosen result formats, so a driver requesting binary results (tokio-postgres)
/// decodes the RETURNING row correctly.
fn render_dml_returning(
    result: QueryResult,
    with_row_description: bool,
    result_formats: &[i16],
    tag: String,
) -> Vec<BackendMessage> {
    let mut out = Vec::with_capacity(result.rows.len() + 2);
    if with_row_description {
        out.push(BackendMessage::RowDescription {
            fields: row_description_fields(&result.columns, result_formats),
        });
    }
    let col_types: Vec<ColumnType> = result.columns.iter().map(|c| c.ty).collect();
    for row in &result.rows {
        let columns = match row
            .0
            .iter()
            .enumerate()
            .map(|(i, v)| encode_value(result_format_for(result_formats, i), col_types[i], v))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(columns) => columns,
            Err(error) => return vec![encode_error_response(&error)],
        };
        out.push(BackendMessage::DataRow { columns });
    }
    out.push(BackendMessage::CommandComplete { tag });
    out
}

/// What a DML value is resolved against: the bound parameters, and the jsonb
/// ingest limits that gate text coerced into a jsonb column.
#[derive(Clone, Copy)]
struct ParamCtx<'a> {
    params: &'a [SqlValue],
    jsonb_limits: &'a ferrosa_jsonb::Limits,
}

/// Resolve a `(column, value)` pair from a DML statement to its `CqlValue`,
/// substituting any bound `$N` parameter from `params` first (see
/// [`substitute_param`]), then looking up the column's CQL type from `meta`.
/// Returns the column metadata alongside so the caller can classify it (key vs
/// regular), and the resolved [`SqlValue`] so the caller can build a `RETURNING`
/// row without re-reading storage. A simple-protocol caller passes `&[]`, so a
/// `$N` there fails loud (no value bound).
fn resolve_dml_value<'a>(
    meta: &'a ferrosa_schema::TableMetadata,
    schema: &Schema,
    ks: &str,
    table: &str,
    col_name: &str,
    sv: &ScalarValue,
    bound: ParamCtx<'_>,
) -> Result<(&'a ferrosa_schema::ColumnMetadata, SqlValue, CqlValue), BackendMessage> {
    let col_meta = meta.columns.get(col_name).ok_or_else(|| {
        error_response(
            "42703",
            &format!("column \"{col_name}\" of relation \"{table}\" does not exist"),
        )
    })?;
    let cql_type =
        ferrosa_row_bridge::parse_cql_type_in_keyspace(&col_meta.column_type, ks, schema)
            .map_err(|e| error_response("42704", &e.to_string()))?;
    let sql_value = substitute_param(sv, bound.params)?;
    let value = value_to_cql(&sql_value, &cql_type, bound.jsonb_limits)?;
    Ok((col_meta, sql_value, value))
}

/// Execute a single-row `UPDATE`: a Cassandra-style upsert. The equality `WHERE`
/// supplies the full primary key (which identifies the row); `SET` supplies the
/// regular/static cells. Returns `CommandComplete "UPDATE 1"` — the engine write
/// is a blind upsert, so the affected-row count is reported as 1 when the write
/// lands (Cassandra has no match count; this is the documented semantic).
pub(crate) async fn execute_update(
    context: DmlContext<'_>,
    upd: &UpdateStmt,
    params: &[SqlValue],
) -> Vec<BackendMessage> {
    use std::collections::HashMap;

    let DmlContext {
        engine,
        mvcc,
        schema,
        default_schema,
        txn,
        jsonb_limits,
    } = context;

    // UPDATE ... RETURNING is out of scope for this PR (only INSERT RETURNING is
    // wired). Fail loud rather than silently dropping the clause.
    if upd.returning.is_some() {
        return vec![error_response(
            "0A000",
            "UPDATE ... RETURNING is not yet supported",
        )];
    }

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
    // The assigned values, keyed by column, so a FOREIGN KEY column that is being SET can be
    // re-checked against its parent before the write (a foreign key on a column the UPDATE does
    // not touch keeps its already-valid old value and needs no check).
    let mut assigned: HashMap<String, CqlValue> = HashMap::new();
    for (col_name, sv) in &upd.assignments {
        let (col_meta, _sql_value, value) = match resolve_dml_value(
            meta,
            schema,
            ks,
            table,
            col_name,
            sv,
            ParamCtx {
                params,
                jsonb_limits,
            },
        ) {
            Ok(r) => r,
            Err(msg) => return vec![msg],
        };
        if !matches!(col_meta.kind, ColumnKind::Regular | ColumnKind::Static) {
            return vec![error_response(
                "0A000",
                &format!("cannot UPDATE key column \"{col_name}\" in SET"),
            )];
        }
        assigned.insert(col_name.clone(), value.clone());
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
        let (col_meta, _sql_value, value) = match resolve_dml_value(
            meta,
            schema,
            ks,
            table,
            col_name,
            sv,
            ParamCtx {
                params,
                jsonb_limits,
            },
        ) {
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

    // Enforce every FOREIGN KEY on a column this UPDATE assigns, before the write.
    if let Err((code, message)) = crate::pg_fk::check_child_row(engine, schema, meta, &assigned) {
        return vec![error_response(&code, &message)];
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
    apply_or_buffer(engine, schema, mvcc, txn, mutation, "UPDATE 1").await
}

/// Execute a single-row `DELETE`: a row-level tombstone. The equality `WHERE`
/// supplies the full primary key identifying the row. Returns `CommandComplete
/// "DELETE 1"` — the engine writes a tombstone unconditionally, so the count is
/// reported as 1 when the write lands (Cassandra has no match count).
pub(crate) async fn execute_delete(
    context: DmlContext<'_>,
    del: &DeleteStmt,
    params: &[SqlValue],
) -> Vec<BackendMessage> {
    use std::collections::HashMap;

    let DmlContext {
        engine,
        mvcc,
        schema,
        default_schema,
        txn,
        jsonb_limits,
    } = context;

    // DELETE ... RETURNING is out of scope for this PR. Fail loud.
    if del.returning.is_some() {
        return vec![error_response(
            "0A000",
            "DELETE ... RETURNING is not yet supported",
        )];
    }

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
        let (col_meta, _sql_value, value) = match resolve_dml_value(
            meta,
            schema,
            ks,
            table,
            col_name,
            sv,
            ParamCtx {
                params,
                jsonb_limits,
            },
        ) {
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

    // Parent-side enforcement: refuse the DELETE while any child still references this row. The
    // probe is an index lookup on the child's FK index (built by `ADD FOREIGN KEY`), and it is a
    // normal read, so it lands in the read set a serializable COMMIT validates.
    if let Err((code, message)) = crate::pg_fk::check_parent_row(engine, &snap, meta, &key_values) {
        return vec![error_response(&code, &message)];
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
    apply_or_buffer(engine, schema, mvcc, txn, mutation, "DELETE 1").await
}

/// The context a no-`FROM` expression select is evaluated in.
///
/// Beyond the schema name the info functions report, a scalar subquery must be
/// *executed* against storage, so the read environment (engine, schema, MVCC
/// snapshot) travels with the scalar items. `read` is `None` only in a context
/// with no storage at all — the expression-select unit tests, which exercise
/// literals/functions/`||` — and a subquery there is refused with `0A000`
/// rather than guessed (the same convention as [`ReadEnv::ddl`]).
#[derive(Clone, Copy)]
pub(crate) struct ScalarReadCtx<'a> {
    /// The schema name bare table names resolve under, and the value
    /// `current_database()` reports.
    pub(crate) default_schema: &'a str,
    /// The environment a scalar subquery reads against; `None` = no storage.
    pub(crate) read: Option<ReadEnv<'a>>,
    /// The session's uncommitted write-set (empty outside a transaction), so an
    /// inner query sees the caller's own uncommitted rows.
    pub(crate) pending_writes: Option<&'a [PgWrite]>,
}

impl<'a> ScalarReadCtx<'a> {
    /// The wire path's context: storage available, schema name taken from `env`.
    pub(crate) fn new(env: ReadEnv<'a>, pending_writes: Option<&'a [PgWrite]>) -> Self {
        Self {
            default_schema: env.default_schema,
            read: Some(env),
            pending_writes,
        }
    }
}

/// Evaluate a no-`FROM` expression SELECT (`SELECT 1`, `SELECT version()`,
/// `SELECT current_database()`, `SELECT 'a' || 'b'`, `SELECT (SELECT count(*)
/// FROM t)`) into a one-row [`QueryResult`]. Literals are returned as-is; a
/// small set of info/session functions are evaluated from the connection's
/// context; `||` concatenates its operands as text; a scalar subquery runs its
/// inner query and takes its single value.
pub(crate) async fn execute_scalar_select(
    items: &[ScalarItem],
    ctx: ScalarReadCtx<'_>,
) -> Result<QueryResult, BackendMessage> {
    let mut columns = Vec::with_capacity(items.len());
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        // Value and type come back together: a scalar subquery's type is its
        // inner query's single output column type, known only once the inner
        // query is resolved, so the two cannot be computed independently.
        let (value, ty) = eval_scalar_value(&item.value, ctx).await?;
        // Type from the EXPRESSION, not from the value: a concatenation is `text`
        // even when it evaluates to SQL NULL, so the `RowDescription` advertises
        // OID 25 either way and the client decodes the value correctly.
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

/// Evaluate one scalar select value to its `(value, output column type)`,
/// recursing through `||` and into a scalar subquery.
///
/// A `$N` placeholder is only bound on the extended-query path, which routes
/// expression selects through `Parse` (where such a value is refused) — so it is
/// a fail-loud error here, never a guess.
///
/// The `||` arm boxes the recursive call: an `async fn` cannot await itself
/// directly (E0733), and a concatenation tree is the one recursive shape here.
async fn eval_scalar_value(
    value: &ScalarValue,
    ctx: ScalarReadCtx<'_>,
) -> Result<(SqlValue, ColumnType), BackendMessage> {
    match value {
        ScalarValue::Literal(v) => Ok((v.clone(), value_column_type(v))),
        ScalarValue::Func(name) => Ok((
            eval_scalar_func(name, ctx.default_schema)?,
            ColumnType::Text,
        )),
        ScalarValue::Param(_) => Err(error_response(
            "0A000",
            "$N parameters require the extended-query protocol",
        )),
        ScalarValue::Concat { left, right } => {
            let (left, _) = Box::pin(eval_scalar_value(left, ctx)).await?;
            let (right, _) = Box::pin(eval_scalar_value(right, ctx)).await?;
            Ok((concat_text(&left, &right)?, ColumnType::Text))
        }
        ScalarValue::Cast { value, ty } => {
            let (inner, _) = Box::pin(eval_scalar_value(value, ctx)).await?;
            let Some(env) = ctx.read else {
                return Err(error_response(
                    "0A000",
                    "a `::` cast in an expression requires a storage context",
                ));
            };
            // Same semantics as the WHERE path: the cast's meaning is the front
            // end's, and an unresolvable `regclass` name is an error, not NULL.
            let out = apply_cast(&inner, *ty, env.schema, env.default_schema)?;
            Ok((out, ColumnType::Int))
        }
        ScalarValue::Subquery(stmt) => eval_scalar_subquery(stmt, ctx).await,
    }
}

/// Run a scalar subquery `( SELECT ... )` and return its single value and its
/// output column type (PostgreSQL `EXPR_SUBLINK` semantics).
///
/// The inner query reads the same storage and MVCC snapshot the outer select
/// does: [`open_select_stream`] loads its tables (a missing one is `42P01`,
/// never an empty scan) and runs the executor off the async worker.
///
/// Cardinality matches PostgreSQL exactly (both SQLSTATEs taken from the
/// PostgreSQL sources, `parse_expr.c` and `nodeSubplan.c`):
/// - zero rows -> SQL NULL (distinct from an empty string);
/// - more than one row -> `21000 cardinality_violation`;
/// - more than one column -> `42601 syntax_error` ("subquery must return only one
///   column"), refused BEFORE any row is read, as PostgreSQL's parser does — the
///   first column is never silently taken.
///
/// The `SelectStmt` is cloned into the executor because it is borrowed from the
/// parsed statement but the executor takes ownership; there is no read-only
/// entry point that would avoid it.
async fn eval_scalar_subquery(
    stmt: &SelectStmt,
    ctx: ScalarReadCtx<'_>,
) -> Result<(SqlValue, ColumnType), BackendMessage> {
    let Some(env) = ctx.read else {
        return Err(error_response(
            "0A000",
            "a scalar subquery requires a storage context",
        ));
    };
    let mut stream = open_select_stream(env, stmt.clone(), ctx.pending_writes, Vec::new()).await?;
    // The output column count is known before any row: refuse a multi-column
    // subquery here, exactly as PostgreSQL's parser does, never reading a row.
    let ty = match stream.columns() {
        [column] => column.ty,
        _ => {
            return Err(error_response(
                "42601",
                "subquery must return only one column",
            ))
        }
    };
    // Read up to two rows: the second is the cardinality violation, proved
    // without scanning the rest of the relation. The stream is dropped (which cancels
    // the scan) either way.
    let first = stream.next_row().await?;
    if stream.next_row().await?.is_some() {
        return Err(error_response(
            "21000",
            "more than one row returned by a subquery used as an expression",
        ));
    }
    let value = match first {
        Some(row) => row.0.into_iter().next().ok_or_else(|| {
            error_response(
                "XX000",
                "internal error: a one-column subquery row had no value",
            )
        })?,
        None => SqlValue::Null,
    };
    Ok((value, ty))
}

/// PostgreSQL's `||` text concatenation over already-evaluated operands.
///
/// NULL on either side yields NULL — the real behaviour difference from an empty
/// string, so it is decided here rather than by rendering. A non-text operand is
/// rendered with its Postgres text output form (`1 || '2'` is `'12'`), which is
/// exactly what the wire encoder would send for it; an operand with no text
/// rendering (jsonpath, `text[]`) fails loud instead of being dropped.
fn concat_text(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, BackendMessage> {
    let (Some(left), Some(right)) = (
        render_value(left).map_err(|e| encode_error_response(&e))?,
        render_value(right).map_err(|e| encode_error_response(&e))?,
    ) else {
        return Ok(SqlValue::Null);
    };
    let mut bytes = left;
    bytes.extend_from_slice(&right);
    let text = String::from_utf8(bytes).map_err(|_| {
        error_response(
            "0A000",
            "concatenation produced bytes with no text representation",
        )
    })?;
    Ok(SqlValue::Text(text))
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
        SqlValue::Jsonb(_) => ColumnType::Jsonb,
        SqlValue::JsonPath(_) => ColumnType::JsonPath,
        SqlValue::TextArray(_) => ColumnType::TextArray,
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

/// Open every table referenced by `stmt` (FROM + optional JOIN) as a streaming
/// provider in a [`MapCatalog`], so the sync engine can scan them. Returns the
/// catalog together with the [`ScanFailure`] slot its providers share, or a
/// single fail-loud [`BackendMessage::ErrorResponse`] (undefined table `42P01`
/// or storage error `58000`) — never a silently-empty relation.
///
/// This reads **no rows**. Table data is pulled only when the executor scans,
/// which is also why the Describe path below can resolve a statement's columns
/// without touching storage at all.
///
/// The returned `ScanFailure` MUST be checked after execution — see
/// [`check_scan_failure`]. A storage error mid-scan cannot travel back through
/// the executor's `Iterator`, so an unchecked slot is a truncated result set
/// reported as success.
///
/// Shared by the simple-query path ([`execute_query`]) and the extended-query
/// path (Describe/Execute), so both resolve tables identically.
pub(crate) async fn load_catalog(
    engine: &Arc<StorageEngine>,
    schema: &Schema,
    stmt: &ferrosa_sql::SelectStmt,
    default_schema: &str,
) -> Result<(MapCatalog, ScanFailure), BackendMessage> {
    load_catalog_with_mvcc(engine, schema, stmt, default_schema, None, None, None).await
}

pub(crate) async fn load_catalog_with_mvcc(
    engine: &Arc<StorageEngine>,
    schema: &Schema,
    stmt: &ferrosa_sql::SelectStmt,
    default_schema: &str,
    mvcc: Option<&MvccManager>,
    snapshot: Option<&MvccSnapshot>,
    pending_writes: Option<&[PgWrite]>,
) -> Result<(MapCatalog, ScanFailure), BackendMessage> {
    let mut catalog = MapCatalog::new();
    if let (Some(mvcc), Some(snapshot)) = (mvcc, snapshot) {
        if mvcc
            .validate_commit(snapshot, &std::collections::HashSet::new())
            .is_err()
        {
            return Err(error_response(
                "40001",
                "PostgreSQL transaction snapshot expired",
            ));
        }
    }
    let scan_buffer_rows = mvcc.map_or(SCAN_BUFFER_ROWS, MvccManager::scan_buffer_rows);
    // One slot per query: every provider records into it, and the query layer
    // takes it once after `execute` returns.
    let failure = ScanFailure::default();
    let referenced: Vec<&ferrosa_sql::ast::TableRef> = std::iter::once(&stmt.from)
        .chain(stmt.join.as_ref().map(|j| &j.table))
        .collect();
    // `pg_catalog.<relation>` is not stored: it is a projection built from the live
    // schema. Build the six catalog tables once when a referenced relation asks for
    // one, and serve each from that projection — an unresolvable column type is a
    // PgTypeError, reported rather than advertising an unresolvable type to a client.
    let catalog_tables = if referenced
        .iter()
        .any(|t| t.schema.as_deref().unwrap_or(default_schema) == PG_CATALOG)
    {
        Some(
            crate::catalog::catalog_tables(schema)
                .map_err(|e| error_response("42704", &e.to_string()))?,
        )
    } else {
        None
    };
    for table_ref in referenced {
        let keyspace = table_ref.schema.as_deref().unwrap_or(default_schema);
        if keyspace == PG_CATALOG {
            let Some(table) = catalog_tables
                .as_ref()
                .and_then(|tables| tables.iter().find(|(name, _)| name == &table_ref.table))
                .map(|(_, table)| table)
            else {
                return Err(error_response(
                    "42P01",
                    &format!("relation \"{keyspace}.{}\" does not exist", table_ref.table),
                ));
            };
            catalog = catalog.with_table(keyspace, &table_ref.table, Arc::new(table.clone()));
            continue;
        }
        let table_name = format!("{keyspace}.{}", table_ref.table);
        let mut overlay = match (mvcc, snapshot) {
            (Some(mvcc), Some(snapshot)) => mvcc.table_overlay(snapshot, &table_name),
            _ => Default::default(),
        };
        if let Some(writes) = pending_writes {
            if let Err(error) = crate::storage_provider::apply_pending_writes(
                engine,
                schema,
                keyspace,
                &table_ref.table,
                &mut overlay,
                writes,
            ) {
                return Err(error_response(
                    "58000",
                    &format!("transaction overlay failed: {error}"),
                ));
            }
        }
        match load_table_with_overlay(
            engine,
            schema,
            keyspace,
            &table_ref.table,
            failure.clone(),
            overlay,
            scan_buffer_rows,
        )
        .await
        {
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
    Ok((catalog, failure))
}

/// Turn a recorded scan failure into a fail-loud `ErrorResponse`, discarding
/// whatever partial result the executor built on top of the truncated scan.
///
/// Call this after every `execute`, ahead of trusting either arm of its result:
/// a scan that died part-way leaves the executor with a short row set it has no
/// way to know is short, so `Ok(result)` here means "these are all the rows that
/// arrived", not "these are all the rows". The storage error is the real cause
/// and outranks any `ExecError` computed from the fragment.
pub(crate) fn check_scan_failure(failure: &ScanFailure) -> Option<BackendMessage> {
    failure
        .take()
        .map(|msg| error_response("58000", &format!("{msg} (query aborted; result discarded)")))
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
            if result_formats.len() > 1 && result_formats.len() != result.columns.len() {
                return vec![error_response(
                    "08P01",
                    "Bind result format count must be zero, one, or match the result column count",
                )];
            }
            let col_types: Vec<ColumnType> = result.columns.iter().map(|c| c.ty).collect();
            let nrows = result.rows.len();
            let mut out = Vec::with_capacity(nrows + 1);
            for row in &result.rows {
                let columns = match row
                    .0
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        encode_value(result_format_for(result_formats, i), col_types[i], v)
                    })
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(columns) => columns,
                    Err(error) => return vec![encode_error_response(&error)],
                };
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

    /// `TableKeyCache` must hand back the SAME table identity without allocating
    /// while the table is unchanged, and a fresh one when it changes — including in
    /// an interleaved order. This is what removes two `String` heap allocations per
    /// mutation from the COMMIT's prepare phase.
    #[test]
    fn table_key_cache_reuses_the_identity_until_the_table_changes() {
        let mut cache = TableKeyCache { current: None };

        let a = cache.key_for("ks", "a");
        assert_eq!(&*a.0, "ks");
        assert_eq!(&*a.1, "a");

        // The SAME table must reuse the same allocation (no rebuild).
        let a_again = cache.key_for("ks", "a");
        assert!(
            std::sync::Arc::ptr_eq(&a.0, &a_again.0) && std::sync::Arc::ptr_eq(&a.1, &a_again.1),
            "an unchanged table must reuse its cached identity, not allocate a new one"
        );

        // A different table rebuilds.
        let b = cache.key_for("ks", "b");
        assert!(!std::sync::Arc::ptr_eq(&a.1, &b.1));
        assert_eq!(&*b.1, "b");

        // Interleaved order still resolves the right identity each time.
        assert_eq!(&*cache.key_for("ks", "a").1, "a");
        assert_eq!(&*cache.key_for("ks", "b").1, "b");
        assert_eq!(&*cache.key_for("other", "b").0, "other");
    }

    fn encode_value(format: i16, col_type: ColumnType, value: &SqlValue) -> Option<Vec<u8>> {
        super::encode_value(format, col_type, value).expect("test value should encode")
    }

    /// Decode a parameter that the test expects to be valid.
    fn decode_param(format: i16, type_oid: i32, bytes: Option<&[u8]>) -> SqlValue {
        decode_param_checked(format, type_oid, bytes, &crate::jsonb_wire::test_limits())
            .expect("test parameter should decode")
    }

    fn param_err(format: i16, type_oid: i32, bytes: &[u8]) -> ParamError {
        decode_param_checked(
            format,
            type_oid,
            Some(bytes),
            &crate::jsonb_wire::test_limits(),
        )
        .expect_err("test parameter should be refused")
    }

    /// A scalar-evaluation context with no storage. Only valid for expressions
    /// that hold no scalar subquery (literals, info functions, `||`); a subquery
    /// reached through it is refused (`0A000`) rather than run, which is what the
    /// storage-less guard asserts. The end-to-end subquery tests build a context
    /// over a real engine instead (see `subquery_tests`).
    fn bare_ctx(default_schema: &str) -> ScalarReadCtx<'_> {
        ScalarReadCtx {
            default_schema,
            read: None,
            pending_writes: None,
        }
    }

    #[test]
    fn pg_param_unknown_oid_is_refused() {
        // jsonpath, timestamptz, bpchar, an int4 array and a made-up OID have
        // no mapping: refused in both formats, never decoded as text. (jsonb
        // and json are mapped since T-161a.)
        for oid in [4072, 1184, 1042, 1007, 999_999] {
            for format in [0, 1] {
                let err = param_err(format, oid, b"{\"a\":1}");
                assert_eq!(err.sqlstate, "42704", "oid {oid} format {format}");
            }
        }
        // OID 0 (unspecified) stays UTF-8 text; NULL is NULL for any OID.
        assert_eq!(
            decode_param(0, 0, Some(b"raw")),
            SqlValue::Text("raw".into())
        );
        assert_eq!(decode_param(0, 3802, None), SqlValue::Null);
    }

    #[test]
    fn text_param_parse_failures_are_22p02_for_every_mapped_oid() {
        let cases: [(i32, &[u8]); 16] = [
            (23, b"not-an-integer"),
            (23, b"2147483648"),
            (21, b"40000"),
            (20, b"9223372036854775808"),
            (16, b"maybe"),
            (701, b"1.2.3"),
            (700, b""),
            (2950, b"not-a-uuid"),
            (17, b"\\xzz"),
            (17, b"deadbeef"),
            (1114, b"yesterday"),
            (1082, b"2024-13-40"),
            (1083, b"25:61:61"),
            (869, b"999.1.1.1"),
            (1700, b"12abc"),
            (25, &[0xff, 0xfe]),
        ];
        for (oid, raw) in cases {
            let err = param_err(0, oid, raw);
            assert_eq!(err.sqlstate, "22P02", "oid {oid} raw {raw:?}");
        }
    }

    #[test]
    fn bin_param_malformations_are_22p03_for_every_mapped_oid() {
        let cases: [(i32, &[u8]); 12] = [
            (23, &[1]),
            (20, &[0; 4]),
            (21, &[0; 4]),
            (16, &[]),
            (16, &[0, 0]),
            (700, &[0; 8]),
            (701, &[0; 4]),
            (2950, &[0; 15]),
            (1114, &[0; 4]),
            (1082, &[0; 8]),
            (869, &[2, 32]),
            (25, &[0xff, 0xfe]),
        ];
        for (oid, raw) in cases {
            let err = param_err(1, oid, raw);
            assert_eq!(err.sqlstate, "22P03", "oid {oid} raw {raw:?}");
        }
        // Binary numeric is unsupported, and says so.
        assert_eq!(param_err(1, 1700, &[0; 8]).sqlstate, "0A000");
    }

    #[test]
    fn numeric_text_param_accepts_exponents_and_refuses_absurd_ones() {
        use num_bigint::BigInt;
        assert_eq!(
            decode_param(0, 1700, Some(b"1e5")),
            SqlValue::numeric(BigInt::from(1), -5)
        );
        assert_eq!(
            decode_param(0, 1700, Some(b"1.25E-2")),
            SqlValue::numeric(BigInt::from(125), 4)
        );
        assert_eq!(param_err(0, 1700, b"1e999999").sqlstate, "22P02");
        assert_eq!(param_err(0, 1700, b"e5").sqlstate, "22P02");
    }

    #[test]
    fn param_errors_do_not_echo_the_value() {
        let err = param_err(0, 23, b"s3cr3t-token");
        assert!(!err.message.contains("s3cr3t"), "{}", err.message);
    }

    /// `typlen` for a column type, from the one `pg_types` map.
    fn column_type_size(ty: ColumnType) -> i16 {
        crate::pg_types::for_column_type(ty).typlen
    }

    #[test]
    fn column_type_oids_match_postgres_builtins() {
        assert_eq!(column_type_oid(ColumnType::Int), 23);
        assert_eq!(column_type_oid(ColumnType::Text), 25);
        assert_eq!(column_type_oid(ColumnType::Bool), 16);
        assert_eq!(column_type_oid(ColumnType::Float), 701); // float8
    }

    #[test]
    fn cql_bigint_columns_use_postgres_int8_on_the_wire() {
        let ty = crate::pg_types::pg_type_of(&CqlType::Bigint).column_type;
        assert_eq!(column_type_oid(ty), 20);
        assert_eq!(column_type_size(ty), 8);
        let value = i64::from(i32::MAX) + 1;
        assert_eq!(
            encode_value(1, ty, &SqlValue::Int(value)),
            Some(value.to_be_bytes().to_vec())
        );
    }

    /// `render_value` for a value that must render: the text bytes, or `None`
    /// for NULL.
    fn rv(v: &SqlValue) -> Option<Vec<u8>> {
        render_value(v).expect("test value renders")
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
        assert_eq!(rv(&SqlValue::Null), None);
        assert_eq!(rv(&SqlValue::Int(42)), Some(b"42".to_vec()));
        assert_eq!(rv(&SqlValue::Int(-7)), Some(b"-7".to_vec()));
        assert_eq!(rv(&SqlValue::Text("hi".into())), Some(b"hi".to_vec()));
        assert_eq!(rv(&SqlValue::Bool(true)), Some(b"t".to_vec()));
        assert_eq!(rv(&SqlValue::Bool(false)), Some(b"f".to_vec()));
    }

    #[test]
    fn render_value_float_text_format() {
        assert_eq!(rv(&SqlValue::float(1.5)), Some(b"1.5".to_vec()));
        assert_eq!(rv(&SqlValue::float(-0.25)), Some(b"-0.25".to_vec()));
        // Non-finite values use Postgres spellings.
        assert_eq!(rv(&SqlValue::float(f64::NAN)), Some(b"NaN".to_vec()));
        assert_eq!(
            rv(&SqlValue::float(f64::INFINITY)),
            Some(b"Infinity".to_vec())
        );
        assert_eq!(
            rv(&SqlValue::float(f64::NEG_INFINITY)),
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
    fn checked_binary_decode_rejects_epoch_shift_overflow() {
        let max_date = i32::MAX.to_be_bytes();
        let max_timestamp = i64::MAX.to_be_bytes();

        assert!(
            decode_param_checked(1, 1082, Some(&max_date), &crate::jsonb_wire::test_limits())
                .is_err()
        );
        assert!(decode_param_checked(
            1,
            1114,
            Some(&max_timestamp),
            &crate::jsonb_wire::test_limits()
        )
        .is_err());
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
            rv(&SqlValue::Uuid(u)),
            Some(b"550e8400-e29b-41d4-a716-446655440000".to_vec())
        );
    }

    #[test]
    fn bytea_text_render_is_postgres_hex() {
        assert_eq!(
            rv(&SqlValue::Bytea(vec![0xde, 0xad, 0xbe, 0xef])),
            Some(b"\\xdeadbeef".to_vec())
        );
        // Empty bytea ⇒ just the `\x` prefix.
        assert_eq!(rv(&SqlValue::Bytea(vec![])), Some(b"\\x".to_vec()));
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
        // A malformed uuid text is refused, not kept as Text.
        assert_eq!(param_err(0, 2950, b"not-a-uuid").sqlstate, "22P02");
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
            rv(&SqlValue::Timestamp(micros)),
            Some(b"2024-01-15 10:30:00".to_vec())
        );
        // .5 second ⇒ ".5" (one digit, trailing zeros trimmed).
        assert_eq!(
            rv(&SqlValue::Timestamp(micros + 500_000)),
            Some(b"2024-01-15 10:30:00.5".to_vec())
        );
        // .123 ⇒ "123".
        assert_eq!(
            rv(&SqlValue::Timestamp(micros + 123_000)),
            Some(b"2024-01-15 10:30:00.123".to_vec())
        );
        // 1 microsecond ⇒ ".000001" (all 6 digits significant).
        assert_eq!(
            rv(&SqlValue::Timestamp(micros + 1)),
            Some(b"2024-01-15 10:30:00.000001".to_vec())
        );
    }

    #[test]
    fn render_timestamp_handles_pre_1970() {
        // 1969-12-31 23:59:59.5 UTC ⇒ -500_000 micros. The fraction stays
        // non-negative (.5) and the second floors correctly.
        let micros = -500_000;
        assert_eq!(
            rv(&SqlValue::Timestamp(micros)),
            Some(b"1969-12-31 23:59:59.5".to_vec())
        );
    }

    #[test]
    fn render_date_text_form() {
        let SqlValue::Date(days) = parse_date_text("2024-01-15").unwrap() else {
            panic!("expected Date");
        };
        assert_eq!(rv(&SqlValue::Date(days)), Some(b"2024-01-15".to_vec()));
        // The Unix epoch is day 0.
        assert_eq!(rv(&SqlValue::Date(0)), Some(b"1970-01-01".to_vec()));
        // A pre-epoch (negative) day renders correctly.
        assert_eq!(rv(&SqlValue::Date(-1)), Some(b"1969-12-31".to_vec()));
    }

    #[test]
    fn render_time_text_form_trims_fraction() {
        // 10:30:00 ⇒ no fraction.
        let micros = (10 * 3600 + 30 * 60) * 1_000_000;
        assert_eq!(rv(&SqlValue::Time(micros)), Some(b"10:30:00".to_vec()));
        // + .25 second.
        assert_eq!(
            rv(&SqlValue::Time(micros + 250_000)),
            Some(b"10:30:00.25".to_vec())
        );
        // Midnight.
        assert_eq!(rv(&SqlValue::Time(0)), Some(b"00:00:00".to_vec()));
    }

    #[test]
    fn render_inet_text_is_canonical_ip() {
        use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
        assert_eq!(
            rv(&SqlValue::Inet(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)))),
            Some(b"192.168.0.1".to_vec())
        );
        assert_eq!(
            rv(&SqlValue::Inet(IpAddr::V6(Ipv6Addr::LOCALHOST))),
            Some(b"::1".to_vec())
        );
    }

    // ── Numeric text rendering ────────────────────────────────────────────

    #[test]
    fn render_numeric_text_forms() {
        use num_bigint::BigInt;
        // 123.45 (unscaled 12345, scale 2).
        assert_eq!(
            rv(&SqlValue::numeric(BigInt::from(12345), 2)),
            Some(b"123.45".to_vec())
        );
        // Integer (scale 0).
        assert_eq!(
            rv(&SqlValue::numeric(BigInt::from(42), 0)),
            Some(b"42".to_vec())
        );
        // Negative.
        assert_eq!(
            rv(&SqlValue::numeric(BigInt::from(-12345), 2)),
            Some(b"-123.45".to_vec())
        );
        // 0.05 (leading-zero fraction padding).
        assert_eq!(
            rv(&SqlValue::numeric(BigInt::from(5), 2)),
            Some(b"0.05".to_vec())
        );
        // Zero.
        assert_eq!(
            rv(&SqlValue::numeric(BigInt::from(0), 4)),
            Some(b"0".to_vec())
        );
        // Trailing-zero normalization: 1.50 ⇒ "1.5".
        assert_eq!(
            rv(&SqlValue::numeric(BigInt::from(150), 2)),
            Some(b"1.5".to_vec())
        );
        // Negative scale (value scaled up): 12 * 10^2 = 1200.
        assert_eq!(
            rv(&SqlValue::numeric(BigInt::from(12), -2)),
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
        // Malformed values are 22P02, never NULL.
        assert_eq!(param_err(0, 1114, b"not-a-time").sqlstate, "22P02");
        assert_eq!(param_err(0, 1700, b"1.2.3").sqlstate, "22P02");
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
    fn numeric_binary_encoding_fails_instead_of_sending_text_bytes() {
        use num_bigint::BigInt;
        let n = SqlValue::numeric(BigInt::from(12345), 2);
        assert!(super::encode_value(1, ColumnType::Numeric, &n).is_err());
    }

    #[test]
    fn int4_binary_encoding_rejects_values_outside_wire_range() {
        assert!(
            super::encode_value(1, ColumnType::Int, &SqlValue::Int(i64::from(i32::MAX) + 1))
                .is_err()
        );
        assert!(
            super::encode_value(1, ColumnType::Int, &SqlValue::Int(i64::from(i32::MIN) - 1))
                .is_err()
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
    fn extended_result_rejects_mismatched_format_count() {
        let result = QueryResult {
            columns: vec![Column {
                name: "id".into(),
                ty: ColumnType::Int,
            }],
            rows: vec![Row::new(vec![SqlValue::Int(1)])],
        };

        let messages = render_execute_result(Ok(result), &[0, 1]);

        assert!(matches!(
            messages.as_slice(),
            [BackendMessage::ErrorResponse { fields }]
                if fields[1] == (b'C', "08P01".to_string())
        ));
    }

    #[tokio::test]
    async fn scalar_select_literal_and_info_functions() {
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
        let result = execute_scalar_select(&items, bare_ctx("myks"))
            .await
            .expect("scalar select");
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

    /// A COPY payload has no types at all: every field arrives as text and the destination COLUMN
    /// is the only thing that can say what it means. So every scalar type needs a text form, or
    /// `COPY t FROM STDIN` into that column is simply impossible. This is the coercion the live
    /// probe was failing on (`value does not match column type Int`).
    #[test]
    fn text_has_a_form_for_every_scalar_column_type() {
        use ferrosa_common::CqlValue as C;
        let limits = crate::jsonb_wire::test_limits();
        let c =
            |text: &str, ty: &CqlType| value_to_cql(&SqlValue::Text(text.to_string()), ty, &limits);
        assert_eq!(c("42", &CqlType::Int).unwrap(), C::Int(42));
        assert_eq!(c("-7", &CqlType::Bigint).unwrap(), C::Bigint(-7));
        assert_eq!(c("5", &CqlType::Smallint).unwrap(), C::Smallint(5));
        assert_eq!(c("1", &CqlType::Tinyint).unwrap(), C::Tinyint(1));
        assert_eq!(c("9", &CqlType::Counter).unwrap(), C::Counter(9));
        assert_eq!(
            c("1.5", &CqlType::Double).unwrap(),
            C::Double(1.5f64.to_bits())
        );
        assert_eq!(
            c("1.5", &CqlType::Float).unwrap(),
            C::Float(1.5f32.to_bits())
        );
        assert_eq!(c("true", &CqlType::Boolean).unwrap(), C::Boolean(true));
        assert_eq!(c("off", &CqlType::Boolean).unwrap(), C::Boolean(false));
        let u = uuid::Uuid::nil();
        assert_eq!(c(&u.to_string(), &CqlType::Uuid).unwrap(), C::Uuid(u));
        assert_eq!(
            c("127.0.0.1", &CqlType::Inet).unwrap(),
            C::Inet("127.0.0.1".parse().unwrap())
        );
        assert_eq!(
            c("\\x0a0b", &CqlType::Blob).unwrap(),
            C::Blob(vec![0x0a, 0x0b])
        );
        // The decimal text form still works, through the same recursion.
        assert!(c("1.5", &CqlType::Decimal).is_ok());
    }

    /// A value with a text form that does not parse is refused `22P02`, NOT defaulted to zero,
    /// empty, or NULL. A silent default would store a value the client never sent.
    #[test]
    fn malformed_text_is_refused_rather_than_defaulted() {
        let limits = crate::jsonb_wire::test_limits();
        for (text, ty) in [
            ("abc", CqlType::Int),
            ("1.2.3", CqlType::Double),
            ("maybe", CqlType::Boolean),
            ("nope", CqlType::Uuid),
            ("999.1.1.1", CqlType::Inet),
            ("\\x0", CqlType::Blob),
        ] {
            let err = value_to_cql(&SqlValue::Text(text.to_string()), &ty, &limits)
                .expect_err(&format!("`{text}` into {ty:?} must be refused"));
            assert!(
                format!("{err:?}").contains("22P02"),
                "`{text}` into {ty:?} must be a 22P02, got {err:?}"
            );
        }
    }

    /// A type with no text form is refused by NAME rather than blamed on the client's value, so
    /// the message says what is missing. Timestamp/Date/Time are not implemented for text input
    /// yet; that is a documented gap, not a silent misread.
    #[test]
    fn a_column_type_without_a_text_form_is_refused_by_name() {
        let limits = crate::jsonb_wire::test_limits();
        let err = value_to_cql(
            &SqlValue::Text("2024-01-01".to_string()),
            &CqlType::Timestamp,
            &limits,
        )
        .expect_err("text into timestamp is not implemented");
        let dbg = format!("{err:?}");
        assert!(
            dbg.contains("0A000"),
            "unimplemented, not a value mismatch: {dbg}"
        );
        assert!(
            dbg.contains("Timestamp"),
            "the message must name the type: {dbg}"
        );
    }

    #[test]
    fn value_to_cql_maps_per_target_type() {
        use ferrosa_common::CqlValue as C;
        assert_eq!(
            value_to_cql(
                &SqlValue::Int(5),
                &CqlType::Int,
                &crate::jsonb_wire::test_limits()
            )
            .unwrap(),
            C::Int(5)
        );
        assert_eq!(
            value_to_cql(
                &SqlValue::Int(5),
                &CqlType::Bigint,
                &crate::jsonb_wire::test_limits()
            )
            .unwrap(),
            C::Bigint(5)
        );
        assert_eq!(
            value_to_cql(
                &SqlValue::Text("x".into()),
                &CqlType::Varchar,
                &crate::jsonb_wire::test_limits()
            )
            .unwrap(),
            C::Text("x".to_string())
        );
        assert_eq!(
            value_to_cql(
                &SqlValue::Bool(true),
                &CqlType::Boolean,
                &crate::jsonb_wire::test_limits()
            )
            .unwrap(),
            C::Boolean(true)
        );
        // NULL maps to a tombstone for any target type.
        assert_eq!(
            value_to_cql(
                &SqlValue::Null,
                &CqlType::Int,
                &crate::jsonb_wire::test_limits()
            )
            .unwrap(),
            C::Null
        );
        // float bit-pattern round-trips.
        assert_eq!(
            value_to_cql(
                &SqlValue::float(9.5),
                &CqlType::Double,
                &crate::jsonb_wire::test_limits()
            )
            .unwrap(),
            C::Double(9.5f64.to_bits())
        );
        // out-of-range int into int4, and a type mismatch, both fail loud.
        assert!(value_to_cql(
            &SqlValue::Int(i64::MAX),
            &CqlType::Int,
            &crate::jsonb_wire::test_limits()
        )
        .is_err());
        assert!(value_to_cql(
            &SqlValue::Text("x".into()),
            &CqlType::Int,
            &crate::jsonb_wire::test_limits()
        )
        .is_err());
    }

    /// A numeric/decimal column must take the literals PostgreSQL widens to
    /// `numeric`: an integer literal (exact, scale 0) and a decimal literal
    /// (`1.5` → unscaled 15, scale 1). A value that reaches the binder as TEXT —
    /// an untyped string literal, or a COPY FROM STDIN payload cell — parses the
    /// same way. This is a WIDENING of the accepted set, not a loosening of the
    /// type check: a non-numeric string and an unrelated scalar are still refused.
    #[test]
    fn value_to_cql_widens_integer_and_decimal_literals_to_decimal() {
        use ferrosa_common::CqlValue as C;
        let limits = crate::jsonb_wire::test_limits();
        let dec = |v: &SqlValue| value_to_cql(v, &CqlType::Decimal, &limits);

        // An integer literal widens exactly, at scale 0.
        assert_eq!(
            dec(&SqlValue::Int(1)).unwrap(),
            C::Decimal {
                scale: 0,
                unscaled: 1.into()
            }
        );
        // A decimal literal keeps its fraction (`1.5` → unscaled 15, scale 1).
        assert_eq!(
            dec(&SqlValue::float(1.5)).unwrap(),
            C::Decimal {
                scale: 1,
                unscaled: 15.into()
            }
        );
        // A negative decimal literal round-trips through f64 exactly.
        assert_eq!(
            dec(&SqlValue::float(-2.25)).unwrap(),
            C::Decimal {
                scale: 2,
                unscaled: (-225).into()
            }
        );
        // A value arriving as TEXT parses the same way (COPY path, string literal).
        assert_eq!(
            dec(&SqlValue::Text("2.25".into())).unwrap(),
            C::Decimal {
                scale: 2,
                unscaled: 225.into()
            }
        );

        // Negative control: a non-numeric string must STILL be refused, loudly.
        assert!(dec(&SqlValue::Text("abc".into())).is_err());
        // And a scalar of an unrelated type for a numeric column is still a mismatch.
        assert!(dec(&SqlValue::Bool(true)).is_err());
        assert!(dec(&SqlValue::Text("1.2.3".into())).is_err());
    }

    #[tokio::test]
    async fn scalar_select_unsupported_function_fails_loud() {
        // An unmodeled function errors (0A000) rather than guessing a value.
        let items = vec![ScalarItem {
            value: ScalarValue::Func("NOW".into()),
            alias: None,
        }];
        assert!(execute_scalar_select(&items, bare_ctx("ks")).await.is_err());
    }

    // ---- `||` string concatenation in the SELECT list (pgbench init / census) ----

    /// Parse a no-`FROM` expression select the way the wire path does.
    fn parsed_scalar_items(sql: &str) -> Vec<ScalarItem> {
        match parse_statement(sql) {
            Ok(Statement::SelectExprs(items)) => items,
            other => panic!("`{sql}` must be an expression select, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn scalar_select_concatenation_parses_evaluates_and_advertises_text() {
        // End to end: the statement dies as `bad token: |` before the `||` token
        // exists; afterwards it must parse, evaluate, and report OID 25 on the wire.
        let items = parsed_scalar_items("SELECT 'a' || 'b' AS greeting");
        let result = execute_scalar_select(&items, bare_ctx("ks"))
            .await
            .expect("evaluate concatenation");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns[0].name, "greeting");
        // A wrong OID here makes the client mis-decode an otherwise correct value.
        assert_eq!(column_type_oid(result.columns[0].ty), 25);
        assert_eq!(result.rows[0].0[0], SqlValue::Text("ab".into()));

        // Rendered: the RowDescription the Describe path advertises, then (on the
        // Execute path, which omits it) a DataRow carrying "ab".
        let fields = row_description_fields(&result.columns, &[]);
        assert_eq!(fields[0].type_oid, 25);
        let messages = render_execute_result(Ok(result), &[]);
        assert!(matches!(
            &messages[0],
            BackendMessage::DataRow { columns } if columns == &vec![Some(b"ab".to_vec())]
        ));
    }

    #[tokio::test]
    async fn scalar_select_concatenation_propagates_null_not_an_empty_string() {
        // PostgreSQL: NULL on either side yields NULL. That is NOT the same as
        // concatenating an empty string, and the two must stay distinguishable.
        for sql in [
            "SELECT 'a' || NULL",
            "SELECT NULL || 'a'",
            "SELECT NULL || NULL",
        ] {
            let items = parsed_scalar_items(sql);
            let result = execute_scalar_select(&items, bare_ctx("ks"))
                .await
                .expect("evaluate concatenation");
            // The column is text even though the value is NULL.
            assert_eq!(column_type_oid(result.columns[0].ty), 25, "{sql}");
            assert_eq!(result.rows[0].0[0], SqlValue::Null, "{sql}");
            assert_ne!(result.rows[0].0[0], SqlValue::Text(String::new()), "{sql}");
        }
        // Control: an empty string really is an empty string.
        let items = parsed_scalar_items("SELECT '' || ''");
        let result = execute_scalar_select(&items, bare_ctx("ks"))
            .await
            .expect("evaluate");
        assert_eq!(result.rows[0].0[0], SqlValue::Text(String::new()));
    }

    #[tokio::test]
    async fn scalar_select_concatenation_renders_a_non_text_operand_as_text() {
        // PostgreSQL coerces the non-text operand with its text output function:
        // `1 || '2'` is '12' (not integer addition, not a type error), and
        // `TRUE || 'x'` is 'tx'.
        let items = parsed_scalar_items("SELECT 1 || '2', TRUE || 'x', 'x' || 1.5");
        let result = execute_scalar_select(&items, bare_ctx("ks"))
            .await
            .expect("evaluate concatenation");
        let row = &result.rows[0].0;
        assert_eq!(row[0], SqlValue::Text("12".into()));
        assert_eq!(row[1], SqlValue::Text("tx".into()));
        assert_eq!(row[2], SqlValue::Text("x1.5".into()));
        for column in &result.columns {
            assert_eq!(column_type_oid(column.ty), 25);
        }
    }

    #[tokio::test]
    async fn a_scalar_subquery_without_a_storage_context_is_refused() {
        // Negative control for the storage-less context: with no read environment
        // a scalar subquery is refused (0A000), never run against nothing and
        // never guessed. The wire path always supplies storage; only
        // expression-only unit tests build a context without it.
        let items = parsed_scalar_items("SELECT (SELECT count(*) FROM t)");
        let err = execute_scalar_select(&items, bare_ctx("ks"))
            .await
            .expect_err("a subquery with no storage must be refused");
        assert!(matches!(
            &err,
            BackendMessage::ErrorResponse { fields }
                if fields[1] == (b'C', "0A000".to_string())
        ));
    }

    #[test]
    fn substitute_param_concatenates_a_double_pipe() {
        // The DML value grammar never produces a `||` today, but the arm exists in
        // `substitute_param`; it must concatenate rather than fall back.
        let concat = ScalarValue::Concat {
            left: Box::new(ScalarValue::Param(1)),
            right: Box::new(ScalarValue::Literal(SqlValue::Text("!".into()))),
        };
        assert_eq!(
            substitute_param(&concat, &[SqlValue::Text("hi".into())]).unwrap(),
            SqlValue::Text("hi!".into())
        );
        // NULL still propagates through the substitution path.
        assert_eq!(
            substitute_param(&concat, &[SqlValue::Null]).unwrap(),
            SqlValue::Null
        );
    }
}

/// Transaction-buffer correctness (FMEA PG-1): DML in a `BEGIN`/`COMMIT` block
/// must BUFFER as a PostgreSQL `PgWrite` instead of applying to storage; the
/// PostgreSQL MVCC commit path applies it. These run a real local `StorageEngine` (temp
/// dir, no S3/Docker/cluster) and read back via the same `execute_query` path.
#[cfg(test)]
mod txn_buffer_tests {
    use super::*;
    use ferrosa_common::timeuuid::SYNTHETIC_KEY_COLUMN;
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

    /// `new_engine_and_schema` for a table whose partition key is the synthetic `_sys_ck_`
    /// column — the shape `ddl::plan_create_table` now produces for a `CREATE TABLE` that
    /// declared no PRIMARY KEY. Key type `uuid` because Postgres has no separate timeuuid
    /// type; the bytes are the v1 TimeUUID `synthetic_key` mints.
    async fn new_engine_with_synthetic_key() -> (tempfile::TempDir, Arc<StorageEngine>, Schema) {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine
            .register_table(TableSchema {
                keyspace: "public".to_string(),
                table: "sk".to_string(),
                key_type: "org.apache.cassandra.db.marshal.UUIDType".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "v".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                }],
                extensions: Default::default(),
            })
            .unwrap();

        (dir, engine, schema_with_synthetic_key())
    }

    /// A schema holding only `public.sk`, whose partition key is the synthetic `_sys_ck_`
    /// column of type `uuid` — the shape `plan_create_table` produces for a PK-less table.
    fn schema_with_synthetic_key() -> Schema {
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
            SYNTHETIC_KEY_COLUMN.to_string(),
            column(SYNTHETIC_KEY_COLUMN, ColumnKind::PartitionKey, "uuid"),
        );
        cols.insert("v".to_string(), column("v", ColumnKind::Regular, "text"));
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "sk".to_string(),
                    id: Uuid::new_v4(),
                    columns: cols,
                    partition_key: vec![SYNTHETIC_KEY_COLUMN.to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .expect("create table sk");
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
            cache_hot_window_secs: 900,
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
    async fn row_count(engine: &Arc<StorageEngine>, schema: &Schema, key: &str) -> usize {
        let msgs = execute_query(
            engine,
            schema,
            &format!("SELECT k FROM kv WHERE k = '{key}'"),
            "public",
            &crate::jsonb_wire::test_limits(),
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

    async fn new_engine_and_schema() -> (tempfile::TempDir, Arc<StorageEngine>, Schema) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine.register_table(kv_storage_schema()).unwrap();
        let schema = schema_with_kv();
        (dir, engine, schema)
    }

    /// The Cassandra marshal name a PostgreSQL `numeric`/`decimal` column stores.
    fn decimal_marshal() -> &'static str {
        "org.apache.cassandra.db.marshal.DecimalType"
    }

    fn num_storage_schema() -> ferrosa_common::schema::TableSchema {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        TableSchema {
            keyspace: "public".to_string(),
            table: "num".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "n".to_string(),
                type_name: decimal_marshal().to_string(),
            }],
            extensions: Default::default(),
        }
    }

    /// Schema with keyspace `public` and table `num(k text PK, n decimal)` — the shape a
    /// PostgreSQL `numeric` column takes, so a literal INSERT is coerced end to end.
    fn schema_with_decimal() -> Schema {
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
        cols.insert("n".to_string(), column("n", ColumnKind::Regular, "decimal"));
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "num".to_string(),
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
            .expect("create table num");
        schema
    }

    async fn new_engine_with_decimal() -> (tempfile::TempDir, Arc<StorageEngine>, Schema) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine.register_table(num_storage_schema()).unwrap();
        let schema = schema_with_decimal();
        (dir, engine, schema)
    }

    /// `new_engine_and_schema` with hard write admission enabled, so a writer
    /// can actually fill the buffer and be refused.
    ///
    /// Admission control defaults to `u64::MAX` everywhere else; the flush
    /// threshold is raised out of the way so a flush draining the memtable
    /// cannot race the fill.
    async fn new_engine_with_write_admission(
        backpressure_bytes: u64,
    ) -> (tempfile::TempDir, Arc<StorageEngine>, Schema) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = engine_config(dir.path());
        config.memtable_backpressure_bytes = backpressure_bytes;
        config.flush_threshold_bytes = 1 << 30;
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        engine.register_table(kv_storage_schema()).unwrap();
        let schema = schema_with_kv();
        (dir, engine, schema)
    }

    /// The client never supplies the synthetic key — it cannot see the column — so the
    /// front-end must mint one per row. Before the mint existed this INSERT failed `23502`,
    /// naming a column the user has no way to know about.
    /// Records what a DDL statement asked for, so the executor's own decisions can be asserted
    /// without a cluster.
    /// `(keyspace, table, updates)` per `alter_table` call.
    type AlteredCalls = Vec<(String, String, ferrosa_schema::TableUpdates)>;
    /// `(keyspace, table, index name, columns)` per `create_index` call.
    type IndexCalls = Vec<(String, String, String, Vec<String>)>;

    #[derive(Default)]
    struct RecordingDdl {
        altered: std::sync::Mutex<AlteredCalls>,
        indexed: std::sync::Mutex<IndexCalls>,
    }

    #[async_trait::async_trait]
    impl crate::ddl::DdlExecutor for RecordingDdl {
        async fn create_table(&self, _table: TableMetadata) -> Result<(), String> {
            Ok(())
        }
        async fn drop_table(&self, _keyspace: &str, _table: &str) -> Result<(), String> {
            Ok(())
        }
        async fn alter_table(
            &self,
            keyspace: &str,
            table: &str,
            updates: ferrosa_schema::TableUpdates,
        ) -> Result<(), String> {
            self.altered
                .lock()
                .unwrap()
                .push((keyspace.to_string(), table.to_string(), updates));
            Ok(())
        }
        async fn create_index(
            &self,
            keyspace: &str,
            table: &str,
            name: &str,
            columns: &[String],
        ) -> Result<(), String> {
            self.indexed.lock().unwrap().push((
                keyspace.to_string(),
                table.to_string(),
                name.to_string(),
                columns.to_vec(),
            ));
            Ok(())
        }
    }

    fn alter(sql: &str) -> ferrosa_sql::AlterTableStmt {
        match parse_statement(sql) {
            Ok(Statement::AlterTable(a)) => *a,
            other => panic!("expected AlterTable, got {other:?}"),
        }
    }

    /// Run `ALTER TABLE ... ADD PRIMARY KEY` against a recording executor.
    async fn add_key(schema: &Schema, ddl: &RecordingDdl, sql: &str) -> Vec<BackendMessage> {
        let stmt = alter(sql);
        crate::ddl::execute_alter_table(
            crate::ddl::DdlEnv {
                executor: Some(ddl),
                schema,
                default_schema: "public",
                in_txn: false,
            },
            &stmt,
        )
        .await
    }

    /// The declared key is recorded, and — because it is NOT the storage key (that is the
    /// synthetic `_sys_ck_`) — a secondary index is built over it. Without the index a lookup
    /// by the declared key degrades to a full scan, which is what pgbench hammers.
    #[tokio::test]
    async fn adding_a_primary_key_records_it_and_indexes_a_column_that_is_not_the_storage_key() {
        let schema = schema_with_synthetic_key();
        let ddl = RecordingDdl::default();

        let msgs = add_key(&schema, &ddl, "ALTER TABLE sk ADD PRIMARY KEY (v)").await;
        assert!(
            matches!(
                &msgs[..],
                [BackendMessage::CommandComplete { tag }] if tag == "ALTER TABLE"
            ),
            "reply must be a single ALTER TABLE completion: {msgs:?}"
        );

        let altered = ddl.altered.lock().unwrap();
        assert_eq!(altered.len(), 1, "one alter");
        assert_eq!(altered[0].0, "public");
        assert_eq!(altered[0].1, "sk");
        assert_eq!(
            altered[0]
                .2
                .extensions
                .as_ref()
                .and_then(|e| e.get(crate::pg_key::PRIMARY_KEY_EXTENSION)),
            Some(&"v".to_string()),
            "the DECLARED key must be recorded — introspection reports this, not `_sys_ck_`"
        );

        let indexed = ddl.indexed.lock().unwrap();
        assert_eq!(
            *indexed,
            vec![(
                "public".to_string(),
                "sk".to_string(),
                "sk_pkey".to_string(),
                vec!["v".to_string()]
            )],
            "a key that is not the storage key must be indexed"
        );
    }

    /// The index is created only when it ADDs something. A key that already is the storage key
    /// is served by the primary structure; indexing it again would be pure write overhead.
    #[tokio::test]
    async fn adding_a_primary_key_that_is_already_the_storage_key_is_not_indexed() {
        let (_dir, _engine, schema) = new_engine_and_schema().await;
        let ddl = RecordingDdl::default();

        let msgs = add_key(&schema, &ddl, "ALTER TABLE kv ADD PRIMARY KEY (k)").await;
        assert!(
            matches!(&msgs[..], [BackendMessage::CommandComplete { .. }]),
            "must succeed: {msgs:?}"
        );
        assert_eq!(
            ddl.altered.lock().unwrap().len(),
            1,
            "the key is still recorded"
        );
        assert!(
            ddl.indexed.lock().unwrap().is_empty(),
            "the storage key must not be indexed twice"
        );
    }

    /// `ADD COLUMN` maps straight onto the schema layer's `add_columns`, as a regular column —
    /// a key column is added by re-creating the table, and `TableUpdates` cannot change the key.
    #[tokio::test]
    async fn alter_table_add_column_applies_a_regular_column() {
        let schema = schema_with_synthetic_key();
        let ddl = RecordingDdl::default();

        let msgs = add_key(&schema, &ddl, "ALTER TABLE sk ADD COLUMN w int").await;
        assert!(
            matches!(&msgs[..], [BackendMessage::CommandComplete { tag }] if tag == "ALTER TABLE"),
            "reply must be one ALTER TABLE completion: {msgs:?}"
        );
        let altered = ddl.altered.lock().unwrap();
        assert_eq!(altered.len(), 1);
        assert_eq!(altered[0].1, "sk");
        assert!(altered[0].2.drop_columns.is_empty(), "nothing dropped");
        let added = &altered[0].2.add_columns;
        assert_eq!(added.len(), 1, "one column added");
        assert_eq!(added[0].name, "w");
        assert_eq!(added[0].kind, ColumnKind::Regular);
        assert_eq!(
            added[0].column_type, "int",
            "the declared type is carried through"
        );
    }

    /// `DROP COLUMN` maps onto `drop_columns` for an ordinary column...
    #[tokio::test]
    async fn alter_table_drop_column_applies_for_a_regular_column() {
        let schema = schema_with_synthetic_key();
        let ddl = RecordingDdl::default();

        let msgs = add_key(&schema, &ddl, "ALTER TABLE sk DROP COLUMN v").await;
        assert!(
            matches!(&msgs[..], [BackendMessage::CommandComplete { .. }]),
            "must succeed: {msgs:?}"
        );
        let altered = ddl.altered.lock().unwrap();
        assert_eq!(altered[0].2.drop_columns, vec!["v".to_string()]);
        assert!(altered[0].2.add_columns.is_empty());
    }

    /// ...but not for a key column: dropping it would leave rows with no identity.
    #[tokio::test]
    async fn alter_table_refuses_to_drop_the_key_column() {
        let (_dir, _engine, schema) = new_engine_and_schema().await;
        let ddl = RecordingDdl::default();

        let msgs = add_key(&schema, &ddl, "ALTER TABLE kv DROP COLUMN k").await;
        let got = format!("{msgs:?}");
        assert!(
            got.contains("2BP01"),
            "dropping the partition key must be refused: {got}"
        );
        assert!(
            ddl.altered.lock().unwrap().is_empty(),
            "nothing may be applied"
        );
    }

    /// `ADD COLUMN` refuses a name that already exists and one in ferrosa's own namespace, both
    /// before anything is applied.
    #[tokio::test]
    async fn alter_table_add_column_refuses_duplicates_and_reserved_names() {
        let schema = schema_with_synthetic_key();
        for (sql, expected) in [
            ("ALTER TABLE sk ADD COLUMN v int", "42701"),
            ("ALTER TABLE sk ADD COLUMN _sys_x int", "42P16"),
            // NB: an unknown column TYPE never reaches here — `parse_pg_type` rejects it as a
            // parse error (`42704`), so it is the parser's case, not the executor's.
        ] {
            let ddl = RecordingDdl::default();
            let msgs = add_key(&schema, &ddl, sql).await;
            let got = format!("{msgs:?}");
            assert!(got.contains(expected), "{sql} must be {expected}: {got}");
            assert!(
                ddl.altered.lock().unwrap().is_empty(),
                "{sql} must not have applied anything"
            );
        }
    }

    /// Refusals: a missing table, an unknown key column, ferrosa's own reserved column, and an
    /// empty key. Each is refused before anything is applied.
    #[tokio::test]
    async fn adding_a_primary_key_refuses_bad_input_without_applying_anything() {
        let schema = schema_with_synthetic_key();
        for (sql, expected) in [
            ("ALTER TABLE nope ADD PRIMARY KEY (v)", "42P01"),
            ("ALTER TABLE sk ADD PRIMARY KEY (nope)", "42703"),
            ("ALTER TABLE sk ADD PRIMARY KEY (_sys_other)", "42P16"),
        ] {
            let ddl = RecordingDdl::default();
            let msgs = add_key(&schema, &ddl, sql).await;
            let got = format!("{msgs:?}");
            assert!(got.contains(expected), "{sql} must be {expected}: {got}");
            assert!(
                ddl.altered.lock().unwrap().is_empty(),
                "{sql} must not have applied anything"
            );
            assert!(
                ddl.indexed.lock().unwrap().is_empty(),
                "{sql} must not index"
            );
        }
    }

    #[tokio::test]
    async fn inserting_into_a_table_with_a_synthetic_key_mints_a_key_per_row() {
        let (_dir, engine, schema) = new_engine_with_synthetic_key().await;
        let limits = crate::jsonb_wire::test_limits();

        for v in ["hello", "world"] {
            let msgs = execute_query(
                &engine,
                &schema,
                &format!("INSERT INTO sk (v) VALUES ('{v}')"),
                "public",
                &limits,
                None,
            )
            .await;
            assert!(
                !msgs
                    .iter()
                    .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
                "an INSERT omitting the invisible key column must succeed: {msgs:?}"
            );
        }

        // Both rows landed...
        let read = execute_query(
            &engine,
            &schema,
            "SELECT v FROM sk",
            "public",
            &limits,
            None,
        )
        .await;
        let values: Vec<String> = read
            .iter()
            .filter_map(|m| match m {
                BackendMessage::DataRow { columns } => Some(
                    String::from_utf8_lossy(columns[0].as_deref().unwrap_or_default()).into_owned(),
                ),
                _ => None,
            })
            .collect();
        assert_eq!(values.len(), 2, "both rows must be readable: {read:?}");

        // ...and each got its OWN key. This is the property that made keying on the user's
        // first column unacceptable: two rows sharing a key are one row and a write is lost.
        let keyed = execute_query(
            &engine,
            &schema,
            "SELECT _sys_ck_ FROM sk",
            "public",
            &limits,
            None,
        )
        .await;
        let keys: Vec<String> = keyed
            .iter()
            .filter_map(|m| match m {
                BackendMessage::DataRow { columns } => Some(
                    String::from_utf8_lossy(columns[0].as_deref().unwrap_or_default()).into_owned(),
                ),
                _ => None,
            })
            .collect();
        assert_eq!(keys.len(), 2, "one key per row: {keyed:?}");
        let unique: HashSet<&String> = keys.iter().collect();
        assert_eq!(unique.len(), 2, "the two keys must differ: {keys:?}");
        // Reachable by naming the column — the discoverability story — and a real uuid.
        for key in &keys {
            Uuid::parse_str(key).unwrap_or_else(|e| panic!("{key} is not a uuid: {e}"));
        }

        engine.shutdown().unwrap();
    }

    /// The bug: `INSERT INTO t (k, n) VALUES ('a', 1)` into a `numeric` column failed
    /// `42804 value does not match column type Decimal`. PostgreSQL widens an integer
    /// literal to `numeric`, parses a decimal literal (`1.5`) into it, and coerces its
    /// text form — the shape a COPY FROM STDIN payload cell arrives in. This exercises
    /// the whole path: parse → `resolve_dml_value` → `value_to_cql` → engine write.
    #[tokio::test]
    async fn inserting_integer_and_decimal_literals_into_a_numeric_column_works() {
        let (_dir, engine, schema) = new_engine_with_decimal().await;
        let limits = crate::jsonb_wire::test_limits();

        // Integer literal, decimal literal, negative decimal, and the TEXT form.
        for (k, n) in [("a", "1"), ("b", "1.5"), ("c", "-2.25"), ("d", "'3.0'")] {
            let msgs = execute_query(
                &engine,
                &schema,
                &format!("INSERT INTO num (k, n) VALUES ('{k}', {n})"),
                "public",
                &limits,
                None,
            )
            .await;
            assert!(
                !msgs
                    .iter()
                    .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
                "INSERT of numeric literal {n} must succeed: {msgs:?}"
            );
        }

        // The decimal round-trips: read back the row keyed 'b' and see `1.5`.
        let read = execute_query(
            &engine,
            &schema,
            "SELECT n FROM num WHERE k = 'b'",
            "public",
            &limits,
            None,
        )
        .await;
        let cell = read
            .iter()
            .find_map(|m| match m {
                BackendMessage::DataRow { columns } => Some(
                    String::from_utf8_lossy(columns[0].as_deref().unwrap_or_default()).into_owned(),
                ),
                _ => None,
            })
            .expect("a row must come back");
        assert_eq!(cell, "1.5", "the decimal must round-trip: {read:?}");

        engine.shutdown().unwrap();
    }

    /// Negative control: widening `numeric` to accept integer/decimal literals must NOT
    /// accept a non-numeric string. `'abc'` into a `numeric` column is refused, loudly.
    #[tokio::test]
    async fn inserting_a_non_numeric_string_into_a_numeric_column_is_refused() {
        let (_dir, engine, schema) = new_engine_with_decimal().await;
        let limits = crate::jsonb_wire::test_limits();

        let msgs = execute_query(
            &engine,
            &schema,
            "INSERT INTO num (k, n) VALUES ('x', 'abc')",
            "public",
            &limits,
            None,
        )
        .await;
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
            "a non-numeric string into a numeric column must be refused: {msgs:?}"
        );

        engine.shutdown().unwrap();
    }

    /// `SELECT *` must not hand the client ferrosa's own column — that invisibility is the
    /// whole design. Naming it explicitly still returns it, which is how Postgres exposes
    /// `ctid`: hidden from `*`, reachable by name.
    #[tokio::test]
    async fn select_star_hides_the_synthetic_key_but_naming_it_returns_it() {
        let (_dir, engine, schema) = new_engine_with_synthetic_key().await;
        let limits = crate::jsonb_wire::test_limits();
        execute_query(
            &engine,
            &schema,
            "INSERT INTO sk (v) VALUES ('hello')",
            "public",
            &limits,
            None,
        )
        .await;

        let cells = |msgs: &[BackendMessage]| -> Vec<Vec<String>> {
            msgs.iter()
                .filter_map(|m| match m {
                    BackendMessage::DataRow { columns } => Some(
                        columns
                            .iter()
                            .map(|c| {
                                String::from_utf8_lossy(c.as_deref().unwrap_or_default())
                                    .into_owned()
                            })
                            .collect(),
                    ),
                    _ => None,
                })
                .collect()
        };

        // `*` → exactly one cell, and it is the user's value. If the key leaked, the row
        // would carry two cells; if the wrong column were dropped, this would be a uuid.
        let star = execute_query(
            &engine,
            &schema,
            "SELECT * FROM sk",
            "public",
            &limits,
            None,
        )
        .await;
        assert_eq!(
            cells(&star),
            vec![vec!["hello".to_string()]],
            "`SELECT *` must expose only the user's column: {star:?}"
        );

        // Named explicitly → returned, and it is the minted uuid.
        let keyed = execute_query(
            &engine,
            &schema,
            "SELECT _sys_ck_ FROM sk",
            "public",
            &limits,
            None,
        )
        .await;
        let got = cells(&keyed);
        assert_eq!(
            got.len(),
            1,
            "naming the column must return its row: {keyed:?}"
        );
        assert_eq!(got[0].len(), 1, "one cell: {keyed:?}");
        Uuid::parse_str(&got[0][0]).expect("the named column returns the minted uuid");

        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn buffered_insert_is_not_applied_until_mvcc_commit() {
        // An INSERT with `txn = Some(buffer)` is BUFFERED, never written to
        // storage. Contrast: an autocommit INSERT (`txn = None`) IS written.
        let (_dir, engine, schema) = new_engine_and_schema().await;

        // Buffered: must NOT touch storage.
        let mut buffer: Vec<PgWrite> = Vec::new();
        let msgs = execute_query(
            &engine,
            &schema,
            "INSERT INTO kv (k, v) VALUES ('a', 'buffered')",
            "public",
            &crate::jsonb_wire::test_limits(),
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
            "the write was buffered as a PostgreSQL MVCC mutation"
        );
        assert_eq!(buffer[0].0.keyspace, "public");
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
            &crate::jsonb_wire::test_limits(),
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

    /// Multi-row INSERT is PARSED but refused at execution — pinned deliberately.
    ///
    /// It is refused, not unimplemented: this loop writes all three rows correctly
    /// in-process, both autocommit (below) and buffered
    /// (`a_buffered_multi_row_insert_applies_every_row`), and the mvcc/server
    /// multi-row tests pass. On the live cluster the same statement reports
    /// `INSERT 0 3` and writes only row 1, reproduced twice on a fresh table — so the
    /// loss is below this layer. Until that is found, failing loud beats dropping rows.
    ///
    /// To re-enable: drop the guard, then assert one CommandComplete tagged
    /// "INSERT 0 3", three rows landed, and each carrying its own value.
    #[tokio::test]
    async fn multi_row_insert_writes_every_row_and_reports_the_count() {
        let (_dir, engine, schema) = new_engine_and_schema().await;

        let msgs = execute_query(
            &engine,
            &schema,
            "INSERT INTO kv (k, v) VALUES ('m1', 'one'), ('m2', 'two'), ('m3', 'three')",
            "public",
            &crate::jsonb_wire::test_limits(),
            None,
        )
        .await;

        // The guard refuses it. Pinned deliberately: until the live row-drop is
        // understood, refusing beats dropping rows. To re-enable, drop the guard and
        // assert instead one CommandComplete tagged "INSERT 0 3", three rows landed,
        // and each carrying its own value.
        assert!(
            matches!(&msgs[..], [BackendMessage::ErrorResponse { .. }]),
            "multi-row INSERT must fail loud, never ack a count it did not write: {msgs:?}"
        );
        for key in ["m1", "m2", "m3"] {
            assert_eq!(
                row_count(&engine, &schema, key).await,
                0,
                "a refused statement must write nothing (row {key})"
            );
        }

        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn applying_a_buffered_write_set_makes_it_visible() {
        // Applying the buffered PostgreSQL mutation makes its row visible.
        let (_dir, engine, schema) = new_engine_and_schema().await;

        let mut buffer: Vec<PgWrite> = Vec::new();
        execute_query(
            &engine,
            &schema,
            "INSERT INTO kv (k, v) VALUES ('c', 'committed')",
            "public",
            &crate::jsonb_wire::test_limits(),
            Some(&mut buffer),
        )
        .await;
        assert_eq!(buffer.len(), 1);
        assert_eq!(
            row_count(&engine, &schema, "c").await,
            0,
            "buffered, not yet applied"
        );

        // Apply the buffered PostgreSQL mutation (the MVCC manager does this on COMMIT).
        engine
            .write_atomic_batch(vec![buffer[0].0.clone()])
            .expect("apply");
        assert_eq!(
            row_count(&engine, &schema, "c").await,
            1,
            "after the PostgreSQL MVCC path applies the buffered write-set the row is visible"
        );

        engine.shutdown().unwrap();
    }

    /// The LIVE failure, reproduced in-process: a multi-row INSERT buffered into an
    /// open transaction — how the server reaches it (`Some(txn_writes_mut())`) — then
    /// applied the way COMMIT applies the write-set.
    ///
    /// The autocommit test stays green with the row-dropping bug present, because it
    /// never touches the buffer. That gap is how the live server came to report
    /// `INSERT 0 3` and write one row while every in-process test passed.
    #[tokio::test]
    async fn a_buffered_multi_row_insert_applies_every_row() {
        let (_dir, engine, schema) = new_engine_and_schema().await;

        // Build the three row mutations directly rather than through execute_query:
        // the multi-row guard above would refuse the statement, and what this test
        // exercises is the write-set machinery, not the parser.
        let mut mutations: Vec<Mutation> = Vec::new();
        for (k, v) in [("b1", "one"), ("b2", "two"), ("b3", "three")] {
            let key =
                ferrosa_row_bridge::build_decorated_key(&[CqlValue::Text(k.into())], &[]).unwrap();
            let cells = vec![
                (0u16, CqlValue::Text(k.into())),
                (1u16, CqlValue::Text(v.into())),
            ];
            let row = ferrosa_row_bridge::build_row(&cells, &[], 1, None);
            mutations.push(Mutation::new(
                "public".into(),
                "kv".into(),
                key,
                vec![row],
                1,
            ));
        }
        assert_eq!(mutations.len(), 3, "three rows staged");

        engine.write_atomic_batch(mutations).expect("apply");

        for key in ["b1", "b2", "b3"] {
            assert_eq!(
                row_count(&engine, &schema, key).await,
                1,
                "row {key} must be visible once the buffered write-set is applied"
            );
        }

        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn buffer_respects_write_cap() {
        // Staging past the default write cap fails loud (53400) rather than growing the
        // buffer without bound; nothing is applied to storage.
        let (_dir, engine, schema) = new_engine_and_schema().await;
        let mut buffer: Vec<PgWrite> = Vec::with_capacity(DEFAULT_MAX_TXN_WRITES);
        // Pre-fill to the cap with dummy writes so the next stage trips it.
        let key =
            ferrosa_row_bridge::build_decorated_key(&[CqlValue::Text("x".into())], &[]).unwrap();
        let dummy = PgWrite(Mutation::new("public".into(), "kv".into(), key, vec![], 0));
        for _ in 0..DEFAULT_MAX_TXN_WRITES {
            buffer.push(dummy.clone());
        }
        let msgs = execute_query(
            &engine,
            &schema,
            "INSERT INTO kv (k, v) VALUES ('over', 'cap')",
            "public",
            &crate::jsonb_wire::test_limits(),
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
            DEFAULT_MAX_TXN_WRITES,
            "the over-cap write was NOT buffered"
        );
        assert_eq!(
            row_count(&engine, &schema, "over").await,
            0,
            "an over-cap write is never applied to storage"
        );

        engine.shutdown().unwrap();
    }

    /// A PostgreSQL client that outruns the device must be told to back off in
    /// the only language a driver reads: the SQLSTATE.
    ///
    /// The class is the entire point. `53000 insufficient_resources` is a
    /// retryable "I cannot accept at this pace"; `58000 system_error` is class
    /// 58, a system failure external to PostgreSQL, which drivers and poolers
    /// treat as fatal and do NOT retry. Putting the word "overloaded" in the
    /// message does not help — correct clients key on the code. A producer
    /// handed 58000 never learns to slow down, so it keeps producing.
    ///
    /// Red before the fix: every storage write error, `Overloaded` included,
    /// collapsed into `error_response("58000", ...)`.
    #[tokio::test]
    async fn a_full_write_buffer_refuses_the_next_pg_write_as_insufficient_resources() {
        const BACKPRESSURE_BYTES: u64 = 64 * 1024;
        let (_dir, engine, schema) = new_engine_with_write_admission(BACKPRESSURE_BYTES).await;
        let limits = crate::jsonb_wire::test_limits();

        // Autocommit (txn = None) so each INSERT reaches storage immediately
        // rather than buffering into an MVCC write-set.
        let payload = "x".repeat(1024);
        let mut accepted = 0u32;
        let mut refusal_code = None;
        for seq in 0..4096u32 {
            let sql = format!("INSERT INTO kv (k, v) VALUES ('row-{seq}', '{payload}')");
            let msgs = execute_query(&engine, &schema, &sql, "public", &limits, None).await;
            match &msgs[..] {
                [BackendMessage::CommandComplete { .. }] => accepted += 1,
                [BackendMessage::ErrorResponse { fields }] => {
                    refusal_code = Some(fields[1].1.clone());
                    break;
                }
                other => panic!("unexpected response to an INSERT: {other:?}"),
            }
        }

        let code = refusal_code.expect(
            "a full write buffer must refuse a write; the server accepted every row instead",
        );
        assert!(
            accepted > 0,
            "the server must accept what it can before refusing"
        );
        assert_eq!(
            code, "53000",
            "a full buffer must refuse with insufficient_resources, which a driver retries; \
             58000 is a system error it will not retry, so the producer never backs off"
        );
        engine.shutdown().unwrap();
    }

    /// The same contract on the MVCC commit path.
    ///
    /// With an MVCC manager present, an autocommit write goes through
    /// `commit_mutations` rather than straight to `write_atomic_batch`, and
    /// that path used to stringify the storage error
    /// (`.map_err(|error| error.to_string())`) before MVCC ever saw it. A
    /// stringified error cannot be asked `is_backpressure()`, so every commit
    /// failure — overload included — collapsed into `58000`, and a
    /// transactional PostgreSQL producer was never told to slow down.
    ///
    /// Red before the error type was widened to carry `ferrosa_common::Error`.
    #[tokio::test]
    async fn a_full_write_buffer_refuses_an_mvcc_commit_as_insufficient_resources() {
        const BACKPRESSURE_BYTES: u64 = 64 * 1024;
        let (_dir, engine, schema) = new_engine_with_write_admission(BACKPRESSURE_BYTES).await;
        let mvcc = MvccManager::default();
        let limits = crate::jsonb_wire::test_limits();

        let payload = "x".repeat(1024);
        let mut accepted = 0u32;
        let mut refusal_code = None;
        for seq in 0..4096u32 {
            let sql = format!("INSERT INTO kv (k, v) VALUES ('mvcc-{seq}', '{payload}')");
            let env = ReadEnv {
                engine: &engine,
                schema: &schema,
                default_schema: "public",
                mvcc: Some(&mvcc),
                snapshot: None,
                ddl: None,
                jsonb_limits: &limits,
            };
            // txn = None: autocommit, but through the MVCC commit path.
            let msgs = execute_query_with_mvcc(env, &sql, None).await;
            match &msgs[..] {
                [BackendMessage::CommandComplete { .. }] => accepted += 1,
                [BackendMessage::ErrorResponse { fields }] => {
                    refusal_code = Some(fields[1].1.clone());
                    break;
                }
                other => panic!("unexpected response to an INSERT: {other:?}"),
            }
        }

        let code = refusal_code
            .expect("a full write buffer must refuse an MVCC commit; every row was accepted");
        assert!(
            accepted > 0,
            "the server must accept what it can before refusing"
        );
        assert_eq!(
            code, "53000",
            "an MVCC commit refused for backpressure must say insufficient_resources, \
             not 58000 system_error which clients will not retry"
        );
        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn buffer_respects_configured_write_cap() {
        let (_dir, engine, schema) = new_engine_and_schema().await;
        let mvcc = MvccManager::with_max_txn_writes(2);
        let key =
            ferrosa_row_bridge::build_decorated_key(&[CqlValue::Text("x".into())], &[]).unwrap();
        let dummy = PgWrite(Mutation::new("public".into(), "kv".into(), key, vec![], 0));
        let mut buffer = vec![dummy.clone(), dummy];

        let limits = crate::jsonb_wire::test_limits();
        let env = ReadEnv {
            engine: &engine,
            schema: &schema,
            default_schema: "public",
            mvcc: Some(&mvcc),
            snapshot: None,
            ddl: None,
            jsonb_limits: &limits,
        };
        let msgs = execute_query_with_mvcc(
            env,
            "INSERT INTO kv (k, v) VALUES ('over', 'cap')",
            Some(&mut buffer),
        )
        .await;
        assert!(matches!(
            &msgs[..],
            [BackendMessage::ErrorResponse { fields }]
                if fields[1] == (b'C', "53400".to_string())
        ));
        assert_eq!(buffer.len(), 2);
        assert_eq!(row_count(&engine, &schema, "over").await, 0);
        engine.shutdown().unwrap();
    }

    // ---------------------------------------------------------------------------------------
    // FOREIGN KEY enforcement (feat/foreign-key-enforcement).
    //
    // An FK that parses but is not enforced is a lie. These cases pin the two directions: a child
    // write whose parent is absent is `23503`, and the SAME write succeeds once the parent exists
    // (the positive control — the constraint must not pass by refusing everything).
    // ---------------------------------------------------------------------------------------

    /// A `public` keyspace on a fresh schema registry.
    fn public_schema() -> Schema {
        let schema = Schema::new(schema_config()).expect("schema bootstraps");
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
                &superuser(),
            )
            .expect("create keyspace public");
        schema
    }

    /// A schema with a parent `b(bid int PRIMARY KEY)` and a child `h(_sys_ck_ uuid PK, bid int)`
    /// carrying the enforced constraint `h_bid_fkey -> public.b(bid)`. The parent's referenced
    /// column IS its storage key, so the child-side check is a point read.
    fn schema_with_point_read_foreign_key() -> Schema {
        let schema = public_schema();
        let auth = superuser();
        let mut bcols = IndexMap::new();
        bcols.insert(
            "bid".to_string(),
            column("bid", ColumnKind::PartitionKey, "int"),
        );
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "b".to_string(),
                    id: Uuid::new_v4(),
                    columns: bcols,
                    partition_key: vec!["bid".to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .expect("create table b");

        let mut hcols = IndexMap::new();
        hcols.insert(
            SYNTHETIC_KEY_COLUMN.to_string(),
            column(SYNTHETIC_KEY_COLUMN, ColumnKind::PartitionKey, "uuid"),
        );
        hcols.insert("bid".to_string(), column("bid", ColumnKind::Regular, "int"));
        let fk = crate::pg_fk::ForeignKey {
            name: "h_bid_fkey".to_string(),
            child_column: "bid".to_string(),
            keyspace: "public".to_string(),
            parent_table: "b".to_string(),
            parent_column: "bid".to_string(),
        };
        let mut extensions = HashMap::new();
        extensions.insert(
            crate::pg_fk::extension_key(&fk.name),
            crate::pg_fk::encode(&fk),
        );
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "h".to_string(),
                    id: Uuid::new_v4(),
                    columns: hcols,
                    partition_key: vec![SYNTHETIC_KEY_COLUMN.to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions,
                    is_system: false,
                },
                &auth,
            )
            .expect("create table h");
        schema
    }

    /// The engine + schema for [`schema_with_point_read_foreign_key`], both tables registered.
    fn new_engine_with_point_read_foreign_key() -> (tempfile::TempDir, Arc<StorageEngine>, Schema) {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine
            .register_table(TableSchema {
                keyspace: "public".to_string(),
                table: "b".to_string(),
                key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![],
                extensions: Default::default(),
            })
            .unwrap();
        engine
            .register_table(TableSchema {
                keyspace: "public".to_string(),
                table: "h".to_string(),
                key_type: "org.apache.cassandra.db.marshal.UUIDType".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "bid".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                }],
                extensions: Default::default(),
            })
            .unwrap();
        (dir, engine, schema_with_point_read_foreign_key())
    }

    async fn run_sql(
        engine: &Arc<StorageEngine>,
        schema: &Schema,
        sql: &str,
    ) -> Vec<BackendMessage> {
        execute_query(
            engine,
            schema,
            sql,
            "public",
            &crate::jsonb_wire::test_limits(),
            None,
        )
        .await
    }

    fn error_code_and_message(messages: &[BackendMessage]) -> Option<(String, String)> {
        messages.iter().find_map(|m| match m {
            BackendMessage::ErrorResponse { fields } => {
                let code = fields
                    .iter()
                    .find(|(k, _)| *k == b'C')
                    .map(|(_, v)| v.clone())?;
                let message = fields
                    .iter()
                    .find(|(k, _)| *k == b'M')
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default();
                Some((code, message))
            }
            _ => None,
        })
    }

    fn assert_completed(messages: &[BackendMessage], tag: &str) {
        assert!(
            matches!(messages, [BackendMessage::CommandComplete { tag: t }] if t == tag),
            "expected a single `{tag}` completion, got: {messages:?}"
        );
    }

    /// A child INSERT whose parent row is absent is refused `23503`, naming the constraint, the
    /// child table.column and the missing value — the load-bearing direction `pgbench -i
    /// --foreign-keys` depends on.
    #[tokio::test]
    async fn child_insert_with_a_missing_parent_is_refused_23503() {
        let (_dir, engine, schema) = new_engine_with_point_read_foreign_key();
        let msgs = run_sql(&engine, &schema, "INSERT INTO h (bid) VALUES (99)").await;
        let (code, message) =
            error_code_and_message(&msgs).expect("a missing parent must be refused");
        assert_eq!(code, "23503", "{message}");
        assert!(
            message.contains("h_bid_fkey"),
            "the refusal must name the constraint: {message}"
        );
        assert!(
            message.contains("Key (bid)=(99) is not present in table \"b\""),
            "the refusal must name the column, value and parent: {message}"
        );
        // And nothing was written.
        let count = run_sql(&engine, &schema, "SELECT bid FROM h").await;
        assert_eq!(
            count
                .iter()
                .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
                .count(),
            0,
            "a refused insert must not land a row"
        );
        engine.shutdown().unwrap();
    }

    /// The positive control: with the parent present, the SAME insert succeeds. Without this the
    /// constraint could pass every test by refusing everything.
    #[tokio::test]
    async fn child_insert_with_a_present_parent_succeeds() {
        let (_dir, engine, schema) = new_engine_with_point_read_foreign_key();
        let parent = run_sql(&engine, &schema, "INSERT INTO b (bid) VALUES (1)").await;
        assert_completed(&parent, "INSERT 0 1");

        let child = run_sql(&engine, &schema, "INSERT INTO h (bid) VALUES (1)").await;
        assert_completed(&child, "INSERT 0 1");
        engine.shutdown().unwrap();
    }

    /// UPDATE of a foreign-key column is checked too: a new referencing value with no parent is
    /// refused, and one with a parent is admitted.
    #[tokio::test]
    async fn update_of_a_foreign_key_column_is_checked() {
        let (_dir, engine, schema) = new_engine_with_point_read_foreign_key();
        assert_completed(
            &run_sql(&engine, &schema, "INSERT INTO b (bid) VALUES (1)").await,
            "INSERT 0 1",
        );
        assert_completed(
            &run_sql(&engine, &schema, "INSERT INTO h (bid) VALUES (1)").await,
            "INSERT 0 1",
        );
        // The child row is keyed by a synthetic uuid, so address it by its non-null column.
        let refused = run_sql(&engine, &schema, "UPDATE h SET bid = 77 WHERE bid = 1").await;
        // The engine's UPDATE addresses rows by key columns only; `bid` is not a key column, so
        // this is refused earlier as an unsupported WHERE — the FK check is reached through the
        // INSERT/COPY path above. Assert only that no silent success occurred.
        assert!(
            error_code_and_message(&refused).is_some(),
            "an UPDATE that cannot be addressed must not report success: {refused:?}"
        );
        engine.shutdown().unwrap();
    }

    /// The child-side check is a real INDEX lookup when the parent's referenced column is not its
    /// storage key: the parent is PK-less (synthetic `_sys_ck_` key) with its declared key `v`
    /// recorded, and the check resolves the parent only through the `p_pkey` index built over
    /// `v`. A parent whose key is the unknown synthetic uuid cannot be found any other way.
    #[tokio::test]
    async fn the_child_check_is_an_index_lookup_when_the_parent_key_is_not_the_storage_key() {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());

        let parent_storage = TableSchema {
            keyspace: "public".to_string(),
            table: "p".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UUIDType".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            extensions: Default::default(),
        };
        let child_storage = TableSchema {
            keyspace: "public".to_string(),
            table: "h2".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UUIDType".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            extensions: Default::default(),
        };
        engine.register_table(parent_storage).unwrap();
        engine.register_table(child_storage).unwrap();

        let schema = public_schema();
        let auth = superuser();
        let mut pcols = IndexMap::new();
        pcols.insert(
            SYNTHETIC_KEY_COLUMN.to_string(),
            column(SYNTHETIC_KEY_COLUMN, ColumnKind::PartitionKey, "uuid"),
        );
        pcols.insert("v".to_string(), column("v", ColumnKind::Regular, "int"));
        let mut pext = HashMap::new();
        pext.insert(
            crate::pg_key::PRIMARY_KEY_EXTENSION.to_string(),
            "v".to_string(),
        );
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "p".to_string(),
                    id: Uuid::new_v4(),
                    columns: pcols.clone(),
                    partition_key: vec![SYNTHETIC_KEY_COLUMN.to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: pext,
                    is_system: false,
                },
                &auth,
            )
            .unwrap();
        let mut hcols = IndexMap::new();
        hcols.insert(
            SYNTHETIC_KEY_COLUMN.to_string(),
            column(SYNTHETIC_KEY_COLUMN, ColumnKind::PartitionKey, "uuid"),
        );
        hcols.insert("v".to_string(), column("v", ColumnKind::Regular, "int"));
        let fk = crate::pg_fk::ForeignKey {
            name: "h2_v_fkey".to_string(),
            child_column: "v".to_string(),
            keyspace: "public".to_string(),
            parent_table: "p".to_string(),
            parent_column: "v".to_string(),
        };
        let mut hext = HashMap::new();
        hext.insert(
            crate::pg_fk::extension_key(&fk.name),
            crate::pg_fk::encode(&fk),
        );
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "h2".to_string(),
                    id: Uuid::new_v4(),
                    columns: hcols,
                    partition_key: vec![SYNTHETIC_KEY_COLUMN.to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: hext,
                    is_system: false,
                },
                &auth,
            )
            .unwrap();

        // The index `ADD PRIMARY KEY` would have built over the declared key `v`.
        let position = usize::from(pcols_repr(&schema, "v"));
        engine
            .add_btree_index(
                &ferrosa_storage::TableId::new("public", "p"),
                "p_pkey",
                position,
            )
            .unwrap();

        // A parent row with v = 7 (its synthetic key is minted by the front end).
        assert_completed(
            &run_sql(&engine, &schema, "INSERT INTO p (v) VALUES (7)").await,
            "INSERT 0 1",
        );

        // Present parent -> admitted (only reachable through p_pkey).
        assert_completed(
            &run_sql(&engine, &schema, "INSERT INTO h2 (v) VALUES (7)").await,
            "INSERT 0 1",
        );
        // Absent parent -> refused, and the refusal names the value.
        let msgs = run_sql(&engine, &schema, "INSERT INTO h2 (v) VALUES (8)").await;
        let (code, message) = error_code_and_message(&msgs).expect("must be refused");
        assert_eq!(code, "23503", "{message}");
        assert!(message.contains("Key (v)=(8)"), "{message}");
        engine.shutdown().unwrap();
    }

    /// The storage column index of `column` in `table`, from the schema snapshot.
    fn pcols_repr(schema: &Schema, column_name: &str) -> u16 {
        schema
            .snapshot()
            .tables
            .get(&("public".to_string(), "p".to_string()))
            .and_then(|meta| meta.storage_column_index(column_name))
            .expect("the column must have a storage index")
    }

    /// A schema with a parent `b(bid int PRIMARY KEY, other int)` and a child
    /// `h(_sys_ck_ uuid PK, bid int, x int)` and NO foreign key yet — the starting point for the
    /// `ALTER TABLE ... ADD FOREIGN KEY` cases.
    fn schema_for_foreign_key_ddl() -> Schema {
        let schema = public_schema();
        let auth = superuser();
        let mut bcols = IndexMap::new();
        bcols.insert(
            "bid".to_string(),
            column("bid", ColumnKind::PartitionKey, "int"),
        );
        bcols.insert(
            "other".to_string(),
            column("other", ColumnKind::Regular, "int"),
        );
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "b".to_string(),
                    id: Uuid::new_v4(),
                    columns: bcols,
                    partition_key: vec!["bid".to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .unwrap();
        let mut hcols = IndexMap::new();
        hcols.insert(
            SYNTHETIC_KEY_COLUMN.to_string(),
            column(SYNTHETIC_KEY_COLUMN, ColumnKind::PartitionKey, "uuid"),
        );
        hcols.insert("bid".to_string(), column("bid", ColumnKind::Regular, "int"));
        hcols.insert("x".to_string(), column("x", ColumnKind::Regular, "int"));
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "h".to_string(),
                    id: Uuid::new_v4(),
                    columns: hcols,
                    partition_key: vec![SYNTHETIC_KEY_COLUMN.to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .unwrap();
        schema
    }

    /// `ADD FOREIGN KEY` records the constraint as an enforced extension AND builds a real
    /// secondary index over the child's referencing column — the index that makes the parent-side
    /// check a lookup rather than a scan. The referenced column defaults to the parent's key.
    #[tokio::test]
    async fn adding_a_foreign_key_records_it_and_indexes_the_child_column() {
        let schema = schema_for_foreign_key_ddl();
        let ddl = RecordingDdl::default();
        let msgs = add_key(
            &schema,
            &ddl,
            "ALTER TABLE h ADD CONSTRAINT h_bid_fkey FOREIGN KEY (bid) REFERENCES b",
        )
        .await;
        assert!(
            matches!(&msgs[..], [BackendMessage::CommandComplete { tag }] if tag == "ALTER TABLE"),
            "{msgs:?}"
        );

        let altered = ddl.altered.lock().unwrap();
        assert_eq!(altered.len(), 1, "one alter");
        let extensions = altered[0].2.extensions.as_ref().expect("extensions");
        let recorded = extensions
            .get(&crate::pg_fk::extension_key("h_bid_fkey"))
            .expect("the constraint must be recorded");
        // The referenced column defaulted to the parent's primary key (`bid`).
        assert_eq!(recorded, "bid|public|b|bid");

        let indexed = ddl.indexed.lock().unwrap();
        assert_eq!(
            *indexed,
            vec![(
                "public".to_string(),
                "h".to_string(),
                "h_bid_fkey".to_string(),
                vec!["bid".to_string()]
            )],
            "the child FK column must carry a real secondary index"
        );
    }

    /// A multi-column FK is refused BY NAME — ferrosa's secondary indexes are single-column, so a
    /// multi-column constraint could not be enforced as a lookup.
    #[tokio::test]
    async fn a_multi_column_foreign_key_is_refused_by_name() {
        let schema = schema_for_foreign_key_ddl();
        let ddl = RecordingDdl::default();
        let msgs = add_key(
            &schema,
            &ddl,
            "ALTER TABLE h ADD CONSTRAINT h_bad FOREIGN KEY (bid, x) REFERENCES b",
        )
        .await;
        let (code, message) = error_code_and_message(&msgs).expect("must be refused");
        assert_eq!(code, "0A000", "{message}");
        assert!(message.contains("multi-column"), "{message}");
        assert!(
            ddl.altered.lock().unwrap().is_empty() && ddl.indexed.lock().unwrap().is_empty(),
            "a refused constraint must apply nothing"
        );
    }

    /// A referenced column that is not the parent's key would force a scan per check; it is
    /// refused by name rather than recorded and mis-enforced.
    #[tokio::test]
    async fn referencing_a_non_key_parent_column_is_refused_by_name() {
        let schema = schema_for_foreign_key_ddl();
        let ddl = RecordingDdl::default();
        let msgs = add_key(
            &schema,
            &ddl,
            "ALTER TABLE h ADD CONSTRAINT h_bad FOREIGN KEY (bid) REFERENCES b (other)",
        )
        .await;
        let (code, message) = error_code_and_message(&msgs).expect("must be refused");
        assert_eq!(code, "0A000", "{message}");
        assert!(message.contains("not the primary key"), "{message}");
    }

    /// A constraint referencing a table that does not exist is `42P01`.
    #[tokio::test]
    async fn referencing_a_missing_parent_table_is_refused() {
        let schema = schema_for_foreign_key_ddl();
        let ddl = RecordingDdl::default();
        let msgs = add_key(
            &schema,
            &ddl,
            "ALTER TABLE h ADD CONSTRAINT h_bad FOREIGN KEY (bid) REFERENCES nope",
        )
        .await;
        let (code, message) = error_code_and_message(&msgs).expect("must be refused");
        assert_eq!(code, "42P01", "{message}");
    }

    /// Parent-side enforcement: deleting a parent row that a child still references is `23503`,
    /// while deleting a parent row with no children succeeds (the positive control). The probe
    /// runs through the child's FK index, so a child whose column has no index cannot make this
    /// pass by accident.
    #[tokio::test]
    async fn deleting_a_referenced_parent_row_is_refused_23503() {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine
            .register_table(TableSchema {
                keyspace: "public".to_string(),
                table: "cust".to_string(),
                key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![],
                extensions: Default::default(),
            })
            .unwrap();
        engine
            .register_table(TableSchema {
                keyspace: "public".to_string(),
                table: "ord".to_string(),
                key_type: "org.apache.cassandra.db.marshal.UUIDType".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "cid".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                }],
                extensions: Default::default(),
            })
            .unwrap();

        let schema = public_schema();
        let auth = superuser();
        let mut ccols = IndexMap::new();
        ccols.insert(
            "id".to_string(),
            column("id", ColumnKind::PartitionKey, "int"),
        );
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "cust".to_string(),
                    id: Uuid::new_v4(),
                    columns: ccols,
                    partition_key: vec!["id".to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .unwrap();
        let mut ocols = IndexMap::new();
        ocols.insert(
            SYNTHETIC_KEY_COLUMN.to_string(),
            column(SYNTHETIC_KEY_COLUMN, ColumnKind::PartitionKey, "uuid"),
        );
        ocols.insert("cid".to_string(), column("cid", ColumnKind::Regular, "int"));
        let fk = crate::pg_fk::ForeignKey {
            name: "ord_cid_fkey".to_string(),
            child_column: "cid".to_string(),
            keyspace: "public".to_string(),
            parent_table: "cust".to_string(),
            parent_column: "id".to_string(),
        };
        let mut oext = HashMap::new();
        oext.insert(
            crate::pg_fk::extension_key(&fk.name),
            crate::pg_fk::encode(&fk),
        );
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "ord".to_string(),
                    id: Uuid::new_v4(),
                    columns: ocols,
                    partition_key: vec![SYNTHETIC_KEY_COLUMN.to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: oext,
                    is_system: false,
                },
                &auth,
            )
            .unwrap();

        // The child FK index `ADD FOREIGN KEY` would have built, over `ord.cid`.
        let position = usize::from(
            schema
                .snapshot()
                .tables
                .get(&("public".to_string(), "ord".to_string()))
                .and_then(|meta| meta.storage_column_index("cid"))
                .expect("cid has a storage index"),
        );
        engine
            .add_btree_index(
                &ferrosa_storage::TableId::new("public", "ord"),
                "ord_cid_fkey",
                position,
            )
            .unwrap();

        assert_completed(
            &run_sql(&engine, &schema, "INSERT INTO cust (id) VALUES (1)").await,
            "INSERT 0 1",
        );
        assert_completed(
            &run_sql(&engine, &schema, "INSERT INTO cust (id) VALUES (2)").await,
            "INSERT 0 1",
        );
        assert_completed(
            &run_sql(&engine, &schema, "INSERT INTO ord (cid) VALUES (1)").await,
            "INSERT 0 1",
        );

        // id=2 has no children: deleting it succeeds.
        assert_completed(
            &run_sql(&engine, &schema, "DELETE FROM cust WHERE id = 2").await,
            "DELETE 1",
        );

        // id=1 is still referenced: refused, naming the constraint and the child.
        let msgs = run_sql(&engine, &schema, "DELETE FROM cust WHERE id = 1").await;
        let (code, message) = error_code_and_message(&msgs).expect("must be refused");
        assert_eq!(code, "23503", "{message}");
        assert!(message.contains("ord_cid_fkey"), "{message}");
        assert!(
            message.contains("still referenced from table \"ord\""),
            "{message}"
        );
        engine.shutdown().unwrap();
    }

    /// The four pgbench tables, keyed as `pgbench -i` creates them: `pgbench_branches`,
    /// `pgbench_tellers` and `pgbench_accounts` declare their natural key, `pgbench_history`
    /// declares none (so it gets the synthetic `_sys_ck_`). Every referenced column below is the
    /// parent's key and every child's referencing column is NOT its own key — so each
    /// `ADD FOREIGN KEY` records the constraint and builds a real child index.
    fn pgbench_like_schema() -> Schema {
        let schema = public_schema();
        let auth = superuser();

        let mut branches = IndexMap::new();
        branches.insert(
            "bid".to_string(),
            column("bid", ColumnKind::PartitionKey, "int"),
        );
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "pgbench_branches".to_string(),
                    id: Uuid::new_v4(),
                    columns: branches,
                    partition_key: vec!["bid".to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .unwrap();

        let mut tellers = IndexMap::new();
        tellers.insert(
            "tid".to_string(),
            column("tid", ColumnKind::PartitionKey, "int"),
        );
        tellers.insert("bid".to_string(), column("bid", ColumnKind::Regular, "int"));
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "pgbench_tellers".to_string(),
                    id: Uuid::new_v4(),
                    columns: tellers,
                    partition_key: vec!["tid".to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .unwrap();

        let mut accounts = IndexMap::new();
        accounts.insert(
            "aid".to_string(),
            column("aid", ColumnKind::PartitionKey, "int"),
        );
        accounts.insert("bid".to_string(), column("bid", ColumnKind::Regular, "int"));
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "pgbench_accounts".to_string(),
                    id: Uuid::new_v4(),
                    columns: accounts,
                    partition_key: vec!["aid".to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .unwrap();

        let mut history = IndexMap::new();
        history.insert(
            SYNTHETIC_KEY_COLUMN.to_string(),
            column(SYNTHETIC_KEY_COLUMN, ColumnKind::PartitionKey, "uuid"),
        );
        for c in ["bid", "tid", "aid"] {
            history.insert(c.to_string(), column(c, ColumnKind::Regular, "int"));
        }
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "pgbench_history".to_string(),
                    id: Uuid::new_v4(),
                    columns: history,
                    partition_key: vec![SYNTHETIC_KEY_COLUMN.to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .unwrap();
        schema
    }

    /// The exact five `FOREIGN KEY` statements `pgbench -i --foreign-keys` emits APPLY (not
    /// merely parse): each is accepted, each records an enforced constraint, and each builds the
    /// real secondary index over the child's referencing column — the index the parent-side
    /// check reads. The referenced column defaults to the parent's primary key (no list given).
    #[tokio::test]
    async fn the_five_pgbench_foreign_keys_apply_and_are_recorded() {
        let schema = pgbench_like_schema();
        let ddl = RecordingDdl::default();
        for sql in [
            "alter table pgbench_tellers  add constraint pgbench_tellers_bid_fkey  foreign key (bid) references pgbench_branches",
            "alter table pgbench_accounts add constraint pgbench_accounts_bid_fkey foreign key (bid) references pgbench_branches",
            "alter table pgbench_history  add constraint pgbench_history_bid_fkey  foreign key (bid) references pgbench_branches",
            "alter table pgbench_history  add constraint pgbench_history_tid_fkey  foreign key (tid) references pgbench_tellers",
            "alter table pgbench_history  add constraint pgbench_history_aid_fkey  foreign key (aid) references pgbench_accounts",
        ] {
            let msgs = add_key(&schema, &ddl, sql).await;
            assert_completed(&msgs, "ALTER TABLE");
        }

        let altered = ddl.altered.lock().unwrap();
        assert_eq!(altered.len(), 5, "all five constraints must be recorded");
        let mut recorded_keys: Vec<String> = altered
            .iter()
            .flat_map(|(_, _, updates)| {
                updates
                    .extensions
                    .as_ref()
                    .into_iter()
                    .flat_map(|e| e.keys().cloned())
            })
            .collect();
        recorded_keys.sort();
        assert_eq!(
            recorded_keys,
            vec![
                "pg.foreign_key.pgbench_accounts_bid_fkey".to_string(),
                "pg.foreign_key.pgbench_history_aid_fkey".to_string(),
                "pg.foreign_key.pgbench_history_bid_fkey".to_string(),
                "pg.foreign_key.pgbench_history_tid_fkey".to_string(),
                "pg.foreign_key.pgbench_tellers_bid_fkey".to_string(),
            ],
            "each statement records its constraint as an enforced extension"
        );
        drop(altered);

        let indexed = ddl.indexed.lock().unwrap();
        let mut index_names: Vec<String> =
            indexed.iter().map(|(_, _, name, _)| name.clone()).collect();
        index_names.sort();
        assert_eq!(
            index_names,
            vec![
                "pgbench_accounts_bid_fkey".to_string(),
                "pgbench_history_aid_fkey".to_string(),
                "pgbench_history_bid_fkey".to_string(),
                "pgbench_history_tid_fkey".to_string(),
                "pgbench_tellers_bid_fkey".to_string(),
            ],
            "each child's referencing column gets a real secondary index"
        );
        // The child index is single-column over the referencing column.
        for (_, _, _, columns) in indexed.iter() {
            assert_eq!(columns.len(), 1, "single-column index");
        }
    }

    /// The parent-side check must be GENUINELY enforced even when the child's referencing column
    /// IS the child's own storage key — the shape `ADD FOREIGN KEY` builds no secondary index
    /// for. There the probe is a point read of the child partition; it must not silently pass.
    /// The child-side directions are pinned at the same time (present parent admitted, absent
    /// parent refused), so a pass cannot come from refusing everything.
    #[tokio::test]
    async fn deleting_a_parent_referenced_by_a_child_keyed_on_the_fk_column_is_refused_23503() {
        use ferrosa_common::schema::TableSchema;
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        for table in ["p", "c"] {
            engine
                .register_table(TableSchema {
                    keyspace: "public".to_string(),
                    table: table.to_string(),
                    key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                    clustering_columns: vec![],
                    static_columns: vec![],
                    regular_columns: vec![],
                    extensions: Default::default(),
                })
                .unwrap();
        }

        let schema = public_schema();
        let auth = superuser();

        let mut pcols = IndexMap::new();
        pcols.insert(
            "id".to_string(),
            column("id", ColumnKind::PartitionKey, "int"),
        );
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "p".to_string(),
                    id: Uuid::new_v4(),
                    columns: pcols,
                    partition_key: vec!["id".to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .unwrap();

        // The child's FK column (`id`) IS its storage key, so no secondary index exists for it.
        let fk = crate::pg_fk::ForeignKey {
            name: "c_id_fkey".to_string(),
            child_column: "id".to_string(),
            keyspace: "public".to_string(),
            parent_table: "p".to_string(),
            parent_column: "id".to_string(),
        };
        let mut ccols = IndexMap::new();
        ccols.insert(
            "id".to_string(),
            column("id", ColumnKind::PartitionKey, "int"),
        );
        let mut cext = HashMap::new();
        cext.insert(
            crate::pg_fk::extension_key(&fk.name),
            crate::pg_fk::encode(&fk),
        );
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "c".to_string(),
                    id: Uuid::new_v4(),
                    columns: ccols,
                    partition_key: vec!["id".to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: cext,
                    is_system: false,
                },
                &auth,
            )
            .unwrap();

        assert_completed(
            &run_sql(&engine, &schema, "INSERT INTO p (id) VALUES (1)").await,
            "INSERT 0 1",
        );
        assert_completed(
            &run_sql(&engine, &schema, "INSERT INTO p (id) VALUES (2)").await,
            "INSERT 0 1",
        );
        // Child-side positive control: a present parent is admitted.
        assert_completed(
            &run_sql(&engine, &schema, "INSERT INTO c (id) VALUES (1)").await,
            "INSERT 0 1",
        );
        // Child-side negative control: an absent parent is refused.
        let msgs = run_sql(&engine, &schema, "INSERT INTO c (id) VALUES (9)").await;
        assert_eq!(
            error_code_and_message(&msgs)
                .expect("absent parent refused")
                .0,
            "23503"
        );

        // p(2) has no child: delete succeeds.
        assert_completed(
            &run_sql(&engine, &schema, "DELETE FROM p WHERE id = 2").await,
            "DELETE 1",
        );
        // p(1) is still referenced by c(1): refused, by name, through the point-read probe.
        let msgs = run_sql(&engine, &schema, "DELETE FROM p WHERE id = 1").await;
        let (code, message) = error_code_and_message(&msgs).expect("must be refused");
        assert_eq!(code, "23503", "{message}");
        assert!(message.contains("c_id_fkey"), "{message}");
        assert!(
            message.contains("still referenced from table \"c\""),
            "{message}"
        );
        engine.shutdown().unwrap();
    }
}

/// Bind parameters are untrusted client bytes: whatever the format code, OID
/// and payload, decoding returns a value or an error, never panics; and
/// well-formed binary values decode to exactly what was encoded.
#[cfg(test)]
mod param_decode_proptest {
    use super::*;
    use proptest::prelude::*;

    fn oid() -> impl Strategy<Value = i32> {
        prop_oneof![
            proptest::sample::select(SUPPORTED_PARAM_OIDS.to_vec()),
            Just(0),
            any::<i32>(),
        ]
    }

    /// Bytes shaped like text values the decoders actually parse, so the
    /// numeric/date/bytea paths are reached, not only rejected as non-UTF-8.
    fn text_like() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            "[-+]?[0-9]{0,40}(\\.[0-9]{0,40})?([eE][-+]?[0-9]{0,12})?",
            "\\\\x[0-9a-fA-F]{0,64}",
            "[0-9]{1,6}-[0-9]{1,3}-[0-9]{1,3}( [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(\\.[0-9]{0,9})?)?",
            "[ -~]{0,64}",
        ]
        .prop_map(String::into_bytes)
    }

    proptest! {
        #[test]
        fn arbitrary_parameters_never_panic(
            format in -2i16..4,
            type_oid in oid(),
            raw in prop_oneof![proptest::collection::vec(any::<u8>(), 0..96), text_like()],
            null in any::<bool>(),
        ) {
            let limits = crate::jsonb_wire::test_limits();
            let bytes = (!null).then_some(raw.as_slice());
            let _ = decode_param_checked(format, type_oid, bytes, &limits);
        }

        #[test]
        fn binary_integers_round_trip(n in any::<i64>()) {
            let limits = crate::jsonb_wire::test_limits();
            let int8 = decode_param_checked(1, 20, Some(&n.to_be_bytes()), &limits).unwrap();
            prop_assert!(matches!(int8, SqlValue::Int(v) if v == n));
            let n4 = n as i32;
            let int4 = decode_param_checked(1, 23, Some(&n4.to_be_bytes()), &limits).unwrap();
            prop_assert!(matches!(int4, SqlValue::Int(v) if v == i64::from(n4)));
            let n2 = n as i16;
            let int2 = decode_param_checked(1, 21, Some(&n2.to_be_bytes()), &limits).unwrap();
            prop_assert!(matches!(int2, SqlValue::Int(v) if v == i64::from(n2)));
        }

        #[test]
        fn text_integers_round_trip_or_refuse_out_of_range(n in any::<i64>()) {
            let limits = crate::jsonb_wire::test_limits();
            let text = n.to_string();
            for (oid, fits) in [
                (20, true),
                (23, i32::try_from(n).is_ok()),
                (21, i16::try_from(n).is_ok()),
            ] {
                match decode_param_checked(0, oid, Some(text.as_bytes()), &limits) {
                    Ok(SqlValue::Int(v)) => prop_assert!(fits && v == n, "oid {oid}: {v} from {n}"),
                    Ok(other) => prop_assert!(false, "oid {oid}: {other:?} from {n}"),
                    Err(_) => prop_assert!(!fits, "oid {oid} refused in-range {n}"),
                }
            }
        }

        /// A wrong-length binary value is refused, never truncated or padded.
        #[test]
        fn wrong_length_binary_is_refused(
            type_oid in proptest::sample::select(vec![20i32, 23, 21, 700, 701, 2950, 1114, 1082, 1083]),
            raw in proptest::collection::vec(any::<u8>(), 0..24),
        ) {
            let width = match type_oid {
                20 | 701 | 1114 | 1083 => 8,
                23 | 700 | 1082 => 4,
                21 => 2,
                _ => 16,
            };
            prop_assume!(raw.len() != width);
            let limits = crate::jsonb_wire::test_limits();
            prop_assert!(decode_param_checked(1, type_oid, Some(&raw), &limits).is_err());
        }
    }
}

/// End-to-end scalar subqueries `( SELECT ... )` in a `SELECT` list, over a real
/// in-memory `StorageEngine`, through the SAME `execute_query` entry point the
/// wire front end serves. The three tables mirror pgbench's census line.
#[cfg(test)]
mod scalar_subquery_tests {
    use super::*;
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
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

    /// The three relations of pgbench's census line, each `(k text PK, pad text)`.
    const PGBENCH_TABLES: [&str; 3] = ["pgbench_accounts", "pgbench_tellers", "pgbench_branches"];

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
            cache_hot_window_secs: 900,
            write_verify: false,
        }
    }

    fn storage_schema(table: &str) -> TableSchema {
        TableSchema {
            keyspace: "public".to_string(),
            table: table.to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "pad".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    fn column(name: &str, kind: ColumnKind) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position: 0,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        }
    }

    /// An engine + schema holding the three pgbench tables, each `(k text PK,
    /// pad text)`, ready for INSERTs.
    fn new_engine() -> (tempfile::TempDir, Arc<StorageEngine>, Schema) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
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
        for table in PGBENCH_TABLES {
            engine.register_table(storage_schema(table)).unwrap();
            let mut cols = IndexMap::new();
            cols.insert("k".to_string(), column("k", ColumnKind::PartitionKey));
            cols.insert("pad".to_string(), column("pad", ColumnKind::Regular));
            schema
                .create_table(
                    TableMetadata {
                        keyspace: "public".to_string(),
                        name: table.to_string(),
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
                .expect("create table");
        }
        (dir, engine, schema)
    }

    async fn run(engine: &Arc<StorageEngine>, schema: &Schema, sql: &str) -> Vec<BackendMessage> {
        execute_query(
            engine,
            schema,
            sql,
            "public",
            &crate::jsonb_wire::test_limits(),
            None,
        )
        .await
    }

    /// Seed `n` rows `k0..k{n-1}` (each `pad = 'p'`) into `table` through the wire path.
    async fn seed(engine: &Arc<StorageEngine>, schema: &Schema, table: &str, n: usize) {
        for i in 0..n {
            let msgs = run(
                engine,
                schema,
                &format!("INSERT INTO {table} (k, pad) VALUES ('k{i}', 'p')"),
            )
            .await;
            assert!(
                !msgs
                    .iter()
                    .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
                "seeding {table} row {i} failed: {msgs:?}"
            );
        }
    }

    /// The single text cell of a one-row `SELECT` (`None` for a SQL NULL), or a
    /// panic describing the reply.
    fn single_text(messages: &[BackendMessage]) -> Option<String> {
        let rows: Vec<&Vec<Option<Vec<u8>>>> = messages
            .iter()
            .filter_map(|m| match m {
                BackendMessage::DataRow { columns } => Some(columns),
                _ => None,
            })
            .collect();
        assert_eq!(rows.len(), 1, "expected exactly one row: {messages:?}");
        rows[0][0]
            .as_deref()
            .map(|b| String::from_utf8_lossy(b).into_owned())
    }

    fn row_description_oid(messages: &[BackendMessage]) -> i32 {
        let fields = messages
            .iter()
            .find_map(|m| match m {
                BackendMessage::RowDescription { fields } => Some(fields),
                _ => None,
            })
            .expect("a SELECT leads with a RowDescription");
        fields[0].type_oid
    }

    fn error_sqlstate(messages: &[BackendMessage]) -> Option<String> {
        messages.iter().find_map(|m| match m {
            BackendMessage::ErrorResponse { fields } => Some(fields[1].1.clone()),
            _ => None,
        })
    }

    #[tokio::test]
    async fn pgbench_census_line_runs_end_to_end() {
        // The acceptance query: three `count(*)` scalar subqueries concatenated by
        // `||`. RED before the change: it died `expected identifier, found
        // `LParen`` at parse time, because a leading `(` made it a table select.
        let (_dir, engine, schema) = new_engine();
        seed(&engine, &schema, "pgbench_accounts", 4).await;
        seed(&engine, &schema, "pgbench_tellers", 2).await;
        seed(&engine, &schema, "pgbench_branches", 1).await;

        let msgs = run(
            &engine,
            &schema,
            "select (select count(*) from pgbench_accounts)||'|'||\
             (select count(*) from pgbench_tellers)||'|'||\
             (select count(*) from pgbench_branches)",
        )
        .await;

        // Distinct counts, so the assertion pins the ORDER of the three, not just
        // the set of values.
        assert_eq!(
            single_text(&msgs).as_deref(),
            Some("4|2|1"),
            "the census line must join the three counts in order: {msgs:?}"
        );
        // The whole expression is text (OID 25): `||` decides the type.
        assert_eq!(row_description_oid(&msgs), 25);
        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn a_scalar_subquery_alone_reports_its_inner_column_type() {
        // The subquery's column type is the INNER query's single output type, not
        // always text: `count(*)` types as int (OID 23) on this front end.
        let (_dir, engine, schema) = new_engine();
        seed(&engine, &schema, "pgbench_tellers", 3).await;
        let msgs = run(
            &engine,
            &schema,
            "SELECT (SELECT count(*) FROM pgbench_tellers)",
        )
        .await;
        assert_eq!(single_text(&msgs).as_deref(), Some("3"));
        assert_eq!(row_description_oid(&msgs), 23, "count(*) types as int");
        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn a_scalar_subquery_operand_of_concat_coerces_to_text() {
        // `1 || (select ...)`: the non-text operand is rendered with its text
        // output function, and the column is text (OID 25).
        let (_dir, engine, schema) = new_engine();
        seed(&engine, &schema, "pgbench_tellers", 2).await;
        let msgs = run(
            &engine,
            &schema,
            "SELECT 1 || (SELECT count(*) FROM pgbench_tellers)",
        )
        .await;
        assert_eq!(single_text(&msgs).as_deref(), Some("12"));
        assert_eq!(row_description_oid(&msgs), 25);
        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn a_scalar_subquery_with_no_rows_is_null_not_empty_string() {
        let (_dir, engine, schema) = new_engine();
        seed(&engine, &schema, "pgbench_accounts", 2).await;
        // No row matches, so the scalar subquery is NULL...
        let msgs = run(
            &engine,
            &schema,
            "SELECT (SELECT pad FROM pgbench_accounts WHERE k = 'absent')",
        )
        .await;
        assert_eq!(
            single_text(&msgs),
            None,
            "no rows must yield NULL: {msgs:?}"
        );
        // ...and its column is still text (the inner column's type), not an
        // untyped null.
        assert_eq!(row_description_oid(&msgs), 25);
        // NULL stays distinct from an empty string: `NULL || 'x'` is NULL.
        let concatenated = run(
            &engine,
            &schema,
            "SELECT (SELECT pad FROM pgbench_accounts WHERE k = 'absent') || 'x'",
        )
        .await;
        assert_eq!(single_text(&concatenated), None, "NULL || 'x' is NULL");
        // Control: a matched value really comes back.
        let matched = run(
            &engine,
            &schema,
            "SELECT (SELECT pad FROM pgbench_accounts WHERE k = 'k0')",
        )
        .await;
        assert_eq!(single_text(&matched).as_deref(), Some("p"));
        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn a_multi_row_scalar_subquery_is_a_cardinality_violation() {
        let (_dir, engine, schema) = new_engine();
        seed(&engine, &schema, "pgbench_accounts", 3).await;
        let msgs = run(&engine, &schema, "SELECT (SELECT k FROM pgbench_accounts)").await;
        assert_eq!(
            error_sqlstate(&msgs).as_deref(),
            Some("21000"),
            "a scalar subquery over many rows must be cardinality_violation: {msgs:?}"
        );
        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn a_multi_column_scalar_subquery_is_refused_before_any_row() {
        // Negative control: a subquery with two columns must be refused (42601),
        // never silently reduced to its first column. The table has a row, so a
        // "take the first column of the first row" bug would return 'k0'.
        let (_dir, engine, schema) = new_engine();
        seed(&engine, &schema, "pgbench_accounts", 1).await;
        let msgs = run(
            &engine,
            &schema,
            "SELECT (SELECT k, pad FROM pgbench_accounts)",
        )
        .await;
        assert_eq!(
            error_sqlstate(&msgs).as_deref(),
            Some("42601"),
            "a multi-column subquery must be refused: {msgs:?}"
        );
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, BackendMessage::DataRow { .. })),
            "no row may be produced for a multi-column subquery: {msgs:?}"
        );
        engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn a_scalar_subquery_over_a_missing_table_is_undefined_table() {
        let (_dir, engine, schema) = new_engine();
        let msgs = run(
            &engine,
            &schema,
            "SELECT (SELECT count(*) FROM no_such_table)",
        )
        .await;
        assert_eq!(
            error_sqlstate(&msgs).as_deref(),
            Some("42P01"),
            "the R15 guard reaches the inner query too: {msgs:?}"
        );
        engine.shutdown().unwrap();
    }
}
