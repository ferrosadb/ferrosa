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
    parse_statement, Column, ColumnType, DeleteStmt, ExecError, InsertStmt, MapCatalog,
    QueryResult, Returning, Row, ScalarItem, ScalarValue, Statement, UpdateStmt, Value as SqlValue,
};
use ferrosa_storage::{Mutation, StorageEngine};

use crate::messages::{BackendMessage, FieldDescription};
use crate::mvcc::{
    MvccCommitError, MvccManager, MvccSnapshot, PgWrite, RowChange, DEFAULT_MAX_TXN_WRITES,
};
use crate::result_stream::{open_stream, ResultStream};
use crate::storage_provider::{load_table_with_overlay, LoadError, ScanFailure, SCAN_BUFFER_ROWS};

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
        ParseError::UnsupportedClause(_) | ParseError::MissingPrimaryKey => "0A000",
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
    select: ferrosa_sql::SelectStmt,
    pending_writes: Option<&[PgWrite]>,
    params: Vec<SqlValue>,
) -> Result<ResultStream, BackendMessage> {
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
fn write_error_response(error: &ferrosa_common::Error) -> BackendMessage {
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
        // No-`FROM` expression query: `SELECT 1`, `SELECT version()`, etc.
        Statement::SelectExprs(items) => match execute_scalar_select(&items, default_schema) {
            Ok(result) => render_result(result, &[]),
            Err(err_msg) => vec![err_msg],
        },
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
    let max_txn_writes = mvcc.map_or(DEFAULT_MAX_TXN_WRITES, MvccManager::max_txn_writes);
    match txn {
        Some(buffer) => {
            if buffer.len() >= max_txn_writes {
                return vec![error_response(
                    "53400",
                    &format!(
                        "transaction write-set exceeds the {max_txn_writes}-write limit; \
                         ROLLBACK required"
                    ),
                )];
            }
            buffer.push(PgWrite(mutation));
            vec![BackendMessage::CommandComplete {
                tag: ok_tag.to_string(),
            }]
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
                    Ok(_) => vec![BackendMessage::CommandComplete {
                        tag: ok_tag.to_string(),
                    }],
                    Err(MvccCommitError::SerializationFailure) => vec![error_response(
                        "40001",
                        "could not serialize PostgreSQL transaction",
                    )],
                    // Now that `Storage` carries the typed error, a commit
                    // refused for backpressure answers 53000 like the direct
                    // write path, instead of collapsing into 58000.
                    Err(MvccCommitError::Storage(error)) => vec![write_error_response(&error)],
                    Err(error) => {
                        vec![error_response("58000", &format!("write failed: {error:?}"))]
                    }
                }
            }
            None => match engine.write_atomic_batch(vec![mutation]) {
                Ok(()) => vec![BackendMessage::CommandComplete {
                    tag: ok_tag.to_string(),
                }],
                Err(e) => vec![write_error_response(&e)],
            },
        },
    }
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
        for mutation in mutations {
            let table = (mutation.keyspace.clone(), mutation.table.clone());
            touched_tables.insert(table.clone());
            for row in &mutation.rows {
                let before = crate::storage_provider::read_row_image(
                    engine,
                    schema,
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
        let pending: Vec<PgWrite> = mutations.iter().cloned().map(PgWrite).collect();
        for (keyspace, table) in touched_tables {
            let overlay = table_rows
                .entry((keyspace.clone(), table.clone()))
                .or_default();
            crate::storage_provider::apply_pending_writes_with_partition_keys(
                engine,
                schema,
                &keyspace,
                &table,
                overlay,
                &pending,
                Some(&mut partition_keys),
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

/// Resolve a DML scalar to a concrete [`SqlValue`], substituting bound
/// parameters from `params` (1-based, the Postgres `$N` numbering). A literal
/// passes through; a `$N` indexes `params`. FAILS LOUD (`08P01`,
/// protocol_violation) when `$N` has no bound value — never a silent default, so
/// a parameter the client failed to bind can never become NULL. Function calls
/// in DML values are unsupported (`0A000`).
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

    // Resolve each named column's value (per its CQL type), substituting bound
    // params; collect regular/static cells by storage index, the CQL values by
    // name for key ordering, and the SQL values by name for RETURNING.
    let mut col_values: HashMap<String, CqlValue> = HashMap::new();
    let mut sql_values: HashMap<String, SqlValue> = HashMap::new();
    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
    // Multi-row INSERT is PARSED but must not be EXECUTED yet.
    //
    // The loop below handles N rows correctly in-process (its unit test writes all
    // three and reads them back), but on the live server a 3-row INSERT reports
    // `INSERT 0 3` and writes only row 1 — reproduced twice on a fresh table.
    // Root cause is open. Until it is found, fail loud: a silent row-drop is
    // strictly worse than an error, which is why this guard exists.
    if ins.rows.len() != 1 {
        return vec![error_response(
            "0A000",
            &format!(
                "multi-row INSERT is parsed but not yet executed ({} rows); \
                 executing it would write only the first row",
                ins.rows.len()
            ),
        )];
    }

    // WHEN EXECUTION IS RE-ENABLED, FIX THIS FIRST: `col_values`, `sql_values` and
    // `regular_cells` above are declared OUTSIDE this loop, so they persist across
    // rows — `regular_cells` is never cleared and accumulates every earlier row's
    // cells. They must be created inside the loop, per row.
    //
    // The statement's own tag: one INSERT of N rows, not N INSERTs of one.
    let tag = format!("INSERT 0 {}", ins.rows.len());
    let mut combined_returning: Option<QueryResult> = None;
    let mut last_write: Vec<BackendMessage> = Vec::new();
    for row_index in 0..ins.rows.len() {
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
    let referenced = std::iter::once(&stmt.from).chain(stmt.join.as_ref().map(|j| &j.table));
    for table_ref in referenced {
        let keyspace = table_ref.schema.as_deref().unwrap_or(default_schema);
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
/// must BUFFER as a PostgreSQL `PgWrite` instead of applying to storage; the
/// PostgreSQL MVCC commit path applies it. These run a real local `StorageEngine` (temp
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

    /// Multi-row INSERT is PARSED but must not be EXECUTED — pinned, deliberately.
    ///
    /// The in-process implementation writes all N rows and reads them back (that is
    /// what this test asserted before). On the live server the same statement
    /// reports `INSERT 0 3` and writes only row 1, reproduced twice on a fresh
    /// table. Root cause is open, so execution fails loud rather than dropping rows.
    ///
    /// When the live path is understood, invert this test: assert 3 rows land, a
    /// single `CommandComplete` tagged `INSERT 0 3`, and nothing concatenated.
    #[tokio::test]
    async fn multi_row_insert_fails_loud_rather_than_dropping_rows() {
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
        assert!(
            matches!(&msgs[..], [BackendMessage::ErrorResponse { .. }]),
            "a multi-row INSERT must fail loud, never ack a count it did not write: {msgs:?}"
        );
        // Nothing may have been written by the refused statement.
        for key in ["m1", "m2", "m3"] {
            assert_eq!(
                row_count(&engine, &schema, key).await,
                0,
                "a refused multi-row INSERT must write nothing (row {key})"
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
