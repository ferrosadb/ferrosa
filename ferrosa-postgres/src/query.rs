//! Query execution: lower a simple-query SQL string onto the bespoke relational
//! engine ([`ferrosa_sql`]) over live ferrosa storage, and render the result set
//! as Postgres backend messages.
//!
//! Pipeline for one `SELECT`:
//!
//! 1. `parse(sql)` → [`ferrosa_sql::SelectStmt`] (syntax errors → `42601`).
//! 2. For every referenced table (`FROM` plus an optional `JOIN`), resolve its
//!    keyspace and [`storage_provider::load_table`] it into an in-memory snapshot,
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

use ferrosa_schema::Schema;
use ferrosa_sql::{
    execute, parse, ColumnType, ExecError, MapCatalog, QueryResult, Value as SqlValue,
};
use ferrosa_storage::StorageEngine;

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
/// `Float -> 701` (float8), `Uuid -> 2950` (uuid), `Bytea -> 17` (bytea).
pub(crate) fn column_type_oid(ty: ColumnType) -> i32 {
    match ty {
        ColumnType::Int => 23,
        ColumnType::Text => 25,
        ColumnType::Bool => 16,
        ColumnType::Float => 701,
        ColumnType::Uuid => 2950,
        ColumnType::Bytea => 17,
    }
}

/// The on-wire fixed size for a column type (`-1` for variable-length text /
/// bytea). `Uuid` is a fixed 16 bytes.
fn column_type_size(ty: ColumnType) -> i16 {
    match ty {
        ColumnType::Int => 4,
        ColumnType::Bool => 1,
        ColumnType::Float => 8,
        ColumnType::Uuid => 16,
        ColumnType::Text | ColumnType::Bytea => -1,
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
    }
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
        // OID 0 (unspecified) or any unknown OID: lenient — keep as text.
        _ => SqlValue::Text(s.to_string()),
    }
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
        // Unknown OID: best-effort text, else NULL (documented fallback — no panic).
        _ => std::str::from_utf8(raw)
            .map(|s| SqlValue::Text(s.to_string()))
            .unwrap_or(SqlValue::Null),
    }
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
/// [`column_type_oid`] / [`column_type_size`]: `ColumnType::Int` ⇒ int4 (OID 23,
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
) -> Vec<BackendMessage> {
    // 1. Parse.
    let stmt = match parse(sql) {
        Ok(stmt) => stmt,
        Err(e) => return vec![error_response("42601", &e.to_string())],
    };

    // 2. Load every referenced table into a catalog. The R15 guard lives in
    //    `load_table`: a missing table is `NoSuchTable`, never an empty scan.
    let catalog = match load_catalog(engine, schema, &stmt, default_schema).await {
        Ok(catalog) => catalog,
        Err(err_msg) => return vec![err_msg],
    };

    // 3. Bind + execute over the materialized snapshots (simple query: no
    //    bound parameters).
    match execute(&stmt, &catalog, default_schema, &[]) {
        Ok(result) => render_result(result, &[]), // simple query: all text format
        Err(e) => vec![exec_error_response(&e)],
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
}
