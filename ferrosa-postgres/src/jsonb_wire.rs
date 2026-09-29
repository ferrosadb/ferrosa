//! Module: jsonb (OID 3802) on the PostgreSQL wire, text and binary (T-161a).
//! Correctness: correct when every accepted input is a validated `JsonbValue`
//! parsed under the configured limits; every refusal is a typed SQLSTATE that
//! never echoes the input; and every output is PostgreSQL's exact jsonb text
//! (D26), or an error, never NULL and never truncated.
//! Last revised: 2026-09-28
//! Last changed: T-161a initial wire codec.
//!
//! # Formats
//!
//! - text format: the JSON text. Output uses `TextStyle::PgText` (D26).
//! - binary format (`jsonb_send`/`jsonb_recv`): one version byte `0x01`, then
//!   the same text. Any other version byte is `XX000` and an empty value is
//!   `08P01`, exactly what PostgreSQL 16 answers (T-301 oracle).
//! - `json` (OID 114) parameters are accepted and stored as jsonb (D11). The
//!   binary form of `json` has no version byte (`json_recv` is plain text).
//!
//! # SQLSTATEs
//!
//! | code    | cause |
//! |---------|-------|
//! | `22P02` | text is not valid JSON or not UTF-8 (offset given, input never echoed) |
//! | `22P05` | a `\u0000` escape: a Postgres text value cannot hold a NUL (T-301) |
//! | `XX000` | binary value with an unknown version byte (PG's `jsonb_recv` uses `elog`; matched byte for byte, T-301) |
//! | `08P01` | binary value with no version byte (PG: insufficient data left in message) |
//! | `22030` | duplicate key under the strict `DuplicateKeyPolicy::Error` |
//! | `54000` | input over a configured limit, or output over the print budget |
//! | `XX001` | a stored cell failed validation (corruption) |
//! | `XX000` | the parser or printer misbehaved (a bug, not bad input) |
//!
//! Tunable limits gate input only (D14b). Output is bounded by the fixed
//! [`TEXT_OUTPUT_BUDGET`], not by the tunables, so lowering a limit never makes
//! stored data unreadable.

use ferrosa_jsonb::{
    parse_text_observed_with, print_to_string, DuplicateKeyObserver, JsonbError, JsonbValue,
    Limits, LimitsConfig, NulPolicy, PrintError, TextStyle,
};

/// The version byte a binary jsonb value starts with (`jsonb_send`).
pub const BINARY_VERSION: u8 = 1;

/// Upper bound on one printed jsonb value (JB-D4). A stored cell is capped far
/// below this by the write limits, so this only stops a pathological
/// amplification (number scale, escapes) from allocating without bound.
pub const TEXT_OUTPUT_BUDGET: usize = 1 << 30;

/// Which input path a value arrived by, for the duplicate-key edge log (D6b).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InputEdge {
    /// A text- or binary-format bound parameter.
    Parameter,
    /// An untyped string literal in the SQL text.
    Literal,
}

impl InputEdge {
    fn name(self) -> &'static str {
        match self {
            InputEdge::Parameter => "pg_parameter",
            InputEdge::Literal => "pg_literal",
        }
    }
}

/// A refusal to send back to the client: SQLSTATE plus a message that names the
/// fault and never the input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WireError {
    pub sqlstate: &'static str,
    pub message: String,
}

impl WireError {
    fn new(sqlstate: &'static str, message: String) -> Self {
        Self { sqlstate, message }
    }
}

/// Limits for tests only: the crate defaults with a 32 MiB write path. Product
/// code never calls this; the binary passes the limits it resolved at startup.
#[doc(hidden)]
pub fn test_limits() -> Limits {
    match Limits::from_config(&LimitsConfig::default(), 32 * 1024 * 1024) {
        Ok(limits) => limits,
        Err(error) => panic!("default jsonb limits must resolve: {error}"),
    }
}

/// Reports a dropped duplicate key as an edge log line (D6b, FM-04). The
/// process has no metrics registry in this crate, so the log line is the
/// observable; the per-edge counter is tracked as follow-up.
struct EdgeLog;

impl DuplicateKeyObserver for EdgeLog {
    fn duplicate_keys_dropped(&self, edge: &str, count: u64) {
        tracing::warn!(
            edge,
            count,
            "jsonb_duplicate_keys_dropped: duplicate object keys resolved last-wins (D6b)"
        );
    }
}

/// Map a parse-side [`JsonbError`] to its SQLSTATE. Exhaustive: a new error
/// variant must choose a code here.
fn input_error(error: &JsonbError) -> WireError {
    let (state, message) = match error {
        JsonbError::Syntax { offset, reason } => (
            "22P02",
            format!("invalid input syntax for type jsonb: {reason} at byte offset {offset}"),
        ),
        JsonbError::InvalidUtf8 { offset } => (
            "22P02",
            format!("invalid input syntax for type jsonb: not valid UTF-8 at byte offset {offset}"),
        ),
        JsonbError::InvalidNumber { offset } => (
            "22P02",
            format!(
                "invalid input syntax for type jsonb: malformed number at byte offset {offset}"
            ),
        ),
        JsonbError::NulEscape { offset } => (
            "22P05",
            format!(
                "unsupported Unicode escape sequence: \\u0000 cannot be converted to text at byte offset {offset}"
            ),
        ),
        JsonbError::NonFiniteNumber => (
            "22P02",
            "invalid input syntax for type jsonb: non-finite number".to_string(),
        ),
        JsonbError::DuplicateKey { .. } => (
            "22030",
            "duplicate JSON object key in jsonb input".to_string(),
        ),
        JsonbError::InputTooLarge { .. }
        | JsonbError::EncodedTooLarge { .. }
        | JsonbError::DepthExceeded { .. }
        | JsonbError::DigitsBeforePointExceeded { .. }
        | JsonbError::DigitsAfterPointExceeded { .. }
        | JsonbError::KeyListTooLong { .. } => ("54000", format!("jsonb input refused: {error}")),
        JsonbError::InflightBudgetExceeded { .. } => {
            ("53200", format!("jsonb input refused: {error}"))
        }
        JsonbError::BuilderMisuse { .. } => ("XX000", format!("jsonb parser fault: {error}")),
        JsonbError::UnknownEnvelope { .. }
        | JsonbError::InvalidEncoding { .. }
        | JsonbError::WrongKind { .. } => (
            "XX001",
            format!("jsonb parser produced a bad cell: {error}"),
        ),
    };
    WireError::new(state, message)
}

/// Parse JSON text (a text-format parameter, a literal, or the tail of a binary
/// value) into a validated cell under `limits`. Duplicate keys resolve
/// last-wins and are logged as an edge (D6b).
pub(crate) fn parse_text_input(
    raw: &[u8],
    limits: &Limits,
    edge: InputEdge,
) -> Result<JsonbValue, WireError> {
    let encoded = parse_text_observed_with(raw, limits, edge.name(), &EdgeLog, NulPolicy::Reject)
        .map_err(|error| input_error(&error))?;
    JsonbValue::from_encoded(encoded).map_err(|error| input_error(&error))
}

/// Parse a binary-format jsonb parameter: version byte `0x01`, then text.
pub(crate) fn parse_binary_input(raw: &[u8], limits: &Limits) -> Result<JsonbValue, WireError> {
    match raw.split_first() {
        Some((&BINARY_VERSION, text)) => parse_text_input(text, limits, InputEdge::Parameter),
        // PostgreSQL 16 raises both from `jsonb_recv` (`elog(ERROR)` and the
        // message-buffer underflow), so its SQLSTATEs are XX000 and 08P01. The
        // differential oracle compares them; ferrosa matches rather than
        // inventing a friendlier code the reference does not give.
        Some((&version, _)) => Err(WireError::new(
            "XX000",
            format!("unsupported jsonb version number {version}"),
        )),
        None => Err(WireError::new(
            "08P01",
            "insufficient data left in message: missing jsonb version byte".to_string(),
        )),
    }
}

/// Map a print failure to its SQLSTATE: over budget is a limit, a reader fault
/// is corruption of the stored cell.
fn print_error(error: &PrintError) -> WireError {
    match error {
        PrintError::BudgetExceeded { max } => WireError::new(
            "54000",
            format!("jsonb text output exceeds the {max}-byte output budget"),
        ),
        PrintError::Sink => WireError::new("XX000", "jsonb text sink refused a write".to_string()),
        PrintError::Jsonb(inner) => {
            WireError::new("XX001", format!("stored jsonb cell is corrupt: {inner}"))
        }
    }
}

/// The PostgreSQL text form of a stored cell (D26), or a typed error.
pub(crate) fn render_text(doc: &JsonbValue) -> Result<Vec<u8>, WireError> {
    let view = doc.view().map_err(|error| {
        WireError::new("XX001", format!("stored jsonb cell is corrupt: {error}"))
    })?;
    print_to_string(view.root(), TextStyle::PgText, TEXT_OUTPUT_BUDGET)
        .map(String::into_bytes)
        .map_err(|error| print_error(&error))
}

/// The binary form (`jsonb_send`): `0x01` then the text form.
pub(crate) fn render_binary(doc: &JsonbValue) -> Result<Vec<u8>, WireError> {
    let text = render_text(doc)?;
    let mut out = Vec::with_capacity(text.len() + 1);
    out.push(BINARY_VERSION);
    out.extend_from_slice(&text);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(text: &str) -> JsonbValue {
        parse_text_input(text.as_bytes(), &test_limits(), InputEdge::Literal).expect("valid json")
    }

    /// PG-T161a-01: PgText output, with the binary form prefixed by 0x01.
    #[test]
    fn jsonb_wire_renders_pg_text_and_binary() {
        let d = doc(r#"{"aa":2,"b":1.10}"#);
        assert_eq!(
            render_text(&d).unwrap(),
            br#"{"b": 1.10, "aa": 2}"#.to_vec()
        );
        let mut want = vec![1u8];
        want.extend_from_slice(br#"{"b": 1.10, "aa": 2}"#);
        assert_eq!(render_binary(&d).unwrap(), want);
    }

    /// PG-T161a-02: version byte rules and typed errors, with no echo.
    #[test]
    fn jsonb_wire_input_errors_are_typed_and_never_echo() {
        let limits = test_limits();
        assert_eq!(
            parse_binary_input(&[], &limits).unwrap_err().sqlstate,
            "08P01"
        );
        assert_eq!(
            parse_binary_input(b"\x02{}", &limits).unwrap_err().sqlstate,
            "XX000"
        );
        assert!(parse_binary_input(b"\x01{}", &limits).is_ok());
        let err = parse_text_input(b"{\"k\": SECRET}", &limits, InputEdge::Parameter).unwrap_err();
        assert_eq!(err.sqlstate, "22P02");
        assert!(err.message.contains("byte offset") && !err.message.contains("SECRET"));
        let err = parse_text_input(&[b'"', 0xff, b'"'], &limits, InputEdge::Parameter).unwrap_err();
        assert_eq!(err.sqlstate, "22P02");
    }

    /// PG-T161a-04: RowDescription carries 3802 / typlen -1, `encode_value`
    /// emits the text and binary forms, and a value with no codec is a typed
    /// error, never NULL (the T-160 fake-NULL trap in `render_value`).
    #[test]
    fn jsonb_wire_row_description_and_encode_value() {
        use crate::query::{encode_value, row_description_fields};
        use ferrosa_sql::{Column, ColumnType, Value};
        let fields = row_description_fields(&[Column::new("doc", ColumnType::Jsonb)], &[1]);
        assert_eq!((fields[0].type_oid, fields[0].type_size), (3802, -1));
        assert_eq!(fields[0].format_code, 1);
        let value = Value::Jsonb(doc(r#"{"aa":2,"b":1}"#));
        let text = encode_value(0, ColumnType::Jsonb, &value).unwrap().unwrap();
        assert_eq!(text, br#"{"b": 1, "aa": 2}"#.to_vec());
        let bin = encode_value(1, ColumnType::Jsonb, &value).unwrap().unwrap();
        assert_eq!(bin[0], 1);
        assert_eq!(&bin[1..], &text[..]);
        for format in [0, 1] {
            let err = encode_value(format, ColumnType::TextArray, &Value::TextArray(vec![]))
                .expect_err("text[] has no codec yet");
            assert_eq!(err.sqlstate, "0A000");
        }
    }

    /// PG-T161a-03: a print failure maps to a limit or corruption code, never a
    /// success with partial text.
    #[test]
    fn jsonb_wire_print_errors_map_to_limit_and_corruption() {
        assert_eq!(
            print_error(&PrintError::BudgetExceeded { max: 1 }).sqlstate,
            "54000"
        );
        assert_eq!(print_error(&PrintError::Sink).sqlstate, "XX000");
        let corrupt = PrintError::Jsonb(JsonbError::UnknownEnvelope { byte: 9 });
        assert_eq!(print_error(&corrupt).sqlstate, "XX001");
    }
}
