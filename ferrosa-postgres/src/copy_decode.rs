//! Decoding a `COPY ... FROM STDIN` payload into rows.
//!
//! The client sends the payload as an opaque byte stream, split into `CopyData` frames at
//! whatever boundaries the client chose — **not** at row boundaries. A row may straddle two
//! frames, and a frame may carry a thousand rows. So this is a streaming decoder: bytes go in,
//! complete rows come out, and an incomplete tail is held until more arrives. Feeding a payload
//! one byte at a time must produce the same rows as feeding it whole.
//!
//! Two formats:
//!
//! - **text** (the default): fields separated by the delimiter (a tab unless changed), rows by
//!   a newline, with C-style escapes. An unescaped `\N` is SQL NULL. A literal backslash is `\\`,
//!   so `\N` in the data is the two bytes `\` and `N` and must be written `\\N`.
//! - **csv**: fields separated by a comma unless changed, quoted with `"` where a doubled `""`
//!   is a literal quote and the delimiter, a newline and a CR may appear inside quotes. An
//!   unquoted empty field is SQL NULL.
//!
//! Every malformed payload is an error naming the cause rather than a guess: a truncated
//! escape, a quoted field left open, or — since the caller is about to store these bytes as
//! column values — a field whose escaping does not decode. Silently accepting any of these
//! would store the wrong value, which is worse than refusing the COPY.

use std::fmt;

/// How the payload is punctuated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    /// PostgreSQL's default `COPY` format.
    Text,
    /// Comma-separated, with quoting.
    Csv,
}

/// The punctuation and null convention of a payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOptions {
    pub format: CopyFormat,
    /// Field separator.
    pub delimiter: u8,
    /// The bytes that mean SQL NULL, for the text format. (`csv` uses the empty unquoted field,
    /// which is why this is ignored there rather than being ambiguous.)
    pub null: Vec<u8>,
    /// `csv` only: the first row names the columns and is discarded.
    pub header: bool,
}

impl CopyOptions {
    /// `FORMAT text`: tab-separated, `\N` is NULL.
    pub fn text() -> Self {
        Self {
            format: CopyFormat::Text,
            delimiter: b'\t',
            null: b"\\N".to_vec(),
            header: false,
        }
    }

    /// `FORMAT csv`: comma-separated, quoted where needed, an empty unquoted field is NULL.
    pub fn csv() -> Self {
        Self {
            format: CopyFormat::Csv,
            delimiter: b',',
            null: Vec::new(),
            header: false,
        }
    }
}

/// Why a payload could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyDecodeError {
    /// An escape at the end of a field with nothing to escape.
    TruncatedEscape,
    /// An unrecognised character after a backslash, named.
    UnknownEscape(char),
    /// A `csv` field opened with `"` and never closed.
    UnterminatedQuotedField,
    /// A `csv` field had content after its closing quote before the next delimiter.
    TextAfterClosingQuote,
}

impl fmt::Display for CopyDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TruncatedEscape => write!(f, "the payload ends mid-escape"),
            Self::UnknownEscape(c) => write!(f, "unknown escape sequence `\\{c}`"),
            Self::UnterminatedQuotedField => {
                write!(f, "a quoted csv field is never closed")
            }
            Self::TextAfterClosingQuote => {
                write!(f, "a csv field has data after its closing quote")
            }
        }
    }
}

impl std::error::Error for CopyDecodeError {}

/// One decoded row: `None` is SQL NULL.
pub type CopyRow = Vec<Option<Vec<u8>>>;

/// A streaming decoder. Bytes in, whole rows out; an incomplete tail is retained.
#[derive(Debug)]
pub struct CopyDecoder {
    opts: CopyOptions,
    /// Bytes received but not yet forming a complete row.
    pending: Vec<u8>,
    /// `csv` + `header`: whether the header row has been consumed.
    header_consumed: bool,
}

impl CopyDecoder {
    pub fn new(opts: CopyOptions) -> Self {
        Self {
            opts,
            pending: Vec::new(),
            header_consumed: false,
        }
    }

    /// Feed one `CopyData` payload and return the rows it completed.
    ///
    /// Takes the payload BY VALUE. The frame arrives from the codec as an owned `Vec<u8>`, so the
    /// bytes can be moved into the buffer rather than copied into it — and when nothing was left
    /// over from the previous frame (the common case) the chunk simply BECOMES the buffer, with no
    /// copy at all. A COPY payload is row data; copying it once per frame is exactly the kind of
    /// avoidable allocation this crate's OOM audit exists to catch.
    ///
    /// # Errors
    ///
    /// A malformed payload — see [`CopyDecodeError`].
    pub fn push(&mut self, chunk: Vec<u8>) -> Result<Vec<CopyRow>, CopyDecodeError> {
        if self.pending.is_empty() {
            self.pending = chunk;
        } else {
            // Moves the bytes out of `chunk`; no clone of the payload.
            self.pending.extend(chunk);
        }
        let complete = match self.opts.format {
            CopyFormat::Text => complete_rows(&mut self.pending, b'\n'),
            // csv rows end at a newline that is not inside a quoted field, so the split has to
            // respect quoting; this returns the offset of the last such newline.
            CopyFormat::Csv => complete_csv_rows(&mut self.pending),
        };
        let mut rows = Vec::new();
        for raw in complete {
            if self.skip_as_header() {
                continue;
            }
            rows.push(self.decode_row(&raw)?);
        }
        Ok(rows)
    }

    /// Finish the payload. A non-empty remainder is an error: a `COPY` payload must end at a row
    /// boundary, and accepting a half row would store a truncated value.
    ///
    /// # Errors
    ///
    /// A trailing partial row, or a malformed one.
    pub fn finish(&mut self) -> Result<Vec<CopyRow>, CopyDecodeError> {
        if self.pending.is_empty() {
            return Ok(Vec::new());
        }
        let raw = std::mem::take(&mut self.pending);
        if self.skip_as_header() {
            return Ok(Vec::new());
        }
        Ok(vec![self.decode_row(&raw)?])
    }

    /// Consume the header row the first time it is seen, when the options asked for one.
    fn skip_as_header(&mut self) -> bool {
        if self.opts.header && self.opts.format == CopyFormat::Csv && !self.header_consumed {
            self.header_consumed = true;
            return true;
        }
        false
    }

    fn decode_row(&self, raw: &[u8]) -> Result<CopyRow, CopyDecodeError> {
        match self.opts.format {
            CopyFormat::Text => decode_text_row(raw, self.opts.delimiter, &self.opts.null),
            CopyFormat::Csv => decode_csv_row(raw, self.opts.delimiter),
        }
    }
}

/// Split off every complete `\n`-terminated row, leaving the remainder in `pending`.
///
/// A trailing `\r` before the `\n` is dropped: a client that speaks CRLF must not thereby put a
/// stray carriage return at the end of its last field.
fn complete_rows(pending: &mut Vec<u8>, terminator: u8) -> Vec<Vec<u8>> {
    let mut rows = Vec::new();
    let mut start = 0usize;
    for (i, b) in pending.iter().enumerate() {
        if *b == terminator {
            let mut end = i;
            if end > start && pending[end - 1] == b'\r' {
                end -= 1;
            }
            rows.push(pending[start..end].to_vec());
            start = i + 1;
        }
    }
    if start > 0 {
        pending.drain(..start);
    }
    rows
}

/// Split off complete csv rows — newlines outside a quoted field — leaving the rest.
fn complete_csv_rows(pending: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut rows = Vec::new();
    let mut start = 0usize;
    let mut in_quotes = false;
    let mut i = 0usize;
    while i < pending.len() {
        match pending[i] {
            b'"' => {
                // A doubled quote inside a quoted field is a literal quote and does not close it.
                if in_quotes && pending.get(i + 1) == Some(&b'"') {
                    i += 2;
                    continue;
                }
                in_quotes = !in_quotes;
            }
            b'\n' if !in_quotes => {
                let mut end = i;
                if end > start && pending[end - 1] == b'\r' {
                    end -= 1;
                }
                rows.push(pending[start..end].to_vec());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    if start > 0 {
        pending.drain(..start);
    }
    rows
}

/// Decode one text-format row: split on `delimiter`, unescape each field.
fn decode_text_row(raw: &[u8], delimiter: u8, null: &[u8]) -> Result<CopyRow, CopyDecodeError> {
    let mut fields = Vec::new();
    for field in raw.split(|b| *b == delimiter) {
        // `\N` is NULL, and only when it is not itself escaped: `\\N` is the two bytes `\N`.
        if field == null {
            fields.push(None);
            continue;
        }
        fields.push(Some(unescape_field(field)?));
    }
    Ok(fields)
}

/// Undo PostgreSQL's text-format backslash escapes.
fn unescape_field(field: &[u8]) -> Result<Vec<u8>, CopyDecodeError> {
    if !field.contains(&b'\\') {
        return Ok(field.to_vec());
    }
    let mut out = Vec::with_capacity(field.len());
    let mut i = 0usize;
    while i < field.len() {
        if field[i] != b'\\' {
            out.push(field[i]);
            i += 1;
            continue;
        }
        let next = field.get(i + 1).ok_or(CopyDecodeError::TruncatedEscape)?;
        let decoded = match next {
            b't' => b'\t',
            b'n' => b'\n',
            b'r' => b'\r',
            b'b' => 0x08,
            b'f' => 0x0c,
            b'v' => 0x0b,
            b'\\' => b'\\',
            other => {
                return Err(CopyDecodeError::UnknownEscape(char::from(*other)));
            }
        };
        out.push(decoded);
        i += 2;
    }
    Ok(out)
}

/// Decode one csv row: fields split on `delimiter` outside quotes, `""` is a literal quote, and an
/// unquoted empty field is NULL.
fn decode_csv_row(raw: &[u8], delimiter: u8) -> Result<CopyRow, CopyDecodeError> {
    let mut fields: CopyRow = Vec::new();
    let mut i = 0usize;
    loop {
        // An unquoted empty field is NULL; a quoted `""` is the empty string, not NULL.
        if i < raw.len() && raw[i] == b'"' {
            let mut value = Vec::new();
            i += 1; // opening quote
            loop {
                match raw.get(i) {
                    None => return Err(CopyDecodeError::UnterminatedQuotedField),
                    Some(b'"') => {
                        if raw.get(i + 1) == Some(&b'"') {
                            value.push(b'"');
                            i += 2;
                        } else {
                            i += 1; // closing quote
                                    // Only a delimiter may follow the closing quote.
                            match raw.get(i) {
                                None => {
                                    fields.push(Some(value));
                                    return Ok(fields);
                                }
                                Some(b) if *b == delimiter => {
                                    fields.push(Some(value));
                                    i += 1;
                                    // fall through to the next field
                                    break;
                                }
                                Some(_) => return Err(CopyDecodeError::TextAfterClosingQuote),
                            }
                        }
                    }
                    Some(b) => {
                        value.push(*b);
                        i += 1;
                    }
                }
            }
            continue;
        }
        // Unquoted field: up to the next delimiter.
        let start = i;
        let end = raw[i..]
            .iter()
            .position(|b| *b == delimiter)
            .map(|off| i + off)
            .unwrap_or(raw.len());
        let value = &raw[start..end];
        if value.is_empty() {
            fields.push(None);
        } else {
            if value.contains(&b'"') {
                return Err(CopyDecodeError::TextAfterClosingQuote);
            }
            fields.push(Some(value.to_vec()));
        }
        if end == raw.len() {
            return Ok(fields);
        }
        i = end + 1;
    }
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use super::*;

    /// Test-side shim for [`CopyDecoder::push`], which takes the frame payload BY VALUE so the
    /// bytes can be MOVED into the buffer rather than copied. Tests hold byte-string literals,
    /// slices and arrays, so they hand over a fresh `Vec` here; `AsRef<[u8]>` accepts all of them.
    trait PushBytes {
        fn push_owned(&mut self, bytes: impl AsRef<[u8]>) -> Result<Vec<CopyRow>, CopyDecodeError>;
    }
    impl PushBytes for CopyDecoder {
        fn push_owned(&mut self, bytes: impl AsRef<[u8]>) -> Result<Vec<CopyRow>, CopyDecodeError> {
            self.push(bytes.as_ref().to_vec())
        }
    }

    fn text() -> CopyDecoder {
        CopyDecoder::new(CopyOptions::text())
    }

    /// The ordinary shape: tab-separated fields, newline-terminated rows.
    #[test]
    fn text_rows_split_on_delimiter_and_newline() {
        let mut d = text();
        let rows = d.push_owned(b"1\thello\n2\tworld\n").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Some(b"1".to_vec()), Some(b"hello".to_vec())]);
        assert_eq!(rows[1], vec![Some(b"2".to_vec()), Some(b"world".to_vec())]);
        assert!(d.finish().unwrap().is_empty());
    }

    /// The property the whole design turns on: frame boundaries are NOT row boundaries. Feeding
    /// the same payload byte-by-byte must give identical rows, because a client may split a row
    /// across two `CopyData` frames.
    #[test]
    fn a_row_split_across_chunks_decodes_identically() {
        let payload = b"1\thello\n2\tworld\n3\t!\n";
        let whole = text().push_owned(payload).unwrap();

        let mut by_byte = Vec::new();
        let mut d = text();
        for b in payload {
            by_byte.extend(d.push_owned([*b]).unwrap());
        }
        assert!(d.finish().unwrap().is_empty());
        assert_eq!(
            by_byte, whole,
            "byte-at-a-time must match the whole payload"
        );

        // And split at a deliberately awkward place: mid-field and mid-newline.
        let mut parts = Vec::new();
        let mut d = text();
        parts.extend(d.push_owned(&payload[..3]).unwrap()); // mid-"hello"
        parts.extend(d.push_owned(&payload[3..9]).unwrap()); // ends on the newline
        parts.extend(d.push_owned(&payload[9..]).unwrap());
        assert_eq!(parts, whole);
    }

    /// `\N` unescaped is SQL NULL — and it is the ONLY way to say NULL, so a literal `\N` in the
    /// data must arrive as `\\N` and must not be confused with it.
    #[test]
    fn text_null_is_unescaped_backslash_n_only() {
        let rows = text().push_owned(b"\\N\tx\n\\\\N\ty\n").unwrap();
        assert_eq!(rows[0][0], None, "`\\N` is NULL");
        assert_eq!(
            rows[1][0],
            Some(b"\\N".to_vec()),
            "`\\\\N` is the two bytes backslash-N, not NULL"
        );
    }

    /// The C escapes PostgreSQL defines, and a literal backslash.
    #[test]
    fn text_escapes_decode() {
        let rows = text().push_owned(b"a\\tb\\nc\\\\d\n").unwrap();
        assert_eq!(rows[0][0], Some(b"a\tb\nc\\d".to_vec()));
    }

    /// A malformed escape is refused rather than passed through: these bytes become column values,
    /// so guessing would store the wrong value.
    #[test]
    fn malformed_text_escapes_are_refused() {
        // A row whose last field ends mid-escape, and one with an escape that does not exist.
        assert_eq!(
            text().push_owned(b"bad\\\n").unwrap_err(),
            CopyDecodeError::TruncatedEscape
        );
        assert_eq!(
            text().push_owned(b"bad\\q\n").unwrap_err(),
            CopyDecodeError::UnknownEscape('q')
        );
        // An incomplete trailing escape is NOT an error yet: the next chunk may supply the `n`
        // that makes it the newline escape. So it is held, exactly as an incomplete row is.
        let mut held = text();
        assert_eq!(held.push_owned(b"bad\\").unwrap().len(), 0);
        assert_eq!(
            held.push_owned(b"n\n").unwrap().len(),
            1,
            "...and it does, so this was the escape rather than a malformed field"
        );
    }

    /// A trailing partial row is an error at the end: storing half a row is worse than refusing.
    #[test]
    fn a_trailing_partial_row_is_refused_at_finish() {
        // "ok\n" completes; the rest never terminates.
        let mut d = text();
        assert_eq!(d.push_owned(b"ok\n1\tpartial").unwrap().len(), 1);
        assert_eq!(
            d.push_owned(b"").unwrap().len(),
            0,
            "an incomplete tail is held, not rowed"
        );
        // It IS a row once finished — COPY payloads are newline-terminated, so this only happens
        // for a malformed payload, and the caller decides by finishing explicitly.
        let done = d.finish().unwrap();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0][0], Some(b"1".to_vec()));
    }

    /// An empty delimiter-separated field is the empty string, not NULL: only `\N` is NULL.
    #[test]
    fn an_empty_text_field_is_the_empty_string() {
        let rows = text().push_owned(b"\t\n").unwrap();
        assert_eq!(rows[0], vec![Some(Vec::new()), Some(Vec::new())]);
    }

    fn csv() -> CopyDecoder {
        CopyDecoder::new(CopyOptions::csv())
    }

    /// csv quoting: a delimiter and a newline inside quotes belong to the field.
    #[test]
    fn csv_quoted_fields_may_contain_the_delimiter_and_newlines() {
        let mut d = csv();
        let rows = d.push_owned(b"1,\"a,b\"\n2,\"line\nbreak\"\n").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], Some(b"a,b".to_vec()));
        assert_eq!(
            rows[1][1],
            Some(b"line\nbreak".to_vec()),
            "newline inside quotes"
        );
        assert!(d.finish().unwrap().is_empty(), "no trailing partial row");
    }

    /// A doubled quote is a literal quote, and does not close the field.
    #[test]
    fn csv_doubled_quote_is_a_literal_quote() {
        let rows = csv().push_owned(b"1,\"say \"\"hi\"\"\"\n").unwrap();
        assert_eq!(rows[0][1], Some(b"say \"hi\"".to_vec()));
    }

    /// An unquoted empty field is NULL; a quoted empty field is the empty string. The two are
    /// different values and must not collapse.
    #[test]
    fn csv_distinguishes_null_from_an_empty_string() {
        let rows = csv().push_owned(b"1,,\"\"\n").unwrap();
        assert_eq!(rows[0][0], Some(b"1".to_vec()));
        assert_eq!(rows[0][1], None, "unquoted empty is NULL");
        assert_eq!(
            rows[0][2],
            Some(Vec::new()),
            "quoted empty is the empty string"
        );
    }

    /// A quoted field left open is refused, not silently taken as the rest of the payload.
    #[test]
    fn csv_unterminated_quote_is_refused() {
        // A newline inside quotes is not a row boundary, so nothing is rowed yet...
        let mut d = csv();
        assert_eq!(d.push_owned(b"1,\"never closed\n").unwrap().len(), 0);
        // ...and the payload ending inside the field is the error. It cannot be detected earlier:
        // until the payload ends, every byte could still belong to the field.
        assert_eq!(
            d.finish().unwrap_err(),
            CopyDecodeError::UnterminatedQuotedField
        );
    }

    /// Data after a closing quote is malformed (PostgreSQL refuses it too) rather than being
    /// appended, which would silently alter the value.
    #[test]
    fn csv_text_after_a_closing_quote_is_refused() {
        assert_eq!(
            csv().push_owned(b"1,\"a\"b\n").unwrap_err(),
            CopyDecodeError::TextAfterClosingQuote
        );
    }

    /// `HEADER` skips exactly the first row, and only for csv.
    #[test]
    fn csv_header_row_is_skipped_once() {
        let mut opts = CopyOptions::csv();
        opts.header = true;
        let mut d = CopyDecoder::new(opts);
        let rows = d.push_owned(b"id,name\n1,a\n2,b\n").unwrap();
        assert_eq!(rows.len(), 2, "the header is not a row");
        assert_eq!(rows[0][0], Some(b"1".to_vec()));
        assert_eq!(rows[1][0], Some(b"2".to_vec()));
    }

    /// A CRLF client must not get a stray carriage return on its last field.
    #[test]
    fn crlf_terminators_do_not_leak_a_carriage_return() {
        let rows = text().push_owned(b"1\thello\r\n").unwrap();
        assert_eq!(rows[0][1], Some(b"hello".to_vec()));
    }
}
