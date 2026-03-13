//! Thin CQL client for ferrosa-ctl.
//!
//! Reuses [`CqlCodec`] for framing. Implements the minimum needed to
//! connect, authenticate, and execute queries against a Ferrosa node.

use std::net::SocketAddr;

use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::error::CqlError;
use crate::frame::{
    CqlCodec, CqlFrame, FrameHeader, Opcode, DEFAULT_MAX_FRAME_SIZE, VERSION_REQUEST,
};

/// A row returned by a query.
#[derive(Debug, Clone)]
pub struct ResultRow {
    pub columns: Vec<Option<Vec<u8>>>,
}

/// Query result.
#[derive(Debug)]
pub struct QueryResult {
    pub rows: Vec<ResultRow>,
    pub column_names: Vec<String>,
}

/// Minimal CQL client.
pub struct CqlClient {
    framed: Framed<TcpStream, CqlCodec>,
    stream_counter: i16,
    ready: bool,
}

impl CqlClient {
    /// Connect to a CQL server and complete the STARTUP handshake.
    pub async fn connect(addr: SocketAddr) -> Result<Self, CqlError> {
        let stream = TcpStream::connect(addr).await?;
        let codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        let mut framed = Framed::new(stream, codec);

        // Build STARTUP body: string map {"CQL_VERSION": "3.0.0"}
        let mut body = BytesMut::new();
        body.put_u16(1); // 1 entry
        let key = b"CQL_VERSION";
        body.put_u16(key.len() as u16);
        body.put_slice(key);
        let val = b"3.0.0";
        body.put_u16(val.len() as u16);
        body.put_slice(val);

        let frame = CqlFrame {
            header: FrameHeader {
                version: VERSION_REQUEST,
                flags: 0,
                stream_id: 0,
                opcode: Opcode::Startup,
                length: body.len() as u32,
            },
            body: body.freeze(),
        };

        framed.send(frame).await?;

        // Read response — expect READY or AUTHENTICATE.
        let resp = framed
            .next()
            .await
            .ok_or_else(|| CqlError::Protocol("connection closed during startup".into()))?
            .map_err(|e| CqlError::Protocol(format!("startup response error: {e}")))?;

        let ready = resp.header.opcode == Opcode::Ready;

        if !ready && resp.header.opcode != Opcode::Authenticate {
            return Err(CqlError::Protocol(format!(
                "unexpected startup response: {:?}",
                resp.header.opcode
            )));
        }

        Ok(Self {
            framed,
            stream_counter: 1,
            ready,
        })
    }

    /// Returns true if the connection completed the handshake with READY.
    pub fn is_ready(&self) -> bool {
        self.ready
    }

    /// Execute a CQL query and return the result.
    pub async fn query(&mut self, cql: &str) -> Result<QueryResult, CqlError> {
        let stream_id = self.next_stream_id();

        // Build QUERY frame body: long-string query + minimal parameters.
        let mut body = BytesMut::new();
        body.put_i32(cql.len() as i32);
        body.put_slice(cql.as_bytes());
        // Query parameters: consistency ONE, no flags.
        body.put_u16(1); // consistency: ONE
        body.put_u8(0); // flags: none

        let frame = CqlFrame {
            header: FrameHeader {
                version: VERSION_REQUEST,
                flags: 0,
                stream_id,
                opcode: Opcode::Query,
                length: body.len() as u32,
            },
            body: body.freeze(),
        };

        self.framed.send(frame).await?;

        let resp = self
            .framed
            .next()
            .await
            .ok_or_else(|| CqlError::Protocol("connection closed".into()))?
            .map_err(|e| CqlError::Protocol(format!("query response error: {e}")))?;

        if resp.header.opcode == Opcode::Error {
            let error_msg = parse_error(&resp.body)?;
            return Err(CqlError::ServerError(error_msg));
        }

        parse_result(&resp.body)
    }

    fn next_stream_id(&mut self) -> i16 {
        let id = self.stream_counter;
        self.stream_counter = self.stream_counter.wrapping_add(1);
        if self.stream_counter < 0 {
            self.stream_counter = 1;
        }
        id
    }
}

/// Parse a CQL ERROR response body into a human-readable message.
fn parse_error(body: &[u8]) -> Result<String, CqlError> {
    if body.len() < 4 {
        return Ok("unknown error".to_string());
    }
    let mut cursor = body;
    let _code = cursor.get_i32();
    if cursor.len() < 2 {
        return Ok("unknown error".to_string());
    }
    let msg_len = cursor.get_u16() as usize;
    if cursor.len() < msg_len {
        return Ok("truncated error message".to_string());
    }
    let msg = std::str::from_utf8(&cursor[..msg_len])
        .unwrap_or("invalid utf8")
        .to_string();
    Ok(msg)
}

/// Parse a CQL RESULT response body.
fn parse_result(body: &[u8]) -> Result<QueryResult, CqlError> {
    if body.len() < 4 {
        return Ok(QueryResult {
            rows: vec![],
            column_names: vec![],
        });
    }
    let mut cursor = body;
    let kind = cursor.get_i32();

    match kind {
        // VOID
        1 => Ok(QueryResult {
            rows: vec![],
            column_names: vec![],
        }),
        // ROWS
        2 => parse_rows(cursor),
        // SET_KEYSPACE, PREPARED, SCHEMA_CHANGE — all return empty for our purposes.
        _ => Ok(QueryResult {
            rows: vec![],
            column_names: vec![],
        }),
    }
}

/// Parse the ROWS result kind from a CQL RESULT body (after the kind i32).
fn parse_rows(mut cursor: &[u8]) -> Result<QueryResult, CqlError> {
    if cursor.len() < 8 {
        return Ok(QueryResult {
            rows: vec![],
            column_names: vec![],
        });
    }
    let flags = cursor.get_i32();
    let col_count = cursor.get_i32() as usize;

    let has_global_table_spec = flags & 0x0001 != 0;

    // Skip global table spec if present.
    if has_global_table_spec {
        skip_short_string(&mut cursor)?;
        skip_short_string(&mut cursor)?;
    }

    // Read column specs.
    let mut column_names = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        if !has_global_table_spec {
            // Per-column keyspace + table names.
            skip_short_string(&mut cursor)?;
            skip_short_string(&mut cursor)?;
        }
        // Column name.
        let name = read_short_string(&mut cursor)?;
        column_names.push(name);
        // Type option: [short id] possibly followed by sub-types.
        skip_type_option(&mut cursor)?;
    }

    // Row count.
    if cursor.len() < 4 {
        return Ok(QueryResult {
            rows: vec![],
            column_names,
        });
    }
    let row_count = cursor.get_i32() as usize;

    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            if cursor.len() < 4 {
                break;
            }
            let cell_len = cursor.get_i32();
            if cell_len < 0 {
                columns.push(None);
            } else {
                let len = cell_len as usize;
                if cursor.len() >= len {
                    columns.push(Some(cursor[..len].to_vec()));
                    cursor.advance(len);
                } else {
                    columns.push(None);
                }
            }
        }
        rows.push(ResultRow { columns });
    }

    Ok(QueryResult { rows, column_names })
}

/// Read a CQL `[short] [bytes]` string from the cursor.
fn read_short_string(cursor: &mut &[u8]) -> Result<String, CqlError> {
    if cursor.len() < 2 {
        return Err(CqlError::Protocol("truncated short string".into()));
    }
    let len = cursor.get_u16() as usize;
    if cursor.len() < len {
        return Err(CqlError::Protocol("truncated short string body".into()));
    }
    let s = std::str::from_utf8(&cursor[..len])
        .unwrap_or("?")
        .to_string();
    cursor.advance(len);
    Ok(s)
}

/// Skip over a CQL short string without returning it.
fn skip_short_string(cursor: &mut &[u8]) -> Result<(), CqlError> {
    if cursor.len() < 2 {
        return Err(CqlError::Protocol("truncated short string".into()));
    }
    let len = cursor.get_u16() as usize;
    if cursor.len() < len {
        return Err(CqlError::Protocol("truncated short string body".into()));
    }
    cursor.advance(len);
    Ok(())
}

/// Skip a CQL type option (id + possible sub-types for collections).
fn skip_type_option(cursor: &mut &[u8]) -> Result<(), CqlError> {
    if cursor.len() < 2 {
        return Err(CqlError::Protocol("truncated type option".into()));
    }
    let type_id = cursor.get_u16();
    match type_id {
        // list (0x0020), set (0x0022) — one sub-type
        0x0020 | 0x0022 => {
            skip_type_option(cursor)?;
        }
        // map (0x0021) — two sub-types
        0x0021 => {
            skip_type_option(cursor)?;
            skip_type_option(cursor)?;
        }
        // tuple (0x0031) — n sub-types
        0x0031 => {
            if cursor.len() < 2 {
                return Err(CqlError::Protocol("truncated tuple type".into()));
            }
            let n = cursor.get_u16() as usize;
            for _ in 0..n {
                skip_type_option(cursor)?;
            }
        }
        // UDT (0x0030) — keyspace + name + n fields
        0x0030 => {
            skip_short_string(cursor)?; // keyspace
            skip_short_string(cursor)?; // name
            if cursor.len() < 2 {
                return Err(CqlError::Protocol("truncated UDT type".into()));
            }
            let n = cursor.get_u16() as usize;
            for _ in 0..n {
                skip_short_string(cursor)?; // field name
                skip_type_option(cursor)?; // field type
            }
        }
        // All other types are simple (no sub-types).
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_row_clone() {
        let row = ResultRow {
            columns: vec![Some(b"hello".to_vec()), None],
        };
        let cloned = row.clone();
        assert_eq!(cloned.columns.len(), 2);
        assert_eq!(cloned.columns[0].as_deref(), Some(b"hello".as_slice()));
        assert!(cloned.columns[1].is_none());
    }

    #[test]
    fn query_result_empty() {
        let result = QueryResult {
            rows: vec![],
            column_names: vec!["id".to_string()],
        };
        assert!(result.rows.is_empty());
        assert_eq!(result.column_names.len(), 1);
    }

    #[test]
    fn parse_error_extracts_message() {
        let mut body = BytesMut::new();
        body.put_i32(0x2000); // syntax error code
        let msg = b"bad query";
        body.put_u16(msg.len() as u16);
        body.put_slice(msg);
        let result = parse_error(&body).unwrap();
        assert_eq!(result, "bad query");
    }

    #[test]
    fn parse_error_short_body() {
        let body = [0u8; 2]; // too short for error code
        let result = parse_error(&body).unwrap();
        assert_eq!(result, "unknown error");
    }

    #[test]
    fn parse_error_truncated_message() {
        let mut body = BytesMut::new();
        body.put_i32(0x0000);
        body.put_u16(100); // claims 100 bytes but none follow
        let result = parse_error(&body).unwrap();
        assert_eq!(result, "truncated error message");
    }

    #[test]
    fn parse_result_void() {
        let mut body = BytesMut::new();
        body.put_i32(1); // VOID
        let result = parse_result(&body).unwrap();
        assert!(result.rows.is_empty());
        assert!(result.column_names.is_empty());
    }

    #[test]
    fn parse_result_empty_body() {
        let result = parse_result(&[]).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn parse_result_set_keyspace() {
        let mut body = BytesMut::new();
        body.put_i32(3); // SET_KEYSPACE
        let result = parse_result(&body).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn parse_rows_single_column_single_row() {
        // Build a minimal ROWS result body (after the kind i32).
        let mut buf = BytesMut::new();
        // Metadata flags: has global table spec
        buf.put_i32(0x0001);
        // Column count
        buf.put_i32(1);
        // Global table spec: keyspace + table
        let ks = b"test_ks";
        buf.put_u16(ks.len() as u16);
        buf.put_slice(ks);
        let tbl = b"test_tbl";
        buf.put_u16(tbl.len() as u16);
        buf.put_slice(tbl);
        // Column spec: name
        let col_name = b"name";
        buf.put_u16(col_name.len() as u16);
        buf.put_slice(col_name);
        // Column type: varchar (0x000D)
        buf.put_u16(0x000D);
        // Row count
        buf.put_i32(1);
        // Row data: one cell
        let cell = b"alice";
        buf.put_i32(cell.len() as i32);
        buf.put_slice(cell);

        let result = parse_rows(&buf).unwrap();
        assert_eq!(result.column_names, vec!["name"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].columns[0].as_deref(),
            Some(b"alice".as_slice())
        );
    }

    #[test]
    fn parse_rows_null_cell() {
        let mut buf = BytesMut::new();
        buf.put_i32(0x0001); // global table spec
        buf.put_i32(1); // 1 column
        buf.put_u16(2);
        buf.put_slice(b"ks");
        buf.put_u16(1);
        buf.put_slice(b"t");
        buf.put_u16(1);
        buf.put_slice(b"c");
        buf.put_u16(0x000D); // varchar
        buf.put_i32(1); // 1 row
        buf.put_i32(-1); // null cell

        let result = parse_rows(&buf).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(result.rows[0].columns[0].is_none());
    }

    #[test]
    fn parse_rows_without_global_table_spec() {
        let mut buf = BytesMut::new();
        buf.put_i32(0x0000); // no global table spec
        buf.put_i32(1); // 1 column
                        // Per-column: keyspace, table, name, type
        buf.put_u16(2);
        buf.put_slice(b"ks");
        buf.put_u16(1);
        buf.put_slice(b"t");
        buf.put_u16(2);
        buf.put_slice(b"id");
        buf.put_u16(0x0009); // int
        buf.put_i32(1); // 1 row
        buf.put_i32(4); // 4-byte int
        buf.put_i32(42);

        let result = parse_rows(&buf).unwrap();
        assert_eq!(result.column_names, vec!["id"]);
        assert_eq!(result.rows.len(), 1);
        let val = result.rows[0].columns[0].as_ref().unwrap();
        assert_eq!(val.len(), 4);
    }

    #[test]
    fn skip_type_option_list() {
        // list<varchar>
        let mut buf = BytesMut::new();
        buf.put_u16(0x0020); // list
        buf.put_u16(0x000D); // varchar
        let mut cursor = &buf[..];
        skip_type_option(&mut cursor).unwrap();
        assert!(cursor.is_empty());
    }

    #[test]
    fn skip_type_option_map() {
        // map<varchar, int>
        let mut buf = BytesMut::new();
        buf.put_u16(0x0021); // map
        buf.put_u16(0x000D); // varchar
        buf.put_u16(0x0009); // int
        let mut cursor = &buf[..];
        skip_type_option(&mut cursor).unwrap();
        assert!(cursor.is_empty());
    }
}
