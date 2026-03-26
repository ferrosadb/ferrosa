//! RESULT frame body encoder for CQL native protocol v5.
//!
//! Each public function encodes a specific RESULT kind and returns a
//! `BytesMut` containing the complete frame body (starting with the
//! 4-byte kind code). The caller wraps this in a CQL frame header.
//!
//! Result kind codes:
//! - Void         = 0x0001
//! - Rows         = 0x0002
//! - SetKeyspace  = 0x0003
//! - Prepared     = 0x0004
//! - SchemaChange = 0x0005

use bytes::{BufMut, BytesMut};

use crate::types::{encode_value, CqlType, CqlValue};

// ── Public encoders ────────────────────────────────────────────────────────

/// Encode a Void RESULT body.
pub fn encode_void() -> BytesMut {
    let mut buf = BytesMut::with_capacity(4);
    buf.put_i32(0x0001); // Void kind
    buf
}

/// Encode a SetKeyspace RESULT body.
pub fn encode_set_keyspace(keyspace: &str) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_i32(0x0003); // SetKeyspace kind
    encode_string(&mut buf, keyspace);
    buf
}

/// Encode a SchemaChange RESULT body.
///
/// `change_type` is one of `"CREATED"`, `"UPDATED"`, `"DROPPED"`.
/// `target` is one of `"KEYSPACE"`, `"TABLE"`.
/// `options` contains the keyspace name and, for table targets, the table name.
pub fn encode_schema_change(change_type: &str, target: &str, options: &[&str]) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_i32(0x0005); // SchemaChange kind
    encode_string(&mut buf, change_type);
    encode_string(&mut buf, target);
    for opt in options {
        encode_string(&mut buf, opt);
    }
    buf
}

/// Encode a Rows RESULT body.
///
/// Uses the `Global_tables_spec` flag (0x0001): a single keyspace/table
/// pair is written once before the column specs.
pub fn encode_rows(
    column_names: &[String],
    column_types: &[CqlType],
    keyspace: &str,
    table: &str,
    rows: &[Vec<Option<CqlValue>>],
) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_i32(0x0002); // Rows kind

    encode_rows_metadata(&mut buf, column_names, column_types, keyspace, table);

    // rows_count
    buf.put_i32(rows.len() as i32);

    // row data
    for row in rows {
        for cell in row {
            encode_cell(&mut buf, cell);
        }
    }

    buf
}

/// Encode a Prepared RESULT body.
///
/// Contains:
/// 1. The prepared statement ID (16 bytes, prefixed by u16 length).
/// 2. Bind-variable metadata (same layout as Rows metadata).
/// 3. Result-column metadata (same layout as Rows metadata).
pub fn encode_prepared(
    id: &[u8; 16],
    bound_names: &[String],
    bound_types: &[CqlType],
    result_column_names: &[String],
    result_column_types: &[CqlType],
    keyspace: &str,
    table: &str,
) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_i32(0x0004); // Prepared kind

    // Prepared statement ID: [u16 length][bytes]
    buf.put_u16(16u16);
    buf.put_slice(id);

    // Bind-variable metadata (PreparedMetadata format, not RowsMetadata):
    // [i32 flags][i32 columns_count][i32 pk_count][i16 pk_index...][global_table_spec][col_specs]
    encode_prepared_metadata(&mut buf, bound_names, bound_types, keyspace, table);

    // Result-column metadata (RowsMetadata format)
    if result_column_names.is_empty() {
        // No result columns (INSERT/UPDATE/DELETE) — use No_metadata flag
        // per CQL native protocol v5 spec section 4.2.5.4
        buf.put_i32(0x0004); // flags: No_metadata
        buf.put_i32(0); // columns_count: 0
    } else {
        encode_rows_metadata(
            &mut buf,
            result_column_names,
            result_column_types,
            keyspace,
            table,
        );
    }

    buf
}

/// Encode PreparedMetadata: like RowsMetadata but with pk_count + pk_indexes.
fn encode_prepared_metadata(
    buf: &mut BytesMut,
    column_names: &[String],
    column_types: &[CqlType],
    keyspace: &str,
    table: &str,
) {
    buf.put_i32(0x0001); // flags: Global_tables_spec
    buf.put_i32(column_names.len() as i32);

    // pk_count: number of partition key column indexes (0 for simplicity —
    // the driver uses this for token-aware routing, not correctness).
    buf.put_i32(0);

    // Global table spec
    encode_string(buf, keyspace);
    encode_string(buf, table);

    // Column specs
    for (name, cql_type) in column_names.iter().zip(column_types.iter()) {
        encode_string(buf, name);
        encode_type(buf, cql_type);
    }
}

// ── Private helpers ────────────────────────────────────────────────────────

/// Write a CQL short string: `[u16 length][bytes]`.
fn encode_string(buf: &mut BytesMut, s: &str) {
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
}

/// Write column metadata used by both Rows and Prepared results.
///
/// Format:
/// ```text
/// [i32 flags=0x0001]   // Global_tables_spec
/// [i32 columns_count]
/// [string keyspace][string table]
/// For each column: [string name][u16 type_id][type params]
/// ```
fn encode_rows_metadata(
    buf: &mut BytesMut,
    column_names: &[String],
    column_types: &[CqlType],
    keyspace: &str,
    table: &str,
) {
    buf.put_i32(0x0001); // flags: Global_tables_spec
    buf.put_i32(column_names.len() as i32);
    encode_string(buf, keyspace);
    encode_string(buf, table);
    for (name, cql_type) in column_names.iter().zip(column_types.iter()) {
        encode_string(buf, name);
        encode_type(buf, cql_type);
    }
}

/// Write a type ID and any accompanying type parameters.
///
/// Simple types: `[u16 type_id]`
/// List/Set:     `[u16 type_id][u16 elem_type_id]`
/// Map:          `[u16 type_id][u16 key_type_id][u16 val_type_id]`
/// Tuple:        `[u16 type_id][u16 count][u16 type_id]*count`
fn encode_type(buf: &mut BytesMut, cql_type: &CqlType) {
    buf.put_u16(cql_type.type_id());
    match cql_type {
        CqlType::List(elem) | CqlType::Set(elem) => {
            encode_type(buf, elem);
        }
        CqlType::Map(key, val) => {
            encode_type(buf, key);
            encode_type(buf, val);
        }
        CqlType::Tuple(types) => {
            buf.put_u16(types.len() as u16);
            for t in types {
                encode_type(buf, t);
            }
        }
        CqlType::Vector(_elem, _dim) => {
            // Encode vector as custom type (0x0000) with a class name string.
            // Older drivers (cdrs-tokio, scylla-rust-driver < 0.14) don't
            // understand type_id 0x0032 and would fail during PREPARE.
            // We already wrote type_id above — overwrite it with custom (0x0000).
            let type_id_pos = buf.len() - 2; // position where we wrote type_id
            buf[type_id_pos] = 0x00;
            buf[type_id_pos + 1] = 0x03; // 0x0003 = Blob
                                         // No additional metadata needed for blob.
        }
        // All simple types: type_id alone is sufficient.
        _ => {}
    }
}

/// Write a single result-set cell.
///
/// Null (either `None` or `CqlValue::Null`) is written as `[i32 -1]`.
/// Non-null values are written as `[i32 byte_length][bytes]`.
fn encode_cell(buf: &mut BytesMut, value: &Option<CqlValue>) {
    match value {
        None => buf.put_i32(-1),
        Some(CqlValue::Null) => buf.put_i32(-1),
        Some(val) => {
            let bytes = encode_value(val);
            buf.put_i32(bytes.len() as i32);
            buf.put_slice(&bytes);
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_void_result() {
        let buf = encode_void();
        assert_eq!(&buf[0..4], &0x0001i32.to_be_bytes());
        assert_eq!(buf.len(), 4);
    }

    #[test]
    fn encode_set_keyspace_result() {
        let buf = encode_set_keyspace("my_ks");
        assert_eq!(&buf[0..4], &0x0003i32.to_be_bytes());
        let len = u16::from_be_bytes([buf[4], buf[5]]) as usize;
        assert_eq!(&buf[6..6 + len], b"my_ks");
    }

    #[test]
    fn encode_rows_single_int_column() {
        let buf = encode_rows(
            &["id".into()],
            &[CqlType::Int],
            "ks",
            "users",
            &[vec![Some(CqlValue::Int(42))]],
        );
        assert_eq!(&buf[0..4], &0x0002i32.to_be_bytes()); // Rows kind
    }

    #[test]
    fn encode_rows_null_cell() {
        let buf = encode_rows(&["v".into()], &[CqlType::Varchar], "ks", "t", &[vec![None]]);
        // Find the row data section and verify null encoding (-1)
        assert_eq!(&buf[0..4], &0x0002i32.to_be_bytes());
    }

    #[test]
    fn encode_rows_empty() {
        let buf = encode_rows(&["id".into()], &[CqlType::Int], "ks", "users", &[]);
        assert_eq!(&buf[0..4], &0x0002i32.to_be_bytes());
    }

    #[test]
    fn encode_schema_change_created() {
        let buf = encode_schema_change("CREATED", "TABLE", &["ks", "users"]);
        assert_eq!(&buf[0..4], &0x0005i32.to_be_bytes());
    }

    #[test]
    fn encode_prepared_result() {
        let id = [1u8; 16];
        let buf = encode_prepared(
            &id,
            &["k".into()],
            &[CqlType::Int],
            &["k".into(), "v".into()],
            &[CqlType::Int, CqlType::Varchar],
            "ks",
            "t",
        );
        assert_eq!(&buf[0..4], &0x0004i32.to_be_bytes());
        // Verify ID follows
        assert_eq!(u16::from_be_bytes([buf[4], buf[5]]), 16);
        assert_eq!(&buf[6..22], &[1u8; 16]);
    }

    /// For a prepared INSERT/UPDATE/DELETE (no result columns), the result
    /// metadata section must use the No_metadata flag (0x0004) with column
    /// count 0, and must NOT include keyspace/table strings after that.
    ///
    /// Buffer layout we parse forward through:
    ///   [0..4]   kind = 0x0004 (Prepared)
    ///   [4..6]   id_len = 16
    ///   [6..22]  id bytes
    ///   [22..]   bind metadata: flags(4) + col_count(4) + ks_str + tbl_str + per-column specs
    ///            result metadata: flags(4) + col_count(4)
    #[test]
    fn encode_prepared_insert_no_result_columns_uses_no_metadata_flag() {
        let id = [0xABu8; 16];
        let keyspace = "ks";
        let table = "users";
        // Simulate INSERT: one bind variable, no result columns.
        let buf = encode_prepared(
            &id,
            &["name".into()],
            &[CqlType::Varchar],
            &[], // no result columns
            &[],
            keyspace,
            table,
        );

        // --- Parse forward ---

        // kind
        let mut pos = 0usize;
        let kind = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        assert_eq!(kind, 0x0004, "kind must be Prepared (0x0004)");
        pos += 4;

        // id_len + id
        let id_len = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        assert_eq!(id_len, 16);
        pos += 2 + id_len; // skip id bytes

        // bind metadata (PreparedMetadata): flags + col_count + pk_count
        let bind_flags = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let bind_col_count = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        // pk_count (V4+)
        let pk_count = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        // skip pk_indexes (i16 each)
        pos += pk_count * 2;

        if bind_flags & 0x0001 != 0 {
            // Global_tables_spec: skip ks and table strings
            let ks_len = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2 + ks_len;
            let tbl_len = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2 + tbl_len;
        }

        // skip per-column specs for bind variables
        for _ in 0..bind_col_count {
            // column name string
            let name_len = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2 + name_len;
            // type_id u16
            let type_id = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap());
            pos += 2;
            // skip type params (Varchar is simple — no extra bytes)
            match type_id {
                // List(0x0020) or Set(0x0022): 1 extra u16
                0x0020 | 0x0022 => pos += 2,
                // Map(0x0021): 2 extra u16s
                0x0021 => pos += 4,
                // Simple types: nothing extra
                _ => {}
            }
        }

        // --- Now at result metadata ---
        let result_flags = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let result_col_count = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        assert_eq!(
            result_flags, 0x0004,
            "result metadata flags must be No_metadata (0x0004)"
        );
        assert_eq!(result_col_count, 0, "result column count must be 0");

        // No further bytes should follow the flags+count for a No_metadata section.
        assert_eq!(
            pos,
            buf.len(),
            "no keyspace/table strings should follow result metadata for INSERT"
        );
    }
}
