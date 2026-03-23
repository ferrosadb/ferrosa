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
/// `target` is one of `"KEYSPACE"`, `"TABLE"`, `"TYPE"`, `"FUNCTION"`, `"AGGREGATE"`.
/// `options` contains the keyspace name and, for non-keyspace targets, the object name.
/// `arg_types` is the list of argument type names, required for FUNCTION/AGGREGATE targets
/// per CQL native protocol v5 section 4.2.5.5. Pass an empty slice for other targets.
pub fn encode_schema_change(change_type: &str, target: &str, options: &[&str]) -> BytesMut {
    encode_schema_change_with_args(change_type, target, options, &[])
}

/// Encode a schema change result with function/aggregate argument types.
///
/// The CQL native protocol requires FUNCTION and AGGREGATE schema change
/// events to include a `[string list]` of argument types after the
/// keyspace and object name.
pub fn encode_schema_change_with_args(
    change_type: &str,
    target: &str,
    options: &[&str],
    arg_types: &[String],
) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_i32(0x0005); // SchemaChange kind
    encode_string(&mut buf, change_type);
    encode_string(&mut buf, target);
    for opt in options {
        encode_string(&mut buf, opt);
    }
    if target == "FUNCTION" || target == "AGGREGATE" {
        // CQL native protocol requires a [string list] of argument types
        buf.put_u16(arg_types.len() as u16);
        for arg in arg_types {
            encode_string(&mut buf, arg);
        }
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
    encode_rows_paged(column_names, column_types, keyspace, table, rows, None)
}

/// Encode a Rows RESULT body with optional paging state.
///
/// When `paging_state` is `Some`, the `Has_more_pages` flag (0x0002) is set
/// in the result metadata and the opaque paging state bytes are included.
/// This signals to the client that more rows are available and they should
/// send the paging_state back in the next QUERY/EXECUTE to continue.
pub fn encode_rows_paged(
    column_names: &[String],
    column_types: &[CqlType],
    keyspace: &str,
    table: &str,
    rows: &[Vec<Option<CqlValue>>],
    paging_state: Option<&[u8]>,
) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_i32(0x0002); // Rows kind

    encode_rows_metadata_paged(
        &mut buf,
        column_names,
        column_types,
        keyspace,
        table,
        paging_state,
    );

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
///
/// `pk_indexes` contains the 0-based positions of partition key columns within the
/// bind variable list. Pass an empty slice when any PK column has no bind marker
/// (equivalent to `pk_count=0`, which disables token-aware routing in drivers).
#[allow(clippy::too_many_arguments)]
pub fn encode_prepared(
    id: &[u8; 16],
    bound_names: &[String],
    bound_types: &[CqlType],
    result_column_names: &[String],
    result_column_types: &[CqlType],
    keyspace: &str,
    table: &str,
    pk_indexes: &[u16],
) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_i32(0x0004); // Prepared kind

    // Prepared statement ID: [u16 length][bytes]
    buf.put_u16(16u16);
    buf.put_slice(id);

    // Bind-variable metadata (includes pk_count + pk_indexes per CQL protocol v4+)
    encode_prepared_bind_metadata(
        &mut buf,
        bound_names,
        bound_types,
        keyspace,
        table,
        pk_indexes,
    );

    // Result-column metadata
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

// ── Private helpers ────────────────────────────────────────────────────────

/// Write a CQL short string: `[u16 length][bytes]`.
fn encode_string(buf: &mut BytesMut, s: &str) {
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
}

/// Write bind-variable metadata for Prepared results.
///
/// Per CQL native protocol v4+ (section 4.2.5.4), the bind-variable
/// metadata includes `pk_count` and `pk_indexes` between `columns_count`
/// and the global table spec. `pk_indexes` maps each partition key column
/// to its 0-based position in the bind variable list — this enables
/// token-aware routing in CQL drivers. An empty `pk_indexes` writes
/// `pk_count=0`, which disables token-aware routing.
fn encode_prepared_bind_metadata(
    buf: &mut BytesMut,
    column_names: &[String],
    column_types: &[CqlType],
    keyspace: &str,
    table: &str,
    pk_indexes: &[u16],
) {
    buf.put_i32(0x0001); // flags: Global_tables_spec
    buf.put_i32(column_names.len() as i32);
    buf.put_i32(pk_indexes.len() as i32);
    for &idx in pk_indexes {
        buf.put_i16(idx as i16);
    }
    encode_string(buf, keyspace);
    encode_string(buf, table);
    for (name, cql_type) in column_names.iter().zip(column_types.iter()) {
        encode_string(buf, name);
        encode_type(buf, cql_type);
    }
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
    encode_rows_metadata_paged(buf, column_names, column_types, keyspace, table, None);
}

/// Write column metadata with optional paging state.
///
/// When `paging_state` is `Some`, sets the `Has_more_pages` flag (0x0002)
/// and writes the paging state bytes after the flags and column count.
///
/// CQL v5 metadata flags:
/// - 0x0001: Global_tables_spec
/// - 0x0002: Has_more_pages
/// - 0x0004: No_metadata
fn encode_rows_metadata_paged(
    buf: &mut BytesMut,
    column_names: &[String],
    column_types: &[CqlType],
    keyspace: &str,
    table: &str,
    paging_state: Option<&[u8]>,
) {
    let mut flags: i32 = 0x0001; // Global_tables_spec
    if paging_state.is_some() {
        flags |= 0x0002; // Has_more_pages
    }
    buf.put_i32(flags);
    buf.put_i32(column_names.len() as i32);

    // If Has_more_pages is set, write the paging state bytes.
    if let Some(state) = paging_state {
        buf.put_i32(state.len() as i32);
        buf.put_slice(state);
    }

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
/// Tuple:        `[u16 type_id][u16 count][type]*count`
/// UDT:          `[u16 type_id][string ks][string name][u16 n][string field_name + type]*n`
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
        CqlType::Vector(elem, dim) => {
            // Cassandra 5.0 encodes vectors as Custom type (0x0000) with
            // the class name string. The type_id is already 0x0000.
            let elem_class = match elem.as_ref() {
                CqlType::Float => "org.apache.cassandra.db.marshal.FloatType",
                CqlType::Double => "org.apache.cassandra.db.marshal.DoubleType",
                _ => "org.apache.cassandra.db.marshal.FloatType",
            };
            let class_name =
                format!("org.apache.cassandra.db.marshal.VectorType({elem_class}, {dim})");
            encode_string(buf, &class_name);
        }
        CqlType::Udt {
            keyspace,
            name,
            fields,
        } => {
            encode_string(buf, keyspace);
            encode_string(buf, name);
            buf.put_u16(fields.len() as u16);
            for (field_name, field_type) in fields {
                encode_string(buf, field_name);
                encode_type(buf, field_type);
            }
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
    fn encode_schema_change_function_includes_arg_types() {
        let arg_types = vec!["int".to_string(), "text".to_string()];
        let buf =
            encode_schema_change_with_args("CREATED", "FUNCTION", &["ks", "my_func"], &arg_types);
        assert_eq!(&buf[0..4], &0x0005i32.to_be_bytes());

        // Parse forward: kind(4) + change_type string + target string + ks string + name string
        let mut pos = 4;

        // change_type "CREATED"
        let len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2 + len;

        // target "FUNCTION"
        let len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2 + len;

        // keyspace "ks"
        let len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2 + len;

        // function name "my_func"
        let len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2 + len;

        // arg_types string list: [u16 count][string]*
        let count = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
        pos += 2;
        assert_eq!(count, 2, "arg_types list should have 2 entries");

        // first arg type "int"
        let len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        assert_eq!(&buf[pos..pos + len], b"int");
        pos += len;

        // second arg type "text"
        let len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        assert_eq!(&buf[pos..pos + len], b"text");
        pos += len;

        assert_eq!(pos, buf.len(), "no trailing bytes");
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
            &[0], // PK column "k" is at bind variable index 0
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
    /// This test also verifies that pk_indexes are written correctly when
    /// partition key columns have bind markers.
    ///
    /// Buffer layout we parse forward through:
    ///   [0..4]   kind = 0x0004 (Prepared)
    ///   [4..6]   id_len = 16
    ///   [6..22]  id bytes
    ///   [22..]   bind metadata: flags(4) + col_count(4) + pk_count(4) + pk_indexes + ks_str + tbl_str + per-column specs
    ///            result metadata: flags(4) + col_count(4)
    #[test]
    fn encode_prepared_insert_no_result_columns_uses_no_metadata_flag() {
        let id = [0xABu8; 16];
        let keyspace = "ks";
        let table = "users";
        // Simulate INSERT with two bind variables: "id" (PK at index 0) and "name".
        let buf = encode_prepared(
            &id,
            &["id".into(), "name".into()],
            &[CqlType::Int, CqlType::Varchar],
            &[], // no result columns
            &[],
            keyspace,
            table,
            &[0], // PK column "id" is at bind variable index 0
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

        // bind metadata: flags
        let bind_flags = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;
        // bind metadata: col_count
        let bind_col_count = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        assert_eq!(bind_col_count, 2, "should have 2 bind variables");
        pos += 4;

        // bind metadata: pk_count + pk_indexes (CQL protocol v4+)
        let pk_count = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        assert_eq!(pk_count, 1, "pk_count should be 1 (one PK column)");
        pos += 4;

        // Read pk_indexes
        for i in 0..pk_count {
            let pk_idx = i16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap());
            if i == 0 {
                assert_eq!(pk_idx, 0, "PK column 'id' should be at bind index 0");
            }
            pos += 2;
        }

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
            // skip type params (Int and Varchar are simple — no extra bytes)
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

    /// Verify that pk_count=0 is written when no pk_indexes are provided
    /// (e.g. when not all PK columns have bind markers).
    #[test]
    fn encode_prepared_empty_pk_indexes() {
        let id = [0xCDu8; 16];
        let buf = encode_prepared(
            &id,
            &["name".into()],
            &[CqlType::Varchar],
            &[],
            &[],
            "ks",
            "t",
            &[], // no PK indexes — disables token-aware routing
        );

        // Skip to bind metadata: kind(4) + id_len(2) + id(16) = offset 22
        let pos = 22;
        // flags(4)
        let pos = pos + 4;
        // col_count(4)
        let pos = pos + 4;
        // pk_count
        let pk_count = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        assert_eq!(pk_count, 0, "pk_count should be 0 for empty pk_indexes");
    }

    /// Verify composite partition key writes multiple pk_indexes.
    #[test]
    fn encode_prepared_composite_pk_indexes() {
        let id = [0xEFu8; 16];
        // INSERT INTO t (a, b, c, d) VALUES (?, ?, ?, ?)
        // Composite PK: (a, c) — bind indexes 0 and 2
        let buf = encode_prepared(
            &id,
            &["a".into(), "b".into(), "c".into(), "d".into()],
            &[CqlType::Int, CqlType::Int, CqlType::Int, CqlType::Varchar],
            &[],
            &[],
            "ks",
            "t",
            &[0, 2], // PK columns at bind indexes 0 and 2
        );

        // Skip to bind metadata: kind(4) + id_len(2) + id(16) = offset 22
        let mut pos = 22;
        // flags(4)
        pos += 4;
        // col_count(4)
        let col_count = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        assert_eq!(col_count, 4);
        pos += 4;
        // pk_count
        let pk_count = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        assert_eq!(pk_count, 2, "composite PK with 2 columns");
        pos += 4;
        // pk_indexes[0]
        let idx0 = i16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap());
        assert_eq!(idx0, 0, "first PK column at bind index 0");
        pos += 2;
        // pk_indexes[1]
        let idx1 = i16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap());
        assert_eq!(idx1, 2, "second PK column at bind index 2");
    }

    /// Exhaustive byte-level test: INSERT INTO t (id uuid, flag boolean) VALUES (?, ?)
    /// Verifies every byte matches CQL protocol v4 PreparedMetadata spec.
    #[test]
    fn encode_prepared_uuid_boolean_full_parse() {
        let id = [0x42u8; 16];
        let buf = encode_prepared(
            &id,
            &["id".into(), "flag".into()],
            &[CqlType::Uuid, CqlType::Boolean],
            &[], // INSERT — no result columns
            &[],
            "test_ks",
            "test_bool",
            &[0], // PK "id" at bind index 0
        );

        let mut pos = 0usize;

        // kind = 0x0004 (Prepared)
        assert_eq!(
            i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()),
            0x0004
        );
        pos += 4;

        // prepared id: [u16 len=16][16 bytes]
        assert_eq!(
            u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()),
            16
        );
        pos += 2 + 16;

        // --- Bind metadata ---
        // flags
        let flags = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        assert_eq!(flags & 0x0001, 0x0001, "Global_tables_spec should be set");
        pos += 4;

        // columns_count
        let col_count = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        assert_eq!(col_count, 2, "2 bind variables");
        pos += 4;

        // pk_count
        let pk_count = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        assert_eq!(pk_count, 1, "1 PK column");
        pos += 4;

        // pk_indexes[0]
        let pk_idx = i16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap());
        assert_eq!(pk_idx, 0, "PK at bind index 0");
        pos += 2;

        // Global table spec: keyspace "test_ks"
        let ks_len = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        assert_eq!(&buf[pos + 2..pos + 2 + ks_len], b"test_ks");
        pos += 2 + ks_len;

        // Global table spec: table "test_bool"
        let tbl_len = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        assert_eq!(&buf[pos + 2..pos + 2 + tbl_len], b"test_bool");
        pos += 2 + tbl_len;

        // Column 0: "id" uuid (0x000C)
        let name_len = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        assert_eq!(&buf[pos + 2..pos + 2 + name_len], b"id");
        pos += 2 + name_len;
        let type_id = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap());
        assert_eq!(type_id, 0x000C, "uuid type_id");
        pos += 2;

        // Column 1: "flag" boolean (0x0004)
        let name_len = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        assert_eq!(&buf[pos + 2..pos + 2 + name_len], b"flag");
        pos += 2 + name_len;
        let type_id = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap());
        assert_eq!(type_id, 0x0004, "boolean type_id");
        pos += 2;

        // --- Result metadata ---
        // flags = No_metadata (0x0004)
        let result_flags = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        assert_eq!(result_flags, 0x0004, "No_metadata flag for INSERT");
        pos += 4;

        // columns_count = 0
        let result_col_count = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        assert_eq!(result_col_count, 0);
        pos += 4;

        // Must be at the exact end of the buffer
        assert_eq!(pos, buf.len(), "no trailing bytes — exact CQL frame");
    }
}
