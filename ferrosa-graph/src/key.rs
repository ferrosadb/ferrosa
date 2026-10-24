//! Storage key derivation: one function, and no names.
//!
//! A graph row's storage key is a function of exactly three things — the
//! entity's schema LABEL, its declared KEY COLUMNS, and the VALUES of those
//! columns — and of nothing else. In particular it is **not** a function of the
//! Cypher variable that names the pattern element. A variable is query scope
//! (`n`, `person`, or none at all for an anonymous node), and [`derive_key`]'s
//! parameter list has no place to put one. That separation is the point of this
//! module: the write path, the read path and the planner all derive through the
//! same call, so renaming a variable — or synthesising a scope id for an
//! anonymous node — cannot change a persisted byte.
//!
//! Two properties follow, and both are load-bearing:
//!
//! - **A key column with no supplied value is generated from the entity's
//!   content digest, at the column's declared width.** A `uuid` key column keeps
//!   16 bytes. Before this module the write path hashed such an entity to 32
//!   bytes while its relationships declared 16-byte endpoint columns, and the
//!   read path hashed to 32 independently, so a written edge could be
//!   untraversable (issue #610). One function, one width decision, no drift.
//! - **Component framing lives here too**, so the partition and clustering
//!   encodings have a single implementation rather than one per call site.

use uuid::Uuid;

use crate::error::{GraphError, Result};
use crate::parser::{Expr, Literal};

/// One declared component of a storage key.
///
/// `name` is the schema's column name — part of the table's declaration, and
/// therefore part of the key. It is never a Cypher variable.
#[derive(Debug, Clone, Copy)]
pub struct KeyColumn<'a> {
    /// Declared column name, for diagnostics.
    pub name: &'a str,
    /// Declared column type, which fixes the component's width.
    pub column_type: &'a str,
}

/// What one key column contributes to the key's bytes.
#[derive(Debug, Clone, Copy)]
pub enum KeyValue<'a> {
    /// A value the statement (or a bound row) supplied, encoded for the
    /// column's declared type.
    Value(&'a Expr),
    /// The entity's content identity, for a key column the statement did not
    /// supply: the column's declared type decides how many of those bytes
    /// become the key. Deterministic, so a repeated `MERGE` of the same content
    /// is one row.
    Content(&'a [u8; 32]),
    /// A key the engine minted rather than derived (`CREATE`'s synthesized id).
    /// Random by design: `CREATE` makes a new entity, `MERGE` matches one.
    Generated(Uuid),
    /// An already-derived component — an edge endpoint's key bytes — passed
    /// through byte-faithfully, and rejected if it is the wrong width for the
    /// declared column.
    Derived(&'a [u8]),
}

/// A row's storage key: the partition bytes and the clustering bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Key {
    pub partition: Vec<u8>,
    pub clustering: Vec<u8>,
}

impl Key {
    /// The decorated partition key storage addresses this row by.
    pub fn decorated_partition(&self) -> ferrosa_common::DecoratedKey {
        ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(self.partition.clone()))
    }
}

/// Derive the storage key of one entity.
///
/// This is the only place a key's bytes are decided. `label` selects the entity
/// for diagnostics (an error names what the user wrote); `partition` /
/// `clustering` are the table's declared key columns; the two value lists are
/// positionally aligned with them.
///
/// A side with no declared columns is an undeclared (schemaless) table: its key
/// is the entity's content digest as-is, which is the only key shape such a row
/// can have.
pub fn derive_key(
    label: &str,
    partition: &[KeyColumn<'_>],
    partition_values: &[KeyValue<'_>],
    clustering: &[KeyColumn<'_>],
    clustering_values: &[KeyValue<'_>],
) -> Result<Key> {
    Ok(Key {
        partition: encode_partition_components(&side(
            label,
            "partition",
            partition,
            partition_values,
        )?),
        clustering: encode_clustering_components(&side(
            label,
            "clustering",
            clustering,
            clustering_values,
        )?),
    })
}

/// The content identity of a `MERGE`: a deterministic digest of its
/// match-property bytes.
///
/// Properties are sorted by name, so two callers that name the same properties
/// in a different order arrive at the same bytes — the R1 mitigation for
/// concurrent MERGEs of one entity (at-most-once by construction: the same
/// content is the same partition key, and a LWW re-write of one key is
/// idempotent). Property names are part of the digest; Cypher variables are not
/// (they never reach this function).
pub fn identity_digest(match_props: &[(String, Expr)]) -> Result<[u8; 32]> {
    let mut sorted: Vec<&(String, Expr)> = match_props.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = blake3::Hasher::new();
    for (name, expr) in &sorted {
        hasher.update(name.as_bytes());
        hasher.update(b"\x00");
        let val_bytes = expr_to_bytes(expr)?;
        hasher.update(&val_bytes);
        hasher.update(b"\x00");
    }
    Ok(*hasher.finalize().as_bytes())
}

/// One side's component bytes, in declared column order.
fn side(
    label: &str,
    side: &str,
    columns: &[KeyColumn<'_>],
    values: &[KeyValue<'_>],
) -> Result<Vec<Vec<u8>>> {
    if columns.is_empty() {
        return match values {
            [] => Ok(Vec::new()),
            [KeyValue::Content(digest)] => Ok(vec![digest.to_vec()]),
            _ => Err(GraphError::Validation(format!(
                "{label}: its {side} key declares no columns, so the only key it can have is the \
                 content identity itself ({} value(s) given)",
                values.len()
            ))),
        };
    }
    if columns.len() != values.len() {
        return Err(GraphError::Validation(format!(
            "{label}: {side} key has {} declared column(s) but {} value(s)",
            columns.len(),
            values.len()
        )));
    }
    columns
        .iter()
        .zip(values)
        .map(|(column, value)| component(label, side, column, value))
        .collect()
}

/// One key column's component bytes.
fn component(
    label: &str,
    side: &str,
    column: &KeyColumn<'_>,
    value: &KeyValue<'_>,
) -> Result<Vec<u8>> {
    match value {
        KeyValue::Value(expr) => encode_expr_for_column_type(expr, column.column_type)
            .map_err(|e| column_error(label, side, column, e)),
        KeyValue::Content(digest) => generated_component(label, side, column, uuid_from(digest)),
        KeyValue::Generated(uuid) => generated_component(label, side, column, *uuid),
        KeyValue::Derived(bytes) => match fixed_width(column.column_type) {
            Some(width) if bytes.len() != width => Err(GraphError::Validation(format!(
                "{label}: {side} key column '{}' is declared {} ({width} bytes) but the \
                     component it was given is {} bytes — the derived endpoint key and this \
                     column disagree on width",
                column.name,
                column.column_type,
                bytes.len()
            ))),
            _ => Ok(bytes.to_vec()),
        },
    }
}

fn column_error(label: &str, side: &str, column: &KeyColumn<'_>, e: GraphError) -> GraphError {
    GraphError::Validation(format!(
        "{label}: {side} key column '{}' ({}) could not be encoded: {e}",
        column.name, column.column_type
    ))
}

/// A generated key's bytes, at the width its declared type stores.
///
/// `uuid` keeps the 16 raw bytes; `text` keeps the 36-byte hyphenated form of
/// the same 16 bytes (what `CREATE` has always synthesized for a text key). Any
/// other type has no canonical generated width, and guessing one is exactly the
/// silent-width defect this module exists to remove.
fn generated_component(
    label: &str,
    side: &str,
    column: &KeyColumn<'_>,
    uuid: Uuid,
) -> Result<Vec<u8>> {
    if is_uuid_type(column.column_type) {
        Ok(uuid.as_bytes().to_vec())
    } else if is_text_type(column.column_type) {
        Ok(uuid.to_string().into_bytes())
    } else {
        Err(GraphError::Validation(format!(
            "{label}: {side} key column '{}' has type '{}', which has no canonical width for a \
             generated key; supply the value explicitly or declare the column as uuid/text",
            column.name, column.column_type
        )))
    }
}

/// The 16 bytes a generated key keeps: the leading half of the content digest.
fn uuid_from(digest: &[u8; 32]) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

/// The byte width a declared key type stores, when it has one.
///
/// Answers one question: how many bytes is this column? It is what makes a
/// component's width checked instead of assumed — a `uuid` key column is 16
/// bytes, and a 32-byte component is the bug that made written edges
/// untraversable.
fn fixed_width(column_type: &str) -> Option<usize> {
    let ty = column_type.to_ascii_lowercase();
    match ty.as_str() {
        "uuid" | "timeuuid" => Some(16),
        "int" => Some(4),
        "bigint" | "long" | "timestamp" => Some(8),
        "double" => Some(8),
        "float" => Some(4),
        "boolean" => Some(1),
        other => {
            if other.ends_with("uuidtype") {
                Some(16)
            } else if other.ends_with("int32type") {
                Some(4)
            } else if other.ends_with("longtype")
                || other.ends_with("timestamptype")
                || other.ends_with("doubletype")
            {
                Some(8)
            } else if other.ends_with("floattype") {
                Some(4)
            } else if other.ends_with("booleantype") {
                Some(1)
            } else {
                None
            }
        }
    }
}

fn is_uuid_type(column_type: &str) -> bool {
    let ty = column_type.to_ascii_lowercase();
    ty == "uuid" || ty == "timeuuid" || ty.ends_with("uuidtype")
}

fn is_text_type(column_type: &str) -> bool {
    let ty = column_type.to_ascii_lowercase();
    matches!(ty.as_str(), "text" | "varchar" | "ascii")
        || ty.ends_with("utf8type")
        || ty.ends_with("asciitype")
}

/// A multi-component partition key: `len(2) || bytes || NUL` per component.
///
/// Public because the key format is a contract: the read side decodes with the
/// exact inverse, and a test that asserts a row's stored bytes is asserting
/// this layout.
pub fn encode_partition_components(components: &[Vec<u8>]) -> Vec<u8> {
    if components.len() == 1 {
        return components[0].clone();
    }
    let mut buf = Vec::new();
    for component in components {
        buf.extend_from_slice(&(component.len() as u16).to_be_bytes());
        buf.extend_from_slice(component);
        buf.push(0x00);
    }
    buf
}

/// A multi-component clustering key: `len(2) || bytes` per component.
pub fn encode_clustering_components(components: &[Vec<u8>]) -> Vec<u8> {
    if components.len() == 1 {
        return components[0].clone();
    }
    let mut buf = Vec::new();
    for component in components {
        buf.extend_from_slice(&(component.len() as u16).to_be_bytes());
        buf.extend_from_slice(component);
    }
    buf
}

/// Convert a `Literal` from the AST into raw bytes for storage.
pub(crate) fn literal_to_bytes(lit: &Literal) -> Vec<u8> {
    match lit {
        Literal::String(s) => s.as_bytes().to_vec(),
        Literal::Integer(i) => i.to_be_bytes().to_vec(),
        Literal::Float(f) => f.to_be_bytes().to_vec(),
        Literal::Bool(b) => vec![if *b { 1 } else { 0 }],
        Literal::Null => vec![],
    }
}

/// Convert an `Expr` to bytes, supporting only `Expr::Literal` for now.
pub(crate) fn expr_to_bytes(expr: &Expr) -> Result<Vec<u8>> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_bytes(lit)),
        other => Err(GraphError::Validation(format!(
            "cannot convert expression to bytes: {other:?}"
        ))),
    }
}

/// Encode one value for a declared column type.
///
/// Every key component and every stored cell goes through this, so a value's
/// byte width follows its declared type: a `uuid` literal is 16 raw bytes, not
/// the 36 bytes of its textual form.
pub(crate) fn encode_expr_for_column_type(expr: &Expr, column_type: &str) -> Result<Vec<u8>> {
    let lower = column_type.to_ascii_lowercase();
    match expr {
        Expr::Literal(Literal::String(s)) if lower == "uuid" || lower.ends_with("uuidtype") => {
            let uuid = Uuid::parse_str(s).map_err(|e| {
                GraphError::Validation(format!("invalid UUID literal '{s}' for {column_type}: {e}"))
            })?;
            Ok(uuid.as_bytes().to_vec())
        }
        Expr::Literal(Literal::String(s))
            if lower == "text"
                || lower == "varchar"
                || lower.ends_with("utf8type")
                || lower.ends_with("asciitype") =>
        {
            Ok(s.as_bytes().to_vec())
        }
        Expr::Literal(Literal::String(s))
            if lower == "timestamp" || lower.ends_with("timestamptype") =>
        {
            let ms = chrono::DateTime::parse_from_rfc3339(s)
                .map_err(|e| {
                    GraphError::Validation(format!("invalid timestamp literal '{s}': {e}"))
                })?
                .timestamp_millis();
            Ok(ms.to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Integer(i))
            if lower == "timestamp" || lower.ends_with("timestamptype") =>
        {
            Ok(i.to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Float(f)) if lower == "double" || lower.ends_with("doubletype") => {
            Ok(f.to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Integer(i))
            if lower == "double" || lower.ends_with("doubletype") =>
        {
            Ok((*i as f64).to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Float(f)) if lower == "float" || lower.ends_with("floattype") => {
            Ok((*f as f32).to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Integer(i)) if lower == "float" || lower.ends_with("floattype") => {
            Ok((*i as f32).to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Integer(i)) if lower == "int" || lower.ends_with("int32type") => {
            let value = i32::try_from(*i).map_err(|_| {
                GraphError::Validation(format!("integer literal {i} does not fit in int"))
            })?;
            Ok(value.to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Integer(i))
            if lower == "bigint" || lower == "long" || lower.ends_with("longtype") =>
        {
            Ok(i.to_be_bytes().to_vec())
        }
        Expr::Literal(Literal::Bool(b)) if lower == "boolean" || lower.ends_with("booleantype") => {
            Ok(vec![u8::from(*b)])
        }
        _ => expr_to_bytes(expr),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn str_expr(value: &str) -> Expr {
        Expr::Literal(Literal::String(value.to_string()))
    }

    fn digest_of(props: &[(String, Expr)]) -> [u8; 32] {
        identity_digest(props).expect("digest")
    }

    /// I5: the key width follows the declared type. This is where the 32-vs-16
    /// defect lived, so it is asserted on the bytes, not on a write succeeding.
    #[test]
    fn a_generated_key_keeps_the_declared_types_width() {
        let digest = [7u8; 32];
        let columns = [KeyColumn {
            name: "id",
            column_type: "uuid",
        }];
        let key = derive_key("Person", &columns, &[KeyValue::Content(&digest)], &[], &[])
            .expect("uuid key");
        assert_eq!(key.partition.len(), 16, "a uuid key column stores 16 bytes");
        assert_eq!(key.partition, digest[..16].to_vec());

        let text_columns = [KeyColumn {
            name: "id",
            column_type: "text",
        }];
        let text_key = derive_key(
            "Person",
            &text_columns,
            &[KeyValue::Content(&digest)],
            &[],
            &[],
        )
        .expect("text key");
        assert_eq!(
            text_key.partition.len(),
            36,
            "a text key is the hyphenated form"
        );
        assert!(std::str::from_utf8(&text_key.partition)
            .unwrap()
            .contains('-'));
    }

    /// A component that does not fit the declared width is refused here, where
    /// the column can be named — not deep inside storage.
    #[test]
    fn a_component_wider_than_the_declared_column_is_refused() {
        let columns = [KeyColumn {
            name: "dst_id",
            column_type: "uuid",
        }];
        let wide = [0u8; 32];
        let err = derive_key("KNOWS", &columns, &[KeyValue::Derived(&wide)], &[], &[])
            .expect_err("32 bytes cannot be a uuid column");
        assert!(
            err.to_string().contains("dst_id") && err.to_string().contains("16 bytes"),
            "the error names the column and its width: {err}"
        );

        let narrow = [0u8; 16];
        let key = derive_key("KNOWS", &columns, &[KeyValue::Derived(&narrow)], &[], &[])
            .expect("16 bytes fits a uuid column");
        assert_eq!(key.partition, narrow.to_vec());
    }

    /// The digest is over property NAMES and values, so two entities that differ
    /// only in their content are different keys — and the same content is the
    /// same key, whatever order the properties were written in.
    #[test]
    fn the_identity_digest_is_order_independent_and_content_sensitive() {
        let one = digest_of(&[
            ("name".into(), str_expr("Alice")),
            ("age".into(), Expr::Literal(Literal::Integer(30))),
        ]);
        let reordered = digest_of(&[
            ("age".into(), Expr::Literal(Literal::Integer(30))),
            ("name".into(), str_expr("Alice")),
        ]);
        assert_eq!(one, reordered);
        let other = digest_of(&[
            ("name".into(), str_expr("Bob")),
            ("age".into(), Expr::Literal(Literal::Integer(30))),
        ]);
        assert_ne!(one, other);
    }

    /// A `Generated` key (CREATE's synthesized id) is encoded at the declared
    /// type's width, exactly like a content-derived one — the width rule is the
    /// function's, not the caller's.
    #[test]
    fn a_generated_uuid_encodes_at_the_declared_width() {
        let uuid = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
        let key = derive_key(
            "Person",
            &[KeyColumn {
                name: "id",
                column_type: "uuid",
            }],
            &[KeyValue::Generated(uuid)],
            &[],
            &[],
        )
        .expect("generated uuid key");
        assert_eq!(key.partition, uuid.as_bytes().to_vec());
    }

    #[test]
    fn literal_to_bytes_string() {
        assert_eq!(literal_to_bytes(&Literal::String("hello".into())), b"hello");
    }

    #[test]
    fn literal_to_bytes_integer() {
        assert_eq!(
            literal_to_bytes(&Literal::Integer(42)),
            42_i64.to_be_bytes().to_vec()
        );
    }

    #[test]
    fn literal_to_bytes_null() {
        assert!(literal_to_bytes(&Literal::Null).is_empty());
    }

    #[test]
    fn a_multi_component_key_is_framed_per_side() {
        let partition = [
            KeyColumn {
                name: "tenant",
                column_type: "text",
            },
            KeyColumn {
                name: "id",
                column_type: "uuid",
            },
        ];
        let clustering = [KeyColumn {
            name: "dst_id",
            column_type: "text",
        }];
        let tenant = str_expr("acme");
        let uuid = Uuid::from_u128(1);
        let dst = str_expr("bob");
        let key = derive_key(
            "Edge",
            &partition,
            &[KeyValue::Value(&tenant), KeyValue::Generated(uuid)],
            &clustering,
            &[KeyValue::Value(&dst)],
        )
        .expect("composite key");
        // partition: len||bytes||NUL for each of the two components
        assert_eq!(&key.partition[..2], &4u16.to_be_bytes());
        assert_eq!(&key.partition[2..6], b"acme");
        assert_eq!(key.partition[6], 0x00);
        assert_eq!(&key.partition[7..9], &16u16.to_be_bytes());
        // clustering with one component is that component, unchanged
        assert_eq!(key.clustering, b"bob".to_vec());
    }
}
