//! Module: jsonb placement rules for table schemas (T-154a, D3, D21).
//!
//! Responsibility: refuse `jsonb` wherever the engine cannot give it a stable
//! key-byte order: in any partition or clustering key column (at any depth,
//! through frozen collections, tuples and UDTs), as a set element, as a map
//! key, and as a vector element. One implementation is shared by every DDL
//! entry point (CQL CREATE/ALTER TABLE, the PG CREATE TABLE plan through
//! `DdlPath`) and by the schema apply paths (`create_table_internal`,
//! `alter_table_internal`, `apply_snapshot`), so a replicated or reloaded bad
//! schema is refused exactly as a fresh statement is.
//!
//! Correctness: a refusal is a typed [`SchemaError`] naming the table, the
//! column and the rule. A column type string that cannot be parsed is also an
//! error, never a skipped check (FMEA SCH-T154a-04). Column types are stored
//! as strings, so this module carries its own tiny type-string reader.
//! Last revised: 2026-09-28
//! Last changed: New module (T-154a).

use std::collections::HashMap;

use ferrosa_common::cql_type::names::{check_jsonb_nesting, JSONB_MARSHAL_CLASS};
use ferrosa_common::CqlType;

use crate::error::SchemaError;
use crate::metadata::column::{ColumnKind, ColumnMetadata};
use crate::metadata::table::TableMetadata;
use crate::metadata::user_type::UserTypeMetadata;

/// UDTs visible to a check, keyed `(keyspace, type name)`.
pub type TypeMap = HashMap<(String, String), UserTypeMetadata>;

/// Deepest type nesting the reader accepts. A guard, not a CQL limit.
const MAX_DEPTH: usize = 32;

/// A parsed column type string.
#[derive(Debug, PartialEq)]
enum Node {
    /// A bare name: `jsonb`, `int`, a UDT name, or a quoted custom class.
    Name(String),
    /// `head<args>`: `list`, `set`, `map`, `tuple`, `vector`, `frozen`.
    Generic(String, Vec<Node>),
    /// A vector dimension.
    Num,
}

struct Reader<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn skip_ws(&mut self) {
        while self.src.get(self.pos).is_some_and(u8::is_ascii_whitespace) {
            self.pos += 1;
        }
    }

    fn eat(&mut self, ch: u8) -> bool {
        self.skip_ws();
        if self.src.get(self.pos) == Some(&ch) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn word(&mut self) -> Result<String, String> {
        self.skip_ws();
        let start = self.pos;
        while self
            .src
            .get(self.pos)
            .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'.')
        {
            self.pos += 1;
        }
        if start == self.pos {
            return Err(format!("expected a type name at offset {start}"));
        }
        String::from_utf8(self.src[start..self.pos].to_vec()).map_err(|e| e.to_string())
    }

    fn quoted(&mut self) -> Result<String, String> {
        let start = self.pos + 1;
        let Some(len) = self.src[start..].iter().position(|b| *b == b'\'') else {
            return Err("unterminated quoted type name".to_string());
        };
        self.pos = start + len + 1;
        String::from_utf8(self.src[start..start + len].to_vec()).map_err(|e| e.to_string())
    }

    fn node(&mut self, depth: usize) -> Result<Node, String> {
        if depth > MAX_DEPTH {
            return Err(format!("type nesting deeper than {MAX_DEPTH}"));
        }
        self.skip_ws();
        if self.src.get(self.pos) == Some(&b'\'') {
            return self.quoted().map(Node::Name);
        }
        let head = self.word()?;
        if head.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(Node::Num);
        }
        if !self.eat(b'<') {
            return Ok(Node::Name(head));
        }
        let mut args = vec![self.node(depth + 1)?];
        while self.eat(b',') {
            args.push(self.node(depth + 1)?);
        }
        if !self.eat(b'>') {
            return Err(format!("expected '>' at offset {}", self.pos));
        }
        Ok(Node::Generic(head.to_ascii_lowercase(), args))
    }
}

fn parse(type_str: &str) -> Result<Node, String> {
    let mut r = Reader {
        src: type_str.as_bytes(),
        pos: 0,
    };
    let node = r.node(0)?;
    r.skip_ws();
    if r.pos != r.src.len() {
        return Err(format!("trailing characters at offset {}", r.pos));
    }
    Ok(node)
}

fn is_jsonb_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("jsonb") || name == JSONB_MARSHAL_CLASS
}

/// Strip any `frozen<...>` wrappers.
fn unfrozen(node: &Node) -> &Node {
    match node {
        Node::Generic(head, args) if head == "frozen" && args.len() == 1 => unfrozen(&args[0]),
        other => other,
    }
}

fn is_jsonb(node: &Node) -> bool {
    matches!(unfrozen(node), Node::Name(n) if is_jsonb_name(n))
}

fn cql_contains_jsonb(t: &CqlType) -> bool {
    match t {
        CqlType::Jsonb => true,
        CqlType::List(i) | CqlType::Set(i) | CqlType::Vector(i, _) => cql_contains_jsonb(i),
        CqlType::Map(k, v) => cql_contains_jsonb(k) || cql_contains_jsonb(v),
        CqlType::Tuple(ts) => ts.iter().any(cql_contains_jsonb),
        CqlType::Udt { fields, .. } => fields.iter().any(|(_, ft)| cql_contains_jsonb(ft)),
        CqlType::Ascii
        | CqlType::Bigint
        | CqlType::Blob
        | CqlType::Boolean
        | CqlType::Counter
        | CqlType::Decimal
        | CqlType::Double
        | CqlType::Float
        | CqlType::Int
        | CqlType::Timestamp
        | CqlType::Uuid
        | CqlType::Varchar
        | CqlType::Varint
        | CqlType::Timeuuid
        | CqlType::Inet
        | CqlType::Date
        | CqlType::Time
        | CqlType::Smallint
        | CqlType::Tinyint
        | CqlType::Duration => false,
    }
}

/// Resolve a bare name to a UDT, `ks.type` or a type in `keyspace`.
fn resolve_udt(name: &str, keyspace: &str, types: &TypeMap) -> Option<CqlType> {
    let (ks, tn) = name.split_once('.').unwrap_or((keyspace, name));
    let udt = types.get(&(ks.to_string(), tn.to_string()))?;
    Some(CqlType::Udt {
        keyspace: udt.keyspace.clone(),
        name: udt.name.clone(),
        fields: udt.fields.clone(),
    })
}

fn node_contains_jsonb(node: &Node, keyspace: &str, types: &TypeMap) -> bool {
    match node {
        Node::Num => false,
        Node::Name(n) if is_jsonb_name(n) => true,
        Node::Name(n) => resolve_udt(n, keyspace, types).is_some_and(|t| cql_contains_jsonb(&t)),
        Node::Generic(_, args) => args.iter().any(|a| node_contains_jsonb(a, keyspace, types)),
    }
}

/// The D21 nestings: set element, map key, vector element, at any depth.
fn node_nesting(
    node: &Node,
    keyspace: &str,
    types: &TypeMap,
) -> Result<(), ferrosa_common::cql_type::names::JsonbNestingError> {
    use ferrosa_common::cql_type::names::JsonbNestingError as E;
    match node {
        Node::Num => Ok(()),
        Node::Name(n) => match resolve_udt(n, keyspace, types) {
            Some(t) => check_jsonb_nesting(&t),
            None => Ok(()),
        },
        Node::Generic(head, args) => {
            let first_is_jsonb = args.first().is_some_and(is_jsonb);
            match head.as_str() {
                "set" if first_is_jsonb => return Err(E::SetElement),
                "map" if first_is_jsonb => return Err(E::MapKey),
                "vector" if first_is_jsonb => return Err(E::VectorElement),
                _ => {}
            }
            args.iter()
                .try_for_each(|a| node_nesting(a, keyspace, types))
        }
    }
}

fn is_key_column(table: &TableMetadata, col: &ColumnMetadata) -> Option<&'static str> {
    if col.kind == ColumnKind::PartitionKey || table.partition_key.contains(&col.name) {
        Some("partition key")
    } else if col.kind == ColumnKind::Clustering
        || table.clustering_key.iter().any(|(n, _)| *n == col.name)
    {
        Some("clustering key")
    } else {
        None
    }
}

/// Check one column of `table`. `key_position` is `Some` for key columns.
fn check_column(
    table: &TableMetadata,
    col: &ColumnMetadata,
    key_position: Option<&'static str>,
    types: &TypeMap,
) -> crate::Result<()> {
    let node = parse(&col.column_type).map_err(|reason| {
        SchemaError::InvalidSchema(format!(
            "{}.{} column '{}': type '{}' cannot be checked for jsonb placement: {reason}",
            table.keyspace, table.name, col.name, col.column_type
        ))
    })?;
    if let Some(position) = key_position {
        if node_contains_jsonb(&node, &table.keyspace, types) {
            return Err(SchemaError::JsonbInKey {
                keyspace: table.keyspace.clone(),
                table: table.name.clone(),
                column: col.name.clone(),
                position,
            });
        }
    }
    node_nesting(&node, &table.keyspace, types).map_err(|rule| SchemaError::JsonbNesting {
        keyspace: table.keyspace.clone(),
        table: table.name.clone(),
        column: col.name.clone(),
        rule,
    })
}

/// Refuse jsonb in key positions and the forbidden nestings for every column
/// of `table` (SCH-T154a-01, -02, -03).
pub fn check_table(table: &TableMetadata, types: &TypeMap) -> crate::Result<()> {
    for col in table.columns.values() {
        check_column(table, col, is_key_column(table, col), types)?;
    }
    Ok(())
}

/// Re-check only the key columns of every table against `types`, used when a
/// UDT changes: a jsonb field added to a UDT already used in a key would put
/// jsonb in that key (SCH-T154a-05, D21).
pub fn check_key_columns<'a>(
    tables: impl IntoIterator<Item = &'a TableMetadata>,
    types: &TypeMap,
) -> crate::Result<()> {
    for table in tables {
        for col in table.columns.values() {
            if let Some(position) = is_key_column(table, col) {
                check_column(table, col, Some(position), types)?;
            }
        }
    }
    Ok(())
}

/// Check columns being added to an existing `table` (ALTER TABLE ADD). Added
/// columns are regular unless they claim otherwise; a key-kind column is
/// refused when it carries jsonb (SCH-T154a-02).
pub fn check_added_columns(
    table: &TableMetadata,
    added: &[ColumnMetadata],
    types: &TypeMap,
) -> crate::Result<()> {
    for col in added {
        check_column(table, col, is_key_column(table, col), types)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_parses_nested_generics_and_dimension() {
        let n = parse("frozen<map<text, vector<float, 3>>>").expect("parses");
        assert!(matches!(n, Node::Generic(ref h, _) if h == "frozen"));
        assert!(parse("list<").is_err());
        assert!(parse("int int").is_err());
    }

    #[test]
    fn quoted_marshal_class_is_jsonb() {
        let n = parse("'org.apache.cassandra.db.marshal.JsonbType'").expect("parses");
        assert!(is_jsonb(&n));
    }
}
