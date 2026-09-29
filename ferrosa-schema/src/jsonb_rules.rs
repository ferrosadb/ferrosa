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
//!
//! T-300 (D24): the interim mode gate. jsonb DDL is permitted on a standalone
//! node only, until the D15a capability ledger lands (T-154b replaces
//! [`jsonb_ddl_permitted`] with the ledger check at the same call sites). The
//! `match` over `DeploymentMode` has no wildcard, so a new mode does not
//! compile until it has a rule. No config key, env var or flag bypasses it.
//! Last revised: 2026-09-28
//! Last changed: Added the standalone-only jsonb DDL gate (T-300).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use ferrosa_common::cql_type::names::{check_jsonb_nesting, JSONB_MARSHAL_CLASS};
use ferrosa_common::deployment_mode::DeploymentMode;
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

/// The typed refusal: jsonb DDL on a node in `mode` (T-300, D24, D15a).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonbDdlRefused {
    pub mode: DeploymentMode,
}

impl std::fmt::Display for JsonbDdlRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "jsonb DDL is allowed on a standalone node only until the capability ledger \
             (D15a) lands; this node is in {} mode",
            self.mode
        )
    }
}

impl std::error::Error for JsonbDdlRefused {}

/// The interim D24 rule. Exhaustive on purpose (SCH-T300-01): adding a
/// `DeploymentMode` variant fails to compile here until someone decides it.
pub fn jsonb_ddl_permitted(mode: DeploymentMode) -> Result<(), JsonbDdlRefused> {
    match mode {
        DeploymentMode::Standalone => Ok(()),
        DeploymentMode::Pair
        | DeploymentMode::Forming
        | DeploymentMode::Cluster
        | DeploymentMode::DegradedPair
        | DeploymentMode::DegradedCluster => Err(JsonbDdlRefused { mode }),
    }
}

/// Refusals so far, per mode: the source of `jsonb_ddl_refused_total{mode}`.
static REFUSED: [AtomicU64; 6] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

/// Exhaustive slot for [`REFUSED`]; a new mode must be given one.
fn mode_slot(mode: DeploymentMode) -> usize {
    match mode {
        DeploymentMode::Standalone => 0,
        DeploymentMode::Pair => 1,
        DeploymentMode::Forming => 2,
        DeploymentMode::Cluster => 3,
        DeploymentMode::DegradedPair => 4,
        DeploymentMode::DegradedCluster => 5,
    }
}

/// `jsonb_ddl_refused_total{mode}` for `mode`, since process start.
pub fn jsonb_ddl_refused_total(mode: DeploymentMode) -> u64 {
    REFUSED[mode_slot(mode)].load(Ordering::Relaxed)
}

/// Gate a statement on `subject` (a table, type or column) whose jsonb-ness
/// is `has_jsonb`. One WARN and one counter tick per refusal (SCH-T300-02).
fn gate(mode: DeploymentMode, subject: String, has_jsonb: bool) -> crate::Result<()> {
    if !has_jsonb {
        return Ok(());
    }
    let Err(refused) = jsonb_ddl_permitted(mode) else {
        return Ok(());
    };
    REFUSED[mode_slot(refused.mode)].fetch_add(1, Ordering::Relaxed);
    tracing::warn!(
        mode = %refused.mode,
        %subject,
        "jsonb DDL refused: standalone only until the D15a capability ledger"
    );
    Err(SchemaError::JsonbDdlRefused {
        mode: refused.mode,
        subject,
    })
}

/// True when any of `column_types` is or nests jsonb. An unparseable type is
/// an error, never a skipped check (SCH-T300-03).
fn any_jsonb(column_types: &[&str], keyspace: &str, types: &TypeMap) -> crate::Result<bool> {
    for ty in column_types {
        let node = parse(ty).map_err(|reason| {
            SchemaError::InvalidSchema(format!(
                "type '{ty}' cannot be checked for jsonb placement: {reason}"
            ))
        })?;
        if node_contains_jsonb(&node, keyspace, types) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The one public gate, shared by the CQL and PG DDL paths: refuse when any of
/// `column_types` (CQL type strings, e.g. `frozen<list<jsonb>>`, or UDT names
/// resolved through `types`) is or nests jsonb and `mode` is not standalone.
pub fn check_jsonb_ddl_allowed(
    mode: DeploymentMode,
    keyspace: &str,
    column_types: &[&str],
    types: &TypeMap,
) -> crate::Result<()> {
    if jsonb_ddl_permitted(mode).is_ok() {
        return Ok(());
    }
    let has = any_jsonb(column_types, keyspace, types)?;
    gate(mode, format!("column types in keyspace {keyspace}"), has)
}

/// Gate CREATE TABLE and every apply of a whole table (SCH-T300-02).
pub fn check_table_ddl_allowed(
    mode: DeploymentMode,
    table: &TableMetadata,
    types: &TypeMap,
) -> crate::Result<()> {
    if jsonb_ddl_permitted(mode).is_ok() {
        return Ok(());
    }
    let tys: Vec<&str> = table
        .columns
        .values()
        .map(|c| c.column_type.as_str())
        .collect();
    let has = any_jsonb(&tys, &table.keyspace, types)?;
    gate(
        mode,
        format!("table {}.{}", table.keyspace, table.name),
        has,
    )
}

/// Gate ALTER TABLE ADD (SCH-T300-02).
pub fn check_added_columns_ddl_allowed(
    mode: DeploymentMode,
    table: &TableMetadata,
    added: &[ColumnMetadata],
    types: &TypeMap,
) -> crate::Result<()> {
    if jsonb_ddl_permitted(mode).is_ok() {
        return Ok(());
    }
    let tys: Vec<&str> = added.iter().map(|c| c.column_type.as_str()).collect();
    let has = any_jsonb(&tys, &table.keyspace, types)?;
    gate(
        mode,
        format!("ALTER TABLE {}.{} ADD", table.keyspace, table.name),
        has,
    )
}

/// Gate CREATE TYPE and ALTER TYPE ADD: a field that is or nests jsonb
/// (SCH-T300-04).
pub fn check_udt_fields_ddl_allowed(
    mode: DeploymentMode,
    keyspace: &str,
    name: &str,
    fields: &[(String, CqlType)],
) -> crate::Result<()> {
    if jsonb_ddl_permitted(mode).is_ok() {
        return Ok(());
    }
    let has = fields.iter().any(|(_, t)| cql_contains_jsonb(t));
    gate(mode, format!("type {keyspace}.{name}"), has)
}

/// `keyspace.table` of every non-system table that holds jsonb, directly or
/// through a UDT: what the leaving-standalone refusal names (SCH-T300-05).
pub fn tables_with_jsonb<'a>(
    tables: impl IntoIterator<Item = &'a TableMetadata>,
    types: &TypeMap,
) -> crate::Result<Vec<String>> {
    let mut named = Vec::new();
    for table in tables.into_iter().filter(|t| !t.is_system) {
        let tys: Vec<&str> = table
            .columns
            .values()
            .map(|c| c.column_type.as_str())
            .collect();
        if any_jsonb(&tys, &table.keyspace, types)? {
            named.push(format!("{}.{}", table.keyspace, table.name));
        }
    }
    Ok(named)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonb_ddl_permitted_only_standalone_unit() {
        assert!(jsonb_ddl_permitted(DeploymentMode::Standalone).is_ok());
        for mode in [
            DeploymentMode::Pair,
            DeploymentMode::Forming,
            DeploymentMode::Cluster,
            DeploymentMode::DegradedPair,
            DeploymentMode::DegradedCluster,
        ] {
            let e = jsonb_ddl_permitted(mode).expect_err("refused");
            assert_eq!(e.mode, mode);
            assert!(e.to_string().contains("D15a"));
        }
    }

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
