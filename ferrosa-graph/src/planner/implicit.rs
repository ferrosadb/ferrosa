//! Schema-on-write: the backing table a first write implies.
//!
//! Ferrosa's graph front end is catalog-first — a Cypher label resolves to a
//! table carrying `graph.label` / `graph.type` extensions. Neo4j, the reference
//! this front end is measured against, is schema-on-write: the first write
//! naming a label creates it. This module derives, from a write statement alone,
//! the [`TableMetadata`] a client would have had to declare up front, so the
//! engine can create it and then execute the write normally.
//!
//! Everything here is a pure function of the statement plus the schema snapshot:
//! no I/O, and no knowledge of the engine. Creation (and its authorization) is
//! the caller's job — see `GraphEngine::ensure_write_tables`.
//!
//! ## The conventions this follows
//!
//! Both rules below are the ones the rest of the tree already uses, so an
//! implicitly created table is indistinguishable from a declared one:
//!
//! - **Table name** — `<label lowercased>_v` for a vertex, `_e` for an edge. The
//!   label→table mapping itself is by extension, not by name
//!   (`planner::logical::resolve_label`), so the name only has to be stable; the
//!   `_v`/`_e` suffix keeps it recognisable and out of the way of a plain CQL
//!   table that happens to share the label's name. This is the shape the repo's
//!   own example schemas use (`person_v` / `knows_e`,
//!   `specs-legacy/archive/bolt-compat-testing.md`).
//! - **Columns** — a `uuid` partition key named `id`, then one column per
//!   property, in the order the pattern first spells them. `id` is the key the
//!   write path fills in for a write that omits it: `create_row_from_schema`
//!   synthesizes a UUID for a missing key column (`executor/expand.rs`), and a
//!   `uuid` column is what the traversal path addresses (`graph_vertex_lookup_key`,
//!   `build_neighbor_vertex_lookup_shape`). A property named `id` is used as that
//!   key column instead, preserving its declared type.
//! - **Types** — the literal's Cypher type: integer → `bigint`, float →
//!   `double`, string → `text`, boolean → `boolean`. A value that is not a
//!   literal (a list, a map, a computed expression) or is `null` has no
//!   inferable type and is an error, not a guess: the column type is fixed at
//!   creation, and a wrong guess silently mis-encodes every later write of that
//!   property (`encode_storage_ordered_cells` does not coerce). Declaring the
//!   table with `CREATE TABLE` remains the way to store those.
//!
//! ## Edges
//!
//! An edge table must declare its endpoint columns *and* endpoint labels
//! (`graph.source` / `graph.target` / `graph.source_label` / `graph.target_label`,
//! enforced by `ferrosa_schema::registry::validate_graph_extensions`). The
//! endpoint labels are taken from the path the relationship sits in — for
//! `MERGE (a:Person)-[:KNOWS]->(b:Person)` they are both `Person`. A relationship
//! whose endpoints carry no label anywhere in the statement cannot be given the
//! contract the DDL enforces, so it is refused rather than created without one.

mod columns;
pub use columns::columns_needing_declarations;

use std::collections::HashMap;

use indexmap::IndexMap;
use uuid::Uuid;

use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnMetadata};
use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
use ferrosa_schema::registry::SchemaSnapshot;
use ferrosa_schema::ColumnKind;

use crate::error::{GraphError, Result};
use crate::parser::{Assignment, Expr, Literal, Pattern, Statement};

/// Columns an implicitly created edge table uses for its endpoints.
const SOURCE_COLUMN: &str = "src_id";
const TARGET_COLUMN: &str = "dst_id";
/// Partition key column of an implicitly created vertex table.
const VERTEX_KEY_COLUMN: &str = "id";
/// Longest identifier `ferrosa_schema::validation` accepts.
const MAX_TABLE_NAME_LEN: usize = 48;

/// Which kind of graph entity a label denotes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelKind {
    Vertex,
    Edge,
}

impl LabelKind {
    fn suffix(self) -> &'static str {
        match self {
            LabelKind::Vertex => "v",
            LabelKind::Edge => "e",
        }
    }

    fn graph_type(self) -> &'static str {
        match self {
            LabelKind::Vertex => "vertex",
            LabelKind::Edge => "edge",
        }
    }
}

/// A label a write statement needs a backing table for, with everything the
/// table's shape depends on.
#[derive(Debug, Clone, PartialEq)]
pub struct NewLabel {
    /// The label exactly as the statement spells it.
    pub label: String,
    pub kind: LabelKind,
    /// Properties written to the entity, in first-appearance order.
    pub props: Vec<(String, Expr)>,
    /// `(source label, target label)`, for an edge.
    pub endpoints: Option<(String, String)>,
}

/// The table name a label maps to.
///
/// Deterministic and total for any label: lowercase, then `_v` / `_e`. A label
/// that is not already a plain identifier (Cypher labels are, but this is a
/// public entry point) is rejected rather than sanitized, because silently
/// rewriting it would break the one-table-per-label contract when two labels
/// sanitize alike.
pub fn table_name_for_label(label: &str, kind: LabelKind) -> Result<String> {
    let lowered = label.to_lowercase();
    let name = format!("{lowered}_{}", kind.suffix());
    let valid = !lowered.is_empty()
        && lowered
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name.len() <= MAX_TABLE_NAME_LEN;
    if !valid {
        return Err(GraphError::Validation(format!(
            "label '{label}' cannot be mapped to a table name: a label must be \
             1-{} ASCII letters, digits or underscores (derived name would be '{name}')",
            MAX_TABLE_NAME_LEN - kind.suffix().len() - 1
        )));
    }
    Ok(name)
}

/// Every label a write statement names that has no backing table in `keyspace`,
/// in the order the tables must be created (vertices before edges).
///
/// Returns an empty vec for reads, and for writes whose labels all resolve — the
/// common case, and the only one that touches the schema.
pub fn labels_needing_tables(
    snap: &SchemaSnapshot,
    keyspace: &str,
    statement: &Statement,
) -> Result<Vec<NewLabel>> {
    let mut builder = LabelCollector {
        snap,
        keyspace,
        var_labels: HashMap::new(),
        vertices: Vec::new(),
        edges: Vec::new(),
    };
    match statement {
        Statement::Create {
            patterns,
            return_clause: _,
        } => builder.collect(patterns, &[])?,
        Statement::Merge {
            patterns,
            set_clause,
            return_clause: _,
        } => builder.collect(patterns, set_clause)?,
        // Reads, SET / REMOVE / DELETE, SUBSCRIBE and the orchestrator-owned
        // statements (FOREACH, CALL {}, UNWIND) never introduce a label: the
        // orchestrator re-enters with the inner update clause, so the hook that
        // calls this runs on the statement that actually writes.
        _ => {}
    }
    let mut labels = builder.vertices;
    labels.extend(builder.edges);
    Ok(labels)
}

struct LabelCollector<'a> {
    snap: &'a SchemaSnapshot,
    keyspace: &'a str,
    /// Variable → label, for the endpoints of a relationship.
    var_labels: HashMap<String, String>,
    vertices: Vec<NewLabel>,
    edges: Vec<NewLabel>,
}

impl<'a> LabelCollector<'a> {
    fn collect(&mut self, patterns: &[Pattern], set_clause: &[Assignment]) -> Result<()> {
        // Two passes: every label bound in the statement first, so a
        // relationship can name its endpoints whichever order the patterns are
        // written in (`MERGE (a)-[:K]->(b)`, `MERGE (b:B)` is not legal Cypher,
        // but `MERGE (a:A) MERGE (a)-[:K]->(b:B)` is).
        for pattern in patterns {
            self.bind_labels(pattern);
        }
        for pattern in patterns {
            self.collect_pattern(pattern, set_clause)?;
        }
        Ok(())
    }

    fn bind_labels(&mut self, pattern: &Pattern) {
        match pattern {
            Pattern::Node {
                var: Some(var),
                label: Some(label),
                ..
            } => {
                self.var_labels.insert(var.clone(), label.clone());
            }
            Pattern::Path(elements) => {
                for element in elements {
                    self.bind_labels(element);
                }
            }
            _ => {}
        }
    }

    fn collect_pattern(&mut self, pattern: &Pattern, set_clause: &[Assignment]) -> Result<()> {
        match pattern {
            Pattern::Node { label, props, .. } => {
                if let Some(label) = label {
                    self.add_vertex(label, props, set_clause);
                }
            }
            // The AST keeps a relationship inside a `Path` (it has no neighbours
            // on its own), so a bare one has no endpoints to name.
            Pattern::Rel {
                rel_type: Some(rel_type),
                props,
                ..
            } if !rel_type.contains('|') => self.add_edge(rel_type, props, None)?,
            Pattern::Rel { .. } => {}
            Pattern::Path(elements) => {
                for (idx, element) in elements.iter().enumerate() {
                    match element {
                        Pattern::Rel {
                            rel_type: Some(rel_type),
                            props,
                            ..
                        } if !rel_type.contains('|') => {
                            let source = idx
                                .checked_sub(1)
                                .and_then(|i| elements.get(i))
                                .and_then(|node| self.node_label(node));
                            let target =
                                elements.get(idx + 1).and_then(|node| self.node_label(node));
                            let endpoints = match (source, target) {
                                (Some(source), Some(target)) => Some((source, target)),
                                _ => None,
                            };
                            self.add_edge(rel_type, props, endpoints)?;
                        }
                        other => self.collect_pattern(other, set_clause)?,
                    }
                }
            }
        }
        Ok(())
    }

    /// The label a node pattern carries, directly or through its variable.
    fn node_label(&self, pattern: &Pattern) -> Option<String> {
        match pattern {
            Pattern::Node { var, label, .. } => label
                .clone()
                .or_else(|| var.as_ref().and_then(|v| self.var_labels.get(v).cloned())),
            _ => None,
        }
    }

    fn add_vertex(&mut self, label: &str, props: &[(String, Expr)], set_clause: &[Assignment]) {
        if self.label_is_declared(label) {
            return;
        }
        let mut props = props.to_vec();
        // A MERGE's SET clause lands in the same row as the create, so those
        // properties must exist as columns of the table being created.
        for assignment in set_clause {
            push_unique(
                &mut props,
                (assignment.property.clone(), assignment.value.clone()),
            );
        }
        push_vertex(&mut self.vertices, label, props);
    }

    fn add_edge(
        &mut self,
        label: &str,
        props: &[(String, Expr)],
        endpoints: Option<(String, String)>,
    ) -> Result<()> {
        if self.label_is_declared(label) {
            return Ok(());
        }
        let Some(endpoints) = endpoints else {
            return Err(GraphError::Validation(format!(
                "cannot create a table for relationship type '{label}': its endpoints carry no \
                 label, so the table cannot declare graph.source_label / graph.target_label, \
                 which an edge table requires. Label the endpoints \
                 (`MERGE (a:Label)-[:{label}]->(b:Label)`) or declare the table with \
                 CREATE TABLE {}.{} ({SOURCE_COLUMN} uuid, {TARGET_COLUMN} uuid, ... \
                 PRIMARY KEY ({SOURCE_COLUMN}, {TARGET_COLUMN})) WITH extensions = \
                 {{'graph.type': 'edge', 'graph.label': '{label}', ...}}.",
                self.keyspace,
                table_name_for_label(label, LabelKind::Edge).unwrap_or_else(|_| "<table>".into()),
            )));
        };
        push_edge(&mut self.edges, label, props.to_vec(), endpoints);
        Ok(())
    }

    fn label_is_declared(&self, label: &str) -> bool {
        crate::planner::logical::resolve_label(self.snap, self.keyspace, label).is_ok()
    }
}

fn push_unique(props: &mut Vec<(String, Expr)>, prop: (String, Expr)) {
    if !props.iter().any(|(name, _)| *name == prop.0) {
        props.push(prop);
    }
}

fn push_vertex(labels: &mut Vec<NewLabel>, label: &str, props: Vec<(String, Expr)>) {
    match labels.iter_mut().find(|existing| existing.label == label) {
        Some(existing) => {
            for prop in props {
                push_unique(&mut existing.props, prop);
            }
        }
        None => labels.push(NewLabel {
            label: label.to_string(),
            kind: LabelKind::Vertex,
            props,
            endpoints: None,
        }),
    }
}

fn push_edge(
    labels: &mut Vec<NewLabel>,
    label: &str,
    props: Vec<(String, Expr)>,
    endpoints: (String, String),
) {
    match labels.iter_mut().find(|existing| existing.label == label) {
        Some(existing) => {
            for prop in props {
                push_unique(&mut existing.props, prop);
            }
        }
        None => labels.push(NewLabel {
            label: label.to_string(),
            kind: LabelKind::Edge,
            props,
            endpoints: Some(endpoints),
        }),
    }
}

/// The table an implicitly created label gets: the metadata an explicit
/// `CREATE TABLE ... WITH extensions = {...}` would have produced.
pub fn table_metadata(
    snap: &SchemaSnapshot,
    keyspace: &str,
    new: &NewLabel,
) -> Result<TableMetadata> {
    let name = table_name_for_label(&new.label, new.kind)?;
    let mut extensions = HashMap::new();
    extensions.insert("graph.type".to_string(), new.kind.graph_type().to_string());
    extensions.insert("graph.label".to_string(), new.label.clone());

    let mut columns = IndexMap::new();
    let mut partition_key = Vec::new();
    let mut clustering_key = Vec::new();

    match new.kind {
        LabelKind::Vertex => {
            // A property named `id` is the vertex key; otherwise the key is the uuid
            // `create_row_from_schema` synthesizes for a CREATE that omits it.
            let key_type = match new.props.iter().find(|(name, _)| name == VERTEX_KEY_COLUMN) {
                Some((_, value)) => column_type(new.label.as_str(), VERTEX_KEY_COLUMN, value)?,
                None => "uuid".to_string(),
            };
            columns.insert(
                VERTEX_KEY_COLUMN.to_string(),
                column(VERTEX_KEY_COLUMN, ColumnKind::PartitionKey, 0, &key_type),
            );
            partition_key.push(VERTEX_KEY_COLUMN.to_string());
        }
        LabelKind::Edge => {
            let (source_label, target_label) = new.endpoints.as_ref().ok_or_else(|| {
                GraphError::Validation(format!(
                    "cannot create a table for relationship type '{}': its endpoint labels \
                     are unknown",
                    new.label
                ))
            })?;
            let endpoint_type = endpoint_key_type(snap, keyspace, source_label, target_label)?;
            columns.insert(
                SOURCE_COLUMN.to_string(),
                column(SOURCE_COLUMN, ColumnKind::PartitionKey, 0, &endpoint_type),
            );
            columns.insert(
                TARGET_COLUMN.to_string(),
                column(TARGET_COLUMN, ColumnKind::Clustering, 0, &endpoint_type),
            );
            partition_key.push(SOURCE_COLUMN.to_string());
            clustering_key.push((TARGET_COLUMN.to_string(), ClusteringOrder::Asc));
            extensions.insert("graph.source".to_string(), SOURCE_COLUMN.to_string());
            extensions.insert("graph.target".to_string(), TARGET_COLUMN.to_string());
            extensions.insert("graph.source_label".to_string(), source_label.clone());
            extensions.insert("graph.target_label".to_string(), target_label.clone());
        }
    }

    for (prop, value) in &new.props {
        if columns.contains_key(prop) {
            continue;
        }
        let ty = column_type(&new.label, prop, value)?;
        columns.insert(prop.clone(), column(prop, ColumnKind::Regular, 0, &ty));
    }

    Ok(TableMetadata {
        keyspace: keyspace.to_string(),
        name,
        id: Uuid::new_v4(),
        columns,
        partition_key,
        clustering_key,
        // Everything below is what `route_create_table` builds for a CREATE
        // TABLE with no table options and no flags.
        params: TableParams::default(),
        flags: Default::default(),
        extensions,
        is_system: false,
    })
}

/// The column type an edge's endpoint column must have: the type of the vertex
/// table's key, so a `uuid` vertex key stays 16 raw bytes on the edge row.
fn endpoint_key_type(
    snap: &SchemaSnapshot,
    keyspace: &str,
    source_label: &str,
    target_label: &str,
) -> Result<String> {
    let key_type_of = |label: &str| -> Result<String> {
        let resolved = crate::planner::logical::resolve_label(snap, keyspace, label)?;
        let meta = snap
            .tables
            .get(&(resolved.keyspace.clone(), resolved.table.clone()))
            .ok_or_else(|| {
                GraphError::Validation(format!(
                    "endpoint label '{label}' resolved to {}.{}, which is not in the schema",
                    resolved.keyspace, resolved.table
                ))
            })?;
        match meta.partition_key.as_slice() {
            [single] => meta
                .columns
                .get(single)
                .map(|c| c.column_type.clone())
                .ok_or_else(|| {
                    GraphError::Validation(format!(
                        "endpoint label '{label}' names table {}.{}, whose key column \
                         '{single}' has no metadata",
                        meta.keyspace, meta.name
                    ))
                }),
            _ => Err(GraphError::Validation(format!(
                "cannot create a table for an edge to label '{label}': its table {}.{} has \
                 a composite partition key, and an implicitly created edge table can only \
                 address a single-component vertex key. Declare the edge table with \
                 CREATE TABLE.",
                meta.keyspace, meta.name
            ))),
        }
    };
    let source = key_type_of(source_label)?;
    let target = key_type_of(target_label)?;
    if source != target {
        return Err(GraphError::Validation(format!(
            "cannot create a table for an edge from label '{source_label}' ({source} key) to \
             label '{target_label}' ({target} key): the endpoint columns of one edge table \
             must have the same type. Declare the edge table with CREATE TABLE."
        )));
    }
    Ok(source)
}

/// The column type a property's first written value implies.
///
/// A non-literal or `null` value has none: guessing would fix a type the value
/// is then encoded against, and `encode_storage_ordered_cells` does not coerce —
/// a guessed `text` would store an integer's big-endian bytes to be read back as
/// text. Failing here keeps that a loud error at the statement that caused it.
fn column_type(label: &str, property: &str, value: &Expr) -> Result<String> {
    match value {
        Expr::Literal(Literal::Integer(_)) => Ok("bigint".to_string()),
        Expr::Literal(Literal::Float(_)) => Ok("double".to_string()),
        Expr::Literal(Literal::String(_)) => Ok("text".to_string()),
        Expr::Literal(Literal::Bool(_)) => Ok("boolean".to_string()),
        Expr::Literal(Literal::Null) => Err(GraphError::Validation(format!(
            "cannot create a table for label '{label}': property '{property}' is null, so its \
             column type cannot be inferred. Write a non-null value first, or declare the \
             table with CREATE TABLE."
        ))),
        other => Err(GraphError::Validation(format!(
            "cannot create a table for label '{label}': property '{property}' is {other:?}, \
             which is not a literal, so its column type cannot be inferred. Declare the table \
             with CREATE TABLE."
        ))),
    }
}

fn column(name: &str, kind: ColumnKind, position: i32, column_type: &str) -> ColumnMetadata {
    ColumnMetadata {
        name: name.to_string(),
        kind,
        position,
        column_type: column_type.to_string(),
        clustering_order: ClusteringOrder::None,
        mask: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;

    #[test]
    fn table_name_is_deterministic_and_case_folded() {
        assert_eq!(
            table_name_for_label("BenchRow", LabelKind::Vertex).unwrap(),
            "benchrow_v"
        );
        assert_eq!(
            table_name_for_label("benchrow", LabelKind::Vertex).unwrap(),
            "benchrow_v"
        );
        assert_eq!(
            table_name_for_label("KNOWS", LabelKind::Edge).unwrap(),
            "knows_e"
        );
        // Same input, same output — the mapping is a pure function of the label.
        for _ in 0..3 {
            assert_eq!(
                table_name_for_label("BenchRow", LabelKind::Vertex).unwrap(),
                table_name_for_label("BenchRow", LabelKind::Vertex).unwrap()
            );
        }
    }

    #[test]
    fn table_name_rejects_a_label_that_is_not_an_identifier() {
        assert!(table_name_for_label("has space", LabelKind::Vertex).is_err());
        assert!(table_name_for_label("", LabelKind::Vertex).is_err());
    }

    fn statement(query: &str) -> Statement {
        parse(query).expect("parses")
    }

    fn empty_snapshot() -> SchemaSnapshot {
        SchemaSnapshot::new()
    }

    #[test]
    fn a_create_names_one_vertex_label_with_its_properties_in_order() {
        let snap = empty_snapshot();
        let labels = labels_needing_tables(
            &snap,
            "ks",
            &statement("CREATE (n:BenchRow {pk: 1, v: 'x'})"),
        )
        .unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].label, "BenchRow");
        assert_eq!(labels[0].kind, LabelKind::Vertex);
        let props: Vec<&str> = labels[0].props.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(props, vec!["pk", "v"]);
    }

    #[test]
    fn a_read_names_no_labels() {
        let snap = empty_snapshot();
        assert!(
            labels_needing_tables(&snap, "ks", &statement("MATCH (n:Person) RETURN n"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn an_inline_relationship_reports_its_endpoint_labels() {
        let snap = empty_snapshot();
        let labels = labels_needing_tables(
            &snap,
            "ks",
            &statement("MERGE (a:Person)-[:KNOWS]->(b:Person)"),
        )
        .unwrap();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].label, "Person");
        assert_eq!(labels[0].kind, LabelKind::Vertex);
        assert_eq!(labels[1].label, "KNOWS");
        assert_eq!(labels[1].kind, LabelKind::Edge);
        assert_eq!(
            labels[1].endpoints,
            Some(("Person".to_string(), "Person".to_string()))
        );
    }

    #[test]
    fn a_relationship_whose_endpoints_carry_no_label_is_refused() {
        let snap = empty_snapshot();
        let err = labels_needing_tables(&snap, "ks", &statement("MERGE (a)-[:KNOWS]->(b)"))
            .expect_err("an edge table needs endpoint labels");
        assert!(
            err.to_string().contains("endpoint"),
            "the error explains the missing endpoint labels: {err}"
        );
    }

    #[test]
    fn a_merge_set_clause_contributes_columns() {
        let snap = empty_snapshot();
        let labels =
            labels_needing_tables(&snap, "ks", &statement("MERGE (n:R {pk: 1}) SET n.v = 'x'"))
                .unwrap();
        let props: Vec<&str> = labels[0].props.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(props, vec!["pk", "v"]);
    }

    #[test]
    fn an_integer_property_is_bigint_and_a_string_is_text() {
        let snap = empty_snapshot();
        let labels = labels_needing_tables(
            &snap,
            "ks",
            &statement("CREATE (n:L {a: 1, b: 's', c: 1.5, d: true})"),
        )
        .unwrap();
        let table = table_metadata(&snap, "ks", &labels[0]).unwrap();
        assert_eq!(table.columns["a"].column_type, "bigint");
        assert_eq!(table.columns["b"].column_type, "text");
        assert_eq!(table.columns["c"].column_type, "double");
        assert_eq!(table.columns["d"].column_type, "boolean");
        assert_eq!(table.partition_key, vec!["id".to_string()]);
        assert_eq!(table.columns["id"].column_type, "uuid");
    }

    #[test]
    fn a_property_named_id_becomes_the_vertex_key() {
        let snap = empty_snapshot();
        let labels =
            labels_needing_tables(&snap, "ks", &statement("CREATE (n:L {id: 'abc', v: 1})"))
                .unwrap();
        let table = table_metadata(&snap, "ks", &labels[0]).unwrap();
        assert_eq!(table.partition_key, vec!["id".to_string()]);
        assert_eq!(table.columns["id"].column_type, "text");
        assert_eq!(table.columns["id"].kind, ColumnKind::PartitionKey);
    }

    #[test]
    fn a_null_property_has_no_inferable_type() {
        let snap = empty_snapshot();
        let labels =
            labels_needing_tables(&snap, "ks", &statement("CREATE (n:L {a: null})")).unwrap();
        let err = table_metadata(&snap, "ks", &labels[0]).expect_err("null has no column type");
        assert!(err.to_string().contains("'a'"), "{err}");
    }
}
