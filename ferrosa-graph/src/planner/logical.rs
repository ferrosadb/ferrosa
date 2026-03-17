//! Logical planner: label resolution and validation.
//!
//! Resolves Cypher labels to tables using schema extensions and validates
//! property references. Performs per-hop authorization checks (T3).

use std::collections::{HashMap, HashSet};

use ferrosa_schema::auth::permission::{check_permission, Permission, Resource};
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::registry::SchemaSnapshot;

use crate::error::{GraphError, Result};
use crate::parser::{Pattern, Statement};

/// A table resolved from a graph label.
#[derive(Debug, Clone)]
pub struct ResolvedTable {
    /// Keyspace the table belongs to.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// Graph type: "vertex" or "edge".
    pub graph_type: String,
    /// The graph label (from extensions).
    pub label: String,
}

/// A logical plan: resolved bindings plus the original statement.
#[derive(Debug)]
pub struct LogicalPlan {
    /// Variable bindings: variable name -> resolved table.
    pub bindings: HashMap<String, ResolvedTable>,
    /// The original parsed statement.
    pub statement: Statement,
    /// The keyspace context for this query.
    pub keyspace: String,
}

impl LogicalPlan {
    /// Returns the set of (keyspace, table) pairs this query depends on.
    /// Used by SUBSCRIBE to know which tables to watch for changes.
    pub fn table_dependencies(&self) -> Vec<(String, String)> {
        let mut seen = HashSet::new();
        let mut deps = Vec::new();
        for resolved in self.bindings.values() {
            let key = (resolved.keyspace.clone(), resolved.table.clone());
            if seen.insert(key.clone()) {
                deps.push(key);
            }
        }
        deps
    }
}

/// Resolve a graph label to a table in the schema snapshot.
///
/// Scans `snap.tables` for a table in the given keyspace whose
/// `graph.label` extension matches `label` (case-insensitive).
pub fn resolve_label(snap: &SchemaSnapshot, keyspace: &str, label: &str) -> Result<ResolvedTable> {
    let label_lower = label.to_lowercase();

    for ((_ks, _tbl), meta) in &snap.tables {
        if meta.keyspace != keyspace {
            continue;
        }
        if let Some(graph_label) = meta.extensions.get("graph.label") {
            if graph_label.to_lowercase() == label_lower {
                let graph_type = meta
                    .extensions
                    .get("graph.type")
                    .cloned()
                    .unwrap_or_default();
                return Ok(ResolvedTable {
                    keyspace: meta.keyspace.clone(),
                    table: meta.name.clone(),
                    graph_type,
                    label: graph_label.clone(),
                });
            }
        }
    }

    Err(GraphError::Validation(format!(
        "no table with graph.label '{label}' found in keyspace '{keyspace}'"
    )))
}

/// Check that `auth` has the required permission on a resolved table.
fn check_table_permission(
    snap: &SchemaSnapshot,
    auth: &AuthContext,
    perm: Permission,
    resolved: &ResolvedTable,
) -> Result<()> {
    let resource = Resource::Table(resolved.keyspace.clone(), resolved.table.clone());
    check_permission(snap, auth, perm, &resource).map_err(|e| {
        GraphError::PermissionDenied(format!(
            "permission {perm} denied on {}.{}: {e}",
            resolved.keyspace, resolved.table
        ))
    })
}

/// Determine the permission needed for a statement.
fn permission_for_statement(stmt: &Statement) -> Permission {
    match stmt {
        Statement::Match { .. } => Permission::Select,
        Statement::Create { .. } => Permission::Modify,
        Statement::Set { .. } => Permission::Modify,
        Statement::Delete { .. } => Permission::Modify,
        Statement::Subscribe { .. } => Permission::Select,
        Statement::Unsubscribe { .. } => Permission::Select,
    }
}

/// Validate a Cypher statement: resolve labels and check permissions.
///
/// Returns a `LogicalPlan` with all bindings resolved.
pub fn validate(
    snap: &SchemaSnapshot,
    auth: &AuthContext,
    keyspace: &str,
    statement: Statement,
) -> Result<LogicalPlan> {
    let perm = permission_for_statement(&statement);
    let mut bindings = HashMap::new();

    // Collect patterns from the statement.
    let patterns: &[Pattern] = match &statement {
        Statement::Match { pattern, .. } => pattern,
        Statement::Create { patterns } => patterns,
        Statement::Set { pattern, .. } => pattern,
        Statement::Delete { pattern, .. } => pattern,
        Statement::Subscribe { inner, .. } => match inner.as_ref() {
            Statement::Match { pattern, .. } => pattern,
            _ => {
                return Err(GraphError::Validation(
                    "SUBSCRIBE requires a MATCH query".to_string(),
                ))
            }
        },
        Statement::Unsubscribe { .. } => {
            return Ok(LogicalPlan {
                bindings,
                statement,
                keyspace: keyspace.to_string(),
            });
        }
    };

    for pat in patterns {
        resolve_pattern_labels(snap, auth, keyspace, perm, pat, &mut bindings)?;
    }

    Ok(LogicalPlan {
        bindings,
        statement,
        keyspace: keyspace.to_string(),
    })
}

/// Walk a pattern, resolving labels and checking permissions for each element.
fn resolve_pattern_labels(
    snap: &SchemaSnapshot,
    auth: &AuthContext,
    keyspace: &str,
    perm: Permission,
    pattern: &Pattern,
    bindings: &mut HashMap<String, ResolvedTable>,
) -> Result<()> {
    match pattern {
        Pattern::Node { var, label, .. } => {
            if let Some(label_str) = label {
                let resolved = resolve_label(snap, keyspace, label_str)?;
                check_table_permission(snap, auth, perm, &resolved)?;
                if let Some(var_name) = var {
                    bindings.insert(var_name.clone(), resolved);
                }
            }
        }
        Pattern::Rel { var, rel_type, .. } => {
            if let Some(rel_type_str) = rel_type {
                let resolved = resolve_label(snap, keyspace, rel_type_str)?;
                check_table_permission(snap, auth, perm, &resolved)?;
                if let Some(var_name) = var {
                    bindings.insert(var_name.clone(), resolved);
                }
            }
        }
        Pattern::Path(elements) => {
            for elem in elements {
                resolve_pattern_labels(snap, auth, keyspace, perm, elem, bindings)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use indexmap::IndexMap;
    use uuid::Uuid;

    use ferrosa_schema::metadata::table::{TableMetadata, TableParams};

    fn make_snap_with_vertex_table(keyspace: &str, table: &str, label: &str) -> SchemaSnapshot {
        let mut snap = SchemaSnapshot::new();
        let mut extensions = HashMap::new();
        extensions.insert("graph.type".to_string(), "vertex".to_string());
        extensions.insert("graph.label".to_string(), label.to_string());

        let meta = TableMetadata {
            keyspace: keyspace.to_string(),
            name: table.to_string(),
            id: Uuid::new_v4(),
            columns: IndexMap::new(),
            partition_key: vec![],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions,
            is_system: false,
        };
        snap.tables
            .insert((keyspace.to_string(), table.to_string()), meta);
        snap
    }

    fn superuser_auth() -> AuthContext {
        AuthContext {
            role: "admin".to_string(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    #[test]
    fn resolve_label_finds_vertex_table() {
        let snap = make_snap_with_vertex_table("social", "person_v", "Person");
        let resolved = resolve_label(&snap, "social", "Person").unwrap();
        assert_eq!(resolved.keyspace, "social");
        assert_eq!(resolved.table, "person_v");
        assert_eq!(resolved.graph_type, "vertex");
        assert_eq!(resolved.label, "Person");
    }

    #[test]
    fn resolve_label_case_insensitive() {
        let snap = make_snap_with_vertex_table("social", "person_v", "Person");
        let resolved = resolve_label(&snap, "social", "person").unwrap();
        assert_eq!(resolved.table, "person_v");
    }

    #[test]
    fn resolve_label_missing_returns_error() {
        let snap = SchemaSnapshot::new();
        let result = resolve_label(&snap, "social", "Person");
        assert!(result.is_err());
        match result.unwrap_err() {
            GraphError::Validation(msg) => {
                assert!(msg.contains("Person"));
                assert!(msg.contains("social"));
            }
            other => panic!("expected Validation error, got: {other:?}"),
        }
    }

    #[test]
    fn resolve_label_wrong_keyspace_not_found() {
        let snap = make_snap_with_vertex_table("social", "person_v", "Person");
        let result = resolve_label(&snap, "other_ks", "Person");
        assert!(result.is_err());
    }

    #[test]
    fn validate_match_resolves_bindings() {
        let snap = make_snap_with_vertex_table("social", "person_v", "Person");
        let auth = superuser_auth();

        let stmt = Statement::Match {
            pattern: vec![Pattern::Node {
                var: Some("n".into()),
                label: Some("Person".into()),
                props: vec![],
            }],
            where_clause: None,
            return_clause: crate::parser::ReturnClause {
                distinct: false,
                items: vec![crate::parser::ReturnItem {
                    expr: crate::parser::Expr::Var("n".into()),
                    alias: None,
                }],
                order_by: vec![],
                limit: None,
            },
        };

        let plan = validate(&snap, &auth, "social", stmt).unwrap();
        assert!(plan.bindings.contains_key("n"));
        assert_eq!(plan.bindings["n"].table, "person_v");
        assert_eq!(plan.keyspace, "social");
    }

    #[test]
    fn validate_permission_denied_for_non_superuser() {
        let snap = make_snap_with_vertex_table("social", "person_v", "Person");
        // No grants for this role
        let auth = AuthContext {
            role: "nobody".to_string(),
            is_superuser: false,
            must_change_password: false,
        };

        let stmt = Statement::Match {
            pattern: vec![Pattern::Node {
                var: Some("n".into()),
                label: Some("Person".into()),
                props: vec![],
            }],
            where_clause: None,
            return_clause: crate::parser::ReturnClause {
                distinct: false,
                items: vec![],
                order_by: vec![],
                limit: None,
            },
        };

        let result = validate(&snap, &auth, "social", stmt);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GraphError::PermissionDenied(_)
        ));
    }

    /// Insert a graph-labeled table into an existing snapshot.
    fn add_graph_table(
        snap: &mut SchemaSnapshot,
        keyspace: &str,
        table: &str,
        label: &str,
        graph_type: &str,
    ) {
        let mut extensions = HashMap::new();
        extensions.insert("graph.type".to_string(), graph_type.to_string());
        extensions.insert("graph.label".to_string(), label.to_string());

        let meta = TableMetadata {
            keyspace: keyspace.to_string(),
            name: table.to_string(),
            id: Uuid::new_v4(),
            columns: IndexMap::new(),
            partition_key: vec![],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions,
            is_system: false,
        };
        snap.tables
            .insert((keyspace.to_string(), table.to_string()), meta);
    }

    #[test]
    fn planner_returns_table_dependencies() {
        // Build a schema with a vertex table (User) and an edge table (FOLLOWS).
        let mut snap = SchemaSnapshot::new();
        add_graph_table(&mut snap, "social", "user_v", "User", "vertex");
        add_graph_table(&mut snap, "social", "follows_e", "FOLLOWS", "edge");

        let auth = superuser_auth();

        // MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u, f
        let stmt = Statement::Match {
            pattern: vec![Pattern::Path(vec![
                Pattern::Node {
                    var: Some("u".into()),
                    label: Some("User".into()),
                    props: vec![],
                },
                Pattern::Rel {
                    var: Some("r".into()),
                    rel_type: Some("FOLLOWS".into()),
                    direction: crate::parser::Direction::Out,
                    props: vec![],
                    length_range: None,
                },
                Pattern::Node {
                    var: Some("f".into()),
                    label: Some("User".into()),
                    props: vec![],
                },
            ])],
            where_clause: None,
            return_clause: crate::parser::ReturnClause {
                distinct: false,
                items: vec![
                    crate::parser::ReturnItem {
                        expr: crate::parser::Expr::Var("u".into()),
                        alias: None,
                    },
                    crate::parser::ReturnItem {
                        expr: crate::parser::Expr::Var("f".into()),
                        alias: None,
                    },
                ],
                order_by: vec![],
                limit: None,
            },
        };

        let plan = validate(&snap, &auth, "social", stmt).unwrap();
        let deps = plan.table_dependencies();

        // Should have at least 2 entries: user_v (vertex) and follows_e (edge).
        // Note: u and f both resolve to user_v, so de-duplication yields 2 unique tables.
        assert!(
            deps.len() >= 2,
            "expected at least 2 table deps, got {}",
            deps.len()
        );

        // Verify the expected tables are present.
        let dep_set: HashSet<_> = deps.into_iter().collect();
        assert!(
            dep_set.contains(&("social".to_string(), "user_v".to_string())),
            "missing user_v dependency"
        );
        assert!(
            dep_set.contains(&("social".to_string(), "follows_e".to_string())),
            "missing follows_e dependency"
        );
    }
}
