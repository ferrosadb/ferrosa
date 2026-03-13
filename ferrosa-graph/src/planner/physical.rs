//! Physical planner: Expand plan with anchor selection.
//!
//! Converts a `LogicalPlan` into a `PhysicalPlan` describing how to
//! execute the query against the storage engine.

use crate::error::{GraphError, Result};
use crate::parser::{Direction, Expr, Pattern, ReturnClause, Statement};
use crate::planner::logical::{LogicalPlan, ResolvedTable};

/// A single hop in a graph traversal.
#[derive(Debug, Clone)]
pub struct Hop {
    /// Variable binding for this hop's target node (if any).
    pub var: Option<String>,
    /// Edge label to filter by (if specified).
    pub edge_label: Option<String>,
    /// Direction of the edge traversal.
    pub direction: Direction,
    /// Resolved edge table (if the label was resolved).
    pub edge_table: Option<ResolvedTable>,
    /// Resolved vertex table at the end of this hop (if the label was resolved).
    pub vertex_table: Option<ResolvedTable>,
}

/// The anchor (starting point) of a graph traversal.
#[derive(Debug, Clone)]
pub struct Anchor {
    /// Variable binding for the anchor node.
    pub var: Option<String>,
    /// Resolved table for the anchor.
    pub table: ResolvedTable,
    /// Filter expressions that apply to the anchor.
    pub filters: Vec<Expr>,
}

/// Physical execution plan.
#[derive(Debug)]
pub enum PhysicalPlan {
    /// Expand from an anchor through a sequence of hops.
    Expand {
        /// Starting vertex.
        anchor: Anchor,
        /// Sequence of edge+vertex hops.
        hops: Vec<Hop>,
        /// Return clause for projecting results.
        return_clause: ReturnClause,
    },
}

/// Convert a logical plan into a physical plan.
///
/// Phase 1: only MATCH statements are supported. The first labeled node
/// in the pattern becomes the anchor; remaining rel+node pairs become hops.
pub fn plan(logical: LogicalPlan) -> Result<PhysicalPlan> {
    match &logical.statement {
        Statement::Match {
            pattern,
            where_clause,
            return_clause,
        } => {
            let filters = extract_filters(where_clause);
            plan_match(pattern, &logical.bindings, filters, return_clause.clone())
        }
        Statement::Create { .. } => Err(GraphError::Validation(
            "CREATE is not yet supported in graph query planner".to_string(),
        )),
        Statement::Set { .. } => Err(GraphError::Validation(
            "SET is not yet supported in graph query planner".to_string(),
        )),
        Statement::Delete { .. } => Err(GraphError::Validation(
            "DELETE is not yet supported in graph query planner".to_string(),
        )),
        Statement::Subscribe { .. } => Err(GraphError::Validation(
            "SUBSCRIBE is not yet supported in graph query planner".to_string(),
        )),
        Statement::Unsubscribe { .. } => Err(GraphError::Validation(
            "UNSUBSCRIBE is not yet supported in graph query planner".to_string(),
        )),
    }
}

/// Extract filter expressions from a WHERE clause.
fn extract_filters(where_clause: &Option<Expr>) -> Vec<Expr> {
    match where_clause {
        Some(expr) => vec![expr.clone()],
        None => vec![],
    }
}

/// Plan a MATCH statement: find anchor and build hops.
fn plan_match(
    patterns: &[Pattern],
    bindings: &std::collections::HashMap<String, ResolvedTable>,
    filters: Vec<Expr>,
    return_clause: ReturnClause,
) -> Result<PhysicalPlan> {
    // We expect the pattern to be a Path or a single Node.
    // Flatten: if there's exactly one Path, use its elements.
    let elements: Vec<&Pattern> = if patterns.len() == 1 {
        match &patterns[0] {
            Pattern::Path(elems) => elems.iter().collect(),
            other => vec![other],
        }
    } else {
        patterns.iter().collect()
    };

    // Find the first labeled node as anchor.
    let mut anchor: Option<Anchor> = None;
    let mut hops: Vec<Hop> = Vec::new();
    let mut i = 0;

    while i < elements.len() {
        match &elements[i] {
            Pattern::Node { var, label, .. } => {
                if anchor.is_none() {
                    // Look up the binding for this node.
                    if let Some(var_name) = var {
                        if let Some(resolved) = bindings.get(var_name) {
                            anchor = Some(Anchor {
                                var: var.clone(),
                                table: resolved.clone(),
                                filters: filters.clone(),
                            });
                            i += 1;
                            continue;
                        }
                    }
                    // If the node has a label but no var binding, try label directly.
                    if let Some(_label_str) = label {
                        if let Some(var_name) = var {
                            if let Some(resolved) = bindings.get(var_name) {
                                anchor = Some(Anchor {
                                    var: var.clone(),
                                    table: resolved.clone(),
                                    filters: filters.clone(),
                                });
                                i += 1;
                                continue;
                            }
                        }
                    }
                    // Unlabeled node without binding: skip.
                    i += 1;
                    continue;
                }
                // After anchor is set, this is a hop target. It should have been
                // consumed as part of a rel+node pair, so just advance.
                i += 1;
            }
            Pattern::Rel {
                var: _,
                rel_type,
                direction,
                ..
            } => {
                if anchor.is_none() {
                    return Err(GraphError::Validation(
                        "relationship pattern found before any anchor node".to_string(),
                    ));
                }
                // Consume this rel and the following node as a hop.
                let edge_label = rel_type.clone();
                let edge_table = rel_type.as_ref().and_then(|rt| {
                    bindings
                        .values()
                        .find(|r| r.label.eq_ignore_ascii_case(rt) && r.graph_type == "edge")
                        .cloned()
                });

                // Look for the next node.
                let (next_var, vertex_table) = if i + 1 < elements.len() {
                    if let Pattern::Node { var, .. } = &elements[i + 1] {
                        let vt = var.as_ref().and_then(|v| bindings.get(v)).cloned();
                        (var.clone(), vt)
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

                hops.push(Hop {
                    var: next_var,
                    edge_label,
                    direction: *direction,
                    edge_table,
                    vertex_table,
                });
                i += 2; // Skip rel + node
            }
            Pattern::Path(_) => {
                // Nested paths shouldn't occur at this level after flattening.
                return Err(GraphError::Validation(
                    "nested path patterns are not supported".to_string(),
                ));
            }
        }
    }

    let anchor = anchor.ok_or_else(|| {
        GraphError::Validation(
            "no anchor node found in pattern (need at least one labeled node)".to_string(),
        )
    })?;

    Ok(PhysicalPlan::Expand {
        anchor,
        hops,
        return_clause,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{ReturnClause, ReturnItem};
    use crate::planner::logical::ResolvedTable;
    use std::collections::HashMap;

    fn person_table() -> ResolvedTable {
        ResolvedTable {
            keyspace: "social".to_string(),
            table: "person_v".to_string(),
            graph_type: "vertex".to_string(),
            label: "Person".to_string(),
        }
    }

    fn knows_table() -> ResolvedTable {
        ResolvedTable {
            keyspace: "social".to_string(),
            table: "knows_e".to_string(),
            graph_type: "edge".to_string(),
            label: "KNOWS".to_string(),
        }
    }

    fn simple_return() -> ReturnClause {
        ReturnClause {
            distinct: false,
            items: vec![ReturnItem {
                expr: Expr::Var("n".into()),
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        }
    }

    #[test]
    fn plan_single_node_match() {
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), person_table());

        let logical = LogicalPlan {
            bindings,
            statement: Statement::Match {
                pattern: vec![Pattern::Node {
                    var: Some("n".into()),
                    label: Some("Person".into()),
                    props: vec![],
                }],
                where_clause: None,
                return_clause: simple_return(),
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        match physical {
            PhysicalPlan::Expand { anchor, hops, .. } => {
                assert_eq!(anchor.var, Some("n".to_string()));
                assert_eq!(anchor.table.table, "person_v");
                assert!(hops.is_empty());
            }
        }
    }

    #[test]
    fn plan_one_hop_match() {
        let mut bindings = HashMap::new();
        bindings.insert("a".to_string(), person_table());
        bindings.insert("b".to_string(), person_table());
        bindings.insert("r".to_string(), knows_table());

        let logical = LogicalPlan {
            bindings,
            statement: Statement::Match {
                pattern: vec![Pattern::Path(vec![
                    Pattern::Node {
                        var: Some("a".into()),
                        label: Some("Person".into()),
                        props: vec![],
                    },
                    Pattern::Rel {
                        var: Some("r".into()),
                        rel_type: Some("KNOWS".into()),
                        direction: Direction::Out,
                        props: vec![],
                    },
                    Pattern::Node {
                        var: Some("b".into()),
                        label: Some("Person".into()),
                        props: vec![],
                    },
                ])],
                where_clause: None,
                return_clause: simple_return(),
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        match physical {
            PhysicalPlan::Expand { anchor, hops, .. } => {
                assert_eq!(anchor.var, Some("a".to_string()));
                assert_eq!(hops.len(), 1);
                assert_eq!(hops[0].edge_label, Some("KNOWS".to_string()));
                assert_eq!(hops[0].direction, Direction::Out);
                assert_eq!(hops[0].var, Some("b".to_string()));
            }
        }
    }

    #[test]
    fn plan_create_not_supported() {
        let logical = LogicalPlan {
            bindings: HashMap::new(),
            statement: Statement::Create { patterns: vec![] },
            keyspace: "social".to_string(),
        };

        let result = plan(logical);
        assert!(result.is_err());
        match result.unwrap_err() {
            GraphError::Validation(msg) => assert!(msg.contains("CREATE")),
            other => panic!("expected Validation, got {other:?}"),
        }
    }
}
