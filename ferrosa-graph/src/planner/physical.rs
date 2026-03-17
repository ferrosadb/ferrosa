//! Physical planner: Expand plan with anchor selection.
//!
//! Converts a `LogicalPlan` into a `PhysicalPlan` describing how to
//! execute the query against the storage engine.

use crate::error::{GraphError, Result};
use crate::executor::aggregate::is_aggregate_function;
use crate::parser::{Assignment, Direction, Expr, Pattern, PropMap, ReturnClause, Statement};
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
    /// Property filter expressions from the relationship pattern.
    pub prop_filters: PropMap,
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

/// A single CREATE operation (node or relationship).
#[derive(Debug, Clone)]
pub struct CreateOp {
    pub var: Option<String>,
    pub table: ResolvedTable,
    pub props: Vec<(String, Expr)>,
}

/// A projection in an aggregate plan.
#[derive(Debug, Clone)]
pub enum AggregateProjection {
    /// A group key (references a column by index in the inner result).
    GroupKey(usize),
    /// An aggregate function (name + argument expression).
    AggregateFunc { name: String, arg: Expr },
}

/// Physical execution plan.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
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
    /// Aggregate results from an inner plan.
    Aggregate {
        /// Inner plan that produces rows to aggregate.
        inner: Box<PhysicalPlan>,
        /// Indices of return items that are group keys (not aggregates).
        group_keys: Vec<usize>,
        /// For each return item, either a group key index or an aggregate function name + arg expr.
        projections: Vec<AggregateProjection>,
        /// Return clause for column names.
        return_clause: ReturnClause,
    },
    /// Create nodes and/or relationships.
    CreateNodes {
        /// Patterns describing nodes/rels to create, with their resolved tables.
        creates: Vec<CreateOp>,
    },
    /// Update properties on matched nodes/rels.
    SetProperties {
        /// First: expand to find matching vertices.
        expand: Box<PhysicalPlan>,
        /// Then: apply these assignments.
        assignments: Vec<(String, String, Expr)>, // (var, property, value)
    },
    /// Delete matched nodes/rels.
    DeleteNodes {
        /// First: expand to find matching vertices.
        expand: Box<PhysicalPlan>,
        /// Variables to delete.
        variables: Vec<String>,
        /// Whether to detach (delete relationships too).
        detach: bool,
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
        Statement::Create { patterns } => plan_create(patterns, &logical.bindings),
        Statement::Set {
            pattern,
            where_clause,
            assignments,
        } => plan_set(pattern, &logical.bindings, where_clause, assignments),
        Statement::Delete {
            pattern,
            where_clause,
            detach,
            variables,
        } => plan_delete(pattern, &logical.bindings, where_clause, variables, *detach),
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

/// Plan a CREATE statement: for each pattern element, look up the binding and
/// create a `CreateOp` with the pattern's properties.
fn plan_create(
    patterns: &[Pattern],
    bindings: &std::collections::HashMap<String, ResolvedTable>,
) -> Result<PhysicalPlan> {
    let mut creates = Vec::new();
    collect_create_ops(patterns, bindings, &mut creates)?;

    if creates.is_empty() {
        return Err(GraphError::Validation(
            "CREATE requires at least one labeled node or relationship".to_string(),
        ));
    }

    Ok(PhysicalPlan::CreateNodes { creates })
}

/// Recursively collect `CreateOp` entries from patterns.
fn collect_create_ops(
    patterns: &[Pattern],
    bindings: &std::collections::HashMap<String, ResolvedTable>,
    creates: &mut Vec<CreateOp>,
) -> Result<()> {
    for pat in patterns {
        match pat {
            Pattern::Node {
                var, label, props, ..
            } => {
                // Need either a binding via the variable or the label to resolve a table.
                let resolved = if let Some(var_name) = var {
                    bindings.get(var_name).cloned()
                } else {
                    None
                };
                if let Some(table) = resolved {
                    creates.push(CreateOp {
                        var: var.clone(),
                        table,
                        props: props.clone(),
                    });
                } else if label.is_some() {
                    return Err(GraphError::Validation(format!(
                        "CREATE node with label '{}' has no resolved binding",
                        label.as_deref().unwrap_or("?")
                    )));
                }
                // Unlabeled nodes without bindings are silently skipped.
            }
            Pattern::Rel {
                var,
                rel_type,
                props,
                ..
            } => {
                let resolved = if let Some(var_name) = var {
                    bindings.get(var_name).cloned()
                } else {
                    // Try looking up by rel_type through bindings values.
                    rel_type.as_ref().and_then(|rt| {
                        bindings
                            .values()
                            .find(|r| r.label.eq_ignore_ascii_case(rt) && r.graph_type == "edge")
                            .cloned()
                    })
                };
                if let Some(table) = resolved {
                    creates.push(CreateOp {
                        var: var.clone(),
                        table,
                        props: props.clone(),
                    });
                } else if rel_type.is_some() {
                    return Err(GraphError::Validation(format!(
                        "CREATE relationship with type '{}' has no resolved binding",
                        rel_type.as_deref().unwrap_or("?")
                    )));
                }
            }
            Pattern::Path(elements) => {
                collect_create_ops(elements, bindings, creates)?;
            }
        }
    }
    Ok(())
}

/// Plan a SET statement: build an Expand plan from the pattern, then wrap it
/// with SetProperties containing the assignments.
fn plan_set(
    patterns: &[Pattern],
    bindings: &std::collections::HashMap<String, ResolvedTable>,
    where_clause: &Option<Expr>,
    assignments: &[Assignment],
) -> Result<PhysicalPlan> {
    let filters = extract_filters(where_clause);

    // Build a dummy return clause that returns all variables from the
    // assignments so that the expand plan can find matching vertices.
    let return_vars: Vec<String> = assignments.iter().map(|a| a.var.clone()).collect();
    let return_clause = ReturnClause {
        distinct: false,
        items: return_vars
            .iter()
            .map(|v| crate::parser::ReturnItem {
                expr: Expr::Var(v.clone()),
                alias: None,
            })
            .collect(),
        order_by: vec![],
        limit: None,
    };

    let expand = plan_match(patterns, bindings, filters, return_clause)?;
    let assignment_tuples: Vec<(String, String, Expr)> = assignments
        .iter()
        .map(|a| (a.var.clone(), a.property.clone(), a.value.clone()))
        .collect();

    Ok(PhysicalPlan::SetProperties {
        expand: Box::new(expand),
        assignments: assignment_tuples,
    })
}

/// Plan a DELETE statement: build an Expand plan from the pattern, then wrap
/// it with DeleteNodes containing the variables to delete.
fn plan_delete(
    patterns: &[Pattern],
    bindings: &std::collections::HashMap<String, ResolvedTable>,
    where_clause: &Option<Expr>,
    variables: &[String],
    detach: bool,
) -> Result<PhysicalPlan> {
    let filters = extract_filters(where_clause);

    let return_clause = ReturnClause {
        distinct: false,
        items: variables
            .iter()
            .map(|v| crate::parser::ReturnItem {
                expr: Expr::Var(v.clone()),
                alias: None,
            })
            .collect(),
        order_by: vec![],
        limit: None,
    };

    let expand = plan_match(patterns, bindings, filters, return_clause)?;

    Ok(PhysicalPlan::DeleteNodes {
        expand: Box::new(expand),
        variables: variables.to_vec(),
        detach,
    })
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
                props,
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
                    prop_filters: props.clone(),
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

    // Check if the return clause contains any aggregate functions.
    let has_aggregates = return_clause.items.iter().any(
        |item| matches!(&item.expr, Expr::Function { name, .. } if is_aggregate_function(name)),
    );

    if has_aggregates {
        // Build projections: classify each return item as group key or aggregate.
        let mut group_keys = Vec::new();
        let mut projections = Vec::new();
        // Build the inner return clause: flatten aggregate args and group keys
        // into plain expressions so the Expand can project raw values.
        let mut inner_items: Vec<crate::parser::ReturnItem> = Vec::new();
        let mut group_key_idx = 0usize;

        for item in &return_clause.items {
            match &item.expr {
                Expr::Function { name, args } if is_aggregate_function(name) => {
                    // Determine the argument expression. For count(*) with no args,
                    // use a Var("*") sentinel.
                    let arg = if args.is_empty() {
                        Expr::Var("*".to_string())
                    } else {
                        args[0].clone()
                    };
                    projections.push(AggregateProjection::AggregateFunc {
                        name: name.to_lowercase(),
                        arg: arg.clone(),
                    });
                    // Add the arg expression to the inner return clause so the
                    // Expand plan produces the raw column the accumulator needs.
                    inner_items.push(crate::parser::ReturnItem {
                        expr: arg,
                        alias: None,
                    });
                }
                _ => {
                    group_keys.push(inner_items.len());
                    projections.push(AggregateProjection::GroupKey(group_key_idx));
                    group_key_idx += 1;
                    inner_items.push(item.clone());
                }
            }
        }

        let inner_return_clause = ReturnClause {
            distinct: false,
            items: inner_items,
            order_by: vec![],
            limit: None,
        };

        let expand = PhysicalPlan::Expand {
            anchor,
            hops,
            return_clause: inner_return_clause,
        };

        Ok(PhysicalPlan::Aggregate {
            inner: Box::new(expand),
            group_keys,
            projections,
            return_clause,
        })
    } else {
        Ok(PhysicalPlan::Expand {
            anchor,
            hops,
            return_clause,
        })
    }
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
            _ => panic!("expected Expand plan"),
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
            _ => panic!("expected Expand plan"),
        }
    }

    #[test]
    fn plan_create_produces_create_nodes() {
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), person_table());

        let logical = LogicalPlan {
            bindings,
            statement: Statement::Create {
                patterns: vec![Pattern::Node {
                    var: Some("n".into()),
                    label: Some("Person".into()),
                    props: vec![(
                        "name".into(),
                        Expr::Literal(crate::parser::Literal::String("Alice".into())),
                    )],
                }],
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        match physical {
            PhysicalPlan::CreateNodes { creates } => {
                assert_eq!(creates.len(), 1);
                assert_eq!(creates[0].var, Some("n".to_string()));
                assert_eq!(creates[0].table.table, "person_v");
                assert_eq!(creates[0].props.len(), 1);
                assert_eq!(creates[0].props[0].0, "name");
            }
            other => panic!("expected CreateNodes, got {other:?}"),
        }
    }

    #[test]
    fn plan_create_empty_patterns_returns_error() {
        let logical = LogicalPlan {
            bindings: HashMap::new(),
            statement: Statement::Create { patterns: vec![] },
            keyspace: "social".to_string(),
        };

        let result = plan(logical);
        assert!(result.is_err());
    }

    #[test]
    fn plan_set_produces_set_properties() {
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), person_table());

        let logical = LogicalPlan {
            bindings,
            statement: Statement::Set {
                pattern: vec![Pattern::Node {
                    var: Some("n".into()),
                    label: Some("Person".into()),
                    props: vec![],
                }],
                where_clause: None,
                assignments: vec![crate::parser::Assignment {
                    var: "n".into(),
                    property: "name".into(),
                    value: Expr::Literal(crate::parser::Literal::String("Bob".into())),
                }],
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        match physical {
            PhysicalPlan::SetProperties {
                expand,
                assignments,
            } => {
                assert!(matches!(*expand, PhysicalPlan::Expand { .. }));
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].0, "n");
                assert_eq!(assignments[0].1, "name");
            }
            other => panic!("expected SetProperties, got {other:?}"),
        }
    }

    #[test]
    fn plan_delete_produces_delete_nodes() {
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), person_table());

        let logical = LogicalPlan {
            bindings,
            statement: Statement::Delete {
                pattern: vec![Pattern::Node {
                    var: Some("n".into()),
                    label: Some("Person".into()),
                    props: vec![],
                }],
                where_clause: None,
                detach: true,
                variables: vec!["n".into()],
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        match physical {
            PhysicalPlan::DeleteNodes {
                expand,
                variables,
                detach,
            } => {
                assert!(matches!(*expand, PhysicalPlan::Expand { .. }));
                assert_eq!(variables, vec!["n".to_string()]);
                assert!(detach);
            }
            other => panic!("expected DeleteNodes, got {other:?}"),
        }
    }

    #[test]
    fn plan_aggregate_count() {
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), person_table());

        let return_clause = ReturnClause {
            distinct: false,
            items: vec![ReturnItem {
                expr: Expr::Function {
                    name: "count".to_string(),
                    args: vec![Expr::Var("n".into())],
                },
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };

        let logical = LogicalPlan {
            bindings,
            statement: Statement::Match {
                pattern: vec![Pattern::Node {
                    var: Some("n".into()),
                    label: Some("Person".into()),
                    props: vec![],
                }],
                where_clause: None,
                return_clause,
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        match physical {
            PhysicalPlan::Aggregate {
                inner,
                group_keys,
                projections,
                ..
            } => {
                assert!(matches!(*inner, PhysicalPlan::Expand { .. }));
                assert!(group_keys.is_empty());
                assert_eq!(projections.len(), 1);
                assert!(matches!(
                    &projections[0],
                    AggregateProjection::AggregateFunc { name, .. } if name == "count"
                ));
            }
            other => panic!("expected Aggregate plan, got {other:?}"),
        }
    }

    #[test]
    fn plan_no_aggregate_returns_expand() {
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), person_table());

        let return_clause = ReturnClause {
            distinct: false,
            items: vec![ReturnItem {
                expr: Expr::Property {
                    var: "n".into(),
                    name: "name".into(),
                },
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };

        let logical = LogicalPlan {
            bindings,
            statement: Statement::Match {
                pattern: vec![Pattern::Node {
                    var: Some("n".into()),
                    label: Some("Person".into()),
                    props: vec![],
                }],
                where_clause: None,
                return_clause,
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        assert!(
            matches!(physical, PhysicalPlan::Expand { .. }),
            "expected Expand plan, got {physical:?}"
        );
    }

    #[test]
    fn plan_hop_with_props() {
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
                        props: vec![(
                            "since".into(),
                            Expr::Literal(crate::parser::Literal::Integer(2020)),
                        )],
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
            PhysicalPlan::Expand { hops, .. } => {
                assert_eq!(hops.len(), 1);
                assert_eq!(hops[0].prop_filters.len(), 1);
                assert_eq!(hops[0].prop_filters[0].0, "since");
                assert_eq!(
                    hops[0].prop_filters[0].1,
                    Expr::Literal(crate::parser::Literal::Integer(2020))
                );
            }
            _ => panic!("expected Expand plan"),
        }
    }

    #[test]
    fn plan_hop_without_props() {
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
            PhysicalPlan::Expand { hops, .. } => {
                assert_eq!(hops.len(), 1);
                assert!(hops[0].prop_filters.is_empty());
            }
            _ => panic!("expected Expand plan"),
        }
    }
}
