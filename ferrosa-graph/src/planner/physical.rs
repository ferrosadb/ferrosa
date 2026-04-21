//! Physical planner: Expand plan with anchor selection.
//!
//! Converts a `LogicalPlan` into a `PhysicalPlan` describing how to
//! execute the query against the storage engine.

use std::collections::HashMap;
use std::time::Duration;

use crate::error::{GraphError, Result};
use crate::executor::aggregate::is_aggregate_function;
use crate::parser::{Assignment, Direction, Expr, Pattern, PropMap, ReturnClause, Statement};
use crate::planner::logical::{LogicalPlan, ResolvedTable};

/// A single hop in a graph traversal.
#[derive(Debug, Clone)]
pub struct Hop {
    /// Variable binding for this hop's target node (if any).
    pub var: Option<String>,
    /// Variable binding for this hop's relationship (if any).
    pub rel_var: Option<String>,
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
    /// Property constraints from the destination node pattern.
    pub target_props: PropMap,
}

/// The anchor (starting point) of a graph traversal.
#[derive(Debug, Clone)]
pub struct Anchor {
    /// Variable binding for the anchor node.
    pub var: Option<String>,
    /// Resolved table for the anchor.
    pub table: ResolvedTable,
    /// Property constraints from the anchor node pattern.
    pub props: PropMap,
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

/// A single MERGE operation: read-before-write by content-addressed key.
///
/// `match_props` drives the deterministic partition key (blake3 hash of sorted
/// property bytes). `create_props` are applied only when no row was found.
///
/// For relationship MergeOps (`table.graph_type == "edge"`), `src_match_props`
/// and `dst_match_props` hold the source and destination node match properties
/// so that `execute_merge` can derive the correct partition and clustering keys:
///
/// - `partition key = content_addressed_key(src_match_props)`  — the source vertex key
/// - `clustering    = content_addressed_key(dst_match_props)`  — the destination vertex key
///
/// The adjacency observer reads `mutation.key` as the source vertex ID and
/// `row.clustering` as the target vertex ID.  Without these fields, hop queries
/// cannot find MERGE-created edges.
///
/// For node MergeOps (`table.graph_type == "vertex"`) both fields are `None`.
#[derive(Debug, Clone)]
pub struct MergeOp {
    pub var: Option<String>,
    pub table: ResolvedTable,
    /// Properties used to derive the edge's own content-addressed key (for
    /// idempotent read-before-write on edge properties).  For nodes this
    /// also provides the partition key.
    pub match_props: Vec<(String, Expr)>,
    /// Additional properties written only on the create arm.
    pub create_props: Vec<(String, Expr)>,
    /// For edge MergeOps: the source vertex's match properties, used to derive
    /// `src_key_bytes` (the SSTable partition key for the edge row).
    /// `None` for node MergeOps.
    pub src_match_props: Option<Vec<(String, Expr)>>,
    /// For edge MergeOps: the destination vertex's match properties, used to
    /// derive `dst_key_bytes` (the SSTable clustering key for the edge row).
    /// `None` for node MergeOps.
    pub dst_match_props: Option<Vec<(String, Expr)>>,
}

/// A projection in an aggregate plan.
#[derive(Debug, Clone)]
pub enum AggregateProjection {
    /// A group key (references a column by index in the inner result).
    GroupKey(usize),
    /// An aggregate function (name + argument expression).
    AggregateFunc { name: String, arg: Expr },
}

/// A single relation in a WCO join pattern.
#[derive(Debug, Clone)]
pub struct JoinRelation {
    /// Source variable.
    pub src_var: String,
    /// Target variable.
    pub dst_var: String,
    /// Direction of the edge traversal.
    pub direction: Direction,
    /// Edge label filter.
    pub edge_label: Option<String>,
    /// Resolved edge table.
    pub edge_table: Option<ResolvedTable>,
}

/// Worst-case optimal join plan using leapfrog triejoin.
#[derive(Debug, Clone)]
pub struct WcoJoinPlan {
    /// Variables involved in the join (elimination order).
    pub variables: Vec<String>,
    /// Relations to join (each produces a sorted iterator).
    pub relations: Vec<JoinRelation>,
    /// Resolved tables for variable bindings (for reading vertex data).
    pub var_tables: HashMap<String, ResolvedTable>,
}

/// Maximum number of hops for variable-length path traversal.
/// Prevents runaway BFS on unbounded `[*]` patterns (threat T13 mitigation).
pub const MAX_VAR_HOPS: u32 = 10;

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
        /// Optional RETURN clause for projecting the created nodes.
        return_clause: Option<ReturnClause>,
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
        /// Mapping from variable name to resolved table name so that
        /// tombstones are written to the correct storage table (e.g.
        /// "Person") rather than the Cypher variable name (e.g. "n").
        variable_tables: HashMap<String, String>,
    },
    /// Subscribe to periodic re-execution of a query.
    Subscribe {
        /// The inner MATCH plan to re-execute.
        inner: Box<PhysicalPlan>,
        /// Poll interval.
        interval: std::time::Duration,
        /// Whether to send only changes (delta mode).
        delta: bool,
        /// Return clause for column names.
        return_clause: ReturnClause,
    },
    /// Variable-length path expansion via BFS.
    ExpandVarLength {
        /// Starting vertex.
        anchor: Anchor,
        /// The relationship hop to repeat.
        hop: Hop,
        /// Minimum number of hops (inclusive).
        min_hops: u32,
        /// Maximum number of hops (inclusive). Capped at MAX_VAR_HOPS.
        max_hops: u32,
        /// Return clause.
        return_clause: ReturnClause,
    },
    /// Worst-case optimal multi-way join via leapfrog triejoin.
    WcoJoin {
        /// The join plan describing variables, relations, and resolved tables.
        plan: WcoJoinPlan,
        /// Return clause for projecting results.
        return_clause: ReturnClause,
    },
    /// MERGE: match-or-create with a content-addressed deterministic key.
    ///
    /// Executes in order: for each MergeOp, read the row; if absent, create it
    /// via the same write path as CREATE (preserving adjacency observer fires).
    /// After all merges, apply the `set_clause` assignments.
    MergeUpsert {
        /// Ordered list of MERGE operations (nodes then relationships).
        merges: Vec<MergeOp>,
        /// Trailing SET assignments: `(var, property, value_expr)`.
        set_clause: Vec<(String, String, Expr)>,
        /// Optional RETURN clause for projecting results.
        return_clause: Option<ReturnClause>,
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
        Statement::Create {
            patterns,
            return_clause,
        } => plan_create(patterns, &logical.bindings, return_clause.as_ref()),
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
        Statement::Subscribe {
            inner,
            interval,
            delta,
        } => plan_subscribe(inner, &logical.bindings, *interval, *delta),
        Statement::Unsubscribe { .. } => {
            // Unsubscribe doesn't need a physical plan; the engine handles it directly.
            Err(GraphError::Validation(
                "UNSUBSCRIBE is handled directly by the engine, not the planner".to_string(),
            ))
        }
        Statement::Merge {
            patterns,
            set_clause,
            return_clause,
        } => plan_merge(
            patterns,
            &logical.bindings,
            set_clause,
            return_clause.as_ref(),
        ),
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
    return_clause: Option<&ReturnClause>,
) -> Result<PhysicalPlan> {
    let mut creates = Vec::new();
    collect_create_ops(patterns, bindings, &mut creates)?;

    if creates.is_empty() {
        return Err(GraphError::Validation(
            "CREATE requires at least one labeled node or relationship".to_string(),
        ));
    }

    Ok(PhysicalPlan::CreateNodes {
        creates,
        return_clause: return_clause.cloned(),
    })
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

/// Plan a MERGE statement: for each pattern, collect match-props and build a
/// `MergeOp`. Operations are emitted in the order declared so that relationship
/// MERGEs see bindings introduced by earlier node MERGEs (R2).
fn plan_merge(
    patterns: &[crate::parser::Pattern],
    bindings: &std::collections::HashMap<String, ResolvedTable>,
    set_clause: &[crate::parser::Assignment],
    return_clause: Option<&ReturnClause>,
) -> Result<PhysicalPlan> {
    let mut merges = Vec::new();
    let mut merge_props_by_var = HashMap::new();
    collect_merge_ops(patterns, bindings, &mut merge_props_by_var, &mut merges)?;

    if merges.is_empty() {
        return Err(GraphError::Validation(
            "MERGE requires at least one labeled node or relationship".to_string(),
        ));
    }

    let set_clause_tuples: Vec<(String, String, Expr)> = set_clause
        .iter()
        .map(|a| (a.var.clone(), a.property.clone(), a.value.clone()))
        .collect();

    Ok(PhysicalPlan::MergeUpsert {
        merges,
        set_clause: set_clause_tuples,
        return_clause: return_clause.cloned(),
    })
}

/// Recursively collect `MergeOp` entries from patterns.
///
/// For `Pattern::Path` elements, this function detects `[Node, Rel, Node]`
/// triplets and sets `dst_match_props` on the Rel MergeOp so that
/// `execute_merge` can derive the destination vertex partition key bytes and
/// use them as the SSTable clustering key.  Without this, hop queries are
/// blind to MERGE-created edges (the adjacency observer reads `row.clustering`
/// as the target vertex ID).
fn collect_merge_ops(
    patterns: &[crate::parser::Pattern],
    bindings: &std::collections::HashMap<String, ResolvedTable>,
    merge_props_by_var: &mut HashMap<String, Vec<(String, crate::parser::Expr)>>,
    merges: &mut Vec<MergeOp>,
) -> Result<()> {
    for pat in patterns {
        match pat {
            crate::parser::Pattern::Node { var, label, props } => {
                let resolved = if let Some(var_name) = var {
                    bindings.get(var_name).cloned()
                } else {
                    None
                };
                if let Some(table) = resolved {
                    if !props.is_empty() {
                        if let Some(var_name) = var.as_ref() {
                            merge_props_by_var.insert(var_name.clone(), props.clone());
                        }
                    }
                    merges.push(MergeOp {
                        var: var.clone(),
                        table,
                        match_props: props.clone(),
                        create_props: vec![],
                        src_match_props: None,
                        dst_match_props: None,
                    });
                } else if label.is_some() {
                    return Err(GraphError::Validation(format!(
                        "MERGE node with label '{}' has no resolved binding",
                        label.as_deref().unwrap_or("?")
                    )));
                }
            }
            crate::parser::Pattern::Rel {
                var,
                rel_type,
                props,
                ..
            } => {
                let resolved = if let Some(var_name) = var {
                    bindings.get(var_name).cloned()
                } else {
                    rel_type.as_ref().and_then(|rt| {
                        bindings
                            .values()
                            .find(|r| r.label.eq_ignore_ascii_case(rt) && r.graph_type == "edge")
                            .cloned()
                    })
                };
                if let Some(table) = resolved {
                    // Bare Rel outside a Path: no src/dst context available.
                    // This is unusual (MERGE normally uses Path), but handle
                    // it gracefully by emitting without src/dst_match_props — the
                    // executor will log ERROR and fail rather than silently
                    // using empty clustering.
                    merges.push(MergeOp {
                        var: var.clone(),
                        table,
                        match_props: props.clone(),
                        create_props: vec![],
                        src_match_props: None,
                        dst_match_props: None,
                    });
                } else if rel_type.is_some() {
                    return Err(GraphError::Validation(format!(
                        "MERGE relationship with type '{}' has no resolved binding",
                        rel_type.as_deref().unwrap_or("?")
                    )));
                }
            }
            crate::parser::Pattern::Path(elements) => {
                collect_merge_ops_from_path(elements, bindings, merge_props_by_var, merges)?;
            }
        }
    }
    Ok(())
}

/// Collect `MergeOp`s from a path, threading `dst_match_props` from the
/// destination node into any relationship MergeOp that sits between two nodes.
///
/// A path is a flat list: `[Node, Rel, Node, Rel, Node, ...]`.  For each
/// `[src_node, rel, dst_node]` triplet, the rel MergeOp receives
/// `dst_match_props = dst_node.props`.
fn collect_merge_ops_from_path(
    elements: &[crate::parser::Pattern],
    bindings: &std::collections::HashMap<String, ResolvedTable>,
    merge_props_by_var: &mut HashMap<String, Vec<(String, crate::parser::Expr)>>,
    merges: &mut Vec<MergeOp>,
) -> Result<()> {
    let mut i = 0;
    while i < elements.len() {
        match &elements[i] {
            crate::parser::Pattern::Node { var, label, props } => {
                // Emit a node MergeOp only on the first pass (i == 0) or when
                // this node is NOT immediately preceded by a Rel we already
                // handled (even indices in a well-formed path are always nodes).
                let resolved = if let Some(var_name) = var {
                    bindings.get(var_name).cloned()
                } else {
                    None
                };
                if let Some(table) = resolved {
                    if !props.is_empty() {
                        if let Some(var_name) = var.as_ref() {
                            merge_props_by_var.insert(var_name.clone(), props.clone());
                        }
                    }
                    merges.push(MergeOp {
                        var: var.clone(),
                        table,
                        match_props: props.clone(),
                        create_props: vec![],
                        src_match_props: None,
                        dst_match_props: None,
                    });
                } else if label.is_some() {
                    return Err(GraphError::Validation(format!(
                        "MERGE node with label '{}' has no resolved binding",
                        label.as_deref().unwrap_or("?")
                    )));
                }
                i += 1;
            }
            crate::parser::Pattern::Rel {
                var,
                rel_type,
                props,
                ..
            } => {
                // Resolve the edge table.
                let resolved = if let Some(var_name) = var {
                    bindings.get(var_name).cloned()
                } else {
                    rel_type.as_ref().and_then(|rt| {
                        bindings
                            .values()
                            .find(|r| r.label.eq_ignore_ascii_case(rt) && r.graph_type == "edge")
                            .cloned()
                    })
                };

                // Peek at the source node (i-1) for src_match_props.
                let src_match_props: Option<Vec<(String, crate::parser::Expr)>> = if i > 0 {
                    if let crate::parser::Pattern::Node {
                        var: src_var,
                        props: src_props,
                        ..
                    } = &elements[i - 1]
                    {
                        if src_props.is_empty() {
                            src_var
                                .as_ref()
                                .and_then(|name| merge_props_by_var.get(name).cloned())
                        } else {
                            Some(src_props.clone())
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Peek at the destination node (i+1) for dst_match_props.
                let dst_match_props: Option<Vec<(String, crate::parser::Expr)>> =
                    if i + 1 < elements.len() {
                        if let crate::parser::Pattern::Node {
                            var: dst_var,
                            props: dst_props,
                            ..
                        } = &elements[i + 1]
                        {
                            if dst_props.is_empty() {
                                dst_var
                                    .as_ref()
                                    .and_then(|name| merge_props_by_var.get(name).cloned())
                            } else {
                                Some(dst_props.clone())
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                if let Some(table) = resolved {
                    merges.push(MergeOp {
                        var: var.clone(),
                        table,
                        match_props: props.clone(),
                        create_props: vec![],
                        src_match_props,
                        dst_match_props,
                    });
                } else if rel_type.is_some() {
                    return Err(GraphError::Validation(format!(
                        "MERGE relationship with type '{}' has no resolved binding",
                        rel_type.as_deref().unwrap_or("?")
                    )));
                }

                // Skip rel only — the dst node is consumed on the next iteration.
                i += 1;
            }
            crate::parser::Pattern::Path(inner) => {
                collect_merge_ops_from_path(inner, bindings, merge_props_by_var, merges)?;
                i += 1;
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

    // Build variable-to-table mapping so the executor writes tombstones
    // to the correct storage table (label name, not Cypher variable name).
    let variable_tables: HashMap<String, String> = variables
        .iter()
        .filter_map(|v| bindings.get(v).map(|rt| (v.clone(), rt.table.clone())))
        .collect();

    Ok(PhysicalPlan::DeleteNodes {
        expand: Box::new(expand),
        variables: variables.to_vec(),
        detach,
        variable_tables,
    })
}

/// Default subscription poll interval (2 seconds).
const DEFAULT_SUBSCRIBE_INTERVAL: Duration = Duration::from_secs(2);

/// Plan a SUBSCRIBE statement: extract the inner MATCH, plan it as an Expand,
/// and wrap in `PhysicalPlan::Subscribe`.
fn plan_subscribe(
    inner: &Statement,
    bindings: &std::collections::HashMap<String, ResolvedTable>,
    interval: Option<Duration>,
    delta: bool,
) -> Result<PhysicalPlan> {
    match inner {
        Statement::Match {
            pattern,
            where_clause,
            return_clause,
        } => {
            let filters = extract_filters(where_clause);
            let inner_plan = plan_match(pattern, bindings, filters, return_clause.clone())?;
            Ok(PhysicalPlan::Subscribe {
                inner: Box::new(inner_plan),
                interval: interval.unwrap_or(DEFAULT_SUBSCRIBE_INTERVAL),
                delta,
                return_clause: return_clause.clone(),
            })
        }
        _ => Err(GraphError::Validation(
            "SUBSCRIBE requires a MATCH query as its inner statement".to_string(),
        )),
    }
}

/// Detect whether a pattern forms a cycle suitable for WCO join.
///
/// Returns `Some(WcoJoinPlan)` if the pattern is cyclic with 3+ relationships
/// (i.e., some variable appears as both source and target across different rels).
/// Returns `None` for linear patterns, which should use Expand.
fn detect_cyclic_pattern(
    elements: &[&Pattern],
    bindings: &HashMap<String, ResolvedTable>,
) -> Option<WcoJoinPlan> {
    // Extract (src_var, rel, dst_var) triples from the path.
    let mut relations = Vec::new();
    let mut all_vars = Vec::new();
    let mut var_counts: HashMap<String, usize> = HashMap::new();

    let mut i = 0;
    while i < elements.len() {
        match &elements[i] {
            Pattern::Node { var: Some(v), .. } => {
                if i == 0 || i == elements.len() - 1 {
                    *var_counts.entry(v.clone()).or_insert(0) += 1;
                }
                i += 1;
            }
            Pattern::Node { var: None, .. } => {
                i += 1;
            }
            Pattern::Rel {
                rel_type,
                direction,
                length_range,
                ..
            } => {
                // Variable-length paths cannot use WCO join.
                if length_range.is_some() {
                    return None;
                }

                // Get source node (previous element) and target node (next element).
                let src_var = if i > 0 {
                    match &elements[i - 1] {
                        Pattern::Node { var: Some(v), .. } => v.clone(),
                        _ => return None,
                    }
                } else {
                    return None;
                };

                let dst_var = if i + 1 < elements.len() {
                    match &elements[i + 1] {
                        Pattern::Node { var: Some(v), .. } => v.clone(),
                        _ => return None,
                    }
                } else {
                    return None;
                };

                let edge_label = rel_type.clone();
                let edge_table = rel_type.as_ref().and_then(|rt| {
                    bindings
                        .values()
                        .find(|r| r.label.eq_ignore_ascii_case(rt) && r.graph_type == "edge")
                        .cloned()
                });

                relations.push(JoinRelation {
                    src_var: src_var.clone(),
                    dst_var: dst_var.clone(),
                    direction: *direction,
                    edge_label,
                    edge_table,
                });

                if !all_vars.contains(&src_var) {
                    all_vars.push(src_var);
                }
                if !all_vars.contains(&dst_var) {
                    all_vars.push(dst_var.clone());
                }
                *var_counts.entry(dst_var).or_insert(0) += 1;

                i += 1;
            }
            Pattern::Path(_) => return None,
        }
    }

    // A cycle requires 3+ relations and at least one variable appearing
    // more than once as an endpoint (e.g., `a` in `(a)->...->(a)`).
    if relations.len() < 3 {
        return None;
    }

    // Check for cycle: the first and last variables must be the same,
    // OR any variable must appear as both a source and target of different relations.
    let has_cycle = var_counts.values().any(|&count| count > 1) || {
        // Check if first node of pattern equals last node.
        let first_var = match elements.first() {
            Some(Pattern::Node { var: Some(v), .. }) => Some(v.as_str()),
            _ => None,
        };
        let last_var = match elements.last() {
            Some(Pattern::Node { var: Some(v), .. }) => Some(v.as_str()),
            _ => None,
        };
        first_var.is_some() && first_var == last_var
    };

    if !has_cycle {
        return None;
    }

    // Build var_tables from bindings.
    let mut var_tables = HashMap::new();
    for var in &all_vars {
        if let Some(resolved) = bindings.get(var) {
            var_tables.insert(var.clone(), resolved.clone());
        }
    }

    Some(WcoJoinPlan {
        variables: all_vars,
        relations,
        var_tables,
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

    // Check for cyclic patterns suitable for WCO join before building Expand.
    if let Some(wco_plan) = detect_cyclic_pattern(&elements, bindings) {
        return Ok(PhysicalPlan::WcoJoin {
            plan: wco_plan,
            return_clause,
        });
    }

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
                            let props = match &elements[i] {
                                Pattern::Node { props, .. } => props.clone(),
                                _ => vec![],
                            };
                            anchor = Some(Anchor {
                                var: var.clone(),
                                table: resolved.clone(),
                                props,
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
                                let props = match &elements[i] {
                                    Pattern::Node { props, .. } => props.clone(),
                                    _ => vec![],
                                };
                                anchor = Some(Anchor {
                                    var: var.clone(),
                                    table: resolved.clone(),
                                    props,
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
                var,
                rel_type,
                direction,
                props,
                length_range,
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
                let (next_var, vertex_table, target_props) = if i + 1 < elements.len() {
                    if let Pattern::Node { var, props, .. } = &elements[i + 1] {
                        let vt = var.as_ref().and_then(|v| bindings.get(v)).cloned();
                        (var.clone(), vt, props.clone())
                    } else {
                        (None, None, vec![])
                    }
                } else {
                    (None, None, vec![])
                };

                // If this is a variable-length path, we handle it specially
                // by emitting an ExpandVarLength plan instead of adding a hop.
                if let Some((min, max_opt)) = length_range {
                    let hop = Hop {
                        var: next_var,
                        rel_var: var.clone(),
                        edge_label,
                        direction: *direction,
                        edge_table,
                        vertex_table,
                        prop_filters: props.clone(),
                        target_props,
                    };

                    let clamped_max = max_opt.map(|m| m.min(MAX_VAR_HOPS)).unwrap_or(MAX_VAR_HOPS);

                    let anchor_val = anchor.clone().ok_or_else(|| {
                        GraphError::Validation("no anchor found for var-length path".to_string())
                    })?;

                    // Variable-length paths consume the rest of the pattern
                    // and return immediately as an ExpandVarLength plan.
                    return Ok(PhysicalPlan::ExpandVarLength {
                        anchor: anchor_val,
                        hop,
                        min_hops: *min,
                        max_hops: clamped_max,
                        return_clause,
                    });
                }

                hops.push(Hop {
                    var: next_var,
                    rel_var: var.clone(),
                    edge_label,
                    direction: *direction,
                    edge_table,
                    vertex_table,
                    prop_filters: props.clone(),
                    target_props,
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
                        length_range: None,
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
                assert_eq!(hops[0].rel_var, Some("r".to_string()));
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
                return_clause: None,
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        match physical {
            PhysicalPlan::CreateNodes { creates, .. } => {
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
            statement: Statement::Create {
                patterns: vec![],
                return_clause: None,
            },
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
                variable_tables,
            } => {
                assert!(matches!(*expand, PhysicalPlan::Expand { .. }));
                assert_eq!(variables, vec!["n".to_string()]);
                assert!(detach);
                assert_eq!(
                    variable_tables.get("n").map(String::as_str),
                    Some("person_v"),
                    "variable_tables must map variable 'n' to storage table 'person_v'"
                );
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
                        length_range: None,
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
                        length_range: None,
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

    #[test]
    fn plan_subscribe_match() {
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), person_table());

        let inner = Statement::Match {
            pattern: vec![Pattern::Node {
                var: Some("n".into()),
                label: Some("Person".into()),
                props: vec![],
            }],
            where_clause: None,
            return_clause: simple_return(),
        };

        let logical = LogicalPlan {
            bindings,
            statement: Statement::Subscribe {
                inner: Box::new(inner),
                interval: Some(std::time::Duration::from_secs(5)),
                delta: true,
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        match physical {
            PhysicalPlan::Subscribe {
                inner,
                interval,
                delta,
                return_clause,
            } => {
                assert!(matches!(*inner, PhysicalPlan::Expand { .. }));
                assert_eq!(interval, std::time::Duration::from_secs(5));
                assert!(delta);
                assert_eq!(return_clause.items.len(), 1);
            }
            other => panic!("expected Subscribe plan, got {other:?}"),
        }
    }

    #[test]
    fn plan_subscribe_default_interval() {
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), person_table());

        let inner = Statement::Match {
            pattern: vec![Pattern::Node {
                var: Some("n".into()),
                label: Some("Person".into()),
                props: vec![],
            }],
            where_clause: None,
            return_clause: simple_return(),
        };

        let logical = LogicalPlan {
            bindings,
            statement: Statement::Subscribe {
                inner: Box::new(inner),
                interval: None,
                delta: false,
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        match physical {
            PhysicalPlan::Subscribe {
                interval, delta, ..
            } => {
                // Default is 2 seconds.
                assert_eq!(interval, std::time::Duration::from_secs(2));
                assert!(!delta);
            }
            other => panic!("expected Subscribe plan, got {other:?}"),
        }
    }

    #[test]
    fn plan_varpath() {
        let mut bindings = HashMap::new();
        bindings.insert("a".to_string(), person_table());
        bindings.insert("b".to_string(), person_table());

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
                        var: None,
                        rel_type: Some("KNOWS".into()),
                        direction: Direction::Out,
                        props: vec![],
                        length_range: Some((1, Some(5))),
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
            PhysicalPlan::ExpandVarLength {
                anchor,
                hop,
                min_hops,
                max_hops,
                ..
            } => {
                assert_eq!(anchor.var, Some("a".to_string()));
                assert_eq!(hop.edge_label, Some("KNOWS".to_string()));
                assert_eq!(min_hops, 1);
                assert_eq!(max_hops, 5);
            }
            other => panic!("expected ExpandVarLength plan, got {other:?}"),
        }
    }

    #[test]
    fn plan_varpath_max_hops_capped() {
        let mut bindings = HashMap::new();
        bindings.insert("a".to_string(), person_table());
        bindings.insert("b".to_string(), person_table());

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
                        var: None,
                        rel_type: Some("KNOWS".into()),
                        direction: Direction::Out,
                        props: vec![],
                        length_range: Some((1, None)), // unbounded
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
            PhysicalPlan::ExpandVarLength {
                min_hops, max_hops, ..
            } => {
                assert_eq!(min_hops, 1);
                assert_eq!(max_hops, MAX_VAR_HOPS);
            }
            other => panic!("expected ExpandVarLength plan, got {other:?}"),
        }
    }

    #[test]
    fn plan_cycle_uses_wco_join() {
        // Triangle: (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(a)
        let mut bindings = HashMap::new();
        bindings.insert("a".to_string(), person_table());
        bindings.insert("b".to_string(), person_table());
        bindings.insert("c".to_string(), person_table());
        bindings.insert("r1".to_string(), knows_table());
        bindings.insert("r2".to_string(), knows_table());
        bindings.insert("r3".to_string(), knows_table());

        let return_clause = ReturnClause {
            distinct: false,
            items: vec![
                ReturnItem {
                    expr: Expr::Var("a".into()),
                    alias: None,
                },
                ReturnItem {
                    expr: Expr::Var("b".into()),
                    alias: None,
                },
                ReturnItem {
                    expr: Expr::Var("c".into()),
                    alias: None,
                },
            ],
            order_by: vec![],
            limit: None,
        };

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
                        var: Some("r1".into()),
                        rel_type: Some("KNOWS".into()),
                        direction: Direction::Out,
                        props: vec![],
                        length_range: None,
                    },
                    Pattern::Node {
                        var: Some("b".into()),
                        label: Some("Person".into()),
                        props: vec![],
                    },
                    Pattern::Rel {
                        var: Some("r2".into()),
                        rel_type: Some("KNOWS".into()),
                        direction: Direction::Out,
                        props: vec![],
                        length_range: None,
                    },
                    Pattern::Node {
                        var: Some("c".into()),
                        label: Some("Person".into()),
                        props: vec![],
                    },
                    Pattern::Rel {
                        var: Some("r3".into()),
                        rel_type: Some("KNOWS".into()),
                        direction: Direction::Out,
                        props: vec![],
                        length_range: None,
                    },
                    Pattern::Node {
                        var: Some("a".into()),
                        label: Some("Person".into()),
                        props: vec![],
                    },
                ])],
                where_clause: None,
                return_clause,
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        match physical {
            PhysicalPlan::WcoJoin { plan, .. } => {
                assert_eq!(plan.variables.len(), 3);
                assert!(plan.variables.contains(&"a".to_string()));
                assert!(plan.variables.contains(&"b".to_string()));
                assert!(plan.variables.contains(&"c".to_string()));
                assert_eq!(plan.relations.len(), 3);
            }
            other => panic!("expected WcoJoin plan, got {other:?}"),
        }
    }

    #[test]
    fn plan_linear_uses_expand() {
        // Linear: (a)-[:KNOWS]->(b)-[:KNOWS]->(c) — no cycle.
        let mut bindings = HashMap::new();
        bindings.insert("a".to_string(), person_table());
        bindings.insert("b".to_string(), person_table());
        bindings.insert("c".to_string(), person_table());
        bindings.insert("r1".to_string(), knows_table());
        bindings.insert("r2".to_string(), knows_table());

        let return_clause = ReturnClause {
            distinct: false,
            items: vec![ReturnItem {
                expr: Expr::Var("c".into()),
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };

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
                        var: Some("r1".into()),
                        rel_type: Some("KNOWS".into()),
                        direction: Direction::Out,
                        props: vec![],
                        length_range: None,
                    },
                    Pattern::Node {
                        var: Some("b".into()),
                        label: Some("Person".into()),
                        props: vec![],
                    },
                    Pattern::Rel {
                        var: Some("r2".into()),
                        rel_type: Some("KNOWS".into()),
                        direction: Direction::Out,
                        props: vec![],
                        length_range: None,
                    },
                    Pattern::Node {
                        var: Some("c".into()),
                        label: Some("Person".into()),
                        props: vec![],
                    },
                ])],
                where_clause: None,
                return_clause,
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        assert!(
            matches!(physical, PhysicalPlan::Expand { .. }),
            "expected Expand plan for linear pattern, got {physical:?}"
        );
    }

    #[test]
    fn plan_merge_produces_merge_upsert() {
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), person_table());

        let logical = LogicalPlan {
            bindings,
            statement: Statement::Merge {
                patterns: vec![crate::parser::Pattern::Node {
                    var: Some("n".into()),
                    label: Some("Person".into()),
                    props: vec![(
                        "name".into(),
                        Expr::Literal(crate::parser::Literal::String("Alice".into())),
                    )],
                }],
                set_clause: vec![],
                return_clause: None,
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        match physical {
            PhysicalPlan::MergeUpsert {
                merges, set_clause, ..
            } => {
                assert_eq!(merges.len(), 1, "expected 1 merge op");
                assert_eq!(merges[0].var, Some("n".to_string()));
                assert_eq!(merges[0].table.table, "person_v");
                assert_eq!(merges[0].match_props.len(), 1);
                assert_eq!(merges[0].match_props[0].0, "name");
                assert!(set_clause.is_empty());
            }
            other => panic!("expected MergeUpsert, got {other:?}"),
        }
    }

    #[test]
    fn plan_two_hop_cycle_uses_expand() {
        // Only 2 relationships — too few for WCO join, should use Expand.
        // (a)-[:KNOWS]->(b)-[:KNOWS]->(a)
        let mut bindings = HashMap::new();
        bindings.insert("a".to_string(), person_table());
        bindings.insert("b".to_string(), person_table());

        let return_clause = ReturnClause {
            distinct: false,
            items: vec![ReturnItem {
                expr: Expr::Var("a".into()),
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };

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
                        var: None,
                        rel_type: Some("KNOWS".into()),
                        direction: Direction::Out,
                        props: vec![],
                        length_range: None,
                    },
                    Pattern::Node {
                        var: Some("b".into()),
                        label: Some("Person".into()),
                        props: vec![],
                    },
                    Pattern::Rel {
                        var: None,
                        rel_type: Some("KNOWS".into()),
                        direction: Direction::Out,
                        props: vec![],
                        length_range: None,
                    },
                    Pattern::Node {
                        var: Some("a".into()),
                        label: Some("Person".into()),
                        props: vec![],
                    },
                ])],
                where_clause: None,
                return_clause,
            },
            keyspace: "social".to_string(),
        };

        let physical = plan(logical).unwrap();
        // 2 relationships is not enough for WCO join.
        assert!(
            matches!(physical, PhysicalPlan::Expand { .. }),
            "expected Expand plan for 2-hop cycle, got {physical:?}"
        );
    }
}
