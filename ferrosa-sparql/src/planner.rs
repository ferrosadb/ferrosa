//! SPARQL algebra → storage read plan translation.
//!
//! Converts `spargebra` algebra trees into a sequence of storage operations
//! against ferrosa's CQL-backed triple store.

use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;

use crate::error::SparqlError;

/// A planned storage operation for evaluating a triple pattern.
#[derive(Debug, Clone)]
pub enum TripleOp {
    /// Lookup by subject (partition key scan): given subject, scan all (pred, obj).
    SubjectLookup {
        graph: String,
        subject: String,
        predicate_filter: Option<String>,
    },
    /// Full scan with predicate filter (uses secondary index on predicate).
    PredicateScan { graph: String, predicate: String },
    /// Full scan with object filter (uses secondary index on object).
    ObjectScan { graph: String, object: String },
    /// Full table scan (no bound terms) — expensive, requires LIMIT.
    FullScan { graph: String },
}

/// A sort ordering for ORDER BY.
#[derive(Debug, Clone)]
pub struct OrderCondition {
    /// Variable name to sort on.
    pub variable: String,
    /// True for ascending, false for descending.
    pub ascending: bool,
}

/// A query execution plan.
#[derive(Debug)]
pub struct QueryPlan {
    /// Ordered list of triple pattern operations to execute.
    pub ops: Vec<(TriplePattern, TripleOp)>,
    /// Variables to project in the result.
    pub projection: Vec<String>,
    /// Optional LIMIT.
    pub limit: Option<usize>,
    /// Optional OFFSET.
    pub offset: Option<usize>,
    /// True when the original query is ASK (not SELECT).
    pub is_ask: bool,
    /// Whether DISTINCT was requested.
    pub distinct: bool,
    /// ORDER BY conditions (empty if none).
    pub order_by: Vec<OrderCondition>,
    /// Number of FILTER expressions that were parsed but not evaluated.
    pub unimplemented_filter_count: usize,
}

/// Plan a SPARQL SELECT or ASK query.
pub fn plan_query(query: &Query, default_graph: &str) -> Result<QueryPlan, SparqlError> {
    match query {
        Query::Select {
            pattern, base_iri, ..
        } => plan_select(
            pattern,
            default_graph,
            base_iri.as_ref().map(|i| i.as_str()),
            false,
        ),
        Query::Ask { pattern, .. } => {
            let mut plan = plan_select(pattern, default_graph, None, true)?;
            plan.limit = Some(1);
            Ok(plan)
        }
        _ => Err(SparqlError::Plan(
            "only SELECT and ASK queries are supported in Sprint 1".into(),
        )),
    }
}

fn plan_select(
    pattern: &GraphPattern,
    default_graph: &str,
    _base_iri: Option<&str>,
    is_ask: bool,
) -> Result<QueryPlan, SparqlError> {
    let mut ops = Vec::new();
    let mut projection = Vec::new();
    let mut limit = None;
    let mut offset = None;
    let mut distinct = false;
    let mut order_by = Vec::new();
    let mut unimplemented_filter_count: usize = 0;

    collect_ops(
        pattern,
        default_graph,
        &mut ops,
        &mut projection,
        &mut limit,
        &mut offset,
        &mut distinct,
        &mut order_by,
        &mut unimplemented_filter_count,
    )?;

    Ok(QueryPlan {
        ops,
        projection,
        limit,
        offset,
        is_ask,
        distinct,
        order_by,
        unimplemented_filter_count,
    })
}

#[allow(clippy::too_many_arguments)]
fn collect_ops(
    pattern: &GraphPattern,
    default_graph: &str,
    ops: &mut Vec<(TriplePattern, TripleOp)>,
    projection: &mut Vec<String>,
    limit: &mut Option<usize>,
    offset: &mut Option<usize>,
    distinct: &mut bool,
    order_by: &mut Vec<OrderCondition>,
    unimplemented_filter_count: &mut usize,
) -> Result<(), SparqlError> {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for tp in patterns {
                let op = plan_triple_pattern(tp, default_graph);
                ops.push((tp.clone(), op));
            }
        }
        GraphPattern::Project { inner, variables } => {
            for var in variables {
                projection.push(var.as_str().to_string());
            }
            collect_ops(
                inner,
                default_graph,
                ops,
                projection,
                limit,
                offset,
                distinct,
                order_by,
                unimplemented_filter_count,
            )?;
        }
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            if *start > 0 {
                *offset = Some(*start);
            }
            if let Some(len) = length {
                *limit = Some(*len);
            }
            collect_ops(
                inner,
                default_graph,
                ops,
                projection,
                limit,
                offset,
                distinct,
                order_by,
                unimplemented_filter_count,
            )?;
        }
        GraphPattern::Filter { inner, .. } => {
            *unimplemented_filter_count += 1;
            tracing::warn!(
                "FILTER expression encountered but not yet evaluated — \
                 results may include non-matching rows"
            );
            collect_ops(
                inner,
                default_graph,
                ops,
                projection,
                limit,
                offset,
                distinct,
                order_by,
                unimplemented_filter_count,
            )?;
        }
        GraphPattern::OrderBy {
            inner, expression, ..
        } => {
            for cond in expression {
                match cond {
                    spargebra::algebra::OrderExpression::Asc(
                        spargebra::algebra::Expression::Variable(v),
                    ) => {
                        order_by.push(OrderCondition {
                            variable: v.as_str().to_string(),
                            ascending: true,
                        });
                    }
                    spargebra::algebra::OrderExpression::Desc(
                        spargebra::algebra::Expression::Variable(v),
                    ) => {
                        order_by.push(OrderCondition {
                            variable: v.as_str().to_string(),
                            ascending: false,
                        });
                    }
                    _ => {
                        tracing::warn!(
                            "ORDER BY expression type not supported; \
                             only simple variable ordering is implemented"
                        );
                    }
                }
            }
            collect_ops(
                inner,
                default_graph,
                ops,
                projection,
                limit,
                offset,
                distinct,
                order_by,
                unimplemented_filter_count,
            )?;
        }
        GraphPattern::Distinct { inner } => {
            *distinct = true;
            collect_ops(
                inner,
                default_graph,
                ops,
                projection,
                limit,
                offset,
                distinct,
                order_by,
                unimplemented_filter_count,
            )?;
        }
        GraphPattern::Reduced { inner } => {
            collect_ops(
                inner,
                default_graph,
                ops,
                projection,
                limit,
                offset,
                distinct,
                order_by,
                unimplemented_filter_count,
            )?;
        }
        _ => {
            return Err(SparqlError::Plan(format!(
                "unsupported graph pattern: {pattern:?}"
            )));
        }
    }
    Ok(())
}

/// Choose a storage operation for a single triple pattern.
fn plan_triple_pattern(tp: &TriplePattern, default_graph: &str) -> TripleOp {
    let graph = default_graph.to_string();

    let subject_bound = matches!(&tp.subject, TermPattern::NamedNode(_));
    let predicate_bound = matches!(&tp.predicate, NamedNodePattern::NamedNode(_));
    let object_bound = matches!(
        &tp.object,
        TermPattern::NamedNode(_) | TermPattern::Literal(_)
    );

    if subject_bound {
        let subject = match &tp.subject {
            TermPattern::NamedNode(n) => n.as_str().to_string(),
            _ => unreachable!(),
        };
        let predicate_filter = if predicate_bound {
            match &tp.predicate {
                NamedNodePattern::NamedNode(n) => Some(n.as_str().to_string()),
                _ => None,
            }
        } else {
            None
        };
        TripleOp::SubjectLookup {
            graph,
            subject,
            predicate_filter,
        }
    } else if predicate_bound {
        let predicate = match &tp.predicate {
            NamedNodePattern::NamedNode(n) => n.as_str().to_string(),
            _ => unreachable!(),
        };
        TripleOp::PredicateScan { graph, predicate }
    } else if object_bound {
        let object = match &tp.object {
            TermPattern::NamedNode(n) => n.as_str().to_string(),
            TermPattern::Literal(l) => l.to_string(),
            _ => unreachable!(),
        };
        TripleOp::ObjectScan { graph, object }
    } else {
        TripleOp::FullScan { graph }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_simple_select() {
        let query = Query::parse("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", None).unwrap();
        let plan = plan_query(&query, "default").unwrap();
        assert_eq!(plan.projection, vec!["s", "p", "o"]);
        assert_eq!(plan.ops.len(), 1);
        assert!(matches!(plan.ops[0].1, TripleOp::FullScan { .. }));
    }

    #[test]
    fn plan_select_with_bound_subject() {
        let query = Query::parse(
            "SELECT ?p ?o WHERE { <http://example.org/alice> ?p ?o }",
            None,
        )
        .unwrap();
        let plan = plan_query(&query, "default").unwrap();
        assert_eq!(plan.ops.len(), 1);
        assert!(matches!(plan.ops[0].1, TripleOp::SubjectLookup { .. }));
    }

    #[test]
    fn plan_select_with_limit() {
        let query = Query::parse("SELECT ?s WHERE { ?s ?p ?o } LIMIT 10", None).unwrap();
        let plan = plan_query(&query, "default").unwrap();
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn plan_ask_sets_is_ask_flag() {
        let query = Query::parse(
            "ASK { <http://example.org/alice> <http://xmlns.com/foaf/0.1/name> ?name }",
            None,
        )
        .unwrap();
        let plan = plan_query(&query, "default").unwrap();
        assert!(plan.is_ask, "ASK query must set is_ask=true");
        assert_eq!(plan.limit, Some(1), "ASK query must limit to 1 row");
    }

    #[test]
    fn plan_select_not_ask() {
        let query = Query::parse("SELECT ?s WHERE { ?s ?p ?o }", None).unwrap();
        let plan = plan_query(&query, "default").unwrap();
        assert!(!plan.is_ask, "SELECT query must set is_ask=false");
    }

    #[test]
    fn plan_distinct_sets_flag() {
        let query = Query::parse("SELECT DISTINCT ?s WHERE { ?s ?p ?o }", None).unwrap();
        let plan = plan_query(&query, "default").unwrap();
        assert!(plan.distinct, "DISTINCT must set distinct=true");
    }

    #[test]
    fn plan_order_by_captures_conditions() {
        let query = Query::parse(
            "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name } ORDER BY ?name",
            None,
        )
        .unwrap();
        let plan = plan_query(&query, "default").unwrap();
        assert_eq!(plan.order_by.len(), 1);
        assert_eq!(plan.order_by[0].variable, "name");
        assert!(plan.order_by[0].ascending);
    }

    #[test]
    fn plan_order_by_desc() {
        let query = Query::parse(
            "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name } ORDER BY DESC(?name)",
            None,
        )
        .unwrap();
        let plan = plan_query(&query, "default").unwrap();
        assert_eq!(plan.order_by.len(), 1);
        assert!(!plan.order_by[0].ascending);
    }

    #[test]
    fn plan_filter_counts_unimplemented() {
        let query = Query::parse(
            "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . FILTER(?name = \"Alice\") }",
            None,
        )
        .unwrap();
        let plan = plan_query(&query, "default").unwrap();
        assert_eq!(
            plan.unimplemented_filter_count, 1,
            "FILTER must increment unimplemented_filter_count"
        );
    }

    #[test]
    fn plan_uses_keyspace_as_graph() {
        let query = Query::parse(
            "SELECT ?p ?o WHERE { <http://example.org/alice> ?p ?o }",
            None,
        )
        .unwrap();
        let plan = plan_query(&query, "my_tenant").unwrap();
        match &plan.ops[0].1 {
            TripleOp::SubjectLookup { graph, .. } => {
                assert_eq!(graph, "my_tenant", "graph must match supplied keyspace");
            }
            other => panic!("expected SubjectLookup, got {other:?}"),
        }
    }
}
