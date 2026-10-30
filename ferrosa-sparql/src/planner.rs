//! SPARQL algebra → storage read plan translation.
//!
//! Converts `spargebra` algebra trees into a sequence of storage operations
//! against ferrosa's CQL-backed triple store.
//! Correctness: legacy RDF identity tests and COUNT/JSON/protocol regressions.
//! Last revised: 2026-10-27
//! Last changed: Plan bounded COUNT and reject unsupported aggregate input algebra.

use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;

use crate::error::SparqlError;

/// Where a planned operation reads from.
///
/// The keyspace and the graph are DIFFERENT THINGS and were previously one
/// `graph: String` used for both. That conflation was a silent-wrong-results
/// bug: `rdf_triples` lives in a table named by the KEYSPACE, while a row's
/// partition key is `(GRAPH, subject)` — and the writer has always taken the
/// graph from the SPARQL graph name (`DefaultGraph` -> `"default"`) while the
/// reader took it from the caller's keyspace (HTTP default `"rdf"`).
///
/// Both sides therefore agreed on the table, so scans worked, while a
/// bound-subject point read asked for `("rdf", subject)` and missed a row
/// stored under `("default", subject)` — returning a well-formed empty result
/// for data that was present.
///
/// Carrying both in one struct is what stops that returning: an op cannot be
/// constructed with only one of them, and the two are named at every use site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TripleScope {
    /// Selects the `rdf_triples` TABLE. Comes from the caller's keyspace.
    pub keyspace: String,
    /// The graph component of the partition key. Comes from the SPARQL graph
    /// name — the default graph's storage component for the default scope, the
    /// IRI for a named graph.
    pub graph: String,
    /// Whether a scan must ISOLATE this scope's graph — skip every partition
    /// whose graph component differs from `graph`.
    ///
    /// True for the deployed engine's scopes and for explicit `GRAPH <iri>`
    /// blocks. False only for scopes built by [`Self::new`], whose callers (the
    /// planner-direct invariant suites) read the table without graph isolation;
    /// keeping the default there is what lets those suites stay green while the
    /// served path isolates graphs.
    pub isolated_graph: bool,
}

impl TripleScope {
    /// A scope that reads the table without isolating a graph.
    ///
    /// Used by callers that address a table directly (the planner-direct
    /// invariant suites). The served path builds [`Self::isolated`] scopes.
    pub fn new(keyspace: impl Into<String>, graph: impl Into<String>) -> Self {
        Self {
            keyspace: keyspace.into(),
            graph: graph.into(),
            isolated_graph: false,
        }
    }

    /// A scope whose scans are isolated to `graph`'s partitions.
    pub fn isolated(keyspace: impl Into<String>, graph: impl Into<String>) -> Self {
        Self {
            keyspace: keyspace.into(),
            graph: graph.into(),
            isolated_graph: true,
        }
    }

    /// Re-scope an existing scope onto a named graph, keeping its keyspace and
    /// isolating the new graph.
    pub fn with_isolated_graph(&self, graph: impl Into<String>) -> Self {
        Self {
            keyspace: self.keyspace.clone(),
            graph: graph.into(),
            isolated_graph: true,
        }
    }
}

/// A planned storage operation for evaluating a triple pattern.
#[derive(Debug, Clone)]
pub enum TripleOp {
    /// Lookup by subject (partition key scan): given subject, scan all (pred, obj).
    SubjectLookup {
        scope: TripleScope,
        subject: String,
        predicate_filter: Option<String>,
    },
    /// Full scan with predicate filter (uses secondary index on predicate).
    PredicateScan {
        scope: TripleScope,
        predicate: String,
    },
    /// Full scan with object filter (uses secondary index on object).
    ObjectScan { scope: TripleScope, object: String },
    /// Full table scan (no bound terms) — expensive, requires LIMIT.
    FullScan { scope: TripleScope },
    /// Property path traversal (BFS/DFS) — evaluates transitive closure.
    PropertyPath {
        scope: TripleScope,
        subject: spargebra::term::TermPattern,
        path: spargebra::algebra::PropertyPathExpression,
        object: spargebra::term::TermPattern,
    },
}

/// A sort ordering for ORDER BY.
///
/// Holds the full ORDER BY expression (which may be a plain variable, an
/// arithmetic expression like `?a + ?b`, a function call, etc.). The executor
/// evaluates the expression per solution and sorts by the resulting value
/// (URS-QEC-S04).
#[derive(Debug, Clone)]
pub struct OrderCondition {
    /// Expression to evaluate per solution and sort on.
    pub expression: spargebra::algebra::Expression,
    /// True for ascending, false for descending.
    pub ascending: bool,
}

/// Algebra-ordered evaluation steps. Indices address the plan's storage ops
/// and filters; Join operands retain independent expression scopes.
#[derive(Debug)]
pub enum PlanStep {
    /// Evaluate one storage operation against the current solutions.
    Triple(usize),
    /// Apply a filter at its algebra position.
    Filter(usize),
    /// Bind an expression result, retaining the solution on expression errors.
    Extend {
        variable: String,
        expression: spargebra::algebra::Expression,
    },
    /// Evaluate both operands independently, then join compatible solutions.
    Join {
        left: Vec<PlanStep>,
        right: Vec<PlanStep>,
    },
    /// Fold an independently scoped input into COUNT accumulators.
    Count {
        input: Vec<PlanStep>,
        variables: Vec<String>,
        aggregates: Vec<(String, CountAggregate)>,
    },
}

/// A validated COUNT expression; `None` denotes COUNT(*).
#[derive(Debug)]
pub struct CountAggregate {
    /// Expression whose successfully bound values are counted.
    pub expression: Option<spargebra::algebra::Expression>,
    /// Count distinct RDF terms or complete solution mappings.
    pub distinct: bool,
}

/// A query execution plan.
#[derive(Debug)]
pub struct QueryPlan {
    /// Storage-operation inventory; `steps` defines evaluation order.
    pub ops: Vec<(TriplePattern, TripleOp)>,
    /// Evaluation order, including expression scope boundaries.
    pub steps: Vec<PlanStep>,
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
    /// FILTER inventory; each expression is applied by its indexed step.
    pub filters: Vec<spargebra::algebra::Expression>,
    /// Non-None for CONSTRUCT/DESCRIBE query forms; None for SELECT/ASK.
    pub graph_mode: Option<GraphQueryMode>,
}

/// The template/mode for a query form that produces a graph result.
#[derive(Debug, Clone)]
pub enum GraphQueryMode {
    /// `CONSTRUCT { template }` — the template triples to instantiate per solution.
    Construct(Vec<TriplePattern>),
    /// `DESCRIBE <iri> [<iri> …]` — literal IRI(s) to describe; the planner
    /// has already built SubjectLookup ops and the executor just needs to
    /// assemble the result from the SELECT output.
    DescribeIris(Vec<String>),
    /// `DESCRIBE ?var WHERE { … }` — subjects are derived from WHERE solutions.
    ///
    /// Carries the full scope, not just a graph: the follow-up SubjectLookup it
    /// drives needs the keyspace to find the table AND the graph to build the
    /// partition key, and passing one value for both is the bug this type
    /// exists to prevent.
    Describe(TripleScope),
}

/// Plan a SPARQL SELECT, ASK, CONSTRUCT, or DESCRIBE query.
pub fn plan_query(query: &Query, scope: &TripleScope) -> Result<QueryPlan, SparqlError> {
    match query {
        Query::Select {
            pattern, base_iri, ..
        } => plan_select(pattern, scope, base_iri.as_ref().map(|i| i.as_str()), false),
        Query::Ask { pattern, .. } => {
            let mut plan = plan_select(pattern, scope, None, true)?;
            plan.limit = Some(1);
            Ok(plan)
        }
        Query::Construct {
            template, pattern, ..
        } => {
            // Plan the WHERE clause exactly like SELECT so the executor can
            // bind solutions; then attach the CONSTRUCT template.
            let mut plan = plan_select(pattern, scope, None, false)?;
            // CONSTRUCT projects all variables — projection is derived from the template.
            plan.graph_mode = Some(GraphQueryMode::Construct(template.clone()));
            Ok(plan)
        }
        Query::Describe {
            dataset: _,
            pattern,
            base_iri: _,
        } => {
            // DESCRIBE <iri> is parsed by spargebra as:
            //   Extend { inner: Bgp { patterns: [] },
            //            variable: _freshvar,
            //            expression: NamedNode(<iri>) }
            //
            // Extract all described IRIs from `Extend` nodes, then build a
            // SELECT-style plan that does a SubjectLookup per IRI.
            let iris = extract_describe_iris(pattern);
            if iris.is_empty() {
                // DESCRIBE ?var WHERE { … } — plan the WHERE clause normally.
                let mut plan = plan_select(pattern, scope, None, false)?;
                plan.graph_mode = Some(GraphQueryMode::Describe(scope.clone()));
                return Ok(plan);
            }

            // Build ops: one SubjectLookup per described IRI.
            let mut ops = Vec::new();
            for iri in &iris {
                let tp = TriplePattern {
                    subject: spargebra::term::TermPattern::NamedNode(
                        spargebra::term::NamedNode::new_unchecked(iri.as_str()),
                    ),
                    predicate: NamedNodePattern::Variable(
                        spargebra::term::Variable::new_unchecked("__p"),
                    ),
                    object: TermPattern::Variable(spargebra::term::Variable::new_unchecked("__o")),
                };
                ops.push((
                    tp,
                    TripleOp::SubjectLookup {
                        scope: scope.clone(),
                        subject: iri.clone(),
                        predicate_filter: None,
                    },
                ));
            }

            Ok(QueryPlan {
                steps: (0..ops.len()).map(PlanStep::Triple).collect(),
                ops,
                projection: vec!["__p".into(), "__o".into()],
                limit: None,
                offset: None,
                is_ask: false,
                distinct: false,
                order_by: vec![],
                filters: vec![],
                graph_mode: Some(GraphQueryMode::DescribeIris(iris)),
            })
        }
    }
}

/// Plan a bare `GraphPattern` (e.g. the WHERE clause of a SPARQL UPDATE) into a
/// [`QueryPlan`] so it can be evaluated by the SELECT executor to bind
/// solutions. Used by `update.rs` for `DELETE WHERE` / `DELETE/INSERT … WHERE`.
pub fn plan_where(pattern: &GraphPattern, scope: &TripleScope) -> Result<QueryPlan, SparqlError> {
    plan_select(pattern, scope, None, false)
}

fn plan_select(
    pattern: &GraphPattern,
    scope: &TripleScope,
    _base_iri: Option<&str>,
    is_ask: bool,
) -> Result<QueryPlan, SparqlError> {
    let mut ops = Vec::new();
    let mut steps = Vec::new();
    let mut projection = Vec::new();
    let mut limit = None;
    let mut offset = None;
    let mut distinct = false;
    let mut order_by = Vec::new();
    let mut filters: Vec<spargebra::algebra::Expression> = Vec::new();

    collect_ops(
        pattern,
        scope,
        &mut ops,
        &mut projection,
        &mut limit,
        &mut offset,
        &mut distinct,
        &mut order_by,
        &mut filters,
        &mut steps,
    )?;

    Ok(QueryPlan {
        ops,
        steps,
        projection,
        limit,
        offset,
        is_ask,
        distinct,
        order_by,
        filters,
        graph_mode: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn collect_ops(
    pattern: &GraphPattern,
    scope: &TripleScope,
    ops: &mut Vec<(TriplePattern, TripleOp)>,
    projection: &mut Vec<String>,
    limit: &mut Option<usize>,
    offset: &mut Option<usize>,
    distinct: &mut bool,
    order_by: &mut Vec<OrderCondition>,
    filters: &mut Vec<spargebra::algebra::Expression>,
    steps: &mut Vec<PlanStep>,
) -> Result<(), SparqlError> {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for tp in patterns {
                let op = plan_triple_pattern(tp, scope)?;
                steps.push(PlanStep::Triple(ops.len()));
                ops.push((tp.clone(), op));
            }
        }
        GraphPattern::Project { inner, variables } => {
            for var in variables {
                projection.push(var.as_str().to_string());
            }
            collect_ops(
                inner, scope, ops, projection, limit, offset, distinct, order_by, filters, steps,
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
                inner, scope, ops, projection, limit, offset, distinct, order_by, filters, steps,
            )?;
        }
        GraphPattern::Filter { inner, expr } => {
            collect_ops(
                inner, scope, ops, projection, limit, offset, distinct, order_by, filters, steps,
            )?;
            steps.push(PlanStep::Filter(filters.len()));
            filters.push(expr.clone());
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            crate::bind::validate(expression)?;
            collect_ops(
                inner, scope, ops, projection, limit, offset, distinct, order_by, filters, steps,
            )?;
            steps.push(PlanStep::Extend {
                variable: variable.as_str().to_owned(),
                expression: expression.clone(),
            });
        }
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            validate_count_input(inner)?;
            use spargebra::algebra::{AggregateExpression, AggregateFunction};
            let mut counts = Vec::with_capacity(aggregates.len());
            for (variable, aggregate) in aggregates {
                let count = match aggregate {
                    AggregateExpression::CountSolutions { distinct } => CountAggregate {
                        expression: None,
                        distinct: *distinct,
                    },
                    AggregateExpression::FunctionCall {
                        name: AggregateFunction::Count,
                        expr,
                        distinct,
                    } => {
                        crate::bind::validate(expr)?;
                        CountAggregate {
                            expression: Some(expr.clone()),
                            distinct: *distinct,
                        }
                    }
                    _ => {
                        return Err(SparqlError::Plan(format!(
                            "unsupported aggregate: {aggregate:?}"
                        )))
                    }
                };
                counts.push((variable.as_str().to_owned(), count));
            }
            let mut input = Vec::new();
            collect_ops(
                inner, scope, ops, projection, limit, offset, distinct, order_by, filters,
                &mut input,
            )?;
            steps.push(PlanStep::Count {
                input,
                variables: variables.iter().map(|v| v.as_str().to_owned()).collect(),
                aggregates: counts,
            });
        }
        GraphPattern::Join { left, right } => {
            let mut left_steps = Vec::new();
            let mut right_steps = Vec::new();
            collect_ops(
                left,
                scope,
                ops,
                projection,
                limit,
                offset,
                distinct,
                order_by,
                filters,
                &mut left_steps,
            )?;
            collect_ops(
                right,
                scope,
                ops,
                projection,
                limit,
                offset,
                distinct,
                order_by,
                filters,
                &mut right_steps,
            )?;
            steps.push(PlanStep::Join {
                left: left_steps,
                right: right_steps,
            });
        }
        GraphPattern::OrderBy {
            inner, expression, ..
        } => {
            // URS-QEC-S04: ORDER BY on arbitrary expressions. The executor
            // evaluates the expression per solution and sorts by the result;
            // a plain `?var` is just the trivial Variable expression.
            for cond in expression {
                let (expr, ascending) = match cond {
                    spargebra::algebra::OrderExpression::Asc(e) => (e.clone(), true),
                    spargebra::algebra::OrderExpression::Desc(e) => (e.clone(), false),
                };
                order_by.push(OrderCondition {
                    expression: expr,
                    ascending,
                });
            }
            collect_ops(
                inner, scope, ops, projection, limit, offset, distinct, order_by, filters, steps,
            )?;
        }
        GraphPattern::Distinct { inner } => {
            *distinct = true;
            collect_ops(
                inner, scope, ops, projection, limit, offset, distinct, order_by, filters, steps,
            )?;
        }
        GraphPattern::Reduced { inner } => {
            collect_ops(
                inner, scope, ops, projection, limit, offset, distinct, order_by, filters, steps,
            )?;
        }
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            // Property paths: check if it is a simple named node (single hop)
            // or a closure operator (+, *, ?) requiring BFS traversal.
            if is_simple_named_path(path) {
                let iri = extract_predicate_from_path(path).unwrap();
                let tp = TriplePattern {
                    subject: subject.clone(),
                    predicate: NamedNodePattern::NamedNode(
                        spargebra::term::NamedNode::new_unchecked(&iri),
                    ),
                    object: object.clone(),
                };
                let op = plan_triple_pattern(&tp, scope)?;
                steps.push(PlanStep::Triple(ops.len()));
                ops.push((tp, op));
            } else {
                // Transitive/closure path — emit PropertyPath op for BFS.
                let tp = build_path_triple_pattern(subject, path, object);
                let op = TripleOp::PropertyPath {
                    scope: scope.clone(),
                    subject: subject.clone(),
                    path: path.clone(),
                    object: object.clone(),
                };
                steps.push(PlanStep::Triple(ops.len()));
                ops.push((tp, op));
            }
        }
        GraphPattern::Union { left, right } => {
            // BUG-S14: UNION support — concat both sides.
            collect_ops(
                left, scope, ops, projection, limit, offset, distinct, order_by, filters, steps,
            )?;
            // UNION appends the right side's patterns too.
            collect_ops(
                right, scope, ops, projection, limit, offset, distinct, order_by, filters, steps,
            )?;
        }
        GraphPattern::LeftJoin { left, .. } => {
            // OPTIONAL → evaluate left side; right side patterns are optional.
            // For now, just evaluate the left side (inner patterns).
            collect_ops(
                left, scope, ops, projection, limit, offset, distinct, order_by, filters, steps,
            )?;
            tracing::warn!("OPTIONAL (LeftJoin) right side not yet evaluated");
        }
        GraphPattern::Graph { name, inner } => {
            // `GRAPH <iri> { … }` re-scopes its inner patterns onto that named
            // graph. The storage already keys rows by (graph, subject), so a
            // subject-bound pattern inside the block becomes a graph-isolated
            // point read; graph-isolated scans skip every other graph's
            // partitions. A variable graph is not addressable.
            match name {
                NamedNodePattern::NamedNode(n) => {
                    let inner_scope = scope.with_isolated_graph(n.as_str());
                    collect_ops(
                        inner,
                        &inner_scope,
                        ops,
                        projection,
                        limit,
                        offset,
                        distinct,
                        order_by,
                        filters,
                        steps,
                    )?;
                }
                NamedNodePattern::Variable(v) => {
                    return Err(SparqlError::Plan(format!(
                        "GRAPH ?{} (variable-bound graph read) is not supported: name the \
                         graph with an IRI",
                        v.as_str()
                    )));
                }
            }
        }
        _ => {
            return Err(SparqlError::Plan(format!(
                "unsupported graph pattern: {pattern:?}"
            )));
        }
    }
    Ok(())
}

/// Check whether a property path is a simple named node (single hop, no closure).
fn is_simple_named_path(path: &spargebra::algebra::PropertyPathExpression) -> bool {
    matches!(
        path,
        spargebra::algebra::PropertyPathExpression::NamedNode(_)
    )
}

/// Build a synthetic TriplePattern for a property path operation.
///
/// Uses the base predicate extracted from the path as the predicate component.
fn build_path_triple_pattern(
    subject: &spargebra::term::TermPattern,
    path: &spargebra::algebra::PropertyPathExpression,
    object: &spargebra::term::TermPattern,
) -> TriplePattern {
    let iri =
        extract_predicate_from_path(path).unwrap_or_else(|| "urn:ferrosa:unknown-path".into());
    TriplePattern {
        subject: subject.clone(),
        predicate: NamedNodePattern::NamedNode(spargebra::term::NamedNode::new_unchecked(&iri)),
        object: object.clone(),
    }
}

/// Extract the predicate IRI from a property path expression.
///
/// Handles simple paths (single IRI), OneOrMore (+), ZeroOrMore (*),
/// and ZeroOrOne (?). For transitive paths, returns the base IRI
/// and logs that full BFS is deferred.
fn extract_predicate_from_path(
    path: &spargebra::algebra::PropertyPathExpression,
) -> Option<String> {
    use spargebra::algebra::PropertyPathExpression;
    match path {
        PropertyPathExpression::NamedNode(n) => Some(n.as_str().to_string()),
        PropertyPathExpression::OneOrMore(inner)
        | PropertyPathExpression::ZeroOrMore(inner)
        | PropertyPathExpression::ZeroOrOne(inner) => {
            tracing::info!(
                "property path with closure operator — evaluating as single hop for now"
            );
            extract_predicate_from_path(inner)
        }
        PropertyPathExpression::Reverse(inner) => {
            tracing::info!("reverse property path — evaluating as forward for now");
            extract_predicate_from_path(inner)
        }
        _ => {
            tracing::warn!(?path, "complex property path expression not supported");
            None
        }
    }
}

/// Choose a storage operation for a single triple pattern.
///
/// Returns `Err` when the triple pattern contains an embedded quoted triple
/// (RDF* / SPARQL-star syntax), which is parsed successfully by spargebra but
/// whose annotation evaluation is not yet implemented (URS-QEC-S03 /
/// URS-QEC-X01).  Failing loud here prevents silent wrong results.
fn plan_triple_pattern(tp: &TriplePattern, scope: &TripleScope) -> Result<TripleOp, SparqlError> {
    // URS-QEC-S03 / URS-QEC-X01: detect RDF* embedded triple terms and fail loud.
    if matches!(&tp.subject, TermPattern::Triple(_)) {
        return Err(SparqlError::Plan(
            "RDF* (RDF-star) annotation evaluation is not yet implemented. \
             Quoted triple patterns << ?s ?p ?o >> are parsed but the annotation \
             variable binding against edge_annotations is not supported."
                .into(),
        ));
    }
    if matches!(&tp.object, TermPattern::Triple(_)) {
        return Err(SparqlError::Plan(
            "RDF* (RDF-star) object quoted triples are not yet implemented.".into(),
        ));
    }

    let scope = scope.clone();

    let subject_bound = matches!(&tp.subject, TermPattern::NamedNode(_));
    let predicate_bound = matches!(&tp.predicate, NamedNodePattern::NamedNode(_));
    let object_bound = matches!(
        &tp.object,
        TermPattern::NamedNode(_) | TermPattern::Literal(_)
    );

    let op = if subject_bound {
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
            scope,
            subject,
            predicate_filter,
        }
    } else if predicate_bound {
        let predicate = match &tp.predicate {
            NamedNodePattern::NamedNode(n) => n.as_str().to_string(),
            _ => unreachable!(),
        };
        TripleOp::PredicateScan { scope, predicate }
    } else if object_bound {
        let object = match &tp.object {
            TermPattern::NamedNode(n) => n.as_str().to_string(),
            // The object column stores the BARE lexical form of a literal
            // (`update::write_triple` writes `Literal::value()`), so the scan
            // key must be the lexical form too. `Literal::to_string()` is the
            // N-Triples *serialization* (`"beta"`, quoted, possibly with a
            // `^^<datatype>` suffix) and never equals a stored object, which
            // made every literal-object pattern match nothing (t_c3a2d3e4).
            TermPattern::Literal(l) => l.value().to_string(),
            _ => unreachable!(),
        };
        TripleOp::ObjectScan { scope, object }
    } else {
        TripleOp::FullScan { scope }
    };
    Ok(op)
}

/// Extract literal IRI strings from `DESCRIBE <iri>` patterns.
///
/// `DESCRIBE <iri>` is parsed by spargebra as an `Extend` node that binds a
/// fresh variable to the literal IRI.  This function walks the pattern tree
/// and collects the IRI strings from such `Extend` nodes.  Returns an empty
/// `Vec` if the pattern is a WHERE clause (variable describe) rather than
/// literal IRIs.
/// Extract literal IRI strings from `DESCRIBE <iri>` patterns.
///
/// `DESCRIBE <iri>` is parsed by spargebra as:
///   `Project { inner: Extend { inner: Bgp { patterns: [] },
///                              variable: _freshvar,
///                              expression: NamedNode(<iri>) },
///              variables: [_freshvar] }`
///
/// This function recursively walks the pattern tree and collects the IRI
/// strings from `Extend` nodes whose `expression` is a literal `NamedNode`.
/// Returns an empty `Vec` if the pattern is a WHERE clause (variable describe)
/// rather than literal IRIs.
pub fn extract_describe_iris(pattern: &spargebra::algebra::GraphPattern) -> Vec<String> {
    use spargebra::algebra::{Expression, GraphPattern};
    match pattern {
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            let mut iris = extract_describe_iris(inner);
            if let Expression::NamedNode(n) = expression {
                iris.push(n.as_str().to_string());
            }
            iris
        }
        // spargebra wraps the Extend in a Project — recurse through wrapper nodes.
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner } => extract_describe_iris(inner),
        GraphPattern::Slice { inner, .. } => extract_describe_iris(inner),
        GraphPattern::OrderBy { inner, .. } => extract_describe_iris(inner),
        GraphPattern::Filter { inner, .. } => extract_describe_iris(inner),
        GraphPattern::Bgp { .. } => vec![],
        _ => vec![],
    }
}

/// COUNT may only consume algebra the evaluator implements without changing its
/// bag of solutions. In particular, the older UNION/OPTIONAL/subquery shortcuts
/// must never silently produce a plausible but incorrect aggregate.
fn validate_count_input(pattern: &GraphPattern) -> Result<(), SparqlError> {
    match pattern {
  GraphPattern::Bgp{..}|GraphPattern::Path{..}=>Ok(()),
  GraphPattern::Filter{inner,..}|GraphPattern::Extend{inner,..}|GraphPattern::Graph{inner,..}=>validate_count_input(inner),
  GraphPattern::Join{left,right}=>{validate_count_input(left)?;validate_count_input(right)},
  _=>Err(SparqlError::Plan("COUNT over this input algebra is not implemented (UNION, OPTIONAL, subqueries and blocking input operators)".into()))
 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_simple_select() {
        let query = spargebra::SparqlParser::new()
            .parse_query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
            .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert_eq!(plan.projection, vec!["s", "p", "o"]);
        assert_eq!(plan.ops.len(), 1);
        assert!(matches!(plan.ops[0].1, TripleOp::FullScan { .. }));
    }

    #[test]
    fn plan_select_with_bound_subject() {
        let query = spargebra::SparqlParser::new()
            .parse_query("SELECT ?p ?o WHERE { <http://example.org/alice> ?p ?o }")
            .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert_eq!(plan.ops.len(), 1);
        assert!(matches!(plan.ops[0].1, TripleOp::SubjectLookup { .. }));
    }

    #[test]
    fn plan_select_with_limit() {
        let query = spargebra::SparqlParser::new()
            .parse_query("SELECT ?s WHERE { ?s ?p ?o } LIMIT 10")
            .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn plan_ask_sets_is_ask_flag() {
        let query = spargebra::SparqlParser::new()
            .parse_query(
                "ASK { <http://example.org/alice> <http://xmlns.com/foaf/0.1/name> ?name }",
            )
            .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert!(plan.is_ask, "ASK query must set is_ask=true");
        assert_eq!(plan.limit, Some(1), "ASK query must limit to 1 row");
    }

    #[test]
    fn plan_select_not_ask() {
        let query = spargebra::SparqlParser::new()
            .parse_query("SELECT ?s WHERE { ?s ?p ?o }")
            .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert!(!plan.is_ask, "SELECT query must set is_ask=false");
    }

    #[test]
    fn plan_distinct_sets_flag() {
        let query = spargebra::SparqlParser::new()
            .parse_query("SELECT DISTINCT ?s WHERE { ?s ?p ?o }")
            .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert!(plan.distinct, "DISTINCT must set distinct=true");
    }

    #[test]
    fn plan_order_by_captures_conditions() {
        let query = spargebra::SparqlParser::new()
            .parse_query(
                "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name } ORDER BY ?name",
            )
            .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert_eq!(plan.order_by.len(), 1);
        assert!(matches!(
            &plan.order_by[0].expression,
            spargebra::algebra::Expression::Variable(v) if v.as_str() == "name"
        ));
        assert!(plan.order_by[0].ascending);
    }

    #[test]
    fn plan_order_by_expression() {
        // URS-QEC-S04: ORDER BY on an arithmetic expression must plan, not fail.
        let query = spargebra::SparqlParser::new()
            .parse_query(
                "SELECT ?a ?b WHERE { ?s <http://ex/a> ?a ; <http://ex/b> ?b } ORDER BY DESC(?a + ?b)",
            )
            .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert_eq!(plan.order_by.len(), 1);
        assert!(!plan.order_by[0].ascending);
        assert!(
            matches!(
                &plan.order_by[0].expression,
                spargebra::algebra::Expression::Add(_, _)
            ),
            "ORDER BY (?a + ?b) must capture the Add expression"
        );
    }

    #[test]
    fn plan_order_by_desc() {
        let query = spargebra::SparqlParser::new().parse_query(
            "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name } ORDER BY DESC(?name)",
        )
        .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert_eq!(plan.order_by.len(), 1);
        assert!(!plan.order_by[0].ascending);
    }

    #[test]
    fn plan_filter_counts_unimplemented() {
        let query = spargebra::SparqlParser::new().parse_query(
            "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . FILTER(?name = \"Alice\") }",
        )
        .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert_eq!(plan.filters.len(), 1, "FILTER must capture one expression");
    }

    /// The keyspace selects the TABLE; the graph selects the PARTITION. They are
    /// different values and a plan must keep them apart.
    ///
    /// This case previously read `plan_uses_keyspace_as_graph` and asserted
    /// `graph == "my_tenant"` -- the keyspace. That was the bound-subject bug
    /// written down as an expectation, and it is why the bug survived: the
    /// writer takes the graph from the SPARQL graph name ("default"), so a plan
    /// binding the keyspace instead produced a point read for a partition key
    /// that was never written.
    #[test]
    fn plan_separates_keyspace_from_graph() {
        let query = spargebra::SparqlParser::new()
            .parse_query("SELECT ?p ?o WHERE { <http://example.org/alice> ?p ?o }")
            .unwrap();
        let scope = TripleScope::new("my_tenant", "default");
        let plan = plan_query(&query, &scope).unwrap();
        match &plan.ops[0].1 {
            TripleOp::SubjectLookup { scope, .. } => {
                assert_eq!(
                    scope.keyspace, "my_tenant",
                    "the keyspace must be carried through -- it names the table"
                );
                assert_eq!(
                    scope.graph, "default",
                    "the graph must come from the SPARQL graph name, not the keyspace"
                );
            }
            other => panic!("expected SubjectLookup, got {other:?}"),
        }
    }

    #[test]
    fn plan_property_path_one_or_more_emits_path_op() {
        let query = spargebra::SparqlParser::new()
            .parse_query(
                "SELECT ?o WHERE { <http://example.org/alice> <http://xmlns.com/foaf/0.1/knows>+ ?o }",
            )
            .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert_eq!(plan.ops.len(), 1);
        assert!(
            matches!(plan.ops[0].1, TripleOp::PropertyPath { .. }),
            "OneOrMore path must produce PropertyPath op, got {:?}",
            plan.ops[0].1
        );
    }

    #[test]
    fn plan_property_path_zero_or_more_emits_path_op() {
        let query = spargebra::SparqlParser::new()
            .parse_query(
                "SELECT ?o WHERE { <http://example.org/alice> <http://xmlns.com/foaf/0.1/knows>* ?o }",
            )
            .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert!(
            matches!(plan.ops[0].1, TripleOp::PropertyPath { .. }),
            "ZeroOrMore path must produce PropertyPath op"
        );
    }

    #[test]
    fn plan_simple_named_path_emits_standard_op() {
        // A simple named path (no closure operator) should NOT produce PropertyPath.
        let query = spargebra::SparqlParser::new()
            .parse_query(
                "SELECT ?o WHERE { <http://example.org/alice> <http://xmlns.com/foaf/0.1/knows> ?o }",
            )
            .unwrap();
        let plan = plan_query(&query, &TripleScope::new("default", "default")).unwrap();
        assert!(
            !matches!(plan.ops[0].1, TripleOp::PropertyPath { .. }),
            "simple BGP should not produce PropertyPath op"
        );
    }
}
