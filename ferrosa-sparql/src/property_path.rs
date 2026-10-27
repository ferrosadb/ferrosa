//! Property path evaluation via BFS/DFS traversal.
//!
//! Implements SPARQL 1.1 property path operators (`+`, `*`, `?`) by
//! walking the adjacency structure of the triple store. Fixes the
//! silent single-hop degradation bug (BUG-S7).
//! Correctness: legacy RDF identity tests and COUNT/JSON/protocol regressions.
//! Last revised: 2026-10-27
//! Last changed: Preserve typed scalar term identity through path results.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use futures::StreamExt;
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::TermPattern;

use crate::error::SparqlError;
use crate::executor::ExecutionLimits;
use crate::results::Binding;
use crate::triple_store;

/// Result of evaluating a property path: pairs of (start_node, end_node).
pub type PathResult = Vec<(Binding, Binding)>;

/// Evaluate a property path pattern against storage.
///
/// Returns all `(subject, object)` pairs reachable via the path expression.
pub async fn evaluate_property_path(
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
    scope: &crate::planner::TripleScope,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<PathResult, SparqlError> {
    match path {
        PropertyPathExpression::NamedNode(predicate) => {
            evaluate_single_hop(
                subject,
                predicate.as_str(),
                object,
                scope,
                write_path,
                limits,
            )
            .await
        }
        PropertyPathExpression::OneOrMore(inner) => {
            evaluate_closure(subject, inner, object, scope, write_path, limits, false).await
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            evaluate_closure(subject, inner, object, scope, write_path, limits, true).await
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            evaluate_zero_or_one(subject, inner, object, scope, write_path, limits).await
        }
        PropertyPathExpression::Reverse(inner) => {
            // Swap subject and object, evaluate, then swap results back.
            let results = Box::pin(evaluate_property_path(
                object, inner, subject, scope, write_path, limits,
            ))
            .await?;
            Ok(results.into_iter().map(|(s, o)| (o, s)).collect())
        }
        _ => Err(SparqlError::Plan(format!(
            "unsupported property path expression: {path:?}"
        ))),
    }
}

/// Single-hop: fetch triples matching (subject, predicate, object).
async fn evaluate_single_hop(
    subject: &TermPattern,
    predicate: &str,
    object: &TermPattern,
    scope: &crate::planner::TripleScope,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<PathResult, SparqlError> {
    let triples = fetch_triples_for_predicate(scope, predicate, write_path, limits).await?;
    filter_by_endpoints(subject, object, &triples)
}

/// Transitive closure via BFS (OneOrMore / ZeroOrMore).
///
/// When `include_start` is true (ZeroOrMore), includes the start node
/// paired with itself. When false (OneOrMore), requires at least one hop.
async fn evaluate_closure(
    subject: &TermPattern,
    inner: &PropertyPathExpression,
    object: &TermPattern,
    scope: &crate::planner::TripleScope,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
    include_start: bool,
) -> Result<PathResult, SparqlError> {
    let predicate = extract_single_predicate(inner)?;
    let adjacency = fetch_triples_for_predicate(scope, &predicate, write_path, limits).await?;
    let starts = collect_start_nodes(subject, &adjacency);

    let mut results = Vec::new();
    for start in &starts {
        let reachable = bfs_reachable(start, &adjacency, include_start);
        let filtered = filter_reachable_by_object(object, start, &reachable);
        results.extend(filtered);
        if results.len() > limits.max_rows {
            return Err(SparqlError::Execution(
                "property path result exceeds row bound".into(),
            ));
        }
    }
    Ok(results)
}

/// ZeroOrOne: return 0-hop (self) and 1-hop matches.
async fn evaluate_zero_or_one(
    subject: &TermPattern,
    inner: &PropertyPathExpression,
    object: &TermPattern,
    scope: &crate::planner::TripleScope,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<PathResult, SparqlError> {
    let predicate = extract_single_predicate(inner)?;
    let adjacency = fetch_triples_for_predicate(scope, &predicate, write_path, limits).await?;
    let starts = collect_start_nodes(subject, &adjacency);

    let mut results = Vec::new();
    for start in &starts {
        // Zero hop: start node matches itself.
        if matches_endpoint(object, start) {
            results.push((start.clone(), start.clone()));
        }
        // One hop: direct neighbors.
        for (s, o) in &adjacency {
            if s == start && matches_endpoint(object, o) {
                results.push((s.clone(), o.clone()));
                if results.len() > limits.max_rows {
                    return Err(SparqlError::Execution(
                        "property path result exceeds row bound".into(),
                    ));
                }
            }
        }
    }
    Ok(results)
}

/// BFS from a start node, collecting all reachable nodes.
///
/// Uses a visited set for cycle detection.
fn bfs_reachable<T: Clone + Eq + std::hash::Hash>(
    start: &T,
    adjacency: &[(T, T)],
    include_start: bool,
) -> Vec<T> {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    let mut reachable = Vec::new();

    if include_start {
        reachable.push(start.clone());
    }
    visited.insert(start.clone());

    // Seed queue with direct neighbors.
    for (s, o) in adjacency {
        if s == start && visited.insert(o.clone()) {
            queue.push_back(o.clone());
            reachable.push(o.clone());
        }
    }

    // BFS over remaining hops.
    while let Some(node) = queue.pop_front() {
        for (s, o) in adjacency {
            if s == &node && visited.insert(o.clone()) {
                queue.push_back(o.clone());
                reachable.push(o.clone());
            }
        }
    }

    reachable
}

/// Extract a simple predicate IRI from a path expression.
///
/// Only supports single named node paths (the base case for closure operators).
fn extract_single_predicate(path: &PropertyPathExpression) -> Result<String, SparqlError> {
    match path {
        PropertyPathExpression::NamedNode(n) => Ok(n.as_str().to_string()),
        _ => Err(SparqlError::Plan(
            "nested property path operators not yet supported in closure".into(),
        )),
    }
}

/// Fetch all (subject, object) pairs for a given predicate from storage.
///
/// The scan STREAMS: partitions arrive one at a time from
/// [`WritePath::range_read_stream_all`] and only the matching `(subject,
/// object)` pairs are retained, so the whole table is never materialized.
///
/// BFS closure is a blocking operator — it cannot emit a reachable node before
/// it has seen the edges that reach it — so the adjacency list it walks IS
/// buffered. That buffer is bounded by [`ExecutionLimits::max_rows`] and
/// crossing the bound is a loud error, never a silent partial traversal (a
/// partial adjacency would produce a wrong answer that looks complete).
async fn fetch_triples_for_predicate(
    scope: &crate::planner::TripleScope,
    predicate: &str,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<PathResult, SparqlError> {
    let table_id = triple_store::triples_table_id(&scope.keyspace);
    let mut partitions = write_path.range_read_stream_all(&table_id, 0).await?;

    let mut pairs = Vec::new();
    let mut rows_read: usize = 0;
    while let Some(partition) = partitions.next().await {
        let partition = partition?;
        let (graph, subject) = triple_store::decode_partition(partition.key.key.as_bytes())?;
        if scope.isolated_graph && graph != scope.graph {
            continue;
        }
        let subject = node_term(&subject);
        for row in &partition.rows {
            rows_read += 1;
            if rows_read > limits.max_rows {
                return Err(SparqlError::Execution(format!(
                    "property path over <{predicate}> read more than {} storage rows. \
                     A partial adjacency would yield a wrong reachability answer that \
                     looks complete, so this fails instead of truncating — constrain the \
                     path or raise the engine's max_rows bound.",
                    limits.max_rows
                )));
            }
            if let Some((pred, obj)) = triple_store::decode_v2_row(row)? {
                if pred == predicate {
                    pairs.push((subject.clone(), obj));
                }
            }
        }
    }
    Ok(pairs)
}

/// Determine start nodes from the subject pattern.
fn collect_start_nodes(subject: &TermPattern, adjacency: &[(Binding, Binding)]) -> Vec<Binding> {
    match subject {
        TermPattern::NamedNode(n) => vec![node_term(n.as_str())],
        TermPattern::Literal(l) => vec![literal_term(l)],
        _ => {
            // Variable subject: collect all unique subjects from adjacency.
            let mut starts: Vec<Binding> = adjacency.iter().map(|(s, _)| s.clone()).collect();
            starts.sort();
            starts.dedup();
            starts
        }
    }
}

/// Check whether a node matches an endpoint pattern.
fn matches_endpoint(pattern: &TermPattern, node: &Binding) -> bool {
    match pattern {
        TermPattern::NamedNode(n) => {
            node.kind() == "uri" && node.scalar_value() == Some(n.as_str())
        }
        TermPattern::Variable(_) => true,
        TermPattern::Literal(l) => literal_term(l) == *node,
        _ => true,
    }
}

/// Filter BFS reachable nodes against the object pattern.
fn filter_reachable_by_object(
    object: &TermPattern,
    start: &Binding,
    reachable: &[Binding],
) -> PathResult {
    reachable
        .iter()
        .filter(|node| matches_endpoint(object, node))
        .map(|node| (start.clone(), node.clone()))
        .collect()
}

/// Filter raw triples by subject/object endpoint patterns.
fn filter_by_endpoints(
    subject: &TermPattern,
    object: &TermPattern,
    triples: &[(Binding, Binding)],
) -> Result<PathResult, SparqlError> {
    let results = triples
        .iter()
        .filter(|(s, o)| matches_endpoint(subject, s) && matches_endpoint(object, o))
        .cloned()
        .collect();
    Ok(results)
}

/// Convert path results to binding sets for the executor.
pub fn path_results_to_bindings(
    subject: &TermPattern,
    object: &TermPattern,
    results: &PathResult,
) -> Vec<std::collections::HashMap<String, Binding>> {
    results
        .iter()
        .filter_map(|(s, o)| {
            let mut row = std::collections::HashMap::new();
            if let TermPattern::Variable(var) = subject {
                row.insert(var.as_str().to_string(), s.clone());
            }
            if let TermPattern::Variable(var) = object {
                if row.get(var.as_str()).is_some_and(|existing| existing != o) {
                    return None;
                }
                row.insert(var.as_str().to_string(), o.clone());
            }
            Some(row)
        })
        .collect()
}

fn node_term(value: &str) -> Binding {
    if value.starts_with("_:") {
        Binding::blank_node(value)
    } else {
        Binding::uri(value)
    }
}
fn literal_term(literal: &spargebra::term::Literal) -> Binding {
    Binding::from_literal(literal)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- BFS cycle detection ---

    #[test]
    fn bfs_reachable_detects_cycle() {
        // Graph: A -> B -> C -> A (cycle)
        let adjacency: Vec<(String, String)> = vec![
            ("A".into(), "B".into()),
            ("B".into(), "C".into()),
            ("C".into(), "A".into()),
        ];
        let reachable = bfs_reachable(&"A".to_string(), &adjacency, false);
        // Should find B and C (and A via cycle), but not loop forever.
        assert!(reachable.contains(&"B".to_string()));
        assert!(reachable.contains(&"C".to_string()));
        // A is revisited via cycle but was already in visited set —
        // however since include_start=false, A was in visited from the start.
        // So no duplicate A in reachable.
        let a_count = reachable.iter().filter(|n| *n == "A").count();
        assert_eq!(a_count, 0, "start node should not appear in OneOrMore");
    }

    #[test]
    fn bfs_reachable_zero_or_more_includes_start() {
        let adjacency: Vec<(String, String)> =
            vec![("A".into(), "B".into()), ("B".into(), "C".into())];
        let reachable = bfs_reachable(&"A".to_string(), &adjacency, true);
        assert!(
            reachable.contains(&"A".to_string()),
            "ZeroOrMore includes start"
        );
        assert!(reachable.contains(&"B".to_string()));
        assert!(reachable.contains(&"C".to_string()));
    }

    // --- Multi-hop property path ---

    #[test]
    fn bfs_multi_hop_returns_transitive_nodes() {
        // Graph: alice -> bob -> carol -> dave
        let adjacency: Vec<(String, String)> = vec![
            ("alice".into(), "bob".into()),
            ("bob".into(), "carol".into()),
            ("carol".into(), "dave".into()),
        ];
        let reachable = bfs_reachable(&"alice".to_string(), &adjacency, false);
        assert_eq!(reachable.len(), 3);
        assert!(reachable.contains(&"bob".to_string()));
        assert!(reachable.contains(&"carol".to_string()));
        assert!(reachable.contains(&"dave".to_string()));
    }

    #[test]
    fn bfs_single_node_no_edges() {
        let adjacency: Vec<(String, String)> = vec![];
        let reachable = bfs_reachable(&"lonely".to_string(), &adjacency, false);
        assert!(
            reachable.is_empty(),
            "no outgoing edges means no reachable nodes"
        );
    }

    #[test]
    fn bfs_diamond_graph_no_duplicates() {
        // Diamond: A -> B, A -> C, B -> D, C -> D
        let adjacency: Vec<(String, String)> = vec![
            ("A".into(), "B".into()),
            ("A".into(), "C".into()),
            ("B".into(), "D".into()),
            ("C".into(), "D".into()),
        ];
        let reachable = bfs_reachable(&"A".to_string(), &adjacency, false);
        // D should appear only once despite two paths.
        let d_count = reachable.iter().filter(|n| *n == "D").count();
        assert_eq!(d_count, 1, "diamond graph must not produce duplicate D");
        assert_eq!(reachable.len(), 3); // B, C, D
    }

    // --- matches_endpoint ---

    #[test]
    fn matches_endpoint_named_node() {
        let pattern =
            TermPattern::NamedNode(spargebra::term::NamedNode::new_unchecked("http://ex/alice"));
        assert!(matches_endpoint(&pattern, &node_term("http://ex/alice")));
        assert!(!matches_endpoint(&pattern, &node_term("http://ex/bob")));
    }

    #[test]
    fn matches_endpoint_variable_matches_anything() {
        let pattern = TermPattern::Variable(spargebra::term::Variable::new_unchecked("x"));
        assert!(matches_endpoint(&pattern, &node_term("anything")));
    }

    // --- extract_single_predicate ---

    #[test]
    fn extract_single_predicate_success() {
        let path = PropertyPathExpression::NamedNode(spargebra::term::NamedNode::new_unchecked(
            "http://ex/knows",
        ));
        let pred = extract_single_predicate(&path).unwrap();
        assert_eq!(pred, "http://ex/knows");
    }

    #[test]
    fn extract_single_predicate_nested_fails() {
        let inner = PropertyPathExpression::NamedNode(spargebra::term::NamedNode::new_unchecked(
            "http://ex/knows",
        ));
        let path = PropertyPathExpression::OneOrMore(Box::new(inner));
        assert!(extract_single_predicate(&path).is_err());
    }

    // --- collect_start_nodes ---

    #[test]
    fn collect_start_nodes_bound_subject() {
        let subject =
            TermPattern::NamedNode(spargebra::term::NamedNode::new_unchecked("http://ex/alice"));
        let adjacency: Vec<(String, String)> = vec![("other".into(), "x".into())];
        let adjacency: Vec<_> = adjacency
            .iter()
            .map(|(s, o)| (node_term(s), node_term(o)))
            .collect();
        let starts = collect_start_nodes(&subject, &adjacency)
            .into_iter()
            .map(|b| b.scalar_value().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(starts, vec!["http://ex/alice"]);
    }

    #[test]
    fn collect_start_nodes_variable_subject() {
        let subject = TermPattern::Variable(spargebra::term::Variable::new_unchecked("s"));
        let adjacency: Vec<(String, String)> = vec![
            ("alice".into(), "bob".into()),
            ("bob".into(), "carol".into()),
            ("alice".into(), "carol".into()),
        ];
        let adjacency: Vec<_> = adjacency
            .iter()
            .map(|(s, o)| (node_term(s), node_term(o)))
            .collect();
        let starts = collect_start_nodes(&subject, &adjacency)
            .into_iter()
            .map(|b| b.scalar_value().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(starts, vec!["alice", "bob"]);
    }
}
