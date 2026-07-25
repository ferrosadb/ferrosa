//! Property path evaluation via BFS/DFS traversal.
//!
//! Implements SPARQL 1.1 property path operators (`+`, `*`, `?`) by
//! walking the adjacency structure of the triple store. Fixes the
//! silent single-hop degradation bug (BUG-S7).

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
pub type PathResult = Vec<(String, String)>;

/// Evaluate a property path pattern against storage.
///
/// Returns all `(subject, object)` pairs reachable via the path expression.
pub async fn evaluate_property_path(
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
    graph: &str,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<PathResult, SparqlError> {
    match path {
        PropertyPathExpression::NamedNode(predicate) => {
            evaluate_single_hop(
                subject,
                predicate.as_str(),
                object,
                graph,
                write_path,
                limits,
            )
            .await
        }
        PropertyPathExpression::OneOrMore(inner) => {
            evaluate_closure(subject, inner, object, graph, write_path, limits, false).await
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            evaluate_closure(subject, inner, object, graph, write_path, limits, true).await
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            evaluate_zero_or_one(subject, inner, object, graph, write_path, limits).await
        }
        PropertyPathExpression::Reverse(inner) => {
            // Swap subject and object, evaluate, then swap results back.
            let results = Box::pin(evaluate_property_path(
                object, inner, subject, graph, write_path, limits,
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
    graph: &str,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<PathResult, SparqlError> {
    let triples = fetch_triples_for_predicate(graph, predicate, write_path, limits).await?;
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
    graph: &str,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
    include_start: bool,
) -> Result<PathResult, SparqlError> {
    let predicate = extract_single_predicate(inner)?;
    let adjacency = fetch_triples_for_predicate(graph, &predicate, write_path, limits).await?;
    let starts = collect_start_nodes(subject, &adjacency);

    let mut results = Vec::new();
    for start in &starts {
        let reachable = bfs_reachable(start, &adjacency, include_start);
        let filtered = filter_reachable_by_object(object, start, &reachable);
        results.extend(filtered);
    }
    Ok(results)
}

/// ZeroOrOne: return 0-hop (self) and 1-hop matches.
async fn evaluate_zero_or_one(
    subject: &TermPattern,
    inner: &PropertyPathExpression,
    object: &TermPattern,
    graph: &str,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<PathResult, SparqlError> {
    let predicate = extract_single_predicate(inner)?;
    let adjacency = fetch_triples_for_predicate(graph, &predicate, write_path, limits).await?;
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
            }
        }
    }
    Ok(results)
}

/// BFS from a start node, collecting all reachable nodes.
///
/// Uses a visited set for cycle detection.
fn bfs_reachable(start: &str, adjacency: &[(String, String)], include_start: bool) -> Vec<String> {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    let mut reachable = Vec::new();

    if include_start {
        reachable.push(start.to_string());
    }
    visited.insert(start.to_string());

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
    graph: &str,
    predicate: &str,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<Vec<(String, String)>, SparqlError> {
    let table_id = triple_store::triples_table_id(graph);
    let mut partitions = write_path.range_read_stream_all(&table_id, 0).await?;

    let mut pairs = Vec::new();
    let mut rows_read: usize = 0;
    while let Some(partition) = partitions.next().await {
        let partition = partition?;
        let subject = crate::executor::extract_subject_from_key(&partition);
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
            let pred = crate::executor::clustering_component(&row.clustering, 0);
            if pred == predicate {
                let obj = crate::executor::clustering_component(&row.clustering, 1);
                pairs.push((subject.clone(), obj));
            }
        }
    }
    Ok(pairs)
}

/// Determine start nodes from the subject pattern.
fn collect_start_nodes(subject: &TermPattern, adjacency: &[(String, String)]) -> Vec<String> {
    match subject {
        TermPattern::NamedNode(n) => vec![n.as_str().to_string()],
        _ => {
            // Variable subject: collect all unique subjects from adjacency.
            let mut starts: Vec<String> = adjacency.iter().map(|(s, _)| s.clone()).collect();
            starts.sort();
            starts.dedup();
            starts
        }
    }
}

/// Check whether a node matches an endpoint pattern.
fn matches_endpoint(pattern: &TermPattern, node: &str) -> bool {
    match pattern {
        TermPattern::NamedNode(n) => n.as_str() == node,
        TermPattern::Variable(_) => true,
        TermPattern::Literal(l) => l.value() == node,
        _ => true,
    }
}

/// Filter BFS reachable nodes against the object pattern.
fn filter_reachable_by_object(
    object: &TermPattern,
    start: &str,
    reachable: &[String],
) -> Vec<(String, String)> {
    reachable
        .iter()
        .filter(|node| matches_endpoint(object, node))
        .map(|node| (start.to_string(), node.clone()))
        .collect()
}

/// Filter raw triples by subject/object endpoint patterns.
fn filter_by_endpoints(
    subject: &TermPattern,
    object: &TermPattern,
    triples: &[(String, String)],
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
        .map(|(s, o)| {
            let mut row = std::collections::HashMap::new();
            if let TermPattern::Variable(var) = subject {
                row.insert(
                    var.as_str().to_string(),
                    Binding {
                        binding_type: "uri".into(),
                        value: s.clone(),
                        datatype: None,
                        lang: None,
                    },
                );
            }
            if let TermPattern::Variable(var) = object {
                row.insert(
                    var.as_str().to_string(),
                    Binding {
                        binding_type: "uri".into(),
                        value: o.clone(),
                        datatype: None,
                        lang: None,
                    },
                );
            }
            row
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- BFS cycle detection ---

    #[test]
    fn bfs_reachable_detects_cycle() {
        // Graph: A -> B -> C -> A (cycle)
        let adjacency = vec![
            ("A".into(), "B".into()),
            ("B".into(), "C".into()),
            ("C".into(), "A".into()),
        ];
        let reachable = bfs_reachable("A", &adjacency, false);
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
        let adjacency = vec![("A".into(), "B".into()), ("B".into(), "C".into())];
        let reachable = bfs_reachable("A", &adjacency, true);
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
        let adjacency = vec![
            ("alice".into(), "bob".into()),
            ("bob".into(), "carol".into()),
            ("carol".into(), "dave".into()),
        ];
        let reachable = bfs_reachable("alice", &adjacency, false);
        assert_eq!(reachable.len(), 3);
        assert!(reachable.contains(&"bob".to_string()));
        assert!(reachable.contains(&"carol".to_string()));
        assert!(reachable.contains(&"dave".to_string()));
    }

    #[test]
    fn bfs_single_node_no_edges() {
        let adjacency: Vec<(String, String)> = vec![];
        let reachable = bfs_reachable("lonely", &adjacency, false);
        assert!(
            reachable.is_empty(),
            "no outgoing edges means no reachable nodes"
        );
    }

    #[test]
    fn bfs_diamond_graph_no_duplicates() {
        // Diamond: A -> B, A -> C, B -> D, C -> D
        let adjacency = vec![
            ("A".into(), "B".into()),
            ("A".into(), "C".into()),
            ("B".into(), "D".into()),
            ("C".into(), "D".into()),
        ];
        let reachable = bfs_reachable("A", &adjacency, false);
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
        assert!(matches_endpoint(&pattern, "http://ex/alice"));
        assert!(!matches_endpoint(&pattern, "http://ex/bob"));
    }

    #[test]
    fn matches_endpoint_variable_matches_anything() {
        let pattern = TermPattern::Variable(spargebra::term::Variable::new_unchecked("x"));
        assert!(matches_endpoint(&pattern, "anything"));
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
        let adjacency = vec![("other".into(), "x".into())];
        let starts = collect_start_nodes(&subject, &adjacency);
        assert_eq!(starts, vec!["http://ex/alice"]);
    }

    #[test]
    fn collect_start_nodes_variable_subject() {
        let subject = TermPattern::Variable(spargebra::term::Variable::new_unchecked("s"));
        let adjacency = vec![
            ("alice".into(), "bob".into()),
            ("bob".into(), "carol".into()),
            ("alice".into(), "carol".into()),
        ];
        let starts = collect_start_nodes(&subject, &adjacency);
        assert_eq!(starts, vec!["alice", "bob"]);
    }
}
