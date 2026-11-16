//! Property path evaluation as a stream of `(start, end)` pairs.
//!
//! Implements SPARQL 1.1 property path operators (`+`, `*`, `?`, `^`) without
//! holding the predicate's adjacency in memory (BUG-S7 fixed the silent
//! single-hop degradation; this module fixes the materialization):
//!
//! - a single hop streams the predicate's edges — a point read when the subject
//!   is a constant, a streaming scan otherwise;
//! - a closure is a breadth-first search that EXPANDS THE FRONTIER BY LOOKUP:
//!   each node's outgoing edges come from a point read of that node's
//!   partition, never from a pre-loaded edge list. The visited set is a
//!   spilling de-duplication set and each BFS level is a spilling bag, so the
//!   closure's working set is bounded however large the reachable graph is;
//! - a variable start enumerates the predicate's distinct subjects from a
//!   streaming scan (de-duplicated through a spilling set) and runs one BFS per
//!   start.
//!
//! Correctness: legacy RDF identity tests; closure pinned past the old row bound.
//! Last revised: 2026-11-16
//! Last changed: Stream path pairs; BFS by per-node lookup with spilled visited/frontier state.

use std::future::ready;
use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::TermPattern;

use crate::error::SparqlError;
use crate::executor::{triple_stream, TripleStream};
use crate::planner::{TripleOp, TripleScope};
use crate::results::Binding;
use crate::spill::{
    binding_heap_bytes, term_key, BagIter, ExecutionContext, Solution, SpillBag, SpillDedup, Term,
    TermRef,
};

/// A lazy stream of `(start, end)` pairs satisfying a path.
pub type PairStream = BoxStream<'static, Result<(Binding, Binding), SparqlError>>;

/// Evaluate a property path pattern as a stream of `(subject, object)` pairs.
pub fn path_pairs(
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
    scope: &TripleScope,
    write_path: &Arc<WritePath>,
    cx: &ExecutionContext,
) -> PairStream {
    let env = Env {
        scope: scope.clone(),
        write_path: Arc::clone(write_path),
        cx: cx.clone(),
    };
    match path {
        PropertyPathExpression::NamedNode(predicate) => {
            single_hop(subject, predicate.as_str(), object, env)
        }
        PropertyPathExpression::OneOrMore(inner) => closure(subject, inner, object, env, false),
        PropertyPathExpression::ZeroOrMore(inner) => closure(subject, inner, object, env, true),
        PropertyPathExpression::ZeroOrOne(inner) => zero_or_one(subject, inner, object, env),
        PropertyPathExpression::Reverse(inner) => {
            // Swap subject and object, evaluate, then swap results back.
            path_pairs(object, inner, subject, scope, write_path, cx)
                .map_ok(|(s, o)| (o, s))
                .boxed()
        }
        _ => fail(SparqlError::Plan(format!(
            "unsupported property path expression: {path:?}"
        ))),
    }
}

/// One path pair as a solution over the pattern's variables, or `None` when a
/// variable used at both ends would need two different values.
pub(crate) fn pair_to_binding(
    subject: &TermPattern,
    object: &TermPattern,
    s: Binding,
    o: Binding,
) -> Option<Solution> {
    let mut row = Solution::new();
    if let TermPattern::Variable(var) = subject {
        row.insert(var.as_str().to_string(), s);
    }
    if let TermPattern::Variable(var) = object {
        if row.get(var.as_str()).is_some_and(|existing| *existing != o) {
            return None;
        }
        row.insert(var.as_str().to_string(), o);
    }
    Some(row)
}

/// What every path stage needs to reach storage and spill.
#[derive(Clone)]
struct Env {
    scope: TripleScope,
    write_path: Arc<WritePath>,
    cx: ExecutionContext,
}

impl Env {
    /// Objects of `node`'s `predicate` edges: one point read of its partition.
    /// Only IRIs and blank nodes have outgoing edges.
    fn out_edges(&self, node: &Binding, predicate: &str) -> TripleStream {
        if !matches!(node, Binding::Uri { .. } | Binding::BlankNode { .. }) {
            return stream::empty().boxed();
        }
        let Some(subject) = node.scalar_value() else {
            return stream::empty().boxed();
        };
        let op = TripleOp::SubjectLookup {
            scope: self.scope.clone(),
            subject: subject.to_owned(),
            predicate_filter: Some(predicate.to_owned()),
        };
        triple_stream(&op, &self.write_path, &self.cx)
    }

    /// Every `predicate` edge in scope, as a streaming scan.
    fn all_edges(&self, predicate: &str) -> TripleStream {
        let op = TripleOp::PredicateScan {
            scope: self.scope.clone(),
            predicate: predicate.to_owned(),
        };
        triple_stream(&op, &self.write_path, &self.cx)
    }

    /// The start nodes for `subject`: the constant itself, or every distinct
    /// subject of a `predicate` edge (de-duplicated through a spilling set).
    fn starts(&self, subject: &TermPattern, predicate: &str) -> NodeStream {
        match subject {
            TermPattern::NamedNode(n) => stream::iter([Ok(node_term(n.as_str()))]).boxed(),
            TermPattern::Literal(l) => stream::iter([Ok(Binding::from_literal(l))]).boxed(),
            _ => {
                let mut seen = self.cx.dedup("sparql-path-starts");
                self.all_edges(predicate)
                    .try_filter_map(move |(s, _, _)| {
                        let start = node_term(&s);
                        let fresh = term_key(&start).and_then(|key| seen.insert(&key));
                        ready(fresh.map(|fresh| fresh.then_some(start)))
                    })
                    .boxed()
            }
        }
    }
}

type NodeStream = BoxStream<'static, Result<Binding, SparqlError>>;

fn fail(e: SparqlError) -> PairStream {
    stream::iter([Err(e)]).boxed()
}

/// Single hop: the predicate's edges whose ends match the patterns.
fn single_hop(
    subject: &TermPattern,
    predicate: &str,
    object: &TermPattern,
    env: Env,
) -> PairStream {
    let edges = match subject {
        TermPattern::NamedNode(n) => env.out_edges(&node_term(n.as_str()), predicate),
        _ => env.all_edges(predicate),
    };
    let subject = subject.clone();
    let object = object.clone();
    edges
        .try_filter_map(move |(s, _, o)| {
            let s = node_term(&s);
            let keep = matches_endpoint(&subject, &s) && matches_endpoint(&object, &o);
            ready(Ok(keep.then_some((s, o))))
        })
        .boxed()
}

/// Transitive closure (OneOrMore / ZeroOrMore): one BFS per start node.
///
/// When `include_start` is true (ZeroOrMore) the start is paired with itself;
/// otherwise at least one hop is required, and the start is never reported
/// even when a cycle leads back to it.
fn closure(
    subject: &TermPattern,
    inner: &PropertyPathExpression,
    object: &TermPattern,
    env: Env,
    include_start: bool,
) -> PairStream {
    let predicate: Arc<str> = match extract_single_predicate(inner) {
        Ok(p) => p.into(),
        Err(e) => return fail(e),
    };
    let object = Arc::new(object.clone());
    let starts = env.starts(subject, &predicate);
    starts
        .map_ok(move |start| {
            bfs(
                start,
                Arc::clone(&predicate),
                Arc::clone(&object),
                env.clone(),
                include_start,
            )
        })
        .try_flatten()
        .boxed()
}

/// ZeroOrOne: the 0-hop self pair and the 1-hop neighbours of each start.
fn zero_or_one(
    subject: &TermPattern,
    inner: &PropertyPathExpression,
    object: &TermPattern,
    env: Env,
) -> PairStream {
    let predicate: Arc<str> = match extract_single_predicate(inner) {
        Ok(p) => p.into(),
        Err(e) => return fail(e),
    };
    let object = Arc::new(object.clone());
    env.starts(subject, &predicate)
        .map_ok(move |start| {
            let hops = env.out_edges(&start, &predicate);
            let object_hop = Arc::clone(&object);
            let first = matches_endpoint(&object, &start).then(|| (start.clone(), start.clone()));
            let neighbours = hops.try_filter_map(move |(_, _, o)| {
                let pair = matches_endpoint(&object_hop, &o).then(|| (start.clone(), o));
                ready(Ok(pair))
            });
            stream::iter(first.map(Ok)).chain(neighbours)
        })
        .try_flatten()
        .boxed()
}

/// A node parked in a BFS level, spilled in the [`Term`] form.
struct SpillNode(Binding);

impl Serialize for SpillNode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        TermRef::from(&self.0).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SpillNode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Term::deserialize(deserializer).map(|term| Self(term.into_binding()))
    }
}

impl ferrosa_storage::external_sort::SpillRow for SpillNode {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Binding>() + binding_heap_bytes(&self.0)
    }
}

/// BFS state between pulls.
struct Bfs {
    start: Binding,
    predicate: Arc<str>,
    object: Arc<TermPattern>,
    env: Env,
    visited: SpillDedup,
    /// The level being expanded.
    current: BagIter<SpillNode>,
    /// The level being discovered.
    next: SpillBag<SpillNode>,
    /// Outgoing edges of the node being expanded.
    edges: Option<TripleStream>,
    /// ZeroOrMore's self pair, still to be reported.
    pending_start: bool,
}

/// Breadth-first closure from `start`, expanding each node by a point lookup.
fn bfs(
    start: Binding,
    predicate: Arc<str>,
    object: Arc<TermPattern>,
    env: Env,
    include_start: bool,
) -> PairStream {
    let mut visited = env.cx.dedup("sparql-path-visited");
    let seeded = term_key(&start).and_then(|key| visited.insert(&key));
    if let Err(e) = seeded {
        return fail(e);
    }
    let edges = Some(env.out_edges(&start, &predicate));
    let state = Bfs {
        next: env.cx.bag("sparql-path-frontier"),
        current: BagIter::Memory(Vec::new().into_iter()),
        start,
        predicate,
        object,
        env,
        visited,
        edges,
        pending_start: include_start,
    };
    stream::unfold(Some(state), |state| async move {
        let mut state = state?;
        match state.advance().await {
            Ok(Some(pair)) => Some((Ok(pair), Some(state))),
            Ok(None) => None,
            // Fail loud, then end: a closure that keeps going after a lost
            // lookup would report a partial reachability set as complete.
            Err(e) => Some((Err(e), None)),
        }
    })
    .boxed()
}

impl Bfs {
    /// Produce the next reachable pair, or `None` when the closure is done.
    async fn advance(&mut self) -> Result<Option<(Binding, Binding)>, SparqlError> {
        if std::mem::take(&mut self.pending_start) && matches_endpoint(&self.object, &self.start) {
            return Ok(Some((self.start.clone(), self.start.clone())));
        }
        loop {
            // 1. Discover: drain the expanding node's edges into the next level.
            if let Some(edges) = self.edges.as_mut() {
                match edges.next().await {
                    Some(edge) => {
                        let (_, _, node) = edge?;
                        if self.visited.insert(&term_key(&node)?)? {
                            self.next.push(SpillNode(node))?;
                        }
                        continue;
                    }
                    None => self.edges = None,
                }
            }
            // 2. Expand: take the next node of the current level, report it and
            //    open its edges.
            if let Some(node) = self.current.next() {
                let SpillNode(node) = node?;
                self.edges = Some(self.env.out_edges(&node, &self.predicate));
                if matches_endpoint(&self.object, &node) {
                    return Ok(Some((self.start.clone(), node)));
                }
                continue;
            }
            // 3. Advance a level, or finish.
            if self.next.is_empty() {
                return Ok(None);
            }
            let level = std::mem::replace(&mut self.next, self.env.cx.bag("sparql-path-frontier"));
            self.current = level.drain()?;
        }
    }
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

/// Check whether a node matches an endpoint pattern.
fn matches_endpoint(pattern: &TermPattern, node: &Binding) -> bool {
    match pattern {
        TermPattern::NamedNode(n) => {
            node.kind() == "uri" && node.scalar_value() == Some(n.as_str())
        }
        TermPattern::Variable(_) => true,
        TermPattern::Literal(l) => Binding::from_literal(l) == *node,
        _ => true,
    }
}

fn node_term(value: &str) -> Binding {
    if value.starts_with("_:") {
        Binding::blank_node(value)
    } else {
        Binding::uri(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn a_spilled_frontier_node_round_trips() {
        for node in [
            node_term("http://ex/a"),
            node_term("_:b1"),
            Binding::literal("x", None, Some("en".into())),
        ] {
            let bytes = serde_json::to_vec(&SpillNode(node.clone())).unwrap();
            let back: SpillNode = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(back.0, node);
        }
    }

    #[test]
    fn a_variable_used_at_both_ends_must_agree() {
        let x = TermPattern::Variable(spargebra::term::Variable::new_unchecked("x"));
        assert!(pair_to_binding(&x, &x, node_term("a"), node_term("b")).is_none());
        let row = pair_to_binding(&x, &x, node_term("a"), node_term("a")).unwrap();
        assert_eq!(row["x"], node_term("a"));
    }
}
