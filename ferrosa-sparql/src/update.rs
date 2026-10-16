//! SPARQL UPDATE operations.
//!
//! Implements the ground-data ops (`INSERT DATA`, `DELETE DATA`) and the
//! pattern-based ops (`DELETE WHERE`, `DELETE/INSERT … WHERE`, `CLEAR`, `DROP`)
//! over the `rdf_triples` store (URS-QEC-D04). Pattern ops plan their WHERE
//! clause through the SELECT executor to bind solutions, then tombstone each
//! resulting ground triple via the same low-level [`StorageEngine`] path as
//! `DELETE DATA`. Genuinely unimplemented ops fail loud (URS-QEC-X01).

use std::collections::HashMap;
use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use spargebra::algebra::GraphTarget;
use spargebra::term::{
    GroundQuad, GroundQuadPattern, GroundTermPattern, NamedNodePattern, Quad, QuadPattern,
    TermPattern,
};

use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::engine::StorageEngine;

use crate::error::SparqlError;
use crate::executor::ExecutionLimits;
use crate::results::Binding;
use crate::triple_store;

/// Result of an UPDATE operation.
#[derive(Debug)]
pub struct UpdateResult {
    pub triples_inserted: usize,
    pub triples_deleted: usize,
}

/// Execute a SPARQL UPDATE statement.
///
/// `write_path` is required for the pattern-based ops, which evaluate their
/// WHERE clause through the SELECT executor; `storage` backs the actual
/// tombstone/insert writes.
pub async fn execute_update(
    update_str: &str,
    keyspace: &str,
    storage: &Arc<StorageEngine>,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<UpdateResult, SparqlError> {
    // The parser recurses per nesting level without a limit.
    crate::nesting::check_nesting(update_str)?;
    let update = spargebra::SparqlParser::new()
        .parse_update(update_str)
        .map_err(|e| SparqlError::Parse(format!("{e}")))?;

    // SPARQL 1.1 update atomicity (URS-QEC-X01): an update request that errors
    // must NOT have partially mutated the store. The executor below applies
    // operations sequentially with no rollback, so we MUST reject the whole
    // request up-front if ANY operation is unaddressable in this
    // single-graph-per-keyspace engine. Without this, a desugared
    // `COPY/MOVE <g> TO DEFAULT` would run its leading `Drop(DEFAULT)` and wipe
    // the default graph before the later unaddressable named-graph read fails.
    validate_update(&update)?;

    let mut total_inserted = 0usize;
    let mut total_deleted = 0usize;

    for op in &update.operations {
        match op {
            spargebra::GraphUpdateOperation::InsertData { data } => {
                for quad in data {
                    insert_quad(quad, keyspace, storage)?;
                    total_inserted += 1;
                }
            }
            spargebra::GraphUpdateOperation::DeleteData { data } => {
                for quad in data {
                    delete_ground_quad(quad, keyspace, storage)?;
                    total_deleted += 1;
                }
            }
            spargebra::GraphUpdateOperation::DeleteInsert {
                delete,
                insert,
                using: _,
                pattern,
            } => {
                let (d, i) = exec_delete_insert(
                    delete, insert, pattern, keyspace, storage, write_path, limits,
                )
                .await?;
                total_deleted += d;
                total_inserted += i;
            }
            spargebra::GraphUpdateOperation::Clear { graph, .. }
            | spargebra::GraphUpdateOperation::Drop { graph, .. } => {
                total_deleted += exec_clear(graph, keyspace, storage, write_path, limits).await?;
            }
            spargebra::GraphUpdateOperation::Load { .. } => {
                // URS-QEC-X01: LOAD fetches and parses an external RDF document.
                // This engine has no HTTP fetch + RDF-document parser pipeline,
                // so it cannot honor LOAD. Fail loud rather than silently
                // succeed with zero triples.
                return Err(SparqlError::Plan(
                    "SPARQL LOAD is not implemented: this endpoint has no RDF \
                     document fetch/parse pipeline"
                        .into(),
                ));
            }
            spargebra::GraphUpdateOperation::Create { .. } => {
                // CREATE GRAPH is a success no-op in the single-graph-per-keyspace
                // model: graphs are implicit (materialized by the partition-key
                // graph component on first insert), so the named graph "exists"
                // after this call with nothing to allocate. Per SPARQL 1.1,
                // CREATE on a fresh graph succeeds; we cannot cheaply detect
                // pre-existence, so (with or without SILENT) we report success.
            }
        }
    }

    Ok(UpdateResult {
        triples_inserted: total_inserted,
        triples_deleted: total_deleted,
    })
}

/// Validate EVERY operation of a parsed update before any of them runs, so an
/// update request that contains an unaddressable operation fails loud with zero
/// mutations (SPARQL 1.1 atomicity / URS-QEC-X01).
///
/// This is the single chokepoint that makes the spargebra `COPY/MOVE/ADD`
/// desugaring safe: those rewrite into a leading `Drop` plus a `DeleteInsert`
/// that reads one graph and writes another. By rejecting the whole request here
/// — before the destructive `Drop` executes — we guarantee no partial side
/// effects.
fn validate_update(update: &spargebra::Update) -> Result<(), SparqlError> {
    for op in &update.operations {
        match op {
            spargebra::GraphUpdateOperation::InsertData { .. }
            | spargebra::GraphUpdateOperation::DeleteData { .. } => {
                // `INSERT DATA` / `DELETE DATA` name their target graph on each
                // quad and the writer keys rows by that graph component, so a
                // named graph is addressable: nothing to reject. (The default
                // graph's storage component is set by the writer.)
            }
            spargebra::GraphUpdateOperation::DeleteInsert {
                delete,
                insert,
                pattern,
                ..
            } => {
                // Every DELETE/INSERT template and the WHERE read must denote
                // ONE graph. A request that reads one graph and writes another
                // is the COPY/MOVE/ADD desugaring, which this engine does not
                // honor — reject it up front, before any (possibly destructive)
                // op runs, so the request is atomic.
                resolve_delete_insert_graph(insert, delete, pattern)?;
            }
            spargebra::GraphUpdateOperation::Clear { graph, silent }
            | spargebra::GraphUpdateOperation::Drop { graph, silent } => {
                check_graph_target(graph, *silent)?;
            }
            spargebra::GraphUpdateOperation::Load { .. } => {
                return Err(SparqlError::Plan(
                    "SPARQL LOAD is not implemented: this endpoint has no RDF \
                     document fetch/parse pipeline"
                        .into(),
                ));
            }
            spargebra::GraphUpdateOperation::Create { .. } => {
                // CREATE is a success no-op (graphs are implicit); nothing to
                // validate. See the execution loop for the rationale.
            }
        }
    }
    Ok(())
}

/// Validate a `CLEAR`/`DROP` target for the atomicity check.
///
/// Every `CLEAR`/`DROP` this validator sees is non-destructive to *other*
/// data, so none can violate atomicity and this always passes. It exists so the
/// up-front pass is exhaustive over operation kinds. (`exec_clear` still treats
/// a named `CLEAR`/`DROP` target as an empty no-op; see its note.)
fn check_graph_target(_target: &GraphTarget, _silent: bool) -> Result<(), SparqlError> {
    Ok(())
}

/// The graph a `DELETE … INSERT … WHERE` request reads and writes.
///
/// A request is honored only when every DELETE template, every INSERT template,
/// and the WHERE read denote the SAME graph. That is what separates a
/// `DELETE WHERE { GRAPH <g> { … } }` (delete and read `<g>`) from the
/// `COPY`/`MOVE`/`ADD` desugaring, which reads one graph and writes another.
#[derive(Debug, Clone, PartialEq, Eq)]
enum GraphDenotation {
    Default,
    Named(String),
}

impl GraphDenotation {
    /// The graph component the storage key uses for this graph.
    fn storage_graph(&self) -> &str {
        match self {
            Self::Default => "default",
            Self::Named(n) => n.as_str(),
        }
    }
}

/// Resolve the single graph a `DELETE … INSERT … WHERE` request operates on, or
/// fail loud if it mixes graphs (the COPY/MOVE/ADD desugaring).
///
/// Returning the resolved graph is what lets the executor read, delete, and
/// insert against the SAME graph, so a named-graph `DELETE WHERE` is exact.
fn resolve_delete_insert_graph(
    insert: &[QuadPattern],
    delete: &[GroundQuadPattern],
    pattern: &spargebra::algebra::GraphPattern,
) -> Result<GraphDenotation, SparqlError> {
    let mut target: Option<GraphDenotation> = None;
    for tmpl in delete {
        let d = graph_denotation_pattern(&tmpl.graph_name, "DELETE")?;
        note_graph(&mut target, d)?;
    }
    for tmpl in insert {
        let d = graph_denotation_pattern(&tmpl.graph_name, "INSERT")?;
        note_graph(&mut target, d)?;
    }
    let mut where_graphs = Vec::new();
    collect_where_graphs(pattern, &mut where_graphs)?;
    if where_graphs.is_empty() {
        // A WHERE clause with no `GRAPH` block reads the default graph.
        where_graphs.push(GraphDenotation::Default);
    }
    for d in where_graphs {
        note_graph(&mut target, d)?;
    }
    Ok(target.unwrap_or(GraphDenotation::Default))
}

/// Fold `d` into the request's single target graph, or fail loud on a mix.
fn note_graph(target: &mut Option<GraphDenotation>, d: GraphDenotation) -> Result<(), SparqlError> {
    match target {
        None => {
            *target = Some(d);
            Ok(())
        }
        Some(existing) if *existing == d => Ok(()),
        Some(existing) => Err(SparqlError::Plan(format!(
            "cross-graph SPARQL UPDATE is not supported: this endpoint reads and writes one \
             graph per request, but this request mixes {} and {} (the ADD/MOVE/COPY \
             desugaring). Split it into same-graph operations.",
            describe_graph(existing),
            describe_graph(&d),
        ))),
    }
}

fn describe_graph(g: &GraphDenotation) -> String {
    match g {
        GraphDenotation::Default => "the default graph".to_string(),
        GraphDenotation::Named(n) => format!("named graph <{n}>"),
    }
}

/// The graph a DELETE/INSERT template targets.
fn graph_denotation_pattern(
    graph: &spargebra::term::GraphNamePattern,
    op: &str,
) -> Result<GraphDenotation, SparqlError> {
    match graph {
        spargebra::term::GraphNamePattern::DefaultGraph => Ok(GraphDenotation::Default),
        spargebra::term::GraphNamePattern::NamedNode(n) => {
            Ok(GraphDenotation::Named(n.as_str().to_string()))
        }
        spargebra::term::GraphNamePattern::Variable(v) => Err(SparqlError::Plan(format!(
            "{op} into a variable-bound graph (?{}) is not supported: name the graph with an IRI",
            v.as_str()
        ))),
    }
}

/// Collect the graphs a WHERE clause reads. A `GRAPH <iri>` block contributes
/// its IRI; a variable graph fails loud. An empty result means the WHERE reads
/// the default graph.
fn collect_where_graphs(
    pattern: &spargebra::algebra::GraphPattern,
    out: &mut Vec<GraphDenotation>,
) -> Result<(), SparqlError> {
    use spargebra::algebra::GraphPattern as G;
    match pattern {
        G::Graph { name, inner } => {
            match name {
                NamedNodePattern::NamedNode(n) => {
                    out.push(GraphDenotation::Named(n.as_str().to_string()));
                }
                NamedNodePattern::Variable(v) => {
                    return Err(SparqlError::Plan(format!(
                        "GRAPH ?{} (variable-bound graph read) is not supported: name the \
                         graph with an IRI",
                        v.as_str()
                    )));
                }
            }
            collect_where_graphs(inner, out)
        }
        G::Bgp { .. } | G::Path { .. } | G::Values { .. } => Ok(()),
        G::Join { left, right } | G::Union { left, right } | G::Minus { left, right } => {
            collect_where_graphs(left, out)?;
            collect_where_graphs(right, out)
        }
        G::LeftJoin { left, right, .. } => {
            collect_where_graphs(left, out)?;
            collect_where_graphs(right, out)
        }
        G::Filter { inner, .. }
        | G::Extend { inner, .. }
        | G::OrderBy { inner, .. }
        | G::Project { inner, .. }
        | G::Distinct { inner }
        | G::Reduced { inner }
        | G::Slice { inner, .. }
        | G::Group { inner, .. }
        | G::Service { inner, .. } => collect_where_graphs(inner, out),
    }
}

/// Execute `DELETE … INSERT … WHERE` (covers `DELETE WHERE`, where `insert` is
/// empty and `pattern` is the delete BGP). Returns `(deleted, inserted)`.
///
/// Per SPARQL 1.1, the WHERE pattern is evaluated first to bind all solutions,
/// then deletes are applied, then inserts. Each delete template is instantiated
/// per solution into a ground triple and tombstoned via the shared
/// `delete_ground_triple` path; each insert template is instantiated and
/// written via `write_triple`.
async fn exec_delete_insert(
    delete: &[GroundQuadPattern],
    insert: &[QuadPattern],
    pattern: &spargebra::algebra::GraphPattern,
    keyspace: &str,
    storage: &Arc<StorageEngine>,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<(usize, usize), SparqlError> {
    // Resolve the single graph this request reads and writes (already validated,
    // re-derived here so the executor keys its read, its deletes, and its
    // inserts against the SAME graph).
    let target = resolve_delete_insert_graph(insert, delete, pattern)?;
    let target_graph = target.storage_graph();

    // Same split as the read path: the keyspace names the table, the graph names
    // the partition. The scope is ISOLATED so the WHERE scan reads only the
    // target graph's partitions — `DELETE WHERE { GRAPH <g> { ?s ?p ?o } }` must
    // tombstone `<g>`'s triples and no other graph's, and an un-claused
    // `DELETE WHERE { ?s ?p ?o }` must clear the default graph only.
    let scope = crate::planner::TripleScope::isolated(keyspace, target_graph);
    let plan = crate::planner::plan_where(pattern, &scope)?;
    let solutions = crate::executor::execute_bindings(&plan, write_path, limits).await?;

    let mut deleted = 0usize;
    let mut inserted = 0usize;

    for sol in &solutions {
        for tmpl in delete {
            if let Some(triple) = instantiate_ground_delete(tmpl, sol) {
                delete_ground_triple(keyspace, target_graph, &triple, storage)?;
                deleted += 1;
            }
        }
    }
    for sol in &solutions {
        for tmpl in insert {
            if let Some(triple) = instantiate_insert(tmpl, sol) {
                write_triple(
                    keyspace,
                    target_graph,
                    &triple.subject,
                    &triple.predicate,
                    &triple.object,
                    &triple.obj_type,
                    &triple.datatype,
                    &triple.language,
                    storage,
                )?;
                inserted += 1;
            }
        }
    }

    Ok((deleted, inserted))
}

/// Execute `CLEAR`/`DROP` over the target graph: enumerate every triple in the
/// graph (full scan via the SELECT executor) and tombstone it. Returns the
/// number of triples deleted.
async fn exec_clear(
    target: &GraphTarget,
    keyspace: &str,
    storage: &Arc<StorageEngine>,
    write_path: &Arc<WritePath>,
    limits: &ExecutionLimits,
) -> Result<usize, SparqlError> {
    // The triple store is single-graph-per-keyspace; the partition-key graph
    // component is the keyspace. DEFAULT / NAMED / ALL therefore all resolve to
    // "every triple this engine can see for `keyspace`". A specific NamedNode
    // target must match the keyspace's graph, else there is nothing to clear.
    if let GraphTarget::NamedNode(n) = target {
        if n.as_str() != keyspace {
            return Ok(0);
        }
    }

    // Full scan: SELECT ?s ?p ?o WHERE { ?s ?p ?o } over the keyspace graph.
    let scan = spargebra::SparqlParser::new()
        .parse_query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
        .map_err(|e| SparqlError::Parse(format!("{e}")))?;
    let scope = crate::planner::TripleScope::new(keyspace, "default");
    let plan = crate::planner::plan_query(&scan, &scope)?;
    let solutions = crate::executor::execute_bindings(&plan, write_path, limits).await?;

    let mut deleted = 0usize;
    for sol in &solutions {
        let s = sol.get("s");
        let p = sol.get("p");
        let o = sol.get("o");
        if let (Some(s), Some(p), Some(o)) = (s, p, o) {
            let triple = GroundTriple {
                subject: s.value.clone(),
                predicate: p.value.clone(),
                object: o.value.clone(),
            };
            delete_ground_triple(keyspace, keyspace, &triple, storage)?;
            deleted += 1;
        }
    }

    Ok(deleted)
}

/// A ground triple to tombstone. The graph it lives in is supplied by the
/// caller (the request's resolved target graph).
struct GroundTriple {
    subject: String,
    predicate: String,
    object: String,
}

/// A ground triple to insert, carrying object typing metadata.
struct InsertTriple {
    subject: String,
    predicate: String,
    object: String,
    obj_type: String,
    datatype: Option<String>,
    language: Option<String>,
}

/// Resolve a `TermPattern` (subject/object position) against a solution into a
/// concrete string value, or `None` if a variable is unbound.
fn resolve_term(term: &TermPattern, sol: &HashMap<String, Binding>) -> Option<String> {
    match term {
        TermPattern::NamedNode(n) => Some(n.as_str().to_string()),
        TermPattern::Literal(l) => Some(l.value().to_string()),
        TermPattern::BlankNode(b) => Some(format!("_:{}", b.as_str())),
        TermPattern::Variable(v) => sol.get(v.as_str()).map(|b| b.value.clone()),
        TermPattern::Triple(_) => None,
    }
}

/// Resolve a ground (no blank nodes) term pattern against a solution.
fn resolve_ground_term(term: &GroundTermPattern, sol: &HashMap<String, Binding>) -> Option<String> {
    match term {
        GroundTermPattern::NamedNode(n) => Some(n.as_str().to_string()),
        GroundTermPattern::Literal(l) => Some(l.value().to_string()),
        GroundTermPattern::Variable(v) => sol.get(v.as_str()).map(|b| b.value.clone()),
        GroundTermPattern::Triple(_) => None,
    }
}

/// Resolve a predicate pattern (named node or variable) against a solution.
fn resolve_predicate(pred: &NamedNodePattern, sol: &HashMap<String, Binding>) -> Option<String> {
    match pred {
        NamedNodePattern::NamedNode(n) => Some(n.as_str().to_string()),
        NamedNodePattern::Variable(v) => sol.get(v.as_str()).map(|b| b.value.clone()),
    }
}

/// Instantiate a delete template against a solution into a ground triple.
/// Returns `None` (skipped, not an error) if any variable is unbound.
fn instantiate_ground_delete(
    tmpl: &GroundQuadPattern,
    sol: &HashMap<String, Binding>,
) -> Option<GroundTriple> {
    let subject = resolve_ground_term(&tmpl.subject, sol)?;
    let predicate = resolve_predicate(&tmpl.predicate, sol)?;
    let object = resolve_ground_term(&tmpl.object, sol)?;
    Some(GroundTriple {
        subject,
        predicate,
        object,
    })
}

/// Instantiate an insert template against a solution. Object typing follows the
/// template term (or the bound variable's type when the term is a variable).
fn instantiate_insert(tmpl: &QuadPattern, sol: &HashMap<String, Binding>) -> Option<InsertTriple> {
    let subject = resolve_term(&tmpl.subject, sol)?;
    let predicate = resolve_predicate(&tmpl.predicate, sol)?;
    let object = resolve_term(&tmpl.object, sol)?;
    let (obj_type, datatype, language) = object_metadata(&tmpl.object, sol);
    Some(InsertTriple {
        subject,
        predicate,
        object,
        obj_type,
        datatype,
        language,
    })
}

/// Determine `(object_type, datatype, language)` for an insert object term.
fn object_metadata(
    term: &TermPattern,
    sol: &HashMap<String, Binding>,
) -> (String, Option<String>, Option<String>) {
    match term {
        TermPattern::NamedNode(_) => ("uri".into(), None, None),
        TermPattern::BlankNode(_) => ("bnode".into(), None, None),
        TermPattern::Literal(l) => {
            let lang = l.language().map(|s| s.to_string());
            let dt = Some(l.datatype().as_str().to_string());
            ("literal".into(), dt, lang)
        }
        TermPattern::Variable(v) => match sol.get(v.as_str()) {
            Some(b) => (b.binding_type.clone(), b.datatype.clone(), b.lang.clone()),
            None => ("literal".into(), None, None),
        },
        TermPattern::Triple(_) => ("triple".into(), None, None),
    }
}

/// Tombstone a ground triple in `graph` (a `rdf_triples` partition component of
/// `keyspace`) via the shared low-level `StorageEngine` write path (same path
/// `DELETE DATA` uses).
fn delete_ground_triple(
    keyspace: &str,
    graph: &str,
    triple: &GroundTriple,
    storage: &Arc<StorageEngine>,
) -> Result<(), SparqlError> {
    tombstone_triple(
        keyspace,
        graph,
        &triple.subject,
        &triple.predicate,
        &triple.object,
        storage,
    )
}

/// Insert a single RDF quad into storage.
fn insert_quad(
    quad: &Quad,
    keyspace: &str,
    storage: &Arc<StorageEngine>,
) -> Result<(), SparqlError> {
    let graph = graph_name_str(&quad.graph_name);
    let subject = named_node_or_var_str(&quad.subject)?;
    let predicate = quad.predicate.as_str().to_string();
    let (object, obj_type, datatype, language) = term_to_rdf(&quad.object)?;

    write_triple(
        keyspace, &graph, &subject, &predicate, &object, &obj_type, &datatype, &language, storage,
    )
}

/// Delete a single ground quad from storage by writing a tombstone row.
fn delete_ground_quad(
    quad: &GroundQuad,
    keyspace: &str,
    storage: &Arc<StorageEngine>,
) -> Result<(), SparqlError> {
    let graph = match &quad.graph_name {
        spargebra::term::GraphName::NamedNode(n) => n.as_str().to_string(),
        spargebra::term::GraphName::DefaultGraph => "default".to_string(),
    };
    let subject = quad.subject.as_str().to_string();
    let predicate = quad.predicate.as_str().to_string();
    let object = match &quad.object {
        spargebra::term::GroundTerm::NamedNode(n) => n.as_str().to_string(),
        spargebra::term::GroundTerm::Literal(l) => l.value().to_string(),
        spargebra::term::GroundTerm::Triple(t) => format!("<<{t}>>"),
    };

    tombstone_triple(keyspace, &graph, &subject, &predicate, &object, storage)
}

/// Shared low-level tombstone primitive: write a deletion-marker row for the
/// triple `(graph, subject, predicate, object)` in `keyspace.rdf_triples`.
///
/// This is the ONE path every SPARQL delete (`DELETE DATA`, `DELETE WHERE`,
/// `DELETE/INSERT … WHERE`, `CLEAR`, `DROP`) funnels through, so the tombstone
/// encoding cannot drift across ops (URS-QEC-D07/X02).
fn tombstone_triple(
    keyspace: &str,
    graph: &str,
    subject: &str,
    predicate: &str,
    object: &str,
    storage: &Arc<StorageEngine>,
) -> Result<(), SparqlError> {
    let table_id = triple_store::triples_table_id(keyspace);
    let key = triple_store::partition_key(graph, subject);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let timestamp = now.as_micros() as i64;

    // Build clustering key matching the triple to delete.
    let clustering = encode_triple_clustering(predicate, object);

    // Write a pure row tombstone (deletion marker). The primary-key liveness
    // MUST be NONE — a LIVE liveness would resurrect the row as an empty live
    // row and the delete would be invisible to the read path (URS-QEC-D05).
    let row = Row {
        clustering,
        cells: vec![],
        deletion: DeletionTime {
            marked_for_delete_at: timestamp,
            local_deletion_time: now.as_secs() as u32,
        },
        primary_key_liveness: LivenessInfo::NONE,
    };

    storage.write(&table_id, &key, row, timestamp)?;
    Ok(())
}

/// Encode the `(predicate, object)` clustering key for an `rdf_triples` row.
///
/// Strict CQL composite-clustering encoding: `[u16 len][bytes]` per column with
/// NO separator byte (validated by `ferrosa-common::schema`). Shared by inserts
/// and tombstones so a tombstone's clustering exactly matches the live row it
/// must shadow (URS-QEC-D07/X02).
fn encode_triple_clustering(predicate: &str, object: &str) -> Vec<u8> {
    let mut clustering = Vec::new();
    clustering.extend_from_slice(&(predicate.len() as u16).to_be_bytes());
    clustering.extend_from_slice(predicate.as_bytes());
    clustering.extend_from_slice(&(object.len() as u16).to_be_bytes());
    clustering.extend_from_slice(object.as_bytes());
    clustering
}

#[allow(clippy::too_many_arguments)]
fn write_triple(
    keyspace: &str,
    graph: &str,
    subject: &str,
    predicate: &str,
    object: &str,
    obj_type: &str,
    datatype: &Option<String>,
    language: &Option<String>,
    storage: &Arc<StorageEngine>,
) -> Result<(), SparqlError> {
    let table_id = triple_store::triples_table_id(keyspace);
    let key = triple_store::partition_key(graph, subject);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;

    let clustering = encode_triple_clustering(predicate, object);

    let mut cells = Vec::new();
    cells.push((
        triple_store::COL_OBJECT_TYPE,
        CellValue::live(obj_type.as_bytes().to_vec(), timestamp),
    ));
    if let Some(dt) = datatype {
        cells.push((
            triple_store::COL_DATATYPE,
            CellValue::live(dt.as_bytes().to_vec(), timestamp),
        ));
    }
    if let Some(lang) = language {
        cells.push((
            triple_store::COL_LANGUAGE,
            CellValue::live(lang.as_bytes().to_vec(), timestamp),
        ));
    }

    let row = Row {
        clustering,
        cells,
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    };

    storage.write(&table_id, &key, row, timestamp)?;
    Ok(())
}

fn graph_name_str(gn: &spargebra::term::GraphName) -> String {
    match gn {
        spargebra::term::GraphName::NamedNode(n) => n.as_str().to_string(),
        spargebra::term::GraphName::DefaultGraph => "default".to_string(),
    }
}

fn named_node_or_var_str(
    subject: &spargebra::term::NamedOrBlankNode,
) -> Result<String, SparqlError> {
    match subject {
        spargebra::term::NamedOrBlankNode::NamedNode(n) => Ok(n.as_str().to_string()),
        spargebra::term::NamedOrBlankNode::BlankNode(b) => Ok(format!("_:{}", b.as_str())),
    }
}

fn term_to_rdf(
    term: &spargebra::term::Term,
) -> Result<(String, String, Option<String>, Option<String>), SparqlError> {
    match term {
        spargebra::term::Term::NamedNode(n) => {
            Ok((n.as_str().to_string(), "uri".into(), None, None))
        }
        spargebra::term::Term::Literal(lit) => {
            let lang = lit.language().map(|l| l.to_string());
            let dt = Some(lit.datatype().as_str().to_string());
            Ok((lit.value().to_string(), "literal".into(), dt, lang))
        }
        spargebra::term::Term::BlankNode(b) => {
            Ok((format!("_:{}", b.as_str()), "bnode".into(), None, None))
        }
        spargebra::term::Term::Triple(t) => Ok((format!("<<{t}>>"), "triple".into(), None, None)),
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn parse_insert_data() {
        let update = spargebra::SparqlParser::new()
            .parse_update("INSERT DATA { <http://ex/alice> <http://ex/name> \"Alice\" }");
        assert!(update.is_ok());
    }

    #[test]
    fn parse_delete_data() {
        let update = spargebra::SparqlParser::new()
            .parse_update("DELETE DATA { <http://ex/alice> <http://ex/name> \"Alice\" }");
        assert!(update.is_ok());
    }
}
