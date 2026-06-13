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
) -> Result<UpdateResult, SparqlError> {
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
    validate_update(&update, keyspace)?;

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
                let (d, i) =
                    exec_delete_insert(delete, insert, pattern, keyspace, storage, write_path)
                        .await?;
                total_deleted += d;
                total_inserted += i;
            }
            spargebra::GraphUpdateOperation::Clear { graph, .. }
            | spargebra::GraphUpdateOperation::Drop { graph, .. } => {
                total_deleted += exec_clear(graph, keyspace, storage, write_path).await?;
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
/// that reads from / writes to a named graph this engine cannot address. By
/// rejecting the whole request here — before the destructive `Drop` executes —
/// we guarantee no partial side effects.
fn validate_update(update: &spargebra::Update, keyspace: &str) -> Result<(), SparqlError> {
    for op in &update.operations {
        match op {
            spargebra::GraphUpdateOperation::InsertData { data } => {
                for quad in data {
                    check_graph_name(&quad.graph_name, keyspace, "INSERT DATA")?;
                }
            }
            spargebra::GraphUpdateOperation::DeleteData { data } => {
                for quad in data {
                    check_graph_name(&quad.graph_name, keyspace, "DELETE DATA")?;
                }
            }
            spargebra::GraphUpdateOperation::DeleteInsert {
                delete,
                insert,
                pattern,
                ..
            } => {
                for tmpl in delete {
                    check_quad_graph_pattern(&tmpl.graph_name, keyspace, "DELETE")?;
                }
                for tmpl in insert {
                    check_quad_graph_pattern(&tmpl.graph_name, keyspace, "INSERT")?;
                }
                // Reject a WHERE clause that reads from a named graph
                // (`GraphPattern::Graph`), e.g. the COPY/MOVE/ADD source.
                check_pattern_graph_reads(pattern, keyspace)?;
            }
            spargebra::GraphUpdateOperation::Clear { graph, silent }
            | spargebra::GraphUpdateOperation::Drop { graph, silent } => {
                check_graph_target(graph, keyspace, *silent)?;
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

/// Validate a concrete (`InsertData`/`DeleteData`) quad's graph name.
fn check_graph_name(
    graph: &spargebra::term::GraphName,
    keyspace: &str,
    op: &str,
) -> Result<(), SparqlError> {
    match graph {
        spargebra::term::GraphName::DefaultGraph => Ok(()),
        spargebra::term::GraphName::NamedNode(n) if n.as_str() == keyspace => Ok(()),
        spargebra::term::GraphName::NamedNode(n) => {
            Err(named_graph_unsupported(op, n.as_str(), keyspace))
        }
    }
}

/// Validate a `CLEAR`/`DROP` target for the atomicity check.
///
/// All `CLEAR`/`DROP` targets are *safe* in this engine: `DefaultGraph`,
/// `NamedGraphs`, and `AllGraphs` resolve to "every triple in this keyspace"
/// (clearable), a `NamedNode` equal to the keyspace is that same graph, and any
/// other `NamedNode` simply does not exist here, so `exec_clear` deletes nothing
/// (a true no-op — it never fakes or mis-targets a deletion, per URS-QEC-X01 and
/// the M1 `clear_non_matching_named_graph_deletes_nothing` contract).
///
/// Because every `CLEAR`/`DROP` is non-destructive-to-other-data, none of them
/// can violate atomicity, so this validation always passes. It exists so the
/// up-front pass is exhaustive over operation kinds (and as the place to tighten
/// non-SILENT semantics later, if the single-graph model ever gains real named
/// graphs).
fn check_graph_target(
    _target: &GraphTarget,
    _keyspace: &str,
    _silent: bool,
) -> Result<(), SparqlError> {
    Ok(())
}

/// Recursively reject any `GraphPattern::Graph` reading from a graph other than
/// the keyspace default graph. Such a named-graph read (produced by the
/// COPY/MOVE/ADD desugaring, or by an explicit `GRAPH <g> { … }` block in a
/// WHERE clause) is not addressable here and must fail loud BEFORE any
/// preceding destructive op runs.
fn check_pattern_graph_reads(
    pattern: &spargebra::algebra::GraphPattern,
    keyspace: &str,
) -> Result<(), SparqlError> {
    use spargebra::algebra::GraphPattern as G;
    match pattern {
        G::Graph { name, inner } => {
            match name {
                NamedNodePattern::NamedNode(n) if n.as_str() == keyspace => {}
                NamedNodePattern::NamedNode(n) => {
                    return Err(SparqlError::Plan(format!(
                        "reading from named graph <{}> is not supported: this endpoint \
                         exposes a single graph per keyspace ('{keyspace}') — so \
                         ADD/MOVE/COPY from a named graph cannot be honored",
                        n.as_str()
                    )));
                }
                NamedNodePattern::Variable(v) => {
                    return Err(SparqlError::Plan(format!(
                        "GRAPH ?{} (variable-bound graph read) is not supported: this \
                         endpoint exposes a single graph per keyspace ('{keyspace}')",
                        v.as_str()
                    )));
                }
            }
            check_pattern_graph_reads(inner, keyspace)
        }
        G::Bgp { .. } | G::Path { .. } | G::Values { .. } => Ok(()),
        G::Join { left, right } | G::Union { left, right } | G::Minus { left, right } => {
            check_pattern_graph_reads(left, keyspace)?;
            check_pattern_graph_reads(right, keyspace)
        }
        G::LeftJoin { left, right, .. } => {
            check_pattern_graph_reads(left, keyspace)?;
            check_pattern_graph_reads(right, keyspace)
        }
        G::Filter { inner, .. }
        | G::Extend { inner, .. }
        | G::OrderBy { inner, .. }
        | G::Project { inner, .. }
        | G::Distinct { inner }
        | G::Reduced { inner }
        | G::Slice { inner, .. }
        | G::Group { inner, .. }
        | G::Service { inner, .. } => check_pattern_graph_reads(inner, keyspace),
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
) -> Result<(usize, usize), SparqlError> {
    // URS-QEC-X01: a delete or insert template whose target graph is a named
    // graph distinct from this keyspace's default graph is NOT addressable in
    // the single-graph-per-keyspace model (the read/write path keys the table
    // by keyspace and does not filter by the graph partition-key component).
    // Honoring it by writing into the default graph would be a silent wrong
    // result, so we fail loud BEFORE evaluating the WHERE or applying any
    // mutation. This is what makes the spargebra ADD/MOVE/COPY desugaring
    // (which targets named graphs) fail loud instead of silently operating on
    // the wrong graph. (A named-graph *read* — `GraphPattern::Graph` in the
    // WHERE clause, produced when COPY/MOVE/ADD source is a named graph — is
    // separately rejected by the planner.)
    for tmpl in delete {
        check_quad_graph_pattern(&tmpl.graph_name, keyspace, "DELETE")?;
    }
    for tmpl in insert {
        check_quad_graph_pattern(&tmpl.graph_name, keyspace, "INSERT")?;
    }

    let plan = crate::planner::plan_where(pattern, keyspace)?;
    let solutions = crate::executor::execute_bindings(&plan, write_path).await?;

    let mut deleted = 0usize;
    let mut inserted = 0usize;

    for sol in &solutions {
        for tmpl in delete {
            if let Some(triple) = instantiate_ground_delete(tmpl, sol) {
                delete_ground_triple(keyspace, &triple, storage)?;
                deleted += 1;
            }
        }
    }
    for sol in &solutions {
        for tmpl in insert {
            if let Some(triple) = instantiate_insert(tmpl, sol) {
                write_triple(
                    keyspace,
                    keyspace,
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
    let plan = crate::planner::plan_query(&scan, keyspace)?;
    let solutions = crate::executor::execute_bindings(&plan, write_path).await?;

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
            delete_ground_triple(keyspace, &triple, storage)?;
            deleted += 1;
        }
    }

    Ok(deleted)
}

/// A ground triple to tombstone (graph is implied by the keyspace).
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

/// Validate that a quad template's target graph (a [`GraphNamePattern`], as
/// carried by both INSERT and DELETE templates in a `DeleteInsert`) is
/// addressable in this single-graph-per-keyspace engine.
///
/// `DefaultGraph` maps to the keyspace's default graph (always OK). A
/// `NamedNode` equal to the keyspace is the same graph (OK). Any other named
/// graph — or a variable-bound graph — is not addressable: return a fail-loud
/// error instead of silently operating on the default graph (URS-QEC-X01).
fn check_quad_graph_pattern(
    graph: &spargebra::term::GraphNamePattern,
    keyspace: &str,
    op: &str,
) -> Result<(), SparqlError> {
    match graph {
        spargebra::term::GraphNamePattern::DefaultGraph => Ok(()),
        spargebra::term::GraphNamePattern::NamedNode(n) if n.as_str() == keyspace => Ok(()),
        spargebra::term::GraphNamePattern::NamedNode(n) => {
            Err(named_graph_unsupported(op, n.as_str(), keyspace))
        }
        spargebra::term::GraphNamePattern::Variable(v) => Err(SparqlError::Plan(format!(
            "{op} into a variable-bound graph (?{}) is not supported: this endpoint \
             exposes a single graph per keyspace ('{keyspace}')",
            v.as_str()
        ))),
    }
}

/// Build the standard fail-loud error for an unaddressable named graph.
fn named_graph_unsupported(op: &str, graph: &str, keyspace: &str) -> SparqlError {
    SparqlError::Plan(format!(
        "{op} into named graph <{graph}> is not supported: this endpoint exposes a \
         single graph per keyspace ('{keyspace}'); named graphs distinct from it \
         are not addressable (so ADD/MOVE/COPY across graphs cannot be honored)"
    ))
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

/// Tombstone a ground triple in `keyspace`'s graph via the shared low-level
/// `StorageEngine` write path (same path `DELETE DATA` uses).
fn delete_ground_triple(
    keyspace: &str,
    triple: &GroundTriple,
    storage: &Arc<StorageEngine>,
) -> Result<(), SparqlError> {
    tombstone_triple(
        keyspace,
        keyspace,
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
