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
                return Err(SparqlError::Plan("SPARQL LOAD is not implemented".into()));
            }
            spargebra::GraphUpdateOperation::Create { .. } => {
                return Err(SparqlError::Plan("SPARQL CREATE is not implemented".into()));
            }
        }
    }

    Ok(UpdateResult {
        triples_inserted: total_inserted,
        triples_deleted: total_deleted,
    })
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
