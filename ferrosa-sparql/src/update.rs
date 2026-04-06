//! SPARQL UPDATE operations: INSERT DATA, DELETE DATA.

use std::sync::Arc;

use spargebra::term::{GroundQuad, Quad};

use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::engine::StorageEngine;

use crate::error::SparqlError;
use crate::triple_store;

/// Result of an UPDATE operation.
#[derive(Debug)]
pub struct UpdateResult {
    pub triples_inserted: usize,
    pub triples_deleted: usize,
}

/// Execute a SPARQL UPDATE statement.
pub fn execute_update(
    update_str: &str,
    keyspace: &str,
    storage: &Arc<StorageEngine>,
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
            _ => {
                return Err(SparqlError::Plan(
                    "only INSERT DATA and DELETE DATA are currently supported".into(),
                ));
            }
        }
    }

    Ok(UpdateResult {
        triples_inserted: total_inserted,
        triples_deleted: total_deleted,
    })
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

/// Delete a single ground quad from storage.
fn delete_ground_quad(
    quad: &GroundQuad,
    _keyspace: &str,
    _storage: &Arc<StorageEngine>,
) -> Result<(), SparqlError> {
    // Deletion requires reading the partition, finding the matching row,
    // and writing a tombstone. Partially implemented — log for now.
    let _subject = quad.subject.as_str().to_string();
    tracing::warn!("DELETE DATA: tombstone write not yet implemented");
    Ok(())
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

    let mut clustering = Vec::new();
    clustering.extend_from_slice(&(predicate.len() as u16).to_be_bytes());
    clustering.extend_from_slice(predicate.as_bytes());
    clustering.push(0);
    clustering.extend_from_slice(&(object.len() as u16).to_be_bytes());
    clustering.extend_from_slice(object.as_bytes());
    clustering.push(0);

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
