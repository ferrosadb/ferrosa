//! SPARQL query executor.
//!
//! Executes a [`QueryPlan`] against ferrosa's StorageEngine, producing
//! binding sets that are serialized into SPARQL results.

use std::collections::HashMap;
use std::sync::Arc;

use ferrosa_storage::engine::StorageEngine;

use crate::error::SparqlError;
use crate::planner::{QueryPlan, TripleOp};
use crate::results::{Binding, SparqlJsonResults};
use crate::triple_store;

/// Execute a query plan and return SPARQL JSON results.
pub fn execute(
    plan: &QueryPlan,
    storage: &Arc<StorageEngine>,
) -> Result<SparqlJsonResults, SparqlError> {
    let mut results = SparqlJsonResults::new(plan.projection.clone());

    // For Sprint 1: evaluate each triple pattern independently and join
    // on shared variables via nested-loop join. This is correct but not
    // optimized — future sprints add hash join and index-nested-loop.
    let mut binding_sets: Vec<HashMap<String, Binding>> = vec![HashMap::new()];

    for (tp, op) in &plan.ops {
        let mut new_bindings = Vec::new();

        let rows = fetch_triples(op, storage)?;

        for existing in &binding_sets {
            for (s, p, o, obj_type, datatype, lang) in &rows {
                let mut row = existing.clone();
                let mut compatible = true;

                // Bind or check subject variable.
                if let spargebra::term::TermPattern::Variable(var) = &tp.subject {
                    let name = var.as_str().to_string();
                    let binding = Binding {
                        binding_type: "uri".into(),
                        value: s.clone(),
                        datatype: None,
                        lang: None,
                    };
                    if let Some(existing_val) = row.get(&name) {
                        if existing_val.value != *s {
                            compatible = false;
                        }
                    } else {
                        row.insert(name, binding);
                    }
                }

                // Bind or check predicate variable.
                if let spargebra::term::NamedNodePattern::Variable(var) = &tp.predicate {
                    let name = var.as_str().to_string();
                    let binding = Binding {
                        binding_type: "uri".into(),
                        value: p.clone(),
                        datatype: None,
                        lang: None,
                    };
                    if let Some(existing_val) = row.get(&name) {
                        if existing_val.value != *p {
                            compatible = false;
                        }
                    } else {
                        row.insert(name, binding);
                    }
                }

                // Bind or check object variable.
                if let spargebra::term::TermPattern::Variable(var) = &tp.object {
                    let name = var.as_str().to_string();
                    let binding = Binding {
                        binding_type: obj_type.clone(),
                        value: o.clone(),
                        datatype: datatype.clone(),
                        lang: lang.clone(),
                    };
                    if let Some(existing_val) = row.get(&name) {
                        if existing_val.value != *o {
                            compatible = false;
                        }
                    } else {
                        row.insert(name, binding);
                    }
                }

                if compatible {
                    new_bindings.push(row);
                }
            }
        }

        binding_sets = new_bindings;
    }

    // Apply OFFSET and LIMIT.
    let start = plan.offset.unwrap_or(0);
    let end = plan.limit.map(|l| start + l).unwrap_or(binding_sets.len());
    let end = end.min(binding_sets.len());

    for row in &binding_sets[start..end] {
        // Project only the requested variables.
        let projected: HashMap<String, Binding> = plan
            .projection
            .iter()
            .filter_map(|var| row.get(var).map(|b| (var.clone(), b.clone())))
            .collect();
        results.add_row(projected);
    }

    Ok(results)
}

/// A fetched triple: (subject, predicate, object, object_type, datatype, language).
type FetchedTriple = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
);

/// Fetch triples from storage for a single triple pattern operation.
fn fetch_triples(
    op: &TripleOp,
    storage: &Arc<StorageEngine>,
) -> Result<Vec<FetchedTriple>, SparqlError> {
    let table_id = match op {
        TripleOp::SubjectLookup { graph, .. }
        | TripleOp::PredicateScan { graph, .. }
        | TripleOp::ObjectScan { graph, .. }
        | TripleOp::FullScan { graph, .. } => {
            // For now, use "default" keyspace. The graph name maps to the
            // keyspace in a real deployment. Sprint 2 will handle named graphs.
            let _ = graph;
            triple_store::triples_table_id("rdf")
        }
    };

    let partitions = match op {
        TripleOp::SubjectLookup { graph, subject, .. } => {
            let key = triple_store::partition_key(graph, subject);
            match storage.read(&table_id, &key)? {
                Some(p) => vec![p],
                None => vec![],
            }
        }
        TripleOp::PredicateScan { .. }
        | TripleOp::ObjectScan { .. }
        | TripleOp::FullScan { .. } => {
            // Full range scan — Sprint 2 will use secondary indexes.
            storage.read_range(&table_id, None, None, 10_000)?
        }
    };

    let mut triples = Vec::new();

    for partition in &partitions {
        // Extract subject from partition key (simplified for Sprint 1).
        let subject = String::from_utf8_lossy(partition.key.key.as_bytes()).to_string();

        for row in &partition.rows {
            // Extract predicate and object from clustering key + cells.
            // This is a simplified extraction — real implementation needs
            // proper CQL composite key decoding.
            let predicate = extract_clustering_string(&row.clustering, 0);
            let object = extract_clustering_string(&row.clustering, 1);

            let obj_type = row
                .cells
                .iter()
                .find(|(idx, _)| *idx == triple_store::COL_OBJECT_TYPE)
                .and_then(|(_, cell)| cell.value.as_ref())
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_else(|| "literal".into());

            let datatype = row
                .cells
                .iter()
                .find(|(idx, _)| *idx == triple_store::COL_DATATYPE)
                .and_then(|(_, cell)| cell.value.as_ref())
                .map(|v| String::from_utf8_lossy(v).to_string());

            let language = row
                .cells
                .iter()
                .find(|(idx, _)| *idx == triple_store::COL_LANGUAGE)
                .and_then(|(_, cell)| cell.value.as_ref())
                .map(|v| String::from_utf8_lossy(v).to_string());

            triples.push((
                subject.clone(),
                predicate,
                object,
                obj_type,
                datatype,
                language,
            ));
        }
    }

    // Apply predicate filter for SubjectLookup.
    if let TripleOp::SubjectLookup {
        predicate_filter: Some(pred),
        ..
    } = op
    {
        triples.retain(|(_, p, _, _, _, _)| p == pred);
    }

    Ok(triples)
}

/// Extract a string component from a CQL clustering key at the given position.
///
/// Clustering keys are encoded as length-prefixed components:
///   [u16 len][bytes][0x00 separator]...
fn extract_clustering_string(clustering: &[u8], position: usize) -> String {
    let mut offset = 0;
    for i in 0..=position {
        if offset + 2 > clustering.len() {
            return String::new();
        }
        let len = u16::from_be_bytes([clustering[offset], clustering[offset + 1]]) as usize;
        offset += 2;
        if i == position {
            if offset + len > clustering.len() {
                return String::new();
            }
            return String::from_utf8_lossy(&clustering[offset..offset + len]).to_string();
        }
        offset += len;
        // Skip end-of-component byte if present.
        if offset < clustering.len() && clustering[offset] == 0 {
            offset += 1;
        }
    }
    String::new()
}
