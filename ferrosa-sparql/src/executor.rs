//! SPARQL query executor.
//!
//! Executes a [`QueryPlan`] against ferrosa's StorageEngine, producing
//! binding sets that are serialized into SPARQL results.

use std::collections::HashMap;
use std::sync::Arc;

use ferrosa_sstable::types::Row;
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
    let mut binding_sets = evaluate_triple_patterns(plan, storage)?;

    // Apply FILTER expressions to binding sets.
    if !plan.filters.is_empty() {
        binding_sets.retain(|row| {
            plan.filters
                .iter()
                .all(|expr| crate::filter::eval_filter(expr, row))
        });
    }

    // BUG-S13: apply ORDER BY.
    apply_order_by(&mut binding_sets, &plan.order_by);

    // BUG-S13: apply DISTINCT.
    if plan.distinct {
        apply_distinct(&mut binding_sets, &plan.projection);
    }

    // BUG-S5 fix: clamp start to len so slicing never panics.
    let start = plan.offset.unwrap_or(0).min(binding_sets.len());
    let end = plan.limit.map(|l| start + l).unwrap_or(binding_sets.len());
    let end = end.min(binding_sets.len());

    let mut results = SparqlJsonResults::new(plan.projection.clone());
    for row in &binding_sets[start..end] {
        let projected: HashMap<String, Binding> = plan
            .projection
            .iter()
            .filter_map(|var| row.get(var).map(|b| (var.clone(), b.clone())))
            .collect();
        results.add_row(projected);
    }

    Ok(results)
}

/// Evaluate all triple patterns via nested-loop join, returning binding sets.
fn evaluate_triple_patterns(
    plan: &QueryPlan,
    storage: &Arc<StorageEngine>,
) -> Result<Vec<HashMap<String, Binding>>, SparqlError> {
    let mut binding_sets: Vec<HashMap<String, Binding>> = vec![HashMap::new()];

    for (tp, op) in &plan.ops {
        let new_bindings = match op {
            TripleOp::PropertyPath {
                graph,
                subject,
                path,
                object,
            } => evaluate_path_op(graph, subject, path, object, &binding_sets, storage)?,
            _ => evaluate_standard_op(tp, op, &binding_sets, storage)?,
        };
        binding_sets = new_bindings;
    }

    Ok(binding_sets)
}

/// Evaluate a property path op via BFS traversal.
fn evaluate_path_op(
    graph: &str,
    subject: &spargebra::term::TermPattern,
    path: &spargebra::algebra::PropertyPathExpression,
    object: &spargebra::term::TermPattern,
    existing_bindings: &[HashMap<String, Binding>],
    storage: &Arc<StorageEngine>,
) -> Result<Vec<HashMap<String, Binding>>, SparqlError> {
    let results =
        crate::property_path::evaluate_property_path(subject, path, object, graph, storage)?;
    let path_bindings = crate::property_path::path_results_to_bindings(subject, object, &results);

    let mut new_bindings = Vec::new();
    for existing in existing_bindings {
        for pb in &path_bindings {
            if let Some(merged) = try_merge_bindings(existing, pb) {
                new_bindings.push(merged);
            }
        }
    }
    Ok(new_bindings)
}

/// Evaluate a standard (non-path) triple pattern op.
fn evaluate_standard_op(
    tp: &spargebra::term::TriplePattern,
    op: &TripleOp,
    existing_bindings: &[HashMap<String, Binding>],
    storage: &Arc<StorageEngine>,
) -> Result<Vec<HashMap<String, Binding>>, SparqlError> {
    let rows = fetch_triples(op, storage)?;
    let mut new_bindings = Vec::new();
    for existing in existing_bindings {
        for triple in &rows {
            if let Some(row) = try_bind_triple(tp, triple, existing) {
                new_bindings.push(row);
            }
        }
    }
    Ok(new_bindings)
}

/// Merge two binding sets if compatible (no conflicting values).
fn try_merge_bindings(
    a: &HashMap<String, Binding>,
    b: &HashMap<String, Binding>,
) -> Option<HashMap<String, Binding>> {
    let mut merged = a.clone();
    for (key, val) in b {
        if !try_insert_binding(&mut merged, key, val) {
            return None;
        }
    }
    Some(merged)
}

/// Try to bind a fetched triple into an existing binding row.
/// Returns `None` if the triple is incompatible with existing bindings.
///
/// BUG-S4 fix: checks both value AND binding_type for compatibility.
fn try_bind_triple(
    tp: &spargebra::term::TriplePattern,
    triple: &FetchedTriple,
    existing: &HashMap<String, Binding>,
) -> Option<HashMap<String, Binding>> {
    let (s, p, o, obj_type, datatype, lang) = triple;
    let mut row = existing.clone();

    // Bind or check subject.
    // BUG-S11 fix: detect blank nodes by `_:` prefix instead of assuming URI.
    if let spargebra::term::TermPattern::Variable(var) = &tp.subject {
        let subject_type = if s.starts_with("_:") { "bnode" } else { "uri" };
        let binding = Binding {
            binding_type: subject_type.into(),
            value: s.clone(),
            datatype: None,
            lang: None,
        };
        if !try_insert_binding(&mut row, var.as_str(), &binding) {
            return None;
        }
    }

    // Bind or check predicate.
    if let spargebra::term::NamedNodePattern::Variable(var) = &tp.predicate {
        let binding = Binding {
            binding_type: "uri".into(),
            value: p.clone(),
            datatype: None,
            lang: None,
        };
        if !try_insert_binding(&mut row, var.as_str(), &binding) {
            return None;
        }
    }

    // Bind or check object.
    if let spargebra::term::TermPattern::Variable(var) = &tp.object {
        let binding = Binding {
            binding_type: obj_type.clone(),
            value: o.clone(),
            datatype: datatype.clone(),
            lang: lang.clone(),
        };
        if !try_insert_binding(&mut row, var.as_str(), &binding) {
            return None;
        }
    }

    Some(row)
}

/// Insert a binding into a row, or check compatibility if already present.
///
/// BUG-S4 fix: compares both `value` and `binding_type`.
/// Returns `false` if the existing binding is incompatible.
fn try_insert_binding(row: &mut HashMap<String, Binding>, name: &str, binding: &Binding) -> bool {
    if let Some(existing) = row.get(name) {
        existing.value == binding.value && existing.binding_type == binding.binding_type
    } else {
        row.insert(name.to_string(), binding.clone());
        true
    }
}

/// Sort binding sets by ORDER BY conditions (BUG-S13).
fn apply_order_by(
    binding_sets: &mut [HashMap<String, Binding>],
    order_by: &[crate::planner::OrderCondition],
) {
    if order_by.is_empty() {
        return;
    }
    binding_sets.sort_by(|a, b| {
        for cond in order_by {
            let va = a.get(&cond.variable).map(|b| b.value.as_str());
            let vb = b.get(&cond.variable).map(|b| b.value.as_str());
            let ord = va.cmp(&vb);
            let ord = if cond.ascending { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Remove duplicate binding rows (BUG-S13).
fn apply_distinct(binding_sets: &mut Vec<HashMap<String, Binding>>, projection: &[String]) {
    let mut seen = std::collections::HashSet::new();
    binding_sets.retain(|row| {
        let key: Vec<(&str, &str, &str)> = projection
            .iter()
            .map(|var| {
                row.get(var)
                    .map(|b| (var.as_str(), b.value.as_str(), b.binding_type.as_str()))
                    .unwrap_or((var.as_str(), "", ""))
            })
            .collect();
        let key_str = format!("{key:?}");
        seen.insert(key_str)
    });
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

/// Maximum number of partitions returned by a range scan.
const SCAN_ROW_CAP: usize = 10_000;

/// Name of the secondary index on the object column.
const OBJECT_INDEX_NAME: &str = "rdf_triples_object_idx";

/// Attempt to fetch partitions matching an object value via secondary index.
///
/// Falls back to a full range scan with post-fetch filtering if the index
/// does not exist. Logs a warning on fallback and on truncation.
fn fetch_by_object_index(
    table_id: &ferrosa_storage::TableId,
    object: &str,
    storage: &Arc<StorageEngine>,
) -> Result<Vec<ferrosa_sstable::types::Partition>, SparqlError> {
    let index_key = ferrosa_index::IndexKey(object.as_bytes().to_vec());
    let indexed = storage.read_by_index(table_id, OBJECT_INDEX_NAME, &index_key)?;
    if !indexed.is_empty() {
        return Ok(indexed);
    }

    // Fallback: no index or no matches — use range scan.
    tracing::warn!(
        object,
        "ObjectScan: no secondary index hit; falling back to full scan with filtering"
    );
    let results = storage.read_range(table_id, None, None, SCAN_ROW_CAP)?;
    if results.len() >= SCAN_ROW_CAP {
        tracing::warn!(
            cap = SCAN_ROW_CAP,
            "ObjectScan results truncated at row cap; results may be incomplete"
        );
    }
    Ok(results)
}

/// Fetch triples from storage for a single triple pattern operation.
fn fetch_triples(
    op: &TripleOp,
    storage: &Arc<StorageEngine>,
) -> Result<Vec<FetchedTriple>, SparqlError> {
    // BUG-S2 fix: use the graph from the execution plan, not hardcoded "rdf".
    let graph = match op {
        TripleOp::SubjectLookup { graph, .. }
        | TripleOp::PredicateScan { graph, .. }
        | TripleOp::ObjectScan { graph, .. }
        | TripleOp::FullScan { graph, .. } => graph.as_str(),
        TripleOp::PropertyPath { .. } => {
            // PropertyPath ops are handled in evaluate_path_op; this is unreachable.
            return Ok(vec![]);
        }
    };
    let table_id = triple_store::triples_table_id(graph);

    let partitions = match op {
        TripleOp::SubjectLookup { graph, subject, .. } => {
            let key = triple_store::partition_key(graph, subject);
            // TODO: route through coordinator for cluster mode — storage.read()
            // only checks LOCAL storage, returning empty when RF=1 and this node
            // doesn't own the token. Requires threading WritePath into the SPARQL
            // executor (see ferrosa-cluster P0 cluster-read bug).
            match storage.read(&table_id, &key)? {
                Some(p) => vec![p],
                None => vec![],
            }
        }
        TripleOp::ObjectScan { object, .. } => fetch_by_object_index(&table_id, object, storage)?,
        TripleOp::PredicateScan { .. } | TripleOp::FullScan { .. } => {
            let results = storage.read_range(&table_id, None, None, SCAN_ROW_CAP)?;
            if results.len() >= SCAN_ROW_CAP {
                tracing::warn!(
                    cap = SCAN_ROW_CAP,
                    "scan results truncated at row cap; results may be incomplete"
                );
            }
            results
        }
        TripleOp::PropertyPath { .. } => return Ok(vec![]),
    };

    let mut triples = Vec::new();
    for partition in &partitions {
        // BUG-S3 fix: decode composite partition key (graph, subject) properly.
        let subject = extract_subject_from_partition_key(partition.key.key.as_bytes());
        extract_rows_from_partition(&partition.rows, &subject, &mut triples);
    }

    apply_scan_filters(op, &mut triples);
    Ok(triples)
}

/// BUG-S3 fix: Properly decode CQL composite partition key to extract the subject.
///
/// Composite key format: `[u16 len][bytes][0x00]` repeated for each component.
/// Component 0 = graph, component 1 = subject.
fn extract_subject_from_partition_key(key_bytes: &[u8]) -> String {
    match extract_composite_component(key_bytes, 1) {
        Some(s) => s,
        None => {
            tracing::warn!(
                key_len = key_bytes.len(),
                "failed to decode subject from composite partition key; \
                 falling back to raw bytes"
            );
            String::from_utf8_lossy(key_bytes).to_string()
        }
    }
}

/// Extract the Nth component from a CQL composite key.
///
/// Format: `[u16 len][bytes][0x00 separator]` per component.
fn extract_composite_component(data: &[u8], position: usize) -> Option<String> {
    let mut offset = 0;
    for i in 0..=position {
        if offset + 2 > data.len() {
            return None;
        }
        let len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        if offset + len > data.len() {
            return None;
        }
        if i == position {
            return Some(String::from_utf8_lossy(&data[offset..offset + len]).to_string());
        }
        offset += len;
        // Skip end-of-component separator byte.
        if offset < data.len() && data[offset] == 0 {
            offset += 1;
        }
    }
    None
}

/// Extract triples from partition rows into the output vec.
fn extract_rows_from_partition(rows: &[Row], subject: &str, triples: &mut Vec<FetchedTriple>) {
    for row in rows {
        let predicate = extract_clustering_string(&row.clustering, 0);
        let object = extract_clustering_string(&row.clustering, 1);

        // BUG-S18 fix: validate object type against known values.
        let raw_obj_type = row
            .cells
            .iter()
            .find(|(idx, _)| *idx == triple_store::COL_OBJECT_TYPE)
            .and_then(|(_, cell)| cell.value.as_ref())
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_else(|| "literal".into());
        let obj_type = match raw_obj_type.as_str() {
            "uri" | "literal" | "bnode" => raw_obj_type,
            other => {
                tracing::warn!(value = other, "invalid object type, defaulting to literal");
                "literal".into()
            }
        };

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
            subject.to_string(),
            predicate,
            object,
            obj_type,
            datatype,
            language,
        ));
    }
}

/// Apply post-fetch filters for scan operations.
///
/// BUG-S8 fix: ObjectScan now filters by object value.
/// BUG-S9 fix: PredicateScan now filters by predicate value.
fn apply_scan_filters(op: &TripleOp, triples: &mut Vec<FetchedTriple>) {
    match op {
        TripleOp::SubjectLookup {
            predicate_filter: Some(pred),
            ..
        } => {
            triples.retain(|(_, p, _, _, _, _)| p == pred);
        }
        TripleOp::PredicateScan { predicate, .. } => {
            triples.retain(|(_, p, _, _, _, _)| p == predicate);
        }
        TripleOp::ObjectScan { object, .. } => {
            triples.retain(|(_, _, o, _, _, _)| o == object);
        }
        _ => {}
    }
}

/// Extract a string component from a CQL clustering key at the given position.
///
/// Clustering keys are encoded as length-prefixed components:
///   `[u16 len][bytes][0x00 separator]...`
///
/// BUG-S10 fix: logs warnings on malformed/truncated keys instead of
/// returning empty strings silently.
fn extract_clustering_string(clustering: &[u8], position: usize) -> String {
    let mut offset = 0;
    for i in 0..=position {
        if offset + 2 > clustering.len() {
            tracing::warn!(
                position,
                clustering_len = clustering.len(),
                byte_offset = offset,
                "clustering key too short: cannot read length prefix for component"
            );
            return String::new();
        }
        let len = u16::from_be_bytes([clustering[offset], clustering[offset + 1]]) as usize;
        offset += 2;
        if i == position {
            if offset + len > clustering.len() {
                tracing::warn!(
                    position,
                    component_len = len,
                    remaining = clustering.len() - offset,
                    "clustering key truncated: component length exceeds remaining bytes"
                );
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
    tracing::warn!(
        position,
        clustering_len = clustering.len(),
        "clustering key: fell through component loop without finding position"
    );
    String::new()
}

/// Extract the subject string from a partition's composite key.
///
/// Public wrapper for use by [`crate::property_path`].
pub fn extract_subject_from_key(partition: &ferrosa_sstable::types::Partition) -> String {
    extract_subject_from_partition_key(partition.key.key.as_bytes())
}

/// Extract a clustering key component by position.
///
/// Public wrapper for use by [`crate::property_path`].
pub fn clustering_component(clustering: &[u8], position: usize) -> String {
    extract_clustering_string(clustering, position)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a CQL length-prefixed component: [u16 len][bytes][0x00].
    fn encode_component(s: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
        buf.push(0);
        buf
    }

    // --- BUG-S3: composite partition key decoding ---

    #[test]
    fn extract_composite_component_first() {
        let mut key = encode_component("default");
        key.extend_from_slice(&encode_component("http://example.org/alice"));
        let graph = extract_composite_component(&key, 0);
        assert_eq!(graph.as_deref(), Some("default"));
    }

    #[test]
    fn extract_composite_component_second() {
        let mut key = encode_component("default");
        key.extend_from_slice(&encode_component("http://example.org/alice"));
        let subject = extract_composite_component(&key, 1);
        assert_eq!(subject.as_deref(), Some("http://example.org/alice"));
    }

    #[test]
    fn extract_composite_component_out_of_range() {
        let key = encode_component("default");
        assert!(extract_composite_component(&key, 1).is_none());
    }

    #[test]
    fn extract_subject_from_partition_key_decodes_second_component() {
        let mut key = encode_component("mygraph");
        key.extend_from_slice(&encode_component("http://example.org/bob"));
        let subject = extract_subject_from_partition_key(&key);
        assert_eq!(subject, "http://example.org/bob");
    }

    // --- BUG-S4: binding type compatibility ---

    #[test]
    fn try_insert_binding_compatible() {
        let mut row = HashMap::new();
        let b = Binding {
            binding_type: "uri".into(),
            value: "http://example.org/alice".into(),
            datatype: None,
            lang: None,
        };
        assert!(try_insert_binding(&mut row, "s", &b));
        assert!(
            try_insert_binding(&mut row, "s", &b),
            "same binding is compatible"
        );
    }

    #[test]
    fn try_insert_binding_incompatible_value() {
        let mut row = HashMap::new();
        let b1 = Binding {
            binding_type: "uri".into(),
            value: "http://example.org/alice".into(),
            datatype: None,
            lang: None,
        };
        let b2 = Binding {
            binding_type: "uri".into(),
            value: "http://example.org/bob".into(),
            datatype: None,
            lang: None,
        };
        assert!(try_insert_binding(&mut row, "s", &b1));
        assert!(!try_insert_binding(&mut row, "s", &b2));
    }

    #[test]
    fn try_insert_binding_incompatible_type() {
        let mut row = HashMap::new();
        let uri_binding = Binding {
            binding_type: "uri".into(),
            value: "http://example.org/alice".into(),
            datatype: None,
            lang: None,
        };
        let literal_binding = Binding {
            binding_type: "literal".into(),
            value: "http://example.org/alice".into(),
            datatype: None,
            lang: None,
        };
        assert!(try_insert_binding(&mut row, "x", &uri_binding));
        assert!(
            !try_insert_binding(&mut row, "x", &literal_binding),
            "different binding_type must be incompatible even with same value"
        );
    }

    // --- BUG-S5: OFFSET clamping ---

    #[test]
    fn offset_clamp_prevents_panic() {
        // Verify that start.min(len) prevents out-of-bounds access.
        let binding_sets: Vec<HashMap<String, Binding>> =
            vec![make_binding_row("s", "a"), make_binding_row("s", "b")];
        let offset: usize = 999;
        let start = offset.min(binding_sets.len());
        let end = binding_sets.len();
        // This must not panic.
        let slice = &binding_sets[start..end];
        assert!(
            slice.is_empty(),
            "OFFSET beyond result size yields empty slice"
        );
    }

    // --- BUG-S13: ORDER BY ---

    #[test]
    fn apply_order_by_sorts_ascending() {
        let mut rows = vec![
            make_binding_row("name", "Charlie"),
            make_binding_row("name", "Alice"),
            make_binding_row("name", "Bob"),
        ];
        let conditions = vec![crate::planner::OrderCondition {
            variable: "name".into(),
            ascending: true,
        }];
        apply_order_by(&mut rows, &conditions);
        let names: Vec<&str> = rows.iter().map(|r| r["name"].value.as_str()).collect();
        assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
    }

    #[test]
    fn apply_order_by_sorts_descending() {
        let mut rows = vec![
            make_binding_row("name", "Alice"),
            make_binding_row("name", "Charlie"),
            make_binding_row("name", "Bob"),
        ];
        let conditions = vec![crate::planner::OrderCondition {
            variable: "name".into(),
            ascending: false,
        }];
        apply_order_by(&mut rows, &conditions);
        let names: Vec<&str> = rows.iter().map(|r| r["name"].value.as_str()).collect();
        assert_eq!(names, vec!["Charlie", "Bob", "Alice"]);
    }

    // --- BUG-S13: DISTINCT ---

    #[test]
    fn apply_distinct_removes_duplicates() {
        let mut rows = vec![
            make_binding_row("name", "Alice"),
            make_binding_row("name", "Bob"),
            make_binding_row("name", "Alice"),
        ];
        let projection = vec!["name".into()];
        apply_distinct(&mut rows, &projection);
        assert_eq!(rows.len(), 2);
    }

    // --- BUG-S10: clustering string extraction with logging ---

    #[test]
    fn extract_clustering_string_empty_on_short_input() {
        let result = extract_clustering_string(&[], 0);
        assert!(
            result.is_empty(),
            "empty clustering key should return empty string"
        );
    }

    #[test]
    fn extract_clustering_string_two_components() {
        let mut clustering = encode_component("http://xmlns.com/foaf/0.1/name");
        clustering.extend_from_slice(&encode_component("Alice"));
        assert_eq!(
            extract_clustering_string(&clustering, 0),
            "http://xmlns.com/foaf/0.1/name"
        );
        assert_eq!(extract_clustering_string(&clustering, 1), "Alice");
    }

    // --- BUG-S8/S9: scan filters ---

    #[test]
    fn predicate_scan_filters_by_predicate() {
        let mut triples = vec![
            triple("s1", "http://foaf/name", "Alice", "literal"),
            triple("s1", "http://foaf/knows", "s2", "uri"),
        ];
        let op = TripleOp::PredicateScan {
            graph: "default".into(),
            predicate: "http://foaf/name".into(),
        };
        apply_scan_filters(&op, &mut triples);
        assert_eq!(triples.len(), 1);
        assert_eq!(triples[0].1, "http://foaf/name");
    }

    #[test]
    fn object_scan_filters_by_object() {
        let mut triples = vec![
            triple("s1", "http://foaf/name", "Alice", "literal"),
            triple("s2", "http://foaf/name", "Bob", "literal"),
        ];
        let op = TripleOp::ObjectScan {
            graph: "default".into(),
            object: "Bob".into(),
        };
        apply_scan_filters(&op, &mut triples);
        assert_eq!(triples.len(), 1);
        assert_eq!(triples[0].0, "s2");
    }

    // --- ObjectScan index + post-filter tests ---

    #[test]
    fn object_scan_post_filter_retains_matching_object() {
        // Verify that ObjectScan applies post-fetch object filter even
        // when results come from a range scan (no index).
        let mut triples = vec![
            triple("s1", "p1", "target", "uri"),
            triple("s2", "p2", "other", "uri"),
            triple("s3", "p3", "target", "uri"),
        ];
        let op = TripleOp::ObjectScan {
            graph: "default".into(),
            object: "target".into(),
        };
        apply_scan_filters(&op, &mut triples);
        assert_eq!(
            triples.len(),
            2,
            "only triples with object=target should remain"
        );
        assert!(triples.iter().all(|(_, _, o, _, _, _)| o == "target"));
    }

    #[test]
    fn scan_row_cap_constant_is_10k() {
        assert_eq!(SCAN_ROW_CAP, 10_000, "scan cap must be 10,000");
    }

    #[test]
    fn object_index_name_is_correct() {
        assert_eq!(
            OBJECT_INDEX_NAME, "rdf_triples_object_idx",
            "index name must match the DDL"
        );
    }

    // --- try_merge_bindings ---

    #[test]
    fn merge_bindings_compatible() {
        let mut a = HashMap::new();
        a.insert(
            "s".into(),
            Binding {
                binding_type: "uri".into(),
                value: "http://ex/alice".into(),
                datatype: None,
                lang: None,
            },
        );
        let mut b = HashMap::new();
        b.insert(
            "o".into(),
            Binding {
                binding_type: "uri".into(),
                value: "http://ex/bob".into(),
                datatype: None,
                lang: None,
            },
        );
        let merged = try_merge_bindings(&a, &b);
        assert!(merged.is_some(), "disjoint bindings must merge");
        let m = merged.unwrap();
        assert!(m.contains_key("s"));
        assert!(m.contains_key("o"));
    }

    #[test]
    fn merge_bindings_conflict() {
        let mut a = HashMap::new();
        a.insert(
            "s".into(),
            Binding {
                binding_type: "uri".into(),
                value: "http://ex/alice".into(),
                datatype: None,
                lang: None,
            },
        );
        let mut b = HashMap::new();
        b.insert(
            "s".into(),
            Binding {
                binding_type: "uri".into(),
                value: "http://ex/bob".into(),
                datatype: None,
                lang: None,
            },
        );
        let merged = try_merge_bindings(&a, &b);
        assert!(merged.is_none(), "conflicting bindings must not merge");
    }

    // --- Helpers ---

    fn make_binding_row(var: &str, val: &str) -> HashMap<String, Binding> {
        let mut row = HashMap::new();
        row.insert(
            var.to_string(),
            Binding {
                binding_type: "literal".into(),
                value: val.into(),
                datatype: None,
                lang: None,
            },
        );
        row
    }

    fn triple(s: &str, p: &str, o: &str, otype: &str) -> FetchedTriple {
        (s.into(), p.into(), o.into(), otype.into(), None, None)
    }
}
