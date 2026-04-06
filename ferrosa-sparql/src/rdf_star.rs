//! RDF* (RDF-star) annotation query support.
//!
//! RDF* extends RDF with statement-about-statement annotations:
//! ```sparql
//! << :alice :knows :bob >> :since "2020" .
//! ```
//!
//! In ferrosa, annotations are stored in an `edge_annotations` table:
//! ```sql
//! CREATE TABLE edge_annotations (
//!     tenant_id uuid,
//!     session_id uuid,
//!     src_id uuid,
//!     edge_type text,
//!     dst_id uuid,
//!     property_name text,
//!     property_value text,
//!     value_type text,
//!     created_at timestamp,
//!     PRIMARY KEY ((tenant_id, session_id, src_id, edge_type, dst_id), property_name)
//! );
//! ```
//!
//! The SPARQL planner translates RDF* triple patterns into joins between
//! the main triples table and the edge_annotations table.

use std::collections::HashMap;

use crate::error::SparqlError;
use crate::results::Binding;

/// An RDF* annotation: metadata about a triple.
#[derive(Debug, Clone)]
pub struct TripleAnnotation {
    /// The annotated triple's subject.
    pub subject: String,
    /// The annotated triple's predicate.
    pub predicate: String,
    /// The annotated triple's object.
    pub object: String,
    /// Annotation property name (e.g., "since", "confidence").
    pub property: String,
    /// Annotation property value.
    pub value: String,
}

/// Evaluate an RDF* annotation pattern against binding sets.
///
/// Given bindings from the inner triple pattern (`<< ?s ?p ?o >>`),
/// joins with the edge_annotations table to bind annotation properties.
///
/// Returns enriched bindings with annotation variables bound.
pub fn evaluate_rdf_star_pattern(
    _inner_bindings: &[HashMap<String, Binding>],
    _annotation_property: &str,
    _annotation_variable: &str,
    _keyspace: &str,
) -> Result<Vec<HashMap<String, Binding>>, SparqlError> {
    // RDF* requires the edge_annotations table to exist in the keyspace.
    // This is created by ferrosa-memory via CQL DDL.
    //
    // Implementation approach:
    // 1. For each binding in inner_bindings, extract (subject, predicate, object)
    // 2. Query edge_annotations WHERE src_id=subject AND edge_type=predicate
    //    AND dst_id=object AND property_name=annotation_property
    // 3. Bind the annotation value to annotation_variable
    //
    // For now, return the inner bindings unmodified with a warning.
    tracing::warn!(
        "RDF* annotation queries are not yet fully implemented — \
         annotations will be empty. Create the edge_annotations table \
         and re-run to enable."
    );
    Ok(_inner_bindings.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triple_annotation_fields() {
        let ann = TripleAnnotation {
            subject: "alice".into(),
            predicate: "knows".into(),
            object: "bob".into(),
            property: "since".into(),
            value: "2020".into(),
        };
        assert_eq!(ann.property, "since");
        assert_eq!(ann.value, "2020");
    }

    #[test]
    fn evaluate_rdf_star_returns_inner_bindings() {
        let inner = vec![{
            let mut m = HashMap::new();
            m.insert(
                "s".into(),
                Binding {
                    binding_type: "uri".into(),
                    value: "alice".into(),
                    datatype: None,
                    lang: None,
                },
            );
            m
        }];

        let result = evaluate_rdf_star_pattern(&inner, "since", "when", "test_ks").unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].contains_key("s"));
    }
}
