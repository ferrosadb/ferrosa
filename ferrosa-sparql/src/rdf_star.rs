//! RDF* (RDF-star) annotation query support — **not yet implemented**.
//!
//! RDF* extends RDF with statement-about-statement annotations:
//! ```sparql
//! << :alice :knows :bob >> :since "2020" .
//! ```
//!
//! ## Current status (URS-QEC-S03a / URS-QEC-X01)
//!
//! Quoted-triple *matching and binding* is **out of scope** for the M3 SPARQL
//! increment and is therefore **fail-loud**, never silently approximated:
//!
//! - The query planner ([`crate::planner`]) rejects any triple pattern with a
//!   quoted triple in subject or object position with `SparqlError::Plan`, so a
//!   SELECT/ASK over `<< ?s ?p ?o >> ?prop ?val` returns a 400, not inner
//!   bindings with the annotation variable silently absent.
//! - [`evaluate_rdf_star_pattern`] below mirrors that fail-loud contract for any
//!   future caller.
//!
//! ## What real support would require (deliberately not built here)
//!
//! Ferrosa's RDF store is the single `rdf_triples` table
//! (`((graph, subject), predicate, object)`); there is **no** dedicated
//! annotation/quoted-triple table, and the SPARQL `INSERT` path can only store a
//! quoted triple as an opaque `<<…>>` string in *object* position — a
//! quoted-triple *subject* (the annotation case) cannot even be inserted today.
//! Real evaluation would need: (1) a storage encoding for quoted-triple terms in
//! key position, (2) parser/insert support for quoted-triple subjects, and
//! (3) a join planner that decomposes `<< s p o >>` and binds the inner pattern
//! before binding the outer annotation property. Until that exists, this module
//! fails loud rather than returning a wrong answer.

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
    // URS-QEC-S03 / URS-QEC-X01: RDF* annotation evaluation is not yet
    // implemented.  Returning the inner bindings without the annotation
    // variable bound would be a silent wrong result — the caller would receive
    // rows that look valid but are missing the requested annotation data.
    // Fail loud instead so the HTTP layer can return 400 Bad Request.
    Err(SparqlError::Plan(
        "RDF* (RDF-star) annotation evaluation is not yet implemented. \
         Quoted triple patterns << ?s ?p ?o >> are parsed but annotation \
         variable binding against edge_annotations is not supported."
            .into(),
    ))
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

    /// URS-QEC-S03 / URS-QEC-X01: `evaluate_rdf_star_pattern` must now fail
    /// loud rather than returning the inner bindings with the annotation variable
    /// silently absent.
    #[test]
    fn evaluate_rdf_star_fails_loud_not_silent_inner_bindings() {
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

        let result = evaluate_rdf_star_pattern(&inner, "since", "when", "test_ks");
        assert!(
            result.is_err(),
            "evaluate_rdf_star_pattern must return Err (fail loud); \
             returning inner bindings silently would be a wrong result"
        );
        let msg = result.unwrap_err().to_string().to_lowercase();
        assert!(
            msg.contains("rdf*")
                || msg.contains("rdf-star")
                || msg.contains("annotation")
                || msg.contains("not")
                || msg.contains("unsupported"),
            "error must describe the unimplemented RDF* feature: {msg}"
        );
    }
}
