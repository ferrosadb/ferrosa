//! SPARQL result serialization.
//!
//! Converts internal binding sets to SPARQL JSON Results format
//! (`application/sparql-results+json`) per the W3C spec.

use serde::Serialize;
use std::collections::HashMap;

/// A single variable binding in a SPARQL result row.
#[derive(Debug, Clone, Serialize)]
pub struct Binding {
    #[serde(rename = "type")]
    pub binding_type: String, // "uri", "literal", "bnode"
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datatype: Option<String>,
    #[serde(rename = "xml:lang", skip_serializing_if = "Option::is_none")]
    pub lang: Option<String>,
}

/// SPARQL JSON Results format (W3C).
#[derive(Debug, Serialize)]
pub struct SparqlJsonResults {
    pub head: ResultHead,
    pub results: ResultBody,
}

#[derive(Debug, Serialize)]
pub struct ResultHead {
    pub vars: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ResultBody {
    pub bindings: Vec<HashMap<String, Binding>>,
}

impl SparqlJsonResults {
    /// Create an empty result set with the given variable names.
    pub fn new(vars: Vec<String>) -> Self {
        Self {
            head: ResultHead { vars },
            results: ResultBody {
                bindings: Vec::new(),
            },
        }
    }

    /// Add a row of bindings.
    pub fn add_row(&mut self, row: HashMap<String, Binding>) {
        self.results.bindings.push(row);
    }

    /// Serialize to JSON bytes.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

/// ASK query result.
#[derive(Debug, Serialize)]
pub struct SparqlAskResult {
    pub head: ResultHead,
    pub boolean: bool,
}

/// Supported SPARQL result content types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultFormat {
    /// `application/sparql-results+json` (default for SELECT/ASK).
    Json,
    /// `text/turtle` (Turtle/N-Triples-like serialization).
    Turtle,
    /// `application/n-triples` (N-Triples format).
    NTriples,
}

impl ResultFormat {
    /// Parse an HTTP Accept header value into a result format.
    ///
    /// Returns the best matching format, defaulting to JSON if no recognized
    /// type is found.
    pub fn from_accept(accept: &str) -> Self {
        // Check for exact or partial matches in priority order.
        if accept.contains("text/turtle") {
            Self::Turtle
        } else if accept.contains("application/n-triples") {
            Self::NTriples
        } else {
            Self::Json
        }
    }

    /// The Content-Type header value for this format.
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Json => "application/sparql-results+json",
            Self::Turtle => "text/turtle",
            Self::NTriples => "application/n-triples",
        }
    }
}

impl SparqlJsonResults {
    /// Serialize to N-Triples format (one triple per line).
    ///
    /// Produces a simple tabular rendering: each binding row becomes one
    /// line with subject/predicate/object terms in N-Triples syntax.
    pub fn to_ntriples(&self) -> Vec<u8> {
        let mut buf = String::new();
        for row in &self.results.bindings {
            let parts: Vec<String> = self
                .head
                .vars
                .iter()
                .filter_map(|var| row.get(var).map(format_ntriples_term))
                .collect();
            if !parts.is_empty() {
                buf.push_str(&parts.join(" "));
                buf.push_str(" .\n");
            }
        }
        buf.into_bytes()
    }

    /// Serialize to a simple Turtle-like format.
    ///
    /// Uses the same N-Triples serialization since Turtle is a superset
    /// of N-Triples. Full Turtle grouping/prefixing is deferred.
    pub fn to_turtle(&self) -> Vec<u8> {
        self.to_ntriples()
    }
}

/// Format a binding value as an N-Triples term.
fn format_ntriples_term(binding: &Binding) -> String {
    match binding.binding_type.as_str() {
        "uri" => format!("<{}>", binding.value),
        "bnode" => {
            if binding.value.starts_with("_:") {
                binding.value.clone()
            } else {
                format!("_:{}", binding.value)
            }
        }
        _ => {
            // Literal: optionally with datatype or language tag.
            let escaped = binding.value.replace('\\', "\\\\").replace('"', "\\\"");
            if let Some(lang) = &binding.lang {
                format!("\"{escaped}\"@{lang}")
            } else if let Some(dt) = &binding.datatype {
                format!("\"{escaped}\"^^<{dt}>")
            } else {
                format!("\"{escaped}\"")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_results_serialize_to_json() {
        let results = SparqlJsonResults::new(vec!["s".into(), "p".into(), "o".into()]);
        let json = results.to_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["head"]["vars"], serde_json::json!(["s", "p", "o"]));
        assert!(parsed["results"]["bindings"].as_array().unwrap().is_empty());
    }

    #[test]
    fn results_with_binding_serialize_correctly() {
        let mut results = SparqlJsonResults::new(vec!["name".into()]);
        let mut row = HashMap::new();
        row.insert(
            "name".into(),
            Binding {
                binding_type: "literal".into(),
                value: "Alice".into(),
                datatype: Some("http://www.w3.org/2001/XMLSchema#string".into()),
                lang: None,
            },
        );
        results.add_row(row);
        let json = results.to_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["results"]["bindings"][0]["name"]["value"], "Alice");
    }

    // --- Content negotiation: ResultFormat ---

    #[test]
    fn result_format_from_accept_turtle() {
        let fmt = ResultFormat::from_accept("text/turtle");
        assert_eq!(fmt, ResultFormat::Turtle);
        assert_eq!(fmt.content_type(), "text/turtle");
    }

    #[test]
    fn result_format_from_accept_ntriples() {
        let fmt = ResultFormat::from_accept("application/n-triples");
        assert_eq!(fmt, ResultFormat::NTriples);
        assert_eq!(fmt.content_type(), "application/n-triples");
    }

    #[test]
    fn result_format_from_accept_json_default() {
        let fmt = ResultFormat::from_accept("application/sparql-results+json");
        assert_eq!(fmt, ResultFormat::Json);
    }

    #[test]
    fn result_format_from_accept_empty_defaults_json() {
        let fmt = ResultFormat::from_accept("");
        assert_eq!(fmt, ResultFormat::Json);
    }

    #[test]
    fn result_format_from_accept_wildcard_defaults_json() {
        let fmt = ResultFormat::from_accept("*/*");
        assert_eq!(fmt, ResultFormat::Json);
    }

    // --- N-Triples serialization ---

    #[test]
    fn ntriples_serialization_uri_binding() {
        let mut results = SparqlJsonResults::new(vec!["s".into(), "p".into(), "o".into()]);
        let mut row = HashMap::new();
        row.insert(
            "s".into(),
            Binding {
                binding_type: "uri".into(),
                value: "http://ex/alice".into(),
                datatype: None,
                lang: None,
            },
        );
        row.insert(
            "p".into(),
            Binding {
                binding_type: "uri".into(),
                value: "http://ex/knows".into(),
                datatype: None,
                lang: None,
            },
        );
        row.insert(
            "o".into(),
            Binding {
                binding_type: "uri".into(),
                value: "http://ex/bob".into(),
                datatype: None,
                lang: None,
            },
        );
        results.add_row(row);
        let nt = String::from_utf8(results.to_ntriples()).unwrap();
        assert!(
            nt.contains("<http://ex/alice>"),
            "subject must be bracketed IRI"
        );
        assert!(
            nt.contains("<http://ex/knows>"),
            "predicate must be bracketed IRI"
        );
        assert!(
            nt.contains("<http://ex/bob>"),
            "object must be bracketed IRI"
        );
        assert!(nt.ends_with(" .\n"), "N-Triples line must end with ' .\\n'");
    }

    #[test]
    fn ntriples_serialization_literal_with_lang() {
        let mut results = SparqlJsonResults::new(vec!["o".into()]);
        let mut row = HashMap::new();
        row.insert(
            "o".into(),
            Binding {
                binding_type: "literal".into(),
                value: "Alice".into(),
                datatype: None,
                lang: Some("en".into()),
            },
        );
        results.add_row(row);
        let nt = String::from_utf8(results.to_ntriples()).unwrap();
        assert!(
            nt.contains("\"Alice\"@en"),
            "literal must include language tag"
        );
    }

    #[test]
    fn ntriples_serialization_literal_with_datatype() {
        let mut results = SparqlJsonResults::new(vec!["o".into()]);
        let mut row = HashMap::new();
        row.insert(
            "o".into(),
            Binding {
                binding_type: "literal".into(),
                value: "42".into(),
                datatype: Some("http://www.w3.org/2001/XMLSchema#integer".into()),
                lang: None,
            },
        );
        results.add_row(row);
        let nt = String::from_utf8(results.to_ntriples()).unwrap();
        assert!(
            nt.contains("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            "literal must include datatype"
        );
    }

    #[test]
    fn format_ntriples_term_bnode() {
        let b = Binding {
            binding_type: "bnode".into(),
            value: "_:b0".into(),
            datatype: None,
            lang: None,
        };
        assert_eq!(format_ntriples_term(&b), "_:b0");
    }
}
