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
}
