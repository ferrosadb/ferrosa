//! SPARQL engine: parse → plan → execute.

use std::sync::Arc;

use ferrosa_storage::engine::StorageEngine;

use crate::error::SparqlError;
use crate::planner;
use crate::results::{SparqlAskResult, SparqlJsonResults};

/// Result of a SPARQL query execution, supporting both SELECT and ASK forms.
pub enum SparqlResult {
    /// SELECT query result (binding sets).
    Select(SparqlJsonResults),
    /// ASK query result (boolean).
    Ask(SparqlAskResult),
}

impl SparqlResult {
    /// Serialize to JSON bytes.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        match self {
            Self::Select(results) => results.to_json(),
            Self::Ask(result) => serde_json::to_vec(result),
        }
    }

    /// Serialize to N-Triples bytes.
    pub fn to_ntriples(&self) -> Vec<u8> {
        match self {
            Self::Select(results) => results.to_ntriples(),
            Self::Ask(result) => format!("# boolean: {}\n", result.boolean).into_bytes(),
        }
    }

    /// Serialize to Turtle bytes (currently N-Triples subset).
    pub fn to_turtle(&self) -> Vec<u8> {
        match self {
            Self::Select(results) => results.to_turtle(),
            Self::Ask(result) => format!("# boolean: {}\n", result.boolean).into_bytes(),
        }
    }

    /// Serialize to the requested format.
    pub fn serialize(
        &self,
        format: crate::results::ResultFormat,
    ) -> Result<Vec<u8>, serde_json::Error> {
        match format {
            crate::results::ResultFormat::Json => self.to_json(),
            crate::results::ResultFormat::Turtle => Ok(self.to_turtle()),
            crate::results::ResultFormat::NTriples => Ok(self.to_ntriples()),
        }
    }
}

/// Configuration for the SPARQL engine.
#[derive(Debug, Clone)]
pub struct SparqlConfig {
    /// Default graph name for queries without explicit FROM.
    pub default_graph: String,
    /// Maximum result rows returned per query.
    pub max_results: usize,
}

impl Default for SparqlConfig {
    fn default() -> Self {
        Self {
            default_graph: "default".into(),
            max_results: 10_000,
        }
    }
}

/// SPARQL query engine backed by ferrosa's StorageEngine.
pub struct SparqlEngine {
    storage: Arc<StorageEngine>,
    config: SparqlConfig,
}

impl SparqlEngine {
    /// Create a new SPARQL engine.
    pub fn new(storage: Arc<StorageEngine>, config: SparqlConfig) -> Self {
        Self { storage, config }
    }

    /// Execute a SPARQL UPDATE and return the result.
    pub fn execute_update(
        &self,
        update_str: &str,
        keyspace: &str,
    ) -> Result<crate::update::UpdateResult, SparqlError> {
        let ks = if keyspace.is_empty() {
            &self.config.default_graph
        } else {
            keyspace
        };
        if !ks.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(SparqlError::KeyspaceNotFound(format!(
                "invalid keyspace name: {ks}"
            )));
        }
        crate::update::execute_update(update_str, ks, &self.storage)
    }

    /// Execute a SPARQL query and return results.
    ///
    /// The `keyspace` parameter scopes the query to a specific tenant/keyspace.
    /// ASK queries return a boolean result; SELECT queries return binding sets.
    pub fn execute(&self, query_str: &str, keyspace: &str) -> Result<SparqlResult, SparqlError> {
        // 1. Parse SPARQL → algebra.
        let query = spargebra::SparqlParser::new()
            .parse_query(query_str)
            .map_err(|e| SparqlError::Parse(format!("{e}")))?;

        // 2. Plan: algebra → storage operations.
        // BUG-S1 fix: use caller-supplied keyspace instead of default_graph.
        let graph = if keyspace.is_empty() {
            &self.config.default_graph
        } else {
            keyspace
        };

        // BUG-S12 fix: validate keyspace name (alphanumeric + underscore only).
        if !graph.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(SparqlError::KeyspaceNotFound(format!(
                "invalid keyspace name: {graph}"
            )));
        }

        let plan = planner::plan_query(&query, graph)?;

        // 3. Execute plan against storage.
        let results = crate::executor::execute(&plan, &self.storage)?;

        // BUG-S6 fix: ASK queries return boolean result format.
        if plan.is_ask {
            let has_results = !results.results.bindings.is_empty();
            return Ok(SparqlResult::Ask(SparqlAskResult {
                head: crate::results::ResultHead { vars: vec![] },
                boolean: has_results,
            }));
        }

        Ok(SparqlResult::Select(results))
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn parse_simple_select() {
        // Verify spargebra parses without error.
        let query =
            spargebra::SparqlParser::new().parse_query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
        assert!(query.is_ok(), "spargebra should parse basic SELECT");
    }

    #[test]
    fn parse_select_with_filter() {
        let query = spargebra::SparqlParser::new().parse_query(
            "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . FILTER(?name = \"Alice\") }",
        );
        assert!(query.is_ok(), "spargebra should parse SELECT with FILTER");
    }

    #[test]
    fn parse_select_with_prefix() {
        let query = spargebra::SparqlParser::new().parse_query(
            "PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT ?name WHERE { ?s foaf:name ?name }",
        );
        assert!(query.is_ok(), "spargebra should parse SELECT with PREFIX");
    }

    #[test]
    fn parse_ask_query() {
        let query = spargebra::SparqlParser::new().parse_query(
            "ASK { <http://example.org/alice> <http://xmlns.com/foaf/0.1/name> ?name }",
        );
        assert!(query.is_ok(), "spargebra should parse ASK");
    }

    #[test]
    fn parse_property_path() {
        let query = spargebra::SparqlParser::new().parse_query(
            "SELECT ?o WHERE { <http://example.org/alice> <http://xmlns.com/foaf/0.1/knows>+ ?o }",
        );
        assert!(query.is_ok(), "spargebra should parse property paths");
    }

    #[test]
    fn parse_insert_data() {
        let update = spargebra::SparqlParser::new().parse_update(
            "INSERT DATA { <http://example.org/alice> <http://xmlns.com/foaf/0.1/name> \"Alice\" }",
        );
        assert!(update.is_ok(), "spargebra should parse INSERT DATA");
    }
}
