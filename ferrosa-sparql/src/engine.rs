//! SPARQL engine: parse → plan → execute.

use std::sync::Arc;

use spargebra::Query;

use ferrosa_storage::engine::StorageEngine;

use crate::error::SparqlError;
use crate::planner;
use crate::results::SparqlJsonResults;

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

    /// Execute a SPARQL query and return JSON results.
    pub fn execute(
        &self,
        query_str: &str,
        keyspace: &str,
    ) -> Result<SparqlJsonResults, SparqlError> {
        let _keyspace = keyspace; // Used in Sprint 2 for keyspace-scoped graphs.

        // 1. Parse SPARQL → algebra.
        let query =
            Query::parse(query_str, None).map_err(|e| SparqlError::Parse(format!("{e}")))?;

        // 2. Plan: algebra → storage operations.
        let plan = planner::plan_query(&query, &self.config.default_graph)?;

        // 3. Execute plan against storage.
        let results = crate::executor::execute(&plan, &self.storage)?;

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_select() {
        // Verify spargebra parses without error.
        let query = Query::parse("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", None);
        assert!(query.is_ok(), "spargebra should parse basic SELECT");
    }

    #[test]
    fn parse_select_with_filter() {
        let query = Query::parse(
            "SELECT ?name WHERE { ?s <http://xmlns.com/foaf/0.1/name> ?name . FILTER(?name = \"Alice\") }",
            None,
        );
        assert!(query.is_ok(), "spargebra should parse SELECT with FILTER");
    }

    #[test]
    fn parse_select_with_prefix() {
        let query = Query::parse(
            "PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT ?name WHERE { ?s foaf:name ?name }",
            None,
        );
        assert!(query.is_ok(), "spargebra should parse SELECT with PREFIX");
    }

    #[test]
    fn parse_ask_query() {
        let query = Query::parse(
            "ASK { <http://example.org/alice> <http://xmlns.com/foaf/0.1/name> ?name }",
            None,
        );
        assert!(query.is_ok(), "spargebra should parse ASK");
    }

    #[test]
    fn parse_property_path() {
        let query = Query::parse(
            "SELECT ?o WHERE { <http://example.org/alice> <http://xmlns.com/foaf/0.1/knows>+ ?o }",
            None,
        );
        assert!(query.is_ok(), "spargebra should parse property paths");
    }

    #[test]
    fn parse_insert_data() {
        let update = spargebra::Update::parse(
            "INSERT DATA { <http://example.org/alice> <http://xmlns.com/foaf/0.1/name> \"Alice\" }",
            None,
        );
        assert!(update.is_ok(), "spargebra should parse INSERT DATA");
    }
}
