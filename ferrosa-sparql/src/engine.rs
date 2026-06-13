//! SPARQL engine: parse → plan → execute.

use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_storage::engine::StorageEngine;

use crate::error::SparqlError;
use crate::planner::{self, GraphQueryMode};
use crate::results::{SparqlAskResult, SparqlJsonResults};

/// A triple in a constructed/described graph result (S01).
#[derive(Debug, Clone)]
pub struct ConstructedTriple {
    /// Subject IRI or blank-node label.
    pub subject: String,
    /// Predicate IRI.
    pub predicate: String,
    /// Object value string.
    pub object: String,
    /// `"uri"`, `"literal"`, or `"bnode"`.
    pub object_type: String,
    /// XSD datatype URI for typed literals.
    pub datatype: Option<String>,
    /// Language tag for `rdf:langString` literals.
    pub lang: Option<String>,
}

/// Result of a SPARQL query execution, supporting SELECT, ASK, and graph forms.
#[derive(Debug)]
pub enum SparqlResult {
    /// SELECT query result (binding sets).
    Select(SparqlJsonResults),
    /// ASK query result (boolean).
    Ask(SparqlAskResult),
    /// CONSTRUCT / DESCRIBE query result (a set of triples forming a graph).
    Graph(Vec<ConstructedTriple>),
}

impl SparqlResult {
    /// Serialize to JSON bytes.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        match self {
            Self::Select(results) => results.to_json(),
            Self::Ask(result) => serde_json::to_vec(result),
            Self::Graph(triples) => {
                // Serialize constructed graph as a JSON array of triple objects
                // (not standard SPARQL Results JSON, but consistent with our
                // internal JSON format for graph results).
                serde_json::to_vec(
                    &triples
                        .iter()
                        .map(|t| {
                            serde_json::json!({
                                "subject":   t.subject,
                                "predicate": t.predicate,
                                "object":    t.object,
                                "type":      t.object_type,
                                "datatype":  t.datatype,
                                "lang":      t.lang,
                            })
                        })
                        .collect::<Vec<_>>(),
                )
            }
        }
    }

    /// Serialize to SPARQL Results XML bytes.
    pub fn to_xml(&self) -> Result<Vec<u8>, String> {
        match self {
            Self::Select(results) => results.to_xml(),
            Self::Ask(result) => {
                let boolean_str = if result.boolean { "true" } else { "false" };
                Ok(format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n\
                     <head/>\n\
                     <boolean>{boolean_str}</boolean>\n\
                     </sparql>\n"
                )
                .into_bytes())
            }
            Self::Graph(triples) => {
                // CONSTRUCT/DESCRIBE — serialize as RDF/XML (simple form).
                let mut buf = String::new();
                buf.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
                buf.push_str(
                    "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n",
                );
                for t in triples {
                    buf.push_str(&format!(
                        "  <rdf:Description rdf:about=\"{}\">\n",
                        t.subject
                    ));
                    buf.push_str(&format!(
                        "    <{pred}>{obj}</{pred}>\n",
                        pred = t.predicate,
                        obj = t.object
                    ));
                    buf.push_str("  </rdf:Description>\n");
                }
                buf.push_str("</rdf:RDF>\n");
                Ok(buf.into_bytes())
            }
        }
    }

    /// Serialize to N-Triples bytes.
    pub fn to_ntriples(&self) -> Vec<u8> {
        match self {
            Self::Select(results) => results.to_ntriples(),
            Self::Ask(result) => format!("# boolean: {}\n", result.boolean).into_bytes(),
            Self::Graph(triples) => {
                let mut buf = String::new();
                for t in triples {
                    let subj = if t.subject.starts_with("_:") {
                        t.subject.clone()
                    } else {
                        format!("<{}>", t.subject)
                    };
                    let pred = format!("<{}>", t.predicate);
                    let obj = match t.object_type.as_str() {
                        "uri" => format!("<{}>", t.object),
                        "bnode" => {
                            if t.object.starts_with("_:") {
                                t.object.clone()
                            } else {
                                format!("_:{}", t.object)
                            }
                        }
                        _ => {
                            let escaped = t.object.replace('\\', "\\\\").replace('"', "\\\"");
                            if let Some(lang) = &t.lang {
                                format!("\"{}\"@{}", escaped, lang)
                            } else if let Some(dt) = &t.datatype {
                                format!("\"{}\"^^<{}>", escaped, dt)
                            } else {
                                format!("\"{}\"", escaped)
                            }
                        }
                    };
                    buf.push_str(&format!("{subj} {pred} {obj} .\n"));
                }
                buf.into_bytes()
            }
        }
    }

    /// Serialize to Turtle bytes (currently N-Triples subset).
    pub fn to_turtle(&self) -> Vec<u8> {
        match self {
            Self::Select(results) => results.to_turtle(),
            Self::Ask(result) => format!("# boolean: {}\n", result.boolean).into_bytes(),
            Self::Graph(_) => self.to_ntriples(),
        }
    }

    /// Serialize to the requested format.
    ///
    /// Returns `Err(String)` on serialization failure. The String carries a
    /// human-readable description (JSON serialization errors are stringified;
    /// XML errors are already strings).
    pub fn serialize(&self, format: crate::results::ResultFormat) -> Result<Vec<u8>, String> {
        match format {
            crate::results::ResultFormat::Json => self.to_json().map_err(|e| e.to_string()),
            crate::results::ResultFormat::Xml => self.to_xml(),
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
    write_path: Arc<WritePath>,
    config: SparqlConfig,
}

impl SparqlEngine {
    /// Create a new SPARQL engine.
    ///
    /// Registers the `rdf_triples` table schema for the default graph
    /// keyspace so that INSERT/DELETE/SELECT queries can execute without
    /// requiring external DDL.
    pub fn new(
        storage: Arc<StorageEngine>,
        write_path: Arc<WritePath>,
        config: SparqlConfig,
    ) -> Self {
        // Register the RDF triples table for the default keyspace.
        let schema = crate::triple_store::rdf_triples_schema(&config.default_graph);
        if let Err(e) = storage.register_table(schema) {
            tracing::warn!(%e, "failed to register rdf_triples table for default graph");
        }
        Self {
            storage,
            write_path,
            config,
        }
    }

    /// Ensure the rdf_triples table is registered for a given keyspace.
    ///
    /// Called lazily when a query targets a keyspace other than the default.
    fn ensure_table_registered(&self, keyspace: &str) {
        let schema = crate::triple_store::rdf_triples_schema(keyspace);
        if let Err(e) = self.storage.register_table(schema) {
            // register_table returns Ok(()) if already registered.
            tracing::debug!(%e, keyspace, "rdf_triples table registration");
        }
    }

    /// Execute a SPARQL UPDATE and return the result.
    ///
    /// Async because pattern-based ops (`DELETE WHERE`, `DELETE/INSERT … WHERE`,
    /// `CLEAR`, `DROP`) evaluate their WHERE clause through the async SELECT
    /// executor before tombstoning the bound triples.
    pub async fn execute_update(
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
        self.ensure_table_registered(ks);
        crate::update::execute_update(update_str, ks, &self.storage, &self.write_path).await
    }

    /// Execute a SPARQL query and return results.
    ///
    /// The `keyspace` parameter scopes the query to a specific tenant/keyspace.
    /// ASK queries return a boolean result; SELECT queries return binding sets.
    pub async fn execute(
        &self,
        query_str: &str,
        keyspace: &str,
    ) -> Result<SparqlResult, SparqlError> {
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

        // Ensure rdf_triples table exists for this keyspace.
        self.ensure_table_registered(graph);

        let plan = planner::plan_query(&query, graph)?;

        // 3. Execute plan against storage.
        let results = crate::executor::execute(&plan, &self.write_path).await?;

        // BUG-S6 fix: ASK queries return boolean result format.
        if plan.is_ask {
            let has_results = !results.results.bindings.is_empty();
            return Ok(SparqlResult::Ask(SparqlAskResult {
                head: crate::results::ResultHead { vars: vec![] },
                boolean: has_results,
            }));
        }

        // URS-QEC-S01: CONSTRUCT / DESCRIBE return a graph result.
        if let Some(graph_mode) = &plan.graph_mode {
            return Ok(SparqlResult::Graph(
                build_graph_result(graph_mode, &results, &plan, &self.write_path).await?,
            ));
        }

        Ok(SparqlResult::Select(results))
    }
}

/// Build the `Vec<ConstructedTriple>` for CONSTRUCT and DESCRIBE queries.
///
/// For `CONSTRUCT { template } WHERE { … }`:
///   Instantiate the template triples once per WHERE solution.  Variables in
///   the template are replaced with the corresponding bound values.
///
/// For `DESCRIBE <iri>`:
///   Run a subject-lookup for the described IRI and return all triples found.
async fn build_graph_result(
    mode: &GraphQueryMode,
    where_results: &SparqlJsonResults,
    plan: &planner::QueryPlan,
    write_path: &Arc<WritePath>,
) -> Result<Vec<ConstructedTriple>, SparqlError> {
    use spargebra::term::{NamedNodePattern, TermPattern};

    match mode {
        GraphQueryMode::Construct(template) => {
            let mut triples = Vec::new();
            for solution in &where_results.results.bindings {
                for tp in template {
                    // Resolve subject.
                    let subject = match &tp.subject {
                        TermPattern::NamedNode(n) => n.as_str().to_string(),
                        TermPattern::Variable(v) => match solution.get(v.as_str()) {
                            Some(b) => b.value.clone(),
                            None => continue,
                        },
                        TermPattern::BlankNode(b) => format!("_:{}", b.as_str()),
                        _ => continue,
                    };

                    // Resolve predicate.
                    let predicate = match &tp.predicate {
                        NamedNodePattern::NamedNode(n) => n.as_str().to_string(),
                        NamedNodePattern::Variable(v) => match solution.get(v.as_str()) {
                            Some(b) => b.value.clone(),
                            None => continue,
                        },
                    };

                    // Resolve object.
                    let (object, object_type, datatype, lang) = match &tp.object {
                        TermPattern::NamedNode(n) => {
                            (n.as_str().to_string(), "uri".to_string(), None, None)
                        }
                        TermPattern::Literal(l) => (
                            l.value().to_string(),
                            "literal".to_string(),
                            Some(l.datatype().as_str().to_string()),
                            l.language().map(|s| s.to_string()),
                        ),
                        TermPattern::BlankNode(b) => {
                            (format!("_:{}", b.as_str()), "bnode".to_string(), None, None)
                        }
                        TermPattern::Variable(v) => match solution.get(v.as_str()) {
                            Some(b) => (
                                b.value.clone(),
                                b.binding_type.clone(),
                                b.datatype.clone(),
                                b.lang.clone(),
                            ),
                            None => continue,
                        },
                        _ => continue,
                    };

                    triples.push(ConstructedTriple {
                        subject,
                        predicate,
                        object,
                        object_type,
                        datatype,
                        lang,
                    });
                }
            }
            Ok(triples)
        }

        GraphQueryMode::DescribeIris(iris_list) => {
            // The planner ran SubjectLookup ops for each IRI. The where_results
            // bindings contain __p and __o rows — but subjects are mixed. Since
            // binding rows don't carry the subject (it was the partition key),
            // we re-issue individual SubjectLookups to correctly attribute rows.
            use crate::planner::TripleOp;
            use spargebra::term::{NamedNode, NamedNodePattern, TermPattern, TriplePattern};

            let mut triples = Vec::new();
            for iri in iris_list {
                let dummy_tp = TriplePattern {
                    subject: TermPattern::NamedNode(NamedNode::new_unchecked(iri.as_str())),
                    predicate: NamedNodePattern::Variable(
                        spargebra::term::Variable::new_unchecked("__p"),
                    ),
                    object: TermPattern::Variable(spargebra::term::Variable::new_unchecked("__o")),
                };
                let op = TripleOp::SubjectLookup {
                    graph: plan.ops.first().map_or_else(
                        || "__default".to_string(),
                        |(_, o)| match o {
                            TripleOp::SubjectLookup { graph, .. } => graph.clone(),
                            _ => "__default".to_string(),
                        },
                    ),
                    subject: iri.clone(),
                    predicate_filter: None,
                };
                let lookup_plan = planner::QueryPlan {
                    ops: vec![(dummy_tp, op)],
                    projection: vec!["__p".into(), "__o".into()],
                    limit: None,
                    offset: None,
                    is_ask: false,
                    distinct: false,
                    order_by: vec![],
                    filters: vec![],
                    graph_mode: None,
                };
                let sub = crate::executor::execute(&lookup_plan, write_path).await?;
                for row in sub.results.bindings {
                    let pred = match row.get("__p") {
                        Some(b) => b.value.clone(),
                        None => continue,
                    };
                    let obj_b = match row.get("__o") {
                        Some(b) => b,
                        None => continue,
                    };
                    triples.push(ConstructedTriple {
                        subject: iri.clone(),
                        predicate: pred,
                        object: obj_b.value.clone(),
                        object_type: obj_b.binding_type.clone(),
                        datatype: obj_b.datatype.clone(),
                        lang: obj_b.lang.clone(),
                    });
                }
            }
            Ok(triples)
        }

        GraphQueryMode::Describe(graph) => {
            // DESCRIBE: collect all triples about every subject bound by the
            // WHERE clause (or the directly described IRI if no WHERE clause
            // was supplied by spargebra).
            let mut subjects: Vec<String> = Vec::new();
            if where_results.results.bindings.is_empty() {
                // No WHERE solutions — nothing to describe.
                return Ok(vec![]);
            }
            // Collect all bound URI values that look like the described subject.
            for sol in &where_results.results.bindings {
                for binding in sol.values() {
                    if binding.binding_type == "uri" && !subjects.contains(&binding.value) {
                        subjects.push(binding.value.clone());
                    }
                }
            }

            let mut triples = Vec::new();
            for subject_iri in subjects {
                // SubjectLookup: fetch all (pred, obj) for this subject.
                use crate::planner::TripleOp;
                use spargebra::term::NamedNode;
                use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};

                let dummy_tp = TriplePattern {
                    subject: TermPattern::NamedNode(NamedNode::new_unchecked(&subject_iri)),
                    predicate: NamedNodePattern::Variable(
                        spargebra::term::Variable::new_unchecked("__p"),
                    ),
                    object: TermPattern::Variable(spargebra::term::Variable::new_unchecked("__o")),
                };
                let op = TripleOp::SubjectLookup {
                    graph: graph.clone(),
                    subject: subject_iri.clone(),
                    predicate_filter: None,
                };
                let dummy_plan = planner::QueryPlan {
                    ops: vec![(dummy_tp, op)],
                    projection: vec!["__p".into(), "__o".into()],
                    limit: None,
                    offset: None,
                    is_ask: false,
                    distinct: false,
                    order_by: vec![],
                    filters: vec![],
                    graph_mode: None,
                };
                let sub_result = crate::executor::execute(&dummy_plan, write_path).await?;
                for row in sub_result.results.bindings {
                    let pred = match row.get("__p") {
                        Some(b) => b.value.clone(),
                        None => continue,
                    };
                    let obj_binding = match row.get("__o") {
                        Some(b) => b,
                        None => continue,
                    };
                    triples.push(ConstructedTriple {
                        subject: subject_iri.clone(),
                        predicate: pred,
                        object: obj_binding.value.clone(),
                        object_type: obj_binding.binding_type.clone(),
                        datatype: obj_binding.datatype.clone(),
                        lang: obj_binding.lang.clone(),
                    });
                }
            }
            Ok(triples)
        }
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

    #[test]
    fn parse_rdf_star_quoted_triple() {
        // RDF* / SPARQL-star: quoted triple pattern << ?s ?p ?o >>
        let query = spargebra::SparqlParser::new().parse_query(
            "SELECT ?conf WHERE { << <http://ex/a> <http://ex/link> <http://ex/b> >> <http://ex/confidence> ?conf }",
        );
        assert!(
            query.is_ok(),
            "spargebra with sparql-12 should parse RDF* quoted triples, got: {:?}",
            query.err()
        );
    }

    #[test]
    fn parse_rdf_star_with_variables() {
        let query = spargebra::SparqlParser::new().parse_query(
            "SELECT ?s ?p ?o ?who WHERE { << ?s ?p ?o >> <http://ex/created_by> ?who }",
        );
        assert!(
            query.is_ok(),
            "spargebra should parse RDF* with variable triple, got: {:?}",
            query.err()
        );
    }
}
