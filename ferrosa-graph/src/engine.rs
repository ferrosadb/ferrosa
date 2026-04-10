//! GraphEngine: composition root for graph query processing.
//!
//! Wires together the parser, planner, executor, adjacency index observer,
//! and reconciliation loop. Provides the top-level `execute()` and `explain()`
//! entry points consumed by the HTTP endpoint.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::Schema;
use ferrosa_storage::StorageEngine;

use tokio_util::sync::CancellationToken;

use crate::adjacency::observer::AdjacencyIndexObserver;
use crate::adjacency::reconcile::spawn_reconciliation;
use crate::error::{GraphError, Result};
use crate::executor::expand::{execute, GraphEngineConfig};
use crate::executor::result::GraphResult;
use crate::executor::subscribe::SubscriptionRegistry;
use crate::parser::{parse, Statement};
use crate::planner::logical::validate;
use crate::planner::physical::{plan, PhysicalPlan};

/// Composite configuration for the graph engine.
pub struct GraphConfig {
    pub engine: GraphEngineConfig,
    pub http: crate::http::GraphHttpConfig,
    pub reconciliation_interval: std::time::Duration,
    pub enabled: bool,
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            engine: GraphEngineConfig::default(),
            http: crate::http::GraphHttpConfig::default(),
            reconciliation_interval: std::time::Duration::from_secs(300),
            enabled: false,
        }
    }
}

/// Default per-connection subscription limit (FMEA F5).
const DEFAULT_MAX_SUBSCRIPTIONS: usize = 8;

/// Central coordinator for graph query processing.
pub struct GraphEngine {
    schema: Arc<Schema>,
    storage: Arc<StorageEngine>,
    config: GraphEngineConfig,
    reconciliation_handles: Vec<tokio::task::JoinHandle<()>>,
    reconciliation_cancel: CancellationToken,
    subscription_registry: Arc<SubscriptionRegistry>,
}

/// Information about a vertex or edge label in the graph schema.
#[derive(Debug, Clone, Serialize)]
pub struct LabelInfo {
    /// Table name in the underlying keyspace.
    pub table: String,
    /// Graph label (from extensions).
    pub label: String,
    /// Property column names.
    pub properties: Vec<String>,
}

/// Graph schema for a keyspace: vertex and edge labels.
#[derive(Debug, Clone, Serialize)]
pub struct GraphSchema {
    /// Vertex label tables.
    pub vertices: Vec<LabelInfo>,
    /// Edge label tables.
    pub edges: Vec<LabelInfo>,
}

impl GraphEngine {
    /// Create a new `GraphEngine`.
    ///
    /// Startup wiring:
    /// - Scans schema for edge tables in each keyspace
    /// - Registers adjacency index observers
    /// - Starts background reconciliation loops
    pub fn new(
        schema: Arc<Schema>,
        storage: Arc<StorageEngine>,
        config: GraphEngineConfig,
        reconciliation_interval: std::time::Duration,
    ) -> Self {
        let snap = schema.snapshot();

        // Discover keyspaces that have edge tables.
        let mut edge_keyspaces = std::collections::HashSet::new();
        for ((ks, _), meta) in &snap.tables {
            if meta.extensions.get("graph.type") == Some(&"edge".to_string()) {
                edge_keyspaces.insert(ks.clone());
            }
        }

        // Register an adjacency observer and start reconciliation for each keyspace.
        let mut reconciliation_handles = Vec::new();
        let reconciliation_cancel = CancellationToken::new();
        for ks in &edge_keyspaces {
            let observer = Arc::new(AdjacencyIndexObserver::new(Arc::clone(&schema), ks.clone()));
            storage.register_observer(observer);

            // spawn_reconciliation requires a tokio runtime. In test
            // contexts (e.g., proptest without #[tokio::test]), there may
            // be no runtime. Check before spawning.
            if tokio::runtime::Handle::try_current().is_ok() {
                let handle = spawn_reconciliation(
                    Arc::clone(&schema),
                    Arc::clone(&storage),
                    ks.clone(),
                    reconciliation_interval,
                    reconciliation_cancel.child_token(),
                );
                reconciliation_handles.push(handle);
            }
        }

        Self {
            schema,
            storage,
            config,
            reconciliation_handles,
            reconciliation_cancel,
            subscription_registry: Arc::new(SubscriptionRegistry::new(DEFAULT_MAX_SUBSCRIPTIONS)),
        }
    }

    /// Execute a Cypher query: parse -> validate -> plan -> execute.
    pub fn execute(&self, query: &str, keyspace: &str, auth: &AuthContext) -> Result<GraphResult> {
        let statement = parse(query)?;
        let snap = self.schema.snapshot();
        let logical = validate(&snap, auth, keyspace, statement)?;
        let physical = plan(logical)?;
        execute(
            physical,
            &self.storage,
            keyspace,
            &self.config,
            Some(self.schema.virtual_tables()),
            Some(&self.schema),
        )
    }

    /// Explain a query: parse -> validate -> plan (return plan description).
    pub fn explain(&self, query: &str, keyspace: &str, auth: &AuthContext) -> Result<String> {
        let statement = parse(query)?;
        let snap = self.schema.snapshot();
        let logical = validate(&snap, auth, keyspace, statement)?;
        let physical = plan(logical)?;
        Ok(format_plan(&physical))
    }

    /// Execute a subscribe query. Returns the initial snapshot, subscription ID,
    /// poll interval, and delta flag. The HTTP layer manages the actual SSE stream.
    pub fn execute_subscribe(
        &self,
        query: &str,
        keyspace: &str,
        auth: &AuthContext,
    ) -> Result<(GraphResult, Duration, bool)> {
        let statement = parse(query)?;

        // Verify this is actually a SUBSCRIBE statement.
        match &statement {
            Statement::Subscribe { .. } => {}
            Statement::Unsubscribe { .. } => {
                return Err(GraphError::Validation(
                    "use the unsubscribe endpoint for UNSUBSCRIBE queries".to_string(),
                ));
            }
            _ => {
                return Err(GraphError::Validation(
                    "expected a SUBSCRIBE statement".to_string(),
                ));
            }
        }

        let snap = self.schema.snapshot();
        let logical = validate(&snap, auth, keyspace, statement)?;
        let physical = plan(logical)?;

        // Extract interval and delta from the Subscribe plan.
        let (interval, delta) = match &physical {
            PhysicalPlan::Subscribe {
                interval, delta, ..
            } => (*interval, *delta),
            _ => unreachable!("SUBSCRIBE statement must produce Subscribe plan"),
        };

        // Execute the initial snapshot.
        let result = execute(
            physical,
            &self.storage,
            keyspace,
            &self.config,
            Some(self.schema.virtual_tables()),
            Some(&self.schema),
        )?;

        Ok((result, interval, delta))
    }

    /// Get a reference to the subscription registry.
    pub fn subscription_registry(&self) -> &Arc<SubscriptionRegistry> {
        &self.subscription_registry
    }

    /// List vertex and edge tables with their labels in a keyspace.
    pub fn graph_schema(&self, keyspace: &str) -> Result<GraphSchema> {
        let snap = self.schema.snapshot();

        let mut vertices = Vec::new();
        let mut edges = Vec::new();

        for ((ks, _), meta) in &snap.tables {
            if ks != keyspace {
                continue;
            }
            let graph_type = meta.extensions.get("graph.type");
            let graph_label = meta.extensions.get("graph.label");

            if let (Some(gtype), Some(label)) = (graph_type, graph_label) {
                let properties: Vec<String> = meta
                    .columns
                    .keys()
                    .filter(|c| {
                        // Exclude partition key and clustering columns from "properties"
                        // list — those are structural, not user properties.
                        let col = &meta.columns[c.as_str()];
                        col.kind == ferrosa_schema::metadata::column::ColumnKind::Regular
                    })
                    .cloned()
                    .collect();

                let info = LabelInfo {
                    table: meta.name.clone(),
                    label: label.clone(),
                    properties,
                };

                match gtype.as_str() {
                    "vertex" => vertices.push(info),
                    "edge" => edges.push(info),
                    _ => {} // unknown graph.type — skip
                }
            }
        }

        Ok(GraphSchema { vertices, edges })
    }

    /// Cancel and abort reconciliation tasks (for graceful shutdown).
    pub fn shutdown(&mut self) {
        self.reconciliation_cancel.cancel();
        for handle in self.reconciliation_handles.drain(..) {
            handle.abort();
        }
    }
}

impl Drop for GraphEngine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Format a physical plan as a human-readable string for EXPLAIN output.
fn format_plan(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Expand {
            anchor,
            hops,
            return_clause,
        } => {
            let mut out = String::new();
            out.push_str("Expand {\n");
            out.push_str(&format!(
                "  anchor: {}.{} (var: {:?})\n",
                anchor.table.keyspace, anchor.table.table, anchor.var
            ));
            if !anchor.filters.is_empty() {
                out.push_str(&format!(
                    "  filters: {} expression(s)\n",
                    anchor.filters.len()
                ));
            }
            for (i, hop) in hops.iter().enumerate() {
                out.push_str(&format!(
                    "  hop[{}]: edge_label={:?}, direction={:?}, var={:?}\n",
                    i, hop.edge_label, hop.direction, hop.var
                ));
            }
            out.push_str(&format!(
                "  return: {} item(s)\n",
                return_clause.items.len()
            ));
            out.push('}');
            out
        }
        PhysicalPlan::CreateNodes { creates } => {
            let mut out = String::new();
            out.push_str("CreateNodes {\n");
            for (i, op) in creates.iter().enumerate() {
                out.push_str(&format!(
                    "  create[{}]: {}.{} (var: {:?}, props: {})\n",
                    i,
                    op.table.keyspace,
                    op.table.table,
                    op.var,
                    op.props.len()
                ));
            }
            out.push('}');
            out
        }
        PhysicalPlan::SetProperties {
            expand,
            assignments,
        } => {
            let mut out = String::new();
            out.push_str("SetProperties {\n");
            out.push_str(&format!("  expand: {}\n", format_plan(expand)));
            out.push_str(&format!("  assignments: {}\n", assignments.len()));
            out.push('}');
            out
        }
        PhysicalPlan::DeleteNodes {
            expand,
            variables,
            detach,
            variable_tables,
        } => {
            let mut out = String::new();
            out.push_str("DeleteNodes {\n");
            out.push_str(&format!("  expand: {}\n", format_plan(expand)));
            out.push_str(&format!("  variables: {:?}\n", variables));
            out.push_str(&format!("  detach: {}\n", detach));
            out.push_str(&format!("  variable_tables: {:?}\n", variable_tables));
            out.push('}');
            out
        }
        PhysicalPlan::Aggregate {
            inner,
            group_keys,
            projections,
            return_clause,
        } => {
            use crate::planner::physical::AggregateProjection;
            let mut out = String::new();
            out.push_str("Aggregate {\n");
            out.push_str(&format!("  inner: {}\n", format_plan(inner)));
            out.push_str(&format!("  group_keys: {:?}\n", group_keys));
            for (i, proj) in projections.iter().enumerate() {
                match proj {
                    AggregateProjection::GroupKey(idx) => {
                        out.push_str(&format!("  projection[{}]: GroupKey({})\n", i, idx));
                    }
                    AggregateProjection::AggregateFunc { name, .. } => {
                        out.push_str(&format!("  projection[{}]: {}()\n", i, name));
                    }
                }
            }
            out.push_str(&format!(
                "  return: {} item(s)\n",
                return_clause.items.len()
            ));
            out.push('}');
            out
        }
        PhysicalPlan::Subscribe {
            inner,
            interval,
            delta,
            return_clause,
        } => {
            let mut out = String::new();
            out.push_str("Subscribe {\n");
            out.push_str(&format!("  inner: {}\n", format_plan(inner)));
            out.push_str(&format!("  interval: {:?}\n", interval));
            out.push_str(&format!("  delta: {}\n", delta));
            out.push_str(&format!(
                "  return: {} item(s)\n",
                return_clause.items.len()
            ));
            out.push('}');
            out
        }
        PhysicalPlan::ExpandVarLength {
            anchor,
            hop,
            min_hops,
            max_hops,
            return_clause,
        } => {
            let mut out = String::new();
            out.push_str("ExpandVarLength {\n");
            out.push_str(&format!(
                "  anchor: {}.{} (var: {:?})\n",
                anchor.table.keyspace, anchor.table.table, anchor.var
            ));
            out.push_str(&format!(
                "  hop: edge_label={:?}, direction={:?}, var={:?}\n",
                hop.edge_label, hop.direction, hop.var
            ));
            out.push_str(&format!("  range: {}..{}\n", min_hops, max_hops));
            out.push_str(&format!(
                "  return: {} item(s)\n",
                return_clause.items.len()
            ));
            out.push('}');
            out
        }
        PhysicalPlan::WcoJoin {
            plan,
            return_clause,
        } => {
            let mut out = String::new();
            out.push_str("WcoJoin (leapfrog triejoin) {\n");
            out.push_str(&format!("  variables: {:?}\n", plan.variables));
            for (i, rel) in plan.relations.iter().enumerate() {
                out.push_str(&format!(
                    "  relation[{}]: ({})--[{:?} {:?}]-->({})\n",
                    i, rel.src_var, rel.edge_label, rel.direction, rel.dst_var
                ));
            }
            out.push_str(&format!(
                "  return: {} item(s)\n",
                return_clause.items.len()
            ));
            out.push('}');
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_config_defaults() {
        let config = GraphConfig::default();
        assert!(!config.enabled);
        assert_eq!(
            config.reconciliation_interval,
            std::time::Duration::from_secs(300)
        );
    }

    /// Helper to create a StorageEngine for tests using a temp directory.
    fn test_storage_engine(dir: &std::path::Path) -> StorageEngine {
        use ferrosa_storage::{
            CommitLogConfig, CompactionConfig, StorageEngineConfig, SyncStrategyConfig,
        };

        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 4096,
                max_segment_age: std::time::Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        };
        StorageEngine::new(config, None).unwrap()
    }

    /// Helper to create a Schema for tests.
    fn test_schema() -> Schema {
        use ferrosa_schema::{
            AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
            RateLimitConfig, SchemaConfig, TestAuditSink,
        };

        Schema::new(SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap()
    }

    #[test]
    fn graph_schema_empty_keyspace() {
        let schema = Arc::new(test_schema());

        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(test_storage_engine(tmp.path()));

        let engine = GraphEngine::new(
            schema,
            storage,
            GraphEngineConfig::default(),
            std::time::Duration::from_secs(300),
        );

        let gs = engine.graph_schema("nonexistent").unwrap();
        assert!(gs.vertices.is_empty());
        assert!(gs.edges.is_empty());
    }

    #[test]
    fn format_plan_expand() {
        use crate::parser::{Expr, ReturnClause, ReturnItem};
        use crate::planner::logical::ResolvedTable;
        use crate::planner::physical::Anchor;

        let plan = PhysicalPlan::Expand {
            anchor: Anchor {
                var: Some("n".to_string()),
                table: ResolvedTable {
                    keyspace: "social".to_string(),
                    table: "person_v".to_string(),
                    graph_type: "vertex".to_string(),
                    label: "Person".to_string(),
                },
                filters: vec![],
            },
            hops: vec![],
            return_clause: ReturnClause {
                distinct: false,
                items: vec![ReturnItem {
                    expr: Expr::Var("n".to_string()),
                    alias: None,
                }],
                order_by: vec![],
                limit: None,
            },
        };

        let output = format_plan(&plan);
        assert!(output.contains("Expand"));
        assert!(output.contains("social.person_v"));
        assert!(output.contains("1 item(s)"));
    }

    #[test]
    fn label_info_serialize() {
        let info = LabelInfo {
            table: "person_v".to_string(),
            label: "Person".to_string(),
            properties: vec!["name".to_string(), "age".to_string()],
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("person_v"));
        assert!(json.contains("Person"));
        assert!(json.contains("name"));
    }

    #[test]
    fn graph_schema_serialize() {
        let gs = GraphSchema {
            vertices: vec![LabelInfo {
                table: "person_v".to_string(),
                label: "Person".to_string(),
                properties: vec!["name".to_string()],
            }],
            edges: vec![],
        };
        let json = serde_json::to_string(&gs).unwrap();
        assert!(json.contains("vertices"));
        assert!(json.contains("Person"));
    }
}
