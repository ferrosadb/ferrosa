//! GraphEngine: composition root for graph query processing.
//!
//! Wires together the parser, planner, executor, adjacency index observer,
//! and reconciliation loop. Provides the top-level `execute()` and `explain()`
//! entry points consumed by the HTTP endpoint.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use serde::Serialize;
use serde_json::Value;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::Schema;
use ferrosa_storage::{ObserverMode, StorageEngine, WriteObserver};

use tokio_util::sync::CancellationToken;

use crate::adjacency::observer::AdjacencyIndexObserver;
use crate::adjacency::reconcile::{reconcile_once, spawn_reconciliation};
use crate::adjacency::{adjacency_keyspace_name, adjacency_table_metadata};
use crate::error::{GraphError, Result};
use crate::executor::expand::{build_columns, execute, GraphEngineConfig};
use crate::executor::result::{GraphResult, QueryStats};
use crate::executor::subscribe::SubscriptionRegistry;
use crate::parser::{
    parse, Assignment, Expr, Literal, Pattern, ReturnClause, Statement, WithPipeline,
};
use crate::planner::logical::validate;
use crate::planner::physical::{plan, PhysicalPlan};

fn adjacency_keyspace_metadata(
    snap: &ferrosa_schema::SchemaSnapshot,
    keyspace: &str,
) -> ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
    let adj_ks = adjacency_keyspace_name(keyspace);
    if let Some(source) = snap.keyspaces.get(keyspace) {
        ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
            name: adj_ks,
            durable_writes: source.durable_writes,
            replication: source.replication.clone(),
        }
    } else {
        ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
            name: adj_ks,
            durable_writes: true,
            replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: std::collections::HashMap::from([(
                    "replication_factor".to_string(),
                    "1".to_string(),
                )]),
            },
        }
    }
}

fn replication_needs_repair(replication: &ferrosa_schema::ReplicationParams) -> bool {
    match replication.strategy.as_str() {
        "SimpleStrategy" => !replication.options.contains_key("replication_factor"),
        "NetworkTopologyStrategy" => replication.options.is_empty(),
        _ => false,
    }
}

fn empty_match_for_missing_label(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Match { .. } | Statement::MatchWith { .. } | Statement::MatchWithOptional { .. }
    )
}

fn empty_match_result(statement: &Statement) -> GraphResult {
    let columns = match statement {
        Statement::Match { return_clause, .. }
        | Statement::MatchWith { return_clause, .. }
        | Statement::MatchWithOptional { return_clause, .. } => build_columns(return_clause),
        _ => vec![],
    };

    GraphResult {
        columns,
        rows: vec![],
        stats: QueryStats::default(),
    }
}

/// Cluster-aware schema-mutation sink for graph-engine-driven DDL.
///
/// The graph engine auto-creates `system_graph_<keyspace>.adjacency`
/// when it first sees an edge table for a keyspace. On a single node
/// this is straightforward — call `Schema::create_*_internal` and
/// `StorageEngine::register_table`. On a multi-node cluster, those
/// local mutations need to propagate to every replica via the cluster's
/// DDL replication path (pair mode, Raft, etc.) so that follower nodes
/// can apply `MutationForward` writes to the adjacency table.
///
/// Implementations:
/// - `LocalGraphSchemaCoordinator`: single-node / unit-test default.
///   Writes directly to the provided `Schema` instance only.
/// - Production cluster wiring should pass a coordinator that routes
///   the DDL operation through the same path regular CQL DDL takes
///   (see ferrosa-cluster ddl_path / pair::ddl).
pub trait GraphSchemaCoordinator: Send + Sync {
    /// Idempotently create the adjacency keyspace cluster-wide.
    /// Must be a no-op (returning Ok(())) if the keyspace already exists.
    fn apply_create_keyspace(
        &self,
        ks: ferrosa_schema::metadata::keyspace::KeyspaceMetadata,
    ) -> Result<()>;

    /// Idempotently create the adjacency table cluster-wide.
    /// Must be a no-op (returning Ok(())) if the table already exists.
    fn apply_create_table(
        &self,
        table: ferrosa_schema::metadata::table::TableMetadata,
    ) -> Result<()>;
}

/// Local-only coordinator. Backwards-compatible single-node default —
/// applies DDL to the provided Schema instance and nothing else.
/// Cluster deployments should pass a coordinator that fans the DDL
/// out via the cluster's replication path (Raft / pair sync) so all
/// peers register the adjacency table in their local StorageEngine.
pub struct LocalGraphSchemaCoordinator {
    schema: Arc<Schema>,
}

impl LocalGraphSchemaCoordinator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
}

impl GraphSchemaCoordinator for LocalGraphSchemaCoordinator {
    fn apply_create_keyspace(
        &self,
        ks: ferrosa_schema::metadata::keyspace::KeyspaceMetadata,
    ) -> Result<()> {
        self.schema
            .create_keyspace_internal(ks)
            .map_err(|e| GraphError::Validation(format!("create_keyspace_internal: {e}")))
    }

    fn apply_create_table(
        &self,
        table: ferrosa_schema::metadata::table::TableMetadata,
    ) -> Result<()> {
        self.schema
            .create_table_internal(table)
            .map_err(|e| GraphError::Validation(format!("create_table_internal: {e}")))
    }
}

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
            reconciliation_interval: std::time::Duration::ZERO,
            enabled: false,
        }
    }
}

fn background_reconciliation_enabled(interval: std::time::Duration) -> bool {
    !interval.is_zero()
}

/// Reconciler poll cadence. Short enough that CI cluster bring-up
/// (migrate finishes ~30s after node startup) doesn't have to wait
/// long for adjacency registration; large enough that the scan loop
/// is invisible cost in steady state.
const ADJACENCY_RECONCILER_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// Background task: scan the local schema for graph.type=edge tables
/// whose keyspace has no adjacency registered yet on this node, and
/// route the registration through the engine's `GraphSchemaCoordinator`.
///
/// This closes the case where edge-table DDL lands in the local
/// schema *after* the engine's startup scan ran (e.g. a follower node
/// receiving Raft-replicated DDL after the leader applied it). Without
/// this loop, that node never gets its `StorageEngine` notified about
/// `system_graph_<ks>.adjacency` and rejects all derived adjacency
/// mutations forwarded by the coordinator.
fn spawn_adjacency_keyspace_reconciler(
    schema: Arc<Schema>,
    schema_coordinator: Arc<dyn GraphSchemaCoordinator>,
    storage: Arc<StorageEngine>,
    registered: Arc<Mutex<HashSet<String>>>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(ADJACENCY_RECONCILER_INTERVAL) => {}
            }

            let snap = schema.snapshot();
            let edge_keyspaces: HashSet<String> = snap
                .tables
                .iter()
                .filter(|(_, meta)| {
                    meta.extensions.get("graph.type") == Some(&"edge".to_string())
                })
                .map(|((ks, _), _)| ks.clone())
                .collect();

            let pending: Vec<String> = {
                let registered = registered.lock().unwrap();
                edge_keyspaces
                    .into_iter()
                    .filter(|ks| !registered.contains(ks))
                    .collect()
            };
            if pending.is_empty() {
                continue;
            }

            for ks in pending {
                let adj_ks = adjacency_keyspace_name(&ks);
                let snap = schema.snapshot();
                let adj_meta = adjacency_keyspace_metadata(&snap, &ks);
                if !snap.keyspaces.contains_key(&adj_ks) {
                    if let Err(e) = schema_coordinator.apply_create_keyspace(adj_meta) {
                        tracing::warn!(
                            keyspace = %adj_ks,
                            error = %e,
                            "adjacency reconciler: create_keyspace failed; will retry"
                        );
                        continue;
                    }
                }
                let adj_table = adjacency_table_metadata(&ks);
                let snap = schema.snapshot();
                if !snap
                    .tables
                    .contains_key(&(adj_ks.clone(), "adjacency".to_string()))
                {
                    if let Err(e) = schema_coordinator.apply_create_table(adj_table.clone()) {
                        tracing::warn!(
                            keyspace = %adj_ks,
                            error = %e,
                            "adjacency reconciler: create_table failed; will retry"
                        );
                        continue;
                    }
                }
                if let Err(e) = storage.register_table(adj_table.to_storage_schema()) {
                    let msg = e.to_string();
                    let already =
                        msg.contains("already registered") || msg.contains("already exists");
                    if !already {
                        tracing::warn!(
                            keyspace = %adj_ks,
                            error = %e,
                            "adjacency reconciler: storage.register_table failed; will retry"
                        );
                        continue;
                    }
                }
                tracing::info!(
                    keyspace = %ks,
                    adjacency = %adj_ks,
                    "adjacency reconciler: registered new edge keyspace"
                );
                registered.lock().unwrap().insert(ks);
            }
        }
    })
}

/// Default per-connection subscription limit (FMEA F5).
const DEFAULT_MAX_SUBSCRIPTIONS: usize = 8;

/// Central coordinator for graph query processing.
pub struct GraphEngine {
    schema: Arc<Schema>,
    /// Retained to keep the `Arc<StorageEngine>` alive for observers and WritePath.
    #[allow(dead_code)]
    storage: Arc<StorageEngine>,
    write_path: Arc<ArcSwap<WritePath>>,
    config: GraphEngineConfig,
    reconciliation_handles: Vec<tokio::task::JoinHandle<()>>,
    reconciliation_cancel: CancellationToken,
    subscription_registry: Arc<SubscriptionRegistry>,
    registered_adjacency_keyspaces: Arc<Mutex<HashSet<String>>>,
    /// Routes adjacency-keyspace and adjacency-table DDL through the
    /// cluster's replication path. Defaults to a local-only coordinator
    /// (`LocalGraphSchemaCoordinator`) when `GraphEngine::new` is used.
    /// Use `GraphEngine::new_with_coordinator` to inject a cluster-aware
    /// implementation so all replicas register the adjacency table.
    schema_coordinator: Arc<dyn GraphSchemaCoordinator>,
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
        write_path: Arc<ArcSwap<WritePath>>,
        config: GraphEngineConfig,
        reconciliation_interval: std::time::Duration,
    ) -> Self {
        let coordinator: Arc<dyn GraphSchemaCoordinator> =
            Arc::new(LocalGraphSchemaCoordinator::new(Arc::clone(&schema)));
        Self::new_with_coordinator(
            schema,
            storage,
            write_path,
            config,
            reconciliation_interval,
            coordinator,
        )
    }

    /// Like `new`, but routes graph-engine-driven DDL (adjacency keyspace +
    /// adjacency table creation) through the provided coordinator. Use this
    /// in multi-node cluster wiring so every replica registers the adjacency
    /// table; `new` defaults to a local-only coordinator.
    pub fn new_with_coordinator(
        schema: Arc<Schema>,
        storage: Arc<StorageEngine>,
        write_path: Arc<ArcSwap<WritePath>>,
        config: GraphEngineConfig,
        reconciliation_interval: std::time::Duration,
        schema_coordinator: Arc<dyn GraphSchemaCoordinator>,
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
            // Ensure the adjacency keyspace + table are registered with both
            // schema and storage BEFORE wiring the observer. Without this the
            // observer produces derived mutations targeting a table that
            // StorageEngine has never seen, and `apply_derived_mutation` logs
            // "target table not registered — derived mutation dropped" for
            // every edge write — silent data loss for hop queries.
            let adj_ks = adjacency_keyspace_name(ks);
            let adj_meta = adjacency_keyspace_metadata(&snap, ks);
            if let Some(existing) = snap.keyspaces.get(&adj_ks) {
                if replication_needs_repair(&existing.replication) {
                    if let Err(e) = schema.alter_keyspace_internal(
                        &adj_ks,
                        ferrosa_schema::metadata::keyspace::KeyspaceUpdates {
                            replication: Some(adj_meta.replication.clone()),
                            durable_writes: Some(adj_meta.durable_writes),
                        },
                    ) {
                        tracing::error!(
                            keyspace = %adj_ks,
                            error = %e,
                            "graph engine: failed to repair adjacency keyspace replication"
                        );
                        continue;
                    }
                }
            } else if let Err(e) = schema_coordinator.apply_create_keyspace(adj_meta) {
                tracing::error!(
                    keyspace = %adj_ks,
                    error = %e,
                    "graph engine: failed to register adjacency keyspace — \
                     derived mutations will be dropped for {ks}; refusing to \
                     register observer"
                );
                continue;
            }
            let adj_meta = adjacency_table_metadata(ks);
            if let Err(e) = schema_coordinator.apply_create_table(adj_meta.clone()) {
                tracing::error!(
                    keyspace = %adj_ks,
                    error = %e,
                    "graph engine: failed to register adjacency table in schema — \
                     derived mutations and graph traversals will be unavailable for {ks}"
                );
                continue;
            }
            let adj_schema = adj_meta.to_storage_schema();
            if let Err(e) = storage.register_table(adj_schema) {
                // Already-registered is fine (another edge table in the same
                // keyspace shares the adjacency table). Any other error means
                // derived mutations would drop — refuse to start the observer.
                let msg = e.to_string();
                let already = msg.contains("already registered") || msg.contains("already exists");
                if !already {
                    tracing::error!(
                        keyspace = %adj_ks,
                        error = %e,
                        "graph engine: failed to register adjacency table with \
                         StorageEngine — refusing to register observer for {ks}"
                    );
                    continue;
                }
            }

            let observer = Arc::new(AdjacencyIndexObserver::new(Arc::clone(&schema), ks.clone()));

            match observer.mode() {
                ObserverMode::Sync => {
                    // Sync observers apply derived adjacency mutations inline on
                    // the write path, so follow-up MATCH queries can see the
                    // relationship immediately after MERGE returns.
                    storage.register_observer(observer);
                }
                ObserverMode::Async => {
                    if tokio::runtime::Handle::try_current().is_ok() {
                        // Runtime available: register the observer with an engine-side
                        // drain task. The drain task applies derived adjacency mutations
                        // through the full write path and catches observer panics loudly.
                        storage.register_async_observer_with_drain(
                            observer as Arc<dyn ferrosa_storage::WriteObserver>,
                            Arc::clone(&storage),
                        );
                    } else {
                        // No runtime available (e.g., proptest sync context): fall back
                        // to register_observer. Async observers registered this way will
                        // drop derived writes because nothing drains the channel, but the
                        // engine still comes up without panicking.
                        storage.register_observer(observer);
                    }
                }
            }

            // spawn_reconciliation requires a tokio runtime. In test
            // contexts (e.g., proptest without #[tokio::test]), there may
            // be no runtime. Check before spawning.
            if background_reconciliation_enabled(reconciliation_interval)
                && tokio::runtime::Handle::try_current().is_ok()
            {
                let handle = spawn_reconciliation(
                    Arc::clone(&schema),
                    Arc::new(WritePath::direct(Arc::clone(&storage))),
                    ks.clone(),
                    reconciliation_interval,
                    reconciliation_cancel.child_token(),
                );
                reconciliation_handles.push(handle);
            }
        }

        let registered_adjacency_keyspaces = Arc::new(Mutex::new(edge_keyspaces));

        // Background reconciler: every cluster node applies Raft-replicated
        // DDL to its local schema independently, and the migrate step often
        // lands *after* GraphEngine startup (in CI especially). Without this
        // task, edge tables added post-startup never get an adjacency
        // registration on the node that missed them at startup, and
        // MutationForward writes to system_graph_<ks>.adjacency are rejected
        // with "table not registered" — the bug captured by
        // public_graph_write_round_trip_for_co_occurs_edges. The reconciler
        // periodically scans the local schema for graph.type=edge tables
        // and ensures adjacency is registered for each, idempotently.
        if tokio::runtime::Handle::try_current().is_ok() {
            spawn_adjacency_keyspace_reconciler(
                Arc::clone(&schema),
                Arc::clone(&schema_coordinator),
                Arc::clone(&storage),
                Arc::clone(&registered_adjacency_keyspaces),
                reconciliation_cancel.child_token(),
            );
        }

        Self {
            schema,
            storage,
            write_path,
            config,
            reconciliation_handles,
            reconciliation_cancel,
            subscription_registry: Arc::new(SubscriptionRegistry::new(DEFAULT_MAX_SUBSCRIPTIONS)),
            registered_adjacency_keyspaces,
            schema_coordinator,
        }
    }

    fn ensure_adjacency_storage_for_keyspace(&self, keyspace: &str) -> Result<bool> {
        let snap = self.schema.snapshot();
        let has_edge_table = snap.tables.iter().any(|((ks, _), meta)| {
            ks == keyspace && meta.extensions.get("graph.type") == Some(&"edge".to_string())
        });
        if !has_edge_table {
            return Ok(false);
        }

        let adj_ks = adjacency_keyspace_name(keyspace);
        let adj_meta = adjacency_keyspace_metadata(&snap, keyspace);
        if !snap.keyspaces.contains_key(&adj_ks) {
            self.schema_coordinator
                .apply_create_keyspace(adj_meta)
                .map_err(|e| {
                    GraphError::Validation(format!(
                        "failed to register graph adjacency keyspace {adj_ks}: {e}"
                    ))
                })?;
        }

        let snap = self.schema.snapshot();
        if !snap
            .tables
            .contains_key(&(adj_ks.clone(), "adjacency".to_string()))
        {
            self.schema_coordinator
                .apply_create_table(adjacency_table_metadata(keyspace))
                .map_err(|e| {
                    GraphError::Validation(format!(
                        "failed to register graph adjacency table {adj_ks}.adjacency: {e}"
                    ))
                })?;
        }

        let adj_schema = adjacency_table_metadata(keyspace).to_storage_schema();
        if let Err(e) = self.storage.register_table(adj_schema) {
            let msg = e.to_string();
            let already = msg.contains("already registered") || msg.contains("already exists");
            if !already {
                return Err(GraphError::Storage(e));
            }
        }
        let should_register_observer = {
            let mut registered = self.registered_adjacency_keyspaces.lock().map_err(|_| {
                GraphError::Internal("graph adjacency keyspace registry lock poisoned".to_string())
            })?;
            registered.insert(keyspace.to_string())
        };
        if should_register_observer {
            self.storage
                .register_observer(Arc::new(AdjacencyIndexObserver::new(
                    Arc::clone(&self.schema),
                    keyspace.to_string(),
                )));
        }

        Ok(should_register_observer)
    }

    /// Execute a Cypher query: parse -> validate -> plan -> execute.
    pub async fn execute(
        &self,
        query: &str,
        keyspace: &str,
        auth: &AuthContext,
    ) -> Result<GraphResult> {
        self.execute_with_params(query, keyspace, auth, &HashMap::new())
            .await
    }

    pub async fn execute_with_params(
        &self,
        query: &str,
        keyspace: &str,
        auth: &AuthContext,
        params: &HashMap<String, Value>,
    ) -> Result<GraphResult> {
        let adjacency_registered = self.ensure_adjacency_storage_for_keyspace(keyspace)?;
        if adjacency_registered {
            let wp = self.write_path.load();
            let metrics = reconcile_once(&self.schema, &wp, keyspace).await;
            tracing::info!(
                keyspace,
                entries_checked = metrics.entries_checked,
                entries_repaired = metrics.entries_repaired,
                "graph engine: reconciled newly registered adjacency keyspace"
            );
        }
        let statement = bind_statement_params(parse(query)?, params)?;
        let snap = self.schema.snapshot();
        let logical = match validate(&snap, auth, keyspace, statement.clone()) {
            Ok(logical) => logical,
            Err(GraphError::Validation(msg))
                if msg.contains("no table with graph.label")
                    && empty_match_for_missing_label(&statement) =>
            {
                return Ok(empty_match_result(&statement));
            }
            Err(e) => return Err(e),
        };
        let physical = plan(logical)?;
        let wp = self.write_path.load();
        execute(
            physical,
            &wp,
            keyspace,
            &self.config,
            Some(self.schema.virtual_tables()),
            Some(&self.schema),
        )
        .await
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
    pub async fn execute_subscribe(
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
        let wp = self.write_path.load();
        let result = execute(
            physical,
            &wp,
            keyspace,
            &self.config,
            Some(self.schema.virtual_tables()),
            Some(&self.schema),
        )
        .await?;

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

fn json_param_to_literal(name: &str, value: &Value) -> Result<Literal> {
    match value {
        Value::Null => Ok(Literal::Null),
        Value::Bool(v) => Ok(Literal::Bool(*v)),
        Value::String(v) => Ok(Literal::String(v.clone())),
        Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                Ok(Literal::Integer(i))
            } else if let Some(f) = v.as_f64() {
                Ok(Literal::Float(f))
            } else {
                Err(GraphError::Validation(format!(
                    "parameter ${name} number cannot be represented"
                )))
            }
        }
        Value::Array(_) | Value::Object(_) => Err(GraphError::Validation(format!(
            "parameter ${name} must be a scalar value"
        ))),
    }
}

fn bind_expr_params(expr: Expr, params: &HashMap<String, Value>) -> Result<Expr> {
    Ok(match expr {
        Expr::Parameter(name) => {
            let value = params.get(&name).ok_or_else(|| {
                GraphError::Validation(format!("missing required query parameter ${name}"))
            })?;
            Expr::Literal(json_param_to_literal(&name, value)?)
        }
        Expr::Function { name, args } => Expr::Function {
            name,
            args: args
                .into_iter()
                .map(|arg| bind_expr_params(arg, params))
                .collect::<Result<Vec<_>>>()?,
        },
        Expr::Distinct(inner) => Expr::Distinct(Box::new(bind_expr_params(*inner, params)?)),
        Expr::Comparison { left, op, right } => Expr::Comparison {
            left: Box::new(bind_expr_params(*left, params)?),
            op,
            right: Box::new(bind_expr_params(*right, params)?),
        },
        Expr::In { value, list } => Expr::In {
            value: Box::new(bind_expr_params(*value, params)?),
            list: Box::new(bind_expr_params(*list, params)?),
        },
        Expr::Arithmetic { left, op, right } => Expr::Arithmetic {
            left: Box::new(bind_expr_params(*left, params)?),
            op,
            right: Box::new(bind_expr_params(*right, params)?),
        },
        Expr::And(left, right) => Expr::And(
            Box::new(bind_expr_params(*left, params)?),
            Box::new(bind_expr_params(*right, params)?),
        ),
        Expr::Or(left, right) => Expr::Or(
            Box::new(bind_expr_params(*left, params)?),
            Box::new(bind_expr_params(*right, params)?),
        ),
        Expr::Not(inner) => Expr::Not(Box::new(bind_expr_params(*inner, params)?)),
        Expr::PatternPredicate {
            start_var,
            hops,
            negated,
        } => Expr::PatternPredicate {
            start_var,
            hops: hops
                .into_iter()
                .map(|hop| {
                    Ok(crate::parser::PatternPredicateHop {
                        rel_type: hop.rel_type,
                        direction: hop.direction,
                        target_label: hop.target_label,
                        target_props: bind_prop_map_params(hop.target_props, params)?,
                    })
                })
                .collect::<std::result::Result<Vec<_>, GraphError>>()?,
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(bind_expr_params(*inner, params)?)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(bind_expr_params(*inner, params)?)),
        Expr::List(items) => Expr::List(
            items
                .into_iter()
                .map(|item| bind_expr_params(item, params))
                .collect::<Result<Vec<_>>>()?,
        ),
        Expr::ListPredicate {
            kind,
            var,
            list,
            predicate,
        } => Expr::ListPredicate {
            kind,
            var,
            list: Box::new(bind_expr_params(*list, params)?),
            predicate: Box::new(bind_expr_params(*predicate, params)?),
        },
        Expr::Map(props) => Expr::Map(bind_prop_map_params(props, params)?),
        Expr::Index { target, index } => Expr::Index {
            target: Box::new(bind_expr_params(*target, params)?),
            index: Box::new(bind_expr_params(*index, params)?),
        },
        Expr::Slice { target, start, end } => Expr::Slice {
            target: Box::new(bind_expr_params(*target, params)?),
            start: start
                .map(|expr| bind_expr_params(*expr, params).map(Box::new))
                .transpose()?,
            end: end
                .map(|expr| bind_expr_params(*expr, params).map(Box::new))
                .transpose()?,
        },
        other => other,
    })
}

fn bind_prop_map_params(
    props: Vec<(String, Expr)>,
    params: &HashMap<String, Value>,
) -> Result<Vec<(String, Expr)>> {
    props
        .into_iter()
        .map(|(name, expr)| Ok((name, bind_expr_params(expr, params)?)))
        .collect()
}

fn bind_pattern_params(pattern: Pattern, params: &HashMap<String, Value>) -> Result<Pattern> {
    Ok(match pattern {
        Pattern::Node { var, label, props } => Pattern::Node {
            var,
            label,
            props: bind_prop_map_params(props, params)?,
        },
        Pattern::Rel {
            var,
            rel_type,
            direction,
            props,
            length_range,
        } => Pattern::Rel {
            var,
            rel_type,
            direction,
            props: bind_prop_map_params(props, params)?,
            length_range,
        },
        Pattern::Path(elements) => Pattern::Path(
            elements
                .into_iter()
                .map(|p| bind_pattern_params(p, params))
                .collect::<Result<Vec<_>>>()?,
        ),
    })
}

fn bind_patterns_params(
    patterns: Vec<Pattern>,
    params: &HashMap<String, Value>,
) -> Result<Vec<Pattern>> {
    patterns
        .into_iter()
        .map(|pattern| bind_pattern_params(pattern, params))
        .collect()
}

fn bind_return_clause_params(
    clause: ReturnClause,
    params: &HashMap<String, Value>,
) -> Result<ReturnClause> {
    Ok(ReturnClause {
        distinct: clause.distinct,
        items: clause
            .items
            .into_iter()
            .map(|item| {
                Ok(crate::parser::ReturnItem {
                    expr: bind_expr_params(item.expr, params)?,
                    alias: item.alias,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        order_by: clause
            .order_by
            .into_iter()
            .map(|item| {
                Ok(crate::parser::OrderItem {
                    expr: bind_expr_params(item.expr, params)?,
                    direction: item.direction,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        limit: clause.limit,
    })
}

fn bind_assignments_params(
    assignments: Vec<Assignment>,
    params: &HashMap<String, Value>,
) -> Result<Vec<Assignment>> {
    assignments
        .into_iter()
        .map(|assignment| {
            Ok(Assignment {
                var: assignment.var,
                property: assignment.property,
                value: bind_expr_params(assignment.value, params)?,
            })
        })
        .collect()
}

fn bind_statement_params(
    statement: Statement,
    params: &HashMap<String, Value>,
) -> Result<Statement> {
    Ok(match statement {
        Statement::Match {
            pattern,
            where_clause,
            return_clause,
        } => Statement::Match {
            pattern: bind_patterns_params(pattern, params)?,
            where_clause: where_clause
                .map(|expr| bind_expr_params(expr, params))
                .transpose()?,
            return_clause: bind_return_clause_params(return_clause, params)?,
        },
        Statement::Union { arms, all } => Statement::Union {
            arms: arms
                .into_iter()
                .map(|arm| bind_statement_params(arm, params))
                .collect::<Result<Vec<_>>>()?,
            all,
        },
        Statement::Unwind {
            expr,
            var,
            with_pipeline,
            return_clause,
        } => Statement::Unwind {
            expr: bind_expr_params(expr, params)?,
            var,
            with_pipeline: with_pipeline
                .map(|pipeline| {
                    Ok::<WithPipeline, GraphError>(WithPipeline {
                        clause: bind_return_clause_params(pipeline.clause, params)?,
                        where_clause: pipeline
                            .where_clause
                            .map(|expr| bind_expr_params(expr, params))
                            .transpose()?,
                    })
                })
                .transpose()?,
            return_clause: bind_return_clause_params(return_clause, params)?,
        },
        Statement::Return { return_clause } => Statement::Return {
            return_clause: bind_return_clause_params(return_clause, params)?,
        },
        Statement::MatchWith {
            pattern,
            where_clause,
            with_pipeline,
            return_clause,
        } => Statement::MatchWith {
            pattern: bind_patterns_params(pattern, params)?,
            where_clause: where_clause
                .map(|expr| bind_expr_params(expr, params))
                .transpose()?,
            with_pipeline: WithPipeline {
                clause: bind_return_clause_params(with_pipeline.clause, params)?,
                where_clause: with_pipeline
                    .where_clause
                    .map(|expr| bind_expr_params(expr, params))
                    .transpose()?,
            },
            return_clause: bind_return_clause_params(return_clause, params)?,
        },
        Statement::MatchWithOptional {
            pattern,
            where_clause,
            optional_pattern,
            optional_where_clause,
            return_clause,
        } => Statement::MatchWithOptional {
            pattern: bind_patterns_params(pattern, params)?,
            where_clause: where_clause
                .map(|expr| bind_expr_params(expr, params))
                .transpose()?,
            optional_pattern: bind_patterns_params(optional_pattern, params)?,
            optional_where_clause: optional_where_clause
                .map(|expr| bind_expr_params(expr, params))
                .transpose()?,
            return_clause: bind_return_clause_params(return_clause, params)?,
        },
        Statement::Create {
            patterns,
            return_clause,
        } => Statement::Create {
            patterns: bind_patterns_params(patterns, params)?,
            return_clause: return_clause
                .map(|clause| bind_return_clause_params(clause, params))
                .transpose()?,
        },
        Statement::Set {
            pattern,
            where_clause,
            assignments,
        } => Statement::Set {
            pattern: bind_patterns_params(pattern, params)?,
            where_clause: where_clause
                .map(|expr| bind_expr_params(expr, params))
                .transpose()?,
            assignments: bind_assignments_params(assignments, params)?,
        },
        Statement::Delete {
            pattern,
            where_clause,
            detach,
            variables,
        } => Statement::Delete {
            pattern: bind_patterns_params(pattern, params)?,
            where_clause: where_clause
                .map(|expr| bind_expr_params(expr, params))
                .transpose()?,
            detach,
            variables,
        },
        Statement::Subscribe {
            inner,
            interval,
            delta,
        } => Statement::Subscribe {
            inner: Box::new(bind_statement_params(*inner, params)?),
            interval,
            delta,
        },
        Statement::Merge {
            patterns,
            set_clause,
            return_clause,
        } => Statement::Merge {
            patterns: bind_patterns_params(patterns, params)?,
            set_clause: bind_assignments_params(set_clause, params)?,
            return_clause: return_clause
                .map(|clause| bind_return_clause_params(clause, params))
                .transpose()?,
        },
        Statement::Unsubscribe { stream_id } => Statement::Unsubscribe { stream_id },
    })
}

/// Format a physical plan as a human-readable string for EXPLAIN output.
fn format_plan(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Union { arms, all } => {
            format!("Union {{ all: {all}, arms: {} }}", arms.len())
        }
        PhysicalPlan::Unwind {
            var, return_clause, ..
        } => {
            format!(
                "Unwind {{ var: {var}, return: {} item(s) }}",
                return_clause.items.len()
            )
        }
        PhysicalPlan::ReturnOnly { return_clause } => {
            format!(
                "ReturnOnly {{ return: {} item(s) }}",
                return_clause.items.len()
            )
        }
        PhysicalPlan::Expand {
            anchor,
            hops,
            optional_hops,
            with_pipeline,
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
            if with_pipeline.is_some() {
                out.push_str(
                    "  with: pipeline
",
                );
            }
            for (i, hop) in optional_hops.iter().enumerate() {
                out.push_str(&format!(
                    "  optional_hop[{}]: edge_label={:?}, direction={:?}, var={:?}\n",
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
        PhysicalPlan::CreateNodes { creates, .. } => {
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
            variable_tables,
        } => {
            let mut out = String::new();
            out.push_str("SetProperties {\n");
            out.push_str(&format!("  expand: {}\n", format_plan(expand)));
            out.push_str(&format!("  assignments: {}\n", assignments.len()));
            out.push_str(&format!("  variable_tables: {:?}\n", variable_tables));
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
        PhysicalPlan::MergeUpsert {
            merges,
            set_clause,
            return_clause,
        } => {
            let mut out = String::new();
            out.push_str("MergeUpsert {\n");
            for (i, op) in merges.iter().enumerate() {
                out.push_str(&format!(
                    "  merge[{}]: {}.{} (var: {:?}, match_props: {})\n",
                    i,
                    op.table.keyspace,
                    op.table.table,
                    op.var,
                    op.match_props.len()
                ));
            }
            out.push_str(&format!(
                "  set_clause: {} assignment(s)\n",
                set_clause.len()
            ));
            if let Some(rc) = return_clause {
                out.push_str(&format!("  return: {} item(s)\n", rc.items.len()));
            }
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
        assert_eq!(config.reconciliation_interval, std::time::Duration::ZERO);
        assert!(!background_reconciliation_enabled(
            config.reconciliation_interval
        ));
    }

    #[test]
    fn positive_reconciliation_interval_enables_background_safety_net() {
        assert!(background_reconciliation_enabled(
            std::time::Duration::from_secs(300)
        ));
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
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
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

        let write_path = Arc::new(arc_swap::ArcSwap::from_pointee(
            ferrosa_cluster::write_path::WritePath::direct(Arc::clone(&storage)),
        ));
        let engine = GraphEngine::new(
            schema,
            storage,
            write_path,
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
                props: vec![],
                filters: vec![],
            },
            hops: vec![],
            optional_hops: vec![],
            with_pipeline: None,
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

    #[test]
    fn adjacency_keyspace_inherits_source_replication() {
        let schema = test_schema();
        schema
            .create_keyspace_internal(ferrosa_schema::KeyspaceMetadata {
                name: "agent_memory".to_string(),
                durable_writes: false,
                replication: ferrosa_schema::ReplicationParams {
                    strategy: "NetworkTopologyStrategy".to_string(),
                    options: std::collections::HashMap::from([(
                        "datacenter1".to_string(),
                        "3".to_string(),
                    )]),
                },
            })
            .unwrap();

        let snap = schema.snapshot();
        let adj = adjacency_keyspace_metadata(&snap, "agent_memory");
        assert_eq!(adj.name, "system_graph_agent_memory");
        assert!(!adj.durable_writes);
        assert_eq!(adj.replication.strategy, "NetworkTopologyStrategy");
        assert_eq!(
            adj.replication.options.get("datacenter1"),
            Some(&"3".to_string())
        );
    }

    #[test]
    fn adjacency_keyspace_defaults_to_simple_strategy_rf1() {
        let snap = test_schema().snapshot();
        let adj = adjacency_keyspace_metadata(&snap, "missing");
        assert_eq!(adj.name, "system_graph_missing");
        assert_eq!(adj.replication.strategy, "SimpleStrategy");
        assert_eq!(
            adj.replication.options.get("replication_factor"),
            Some(&"1".to_string())
        );
    }

    #[test]
    fn replication_repair_detects_missing_simple_strategy_rf() {
        let broken = ferrosa_schema::ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: std::collections::HashMap::new(),
        };
        assert!(replication_needs_repair(&broken));
    }

    #[test]
    fn engine_startup_re_registers_adjacency_table_in_schema() {
        use std::collections::{HashMap, HashSet};

        use ferrosa_schema::{
            ClusteringOrder, ColumnKind, ColumnMetadata, TableFlag, TableMetadata, TableParams,
        };
        use indexmap::IndexMap;

        let schema = Arc::new(test_schema());
        schema
            .create_keyspace_internal(ferrosa_schema::KeyspaceMetadata {
                name: "agent_memory".to_string(),
                durable_writes: true,
                replication: ferrosa_schema::ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: std::collections::HashMap::from([(
                        "replication_factor".to_string(),
                        "1".to_string(),
                    )]),
                },
            })
            .unwrap();

        let mut extensions = HashMap::new();
        extensions.insert("graph.type".to_string(), "edge".to_string());
        extensions.insert("graph.label".to_string(), "TYPED_EDGE".to_string());
        extensions.insert("graph.source".to_string(), "src_id".to_string());
        extensions.insert("graph.target".to_string(), "dst_id".to_string());
        extensions.insert("graph.source_label".to_string(), "Entity".to_string());
        extensions.insert("graph.target_label".to_string(), "Entity".to_string());

        let mut columns = IndexMap::new();
        columns.insert(
            "tenant_id".to_string(),
            ColumnMetadata {
                name: "tenant_id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "session_id".to_string(),
            ColumnMetadata {
                name: "session_id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 1,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "src_id".to_string(),
            ColumnMetadata {
                name: "src_id".to_string(),
                kind: ColumnKind::Clustering,
                position: 0,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::Asc,
                mask: None,
            },
        );
        columns.insert(
            "edge_type".to_string(),
            ColumnMetadata {
                name: "edge_type".to_string(),
                kind: ColumnKind::Clustering,
                position: 1,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::Asc,
                mask: None,
            },
        );
        columns.insert(
            "dst_id".to_string(),
            ColumnMetadata {
                name: "dst_id".to_string(),
                kind: ColumnKind::Clustering,
                position: 2,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::Asc,
                mask: None,
            },
        );
        columns.insert(
            "weight".to_string(),
            ColumnMetadata {
                name: "weight".to_string(),
                kind: ColumnKind::Regular,
                position: -1,
                column_type: "float".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );

        schema
            .create_table_internal(TableMetadata {
                keyspace: "agent_memory".to_string(),
                name: "typed_edges".to_string(),
                id: uuid::Uuid::new_v4(),
                columns,
                partition_key: vec!["tenant_id".to_string(), "session_id".to_string()],
                clustering_key: vec![
                    ("src_id".to_string(), ClusteringOrder::Asc),
                    ("edge_type".to_string(), ClusteringOrder::Asc),
                    ("dst_id".to_string(), ClusteringOrder::Asc),
                ],
                params: TableParams::default(),
                flags: HashSet::from([TableFlag::Compound]),
                extensions,
                is_system: false,
            })
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(test_storage_engine(dir.path()));
        storage
            .register_table(
                schema
                    .snapshot()
                    .tables
                    .get(&("agent_memory".to_string(), "typed_edges".to_string()))
                    .unwrap()
                    .to_storage_schema(),
            )
            .unwrap();

        let _engine = GraphEngine::new(
            Arc::clone(&schema),
            storage.clone(),
            Arc::new(arc_swap::ArcSwap::from_pointee(
                ferrosa_cluster::write_path::WritePath::direct(storage),
            )),
            GraphEngineConfig::default(),
            std::time::Duration::from_secs(60),
        );

        let snap = schema.snapshot();
        assert!(
            snap.keyspaces.contains_key("system_graph_agent_memory"),
            "graph engine startup must restore the adjacency keyspace into the live schema"
        );
        assert!(
            snap.tables.contains_key(&(
                "system_graph_agent_memory".to_string(),
                "adjacency".to_string()
            )),
            "graph engine startup must restore the adjacency table into the live schema"
        );
    }
}
