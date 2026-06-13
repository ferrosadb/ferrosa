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
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::Schema;
use ferrosa_storage::StorageEngine;

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

fn pattern_requires_adjacency(pattern: &Pattern) -> bool {
    match pattern {
        Pattern::Rel { .. } => true,
        Pattern::Path(parts) => parts.iter().any(pattern_requires_adjacency),
        Pattern::Node { props, .. } => prop_map_requires_adjacency(props),
    }
}

fn prop_map_requires_adjacency(props: &[(String, Expr)]) -> bool {
    props.iter().any(|(_, expr)| expr_requires_adjacency(expr))
}

fn expr_requires_adjacency(expr: &Expr) -> bool {
    match expr {
        Expr::Function { args, .. } | Expr::List(args) => args.iter().any(expr_requires_adjacency),
        Expr::Distinct(inner) | Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            expr_requires_adjacency(inner)
        }
        Expr::Comparison { left, right, .. }
        | Expr::In {
            value: left,
            list: right,
        }
        | Expr::Arithmetic { left, right, .. }
        | Expr::And(left, right)
        | Expr::Or(left, right)
        | Expr::Index {
            target: left,
            index: right,
        } => expr_requires_adjacency(left) || expr_requires_adjacency(right),
        Expr::ListPredicate {
            list, predicate, ..
        } => expr_requires_adjacency(list) || expr_requires_adjacency(predicate),
        Expr::Map(props) => prop_map_requires_adjacency(props),
        Expr::Slice { target, start, end } => {
            expr_requires_adjacency(target)
                || start.as_deref().is_some_and(expr_requires_adjacency)
                || end.as_deref().is_some_and(expr_requires_adjacency)
        }
        Expr::PatternPredicate { hops, .. } => !hops.is_empty(),
        Expr::Var(_) | Expr::Property { .. } | Expr::Literal(_) | Expr::Parameter(_) => false,
    }
}

fn return_clause_requires_adjacency(return_clause: &ReturnClause) -> bool {
    return_clause
        .items
        .iter()
        .any(|item| expr_requires_adjacency(&item.expr))
        || return_clause
            .order_by
            .iter()
            .any(|item| expr_requires_adjacency(&item.expr))
}

fn with_pipeline_requires_adjacency(with_pipeline: &WithPipeline) -> bool {
    return_clause_requires_adjacency(&with_pipeline.clause)
        || with_pipeline
            .where_clause
            .as_ref()
            .is_some_and(expr_requires_adjacency)
}

fn statement_requires_adjacency(statement: &Statement) -> bool {
    match statement {
        Statement::Match {
            pattern,
            where_clause,
            return_clause,
        } => {
            pattern.iter().any(pattern_requires_adjacency)
                || where_clause.as_ref().is_some_and(expr_requires_adjacency)
                || return_clause_requires_adjacency(return_clause)
        }
        Statement::MatchWith {
            pattern,
            where_clause,
            with_pipeline,
            return_clause,
        } => {
            pattern.iter().any(pattern_requires_adjacency)
                || where_clause.as_ref().is_some_and(expr_requires_adjacency)
                || with_pipeline_requires_adjacency(with_pipeline)
                || return_clause_requires_adjacency(return_clause)
        }
        Statement::Return { return_clause } => return_clause_requires_adjacency(return_clause),
        Statement::Unwind {
            expr,
            with_pipeline,
            return_clause,
            ..
        } => {
            expr_requires_adjacency(expr)
                || with_pipeline
                    .as_ref()
                    .is_some_and(with_pipeline_requires_adjacency)
                || return_clause_requires_adjacency(return_clause)
        }
        Statement::Union { arms, .. } => arms.iter().any(statement_requires_adjacency),
        Statement::MatchWithOptional {
            pattern,
            where_clause,
            optional_pattern,
            optional_where_clause,
            return_clause,
        } => {
            pattern.iter().any(pattern_requires_adjacency)
                || optional_pattern.iter().any(pattern_requires_adjacency)
                || where_clause.as_ref().is_some_and(expr_requires_adjacency)
                || optional_where_clause
                    .as_ref()
                    .is_some_and(expr_requires_adjacency)
                || return_clause_requires_adjacency(return_clause)
        }
        Statement::Create {
            patterns,
            return_clause,
        } => {
            patterns.iter().any(pattern_requires_adjacency)
                || return_clause
                    .as_ref()
                    .is_some_and(return_clause_requires_adjacency)
        }
        Statement::Set {
            pattern,
            where_clause,
            assignments,
        } => {
            pattern.iter().any(pattern_requires_adjacency)
                || where_clause.as_ref().is_some_and(expr_requires_adjacency)
                || assignments
                    .iter()
                    .any(|assignment| expr_requires_adjacency(&assignment.value))
        }
        Statement::Remove {
            pattern,
            where_clause,
            ..
        } => {
            pattern.iter().any(pattern_requires_adjacency)
                || where_clause.as_ref().is_some_and(expr_requires_adjacency)
        }
        Statement::Delete {
            pattern,
            where_clause,
            ..
        } => {
            pattern.iter().any(pattern_requires_adjacency)
                || where_clause.as_ref().is_some_and(expr_requires_adjacency)
        }
        Statement::Subscribe { inner, .. } => statement_requires_adjacency(inner),
        Statement::Unsubscribe { .. } => false,
        Statement::Merge {
            patterns,
            set_clause,
            return_clause,
        } => {
            patterns.iter().any(pattern_requires_adjacency)
                || set_clause
                    .iter()
                    .any(|assignment| expr_requires_adjacency(&assignment.value))
                || return_clause
                    .as_ref()
                    .is_some_and(return_clause_requires_adjacency)
        }
    }
}

/// Cluster-aware schema-mutation sink for graph-engine-driven DDL.
///
/// The graph engine auto-creates `system_graph_<keyspace>.adjacency`
/// when it first sees an edge table for a keyspace. The DDL must take
/// the same path as regular CQL `CREATE TABLE`: through `DdlPath`, so
/// that the cluster state machine applies it on every replica (schema
/// + `StorageEngine::register_table`). Direct calls to
/// `Schema::create_*_internal` skip the cluster apply and leave
/// replicas unable to accept `MutationForward` writes against the
/// system table.
///
/// Implementations:
/// - `LocalGraphSchemaCoordinator`: single-node / unit-test default.
///   Applies DDL to the provided `Schema` only (matches `DdlPath::Direct`).
/// - `ClusterGraphSchemaCoordinator`: production wiring. Holds the same
///   `Arc<ArcSwap<DdlPath>>` regular CQL uses, so adjacency DDL goes
///   through `DdlPath::execute(...)` and reaches every replica.
#[async_trait]
pub trait GraphSchemaCoordinator: Send + Sync {
    /// Idempotently create the adjacency keyspace cluster-wide.
    /// Must be a no-op (returning `Ok(())`) if the keyspace already exists.
    async fn apply_create_keyspace(
        &self,
        ks: ferrosa_schema::metadata::keyspace::KeyspaceMetadata,
    ) -> Result<()>;

    /// Idempotently create the adjacency table cluster-wide.
    /// Must be a no-op (returning `Ok(())`) if the table already exists.
    async fn apply_create_table(
        &self,
        table: ferrosa_schema::metadata::table::TableMetadata,
    ) -> Result<()>;
}

/// Local-only coordinator. Single-node / unit-test default — applies
/// DDL to the provided Schema instance and nothing else. The trait is
/// async for symmetry with the cluster coordinator; the local impl
/// completes synchronously.
pub struct LocalGraphSchemaCoordinator {
    schema: Arc<Schema>,
}

impl LocalGraphSchemaCoordinator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
}

#[async_trait]
impl GraphSchemaCoordinator for LocalGraphSchemaCoordinator {
    async fn apply_create_keyspace(
        &self,
        ks: ferrosa_schema::metadata::keyspace::KeyspaceMetadata,
    ) -> Result<()> {
        self.schema
            .create_keyspace_internal(ks)
            .map_err(|e| GraphError::Validation(format!("create_keyspace_internal: {e}")))
    }

    async fn apply_create_table(
        &self,
        table: ferrosa_schema::metadata::table::TableMetadata,
    ) -> Result<()> {
        self.schema
            .create_table_internal(table)
            .map_err(|e| GraphError::Validation(format!("create_table_internal: {e}")))
    }
}

/// Cluster-aware coordinator. Routes adjacency DDL through the same
/// `DdlPath` regular CQL `CREATE TABLE` uses, so every replica's state
/// machine applies the DDL — registering the adjacency table in its
/// local schema and storage engine.
///
/// `Direct` (standalone) and `Pair` modes are handled by `DdlPath::execute`
/// itself; the cluster coordinator doesn't special-case them.
pub struct ClusterGraphSchemaCoordinator {
    ddl_path: Arc<ArcSwap<ferrosa_cluster::ddl_path::DdlPath>>,
}

impl ClusterGraphSchemaCoordinator {
    pub fn new(ddl_path: Arc<ArcSwap<ferrosa_cluster::ddl_path::DdlPath>>) -> Self {
        Self { ddl_path }
    }
}

#[async_trait]
impl GraphSchemaCoordinator for ClusterGraphSchemaCoordinator {
    async fn apply_create_keyspace(
        &self,
        ks: ferrosa_schema::metadata::keyspace::KeyspaceMetadata,
    ) -> Result<()> {
        let guard = self.ddl_path.load();
        guard
            .execute(ferrosa_cluster::pair::ddl::DdlOperation::CreateKeyspace(ks))
            .await
            .map_err(|e| GraphError::Validation(format!("cluster DDL CreateKeyspace: {e}")))
    }

    async fn apply_create_table(
        &self,
        table: ferrosa_schema::metadata::table::TableMetadata,
    ) -> Result<()> {
        let guard = self.ddl_path.load();
        guard
            .execute(ferrosa_cluster::pair::ddl::DdlOperation::CreateTable(
                Box::new(table),
            ))
            .await
            .map_err(|e| GraphError::Validation(format!("cluster DDL CreateTable: {e}")))
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

/// Default per-connection subscription limit (FMEA F5). Operators
/// can tune via `FERROSA_GRAPH_MAX_SUBSCRIPTIONS`; zero or unparseable
/// values fall back to this default.
pub const DEFAULT_MAX_SUBSCRIPTIONS: usize = 8;

fn max_subscriptions() -> usize {
    std::env::var("FERROSA_GRAPH_MAX_SUBSCRIPTIONS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_MAX_SUBSCRIPTIONS)
}

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
    registered_adjacency_keyspaces: Mutex<HashSet<String>>,
    /// Routes adjacency-keyspace and adjacency-table DDL through the
    /// cluster's replication path. Defaults to a local-only coordinator
    /// (`LocalGraphSchemaCoordinator`) when `GraphEngine::new` is used.
    /// Use `GraphEngine::new_with_coordinator` to inject a cluster-aware
    /// implementation so all replicas register the adjacency table.
    schema_coordinator: Arc<dyn GraphSchemaCoordinator>,
    /// Cached at construction so the lazy adjacency-registration path
    /// (`ensure_adjacency_storage_for_keyspace`) can spawn the
    /// reconciliation loop with the configured cadence on first use of
    /// each keyspace.
    reconciliation_interval: std::time::Duration,
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
        // Adjacency-keyspace registration is done lazily on the first
        // graph query that touches the keyspace (see
        // `ensure_adjacency_storage_for_keyspace`), so the constructor
        // doesn't need to walk edge tables here. The lazy path is the
        // only path that creates the system_graph_<ks>.adjacency table
        // — and it goes through the cluster DDL coordinator, which
        // means every replica's state machine applies the DDL and
        // registers the table in its local schema + StorageEngine.
        //
        // Repair of an existing adjacency keyspace's replication
        // settings (a no-op unless schema is out of date) also moves to
        // the lazy path; nothing here is so urgent that it can't wait
        // for the first query.
        Self {
            schema,
            storage,
            write_path,
            config,
            reconciliation_handles: Vec::new(),
            reconciliation_cancel: CancellationToken::new(),
            subscription_registry: Arc::new(SubscriptionRegistry::new(max_subscriptions())),
            registered_adjacency_keyspaces: Mutex::new(HashSet::new()),
            schema_coordinator,
            reconciliation_interval,
        }
    }

    /// Test-visible alias for the lazy adjacency-registration path.
    /// Production callers go through `execute_with_params`; the test
    /// suite exercises this directly to pin the coordinator-routing
    /// contract without spinning up a full query pipeline.
    #[doc(hidden)]
    pub async fn ensure_adjacency_storage_for_keyspace_for_test(
        &self,
        keyspace: &str,
    ) -> Result<bool> {
        self.ensure_adjacency_storage_for_keyspace(keyspace).await
    }

    async fn ensure_adjacency_storage_for_keyspace(&self, keyspace: &str) -> Result<bool> {
        let snap = self.schema.snapshot();
        let has_edge_table = snap.tables.iter().any(|((ks, _), meta)| {
            ks == keyspace && meta.extensions.get("graph.type") == Some(&"edge".to_string())
        });
        if !has_edge_table {
            return Ok(false);
        }

        // Fast path: already registered. We check the in-process set
        // before doing any DDL work; the DDL coordinator's operations
        // are idempotent but submitting them through Raft on every
        // query just to discover "no-op" would be wasteful.
        {
            let registered = self.registered_adjacency_keyspaces.lock().map_err(|_| {
                GraphError::Internal("graph adjacency keyspace registry lock poisoned".to_string())
            })?;
            if registered.contains(keyspace) {
                return Ok(false);
            }
        }

        let adj_ks = adjacency_keyspace_name(keyspace);
        let adj_meta = adjacency_keyspace_metadata(&snap, keyspace);
        if let Some(existing) = snap.keyspaces.get(&adj_ks) {
            if replication_needs_repair(&existing.replication) {
                if let Err(e) = self.schema.alter_keyspace_internal(
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
                }
            }
        } else {
            self.schema_coordinator
                .apply_create_keyspace(adj_meta)
                .await
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
                .await
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

            // Background reconciliation for this keyspace. Same gating
            // the old startup-time path used: a configured non-zero
            // interval + a tokio runtime available.
            if background_reconciliation_enabled(self.reconciliation_interval)
                && tokio::runtime::Handle::try_current().is_ok()
            {
                // Cancellation is via `self.reconciliation_cancel` (child
                // token), so the JoinHandle is intentionally dropped —
                // we don't await individual reconcile tasks at shutdown.
                drop(spawn_reconciliation(
                    Arc::clone(&self.schema),
                    Arc::new(WritePath::direct(Arc::clone(&self.storage))),
                    keyspace.to_string(),
                    self.reconciliation_interval,
                    self.reconciliation_cancel.child_token(),
                ));
            }
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
        let statement = bind_statement_params(parse(query)?, params)?;
        if statement_requires_adjacency(&statement) {
            let adjacency_registered = self.ensure_adjacency_storage_for_keyspace(keyspace).await?;
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
        }
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
        Statement::Remove {
            pattern,
            where_clause,
            items,
        } => Statement::Remove {
            pattern: bind_patterns_params(pattern, params)?,
            where_clause: where_clause
                .map(|expr| bind_expr_params(expr, params))
                .transpose()?,
            // REMOVE items carry only variable/property/label identifiers, no
            // parameterizable expressions, so they pass through unchanged.
            items,
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
            post_filters: _,
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
        PhysicalPlan::RemoveProperties {
            expand,
            items,
            variable_tables,
        } => {
            let mut out = String::new();
            out.push_str("RemoveProperties {\n");
            out.push_str(&format!("  expand: {}\n", format_plan(expand)));
            out.push_str(&format!("  items: {}\n", items.len()));
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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
                batch: Default::default(),
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
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

    fn superuser_auth() -> AuthContext {
        AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    struct CountingGraphSchemaCoordinator {
        keyspaces: AtomicUsize,
        tables: AtomicUsize,
    }

    impl CountingGraphSchemaCoordinator {
        fn new() -> Self {
            Self {
                keyspaces: AtomicUsize::new(0),
                tables: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.keyspaces.load(Ordering::SeqCst) + self.tables.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl GraphSchemaCoordinator for CountingGraphSchemaCoordinator {
        async fn apply_create_keyspace(
            &self,
            _ks: ferrosa_schema::metadata::keyspace::KeyspaceMetadata,
        ) -> Result<()> {
            self.keyspaces.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn apply_create_table(
            &self,
            _table: ferrosa_schema::metadata::table::TableMetadata,
        ) -> Result<()> {
            self.tables.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn register_minimal_edge_table(schema: &Schema, keyspace: &str) {
        use std::collections::{HashMap, HashSet};

        use ferrosa_schema::{
            ClusteringOrder, ColumnKind, ColumnMetadata, TableFlag, TableMetadata, TableParams,
        };
        use indexmap::IndexMap;

        schema
            .create_keyspace_internal(ferrosa_schema::KeyspaceMetadata {
                name: keyspace.to_string(),
                durable_writes: true,
                replication: ferrosa_schema::ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
                },
            })
            .unwrap();

        let mut columns = IndexMap::new();
        columns.insert(
            "src_id".to_string(),
            ColumnMetadata {
                name: "src_id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "dst_id".to_string(),
            ColumnMetadata {
                name: "dst_id".to_string(),
                kind: ColumnKind::Clustering,
                position: 0,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::Asc,
                mask: None,
            },
        );

        let mut extensions = HashMap::new();
        extensions.insert("graph.type".to_string(), "edge".to_string());
        extensions.insert("graph.label".to_string(), "KNOWS".to_string());
        extensions.insert("graph.source".to_string(), "src_id".to_string());
        extensions.insert("graph.target".to_string(), "dst_id".to_string());
        extensions.insert("graph.source_label".to_string(), "Person".to_string());
        extensions.insert("graph.target_label".to_string(), "Person".to_string());

        schema
            .create_table_internal(TableMetadata {
                keyspace: keyspace.to_string(),
                name: "knows_e".to_string(),
                id: uuid::Uuid::new_v4(),
                columns,
                partition_key: vec!["src_id".to_string()],
                clustering_key: vec![("dst_id".to_string(), ClusteringOrder::Asc)],
                params: TableParams::default(),
                flags: HashSet::from([TableFlag::Compound]),
                extensions,
                is_system: false,
            })
            .unwrap();
    }

    #[tokio::test]
    async fn scalar_return_does_not_touch_lazy_adjacency_registration() {
        let schema = Arc::new(test_schema());
        register_minimal_edge_table(&schema, "agent_memory");

        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(test_storage_engine(dir.path()));
        storage
            .register_table(
                schema
                    .snapshot()
                    .tables
                    .get(&("agent_memory".to_string(), "knows_e".to_string()))
                    .unwrap()
                    .to_storage_schema(),
            )
            .unwrap();

        let coordinator = Arc::new(CountingGraphSchemaCoordinator::new());
        let engine = GraphEngine::new_with_coordinator(
            Arc::clone(&schema),
            Arc::clone(&storage),
            Arc::new(arc_swap::ArcSwap::from_pointee(
                ferrosa_cluster::write_path::WritePath::direct(storage),
            )),
            GraphEngineConfig::default(),
            std::time::Duration::ZERO,
            coordinator.clone(),
        );

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            engine.execute("RETURN 1", "agent_memory", &superuser_auth()),
        )
        .await
        .expect("scalar RETURN should complete without adjacency DDL/reconcile")
        .expect("scalar RETURN should succeed");

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            coordinator.calls(),
            0,
            "scalar RETURN must not force lazy adjacency registration for unrelated edge tables"
        );
    }

    #[test]
    fn relationship_patterns_still_require_lazy_adjacency_registration() {
        let relationship_query = parse("MATCH (:Person)-[:KNOWS]->(:Person) RETURN 1").unwrap();
        assert!(
            statement_requires_adjacency(&relationship_query),
            "relationship traversals must still materialize adjacency storage"
        );

        let scalar_query = parse("RETURN 1").unwrap();
        assert!(
            !statement_requires_adjacency(&scalar_query),
            "scalar queries must stay independent from adjacency storage"
        );
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
            post_filters: vec![],
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

    #[tokio::test]
    async fn engine_lazy_path_re_registers_adjacency_table_in_schema() {
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

        let engine = GraphEngine::new(
            Arc::clone(&schema),
            storage.clone(),
            Arc::new(arc_swap::ArcSwap::from_pointee(
                ferrosa_cluster::write_path::WritePath::direct(storage),
            )),
            GraphEngineConfig::default(),
            std::time::Duration::from_secs(60),
        );

        // Adjacency registration is lazy now — the constructor doesn't
        // create it. The first query (or the test hook below, which
        // mirrors the lazy path) is what materialises the system
        // adjacency keyspace + table on every replica via the
        // GraphSchemaCoordinator.
        engine
            .ensure_adjacency_storage_for_keyspace_for_test("agent_memory")
            .await
            .expect("lazy adjacency registration");

        let snap = schema.snapshot();
        assert!(
            snap.keyspaces.contains_key("system_graph_agent_memory"),
            "graph engine lazy path must register the adjacency keyspace in the live schema"
        );
        assert!(
            snap.tables.contains_key(&(
                "system_graph_agent_memory".to_string(),
                "adjacency".to_string()
            )),
            "graph engine lazy path must register the adjacency table in the live schema"
        );
    }
}
