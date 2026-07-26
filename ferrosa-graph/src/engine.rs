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
use crate::executor::aggregate::{create_accumulator, is_aggregate_function};
use crate::executor::eval::eval_expr;
use crate::executor::expand::{
    build_columns, execute, execute_streaming_owned, sort_rows, GraphEngineConfig, OwnedExecCtx,
};
use crate::executor::result::{GraphResult, QueryStats};
use crate::executor::stream::{collect_to_graph_result, stream_from_rows, RowStream};
use crate::executor::subscribe::SubscriptionRegistry;
use crate::parser::{
    parse, Assignment, Expr, Literal, Pattern, ReturnClause, ReturnItem, Statement, WithPipeline,
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

/// Present an already-materialized [`GraphResult`] in the streaming shape.
///
/// Every use marks a path that genuinely buffers — FOREACH and `CALL {}` (both
/// orchestrate many sub-executions), and the missing-label short circuit. Naming
/// it keeps those honest instead of hiding them behind an inline
/// `stream_from_rows`.
fn buffered_as_stream(result: GraphResult) -> (Vec<String>, RowStream<'static>, QueryStats) {
    (result.columns, stream_from_rows(result.rows), result.stats)
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
        Expr::ListComprehension {
            list,
            filter,
            projection,
            ..
        } => {
            expr_requires_adjacency(list)
                || filter.as_deref().is_some_and(expr_requires_adjacency)
                || projection.as_deref().is_some_and(expr_requires_adjacency)
        }
        // A pattern comprehension always traverses edges.
        Expr::PatternComprehension { .. } => true,
        Expr::Map(props) => prop_map_requires_adjacency(props),
        Expr::MapProjection { selectors, .. } => selectors.iter().any(|s| match s {
            crate::parser::MapProjectionSelector::Literal { value, .. } => {
                expr_requires_adjacency(value)
            }
            crate::parser::MapProjectionSelector::Property(_)
            | crate::parser::MapProjectionSelector::All => false,
        }),
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
        Statement::Foreach { body, .. } => body.iter().any(statement_requires_adjacency),
        Statement::CallSubquery { outer, inner, .. } => {
            statement_requires_adjacency(outer) || statement_requires_adjacency(inner)
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

/// Spill backend backing the graph executor's unbounded ORDER BY: reserves a
/// cancellable temp-sort directory from the storage engine, exactly as the CQL
/// ORDER BY path does. The returned reservation's `Drop` removes the directory,
/// so an aborted or cancelled query cleans up like a successful one.
struct StorageEngineSpill {
    storage: Arc<StorageEngine>,
}

impl std::fmt::Debug for StorageEngineSpill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StorageEngineSpill")
    }
}

impl crate::executor::spill::SpillReserver for StorageEngineSpill {
    fn reserve(
        &self,
        label: &str,
    ) -> ferrosa_common::Result<ferrosa_storage::TempSortTableReservation> {
        self.storage
            .reserve_order_by_temp_sort_table("graph", label)
    }
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
        mut config: GraphEngineConfig,
        reconciliation_interval: std::time::Duration,
        schema_coordinator: Arc<dyn GraphSchemaCoordinator>,
    ) -> Self {
        // Give the executor a spill backend so an unbounded ORDER BY sorts
        // through the storage engine's bounded external merge sort instead of
        // being capped in memory (t_4ce82a3e). Only set when the caller left it
        // unset, so an explicit override (tests) still wins.
        if config.spill.is_none() {
            config.spill = Some(Arc::new(StorageEngineSpill {
                storage: Arc::clone(&storage),
            }));
        }
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

    /// Buffered façade over [`GraphEngine::execute_stream_with_params`]: drains
    /// the row stream into a `GraphResult`. There is one query pipeline, not
    /// two — this entry point adds only the materialization.
    pub async fn execute_with_params(
        &self,
        query: &str,
        keyspace: &str,
        auth: &AuthContext,
        params: &HashMap<String, Value>,
    ) -> Result<GraphResult> {
        let (columns, rows, stats) = self
            .execute_stream_with_params(query, keyspace, auth, params)
            .await?;
        // `usize::MAX`, deliberately NOT `config.max_result_rows` — matching
        // `executor::expand::execute`, which has never applied a global cap.
        collect_to_graph_result(columns, rows, stats, usize::MAX).await
    }

    /// Execute a query and return its columns, a `'static` row stream, and the
    /// stats known at that point.
    ///
    /// The streaming entry point for transports: the row stream borrows nothing
    /// from this call, so it can be handed to an HTTP response body that
    /// outlives the handler frame (see
    /// [`crate::executor::expand::OwnedExecCtx`]).
    ///
    /// # What actually streams
    ///
    /// Whatever [`crate::executor::expand::execute_streaming`] streams — today
    /// `Subscribe`, `UNION ALL`, `RETURN`-only, the `Expand` projection without
    /// `ORDER BY`, `SET`/`REMOVE`, and `DISTINCT`. Everything else computes a
    /// buffered result and hands it back as an already-materialized stream. This
    /// is a transport change, not an operator conversion.
    ///
    /// `FOREACH` and `CALL {}` are orchestrated across many sub-executions and
    /// are materialized here, exactly as before.
    ///
    /// # Stats
    ///
    /// `execution_ms` is measured where the executor measures it today — at
    /// setup, i.e. *before* the rows are pulled. A caller that drains the stream
    /// and wants a total should add its own drain time; the HTTP transport does.
    pub async fn execute_stream_with_params(
        &self,
        query: &str,
        keyspace: &str,
        auth: &AuthContext,
        params: &HashMap<String, Value>,
    ) -> Result<(Vec<String>, RowStream<'static>, QueryStats)> {
        let statement = bind_statement_params(parse(query)?, params)?;
        if let Statement::Foreach { var, list, body } = statement {
            let result = self
                .execute_foreach(&var, &list, &body, keyspace, auth)
                .await?;
            return Ok(buffered_as_stream(result));
        }
        if let Statement::CallSubquery {
            outer,
            imports,
            inner,
            return_clause,
        } = statement
        {
            let result = self
                .execute_call_subquery(*outer, &imports, *inner, return_clause, keyspace, auth)
                .await?;
            return Ok(buffered_as_stream(result));
        }
        self.execute_statement_streaming(statement, keyspace, auth)
            .await
    }

    /// Validate, plan, and execute a single non-orchestrated statement (i.e. not
    /// FOREACH / CALL {}, which the engine expands first). Shared by the top-level
    /// query path and the CALL {} subquery orchestrator.
    ///
    /// Buffered façade over [`GraphEngine::execute_statement_streaming`].
    async fn execute_statement(
        &self,
        statement: Statement,
        keyspace: &str,
        auth: &AuthContext,
    ) -> Result<GraphResult> {
        let (columns, rows, stats) = self
            .execute_statement_streaming(statement, keyspace, auth)
            .await?;
        collect_to_graph_result(columns, rows, stats, usize::MAX).await
    }

    /// The streaming half of [`GraphEngine::execute_statement`].
    async fn execute_statement_streaming(
        &self,
        statement: Statement,
        keyspace: &str,
        auth: &AuthContext,
    ) -> Result<(Vec<String>, RowStream<'static>, QueryStats)> {
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
                return Ok(buffered_as_stream(empty_match_result(&statement)));
            }
            Err(e) => return Err(e),
        };
        let physical = plan(logical)?;
        execute_streaming_owned(physical, self.owned_exec_ctx(keyspace)).await
    }

    /// Collect the engine's execution handles as owned values.
    ///
    /// `load_full()` rather than `load()`, and `virtual_tables_arc()` rather
    /// than `virtual_tables()`: both of the borrowed forms produce a guard or a
    /// reference tied to *this* frame, which is precisely what stops a row
    /// stream from outliving the handler that started it.
    fn owned_exec_ctx(&self, keyspace: &str) -> OwnedExecCtx {
        OwnedExecCtx {
            write_path: self.write_path.load_full(),
            keyspace: keyspace.to_string(),
            config: Arc::new(self.config.clone()),
            virtual_tables: Some(self.schema.virtual_tables_arc()),
            schema: Some(Arc::clone(&self.schema)),
        }
    }

    /// Execute `FOREACH (var IN list | body...)`: run each body update clause
    /// once per list element.
    ///
    /// The whole FOREACH tree — including **nested FOREACH bodies** — is recursively
    /// materialized, validated, and planned into one ordered list of physical plans
    /// **before any is executed** (`plan_foreach`). The plans preserve openCypher
    /// per-element source ordering: for each outer element, that element's body
    /// clauses (and the fully-expanded plans of any nested FOREACH) appear in source
    /// order before the next element's. Execution then runs the list in order.
    ///
    /// Rollback scope (deliberate, fail-loud): WritePath has no cross-statement
    /// transaction, so the guaranteed-atomic class is the **validate/plan failure
    /// class** — if any (element × clause), at any nesting depth, fails to validate
    /// or plan (unknown label, type error, malformed clause), the call returns the
    /// error with ZERO writes performed. An execution-time failure (a write error
    /// after earlier plans committed) is surfaced loudly but cannot be rolled back
    /// here; that would require a storage-level batch and is not claimed.
    async fn execute_foreach(
        &self,
        var: &str,
        list: &Expr,
        body: &[Statement],
        keyspace: &str,
        auth: &AuthContext,
    ) -> Result<GraphResult> {
        // Phase 1 — recursively plan the entire FOREACH tree. Any validation/plan
        // failure (at any depth) aborts here, before a single write.
        let planned = self.plan_foreach(var, list, body, keyspace, auth).await?;
        let element_count = planned.element_count;

        // Phase 2 — execute the pre-planned clauses in source order.
        let mut stats = QueryStats::default();
        for physical in planned.plans {
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
            stats.vertices_written += result.stats.vertices_written;
            stats.vertices_deleted += result.stats.vertices_deleted;
        }

        Ok(GraphResult {
            columns: vec!["status".to_string()],
            rows: vec![vec![Value::String(format!(
                "FOREACH applied to {element_count} element(s)"
            ))]],
            stats,
        })
    }

    /// Recursively materialize + validate + plan a FOREACH into an ordered list of
    /// physical plans, preserving per-element source ordering. Nested FOREACHs are
    /// expanded in place (their plans interleave at the point the nested clause
    /// appears, once per outer element). No execution happens here, so any failure
    /// leaves zero writes.
    async fn plan_foreach(
        &self,
        var: &str,
        list: &Expr,
        body: &[Statement],
        keyspace: &str,
        auth: &AuthContext,
    ) -> Result<PlannedForeach> {
        let list_value = eval_expr(list, &HashMap::new())
            .map_err(|e| GraphError::Validation(format!("FOREACH list expression: {e}")))?;
        let elements = match list_value {
            Value::Array(items) => items,
            Value::Null => Vec::new(),
            other => {
                return Err(GraphError::Validation(format!(
                    "FOREACH expects a list, got {other}"
                )));
            }
        };

        let mut plans: Vec<PhysicalPlan> = Vec::new();
        for element in &elements {
            let replacement = value_to_expr(element)?;
            for stmt in body {
                let bound = subst_var_statement(stmt.clone(), var, &replacement);
                match bound {
                    Statement::Foreach {
                        var: inner_var,
                        list: inner_list,
                        body: inner_body,
                    } => {
                        // Expand the nested loop in place, preserving source order.
                        let nested = Box::pin(self.plan_foreach(
                            &inner_var,
                            &inner_list,
                            &inner_body,
                            keyspace,
                            auth,
                        ))
                        .await?;
                        plans.extend(nested.plans);
                    }
                    leaf => {
                        if statement_requires_adjacency(&leaf) {
                            self.ensure_adjacency_storage_for_keyspace(keyspace).await?;
                        }
                        let snap = self.schema.snapshot();
                        let logical = validate(&snap, auth, keyspace, leaf)?;
                        plans.push(plan(logical)?);
                    }
                }
            }
        }

        Ok(PlannedForeach {
            plans,
            element_count: elements.len(),
        })
    }

    /// Execute a correlated `CALL {}` subquery: run the `inner` subquery once per
    /// `outer` row, with each `imports` variable bound to that row, and unite the
    /// results.
    ///
    /// Semantics (openCypher CALL {} subquery):
    /// - **Returning** subquery: the inner statement projects rows. Each inner row
    ///   is paired with its driving outer row. The optional trailing `return_clause`
    ///   projects over the combined `outer ∪ inner` bindings; without one, the inner
    ///   rows are returned directly. Result rows are the UNION over all outer rows.
    /// - **Unit** subquery: the inner statement performs only updates (no RETURN).
    ///   It runs for its write side effects once per outer row and does not change
    ///   the outer cardinality; the outer rows pass through unchanged (or are
    ///   projected by the trailing RETURN if present).
    ///
    /// Correlation is by value substitution (the same machinery as FOREACH): each
    /// outer row's imported values are materialized into the inner statement before
    /// it is validated/planned/executed. Reads of imported node properties
    /// (`p.name`) resolve against the materialized node map.
    ///
    /// Fail-loud: a nested `CALL {}` inside the body is rejected at parse time;
    /// any inner validation/plan/exec error surfaces with the outer row's context.
    async fn execute_call_subquery(
        &self,
        outer: Statement,
        imports: &[String],
        inner: Statement,
        return_clause: Option<ReturnClause>,
        keyspace: &str,
        auth: &AuthContext,
    ) -> Result<GraphResult> {
        // 1. Run the outer query, projecting each imported variable as a column so
        //    we can bind it per row. The outer is the MATCH built by the parser with
        //    an empty RETURN; we replace it with `RETURN <imports...>`.
        let outer = project_imports(outer, imports)?;
        let outer_result = self.execute_statement(outer, keyspace, auth).await?;
        let import_cols = outer_result.columns.clone();

        let inner_returns = statement_is_returning(&inner);
        let mut out_columns: Option<Vec<String>> = None;
        let mut out_rows: Vec<Vec<Value>> = Vec::new();
        // Combined `outer ∪ inner` binding maps for the trailing RETURN, accumulated
        // across ALL outer rows. The trailing clause is a single projection over the
        // whole united stream, so DISTINCT / ORDER BY / LIMIT / aggregation must be
        // applied once at the end — never per outer row.
        let mut trailing_bindings: Vec<HashMap<String, Value>> = Vec::new();
        let mut stats = QueryStats::default();

        // 2. For each outer row, materialize imports into the inner subquery and run.
        for outer_row in &outer_result.rows {
            let mut import_bindings: HashMap<String, Value> = HashMap::new();
            for (col, value) in import_cols.iter().zip(outer_row.iter()) {
                import_bindings.insert(col.clone(), value.clone());
            }

            // Substitute every imported variable into the inner statement, then
            // constant-fold the now-closed expressions (e.g. `p.age + 1000` becomes
            // a literal once `p` is materialized) so update clauses that only accept
            // literal property values still see one.
            let mut bound_inner = inner.clone();
            for name in imports {
                if let Some(value) = import_bindings.get(name) {
                    let replacement = value_to_expr(value)?;
                    bound_inner = subst_var_statement(bound_inner, name, &replacement);
                }
            }
            let bound_inner = fold_constants_statement(bound_inner)?;

            let inner_result = self.execute_statement(bound_inner, keyspace, auth).await?;
            stats.vertices_written += inner_result.stats.vertices_written;
            stats.vertices_deleted += inner_result.stats.vertices_deleted;
            stats.vertices_read += inner_result.stats.vertices_read;
            stats.edges_read += inner_result.stats.edges_read;
            stats.edges_deleted += inner_result.stats.edges_deleted;

            // 3. Combine per the subquery shape.
            if return_clause.is_some() {
                // Trailing RETURN: accumulate the combined `outer ∪ inner` bindings.
                // A unit inner contributes no inner columns, so the trailing RETURN
                // sees only the outer (import) bindings and yields one binding per
                // outer row. A returning inner yields one combined binding per inner
                // row. Projection is deferred to the finalizer.
                let inner_rows = if inner_returns {
                    rows_as_bindings(&inner_result)
                } else {
                    vec![HashMap::new()]
                };
                for inner_binding in inner_rows {
                    let mut combined = import_bindings.clone();
                    combined.extend(inner_binding);
                    trailing_bindings.push(combined);
                }
            } else if inner_returns {
                // No trailing RETURN: unite the inner rows directly.
                set_or_check_columns(&mut out_columns, &inner_result.columns)?;
                out_rows.extend(inner_result.rows);
            }
            // Unit subquery with no trailing RETURN: nothing to project; the writes
            // already happened. (Outer cardinality is preserved but yields no
            // columns, matching a write-only statement's empty result shape.)
        }

        // 4. If there is a trailing RETURN, run the full projection pipeline over the
        //    united bindings: grouped aggregation (if any aggregate item), then
        //    DISTINCT, ORDER BY, and LIMIT — matching openCypher RETURN semantics so
        //    these operators are never silently dropped (URS-QEC-X01).
        if let Some(rc) = &return_clause {
            let (cols, rows) = project_trailing_return(rc, &trailing_bindings, &self.config)?;
            out_columns = Some(cols);
            out_rows = rows;
        }

        Ok(GraphResult {
            columns: out_columns.unwrap_or_default(),
            rows: out_rows,
            stats,
        })
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
        Statement::Foreach { var, list, body } => Statement::Foreach {
            var,
            list: bind_expr_params(list, params)?,
            body: body
                .into_iter()
                .map(|stmt| bind_statement_params(stmt, params))
                .collect::<Result<Vec<_>>>()?,
        },
        Statement::CallSubquery {
            outer,
            imports,
            inner,
            return_clause,
        } => Statement::CallSubquery {
            outer: Box::new(bind_statement_params(*outer, params)?),
            imports,
            inner: Box::new(bind_statement_params(*inner, params)?),
            return_clause: return_clause
                .map(|clause| bind_return_clause_params(clause, params))
                .transpose()?,
        },
    })
}

/// Rewrite the `outer` driving statement of a CALL {} subquery so it projects each
/// imported variable as a column (`RETURN p, q, ...`). The parser builds the outer
/// as a `MATCH` with an empty RETURN; here we fill in the projection so the engine
/// can read one binding per imported variable per row.
///
/// Only a plain `MATCH` outer is supported as the CALL {} driver. Anything else
/// fails loud rather than silently dropping the correlation.
fn project_imports(outer: Statement, imports: &[String]) -> Result<Statement> {
    match outer {
        Statement::Match {
            pattern,
            where_clause,
            ..
        } => Ok(Statement::Match {
            pattern,
            where_clause,
            return_clause: ReturnClause {
                distinct: false,
                items: imports
                    .iter()
                    .map(|name| ReturnItem {
                        expr: Expr::Var(name.clone()),
                        alias: Some(name.clone()),
                    })
                    .collect(),
                order_by: vec![],
                limit: None,
            },
        }),
        other => Err(GraphError::Validation(format!(
            "CALL {{}} subquery driver must be a MATCH, got {other:?}"
        ))),
    }
}

/// Whether a statement projects rows (has a RETURN). Returning statements feed the
/// CALL {} union; non-returning (unit) statements run only for write side effects.
fn statement_is_returning(stmt: &Statement) -> bool {
    match stmt {
        Statement::Match { .. }
        | Statement::MatchWith { .. }
        | Statement::MatchWithOptional { .. }
        | Statement::Unwind { .. }
        | Statement::Return { .. }
        | Statement::Union { .. } => true,
        Statement::Create { return_clause, .. } => return_clause.is_some(),
        Statement::Merge { return_clause, .. } => return_clause.is_some(),
        Statement::CallSubquery { return_clause, .. } => return_clause.is_some(),
        Statement::Set { .. }
        | Statement::Remove { .. }
        | Statement::Delete { .. }
        | Statement::Foreach { .. }
        | Statement::Subscribe { .. }
        | Statement::Unsubscribe { .. } => false,
    }
}

/// Whether an expression contains an aggregate function call anywhere in its tree.
/// Used to decide whether the trailing CALL {} RETURN groups+aggregates over the
/// united rows or projects them one-to-one.
fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args } => {
            is_aggregate_function(name) || args.iter().any(expr_contains_aggregate)
        }
        Expr::Distinct(inner) | Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            expr_contains_aggregate(inner)
        }
        Expr::Comparison { left, right, .. }
        | Expr::Arithmetic { left, right, .. }
        | Expr::And(left, right)
        | Expr::Or(left, right) => expr_contains_aggregate(left) || expr_contains_aggregate(right),
        Expr::In { value, list } => expr_contains_aggregate(value) || expr_contains_aggregate(list),
        Expr::List(items) => items.iter().any(expr_contains_aggregate),
        Expr::Index { target, index } => {
            expr_contains_aggregate(target) || expr_contains_aggregate(index)
        }
        Expr::Slice { target, start, end } => {
            expr_contains_aggregate(target)
                || start.as_deref().is_some_and(expr_contains_aggregate)
                || end.as_deref().is_some_and(expr_contains_aggregate)
        }
        _ => false,
    }
}

/// Project a trailing CALL {} RETURN clause over the fully-united `outer ∪ inner`
/// bindings, applying the complete openCypher RETURN pipeline:
///
/// 1. If any RETURN item contains an aggregate (`count`/`sum`/`avg`/`min`/`max`/
///    `collect`), group by the *non-aggregate* items and aggregate per group.
///    Otherwise project one row per binding.
/// 2. DISTINCT (dedup whole rows).
/// 3. ORDER BY (via the shared `sort_rows`).
/// 4. LIMIT (truncate).
///
/// None of these are silently dropped — a trailing DISTINCT/ORDER BY/LIMIT/aggregate
/// is honored or fails loud, satisfying URS-QEC-X01.
fn project_trailing_return(
    rc: &ReturnClause,
    bindings: &[HashMap<String, Value>],
    config: &GraphEngineConfig,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let columns = build_columns(rc);
    let has_aggregate = rc
        .items
        .iter()
        .any(|item| expr_contains_aggregate(&item.expr));

    let mut rows = if has_aggregate {
        project_trailing_aggregate(rc, bindings, config)?
    } else {
        let mut out = Vec::with_capacity(bindings.len());
        for binding in bindings {
            let mut row = Vec::with_capacity(rc.items.len());
            for item in &rc.items {
                row.push(eval_expr(&item.expr, binding)?);
            }
            out.push(row);
        }
        out
    };

    // DISTINCT: dedup whole projected rows, preserving first-seen order.
    if rc.distinct {
        let mut seen: HashSet<String> = HashSet::new();
        rows.retain(|row| seen.insert(serde_json::to_string(row).unwrap_or_default()));
    }

    // ORDER BY then LIMIT, reusing the executor's shared sort.
    sort_rows(&mut rows, &columns, &rc.order_by);
    if let Some(limit) = rc.limit {
        rows.truncate(limit.max(0) as usize);
    }

    Ok((columns, rows))
}

/// Grouped aggregation for a trailing CALL {} RETURN containing aggregate items.
///
/// openCypher grouping: the non-aggregate RETURN items form the grouping key; the
/// aggregate items accumulate per group. With no non-aggregate items there is a
/// single (implicit) group, so `count(*)` over an empty stream still yields one row
/// with `0`. Each group emits one row in first-seen key order.
fn project_trailing_aggregate(
    rc: &ReturnClause,
    bindings: &[HashMap<String, Value>],
    config: &GraphEngineConfig,
) -> Result<Vec<Vec<Value>>> {
    // Classify each RETURN item as a grouping key or an aggregate.
    let item_is_aggregate: Vec<bool> = rc
        .items
        .iter()
        .map(|item| expr_contains_aggregate(&item.expr))
        .collect();
    let has_group_keys = item_is_aggregate.iter().any(|agg| !agg);

    // Ordered group keys (serialized key -> insertion index) + per-group binding lists.
    let mut group_order: Vec<String> = Vec::new();
    let mut group_rows: HashMap<String, Vec<&HashMap<String, Value>>> = HashMap::new();
    let mut group_key_values: HashMap<String, Vec<Value>> = HashMap::new();

    for binding in bindings {
        // The grouping key is the tuple of evaluated non-aggregate items.
        let mut key_vals: Vec<Value> = Vec::new();
        for (item, is_agg) in rc.items.iter().zip(&item_is_aggregate) {
            if !is_agg {
                key_vals.push(eval_expr(&item.expr, binding)?);
            }
        }
        let key_str = serde_json::to_string(&key_vals).unwrap_or_default();
        if !group_rows.contains_key(&key_str) {
            if group_order.len() >= config.max_groups {
                return Err(GraphError::ResourceLimit(format!(
                    "CALL {{}} trailing aggregation group count limit exceeded: {} (limit: {})",
                    group_order.len() + 1,
                    config.max_groups
                )));
            }
            group_order.push(key_str.clone());
            group_key_values.insert(key_str.clone(), key_vals);
        }
        group_rows.entry(key_str).or_default().push(binding);
    }

    // No rows and no grouping keys: still emit a single implicit group so bare
    // aggregates (e.g. count(*)) return their zero value rather than no rows.
    if group_order.is_empty() && !has_group_keys {
        group_order.push(String::new());
        group_key_values.insert(String::new(), Vec::new());
        group_rows.insert(String::new(), Vec::new());
    }

    let mut out_rows = Vec::with_capacity(group_order.len());
    for key_str in &group_order {
        let rows_in_group = group_rows.get(key_str).cloned().unwrap_or_default();
        let key_vals = group_key_values.get(key_str).cloned().unwrap_or_default();
        let mut key_iter = key_vals.into_iter();

        let mut out_row = Vec::with_capacity(rc.items.len());
        for (item, is_agg) in rc.items.iter().zip(&item_is_aggregate) {
            if *is_agg {
                out_row.push(eval_trailing_aggregate(&item.expr, &rows_in_group, config)?);
            } else {
                out_row.push(key_iter.next().unwrap_or(Value::Null));
            }
        }
        out_rows.push(out_row);
    }
    Ok(out_rows)
}

/// Evaluate a single aggregate RETURN expression over one group's bindings.
///
/// Supports a bare aggregate call (`count(*)`, `sum(x)`, `collect(x)`), including a
/// `DISTINCT` argument (`count(DISTINCT n.age)`). A non-aggregate expression nested
/// around an aggregate (e.g. `count(*) + 1`) is rejected fail-loud rather than
/// silently mis-evaluated.
fn eval_trailing_aggregate(
    expr: &Expr,
    rows: &[&HashMap<String, Value>],
    config: &GraphEngineConfig,
) -> Result<Value> {
    let Expr::Function { name, args } = expr else {
        return Err(GraphError::Validation(format!(
            "unsupported aggregate expression in CALL {{}} trailing RETURN: {expr:?} \
             (only a bare aggregate call such as count(*) is supported)"
        )));
    };
    if !is_aggregate_function(name) {
        return Err(GraphError::Validation(format!(
            "unsupported aggregate expression in CALL {{}} trailing RETURN: {expr:?} \
             (only a bare aggregate call such as count(*) is supported)"
        )));
    }

    // Unwrap a single DISTINCT argument; track it so duplicate arg values are
    // accumulated once.
    let (arg, distinct): (Option<&Expr>, bool) = match args.as_slice() {
        [] => (None, false),
        [Expr::Distinct(inner)] => (Some(inner.as_ref()), true),
        [single] => (Some(single), false),
        _ => {
            return Err(GraphError::Validation(format!(
                "aggregate {name}() in CALL {{}} trailing RETURN takes a single argument"
            )))
        }
    };
    let count_star =
        name.eq_ignore_ascii_case("count") && matches!(arg, Some(Expr::Var(v)) if v == "*");

    let mut acc = create_accumulator(name, count_star, config.max_collect_size)?;
    let mut seen: HashSet<String> = HashSet::new();
    for binding in rows {
        let value = match arg {
            // count(*) counts every row regardless of the argument value.
            _ if count_star => Value::from(1),
            Some(e) => eval_expr(e, binding)?,
            None => Value::Null,
        };
        if distinct {
            let key = serde_json::to_string(&value).unwrap_or_default();
            if !seen.insert(key) {
                continue;
            }
        }
        acc.accumulate(&value);
    }
    Ok(acc.finish())
}

/// Turn a `GraphResult`'s rows into per-row binding maps keyed by column name, so
/// trailing-RETURN expressions can read inner-subquery outputs by name.
fn rows_as_bindings(result: &GraphResult) -> Vec<HashMap<String, Value>> {
    result
        .rows
        .iter()
        .map(|row| {
            result
                .columns
                .iter()
                .cloned()
                .zip(row.iter().cloned())
                .collect()
        })
        .collect()
}

/// Set the output columns on first use, or verify subsequent unions agree. Mirrors
/// the UNION arm-shape check: differing column shapes across outer rows is a hard
/// error, not a silent last-write-wins.
fn set_or_check_columns(slot: &mut Option<Vec<String>>, cols: &[String]) -> Result<()> {
    match slot {
        Some(existing) if existing.as_slice() != cols => Err(GraphError::Validation(
            "CALL {} subquery produced inconsistent column shapes across rows".to_string(),
        )),
        Some(_) => Ok(()),
        None => {
            *slot = Some(cols.to_vec());
            Ok(())
        }
    }
}

/// Convert a runtime JSON value into the equivalent literal `Expr`.
///
/// Used to materialize a FOREACH loop element into the body's expressions:
/// scalars become `Literal`s, arrays become `Expr::List`, and objects become
/// `Expr::Map`, so nested structures (e.g. `FOREACH (m IN [{name:'a'}] | ...)`)
/// substitute correctly.
/// Fully-expanded plan for a FOREACH subtree: physical plans in per-element
/// source order, plus the outer element count for the status row.
struct PlannedForeach {
    plans: Vec<PhysicalPlan>,
    element_count: usize,
}

fn value_to_expr(value: &Value) -> Result<Expr> {
    Ok(match value {
        Value::Null => Expr::Literal(Literal::Null),
        Value::Bool(b) => Expr::Literal(Literal::Bool(*b)),
        Value::String(s) => Expr::Literal(Literal::String(s.clone())),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Expr::Literal(Literal::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Expr::Literal(Literal::Float(f))
            } else {
                return Err(GraphError::Validation(
                    "FOREACH element number cannot be represented".to_string(),
                ));
            }
        }
        Value::Array(items) => Expr::List(
            items
                .iter()
                .map(value_to_expr)
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Object(map) => Expr::Map(
            map.iter()
                .map(|(k, v)| Ok((k.clone(), value_to_expr(v)?)))
                .collect::<Result<Vec<_>>>()?,
        ),
    })
}

/// Substitute every reference to FOREACH loop variable `var` in `expr` with
/// `replacement` (the materialized current element). A nested FOREACH/comprehension
/// that rebinds the same name shadows the outer binding, so substitution stops at
/// the shadowing scope.
fn subst_var_expr(expr: Expr, var: &str, replacement: &Expr) -> Expr {
    let recur = |e: Expr| subst_var_expr(e, var, replacement);
    let boxed = |e: Box<Expr>| Box::new(subst_var_expr(*e, var, replacement));
    match expr {
        Expr::Var(name) if name == var => replacement.clone(),
        // Property access on the substituted variable: when `replacement` is the
        // materialized node/map (the common case for an imported CALL {} variable),
        // resolve `var.name` to the map's `name` entry. This lets a correlated
        // subquery read properties off an imported node (`WITH p ... p.name`).
        // A missing key resolves to NULL (openCypher property-of-missing-key).
        Expr::Property { var: pvar, name } if pvar == var => {
            if let Expr::Map(entries) = replacement {
                entries
                    .iter()
                    .find(|(k, _)| k == &name)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Expr::Literal(Literal::Null))
            } else {
                Expr::Property { var: pvar, name }
            }
        }
        Expr::Function { name, args } => Expr::Function {
            name,
            args: args.into_iter().map(recur).collect(),
        },
        Expr::Distinct(inner) => Expr::Distinct(boxed(inner)),
        Expr::Comparison { left, op, right } => Expr::Comparison {
            left: boxed(left),
            op,
            right: boxed(right),
        },
        Expr::In { value, list } => Expr::In {
            value: boxed(value),
            list: boxed(list),
        },
        Expr::Arithmetic { left, op, right } => Expr::Arithmetic {
            left: boxed(left),
            op,
            right: boxed(right),
        },
        Expr::And(l, r) => Expr::And(boxed(l), boxed(r)),
        Expr::Or(l, r) => Expr::Or(boxed(l), boxed(r)),
        Expr::Not(inner) => Expr::Not(boxed(inner)),
        Expr::List(items) => Expr::List(items.into_iter().map(recur).collect()),
        Expr::ListPredicate {
            kind,
            var: inner_var,
            list,
            predicate,
        } => {
            let list = boxed(list);
            // Inner var shadows the loop var inside the predicate.
            let predicate = if inner_var == var {
                predicate
            } else {
                boxed(predicate)
            };
            Expr::ListPredicate {
                kind,
                var: inner_var,
                list,
                predicate,
            }
        }
        Expr::ListComprehension {
            var: inner_var,
            list,
            filter,
            projection,
        } => {
            let list = boxed(list);
            let shadowed = inner_var == var;
            let map_inner = |b: Box<Expr>| {
                if shadowed {
                    b
                } else {
                    Box::new(subst_var_expr(*b, var, replacement))
                }
            };
            Expr::ListComprehension {
                var: inner_var,
                list,
                filter: filter.map(map_inner),
                projection: projection.map(map_inner),
            }
        }
        Expr::Map(props) => Expr::Map(
            props
                .into_iter()
                .map(|(k, v)| (k, subst_var_expr(v, var, replacement)))
                .collect(),
        ),
        Expr::Index { target, index } => Expr::Index {
            target: boxed(target),
            index: boxed(index),
        },
        Expr::Slice { target, start, end } => Expr::Slice {
            target: boxed(target),
            start: start.map(boxed),
            end: end.map(boxed),
        },
        Expr::IsNull(inner) => Expr::IsNull(boxed(inner)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(boxed(inner)),
        other => other,
    }
}

/// Substitute the loop variable in a property map (pattern props / map literals).
fn subst_var_prop_map(
    props: Vec<(String, Expr)>,
    var: &str,
    replacement: &Expr,
) -> Vec<(String, Expr)> {
    props
        .into_iter()
        .map(|(k, v)| (k, subst_var_expr(v, var, replacement)))
        .collect()
}

/// Substitute the loop variable inside a pattern's property values.
fn subst_var_pattern(pattern: Pattern, var: &str, replacement: &Expr) -> Pattern {
    match pattern {
        Pattern::Node {
            var: node_var,
            label,
            props,
        } => Pattern::Node {
            var: node_var,
            label,
            props: subst_var_prop_map(props, var, replacement),
        },
        Pattern::Rel {
            var: rel_var,
            rel_type,
            direction,
            props,
            length_range,
        } => Pattern::Rel {
            var: rel_var,
            rel_type,
            direction,
            props: subst_var_prop_map(props, var, replacement),
            length_range,
        },
        Pattern::Path(elements) => Pattern::Path(
            elements
                .into_iter()
                .map(|p| subst_var_pattern(p, var, replacement))
                .collect(),
        ),
    }
}

/// Substitute the FOREACH loop variable `var` throughout a body update statement,
/// materializing the current element as `replacement`. Only update clauses appear
/// in a FOREACH body; non-update statements are returned unchanged (they are
/// rejected at parse time, so this is purely defensive).
fn subst_var_statement(stmt: Statement, var: &str, replacement: &Expr) -> Statement {
    match stmt {
        Statement::Create {
            patterns,
            return_clause,
        } => Statement::Create {
            patterns: patterns
                .into_iter()
                .map(|p| subst_var_pattern(p, var, replacement))
                .collect(),
            return_clause,
        },
        Statement::Merge {
            patterns,
            set_clause,
            return_clause,
        } => Statement::Merge {
            patterns: patterns
                .into_iter()
                .map(|p| subst_var_pattern(p, var, replacement))
                .collect(),
            set_clause: set_clause
                .into_iter()
                .map(|a| Assignment {
                    var: a.var,
                    property: a.property,
                    value: subst_var_expr(a.value, var, replacement),
                })
                .collect(),
            return_clause,
        },
        Statement::Set {
            pattern,
            where_clause,
            assignments,
        } => Statement::Set {
            pattern: pattern
                .into_iter()
                .map(|p| subst_var_pattern(p, var, replacement))
                .collect(),
            where_clause: where_clause.map(|w| subst_var_expr(w, var, replacement)),
            assignments: assignments
                .into_iter()
                .map(|a| Assignment {
                    var: a.var,
                    property: a.property,
                    value: subst_var_expr(a.value, var, replacement),
                })
                .collect(),
        },
        // Nested FOREACH: the inner loop var shadows the outer one inside its body.
        Statement::Foreach {
            var: inner_var,
            list,
            body,
        } => {
            let list = subst_var_expr(list, var, replacement);
            let body = if inner_var == var {
                body
            } else {
                body.into_iter()
                    .map(|s| subst_var_statement(s, var, replacement))
                    .collect()
            };
            Statement::Foreach {
                var: inner_var,
                list,
                body,
            }
        }
        // Read statements appear as CALL {} subquery bodies (correlated reads). The
        // imported variable is materialized into their projections / patterns /
        // filters so the subquery sees the outer row.
        Statement::Return { return_clause } => Statement::Return {
            return_clause: subst_var_return_clause(return_clause, var, replacement),
        },
        Statement::Match {
            pattern,
            where_clause,
            return_clause,
        } => Statement::Match {
            pattern: pattern
                .into_iter()
                .map(|p| subst_var_pattern(p, var, replacement))
                .collect(),
            where_clause: where_clause.map(|w| subst_var_expr(w, var, replacement)),
            return_clause: subst_var_return_clause(return_clause, var, replacement),
        },
        Statement::MatchWith {
            pattern,
            where_clause,
            with_pipeline,
            return_clause,
        } => Statement::MatchWith {
            pattern: pattern
                .into_iter()
                .map(|p| subst_var_pattern(p, var, replacement))
                .collect(),
            where_clause: where_clause.map(|w| subst_var_expr(w, var, replacement)),
            with_pipeline: subst_var_with_pipeline(with_pipeline, var, replacement),
            return_clause: subst_var_return_clause(return_clause, var, replacement),
        },
        Statement::Unwind {
            expr,
            var: unwind_var,
            with_pipeline,
            return_clause,
        } => {
            // The UNWIND alias shadows the imported var inside the rest of the query.
            let expr = subst_var_expr(expr, var, replacement);
            if unwind_var == var {
                Statement::Unwind {
                    expr,
                    var: unwind_var,
                    with_pipeline,
                    return_clause,
                }
            } else {
                Statement::Unwind {
                    expr,
                    var: unwind_var,
                    with_pipeline: with_pipeline
                        .map(|wp| subst_var_with_pipeline(wp, var, replacement)),
                    return_clause: subst_var_return_clause(return_clause, var, replacement),
                }
            }
        }
        // Any other statement form as a subquery body is not substituted here; it is
        // either rejected upstream or correlation-free, so pass through unchanged.
        other => other,
    }
}

/// Substitute the imported variable throughout a RETURN clause (projection items
/// and ORDER BY expressions). LIMIT/DISTINCT carry no variable references.
fn subst_var_return_clause(rc: ReturnClause, var: &str, replacement: &Expr) -> ReturnClause {
    ReturnClause {
        distinct: rc.distinct,
        items: rc
            .items
            .into_iter()
            .map(|item| ReturnItem {
                expr: subst_var_expr(item.expr, var, replacement),
                alias: item.alias,
            })
            .collect(),
        order_by: rc
            .order_by
            .into_iter()
            .map(|o| crate::parser::OrderItem {
                expr: subst_var_expr(o.expr, var, replacement),
                direction: o.direction,
            })
            .collect(),
        limit: rc.limit,
    }
}

/// Substitute the imported variable inside a WITH pipeline (its projection and
/// trailing WHERE filter).
fn subst_var_with_pipeline(wp: WithPipeline, var: &str, replacement: &Expr) -> WithPipeline {
    WithPipeline {
        clause: subst_var_return_clause(wp.clause, var, replacement),
        where_clause: wp.where_clause.map(|w| subst_var_expr(w, var, replacement)),
    }
}

/// Whether an expression is *closed*: it contains no free variable, property, or
/// parameter reference, so it can be evaluated with empty bindings. Used to decide
/// whether a substituted expression can be safely constant-folded to a literal.
fn expr_is_closed(expr: &Expr) -> bool {
    match expr {
        Expr::Var(_) | Expr::Property { .. } | Expr::Parameter(_) => false,
        Expr::Literal(_) => true,
        Expr::Distinct(e) | Expr::Not(e) | Expr::IsNull(e) | Expr::IsNotNull(e) => {
            expr_is_closed(e)
        }
        Expr::Comparison { left, right, .. }
        | Expr::Arithmetic { left, right, .. }
        | Expr::And(left, right)
        | Expr::Or(left, right) => expr_is_closed(left) && expr_is_closed(right),
        Expr::In { value, list } => expr_is_closed(value) && expr_is_closed(list),
        Expr::Index { target, index } => expr_is_closed(target) && expr_is_closed(index),
        Expr::Function { args, .. } => args.iter().all(expr_is_closed),
        Expr::List(items) => items.iter().all(expr_is_closed),
        Expr::Map(entries) => entries.iter().all(|(_, v)| expr_is_closed(v)),
        // Conservatively treat any other form as non-closed: do not fold it.
        _ => false,
    }
}

/// Constant-fold a closed expression to a literal by evaluating it with empty
/// bindings. Non-closed (still-correlated/pattern-referencing) expressions are
/// returned unchanged so the executor evaluates them in context.
fn fold_expr(expr: Expr) -> Result<Expr> {
    if expr_is_closed(&expr) {
        let value = eval_expr(&expr, &HashMap::new())?;
        return value_to_expr(&value);
    }
    Ok(expr)
}

/// Fold the property-map of a pattern (node/rel props): any closed value becomes a
/// literal. Used after import substitution so update clauses that require literal
/// property values accept correlated-then-folded derived values.
fn fold_pattern(pattern: Pattern) -> Result<Pattern> {
    let fold_props = |props: Vec<(String, Expr)>| -> Result<Vec<(String, Expr)>> {
        props
            .into_iter()
            .map(|(k, v)| Ok((k, fold_expr(v)?)))
            .collect()
    };
    Ok(match pattern {
        Pattern::Node { var, label, props } => Pattern::Node {
            var,
            label,
            props: fold_props(props)?,
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
            props: fold_props(props)?,
            length_range,
        },
        Pattern::Path(elements) => Pattern::Path(
            elements
                .into_iter()
                .map(fold_pattern)
                .collect::<Result<Vec<_>>>()?,
        ),
    })
}

/// Constant-fold closed expressions in an import-substituted inner subquery
/// statement. Only update clauses (whose property/assignment values may need to be
/// literals) are folded; read clauses are left for the executor to evaluate.
fn fold_constants_statement(stmt: Statement) -> Result<Statement> {
    Ok(match stmt {
        Statement::Create {
            patterns,
            return_clause,
        } => Statement::Create {
            patterns: patterns
                .into_iter()
                .map(fold_pattern)
                .collect::<Result<Vec<_>>>()?,
            return_clause,
        },
        Statement::Merge {
            patterns,
            set_clause,
            return_clause,
        } => Statement::Merge {
            patterns: patterns
                .into_iter()
                .map(fold_pattern)
                .collect::<Result<Vec<_>>>()?,
            set_clause: set_clause
                .into_iter()
                .map(|a| {
                    Ok(Assignment {
                        var: a.var,
                        property: a.property,
                        value: fold_expr(a.value)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            return_clause,
        },
        Statement::Set {
            pattern,
            where_clause,
            assignments,
        } => Statement::Set {
            pattern,
            where_clause,
            assignments: assignments
                .into_iter()
                .map(|a| {
                    Ok(Assignment {
                        var: a.var,
                        property: a.property,
                        value: fold_expr(a.value)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        },
        // Read clauses and others: no folding needed (executor evaluates exprs).
        other => other,
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

    fn binding(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn rc_single(expr: Expr, alias: &str) -> ReturnClause {
        ReturnClause {
            distinct: false,
            items: vec![ReturnItem {
                expr,
                alias: Some(alias.to_string()),
            }],
            order_by: vec![],
            limit: None,
        }
    }

    #[test]
    fn expr_contains_aggregate_detects_nested_calls() {
        assert!(expr_contains_aggregate(&Expr::Function {
            name: "count".into(),
            args: vec![Expr::Var("*".into())],
        }));
        // Aggregate buried inside arithmetic is still detected.
        assert!(expr_contains_aggregate(&Expr::Arithmetic {
            left: Box::new(Expr::Function {
                name: "sum".into(),
                args: vec![Expr::Var("x".into())],
            }),
            op: crate::parser::ArithOp::Add,
            right: Box::new(Expr::Literal(Literal::Integer(1))),
        }));
        // No aggregate: a plain property projection.
        assert!(!expr_contains_aggregate(&Expr::Property {
            var: "p".into(),
            name: "age".into(),
        }));
    }

    #[test]
    fn trailing_return_count_star_groups_over_all_rows() {
        let rc = rc_single(
            Expr::Function {
                name: "count".into(),
                args: vec![Expr::Var("*".into())],
            },
            "n",
        );
        let rows = vec![
            binding(&[("a", Value::from(1))]),
            binding(&[("a", Value::from(2))]),
            binding(&[("a", Value::from(3))]),
        ];
        let (cols, out) =
            project_trailing_return(&rc, &rows, &GraphEngineConfig::default()).unwrap();
        assert_eq!(cols, vec!["n".to_string()]);
        assert_eq!(out, vec![vec![Value::from(3u64)]]);
    }

    #[test]
    fn trailing_return_count_star_empty_stream_is_zero_row() {
        let rc = rc_single(
            Expr::Function {
                name: "count".into(),
                args: vec![Expr::Var("*".into())],
            },
            "n",
        );
        let (_cols, out) =
            project_trailing_return(&rc, &[], &GraphEngineConfig::default()).unwrap();
        assert_eq!(
            out,
            vec![vec![Value::from(0u64)]],
            "bare count(*) over an empty united stream must emit one row with 0"
        );
    }

    #[test]
    fn trailing_return_distinct_dedups_whole_rows() {
        let rc = ReturnClause {
            distinct: true,
            items: vec![ReturnItem {
                expr: Expr::Var("one".into()),
                alias: Some("one".into()),
            }],
            order_by: vec![],
            limit: None,
        };
        let rows = vec![
            binding(&[("one", Value::from(1))]),
            binding(&[("one", Value::from(1))]),
            binding(&[("one", Value::from(1))]),
        ];
        let (_cols, out) =
            project_trailing_return(&rc, &rows, &GraphEngineConfig::default()).unwrap();
        assert_eq!(out, vec![vec![Value::from(1)]]);
    }

    #[test]
    fn trailing_return_limit_truncates() {
        let mut rc = rc_single(Expr::Var("a".into()), "a");
        rc.limit = Some(1);
        let rows = vec![
            binding(&[("a", Value::from(10))]),
            binding(&[("a", Value::from(20))]),
            binding(&[("a", Value::from(30))]),
        ];
        let (_cols, out) =
            project_trailing_return(&rc, &rows, &GraphEngineConfig::default()).unwrap();
        assert_eq!(out.len(), 1, "LIMIT 1 must truncate to one row");
    }

    #[test]
    fn trailing_return_non_bare_aggregate_fails_loud() {
        // `count(*) + 1` wraps an aggregate in arithmetic. We do not silently
        // mis-evaluate it — we fail loud (URS-QEC-X01).
        let rc = rc_single(
            Expr::Arithmetic {
                left: Box::new(Expr::Function {
                    name: "count".into(),
                    args: vec![Expr::Var("*".into())],
                }),
                op: crate::parser::ArithOp::Add,
                right: Box::new(Expr::Literal(Literal::Integer(1))),
            },
            "n",
        );
        let rows = vec![binding(&[("a", Value::from(1))])];
        let err = project_trailing_return(&rc, &rows, &GraphEngineConfig::default())
            .expect_err("non-bare aggregate must be rejected, not silently wrong");
        assert!(
            matches!(err, GraphError::Validation(_)),
            "expected a loud validation error, got {err:?}"
        );
    }
}
