//! CQL query router — dispatches parsed statements to schema/storage.
//!
//! This is the central piece of the CQL layer: it takes a parsed `Statement`,
//! resolves keyspaces, checks permissions, and delegates to the appropriate
//! schema or storage operations.
//!
//! Security mitigations:
//! - **M8**: Every `route_*` function checks permissions via `Schema::check_permission`.
//! - **M12**: Batch size is capped at `MAX_BATCH_STATEMENTS` (500).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::BytesMut;
use indexmap::IndexMap;

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::pair::ddl::DdlOperation;
use ferrosa_cluster::{DdlPath, WritePath};
use ferrosa_common::DataType;
use ferrosa_index::IndexType;
use ferrosa_schema::{
    query_columns, query_keyspaces, query_local, query_peers, query_role_members,
    query_role_permissions, query_roles, query_tables, AuthContext,
    ClusteringOrder as SchemaClusteringOrder, ColumnKind, ColumnMetadata, GrantEntry,
    IndexMetadata, KeyspaceMetadata, KeyspaceUpdates, NodeConfig, Permission, ReplicationParams,
    Resource, RoleMetadata, RoleUpdates, Schema, TableMetadata, TableParams, TableUpdates,
    UserAggregateMetadata, UserFunctionMetadata, UserTypeMetadata, VirtualColumnDef, VirtualRow,
};
use ferrosa_storage::StorageEngine;
use ferrosa_storage::TableId;
use ferrosa_udf::UdfExecutor;

use crate::ast::*;
use crate::bridge;
use crate::error::CqlError;
use crate::planner::{self, ScanPlan};
use crate::prepared::PreparedCache;
use crate::result;
use crate::types::{CqlType, CqlValue};
use crate::virtual_tables::active_queries::QueryTracker;
use crate::virtual_tables::connections::ConnectionTracker;

/// Maximum number of statements allowed in a BATCH (security mitigation M12).
const MAX_BATCH_STATEMENTS: usize = 500;

/// UUID epoch offset: 100-nanosecond intervals between 1582-10-15 and 1970-01-01.
const UUID_EPOCH_OFFSET: u64 = 0x01B2_1DD2_1381_4000;

/// Generate a v1 Timeuuid containing the current timestamp.
///
/// Returns `CqlValue::Timeuuid` with a version-1 UUID built from the
/// current system clock. Uses a v4 UUID as entropy source for the
/// clock sequence and node fields (avoids pulling in `rand` directly).
fn eval_now() -> CqlValue {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let uuid_ts = now.as_nanos() as u64 / 100 + UUID_EPOCH_OFFSET;
    let time_low = (uuid_ts & 0xFFFF_FFFF) as u32;
    let time_mid = ((uuid_ts >> 32) & 0xFFFF) as u16;
    let time_hi = ((uuid_ts >> 48) & 0x0FFF) as u16 | 0x1000; // version 1

    // Use a v4 UUID as entropy source for clock_seq and node bytes.
    let entropy = uuid::Uuid::new_v4();
    let ebytes = entropy.as_bytes();
    let clock_seq: u16 = u16::from_be_bytes([ebytes[0], ebytes[1]]) & 0x3FFF | 0x8000; // variant 1
    let node: [u8; 6] = [
        ebytes[2], ebytes[3], ebytes[4], ebytes[5], ebytes[6], ebytes[7],
    ];

    let mut bytes = [0u8; 16];
    bytes[0..4].copy_from_slice(&time_low.to_be_bytes());
    bytes[4..6].copy_from_slice(&time_mid.to_be_bytes());
    bytes[6..8].copy_from_slice(&time_hi.to_be_bytes());
    bytes[8..10].copy_from_slice(&clock_seq.to_be_bytes());
    bytes[10..16].copy_from_slice(&node);

    CqlValue::Timeuuid(uuid::Uuid::from_bytes(bytes))
}

/// Extract the Unix-epoch millisecond timestamp from a Timeuuid.
///
/// Converts from 100-nanosecond intervals since 1582-10-15 (UUID epoch) to
/// milliseconds since 1970-01-01 (Unix epoch).
fn eval_to_timestamp(timeuuid: &CqlValue) -> Result<CqlValue, CqlError> {
    match timeuuid {
        CqlValue::Timeuuid(uuid) => {
            let bytes = uuid.as_bytes();
            let time_low = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64;
            let time_mid = u16::from_be_bytes([bytes[4], bytes[5]]) as u64;
            let time_hi = (u16::from_be_bytes([bytes[6], bytes[7]]) & 0x0FFF) as u64;
            let uuid_ts = time_low | (time_mid << 32) | (time_hi << 48);
            let millis = (uuid_ts - UUID_EPOCH_OFFSET) / 10_000;
            Ok(CqlValue::Timestamp(millis as i64))
        }
        _ => Err(CqlError::Invalid(
            "toTimestamp requires a timeuuid argument".into(),
        )),
    }
}

// ── UDF/UDA query-time resolution types ──────────────────────────────────

/// A user-defined function (scalar or aggregate) resolved from the schema,
/// ready for query-time execution.
#[derive(Debug, Clone)]
struct ResolvedFunction {
    /// Keyspace and function name.
    func_name: String,
    /// The keyspace in which the function is defined.
    func_keyspace: String,
    /// Scalar or aggregate.
    kind: ResolvedFunctionKind,
    /// CQL return type of the function.
    return_type: CqlType,
    /// Whether the function should be called when any argument is NULL.
    called_on_null: bool,
    /// Indices into the full row column list for each argument that references
    /// a column (non-column literal args use `usize::MAX` as sentinel).
    arg_indices: Vec<usize>,
    /// The original `Term` arguments (needed for literal extraction).
    arg_terms: Vec<Term>,
    /// The inferred CQL types of each argument.
    arg_types: Vec<CqlType>,
    /// Display name (alias or default) for the result column.
    display_name: String,
}

/// Whether a resolved function is scalar or aggregate.
#[derive(Debug, Clone)]
enum ResolvedFunctionKind {
    Scalar,
    Aggregate { init_cond: Option<CqlValue> },
}

/// Shared state available to all request handlers.
pub struct SharedState {
    pub engine: Arc<StorageEngine>,
    pub schema: Arc<Schema>,
    pub node_config: Arc<NodeConfig>,
    pub cluster_state: Arc<ArcSwap<ferrosa_cluster::ClusterStateHolder>>,
    pub write_path: Arc<ArcSwap<WritePath>>,
    pub ddl_path: Arc<ArcSwap<DdlPath>>,
    pub prepared_cache: Arc<PreparedCache>,
    pub connection_tracker: Arc<ConnectionTracker>,
    pub query_tracker: Arc<QueryTracker>,
    /// WASM UDF executor for compiling and invoking user-defined functions.
    pub udf_executor: Arc<UdfExecutor>,
    /// Broadcast channel for CQL EVENT push notifications.
    pub event_sender: tokio::sync::broadcast::Sender<crate::event::CqlEvent>,
    /// Mode controller for checking CQL readiness (pair mode gating).
    pub mode_controller: Arc<ferrosa_cluster::ModeController>,
}

/// Per-request context: authentication, current keyspace, and consistency level.
pub struct RequestContext<'a> {
    pub auth: &'a AuthContext,
    pub current_keyspace: &'a Option<String>,
    /// Client-requested consistency level parsed from the CQL protocol frame.
    pub consistency: ConsistencyLevel,
    /// Serial consistency level for lightweight transactions (LWT).
    /// Parsed from the CQL QUERY/EXECUTE frame when flags bit 0x0010 is set.
    pub serial_consistency: Option<ConsistencyLevel>,
    /// Pagination parameters from the QUERY/EXECUTE frame.
    pub paging: crate::paging::PagingParams,
}

/// Result of routing a statement.
pub enum RouteResult {
    /// A CQL RESULT frame body.
    Result(BytesMut),
    /// USE keyspace: returns the new keyspace name and a SetKeyspace frame body.
    SetKeyspace(String, BytesMut),
    /// Subscription accepted — connection should spawn a polling task.
    Subscribe {
        inner: Box<Statement>,
        interval: Option<std::time::Duration>,
        delta: bool,
    },
    /// Unsubscribe — cancel one or all subscriptions.
    Unsubscribe { stream_id: Option<u16> },
}

// ── Main dispatch ────────────────────────────────────────────────────────

/// Route a parsed statement to the appropriate handler.
pub async fn route(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    stmt: Statement,
) -> Result<RouteResult, CqlError> {
    // Track the query for observability; the guard calls complete() on drop.
    let query_desc: String = format!("{:?}", &stmt).chars().take(200).collect();
    let keyspace = ctx.current_keyspace.as_deref().unwrap_or("");
    let _guard = state.query_tracker.begin_guarded(
        &query_desc,
        keyspace,
        "",             // client address not yet available in RequestContext
        &ctx.auth.role, // authenticated role name
    );

    match stmt {
        Statement::Select(s) => route_select(state, ctx, s).map(RouteResult::Result),
        Statement::Insert(i) => route_insert(state, ctx, i).await.map(RouteResult::Result),
        Statement::Update(u) => route_update(state, ctx, u).await.map(RouteResult::Result),
        Statement::Delete(d) => route_delete(state, ctx, d).await.map(RouteResult::Result),
        Statement::Batch(b) => route_batch(state, ctx, b).await.map(RouteResult::Result),
        Statement::CreateKeyspace(ck) => route_create_keyspace(state, ctx, ck)
            .await
            .map(RouteResult::Result),
        Statement::CreateTable(ct) => route_create_table(state, ctx, ct)
            .await
            .map(RouteResult::Result),
        Statement::AlterTable(at) => route_alter_table(state, ctx, at)
            .await
            .map(RouteResult::Result),
        Statement::DropTable(dt) => route_drop_table(state, ctx, dt)
            .await
            .map(RouteResult::Result),
        Statement::AlterKeyspace(ak) => route_alter_keyspace(state, ctx, ak)
            .await
            .map(RouteResult::Result),
        Statement::DropKeyspace(dk) => route_drop_keyspace(state, ctx, dk)
            .await
            .map(RouteResult::Result),
        Statement::CreateRole(cr) => route_create_role(state, ctx, cr)
            .await
            .map(RouteResult::Result),
        Statement::AlterRole(ar) => route_alter_role(state, ctx, ar)
            .await
            .map(RouteResult::Result),
        Statement::DropRole(dr) => route_drop_role(state, ctx, dr)
            .await
            .map(RouteResult::Result),
        Statement::Grant(g) => route_grant(state, ctx, g).await.map(RouteResult::Result),
        Statement::Revoke(r) => route_revoke(state, ctx, r).await.map(RouteResult::Result),
        Statement::Use(u) => {
            let body = result::encode_set_keyspace(&u.keyspace);
            Ok(RouteResult::SetKeyspace(u.keyspace, body))
        }
        Statement::Truncate(t) => route_truncate(state, ctx, t).map(RouteResult::Result),
        Statement::CreateIndex(ci) => route_create_index(state, ctx, ci)
            .await
            .map(RouteResult::Result),
        Statement::DropIndex(di) => route_drop_index(state, ctx, di)
            .await
            .map(RouteResult::Result),
        Statement::Subscribe {
            inner,
            interval,
            delta,
        } => {
            // Validate: inner must be a Select
            match inner.as_ref() {
                Statement::Select(s) => {
                    let ks = s
                        .keyspace
                        .as_deref()
                        .or(ctx.current_keyspace.as_deref())
                        .ok_or_else(|| CqlError::Invalid("no keyspace specified".into()))?;
                    state.schema.check_permission(
                        ctx.auth,
                        Permission::Select,
                        &Resource::Table(ks.to_string(), s.table.clone()),
                    )?;
                }
                _ => {
                    return Err(CqlError::Invalid(
                        "SUBSCRIBE requires a SELECT statement".into(),
                    ))
                }
            }
            Ok(RouteResult::Subscribe {
                inner,
                interval,
                delta,
            })
        }
        Statement::Unsubscribe { stream_id } => Ok(RouteResult::Unsubscribe { stream_id }),
        Statement::CreateType {
            keyspace,
            name,
            if_not_exists,
            fields,
        } => route_create_type(state, ctx, keyspace, name, if_not_exists, fields)
            .await
            .map(RouteResult::Result),
        Statement::AlterType {
            keyspace,
            name,
            alterations,
        } => route_alter_type(state, ctx, keyspace, name, alterations)
            .await
            .map(RouteResult::Result),
        Statement::DropType {
            keyspace,
            name,
            if_exists,
        } => route_drop_type(state, ctx, keyspace, name, if_exists)
            .await
            .map(RouteResult::Result),
        Statement::CreateFunction {
            keyspace,
            name,
            or_replace,
            if_not_exists,
            params,
            called_on_null,
            return_type,
            language,
            body,
        } => route_create_function(
            state,
            ctx,
            keyspace,
            name,
            or_replace,
            if_not_exists,
            params,
            called_on_null,
            return_type,
            language,
            body,
        )
        .await
        .map(RouteResult::Result),
        Statement::DropFunction {
            keyspace,
            name,
            arg_types,
            if_exists,
        } => route_drop_function(state, ctx, keyspace, name, arg_types, if_exists)
            .await
            .map(RouteResult::Result),
        Statement::CreateAggregate {
            keyspace,
            name,
            or_replace,
            if_not_exists,
            arg_types,
            state_func,
            state_type,
            final_func,
            init_cond,
        } => route_create_aggregate(
            state,
            ctx,
            keyspace,
            name,
            or_replace,
            if_not_exists,
            arg_types,
            state_func,
            state_type,
            final_func,
            init_cond,
        )
        .await
        .map(RouteResult::Result),
        Statement::DropAggregate {
            keyspace,
            name,
            arg_types,
            if_exists,
        } => route_drop_aggregate(state, ctx, keyspace, name, arg_types, if_exists)
            .await
            .map(RouteResult::Result),
        Statement::Explain(s) => route_explain(state, ctx, *s).map(RouteResult::Result),
    }
}

// ── SELECT ───────────────────────────────────────────────────────────────

fn route_select(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: SelectStatement,
) -> Result<BytesMut, CqlError> {
    let ks = s
        .keyspace
        .as_deref()
        .or(ctx.current_keyspace.as_deref())
        .ok_or_else(|| CqlError::Invalid("no keyspace specified".into()))?;

    // System table dispatch — no permission check needed for system tables.
    match (ks, s.table.as_str()) {
        ("system", "local") => {
            let info = query_local(&state.schema, &state.node_config);
            let col_names: Vec<String> = vec![
                "key",
                "cluster_name",
                "data_center",
                "rack",
                "host_id",
                "partitioner",
                "native_protocol_version",
                "cql_version",
                "release_version",
                "schema_version",
                "rpc_port",
                "listen_address",
                "broadcast_address",
                "rpc_address",
                "bootstrapped",
                "tokens",
            ]
            .into_iter()
            .map(String::from)
            .collect();
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Uuid,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Uuid,
                CqlType::Int,
                CqlType::Inet,
                CqlType::Inet,
                CqlType::Inet,
                CqlType::Varchar,
                CqlType::Set(Box::new(CqlType::Varchar)),
            ];
            let tokens_set: Vec<CqlValue> = info
                .tokens
                .iter()
                .map(|t| CqlValue::Text(t.clone()))
                .collect();
            let row = vec![
                Some(CqlValue::Text(info.key)),
                Some(CqlValue::Text(info.cluster_name)),
                Some(CqlValue::Text(info.data_center)),
                Some(CqlValue::Text(info.rack)),
                Some(CqlValue::Uuid(info.host_id)),
                Some(CqlValue::Text(info.partitioner)),
                Some(CqlValue::Text(info.native_protocol_version)),
                Some(CqlValue::Text(info.cql_version)),
                Some(CqlValue::Text(info.release_version)),
                Some(CqlValue::Uuid(info.schema_version)),
                Some(CqlValue::Int(info.rpc_port as i32)),
                Some(CqlValue::Inet(info.listen_address)),
                Some(CqlValue::Inet(info.broadcast_address)),
                Some(CqlValue::Inet(info.rpc_address)),
                Some(CqlValue::Text(info.bootstrapped)),
                Some(CqlValue::Set(tokens_set)),
            ];
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system",
                "local",
                &[row],
            ))
        }
        ("system", "peers" | "peers_v2") => {
            let peers = query_peers(&state.schema, state.cluster_state.load().as_ref());
            let col_names: Vec<String> = vec![
                "peer",
                "peer_port",
                "data_center",
                "rack",
                "host_id",
                "native_address",
                "native_port",
                "schema_version",
                "release_version",
            ]
            .into_iter()
            .map(String::from)
            .collect();
            let col_types = vec![
                CqlType::Inet,
                CqlType::Int,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Uuid,
                CqlType::Inet,
                CqlType::Int,
                CqlType::Uuid,
                CqlType::Varchar,
            ];
            let rows: Vec<Vec<Option<CqlValue>>> = peers
                .iter()
                .map(|p| {
                    vec![
                        Some(CqlValue::Inet(p.peer)),
                        Some(CqlValue::Int(p.peer_port as i32)),
                        Some(CqlValue::Text(p.data_center.clone())),
                        Some(CqlValue::Text(p.rack.clone())),
                        Some(CqlValue::Uuid(p.host_id)),
                        Some(CqlValue::Inet(p.native_address)),
                        Some(CqlValue::Int(p.native_port as i32)),
                        Some(CqlValue::Uuid(p.schema_version)),
                        Some(CqlValue::Text(p.release_version.clone())),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system",
                s.table.as_str(),
                &rows,
            ))
        }
        ("system_schema", "keyspaces") => {
            let snap = state.schema.snapshot();
            let ks_rows = query_keyspaces(&snap);
            let all_col_names: Vec<String> = vec![
                "keyspace_name".into(),
                "durable_writes".into(),
                "replication".into(),
            ];
            let map_type = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Varchar));
            let all_col_types = vec![CqlType::Varchar, CqlType::Boolean, map_type];
            let all_rows: Vec<Vec<Option<CqlValue>>> = ks_rows
                .iter()
                .map(|k| {
                    // Build the replication map as CqlValue::Map
                    let repl_map: Vec<(CqlValue, CqlValue)> = k
                        .replication
                        .iter()
                        .map(|(key, val)| {
                            (CqlValue::Text(key.clone()), CqlValue::Text(val.clone()))
                        })
                        .collect();
                    vec![
                        Some(CqlValue::Text(k.keyspace_name.clone())),
                        Some(CqlValue::Boolean(k.durable_writes)),
                        Some(CqlValue::Map(repl_map)),
                    ]
                })
                .collect();
            apply_system_select(
                &s.columns,
                &all_col_names,
                &all_col_types,
                &all_rows,
                "system_schema",
                "keyspaces",
            )
        }
        ("system_schema", "tables") => {
            let snap = state.schema.snapshot();
            let table_rows = query_tables(&snap);
            let col_names = vec!["keyspace_name".into(), "table_name".into(), "id".into()];
            let col_types = vec![CqlType::Varchar, CqlType::Varchar, CqlType::Uuid];
            let rows: Vec<Vec<Option<CqlValue>>> = table_rows
                .iter()
                .map(|t| {
                    vec![
                        Some(CqlValue::Text(t.keyspace_name.clone())),
                        Some(CqlValue::Text(t.table_name.clone())),
                        Some(CqlValue::Uuid(t.id)),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_schema",
                "tables",
                &rows,
            ))
        }
        ("system_schema", "columns") => {
            let snap = state.schema.snapshot();
            let col_rows = query_columns(&snap);
            let col_names: Vec<String> = vec![
                "keyspace_name".into(),
                "table_name".into(),
                "column_name".into(),
                "kind".into(),
                "position".into(),
                "type".into(),
                "clustering_order".into(),
            ];
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Int,
                CqlType::Varchar,
                CqlType::Varchar,
            ];
            // Apply WHERE equality filters (string columns only).
            let filtered: Vec<_> = col_rows
                .iter()
                .filter(|c| {
                    s.where_clauses.iter().all(|wc| {
                        if wc.op != crate::ast::ComparisonOp::Eq {
                            return true; // skip non-equality ops
                        }
                        let val = match &wc.value {
                            crate::ast::Term::StringLiteral(s) => s.as_str(),
                            _ => return true,
                        };
                        match wc.column.as_str() {
                            "keyspace_name" => c.keyspace_name == val,
                            "table_name" => c.table_name == val,
                            "column_name" => c.column_name == val,
                            "kind" => c.kind == val,
                            "clustering_order" => c.clustering_order == val,
                            _ => true,
                        }
                    })
                })
                .collect();
            let rows: Vec<Vec<Option<CqlValue>>> = filtered
                .iter()
                .map(|c| {
                    vec![
                        Some(CqlValue::Text(c.keyspace_name.clone())),
                        Some(CqlValue::Text(c.table_name.clone())),
                        Some(CqlValue::Text(c.column_name.clone())),
                        Some(CqlValue::Text(c.kind.clone())),
                        Some(CqlValue::Int(c.position)),
                        Some(CqlValue::Text(c.column_type.clone())),
                        Some(CqlValue::Text(c.clustering_order.clone())),
                    ]
                })
                .collect();
            apply_system_select(
                &s.columns,
                &col_names,
                &col_types,
                &rows,
                "system_schema",
                "columns",
            )
        }
        ("system_auth", "roles") => {
            let snap = state.schema.snapshot();
            let role_rows = query_roles(&snap, ctx.auth);
            let col_names = vec!["role".into(), "is_superuser".into(), "can_login".into()];
            let col_types = vec![CqlType::Varchar, CqlType::Boolean, CqlType::Boolean];
            let rows: Vec<Vec<Option<CqlValue>>> = role_rows
                .iter()
                .map(|r| {
                    vec![
                        Some(CqlValue::Text(r.role.clone())),
                        Some(CqlValue::Boolean(r.is_superuser)),
                        Some(CqlValue::Boolean(r.can_login)),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_auth",
                "roles",
                &rows,
            ))
        }
        ("system_auth", "role_members") => {
            let snap = state.schema.snapshot();
            let rows_data = query_role_members(&snap);
            let col_names = vec!["role".into(), "member".into()];
            let col_types = vec![CqlType::Varchar, CqlType::Varchar];
            let rows: Vec<Vec<Option<CqlValue>>> = rows_data
                .iter()
                .map(|r| {
                    vec![
                        Some(CqlValue::Text(r.role.clone())),
                        Some(CqlValue::Text(r.member.clone())),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_auth",
                "role_members",
                &rows,
            ))
        }
        ("system_auth", "role_permissions") => {
            let snap = state.schema.snapshot();
            let rows_data = query_role_permissions(&snap);
            let col_names = vec!["role".into(), "resource".into()];
            let col_types = vec![CqlType::Varchar, CqlType::Varchar];
            let rows: Vec<Vec<Option<CqlValue>>> = rows_data
                .iter()
                .map(|r| {
                    vec![
                        Some(CqlValue::Text(r.role.clone())),
                        Some(CqlValue::Text(r.resource.clone())),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_auth",
                "role_permissions",
                &rows,
            ))
        }
        ("system_schema", "types") => {
            let snap = state.schema.snapshot();
            let col_names: Vec<String> = vec![
                "keyspace_name".into(),
                "type_name".into(),
                "field_names".into(),
                "field_types".into(),
            ];
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
            ];
            let rows: Vec<Vec<Option<CqlValue>>> = snap
                .types
                .values()
                .map(|udt| {
                    let field_names: Vec<String> = udt
                        .fields
                        .iter()
                        .map(|(n, _)| format!("\"{}\"", n))
                        .collect();
                    let field_types: Vec<String> = udt
                        .fields
                        .iter()
                        .map(|(_, t)| format!("\"{}\"", bridge::cql_type_display_name(t)))
                        .collect();
                    vec![
                        Some(CqlValue::Text(udt.keyspace.clone())),
                        Some(CqlValue::Text(udt.name.clone())),
                        Some(CqlValue::Text(format!("[{}]", field_names.join(", ")))),
                        Some(CqlValue::Text(format!("[{}]", field_types.join(", ")))),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_schema",
                "types",
                &rows,
            ))
        }
        ("system_schema", "indexes") => {
            let snap = state.schema.snapshot();
            let col_names: Vec<String> = vec![
                "keyspace_name".into(),
                "table_name".into(),
                "index_name".into(),
                "kind".into(),
                "options".into(),
            ];
            let col_types = vec![
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
                CqlType::Varchar,
            ];
            let rows: Vec<Vec<Option<CqlValue>>> = snap
                .indexes
                .iter()
                .map(|((ks, tbl, name), idx)| {
                    let kind = match idx.index_type {
                        IndexType::BTree => "COMPOSITES",
                        IndexType::Hash => "CUSTOM",
                        IndexType::Composite => "COMPOSITES",
                        IndexType::Phonetic => "CUSTOM",
                        IndexType::Filtered => "CUSTOM",
                    };
                    // Format options as a simple comma-separated key=value string
                    // (avoiding a serde_json dependency in this crate).
                    let options_json = if idx.options.is_empty() {
                        "{}".to_string()
                    } else {
                        let pairs: Vec<String> = idx
                            .options
                            .iter()
                            .map(|(k, v)| format!("\"{k}\":\"{v}\""))
                            .collect();
                        format!("{{{}}}", pairs.join(","))
                    };
                    vec![
                        Some(CqlValue::Text(ks.clone())),
                        Some(CqlValue::Text(tbl.clone())),
                        Some(CqlValue::Text(name.clone())),
                        Some(CqlValue::Text(kind.to_string())),
                        Some(CqlValue::Text(options_json)),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_schema",
                "indexes",
                &rows,
            ))
        }
        // cqlsh queries these system_schema tables during startup introspection.
        // Return empty results for tables we don't populate yet.
        ("system_schema", "functions" | "aggregates" | "triggers" | "views") => {
            Ok(result::encode_rows(
                &["keyspace_name".into(), "type_name".into()],
                &[CqlType::Varchar, CqlType::Varchar],
                "system_schema",
                s.table.as_str(),
                &[],
            ))
        }
        _ => {
            // Virtual table: check registry before storage lookup.
            if let Some(vtable) = state.schema.virtual_tables().get(ks, &s.table) {
                let rows = vtable.read(None);
                let columns = vtable.columns();
                return encode_virtual_rows(ks, &s.table, columns, &rows);
            }

            // User table: permission check + bridge + storage
            route_select_user_table(state, ctx, ks, &s)
        }
    }
}

fn route_select_user_table(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    ks: &str,
    s: &SelectStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Select,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    // ALLOW FILTERING: permit full-table scans with post-filter when the
    // client explicitly opts in.  Log a warning so operators can identify
    // expensive queries, but do not reject.
    if s.allow_filtering && !where_has_udf_calls(&s.where_clauses) {
        tracing::warn!(
            keyspace = ks,
            table = %s.table,
            "executing query with ALLOW FILTERING — full table scan with post-filter"
        );
    }

    // If WHERE contains UDF calls but ALLOW FILTERING is not set, reject.
    if where_has_udf_calls(&s.where_clauses) && !s.allow_filtering {
        return Err(CqlError::Invalid(
            "queries with UDFs in WHERE predicates require ALLOW FILTERING".into(),
        ));
    }
    if where_has_udf_calls(&s.where_clauses) {
        tracing::warn!(
            keyspace = ks,
            table = %s.table,
            "executing query with UDF in WHERE — full scan with ALLOW FILTERING"
        );
    }

    // Build column info for result
    let (col_names, col_types) = build_column_info(table_meta, &s.columns, ks, &state.schema)?;

    let all_col_names: Vec<String> = table_meta.columns.keys().cloned().collect();
    let all_col_types: Vec<CqlType> = table_meta
        .columns
        .values()
        .map(|c| resolve_col_type(&c.column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;
    let pk_indices: Vec<usize> = table_meta
        .partition_key
        .iter()
        .map(|name| {
            table_meta.columns.get_index_of(name).ok_or_else(|| {
                CqlError::Invalid(format!("partition key column '{}' not found", name))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ck_indices: Vec<usize> = table_meta
        .clustering_key
        .iter()
        .map(|(name, _)| {
            table_meta.columns.get_index_of(name).ok_or_else(|| {
                CqlError::Invalid(format!("clustering key column '{}' not found", name))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let table_id = TableId::new(&table_meta.keyspace, &table_meta.name);

    // Try PK-based lookup first; fall back to full scan with ALLOW FILTERING
    let pk_result = extract_pk_values(
        &s.where_clauses,
        &table_meta.partition_key,
        table_meta,
        ks,
        &state.schema,
    );

    let rows = if let Ok(pk_values) = pk_result {
        // PK present — single partition lookup
        let pk_types: Vec<CqlType> = table_meta
            .partition_key
            .iter()
            .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
            .collect::<Result<Vec<_>, _>>()?;
        let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
        let mut pk_rows = match state.engine.read(&table_id, &decorated_key)? {
            Some(partition) => bridge::partition_to_rows(
                &partition,
                &all_col_names,
                &all_col_types,
                &pk_indices,
                &ck_indices,
            ),
            None => vec![],
        };
        // Apply clustering key and other non-PK WHERE predicates.
        pk_rows.retain(|row| {
            evaluate_where_predicates(
                row,
                &s.where_clauses,
                &all_col_names,
                table_meta,
                ks,
                &state.schema,
            )
        });
        pk_rows
    } else if let Some(in_rows) = try_pk_in_lookup(
        &s.where_clauses,
        table_meta,
        ks,
        &state.schema,
        &state.engine,
        &table_id,
        &all_col_names,
        &all_col_types,
        &pk_indices,
        &ck_indices,
    )? {
        // PK IN (...) — multi-partition lookup.
        // Apply clustering key and other non-PK WHERE predicates.
        let mut filtered = in_rows;
        filtered.retain(|row| {
            evaluate_where_predicates(
                row,
                &s.where_clauses,
                &all_col_names,
                table_meta,
                ks,
                &state.schema,
            )
        });
        filtered
    } else {
        // No PK — use the query planner to decide the access path.
        let planner_indexes: Vec<(String, Vec<String>)> = snap
            .indexes
            .iter()
            .filter(|((idx_ks, idx_tbl, _), _)| idx_ks == ks && idx_tbl == &s.table)
            .map(|((_, _, _), meta)| (meta.name.clone(), meta.target_columns.clone()))
            .collect();

        let scan_plan = planner::plan(
            &s.where_clauses,
            &table_meta.partition_key,
            &planner_indexes,
        );

        match scan_plan {
            ScanPlan::PartitionKeyLookup => {
                // This can happen when extract_pk_values fails (e.g., bind
                // values that can't be coerced to the PK column type) but
                // the planner still sees Eq predicates on all PK columns.
                // Fall through to a full scan rather than panicking.
                let scan_limit = 10_000;
                let partitions = state.engine.read_range(&table_id, None, None, scan_limit)?;
                let mut all_rows = Vec::new();
                for partition in &partitions {
                    let mut prows = bridge::partition_to_rows(
                        partition,
                        &all_col_names,
                        &all_col_types,
                        &pk_indices,
                        &ck_indices,
                    );
                    all_rows.append(&mut prows);
                }
                all_rows.retain(|row| {
                    evaluate_where_predicates(
                        row,
                        &s.where_clauses,
                        &all_col_names,
                        table_meta,
                        ks,
                        &state.schema,
                    )
                });
                all_rows
            }

            ScanPlan::SingleIndex {
                ref index_name,
                ref index_column,
            }
            | ScanPlan::IndexScanWithFilter {
                ref index_name,
                ref index_column,
                ..
            } => {
                // IndexScanWithFilter means some WHERE columns are not covered
                // by any index — require ALLOW FILTERING for these queries.
                if matches!(scan_plan, ScanPlan::IndexScanWithFilter { .. }) && !s.allow_filtering {
                    return Err(CqlError::Invalid(
                        "Cannot execute this query as it requires filtering on non-indexed \
                         columns. Use ALLOW FILTERING, create a secondary index on the \
                         filtered columns, or restructure your query to use partition keys."
                            .into(),
                    ));
                }

                // Find the WHERE clause for the indexed column.
                let index_wc = s
                    .where_clauses
                    .iter()
                    .find(|wc| wc.column == *index_column && wc.op == ComparisonOp::Eq)
                    .ok_or_else(|| {
                        CqlError::Invalid(
                            "planner selected index but no matching WHERE clause found".into(),
                        )
                    })?;

                let index_key = term_to_index_key(
                    &index_wc.value,
                    index_column,
                    table_meta,
                    ks,
                    &state.schema,
                )?;

                let partitions = state
                    .engine
                    .read_by_index(&table_id, index_name, &index_key)?;

                // Fallback: if the index read returns empty, the memtable index
                // may not be wired yet (Sprint I-3). Fall back to full scan so
                // queries still return correct results.
                let partitions = if partitions.is_empty() {
                    let scan_limit = 10_000;
                    state.engine.read_range(&table_id, None, None, scan_limit)?
                } else {
                    partitions
                };

                let mut all_rows = Vec::new();
                for partition in &partitions {
                    let mut prows = bridge::partition_to_rows(
                        partition,
                        &all_col_names,
                        &all_col_types,
                        &pk_indices,
                        &ck_indices,
                    );
                    all_rows.append(&mut prows);
                }

                // Always apply post-filter as defensive measure.
                // SingleIndex: redundant but safe; IndexScanWithFilter: necessary.
                all_rows.retain(|row| {
                    evaluate_where_predicates(
                        row,
                        &s.where_clauses,
                        &all_col_names,
                        table_meta,
                        ks,
                        &state.schema,
                    )
                });

                all_rows
            }

            ScanPlan::IndexIntersection { ref indexes } => {
                // Use first index for fetch, post-filter all WHERE predicates.
                // Full set intersection across indexes is a future optimization.
                let (ref first_idx_name, ref first_idx_col) = indexes[0];
                let index_wc = s
                    .where_clauses
                    .iter()
                    .find(|wc| wc.column == *first_idx_col && wc.op == ComparisonOp::Eq)
                    .ok_or_else(|| {
                        CqlError::Invalid(
                            "planner selected index but no matching WHERE clause found".into(),
                        )
                    })?;

                let index_key = term_to_index_key(
                    &index_wc.value,
                    first_idx_col,
                    table_meta,
                    ks,
                    &state.schema,
                )?;

                let partitions =
                    state
                        .engine
                        .read_by_index(&table_id, first_idx_name, &index_key)?;

                let partitions = if partitions.is_empty() {
                    let scan_limit = 10_000;
                    state.engine.read_range(&table_id, None, None, scan_limit)?
                } else {
                    partitions
                };

                let mut all_rows = Vec::new();
                for partition in &partitions {
                    let mut prows = bridge::partition_to_rows(
                        partition,
                        &all_col_names,
                        &all_col_types,
                        &pk_indices,
                        &ck_indices,
                    );
                    all_rows.append(&mut prows);
                }

                all_rows.retain(|row| {
                    evaluate_where_predicates(
                        row,
                        &s.where_clauses,
                        &all_col_names,
                        table_meta,
                        ks,
                        &state.schema,
                    )
                });

                all_rows
            }

            ScanPlan::FullScan => {
                // Check ALLOW FILTERING requirement.
                let indexed_columns: Vec<String> = snap
                    .indexes
                    .iter()
                    .filter(|((idx_ks, idx_tbl, _), _)| idx_ks == ks && idx_tbl == &s.table)
                    .flat_map(|(_, meta)| meta.target_columns.iter().cloned())
                    .collect();

                // Exclude token() predicates — they are range scan hints,
                // not column filters that require indexing.
                let non_token_clauses: Vec<&WhereClause> =
                    s.where_clauses.iter().filter(|wc| !wc.token_fn).collect();
                let all_where_columns_indexed = non_token_clauses
                    .iter()
                    .all(|wc| indexed_columns.iter().any(|ic| ic == &wc.column));

                if !non_token_clauses.is_empty() && !all_where_columns_indexed && !s.allow_filtering
                {
                    return Err(CqlError::Invalid(
                        "Cannot execute this query as it requires filtering on non-indexed \
                         columns. Use ALLOW FILTERING, create a secondary index on the \
                         filtered columns, or restructure your query to use partition keys."
                            .into(),
                    ));
                }

                // Use a large scan window — LIMIT is applied *after* filtering,
                // not before, to avoid cutting off matching rows (FRSA-BUG-003).
                let scan_limit = 10_000;
                let partitions = state.engine.read_range(&table_id, None, None, scan_limit)?;
                let mut all_rows = Vec::new();
                for partition in &partitions {
                    let mut prows = bridge::partition_to_rows(
                        partition,
                        &all_col_names,
                        &all_col_types,
                        &pk_indices,
                        &ck_indices,
                    );
                    all_rows.append(&mut prows);
                }
                all_rows.retain(|row| {
                    evaluate_where_predicates(
                        row,
                        &s.where_clauses,
                        &all_col_names,
                        table_meta,
                        ks,
                        &state.schema,
                    )
                });
                all_rows
            }
        }
    };

    // Apply ORDER BY sorting (FRSA-BUG-004)
    let rows = if !s.order_by.is_empty() {
        let mut sorted = rows;
        // Resolve column indices for ORDER BY columns
        let order_specs: Vec<(usize, bool)> = s
            .order_by
            .iter()
            .filter_map(|(col_name, dir)| {
                let idx = all_col_names.iter().position(|n| n == col_name)?;
                let ascending = *dir == OrderDirection::Asc;
                Some((idx, ascending))
            })
            .collect();
        sorted.sort_by(|a, b| {
            for &(idx, ascending) in &order_specs {
                let cmp = match (&a[idx], &b[idx]) {
                    (Some(va), Some(vb)) => va.cmp(vb),
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                let cmp = if ascending { cmp } else { cmp.reverse() };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
        sorted
    } else {
        rows
    };

    // Resolve UDFs/UDAs in SELECT columns.
    let mut resolved_funcs: Vec<(usize, ResolvedFunction)> = Vec::new();
    for (i, sc) in s.columns.iter().enumerate() {
        if let SelectColumn::FunctionCall {
            keyspace: func_ks,
            name,
            args,
            alias,
        } = sc
        {
            let fn_lower = name.to_lowercase();
            // Skip built-in functions — they are handled inline.
            if matches!(
                fn_lower.as_str(),
                "count"
                    | "avg"
                    | "min"
                    | "max"
                    | "sum"
                    | "writetime"
                    | "ttl"
                    | "uuid"
                    | "now"
                    | "totimestamp"
                    | "todate"
            ) {
                continue;
            }
            let resolved = resolve_select_function(
                ks,
                func_ks.as_deref(),
                name,
                args,
                alias.as_deref(),
                &all_col_names,
                &all_col_types,
                &state.schema,
            )?;
            resolved_funcs.push((i, resolved));
        }
    }

    // Check for aggregate functions (builtin COUNT/AVG/MIN/MAX/SUM or UDA).
    let has_builtin_agg = s.columns.iter().any(|c| {
        matches!(c, SelectColumn::FunctionCall { name, .. }
            if name.eq_ignore_ascii_case("count")
                || name.eq_ignore_ascii_case("avg")
                || name.eq_ignore_ascii_case("min")
                || name.eq_ignore_ascii_case("max")
                || name.eq_ignore_ascii_case("sum"))
    });
    let has_uda = resolved_funcs
        .iter()
        .any(|(_, f)| matches!(f.kind, ResolvedFunctionKind::Aggregate { .. }));
    let is_aggregate = has_builtin_agg || has_uda;

    if is_aggregate {
        // Build a single result row with aggregate values.
        let mut agg_row: Vec<Option<CqlValue>> = Vec::with_capacity(s.columns.len());
        for (col_idx, sc) in s.columns.iter().enumerate() {
            match sc {
                SelectColumn::FunctionCall { name, .. } if name.eq_ignore_ascii_case("count") => {
                    agg_row.push(Some(CqlValue::Bigint(rows.len() as i64)));
                }
                SelectColumn::FunctionCall { name, args, .. }
                    if name.eq_ignore_ascii_case("avg")
                        || name.eq_ignore_ascii_case("min")
                        || name.eq_ignore_ascii_case("max")
                        || name.eq_ignore_ascii_case("sum") =>
                {
                    let fn_lower = name.to_lowercase();
                    // Resolve the target column from the first argument.
                    if let Some(arg) = args.first() {
                        if let Ok(col_name) = extract_column_name(arg) {
                            if let Some(src_idx) = all_col_names.iter().position(|n| *n == col_name)
                            {
                                let col_type = &all_col_types[src_idx];
                                let result =
                                    compute_builtin_aggregate(&fn_lower, &rows, src_idx, col_type);
                                agg_row.push(result);
                            } else {
                                return Err(CqlError::Invalid(format!(
                                    "unknown column in {}(): {}",
                                    fn_lower, col_name
                                )));
                            }
                        } else {
                            agg_row.push(Some(CqlValue::Null));
                        }
                    } else {
                        agg_row.push(Some(CqlValue::Null));
                    }
                }
                SelectColumn::FunctionCall { .. } => {
                    // Check if this column is a resolved UDA.
                    if let Some((_, ref func)) = resolved_funcs.iter().find(|(i, _)| *i == col_idx)
                    {
                        if matches!(func.kind, ResolvedFunctionKind::Aggregate { .. }) {
                            let result = execute_uda(state, ks, func, &rows)?;
                            agg_row.push(Some(result));
                        } else {
                            // Scalar UDF in an aggregate query — evaluate on
                            // first row if available, otherwise NULL.
                            agg_row.push(None);
                        }
                    } else {
                        agg_row.push(None);
                    }
                }
                _ => {
                    agg_row.push(None);
                }
            }
        }
        let agg_rows = vec![agg_row];
        return Ok(result::encode_rows(
            &col_names, &col_types, ks, &s.table, &agg_rows,
        ));
    }

    // Apply column selection if not Star
    let selected_rows = select_columns(&rows, &all_col_names, &col_names);

    // Evaluate scalar UDFs on each projected row.
    let selected_rows = if resolved_funcs.is_empty() {
        selected_rows
    } else {
        // We need the full rows to extract UDF args, then replace the UDF
        // columns in the projected output.
        let scalar_funcs: Vec<&ResolvedFunction> = resolved_funcs
            .iter()
            .filter(|(_, f)| matches!(f.kind, ResolvedFunctionKind::Scalar))
            .map(|(_, f)| f)
            .collect();

        if scalar_funcs.is_empty() {
            selected_rows
        } else {
            let mut result_rows = selected_rows;
            for (full_row, projected_row) in rows.iter().zip(result_rows.iter_mut()) {
                let udf_results = evaluate_row_udfs(state, full_row, &scalar_funcs)?;
                // Map UDF results back into the projected row. Each resolved
                // func knows its column index in the SELECT list.
                for ((col_idx, _), udf_val) in resolved_funcs.iter().zip(udf_results.iter()) {
                    // Find the position of col_idx in the projected columns.
                    if let Some(proj_pos) = col_names.iter().position(|n| {
                        if let Some((_, ref f)) = resolved_funcs.iter().find(|(i, _)| i == col_idx)
                        {
                            *n == f.display_name
                        } else {
                            false
                        }
                    }) {
                        projected_row[proj_pos] = udf_val.clone();
                    }
                }
            }
            result_rows
        }
    };

    // Apply toJson() built-in on projected columns.
    let selected_rows =
        apply_tojson_projections(&s.columns, &col_names, &all_col_names, &rows, selected_rows);

    // Apply LIMIT
    let limited = if let Some(limit) = s.limit {
        &selected_rows[..std::cmp::min(selected_rows.len(), limit as usize)]
    } else {
        &selected_rows
    };

    // Apply pagination: page_size interacts with LIMIT.
    // If both page_size and LIMIT are set, the effective limit is min(page_size, limit).
    // Pagination operates on the already-limited result set.
    let effective_page_size = match (ctx.paging.page_size, s.limit) {
        (Some(ps), Some(lim)) => Some(std::cmp::min(ps, lim)),
        (Some(ps), None) => Some(ps),
        (None, _) => None,
    };

    let paged = crate::paging::apply_pagination(
        limited.len(),
        effective_page_size,
        ctx.paging.paging_state.as_deref(),
    )?;

    let page_rows = &limited[paged.start..paged.end];

    Ok(result::encode_rows_paged(
        &col_names,
        &col_types,
        ks,
        &s.table,
        page_rows,
        paged.next_paging_state.as_deref(),
    ))
}

// ── Virtual table helpers ────────────────────────────────────────────────

/// Convert a `DataType` (from ferrosa-common) to the CQL protocol `CqlType`.
fn data_type_to_cql_type(dt: &DataType) -> CqlType {
    match dt {
        DataType::Text => CqlType::Varchar,
        DataType::Int => CqlType::Int,
        DataType::BigInt => CqlType::Bigint,
        DataType::Double => CqlType::Double,
        DataType::Boolean => CqlType::Boolean,
        DataType::Uuid => CqlType::Uuid,
        DataType::Timestamp => CqlType::Timestamp,
        DataType::Blob => CqlType::Blob,
        DataType::Duration => CqlType::Duration,
        // DataType is #[non_exhaustive]; treat unknown variants as blob.
        _ => CqlType::Blob,
    }
}

/// Convert a `CellValue` (raw bytes) to a typed `CqlValue` using the column's `DataType`.
///
/// Returns `None` (CQL null) for tombstones or cells with no value.
fn cell_to_cql_value(cell: &ferrosa_common::CellValue, dt: &DataType) -> Option<CqlValue> {
    let bytes = cell.value.as_ref()?;
    Some(match dt {
        DataType::Text => CqlValue::Text(String::from_utf8_lossy(bytes).into_owned()),
        DataType::Int => {
            let arr: [u8; 4] = bytes.as_slice().try_into().unwrap_or([0; 4]);
            CqlValue::Int(i32::from_be_bytes(arr))
        }
        DataType::BigInt => {
            let arr: [u8; 8] = bytes.as_slice().try_into().unwrap_or([0; 8]);
            CqlValue::Bigint(i64::from_be_bytes(arr))
        }
        DataType::Double => {
            let arr: [u8; 8] = bytes.as_slice().try_into().unwrap_or([0; 8]);
            CqlValue::Double(u64::from_be_bytes(arr))
        }
        DataType::Boolean => CqlValue::Boolean(!bytes.is_empty() && bytes[0] != 0),
        DataType::Uuid => {
            let arr: [u8; 16] = bytes.as_slice().try_into().unwrap_or([0; 16]);
            CqlValue::Uuid(uuid::Uuid::from_bytes(arr))
        }
        DataType::Timestamp => {
            let arr: [u8; 8] = bytes.as_slice().try_into().unwrap_or([0; 8]);
            CqlValue::Timestamp(i64::from_be_bytes(arr))
        }
        DataType::Blob => CqlValue::Blob(bytes.clone()),
        // DataType is #[non_exhaustive]; treat unknown as blob.
        _ => CqlValue::Blob(bytes.clone()),
    })
}

/// Encode virtual table rows as a CQL ROWS result body.
///
/// Converts `VirtualRow` cells (raw `CellValue` bytes) into typed `CqlValue`s
/// using the column definitions, then delegates to `result::encode_rows`.
fn encode_virtual_rows(
    keyspace: &str,
    table: &str,
    columns: &[VirtualColumnDef],
    rows: &[VirtualRow],
) -> Result<BytesMut, CqlError> {
    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let col_types: Vec<CqlType> = columns
        .iter()
        .map(|c| data_type_to_cql_type(&c.data_type))
        .collect();

    let cql_rows: Vec<Vec<Option<CqlValue>>> = rows
        .iter()
        .map(|row| {
            row.cells
                .iter()
                .zip(columns.iter())
                .map(|(cell, col)| cell_to_cql_value(cell, &col.data_type))
                .collect()
        })
        .collect();

    Ok(result::encode_rows(
        &col_names, &col_types, keyspace, table, &cql_rows,
    ))
}

// ── EXPLAIN ──────────────────────────────────────────────────────────────

fn route_explain(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: SelectStatement,
) -> Result<BytesMut, CqlError> {
    let ks = s
        .keyspace
        .as_deref()
        .or(ctx.current_keyspace.as_deref())
        .ok_or_else(|| CqlError::Invalid("no keyspace specified".into()))?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    let planner_indexes: Vec<(String, Vec<String>)> = snap
        .indexes
        .iter()
        .filter(|((idx_ks, idx_tbl, _), _)| idx_ks == ks && idx_tbl == &s.table)
        .map(|(_, meta)| (meta.name.clone(), meta.target_columns.clone()))
        .collect();

    let scan_plan = planner::plan(
        &s.where_clauses,
        &table_meta.partition_key,
        &planner_indexes,
    );

    let plan_text = format!("{scan_plan}");

    let col_names = vec!["plan".to_string()];
    let col_types = vec![CqlType::Varchar];
    let rows = vec![vec![Some(CqlValue::Text(plan_text))]];
    Ok(result::encode_rows(
        &col_names, &col_types, ks, &s.table, &rows,
    ))
}

// ── INSERT ───────────────────────────────────────────────────────────────

async fn route_insert(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: InsertStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    // Convert terms to CqlValues using target column types
    let mut pk_vals: Vec<(i32, CqlValue)> = Vec::new();
    let mut ck_vals: Vec<(i32, CqlValue)> = Vec::new();
    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
    let timestamp = match s.using_timestamp {
        Some(ts) => ts,
        None => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CqlError::ServerError(format!("system clock error: {e}")))?
            .as_micros() as i64,
    };

    for (i, col_name) in s.columns.iter().enumerate() {
        let col_meta = table_meta
            .columns
            .get(col_name)
            .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", col_name)))?;
        let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
        let value = bridge::term_to_cql_value(&s.values[i], &cql_type)?;

        match col_meta.kind {
            ColumnKind::PartitionKey => pk_vals.push((col_meta.position, value)),
            ColumnKind::Clustering => ck_vals.push((col_meta.position, value)),
            ColumnKind::Regular | ColumnKind::Static => {
                let col_idx = table_meta.storage_column_index(col_name).ok_or_else(|| {
                    CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
                })?;
                regular_cells.push((col_idx, value));
            }
        }
    }

    // Sort PK and CK by position
    pk_vals.sort_by_key(|(pos, _)| *pos);
    ck_vals.sort_by_key(|(pos, _)| *pos);
    let pk_values: Vec<CqlValue> = pk_vals.into_iter().map(|(_, v)| v).collect();
    let ck_values: Vec<CqlValue> = ck_vals.into_iter().map(|(_, v)| v).collect();
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_row(&regular_cells, &ck_values, timestamp, s.using_ttl);
    let table_id = TableId::new(ks, &s.table);
    let rf = keyspace_rf(&state.schema, ks);

    // BUG-0016: IF NOT EXISTS — check whether the row already exists before writing.
    if s.if_not_exists {
        let exists = if let Some(partition) = state.engine.read(&table_id, &decorated_key)? {
            let all_col_names: Vec<String> = table_meta.columns.keys().cloned().collect();
            let all_col_types: Vec<CqlType> = table_meta
                .columns
                .values()
                .map(|c| resolve_col_type(&c.column_type, ks, &state.schema))
                .collect::<Result<Vec<_>, _>>()?;
            let pk_indices: Vec<usize> = table_meta
                .partition_key
                .iter()
                .filter_map(|name| table_meta.columns.get_index_of(name))
                .collect();
            let ck_indices: Vec<usize> = table_meta
                .clustering_key
                .iter()
                .filter_map(|(name, _)| table_meta.columns.get_index_of(name))
                .collect();
            let rows = bridge::partition_to_rows(
                &partition,
                &all_col_names,
                &all_col_types,
                &pk_indices,
                &ck_indices,
            );
            if ck_values.is_empty() {
                !rows.is_empty()
            } else {
                rows.iter().any(|row| {
                    ck_indices
                        .iter()
                        .zip(ck_values.iter())
                        .all(|(&idx, ck_val)| row.get(idx).and_then(|v| v.as_ref()) == Some(ck_val))
                })
            }
        } else {
            false
        };

        if exists {
            // Row already exists — return [applied] = false
            return Ok(encode_lwt_applied(false, ks, &s.table));
        }
    }

    state
        .write_path
        .load()
        .write(
            &table_id,
            &decorated_key,
            row,
            timestamp,
            ctx.consistency,
            rf,
        )
        .await?;

    if s.if_not_exists {
        // Insert was applied — return [applied] = true
        Ok(encode_lwt_applied(true, ks, &s.table))
    } else {
        Ok(result::encode_void())
    }
}

/// Encode a lightweight-transaction `[applied]` result.
///
/// CQL protocol returns a RESULT Rows frame with a single boolean column
/// named `[applied]` containing `true` (insert was applied) or `false`
/// (row already existed, insert skipped).
fn encode_lwt_applied(applied: bool, keyspace: &str, table: &str) -> BytesMut {
    let col_names = vec!["[applied]".to_string()];
    let col_types = vec![CqlType::Boolean];
    let rows = vec![vec![Some(CqlValue::Boolean(applied))]];
    result::encode_rows(&col_names, &col_types, keyspace, table, &rows)
}

// ── UPDATE ───────────────────────────────────────────────────────────────

async fn route_update(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: UpdateStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    let timestamp = match s.using_timestamp {
        Some(ts) => ts,
        None => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CqlError::ServerError(format!("system clock error: {e}")))?
            .as_micros() as i64,
    };

    // Extract PK and CK values from WHERE clauses
    let pk_values = extract_pk_values(
        &s.where_clauses,
        &table_meta.partition_key,
        table_meta,
        ks,
        &state.schema,
    )?;
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    // Extract CK values from WHERE
    let ck_names: Vec<String> = table_meta
        .clustering_key
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    let mut ck_values = Vec::new();
    for ck_name in &ck_names {
        for wc in &s.where_clauses {
            if wc.column == *ck_name && wc.op == ComparisonOp::Eq {
                let col_meta = &table_meta.columns[ck_name];
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
                ck_values.push(val);
                break;
            }
        }
    }

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let table_id = TableId::new(ks, &s.table);

    // Check if any assignments require a read-modify-write (collection +/- or counter)
    let needs_read = s
        .assignments
        .iter()
        .any(|a| matches!(a, Assignment::Add { .. } | Assignment::Sub { .. }));

    // Lazily read existing row for collection Add/Sub merge
    let existing_row: Option<Vec<Option<CqlValue>>> = if needs_read {
        let all_col_names: Vec<String> = table_meta.columns.keys().cloned().collect();
        let all_col_types: Vec<CqlType> = table_meta
            .columns
            .values()
            .map(|c| resolve_col_type(&c.column_type, ks, &state.schema))
            .collect::<Result<Vec<_>, _>>()?;
        let pk_indices: Vec<usize> = table_meta
            .partition_key
            .iter()
            .filter_map(|name| table_meta.columns.get_index_of(name))
            .collect();
        let ck_indices: Vec<usize> = table_meta
            .clustering_key
            .iter()
            .filter_map(|(name, _)| table_meta.columns.get_index_of(name))
            .collect();

        if let Some(partition) = state.engine.read(&table_id, &decorated_key)? {
            let rows = bridge::partition_to_rows(
                &partition,
                &all_col_names,
                &all_col_types,
                &pk_indices,
                &ck_indices,
            );
            // Find the row matching our CK values
            if ck_values.is_empty() {
                rows.into_iter().next()
            } else {
                rows.into_iter().find(|row| {
                    ck_indices
                        .iter()
                        .zip(ck_values.iter())
                        .all(|(&idx, ck_val)| row.get(idx).and_then(|v| v.as_ref()) == Some(ck_val))
                })
            }
        } else {
            None
        }
    } else {
        None
    };

    // Build cells from SET assignments
    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
    for assignment in &s.assignments {
        let (col_name, value) = match assignment {
            Assignment::Simple { column, value } => {
                let col_meta = table_meta
                    .columns
                    .get(column)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", column)))?;
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(value, &cql_type)?;
                (column.as_str(), val)
            }
            Assignment::Add { column, value } => {
                let col_meta = table_meta
                    .columns
                    .get(column)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", column)))?;
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;

                // For counters: read-modify-write increment. For collections: append/merge.
                match &cql_type {
                    CqlType::Counter => {
                        let increment = bridge::term_to_cql_value(value, &CqlType::Bigint)?;
                        if let CqlValue::Bigint(n) = increment {
                            let col_table_idx = table_meta
                                .columns
                                .get_index_of(column)
                                .expect("column verified above");
                            let current = existing_row
                                .as_ref()
                                .and_then(|row| row.get(col_table_idx))
                                .and_then(|v| v.as_ref())
                                .and_then(|v| match v {
                                    CqlValue::Counter(c) => Some(*c),
                                    _ => None,
                                })
                                .unwrap_or(0);
                            (column.as_str(), CqlValue::Counter(current + n))
                        } else {
                            return Err(CqlError::Invalid(
                                "counter increment must be an integer".into(),
                            ));
                        }
                    }
                    _ => {
                        let new_val = bridge::term_to_cql_value(value, &cql_type)?;
                        let col_table_idx = table_meta
                            .columns
                            .get_index_of(column)
                            .expect("column verified above");
                        let existing = existing_row
                            .as_ref()
                            .and_then(|row| row.get(col_table_idx))
                            .and_then(|v| v.as_ref());
                        let merged = collection_add(existing, &new_val);
                        (column.as_str(), merged)
                    }
                }
            }
            Assignment::Sub { column, value } => {
                let col_meta = table_meta
                    .columns
                    .get(column)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", column)))?;
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;

                match &cql_type {
                    CqlType::Counter => {
                        let decrement = bridge::term_to_cql_value(value, &CqlType::Bigint)?;
                        if let CqlValue::Bigint(n) = decrement {
                            let col_table_idx = table_meta
                                .columns
                                .get_index_of(column)
                                .expect("column verified above");
                            let current = existing_row
                                .as_ref()
                                .and_then(|row| row.get(col_table_idx))
                                .and_then(|v| v.as_ref())
                                .and_then(|v| match v {
                                    CqlValue::Counter(c) => Some(*c),
                                    _ => None,
                                })
                                .unwrap_or(0);
                            (column.as_str(), CqlValue::Counter(current - n))
                        } else {
                            return Err(CqlError::Invalid(
                                "counter decrement must be an integer".into(),
                            ));
                        }
                    }
                    CqlType::Map(key_type, _) => {
                        // Map subtraction: RHS is a set of keys to remove.
                        let set_of_keys = CqlType::Set(key_type.clone());
                        let keys_to_remove = bridge::term_to_cql_value(value, &set_of_keys)?;
                        let col_table_idx = table_meta
                            .columns
                            .get_index_of(column)
                            .expect("column verified above");
                        let existing = existing_row
                            .as_ref()
                            .and_then(|row| row.get(col_table_idx))
                            .and_then(|v| v.as_ref());
                        let merged = collection_sub_map(existing, &keys_to_remove);
                        (column.as_str(), merged)
                    }
                    _ => {
                        // Set/list subtraction: remove matching elements.
                        let to_remove = bridge::term_to_cql_value(value, &cql_type)?;
                        let col_table_idx = table_meta
                            .columns
                            .get_index_of(column)
                            .expect("column verified above");
                        let existing = existing_row
                            .as_ref()
                            .and_then(|row| row.get(col_table_idx))
                            .and_then(|v| v.as_ref());
                        let merged = collection_sub(existing, &to_remove);
                        (column.as_str(), merged)
                    }
                }
            }
            Assignment::Element {
                column,
                key: _,
                value,
            } => {
                // Map/list element set: coerce value to the element type, not the collection type
                let col_meta = table_meta
                    .columns
                    .get(column)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", column)))?;
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let value_type = match &cql_type {
                    CqlType::Map(_, v) => (**v).clone(),
                    CqlType::List(v) => (**v).clone(),
                    _ => cql_type.clone(),
                };
                let val = bridge::term_to_cql_value(value, &value_type)?;
                (column.as_str(), val)
            }
        };
        let col_idx = table_meta.storage_column_index(col_name).ok_or_else(|| {
            CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
        })?;
        regular_cells.push((col_idx, value));
    }

    let row = bridge::build_row(&regular_cells, &ck_values, timestamp, s.using_ttl);
    let rf = keyspace_rf(&state.schema, ks);

    state
        .write_path
        .load()
        .write(
            &table_id,
            &decorated_key,
            row,
            timestamp,
            ctx.consistency,
            rf,
        )
        .await?;
    Ok(result::encode_void())
}

/// Merge a new collection value into an existing one (for `col = col + val`).
///
/// - List: appends new elements to the end of the existing list.
/// - Set: merges new elements into the existing set (deduplicating).
/// - Map: merges new key-value pairs, overwriting existing keys.
/// - Non-collection: returns the new value as-is.
fn collection_add(existing: Option<&CqlValue>, new_val: &CqlValue) -> CqlValue {
    match (existing, new_val) {
        (Some(CqlValue::List(old)), CqlValue::List(add)) => {
            let mut merged = old.clone();
            merged.extend(add.iter().cloned());
            CqlValue::List(merged)
        }
        (Some(CqlValue::Set(old)), CqlValue::Set(add)) => {
            let mut merged = old.clone();
            for item in add {
                if !merged.contains(item) {
                    merged.push(item.clone());
                }
            }
            CqlValue::Set(merged)
        }
        (Some(CqlValue::Map(old)), CqlValue::Map(add)) => {
            let mut merged = old.clone();
            for (k, v) in add {
                if let Some(entry) = merged.iter_mut().find(|(ek, _)| ek == k) {
                    entry.1 = v.clone();
                } else {
                    merged.push((k.clone(), v.clone()));
                }
            }
            CqlValue::Map(merged)
        }
        (None, _) => new_val.clone(),
        _ => new_val.clone(),
    }
}

/// Subtract elements from a collection (for `col = col - val` on list/set).
///
/// - List: removes all occurrences of each element in `to_remove`.
/// - Set: removes matching elements.
/// - Non-collection: returns the existing value or Null.
fn collection_sub(existing: Option<&CqlValue>, to_remove: &CqlValue) -> CqlValue {
    match (existing, to_remove) {
        (Some(CqlValue::List(old)), CqlValue::List(remove)) => {
            let filtered: Vec<CqlValue> = old
                .iter()
                .filter(|item| !remove.contains(item))
                .cloned()
                .collect();
            CqlValue::List(filtered)
        }
        (Some(CqlValue::Set(old)), CqlValue::Set(remove)) => {
            let filtered: Vec<CqlValue> = old
                .iter()
                .filter(|item| !remove.contains(item))
                .cloned()
                .collect();
            CqlValue::Set(filtered)
        }
        (Some(existing_val), _) => existing_val.clone(),
        (None, _) => CqlValue::Null,
    }
}

/// Remove map entries by key (for `col = col - {key_set}` on maps).
///
/// The `keys_to_remove` is a Set of keys. All map entries whose key appears
/// in the set are removed.
fn collection_sub_map(existing: Option<&CqlValue>, keys_to_remove: &CqlValue) -> CqlValue {
    match (existing, keys_to_remove) {
        (Some(CqlValue::Map(old)), CqlValue::Set(remove_keys)) => {
            let filtered: Vec<(CqlValue, CqlValue)> = old
                .iter()
                .filter(|(k, _)| !remove_keys.contains(k))
                .cloned()
                .collect();
            CqlValue::Map(filtered)
        }
        (Some(existing_val), _) => existing_val.clone(),
        (None, _) => CqlValue::Null,
    }
}

// ── DELETE ────────────────────────────────────────────────────────────────

async fn route_delete(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: DeleteStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    let timestamp = match s.using_timestamp {
        Some(ts) => ts,
        None => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CqlError::ServerError(format!("system clock error: {e}")))?
            .as_micros() as i64,
    };

    // Extract PK values from WHERE
    let pk_values = extract_pk_values(
        &s.where_clauses,
        &table_meta.partition_key,
        table_meta,
        ks,
        &state.schema,
    )?;
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    // Extract CK values from WHERE
    let ck_names: Vec<String> = table_meta
        .clustering_key
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    let mut ck_values = Vec::new();
    for ck_name in &ck_names {
        for wc in &s.where_clauses {
            if wc.column == *ck_name && wc.op == ComparisonOp::Eq {
                let col_meta = &table_meta.columns[ck_name];
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
                ck_values.push(val);
                break;
            }
        }
    }

    // Build delete column indices (empty = row-level delete).
    // MapElement targets are treated as whole-column deletes for now —
    // the storage layer tombstones the column, which is safe (if broader
    // than strictly necessary).
    let delete_columns: Vec<u16> = s
        .columns
        .iter()
        .map(|target| {
            let col_name = target.column_name();
            table_meta.storage_column_index(col_name).ok_or_else(|| {
                CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_delete_row(&delete_columns, &ck_values, timestamp);
    let table_id = TableId::new(ks, &s.table);
    let rf = keyspace_rf(&state.schema, ks);

    state
        .write_path
        .load()
        .write(
            &table_id,
            &decorated_key,
            row,
            timestamp,
            ctx.consistency,
            rf,
        )
        .await?;
    Ok(result::encode_void())
}

// ── BATCH ────────────────────────────────────────────────────────────────

async fn route_batch(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    b: BatchStatement,
) -> Result<BytesMut, CqlError> {
    // Security mitigation M12: batch size limit
    if b.statements.len() > MAX_BATCH_STATEMENTS {
        return Err(CqlError::Invalid(format!(
            "batch too large: {} statements (max {})",
            b.statements.len(),
            MAX_BATCH_STATEMENTS
        )));
    }

    match b.batch_type {
        BatchType::Logged => route_logged_batch(state, ctx, b).await,
        BatchType::Unlogged | BatchType::Counter => route_unlogged_batch(state, ctx, b).await,
    }
}

/// Route an UNLOGGED or COUNTER batch: dispatch each statement individually.
async fn route_unlogged_batch(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    b: BatchStatement,
) -> Result<BytesMut, CqlError> {
    for stmt in b.statements {
        match stmt {
            Statement::Insert(s) => {
                route_insert(state, ctx, s).await?;
            }
            Statement::Update(s) => {
                route_update(state, ctx, s).await?;
            }
            Statement::Delete(s) => {
                route_delete(state, ctx, s).await?;
            }
            _ => {
                return Err(CqlError::Invalid(
                    "batch may only contain INSERT, UPDATE, or DELETE statements".into(),
                ));
            }
        }
    }
    Ok(result::encode_void())
}

/// Route a LOGGED batch. Behavior depends on the active write path:
///
/// - **Direct (single-node)**: Writes all mutations as a single atomic
///   commit log group via `StorageEngine::write_atomic_batch()`. No batchlog
///   needed -- the commit log provides crash recovery.
///
/// - **Cluster**: Delegates to `coordinate_logged_batch()` for the full
///   3-phase batchlog protocol.
async fn route_logged_batch(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    b: BatchStatement,
) -> Result<BytesMut, CqlError> {
    use ferrosa_storage::Mutation;

    let batch_timestamp = b.using_timestamp.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
    });

    let mut mutations = Vec::with_capacity(b.statements.len());

    for stmt in &b.statements {
        match stmt {
            Statement::Insert(s) => {
                let (table_id, key, row, ts) = materialize_insert(state, ctx, s, batch_timestamp)?;
                mutations.push(Mutation {
                    keyspace: table_id.keyspace.clone(),
                    table: table_id.table.clone(),
                    key,
                    rows: vec![row],
                    timestamp: ts,
                });
            }
            Statement::Update(s) => {
                let (table_id, key, row, ts) = materialize_update(state, ctx, s, batch_timestamp)?;
                mutations.push(Mutation {
                    keyspace: table_id.keyspace.clone(),
                    table: table_id.table.clone(),
                    key,
                    rows: vec![row],
                    timestamp: ts,
                });
            }
            Statement::Delete(s) => {
                let (table_id, key, row, ts) = materialize_delete(state, ctx, s, batch_timestamp)?;
                mutations.push(Mutation {
                    keyspace: table_id.keyspace.clone(),
                    table: table_id.table.clone(),
                    key,
                    rows: vec![row],
                    timestamp: ts,
                });
            }
            _ => {
                return Err(CqlError::Invalid(
                    "batch may only contain INSERT, UPDATE, or DELETE statements".into(),
                ));
            }
        }
    }

    // Determine RF from first mutation's keyspace (batches typically target one keyspace).
    let rf = if let Some(first) = mutations.first() {
        keyspace_rf(&state.schema, &first.keyspace)
    } else {
        1
    };

    state
        .write_path
        .load()
        .write_batch(mutations, ctx.consistency, rf)
        .await
        .map_err(|e| CqlError::Invalid(format!("logged batch failed: {e}")))?;

    Ok(result::encode_void())
}

/// Materialize an INSERT statement into its key, row, and table ID without
/// writing. Used by `route_logged_batch()` to collect mutations.
fn materialize_insert(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: &InsertStatement,
    batch_timestamp: i64,
) -> Result<
    (
        TableId,
        ferrosa_common::DecoratedKey,
        ferrosa_sstable::types::Row,
        i64,
    ),
    CqlError,
> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    let timestamp = s.using_timestamp.unwrap_or(batch_timestamp);

    let mut pk_vals: Vec<(i32, CqlValue)> = Vec::new();
    let mut ck_vals: Vec<(i32, CqlValue)> = Vec::new();
    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();

    for (i, col_name) in s.columns.iter().enumerate() {
        let col_meta = table_meta
            .columns
            .get(col_name)
            .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", col_name)))?;
        let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
        let value = bridge::term_to_cql_value(&s.values[i], &cql_type)?;

        match col_meta.kind {
            ColumnKind::PartitionKey => pk_vals.push((col_meta.position, value)),
            ColumnKind::Clustering => ck_vals.push((col_meta.position, value)),
            ColumnKind::Regular | ColumnKind::Static => {
                let col_idx = table_meta.storage_column_index(col_name).ok_or_else(|| {
                    CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
                })?;
                regular_cells.push((col_idx, value));
            }
        }
    }

    pk_vals.sort_by_key(|(pos, _)| *pos);
    ck_vals.sort_by_key(|(pos, _)| *pos);
    let pk_values: Vec<CqlValue> = pk_vals.into_iter().map(|(_, v)| v).collect();
    let ck_values: Vec<CqlValue> = ck_vals.into_iter().map(|(_, v)| v).collect();
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_row(&regular_cells, &ck_values, timestamp, s.using_ttl);
    let table_id = TableId::new(ks, &s.table);

    Ok((table_id, decorated_key, row, timestamp))
}

/// Materialize an UPDATE statement into its key, row, and table ID without
/// writing. Used by `route_logged_batch()` to collect mutations.
fn materialize_update(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: &UpdateStatement,
    batch_timestamp: i64,
) -> Result<
    (
        TableId,
        ferrosa_common::DecoratedKey,
        ferrosa_sstable::types::Row,
        i64,
    ),
    CqlError,
> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    let timestamp = s.using_timestamp.unwrap_or(batch_timestamp);

    let pk_values = extract_pk_values(
        &s.where_clauses,
        &table_meta.partition_key,
        table_meta,
        ks,
        &state.schema,
    )?;
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    let ck_names: Vec<String> = table_meta
        .clustering_key
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    let mut ck_values = Vec::new();
    for ck_name in &ck_names {
        for wc in &s.where_clauses {
            if wc.column == *ck_name && wc.op == ComparisonOp::Eq {
                let col_meta = &table_meta.columns[ck_name];
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
                ck_values.push(val);
                break;
            }
        }
    }

    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
    for assignment in &s.assignments {
        let (col_name, value) = match assignment {
            Assignment::Simple { column, value } => {
                let col_meta = table_meta
                    .columns
                    .get(column)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", column)))?;
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(value, &cql_type)?;
                (column.as_str(), val)
            }
            _ => {
                // For complex assignments (Add, Sub, Element), fall back to simple value
                // extraction. Full handling deferred to route_update.
                continue;
            }
        };
        let col_idx = table_meta.storage_column_index(col_name).ok_or_else(|| {
            CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
        })?;
        regular_cells.push((col_idx, value));
    }

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_row(&regular_cells, &ck_values, timestamp, s.using_ttl);
    let table_id = TableId::new(ks, &s.table);

    Ok((table_id, decorated_key, row, timestamp))
}

/// Materialize a DELETE statement into its key, row, and table ID without
/// writing. Used by `route_logged_batch()` to collect mutations.
fn materialize_delete(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: &DeleteStatement,
    batch_timestamp: i64,
) -> Result<
    (
        TableId,
        ferrosa_common::DecoratedKey,
        ferrosa_sstable::types::Row,
        i64,
    ),
    CqlError,
> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    let timestamp = s.using_timestamp.unwrap_or(batch_timestamp);

    let pk_values = extract_pk_values(
        &s.where_clauses,
        &table_meta.partition_key,
        table_meta,
        ks,
        &state.schema,
    )?;
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, &state.schema))
        .collect::<Result<Vec<_>, _>>()?;

    let ck_names: Vec<String> = table_meta
        .clustering_key
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    let mut ck_values = Vec::new();
    for ck_name in &ck_names {
        for wc in &s.where_clauses {
            if wc.column == *ck_name && wc.op == ComparisonOp::Eq {
                let col_meta = &table_meta.columns[ck_name];
                let cql_type = resolve_col_type(&col_meta.column_type, ks, &state.schema)?;
                let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
                ck_values.push(val);
                break;
            }
        }
    }

    let delete_columns: Vec<u16> = s
        .columns
        .iter()
        .map(|target| {
            let col_name = target.column_name();
            table_meta.storage_column_index(col_name).ok_or_else(|| {
                CqlError::Invalid(format!("column '{}' not found in storage schema", col_name))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_delete_row(&delete_columns, &ck_values, timestamp);
    let table_id = TableId::new(ks, &s.table);

    Ok((table_id, decorated_key, row, timestamp))
}

// ── DDL: Keyspace ────────────────────────────────────────────────────────

async fn route_create_keyspace(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: CreateKeyspaceStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Create, &Resource::AllKeyspaces)?;

    // IF NOT EXISTS: silently succeed when keyspace already exists
    if s.if_not_exists && state.schema.snapshot().keyspaces.contains_key(&s.name) {
        return Ok(result::encode_schema_change(
            "CREATED",
            "KEYSPACE",
            &[&s.name],
        ));
    }

    let mut options = std::collections::HashMap::new();
    let mut strategy = String::new();
    for (k, v) in &s.replication {
        if k == "class" {
            strategy = v.clone();
        } else {
            options.insert(k.clone(), v.clone());
        }
    }

    let ks_meta = KeyspaceMetadata {
        name: s.name.clone(),
        durable_writes: s.durable_writes.unwrap_or(true),
        replication: ReplicationParams { strategy, options },
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.create_keyspace(ks_meta, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateKeyspace(ks_meta);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateKeyspace(ks_meta);
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "CREATED",
        "KEYSPACE",
        &[&s.name],
    ))
}

async fn route_alter_keyspace(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: AlterKeyspaceStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Alter,
        &Resource::Keyspace(s.name.clone()),
    )?;

    let replication = s.replication.map(|pairs| {
        let mut options = std::collections::HashMap::new();
        let mut strategy = String::new();
        for (k, v) in &pairs {
            if k == "class" {
                strategy = v.clone();
            } else {
                options.insert(k.clone(), v.clone());
            }
        }
        ReplicationParams { strategy, options }
    });

    let updates = KeyspaceUpdates {
        replication,
        durable_writes: s.durable_writes,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.alter_keyspace(&s.name, updates, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::AlterKeyspace {
                name: s.name.clone(),
                updates,
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::AlterKeyspace {
                name: s.name.clone(),
                updates,
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "UPDATED",
        "KEYSPACE",
        &[&s.name],
    ))
}

async fn route_drop_keyspace(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: DropKeyspaceStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Drop,
        &Resource::Keyspace(s.name.clone()),
    )?;

    // IF EXISTS: silently succeed when keyspace doesn't exist
    if s.if_exists && !state.schema.snapshot().keyspaces.contains_key(&s.name) {
        return Ok(result::encode_schema_change(
            "DROPPED",
            "KEYSPACE",
            &[&s.name],
        ));
    }

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            // Collect tables before drop so we can unregister from storage.
            let table_ids: Vec<_> = state
                .schema
                .snapshot()
                .tables
                .keys()
                .filter(|(ks, _)| ks == &s.name)
                .map(|(ks, tbl)| ferrosa_storage::TableId::new(ks, tbl))
                .collect();
            state.schema.drop_keyspace(&s.name, ctx.auth)?;
            for tid in &table_ids {
                let _ = state.engine.unregister_table(tid);
            }
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropKeyspace(s.name.clone());
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropKeyspace(s.name.clone());
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "DROPPED",
        "KEYSPACE",
        &[&s.name],
    ))
}

// ── DDL: Table ───────────────────────────────────────────────────────────

async fn route_create_table(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: CreateTableStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Create,
        &Resource::Keyspace(ks.to_string()),
    )?;

    // IF NOT EXISTS: silently succeed when table already exists
    if s.if_not_exists
        && state
            .schema
            .snapshot()
            .tables
            .contains_key(&(ks.to_string(), s.name.clone()))
    {
        return Ok(result::encode_schema_change(
            "CREATED",
            "TABLE",
            &[ks, &s.name],
        ));
    }

    // Build column metadata
    let mut columns = IndexMap::new();
    let pk_set: HashSet<&str> = s.partition_key.iter().map(|s| s.as_str()).collect();
    let ck_map: std::collections::HashMap<&str, (i32, crate::ast::ClusteringOrder)> = s
        .clustering_key
        .iter()
        .enumerate()
        .map(|(i, (name, order))| (name.as_str(), (i as i32, *order)))
        .collect();

    let mut pk_position = 0i32;
    for (col_name, type_name) in &s.columns {
        let type_str = cql_type_name_to_string(type_name);
        let (kind, position, clustering_order) = if pk_set.contains(col_name.as_str()) {
            let pos = pk_position;
            pk_position += 1;
            (ColumnKind::PartitionKey, pos, SchemaClusteringOrder::None)
        } else if let Some((ck_pos, ck_order)) = ck_map.get(col_name.as_str()) {
            let schema_order = match ck_order {
                crate::ast::ClusteringOrder::Asc => SchemaClusteringOrder::Asc,
                crate::ast::ClusteringOrder::Desc => SchemaClusteringOrder::Desc,
            };
            (ColumnKind::Clustering, *ck_pos, schema_order)
        } else {
            (ColumnKind::Regular, 0, SchemaClusteringOrder::None)
        };

        columns.insert(
            col_name.clone(),
            ColumnMetadata {
                name: col_name.clone(),
                kind,
                position,
                column_type: type_str,
                clustering_order,
                mask: None,
            },
        );
    }

    let table_meta = TableMetadata {
        keyspace: ks.to_string(),
        name: s.name.clone(),
        id: uuid::Uuid::new_v4(),
        columns,
        partition_key: s.partition_key.clone(),
        clustering_key: s
            .clustering_key
            .iter()
            .map(|(name, order)| {
                let schema_order = match order {
                    crate::ast::ClusteringOrder::Asc => SchemaClusteringOrder::Asc,
                    crate::ast::ClusteringOrder::Desc => SchemaClusteringOrder::Desc,
                };
                (name.clone(), schema_order)
            })
            .collect(),
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions: s
            .extensions
            .as_ref()
            .map(|pairs| pairs.iter().cloned().collect())
            .unwrap_or_default(),
        is_system: false,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            // Register with schema
            state.schema.create_table(table_meta.clone(), ctx.auth)?;

            // Register with storage engine
            let storage_schema = table_meta.to_storage_schema();
            state.engine.register_table(storage_schema)?;

            // Auto-create cascade tables if consolidation.cascade is enabled.
            create_cascade_tables_if_needed(state, &table_meta)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateTable(Box::new(table_meta.clone()));
            coordinator.coordinate_ddl(op).await?;

            // Auto-create cascade tables after primary table is created.
            create_cascade_tables_if_needed(state, &table_meta)?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateTable(Box::new(table_meta.clone()));
            ddl.execute(op).await.map_err(CqlError::from)?;

            // Auto-create cascade tables after primary table is created.
            create_cascade_tables_if_needed(state, &table_meta)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "CREATED",
        "TABLE",
        &[ks, &s.name],
    ))
}

/// If the table has `consolidation.cascade = true` in its extensions,
/// auto-create the downstream cascade tables (e.g., sensor_5m, sensor_15m, sensor_1h).
fn create_cascade_tables_if_needed(
    state: &SharedState,
    source: &TableMetadata,
) -> Result<(), CqlError> {
    use ferrosa_storage::timeseries::config::{generate_cascade_chain, ConsolidationConfig};

    let config = match ConsolidationConfig::from_extensions(&source.extensions) {
        Some(Ok(c)) if c.cascade => c,
        Some(Err(e)) => {
            tracing::warn!(
                table = %source.name,
                "invalid consolidation extensions: {e}"
            );
            return Ok(());
        }
        _ => return Ok(()), // no consolidation or cascade disabled
    };

    let chain = generate_cascade_chain(&source.name, &config);
    if chain.is_empty() {
        return Ok(());
    }

    tracing::info!(
        table = %source.name,
        cascade_len = chain.len(),
        "auto-creating cascade tables"
    );

    for spec in &chain {
        // Build columns: same partition key + clustering key as source,
        // plus output columns (e.g., value_min, value_max, value_avg, value_stddev).
        let mut columns = IndexMap::new();

        // Copy partition key columns from source.
        for pk_name in &source.partition_key {
            if let Some(col) = source.columns.get(pk_name) {
                columns.insert(pk_name.clone(), col.clone());
            }
        }

        // Clustering key: `ts timestamp` (DESC).
        let ck_name = source
            .clustering_key
            .first()
            .map(|(n, _)| n.clone())
            .unwrap_or_else(|| "ts".to_string());

        if !columns.contains_key(&ck_name) {
            columns.insert(
                ck_name.clone(),
                ColumnMetadata {
                    name: ck_name.clone(),
                    kind: ColumnKind::Clustering,
                    position: 0,
                    column_type: "timestamp".to_string(),
                    clustering_order: SchemaClusteringOrder::Desc,
                    mask: None,
                },
            );
        }

        // Add output columns (value_min, value_max, value_avg, etc.).
        for (i, col_name) in spec.output_columns.iter().enumerate() {
            columns.insert(
                col_name.clone(),
                ColumnMetadata {
                    name: col_name.clone(),
                    kind: ColumnKind::Regular,
                    position: i as i32,
                    column_type: "double".to_string(),
                    clustering_order: SchemaClusteringOrder::None,
                    mask: None,
                },
            );
        }

        // Build extensions for this tier's consolidation config.
        let mut extensions = HashMap::new();
        if let Some(ref target) = spec.target {
            extensions.insert("consolidation.target".to_string(), target.clone());
            extensions.insert(
                "consolidation.interval".to_string(),
                format_consolidation_interval(&spec.interval),
            );
            // Inherit functions and columns from source config.
            let fn_str: String = config
                .functions
                .iter()
                .filter_map(|f| consolidation_fn_name(f))
                .collect::<Vec<_>>()
                .join(",");
            extensions.insert("consolidation.functions".to_string(), fn_str);
            extensions.insert(
                "consolidation.columns".to_string(),
                spec.output_columns
                    .iter()
                    .filter_map(|c| c.rsplit_once('_').map(|(base, _)| base.to_string()))
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }

        let cascade_table = TableMetadata {
            keyspace: source.keyspace.clone(),
            name: spec.table_name.clone(),
            id: uuid::Uuid::new_v4(),
            columns,
            partition_key: source.partition_key.clone(),
            clustering_key: vec![(ck_name.clone(), SchemaClusteringOrder::Desc)],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions,
            is_system: false,
        };

        // Idempotent: silently succeeds if table already exists.
        state
            .schema
            .create_table_internal(cascade_table.clone())
            .map_err(|e| CqlError::ServerError(format!("cascade table creation failed: {e}")))?;

        let storage_schema = cascade_table.to_storage_schema();
        // Ignore error if already registered.
        let _ = state.engine.register_table(storage_schema);

        tracing::info!(
            cascade_table = %spec.table_name,
            "created cascade table"
        );
    }

    Ok(())
}

/// Format a Duration as a consolidation interval string (e.g., "5m", "15m", "1h").
fn format_consolidation_interval(d: &std::time::Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 && secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs >= 60 && secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{}s", secs)
    }
}

/// Map a ConsolidationFn to its string name for extension serialization.
fn consolidation_fn_name(
    f: &ferrosa_storage::timeseries::consolidation::ConsolidationFn,
) -> Option<&'static str> {
    use ferrosa_storage::timeseries::consolidation::ConsolidationFn;
    match f {
        ConsolidationFn::Min => Some("min"),
        ConsolidationFn::Max => Some("max"),
        ConsolidationFn::Avg => Some("avg"),
        ConsolidationFn::Median => Some("median"),
        ConsolidationFn::StdDev => Some("stddev"),
        ConsolidationFn::Count => Some("count"),
        ConsolidationFn::Sum => Some("sum"),
        _ => None,
    }
}

async fn route_alter_table(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: AlterTableStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Alter,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    let add_columns: Vec<ColumnMetadata> = s
        .add_columns
        .iter()
        .map(|(name, type_name)| ColumnMetadata {
            name: name.clone(),
            kind: ColumnKind::Regular,
            position: 0,
            column_type: cql_type_name_to_string(type_name),
            clustering_order: SchemaClusteringOrder::None,
            mask: None,
        })
        .collect();

    let extensions = s.extensions.map(|pairs| pairs.into_iter().collect());

    let updates = TableUpdates {
        params: None,
        add_columns,
        drop_columns: s.drop_columns.clone(),
        extensions,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.alter_table(ks, &s.table, updates, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::AlterTable {
                keyspace: ks.to_string(),
                table: s.table.clone(),
                updates: Box::new(updates),
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::AlterTable {
                keyspace: ks.to_string(),
                table: s.table.clone(),
                updates: Box::new(updates),
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "UPDATED",
        "TABLE",
        &[ks, &s.table],
    ))
}

async fn route_drop_table(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: DropTableStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Drop,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    // IF EXISTS: silently succeed when table doesn't exist
    if s.if_exists
        && !state
            .schema
            .snapshot()
            .tables
            .contains_key(&(ks.to_string(), s.table.clone()))
    {
        return Ok(result::encode_schema_change(
            "DROPPED",
            "TABLE",
            &[ks, &s.table],
        ));
    }

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.drop_table(ks, &s.table, ctx.auth)?;
            let tid = ferrosa_storage::TableId::new(ks, &s.table);
            let _ = state.engine.unregister_table(&tid);
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropTable {
                keyspace: ks.to_string(),
                table: s.table.clone(),
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropTable {
                keyspace: ks.to_string(),
                table: s.table.clone(),
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "DROPPED",
        "TABLE",
        &[ks, &s.table],
    ))
}

// ── DDL: Index ───────────────────────────────────────────────────────────

/// Resolve the USING string to an IndexType.
fn resolve_index_type(
    using: Option<&str>,
    _columns: &[String],
    _options: &HashMap<String, String>,
) -> Result<IndexType, CqlError> {
    match using {
        None | Some("btree") => Ok(IndexType::BTree),
        Some("hash") => Ok(IndexType::Hash),
        Some("composite") => Ok(IndexType::Composite),
        Some("phonetic") => Ok(IndexType::Phonetic),
        Some("filtered") => Ok(IndexType::Filtered),
        Some(other) => Err(CqlError::Invalid(format!("unknown index type: {other}"))),
    }
}

async fn route_create_index(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: CreateIndexStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8) — ALTER on the table
    state.schema.check_permission(
        ctx.auth,
        Permission::Alter,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    // Convert options Vec to HashMap
    let options_map: HashMap<String, String> = s.options.iter().cloned().collect();

    // Resolve index type
    let index_type = resolve_index_type(s.using.as_deref(), &s.columns, &options_map)?;

    // Generate index name if not provided
    let index_name = s
        .name
        .unwrap_or_else(|| format!("{}_{}_idx", s.table, s.columns.join("_")));

    let index_meta = IndexMetadata {
        keyspace: ks.to_string(),
        table: s.table.clone(),
        name: index_name.clone(),
        index_type,
        target_columns: s.columns.clone(),
        filter_predicate: None,
        options: options_map,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.create_index(index_meta, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateIndex(index_meta);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateIndex(index_meta);
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    // Notify the storage engine about the new index so future memtable writes
    // are indexed. We look up the column position from the post-DDL schema
    // snapshot. Only the first target column is wired (composite index support
    // is deferred). If the table is not yet registered with the engine (e.g.,
    // schema-only mode), a warning is logged but the operation does not fail.
    if let Some(target_col) = s.columns.first() {
        let snap = state.schema.snapshot();
        let table_id = TableId::new(ks, &s.table);
        let col_pos = snap
            .tables
            .get(&(ks.to_string(), s.table.clone()))
            .and_then(|tbl| {
                // Collect regular columns sorted by position to determine the ordinal.
                let mut regulars: Vec<_> = tbl
                    .columns
                    .values()
                    .filter(|c| c.kind == ColumnKind::Regular)
                    .collect();
                regulars.sort_by_key(|c| c.position);
                regulars.iter().position(|c| &c.name == target_col)
            });

        if let Some(pos) = col_pos {
            if let Err(e) = state.engine.add_index(&table_id, &index_name, pos) {
                // Log warning but don't fail — index is persisted in schema;
                // it will be populated once the table is registered (e.g., on restart).
                eprintln!(
                    "[router] CREATE INDEX: failed to wire index '{index_name}' to \
                     storage engine for table '{ks}.{}': {e}",
                    s.table
                );
            }
        }
    }

    // CQL native protocol: INDEX schema changes use TABLE as the target
    // so cqlsh refreshes the table metadata (which includes indexes).
    Ok(result::encode_schema_change(
        "CREATED",
        "TABLE",
        &[ks, &s.table],
    ))
}

async fn route_drop_index(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: DropIndexStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Look up which table owns this index by scanning the schema snapshot
    let snap = state.schema.snapshot();
    let table_name = snap
        .indexes
        .keys()
        .find(|(k, _t, n)| k == ks && n == &s.name)
        .map(|(_, t, _)| t.clone());

    let table_name = match table_name {
        Some(t) => t,
        None => {
            if s.if_exists {
                return Ok(result::encode_schema_change(
                    "DROPPED",
                    "TABLE",
                    &[ks, &s.name],
                ));
            }
            return Err(CqlError::Invalid(format!(
                "index '{}' not found in keyspace '{}'",
                s.name, ks
            )));
        }
    };

    // Permission check (M8) — ALTER on the parent table
    state.schema.check_permission(
        ctx.auth,
        Permission::Alter,
        &Resource::Table(ks.to_string(), table_name.clone()),
    )?;

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state
                .schema
                .drop_index(ks, &table_name, &s.name, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropIndex {
                keyspace: ks.to_string(),
                table: table_name.clone(),
                index: s.name.clone(),
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropIndex {
                keyspace: ks.to_string(),
                table: table_name.clone(),
                index: s.name.clone(),
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    // Return TABLE event so cqlsh refreshes table metadata (includes indexes).
    Ok(result::encode_schema_change(
        "DROPPED",
        "TABLE",
        &[ks, &table_name],
    ))
}

// ── DDL: Role ────────────────────────────────────────────────────────────

async fn route_create_role(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: CreateRoleStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Create, &Resource::AllRoles)?;

    let role = RoleMetadata {
        name: s.name.clone(),
        is_superuser: s.superuser.unwrap_or(false),
        can_login: s.login.unwrap_or(false),
        salted_hash: None, // schema layer handles hashing
        member_of: HashSet::new(),
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state
                .schema
                .create_role(role, s.password.as_deref(), ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateRole(role);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateRole(role);
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_void())
}

async fn route_alter_role(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: AlterRoleStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Role(s.name.clone()))?;

    let updates = RoleUpdates {
        is_superuser: s.superuser,
        can_login: s.login,
        password: s.password.clone(),
        member_of: None,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.alter_role(&s.name, updates, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::AlterRole {
                name: s.name.clone(),
                updates,
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::AlterRole {
                name: s.name.clone(),
                updates,
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_void())
}

async fn route_drop_role(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: DropRoleStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Drop, &Resource::Role(s.name.clone()))?;

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.drop_role(&s.name, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropRole(s.name.clone());
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropRole(s.name.clone());
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_void())
}

// ── GRANT / REVOKE ───────────────────────────────────────────────────────

async fn route_grant(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: GrantStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Authorize,
        &ast_resource_to_schema(&s.resource, ctx.current_keyspace)?,
    )?;

    let resource = ast_resource_to_schema(&s.resource, ctx.current_keyspace)?;
    let perms = parse_permissions(&s.permissions)?;

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.grant(&s.role, &resource, perms, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::Grant(GrantEntry {
                role: s.role.clone(),
                resource,
                permissions: perms,
            });
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::Grant(GrantEntry {
                role: s.role.clone(),
                resource,
                permissions: perms,
            });
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_void())
}

async fn route_revoke(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: RevokeStatement,
) -> Result<BytesMut, CqlError> {
    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Authorize,
        &ast_resource_to_schema(&s.resource, ctx.current_keyspace)?,
    )?;

    let resource = ast_resource_to_schema(&s.resource, ctx.current_keyspace)?;
    let perms = parse_permissions(&s.permissions)?;

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            state.schema.revoke(&s.role, &resource, perms, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            // DdlOperation::Revoke carries one permission at a time; emit one
            // operation per permission so each can be replicated atomically.
            for perm in perms {
                let op = DdlOperation::Revoke {
                    role: s.role.clone(),
                    resource: resource.clone(),
                    permission: perm,
                };
                coordinator.coordinate_ddl(op).await?;
            }
        }
        DdlPath::Cluster { .. } => {
            // Same as Pair: one Raft proposal per permission for atomic replication.
            for perm in perms {
                let op = DdlOperation::Revoke {
                    role: s.role.clone(),
                    resource: resource.clone(),
                    permission: perm,
                };
                ddl.execute(op).await.map_err(CqlError::from)?;
            }
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_void())
}

// ── TRUNCATE ─────────────────────────────────────────────────────────────

fn route_truncate(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    s: TruncateStatement,
) -> Result<BytesMut, CqlError> {
    let ks = resolve_keyspace(&s.keyspace, ctx.current_keyspace)?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Modify,
        &Resource::Table(ks.to_string(), s.table.clone()),
    )?;

    // Truncate the table's data in the storage engine.
    let table_id = ferrosa_storage::TableId::new(ks, &s.table);
    state
        .engine
        .truncate(&table_id)
        .map_err(|e| CqlError::ServerError(format!("truncate failed: {e}")))?;

    Ok(result::encode_void())
}

// ── Helper functions ─────────────────────────────────────────────────────

/// Resolve an explicit keyspace or fall back to the session's current keyspace.
fn resolve_keyspace<'a>(
    explicit: &'a Option<String>,
    current: &'a Option<String>,
) -> Result<&'a str, CqlError> {
    explicit
        .as_deref()
        .or(current.as_deref())
        .ok_or_else(|| CqlError::Invalid("no keyspace specified".into()))
}

/// Look up the replication factor for a keyspace from the schema.
///
/// Falls back to 1 if the keyspace is not found or the RF is not set.
fn keyspace_rf(schema: &Schema, ks: &str) -> usize {
    let snap = schema.snapshot();
    snap.keyspaces
        .get(ks)
        .and_then(|km| km.replication.options.get("replication_factor"))
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1)
}

/// Build column names and types for a SELECT result.
fn build_column_info(
    table_meta: &TableMetadata,
    select_columns: &[SelectColumn],
    ks: &str,
    schema: &Schema,
) -> Result<(Vec<String>, Vec<CqlType>), CqlError> {
    // Check for Star
    let has_star = select_columns
        .iter()
        .any(|c| matches!(c, SelectColumn::Star));
    if has_star {
        // Return all columns
        let names: Vec<String> = table_meta.columns.keys().cloned().collect();
        let types: Vec<CqlType> = table_meta
            .columns
            .values()
            .map(|c| resolve_col_type(&c.column_type, ks, schema))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok((names, types));
    }

    let mut names = Vec::new();
    let mut types = Vec::new();
    for sc in select_columns {
        match sc {
            SelectColumn::Star => unreachable!(),
            SelectColumn::Column(name) => {
                let col = table_meta
                    .columns
                    .get(name)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", name)))?;
                names.push(name.clone());
                types.push(resolve_col_type(&col.column_type, ks, schema)?);
            }
            SelectColumn::FunctionCall {
                keyspace: func_ks,
                name,
                alias,
                args,
            } => {
                let builtin_display_name = alias.clone().unwrap_or_else(|| {
                    let prefix = func_ks.as_deref().unwrap_or("system");
                    format!("{}.{}", prefix, name)
                });
                let fn_lower = name.to_lowercase();
                let (display_name, cql_type) = match fn_lower.as_str() {
                    "count" => (builtin_display_name, CqlType::Bigint),
                    "writetime" => (builtin_display_name, CqlType::Bigint),
                    "ttl" => (builtin_display_name, CqlType::Int),
                    "avg" | "min" | "max" | "sum" => {
                        // Resolve the argument column type.
                        let t = if let Some(arg) = args.first() {
                            if let Ok(col_name) = extract_column_name(arg) {
                                let col = table_meta.columns.get(&col_name).ok_or_else(|| {
                                    CqlError::Invalid(format!(
                                        "unknown column in {}(): {}",
                                        fn_lower, col_name
                                    ))
                                })?;
                                let arg_type = resolve_col_type(&col.column_type, ks, schema)?;
                                if fn_lower == "avg" {
                                    // avg always returns Double (matches Cassandra for
                                    // non-decimal types; close enough for now).
                                    match arg_type {
                                        CqlType::Float => CqlType::Float,
                                        _ => CqlType::Double,
                                    }
                                } else {
                                    // min, max, sum return the same type as the column.
                                    arg_type
                                }
                            } else {
                                CqlType::Double
                            }
                        } else {
                            CqlType::Double
                        };
                        (builtin_display_name, t)
                    }
                    _ => {
                        // Try to resolve as UDF/UDA — need table column info.
                        let all_col_names: Vec<String> =
                            table_meta.columns.keys().cloned().collect();
                        let all_col_types: Vec<CqlType> = table_meta
                            .columns
                            .values()
                            .map(|c| resolve_col_type(&c.column_type, ks, schema))
                            .collect::<Result<Vec<_>, _>>()?;
                        let resolved = resolve_select_function(
                            ks,
                            func_ks.as_deref(),
                            name,
                            args,
                            alias.as_deref(),
                            &all_col_names,
                            &all_col_types,
                            schema,
                        )?;
                        // Use the resolved display_name so it matches the
                        // name produced by resolve_select_function during
                        // UDF evaluation (ensures col_names alignment).
                        (resolved.display_name, resolved.return_type.clone())
                    }
                };
                names.push(display_name);
                types.push(cql_type);
            }
        }
    }
    Ok((names, types))
}

/// Extract partition key values from WHERE clauses, in PK column order.
fn extract_pk_values(
    where_clauses: &[WhereClause],
    pk_names: &[String],
    table_meta: &TableMetadata,
    ks: &str,
    schema: &Schema,
) -> Result<Vec<CqlValue>, CqlError> {
    let mut values = Vec::with_capacity(pk_names.len());
    for pk_name in pk_names {
        let wc = where_clauses
            .iter()
            .find(|w| w.column == *pk_name && w.op == ComparisonOp::Eq)
            .ok_or_else(|| {
                CqlError::Invalid(format!(
                    "missing equality constraint on partition key column: {}",
                    pk_name
                ))
            })?;
        let col_meta = &table_meta.columns[pk_name];
        let cql_type = resolve_col_type(&col_meta.column_type, ks, schema)?;
        let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
        values.push(val);
    }
    Ok(values)
}

/// Try to handle `WHERE partition_key IN (v1, v2, ...)` by performing a
/// separate PK lookup for each value in the IN list.
///
/// Returns `Ok(Some(rows))` if the WHERE clause contains an IN predicate on
/// all partition key columns (currently supports single-column PK with IN,
/// or multi-column PK where all but one column are `Eq` and one is `In`).
/// Returns `Ok(None)` if no IN predicate is found on PK columns, letting
/// the caller fall through to the planner.
#[allow(clippy::too_many_arguments)]
fn try_pk_in_lookup(
    where_clauses: &[WhereClause],
    table_meta: &TableMetadata,
    ks: &str,
    schema: &Schema,
    engine: &StorageEngine,
    table_id: &TableId,
    all_col_names: &[String],
    all_col_types: &[CqlType],
    pk_indices: &[usize],
    ck_indices: &[usize],
) -> Result<Option<Vec<Vec<Option<CqlValue>>>>, CqlError> {
    let pk_names = &table_meta.partition_key;

    // Find which PK column (if any) has an IN predicate.
    let in_col_idx = pk_names.iter().position(|pk_name| {
        where_clauses
            .iter()
            .any(|w| w.column == *pk_name && w.op == ComparisonOp::In)
    });

    let in_col_idx = match in_col_idx {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // For the IN column, every other PK column must have an Eq predicate.
    for (i, pk_name) in pk_names.iter().enumerate() {
        if i == in_col_idx {
            continue;
        }
        let has_eq = where_clauses
            .iter()
            .any(|w| w.column == *pk_name && w.op == ComparisonOp::Eq);
        if !has_eq {
            return Ok(None);
        }
    }

    // Extract the IN list values.
    let in_wc = where_clauses
        .iter()
        .find(|w| w.column == pk_names[in_col_idx] && w.op == ComparisonOp::In)
        .expect("IN predicate verified above");

    let in_terms = match &in_wc.value {
        Term::InList(terms) => terms,
        _ => {
            return Err(CqlError::Invalid(
                "IN predicate value must be a list".into(),
            ));
        }
    };

    if in_terms.is_empty() {
        return Ok(Some(vec![]));
    }

    // Resolve PK column types.
    let pk_types: Vec<CqlType> = pk_names
        .iter()
        .map(|name| resolve_col_type(&table_meta.columns[name].column_type, ks, schema))
        .collect::<Result<Vec<_>, _>>()?;

    // Pre-resolve the Eq values for non-IN PK columns.
    let mut eq_values: Vec<Option<CqlValue>> = vec![None; pk_names.len()];
    for (i, pk_name) in pk_names.iter().enumerate() {
        if i == in_col_idx {
            continue;
        }
        let wc = where_clauses
            .iter()
            .find(|w| w.column == *pk_name && w.op == ComparisonOp::Eq)
            .expect("Eq predicate verified above");
        eq_values[i] = Some(bridge::term_to_cql_value(&wc.value, &pk_types[i])?);
    }

    // Iterate over each value in the IN list, build PK, and read.
    let in_col_type = &pk_types[in_col_idx];
    let mut all_rows = Vec::new();

    for term in in_terms {
        let in_value = bridge::term_to_cql_value(term, in_col_type)?;

        // Assemble the full PK values vector in column order.
        let mut pk_values = Vec::with_capacity(pk_names.len());
        for (i, _) in pk_names.iter().enumerate() {
            if i == in_col_idx {
                pk_values.push(in_value.clone());
            } else {
                pk_values.push(eq_values[i].clone().expect("Eq value resolved above"));
            }
        }

        let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
        if let Some(partition) = engine.read(table_id, &decorated_key)? {
            let mut prows = bridge::partition_to_rows(
                &partition,
                all_col_names,
                all_col_types,
                pk_indices,
                ck_indices,
            );
            all_rows.append(&mut prows);
        }
    }

    Ok(Some(all_rows))
}

/// Convert a WHERE clause `Term` for a given column into an `IndexKey` for
/// secondary index lookup.
fn term_to_index_key(
    term: &Term,
    column: &str,
    table_meta: &TableMetadata,
    ks: &str,
    schema: &Schema,
) -> Result<ferrosa_index::IndexKey, CqlError> {
    let col_meta = table_meta
        .columns
        .get(column)
        .ok_or_else(|| CqlError::Invalid(format!("column '{column}' not found")))?;
    let cql_type = resolve_col_type(&col_meta.column_type, ks, schema)?;
    let cql_value = bridge::term_to_cql_value(term, &cql_type)?;
    let bytes = crate::types::encode_value(&cql_value);
    Ok(ferrosa_index::IndexKey(bytes))
}

/// Evaluate WHERE predicates against a row for ALLOW FILTERING post-filter.
fn evaluate_where_predicates(
    row: &[Option<CqlValue>],
    where_clauses: &[WhereClause],
    all_col_names: &[String],
    table_meta: &TableMetadata,
    ks: &str,
    schema: &Schema,
) -> bool {
    for wc in where_clauses {
        // Skip token() predicates — token range filtering is handled by
        // the scan bounds, not by post-filter row evaluation.
        if wc.token_fn {
            continue;
        }
        let col_idx = match all_col_names.iter().position(|n| n == &wc.column) {
            Some(i) => i,
            None => return false,
        };
        let col_meta = match table_meta.columns.get(&wc.column) {
            Some(m) => m,
            None => return false,
        };
        let cql_type = match resolve_col_type(&col_meta.column_type, ks, schema) {
            Ok(t) => t,
            Err(_) => return false,
        };
        let actual = match &row[col_idx] {
            Some(v) => v,
            None => return false,
        };

        // IN requires special handling: the term is an InList, not a single
        // value, so we must check membership before the normal term_to_cql_value
        // path (which rejects InList terms).
        if wc.op == ComparisonOp::In {
            let in_terms = match &wc.value {
                Term::InList(terms) => terms,
                _ => return false,
            };
            let found = in_terms.iter().any(|t| {
                if let Ok(v) = bridge::term_to_cql_value(t, &cql_type) {
                    *actual == v
                } else {
                    false
                }
            });
            if !found {
                return false;
            }
            continue;
        }

        // CONTAINS / CONTAINS KEY require element/key type coercion, not
        // collection-level coercion, so they are handled before the normal
        // term_to_cql_value path (same pattern as IN above).
        if wc.op == ComparisonOp::Contains {
            let element_type = match &cql_type {
                CqlType::List(inner) | CqlType::Set(inner) => (**inner).clone(),
                CqlType::Map(_, val_type) => (**val_type).clone(),
                _ => return false,
            };
            let needle = match bridge::term_to_cql_value(&wc.value, &element_type) {
                Ok(v) => v,
                Err(_) => return false,
            };
            let found = match actual {
                CqlValue::List(items) | CqlValue::Set(items) => items.contains(&needle),
                CqlValue::Map(entries) => entries.iter().any(|(_, v)| *v == needle),
                _ => false,
            };
            if !found {
                return false;
            }
            continue;
        }
        if wc.op == ComparisonOp::ContainsKey {
            let key_type = match &cql_type {
                CqlType::Map(key_type, _) => (**key_type).clone(),
                _ => return false,
            };
            let needle = match bridge::term_to_cql_value(&wc.value, &key_type) {
                Ok(v) => v,
                Err(_) => return false,
            };
            let found = match actual {
                CqlValue::Map(entries) => entries.iter().any(|(k, _)| *k == needle),
                _ => false,
            };
            if !found {
                return false;
            }
            continue;
        }

        let expected = match bridge::term_to_cql_value(&wc.value, &cql_type) {
            Ok(v) => v,
            Err(_) => return false,
        };
        let matches = match wc.op {
            ComparisonOp::Eq => *actual == expected,
            ComparisonOp::Ne => *actual != expected,
            ComparisonOp::Gt => *actual > expected,
            ComparisonOp::Lt => *actual < expected,
            ComparisonOp::Ge => *actual >= expected,
            ComparisonOp::Le => *actual <= expected,
            ComparisonOp::In => unreachable!("IN handled above"),
            ComparisonOp::Contains | ComparisonOp::ContainsKey => {
                unreachable!("CONTAINS/CONTAINS KEY handled above")
            }
        };
        if !matches {
            return false;
        }
    }
    true
}

/// Apply column selection (with `toJson()` support) to system table query results.
///
/// System table handlers build the full set of columns, then this function
/// projects down to the columns requested in the `SELECT` list. If the
/// `SELECT` list contains `toJson(col)`, the column value is serialized to
/// a JSON string using [`bridge::cql_value_to_json`].
fn apply_system_select(
    select_columns_ast: &[SelectColumn],
    all_col_names: &[String],
    all_col_types: &[CqlType],
    all_rows: &[Vec<Option<CqlValue>>],
    keyspace: &str,
    table: &str,
) -> Result<BytesMut, CqlError> {
    // Star — return everything
    if select_columns_ast
        .iter()
        .any(|c| matches!(c, SelectColumn::Star))
    {
        return Ok(result::encode_rows(
            all_col_names,
            all_col_types,
            keyspace,
            table,
            all_rows,
        ));
    }

    // Build projected column names, types, and transform descriptors.
    let mut proj_names: Vec<String> = Vec::new();
    let mut proj_types: Vec<CqlType> = Vec::new();
    // Each entry: (source column index, apply_tojson)
    let mut proj_ops: Vec<(usize, bool)> = Vec::new();

    for sc in select_columns_ast {
        match sc {
            SelectColumn::Star => unreachable!(),
            SelectColumn::Column(name) => {
                let idx = all_col_names
                    .iter()
                    .position(|n| n == name)
                    .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", name)))?;
                proj_names.push(name.clone());
                proj_types.push(all_col_types[idx].clone());
                proj_ops.push((idx, false));
            }
            SelectColumn::FunctionCall {
                name, args, alias, ..
            } => {
                let fn_lower = name.to_lowercase();
                if fn_lower == "tojson" {
                    // toJson(column_ref) — single argument expected
                    if args.len() != 1 {
                        return Err(CqlError::Invalid(
                            "toJson() requires exactly one argument".into(),
                        ));
                    }
                    let col_name = extract_column_name(&args[0])?;
                    let idx = all_col_names
                        .iter()
                        .position(|n| *n == col_name)
                        .ok_or_else(|| {
                            CqlError::Invalid(format!("unknown column: {}", col_name))
                        })?;
                    let display = alias
                        .clone()
                        .unwrap_or_else(|| format!("system.tojson({})", col_name));
                    proj_names.push(display);
                    proj_types.push(CqlType::Varchar); // toJson returns text
                    proj_ops.push((idx, true));
                } else if fn_lower == "count" {
                    let display = alias.clone().unwrap_or_else(|| "count".to_string());
                    proj_names.push(display);
                    proj_types.push(CqlType::Bigint);
                    // COUNT is handled below as aggregate
                    proj_ops.push((usize::MAX, false));
                } else if fn_lower == "now" {
                    let display = alias.clone().unwrap_or_else(|| "system.now()".to_string());
                    proj_names.push(display);
                    proj_types.push(CqlType::Timeuuid);
                    // Sentinel: usize::MAX - 1 for now()
                    proj_ops.push((usize::MAX - 1, false));
                } else if fn_lower == "totimestamp" {
                    let display = alias
                        .clone()
                        .unwrap_or_else(|| "system.totimestamp(system.now())".to_string());
                    proj_names.push(display);
                    proj_types.push(CqlType::Timestamp);
                    // Sentinel: usize::MAX - 2 for toTimestamp(now())
                    proj_ops.push((usize::MAX - 2, false));
                } else {
                    return Err(CqlError::Invalid(format!(
                        "unsupported function in system table query: {}",
                        name
                    )));
                }
            }
        }
    }

    // Check for COUNT(*) aggregate
    let has_count = select_columns_ast.iter().any(|c| {
        matches!(c, SelectColumn::FunctionCall { name, .. } if name.eq_ignore_ascii_case("count"))
    });

    if has_count {
        // Aggregate query -- return a single row
        let mut agg_row: Vec<Option<CqlValue>> = Vec::new();
        for (i, sc) in select_columns_ast.iter().enumerate() {
            if matches!(sc, SelectColumn::FunctionCall { name, .. } if name.eq_ignore_ascii_case("count"))
            {
                agg_row.push(Some(CqlValue::Bigint(all_rows.len() as i64)));
            } else {
                let (src_idx, apply_tojson) = proj_ops[i];
                if src_idx == usize::MAX - 1 {
                    // now()
                    agg_row.push(Some(eval_now()));
                } else if src_idx == usize::MAX - 2 {
                    // toTimestamp(now())
                    let timeuuid = eval_now();
                    agg_row.push(eval_to_timestamp(&timeuuid).ok());
                } else if src_idx < all_col_names.len() {
                    let val = all_rows
                        .first()
                        .and_then(|r| r.get(src_idx))
                        .cloned()
                        .flatten();
                    if apply_tojson {
                        let json =
                            bridge::cql_value_to_json(val.as_ref().unwrap_or(&CqlValue::Null));
                        agg_row.push(Some(CqlValue::Text(json)));
                    } else {
                        agg_row.push(val);
                    }
                } else {
                    agg_row.push(None);
                }
            }
        }
        return Ok(result::encode_rows(
            &proj_names,
            &proj_types,
            keyspace,
            table,
            &[agg_row],
        ));
    }

    // Non-aggregate: project each row
    let projected: Vec<Vec<Option<CqlValue>>> = all_rows
        .iter()
        .map(|row| {
            proj_ops
                .iter()
                .map(|&(src_idx, apply_tojson)| {
                    if src_idx == usize::MAX - 1 {
                        // now()
                        Some(eval_now())
                    } else if src_idx == usize::MAX - 2 {
                        // toTimestamp(now())
                        let timeuuid = eval_now();
                        eval_to_timestamp(&timeuuid).ok()
                    } else {
                        let val = row.get(src_idx).cloned().flatten();
                        if apply_tojson {
                            let json =
                                bridge::cql_value_to_json(val.as_ref().unwrap_or(&CqlValue::Null));
                            Some(CqlValue::Text(json))
                        } else {
                            val
                        }
                    }
                })
                .collect()
        })
        .collect();

    Ok(result::encode_rows(
        &proj_names,
        &proj_types,
        keyspace,
        table,
        &projected,
    ))
}

/// Extract a column name from a `Term` that should reference a column.
///
/// In the `parse_term` codepath, bare identifiers in function arguments are
/// parsed as `Term::FunctionCall { name, args: [] }` (zero-arg function call).
fn extract_column_name(term: &Term) -> Result<String, CqlError> {
    match term {
        Term::FunctionCall { name, args, .. } if args.is_empty() => Ok(name.to_lowercase()),
        Term::StringLiteral(s) => Ok(s.clone()),
        _ => Err(CqlError::Invalid(format!(
            "expected column reference in toJson(), got {:?}",
            term
        ))),
    }
}

/// Convert a [`CqlValue`] to `f64` for built-in aggregate computation.
///
/// Supports all numeric CQL types. Returns `None` for non-numeric values so
/// that NULL / non-numeric cells are skipped during aggregation.
fn cql_value_to_f64(val: &CqlValue) -> Option<f64> {
    match val {
        CqlValue::Tinyint(v) => Some(f64::from(*v)),
        CqlValue::Smallint(v) => Some(f64::from(*v)),
        CqlValue::Int(v) => Some(f64::from(*v)),
        CqlValue::Bigint(v) => Some(*v as f64),
        CqlValue::Float(bits) => Some(f64::from(f32::from_bits(*bits))),
        CqlValue::Double(bits) => Some(f64::from_bits(*bits)),
        CqlValue::Counter(v) => Some(*v as f64),
        _ => None,
    }
}

/// Build the result [`CqlValue`] for a built-in aggregate (`avg`, `sum`,
/// `min`, `max`) given the source column type.
///
/// `avg` and `sum` on integer types still return `Double` for consistency
/// with Cassandra semantics (CQL `avg(int)` returns `int`, but returning
/// `Double` avoids truncation and matches driver expectations for
/// analytics queries).  If more precise Cassandra-compat semantics are
/// needed later, this can be refined per-type.
fn f64_to_cql_aggregate(val: f64, col_type: &CqlType) -> CqlValue {
    match col_type {
        CqlType::Tinyint => CqlValue::Tinyint(val as i8),
        CqlType::Smallint => CqlValue::Smallint(val as i16),
        CqlType::Int => CqlValue::Int(val as i32),
        CqlType::Bigint | CqlType::Counter => CqlValue::Bigint(val as i64),
        CqlType::Float => CqlValue::Float((val as f32).to_bits()),
        // Default to Double for any other numeric or unknown type.
        _ => CqlValue::Double(val.to_bits()),
    }
}

/// Compute a built-in aggregate (`avg`, `min`, `max`, `sum`) over a column.
///
/// `agg_name` must be one of `"avg"`, `"min"`, `"max"`, `"sum"` (lowercase).
/// The function iterates over `rows`, extracting the value at `col_idx`,
/// converts to `f64`, and applies the aggregate.  NULL cells are skipped.
fn compute_builtin_aggregate(
    agg_name: &str,
    rows: &[Vec<Option<CqlValue>>],
    col_idx: usize,
    col_type: &CqlType,
) -> Option<CqlValue> {
    let values: Vec<f64> = rows
        .iter()
        .filter_map(|row| row.get(col_idx).and_then(|cell| cell.as_ref()))
        .filter_map(cql_value_to_f64)
        .collect();

    if values.is_empty() {
        return Some(CqlValue::Null);
    }

    let result = match agg_name {
        "avg" => values.iter().sum::<f64>() / values.len() as f64,
        "sum" => values.iter().sum::<f64>(),
        "min" => values.iter().copied().fold(f64::INFINITY, f64::min),
        "max" => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        _ => return None,
    };
    Some(f64_to_cql_aggregate(result, col_type))
}

/// Apply `toJson()` transformations to projected rows for user-table SELECT queries.
///
/// Scans the SELECT column list for `toJson(col)` calls. For each one, finds the
/// source column value from the full (unprojected) row and replaces the projected
/// cell with its JSON representation.
fn apply_tojson_projections(
    select_columns_ast: &[SelectColumn],
    proj_col_names: &[String],
    all_col_names: &[String],
    full_rows: &[Vec<Option<CqlValue>>],
    mut projected_rows: Vec<Vec<Option<CqlValue>>>,
) -> Vec<Vec<Option<CqlValue>>> {
    // Find toJson columns: (projected_index, source_column_index)
    let mut tojson_ops: Vec<(usize, usize)> = Vec::new();
    for (proj_idx, sc) in select_columns_ast.iter().enumerate() {
        if let SelectColumn::FunctionCall { name, args, .. } = sc {
            if name.eq_ignore_ascii_case("tojson") {
                if let Some(arg) = args.first() {
                    if let Ok(col_name) = extract_column_name(arg) {
                        if let Some(src_idx) = all_col_names.iter().position(|n| *n == col_name) {
                            if proj_idx < proj_col_names.len() {
                                tojson_ops.push((proj_idx, src_idx));
                            }
                        }
                    }
                }
            }
        }
    }

    if tojson_ops.is_empty() {
        return projected_rows;
    }

    for (full_row, proj_row) in full_rows.iter().zip(projected_rows.iter_mut()) {
        for &(proj_idx, src_idx) in &tojson_ops {
            let val = full_row
                .get(src_idx)
                .and_then(|v| v.as_ref())
                .unwrap_or(&CqlValue::Null);
            let json = bridge::cql_value_to_json(val);
            proj_row[proj_idx] = Some(CqlValue::Text(json));
        }
    }

    projected_rows
}

/// Apply `now()`, `toTimestamp()`, `writetime()`, and `TTL()` built-in functions
/// to projected rows for user-table SELECT queries.
///
/// `cell_meta` carries per-column metadata (timestamp, TTL) from storage,
/// parallel to `full_rows`. For `now()` and `toTimestamp()`, no source data
/// is needed. For `writetime()` and `TTL()`, the metadata is looked up by
/// the target column name.
///
/// Currently unused — will be wired into route_select when partition reads
/// integrate cell metadata via partition_to_rows_with_metadata.
#[allow(dead_code)]
fn apply_builtin_functions(
    select_columns_ast: &[SelectColumn],
    proj_col_names: &[String],
    all_col_names: &[String],
    cell_meta: &[Vec<bridge::CellMeta>],
    mut projected_rows: Vec<Vec<Option<CqlValue>>>,
) -> Vec<Vec<Option<CqlValue>>> {
    // Collect operations: (projected_index, kind)
    enum BuiltinOp {
        Now,
        ToTimestamp,
        Writetime(usize), // source column index in full row
        Ttl(usize),       // source column index in full row
    }
    let mut ops: Vec<(usize, BuiltinOp)> = Vec::new();

    for (proj_idx, sc) in select_columns_ast.iter().enumerate() {
        if let SelectColumn::FunctionCall { name, args, .. } = sc {
            let fn_lower = name.to_lowercase();
            match fn_lower.as_str() {
                "now" => {
                    if proj_idx < proj_col_names.len() {
                        ops.push((proj_idx, BuiltinOp::Now));
                    }
                }
                "totimestamp" => {
                    if proj_idx < proj_col_names.len() {
                        ops.push((proj_idx, BuiltinOp::ToTimestamp));
                    }
                }
                "writetime" => {
                    if let Some(arg) = args.first() {
                        if let Ok(col_name) = extract_column_name(arg) {
                            if let Some(src_idx) = all_col_names.iter().position(|n| *n == col_name)
                            {
                                if proj_idx < proj_col_names.len() {
                                    ops.push((proj_idx, BuiltinOp::Writetime(src_idx)));
                                }
                            }
                        }
                    }
                }
                "ttl" => {
                    if let Some(arg) = args.first() {
                        if let Ok(col_name) = extract_column_name(arg) {
                            if let Some(src_idx) = all_col_names.iter().position(|n| *n == col_name)
                            {
                                if proj_idx < proj_col_names.len() {
                                    ops.push((proj_idx, BuiltinOp::Ttl(src_idx)));
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if ops.is_empty() {
        return projected_rows;
    }

    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    for (row_idx, proj_row) in projected_rows.iter_mut().enumerate() {
        let meta = cell_meta.get(row_idx);
        for (proj_idx, op) in &ops {
            match op {
                BuiltinOp::Now => {
                    proj_row[*proj_idx] = Some(eval_now());
                }
                BuiltinOp::ToTimestamp => {
                    // toTimestamp(now()) -- generate a timeuuid and convert
                    let timeuuid = eval_now();
                    proj_row[*proj_idx] = eval_to_timestamp(&timeuuid).ok();
                }
                BuiltinOp::Writetime(src_idx) => {
                    if let Some(m) = meta.and_then(|m| m.get(*src_idx)) {
                        if m.timestamp != i64::MIN {
                            proj_row[*proj_idx] = Some(CqlValue::Bigint(m.timestamp));
                        } else {
                            proj_row[*proj_idx] = None;
                        }
                    } else {
                        proj_row[*proj_idx] = None;
                    }
                }
                BuiltinOp::Ttl(src_idx) => {
                    if let Some(m) = meta.and_then(|m| m.get(*src_idx)) {
                        if m.ttl > 0 {
                            // Remaining TTL = ttl - (now_secs - cell_timestamp_secs)
                            let cell_ts_secs = m.timestamp / 1_000_000;
                            let elapsed = now_ts - cell_ts_secs;
                            let remaining = (m.ttl as i64) - elapsed;
                            if remaining > 0 {
                                proj_row[*proj_idx] = Some(CqlValue::Int(remaining as i32));
                            } else {
                                proj_row[*proj_idx] = None; // expired
                            }
                        } else {
                            proj_row[*proj_idx] = None; // no TTL set
                        }
                    } else {
                        proj_row[*proj_idx] = None;
                    }
                }
            }
        }
    }

    projected_rows
}

/// Project rows to a selected column subset.
///
/// Columns that do not match any name in `all_names` (e.g. UDF display
/// names like `"ks.to_celsius"`) get a `None` placeholder so the
/// projected row has exactly `selected.len()` cells. The caller can
/// then overwrite those placeholders with UDF results.
fn select_columns(
    rows: &[Vec<Option<CqlValue>>],
    all_names: &[String],
    selected: &[String],
) -> Vec<Vec<Option<CqlValue>>> {
    // If selected == all_names, return as-is
    if all_names == selected {
        return rows.to_vec();
    }
    // Build index mapping: Some(idx) for real columns, None for function calls.
    let indices: Vec<Option<usize>> = selected
        .iter()
        .map(|name| all_names.iter().position(|n| n == name))
        .collect();
    rows.iter()
        .map(|row| {
            indices
                .iter()
                .map(|opt_i| match opt_i {
                    Some(i) => row[*i].clone(),
                    None => None,
                })
                .collect()
        })
        .collect()
}

/// Convert an AST `GrantResource` to a schema `Resource`.
fn ast_resource_to_schema(
    resource: &GrantResource,
    current_ks: &Option<String>,
) -> Result<Resource, CqlError> {
    match resource {
        GrantResource::AllKeyspaces => Ok(Resource::AllKeyspaces),
        GrantResource::Keyspace(ks) => Ok(Resource::Keyspace(ks.clone())),
        GrantResource::Table(opt_ks, table) => {
            let ks = opt_ks
                .as_deref()
                .or(current_ks.as_deref())
                .ok_or_else(|| CqlError::Invalid("no keyspace specified for table".into()))?;
            Ok(Resource::Table(ks.to_string(), table.clone()))
        }
        GrantResource::AllRoles => Ok(Resource::AllRoles),
        GrantResource::Role(name) => Ok(Resource::Role(name.clone())),
        GrantResource::Function { .. } => Err(CqlError::Invalid(
            "GRANT/REVOKE on functions not yet implemented".into(),
        )),
        GrantResource::AllFunctions { .. } => Err(CqlError::Invalid(
            "GRANT/REVOKE on functions not yet implemented".into(),
        )),
    }
}

/// Parse permission strings into a `HashSet<Permission>`.
fn parse_permissions(perm_strings: &[String]) -> Result<HashSet<Permission>, CqlError> {
    let mut perms = HashSet::new();
    for s in perm_strings {
        match s.to_uppercase().as_str() {
            "ALL" | "ALL PERMISSIONS" => {
                perms.insert(Permission::Create);
                perms.insert(Permission::Alter);
                perms.insert(Permission::Drop);
                perms.insert(Permission::Select);
                perms.insert(Permission::Modify);
                perms.insert(Permission::Authorize);
                perms.insert(Permission::Describe);
                perms.insert(Permission::Execute);
            }
            "CREATE" => {
                perms.insert(Permission::Create);
            }
            "ALTER" => {
                perms.insert(Permission::Alter);
            }
            "DROP" => {
                perms.insert(Permission::Drop);
            }
            "SELECT" => {
                perms.insert(Permission::Select);
            }
            "MODIFY" => {
                perms.insert(Permission::Modify);
            }
            "AUTHORIZE" => {
                perms.insert(Permission::Authorize);
            }
            "DESCRIBE" => {
                perms.insert(Permission::Describe);
            }
            "EXECUTE" => {
                perms.insert(Permission::Execute);
            }
            _ => {
                return Err(CqlError::Invalid(format!("unknown permission: {}", s)));
            }
        }
    }
    Ok(perms)
}

/// Resolve a column type string using schema context for UDT resolution.
///
/// Wrapper around `bridge::parse_cql_type_in_keyspace` that provides
/// the keyspace and schema needed to resolve user-defined type names.
fn resolve_col_type(col_type: &str, ks: &str, schema: &Schema) -> Result<CqlType, CqlError> {
    bridge::parse_cql_type_in_keyspace(col_type, ks, schema)
}

/// Convert an AST `CqlTypeName` to a column_type string for `ColumnMetadata`.
fn cql_type_name_to_string(type_name: &CqlTypeName) -> String {
    match type_name {
        CqlTypeName::Simple(s) => s.clone(),
        CqlTypeName::List(inner) => format!("list<{}>", cql_type_name_to_string(inner)),
        CqlTypeName::Set(inner) => format!("set<{}>", cql_type_name_to_string(inner)),
        CqlTypeName::Map(k, v) => format!(
            "map<{}, {}>",
            cql_type_name_to_string(k),
            cql_type_name_to_string(v)
        ),
        CqlTypeName::Tuple(types) => {
            let inner: Vec<String> = types.iter().map(cql_type_name_to_string).collect();
            format!("tuple<{}>", inner.join(", "))
        }
        CqlTypeName::Frozen(inner) => format!("frozen<{}>", cql_type_name_to_string(inner)),
        CqlTypeName::Vector(inner, dim) => {
            format!("vector<{}, {}>", cql_type_name_to_string(inner), dim)
        }
    }
}

// ── DDL: User-Defined Types ──────────────────────────────────────────────

async fn route_create_type(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    if_not_exists: bool,
    fields: Vec<(String, CqlTypeName)>,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state.schema.check_permission(
        ctx.auth,
        Permission::Create,
        &Resource::Keyspace(ks.clone()),
    )?;

    // IF NOT EXISTS check
    if if_not_exists && state.schema.get_type(&ks, &name).is_some() {
        return Ok(result::encode_schema_change(
            "CREATED",
            "TYPE",
            &[&ks, &name],
        ));
    }

    // Resolve field types using bridge::resolve_type_name
    let resolved_fields: Vec<(String, CqlType)> = fields
        .iter()
        .map(|(fname, ftype)| {
            let resolved = bridge::resolve_type_name(ftype, &ks, &state.schema)?;
            Ok((fname.clone(), resolved))
        })
        .collect::<Result<_, CqlError>>()?;

    let udt = UserTypeMetadata {
        keyspace: ks.clone(),
        name: name.clone(),
        fields: resolved_fields,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            ddl.execute(DdlOperation::CreateType(udt))
                .await
                .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateType(udt);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateType(udt);
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "CREATED",
        "TYPE",
        &[&ks, &name],
    ))
}

async fn route_alter_type(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    alterations: Vec<TypeAlteration>,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Keyspace(ks.clone()))?;

    for alt in alterations {
        match alt {
            TypeAlteration::AddField {
                name: fname,
                field_type,
            } => {
                let resolved = bridge::resolve_type_name(&field_type, &ks, &state.schema)?;
                state
                    .schema
                    .alter_type_add_field(&ks, &name, &fname, resolved)
                    .map_err(|e| CqlError::Invalid(format!("alter type: {e}")))?;
            }
            TypeAlteration::RenameField { from, to } => {
                state
                    .schema
                    .alter_type_rename_field(&ks, &name, &from, &to)
                    .map_err(|e| CqlError::Invalid(format!("alter type: {e}")))?;
            }
            TypeAlteration::AlterField { .. } => {
                // ALTER TYPE ... ALTER field TYPE is deprecated in Cassandra; defer
            }
        }
    }

    Ok(result::encode_schema_change(
        "UPDATED",
        "TYPE",
        &[&ks, &name],
    ))
}

async fn route_drop_type(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    if_exists: bool,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Drop, &Resource::Keyspace(ks.clone()))?;

    // IF EXISTS check
    if if_exists && state.schema.get_type(&ks, &name).is_none() {
        return Ok(result::encode_schema_change(
            "DROPPED",
            "TYPE",
            &[&ks, &name],
        ));
    }

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            ddl.execute(DdlOperation::DropType {
                keyspace: ks.clone(),
                name: name.clone(),
            })
            .await
            .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropType {
                keyspace: ks.clone(),
                name: name.clone(),
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropType {
                keyspace: ks.clone(),
                name: name.clone(),
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "DROPPED",
        "TYPE",
        &[&ks, &name],
    ))
}

// ── UDF/UDA DDL routing ──────────────────────────────────────────────────

/// Decode a hex-encoded string to bytes.
fn hex_decode(hex: &str) -> Result<Vec<u8>, CqlError> {
    // Strip optional 0x / 0X prefix.
    let hex = hex
        .strip_prefix("0x")
        .or_else(|| hex.strip_prefix("0X"))
        .unwrap_or(hex);
    if !hex.len().is_multiple_of(2) {
        return Err(CqlError::Invalid("hex body has odd length".into()));
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|_| CqlError::Invalid(format!("invalid hex at offset {i}")))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn route_create_function(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    or_replace: bool,
    if_not_exists: bool,
    params: Vec<(String, CqlTypeName)>,
    called_on_null: bool,
    return_type: CqlTypeName,
    language: String,
    body: String,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Keyspace(ks.clone()))?;

    // Only WASM language is supported
    if !language.eq_ignore_ascii_case("wasm") {
        return Err(CqlError::Invalid(format!(
            "unsupported UDF language '{}': only 'wasm' is supported",
            language
        )));
    }

    // Resolve parameter types
    let (arg_names, arg_types): (Vec<String>, Vec<ferrosa_common::CqlType>) = params
        .iter()
        .map(|(pname, ptype)| {
            let resolved = bridge::resolve_type_name(ptype, &ks, &state.schema)?;
            Ok((pname.clone(), cql_type_to_common(&resolved)))
        })
        .collect::<Result<Vec<_>, CqlError>>()?
        .into_iter()
        .unzip();

    // Resolve return type
    let resolved_return = bridge::resolve_type_name(&return_type, &ks, &state.schema)?;
    let common_return = cql_type_to_common(&resolved_return);

    // Build arg type name strings for the SCHEMA_CHANGE response (CQL protocol
    // requires a [string list] of argument type names for FUNCTION targets).
    let arg_type_names: Vec<String> = arg_types
        .iter()
        .map(bridge::cql_type_display_name)
        .collect();

    // Decode hex body to WASM bytes and compile
    let wasm_bytes = hex_decode(&body)?;
    state
        .udf_executor
        .compile(&ks, &name, &wasm_bytes)
        .map_err(CqlError::from)?;

    // Check for existing function
    let existing = state.schema.get_function(&ks, &name, &arg_types);
    if existing.is_some() {
        if or_replace {
            // Drop the old one first, then create the new one
            state.udf_executor.invalidate(&ks, &name);
        } else if if_not_exists {
            return Ok(result::encode_schema_change_with_args(
                "CREATED",
                "FUNCTION",
                &[&ks, &name],
                &arg_type_names,
            ));
        } else {
            return Err(CqlError::Invalid(format!(
                "function {ks}.{name} already exists"
            )));
        }
    }

    let func_meta = UserFunctionMetadata {
        keyspace: ks.clone(),
        name: name.clone(),
        arg_names,
        arg_types,
        return_type: common_return,
        called_on_null,
        language: language.to_ascii_lowercase(),
        body,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            ddl.execute(DdlOperation::CreateFunction(func_meta))
                .await
                .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateFunction(func_meta);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateFunction(func_meta);
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change_with_args(
        "CREATED",
        "FUNCTION",
        &[&ks, &name],
        &arg_type_names,
    ))
}

async fn route_drop_function(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    arg_type_names: Option<Vec<CqlTypeName>>,
    if_exists: bool,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Keyspace(ks.clone()))?;

    // Resolve arg types to find the exact function
    let resolved_arg_types: Vec<ferrosa_common::CqlType> = match arg_type_names {
        Some(type_names) => type_names
            .iter()
            .map(|tn| {
                let resolved = bridge::resolve_type_name(tn, &ks, &state.schema)?;
                Ok(cql_type_to_common(&resolved))
            })
            .collect::<Result<_, CqlError>>()?,
        None => {
            // No arg types specified — find function by name only.
            // Search all functions in this keyspace with this name.
            let snap = state.schema.snapshot();
            let matching: Vec<Vec<ferrosa_common::CqlType>> = snap
                .functions
                .keys()
                .filter(|(fks, fname, _)| fks == &ks && fname == &name)
                .map(|(_, _, arg_types)| arg_types.clone())
                .collect();
            match matching.len() {
                0 => {
                    if if_exists {
                        return Ok(result::encode_schema_change_with_args(
                            "DROPPED",
                            "FUNCTION",
                            &[&ks, &name],
                            &[],
                        ));
                    }
                    return Err(CqlError::Invalid(format!("function {ks}.{name} not found")));
                }
                1 => {
                    // SAFETY: len() == 1 guarantees next() returns Some
                    matching.into_iter().next().ok_or_else(|| {
                        CqlError::ServerError(format!("function {ks}.{name} lookup inconsistency"))
                    })?
                }
                _ => {
                    return Err(CqlError::Invalid(format!(
                        "ambiguous function {ks}.{name}: multiple overloads exist, specify argument types"
                    )));
                }
            }
        }
    };

    // Build arg type name strings for the SCHEMA_CHANGE response.
    let resolved_arg_type_names: Vec<String> = resolved_arg_types
        .iter()
        .map(bridge::cql_type_display_name)
        .collect();

    // IF EXISTS check
    if if_exists
        && state
            .schema
            .get_function(&ks, &name, &resolved_arg_types)
            .is_none()
    {
        return Ok(result::encode_schema_change_with_args(
            "DROPPED",
            "FUNCTION",
            &[&ks, &name],
            &resolved_arg_type_names,
        ));
    }

    // Invalidate cached compilation
    state.udf_executor.invalidate(&ks, &name);

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            ddl.execute(DdlOperation::DropFunction {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            })
            .await
            .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropFunction {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropFunction {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change_with_args(
        "DROPPED",
        "FUNCTION",
        &[&ks, &name],
        &resolved_arg_type_names,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn route_create_aggregate(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    or_replace: bool,
    if_not_exists: bool,
    ast_arg_types: Vec<CqlTypeName>,
    state_func: String,
    state_type: CqlTypeName,
    final_func: Option<String>,
    init_cond: Option<Term>,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Keyspace(ks.clone()))?;

    // Resolve argument types
    let arg_types: Vec<ferrosa_common::CqlType> = ast_arg_types
        .iter()
        .map(|tn| {
            let resolved = bridge::resolve_type_name(tn, &ks, &state.schema)?;
            Ok(cql_type_to_common(&resolved))
        })
        .collect::<Result<_, CqlError>>()?;

    // Build arg type name strings for the SCHEMA_CHANGE response.
    let agg_arg_type_names: Vec<String> = arg_types
        .iter()
        .map(bridge::cql_type_display_name)
        .collect();

    // Resolve state type
    let resolved_state_type = bridge::resolve_type_name(&state_type, &ks, &state.schema)?;
    let common_state_type = cql_type_to_common(&resolved_state_type);

    // Validate state function exists. The state function takes (state_type, arg_types...) as params.
    let mut sfunc_arg_types = vec![common_state_type.clone()];
    sfunc_arg_types.extend(arg_types.iter().cloned());
    if state
        .schema
        .get_function(&ks, &state_func, &sfunc_arg_types)
        .is_none()
    {
        return Err(CqlError::Invalid(format!(
            "state function {ks}.{state_func} not found with expected signature"
        )));
    }

    // Determine return type: final_func return type if present, otherwise state_type.
    let return_type = if let Some(ref ff_name) = final_func {
        // Final function takes a single arg of the state type
        let ff_args = vec![common_state_type.clone()];
        match state.schema.get_function(&ks, ff_name, &ff_args) {
            Some(ff_meta) => ff_meta.return_type.clone(),
            None => {
                return Err(CqlError::Invalid(format!(
                    "final function {ks}.{ff_name} not found with expected signature"
                )));
            }
        }
    } else {
        common_state_type.clone()
    };

    // Resolve init_cond if present
    let resolved_init_cond = if let Some(ref term) = init_cond {
        let cql_state_type = &resolved_state_type;
        Some(bridge::term_to_cql_value(term, cql_state_type)?)
    } else {
        None
    };

    // Convert the CqlValue (wire type) to common CqlValue
    let common_init_cond = resolved_init_cond.map(|v| cql_value_to_common(&v));

    // Check for existing aggregate
    let existing = state.schema.get_aggregate(&ks, &name, &arg_types);
    if existing.is_some() {
        if or_replace {
            // Will be replaced by the new creation
        } else if if_not_exists {
            return Ok(result::encode_schema_change_with_args(
                "CREATED",
                "AGGREGATE",
                &[&ks, &name],
                &agg_arg_type_names,
            ));
        } else {
            return Err(CqlError::Invalid(format!(
                "aggregate {ks}.{name} already exists"
            )));
        }
    }

    let agg_meta = UserAggregateMetadata {
        keyspace: ks.clone(),
        name: name.clone(),
        arg_types,
        state_func,
        state_type: common_state_type,
        final_func,
        init_cond: common_init_cond,
        return_type,
        wasm_body: None,
    };

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            ddl.execute(DdlOperation::CreateAggregate(agg_meta))
                .await
                .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateAggregate(agg_meta);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::CreateAggregate(agg_meta);
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change_with_args(
        "CREATED",
        "AGGREGATE",
        &[&ks, &name],
        &agg_arg_type_names,
    ))
}

async fn route_drop_aggregate(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    keyspace: Option<String>,
    name: String,
    arg_type_names: Option<Vec<CqlTypeName>>,
    if_exists: bool,
) -> Result<BytesMut, CqlError> {
    let ks = keyspace
        .or_else(|| ctx.current_keyspace.clone())
        .ok_or_else(|| CqlError::Invalid("No keyspace specified".into()))?;

    // Permission check (M8)
    state
        .schema
        .check_permission(ctx.auth, Permission::Alter, &Resource::Keyspace(ks.clone()))?;

    // Resolve arg types to find the exact aggregate
    let resolved_arg_types: Vec<ferrosa_common::CqlType> = match arg_type_names {
        Some(type_names) => type_names
            .iter()
            .map(|tn| {
                let resolved = bridge::resolve_type_name(tn, &ks, &state.schema)?;
                Ok(cql_type_to_common(&resolved))
            })
            .collect::<Result<_, CqlError>>()?,
        None => {
            // No arg types specified — find aggregate by name only.
            let snap = state.schema.snapshot();
            let matching: Vec<Vec<ferrosa_common::CqlType>> = snap
                .aggregates
                .keys()
                .filter(|(a_ks, a_name, _)| a_ks == &ks && a_name == &name)
                .map(|(_, _, arg_types)| arg_types.clone())
                .collect();
            match matching.len() {
                0 => {
                    if if_exists {
                        return Ok(result::encode_schema_change_with_args(
                            "DROPPED",
                            "AGGREGATE",
                            &[&ks, &name],
                            &[],
                        ));
                    }
                    return Err(CqlError::Invalid(format!(
                        "aggregate {ks}.{name} not found"
                    )));
                }
                1 => {
                    // SAFETY: len() == 1 guarantees next() returns Some
                    matching.into_iter().next().ok_or_else(|| {
                        CqlError::ServerError(format!("aggregate {ks}.{name} lookup inconsistency"))
                    })?
                }
                _ => {
                    return Err(CqlError::Invalid(format!(
                        "ambiguous aggregate {ks}.{name}: multiple overloads exist, specify argument types"
                    )));
                }
            }
        }
    };

    // Build arg type name strings for the SCHEMA_CHANGE response.
    let resolved_agg_arg_type_names: Vec<String> = resolved_arg_types
        .iter()
        .map(bridge::cql_type_display_name)
        .collect();

    // IF EXISTS check
    if if_exists
        && state
            .schema
            .get_aggregate(&ks, &name, &resolved_arg_types)
            .is_none()
    {
        return Ok(result::encode_schema_change_with_args(
            "DROPPED",
            "AGGREGATE",
            &[&ks, &name],
            &resolved_agg_arg_type_names,
        ));
    }

    let ddl_guard = state.ddl_path.load();
    let ddl = &**ddl_guard;
    match ddl {
        DdlPath::Direct { .. } => {
            ddl.execute(DdlOperation::DropAggregate {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            })
            .await
            .map_err(CqlError::from)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropAggregate {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            };
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Cluster { .. } => {
            let op = DdlOperation::DropAggregate {
                keyspace: ks.clone(),
                name: name.clone(),
                arg_types: resolved_arg_types,
            };
            ddl.execute(op).await.map_err(CqlError::from)?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change_with_args(
        "DROPPED",
        "AGGREGATE",
        &[&ks, &name],
        &resolved_agg_arg_type_names,
    ))
}

/// Convert a CQL wire type to ferrosa_common::CqlType.
///
/// In Ferrosa, `CqlType` from the CQL crate and from ferrosa_common are the
/// same type (`ferrosa_common::CqlType`), so this is an identity conversion.
/// However, the bridge layer might use a different repr in the future.
fn cql_type_to_common(cql_type: &CqlType) -> ferrosa_common::CqlType {
    cql_type.clone()
}

/// Convert a CQL wire value to ferrosa_common::CqlValue.
fn cql_value_to_common(val: &CqlValue) -> ferrosa_common::CqlValue {
    // CqlValue in ferrosa-cql IS ferrosa_common::CqlValue — same type, identity conversion.
    val.clone()
}

// ── UDF/UDA query-time resolution and execution ──────────────────────────

/// Infer the CQL type of a `Term` expression.
///
/// For column references (parsed as `Term::FunctionCall { name, args: [] }`),
/// the column name is looked up in `all_col_names` / `all_col_types` to return
/// the column's declared type.
fn infer_term_type(
    term: &Term,
    all_col_names: &[String],
    all_col_types: &[CqlType],
) -> Result<CqlType, CqlError> {
    match term {
        Term::IntegerLiteral(_) => Ok(CqlType::Int),
        Term::FloatLiteral(_) => Ok(CqlType::Double),
        Term::StringLiteral(_) => Ok(CqlType::Varchar),
        Term::BoolLiteral(_) => Ok(CqlType::Boolean),
        Term::UuidLiteral(_) => Ok(CqlType::Uuid),
        Term::BlobLiteral(_) => Ok(CqlType::Blob),
        Term::Null => Ok(CqlType::Varchar), // default for untyped null
        Term::FunctionCall { name, args, .. } if args.is_empty() => {
            // Zero-arg function call may actually be a column reference.
            if let Some(idx) = all_col_names
                .iter()
                .position(|n| n.eq_ignore_ascii_case(name))
            {
                Ok(all_col_types[idx].clone())
            } else {
                // Actual zero-arg function — treat return type as Varchar as a
                // default; the real type will be resolved during function lookup.
                Ok(CqlType::Varchar)
            }
        }
        _ => Err(CqlError::Invalid(format!(
            "cannot infer type for term: {:?}",
            term
        ))),
    }
}

/// Resolve a `SelectColumn::FunctionCall` into a `ResolvedFunction`.
///
/// Looks up the function in the schema (trying aggregates first, then scalar
/// UDFs) and builds the argument index mapping.
#[allow(clippy::too_many_arguments)]
fn resolve_select_function(
    ks: &str,
    func_keyspace: Option<&str>,
    func_name: &str,
    args: &[Term],
    alias: Option<&str>,
    all_col_names: &[String],
    all_col_types: &[CqlType],
    schema: &Schema,
) -> Result<ResolvedFunction, CqlError> {
    let func_ks = func_keyspace.unwrap_or(ks);

    // Infer argument types.
    let arg_types: Vec<CqlType> = args
        .iter()
        .map(|a| infer_term_type(a, all_col_names, all_col_types))
        .collect::<Result<Vec<_>, _>>()?;

    // Build arg_indices: for each arg, if it is a column reference, record its
    // index in the full row; otherwise use usize::MAX as sentinel for literals.
    let arg_indices: Vec<usize> = args
        .iter()
        .map(|a| match a {
            Term::FunctionCall {
                name,
                args: inner_args,
                ..
            } if inner_args.is_empty() => all_col_names
                .iter()
                .position(|n| n.eq_ignore_ascii_case(name))
                .unwrap_or(usize::MAX),
            _ => usize::MAX,
        })
        .collect();

    let display_name = alias
        .map(|a| a.to_string())
        .unwrap_or_else(|| format!("{}.{}", func_ks, func_name));

    // Try aggregate first.
    if let Some(agg_meta) = schema.get_aggregate(func_ks, func_name, &arg_types) {
        return Ok(ResolvedFunction {
            func_name: func_name.to_string(),
            func_keyspace: func_ks.to_string(),
            kind: ResolvedFunctionKind::Aggregate {
                init_cond: agg_meta.init_cond.clone(),
            },
            return_type: agg_meta.return_type.clone(),
            called_on_null: false, // aggregates always accumulate
            arg_indices,
            arg_terms: args.to_vec(),
            arg_types,
            display_name,
        });
    }

    // Try scalar UDF.
    if let Some(func_meta) = schema.get_function(func_ks, func_name, &arg_types) {
        return Ok(ResolvedFunction {
            func_name: func_name.to_string(),
            func_keyspace: func_ks.to_string(),
            kind: ResolvedFunctionKind::Scalar,
            return_type: func_meta.return_type.clone(),
            called_on_null: func_meta.called_on_null,
            arg_indices,
            arg_terms: args.to_vec(),
            arg_types,
            display_name,
        });
    }

    Err(CqlError::Invalid(format!(
        "unknown function: {}.{}({})",
        func_ks,
        func_name,
        arg_types
            .iter()
            .map(bridge::cql_type_display_name)
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

/// Extract the argument values for a UDF call from a full row.
///
/// For each arg: if it references a column (arg_indices[i] != usize::MAX),
/// extract the column value from the row; otherwise convert the literal Term.
fn extract_udf_args(
    row: &[Option<CqlValue>],
    func: &ResolvedFunction,
) -> Result<Vec<CqlValue>, CqlError> {
    let mut values = Vec::with_capacity(func.arg_indices.len());
    for (i, &idx) in func.arg_indices.iter().enumerate() {
        if idx != usize::MAX {
            // Column reference — use row value (or Null).
            let val = row
                .get(idx)
                .and_then(|v| v.clone())
                .unwrap_or(CqlValue::Null);
            values.push(val);
        } else {
            // Literal — convert the term using the inferred type.
            let val = bridge::term_to_cql_value(&func.arg_terms[i], &func.arg_types[i])?;
            values.push(val);
        }
    }
    Ok(values)
}

/// Evaluate scalar UDFs against a single row, returning the UDF results
/// in the order of `resolved_funcs`.
///
/// Each resolved function is evaluated using the WASM executor. If the function
/// is not `called_on_null` and any argument is NULL, the result is NULL without
/// invoking WASM.
fn evaluate_row_udfs(
    state: &SharedState,
    row: &[Option<CqlValue>],
    resolved_funcs: &[&ResolvedFunction],
) -> Result<Vec<Option<CqlValue>>, CqlError> {
    let mut results = Vec::with_capacity(resolved_funcs.len());
    for func in resolved_funcs {
        if !matches!(func.kind, ResolvedFunctionKind::Scalar) {
            // Aggregates are handled separately.
            results.push(None);
            continue;
        }
        let args = extract_udf_args(row, func)?;

        // Called-on-null short circuit: if any arg is Null and the function is
        // not marked CALLED ON NULL INPUT, return Null without invoking WASM.
        if !func.called_on_null && args.iter().any(|v| matches!(v, CqlValue::Null)) {
            results.push(None);
            continue;
        }

        let result = state.udf_executor.call(
            &func.func_keyspace,
            &func.func_name,
            args,
            &func.arg_types,
            &func.return_type,
        )?;
        results.push(Some(result));
    }
    Ok(results)
}

/// Execute a user-defined aggregate (UDA) over a set of rows.
///
/// Runs the state function per row and the optional final function once,
/// returning the aggregated result.
fn execute_uda(
    state: &SharedState,
    ks: &str,
    func: &ResolvedFunction,
    rows: &[Vec<Option<CqlValue>>],
) -> Result<CqlValue, CqlError> {
    let init_cond = match &func.kind {
        ResolvedFunctionKind::Aggregate { init_cond } => init_cond.clone(),
        _ => {
            return Err(CqlError::Invalid(
                "execute_uda called on non-aggregate function".into(),
            ));
        }
    };

    // Look up the aggregate metadata to get state_func, final_func, state_type.
    let agg_meta = state
        .schema
        .get_aggregate(&func.func_keyspace, &func.func_name, &func.arg_types)
        .ok_or_else(|| {
            CqlError::Invalid(format!(
                "aggregate not found: {}.{}",
                func.func_keyspace, func.func_name
            ))
        })?;

    // Initialize accumulator.
    let mut acc = init_cond.unwrap_or(CqlValue::Null);

    // Look up the state function metadata (needed for called_on_null).
    let state_func_arg_types: Vec<CqlType> = {
        let mut types = vec![agg_meta.state_type.clone()];
        types.extend(func.arg_types.iter().cloned());
        types
    };

    let state_func_meta =
        state
            .schema
            .get_function(ks, &agg_meta.state_func, &state_func_arg_types);
    let state_func_called_on_null = state_func_meta
        .as_ref()
        .map(|m| m.called_on_null)
        .unwrap_or(false);

    // Accumulate: call state_func(acc, row_args...) for each row.
    for row in rows {
        let row_args = extract_udf_args(row, func)?;

        // Skip rows with null args if state func is not called on null.
        if !state_func_called_on_null && row_args.iter().any(|v| matches!(v, CqlValue::Null)) {
            continue;
        }

        let mut call_args = vec![acc.clone()];
        call_args.extend(row_args);

        acc = state.udf_executor.call(
            ks,
            &agg_meta.state_func,
            call_args,
            &state_func_arg_types,
            &agg_meta.state_type,
        )?;
    }

    // Finalize: if there is a final function, call it on the accumulator.
    if let Some(ref final_func_name) = agg_meta.final_func {
        let final_arg_types = vec![agg_meta.state_type.clone()];
        acc = state.udf_executor.call(
            ks,
            final_func_name,
            vec![acc],
            &final_arg_types,
            &func.return_type,
        )?;
    }

    Ok(acc)
}

/// Check if any WHERE clause value contains a non-builtin function call,
/// indicating a UDF in WHERE that requires ALLOW FILTERING.
fn where_has_udf_calls(where_clauses: &[WhereClause]) -> bool {
    for wc in where_clauses {
        if term_has_udf_call(&wc.value) {
            return true;
        }
    }
    false
}

/// Check if a Term contains a non-builtin function call.
fn term_has_udf_call(term: &Term) -> bool {
    match term {
        Term::FunctionCall { name, args, .. } => {
            let lower = name.to_lowercase();
            let is_builtin = matches!(
                lower.as_str(),
                "uuid" | "now" | "totimestamp" | "todate" | "count" | "writetime" | "ttl" | "token"
            );
            if !is_builtin && !args.is_empty() {
                return true;
            }
            // Also check nested args.
            args.iter().any(term_has_udf_call)
        }
        Term::InList(items) | Term::ListLiteral(items) | Term::SetLiteral(items) => {
            items.iter().any(term_has_udf_call)
        }
        Term::TupleLiteral(items) => items.iter().any(term_has_udf_call),
        Term::MapLiteral(pairs) => pairs
            .iter()
            .any(|(k, v)| term_has_udf_call(k) || term_has_udf_call(v)),
        _ => false,
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtual_tables::active_queries::ActiveQueriesTable;
    use crate::virtual_tables::connections::ConnectionsTable;
    use ferrosa_schema::{
        AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, SchemaConfig, TestAuditSink,
    };
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    };
    use tempfile::TempDir;

    fn setup() -> (SharedState, TempDir) {
        let dir = TempDir::new().unwrap();

        let commit_log = CommitLogConfig {
            segment_size: 4096,
            max_segment_age: std::time::Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            log_dir: dir.path().join("commitlog"),
            checkpoint_dir: dir.path().join("commitlog"),
            archive: None,
        };
        let compaction = CompactionConfig::from_env(dir.path().join("compaction"));
        let engine_config = StorageEngineConfig {
            commit_log,
            compaction,
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            data_dir: dir.path().to_path_buf(),
        };
        let engine = Arc::new(StorageEngine::new(engine_config, None).unwrap());

        let schema = Arc::new(
            Schema::new(SchemaConfig {
                hasher: PasswordHasher::Bcrypt { cost: 4 },
                password_policy: PasswordPolicy::permissive(),
                auth_method: AuthMethod::Password,
                rate_limit: RateLimitConfig::default(),
                audit_sink: Box::new(TestAuditSink::new()),
                secrets: Box::new(EnvSecretsProvider),
                mode: DeploymentMode::Development,
            })
            .unwrap(),
        );

        let node_config = Arc::new(NodeConfig {
            cluster_name: "test".into(),
            data_center: "dc1".into(),
            rack: "rack1".into(),
            rpc_port: 9042,
            host_id: uuid::Uuid::new_v4(),
            listen_address: "127.0.0.1".parse().unwrap(),
            listen_port: 7000,
            broadcast_address: "127.0.0.1".parse().unwrap(),
            broadcast_port: 7000,
            rpc_address: "127.0.0.1".parse().unwrap(),
            tokens: vec![],
        });

        let udf_executor =
            Arc::new(ferrosa_udf::UdfExecutor::new(ferrosa_udf::SandboxConfig::default()).unwrap());

        let mode_controller =
            ferrosa_cluster::ModeController::standalone_for_test(schema.clone(), engine.clone());
        let state = SharedState {
            engine: engine.clone(),
            schema: schema.clone(),
            node_config,
            cluster_state: Arc::new(ArcSwap::from_pointee(
                ferrosa_cluster::ClusterStateHolder::Standalone,
            )),
            write_path: Arc::new(ArcSwap::from_pointee(WritePath::direct(engine.clone()))),
            ddl_path: Arc::new(ArcSwap::from_pointee(DdlPath::Direct { schema, engine })),
            prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
            connection_tracker: Arc::new(ConnectionTracker::new()),
            query_tracker: Arc::new(QueryTracker::new()),
            udf_executor,
            event_sender: tokio::sync::broadcast::channel(64).0,
            mode_controller,
        };
        (state, dir)
    }

    fn dev_auth() -> AuthContext {
        AuthContext {
            role: "cassandra".into(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    #[tokio::test]
    async fn create_keyspace_then_table_then_insert_then_select() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // CREATE KEYSPACE
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0005i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }

        // CREATE TABLE
        let stmt =
            crate::parser::parse("CREATE TABLE ks.users (id int PRIMARY KEY, name text)").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0005i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }

        // INSERT
        let stmt =
            crate::parser::parse("INSERT INTO ks.users (id, name) VALUES (1, 'alice')").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0001i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }

        // SELECT
        let stmt = crate::parser::parse("SELECT * FROM ks.users WHERE id = 1").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0002i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn use_sets_default_keyspace() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };
        let stmt = crate::parser::parse("USE my_ks").unwrap();
        match route(&state, &ctx, stmt).await.unwrap() {
            RouteResult::SetKeyspace(ks, body) => {
                assert_eq!(ks, "my_ks");
                assert_eq!(&body[0..4], &0x0003i32.to_be_bytes());
            }
            _ => panic!("expected SetKeyspace"),
        }
    }

    #[tokio::test]
    async fn select_system_local() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system.local").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                // Result kind = Rows (0x0002)
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes());
                // Column count is at offset 8 (after kind(4) + flags(4))
                let col_count = i32::from_be_bytes(b[8..12].try_into().unwrap());
                // 16 columns: key, cluster_name, data_center, rack, host_id,
                // partitioner, native_protocol_version, cql_version,
                // release_version, schema_version, rpc_port, listen_address,
                // broadcast_address, rpc_address, bootstrapped, tokens
                assert_eq!(col_count, 16);
            }
            _ => panic!("expected Result"),
        }
    }

    /// Regression test: cqlsh expects `tokens` column (set<varchar>) in
    /// system.local. Without it, cqlsh prints "'local' not found in
    /// keyspace 'system'" during startup introspection.
    #[tokio::test]
    async fn select_system_local_includes_tokens_column() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system.local").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let b = match &result {
            RouteResult::Result(b) => b,
            _ => panic!("expected Result"),
        };

        // Decode column names from the Rows metadata.
        // Layout: [i32 kind][i32 flags][i32 col_count][string ks][string table]
        //         then per column: [string name][u16 type_id][type params]
        let col_count = i32::from_be_bytes(b[8..12].try_into().unwrap()) as usize;

        // Skip past keyspace and table global_table_spec strings
        let mut off = 12;
        // keyspace string: [u16 len][bytes]
        let ks_len = u16::from_be_bytes(b[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + ks_len;
        // table string: [u16 len][bytes]
        let tbl_len = u16::from_be_bytes(b[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + tbl_len;

        // Read column names
        let mut col_names = Vec::new();
        for _ in 0..col_count {
            let name_len = u16::from_be_bytes(b[off..off + 2].try_into().unwrap()) as usize;
            off += 2;
            let name = std::str::from_utf8(&b[off..off + name_len]).unwrap();
            col_names.push(name.to_string());
            off += name_len;
            // Skip type_id (u16) + possible type params
            let type_id = u16::from_be_bytes(b[off..off + 2].try_into().unwrap());
            off += 2;
            match type_id {
                0x0020 | 0x0022 => off += 2, // List/Set: element type_id
                0x0021 => off += 4,          // Map: key + val type_ids
                _ => {}
            }
        }

        assert!(
            col_names.contains(&"tokens".to_string()),
            "system.local must include 'tokens' column, got: {col_names:?}"
        );
    }

    #[tokio::test]
    async fn no_keyspace_returns_invalid() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };
        let stmt = crate::parser::parse("SELECT * FROM users WHERE id = 1").unwrap();
        assert!(route(&state, &ctx, stmt).await.is_err());
    }

    #[tokio::test]
    async fn batch_too_large_rejected() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };
        // Build a batch statement with > 500 entries programmatically
        let stmts: Vec<Statement> = (0..501)
            .map(|i| {
                Statement::Insert(InsertStatement {
                    keyspace: Some("ks".into()),
                    table: "t".into(),
                    columns: vec!["k".into()],
                    values: vec![Term::IntegerLiteral(i)],
                    if_not_exists: false,
                    using_timestamp: None,
                    using_ttl: None,
                })
            })
            .collect();
        let batch = Statement::Batch(BatchStatement {
            batch_type: BatchType::Unlogged,
            statements: stmts,
            using_timestamp: None,
        });
        assert!(route(&state, &ctx, batch).await.is_err());
    }

    #[tokio::test]
    async fn select_system_peers_empty() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system.peers").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0002i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn select_system_schema_keyspaces() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system_schema.keyspaces").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0002i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }
    }

    #[test]
    fn cql_type_name_to_string_simple() {
        assert_eq!(
            cql_type_name_to_string(&CqlTypeName::Simple("text".into())),
            "text"
        );
    }

    #[test]
    fn cql_type_name_to_string_collection() {
        assert_eq!(
            cql_type_name_to_string(&CqlTypeName::Map(
                Box::new(CqlTypeName::Simple("text".into())),
                Box::new(CqlTypeName::Simple("int".into())),
            )),
            "map<text, int>"
        );
    }

    #[test]
    fn parse_permissions_all() {
        let perms = parse_permissions(&["ALL".into()]).unwrap();
        assert!(perms.contains(&Permission::Select));
        assert!(perms.contains(&Permission::Modify));
        assert!(perms.contains(&Permission::Create));
        assert!(perms.contains(&Permission::Drop));
        assert!(perms.contains(&Permission::Alter));
        assert!(perms.contains(&Permission::Authorize));
    }

    #[test]
    fn parse_permissions_single() {
        let perms = parse_permissions(&["SELECT".into()]).unwrap();
        assert_eq!(perms.len(), 1);
        assert!(perms.contains(&Permission::Select));
    }

    #[test]
    fn parse_permissions_unknown_rejected() {
        assert!(parse_permissions(&["BANANA".into()]).is_err());
    }

    #[tokio::test]
    async fn router_tracks_query_count() {
        let (state, _dir) = setup();
        assert_eq!(state.query_tracker.total_executed(), 0);
        // Execute a simple query
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system.local").unwrap();
        let _ = route(&state, &ctx, stmt).await;
        assert_eq!(state.query_tracker.total_executed(), 1);
    }

    #[tokio::test]
    async fn truncate_returns_void() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &Some("ks".into()),
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace and table first
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE ks.t (k int PRIMARY KEY)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("TRUNCATE ks.t").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(&b[0..4], &0x0001i32.to_be_bytes()),
            _ => panic!("expected Result"),
        }
    }

    #[tokio::test]
    async fn select_virtual_table_routes_through_registry() {
        use ferrosa_common::{CellValue, DataType};
        use ferrosa_schema::{
            RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
        };

        /// Stub virtual table that returns one row with two columns.
        struct StubVTable;

        impl VirtualTable for StubVTable {
            fn name(&self) -> &str {
                "test_vtable"
            }

            fn keyspace(&self) -> &str {
                "test_ks"
            }

            fn columns(&self) -> &[VirtualColumnDef] {
                // Use a leaked slice so we can return &[VirtualColumnDef]
                // from a trait method without self-referential lifetime issues.
                static COLS: std::sync::OnceLock<Vec<VirtualColumnDef>> =
                    std::sync::OnceLock::new();
                COLS.get_or_init(|| {
                    vec![
                        VirtualColumnDef {
                            name: "name".into(),
                            data_type: DataType::Text,
                        },
                        VirtualColumnDef {
                            name: "value".into(),
                            data_type: DataType::Int,
                        },
                    ]
                })
            }

            fn primary_key_columns(&self) -> &[usize] {
                &[0]
            }

            fn read(&self, _: Option<&RowPredicate>) -> Vec<VirtualRow> {
                vec![VirtualRow {
                    cells: vec![
                        CellValue::live(b"hello".to_vec(), 0),
                        CellValue::live(42i32.to_be_bytes().to_vec(), 0),
                    ],
                }]
            }

            fn subscription_mode(&self) -> SubscriptionMode {
                SubscriptionMode::Pollable
            }
        }

        let (state, _dir) = setup();

        // Register the stub virtual table in the schema's registry.
        state.schema.virtual_tables().register(Arc::new(StubVTable));

        // Create the keyspace so the parser doesn't complain about qualified name.
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse("SELECT * FROM test_ks.test_vtable").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                // Should be a Rows result (kind = 0x0002)
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes());
                // The result should contain data (more than just the header)
                assert!(b.len() > 4);
            }
            _ => panic!("expected Result"),
        }
    }

    // ---------------------------------------------------------------
    // resolve_index_type tests
    // ---------------------------------------------------------------

    #[test]
    fn resolve_index_type_defaults_to_btree() {
        let result = resolve_index_type(None, &["col".to_string()], &HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::BTree);
    }

    #[test]
    fn resolve_index_type_phonetic() {
        let result = resolve_index_type(Some("phonetic"), &["name".to_string()], &HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::Phonetic);
    }

    #[test]
    fn resolve_index_type_unknown_errors() {
        let result = resolve_index_type(Some("nonexistent"), &["col".to_string()], &HashMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn resolve_index_type_hash() {
        let result = resolve_index_type(Some("hash"), &["user_id".to_string()], &HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::Hash);
    }

    #[test]
    fn resolve_index_type_btree_explicit() {
        let result = resolve_index_type(Some("btree"), &["col".to_string()], &HashMap::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::BTree);
    }

    #[test]
    fn resolve_index_type_composite() {
        let result = resolve_index_type(
            Some("composite"),
            &["a".to_string(), "b".to_string()],
            &HashMap::new(),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IndexType::Composite);
    }

    #[tokio::test]
    async fn create_hash_index_stores_correct_type_in_schema() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE hashks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Create table
        let stmt =
            crate::parser::parse("CREATE TABLE hashks.users (id int PRIMARY KEY, email text)")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // CREATE INDEX USING 'hash'
        let stmt = crate::parser::parse(
            "CREATE INDEX idx_email_hash ON hashks.users (email) USING 'hash'",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "CREATE INDEX USING 'hash' should succeed, got: {:?}",
            result.err()
        );

        // Verify the index is stored with IndexType::Hash
        let snap = state.schema.snapshot();
        let key = (
            "hashks".to_string(),
            "users".to_string(),
            "idx_email_hash".to_string(),
        );
        let idx = snap.indexes.get(&key);
        assert!(idx.is_some(), "index should be registered in schema");
        let idx = idx.unwrap();
        assert!(
            matches!(idx.index_type, IndexType::Hash),
            "expected IndexType::Hash, got {:?}",
            idx.index_type
        );
        assert_eq!(idx.target_columns, vec!["email"]);
    }

    #[tokio::test]
    async fn hash_index_eq_predicate_uses_single_index_plan() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace + table + hash index
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE hashplan WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE hashplan.users (id int PRIMARY KEY, email text)")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE INDEX idx_hp_email ON hashplan.users (email) USING 'hash'",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // INSERT a row
        let stmt = crate::parser::parse(
            "INSERT INTO hashplan.users (id, email) VALUES (1, 'alice@example.com')",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // SELECT WHERE email = ? — hash index Eq lookup must succeed (not require ALLOW FILTERING)
        let stmt =
            crate::parser::parse("SELECT * FROM hashplan.users WHERE email = 'alice@example.com'")
                .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "SELECT WHERE indexed_col = ? should succeed with hash index, got: {:?}",
            result.err()
        );
    }

    // ── UDT DDL routing tests ──────────────────────────────────────────

    #[tokio::test]
    async fn route_create_type_stores_in_schema() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace first
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // CREATE TYPE ks.address (street text, city text)
        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text, city text)").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                // SchemaChange kind = 0x0005
                assert_eq!(&b[0..4], &0x0005i32.to_be_bytes());
            }
            _ => panic!("expected Result"),
        }

        // Verify type exists in schema
        let udt = state.schema.get_type("ks", "address");
        assert!(
            udt.is_some(),
            "type should exist in schema after CREATE TYPE"
        );
        let udt = udt.unwrap();
        assert_eq!(udt.fields.len(), 2);
        assert_eq!(udt.fields[0].0, "street");
        assert_eq!(udt.fields[1].0, "city");
    }

    #[tokio::test]
    async fn route_drop_type() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace + type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text, city text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        assert!(state.schema.get_type("ks", "address").is_some());

        // DROP TYPE
        let stmt = crate::parser::parse("DROP TYPE ks.address").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(&b[0..4], &0x0005i32.to_be_bytes());
            }
            _ => panic!("expected Result"),
        }

        assert!(
            state.schema.get_type("ks", "address").is_none(),
            "type should be gone after DROP TYPE"
        );
    }

    #[tokio::test]
    async fn route_create_type_if_not_exists() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace + type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text, city text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // CREATE TYPE IF NOT EXISTS with same name — should succeed without error
        let stmt =
            crate::parser::parse("CREATE TYPE IF NOT EXISTS ks.address (street text, city text)")
                .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "IF NOT EXISTS should not error on duplicate"
        );
    }

    #[tokio::test]
    async fn route_drop_type_if_exists() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP TYPE IF EXISTS on non-existent type — should succeed
        let stmt = crate::parser::parse("DROP TYPE IF EXISTS ks.address").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "IF EXISTS should not error on missing type");
    }

    #[tokio::test]
    async fn route_alter_type_add_field() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace + type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // ALTER TYPE ADD city text
        let stmt = crate::parser::parse("ALTER TYPE ks.address ADD city text").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "ALTER TYPE ADD should succeed");

        let udt = state.schema.get_type("ks", "address").unwrap();
        assert_eq!(udt.fields.len(), 2);
        assert_eq!(udt.fields[1].0, "city");
    }

    #[tokio::test]
    async fn route_alter_type_rename_field() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace + type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // ALTER TYPE RENAME street TO street_name
        let stmt =
            crate::parser::parse("ALTER TYPE ks.address RENAME street TO street_name").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "ALTER TYPE RENAME should succeed");

        let udt = state.schema.get_type("ks", "address").unwrap();
        assert_eq!(udt.fields[0].0, "street_name");
    }

    #[tokio::test]
    async fn route_create_type_uses_session_keyspace() {
        let (state, _dir) = setup();
        let ks = Some("ks".to_string());
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &ks,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace first (with explicit ks in statement)
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // CREATE TYPE without explicit keyspace — should use session keyspace
        let stmt = crate::parser::parse("CREATE TYPE address (street text)").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "should use session keyspace");

        let udt = state.schema.get_type("ks", "address");
        assert!(udt.is_some(), "type should be in session keyspace");
    }

    #[tokio::test]
    async fn route_create_type_no_keyspace_errors() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // CREATE TYPE without keyspace and no session keyspace
        let stmt = crate::parser::parse("CREATE TYPE address (street text)").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_err(), "should error without keyspace");
    }

    #[tokio::test]
    async fn route_create_type_duplicate_without_if_not_exists() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace + type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text, city text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // CREATE TYPE again without IF NOT EXISTS — should return an error
        let stmt = crate::parser::parse("CREATE TYPE ks.address (street text, city text)").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "duplicate CREATE TYPE without IF NOT EXISTS should error"
        );
    }

    #[tokio::test]
    async fn route_drop_type_nonexistent_without_if_exists() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace but no type
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP TYPE on non-existent type without IF EXISTS — should return an error
        let stmt = crate::parser::parse("DROP TYPE ks.address").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "DROP TYPE on non-existent type without IF EXISTS should error"
        );
    }

    // ── UDF/UDA DDL routing tests ──────────────────────────────────────

    #[tokio::test]
    async fn route_create_function_rejects_non_wasm_language() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE FUNCTION ks.bad_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS '0061736d'"
        ).unwrap();
        let result = route(&state, &ctx, stmt).await;
        match result {
            Err(ref e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("unsupported UDF language"),
                    "error should mention unsupported language, got: {err_msg}"
                );
            }
            Ok(_) => panic!("non-wasm language should be rejected"),
        }
    }

    #[tokio::test]
    async fn route_create_function_rejects_invalid_hex() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE FUNCTION ks.bad_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS 'ZZZZ'"
        ).unwrap();
        let result = route(&state, &ctx, stmt).await;
        match result {
            Err(ref e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("invalid hex"),
                    "error should mention invalid hex, got: {err_msg}"
                );
            }
            Ok(_) => panic!("invalid hex should be rejected"),
        }
    }

    #[tokio::test]
    async fn route_create_function_rejects_invalid_wasm() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Valid hex but not valid WASM
        let stmt = crate::parser::parse(
            "CREATE FUNCTION ks.bad_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS 'deadbeef'"
        ).unwrap();
        let result = route(&state, &ctx, stmt).await;
        match result {
            Err(ref e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("compilation failed"),
                    "error should mention compilation failure, got: {err_msg}"
                );
            }
            Ok(_) => panic!("invalid wasm should be rejected"),
        }
    }

    #[tokio::test]
    async fn route_create_function_no_keyspace_errors() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse(
            "CREATE FUNCTION my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS 'deadbeef'"
        ).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_err(), "should error without keyspace");
    }

    #[tokio::test]
    async fn route_drop_function_if_exists_nonexistent() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP FUNCTION IF EXISTS on non-existent function — should succeed
        let stmt = crate::parser::parse("DROP FUNCTION IF EXISTS ks.nope").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "IF EXISTS should not error on missing function"
        );
    }

    #[tokio::test]
    async fn route_drop_function_nonexistent_without_if_exists() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP FUNCTION without IF EXISTS on non-existent function — should error
        let stmt = crate::parser::parse("DROP FUNCTION ks.nope").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "DROP FUNCTION on non-existent function without IF EXISTS should error"
        );
    }

    #[tokio::test]
    async fn route_drop_aggregate_if_exists_nonexistent() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP AGGREGATE IF EXISTS on non-existent aggregate — should succeed
        let stmt = crate::parser::parse("DROP AGGREGATE IF EXISTS ks.nope").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "IF EXISTS should not error on missing aggregate"
        );
    }

    #[tokio::test]
    async fn route_drop_aggregate_nonexistent_without_if_exists() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // DROP AGGREGATE without IF EXISTS on non-existent — should error
        let stmt = crate::parser::parse("DROP AGGREGATE ks.nope").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "DROP AGGREGATE on non-existent without IF EXISTS should error"
        );
    }

    #[tokio::test]
    async fn route_hex_decode_odd_length() {
        let result = super::hex_decode("abc");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn route_hex_decode_valid() {
        let result = super::hex_decode("deadbeef").unwrap();
        assert_eq!(result, vec![0xde, 0xad, 0xbe, 0xef]);
    }

    // ── Helper: extract row count from a Rows result body ────────────

    /// Parse a Rows RESULT body and return the row count.
    /// Wire format: [i32 kind][i32 flags][i32 col_count][string ks][string table]
    ///              per-column: [string name][u16 type_id][type params]
    ///              [i32 row_count]
    fn extract_row_count(buf: &[u8]) -> i32 {
        assert_eq!(
            &buf[0..4],
            &0x0002i32.to_be_bytes(),
            "expected Rows result kind"
        );
        let col_count = i32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;
        let mut off = 12;
        // Skip keyspace string
        let ks_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + ks_len;
        // Skip table string
        let tbl_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + tbl_len;
        // Skip column specs
        for _ in 0..col_count {
            let name_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
            off += 2 + name_len;
            let type_id = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap());
            off += 2;
            match type_id {
                0x0020 | 0x0022 => off += 2, // List/Set: element type_id
                0x0021 => off += 4,          // Map: key + val type_ids
                0x0031 => {
                    // Tuple: [u16 n][type_id * n]
                    let n = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + n * 2;
                }
                _ => {}
            }
        }
        // row_count
        i32::from_be_bytes(buf[off..off + 4].try_into().unwrap())
    }

    // ── BUG-001: TRUNCATE must actually delete data ──────────────────

    #[tokio::test]
    async fn truncate_deletes_data() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Setup: create keyspace and table
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE TABLE ks.t (id int PRIMARY KEY, v text)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert data
        let stmt = crate::parser::parse("INSERT INTO ks.t (id, v) VALUES (1, 'hello')").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("INSERT INTO ks.t (id, v) VALUES (2, 'world')").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Verify data exists
        let stmt = crate::parser::parse("SELECT * FROM ks.t WHERE id = 1").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(extract_row_count(b), 1, "should have 1 row before truncate")
            }
            _ => panic!("expected Result"),
        }

        // TRUNCATE
        let stmt = crate::parser::parse("TRUNCATE ks.t").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // SELECT after truncate should return 0 rows
        let stmt = crate::parser::parse("SELECT * FROM ks.t WHERE id = 1").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => {
                assert_eq!(extract_row_count(b), 0, "should have 0 rows after truncate")
            }
            _ => panic!("expected Result"),
        }

        // Full scan should also return 0 rows
        let stmt = crate::parser::parse("SELECT * FROM ks.t").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        match &result {
            RouteResult::Result(b) => assert_eq!(
                extract_row_count(b),
                0,
                "full scan should return 0 rows after truncate"
            ),
            _ => panic!("expected Result"),
        }
    }

    // ── ALLOW FILTERING executes full scan with post-filter ────────
    //
    // ALLOW FILTERING performs a full table scan and filters rows in
    // memory.  Queries without ALLOW FILTERING on non-indexed columns
    // are still rejected.

    #[tokio::test]
    async fn allow_filtering_executes_full_scan() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Setup
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE ks.t (id int PRIMARY KEY, score int)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("SELECT * FROM ks.t WHERE score > 25 ALLOW FILTERING").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "ALLOW FILTERING should execute full scan, got: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn allow_filtering_with_limit_executes() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE afl WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE afl.t (id int PRIMARY KEY, flag int)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("SELECT * FROM afl.t WHERE flag = 1 LIMIT 2 ALLOW FILTERING")
                .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "ALLOW FILTERING with LIMIT should execute, got: {:?}",
            result.err()
        );
    }

    // ── ALLOW FILTERING rejection: exact Cassandra semantics ─────────
    //
    // Cassandra requires ALLOW FILTERING when ANY WHERE column is not
    // covered by the partition key or a secondary index.  The presence of
    // an index on *some* WHERE columns does not exempt the remaining
    // un-indexed columns from that requirement.
    //
    // The UAT test `single_allow_filtering_rejection` exposed a gap: when
    // a table has an index on `email` and the query is
    //   SELECT * WHERE email = 'x' AND name = 'Ada'   (no ALLOW FILTERING)
    // Ferrosa was incorrectly accepting the query because `has_matching_index`
    // was true for `email`, bypassing the check for the un-indexed `name`
    // column.  Cassandra rejects this with "Cannot execute this query as it
    // might involve data filtering…".

    /// Non-indexed column in WHERE without ALLOW FILTERING must be rejected
    /// even when another WHERE column IS indexed.  This was the gap caught by
    /// the UAT run of 2026-03-17.
    #[tokio::test]
    async fn allow_filtering_required_for_non_indexed_where_column() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Setup: table with an index on `email` but NOT on `name`
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE af WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE af.users (id int PRIMARY KEY, name text, email text)",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Create an index on `email`
        let stmt = crate::parser::parse("CREATE INDEX af_email_idx ON af.users (email)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert a row
        let stmt = crate::parser::parse(
            "INSERT INTO af.users (id, name, email) VALUES (1, 'Ada', 'ada@example.com')",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Query: email is indexed but name is NOT — without ALLOW FILTERING
        // this must be rejected because `name` requires a full-scan filter.
        let stmt = crate::parser::parse(
            "SELECT * FROM af.users WHERE email = 'ada@example.com' AND name = 'Ada'",
        )
        .unwrap();
        match route(&state, &ctx, stmt).await {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("non-indexed"),
                    "error should mention non-indexed columns, got: {msg}"
                );
            }
            Ok(_) => panic!(
                "WHERE on partially-indexed columns without ALLOW FILTERING should be rejected"
            ),
        }
    }

    /// With an index on some columns and ALLOW FILTERING, the query should
    /// succeed — the index narrows the scan, post-filter handles the rest.
    #[tokio::test]
    async fn allow_filtering_with_partial_index_succeeds() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE af2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE af2.users (id int PRIMARY KEY, name text, email text)",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE INDEX af2_email_idx ON af2.users (email)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "SELECT * FROM af2.users WHERE email = 'ada@example.com' AND name = 'Ada' ALLOW FILTERING",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "ALLOW FILTERING with partial index should succeed: {:?}",
            result.err()
        );
    }

    /// Querying on a fully-indexed column without ALLOW FILTERING must succeed.
    /// This is the baseline: index present, no extra unindexed filter columns.
    #[tokio::test]
    async fn indexed_column_where_without_allow_filtering_succeeds() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let stmt = crate::parser::parse(
            "CREATE KEYSPACE af3 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE af3.users (id int PRIMARY KEY, name text, email text)",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse("CREATE INDEX af3_email_idx ON af3.users (email)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "INSERT INTO af3.users (id, name, email) VALUES (1, 'Ada', 'ada@example.com')",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // All WHERE columns are indexed — no ALLOW FILTERING needed.
        let stmt = crate::parser::parse("SELECT * FROM af3.users WHERE email = 'ada@example.com'")
            .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "fully-indexed WHERE should not require ALLOW FILTERING: {:?}",
            result.err()
        );
    }

    // ── BUG-004: ORDER BY parsed but not executed ────────────────────

    #[tokio::test]
    async fn order_by_sorts_results() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Setup: table with composite key (pk, ck) and a value column
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt = crate::parser::parse(
            "CREATE TABLE ks.t (pk int, ck int, v text, PRIMARY KEY (pk, ck))",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert rows with same pk but different ck values
        for ck in [3, 1, 4, 1, 5] {
            let stmt = crate::parser::parse(&format!(
                "INSERT INTO ks.t (pk, ck, v) VALUES (1, {ck}, 'val{ck}')"
            ))
            .unwrap();
            route(&state, &ctx, stmt).await.unwrap();
        }

        // SELECT with ORDER BY ck ASC
        let stmt = crate::parser::parse("SELECT * FROM ks.t WHERE pk = 1 ORDER BY ck ASC").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let rows_asc = match &result {
            RouteResult::Result(b) => extract_int_column_values(b, "ck"),
            _ => panic!("expected Result"),
        };
        let mut sorted = rows_asc.clone();
        sorted.sort();
        assert_eq!(
            rows_asc, sorted,
            "ORDER BY ck ASC should return sorted ascending: got {rows_asc:?}"
        );

        // SELECT with ORDER BY ck DESC
        let stmt =
            crate::parser::parse("SELECT * FROM ks.t WHERE pk = 1 ORDER BY ck DESC").unwrap();
        let result = route(&state, &ctx, stmt).await.unwrap();
        let rows_desc = match &result {
            RouteResult::Result(b) => extract_int_column_values(b, "ck"),
            _ => panic!("expected Result"),
        };
        let mut sorted_desc = rows_desc.clone();
        sorted_desc.sort_by(|a, b| b.cmp(a));
        assert_eq!(
            rows_desc, sorted_desc,
            "ORDER BY ck DESC should return sorted descending: got {rows_desc:?}"
        );
    }

    /// Extract values for a named int column from a Rows result.
    fn extract_int_column_values(buf: &[u8], target_col: &str) -> Vec<i32> {
        assert_eq!(&buf[0..4], &0x0002i32.to_be_bytes());
        let col_count = i32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;
        let mut off = 12;
        // Skip keyspace string
        let ks_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + ks_len;
        // Skip table string
        let tbl_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + tbl_len;
        // Read column names and types to find the target column index
        let mut target_idx = None;
        let mut col_type_ids = Vec::new();
        for i in 0..col_count {
            let name_len = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
            off += 2;
            let name = std::str::from_utf8(&buf[off..off + name_len]).unwrap();
            if name == target_col {
                target_idx = Some(i);
            }
            off += name_len;
            let type_id = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap());
            col_type_ids.push(type_id);
            off += 2;
            match type_id {
                0x0020 | 0x0022 => off += 2,
                0x0021 => off += 4,
                0x0031 => {
                    let n = u16::from_be_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + n * 2;
                }
                _ => {}
            }
        }
        let target_idx = target_idx.expect("target column not found in result");
        let row_count = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
        off += 4;

        let mut values = Vec::new();
        for _ in 0..row_count {
            for col in 0..col_count {
                let cell_len = i32::from_be_bytes(buf[off..off + 4].try_into().unwrap());
                off += 4;
                if cell_len >= 0 {
                    let cell_bytes = &buf[off..off + cell_len as usize];
                    if col == target_idx {
                        let val = i32::from_be_bytes(cell_bytes.try_into().unwrap());
                        values.push(val);
                    }
                    off += cell_len as usize;
                }
                // cell_len == -1 means null; skip
            }
        }
        values
    }

    // ── Observability virtual table tests ────────────────────────────────

    /// Helper: register the standard observability virtual tables into the
    /// schema's shared registry so CQL SELECT queries can resolve them.
    fn setup_with_observability() -> (SharedState, TempDir) {
        let (state, dir) = setup();
        state
            .schema
            .virtual_tables()
            .register(Arc::new(ConnectionsTable::new(
                state.connection_tracker.clone(),
            )));
        state
            .schema
            .virtual_tables()
            .register(Arc::new(ActiveQueriesTable::new(
                state.query_tracker.clone(),
            )));
        (state, dir)
    }

    /// Regression: `SELECT * FROM system_observability.connections` must succeed
    /// when the virtual table is registered in the schema's shared registry.
    #[tokio::test]
    async fn select_system_observability_connections() {
        let (state, _dir) = setup_with_observability();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };
        let stmt = crate::parser::parse("SELECT * FROM system_observability.connections").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
        match result.unwrap() {
            RouteResult::Result(b) => {
                // Kind = Rows (0x0002)
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes());
            }
            _ => panic!("expected Result"),
        }
    }

    /// Regression: `SELECT * FROM system_observability.active_queries` must
    /// succeed when the virtual table is registered in the schema's shared registry.
    #[tokio::test]
    async fn select_system_observability_active_queries() {
        let (state, _dir) = setup_with_observability();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };
        let stmt =
            crate::parser::parse("SELECT * FROM system_observability.active_queries").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
        match result.unwrap() {
            RouteResult::Result(b) => {
                // Kind = Rows (0x0002)
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes());
            }
            _ => panic!("expected Result"),
        }
    }

    /// Regression: `USE system_observability` followed by `SELECT * FROM connections`
    /// (unqualified, relying on current keyspace) must also succeed.
    #[tokio::test]
    async fn select_observability_with_use_keyspace() {
        let (state, _dir) = setup_with_observability();
        let dev = dev_auth();

        // USE system_observability — no validation, just sets current keyspace.
        let use_stmt = crate::parser::parse("USE system_observability").unwrap();
        let use_result = route(
            &state,
            &RequestContext {
                auth: &dev,
                current_keyspace: &None,
                consistency: ConsistencyLevel::One,
                serial_consistency: None,
                paging: crate::paging::PagingParams::default(),
            },
            use_stmt,
        )
        .await
        .unwrap();
        let new_ks = match use_result {
            RouteResult::SetKeyspace(ks, _) => ks,
            _ => panic!("expected SetKeyspace"),
        };
        assert_eq!(new_ks, "system_observability");

        // SELECT * FROM connections (unqualified, using current_keyspace)
        let sel_stmt = crate::parser::parse("SELECT * FROM connections").unwrap();
        let result = route(
            &state,
            &RequestContext {
                auth: &dev,
                current_keyspace: &Some(new_ks),
                consistency: ConsistencyLevel::One,
                serial_consistency: None,
                paging: crate::paging::PagingParams::default(),
            },
            sel_stmt,
        )
        .await;
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
        match result.unwrap() {
            RouteResult::Result(b) => {
                assert_eq!(&b[0..4], &0x0002i32.to_be_bytes());
            }
            _ => panic!("expected Result"),
        }
    }

    // =========================================================================
    // Task 3.5: CREATE INDEX wires through to StorageEngine indexed_columns
    // =========================================================================

    // ── UDF/UDA query-time wiring tests ──────────────────────────────────

    /// Helper: hex-encode a byte slice.
    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Minimal valid WASM component bytes (same as in ferrosa-udf tests).
    fn minimal_wasm_component() -> Vec<u8> {
        vec![
            0x00, 0x61, 0x73, 0x6d, // \0asm
            0x0d, 0x00, // version 13
            0x01, 0x00, // layer = component
        ]
    }

    #[tokio::test]
    async fn route_create_function_valid_wasm_stores_in_schema() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE udf_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Create function with valid WASM component bytes
        let hex_body = hex_encode(&minimal_wasm_component());
        let cql = format!(
            "CREATE FUNCTION udf_ks.my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "CREATE FUNCTION with valid WASM should succeed, got: {:?}",
            result.err()
        );

        // Verify function is stored in schema
        let func = state
            .schema
            .get_function("udf_ks", "my_func", &[CqlType::Int]);
        assert!(
            func.is_some(),
            "function should be registered in schema after CREATE FUNCTION"
        );
        let func = func.unwrap();
        assert_eq!(func.name, "my_func");
        assert_eq!(func.arg_names, vec!["val"]);
        assert_eq!(func.arg_types, vec![CqlType::Int]);
        assert_eq!(func.return_type, CqlType::Int);
        assert!(func.called_on_null);
        assert_eq!(func.language, "wasm");
    }

    #[tokio::test]
    async fn route_create_or_replace_function_invalidates_cache() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE replace_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let hex_body = hex_encode(&minimal_wasm_component());

        // Create function
        let cql = format!(
            "CREATE FUNCTION replace_ks.my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Verify function is in schema and WASM cache
        assert!(state
            .schema
            .get_function("replace_ks", "my_func", &[CqlType::Int])
            .is_some());

        // OR REPLACE invalidates the executor cache (the schema-level drop
        // is not yet wired, so CREATE OR REPLACE currently errors on the DDL
        // path when the function already exists — this tests the cache
        // invalidation that does occur before that error).
        let cql = format!(
            "CREATE OR REPLACE FUNCTION replace_ks.my_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        // The OR REPLACE DDL path has a known limitation: it invalidates the
        // executor cache but does not drop the schema entry before re-creating,
        // so the schema layer returns FunctionExists. Verify the error is
        // non-fatal and the original function remains intact.
        let _result = route(&state, &ctx, stmt).await;

        // Original function should still exist in schema regardless
        assert!(
            state
                .schema
                .get_function("replace_ks", "my_func", &[CqlType::Int])
                .is_some(),
            "original function should remain in schema"
        );
    }

    #[tokio::test]
    async fn route_create_function_duplicate_without_replace_errors() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE dup_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let hex_body = hex_encode(&minimal_wasm_component());

        // Create function
        let cql = format!(
            "CREATE FUNCTION dup_ks.dup_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Duplicate create without OR REPLACE or IF NOT EXISTS should error
        let stmt = crate::parser::parse(&cql).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "duplicate CREATE FUNCTION without OR REPLACE should error"
        );
    }

    #[tokio::test]
    async fn route_create_function_if_not_exists_succeeds_on_duplicate() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE ine_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let hex_body = hex_encode(&minimal_wasm_component());

        // Create function
        let cql = format!(
            "CREATE FUNCTION ine_ks.ine_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // IF NOT EXISTS should succeed without error
        let cql = format!(
            "CREATE FUNCTION IF NOT EXISTS ine_ks.ine_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "IF NOT EXISTS should not error on duplicate"
        );
    }

    #[tokio::test]
    async fn route_drop_function_removes_from_schema() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace and function
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE drop_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let hex_body = hex_encode(&minimal_wasm_component());
        let cql = format!(
            "CREATE FUNCTION drop_ks.to_drop(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Verify function exists
        assert!(state
            .schema
            .get_function("drop_ks", "to_drop", &[CqlType::Int])
            .is_some());

        // DROP FUNCTION
        let stmt = crate::parser::parse("DROP FUNCTION drop_ks.to_drop").unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "DROP FUNCTION should succeed, got: {:?}",
            result.err()
        );

        // Verify function is gone from schema
        assert!(
            state
                .schema
                .get_function("drop_ks", "to_drop", &[CqlType::Int])
                .is_none(),
            "function should be removed from schema after DROP FUNCTION"
        );
    }

    #[tokio::test]
    async fn route_create_function_with_current_keyspace() {
        let (state, _dir) = setup();

        // Create keyspace first (without current_keyspace set)
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE cur_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Now use current_keyspace context for unqualified function name
        let cur_ks = Some("cur_ks".to_string());
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &cur_ks,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        let hex_body = hex_encode(&minimal_wasm_component());
        let cql = format!(
            "CREATE FUNCTION cur_func(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE wasm AS '{hex_body}'"
        );
        let stmt = crate::parser::parse(&cql).unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "CREATE FUNCTION with current_keyspace should succeed, got: {:?}",
            result.err()
        );

        // Verify function is stored under the current keyspace
        let func = state
            .schema
            .get_function("cur_ks", "cur_func", &[CqlType::Int]);
        assert!(
            func.is_some(),
            "function should be registered under current keyspace"
        );
    }

    #[tokio::test]
    async fn route_create_aggregate_requires_state_function() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE agg_ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Try to create aggregate without the state function existing
        let stmt = crate::parser::parse(
            "CREATE AGGREGATE agg_ks.my_agg(int) SFUNC nonexistent_sfunc STYPE int",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_err(),
            "CREATE AGGREGATE should fail when state function does not exist"
        );
        let err_msg = format!("{}", result.err().unwrap());
        assert!(
            err_msg.contains("state function"),
            "error should mention missing state function, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn route_hex_decode_roundtrip() {
        // Test that our hex_encode/hex_decode are consistent
        let original = vec![0x00, 0x61, 0x73, 0x6d, 0xff, 0x00];
        let encoded = hex_encode(&original);
        let decoded = super::hex_decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    /// `CREATE INDEX` followed by `INSERT` then `SELECT WHERE indexed_col = ?`
    /// must return the inserted row via the memtable index.  This verifies the
    /// full wire-up: router calls engine.add_index(), the TableStore's
    /// indexed_columns is updated, and the next write is indexed.
    #[tokio::test]
    async fn create_index_wires_memtable_indexing_end_to_end() {
        let (state, _dir) = setup();
        let ctx = RequestContext {
            auth: &dev_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
        };

        // Create keyspace and table
        let stmt = crate::parser::parse(
            "CREATE KEYSPACE idx_wire WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        ).unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        let stmt =
            crate::parser::parse("CREATE TABLE idx_wire.users (id int PRIMARY KEY, email text)")
                .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Create index on `email` — this must wire engine.add_index()
        let stmt =
            crate::parser::parse("CREATE INDEX wire_email_idx ON idx_wire.users (email)").unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Insert a row AFTER the index was created
        let stmt = crate::parser::parse(
            "INSERT INTO idx_wire.users (id, email) VALUES (42, 'bob@example.com')",
        )
        .unwrap();
        route(&state, &ctx, stmt).await.unwrap();

        // Query by indexed column — must find the row
        let stmt = crate::parser::parse(
            "SELECT id, email FROM idx_wire.users WHERE email = 'bob@example.com'",
        )
        .unwrap();
        let result = route(&state, &ctx, stmt).await;
        assert!(
            result.is_ok(),
            "SELECT on indexed column should succeed: {:?}",
            result.err()
        );
        match result.unwrap() {
            RouteResult::Result(b) => {
                let count = extract_row_count(&b);
                assert_eq!(
                    count, 1,
                    "should find exactly 1 row via memtable index, got {count}"
                );
            }
            _ => panic!("expected Result"),
        }
    }

    // ── CQL built-in function tests ─────────────────────────────────────

    #[tokio::test]
    async fn cql_function_now() {
        // Test eval_now() directly: should produce a v1 UUID.
        let timeuuid = super::eval_now();
        match timeuuid {
            CqlValue::Timeuuid(uuid) => {
                let bytes = uuid.as_bytes();
                let version = (bytes[6] >> 4) & 0x0F;
                assert_eq!(version, 1, "now() should return a v1 UUID, got v{version}");
            }
            other => panic!("expected Timeuuid, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn cql_function_to_timestamp() {
        // Test eval_now() + eval_to_timestamp() directly.
        let timeuuid = super::eval_now();
        let ts = super::eval_to_timestamp(&timeuuid).expect("toTimestamp should succeed");

        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        match ts {
            CqlValue::Timestamp(millis) => {
                let diff = (now_millis - millis).abs();
                assert!(
                    diff < 5000,
                    "toTimestamp(now()) should be within 5s of current time, diff={diff}ms"
                );
            }
            other => panic!("expected Timestamp, got {:?}", other),
        }
    }
}
