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

use ferrosa_cluster::pair::ddl::DdlOperation;
use ferrosa_cluster::{DdlPath, WritePath};
use ferrosa_common::DataType;
use ferrosa_index::{DistanceMetric, IndexType, PhoneticAlgorithm, VectorMethod};
use ferrosa_schema::{
    query_columns, query_keyspaces, query_local, query_peers, query_role_members,
    query_role_permissions, query_roles, query_tables, AuthContext,
    ClusteringOrder as SchemaClusteringOrder, ColumnKind, ColumnMetadata, GrantEntry,
    IndexMetadata, KeyspaceMetadata, KeyspaceUpdates, NodeConfig, Permission, ReplicationParams,
    Resource, RoleMetadata, RoleUpdates, Schema, TableMetadata, TableParams, TableUpdates,
    VirtualColumnDef, VirtualRow,
};
use ferrosa_storage::StorageEngine;
use ferrosa_storage::TableId;

use crate::ast::*;
use crate::bridge;
use crate::error::CqlError;
use crate::prepared::PreparedCache;
use crate::result;
use crate::types::{CqlType, CqlValue};
use crate::virtual_tables::active_queries::QueryTracker;
use crate::virtual_tables::connections::ConnectionTracker;

/// Maximum number of statements allowed in a BATCH (security mitigation M12).
const MAX_BATCH_STATEMENTS: usize = 500;

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
}

/// Per-request context: authentication and current keyspace.
pub struct RequestContext<'a> {
    pub auth: &'a AuthContext,
    pub current_keyspace: &'a Option<String>,
}

/// Result of routing a statement.
pub enum RouteResult {
    /// A CQL RESULT frame body.
    Result(BytesMut),
    /// USE keyspace: returns the new keyspace name and a SetKeyspace frame body.
    SetKeyspace(String, BytesMut),
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
        Statement::Subscribe { .. } | Statement::Unsubscribe { .. } => Err(CqlError::Invalid(
            "SUBSCRIBE/UNSUBSCRIBE not yet supported".to_string(),
        )),
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
            let col_names = vec!["keyspace_name".into(), "durable_writes".into()];
            let col_types = vec![CqlType::Varchar, CqlType::Boolean];
            let rows: Vec<Vec<Option<CqlValue>>> = ks_rows
                .iter()
                .map(|k| {
                    vec![
                        Some(CqlValue::Text(k.keyspace_name.clone())),
                        Some(CqlValue::Boolean(k.durable_writes)),
                    ]
                })
                .collect();
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_schema",
                "keyspaces",
                &rows,
            ))
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
            let col_names = vec![
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
            let rows: Vec<Vec<Option<CqlValue>>> = col_rows
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
            Ok(result::encode_rows(
                &col_names,
                &col_types,
                "system_schema",
                "columns",
                &rows,
            ))
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
        // cqlsh queries these system_schema tables during startup introspection.
        // Return empty results for tables we don't populate yet.
        (
            "system_schema",
            "types" | "functions" | "aggregates" | "triggers" | "views" | "indexes",
        ) => Ok(result::encode_rows(
            &["keyspace_name".into(), "type_name".into()],
            &[CqlType::Varchar, CqlType::Varchar],
            "system_schema",
            s.table.as_str(),
            &[],
        )),
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

    if s.allow_filtering {
        return Err(CqlError::Invalid("ALLOW FILTERING not supported".into()));
    }

    let snap = state.schema.snapshot();
    let table_meta = snap
        .tables
        .get(&(ks.to_string(), s.table.clone()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, s.table)))?;

    // Build column info for result
    let (col_names, col_types) = build_column_info(table_meta, &s.columns)?;

    // Extract PK values from WHERE clauses
    let pk_values = extract_pk_values(&s.where_clauses, &table_meta.partition_key, table_meta)?;
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| bridge::parse_cql_type(&table_meta.columns[name].column_type))
        .collect::<Result<Vec<_>, _>>()?;

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let table_id = TableId::new(&table_meta.keyspace, &table_meta.name);

    match state.engine.read(&table_id, &decorated_key)? {
        Some(partition) => {
            // Build column index maps for partition_to_rows
            let all_col_names: Vec<String> = table_meta.columns.keys().cloned().collect();
            let all_col_types: Vec<CqlType> = table_meta
                .columns
                .values()
                .map(|c| bridge::parse_cql_type(&c.column_type))
                .collect::<Result<Vec<_>, _>>()?;
            let pk_indices: Vec<usize> = table_meta
                .partition_key
                .iter()
                .map(|name| table_meta.columns.get_index_of(name).unwrap())
                .collect();
            let ck_indices: Vec<usize> = table_meta
                .clustering_key
                .iter()
                .map(|(name, _)| table_meta.columns.get_index_of(name).unwrap())
                .collect();

            let rows = bridge::partition_to_rows(
                &partition,
                &all_col_names,
                &all_col_types,
                &pk_indices,
                &ck_indices,
            );

            // Apply column selection if not Star
            let selected_rows = select_columns(&rows, &all_col_names, &col_names);

            // Apply LIMIT
            let limited = if let Some(limit) = s.limit {
                &selected_rows[..std::cmp::min(selected_rows.len(), limit as usize)]
            } else {
                &selected_rows
            };

            Ok(result::encode_rows(
                &col_names, &col_types, ks, &s.table, limited,
            ))
        }
        None => Ok(result::encode_rows(
            &col_names,
            &col_types,
            ks,
            &s.table,
            &[],
        )),
    }
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
    let timestamp = s.using_timestamp.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    });

    for (i, col_name) in s.columns.iter().enumerate() {
        let col_meta = table_meta
            .columns
            .get(col_name)
            .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", col_name)))?;
        let cql_type = bridge::parse_cql_type(&col_meta.column_type)?;
        let value = bridge::term_to_cql_value(&s.values[i], &cql_type)?;

        match col_meta.kind {
            ColumnKind::PartitionKey => pk_vals.push((col_meta.position, value)),
            ColumnKind::Clustering => ck_vals.push((col_meta.position, value)),
            ColumnKind::Regular | ColumnKind::Static => {
                let col_idx = table_meta.columns.get_index_of(col_name).unwrap() as u16;
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
        .map(|name| bridge::parse_cql_type(&table_meta.columns[name].column_type))
        .collect::<Result<Vec<_>, _>>()?;

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_row(&regular_cells, &ck_values, timestamp, s.using_ttl);
    let table_id = TableId::new(ks, &s.table);

    state
        .write_path
        .load()
        .write(&table_id, &decorated_key, row, timestamp)
        .await?;
    Ok(result::encode_void())
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

    let timestamp = s.using_timestamp.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    });

    // Extract PK and CK values from WHERE clauses
    let pk_values = extract_pk_values(&s.where_clauses, &table_meta.partition_key, table_meta)?;
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| bridge::parse_cql_type(&table_meta.columns[name].column_type))
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
                let cql_type = bridge::parse_cql_type(&col_meta.column_type)?;
                let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
                ck_values.push(val);
                break;
            }
        }
    }

    // Build cells from SET assignments
    let mut regular_cells: Vec<(u16, CqlValue)> = Vec::new();
    for (col_name, term) in &s.assignments {
        let col_meta = table_meta
            .columns
            .get(col_name)
            .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", col_name)))?;
        let cql_type = bridge::parse_cql_type(&col_meta.column_type)?;
        let value = bridge::term_to_cql_value(term, &cql_type)?;
        let col_idx = table_meta.columns.get_index_of(col_name).unwrap() as u16;
        regular_cells.push((col_idx, value));
    }

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_row(&regular_cells, &ck_values, timestamp, s.using_ttl);
    let table_id = TableId::new(ks, &s.table);

    state
        .write_path
        .load()
        .write(&table_id, &decorated_key, row, timestamp)
        .await?;
    Ok(result::encode_void())
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

    let timestamp = s.using_timestamp.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    });

    // Extract PK values from WHERE
    let pk_values = extract_pk_values(&s.where_clauses, &table_meta.partition_key, table_meta)?;
    let pk_types: Vec<CqlType> = table_meta
        .partition_key
        .iter()
        .map(|name| bridge::parse_cql_type(&table_meta.columns[name].column_type))
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
                let cql_type = bridge::parse_cql_type(&col_meta.column_type)?;
                let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
                ck_values.push(val);
                break;
            }
        }
    }

    // Build delete column indices (empty = row-level delete)
    let delete_columns: Vec<u16> = s
        .columns
        .iter()
        .map(|col_name| {
            table_meta
                .columns
                .get_index_of(col_name)
                .ok_or_else(|| CqlError::Invalid(format!("unknown column: {}", col_name)))
                .map(|idx| idx as u16)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let decorated_key = bridge::build_decorated_key(&pk_values, &pk_types)?;
    let row = bridge::build_delete_row(&delete_columns, &ck_values, timestamp);
    let table_id = TableId::new(ks, &s.table);

    state
        .write_path
        .load()
        .write(&table_id, &decorated_key, row, timestamp)
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

    match &**state.ddl_path.load() {
        DdlPath::Direct { .. } => {
            state.schema.create_keyspace(ks_meta, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateKeyspace(ks_meta);
            coordinator.coordinate_ddl(op).await?;
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

    match &**state.ddl_path.load() {
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

    match &**state.ddl_path.load() {
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
        extensions: std::collections::HashMap::new(),
        is_system: false,
    };

    match &**state.ddl_path.load() {
        DdlPath::Direct { .. } => {
            // Register with schema
            state.schema.create_table(table_meta.clone(), ctx.auth)?;

            // Register with storage engine
            let storage_schema = table_meta.to_storage_schema();
            state.engine.register_table(storage_schema)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateTable(Box::new(table_meta));
            coordinator.coordinate_ddl(op).await?;
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

    let updates = TableUpdates {
        params: None,
        add_columns,
        drop_columns: s.drop_columns.clone(),
        extensions: None,
    };

    match &**state.ddl_path.load() {
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

    match &**state.ddl_path.load() {
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
    columns: &[String],
    options: &HashMap<String, String>,
) -> Result<IndexType, CqlError> {
    match using {
        None | Some("btree") => Ok(IndexType::BTree),
        Some("hash") => Ok(IndexType::Hash),
        Some("composite") => Ok(IndexType::Composite {
            columns: columns.to_vec(),
        }),
        Some("phonetic") => {
            let algorithm = match options.get("algorithm").map(|s| s.as_str()) {
                Some("soundex") | None => PhoneticAlgorithm::Soundex,
                Some("metaphone") => PhoneticAlgorithm::Metaphone,
                Some("double_metaphone") => PhoneticAlgorithm::DoubleMetaphone,
                Some("caverphone") => PhoneticAlgorithm::Caverphone,
                Some(other) => {
                    return Err(CqlError::Invalid(format!(
                        "unknown phonetic algorithm: {other}"
                    )))
                }
            };
            Ok(IndexType::Phonetic { algorithm })
        }
        Some("vector") => {
            let dimensions: u32 = options
                .get("dimensions")
                .ok_or_else(|| {
                    CqlError::Invalid("vector index requires 'dimensions' option".to_string())
                })?
                .parse()
                .map_err(|_| CqlError::Invalid("invalid dimensions value".to_string()))?;

            let metric = match options.get("metric").map(|s| s.as_str()) {
                Some("cosine") | None => DistanceMetric::Cosine,
                Some("l2") => DistanceMetric::L2,
                Some("inner_product") => DistanceMetric::InnerProduct,
                Some(other) => {
                    return Err(CqlError::Invalid(format!(
                        "unknown distance metric: {other}"
                    )))
                }
            };

            let method = match options.get("method").map(|s| s.as_str()) {
                Some("hnsw") | None => {
                    let m = options
                        .get("m")
                        .map(|s| s.parse().unwrap_or(16))
                        .unwrap_or(16);
                    let ef_construction = options
                        .get("ef_construction")
                        .map(|s| s.parse().unwrap_or(200))
                        .unwrap_or(200);
                    VectorMethod::Hnsw { m, ef_construction }
                }
                Some("ivfflat") => {
                    let lists = options
                        .get("lists")
                        .map(|s| s.parse().unwrap_or(100))
                        .unwrap_or(100);
                    VectorMethod::IvfFlat { lists }
                }
                Some(other) => {
                    return Err(CqlError::Invalid(format!("unknown vector method: {other}")))
                }
            };

            Ok(IndexType::Vector {
                method,
                metric,
                dimensions,
            })
        }
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

    match &**state.ddl_path.load() {
        DdlPath::Direct { .. } => {
            state.schema.create_index(index_meta, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateIndex(index_meta);
            coordinator.coordinate_ddl(op).await?;
        }
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "CREATED",
        "INDEX",
        &[ks, &index_name],
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
                    "INDEX",
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

    match &**state.ddl_path.load() {
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
        DdlPath::Unavailable => {
            return Err(CqlError::ServerError(
                "DDL unavailable: peer lost".to_string(),
            ));
        }
    }

    Ok(result::encode_schema_change(
        "DROPPED",
        "INDEX",
        &[ks, &s.name],
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

    match &**state.ddl_path.load() {
        DdlPath::Direct { .. } => {
            state
                .schema
                .create_role(role, s.password.as_deref(), ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::CreateRole(role);
            coordinator.coordinate_ddl(op).await?;
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

    match &**state.ddl_path.load() {
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

    match &**state.ddl_path.load() {
        DdlPath::Direct { .. } => {
            state.schema.drop_role(&s.name, ctx.auth)?;
        }
        DdlPath::Pair(coordinator) => {
            let op = DdlOperation::DropRole(s.name.clone());
            coordinator.coordinate_ddl(op).await?;
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

    match &**state.ddl_path.load() {
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

    match &**state.ddl_path.load() {
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

    // Truncation is a follow-on storage feature; return Void for now.
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

/// Build column names and types for a SELECT result.
fn build_column_info(
    table_meta: &TableMetadata,
    select_columns: &[SelectColumn],
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
            .map(|c| bridge::parse_cql_type(&c.column_type))
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
                types.push(bridge::parse_cql_type(&col.column_type)?);
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
        let cql_type = bridge::parse_cql_type(&col_meta.column_type)?;
        let val = bridge::term_to_cql_value(&wc.value, &cql_type)?;
        values.push(val);
    }
    Ok(values)
}

/// Project rows to a selected column subset.
fn select_columns(
    rows: &[Vec<Option<CqlValue>>],
    all_names: &[String],
    selected: &[String],
) -> Vec<Vec<Option<CqlValue>>> {
    // If selected == all_names, return as-is
    if all_names == selected {
        return rows.to_vec();
    }
    // Build index mapping
    let indices: Vec<usize> = selected
        .iter()
        .filter_map(|name| all_names.iter().position(|n| n == name))
        .collect();
    rows.iter()
        .map(|row| indices.iter().map(|&i| row[i].clone()).collect())
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
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
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
    fn resolve_index_type_vector_hnsw() {
        let mut opts = HashMap::new();
        opts.insert("method".to_string(), "hnsw".to_string());
        opts.insert("metric".to_string(), "cosine".to_string());
        opts.insert("dimensions".to_string(), "768".to_string());
        let result = resolve_index_type(Some("vector"), &["embed".to_string()], &opts);
        assert!(result.is_ok());
        match result.unwrap() {
            IndexType::Vector {
                method,
                metric,
                dimensions,
            } => {
                assert!(matches!(method, VectorMethod::Hnsw { .. }));
                assert_eq!(metric, DistanceMetric::Cosine);
                assert_eq!(dimensions, 768);
            }
            other => panic!("expected Vector, got {:?}", other),
        }
    }

    #[test]
    fn resolve_index_type_vector_missing_dimensions_errors() {
        let mut opts = HashMap::new();
        opts.insert("method".to_string(), "hnsw".to_string());
        let result = resolve_index_type(Some("vector"), &["embed".to_string()], &opts);
        assert!(result.is_err());
    }
}
