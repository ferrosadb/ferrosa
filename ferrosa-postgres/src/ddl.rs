//! Module: Postgres DDL execution (T-132a): `CREATE TABLE [IF NOT EXISTS]`.
//!
//! Responsibility: turn a parsed `Statement::CreateTable` into a
//! `TableMetadata` and apply it through the SAME schema-change path CQL
//! `CREATE TABLE` uses (`ferrosa_cluster::ddl_path::DdlPath`: direct in
//! standalone mode, coordinator in pair mode, Raft-replicated in cluster mode).
//! There is no second path: [`ClusterDdl`] is a thin adapter over `DdlPath`.
//! Correctness: every refusal is a typed SQLSTATE naming the cause. A type with
//! no `pg_types` mapping is `42704`; `json`/`jsonb` create a CQL `jsonb` column
//! (T-161a; `json` is stored as jsonb, D11), gated by [`check_jsonb_ddl_allowed`]
//! (T-300); nothing is stored under a guessed type.
//! Last revised: 2026-09-28
//! Last changed: jsonb refused in PRIMARY KEY columns, 42P16 (T-154a); T-161a lifted the
//! jsonb/json column refusal (was T-132a).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use ferrosa_cluster::ddl_path::DdlPath;
use ferrosa_cluster::pair::ddl::DdlOperation;
use ferrosa_common::cql_type::CqlType;
use ferrosa_schema::{
    ClusteringOrder, ColumnKind, ColumnMetadata, Schema, TableMetadata, TableParams,
};
use ferrosa_sql::{ColumnDef, CreateTableStmt, PgType};
use indexmap::IndexMap;

use crate::messages::BackendMessage;
use crate::pg_types::{cql_type_for_pg_name, PgTypeError};
use crate::query::error_response;

/// Applies schema changes for the Postgres front-end.
///
/// Implemented by [`ClusterDdl`] (the production path). A failure is reported
/// as a message string; the caller maps it to `58000` (system error).
#[async_trait]
pub trait DdlExecutor: Send + Sync {
    /// Create `table` through the deployment's schema-change path.
    async fn create_table(&self, table: TableMetadata) -> Result<(), String>;
}

/// [`DdlExecutor`] over the shared, atomically swappable [`DdlPath`] that the
/// CQL router also holds, so Postgres DDL is replicated exactly as CQL DDL is.
pub struct ClusterDdl {
    path: Arc<ArcSwap<DdlPath>>,
}

impl ClusterDdl {
    pub fn new(path: Arc<ArcSwap<DdlPath>>) -> Self {
        Self { path }
    }
}

#[async_trait]
impl DdlExecutor for ClusterDdl {
    async fn create_table(&self, table: TableMetadata) -> Result<(), String> {
        let path = self.path.load_full();
        path.execute(DdlOperation::CreateTable(Box::new(table)))
            .await
            .map_err(|e| e.to_string())
    }
}

/// What a `CREATE TABLE` runs against: the schema registry it checks for
/// existing tables and the executor that applies the change.
#[derive(Clone, Copy)]
pub(crate) struct DdlEnv<'a> {
    pub(crate) executor: Option<&'a dyn DdlExecutor>,
    pub(crate) schema: &'a Schema,
    pub(crate) default_schema: &'a str,
    pub(crate) in_txn: bool,
}

fn refuse(code: &str, message: &str) -> Vec<BackendMessage> {
    vec![error_response(code, message)]
}

fn complete() -> Vec<BackendMessage> {
    vec![BackendMessage::CommandComplete {
        tag: "CREATE TABLE".to_string(),
    }]
}

/// Execute `CREATE TABLE [IF NOT EXISTS]` (FMEA PG-T132a-01..05).
///
/// Reply is `CREATE TABLE` on success and on `IF NOT EXISTS` over an existing
/// table (no NOTICE: this front-end has no `NoticeResponse`). Every refusal is
/// one `ErrorResponse` with a typed SQLSTATE.
pub(crate) async fn execute_create_table(
    env: DdlEnv<'_>,
    stmt: &CreateTableStmt,
) -> Vec<BackendMessage> {
    if env.in_txn {
        return refuse(
            "25001",
            "CREATE TABLE cannot run inside a transaction block: DDL is not transactional here",
        );
    }
    let Some(executor) = env.executor else {
        return refuse(
            "0A000",
            "CREATE TABLE is not available: this server has no schema-change path",
        );
    };
    // Authorization (CREATE on the keyspace) already ran at dispatch in
    // `authz::statement_permissions`, before this function is reached.
    let keyspace = stmt.name.schema.as_deref().unwrap_or(env.default_schema);
    if !env.schema.snapshot().keyspaces.contains_key(keyspace) {
        return refuse("3F000", &format!("schema \"{keyspace}\" does not exist"));
    }
    if let Some(reply) = existing_table_reply(env.schema, keyspace, stmt) {
        return reply;
    }
    let table = match plan_create_table(stmt, keyspace) {
        Ok(table) => table,
        Err(refusal) => return vec![refusal],
    };
    if let Err(error) = env.schema.check_create_table_jsonb(&table) {
        return vec![schema_refusal(&error)];
    }
    match executor.create_table(table).await {
        Ok(()) => complete(),
        // A concurrent CREATE can win between the check above and the apply.
        Err(error) => existing_table_reply(env.schema, keyspace, stmt)
            .unwrap_or_else(|| refuse("58000", &format!("CREATE TABLE failed: {error}"))),
    }
}

/// `Some(reply)` when the table already exists: success under `IF NOT EXISTS`,
/// `42P07` otherwise.
fn existing_table_reply(
    schema: &Schema,
    keyspace: &str,
    stmt: &CreateTableStmt,
) -> Option<Vec<BackendMessage>> {
    let key = (keyspace.to_string(), stmt.name.table.clone());
    if !schema.snapshot().tables.contains_key(&key) {
        return None;
    }
    Some(if stmt.if_not_exists {
        complete()
    } else {
        refuse(
            "42P07",
            &format!("relation \"{}\" already exists", stmt.name.table),
        )
    })
}

/// Build the `TableMetadata` for `stmt` in `keyspace`, or the refusal.
///
/// The first primary-key column is the partition key and the rest are
/// clustering columns in declared order, ascending (D10). Columns keep their
/// declared order.
pub(crate) fn plan_create_table(
    stmt: &CreateTableStmt,
    keyspace: &str,
) -> Result<TableMetadata, BackendMessage> {
    let Some((partition, clustering)) = stmt.primary_key.split_first() else {
        return Err(error_response(
            "0A000",
            "CREATE TABLE without a PRIMARY KEY is not supported",
        ));
    };
    refuse_jsonb_primary_key(stmt)?;
    let mut columns = IndexMap::new();
    for def in &stmt.columns {
        let column_type = cql_type_string(def)?;
        let (kind, position, clustering_order) = if def.name == *partition {
            (ColumnKind::PartitionKey, 0, ClusteringOrder::None)
        } else if let Some(i) = clustering.iter().position(|c| *c == def.name) {
            (ColumnKind::Clustering, i as i32, ClusteringOrder::Asc)
        } else {
            (ColumnKind::Regular, 0, ClusteringOrder::None)
        };
        columns.insert(
            def.name.clone(),
            ColumnMetadata {
                name: def.name.clone(),
                kind,
                position,
                column_type: column_type.to_string(),
                clustering_order,
                mask: None,
            },
        );
    }
    Ok(TableMetadata {
        keyspace: keyspace.to_string(),
        name: stmt.name.table.clone(),
        id: uuid::Uuid::new_v4(),
        columns,
        partition_key: vec![partition.clone()],
        clustering_key: clustering
            .iter()
            .map(|c| (c.clone(), ClusteringOrder::Asc))
            .collect(),
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions: HashMap::new(),
        is_system: false,
    })
}

/// D3 (PG-T154a-01): jsonb cannot be in a key. Postgres accepts
/// `doc jsonb PRIMARY KEY`; ferrosa keeps jsonb out of key bytes, so the plan
/// is refused with `42P16` naming the column, before the general jsonb refusal
/// (which lifts with T-300) can answer a less specific `0A000`.
fn refuse_jsonb_primary_key(stmt: &CreateTableStmt) -> Result<(), BackendMessage> {
    let key_jsonb = stmt.columns.iter().find(|def| {
        matches!(def.ty, PgType::Json | PgType::Jsonb) && stmt.primary_key.contains(&def.name)
    });
    match key_jsonb {
        Some(def) => Err(error_response(
            "42P16",
            &format!(
                "column \"{}\" is jsonb and cannot be in the PRIMARY KEY: jsonb is allowed in non-key columns only",
                def.name
            ),
        )),
        None => Ok(()),
    }
}

/// Map a schema-registry jsonb refusal (the propose-side check, PG-T154a-02)
/// to its SQLSTATE: `42P16` for a key, `0A000` for a forbidden nesting.
fn schema_refusal(error: &ferrosa_schema::SchemaError) -> BackendMessage {
    use ferrosa_schema::SchemaError;
    let code = match error {
        SchemaError::JsonbInKey { .. } => "42P16",
        _ => "0A000",
    };
    error_response(code, &error.to_string())
}

/// The single call point for the jsonb DDL gate (D24, D15a).
///
/// Always `Ok` today. T-300 replaces the body with the standalone-vs-cluster
/// check: jsonb DDL is allowed on single-node deployments and refused in
/// cluster mode until the D15a capability ledger lands. The refusal is lifted
/// by that gate, never by a flag. No mode check belongs anywhere else.
pub fn check_jsonb_ddl_allowed() -> Result<(), BackendMessage> {
    Ok(())
}

/// The CQL type string stored for `def`, through the one PG-name map (D10).
fn cql_type_string(def: &ColumnDef) -> Result<&'static str, BackendMessage> {
    if matches!(def.ty, PgType::Json | PgType::Jsonb) {
        check_jsonb_ddl_allowed()?;
    }
    let cql = cql_type_for_pg_name(pg_type_name(def.ty)).map_err(|e| match e {
        PgTypeError::UnknownPgTypeName(name) => error_response(
            "42704",
            &format!("type \"{name}\" does not exist (column \"{}\")", def.name),
        ),
        PgTypeError::UnresolvedCqlType {
            column_type,
            reason,
        } => error_response(
            "XX000",
            &format!("type map returned an unresolvable type {column_type}: {reason}"),
        ),
    })?;
    cql_type_name(&cql).ok_or_else(|| {
        error_response(
            "0A000",
            &format!(
                "column \"{}\": {cql:?} is not creatable from PG DDL",
                def.name
            ),
        )
    })
}

/// The canonical Postgres `typname` of a parsed column type.
fn pg_type_name(ty: PgType) -> &'static str {
    match ty {
        PgType::SmallInt => "int2",
        PgType::Integer => "int4",
        PgType::BigInt => "int8",
        PgType::Real => "float4",
        PgType::DoublePrecision => "float8",
        PgType::Numeric { .. } => "numeric",
        PgType::Boolean => "bool",
        PgType::Text => "text",
        PgType::Varchar(_) => "varchar",
        PgType::Bytea => "bytea",
        PgType::Uuid => "uuid",
        PgType::Timestamp => "timestamp",
        PgType::TimestampTz => "timestamptz",
        PgType::Date => "date",
        PgType::Time => "time",
        PgType::Inet => "inet",
        PgType::Jsonb => "jsonb",
        PgType::Json => "json",
    }
}

/// The CQL type name stored in `ColumnMetadata::column_type`, for the scalar
/// types the PG-name map can produce. `None` for composites.
fn cql_type_name(ty: &CqlType) -> Option<&'static str> {
    match ty {
        CqlType::Ascii => Some("ascii"),
        CqlType::Bigint => Some("bigint"),
        CqlType::Blob => Some("blob"),
        CqlType::Boolean => Some("boolean"),
        CqlType::Counter => Some("counter"),
        CqlType::Decimal => Some("decimal"),
        CqlType::Double => Some("double"),
        CqlType::Float => Some("float"),
        CqlType::Int => Some("int"),
        CqlType::Timestamp => Some("timestamp"),
        CqlType::Uuid => Some("uuid"),
        CqlType::Varchar => Some("text"),
        CqlType::Varint => Some("varint"),
        CqlType::Timeuuid => Some("timeuuid"),
        CqlType::Inet => Some("inet"),
        CqlType::Date => Some("date"),
        CqlType::Time => Some("time"),
        CqlType::Smallint => Some("smallint"),
        CqlType::Tinyint => Some("tinyint"),
        CqlType::Duration => Some("duration"),
        CqlType::Jsonb => Some("jsonb"),
        CqlType::List(_)
        | CqlType::Map(_, _)
        | CqlType::Set(_)
        | CqlType::Tuple(_)
        | CqlType::Udt { .. }
        | CqlType::Vector(_, _) => None,
    }
}
