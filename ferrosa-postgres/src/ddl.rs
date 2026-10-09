//! Module: Postgres DDL execution (T-132a): `CREATE TABLE [IF NOT EXISTS]`.
//!
//! Responsibility: turn a parsed `Statement::CreateTable` into a
//! `TableMetadata` and apply it through the SAME schema-change path CQL
//! `CREATE TABLE` uses (`ferrosa_cluster::ddl_path::DdlPath`: direct in
//! standalone mode, coordinator in pair mode, Raft-replicated in cluster mode).
//! There is no second path: [`ClusterDdl`] is a thin adapter over `DdlPath`.
//! Correctness: every refusal is a typed SQLSTATE naming the cause. A type with
//! no `pg_types` mapping is `42704`; `json`/`jsonb` create a CQL `jsonb` column
//! (T-161a; `json` is stored as jsonb, D11), gated by `Schema::check_create_table_jsonb` (T-154a key rules plus the T-300
//! standalone-only rule)
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
use ferrosa_common::timeuuid::{is_reserved_column_name, SYNTHETIC_KEY_COLUMN};
use ferrosa_schema::{
    ClusteringOrder, ColumnKind, ColumnMetadata, Schema, TableMetadata, TableParams, TableUpdates,
};
use ferrosa_sql::{
    AlterOperation, AlterTableStmt, ColumnDef, CreateTableStmt, DropTableStatement, PgType,
};
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
    /// Drop `keyspace.table` through the same path. A missing table is the
    /// caller's concern (it checks existence first); this reports only apply
    /// failures.
    async fn drop_table(&self, keyspace: &str, table: &str) -> Result<(), String>;
    /// Apply `updates` to `keyspace.table` through the same path.
    async fn alter_table(
        &self,
        keyspace: &str,
        table: &str,
        updates: TableUpdates,
    ) -> Result<(), String>;
    /// Create a secondary index over `columns` through the same path.
    ///
    /// Takes the pieces rather than an `IndexMetadata` so the front-end needs no dependency on
    /// `ferrosa-index`; the construction lives beside `DdlOperation::CreateIndex`.
    async fn create_index(
        &self,
        keyspace: &str,
        table: &str,
        name: &str,
        columns: &[String],
    ) -> Result<(), String>;
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

    async fn drop_table(&self, keyspace: &str, table: &str) -> Result<(), String> {
        let path = self.path.load_full();
        path.execute(DdlOperation::DropTable {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        })
        .await
        .map_err(|e| e.to_string())
    }

    async fn alter_table(
        &self,
        keyspace: &str,
        table: &str,
        updates: TableUpdates,
    ) -> Result<(), String> {
        let path = self.path.load_full();
        path.execute(DdlOperation::AlterTable {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            updates: Box::new(updates),
        })
        .await
        .map_err(|e| e.to_string())
    }

    async fn create_index(
        &self,
        keyspace: &str,
        table: &str,
        name: &str,
        columns: &[String],
    ) -> Result<(), String> {
        let path = self.path.load_full();
        let index = ferrosa_cluster::pair::ddl::secondary_index(keyspace, table, name, columns);
        path.execute(DdlOperation::CreateIndex(index))
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
    complete_with("CREATE TABLE")
}

/// The same, for a statement whose completion tag differs.
fn complete_with(tag: &str) -> Vec<BackendMessage> {
    vec![BackendMessage::CommandComplete {
        tag: tag.to_string(),
    }]
}

/// Execute `ALTER TABLE <table> <operation>`.
///
/// Three operations, and what each is for:
///
/// - **ADD PRIMARY KEY** records the *declared* key ([`crate::pg_key`]) so introspection reports
///   the key the user asked for rather than the synthetic `_sys_ck_` a PK-less table was given,
///   and builds a **secondary index** over it when that key is not the storage key — without
///   which a lookup by the declared key degrades to a full scan.
/// - **ADD COLUMN** and **DROP COLUMN** map straight onto `TableUpdates`.
///
/// Everything else is refused by the parser, so nothing here has to guess. Each validation runs
/// before the first write, so a refused statement applies nothing.
pub(crate) async fn execute_alter_table(
    env: DdlEnv<'_>,
    stmt: &AlterTableStmt,
) -> Vec<BackendMessage> {
    if env.in_txn {
        return refuse(
            "25001",
            "ALTER TABLE cannot run inside a transaction block: DDL is not transactional here",
        );
    }
    let Some(executor) = env.executor else {
        return refuse(
            "0A000",
            "ALTER TABLE is not available: this server has no schema-change path",
        );
    };
    let keyspace = stmt.table.schema.as_deref().unwrap_or(env.default_schema);
    let key = (keyspace.to_string(), stmt.table.table.clone());
    let Some(meta) = env.schema.snapshot().tables.get(&key).cloned() else {
        return refuse(
            "42P01",
            &format!("relation \"{}\" does not exist", stmt.table.table),
        );
    };

    match &stmt.operation {
        AlterOperation::AddPrimaryKey(columns) => {
            execute_add_primary_key(executor, keyspace, &meta, columns).await
        }
        AlterOperation::AddColumn(def) => {
            if is_reserved_column_name(&def.name) {
                return refuse(
                    "42P16",
                    &format!(
                        "column name \"{}\" is reserved: the `_sys_` prefix belongs to ferrosa",
                        def.name
                    ),
                );
            }
            if meta.columns.contains_key(&def.name) {
                return refuse(
                    "42701",
                    &format!(
                        "column \"{}\" of relation \"{}\" already exists",
                        def.name, meta.name
                    ),
                );
            }
            // The type must map before anything is written, so an unknown type is a refusal
            // rather than a half-applied change.
            let column_type = match cql_type_string(def) {
                Ok(t) => t,
                Err(msg) => return vec![msg],
            };
            let updates = TableUpdates {
                params: None,
                add_columns: vec![ColumnMetadata {
                    name: def.name.clone(),
                    // A column added by ALTER is a regular column: a key column is added by
                    // re-creating the table, and `TableUpdates` cannot change the key anyway.
                    kind: ColumnKind::Regular,
                    position: 0,
                    column_type: column_type.to_string(),
                    clustering_order: ClusteringOrder::None,
                    mask: None,
                }],
                drop_columns: Vec::new(),
                extensions: None,
            };
            if let Err(error) = executor
                .alter_table(keyspace, &stmt.table.table, updates)
                .await
            {
                return refuse("58000", &format!("ALTER TABLE failed: {error}"));
            }
            complete_with("ALTER TABLE")
        }
        AlterOperation::DropColumn(name) => {
            if is_reserved_column_name(name) {
                return refuse(
                    "42P16",
                    &format!(
                        "column name \"{name}\" is reserved: the `_sys_` prefix belongs to ferrosa"
                    ),
                );
            }
            if !meta.columns.contains_key(name) {
                return refuse(
                    "42703",
                    &format!(
                        "column \"{name}\" of relation \"{}\" does not exist",
                        meta.name
                    ),
                );
            }
            // Dropping a key column would leave rows with no identity — and, for a declared key,
            // an extension naming a column that no longer exists.
            if crate::pg_key::storage_key_columns(&meta).contains(name)
                || crate::pg_key::of(&meta).contains(name)
            {
                return refuse(
                    "2BP01",
                    &format!("cannot drop column \"{name}\" because the primary key depends on it"),
                );
            }
            let updates = TableUpdates {
                params: None,
                add_columns: Vec::new(),
                drop_columns: vec![name.clone()],
                extensions: None,
            };
            if let Err(error) = executor
                .alter_table(keyspace, &stmt.table.table, updates)
                .await
            {
                return refuse("58000", &format!("ALTER TABLE failed: {error}"));
            }
            complete_with("ALTER TABLE")
        }
    }
}

/// Record the declared key and, when it is not the storage key, index it.
///
/// The record is applied FIRST, so a failure to index still leaves the declared key correct
/// rather than losing it; `58000` names which step failed.
async fn execute_add_primary_key(
    executor: &dyn DdlExecutor,
    keyspace: &str,
    meta: &TableMetadata,
    columns: &[String],
) -> Vec<BackendMessage> {
    if columns.is_empty() {
        return refuse("42601", "a PRIMARY KEY must name at least one column");
    }
    for name in columns {
        if is_reserved_column_name(name) {
            return refuse(
                "42P16",
                &format!(
                    "column name \"{name}\" is reserved: the `_sys_` prefix belongs to ferrosa"
                ),
            );
        }
        if !meta.columns.contains_key(name) {
            return refuse(
                "42703",
                &format!("column \"{name}\" named in the PRIMARY KEY does not exist"),
            );
        }
    }

    let updates = TableUpdates {
        params: None,
        add_columns: Vec::new(),
        drop_columns: Vec::new(),
        extensions: Some(HashMap::from([(
            crate::pg_key::PRIMARY_KEY_EXTENSION.to_string(),
            crate::pg_key::encode(columns),
        )])),
    };
    if let Err(error) = executor.alter_table(keyspace, &meta.name, updates).await {
        return refuse("58000", &format!("ALTER TABLE failed: {error}"));
    }

    // Only when it ADDs something: a key that already is the storage key is served by the
    // primary structure, and indexing it again would be pure write overhead.
    if columns != crate::pg_key::storage_key_columns(meta) {
        let name = format!("{}_pkey", meta.name);
        if let Err(error) = executor
            .create_index(keyspace, &meta.name, &name, columns)
            .await
        {
            return refuse(
                "58000",
                &format!("the key was recorded but indexing it failed: {error}"),
            );
        }
    }

    complete_with("ALTER TABLE")
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

/// Execute `DROP TABLE [IF EXISTS] a [, b, ...]` (pgbench -i's drop-all step).
///
/// Each named table is resolved in its schema (defaulting to `env.default_schema`)
/// and dropped through the SAME schema-change path `CREATE TABLE` uses. A table
/// that does not exist is an error (`42P01`) unless `IF EXISTS` was given, in
/// which case it is skipped — mirroring PostgreSQL. Reply is `DROP TABLE` on
/// success. Every refusal is one `ErrorResponse` with a typed SQLSTATE.
pub(crate) async fn execute_drop_table(
    env: DdlEnv<'_>,
    stmt: &DropTableStatement,
) -> Vec<BackendMessage> {
    if env.in_txn {
        return refuse(
            "25001",
            "DROP TABLE cannot run inside a transaction block: DDL is not transactional here",
        );
    }
    let Some(executor) = env.executor else {
        return refuse(
            "0A000",
            "DROP TABLE is not available: this server has no schema-change path",
        );
    };
    // Drop in the order given. Existence is checked per table so the reply is
    // exact about which name failed; the executor reports only apply failures.
    for target in &stmt.tables {
        let keyspace = target.schema.as_deref().unwrap_or(env.default_schema);
        let key = (keyspace.to_string(), target.table.clone());
        if !env.schema.snapshot().tables.contains_key(&key) {
            if stmt.if_exists {
                continue; // PostgreSQL: missing table is a no-op under IF EXISTS.
            }
            return refuse(
                "42P01",
                &format!("relation \"{}\" does not exist", target.table),
            );
        }
        if let Err(error) = executor.drop_table(keyspace, &target.table).await {
            return refuse(
                "58000",
                &format!("DROP TABLE failed for \"{}\": {error}", target.table),
            );
        }
    }
    vec![BackendMessage::CommandComplete {
        tag: "DROP TABLE".to_string(),
    }]
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
    // The `_sys_` prefix is ferrosa's own. The front end recognises it by name to filter
    // the column out of `SELECT *` and to give it a negative `attnum` in `pg_attribute`,
    // so accepting a user column in that namespace would make both those rules lie.
    for def in &stmt.columns {
        if is_reserved_column_name(&def.name) {
            return Err(error_response(
                "42P16",
                &format!(
                    "column name \"{}\" is reserved: the `_sys_` prefix belongs to ferrosa",
                    def.name
                ),
            ));
        }
    }

    // PostgreSQL allows a table with no PRIMARY KEY; ferrosa's storage needs a partition
    // key. Synthesize one on a fresh v1-TimeUUID column at position 0 rather than keying on
    // the user's first column: that assumed the first column is unique and silently lost
    // writes wherever it was not. A UUID key is unique by construction.
    //
    // Postgres has no separate timeuuid type, so the column is reported as `uuid` — the
    // same 16 bytes, and what a PG driver can actually decode.
    let mut declared_columns: Vec<ColumnDef> = stmt.columns.clone();
    let mut primary_key: Vec<String> = stmt.primary_key.clone();
    if primary_key.is_empty() {
        declared_columns.insert(
            0,
            ColumnDef {
                name: SYNTHETIC_KEY_COLUMN.to_string(),
                ty: PgType::Uuid,
                not_null: true,
                primary_key: true,
            },
        );
        primary_key = vec![SYNTHETIC_KEY_COLUMN.to_string()];
    }

    let Some((partition, clustering)) = primary_key.split_first() else {
        // Unreachable: a table that declared no key just gained the synthetic one above.
        return Err(error_response(
            "XX000",
            "internal: CREATE TABLE planned with no key column",
        ));
    };
    refuse_jsonb_primary_key(&declared_columns, &primary_key)?;
    let mut columns = IndexMap::new();
    for def in &declared_columns {
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
        extensions: declared_key_extension(&stmt.primary_key),
        is_system: false,
    })
}

/// The primary key PostgreSQL should later report, recorded on the table as
/// [`crate::pg_key::PRIMARY_KEY_EXTENSION`].
///
/// Only a **declared** key is recorded. A synthesized `_sys_ck_` key is deliberately left
/// out: PostgreSQL would report no primary key for a table created without one, and showing
/// it ferrosa's internal column instead would be a lie a client cannot detect. `pg_key::of`
/// falls back to the storage key for tables that never came through the Postgres front end.
fn declared_key_extension(declared: &[String]) -> HashMap<String, String> {
    let mut extensions = HashMap::new();
    if !declared.is_empty() {
        extensions.insert(
            crate::pg_key::PRIMARY_KEY_EXTENSION.to_string(),
            crate::pg_key::encode(declared),
        );
    }
    extensions
}

/// D3 (PG-T154a-01): jsonb cannot be in a key. Postgres accepts
/// `doc jsonb PRIMARY KEY`; ferrosa keeps jsonb out of key bytes, so the plan
/// is refused with `42P16` naming the column, before the general jsonb refusal
/// (which lifts with T-300) can answer a less specific `0A000`.
fn refuse_jsonb_primary_key(
    columns: &[ColumnDef],
    primary_key: &[String],
) -> Result<(), BackendMessage> {
    let key_jsonb = columns.iter().find(|def| {
        matches!(def.ty, PgType::Json | PgType::Jsonb) && primary_key.contains(&def.name)
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

/// Map a refusal from `Schema::check_create_table_jsonb` (the propose-side
/// check: T-154a key placement plus the T-300 standalone-only gate) to its
/// SQLSTATE (t_57fa8a9e).
fn schema_refusal(error: &ferrosa_schema::SchemaError) -> BackendMessage {
    use ferrosa_schema::SchemaError;
    let code = match error {
        SchemaError::JsonbInKey { .. } => "42P16",
        // jsonb nesting that is never allowed, and jsonb DDL outside a
        // standalone node until the D15a ledger (D24): both unsupported here.
        SchemaError::JsonbNesting { .. } | SchemaError::JsonbDdlRefused { .. } => "0A000",
        // A column type string we generated ourselves failed to parse.
        SchemaError::InvalidSchema(_) => "XX000",
        // `SchemaError` is `#[non_exhaustive]`, so a wildcard is mandatory here
        // and a new variant cannot be caught at compile time. Nothing else is
        // produced by `check_create_table_jsonb`; reaching this arm is an
        // internal error, reported loudly as XX000, never as "unsupported".
        other => {
            tracing::error!(error = %other, "unexpected schema error from the jsonb DDL check");
            "XX000"
        }
    };
    error_response(code, &error.to_string())
}

/// The CQL type string stored for `def`, through the one PG-name map (D10).
fn cql_type_string(def: &ColumnDef) -> Result<&'static str, BackendMessage> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::timeuuid::{is_reserved_column_name, SYNTHETIC_KEY_COLUMN};
    use ferrosa_sql::{parse_statement, Statement};

    fn plan(sql: &str) -> Result<TableMetadata, BackendMessage> {
        let Statement::CreateTable(stmt) = parse_statement(sql).expect("must parse") else {
            panic!("expected a CreateTable");
        };
        plan_create_table(&stmt, "public")
    }

    fn plan_err(sql: &str) -> String {
        format!("{:?}", plan(sql).expect_err("must be refused"))
    }

    /// PostgreSQL allows a table with no PRIMARY KEY. ferrosa's storage needs a partition
    /// key, so the table gets a synthetic `_sys_ck_` column as its key. pgbench's own
    /// `pgbench_accounts (aid, bid, abalance, filler char(84))` is exactly this shape, and
    /// refusing it is what stops `pgbench -i`.
    #[test]
    fn a_pk_less_create_table_gets_a_synthetic_key_column() {
        let meta =
            plan("CREATE TABLE pgbench_accounts (aid int, bid int, abalance int, filler char(84))")
                .expect("a PK-less CREATE TABLE must plan");

        assert_eq!(
            meta.partition_key,
            vec![SYNTHETIC_KEY_COLUMN.to_string()],
            "the synthetic column carries the key"
        );
        assert!(
            meta.clustering_key.is_empty(),
            "nothing was declared as a clustering key"
        );
        let key = meta
            .columns
            .get(SYNTHETIC_KEY_COLUMN)
            .expect("the synthetic column must exist");
        assert_eq!(key.kind, ColumnKind::PartitionKey);
        assert_eq!(
            key.column_type, "uuid",
            "the key is a v1 TimeUUID; Postgres has no separate timeuuid type so it is \
             stored and reported as uuid (same 16-byte layout)"
        );
        let names: Vec<&str> = meta.columns.keys().map(String::as_str).collect();
        assert_eq!(
            names,
            vec![SYNTHETIC_KEY_COLUMN, "aid", "bid", "abalance", "filler"],
            "the synthetic column goes first, then the declared columns in order"
        );
        for name in ["aid", "bid", "abalance", "filler"] {
            assert_eq!(
                meta.columns.get(name).map(|c| c.kind),
                Some(ColumnKind::Regular)
            );
        }
    }

    /// The declared key is recorded so introspection can report it. PostgreSQL would report
    /// no primary key for a table that declared none — so a *synthesized* key must not be
    /// recorded as one, or `\d` would advertise ferrosa's internal column as the table's key.
    #[test]
    fn the_declared_key_is_recorded_and_a_synthesized_one_is_not() {
        let declared =
            plan("CREATE TABLE t (a int, b int, PRIMARY KEY (a, b))").expect("must plan");
        assert_eq!(
            crate::pg_key::recorded(&declared),
            Some(vec!["a".to_string(), "b".to_string()]),
            "the declared key must be reported"
        );

        let synthesized = plan("CREATE TABLE t (aid int, bid int)").expect("must plan");
        assert_eq!(
            crate::pg_key::recorded(&synthesized),
            None,
            "a table that declared no key must report none, not the synthetic one"
        );
        assert!(
            crate::pg_key::of(&synthesized).is_empty(),
            "and the synthetic key must not surface through the derived path either"
        );
    }

    /// A declared key is honoured exactly, and nothing is synthesized.
    #[test]
    fn a_declared_primary_key_is_not_shadowed() {
        let meta = plan("CREATE TABLE t (a int PRIMARY KEY, b int)").expect("must plan");
        assert_eq!(meta.partition_key, vec!["a".to_string()]);
        assert!(
            !meta.columns.contains_key(SYNTHETIC_KEY_COLUMN),
            "a table that declared a key must not gain a synthetic one"
        );
    }

    /// The `_sys_` prefix is ferrosa's. A user who could declare a column in it would make
    /// the front-end rules that recognise the prefix by name lie — it is filtered from
    /// `SELECT *` and carries a negative `attnum` in `pg_attribute`.
    #[test]
    fn a_user_column_in_the_reserved_prefix_is_refused() {
        for sql in [
            "CREATE TABLE t (_sys_ck_ int, b int)",
            "CREATE TABLE t (b int, _SYS_anything int)",
            "CREATE TABLE t (_Sys_X int, b int, PRIMARY KEY (_Sys_X))",
        ] {
            let text = plan_err(sql);
            assert!(
                text.contains("reserved"),
                "{sql} must be refused as reserved: {text}"
            );
        }
        assert!(is_reserved_column_name(SYNTHETIC_KEY_COLUMN));
    }
}
