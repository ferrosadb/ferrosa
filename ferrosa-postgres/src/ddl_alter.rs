//! Module: the column and constraint actions of `ALTER TABLE` (Oban's migrations).
//!
//! A statement's actions are planned in order against a working copy of the table, then
//! applied as ONE schema change (`TableUpdates`), so a statement is applied whole or not at
//! all: an action that is refused refuses the statement and nothing is applied.
//!
//! * `ADD COLUMN [IF NOT EXISTS] c t [DEFAULT e] [NOT NULL]` — `IF NOT EXISTS` over an
//!   existing column is a no-op. On a table that already has rows, a column with a
//!   `DEFAULT` or `NOT NULL` is refused (`0A000`): PostgreSQL backfills the default into the
//!   existing rows and ferrosa does not, so they would read NULL.
//! * `ALTER COLUMN c TYPE t` — the column's current type is a no-op; any other type is
//!   refused by name (`0A000`): rewriting stored values safely is not supported.
//! * `ALTER COLUMN c SET DEFAULT e` / `DROP DEFAULT` — recorded for the INSERT path.
//! * `ALTER COLUMN c SET NOT NULL` — validated: refused (`23502`) if an existing row is NULL.
//!   `DROP NOT NULL` clears it (a key column cannot lose it, `42P16`).
//! * `ADD [CONSTRAINT n] CHECK (e) [NOT VALID]` — validated against existing rows (`23514`)
//!   unless `NOT VALID`; enforced on every later write either way (`pg_constraint`).
//! * `DROP CONSTRAINT [IF EXISTS] n` — a CHECK. A foreign key or primary key cannot be
//!   dropped (`0A000`); a missing name is `42704` unless `IF EXISTS`.
//! * `SET UNLOGGED` / `SET LOGGED` — recorded (`pg.unlogged`), not applied: ferrosa keeps
//!   the table durable, which is stronger than `UNLOGGED` asks for.
//!
//! Validation of existing rows runs a `LIMIT 1` probe query ([`RowProbe`]). Inside a
//! transaction block it sees committed rows; a table created earlier in the same block has
//! none. A row written by another session between the probe and COMMIT is not re-checked
//! (the same window PostgreSQL closes with its lock, which ferrosa's DDL does not take).
//!
//! Last revised: 2026-11-08

use std::collections::HashMap;

use async_trait::async_trait;
use ferrosa_schema::{ClusteringOrder, ColumnKind, ColumnMetadata, TableMetadata, TableUpdates};
use ferrosa_sql::{AlterOperation, ColumnDef};

use crate::ddl::{DdlEnv, DdlExecutor};
use crate::messages::BackendMessage;
use crate::query::error_response;

/// Answers "does any row of `keyspace.table` satisfy `predicate`?" for validation.
#[async_trait]
pub(crate) trait RowProbe: Send + Sync {
    async fn any_row(
        &self,
        keyspace: &str,
        table: &str,
        predicate: &str,
    ) -> Result<bool, BackendMessage>;
}

/// The planned change: one `TableUpdates`, plus the validations it needs.
struct Plan {
    meta: TableMetadata,
    add_columns: Vec<ColumnMetadata>,
    drop_columns: Vec<String>,
    extensions: HashMap<String, String>,
    /// `(predicate that must match NO row, the refusal if one does)`.
    validations: Vec<(String, BackendMessage)>,
    /// Columns added with a DEFAULT or NOT NULL: refused if the table has rows.
    needs_empty: Vec<String>,
}

/// Plan and apply `ops` on `meta` as one schema change.
pub(crate) async fn execute_actions(
    env: &DdlEnv<'_>,
    executor: &dyn DdlExecutor,
    keyspace: &str,
    meta: TableMetadata,
    ops: Vec<AlterOperation>,
) -> Vec<BackendMessage> {
    let table = meta.name.clone(); // the plan takes `meta`; the name is needed after it
    let mut plan = Plan {
        meta,
        add_columns: Vec::new(),
        drop_columns: Vec::new(),
        extensions: HashMap::new(),
        validations: Vec::new(),
        needs_empty: Vec::new(),
    };
    for op in ops {
        if let Err(refusal) = plan_action(env, keyspace, &mut plan, op) {
            return vec![refusal];
        }
    }
    if let Err(refusal) = validate(env, keyspace, &table, &mut plan).await {
        return vec![refusal];
    }
    let updates = TableUpdates {
        params: None,
        add_columns: plan.add_columns,
        drop_columns: plan.drop_columns,
        extensions: (!plan.extensions.is_empty()).then_some(plan.extensions),
    };
    let changes_nothing = updates.add_columns.is_empty()
        && updates.drop_columns.is_empty()
        && updates.extensions.is_none();
    if changes_nothing {
        return complete();
    }
    match executor.alter_table(keyspace, &table, updates).await {
        Ok(()) => complete(),
        Err(error) => vec![error_response(
            "58000",
            &format!("ALTER TABLE failed: {error}"),
        )],
    }
}

fn complete() -> Vec<BackendMessage> {
    vec![BackendMessage::CommandComplete {
        tag: "ALTER TABLE".to_string(),
    }]
}

/// Run the plan's validations against existing rows, unless the table has none yet
/// (created earlier in this transaction block).
async fn validate(
    env: &DdlEnv<'_>,
    keyspace: &str,
    table: &str,
    plan: &mut Plan,
) -> Result<(), BackendMessage> {
    let committed = env
        .schema
        .snapshot()
        .tables
        .contains_key(&(keyspace.to_string(), table.to_string()));
    if !committed || (plan.validations.is_empty() && plan.needs_empty.is_empty()) {
        return Ok(());
    }
    let Some(probe) = env.probe else {
        return Err(error_response(
            "0A000",
            "this ALTER TABLE must check the table's existing rows, and this path cannot read them",
        ));
    };
    if let Some(column) = plan.needs_empty.first() {
        if probe.any_row(keyspace, table, "TRUE").await? {
            return Err(error_response(
                "0A000",
                &format!(
                    "adding column \"{column}\" with a DEFAULT or NOT NULL to a table that has \
                     rows is not supported: ferrosa does not backfill existing rows"
                ),
            ));
        }
    }
    for (predicate, refusal) in std::mem::take(&mut plan.validations) {
        if probe.any_row(keyspace, table, &predicate).await? {
            return Err(refusal);
        }
    }
    Ok(())
}

fn plan_action(
    env: &DdlEnv<'_>,
    keyspace: &str,
    plan: &mut Plan,
    op: AlterOperation,
) -> Result<(), BackendMessage> {
    match op {
        AlterOperation::AddColumn(def) => add_column(env, keyspace, plan, def, false),
        AlterOperation::AddColumnIfNotExists(def) => add_column(env, keyspace, plan, def, true),
        AlterOperation::DropColumn(name) => drop_column(plan, name),
        AlterOperation::AlterColumnType {
            column,
            ty,
            user_type,
        } => alter_type(keyspace, plan, &column, ty, user_type.as_ref()),
        AlterOperation::SetDefault { column, sql } => {
            let def = existing_column(plan, &column)?;
            check_enum_default(env, &plan.meta, &def.name, &sql)?;
            let (k, v) = crate::pg_default::default_extension(&column, &sql);
            set_extension(plan, k, v);
            Ok(())
        }
        AlterOperation::DropDefault { column } => {
            existing_column(plan, &column)?;
            let (k, v) = crate::pg_default::default_extension(&column, "");
            set_extension(plan, k, v);
            Ok(())
        }
        AlterOperation::SetNotNull { column } => set_not_null(plan, column),
        AlterOperation::DropNotNull { column } => drop_not_null(plan, column),
        AlterOperation::AddCheck {
            name,
            sql,
            not_valid,
        } => add_check(plan, name, sql, not_valid),
        AlterOperation::DropConstraint { name, if_exists } => {
            drop_constraint(plan, name, if_exists)
        }
        AlterOperation::SetLogged(logged) => {
            let value = if logged { "" } else { "true" };
            set_extension(plan, "pg.unlogged".to_string(), value.to_string());
            Ok(())
        }
        AlterOperation::AddPrimaryKey(_) | AlterOperation::AddForeignKey(_) => Err(error_response(
            "0A000",
            "ADD PRIMARY KEY / ADD FOREIGN KEY must be the only action of their ALTER TABLE",
        )),
    }
}

/// Record an extension in the plan and in its working copy of the table.
fn set_extension(plan: &mut Plan, key: String, value: String) {
    // The working copy must see the change for the plan's later actions.
    plan.meta.extensions.insert(key.clone(), value.clone());
    plan.extensions.insert(key, value);
}

fn existing_column(plan: &Plan, column: &str) -> Result<ColumnMetadata, BackendMessage> {
    plan.meta.columns.get(column).cloned().ok_or_else(|| {
        error_response(
            "42703",
            &format!(
                "column \"{column}\" of relation \"{}\" does not exist",
                plan.meta.name
            ),
        )
    })
}

fn is_key(meta: &TableMetadata, column: &str) -> bool {
    crate::pg_key::storage_key_columns(meta)
        .iter()
        .any(|c| c == column)
}

fn add_column(
    env: &DdlEnv<'_>,
    keyspace: &str,
    plan: &mut Plan,
    def: ColumnDef,
    if_not_exists: bool,
) -> Result<(), BackendMessage> {
    if plan.meta.columns.contains_key(&def.name) {
        if if_not_exists {
            return Ok(());
        }
        return Err(error_response(
            "42701",
            &format!(
                "column \"{}\" of relation \"{}\" already exists",
                def.name, plan.meta.name
            ),
        ));
    }
    if ferrosa_common::timeuuid::is_reserved_column_name(&def.name) {
        return Err(error_response(
            "42P16",
            &format!("column name \"{}\" is reserved", def.name),
        ));
    }
    if def.primary_key || def.serial {
        return Err(error_response(
            "0A000",
            "ADD COLUMN cannot add a key or bigserial column: re-create the table",
        ));
    }
    let column_type = crate::ddl::cql_type_string(&def)?;
    for (k, v) in crate::ddl::user_type_extensions(&env.view, keyspace, std::slice::from_ref(&def))?
    {
        set_extension(plan, k, v);
    }
    if let Some(sql) = &def.default {
        check_enum_default(env, &plan.meta, &def.name, sql)?;
        let (k, v) = crate::pg_default::default_extension(&def.name, sql);
        set_extension(plan, k, v);
    }
    if def.not_null {
        let (k, v) = crate::pg_constraint::not_null_extension(&def.name);
        set_extension(plan, k, v);
    }
    if def.default.is_some() || def.not_null {
        plan.needs_empty.push(def.name.clone()); // named in the refusal, if one comes
    }
    let column = ColumnMetadata {
        name: def.name,
        kind: ColumnKind::Regular,
        position: 0,
        column_type: column_type.to_string(),
        clustering_order: ClusteringOrder::None,
        mask: None,
    };
    // The working copy sees the column for the plan's later actions; the update adds it.
    plan.meta
        .columns
        .insert(column.name.clone(), column.clone());
    plan.add_columns.push(column);
    Ok(())
}

fn drop_column(plan: &mut Plan, name: String) -> Result<(), BackendMessage> {
    existing_column(plan, &name)?;
    if is_key(&plan.meta, &name) || crate::pg_key::of(&plan.meta).contains(&name) {
        return Err(error_response(
            "2BP01",
            &format!("cannot drop column \"{name}\" because the primary key depends on it"),
        ));
    }
    plan.meta.columns.shift_remove(&name);
    plan.drop_columns.push(name);
    Ok(())
}

fn alter_type(
    keyspace: &str,
    plan: &Plan,
    column: &str,
    ty: ferrosa_sql::PgType,
    user_type: Option<&ferrosa_sql::ast::TableRef>,
) -> Result<(), BackendMessage> {
    let current = existing_column(plan, column)?;
    let same = match user_type {
        Some(name) => {
            let ks = name.schema.as_deref().unwrap_or(keyspace);
            crate::pg_enum::column_enum(&plan.meta, column) == Some((ks, name.table.as_str()))
        }
        None => {
            let def = ColumnDef {
                name: column.to_string(),
                ty,
                not_null: false,
                primary_key: false,
                user_type: None,
                default: None,
                serial: false,
            };
            crate::pg_enum::column_enum(&plan.meta, column).is_none()
                && crate::ddl::cql_type_string(&def).is_ok_and(|t| t == current.column_type)
        }
    };
    if same {
        return Ok(()); // the column already has this type: a no-op
    }
    Err(error_response(
        "0A000",
        &format!(
            "changing the type of column \"{column}\" (currently {}) is not supported: stored \
             values would have to be rewritten",
            current.column_type
        ),
    ))
}

fn set_not_null(plan: &mut Plan, column: String) -> Result<(), BackendMessage> {
    existing_column(plan, &column)?;
    if is_key(&plan.meta, &column) {
        return Ok(()); // key columns are NOT NULL by construction
    }
    plan.validations.push((
        format!("{} IS NULL", quote_ident(&column)),
        error_response(
            "23502",
            &format!(
                "column \"{column}\" of relation \"{}\" contains null values",
                plan.meta.name
            ),
        ),
    ));
    let (k, v) = crate::pg_constraint::not_null_extension(&column);
    set_extension(plan, k, v);
    Ok(())
}

fn drop_not_null(plan: &mut Plan, column: String) -> Result<(), BackendMessage> {
    existing_column(plan, &column)?;
    if is_key(&plan.meta, &column) {
        return Err(error_response(
            "42P16",
            &format!("column \"{column}\" is in a primary key"),
        ));
    }
    let (k, _) = crate::pg_constraint::not_null_extension(&column);
    set_extension(plan, k, String::new());
    Ok(())
}

fn add_check(
    plan: &mut Plan,
    name: Option<String>,
    sql: String,
    not_valid: bool,
) -> Result<(), BackendMessage> {
    let name = name.unwrap_or_else(|| format!("{}_check", plan.meta.name));
    if crate::pg_constraint::has_check(&plan.meta, &name) {
        return Err(error_response(
            "42710",
            &format!(
                "constraint \"{name}\" for relation \"{}\" already exists",
                plan.meta.name
            ),
        ));
    }
    if !not_valid {
        plan.validations.push((
            format!("NOT ({sql})"),
            error_response(
                "23514",
                &format!(
                    "check constraint \"{name}\" of relation \"{}\" is violated by some row",
                    plan.meta.name
                ),
            ),
        ));
    }
    let (k, v) = crate::pg_constraint::check_extension(&name, &sql);
    set_extension(plan, k, v);
    Ok(())
}

fn drop_constraint(plan: &mut Plan, name: String, if_exists: bool) -> Result<(), BackendMessage> {
    if crate::pg_constraint::has_check(&plan.meta, &name) {
        let (k, _) = crate::pg_constraint::check_extension(&name, "");
        set_extension(plan, k, String::new());
        return Ok(());
    }
    if plan
        .meta
        .extensions
        .contains_key(&crate::pg_fk::extension_key(&name))
        || name == format!("{}_pkey", plan.meta.name)
    {
        return Err(error_response(
            "0A000",
            &format!("dropping key or foreign-key constraint \"{name}\" is not supported"),
        ));
    }
    if if_exists {
        return Ok(());
    }
    Err(error_response(
        "42704",
        &format!(
            "constraint \"{name}\" of relation \"{}\" does not exist",
            plan.meta.name
        ),
    ))
}

/// A string-literal DEFAULT on an enum column must be one of its labels, as PostgreSQL
/// checks when it casts the default to the column type.
pub(crate) fn check_enum_default(
    env: &DdlEnv<'_>,
    meta: &TableMetadata,
    column: &str,
    sql: &str,
) -> Result<(), BackendMessage> {
    let Some((ks, name)) = crate::pg_enum::column_enum(meta, column) else {
        return Ok(());
    };
    let Some(label) = string_literal(sql) else {
        return Ok(()); // a non-literal default is cast at INSERT time
    };
    crate::pg_enum::cast_text_to_enum(&env.view, ks, name, &label).map(|_| ())
}

/// The text of a canonical SQL string literal (`'it''s'` → `it's`), if `sql` is one.
fn string_literal(sql: &str) -> Option<String> {
    let inner = sql.strip_prefix('\'')?.strip_suffix('\'')?;
    if inner.replace("''", "").contains('\'') {
        return None; // more than one literal, e.g. 'a' || 'b'
    }
    Some(inner.replace("''", "'"))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
