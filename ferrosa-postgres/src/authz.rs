//! Statement authorization for the PostgreSQL front-end (t_e1c819ad).
//!
//! Every statement is checked against the SAME permission model the CQL router
//! uses (`Schema::check_permission`: grants on `ALL KEYSPACES` / keyspace /
//! table, role inheritance, superuser bypass). A PostgreSQL schema is a
//! Ferrosa keyspace, so `public.kv` is `Resource::Table("public", "kv")`.
//!
//! | statement | required |
//! |---|---|
//! | `SELECT … FROM t [JOIN u]` | `SELECT` on each table read |
//! | `INSERT` / `UPDATE` / `DELETE` on `t` | `MODIFY` on `t` |
//! | … `RETURNING` | additionally `SELECT` on `t` (the row is read back) |
//! | `SELECT <exprs>` (no `FROM`), `BEGIN`/`COMMIT`/`ROLLBACK`, `SET`/`RESET` | nothing — no table is touched |
//!
//! | `CREATE TABLE` | `CREATE` on the target keyspace |
//!
//! The mapping
//! matches on every statement kind with no wildcard arm, so adding a kind to
//! `ferrosa_sql::Statement` or [`PreparedKind`] does not compile until it is
//! given an explicit rule — an unmapped kind can never run unchecked.
//!
//! A denial is SQLSTATE `42501` `insufficient_privilege`.

use ferrosa_schema::auth::permission::{Permission, Resource};
use ferrosa_schema::{AuthContext, Schema};
use ferrosa_sql::ast::TableRef;
use ferrosa_sql::{SelectStmt, Statement};

use crate::extended::PreparedKind;
use crate::messages::BackendMessage;
use crate::query;

/// SQLSTATE `insufficient_privilege`.
pub(crate) const INSUFFICIENT_PRIVILEGE: &str = "42501";

fn table(table: &TableRef, default_schema: &str) -> Resource {
    Resource::Table(
        table
            .schema
            .clone()
            .unwrap_or_else(|| default_schema.to_string()),
        table.table.clone(),
    )
}

fn select_reads(select: &SelectStmt, default_schema: &str) -> Vec<(Permission, Resource)> {
    let mut required = vec![(Permission::Select, table(&select.from, default_schema))];
    if let Some(join) = &select.join {
        required.push((Permission::Select, table(&join.table, default_schema)));
    }
    required
}

fn dml(target: &TableRef, returning: bool, default_schema: &str) -> Vec<(Permission, Resource)> {
    let resource = table(target, default_schema);
    let mut required = vec![(Permission::Modify, resource.clone())];
    if returning {
        required.push((Permission::Select, resource));
    }
    required
}

/// Permissions a simple-query statement needs.
pub(crate) fn statement_permissions(
    stmt: &Statement,
    default_schema: &str,
) -> Vec<(Permission, Resource)> {
    match stmt {
        Statement::Select(select) => select_reads(select, default_schema),
        Statement::Insert(ins) => dml(&ins.table, ins.returning.is_some(), default_schema),
        Statement::Update(upd) => dml(&upd.table, upd.returning.is_some(), default_schema),
        Statement::Delete(del) => dml(&del.table, del.returning.is_some(), default_schema),
        // DDL (T-132a, t_d3930503): CREATE on the target keyspace, as the CQL
        // router requires for CREATE TABLE. Checked at dispatch, before the
        // executor looks at the schema, so a denial reveals nothing about
        // whether the keyspace or table exists.
        Statement::CreateTable(create) => vec![(
            Permission::Create,
            Resource::Keyspace(
                create
                    .name
                    .schema
                    .clone()
                    .unwrap_or_else(|| default_schema.to_string()),
            ),
        )],
        Statement::SelectExprs(_)
        | Statement::Begin { .. }
        | Statement::Commit
        | Statement::Rollback
        | Statement::Set { .. }
        | Statement::Reset { .. } => Vec::new(),
    }
}

/// Permissions an extended-protocol prepared statement needs.
pub(crate) fn prepared_permissions(
    kind: &PreparedKind,
    default_schema: &str,
) -> Vec<(Permission, Resource)> {
    match kind {
        PreparedKind::Select(select) => select_reads(select, default_schema),
        PreparedKind::Insert(ins) => dml(&ins.table, ins.returning.is_some(), default_schema),
        PreparedKind::Update(upd) => dml(&upd.table, upd.returning.is_some(), default_schema),
        PreparedKind::Delete(del) => dml(&del.table, del.returning.is_some(), default_schema),
        PreparedKind::Exprs(_) => Vec::new(),
    }
}

/// Check every required permission; the first denial becomes a `42501`
/// `ErrorResponse` naming the role, permission and resource.
pub(crate) fn authorize(
    schema: &Schema,
    auth: &AuthContext,
    required: &[(Permission, Resource)],
) -> Result<(), BackendMessage> {
    for (permission, resource) in required {
        if let Err(error) = schema.check_permission(auth, *permission, resource) {
            tracing::info!(
                role = %auth.role,
                %permission,
                %resource,
                "PostgreSQL statement refused: insufficient privilege"
            );
            return Err(query::error_response(
                INSUFFICIENT_PRIVILEGE,
                &error.to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn perms(sql: &str) -> Vec<(Permission, Resource)> {
        statement_permissions(&ferrosa_sql::parse_statement(sql).unwrap(), "public")
    }

    fn t(ks: &str, tbl: &str) -> Resource {
        Resource::Table(ks.into(), tbl.into())
    }

    #[test]
    fn select_needs_select_on_every_table_read() {
        assert_eq!(
            perms("SELECT a FROM kv"),
            vec![(Permission::Select, t("public", "kv"))]
        );
        assert_eq!(
            perms("SELECT u.a FROM other.users u JOIN orders o ON u.id = o.uid"),
            vec![
                (Permission::Select, t("other", "users")),
                (Permission::Select, t("public", "orders")),
            ]
        );
    }

    #[test]
    fn dml_needs_modify_and_returning_adds_select() {
        assert_eq!(
            perms("INSERT INTO kv (k, v) VALUES ('a', 'b')"),
            vec![(Permission::Modify, t("public", "kv"))]
        );
        assert_eq!(
            perms("UPDATE kv SET v = 'x' WHERE k = 'a'"),
            vec![(Permission::Modify, t("public", "kv"))]
        );
        assert_eq!(
            perms("DELETE FROM kv WHERE k = 'a'"),
            vec![(Permission::Modify, t("public", "kv"))]
        );
        assert_eq!(
            perms("INSERT INTO kv (k, v) VALUES ('a', 'b') RETURNING k"),
            vec![
                (Permission::Modify, t("public", "kv")),
                (Permission::Select, t("public", "kv")),
            ]
        );
    }

    #[test]
    fn create_table_needs_create_on_the_target_keyspace() {
        assert_eq!(
            perms("CREATE TABLE docs (id int PRIMARY KEY, doc jsonb)"),
            vec![(Permission::Create, Resource::Keyspace("public".into()))]
        );
        // Only `public` parses today (T-130 refuses other schemas by name).
        assert_eq!(
            perms("CREATE TABLE IF NOT EXISTS public.docs (id int PRIMARY KEY)"),
            vec![(Permission::Create, Resource::Keyspace("public".into()))]
        );
    }

    #[test]
    fn table_free_statements_need_nothing() {
        for sql in ["SELECT 1", "BEGIN", "COMMIT", "ROLLBACK"] {
            assert!(perms(sql).is_empty(), "{sql}");
        }
    }
}
