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
//! | `TRUNCATE [TABLE] t [, …]` | `MODIFY` on EACH `t` (the CQL router's rule) |
//! | `VACUUM …`, `ANALYZE …` | nothing — a deliberate no-op touches no table |
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
        // DROP TABLE drops one or more tables, so it needs DROP on EACH table.
        // Checked per table here (before the executor looks at the schema) so a
        // denial reveals nothing about whether a given table exists.
        Statement::DropTable(drop) => drop
            .tables
            .iter()
            .map(|t| (Permission::Drop, table(t, default_schema)))
            .collect(),
        // ADD PRIMARY KEY alters the table — and may create an index over the new key — so it
        // needs ALTER, the same permission CQL's ALTER TABLE requires.
        Statement::AlterTable(alter) => {
            vec![(Permission::Alter, table(&alter.table, default_schema))]
        }
        // COPY FROM STDIN writes rows, so it needs MODIFY — the same permission an INSERT needs.
        Statement::CopyFromStdin(copy) => {
            vec![(Permission::Modify, table(&copy.table, default_schema))]
        }
        // TRUNCATE removes every row of each named table, so it needs MODIFY on
        // EACH table — the SAME permission the CQL `TRUNCATE` router requires
        // (`ferrosa-cql/src/router.rs::route_truncate`). Checked per table here,
        // before the executor looks at the schema, so a denial reveals nothing
        // about whether a table exists, and the two front ends cannot disagree
        // about who may empty a table.
        Statement::Truncate(truncate) => truncate
            .tables
            .iter()
            .map(|t| (Permission::Modify, table(t, default_schema)))
            .collect(),
        // VACUUM and ANALYZE are deliberate no-ops that touch no data and read no
        // rows, so they need no table permission — the same treatment a session
        // statement gets.
        Statement::Vacuum(_) | Statement::Analyze(_) => Vec::new(),
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

    #[test]
    fn truncate_needs_modify_on_every_named_table() {
        // Parity with the CQL router's `route_truncate` (Permission::Modify).
        assert_eq!(
            perms("TRUNCATE TABLE kv"),
            vec![(Permission::Modify, t("public", "kv"))]
        );
        assert_eq!(
            perms("TRUNCATE public.a, b"),
            vec![
                (Permission::Modify, t("public", "a")),
                (Permission::Modify, t("public", "b")),
            ]
        );
    }

    #[test]
    fn vacuum_and_analyze_require_no_table_permission() {
        for sql in [
            "VACUUM",
            "VACUUM FULL",
            "VACUUM ANALYZE kv",
            "ANALYZE",
            "ANALYSE kv",
        ] {
            assert!(perms(sql).is_empty(), "{sql}");
        }
    }
}
