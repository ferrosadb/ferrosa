//! Parsing of general `UPDATE` / `DELETE` (lane W2) and of row-locking clauses.
//!
//! The statements are the Oban census's (`ferrosa-postgres/tests/fixtures/oban_census.sql`).
//! A statement the keyed legacy grammar can carry must still parse to the legacy
//! `Statement::Update` / `Statement::Delete`, so pgbench keeps its fast path.

use ferrosa_sql::{
    parse_statement, DmlAction, DmlStmt, LockStrength, LockWait, ParseError, Projection,
    SelectItem, Statement,
};

const CLAIM: &str = r#"WITH "subset" AS (SELECT so0."id" AS "id" FROM "public"."oban_jobs" AS so0 WHERE (so0."state" = 'available') AND (so0."queue" = $1) ORDER BY so0."priority", so0."scheduled_at", so0."id" LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE "public"."oban_jobs" AS o0 SET "state" = $3, "attempted_at" = $4, "attempted_by" = $5, "attempt" = o0."attempt" + $6 FROM "subset" AS f1 WHERE (o0."id" = f1."id") AND (o0."attempt" < o0."max_attempts") RETURNING o0."id", o0."state", o0."queue", o0."worker", o0."args", o0."meta", o0."tags", o0."errors", o0."attempt", o0."attempted_by", o0."max_attempts", o0."priority", o0."attempted_at", o0."cancelled_at", o0."completed_at", o0."discarded_at", o0."inserted_at", o0."scheduled_at""#;

const ACK: &str = r#"UPDATE "public"."oban_jobs" AS o0 SET "state" = $1, "completed_at" = $2 WHERE (o0."id" = $3) AND (o0."state" = ANY($4)) AND (o0."attempted_at" = $5)"#;

fn dml(sql: &str) -> DmlStmt {
    match parse_statement(sql) {
        Ok(Statement::Dml(d)) => *d,
        other => panic!("expected a general DML statement for {sql}, got {other:?}"),
    }
}

fn sets(d: &DmlStmt) -> Vec<&str> {
    match &d.action {
        DmlAction::Update(sets) => sets.iter().map(|(c, _)| c.as_str()).collect(),
        DmlAction::Delete => panic!("expected an UPDATE"),
    }
}

#[test]
fn the_oban_claim_parses_with_its_cte_from_and_returning() {
    let d = dml(CLAIM);
    assert_eq!(d.ctes.len(), 1);
    assert_eq!(d.ctes[0].name, "subset");
    let lock = d.ctes[0]
        .query
        .lock
        .as_ref()
        .expect("FOR UPDATE is recorded");
    assert_eq!(lock.strength, LockStrength::Update);
    assert_eq!(lock.wait, LockWait::SkipLocked);
    assert_eq!(d.ctes[0].query.limit_param, Some(2));
    assert_eq!(d.target.schema.as_deref(), Some("public"));
    assert_eq!(d.target.table, "oban_jobs");
    assert_eq!(d.target.alias.as_deref(), Some("o0"));
    assert_eq!(
        sets(&d),
        ["state", "attempted_at", "attempted_by", "attempt"]
    );
    assert_eq!(d.from.len(), 1);
    assert_eq!(d.from[0].table, "subset");
    assert_eq!(d.from[0].alias.as_deref(), Some("f1"));
    assert!(d.filter.is_some());
    match &d.returning {
        Some(Projection::Items(items)) => assert_eq!(items.len(), 18),
        other => panic!("expected 18 RETURNING items, got {other:?}"),
    }
    assert_eq!(d.verb(), "UPDATE");
}

#[test]
fn the_oban_ack_is_general_because_of_any() {
    let d = dml(ACK);
    assert_eq!(d.target.alias.as_deref(), Some("o0"));
    assert_eq!(sets(&d), ["state", "completed_at"]);
    assert!(d.from.is_empty() && d.ctes.is_empty() && d.returning.is_none());
}

#[test]
fn a_plugin_delete_with_an_in_subquery_parses() {
    let d = dml("DELETE FROM public.oban_jobs WHERE id IN \
         (SELECT id FROM public.oban_jobs WHERE state = 'completed' LIMIT 10)");
    assert_eq!(d.action, DmlAction::Delete);
    assert_eq!(d.verb(), "DELETE");
    assert!(d.filter.is_some());
}

#[test]
fn a_plugin_update_with_array_append_parses() {
    let d = dml("UPDATE oban_jobs AS o0 SET state = 'available', \
         errors = array_append(o0.errors, $1) \
         WHERE o0.id IN (SELECT id FROM oban_jobs WHERE state = 'executing' LIMIT $2)");
    assert_eq!(sets(&d), ["state", "errors"]);
}

#[test]
fn keyed_shapes_keep_the_legacy_statements() {
    assert!(matches!(
        parse_statement("UPDATE t SET a = a + 1 WHERE k = 1"),
        Ok(Statement::Update(_))
    ));
    assert!(matches!(
        parse_statement("UPDATE pgbench_accounts SET abalance = abalance + $1 WHERE aid = $2"),
        Ok(Statement::Update(_))
    ));
    assert!(matches!(
        parse_statement("DELETE FROM t WHERE k = $1"),
        Ok(Statement::Delete(_))
    ));
}

#[test]
fn returning_on_update_or_delete_is_general() {
    let d = dml("UPDATE t SET a = 1 WHERE k = 1 RETURNING a, a + 1 AS b");
    match &d.returning {
        Some(Projection::Items(items)) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[1].alias(), Some("b"));
            assert!(matches!(items[1], SelectItem::Expr { .. }));
        }
        other => panic!("expected items, got {other:?}"),
    }
    let d = dml("DELETE FROM t WHERE k = 1 RETURNING *");
    assert_eq!(d.returning, Some(Projection::Star));
}

#[test]
fn update_and_delete_without_where_are_general() {
    assert!(dml("UPDATE t SET a = 1").filter.is_none());
    assert!(dml("DELETE FROM t").filter.is_none());
}

#[test]
fn delete_using_and_aliases_parse() {
    let d = dml("DELETE FROM t AS x USING u, v WHERE x.k = u.k AND u.k = v.k");
    assert_eq!(d.target.alias.as_deref(), Some("x"));
    assert_eq!(d.from.len(), 2);
    let d = dml("DELETE FROM t x WHERE x.a > 3");
    assert_eq!(d.target.alias.as_deref(), Some("x"));
}

#[test]
fn with_delete_parses() {
    let d = dml(
        "WITH old AS (SELECT k FROM t WHERE a < 0) DELETE FROM t WHERE k IN (SELECT k FROM old)",
    );
    assert_eq!(d.ctes.len(), 1);
    assert_eq!(d.action, DmlAction::Delete);
}

#[test]
fn read_select_reads_every_relation() {
    let d = dml(CLAIM);
    let read = d.read_select();
    let tables: Vec<&str> = read.tables().iter().map(|t| t.table.as_str()).collect();
    assert_eq!(
        tables,
        ["oban_jobs", "oban_jobs"],
        "the CTE body and the target"
    );
    let d = dml("UPDATE t SET a = (SELECT max(b) FROM u) WHERE k IN (SELECT k FROM v) RETURNING (SELECT 1 FROM w LIMIT 1)");
    let read = d.read_select();
    let mut tables: Vec<&str> = read.tables().iter().map(|t| t.table.as_str()).collect();
    tables.sort_unstable();
    assert_eq!(tables, ["t", "u", "v", "w"]);
}

fn refused(sql: &str) -> &'static str {
    match parse_statement(sql) {
        Err(ParseError::UnsupportedDml(what)) => what,
        other => panic!("expected an UnsupportedDml refusal for {sql}, got {other:?}"),
    }
}

#[test]
fn unsupported_forms_are_refused_by_name() {
    assert_eq!(refused("UPDATE t SET (a, b) = (1, 2)"), "SET (column list)");
    assert_eq!(refused("UPDATE t SET a = DEFAULT"), "SET column = DEFAULT");
    assert_eq!(
        refused("UPDATE t SET a = 1 FROM u JOIN v ON u.k = v.k"),
        "JOIN in UPDATE … FROM / DELETE … USING"
    );
    assert_eq!(
        refused("UPDATE t SET a = 1 WHERE CURRENT OF c"),
        "WHERE CURRENT OF"
    );
    assert_eq!(refused("UPDATE ONLY t SET a = 1"), "ONLY");
}

fn lock_of(sql: &str) -> ferrosa_sql::RowLock {
    match parse_statement(sql) {
        Ok(Statement::Select(s)) => s.lock.expect("lock recorded"),
        other => panic!("expected a SELECT for {sql}, got {other:?}"),
    }
}

#[test]
fn row_locking_clauses_are_recorded() {
    let l = lock_of("SELECT k FROM t FOR UPDATE");
    assert_eq!((l.strength, l.wait), (LockStrength::Update, LockWait::Wait));
    let l = lock_of("SELECT k FROM t WHERE k = 1 FOR UPDATE NOWAIT");
    assert_eq!(l.wait, LockWait::NoWait);
    let l = lock_of("SELECT k FROM t FOR NO KEY UPDATE SKIP LOCKED");
    assert_eq!(
        (l.strength, l.wait),
        (LockStrength::NoKeyUpdate, LockWait::SkipLocked)
    );
    let l = lock_of("SELECT k FROM t AS x FOR SHARE OF x");
    assert_eq!(
        (l.strength, l.of.as_slice()),
        (LockStrength::Share, ["x".to_string()].as_slice())
    );
    let l = lock_of("SELECT k FROM t FOR KEY SHARE");
    assert_eq!(l.strength, LockStrength::KeyShare);
    // Either order with LIMIT, as PostgreSQL accepts.
    let l = lock_of("SELECT k FROM t LIMIT 1 FOR UPDATE");
    assert_eq!(l.strength, LockStrength::Update);
    match parse_statement("SELECT k FROM t FOR UPDATE LIMIT 3") {
        Ok(Statement::Select(s)) => {
            assert_eq!(s.limit, Some(3));
            assert!(s.lock.is_some());
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        parse_statement("SELECT k FROM t").map(|s| match s {
            Statement::Select(s) => s.lock,
            _ => panic!(),
        }),
        Ok(None)
    ));
}
