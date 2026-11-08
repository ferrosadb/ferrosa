//! Module: `LOCK TABLE` — Ecto's migration lock, given real semantics.
//!
//! Ecto takes `LOCK TABLE "schema_migrations" IN SHARE UPDATE EXCLUSIVE MODE` at the start
//! of every migration transaction so that two nodes migrating at once run one after the
//! other. Accepting it as a no-op would let both migrate concurrently. What it does here:
//!
//! * **Only inside a transaction block** (`25P01` otherwise, as PostgreSQL), and the table
//!   must exist (`42P01`), checked before anything is taken.
//! * **Standalone (Direct) node: a real, blocking lock.** The transaction takes local
//!   writer ownership of every stripe of the table (`mvcc::writer_ownership`, the machinery
//!   `UPDATE`/`DELETE`/`TRUNCATE` use), held until `COMMIT`/`ROLLBACK`. A second `LOCK` of the
//!   table, and any writer of it, waits; a cycle is a deadlock (`40P01`). Every mode is
//!   treated the same, which is **stronger** than PostgreSQL for the weaker modes:
//!   `SHARE UPDATE EXCLUSIVE` does not block plain `INSERT`s there, and does here. Readers
//!   are never blocked (reads take no ownership).
//! * **Every node: the table enters the transaction's read set**, so a serializable
//!   transaction — every transaction block on a cluster, which runs on Accord (D11) — fails
//!   at `COMMIT` with `40001` if another transaction wrote the table after it began. On a
//!   cluster that is the whole lock: optimistic, not blocking. Two concurrent migrators
//!   both proceed, and the one that commits second is refused, which is what Ecto's lock is
//!   for (the migration runs once). Writer ownership is node-local, so it is not taken on a
//!   cluster, where it would protect nothing.
//! * `NOWAIT` is refused at parse time (the Direct lock waits).
//!
//! Last revised: 2026-11-08

use ferrosa_sql::LockStmt;

use crate::extended::Session;
use crate::messages::BackendMessage;
use crate::mvcc::writer_ownership::STRIPES;
use crate::query::error_response;
use crate::server::QueryContext;

/// Execute `LOCK TABLE` for `session`. An error leaves the caller to abort the block.
pub(crate) async fn lock_tables(
    ctx: &QueryContext,
    session: &mut Session,
    stmt: &LockStmt,
) -> Vec<BackendMessage> {
    if !session.in_txn() {
        return vec![error_response(
            "25P01",
            "LOCK TABLE can only be used in transaction blocks",
        )];
    }
    let targets = match resolve(ctx, stmt) {
        Ok(targets) => targets,
        Err(error) => return vec![error],
    };
    for (name, id) in targets {
        if let Err(error) = lock_one(ctx, session, name, id).await {
            return vec![error];
        }
    }
    vec![BackendMessage::CommandComplete {
        tag: "LOCK TABLE".to_string(),
    }]
}

/// Resolve every named table to its qualified name and incarnation before taking anything,
/// so a missing table takes no lock.
fn resolve(
    ctx: &QueryContext,
    stmt: &LockStmt,
) -> Result<Vec<(String, uuid::Uuid)>, BackendMessage> {
    let snapshot = ctx.schema.snapshot();
    stmt.tables
        .iter()
        .map(|t| {
            let keyspace = t.schema.as_deref().unwrap_or(&ctx.default_schema);
            snapshot
                .tables
                // The table map is keyed by an owned pair, so a lookup needs one.
                .get(&(keyspace.to_string(), t.table.clone()))
                .map(|meta| (format!("{keyspace}.{}", t.table), meta.id))
                .ok_or_else(|| {
                    error_response("42P01", &format!("relation \"{}\" does not exist", t.table))
                })
        })
        .collect()
}

async fn lock_one(
    ctx: &QueryContext,
    session: &mut Session,
    name: String,
    id: uuid::Uuid,
) -> Result<(), BackendMessage> {
    if ctx.accord.committer().is_none() {
        let writers = &mut session.txn_writes_mut().writers;
        for slot in 0..STRIPES {
            writers.acquire(&ctx.mvcc.writers, id, slot).await?;
        }
    }
    if let Some(snapshot) = session.txn_snapshot() {
        ctx.mvcc
            .bind_relation(snapshot, &name, id)
            .map_err(|_| error_response("40001", "table incarnation changed during transaction"))?;
    }
    session.txn_read_tables_mut().insert(name);
    Ok(())
}
