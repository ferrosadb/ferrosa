//! Module: PostgreSQL `TRUNCATE` over the **replicated** cluster write path.
//!
//! Responsibility: turn a parsed `Statement::Truncate` into a cluster-wide
//! removal of every row of each named table. There is exactly ONE path:
//! [`TruncateExecutor`], whose production implementation ([`ClusterTruncate`])
//! calls `ferrosa_cluster::WritePath::truncate` — the SAME replicated write path
//! the CQL router's `route_truncate` uses (`ferrosa-cql/src/router.rs`), where a
//! coordinator fans the truncate out to **all cluster nodes** in cluster mode
//! and truncates the single local engine in standalone/pair mode.
//!
//! ## Why the local `StorageEngine::truncate` is never called here
//!
//! `ferrosa_storage::StorageEngine::truncate` empties ONE node's local replica.
//! Over a 3-node cluster that leaves the other two holding the old data, so the
//! nodes then disagree about the table's contents — silent divergence, the exact
//! failure mode this codebase forbids. Routing through [`WritePath`] is what
//! makes `TRUNCATE` land on every node (or fail loud), so that is the only
//! implementation this module will call.
//!
//! ## Fail loud
//!
//! - No executor (`None`) means the front-end has no replicated write path
//!   (unit-test contexts): the statement is refused with `0A000` rather than
//!   reported as done — see [`execute_truncate`].
//! - `TRUNCATE` inside an explicit transaction block is refused with `25001`.
//!   ferrosa applies the truncate immediately through [`WritePath`]; it is not
//!   buffered with the PostgreSQL MVCC write-set and would NOT roll back, so
//!   reporting success for a statement a later `ROLLBACK` cannot undo would be a
//!   lie.
//! - A missing table is `42P01`, checked before the write so a refused
//!   statement changes nothing.
//!
//! Last revised: 2026-10-09
//! Last changed: New module; `TRUNCATE` routed through the replicated write path.

use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use ferrosa_cluster::WritePath;
use ferrosa_schema::Schema;
use ferrosa_sql::TruncateStatement;
use ferrosa_storage::TableId;

use crate::messages::BackendMessage;
use crate::query::error_response;

/// Removes every row of `keyspace.table` **across all cluster nodes**.
///
/// Implemented by [`ClusterTruncate`] (the production path). A failure is
/// reported as a message string; the caller maps it to `58000` (system error).
#[async_trait]
pub trait TruncateExecutor: Send + Sync {
    /// Truncate `keyspace.table` through the deployment's replicated write path.
    /// A missing table is the caller's concern (it checks existence first); this
    /// reports only apply failures.
    async fn truncate(&self, keyspace: &str, table: &str) -> Result<(), String>;
}

/// [`TruncateExecutor`] over the shared, atomically swappable [`WritePath`] the
/// CQL router also holds, so PostgreSQL `TRUNCATE` is replicated exactly as CQL
/// `TRUNCATE` is.
pub struct ClusterTruncate {
    path: Arc<ArcSwap<WritePath>>,
}

impl ClusterTruncate {
    pub fn new(path: Arc<ArcSwap<WritePath>>) -> Self {
        Self { path }
    }
}

#[async_trait]
impl TruncateExecutor for ClusterTruncate {
    async fn truncate(&self, keyspace: &str, table: &str) -> Result<(), String> {
        let path = self.path.load_full();
        let table_id = TableId::new(keyspace, table);
        path.truncate(&table_id).await.map_err(|e| e.to_string())
    }
}

/// What a `TRUNCATE` runs against: the replicated write path to remove rows
/// through, and the schema registry it checks each named table exists in.
#[derive(Clone, Copy)]
pub(crate) struct TruncateEnv<'a> {
    pub(crate) executor: Option<&'a dyn TruncateExecutor>,
    pub(crate) schema: &'a Schema,
    pub(crate) default_schema: &'a str,
    pub(crate) in_txn: bool,
}

fn refuse(code: &str, message: &str) -> Vec<BackendMessage> {
    vec![error_response(code, message)]
}

/// Execute `TRUNCATE [TABLE] a [, b, ...]`.
///
/// Each named table is resolved in its schema (defaulting to `env.default_schema`)
/// and truncated through the SAME replicated write path CQL `TRUNCATE` uses. A
/// table that does not exist is an error (`42P01`), checked before any write so a
/// refused statement changes nothing. Reply is `TRUNCATE TABLE` on success. Every
/// refusal is one `ErrorResponse` with a typed SQLSTATE.
pub(crate) async fn execute_truncate(
    env: TruncateEnv<'_>,
    stmt: &TruncateStatement,
) -> Vec<BackendMessage> {
    if env.in_txn {
        return refuse(
            "25001",
            "TRUNCATE cannot run inside a transaction block: ferrosa applies it immediately \
             and cannot roll it back",
        );
    }
    let Some(executor) = env.executor else {
        return refuse(
            "0A000",
            "TRUNCATE is not available: this server has no replicated write path, and a \
             node-local truncate would leave the cluster's replicas disagreeing",
        );
    };
    // Truncate in the order given. Existence is checked per table so the reply is
    // exact about which name failed, and a missing table applies nothing.
    for target in &stmt.tables {
        let keyspace = target.schema.as_deref().unwrap_or(env.default_schema);
        let key = (keyspace.to_string(), target.table.clone());
        if !env.schema.snapshot().tables.contains_key(&key) {
            return refuse(
                "42P01",
                &format!("relation \"{}\" does not exist", target.table),
            );
        }
        if let Err(error) = executor.truncate(keyspace, &target.table).await {
            return refuse(
                "58000",
                &format!("TRUNCATE failed for \"{}\": {error}", target.table),
            );
        }
    }
    vec![BackendMessage::CommandComplete {
        tag: "TRUNCATE TABLE".to_string(),
    }]
}
