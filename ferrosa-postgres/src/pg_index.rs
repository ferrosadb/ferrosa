//! Module: `CREATE INDEX` / `DROP INDEX` (Oban's migrations).
//!
//! Ferrosa's secondary indexes (`ferrosa-index`, built through `DdlPath`) cover ONE column.
//! So:
//!
//! * a **single-column, non-partial `btree`/`hash` index in ascending order** is a real
//!   secondary index (`DdlExecutor::create_index`, which waits for backfill on a standalone
//!   node and is refused on pair/cluster until replica readiness exists — the existing
//!   `t_9fba4678` gap);
//! * a **composite, partial, `DESC` or `gin` index is accepted and recorded, but builds
//!   nothing and is never used for planning**: queries that would have used it scan. This is
//!   stated here and in the README rather than pretended otherwise. Its predicate must parse.
//!
//! Every index is recorded on its table as `pg.index.<name>` = a JSON description
//! (`""` once dropped). Index names are unique per schema (`42P07`); `IF NOT EXISTS` over an
//! existing name is a no-op. `DROP INDEX` of a recorded index removes the record (and the
//! physical index, if built); the indexes behind a primary or foreign key cannot be dropped
//! (`0A000`); a missing name is `42704` unless `IF EXISTS`. Both are transactional DDL.
//! `UNIQUE`, expression keys, `CONCURRENTLY` and other methods are refused at parse time.
//!
//! Last revised: 2026-11-08

use std::collections::HashMap;

use ferrosa_schema::{SchemaSnapshot, TableUpdates};
use ferrosa_sql::{CreateIndexStmt, DropIndexStmt};

use crate::ddl::{DdlEnv, DdlExecutor};
use crate::messages::BackendMessage;
use crate::query::error_response;

const INDEX_PREFIX: &str = "pg.index.";

fn record_key(name: &str) -> String {
    format!("{INDEX_PREFIX}{name}")
}

/// The table in `keyspace` that records index `name`, if any.
fn owner(view: &SchemaSnapshot, keyspace: &str, name: &str) -> Option<String> {
    let key = record_key(name);
    view.tables
        .iter()
        .find(|((ks, _), meta)| {
            ks == keyspace && meta.extensions.get(&key).is_some_and(|v| !v.is_empty())
        })
        .map(|((_, table), _)| table.clone()) // the owner's name outlives the snapshot borrow
}

fn physical(view: &SchemaSnapshot, keyspace: &str, name: &str) -> Option<String> {
    view.indexes
        .keys()
        .find(|(ks, _, idx)| ks == keyspace && idx == name)
        .map(|(_, table, _)| table.clone()) // as above
}

async fn record(
    executor: &dyn DdlExecutor,
    keyspace: &str,
    table: &str,
    name: &str,
    value: String,
) -> Result<(), BackendMessage> {
    let updates = TableUpdates {
        params: None,
        add_columns: Vec::new(),
        drop_columns: Vec::new(),
        extensions: Some(HashMap::from([(record_key(name), value)])),
    };
    executor
        .alter_table(keyspace, table, updates)
        .await
        .map_err(|e| error_response("58000", &format!("index DDL failed: {e}")))
}

fn done(tag: &str) -> Vec<BackendMessage> {
    vec![BackendMessage::CommandComplete {
        tag: tag.to_string(),
    }]
}

/// `CREATE INDEX`.
pub(crate) async fn execute_create_index(
    env: DdlEnv<'_>,
    executor: &dyn DdlExecutor,
    stmt: CreateIndexStmt,
) -> Vec<BackendMessage> {
    let keyspace = stmt.table.schema.as_deref().unwrap_or(env.default_schema);
    let Some(meta) = env
        .view
        .tables
        .get(&(keyspace.to_string(), stmt.table.table.clone()))
    else {
        return vec![error_response(
            "42P01",
            &format!("relation \"{}\" does not exist", stmt.table.table),
        )];
    };
    if let Some((missing, _)) = stmt
        .columns
        .iter()
        .find(|(c, _)| !meta.columns.contains_key(c))
    {
        return vec![error_response(
            "42703",
            &format!("column \"{missing}\" does not exist"),
        )];
    }
    let taken = owner(&env.view, keyspace, &stmt.name).is_some()
        || physical(&env.view, keyspace, &stmt.name).is_some()
        || env
            .view
            .tables
            .contains_key(&(keyspace.to_string(), stmt.name.clone()));
    if taken {
        if stmt.if_not_exists {
            return done("CREATE INDEX");
        }
        return vec![error_response(
            "42P07",
            &format!("relation \"{}\" already exists", stmt.name),
        )];
    }
    let builds = stmt.columns.len() == 1
        && !stmt.columns[0].1
        && stmt.predicate.is_none()
        && stmt.method != "gin";
    let description = serde_json::json!({
        "method": stmt.method,
        "columns": stmt.columns,
        "predicate": stmt.predicate,
        "physical": builds,
    })
    .to_string();
    let table = stmt.table.table;
    if let Err(e) = record(executor, keyspace, &table, &stmt.name, description).await {
        return vec![e];
    }
    if builds {
        let column = [stmt.columns[0].0.clone()]; // the executor takes a column slice
        if let Err(e) = executor
            .create_index(keyspace, &table, &stmt.name, &column)
            .await
        {
            return vec![error_response(
                "58000",
                &format!("the index was recorded but building it failed: {e}"),
            )];
        }
    }
    done("CREATE INDEX")
}

/// `DROP INDEX`.
pub(crate) async fn execute_drop_index(
    env: DdlEnv<'_>,
    executor: &dyn DdlExecutor,
    stmt: DropIndexStmt,
) -> Vec<BackendMessage> {
    for target in stmt.names {
        let keyspace = target.schema.as_deref().unwrap_or(env.default_schema);
        let name = target.table;
        let Some(table) = owner(&env.view, keyspace, &name) else {
            if physical(&env.view, keyspace, &name).is_some() {
                return vec![error_response(
                    "0A000",
                    &format!(
                        "index \"{name}\" backs a primary or foreign key and cannot be dropped"
                    ),
                )];
            }
            if stmt.if_exists {
                continue;
            }
            return vec![error_response(
                "42704",
                &format!("index \"{name}\" does not exist"),
            )];
        };
        if physical(&env.view, keyspace, &name).is_some() {
            if let Err(e) = executor.drop_index(keyspace, &table, &name).await {
                return vec![error_response("58000", &format!("DROP INDEX failed: {e}"))];
            }
        }
        if let Err(e) = record(executor, keyspace, &table, &name, String::new()).await {
            return vec![e];
        }
    }
    done("DROP INDEX")
}
