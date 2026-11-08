//! Module: transactional DDL — schema changes buffered inside `BEGIN … COMMIT`.
//!
//! Responsibility: Ecto runs every migration inside a transaction block, so a
//! `CREATE TABLE`, `ALTER TABLE` or `CREATE INDEX` arrives inside `BEGIN`. Ferrosa
//! replicates a schema change as one operation at a time through
//! `ferrosa_cluster::ddl_path::DdlPath` (one Raft entry each in cluster mode), so a
//! transaction's DDL is **buffered** here and **replayed at `COMMIT`**:
//!
//! * each DDL statement inside the block is planned against the *view* — the committed
//!   schema with the statements buffered so far applied ([`DdlBatch::view`]); a statement
//!   the view refuses is refused to the client and never buffered;
//! * the committed schema does not change before `COMMIT`; `ROLLBACK`, or a failed
//!   statement, drops the batch with the rest of the transaction (it lives in
//!   [`crate::txn_write_set::TxnWriteSet`]);
//! * at `COMMIT` the batch is first **preflighted** against the schema as it is *now*
//!   ([`preflight`]): if a concurrent session changed it so a buffered step no longer
//!   applies, the transaction fails with `40001` and **nothing** is applied;
//! * then the steps are replayed in order through the deployment's schema-change path
//!   ([`replay`]).
//!
//! Failure semantics, stated plainly (Lean: `Ferrosa.Sql.Ddl`). The replay is a sequence
//! of independent replicated schema changes, so it is atomic only when every step
//! succeeds. If step `k` of `n` fails at replay (the schema-change path became
//! unavailable, or a concurrent change landed between the preflight and that step), steps
//! `1 … k-1` **have been applied and are not withdrawn**: compensating them (dropping a
//! table another session may already be writing into) would be a second non-atomic
//! sequence with its own partial failures. The client gets `58000` naming exactly the
//! applied steps, the failed step with its cause, and the steps never run; the same is
//! logged at `error`. The transaction's row writes are then not committed. The outcome is
//! never a silent partial application (`failed_commit_reports_the_exact_prefix`).
//!
//! DDL is applied **before** the transaction's row writes, because those may target a
//! table the batch creates. If the row writes then fail to commit (e.g. `40001`), the
//! schema changes stay applied, and the error says so.
//!
//! DML inside the block runs against the *committed* schema: a table created earlier in
//! the same block does not exist for an `INSERT` yet (`42P01`).
//!
//! Correctness: [`apply_step`] is the Lean model's `apply` (a partial function on the
//! schema); [`replay`] is its `commit`.
//! Last revised: 2026-11-08

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ferrosa_schema::{Schema, SchemaSnapshot, TableMetadata, TableUpdates};

use crate::ddl::DdlExecutor;
use crate::messages::BackendMessage;
use crate::query::error_response;

/// The most DDL statements one transaction may buffer. Oban's whole v1→v12 migration is
/// about forty; the bound keeps a runaway client from growing the session without limit.
pub(crate) const MAX_BUFFERED_DDL: usize = 4096;

/// One buffered schema change: exactly what will be sent to the schema-change path.
#[derive(Debug, Clone)]
pub(crate) enum DdlStep {
    CreateTable(Box<TableMetadata>),
    DropTable {
        keyspace: String,
        table: String,
    },
    AlterTable {
        keyspace: String,
        table: String,
        /// The incarnation the change was planned against; a table dropped and
        /// re-created since then is a different table and the step no longer applies.
        id: uuid::Uuid,
        /// Boxed: `TableUpdates` is far larger than the other variants.
        updates: Box<TableUpdates>,
    },
    CreateIndex {
        keyspace: String,
        table: String,
        name: String,
        columns: Vec<String>,
    },
    DropIndex {
        keyspace: String,
        table: String,
        name: String,
    },
}

impl DdlStep {
    /// What the step does, for the COMMIT report and the log.
    pub(crate) fn label(&self) -> String {
        match self {
            DdlStep::CreateTable(meta) => format!("CREATE TABLE {}.{}", meta.keyspace, meta.name),
            DdlStep::DropTable { keyspace, table } => format!("DROP TABLE {keyspace}.{table}"),
            DdlStep::AlterTable {
                keyspace, table, ..
            } => format!("ALTER TABLE {keyspace}.{table}"),
            DdlStep::CreateIndex { keyspace, name, .. } => {
                format!("CREATE INDEX {keyspace}.{name}")
            }
            DdlStep::DropIndex { keyspace, name, .. } => format!("DROP INDEX {keyspace}.{name}"),
        }
    }
}

/// Why a step does not apply to a schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepConflict(pub(crate) String);

fn table_key(keyspace: &str, table: &str) -> (String, String) {
    (keyspace.to_string(), table.to_string())
}

/// Apply one step to an in-memory schema, or say why it does not apply. The Lean model's
/// `apply`: the view, the preflight and the buffer all use this one function.
pub(crate) fn apply_step(snap: &mut SchemaSnapshot, step: &DdlStep) -> Result<(), StepConflict> {
    match step {
        DdlStep::CreateTable(meta) => apply_create_table(snap, meta),
        DdlStep::DropTable { keyspace, table } => {
            let key = table_key(keyspace, table);
            if snap.tables.remove(&key).is_none() {
                return Err(StepConflict(format!(
                    "table {keyspace}.{table} does not exist"
                )));
            }
            snap.indexes
                .retain(|(ks, tbl, _), _| !(ks == keyspace && tbl == table));
            Ok(())
        }
        DdlStep::AlterTable {
            keyspace,
            table,
            id,
            updates,
        } => apply_alter_table(snap, keyspace, table, *id, updates),
        DdlStep::CreateIndex {
            keyspace,
            table,
            name,
            columns,
        } => apply_create_index(snap, keyspace, table, name, columns),
        DdlStep::DropIndex {
            keyspace,
            table,
            name,
        } => {
            let key = (keyspace.clone(), table.clone(), name.clone());
            match snap.indexes.remove(&key) {
                Some(_) => Ok(()),
                None => Err(StepConflict(format!(
                    "index {keyspace}.{name} does not exist"
                ))),
            }
        }
    }
}

fn apply_create_table(snap: &mut SchemaSnapshot, meta: &TableMetadata) -> Result<(), StepConflict> {
    if !snap.keyspaces.contains_key(&meta.keyspace) {
        return Err(StepConflict(format!(
            "schema {} does not exist",
            meta.keyspace
        )));
    }
    let key = table_key(&meta.keyspace, &meta.name);
    if snap.tables.contains_key(&key) {
        return Err(StepConflict(format!(
            "table {}.{} already exists",
            meta.keyspace, meta.name
        )));
    }
    // The view needs its own copy: the buffered step keeps the original for the replay,
    // and a later ALTER in the same block changes the view's copy, not the step.
    snap.tables.insert(key, meta.clone());
    Ok(())
}

fn apply_alter_table(
    snap: &mut SchemaSnapshot,
    keyspace: &str,
    table: &str,
    id: uuid::Uuid,
    updates: &TableUpdates,
) -> Result<(), StepConflict> {
    let Some(meta) = snap.tables.get_mut(&table_key(keyspace, table)) else {
        return Err(StepConflict(format!(
            "table {keyspace}.{table} does not exist"
        )));
    };
    if meta.id != id {
        return Err(StepConflict(format!(
            "table {keyspace}.{table} was dropped and re-created"
        )));
    }
    if let Some(col) = updates
        .add_columns
        .iter()
        .find(|c| meta.columns.contains_key(&c.name))
    {
        return Err(StepConflict(format!(
            "column {} of {keyspace}.{table} already exists",
            col.name
        )));
    }
    if let Some(col) = updates
        .drop_columns
        .iter()
        .find(|c| !meta.columns.contains_key(*c))
    {
        return Err(StepConflict(format!(
            "column {col} of {keyspace}.{table} does not exist"
        )));
    }
    // Same order as `Schema::alter_table_internal`, so the view is what the replay builds.
    if let Some(params) = &updates.params {
        meta.params = params.clone(); // view copy; the step keeps the original for replay
    }
    for col in &updates.add_columns {
        meta.columns.insert(col.name.clone(), col.clone()); // view copy, as above
    }
    for name in &updates.drop_columns {
        meta.columns.shift_remove(name);
    }
    for (k, v) in updates.extensions.iter().flatten() {
        meta.extensions.insert(k.clone(), v.clone()); // view copy, as above
    }
    Ok(())
}

fn apply_create_index(
    snap: &mut SchemaSnapshot,
    keyspace: &str,
    table: &str,
    name: &str,
    columns: &[String],
) -> Result<(), StepConflict> {
    if !snap.tables.contains_key(&table_key(keyspace, table)) {
        return Err(StepConflict(format!(
            "table {keyspace}.{table} does not exist"
        )));
    }
    let key = (keyspace.to_string(), table.to_string(), name.to_string());
    if snap.indexes.contains_key(&key) {
        return Err(StepConflict(format!(
            "index {keyspace}.{name} already exists"
        )));
    }
    let index = ferrosa_cluster::pair::ddl::secondary_index(keyspace, table, name, columns);
    snap.indexes.insert(key, index);
    Ok(())
}

/// A transaction's buffered DDL and the view it has produced.
#[derive(Debug, Default)]
pub(crate) struct DdlBatch {
    steps: Vec<DdlStep>,
    /// The committed schema (as of the first buffered step) with every step applied.
    /// `None` until the first step; shared copy-on-write with the statement reading it.
    view: Option<Arc<SchemaSnapshot>>,
}

impl DdlBatch {
    pub(crate) fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// The schema a DDL statement inside the block plans against.
    pub(crate) fn view(&self, committed: &Schema) -> Arc<SchemaSnapshot> {
        match &self.view {
            Some(view) => Arc::clone(view),
            None => committed.snapshot(),
        }
    }

    /// Buffer `step` on top of the view. A step the view refuses is not buffered.
    pub(crate) fn push(&mut self, committed: &Schema, step: DdlStep) -> Result<(), String> {
        if self.steps.len() >= MAX_BUFFERED_DDL {
            return Err(format!(
                "a transaction may buffer at most {MAX_BUFFERED_DDL} DDL statements"
            ));
        }
        let view = self.view.get_or_insert_with(|| committed.snapshot());
        // Copy-on-write: the snapshot is shared with the registry (and with the statement
        // that is planning against it), so the first change copies it once.
        apply_step(Arc::make_mut(view), &step).map_err(|c| c.0)?;
        self.steps.push(step);
        Ok(())
    }

    /// The buffered steps, in order, consuming the batch.
    pub(crate) fn into_steps(self) -> Vec<DdlStep> {
        self.steps
    }
}

/// A [`DdlExecutor`] that records each schema change into a transaction's batch instead of
/// applying it. The DDL executors in [`crate::ddl`] run unchanged against it.
pub(crate) struct Recorder<'a> {
    batch: Mutex<&'a mut DdlBatch>,
    committed: &'a Schema,
}

impl<'a> Recorder<'a> {
    pub(crate) fn new(batch: &'a mut DdlBatch, committed: &'a Schema) -> Self {
        Self {
            batch: Mutex::new(batch),
            committed,
        }
    }

    fn record(&self, step: DdlStep) -> Result<(), String> {
        let mut batch = self
            .batch
            .lock()
            .map_err(|_| "transaction DDL batch lock poisoned".to_string())?;
        batch.push(self.committed, step)
    }
}

#[async_trait]
impl DdlExecutor for Recorder<'_> {
    async fn create_table(&self, table: TableMetadata) -> Result<(), String> {
        self.record(DdlStep::CreateTable(Box::new(table)))
    }

    async fn drop_table(&self, keyspace: &str, table: &str) -> Result<(), String> {
        self.record(DdlStep::DropTable {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        })
    }

    async fn alter_table(
        &self,
        keyspace: &str,
        table: &str,
        updates: TableUpdates,
    ) -> Result<(), String> {
        let id = {
            let batch = self
                .batch
                .lock()
                .map_err(|_| "transaction DDL batch lock poisoned".to_string())?;
            batch
                .view(self.committed)
                .tables
                .get(&table_key(keyspace, table))
                .map(|meta| meta.id)
                .ok_or_else(|| format!("table {keyspace}.{table} does not exist"))?
        };
        self.record(DdlStep::AlterTable {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            id,
            updates: Box::new(updates),
        })
    }

    async fn create_index(
        &self,
        keyspace: &str,
        table: &str,
        name: &str,
        columns: &[String],
    ) -> Result<(), String> {
        self.record(DdlStep::CreateIndex {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            name: name.to_string(),
            columns: columns.to_vec(),
        })
    }

    async fn drop_index(&self, keyspace: &str, table: &str, name: &str) -> Result<(), String> {
        self.record(DdlStep::DropIndex {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            name: name.to_string(),
        })
    }
}

/// Check that every step still applies, in order, to the schema as it is now. No schema
/// change is made; a conflict names the step that no longer applies.
pub(crate) fn preflight(committed: &Schema, steps: &[DdlStep]) -> Result<(), BackendMessage> {
    // A private copy to apply the steps to; the registry itself is not touched.
    let mut probe = SchemaSnapshot::clone(&committed.snapshot());
    for step in steps {
        if let Err(conflict) = apply_step(&mut probe, step) {
            tracing::warn!(
                step = %step.label(),
                reason = %conflict.0,
                "transactional DDL refused at COMMIT: the schema changed since it was planned"
            );
            return Err(error_response(
                "40001",
                &format!(
                    "could not serialize transaction: the schema changed concurrently and \
                     \"{}\" no longer applies ({}); no schema change was applied",
                    step.label(),
                    conflict.0
                ),
            ));
        }
    }
    Ok(())
}

/// The result of replaying a batch: the Lean model's `CommitResult`, with labels.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReplayOutcome {
    Committed {
        applied: usize,
    },
    FailedAt {
        applied: Vec<String>,
        failed: String,
        error: String,
        not_applied: Vec<String>,
    },
}

/// Send one step to the schema-change path.
async fn exec_step(executor: &dyn DdlExecutor, step: DdlStep) -> Result<(), String> {
    match step {
        DdlStep::CreateTable(meta) => executor.create_table(*meta).await,
        DdlStep::DropTable { keyspace, table } => executor.drop_table(&keyspace, &table).await,
        DdlStep::AlterTable {
            keyspace,
            table,
            updates,
            ..
        } => executor.alter_table(&keyspace, &table, *updates).await,
        DdlStep::CreateIndex {
            keyspace,
            table,
            name,
            columns,
        } => {
            executor
                .create_index(&keyspace, &table, &name, &columns)
                .await
        }
        DdlStep::DropIndex {
            keyspace,
            table,
            name,
        } => executor.drop_index(&keyspace, &table, &name).await,
    }
}

/// Replay `steps` in order, stopping at the first failure and reporting exactly which
/// steps were applied, which failed and which never ran.
pub(crate) async fn replay(executor: &dyn DdlExecutor, steps: Vec<DdlStep>) -> ReplayOutcome {
    let total = steps.len();
    let mut applied: Vec<String> = Vec::with_capacity(total);
    let mut rest = steps.into_iter();
    while let Some(step) = rest.next() {
        let label = step.label();
        if let Err(error) = exec_step(executor, step).await {
            return ReplayOutcome::FailedAt {
                applied,
                failed: label,
                error,
                not_applied: rest.map(|s| s.label()).collect(),
            };
        }
        applied.push(label);
    }
    debug_assert_eq!(applied.len(), total);
    ReplayOutcome::Committed { applied: total }
}

/// The client's error for a replay that stopped partway.
pub(crate) fn partial_failure_response(outcome: &ReplayOutcome) -> Option<BackendMessage> {
    let ReplayOutcome::FailedAt {
        applied,
        failed,
        error,
        not_applied,
    } = outcome
    else {
        return None;
    };
    let total = applied.len() + 1 + not_applied.len();
    let message = format!(
        "COMMIT applied {} of {total} schema changes and then failed; the applied ones are \
         NOT rolled back. Applied: [{}]. Failed: {failed}: {error}. Not applied: [{}]. The \
         transaction's row writes were not committed.",
        applied.len(),
        applied.join(", "),
        not_applied.join(", "),
    );
    tracing::error!(%message, "transactional DDL partially applied at COMMIT");
    Some(error_response("58000", &message))
}

/// Preflight and replay a transaction's DDL at COMMIT. `Ok(n)` is the number of schema
/// changes applied (all of them).
pub(crate) async fn commit_batch(
    executor: &dyn DdlExecutor,
    committed: &Schema,
    batch: DdlBatch,
) -> Result<usize, BackendMessage> {
    let steps = batch.into_steps();
    preflight(committed, &steps)?;
    let outcome = replay(executor, steps).await;
    match partial_failure_response(&outcome) {
        Some(error) => Err(error),
        None => match outcome {
            ReplayOutcome::Committed { applied } => Ok(applied),
            ReplayOutcome::FailedAt { .. } => Err(error_response(
                "XX000",
                "internal: a failed DDL replay produced no report",
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Applies nothing; fails the `fail_at`-th call (1-based) and counts calls.
    struct FailingExecutor {
        calls: AtomicUsize,
        fail_at: usize,
    }

    impl FailingExecutor {
        fn call(&self) -> Result<(), String> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if n == self.fail_at {
                Err("schema-change path unavailable".to_string())
            } else {
                Ok(())
            }
        }
    }

    #[async_trait]
    impl DdlExecutor for FailingExecutor {
        async fn create_table(&self, _table: TableMetadata) -> Result<(), String> {
            self.call()
        }
        async fn drop_table(&self, _k: &str, _t: &str) -> Result<(), String> {
            self.call()
        }
        async fn alter_table(&self, _k: &str, _t: &str, _u: TableUpdates) -> Result<(), String> {
            self.call()
        }
        async fn create_index(
            &self,
            _k: &str,
            _t: &str,
            _n: &str,
            _c: &[String],
        ) -> Result<(), String> {
            self.call()
        }
        async fn drop_index(&self, _k: &str, _t: &str, _n: &str) -> Result<(), String> {
            self.call()
        }
    }

    fn drop_step(table: &str) -> DdlStep {
        DdlStep::DropTable {
            keyspace: "public".to_string(),
            table: table.to_string(),
        }
    }

    /// The Lean model's `third_of_five_fails_loudly`, against the real replay loop: the
    /// report names the two applied steps, the failed one and the two never run, and the
    /// executor was never called for the last two.
    #[tokio::test]
    async fn third_of_five_reports_the_exact_prefix() {
        let executor = FailingExecutor {
            calls: AtomicUsize::new(0),
            fail_at: 3,
        };
        let steps = ["a", "b", "c", "d", "e"].map(drop_step).to_vec();
        let outcome = replay(&executor, steps).await;
        assert_eq!(
            outcome,
            ReplayOutcome::FailedAt {
                applied: vec!["DROP TABLE public.a".into(), "DROP TABLE public.b".into()],
                failed: "DROP TABLE public.c".into(),
                error: "schema-change path unavailable".into(),
                not_applied: vec!["DROP TABLE public.d".into(), "DROP TABLE public.e".into()],
            }
        );
        assert_eq!(
            executor.calls.load(Ordering::SeqCst),
            3,
            "steps 4 and 5 never ran"
        );
        let Some(BackendMessage::ErrorResponse { fields }) = partial_failure_response(&outcome)
        else {
            panic!("a partial replay must produce an error");
        };
        assert!(fields.contains(&(b'C', "58000".to_string())));
        let message = &fields.iter().find(|(f, _)| *f == b'M').expect("message").1;
        assert!(message.contains("applied 2 of 5"), "{message}");
        assert!(message.contains("NOT rolled back"), "{message}");
    }

    #[tokio::test]
    async fn a_full_replay_commits_every_step_in_order() {
        let executor = FailingExecutor {
            calls: AtomicUsize::new(0),
            fail_at: 0,
        };
        let outcome = replay(&executor, ["a", "b"].map(drop_step).to_vec()).await;
        assert_eq!(outcome, ReplayOutcome::Committed { applied: 2 });
        assert!(partial_failure_response(&outcome).is_none());
    }

    /// A failure on the very first step applies nothing and still says so exactly.
    #[tokio::test]
    async fn first_step_failure_applies_nothing() {
        let executor = FailingExecutor {
            calls: AtomicUsize::new(0),
            fail_at: 1,
        };
        let outcome = replay(&executor, ["a", "b"].map(drop_step).to_vec()).await;
        let ReplayOutcome::FailedAt {
            applied,
            not_applied,
            ..
        } = outcome
        else {
            panic!("must fail");
        };
        assert!(applied.is_empty());
        assert_eq!(not_applied, vec!["DROP TABLE public.b".to_string()]);
    }
}
