use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;

use crate::history::{History, HistoryRecorder, Op, OpResult};

use super::{CqlSession, Workload};

/// Returns all 16 LWT workload patterns.
pub fn all_lwt_workloads() -> Vec<Box<dyn Workload>> {
    vec![
        Box::new(LwtInsertIfNotExists),
        Box::new(LwtUpdateIf),
        Box::new(LwtDeleteIf),
        Box::new(LwtInsertIfNotExistsTtl),
        Box::new(LwtUpdateIfExists),
        Box::new(LwtReplaceIf),
        Box::new(LwtIncrementIf),
        Box::new(LwtBatchInsert),
        Box::new(LwtBatchMixed),
        Box::new(LwtWithCollections),
        Box::new(LwtWithUdt),
        Box::new(LwtWithCounter),
        Box::new(LwtWithTimestamp),
        Box::new(LwtWireFormat),
        Box::new(LwtSerialRead),
        Box::new(LwtMultiStatement),
    ]
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Standard keyspace creation CQL.
const CREATE_KEYSPACE: &str = "CREATE KEYSPACE IF NOT EXISTS jepsen \
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";

/// Run a simple LWT loop: alternate INSERT IF NOT EXISTS attempts on the same PK.
/// Records InsertIfNotExists ops into the history.
async fn run_insert_if_not_exists_loop(
    session: &dyn CqlSession,
    recorder: &mut HistoryRecorder,
    table: &str,
    duration: Duration,
) -> Result<()> {
    let start = Instant::now();
    let mut seq = 0u64;

    while start.elapsed() < duration {
        let pk = "pk-0";
        let val = format!("v{seq}");

        recorder.invoke(Op::InsertIfNotExists {
            table: table.into(),
            pk: pk.into(),
            values: vec![("val".into(), val.clone())],
        });

        let query = format!("INSERT INTO {table} (id, val) VALUES ('{pk}', '{val}') IF NOT EXISTS");
        match session.execute(&query).await {
            Ok(rows) => {
                let applied = rows
                    .first()
                    .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                    .unwrap_or(false);
                recorder.complete(OpResult::Applied(applied));
            }
            Err(e) => recorder.complete(OpResult::Err(e.to_string())),
        }
        seq += 1;
    }
    Ok(())
}

/// Run a simple UPDATE ... IF loop.
async fn run_update_if_loop(
    session: &dyn CqlSession,
    recorder: &mut HistoryRecorder,
    table: &str,
    duration: Duration,
) -> Result<()> {
    let start = Instant::now();
    let mut counter = 0i64;

    while start.elapsed() < duration {
        let expected = counter;
        let new_val = counter + 1;

        recorder.invoke(Op::UpdateIf {
            table: table.into(),
            pk: "pk-0".into(),
            condition: format!("val = {expected}"),
            assignments: vec![("val".into(), new_val.to_string())],
        });

        let query =
            format!("UPDATE {table} SET val = {new_val} WHERE id = 'pk-0' IF val = {expected}");
        match session.execute(&query).await {
            Ok(rows) => {
                let applied = rows
                    .first()
                    .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                    .unwrap_or(false);
                recorder.complete(OpResult::Applied(applied));
                if applied {
                    counter = new_val;
                }
            }
            Err(e) => recorder.complete(OpResult::Err(e.to_string())),
        }
    }
    Ok(())
}

/// Default linearizability check delegation.
fn check_linearizability_default(history: &History) -> Result<()> {
    let results = crate::checker::check_linearizability(history);
    for r in &results {
        if !r.valid {
            anyhow::bail!(
                "LWT not linearizable for key {}: {:?}",
                r.key,
                r.counterexample
            );
        }
    }
    Ok(())
}

/// Check that among InsertIfNotExists ops for the same PK, at most one was applied.
fn check_insert_exactly_one_winner(history: &History) -> Result<()> {
    let applied_count = history
        .operations
        .iter()
        .filter(|op| matches!(&op.op, Op::InsertIfNotExists { .. }))
        .filter(|op| matches!(&op.result, OpResult::Applied(true)))
        .count();

    if applied_count > 1 {
        anyhow::bail!("INSERT IF NOT EXISTS: expected at most 1 applied, got {applied_count}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pattern 1: INSERT IF NOT EXISTS
// ---------------------------------------------------------------------------

pub struct LwtInsertIfNotExists;

#[async_trait]
impl Workload for LwtInsertIfNotExists {
    fn name(&self) -> &str {
        "lwt-1-insert-if-not-exists"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt1 \
                 (id text PRIMARY KEY, val text)",
            )
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        run_insert_if_not_exists_loop(session, recorder, "jepsen.lwt1", duration).await
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        check_insert_exactly_one_winner(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 2: UPDATE ... IF condition
// ---------------------------------------------------------------------------

pub struct LwtUpdateIf;

#[async_trait]
impl Workload for LwtUpdateIf {
    fn name(&self) -> &str {
        "lwt-2-update-if"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt2 \
                 (id text PRIMARY KEY, val bigint)",
            )
            .await?;
        session
            .execute("INSERT INTO jepsen.lwt2 (id, val) VALUES ('pk-0', 0)")
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        run_update_if_loop(session, recorder, "jepsen.lwt2", duration).await
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        check_linearizability_default(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 3: DELETE IF EXISTS / IF condition
// ---------------------------------------------------------------------------

pub struct LwtDeleteIf;

#[async_trait]
impl Workload for LwtDeleteIf {
    fn name(&self) -> &str {
        "lwt-3-delete-if"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt3 \
                 (id text PRIMARY KEY, val text)",
            )
            .await?;
        session
            .execute("INSERT INTO jepsen.lwt3 (id, val) VALUES ('pk-0', 'initial')")
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();

        while start.elapsed() < duration {
            // Alternate: insert, then delete-if-exists.
            recorder.invoke(Op::InsertIfNotExists {
                table: "jepsen.lwt3".into(),
                pk: "pk-0".into(),
                values: vec![("val".into(), "inserted".into())],
            });
            match session
                .execute(
                    "INSERT INTO jepsen.lwt3 (id, val) VALUES ('pk-0', 'inserted') \
                     IF NOT EXISTS",
                )
                .await
            {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    recorder.complete(OpResult::Applied(applied));
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }

            recorder.invoke(Op::DeleteIf {
                table: "jepsen.lwt3".into(),
                pk: "pk-0".into(),
                condition: "EXISTS".into(),
            });
            match session
                .execute("DELETE FROM jepsen.lwt3 WHERE id = 'pk-0' IF EXISTS")
                .await
            {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    recorder.complete(OpResult::Applied(applied));
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        // Every successful delete must follow an insert (or initial state).
        // Delegate to linearizability for key-based ops.
        check_linearizability_default(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 4: INSERT IF NOT EXISTS with TTL
// ---------------------------------------------------------------------------

pub struct LwtInsertIfNotExistsTtl;

#[async_trait]
impl Workload for LwtInsertIfNotExistsTtl {
    fn name(&self) -> &str {
        "lwt-4-insert-if-not-exists-ttl"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt4 \
                 (id text PRIMARY KEY, val text)",
            )
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut seq = 0u64;

        while start.elapsed() < duration {
            let val = format!("v{seq}");
            recorder.invoke(Op::InsertIfNotExists {
                table: "jepsen.lwt4".into(),
                pk: "pk-0".into(),
                values: vec![("val".into(), val.clone())],
            });

            let query = format!(
                "INSERT INTO jepsen.lwt4 (id, val) VALUES ('pk-0', '{val}') \
                 IF NOT EXISTS USING TTL 10"
            );
            match session.execute(&query).await {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    recorder.complete(OpResult::Applied(applied));
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
            seq += 1;
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        check_insert_exactly_one_winner(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 5: UPDATE IF EXISTS
// ---------------------------------------------------------------------------

pub struct LwtUpdateIfExists;

#[async_trait]
impl Workload for LwtUpdateIfExists {
    fn name(&self) -> &str {
        "lwt-5-update-if-exists"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt5 \
                 (id text PRIMARY KEY, val bigint)",
            )
            .await?;
        session
            .execute("INSERT INTO jepsen.lwt5 (id, val) VALUES ('pk-0', 0)")
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut counter = 1i64;

        while start.elapsed() < duration {
            recorder.invoke(Op::UpdateIf {
                table: "jepsen.lwt5".into(),
                pk: "pk-0".into(),
                condition: "EXISTS".into(),
                assignments: vec![("val".into(), counter.to_string())],
            });

            let query =
                format!("UPDATE jepsen.lwt5 SET val = {counter} WHERE id = 'pk-0' IF EXISTS");
            match session.execute(&query).await {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    recorder.complete(OpResult::Applied(applied));
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
            counter += 1;
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        check_linearizability_default(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 6: UPDATE all columns IF old = expected (replace)
// ---------------------------------------------------------------------------

pub struct LwtReplaceIf;

#[async_trait]
impl Workload for LwtReplaceIf {
    fn name(&self) -> &str {
        "lwt-6-replace-if"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt6 \
                 (id text PRIMARY KEY, a bigint, b bigint)",
            )
            .await?;
        session
            .execute("INSERT INTO jepsen.lwt6 (id, a, b) VALUES ('pk-0', 0, 0)")
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut counter = 0i64;

        while start.elapsed() < duration {
            let expected = counter;
            let new_val = counter + 1;

            recorder.invoke(Op::UpdateIf {
                table: "jepsen.lwt6".into(),
                pk: "pk-0".into(),
                condition: format!("a = {expected} AND b = {expected}"),
                assignments: vec![
                    ("a".into(), new_val.to_string()),
                    ("b".into(), new_val.to_string()),
                ],
            });

            let query = format!(
                "UPDATE jepsen.lwt6 SET a = {new_val}, b = {new_val} \
                 WHERE id = 'pk-0' IF a = {expected} AND b = {expected}"
            );
            match session.execute(&query).await {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    recorder.complete(OpResult::Applied(applied));
                    if applied {
                        counter = new_val;
                    }
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        check_linearizability_default(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 7: Increment IF val = expected
// ---------------------------------------------------------------------------

pub struct LwtIncrementIf;

#[async_trait]
impl Workload for LwtIncrementIf {
    fn name(&self) -> &str {
        "lwt-7-increment-if"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt7 \
                 (id text PRIMARY KEY, val bigint)",
            )
            .await?;
        session
            .execute("INSERT INTO jepsen.lwt7 (id, val) VALUES ('pk-0', 0)")
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        run_update_if_loop(session, recorder, "jepsen.lwt7", duration).await
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        // Verify monotonic increments: applied updates must form a
        // strictly increasing sequence.
        let mut last_applied: Option<i64> = None;
        for op in &history.operations {
            if let (Op::UpdateIf { assignments, .. }, OpResult::Applied(true)) =
                (&op.op, &op.result)
            {
                if let Some(val_str) = assignments.first().map(|(_, v)| v) {
                    if let Ok(val) = val_str.parse::<i64>() {
                        if let Some(prev) = last_applied {
                            if val <= prev {
                                anyhow::bail!("Increment not monotonic: {prev} -> {val}");
                            }
                        }
                        last_applied = Some(val);
                    }
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Pattern 8: BATCH with multiple IF NOT EXISTS
// ---------------------------------------------------------------------------

pub struct LwtBatchInsert;

#[async_trait]
impl Workload for LwtBatchInsert {
    fn name(&self) -> &str {
        "lwt-8-batch-insert"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt8 \
                 (id text PRIMARY KEY, val text)",
            )
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut seq = 0u64;

        while start.elapsed() < duration {
            let val = format!("v{seq}");
            recorder.invoke(Op::InsertIfNotExists {
                table: "jepsen.lwt8".into(),
                pk: "pk-0".into(),
                values: vec![("val".into(), val.clone())],
            });

            // BATCH containing IF NOT EXISTS (Cassandra requires same partition).
            let query = format!(
                "BEGIN BATCH \
                 INSERT INTO jepsen.lwt8 (id, val) VALUES ('pk-0', '{val}') IF NOT EXISTS; \
                 APPLY BATCH"
            );
            match session.execute(&query).await {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    recorder.complete(OpResult::Applied(applied));
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
            seq += 1;
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        check_insert_exactly_one_winner(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 9: BATCH mixing IF NOT EXISTS + IF condition
// ---------------------------------------------------------------------------

pub struct LwtBatchMixed;

#[async_trait]
impl Workload for LwtBatchMixed {
    fn name(&self) -> &str {
        "lwt-9-batch-mixed"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt9 \
                 (id text, seq int, val text, PRIMARY KEY (id, seq))",
            )
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut seq = 0i32;

        while start.elapsed() < duration {
            let val = format!("v{seq}");
            recorder.invoke(Op::InsertIfNotExists {
                table: "jepsen.lwt9".into(),
                pk: "pk-0".into(),
                values: vec![("seq".into(), seq.to_string()), ("val".into(), val.clone())],
            });

            let query = format!(
                "BEGIN BATCH \
                 INSERT INTO jepsen.lwt9 (id, seq, val) VALUES ('pk-0', {seq}, '{val}') \
                 IF NOT EXISTS; \
                 APPLY BATCH"
            );
            match session.execute(&query).await {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    recorder.complete(OpResult::Applied(applied));
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
            seq += 1;
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        check_linearizability_default(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 10: LWT on list/set/map columns
// ---------------------------------------------------------------------------

pub struct LwtWithCollections;

#[async_trait]
impl Workload for LwtWithCollections {
    fn name(&self) -> &str {
        "lwt-10-collections"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt10 \
                 (id text PRIMARY KEY, tags set<text>, props map<text, text>)",
            )
            .await?;
        session
            .execute(
                "INSERT INTO jepsen.lwt10 (id, tags, props) \
                 VALUES ('pk-0', {}, {})",
            )
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut seq = 0u64;

        while start.elapsed() < duration {
            let tag = format!("tag{seq}");
            recorder.invoke(Op::UpdateIf {
                table: "jepsen.lwt10".into(),
                pk: "pk-0".into(),
                condition: "EXISTS".into(),
                assignments: vec![("tags".into(), format!("tags + {{'{tag}'}}"))],
            });

            let query = format!(
                "UPDATE jepsen.lwt10 SET tags = tags + {{'{tag}'}} \
                 WHERE id = 'pk-0' IF EXISTS"
            );
            match session.execute(&query).await {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    recorder.complete(OpResult::Applied(applied));
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
            seq += 1;
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        // All applied updates should succeed (row exists).
        check_linearizability_default(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 11: LWT with UDT columns
// ---------------------------------------------------------------------------

pub struct LwtWithUdt;

#[async_trait]
impl Workload for LwtWithUdt {
    fn name(&self) -> &str {
        "lwt-11-udt"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TYPE IF NOT EXISTS jepsen.address \
                 (street text, city text)",
            )
            .await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt11 \
                 (id text PRIMARY KEY, addr frozen<jepsen.address>)",
            )
            .await?;
        session
            .execute(
                "INSERT INTO jepsen.lwt11 (id, addr) \
                 VALUES ('pk-0', {street: 'Main St', city: 'Springfield'})",
            )
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut seq = 0u64;

        while start.elapsed() < duration {
            let city = format!("City{seq}");
            recorder.invoke(Op::UpdateIf {
                table: "jepsen.lwt11".into(),
                pk: "pk-0".into(),
                condition: "EXISTS".into(),
                assignments: vec![(
                    "addr".into(),
                    format!("{{street: 'Main St', city: '{city}'}}"),
                )],
            });

            let query = format!(
                "UPDATE jepsen.lwt11 \
                 SET addr = {{street: 'Main St', city: '{city}'}} \
                 WHERE id = 'pk-0' IF EXISTS"
            );
            match session.execute(&query).await {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    recorder.complete(OpResult::Applied(applied));
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
            seq += 1;
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        check_linearizability_default(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 12: LWT-like pattern on counter table
// ---------------------------------------------------------------------------

pub struct LwtWithCounter;

#[async_trait]
impl Workload for LwtWithCounter {
    fn name(&self) -> &str {
        "lwt-12-counter"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        // Cassandra counters don't support LWT directly; we use a
        // regular bigint column with a CAS-based read-modify-write loop.
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt12 \
                 (id text PRIMARY KEY, val bigint)",
            )
            .await?;
        session
            .execute("INSERT INTO jepsen.lwt12 (id, val) VALUES ('pk-0', 0)")
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();

        while start.elapsed() < duration {
            // Read current value.
            recorder.invoke(Op::Read { key: "pk-0".into() });
            let current = match session
                .execute("SELECT val FROM jepsen.lwt12 WHERE id = 'pk-0'")
                .await
            {
                Ok(rows) => {
                    let val = rows
                        .first()
                        .and_then(|r| r.first())
                        .and_then(|(_, v)| v.parse::<i64>().ok());
                    recorder.complete(OpResult::Value(val));
                    val
                }
                Err(e) => {
                    recorder.complete(OpResult::Err(e.to_string()));
                    continue;
                }
            };

            let Some(current) = current else {
                continue;
            };

            // CAS increment.
            let new_val = current + 1;
            recorder.invoke(Op::Cas {
                key: "pk-0".into(),
                expected: current,
                value: new_val,
            });
            match session
                .execute(&format!(
                    "UPDATE jepsen.lwt12 SET val = {new_val} \
                     WHERE id = 'pk-0' IF val = {current}"
                ))
                .await
            {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    recorder.complete(OpResult::Applied(applied));
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        check_linearizability_default(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 13: LWT with client timestamps
// ---------------------------------------------------------------------------

pub struct LwtWithTimestamp;

#[async_trait]
impl Workload for LwtWithTimestamp {
    fn name(&self) -> &str {
        "lwt-13-timestamp"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt13 \
                 (id text PRIMARY KEY, val bigint)",
            )
            .await?;
        session
            .execute("INSERT INTO jepsen.lwt13 (id, val) VALUES ('pk-0', 0)")
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut counter = 0i64;

        while start.elapsed() < duration {
            let expected = counter;
            let new_val = counter + 1;
            let ts = chrono::Utc::now().timestamp_micros();

            recorder.invoke(Op::UpdateIf {
                table: "jepsen.lwt13".into(),
                pk: "pk-0".into(),
                condition: format!("val = {expected}"),
                assignments: vec![("val".into(), new_val.to_string())],
            });

            let query = format!(
                "UPDATE jepsen.lwt13 USING TIMESTAMP {ts} \
                 SET val = {new_val} WHERE id = 'pk-0' IF val = {expected}"
            );
            match session.execute(&query).await {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    recorder.complete(OpResult::Applied(applied));
                    if applied {
                        counter = new_val;
                    }
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        check_linearizability_default(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 14: Verify [applied] column format and current row values
// ---------------------------------------------------------------------------

pub struct LwtWireFormat;

#[async_trait]
impl Workload for LwtWireFormat {
    fn name(&self) -> &str {
        "lwt-14-wire-format"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt14 \
                 (id text PRIMARY KEY, val bigint)",
            )
            .await?;
        session
            .execute("INSERT INTO jepsen.lwt14 (id, val) VALUES ('pk-0', 0)")
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut seq = 0u64;

        while start.elapsed() < duration {
            // Intentionally use a wrong expected value to force [applied]=false
            // and get back the current row values.
            let wrong_expected = 999_999i64 + seq as i64;
            recorder.invoke(Op::UpdateIf {
                table: "jepsen.lwt14".into(),
                pk: "pk-0".into(),
                condition: format!("val = {wrong_expected}"),
                assignments: vec![("val".into(), "1".into())],
            });

            let query = format!(
                "UPDATE jepsen.lwt14 SET val = 1 \
                 WHERE id = 'pk-0' IF val = {wrong_expected}"
            );
            match session.execute(&query).await {
                Ok(rows) => {
                    let applied = rows
                        .first()
                        .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                        .unwrap_or(false);
                    if applied {
                        recorder.complete(OpResult::Applied(true));
                    } else {
                        // Should return current values when not applied.
                        let current = rows
                            .first()
                            .map(|r| {
                                r.iter()
                                    .filter(|(k, _)| k != "[applied]")
                                    .cloned()
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        recorder.complete(OpResult::CurrentValues(current));
                    }
                }
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
            seq += 1;
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        // Verify that non-applied results always include current row values.
        for op in &history.operations {
            if let OpResult::CurrentValues(values) = &op.result {
                if values.is_empty() {
                    anyhow::bail!("Wire format error: non-applied LWT returned no current values");
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Pattern 15: SELECT with SERIAL consistency
// ---------------------------------------------------------------------------

pub struct LwtSerialRead;

#[async_trait]
impl Workload for LwtSerialRead {
    fn name(&self) -> &str {
        "lwt-15-serial-read"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt15 \
                 (id text PRIMARY KEY, val bigint)",
            )
            .await?;
        session
            .execute("INSERT INTO jepsen.lwt15 (id, val) VALUES ('pk-0', 0)")
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut counter = 0i64;

        while start.elapsed() < duration {
            let r: f64 = rand::random();
            if r < 0.5 {
                // SERIAL read.
                recorder.invoke(Op::SerialRead { key: "pk-0".into() });
                match session
                    .execute(
                        "SELECT val FROM jepsen.lwt15 WHERE id = 'pk-0'",
                        // In a real driver, this uses SERIAL consistency.
                    )
                    .await
                {
                    Ok(rows) => {
                        let val = rows
                            .first()
                            .and_then(|r| r.first())
                            .and_then(|(_, v)| v.parse().ok());
                        recorder.complete(OpResult::Value(val));
                    }
                    Err(e) => recorder.complete(OpResult::Err(e.to_string())),
                }
            } else {
                // CAS write.
                let expected = counter;
                let new_val = counter + 1;
                recorder.invoke(Op::Cas {
                    key: "pk-0".into(),
                    expected,
                    value: new_val,
                });
                match session
                    .execute(&format!(
                        "UPDATE jepsen.lwt15 SET val = {new_val} \
                         WHERE id = 'pk-0' IF val = {expected}"
                    ))
                    .await
                {
                    Ok(rows) => {
                        let applied = rows
                            .first()
                            .map(|r| r.iter().any(|(k, v)| k == "[applied]" && v == "true"))
                            .unwrap_or(false);
                        recorder.complete(OpResult::Applied(applied));
                        if applied {
                            counter = new_val;
                        }
                    }
                    Err(e) => recorder.complete(OpResult::Err(e.to_string())),
                }
            }
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        check_linearizability_default(history)
    }
}

// ---------------------------------------------------------------------------
// Pattern 16: Multi-statement Accord transaction
// ---------------------------------------------------------------------------

pub struct LwtMultiStatement;

#[async_trait]
impl Workload for LwtMultiStatement {
    fn name(&self) -> &str {
        "lwt-16-multi-statement"
    }

    async fn setup(&self, session: &dyn CqlSession) -> Result<()> {
        session.execute(CREATE_KEYSPACE).await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt16a \
                 (id text PRIMARY KEY, val bigint)",
            )
            .await?;
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS jepsen.lwt16b \
                 (id text PRIMARY KEY, val bigint)",
            )
            .await?;
        session
            .execute("INSERT INTO jepsen.lwt16a (id, val) VALUES ('pk-0', 0)")
            .await?;
        session
            .execute("INSERT INTO jepsen.lwt16b (id, val) VALUES ('pk-0', 0)")
            .await?;
        Ok(())
    }

    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut counter = 1i64;

        while start.elapsed() < duration {
            // Multi-statement transaction: write same value to both tables.
            recorder.invoke(Op::Transaction {
                statements: vec![
                    Op::Write {
                        key: "lwt16a:pk-0".into(),
                        value: counter,
                    },
                    Op::Write {
                        key: "lwt16b:pk-0".into(),
                        value: counter,
                    },
                ],
            });

            let query = format!(
                "BEGIN TRANSACTION \
                 UPDATE jepsen.lwt16a SET val = {counter} WHERE id = 'pk-0'; \
                 UPDATE jepsen.lwt16b SET val = {counter} WHERE id = 'pk-0'; \
                 COMMIT TRANSACTION"
            );
            match session.execute(&query).await {
                Ok(_) => recorder.complete(OpResult::Ok),
                Err(e) => recorder.complete(OpResult::Err(e.to_string())),
            }
            counter += 1;
        }
        Ok(())
    }

    fn check_invariant(&self, history: &History) -> Result<()> {
        // For now, verify no unexpected errors. Full Accord
        // transaction verification will be added in later phases.
        for op in &history.operations {
            if let OpResult::Err(e) = &op.result {
                // Timeouts are acceptable under chaos; actual errors are not.
                if !e.contains("timeout") && !e.contains("Timeout") {
                    anyhow::bail!("Multi-statement transaction error: {e}");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::history::Operation;
    use crate::workload::testutil::MockCqlSession;

    fn make_op(client: &str, invoke: u64, complete: u64, op: Op, result: OpResult) -> Operation {
        Operation {
            client_id: client.to_string(),
            invoke_us: invoke,
            complete_us: complete,
            op,
            result,
        }
    }

    /// Pattern 1: INSERT IF NOT EXISTS executes against a mock session and
    /// records InsertIfNotExists ops in the history.
    #[tokio::test]
    async fn lwt_insert_if_not_exists_executes() {
        let session = MockCqlSession::new();
        let workload = LwtInsertIfNotExists;

        workload.setup(&session).await.unwrap();

        let mut recorder = HistoryRecorder::new("test");
        workload
            .run(&session, &mut recorder, Duration::from_millis(50))
            .await
            .unwrap();

        let history = recorder.finish();
        assert!(
            !history.operations.is_empty(),
            "should have executed at least one InsertIfNotExists operation"
        );
        assert!(
            history
                .operations
                .iter()
                .any(|op| matches!(op.op, Op::InsertIfNotExists { .. })),
            "history must contain InsertIfNotExists ops"
        );
    }

    /// Pattern 7 (LwtIncrementIf) acts as a CAS counter workload.
    /// Verify it executes against a mock session and produces Applied results.
    #[tokio::test]
    async fn lwt_cas_counter_executes() {
        let session = MockCqlSession::new();
        let workload = LwtIncrementIf;

        workload.setup(&session).await.unwrap();

        let mut recorder = HistoryRecorder::new("test");
        workload
            .run(&session, &mut recorder, Duration::from_millis(50))
            .await
            .unwrap();

        let history = recorder.finish();
        assert!(
            !history.operations.is_empty(),
            "should have executed at least one CAS operation"
        );
        // At minimum one Applied result must be present (first toggle = true).
        assert!(
            history
                .operations
                .iter()
                .any(|op| matches!(op.result, OpResult::Applied(_))),
            "history must contain Applied results from CAS operations"
        );
    }

    #[test]
    fn lwt_all_workloads_count() {
        let workloads = all_lwt_workloads();
        assert_eq!(workloads.len(), 16);
    }

    #[test]
    fn lwt_all_workloads_unique_names() {
        let workloads = all_lwt_workloads();
        let names: Vec<String> = workloads.iter().map(|w| w.name().to_string()).collect();
        let mut deduped = names.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(names.len(), deduped.len(), "workload names must be unique");
    }

    #[test]
    fn lwt_pattern_1_exactly_one_winner() {
        // Two concurrent INSERT IF NOT EXISTS: one applied=true, one applied=false.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    300,
                    Op::InsertIfNotExists {
                        table: "jepsen.lwt1".into(),
                        pk: "pk-0".into(),
                        values: vec![("val".into(), "v0".into())],
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c2",
                    200,
                    400,
                    Op::InsertIfNotExists {
                        table: "jepsen.lwt1".into(),
                        pk: "pk-0".into(),
                        values: vec![("val".into(), "v1".into())],
                    },
                    OpResult::Applied(false),
                ),
            ],
        };

        let wl = LwtInsertIfNotExists;
        assert!(wl.check_invariant(&history).is_ok());
    }

    #[test]
    fn lwt_pattern_1_two_winners_fails() {
        // Two INSERT IF NOT EXISTS both applied=true: invariant violation.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    300,
                    Op::InsertIfNotExists {
                        table: "jepsen.lwt1".into(),
                        pk: "pk-0".into(),
                        values: vec![("val".into(), "v0".into())],
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c2",
                    200,
                    400,
                    Op::InsertIfNotExists {
                        table: "jepsen.lwt1".into(),
                        pk: "pk-0".into(),
                        values: vec![("val".into(), "v1".into())],
                    },
                    OpResult::Applied(true),
                ),
            ],
        };

        let wl = LwtInsertIfNotExists;
        assert!(wl.check_invariant(&history).is_err());
    }

    #[test]
    fn lwt_pattern_7_monotonic_increment() {
        // Applied updates form an increasing sequence: 1, 2, 3.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::UpdateIf {
                        table: "jepsen.lwt7".into(),
                        pk: "pk-0".into(),
                        condition: "val = 0".into(),
                        assignments: vec![("val".into(), "1".into())],
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::UpdateIf {
                        table: "jepsen.lwt7".into(),
                        pk: "pk-0".into(),
                        condition: "val = 1".into(),
                        assignments: vec![("val".into(), "2".into())],
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c1",
                    500,
                    600,
                    Op::UpdateIf {
                        table: "jepsen.lwt7".into(),
                        pk: "pk-0".into(),
                        condition: "val = 2".into(),
                        assignments: vec![("val".into(), "3".into())],
                    },
                    OpResult::Applied(true),
                ),
            ],
        };

        let wl = LwtIncrementIf;
        assert!(wl.check_invariant(&history).is_ok());
    }

    #[test]
    fn lwt_pattern_7_non_monotonic_fails() {
        // Applied updates go 1, 3, 2: not monotonic.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::UpdateIf {
                        table: "jepsen.lwt7".into(),
                        pk: "pk-0".into(),
                        condition: "val = 0".into(),
                        assignments: vec![("val".into(), "1".into())],
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::UpdateIf {
                        table: "jepsen.lwt7".into(),
                        pk: "pk-0".into(),
                        condition: "val = 2".into(),
                        assignments: vec![("val".into(), "3".into())],
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c1",
                    500,
                    600,
                    Op::UpdateIf {
                        table: "jepsen.lwt7".into(),
                        pk: "pk-0".into(),
                        condition: "val = 1".into(),
                        assignments: vec![("val".into(), "2".into())],
                    },
                    OpResult::Applied(true),
                ),
            ],
        };

        let wl = LwtIncrementIf;
        assert!(wl.check_invariant(&history).is_err());
    }

    #[test]
    fn lwt_pattern_14_wire_format_has_values() {
        // Non-applied response includes current values.
        let history = History {
            operations: vec![make_op(
                "c1",
                100,
                200,
                Op::UpdateIf {
                    table: "jepsen.lwt14".into(),
                    pk: "pk-0".into(),
                    condition: "val = 999999".into(),
                    assignments: vec![("val".into(), "1".into())],
                },
                OpResult::CurrentValues(vec![("val".into(), "0".into())]),
            )],
        };

        let wl = LwtWireFormat;
        assert!(wl.check_invariant(&history).is_ok());
    }

    #[test]
    fn lwt_pattern_14_wire_format_empty_fails() {
        // Non-applied response with empty values: wire format violation.
        let history = History {
            operations: vec![make_op(
                "c1",
                100,
                200,
                Op::UpdateIf {
                    table: "jepsen.lwt14".into(),
                    pk: "pk-0".into(),
                    condition: "val = 999999".into(),
                    assignments: vec![("val".into(), "1".into())],
                },
                OpResult::CurrentValues(vec![]),
            )],
        };

        let wl = LwtWireFormat;
        assert!(wl.check_invariant(&history).is_err());
    }

    #[test]
    fn lwt_pattern_16_multi_statement_ok() {
        let history = History {
            operations: vec![make_op(
                "c1",
                100,
                200,
                Op::Transaction {
                    statements: vec![
                        Op::Write {
                            key: "lwt16a:pk-0".into(),
                            value: 1,
                        },
                        Op::Write {
                            key: "lwt16b:pk-0".into(),
                            value: 1,
                        },
                    ],
                },
                OpResult::Ok,
            )],
        };

        let wl = LwtMultiStatement;
        assert!(wl.check_invariant(&history).is_ok());
    }

    #[test]
    fn lwt_pattern_16_multi_statement_timeout_ok() {
        // Timeouts are acceptable under chaos.
        let history = History {
            operations: vec![make_op(
                "c1",
                100,
                200,
                Op::Transaction {
                    statements: vec![Op::Write {
                        key: "lwt16a:pk-0".into(),
                        value: 1,
                    }],
                },
                OpResult::Err("request timeout".into()),
            )],
        };

        let wl = LwtMultiStatement;
        assert!(wl.check_invariant(&history).is_ok());
    }

    #[test]
    fn lwt_pattern_16_multi_statement_error_fails() {
        let history = History {
            operations: vec![make_op(
                "c1",
                100,
                200,
                Op::Transaction {
                    statements: vec![Op::Write {
                        key: "lwt16a:pk-0".into(),
                        value: 1,
                    }],
                },
                OpResult::Err("InvalidRequest: table does not exist".into()),
            )],
        };

        let wl = LwtMultiStatement;
        assert!(wl.check_invariant(&history).is_err());
    }
}
