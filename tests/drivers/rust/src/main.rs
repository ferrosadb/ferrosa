/// CQL driver smoke tests using the scylla Rust driver.
///
/// Each test is idempotent and uses the `rust_test` keyspace.
/// Exits with code 0 on success, 1 on any failure.
use std::env;
use std::sync::atomic::{AtomicU32, Ordering};

use chrono::Utc;
use scylla::frame::value::CqlTimestamp;
use scylla::statement::batch::Batch;
use scylla::statement::batch::BatchType;
use scylla::Session;
use scylla::SessionBuilder;
use uuid::Uuid;

const KEYSPACE: &str = "rust_test";

static PASSED: AtomicU32 = AtomicU32::new(0);
static FAILED: AtomicU32 = AtomicU32::new(0);

fn ferrosa_host() -> String {
    env::var("FERROSA_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

fn ferrosa_port() -> u16 {
    env::var("FERROSA_CQL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9042)
}

/// Run a named test, printing PASS/FAIL and updating counters.
async fn run_test<F, Fut>(name: &str, f: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    match f().await {
        Ok(()) => {
            println!("  PASS  {}", name);
            PASSED.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            eprintln!("  FAIL  {}", name);
            eprintln!("        {}", e);
            FAILED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Helper: fully-qualified table name.
fn fq(table: &str) -> String {
    format!("{}.{}", KEYSPACE, table)
}

#[tokio::main]
async fn main() {
    println!("Rust (scylla) CQL driver smoke tests");
    println!("=====================================\n");

    let addr = format!("{}:{}", ferrosa_host(), ferrosa_port());

    // ---- Connection ---------------------------------------------------------

    let session: Session = match SessionBuilder::new().known_node(&addr).build().await {
        Ok(s) => {
            println!("  PASS  connect");
            PASSED.fetch_add(1, Ordering::Relaxed);
            s
        }
        Err(e) => {
            eprintln!("  FAIL  connect");
            eprintln!("        {}", e);
            eprintln!("\nCannot proceed without a session. Aborting.");
            std::process::exit(1);
        }
    };

    // ---- Introspection ------------------------------------------------------

    run_test("system_local", || async {
        let res = session
            .query_unpaged("SELECT cluster_name, data_center FROM system.local", &[])
            .await?;
        let rows_result = res.into_rows_result()?;
        let (cluster_name, _dc): (Option<String>, Option<String>) = rows_result.first_row()?;
        assert!(cluster_name.is_some(), "cluster_name should not be null");
        Ok(())
    })
    .await;

    run_test("system_peers", || async {
        let res = session
            .query_unpaged("SELECT * FROM system.peers", &[])
            .await?;
        // Single-node may have 0 peers; just verify no error.
        assert!(res.is_rows());
        Ok(())
    })
    .await;

    run_test("system_schema_keyspaces", || async {
        let res = session
            .query_unpaged("SELECT * FROM system_schema.keyspaces", &[])
            .await?;
        assert!(res.is_rows());
        Ok(())
    })
    .await;

    // ---- DDL ----------------------------------------------------------------

    run_test("create_keyspace", || async {
        let q = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} \
             WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}",
            KEYSPACE
        );
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    run_test("create_table_users", || async {
        let q = format!(
            "CREATE TABLE IF NOT EXISTS {}.users (\
                id int PRIMARY KEY, \
                name text, \
                email text, \
                active boolean, \
                score float, \
                rating double, \
                age bigint, \
                profile blob, \
                user_uuid uuid, \
                created_at timestamp\
             )",
            KEYSPACE
        );
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    run_test("create_table_events", || async {
        let q = format!(
            "CREATE TABLE IF NOT EXISTS {}.events (\
                user_id int, \
                ts timestamp, \
                data text, \
                PRIMARY KEY (user_id, ts)\
             )",
            KEYSPACE
        );
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    run_test("create_table_collections", || async {
        let q = format!(
            "CREATE TABLE IF NOT EXISTS {}.collections (\
                id int PRIMARY KEY, \
                tags list<text>, \
                scores set<int>, \
                props map<text, text>\
             )",
            KEYSPACE
        );
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    run_test("alter_table_add_column", || async {
        let q = format!("ALTER TABLE {}.users ADD phone text", KEYSPACE);
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    run_test("create_index", || async {
        let q = format!("CREATE INDEX IF NOT EXISTS ON {}.users (name)", KEYSPACE);
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    // ---- DML writes ---------------------------------------------------------

    run_test("insert_text_int", || async {
        let q = format!(
            "INSERT INTO {} (id, name, email) VALUES (1, 'Alice', 'alice@test.com')",
            fq("users")
        );
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    run_test("insert_boolean", || async {
        let q = format!(
            "INSERT INTO {} (id, name, active) VALUES (2, 'BoolUser', true)",
            fq("users")
        );
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    run_test("insert_float_double", || async {
        let q = format!(
            "INSERT INTO {} (id, score, rating) VALUES (3, 95.5, 99.12345678)",
            fq("users")
        );
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    run_test("insert_bigint", || async {
        let q = format!(
            "INSERT INTO {} (id, age) VALUES (4, 9223372036854775807)",
            fq("users")
        );
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    run_test("insert_blob", || async {
        let q = format!(
            "INSERT INTO {} (id, profile) VALUES (5, 0xdeadbeef)",
            fq("users")
        );
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    run_test("insert_uuid", || async {
        let test_uuid = Uuid::new_v4();
        let q = format!("INSERT INTO {} (id, user_uuid) VALUES (?, ?)", fq("users"));
        session.query_unpaged(q, (6_i32, test_uuid)).await?;
        Ok(())
    })
    .await;

    run_test("insert_timestamp", || async {
        let now = Utc::now();
        let ts = CqlTimestamp(now.timestamp_millis());
        let q = format!("INSERT INTO {} (id, created_at) VALUES (?, ?)", fq("users"));
        session.query_unpaged(q, (7_i32, ts)).await?;
        Ok(())
    })
    .await;

    run_test("insert_null", || async {
        // Insert a row with an explicit NULL for the email column.
        let null_text: Option<String> = None;
        let q = format!(
            "INSERT INTO {} (id, name, email) VALUES (?, ?, ?)",
            fq("users")
        );
        session
            .query_unpaged(q, (8_i32, "NullUser", null_text))
            .await?;
        Ok(())
    })
    .await;

    run_test("insert_if_not_exists", || async {
        // First insert should be applied.
        let q = format!(
            "INSERT INTO {} (id, name) VALUES (900, 'LwtUser') IF NOT EXISTS",
            fq("users")
        );
        let res1 = session.query_unpaged(q, &[]).await?;
        let rows1 = res1.into_rows_result()?;
        let (applied,): (bool,) = rows1.first_row()?;
        assert!(applied, "first INSERT IF NOT EXISTS should be applied");

        // Second insert with same PK should NOT be applied.
        let q2 = format!(
            "INSERT INTO {} (id, name) VALUES (900, 'LwtUser') IF NOT EXISTS",
            fq("users")
        );
        let res2 = session.query_unpaged(q2, &[]).await?;
        let rows2 = res2.into_rows_result()?;
        let (applied2,): (bool,) = rows2.first_row()?;
        assert!(
            !applied2,
            "second INSERT IF NOT EXISTS should NOT be applied"
        );
        Ok(())
    })
    .await;

    run_test("update_row", || async {
        let q = format!(
            "UPDATE {} SET email = 'new@test.com' WHERE id = 1",
            fq("users")
        );
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    run_test("delete_row", || async {
        // Insert a row to delete.
        let q_ins = format!(
            "INSERT INTO {} (id, name) VALUES (999, 'DeleteMe')",
            fq("users")
        );
        session.query_unpaged(q_ins, &[]).await?;

        // Delete it.
        let q_del = format!("DELETE FROM {} WHERE id = 999", fq("users"));
        session.query_unpaged(q_del, &[]).await?;

        // Verify it is gone.
        let q_sel = format!("SELECT * FROM {} WHERE id = 999", fq("users"));
        let res = session.query_unpaged(q_sel, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let row: Option<(i32,)> = rows_result.maybe_first_row()?;
        assert!(row.is_none(), "deleted row should not be found");
        Ok(())
    })
    .await;

    run_test("insert_collections", || async {
        let q = format!(
            "INSERT INTO {} (id, tags, scores, props) VALUES (?, ?, ?, ?)",
            fq("collections")
        );
        session
            .query_unpaged(
                q,
                (
                    1_i32,
                    vec!["rust", "scylla", "ferrosa"],
                    vec![100_i32, 200_i32, 300_i32],
                    std::collections::HashMap::from([
                        ("env".to_string(), "test".to_string()),
                        ("driver".to_string(), "scylla-rust".to_string()),
                    ]),
                ),
            )
            .await?;
        Ok(())
    })
    .await;

    run_test("insert_clustering_events", || async {
        let q1 = format!(
            "INSERT INTO {} (user_id, ts, data) VALUES (1, '2024-01-01T00:00:00Z', 'login')",
            fq("events")
        );
        session.query_unpaged(q1, &[]).await?;

        let q2 = format!(
            "INSERT INTO {} (user_id, ts, data) VALUES (1, '2024-01-01T01:00:00Z', 'logout')",
            fq("events")
        );
        session.query_unpaged(q2, &[]).await?;
        Ok(())
    })
    .await;

    // ---- DML reads ----------------------------------------------------------

    run_test("select_by_pk", || async {
        let q = format!("SELECT id, name, email FROM {} WHERE id = 1", fq("users"));
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let (id, name, email): (i32, String, String) = rows_result.single_row()?;
        assert_eq!(id, 1);
        assert_eq!(name, "Alice");
        assert_eq!(email, "new@test.com"); // Updated by update_row test
        Ok(())
    })
    .await;

    run_test("select_boolean", || async {
        let q = format!("SELECT active FROM {} WHERE id = 2", fq("users"));
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let (active,): (bool,) = rows_result.single_row()?;
        assert!(active, "active should be true");
        Ok(())
    })
    .await;

    run_test("select_float_double", || async {
        let q = format!("SELECT score, rating FROM {} WHERE id = 3", fq("users"));
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let (score, rating): (f32, f64) = rows_result.single_row()?;
        assert!(
            (score - 95.5_f32).abs() < 0.01,
            "score mismatch: got {}",
            score
        );
        assert!(
            (rating - 99.12345678_f64).abs() < 0.0001,
            "rating mismatch: got {}",
            rating
        );
        Ok(())
    })
    .await;

    run_test("select_bigint", || async {
        let q = format!("SELECT age FROM {} WHERE id = 4", fq("users"));
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let (age,): (i64,) = rows_result.single_row()?;
        assert_eq!(age, i64::MAX, "age should be max i64");
        Ok(())
    })
    .await;

    run_test("select_clustering_range", || async {
        let q = format!(
            "SELECT data FROM {} WHERE user_id = 1 ORDER BY ts ASC",
            fq("events")
        );
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let mut events: Vec<String> = Vec::new();
        for row in rows_result.rows::<(String,)>()? {
            let (data,) = row?;
            events.push(data);
        }
        assert_eq!(events.len(), 2, "expected 2 events, got {}", events.len());
        assert_eq!(events[0], "login", "first event");
        assert_eq!(events[1], "logout", "second event");
        Ok(())
    })
    .await;

    run_test("select_count", || async {
        let q = format!("SELECT COUNT(*) FROM {}", fq("users"));
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let (count,): (i64,) = rows_result.single_row()?;
        assert!(count > 0, "count should be > 0, got {}", count);
        Ok(())
    })
    .await;

    run_test("select_limit", || async {
        let q = format!("SELECT * FROM {} LIMIT 2", fq("users"));
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let mut count = 0_u32;
        for row in rows_result.rows::<(i32,)>()? {
            let _ = row?;
            count += 1;
        }
        assert!(
            count <= 2,
            "LIMIT 2 should return at most 2 rows, got {}",
            count
        );
        Ok(())
    })
    .await;

    run_test("select_collections", || async {
        let q = format!(
            "SELECT tags, scores, props FROM {} WHERE id = 1",
            fq("collections")
        );
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let (tags, scores, props): (
            Vec<String>,
            Vec<i32>,
            std::collections::HashMap<String, String>,
        ) = rows_result.single_row()?;

        // Verify list
        assert_eq!(tags.len(), 3, "expected 3 tags");
        assert_eq!(tags[0], "rust");
        assert_eq!(tags[1], "scylla");
        assert_eq!(tags[2], "ferrosa");

        // Verify set (order may vary in a set, check contents)
        assert_eq!(scores.len(), 3, "expected 3 scores");
        assert!(scores.contains(&100));
        assert!(scores.contains(&200));
        assert!(scores.contains(&300));

        // Verify map
        assert_eq!(props.len(), 2, "expected 2 props");
        assert_eq!(props.get("env").map(|s| s.as_str()), Some("test"));
        assert_eq!(props.get("driver").map(|s| s.as_str()), Some("scylla-rust"));
        Ok(())
    })
    .await;

    run_test("select_after_update", || async {
        let q = format!("SELECT email FROM {} WHERE id = 1", fq("users"));
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let (email,): (String,) = rows_result.single_row()?;
        assert_eq!(email, "new@test.com", "email should be updated");
        Ok(())
    })
    .await;

    run_test("select_after_delete", || async {
        let q = format!("SELECT * FROM {} WHERE id = 999", fq("users"));
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let row: Option<(i32,)> = rows_result.maybe_first_row()?;
        assert!(row.is_none(), "row 999 should have been deleted");
        Ok(())
    })
    .await;

    // ---- Prepared statements ------------------------------------------------

    run_test("prepared_insert", || async {
        let prepared = session
            .prepare(format!(
                "INSERT INTO {} (id, name, email) VALUES (?, ?, ?)",
                fq("users")
            ))
            .await?;
        session
            .execute_unpaged(
                &prepared,
                (200_i32, "RustPrepared", "rust-prepared@test.com"),
            )
            .await?;
        Ok(())
    })
    .await;

    run_test("prepared_select", || async {
        let prepared = session
            .prepare(format!(
                "SELECT name, email FROM {} WHERE id = ?",
                fq("users")
            ))
            .await?;
        let res = session.execute_unpaged(&prepared, (200_i32,)).await?;
        let rows_result = res.into_rows_result()?;
        let (name, email): (String, String) = rows_result.single_row()?;
        assert_eq!(name, "RustPrepared");
        assert_eq!(email, "rust-prepared@test.com");
        Ok(())
    })
    .await;

    run_test("prepared_update", || async {
        let prepared = session
            .prepare(format!("UPDATE {} SET email = ? WHERE id = ?", fq("users")))
            .await?;
        session
            .execute_unpaged(&prepared, ("updated-prepared@test.com", 200_i32))
            .await?;

        // Verify update
        let q = format!("SELECT email FROM {} WHERE id = 200", fq("users"));
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let (email,): (String,) = rows_result.single_row()?;
        assert_eq!(email, "updated-prepared@test.com");
        Ok(())
    })
    .await;

    run_test("prepared_delete", || async {
        let prepared = session
            .prepare(format!("DELETE FROM {} WHERE id = ?", fq("users")))
            .await?;
        session.execute_unpaged(&prepared, (200_i32,)).await?;

        // Verify deletion
        let q = format!("SELECT * FROM {} WHERE id = 200", fq("users"));
        let res = session.query_unpaged(q, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let row: Option<(i32,)> = rows_result.maybe_first_row()?;
        assert!(row.is_none(), "row 200 should have been deleted");
        Ok(())
    })
    .await;

    // ---- Batch --------------------------------------------------------------

    run_test("batch_insert", || async {
        let mut batch = Batch::new(BatchType::Logged);
        let stmt = format!(
            "INSERT INTO {} (id, name, email) VALUES (?, ?, ?)",
            fq("users")
        );
        batch.append_statement(stmt.as_str());
        batch.append_statement(stmt.as_str());
        batch.append_statement(stmt.as_str());

        let values = (
            (301_i32, "BatchUser1", "batch1@test.com"),
            (302_i32, "BatchUser2", "batch2@test.com"),
            (303_i32, "BatchUser3", "batch3@test.com"),
        );

        session.batch(&batch, values).await?;

        // Verify all three rows exist.
        for id in [301, 302, 303] {
            let q = format!("SELECT id FROM {} WHERE id = ?", fq("users"));
            let res = session.query_unpaged(q, (id,)).await?;
            let rows_result = res.into_rows_result()?;
            let row: Option<(i32,)> = rows_result.maybe_first_row()?;
            assert!(row.is_some(), "batch-inserted row {} should exist", id);
        }
        Ok(())
    })
    .await;

    // ---- TTL ----------------------------------------------------------------

    run_test("insert_with_ttl", || async {
        let q = format!(
            "INSERT INTO {} (id, name) VALUES (800, 'TtlUser') USING TTL 1",
            fq("users")
        );
        session.query_unpaged(q, &[]).await?;

        // Wait for TTL to expire.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let q_sel = format!("SELECT * FROM {} WHERE id = 800", fq("users"));
        let res = session.query_unpaged(q_sel, &[]).await?;
        let rows_result = res.into_rows_result()?;
        let row: Option<(i32,)> = rows_result.maybe_first_row()?;
        assert!(row.is_none(), "TTL row should have expired");
        Ok(())
    })
    .await;

    // ---- Error handling -----------------------------------------------------

    run_test("query_nonexistent_table", || async {
        let q = format!("SELECT * FROM {}.nonexistent_table_xyz", KEYSPACE);
        let res = session.query_unpaged(q, &[]).await;
        assert!(
            res.is_err(),
            "querying a nonexistent table should return an error"
        );
        Ok(())
    })
    .await;

    run_test("invalid_cql_syntax", || async {
        let res = session
            .query_unpaged("NOT VALID CQL AT ALL", &[])
            .await;
        assert!(res.is_err(), "invalid CQL syntax should return an error");
        Ok(())
    })
    .await;

    // ---- Cleanup ------------------------------------------------------------

    run_test("drop_keyspace", || async {
        let q = format!("DROP KEYSPACE IF EXISTS {}", KEYSPACE);
        session.query_unpaged(q, &[]).await?;
        Ok(())
    })
    .await;

    // ---- Report -------------------------------------------------------------

    let p = PASSED.load(Ordering::Relaxed);
    let f = FAILED.load(Ordering::Relaxed);

    println!("\n=====================================");
    println!("Results: {} passed, {} failed", p, f);
    println!("=====================================");

    std::process::exit(if f > 0 { 1 } else { 0 });
}
