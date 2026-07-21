//! Elle `list-append` history generator for the Accord strict-serializability
//! validation (t_d7ffb5b7).
//!
//! Model: each list is a `list<int>` column (order-preserving).
//! - **append(k, v)** = `BEGIN TRANSACTION; UPDATE la SET v = v + [v] WHERE k=?;
//!   COMMIT` — an Accord-serialized write. `v` is globally unique so Elle can
//!   recover the per-key version order.
//! - **read(k)** = `SELECT v FROM la WHERE k=?` at **SERIAL** consistency — a
//!   linearizable read routed through Accord (cluster mode). Returns the list in
//!   append order.
//!
//! Concurrent workers share one pinned coordinator (so each `BEGIN…COMMIT` is
//! atomic on one connection); concurrency comes from many client tasks. Each op
//! logs an `:invoke` before and an `:ok`/`:fail` after into a shared, time-
//! ordered history, which is written as Elle-compatible EDN.
//!
//! Usage: cargo run -p ferrosa-jepsen --example elle_list_append -- <host:port> <out.edn> [keys] [ops-per-worker] [workers]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::PoolSize;
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::statement::{Consistency, Statement};
use scylla::value::{CqlValue, Row};

/// A single Elle history event (already rendered to EDN by `edn`).
struct Event(String);

fn edn_txn(process: u64, kind: &str, f: &str, value: &str) -> Event {
    Event(format!(
        "{{:process {process} :type {kind} :f {f} :value {value}}}"
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let contact = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:9042".into());
    let out_path = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "elle-history.edn".into());
    let keys: i32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(8);
    let ops_per_worker: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(200);
    let workers: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(6);
    let rf: u32 = args.get(6).and_then(|s| s.parse().ok()).unwrap_or(3);

    // Probe once to learn the coordinator node to pin to. Each worker then builds
    // its OWN pinned session (one connection) so that a BEGIN…COMMIT is atomic on
    // that worker's connection AND concurrent workers don't interleave their
    // transactions on a shared connection (the server buffers a transaction per
    // connection — a shared connection yields "nested transactions").
    let probe = SessionBuilder::new()
        .known_nodes([&contact])
        .build()
        .await?;
    let target = probe
        .get_cluster_state()
        .get_nodes_info()
        .iter()
        .min_by_key(|n| n.host_id)
        .cloned()
        .context("no known nodes")?;
    drop(probe);

    let session = Arc::new(connect_pinned(&contact, &target).await?);

    // Fresh schema. RF=3 so cluster mode is active and reads route through Accord.
    session
        .query_unpaged("DROP KEYSPACE IF EXISTS elle", &[])
        .await
        .ok();
    session
        .query_unpaged(
            format!("CREATE KEYSPACE elle WITH replication = {{'class':'SimpleStrategy','replication_factor':{rf}}}"),
            &[],
        )
        .await?;
    session
        .query_unpaged("CREATE TABLE elle.la (k int PRIMARY KEY, v list<int>)", &[])
        .await?;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let history: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
    let next_val = Arc::new(AtomicU64::new(1));
    let failures = Arc::new(AtomicU64::new(0));
    // Tally normalized BEGIN/UPDATE/COMMIT failure reasons so a run reports WHY
    // ops went :fail/:info — the key to driving indeterminate commits down (the
    // error text was previously only eprintln'd for the first 3 and lost).
    let errors: Arc<Mutex<std::collections::BTreeMap<String, u64>>> =
        Arc::new(Mutex::new(std::collections::BTreeMap::new()));

    let mut handles = Vec::new();
    for w in 0..workers {
        // Each worker gets its OWN pinned connection ⇒ its own transaction state.
        let mut session = Arc::new(connect_pinned(&contact, &target).await?);
        let history = history.clone();
        let next_val = next_val.clone();
        let failures = failures.clone();
        let errors = errors.clone();
        let contact = contact.clone();
        let target = target.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..ops_per_worker {
                // Deterministic-but-varied key/op choice per (worker, i) — no RNG
                // (Math.random-style nondeterminism is avoided; index-derived).
                let k =
                    ((w.wrapping_mul(2_654_435_761) ^ i.wrapping_mul(40_503)) as i32 % keys).abs();
                let is_append = (w + i) % 3 != 0; // ~2/3 appends, 1/3 reads
                if is_append {
                    let v = next_val.fetch_add(1, Ordering::SeqCst) as i64;
                    log(
                        &history,
                        edn_txn(w, ":invoke", ":txn", &format!("[[:append {k} {v}]]")),
                    );
                    // BEGIN/UPDATE/COMMIT are three statements on the pinned conn.
                    // Outcome classification is what makes the Elle history sound:
                    //   - BEGIN or UPDATE fails  -> the txn definitely did NOT commit
                    //     (nothing was applied): a clean :fail.
                    //   - COMMIT fails/times out -> INDETERMINATE (the write may or
                    //     may not have landed): :info, never :fail — logging it :fail
                    //     would let a write that actually committed masquerade as a
                    //     failed op and fabricate an Elle "aberrant read" violation.
                    let upd = format!("UPDATE elle.la SET v = v + [{v}] WHERE k = {k}");
                    let outcome = if let Err(e) = session
                        .query_unpaged("BEGIN TRANSACTION".to_string(), &[])
                        .await
                    {
                        Some((":fail", format!("BEGIN: {e}")))
                    } else if let Err(e) = session.query_unpaged(upd.clone(), &[]).await {
                        Some((":fail", format!("UPDATE: {e}")))
                    } else if let Err(e) = session.query_unpaged("COMMIT".to_string(), &[]).await {
                        Some((":info", format!("COMMIT: {e}")))
                    } else {
                        None
                    };
                    match outcome {
                        None => {
                            log(
                                &history,
                                edn_txn(w, ":ok", ":txn", &format!("[[:append {k} {v}]]")),
                            );
                        }
                        Some((kind, why)) => {
                            if failures.load(Ordering::Relaxed) < 3 {
                                eprintln!("APPEND {kind} on {why}");
                            }
                            *errors
                                .lock()
                                .unwrap()
                                .entry(normalize_err(kind, &why))
                                .or_insert(0) += 1;
                            // Clear any half-open transaction on THIS connection so the
                            // next BEGIN doesn't hit "nested transactions". ROLLBACK is
                            // cheap and idempotent; only reconnect if it, too, fails.
                            if session
                                .query_unpaged("ROLLBACK".to_string(), &[])
                                .await
                                .is_err()
                            {
                                if let Ok(fresh) = connect_pinned(&contact, &target).await {
                                    session = Arc::new(fresh);
                                }
                            }
                            failures.fetch_add(1, Ordering::Relaxed);
                            log(
                                &history,
                                edn_txn(w, kind, ":txn", &format!("[[:append {k} {v}]]")),
                            );
                        }
                    }
                } else {
                    log(
                        &history,
                        edn_txn(w, ":invoke", ":txn", &format!("[[:r {k} nil]]")),
                    );
                    let mut read = Statement::new(format!("SELECT v FROM elle.la WHERE k = {k}"));
                    read.set_consistency(Consistency::Serial);
                    match session.query_unpaged(read, &[]).await {
                        Ok(res) => {
                            let list = res
                                .into_rows_result()
                                .ok()
                                .and_then(|rr| {
                                    rr.rows::<Row>()
                                        .ok()
                                        .and_then(|mut it| it.next().and_then(|r| r.ok()))
                                })
                                .map(|row| decode_int_list(row.columns.first()))
                                .unwrap_or_default();
                            let elems = list
                                .iter()
                                .map(|n| n.to_string())
                                .collect::<Vec<_>>()
                                .join(" ");
                            log(
                                &history,
                                edn_txn(w, ":ok", ":txn", &format!("[[:r {k} [{elems}]]]")),
                            );
                        }
                        Err(_) => {
                            failures.fetch_add(1, Ordering::Relaxed);
                            log(
                                &history,
                                edn_txn(w, ":fail", ":txn", &format!("[[:r {k} nil]]")),
                            );
                        }
                    }
                }
            }
        }));
    }
    for h in handles {
        h.await.ok();
    }

    // Report WHY ops failed, most frequent first — the harness diagnostic that
    // tells us whether :info is a real cluster failure mode or a client artifact.
    {
        let errs = errors.lock().unwrap();
        if !errs.is_empty() {
            let mut v: Vec<(&String, &u64)> = errs.iter().collect();
            v.sort_by(|a, b| b.1.cmp(a.1));
            eprintln!("=== failure reasons (normalized, most frequent first) ===");
            for (reason, count) in v {
                eprintln!("  {count:>5}  {reason}");
            }
        }
    }

    let hist = history.lock().unwrap();
    let mut edn = String::from("[\n");
    for e in hist.iter() {
        edn.push(' ');
        edn.push_str(&e.0);
        edn.push('\n');
    }
    edn.push_str("]\n");
    std::fs::write(&out_path, edn)?;
    println!(
        "wrote {} events to {out_path} ({} failures)",
        hist.len(),
        failures.load(Ordering::Relaxed)
    );
    Ok(())
}

/// Build a session with a single connection pinned to `target` (one connection ⇒
/// one server-side transaction context), keeping the full topology connected so
/// the coordinator can fan Accord out to the other replicas.
async fn connect_pinned(contact: &str, target: &Arc<scylla::cluster::Node>) -> Result<Session> {
    let profile = ExecutionProfile::builder()
        .load_balancing_policy(SingleTargetLoadBalancingPolicy::new(
            NodeIdentifier::Node(target.clone()),
            None,
        ))
        // NO retries: the default retry policy can re-drive a timed-out statement
        // on ANOTHER node, splitting a connection-stateful BEGIN/UPDATE/COMMIT
        // across nodes ("COMMIT outside of a transaction" / "nested transactions").
        // A transaction must run exactly once on its one pinned connection.
        .retry_policy(Arc::new(scylla::policies::retry::FallthroughRetryPolicy))
        // Generous per-request timeout so a slow Accord commit under load returns
        // its real outcome instead of a client timeout that abandons the txn.
        .request_timeout(Some(std::time::Duration::from_secs(30)))
        .build();
    Ok(SessionBuilder::new()
        .known_nodes([contact])
        .default_execution_profile_handle(profile.into_handle())
        .pool_size(PoolSize::PerHost(std::num::NonZeroUsize::new(1).unwrap()))
        .build()
        .await?)
}

fn log(history: &Arc<Mutex<Vec<Event>>>, e: Event) {
    history.lock().unwrap().push(e);
}

/// Collapse a failure to a stable category so similar errors tally together:
/// the op kind plus the message with digit runs replaced by `#` (partition keys,
/// timestamps, node ids vary per op but the failure class does not) and
/// truncated. Only digits are replaced, so error words stay readable.
fn normalize_err(kind: &str, why: &str) -> String {
    let mut s: String = why
        .chars()
        .map(|c| if c.is_ascii_digit() { '#' } else { c })
        .collect();
    while s.contains("##") {
        s = s.replace("##", "#");
    }
    s.truncate(110);
    format!("{kind} {s}")
}

fn decode_int_list(col: Option<&Option<CqlValue>>) -> Vec<i64> {
    match col {
        Some(Some(CqlValue::List(items))) => items
            .iter()
            .filter_map(|v| match v {
                CqlValue::Int(n) => Some(*n as i64),
                CqlValue::BigInt(n) => Some(*n),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}
