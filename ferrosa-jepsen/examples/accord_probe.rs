//! Isolation probe for the Accord read-visibility bug (t_12191f45): does a
//! committed write read back immediately? Compares a SCALAR column vs a LIST
//! (collection) column, and a transactional (BEGIN/COMMIT, Accord) write vs a
//! plain non-transactional write — to localize whether the empty-read is
//! collection-specific and/or Accord-apply-specific.
//!
//! Usage: cargo run -p ferrosa-jepsen --example accord_probe -- <host:port>

use anyhow::{Context, Result};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::PoolSize;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::statement::{Consistency, Statement};
use scylla::value::{CqlValue, Row};

async fn connect(contact: &str) -> Result<Session> {
    let probe = SessionBuilder::new().known_nodes([contact]).build().await?;
    let target = probe
        .get_cluster_state()
        .get_nodes_info()
        .iter()
        .min_by_key(|n| n.host_id)
        .cloned()
        .context("no known nodes")?;
    drop(probe);
    let profile = ExecutionProfile::builder()
        .load_balancing_policy(SingleTargetLoadBalancingPolicy::new(
            NodeIdentifier::Node(target),
            None,
        ))
        .retry_policy(std::sync::Arc::new(
            scylla::policies::retry::FallthroughRetryPolicy,
        ))
        .request_timeout(Some(std::time::Duration::from_secs(30)))
        .build();
    Ok(SessionBuilder::new()
        .known_nodes([contact])
        .default_execution_profile_handle(profile.into_handle())
        .pool_size(PoolSize::PerHost(std::num::NonZeroUsize::new(1).unwrap()))
        .build()
        .await?)
}

async fn read_v(s: &Session, table: &str, cl: Consistency) -> String {
    let mut st = Statement::new(format!("SELECT v FROM probe.{table} WHERE k = 1"));
    st.set_consistency(cl);
    match s.query_unpaged(st, &[]).await {
        Ok(res) => match res
            .into_rows_result()
            .ok()
            .and_then(|rr| rr.rows::<Row>().ok().and_then(|mut it| it.next().and_then(|r| r.ok())))
        {
            Some(row) => format!("{:?}", row.columns.first()),
            None => "NO ROW".to_string(),
        },
        Err(e) => format!("ERR: {e}"),
    }
}

async fn txn_write(s: &Session, stmt: &str) -> Result<()> {
    for q in ["BEGIN TRANSACTION", stmt, "COMMIT"] {
        s.query_unpaged(q.to_string(), &[]).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let contact = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:9042".into());
    let s = connect(&contact).await?;

    s.query_unpaged("DROP KEYSPACE IF EXISTS probe", &[]).await.ok();
    s.query_unpaged(
        "CREATE KEYSPACE probe WITH replication = {'class':'SimpleStrategy','replication_factor':3}",
        &[],
    )
    .await?;
    s.query_unpaged("CREATE TABLE probe.s (k int PRIMARY KEY, v int)", &[]).await?;
    s.query_unpaged("CREATE TABLE probe.l (k int PRIMARY KEY, v list<int>)", &[]).await?;
    s.query_unpaged("CREATE TABLE probe.sn (k int PRIMARY KEY, v int)", &[]).await?;
    s.query_unpaged("CREATE TABLE probe.ln (k int PRIMARY KEY, v list<int>)", &[]).await?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // 1. Accord (transactional) SCALAR write, then immediate read.
    txn_write(&s, "UPDATE probe.s SET v = 42 WHERE k = 1").await?;
    println!("[Accord txn] SCALAR  immediate: serial={}  quorum={}",
        read_v(&s, "s", Consistency::Serial).await,
        read_v(&s, "s", Consistency::Quorum).await);

    // 2. Accord (transactional) LIST append, then immediate read.
    txn_write(&s, "UPDATE probe.l SET v = v + [7] WHERE k = 1").await?;
    println!("[Accord txn] LIST    immediate: serial={}  quorum={}",
        read_v(&s, "l", Consistency::Serial).await,
        read_v(&s, "l", Consistency::Quorum).await);

    // 3. Plain (non-transactional) SCALAR + LIST writes (control — no Accord).
    s.query_unpaged("UPDATE probe.sn SET v = 99 WHERE k = 1".to_string(), &[]).await?;
    s.query_unpaged("UPDATE probe.ln SET v = v + [5] WHERE k = 1".to_string(), &[]).await?;
    println!("[plain]      SCALAR  immediate: quorum={}", read_v(&s, "sn", Consistency::Quorum).await);
    println!("[plain]      LIST    immediate: quorum={}", read_v(&s, "ln", Consistency::Quorum).await);

    // 4. Re-read the Accord writes after a delay (does the collection appear later?).
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    println!("[Accord txn] SCALAR  after 5s:  quorum={}", read_v(&s, "s", Consistency::Quorum).await);
    println!("[Accord txn] LIST    after 5s:  quorum={}", read_v(&s, "l", Consistency::Quorum).await);

    let _ = CqlValue::Int(0); // keep the import used across scylla versions
    Ok(())
}
