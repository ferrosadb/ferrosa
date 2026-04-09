//! Regression test: LWW must survive memtable flushes in cluster mode.
//!
//! Scenario:
//! 1. Write value A at high timestamp (ts=9999999)
//! 2. Write enough data to trigger a memtable flush (64MB default threshold)
//! 3. Write value B at LOW timestamp (ts=1111111) to the SAME key
//! 4. Read back — must get value A (higher timestamp wins LWW)
//!
//! This test caught a bug where the SSTable merge path lost track of
//! timestamps, causing later-arriving lower-timestamp writes to overwrite
//! higher-timestamp values after a flush.
//!
//! Run: cargo run -p ferrosa-cql --example lww_flush_regression
//! Requires: running 3-node cluster (./scripts/cluster.sh up)

use ferrosa_cql::client::CqlClient;
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let nodes: Vec<SocketAddr> = vec![
        "127.0.0.1:9042".parse().unwrap(),
        "127.0.0.1:9043".parse().unwrap(),
        "127.0.0.1:9044".parse().unwrap(),
    ];

    // Setup
    let mut c1 = CqlClient::connect(nodes[0]).await.expect("connect node1");
    c1.query("CREATE KEYSPACE IF NOT EXISTS flush_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}").await.ok();
    c1.query("USE flush_test").await.expect("USE");
    c1.query("CREATE TABLE IF NOT EXISTS t (pk text, ck int, val blob, PRIMARY KEY (pk, ck))")
        .await
        .ok();
    c1.query("TRUNCATE t").await.ok();
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("=== Phase 1: Write high-ts value to target key via node1 ===");
    let high_ts = 9_999_999i64;
    let high_val = "AAAA".repeat(100); // 400 bytes, easy to identify
    let high_hex: String = high_val
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    c1.query_quorum(&format!(
        "INSERT INTO t (pk, ck, val) VALUES ('TARGET', 1, 0x{high_hex}) USING TIMESTAMP {high_ts}"
    ))
    .await
    .expect("write high-ts");
    println!(
        "  Written 'TARGET' with {} bytes at ts={high_ts}",
        high_val.len()
    );

    println!("=== Phase 2: Write bulk data to trigger flush (>64MB) ===");
    // Write through ALL nodes to ensure all replicas get enough data to flush.
    // Each write is ~4KB, need ~16K writes to hit 64MB per node.
    let bulk_val = "X".repeat(4096);
    let bulk_hex: String = bulk_val
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let mut clients: Vec<CqlClient> = Vec::new();
    for node in &nodes {
        let mut c = CqlClient::connect(*node).await.expect("connect");
        c.query("USE flush_test").await.expect("USE");
        clients.push(c);
    }

    let mut written = 0u64;
    for i in 0..20_000u32 {
        let idx = i as usize % 3;
        let client = &mut clients[idx];
        let key = format!("bulk{i:08}");
        let ts = 5_000_000i64 + i as i64; // mid-range timestamp
        if let Err(e) = client
            .query(&format!(
                "INSERT INTO t (pk, ck, val) VALUES ('{key}', 1, 0x{bulk_hex}) USING TIMESTAMP {ts}"
            ))
            .await
        {
            eprintln!("  bulk write error at i={i}: {e}");
            break;
        }
        written += bulk_val.len() as u64;
        if i % 5000 == 0 && i > 0 {
            println!("  {i} bulk writes ({} MB)", written / 1_000_000);
        }
    }
    println!("  Bulk phase done: {} MB written", written / 1_000_000);

    // Wait for flushes to settle
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("=== Phase 3: Write low-ts value to target key via node2 ===");
    let low_ts = 1_111_111i64;
    let low_val = "BBBB".repeat(50); // 200 bytes, different size
    let low_hex: String = low_val
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let mut c2 = CqlClient::connect(nodes[1]).await.expect("connect node2");
    c2.query("USE flush_test").await.expect("USE");
    c2.query_quorum(&format!(
        "INSERT INTO t (pk, ck, val) VALUES ('TARGET', 1, 0x{low_hex}) USING TIMESTAMP {low_ts}"
    ))
    .await
    .expect("write low-ts");
    println!(
        "  Written 'TARGET' with {} bytes at ts={low_ts}",
        low_val.len()
    );

    println!("=== Phase 4: Verify — high-ts value must win ===");
    let mut all_pass = true;
    for (i, node) in nodes.iter().enumerate() {
        let mut c = CqlClient::connect(*node).await.expect("connect");
        c.query("USE flush_test").await.expect("USE");
        let r = c
            .query("SELECT val FROM t WHERE pk = 'TARGET' AND ck = 1")
            .await
            .expect("read");
        let val = r.rows[0]
            .columns
            .first()
            .and_then(|c| c.as_ref())
            .expect("not null");

        if val.len() == high_val.len() && val == high_val.as_bytes() {
            println!(
                "  node{}: PASS ({} bytes = high-ts value)",
                i + 1,
                val.len()
            );
        } else if val.len() == low_val.len() {
            println!(
                "  node{}: FAIL ({} bytes = LOW-ts value — LWW violated!)",
                i + 1,
                val.len()
            );
            all_pass = false;
        } else {
            println!(
                "  node{}: FAIL ({} bytes = UNKNOWN value)",
                i + 1,
                val.len()
            );
            all_pass = false;
        }
    }

    println!();
    if all_pass {
        println!("PASS: LWW correctly preserved across flush boundary");
    } else {
        println!("FAIL: LWW violated — lower timestamp overwrote higher after flush");
        std::process::exit(1);
    }
}
