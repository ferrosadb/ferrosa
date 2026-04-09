//! Concurrent LWW test against a running cluster.
//!
//! 8 writers with different timestamp ranges write to 200 keys for 30 seconds,
//! triggering memtable flushes. Then verifies every key has the highest-timestamp value.
//!
//! Run: cargo run -p ferrosa-cql --example concurrent_lww_test
//!
//! Requires a running 3-node cluster (./scripts/cluster.sh up).

use ferrosa_cql::client::CqlClient;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    let nodes: Vec<SocketAddr> = vec![
        "127.0.0.1:9042".parse().unwrap(),
        "127.0.0.1:9043".parse().unwrap(),
        "127.0.0.1:9044".parse().unwrap(),
    ];

    // Setup
    let mut c = CqlClient::connect(nodes[0]).await.expect("connect");
    c.query("CREATE KEYSPACE IF NOT EXISTS lww_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}").await.ok();
    c.query("USE lww_test").await.expect("USE");
    c.query("CREATE TABLE IF NOT EXISTS t (pk text, ck int, val blob, PRIMARY KEY (pk, ck))")
        .await
        .ok();
    c.query("TRUNCATE t").await.ok();
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(c);

    let num_writers = 8usize;
    let key_space = 5_000usize;
    let test_duration = Duration::from_secs(60);
    let stop = Arc::new(AtomicBool::new(false));

    // Ground truth: key → (value, timestamp)
    type GT = Arc<Mutex<HashMap<String, (Vec<u8>, i64)>>>;
    let gt: GT = Arc::new(Mutex::new(HashMap::new()));

    // Spawn writers
    let mut handles = Vec::new();
    for worker_id in 0..num_writers {
        let node = nodes[worker_id % nodes.len()];
        let stop = Arc::clone(&stop);
        let gt = Arc::clone(&gt);

        handles.push(tokio::spawn(async move {
            let mut client = CqlClient::connect(node).await.expect("connect");
            client.query("USE lww_test").await.expect("USE");

            let mut ts = (worker_id as i64) * 1_000_000_000;
            let mut write_count = 0u64;

            while !stop.load(Ordering::Relaxed) {
                let key_idx = ((worker_id * 31 + write_count as usize * 7) % key_space) as u32;
                let key_str = format!("k{key_idx:06}");
                ts += 1;

                // Large deterministic value (1024-4096 bytes like the loadgen)
                let val_len = 1024 + ((ts as usize * 31) % 3072);
                let value: Vec<u8> = (0..val_len)
                    .map(|i| ((ts as u8).wrapping_add(i as u8)).wrapping_mul(37))
                    .collect();
                let val_hex: String = value.iter().map(|b| format!("{b:02x}")).collect();

                let cql = format!(
                    "INSERT INTO t (pk, ck, val) VALUES ('{key_str}', 1, 0x{val_hex}) \
                     USING TIMESTAMP {ts}"
                );

                match client.query_quorum(&cql).await {
                    Ok(_) => {
                        // Record in ground truth (LWW by timestamp)
                        let mut map = gt.lock().unwrap();
                        let entry = map.entry(key_str).or_insert_with(|| (Vec::new(), 0));
                        if ts >= entry.1 {
                            entry.0 = value;
                            entry.1 = ts;
                        }
                        write_count += 1;
                    }
                    Err(e) => {
                        eprintln!("worker {worker_id} write error: {e}");
                    }
                }
            }
            eprintln!("worker {worker_id}: {write_count} writes, final ts={ts}");
        }));
    }

    // Spawn reader workers (like the loadgen)
    for reader_id in 0..8usize {
        let node = nodes[reader_id % nodes.len()];
        let stop = Arc::clone(&stop);
        handles.push(tokio::spawn(async move {
            let mut client = CqlClient::connect(node).await.expect("connect reader");
            client.query("USE lww_test").await.expect("USE");
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key_idx = ((reader_id * 13 + count as usize * 11) % key_space) as u32;
                let cql = format!("SELECT val FROM t WHERE pk = 'k{key_idx:06}' AND ck = 1");
                let _ = client.query_quorum(&cql).await;
                count += 1;
            }
            eprintln!("reader {reader_id}: {count} reads");
        }));
    }

    // Run for test_duration
    let start = Instant::now();
    while start.elapsed() < test_duration {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let elapsed = start.elapsed().as_secs();
        eprint!("\r  running... {elapsed}s / {}s", test_duration.as_secs());
    }
    eprintln!();
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.await.ok();
    }

    // Verify — clone the map so we don't hold the lock across await points.
    let snapshot: HashMap<String, (Vec<u8>, i64)> = gt.lock().unwrap().clone();
    let mut mismatches = 0u32;
    let mut missing = 0u32;
    let mut checked = 0u32;

    let mut client = CqlClient::connect(nodes[0]).await.expect("connect");
    client.query("USE lww_test").await.expect("USE");

    for (key, (expected_val, expected_ts)) in snapshot.iter() {
        let cql = format!("SELECT val FROM t WHERE pk = '{key}' AND ck = 1");
        match client.query_quorum(&cql).await {
            Ok(result) => {
                if result.rows.is_empty() {
                    missing += 1;
                    if missing <= 3 {
                        eprintln!("MISSING: {key}");
                    }
                } else if let Some(got) = result.rows[0].columns.first().and_then(|c| c.as_deref())
                {
                    if got != expected_val.as_slice() {
                        mismatches += 1;
                        if mismatches <= 5 {
                            let got_str = std::str::from_utf8(got).unwrap_or("(binary)");
                            let exp_str = std::str::from_utf8(expected_val).unwrap_or("(binary)");
                            eprintln!(
                                "MISMATCH {key}: expected '{exp_str}' (ts={expected_ts}), \
                                 got '{got_str}'"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("read error for {key}: {e}");
                mismatches += 1;
            }
        }
        checked += 1;
    }

    eprintln!("\n=== Result: {checked} keys, {mismatches} mismatches, {missing} missing ===");
    if mismatches == 0 && missing == 0 {
        eprintln!("PASS");
    } else {
        eprintln!("FAIL");
        std::process::exit(1);
    }
}
