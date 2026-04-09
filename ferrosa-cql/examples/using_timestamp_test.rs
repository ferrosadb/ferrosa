//! Test USING TIMESTAMP with concurrent writes across multiple nodes.
use ferrosa_cql::client::CqlClient;

#[tokio::main]
async fn main() {
    let nodes: Vec<std::net::SocketAddr> = vec![
        "127.0.0.1:9042".parse().unwrap(),
        "127.0.0.1:9043".parse().unwrap(),
        "127.0.0.1:9044".parse().unwrap(),
    ];

    // Setup via node1
    let mut c1 = CqlClient::connect(nodes[0]).await.expect("connect node1");
    c1.query("CREATE KEYSPACE IF NOT EXISTS ts_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}").await.ok();
    c1.query("USE ts_test").await.expect("USE");
    c1.query("CREATE TABLE IF NOT EXISTS t (pk text PRIMARY KEY, val blob)")
        .await
        .ok();
    c1.query("TRUNCATE t").await.ok();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 1: Write HIGH ts through node1, then LOW ts through node2
    println!("--- Test 1: high-then-low across nodes ---");
    c1.query("INSERT INTO t (pk, val) VALUES ('k1', 0xAAAA) USING TIMESTAMP 9999999")
        .await
        .expect("write high via node1");

    let mut c2 = CqlClient::connect(nodes[1]).await.expect("connect node2");
    c2.query("USE ts_test").await.expect("USE");
    c2.query("INSERT INTO t (pk, val) VALUES ('k1', 0xBBBB) USING TIMESTAMP 1111111")
        .await
        .expect("write low via node2");

    let mut pass = true;
    for (i, node) in nodes.iter().enumerate() {
        let mut c = CqlClient::connect(*node).await.expect("connect");
        c.query("USE ts_test").await.expect("USE");
        let r = c
            .query("SELECT val FROM t WHERE pk = 'k1'")
            .await
            .expect("read");
        let val = r.rows[0].columns[0].as_ref().expect("not null");
        if val == &[0xAA, 0xAA] {
            println!("  node{}: PASS", i + 1);
        } else {
            println!("  node{}: FAIL (got {:?}, expected [AA, AA])", i + 1, val);
            pass = false;
        }
    }
    println!("Result: {}\n", if pass { "PASS" } else { "FAIL" });

    // Test 2: 10 sequential writes from different nodes, highest ts should win
    println!("--- Test 2: 10 writes, highest ts=9999999 at i=5 ---");
    for i in 0..10u8 {
        let node = nodes[i as usize % 3];
        let mut c = CqlClient::connect(node).await.expect("connect");
        c.query("USE ts_test").await.expect("USE");
        let ts = if i == 5 { 9999999 } else { 1000000 + i as i64 };
        let cql =
            format!("INSERT INTO t (pk, val) VALUES ('k3', 0x{i:02X}{i:02X}) USING TIMESTAMP {ts}");
        c.query(&cql).await.expect("write");
    }

    let mut pass = true;
    for (i, node) in nodes.iter().enumerate() {
        let mut c = CqlClient::connect(*node).await.expect("connect");
        c.query("USE ts_test").await.expect("USE");
        let r = c
            .query("SELECT val FROM t WHERE pk = 'k3'")
            .await
            .expect("read");
        let val = r.rows[0].columns[0].as_ref().expect("not null");
        if val == &[0x05, 0x05] {
            println!("  node{}: PASS", i + 1);
        } else {
            println!("  node{}: FAIL (got {:?}, expected [05, 05])", i + 1, val);
            pass = false;
        }
    }
    println!("Result: {}", if pass { "PASS" } else { "FAIL" });
}
