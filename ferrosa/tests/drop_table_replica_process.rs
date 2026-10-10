//! Invariant **ALL REPLICAS AGREE** for `DROP TABLE` (forge t_c8625592) —
//! the REAL multi-node PROCESS test.
//!
//! The in-process cluster test (`ferrosa-cluster/tests/drop_table_replica_agreement.rs`)
//! drives three real Raft voters with real engines but elides the process
//! boundary. This test closes that gap: it spawns **three real `ferrosa` node
//! processes**, forms a real cluster, and drives `DROP TABLE` over the
//! **PostgreSQL wire** (there is no `cqlsh` on this host). One node's table
//! removal actually fails — its `sstables/public.drop_proc_tbl` directory is
//! made unremovable with the same order-independent POSIX mode-000 seam the
//! storage tests use — and the assertions are at the *process* level:
//!
//! 1. the client receives an **ERROR** for the refused DROP, not success;
//! 2. the applying node **does not serve** the dropped rows afterwards;
//! 3. the rows do **not come back after restarting that node**.
//!
//! RED before the fix: `ddl_path::execute_via_raft` discarded `RaftResponse::data`
//! and returned `Ok` for a DROP the applying node had refused, so the client was
//! told the DROP succeeded (assertion 1 fails). See forge t_c8625592,
//! `ferrosa-cluster` FMEA CL-47, `ferrosa-storage` FMEA ST-92.
//!
//! The topology mirrors `tests/docker-compose.cluster.yml`: node 0 is the seed,
//! nodes 1 and 2 seed off node 0. Every node binds its OWN band of eight ports
//! (CQL/web/internode/graph/bolt/postgres/sparql/flight) — a shared port is a
//! spurious `Address already in use` that looks like a code bug (see the
//! `ferrosa-node-isolation` notes). Auth is ENABLED so the PG front end accepts a
//! login at all (the PG listener is gated on `FERROSA_AUTH_ENABLED`, not the
//! deprecated `FERROSA_AUTH_DISABLED`); development mode seeds the well-known
//! `ferrosa_admin` superuser.

#![cfg(unix)]

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};

/// The dropped table. The PG front end's default schema is `public`.
const TABLE: &str = "drop_proc_tbl";
/// A value ONLY the pre-DROP rows carry. Its reappearance anywhere is the
/// resurrection this test exists to catch.
const OLD_VALUE: &str = "OLD_SURVIVOR_ROW";
const N_NODES: usize = 3;
/// `base64("ferrosa_admin:ferrosa_admin")` — the seeded superuser, development mode.
const ADMIN_BASIC: &str = "Basic ZmVycm9zYV9hZG1pbjpmZXJyb3NhX2FkbWlu";

// ── ports ─────────────────────────────────────────────────────────────────────

/// The eight listeners one node binds. A node that fails any of these never
/// becomes fully ready and must not be used as evidence.
#[derive(Clone, Copy)]
struct Ports {
    cql: u16,
    web: u16,
    internode: u16,
    graph: u16,
    bolt: u16,
    pg: u16,
    sparql: u16,
    flight: u16,
}

fn free_port(taken: &mut HashSet<u16>) -> u16 {
    for _ in 0..200 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        if taken.insert(port) {
            return port;
        }
    }
    panic!("could not allocate a free port");
}

fn alloc_ports(taken: &mut HashSet<u16>) -> Ports {
    Ports {
        cql: free_port(taken),
        web: free_port(taken),
        internode: free_port(taken),
        graph: free_port(taken),
        bolt: free_port(taken),
        pg: free_port(taken),
        sparql: free_port(taken),
        flight: free_port(taken),
    }
}

// ── node process ──────────────────────────────────────────────────────────────

struct Node {
    /// Kept alive for the whole test; its directory survives across a restart of
    /// `child`.
    _dir: tempfile::TempDir,
    data_dir: PathBuf,
    ports: Ports,
    host_id: String,
    child: Child,
}

impl Node {
    fn table_dir(&self) -> PathBuf {
        self.data_dir
            .join("sstables")
            .join(format!("public.{TABLE}"))
    }

    fn log_path(&self) -> PathBuf {
        self.data_dir.join("node.log")
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Restart this node's process in place, reusing its data directory.
    fn restart(&mut self, cluster_name: &str, seed: Option<&str>) {
        self.stop();
        self.child = spawn_child(&self.data_dir, self.ports, &self.host_id, cluster_name, seed);
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Launch one `ferrosa` process against an existing data directory.
fn spawn_child(
    data_dir: &Path,
    ports: Ports,
    host_id: &str,
    cluster_name: &str,
    seed: Option<&str>,
) -> Child {
    let log = std::fs::File::create(data_dir.join("node.log")).expect("create node log");
    let stderr = log.try_clone().expect("clone log handle");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ferrosa"));
    cmd.env("RUST_MIN_STACK", "33554432") // debug-profile worker stack
        .env("RUST_LOG", "info")
        .env("FERROSA_AUTH_ENABLED", "true")
        .env("FERROSA_DATA_DIR", data_dir)
        .env("FERROSA_CLUSTER_NAME", cluster_name)
        .env("FERROSA_HOST_ID", host_id)
        .env("FERROSA_CQL_BIND", format!("127.0.0.1:{}", ports.cql))
        .env("FERROSA_WEB_BIND", format!("127.0.0.1:{}", ports.web))
        .env(
            "FERROSA_INTERNODE_BIND",
            format!("127.0.0.1:{}", ports.internode),
        )
        .env(
            "FERROSA_INTERNODE_BROADCAST",
            format!("127.0.0.1:{}", ports.internode),
        )
        .env("FERROSA_GRAPH_BIND", format!("127.0.0.1:{}", ports.graph))
        .env("FERROSA_BOLT_PORT", ports.bolt.to_string())
        .env("FERROSA_POSTGRES_BIND", format!("127.0.0.1:{}", ports.pg))
        .env("FERROSA_SPARQL_BIND", format!("127.0.0.1:{}", ports.sparql))
        .env("FERROSA_FLIGHT_BIND", format!("127.0.0.1:{}", ports.flight))
        // Flush quickly so the seeded rows reach SSTables before the DROP: the
        // resurrection this test guards is orphaned SSTables on disk, not rows
        // still in a memtable.
        .env("FERROSA_FLUSH_INTERVAL_SECS", "1")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));
    if let Some(seed) = seed {
        cmd.env("FERROSA_SEED", seed);
    }
    cmd.spawn().expect("spawn ferrosa node")
}

fn spawn_node(
    dir: tempfile::TempDir,
    ports: Ports,
    host_id: String,
    cluster_name: &str,
    seed: Option<&str>,
) -> Node {
    let data_dir = dir.path().to_path_buf();
    let child = spawn_child(&data_dir, ports, &host_id, cluster_name, seed);
    Node {
        _dir: dir,
        data_dir,
        ports,
        host_id,
        child,
    }
}

// ── readiness ─────────────────────────────────────────────────────────────────

/// A minimal HTTP/1.1 GET. Returns the raw response text, or `None` if the
/// listener is not up yet.
fn http_get(port: u16, path: &str, authorization: Option<&str>) -> Option<String> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(500)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
    let auth = authorization
        .map(|a| format!("Authorization: {a}\r\n"))
        .unwrap_or_default();
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{auth}Connection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

/// Extract a top-level `"key":"value"` string field from a small flat JSON object,
/// without pulling in a JSON parser.
fn json_str(body: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":\"");
    let start = body.find(&needle)? + needle.len();
    let rest = &body[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Wait until the node reports `{"ready":true}` on `/readyz` (leader elected and
/// every listener up). Panics with the node log if it never does.
fn wait_ready(node: &Node, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(body) = http_get(node.ports.web, "/readyz", None) {
            if body.contains("\"ready\":true") {
                return;
            }
        }
        if Instant::now() >= deadline {
            let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
            panic!(
                "node {} did not become ready within {timeout:?}; web={} log tail:\n{}",
                node.host_id,
                node.ports.web,
                tail(&log, 40)
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// The node reporting `ferrosa_raft_is_leader 1` on the unauthenticated
/// `/metrics` endpoint — the Raft leader that a DDL `client_write` will apply on.
fn find_leader(nodes: &[Node]) -> Option<usize> {
    nodes.iter().position(|node| {
        http_get(node.ports.web, "/metrics", None)
            .map(|body| {
                body.lines()
                    .any(|line| line.trim() == "ferrosa_raft_is_leader 1")
            })
            .unwrap_or(false)
    })
}

/// Whether this node has installed the Raft-replicated `DdlPath::Cluster`.
fn ddl_path_is_cluster(node: &Node) -> bool {
    http_get(node.ports.web, "/api/cluster/status", Some(ADMIN_BASIC))
        .map(|body| json_str(&body, "ddl_path").as_deref() == Some("cluster"))
        .unwrap_or(false)
}

// ── PostgreSQL wire ───────────────────────────────────────────────────────────

async fn pg_connect(port: u16) -> Client {
    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("ferrosa_admin")
        .password("ferrosa_admin")
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .expect("PostgreSQL SCRAM handshake must succeed as ferrosa_admin");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// `SELECT id, v FROM TABLE`. A relation-not-exist error is returned as `Err`
/// (also "not serving the dropped rows").
async fn read_rows(client: &Client) -> Result<Vec<(i32, String)>, String> {
    let rows = client
        .query(&format!("SELECT id, v FROM {TABLE}"), &[])
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| (row.get::<_, i32>("id"), row.get::<_, String>("v")))
        .collect())
}

fn serves_old(rows: &[(i32, String)]) -> bool {
    rows.iter().any(|(_, v)| v == OLD_VALUE)
}

/// A Postgres error rendered with its SQLSTATE, message and detail — the bare
/// `Display` is only "db error".
fn describe(error: &tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!(
            "SQLSTATE {} {} (detail {:?})",
            db.code().code(),
            db.message(),
            db.detail()
        ),
        None => format!("{error:?}"),
    }
}

// ── filesystem seam ───────────────────────────────────────────────────────────

/// Restores the directory mode so the tempdir can be cleaned up.
struct RestoreDirMode(PathBuf);

impl Drop for RestoreDirMode {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(md) = std::fs::metadata(&self.0) {
            let mut p = md.permissions();
            p.set_mode(0o755);
            let _ = std::fs::set_permissions(&self.0, p);
        }
    }
}

/// Make the table's SSTable directory **unremovable** so the node's
/// `remove_dir_all` cannot complete — the order-independent form of "the DROP's
/// removal failed". Guarantees the directory exists and is non-empty first.
fn sabotage_table_dir(table_dir: &Path) -> RestoreDirMode {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(table_dir).expect("create table dir");
    // A sentinel makes the directory non-empty even if the engine has not flushed
    // any SSTable yet, so the refused drop has something it cannot remove.
    std::fs::write(table_dir.join("drop-sabotage.sentinel"), b"unremovable")
        .expect("write sentinel");
    let mut perms = std::fs::metadata(table_dir).unwrap().permissions();
    perms.set_mode(0o000);
    std::fs::set_permissions(table_dir, perms).expect("chmod 000 table dir");
    RestoreDirMode(table_dir.to_path_buf())
}

// ── the test ──────────────────────────────────────────────────────────────────

/// Debug helper: drive an already-running cluster's PG port. Ignored unless
/// `DROP_ATTACH_PG` names a port. Not part of the invariant gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn debug_attach_manual_cluster() {
    let Ok(port) = std::env::var("DROP_ATTACH_PG") else {
        return;
    };
    let port: u16 = port.parse().unwrap();
    let client = pg_connect(port).await;
    eprintln!(
        "CREATE: {:?}",
        client
            .batch_execute(&format!("CREATE TABLE {TABLE} (id int PRIMARY KEY, v text)"))
            .await
            .map_err(|e| describe(&e))
    );
    eprintln!(
        "INSERT: {:?}",
        client
            .execute(&format!("INSERT INTO {TABLE} (id, v) VALUES (1, '{OLD_VALUE}')"), &[])
            .await
            .map_err(|e| describe(&e))
    );
    eprintln!("SELECT: {:?}", read_rows(&client).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_drop_on_a_real_node_process_fails_loud_and_never_resurrects() {
    let cluster_name = format!("drop-proc-{}", std::process::id());
    let mut taken: HashSet<u16> = HashSet::new();

    // Allocate every node's ports up front, so each node can be given the others
    // as seeds. Every node seeds off the other two (a full mesh): that makes the
    // auto-created `public` keyspace RF=3 on every node — its replication factor
    // is `seeds + 1` — so an Accord write has two remote replicas to reach a
    // quorum from. With the docker-compose star (node 0 has no seed) the keyspace
    // is RF=1 and an Accord write that resolves to the coordinator's own host
    // fails "unknown peer: <own host_id>" and never commits.
    let all_ports: Vec<Ports> = (0..N_NODES).map(|_| alloc_ports(&mut taken)).collect();
    let internode: Vec<String> = all_ports
        .iter()
        .map(|p| format!("127.0.0.1:{}", p.internode))
        .collect();
    let seeds_for = |i: usize| -> String {
        internode
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, a)| a.clone())
            .collect::<Vec<_>>()
            .join(",")
    };

    let mut nodes: Vec<Node> = Vec::new();
    for i in 0..N_NODES {
        let dir = tempfile::tempdir().expect("node data dir");
        // Accord derives a node's numeric id from the FIRST eight bytes of its
        // host UUID, so the distinguishing group must lead: three host ids that
        // differ only in a trailing group collide on one Accord node id and every
        // write fails "unknown peer" against a self-send.
        let host_id = format!("0000000{i}-0000-0000-0000-000000000000");
        let node = spawn_node(dir, all_ports[i], host_id, &cluster_name, Some(&seeds_for(i)));
        nodes.push(node);
    }
    for node in &nodes {
        wait_ready(node, Duration::from_secs(90));
    }

    // Every node must be fully up; a half-bound cluster invalidates everything.
    for node in &nodes {
        let log = std::fs::read_to_string(node.log_path()).unwrap_or_default();
        assert!(
            !log.contains("Address already in use"),
            "node {} hit a port collision — the cluster is not healthy:\n{}",
            node.host_id,
            tail(&log, 30)
        );
    }

    // The DDL must route through the Raft-replicated path; give the cluster time
    // to install `DdlPath::Cluster` and elect a leader.
    {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if ddl_path_is_cluster(&nodes[0]) && find_leader(&nodes).is_some() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "cluster never installed the replicated DDL path / elected a leader within 60s"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    let leader = find_leader(&nodes).expect("a leader is present once the cluster formed");

    // Create the table through the SAME replicated DDL path CQL uses, via PG.
    // Retry while the default `public` keyspace is being created at boot.
    let client = pg_connect(nodes[leader].ports.pg).await;
    {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match client
                .batch_execute(&format!("CREATE TABLE {TABLE} (id int PRIMARY KEY, v text)"))
                .await
            {
                Ok(()) => break,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "CREATE TABLE {TABLE} never succeeded: {}",
                        describe(&e)
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    // Seed distinctive rows — one statement each, since a multi-row INSERT is
    // refused fail-loud on a cluster. The first insert doubles as the wait for
    // full Accord membership: right after bootstrap the peer set can still be
    // incomplete, so a write is briefly refused with "Accord quorum unavailable".
    for id in 1..=3 {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match client
                .execute(
                    &format!("INSERT INTO {TABLE} (id, v) VALUES ({id}, '{OLD_VALUE}')"),
                    &[],
                )
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "INSERT row {id} never succeeded: {}\n--- leader log tail ---\n{}",
                        describe(&e),
                        tail(
                            &std::fs::read_to_string(nodes[leader].log_path()).unwrap_or_default(),
                            40
                        )
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    // Let the fast flush interval land the rows in SSTables, then confirm the
    // rows are readable and the applied table's directory is on disk.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let pre = read_rows(&client)
        .await
        .unwrap_or_else(|e| panic!("SELECT of seeded rows failed: {e}"));
    assert!(
        serves_old(&pre),
        "precondition: the seeded rows must be readable before the DROP, got {pre:?}"
    );
    assert!(
        nodes[leader].table_dir().exists(),
        "precondition: the applied table's directory must exist on disk before the DROP"
    );

    // Sabotage EVERY node's table directory, so whichever node applies the DROP
    // (and every follower that replays it) cannot remove this table's SSTables.
    let guards: Vec<RestoreDirMode> = nodes
        .iter()
        .map(|n| sabotage_table_dir(&n.table_dir()))
        .collect();

    // (1) The refused DROP must FAIL LOUD to the client, not report success.
    let drop_result = client.batch_execute(&format!("DROP TABLE {TABLE}")).await;
    assert!(
        drop_result.is_err(),
        "a DROP the applying node refused must surface an ERROR to the client, \
         but the PostgreSQL DROP TABLE reported success"
    );
    eprintln!(
        "refused DROP surfaced to the client as: {}",
        describe(drop_result.as_ref().unwrap_err())
    );

    // The removal never completed: the directory (with its sentinel) is still on
    // disk, which is what makes the next assertions meaningful.
    assert!(
        nodes[leader].table_dir().exists(),
        "the refused removal must have left the table directory on disk"
    );

    // (2) No node may serve the dropped rows afterwards.
    for node in &nodes {
        let c = pg_connect(node.ports.pg).await;
        match read_rows(&c).await {
            Ok(rows) => assert!(
                !serves_old(&rows),
                "node {} still serves the dropped table's rows: {rows:?}",
                node.host_id
            ),
            Err(_) => { /* relation does not exist — also not serving the rows */ }
        }
    }

    // Restore the directory modes so the node can start and sweep on restart.
    drop(guards);

    // (3) Restart the node that APPLIED the drop; the rows must not come back.
    let seed = seeds_for(leader);
    let restarted = &mut nodes[leader];
    restarted.restart(&cluster_name, Some(&seed));
    wait_ready(restarted, Duration::from_secs(90));

    let c = pg_connect(restarted.ports.pg).await;
    match read_rows(&c).await {
        Ok(rows) => assert!(
            !serves_old(&rows),
            "after restarting the applying node, the dropped rows came back: {rows:?}"
        ),
        Err(_) => { /* relation does not exist — the durable drop held */ }
    }
}
