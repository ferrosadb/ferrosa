//! `FERROSA_DATA_CENTER`, proven on real node processes.
//!
//! The operator's rule: **if a DC is set, it is used.** The reported symptom was
//! the opposite — a node started with a configured DC still reported the DC it
//! read out of `system.peers` (`datacenter1`), i.e. the value that was set was
//! not the value that took effect.
//!
//! Two real `ferrosa` processes, one per data center, the second seeding off the
//! first so the join path — the one that authors a peer's `NodeInfo` — runs with
//! two different DCs in play. Then, over the CQL wire:
//!
//! * **I1 — a configured DC stays set.** Every node's `system.local.data_center`
//!   (and `.rack`) is the value its process was started with.
//! * **I2 — a peer reports the PEER's DC, never the observer's.** For every
//!   `system.peers` row, the `data_center` belongs to the node that owns that
//!   peer port. With the stamping defect in place the dc2 peer reads the dc1
//!   observer's DC, which is exactly the "it says datacenter1" report.
//! * **I3 — the cluster actually learns the peer.** The peer must appear in
//!   `system.peers` at all; a test asserting I2 over an empty peer set would
//!   prove nothing.

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::ports::PortAllocator;
use ferrosa_cql::client::{CqlClient, ResultRow};

/// The two data centers. Deliberately AWS-region names, because that is what an
/// operator writes into `FERROSA_DATA_CENTER`, and because neither collides with
/// the `datacenter1` default the defect fell back to.
const DC_WEST: &str = "us-west-2";
const DC_EAST: &str = "eu-west-1";

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

/// A band of eight ports for one node, from the shared allocator whose ports sit
/// BELOW the kernel's ephemeral range (`tests/common/ports.rs`).
fn alloc_ports(alloc: &mut PortAllocator) -> Ports {
    Ports {
        cql: alloc.get(),
        web: alloc.get(),
        internode: alloc.get(),
        graph: alloc.get(),
        bolt: alloc.get(),
        pg: alloc.get(),
        sparql: alloc.get(),
        flight: alloc.get(),
    }
}

// ── node process ──────────────────────────────────────────────────────────────

struct Node {
    _dir: tempfile::TempDir,
    data_dir: PathBuf,
    ports: Ports,
    host_id: String,
    /// The DC this process was started with, and the rack alongside it.
    dc: String,
    rack: String,
    child: Child,
}

impl Node {
    fn log_path(&self) -> PathBuf {
        self.data_dir.join("node.log")
    }

    fn log_tail(&self, lines: usize) -> String {
        let log = std::fs::read_to_string(self.log_path()).unwrap_or_default();
        let all: Vec<&str> = log.lines().collect();
        let start = all.len().saturating_sub(lines);
        all[start..].join("\n")
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
        // A formation that fails destroys its own evidence: the node's log lives
        // inside this node's tempdir, which Drop removes -- so a failed run leaves
        // nothing but the 60-line excerpt the panic printed, and a diagnosis made
        // from an excerpt is a guess. When the caller asks to keep logs, copy the
        // log out first. (Drop also runs while unwinding, which is exactly the
        // failing path.) Opt-in: with the variable unset this changes nothing.
        if let Some(root) = std::env::var_os("FERROSA_TEST_KEEP_NODE_LOGS") {
            let dest = std::path::Path::new(&root).join(format!("{}-node.log", self.host_id));
            if let Some(parent) = dest.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::copy(self.log_path(), dest);
        }
    }
}

/// One node's launch parameters. A struct rather than eight positional
/// arguments: a swapped `dc`/`rack` at a call site is invisible in review and
/// would silently publish the wrong topology.
struct NodeSpec<'a> {
    dir: tempfile::TempDir,
    ports: Ports,
    host_id: String,
    cluster_name: &'a str,
    seed: Option<&'a str>,
    dc: &'a str,
    rack: &'a str,
    expected_cluster_size: usize,
}

/// Launch one `ferrosa` process with an explicit data center and rack.
fn spawn_node(spec: NodeSpec<'_>) -> Node {
    let NodeSpec {
        dir,
        ports,
        host_id,
        cluster_name,
        seed,
        dc,
        rack,
        expected_cluster_size,
    } = spec;
    let data_dir = dir.path().to_path_buf();
    let log = std::fs::File::create(data_dir.join("node.log")).expect("create node log");
    let stderr = log.try_clone().expect("clone log handle");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ferrosa"));
    cmd.env("RUST_MIN_STACK", "33554432") // debug-profile worker stack
        .env(
            "RUST_LOG",
            std::env::var("FERROSA_TEST_RUST_LOG").unwrap_or_else(|_| "info".into()),
        )
        .env("FERROSA_AUTH_ENABLED", "false")
        .env("FERROSA_DATA_DIR", &data_dir)
        .env("FERROSA_CLUSTER_NAME", cluster_name)
        .env("FERROSA_HOST_ID", &host_id)
        .env("FERROSA_DATA_CENTER", dc)
        .env("FERROSA_RACK", rack)
        .env(
            "FERROSA_EXPECTED_CLUSTER_SIZE",
            expected_cluster_size.to_string(),
        )
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
        .env("FERROSA_FLUSH_INTERVAL_SECS", "1")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));
    if let Some(seed) = seed {
        cmd.env("FERROSA_SEED", seed);
    }

    Node {
        _dir: dir,
        data_dir,
        ports,
        host_id,
        dc: dc.to_string(),
        rack: rack.to_string(),
        child: cmd.spawn().expect("spawn ferrosa node"),
    }
}

// ── readiness ─────────────────────────────────────────────────────────────────

/// Poll `/readyz` until the node reports ready. A cluster-mode node only does so
/// once its formation gate is satisfied, so this waits for real membership, not
/// just a bound socket.
fn wait_ready(node: &Node, timeout: Duration) {
    let url = format!("http://127.0.0.1:{}/readyz", node.ports.web);
    let deadline = Instant::now() + timeout;
    let mut last = String::new();
    while Instant::now() < deadline {
        if let Ok(body) = ureq_get(&url) {
            if body.contains("\"ready\":true") {
                return;
            }
            last = body;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!(
        "node {} (dc {}) was not ready within {timeout:?}; last /readyz body: {last}\n\
         === log tail ===\n{}",
        node.host_id,
        node.dc,
        node.log_tail(60)
    );
}

/// A minimal blocking GET, so readiness does not need the async runtime.
fn ureq_get(url: &str) -> Result<String, String> {
    use std::io::{Read, Write};
    let addr: SocketAddr = {
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| format!("unsupported url {url}"))?;
        let host_port = rest.split('/').next().unwrap_or(rest);
        host_port
            .parse()
            .map_err(|e| format!("bad host:port in {url}: {e}"))?
    };
    let mut stream = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2))
        .map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|e| e.to_string())?;
    let path = format!("/{}", url.splitn(4, '/').nth(3).unwrap_or(""));
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .map_err(|e| e.to_string())?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).map_err(|e| e.to_string())?;
    Ok(raw)
}

// ── CQL ───────────────────────────────────────────────────────────────────────

async fn cql_connect(port: u16) -> CqlClient {
    CqlClient::connect(format!("127.0.0.1:{port}").parse().unwrap())
        .await
        .expect("CQL handshake")
}

fn cell<'a>(row: &'a ResultRow, names: &[String], column: &str) -> Option<&'a [u8]> {
    let index = names.iter().position(|name| name == column)?;
    row.columns.get(index).and_then(|value| value.as_deref())
}

fn text(row: &ResultRow, names: &[String], column: &str) -> Option<String> {
    cell(row, names, column).map(|bytes| String::from_utf8_lossy(bytes).to_string())
}

fn int(row: &ResultRow, names: &[String], column: &str) -> Option<i32> {
    let bytes = cell(row, names, column)?;
    let arr: [u8; 4] = bytes.try_into().ok()?;
    Some(i32::from_be_bytes(arr))
}

/// `(data_center, rack)` this node reports about ITSELF.
async fn own_topology(client: &mut CqlClient) -> (String, String) {
    let result = client
        .query("SELECT data_center, rack FROM system.local")
        .await
        .expect("system.local");
    let row = result.rows.first().expect("system.local returns one row");
    (
        text(row, &result.column_names, "data_center").unwrap_or_default(),
        text(row, &result.column_names, "rack").unwrap_or_default(),
    )
}

/// Every peer the node knows: `(peer_port, data_center, rack)`.
async fn peer_topology(client: &mut CqlClient) -> Vec<(i32, String, String)> {
    let result = client
        .query("SELECT peer_port, data_center, rack FROM system.peers")
        .await
        .expect("system.peers");
    result
        .rows
        .iter()
        .map(|row| {
            (
                int(row, &result.column_names, "peer_port").unwrap_or(-1),
                text(row, &result.column_names, "data_center").unwrap_or_default(),
                text(row, &result.column_names, "rack").unwrap_or_default(),
            )
        })
        .collect()
}

// ── the tests ─────────────────────────────────────────────────────────────────

/// Everything the operator rule requires of a node's OWN view, on two real
/// processes in two data centers: the configured DC is the DC in use, and no
/// peer the node does know is labelled with anyone else's topology.
///
/// This is the reported defect, reproduced and closed: before the fix,
/// `system.local` on a node started with `FERROSA_DATA_CENTER=us-west-2`
/// answered `datacenter1`, because the CQL layer's `NodeConfig` was built from
/// `NodeConfig::default()` and never seeded from the resolved config.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_configured_data_center_is_used_and_stays_set() {
    let cluster_name = format!("dc-proc-{}", std::process::id());
    let mut alloc = PortAllocator::new();
    let all_ports: Vec<Ports> = (0..2).map(|_| alloc_ports(&mut alloc)).collect();
    let seed_hub = format!("127.0.0.1:{}", all_ports[0].internode);

    // Distinct host ids in both halves: Raft's `uuid_to_node_id` reads bytes[8..16]
    // and Accord reads bytes[0..8].
    let host_id = |n: u8| {
        let d = 0x11u8 * n;
        format!(
            "{d:02x}{d:02x}{d:02x}{d:02x}-{d:02x}{d:02x}-{d:02x}{d:02x}-\
             {d:02x}{d:02x}-{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}"
        )
    };

    let mut west = spawn_node(NodeSpec {
        dir: tempfile::tempdir().unwrap(),
        ports: all_ports[0],
        host_id: host_id(1),
        cluster_name: &cluster_name,
        seed: None,
        dc: DC_WEST,
        rack: "rack-west-a",
        expected_cluster_size: 1,
    });
    let mut east = spawn_node(NodeSpec {
        dir: tempfile::tempdir().unwrap(),
        ports: all_ports[1],
        host_id: host_id(2),
        cluster_name: &cluster_name,
        seed: Some(&seed_hub),
        dc: DC_EAST,
        rack: "rack-east-a",
        expected_cluster_size: 1,
    });
    let nodes = [&west, &east];

    for node in nodes {
        wait_ready(node, Duration::from_secs(90));
        assert!(
            !node.log_tail(400).contains("Address already in use"),
            "node {} hit a port collision:\n{}",
            node.host_id,
            node.log_tail(30)
        );
    }

    for node in nodes {
        let mut client = cql_connect(node.ports.cql).await;

        // I1 — the configured DC is the DC in use.
        let (dc, rack) = own_topology(&mut client).await;
        assert_eq!(
            dc,
            node.dc,
            "node {} was started with FERROSA_DATA_CENTER={} but system.local reports {dc} \
             — the configured data center is not the one in use\n=== log tail ===\n{}",
            node.host_id,
            node.dc,
            node.log_tail(40)
        );
        assert_eq!(
            rack, node.rack,
            "node {} was started with FERROSA_RACK={} but system.local reports {rack}",
            node.host_id, node.rack
        );

        // I2 — no peer is labelled with another node's topology.
        for (peer_port, peer_dc, peer_rack) in peer_topology(&mut client).await {
            let owner = nodes
                .iter()
                .find(|candidate| candidate.ports.internode as i32 == peer_port)
                .unwrap_or_else(|| {
                    panic!(
                        "node {} reported a peer on port {peer_port} that is not one of the two \
                         configured nodes",
                        node.host_id
                    )
                });
            assert_eq!(
                peer_dc, owner.dc,
                "node {} (dc {}) reports its peer (port {peer_port}, started with \
                 FERROSA_DATA_CENTER={}) as data_center={peer_dc}: a peer's committed topology \
                 is the OBSERVER's, so placement and LOCAL_* quorums are decided from a DC that \
                 peer does not belong to",
                node.host_id, node.dc, owner.dc
            );
            assert_eq!(peer_rack, owner.rack, "peer {peer_port} rack");
        }
    }

    west.stop();
    east.stop();
}

/// A peer in ANOTHER data center announces its own DC, and the receiving node
/// records what was ANNOUNCED rather than stamping the peer with its own DC.
///
/// This is the wire half of the data-center work: that `system.local` reports the
/// configured DC is pinned by `a_configured_data_center_is_used_and_stays_set`, and
/// that the cross-DC *surface* is the relay registry (not `system.peers`) is pinned
/// by `cross_dc_registry_holds_the_other_dcs_and_not_this_one` in `ferrosa-cluster`.
/// What no unit test can show is that the announcement survives a real handshake
/// between two processes — the failure that made every node report `datacenter1`
/// was that the peer's own DC never arrived at all.
///
/// A DC's Raft group holds only its own nodes (ADR-015), so a node's `system.peers`
/// lists its own DC; the other DC's nodes are reached through the registry. Tracked
/// as CL-18.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_in_another_data_center_announces_its_own_dc_on_the_handshake() {
    let cluster_name = format!("dc-cross-{}", std::process::id());
    let mut alloc = PortAllocator::new();
    let all_ports: Vec<Ports> = (0..2).map(|_| alloc_ports(&mut alloc)).collect();
    let seed_hub = format!("127.0.0.1:{}", all_ports[0].internode);

    let mut west = spawn_node(NodeSpec {
        dir: tempfile::tempdir().unwrap(),
        ports: all_ports[0],
        host_id: "11111111-1111-1111-1111-111111111111".to_string(),
        cluster_name: &cluster_name,
        seed: None,
        dc: DC_WEST,
        rack: "rack-west-a",
        expected_cluster_size: 1,
    });
    let mut east = spawn_node(NodeSpec {
        dir: tempfile::tempdir().unwrap(),
        ports: all_ports[1],
        host_id: "22222222-2222-2222-2222-222222222222".to_string(),
        cluster_name: &cluster_name,
        seed: Some(&seed_hub),
        dc: DC_EAST,
        rack: "rack-east-a",
        expected_cluster_size: 1,
    });
    wait_ready(&west, Duration::from_secs(90));
    wait_ready(&east, Duration::from_secs(90));

    // Whichever node received the connection logs the DC the OTHER announced. Poll:
    // the announcement lands when the connection is established, not when the peer
    // finishes booting. Either direction satisfies the invariant, so both logs count —
    // which side dialled is an implementation detail (the leader usually dials out).
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let west_logs_east = format!("data_center=Some(\"{DC_EAST}\")");
    let east_logs_west = format!("data_center=Some(\"{DC_WEST}\")");
    loop {
        let west_log = std::fs::read_to_string(west.log_path()).unwrap_or_default();
        let east_log = std::fs::read_to_string(east.log_path()).unwrap_or_default();
        if west_log.contains(&west_logs_east) || east_log.contains(&east_logs_west) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the peer's own DC never reached either node's record: a node in {DC_EAST} \
             must announce {DC_EAST} and a node in {DC_WEST} must announce {DC_WEST}, \
             never the receiver's own DC — that is the defect that made every node \
             report `datacenter1`\n=== west log tail ===\n{}\n=== east log tail ===\n{}",
            west.log_tail(25),
            east.log_tail(25)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    west.stop();
    east.stop();
}

// ── the cross-DC relay, on two real processes ─────────────────────────────────

/// Poll `node`'s log until it records a peer that announced `dc`.
///
/// This is the precondition the relay depends on: a DC is only a relay target
/// once this node has actually met a node of it, and the announcement is what
/// the DC registry is built from.
fn wait_for_announced_peer(node: &Node, dc: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut seen = String::new();
    while Instant::now() < deadline {
        seen = node.log_tail(600);
        if seen.contains(&format!("data_center=Some(\"{dc}\")")) {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!(
        "node {} never recorded a peer announcing {dc} — the DC registry stays empty, so no \
         cross-DC copy can be addressed\n=== log tail ===\n{seen}",
        node.host_id
    );
}

/// Connect, showing the node's own log if CQL refuses the handshake — otherwise
/// a refused STARTUP is a bare error string with no way to see why.
async fn connect_or_dump(node: &Node) -> CqlClient {
    match CqlClient::connect(format!("127.0.0.1:{}", node.ports.cql).parse().unwrap()).await {
        Ok(client) => client,
        Err(e) => panic!(
            "CQL handshake with node {} (dc {}, port {}) failed: {e}\n=== log tail ===\n{}",
            node.host_id,
            node.dc,
            node.ports.cql,
            node.log_tail(60)
        ),
    }
}

/// Does `node` have `keyspace` in its own schema right now?
async fn has_keyspace(node: &Node, keyspace: &str) -> bool {
    let mut client = connect_or_dump(node).await;
    let result = client
        .query("SELECT keyspace_name FROM system_schema.keyspaces")
        .await
        .expect("system_schema.keyspaces");
    result
        .rows
        .iter()
        .any(|row| text(row, &result.column_names, "keyspace_name").as_deref() == Some(keyspace))
}

/// The acceptance proof for the cross-DC relay: a keyspace created through CQL on
/// a node in `us-west-2` must appear on the nodes of `eu-west-1`.
///
/// Each DC needs its own three-voter Raft group. Two nodes declared with
/// expected size two exercise pair replication, and expected size one only
/// permits standalone readiness. This proof therefore runs six real processes,
/// three voters per DC, through one CQL CREATE and bounded convergence checks.
/// Readiness must include Cluster DDL publication: a live leader with Direct
/// DDL can acknowledge a local CREATE before the cross-DC relay is installed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ddl_created_in_one_dc_reaches_the_other() {
    const VOTERS_PER_DC: usize = 3;
    let cluster_name = format!("dc-relay-{}", std::process::id());
    let mut alloc = PortAllocator::new();
    let ports: Vec<Ports> = (0..VOTERS_PER_DC * 2)
        .map(|_| alloc_ports(&mut alloc))
        .collect();
    let seed_hub = format!("127.0.0.1:{}", ports[0].internode);

    // Distinct host ids in both halves: Raft's `uuid_to_node_id` reads bytes[8..16]
    // and Accord reads bytes[0..8].
    let host_id = |n: u8| {
        // A small multiplier on purpose: `0x11 * n` stays inside a `u8` for the
        // six nodes this test runs. A larger base (0x33) overflows at n = 6 —
        // which is how this test failed the first time it was ever RUN, having
        // been pinned `#[ignore]`d while the topology it needed was unavailable.
        let d = 0x11u8 * n;
        format!(
            "{d:02x}{d:02x}{d:02x}{d:02x}-{d:02x}{d:02x}-{d:02x}{d:02x}-\
             {d:02x}{d:02x}-{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}{d:02x}"
        )
    };

    let mut nodes = Vec::new();
    for (index, ports) in ports.into_iter().enumerate() {
        let (dc, rack) = if index < VOTERS_PER_DC {
            (DC_WEST, "rack-west-a")
        } else {
            (DC_EAST, "rack-east-a")
        };
        nodes.push(spawn_node(NodeSpec {
            dir: tempfile::tempdir().unwrap(),
            ports,
            host_id: host_id(index as u8 + 1),
            cluster_name: &cluster_name,
            seed: if index == 0 { None } else { Some(&seed_hub) },
            dc,
            rack,
            expected_cluster_size: VOTERS_PER_DC,
        }));
    }
    let (west_nodes, east_nodes) = nodes.split_at(VOTERS_PER_DC);

    for node in &nodes {
        wait_ready(node, Duration::from_secs(120));
        assert!(
            !node.log_tail(400).contains("Address already in use"),
            "node {} hit a port collision:\n{}",
            node.host_id,
            node.log_tail(30)
        );
    }

    // The relay targets a DC this node has met.
    wait_for_announced_peer(&west_nodes[0], DC_EAST, Duration::from_secs(60));

    let keyspace = format!("relay_probe_{}", std::process::id());
    let mut west_client = connect_or_dump(&west_nodes[0]).await;
    west_client
        .query(&format!(
            "CREATE KEYSPACE {keyspace} WITH replication = \
             {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        ))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "the local DDL must commit in its own DC: {e}\n=== west log ===\n{}",
                west_nodes[0].log_tail(40)
            )
        });

    // It must be in every node of the DC that committed it. The client's CREATE
    // returning means the local group committed it; a FOLLOWER applies the schema
    // from the replicated log asynchronously, so this is a bounded convergence check
    // rather than an instantaneous one. The bound is what makes it a test: a lag that
    // keeps growing is a real defect, so how long it took is reported.
    let local_deadline = Instant::now() + Duration::from_secs(20);
    let local_start = Instant::now();
    let mut last_missing: Option<String> = None;
    loop {
        let mut missing = None;
        for node in west_nodes {
            if !has_keyspace(node, &keyspace).await {
                missing = Some(node);
                break;
            }
        }
        match missing {
            None => break,
            Some(node) => {
                if Instant::now() >= local_deadline {
                    panic!(
                        "the DDL never reached node {} in the DC that committed it after {:?}\n=== log ===\n{}",
                        node.host_id,
                        local_start.elapsed(),
                        node.log_tail(40)
                    );
                }
                last_missing = Some(node.host_id.to_string());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    let local_lag = local_start.elapsed();
    assert!(
        local_lag < Duration::from_secs(5),
        "the DDL took {local_lag:?} to become visible in its own DC (last missing: {last_missing:?}) \
         — convergence that slow is a regression, not scheduling noise"
    );

    // And it must arrive in the other DC, delivered by that DC's own group.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut missing = Vec::new();
        for node in east_nodes {
            if !has_keyspace(node, &keyspace).await {
                missing.push(node.host_id.clone());
            }
        }
        if missing.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the keyspace never reached {DC_EAST} (missing on {missing:?}): the cross-DC copy did \
             not arrive\n=== west log (sender) ===\n{}\n=== east log (receiver) ===\n{}",
            west_nodes[0].log_tail(45),
            east_nodes[0].log_tail(45)
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
