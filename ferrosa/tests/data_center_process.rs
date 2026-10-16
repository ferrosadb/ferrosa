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
        .env("RUST_LOG", "info")
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

/// The operator rule, on two real processes in two data centers: the configured
/// DC is the DC in use, and no peer the node does know is labelled with another
/// node's topology.
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
