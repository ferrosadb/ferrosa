//! Cross-DC delivery, proven on real Raft groups: ONE copy per remote data
//! center, and that copy is then replicated inside the receiving DC by its own
//! group's mesh.
//!
//! The shape under test is the multi-DC consensus shape itself: a cluster of N
//! DCs holds N-1 cross-DC links per DC, regardless of how many nodes each DC
//! runs. Three DCs therefore produce exactly two sends — one to dc2 and one to
//! dc3 — and each receiving DC's group replicates the single copy to all of its
//! own voters, so the far DC reaches its own quorum without the sender ever
//! addressing its members.
//!
//! The clusters here are the real in-process openraft harness (three voters
//! each), not mocks: `last_applied` on every voter is the evidence that the
//! single copy became a quorum inside the receiving DC.

mod common;

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use common::raft_harness::TestCluster;

use ferrosa_cluster::accord::cross_dc_dispatch::{
    dc_registry, CrossDcDispatch, CrossDcSender, DcContact,
};
use ferrosa_cluster::raft::{RaftCommand, RaftOp, RaftResponse};

fn noop_command(tag: &str) -> RaftCommand {
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use std::collections::HashMap as Map;
    let mut opts = Map::new();
    opts.insert("replication_factor".to_string(), "1".to_string());
    RaftCommand {
        op: RaftOp::CreateKeyspace(KeyspaceMetadata {
            name: format!("cross_dc_{tag}_{}", uuid::Uuid::new_v4().simple()),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: opts,
            },
        }),
        schema_version: uuid::Uuid::new_v4(),
    }
}

/// Delivers a copy by proposing it on the target DC's own leader — the same
/// thing the production channel achieves over the wire (the contact forwards
/// into its group), with the transport left out so the test observes replication
/// rather than sockets.
struct RealDcSender<'a> {
    clusters: HashMap<String, &'a TestCluster>,
    sends_per_dc: Mutex<HashMap<String, usize>>,
}

impl<'a> RealDcSender<'a> {
    fn new(clusters: &[(&str, &'a TestCluster)]) -> Self {
        Self {
            clusters: clusters
                .iter()
                .map(|(dc, c)| ((*dc).to_string(), *c))
                .collect(),
            sends_per_dc: Mutex::new(HashMap::new()),
        }
    }

    fn sends_for(&self, dc: &str) -> usize {
        *self.sends_per_dc.lock().unwrap().get(dc).unwrap_or(&0)
    }
}

#[async_trait::async_trait]
impl CrossDcSender<RaftCommand> for RealDcSender<'_> {
    async fn send_to_contact(&self, contact: &DcContact, op: &RaftCommand) -> Result<(), String> {
        *self
            .sends_per_dc
            .lock()
            .unwrap()
            .entry(contact.dc.clone())
            .or_insert(0) += 1;
        let cluster = self
            .clusters
            .get(&contact.dc)
            .ok_or_else(|| format!("no cluster for dc {}", contact.dc))?;
        match cluster.propose_on_leader(op.clone()).await {
            Ok(RaftResponse::Ok) => Ok(()),
            Ok(other) => Err(format!("{} refused the copy: {other:?}", contact.dc)),
            Err(e) => Err(format!("{} did not accept the copy: {e:?}", contact.dc)),
        }
    }
}

/// Applied index of every node in a cluster, in node-id order.
fn applied(cluster: &TestCluster) -> Vec<(u64, u64)> {
    let nodes = cluster.nodes();
    let mut snapshot: Vec<(u64, u64)> = nodes
        .iter()
        .map(|node| {
            let metrics = node.raft.metrics().borrow().clone();
            (
                node.node_id,
                metrics.last_applied.map(|l| l.index).unwrap_or(0),
            )
        })
        .collect();
    snapshot.sort_unstable();
    snapshot
}

/// Poll until every node in `cluster` has applied at least `index`.
async fn wait_for_applied(cluster: &TestCluster, index: u64, dc: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if applied(cluster)
            .iter()
            .all(|(_, applied)| *applied >= index)
        {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "{dc} did not replicate the single copy to all of its voters: {:?}",
                applied(cluster)
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Every node of `cluster` as a peer, with host ids namespaced per DC so three
/// clusters (whose harness node ids all start at 1) do not collide.
fn peers_of(cluster: &TestCluster, dc_index: u128) -> Vec<(uuid::Uuid, std::net::SocketAddr)> {
    cluster
        .nodes()
        .iter()
        .map(|node| {
            let host_id = uuid::Uuid::from_u128((dc_index << 32) | node.node_id as u128);
            (host_id, "127.0.0.1:7000".parse().unwrap())
        })
        .collect::<Vec<_>>()
}

/// Three DCs → exactly two copies (one per remote DC), and each receiving DC's
/// OWN group replicates that single copy to all three of its voters. The local
/// DC receives nothing from the dispatcher: it committed locally already.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_copy_per_remote_dc_reaches_that_dcs_own_quorum() {
    let dc1 = TestCluster::with_voters(3).await;
    let dc2 = TestCluster::with_voters(3).await;
    let dc3 = TestCluster::with_voters(3).await;
    dc1.require_leader(Duration::from_secs(10)).await;
    dc2.require_leader(Duration::from_secs(10)).await;
    dc3.require_leader(Duration::from_secs(10)).await;

    // Every node in the cluster, labelled with the DC it announces.
    let mut peers = Vec::new();
    let mut peer_dcs = HashMap::new();
    for (index, dc, cluster) in [(1u128, "dc1", &dc1), (2, "dc2", &dc2), (3, "dc3", &dc3)] {
        for (host_id, addr) in peers_of(cluster, index) {
            peer_dcs.insert(host_id, dc.to_string());
            peers.push((host_id, addr));
        }
    }

    let registry = dc_registry(&peers, &peer_dcs, "dc1");
    assert_eq!(
        registry.keys().cloned().collect::<Vec<_>>(),
        vec!["dc2".to_string(), "dc3".to_string()],
        "a 3-DC cluster holds one link per other DC, not one per remote node"
    );
    assert!(
        registry.values().all(|link| !link.candidates.is_empty()),
        "every remote DC is reachable through at least one known node"
    );

    let dc1_before = applied(&dc1);
    let dc2_before = applied(&dc2);
    let dc3_before = applied(&dc3);

    let sender = RealDcSender::new(&[("dc2", &dc2), ("dc3", &dc3)]);
    let report = CrossDcDispatch::default()
        .fan_out_registry(&sender, &noop_command("fanout"), &registry)
        .await;

    assert!(report.is_complete(&registry), "report: {report:?}");
    assert_eq!(
        sender.sends_for("dc2"),
        1,
        "dc2 is addressed once, however many nodes it runs"
    );
    assert_eq!(sender.sends_for("dc3"), 1);
    assert_eq!(report.retries, 0);

    // The receiving DCs' own meshes did the intra-DC work: every voter applied
    // the one copy that arrived.
    let (_, dc2_target) = *dc2_before.iter().max_by_key(|(id, _)| *id).unwrap();
    let (_, dc3_target) = *dc3_before.iter().max_by_key(|(id, _)| *id).unwrap();
    wait_for_applied(&dc2, dc2_target + 1, "dc2").await;
    wait_for_applied(&dc3, dc3_target + 1, "dc3").await;

    assert_eq!(
        applied(&dc1),
        dc1_before,
        "the local DC committed locally; the dispatcher must not send it a copy"
    );

    dc1.shutdown().await;
    dc2.shutdown().await;
    dc3.shutdown().await;
}

/// A two-DC cluster holds one link in each direction: dc1's contact set is dc2,
/// and dc2's is dc1 — the ring property at the smallest N.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_dcs_hold_one_link_each_way() {
    let dc1 = TestCluster::with_voters(3).await;
    let dc2 = TestCluster::with_voters(2).await;
    dc1.require_leader(Duration::from_secs(10)).await;
    dc2.require_leader(Duration::from_secs(10)).await;

    let mut peers = Vec::new();
    let mut peer_dcs = HashMap::new();
    for (index, dc, cluster) in [(1u128, "dc1", &dc1), (2, "dc2", &dc2)] {
        for (host_id, addr) in peers_of(cluster, index) {
            peer_dcs.insert(host_id, dc.to_string());
            peers.push((host_id, addr));
        }
    }

    let from_dc1 = dc_registry(&peers, &peer_dcs, "dc1");
    let from_dc2 = dc_registry(&peers, &peer_dcs, "dc2");

    assert_eq!(from_dc1.len(), 1);
    assert_eq!(
        from_dc1["dc2"].candidates[0].host_id,
        uuid::Uuid::from_u128((2u128 << 32) | 1),
        "the first candidate tried in dc2 is its lowest host id"
    );
    assert_eq!(from_dc2.len(), 1);
    assert!(
        from_dc2.contains_key("dc1"),
        "the link is bidirectional: each DC holds one to the other"
    );

    // A copy dispatched from dc2 lands in dc1's group and its mesh replicates it.
    let dc1_before = applied(&dc1);
    let sender = RealDcSender::new(&[("dc1", &dc1)]);
    let report = CrossDcDispatch::default()
        .fan_out_registry(&sender, &noop_command("reverse"), &from_dc2)
        .await;
    assert!(report.is_complete(&from_dc2));
    let (_, target) = *dc1_before.iter().max_by_key(|(id, _)| *id).unwrap();
    wait_for_applied(&dc1, target + 1, "dc1").await;

    dc1.shutdown().await;
    dc2.shutdown().await;
}
