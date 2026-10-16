//! A committed DDL relayed across data centers, on real Raft groups: ONE copy
//! per remote DC, and that copy becomes the receiving DC's own quorum.
//!
//! This is the DDL half of the cross-DC shape — the same one-copy-per-DC rule the
//! Accord/Raft command path uses, exercised through the command a DDL actually
//! commits (`RaftOp::CreateKeyspace`). The receiving cluster is a real
//! three-voter openraft group, so its `keyspaces` map on every voter is evidence
//! that the single relayed copy reached that DC's quorum without the sender ever
//! addressing its members one by one.

mod common;

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use common::raft_harness::TestCluster;

use ferrosa_cluster::accord::cross_dc_dispatch::{
    cross_dc_pending_repairs_total, dc_registry, CrossDcDispatch, CrossDcOutcome,
    CrossDcRetryPolicy, CrossDcSender, DcContact,
};
use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::raft::{RaftCommand, RaftOp, RaftResponse};

const DC_WEST: &str = "us-west-2";
const DC_EAST: &str = "eu-west-1";

fn keyspace_ddl(name: &str) -> RaftCommand {
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use std::collections::HashMap as Map;
    let mut options = Map::new();
    options.insert("replication_factor".to_string(), "3".to_string());
    RaftCommand {
        op: RaftOp::CreateKeyspace(KeyspaceMetadata {
            name: name.to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options,
            },
        }),
        schema_version: uuid::Uuid::new_v4(),
    }
}

/// The registry a node in `dc_west` builds for `dc_east`'s voters.
fn east_registry(
    east: &TestCluster,
) -> std::collections::BTreeMap<String, ferrosa_cluster::accord::cross_dc_dispatch::DcLink> {
    let peers: Vec<(uuid::Uuid, std::net::SocketAddr)> = east
        .nodes()
        .iter()
        .map(|node| {
            (
                uuid::Uuid::from_u128(node.node_id as u128),
                "127.0.0.1:7000".parse().unwrap(),
            )
        })
        .collect();
    let peer_dcs: HashMap<uuid::Uuid, String> = peers
        .iter()
        .map(|(host_id, _)| (*host_id, DC_EAST.to_string()))
        .collect();
    dc_registry(&peers, &peer_dcs, DC_WEST)
}

/// Stands in for the production channel: whichever node of the receiving DC is
/// contacted hands the DDL to that DC's leader — the `ClusterDdlForwardHandler`
/// side of the relay — so the copy enters that DC's own group.
struct DcForwardSender<'a> {
    cluster: &'a TestCluster,
    sends_per_dc: Mutex<HashMap<String, usize>>,
}

impl<'a> DcForwardSender<'a> {
    fn new(cluster: &'a TestCluster) -> Self {
        Self {
            cluster,
            sends_per_dc: Mutex::new(HashMap::new()),
        }
    }

    fn sends_for(&self, dc: &str) -> usize {
        *self.sends_per_dc.lock().unwrap().get(dc).unwrap_or(&0)
    }
}

#[async_trait::async_trait]
impl CrossDcSender<RaftCommand> for DcForwardSender<'_> {
    async fn send_to_contact(&self, contact: &DcContact, op: &RaftCommand) -> Result<(), String> {
        *self
            .sends_per_dc
            .lock()
            .unwrap()
            .entry(contact.dc.clone())
            .or_insert(0) += 1;
        // Whichever node of the receiving DC was contacted, the DDL enters that
        // DC's own group through its leader — what the production handler does.
        self.cluster.require_leader(Duration::from_secs(10)).await;
        match self.cluster.propose_on_leader(op.clone()).await {
            Ok(RaftResponse::Ok) => Ok(()),
            Ok(other) => Err(format!("{} refused the DDL: {other:?}", contact.dc)),
            Err(e) => Err(format!("{} did not commit the DDL: {e:?}", contact.dc)),
        }
    }
}

/// A sender that cannot reach the DC at all.
struct UnreachableSender;

#[async_trait::async_trait]
impl CrossDcSender<RaftCommand> for UnreachableSender {
    async fn send_to_contact(&self, contact: &DcContact, _op: &RaftCommand) -> Result<(), String> {
        Err(format!("{} is unreachable", contact.dc))
    }
}

/// Wait until every voter of `cluster` holds `keyspace` in its applied state.
///
/// A commit is awaited on the LEADER; followers apply asynchronously, so an
/// immediate snapshot would be racy rather than wrong — the bounded wait is what
/// makes "the DC has it" a claim about the DC, not about one node.
async fn wait_for_keyspace(cluster: &TestCluster, keyspace: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut missing = Vec::new();
        for node in cluster.nodes().iter() {
            if !node.state_snapshot().await.keyspaces.contains_key(keyspace) {
                missing.push(node.node_id);
            }
        }
        if missing.is_empty() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "keyspace {keyspace} never reached every voter of the cluster; still missing on {missing:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_ddl_copy_reaches_the_remote_dcs_own_quorum() {
    let west = TestCluster::with_voters(3).await;
    let east = TestCluster::with_voters(3).await;
    west.require_leader(Duration::from_secs(15)).await;
    east.require_leader(Duration::from_secs(15)).await;

    let keyspace = format!("relayed_{}", uuid::Uuid::new_v4().simple());
    let ddl = keyspace_ddl(&keyspace);

    // The DDL commits in its own DC first — that is the client's write.
    let committed = west.propose_on_leader(ddl.clone()).await;
    assert!(
        matches!(committed, Ok(RaftResponse::Ok)),
        "the local DDL commit is the precondition for relaying: {committed:?}"
    );
    wait_for_keyspace(&west, &keyspace).await;

    let registry = east_registry(&east);
    assert_eq!(registry.len(), 1, "one link: dc2 is the only other DC");
    assert_eq!(
        registry[DC_EAST].candidates.len(),
        3,
        "the link holds all three of dc2's nodes"
    );

    let sender = DcForwardSender::new(&east);
    let report = CrossDcDispatch::default()
        .fan_out_registry(&sender, &ddl, &registry)
        .await;

    assert_eq!(
        sender.sends_for(DC_EAST),
        1,
        "three remote nodes, ONE copy: the WAN carries one DDL per DC"
    );
    assert_eq!(report.delivered, vec![DC_EAST.to_string()]);
    assert!(report.failed.is_empty(), "report: {report:?}");

    // The single copy became dc2's own quorum: every voter applies it, and the
    // sender never addressed its members one by one.
    wait_for_keyspace(&east, &keyspace).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ddl_copy_owed_to_an_unreachable_dc_is_counted_not_dropped() {
    let east = TestCluster::with_voters(3).await;
    east.require_leader(Duration::from_secs(15)).await;

    let registry = east_registry(&east);
    let ddl = keyspace_ddl(&format!("owed_{}", uuid::Uuid::new_v4().simple()));
    let dispatch = CrossDcDispatch::with_policy(CrossDcRetryPolicy {
        max_attempts: 2,
        backoff: Duration::from_millis(1),
    });
    let before = cross_dc_pending_repairs_total();

    // The local-DC levels are satisfied here; the far DC's copy is owed.
    let outcome = dispatch
        .deliver_for_cl(
            ConsistencyLevel::LocalQuorum,
            &UnreachableSender,
            &ddl,
            &registry,
        )
        .await;
    match outcome {
        CrossDcOutcome::PendingLocally { owed, .. } => {
            assert_eq!(owed, vec![DC_EAST.to_string()]);
        }
        other => panic!("LOCAL_QUORUM must degrade to a counted debt, got {other:?}"),
    }
    assert_eq!(
        cross_dc_pending_repairs_total(),
        before + 1,
        "the owed DDL is counted, so a repair path has something to drain"
    );

    // A level that requires every DC must NOT report success.
    let strict = dispatch
        .deliver_for_cl(
            ConsistencyLevel::EachQuorum,
            &UnreachableSender,
            &ddl,
            &registry,
        )
        .await;
    match strict {
        CrossDcOutcome::NotSatisfied { missing, .. } => {
            assert_eq!(missing, vec![DC_EAST.to_string()]);
        }
        other => panic!("EACH_QUORUM with dc2 unreachable must be NotSatisfied, got {other:?}"),
    }
}
