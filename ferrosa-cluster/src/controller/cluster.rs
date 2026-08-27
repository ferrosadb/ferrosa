//! Cluster mode transition logic: forming and full cluster.
//!
//! Responsibility: construct the Raft/ring/write-path stack and publish it in
//! formation order while preserving queued schema mutations.
//! Correctness: formation work is bounded, cancel-aware, and never advertises
//! durable topology before the relevant consensus state exists; Raft fatal
//! metrics are supervised by the shared fail-closed health gate.
//! Last revised: 2026-08-27
//! Last changed: Combined bounded formation/DDL replay with fatal Raft metrics
//! supervision.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

const MAX_CLUSTER_MEMBER_MARKER_BYTES: usize = 4096;

struct SavedClusterMemberMarker {
    bytes: [u8; MAX_CLUSTER_MEMBER_MARKER_BYTES],
    len: usize,
}

fn read_cluster_member_marker(
    marker: &std::path::Path,
) -> std::io::Result<Option<SavedClusterMemberMarker>> {
    let mut file = match std::fs::File::open(marker) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let declared_len = file.metadata()?.len();
    if declared_len > MAX_CLUSTER_MEMBER_MARKER_BYTES as u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "cluster membership marker is {declared_len} bytes; maximum is {MAX_CLUSTER_MEMBER_MARKER_BYTES}"
            ),
        ));
    }

    let len = declared_len as usize;
    let mut bytes = [0u8; MAX_CLUSTER_MEMBER_MARKER_BYTES];
    file.read_exact(&mut bytes[..len])?;
    let mut extra = [0u8; 1];
    if file.read(&mut extra)? != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "cluster membership marker grew beyond its validated bound",
        ));
    }
    Ok(Some(SavedClusterMemberMarker { bytes, len }))
}

fn restore_cluster_member_marker_atomic(
    raft_dir: &std::path::Path,
    saved: &SavedClusterMemberMarker,
) -> std::io::Result<()> {
    let live = raft_dir.join(DeploymentMode::CLUSTER_MEMBER_MARKER);
    let staging = raft_dir.join(".cluster-member.staging");
    let result = (|| {
        let mut file = std::fs::File::create(&staging)?;
        file.write_all(&saved.bytes[..saved.len])?;
        file.flush()?;
        file.sync_all()?;
        if file.metadata()?.len() != saved.len as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "staged cluster membership marker length mismatch",
            ));
        }
        std::fs::rename(&staging, &live)?;
        std::fs::File::open(raft_dir)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&staging);
    }
    result
}

/// Process-wide counters for cluster-bootstrap silent-failure detectors.
/// These fire when previously-swallowed paths now surface errors loudly:
///
/// - `RAFT_PUBLISH_NO_SUBSCRIBERS` — the LazyRaft watch had no live
///   subscribers when the spawned bootstrap task tried to publish the new
///   Raft instance. A non-zero value means a node booted Raft but no
///   handler could observe it; the cluster will appear healthy but reads
///   and writes through that node will hang.
/// - `RAFT_INITIALIZE_FAILURES` — `raft.initialize(members)` returned
///   Err. Some Errs are benign ("already initialized") and are kept
///   suppressed; this counter only tracks the unexpected variants.
/// - `LEADER_ELECTION_TIMEOUTS` — the seed waited ~30s for a leader and
///   gave up, falling back to Pair mode. A non-zero value here is
///   normally a bug (network partition, peer crash during formation),
///   not steady-state behavior.
pub(crate) static RAFT_PUBLISH_NO_SUBSCRIBERS: AtomicU64 = AtomicU64::new(0);
pub(crate) static RAFT_INITIALIZE_FAILURES: AtomicU64 = AtomicU64::new(0);
pub(crate) static LEADER_ELECTION_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
/// Number of times cluster formation started with fewer nodes than the
/// configured replication factor. Non-zero means writes during this
/// formation window had reduced durability — run repair to recover.
pub static FORMATION_REDUCED_DURABILITY_WRITES: AtomicU64 = AtomicU64::new(0);

/// Snapshot of the silent-failure counters. Public so the metrics endpoint
/// and tests can observe whether the cluster bootstrap has produced any
/// signals that would previously have been hidden.
pub fn bootstrap_silent_failure_counts() -> (u64, u64, u64) {
    (
        RAFT_PUBLISH_NO_SUBSCRIBERS.load(AtomicOrdering::Relaxed),
        RAFT_INITIALIZE_FAILURES.load(AtomicOrdering::Relaxed),
        LEADER_ELECTION_TIMEOUTS.load(AtomicOrdering::Relaxed),
    )
}

use arc_swap::ArcSwap;
use ferrosa_net::codec::{Lane, MsgType};
use ferrosa_net::config::NetConfig;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::pool::PriorityPool;
use ferrosa_net::rpc::handler::PeerId;
use ferrosa_net::rpc::RpcHandler;
use uuid::Uuid;

use crate::consistency::ConsistencyLevel;
use crate::coordinator::{
    batch::{BatchlogDeleteHandler, BatchlogReplayHandler, BatchlogWriteHandler},
    ClusterCoordinator, RepairWriteHandler, TruncateForwardHandler,
};
use crate::ddl_path::{execute_via_raft, ClusterDdlForwardHandler, DdlPath};
use crate::mode::DeploymentMode;
use crate::pair::ddl::DdlOperation;
use crate::raft::handlers::{
    FulltextSearchHandler, IndexReadHandler, IndexReadInPartitionHandler, RaftAppendHandler,
    RaftSnapshotHandler, RaftVoteHandler, RangeReadHandler, ReadRequestHandler,
};
use crate::raft::log_store::SledLogStore;
use crate::raft::network::FerrosRaftNetworkFactory;
use crate::raft::state_machine::FerrosStateMachine;
use crate::raft::{uuid_to_node_id, FerrosRaft, NodeInfo, NodeState};
use crate::ring::TokenRing;
use crate::state::RaftClusterState;
use crate::streaming::{
    sender::{SstableSendRequest, StreamSender},
    StreamConfig, StreamedMutation,
};
use crate::write_path::WritePath;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum InvitePeerConnectionPlan {
    SkipSelf,
    KeepLiveKnownPeer {
        known_addr: String,
        invite_addr: SocketAddr,
    },
    AlreadyConnected,
    Connect {
        reverse_addr: SocketAddr,
        previous_addr: Option<String>,
    },
}

pub(super) fn plan_invite_peer_connection(
    local_host_id: Uuid,
    peer_id: Uuid,
    peer_addr: SocketAddr,
    internode_port: u16,
    known_addr: Option<&str>,
    live: bool,
) -> InvitePeerConnectionPlan {
    if peer_id == local_host_id {
        return InvitePeerConnectionPlan::SkipSelf;
    }

    let reverse_addr = SocketAddr::new(peer_addr.ip(), internode_port);
    let invite_addr = reverse_addr.to_string();

    if live {
        return match known_addr {
            Some(known) if known == invite_addr => InvitePeerConnectionPlan::AlreadyConnected,
            Some(known) => InvitePeerConnectionPlan::KeepLiveKnownPeer {
                known_addr: known.to_string(),
                invite_addr: reverse_addr,
            },
            None => InvitePeerConnectionPlan::AlreadyConnected,
        };
    }

    InvitePeerConnectionPlan::Connect {
        reverse_addr,
        previous_addr: known_addr.map(ToOwned::to_owned),
    }
}

use super::{ClusterStateHolder, ModeController};

pub(super) fn should_initialize_seed_membership(
    was_seed: bool,
    has_recovered_membership: bool,
    has_recovered_topology_state: bool,
) -> bool {
    was_seed && !has_recovered_membership && !has_recovered_topology_state
}

pub(super) fn should_run_bootstrap_streaming(has_recovered_topology_state: bool) -> bool {
    !has_recovered_topology_state
}

/// Keyspaces that should be re-proposed through Raft during the
/// post-Raft-init schema-convergence pass (`transition_to_cluster`
/// `ReplaySchema` phase).
///
/// The built-in Cassandra system keyspaces (`system`, `system_schema`,
/// `system_auth`, etc.) are hardcoded on every node and never need
/// replay. **The graph engine's `system_graph_<user_ks>` keyspaces are
/// NOT built-in** — they are constructed lazily on the first graph
/// query, often while the local node's `DdlPath` is still `Direct` or
/// `Forming`. If that happens, the adjacency keyspace + table get
/// registered locally only; followers never learn about them and
/// reject every adjacency `MutationForward` with
/// "table not registered: system_graph_<ks>.adjacency".
///
/// Including `system_graph_*` here lets the leader re-fire those
/// `CreateKeyspace` + `CreateTable` ops through Raft on Cluster-mode
/// transition, which propagates them to every replica's state
/// machine and storage engine.
pub(super) fn keyspace_needs_cluster_replay(name: &str) -> bool {
    !ferrosa_schema::is_system_keyspace(name)
}

/// Resolve the formation replication factor from the current schema.
///
/// Reads all user keyspaces (non-system) from `schema` and returns the
/// maximum RF across them.  For `NetworkTopologyStrategy`, uses the
/// per-DC factor for `local_dc` if present; otherwise sums all DC values.
/// Returns `default_rf` (typically 1 or 3) when there are no user keyspaces.
///
/// This must NOT hardcode 1 — the caller is responsible for emitting a
/// WARN when the cluster does not yet have enough nodes to satisfy this RF.
pub(super) fn resolve_formation_rf(
    schema: &ferrosa_schema::Schema,
    local_dc: &str,
    default_rf: usize,
) -> usize {
    let snapshot = schema.snapshot();
    let mut max_rf = default_rf;

    for (name, ks) in &snapshot.keyspaces {
        if ferrosa_schema::is_system_keyspace(name) {
            continue;
        }
        let rf = match ks.replication.strategy.as_str() {
            "SimpleStrategy" => ks
                .replication
                .options
                .get("replication_factor")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1),
            "NetworkTopologyStrategy" => {
                if let Some(dc_rf) = ks.replication.options.get(local_dc) {
                    dc_rf.parse::<usize>().unwrap_or(1)
                } else {
                    // Sum all DCs, skipping the "class" meta-key.
                    ks.replication
                        .options
                        .iter()
                        .filter(|(k, _)| k.as_str() != "class")
                        .filter_map(|(_, v)| v.parse::<usize>().ok())
                        .sum()
                }
            }
            _ => 1,
        };
        if rf > max_rf {
            max_rf = rf;
        }
    }

    max_rf
}

/// Drain a DDL queue (W1.14, P0-1 hazard).
///
/// On the leader-elected path of `transition_to_cluster` we replay
/// every operation queued during the Forming state.  Naive `try_recv`
/// loops drop ops sent *during* the drain (a sender that loaded the
/// `DdlPath::Forming` Arc just before the swap to `Cluster` and
/// hadn't yet completed the send).  This helper waits until the
/// channel has been observed empty `REQUIRED_CONSECUTIVE_EMPTY` times
/// in a row, with a `COOL_DOWN` delay between probes, capped by a
/// hard wall-clock deadline.  This gives in-flight `Forming` senders
/// up to `COOL_DOWN * REQUIRED_CONSECUTIVE_EMPTY` (= 150 ms) to land.
///
/// Generic over the op-processor `F` so unit tests can substitute a
/// counter without spinning up a Raft.
pub(super) async fn drain_ddl_queue<Op, F, Fut>(
    mut rx: tokio::sync::mpsc::Receiver<Op>,
    mut process: F,
) -> usize
where
    F: FnMut(Op) -> Fut,
    Fut: std::future::Future<Output = crate::error::Result<()>>,
{
    const COOL_DOWN: std::time::Duration = std::time::Duration::from_millis(50);
    const REQUIRED_CONSECUTIVE_EMPTY: usize = 3;
    const HARD_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

    let mut replayed = 0usize;
    let drain_deadline = tokio::time::Instant::now() + HARD_DEADLINE;
    let mut consecutive_empty = 0usize;

    while consecutive_empty < REQUIRED_CONSECUTIVE_EMPTY
        && tokio::time::Instant::now() < drain_deadline
    {
        match rx.try_recv() {
            Ok(op) => {
                consecutive_empty = 0;
                if let Err(e) = process(op).await {
                    tracing::warn!(%e, "drain_ddl_queue: process failed");
                } else {
                    replayed += 1;
                }
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                consecutive_empty += 1;
                tokio::time::sleep(COOL_DOWN).await;
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
    replayed
}

/// W6.3: per-DC log directory layout.
///
/// Each DC's Raft log lives in `raft_data_dir/<dc-name>/`, isolated
/// from any other DC running on the same physical disk. Single-DC
/// deployments transparently land under
/// `raft_data_dir/<DEFAULT_DC_NAME>/` — readers tolerant to legacy
/// `raft_data_dir/` layouts must migrate at upgrade (ADR-015 R3).
/// Where this node's Raft state lives, from config alone.
///
/// Extracted so startup and runtime cannot disagree about the path. The
/// cluster-membership marker is written by `transition_to_cluster` and read
/// before any peer connects; if those two computed the directory differently
/// the marker would be written where nobody looks, and the node would forget
/// it was a member -- the exact bug this is fixing, reintroduced by a
/// duplicated expression.
impl ModeController {
    /// Move a stranded Raft directory aside and start a clean one, keeping the
    /// `cluster-member` marker.
    ///
    /// The marker has to survive. It is what tells the restarted node it was a
    /// cluster member, so it comes back as `DegradedCluster` -- refusing
    /// queries until it has real data -- instead of forming a fresh pair on
    /// top of a cluster it is still a committed member of. Losing it here
    /// would turn a recoverable strand into the split brain the deployment
    /// mode work exists to prevent.
    ///
    /// The old directory is retained rather than deleted: it is the only
    /// evidence of how the node got stranded.
    pub(crate) fn reset_stranded_raft_state(
        raft_dir: &std::path::Path,
    ) -> std::io::Result<Option<std::path::PathBuf>> {
        let marker = raft_dir.join(DeploymentMode::CLUSTER_MEMBER_MARKER);
        let saved_marker = read_cluster_member_marker(&marker)?;

        let counts = crate::raft::log_store::SledLogStore::reset(raft_dir)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        if let Some(saved) = saved_marker {
            restore_cluster_member_marker_atomic(raft_dir, &saved)?;
        }

        Ok(counts.backup_path)
    }
}

pub fn resolve_raft_dir(config: &crate::config::ClusterConfig) -> std::path::PathBuf {
    configured_raft_dir(config)
        .unwrap_or_else(|| raft_log_dir_for_dc(&default_raft_base(), &config.data_center))
}

/// Where this config *says* its Raft state lives, if it says at all.
///
/// `None` means neither `raft_data_dir` nor `FERROSA_DATA_DIR` was set and the
/// caller would be falling back to the compiled-in `/var/lib/ferrosa`.
///
/// The distinction matters for decisions that read the filesystem to work out
/// what this node *is*. `ModeController::new` consults the `cluster-member`
/// marker to decide whether to come up as a returning cluster member, and with
/// a default-constructed config that lookup lands on a machine-global path --
/// so a controller's initial mode, and therefore whether it will serve
/// queries, would depend on ambient host state rather than on its
/// configuration. Unit tests build controllers with `ClusterConfig::default()`
/// all the time; on a host with `FERROSA_DATA_DIR` exported, or where
/// `/var/lib` is unreadable (`was_cluster_member` deliberately assumes
/// membership on an unreadable marker), they would silently start in a
/// different mode than the one they assert.
pub fn configured_raft_dir(config: &crate::config::ClusterConfig) -> Option<std::path::PathBuf> {
    let local_dc = config.data_center.clone();
    let base = match config.raft_data_dir_for_dc(&local_dc) {
        Some(dir) => dir,
        None => std::path::Path::new(&std::env::var("FERROSA_DATA_DIR").ok()?).join("raft"),
    };
    Some(raft_log_dir_for_dc(&base, &local_dc))
}

fn default_raft_base() -> std::path::PathBuf {
    std::path::Path::new(
        &std::env::var("FERROSA_DATA_DIR").unwrap_or_else(|_| "/var/lib/ferrosa".into()),
    )
    .join("raft")
}

pub fn raft_log_dir_for_dc(base: &std::path::Path, dc_name: &str) -> std::path::PathBuf {
    base.join(dc_name)
}

/// W6.3: partition a `(host_id, addr)` peer list by DC, given a lookup
/// table from `host_id` to DC name. Peers not present in `dcs` default
/// to `local_dc` — this preserves single-DC behavior when no per-peer
/// DC metadata has been published.
///
/// Returns `(local_dc_peers, other_dcs)` where `other_dcs` groups
/// non-local peers by DC name. Local-DC voters drive Raft membership;
/// non-local-DC peers participate in cross-DC routing only (Sprint 7
/// will plumb them into Accord).
/// Result of [`partition_peers_by_dc`] — groups peers by DC.
pub type PeerDcPartition = (
    Vec<(uuid::Uuid, SocketAddr)>,
    std::collections::BTreeMap<String, Vec<(uuid::Uuid, SocketAddr)>>,
);

pub fn partition_peers_by_dc(
    peers: &[(uuid::Uuid, SocketAddr)],
    dcs: &std::collections::HashMap<uuid::Uuid, String>,
    local_dc: &str,
) -> PeerDcPartition {
    let mut local = Vec::new();
    let mut others: std::collections::BTreeMap<String, Vec<(uuid::Uuid, SocketAddr)>> =
        std::collections::BTreeMap::new();
    for (uuid, addr) in peers {
        let dc = dcs.get(uuid).map(String::as_str).unwrap_or(local_dc);
        if dc == local_dc {
            local.push((*uuid, *addr));
        } else {
            others
                .entry(dc.to_string())
                .or_default()
                .push((*uuid, *addr));
        }
    }
    (local, others)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_recovered_topology_refresh_plan(
    local_host_id: uuid::Uuid,
    local_addr: String,
    local_cql_broadcast: Option<String>,
    data_center: &str,
    rack: &str,
    peers: &[(uuid::Uuid, SocketAddr)],
    peer_cql_broadcasts: &std::collections::HashMap<uuid::Uuid, Option<String>>,
    peer_internode_broadcasts: &std::collections::HashMap<uuid::Uuid, Option<String>>,
) -> Vec<crate::raft::NodeInfo> {
    let mut plan = Vec::with_capacity(peers.len() + 1);
    plan.push(crate::raft::NodeInfo {
        host_id: local_host_id,
        addr: local_addr,
        data_center: data_center.to_string(),
        rack: rack.to_string(),
        state: crate::raft::NodeState::Normal,
        cql_broadcast: local_cql_broadcast,
    });
    for (peer_uuid, addr) in peers {
        // Prefer the re-resolvable advertised internode hostname; fall back to
        // the observed connection IP only when no hostname was advertised.
        let peer_internode_broadcast = peer_internode_broadcasts.get(peer_uuid).cloned().flatten();
        let node_addr =
            super::membership::node_info_addr(*addr, peer_internode_broadcast.as_deref());
        plan.push(crate::raft::NodeInfo {
            host_id: *peer_uuid,
            addr: node_addr,
            data_center: data_center.to_string(),
            rack: rack.to_string(),
            state: crate::raft::NodeState::Normal,
            cql_broadcast: peer_cql_broadcasts.get(peer_uuid).cloned().flatten(),
        });
    }
    plan
}

pub(super) fn build_recovered_topology_token_repair_plan(
    refresh_plan: &[crate::raft::NodeInfo],
    num_tokens: usize,
) -> Vec<(u64, Vec<crate::raft::Token>)> {
    refresh_plan
        .iter()
        .map(|node_info| {
            let node_id = uuid_to_node_id(node_info.host_id);
            (
                node_id,
                crate::controller::token::deterministic_tokens_for_node(node_id, num_tokens),
            )
        })
        .collect()
}

pub(super) fn build_initial_raft_members(
    local_host_id: uuid::Uuid,
    local_addr: SocketAddr,
    peers: &[(uuid::Uuid, SocketAddr)],
) -> std::collections::BTreeMap<u64, openraft::BasicNode> {
    let local_node_id = uuid_to_node_id(local_host_id);
    let mut members = std::collections::BTreeMap::new();
    members.insert(
        local_node_id,
        openraft::BasicNode {
            addr: local_addr.to_string(),
        },
    );
    for (peer_uuid, addr) in peers {
        let peer_node_id = uuid_to_node_id(*peer_uuid);
        members.insert(
            peer_node_id,
            openraft::BasicNode {
                addr: addr.to_string(),
            },
        );
    }
    members
}

pub(super) fn build_raft_members_from_node_info(
    nodes: &[crate::raft::NodeInfo],
) -> std::collections::BTreeMap<u64, openraft::BasicNode> {
    nodes
        .iter()
        .map(|node| {
            (
                uuid_to_node_id(node.host_id),
                openraft::BasicNode {
                    addr: node.addr.clone(),
                },
            )
        })
        .collect()
}

pub(super) fn membership_addresses_need_repair(
    current: &std::collections::BTreeMap<u64, openraft::BasicNode>,
    desired: &std::collections::BTreeMap<u64, openraft::BasicNode>,
) -> bool {
    desired.iter().any(|(node_id, desired_node)| {
        !desired_node.addr.is_empty()
            && current
                .get(node_id)
                .is_none_or(|current_node| current_node.addr != desired_node.addr)
    })
}

impl ModeController {
    fn normalize_cluster_peer_addr(&self, addr: SocketAddr) -> SocketAddr {
        SocketAddr::new(addr.ip(), self.net_config.bind_addr.port())
    }

    /// Send a `ClusterInvite` to a single peer with the current cluster's
    /// known peer list (everyone except the recipient). Used when a peer
    /// connects (or reconnects) after this node has already finished its
    /// pair → forming → cluster transition: the original one-shot invite
    /// from `transition_to_cluster` is long gone, but the new peer still
    /// needs to learn about the cluster so it can transition out of pair
    /// mode and register its Raft handlers.
    ///
    /// Without this, recreating any single node post-cluster-formation
    /// silently breaks Raft quorum forever — the recreated node stays
    /// in pair mode, the leader's elections fail (no votes), and reads
    /// at LOCAL_QUORUM time out.
    pub(crate) fn send_cluster_invite_to(&self, recipient: Uuid) {
        let pm_guard = self.peer_manager.load();
        let Some(pm) = pm_guard.as_ref().as_ref().cloned() else {
            return;
        };
        let local_host_id = self.local_host_id;
        let connected_peers = self.connected_peers.lock().clone();
        let Some(plan) =
            super::invite::plan_reconnect_invite(super::invite::ReconnectInvitePlanInput {
                local_host_id,
                local_addr: Some(self.net_config.broadcast_addr),
                recipient,
                connected_peers: &connected_peers,
            })
        else {
            return;
        };
        let reserved_at = std::time::Instant::now();
        {
            let mut recent = self.recent_reconnect_invites.lock();
            if !super::invite::reserve_reconnect_invite(
                &mut recent,
                recipient,
                reserved_at,
                super::CLUSTER_RECONNECT_INVITE_COOLDOWN,
                super::MAX_CONNECTED_PEERS,
            ) {
                tracing::debug!(peer = %recipient, "ClusterInvite delivery suppressed by reconnect cooldown");
                return;
            }
        }
        let invite = Message::ClusterInvite {
            initiator: plan.initiator,
            peers: plan.peers,
        };
        let pm_clone = pm.clone();
        let recent_invites = self.recent_reconnect_invites.clone();
        self.spawn_tracked(async move {
            let mut last_error = None;
            for attempt in 0..10 {
                match pm_clone.send(recipient, invite.clone(), Lane::Data).await {
                    Ok(_) => {
                        tracing::info!(
                            peer = %recipient,
                            "ClusterInvite delivered (cluster-mode reconnect)"
                        );
                        return;
                    }
                    Err(e) => {
                        if attempt < 9 {
                            tracing::debug!(
                                peer = %recipient, attempt, %e,
                                "ClusterInvite delivery retry (cluster-mode reconnect)"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                        last_error = Some(e.to_string());
                    }
                }
            }

            // Report the reason, and give the cooldown back.
            //
            // This branch used to log a WARN without the error -- the retries
            // carried `%e` at debug, which is off by default, so operators
            // learned that delivery failed and never why. The peer this invite
            // was for cannot join the cluster without it, and the function's
            // own doc comment says as much: failing here "silently breaks Raft
            // quorum forever".
            //
            // Releasing the reservation matters as much as the log line.
            // Nothing re-triggers an invite except a peer-connect event, and
            // the reservation taken above would suppress the next one for the
            // rest of the cooldown -- so a single failed delivery could strand
            // a peer in Pair mode indefinitely, which is what happened to
            // node1 on 2026-08-20.
            tracing::warn!(
                peer = %recipient,
                error = last_error.as_deref().unwrap_or("unknown"),
                "ClusterInvite delivery failed after 10 attempts (cluster-mode \
                 reconnect); this peer cannot join until an invite reaches it. \
                 Releasing the cooldown so the next peer event can retry."
            );
            super::invite::release_reconnect_invite(
                &mut recent_invites.lock(),
                recipient,
                reserved_at,
            );
        });
    }

    /// Transition from Pair to Forming: broadcast ClusterInvite and prepare
    /// for mesh formation. Does NOT initialize Raft — that happens in
    /// `transition_to_cluster` after all peers are connected.
    pub(super) fn transition_to_forming(&self, peers: Vec<(Uuid, SocketAddr)>) {
        if self.peer_manager.load().is_none() {
            tracing::error!("cannot transition to forming: peer_manager not set");
            return;
        }

        self.try_transition_mode(DeploymentMode::Forming);
        // Record committed cluster size for quorum calculations (peers + self).
        self.committed_cluster_size
            .store(peers.len() + 1, std::sync::atomic::Ordering::Relaxed);
        // Queue DDL during formation — operations are replayed after Raft leader
        // election instead of being rejected (FMEA F3).
        let (ddl_tx, ddl_rx) =
            tokio::sync::mpsc::channel(crate::ddl_path::FORMING_DDL_QUEUE_CAPACITY);
        *self.ddl_queue_rx.lock() = Some(ddl_rx);
        self.ddl_path
            .store(Arc::new(DdlPath::Forming { queue: ddl_tx }));
        self.formation_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.seen_invite_initiators.lock().clear();
        tracing::info!(
            peer_count = peers.len(),
            epoch = self
                .formation_epoch
                .load(std::sync::atomic::Ordering::Relaxed),
            "mode transition: pair -> forming (broadcasting ClusterInvite)"
        );

        // ClusterInvite delivery moved into the Raft init task (transition_to_cluster)
        // to ensure invites arrive before elections start.

        // Record when we entered Forming — the timeout check happens in the
        // Raft init background task (transition_to_cluster) and also in
        // on_peer_connected if mode is still Forming.
        // The actual timeout logic is inside transition_to_cluster's leader
        // election poll: if election doesn't complete in 30s AND formation
        // timeout is exceeded, the mode reverts to Pair.

        // Now proceed to cluster transition with all known peers.
        // In the future, this will wait for mesh completion before Raft init.
        // For now, proceed immediately (matches current behavior).
        self.transition_to_cluster(peers);
    }

    /// 4. TokenRing with deterministic initial token assignment
    /// 5. ClusterCoordinator for replica-aware writes
    /// 6. Swaps write path, DDL path, and cluster state atomically
    pub(super) fn transition_to_cluster(&self, peers: Vec<(Uuid, SocketAddr)>) {
        let peers: Vec<(Uuid, SocketAddr)> = peers
            .into_iter()
            .map(|(peer_uuid, addr)| (peer_uuid, self.normalize_cluster_peer_addr(addr)))
            .collect();
        let phase_runner = super::bootstrap::runner::BootstrapPhaseRunner::canonical();
        tracing::debug!(
            phase_count = phase_runner.phase_order().len(),
            "transition_to_cluster: bootstrap phase runner selected"
        );

        // ADR-015: partition the peer set by DC. Only same-DC
        // peers participate in this node's Raft group; cross-DC peers
        // are tracked in the controller's connected_peers map for
        // future Accord routing but are not voters here.
        //
        // Backward-compat: peers with no recorded DC default to the
        // local DC, so existing single-DC clusters keep their full
        // peer list as voters unchanged.
        let local_dc = self.config.data_center.clone();
        let peer_dc_map = self.peer_dcs_snapshot();
        let (peers, cross_dc_peers) = partition_peers_by_dc(&peers, &peer_dc_map, &local_dc);
        if !cross_dc_peers.is_empty() {
            tracing::info!(
                local_dc = %local_dc,
                local_voters = peers.len(),
                cross_dc_count = cross_dc_peers.values().map(|v| v.len()).sum::<usize>(),
                "transition_to_cluster: cross-DC peers excluded from local Raft group; Accord cross-DC routing is deferred"
            );
        }
        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                tracing::error!("cannot transition to cluster: peer_manager not set");
                return;
            }
        };
        let peer_cql_broadcasts: std::collections::HashMap<Uuid, Option<String>> = peers
            .iter()
            .map(|(peer_uuid, _)| {
                (
                    *peer_uuid,
                    peer_manager.get_peer_cql_broadcast_sync(*peer_uuid),
                )
            })
            .collect();
        // Parallel map of advertised internode-broadcast hostnames so the
        // recovered-topology refresh commits re-resolvable hostnames (not frozen
        // IPs) for peers, matching what trigger_cluster_join commits on join.
        let peer_internode_broadcasts: std::collections::HashMap<Uuid, Option<String>> = peers
            .iter()
            .map(|(peer_uuid, _)| {
                (
                    *peer_uuid,
                    peer_manager.get_peer_internode_broadcast_sync(*peer_uuid),
                )
            })
            .collect();

        // Determine whether this node is the seed (responsible for calling
        // raft.initialize()). The seed is the node with the highest UUID among
        // ALL cluster members (self + all peers). We must include self in the
        // comparison to handle the case where the ClusterInvite's peer list
        // does not include the initiator — without this, a node that only sees
        // a subset of peers can incorrectly believe it is the seed (RC-3 race).
        let mut all_member_uuids: Vec<uuid::Uuid> = peers.iter().map(|(id, _)| *id).collect();
        all_member_uuids.push(self.local_host_id);
        let max_uuid = all_member_uuids.iter().max().copied().unwrap_or_default();
        let was_seed = self.local_host_id == max_uuid;

        // Flush all memtables to SSTables before the write path switches to the
        // ClusterCoordinator. Data written in standalone/pair mode lives only in
        // node1's memtable — without flushing, token redistribution makes it
        // unreachable via the coordinator (P0 data loss bug).
        if let Err(e) = self.storage.flush_all() {
            tracing::error!(%e, "failed to flush memtables before cluster transition");
        }

        // 0. Ensure PeerManager has outbound connections to ALL peers.
        //
        // BUG FIX: When transitioning from pair → cluster, only the first peer
        // (from transition_to_pair) has a reverse outbound pool. The second peer
        // (which triggered this transition) may only have an inbound connection.
        // Raft needs to SEND to all peers, so we must create outbound pools for
        // any peer the PeerManager doesn't already know about.
        let net_cfg = self.net_config.clone();
        let local_id = self.local_host_id;
        let internode_port = self.net_config.bind_addr.port();
        let raft_rt_for_connect = self.raft_runtime.get().cloned();
        let data_rt_for_connect = self.data_runtime.get().cloned();
        for (peer_uuid, peer_addr) in &peers {
            if !peer_manager.has_live_peer(*peer_uuid) {
                let pm = peer_manager.clone();
                let cfg = net_cfg.clone();
                let uuid = *peer_uuid;
                let reverse_addr = SocketAddr::new(peer_addr.ip(), internode_port);
                let raft_rt = raft_rt_for_connect.clone();
                let data_rt = data_rt_for_connect.clone();
                self.spawn_tracked(async move {
                    match PriorityPool::connect(
                        cfg,
                        local_id,
                        &reverse_addr.to_string(),
                        raft_rt,
                        data_rt,
                    )
                    .await
                    {
                        Ok(pool) => {
                            pm.add_peer((uuid, reverse_addr), pool).await;
                            tracing::info!(%uuid, %reverse_addr, "cluster: reverse connection established");
                        }
                        Err(e) => {
                            tracing::warn!(%uuid, %e, "cluster: reverse connection failed");
                        }
                    }
                });
            }
        }

        // 1. Create sled log store. W6.3 (ADR-015): per-DC subdir so a
        // node hosting multiple DCs (operator command `bootstrap-dc`,
        // W6.7) keeps each DC's log isolated. Single-DC deployments
        // land under `<base>/<local_dc>/`; on upgrade, an installer or
        // operator must migrate any existing flat-layout `raft_data_dir`
        // (ADR-015 R3). W6.8: a per-DC override
        // (`FERROSA_RAFT_DATA_DIR_<DC>`) takes precedence over the
        // node-level default.
        let raft_base = if let Some(dir) = self.config.raft_data_dir_for_dc(&local_dc) {
            dir
        } else {
            let data_dir =
                std::env::var("FERROSA_DATA_DIR").unwrap_or_else(|_| "/var/lib/ferrosa".into());
            std::path::Path::new(&data_dir).join("raft")
        };
        let raft_dir = raft_log_dir_for_dc(&raft_base, &local_dc);
        if let Err(e) = std::fs::create_dir_all(&raft_dir) {
            tracing::error!(%e, dir = %raft_dir.display(), "failed to create per-DC raft log dir");
            return;
        }
        let mut log_store = match SledLogStore::new(&raft_dir) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(%e, "failed to create Raft log store");
                return;
            }
        };
        let snapshot_path = raft_dir.join("state-machine.snapshot.bin");

        // 2. Create state machine from current schema
        let mut state_machine = FerrosStateMachine::with_side_effects_and_snapshot_path(
            self.schema.clone(),
            self.storage.clone(),
            snapshot_path,
        );
        // Join the two halves of the purge guard. The log store will not purge
        // past what this state machine reports durable; without this handshake
        // the watermark stays 0 and the guard is inert.
        state_machine.set_durable_applied_handle(log_store.durable_applied_handle());

        let recovered_persisted_snapshot = match state_machine.recover_from_persisted_snapshot() {
            Ok(recovered) => recovered,
            Err(e) => {
                tracing::warn!(%e, "failed to recover persisted raft snapshot");
                false
            }
        };

        // Reconcile the state machine against the log store's purge point
        // BEFORE openraft reads either. Two durable facts have to agree for a
        // node to restart on its own log, and until 2026-08-20 nothing checked
        // that they did -- openraft found out during re-apply and failed Fatal
        // with an index range and no cause. See `crate::raft::local_state`.
        match log_store.last_purged_log_id() {
            Ok(purge_point) => {
                let classification = crate::raft::local_state::classify_local_raft_state(
                    state_machine.last_applied_index(),
                    purge_point.map(|log_id| log_id.index),
                );
                match classification {
                    crate::raft::local_state::LocalRaftState::StrandedBehindPurge {
                        last_applied,
                        last_purged,
                    } => {
                        tracing::error!(
                            last_applied,
                            last_purged,
                            missing = format!("{}..={}", last_applied + 1, last_purged),
                            dir = %raft_dir.display(),
                            "local Raft state is unusable: entries were purged before they \
                             were applied, so this node holds no copy of them. Resetting \
                             local Raft state (retained for inspection) and rejoining to \
                             receive a snapshot from the leader."
                        );
                        match Self::reset_stranded_raft_state(&raft_dir) {
                            Ok(backup) => {
                                tracing::warn!(
                                    backup = %backup.as_deref().unwrap_or(std::path::Path::new("<none>")).display(),
                                    "stranded Raft state moved aside; rebuilding from the leader"
                                );
                                match SledLogStore::new(&raft_dir) {
                                    Ok(fresh) => {
                                        log_store = fresh;
                                        state_machine =
                                            FerrosStateMachine::with_side_effects_and_snapshot_path(
                                                self.schema.clone(),
                                                self.storage.clone(),
                                                raft_dir.join("state-machine.snapshot.bin"),
                                            );
                                        state_machine.set_durable_applied_handle(
                                            log_store.durable_applied_handle(),
                                        );
                                    }
                                    Err(e) => {
                                        tracing::error!(%e, "failed to recreate Raft log store after reset");
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    %e,
                                    "could not reset stranded Raft state; this node cannot \
                                     rejoin and needs operator repair"
                                );
                                return;
                            }
                        }
                    }
                    crate::raft::local_state::LocalRaftState::NeedsPurgePointBaseline {
                        ..
                    } => {
                        state_machine.recover_from_purge_point(purge_point);
                    }
                    crate::raft::local_state::LocalRaftState::Usable => {}
                }
            }
            Err(e) => tracing::warn!(%e, "failed to read last_purged from log store"),
        }

        if !state_machine.has_topology_state() {
            match log_store.recover_topology_state() {
                Ok(topology) if !topology.members.is_empty() || !topology.token_map.is_empty() => {
                    tracing::warn!(
                        member_count = topology.members.len(),
                        token_count = topology.token_map.len(),
                        "raft state machine snapshot missing topology; recovered committed topology from raft log"
                    );
                    state_machine.seed_topology(topology.members, topology.token_map);
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(%e, "failed to recover topology from raft log"),
            }
        }

        // Recover membership from the log if it was lost (e.g., OOM kill
        // before snapshot persisted). Without valid membership, no election
        // can happen and the cluster stays stuck as Learners.
        let has_recovered_membership = match log_store.find_last_membership() {
            Ok(membership) => {
                let recovered_from_log = membership.is_some();
                state_machine.recover_membership(membership);
                let recovered_from_topology =
                    state_machine.recover_membership_from_topology_state();
                if recovered_from_topology && !recovered_from_log {
                    tracing::warn!(
                        member_count = state_machine.state().members.len(),
                        "raft membership was missing from log; synthesized voters from committed topology state"
                    );
                }
                recovered_from_log || recovered_from_topology
            }
            Err(e) => {
                tracing::warn!(%e, "failed to scan log for membership");
                let recovered_from_topology =
                    state_machine.recover_membership_from_topology_state();
                if recovered_from_topology {
                    tracing::warn!(
                        member_count = state_machine.state().members.len(),
                        "raft membership scan failed; synthesized voters from committed topology state"
                    );
                }
                recovered_from_topology
            }
        };
        let has_recovered_topology_state = state_machine.has_topology_state();

        // 3. Create network factory
        let network_factory = FerrosRaftNetworkFactory::new(peer_manager.clone());
        let local_node_id = uuid_to_node_id(self.local_host_id);

        // Register node mappings for all peers
        for (peer_uuid, _addr) in &peers {
            let peer_node_id = uuid_to_node_id(*peer_uuid);
            network_factory.register_node(peer_node_id, *peer_uuid);
        }
        // Register self
        network_factory.register_node(local_node_id, self.local_host_id);

        // Capture the node_map Arc before the factory is consumed by FerrosRaft::new.
        // This shared map is used by DdlPath::Cluster to resolve leader NodeId → Uuid.
        let node_map_for_ddl = network_factory.node_map();
        let node_map_for_bootstrap = node_map_for_ddl.clone();
        // Publish the shared node_map so `on_peer_connected` can register a
        // (re)connecting peer's host_id — the leader must learn a reconnecting
        // committed member's host_id even when no `JoinNode` fires, or raft RPCs
        // to it stay "registration pending" forever.
        self.raft_node_map.store(Some(node_map_for_ddl.clone()));
        // Clone peer_manager for DdlPath::Cluster forwarding (ClusterCoordinator
        // will consume `peer_manager` below).
        let peer_manager_for_ddl = peer_manager.clone();
        let peer_manager_for_bootstrap = peer_manager.clone();
        let local_host_id_for_refresh = self.local_host_id;
        // Advertise the re-resolvable internode-broadcast HOSTNAME (when configured)
        // as this node's own NodeInfo.addr, so the seed/self membership entry is not
        // frozen to the startup IP and re-resolves across container IP churn.
        let local_addr_for_refresh = self.net_config.advertised_internode_addr();
        let local_raft_addr_for_init = self.net_config.broadcast_addr;
        let local_cql_broadcast_for_refresh = self.config.cql_broadcast.clone();

        // 4. Build TokenRing with deterministic initial tokens.
        // If a durable snapshot was recovered, let the state machine repopulate
        // the live ring from committed topology instead of reseeding a fresh
        // local-only bootstrap view.
        let ring_arc = Arc::new(ArcSwap::from_pointee(TokenRing::new()));
        if state_machine.has_topology_state() {
            tracing::info!(
                recovered_persisted_snapshot,
                member_count = state_machine.state().members.len(),
                token_count = state_machine.state().token_map.len(),
                "recovered raft topology from persisted state machine snapshot"
            );
            state_machine.set_ring(ring_arc.clone());
            state_machine.set_ring_observer(self.ring.clone());
            state_machine.sync_live_ring_from_state();
        } else {
            let mut ring = TokenRing::new();

            // Add local node — use the re-resolvable internode-broadcast hostname
            // (when configured) so the seed entry survives container IP churn.
            let broadcast = self.net_config.advertised_internode_addr();
            ring.add_node(
                local_node_id,
                NodeInfo {
                    host_id: self.local_host_id,
                    addr: broadcast,
                    data_center: self.config.data_center.clone(),
                    rack: self.config.rack.clone(),
                    state: NodeState::Normal,
                    cql_broadcast: self.config.cql_broadcast.clone(),
                },
            );

            // Add peers
            for (peer_uuid, addr) in &peers {
                let peer_node_id = uuid_to_node_id(*peer_uuid);
                let peer_internode_broadcast =
                    peer_internode_broadcasts.get(peer_uuid).cloned().flatten();
                ring.add_node(
                    peer_node_id,
                    NodeInfo {
                        host_id: *peer_uuid,
                        addr: super::membership::node_info_addr(
                            *addr,
                            peer_internode_broadcast.as_deref(),
                        ),
                        data_center: self.config.data_center.clone(),
                        rack: self.config.rack.clone(),
                        state: NodeState::Normal,
                        cql_broadcast: peer_cql_broadcasts.get(peer_uuid).cloned().flatten(),
                    },
                );
            }

            let num_tokens = self.config.num_tokens as usize;
            let local_tokens =
                crate::controller::token::deterministic_tokens_for_node(local_node_id, num_tokens);
            ring.assign_tokens(local_node_id, &local_tokens);

            tracing::info!(
                local = local_node_id,
                peer_count = peers.len(),
                num_tokens,
                "building initial token ring (self-only); peers will populate via Raft"
            );

            ring_arc.store(Arc::new(ring));

            let mut members = std::collections::BTreeMap::new();
            let mut token_map = std::collections::BTreeMap::new();
            let ring_snap = ring_arc.load();
            if let Some(info) = ring_snap.get_node(local_node_id) {
                members.insert(local_node_id, info.clone());
            }
            for tok in &local_tokens {
                token_map.insert(*tok, local_node_id);
            }
            state_machine.seed_topology(members, token_map);
            state_machine.set_ring(ring_arc.clone());
            state_machine.set_ring_observer(self.ring.clone());
        }

        // Expose the live ring snapshot for observability (web API, CLI).
        // We capture a snapshot of the ring at this point; it will be updated
        // by the Raft state machine as tokens are reassigned.
        {
            let ring_snapshot = Arc::new((**ring_arc.load()).clone());
            self.set_token_ring(ring_snapshot);
        }

        // 5. Create coordinator
        //
        // Use the maximum RF across all user keyspaces so the coordinator
        // enforces the durability guarantee the operator configured.
        // CL=ONE is kept intentionally: during formation the cluster may not
        // yet have enough live replicas to meet a higher CL, and CL is
        // orthogonal to RF — operators can ALTER the CL policy once all nodes
        // reach Normal state.
        //
        // If fewer nodes are currently available than the configured RF, we
        // accept writes with reduced durability and emit a loud warning so
        // operators know to run REPAIR once the ring is fully formed.
        let initial_rf = resolve_formation_rf(&self.schema, &self.config.data_center, 3);
        let cluster_size = peers.len() + 1; // self + peers
        let initial_cl = ConsistencyLevel::One;
        if initial_rf > cluster_size {
            FORMATION_REDUCED_DURABILITY_WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::warn!(
                configured_rf = initial_rf,
                current_cluster_size = cluster_size,
                "FORMATION DURABILITY WARNING: cluster has fewer nodes than configured RF; \
                 writes during formation have reduced durability. Run REPAIR once all \
                 nodes reach Normal state to restore full replication."
            );
        }
        let coordinator = Arc::new(
            ClusterCoordinator::new(
                ring_arc.clone(),
                peer_manager,
                local_node_id,
                self.storage.clone(),
                initial_rf,
                initial_cl,
            )
            .with_hint_store(self.hint_store.clone()),
        );

        let repair_metrics_for_handler = coordinator.repair_metrics.clone();
        // Capture handles for the ADR-020 streaming-handler registration
        // below; the coordinator itself is moved into WritePath::cluster
        // on the next line and is no longer directly accessible.
        let stream_router_for_handler = coordinator.stream_router();
        let peer_manager_for_handler = coordinator.peer_manager.clone();
        let peer_manager_for_fulltext = coordinator.peer_manager.clone();

        // 6. Swap write path — cluster coordinator handles replica routing.
        self.write_path
            .store(Arc::new(WritePath::cluster(coordinator)));

        // DdlPath::Cluster needs the Raft instance — Raft initialization is async
        // and happens in a background task. Keep DDL on Direct path during the
        // transition window so standalone/pair DDL continues to work. Once Raft
        // is initialized and a leader is elected, the background task will:
        //   1. Swap DDL path to DdlPath::Cluster
        //   2. Replay the current local schema state through Raft so all
        //      followers converge on the same schema
        self.ddl_path.store(Arc::new(DdlPath::Direct {
            schema: self.schema.clone(),
            engine: self.storage.clone(),
        }));

        // Swap cluster state to Raft-based
        self.cluster_state
            .store(Arc::new(ClusterStateHolder::Cluster(
                RaftClusterState::with_peer_manager(
                    ring_arc,
                    local_node_id,
                    peer_manager_for_bootstrap.clone(),
                ),
            )));

        // Clear pair context — no longer in pair mode
        *self.pair_context.lock() = None;

        self.try_transition_mode(DeploymentMode::Cluster);

        // Durably record that this node has been a cluster member, BEFORE
        // announcing the transition. On restart this is what stops the node
        // re-deriving its shape from whichever peer reconnects first and
        // settling into a pair while the leader is still replicating to it.
        //
        // A failure here is logged, not fatal: the node is a working cluster
        // member right now, and refusing to run because a marker could not be
        // written would turn a future recovery problem into a present outage.
        // It does mean a restart would forget, so it is an ERROR.
        let raft_dir = resolve_raft_dir(&self.config);
        if let Err(error) = DeploymentMode::record_cluster_membership(&raft_dir) {
            tracing::error!(
                %error,
                dir = %raft_dir.display(),
                "could not record cluster membership; this node will forget it was \
            a member if it restarts, and may rejoin as a pair"
            );
        }

        tracing::info!(
            node_id = local_node_id,
            peers = peers.len(),
            "mode transition: pair -> cluster (raft init spawned)"
        );

        // Spawn background Raft initialization — Raft::new() is async and
        // must not block the PeerEventListener callback.
        let raft_groups_swap = self.raft_groups.clone();
        let local_raft_group_id = crate::raft::RaftGroupId::for_dc(&self.config.data_center);
        let ddl_path = self.ddl_path.clone();
        let mode_swap = self.mode.clone();
        let registry = self.registry.clone();
        let storage_for_bootstrap = self.storage.clone();
        let schema_for_bootstrap = self.schema.clone();
        let ring_for_bootstrap = self.ring.clone();
        // Used by the bootstrap-promotion logic later in the spawn block to
        // count how many BootstrapComplete acks to wait for and to issue
        // SetNodeState{Normal} for each non-leader. Local view of the
        // member set is fine here because it's only used for bookkeeping
        // (counts + iteration), NOT for token-ring construction (which is
        // now Raft-driven via the seed-authored JoinNode + AssignTokens).
        let all_node_ids_for_bootstrap: Vec<u64> = std::iter::once(local_node_id)
            .chain(peers.iter().map(|(uuid, _)| uuid_to_node_id(*uuid)))
            .collect();
        let cluster_name = self.config.cluster_name.clone();
        let config_for_promotion = self.config.clone();
        let raft_heartbeat_ms = self.config.raft_heartbeat_ms;
        let raft_election_min_ms = self.config.raft_election_timeout_min_ms;
        let raft_election_max_ms = self.config.raft_election_timeout_max_ms;
        // ADR-012: PreVote + CheckQuorum knobs from the ferrosa-cluster config.
        let raft_enable_pre_vote = self.config.raft_enable_pre_vote;
        let raft_check_quorum_ratio = self.config.raft_check_quorum_ratio;
        let schema_for_replay = self.schema.clone();
        let ddl_queue_rx = self.ddl_queue_rx.clone();
        let raft_runtime: Option<Arc<tokio::runtime::Runtime>> = self.raft_runtime.get().cloned();
        // Cancel token forwarded into the bootstrap spawn so that the election
        // guard watchdog respects graceful shutdown.
        let election_guard_cancel = self.cancel.clone();
        // Register Raft RPC handlers BEFORE spawning the init task.
        // Handlers use LazyRaft to wait for the instance to be ready.
        // This eliminates the race where vote requests arrive before
        // handlers are registered.
        use crate::raft::handlers::LazyRaft;
        let (raft_tx, lazy_raft) = LazyRaft::channel();

        let append_handler = Arc::new(RaftAppendHandler::new(lazy_raft.clone()));
        self.registry
            .register(MsgType::RaftAppendEntries, append_handler);

        let vote_handler = Arc::new(RaftVoteHandler::new(lazy_raft.clone()));
        self.registry.register(MsgType::RaftVote, vote_handler);

        let snapshot_handler = Arc::new(RaftSnapshotHandler::new(lazy_raft));
        self.registry
            .register(MsgType::RaftInstallSnapshot, snapshot_handler);

        let (cluster_forward_tx, cluster_forward_lazy_raft) = LazyRaft::channel();
        let cluster_forward_handler = Arc::new(
            crate::raft_forward::LazyClusterMembershipForwardHandler::new(
                cluster_forward_lazy_raft,
            ),
        );
        self.registry
            .register(MsgType::ClusterMembershipForward, cluster_forward_handler);

        let repair_handler = Arc::new(RepairWriteHandler::new(
            self.storage.clone(),
            repair_metrics_for_handler,
        ));
        self.registry.register(MsgType::RepairWrite, repair_handler);

        let range_read_handler = Arc::new(RangeReadHandler::new(self.storage.clone()));
        self.registry
            .register(MsgType::RangeReadRequest, range_read_handler);

        // ADR-020 streaming range-read handlers. These coexist with
        // the legacy single-shot RangeReadRequest path above —
        // the streaming path is selected by the coordinator only
        // when FERROSA_BULK_STREAMING_RANGE_READ=1. Both endpoints
        // are registered on every node so a mixed-mode cluster can
        // serve either flow.
        //
        // Server side: handles inbound RangeReadStreamRequest by
        // spawning a task that fires chunks back to the originator
        // via PeerManager::fire on Lane::Bulk.
        use crate::coordinator::range_read_stream::STREAMING_CHUNK_PARTITIONS;
        use crate::coordinator::stream_request_handler::{
            PeerManagerSinkFactory, RangeReadStreamRequestHandler,
        };
        let sink_factory = Arc::new(PeerManagerSinkFactory::new(peer_manager_for_handler));
        // The new StreamRangeReader trait (ADR-020 Phase 2 work) is
        // implemented on `Arc<StorageEngine>` because the impl needs
        // to clone the engine handle for spawn_blocking. The factory
        // wraps that in another Arc for shared ownership across
        // concurrent RPC handlers — `Arc<Arc<StorageEngine>>` is
        // cheap (two atomic pointer copies) and lets the trait stay
        // generic over `R: StreamRangeReader + 'static`.
        let stream_request_handler = Arc::new(RangeReadStreamRequestHandler::new(
            Arc::new(self.storage.clone()),
            sink_factory,
            STREAMING_CHUNK_PARTITIONS,
        ));
        self.registry.register(
            MsgType::RangeReadStreamRequest,
            stream_request_handler.clone(),
        );
        // The SAME handler also serves RangeReadStreamCancel: it owns the
        // per-request `CancellationToken` map, so a coordinator that abandons a
        // coordinated stream mid-flight (every paged read, on every page but
        // the last — t_3fc6be3c/t_dc729b1d) can fire `RangeReadStreamCancel`
        // and have the in-flight producer stop between batches. Without this
        // registration the cancel frame arrives with no handler and is
        // dropped, leaking a whole-table scan onto the Bulk lane.
        self.registry
            .register(MsgType::RangeReadStreamCancel, stream_request_handler);

        // Coordinator side: routes inbound chunk/heartbeat/done
        // frames through the coordinator's shared StreamRouter so
        // they reach the per-request consume_range_stream receiver.
        // Single handler instance registered against all three
        // streaming response MsgTypes.
        use crate::coordinator::stream_frame_router::StreamFrameRouter;
        let frame_router = Arc::new(StreamFrameRouter::new(stream_router_for_handler));
        self.registry
            .register(MsgType::RangeReadStreamChunk, frame_router.clone());
        self.registry
            .register(MsgType::RangeReadStreamHeartbeat, frame_router.clone());
        self.registry
            .register(MsgType::RangeReadStreamDone, frame_router.clone());

        // Streaming fulltext search (t_4ae47a9f) — the fts_match twin of the
        // ADR-020 streaming range read. Server side: walks the local FTI via
        // fulltext_search_each on a blocking thread and fires bounded key
        // chunks back on Lane::Bulk. Registered on every node alongside the
        // legacy single-shot FulltextSearchRequest so mixed-mode clusters can
        // serve either flow (the coordinator picks per query shape).
        use crate::coordinator::fulltext_stream::{
            FulltextSearchStreamRequestHandler, FULLTEXT_STREAM_CHUNK_KEYS,
        };
        let fulltext_sink_factory = Arc::new(
            crate::coordinator::stream_request_handler::PeerManagerSinkFactory::new(
                peer_manager_for_fulltext,
            ),
        );
        let fulltext_stream_handler = Arc::new(FulltextSearchStreamRequestHandler::new(
            Arc::new(self.storage.clone()),
            fulltext_sink_factory,
            FULLTEXT_STREAM_CHUNK_KEYS,
        ));
        self.registry.register(
            MsgType::FulltextSearchStreamRequest,
            fulltext_stream_handler.clone(),
        );
        // Same handler serves Cancel — it owns the per-request tokens.
        self.registry
            .register(MsgType::FulltextSearchStreamCancel, fulltext_stream_handler);
        // Coordinator side: the shared StreamFrameRouter routes the fulltext
        // response frames through the same seq/Done bookkeeping.
        self.registry
            .register(MsgType::FulltextSearchStreamChunk, frame_router.clone());
        self.registry
            .register(MsgType::FulltextSearchStreamHeartbeat, frame_router.clone());
        self.registry
            .register(MsgType::FulltextSearchStreamDone, frame_router);

        let index_read_handler = Arc::new(IndexReadHandler::new(self.storage.clone()));
        self.registry
            .register(MsgType::IndexReadRequest, index_read_handler);

        // Keyed (partition-restricted) secondary-index reads (t_430c4188) —
        // without this, a remote coordinator's keyed index consult would time
        // out and the query would degrade to its partition-scan fallback.
        let index_read_in_partition_handler =
            Arc::new(IndexReadInPartitionHandler::new(self.storage.clone()));
        self.registry.register(
            MsgType::IndexReadInPartitionRequest,
            index_read_in_partition_handler,
        );

        let fulltext_search_handler = Arc::new(FulltextSearchHandler::new(self.storage.clone()));
        self.registry
            .register(MsgType::FulltextSearchRequest, fulltext_search_handler);

        let read_handler = Arc::new(ReadRequestHandler::new(self.storage.clone()));
        self.registry
            .register(MsgType::ReadRequest, read_handler.clone());
        self.registry
            .register(MsgType::PartitionSuffixReadRequest, read_handler);

        // Register batchlog handlers — without these, logged batch writes
        // sent to remote nodes are silently dropped.
        let batchlog_write = Arc::new(BatchlogWriteHandler::new(self.storage.clone()));
        self.registry
            .register(MsgType::BatchlogWrite, batchlog_write);
        let batchlog_delete = Arc::new(BatchlogDeleteHandler::new(self.storage.clone()));
        self.registry
            .register(MsgType::BatchlogDelete, batchlog_delete);
        let batchlog_replay = Arc::new(BatchlogReplayHandler::new(self.storage.clone()));
        self.registry
            .register(MsgType::BatchlogReplay, batchlog_replay);

        // Register truncate handler — without this, TRUNCATE TABLE only
        // clears the coordinator's local storage; remote replicas keep data.
        let truncate_handler = Arc::new(TruncateForwardHandler::new(self.storage.clone()));
        self.registry
            .register(MsgType::TruncateForward, truncate_handler);

        // Register Accord consensus handlers for all 6 inbound message types.
        // The AccordHandler dispatches to a shared AccordStateMachine. Response
        // types (PreAcceptOK, AcceptOK, ReadOK, ApplyOK, RecoverOK) are sent
        // back by the coordinator, not received as RPC requests.
        let accord_state_for_maintenance;
        {
            use crate::accord::handlers::{publish_accord_state, AccordHandler, AccordState};
            use crate::accord::state_machine::build_accord_state_machine;
            use ferrosa_storage::accord::sync_writer::FileSyncWriter;

            let accord_dir = std::env::var("FERROSA_DATA_DIR")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| std::path::PathBuf::from("/var/lib/ferrosa"))
                .join("accord");
            let accord_dir = match std::fs::create_dir_all(&accord_dir) {
                Ok(()) => accord_dir,
                Err(_) => {
                    // Fallback to temp dir (e.g., in tests or containerless envs).
                    let tmp = std::env::temp_dir().join("ferrosa-accord");
                    let _ = std::fs::create_dir_all(&tmp);
                    tmp
                }
            };
            // The sync writer needs the append-only log FILE, not the directory.
            // Passing `accord_dir` here made every write_and_sync open a directory
            // (EISDIR) → every PreAccept persist failed → SmResponse::None → every
            // Accord transaction failed "quorum unavailable" (self + remote votes).
            let sync_writer = Arc::new(FileSyncWriter::new(accord_dir.join("protocol.log")));
            // Wire the live StorageEngine into the Accord state machine so an
            // applied LWT is durably persisted BEFORE the replica returns
            // ApplyOK — closing the production phantom-write gap
            // (bug-accord-lwt-acks-phantom-write.md). Previously this used
            // `AccordStateMachine::new`, which carries a NoopStorageApplier:
            // a replica recorded (txn_id, t) and acked while nothing landed.
            let built: AccordState = Arc::new(parking_lot::Mutex::new(build_accord_state_machine(
                uuid_to_node_id(self.local_host_id),
                sync_writer,
                self.storage.clone(),
                self.accord_clock(),
            )));
            // Publish into the shared slot so the session layer's transaction
            // committer votes the coordinator's own PreAccept against THIS exact
            // instance (same Arc the handler serves) — the last mile that makes
            // live BEGIN…COMMIT reach quorum instead of "Accord quorum
            // unavailable" (a node is never in its own peer map).
            let accord_state = publish_accord_state(&self.accord_state_slot, built);
            accord_state_for_maintenance = accord_state.clone();

            let accord_handler = Arc::new(AccordHandler::new(
                accord_state,
                uuid_to_node_id(self.local_host_id),
            ));
            self.registry
                .register(MsgType::AccordPreAccept, accord_handler.clone());
            self.registry
                .register(MsgType::AccordPreAcceptV2, accord_handler.clone());
            self.registry
                .register(MsgType::AccordAccept, accord_handler.clone());
            self.registry
                .register(MsgType::AccordCommit, accord_handler.clone());
            self.registry
                .register(MsgType::AccordRead, accord_handler.clone());
            self.registry
                .register(MsgType::AccordApply, accord_handler.clone());
            self.registry
                .register(MsgType::AccordApplyV2, accord_handler.clone());
            self.registry
                .register(MsgType::AccordRecover, accord_handler);
        }

        // Register streaming handlers — row-based and SSTable file-based.
        // Without these, bootstrap streaming from the leader fails with
        // "no handler registered msg_type=StreamStart" on the receiver.
        let stream_handler = Arc::new(crate::streaming::StreamHandler::new(self.storage.clone()));
        self.registry
            .register(MsgType::StreamStart, stream_handler.clone());
        self.registry
            .register(MsgType::StreamChunk, stream_handler.clone());
        self.registry.register(MsgType::StreamEnd, stream_handler);

        let data_dir = std::env::var("FERROSA_DATA_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from("/var/lib/ferrosa"));
        let sstable_stream_handler =
            Arc::new(crate::streaming::SstableStreamHandler::new(data_dir));
        self.registry
            .register(MsgType::SstableStreamStart, sstable_stream_handler.clone());
        self.registry
            .register(MsgType::SstableStreamChunk, sstable_stream_handler.clone());
        self.registry
            .register(MsgType::SstableStreamEnd, sstable_stream_handler);

        // Register BootstrapComplete handler — increments a shared counter
        // so the leader's polling loop can track how many nodes have finished.
        let bootstrap_complete_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let counter = bootstrap_complete_count.clone();
            struct BootstrapCompleteHandler {
                counter: Arc<std::sync::atomic::AtomicUsize>,
            }
            #[async_trait::async_trait]
            impl RpcHandler for BootstrapCompleteHandler {
                async fn handle(
                    &self,
                    _from: ferrosa_net::rpc::handler::PeerId,
                    msg: Message,
                ) -> Option<Message> {
                    if let Message::BootstrapComplete { node_id } = msg {
                        let prev = self
                            .counter
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::info!(
                            count = prev + 1,
                            %node_id,
                            "received BootstrapComplete from peer"
                        );
                    }
                    Some(Message::BootstrapCompleteAck)
                }
            }
            let handler = Arc::new(BootstrapCompleteHandler { counter });
            self.registry.register(MsgType::BootstrapComplete, handler);
        }

        // Spawn periodic maintenance loop for memory-bounded data structures
        // and storage drain work requested by foreground writes.
        {
            let storage = self.storage.clone();
            let accord_state = accord_state_for_maintenance;
            // Honor shutdown: without this the maintenance loop ignores the
            // cancel token and shutdown() blocks the full 10s drain deadline
            // before abort_all() force-kills it (×3 nodes = ~30s in tests).
            let maint_cancel = self.cancel.clone();
            self.spawn_tracked(async move {
                let urgent_flush_interval_millis =
                    std::env::var("FERROSA_URGENT_FLUSH_INTERVAL_MILLIS")
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(100u64)
                        .max(1);
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(
                    urgent_flush_interval_millis,
                ));
                let mut ticks = 0u64;
                loop {
                    // Stop promptly on shutdown instead of being force-aborted.
                    tokio::select! {
                        _ = maint_cancel.cancelled() => break,
                        _ = interval.tick() => {}
                    }
                    ticks = ticks.wrapping_add(1);

                    if storage.take_flush_request() {
                        let storage_for_flush = storage.clone();
                        match tokio::task::spawn_blocking(move || {
                            storage_for_flush.flush_if_needed()
                        })
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => tracing::warn!(
                                %e,
                                "maintenance: requested storage flush failed"
                            ),
                            Err(e) => tracing::warn!(
                                %e,
                                "maintenance: requested storage flush task failed"
                            ),
                        }
                    }

                    if storage.take_s3_sync_request() {
                        match storage.sync_sstables_to_s3().await {
                            Ok(uploaded) => {
                                if uploaded > 0 {
                                    tracing::info!(
                                        uploaded,
                                        "maintenance: uploaded queued SSTables to S3"
                                    );
                                }
                            }
                            Err(e) => tracing::warn!(
                                %e,
                                "maintenance: requested S3 SSTable sync failed"
                            ),
                        }
                    }

                    // Log closed-segment buffer memory (P0 OOM regression detector).
                    let closed_buf_bytes = storage.closed_segment_buffer_bytes();
                    if closed_buf_bytes > 0 {
                        tracing::warn!(
                            closed_buf_bytes,
                            "maintenance: closed commit log segments still holding buffer memory"
                        );
                    }

                    let prune_ticks = (60_000u64 / urgent_flush_interval_millis).max(1);
                    if ticks.is_multiple_of(prune_ticks) {
                        // Prune applied Accord transactions to prevent unbounded
                        // memory growth in txn_states and committed_txns.
                        let pruned = {
                            let mut sm = accord_state.lock();
                            sm.prune_applied()
                        };
                        if pruned > 0 {
                            tracing::info!(
                                pruned,
                                "maintenance: pruned applied Accord transactions"
                            );
                        }

                        // Log table-level memory stats.
                        let table_count = storage.table_count();
                        let accord_txns = accord_state.lock().txn_count();
                        tracing::info!(
                            table_count,
                            closed_buf_bytes,
                            accord_txns,
                            "maintenance: periodic memory check"
                        );
                    }
                }
            });
        }

        let bootstrap_complete_counter = bootstrap_complete_count;
        let consensus_health = self.consensus_health();
        self.spawn_tracked(async move {
            // Deliver ClusterInvite to all peers BEFORE starting Raft.
            // This ensures peers transition to cluster mode and register
            // Raft handlers before elections begin.
            // Build invite inside the async block using captured peer list.
            // local_host_id is captured via the `peers` vec (all peers except self).
            let invite_initiator = {
                // Recover the local UUID from the node_map.
                // Read through poison — a poisoned lock still has valid data.
                let map = node_map_for_bootstrap.read().unwrap_or_else(|e| e.into_inner());
                map.get(&local_node_id).copied().unwrap_or_default()
            };
            let invite = Message::ClusterInvite {
                initiator: invite_initiator,
                peers: peers
                    .iter()
                    .map(|(id, addr)| (*id, *addr))
                    .collect(),
            };
            for (peer_id, _) in &peers {
                for attempt in 0..10 {
                    match peer_manager_for_bootstrap
                        .send(*peer_id, invite.clone(), Lane::Data)
                        .await
                    {
                        Ok(_) => {
                            tracing::info!(peer = %peer_id, "ClusterInvite delivered");
                            break;
                        }
                        Err(e) => {
                            if attempt < 9 {
                                tracing::debug!(
                                    peer = %peer_id, attempt, %e,
                                    "ClusterInvite delivery retry"
                                );
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            } else {
                                tracing::warn!(
                                    peer = %peer_id,
                                    "ClusterInvite delivery failed after 10 attempts"
                                );
                            }
                        }
                    }
                }
            }

            // Build openraft Config
            //
            // ADR-012 wires `enable_pre_vote` (Ongaro §9.6) and
            // `check_quorum_ratio` (Ongaro §6.4). Both fields are ferrosa-fork
            // extensions exposed by the patched openraft (`correctness/prevote-checkquorum`
            // branch) and inert against upstream openraft (defaults are
            // upstream-compatible: pre_vote=false, ratio=0.0).
            let raft_max_payload_entries = match std::env::var("FERROSA_RAFT_MAX_PAYLOAD_ENTRIES")
                .ok()
                .and_then(|raw| match raw.parse::<u64>() {
                    Ok(n) if n > 0 => Some(n),
                    Ok(_) => {
                        tracing::warn!(
                            "FERROSA_RAFT_MAX_PAYLOAD_ENTRIES must be greater than zero, using default"
                        );
                        None
                    }
                    Err(e) => {
                        tracing::warn!(
                            %e,
                            "FERROSA_RAFT_MAX_PAYLOAD_ENTRIES parse error, using default"
                        );
                        None
                    }
                }) {
                Some(n) => n,
                None => openraft::Config::default().max_payload_entries,
            };

            let raft_config = match (openraft::Config {
                cluster_name,
                heartbeat_interval: raft_heartbeat_ms,
                // Data replication gets 10x the heartbeat timeout so followers
                // have time for sled disk writes without blocking heartbeats.
                replication_lag_timeout: raft_heartbeat_ms * 10,
                election_timeout_min: raft_election_min_ms,
                election_timeout_max: raft_election_max_ms,
                max_payload_entries: raft_max_payload_entries,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
                enable_pre_vote: raft_enable_pre_vote,
                check_quorum_ratio: raft_check_quorum_ratio,
                ..Default::default()
            })
            .validate()
            {
                Ok(cfg) => Arc::new(cfg),
                Err(e) => {
                    tracing::error!(%e, "invalid raft config, staying in cluster mode without raft DDL");
                    return;
                }
            };

            // Option B: Wait for Raft lane readiness before creating the
            // Raft instance. Elections start as soon as `FerrosRaft::new()`
            // returns, so outbound connections must exist first.
            {
                let deadline = tokio::time::Instant::now()
                    + std::time::Duration::from_secs(10);
                for (peer_uuid, _) in &peers {
                    let mut waited = false;
                    while !peer_manager_for_bootstrap.has_live_peer(*peer_uuid) {
                        if !waited {
                            tracing::debug!(
                                peer = %peer_uuid,
                                "waiting for peer connection..."
                            );
                            waited = true;
                        }
                        if tokio::time::Instant::now() > deadline {
                            tracing::warn!(
                                peer = %peer_uuid,
                                "Raft lane readiness timeout — proceeding anyway"
                            );
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
                tracing::info!("all Raft lane connections verified");
            }

            // Non-seed nodes create Raft promptly — they need to be ready
            // to RESPOND to Vote RPCs from the seed. Without a Raft instance,
            // the LazyRaft handlers timeout, preventing the seed from winning.
            // The key invariant: only the seed calls initialize(), so only the
            // seed starts elections. Non-seeds are passive responders.

            // Create the Raft instance on the dedicated Raft runtime so openraft's
            // internal tasks (replication, election) run there. This keeps
            // reply_rx.await off the busy main runtime, eliminating 100ms+
            // scheduling delays on heartbeat round-trips.
            let persisted_raft_state = match <SledLogStore as openraft::storage::RaftLogStorage<
                crate::raft::FerrosRaftConfig,
            >>::get_log_state(&mut log_store)
            .await
            {
                Ok(log_state) => {
                    let has_persisted_vote =
                        match <SledLogStore as openraft::storage::RaftLogStorage<
                            crate::raft::FerrosRaftConfig,
                        >>::read_vote(&mut log_store)
                        .await
                        {
                            Ok(vote) => vote.is_some(),
                            Err(e) => {
                                tracing::warn!(%e, "failed to read raft vote before bootstrap");
                                false
                            }
                        };
                    (log_state.last_log_id.is_some(), has_persisted_vote)
                }
                Err(e) => {
                    tracing::warn!(%e, "failed to read raft log state before bootstrap");
                    (false, false)
                }
            };

            let raft = if let Some(raft_rt) = raft_runtime.as_ref() {
                match raft_rt
                    .spawn(async move {
                        FerrosRaft::new(
                            local_node_id,
                            raft_config,
                            network_factory,
                            log_store,
                            state_machine,
                        )
                        .await
                    })
                    .await
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(fatal)) => {
                        tracing::error!(%fatal, "raft initialization failed (Fatal)");
                        return;
                    }
                    Err(e) => {
                        tracing::error!(%e, "raft runtime join error");
                        return;
                    }
                }
            } else {
                match FerrosRaft::new(
                    local_node_id,
                    raft_config,
                    network_factory,
                    log_store,
                    state_machine,
                )
                .await
                {
                    Ok(r) => r,
                    Err(fatal) => {
                        tracing::error!(%fatal, "raft initialization failed (Fatal)");
                        return;
                    }
                }
            };

            let raft_arc = Arc::new(raft);

            // Publish the Raft instance — handlers waiting in LazyRaft::get() will unblock.
            // If no subscribers are listening (LazyRaft was never queried), the
            // watch send returns Err. That's not benign at bootstrap time: it
            // means the cluster came up but nothing in this process has hold
            // of the Raft handle, so the node will silently fall behind.
            // Surface this as an error and increment the silent-failure
            // counter so the situation is observable.
            if let Err(e) = raft_tx.send(Some(raft_arc.clone())) {
                RAFT_PUBLISH_NO_SUBSCRIBERS.fetch_add(1, AtomicOrdering::Relaxed);
                tracing::error!(
                    error = %e,
                    "raft instance published to a watch with no subscribers — \
                     LazyRaft consumers may be missing; node will not handle Raft RPCs"
                );
            }
            if let Err(e) = cluster_forward_tx.send(Some(raft_arc.clone())) {
                RAFT_PUBLISH_NO_SUBSCRIBERS.fetch_add(1, AtomicOrdering::Relaxed);
                tracing::error!(
                    error = %e,
                    "raft instance published to ClusterMembershipForward watch with no subscribers"
                );
            }

            // Also publish to the controller's raft_groups map so that
            // controller.raft() returns Some() during the election loop.
            // Without this, external callers (tests, DDL) cannot observe
            // the leader until the entire background task completes.
            //
            // ADR-015: per-DC Raft groups are keyed by RaftGroupId derived
            // from the configured data_center. Single-DC deployments install
            // exactly one group; multi-DC bootstrap (Sprint 6 W6.3) extends
            // this to one per DC.
            {
                let prev = raft_groups_swap.load_full();
                let mut next: std::collections::HashMap<
                    crate::raft::RaftGroupId,
                    Arc<FerrosRaft>,
                > = (*prev).clone();
                next.insert(local_raft_group_id, raft_arc.clone());
                raft_groups_swap.store(Arc::new(next));
            }

            // Spawn the election-storm watchdog (P0-17 fix, path c).
            //
            // The watchdog monitors term deltas on this node.  When a node
            // whose log has fallen behind the cluster repeatedly fires
            // elections (storm), the watchdog suppresses elections via
            // `enable_elect(false)` so the leader can deliver an
            // InstallSnapshot without contention.  Elections are
            // automatically re-enabled after STORM_SUPPRESS_MS.
            {
                use crate::raft::election_guard::run_election_guard;
                let guard_raft = raft_arc.clone();
                let guard_cancel = election_guard_cancel.clone();
                let guard_timeout_min = raft_election_min_ms;
                ferrosa_net::task_pool::TaskPool::current("raft-election-guard").spawn(async move {
                    run_election_guard(guard_raft, guard_cancel, guard_timeout_min).await;
                });
            }

            // Publish Raft leadership/term as Prometheus gauges. A dedicated
            // poller (NOT the election guard — the guard is ADR-012-deprecated)
            // derives leadership from `current_leader()`, the same reliable
            // source `/readyz` uses.
            {
                use crate::raft::consensus_metrics::run_consensus_metrics_poller;
                let metrics_raft = raft_arc.clone();
                let metrics_cancel = election_guard_cancel.clone();
                let metrics_health = consensus_health.clone();
                ferrosa_net::task_pool::TaskPool::current("raft-consensus-metrics").spawn(
                    async move {
                        run_consensus_metrics_poller(
                            metrics_raft,
                            metrics_cancel,
                            metrics_health,
                        )
                        .await;
                    },
                );
            }

            // Spawn the leader-side snapshot-push sweep (P0-20 fix, path b).
            //
            // When this node is the leader, the sweep periodically detects
            // followers whose matched log is far behind the committed index and
            // calls trigger().snapshot() + trigger().heartbeat() to push the
            // snapshot.  This closes the gap left by P0-17/P0-19: the election
            // guard suppresses the storm, but the leader still needs to be
            // nudged to actually deliver the snapshot.
            {
                use crate::raft::snapshot_pusher::run_snapshot_pusher;
                let pusher_raft = raft_arc.clone();
                let pusher_cancel = election_guard_cancel.clone();
                ferrosa_net::task_pool::TaskPool::current("raft-snapshot-pusher").spawn(async move {
                    run_snapshot_pusher(
                        pusher_raft,
                        pusher_cancel,
                        5_000, // sweep every 5 s
                        10,    // lag_threshold: 10 entries
                    )
                    .await;
                });
            }

            // Build initial membership: all known nodes including self.
            // OpenRaft commits this map durably; the local seed must not
            // author itself with an empty address.
            let members = build_initial_raft_members(
                local_host_id_for_refresh,
                local_raft_addr_for_init,
                &peers,
            );

            // Only the seed (original Primary) calls initialize().
            // Non-seed nodes will receive their membership via AppendEntries
            // from the leader. This prevents CF-T17 (membership race from
            // independent initialize() calls with potentially different member lists).
            let initialized_seed_membership = should_initialize_seed_membership(
                was_seed,
                has_recovered_membership,
                has_recovered_topology_state,
            );
            if initialized_seed_membership {
                if let Err(e) = raft_arc.initialize(members).await {
                    // Some failures are expected (already initialized after a
                    // restart-and-rejoin); others (e.g. APIError on a corrupt
                    // log) are real. Increment the counter unconditionally so
                    // the rate is visible in metrics; operators correlate
                    // with logs to distinguish benign from real.
                    RAFT_INITIALIZE_FAILURES.fetch_add(1, AtomicOrdering::Relaxed);
                    tracing::warn!(%e, "raft initialize returned error (may be already initialized)");
                }
            } else if was_seed {
                tracing::info!(
                    has_persisted_log = persisted_raft_state.0,
                    has_persisted_vote = persisted_raft_state.1,
                    has_recovered_membership,
                    has_recovered_topology_state,
                    "seed has persisted raft state; skipping initialize and waiting for election"
                );
            } else {
                tracing::info!("non-seed node — skipping raft.initialize(), waiting for leader AppendEntries");
            }

            // Wait for leader election.
            //
            // W1.17 — the deadline is driven by `formation_timeout_secs`
            // when configured (operator override) so a small test or
            // failure-mode harness can assert the Forming → Pair
            // fallback fires deterministically.  Default keeps the
            // historical ~30 s budget.
            let formation_deadline_secs = config_for_promotion
                .formation_timeout_secs
                .unwrap_or(30);
            let formation_deadline_secs = formation_deadline_secs.max(1);
            let election_start = tokio::time::Instant::now();
            let election_deadline = election_start
                + std::time::Duration::from_secs(formation_deadline_secs);
            let mut leader = None;
            let mut attempt: u32 = 0;
            loop {
                if let Some(lid) = raft_arc.current_leader().await {
                    leader = Some(lid);
                    break;
                }
                if tokio::time::Instant::now() >= election_deadline {
                    break;
                }
                let backoff =
                    std::time::Duration::from_millis(if attempt < 10 { 100 } else { 500 });
                tokio::time::sleep(backoff).await;
                attempt = attempt.saturating_add(1);
            }

            match leader {
                Some(lid) => {
                    tracing::info!(
                        leader = lid,
                        "raft leader elected, swapping DDL path to Cluster"
                    );
                    if has_recovered_topology_state {
                        let refresh_plan = build_recovered_topology_refresh_plan(
                            local_host_id_for_refresh,
                            local_addr_for_refresh.clone(),
                            local_cql_broadcast_for_refresh.clone(),
                            &config_for_promotion.data_center,
                            &config_for_promotion.rack,
                            &peers,
                            &peer_cql_broadcasts,
                            &peer_internode_broadcasts,
                        );
                        let desired_raft_members = build_raft_members_from_node_info(&refresh_plan);
                        let raft_for_membership_repair = raft_arc.clone();
                        ferrosa_net::task_pool::TaskPool::current("raft-membership-repair").spawn(async move {
                            let mut interval =
                                tokio::time::interval(std::time::Duration::from_secs(2));
                            let mut clean_observations = 0u8;
                            loop {
                                interval.tick().await;
                                let metrics = raft_for_membership_repair.metrics().borrow().clone();
                                if metrics.state != openraft::ServerState::Leader {
                                    clean_observations = 0;
                                    continue;
                                }
                                let current_members = metrics
                                    .membership_config
                                    .nodes()
                                    .map(|(node_id, node)| (*node_id, node.clone()))
                                    .collect();
                                if !membership_addresses_need_repair(
                                    &current_members,
                                    &desired_raft_members,
                                ) {
                                    clean_observations = clean_observations.saturating_add(1);
                                    if clean_observations >= 2 {
                                        tracing::info!(
                                            "OpenRaft membership node addresses match recovered topology"
                                        );
                                        return;
                                    }
                                    continue;
                                }
                                clean_observations = 0;
                                if let Err(e) = raft_for_membership_repair
                                    .change_membership(
                                        openraft::ChangeMembers::SetNodes(
                                            desired_raft_members.clone(),
                                        ),
                                        true,
                                    )
                                    .await
                                {
                                    tracing::warn!(
                                        %e,
                                        "leader failed to repair OpenRaft membership node addresses; will retry"
                                    );
                                } else {
                                    tracing::info!(
                                        "submitted OpenRaft membership node-address repair"
                                    );
                                }
                            }
                        });
                    }
                    if has_recovered_topology_state && lid == local_node_id {
                        let refresh_plan = build_recovered_topology_refresh_plan(
                            local_host_id_for_refresh,
                            local_addr_for_refresh.clone(),
                            local_cql_broadcast_for_refresh.clone(),
                            &config_for_promotion.data_center,
                            &config_for_promotion.rack,
                            &peers,
                            &peer_cql_broadcasts,
                            &peer_internode_broadcasts,
                        );
                        let token_repair_plan = build_recovered_topology_token_repair_plan(
                            &refresh_plan,
                            config_for_promotion.num_tokens as usize,
                        );
                        for node_info in refresh_plan {
                            if let Err(e) = raft_arc
                                .client_write(crate::raft::RaftCommand {
                                    op: crate::raft::RaftOp::UpdateNodeInfo(node_info.clone()),
                                    schema_version: Uuid::new_v4(),
                                })
                                .await
                            {
                                tracing::warn!(
                                    host_id = %node_info.host_id,
                                    leader = lid,
                                    %e,
                                    "leader failed to refresh recovered topology metadata"
                                );
                            }
                        }
                        for (node_id, tokens) in token_repair_plan {
                            if let Err(e) = raft_arc
                                .client_write(crate::raft::RaftCommand {
                                    op: crate::raft::RaftOp::AssignTokens { node_id, tokens },
                                    schema_version: Uuid::new_v4(),
                                })
                                .await
                            {
                                tracing::warn!(
                                    node_id,
                                    leader = lid,
                                    %e,
                                    "leader failed to refresh recovered topology token assignment"
                                );
                            }
                        }
                    }
                    if initialized_seed_membership {
                        // Author peer topology only AFTER a leader exists.
                        // Firing client_write() immediately after initialize()
                        // races with leader election and yields
                        // "has to forward request to: None, None" on the seed.
                        let seed_num_tokens = config_for_promotion.num_tokens as usize;

                        // Propose JoinNode + AssignTokens for the seed
                        // itself, so the seed's identity replicates to every
                        // follower's `state.members` / `state.token_map`.
                        // Without this, follower state machines only learn
                        // about *peers* and never about the seed — meaning
                        // their `system.peers` view reports zero tokens for
                        // the seed and writes coordinated through followers
                        // never land on the seed's token range.  Cassandra
                        // load tests then see roughly N-1 owners instead of
                        // N, and the "diabolical" single-owner pattern
                        // persists from the follower side.
                        let local_node_info = crate::raft::NodeInfo {
                            host_id: local_host_id_for_refresh,
                            addr: local_addr_for_refresh.clone(),
                            data_center: config_for_promotion.data_center.clone(),
                            rack: config_for_promotion.rack.clone(),
                            state: crate::raft::NodeState::Normal,
                            cql_broadcast: local_cql_broadcast_for_refresh.clone(),
                        };
                        let self_join_cmd = crate::raft::RaftCommand {
                            op: crate::raft::RaftOp::JoinNode(local_node_info),
                            schema_version: Uuid::new_v4(),
                        };
                        if let Err(e) = raft_arc.client_write(self_join_cmd).await {
                            tracing::warn!(
                                local = local_node_id,
                                leader = lid,
                                %e,
                                "seed: JoinNode for self failed after leader election"
                            );
                        }
                        let self_tokens = crate::controller::token::deterministic_tokens_for_node(
                            local_node_id,
                            seed_num_tokens,
                        );
                        let self_assign_cmd = crate::raft::RaftCommand {
                            op: crate::raft::RaftOp::AssignTokens {
                                node_id: local_node_id,
                                tokens: self_tokens,
                            },
                            schema_version: Uuid::new_v4(),
                        };
                        if let Err(e) = raft_arc.client_write(self_assign_cmd).await {
                            tracing::warn!(
                                local = local_node_id,
                                leader = lid,
                                %e,
                                "seed: AssignTokens for self failed after leader election"
                            );
                        }

                        for (peer_uuid, addr) in &peers {
                            let peer_node_id = uuid_to_node_id(*peer_uuid);
                            let peer_internode_broadcast =
                                peer_internode_broadcasts.get(peer_uuid).cloned().flatten();
                            let node_info = crate::raft::NodeInfo {
                                host_id: *peer_uuid,
                                addr: super::membership::node_info_addr(
                                    *addr,
                                    peer_internode_broadcast.as_deref(),
                                ),
                                data_center: config_for_promotion.data_center.clone(),
                                rack: config_for_promotion.rack.clone(),
                                state: crate::raft::NodeState::Normal,
                                cql_broadcast: peer_cql_broadcasts
                                    .get(peer_uuid)
                                    .cloned()
                                    .flatten(),
                            };
                            let join_cmd = crate::raft::RaftCommand {
                                op: crate::raft::RaftOp::JoinNode(node_info),
                                schema_version: Uuid::new_v4(),
                            };
                            if let Err(e) = raft_arc.client_write(join_cmd).await {
                                tracing::warn!(
                                    peer = %peer_uuid,
                                    leader = lid,
                                    %e,
                                    "seed: JoinNode for peer failed after leader election"
                                );
                            }
                            let peer_tokens = crate::controller::token::deterministic_tokens_for_node(
                                peer_node_id,
                                seed_num_tokens,
                            );
                            let assign_cmd = crate::raft::RaftCommand {
                                op: crate::raft::RaftOp::AssignTokens {
                                    node_id: peer_node_id,
                                    tokens: peer_tokens,
                                },
                                schema_version: Uuid::new_v4(),
                            };
                            if let Err(e) = raft_arc.client_write(assign_cmd).await {
                                tracing::warn!(
                                    peer = %peer_uuid,
                                    leader = lid,
                                    %e,
                                    "seed: AssignTokens for peer failed after leader election"
                                );
                            }
                        }
                        tracing::info!(
                            peer_count = peers.len(),
                            leader = lid,
                            "seed: submitted JoinNode + AssignTokens via Raft for every peer"
                        );
                    }
                    // Register the cluster DDL forward handler so that when a
                    // non-leader forwards a PairDdlForward to the leader, the
                    // leader proposes it through Raft rather than applying
                    // directly (which would bypass consensus).
                    let cluster_ddl_handler = Arc::new(ClusterDdlForwardHandler::new(
                        raft_arc.clone(),
                        peer_manager_for_ddl.clone(),
                        node_map_for_ddl.clone(),
                    ));
                    registry.register(MsgType::PairDdlForward, cluster_ddl_handler);

                    ddl_path.store(Arc::new(DdlPath::Cluster {
                        raft: raft_arc.clone(),
                        peer_manager: peer_manager_for_ddl,
                        node_map: node_map_for_ddl,
                    }));


                    // Drain any DDL operations queued during Forming state.
                    let maybe_rx = ddl_queue_rx.lock().take();
                    if let Some(rx) = maybe_rx {
                        let raft_for_drain = raft_arc.clone();
                        let replayed = drain_ddl_queue(rx, |op| {
                            let raft = raft_for_drain.clone();
                            async move { execute_via_raft(&raft, op).await.map(|_| ()) }
                        })
                        .await;
                        if replayed > 0 {
                            tracing::info!(
                                count = replayed,
                                "replayed queued DDL operations from Forming state",
                            );
                        }
                    }

                    // --- ReplaySchema phase: schema convergence (all nodes) ---
                    //
                    // Every node replays its local schema so that all peers
                    // learn about user-created keyspaces/tables. The leader
                    // proposes directly via Raft; non-leaders forward to the
                    // leader via the existing PairDdlForward RPC.
                    {
                        let schema_snap = schema_for_replay.snapshot();
                        // See `keyspace_needs_cluster_replay` for why we
                        // use exact-match `is_system_keyspace` here
                        // instead of a `starts_with("system")` prefix
                        // check: `system_graph_<user_ks>` keyspaces are
                        // user-data-derived and MUST replicate.
                        let user_ks: Vec<_> = schema_snap
                            .keyspaces
                            .iter()
                            .filter(|(name, _)| keyspace_needs_cluster_replay(name))
                            .collect();
                        let user_tables: Vec<_> = schema_snap
                            .tables
                            .iter()
                            .filter(|((ks, _), _)| keyspace_needs_cluster_replay(ks))
                            .collect();

                        if !user_ks.is_empty() || !user_tables.is_empty() {
                            if lid == local_node_id {
                                tracing::info!(
                                    ks_count = user_ks.len(),
                                    table_count = user_tables.len(),
                                    "leader: replaying local schema through Raft"
                                );
                                for (name, ks) in &user_ks {
                                    let op = DdlOperation::CreateKeyspace((*ks).clone());
                                    if let Err(e) = execute_via_raft(&raft_arc, op).await {
                                        tracing::warn!(%e, ks = %name, "schema replay: CreateKeyspace failed (may already exist)");
                                    }
                                }
                                for ((ks, _tbl), table) in &user_tables {
                                    let op = DdlOperation::CreateTable(Box::new((*table).clone()));
                                    if let Err(e) = execute_via_raft(&raft_arc, op).await {
                                        tracing::warn!(%e, ks, "schema replay: CreateTable failed (may already exist)");
                                    }
                                }
                            } else {
                                // Resolve leader NodeId → Uuid for RPC.
                                let leader_uuid = {
                                    let map = node_map_for_bootstrap.read().unwrap_or_else(|e| e.into_inner());
                                    map.get(&lid).copied()
                                };
                                if let Some(leader_uuid) = leader_uuid {
                                    tracing::info!(
                                        ks_count = user_ks.len(),
                                        table_count = user_tables.len(),
                                        "non-leader: forwarding local schema to leader"
                                    );
                                    for (name, ks) in &user_ks {
                                        let op = DdlOperation::CreateKeyspace((*ks).clone());
                                        // Bootstrap schema hand-off, not a client
                                        // DDL — no read-your-writes wait needed.
                                        if let Err(e) = crate::ddl_path::forward_ddl_to_leader(
                                            None,
                                            &peer_manager_for_bootstrap,
                                            leader_uuid,
                                            op,
                                        )
                                        .await
                                        {
                                            tracing::warn!(%e, ks = %name, "schema forward: CreateKeyspace failed");
                                        }
                                    }
                                    for ((ks, _tbl), table) in &user_tables {
                                        let op = DdlOperation::CreateTable(Box::new((*table).clone()));
                                        // Bootstrap schema hand-off, not a client
                                        // DDL — no read-your-writes wait needed.
                                        if let Err(e) = crate::ddl_path::forward_ddl_to_leader(
                                            None,
                                            &peer_manager_for_bootstrap,
                                            leader_uuid,
                                            op,
                                        )
                                        .await
                                        {
                                            tracing::warn!(%e, ks, "schema forward: CreateTable failed");
                                        }
                                    }
                                } else {
                                    tracing::warn!("cannot forward schema: leader UUID not in node_map");
                                }
                            }
                        }
                    }

                    if should_run_bootstrap_streaming(has_recovered_topology_state) {
                        // --- BootstrapStream phase: data streaming (all nodes) ---
                        //
                        // Every node reads from its local storage and streams
                        // partitions that belong to other nodes per the new ring.
                        // Nodes with no data complete instantly (zero iterations).
                        //
                        // Skip this on restart when topology was already recovered:
                        // the cluster is reforming, not adding new token owners.
                        tracing::info!("starting bootstrap streaming to new token owners");


                        if let Some(ring) = &**ring_for_bootstrap.load() {
                            let schema_snap = schema_for_bootstrap.snapshot();
                            let config = StreamConfig::default();
                            let node_map = node_map_for_bootstrap.read().unwrap_or_else(|e| e.into_inner()).clone();
                            let mut session_counter = 0_u64;

                            for (ks, tbl) in schema_snap.tables.keys() {
                                if ks.starts_with("system") {
                                    continue;
                                }
                                let table_id = ferrosa_storage::commitlog::TableId::new(ks, tbl);
                                // Prefer SSTable file streaming before any row-range scan.
                                // `StorageEngine::read_range` materializes all partitions from
                                // all SSTables before applying its limit; on large recovered
                                // tables that can OOM the joining/restarting node before the
                                // existing bulk path has a chance to run.
                                if let Err(e) = storage_for_bootstrap.flush_all() {
                                    tracing::warn!(%e, ks, tbl, "bootstrap: flush before SSTable stream failed; skipping row fallback to avoid unbounded materialization");
                                    continue;
                                }

                                let sstable_base = storage_for_bootstrap
                                    .data_dir()
                                    .join("sstables")
                                    .join(table_id.to_string());

                                let sstable_dirs: Vec<std::path::PathBuf> = match std::fs::read_dir(&sstable_base) {
                                    Ok(entries) => entries
                                        .filter_map(|e| e.ok())
                                        .filter(|e| e.path().is_dir())
                                        .map(|e| e.path())
                                        .collect(),
                                    Err(_) => {
                                        tracing::debug!(ks, tbl, "bootstrap: no SSTable dir at {}, using empty/small row path", sstable_base.display());
                                        vec![]
                                    }
                                };

                                let stream_plan = super::bootstrap::bootstrap_stream::plan_table_stream(
                                    super::bootstrap::bootstrap_stream::TableStreamPlanInput {
                                        sstable_dir_count: sstable_dirs.len(),
                                        row_fallback_limit: super::bootstrap::bootstrap_stream::BOUNDED_ROW_FALLBACK_LIMIT,
                                    },
                                );
                                if let super::bootstrap::bootstrap_stream::TableStreamPlan::SstableBulk { .. } = stream_plan {
                                    tracing::info!(
                                        ks,
                                        tbl,
                                        sstables = sstable_dirs.len(),
                                        "bootstrap: using SSTable bulk transfer before row materialization"
                                    );

                                    let mut sstable_streamed = false;
                                    for sstable_dir in &sstable_dirs {
                                        let sstable_id = sstable_dir
                                            .file_name()
                                            .map(|n| n.to_string_lossy().to_string())
                                            .unwrap_or_else(|| "unknown".to_string());

                                        for (&target_node_id, &target_uuid) in &node_map {
                                            if target_node_id == local_node_id {
                                                continue;
                                            }
                                            session_counter += 1;
                                            let request = SstableSendRequest {
                                                sstable_dir,
                                                keyspace: ks,
                                                table: tbl,
                                                sstable_id: &sstable_id,
                                                session_id: session_counter,
                                                source_node: local_node_id,
                                                chunk_size: config.chunk_size_bytes,
                                            };
                                            match StreamSender::send_sstable_files(
                                                &request,
                                                &peer_manager_for_bootstrap,
                                                target_uuid,
                                            )
                                            .await
                                            {
                                                Ok(bytes) => {
                                                    tracing::info!(
                                                        target = target_node_id,
                                                        bytes,
                                                        sstable_id = %sstable_id,
                                                        "bootstrap: SSTable streamed {ks}.{tbl}"
                                                    );
                                                    sstable_streamed = true;
                                                }
                                                Err(e) => {
                                                    tracing::warn!(
                                                        %e,
                                                        target = target_node_id,
                                                        sstable_id = %sstable_id,
                                                        "bootstrap: SSTable stream failed for {ks}.{tbl}; not falling back to unbounded row materialization"
                                                    );
                                                }
                                            }
                                        }
                                    }

                                    if !sstable_streamed {
                                        tracing::warn!(
                                            ks,
                                            tbl,
                                            sstables = sstable_dirs.len(),
                                            "bootstrap: all SSTable streams failed; leaving table for retry/repair instead of row materialization"
                                        );
                                    }
                                    continue;
                                }

                                let row_fallback_limit = match stream_plan {
                                    super::bootstrap::bootstrap_stream::TableStreamPlan::BoundedRows { limit } => limit,
                                    super::bootstrap::bootstrap_stream::TableStreamPlan::RetryRequired => {
                                        tracing::warn!(
                                            ks,
                                            tbl,
                                            "bootstrap: table stream requires retry/repair; skipping row materialization"
                                        );
                                        continue;
                                    }
                                    super::bootstrap::bootstrap_stream::TableStreamPlan::SstableBulk { .. } => unreachable!(
                                        "SSTable-backed bootstrap tables return above and never row-materialize"
                                    ),
                                };
                                let partitions = match storage_for_bootstrap.read_range(
                                    &table_id,
                                    None,
                                    None,
                                    row_fallback_limit,
                                ) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        tracing::warn!(%e, ks, tbl, "bootstrap: failed to read small in-memory table");
                                        continue;
                                    }
                                };

                                let mut by_node: std::collections::HashMap<u64, Vec<StreamedMutation>> =
                                    std::collections::HashMap::new();
                                for partition in &partitions {
                                    let token = partition.key.token.0;
                                    let owner = ring.primary_owner(token).unwrap_or(local_node_id);

                                    if owner != local_node_id {
                                        use crate::raft::handlers::RowWire;
                                        let wire_rows: Vec<RowWire> = partition.rows
                                            .iter()
                                            .cloned()
                                            .map(RowWire::from)
                                            .collect();
                                        let row_bytes = match bincode::serialize(&wire_rows) {
                                            Ok(bytes) => bytes,
                                            Err(e) => {
                                                tracing::error!(
                                                    %e,
                                                    partition_key = ?partition.key,
                                                    "bootstrap: failed to serialize rows, skipping partition (data loss avoided)"
                                                );
                                                continue;
                                            }
                                        };
                                        let ts = partition.rows.first()
                                            .and_then(|r| r.cells.first())
                                            .map(|(_, cv)| cv.timestamp)
                                            .unwrap_or(0);

                                        by_node.entry(owner).or_default().push(StreamedMutation {
                                            keyspace: ks.clone(),
                                            table: tbl.clone(),
                                            key: partition.key.key.as_bytes().to_vec(),
                                            row: row_bytes,
                                            timestamp: ts,
                                        });
                                    }
                                }

                                for (target_node_id, mutations) in by_node {
                                    let target_uuid = node_map.get(&target_node_id).copied();

                                    if let Some(uuid) = target_uuid {
                                        session_counter += 1;
                                        let count = mutations.len();
                                        if let Err(e) = StreamSender::send_stream(
                                            mutations,
                                            &peer_manager_for_bootstrap,
                                            uuid,
                                            session_counter,
                                            (i64::MIN, i64::MAX),
                                            local_node_id,
                                            &config,
                                        )
                                        .await
                                        {
                                            tracing::warn!(
                                                %e,
                                                target = target_node_id,
                                                "bootstrap streaming failed for {ks}.{tbl}"
                                            );
                                        } else {
                                            tracing::info!(
                                                target = target_node_id,
                                                count,
                                                "bootstrapped {ks}.{tbl} to node {target_node_id}"
                                            );
                                        }
                                    }
                                }
                            }
                            tracing::info!("bootstrap streaming complete on this node");
                        }

                        if lid != local_node_id {
                            let leader_uuid = {
                                let map = node_map_for_bootstrap.read().unwrap_or_else(|e| e.into_inner());
                                map.get(&lid).copied()
                            };
                            if let Some(leader_uuid) = leader_uuid {
                                let msg = Message::BootstrapComplete {
                                    node_id: local_id,
                                };
                                if let Err(e) = peer_manager_for_bootstrap
                                    .send(leader_uuid, msg, Lane::Data)
                                    .await
                                {
                                    tracing::warn!(%e, "failed to send BootstrapComplete to leader");
                                } else {
                                    tracing::info!("sent BootstrapComplete to leader");
                                }
                            }
                        }

                        if lid == local_node_id {
                            let promotion_timeout = config_for_promotion
                                .formation_timeout_secs
                                .map(|s| s / 3)
                                .unwrap_or(20);

                            let expected_count = all_node_ids_for_bootstrap
                                .iter()
                                .filter(|&&nid| nid != local_node_id)
                                .count();
                            let mut received_count = 0usize;
                            let deadline = tokio::time::Instant::now()
                                + std::time::Duration::from_secs(promotion_timeout);

                            tracing::info!(
                                expected = expected_count,
                                timeout_secs = promotion_timeout,
                                "leader waiting for BootstrapComplete from joining nodes"
                            );

                            while received_count < expected_count {
                                if tokio::time::Instant::now() >= deadline {
                                    tracing::warn!(
                                        received = received_count,
                                        expected = expected_count,
                                        "promotion timeout — proceeding with available nodes"
                                    );
                                    break;
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                received_count = bootstrap_complete_counter
                                    .load(std::sync::atomic::Ordering::Relaxed);
                            }

                            tracing::info!(
                                received = received_count,
                                "proceeding to promote joining nodes"
                            );
                            for &nid in &all_node_ids_for_bootstrap {
                                if nid != local_node_id {
                                    let cmd = crate::raft::RaftCommand {
                                        op: crate::raft::RaftOp::SetNodeState {
                                            node_id: nid,
                                            state: NodeState::Normal,
                                        },
                                        schema_version: Uuid::new_v4(),
                                    };
                                    if let Err(e) = raft_arc.client_write(cmd).await {
                                        tracing::warn!(
                                            node_id = nid,
                                            %e,
                                            "failed to promote node to Normal"
                                        );
                                    }
                                }
                            }
                            tracing::info!("bootstrap complete — all nodes promoted to Normal");
                        }
                    } else {
                        tracing::info!(
                            "raft recovered committed topology; skipping bootstrap streaming and promotion"
                        );
                    }
                }
                None => {
                    // Election timeout. Previously logged at warn and silently
                    // reverted to Pair mode — operators had no metric to alert
                    // on. Now: count the timeout (silent-failure detector) and
                    // surface at error level. The fallback to Pair is still
                    // the right behavior so the node keeps serving local data,
                    // but operators must be notified that the cluster did not
                    // form.
                    LEADER_ELECTION_TIMEOUTS.fetch_add(1, AtomicOrdering::Relaxed);
                    tracing::error!(
                        peers = peers.len(),
                        deadline_secs = formation_deadline_secs,
                        "raft leader election timed out — reverting to Pair mode \
                         (this is a fail-loud signal: cluster formation did not complete)"
                    );
                    // Revert to Pair mode — formation failed. The Raft instance
                    // is stored but non-functional (no leader).
                    mode_swap.store(Arc::new(DeploymentMode::Pair));
                    // Restore DDL path from Blocked to Direct (single-node fallback).
                    // Without this, DDL stays blocked indefinitely after failed formation.
                    ddl_path.store(Arc::new(DdlPath::Direct {
                        schema: schema_for_replay.clone(),
                        engine: storage_for_bootstrap.clone(),
                    }));
                    tracing::info!("DDL path restored to Direct after formation timeout");

                    // P0-21 FIX: Spawn a background rejoin task that contacts
                    // the existing cluster's leader and asks it to add this node
                    // as a voter via JoinNode.  This closes the gap where the
                    // node is stuck in election-storm limbo with no path to
                    // converge (P0-17/P0-19 suppress elections, P0-20 pusher
                    // stays silent because this node isn't in the voter set).
                    {
                        use crate::controller::cluster_rejoin::attempt_rejoin;
                        let rejoin_self_id = local_host_id_for_refresh;
                        let rejoin_self_addr = local_addr_for_refresh.clone();
                        let rejoin_dc = config_for_promotion.data_center.clone();
                        let rejoin_rack = config_for_promotion.rack.clone();
                        let rejoin_cql_broadcast = local_cql_broadcast_for_refresh.clone();
                        let rejoin_peers = peers.clone();
                        let rejoin_pm = peer_manager_for_bootstrap.clone();
                        ferrosa_net::task_pool::TaskPool::current("cluster-rejoin").spawn(async move {
                            tracing::info!(
                                self_id = %rejoin_self_id,
                                peer_count = rejoin_peers.len(),
                                "cluster_rejoin: formation timed out — attempting to add self \
                                 to existing cluster voter set (P0-21)"
                            );
                            match attempt_rejoin(
                                rejoin_self_id,
                                rejoin_self_addr,
                                rejoin_dc,
                                rejoin_rack,
                                rejoin_cql_broadcast,
                                rejoin_peers,
                                rejoin_pm,
                            )
                            .await
                            {
                                Ok(()) => {
                                    tracing::info!(
                                        self_id = %rejoin_self_id,
                                        "cluster_rejoin: JoinNode accepted by leader — \
                                         awaiting snapshot + replication convergence"
                                    );
                                }
                                Err(e) => {
                                    tracing::error!(
                                        self_id = %rejoin_self_id,
                                        error = %e,
                                        "cluster_rejoin: EXHAUSTED — node is NOT a voter. \
                                         Operator intervention required. \
                                         (CLUSTER_REJOIN_FAILURES_TOTAL incremented, P0-21)"
                                    );
                                }
                            }
                        });
                    }
                }
            }

            // Store the raft instance so it is accessible via controller.raft()
            // (ADR-015: keyed by per-DC RaftGroupId).
            {
                let prev = raft_groups_swap.load_full();
                let mut next: std::collections::HashMap<
                    crate::raft::RaftGroupId,
                    Arc<FerrosRaft>,
                > = (*prev).clone();
                next.insert(local_raft_group_id, raft_arc);
                raft_groups_swap.store(Arc::new(next));
            }
        });
    }
}

// ---------------------------------------------------------------------------
// ClusterInviteHandler — connects to discovered peers on invite receipt
// ---------------------------------------------------------------------------

/// RPC handler for `ClusterInvite` messages.
///
/// When a node receives a `ClusterInvite`, it:
/// 1. Connects to any peers listed in the invite that it doesn't already know.
/// 2. Re-broadcasts the invite to those newly connected peers.
/// 3. Replies with `ClusterInviteAck`.
pub struct ClusterInviteHandler {
    local_host_id: Uuid,
    peer_manager: Arc<PeerManager>,
    net_config: Arc<NetConfig>,
    /// Weak reference to the ModeController for triggering cluster transition
    /// when this node receives a ClusterInvite while in Pair mode.
    controller: std::sync::Weak<ModeController>,
}

impl ClusterInviteHandler {
    pub fn new(
        local_host_id: Uuid,
        peer_manager: Arc<PeerManager>,
        net_config: Arc<NetConfig>,
        controller: std::sync::Weak<ModeController>,
    ) -> Self {
        Self {
            local_host_id,
            peer_manager,
            net_config,
            controller,
        }
    }
}

#[async_trait::async_trait]
impl RpcHandler for ClusterInviteHandler {
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        let (initiator, peers) = match msg {
            Message::ClusterInvite { initiator, peers } => (initiator, peers),
            _ => return None,
        };

        tracing::info!(
            %initiator,
            peer_count = peers.len(),
            "received ClusterInvite"
        );

        // Find peers we don't already know about, plus peers whose
        // address has changed since we last connected. The address-
        // change case is critical for recovery: when a node is recreated
        // (eg `podman compose up -d` after a stop+rm), it comes up with
        // a fresh container IP. The old peer entry in peer_manager
        // points at the dead address and `has_live_peer` returns true
        // (the entry exists, even if the underlying TCP pool is broken).
        // Without refreshing on address change, every peer keeps trying
        // to reach the recreated node at its dead address forever
        // ("No route to host"), Raft replication times out, and reads
        // at LOCAL_QUORUM fail.
        let internode_port = self.net_config.bind_addr.port();
        let mut new_peers = Vec::new();
        for (peer_id, peer_addr) in &peers {
            let known_addr = self.peer_manager.peer_addr(*peer_id).await;
            let live = self.peer_manager.has_live_peer(*peer_id);
            match plan_invite_peer_connection(
                self.local_host_id,
                *peer_id,
                *peer_addr,
                internode_port,
                known_addr.as_deref(),
                live,
            ) {
                InvitePeerConnectionPlan::SkipSelf | InvitePeerConnectionPlan::AlreadyConnected => {
                }
                InvitePeerConnectionPlan::KeepLiveKnownPeer {
                    known_addr,
                    invite_addr,
                } => {
                    tracing::warn!(
                        peer = %peer_id,
                        %known_addr,
                        %invite_addr,
                        "cluster invite: ignoring conflicting address for live peer"
                    );
                }
                InvitePeerConnectionPlan::Connect {
                    reverse_addr,
                    previous_addr,
                } => {
                    if previous_addr.as_deref() != Some(&reverse_addr.to_string()) {
                        tracing::info!(
                            peer = %peer_id,
                            old_addr = ?previous_addr,
                            new_addr = %reverse_addr,
                            "cluster invite: peer address changed, refreshing reverse connection"
                        );
                    }
                    new_peers.push((*peer_id, reverse_addr));
                }
            }
        }

        // Connect to unknown peers using a local JoinSet so we can await
        // completion before re-broadcasting (replaces raw tokio::spawn +
        // fixed 500ms sleep).
        let mut connect_tasks = tokio::task::JoinSet::new();
        let raft_rt_for_connect = self.peer_manager.raft_runtime();
        let data_rt_for_connect = self.peer_manager.data_runtime();
        // Resolve the controller once for the per-peer connect cooldown below.
        // A live upgrade is the normal case; if it fails the node is shutting
        // down (no further invite rounds, so no storm to guard against).
        let connect_cooldown_ctrl = self.controller.upgrade();
        for (peer_id, reverse_addr) in &new_peers {
            // Rate-limit connect attempts per discovered peer. A peer that can
            // never be reached (stale host_id, dead listener) would otherwise
            // be re-dialed on every invite round, and the re-broadcast below
            // keeps the rounds coming — a self-amplifying connection storm that
            // exhausts the local ephemeral port range and wedges all outbound
            // networking. The cooldown caps it to one attempt per peer per
            // CLUSTER_RECONNECT_INVITE_COOLDOWN, mirroring invite delivery.
            if let Some(ctrl) = connect_cooldown_ctrl.as_ref() {
                let mut recent = ctrl.recent_invite_connects.lock();
                if !super::invite::reserve_reconnect_invite(
                    &mut recent,
                    *peer_id,
                    std::time::Instant::now(),
                    super::CLUSTER_RECONNECT_INVITE_COOLDOWN,
                    super::MAX_CONNECTED_PEERS,
                ) {
                    tracing::debug!(
                        peer = %peer_id,
                        "cluster invite: connect to discovered peer suppressed by cooldown"
                    );
                    continue;
                }
            }

            let pm = self.peer_manager.clone();
            let cfg = self.net_config.clone();
            let local_id = self.local_host_id;
            let uuid = *peer_id;
            let addr = *reverse_addr;
            let raft_rt = raft_rt_for_connect.clone();
            let data_rt = data_rt_for_connect.clone();

            connect_tasks.spawn(async move {
                match PriorityPool::connect(cfg, local_id, &addr.to_string(), raft_rt, data_rt)
                    .await
                {
                    Ok(pool) => {
                        pm.add_peer((uuid, addr), pool).await;
                        tracing::info!(
                            %uuid,
                            %addr,
                            "cluster invite: connected to discovered peer"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            %uuid,
                            %e,
                            "cluster invite: failed to connect to discovered peer"
                        );
                    }
                }
            });
        }

        // Wait for all connection tasks to complete, then re-broadcast
        // immediately. This replaces the previous raw tokio::spawn + 500ms
        // sleep, providing both panic visibility and faster re-broadcast.
        if !new_peers.is_empty() {
            let pm = self.peer_manager.clone();
            let invite = Message::ClusterInvite {
                initiator,
                peers: peers.clone(),
            };

            // Drain the JoinSet — log any panicked tasks.
            while let Some(result) = connect_tasks.join_next().await {
                if let Err(e) = result {
                    tracing::error!("invite handler connection task panicked: {e}");
                }
            }

            // Re-broadcast immediately now that connections are established.
            for (peer_id, _) in &new_peers {
                if let Err(e) = pm.fire(*peer_id, invite.clone(), Lane::Data).await {
                    tracing::debug!(
                        peer = %peer_id,
                        %e,
                        "cluster invite: re-broadcast failed (peer may not be connected yet)"
                    );
                }
            }
        }

        // If this node is in Pair mode, the invite signals that a 3rd node
        // has joined and we should transition to cluster mode. Without this,
        // only the node that saw the 3rd peer connection transitions — the
        // other nodes stay in Pair forever and never register Raft handlers.
        if let Some(ctrl) = self.controller.upgrade() {
            let mode = ctrl.mode();
            if mode == DeploymentMode::Pair || mode == DeploymentMode::Standalone {
                // Include the initiator in the peer list so that all nodes
                // have a complete view of cluster membership. Without this,
                // a node receiving the invite would only see a subset of
                // peers and might incorrectly determine the seed (RC-3 race).
                let mut all_peers: Vec<(Uuid, std::net::SocketAddr)> = peers
                    .iter()
                    .filter(|(id, _)| *id != self.local_host_id)
                    .cloned()
                    .collect();
                // Add the initiator if not already in the list. Use the
                // sender's address from the RPC handler (from.1).
                if initiator != self.local_host_id
                    && !all_peers.iter().any(|(id, _)| *id == initiator)
                {
                    all_peers.push((initiator, from.1));
                }
                if all_peers.len() >= 2 {
                    tracing::info!(
                        peer_count = all_peers.len(),
                        "cluster invite: triggering cluster transition from {mode:?}"
                    );
                    ctrl.transition_to_cluster(all_peers);
                }
            }
        }

        Some(Message::ClusterInviteAck {
            host_id: self.local_host_id,
        })
    }
}
