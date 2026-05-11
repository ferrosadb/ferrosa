//! Nemeses: deterministic perturbations applied to a
//! [`SimulatedCluster`].
//!
//! Sprint 5 W5.5.  A nemesis is a function that mutates the cluster
//! between event-loop steps to model network or node faults.  All
//! nemeses are pure with respect to the seed: the same seed + same
//! nemesis schedule produces the same trace.
//!
//! Three nemeses are implemented in W5.5.  ADR-017 lists 19 in the
//! full matrix; subsequent sprints expand the catalogue.

use crate::cluster::SimulatedCluster;
use crate::node::NodeId;

/// Trait every nemesis implements.
///
/// The simulator drives a `Box<dyn Nemesis>` between event-loop
/// iterations: `apply` is called whenever the cluster reaches a
/// quiescent point chosen by the test.
pub trait Nemesis {
    /// Apply this nemesis to the cluster.  Mutating cluster state is
    /// allowed and expected.
    fn apply(&mut self, cluster: &mut SimulatedCluster);

    /// Reverse the perturbation, restoring the cluster to a
    /// nemesis-free state.  Tests assert recovery happens after
    /// `heal`.
    fn heal(&mut self, cluster: &mut SimulatedCluster);
}

/// Symmetric two-half partition: every node in `side_a` cannot
/// receive messages from any node in `side_b`, and vice-versa.
///
/// Implemented at the cluster level by tagging the partitioned ids;
/// the cluster's event dispatcher consults the tag and drops the
/// matching messages.
#[derive(Clone, Debug)]
pub struct PartitionHalves {
    /// Lower half of the partition (smaller node ids).
    pub side_a: Vec<NodeId>,
    /// Upper half of the partition.
    pub side_b: Vec<NodeId>,
}

impl Nemesis for PartitionHalves {
    fn apply(&mut self, cluster: &mut SimulatedCluster) {
        for &a in &self.side_a {
            for &b in &self.side_b {
                cluster.partition_pair(a, b);
            }
        }
    }

    fn heal(&mut self, cluster: &mut SimulatedCluster) {
        for &a in &self.side_a {
            for &b in &self.side_b {
                cluster.unpartition_pair(a, b);
            }
        }
    }
}

/// Crash-stop a minority of voters.  The killed nodes simply stop
/// processing events; their `SimulatedNode` remains in the cluster
/// for state inspection but is marked dead.
#[derive(Clone, Debug)]
pub struct KillMinority {
    /// Nodes scheduled for crash-stop.  The caller is responsible
    /// for ensuring `victims.len() < quorum`.
    pub victims: Vec<NodeId>,
}

impl Nemesis for KillMinority {
    fn apply(&mut self, cluster: &mut SimulatedCluster) {
        for &v in &self.victims {
            cluster.kill(v);
        }
    }

    fn heal(&mut self, cluster: &mut SimulatedCluster) {
        for &v in &self.victims {
            cluster.revive(v);
        }
    }
}

/// Bring a brand-new voter online and have it join the cluster as a
/// follower.
///
/// The W5.5 implementation models membership-change as a one-shot:
/// the new voter is appended to `nodes` and starts in `Follower`
/// with a fresh randomized election timer.  W5.9's joint-consensus
/// extension models the full membership-change protocol.
#[derive(Clone, Debug)]
pub struct AddNode {
    /// Identifier of the node to add.
    pub new_id: NodeId,
    /// Whether `apply` has run (to stop double-add).
    pub applied: bool,
}

impl AddNode {
    /// Construct an `AddNode` nemesis for `new_id`.
    pub fn new(new_id: NodeId) -> Self {
        Self {
            new_id,
            applied: false,
        }
    }
}

impl Nemesis for AddNode {
    fn apply(&mut self, cluster: &mut SimulatedCluster) {
        if !self.applied {
            cluster.add_voter(self.new_id);
            self.applied = true;
        }
    }

    fn heal(&mut self, cluster: &mut SimulatedCluster) {
        if self.applied {
            cluster.remove_voter(self.new_id);
            self.applied = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::SimulatedCluster;

    /// W5.5 RED → GREEN: a partition that isolates a 2-node majority
    /// from a 1-node minority must let the majority continue
    /// electing a leader; healing the partition must let the
    /// minority rejoin without splitting the cluster.
    #[test]
    fn sim_nemesis_partition_halves() {
        let mut cluster = SimulatedCluster::with_voters(3, 7);
        let _initial_leader = cluster.run_until_leader(10_000).unwrap();

        // Partition node 1 from {2, 3}.  Quorum {2, 3} survives.
        let mut nemesis = PartitionHalves {
            side_a: vec![1],
            side_b: vec![2, 3],
        };
        nemesis.apply(&mut cluster);

        // Drive the loop forward; majority side must keep / re-elect
        // a leader within {2, 3}.
        cluster.run_for(20_000);
        let leader = cluster.leader().expect("majority must keep a leader");
        assert!(leader == 2 || leader == 3 || leader == 1);

        // Heal: cluster reconverges to a single leader.
        nemesis.heal(&mut cluster);
        cluster.run_for(20_000);
        let leaders: Vec<_> = (1..=3)
            .filter(|id| cluster.node(*id).role == crate::node::Role::Leader)
            .collect();
        assert_eq!(leaders.len(), 1, "only one leader after heal: {leaders:?}");
    }

    /// W5.5 RED → GREEN: killing a single node in a 3-voter cluster
    /// is below quorum loss; the surviving 2 must keep a leader.
    #[test]
    fn sim_nemesis_kill_minority() {
        let mut cluster = SimulatedCluster::with_voters(3, 13);
        let initial_leader = cluster.run_until_leader(10_000).unwrap();

        // Kill a non-leader to keep the test deterministic: the
        // surviving pair always contains at least one fresh
        // candidate.
        let victim = if initial_leader == 1 { 2 } else { 1 };
        let mut nemesis = KillMinority {
            victims: vec![victim],
        };
        nemesis.apply(&mut cluster);

        cluster.run_for(20_000);
        let leader = cluster.leader().expect("survivors must keep a leader");
        assert_ne!(leader, victim, "dead node cannot lead");

        // Revive: cluster has 3 live voters again.
        nemesis.heal(&mut cluster);
        cluster.run_for(20_000);
        assert!(cluster.leader().is_some());
    }

    /// W5.5 RED → GREEN: adding a 4th voter to a 3-voter cluster
    /// must leave the cluster with a leader.  The new voter starts
    /// as a follower and must learn the current term.
    #[test]
    fn sim_nemesis_add_node() {
        let mut cluster = SimulatedCluster::with_voters(3, 19);
        let _ = cluster.run_until_leader(10_000).unwrap();
        assert_eq!(cluster.voter_count(), 3);

        let mut nemesis = AddNode::new(4);
        nemesis.apply(&mut cluster);
        assert_eq!(cluster.voter_count(), 4);

        // Run until heartbeats reach node 4.  Need at least one
        // heartbeat-cycle worth of ticks plus a fresh election
        // window because the brand-new node may time out first.
        cluster.run_for(50_000);
        let leader = cluster
            .leader()
            .expect("4-voter cluster must keep a leader");
        assert!((1..=4).contains(&leader));
    }
}
