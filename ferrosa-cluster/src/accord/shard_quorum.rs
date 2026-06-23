//! Per-shard quorum tracking for multi-key Accord transactions (Phase 2).
//!
//! A multi-key transaction's write-set spans one or more **shards** — each a
//! token-range replica group (`ring.replicas(token, rf)`). Commit and Apply are
//! durable only when a slow quorum is reached **within every shard
//! independently**. A single global ack counter (the single-key path's
//! `commit_acks`/`apply_acks`) is *wrong* for multi-key: a transaction that
//! collected a full quorum from shard A and a single ack from shard B has NOT
//! committed shard B, even though the *total* ack count may exceed a global
//! quorum threshold. That would let one leg of a cross-shard transfer persist
//! while the other is lost — the exact non-atomicity Accord exists to prevent.
//!
//! [`ShardQuorum`] records acks per shard (deduplicated by node) and reports
//! success only when **each** shard has independently reached
//! `slow_quorum_size(shard_rf)`.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use super::coordinator::slow_quorum_size;
use super::ShardId;

/// Acks collected for one shard's replica group.
#[derive(Debug, Clone)]
struct ShardAcks {
    /// The shard's replica node ids (its replication factor is `replicas.len()`).
    replicas: HashSet<Uuid>,
    /// Distinct replica nodes that have acked so far.
    acked: HashSet<Uuid>,
}

/// Tracks per-shard commit/apply acknowledgements for a multi-key transaction.
#[derive(Debug, Clone)]
pub struct ShardQuorum {
    shards: HashMap<ShardId, ShardAcks>,
}

impl ShardQuorum {
    /// Build a tracker from the participant set: `shard_id -> replica node ids`.
    ///
    /// # Panics
    /// Panics if `participants` is empty or any shard has no replicas — a
    /// transaction with no shards/replicas can never reach quorum and signals a
    /// caller bug rather than a runtime condition.
    pub fn new(participants: &HashMap<ShardId, Vec<Uuid>>) -> Self {
        assert!(
            !participants.is_empty(),
            "ShardQuorum requires at least one shard"
        );
        let shards = participants
            .iter()
            .map(|(&shard, replicas)| {
                assert!(
                    !replicas.is_empty(),
                    "shard {shard} must have at least one replica"
                );
                (
                    shard,
                    ShardAcks {
                        replicas: replicas.iter().copied().collect(),
                        acked: HashSet::new(),
                    },
                )
            })
            .collect();
        Self { shards }
    }

    /// Record an ack from `node`. The ack counts toward **every** shard whose
    /// replica group contains `node` (a node may replicate multiple shards).
    /// Idempotent per (shard, node): re-acking is a no-op.
    pub fn record_node_ack(&mut self, node: Uuid) {
        for acks in self.shards.values_mut() {
            if acks.replicas.contains(&node) {
                acks.acked.insert(node);
            }
        }
    }

    /// True iff **every** shard has independently reached its slow quorum
    /// (`slow_quorum_size(shard_rf)`).
    pub fn all_reached(&self) -> bool {
        self.shards.values().all(|acks| {
            let rf = acks.replicas.len();
            acks.acked.len() >= slow_quorum_size(rf)
        })
    }

    /// The shards that have **not** yet reached quorum (for fail-loud
    /// diagnostics). Empty iff [`Self::all_reached`] is true.
    pub fn unmet(&self) -> Vec<ShardId> {
        let mut out: Vec<ShardId> = self
            .shards
            .iter()
            .filter(|(_, acks)| acks.acked.len() < slow_quorum_size(acks.replicas.len()))
            .map(|(&sid, _)| sid)
            .collect();
        out.sort_unstable();
        out
    }
}

/// The per-shard participant set for a multi-key transaction.
///
/// Built by grouping the write-set's keys by their token-range replica set
/// (`ring.replicas(token, rf)`): keys whose replica sets are identical share a
/// shard, and each distinct replica set is one shard. The driver uses `shards`
/// to seed a [`ShardQuorum`] and to fan out each shard's keys to its replicas,
/// and `key_shard` to route each write-set entry to the shard that owns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantSet {
    /// `shard_id -> the shard's replica host ids` (sorted, deduplicated).
    pub shards: HashMap<ShardId, Vec<Uuid>>,
    /// For each input key (by position): the `ShardId` that owns it.
    pub key_shard: Vec<ShardId>,
}

impl ParticipantSet {
    /// Build the participant set from a write-set's keys. `replicas_of(key)`
    /// returns the replica host ids for that key's token range (the driver wires
    /// this to `ring.replicas(partitioner.token(key), rf)` mapped to host ids).
    ///
    /// Keys with identical replica sets collapse to one shard. `ShardId`s are
    /// assigned deterministically (by the sorted distinct replica sets), so the
    /// same write-set always yields the same shard layout.
    ///
    /// # Panics
    /// Panics if `keys` is empty or any key resolves to zero replicas — both
    /// signal a caller bug (a transaction must write at least one key, and every
    /// key must have a replica set).
    pub fn build(keys: &[Vec<u8>], replicas_of: impl Fn(&[u8]) -> Vec<Uuid>) -> Self {
        assert!(!keys.is_empty(), "write-set must have at least one key");

        // Each key's canonical (sorted, deduped) replica set.
        let per_key: Vec<Vec<Uuid>> = keys
            .iter()
            .map(|k| {
                let mut r = replicas_of(k);
                r.sort_unstable();
                r.dedup();
                assert!(!r.is_empty(), "key resolved to zero replicas");
                r
            })
            .collect();

        // Distinct replica sets, sorted → stable ShardId = position.
        let mut distinct: Vec<Vec<Uuid>> = per_key.clone();
        distinct.sort();
        distinct.dedup();

        let shards: HashMap<ShardId, Vec<Uuid>> = distinct
            .iter()
            .enumerate()
            .map(|(i, set)| (i as ShardId, set.clone()))
            .collect();
        let key_shard: Vec<ShardId> = per_key
            .iter()
            .map(|set| {
                distinct
                    .iter()
                    .position(|s| s == set)
                    .expect("every key's replica set is in the distinct list")
                    as ShardId
            })
            .collect();

        Self { shards, key_shard }
    }

    /// Seed a [`ShardQuorum`] from this participant set.
    pub fn quorum(&self) -> ShardQuorum {
        ShardQuorum::new(&self.shards)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn participants(entries: &[(ShardId, &[u128])]) -> HashMap<ShardId, Vec<Uuid>> {
        entries
            .iter()
            .map(|(sid, nodes)| (*sid, nodes.iter().map(|&n| node(n)).collect()))
            .collect()
    }

    #[test]
    fn single_shard_needs_slow_quorum() {
        // One shard, RF=3 → slow quorum = 2.
        let mut q = ShardQuorum::new(&participants(&[(0, &[1, 2, 3])]));
        assert!(!q.all_reached(), "0 acks < quorum");
        q.record_node_ack(node(1));
        assert!(!q.all_reached(), "1 ack < quorum(3)=2");
        q.record_node_ack(node(2));
        assert!(q.all_reached(), "2 acks reach quorum(3)=2");
    }

    #[test]
    fn record_is_idempotent_per_node() {
        let mut q = ShardQuorum::new(&participants(&[(0, &[1, 2, 3])]));
        q.record_node_ack(node(1));
        q.record_node_ack(node(1)); // duplicate must not count twice
        assert!(!q.all_reached(), "one distinct ack is still below quorum");
    }

    #[test]
    fn ack_from_non_replica_is_ignored() {
        let mut q = ShardQuorum::new(&participants(&[(0, &[1, 2, 3])]));
        q.record_node_ack(node(99)); // not a replica of shard 0
        q.record_node_ack(node(1));
        assert!(!q.all_reached(), "the stray ack must not count");
    }

    /// THE per-shard invariant: a full quorum in shard A plus a single ack in
    /// shard B must NOT count as committed — shard B is a minority. A global
    /// counter (4 distinct acks ≥ slow_quorum_size(6)=4) would wrongly report
    /// success; per-shard correctly blocks on shard B.
    #[test]
    fn minority_in_one_shard_blocks_the_whole_txn() {
        let mut q = ShardQuorum::new(&participants(&[
            (0, &[1, 2, 3]), // shard A
            (1, &[4, 5, 6]), // shard B
        ]));
        // Shard A: full quorum (all three).
        q.record_node_ack(node(1));
        q.record_node_ack(node(2));
        q.record_node_ack(node(3));
        // Shard B: a single ack (minority of RF=3).
        q.record_node_ack(node(4));

        assert!(
            !q.all_reached(),
            "shard B has only 1/3 acks (< quorum 2) — txn must NOT be committed"
        );
        assert_eq!(q.unmet(), vec![1], "shard B (id 1) is the unmet shard");
    }

    #[test]
    fn both_shards_reaching_quorum_commits() {
        let mut q = ShardQuorum::new(&participants(&[(0, &[1, 2, 3]), (1, &[4, 5, 6])]));
        for n in [1, 2, 4, 5] {
            q.record_node_ack(node(n));
        }
        assert!(q.all_reached(), "both shards at 2/3 → quorum each");
        assert!(q.unmet().is_empty());
    }

    #[test]
    fn overlapping_replica_counts_for_every_shard_it_owns() {
        // Node 3 replicates BOTH shards (e.g. ring wrap-around). Its ack counts
        // toward each shard's quorum independently.
        let mut q = ShardQuorum::new(&participants(&[(0, &[1, 2, 3]), (1, &[3, 4, 5])]));
        q.record_node_ack(node(3)); // counts for shard 0 AND shard 1
        q.record_node_ack(node(1)); // shard 0 → now 2/3
        q.record_node_ack(node(4)); // shard 1 → now 2/3
        assert!(
            q.all_reached(),
            "the shared node satisfied both shards' quorums"
        );
    }

    #[test]
    fn participant_set_collapses_keys_with_identical_replicas_to_one_shard() {
        let ps = ParticipantSet::build(&[b"a".to_vec(), b"b".to_vec()], |_k| {
            vec![node(1), node(2), node(3)]
        });
        assert_eq!(ps.shards.len(), 1, "shared replica set → one shard");
        assert_eq!(ps.key_shard, vec![0, 0]);
    }

    #[test]
    fn participant_set_distinct_replica_sets_become_distinct_shards() {
        let ps = ParticipantSet::build(&[b"a".to_vec(), b"b".to_vec()], |k| {
            if k == b"a" {
                vec![node(1), node(2), node(3)]
            } else {
                vec![node(4), node(5), node(6)]
            }
        });
        assert_eq!(ps.shards.len(), 2);
        assert_ne!(
            ps.key_shard[0], ps.key_shard[1],
            "a and b in different shards"
        );

        // Quorum in only ONE shard must not satisfy the whole txn.
        let mut q = ps.quorum();
        for r in &ps.shards[&ps.key_shard[0]] {
            q.record_node_ack(*r);
        }
        assert!(!q.all_reached(), "the other shard is still unmet");
    }

    #[test]
    fn participant_set_overlapping_but_unequal_sets_are_distinct_shards() {
        let ps = ParticipantSet::build(&[b"a".to_vec(), b"b".to_vec()], |k| {
            if k == b"a" {
                vec![node(1), node(2), node(3)]
            } else {
                vec![node(3), node(4), node(5)] // shares node 3, not identical
            }
        });
        assert_eq!(ps.shards.len(), 2, "overlap ≠ identical → 2 shards");
    }

    #[test]
    fn participant_set_dedups_replicas_within_a_key() {
        let ps = ParticipantSet::build(&[b"a".to_vec()], |_k| vec![node(1), node(1), node(2)]);
        assert_eq!(ps.shards[&0].len(), 2, "duplicate replica deduped → rf=2");
    }

    use proptest::prelude::*;
    use std::collections::BTreeSet;

    proptest! {
        /// Fuzz shard-count × per-shard RF × overlapping replicas × ack pattern
        /// against an independent oracle: `all_reached()` is true iff EVERY
        /// shard independently has ≥ `slow_quorum_size(rf)` of its DISTINCT
        /// replicas acked.
        #[test]
        fn all_reached_matches_per_shard_oracle(
            shards in prop::collection::vec(prop::collection::vec(1u128..10, 1..6), 1..5),
            ack_bits in any::<u64>(),
        ) {
            let participants: HashMap<ShardId, Vec<Uuid>> = shards
                .iter()
                .enumerate()
                .map(|(i, nodes)| (i as ShardId, nodes.iter().map(|&n| node(n)).collect()))
                .collect();
            let mut q = ShardQuorum::new(&participants);

            // Ack a deterministic subset of the distinct nodes (one bit each).
            let all_nodes: BTreeSet<u128> = shards.iter().flatten().copied().collect();
            let mut acked: HashSet<u128> = HashSet::new();
            for (i, &n) in all_nodes.iter().enumerate() {
                if (ack_bits >> (i % 64)) & 1 == 1 {
                    q.record_node_ack(node(n));
                    acked.insert(n);
                }
            }

            let expected = shards.iter().all(|nodes| {
                let distinct: BTreeSet<u128> = nodes.iter().copied().collect();
                let got = distinct.iter().filter(|n| acked.contains(n)).count();
                got >= slow_quorum_size(distinct.len())
            });
            prop_assert_eq!(q.all_reached(), expected);
        }
    }
}
