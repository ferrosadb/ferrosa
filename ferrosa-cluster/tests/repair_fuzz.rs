//! Property-based fuzz harness for the anti-entropy repair executor.
//!
//! Spec: `specs/proposed/repair-fuzz-harness-design.md`. Drives the repair
//! convergence properties against `LocalRepairExecutor` + `InMemoryRepairStore`
//! over randomly-generated divergent replica sets (`arb_replica_set`):
//!
//! - #4 convergence: divergent replicas -> after repair every replica holds the
//!   per-(whole-)partition LWW union; running repair again is a no-op.
//! - #4 idempotence: a second repair pass streams zero partitions.
//! - #7 quarantine-safety: a replica missing a partition (the "corrupt gen
//!   excluded" case modelled as a dropped partition) recovers it via repair
//!   from a healthy replica; never lost when any replica still has it.
//!
//! The convergence oracle is `test_support::lww_merge` — *whole-partition*
//! max-timestamp LWW, matching the shipped repair model (`diff_partition_sets`
//! picks the highest `newest_partition_timestamp` per key; equal-ts/different-
//! content collisions are surfaced as ties, not silently merged). The
//! generators avoid exact ties (distinct timestamp deltas), so the union is
//! well-defined; the harness still tolerates tie keys defensively.
//!
//! Case count: override with `PROPTEST_CASES` (default raised to 512 here).

use std::collections::BTreeSet;
use std::sync::Arc;

use proptest::prelude::*;

use ferrosa_sstable::types::Partition;
use ferrosa_storage::test_support::{arb_replica_set, lww_merge, newest_ts};

use ferrosa_cluster::repair::{
    InMemoryRepairStore, LocalRepairExecutor, RepairStore, SessionExecutor,
};
use ferrosa_storage::TableId;

const CASES: u32 = 512;

fn keys_of(parts: &[Partition]) -> BTreeSet<Vec<u8>> {
    parts
        .iter()
        .map(|p| p.key.key.as_bytes().to_vec())
        .collect()
}

/// Build an in-memory repair store seeded with `parts`.
async fn store_with(parts: &[Partition]) -> Arc<InMemoryRepairStore> {
    let s = Arc::new(InMemoryRepairStore::new());
    for p in parts {
        s.insert(p.clone()).await;
    }
    s
}

/// Repair every replica pairwise until convergence: repair replica[0] against
/// each other replica (both directions in one session), then a second sweep so
/// data discovered on replica[0] propagates back out. Two sweeps suffice for a
/// star topology because session repair is bidirectional.
async fn converge_all(stores: &[Arc<InMemoryRepairStore>], table: &TableId) {
    if stores.len() < 2 {
        return;
    }
    for _sweep in 0..2 {
        for i in 1..stores.len() {
            let executor = LocalRepairExecutor {
                local: stores[0].clone() as Arc<dyn RepairStore>,
                remotes: [(i as u64, stores[i].clone() as Arc<dyn RepairStore>)]
                    .into_iter()
                    .collect(),
            };
            executor
                .run_session(table, i64::MIN, i64::MAX, i as u64)
                .await
                .expect("repair session must not error");
        }
    }
}

/// Token-sorted snapshot of a store's partitions.
async fn sorted_snapshot(s: &InMemoryRepairStore) -> Vec<Partition> {
    let mut snap = s.snapshot().await;
    snap.sort_by(|a, b| a.key.cmp(&b.key));
    snap
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

// -------------------------------------------------------------------------
// Property #4 — convergence + idempotence.
// -------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// After repairing a divergent replica set, every replica must hold the
    /// same key set, and that key set must equal the keys of the LWW union of
    /// the inputs. A second repair pass must stream zero partitions (idempotent).
    #[test]
    fn divergent_replicas_converge_and_repair_is_idempotent(
        rs in arb_replica_set(8, 3, 3),
    ) {
        let rt = rt();
        rt.block_on(async {
            let table = TableId::new("ks", "tbl");
            let stores: Vec<Arc<InMemoryRepairStore>> = {
                let mut v = Vec::new();
                for r in &rs.replicas {
                    v.push(store_with(r).await);
                }
                v
            };

            // Oracle: the whole-partition LWW union of the input replicas.
            let oracle = lww_merge(&rs.replicas);
            let oracle_keys = keys_of(&oracle);

            converge_all(&stores, &table).await;

            // Every replica converges to the union's key set.
            for (i, s) in stores.iter().enumerate() {
                let snap = sorted_snapshot(s).await;
                prop_assert_eq!(
                    keys_of(&snap), oracle_keys.clone(),
                    "replica {} key set diverged from LWW union after repair", i
                );
            }

            // Per key, every replica must hold the winning (max-timestamp)
            // version. Compare against the oracle's per-key newest timestamp.
            for want in &oracle {
                let key = want.key.key.as_bytes();
                let want_ts = newest_ts(want);
                for (i, s) in stores.iter().enumerate() {
                    let snap = s.snapshot().await;
                    let got = snap.iter().find(|p| p.key.key.as_bytes() == key);
                    let got = match got {
                        Some(g) => g,
                        None => {
                            return Err(TestCaseError::fail(format!(
                                "replica {i} missing key {key:?} after repair"
                            )));
                        }
                    };
                    prop_assert_eq!(
                        newest_ts(got), want_ts,
                        "replica {} key {:?} converged to ts {} but LWW winner is ts {}",
                        i, key, newest_ts(got), want_ts
                    );
                }
            }

            // Idempotence: a third sweep between [0] and each peer streams nothing.
            for i in 1..stores.len() {
                let executor = LocalRepairExecutor {
                    local: stores[0].clone() as Arc<dyn RepairStore>,
                    remotes: [(i as u64, stores[i].clone() as Arc<dyn RepairStore>)]
                        .into_iter().collect(),
                };
                let stats = executor.run_session(&table, i64::MIN, i64::MAX, i as u64).await.unwrap();
                prop_assert_eq!(
                    stats.partitions_streamed_in + stats.partitions_streamed_out, 0,
                    "repair was not idempotent: replica 0 <-> {} still streamed partitions", i
                );
            }
            Ok(())
        })?;
    }
}

// -------------------------------------------------------------------------
// Property #7 — quarantine safety: a partition missing on one replica (the
// "corrupt gen excluded" case) is recovered from a healthy replica and never
// lost while any replica still holds it.
// -------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// Take the LWW union as the healthy baseline, give one replica the full
    /// set and another replica a subset (some partitions "quarantined" = absent
    /// — the corrupt-gen-excluded model). After repair, the degraded replica
    /// must recover every partition the healthy replica has.
    #[test]
    fn quarantined_partitions_recovered_from_healthy_replica(
        rs in arb_replica_set(8, 3, 2),
        drop_mask in prop::collection::vec(any::<bool>(), 0..8),
    ) {
        let rt = rt();
        rt.block_on(async {
            prop_assume!(!rs.base.is_empty());
            let table = TableId::new("ks", "tbl");

            // Healthy replica = the full base set (every partition present).
            let healthy = store_with(&rs.base).await;

            // Degraded replica = base with masked partitions "quarantined"
            // (removed). This models a generation excluded by the startup
            // smoke-test on one node.
            let degraded_parts: Vec<Partition> = rs.base.iter().enumerate()
                .filter(|(i, _)| !drop_mask.get(*i).copied().unwrap_or(false))
                .map(|(_, p)| p.clone())
                .collect();
            let degraded = store_with(&degraded_parts).await;

            let healthy_keys = keys_of(&rs.base);

            let stores = vec![degraded.clone(), healthy.clone()];
            converge_all(&stores, &table).await;

            // The degraded replica must now hold every key the healthy replica
            // has — quarantined partitions are recovered, never lost while a
            // healthy replica still has them.
            let recovered = sorted_snapshot(&degraded).await;
            let recovered_keys = keys_of(&recovered);
            prop_assert!(
                healthy_keys.is_subset(&recovered_keys),
                "quarantined partitions NOT recovered: degraded replica missing {:?}",
                healthy_keys.difference(&recovered_keys).collect::<Vec<_>>()
            );
            Ok(())
        })?;
    }
}
