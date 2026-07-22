//! Module: Two-level (group→query) hierarchical fair run queue (B3 T3.1).
//! Correctness: Correct when `pick_next` returns the least-`vruntime` query of
//!   the least-`vruntime` group, a group's `vruntime` advances by the service of
//!   *any* of its queries (so its aggregate share is independent of its query
//!   count), and both group- and query-level `min_vruntime` floors are monotonic.
//! Last revised: 2026-07-22
//! Last changed: New module — B3 T3.1. Extends the single-group [`RunQueue`]
//!   (B1) to two levels so scheduling is fair *between tenants* (groups), not
//!   just between queries: a tenant that submits 100 queries gets the same
//!   aggregate share as one that submits 1 (FM-12, the anti-gaming property).
//!
//! # Model (CFS group scheduling, adapted)
//!
//! The outer level schedules **groups** (tenants) by group `vruntime`; the inner
//! level schedules a group's **queries** by query `vruntime` (a nested
//! [`RunQueue`]). Picking runs the least-`vruntime` query of the least-`vruntime`
//! group. When a query runs for some *service*, both its own `vruntime`
//! ([`advance_vruntime`] on the [`SchedEntity`]) and its **group's** `vruntime`
//! ([`charge`](GroupRunQueue::charge)) advance — so a group drifts back in
//! proportion to the *total* service consumed by all its queries. That is what
//! makes the per-tenant share independent of query count.
//!
//! `GroupId` is an opaque `u64` (the caller maps `TenantContext` → id) so this
//! leaf crate keeps its tokio-only dependency set (DSM guard).

use std::collections::{BTreeMap, HashMap};

use crate::runqueue::{RunQueue, SchedEntity};
use crate::scheduler::advance_vruntime;

/// Opaque tenant/group identifier. The caller maps its `TenantContext` to a
/// stable `u64`.
pub type GroupId = u64;

/// Default fair-share weight of a group when the caller does not set a per-tenant
/// weight (T3.2). Equal-weight groups get equal aggregate share.
pub const DEFAULT_GROUP_WEIGHT: u32 = 1024;

struct Group {
    /// Group-level virtual runtime — the sum of its queries' service, weighted
    /// by the group weight. Least-`vruntime` group is scheduled next.
    vruntime: u64,
    /// Fair-share weight for this group (per-tenant share; T3.2).
    weight: u32,
    /// This group's queries, ordered by query `vruntime` (the inner level).
    queries: RunQueue,
    /// The group's current key in `tree`, or `None` when it has no queued
    /// queries (a running query's group is absent until the query re-enqueues).
    tree_key: Option<(u64, u64)>,
}

/// A two-level fair run queue: groups (tenants) over queries.
pub struct GroupRunQueue {
    /// `(group_vruntime, seq) → group_id`, holding only groups with ≥1 queued
    /// query. `seq` breaks ties in group-arrival order (FIFO among equal groups).
    tree: BTreeMap<(u64, u64), GroupId>,
    groups: HashMap<GroupId, Group>,
    /// Monotonic group-level floor: a newly-arriving or waking group cannot
    /// undercut running groups by more than `boost`.
    min_vruntime: u64,
    seq: u64,
    boost: u64,
}

/// A query picked to run, tagged with the group it belongs to (pass `group` back
/// to [`charge`](GroupRunQueue::charge) when the query consumes service, and
/// `group_weight` to re-enqueue it on a cooperative yield).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Picked {
    pub group: GroupId,
    pub group_weight: u32,
    pub entity: SchedEntity,
}

impl GroupRunQueue {
    /// A queue whose waking groups may dip at most `boost` below the group-level
    /// `min_vruntime` (bounded sleeper credit, as in [`RunQueue::new`]).
    pub fn new(boost: u64) -> Self {
        Self {
            tree: BTreeMap::new(),
            groups: HashMap::new(),
            min_vruntime: 0,
            seq: 0,
            boost,
        }
    }

    /// Enqueue `entity` under group `group_id` (weight `group_weight`), creating
    /// the group if it is new. A new/waking group's `vruntime` is clamped up to
    /// at least `min_vruntime - boost` so it cannot monopolize on arrival.
    pub fn enqueue(&mut self, group_id: GroupId, group_weight: u32, entity: SchedEntity) {
        let floor = self.min_vruntime.saturating_sub(self.boost);
        let group = self.groups.entry(group_id).or_insert_with(|| Group {
            vruntime: floor,
            weight: group_weight.max(1),
            queries: RunQueue::new(self.boost),
            tree_key: None,
        });
        // A group that had gone idle wakes clamped to the current floor.
        if group.tree_key.is_none() {
            group.vruntime = group.vruntime.max(floor);
        }
        group.queries.enqueue(entity);
        if group.tree_key.is_none() {
            let key = (group.vruntime, self.seq);
            self.seq += 1;
            self.tree.insert(key, group_id);
            group.tree_key = Some(key);
        }
    }

    /// Remove and return the least-`vruntime` query of the least-`vruntime`
    /// group (FIFO among ties at each level), advancing the group-level floor.
    /// `None` if empty.
    pub fn pick_next(&mut self) -> Option<Picked> {
        let (&key, &group_id) = self.tree.iter().next()?;
        let group = self
            .groups
            .get_mut(&group_id)
            .expect("group in tree exists");
        let entity = group
            .queries
            .pick_next()
            .expect("group in tree has a query");
        self.min_vruntime = self.min_vruntime.max(group.vruntime);
        // If the group has no more queued queries, it leaves the outer tree
        // until one of its queries re-enqueues.
        let group_weight = group.weight;
        if group.queries.is_empty() {
            self.tree.remove(&key);
            group.tree_key = None;
        }
        Some(Picked {
            group: group_id,
            group_weight,
            entity,
        })
    }

    /// The `(group_vruntime, query_vruntime)` of the query that [`pick_next`] would
    /// return — the lexicographic priority of the most-deserving waiting scan.
    /// A running scan yields when this is strictly smaller than its own
    /// `(group_vruntime, query_vruntime)`, giving fairness at both levels: same
    /// group → compare queries; different group → the group dominates.
    ///
    /// [`pick_next`]: GroupRunQueue::pick_next
    pub fn peek_min(&self) -> Option<(u64, u64)> {
        let (&(group_vruntime, _), group_id) = self.tree.iter().next()?;
        let query_vruntime = self.groups[group_id].queries.peek_min_vruntime()?;
        Some((group_vruntime, query_vruntime))
    }

    /// Charge `service` to group `group_id` (a query in it ran for `service`),
    /// advancing the group's `vruntime` weighted by the group weight and re-keying
    /// it in the outer tree. This is what equalizes *aggregate* per-tenant share
    /// regardless of how many queries the tenant runs.
    pub fn charge(&mut self, group_id: GroupId, service: u64) {
        let Some(group) = self.groups.get_mut(&group_id) else {
            return;
        };
        group.vruntime = advance_vruntime(group.vruntime, service, group.weight);
        if let Some(old) = group.tree_key.take() {
            self.tree.remove(&old);
            let key = (group.vruntime, self.seq);
            self.seq += 1;
            self.tree.insert(key, group_id);
            group.tree_key = Some(key);
        }
    }

    /// The `vruntime` of the least group with queued work (`None` if empty) —
    /// for a should-switch decision at the group level.
    pub fn peek_min_group_vruntime(&self) -> Option<u64> {
        self.tree.keys().next().map(|(vruntime, _seq)| *vruntime)
    }

    /// Current `vruntime` of `group_id` (`None` if the group is unknown) — lets a
    /// running query compare its group against the least waiting group.
    pub fn group_vruntime(&self, group_id: GroupId) -> Option<u64> {
        self.groups.get(&group_id).map(|g| g.vruntime)
    }

    /// Total queued queries across all groups (the admission-waiter count, for
    /// the bounded-queue / `Overloaded` backpressure check).
    pub fn len(&self) -> usize {
        self.groups.values().map(|g| g.queries.len()).sum()
    }

    /// Remove the queued query `id` from whichever group holds it, dropping the
    /// group from the outer tree if it becomes empty. Returns whether one was
    /// removed. Used by admission cancellation cleanup (across groups).
    pub fn remove_by_id(&mut self, id: u64) -> bool {
        let mut emptied_key: Option<(u64, u64)> = None;
        let mut removed = false;
        for group in self.groups.values_mut() {
            if group.queries.remove_by_id(id) {
                removed = true;
                if group.queries.is_empty() {
                    emptied_key = group.tree_key.take();
                }
                break;
            }
        }
        if let Some(key) = emptied_key {
            self.tree.remove(&key);
        }
        removed
    }

    /// The monotonic group-level floor.
    pub fn min_vruntime(&self) -> u64 {
        self.min_vruntime
    }

    /// Number of groups with queued queries.
    pub fn active_groups(&self) -> usize {
        self.tree.len()
    }

    /// Whether any group has a queued query.
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchedClass;

    fn ent(id: u64) -> SchedEntity {
        SchedEntity::new(id, SchedClass::Bulk)
    }

    #[test]
    fn picks_least_vruntime_group_then_least_query() {
        let mut q = GroupRunQueue::new(0);
        // Group 10 has queries 0,1; group 20 has query 2. All equal weight.
        q.enqueue(10, DEFAULT_GROUP_WEIGHT, ent(0));
        q.enqueue(10, DEFAULT_GROUP_WEIGHT, ent(1));
        q.enqueue(20, DEFAULT_GROUP_WEIGHT, ent(2));
        // Both groups at vruntime 0; group 10 arrived first → picked first.
        let p = q.pick_next().unwrap();
        assert_eq!(p.group, 10);
        // Charge group 10 a big service → group 20 is now more deserving.
        q.charge(10, 1000);
        q.enqueue(10, DEFAULT_GROUP_WEIGHT, ent(0)); // requeue the ran query
        let p = q.pick_next().unwrap();
        assert_eq!(
            p.group, 20,
            "after group 10 ran, group 20 is least-vruntime"
        );
    }

    /// T3.9 / FM-12 — the anti-gaming property: a tenant that floods the queue
    /// with 100 queries gets the SAME aggregate share as a tenant with 1 query.
    /// Fairness is per-GROUP: group `vruntime` advances by the service of
    /// whichever of its queries ran, so 100 queries share one group's slice.
    #[test]
    fn one_tenant_hundred_queries_equals_one_tenant_one_query() {
        const HEAVY: GroupId = 1; // 100 queries
        const LIGHT: GroupId = 2; // 1 query
        let mut q = GroupRunQueue::new(0);
        for id in 0..100u64 {
            q.enqueue(HEAVY, DEFAULT_GROUP_WEIGHT, ent(id));
        }
        q.enqueue(LIGHT, DEFAULT_GROUP_WEIGHT, ent(1000));

        let mut service = std::collections::HashMap::<GroupId, u64>::new();
        // Each round: run one query for a fixed unit of service, then requeue it
        // (both tenants have effectively unbounded work).
        for _ in 0..10_000 {
            let p = q.pick_next().expect("work remains");
            *service.entry(p.group).or_insert(0) += 1;
            q.charge(p.group, 1);
            // Requeue the query that just ran (its own vruntime advanced).
            let mut e = p.entity;
            e.vruntime = advance_vruntime(e.vruntime, 1, e.weight);
            q.enqueue(p.group, DEFAULT_GROUP_WEIGHT, e);
        }

        let heavy = service[&HEAVY] as f64;
        let light = service[&LIGHT] as f64;
        let ratio = heavy / light;
        // Equal group weight → equal aggregate share (~1:1), NOT 100:1.
        assert!(
            (0.8..=1.25).contains(&ratio),
            "per-tenant share should be independent of query count: \
             heavy(100q)={heavy} light(1q)={light} ratio={ratio:.2}"
        );
    }

    #[test]
    fn per_tenant_weight_sets_share() {
        // T3.2 preview: a 3× weight group gets ~3× the aggregate share. Service
        // is a realistic per-round cost (like elapsed µs) so the weighting shows
        // in the proportion rather than being masked by the ≥1 rounding floor.
        const BIG: GroupId = 1;
        const SMALL: GroupId = 2;
        const SERVICE: u64 = 1000;
        let weight_of = |g: GroupId| {
            if g == BIG {
                DEFAULT_GROUP_WEIGHT * 3
            } else {
                DEFAULT_GROUP_WEIGHT
            }
        };
        let mut q = GroupRunQueue::new(0);
        q.enqueue(BIG, weight_of(BIG), ent(0));
        q.enqueue(SMALL, weight_of(SMALL), ent(1));
        let mut service = std::collections::HashMap::<GroupId, u64>::new();
        for _ in 0..9000 {
            let p = q.pick_next().expect("work remains");
            *service.entry(p.group).or_insert(0) += 1;
            q.charge(p.group, SERVICE);
            let mut e = p.entity;
            e.vruntime = advance_vruntime(e.vruntime, SERVICE, e.weight);
            q.enqueue(p.group, weight_of(p.group), e);
        }
        let ratio = service[&BIG] as f64 / service[&SMALL] as f64;
        assert!(
            (2.4..=3.6).contains(&ratio),
            "share should scale with weight (~3:1), got {ratio:.2} ({service:?})"
        );
    }

    #[test]
    fn empty_and_active_groups_track_membership() {
        let mut q = GroupRunQueue::new(0);
        assert!(q.is_empty());
        q.enqueue(1, DEFAULT_GROUP_WEIGHT, ent(0));
        q.enqueue(2, DEFAULT_GROUP_WEIGHT, ent(1));
        assert_eq!(q.active_groups(), 2);
        q.pick_next(); // group 1's only query leaves → group 1 exits the tree
        assert_eq!(q.active_groups(), 1);
        q.pick_next();
        assert!(q.is_empty());
    }
}
