//! Binary-side wiring for automatic anti-entropy repair.
//!
//! The cluster layer ships the *primitives* (`RepairCoordinator`,
//! `AutoRepairScheduler`, `ClusterRepairView`, `ClusterRepairTrigger`) and the
//! *ports* (`RepairContext`, storage's `RepairTrigger` /
//! `ExecutorProvider`). This module is the place in the `ferrosa` binary that
//! satisfies the `RepairContext` port against live node state (the
//! `ModeController`, `StorageEngine`, and `Schema`) and exposes the single
//! `build_repair_executor` code path that both the periodic scheduler and the
//! self-heal refill trigger's executor provider resolve against the current
//! ring.
//!
//! See `specs/proposed/automatic-repair-scheduler-design.md` and its FMEA.

use std::collections::HashMap;
use std::sync::Arc;

use ferrosa_cluster::ModeController;
use ferrosa_cluster::{
    LocalRepairExecutor, RemoteRepairStore, RepairContext, SessionExecutor,
    StorageEngineRepairStore,
};
use ferrosa_schema::Schema;
use ferrosa_storage::{StorageEngine, TableId};

/// Build a repair executor for the current ring, resolving this node's
/// `node_id` from its `host_id`. Returns `None` when not in cluster mode, the
/// peer manager is not initialised, or this node is not yet in the ring.
///
/// This is the **single** executor-construction path. The scheduler's
/// [`BinaryRepairContext::build_executor`] and the self-heal refill trigger's
/// [`ExecutorProvider`](ferrosa_cluster::repair::ExecutorProvider) closure both
/// call it, so there is exactly one place that builds the local
/// [`StorageEngineRepairStore`] + per-peer [`RemoteRepairStore`] →
/// [`LocalRepairExecutor`].
///
/// FMEA: every "not ready" condition is a clean `None` (the caller treats it as
/// a no-op), never a panic or a fake executor.
pub fn build_repair_executor(
    mode_controller: &ModeController,
    storage: &Arc<StorageEngine>,
) -> Option<Arc<dyn SessionExecutor>> {
    let ring = mode_controller.token_ring()?;
    let peer_manager = mode_controller.peer_manager_arc()?;

    // Resolve our own node_id by looking up our host_id in the ring.
    let host_id = mode_controller.host_id();
    let local_node_id = ring.node_ids().iter().copied().find(|&id| {
        ring.get_node(id)
            .is_some_and(|info| info.host_id == host_id)
    })?;

    // local = in-process storage; remotes = one RemoteRepairStore per other
    // node in the ring.
    let local: Arc<dyn ferrosa_cluster::RepairStore> =
        Arc::new(StorageEngineRepairStore::new(storage.clone()));
    let mut remotes: HashMap<u64, Arc<dyn ferrosa_cluster::RepairStore>> = HashMap::new();
    for node_id in ring.node_ids() {
        if node_id == local_node_id {
            continue;
        }
        let Some(node_info) = ring.get_node(node_id) else {
            continue;
        };
        let remote: Arc<dyn ferrosa_cluster::RepairStore> = Arc::new(RemoteRepairStore {
            host_id: node_info.host_id,
            peer_manager: peer_manager.clone(),
        });
        remotes.insert(node_id, remote);
    }

    Some(Arc::new(LocalRepairExecutor { local, remotes }))
}

/// Parse a keyspace's replication factor from its replication options.
///
/// FMEA #11 — never hardcode `3`. For `SimpleStrategy` the RF is the
/// `replication_factor` option; for `NetworkTopologyStrategy` it is the **sum**
/// of the per-DC factors (every non-`replication_factor`, non-`class` option is
/// a `dc -> rf` entry). When no factor can be parsed we fall back to `default`
/// (the caller passes 3, the platform default) so a malformed keyspace still
/// gets repaired against a sane peer set rather than being silently skipped.
pub fn replication_factor_for(options: &HashMap<String, String>, default: usize) -> usize {
    // SimpleStrategy / explicit cluster-wide factor.
    if let Some(rf) = options
        .get("replication_factor")
        .and_then(|v| v.trim().parse::<usize>().ok())
    {
        if rf >= 1 {
            return rf;
        }
    }

    // NetworkTopologyStrategy: sum the per-DC factors. Keys other than the
    // strategy `class` and the cluster-wide `replication_factor` are DC names.
    let dc_sum: usize = options
        .iter()
        .filter(|(k, _)| k.as_str() != "class" && k.as_str() != "replication_factor")
        .filter_map(|(_, v)| v.trim().parse::<usize>().ok())
        .sum();
    if dc_sum >= 1 {
        return dc_sum;
    }

    default
}

/// Enumerate user tables eligible for auto-repair, each paired with its
/// keyspace replication factor (FMEA #11). Thin wrapper over
/// [`user_tables_from_snapshot`] that takes a live [`Schema`].
pub fn user_tables_from_schema(
    schema: &Schema,
    skip_prefixes: &[String],
    default_rf: usize,
) -> Vec<(TableId, usize)> {
    user_tables_from_snapshot(&schema.snapshot(), skip_prefixes, default_rf)
}

/// Enumerate user tables eligible for auto-repair from a schema snapshot, each
/// paired with its keyspace replication factor (FMEA #11). Pure function —
/// unit-testable from a constructed [`ferrosa_schema::SchemaSnapshot`] with no `Schema`/auth.
///
/// `skip_prefixes` are keyspace-name prefixes to exclude (system/internal); a
/// keyspace whose name starts with any prefix is dropped. `default_rf` is used
/// only when a keyspace's replication options cannot be parsed.
pub fn user_tables_from_snapshot(
    snapshot: &ferrosa_schema::SchemaSnapshot,
    skip_prefixes: &[String],
    default_rf: usize,
) -> Vec<(TableId, usize)> {
    let skipped = |ks: &str| skip_prefixes.iter().any(|p| ks.starts_with(p.as_str()));

    let mut out: Vec<(TableId, usize)> = Vec::new();
    for (ks, table) in snapshot.tables.keys() {
        if skipped(ks) {
            continue;
        }
        let rf = snapshot
            .keyspaces
            .get(ks)
            .map(|k| replication_factor_for(&k.replication.options, default_rf))
            .unwrap_or(default_rf);
        if rf >= 1 {
            out.push((TableId::new(ks.clone(), table.clone()), rf));
        }
    }
    // Deterministic order so a tick's round-robin cursor is stable across calls.
    out.sort_by(|a, b| (a.0.keyspace(), a.0.table()).cmp(&(b.0.keyspace(), b.0.table())));
    out
}

/// Production [`RepairContext`]: the scheduler's window into live node state.
///
/// Holds the handles the four accessors need. Every accessor returns "not
/// ready" as `None` so the scheduler no-ops a tick when the node is not in
/// cluster mode / not yet in the ring (the scheduler's documented contract).
pub struct BinaryRepairContext {
    mode_controller: Arc<ModeController>,
    storage: Arc<StorageEngine>,
    schema: Arc<Schema>,
    /// Keyspace-name prefixes to skip (mirrors `AutoRepairConfig::skip_keyspaces`).
    skip_prefixes: Vec<String>,
    /// RF fallback when a keyspace's options can't be parsed.
    default_rf: usize,
}

impl BinaryRepairContext {
    /// Construct from the live handles. `skip_prefixes` should match the
    /// scheduler config's skip list; `default_rf` is the RF fallback (3).
    pub fn new(
        mode_controller: Arc<ModeController>,
        storage: Arc<StorageEngine>,
        schema: Arc<Schema>,
        skip_prefixes: Vec<String>,
        default_rf: usize,
    ) -> Self {
        Self {
            mode_controller,
            storage,
            schema,
            skip_prefixes,
            default_rf,
        }
    }
}

impl RepairContext for BinaryRepairContext {
    fn token_ring(&self) -> Option<ferrosa_cluster::ring::TokenRing> {
        // ModeController hands out `Arc<TokenRing>`; the trait wants an owned
        // snapshot, so deref-clone (cheap relative to a repair cycle).
        self.mode_controller.token_ring().map(|r| (*r).clone())
    }

    fn local_node_id(&self) -> Option<u64> {
        let ring = self.mode_controller.token_ring()?;
        let host_id = self.mode_controller.host_id();
        ring.node_ids().iter().copied().find(|&id| {
            ring.get_node(id)
                .is_some_and(|info| info.host_id == host_id)
        })
    }

    fn build_executor(&self) -> Option<Arc<dyn SessionExecutor>> {
        build_repair_executor(&self.mode_controller, &self.storage)
    }

    fn user_tables(&self) -> Vec<(TableId, usize)> {
        user_tables_from_schema(&self.schema, &self.skip_prefixes, self.default_rf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_schema::{
        KeyspaceMetadata, ReplicationParams, SchemaSnapshot, TableMetadata, TableParams,
    };

    fn rf_opts(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn rf_simple_strategy_uses_replication_factor() {
        let opts = rf_opts(&[("class", "SimpleStrategy"), ("replication_factor", "5")]);
        assert_eq!(replication_factor_for(&opts, 3), 5);
    }

    #[test]
    fn rf_network_topology_sums_per_dc_factors() {
        let opts = rf_opts(&[
            ("class", "NetworkTopologyStrategy"),
            ("dc1", "3"),
            ("dc2", "2"),
        ]);
        assert_eq!(replication_factor_for(&opts, 3), 5);
    }

    #[test]
    fn rf_falls_back_to_default_when_unparseable() {
        let opts = rf_opts(&[("class", "SimpleStrategy")]);
        assert_eq!(replication_factor_for(&opts, 3), 3);
        // RF=0 is invalid; fall back rather than repair against nobody.
        let zero = rf_opts(&[("replication_factor", "0")]);
        assert_eq!(replication_factor_for(&zero, 3), 3);
    }

    fn ks(name: &str, rf: &str) -> KeyspaceMetadata {
        KeyspaceMetadata {
            name: name.to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: rf_opts(&[("class", "SimpleStrategy"), ("replication_factor", rf)]),
            },
        }
    }

    fn table(keyspace: &str, name: &str) -> TableMetadata {
        TableMetadata {
            keyspace: keyspace.to_string(),
            name: name.to_string(),
            id: uuid::Uuid::new_v4(),
            columns: Default::default(),
            partition_key: vec!["id".to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: Default::default(),
            extensions: Default::default(),
            is_system: false,
        }
    }

    #[test]
    fn user_tables_skips_system_and_uses_per_keyspace_rf() {
        let mut snap = SchemaSnapshot::new();
        snap.keyspaces.insert("app".into(), ks("app", "2"));
        snap.keyspaces
            .insert("analytics".into(), ks("analytics", "3"));
        snap.keyspaces
            .insert("system_auth".into(), ks("system_auth", "1"));
        snap.tables
            .insert(("app".into(), "users".into()), table("app", "users"));
        snap.tables.insert(
            ("analytics".into(), "events".into()),
            table("analytics", "events"),
        );
        snap.tables.insert(
            ("system_auth".into(), "roles".into()),
            table("system_auth", "roles"),
        );

        let skip = vec!["system".to_string()];
        let tables = user_tables_from_snapshot(&snap, &skip, 3);

        // system keyspaces excluded; both user tables present, each with its
        // own keyspace RF (FMEA #11).
        let app = tables
            .iter()
            .find(|(t, _)| t.keyspace() == "app" && t.table() == "users")
            .expect("app.users present");
        assert_eq!(app.1, 2, "app keyspace RF=2");
        let analytics = tables
            .iter()
            .find(|(t, _)| t.keyspace() == "analytics" && t.table() == "events")
            .expect("analytics.events present");
        assert_eq!(analytics.1, 3, "analytics keyspace RF=3");
        assert!(
            tables
                .iter()
                .all(|(t, _)| !t.keyspace().starts_with("system")),
            "no system keyspace table is selected for auto-repair"
        );
        // Deterministic order (analytics < app lexicographically).
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].0.keyspace(), "analytics");
        assert_eq!(tables[1].0.keyspace(), "app");
    }

    #[test]
    fn user_tables_uses_default_rf_when_keyspace_metadata_missing() {
        let mut snap = SchemaSnapshot::new();
        // Table present but its keyspace has no metadata entry → default RF.
        snap.tables
            .insert(("orphan".into(), "t".into()), table("orphan", "t"));
        let tables = user_tables_from_snapshot(&snap, &["system".to_string()], 3);
        assert_eq!(tables, vec![(TableId::new("orphan", "t"), 3)]);
    }
}
