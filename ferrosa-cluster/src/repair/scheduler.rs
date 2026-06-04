//! Automatic anti-entropy repair scheduler.
//!
//! Design: `specs/proposed/automatic-repair-scheduler-design.md`
//! FMEA:   `specs/proposed/automatic-repair-scheduler-fmea.md`
//!
//! The cluster already has the repair *primitives* (`RepairCoordinator::
//! repair_table` → bounded Merkle-diff-then-stream sessions). What was missing
//! for *automatic* repair is a deterministic driver that decides **which ranges
//! this node should initiate** so that, when every node runs the scheduler, each
//! token range is repaired by exactly **one** initiator — not once per replica.
//!
//! That decision is [`select_initiated_ranges`], a **pure function** of the ring
//! (no clock, no RNG, no IO — FMEA #4): for each range the local node replicates,
//! the initiator is the live replica with the **lowest `host_id`**. Only when the
//! local node *is* that initiator does the range appear in the result. This kills
//! the thundering-herd failure mode (FMEA #1) without an election: same ring →
//! same single initiator, recomputed each tick so membership churn self-corrects.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ferrosa_net::task_pool::TaskPool;
use ferrosa_storage::TableId;

use crate::ring::TokenRing;

use super::coordinator::{RepairCoordinator, SessionExecutor, SessionResult};
use super::{coordinator::owned_token_ranges, repair_participants};

/// One token range this node should initiate repair for, with the live peers to
/// repair against. `[start, end)` over the vnode partitioning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitiatedRange {
    pub start: i64,
    pub end: i64,
    /// Live replica peers (excluding self) to run sessions against.
    pub peers: Vec<u64>,
}

/// Ranges the local node should **initiate** anti-entropy repair for this cycle,
/// as the deterministic single initiator.
///
/// For every range the local node replicates (at `rf`), the initiator is the
/// **live** replica with the lowest `host_id`. The range is returned only when
/// the local node is that initiator and at least one live peer exists. Pure
/// function of the ring — no IO/clock/RNG, so it is unit-testable and produces
/// the same selection on every node given the same ring (FMEA #1 herd, #4
/// determinism, #5 churn-idempotence).
pub fn select_initiated_ranges(
    ring: &TokenRing,
    local_node_id: u64,
    rf: usize,
) -> Vec<InitiatedRange> {
    // The local node must be in the ring to compare host_ids; if it isn't yet
    // (still joining), it initiates nothing.
    let local_host = match ring.get_node(local_node_id) {
        Some(info) => info.host_id,
        None => return Vec::new(),
    };

    let mut out = Vec::new();
    for (start, end) in owned_token_ranges(ring, local_node_id, rf) {
        // Live replicas of this range (down/joining nodes filtered out).
        let participants = repair_participants(ring, start, rf);

        // Peers to repair against = live participants other than self. With no
        // peer there is nothing to reconcile (single-node / all-peers-down).
        let peers: Vec<u64> = participants
            .iter()
            .copied()
            .filter(|&p| p != local_node_id)
            .collect();
        if peers.is_empty() {
            continue;
        }

        // Deterministic initiator = live participant with the lowest host_id.
        // host_id is the durable per-node identity (stable across restarts), so
        // the choice is stable for a given ring and survives node_id reuse.
        let initiator_host = participants
            .iter()
            .filter_map(|&n| ring.get_node(n).map(|info| info.host_id))
            .min();

        if initiator_host == Some(local_host) {
            out.push(InitiatedRange { start, end, peers });
        }
    }
    out
}

/// Deterministic configuration for the automatic repair scheduler. All values
/// come from env (fixed defaults otherwise) so behaviour is reproducible.
#[derive(Debug, Clone)]
pub struct AutoRepairConfig {
    /// Master switch. `FERROSA_AUTO_REPAIR_ENABLED` (default **on**).
    pub enabled: bool,
    /// Full-coverage period — every owned range is repaired once per interval,
    /// spread round-robin across tables. `FERROSA_AUTO_REPAIR_INTERVAL_SECS`
    /// (default 86400 = 24h).
    pub interval: Duration,
    /// Tables repaired per sub-tick (load shaping).
    /// `FERROSA_AUTO_REPAIR_MAX_CONCURRENT_TABLES` (default 1).
    pub max_concurrent_tables: usize,
    /// Keyspace name prefixes never auto-repaired (system/internal).
    /// `FERROSA_AUTO_REPAIR_SKIP_KEYSPACES` (comma-separated; default `system`).
    pub skip_keyspaces: Vec<String>,
}

impl Default for AutoRepairConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(86_400),
            max_concurrent_tables: 1,
            skip_keyspaces: vec!["system".to_string()],
        }
    }
}

impl AutoRepairConfig {
    /// Build from environment, falling back to [`Default`] for any unset/unparseable var.
    pub fn from_env() -> Self {
        let d = Self::default();
        let enabled = std::env::var("FERROSA_AUTO_REPAIR_ENABLED")
            .ok()
            .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off" | "no"))
            .unwrap_or(d.enabled);
        let interval = std::env::var("FERROSA_AUTO_REPAIR_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&s| s > 0)
            .map(Duration::from_secs)
            .unwrap_or(d.interval);
        let max_concurrent_tables = std::env::var("FERROSA_AUTO_REPAIR_MAX_CONCURRENT_TABLES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(d.max_concurrent_tables);
        let skip_keyspaces = std::env::var("FERROSA_AUTO_REPAIR_SKIP_KEYSPACES")
            .ok()
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or(d.skip_keyspaces);
        Self {
            enabled,
            interval,
            max_concurrent_tables,
            skip_keyspaces,
        }
    }

    /// True if `table`'s keyspace matches any skip prefix.
    pub fn is_skipped(&self, table: &TableId) -> bool {
        self.skip_keyspaces
            .iter()
            .any(|p| table.keyspace().starts_with(p.as_str()))
    }
}

/// Pick the next batch of tables to repair this sub-tick, round-robin from
/// `cursor`, skipping system/skip-listed keyspaces and tables already in flight.
/// Pure + deterministic. Returns `(tables_to_repair, next_cursor)`.
///
/// Round-robin (not always-from-zero) guarantees forward progress across the
/// whole table set within one `interval` even if early tables are persistently
/// in-flight, and bounds per-tick load to `max_concurrent` (FMEA #8 runaway
/// cadence).
pub fn select_tables_for_tick(
    tables: &[TableId],
    cursor: usize,
    max_concurrent: usize,
    in_flight: &HashSet<TableId>,
    cfg: &AutoRepairConfig,
) -> (Vec<TableId>, usize) {
    let n = tables.len();
    if n == 0 || max_concurrent == 0 {
        return (Vec::new(), 0);
    }
    let mut picked = Vec::new();
    let mut i = cursor % n;
    let mut scanned = 0usize;
    // Scan at most one full lap so we never spin on an all-skipped set.
    while picked.len() < max_concurrent && scanned < n {
        let t = &tables[i];
        if !cfg.is_skipped(t) && !in_flight.contains(t) {
            picked.push(t.clone());
        }
        i = (i + 1) % n;
        scanned += 1;
    }
    (picked, i)
}

// ---------------------------------------------------------------------------
// Metrics (FMEA #7 — silent repair). Process-global atomic counters in the
// crate's existing free-function style (see `coordinator/metrics.rs`). Never
// auto-resolved divergence is loud both in logs AND in these counters so an
// operator watching Prometheus sees recurring divergence/corruption.
// ---------------------------------------------------------------------------

static AUTO_REPAIR_TICKS: AtomicU64 = AtomicU64::new(0);
static AUTO_REPAIR_TABLES_REPAIRED: AtomicU64 = AtomicU64::new(0);
static AUTO_REPAIR_SESSIONS_OK: AtomicU64 = AtomicU64::new(0);
static AUTO_REPAIR_SESSIONS_FAILED: AtomicU64 = AtomicU64::new(0);
static AUTO_REPAIR_PARTITIONS_STREAMED: AtomicU64 = AtomicU64::new(0);
static AUTO_REPAIR_TIMESTAMP_TIES: AtomicU64 = AtomicU64::new(0);
static AUTO_REPAIR_SKIPPED_NOT_READY: AtomicU64 = AtomicU64::new(0);

/// Increment the per-tick counter (one per `run_tick`, ready or not).
pub fn inc_auto_repair_tick() {
    AUTO_REPAIR_TICKS.fetch_add(1, Ordering::Relaxed);
}

/// Render the `ferrosa_auto_repair_*` counters in Prometheus exposition format.
/// Mirrors `coordinator::metrics::render_prometheus`.
pub fn render_prometheus() -> String {
    format!(
        "# HELP ferrosa_auto_repair_ticks_total Automatic repair sub-ticks executed.\n\
         # TYPE ferrosa_auto_repair_ticks_total counter\n\
         ferrosa_auto_repair_ticks_total {}\n\
         # HELP ferrosa_auto_repair_tables_repaired_total Tables for which automatic repair ran a cycle.\n\
         # TYPE ferrosa_auto_repair_tables_repaired_total counter\n\
         ferrosa_auto_repair_tables_repaired_total {}\n\
         # HELP ferrosa_auto_repair_sessions_ok_total Automatic repair sessions that completed without error.\n\
         # TYPE ferrosa_auto_repair_sessions_ok_total counter\n\
         ferrosa_auto_repair_sessions_ok_total {}\n\
         # HELP ferrosa_auto_repair_sessions_failed_total Automatic repair sessions that returned an error.\n\
         # TYPE ferrosa_auto_repair_sessions_failed_total counter\n\
         ferrosa_auto_repair_sessions_failed_total {}\n\
         # HELP ferrosa_auto_repair_partitions_streamed_total Partitions streamed (in+out) by automatic repair.\n\
         # TYPE ferrosa_auto_repair_partitions_streamed_total counter\n\
         ferrosa_auto_repair_partitions_streamed_total {}\n\
         # HELP ferrosa_auto_repair_timestamp_ties_total Divergent partitions with identical timestamps surfaced (not auto-resolved).\n\
         # TYPE ferrosa_auto_repair_timestamp_ties_total counter\n\
         ferrosa_auto_repair_timestamp_ties_total {}\n\
         # HELP ferrosa_auto_repair_skipped_not_ready_total Ticks skipped because the node was not ring-ready.\n\
         # TYPE ferrosa_auto_repair_skipped_not_ready_total counter\n\
         ferrosa_auto_repair_skipped_not_ready_total {}\n",
        AUTO_REPAIR_TICKS.load(Ordering::Relaxed),
        AUTO_REPAIR_TABLES_REPAIRED.load(Ordering::Relaxed),
        AUTO_REPAIR_SESSIONS_OK.load(Ordering::Relaxed),
        AUTO_REPAIR_SESSIONS_FAILED.load(Ordering::Relaxed),
        AUTO_REPAIR_PARTITIONS_STREAMED.load(Ordering::Relaxed),
        AUTO_REPAIR_TIMESTAMP_TIES.load(Ordering::Relaxed),
        AUTO_REPAIR_SKIPPED_NOT_READY.load(Ordering::Relaxed),
    )
}

// ---------------------------------------------------------------------------
// RepairContext — the binary's window into live cluster state at tick time.
// ---------------------------------------------------------------------------

/// Everything the scheduler needs from the running node at the moment of a tick,
/// behind a trait so it is fully mockable with no IO.
///
/// The production implementation lives in the binary (`ferrosa/src/main.rs`):
/// it reads the `ModeController`'s `TokenRing`, resolves the local `node_id`
/// from `host_id`, and builds the executor exactly as the `POST /api/cluster/
/// repair` handler does (local `StorageEngineRepairStore` + per-peer
/// `RemoteRepairStore` wrapped in a `LocalRepairExecutor`).
///
/// Every accessor returns "not ready" as `None` (the node has not yet been
/// placed in the ring / is not in cluster mode). The scheduler treats *any*
/// `None` as a no-op tick (FMEA: only initiate when ring-ready with peers).
///
/// All methods are synchronous: building the executor is synchronous in
/// production, and a tick takes a fresh snapshot of all four together so they
/// describe one consistent ring view.
pub trait RepairContext: Send + Sync {
    /// Current cluster token ring, or `None` when not in cluster mode.
    fn token_ring(&self) -> Option<TokenRing>;

    /// This node's `node_id` within the ring, or `None` until it has been
    /// placed in the ring (still joining / single-node / not in cluster mode).
    fn local_node_id(&self) -> Option<u64>;

    /// A repair executor wired for the *current* ring (local store + per-peer
    /// remotes), or `None` when the node is not ready to repair.
    fn build_executor(&self) -> Option<Arc<dyn SessionExecutor>>;

    /// User tables eligible for auto-repair, each paired with its keyspace
    /// replication factor (FMEA #11 — per-keyspace RF, never a hardcoded 3).
    fn user_tables(&self) -> Vec<(TableId, usize)>;
}

// ---------------------------------------------------------------------------
// AutoRepairScheduler — the periodic driver.
// ---------------------------------------------------------------------------

/// Deterministic, loud, bounded background driver for automatic anti-entropy
/// repair. Holds the repair coordinator, a [`RepairContext`] into live cluster
/// state, the [`AutoRepairConfig`], a round-robin cursor over the table set, and
/// the set of tables currently in flight (so a tick never double-schedules a
/// table — FMEA #6).
pub struct AutoRepairScheduler {
    coord: RepairCoordinator,
    ctx: Arc<dyn RepairContext>,
    cfg: AutoRepairConfig,
    /// Round-robin position into the live table list (FMEA #8 — bounded per-tick
    /// load, forward progress across the whole set within one interval).
    cursor: usize,
    /// Tables a tick is actively repairing; cleared as each table completes.
    in_flight: HashSet<TableId>,
}

impl AutoRepairScheduler {
    /// Construct a scheduler. `coord` bounds per-table session concurrency;
    /// `ctx` supplies the live ring/executor/tables each tick; `cfg` controls
    /// cadence and skip-lists.
    pub fn new(coord: RepairCoordinator, ctx: Arc<dyn RepairContext>, cfg: AutoRepairConfig) -> Self {
        Self {
            coord,
            ctx,
            cfg,
            cursor: 0,
            in_flight: HashSet::new(),
        }
    }

    /// The repair interval this scheduler was configured with.
    pub fn interval(&self) -> Duration {
        self.cfg.interval
    }

    /// Run a single sub-tick: snapshot the live ring/executor/tables, pick this
    /// tick's tables round-robin (skipping system + in-flight), and run
    /// `repair_initiated` for each against that table's per-keyspace RF.
    ///
    /// No-op (loud INFO + metric) when the node is not ring-ready — any of
    /// `token_ring` / `local_node_id` / `build_executor` returning `None`.
    pub async fn run_tick(&mut self) {
        inc_auto_repair_tick();

        // One consistent snapshot of the ring view for the whole tick.
        let (ring, local_node_id, executor) =
            match (self.ctx.token_ring(), self.ctx.local_node_id(), self.ctx.build_executor()) {
                (Some(ring), Some(id), Some(exec)) => (ring, id, exec),
                _ => {
                    AUTO_REPAIR_SKIPPED_NOT_READY.fetch_add(1, Ordering::Relaxed);
                    tracing::info!(
                        "auto-repair: node not ring-ready (no ring / node_id / executor); skipping tick"
                    );
                    return;
                }
            };

        let tables_with_rf = self.ctx.user_tables();
        let tables: Vec<TableId> = tables_with_rf.iter().map(|(t, _)| t.clone()).collect();
        let rf_of = |t: &TableId| -> usize {
            tables_with_rf
                .iter()
                .find(|(tid, _)| tid == t)
                .map(|(_, rf)| *rf)
                .unwrap_or(1)
        };

        let (picked, next_cursor) = select_tables_for_tick(
            &tables,
            self.cursor,
            self.cfg.max_concurrent_tables,
            &self.in_flight,
            &self.cfg,
        );
        self.cursor = next_cursor;

        if picked.is_empty() {
            tracing::info!("auto-repair: no eligible tables this tick");
            return;
        }

        for table in picked {
            // Mark in-flight for the duration so a concurrent manual repair or
            // a later tick won't double-schedule this table (FMEA #6).
            self.in_flight.insert(table.clone());
            let rf = rf_of(&table);
            tracing::info!(
                keyspace = table.keyspace(),
                table = table.table(),
                rf,
                "auto-repair: starting initiated repair for table"
            );

            let results = self
                .coord
                .repair_initiated(executor.clone(), &ring, local_node_id, rf, &table)
                .await;
            Self::observe(&table, &results);

            self.in_flight.remove(&table);
            AUTO_REPAIR_TABLES_REPAIRED.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Aggregate + loudly report one table's session results (FMEA #7).
    fn observe(table: &TableId, results: &[SessionResult]) {
        let mut ok = 0u64;
        let mut failed = 0u64;
        let mut streamed = 0u64;
        let mut ties = 0u64;
        for r in results {
            match &r.result {
                Ok(stats) => {
                    ok += 1;
                    streamed += stats.partitions_streamed_in + stats.partitions_streamed_out;
                    ties += stats.timestamp_ties;
                }
                Err(e) => {
                    failed += 1;
                    tracing::warn!(
                        keyspace = table.keyspace(),
                        table = table.table(),
                        peer = r.peer,
                        range_start = r.range_start,
                        range_end = r.range_end,
                        error = %e,
                        "auto-repair: session failed"
                    );
                }
            }
        }
        AUTO_REPAIR_SESSIONS_OK.fetch_add(ok, Ordering::Relaxed);
        AUTO_REPAIR_SESSIONS_FAILED.fetch_add(failed, Ordering::Relaxed);
        AUTO_REPAIR_PARTITIONS_STREAMED.fetch_add(streamed, Ordering::Relaxed);
        AUTO_REPAIR_TIMESTAMP_TIES.fetch_add(ties, Ordering::Relaxed);

        if streamed > 0 || ties > 0 {
            // Divergence found — WARN so a recurring divergence is operator-visible.
            tracing::warn!(
                keyspace = table.keyspace(),
                table = table.table(),
                sessions_ok = ok,
                sessions_failed = failed,
                partitions_streamed = streamed,
                timestamp_ties = ties,
                "auto-repair: DIVERGENCE reconciled for table"
            );
        } else {
            tracing::info!(
                keyspace = table.keyspace(),
                table = table.table(),
                sessions_ok = ok,
                sessions_failed = failed,
                "auto-repair: table converged (no divergence)"
            );
        }
    }

    /// Sub-tick period that covers the whole table set once per `interval`:
    /// `interval / ceil(table_count / max_concurrent_tables)`. With 0 tables (or
    /// `max_concurrent_tables == 0`) there is nothing to spread, so wait one full
    /// interval. Pure helper so the cadence math is unit-testable.
    pub fn sub_tick(interval: Duration, table_count: usize, max_concurrent: usize) -> Duration {
        if table_count == 0 || max_concurrent == 0 {
            return interval;
        }
        let sub_ticks = table_count.div_ceil(max_concurrent) as u32;
        // div_ceil of a positive count is >= 1, so this never divides by zero.
        interval / sub_ticks.max(1)
    }

    /// Spawn the scheduler's background loop.
    ///
    /// When `cfg.enabled` is false, logs that auto-repair is disabled and returns
    /// a **no-op** handle that finishes immediately — the loop never starts (the
    /// manual `POST /repair` endpoint still works). Otherwise loops on a
    /// `tokio::time::interval` sized so the whole live table set is covered once
    /// per `cfg.interval`, recomputing the sub-tick from the live table count
    /// each cycle, until `shutdown` flips to `true` (or its sender drops).
    pub fn spawn(
        mut scheduler: AutoRepairScheduler,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        if !scheduler.cfg.enabled {
            tracing::info!(
                "auto-repair: disabled (FERROSA_AUTO_REPAIR_ENABLED=off); scheduler not started"
            );
            // No-op handle: do NOT enter the loop.
            return TaskPool::current("auto-repair").spawn(async move {});
        }

        let interval_cfg = scheduler.cfg.interval;
        let max_concurrent = scheduler.cfg.max_concurrent_tables;
        tracing::info!(
            interval_secs = interval_cfg.as_secs(),
            max_concurrent_tables = max_concurrent,
            "auto-repair: scheduler started"
        );

        TaskPool::current("auto-repair").spawn(async move {
            loop {
                // Recompute the sub-tick from the live table count each cycle so
                // membership/schema churn re-spreads coverage (design: cadence).
                let table_count = scheduler.ctx.user_tables().len();
                let period = AutoRepairScheduler::sub_tick(interval_cfg, table_count, max_concurrent);
                tokio::select! {
                    result = shutdown.changed() => {
                        if result.is_err() || *shutdown.borrow() {
                            tracing::info!("auto-repair: shutting down");
                            break;
                        }
                    }
                    _ = tokio::time::sleep(period) => {
                        scheduler.run_tick().await;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{NodeInfo, NodeState};
    use uuid::Uuid;

    fn node(addr: &str, host_id: u128, state: NodeState) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::from_u128(host_id),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state,
            cql_broadcast: None,
        }
    }

    /// 3-node ring, RF=3, with FIXED host_ids (1 < 2 < 3 by UUID order) so the
    /// initiator is deterministic and assertable. node_id N has host_id N.
    fn three_node_ring() -> TokenRing {
        let mut ring = TokenRing::new();
        ring.add_node(1, node("10.0.0.1:7000", 1, NodeState::Normal));
        ring.add_node(2, node("10.0.0.2:7000", 2, NodeState::Normal));
        ring.add_node(3, node("10.0.0.3:7000", 3, NodeState::Normal));
        ring.assign_tokens(1, &[100, 400, 700]);
        ring.assign_tokens(2, &[200, 500, 800]);
        ring.assign_tokens(3, &[300, 600, 900]);
        ring
    }

    /// FMEA #1 (thundering herd): with RF=3 on a 3-node ring every range's
    /// replica set is {1,2,3}, so the lowest-host_id node (node 1) is the sole
    /// initiator. node 1 selects every owned range; nodes 2 and 3 select none.
    #[test]
    fn exactly_one_initiator_per_range_no_herd() {
        let ring = three_node_ring();

        let n1 = select_initiated_ranges(&ring, 1, 3);
        let n2 = select_initiated_ranges(&ring, 2, 3);
        let n3 = select_initiated_ranges(&ring, 3, 3);

        assert!(!n1.is_empty(), "lowest-host_id node must initiate its ranges");
        assert!(n2.is_empty(), "node 2 must NOT initiate (node 1 is lower)");
        assert!(n3.is_empty(), "node 3 must NOT initiate (node 1 is lower)");

        // Every initiated range repairs against the other two replicas.
        for r in &n1 {
            let mut peers = r.peers.clone();
            peers.sort_unstable();
            assert_eq!(peers, vec![2, 3], "RF=3 peers = the two non-self replicas");
            assert!(r.start < r.end, "range must be non-empty [start,end)");
        }
    }

    /// FMEA #4 (determinism): same ring → identical selection, every call.
    #[test]
    fn selection_is_deterministic() {
        let ring = three_node_ring();
        let a = select_initiated_ranges(&ring, 1, 3);
        let b = select_initiated_ranges(&ring, 1, 3);
        assert_eq!(a, b);
    }

    /// FMEA #5 (membership churn): if the lowest-host_id node goes down, the
    /// next-lowest LIVE replica becomes the initiator — no range is orphaned and
    /// no two nodes initiate the same range.
    #[test]
    fn initiator_fails_over_when_lowest_host_down() {
        let mut ring = three_node_ring();
        // Take node 1 (lowest host_id) out of the live set.
        ring.set_node_state(1, NodeState::Leaving);

        let n2 = select_initiated_ranges(&ring, 2, 3);
        let n3 = select_initiated_ranges(&ring, 3, 3);

        assert!(
            !n2.is_empty(),
            "node 2 (now lowest live host_id) must take over initiation"
        );
        assert!(n3.is_empty(), "node 3 still defers to node 2");
        // The down node is never a repair peer.
        for r in &n2 {
            assert!(!r.peers.contains(&1), "down node must not be a peer");
        }
    }

    /// Single-node ring (or RF=1): no peers to reconcile → initiate nothing.
    #[test]
    fn single_node_selects_nothing() {
        let mut ring = TokenRing::new();
        ring.add_node(1, node("10.0.0.1:7000", 1, NodeState::Normal));
        ring.assign_tokens(1, &[100, 400, 700]);
        assert!(select_initiated_ranges(&ring, 1, 1).is_empty());
        assert!(select_initiated_ranges(&ring, 1, 3).is_empty());
    }

    /// A node not yet in the ring initiates nothing (no host_id to compare).
    #[test]
    fn unknown_local_node_selects_nothing() {
        let ring = three_node_ring();
        assert!(select_initiated_ranges(&ring, 999, 3).is_empty());
    }

    fn tid(ks: &str, t: &str) -> TableId {
        TableId::new(ks, t)
    }

    #[test]
    fn config_defaults_are_self_managing() {
        let c = AutoRepairConfig::default();
        assert!(c.enabled, "auto-repair on by default");
        assert_eq!(c.interval, Duration::from_secs(86_400), "24h default");
        assert_eq!(c.max_concurrent_tables, 1);
        assert!(c.is_skipped(&tid("system_auth", "roles")), "system* skipped");
        assert!(!c.is_skipped(&tid("app", "users")), "user keyspace not skipped");
    }

    #[test]
    fn select_tables_skips_system_and_in_flight_round_robin() {
        let cfg = AutoRepairConfig::default();
        let tables = vec![
            tid("system", "local"),
            tid("app", "t1"),
            tid("app", "t2"),
            tid("app", "t3"),
        ];
        let mut in_flight = HashSet::new();
        in_flight.insert(tid("app", "t1"));

        // max 2: skip system.local (system*) and app.t1 (in-flight) → t2, t3.
        let (picked, _next) = select_tables_for_tick(&tables, 0, 2, &in_flight, &cfg);
        assert_eq!(picked, vec![tid("app", "t2"), tid("app", "t3")]);
    }

    #[test]
    fn select_tables_round_robin_covers_all_over_ticks() {
        let cfg = AutoRepairConfig::default();
        let tables = vec![tid("app", "a"), tid("app", "b"), tid("app", "c")];
        let empty = HashSet::new();

        // One table per tick (default max_concurrent_tables=1).
        let (p0, c0) = select_tables_for_tick(&tables, 0, 1, &empty, &cfg);
        let (p1, c1) = select_tables_for_tick(&tables, c0, 1, &empty, &cfg);
        let (p2, _c2) = select_tables_for_tick(&tables, c1, 1, &empty, &cfg);

        let mut covered: Vec<TableId> = Vec::new();
        covered.extend(p0);
        covered.extend(p1);
        covered.extend(p2);
        covered.sort_by(|a, b| a.table().cmp(b.table()));
        assert_eq!(covered, tables, "three ticks cover every table exactly once");
    }

    #[test]
    fn select_tables_empty_or_all_skipped_is_safe() {
        let cfg = AutoRepairConfig::default();
        let empty = HashSet::new();
        assert!(select_tables_for_tick(&[], 0, 1, &empty, &cfg).0.is_empty());
        // All-system set → nothing picked, no infinite scan.
        let sys = vec![tid("system", "a"), tid("system_auth", "b")];
        assert!(select_tables_for_tick(&sys, 0, 4, &empty, &cfg).0.is_empty());
    }

    // -- AutoRepairScheduler / RepairContext tests (no real IO) -------------

    use async_trait::async_trait;
    use std::sync::Mutex;

    /// Recording mock executor — mirrors `coordinator.rs`'s MockExecutor. Records
    /// every `(table, range, peer)` invocation so a test can assert exactly which
    /// tables/ranges a tick scheduled, with zero IO.
    struct RecordingExecutor {
        calls: Mutex<Vec<(TableId, i64, i64, u64)>>,
    }

    impl RecordingExecutor {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
            }
        }
        fn tables_seen(&self) -> Vec<TableId> {
            let mut v: Vec<TableId> = self
                .calls
                .lock()
                .unwrap()
                .iter()
                .map(|(t, _, _, _)| t.clone())
                .collect();
            v.sort_by(|a, b| a.table().cmp(b.table()));
            v.dedup();
            v
        }
    }

    #[async_trait]
    impl SessionExecutor for RecordingExecutor {
        async fn run_session(
            &self,
            table: &TableId,
            range_start: i64,
            range_end: i64,
            peer: u64,
        ) -> Result<crate::repair::SessionStats, String> {
            self.calls
                .lock()
                .unwrap()
                .push((table.clone(), range_start, range_end, peer));
            Ok(crate::repair::SessionStats::default())
        }
    }

    /// Mock context returning a fixed ring + a shared recording executor + a
    /// fixed table list, or `None` for any field when `ready == false`.
    struct MockContext {
        ring: TokenRing,
        local_node_id: u64,
        exec: Arc<RecordingExecutor>,
        tables: Vec<(TableId, usize)>,
        ready: bool,
    }

    impl RepairContext for MockContext {
        fn token_ring(&self) -> Option<TokenRing> {
            self.ready.then(|| self.ring.clone())
        }
        fn local_node_id(&self) -> Option<u64> {
            self.ready.then_some(self.local_node_id)
        }
        fn build_executor(&self) -> Option<Arc<dyn SessionExecutor>> {
            self.ready
                .then(|| self.exec.clone() as Arc<dyn SessionExecutor>)
        }
        fn user_tables(&self) -> Vec<(TableId, usize)> {
            self.tables.clone()
        }
    }

    /// Build a ready MockContext where the local node is the lowest-host_id node
    /// (node 1), so `repair_initiated` selects ranges and the executor records work.
    fn ready_ctx(tables: Vec<(TableId, usize)>) -> (Arc<MockContext>, Arc<RecordingExecutor>) {
        let exec = Arc::new(RecordingExecutor::new());
        let ctx = Arc::new(MockContext {
            ring: three_node_ring(),
            local_node_id: 1,
            exec: exec.clone(),
            tables,
            ready: true,
        });
        (ctx, exec)
    }

    /// run_tick on a ready node repairs exactly the round-robin-selected table(s)
    /// and skips ones already in-flight; the cursor advances across ticks.
    #[tokio::test]
    async fn run_tick_repairs_selected_tables_and_advances_cursor() {
        let tables = vec![
            (tid("app", "a"), 3usize),
            (tid("app", "b"), 3),
            (tid("app", "c"), 3),
        ];
        let (ctx, exec) = ready_ctx(tables);
        let cfg = AutoRepairConfig {
            max_concurrent_tables: 1,
            ..AutoRepairConfig::default()
        };
        let mut sched = AutoRepairScheduler::new(RepairCoordinator::default(), ctx, cfg);

        sched.run_tick().await; // tick 1 -> a
        sched.run_tick().await; // tick 2 -> b
        sched.run_tick().await; // tick 3 -> c

        // Each table repaired exactly once across three ticks (cursor advanced).
        assert_eq!(
            exec.tables_seen(),
            vec![tid("app", "a"), tid("app", "b"), tid("app", "c")],
            "three ticks cover every table once, in round-robin order"
        );
    }

    /// A table marked in-flight (by an external/manual repair) is skipped (FMEA #6).
    /// We pre-seed the scheduler's in-flight set via the public selection helper:
    /// run a tick with `app.a` in flight and confirm only `app.b` runs.
    #[tokio::test]
    async fn run_tick_skips_in_flight_table() {
        let tables = vec![(tid("app", "a"), 3usize), (tid("app", "b"), 3)];
        let (ctx, exec) = ready_ctx(tables);
        let cfg = AutoRepairConfig {
            max_concurrent_tables: 2, // would pick BOTH if nothing were in-flight
            ..AutoRepairConfig::default()
        };
        let mut sched = AutoRepairScheduler::new(RepairCoordinator::default(), ctx, cfg);
        sched.in_flight.insert(tid("app", "a"));

        sched.run_tick().await;

        assert_eq!(
            exec.tables_seen(),
            vec![tid("app", "b")],
            "in-flight app.a is skipped; only app.b is repaired"
        );
    }

    /// Context not ready (any field None) → run_tick is a no-op (no sessions).
    #[tokio::test]
    async fn run_tick_no_ops_when_context_not_ready() {
        let exec = Arc::new(RecordingExecutor::new());
        let ctx = Arc::new(MockContext {
            ring: three_node_ring(),
            local_node_id: 1,
            exec: exec.clone(),
            tables: vec![(tid("app", "a"), 3)],
            ready: false, // not in cluster mode / not placed in ring
        });
        let mut sched =
            AutoRepairScheduler::new(RepairCoordinator::default(), ctx, AutoRepairConfig::default());

        sched.run_tick().await;

        assert!(
            exec.calls.lock().unwrap().is_empty(),
            "not-ready context must schedule zero sessions"
        );
    }

    /// Disabled config → spawn returns a handle that finishes immediately without
    /// ever entering the loop (no tick runs even if we wait).
    #[tokio::test]
    async fn spawn_disabled_is_no_op() {
        let tables = vec![(tid("app", "a"), 3usize)];
        let (ctx, exec) = ready_ctx(tables);
        let cfg = AutoRepairConfig {
            enabled: false,
            interval: Duration::from_millis(1), // tiny — would fire fast IF it looped
            ..AutoRepairConfig::default()
        };
        let sched = AutoRepairScheduler::new(RepairCoordinator::default(), ctx, cfg);
        let (_tx, rx) = tokio::sync::watch::channel(false);

        let handle = AutoRepairScheduler::spawn(sched, rx);
        // A no-op handle completes on its own; join it (bounded, no hang).
        handle.await.expect("disabled scheduler task joins cleanly");

        assert!(
            exec.calls.lock().unwrap().is_empty(),
            "disabled scheduler must never run a tick"
        );
    }

    /// Enabled scheduler loop runs at least one tick, then stops on shutdown.
    #[tokio::test]
    async fn spawn_enabled_ticks_then_shuts_down() {
        let tables = vec![(tid("app", "a"), 3usize)];
        let (ctx, exec) = ready_ctx(tables);
        // interval=10ms, 1 table, max_concurrent=1 → sub_tick = 10ms.
        let cfg = AutoRepairConfig {
            enabled: true,
            interval: Duration::from_millis(10),
            max_concurrent_tables: 1,
            ..AutoRepairConfig::default()
        };
        let sched = AutoRepairScheduler::new(RepairCoordinator::default(), ctx, cfg);
        let (tx, rx) = tokio::sync::watch::channel(false);

        let handle = AutoRepairScheduler::spawn(sched, rx);
        // Give the loop time to fire at least one sub-tick.
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(true).expect("shutdown signal sends");
        handle.await.expect("scheduler task joins after shutdown");

        assert!(
            !exec.calls.lock().unwrap().is_empty(),
            "enabled scheduler ran at least one repair tick before shutdown"
        );
    }

    /// sub_tick spreads the table set across `interval`, and degrades to one full
    /// interval when there is nothing to spread.
    #[test]
    fn sub_tick_spreads_table_set_over_interval() {
        let iv = Duration::from_secs(24);
        // 4 tables, 1 at a time → 4 sub-ticks of 6s each.
        assert_eq!(
            AutoRepairScheduler::sub_tick(iv, 4, 1),
            Duration::from_secs(6)
        );
        // 4 tables, 2 at a time → ceil(4/2)=2 sub-ticks of 12s.
        assert_eq!(
            AutoRepairScheduler::sub_tick(iv, 4, 2),
            Duration::from_secs(12)
        );
        // 5 tables, 2 at a time → ceil(5/2)=3 sub-ticks of 8s.
        assert_eq!(AutoRepairScheduler::sub_tick(iv, 5, 2), iv / 3);
        // 0 tables → one full interval (nothing to spread, don't divide by zero).
        assert_eq!(AutoRepairScheduler::sub_tick(iv, 0, 1), iv);
        // 0 max_concurrent → one full interval.
        assert_eq!(AutoRepairScheduler::sub_tick(iv, 4, 0), iv);
    }

    /// Metrics render in Prometheus format and a tick bumps the tick counter.
    #[test]
    fn metrics_render_and_count_ticks() {
        inc_auto_repair_tick();
        let text = render_prometheus();
        assert!(text.contains("ferrosa_auto_repair_ticks_total"));
        assert!(text.contains("ferrosa_auto_repair_sessions_failed_total"));
        assert!(text.contains("ferrosa_auto_repair_skipped_not_ready_total"));
    }
}
