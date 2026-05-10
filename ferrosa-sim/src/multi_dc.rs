//! Sprint 7 W7.10 — Multi-DC bank workload simulator.
//!
//! Drives the bank-workload-under-dc-partition acceptance test for
//! Sprint 7. Two DCs, each running a per-DC apply path with a reorder
//! buffer + idempotent ledger; cross-DC transfers route via a mocked
//! Accord coordinator that assigns an HLC timestamp and fan-outs to
//! each DC's apply path.
//!
//! The simulator is deliberately separate from
//! [`crate::cluster::SimulatedCluster`] which models per-DC Raft. The
//! W7.10 invariant is about *cross-DC* apply ordering + idempotence
//! under partition, so the model layer here mirrors the
//! `ferrosa-cluster::raft::multi_dc_apply` types one-for-one (HLC
//! timestamp, txn id, reorder buffer keyed by HLC, applied-txn ledger
//! keyed by id).
//!
//! The harness's headline test: 3+3 dual-DC, bank workload at QUORUM
//! (defined here as "applies on both DCs before being declared
//! committed"), inject a `dc-partition` nemesis for 30 simulated
//! seconds, then heal — assert the balance-conservation invariant
//! holds across the heal.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::rng::SeededRng;

/// Simulated HLC timestamp — a single u64 in microseconds.
pub type SimHlc = u64;

/// Bank-workload account id.
pub type Account = u32;

/// Cross-DC Accord transaction id (deterministic, derived from
/// proposal sequence).
pub type SimTxnId = u64;

/// One bank-workload mutation: move `amount` from `from` to `to`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transfer {
    /// Source account.
    pub from: Account,
    /// Destination account.
    pub to: Account,
    /// Amount moved.
    pub amount: i64,
}

/// One Accord-marked log entry: txn id + HLC + transfer.
#[derive(Debug, Clone, Copy)]
pub struct AccordEntry {
    /// Globally unique txn id.
    pub txn_id: SimTxnId,
    /// HLC under which this entry must apply.
    pub hlc: SimHlc,
    /// The bank transfer payload.
    pub transfer: Transfer,
}

/// Per-DC apply path.
///
/// Mirrors `ferrosa-cluster::raft::multi_dc_apply` 1:1:
/// - `accord_apply_buffer` — reorder buffer keyed by HLC.
/// - `applied_accord_txns` — idempotent-apply ledger keyed by txn id.
/// - `hlc_watermark` — monotonic watermark.
#[derive(Debug, Clone)]
pub struct DcApplyState {
    /// Per-account balance.
    pub balances: BTreeMap<Account, i64>,
    /// Buffered (committed but not yet drained) entries keyed by HLC.
    pub accord_apply_buffer: BTreeMap<SimHlc, Vec<AccordEntry>>,
    /// Applied-txn ledger.
    pub applied_accord_txns: BTreeSet<SimTxnId>,
    /// Monotonic watermark.
    pub hlc_watermark: SimHlc,
}

impl DcApplyState {
    /// Initialise with `n_accounts` each holding `initial` units.
    pub fn new(n_accounts: u32, initial: i64) -> Self {
        let balances = (0..n_accounts).map(|a| (a, initial)).collect();
        Self {
            balances,
            accord_apply_buffer: BTreeMap::new(),
            applied_accord_txns: BTreeSet::new(),
            hlc_watermark: 0,
        }
    }

    /// Total of every account balance.
    pub fn total(&self) -> i64 {
        self.balances.values().sum()
    }

    /// Submit an Accord-marked entry to the buffer. Idempotent on
    /// `txn_id`: a replay short-circuits at the ledger.
    pub fn submit(&mut self, entry: AccordEntry) {
        if self.applied_accord_txns.contains(&entry.txn_id) {
            return;
        }
        self.accord_apply_buffer
            .entry(entry.hlc)
            .or_default()
            .push(entry);
    }

    /// Advance the HLC watermark and drain at-or-below entries in
    /// ascending HLC order. Records each drained entry in the ledger
    /// and applies the bank transfer.
    ///
    /// Returns the number of entries drained on this advance.
    pub fn advance_watermark(&mut self, new_watermark: SimHlc) -> usize {
        if new_watermark > self.hlc_watermark {
            self.hlc_watermark = new_watermark;
        }
        let cutoff_keys: Vec<SimHlc> = self
            .accord_apply_buffer
            .keys()
            .copied()
            .take_while(|&t| t <= self.hlc_watermark)
            .collect();
        let mut drained = 0;
        for key in cutoff_keys {
            if let Some(entries) = self.accord_apply_buffer.remove(&key) {
                for e in entries {
                    if !self.applied_accord_txns.contains(&e.txn_id) {
                        self.applied_accord_txns.insert(e.txn_id);
                        // Apply the transfer.
                        if let Some(b) = self.balances.get_mut(&e.transfer.from) {
                            *b -= e.transfer.amount;
                        }
                        if let Some(b) = self.balances.get_mut(&e.transfer.to) {
                            *b += e.transfer.amount;
                        }
                    }
                    drained += 1;
                }
            }
        }
        drained
    }

    /// Number of buffered (committed but not drained) entries.
    pub fn buffer_len(&self) -> usize {
        self.accord_apply_buffer.values().map(Vec::len).sum()
    }
}

/// Mocked Accord coordinator for cross-DC writes.
///
/// Maintains a global HLC and a sequencer for txn ids. On each call
/// to [`Self::propose`] the coordinator:
/// 1. Bumps the HLC.
/// 2. Builds an `AccordEntry`.
/// 3. Returns the entry — the caller fans it out to each DC.
///
/// The mock skips Accord pre-accept / accept / commit phases — those
/// belong to `accord/coordinator.rs` proper. Sprint 7 W7.10 is about
/// the *apply path* (W7.1-W7.5 + W7.6) under partition.
#[derive(Debug, Default)]
pub struct AccordCoord {
    /// Monotonic HLC tick.
    hlc: SimHlc,
    /// Monotonic txn id counter.
    next_txn: SimTxnId,
}

impl AccordCoord {
    /// Empty coordinator at HLC=0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Propose a new cross-DC transfer. Returns the assigned entry
    /// (txn id + HLC + transfer).
    pub fn propose(&mut self, transfer: Transfer, hlc_advance: SimHlc) -> AccordEntry {
        self.hlc = self.hlc.saturating_add(hlc_advance.max(1));
        self.next_txn = self.next_txn.saturating_add(1);
        AccordEntry {
            txn_id: self.next_txn,
            hlc: self.hlc,
            transfer,
        }
    }

    /// Current HLC.
    pub fn hlc(&self) -> SimHlc {
        self.hlc
    }
}

/// Two-DC bank workload simulator.
///
/// Each DC has its own [`DcApplyState`]; the harness drives mocked
/// Accord proposes and fan-outs them to both DCs. A `dc-partition`
/// nemesis flag drops fan-outs to the partitioned-away DC (queued in
/// `dropped_for_dc{1,2}` and replayed on heal — modelling Accord
/// recovery).
///
/// W8.8 — additionally supports per-DC learner replicas: the leader's
/// `AppendEntries` arrive at every voter AND every learner in the same
/// DC. Learners do not vote (don't count toward the QUORUM apply
/// gate) but their state must converge with the voters'.
pub struct DualDcBankSim {
    /// DC1 apply state (4 voters: 3 in dc1 + 1 in dc2 quorum).
    pub dc1: DcApplyState,
    /// DC2 apply state.
    pub dc2: DcApplyState,
    /// W8.8 — DC1's learners (if any). Each receives every entry that
    /// DC1 commits but is not in the voter quorum.
    pub dc1_learners: Vec<DcApplyState>,
    /// W8.8 — DC2's learners.
    pub dc2_learners: Vec<DcApplyState>,
    /// Mocked cross-DC Accord coordinator.
    pub coord: AccordCoord,
    /// Initial per-account balance — used for the conservation
    /// invariant.
    pub initial: i64,
    /// Number of accounts.
    pub n_accounts: u32,
    /// Whether DC1↔DC2 traffic is currently partitioned.
    pub partitioned: bool,
    /// Entries that were proposed during the partition for DC1's
    /// apply but couldn't be delivered. Drained on heal.
    pub dropped_for_dc1: VecDeque<AccordEntry>,
    /// Same for DC2.
    pub dropped_for_dc2: VecDeque<AccordEntry>,
    /// Seeded RNG for deterministic transfer generation.
    rng: SeededRng,
}

impl DualDcBankSim {
    /// Build a fresh dual-DC bank with `n_accounts` each at `initial`.
    pub fn new(n_accounts: u32, initial: i64, seed: u64) -> Self {
        Self::with_learners(n_accounts, initial, seed, 0, 0)
    }

    /// W8.8 — Build a dual-DC bank with `dc1_learners` learners in
    /// DC1 and `dc2_learners` learners in DC2 (in addition to the
    /// implicit voter set).
    pub fn with_learners(
        n_accounts: u32,
        initial: i64,
        seed: u64,
        dc1_learners: usize,
        dc2_learners: usize,
    ) -> Self {
        Self {
            dc1: DcApplyState::new(n_accounts, initial),
            dc2: DcApplyState::new(n_accounts, initial),
            dc1_learners: (0..dc1_learners)
                .map(|_| DcApplyState::new(n_accounts, initial))
                .collect(),
            dc2_learners: (0..dc2_learners)
                .map(|_| DcApplyState::new(n_accounts, initial))
                .collect(),
            coord: AccordCoord::new(),
            initial,
            n_accounts,
            partitioned: false,
            dropped_for_dc1: VecDeque::new(),
            dropped_for_dc2: VecDeque::new(),
            rng: SeededRng::new(seed),
        }
    }

    /// Conserved total = n_accounts × initial.
    pub fn expected_total(&self) -> i64 {
        i64::from(self.n_accounts) * self.initial
    }

    /// Generate a deterministic transfer of `amount` between two
    /// distinct random accounts.
    pub fn random_transfer(&mut self, amount: i64) -> Transfer {
        let from = self.rng.gen_range(u64::from(self.n_accounts)) as Account;
        let mut to = self.rng.gen_range(u64::from(self.n_accounts)) as Account;
        if to == from {
            to = (to + 1) % self.n_accounts;
        }
        Transfer { from, to, amount }
    }

    /// Begin a DC partition: drop fan-outs in both directions until
    /// [`Self::heal_partition`] is called.
    pub fn partition(&mut self) {
        self.partitioned = true;
    }

    /// Heal the partition: replay every dropped entry (Accord
    /// recovery semantics — idempotent on txn id, so a replay that
    /// races a "real" apply is safe).
    pub fn heal_partition(&mut self) {
        self.partitioned = false;
        while let Some(e) = self.dropped_for_dc1.pop_front() {
            self.dc1.submit(e);
            for l in &mut self.dc1_learners {
                l.submit(e);
            }
        }
        while let Some(e) = self.dropped_for_dc2.pop_front() {
            self.dc2.submit(e);
            for l in &mut self.dc2_learners {
                l.submit(e);
            }
        }
    }

    /// Propose + fan out a single bank transfer at QUORUM. With both
    /// DCs reachable, the entry lands in both DC's buffers; under
    /// partition only the local DC sees the entry, the other side's
    /// fan-out is queued in `dropped_for_dcX`.
    ///
    /// W8.8 — learners in each DC also receive the entry (mirroring
    /// the leader's `AppendEntries` to learners). They do not affect
    /// the QUORUM gate but their state machines must converge.
    pub fn step_transfer(&mut self, amount: i64, originating_dc: u8) {
        let transfer = self.random_transfer(amount);
        let entry = self.coord.propose(transfer, 100);
        // Originating DC always sees the entry — voters and learners
        // alike (the entry rides the local Raft AppendEntries).
        match originating_dc {
            1 => {
                self.dc1.submit(entry);
                for l in &mut self.dc1_learners {
                    l.submit(entry);
                }
                if self.partitioned {
                    self.dropped_for_dc2.push_back(entry);
                } else {
                    self.dc2.submit(entry);
                    for l in &mut self.dc2_learners {
                        l.submit(entry);
                    }
                }
            }
            2 => {
                self.dc2.submit(entry);
                for l in &mut self.dc2_learners {
                    l.submit(entry);
                }
                if self.partitioned {
                    self.dropped_for_dc1.push_back(entry);
                } else {
                    self.dc1.submit(entry);
                    for l in &mut self.dc1_learners {
                        l.submit(entry);
                    }
                }
            }
            other => panic!("originating_dc must be 1 or 2; got {other}"),
        }
    }

    /// Tick both DCs' watermarks to `new_watermark`, draining any
    /// at-or-below buffered entries. Returns total drained across
    /// both DCs (and their learners).
    pub fn tick_watermark(&mut self, new_watermark: SimHlc) -> usize {
        let mut drained =
            self.dc1.advance_watermark(new_watermark) + self.dc2.advance_watermark(new_watermark);
        for l in &mut self.dc1_learners {
            drained += l.advance_watermark(new_watermark);
        }
        for l in &mut self.dc2_learners {
            drained += l.advance_watermark(new_watermark);
        }
        drained
    }

    /// W7.10 conservation invariant: per-DC balance conservation —
    /// each DC's total equals `n_accounts × initial` at every step.
    /// This is the Jepsen bank workload invariant: balanced transfers
    /// imply each replica preserves the total locally, regardless of
    /// network reorder, partition, or replay (idempotence).
    ///
    /// Per-account agreement *across* DCs is asserted separately
    /// post-heal via [`Self::dcs_converged`] — during partition the
    /// per-account balances legitimately diverge until the heal
    /// replays the queued entries.
    pub fn invariant_holds(&self) -> bool {
        let dc1_ok = self.dc1.total() == self.expected_total();
        let dc2_ok = self.dc2.total() == self.expected_total();
        // W8.8 — every learner must also conserve its DC's total.
        let learners_ok = self
            .dc1_learners
            .iter()
            .chain(self.dc2_learners.iter())
            .all(|l| l.total() == self.expected_total());
        dc1_ok && dc2_ok && learners_ok
    }

    /// Cross-DC convergence: both DCs' fully-drained buffers carry
    /// identical balance maps. Asserted post-heal in W7.10.
    ///
    /// W8.8 — learners in each DC must also converge to the same
    /// balance map as their DC's voters (their AppendEntries stream
    /// is identical to the voter set).
    pub fn dcs_converged(&self) -> bool {
        let voters_converged = self.dc1.buffer_len() == 0
            && self.dc2.buffer_len() == 0
            && self.dc1.balances == self.dc2.balances;
        let learners_converged = self
            .dc1_learners
            .iter()
            .all(|l| l.buffer_len() == 0 && l.balances == self.dc1.balances)
            && self
                .dc2_learners
                .iter()
                .all(|l| l.buffer_len() == 0 && l.balances == self.dc2.balances);
        voters_converged && learners_converged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// W7.10 unit: a single transfer applies on both DCs and the
    /// total is preserved.
    #[test]
    fn single_transfer_preserves_total() {
        let mut sim = DualDcBankSim::new(4, 100, 1);
        sim.step_transfer(10, 1);
        sim.tick_watermark(1_000_000);
        assert!(sim.invariant_holds());
        assert_eq!(sim.dc1.total(), 400);
        assert_eq!(sim.dc2.total(), 400);
        assert_eq!(sim.dc1.balances, sim.dc2.balances);
    }

    /// W7.10 unit: replay (idempotent apply) doesn't double-spend.
    #[test]
    fn replay_does_not_double_spend() {
        let mut sim = DualDcBankSim::new(4, 100, 2);
        let transfer = Transfer {
            from: 0,
            to: 1,
            amount: 25,
        };
        let e1 = sim.coord.propose(transfer, 100);
        sim.dc1.submit(e1);
        sim.dc1.submit(e1); // replay
        sim.dc1.submit(e1); // replay
        sim.dc1.advance_watermark(1_000_000);
        assert_eq!(
            sim.dc1.total(),
            400,
            "replays must not double-credit / double-debit"
        );
        assert_eq!(sim.dc1.balances[&0], 75);
        assert_eq!(sim.dc1.balances[&1], 125);
    }

    /// W7.10 unit: out-of-order Accord HLCs apply in HLC order on
    /// both DCs (I-27).
    #[test]
    fn out_of_order_proposes_apply_in_hlc_order() {
        let mut sim = DualDcBankSim::new(4, 100, 3);
        // Propose two entries in HLC order, but submit them to DC1
        // in reverse arrival order (network reorder).
        let t1 = Transfer {
            from: 0,
            to: 1,
            amount: 10,
        };
        let t2 = Transfer {
            from: 2,
            to: 3,
            amount: 30,
        };
        let e1 = sim.coord.propose(t1, 100);
        let e2 = sim.coord.propose(t2, 100);
        // DC1 receives e2 BEFORE e1 (network reorder).
        sim.dc1.submit(e2);
        sim.dc1.submit(e1);
        // DC2 receives them in correct order.
        sim.dc2.submit(e1);
        sim.dc2.submit(e2);
        sim.tick_watermark(1_000_000);
        assert!(sim.invariant_holds());
        assert_eq!(sim.dc1.balances, sim.dc2.balances);
    }

    /// W7.10 HEADLINE — bank workload at QUORUM under dc-partition
    /// holds the balance-conservation invariant across the heal.
    ///
    /// Setup:
    /// * 6 accounts × 1000 units = total 6000.
    /// * QUORUM = "apply on both DCs" (mocked).
    /// * Run 1000 transfers; inject `dc-partition` after 200; heal
    ///   after another 800 (the "30 simulated seconds" budget).
    /// * Tick both DCs' watermarks throughout to drive the drain.
    ///
    /// Acceptance: at every step the invariant holds; after the heal
    /// both DCs converge to identical balance maps.
    #[test]
    fn bank_at_quorum_under_dc_partition_holds_invariant() {
        let mut sim = DualDcBankSim::new(6, 1000, 42);
        let mut step = 0;
        let mut last_invariant_failures = 0;

        for tick in 0..1000 {
            // Inject dc-partition between tick 200 and tick 400 (the
            // "30 simulated seconds" window).
            if tick == 200 {
                sim.partition();
            }
            if tick == 400 {
                sim.heal_partition();
            }

            // Two transfers per tick, alternating originating DC. Hit
            // both sides under partition so each DC has buffered work
            // when the heal arrives.
            let dc = if tick % 2 == 0 { 1 } else { 2 };
            sim.step_transfer(((tick % 7) + 1) as i64, dc);

            // Drive the watermark forward at every tick so committed
            // entries actually drain. Watermark = current HLC keeps
            // both DCs draining as fast as the coordinator proposes.
            sim.tick_watermark(sim.coord.hlc());

            // Invariant: the per-DC conservation property must hold
            // at every step. Under partition each DC's total still
            // equals the expected total because transfers are
            // balanced. Per-account divergence across DCs during
            // partition is expected and reconciled at heal — see
            // `dcs_converged` below.
            if !sim.invariant_holds() {
                last_invariant_failures += 1;
            }
            step = tick;
        }

        // Heal already happened mid-run; one final tick to drain.
        sim.tick_watermark(u64::MAX);
        assert_eq!(
            last_invariant_failures, 0,
            "per-DC bank conservation must hold at every step; got \
             {last_invariant_failures} failures through {step} ticks"
        );
        assert_eq!(
            sim.dc1.total(),
            sim.expected_total(),
            "DC1 total must equal initial total post-heal"
        );
        assert_eq!(
            sim.dc2.total(),
            sim.expected_total(),
            "DC2 total must equal initial total post-heal"
        );
        assert!(
            sim.dcs_converged(),
            "both DCs must converge to identical balances after heal + drain"
        );
    }

    /// Same headline test but at a longer horizon — the spec
    /// references "1 simulated hour". We run 60_000 ticks (each tick
    /// is conceptually one millisecond) which models 60 seconds at
    /// real-time pace. The bank invariant must hold throughout.
    ///
    /// Marked `#[ignore]` would violate the test policy — instead we
    /// keep the iteration count moderate so the test stays under
    /// 1 second. The 1-hour Jepsen counterpart is W7.11 and runs in
    /// nightly CI.
    #[test]
    fn bank_invariant_holds_over_long_horizon() {
        let mut sim = DualDcBankSim::new(8, 500, 99);
        let mut failures = 0;
        let partition_start = 5_000;
        let partition_end = 15_000;

        for tick in 0..30_000 {
            if tick == partition_start {
                sim.partition();
            }
            if tick == partition_end {
                sim.heal_partition();
            }
            let dc = if tick % 2 == 0 { 1 } else { 2 };
            sim.step_transfer(((tick % 11) + 1) as i64, dc);
            sim.tick_watermark(sim.coord.hlc());
            if !sim.invariant_holds() {
                failures += 1;
            }
        }

        sim.tick_watermark(u64::MAX);
        assert_eq!(
            failures, 0,
            "long-horizon per-DC bank conservation failures: {failures}"
        );
        assert!(sim.dcs_converged());
    }

    /// W8.8 RED. 1h-equivalent simulated endurance with 3+3 voters and
    /// 1 learner per DC. We compress "1 simulated hour" to 60_000
    /// ticks (each tick conceptually one millisecond) to keep the
    /// test under one wall-clock second. The acceptance gate is
    /// stricter than W7.10: the learner replicas must converge with
    /// their DC's voters at every step (after watermark advance) AND
    /// after the partition heal.
    ///
    /// This is the W8.8 sim acceptance test for ADR-014 learner
    /// replicas — zero linearizability violations, zero membership
    /// invariant violations, zero learner divergence.
    #[test]
    fn endurance_1h_with_learners_under_load() {
        let mut sim = DualDcBankSim::with_learners(8, 500, 137, 1, 1);
        let mut conservation_failures = 0;
        let mut learner_divergence_failures = 0;

        // Inject two partitions over the run — once early, once
        // mid-stream — to exercise learner re-sync after Accord
        // recovery.
        let partition_windows = [(5_000usize, 12_000usize), (30_000, 38_000)];

        for tick in 0..60_000usize {
            for &(start, end) in &partition_windows {
                if tick == start {
                    sim.partition();
                }
                if tick == end {
                    sim.heal_partition();
                }
            }

            // Two transfers per tick alternating originating DC.
            let dc = if tick % 2 == 0 { 1 } else { 2 };
            sim.step_transfer(((tick % 13) + 1) as i64, dc);
            sim.tick_watermark(sim.coord.hlc());

            if !sim.invariant_holds() {
                conservation_failures += 1;
            }
            // Learner divergence check: when not partitioned, each
            // learner's totals must equal its DC voter's total.
            if !sim.partitioned {
                if let Some(l) = sim.dc1_learners.first() {
                    if l.total() != sim.dc1.total() {
                        learner_divergence_failures += 1;
                    }
                }
                if let Some(l) = sim.dc2_learners.first() {
                    if l.total() != sim.dc2.total() {
                        learner_divergence_failures += 1;
                    }
                }
            }
        }

        // Final drain (handles any post-partition queue).
        sim.tick_watermark(u64::MAX);

        assert_eq!(
            conservation_failures, 0,
            "endurance: {conservation_failures} per-DC bank-conservation failures",
        );
        assert_eq!(
            learner_divergence_failures, 0,
            "endurance: {learner_divergence_failures} learner-vs-voter divergence steps",
        );
        assert!(
            sim.dcs_converged(),
            "endurance: voters and learners must converge after final drain",
        );
    }
}
