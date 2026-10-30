//! What a run's consistency configuration licenses.
//!
//! CL=ONE on a three-node cluster does not guarantee what CL=ALL guarantees, and that is by design.
//! So a verdict is a function of `(history, profile)` rather than of the history alone:
//!
//! * a clean run under a weak profile is **not evidence** — reporting it as a pass over-claims;
//! * a violation under a weak profile is **not a defect** — reporting it as a failure under-claims.
//!
//! Collapsing either one into pass/fail is how a suite ends up asserting a guarantee the
//! configuration never made. The Lean counterpart is
//! `specs/lean4/Ferrosa/Testing/{Guarantees,Histories}.lean`, where
//! `nothing_is_confirmed_that_the_profile_cannot_support` and
//! `nothing_is_excused_that_the_profile_can_convict` are proved over every book and profile.
//!
//! The deciding condition is the replica-set overlap, `W + R > RF`: only then must a read intersect
//! every write's replica set. A read answered by another datacenter defeats the argument even when
//! the counts overlap, because the overlap is a statement about one replica set — which is why
//! DC_QUORUM's local reads cannot establish strict serializability no matter the level name.

use serde::{Deserialize, Serialize};

/// A consistency level, mirroring the wire values in `ferrosa-cluster`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    One,
    Quorum,
    LocalQuorum,
    All,
}

impl Level {
    /// Replicas that must respond for this level on a cluster of `replication_factor` replicas.
    ///
    /// `LocalQuorum` is a quorum *within the origin datacenter*; with a single datacenter it is the
    /// same count as `Quorum`, and the difference is routing rather than arithmetic — see
    /// [`ConsistencyProfile::cross_datacenter_read`].
    pub fn replicas(self, replication_factor: u32) -> u32 {
        let rf = replication_factor.max(1);
        match self {
            Level::One => 1,
            Level::Quorum | Level::LocalQuorum => rf / 2 + 1,
            Level::All => rf,
        }
        .min(rf)
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "one" | "1" => Some(Level::One),
            "quorum" => Some(Level::Quorum),
            "local_quorum" | "localquorum" => Some(Level::LocalQuorum),
            "all" => Some(Level::All),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Level::One => "ONE",
            Level::Quorum => "QUORUM",
            Level::LocalQuorum => "LOCAL_QUORUM",
            Level::All => "ALL",
        }
    }
}

/// The configuration a run's assertions are scoped to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsistencyProfile {
    pub replication_factor: u32,
    pub write_level: Level,
    pub read_level: Level,
    /// True when a read may be answered by a datacenter that does not hold the write.
    pub cross_datacenter_read: bool,
}

impl Default for ConsistencyProfile {
    /// The suite's default: three replicas, quorum on both paths, single datacenter. This is the
    /// weakest configuration that still overlaps (`2 + 2 > 3`), so a default run can convict.
    fn default() -> Self {
        Self {
            replication_factor: 3,
            write_level: Level::Quorum,
            read_level: Level::Quorum,
            cross_datacenter_read: false,
        }
    }
}

impl ConsistencyProfile {
    pub fn new(
        replication_factor: u32,
        write_level: Level,
        read_level: Level,
        cross_datacenter_read: bool,
    ) -> Self {
        Self {
            replication_factor,
            write_level,
            read_level,
            cross_datacenter_read,
        }
    }

    pub fn write_replicas(&self) -> u32 {
        self.write_level.replicas(self.replication_factor)
    }

    pub fn read_replicas(&self) -> u32 {
        self.read_level.replicas(self.replication_factor)
    }

    /// `W + R > RF`: a read must intersect every write's replica set.
    pub fn overlaps(&self) -> bool {
        self.write_replicas() + self.read_replicas() > self.replication_factor.max(1)
    }

    /// Whether a read issued under this profile can see every acknowledged write.
    pub fn reads_every_acknowledged_write(&self) -> bool {
        self.overlaps() && !self.cross_datacenter_read
    }

    /// **The verdict.** `observed_clean` is the raw outcome of the checkers; this decides what the
    /// run may claim from it.
    pub fn classify(&self, observed_clean: bool) -> Assurance {
        if !self.overlaps() {
            return Assurance::NotTested(NotTested::ReadsDoNotOverlapWrites);
        }
        if self.cross_datacenter_read {
            return Assurance::NotTested(NotTested::CrossDatacenterRead);
        }
        if observed_clean {
            Assurance::Confirmed
        } else {
            Assurance::Refuted
        }
    }

    /// The run's declared configuration, from the environment. `FERROSA_JEPSEN_RF`,
    /// `FERROSA_JEPSEN_WRITE_CL`, `FERROSA_JEPSEN_READ_CL`, `FERROSA_JEPSEN_CROSS_DC_READ`.
    ///
    /// Reading it here rather than inferring it from a level name is deliberate: a level name cannot
    /// express whether a read was served locally, and that single bit is what separates a real loss
    /// from an expected one.
    pub fn from_env() -> Self {
        let mut p = ConsistencyProfile::default();
        if let Ok(v) = std::env::var("FERROSA_JEPSEN_RF") {
            if let Ok(n) = v.trim().parse::<u32>() {
                p.replication_factor = n.max(1);
            }
        }
        if let Ok(v) = std::env::var("FERROSA_JEPSEN_WRITE_CL") {
            if let Some(l) = Level::parse(&v) {
                p.write_level = l;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_JEPSEN_READ_CL") {
            if let Some(l) = Level::parse(&v) {
                p.read_level = l;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_JEPSEN_CROSS_DC_READ") {
            let v = v.trim().to_ascii_lowercase();
            p.cross_datacenter_read = matches!(v.as_str(), "1" | "true" | "yes");
        }
        p
    }
}

/// Why a run could not decide anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotTested {
    /// `W + R <= RF`: the read need not intersect the write's replica set.
    ReadsDoNotOverlapWrites,
    /// The read may be answered by a datacenter that does not hold the write.
    CrossDatacenterRead,
}

impl NotTested {
    pub fn reason(self) -> &'static str {
        match self {
            NotTested::ReadsDoNotOverlapWrites => {
                "W + R <= RF: a read need not intersect every write's replica set"
            }
            NotTested::CrossDatacenterRead => {
                "a read may be answered by a datacenter that does not hold the write"
            }
        }
    }
}

/// Three outcomes, not two. Two would force either an over-claim (an inconclusive run called green)
/// or an under-claim (a semantics consequence called a failure).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Assurance {
    /// The run observed nothing and the profile was strong enough for that to mean something.
    Confirmed,
    /// The run observed a violation the profile cannot explain.
    Refuted,
    /// The profile cannot support either claim.
    NotTested(NotTested),
}

impl Default for Assurance {
    /// Never claim by default: an unset assurance is "not tested", not "confirmed".
    fn default() -> Self {
        Assurance::NotTested(NotTested::ReadsDoNotOverlapWrites)
    }
}

impl Assurance {
    pub fn is_confirmed(self) -> bool {
        matches!(self, Assurance::Confirmed)
    }

    pub fn is_refuted(self) -> bool {
        matches!(self, Assurance::Refuted)
    }

    pub fn is_not_tested(self) -> bool {
        matches!(self, Assurance::NotTested(_))
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Assurance::Confirmed => "confirmed",
            Assurance::Refuted => "refuted",
            Assurance::NotTested(_) => "not_tested",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(rf: u32, w: Level, r: Level, cross: bool) -> ConsistencyProfile {
        ConsistencyProfile::new(rf, w, r, cross)
    }

    /// On three nodes CL=ONE and CL=ALL do not share a durability guarantee, by design.
    #[test]
    fn one_and_all_do_not_share_a_guarantee() {
        assert_eq!(p(3, Level::One, Level::One, false).write_replicas(), 1);
        assert_eq!(p(3, Level::All, Level::All, false).write_replicas(), 3);
    }

    /// CL=ONE plus CL=ONE does not overlap on three nodes.
    #[test]
    fn one_and_one_does_not_overlap() {
        let profile = p(3, Level::One, Level::One, false);
        assert!(!profile.overlaps());
        assert!(!profile.reads_every_acknowledged_write());
    }

    /// Quorum plus quorum overlaps.
    #[test]
    fn quorum_and_quorum_overlaps() {
        assert!(p(3, Level::Quorum, Level::Quorum, false).overlaps());
    }

    /// It is the pair that matters: CL=ALL writes with CL=ONE reads still overlap, because a write on
    /// every replica intersects any single-replica read.
    #[test]
    fn all_write_with_one_read_still_overlaps() {
        assert!(p(3, Level::All, Level::One, false).reads_every_acknowledged_write());
        assert!(p(3, Level::One, Level::All, false).reads_every_acknowledged_write());
    }

    /// CL=ONE writes with CL=QUORUM reads do not overlap on three nodes (1 + 2 is not > 3).
    #[test]
    fn one_write_with_quorum_read_does_not_overlap() {
        assert!(!p(3, Level::One, Level::Quorum, false).reads_every_acknowledged_write());
    }

    /// A cross-datacenter read defeats the overlap even at CL=ALL.
    #[test]
    fn cross_datacenter_read_defeats_the_overlap() {
        let profile = p(3, Level::All, Level::All, true);
        assert!(profile.overlaps());
        assert!(!profile.reads_every_acknowledged_write());
        assert_eq!(
            profile.classify(true),
            Assurance::NotTested(NotTested::CrossDatacenterRead)
        );
    }

    /// Raising either level can only add guarantees, so a weaker configuration is sound not buggy.
    #[test]
    fn raising_a_level_never_removes_a_guarantee() {
        for rf in 1..=7u32 {
            for w in [Level::One, Level::Quorum, Level::All] {
                for r in [Level::One, Level::Quorum, Level::All] {
                    let base = p(rf, w, r, false);
                    if base.overlaps() {
                        for w2 in [Level::One, Level::Quorum, Level::All] {
                            for r2 in [Level::One, Level::Quorum, Level::All] {
                                if w2.replicas(rf) >= w.replicas(rf)
                                    && r2.replicas(rf) >= r.replicas(rf)
                                {
                                    assert!(
                                        p(rf, w2, r2, false).overlaps(),
                                        "rf={rf} {w:?}/{r:?} overlaps but {w2:?}/{r2:?} does not"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// **No over-claim**: a clean run under a profile that cannot see every acknowledged write is
    /// never confirmed.
    #[test]
    fn a_clean_run_under_one_one_is_not_confirmed() {
        assert_eq!(
            p(3, Level::One, Level::One, false).classify(true),
            Assurance::NotTested(NotTested::ReadsDoNotOverlapWrites)
        );
    }

    /// **No under-claim**: an anomaly under a profile that can convict is refuted, not excused.
    #[test]
    fn an_anomaly_under_quorum_quorum_is_refuted() {
        assert_eq!(
            p(3, Level::Quorum, Level::Quorum, false).classify(false),
            Assurance::Refuted
        );
    }

    /// And an anomaly under a weak profile is not a defect: the same history is refuted or untested
    /// depending only on the profile.
    #[test]
    fn a_verdict_without_the_profile_is_not_a_verdict() {
        assert_eq!(
            p(3, Level::Quorum, Level::Quorum, false).classify(false),
            Assurance::Refuted
        );
        assert_eq!(
            p(3, Level::One, Level::One, false).classify(false),
            Assurance::NotTested(NotTested::ReadsDoNotOverlapWrites)
        );
    }

    #[test]
    fn level_round_trips_through_its_name() {
        for l in [Level::One, Level::Quorum, Level::LocalQuorum, Level::All] {
            assert_eq!(Level::parse(l.as_str()), Some(l));
        }
        assert_eq!(Level::parse("nonsense"), None);
    }
}
