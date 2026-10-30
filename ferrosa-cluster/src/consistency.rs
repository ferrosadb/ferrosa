use serde::{Deserialize, Serialize};

/// CQL consistency levels for read and write operations.
///
/// Each level defines how many replica acknowledgements are required
/// before the coordinator responds to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalOne,
    LocalQuorum,
    EachQuorum,
    /// Serial consistency for lightweight transactions (LWT read phase).
    /// Wire code: 0x0008.
    Serial,
    /// Data-center-local serial consistency for lightweight transactions.
    /// Wire code: 0x0009.
    LocalSerial,
    /// Quorum in the origin datacenter **and** a quorum of datacenters, each reaching its own
    /// quorum. Ferrosa-specific: Cassandra has no equivalent, so this uses an out-of-band wire code
    /// and a stock driver rejects it with a protocol error rather than misreading it. New
    /// applications can opt in by string (`"DC_QUORUM"`) or by code. Wire code: 0x0100.
    DcQuorum,
}

impl ConsistencyLevel {
    /// Number of replicas that must acknowledge before responding.
    ///
    /// # Panics
    ///
    /// Panics if called with `EachQuorum`, which a single total cannot express -- use
    /// `block_for_dc`. For `DcQuorum` this returns the node-quorum condition only; the datacenter
    /// shape lives in `coordinator::dc_quorum`.
    pub fn block_for(&self, rf: usize) -> usize {
        match self {
            Self::One | Self::LocalOne => 1,
            Self::Two => 2.min(rf),
            Self::Three => 3.min(rf),
            // DC_QUORUM's node-quorum condition, and its safe value on a single datacenter. A
            // request path that needs the full requirement (the origin datacenter's quorum plus a
            // quorum of datacenters) must go through `coordinator::dc_quorum`, but no path may
            // panic on a level a client is allowed to send.
            Self::Quorum | Self::LocalQuorum | Self::Serial | Self::DcQuorum => rf / 2 + 1,
            Self::LocalSerial => rf / 2 + 1,
            Self::All => rf,
            Self::EachQuorum => {
                // EACH_QUORUM's exact requirement is the sum of the per-datacenter quorums, which
                // only `block_for_dc` can express. There is no datacenter vector here, so the only
                // total answer is the single-datacenter node quorum — the same value the single-DC
                // read and write paths need. A path that must satisfy the whole EACH_QUORUM
                // requirement goes through `coordinator::dc_quorum`, never through this function;
                // but no path may panic on a level a client is allowed to send.
                rf / 2 + 1
            }
        }
    }

    /// Number of replicas per data center that must acknowledge.
    pub fn block_for_dc(&self, dc_rf: usize) -> usize {
        match self {
            Self::EachQuorum | Self::LocalQuorum | Self::LocalSerial | Self::DcQuorum => {
                dc_rf / 2 + 1
            }
            _ => self.block_for(dc_rf),
        }
    }

    /// Decode from the CQL native protocol wire format (u16).
    ///
    /// See CQL native protocol v5 section 2.2 for the consistency enum values.
    pub fn from_wire(code: u16) -> Option<Self> {
        match code {
            0x0001 => Some(Self::One),
            0x0002 => Some(Self::Two),
            0x0003 => Some(Self::Three),
            0x0004 => Some(Self::Quorum),
            0x0005 => Some(Self::All),
            0x0006 => Some(Self::LocalQuorum),
            0x0007 => Some(Self::EachQuorum),
            0x0008 => Some(Self::Serial),
            0x0009 => Some(Self::LocalSerial),
            0x000A => Some(Self::LocalOne),
            // Out of Cassandra's assigned range (0x0000-0x000A) on purpose: a stock driver must
            // fail loudly on DC_QUORUM rather than silently interpret it as another level.
            0x0100 => Some(Self::DcQuorum),
            _ => None,
        }
    }

    /// Parse from CQL string representation.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ONE" => Some(Self::One),
            "TWO" => Some(Self::Two),
            "THREE" => Some(Self::Three),
            "QUORUM" => Some(Self::Quorum),
            "ALL" => Some(Self::All),
            "LOCAL_ONE" => Some(Self::LocalOne),
            "LOCAL_QUORUM" => Some(Self::LocalQuorum),
            "EACH_QUORUM" => Some(Self::EachQuorum),
            "SERIAL" => Some(Self::Serial),
            "LOCAL_SERIAL" => Some(Self::LocalSerial),
            "DC_QUORUM" => Some(Self::DcQuorum),
            _ => None,
        }
    }

    /// Encode to the CQL native protocol wire format (u16).
    pub fn to_wire(&self) -> u16 {
        match self {
            Self::One => 0x0001,
            Self::Two => 0x0002,
            Self::Three => 0x0003,
            Self::Quorum => 0x0004,
            Self::All => 0x0005,
            Self::LocalQuorum => 0x0006,
            Self::EachQuorum => 0x0007,
            Self::Serial => 0x0008,
            Self::LocalSerial => 0x0009,
            Self::LocalOne => 0x000A,
            Self::DcQuorum => 0x0100,
        }
    }

    /// CQL string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::One => "ONE",
            Self::Two => "TWO",
            Self::Three => "THREE",
            Self::Quorum => "QUORUM",
            Self::All => "ALL",
            Self::LocalOne => "LOCAL_ONE",
            Self::LocalQuorum => "LOCAL_QUORUM",
            Self::EachQuorum => "EACH_QUORUM",
            Self::Serial => "SERIAL",
            Self::LocalSerial => "LOCAL_SERIAL",
            Self::DcQuorum => "DC_QUORUM",
        }
    }
}

impl std::fmt::Display for ConsistencyLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn block_for_one() {
        assert_eq!(ConsistencyLevel::One.block_for(3), 1);
        assert_eq!(ConsistencyLevel::One.block_for(1), 1);
    }

    #[test]
    fn block_for_quorum() {
        assert_eq!(ConsistencyLevel::Quorum.block_for(3), 2);
        assert_eq!(ConsistencyLevel::Quorum.block_for(5), 3);
        assert_eq!(ConsistencyLevel::Quorum.block_for(1), 1);
    }

    #[test]
    fn block_for_all() {
        assert_eq!(ConsistencyLevel::All.block_for(3), 3);
        assert_eq!(ConsistencyLevel::All.block_for(1), 1);
    }

    #[test]
    fn block_for_two_capped_at_rf() {
        assert_eq!(ConsistencyLevel::Two.block_for(1), 1);
        assert_eq!(ConsistencyLevel::Two.block_for(3), 2);
    }

    #[test]
    fn block_for_dc() {
        assert_eq!(ConsistencyLevel::EachQuorum.block_for_dc(3), 2);
        assert_eq!(ConsistencyLevel::LocalQuorum.block_for_dc(3), 2);
    }

    /// Every level the wire decoder can produce, derived from the decoder itself.
    ///
    /// A client can send any of these codes, so a hand-written list here would let a new variant
    /// slip past the totality checks below.
    fn wire_decodable_levels() -> impl Iterator<Item = ConsistencyLevel> {
        (0u16..=u16::MAX).filter_map(ConsistencyLevel::from_wire)
    }

    /// `block_for` is called on the request path with a level that came off the wire, so it must
    /// answer for every decodable level instead of panicking on one of them.
    #[test]
    fn block_for_answers_every_wire_decodable_level() {
        let mut seen = Vec::new();
        for cl in wire_decodable_levels() {
            let answered = std::panic::catch_unwind(|| cl.block_for(3));
            assert!(
                answered.is_ok(),
                "block_for({cl}) panicked for a level the wire decoder produces"
            );
            let bf = answered.unwrap();
            assert!(
                (1..=3).contains(&bf),
                "blockFor({cl}, 3) = {bf} is outside 1..=rf"
            );
            assert_eq!(ConsistencyLevel::from_wire(cl.to_wire()), Some(cl));
            seen.push(cl);
        }
        assert_eq!(seen.len(), 11, "wire decoder variant set changed: {seen:?}");
    }

    /// EACH_QUORUM's exact requirement is the sum of per-datacenter quorums, which only
    /// `block_for_dc` can express. Without a datacenter vector the only total answer is the
    /// single-datacenter node quorum -- the same value the single-DC write/read paths need.
    #[test]
    fn block_for_each_quorum_is_the_single_dc_node_quorum() {
        for rf in 1..=9usize {
            assert_eq!(
                ConsistencyLevel::EachQuorum.block_for(rf),
                rf / 2 + 1,
                "EACH_QUORUM without a DC vector must fall back to the node quorum"
            );
            assert_eq!(
                ConsistencyLevel::EachQuorum.block_for_dc(rf),
                rf / 2 + 1,
                "the per-DC requirement stays the per-DC quorum"
            );
        }
    }

    #[test]
    fn from_str_roundtrip() {
        for cl in &[
            ConsistencyLevel::One,
            ConsistencyLevel::Two,
            ConsistencyLevel::Three,
            ConsistencyLevel::Quorum,
            ConsistencyLevel::All,
            ConsistencyLevel::LocalOne,
            ConsistencyLevel::LocalQuorum,
            ConsistencyLevel::EachQuorum,
            ConsistencyLevel::Serial,
            ConsistencyLevel::LocalSerial,
        ] {
            assert_eq!(ConsistencyLevel::from_str(cl.as_str()), Some(*cl));
        }
    }

    #[test]
    fn from_str_unknown_returns_none() {
        assert_eq!(ConsistencyLevel::from_str("INVALID"), None);
    }

    #[test]
    fn consistency_serial_wire_code() {
        assert_eq!(
            ConsistencyLevel::from_wire(0x0008),
            Some(ConsistencyLevel::Serial)
        );
        assert_eq!(
            ConsistencyLevel::from_wire(0x0009),
            Some(ConsistencyLevel::LocalSerial)
        );
        assert_eq!(ConsistencyLevel::Serial.to_wire(), 0x0008);
        assert_eq!(ConsistencyLevel::LocalSerial.to_wire(), 0x0009);
    }

    proptest! {
        #[test]
        fn block_for_never_exceeds_rf(rf in 1usize..=100) {
            for cl in wire_decodable_levels() {
                let bf = cl.block_for(rf);
                prop_assert!(bf <= rf, "blockFor({:?}, {}) = {} exceeds RF", cl, rf, bf);
            }
        }

        #[test]
        fn quorum_is_majority(rf in 1usize..=100) {
            let bf = ConsistencyLevel::Quorum.block_for(rf);
            prop_assert!(bf > rf / 2, "QUORUM blockFor({}) = {} is not majority", rf, bf);
        }

        #[test]
        fn block_for_at_least_one(rf in 1usize..=100) {
            for cl in wire_decodable_levels() {
                let bf = cl.block_for(rf);
                prop_assert!(bf >= 1, "blockFor({:?}, {}) = 0", cl, rf);
            }
        }
    }
}
