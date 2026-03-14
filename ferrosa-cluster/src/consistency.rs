/// CQL consistency levels for read and write operations.
///
/// Each level defines how many replica acknowledgements are required
/// before the coordinator responds to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsistencyLevel {
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalOne,
    LocalQuorum,
    EachQuorum,
}

impl ConsistencyLevel {
    /// Number of replicas that must acknowledge before responding.
    ///
    /// # Panics
    ///
    /// Panics if called with `EachQuorum` — use `block_for_dc` instead.
    pub fn block_for(&self, rf: usize) -> usize {
        match self {
            Self::One | Self::LocalOne => 1,
            Self::Two => 2.min(rf),
            Self::Three => 3.min(rf),
            Self::Quorum | Self::LocalQuorum => rf / 2 + 1,
            Self::All => rf,
            Self::EachQuorum => {
                panic!("use block_for_dc() for EACH_QUORUM");
            }
        }
    }

    /// Number of replicas per data center that must acknowledge.
    pub fn block_for_dc(&self, dc_rf: usize) -> usize {
        match self {
            Self::EachQuorum | Self::LocalQuorum => dc_rf / 2 + 1,
            _ => self.block_for(dc_rf),
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
            _ => None,
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
        ] {
            assert_eq!(ConsistencyLevel::from_str(cl.as_str()), Some(*cl));
        }
    }

    #[test]
    fn from_str_unknown_returns_none() {
        assert_eq!(ConsistencyLevel::from_str("INVALID"), None);
    }

    proptest! {
        #[test]
        fn block_for_never_exceeds_rf(rf in 1usize..=100) {
            for cl in &[
                ConsistencyLevel::One,
                ConsistencyLevel::Two,
                ConsistencyLevel::Three,
                ConsistencyLevel::Quorum,
                ConsistencyLevel::All,
                ConsistencyLevel::LocalOne,
                ConsistencyLevel::LocalQuorum,
            ] {
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
            for cl in &[
                ConsistencyLevel::One,
                ConsistencyLevel::Two,
                ConsistencyLevel::Three,
                ConsistencyLevel::Quorum,
                ConsistencyLevel::All,
            ] {
                let bf = cl.block_for(rf);
                prop_assert!(bf >= 1, "blockFor({:?}, {}) = 0", cl, rf);
            }
        }
    }
}
