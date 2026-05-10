use anyerror::AnyError;

/// Error variants related to configuration.
#[derive(Debug, thiserror::Error)]
#[derive(PartialEq, Eq)]
pub enum ConfigError {
    #[error("ParseError: {source} while parsing ({args:?})")]
    ParseError { source: AnyError, args: Vec<String> },

    /// The min election timeout is not smaller than the max election timeout.
    #[error("election timeout: min({min}) must be < max({max})")]
    ElectionTimeout { min: u64, max: u64 },

    #[error("max_payload_entries must be > 0")]
    MaxPayloadIs0,

    #[error("election_timeout_min({election_timeout_min}) must be > heartbeat_interval({heartbeat_interval})")]
    ElectionTimeoutLTHeartBeat {
        election_timeout_min: u64,
        heartbeat_interval: u64,
    },

    #[error("snapshot policy string is invalid: '{invalid:?}' expect: '{syntax}'")]
    InvalidSnapshotPolicy { invalid: String, syntax: String },

    #[error("{reason} when parsing {invalid:?}")]
    InvalidNumber { invalid: String, reason: String },

    /// `check_quorum_ratio` outside the accepted range `[0.0, 2.0]`.
    /// `0.0` is the disabled sentinel; values above `2.0` are nonsensical
    /// (would only step down after twice the election timeout). See ADR-012.
    ///
    /// Stored as the raw bits (`f64::to_bits`) so the enum can derive `Eq`
    /// (f64 is `PartialEq` only). Decoder via `bit-pattern reinterpret`.
    #[error("check_quorum_ratio must be in [0.0, 2.0], got {}", f64::from_bits(*ratio_bits))]
    InvalidCheckQuorumRatio { ratio_bits: u64 },
}

impl ConfigError {
    /// Construct an `InvalidCheckQuorumRatio` from an `f64` (handles the
    /// `Eq`-derive bit-encoding).
    pub(crate) fn invalid_check_quorum_ratio(ratio: f64) -> Self {
        Self::InvalidCheckQuorumRatio {
            ratio_bits: ratio.to_bits(),
        }
    }
}
