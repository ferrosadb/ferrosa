use std::fmt;

#[derive(Debug, Clone, Copy)]
#[derive(PartialEq, Eq)]
#[derive(Hash)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum RPCTypes {
    Vote,
    /// Pre-Vote probe — see [`crate::raft::PreVoteRequest`] (Ongaro §9.6).
    PreVote,
    AppendEntries,
    InstallSnapshot,
    /// Leadership-Transfer directive — see [`crate::raft::TimeoutNowRequest`] (Ongaro §3.10).
    TimeoutNow,
}

impl fmt::Display for RPCTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
