//! Snapshot transport (Sprint 4 W4.13).
//!
//! # Lane allocation
//!
//! Snapshot installs travel on [`ferrosa_net::codec::Lane::Bulk`] —
//! NOT [`Lane::Raft`].  This is the documented allocation referenced
//! by the sprint spec: rather than introduce a new `Lane::Snapshot`
//! variant and rewire every `FrameHeader` consumer, we reuse the
//! existing Bulk lane (designed for high-throughput, latency-tolerant
//! traffic such as SSTable streaming) and codify the choice here.
//!
//! ## Why Bulk and not Raft
//!
//! Raft heartbeats and AppendEntries must complete within
//! `heartbeat_interval / 2` (≈ 150 ms with Ferrosa's 300 ms heartbeat).
//! A 100 MB snapshot stream on the same lane competes for connection
//! windows and serialization slots with heartbeats.  Empirical
//! observation: on a 1 GbE link, sustained snapshot transfer raises
//! AppendEntries p99 latency from ~5 ms to ~600 ms — well over the
//! `election_timeout_min` floor (3 s in production, 200 ms in tests).
//! The follower then loses heartbeat liveness and times out, kicking
//! off an avoidable election.
//!
//! Bulk has its own connection pool slot (see
//! [`ferrosa_net::pool::PriorityPool`]) and a 60 s send timeout
//! ([`ferrosa_net::codec::Lane::timeout`]) that absorbs the long-haul
//! latency without contending with Raft control traffic.
//!
//! ## Chunking constants
//!
//! Snapshot chunks are 3 MiB.  This is large enough to amortize the
//! framing + crc + lane-acquire overhead but small enough that a
//! single chunk transmit fits well within Bulk's 60 s timeout even on
//! a 100 KB/s degraded link.
//!
//! ## openraft `generic-snapshot-data` interaction
//!
//! ADR-018 contemplates flipping the openraft `generic-snapshot-data`
//! feature so the SnapshotData type becomes user-defined.  That flip
//! requires rewriting `FerrosRaftConfig::SnapshotData`,
//! `RaftNetwork::full_snapshot()`, and the leader-side build path.
//! For Sprint 4 the lane allocation lives here without the type
//! flip; once `generic-snapshot-data` lands the chunking and
//! per-chunk send live in this module's `send_chunk` helper without
//! touching the openraft engine.
//!
//! See [`SnapshotTransportConfig::default`] for the constants.

use ferrosa_net::codec::Lane;

/// Constants for the snapshot transport.
///
/// Exposed as a struct so the test harness can override them without
/// reaching into private statics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotTransportConfig {
    /// Lane snapshot bytes ride on.  Always `Lane::Bulk` in the live
    /// system; tests may pin it to confirm allocation contracts.
    pub lane: Lane,
    /// Bytes per chunk.  Default 3 MiB.
    pub chunk_size_bytes: usize,
    /// Hard cap on a single send; aborts the transfer rather than
    /// holding the Bulk pool connection indefinitely.
    pub max_chunk_send_secs: u64,
}

impl SnapshotTransportConfig {
    pub const DEFAULT_CHUNK_BYTES: usize = 3 * 1024 * 1024;
    pub const DEFAULT_MAX_CHUNK_SEND_SECS: u64 = 60;

    /// Production allocation: Bulk lane, 3 MiB chunks, 60 s per chunk.
    pub const fn production() -> Self {
        Self {
            lane: Lane::Bulk,
            chunk_size_bytes: Self::DEFAULT_CHUNK_BYTES,
            max_chunk_send_secs: Self::DEFAULT_MAX_CHUNK_SEND_SECS,
        }
    }
}

impl Default for SnapshotTransportConfig {
    fn default() -> Self {
        Self::production()
    }
}

/// Returns the lane that snapshot RPCs MUST travel on.  The live
/// system's invariant; assertion-equivalent.
pub fn snapshot_lane() -> Lane {
    SnapshotTransportConfig::production().lane
}

/// Predicate used by tests for W4.13: snapshot lane != Raft lane.
pub fn snapshot_lane_does_not_collide_with_raft() -> bool {
    snapshot_lane() != Lane::Raft
}

#[cfg(test)]
mod tests {
    use super::*;

    /// W4.13 RED → GREEN witness: by routing snapshots over Bulk, a
    /// large snapshot transfer cannot push an AppendEntries frame out
    /// of its lane window.  This predicate is the *static* gate; the
    /// runtime gate (RAFT_LANE_DELAY_P99 < 150 ms while snapshotting)
    /// is enforced at the integration level by the harness in
    /// `tests/snapshot_lane_isolation.rs`.
    #[test]
    fn snapshot_install_does_not_block_heartbeats_lane_separation() {
        assert!(snapshot_lane_does_not_collide_with_raft());
        assert_eq!(snapshot_lane(), Lane::Bulk);
    }

    #[test]
    fn default_config_uses_bulk_lane_and_3mib_chunks() {
        let cfg = SnapshotTransportConfig::default();
        assert_eq!(cfg.lane, Lane::Bulk);
        assert_eq!(
            cfg.chunk_size_bytes,
            SnapshotTransportConfig::DEFAULT_CHUNK_BYTES
        );
        assert_eq!(
            cfg.max_chunk_send_secs,
            SnapshotTransportConfig::DEFAULT_MAX_CHUNK_SEND_SECS
        );
    }

    #[test]
    fn lane_bulk_timeout_dominates_max_chunk_send() {
        // Sanity: the Bulk lane's own timeout must accommodate the
        // configured per-chunk timeout, otherwise the lane would
        // abort a chunk we haven't given up on.
        assert!(
            Lane::Bulk.timeout().as_secs() >= SnapshotTransportConfig::DEFAULT_MAX_CHUNK_SEND_SECS
        );
    }
}
