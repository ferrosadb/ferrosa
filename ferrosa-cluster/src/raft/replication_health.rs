//! Module: Per-follower replication progress, as the leader sees it.
//! Correctness: Correct when each leader sweep records every follower's lag
//!   (leader committed index minus the follower's matched index), a follower
//!   whose lag above the push threshold has not shrunk for
//!   [`ReplicationHealth::STALL_SWEEPS`] consecutive sweeps is reported by
//!   [`ReplicationHealth::stalled_peers`], and every snapshot chunk sent to a
//!   peer updates its consecutive-failure streak.
//! Last revised: 2026-10-10
//! Last changed: Created for outage 2026-10-10 (F3, decision D4).
//!
//! # Why
//!
//! On 2026-10-10 node2 sat at a fixed index for about an hour while the leader
//! offered it the same stale snapshot ~160,000 times. Every node answered
//! ready, and no metric said that one follower was not moving. This module is
//! that signal: Prometheus series per peer, and a readiness detail that names
//! the stuck peer.
//!
//! # Feeds
//!
//! - [`super::snapshot_pusher::run_snapshot_pusher`] calls
//!   [`ReplicationHealth::observe_sweep`] on every leader sweep and
//!   [`ReplicationHealth::clear`] when the node is not the leader.
//! - [`super::network::FerrosRaftNetwork::install_snapshot`] calls
//!   [`ReplicationHealth::record_snapshot_send`] for every chunk.

use std::collections::BTreeMap;
use std::fmt::Write as _;

/// A follower whose lag has not shrunk for [`ReplicationHealth::STALL_SWEEPS`] sweeps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StalledPeer {
    pub peer: u64,
    pub lag: u64,
    pub flat_sweeps: u32,
}

#[derive(Debug, Default, Clone, Copy)]
struct PeerProgress {
    /// Lag at the last sweep; `None` until the peer is first swept.
    lag: Option<u64>,
    /// Consecutive sweeps, while above the threshold, without the lag shrinking.
    flat_sweeps: u32,
    /// Consecutive failed snapshot-chunk sends; reset by a successful one.
    snapshot_send_failures: u64,
}

/// Per-peer replication progress, shared by the snapshot pusher, the Raft
/// network and the readiness / metrics endpoints.
#[derive(Debug, Default)]
pub struct ReplicationHealth {
    peers: parking_lot::Mutex<BTreeMap<u64, PeerProgress>>,
}

impl ReplicationHealth {
    /// Sweeps without progress before a lagging peer is reported stalled. At
    /// the production sweep interval (5 s) that is 30 s: long enough that a
    /// snapshot in flight is not reported, short enough to page well inside
    /// the hour the 2026-10-10 follower was stuck.
    pub const STALL_SWEEPS: u32 = 6;

    pub fn new() -> Self {
        Self::default()
    }

    /// Record one leader sweep: `peers` is every follower's `(id, matched
    /// index)`. A follower absent from `peers` is no longer replicated to and
    /// is forgotten (its snapshot streak with it).
    pub fn observe_sweep(&self, leader_committed: u64, lag_threshold: u64, peers: &[(u64, u64)]) {
        let mut map = self.peers.lock();
        map.retain(|id, _| peers.iter().any(|(peer, _)| peer == id));
        for &(peer, matched) in peers {
            let lag = leader_committed.saturating_sub(matched);
            let progress = map.entry(peer).or_default();
            progress.flat_sweeps = match progress.lag {
                Some(previous) if lag > lag_threshold && lag >= previous => {
                    progress.flat_sweeps.saturating_add(1)
                }
                _ => 0,
            };
            progress.lag = Some(lag);
        }
    }

    /// Forget all lag state: only a leader knows its followers' progress.
    pub fn clear(&self) {
        self.peers.lock().clear();
    }

    /// Record one snapshot-chunk send to `peer`.
    pub fn record_snapshot_send(&self, peer: u64, ok: bool) {
        let mut map = self.peers.lock();
        let progress = map.entry(peer).or_default();
        progress.snapshot_send_failures = if ok {
            0
        } else {
            progress.snapshot_send_failures.saturating_add(1)
        };
    }

    /// Consecutive failed snapshot sends to `peer` (0 if none or unknown).
    pub fn snapshot_send_failures(&self, peer: u64) -> u64 {
        self.peers
            .lock()
            .get(&peer)
            .map_or(0, |p| p.snapshot_send_failures)
    }

    /// Followers whose lag has not shrunk for at least `STALL_SWEEPS` sweeps.
    pub fn stalled_peers(&self) -> Vec<StalledPeer> {
        self.peers
            .lock()
            .iter()
            .filter(|(_, p)| p.flat_sweeps >= Self::STALL_SWEEPS)
            .filter_map(|(&peer, p)| {
                p.lag.map(|lag| StalledPeer {
                    peer,
                    lag,
                    flat_sweeps: p.flat_sweeps,
                })
            })
            .collect()
    }

    /// Append the per-peer series in Prometheus text format.
    pub fn render_prometheus(&self, out: &mut String) {
        let peers: Vec<(u64, PeerProgress)> =
            self.peers.lock().iter().map(|(&id, &p)| (id, p)).collect();
        out.push_str(
            "# HELP ferrosa_raft_replication_lag_entries Leader committed index minus the \
             follower's matched index, at the last leader sweep.\n\
             # TYPE ferrosa_raft_replication_lag_entries gauge\n",
        );
        for (peer, p) in &peers {
            if let Some(lag) = p.lag {
                // Writing to a String cannot fail.
                let _ = writeln!(
                    out,
                    "ferrosa_raft_replication_lag_entries{{peer=\"{peer}\"}} {lag}"
                );
            }
        }
        out.push_str(
            "# HELP ferrosa_raft_replication_lag_flat_sweeps Consecutive leader sweeps in which \
             a lagging follower's lag did not shrink; readiness reports degraded at 6.\n\
             # TYPE ferrosa_raft_replication_lag_flat_sweeps gauge\n",
        );
        for (peer, p) in &peers {
            if p.lag.is_some() {
                let _ = writeln!(
                    out,
                    "ferrosa_raft_replication_lag_flat_sweeps{{peer=\"{peer}\"}} {}",
                    p.flat_sweeps
                );
            }
        }
        out.push_str(
            "# HELP ferrosa_raft_snapshot_send_consecutive_failures Consecutive failed snapshot \
             chunk sends to the follower; reset by a successful send.\n\
             # TYPE ferrosa_raft_snapshot_send_consecutive_failures gauge\n",
        );
        for (peer, p) in &peers {
            let _ = writeln!(
                out,
                "ferrosa_raft_snapshot_send_consecutive_failures{{peer=\"{peer}\"}} {}",
                p.snapshot_send_failures
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T-F3-4 (outage 2026-10-10, F3/D4). Per-peer replication lag and
    /// consecutive snapshot-send failures appear in the Prometheus output with
    /// their values; a successful send resets the failure count.
    #[test]
    fn replication_metrics_export_per_peer_lag_and_snapshot_failures() {
        let health = ReplicationHealth::new();
        health.observe_sweep(1000, 10, &[(2, 400), (3, 999)]);
        health.record_snapshot_send(2, false);
        health.record_snapshot_send(2, false);

        let mut out = String::new();
        health.render_prometheus(&mut out);
        for line in [
            "# TYPE ferrosa_raft_replication_lag_entries gauge",
            "ferrosa_raft_replication_lag_entries{peer=\"2\"} 600",
            "ferrosa_raft_replication_lag_entries{peer=\"3\"} 1",
            "# TYPE ferrosa_raft_snapshot_send_consecutive_failures gauge",
            "ferrosa_raft_snapshot_send_consecutive_failures{peer=\"2\"} 2",
            "ferrosa_raft_replication_lag_flat_sweeps{peer=\"2\"} 0",
        ] {
            assert!(
                out.lines().any(|l| l == line),
                "missing `{line}` in:\n{out}"
            );
        }

        health.record_snapshot_send(2, true);
        let mut out = String::new();
        health.render_prometheus(&mut out);
        assert!(
            out.lines()
                .any(|l| l == "ferrosa_raft_snapshot_send_consecutive_failures{peer=\"2\"} 0"),
            "a successful send must reset the streak:\n{out}"
        );
    }

    /// The stall rule behind T-F3-5: lag that does not shrink for
    /// `STALL_SWEEPS` sweeps names the peer; shrinking lag, or lag within the
    /// threshold, does not.
    #[test]
    fn a_peer_is_stalled_only_when_its_lag_stops_shrinking() {
        let health = ReplicationHealth::new();
        for sweep in 0..=ReplicationHealth::STALL_SWEEPS {
            let committed = 1000 + u64::from(sweep) * 10;
            // Peer 2 is frozen (lag grows); peer 3 closes 50 a sweep; peer 4
            // is within the threshold.
            health.observe_sweep(
                committed,
                10,
                &[
                    (2, 400),
                    (3, 100 + u64::from(sweep) * 60),
                    (4, committed - 3),
                ],
            );
        }
        assert_eq!(
            health.stalled_peers(),
            vec![StalledPeer {
                peer: 2,
                lag: 1000 + u64::from(ReplicationHealth::STALL_SWEEPS) * 10 - 400,
                flat_sweeps: ReplicationHealth::STALL_SWEEPS,
            }]
        );

        // Progress clears it.
        health.observe_sweep(1100, 10, &[(2, 900)]);
        assert!(health.stalled_peers().is_empty());
    }
}
