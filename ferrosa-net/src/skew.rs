// ferrosa-net/src/skew.rs
//! Per-peer RTT and clock skew tracking derived from heartbeat measurements.
//!
//! Used by the Accord protocol to compute SkewMax — the maximum observed clock
//! offset across all peers — which determines the reorder buffer deadline for
//! PreAccept messages.

use std::collections::HashMap;
use std::time::Duration;

/// Per-peer RTT and clock skew tracking derived from heartbeat measurements.
pub struct SkewTracker {
    /// Per-peer sliding window of RTT samples.
    rtt_windows: HashMap<u64, Vec<Duration>>,
    /// Per-peer sliding window of observed clock offset samples (nanoseconds, signed).
    skew_windows: HashMap<u64, Vec<i64>>,
    /// Window size for sliding windows.
    window_size: usize,
    /// Hard ceiling for SkewMax (default 2s = 2_000_000_000ns).
    skew_ceiling_ns: u64,
    /// Computed SkewMax (P99.9 of observed offsets across all peers).
    skew_max_ns: u64,
}

impl SkewTracker {
    /// Create a new tracker with the given sliding window size and hard ceiling.
    ///
    /// # Panics
    ///
    /// Panics if `window_size` is zero.
    pub fn new(window_size: usize, skew_ceiling_ns: u64) -> Self {
        assert!(window_size > 0, "window_size must be positive");
        Self {
            rtt_windows: HashMap::new(),
            skew_windows: HashMap::new(),
            window_size,
            skew_ceiling_ns,
            skew_max_ns: 0,
        }
    }

    /// Record a heartbeat round-trip measurement.
    ///
    /// - `peer_id`: numeric identifier for the remote peer.
    /// - `sent_at_ns`: local wall-clock time (ns since epoch) when the Ping was sent.
    /// - `recv_at_ns`: remote wall-clock time (ns since epoch) when the remote received the Ping.
    /// - `local_time_ns`: local wall-clock time (ns since epoch) when the Pong was received.
    ///
    /// RTT = local_time_ns - sent_at_ns.
    /// Observed clock offset = recv_at_ns - (sent_at_ns + RTT/2).
    pub fn record_heartbeat(
        &mut self,
        peer_id: u64,
        sent_at_ns: u64,
        recv_at_ns: u64,
        local_time_ns: u64,
    ) {
        // Compute RTT (guard against underflow from bad clocks).
        let rtt_ns = local_time_ns.saturating_sub(sent_at_ns);
        let rtt = Duration::from_nanos(rtt_ns);

        // Observed clock offset: how far the remote clock is from ours.
        // midpoint = sent_at_ns + rtt_ns/2 (our best guess of "true" time at remote)
        // offset = recv_at_ns - midpoint (positive = remote clock is ahead)
        let midpoint = sent_at_ns + rtt_ns / 2;
        let offset_ns = recv_at_ns as i64 - midpoint as i64;

        // Insert into RTT sliding window.
        let rtt_window = self
            .rtt_windows
            .entry(peer_id)
            .or_insert_with(|| Vec::with_capacity(self.window_size));
        if rtt_window.len() >= self.window_size {
            rtt_window.remove(0);
        }
        rtt_window.push(rtt);

        // Insert into skew sliding window.
        let skew_window = self
            .skew_windows
            .entry(peer_id)
            .or_insert_with(|| Vec::with_capacity(self.window_size));
        if skew_window.len() >= self.window_size {
            skew_window.remove(0);
        }
        skew_window.push(offset_ns);

        // Recompute skew_max after every sample.
        self.recompute_skew_max();
    }

    /// P99 RTT for a specific peer.
    pub fn peer_rtt_p99(&self, peer_id: u64) -> Option<Duration> {
        let window = self.rtt_windows.get(&peer_id)?;
        if window.is_empty() {
            return None;
        }
        Some(percentile_duration(window, 99.0))
    }

    /// P99.9 of absolute clock offsets across all peers, capped at the ceiling.
    pub fn skew_max(&self) -> Duration {
        Duration::from_nanos(self.skew_max_ns)
    }

    /// Returns true if this peer's skew samples are more than 3 standard
    /// deviations from the median absolute offset across all peers.
    pub fn is_outlier(&self, peer_id: u64) -> bool {
        let peer_window = match self.skew_windows.get(&peer_id) {
            Some(w) if !w.is_empty() => w,
            _ => return false,
        };

        // Collect per-peer median absolute offsets.
        let mut peer_medians: Vec<f64> = Vec::new();
        for window in self.skew_windows.values() {
            if window.is_empty() {
                continue;
            }
            let median = median_abs(window);
            peer_medians.push(median);
        }

        if peer_medians.len() < 2 {
            return false;
        }

        let global_median = median_f64(&peer_medians);
        let std_dev = std_dev_f64(&peer_medians, global_median);
        if std_dev < 1e-9 {
            // All peers essentially identical; only flag if this peer differs at all.
            let this_median = median_abs(peer_window);
            return (this_median - global_median).abs() > 1e-9;
        }

        let this_median = median_abs(peer_window);
        let z_score = (this_median - global_median).abs() / std_dev;
        z_score > 3.0
    }

    /// Maximum of all peers' P99 RTT.
    pub fn max_peer_latency(&self) -> Duration {
        self.rtt_windows
            .keys()
            .filter_map(|&pid| self.peer_rtt_p99(pid))
            .max()
            .unwrap_or(Duration::ZERO)
    }

    /// Reorder buffer deadline for a PreAccept message.
    ///
    /// From spec section 7.1:
    ///   deadline = t0_time + skew_max + max_peer_latency
    ///
    /// If coordinator_id == local_id, RTT component is zero (local coordination).
    pub fn reorder_deadline_ns(&self, t0_time: u64, coordinator_id: u64, local_id: u64) -> u64 {
        let skew = self.skew_max_ns;
        let latency = if coordinator_id == local_id {
            0
        } else {
            self.max_peer_latency().as_nanos() as u64
        };
        t0_time.saturating_add(skew).saturating_add(latency)
    }

    /// Recompute skew_max from all per-peer skew windows.
    ///
    /// Outlier peers are excluded from the P99.9 computation to prevent
    /// a single misbehaving node from inflating the deadline.
    fn recompute_skew_max(&mut self) {
        // Collect all absolute offsets from non-outlier peers.
        // We do a two-pass approach: first compute outlier status, then
        // aggregate only from non-outlier peers.
        let outlier_peers: Vec<u64> = self
            .skew_windows
            .keys()
            .copied()
            .filter(|&pid| self.is_outlier_internal(pid))
            .collect();

        let mut all_abs_offsets: Vec<u64> = Vec::new();
        for (&pid, window) in &self.skew_windows {
            if outlier_peers.contains(&pid) {
                continue;
            }
            for &offset in window {
                all_abs_offsets.push(offset.unsigned_abs());
            }
        }

        if all_abs_offsets.is_empty() {
            // If all peers are outliers, fall back to all samples.
            for window in self.skew_windows.values() {
                for &offset in window {
                    all_abs_offsets.push(offset.unsigned_abs());
                }
            }
        }

        if all_abs_offsets.is_empty() {
            self.skew_max_ns = 0;
            return;
        }

        let p999 = percentile_u64(&all_abs_offsets, 99.9);
        self.skew_max_ns = p999.min(self.skew_ceiling_ns);
    }

    /// Internal outlier check that doesn't trigger recompute (avoids infinite recursion).
    fn is_outlier_internal(&self, peer_id: u64) -> bool {
        let peer_window = match self.skew_windows.get(&peer_id) {
            Some(w) if !w.is_empty() => w,
            _ => return false,
        };

        let mut peer_medians: Vec<f64> = Vec::new();
        for window in self.skew_windows.values() {
            if window.is_empty() {
                continue;
            }
            peer_medians.push(median_abs(window));
        }

        if peer_medians.len() < 2 {
            return false;
        }

        let global_median = median_f64(&peer_medians);
        let std_dev = std_dev_f64(&peer_medians, global_median);
        if std_dev < 1e-9 {
            let this_median = median_abs(peer_window);
            return (this_median - global_median).abs() > 1e-9;
        }

        let this_median = median_abs(peer_window);
        let z_score = (this_median - global_median).abs() / std_dev;
        z_score > 3.0
    }
}

// ---------------------------------------------------------------------------
// Statistical helpers
// ---------------------------------------------------------------------------

/// Compute percentile of a Duration slice using the "nearest-rank" method.
///
/// For P99 with 100 samples, this returns the value at rank ceil(99/100 * 100) = 99
/// (1-indexed), i.e. sorted[98] (0-indexed). The highest sample is at index 99.
fn percentile_duration(samples: &[Duration], pct: f64) -> Duration {
    assert!(!samples.is_empty());
    let mut sorted: Vec<Duration> = samples.to_vec();
    sorted.sort();
    let rank = (pct / 100.0 * sorted.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[idx]
}

/// Compute percentile of a u64 slice using the "nearest-rank" method.
fn percentile_u64(samples: &[u64], pct: f64) -> u64 {
    assert!(!samples.is_empty());
    let mut sorted: Vec<u64> = samples.to_vec();
    sorted.sort();
    let rank = (pct / 100.0 * sorted.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[idx]
}

/// Median of absolute values from a signed i64 slice, returned as f64.
fn median_abs(samples: &[i64]) -> f64 {
    assert!(!samples.is_empty());
    let mut abs_vals: Vec<u64> = samples.iter().map(|v| v.unsigned_abs()).collect();
    abs_vals.sort();
    let mid = abs_vals.len() / 2;
    if abs_vals.len().is_multiple_of(2) {
        (abs_vals[mid - 1] as f64 + abs_vals[mid] as f64) / 2.0
    } else {
        abs_vals[mid] as f64
    }
}

/// Median of f64 slice.
fn median_f64(samples: &[f64]) -> f64 {
    assert!(!samples.is_empty());
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

/// Standard deviation of f64 slice given a precomputed mean.
fn std_dev_f64(samples: &[f64], mean: f64) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }
    let variance: f64 =
        samples.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (samples.len() as f64 - 1.0);
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000; // 1ms in nanoseconds

    /// Helper: simulate a heartbeat where the remote received the ping at
    /// `sent_at + one_way_latency` and we received the pong at `sent_at + rtt`.
    /// Remote clock offset is `remote_offset_ns` from our clock.
    fn simulate_heartbeat(
        tracker: &mut SkewTracker,
        peer_id: u64,
        sent_at_ns: u64,
        rtt_ns: u64,
        remote_offset_ns: i64,
    ) {
        // Remote received the ping at our sent_at + half_rtt + their clock offset.
        let half_rtt = rtt_ns / 2;
        let recv_at_ns = (sent_at_ns + half_rtt) as i64 + remote_offset_ns;
        let local_time_ns = sent_at_ns + rtt_ns;
        tracker.record_heartbeat(peer_id, sent_at_ns, recv_at_ns as u64, local_time_ns);
    }

    #[test]
    fn heartbeat_rtt_tracking() {
        let mut tracker = SkewTracker::new(100, 2_000 * MS);

        // Record 10 heartbeats from peer 1 with known RTTs (1ms to 10ms).
        for i in 1..=10u64 {
            let sent = i * 100 * MS;
            let rtt = i * MS; // 1ms, 2ms, ..., 10ms
            simulate_heartbeat(&mut tracker, 1, sent, rtt, 0);
        }

        let p99 = tracker.peer_rtt_p99(1).unwrap();
        // With 10 samples, P99 should be close to the maximum (10ms).
        assert!(
            p99 >= Duration::from_millis(9),
            "P99 should be >= 9ms, got {:?}",
            p99
        );
        assert!(
            p99 <= Duration::from_millis(10),
            "P99 should be <= 10ms, got {:?}",
            p99
        );
    }

    #[test]
    fn per_peer_latency_p99() {
        let mut tracker = SkewTracker::new(200, 2_000 * MS);

        // 100 heartbeats: 98 at 1ms RTT, 2 at 50ms RTT.
        // P99 nearest-rank: rank = ceil(0.99 * 100) = 99, index = 98.
        // sorted: [1ms x 98, 50ms x 2] => sorted[98] = 50ms.
        for i in 0..98u64 {
            let sent = i * 10 * MS;
            simulate_heartbeat(&mut tracker, 1, sent, MS, 0);
        }
        for i in 98..100u64 {
            let sent = i * 10 * MS;
            simulate_heartbeat(&mut tracker, 1, sent, 50 * MS, 0);
        }

        let p99 = tracker.peer_rtt_p99(1).unwrap();
        assert!(
            p99 >= Duration::from_millis(49),
            "P99 should be >= 49ms, got {:?}",
            p99
        );
        assert!(
            p99 <= Duration::from_millis(50),
            "P99 should be <= 50ms, got {:?}",
            p99
        );
    }

    #[test]
    fn skew_max_measurement() {
        let mut tracker = SkewTracker::new(100, 2_000 * MS);

        // Three peers with different clock offsets.
        for i in 0..20u64 {
            let t = i * 50 * MS;
            simulate_heartbeat(&mut tracker, 1, t, MS, 5 * MS as i64); // +5ms
            simulate_heartbeat(&mut tracker, 2, t, MS, 3 * MS as i64); // +3ms
            simulate_heartbeat(&mut tracker, 3, t, MS, MS as i64); // +1ms
        }

        let skew = tracker.skew_max();
        // SkewMax should be ~5ms (the P99.9 of offsets, dominated by peer 1).
        assert!(
            skew >= Duration::from_millis(4),
            "SkewMax should be >= 4ms, got {:?}",
            skew
        );
        assert!(
            skew <= Duration::from_millis(6),
            "SkewMax should be <= 6ms, got {:?}",
            skew
        );
    }

    #[test]
    fn skew_outlier_rejection() {
        let mut tracker = SkewTracker::new(100, 2_000 * MS);

        // Ten stable peers with ~2ms offset (enough to make the outlier > 3 sigma).
        for i in 0..50u64 {
            let t = i * 20 * MS;
            for pid in 1..=10u64 {
                simulate_heartbeat(&mut tracker, pid, t, MS, 2 * MS as i64);
            }
        }

        // One unstable peer with 500ms offset.
        for i in 0..50u64 {
            let t = i * 20 * MS;
            simulate_heartbeat(&mut tracker, 99, t, MS, 500 * MS as i64);
        }

        assert!(
            tracker.is_outlier(99),
            "peer 99 with 500ms offset should be an outlier"
        );
        assert!(
            !tracker.is_outlier(1),
            "peer 1 with 2ms offset should not be an outlier"
        );

        // SkewMax should NOT be inflated by the outlier.
        let skew = tracker.skew_max();
        assert!(
            skew < Duration::from_millis(50),
            "SkewMax should be well below 50ms without the outlier, got {:?}",
            skew
        );
    }

    #[test]
    fn skew_hard_ceiling() {
        // Set ceiling to 100ms.
        let ceiling = 100 * MS;
        let mut tracker = SkewTracker::new(100, ceiling);

        // Peer with enormous clock offset (10 seconds).
        for i in 0..50u64 {
            let t = i * 20 * MS;
            simulate_heartbeat(&mut tracker, 1, t, MS, 10_000 * MS as i64);
        }

        let skew = tracker.skew_max();
        assert!(
            skew <= Duration::from_nanos(ceiling),
            "SkewMax should not exceed ceiling of {:?}, got {:?}",
            Duration::from_nanos(ceiling),
            skew
        );
    }

    #[test]
    fn reject_future_timestamp_preaccept() {
        let mut tracker = SkewTracker::new(100, 2_000 * MS);

        // Establish skew_max of ~10ms.
        for i in 0..50u64 {
            let t = i * 20 * MS;
            simulate_heartbeat(&mut tracker, 1, t, MS, 10 * MS as i64);
        }

        let skew = tracker.skew_max();
        assert!(
            skew >= Duration::from_millis(9),
            "Setup: SkewMax should be ~10ms, got {:?}",
            skew
        );

        // A PreAccept timestamp 15ms in the future from local perspective.
        let local_now = 1_000_000 * MS; // arbitrary "now"
        let t0 = local_now + 15 * MS; // 15ms ahead

        // Detection: t0 > local_now + skew_max means this timestamp is
        // beyond what clock skew can explain.
        let max_acceptable = local_now + skew.as_nanos() as u64;
        assert!(
            t0 > max_acceptable,
            "t0 ({}) should exceed max acceptable ({})",
            t0,
            max_acceptable
        );
    }

    #[test]
    fn accept_past_timestamp_preaccept() {
        let mut tracker = SkewTracker::new(100, 2_000 * MS);

        // Establish skew_max of ~10ms.
        for i in 0..50u64 {
            let t = i * 20 * MS;
            simulate_heartbeat(&mut tracker, 1, t, MS, 10 * MS as i64);
        }

        let skew = tracker.skew_max();

        // A PreAccept timestamp 5ms in the past — should be fine.
        let local_now = 1_000_000 * MS;
        let t0 = local_now - 5 * MS;

        let max_acceptable = local_now + skew.as_nanos() as u64;
        assert!(
            t0 <= max_acceptable,
            "t0 ({}) should be within max acceptable ({})",
            t0,
            max_acceptable
        );
    }
}
