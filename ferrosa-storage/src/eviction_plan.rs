//! Eviction order for uploaded SSTables held in the local disk cache.
//!
//! Local disk is a cache over the object store, so an uploaded SSTable may be
//! evicted and read back on demand. Which ones go first matters: on
//! 2026-09-29 the evictor ordered candidates by file mtime (write age), so a
//! table that is read constantly but rarely written — `schema_version` — was
//! first in line, and every read of it then paid an object-store round trip
//! or failed outright.
//!
//! The plan here keys on read recency instead:
//!
//! - A table read by a foreground query within the hot window is **hot**; none
//!   of its SSTables are eviction candidates. Their bytes are reported so the
//!   caller can say loudly when hot data alone keeps the cache over its limit.
//! - Cold candidates are ordered least-recently-read first. A table that has
//!   never been read since startup sorts before any table that has, and ties
//!   fall back to write age (oldest first).
//! - A hot window of zero disables hotness: every candidate is cold.
//!
//! The function is pure so the policy is tested without an engine.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

/// One uploaded, locally cached SSTable generation that eviction may remove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EvictionCandidate {
    /// Manifest table key, `<keyspace>.<table>`.
    pub table: String,
    /// SSTable generation id as recorded in the manifest.
    pub sstable_id: String,
    /// The table's local SSTable directory.
    pub table_dir: PathBuf,
    /// Local bytes the generation occupies.
    pub size: u64,
    /// Newest mtime across the generation's components (its write age).
    pub last_modified: SystemTime,
}

/// The candidates split into what may be evicted, in order, and what may not.
#[derive(Debug, Default)]
pub(crate) struct EvictionOrder {
    /// Evictable candidates, first-to-evict first.
    pub cold: Vec<EvictionCandidate>,
    /// Bytes held by SSTables of hot tables (never evicted).
    pub hot_bytes: u64,
    /// Hot tables, sorted, for the log line that names them.
    pub hot_tables: Vec<String>,
}

/// Orders `candidates` for eviction given each table's last foreground read.
///
/// `last_read` maps a manifest table key to the time of its most recent
/// foreground read; a table absent from it has not been read since startup.
pub(crate) fn order_for_eviction(
    candidates: Vec<EvictionCandidate>,
    last_read: &HashMap<String, SystemTime>,
    now: SystemTime,
    hot_window: Duration,
) -> EvictionOrder {
    let is_hot = |table: &str| -> bool {
        if hot_window.is_zero() {
            return false;
        }
        match last_read.get(table) {
            None => false,
            // A stamp ahead of `now` (clock stepped back) means a very recent read.
            Some(read_at) => match now.duration_since(*read_at) {
                Ok(age) => age < hot_window,
                Err(_) => true,
            },
        }
    };

    let mut order = EvictionOrder::default();
    let mut hot_tables = std::collections::BTreeSet::new();
    for candidate in candidates {
        if is_hot(&candidate.table) {
            order.hot_bytes = order.hot_bytes.saturating_add(candidate.size);
            hot_tables.insert(candidate.table);
        } else {
            order.cold.push(candidate);
        }
    }
    order.hot_tables = hot_tables.into_iter().collect();
    // `None` sorts before `Some`: never-read tables go first.
    order.cold.sort_by(|a, b| {
        let read_a = last_read.get(&a.table);
        let read_b = last_read.get(&b.table);
        read_a
            .cmp(&read_b)
            .then(a.last_modified.cmp(&b.last_modified))
            .then_with(|| a.table.cmp(&b.table))
            .then_with(|| a.sstable_id.cmp(&b.sstable_id))
    });
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: Duration = Duration::from_secs(3600);

    fn at(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn cand(table: &str, id: &str, size: u64, written: u64) -> EvictionCandidate {
        EvictionCandidate {
            table: table.to_string(),
            sstable_id: id.to_string(),
            table_dir: PathBuf::from(format!("/data/sstables/{table}")),
            size,
            last_modified: at(written),
        }
    }

    fn ids(order: &EvictionOrder) -> Vec<&str> {
        order.cold.iter().map(|c| c.sstable_id.as_str()).collect()
    }

    #[test]
    fn a_table_read_inside_the_hot_window_is_never_a_candidate() {
        // Given schema_version written long ago but read a minute ago, and a
        // bulk table written recently and never read.
        let now = at(100_000);
        let candidates = vec![
            cand("ks.schema_version", "1", 10, 1_000),
            cand("ks.bulk", "2", 500, 90_000),
        ];
        let last_read = HashMap::from([("ks.schema_version".to_string(), at(99_940))]);

        let order = order_for_eviction(candidates, &last_read, now, HOUR);

        // Then only the bulk table may be evicted, despite being newer.
        assert_eq!(ids(&order), vec!["2"]);
        assert_eq!(order.hot_bytes, 10);
        assert_eq!(order.hot_tables, vec!["ks.schema_version".to_string()]);
    }

    #[test]
    fn never_read_tables_are_evicted_before_read_ones_oldest_write_first() {
        let now = at(100_000);
        let candidates = vec![
            cand("ks.read_long_ago", "a", 1, 1_000),
            cand("ks.never_read", "b2", 1, 5_000),
            cand("ks.never_read", "b1", 1, 2_000),
        ];
        // Read, but outside the one-hour window.
        let last_read = HashMap::from([("ks.read_long_ago".to_string(), at(10_000))]);

        let order = order_for_eviction(candidates, &last_read, now, HOUR);

        assert_eq!(ids(&order), vec!["b1", "b2", "a"]);
        assert_eq!(order.hot_bytes, 0);
        assert!(order.hot_tables.is_empty());
    }

    #[test]
    fn cold_read_tables_are_evicted_least_recently_read_first() {
        let now = at(100_000);
        let candidates = vec![
            cand("ks.read_recently", "r", 1, 1_000),
            cand("ks.read_earlier", "e", 1, 50_000),
        ];
        let last_read = HashMap::from([
            ("ks.read_recently".to_string(), at(90_000)),
            ("ks.read_earlier".to_string(), at(20_000)),
        ]);

        let order = order_for_eviction(candidates, &last_read, now, HOUR);

        // Write age does not override read recency.
        assert_eq!(ids(&order), vec!["e", "r"]);
    }

    #[test]
    fn a_zero_hot_window_makes_every_table_cold() {
        let now = at(100_000);
        let candidates = vec![cand("ks.t", "1", 7, 1_000)];
        let last_read = HashMap::from([("ks.t".to_string(), now)]);

        let order = order_for_eviction(candidates, &last_read, now, Duration::ZERO);

        assert_eq!(ids(&order), vec!["1"]);
        assert_eq!(order.hot_bytes, 0);
    }

    #[test]
    fn hot_bytes_sum_every_generation_of_every_hot_table() {
        let now = at(100_000);
        let candidates = vec![
            cand("ks.h1", "1", 10, 1),
            cand("ks.h1", "2", 20, 2),
            cand("ks.h2", "3", 30, 3),
            cand("ks.cold", "4", 40, 4),
        ];
        let last_read = HashMap::from([
            ("ks.h1".to_string(), at(99_999)),
            ("ks.h2".to_string(), now),
        ]);

        let order = order_for_eviction(candidates, &last_read, now, HOUR);

        assert_eq!(ids(&order), vec!["4"]);
        assert_eq!(order.hot_bytes, 60);
        assert_eq!(
            order.hot_tables,
            vec!["ks.h1".to_string(), "ks.h2".to_string()]
        );
    }

    #[test]
    fn a_read_stamped_in_the_future_counts_as_hot() {
        // A wall-clock step backwards must not make a just-read table cold.
        let now = at(100_000);
        let candidates = vec![cand("ks.t", "1", 1, 1)];
        let last_read = HashMap::from([("ks.t".to_string(), at(100_500))]);

        let order = order_for_eviction(candidates, &last_read, now, HOUR);

        assert!(order.cold.is_empty());
        assert_eq!(order.hot_bytes, 1);
    }
}
