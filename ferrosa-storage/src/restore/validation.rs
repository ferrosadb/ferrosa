//! Validation and filtering functions for PITR commit log replay.
//!
//! All functions here are pure (no I/O) and operate over in-memory data.
//! They are called during restore to:
//! - Validate the archived segment sequence is contiguous (no gaps).
//! - Filter mutations by timestamp for precise point-in-time cutoffs.
//! - Filter mutations by schema to skip dropped tables.

use std::collections::HashSet;

use crate::commitlog::mutation::Mutation;

/// Validates that a node ID matches the expected node.
///
/// Returns `Ok(())` if `node_id == expected`, otherwise returns an error
/// describing the mismatch. Used during restore to confirm archive segments
/// originated from the intended node. If `force` is true, mismatches are allowed.
pub fn validate_node_id(node_id: &str, expected: &str, force: bool) -> ferrosa_common::Result<()> {
    if node_id == expected || force {
        Ok(())
    } else {
        Err(ferrosa_common::Error::InvalidFormat(format!(
            "node ID mismatch: expected {expected:?}, got {node_id:?}; use force=true to override"
        )))
    }
}

/// Validates that archived segment IDs form a contiguous sequence.
///
/// Returns `Ok(())` if all segments from `min_segment_id` to `max_segment_id`
/// are present. Returns `Err` listing missing segment IDs if gaps exist.
pub fn validate_segment_continuity(
    available_segment_ids: &[u64],
    min_segment_id: u64,
) -> ferrosa_common::Result<()> {
    if available_segment_ids.is_empty() {
        return Ok(()); // No segments to validate
    }
    let max_id = *available_segment_ids.iter().max().unwrap();
    let available: HashSet<u64> = available_segment_ids.iter().copied().collect();
    let missing: Vec<u64> = (min_segment_id..=max_id)
        .filter(|id| !available.contains(id))
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(ferrosa_common::Error::InvalidFormat(format!(
            "archive has gaps: missing segment IDs {:?}",
            missing
        )))
    }
}

/// Filters mutations to include only those at or before the given timestamp.
///
/// The boundary is inclusive: mutations with `timestamp == point_in_time` ARE included.
/// This matches Cassandra's PITR semantics.
pub fn filter_mutations_by_timestamp(
    mutations: Vec<Mutation>,
    point_in_time: i64,
) -> Vec<Mutation> {
    mutations
        .into_iter()
        .filter(|m| m.timestamp <= point_in_time)
        .collect()
}

/// Filters mutations, keeping only those whose (keyspace, table) exists
/// in the provided set of known tables.
///
/// Unknown tables are logged and skipped — not an error. This handles
/// the case where a table was dropped after the snapshot was taken.
pub fn filter_mutations_by_schema(
    mutations: Vec<Mutation>,
    known_tables: &HashSet<(String, String)>,
) -> (Vec<Mutation>, Vec<String>) {
    let mut kept = Vec::new();
    let mut warnings = Vec::new();

    for m in mutations {
        let key = (m.keyspace.clone(), m.table.clone());
        if known_tables.contains(&key) {
            kept.push(m);
        } else {
            warnings.push(format!(
                "skipping mutation for unknown table {}.{}",
                m.keyspace, m.table
            ));
        }
    }

    (kept, warnings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{DecoratedKey, PartitionKey};

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /// Creates a minimal Mutation with the given timestamp.
    fn make_mutation(timestamp: i64) -> Mutation {
        Mutation {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"k".to_vec())),
            rows: vec![],
            timestamp,
        }
    }

    /// Creates a minimal Mutation for a specific (keyspace, table) with the given timestamp.
    fn make_mutation_for(keyspace: &str, table: &str, timestamp: i64) -> Mutation {
        Mutation {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"k".to_vec())),
            rows: vec![],
            timestamp,
        }
    }

    // ---------------------------------------------------------------------------
    // Task 3.1: Node-ID validation
    // ---------------------------------------------------------------------------

    #[test]
    fn node_id_match_passes() {
        assert!(validate_node_id("node-1", "node-1", false).is_ok());
    }

    #[test]
    fn node_id_mismatch_fails() {
        let err = validate_node_id("node-2", "node-1", false).unwrap_err();
        assert!(
            format!("{err}").contains("node-1"),
            "error should mention expected node id"
        );
        assert!(
            format!("{err}").contains("node-2"),
            "error should mention actual node id"
        );
    }

    #[test]
    fn node_id_mismatch_with_force_passes() {
        assert!(validate_node_id("node-2", "node-1", true).is_ok());
    }

    // ---------------------------------------------------------------------------
    // Task 3.2: Segment continuity validation
    // ---------------------------------------------------------------------------

    #[test]
    fn contiguous_segments_pass() {
        assert!(validate_segment_continuity(&[5, 6, 7, 8], 5).is_ok());
    }

    #[test]
    fn gap_detected() {
        let err = validate_segment_continuity(&[5, 7, 8], 5).unwrap_err();
        assert!(format!("{err}").contains("6"));
    }

    #[test]
    fn empty_segments_pass() {
        assert!(validate_segment_continuity(&[], 1).is_ok());
    }

    #[test]
    fn single_segment_no_gap() {
        assert!(validate_segment_continuity(&[3], 3).is_ok());
    }

    #[test]
    fn multiple_gaps_all_reported() {
        let err = validate_segment_continuity(&[1, 4, 7], 1).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("2"), "gap at 2 should be reported");
        assert!(msg.contains("3"), "gap at 3 should be reported");
        assert!(msg.contains("5"), "gap at 5 should be reported");
        assert!(msg.contains("6"), "gap at 6 should be reported");
    }

    #[test]
    fn segments_out_of_order_still_valid() {
        // Order doesn't matter — we only care about the set.
        assert!(validate_segment_continuity(&[8, 5, 7, 6], 5).is_ok());
    }

    // ---------------------------------------------------------------------------
    // Task 3.3: Timestamp filtering
    // ---------------------------------------------------------------------------

    #[test]
    fn filter_keeps_mutations_at_boundary() {
        let mutations = vec![
            make_mutation(1000),
            make_mutation(2000),
            make_mutation(3000),
        ];
        let filtered = filter_mutations_by_timestamp(mutations, 2000);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].timestamp, 1000);
        assert_eq!(filtered[1].timestamp, 2000); // inclusive boundary
    }

    #[test]
    fn filter_excludes_future_mutations() {
        let mutations = vec![make_mutation(5000)];
        let filtered = filter_mutations_by_timestamp(mutations, 3000);
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_keeps_all_when_pit_is_max() {
        let mutations = vec![make_mutation(1000), make_mutation(2000)];
        let filtered = filter_mutations_by_timestamp(mutations, i64::MAX);
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn filter_empty_input_returns_empty() {
        let filtered = filter_mutations_by_timestamp(vec![], 1000);
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_all_excluded() {
        let mutations = vec![make_mutation(100), make_mutation(200)];
        let filtered = filter_mutations_by_timestamp(mutations, 50);
        assert!(filtered.is_empty());
    }

    // ---------------------------------------------------------------------------
    // Task 3.5: Schema validation during replay
    // ---------------------------------------------------------------------------

    #[test]
    fn schema_filter_keeps_known_tables() {
        let known: HashSet<(String, String)> =
            [("ks".into(), "users".into())].into_iter().collect();
        let mutations = vec![
            make_mutation_for("ks", "users", 1000),
            make_mutation_for("ks", "dropped_table", 2000),
        ];
        let (kept, warnings) = filter_mutations_by_schema(mutations, &known);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].table, "users");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("dropped_table"));
    }

    #[test]
    fn schema_filter_all_known() {
        let known: HashSet<(String, String)> = [("ks".into(), "t".into())].into_iter().collect();
        let mutations = vec![make_mutation_for("ks", "t", 1000)];
        let (kept, warnings) = filter_mutations_by_schema(mutations, &known);
        assert_eq!(kept.len(), 1);
        assert!(warnings.is_empty());
    }

    #[test]
    fn schema_filter_all_unknown() {
        let known: HashSet<(String, String)> = HashSet::new();
        let mutations = vec![
            make_mutation_for("ks", "t1", 1000),
            make_mutation_for("ks", "t2", 2000),
        ];
        let (kept, warnings) = filter_mutations_by_schema(mutations, &known);
        assert!(kept.is_empty());
        assert_eq!(warnings.len(), 2);
    }

    #[test]
    fn schema_filter_warning_mentions_keyspace_and_table() {
        let known: HashSet<(String, String)> = HashSet::new();
        let mutations = vec![make_mutation_for("my_ks", "my_table", 1000)];
        let (_, warnings) = filter_mutations_by_schema(mutations, &known);
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("my_ks"),
            "warning should contain keyspace"
        );
        assert!(
            warnings[0].contains("my_table"),
            "warning should contain table"
        );
    }

    #[test]
    fn schema_filter_cross_keyspace() {
        let known: HashSet<(String, String)> =
            [("ks1".into(), "t".into()), ("ks2".into(), "t".into())]
                .into_iter()
                .collect();
        // Same table name but different keyspace — both known.
        let mutations = vec![
            make_mutation_for("ks1", "t", 1000),
            make_mutation_for("ks2", "t", 2000),
            make_mutation_for("ks3", "t", 3000), // unknown keyspace
        ];
        let (kept, warnings) = filter_mutations_by_schema(mutations, &known);
        assert_eq!(kept.len(), 2);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("ks3"));
    }
}
