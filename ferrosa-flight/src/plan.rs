//! Distributed-read planning (W-002).
//!
//! The CQL `SELECT` path supports `token(pk)` predicates, so a per-token-range
//! Flight read is just the base `SELECT` plus a `token(...)` bound. This module
//! generates those bounded statements. `GetFlightInfo` will emit one
//! `FlightEndpoint` per ring token range — each ticket a bounded `SELECT`,
//! located on the replica that owns the range — letting a client read ranges in
//! parallel.
//!
//! Cluster-dependent prerequisites (tracked follow-ups, NOT in this module):
//! ring token-range enumeration from `cluster_state`, and advertising each
//! node's Flight gRPC address (the topology currently tracks only the
//! internode/CQL addresses), needed for `FlightEndpoint` locations.

/// Append a half-open `(start, end]` token-range bound to a base `SELECT`.
///
/// `pk_columns` are the partition-key columns for the `token(...)` call (in key
/// order). The half-open convention matches Cassandra's ring ranges; the full
/// ring is one `(MIN, MAX]` range, so wrap-around is avoided by treating MIN as
/// an exclusive sentinel.
pub fn token_bounded_select(
    base_select: &str,
    pk_columns: &[String],
    start: i64,
    end: i64,
) -> String {
    let pks = pk_columns.join(", ");
    let bound = format!("token({pks}) > {start} AND token({pks}) <= {end}");
    if base_select.to_ascii_lowercase().contains(" where ") {
        format!("{base_select} AND {bound}")
    } else {
        format!("{base_select} WHERE {bound}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_where_when_absent() {
        let s = token_bounded_select("SELECT id FROM ks.t", &["id".to_string()], -10, 20);
        assert_eq!(
            s,
            "SELECT id FROM ks.t WHERE token(id) > -10 AND token(id) <= 20"
        );
    }

    #[test]
    fn appends_and_when_where_present() {
        let s = token_bounded_select(
            "SELECT id FROM ks.t WHERE id = 5",
            &["id".to_string()],
            0,
            9,
        );
        assert_eq!(
            s,
            "SELECT id FROM ks.t WHERE id = 5 AND token(id) > 0 AND token(id) <= 9"
        );
    }

    #[test]
    fn composite_partition_key() {
        let s = token_bounded_select(
            "SELECT * FROM ks.t",
            &["a".to_string(), "b".to_string()],
            1,
            2,
        );
        assert!(s.contains("token(a, b) > 1 AND token(a, b) <= 2"), "{s}");
    }
}
