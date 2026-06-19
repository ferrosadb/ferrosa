//! Distributed-read planning (W-002).
//!
//! The CQL `SELECT` path supports `token(pk)` predicates, so a per-token-range
//! Flight read is just the base `SELECT` plus a `token(...)` bound. This module
//! generates those bounded statements. `GetFlightInfo` emits one
//! `FlightEndpoint` per ring token range — each ticket a bounded `SELECT`,
//! located on the replica(s) that own the range — letting a client read ranges
//! in parallel.
//!
//! [`distributed_endpoints`] is the pure planner: given the live [`TokenRing`],
//! the keyspace [`ReplicationStrategy`], the table's partition-key columns, and
//! a resolver mapping a ring node-id to that node's advertised Flight address,
//! it returns one [`EndpointPlan`] per range. The wiring in
//! [`crate::service`]`::get_flight_info` supplies the resolver (local node from
//! `FERROSA_FLIGHT_BROADCAST`; remote nodes from the ring's internode host +
//! the configured Flight port) and turns each plan into a `FlightEndpoint`.
//!
//! Standalone / empty ring (no cluster topology) yields a single plan carrying
//! the unbounded base `SELECT` and no location, which preserves the prior
//! single-self-endpoint behavior.

use ferrosa_cluster::ring::strategy::ReplicationStrategy;
use ferrosa_cluster::ring::TokenRing;

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

/// One distributed Flight endpoint: a token-range-scoped `SELECT` ticket plus
/// the advertised Flight addresses of the replicas that own the range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointPlan {
    /// CQL the client should redeem via `DoGet` (bounded to this range, unless
    /// it is the single whole-ring fallback, which carries the base `SELECT`).
    pub ticket_cql: String,
    /// Advertised Flight addresses (e.g. `grpc://host:port`) of the replicas
    /// that own this range. May be empty when a replica's address cannot be
    /// resolved (the client then falls back to the connection it asked on).
    pub locations: Vec<String>,
}

/// Enumerate every half-open `(start, end]` token range over the ring, paired
/// with the replica node-ids that own it under `strategy`.
///
/// Mirrors the partitioning used by repair's range enumeration: each adjacent
/// pair of distinct ring tokens is a range, owned by the replica set of the
/// range's *start* token. The wrap-around segment from `i64::MIN` to the
/// smallest token is emitted explicitly so callers never special-case it.
///
/// Returns an empty vec for an empty ring — the caller treats that as the
/// standalone single-endpoint case.
pub fn ring_token_ranges(ring: &TokenRing, strategy: &ReplicationStrategy) -> Vec<RangeOwners> {
    let mut tokens: Vec<i64> = ring
        .node_ids()
        .iter()
        .flat_map(|&n| ring.tokens_for_node(n))
        .collect();
    tokens.sort_unstable();
    tokens.dedup();
    if tokens.is_empty() {
        return Vec::new();
    }

    let mut ranges = Vec::with_capacity(tokens.len() + 1);
    let first = tokens[0];
    // Leading wrap segment (MIN, first] when the ring does not start at MIN.
    if first != i64::MIN {
        ranges.push(RangeOwners {
            start: i64::MIN,
            end: first,
            replicas: ring.replicas_for_strategy(i64::MIN, strategy),
        });
    }
    for (i, &start) in tokens.iter().enumerate() {
        let end = tokens.get(i + 1).copied().unwrap_or(i64::MAX);
        ranges.push(RangeOwners {
            start,
            end,
            replicas: ring.replicas_for_strategy(start, strategy),
        });
    }
    ranges
}

/// A token range and the replica node-ids that own it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeOwners {
    pub start: i64,
    pub end: i64,
    pub replicas: Vec<u64>,
}

/// Build one [`EndpointPlan`] per ring token range for `base_select`.
///
/// `resolve_flight_addr` maps a replica's ring node-id to its advertised Flight
/// address; it returns `None` when the address is unknown (e.g. a node with no
/// Flight broadcast configured), in which case that replica is simply omitted
/// from the range's locations — the endpoint is still emitted so the data is
/// not dropped, and the client falls back to the connection it queried on.
///
/// When the ring is empty (standalone / no cluster topology), a single plan is
/// returned carrying the unbounded `base_select` and no location — preserving
/// the prior single-self-endpoint behavior.
pub fn distributed_endpoints<F>(
    base_select: &str,
    pk_columns: &[String],
    ring: &TokenRing,
    strategy: &ReplicationStrategy,
    mut resolve_flight_addr: F,
) -> Vec<EndpointPlan>
where
    F: FnMut(u64) -> Option<String>,
{
    let ranges = ring_token_ranges(ring, strategy);
    if ranges.is_empty() {
        return vec![EndpointPlan {
            ticket_cql: base_select.to_string(),
            locations: Vec::new(),
        }];
    }
    ranges
        .into_iter()
        .map(|r| EndpointPlan {
            ticket_cql: token_bounded_select(base_select, pk_columns, r.start, r.end),
            locations: r
                .replicas
                .iter()
                .filter_map(|&nid| resolve_flight_addr(nid))
                .collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_cluster::raft::{NodeInfo, NodeState};
    use uuid::Uuid;

    fn ring_node(addr: &str) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        }
    }

    fn simple(rf: usize) -> ReplicationStrategy {
        ReplicationStrategy::Simple {
            replication_factor: rf,
        }
    }

    #[test]
    fn empty_ring_yields_single_unbounded_endpoint() {
        let ring = TokenRing::new();
        let plans = distributed_endpoints(
            "SELECT * FROM ks.t",
            &["id".to_string()],
            &ring,
            &simple(1),
            |_| Some("grpc://node:50051".to_string()),
        );
        assert_eq!(plans.len(), 1, "standalone/empty ring → one endpoint");
        assert_eq!(plans[0].ticket_cql, "SELECT * FROM ks.t");
        assert!(
            plans[0].locations.is_empty(),
            "self endpoint carries no explicit location"
        );
    }

    #[test]
    fn multi_node_ring_yields_endpoint_per_range_with_distinct_tickets() {
        let mut ring = TokenRing::new();
        ring.add_node(1, ring_node("10.0.0.1:7000"));
        ring.add_node(2, ring_node("10.0.0.2:7000"));
        ring.add_node(3, ring_node("10.0.0.3:7000"));
        ring.assign_tokens(1, &[0]);
        ring.assign_tokens(2, &[100]);
        ring.assign_tokens(3, &[200]);

        let plans = distributed_endpoints(
            "SELECT id FROM ks.t",
            &["id".to_string()],
            &ring,
            &simple(1),
            |nid| Some(format!("grpc://10.0.0.{nid}:50051")),
        );

        // Ranges: (MIN,0], (0,100], (100,200], (200,MAX] → 4 ranges.
        assert_eq!(plans.len(), 4, "one endpoint per ring range: {plans:?}");

        // Every ticket is a distinct token-bounded SELECT.
        let tickets: std::collections::HashSet<&str> =
            plans.iter().map(|p| p.ticket_cql.as_str()).collect();
        assert_eq!(tickets.len(), plans.len(), "distinct range tickets");
        for p in &plans {
            assert!(
                p.ticket_cql.contains("token(id) >") && p.ticket_cql.contains("token(id) <="),
                "ticket carries token bounds: {}",
                p.ticket_cql
            );
            assert_eq!(p.locations.len(), 1, "RF=1 → exactly one replica location");
            assert!(
                p.locations[0].starts_with("grpc://10.0.0."),
                "location is the owning replica's Flight addr: {:?}",
                p.locations
            );
        }
    }

    #[test]
    fn rf_greater_than_one_advertises_multiple_replica_locations() {
        let mut ring = TokenRing::new();
        ring.add_node(1, ring_node("10.0.0.1:7000"));
        ring.add_node(2, ring_node("10.0.0.2:7000"));
        ring.add_node(3, ring_node("10.0.0.3:7000"));
        ring.assign_tokens(1, &[0]);
        ring.assign_tokens(2, &[100]);
        ring.assign_tokens(3, &[200]);

        let plans = distributed_endpoints(
            "SELECT id FROM ks.t",
            &["id".to_string()],
            &ring,
            &simple(2),
            |nid| Some(format!("grpc://10.0.0.{nid}:50051")),
        );
        assert!(
            plans.iter().all(|p| p.locations.len() == 2),
            "RF=2 → two replica locations per range: {plans:?}"
        );
    }

    #[test]
    fn unresolvable_replica_address_is_omitted_not_faked() {
        let mut ring = TokenRing::new();
        ring.add_node(1, ring_node("10.0.0.1:7000"));
        ring.add_node(2, ring_node("10.0.0.2:7000"));
        ring.assign_tokens(1, &[0]);
        ring.assign_tokens(2, &[100]);

        // Node 2 has no resolvable Flight address.
        let plans = distributed_endpoints(
            "SELECT id FROM ks.t",
            &["id".to_string()],
            &ring,
            &simple(1),
            |nid| (nid == 1).then(|| "grpc://10.0.0.1:50051".to_string()),
        );
        // Ranges owned by node 2 have zero locations (no fabricated address),
        // but the endpoint is still present so the data is reachable.
        assert!(
            plans.iter().any(|p| p.locations.is_empty()),
            "node-2 range emits an endpoint with no faked location: {plans:?}"
        );
        assert!(
            plans
                .iter()
                .any(|p| p.locations == vec!["grpc://10.0.0.1:50051"]),
            "node-1 ranges still carry the real location: {plans:?}"
        );
    }
}

#[cfg(test)]
mod token_bound_tests {
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
