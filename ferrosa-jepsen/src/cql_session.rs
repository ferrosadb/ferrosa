//! CQL transport for Jepsen workloads with stable coordinator affinity.
//! Correctness: every transaction statement uses one coordinator selected from
//! the executing session's own metadata, and CQL results preserve row values.
//! Last revised: 2026-08-08
//! Last changed: Select the pinned coordinator from the destination session.

use std::num::NonZeroUsize;

use anyhow::{Context, Result};
use async_trait::async_trait;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::PoolSize;
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::value::{CqlValue, Row};
use uuid::Uuid;

use crate::workload::CqlSession;

/// A real CQL session backed by the scylla driver.
///
/// Connects to a Ferrosa (or Cassandra-compatible) cluster and executes
/// CQL queries, returning results as string-pair rows for workload inspection.
pub struct ScyllaCqlSession {
    session: Session,
}

fn coordinator_identifier(host_id: Uuid) -> NodeIdentifier {
    NodeIdentifier::HostId(host_id)
}

fn coordinator_host_id<'a>(host_ids: impl IntoIterator<Item = &'a Uuid>) -> Result<Uuid> {
    host_ids
        .into_iter()
        .copied()
        .min()
        .context("cluster reports no known nodes to pin to")
}

impl ScyllaCqlSession {
    /// Connect to the cluster at the given contact points (e.g. `["127.0.0.1:9042"]`).
    ///
    /// The session **pins every query to a single coordinator node** so a
    /// `BEGIN…COMMIT` block's statements share one connection — the server buffers
    /// a transaction per connection, so without affinity the token-aware pool
    /// would scatter the statements across coordinators and the buffered DML would
    /// never reach the `BEGIN`'s connection (silently non-atomic).
    ///
    /// Pinning uses a stable host ID discovered from the destination session's
    /// own cluster metadata. Selecting it from a separate probe session is not
    /// safe: topology discovery can yield different node sets, leaving the
    /// destination policy with an ID it cannot resolve and an empty query plan.
    /// The full topology stays connected (`known_nodes` + `PoolSize(1)`); the
    /// pinned node forwards writes as the Accord coordinator, so cross-shard
    /// fan-out still happens.
    pub async fn connect(contact_points: &[String]) -> Result<Self> {
        assert!(
            !contact_points.is_empty(),
            "contact_points must not be empty"
        );

        // Build with the normal policy so topology discovery can complete, then
        // remap the same session's profile to a coordinator that its metadata
        // can resolve. ExecutionProfileHandle remapping is observed by the
        // session because the builder receives a clone of this handle.
        let mut profile_handle = ExecutionProfile::builder().build().into_handle();
        let session = SessionBuilder::new()
            .known_nodes(contact_points)
            .default_execution_profile_handle(profile_handle.clone())
            .pool_size(PoolSize::PerHost(
                NonZeroUsize::new(1).expect("1 is nonzero"),
            ))
            .build()
            .await
            .context("failed to connect to CQL cluster")?;
        let target_host_id = coordinator_host_id(
            session
                .get_cluster_state()
                .get_nodes_info()
                .iter()
                .map(|node| &node.host_id),
        )?;

        let policy =
            SingleTargetLoadBalancingPolicy::new(coordinator_identifier(target_host_id), None);
        let profile = ExecutionProfile::builder()
            .load_balancing_policy(policy)
            .build();
        profile_handle.map_to_another_profile(profile);
        Ok(Self { session })
    }
}

/// Convert a `CqlValue` to its string representation for history recording.
fn cql_value_to_string(value: Option<CqlValue>) -> String {
    match value {
        None => "null".to_string(),
        Some(v) => match v {
            CqlValue::Ascii(s) | CqlValue::Text(s) => s,
            CqlValue::BigInt(n) => n.to_string(),
            CqlValue::Int(n) => n.to_string(),
            CqlValue::SmallInt(n) => n.to_string(),
            CqlValue::TinyInt(n) => n.to_string(),
            CqlValue::Counter(c) => c.0.to_string(),
            CqlValue::Float(f) => f.to_string(),
            CqlValue::Double(d) => d.to_string(),
            CqlValue::Boolean(b) => b.to_string(),
            CqlValue::Blob(bytes) => format!("0x{}", hex_encode(&bytes)),
            CqlValue::Uuid(u) => u.to_string(),
            CqlValue::Timeuuid(t) => t.to_string(),
            CqlValue::Inet(addr) => addr.to_string(),
            CqlValue::Timestamp(ts) => ts.0.to_string(),
            CqlValue::Date(d) => d.0.to_string(),
            CqlValue::Time(t) => t.0.to_string(),
            other => format!("{other:?}"),
        },
    }
}

/// Minimal hex encoding without external dependencies.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[async_trait]
impl CqlSession for ScyllaCqlSession {
    async fn execute(&self, query: &str) -> Result<Vec<Vec<(String, String)>>> {
        assert!(!query.is_empty(), "query must not be empty");
        // Flatten the driver error into the message (not just an anyhow context)
        // so the workload's `e.to_string()` surfaces the ROOT cause — a bare
        // "executing query: …" with the real failure hidden is a fail-loud gap
        // that masked a connectivity regression once already.
        let result = self
            .session
            .query_unpaged(query, &[])
            .await
            .map_err(|e| anyhow::anyhow!("executing query `{query}`: {e}"))?;

        // Non-SELECT statements (CREATE, INSERT, UPDATE, DELETE) produce no rows.
        let rows_result = match result.into_rows_result() {
            Ok(rr) => rr,
            Err(_) => return Ok(vec![]),
        };

        let col_names: Vec<String> = rows_result
            .column_specs()
            .iter()
            .map(|spec| spec.name().to_string())
            .collect();

        let mut output = Vec::with_capacity(rows_result.rows_num());
        for row_result in rows_result.rows::<Row>()? {
            let row = row_result.context("deserializing row")?;
            assert_eq!(
                row.columns.len(),
                col_names.len(),
                "column count mismatch between metadata and row data"
            );
            let pairs: Vec<(String, String)> = col_names
                .iter()
                .zip(row.columns)
                .map(|(name, val)| (name.clone(), cql_value_to_string(val)))
                .collect();
            output.push(pairs);
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinning_uses_a_host_id_identifier() {
        let host_id = Uuid::new_v4();

        match coordinator_identifier(host_id) {
            NodeIdentifier::HostId(actual) => assert_eq!(actual, host_id),
            other => panic!("expected host-ID coordinator pin, got {other:?}"),
        }
    }

    #[test]
    fn pinning_selects_a_coordinator_from_destination_metadata() {
        let destination_host_ids = [
            Uuid::parse_str("00000000-0000-0000-0000-000000000020").unwrap(),
            Uuid::parse_str("00000000-0000-0000-0000-000000000010").unwrap(),
        ];

        let selected = coordinator_host_id(destination_host_ids.iter()).unwrap();

        assert_eq!(selected, destination_host_ids[1]);
    }

    #[test]
    fn pinning_fails_loudly_when_destination_metadata_is_empty() {
        let destination_host_ids: [Uuid; 0] = [];

        let error = coordinator_host_id(destination_host_ids.iter()).unwrap_err();

        assert_eq!(
            error.to_string(),
            "cluster reports no known nodes to pin to"
        );
    }

    /// Verify that ScyllaCqlSession connects to a real cluster and can execute a
    /// system query. Requires FERROSA_TEST_CONTAINERS=1 and a cluster on port 49042.
    #[cfg(feature = "live-infra-tests")]
    #[tokio::test]
    async fn rust_driver_connects_to_cluster() {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            panic!(
                "FERROSA_TEST_CONTAINERS not set — \
                 start a Cassandra-compatible cluster on port 49042 first \
                 (e.g. via docker-compose in ferrosa-jepsen/docker)"
            );
        }
        let session = ScyllaCqlSession::connect(&["localhost:49042".to_string()])
            .await
            .expect("connect to cluster");
        let rows = session
            .execute("SELECT now() FROM system.local")
            .await
            .expect("execute system query");
        assert!(
            !rows.is_empty(),
            "SELECT from system.local must return at least one row"
        );
        let (col_name, _val) = &rows[0][0];
        assert_eq!(
            col_name, "now()",
            "column name must match the selected expression"
        );
    }

    #[test]
    fn cql_value_to_string_conversions() {
        assert_eq!(cql_value_to_string(None), "null");
        assert_eq!(cql_value_to_string(Some(CqlValue::Int(42))), "42");
        assert_eq!(
            cql_value_to_string(Some(CqlValue::Text("hello".into()))),
            "hello"
        );
        assert_eq!(cql_value_to_string(Some(CqlValue::Boolean(true))), "true");
        assert_eq!(cql_value_to_string(Some(CqlValue::BigInt(-1))), "-1");
    }

    #[test]
    fn hex_encode_basic() {
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
        assert_eq!(hex_encode(&[]), "");
        assert_eq!(hex_encode(&[0x00]), "00");
    }
}
