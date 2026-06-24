use anyhow::{Context, Result};
use async_trait::async_trait;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::value::{CqlValue, Row};

use crate::workload::CqlSession;

/// A real CQL session backed by the scylla driver.
///
/// Connects to a Ferrosa (or Cassandra-compatible) cluster and executes
/// CQL queries, returning results as string-pair rows for workload inspection.
pub struct ScyllaCqlSession {
    session: Session,
}

impl ScyllaCqlSession {
    /// Connect to the cluster at the given contact points (e.g. `["127.0.0.1:9042"]`).
    ///
    /// Uses the standard multi-node `known_nodes` session (the proven path the
    /// nightly multi-DC runner relies on). The driver discovers the full topology
    /// and routes each query token-aware.
    ///
    /// NOTE on transactions: a `BEGIN…COMMIT` block buffers on one *server
    /// connection*, so to be atomic its statements must all reach the same
    /// connection. The token-aware pool can scatter them across coordinators, so
    /// the atomic-transaction bank workload needs connection affinity — a
    /// **single-node load-balancing policy** that pins every query to one
    /// coordinator while keeping the full topology connected (so the cluster
    /// stays reachable). An earlier attempt to pin via `AllowListHostFilter`
    /// broke connectivity: the filter matches a node's *advertised* address,
    /// which differs from the port-mapped contact point in Docker, so it filtered
    /// out the only node and every query failed. See t_ec740b8f.
    pub async fn connect(contact_points: &[String]) -> Result<Self> {
        assert!(
            !contact_points.is_empty(),
            "contact_points must not be empty"
        );
        let session = SessionBuilder::new()
            .known_nodes(contact_points)
            .build()
            .await
            .context("failed to connect to CQL cluster")?;
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
