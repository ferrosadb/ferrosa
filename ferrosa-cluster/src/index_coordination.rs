//! Distributed index coordination types and logic.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Payload for an `IndexBuildRequest` net message.
///
/// Sent to a remote node (or indexer) to request an index build.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexBuildRequestPayload {
    /// S3 path(s) of SSTable data files to index.
    pub sstable_s3_paths: Vec<String>,
    /// Keyspace of the indexed table.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// Index name.
    pub index_name: String,
    /// Serialized index metadata (JSON).
    pub index_metadata_json: String,
    /// Serialized table schema (JSON) for column resolution.
    pub table_schema_json: String,
}

/// Payload for an `IndexBuildComplete` net message.
///
/// Sent back from the builder to the coordinator after a build finishes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexBuildCompletePayload {
    /// Keyspace of the indexed table.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// Index name.
    pub index_name: String,
    /// S3 paths of the produced sidecar index files.
    pub sidecar_s3_paths: Vec<String>,
    /// Whether the build succeeded.
    pub success: bool,
    /// Error message if the build failed.
    pub error: Option<String>,
}

/// Encode an [`IndexBuildRequestPayload`] to wire bytes.
pub fn encode_build_request(payload: &IndexBuildRequestPayload) -> Bytes {
    Bytes::from(bincode::serialize(payload).unwrap_or_default())
}

/// Decode an [`IndexBuildRequestPayload`] from wire bytes.
pub fn decode_build_request(bytes: &[u8]) -> Option<IndexBuildRequestPayload> {
    bincode::deserialize(bytes).ok()
}

/// Encode an [`IndexBuildCompletePayload`] to wire bytes.
pub fn encode_build_complete(payload: &IndexBuildCompletePayload) -> Bytes {
    Bytes::from(bincode::serialize(payload).unwrap_or_default())
}

/// Decode an [`IndexBuildCompletePayload`] from wire bytes.
pub fn decode_build_complete(bytes: &[u8]) -> Option<IndexBuildCompletePayload> {
    bincode::deserialize(bytes).ok()
}

/// Backend that offloads index builds to a remote node.
///
/// Sends `IndexBuildRequest` via `PeerManager` and waits for
/// `IndexBuildComplete`. The remote node reads SSTables from S3,
/// builds sidecar index files, uploads them to S3, and responds.
///
/// Implements `IndexBuildBackend` (from S-1) so it can be swapped
/// in place of `LocalBackend` on indexer-enabled nodes.
pub struct RemoteNodeBackend {
    /// UUID of the target node to send the build request to.
    pub target_node: uuid::Uuid,
}

impl RemoteNodeBackend {
    /// Request a remote index build.
    ///
    /// Currently returns an error (requires PeerManager wiring and S-1
    /// `IndexBuildBackend` trait). Will be completed when S-1 lands.
    pub async fn build_remote(
        &self,
        _keyspace: &str,
        _table: &str,
        _index_name: &str,
        _sstable_s3_paths: &[&str],
    ) -> Result<IndexBuildCompletePayload, String> {
        Err("RemoteNodeBackend: not yet wired to PeerManager".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_build_request_payload_serde_roundtrip() {
        let payload = IndexBuildRequestPayload {
            sstable_s3_paths: vec!["s3://bucket/ks/tbl/sst-001-Data.db".into()],
            keyspace: "ks".into(),
            table: "tbl".into(),
            index_name: "idx_email".into(),
            index_metadata_json: r#"{"name":"idx_email"}"#.into(),
            table_schema_json: r#"{"keyspace":"ks"}"#.into(),
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let decoded: IndexBuildRequestPayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn index_build_complete_payload_serde_roundtrip() {
        let payload = IndexBuildCompletePayload {
            keyspace: "ks".into(),
            table: "tbl".into(),
            index_name: "idx_email".into(),
            sidecar_s3_paths: vec!["s3://bucket/ks/tbl/idx_email.fxsi".into()],
            success: true,
            error: None,
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let decoded: IndexBuildCompletePayload = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn index_build_complete_payload_failure_case() {
        let payload = IndexBuildCompletePayload {
            keyspace: "ks".into(),
            table: "tbl".into(),
            index_name: "idx".into(),
            sidecar_s3_paths: vec![],
            success: false,
            error: Some("SSTable not found in S3".into()),
        };
        let bytes = bincode::serialize(&payload).unwrap();
        let decoded: IndexBuildCompletePayload = bincode::deserialize(&bytes).unwrap();
        assert!(!decoded.success);
        assert_eq!(decoded.error.as_deref(), Some("SSTable not found in S3"));
    }

    #[test]
    fn encode_decode_build_request_roundtrip() {
        let payload = IndexBuildRequestPayload {
            sstable_s3_paths: vec!["s3://b/k".into()],
            keyspace: "ks".into(),
            table: "tbl".into(),
            index_name: "idx".into(),
            index_metadata_json: "{}".into(),
            table_schema_json: "{}".into(),
        };
        let bytes = encode_build_request(&payload);
        let decoded = decode_build_request(&bytes).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn encode_decode_build_complete_roundtrip() {
        let payload = IndexBuildCompletePayload {
            keyspace: "ks".into(),
            table: "tbl".into(),
            index_name: "idx".into(),
            sidecar_s3_paths: vec!["s3://b/k/idx.fxsi".into()],
            success: true,
            error: None,
        };
        let bytes = encode_build_complete(&payload);
        let decoded = decode_build_complete(&bytes).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn remote_node_backend_construction() {
        let backend = RemoteNodeBackend {
            target_node: uuid::Uuid::new_v4(),
        };
        assert_ne!(backend.target_node, uuid::Uuid::nil());
    }

    #[tokio::test]
    async fn remote_node_backend_build_returns_not_implemented() {
        let backend = RemoteNodeBackend {
            target_node: uuid::Uuid::new_v4(),
        };
        let result = backend
            .build_remote("ks", "tbl", "idx", &["s3://b/sst"])
            .await;
        assert!(result.is_err());
    }
}
