//! Pull-mode manifest watcher for `backend=off` deployments.
//!
//! Polls the engine's manifest endpoint, identifies SSTables that lack
//! sidecar index files in S3, and enqueues build jobs for them.

use std::sync::Arc;
use std::time::Duration;

use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

use crate::worker::{BuildRequest, WorkerPool};

/// Manifest entry returned by the engine's `GET /internal/manifest` endpoint.
#[derive(Debug, serde::Deserialize)]
pub struct ManifestEntry {
    pub sstable_id: String,
    pub table_id: String,
    pub keyspace: String,
    pub table: String,
    /// Secondary indexes declared on this table: (index_name, column_position).
    #[serde(default)]
    pub indexes: Vec<(String, usize)>,
}

/// Run the pull-mode loop. Polls the manifest endpoint at `poll_interval`,
/// checks S3 for missing sidecar files, and builds them.
pub async fn run(
    pool: Arc<WorkerPool>,
    manifest_endpoint: String,
    poll_interval: Duration,
    object_store: Arc<dyn ObjectStore>,
    s3_prefix: String,
) {
    tracing::info!(
        manifest_endpoint = %manifest_endpoint,
        poll_interval = ?poll_interval,
        "starting pull-mode manifest watcher"
    );

    loop {
        match fetch_manifest(&manifest_endpoint).await {
            Ok(entries) => {
                let mut builds_started = 0;
                for entry in &entries {
                    for (index_name, col_pos) in &entry.indexes {
                        let sidecar_filename = format!("{}-{index_name}.sidecar", entry.sstable_id);

                        let hex = ferrosa_storage::upload::hex_prefix_for(&entry.sstable_id);
                        let sidecar_s3_key = if s3_prefix.is_empty() {
                            format!("{hex}/{}/{sidecar_filename}", entry.table_id)
                        } else {
                            format!("{s3_prefix}/{hex}/{}/{sidecar_filename}", entry.table_id)
                        };

                        let sidecar_path = ObjectPath::from(sidecar_s3_key.clone());

                        // Check if sidecar already exists in S3.
                        match object_store.head(&sidecar_path).await {
                            Ok(_) => continue, // already built
                            Err(object_store::Error::NotFound { .. }) => {
                                // Needs building.
                            }
                            Err(e) => {
                                tracing::warn!(
                                    sstable_id = %entry.sstable_id,
                                    error = %e,
                                    "failed to check sidecar existence"
                                );
                                continue;
                            }
                        }

                        let sstable_prefix = if s3_prefix.is_empty() {
                            format!("{hex}/{}/{}", entry.table_id, entry.sstable_id)
                        } else {
                            format!("{s3_prefix}/{hex}/{}/{}", entry.table_id, entry.sstable_id)
                        };

                        let req = BuildRequest {
                            sstable_id: entry.sstable_id.clone(),
                            index_name: index_name.clone(),
                            index_type: "btree".into(), // pull mode uses default
                            artifact_kind: None,
                            direct_upload: false,
                            s3_endpoint: String::new(), // uses pre-configured store
                            s3_bucket: String::new(),
                            s3_prefix: sstable_prefix,
                            table: (entry.keyspace.clone(), entry.table.clone()),
                            column_position: *col_pos,
                            priority: "normal".into(),
                        };

                        let pool = Arc::clone(&pool);
                        tokio::spawn(async move {
                            pool.execute(req).await;
                        });
                        builds_started += 1;
                    }
                }
                if builds_started > 0 {
                    tracing::info!(
                        builds_started,
                        total_sstables = entries.len(),
                        "enqueued index builds from manifest"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to fetch manifest");
            }
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Fetch the manifest from the engine's internal HTTP endpoint.
async fn fetch_manifest(endpoint: &str) -> Result<Vec<ManifestEntry>, String> {
    let resp = reqwest::get(endpoint)
        .await
        .map_err(|e| format!("HTTP GET {endpoint}: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("HTTP {} from {endpoint}", resp.status()));
    }

    let entries: Vec<ManifestEntry> = resp
        .json()
        .await
        .map_err(|e| format!("parse manifest: {e}"))?;

    Ok(entries)
}
