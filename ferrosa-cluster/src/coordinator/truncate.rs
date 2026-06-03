//! Truncate coordination — fans out TRUNCATE to all cluster nodes.

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::task_pool::TaskPool;
use ferrosa_storage::TableId;

use super::encode_truncate_payload;
use super::ClusterCoordinator;

impl ClusterCoordinator {
    /// Truncate a table on all cluster nodes.
    ///
    /// Unlike writes which target RF replicas, truncate must reach EVERY node
    /// because every node may hold data for the table.
    ///
    /// 1. Truncate locally.
    /// 2. Fan out `TruncateForward` to all remote nodes.
    /// 3. Wait for all ACKs (best-effort — log failures but don't block).
    pub async fn coordinate_truncate(&self, table_id: &TableId) -> crate::error::Result<()> {
        // 1. Truncate local storage first.
        self.storage
            .truncate(table_id)
            .map_err(crate::error::ClusterError::Storage)?;

        // 2. Fan out to all remote nodes in the ring.
        let ring = self.ring.load();
        let remote_host_ids: Vec<uuid::Uuid> = ring
            .node_ids()
            .iter()
            .filter(|&&node_id| node_id != self.local_node_id)
            .filter_map(|&node_id| ring.get_node(node_id).map(|n| n.host_id))
            .collect();
        drop(ring);

        if remote_host_ids.is_empty() {
            return Ok(());
        }

        let payload = encode_truncate_payload(table_id);

        // 3. Send TruncateForward to each remote node concurrently.
        let mut handles = Vec::with_capacity(remote_host_ids.len());
        for hid in &remote_host_ids {
            let pm = self.peer_manager.clone();
            let body = payload.clone();
            let host_id = *hid;
            handles.push(TaskPool::current("truncate-fanout").spawn(async move {
                match pm
                    .send(host_id, Message::TruncateForward(body), Lane::Data)
                    .await
                {
                    Ok(Message::TruncateAck(_)) => {
                        tracing::info!(%host_id, "truncate ACK received");
                    }
                    Ok(other) => {
                        tracing::warn!(?other, %host_id, "unexpected truncate response");
                    }
                    Err(e) => {
                        tracing::warn!(%e, %host_id, "TruncateForward failed");
                    }
                }
            }));
        }

        // Wait for all remotes (best-effort).
        for h in handles {
            let _ = h.await;
        }

        Ok(())
    }
}
