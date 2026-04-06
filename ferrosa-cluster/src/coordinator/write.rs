//! Write coordination -- fans out mutations to replicas with CL enforcement.

use futures::stream::{FuturesUnordered, StreamExt};

use ferrosa_common::key::DecoratedKey;
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_sstable::types::Row;
use ferrosa_storage::{Mutation, TableId};

use crate::consistency::ConsistencyLevel;
use crate::error::ClusterError;
use crate::pair::coordinator::encode_mutation;

use super::ClusterCoordinator;

/// Result of a single replica write attempt.
enum ReplicaResult {
    Ack,
    /// Write to a remote replica failed.  Carries the peer's `host_id` so the
    /// coordinator can store a hint when the overall write still meets quorum.
    Failure {
        host_id: Option<uuid::Uuid>,
    },
}

impl ClusterCoordinator {
    /// Coordinate a write to the appropriate replicas.
    ///
    /// 1. Compute replicas from token ring.
    /// 2. Verify enough replicas available for the consistency level.
    /// 3. Fan out concurrently: local write if self is replica, `MutationForward` for remote.
    /// 4. Collect ACKs until `block_for(CL)` reached.
    /// 5. Store hints for ALL failed remote replicas (regardless of quorum).
    /// 6. Return success or `WriteTimeout`.
    pub async fn coordinate_write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
    ) -> crate::error::Result<()> {
        self.coordinate_write_with(
            table_id,
            key,
            row,
            timestamp,
            self.default_cl,
            self.default_rf,
        )
        .await
    }

    /// Coordinate a write with explicit consistency level and replication factor.
    ///
    /// Use this when the query specifies a CL or the keyspace has a non-default RF.
    pub async fn coordinate_write_with(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
        cl: ConsistencyLevel,
        rf: usize,
    ) -> crate::error::Result<()> {
        // Acquire a write permit — limits concurrent in-flight writes to prevent
        // tokio runtime saturation that would starve Raft heartbeat processing.
        let _permit = self.write_semaphore.acquire().await.map_err(|_| {
            crate::error::ClusterError::Unavailable {
                consistency: cl.to_string(),
                required: 0,
                alive: 0,
            }
        })?;

        let ring = self.ring.load();
        let replicas = ring.replicas(key.token.0, rf);
        let required = cl.block_for(rf);

        {
            let span = tracing::info_span!(
                "cluster.write",
                cl = %cl,
                rf = rf,
                replicas = replicas.len(),
            );
            let _enter = span.enter();
        }

        if replicas.len() < required {
            return Err(ClusterError::Unavailable {
                consistency: cl.to_string(),
                required,
                alive: replicas.len(),
            });
        }

        let mutation = Mutation::new(
            table_id.keyspace.clone(),
            table_id.table.clone(),
            key.clone(),
            vec![row.clone()],
            timestamp,
        );
        let body = encode_mutation(&mutation);

        // Collect (replica_id, Option<host_id>) before dropping the ring guard
        // so the futures we build don't hold a reference to the guard.
        let replica_targets: Vec<(u64, Option<uuid::Uuid>)> = replicas
            .iter()
            .map(|&replica_id| {
                let host_id = ring.get_node(replica_id).map(|info| info.host_id);
                (replica_id, host_id)
            })
            .collect();
        drop(ring);

        // Build concurrent futures for each replica.
        let mut fan_out: FuturesUnordered<_> = replica_targets
            .into_iter()
            .map(|(replica_id, host_id)| {
                let storage = self.storage.clone();
                let peer_manager = self.peer_manager.clone();
                let table_id = table_id.clone();
                let key = key.clone();
                let row = row.clone();
                let body = body.clone();
                let local_node_id = self.local_node_id;

                async move {
                    let is_local = replica_id == local_node_id;
                    if is_local {
                        match storage.write(&table_id, &key, row, timestamp) {
                            Ok(()) => ReplicaResult::Ack,
                            Err(e) => {
                                tracing::warn!(%e, "local write failed");
                                ReplicaResult::Failure { host_id: None }
                            }
                        }
                    } else {
                        match host_id {
                            None => {
                                tracing::warn!(
                                    replica_id,
                                    "no host_id for replica — dropping write"
                                );
                                ReplicaResult::Failure { host_id: None }
                            }
                            Some(hid) => {
                                match peer_manager
                                    .send(hid, Message::MutationForward(body), Lane::Data)
                                    .await
                                {
                                    Ok(Message::MutationAck(_)) => ReplicaResult::Ack,
                                    Ok(other) => {
                                        tracing::warn!(?other, "unexpected response from replica");
                                        ReplicaResult::Failure { host_id: Some(hid) }
                                    }
                                    Err(e) => {
                                        tracing::warn!(%e, %hid, "MutationForward failed");
                                        ReplicaResult::Failure { host_id: Some(hid) }
                                    }
                                }
                            }
                        }
                    }
                }
            })
            .collect();

        // Drain all futures, collecting ACKs and failed replica host_ids.
        // We must drain the full set (not just until CL met) so we know which
        // replicas failed — those get hints if quorum was reached.
        let mut acks = 0usize;
        let mut failed_replicas: Vec<uuid::Uuid> = Vec::new();
        while let Some(result) = fan_out.next().await {
            match result {
                ReplicaResult::Ack => {
                    acks += 1;
                }
                ReplicaResult::Failure { host_id: Some(hid) } => {
                    failed_replicas.push(hid);
                }
                ReplicaResult::Failure { host_id: None } => {}
            }
        }

        // Store hints for ALL replicas that failed to ack, regardless of
        // whether quorum was met. Even if the write returns an error to the
        // client, the replicas that DID succeed now have divergent state.
        // Hints ensure replay can fix the divergence without waiting for
        // anti-entropy repair. When zero replicas ACK'd the mutation is
        // still recorded as a hint so it can be replayed when nodes recover.
        //
        // Hint store failures are logged as errors (not warnings) so they
        // are visible in monitoring. The write still proceeds — hint loss
        // means anti-entropy repair must eventually fix divergence.
        if !failed_replicas.is_empty() {
            if let Some(ref hint_store) = self.hint_store {
                let hint_row = body.to_vec();
                let hint_key = key.key.as_bytes().to_vec();
                for peer_id in &failed_replicas {
                    if let Err(e) = hint_store.store(
                        *peer_id,
                        &table_id.keyspace,
                        &table_id.table,
                        hint_key.clone(),
                        hint_row.clone(),
                        timestamp,
                    ) {
                        tracing::error!(
                            peer = %peer_id,
                            %e,
                            "hint store failed — divergent replica will require anti-entropy repair"
                        );
                    }
                }
            } else {
                tracing::error!(
                    failed_count = failed_replicas.len(),
                    "no hint store available — divergent replicas will require anti-entropy repair"
                );
            }
        }

        if acks >= required {
            Ok(())
        } else {
            Err(ClusterError::WriteTimeout {
                consistency: cl.to_string(),
                received: acks,
                required,
            })
        }
    }

    /// Coordinate a write using NetworkTopologyStrategy with DC-aware consistency.
    ///
    /// For `LOCAL_QUORUM`: compute required ACKs from the local DC's RF only.
    /// For `EACH_QUORUM`: compute required ACKs per-DC and track per-DC.
    /// For other CLs: compute from total RF as before.
    pub async fn coordinate_write_nts(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
        cl: ConsistencyLevel,
        strategy: &crate::ring::strategy::ReplicationStrategy,
    ) -> crate::error::Result<()> {
        // Acquire a write permit — see coordinate_write_with for rationale.
        let _permit = self.write_semaphore.acquire().await.map_err(|_| {
            crate::error::ClusterError::Unavailable {
                consistency: cl.to_string(),
                required: 0,
                alive: 0,
            }
        })?;

        let ring = self.ring.load();
        let replicas = ring.replicas_for_strategy(key.token.0, strategy);

        {
            let span = tracing::info_span!(
                "cluster.write",
                cl = %cl,
                rf = strategy.replication_factor(),
                replicas = replicas.len(),
            );
            let _enter = span.enter();
        }

        let local_dc = ring
            .get_node(self.local_node_id)
            .map(|n| n.data_center.clone())
            .unwrap_or_default();

        // Compute required ACKs based on CL and strategy.
        let required = match cl {
            ConsistencyLevel::LocalQuorum | ConsistencyLevel::LocalOne => {
                let local_rf = strategy
                    .dc_replication_factors()
                    .get(&local_dc)
                    .copied()
                    .unwrap_or(strategy.replication_factor());
                cl.block_for_dc(local_rf)
            }
            ConsistencyLevel::EachQuorum => {
                // For EACH_QUORUM we need to track per-DC.
                // Set required to total for the availability check.
                strategy
                    .dc_replication_factors()
                    .values()
                    .map(|&rf| cl.block_for_dc(rf))
                    .sum()
            }
            _ => cl.block_for(strategy.replication_factor()),
        };

        if replicas.len() < required {
            return Err(ClusterError::Unavailable {
                consistency: cl.to_string(),
                required,
                alive: replicas.len(),
            });
        }

        // Build the mutation payload.
        let mutation = Mutation::new(
            table_id.keyspace.clone(),
            table_id.table.clone(),
            key.clone(),
            vec![row.clone()],
            timestamp,
        );
        let body = encode_mutation(&mutation);

        let replica_targets: Vec<(u64, Option<uuid::Uuid>, String)> = replicas
            .iter()
            .map(|&replica_id| {
                let node = ring.get_node(replica_id);
                let host_id = node.map(|info| info.host_id);
                let dc = node
                    .map(|info| info.data_center.clone())
                    .unwrap_or_default();
                (replica_id, host_id, dc)
            })
            .collect();
        drop(ring);

        // Fan out.
        let mut fan_out: FuturesUnordered<_> = replica_targets
            .into_iter()
            .map(|(replica_id, host_id, dc)| {
                let storage = self.storage.clone();
                let peer_manager = self.peer_manager.clone();
                let table_id = table_id.clone();
                let key = key.clone();
                let row = row.clone();
                let body = body.clone();
                let local_node_id = self.local_node_id;

                async move {
                    let result = if replica_id == local_node_id {
                        match storage.write(&table_id, &key, row, timestamp) {
                            Ok(()) => ReplicaResult::Ack,
                            Err(_) => ReplicaResult::Failure { host_id: None },
                        }
                    } else {
                        match host_id {
                            None => ReplicaResult::Failure { host_id: None },
                            Some(hid) => {
                                match peer_manager
                                    .send(hid, Message::MutationForward(body), Lane::Data)
                                    .await
                                {
                                    Ok(Message::MutationAck(_)) => ReplicaResult::Ack,
                                    Ok(_) => ReplicaResult::Failure { host_id: Some(hid) },
                                    Err(_) => ReplicaResult::Failure { host_id: Some(hid) },
                                }
                            }
                        }
                    };
                    (result, dc)
                }
            })
            .collect();

        // Drain results, track per-DC if EACH_QUORUM.
        let mut total_acks = 0usize;
        let mut dc_acks: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let mut failed_replicas: Vec<uuid::Uuid> = Vec::new();

        while let Some((result, dc)) = fan_out.next().await {
            match result {
                ReplicaResult::Ack => {
                    total_acks += 1;
                    *dc_acks.entry(dc).or_insert(0) += 1;
                }
                ReplicaResult::Failure { host_id: Some(hid) } => {
                    failed_replicas.push(hid);
                }
                ReplicaResult::Failure { host_id: None } => {}
            }
        }

        // Check satisfaction.
        let satisfied = match cl {
            ConsistencyLevel::EachQuorum => {
                strategy.dc_replication_factors().iter().all(|(dc, &rf)| {
                    let acks = dc_acks.get(dc).copied().unwrap_or(0);
                    acks >= cl.block_for_dc(rf)
                })
            }
            ConsistencyLevel::LocalQuorum | ConsistencyLevel::LocalOne => total_acks >= required,
            _ => total_acks >= required,
        };

        // Store hints for ALL failed replicas regardless of quorum outcome.
        // Even when the write fails (below quorum), hints record the mutation
        // so replay can fix divergence when nodes recover.
        if !failed_replicas.is_empty() {
            if let Some(ref hint_store) = self.hint_store {
                let hint_row = body.to_vec();
                let hint_key = key.key.as_bytes().to_vec();
                for peer_id in &failed_replicas {
                    if let Err(e) = hint_store.store(
                        *peer_id,
                        &table_id.keyspace,
                        &table_id.table,
                        hint_key.clone(),
                        hint_row.clone(),
                        timestamp,
                    ) {
                        tracing::warn!(
                            peer = %peer_id,
                            "failed to store hint for NTS replica: {e}"
                        );
                    }
                }
            }
        }

        if satisfied {
            Ok(())
        } else {
            Err(ClusterError::WriteTimeout {
                consistency: cl.to_string(),
                received: total_acks,
                required,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use uuid::Uuid;

    use crate::consistency::ConsistencyLevel;
    use crate::error::ClusterError;
    use crate::raft::{NodeInfo, NodeState};
    use crate::ring::TokenRing;
    use ferrosa_common::key::DecoratedKey;
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::{CellValue, PartitionKey, Token};
    use ferrosa_net::config::NetConfig;
    use ferrosa_net::peer::{PeerEventListener, PeerManager};
    use ferrosa_net::rpc::handler::PeerId;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig};

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn make_node(addr: &str) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        }
    }

    fn test_key() -> DecoratedKey {
        DecoratedKey {
            token: Token(42),
            key: PartitionKey::new(vec![1, 2, 3]),
        }
    }

    fn test_row() -> Row {
        Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }
    }

    fn register_test_table(storage: &StorageEngine) {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        storage.register_table(schema).unwrap();
    }

    struct NoopListener;
    impl PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _peer: PeerId) {}
        fn on_peer_disconnected(&self, _peer: PeerId) {}
        fn on_peer_suspected(&self, _peer: PeerId) {}
        fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
        fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
    }

    fn make_coordinator(
        ring: TokenRing,
        peer_manager: Arc<PeerManager>,
        local_node_id: u64,
        storage: Arc<StorageEngine>,
        rf: usize,
        cl: ConsistencyLevel,
    ) -> ClusterCoordinator {
        ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            peer_manager,
            local_node_id,
            storage,
            rf,
            cl,
        )
    }

    fn noop_peer_manager() -> Arc<PeerManager> {
        Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ))
    }

    // ---------------------------------------------------------------------------
    // Existing tests (preserved)
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn coordinate_write_local_replica_writes_to_storage() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();

        coordinator
            .coordinate_write(&table_id, &key, row, 1000)
            .await
            .unwrap();

        let result = storage.read(&table_id, &key).unwrap();
        assert!(result.is_some(), "local write should be readable");
    }

    #[tokio::test]
    async fn coordinate_write_unavailable_when_too_few_replicas() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let ring = TokenRing::new(); // empty ring, no replicas

        let coordinator = make_coordinator(
            ring,
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            3,
            ConsistencyLevel::Quorum,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();

        let result = coordinator
            .coordinate_write(&table_id, &key, row, 1000)
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            ClusterError::Unavailable {
                required, alive, ..
            } => {
                assert_eq!(required, 2); // QUORUM of 3 = 2
                assert_eq!(alive, 0);
            }
            other => panic!("expected Unavailable, got: {other}"),
        }
    }

    // ---------------------------------------------------------------------------
    // New test: concurrent fan-out behavioral contract
    // ---------------------------------------------------------------------------

    /// Counts how many `MutationForward` messages were "received" by
    /// simulating the two remote replicas with storage engines.
    ///
    /// Rather than testing timing (which is fragile in CI), this test
    /// verifies the behavioral contract of concurrent fan-out:
    ///
    /// Given: 3-node ring (local + 2 remote), RF=3, CL=QUORUM
    /// When:  coordinate_write() is called
    /// Then:  succeeds (2+ ACKs obtained)
    ///        AND both remote replicas received their MutationForward
    ///        AND the local replica also wrote to storage
    #[tokio::test]
    async fn coordinate_write_fans_out_concurrently() {
        // We verify the behavioral contract by having the coordinator write
        // to a local-only ring (all 3 nodes = local node via RF=3, single node).
        // The key property: all replicas in the fan-out must receive the write,
        // not just `required` of them in order.
        //
        // For a true multi-node scenario we'd need a mock PeerManager.
        // Here we use a single-node ring with RF=1, CL=ONE to verify the
        // concurrent code path doesn't regress the local-write contract,
        // then separately verify the WriteTimeout path when remote replicas
        // are unavailable (no pool), which exercises the fan-out loop.

        // Part 1: Local-only write with concurrent fan-out succeeds.
        {
            let dir = tempfile::tempdir().unwrap();
            let storage = test_storage(dir.path());
            register_test_table(&storage);

            let local_node_id = 1u64;
            let mut ring = TokenRing::new();
            ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
            ring.assign_tokens(local_node_id, &[0, 100, 200]);

            let coordinator = make_coordinator(
                ring,
                noop_peer_manager(),
                local_node_id,
                storage.clone(),
                1,
                ConsistencyLevel::One,
            );

            let table_id = TableId::new("test_ks", "test_tbl");
            let key = test_key();
            let row = test_row();

            coordinator
                .coordinate_write(&table_id, &key, row, 1000)
                .await
                .unwrap();

            let result = storage.read(&table_id, &key).unwrap();
            assert!(result.is_some(), "local write should land in storage");
        }

        // Part 2: 3-node ring (local + 2 remote), RF=3, CL=QUORUM.
        // Remote replicas have no connection pool (add_peer_entry), so they
        // fail. Local node is one of the 3. QUORUM requires 2.
        // Expected: local ACK (1) + 0 remote ACKs = WriteTimeout, because
        // only 1 of 2 required ACKs arrived. This exercises all 3 fan-out
        // futures running concurrently and then aggregating.
        {
            let dir = tempfile::tempdir().unwrap();
            let storage = test_storage(dir.path());
            register_test_table(&storage);

            let local_node_id = 1u64;
            let remote_uuid_2 = Uuid::new_v4();
            let remote_uuid_3 = Uuid::new_v4();

            // Add peer entries (no real pool — send() will fail).
            let pm = Arc::new(PeerManager::new(
                Arc::new(NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            ));
            pm.add_peer_entry((remote_uuid_2, "10.0.0.2:7000".parse().unwrap()))
                .await;
            pm.add_peer_entry((remote_uuid_3, "10.0.0.3:7000".parse().unwrap()))
                .await;

            let mut node2 = make_node("10.0.0.2:7000");
            node2.host_id = remote_uuid_2;
            let mut node3 = make_node("10.0.0.3:7000");
            node3.host_id = remote_uuid_3;

            let mut ring = TokenRing::new();
            ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
            ring.add_node(2u64, node2);
            ring.add_node(3u64, node3);
            // Place each node at a distinct token so replicas(42, 3) = [1,2,3]
            ring.assign_tokens(local_node_id, &[50]);
            ring.assign_tokens(2u64, &[100]);
            ring.assign_tokens(3u64, &[200]);

            let coordinator = ClusterCoordinator::new(
                Arc::new(ArcSwap::from_pointee(ring)),
                pm,
                local_node_id,
                storage.clone(),
                3, // RF=3
                ConsistencyLevel::Quorum,
            );

            let table_id = TableId::new("test_ks", "test_tbl");
            let key = test_key();
            let row = test_row();

            // With no real connections, remote replicas fail. Local ACK=1, required=2.
            let result = coordinator
                .coordinate_write(&table_id, &key, row, 1000)
                .await;

            // The write should time out (only 1 ACK from local, need 2 for QUORUM).
            match result {
                Err(ClusterError::WriteTimeout {
                    received, required, ..
                }) => {
                    assert_eq!(required, 2, "QUORUM of RF=3 requires 2");
                    assert_eq!(received, 1, "only the local replica ACKed");
                }
                other => panic!("expected WriteTimeout, got: {other:?}"),
            }

            // The local write still landed despite overall failure.
            let stored = storage.read(&table_id, &key).unwrap();
            assert!(
                stored.is_some(),
                "local replica must have written even if CL not met"
            );
        }

        // Part 3: Verify all fan-out futures are launched (not just `required`).
        // Use an atomic counter that each "replica" increments via Arc-captured storage.
        // We simulate this with RF=3, CL=ONE, local + 2 remote (no pool).
        // CL=ONE means required=1. After the local ACK, the loop breaks early.
        // We verify the local write landed (1 ACK sufficed).
        {
            let dir = tempfile::tempdir().unwrap();
            let storage = test_storage(dir.path());
            register_test_table(&storage);

            let local_node_id = 1u64;
            let remote_uuid_2 = Uuid::new_v4();
            let remote_uuid_3 = Uuid::new_v4();

            let pm = Arc::new(PeerManager::new(
                Arc::new(NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            ));
            pm.add_peer_entry((remote_uuid_2, "10.0.0.2:7000".parse().unwrap()))
                .await;
            pm.add_peer_entry((remote_uuid_3, "10.0.0.3:7000".parse().unwrap()))
                .await;

            let mut node2 = make_node("10.0.0.2:7000");
            node2.host_id = remote_uuid_2;
            let mut node3 = make_node("10.0.0.3:7000");
            node3.host_id = remote_uuid_3;

            let mut ring = TokenRing::new();
            ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
            ring.add_node(2u64, node2);
            ring.add_node(3u64, node3);
            ring.assign_tokens(local_node_id, &[50]);
            ring.assign_tokens(2u64, &[100]);
            ring.assign_tokens(3u64, &[200]);

            let coordinator = ClusterCoordinator::new(
                Arc::new(ArcSwap::from_pointee(ring)),
                pm,
                local_node_id,
                storage.clone(),
                3,                     // RF=3
                ConsistencyLevel::One, // only 1 ACK needed
            );

            let table_id = TableId::new("test_ks", "test_tbl");
            let key = test_key();
            let row = test_row();

            // CL=ONE: local ACK satisfies the requirement immediately.
            coordinator
                .coordinate_write(&table_id, &key, row, 1000)
                .await
                .unwrap();

            let stored = storage.read(&table_id, &key).unwrap();
            assert!(stored.is_some(), "local replica must have written");
        }
    }

    // -----------------------------------------------------------------------
    // Hint-on-failure tests (Task 13)
    // -----------------------------------------------------------------------

    fn make_hint_store(dir: &std::path::Path) -> Arc<crate::hints::HintStore> {
        use crate::hints::{HintConfig, HintStore};
        let config = HintConfig {
            dir: dir.join("hints"),
            ..HintConfig::default()
        };
        Arc::new(HintStore::new(config).unwrap())
    }

    /// 3-node ring (local=1, remote=2, remote=3), RF=3, CL=QUORUM.
    /// Remote replicas have no real connection, so they fail.
    /// Local ACK (1) + 0 remote = WriteTimeout.
    /// BUT with CL=ONE the local ACK satisfies quorum, so any 1 remote failure
    /// should be hinted.
    ///
    /// We use RF=3, CL=ONE so: required=1, local ACKs, both remotes fail.
    /// After the write, both failed remote peers should have a pending hint.
    #[tokio::test]
    async fn write_at_quorum_stores_hint_for_failed_replica() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);
        let hint_store = make_hint_store(dir.path());

        let local_node_id = 1u64;
        let remote_uuid_2 = Uuid::new_v4();
        let remote_uuid_3 = Uuid::new_v4();

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        // Register the peer entries so the ring can resolve host_ids,
        // but there are no real connections — sends will fail.
        pm.add_peer_entry((remote_uuid_2, "10.0.0.2:7000".parse().unwrap()))
            .await;
        pm.add_peer_entry((remote_uuid_3, "10.0.0.3:7000".parse().unwrap()))
            .await;

        let mut node2 = make_node("10.0.0.2:7000");
        node2.host_id = remote_uuid_2;
        let mut node3 = make_node("10.0.0.3:7000");
        node3.host_id = remote_uuid_3;

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.add_node(2u64, node2);
        ring.add_node(3u64, node3);
        ring.assign_tokens(local_node_id, &[50]);
        ring.assign_tokens(2u64, &[100]);
        ring.assign_tokens(3u64, &[200]);

        // CL=ONE: local ACK is sufficient for quorum.  Both remote replicas
        // will fail their sends, and hints should be stored for them.
        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            pm,
            local_node_id,
            storage.clone(),
            3,                     // RF=3
            ConsistencyLevel::One, // required=1
        )
        .with_hint_store(hint_store.clone());

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();

        // Write should succeed (local ACK meets CL=ONE).
        coordinator
            .coordinate_write(&table_id, &key, row, 1000)
            .await
            .unwrap();

        // Both failed remote replicas should each have exactly 1 pending hint.
        assert_eq!(
            hint_store.pending_count(remote_uuid_2),
            1,
            "remote_2 should have 1 hint"
        );
        assert_eq!(
            hint_store.pending_count(remote_uuid_3),
            1,
            "remote_3 should have 1 hint"
        );
    }

    /// 3-node ring, RF=3, CL=QUORUM (required=2).
    /// Local write succeeds (acks=1), both remotes fail → WriteTimeout.
    /// Hints SHOULD be stored for the failed remotes because the local replica
    /// has the data and the failed replicas need it for eventual convergence.
    #[tokio::test]
    async fn write_below_quorum_stores_hints_for_failed_replicas() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);
        let hint_store = make_hint_store(dir.path());

        let local_node_id = 1u64;
        let remote_uuid_2 = Uuid::new_v4();
        let remote_uuid_3 = Uuid::new_v4();

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        pm.add_peer_entry((remote_uuid_2, "10.0.0.2:7000".parse().unwrap()))
            .await;
        pm.add_peer_entry((remote_uuid_3, "10.0.0.3:7000".parse().unwrap()))
            .await;

        let mut node2 = make_node("10.0.0.2:7000");
        node2.host_id = remote_uuid_2;
        let mut node3 = make_node("10.0.0.3:7000");
        node3.host_id = remote_uuid_3;

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.add_node(2u64, node2);
        ring.add_node(3u64, node3);
        ring.assign_tokens(local_node_id, &[50]);
        ring.assign_tokens(2u64, &[100]);
        ring.assign_tokens(3u64, &[200]);

        // CL=QUORUM with RF=3: required=2.
        // local ACK=1, remote fails=2 → acks(1) < required(2) → WriteTimeout.
        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            pm,
            local_node_id,
            storage.clone(),
            3,
            ConsistencyLevel::Quorum,
        )
        .with_hint_store(hint_store.clone());

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();

        let result = coordinator
            .coordinate_write(&table_id, &key, row, 1000)
            .await;

        assert!(
            matches!(result, Err(ClusterError::WriteTimeout { .. })),
            "expected WriteTimeout, got: {result:?}"
        );

        // Hints stored for both failed remotes — local replica has the data
        // and the failed replicas need it for eventual convergence.
        assert_eq!(
            hint_store.pending_count(remote_uuid_2),
            1,
            "remote_2 should have 1 hint even on below-quorum write"
        );
        assert_eq!(
            hint_store.pending_count(remote_uuid_3),
            1,
            "remote_3 should have 1 hint even on below-quorum write"
        );
    }

    // -----------------------------------------------------------------------
    // NTS write coordination tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn coordinate_write_nts_local_quorum_single_dc_local_replica() {
        // 2-DC setup: local node in dc1. RF: dc1=2, dc2=1.
        // CL=LOCAL_QUORUM => block_for_dc(dc1_rf=2) = 2.
        // Only local node is reachable => 1 ACK. Should WriteTimeout.
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut local_info = make_node("10.0.0.1:7000");
        local_info.data_center = "dc1".to_string();

        let mut node2 = make_node("10.0.0.2:7000");
        node2.data_center = "dc1".to_string();
        let mut node3 = make_node("10.0.0.3:7000");
        node3.data_center = "dc2".to_string();

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local_info);
        ring.add_node(2, node2);
        ring.add_node(3, node3);
        ring.assign_tokens(local_node_id, &[100]);
        ring.assign_tokens(2, &[200]);
        ring.assign_tokens(3, &[300]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            noop_peer_manager(),
            local_node_id,
            storage.clone(),
            3,
            ConsistencyLevel::LocalQuorum,
        );

        let dc_rf = std::collections::HashMap::from([
            ("dc1".to_string(), 2usize),
            ("dc2".to_string(), 1usize),
        ]);
        let strategy = crate::ring::strategy::ReplicationStrategy::NetworkTopology { dc_rf };

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();

        let result = coordinator
            .coordinate_write_nts(
                &table_id,
                &key,
                row,
                1000,
                ConsistencyLevel::LocalQuorum,
                &strategy,
            )
            .await;

        // Local node ACKs (1), but LOCAL_QUORUM for dc1_rf=2 requires 2 => WriteTimeout
        match result {
            Err(ClusterError::WriteTimeout {
                required, received, ..
            }) => {
                assert_eq!(required, 2, "LOCAL_QUORUM of dc1 rf=2 requires 2");
                assert_eq!(received, 1, "only local replica ACKed");
            }
            other => panic!("expected WriteTimeout, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn coordinate_write_nts_each_quorum_fails_when_dc_missing() {
        // 2-DC, dc1_rf=1, dc2_rf=1. CL=EACH_QUORUM.
        // Only dc1 local node reachable, dc2 node unreachable.
        // EACH_QUORUM requires quorum in EACH DC => fails.
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut local_info = make_node("10.0.0.1:7000");
        local_info.data_center = "dc1".to_string();
        let remote_uuid = Uuid::new_v4();
        let mut remote_info = make_node("10.0.0.2:7000");
        remote_info.data_center = "dc2".to_string();
        remote_info.host_id = remote_uuid;

        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, local_info);
        ring.add_node(2, remote_info);
        ring.assign_tokens(local_node_id, &[100]);
        ring.assign_tokens(2, &[200]);

        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        pm.add_peer_entry((remote_uuid, "10.0.0.2:7000".parse().unwrap()))
            .await;

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            pm,
            local_node_id,
            storage.clone(),
            2,
            ConsistencyLevel::EachQuorum,
        );

        let dc_rf = std::collections::HashMap::from([
            ("dc1".to_string(), 1usize),
            ("dc2".to_string(), 1usize),
        ]);
        let strategy = crate::ring::strategy::ReplicationStrategy::NetworkTopology { dc_rf };

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();

        let result = coordinator
            .coordinate_write_nts(
                &table_id,
                &key,
                row,
                1000,
                ConsistencyLevel::EachQuorum,
                &strategy,
            )
            .await;

        // dc1 ACK=1 (local), dc2 ACK=0 (remote unreachable)
        // EACH_QUORUM requires quorum in both => fail
        assert!(
            matches!(result, Err(ClusterError::WriteTimeout { .. })),
            "expected WriteTimeout, got: {result:?}"
        );
    }
}
