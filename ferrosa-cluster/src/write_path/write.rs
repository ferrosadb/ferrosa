//! Single-row writes through [`WritePath`] for Direct, Pair and Cluster.

use super::*;

impl WritePath {
    /// Write a row. In standalone mode this goes directly to storage.
    /// In pair mode this goes through the PairCoordinator which handles
    /// replication (primary) or forwarding (secondary).
    /// In cluster mode the replication strategy determines whether to use
    /// SimpleStrategy or NetworkTopologyStrategy DC-aware coordination.
    pub async fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Arc<Row>,
        timestamp: i64,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> ferrosa_common::Result<()> {
        self.await_write_admission(table_id).await?;
        match self {
            Self::Direct(engine) => engine.write(table_id, key, row, timestamp),
            Self::Unavailable => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, writes rejected until operator promotes".into(),
            )),
            Self::DegradedPair(_) => Err(ferrosa_common::Error::InvalidData(
                "pair mode: primary unavailable, writes rejected until operator promotes".into(),
            )),
            Self::Pair(coordinator) => {
                let mutation = Mutation::new(
                    table_id.keyspace.clone(),
                    table_id.table.clone(),
                    key.clone(),
                    vec![row],
                    timestamp,
                );
                let res = coordinator
                    .coordinate_write_with(&mutation, cl, strategy.replication_factor())
                    .await
                    .map_err(cluster_error_to_common);
                if res.is_ok() {
                    if let Some((bus, ev)) = committed_cdc_event(
                        coordinator.local_storage(),
                        table_id,
                        &mutation.key,
                        &mutation.rows,
                        mutation.timestamp,
                    ) {
                        bus.publish(ev);
                    }
                }
                res
            }
            Self::Cluster(coordinator) => {
                // Capture the committed-CDC payload before `row` is moved into
                // the coordinate call; clones only if a subscriber is listening.
                let pending = committed_cdc_event(
                    &coordinator.storage,
                    table_id,
                    key,
                    std::slice::from_ref(&row),
                    timestamp,
                );
                let res = match strategy {
                    ReplicationStrategy::Simple { replication_factor } => coordinator
                        .coordinate_write_with(
                            table_id,
                            key,
                            row,
                            timestamp,
                            cl,
                            *replication_factor,
                        )
                        .await
                        .map_err(cluster_error_to_common),
                    ReplicationStrategy::NetworkTopology { .. } => coordinator
                        .coordinate_write_nts(table_id, key, row, timestamp, cl, strategy)
                        .await
                        .map_err(cluster_error_to_common),
                };
                if res.is_ok() {
                    if let Some((bus, ev)) = pending {
                        bus.publish(ev);
                    }
                }
                res
            }
        }
    }
}
