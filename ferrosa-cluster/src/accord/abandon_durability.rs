//! Red-first test: a reported `TxnAbandoned` must leave **no committed row
//! readable**, and that must still hold across a node restart.
//!
//! # The invariant
//!
//! [`AccordDriverError::TxnAbandoned`] is the client contract for "this
//! transaction was NOT committed; it is safe to retry" (rendered with the
//! `abandoned:` prefix, which the PostgreSQL front end maps to a retryable
//! 40001 and the CQL router to a "rolled back and NOT applied" server error).
//! A client that is told this **must not** be able to observe any row the
//! transaction wrote — otherwise the transaction is not atomic and a retry can
//! double-apply.
//!
//! # Why this is the right probe
//!
//! The coordinator applies its OWN shard in `apply_phase` before the Apply
//! quorum is known. With an RF=3 single-shard write-set, that shard is a third
//! of the rows. This test drives the real driver end to end with the
//! coordinator as a replica that owns the key, a REAL storage engine behind the
//! local state machine, and remote replicas that refuse every `Apply` (so the
//! Apply quorum is never reached and the driver abandons). It asserts the
//! invariant on the VALUES — a point lookup of the specific key — never on a
//! count, because a count is exactly the property a partial write preserves.
//!
//! The assertion is deliberately a point lookup + a restart, not a `COUNT(*)`:
//! a count cannot distinguish "the coordinator wrote its third" from "the whole
//! table loaded", and the count instrument itself has a documented history of
//! returning the wrong number.
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use uuid::Uuid;

    use ferrosa_common::accord::{HybridLogicalClock, TxnId};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_net::codec::Lane;
    use ferrosa_net::message::Message;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;
    use ferrosa_storage::{Mutation, StorageEngine, StorageEngineConfig, TableId};

    use crate::accord::apply::{EngineStorageApplier, StorageApplier};
    use crate::accord::coordinator::{AccordCoordinatorDriver, AccordDriverError};
    use crate::accord::handlers::AccordState;
    use crate::accord::state_machine::AccordStateMachine;
    use crate::accord::transport::AccordTransport;
    use crate::accord::wire::{
        CommitOkPayload, CommitPayload, PreAcceptOkPayload, PreAcceptPayload, ReadPredicate,
    };

    const KS: &str = "aband_ks";
    const TABLE: &str = "aband_table";

    fn node_id_of(u: Uuid) -> u64 {
        u64::from_be_bytes(u.as_bytes()[..8].try_into().expect("uuid is 16 bytes"))
    }

    fn test_schema() -> TableSchema {
        TableSchema {
            keyspace: KS.to_string(),
            table: TABLE.to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    fn open_engine(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig::test_config(dir);
        let engine = StorageEngine::new(config, None).expect("open storage engine");
        engine
            .register_table(test_schema())
            .expect("register table");
        Arc::new(engine)
    }

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_row(value: &[u8], cell_ts: i64) -> Row {
        Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(value.to_vec(), cell_ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(cell_ts),
        }
    }

    /// Serialize a single-row commit-log `Mutation` — the exact wire format the
    /// production [`EngineStorageApplier`] decodes and persists.
    fn encoded_mutation(key: &DecoratedKey, value: &[u8], cell_ts: i64) -> Vec<u8> {
        let m = Mutation::new(
            KS.to_string(),
            TABLE.to_string(),
            key.clone(),
            vec![make_row(value, cell_ts)],
            cell_ts,
        );
        let mut buf = vec![0u8; m.serialized_size()];
        m.serialize_into(&mut buf);
        buf
    }

    /// Read the value of column 0 of the first row of `key`, or `None` when the
    /// row is absent. A point lookup, not a count.
    fn read_row(engine: &StorageEngine, key: &DecoratedKey) -> Option<Vec<u8>> {
        let partition = engine
            .read(&TableId::new(KS, TABLE), key)
            .expect("engine read")?;
        let row = partition.rows.first()?;
        row.cells.first().and_then(|(_, c)| c.value.clone())
    }

    /// Two remote replicas that agree on PreAccept and Commit and answer `Apply`
    /// per `ack_apply`. With `ack_apply: false` they answer an ApplyOK for a
    /// DIFFERENT transaction, which the driver's quorum predicate rejects — the
    /// Apply quorum (self + 1 of 2) is never reached and the driver must abandon.
    /// With `ack_apply: true` they echo the awaited `txn_id`, so the quorum is met
    /// and the transaction commits and applies (the restart negative control).
    struct ReplicaTransport {
        ack_apply: bool,
    }

    #[async_trait]
    impl AccordTransport for ReplicaTransport {
        async fn send(
            &self,
            host_id: Uuid,
            msg: Message,
            _lane: Lane,
        ) -> ferrosa_net::error::Result<Message> {
            match msg {
                Message::AccordPreAccept(bytes) => {
                    let request: PreAcceptPayload = bincode::deserialize(&bytes).unwrap();
                    let response = PreAcceptOkPayload {
                        from: node_id_of(host_id),
                        t: request.t0,
                        deps: Vec::new(),
                        snapshot_stale: false,
                    };
                    Ok(Message::AccordPreAcceptOK(bytes::Bytes::from(
                        bincode::serialize(&response).unwrap(),
                    )))
                }
                Message::AccordCommit(bytes) => {
                    let request: CommitPayload = bincode::deserialize(&bytes).unwrap();
                    let ack = CommitOkPayload {
                        txn_id: request.txn_id,
                        from: node_id_of(host_id),
                    };
                    Ok(Message::AccordCommit(bytes::Bytes::from(
                        bincode::serialize(&ack).unwrap(),
                    )))
                }
                apply @ (Message::AccordApply(_) | Message::AccordApplyV2(_)) => {
                    use crate::accord::wire::{ApplyOkPayload, ApplyPayload};
                    let request: ApplyPayload = match &apply {
                        Message::AccordApply(b) => bincode::deserialize(b).unwrap(),
                        _ => unreachable!("v2 not used for a single-key write-set"),
                    };
                    // Refuse (default): an ApplyOK for a DIFFERENT txn proves nothing
                    // about whether THIS transaction applied, so it never counts.
                    let txn_id = if self.ack_apply {
                        request.txn_id
                    } else {
                        TxnId(ferrosa_common::accord::Timestamp {
                            node: request.txn_id.0.node.wrapping_add(1),
                            ..request.txn_id.0
                        })
                    };
                    let ack = ApplyOkPayload {
                        txn_id,
                        from: node_id_of(host_id),
                    };
                    Ok(Message::AccordApplyOK(bytes::Bytes::from(
                        bincode::serialize(&ack).unwrap(),
                    )))
                }
                other => panic!("unexpected Accord message in abandon test: {other:?}"),
            }
        }
    }

    /// THE INVARIANT. A transaction the driver reports as abandoned
    /// (`TxnAbandoned` — "NOT committed; safe to retry") must leave no committed
    /// row readable, and none after a restart either.
    #[tokio::test]
    async fn an_abandoned_transaction_leaves_no_row_readable_across_restart() {
        let self_host = Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let remote1 = Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let remote2 = Uuid::from_u128((0x2222_u128 << 64) | 0x2222);
        let self_node = node_id_of(self_host);

        let dir = tempfile::tempdir().unwrap();
        let engine = open_engine(dir.path());
        let applier = Arc::new(EngineStorageApplier::new(engine.clone()));
        let sm = AccordStateMachine::with_applier(
            self_node,
            Arc::new(MockSyncWriter::new()),
            applier.clone() as Arc<dyn StorageApplier>,
        );
        let local_state: AccordState = Arc::new(parking_lot::Mutex::new(sm));

        let key = make_key("row-1");
        let mutation = encoded_mutation(&key, b"payload", 1_000);

        let clock = HybridLogicalClock::new(self_node, 0);
        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            self_node,
            vec![self_host, remote1, remote2],
            Arc::new(ReplicaTransport { ack_apply: false }),
            false,
            &clock,
            vec![(b"row-1".to_vec(), mutation)],
        )
        .with_local_accord_state(local_state.clone())
        .with_read_predicate(ReadPredicate::Always);

        let result = driver.run_transaction().await;

        assert!(
            matches!(result, Err(AccordDriverError::TxnAbandoned { .. })),
            "the coordinator must report the transaction as abandoned (the client \
             contract is 'NOT committed; safe to retry'); got {result:?}"
        );

        // THE INVARIANT: a reported abandon leaves no committed row readable.
        assert_eq!(
            read_row(&engine, &key),
            None,
            "a transaction the driver reported as ABANDONED (not committed; safe to \
             retry) must leave no committed row readable — but the coordinator's own \
             shard was durably written before the abandon decision"
        );

        // ...and it must still hold across a restart. `shutdown()` FLUSHES every table;
        // dropping the engine does NOT (StorageEngine has no Drop impl), so without this
        // the reopen would find nothing regardless of what was written and the assertion
        // below would be vacuous. Every other restart test in this workspace calls it.
        drop(driver);
        drop(local_state);
        drop(applier);
        engine.shutdown().expect("shutdown storage engine");
        drop(engine);

        let reopened = open_engine(dir.path());
        assert_eq!(
            read_row(&reopened, &key),
            None,
            "the abandoned row must not survive a restart: the commit log replay \
             resurrected a write the client was told did not happen"
        );
    }

    /// Negative control for the restart assertion above: a transaction the driver
    /// reports as COMMITTED (the remotes ack its Apply) DOES write the row, and the
    /// row DOES survive the same restart. Without this, the "absent after restart"
    /// assertion could pass vacuously if the reopen/replay path were broken.
    #[tokio::test]
    async fn a_committed_row_survives_the_same_restart() {
        let self_host = Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let remote1 = Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let remote2 = Uuid::from_u128((0x2222_u128 << 64) | 0x2222);
        let self_node = node_id_of(self_host);

        let dir = tempfile::tempdir().unwrap();
        let engine = open_engine(dir.path());
        let applier = Arc::new(EngineStorageApplier::new(engine.clone()));
        let sm = AccordStateMachine::with_applier(
            self_node,
            Arc::new(MockSyncWriter::new()),
            applier.clone() as Arc<dyn StorageApplier>,
        );
        let local_state: AccordState = Arc::new(parking_lot::Mutex::new(sm));

        let key = make_key("row-1");
        let mutation = encoded_mutation(&key, b"payload", 1_000);

        let clock = HybridLogicalClock::new(self_node, 0);
        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            self_node,
            vec![self_host, remote1, remote2],
            Arc::new(ReplicaTransport { ack_apply: true }),
            false,
            &clock,
            vec![(b"row-1".to_vec(), mutation)],
        )
        .with_local_accord_state(local_state.clone())
        .with_read_predicate(ReadPredicate::Always);

        let result = driver.run_transaction().await;
        assert!(
            result.is_ok(),
            "a quorum-acked Apply must commit: {result:?}"
        );
        assert_eq!(
            read_row(&engine, &key).as_deref(),
            Some(b"payload".as_slice()),
            "a COMMITTED transaction's row must be readable"
        );

        drop(driver);
        drop(local_state);
        drop(applier);
        engine.shutdown().expect("shutdown storage engine");
        drop(engine);

        let reopened = open_engine(dir.path());
        assert_eq!(
            read_row(&reopened, &key).as_deref(),
            Some(b"payload".as_slice()),
            "a COMMITTED row must survive the restart — the restart assertion in the \
             abandon test is not vacuous"
        );
    }
}
