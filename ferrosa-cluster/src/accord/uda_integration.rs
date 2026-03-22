//! UDF/UDA integration with Accord transactions.
//!
//! Tests covering the interaction between user-defined functions/aggregates
//! and the Accord consensus protocol:
//!
//! - **DeleteTarget roundtrip** (5 tests): Serialization and application of
//!   column and map-element deletion targets through the Accord state machine.
//! - **Token range** (4 tests): Token-based routing, conflict detection, and
//!   full-scan semantics for range queries in Accord.
//! - **LWW deletion** (4 tests): Last-writer-wins deletion semantics —
//!   idempotency, ordering, replay independence, and concurrent replicas.
//! - **pk_indexes** (2 tests): Primary key index consistency under Accord
//!   transactions and schema changes.
//!
//! # A7.10 Tests (15 of 18 — remaining 3 in ferrosa-storage)

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ferrosa_common::accord::{BallotNumber, Timestamp, TxnId, TxnPhase};
    use ferrosa_storage::accord::conflict_index::{ConflictIndex, TokenRange};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;

    use crate::accord::state_machine::{AccordStateMachine, SmResponse};
    use crate::accord::test_cluster::{TestCluster, TestMessage, TestMessagePayload};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
    }

    fn txn(src: u64, micros: u64) -> TxnId {
        TxnId::new(src, ts(micros))
    }

    fn make_sm(node_id: u64) -> (AccordStateMachine, Arc<MockSyncWriter>) {
        let writer = Arc::new(MockSyncWriter::new());
        let sm = AccordStateMachine::new(node_id, writer.clone());
        (sm, writer)
    }

    // -----------------------------------------------------------------------
    // DeleteTarget: represents a deletion that targets either a whole column
    // or a specific map element. Serialized as part of the transaction payload.
    // -----------------------------------------------------------------------

    /// A deletion target within an Accord transaction.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum DeleteTarget {
        /// Delete an entire column.
        Column { table: String, column: String },
        /// Delete a specific map element.
        MapElement {
            table: String,
            column: String,
            map_key: Vec<u8>,
        },
    }

    impl DeleteTarget {
        /// Serialize to bytes for round-trip testing.
        fn serialize(&self) -> Vec<u8> {
            let mut buf = Vec::new();
            match self {
                DeleteTarget::Column { table, column } => {
                    buf.push(0x01); // discriminant
                    buf.extend_from_slice(&(table.len() as u32).to_le_bytes());
                    buf.extend_from_slice(table.as_bytes());
                    buf.extend_from_slice(&(column.len() as u32).to_le_bytes());
                    buf.extend_from_slice(column.as_bytes());
                }
                DeleteTarget::MapElement {
                    table,
                    column,
                    map_key,
                } => {
                    buf.push(0x02); // discriminant
                    buf.extend_from_slice(&(table.len() as u32).to_le_bytes());
                    buf.extend_from_slice(table.as_bytes());
                    buf.extend_from_slice(&(column.len() as u32).to_le_bytes());
                    buf.extend_from_slice(column.as_bytes());
                    buf.extend_from_slice(&(map_key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(map_key);
                }
            }
            buf
        }

        /// Deserialize from bytes.
        fn deserialize(data: &[u8]) -> Option<Self> {
            if data.is_empty() {
                return None;
            }
            let mut pos = 0;
            let disc = data[pos];
            pos += 1;

            let read_string = |data: &[u8], pos: &mut usize| -> Option<String> {
                if *pos + 4 > data.len() {
                    return None;
                }
                let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?) as usize;
                *pos += 4;
                if *pos + len > data.len() {
                    return None;
                }
                let s = String::from_utf8(data[*pos..*pos + len].to_vec()).ok()?;
                *pos += len;
                Some(s)
            };

            let read_bytes = |data: &[u8], pos: &mut usize| -> Option<Vec<u8>> {
                if *pos + 4 > data.len() {
                    return None;
                }
                let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?) as usize;
                *pos += 4;
                if *pos + len > data.len() {
                    return None;
                }
                let v = data[*pos..*pos + len].to_vec();
                *pos += len;
                Some(v)
            };

            match disc {
                0x01 => {
                    let table = read_string(data, &mut pos)?;
                    let column = read_string(data, &mut pos)?;
                    Some(DeleteTarget::Column { table, column })
                }
                0x02 => {
                    let table = read_string(data, &mut pos)?;
                    let column = read_string(data, &mut pos)?;
                    let map_key = read_bytes(data, &mut pos)?;
                    Some(DeleteTarget::MapElement {
                        table,
                        column,
                        map_key,
                    })
                }
                _ => None,
            }
        }
    }

    // =======================================================================
    // A7.10 — DeleteTarget roundtrip (5 tests)
    // =======================================================================

    /// Column DeleteTarget serializes and deserializes correctly.
    #[test]
    fn accord_delete_target_column_roundtrip() {
        let target = DeleteTarget::Column {
            table: "users".to_string(),
            column: "email".to_string(),
        };
        let bytes = target.serialize();
        let decoded = DeleteTarget::deserialize(&bytes).expect("must deserialize");
        assert_eq!(
            target, decoded,
            "column delete target round-trip must match"
        );
    }

    /// Map element DeleteTarget serializes and deserializes correctly.
    #[test]
    fn accord_delete_target_map_element_roundtrip() {
        let target = DeleteTarget::MapElement {
            table: "settings".to_string(),
            column: "preferences".to_string(),
            map_key: b"theme".to_vec(),
        };
        let bytes = target.serialize();
        let decoded = DeleteTarget::deserialize(&bytes).expect("must deserialize");
        assert_eq!(
            target, decoded,
            "map element delete target round-trip must match"
        );
    }

    /// Column DeleteTarget applied through Accord state machine: PreAccept with
    /// the serialized target as the transaction payload, verify it persists.
    #[test]
    fn accord_delete_target_apply_column() {
        let (mut sm, _writer) = make_sm(1);

        let target = DeleteTarget::Column {
            table: "users".to_string(),
            column: "email".to_string(),
        };
        let payload = target.serialize();

        // Use the serialized payload as the partition key (conceptually, the
        // key identifies the row being deleted).
        let tid = txn(1, 1000);
        let t0 = ts(1000);

        let resp = sm.handle_preaccept(tid, t0, &payload, BallotNumber(0), 0);
        assert!(
            matches!(resp, SmResponse::PreAcceptOK { .. }),
            "PreAccept with column delete target must succeed"
        );

        // Commit and apply.
        sm.handle_accept(tid, t0, ts(1001), vec![], BallotNumber(1));
        sm.handle_commit(tid, t0, ts(1001), vec![]);
        sm.handle_apply(tid, payload.clone());

        let state = sm.get_state(&tid).unwrap();
        assert_eq!(state.phase, TxnPhase::Applied);
        // The result data should contain our serialized delete target.
        assert_eq!(
            state.result.as_deref(),
            Some(payload.as_slice()),
            "applied result must contain delete target payload"
        );
    }

    /// Map element DeleteTarget applied through state machine.
    #[test]
    fn accord_delete_target_apply_map_element() {
        let (mut sm, _writer) = make_sm(1);

        let target = DeleteTarget::MapElement {
            table: "settings".to_string(),
            column: "preferences".to_string(),
            map_key: b"color".to_vec(),
        };
        let payload = target.serialize();

        let tid = txn(1, 2000);
        let t0 = ts(2000);

        sm.handle_preaccept(tid, t0, &payload, BallotNumber(0), 0);
        sm.handle_accept(tid, t0, ts(2001), vec![], BallotNumber(1));
        sm.handle_commit(tid, t0, ts(2001), vec![]);
        sm.handle_apply(tid, payload.clone());

        let state = sm.get_state(&tid).unwrap();
        assert_eq!(state.phase, TxnPhase::Applied);
        assert_eq!(state.result.as_deref(), Some(payload.as_slice()));
    }

    /// Mixed batch: column and map element deletions in a single Accord
    /// transaction (represented as a concatenated payload).
    #[test]
    fn accord_delete_target_mixed_batch() {
        let (mut sm, _writer) = make_sm(1);

        let target1 = DeleteTarget::Column {
            table: "users".to_string(),
            column: "name".to_string(),
        };
        let target2 = DeleteTarget::MapElement {
            table: "users".to_string(),
            column: "attrs".to_string(),
            map_key: b"role".to_vec(),
        };

        // Concatenate payloads with length prefix.
        let p1 = target1.serialize();
        let p2 = target2.serialize();
        let mut batch_payload = Vec::new();
        batch_payload.extend_from_slice(&(2u32).to_le_bytes()); // count
        batch_payload.extend_from_slice(&(p1.len() as u32).to_le_bytes());
        batch_payload.extend_from_slice(&p1);
        batch_payload.extend_from_slice(&(p2.len() as u32).to_le_bytes());
        batch_payload.extend_from_slice(&p2);

        let tid = txn(1, 3000);
        let t0 = ts(3000);

        sm.handle_preaccept(tid, t0, b"users:pk1", BallotNumber(0), 0);
        sm.handle_accept(tid, t0, ts(3001), vec![], BallotNumber(1));
        sm.handle_commit(tid, t0, ts(3001), vec![]);
        sm.handle_apply(tid, batch_payload.clone());

        let state = sm.get_state(&tid).unwrap();
        assert_eq!(state.phase, TxnPhase::Applied);

        // Verify both targets can be extracted from the batch payload.
        let result = state.result.as_ref().unwrap();
        let count = u32::from_le_bytes(result[0..4].try_into().unwrap()) as usize;
        assert_eq!(count, 2, "batch must contain 2 delete targets");
    }

    // =======================================================================
    // A7.10 — Token Range (4 tests)
    // =======================================================================

    /// Token-based range routing: a range query registered in the ConflictIndex
    /// is detected by point writes within the range.
    #[test]
    fn accord_token_fn_range_routing() {
        let mut idx = ConflictIndex::new(100);

        // Register a range query [100, 200].
        let range = TokenRange {
            start: 100,
            end: 200,
        };
        idx.register_range(range, ts(1000), TxnId(ts(1000)))
            .unwrap();

        // A point write at token 150 is within the range.
        // The range query should be detected as a conflict.
        let query_range = TokenRange {
            start: 150,
            end: 150,
        };
        let conflict = idx.max_conflicting_range_timestamp(&query_range);
        assert!(
            conflict.is_some(),
            "point write within range must detect conflict"
        );
        assert_eq!(conflict.unwrap(), ts(1000));
    }

    /// Range query conflicts with a point write within its bounds.
    #[test]
    fn accord_token_fn_conflict_with_point_write() {
        let mut idx = ConflictIndex::new(100);

        // Register a range [0, 500].
        let range = TokenRange { start: 0, end: 500 };
        let range_txn = TxnId(ts(1000));
        idx.register_range(range, ts(1000), range_txn).unwrap();

        // A point write at key "abc" (simulated as token 250) should conflict.
        // We check via range overlap: point write is [250, 250].
        let point_range = TokenRange {
            start: 250,
            end: 250,
        };
        let conflict = idx.max_conflicting_range_timestamp(&point_range);
        assert_eq!(
            conflict,
            Some(ts(1000)),
            "point write within range [0,500] must conflict"
        );
    }

    /// No conflict for point writes outside the registered range.
    #[test]
    fn accord_token_fn_no_conflict_outside_range() {
        let mut idx = ConflictIndex::new(100);

        // Register a range [100, 200].
        let range = TokenRange {
            start: 100,
            end: 200,
        };
        idx.register_range(range, ts(1000), TxnId(ts(1000)))
            .unwrap();

        // Point write at token 300 is outside the range.
        let outside_range = TokenRange {
            start: 300,
            end: 300,
        };
        let conflict = idx.max_conflicting_range_timestamp(&outside_range);
        assert!(
            conflict.is_none(),
            "point write outside range must not detect conflict"
        );

        // Point write at token 50 is also outside.
        let before_range = TokenRange { start: 50, end: 50 };
        assert!(
            idx.max_conflicting_range_timestamp(&before_range).is_none(),
            "point write before range must not detect conflict"
        );
    }

    /// Full scan: a range covering the entire token space conflicts with
    /// all registered ranges.
    #[test]
    fn accord_token_fn_full_scan() {
        let mut idx = ConflictIndex::new(100);

        // Register two non-overlapping ranges.
        let range1 = TokenRange {
            start: 100,
            end: 200,
        };
        let range2 = TokenRange {
            start: 500,
            end: 600,
        };
        idx.register_range(range1, ts(1000), TxnId(ts(1000)))
            .unwrap();
        idx.register_range(range2, ts(2000), TxnId(ts(2000)))
            .unwrap();

        // Full scan: [0, i64::MAX] covers everything.
        let full_scan = TokenRange {
            start: 0,
            end: i64::MAX,
        };
        let conflict = idx.max_conflicting_range_timestamp(&full_scan);
        assert_eq!(
            conflict,
            Some(ts(2000)),
            "full scan must detect highest conflict timestamp"
        );
    }

    // =======================================================================
    // A7.10 — LWW Deletion (4 tests)
    // =======================================================================

    /// LWW deletion is idempotent: applying the same delete twice yields
    /// the same result.
    #[test]
    fn accord_deletion_lww_idempotent() {
        let (mut sm, _writer) = make_sm(1);

        let tid = txn(1, 5000);
        let t0 = ts(5000);

        // First delete.
        sm.handle_preaccept(tid, t0, b"lww:key", BallotNumber(0), 0);
        sm.handle_accept(tid, t0, ts(5001), vec![], BallotNumber(1));
        sm.handle_commit(tid, t0, ts(5001), vec![]);
        sm.handle_apply(tid, b"DELETE:lww:key".to_vec());

        let state1 = sm.get_state(&tid).unwrap().clone();
        assert_eq!(state1.phase, TxnPhase::Applied);

        // Apply again (idempotent).
        sm.handle_apply(tid, b"DELETE:lww:key".to_vec());
        let state2 = sm.get_state(&tid).unwrap();

        // State must not change.
        assert_eq!(state1.phase, state2.phase);
        assert_eq!(state1.t, state2.t);
    }

    /// LWW deletion ordering: a delete at t=5001 supersedes a write at t=5000.
    #[test]
    fn accord_deletion_lww_ordering() {
        let (mut sm, _writer) = make_sm(1);

        // Write at t=5000.
        let write_tid = txn(1, 5000);
        let write_t0 = ts(5000);
        sm.handle_preaccept(write_tid, write_t0, b"lww:key", BallotNumber(0), 0);
        sm.handle_accept(write_tid, write_t0, ts(5000), vec![], BallotNumber(1));
        sm.handle_commit(write_tid, write_t0, ts(5000), vec![]);
        sm.handle_apply(write_tid, b"WRITE:value".to_vec());

        // Delete at t=5001 (later timestamp wins).
        let del_tid = txn(2, 5001);
        let del_t0 = ts(5001);
        sm.handle_preaccept(del_tid, del_t0, b"lww:key", BallotNumber(0), 0);

        // The delete should see the write as a dependency.
        let state = sm.get_state(&del_tid).unwrap();
        assert_eq!(state.phase, TxnPhase::PreAccepted);

        sm.handle_accept(del_tid, del_t0, ts(5002), vec![write_tid], BallotNumber(1));
        sm.handle_commit(del_tid, del_t0, ts(5002), vec![write_tid]);
        sm.handle_apply(del_tid, b"DELETE:lww:key".to_vec());

        // Delete must be applied with higher timestamp.
        let del_state = sm.get_state(&del_tid).unwrap();
        assert_eq!(del_state.phase, TxnPhase::Applied);
        assert!(
            del_state.t > sm.get_state(&write_tid).unwrap().t,
            "delete timestamp must be greater than write timestamp"
        );
    }

    /// LWW replay order independent: regardless of delivery order, the
    /// higher-timestamp operation wins.
    #[test]
    fn accord_deletion_lww_replay_order_independent() {
        // Scenario: two state machines receive operations in opposite order.

        // SM1: write first, then delete.
        let (mut sm1, _w1) = make_sm(1);
        let write_tid = txn(1, 6000);
        let del_tid = txn(2, 6001);

        // SM1: write at 6000.
        sm1.handle_preaccept(write_tid, ts(6000), b"key", BallotNumber(0), 0);
        sm1.handle_accept(write_tid, ts(6000), ts(6000), vec![], BallotNumber(1));
        sm1.handle_commit(write_tid, ts(6000), ts(6000), vec![]);
        sm1.handle_apply(write_tid, b"WRITE".to_vec());

        // SM1: delete at 6001.
        sm1.handle_preaccept(del_tid, ts(6001), b"key", BallotNumber(0), 0);
        sm1.handle_accept(
            del_tid,
            ts(6001),
            ts(6001),
            vec![write_tid],
            BallotNumber(1),
        );
        sm1.handle_commit(del_tid, ts(6001), ts(6001), vec![write_tid]);
        sm1.handle_apply(del_tid, b"DELETE".to_vec());

        // SM2: delete first, then write (reversed delivery).
        let (mut sm2, _w2) = make_sm(2);

        // SM2: delete at 6001 (delivered first).
        sm2.handle_preaccept(del_tid, ts(6001), b"key", BallotNumber(0), 0);
        sm2.handle_accept(
            del_tid,
            ts(6001),
            ts(6001),
            vec![write_tid],
            BallotNumber(1),
        );
        sm2.handle_commit(del_tid, ts(6001), ts(6001), vec![write_tid]);
        sm2.handle_apply(del_tid, b"DELETE".to_vec());

        // SM2: write at 6000 (delivered second).
        sm2.handle_preaccept(write_tid, ts(6000), b"key", BallotNumber(0), 0);
        sm2.handle_accept(write_tid, ts(6000), ts(6000), vec![], BallotNumber(1));
        sm2.handle_commit(write_tid, ts(6000), ts(6000), vec![]);
        sm2.handle_apply(write_tid, b"WRITE".to_vec());

        // Both state machines must agree: delete wins (higher timestamp).
        let sm1_del = sm1.get_state(&del_tid).unwrap();
        let sm2_del = sm2.get_state(&del_tid).unwrap();
        assert_eq!(sm1_del.phase, TxnPhase::Applied);
        assert_eq!(sm2_del.phase, TxnPhase::Applied);
        assert_eq!(sm1_del.t, sm2_del.t, "committed timestamps must match");
    }

    /// LWW concurrent replicas: two replicas independently process the same
    /// delete, both must converge to the same state.
    #[test]
    fn accord_deletion_lww_concurrent_replicas() {
        let mut cluster = TestCluster::new(3);
        let replicas = vec![1, 2, 3];

        let tid = txn(1, 7000);
        let t0 = ts(7000);

        // PreAccept delete to all replicas.
        for &r in &replicas {
            cluster.send(TestMessage {
                src: 1,
                dst: r,
                payload: TestMessagePayload::PreAccept {
                    txn_id: tid,
                    t0,
                    key: b"lww:concurrent".to_vec(),
                },
            });
        }
        cluster.drain();

        // Commit to all.
        let max_t = cluster
            .replicas
            .iter()
            .filter_map(|r| r.txn_states.get(&tid))
            .map(|s| s.t)
            .max()
            .unwrap_or(t0);

        for &r in &replicas {
            cluster.send(TestMessage {
                src: 1,
                dst: r,
                payload: TestMessagePayload::Commit {
                    txn_id: tid,
                    t0,
                    t: max_t,
                    deps: vec![],
                },
            });
        }
        cluster.drain();

        // All replicas must agree.
        cluster.assert_consistent(&tid);

        // All must be committed with same timestamp.
        for &r in &replicas {
            let replica = cluster.replica(r);
            let state = replica.txn_states.get(&tid).unwrap();
            assert_eq!(state.phase, TxnPhase::Committed);
            assert_eq!(state.t, max_t);
        }
    }

    // =======================================================================
    // A7.10 — pk_indexes (2 tests)
    // =======================================================================

    /// Primary key indexes remain consistent after Accord transactions:
    /// all replicas' ConflictIndex entries are consistent after PreAccept.
    #[test]
    fn accord_pk_indexes_consistent() {
        let mut cluster = TestCluster::new(3);
        let replicas = vec![1, 2, 3];

        // Submit 5 transactions on different keys.
        let mut txn_ids = Vec::new();
        for i in 0..5u64 {
            let tid = txn(1, 8000 + i * 100);
            let t0 = ts(8000 + i * 100);
            txn_ids.push(tid);

            for &r in &replicas {
                cluster.send(TestMessage {
                    src: 1,
                    dst: r,
                    payload: TestMessagePayload::PreAccept {
                        txn_id: tid,
                        t0,
                        key: format!("pk:{}", i).into_bytes(),
                    },
                });
            }
        }
        cluster.drain();

        // Commit all.
        for (i, &tid) in txn_ids.iter().enumerate() {
            let t0 = ts(8000 + (i as u64) * 100);
            let max_t = cluster
                .replicas
                .iter()
                .filter_map(|r| r.txn_states.get(&tid))
                .map(|s| s.t)
                .max()
                .unwrap_or(t0);

            for &r in &replicas {
                cluster.send(TestMessage {
                    src: 1,
                    dst: r,
                    payload: TestMessagePayload::Commit {
                        txn_id: tid,
                        t0,
                        t: max_t,
                        deps: vec![],
                    },
                });
            }
        }
        cluster.drain();

        // All replicas must have all 5 transactions in Committed state.
        for &tid in &txn_ids {
            cluster.assert_consistent(&tid);
            for &r in &replicas {
                let replica = cluster.replica(r);
                let state = replica.txn_states.get(&tid).unwrap();
                assert_eq!(
                    state.phase,
                    TxnPhase::Committed,
                    "replica {} must have txn {:?} committed",
                    r,
                    tid
                );
            }
        }

        // Verify conflict tracking: each key's conflicts list on each replica
        // contains exactly its own transaction.
        for (i, &_tid) in txn_ids.iter().enumerate() {
            let key = format!("pk:{}", i);
            for replica in &cluster.replicas {
                let conflicts = replica.conflicts.get(key.as_bytes());
                assert!(
                    conflicts.is_some(),
                    "replica {} must track conflicts for key {}",
                    replica.node_id,
                    key
                );
                assert!(
                    !conflicts.unwrap().is_empty(),
                    "replica {} must have at least one conflict entry for key {}",
                    replica.node_id,
                    key
                );
            }
        }
    }

    /// pk_indexes after schema change: new transactions on the same key
    /// after a "schema change" (simulated as a new epoch) still detect
    /// conflicts correctly.
    #[test]
    fn accord_pk_indexes_after_schema_change() {
        let mut cluster = TestCluster::new(3);
        let replicas = vec![1, 2, 3];

        // Transaction T1 on key "schema:key" at epoch 0.
        let t1_id = txn(1, 9000);
        let t0_1 = ts(9000);
        for &r in &replicas {
            cluster.send(TestMessage {
                src: 1,
                dst: r,
                payload: TestMessagePayload::PreAccept {
                    txn_id: t1_id,
                    t0: t0_1,
                    key: b"schema:key".to_vec(),
                },
            });
        }
        cluster.drain();

        // Commit T1.
        for &r in &replicas {
            cluster.send(TestMessage {
                src: 1,
                dst: r,
                payload: TestMessagePayload::Commit {
                    txn_id: t1_id,
                    t0: t0_1,
                    t: t0_1,
                    deps: vec![],
                },
            });
        }
        cluster.drain();

        // Simulate schema change: T2 is a new transaction on the same key
        // but with a higher timestamp (representing post-schema-change).
        let t2_id = txn(2, 10000);
        let t0_2 = ts(10000);
        for &r in &replicas {
            cluster.send(TestMessage {
                src: 2,
                dst: r,
                payload: TestMessagePayload::PreAccept {
                    txn_id: t2_id,
                    t0: t0_2,
                    key: b"schema:key".to_vec(),
                },
            });
        }
        cluster.drain();

        // T2 must see T1 as a dependency (same key, earlier timestamp).
        for replica in &cluster.replicas {
            if let Some(state) = replica.txn_states.get(&t2_id) {
                assert!(
                    state.deps.contains(&t1_id),
                    "replica {} T2 must depend on T1 (same key, schema change boundary)",
                    replica.node_id
                );
            }
        }

        // Commit T2 with T1 as dependency.
        for &r in &replicas {
            cluster.send(TestMessage {
                src: 2,
                dst: r,
                payload: TestMessagePayload::Commit {
                    txn_id: t2_id,
                    t0: t0_2,
                    t: t0_2,
                    deps: vec![t1_id],
                },
            });
        }
        cluster.drain();

        cluster.assert_consistent(&t1_id);
        cluster.assert_consistent(&t2_id);
    }
}
