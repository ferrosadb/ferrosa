//! Accord protocol wire types — shared between the coordinator and replica
//! handler, serialized via bincode over `ferrosa-net`'s opaque `Bytes` payload.
//!
//! All types in this module are `pub(crate)` — they are an internal
//! serialisation contract and must not leak through the crate's public API.

use ferrosa_common::accord::{BallotNumber, Timestamp, TxnId};

// ---------------------------------------------------------------------------
// Coordinator → Replica
// ---------------------------------------------------------------------------

/// PreAccept request sent from coordinator to each replica.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PreAcceptPayload {
    pub(crate) txn_id: TxnId,
    pub(crate) t0: Timestamp,
    pub(crate) key: Vec<u8>,
    pub(crate) ballot: BallotNumber,
    pub(crate) epoch: u64,
}

/// Accept request sent from coordinator to each replica (slow path).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct AcceptPayload {
    pub(crate) txn_id: TxnId,
    pub(crate) t0: Timestamp,
    pub(crate) t: Timestamp,
    pub(crate) deps: Vec<TxnId>,
    pub(crate) ballot: BallotNumber,
}

/// Commit broadcast from coordinator to all replicas.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CommitPayload {
    pub(crate) txn_id: TxnId,
    pub(crate) t0: Timestamp,
    pub(crate) t: Timestamp,
    pub(crate) deps: Vec<TxnId>,
}

/// Apply request broadcast from coordinator to all replicas.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ApplyPayload {
    pub(crate) txn_id: TxnId,
    pub(crate) result_data: Vec<u8>,
}

/// Recovery probe from a recovery coordinator.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RecoverPayload {
    pub(crate) txn_id: TxnId,
    pub(crate) t0: Timestamp,
    pub(crate) ballot: BallotNumber,
}

// ---------------------------------------------------------------------------
// Multi-key (multi-partition) transactions — additive V2 wire family.
//
// bincode is NOT self-describing, so we cannot append fields to the shipped
// single-key payloads above without breaking the wire format. Multi-key
// transactions therefore travel on NEW message variants
// (`AccordPreAcceptV2`/`AccordApplyV2`) carrying V2 payloads. The single-key
// path keeps its exact bytes; a single-key transaction is the degenerate
// `writes.len() == 1` case of the multi-key path. The intermediate
// Accept/Commit phases carry only `txn_id`/`t`/`deps`, which are
// key-independent, so they REUSE the v1 [`AcceptPayload`] / [`CommitPayload`]
// rather than introducing redundant V2 twins.
//
// Only the types with a consumer in *this* phase are defined here:
// [`WriteSetEntry`] + [`ApplyV2Payload`] back the single-node multi-key apply
// path. `PreAcceptV2Payload` (the key-union PreAccept) lands with the Phase 2
// multi-shard PreAccept fan-out that first constructs it; the wire code
// `AccordPreAcceptV2` is reserved now (round-trip tested in `ferrosa-net`).
// ---------------------------------------------------------------------------

/// One write in a multi-key transaction's write-set: the raw partition-key
/// bytes (used for Accord conflict ordering and replica/shard routing) paired
/// with the encoded commit-log `Mutation` to apply for that key.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct WriteSetEntry {
    /// Raw partition-key bytes for this write (conflict ordering + routing).
    pub(crate) key: Vec<u8>,
    /// Encoded self-describing commit-log `Mutation` to apply for this key.
    pub(crate) mutation: Vec<u8>,
}

/// Apply request for a multi-key transaction.
///
/// Carries the full write-set; each replica applies the mutations for the keys
/// it owns (in dependency order via the `DepWaitApplier`). The single-key
/// [`ApplyPayload`] is the degenerate one-entry case.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ApplyV2Payload {
    pub(crate) txn_id: TxnId,
    /// All `(key, mutation)` writes for this transaction.
    pub(crate) writes: Vec<WriteSetEntry>,
}

/// PreAccept request for a multi-key transaction.
///
/// Carries every partition key the transaction writes so the replica registers
/// the txn under all of them and returns the UNION of dependencies across keys
/// (t_276e12). The single-key [`PreAcceptPayload`] is the degenerate one-key
/// case, kept byte-identical for single-partition LWT.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PreAcceptV2Payload {
    pub(crate) txn_id: TxnId,
    pub(crate) t0: Timestamp,
    /// All partition keys the transaction writes (conflict-ordering keys).
    pub(crate) keys: Vec<Vec<u8>>,
    pub(crate) ballot: BallotNumber,
    pub(crate) epoch: u64,
    /// PostgreSQL snapshot timestamp. When present, replicas reject a snapshot
    /// that is older than a committed or in-flight transaction on a conflict key.
    #[serde(default)]
    pub(crate) snapshot_ts: Option<Timestamp>,
}

// ---------------------------------------------------------------------------
// Replica → Coordinator
// ---------------------------------------------------------------------------

/// PreAcceptOK response from a replica.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PreAcceptOkPayload {
    /// The replica's node ID, so the coordinator knows who responded.
    pub(crate) from: u64,
    /// Replica's proposed execution timestamp (may differ from t0 if conflict).
    pub(crate) t: Timestamp,
    /// Dependency set detected by this replica.
    pub(crate) deps: Vec<TxnId>,
    /// True when this replica refuses an MVCC snapshot older than a known
    /// conflicting PostgreSQL transaction. Default false for rolling upgrades.
    #[serde(default)]
    pub(crate) snapshot_stale: bool,
}

/// AcceptOK response from a replica (slow path).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct AcceptOkPayload {
    pub(crate) txn_id: TxnId,
    /// Effective dependencies retained by this replica when accepting.
    pub(crate) deps: Vec<TxnId>,
}

/// Pre-dependency AcceptOK shape, accepted during rolling upgrades.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LegacyAcceptOkPayload {
    pub(crate) txn_id: TxnId,
}

// ---------------------------------------------------------------------------
// Gap 4: Linearizable read-vote (coordinator → replica → coordinator)
// ---------------------------------------------------------------------------

/// Read-vote request: coordinator asks each replica to read the current row
/// value *within the Accord epoch* so that the IF condition can be evaluated
/// linearly across F+1 replicas at the agreed execution timestamp `t`.
///
/// Sent from coordinator to each replica after consensus (Commit phase) but
/// before the LWT result is returned to the client.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReadVotePayload {
    pub(crate) txn_id: TxnId,
    /// Agreed execution timestamp (from Commit).
    pub(crate) t: Timestamp,
    /// Partition key bytes.
    pub(crate) key: Vec<u8>,
    /// Predicate descriptor: how the replica should answer the read-vote.
    ///
    /// Defaults (via `#[serde(default)]`) to [`ReadPredicate::NotExists`] so a
    /// pre-upgrade coordinator that omits the field still gets the existing
    /// `INSERT IF NOT EXISTS` existence semantics.
    #[serde(default)]
    pub(crate) predicate: ReadPredicate,
}

/// What the read-vote must determine on the replica.
///
/// The replica never interprets CQL predicate operators (those types live in
/// `ferrosa-cql`, which depends on this crate). For a generic `IF col=val`, the
/// replica only performs the linearizable read-at-`t` and returns the row
/// bytes; the coordinator (which owns the table schema) evaluates the predicate
/// with the canonical `eval_if_conditions`.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum ReadPredicate {
    /// Retired, kept only so its wire tag decodes. Replicas and the
    /// coordinator's own replica REFUSE it (abstain), so a transaction carrying
    /// it cannot reach F+1 and fails loud.
    ///
    /// It used to be answered from the Accord conflict index, keyed by
    /// partition-key bytes with no table or clustering: an earlier Accord write
    /// with the same key bytes in another table, or to another row, read as
    /// "exists" (t_7a0acbc8), and a row written without Accord was never seen.
    /// `INSERT IF NOT EXISTS` now sends [`ReadPredicate::ReadRow`] and the
    /// coordinator gates on the row it reads (t_fe2426bb).
    ///
    /// It stays the default so a driver built without an explicit predicate,
    /// or a pre-upgrade peer that omits the field, fails closed instead of
    /// applying unconditionally.
    #[default]
    NotExists,
    /// Generic `IF <conditions>`: the replica reads the row at `t` and returns
    /// its serialized bytes; the coordinator evaluates the predicate. Carries the
    /// `keyspace`/`table` so the replica's [`StorageReader`] can target the read.
    ///
    /// [`StorageReader`]: crate::accord::apply::StorageReader
    ReadRow {
        /// Keyspace of the target table.
        keyspace: String,
        /// Target table name.
        table: String,
    },
    /// PostgreSQL transaction-begin barrier. The replica votes only after
    /// transactions ordered before this timestamp have applied locally.
    SnapshotBarrier,
    /// Unconditional commit: there is no `IF` to evaluate, so the transaction
    /// always applies after commit. The coordinator SKIPS the read-vote phase
    /// entirely (no `AccordRead` fan-out). This is the path for a general
    /// multi-key SQL transaction (`BEGIN`/`COMMIT`), which has no LWT condition.
    Always,
    /// A conditional statement on one row: as [`ReadPredicate::ReadRow`], but
    /// the replica returns only the row at `clustering` (empty for a table
    /// without clustering columns), so a wide partition does not cross the
    /// wire and a write to another row of the partition cannot make the
    /// replicas' answers differ (t_5504f601).
    ///
    /// Appended last so the earlier variants keep their wire tags. A replica
    /// that predates it cannot decode the vote and does not answer, so the
    /// coordinator's F+1 agreement fails loud during a rolling upgrade.
    ReadClusteringRow {
        /// Keyspace of the target table.
        keyspace: String,
        /// Target table name.
        table: String,
        /// Serialized clustering of the row the statement writes.
        clustering: Vec<u8>,
    },
}

/// The storage read a row-reading read-vote performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRead<'a> {
    /// Keyspace of the target table.
    pub keyspace: &'a str,
    /// Target table name.
    pub table: &'a str,
    /// The one row to return, or `None` for the whole partition.
    pub clustering: Option<&'a [u8]>,
}

impl ReadPredicate {
    /// The row read this predicate asks replicas for, if it reads one.
    pub fn row_read(&self) -> Option<RowRead<'_>> {
        match self {
            Self::ReadRow { keyspace, table } => Some(RowRead {
                keyspace,
                table,
                clustering: None,
            }),
            Self::ReadClusteringRow {
                keyspace,
                table,
                clustering,
            } => Some(RowRead {
                keyspace,
                table,
                clustering: Some(clustering),
            }),
            Self::NotExists | Self::SnapshotBarrier | Self::Always => None,
        }
    }
}

/// Read-vote response from a replica.
///
/// Each replica reads the row at timestamp `t` (after waiting for all deps
/// to be applied) and reports whether the IF condition held.
///
/// For `INSERT IF NOT EXISTS`, `condition_holds` is true iff the row did NOT
/// exist at timestamp `t` (i.e., the write should apply).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReadVoteOkPayload {
    pub(crate) txn_id: TxnId,
    /// The replica that sent this response.
    pub(crate) from: u64,
    /// True if the IF condition held (the write should be applied).
    pub(crate) condition_holds: bool,
    /// Serialized current row value (empty when condition holds, populated
    /// when it does not — used to build the [applied]=false result set).
    pub(crate) current_row: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Gap 5: Apply-phase acknowledgement (coordinator → replica → coordinator)
// ---------------------------------------------------------------------------

/// ApplyOK response from a replica (used by coordinator to wait for F+1
/// apply acknowledgements before returning the LWT result to the client).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ApplyOkPayload {
    pub(crate) txn_id: TxnId,
    /// The replica that sent this acknowledgement.
    pub(crate) from: u64,
}

// ---------------------------------------------------------------------------
// Gap 5: Commit-phase acknowledgement (coordinator → replica → coordinator)
// ---------------------------------------------------------------------------

/// CommitOK response from a replica.
///
/// Commit is fire-and-forget in Accord, but the coordinator's per-shard quorum
/// still counts one ack per replica and the request-response transport needs a
/// reply. The reply must therefore PROVE it processed THIS transaction: it
/// echoes the inbound [`CommitPayload::txn_id`], exactly as [`ApplyOkPayload`]
/// does for Apply. Without it a coordinator could count a peer's reply toward
/// the commit quorum for whatever transaction it happened to be awaiting
/// (CL-48's sibling). An empty or unparseable body is NOT an ack.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CommitOkPayload {
    pub(crate) txn_id: TxnId,
    /// The replica that sent this acknowledgement.
    pub(crate) from: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(epoch: u64, time: u64, seq: u32, node: u64) -> Timestamp {
        Timestamp {
            epoch,
            time,
            seq,
            node,
        }
    }

    fn txn(epoch: u64, time: u64, seq: u32, node: u64) -> TxnId {
        TxnId(ts(epoch, time, seq, node))
    }

    fn assert_bincode_roundtrip<T>(value: &T)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
    {
        let encoded = bincode::serialize(value).expect("payload should serialize");
        let decoded: T = bincode::deserialize(&encoded).expect("payload should deserialize");
        assert_eq!(decoded, *value);
    }

    #[test]
    fn coordinator_to_replica_payloads_preserve_identity_ordering_and_payload_bytes() {
        let t0 = ts(7, 1_000, 2, 11);
        let t = ts(7, 1_250, 4, 22);
        let txn_id = txn(7, 1_000, 2, 11);
        let dep_a = txn(7, 900, 1, 33);
        let dep_b = txn(7, 950, 3, 44);

        assert_bincode_roundtrip(&PreAcceptPayload {
            txn_id,
            t0,
            key: b"partition-key\0with-bytes".to_vec(),
            ballot: BallotNumber(42),
            epoch: 7,
        });
        assert_bincode_roundtrip(&AcceptPayload {
            txn_id,
            t0,
            t,
            deps: vec![dep_a, dep_b],
            ballot: BallotNumber(43),
        });
        assert_bincode_roundtrip(&CommitPayload {
            txn_id,
            t0,
            t,
            deps: vec![dep_a, dep_b],
        });
        assert_bincode_roundtrip(&ApplyPayload {
            txn_id,
            result_data: b"[applied]=true\nrow=value".to_vec(),
        });
        assert_bincode_roundtrip(&RecoverPayload {
            txn_id,
            t0,
            ballot: BallotNumber(44),
        });
        assert_bincode_roundtrip(&ReadVotePayload {
            txn_id,
            t,
            key: b"read-vote-key".to_vec(),
            predicate: ReadPredicate::NotExists,
        });
        assert_bincode_roundtrip(&ReadVotePayload {
            txn_id,
            t,
            key: b"read-vote-key".to_vec(),
            predicate: ReadPredicate::ReadRow {
                keyspace: "ks".into(),
                table: "t".into(),
            },
        });
    }

    #[test]
    fn multikey_v2_payloads_roundtrip_including_single_key_degenerate_case() {
        let txn_id = txn(7, 1_000, 2, 11);

        // Two-key write-set: distinct keys, distinct mutation bytes.
        assert_bincode_roundtrip(&ApplyV2Payload {
            txn_id,
            writes: vec![
                WriteSetEntry {
                    key: b"key-alpha".to_vec(),
                    mutation: b"mutation-for-alpha".to_vec(),
                },
                WriteSetEntry {
                    key: b"key-beta\0bin".to_vec(),
                    mutation: b"mutation-for-beta".to_vec(),
                },
            ],
        });

        // Degenerate single-key case: a one-entry V2 write-set round-trips and
        // carries exactly the same key+mutation a single-key txn would.
        let single = ApplyV2Payload {
            txn_id,
            writes: vec![WriteSetEntry {
                key: b"only-key".to_vec(),
                mutation: b"only-mutation".to_vec(),
            }],
        };
        assert_bincode_roundtrip(&single);
        assert_eq!(single.writes.len(), 1);
        assert_eq!(single.writes[0].key, b"only-key");
        assert_eq!(single.writes[0].mutation, b"only-mutation");

        // Empty write-set (read-only / protocol-only) round-trips too.
        assert_bincode_roundtrip(&ApplyV2Payload {
            txn_id,
            writes: vec![],
        });
    }

    /// The DIRECT-FROM-BORROW capnp encoder the live apply fan-out uses
    /// (`ferrosa_net::protocol::encode_accord_apply_v2`, which writes each borrowed
    /// `(key, mutation)` slice straight into the capnp arena) must produce a frame that
    /// decodes to exactly the `ApplyV2Payload` the bincode path decodes to — for the
    /// empty, single-entry and multi-entry write-sets, and keys / mutations with
    /// embedded NULs. This is the wire-equivalence the migrated send path relies on: a
    /// replica decoding the capnp frame applies the same write-set it always did.
    #[test]
    fn capnp_apply_v2_from_borrowed_slices_matches_the_bincode_frame() {
        use ferrosa_net::protocol::{
            decode_accord_apply_v2, encode_accord_apply_v2, AccordControlMessage, AccordTxnId,
        };

        let txn_id = txn(21, 4_242, 3, 22);
        let accord = AccordTxnId {
            epoch: txn_id.0.epoch,
            time: txn_id.0.time,
            seq: txn_id.0.seq,
            node: txn_id.0.node,
        };
        let case = |writes: Vec<WriteSetEntry>| {
            let bincode_decoded: ApplyV2Payload = bincode::deserialize(
                &bincode::serialize(&ApplyV2Payload {
                    txn_id,
                    writes: writes.clone(),
                })
                .expect("bincode encodes"),
            )
            .expect("bincode decodes");
            let frame = encode_accord_apply_v2(
                accord,
                writes
                    .iter()
                    .map(|w| (w.key.as_slice(), w.mutation.as_slice())),
            )
            .expect("capnp frame encodes");
            let capnp = match decode_accord_apply_v2(&frame).expect("capnp frame decodes") {
                AccordControlMessage::ApplyV2 { txn_id, writes } => ApplyV2Payload {
                    txn_id: TxnId(Timestamp {
                        epoch: txn_id.epoch,
                        time: txn_id.time,
                        seq: txn_id.seq,
                        node: txn_id.node,
                    }),
                    writes: writes
                        .into_iter()
                        .map(|w| WriteSetEntry {
                            key: w.key,
                            mutation: w.mutation,
                        })
                        .collect(),
                },
                other => panic!("expected an ApplyV2 payload, got {other:?}"),
            };
            assert_eq!(
                capnp, bincode_decoded,
                "the capnp frame must decode to exactly what the bincode frame decoded to"
            );
            assert_eq!(capnp, ApplyV2Payload { txn_id, writes });
        };

        case(vec![]);
        case(vec![WriteSetEntry {
            key: b"only-key".to_vec(),
            mutation: b"only-mutation".to_vec(),
        }]);
        case(vec![
            WriteSetEntry {
                key: b"key-alpha".to_vec(),
                mutation: b"mutation-for-alpha".to_vec(),
            },
            WriteSetEntry {
                key: b"key-beta\0bin".to_vec(),
                mutation: b"mutation-for-beta".to_vec(),
            },
        ]);
    }

    #[test]
    fn replica_to_coordinator_payloads_preserve_sender_condition_and_current_row() {
        let txn_id = txn(9, 2_000, 5, 55);
        let t = ts(9, 2_010, 6, 66);
        let dep_a = txn(9, 1_900, 1, 77);
        let dep_b = txn(9, 1_950, 2, 88);

        assert_bincode_roundtrip(&PreAcceptOkPayload {
            from: 2,
            t,
            deps: vec![dep_a, dep_b],
            snapshot_stale: false,
        });
        assert_bincode_roundtrip(&AcceptOkPayload {
            txn_id,
            deps: vec![dep_a, dep_b],
        });
        let current_accept_ok = bincode::serialize(&AcceptOkPayload {
            txn_id,
            deps: vec![dep_a, dep_b],
        })
        .unwrap();
        assert_eq!(
            bincode::deserialize::<LegacyAcceptOkPayload>(&current_accept_ok)
                .unwrap()
                .txn_id,
            txn_id,
            "older coordinators must be able to ignore the trailing dependency field"
        );
        assert_bincode_roundtrip(&ReadVoteOkPayload {
            txn_id,
            from: 3,
            condition_holds: false,
            current_row: b"existing-row-bytes".to_vec(),
        });
        assert_bincode_roundtrip(&ApplyOkPayload { txn_id, from: 4 });
        assert_bincode_roundtrip(&CommitOkPayload { txn_id, from: 5 });
    }

    /// The capnp Accord frame must decode to EXACTLY what the bincode frame
    /// decoded to. This is the migration-equivalence gate for the bulk data path:
    /// the empty, single-entry and multi-entry write-sets all survive both encoders
    /// as the same value.
    #[test]
    fn capnp_accord_apply_v2_decodes_to_exactly_what_the_bincode_frame_decoded_to() {
        use ferrosa_net::protocol::{
            decode_accord_envelope, encode_accord_envelope, AccordControlMessage, AccordTxnId,
            AccordWriteSetEntry,
        };

        fn to_accord_txn(id: TxnId) -> AccordTxnId {
            AccordTxnId {
                epoch: id.0.epoch,
                time: id.0.time,
                seq: id.0.seq,
                node: id.0.node,
            }
        }

        fn to_capnp(payload: &ApplyV2Payload) -> AccordControlMessage {
            AccordControlMessage::ApplyV2 {
                txn_id: to_accord_txn(payload.txn_id),
                writes: payload
                    .writes
                    .iter()
                    .map(|w| AccordWriteSetEntry {
                        key: w.key.clone(),
                        mutation: w.mutation.clone(),
                    })
                    .collect(),
            }
        }

        fn from_capnp(msg: &AccordControlMessage) -> ApplyV2Payload {
            match msg {
                AccordControlMessage::ApplyV2 { txn_id, writes } => ApplyV2Payload {
                    txn_id: TxnId(Timestamp {
                        epoch: txn_id.epoch,
                        time: txn_id.time,
                        seq: txn_id.seq,
                        node: txn_id.node,
                    }),
                    writes: writes
                        .iter()
                        .map(|w| WriteSetEntry {
                            key: w.key.clone(),
                            mutation: w.mutation.clone(),
                        })
                        .collect(),
                },
                other => panic!("expected an ApplyV2 payload, got {other:?}"),
            }
        }

        let txn_id = txn(21, 4_242, 3, 22);
        let cases: [Vec<WriteSetEntry>; 3] = [
            vec![],
            vec![WriteSetEntry {
                key: b"only-key".to_vec(),
                mutation: b"only-mutation".to_vec(),
            }],
            vec![
                WriteSetEntry {
                    key: b"key-alpha".to_vec(),
                    mutation: b"mutation-for-alpha".to_vec(),
                },
                WriteSetEntry {
                    key: b"key-beta\0with-nul".to_vec(),
                    mutation: vec![0x7Fu8; 4096],
                },
            ],
        ];

        for writes in cases {
            let payload = ApplyV2Payload { txn_id, writes };
            // The bincode frame the live path ships today.
            let bincode_bytes = bincode::serialize(&payload).expect("bincode encodes");
            let bincode_decoded: ApplyV2Payload =
                bincode::deserialize(&bincode_bytes).expect("bincode decodes");
            // The capnp frame the migrated path will ship.
            let capnp_frame =
                encode_accord_envelope(&to_capnp(&payload)).expect("capnp frame encodes");
            let capnp_decoded = from_capnp(&decode_accord_envelope(&capnp_frame).expect("decodes"));
            assert_eq!(
                capnp_decoded, bincode_decoded,
                "the capnp Accord frame must decode to exactly what the bincode frame decoded to"
            );
            // And both agree with the original payload.
            assert_eq!(capnp_decoded, payload);
        }
    }

    fn accord_txn(id: TxnId) -> ferrosa_net::protocol::AccordTxnId {
        ferrosa_net::protocol::AccordTxnId {
            epoch: id.0.epoch,
            time: id.0.time,
            seq: id.0.seq,
            node: id.0.node,
        }
    }

    /// The region-REFERENCE frame must decode BY OFFSET to exactly the mutation bytes
    /// the inline frame carried, across every write-set shape: EMPTY, a single entry, a
    /// multi-entry set, keys/mutations with embedded NULs, and an entry that ends at the
    /// region's LAST offset (so an off-by-one in the bounds is caught).
    #[test]
    fn region_apply_frame_decodes_by_offset_to_the_inline_frame_bytes() {
        use ferrosa_net::protocol::{
            decode_accord_apply_v2, decode_accord_apply_v2_region, encode_accord_apply_v2,
            encode_accord_apply_v2_region, AccordControlMessage,
        };

        let txn_id = txn(21, 4_242, 3, 22);
        let accord = accord_txn(txn_id);
        let cases: [Vec<(Vec<u8>, Vec<u8>)>; 3] = [
            vec![],
            vec![(b"only-key".to_vec(), b"only-mutation".to_vec())],
            vec![
                (b"key-alpha".to_vec(), b"mutation-for-alpha".to_vec()),
                (b"key-beta\0with-nul".to_vec(), vec![0x7Fu8; 4096]),
                (b"key-gamma".to_vec(), b"tail\0entry".to_vec()),
            ],
        ];

        for writes in cases {
            let region_frame =
                encode_accord_apply_v2_region(accord, writes.iter().map(|(_, m)| m.as_slice()))
                    .expect("region frame encodes");
            let view = decode_accord_apply_v2_region(&region_frame).expect("region frame decodes");
            assert_eq!(view.txn_id, accord, "the header carries the txn stamp");
            assert_eq!(view.len(), writes.len());

            // Every entry reads back byte-for-byte, and the LAST entry ends exactly at
            // the region's end.
            let region_mutations: Vec<&[u8]> = view
                .mutations()
                .map(|entry| entry.expect("entry in bounds"))
                .collect();
            assert_eq!(region_mutations.len(), writes.len());
            for (index, ((_, expected), got)) in writes.iter().zip(&region_mutations).enumerate() {
                assert_eq!(
                    *got,
                    expected.as_slice(),
                    "region entry {index} must decode by offset to the staged bytes"
                );
            }
            if let Some((_, last)) = writes.last() {
                let last_start = view.indexed_region_len() - last.len() as u64;
                assert_eq!(
                    view.entry(writes.len() - 1).expect("last entry in bounds"),
                    last.as_slice(),
                    "an entry at the region's LAST offset must decode exactly"
                );
                assert_eq!(last_start as usize, view.region().len() - last.len());
            }
            // One past the end FAILS LOUD — never a silent empty write.
            assert!(
                view.entry(writes.len()).is_err(),
                "an index past the last entry must fail loud"
            );

            // Equivalence: the inline frame's mutation bytes are exactly the region's.
            let inline_frame = encode_accord_apply_v2(
                accord,
                writes.iter().map(|(k, m)| (k.as_slice(), m.as_slice())),
            )
            .expect("inline frame encodes");
            let inline_mutations: Vec<Vec<u8>> =
                match decode_accord_apply_v2(&inline_frame).expect("inline frame decodes") {
                    AccordControlMessage::ApplyV2 { writes, .. } => {
                        writes.into_iter().map(|w| w.mutation).collect()
                    }
                    other => panic!("expected an ApplyV2 payload, got {other:?}"),
                };
            assert_eq!(
                region_mutations.len(),
                inline_mutations.len(),
                "the region and inline frames carry the same entry count"
            );
            for (region_bytes, inline_bytes) in region_mutations.iter().zip(&inline_mutations) {
                assert_eq!(
                    *region_bytes,
                    inline_bytes.as_slice(),
                    "a region entry must equal the inline frame's mutation bytes"
                );
            }
        }
    }

    /// The region frame must SHRINK the wire: it drops the per-entry key and the capnp
    /// struct/pointer overhead, replacing them with ONE contiguous region plus a flat
    /// (offset, length) index. Assert it directly on the largest frame the fan-out ships.
    #[test]
    fn region_apply_frame_is_smaller_than_the_inline_capnp_frame() {
        use ferrosa_net::protocol::{encode_accord_apply_v2, encode_accord_apply_v2_region};

        let accord = accord_txn(txn(7, 1, 1, 1));
        let writes: Vec<(Vec<u8>, Vec<u8>)> = (0..2_000u32)
            .map(|i| {
                (
                    format!("partition-key-{i:08}").into_bytes(),
                    vec![0xABu8; 128],
                )
            })
            .collect();

        let inline = encode_accord_apply_v2(
            accord,
            writes.iter().map(|(k, m)| (k.as_slice(), m.as_slice())),
        )
        .expect("inline frame encodes")
        .len();
        let region =
            encode_accord_apply_v2_region(accord, writes.iter().map(|(_, m)| m.as_slice()))
                .expect("region frame encodes")
                .len();

        assert!(
            region < inline,
            "the region frame must be SMALLER than the inline capnp frame \
             (region={region}, inline={inline})"
        );
    }

    /// A header whose index escapes the region — or is not a contiguous partition of it
    /// — must FAIL LOUD, so a peer never applies a truncated or misaligned write-set.
    #[test]
    fn a_region_index_that_does_not_cover_the_region_fails_loud() {
        use ferrosa_net::protocol::{
            decode_accord_apply_v2_region, encode_accord_envelope, AccordControlMessage,
        };

        let txn_id = txn(9, 9, 9, 9);
        let accord = accord_txn(txn_id);
        let build = |offsets: Vec<u64>, lengths: Vec<u32>, region: &[u8]| {
            let mut frame = encode_accord_envelope(&AccordControlMessage::ApplyV2Region {
                txn_id: accord,
                offsets,
                lengths,
            })
            .expect("header encodes");
            frame.extend_from_slice(region);
            frame
        };

        // A length that runs past the region.
        let escaping = build(vec![0], vec![99], b"short");
        assert!(
            decode_accord_apply_v2_region(&escaping).is_err(),
            "an entry length past the region end must fail loud"
        );

        // A non-contiguous offset (a gap the region does not have).
        let gapped = build(vec![0, 8], vec![2, 2], b"aabbccdd");
        assert!(
            decode_accord_apply_v2_region(&gapped).is_err(),
            "a non-contiguous index must fail loud"
        );

        // A header whose lengths do not cover the whole region (trailing bytes).
        let truncated = build(vec![0], vec![2], b"abcd");
        assert!(
            decode_accord_apply_v2_region(&truncated).is_err(),
            "an index that does not cover the whole region must fail loud"
        );
    }
}
