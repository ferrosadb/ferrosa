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
}

/// AcceptOK response from a replica (slow path).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct AcceptOkPayload {
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
    /// `INSERT IF NOT EXISTS`: condition holds iff the row does NOT exist at `t`.
    /// Evaluated on the replica via the existence path (no schema needed).
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
    /// Unconditional commit: there is no `IF` to evaluate, so the transaction
    /// always applies after commit. The coordinator SKIPS the read-vote phase
    /// entirely (no `AccordRead` fan-out). This is the path for a general
    /// multi-key SQL transaction (`BEGIN`/`COMMIT`), which has no LWT condition.
    Always,
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
        });
        assert_bincode_roundtrip(&AcceptOkPayload { txn_id });
        assert_bincode_roundtrip(&ReadVoteOkPayload {
            txn_id,
            from: 3,
            condition_holds: false,
            current_row: b"existing-row-bytes".to_vec(),
        });
        assert_bincode_roundtrip(&ApplyOkPayload { txn_id, from: 4 });
    }
}
