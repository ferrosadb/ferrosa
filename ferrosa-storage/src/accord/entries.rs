//! Accord commit log entry types and serialization.
//!
//! Two entry types support the dual-log architecture:
//!
//! - [`AccordProtocolEntry`] — PreAccepted / Accepted / Committed states.
//!   Written to the local-only protocol log (never uploaded to S3).
//! - [`AccordAppliedEntry`] — Final applied state with result data.
//!   Written to the main commit log (uploaded to S3).
//!
//! # Wire format
//!
//! Each serialized entry uses a simple binary format:
//!
//! ```text
//! [1-byte discriminant] [length-prefixed fields...] [4-byte CRC32]
//! ```
//!
//! Length-prefixed fields use a 4-byte little-endian length prefix followed
//! by the field bytes. Fixed-size fields (u64, u32) are little-endian.
//! The CRC32 covers all bytes preceding the checksum.

use std::io;

// ---------------------------------------------------------------------------
// Accord-specific types (local definitions until A1.1 merges to ferrosa-common)
// ---------------------------------------------------------------------------

/// Accord hybrid logical timestamp: physical time + logical counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Timestamp {
    /// Microseconds since epoch (physical component).
    pub epoch_micros: u64,
    /// Logical counter for ordering within the same physical time.
    pub logical: u32,
}

/// Unique identifier for an Accord transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxnId {
    /// The originating node's identifier.
    pub node: u64,
    /// The timestamp at which the transaction was proposed.
    pub timestamp: Timestamp,
}

/// Ballot used in the Accepted phase of Accord's consensus protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AcceptedBallot {
    /// Ballot number (monotonically increasing per node).
    pub ballot: u64,
    /// Node that issued this ballot.
    pub node: u64,
}

// ---------------------------------------------------------------------------
// Entry types
// ---------------------------------------------------------------------------

/// Accord protocol log entries (local-only, not uploaded to S3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccordProtocolEntry {
    /// Transaction has been pre-accepted by this node.
    PreAccepted {
        txn_id: TxnId,
        /// Original proposed timestamp.
        t0: Timestamp,
        /// Possibly-adjusted timestamp.
        t: Timestamp,
        /// Dependency set.
        deps: Vec<TxnId>,
    },
    /// Transaction has been accepted (fast-path failed, classic round).
    Accepted {
        txn_id: TxnId,
        t0: Timestamp,
        t: Timestamp,
        deps: Vec<TxnId>,
        accepted_ballot: AcceptedBallot,
    },
    /// Transaction has been committed (durable agreement on ordering).
    Committed {
        txn_id: TxnId,
        t: Timestamp,
        deps: Vec<TxnId>,
    },
}

/// Accord entry for the main commit log (uploaded to S3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccordAppliedEntry {
    pub txn_id: TxnId,
    pub t: Timestamp,
    /// Serialized result of applying the transaction.
    pub result: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Discriminants
// ---------------------------------------------------------------------------

const DISC_PRE_ACCEPTED: u8 = 1;
const DISC_ACCEPTED: u8 = 2;
const DISC_COMMITTED: u8 = 3;
const DISC_APPLIED: u8 = 4;

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

/// Write a `Timestamp` (12 bytes: u64 + u32).
fn write_timestamp(buf: &mut Vec<u8>, ts: &Timestamp) {
    buf.extend_from_slice(&ts.epoch_micros.to_le_bytes());
    buf.extend_from_slice(&ts.logical.to_le_bytes());
}

/// Read a `Timestamp` from a cursor position. Advances `pos`.
fn read_timestamp(data: &[u8], pos: &mut usize) -> io::Result<Timestamp> {
    if *pos + 12 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated timestamp",
        ));
    }
    let epoch_micros = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    let logical = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(Timestamp {
        epoch_micros,
        logical,
    })
}

/// Write a `TxnId` (20 bytes: u64 node + Timestamp).
fn write_txn_id(buf: &mut Vec<u8>, id: &TxnId) {
    buf.extend_from_slice(&id.node.to_le_bytes());
    write_timestamp(buf, &id.timestamp);
}

/// Read a `TxnId`.
fn read_txn_id(data: &[u8], pos: &mut usize) -> io::Result<TxnId> {
    if *pos + 8 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated txn_id node",
        ));
    }
    let node = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    let timestamp = read_timestamp(data, pos)?;
    Ok(TxnId { node, timestamp })
}

/// Write a `AcceptedBallot` (16 bytes: 2 x u64).
fn write_ballot(buf: &mut Vec<u8>, b: &AcceptedBallot) {
    buf.extend_from_slice(&b.ballot.to_le_bytes());
    buf.extend_from_slice(&b.node.to_le_bytes());
}

/// Read an `AcceptedBallot`.
fn read_ballot(data: &[u8], pos: &mut usize) -> io::Result<AcceptedBallot> {
    if *pos + 16 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated ballot",
        ));
    }
    let ballot = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    let node = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(AcceptedBallot { ballot, node })
}

/// Write a length-prefixed `Vec<TxnId>`.
fn write_deps(buf: &mut Vec<u8>, deps: &[TxnId]) {
    let count = deps.len() as u32;
    buf.extend_from_slice(&count.to_le_bytes());
    for dep in deps {
        write_txn_id(buf, dep);
    }
}

/// Read a length-prefixed `Vec<TxnId>`.
fn read_deps(data: &[u8], pos: &mut usize) -> io::Result<Vec<TxnId>> {
    if *pos + 4 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated deps count",
        ));
    }
    let count = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    let mut deps = Vec::with_capacity(count.min(4096)); // cap pre-alloc
    for _ in 0..count {
        deps.push(read_txn_id(data, pos)?);
    }
    Ok(deps)
}

/// Write a length-prefixed byte blob.
fn write_blob(buf: &mut Vec<u8>, blob: &[u8]) {
    let len = blob.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(blob);
}

/// Read a length-prefixed byte blob.
fn read_blob(data: &[u8], pos: &mut usize) -> io::Result<Vec<u8>> {
    if *pos + 4 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated blob length",
        ));
    }
    let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    if *pos + len > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated blob data",
        ));
    }
    let blob = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(blob)
}

// ---------------------------------------------------------------------------
// AccordProtocolEntry serialization
// ---------------------------------------------------------------------------

impl AccordProtocolEntry {
    /// Serialize this entry to bytes.
    ///
    /// Format: `[1-byte discriminant] [fields...] [4-byte CRC32]`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        match self {
            Self::PreAccepted {
                txn_id,
                t0,
                t,
                deps,
            } => {
                buf.push(DISC_PRE_ACCEPTED);
                write_txn_id(&mut buf, txn_id);
                write_timestamp(&mut buf, t0);
                write_timestamp(&mut buf, t);
                write_deps(&mut buf, deps);
            }
            Self::Accepted {
                txn_id,
                t0,
                t,
                deps,
                accepted_ballot,
            } => {
                buf.push(DISC_ACCEPTED);
                write_txn_id(&mut buf, txn_id);
                write_timestamp(&mut buf, t0);
                write_timestamp(&mut buf, t);
                write_deps(&mut buf, deps);
                write_ballot(&mut buf, accepted_ballot);
            }
            Self::Committed { txn_id, t, deps } => {
                buf.push(DISC_COMMITTED);
                write_txn_id(&mut buf, txn_id);
                write_timestamp(&mut buf, t);
                write_deps(&mut buf, deps);
            }
        }
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Deserialize an entry from bytes, verifying the CRC32 checksum.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, io::Error> {
        // Minimum: 1 discriminant + 4 CRC = 5 bytes.
        if bytes.len() < 5 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "entry too short",
            ));
        }

        // Verify CRC.
        let payload_len = bytes.len() - 4;
        let stored_crc = u32::from_le_bytes(bytes[payload_len..].try_into().unwrap());
        let computed_crc = crc32fast::hash(&bytes[..payload_len]);
        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("CRC mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"),
            ));
        }

        let mut pos = 0;
        let disc = bytes[pos];
        pos += 1;

        match disc {
            DISC_PRE_ACCEPTED => {
                let txn_id = read_txn_id(bytes, &mut pos)?;
                let t0 = read_timestamp(bytes, &mut pos)?;
                let t = read_timestamp(bytes, &mut pos)?;
                let deps = read_deps(bytes, &mut pos)?;
                Ok(Self::PreAccepted {
                    txn_id,
                    t0,
                    t,
                    deps,
                })
            }
            DISC_ACCEPTED => {
                let txn_id = read_txn_id(bytes, &mut pos)?;
                let t0 = read_timestamp(bytes, &mut pos)?;
                let t = read_timestamp(bytes, &mut pos)?;
                let deps = read_deps(bytes, &mut pos)?;
                let accepted_ballot = read_ballot(bytes, &mut pos)?;
                Ok(Self::Accepted {
                    txn_id,
                    t0,
                    t,
                    deps,
                    accepted_ballot,
                })
            }
            DISC_COMMITTED => {
                let txn_id = read_txn_id(bytes, &mut pos)?;
                let t = read_timestamp(bytes, &mut pos)?;
                let deps = read_deps(bytes, &mut pos)?;
                Ok(Self::Committed { txn_id, t, deps })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown discriminant: {disc}"),
            )),
        }
    }

    /// Returns the `TxnId` this entry belongs to.
    pub fn txn_id(&self) -> &TxnId {
        match self {
            Self::PreAccepted { txn_id, .. }
            | Self::Accepted { txn_id, .. }
            | Self::Committed { txn_id, .. } => txn_id,
        }
    }
}

// ---------------------------------------------------------------------------
// AccordAppliedEntry serialization
// ---------------------------------------------------------------------------

impl AccordAppliedEntry {
    /// Serialize this entry to bytes.
    ///
    /// Format: `[discriminant=4] [txn_id] [timestamp] [blob result] [4-byte CRC32]`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64 + self.result.len());
        buf.push(DISC_APPLIED);
        write_txn_id(&mut buf, &self.txn_id);
        write_timestamp(&mut buf, &self.t);
        write_blob(&mut buf, &self.result);
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Deserialize an entry from bytes, verifying the CRC32 checksum.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() < 5 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "entry too short",
            ));
        }

        // Verify CRC.
        let payload_len = bytes.len() - 4;
        let stored_crc = u32::from_le_bytes(bytes[payload_len..].try_into().unwrap());
        let computed_crc = crc32fast::hash(&bytes[..payload_len]);
        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("CRC mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"),
            ));
        }

        let mut pos = 0;
        let disc = bytes[pos];
        pos += 1;

        if disc != DISC_APPLIED {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected Applied discriminant ({DISC_APPLIED}), got {disc}"),
            ));
        }

        let txn_id = read_txn_id(bytes, &mut pos)?;
        let t = read_timestamp(bytes, &mut pos)?;
        let result = read_blob(bytes, &mut pos)?;

        Ok(Self { txn_id, t, result })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_timestamp(micros: u64, logical: u32) -> Timestamp {
        Timestamp {
            epoch_micros: micros,
            logical,
        }
    }

    fn make_txn_id(node: u64, micros: u64, logical: u32) -> TxnId {
        TxnId {
            node,
            timestamp: make_timestamp(micros, logical),
        }
    }

    fn make_ballot(ballot: u64, node: u64) -> AcceptedBallot {
        AcceptedBallot { ballot, node }
    }

    /// Test 3: accord_commitlog_roundtrip
    ///
    /// For each entry type (PreAccepted, Accepted, Committed, Applied):
    /// serialize to bytes, deserialize back, assert all fields match.
    /// Edge cases: empty deps, 256 deps, empty result, 1MB result.
    #[test]
    fn accord_commitlog_roundtrip() {
        let txn1 = make_txn_id(1, 1000, 0);
        let t0 = make_timestamp(1000, 0);
        let t = make_timestamp(1001, 1);

        // --- PreAccepted: empty deps ---
        let entry = AccordProtocolEntry::PreAccepted {
            txn_id: txn1,
            t0,
            t,
            deps: vec![],
        };
        let bytes = entry.serialize();
        let decoded = AccordProtocolEntry::deserialize(&bytes).unwrap();
        assert_eq!(entry, decoded);

        // --- PreAccepted: 256 deps ---
        let many_deps: Vec<TxnId> = (0..256).map(|i| make_txn_id(i, 2000 + i, 0)).collect();
        let entry = AccordProtocolEntry::PreAccepted {
            txn_id: txn1,
            t0,
            t,
            deps: many_deps.clone(),
        };
        let bytes = entry.serialize();
        let decoded = AccordProtocolEntry::deserialize(&bytes).unwrap();
        assert_eq!(entry, decoded);

        // --- Accepted ---
        let ballot = make_ballot(42, 7);
        let entry = AccordProtocolEntry::Accepted {
            txn_id: txn1,
            t0,
            t,
            deps: vec![make_txn_id(2, 999, 3)],
            accepted_ballot: ballot,
        };
        let bytes = entry.serialize();
        let decoded = AccordProtocolEntry::deserialize(&bytes).unwrap();
        assert_eq!(entry, decoded);

        // --- Committed ---
        let entry = AccordProtocolEntry::Committed {
            txn_id: txn1,
            t,
            deps: vec![make_txn_id(3, 500, 0), make_txn_id(4, 600, 1)],
        };
        let bytes = entry.serialize();
        let decoded = AccordProtocolEntry::deserialize(&bytes).unwrap();
        assert_eq!(entry, decoded);

        // --- Applied: empty result ---
        let entry = AccordAppliedEntry {
            txn_id: txn1,
            t,
            result: vec![],
        };
        let bytes = entry.serialize();
        let decoded = AccordAppliedEntry::deserialize(&bytes).unwrap();
        assert_eq!(entry, decoded);

        // --- Applied: 1MB result ---
        let big_result = vec![0xAB_u8; 1024 * 1024];
        let entry = AccordAppliedEntry {
            txn_id: txn1,
            t,
            result: big_result,
        };
        let bytes = entry.serialize();
        let decoded = AccordAppliedEntry::deserialize(&bytes).unwrap();
        assert_eq!(entry, decoded);
    }

    /// Test 7: protocol_log_corrupt_entry_skipped
    ///
    /// Serialize an entry, corrupt a byte in the CRC region,
    /// attempt deserialize, assert CRC error.
    #[test]
    fn protocol_log_corrupt_entry_skipped() {
        let entry = AccordProtocolEntry::PreAccepted {
            txn_id: make_txn_id(1, 1000, 0),
            t0: make_timestamp(1000, 0),
            t: make_timestamp(1001, 1),
            deps: vec![make_txn_id(2, 999, 0)],
        };
        let mut bytes = entry.serialize();
        assert!(bytes.len() > 10);

        // Corrupt a byte in the payload (not the CRC itself) so the CRC check fails.
        bytes[5] ^= 0xFF;

        let result = AccordProtocolEntry::deserialize(&bytes);
        assert!(
            result.is_err(),
            "corrupted entry should fail deserialization"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("CRC mismatch"),
            "error should mention CRC mismatch, got: {err}"
        );

        // Also corrupt an Applied entry.
        let applied = AccordAppliedEntry {
            txn_id: make_txn_id(1, 1000, 0),
            t: make_timestamp(1001, 1),
            result: vec![1, 2, 3],
        };
        let mut bytes = applied.serialize();
        // Corrupt the CRC bytes directly.
        let crc_start = bytes.len() - 4;
        bytes[crc_start] ^= 0xFF;

        let result = AccordAppliedEntry::deserialize(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("CRC mismatch"));
    }
}
