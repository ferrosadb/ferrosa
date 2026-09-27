//! Framing and apply-observer seam for PostgreSQL MVCC metadata carried through
//! the existing Accord write set. Cassandra mutations remain raw commit-log
//! mutations and do not use this envelope.

use ferrosa_common::accord::{Timestamp, TxnId};

const MAGIC: &[u8; 8] = b"FRPGMVC1";

/// Wrap a normal storage mutation with opaque PostgreSQL row-version metadata.
pub fn encode_mutation(storage_mutation: &[u8], mvcc_metadata: &[u8]) -> Result<Vec<u8>, String> {
    let metadata_len = u32::try_from(mvcc_metadata.len())
        .map_err(|_| "PostgreSQL MVCC metadata exceeds the framing limit".to_string())?;
    let mut framed =
        Vec::with_capacity(MAGIC.len() + 4 + mvcc_metadata.len() + storage_mutation.len());
    framed.extend_from_slice(MAGIC);
    framed.extend_from_slice(&metadata_len.to_be_bytes());
    framed.extend_from_slice(mvcc_metadata);
    framed.extend_from_slice(storage_mutation);
    Ok(framed)
}

/// Return the underlying storage mutation and optional PostgreSQL metadata.
/// Raw mutations from CQL and marker-only transactions pass through unchanged.
pub fn decode_mutation(data: &[u8]) -> Result<(&[u8], Option<&[u8]>), String> {
    if !data.starts_with(MAGIC) {
        return Ok((data, None));
    }
    let header_len = MAGIC.len() + 4;
    let header = data
        .get(..header_len)
        .ok_or_else(|| "truncated PostgreSQL MVCC mutation envelope".to_string())?;
    let metadata_len = u32::from_be_bytes(
        header[MAGIC.len()..header_len]
            .try_into()
            .map_err(|_| "invalid PostgreSQL MVCC metadata length".to_string())?,
    ) as usize;
    let metadata_end = header_len
        .checked_add(metadata_len)
        .ok_or_else(|| "PostgreSQL MVCC metadata length overflow".to_string())?;
    let metadata = data
        .get(header_len..metadata_end)
        .ok_or_else(|| "truncated PostgreSQL MVCC metadata".to_string())?;
    let mutation = data
        .get(metadata_end..)
        .ok_or_else(|| "missing storage mutation in PostgreSQL MVCC envelope".to_string())?;
    if mutation.is_empty() {
        return Err("empty storage mutation in PostgreSQL MVCC envelope".to_string());
    }
    Ok((mutation, Some(metadata)))
}

/// Receives committed PostgreSQL row images after Accord applies the storage
/// write set on a replica. Implementations must be idempotent by `(txn_id, t)`.
pub trait PostgresMvccApplyObserver: Send + Sync + 'static {
    fn prepare_postgres_apply(
        &self,
        txn_id: TxnId,
        t: Timestamp,
        metadata: &[Vec<u8>],
    ) -> Result<(), String>;

    fn on_postgres_apply(
        &self,
        txn_id: TxnId,
        t: Timestamp,
        metadata: &[Vec<u8>],
    ) -> Result<(), String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_envelope_round_trips_and_raw_cql_bytes_pass_through() {
        let framed = encode_mutation(b"mutation", b"row images").unwrap();
        assert_eq!(
            decode_mutation(&framed).unwrap(),
            (b"mutation".as_slice(), Some(b"row images".as_slice()))
        );
        assert_eq!(
            decode_mutation(b"cql mutation").unwrap(),
            (b"cql mutation".as_slice(), None)
        );
    }

    #[test]
    fn malformed_postgres_envelope_fails_loud() {
        assert!(decode_mutation(MAGIC).is_err());
        let mut truncated = MAGIC.to_vec();
        truncated.extend_from_slice(&100u32.to_be_bytes());
        truncated.extend_from_slice(b"short");
        assert!(decode_mutation(&truncated).is_err());
    }
}
