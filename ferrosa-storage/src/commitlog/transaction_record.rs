//! A bounded local transaction occupies one CRC-protected WAL entry. A torn
//! record therefore yields no mutations. Existing ordinary records stay readable.
use super::mutation::Mutation;
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

const KEYSPACE: &str = "\0ferrosa.transaction.v1";
const TABLE: &str = "batch";

pub(crate) fn encode(mutations: &[Mutation], maximum: usize) -> ferrosa_common::Result<Mutation> {
    let too_large = || {
        ferrosa_common::Error::InvalidData(
            "transaction batch exceeds commit-log segment capacity".into(),
        )
    };
    let bytes = mutations.iter().try_fold(0usize, |total, mutation| {
        let size = mutation.serialized_size();
        u32::try_from(size).map_err(|_| too_large())?;
        total
            .checked_add(4)
            .and_then(|n| n.checked_add(size))
            .filter(|n| *n <= maximum)
            .ok_or_else(too_large)
    })?;
    let mut payload = Vec::with_capacity(bytes);
    for mutation in mutations {
        let size = mutation.serialized_size();
        payload.extend_from_slice(&(size as u32).to_be_bytes());
        let start = payload.len();
        payload.resize(start + size, 0);
        mutation.serialize_into(&mut payload[start..]);
    }
    Ok(Mutation::new(
        KEYSPACE.into(),
        TABLE.into(),
        DecoratedKey::new(PartitionKey::new(Vec::new())),
        vec![Row {
            clustering: Vec::new(),
            cells: vec![(0, CellValue::live(payload, 0))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        }],
        0,
    ))
}

pub(crate) fn decode(record: Mutation) -> ferrosa_common::Result<Vec<Mutation>> {
    if record.keyspace != KEYSPACE {
        return Ok(vec![record]);
    }
    let invalid =
        || ferrosa_common::Error::InvalidFormat("invalid transaction WAL envelope".into());
    if record.table != TABLE || record.rows.len() != 1 || record.rows[0].cells.len() != 1 {
        return Err(invalid());
    }
    let mut payload = record.rows[0].cells[0]
        .1
        .value
        .as_deref()
        .ok_or_else(invalid)?;
    let mut mutations = Vec::new();
    while !payload.is_empty() {
        let length = payload.get(..4).ok_or_else(invalid)?;
        let size = u32::from_be_bytes(length.try_into().unwrap()) as usize;
        let frame = payload
            .get(4..4usize.checked_add(size).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        let mutation = Mutation::deserialize_from(frame).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("transaction WAL mutation: {e}"))
        })?;
        if mutation.keyspace == KEYSPACE {
            return Err(invalid());
        }
        mutations.push(mutation);
        payload = &payload[4 + size..];
    }
    if mutations.is_empty() {
        return Err(invalid());
    }
    Ok(mutations)
}
