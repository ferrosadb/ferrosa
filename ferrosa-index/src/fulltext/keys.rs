//! Row-granular FTI document-key codec.
//!
//! Historically the FTI keyed each document by **partition key** and (worse)
//! concatenated every row's text in a partition into one document, so a hit
//! identified only the partition — leaking non-matching clustering rows on
//! `WHERE … AND col = fts_match(…)` against clustered tables (t_da51e20c).
//!
//! The index now keys each document by the **full primary key** so a hit
//! identifies an exact row. This codec serializes that key as
//! `[tag][pk_len: u32 BE][partition_key][clustering]` and parses the partition
//! portion back out (the router reads partitions by partition key, then keeps
//! only the rows whose full key actually matched).
//!
//! The leading [`ROW_KEY_TAG`] byte distinguishes a row-granular key from a
//! legacy bare-partition-key id, so a stale sidecar (pre-rebuild) is detected
//! rather than silently misparsed.

/// Format tag marking a row-granular full-key document id.
const ROW_KEY_TAG: u8 = 0x01;

/// Encode a row's document key as `[tag][pk_len: u32 BE][partition_key][clustering]`.
pub fn encode_doc_key(partition_key: &[u8], clustering: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + partition_key.len() + clustering.len());
    out.push(ROW_KEY_TAG);
    out.extend_from_slice(&(partition_key.len() as u32).to_be_bytes());
    out.extend_from_slice(partition_key);
    out.extend_from_slice(clustering);
    out
}

/// Extract the partition-key portion of a row-granular doc key, or `None` if the
/// bytes are not in the row-granular format (e.g. a legacy partition-key-only id
/// from a sidecar built before this change — such a sidecar must be rebuilt).
pub fn doc_key_partition(doc_key: &[u8]) -> Option<&[u8]> {
    if doc_key.first() != Some(&ROW_KEY_TAG) || doc_key.len() < 5 {
        return None;
    }
    let pk_len = u32::from_be_bytes(doc_key[1..5].try_into().ok()?) as usize;
    let start = 5usize;
    let end = start.checked_add(pk_len)?;
    if end > doc_key.len() {
        return None;
    }
    Some(&doc_key[start..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_extracts_partition_key() {
        let pk = b"tenant=1".to_vec();
        let ck = b"sid=1,id=1".to_vec();
        let key = encode_doc_key(&pk, &ck);
        assert_eq!(doc_key_partition(&key), Some(pk.as_slice()));
    }

    #[test]
    fn distinct_clustering_yields_distinct_keys_same_partition() {
        let pk = b"tenant=1".to_vec();
        let a = encode_doc_key(&pk, b"id=1");
        let b = encode_doc_key(&pk, b"id=2");
        assert_ne!(
            a, b,
            "rows in the same partition must have distinct doc keys"
        );
        assert_eq!(doc_key_partition(&a), doc_key_partition(&b));
    }

    #[test]
    fn empty_clustering_is_supported() {
        // Tables with no clustering columns: full key == partition key.
        let pk = b"pk".to_vec();
        let key = encode_doc_key(&pk, b"");
        assert_eq!(doc_key_partition(&key), Some(pk.as_slice()));
    }

    #[test]
    fn legacy_partition_key_id_is_rejected() {
        // A bare partition key (no tag) from a stale sidecar must not misparse.
        assert_eq!(doc_key_partition(b"raw-partition-key"), None);
        assert_eq!(doc_key_partition(&[]), None);
    }
}
