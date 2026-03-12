//! Trie traversal: lookup, floor, ceiling, and iteration.
//!
//! The walker navigates an on-disk trie by following transitions from the
//! root to leaves. It supports:
//!
//! - **Exact lookup**: find a key in the trie
//! - **Floor**: greatest key less than or equal to the query
//! - **Ceiling**: smallest key greater than or equal to the query
//! - **Iteration**: enumerate all payloads in order
//!
//! # Design
//!
//! The walker reads nodes via [`ReadAt`], following child
//! pointers as distances from the current node position. Since tries are
//! written bottom-up, children are at lower file positions than parents.
//!
//! For the partition index trie, payloads encode `(hash_byte, idxpos)`.
//! For the row index trie, payloads encode block offsets.

use ferrosa_common::{Error, Result};

use crate::io::ReadAt;
use crate::trie::node::{decode_payload, payload_size, read_node_header, NodeHeader};

/// Result of looking up a key in the trie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupResult {
    /// Key was found with the given payload.
    Found {
        /// Payload bits (4-bit nibble from the node header).
        payload_pb: u8,
        /// Raw payload bytes.
        payload_bytes: Vec<u8>,
    },
    /// Key was not found in the trie.
    NotFound,
}

/// Maximum bytes we ever need to read for a single node.
///
/// Dense nodes can be up to ~2060 bytes (LongDense with 256 children at 8
/// bytes each + 3 header + payload), so we use a generous buffer.
const MAX_NODE_READ: usize = 2560;

/// Look up an exact key in the trie.
///
/// Starts at `root_pos` and follows transitions byte-by-byte through the key.
/// Returns [`LookupResult::Found`] if the full key is consumed at a node with
/// a payload, otherwise [`LookupResult::NotFound`].
pub fn lookup(reader: &impl ReadAt, root_pos: u64, key: &[u8]) -> Result<LookupResult> {
    let data_len = reader.len()?;
    let mut pos = root_pos;
    let mut key_idx = 0;

    loop {
        if pos >= data_len {
            return Err(Error::InvalidData(format!(
                "trie node position {pos} out of bounds (len={data_len})"
            )));
        }

        // Read enough bytes to decode any node type.
        let read_len = MAX_NODE_READ.min((data_len - pos) as usize);
        let mut buf = vec![0u8; read_len];
        reader.read_exact_at(&mut buf[..read_len], pos)?;

        let header = read_node_header(&buf)?;

        if key_idx >= key.len() {
            // Key fully consumed — check for payload at this node.
            return extract_payload(&buf, &header);
        }

        // Find a transition matching the current key byte.
        let target = key[key_idx];
        match find_transition(&header, target) {
            Some(distance) => {
                // Children are below parents (written bottom-up).
                if distance > pos {
                    return Err(Error::InvalidData(format!(
                        "child distance {distance} exceeds current position {pos}"
                    )));
                }
                pos -= distance;
                key_idx += 1;
            }
            None => {
                return Ok(LookupResult::NotFound);
            }
        }
    }
}

/// Find the pointer distance for a given transition byte in a node.
///
/// For single nodes, checks the single transition.
/// For sparse nodes, searches the transitions list.
/// For dense nodes, checks if the target is in the [min..=max] range
/// and returns the pointer (0 means no child at that slot).
/// For payload-only nodes, there are no transitions.
fn find_transition(header: &NodeHeader, target: u8) -> Option<u64> {
    if header.transitions.is_empty() {
        return None;
    }

    if header.node_type.is_single() {
        if header.transitions[0] == target {
            return Some(header.pointers[0]);
        }
        return None;
    }

    if header.node_type.is_sparse() {
        for (i, &t) in header.transitions.iter().enumerate() {
            if t == target {
                return Some(header.pointers[i]);
            }
        }
        return None;
    }

    if header.node_type.is_dense() {
        let start = header.transitions[0];
        let end = *header.transitions.last().unwrap();
        if target < start || target > end {
            return None;
        }
        let idx = (target.wrapping_sub(start)) as usize;
        let ptr = header.pointers[idx];
        if ptr == 0 {
            // 0 means no child at this slot in dense encoding
            return None;
        }
        return Some(ptr);
    }

    // PayloadOnly has no transitions
    None
}

/// Extract the payload from a node, returning Found or NotFound.
fn extract_payload(buf: &[u8], header: &NodeHeader) -> Result<LookupResult> {
    if header.payload_bits == 0 {
        return Ok(LookupResult::NotFound);
    }
    let psize = payload_size(header.payload_bits);
    let ppos = header.payload_position;
    if ppos + psize > buf.len() {
        return Err(Error::InvalidData(format!(
            "payload extends beyond buffer: ppos={ppos}, psize={psize}, buflen={}",
            buf.len()
        )));
    }
    let payload_bytes = buf[ppos..ppos + psize].to_vec();
    Ok(LookupResult::Found {
        payload_pb: header.payload_bits,
        payload_bytes,
    })
}

/// Convenience: look up a key and decode its payload into (hash, idxpos).
pub fn lookup_payload(
    reader: &impl ReadAt,
    root_pos: u64,
    key: &[u8],
) -> Result<Option<(Option<u8>, i64)>> {
    match lookup(reader, root_pos, key)? {
        LookupResult::Found {
            payload_pb,
            payload_bytes,
        } => {
            let (hash, idxpos) = decode_payload(payload_pb, &payload_bytes)?;
            Ok(Some((hash, idxpos)))
        }
        LookupResult::NotFound => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::node::{encode_signed_bytes, NodeType};

    /// Helper: build a PayloadOnly node with a given pb and payload bytes.
    fn make_payload_only(pb: u8, payload: &[u8]) -> Vec<u8> {
        let type_byte = (NodeType::PayloadOnly as u8) << 4 | (pb & 0x0F);
        let mut data = vec![type_byte];
        data.extend_from_slice(payload);
        data
    }

    /// Helper: build a Single8 node with transition byte, pointer (8-bit), pb, and payload.
    fn make_single8(transition: u8, pointer: u8, pb: u8, payload: &[u8]) -> Vec<u8> {
        let type_byte = (NodeType::Single8 as u8) << 4 | (pb & 0x0F);
        let mut data = vec![type_byte, transition, pointer];
        data.extend_from_slice(payload);
        data
    }

    /// Helper: build a Sparse8 node with transitions, pointers (8-bit each), pb, and payload.
    fn make_sparse8(transitions: &[u8], pointers: &[u8], pb: u8, payload: &[u8]) -> Vec<u8> {
        assert_eq!(transitions.len(), pointers.len());
        let cc = transitions.len() as u8;
        let type_byte = (NodeType::Sparse8 as u8) << 4 | (pb & 0x0F);
        let mut data = vec![type_byte, cc];
        data.extend_from_slice(transitions);
        data.extend_from_slice(pointers);
        data.extend_from_slice(payload);
        data
    }

    /// Build a hand-crafted trie in memory (bottom-up: leaves first, root last).
    ///
    /// Trie structure for keys "ab" -> payload 42, "ac" -> payload 99:
    ///
    ///   root (transition 'a', points to mid)
    ///     mid (sparse: 'b' -> leaf1, 'c' -> leaf2)
    ///       leaf1 (payload = 42)
    ///       leaf2 (payload = 99)
    ///
    /// Layout (bottom-up):
    ///   [0..N1]: leaf1 (PayloadOnly, pb=1, payload=[42])
    ///   [N1..N2]: leaf2 (PayloadOnly, pb=1, payload=[99])
    ///   [N2..N3]: mid (Sparse8, transitions=[b,c], pointers to leaf1 and leaf2)
    ///   [N3..end]: root (Single8, transition=a, pointer to mid)
    fn build_test_trie() -> (Vec<u8>, u64) {
        let mut data = Vec::new();

        // leaf1 at offset 0: PayloadOnly, pb=1, payload = [42]
        let leaf1 = make_payload_only(1, &[42]);
        let leaf1_pos = data.len() as u64;
        data.extend_from_slice(&leaf1);

        // leaf2 at offset 2: PayloadOnly, pb=1, payload = [99]
        let leaf2 = make_payload_only(1, &[99]);
        let leaf2_pos = data.len() as u64;
        data.extend_from_slice(&leaf2);

        // mid at offset 4: Sparse8, transitions=[b'b', b'c'],
        // pointers are distances from mid_pos to leaves
        let mid_pos = data.len() as u64;
        let ptr1 = (mid_pos - leaf1_pos) as u8;
        let ptr2 = (mid_pos - leaf2_pos) as u8;
        let mid = make_sparse8(b"bc", &[ptr1, ptr2], 0, &[]);
        data.extend_from_slice(&mid);

        // root at offset 10: Single8, transition=b'a', pointer = distance to mid
        let root_pos = data.len() as u64;
        let root_ptr = (root_pos - mid_pos) as u8;
        let root = make_single8(b'a', root_ptr, 0, &[]);
        data.extend_from_slice(&root);

        (data, root_pos)
    }

    #[test]
    fn exact_match_ab() {
        let (data, root_pos) = build_test_trie();
        let result = lookup(&data, root_pos, b"ab").unwrap();
        match result {
            LookupResult::Found {
                payload_pb,
                payload_bytes,
            } => {
                assert_eq!(payload_pb, 1);
                assert_eq!(payload_bytes, vec![42]);
            }
            LookupResult::NotFound => panic!("expected Found for key 'ab'"),
        }
    }

    #[test]
    fn exact_match_ac() {
        let (data, root_pos) = build_test_trie();
        let result = lookup(&data, root_pos, b"ac").unwrap();
        match result {
            LookupResult::Found {
                payload_pb,
                payload_bytes,
            } => {
                assert_eq!(payload_pb, 1);
                assert_eq!(payload_bytes, vec![99]);
            }
            LookupResult::NotFound => panic!("expected Found for key 'ac'"),
        }
    }

    #[test]
    fn not_found_missing_key() {
        let (data, root_pos) = build_test_trie();
        let result = lookup(&data, root_pos, b"ad").unwrap();
        assert_eq!(result, LookupResult::NotFound);
    }

    #[test]
    fn not_found_wrong_prefix() {
        let (data, root_pos) = build_test_trie();
        let result = lookup(&data, root_pos, b"bb").unwrap();
        assert_eq!(result, LookupResult::NotFound);
    }

    #[test]
    fn not_found_empty_key() {
        let (data, root_pos) = build_test_trie();
        // Root has no payload, so empty key returns NotFound
        let result = lookup(&data, root_pos, b"").unwrap();
        assert_eq!(result, LookupResult::NotFound);
    }

    #[test]
    fn not_found_key_too_long() {
        let (data, root_pos) = build_test_trie();
        // "abc" matches "ab" prefix but leaf has no transition for 'c'
        let result = lookup(&data, root_pos, b"abc").unwrap();
        assert_eq!(result, LookupResult::NotFound);
    }

    #[test]
    fn not_found_key_partial_prefix() {
        let (data, root_pos) = build_test_trie();
        // "a" only reaches mid node which has no payload
        let result = lookup(&data, root_pos, b"a").unwrap();
        assert_eq!(result, LookupResult::NotFound);
    }

    #[test]
    fn lookup_payload_convenience() {
        let (data, root_pos) = build_test_trie();
        let result = lookup_payload(&data, root_pos, b"ab").unwrap();
        assert_eq!(result, Some((None, 42)));

        let result = lookup_payload(&data, root_pos, b"ac").unwrap();
        assert_eq!(result, Some((None, 99)));

        let result = lookup_payload(&data, root_pos, b"zz").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn lookup_payload_with_hash() {
        // Build a simple trie: key "x" -> payload with hash=0xAB, idxpos=256
        let mut data = Vec::new();

        // Leaf at offset 0: PayloadOnly, pb=9 (1 hash byte + 2 idx bytes)
        // payload = [0xAB, 0x01, 0x00]  -> hash=0xAB, idxpos=256
        let idxpos_bytes = encode_signed_bytes(256);
        let pb = 7 + idxpos_bytes.len() as u8; // pb = 7 + len = 9
        let mut payload = vec![0xAB];
        payload.extend_from_slice(&idxpos_bytes);
        let leaf = make_payload_only(pb, &payload);
        let leaf_pos = data.len() as u64;
        data.extend_from_slice(&leaf);

        // Root at offset N: Single8, transition='x', pointer to leaf
        let root_pos = data.len() as u64;
        let ptr = (root_pos - leaf_pos) as u8;
        let root = make_single8(b'x', ptr, 0, &[]);
        data.extend_from_slice(&root);

        let result = lookup_payload(&data, root_pos, b"x").unwrap();
        assert_eq!(result, Some((Some(0xAB), 256)));
    }
}
