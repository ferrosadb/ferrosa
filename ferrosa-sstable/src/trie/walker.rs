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

use crate::io::ReadAt;
use crate::trie::node::{self, NodeHeader};
use ferrosa_common::Result;

/// Result of a trie lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupResult {
    /// Exact match found. Payload bytes at the given position.
    Found {
        payload_pb: u8,
        payload_bytes: Vec<u8>,
    },
    /// No match found.
    NotFound,
}

/// Walk a trie to find an exact key match.
///
/// `reader`: the trie file (or slice)
/// `root_pos`: file position of the root node
/// `key`: the byte sequence to look up
pub fn lookup(reader: &impl ReadAt, root_pos: u64, key: &[u8]) -> Result<LookupResult> {
    let mut pos = root_pos;
    let mut key_idx = 0;

    loop {
        // Read enough bytes for any node (max ~2KB for LongDense with 256 children)
        let mut buf = vec![0u8; 2060];
        let file_len = reader.len()?;
        let read_len = buf.len().min((file_len - pos) as usize);
        reader.read_at(&mut buf[..read_len], pos)?;

        let header = node::read_node_header(&buf[..read_len])?;

        if key_idx >= key.len() {
            // We've consumed the entire key. Check for payload.
            if header.pb > 0 {
                let ps = node::payload_size(header.pb);
                let payload_start = header.payload_offset;
                let payload_bytes = buf[payload_start..payload_start + ps].to_vec();
                return Ok(LookupResult::Found {
                    payload_pb: header.pb,
                    payload_bytes,
                });
            } else {
                return Ok(LookupResult::NotFound);
            }
        }

        let target = key[key_idx];

        // Find transition matching target byte
        match find_transition(&header, target) {
            Some(child_distance) => {
                // Child is at pos - child_distance (written before parent)
                pos -= child_distance;
                key_idx += 1;
            }
            None => {
                return Ok(LookupResult::NotFound);
            }
        }
    }
}

/// Find the child pointer distance for a given transition byte.
fn find_transition(header: &NodeHeader, target: u8) -> Option<u64> {
    for (i, &trans) in header.transitions.iter().enumerate() {
        if trans == target {
            return Some(header.child_pointers[i]);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal hand-crafted trie for testing.
    /// Trie structure: root has transitions 'a' and 'b'.
    /// 'a' leads to a leaf with payload.
    /// 'b' leads to a leaf with payload.
    ///
    /// Since tries are bottom-up, leaves come first in the file:
    /// pos 0: leaf for 'a' (PayloadOnly, pb=8, hash=0xAA, idxpos=0x42)
    /// pos 3: leaf for 'b' (PayloadOnly, pb=8, hash=0xBB, idxpos=0x43)
    /// pos 6: root (Sparse8, 2 children, transitions ['a','b'], pointers [6, 3])
    fn build_test_trie() -> Vec<u8> {
        vec![
            // Leaf 'a' at pos 0: PayloadOnly with pb=8 (hash + 1-byte idxpos)
            0x08, // type=0, pb=8
            0xAA, // hash byte
            0x42, // idxpos = 0x42
            // Leaf 'b' at pos 3: PayloadOnly with pb=8
            0x08, // type=0, pb=8
            0xBB, // hash byte
            0x43, // idxpos = 0x43
            // Root at pos 6: Sparse8 with 2 children
            0x50, // type=5 (Sparse8), pb=0
            0x02, // cc=2
            b'a', // transition 'a'
            b'b', // transition 'b'
            0x06, // pointer to 'a' leaf: root_pos(6) - leaf_pos(0) = 6
            0x03, // pointer to 'b' leaf: root_pos(6) - leaf_pos(3) = 3
        ]
    }

    #[test]
    fn lookup_exact_match() {
        let trie = build_test_trie();
        let root_pos = 6u64;

        let result = lookup(&trie.as_slice(), root_pos, b"a").unwrap();
        match result {
            LookupResult::Found {
                payload_pb,
                payload_bytes,
            } => {
                assert_eq!(payload_pb, 8);
                let (hash, idxpos) = node::decode_payload(payload_pb, &payload_bytes).unwrap();
                assert_eq!(hash, Some(0xAA));
                assert_eq!(idxpos, 0x42);
            }
            _ => panic!("expected Found"),
        }
    }

    #[test]
    fn lookup_second_key() {
        let trie = build_test_trie();
        let result = lookup(&trie.as_slice(), 6, b"b").unwrap();
        match result {
            LookupResult::Found {
                payload_pb,
                payload_bytes,
            } => {
                let (hash, idxpos) = node::decode_payload(payload_pb, &payload_bytes).unwrap();
                assert_eq!(hash, Some(0xBB));
                assert_eq!(idxpos, 0x43);
            }
            _ => panic!("expected Found"),
        }
    }

    #[test]
    fn lookup_not_found() {
        let trie = build_test_trie();
        let result = lookup(&trie.as_slice(), 6, b"c").unwrap();
        assert_eq!(result, LookupResult::NotFound);
    }

    #[test]
    fn lookup_empty_key() {
        let trie = build_test_trie();
        // Root has no payload (pb=0), so empty key = NotFound
        let result = lookup(&trie.as_slice(), 6, b"").unwrap();
        assert_eq!(result, LookupResult::NotFound);
    }

    #[test]
    fn lookup_key_too_long() {
        let trie = build_test_trie();
        // "ab" tries to follow 'a' then 'b', but 'a' leaf has no transitions
        let result = lookup(&trie.as_slice(), 6, b"ab").unwrap();
        assert_eq!(result, LookupResult::NotFound);
    }
}
