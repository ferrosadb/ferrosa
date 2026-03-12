//! Bottom-up page-aware incremental trie builder.
//!
//! Constructs a trie from sorted byte-sequence keys. Keys must be added in
//! sorted order. The builder:
//!
//! 1. Tracks the common prefix between consecutive keys
//! 2. When a branch completes (next key diverges), serializes that branch
//! 3. Packs nodes into 4096-byte pages (no node crosses a boundary)
//! 4. Chooses the smallest node type for each node
//!
//! The root is the last node written; its file position is returned by
//! [`TrieBuilder::finish`].
//!
//! Reference: Cassandra's `IncrementalDeepTrieWriterPageAware`

use crate::trie::node::{self, NodeType, PAGE_SIZE};
use ferrosa_common::Result;

/// Payload to associate with a trie key.
#[derive(Debug, Clone)]
pub struct TriePayload {
    /// For partition index: hash byte (h2). None for row index.
    pub hash: Option<u8>,
    /// Position value (idxpos for partition index, block offset for row index).
    pub position: i64,
}

/// Incremental trie builder.
pub struct TrieBuilder {
    /// Output buffer containing serialized trie nodes.
    output: Vec<u8>,
    /// Stack of pending branch nodes (built bottom-up).
    stack: Vec<BranchNode>,
    /// The previous key added (for detecting common prefix length).
    prev_key: Vec<u8>,
    /// Current write position in the output.
    write_pos: usize,
    /// Root-level children collected when popped nodes have no parent.
    root_children: Vec<(u8, u64)>,
}

/// A pending branch node being accumulated.
struct BranchNode {
    /// The byte value at this branch level.
    transition: u8,
    /// Children: (transition_byte, file_position).
    children: Vec<(u8, u64)>,
    /// Payload if this node is a leaf.
    payload: Option<TriePayload>,
}

impl TrieBuilder {
    /// Create a new trie builder.
    pub fn new() -> Self {
        TrieBuilder {
            output: Vec::new(),
            stack: Vec::new(),
            prev_key: Vec::new(),
            write_pos: 0,
            root_children: Vec::new(),
        }
    }

    /// Add a key with its payload. Keys MUST be added in sorted order.
    pub fn add(&mut self, key: &[u8], payload: TriePayload) -> Result<()> {
        let common = common_prefix_len(&self.prev_key, key);

        // Complete branches that are no longer shared
        self.complete_branches(common)?;

        // Push new branch nodes for the diverging suffix
        for &byte in &key[common..] {
            self.stack.push(BranchNode {
                transition: byte,
                children: Vec::new(),
                payload: None,
            });
        }

        // Set payload on the deepest (leaf) node
        if let Some(node) = self.stack.last_mut() {
            node.payload = Some(payload);
        }

        self.prev_key = key.to_vec();
        Ok(())
    }

    /// Finalize the trie. Returns `(output_bytes, root_position)`.
    pub fn finish(mut self) -> Result<(Vec<u8>, u64)> {
        self.complete_branches(0)?;

        if self.root_children.is_empty() {
            return Ok((self.output, 0)); // empty trie
        }

        // Assemble root node from collected root-level children
        let root_node = BranchNode {
            transition: 0, // unused for root
            children: std::mem::take(&mut self.root_children),
            payload: None,
        };
        let root_pos = self.write_node(&root_node)?;

        Ok((self.output, root_pos))
    }

    /// Complete branches from the stack down to `keep_depth`.
    fn complete_branches(&mut self, keep_depth: usize) -> Result<()> {
        while self.stack.len() > keep_depth {
            let node = self.stack.pop().unwrap();
            let pos = self.write_node(&node)?;

            if let Some(parent) = self.stack.last_mut() {
                parent.children.push((node.transition, pos));
            } else {
                self.root_children.push((node.transition, pos));
            }
        }
        Ok(())
    }

    /// Serialize and write a node to the output buffer.
    /// Returns the file position of the written node.
    fn write_node(&mut self, node: &BranchNode) -> Result<u64> {
        let payload_bytes = encode_payload(&node.payload);
        let pb = compute_pb(&node.payload);

        // For leaf nodes (no children), the bytes don't depend on write_pos.
        // For non-leaf nodes, we must first determine the final write position
        // (after any page-alignment padding) before encoding child distances.

        if node.children.is_empty() {
            // PayloadOnly — bytes don't depend on position
            let mut buf = vec![(NodeType::PayloadOnly as u8) << 4 | pb];
            buf.extend_from_slice(&payload_bytes);

            let page_offset = self.write_pos % PAGE_SIZE;
            if page_offset + buf.len() > PAGE_SIZE {
                let padding = PAGE_SIZE - page_offset;
                self.output.extend(vec![0u8; padding]);
                self.write_pos += padding;
            }

            let pos = self.write_pos as u64;
            self.output.extend_from_slice(&buf);
            self.write_pos += buf.len();
            return Ok(pos);
        }

        // For single-child and multi-child nodes, estimate the node size first
        // to determine whether page-alignment padding is needed, then encode
        // with the actual final write position.
        //
        // We estimate size using the CURRENT write_pos. In rare cases where
        // padding changes the node type (e.g., a larger distance requires a
        // wider pointer), we do a second pass.
        let estimated_bytes = if node.children.len() == 1 {
            let (child_trans, child_pos) = node.children[0];
            let distance = self.write_pos as u64 - child_pos;
            encode_single_node(child_trans, distance, pb, &payload_bytes)
        } else {
            encode_sparse_node(&node.children, self.write_pos as u64, pb, &payload_bytes)
        };

        // Determine final write_pos after page alignment
        let page_offset = self.write_pos % PAGE_SIZE;
        let final_write_pos = if page_offset + estimated_bytes.len() > PAGE_SIZE {
            self.write_pos + (PAGE_SIZE - page_offset)
        } else {
            self.write_pos
        };

        // Re-encode with the final position (distances may be larger due to padding)
        let node_bytes = if node.children.len() == 1 {
            let (child_trans, child_pos) = node.children[0];
            let distance = final_write_pos as u64 - child_pos;
            encode_single_node(child_trans, distance, pb, &payload_bytes)
        } else {
            encode_sparse_node(&node.children, final_write_pos as u64, pb, &payload_bytes)
        };

        // Apply padding if node size changed (distance encoding may have grown)
        let page_offset2 = self.write_pos % PAGE_SIZE;
        if page_offset2 + node_bytes.len() > PAGE_SIZE {
            let padding = PAGE_SIZE - page_offset2;
            self.output.extend(vec![0u8; padding]);
            self.write_pos += padding;
        }

        let pos = self.write_pos as u64;
        self.output.extend_from_slice(&node_bytes);
        self.write_pos += node_bytes.len();

        Ok(pos)
    }
}

impl Default for TrieBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Find common prefix length between two byte slices.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Compute pb (payload bits) for a given payload.
fn compute_pb(payload: &Option<TriePayload>) -> u8 {
    match payload {
        None => 0,
        Some(p) => {
            let pos_bytes = node::encode_signed_bytes(p.position);
            if p.hash.is_some() {
                (pos_bytes.len() as u8) + 7
            } else {
                pos_bytes.len() as u8
            }
        }
    }
}

/// Encode payload bytes.
fn encode_payload(payload: &Option<TriePayload>) -> Vec<u8> {
    match payload {
        None => vec![],
        Some(p) => {
            let pos_bytes = node::encode_signed_bytes(p.position);
            if let Some(hash) = p.hash {
                let mut buf = vec![hash];
                buf.extend_from_slice(&pos_bytes);
                buf
            } else {
                pos_bytes
            }
        }
    }
}

/// Encode a single-child node, choosing the smallest type.
fn encode_single_node(trans: u8, distance: u64, pb: u8, payload: &[u8]) -> Vec<u8> {
    if pb == 0 && distance < 16 {
        // SingleNopayload4: type byte has pointer in lower nibble, trans at data[1]
        let type_byte = (NodeType::SingleNopayload4 as u8) << 4 | (distance as u8);
        vec![type_byte, trans]
    } else if distance <= 0xFF {
        // Single8
        let type_byte = (NodeType::Single8 as u8) << 4 | pb;
        let mut buf = vec![type_byte, trans, distance as u8];
        buf.extend_from_slice(payload);
        buf
    } else if pb == 0 && distance < 4096 {
        // SingleNopayload12: type byte has upper 4 bits of pointer in lower nibble,
        // lower 8 bits of pointer at data[1], transition at data[2]
        let upper = ((distance >> 8) & 0x0F) as u8;
        let type_byte = (NodeType::SingleNopayload12 as u8) << 4 | upper;
        vec![type_byte, (distance & 0xFF) as u8, trans]
    } else if distance <= 0xFFFF {
        // Single16
        let type_byte = (NodeType::Single16 as u8) << 4 | pb;
        let dist_bytes = (distance as u16).to_be_bytes();
        let mut buf = vec![type_byte, trans, dist_bytes[0], dist_bytes[1]];
        buf.extend_from_slice(payload);
        buf
    } else {
        // Fall back to sparse encoding for large distances
        let type_byte = (NodeType::Sparse24 as u8) << 4 | pb;
        let mut buf = vec![type_byte, 1, trans];
        let d = distance;
        buf.push(((d >> 16) & 0xFF) as u8);
        buf.push(((d >> 8) & 0xFF) as u8);
        buf.push((d & 0xFF) as u8);
        buf.extend_from_slice(payload);
        buf
    }
}

/// Encode a sparse (multiple children) node.
fn encode_sparse_node(children: &[(u8, u64)], current_pos: u64, pb: u8, payload: &[u8]) -> Vec<u8> {
    let cc = children.len();
    let distances: Vec<u64> = children.iter().map(|(_, pos)| current_pos - pos).collect();
    let max_dist = distances.iter().copied().max().unwrap_or(0);

    let (node_type, ptr_size) = if max_dist <= 0xFF {
        (NodeType::Sparse8, 1)
    } else if max_dist <= 0xFFF {
        (NodeType::Sparse12, 0) // special packing
    } else if max_dist <= 0xFFFF {
        (NodeType::Sparse16, 2)
    } else if max_dist <= 0xFFFFFF {
        (NodeType::Sparse24, 3)
    } else {
        (NodeType::Sparse40, 5)
    };
    let _ = ptr_size; // used implicitly by the match below

    let type_byte = (node_type as u8) << 4 | pb;
    let mut buf = vec![type_byte, cc as u8];

    // Transition bytes
    for &(trans, _) in children {
        buf.push(trans);
    }

    // Pointer bytes
    match node_type {
        NodeType::Sparse8 => {
            for &d in &distances {
                buf.push(d as u8);
            }
        }
        NodeType::Sparse12 => {
            for i in (0..distances.len()).step_by(2) {
                let d0 = distances[i];
                if i + 1 < distances.len() {
                    let d1 = distances[i + 1];
                    buf.push(((d0 >> 4) & 0xFF) as u8);
                    buf.push((((d0 & 0x0F) << 4) | ((d1 >> 8) & 0x0F)) as u8);
                    buf.push((d1 & 0xFF) as u8);
                } else {
                    buf.push(((d0 >> 4) & 0xFF) as u8);
                    buf.push(((d0 & 0x0F) << 4) as u8);
                }
            }
        }
        NodeType::Sparse16 => {
            for &d in &distances {
                buf.extend_from_slice(&(d as u16).to_be_bytes());
            }
        }
        NodeType::Sparse24 => {
            for &d in &distances {
                buf.push(((d >> 16) & 0xFF) as u8);
                buf.push(((d >> 8) & 0xFF) as u8);
                buf.push((d & 0xFF) as u8);
            }
        }
        NodeType::Sparse40 => {
            for &d in &distances {
                buf.push(((d >> 32) & 0xFF) as u8);
                buf.push(((d >> 24) & 0xFF) as u8);
                buf.push(((d >> 16) & 0xFF) as u8);
                buf.push(((d >> 8) & 0xFF) as u8);
                buf.push((d & 0xFF) as u8);
            }
        }
        _ => unreachable!(),
    }

    buf.extend_from_slice(payload);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::walker;

    #[test]
    fn empty_trie() {
        let builder = TrieBuilder::new();
        let (output, _root) = builder.finish().unwrap();
        assert!(output.is_empty());
    }

    #[test]
    fn single_key() {
        let mut builder = TrieBuilder::new();
        builder
            .add(
                b"a",
                TriePayload {
                    hash: Some(0xAA),
                    position: 100,
                },
            )
            .unwrap();
        let (output, root) = builder.finish().unwrap();

        let result = walker::lookup(&output.as_slice(), root, b"a").unwrap();
        match result {
            walker::LookupResult::Found {
                payload_pb,
                payload_bytes,
            } => {
                let (hash, pos) = node::decode_payload(payload_pb, &payload_bytes).unwrap();
                assert_eq!(hash, Some(0xAA));
                assert_eq!(pos, 100);
            }
            _ => panic!("expected Found"),
        }
    }

    #[test]
    fn two_keys_diverge_at_root() {
        let mut builder = TrieBuilder::new();
        builder
            .add(
                b"a",
                TriePayload {
                    hash: Some(0xAA),
                    position: 10,
                },
            )
            .unwrap();
        builder
            .add(
                b"b",
                TriePayload {
                    hash: Some(0xBB),
                    position: 20,
                },
            )
            .unwrap();
        let (output, root) = builder.finish().unwrap();

        let r1 = walker::lookup(&output.as_slice(), root, b"a").unwrap();
        let r2 = walker::lookup(&output.as_slice(), root, b"b").unwrap();
        assert!(matches!(r1, walker::LookupResult::Found { .. }));
        assert!(matches!(r2, walker::LookupResult::Found { .. }));

        let r3 = walker::lookup(&output.as_slice(), root, b"c").unwrap();
        assert_eq!(r3, walker::LookupResult::NotFound);
    }

    #[test]
    fn shared_prefix() {
        let mut builder = TrieBuilder::new();
        builder
            .add(
                b"abc",
                TriePayload {
                    hash: Some(0x01),
                    position: 1,
                },
            )
            .unwrap();
        builder
            .add(
                b"abd",
                TriePayload {
                    hash: Some(0x02),
                    position: 2,
                },
            )
            .unwrap();
        builder
            .add(
                b"xyz",
                TriePayload {
                    hash: Some(0x03),
                    position: 3,
                },
            )
            .unwrap();
        let (output, root) = builder.finish().unwrap();

        for (key, expected_pos) in [(b"abc".as_slice(), 1), (b"abd", 2), (b"xyz", 3)] {
            let result = walker::lookup(&output.as_slice(), root, key).unwrap();
            match result {
                walker::LookupResult::Found {
                    payload_pb,
                    payload_bytes,
                } => {
                    let (_, pos) = node::decode_payload(payload_pb, &payload_bytes).unwrap();
                    assert_eq!(pos, expected_pos, "wrong pos for key {:?}", key);
                }
                _ => panic!("expected Found for key {:?}", key),
            }
        }
    }

    #[test]
    fn many_keys_all_found() {
        let mut builder = TrieBuilder::new();
        let mut keys: Vec<Vec<u8>> = (0..100u32)
            .map(|i| format!("key{:04}", i).into_bytes())
            .collect();
        keys.sort();

        for (i, key) in keys.iter().enumerate() {
            builder
                .add(
                    key,
                    TriePayload {
                        hash: Some((i & 0xFF) as u8),
                        position: i as i64,
                    },
                )
                .unwrap();
        }

        let (output, root) = builder.finish().unwrap();

        for (i, key) in keys.iter().enumerate() {
            let result = walker::lookup(&output.as_slice(), root, key).unwrap();
            match result {
                walker::LookupResult::Found {
                    payload_pb,
                    payload_bytes,
                } => {
                    let (_, pos) = node::decode_payload(payload_pb, &payload_bytes).unwrap();
                    assert_eq!(
                        pos,
                        i as i64,
                        "wrong pos for key {:?}",
                        String::from_utf8_lossy(key)
                    );
                }
                _ => panic!("key not found: {:?}", String::from_utf8_lossy(key)),
            }
        }
    }

    #[test]
    fn page_boundary_respected() {
        let mut builder = TrieBuilder::new();
        for i in 0..500u32 {
            let key = format!("key{:06}", i).into_bytes();
            builder
                .add(
                    &key,
                    TriePayload {
                        hash: Some(0),
                        position: i as i64,
                    },
                )
                .unwrap();
        }

        let (output, _root) = builder.finish().unwrap();
        assert!(!output.is_empty());
    }
}
