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

use ferrosa_common::{Error, Result};
use smallvec::SmallVec;

use crate::trie::node::{encode_signed_bytes, NodeType, PAGE_SIZE};

/// Payload attached to a leaf in the trie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriePayload {
    /// Optional hash byte (partition index uses this).
    pub hash: Option<u8>,
    /// Position value (file offset, block offset, etc.).
    pub position: i64,
}

/// An in-progress branch node on the builder stack.
///
/// Stack index 0 is the implicit root (transition byte is unused).
/// Stack indices 1..N correspond to the bytes of the current key path.
#[derive(Debug, Clone)]
struct BranchNode {
    /// The transition byte from the parent to this node.
    /// For the root (stack index 0), this is unused.
    transition: u8,
    /// Children: (transition_byte, absolute_position_in_output).
    children: SmallVec<[(u8, u64); 16]>,
    /// Payload if this node is a leaf (or an internal node with a payload).
    payload: Option<TriePayload>,
}

/// Incremental trie builder that produces a page-aligned byte buffer.
///
/// Keys must be added in sorted (lexicographic) order. The builder
/// maintains a stack of branch nodes and serializes completed branches
/// as new keys are added.
pub struct TrieBuilder {
    /// The output buffer, written bottom-up.
    output: Vec<u8>,
    /// Stack of in-progress branch nodes.
    /// Index 0 is always the implicit root.
    /// Indices 1..N correspond to key byte depths.
    stack: Vec<BranchNode>,
    /// The previous key, used to detect the branch point.
    prev_key: Vec<u8>,
    /// Current write position (= output.len()).
    write_pos: usize,
    /// Position of the last node written (becomes root after finish).
    last_node_pos: u64,
    /// Whether any key has been added yet.
    has_keys: bool,
    /// Reused node, payload, and pointer workspaces. Trie node encoding is on
    /// the per-key hot path, so its temporary bytes must not allocate per node.
    encoded_node: Vec<u8>,
    payload_scratch: Vec<u8>,
    distance_scratch: Vec<u64>,
    dense_distance_scratch: Vec<u64>,
}

impl TrieBuilder {
    /// Create a new, empty trie builder.
    pub fn new() -> Self {
        Self {
            output: Vec::new(),
            stack: Vec::new(),
            prev_key: Vec::new(),
            write_pos: 0,
            last_node_pos: 0,
            has_keys: false,
            encoded_node: Vec::new(),
            payload_scratch: Vec::new(),
            distance_scratch: Vec::new(),
            dense_distance_scratch: Vec::new(),
        }
    }

    /// Add a key with its payload. Keys **must** be added in sorted order.
    pub fn add(&mut self, key: &[u8], payload: TriePayload) -> Result<()> {
        // Verify sorted order.
        if self.has_keys && key <= self.prev_key.as_slice() {
            return Err(Error::InvalidData(format!(
                "keys must be added in sorted order: {:?} <= {:?}",
                key, self.prev_key
            )));
        }

        let prefix_len = if self.has_keys {
            common_prefix_len(&self.prev_key, key)
        } else {
            0
        };

        if !self.has_keys {
            // First key: push the implicit root node.
            self.stack.push(BranchNode {
                transition: 0, // unused for root
                children: SmallVec::new(),
                payload: None,
            });
        } else {
            // Complete branches that are no longer shared.
            // Stack depth for prefix_len bytes of shared prefix is prefix_len + 1
            // (root at 0, then one node per byte). We want to keep
            // prefix_len + 1 nodes (the root + shared prefix bytes).
            self.complete_branches(prefix_len + 1)?;
        }

        // Extend the stack for each new byte in the key beyond the common prefix.
        for &b in &key[prefix_len..] {
            self.stack.push(BranchNode {
                transition: b,
                children: SmallVec::new(),
                payload: None,
            });
        }

        // Set the payload on the deepest (leaf) node.
        // For an empty key, the root itself gets the payload.
        if let Some(leaf) = self.stack.last_mut() {
            leaf.payload = Some(payload);
        }

        self.prev_key.clear();
        self.prev_key.extend_from_slice(key);
        self.has_keys = true;
        Ok(())
    }

    /// Finish the trie, returning `(output_bytes, root_position)`.
    ///
    /// Returns an empty output with root_position 0 if no keys were added.
    pub fn finish(mut self) -> Result<(Vec<u8>, u64)> {
        if !self.has_keys {
            return Ok((Vec::new(), 0));
        }

        // Complete all remaining branches including the root (depth 0).
        self.complete_branches(0)?;

        Ok((self.output, self.last_node_pos))
    }

    /// Serialize completed branches from the stack.
    ///
    /// Keeps `keep_depth` nodes on the stack, serializing everything deeper.
    /// Each serialized node's position is recorded as a child of its parent.
    fn complete_branches(&mut self, keep_depth: usize) -> Result<()> {
        while self.stack.len() > keep_depth {
            let node = self.stack.pop().unwrap();
            let pos = self.write_node(&node)?;

            if let Some(parent) = self.stack.last_mut() {
                parent.children.push((node.transition, pos));
            } else {
                // This was the root node; record its position.
                self.last_node_pos = pos;
            }
        }
        Ok(())
    }

    /// Serialize a single node and write it to the output buffer.
    ///
    /// Returns the absolute position of the written node in the output.
    fn write_node(&mut self, node: &BranchNode) -> Result<u64> {
        encode_node_into(
            node,
            self.write_pos as u64,
            &mut self.encoded_node,
            &mut self.payload_scratch,
            &mut self.distance_scratch,
            &mut self.dense_distance_scratch,
        )?;
        let encoded_len = self.encoded_node.len();

        // Page alignment: if the encoded node would cross a page boundary, pad.
        let current_page_offset = self.write_pos % PAGE_SIZE;
        if current_page_offset != 0 && current_page_offset + encoded_len > PAGE_SIZE {
            let pad = PAGE_SIZE - current_page_offset;
            self.output.resize(self.output.len() + pad, 0);
            self.write_pos += pad;

            // Re-encode with the new position (distances may have changed).
            encode_node_into(
                node,
                self.write_pos as u64,
                &mut self.encoded_node,
                &mut self.payload_scratch,
                &mut self.distance_scratch,
                &mut self.dense_distance_scratch,
            )?;
            let pos = self.write_pos as u64;
            self.output.extend_from_slice(&self.encoded_node);
            self.write_pos += self.encoded_node.len();
            return Ok(pos);
        }

        let pos = self.write_pos as u64;
        self.output.extend_from_slice(&self.encoded_node);
        self.write_pos += self.encoded_node.len();

        Ok(pos)
    }
}

impl Default for TrieBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Length of the common prefix between two byte slices.
pub fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Compute the payload bits (pb) nibble for a payload.
///
/// `pb = 0` means "no payload" in the trie format, so we must ensure
/// that any present payload produces `pb >= 1`. When `position == 0`
/// and there is no hash, we use `pb = 1` (one zero byte).
fn compute_pb(payload: &TriePayload) -> u8 {
    let idx_len = encode_signed_bytes(payload.position).len().max(1);
    match payload.hash {
        None => idx_len as u8,
        Some(_) => (7 + idx_len) as u8,
    }
}

/// Encode a node into reusable workspaces, avoiding a temporary Vec per node.
fn encode_node_into(
    node: &BranchNode,
    current_pos: u64,
    out: &mut Vec<u8>,
    payload_bytes: &mut Vec<u8>,
    distances: &mut Vec<u64>,
    dense_distances: &mut Vec<u64>,
) -> Result<()> {
    out.clear();
    payload_bytes.clear();
    let pb = if let Some(payload) = &node.payload {
        encode_payload_into(payload, payload_bytes);
        compute_pb(payload)
    } else {
        0
    };

    match node.children.len() {
        0 => {
            out.push((NodeType::PayloadOnly as u8) << 4 | (pb & 0x0F));
            out.extend_from_slice(payload_bytes);
        }
        1 => encode_single_node_into(
            node.children[0].0,
            current_pos,
            node.children[0].1,
            pb,
            payload_bytes,
            out,
            &node.children,
            distances,
            dense_distances,
        )?,
        _ => encode_sparse_node_into(
            &node.children,
            current_pos,
            pb,
            payload_bytes,
            out,
            distances,
            dense_distances,
        )?,
    }
    Ok(())
}

fn encode_payload_into(payload: &TriePayload, out: &mut Vec<u8>) {
    out.clear();
    let value = payload.position;
    let num_bytes = if value == 0 {
        1
    } else {
        let abs = if value < 0 { !value } else { value };
        let lz = abs.leading_zeros();
        let mut bytes = (64 - lz + 1).div_ceil(8) as usize;
        let top_byte = (value >> ((bytes - 1) * 8)) as u8;
        if (top_byte & 0x80 != 0) != (value < 0) {
            bytes += 1;
        }
        bytes
    };
    if let Some(hash) = payload.hash {
        out.push(hash);
    }
    for i in (0..num_bytes).rev() {
        out.push((value >> (i * 8)) as u8);
    }
}

#[allow(clippy::too_many_arguments)]
fn encode_single_node_into(
    trans: u8,
    current_pos: u64,
    child_pos: u64,
    pb: u8,
    payload_bytes: &[u8],
    out: &mut Vec<u8>,
    children: &[(u8, u64)],
    distances: &mut Vec<u64>,
    dense_distances: &mut Vec<u64>,
) -> Result<()> {
    let distance = current_pos - child_pos;
    if pb == 0 && distance <= 0x0F {
        out.extend_from_slice(&[
            ((NodeType::SingleNopayload4 as u8) << 4) | (distance as u8 & 0x0F),
            trans,
        ]);
    } else if distance <= 0xFF {
        out.extend_from_slice(&[
            ((NodeType::Single8 as u8) << 4) | (pb & 0x0F),
            trans,
            distance as u8,
        ]);
        out.extend_from_slice(payload_bytes);
    } else if pb == 0 && distance <= 0xFFF {
        let ptr_hi = ((distance >> 8) & 0x0F) as u8;
        out.extend_from_slice(&[
            ((NodeType::SingleNopayload12 as u8) << 4) | ptr_hi,
            distance as u8,
            trans,
        ]);
    } else if distance <= 0xFFFF {
        out.extend_from_slice(&[
            ((NodeType::Single16 as u8) << 4) | (pb & 0x0F),
            trans,
            (distance >> 8) as u8,
            distance as u8,
        ]);
        out.extend_from_slice(payload_bytes);
    } else {
        encode_sparse_node_into(
            children,
            current_pos,
            pb,
            payload_bytes,
            out,
            distances,
            dense_distances,
        )?;
    }
    Ok(())
}

fn encode_sparse_node_into(
    children: &[(u8, u64)],
    current_pos: u64,
    pb: u8,
    payload_bytes: &[u8],
    out: &mut Vec<u8>,
    distances: &mut Vec<u64>,
    dense_distances: &mut Vec<u64>,
) -> Result<()> {
    distances.clear();
    distances.extend(
        children
            .iter()
            .map(|&(_, child_pos)| current_pos - child_pos),
    );
    if children.len() > u8::MAX as usize {
        return encode_dense_node_into(
            children,
            distances,
            pb,
            payload_bytes,
            out,
            dense_distances,
        );
    }
    let max_distance = distances.iter().copied().max().unwrap_or(0);
    let (node_type, bytes_per_ptr) = if max_distance <= 0xFF {
        (NodeType::Sparse8, 1)
    } else if max_distance <= 0xFFF {
        (NodeType::Sparse12, 0)
    } else if max_distance <= 0xFFFF {
        (NodeType::Sparse16, 2)
    } else if max_distance <= 0xFF_FFFF {
        (NodeType::Sparse24, 3)
    } else if max_distance <= 0xFF_FFFF_FFFF {
        (NodeType::Sparse40, 5)
    } else {
        return Err(Error::InvalidData(format!(
            "child distance {max_distance} too large for any sparse type"
        )));
    };
    out.push((node_type as u8) << 4 | (pb & 0x0F));
    out.push(children.len() as u8);
    out.extend(children.iter().map(|&(transition, _)| transition));
    if node_type == NodeType::Sparse12 {
        write_12bit_pointers(out, distances);
    } else {
        for &distance in distances.iter() {
            write_be_unsigned(out, distance, bytes_per_ptr);
        }
    }
    out.extend_from_slice(payload_bytes);
    Ok(())
}

fn encode_dense_node_into(
    children: &[(u8, u64)],
    distances: &[u64],
    pb: u8,
    payload_bytes: &[u8],
    out: &mut Vec<u8>,
    dense_distances: &mut Vec<u64>,
) -> Result<()> {
    let min_transition = children.iter().map(|(transition, _)| *transition).min();
    let max_transition = children.iter().map(|(transition, _)| *transition).max();
    let (Some(min_transition), Some(max_transition)) = (min_transition, max_transition) else {
        return Err(Error::InvalidData(
            "cannot encode dense trie node with no children".to_string(),
        ));
    };
    let span = (max_transition as usize) - (min_transition as usize) + 1;
    dense_distances.clear();
    dense_distances.resize(span, 0);
    for ((transition, _), distance) in children.iter().zip(distances.iter().copied()) {
        dense_distances[(*transition as usize) - (min_transition as usize)] = distance;
    }
    let max_distance = dense_distances.iter().copied().max().unwrap_or(0);
    let (node_type, bytes_per_ptr) = if max_distance <= 0xFFF {
        (NodeType::Dense12, 0)
    } else if max_distance <= 0xFFFF {
        (NodeType::Dense16, 2)
    } else if max_distance <= 0xFF_FFFF {
        (NodeType::Dense24, 3)
    } else if max_distance <= 0xFFFF_FFFF {
        (NodeType::Dense32, 4)
    } else if max_distance <= 0xFF_FFFF_FFFF {
        (NodeType::Dense40, 5)
    } else {
        (NodeType::LongDense, 8)
    };
    out.extend_from_slice(&[
        (node_type as u8) << 4 | (pb & 0x0F),
        min_transition,
        (span - 1) as u8,
    ]);
    if node_type == NodeType::Dense12 {
        write_12bit_pointers(out, dense_distances);
    } else {
        for &distance in dense_distances.iter() {
            write_be_unsigned(out, distance, bytes_per_ptr);
        }
    }
    out.extend_from_slice(payload_bytes);
    Ok(())
}

/// Write a sequence of 12-bit values packed into bytes.
///
/// Two 12-bit values are packed into 3 bytes: `[hi0, lo0|hi1, lo1]`.
fn write_12bit_pointers(out: &mut Vec<u8>, values: &[u64]) {
    let mut i = 0;
    while i + 1 < values.len() {
        let a = values[i] as u16;
        let b = values[i + 1] as u16;
        out.push((a >> 4) as u8);
        out.push(((a << 4) | (b >> 8)) as u8);
        out.push(b as u8);
        i += 2;
    }
    if i < values.len() {
        // Odd value: write as upper 12 bits of 2 bytes.
        let a = values[i] as u16;
        out.push((a >> 4) as u8);
        out.push((a << 4) as u8);
    }
}

/// Write an unsigned big-endian integer of `n` bytes.
fn write_be_unsigned(out: &mut Vec<u8>, val: u64, n: usize) {
    for i in (0..n).rev() {
        out.push((val >> (i * 8)) as u8);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::walker::lookup_payload;

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
                b"hello",
                TriePayload {
                    hash: None,
                    position: 42,
                },
            )
            .unwrap();
        let (output, root) = builder.finish().unwrap();

        let result = lookup_payload(&output, root, b"hello").unwrap();
        assert_eq!(result, Some((None, 42)));
    }

    #[test]
    fn two_keys_diverge_at_root() {
        let mut builder = TrieBuilder::new();
        builder
            .add(
                b"a",
                TriePayload {
                    hash: None,
                    position: 10,
                },
            )
            .unwrap();
        builder
            .add(
                b"b",
                TriePayload {
                    hash: None,
                    position: 20,
                },
            )
            .unwrap();
        let (output, root) = builder.finish().unwrap();

        assert_eq!(
            lookup_payload(&output, root, b"a").unwrap(),
            Some((None, 10))
        );
        assert_eq!(
            lookup_payload(&output, root, b"b").unwrap(),
            Some((None, 20))
        );
        assert_eq!(lookup_payload(&output, root, b"c").unwrap(), None);
    }

    #[test]
    fn shared_prefix() {
        let mut builder = TrieBuilder::new();
        builder
            .add(
                b"abc",
                TriePayload {
                    hash: None,
                    position: 100,
                },
            )
            .unwrap();
        builder
            .add(
                b"abd",
                TriePayload {
                    hash: None,
                    position: 200,
                },
            )
            .unwrap();
        builder
            .add(
                b"xyz",
                TriePayload {
                    hash: None,
                    position: 300,
                },
            )
            .unwrap();
        let (output, root) = builder.finish().unwrap();

        assert_eq!(
            lookup_payload(&output, root, b"abc").unwrap(),
            Some((None, 100))
        );
        assert_eq!(
            lookup_payload(&output, root, b"abd").unwrap(),
            Some((None, 200))
        );
        assert_eq!(
            lookup_payload(&output, root, b"xyz").unwrap(),
            Some((None, 300))
        );
        assert_eq!(lookup_payload(&output, root, b"ab").unwrap(), None);
        assert_eq!(lookup_payload(&output, root, b"xyx").unwrap(), None);
    }

    #[test]
    fn many_keys_all_found() {
        let mut builder = TrieBuilder::new();
        let keys: Vec<Vec<u8>> = (0..100u32)
            .map(|i| format!("key_{i:04}").into_bytes())
            .collect();

        for (i, key) in keys.iter().enumerate() {
            builder
                .add(
                    key,
                    TriePayload {
                        hash: None,
                        position: i as i64 * 10,
                    },
                )
                .unwrap();
        }

        let (output, root) = builder.finish().unwrap();

        for (i, key) in keys.iter().enumerate() {
            let result = lookup_payload(&output, root, key).unwrap();
            assert_eq!(
                result,
                Some((None, i as i64 * 10)),
                "failed for key {:?}",
                String::from_utf8_lossy(key)
            );
        }

        // Non-existent key.
        assert_eq!(lookup_payload(&output, root, b"key_9999").unwrap(), None);
    }

    #[test]
    fn page_boundary_respected() {
        let mut builder = TrieBuilder::new();
        for i in 0..500u32 {
            builder
                .add(
                    &format!("key_{i:06}").into_bytes(),
                    TriePayload {
                        hash: Some((i & 0xFF) as u8),
                        position: i as i64 * 100,
                    },
                )
                .unwrap();
        }
        let (output, root) = builder.finish().unwrap();
        assert!(!output.is_empty());

        // Verify a sample of keys are findable.
        for i in [0u32, 1, 50, 250, 499] {
            let key = format!("key_{i:06}").into_bytes();
            let result = lookup_payload(&output, root, &key).unwrap();
            assert_eq!(
                result,
                Some((Some((i & 0xFF) as u8), i as i64 * 100)),
                "failed for key {:?}",
                String::from_utf8_lossy(&key)
            );
        }
    }

    #[test]
    fn payload_with_hash() {
        let mut builder = TrieBuilder::new();
        builder
            .add(
                b"test",
                TriePayload {
                    hash: Some(0xAB),
                    position: 12345,
                },
            )
            .unwrap();
        let (output, root) = builder.finish().unwrap();

        let result = lookup_payload(&output, root, b"test").unwrap();
        assert_eq!(result, Some((Some(0xAB), 12345)));
    }

    #[test]
    fn unsorted_keys_error() {
        let mut builder = TrieBuilder::new();
        builder
            .add(
                b"b",
                TriePayload {
                    hash: None,
                    position: 1,
                },
            )
            .unwrap();
        let err = builder
            .add(
                b"a",
                TriePayload {
                    hash: None,
                    position: 2,
                },
            )
            .unwrap_err();
        assert!(err.to_string().contains("sorted order"));
    }
}
