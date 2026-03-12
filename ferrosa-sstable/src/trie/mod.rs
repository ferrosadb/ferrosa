//! On-disk trie implementation for BTI SSTable indices.
//!
//! Both the partition index (Partitions.db) and row index (Rows.db) use the
//! same trie encoding. The trie maps byte sequences (key prefixes or
//! clustering separators) to payload values (file positions).
//!
//! # Node Types
//!
//! 16 node types (codes 0x0–0xF) encode transitions from a node to its
//! children. Each node begins with a single byte: 4 bits of type + 4 bits
//! of payload info. The builder chooses whichever encoding produces the
//! smallest representation.
//!
//! # Page Alignment
//!
//! Nodes are packed into 4096-byte pages. No node crosses a page boundary,
//! ensuring a single page fetch reads a complete node.
//!
//! # Pointers
//!
//! Child pointers are stored as distances (offsets from current node position
//! to child). Since tries are written bottom-up, children always precede
//! parents in the file.
//!
//! Reference: Cassandra's `BtiFormat.md`, `IncrementalTrieWriterPageAware`

pub mod builder;
pub mod node;
pub mod walker;
