//! Cassandra-compatible BTI SSTable reader and writer.
//!
//! This crate reads and writes [BTI (Big Trie-Indexed)][bti] SSTables,
//! the default on-disk format in Apache Cassandra 5.x. It provides:
//!
//! - [`io::ReadAt`] / [`io::WriteAt`] — positional I/O traits decoupled from filesystem vs S3
//! - `SSTableReader` — open and query BTI SSTables (planned)
//! - `SSTableWriter` — write new BTI SSTables from sorted input (planned)
//!
//! # Architecture
//!
//! SSTables consist of 7 component files (Data.db, Partitions.db, Rows.db,
//! Filter.db, CompressionInfo.db, Statistics.db, TOC.txt). This crate handles
//! all components through composable internal modules, with the public API
//! exposed via `SSTableReader` and `SSTableWriter`.
//!
//! All I/O is synchronous via the [`io::ReadAt`]/[`io::WriteAt`] traits. Async
//! wrappers (for S3) live in `ferrosa-storage`.
//!
//! [bti]: https://cassandra.apache.org/doc/latest/cassandra/architecture/storage-engine.html

pub mod bloom;
pub mod compression;
pub mod io;
pub mod types;
pub mod varint;
