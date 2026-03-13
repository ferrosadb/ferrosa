//! Cassandra-compatible BTI SSTable reader and writer.
//!
//! This crate reads and writes [BTI (Big Trie-Indexed)][bti] SSTables,
//! the default on-disk format in Apache Cassandra 5.x. It provides:
//!
//! - [`io::ReadAt`] / [`io::WriteAt`] — positional I/O traits decoupled from filesystem vs S3
//! - [`SSTableReader`] — open and query BTI SSTables
//! - [`SSTableWriter`] — write new BTI SSTables from sorted input
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
pub mod byte_comparable;
pub mod compression;
pub mod data;
pub mod io;
pub mod marshal;
pub mod partition_index;
pub mod reader;
pub mod row_index;
pub mod statistics;
pub mod toc;
pub mod trie;
pub mod types;
pub mod varint;
pub mod writer;

pub use compression::Compression;
pub use io::{FileReadAt, FileWriteAt, ReadAt, WriteAt};
pub use reader::{SSTableComponents, SSTableReader};
pub use types::{DeletionTime, LivenessInfo, Partition, Row};
pub use writer::{SSTableOutput, SSTableWriter, WriteOptions};
