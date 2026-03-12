//! Size-Tiered Compaction Strategy (STCS) for SSTables.
//!
//! Groups SSTables into buckets by similar size. When a bucket reaches
//! `min_threshold`, the SSTables in that bucket are merged into a single
//! output. The `CompactionStrategy` trait allows future strategies (LCS, TWCS).

pub mod executor;
pub mod metadata;
pub mod strategy;

pub use executor::{CompactionExecutor, CompactionResult};
pub use metadata::{CompactionTask, SSTableMetadata};
pub use strategy::{CompactionConfig, CompactionStrategy, SizeTieredStrategy};
