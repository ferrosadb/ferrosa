//! Compaction strategies for SSTables.
//!
//! - **STCS**: Size-Tiered — groups SSTables by similar size, merges when
//!   a bucket reaches `min_threshold`.
//! - **UCS**: Unified — density-based levels with configurable fan factor.
//!   Subsumes STCS (W=large), LCS (W=2), and TWCS behavior.

pub mod executor;
pub mod metadata;
pub mod strategy;
pub mod strategy_ucs;

pub use executor::{CompactionExecutor, CompactionResult};
pub use metadata::{CompactionTask, SSTableMetadata};
pub use strategy::{CompactionConfig, CompactionStrategy, SizeTieredStrategy};
pub use strategy_ucs::UnifiedCompactionStrategy;
