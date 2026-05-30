//! Compaction strategies for SSTables.
//!
//! - **STCS**: Size-Tiered — groups SSTables by similar size, merges when
//!   a bucket reaches `min_threshold`.
//! - **UCS**: Unified — density-based levels with configurable fan factor.
//!   Subsumes STCS (W=large), LCS (W=2), and TWCS behavior.

pub mod executor;
pub mod finalize;
pub mod metadata;
pub mod strategy;
pub mod strategy_ucs;

/// Compaction correctness validator (oracle + differential checks). Compiled
/// only for tests or when the `compaction-validator` feature is enabled.
#[cfg(any(test, feature = "compaction-validator"))]
pub mod validator;

pub use executor::{CompactionExecutor, CompactionResult};
pub use metadata::{CompactionTask, SSTableMetadata};
pub use strategy::{CompactionConfig, CompactionStrategy, SizeTieredStrategy};
pub use strategy_ucs::{UcsConfig, UnifiedCompactionStrategy};
