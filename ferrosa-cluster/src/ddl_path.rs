//! DDL path abstraction for runtime mode transitions.
//!
//! Parallels `WritePath` — the CQL router calls `DdlPath::execute()`
//! for all DDL operations. Swapped atomically via `ArcSwap`.

use std::sync::Arc;

use ferrosa_schema::Schema;
use ferrosa_storage::engine::StorageEngine;

use crate::pair::ddl::DdlCoordinator;

/// The active DDL path. Swapped atomically via `ArcSwap` when
/// the deployment mode changes (standalone → pair → cluster).
pub enum DdlPath {
    /// Standalone: DDL applied directly to local schema + storage.
    Direct {
        schema: Arc<Schema>,
        engine: Arc<StorageEngine>,
    },
    /// Pair mode: DDL routed through DdlCoordinator (primary authority).
    Pair(Arc<DdlCoordinator>),
    /// Degraded: peer lost, DDL rejected until operator promotes.
    Unavailable,
}
