//! How the PostgreSQL front-end reaches the cluster's **range-read** path,
//! without depending on `ferrosa-cluster`'s coordinator type.
//!
//! ## Why this exists
//!
//! `storage_provider::produce_scan` used to answer every `SELECT` from this
//! node's LOCAL `TableStore` (`StorageEngine::range_iter`). That is only
//! correct while the query's keyspace is replicated to *every* node the query
//! can be served from: with a replication factor below the node count the local
//! node owns only some partitions, so a local scan returns a FRACTION of the
//! committed rows — silently, because a short result set is indistinguishable
//! from a small table. Even at RF == node count a node that is mid-join, or has
//! lost a partition, under-returns the same way.
//!
//! The read therefore has to consult the ring, exactly as the CQL read path
//! does (`WritePath::range_read_stream_all_with` at the client's consistency
//! level). This module is the seam: the binary owns the live `WritePath` (which
//! is created before the node has formed a cluster, and swapped atomically on
//! every mode transition), so the handle is a *factory* re-asked per statement —
//! the same reason [`crate::AccordAccess`]'s committer is a factory.
//!
//! ## Consistency level
//!
//! The PostgreSQL wire protocol has no consistency-level field, so the
//! front-end chooses one deliberately: [`DEFAULT_READ_CONSISTENCY`] is
//! `QUORUM`. Paired with a `QUORUM` write, `W + R > RF` holds — the standard
//! coherent pairing — so a read never misses a committed write that a quorum
//! acknowledges. It is also what makes a replication factor of *quorum* (rather
//! than the whole cluster) sufficient: see the crate README.
//!
//! ## Fail loud
//!
//! The coordinator refuses (with an error, never a warning) to serve a read
//! when it cannot reach the replicas the consistency level requires. That is
//! the property this seam must preserve: an unreachable owner produces a loud
//! failure, never a short result set that looks like the whole answer.

use std::pin::Pin;
use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::ring::strategy::ReplicationStrategy;
use ferrosa_cluster::write_path::WritePath;
use ferrosa_sstable::types::Partition;
use ferrosa_storage::TableId;
use futures::{Stream, StreamExt};

/// The consistency level the PostgreSQL front end reads at.
///
/// `QUORUM` reads paired with `QUORUM` writes give `W + R > RF`, so a read on
/// any node sees every committed write at that level. `ONE` would make a read
/// complete only while the local node happens to hold every partition it is
/// asked for — the defect this module exists to remove.
pub const DEFAULT_READ_CONSISTENCY: ConsistencyLevel = ConsistencyLevel::Quorum;

/// A stream of the partitions one full-table range read produces, with failures
/// carried inline (a stream cannot report a mid-read error any other way).
pub type PartitionStream = Pin<Box<dyn Stream<Item = Result<Partition, String>> + Send>>;

/// One full-table range read against the deployment's read path.
///
/// Implemented by [`ClusterRangeRead`] over the live [`WritePath`]. A failure
/// (a replica the consistency level needs is unreachable) is an `Err`, never an
/// empty or short stream.
#[async_trait::async_trait]
pub trait RangeRead: Send + Sync {
    /// Open the stream of every partition of `table_id`, reading at `cl` with
    /// the keyspace's `strategy`.
    async fn stream_all(
        &self,
        table_id: &TableId,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> Result<PartitionStream, String>;
}

/// [`RangeRead`] over the shared, atomically swappable [`WritePath`] the CQL
/// router also holds, so a PostgreSQL `SELECT` fans out across the ring exactly
/// as a CQL `SELECT` does.
pub struct ClusterRangeRead {
    path: Arc<ArcSwap<WritePath>>,
}

impl ClusterRangeRead {
    pub fn new(path: Arc<ArcSwap<WritePath>>) -> Self {
        Self { path }
    }
}

#[async_trait::async_trait]
impl RangeRead for ClusterRangeRead {
    async fn stream_all(
        &self,
        table_id: &TableId,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> Result<PartitionStream, String> {
        // Load the current write path and open a full (uncapped) scan at the
        // caller's consistency level. `0` is "no row cap": the stream is the
        // whole table, delivered under backpressure, never materialised.
        let path = self.path.load_full();
        let stream = path
            .range_read_stream_all_with(table_id, 0, cl, strategy)
            .await
            .map_err(|error| error.to_string())?;
        Ok(Box::pin(
            stream.map(|item| item.map_err(|error| error.to_string())),
        ))
    }
}

/// A range-read handle the node can offer *right now*, or `None` if it is not a
/// cluster (or has lost its write path).
type RangeReadFactory = dyn Fn() -> Option<Arc<dyn RangeRead>> + Send + Sync;

/// The PostgreSQL front-end's handle on the cluster range-read path.
///
/// Mirrors [`crate::AccordAccess`]: the front-end listener is bound before the
/// node has formed a cluster, so a value captured at construction would be
/// `None` forever. The factory re-answers on every statement.
#[derive(Clone, Default)]
pub struct ClusterRead {
    factory: Option<Arc<RangeReadFactory>>,
}

impl ClusterRead {
    /// The deployment has no cluster range-read path (standalone nodes and unit
    /// tests): the read falls back to this node's local storage, which IS the
    /// complete answer for a standalone node.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Production wiring: `factory` re-answers against the node's live state on
    /// every statement.
    pub fn live<F>(factory: F) -> Self
    where
        F: Fn() -> Option<Arc<dyn RangeRead>> + Send + Sync + 'static,
    {
        Self {
            factory: Some(Arc::new(factory)),
        }
    }

    /// A reader the node can offer for the statement about to run, or `None` if
    /// it is not a cluster (fall back to local storage).
    pub fn reader(&self) -> Option<Arc<dyn RangeRead>> {
        self.factory.as_ref().and_then(|factory| factory())
    }
}
