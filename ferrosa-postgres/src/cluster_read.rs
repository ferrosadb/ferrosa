//! Correctness: point reads preserve SQL equality, snapshot visibility and cluster consistency.
//! Last revised: 2026-10-24
//! Last changed: Route safe scalar key equalities through exact partition reads and MVCC lookup.
//!
//! How the PostgreSQL front-end reaches the cluster's keyed and range-read paths,
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
//! The level is configurable per node through
//! [`READ_CONSISTENCY_ENV`] (`FERROSA_PG_READ_CONSISTENCY`), resolved by
//! [`read_consistency`]. The default is unchanged; the knob exists because
//! `QUORUM` is answered by *some* quorum of the replicas and therefore says
//! nothing about whether **this** node holds a row — when the replication
//! factor spans the ring, `ONE` is the level at which the coordinator is served
//! from its own replicas alone, which is the property a per-replica durability
//! check has to measure. See [`read_consistency_from`].
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
///
/// This is the DEFAULT, not the only choice: [`read_consistency`] resolves
/// `FERROSA_PG_READ_CONSISTENCY` over it.
pub const DEFAULT_READ_CONSISTENCY: ConsistencyLevel = ConsistencyLevel::Quorum;

/// The environment variable that selects the front end's read consistency
/// level for this node (`ONE`, `LOCAL_ONE`, `QUORUM`, `ALL`, ...; any name
/// [`ConsistencyLevel::from_str`] accepts, case-insensitively).
pub const READ_CONSISTENCY_ENV: &str = "FERROSA_PG_READ_CONSISTENCY";

/// Resolve the configured read consistency level from a raw setting.
///
/// * `None`, an unset variable, or a blank one — [`DEFAULT_READ_CONSISTENCY`].
/// * A level name — that level.
/// * Anything else — an `Err` naming the value and the accepted set. It is
///   deliberately NOT a fallback: silently reading at a different level than
///   the operator asked for is the class of defect this module exists to
///   remove, so a misconfiguration fails loud on every scan instead.
pub fn read_consistency_from(value: Option<&str>) -> Result<ConsistencyLevel, String> {
    let raw = match value {
        // `FOO= cmd` and `unset FOO` are the same intent; a blank value is
        // "unset", not a level.
        None => return Ok(DEFAULT_READ_CONSISTENCY),
        Some(raw) if raw.trim().is_empty() => return Ok(DEFAULT_READ_CONSISTENCY),
        Some(raw) => raw,
    };
    ConsistencyLevel::from_str(raw.trim()).ok_or_else(|| {
        format!(
            "{READ_CONSISTENCY_ENV}={raw:?} is not a consistency level; expected one of ONE, \
             TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL"
        )
    })
}

/// [`read_consistency_from`] over this process's environment, resolved once.
///
/// Cached so the per-scan path never re-reads the environment; the environment
/// cannot change mid-process in any way that should affect a read level.
pub fn read_consistency() -> Result<ConsistencyLevel, String> {
    static RESOLVED: std::sync::OnceLock<Result<ConsistencyLevel, String>> =
        std::sync::OnceLock::new();
    RESOLVED
        .get_or_init(|| read_consistency_from(std::env::var(READ_CONSISTENCY_ENV).ok().as_deref()))
        .clone()
}

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
    /// Read exactly one partition at the same consistency and replication policy
    /// as a scan. Implementations without a keyed operation retain the complete
    /// cluster scan; they must never substitute local storage on a read failure.
    async fn stream_key(
        &self,
        table_id: &TableId,
        key: &ferrosa_common::DecoratedKey,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> Result<PartitionStream, String> {
        let stream = self.stream_all(table_id, cl, strategy).await?;
        let key = key.clone();
        Ok(Box::pin(stream.filter_map(move |item| {
            let keep = match &item {
                Ok(partition) => partition.key == key,
                Err(_) => true,
            };
            futures::future::ready(keep.then_some(item))
        })))
    }

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
    async fn stream_key(
        &self,
        table_id: &TableId,
        key: &ferrosa_common::DecoratedKey,
        cl: ConsistencyLevel,
        strategy: &ReplicationStrategy,
    ) -> Result<PartitionStream, String> {
        let path = self.path.load_full();
        let partition = path
            .pk_read(table_id, key, cl, strategy)
            .await
            .map_err(|error| error.to_string())?;
        Ok(Box::pin(futures::stream::iter(partition.map(Ok))))
    }

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

/// Regression guards for the read-level resolver.
///
/// These were written AFTER `read_consistency_from` (so they are a guard, not
/// the red that produced it): the function is instrumentation — the only way a
/// test outside the process can ask for a read served from one node's own
/// replicas. The guard worth having is the FAIL-LOUD one: a mis-set level must
/// never quietly become a different level.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_setting_is_the_quorum_default() {
        assert_eq!(
            read_consistency_from(None).expect("unset must resolve"),
            DEFAULT_READ_CONSISTENCY
        );
        assert_eq!(DEFAULT_READ_CONSISTENCY, ConsistencyLevel::Quorum);
    }

    /// `FOO= cmd` leaves the variable present but empty; that is "unset", and
    /// must not be an error and must not be a level.
    #[test]
    fn blank_setting_is_unset_and_not_an_error() {
        assert_eq!(
            read_consistency_from(Some("")).unwrap(),
            ConsistencyLevel::Quorum
        );
        assert_eq!(
            read_consistency_from(Some("   ")).unwrap(),
            ConsistencyLevel::Quorum
        );
    }

    #[test]
    fn an_explicit_level_is_honoured_case_insensitively() {
        assert_eq!(
            read_consistency_from(Some("one")).unwrap(),
            ConsistencyLevel::One
        );
        assert_eq!(
            read_consistency_from(Some("ONE")).unwrap(),
            ConsistencyLevel::One
        );
        assert_eq!(
            read_consistency_from(Some("  local_one  ")).unwrap(),
            ConsistencyLevel::LocalOne
        );
        assert_eq!(
            read_consistency_from(Some("quorum")).unwrap(),
            ConsistencyLevel::Quorum
        );
        assert_eq!(
            read_consistency_from(Some("all")).unwrap(),
            ConsistencyLevel::All
        );
    }

    /// A value that is not a level must not silently become one. This is the
    /// guard that bites: it asserts the refusal names the offending value and
    /// the accepted set, so an operator can fix it from the log alone.
    #[test]
    fn a_bogus_level_fails_loud_and_names_the_value() {
        let error = read_consistency_from(Some("nearest"))
            .expect_err("a bogus level must be refused, never silently replaced");
        assert!(
            error.contains("nearest"),
            "the refusal must quote the value: {error}"
        );
        assert!(
            error.contains(READ_CONSISTENCY_ENV),
            "the refusal must name the variable to fix: {error}"
        );
        assert!(
            error.contains("LOCAL_QUORUM"),
            "the refusal must list the accepted set: {error}"
        );
    }
}
