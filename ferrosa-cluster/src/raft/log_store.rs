//! Sled-backed Raft log storage.
//!
//! [`SledLogStore`] persists Raft log entries and vote metadata using
//! [sled](https://docs.rs/sled) — an embedded, ordered key-value store.
//!
//! Two named trees are used:
//!
//! - **`log`** — log entries keyed by big-endian `u64` index so that sled's
//!   ordered iteration yields entries in index order.
//! - **`meta`** — small metadata values: `vote`, `committed`, and
//!   `last_purged`.

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;

use openraft::storage::LogFlushed;
use openraft::{
    AnyError, Entry, LogId, LogState, RaftLogReader, StorageError, StorageIOError, Vote,
};

use super::FerrosRaftConfig;

// ---------------------------------------------------------------------------
// Meta-tree keys
// ---------------------------------------------------------------------------

const META_VOTE: &[u8] = b"vote";
const META_COMMITTED: &[u8] = b"committed";
const META_LAST_PURGED: &[u8] = b"last_purged";

// ---------------------------------------------------------------------------
// SledLogStore
// ---------------------------------------------------------------------------

/// Sled-backed implementation of openraft's `RaftLogStorage` and
/// `RaftLogReader` traits.
pub struct SledLogStore {
    #[allow(dead_code)]
    db: sled::Db,
    /// Tree for log entries: key = big-endian u64 index, value = bincode(Entry).
    log: sled::Tree,
    /// Tree for metadata: vote, committed, last_purged.
    meta: sled::Tree,
}

#[allow(clippy::result_large_err)] // StorageIOError is 224 bytes — dictated by openraft
impl SledLogStore {
    /// Open (or create) a log store at the given filesystem `path`.
    pub fn new(path: &Path) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        let log = db.open_tree("log")?;
        let meta = db.open_tree("meta")?;
        Ok(Self { db, log, meta })
    }

    // -- helpers ----------------------------------------------------------

    fn index_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    fn serialize_entry(entry: &Entry<FerrosRaftConfig>) -> Result<Vec<u8>, StorageIOError<u64>> {
        bincode::serialize(entry).map_err(|e| StorageIOError::write_logs(to_any_error(e)))
    }

    fn deserialize_entry(bytes: &[u8]) -> Result<Entry<FerrosRaftConfig>, StorageIOError<u64>> {
        bincode::deserialize(bytes).map_err(|e| StorageIOError::read_logs(to_any_error(e)))
    }

    fn save_meta<T: serde::Serialize>(
        meta: &sled::Tree,
        key: &[u8],
        value: &T,
    ) -> Result<(), StorageIOError<u64>> {
        let bytes =
            bincode::serialize(value).map_err(|e| StorageIOError::write(to_any_error(e)))?;
        meta.insert(key, bytes)
            .map_err(|e| StorageIOError::write(to_any_error(e)))?;
        Ok(())
    }

    fn load_meta<T: serde::de::DeserializeOwned>(
        meta: &sled::Tree,
        key: &[u8],
    ) -> Result<Option<T>, StorageIOError<u64>> {
        match meta
            .get(key)
            .map_err(|e| StorageIOError::read(to_any_error(e)))?
        {
            Some(bytes) => {
                let val = bincode::deserialize(&bytes)
                    .map_err(|e| StorageIOError::read(to_any_error(e)))?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Return the last entry currently present in the log tree.
    fn last_entry_log_id(&self) -> Result<Option<LogId<u64>>, StorageIOError<u64>> {
        let last = self
            .log
            .last()
            .map_err(|e| StorageIOError::read_logs(to_any_error(e)))?;
        match last {
            Some((_k, v)) => {
                let entry = Self::deserialize_entry(&v)?;
                Ok(Some(entry.log_id))
            }
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// RaftLogReader
// ---------------------------------------------------------------------------

impl RaftLogReader<FerrosRaftConfig> for SledLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<FerrosRaftConfig>>, StorageError<u64>> {
        let start_bytes = match range.start_bound() {
            std::ops::Bound::Included(&idx) => std::ops::Bound::Included(Self::index_key(idx)),
            std::ops::Bound::Excluded(&idx) => std::ops::Bound::Excluded(Self::index_key(idx)),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };
        let end_bytes = match range.end_bound() {
            std::ops::Bound::Included(&idx) => std::ops::Bound::Included(Self::index_key(idx)),
            std::ops::Bound::Excluded(&idx) => std::ops::Bound::Excluded(Self::index_key(idx)),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };

        let mut entries = Vec::new();
        for item in self.log.range((start_bytes, end_bytes)) {
            let (_k, v) = item.map_err(|e| StorageIOError::read_logs(to_any_error(e)))?;
            let entry = Self::deserialize_entry(&v)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// RaftLogStorage
// ---------------------------------------------------------------------------

impl openraft::storage::RaftLogStorage<FerrosRaftConfig> for SledLogStore {
    type LogReader = SledLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<FerrosRaftConfig>, StorageError<u64>> {
        let last_purged: Option<LogId<u64>> = Self::load_meta(&self.meta, META_LAST_PURGED)?;

        let last_in_log = self.last_entry_log_id()?;

        let last_log_id = last_in_log.or(last_purged);

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // sled trees are cheaply cloneable (they share the inner Arc).
        SledLogStore {
            db: self.db.clone(),
            log: self.log.clone(),
            meta: self.meta.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let meta = self.meta.clone();
        let bytes =
            bincode::serialize(vote).map_err(|e| StorageIOError::write_vote(to_any_error(e)))?;
        tokio::task::spawn_blocking(move || -> Result<(), Box<StorageIOError<u64>>> {
            meta.insert(META_VOTE, bytes)
                .map_err(|e| Box::new(StorageIOError::write_vote(to_any_error(e))))?;
            meta.flush()
                .map_err(|e| Box::new(StorageIOError::write_vote(to_any_error(e))))?;
            Ok(())
        })
        .await
        .map_err(|e| StorageIOError::write_vote(to_any_error(e)))?
        .map_err(|e| *e)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(Self::load_meta(&self.meta, META_VOTE)?)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        Self::save_meta(&self.meta, META_COMMITTED, &committed)?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(Self::load_meta::<Option<LogId<u64>>>(&self.meta, META_COMMITTED)?.unwrap_or(None))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<FerrosRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<FerrosRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut batch = sled::Batch::default();
        for entry in entries {
            let key = Self::index_key(entry.log_id.index);
            let val = Self::serialize_entry(&entry)?;
            batch.insert(&key, val);
        }

        // Run sled disk IO on a blocking thread so the async Raft runtime
        // stays responsive for heartbeat processing.
        let log = self.log.clone();
        tokio::task::spawn_blocking(move || -> Result<(), Box<StorageIOError<u64>>> {
            log.apply_batch(batch)
                .map_err(|e| Box::new(StorageIOError::write_logs(to_any_error(e))))?;
            Ok(())
        })
        .await
        .map_err(|e| StorageIOError::write_logs(to_any_error(e)))?
        .map_err(|e| *e)?;

        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let log = self.log.clone();
        let start = Self::index_key(log_id.index);
        tokio::task::spawn_blocking(move || -> Result<(), Box<StorageIOError<u64>>> {
            let keys_to_remove: Vec<sled::IVec> = log
                .range(start..)
                .filter_map(|r| r.ok().map(|(k, _v)| k))
                .collect();
            let mut batch = sled::Batch::default();
            for key in keys_to_remove {
                batch.remove(key);
            }
            log.apply_batch(batch)
                .map_err(|e| Box::new(StorageIOError::write_logs(to_any_error(e))))?;
            Ok(())
        })
        .await
        .map_err(|e| StorageIOError::write_logs(to_any_error(e)))?
        .map_err(|e| *e)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        // Persist the purge point first so recovery can reconstruct state.
        Self::save_meta(&self.meta, META_LAST_PURGED, &log_id)?;

        // Remove all entries with index <= log_id.index.
        // Big-endian keys: 0x0000..0000 through index_key(log_id.index) inclusive.
        let end_inclusive = Self::index_key(log_id.index);
        let keys_to_remove: Vec<sled::IVec> = self
            .log
            .range(..=end_inclusive)
            .filter_map(|r| r.ok().map(|(k, _v)| k))
            .collect();

        let mut batch = sled::Batch::default();
        for key in keys_to_remove {
            batch.remove(key);
        }
        self.log
            .apply_batch(batch)
            .map_err(|e| StorageIOError::write_logs(to_any_error(e)))?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Convert an error into an `AnyError` for openraft storage errors.
fn to_any_error(e: impl std::error::Error + Send + Sync + 'static) -> AnyError {
    AnyError::new(&e)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use openraft::storage::RaftLogStorage;
    use openraft::{CommittedLeaderId, EntryPayload};

    /// Helper: create a blank entry at the given index.
    fn blank_entry(term: u64, index: u64) -> Entry<FerrosRaftConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Blank,
        }
    }

    // -- append_and_read_back ---------------------------------------------

    #[tokio::test]
    async fn append_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        let entries = vec![blank_entry(1, 1), blank_entry(1, 2), blank_entry(1, 3)];

        // We can't construct a real LogFlushed (its constructor is pub(crate)
        // inside openraft), so we use the RaftLogReader trait directly and
        // insert via the sled batch path that `append` uses internally.
        {
            let mut batch = sled::Batch::default();
            for entry in &entries {
                let key = SledLogStore::index_key(entry.log_id.index);
                let val = SledLogStore::serialize_entry(entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        let read_back: Vec<Entry<FerrosRaftConfig>> =
            store.try_get_log_entries(1u64..4u64).await.unwrap();

        assert_eq!(read_back.len(), 3);
        for (i, entry) in read_back.iter().enumerate() {
            assert_eq!(entry.log_id.index, (i as u64) + 1);
        }
    }

    // -- purge_removes_old_entries ----------------------------------------

    #[tokio::test]
    async fn purge_removes_old_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Insert entries 1..=5 directly.
        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=5 {
                let entry = blank_entry(1, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        // Purge up to index 3 (inclusive).
        let purge_id = LogId::new(CommittedLeaderId::new(1, 0), 3);
        store.purge(purge_id).await.unwrap();

        // Entries 1-3 should be gone.
        let remaining = store.try_get_log_entries(1u64..6u64).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index, 4);
        assert_eq!(remaining[1].log_id.index, 5);

        // last_purged should be recorded.
        let state =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::get_log_state(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(state.last_purged_log_id, Some(purge_id));
    }

    // -- vote_persistence -------------------------------------------------

    #[tokio::test]
    async fn vote_persistence() {
        let dir = tempfile::tempdir().unwrap();

        let vote = Vote::new(5, 42);

        // Save via first store instance.
        {
            let mut store = SledLogStore::new(dir.path()).unwrap();
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::save_vote(
                &mut store, &vote,
            )
            .await
            .unwrap();
        }

        // Re-open and read back.
        {
            let mut store = SledLogStore::new(dir.path()).unwrap();
            let read_back =
                <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::read_vote(
                    &mut store,
                )
                .await
                .unwrap();
            assert_eq!(read_back, Some(vote));
        }
    }

    // -- empty_store_state ------------------------------------------------

    #[tokio::test]
    async fn empty_store_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        let state =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::get_log_state(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(state.last_log_id, None);

        let vote =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::read_vote(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(vote, None);
    }

    // -- get_log_state with entries present --------------------------------

    #[tokio::test]
    async fn get_log_state_returns_last_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Insert entries 1..=3 directly.
        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=3 {
                let entry = blank_entry(2, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        let state =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::get_log_state(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(state.last_purged_log_id, None);
        assert!(state.last_log_id.is_some());
        assert_eq!(state.last_log_id.unwrap().index, 3);
    }

    // -- save_committed / read_committed round-trip -----------------------

    #[tokio::test]
    async fn committed_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Initially no committed value.
        let initial =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::read_committed(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(initial, None);

        // Save a committed log id.
        let committed = Some(LogId::new(CommittedLeaderId::new(3, 0), 7));
        <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::save_committed(
            &mut store, committed,
        )
        .await
        .unwrap();

        // Read back.
        let read_back =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::read_committed(
                &mut store,
            )
            .await
            .unwrap();
        assert_eq!(read_back, committed);
    }

    // -- truncate removes entries from given index onward ------------------

    #[tokio::test]
    async fn truncate_removes_tail_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Insert entries 1..=5.
        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=5 {
                let entry = blank_entry(1, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        // Truncate from index 3 onward (3, 4, 5 removed).
        let truncate_id = LogId::new(CommittedLeaderId::new(1, 0), 3);
        <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::truncate(
            &mut store,
            truncate_id,
        )
        .await
        .unwrap();

        // Only entries 1 and 2 should remain.
        let remaining = store.try_get_log_entries(1u64..6u64).await.unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].log_id.index, 1);
        assert_eq!(remaining[1].log_id.index, 2);
    }

    // -- get_log_reader returns independent reader -------------------------

    #[tokio::test]
    async fn get_log_reader_returns_reader_with_same_data() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Insert an entry
        {
            let mut batch = sled::Batch::default();
            let entry = blank_entry(1, 1);
            let key = SledLogStore::index_key(1);
            let val = SledLogStore::serialize_entry(&entry).unwrap();
            batch.insert(&key, val);
            store.log.apply_batch(batch).unwrap();
        }

        let mut reader =
            <SledLogStore as openraft::storage::RaftLogStorage<FerrosRaftConfig>>::get_log_reader(
                &mut store,
            )
            .await;

        let entries = reader.try_get_log_entries(1u64..2u64).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 1);
    }

    // -- range queries with various bounds --------------------------------

    #[tokio::test]
    async fn try_get_log_entries_empty_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        // Insert entries 1..=3
        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=3 {
                let entry = blank_entry(1, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        // Range that doesn't match any entries
        let entries = store.try_get_log_entries(10u64..20u64).await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn try_get_log_entries_single_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SledLogStore::new(dir.path()).unwrap();

        {
            let mut batch = sled::Batch::default();
            for idx in 1u64..=5 {
                let entry = blank_entry(1, idx);
                let key = SledLogStore::index_key(idx);
                let val = SledLogStore::serialize_entry(&entry).unwrap();
                batch.insert(&key, val);
            }
            store.log.apply_batch(batch).unwrap();
        }

        // Get exactly one entry
        let entries = store.try_get_log_entries(3u64..4u64).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 3);
    }
}
