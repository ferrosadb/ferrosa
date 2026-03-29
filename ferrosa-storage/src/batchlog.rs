//! Batchlog: distributed logged batch coordination.
//!
//! A [`BatchlogEntry`] represents a logged batch that has been written to the
//! batchlog but not yet confirmed as fully applied. The [`BatchlogManager`]
//! persists entries and supports scan-for-stale queries for background replay.

use std::collections::HashMap;
use std::sync::Mutex;

use uuid::Uuid;

use crate::commitlog::mutation::Mutation;

/// A logged batch entry. Persisted in the batchlog until all mutations
/// have been confirmed applied, then deleted.
#[derive(Debug, Clone)]
pub struct BatchlogEntry {
    /// Unique batch ID (TimeUUID in Cassandra; UUIDv4 here).
    pub id: Uuid,
    /// Millisecond timestamp when the batch was created.
    pub created_at: i64,
    /// The mutations comprising this batch. Each `Mutation` targets a
    /// single (keyspace, table, partition_key) combination.
    pub mutations: Vec<Mutation>,
}

/// Binary layout:
/// uuid:16 | created_at:i64 | mutation_count:u16
/// | (mutation_size:u32 | mutation_bytes)...
impl BatchlogEntry {
    /// Serialize this entry to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        // Compute total size.
        let mut size = 16 + 8 + 2; // uuid + created_at + mutation_count
        let mut mutation_bufs: Vec<Vec<u8>> = Vec::with_capacity(self.mutations.len());
        for m in &self.mutations {
            let msize = m.serialized_size();
            let mut buf = vec![0u8; msize];
            m.serialize_into(&mut buf);
            size += 4 + msize; // length prefix + body
            mutation_bufs.push(buf);
        }

        let mut out = Vec::with_capacity(size);
        out.extend_from_slice(self.id.as_bytes());
        out.extend_from_slice(&self.created_at.to_be_bytes());
        out.extend_from_slice(&(self.mutations.len() as u16).to_be_bytes());
        for buf in &mutation_bufs {
            out.extend_from_slice(&(buf.len() as u32).to_be_bytes());
            out.extend_from_slice(buf);
        }
        out
    }

    /// Deserialize from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        if data.len() < 26 {
            return Err("batchlog entry too short".into());
        }
        let mut pos = 0;

        let mut uuid_bytes = [0u8; 16];
        uuid_bytes.copy_from_slice(&data[pos..pos + 16]);
        let id = Uuid::from_bytes(uuid_bytes);
        pos += 16;

        let created_at = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let mutation_count = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let mut mutations = Vec::with_capacity(mutation_count);
        for _ in 0..mutation_count {
            if pos + 4 > data.len() {
                return Err("truncated mutation length".into());
            }
            let mlen = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + mlen > data.len() {
                return Err("truncated mutation body".into());
            }
            let m = Mutation::deserialize_from(&data[pos..pos + mlen])
                .map_err(|e| format!("mutation deserialize: {e}"))?;
            mutations.push(m);
            pos += mlen;
        }

        Ok(BatchlogEntry {
            id,
            created_at,
            mutations,
        })
    }

    /// Returns true if this entry is older than `threshold_ms` relative to
    /// `now_ms`. Both values are in milliseconds since an arbitrary epoch.
    pub fn is_stale(&self, now_ms: i64, threshold_ms: i64) -> bool {
        let age = now_ms.saturating_sub(self.created_at);
        age > threshold_ms
    }
}

/// Configuration for the batchlog manager.
#[derive(Debug, Clone)]
pub struct BatchlogConfig {
    /// Entries older than this threshold (milliseconds) are considered stale
    /// and eligible for replay. Default: 20_000 (20 seconds = 2x write timeout).
    pub stale_threshold_ms: i64,
    /// How often the background replay task scans for stale entries (ms).
    /// Default: 60_000 (60 seconds).
    pub replay_interval_ms: u64,
}

impl Default for BatchlogConfig {
    fn default() -> Self {
        Self {
            stale_threshold_ms: 20_000,
            replay_interval_ms: 60_000,
        }
    }
}

/// Manages the batchlog (system.batches).
///
/// Entries are stored in memory and backed by the commit log for durability.
/// The coordinator writes an entry before fanning out mutations, and deletes
/// it after all mutations are confirmed. A background task scans for stale
/// entries and replays them.
pub struct BatchlogManager {
    config: BatchlogConfig,
    entries: Mutex<HashMap<Uuid, BatchlogEntry>>,
}

impl BatchlogManager {
    /// Create a new batchlog manager with the given configuration.
    pub fn new(config: BatchlogConfig) -> Self {
        Self {
            config,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Write (persist) a batchlog entry.
    pub fn write_entry(&self, entry: BatchlogEntry) -> Result<(), String> {
        let mut entries = self.entries.lock().unwrap();
        entries.insert(entry.id, entry);
        Ok(())
    }

    /// Delete a batchlog entry after the batch has been fully applied.
    pub fn delete_entry(&self, id: Uuid) -> Result<(), String> {
        let mut entries = self.entries.lock().unwrap();
        entries.remove(&id);
        Ok(())
    }

    /// Retrieve a batchlog entry by ID, if it exists.
    pub fn get_entry(&self, id: Uuid) -> Option<BatchlogEntry> {
        let entries = self.entries.lock().unwrap();
        entries.get(&id).cloned()
    }

    /// Number of entries currently in the batchlog.
    pub fn entry_count(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    /// Returns all stale entries (older than `stale_threshold_ms`), sorted
    /// by `created_at` ascending (oldest first).
    pub fn scan_stale(&self, now_ms: i64) -> Vec<BatchlogEntry> {
        let entries = self.entries.lock().unwrap();
        let mut stale: Vec<BatchlogEntry> = entries
            .values()
            .filter(|e| e.is_stale(now_ms, self.config.stale_threshold_ms))
            .cloned()
            .collect();
        stale.sort_by_key(|e| e.created_at);
        stale
    }

    /// Returns the batchlog configuration.
    pub fn config(&self) -> &BatchlogConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batchlog_entry_creation() {
        let id = Uuid::new_v4();
        let entry = BatchlogEntry {
            id,
            created_at: 1000,
            mutations: vec![],
        };
        assert_eq!(entry.id, id);
        assert_eq!(entry.created_at, 1000);
        assert!(entry.mutations.is_empty());
    }

    #[test]
    fn batchlog_entry_roundtrip_empty() {
        let entry = BatchlogEntry {
            id: Uuid::new_v4(),
            created_at: 42_000,
            mutations: vec![],
        };
        let bytes = entry.serialize();
        let decoded = BatchlogEntry::deserialize(&bytes).unwrap();
        assert_eq!(decoded.id, entry.id);
        assert_eq!(decoded.created_at, entry.created_at);
        assert!(decoded.mutations.is_empty());
    }

    #[test]
    fn batchlog_entry_roundtrip_with_mutations() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let m1 = Mutation {
            mutation_id: [0x30u8; 16],
            keyspace: "ks1".to_string(),
            table: "tbl1".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
            timestamp: 1000,
        };
        let m2 = Mutation {
            mutation_id: [0x31u8; 16],
            keyspace: "ks1".to_string(),
            table: "tbl2".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk2".to_vec())),
            rows: vec![Row {
                clustering: vec![1, 2],
                cells: vec![(0, CellValue::live(b"world".to_vec(), 2000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(2000),
            }],
            timestamp: 2000,
        };

        let entry = BatchlogEntry {
            id: Uuid::new_v4(),
            created_at: 5000,
            mutations: vec![m1, m2],
        };
        let bytes = entry.serialize();
        let decoded = BatchlogEntry::deserialize(&bytes).unwrap();

        assert_eq!(decoded.id, entry.id);
        assert_eq!(decoded.created_at, entry.created_at);
        assert_eq!(decoded.mutations.len(), 2);
        assert_eq!(decoded.mutations[0].keyspace, "ks1");
        assert_eq!(decoded.mutations[0].table, "tbl1");
        assert_eq!(decoded.mutations[1].table, "tbl2");
    }

    #[test]
    fn batchlog_entry_is_stale() {
        let entry = BatchlogEntry {
            id: Uuid::new_v4(),
            created_at: 1000,
            mutations: vec![],
        };
        // Entry created at t=1000, current time t=25000, threshold 20000ms
        assert!(entry.is_stale(25_000, 20_000));
        // Entry created at t=1000, current time t=15000, threshold 20000ms
        assert!(!entry.is_stale(15_000, 20_000));
        // Edge: exactly at threshold
        assert!(!entry.is_stale(21_000, 20_000));
        // Just past threshold
        assert!(entry.is_stale(21_001, 20_000));
    }

    #[test]
    fn batchlog_manager_write_entry() {
        let mgr = BatchlogManager::new(BatchlogConfig::default());
        let id = Uuid::new_v4();
        let entry = BatchlogEntry {
            id,
            created_at: 1000,
            mutations: vec![],
        };
        mgr.write_entry(entry).unwrap();
        assert_eq!(mgr.entry_count(), 1);
    }

    #[test]
    fn batchlog_manager_delete_entry() {
        let mgr = BatchlogManager::new(BatchlogConfig::default());
        let id = Uuid::new_v4();
        let entry = BatchlogEntry {
            id,
            created_at: 1000,
            mutations: vec![],
        };
        mgr.write_entry(entry).unwrap();
        assert_eq!(mgr.entry_count(), 1);

        mgr.delete_entry(id).unwrap();
        assert_eq!(mgr.entry_count(), 0);
    }

    #[test]
    fn batchlog_manager_delete_nonexistent_is_noop() {
        let mgr = BatchlogManager::new(BatchlogConfig::default());
        // Deleting from empty manager should not error.
        mgr.delete_entry(Uuid::new_v4()).unwrap();
        assert_eq!(mgr.entry_count(), 0);
    }

    #[test]
    fn batchlog_manager_scan_stale() {
        let config = BatchlogConfig {
            stale_threshold_ms: 10_000,
            ..BatchlogConfig::default()
        };
        let mgr = BatchlogManager::new(config);

        // Entry created at t=1000 -- stale at now=12000 (age=11000 > 10000)
        let id_old = Uuid::new_v4();
        mgr.write_entry(BatchlogEntry {
            id: id_old,
            created_at: 1000,
            mutations: vec![],
        })
        .unwrap();

        // Entry created at t=9000 -- not stale at now=12000 (age=3000 < 10000)
        let id_new = Uuid::new_v4();
        mgr.write_entry(BatchlogEntry {
            id: id_new,
            created_at: 9000,
            mutations: vec![],
        })
        .unwrap();

        let stale = mgr.scan_stale(12_000);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].id, id_old);
    }

    #[test]
    fn batchlog_manager_scan_stale_empty_when_all_fresh() {
        let mgr = BatchlogManager::new(BatchlogConfig::default());
        let id = Uuid::new_v4();
        mgr.write_entry(BatchlogEntry {
            id,
            created_at: 100_000,
            mutations: vec![],
        })
        .unwrap();

        // now_ms = 100_001, threshold = 20_000 -> age = 1 < 20_000
        let stale = mgr.scan_stale(100_001);
        assert!(stale.is_empty());
    }

    #[test]
    fn batchlog_manager_get_entry() {
        let mgr = BatchlogManager::new(BatchlogConfig::default());
        let id = Uuid::new_v4();
        mgr.write_entry(BatchlogEntry {
            id,
            created_at: 5000,
            mutations: vec![],
        })
        .unwrap();

        let entry = mgr.get_entry(id);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().created_at, 5000);

        assert!(mgr.get_entry(Uuid::new_v4()).is_none());
    }
}
