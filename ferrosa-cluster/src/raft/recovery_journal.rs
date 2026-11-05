//! Durable recovery progress for a single Raft group.
//! The session guard is armed before physical effects, not once per entry.

use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::PathBuf,
    sync::Mutex,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryPhase {
    Initialized,
    SessionArmed,
    SnapshotSynced,
    Quarantined,
    Repairing,
    Verifying,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryLogPosition {
    pub term: u64,
    pub node_id: u64,
    pub index: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRecord {
    pub version: u32,
    pub group: String,
    pub session: String,
    pub phase: RecoveryPhase,
    pub reason: String,
    #[serde(default)]
    pub failed_at: Option<u64>,
    pub synced: u64,
    pub purge_point: u64,
    pub received: u64,
    pub source: Option<u64>,
    #[serde(default)]
    pub retirement_scope: Option<uuid::Uuid>,
    #[serde(default)]
    pub committed_target: Option<RecoveryLogPosition>,
}

pub struct RecoveryJournal {
    path: PathBuf,
    group: String,
    lease: File,
    io_serial: Mutex<()>,
}

impl RecoveryJournal {
    pub fn open(path: PathBuf, group: String) -> io::Result<(Self, Option<RecoveryRecord>)> {
        let lock_path = path.with_extension("lock");
        let existing_lease = lock_path.try_exists()?;
        match fs::symlink_metadata(&lock_path) {
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "recovery lock is not a regular file",
                ));
            }
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lease = options.open(lock_path)?;
        lease.try_lock().map_err(io::Error::other)?;
        let journal = Self {
            path,
            group,
            lease,
            io_serial: Mutex::new(()),
        };
        let previous = journal.read_record()?;
        if previous.is_none() {
            if existing_lease {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "established recovery lease has no durable record",
                ));
            }
            let initial = RecoveryRecord {
                version: 1,
                group: journal.group.clone(),
                session: uuid::Uuid::new_v4().to_string(),
                phase: RecoveryPhase::Initialized,
                reason: "no physical effects admitted".into(),
                failed_at: None,
                synced: 0,
                purge_point: 0,
                received: 0,
                source: None,
                retirement_scope: None,
                committed_target: None,
            };
            journal.persist(&initial)?;
        }
        Ok((journal, previous))
    }

    pub fn arm_session(&self, synced: u64, purge_point: u64) -> io::Result<RecoveryRecord> {
        let mut record = self
            .read_record()?
            .ok_or_else(|| io::Error::other("initialized recovery record is missing"))?;
        if record.phase != RecoveryPhase::Initialized {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "existing recovery session must be resumed",
            ));
        }
        record.phase = RecoveryPhase::SessionArmed;
        record.reason = "unclean Raft apply session".into();
        record.synced = synced;
        record.purge_point = purge_point;
        self.persist(&record)?;
        Ok(record)
    }

    pub fn persist(&self, record: &RecoveryRecord) -> io::Result<()> {
        self.persist_with(record, |file, bytes| file.write_all(bytes))
    }

    fn read_record(&self) -> io::Result<Option<RecoveryRecord>> {
        match fs::symlink_metadata(&self.path) {
            Ok(metadata) if metadata.file_type().is_file() => {
                let record: RecoveryRecord = serde_json::from_slice(&fs::read(&self.path)?)?;
                if record.version != 1 || record.group != self.group {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "recovery journal version/group mismatch",
                    ));
                }
                Ok(Some(record))
            }
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recovery journal is not a regular file",
            )),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn persist_with(
        &self,
        record: &RecoveryRecord,
        write: impl FnOnce(&mut File, &[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        // The mutex contains no mutable data. Every replacement revalidates the
        // on-disk record; an unwound writer cannot invalidate a cached payload.
        let _serial = self
            .io_serial
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if record.version != 1 || record.group != self.group {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovery journal version/group mismatch",
            ));
        }
        if let Some(previous) = self.read_record()? {
            if previous.session != record.session
                || record.synced < previous.synced
                || record.purge_point < previous.purge_point
                || record.received < previous.received
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "stale recovery session/progress",
                ));
            }
        }
        let parent = self
            .path
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "journal has no parent"))?;
        let temporary = parent.join(format!(".raft-recovery-{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            write(&mut file, &serde_json::to_vec(record)?)?;
            file.sync_all()?;
            fs::rename(&temporary, &self.path)?;
            File::open(parent)?.sync_all()
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
}

impl Drop for RecoveryJournal {
    fn drop(&mut self) {
        // Keep the lock inode: another process may already be waiting on it.
        let _ = self.lease.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_preserves_quarantine_reason_and_verified_repair_position() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dc-a.recovery.json");
        let (journal, previous) = RecoveryJournal::open(path.clone(), "dc-a".into()).unwrap();
        assert!(previous.is_none());
        let mut record = journal.arm_session(7, 9).unwrap();
        record.phase = RecoveryPhase::Verifying;
        record.reason = "snapshot does not cover purge point 9".into();
        record.failed_at = Some(9);
        record.source = Some(2);
        record.received = 10;
        journal.persist(&record).unwrap();
        drop(journal);

        let (_, reopened) = RecoveryJournal::open(path, "dc-a".into()).unwrap();
        assert_eq!(
            reopened,
            Some(record),
            "restart must resume quarantine and the delivered content position"
        );
    }

    #[test]
    fn failed_progress_write_preserves_preexisting_durable_session_guard() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dc-a.recovery.json");
        let (journal, _) = RecoveryJournal::open(path.clone(), "dc-a".into()).unwrap();
        let armed = journal.arm_session(7, 9).unwrap();
        let mut failed = armed.clone();
        failed.phase = RecoveryPhase::Quarantined;
        failed.failed_at = Some(10);
        failed.reason = "physical projection failed at 10".into();
        let error = journal
            .persist_with(&failed, |file, _| {
                file.write_all(b"partial")?;
                Err(io::Error::other("injected progress write failure"))
            })
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("injected progress write failure"));
        drop(journal);
        let (_, reopened) = RecoveryJournal::open(path, "dc-a".into()).unwrap();
        assert_eq!(reopened, Some(armed));
    }

    #[test]
    fn stale_progress_and_rearming_cannot_clear_a_repair() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dc-a.recovery.json");
        let (journal, _) = RecoveryJournal::open(path.clone(), "dc-a".into()).unwrap();
        let mut record = journal.arm_session(7, 9).unwrap();
        record.phase = RecoveryPhase::Repairing;
        record.source = Some(2);
        record.received = 10;
        journal.persist(&record).unwrap();
        let mut stale = record.clone();
        stale.received = 8;
        assert!(journal.persist(&stale).is_err());
        assert!(journal.arm_session(7, 9).is_err());
        drop(journal);
        let (_, reopened) = RecoveryJournal::open(path, "dc-a".into()).unwrap();
        assert_eq!(reopened, Some(record));
    }

    #[test]
    fn another_group_cannot_adopt_the_journal() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dc-a.recovery.json");
        let (journal, _) = RecoveryJournal::open(path.clone(), "dc-a".into()).unwrap();
        journal.arm_session(7, 9).unwrap();
        drop(journal);
        assert!(RecoveryJournal::open(path.clone(), "dc-b".into()).is_err());
        let (_, reopened) = RecoveryJournal::open(path, "dc-a".into()).unwrap();
        assert!(reopened.is_some());
    }
    #[test]
    fn runtime_review_established_missing_record_is_not_fresh() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dc-a.recovery.json");
        let (journal, previous) = RecoveryJournal::open(path.clone(), "dc-a".into()).unwrap();
        assert!(
            previous.is_none(),
            "genuine fresh initialization stays supported"
        );
        journal.arm_session(7, 9).unwrap();
        drop(journal);
        std::fs::remove_file(&path).unwrap();
        assert!(
            RecoveryJournal::open(path, "dc-a".into()).is_err(),
            "an established lease without its durable record must fail closed"
        );
    }
}
