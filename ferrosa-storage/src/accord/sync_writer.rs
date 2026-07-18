//! Fsync-before-ack writer for Accord protocol durability.
//!
//! The Accord protocol requires that commit log entries are fsynced to disk
//! BEFORE protocol replies are sent. This ensures durability even if the
//! process crashes immediately after sending a reply.
//!
//! # Key Invariant
//!
//! Any code path that sends a protocol reply (PreAcceptOK, AcceptOK, etc.)
//! must call [`SyncWriter::write_and_sync`] first. If it returns
//! [`SyncWriteResult::FsyncFailed`], no reply is sent.
//!
//! # Production vs Testing
//!
//! - [`FileSyncWriter`] — production implementation using `File::sync_all()`
//! - [`MockSyncWriter`] — test implementation that records call ordering

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

/// Result of a sync write operation.
#[derive(Debug)]
pub enum SyncWriteResult {
    /// Write and fsync completed successfully.
    Ok,
    /// Fsync failed — caller must NOT send a protocol reply.
    FsyncFailed(io::Error),
}

impl SyncWriteResult {
    /// Returns true if the write and fsync succeeded.
    pub fn is_ok(&self) -> bool {
        matches!(self, SyncWriteResult::Ok)
    }

    /// Returns true if the fsync failed.
    pub fn is_failed(&self) -> bool {
        matches!(self, SyncWriteResult::FsyncFailed(_))
    }
}

/// Trait for write-and-sync operations.
///
/// Implementations guarantee that data is durable on disk after
/// `write_and_sync` returns `Ok`. This is the foundation for
/// Accord's fsync-before-ack invariant.
pub trait SyncWriter: Send + Sync {
    /// Write data and fsync. Returns only after fsync completes.
    ///
    /// If fsync fails, returns `FsyncFailed` and the caller must NOT
    /// send a protocol reply.
    fn write_and_sync(&self, data: &[u8]) -> SyncWriteResult;
}

// ---------------------------------------------------------------------------
// FileSyncWriter — production implementation
// ---------------------------------------------------------------------------

/// Production implementation using `File::sync_all()`.
///
/// Appends data to a file and calls `sync_all()` to guarantee the data
/// is durable on disk before returning. Each call opens, writes, syncs,
/// and closes the file to ensure fsync covers the written data.
pub struct FileSyncWriter {
    /// Path to the log file.
    path: PathBuf,
}

impl FileSyncWriter {
    /// Create a new `FileSyncWriter` that writes to the given path.
    ///
    /// The file is created if it does not exist. The parent directory
    /// must already exist.
    pub fn new(path: PathBuf) -> Self {
        assert!(
            path.parent()
                .is_some_and(|p| p.exists() || p == std::path::Path::new("")),
            "parent directory must exist: {:?}",
            path.parent()
        );
        // Fail loud: `path` is the append-only log FILE. If a caller hands us a
        // directory (the accord dir itself), every `write_and_sync` would open a
        // directory as an append file (EISDIR) and silently fail forever, which
        // makes every Accord PreAccept return `None` → "quorum unavailable".
        assert!(
            !path.is_dir(),
            "FileSyncWriter path must be a file, not a directory: {path:?}"
        );
        Self { path }
    }
}

impl SyncWriter for FileSyncWriter {
    fn write_and_sync(&self, data: &[u8]) -> SyncWriteResult {
        let file_result = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path);

        let mut file = match file_result {
            Ok(f) => f,
            Err(e) => return SyncWriteResult::FsyncFailed(e),
        };

        if let Err(e) = file.write_all(data) {
            return SyncWriteResult::FsyncFailed(e);
        }

        match file.sync_all() {
            Ok(()) => SyncWriteResult::Ok,
            Err(e) => SyncWriteResult::FsyncFailed(e),
        }
    }
}

// ---------------------------------------------------------------------------
// MockSyncWriter — test implementation
// ---------------------------------------------------------------------------

/// Records what operations were called during a sync write, for test assertions.
#[derive(Debug, Clone, PartialEq)]
pub enum SyncWriteCall {
    /// A write operation was performed.
    Write,
    /// An fsync completed successfully.
    Fsync,
    /// An fsync was attempted but failed.
    FsyncFailed,
}

/// Mock implementation for testing that records call ordering.
///
/// Tests use this to verify that writes happen before fsyncs, and that
/// fsync failures prevent protocol replies from being sent.
pub struct MockSyncWriter {
    /// Log of all calls made, in order.
    pub call_log: Mutex<Vec<SyncWriteCall>>,
    /// When true, the next fsync will fail.
    pub fail_fsync: AtomicBool,
}

impl MockSyncWriter {
    /// Create a new `MockSyncWriter` with an empty call log and fsync enabled.
    pub fn new() -> Self {
        Self {
            call_log: Mutex::new(Vec::new()),
            fail_fsync: AtomicBool::new(false),
        }
    }

    /// Set whether fsync should fail on subsequent calls.
    pub fn set_fsync_failure(&self, fail: bool) {
        self.fail_fsync.store(fail, Ordering::Release);
    }

    /// Return a snapshot of all recorded calls.
    pub fn calls(&self) -> Vec<SyncWriteCall> {
        self.call_log
            .lock()
            .expect("call_log lock poisoned")
            .clone()
    }
}

impl Default for MockSyncWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncWriter for MockSyncWriter {
    fn write_and_sync(&self, _data: &[u8]) -> SyncWriteResult {
        let mut log = self.call_log.lock().expect("call_log lock poisoned");

        // Record the write.
        log.push(SyncWriteCall::Write);

        // Attempt fsync.
        if self.fail_fsync.load(Ordering::Acquire) {
            log.push(SyncWriteCall::FsyncFailed);
            SyncWriteResult::FsyncFailed(io::Error::other("simulated fsync failure"))
        } else {
            log.push(SyncWriteCall::Fsync);
            SyncWriteResult::Ok
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A `FileSyncWriter` must persist to a real FILE inside the accord dir and
    /// return `Ok` — the path is a log file, never the directory itself. This is
    /// the regression for the production bug where `transition_to_cluster` passed
    /// the accord *directory* to `FileSyncWriter`: every `write_and_sync` then
    /// opened a directory as an append file (EISDIR) → `FsyncFailed` → every
    /// Accord `handle_preaccept` returned `SmResponse::None` → every transaction
    /// failed "Accord quorum unavailable".
    #[test]
    fn file_sync_writer_persists_to_a_file_not_the_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log_path = dir.path().join("protocol.log");
        let writer = FileSyncWriter::new(log_path.clone());

        let result = writer.write_and_sync(b"PreAccepted:1:2");
        assert!(
            result.is_ok(),
            "write_and_sync to a real file must succeed, got {result:?}"
        );
        let persisted = std::fs::read(&log_path).expect("log file must exist");
        assert_eq!(persisted, b"PreAccepted:1:2");
    }

    /// Fail loud: constructing a `FileSyncWriter` on an existing DIRECTORY is a
    /// caller bug (the write path would silently EISDIR forever). Reject it at
    /// construction instead of failing every fsync at runtime.
    #[test]
    #[should_panic(expected = "must be a file, not a directory")]
    fn file_sync_writer_rejects_a_directory_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let _ = FileSyncWriter::new(dir.path().to_path_buf());
    }

    /// Test 1: Verify that write happens before fsync in the call ordering.
    ///
    /// Uses MockSyncWriter to record the sequence of operations and asserts
    /// that [Write, Fsync] is the exact ordering.
    #[test]
    fn fsync_before_ack_ordering() {
        let writer = MockSyncWriter::new();
        let result = writer.write_and_sync(b"preaccept-data");

        assert!(result.is_ok(), "write_and_sync should succeed");

        let calls = writer.calls();
        assert_eq!(
            calls,
            vec![SyncWriteCall::Write, SyncWriteCall::Fsync],
            "write must happen before fsync"
        );
    }

    /// Test 2: Simulate an Accept handler — write AccordAccepted entry, then
    /// verify the write-before-fsync ordering.
    ///
    /// In production, the Accept handler would serialize an AccordAccepted
    /// entry and pass it to write_and_sync. We simulate this with a
    /// representative byte payload.
    #[test]
    fn fsync_before_ack_accept() {
        let writer = MockSyncWriter::new();

        // Simulate serializing an AccordAccepted entry.
        let accepted_entry = b"AccordAccepted{txn_id=1234,ballot=5,timestamp=67890}";

        let result = writer.write_and_sync(accepted_entry);
        assert!(result.is_ok(), "accept write_and_sync should succeed");

        let calls = writer.calls();
        assert_eq!(calls.len(), 2, "should have exactly two calls");
        assert_eq!(
            calls[0],
            SyncWriteCall::Write,
            "first call must be Write (data written before fsync)"
        );
        assert_eq!(
            calls[1],
            SyncWriteCall::Fsync,
            "second call must be Fsync (fsync before ack)"
        );
    }

    /// Test 3: Simulate an Apply handler — write AccordApplied entry, verify
    /// ordering.
    ///
    /// The applied=true flag should only be set AFTER fsync succeeds. This
    /// test documents that contract: if write_and_sync returns Ok, the caller
    /// is permitted to set applied=true. If it returns FsyncFailed, the
    /// applied flag must NOT be set.
    #[test]
    fn fsync_before_ack_apply() {
        let writer = MockSyncWriter::new();

        // Simulate serializing an AccordApplied entry.
        let applied_entry = b"AccordApplied{txn_id=1234,applied=pending}";

        let result = writer.write_and_sync(applied_entry);
        assert!(result.is_ok(), "apply write_and_sync should succeed");

        let calls = writer.calls();
        assert_eq!(
            calls,
            vec![SyncWriteCall::Write, SyncWriteCall::Fsync],
            "write must happen before fsync"
        );

        // Contract: applied=true is only safe to set AFTER write_and_sync
        // returns Ok. Simulate the decision gate:
        let applied = result.is_ok(); // only true after successful fsync
        assert!(
            applied,
            "applied flag must only be set after fsync succeeds"
        );

        // Verify the inverse: if fsync fails, applied must NOT be set.
        let writer2 = MockSyncWriter::new();
        writer2.set_fsync_failure(true);
        let result2 = writer2.write_and_sync(applied_entry);
        let applied2 = result2.is_ok();
        assert!(!applied2, "applied flag must NOT be set when fsync fails");
    }

    /// Test 4: Verify that fsync failure prevents protocol replies.
    ///
    /// When fsync fails, write_and_sync returns FsyncFailed, and the call
    /// log records [Write, FsyncFailed] — no successful Fsync entry.
    #[test]
    fn fsync_failure_prevents_reply() {
        let writer = MockSyncWriter::new();
        writer.set_fsync_failure(true);

        let result = writer.write_and_sync(b"preaccept-data");

        assert!(result.is_failed(), "should return FsyncFailed");

        let calls = writer.calls();
        assert_eq!(
            calls,
            vec![SyncWriteCall::Write, SyncWriteCall::FsyncFailed],
            "call log should show Write then FsyncFailed (no successful Fsync)"
        );

        // The caller must NOT send a reply when fsync fails.
        // This is enforced by checking the result before sending.
        let should_send_reply = result.is_ok();
        assert!(
            !should_send_reply,
            "protocol reply must NOT be sent after fsync failure"
        );
    }

    /// Test 5: Verify that two independent shards (MockSyncWriters) operate
    /// independently — one does not block the other.
    ///
    /// This is a simple independence test: both shards write and sync
    /// successfully, and each has its own isolated call log.
    #[test]
    fn fsync_latency_does_not_block_other_shards() {
        let shard1 = MockSyncWriter::new();
        let shard2 = MockSyncWriter::new();

        // Write on shard 1.
        let result1 = shard1.write_and_sync(b"shard1-preaccept");
        assert!(result1.is_ok(), "shard 1 should succeed");

        // Write on shard 2.
        let result2 = shard2.write_and_sync(b"shard2-accept");
        assert!(result2.is_ok(), "shard 2 should succeed");

        // Each shard has its own independent call log.
        let calls1 = shard1.calls();
        let calls2 = shard2.calls();

        assert_eq!(
            calls1,
            vec![SyncWriteCall::Write, SyncWriteCall::Fsync],
            "shard 1 should have independent write+fsync"
        );
        assert_eq!(
            calls2,
            vec![SyncWriteCall::Write, SyncWriteCall::Fsync],
            "shard 2 should have independent write+fsync"
        );

        // Verify isolation: failing shard 1 does not affect shard 2.
        shard1.set_fsync_failure(true);

        let result1_fail = shard1.write_and_sync(b"shard1-will-fail");
        let result2_ok = shard2.write_and_sync(b"shard2-still-ok");

        assert!(
            result1_fail.is_failed(),
            "shard 1 should fail after setting failure"
        );
        assert!(
            result2_ok.is_ok(),
            "shard 2 must not be affected by shard 1 failure"
        );
    }

    /// Bonus: FileSyncWriter integration test with a real temporary file.
    #[test]
    fn file_sync_writer_writes_and_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("accord-protocol.log");

        let writer = FileSyncWriter::new(path.clone());

        let result = writer.write_and_sync(b"entry-1");
        assert!(result.is_ok(), "first write should succeed");

        let result = writer.write_and_sync(b"entry-2");
        assert!(result.is_ok(), "second write should succeed");

        // Verify file contents (both entries appended).
        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"entry-1entry-2");
    }
}
