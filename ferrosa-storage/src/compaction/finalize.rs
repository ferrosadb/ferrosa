//! Pure planning helpers for compaction finalization.
//!
//! `StorageEngine::poll_compactions` owns the side effects: opening output SSTables,
//! submitting uploads, saving manifests, and enqueueing deletes. This module keeps
//! the ordering decisions small and testable so crash-safety invariants are not
//! buried in the polling loop.

use std::time::Duration;

use crate::compaction::metadata::SSTableMetadata;
use crate::manifest::ManifestEntry;

/// Result of waiting for the upload worker to confirm a compaction output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadConfirmation {
    Confirmed,
    Failed { message: String },
    WorkerDropped,
}

/// What to do with the durable pending-upload log entry after upload confirmation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingLogDecision {
    /// Upload is confirmed in object storage; removing the replay marker is safe.
    RemoveConfirmed,
    /// Upload did not complete; leave the marker so startup replay can retry.
    KeepForReplay,
}

/// Manifest mutation required after a compacted output has been confirmed durable.
#[derive(Debug, Clone)]
pub struct ManifestUpdatePlan {
    pub table_id: String,
    pub remove_input_ids: Vec<String>,
    pub removals_for_cas_retry: Vec<(String, Vec<String>)>,
    pub add_output: ManifestEntry,
}

/// Fire-and-forget deletion of a compacted-away input SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputDeletionTaskPlan {
    pub table_id: String,
    pub sstable_id: String,
    pub grace_period: Duration,
}

/// Input-deletion enqueue policy after the manifest is updated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputDeletionPlan {
    pub tasks: Vec<InputDeletionTaskPlan>,
    pub fire_and_forget: bool,
}

pub fn pending_log_decision_after_upload(confirmation: UploadConfirmation) -> PendingLogDecision {
    match confirmation {
        UploadConfirmation::Confirmed => PendingLogDecision::RemoveConfirmed,
        UploadConfirmation::Failed { .. } | UploadConfirmation::WorkerDropped => {
            PendingLogDecision::KeepForReplay
        }
    }
}

pub fn upload_confirmation_from_result(
    result: Result<Result<(), String>, tokio::sync::oneshot::error::RecvError>,
) -> UploadConfirmation {
    match result {
        Ok(Ok(())) => UploadConfirmation::Confirmed,
        Ok(Err(message)) => UploadConfirmation::Failed { message },
        Err(_) => UploadConfirmation::WorkerDropped,
    }
}

pub fn plan_manifest_update(
    table_id: &str,
    inputs: &[SSTableMetadata],
    output: &SSTableMetadata,
    output_size: u64,
) -> ManifestUpdatePlan {
    let remove_input_ids: Vec<String> = inputs.iter().map(|input| input.id.clone()).collect();
    ManifestUpdatePlan {
        table_id: table_id.to_string(),
        removals_for_cas_retry: vec![(table_id.to_string(), remove_input_ids.clone())],
        remove_input_ids,
        add_output: ManifestEntry {
            id: output.id.clone(),
            size: output_size,
            min_token: output.min_token,
            max_token: output.max_token,
            min_timestamp: output.min_timestamp,
            max_timestamp: output.max_timestamp,
        },
    }
}

pub fn plan_input_deletions(
    table_id: &str,
    inputs: &[SSTableMetadata],
    grace_period: Duration,
) -> InputDeletionPlan {
    InputDeletionPlan {
        tasks: inputs
            .iter()
            .map(|input| InputDeletionTaskPlan {
                table_id: table_id.to_string(),
                sstable_id: input.id.clone(),
                grace_period,
            })
            .collect(),
        fire_and_forget: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn sstable(id: &str, size_bytes: u64) -> SSTableMetadata {
        SSTableMetadata {
            id: id.to_string(),
            path: PathBuf::from(format!("/tmp/{id}")),
            size_bytes,
            min_token: 10,
            max_token: 20,
            min_timestamp: 30,
            max_timestamp: 40,
            partition_count: 5,
        }
    }

    #[test]
    fn compaction_finalize_keeps_pending_log_when_upload_fails() {
        let decision = pending_log_decision_after_upload(UploadConfirmation::Failed {
            message: "s3 timeout".to_string(),
        });

        assert_eq!(decision, PendingLogDecision::KeepForReplay);
    }

    #[test]
    fn compaction_finalize_removes_pending_log_after_upload_confirmed() {
        let decision = pending_log_decision_after_upload(UploadConfirmation::Confirmed);

        assert_eq!(decision, PendingLogDecision::RemoveConfirmed);
    }

    #[test]
    fn compaction_finalize_upload_confirmation_classifies_worker_drop_as_replayable() {
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        drop(tx);

        let confirmation = upload_confirmation_from_result(rx.blocking_recv());

        assert_eq!(confirmation, UploadConfirmation::WorkerDropped);
        assert_eq!(
            pending_log_decision_after_upload(confirmation),
            PendingLogDecision::KeepForReplay
        );
    }

    #[test]
    fn compaction_finalize_applies_manifest_removals_with_output_add() {
        let inputs = vec![sstable("10", 100), sstable("11", 200)];
        let output = sstable("12", 4096);

        let plan = plan_manifest_update("ks.tbl", &inputs, &output, 1234);

        assert_eq!(plan.table_id, "ks.tbl");
        assert_eq!(plan.remove_input_ids, vec!["10", "11"]);
        assert_eq!(
            plan.removals_for_cas_retry,
            vec![(
                "ks.tbl".to_string(),
                vec!["10".to_string(), "11".to_string()]
            )]
        );
        assert_eq!(plan.add_output.id, "12");
        assert_eq!(plan.add_output.size, 1234);
        assert_eq!(plan.add_output.min_token, 10);
        assert_eq!(plan.add_output.max_token, 20);
        assert_eq!(plan.add_output.min_timestamp, 30);
        assert_eq!(plan.add_output.max_timestamp, 40);
    }

    #[test]
    fn compaction_finalize_does_not_block_on_input_deletion_completion() {
        let inputs = vec![sstable("10", 100), sstable("11", 200)];

        let plan = plan_input_deletions("ks.tbl", &inputs, Duration::from_secs(3600));

        assert_eq!(plan.tasks.len(), 2);
        assert_eq!(plan.tasks[0].table_id, "ks.tbl");
        assert_eq!(plan.tasks[0].sstable_id, "10");
        assert_eq!(plan.tasks[0].grace_period, Duration::from_secs(3600));
        assert!(
            plan.fire_and_forget,
            "input S3 deletions are best-effort and must not await completion"
        );
    }
}
