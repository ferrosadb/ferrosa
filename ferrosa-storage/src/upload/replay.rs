use std::path::Path;

use super::{SstableComponentFile, UploadTask};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingUploadReplaySubmit {
    Submitted,
    QueueFull,
    Failed(String),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PendingUploadReplayReport {
    pub submitted: usize,
    pub queue_full_at: Option<String>,
    pub remaining_entries: usize,
    pub missing_files: Vec<(String, String)>,
    pub submit_failures: Vec<(String, String, String)>,
}

pub fn replay_pending_upload_entries<F>(
    entries: &[(String, String)],
    data_dir: &Path,
    compaction_output_dir: &Path,
    mut submit: F,
) -> PendingUploadReplayReport
where
    F: FnMut(UploadTask) -> PendingUploadReplaySubmit,
{
    let mut report = PendingUploadReplayReport::default();

    for (idx, (table_id, sstable_id)) in entries.iter().enumerate() {
        let Some(files) =
            find_pending_upload_files(data_dir, compaction_output_dir, table_id, sstable_id)
        else {
            report
                .missing_files
                .push((table_id.clone(), sstable_id.clone()));
            report.remaining_entries += 1;
            continue;
        };

        let task = UploadTask::SSTable {
            table_id: table_id.clone(),
            sstable_id: sstable_id.clone(),
            files,
            on_complete: None,
        };

        match submit(task) {
            PendingUploadReplaySubmit::Submitted => {
                report.submitted += 1;
            }
            PendingUploadReplaySubmit::QueueFull => {
                report.queue_full_at = Some(sstable_id.clone());
                report.remaining_entries += entries.len() - idx;
                break;
            }
            PendingUploadReplaySubmit::Failed(reason) => {
                report
                    .submit_failures
                    .push((table_id.clone(), sstable_id.clone(), reason));
                report.remaining_entries += 1;
            }
        }
    }

    report
}

pub(crate) fn find_pending_upload_files(
    data_dir: &Path,
    compaction_output_dir: &Path,
    table_id: &str,
    sstable_id: &str,
) -> Option<Vec<SstableComponentFile>> {
    let gen = sstable_id.parse().unwrap_or(0);
    let flush_dir = data_dir.join("sstables").join(table_id);
    let files = collect_sstable_files(&flush_dir, gen);
    if !files.is_empty() {
        return Some(files);
    }

    let compaction_dir = compaction_output_dir.join(table_id);
    let files = collect_sstable_files(&compaction_dir, gen);
    if files.is_empty() {
        None
    } else {
        Some(files)
    }
}

fn collect_sstable_files(table_dir: &Path, gen: u64) -> Vec<SstableComponentFile> {
    let gen_str = gen.to_string();
    let generation_dir = table_dir.join(&gen_str);
    if generation_dir.join(format!("{gen_str}-Data.db")).exists() {
        return collect_sstable_files_in_dir(&generation_dir, &gen_str);
    }

    collect_sstable_files_in_dir(table_dir, &gen_str)
}

fn collect_sstable_files_in_dir(dir: &Path, gen_str: &str) -> Vec<SstableComponentFile> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_str()?.to_string();
            if name.starts_with(&format!("{gen_str}-")) {
                Some(SstableComponentFile::new(name, e.path()))
            } else {
                None
            }
        })
        .collect();
    files.sort_by(|left, right| left.name.cmp(&right.name));
    files
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::upload::{PendingUploadsLog, UploadTask};

    fn write_component(root: &Path, table_id: &str, sstable_id: &str) {
        let table_dir = root.join(table_id);
        std::fs::create_dir_all(&table_dir).unwrap();
        std::fs::write(table_dir.join(format!("{sstable_id}-Data.db")), b"sstable").unwrap();
    }

    fn write_component_in_generation_dir(root: &Path, table_id: &str, sstable_id: &str) {
        let generation_dir = root.join(table_id).join(sstable_id);
        std::fs::create_dir_all(&generation_dir).unwrap();
        std::fs::write(
            generation_dir.join(format!("{sstable_id}-Data.db")),
            b"sstable",
        )
        .unwrap();
    }

    #[test]
    fn pending_upload_replay_stops_at_queue_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let compaction_dir = dir.path().join("compaction");
        let table_id = "test_keyspace.test_table";
        for sstable_id in ["1", "2", "3"] {
            write_component(&data_dir.join("sstables"), table_id, sstable_id);
        }
        let entries = vec![
            (table_id.to_string(), "1".to_string()),
            (table_id.to_string(), "2".to_string()),
            (table_id.to_string(), "3".to_string()),
        ];

        let mut submitted = Vec::new();
        let report = replay_pending_upload_entries(&entries, &data_dir, &compaction_dir, |task| {
            let UploadTask::SSTable { sstable_id, .. } = task else {
                panic!("unexpected upload task");
            };
            submitted.push(sstable_id);
            if submitted.len() == 1 {
                PendingUploadReplaySubmit::Submitted
            } else {
                PendingUploadReplaySubmit::QueueFull
            }
        });

        assert_eq!(submitted, vec!["1", "2"]);
        assert_eq!(report.submitted, 1);
        assert_eq!(report.queue_full_at.as_deref(), Some("2"));
        assert_eq!(report.remaining_entries, 2);
        assert!(report.missing_files.is_empty());
    }

    #[test]
    fn pending_upload_replay_leaves_log_entries_when_queue_full() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let compaction_dir = dir.path().join("compaction");
        let table_id = "test_keyspace.test_table";
        for sstable_id in ["1", "2"] {
            write_component(&data_dir.join("sstables"), table_id, sstable_id);
        }
        let log = PendingUploadsLog::open(&data_dir.join("pending-uploads.log")).unwrap();
        log.add_entry(table_id, "1").unwrap();
        log.add_entry(table_id, "2").unwrap();
        let entries = log.pending_entries().unwrap();

        let report = replay_pending_upload_entries(&entries, &data_dir, &compaction_dir, |_task| {
            PendingUploadReplaySubmit::QueueFull
        });

        assert_eq!(report.submitted, 0);
        assert_eq!(report.remaining_entries, 2);
        assert_eq!(log.pending_entries().unwrap(), entries);
    }

    #[test]
    fn pending_upload_replay_reports_missing_sstable_files() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let compaction_dir = dir.path().join("compaction");
        let entries = vec![("test_keyspace.test_table".to_string(), "99".to_string())];
        let mut submitted = 0;

        let report = replay_pending_upload_entries(&entries, &data_dir, &compaction_dir, |_task| {
            submitted += 1;
            PendingUploadReplaySubmit::Submitted
        });

        assert_eq!(submitted, 0);
        assert_eq!(report.submitted, 0);
        assert_eq!(report.missing_files, entries);
        assert_eq!(report.remaining_entries, 1);
    }

    #[test]
    fn pending_upload_replay_submits_compaction_output_when_flush_dir_missing() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let compaction_dir = dir.path().join("compaction");
        let table_id = "test_keyspace.test_table";
        write_component(&compaction_dir, table_id, "7");
        let entries = vec![(table_id.to_string(), "7".to_string())];
        let mut submitted_file_names = Vec::new();

        let report = replay_pending_upload_entries(&entries, &data_dir, &compaction_dir, |task| {
            let UploadTask::SSTable { files, .. } = task else {
                panic!("unexpected upload task");
            };
            submitted_file_names.extend(files.into_iter().map(|file| file.name));
            PendingUploadReplaySubmit::Submitted
        });

        assert_eq!(report.submitted, 1);
        assert_eq!(submitted_file_names, vec!["7-Data.db"]);
        assert!(report.missing_files.is_empty());
        assert_eq!(report.remaining_entries, 0);
    }

    #[test]
    fn pending_upload_replay_submits_generation_directory_components() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let compaction_dir = dir.path().join("compaction");
        let table_id = "test_keyspace.test_table";
        write_component_in_generation_dir(&data_dir.join("sstables"), table_id, "7");
        let entries = vec![(table_id.to_string(), "7".to_string())];
        let mut submitted_file_names = Vec::new();

        let report = replay_pending_upload_entries(&entries, &data_dir, &compaction_dir, |task| {
            let UploadTask::SSTable { files, .. } = task else {
                panic!("unexpected upload task");
            };
            submitted_file_names.extend(files.into_iter().map(|file| file.name));
            PendingUploadReplaySubmit::Submitted
        });

        assert_eq!(report.submitted, 1);
        assert_eq!(submitted_file_names, vec!["7-Data.db"]);
        assert!(report.missing_files.is_empty());
        assert_eq!(report.remaining_entries, 0);
    }
}
