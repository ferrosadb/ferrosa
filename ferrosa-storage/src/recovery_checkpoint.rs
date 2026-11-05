//! Physical checkpoint staging with identity-bound file verification.
//! Correctness: staging never changes the preserved local root or grants live admission.
//! Last revised: 2026-11-05
//! Last changed: Establish typed staging boundary and real SSTable contract tests.

use ferrosa_common::{schema::TableSchema, Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Trusted Raft context supplied by the repair producer, not inferred from file timestamps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointAnchor {
    pub group_id: Uuid,
    pub term: u64,
    pub index: u64,
}

/// A table incarnation and the schema required to interpret its physical files.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointTable {
    pub incarnation: Uuid,
    pub schema: TableSchema,
}

/// One immutable physical file, addressed relative to the checkpoint root.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointFile {
    pub relative_path: String,
    pub len: u64,
    pub sha256: [u8; 32],
}

/// Versioned physical inventory bound to an explicit logical anchor.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointManifest {
    pub version: u32,
    pub anchor: CheckpointAnchor,
    pub tables: Vec<CheckpointTable>,
    pub files: Vec<CheckpointFile>,
}

/// Explicit caller policy; limit exhaustion refuses rather than truncating a checkpoint.
#[derive(Clone, Copy, Debug)]
pub struct CheckpointLimits {
    pub max_manifest_bytes: usize,
    pub max_files: usize,
    pub max_file_bytes: u64,
    pub max_total_bytes: u64,
}

/// Evidence about a fresh staged root only. It cannot authorize live admission.
#[derive(Debug)]
pub struct VerifiedStagedCheckpoint {
    root: PathBuf,
    anchor: CheckpointAnchor,
    manifest_digest: [u8; 32],
}
impl VerifiedStagedCheckpoint {
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn anchor(&self) -> CheckpointAnchor {
        self.anchor
    }
    pub fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidFormat(message.into())
}

use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
};

struct WriterLease(File);
impl Drop for WriterLease {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.0);
    }
}
fn open_regular(path: &Path, write: bool, fresh: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(!write).write(write).create_new(fresh);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("checkpoint requires regular files"));
    }
    Ok(file)
}
fn digest_file(path: &Path, expected: &CheckpointFile) -> Result<()> {
    let mut file = open_regular(path, false, false)?;
    if file.metadata()?.len() != expected.len {
        return Err(invalid("checkpoint file length mismatch"));
    }
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    let mut count = 0u64;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        count = count
            .checked_add(n as u64)
            .ok_or_else(|| invalid("checkpoint length overflow"))?;
        if count > expected.len {
            return Err(invalid("checkpoint grew during verification"));
        }
        hash.update(&buffer[..n]);
    }
    let actual: [u8; 32] = hash.finalize().into();
    if count != expected.len || actual != expected.sha256 {
        return Err(invalid("checkpoint digest mismatch"));
    }
    Ok(())
}
struct BoundedBytes {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("checkpoint manifest limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn manifest_bytes(manifest: &CheckpointManifest, limits: CheckpointLimits) -> Result<Vec<u8>> {
    let mut out = BoundedBytes {
        bytes: Vec::new(),
        limit: limits.max_manifest_bytes,
    };
    serde_json::to_writer(&mut out, manifest).map_err(|e| invalid(e.to_string()))?;
    // Bound before constructing a canonical metadata tree; never materialize file contents.
    let mut value: serde_json::Value =
        serde_json::from_slice(&out.bytes).map_err(|e| invalid(e.to_string()))?;
    fn sort(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                map.sort_keys();
                for child in map.values_mut() {
                    sort(child);
                }
            }
            serde_json::Value::Array(values) => {
                for child in values {
                    sort(child);
                }
            }
            _ => {}
        }
    }
    sort(&mut value);
    serde_json::to_vec(&value).map_err(|e| invalid(e.to_string()))
}
fn validate_manifest(manifest: &CheckpointManifest, limits: CheckpointLimits) -> Result<()> {
    if manifest.version != 1 || manifest.files.len() > limits.max_files {
        return Err(invalid("unsupported checkpoint version or file limit"));
    }
    let mut tables = BTreeSet::new();
    let mut incarnations = BTreeSet::new();
    for table in &manifest.tables {
        let name = format!("{}.{}", table.schema.keyspace, table.schema.table);
        if name.contains('/')
            || name.contains('\\')
            || !tables.insert(name)
            || !incarnations.insert(table.incarnation)
        {
            return Err(invalid("invalid or duplicate checkpoint table"));
        }
    }
    let mut paths = BTreeSet::new();
    let mut total = 0u64;
    for file in &manifest.files {
        let path = Path::new(&file.relative_path);
        let components: Vec<_> = path.components().collect();
        if components.len() != 3
            || components
                .iter()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
            || components[0].as_os_str() != "sstables"
            || !tables.contains(&components[1].as_os_str().to_string_lossy().into_owned())
            || !paths.insert(file.relative_path.clone())
            || file.relative_path.contains('\\')
        {
            return Err(invalid("invalid or duplicate checkpoint path"));
        }
        total = total
            .checked_add(file.len)
            .ok_or_else(|| invalid("checkpoint size overflow"))?;
        if file.len > limits.max_file_bytes || total > limits.max_total_bytes {
            return Err(invalid("checkpoint size limit"));
        }
    }
    Ok(())
}
fn checked_parent(root: &Path, relative: &Path, create: bool) -> Result<()> {
    let mut path = root.to_path_buf();
    for component in relative
        .parent()
        .ok_or_else(|| invalid("missing file parent"))?
        .components()
    {
        path.push(component);
        if create && !path.exists() {
            fs::create_dir(&path)?;
            File::open(path.parent().unwrap())?.sync_all()?;
        }
        let meta = fs::symlink_metadata(&path)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(invalid("checkpoint path contains non-directory"));
        }
    }
    Ok(())
}
fn stage(
    manifest: &CheckpointManifest,
    incoming: &Path,
    destination: &Path,
    local: &Path,
    limits: CheckpointLimits,
    resume: bool,
) -> Result<VerifiedStagedCheckpoint> {
    let bytes = manifest_bytes(manifest, limits)?;
    validate_manifest(manifest, limits)?;
    let incoming = incoming.canonicalize()?;
    let local = local.canonicalize()?;
    // Resolve and reject overlap before creating anything beneath either input root.
    let root = if resume {
        if fs::symlink_metadata(destination)?.file_type().is_symlink() {
            return Err(invalid("symlink stage root"));
        }
        destination.canonicalize()?
    } else {
        let parent = destination
            .parent()
            .ok_or_else(|| invalid("missing stage parent"))?
            .canonicalize()?;
        let name = destination
            .file_name()
            .ok_or_else(|| invalid("missing stage name"))?;
        parent.join(name)
    };
    if root.starts_with(&local)
        || local.starts_with(&root)
        || root.starts_with(&incoming)
        || incoming.starts_with(&root)
    {
        return Err(invalid("checkpoint roots overlap"));
    }
    if !resume {
        fs::create_dir(&root)?;
        File::open(
            root.parent()
                .ok_or_else(|| invalid("missing stage parent"))?,
        )?
        .sync_all()?;
    }
    let lock_path = root.join(".checkpoint-writer.lock");
    let lock = match open_regular(&lock_path, true, true) {
        Ok(file) => file,
        Err(_) if resume => open_regular(&lock_path, true, false)?,
        Err(error) => return Err(error),
    };
    fs2::FileExt::try_lock_exclusive(&lock)?;
    let _lease = WriterLease(lock);
    let identity = root.join(".checkpoint-manifest.json");
    if resume {
        let file = open_regular(&identity, false, false)?;
        let mut stored = Vec::new();
        file.take((limits.max_manifest_bytes as u64).saturating_add(1))
            .read_to_end(&mut stored)?;
        if stored != bytes {
            return Err(invalid("checkpoint stage identity mismatch"));
        }
    } else {
        let mut file = open_regular(&identity, true, true)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        File::open(&root)?.sync_all()?;
    }
    for expected in &manifest.files {
        let relative = Path::new(&expected.relative_path);
        checked_parent(&incoming, relative, false)?;
        checked_parent(&root, relative, true)?;
        let output = root.join(relative);
        let temporary = output.with_file_name(format!(
            "{}.checkpoint-part",
            output.file_name().unwrap().to_string_lossy()
        ));
        if output.exists() {
            digest_file(&output, expected)?;
            if temporary.exists() {
                // A crash may leave both names after no-overwrite promotion. Never remove
                // an unexpected temporary until its actual content matches this identity.
                digest_file(&temporary, expected)?;
                fs::remove_file(&temporary)?;
                File::open(output.parent().unwrap())?.sync_all()?;
            }
            continue;
        }
        // A failed copy is never overwritten. Its private temporary is discarded under the stage lease.
        if temporary.exists() {
            let _ = open_regular(&temporary, false, false)?;
            fs::remove_file(&temporary)?;
        }
        let mut source = open_regular(&incoming.join(relative), false, false)?;
        let mut target = open_regular(&temporary, true, true)?;
        let copied = std::io::copy(
            &mut Read::by_ref(&mut source).take(expected.len.saturating_add(1)),
            &mut target,
        )?;
        target.sync_all()?;
        if copied != expected.len {
            return Err(invalid("checkpoint source length changed"));
        }
        digest_file(&temporary, expected)?;
        fs::hard_link(&temporary, &output)?;
        fs::remove_file(&temporary)?;
        File::open(output.parent().unwrap())?.sync_all()?;
    }
    for expected in &manifest.files {
        digest_file(&root.join(&expected.relative_path), expected)?;
    }
    // Reject unlisted payloads: opening this root later must not discover hidden generations.
    let expected_paths: BTreeSet<_> = manifest
        .files
        .iter()
        .map(|file| file.relative_path.as_str())
        .collect();
    let payload = root.join("sstables");
    if payload.exists() {
        for table in fs::read_dir(&payload)? {
            let table = table?;
            if !table.file_type()?.is_dir() {
                return Err(invalid("unexpected checkpoint payload entry"));
            }
            for entry in fs::read_dir(table.path())? {
                let entry = entry?;
                let relative = entry
                    .path()
                    .strip_prefix(&root)
                    .map_err(|e| invalid(e.to_string()))?
                    .to_string_lossy()
                    .into_owned();
                if !entry.file_type()?.is_file() || !expected_paths.contains(relative.as_str()) {
                    return Err(invalid("unlisted checkpoint payload"));
                }
            }
        }
    }
    let mut generations = BTreeSet::new();
    for expected in &manifest.files {
        let path = root.join(&expected.relative_path);
        let generation = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.split_once('-'))
            .and_then(|(prefix, _)| prefix.parse::<u64>().ok())
            .ok_or_else(|| invalid("unsupported checkpoint component name"))?;
        generations.insert((path.parent().unwrap().to_path_buf(), generation));
    }
    // Inspect every referenced generation, including one whose Data component is absent.
    for (directory, generation) in generations {
        crate::StorageEngine::smoke_test_generation(&directory, generation)?;
    }
    Ok(VerifiedStagedCheckpoint {
        root,
        anchor: manifest.anchor,
        manifest_digest: Sha256::digest(&bytes).into(),
    })
}
/// Blocking fresh-root staging. The preserved local root is never modified.
pub fn stage_checkpoint(
    manifest: &CheckpointManifest,
    incoming: &Path,
    destination: &Path,
    preserved_local_root: &Path,
    limits: CheckpointLimits,
) -> Result<VerifiedStagedCheckpoint> {
    stage(
        manifest,
        incoming,
        destination,
        preserved_local_root,
        limits,
        false,
    )
}
/// Resume only the same durable stage identity, rechecking actual files before returning evidence.
pub fn resume_checkpoint_stage(
    manifest: &CheckpointManifest,
    incoming: &Path,
    destination: &Path,
    preserved_local_root: &Path,
    limits: CheckpointLimits,
) -> Result<VerifiedStagedCheckpoint> {
    stage(
        manifest,
        incoming,
        destination,
        preserved_local_root,
        limits,
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flush::{build_serialization_header, FileFlushTarget, FlushTarget};
    use ferrosa_common::{schema::ColumnDefinition, CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::{
        types::{DeletionTime, LivenessInfo, Partition, Row},
        writer::SSTableWriter,
        WriteOptions,
    };
    use sha2::{Digest, Sha256};

    fn schema() -> TableSchema {
        TableSchema {
            keyspace: "ks".into(),
            table: "t".into(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            regular_columns: vec![ColumnDefinition {
                name: "v".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
            ..TableSchema::default()
        }
    }
    fn write_table(root: &Path, value: &[u8], timestamp: i64) -> u64 {
        let table = root.join("sstables/ks.t");
        std::fs::create_dir_all(&table).unwrap();
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::new(b"k".to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            }],
        };
        let header = build_serialization_header(&schema(), std::slice::from_ref(&partition));
        let mut writer = SSTableWriter::new(
            WriteOptions {
                compression: None,
                ..WriteOptions::default()
            },
            header,
        );
        writer.add_partition(&partition).unwrap();
        let target = FileFlushTarget::new(table).unwrap();
        target.flush(writer.finish().unwrap()).unwrap();
        target.generation()
    }
    fn manifest(root: &Path) -> CheckpointManifest {
        let mut files: Vec<_> = std::fs::read_dir(root.join("sstables/ks.t"))
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let bytes = std::fs::read(&path).unwrap();
                CheckpointFile {
                    relative_path: path
                        .strip_prefix(root)
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_owned(),
                    len: bytes.len() as u64,
                    sha256: Sha256::digest(&bytes).into(),
                }
            })
            .collect();
        files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        CheckpointManifest {
            version: 1,
            anchor: CheckpointAnchor {
                group_id: Uuid::from_u128(1),
                term: 2,
                index: 3,
            },
            tables: vec![CheckpointTable {
                incarnation: Uuid::from_u128(4),
                schema: schema(),
            }],
            files,
        }
    }
    fn limits() -> CheckpointLimits {
        CheckpointLimits {
            max_manifest_bytes: 1 << 20,
            max_files: 64,
            max_file_bytes: 1 << 24,
            max_total_bytes: 1 << 26,
        }
    }

    #[test]
    fn checkpoint_stage_verifies_real_sstables_and_preserves_newer_local_files() {
        let dir = tempfile::tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        let local = dir.path().join("local");
        let destination = dir.path().join("stage");
        let peer_generation = write_table(&incoming, b"peer-old", 10);
        let local_generation = write_table(&local, b"local-new", 100);
        let local_before = manifest(&local);
        let expected = manifest(&incoming);
        let stage = stage_checkpoint(&expected, &incoming, &destination, &local, limits())
            .expect("valid peer checkpoint must stage");
        assert_eq!(stage.anchor(), expected.anchor);
        assert_ne!(stage.manifest_digest(), [0; 32]);
        crate::StorageEngine::smoke_test_generation(
            &stage.root().join("sstables/ks.t"),
            peer_generation,
        )
        .unwrap();
        crate::StorageEngine::smoke_test_generation(&local.join("sstables/ks.t"), local_generation)
            .unwrap();
        let after = manifest(&local);
        assert_eq!(
            serde_json::to_value(local_before.files).unwrap(),
            serde_json::to_value(after.files).unwrap(),
            "newer local files must remain byte-for-byte unchanged"
        );
    }

    #[test]
    fn checkpoint_stage_reopens_same_identity_and_refuses_a_different_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        let local = dir.path().join("local");
        let destination = dir.path().join("stage");
        write_table(&incoming, b"peer", 10);
        std::fs::create_dir(&local).unwrap();
        let expected = manifest(&incoming);
        let first = stage_checkpoint(&expected, &incoming, &destination, &local, limits())
            .expect("initial stage");
        let digest = first.manifest_digest();
        drop(first);
        let resumed = resume_checkpoint_stage(&expected, &incoming, &destination, &local, limits())
            .expect("same checkpoint survives restart");
        assert_eq!(resumed.manifest_digest(), digest);
        let mut wrong = expected.clone();
        wrong.anchor.index += 1;
        assert!(
            resume_checkpoint_stage(&wrong, &incoming, &destination, &local, limits()).is_err()
        );
    }
    #[test]
    fn checkpoint_stage_refuses_missing_data_component_even_when_manifest_matches() {
        let dir = tempfile::tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        let local = dir.path().join("local");
        std::fs::create_dir(&local).unwrap();
        let generation = write_table(&incoming, b"peer", 10);
        std::fs::remove_file(incoming.join(format!("sstables/ks.t/{generation}-Data.db"))).unwrap();
        let expected = manifest(&incoming);
        assert!(
            stage_checkpoint(
                &expected,
                &incoming,
                &dir.path().join("stage"),
                &local,
                limits()
            )
            .is_err(),
            "an internally incomplete checkpoint cannot mint staged evidence"
        );
    }

    #[test]
    fn checkpoint_stage_resumes_crash_after_file_promotion_before_temp_unlink() {
        let dir = tempfile::tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        let local = dir.path().join("local");
        let destination = dir.path().join("stage");
        std::fs::create_dir(&local).unwrap();
        write_table(&incoming, b"peer", 10);
        let expected = manifest(&incoming);
        stage_checkpoint(&expected, &incoming, &destination, &local, limits()).unwrap();
        let target = destination.join(&expected.files[0].relative_path);
        let partial = target.with_file_name(format!(
            "{}.checkpoint-part",
            target.file_name().unwrap().to_string_lossy()
        ));
        std::fs::hard_link(&target, &partial).unwrap();
        resume_checkpoint_stage(&expected, &incoming, &destination, &local, limits())
            .expect("durable promoted file survives restart before unlink");
        assert!(!partial.exists());
    }

    #[test]
    fn checkpoint_stage_digest_failure_can_resume_without_replacing_local_files() {
        let dir = tempfile::tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        let local = dir.path().join("local");
        let destination = dir.path().join("stage");
        write_table(&incoming, b"peer", 10);
        write_table(&local, b"newer-local", 100);
        let local_before = manifest(&local);
        let expected = manifest(&incoming);
        let source = incoming.join(&expected.files[0].relative_path);
        let valid = std::fs::read(&source).unwrap();
        std::fs::write(&source, b"damaged").unwrap();
        assert!(stage_checkpoint(&expected, &incoming, &destination, &local, limits()).is_err());
        std::fs::write(&source, valid).unwrap();
        resume_checkpoint_stage(&expected, &incoming, &destination, &local, limits()).unwrap();
        assert_eq!(
            serde_json::to_value(local_before.files).unwrap(),
            serde_json::to_value(manifest(&local).files).unwrap()
        );
        let collision = destination.join(&expected.files[0].relative_path);
        std::fs::write(&collision, b"unrelated-collision").unwrap();
        assert!(
            resume_checkpoint_stage(&expected, &incoming, &destination, &local, limits()).is_err()
        );
        assert_eq!(std::fs::read(collision).unwrap(), b"unrelated-collision");
    }
    #[test]
    fn checkpoint_stage_refuses_overlap_before_touching_preserved_local_root() {
        let dir = tempfile::tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        let local = dir.path().join("local");
        write_table(&incoming, b"peer", 10);
        write_table(&local, b"local", 100);
        let destination = local.join("must-not-create");
        assert!(stage_checkpoint(
            &manifest(&incoming),
            &incoming,
            &destination,
            &local,
            limits()
        )
        .is_err());
        assert!(
            !destination.exists(),
            "invalid overlap must not mutate preserved local root"
        );
    }
    #[test]
    fn checkpoint_stage_refuses_limits_and_unlisted_payload_without_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        let local = dir.path().join("local");
        let destination = dir.path().join("stage");
        std::fs::create_dir(&local).unwrap();
        write_table(&incoming, b"peer", 10);
        let expected = manifest(&incoming);
        let mut bounded = limits();
        bounded.max_files = 0;
        assert!(stage_checkpoint(&expected, &incoming, &destination, &local, bounded).is_err());
        assert!(!destination.exists());
        stage_checkpoint(&expected, &incoming, &destination, &local, limits()).unwrap();
        let extra = destination.join("sstables/ks.t/999-Data.db");
        std::fs::write(&extra, b"foreign").unwrap();
        assert!(
            resume_checkpoint_stage(&expected, &incoming, &destination, &local, limits()).is_err()
        );
        assert_eq!(std::fs::read(extra).unwrap(), b"foreign");
    }

    #[test]
    fn checkpoint_stage_exclusive_writer_lease_refuses_competing_resume() {
        let dir = tempfile::tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        let local = dir.path().join("local");
        let destination = dir.path().join("stage");
        std::fs::create_dir(&local).unwrap();
        write_table(&incoming, b"peer", 10);
        let expected = manifest(&incoming);
        stage_checkpoint(&expected, &incoming, &destination, &local, limits()).unwrap();
        let lock = open_regular(&destination.join(".checkpoint-writer.lock"), true, false).unwrap();
        fs2::FileExt::try_lock_exclusive(&lock).unwrap();
        let lease = WriterLease(lock);
        assert!(
            resume_checkpoint_stage(&expected, &incoming, &destination, &local, limits()).is_err()
        );
        drop(lease);
        resume_checkpoint_stage(&expected, &incoming, &destination, &local, limits()).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn checkpoint_stage_refuses_source_symlink_and_manifest_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        let local = dir.path().join("local");
        std::fs::create_dir(&local).unwrap();
        write_table(&incoming, b"peer", 10);
        let expected = manifest(&incoming);
        let mut traversal = expected.clone();
        traversal.files[0].relative_path = "sstables/../outside".into();
        assert!(stage_checkpoint(
            &traversal,
            &incoming,
            &dir.path().join("bad-path"),
            &local,
            limits()
        )
        .is_err());
        let source = incoming.join(&expected.files[0].relative_path);
        let held = dir.path().join("held-source");
        std::fs::rename(&source, &held).unwrap();
        std::os::unix::fs::symlink(&held, &source).unwrap();
        assert!(stage_checkpoint(
            &expected,
            &incoming,
            &dir.path().join("bad-link"),
            &local,
            limits()
        )
        .is_err());
    }
}
