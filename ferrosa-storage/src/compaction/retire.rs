//! Hide input generations durably before reclaiming their component files.
//! Correctness: Data.db leaves discovery first; failed retirement keeps the intent.
//! Last revised: 2026-09-30
//! Last changed: Hold the generation guard; clear the input's eviction marker (ST-61).

use std::io;
use std::path::Path;

/// Returns false after a logged failure, so the caller retains its durable intent.
/// Directory iteration streams names; no component bytes or file list is materialized.
///
/// Holds the generation's guard for the whole retirement, so a concurrent S3
/// rehydrate of the same generation either finishes first or sees it retired.
/// May wait for an in-flight rehydrate of this generation to finish.
pub(crate) fn retire(path: &Path, generation: &str) -> bool {
    let root = if path.file_name().is_some_and(|name| name == generation) {
        path.parent().unwrap_or(path)
    } else {
        path
    };
    let slot = crate::generation_guard::slot(root, generation);
    let mut guard = slot.lock();
    let result = retire_inner(root, generation);
    if let Err(error) = result {
        tracing::warn!(%error, errno = ?error.raw_os_error(), path = %root.display(), generation,
            "compaction: input retirement failed; retaining intent for reconciliation");
        crate::metrics::inc_compaction_retire_failures();
        false
    } else {
        guard.mark_retired();
        true
    }
}

/// Removes the generation's `<gen>.evicted` marker, durably. The marker tells a
/// restart "evicted, restore it" from "compacted away, leave it gone"; once the
/// input is retired and its replacement published it is stale, and leaving it
/// makes every start log an ERROR for rows that are deliberately not served.
/// Runs last, after the components are gone, so a failure above keeps the marker.
fn clear_eviction_marker(root: &Path, generation: &str) -> io::Result<()> {
    let marker = crate::engine::StorageEngine::evicted_marker_path(root, generation);
    match std::fs::remove_file(&marker) {
        Ok(()) => sync_directory(root),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(test)]
    crate::flush::fsync_probe::note_dir(path);
    Ok(())
}

fn retire_inner(root: &Path, generation: &str) -> io::Result<()> {
    if !root.try_exists()? {
        return Ok(());
    }
    let live = root.join(generation);
    let retired = root.join(format!(".retired-{generation}"));
    if live.try_exists()? {
        rename_component(&live, &retired)?;
        sync_directory(root)?;
    }
    // A directory generation may also have legacy sidecars beside it.
    // Move Data.db before any other flat component: it is the discovery key.
    let data = root.join(format!("{generation}-Data.db"));
    if data.try_exists()? {
        std::fs::create_dir_all(&retired)?;
        rename_component(&data, &retired.join(data.file_name().unwrap()))?;
        sync_directory(&retired)?;
        sync_directory(root)?;
    }
    let prefix = format!("{generation}-");
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().starts_with(&prefix) {
            std::fs::create_dir_all(&retired)?;
            rename_component(&entry.path(), &retired.join(entry.file_name()))?;
        }
    }
    if retired.try_exists()? {
        sync_directory(&retired)?;
        sync_directory(root)?;
        remove_retired(&retired)?;
        sync_directory(root)?;
    }
    clear_eviction_marker(root, generation)
}

fn rename_component(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(test)]
    tests::before_rename(source)?;
    std::fs::rename(source, destination)
}

/// Single reclamation seam; a later retention policy can defer this removal.
fn remove_retired(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    crate::flush::fsync_probe::note_unlink(path);
    std::fs::remove_dir_all(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    type RetireInputHook = Rc<dyn Fn(&Path) -> io::Result<()>>;
    thread_local! {
        static RETIRE_INPUT: RefCell<Option<RetireInputHook>> = RefCell::new(None);
    }

    struct ScopedRetireInput(Option<RetireInputHook>);
    impl Drop for ScopedRetireInput {
        fn drop(&mut self) {
            RETIRE_INPUT.with(|slot| slot.replace(self.0.take()));
        }
    }

    pub(super) fn before_rename(path: &Path) -> io::Result<()> {
        let hook = RETIRE_INPUT.with(|slot| slot.borrow().clone());
        match hook {
            Some(hook) => hook(path),
            None => Ok(()),
        }
    }

    #[test]
    fn compaction_retire_component_failure_is_hidden_and_retryable() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("7-Data.db"), b"data").unwrap();
        std::fs::write(dir.path().join("7-title.sidecar"), b"index").unwrap();
        {
            let hook: RetireInputHook = Rc::new(|path| {
                if path
                    .extension()
                    .is_some_and(|extension| extension == "sidecar")
                {
                    Err(io::Error::from_raw_os_error(libc::EACCES))
                } else {
                    Ok(())
                }
            });
            let _scope = ScopedRetireInput(RETIRE_INPUT.with(|slot| slot.replace(Some(hook))));
            assert!(!retire(dir.path(), "7"));
            assert!(!dir.path().join("7-Data.db").exists());
            assert!(dir.path().join(".retired-7/7-Data.db").exists());
            assert!(dir.path().join("7-title.sidecar").exists());
        }
        assert!(retire(dir.path(), "7"));
        assert!(!dir.path().join(".retired-7").exists());
        assert!(!dir.path().join("7-title.sidecar").exists());
    }

    #[test]
    fn compaction_retire_failure_preserves_live_input_for_retry() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("7-Data.db"), b"live").unwrap();
        std::fs::write(dir.path().join(".retired-7"), b"obstruction").unwrap();
        assert!(!retire(dir.path(), "7"));
        assert!(dir.path().join("7-Data.db").exists());
        std::fs::remove_file(dir.path().join(".retired-7")).unwrap();
        assert!(retire(dir.path(), "7"));
        assert!(!dir.path().join("7-Data.db").exists());
        assert!(retire(dir.path(), "7"));
    }

    /// ST-61: an evicted input has no local files, so retiring it used to leave
    /// its marker behind and every later start logged an ERROR for it.
    #[test]
    fn retiring_an_evicted_input_clears_its_marker() {
        let dir = tempfile::tempdir().unwrap();
        let marker = crate::engine::StorageEngine::evicted_marker_path(dir.path(), "7");
        std::fs::write(&marker, b"").unwrap();
        let bystander = crate::engine::StorageEngine::evicted_marker_path(dir.path(), "8");
        std::fs::write(&bystander, b"").unwrap();

        assert!(retire(dir.path(), "7"));

        assert!(!marker.exists(), "the retired input's marker is stale");
        assert!(
            bystander.exists(),
            "another generation's marker is untouched"
        );
    }

    /// An input rehydrated for compaction keeps its marker (the read-through
    /// hook does not clear it) and has local files; both must go.
    #[test]
    fn retiring_a_rehydrated_input_clears_its_files_and_marker() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("7-Data.db"), b"data").unwrap();
        let marker = crate::engine::StorageEngine::evicted_marker_path(dir.path(), "7");
        std::fs::write(&marker, b"").unwrap();

        assert!(retire(dir.path(), "7"));

        assert!(!dir.path().join("7-Data.db").exists());
        assert!(!marker.exists());
    }

    /// A marker that cannot be removed fails the retirement loudly (false, so
    /// the caller keeps its durable intent and reconciliation retries it).
    #[test]
    fn a_marker_that_cannot_be_removed_fails_the_retirement() {
        let dir = tempfile::tempdir().unwrap();
        let marker = crate::engine::StorageEngine::evicted_marker_path(dir.path(), "7");
        std::fs::create_dir(&marker).unwrap();
        std::fs::write(marker.join("obstruction"), b"x").unwrap();

        assert!(!retire(dir.path(), "7"));
        assert!(marker.exists());
    }

    #[test]
    fn retire_blocks_while_the_generation_is_being_rehydrated() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("7-Data.db"), b"data").unwrap();
        let slot = crate::generation_guard::slot(dir.path(), "7");
        let in_flight = slot.lock();

        let root = dir.path().to_path_buf();
        let retiring = std::thread::spawn(move || retire(&root, "7"));
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert!(
            !retiring.is_finished(),
            "retirement must wait for the in-flight rehydrate"
        );
        assert!(dir.path().join("7-Data.db").exists());

        drop(in_flight);
        assert!(retiring.join().unwrap());
        assert!(!dir.path().join("7-Data.db").exists());
    }

    #[test]
    fn compaction_retire_resumes_after_data_moved() {
        let dir = tempfile::tempdir().unwrap();
        let retired = dir.path().join(".retired-7");
        std::fs::create_dir(&retired).unwrap();
        std::fs::write(retired.join("7-Data.db"), b"hidden").unwrap();
        std::fs::write(dir.path().join("7-title.sidecar"), b"remaining").unwrap();
        assert!(retire(dir.path(), "7"));
        assert!(!retired.exists());
        assert!(!dir.path().join("7-title.sidecar").exists());
    }
}
