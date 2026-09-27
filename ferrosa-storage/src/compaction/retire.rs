//! Hide input generations durably before reclaiming their component files.
//! Correctness: Data.db leaves discovery first; failed retirement keeps the intent.
//! Last revised: 2026-09-26
//! Last changed: Retire generation directories and flat sidecars with durable renames.

use std::io;
use std::path::Path;

/// Returns false after a logged failure, so the caller retains its durable intent.
/// Directory iteration streams names; no component bytes or file list is materialized.
pub(crate) fn retire(path: &Path, generation: &str) -> bool {
    let root = if path.file_name().is_some_and(|name| name == generation) {
        path.parent().unwrap_or(path)
    } else {
        path
    };
    let result = retire_inner(root, generation);
    if let Err(error) = result {
        tracing::warn!(%error, errno = ?error.raw_os_error(), path = %root.display(), generation,
            "compaction: input retirement failed; retaining intent for reconciliation");
        crate::metrics::inc_compaction_retire_failures();
        false
    } else {
        true
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
    Ok(())
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
