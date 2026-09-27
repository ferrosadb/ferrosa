//! Test-only readback checkpoint, scoped to a temporary directory.
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

type Hook = dyn Fn() -> ferrosa_common::Result<()> + Send + Sync;
type Registration = (PathBuf, Arc<Hook>);
fn hooks() -> &'static Mutex<Vec<Registration>> {
    static HOOKS: OnceLock<Mutex<Vec<Registration>>> = OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(Vec::new()))
}
pub(crate) struct Guard(PathBuf);
pub(crate) fn install(root: PathBuf, hook: Arc<Hook>) -> Guard {
    let mut registrations = hooks().lock().unwrap();
    assert!(registrations
        .iter()
        .all(|(path, _)| !root.starts_with(path) && !path.starts_with(&root)));
    registrations.push((root.clone(), hook));
    Guard(root)
}
pub(crate) fn for_path(path: &Path) -> Option<Arc<Hook>> {
    hooks()
        .lock()
        .unwrap()
        .iter()
        .find(|(root, _)| path.starts_with(root))
        .map(|(_, hook)| Arc::clone(hook))
}
impl Drop for Guard {
    fn drop(&mut self) {
        hooks().lock().unwrap().retain(|(root, _)| root != &self.0);
    }
}
