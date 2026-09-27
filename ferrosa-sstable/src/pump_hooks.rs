//! Path-scoped, test-only injection at the pump's open boundary.
//! Correctness: a hook observes each open once and never leaks into another root.
//! Last revised: 2026-09-26
//! Last changed: Added open-time sink wrapping for engine wiring/backpressure tests.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use super::{DirectMode, PumpConfig, SegmentSink, MAX_QUEUE_DEPTH};

/// Optional per-root scheduling overrides; component pumps retain depth zero.
#[derive(Clone, Copy, Debug, Default)]
pub struct PumpOverrides {
    pub segment_bytes: Option<usize>,
    pub queue_depth: Option<usize>,
}

/// The effective configuration passed to an opened pump.
#[derive(Clone, Debug)]
pub struct PumpOpen {
    pub path: PathBuf,
    pub block: usize,
    pub segment: usize,
    pub depth: usize,
    pub mode: DirectMode,
}

/// Wrap a real sink to inject gates/faults or inspect physical operations.
pub type SinkHook = dyn Fn(&PumpOpen, Box<dyn SegmentSink>) -> Box<dyn SegmentSink> + Send + Sync;

struct Registration {
    root: PathBuf,
    overrides: PumpOverrides,
    hook: Arc<SinkHook>,
}

fn registrations() -> &'static Mutex<Vec<Registration>> {
    static HOOKS: OnceLock<Mutex<Vec<Registration>>> = OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Unregisters the root at scope exit, including during unwinding.
pub struct SinkHookGuard {
    root: PathBuf,
}

/// Install a hook for a unique directory tree. Overlapping roots are rejected
/// so parallel tests cannot capture each other's component files.
pub fn install_sink_hook(
    root: PathBuf,
    overrides: PumpOverrides,
    hook: Arc<SinkHook>,
) -> SinkHookGuard {
    assert!(overrides.queue_depth.is_none_or(|n| n <= MAX_QUEUE_DEPTH));
    assert!(overrides.segment_bytes.is_none_or(|n| n > 0));
    let mut registered = registrations().lock().unwrap_or_else(|p| p.into_inner());
    assert!(
        registered
            .iter()
            .all(|r| !root.starts_with(&r.root) && !r.root.starts_with(&root)),
        "pump hook roots must not overlap"
    );
    registered.push(Registration {
        root: root.clone(),
        overrides,
        hook,
    });
    SinkHookGuard { root }
}

impl Drop for SinkHookGuard {
    fn drop(&mut self) {
        registrations()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|r| r.root != self.root);
    }
}

pub(super) fn prepare(
    sink: Box<dyn SegmentSink>,
    path: &Path,
    block: usize,
    segment: usize,
    depth: usize,
    apply_depth: bool,
) -> (Box<dyn SegmentSink>, usize, usize) {
    let registration = registrations()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .find(|r| path.starts_with(&r.root))
        .map(|r| (r.overrides, Arc::clone(&r.hook)));
    let Some((overrides, hook)) = registration else {
        return (sink, segment, depth);
    };
    let segment = overrides.segment_bytes.map_or(segment, |segment_bytes| {
        PumpConfig {
            segment_bytes,
            queue_depth: depth,
        }
        .effective_segment(block)
    });
    let depth = if apply_depth {
        overrides.queue_depth.unwrap_or(depth)
    } else {
        depth
    };
    let opened = PumpOpen {
        path: path.to_path_buf(),
        block,
        segment,
        depth,
        mode: sink.mode(),
    };
    (hook(&opened, sink), segment, depth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pump::test_support::RecordingSink;
    use crate::pump::{AlignedPump, NeverAbort};

    #[test]
    fn wiring_sink_hook_is_scoped_and_runs_once_per_open() {
        let dir = tempfile::tempdir().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&seen);
        let guard = install_sink_hook(
            dir.path().to_path_buf(),
            PumpOverrides {
                segment_bytes: Some(8192),
                queue_depth: Some(1),
            },
            Arc::new(move |opened, sink| {
                observed.lock().unwrap().push(opened.clone());
                sink
            }),
        );
        let (sink, _) = RecordingSink::new(DirectMode::Buffered);
        let mut data = AlignedPump::open_with_depth(
            Box::new(sink),
            4096,
            4096,
            dir.path().join("Data.db"),
            2,
            Arc::new(NeverAbort::new()),
        );
        data.write_all(&[7; 16384]).unwrap();
        data.finish().unwrap();
        let (sink, _) = RecordingSink::new(DirectMode::Buffered);
        AlignedPump::open(Box::new(sink), 4096, 4096, dir.path().join("Rows.db"))
            .finish()
            .unwrap();
        assert_eq!(seen.lock().unwrap().len(), 2);
        assert_eq!(seen.lock().unwrap()[0].depth, 1);
        assert_eq!(seen.lock().unwrap()[1].depth, 0);
        assert!(seen.lock().unwrap().iter().all(|open| open.segment == 8192));
        drop(guard);
        let (sink, _) = RecordingSink::new(DirectMode::Buffered);
        AlignedPump::open(Box::new(sink), 4096, 4096, dir.path().join("Filter.db"))
            .finish()
            .unwrap();
        assert_eq!(
            seen.lock().unwrap().len(),
            2,
            "dropped hook must not be reused"
        );
    }
}
