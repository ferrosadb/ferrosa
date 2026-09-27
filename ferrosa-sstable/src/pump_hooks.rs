//! Path-scoped, test-only injection at the pump's open boundary.
//! Correctness: a hook observes each open once and never leaks into another root.
//! Last revised: 2026-09-26
//! Last changed: Added open-time sink wrapping for engine wiring/backpressure tests.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use super::{max_queue_depth_from_env, DirectMode, PumpConfig, SegmentSink};

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

type BypassHook = dyn Fn(&Path) + Send + Sync;

struct Registration {
    root: PathBuf,
    overrides: PumpOverrides,
    hook: Arc<SinkHook>,
    bypass: Option<Arc<BypassHook>>,
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
    assert!(overrides
        .queue_depth
        .is_none_or(|n| n <= max_queue_depth_from_env()));
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
        bypass: None,
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

/// A successful component write issued to the sink (one entry per vector).
#[derive(Clone, Debug)]
pub struct PumpWrite {
    pub offset: u64,
    pub len: usize,
}

/// Per-file counters and physical shape captured through the real sink.
#[derive(Clone, Debug)]
pub struct PumpFileTrace {
    pub opened: PumpOpen,
    pub writes: Vec<PumpWrite>,
    pub final_len: Option<u64>,
    pub syncs: usize,
}

#[derive(Default)]
struct TraceState {
    files: Vec<Arc<Mutex<PumpFileTrace>>>,
    bypasses: Vec<PathBuf>,
}

/// Scoped recording of pump-only component writes through a real engine.
pub struct PumpTrace {
    _guard: SinkHookGuard,
    state: Arc<Mutex<TraceState>>,
}

impl PumpTrace {
    pub fn install(root: PathBuf, overrides: PumpOverrides) -> Self {
        let state = Arc::new(Mutex::new(TraceState::default()));
        let observed = Arc::clone(&state);
        let guard = install_sink_hook(
            root.clone(),
            overrides,
            Arc::new(move |opened, sink| {
                let trace = Arc::new(Mutex::new(PumpFileTrace {
                    opened: opened.clone(),
                    writes: Vec::new(),
                    final_len: None,
                    syncs: 0,
                }));
                observed.lock().unwrap().files.push(Arc::clone(&trace));
                Box::new(TracedSink { sink, trace })
            }),
        );
        let observed = Arc::clone(&state);
        registrations()
            .lock()
            .unwrap()
            .iter_mut()
            .find(|r| r.root == root)
            .expect("hook was just installed")
            .bypass = Some(Arc::new(move |path| {
            observed.lock().unwrap().bypasses.push(path.to_path_buf());
        }));
        Self {
            _guard: guard,
            state,
        }
    }

    pub fn files(&self) -> Vec<PumpFileTrace> {
        self.state
            .lock()
            .unwrap()
            .files
            .iter()
            .map(|f| f.lock().unwrap().clone())
            .collect()
    }

    pub fn bypasses(&self) -> Vec<PathBuf> {
        self.state.lock().unwrap().bypasses.clone()
    }

    /// Check complete component sets and the bounded, aligned write contract.
    pub fn assert_complete_sstables(&self, expected: usize) {
        self.assert_complete_sstables_with_sync_policy(expected, false);
    }

    /// Check complete component sets and write shape when the publication
    /// target owns the durability barrier instead of each pump.
    pub fn assert_complete_sstables_with_deferred_sync(&self, expected: usize) {
        self.assert_complete_sstables_with_sync_policy(expected, true);
    }

    fn assert_complete_sstables_with_sync_policy(
        &self,
        expected: usize,
        allow_deferred_sync: bool,
    ) {
        use std::collections::{BTreeMap, BTreeSet};
        assert!(
            self.bypasses().is_empty(),
            "component writes bypassed pump: {:?}",
            self.bypasses()
        );
        let mut groups = BTreeMap::<PathBuf, BTreeSet<String>>::new();
        for file in self.files() {
            let name = file
                .opened
                .path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            assert_ne!(name, "Data.raw");
            assert!(
                groups
                    .entry(file.opened.path.parent().unwrap().to_path_buf())
                    .or_default()
                    .insert(name),
                "component opened more than once: {:?}",
                file.opened.path
            );
            // Block-exact and empty files need no truncate.
            let len = file.final_len.unwrap_or_else(|| {
                file.writes
                    .iter()
                    .map(|w| w.offset + w.len as u64)
                    .max()
                    .unwrap_or(0)
            });
            if !allow_deferred_sync {
                assert!(
                    file.syncs > 0,
                    "component must be synced: {:?}",
                    file.opened.path
                );
            }
            assert!(
                file.writes.len() as u64 <= len.div_ceil(file.opened.segment as u64) + 1,
                "write amplification for {:?}: {} writes for {len} bytes with segment {}",
                file.opened.path,
                file.writes.len(),
                file.opened.segment
            );
            let write_count = file.writes.len();
            for (i, write) in file.writes.into_iter().enumerate() {
                assert_eq!(
                    write.offset % file.opened.block as u64,
                    0,
                    "unaligned offset: {:?}",
                    file.opened.path
                );
                if i + 1 < write_count {
                    assert_eq!(
                        write.len % file.opened.block,
                        0,
                        "unaligned length: {:?}",
                        file.opened.path
                    );
                }
            }
        }
        assert_eq!(
            groups.len(),
            expected,
            "unexpected SSTable component groups"
        );
        for (dir, names) in groups {
            for name in [
                "Data.db",
                "Partitions.db",
                "Rows.db",
                "Filter.db",
                "Statistics.db",
                "Digest.crc32",
                "TOC.txt",
            ] {
                assert!(names.contains(name), "missing {name} in {dir:?}: {names:?}");
            }
            assert_eq!(names.len(), 8, "unexpected component set in {dir:?}");
            assert_ne!(
                names.contains("CompressionInfo.db"),
                names.contains("CRC.db")
            );
        }
    }
}

pub(super) fn note_bypass(path: &Path) {
    let callback = registrations()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .find(|r| path.starts_with(&r.root))
        .and_then(|r| r.bypass.clone());
    if let Some(callback) = callback {
        callback(path);
    }
}

struct TracedSink {
    sink: Box<dyn SegmentSink>,
    trace: Arc<Mutex<PumpFileTrace>>,
}

impl SegmentSink for TracedSink {
    fn pwrite(&mut self, buf: &[u8], offset: u64) -> super::Result<()> {
        self.sink.pwrite(buf, offset)?;
        self.trace.lock().unwrap().writes.push(PumpWrite {
            offset,
            len: buf.len(),
        });
        Ok(())
    }
    fn pwritev(&mut self, bufs: &[&[u8]], offset: u64) -> super::Result<()> {
        self.sink.pwritev(bufs, offset)?;
        self.trace.lock().unwrap().writes.push(PumpWrite {
            offset,
            len: bufs.iter().map(|buf| buf.len()).sum(),
        });
        Ok(())
    }
    fn sync_data(&mut self) -> super::Result<()> {
        self.sink.sync_data()?;
        self.trace.lock().unwrap().syncs += 1;
        Ok(())
    }
    fn set_len(&mut self, len: u64) -> super::Result<()> {
        self.sink.set_len(len)?;
        self.trace.lock().unwrap().final_len = Some(len);
        Ok(())
    }
    fn fadvise_dontneed(&mut self) -> super::Result<()> {
        self.sink.fadvise_dontneed()
    }
    fn mode(&self) -> DirectMode {
        self.sink.mode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pump::test_support::RecordingSink;
    use crate::pump::{AlignedPump, NeverAbort};

    #[test]
    fn wiring_component_counters_match_real_sink_trace() {
        // Counters are process-wide. An isolated child makes exact deltas sound
        // while the rest of the suite continues running concurrently.
        if std::env::var_os("FERROSA_TEST_PUMP_COUNTER_CHILD").is_some() {
            use crate::pump::{component_metrics, BufferedFileSink};
            let dir = tempfile::tempdir().unwrap();
            let trace = PumpTrace::install(dir.path().to_path_buf(), PumpOverrides::default());
            let before = component_metrics::snapshot();
            for component in component_metrics::COMPONENTS {
                let path = dir.path().join(component);
                let sink = BufferedFileSink::create(&path).unwrap();
                let mut pump = AlignedPump::open_with_depth(
                    Box::new(sink),
                    4096,
                    8192,
                    path,
                    1,
                    Arc::new(NeverAbort::new()),
                );
                pump.write_all(&vec![7; 20_001]).unwrap();
                pump.finish().unwrap();
            }
            let after = component_metrics::snapshot();
            for (before, after) in before.iter().zip(after) {
                let files: Vec<_> = trace
                    .files()
                    .into_iter()
                    .filter(|f| f.opened.path.ends_with(after.component))
                    .collect();
                assert_eq!(after.files[2] - before.files[2], files.len() as u64);
                assert_eq!(
                    after.bytes - before.bytes,
                    files
                        .iter()
                        .flat_map(|f| &f.writes)
                        .map(|w| w.len as u64)
                        .sum::<u64>()
                );
                assert_eq!(
                    after.writes - before.writes,
                    files.iter().map(|f| f.writes.len() as u64).sum::<u64>()
                );
            }
            let mut rendered = String::new();
            component_metrics::render_prometheus(&mut rendered);
            assert!(rendered
                .contains("write_pump_files_total{component=\"Data.db\",mode=\"buffered\"} 1"));
        } else {
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "pump::hooks::tests::wiring_component_counters_match_real_sink_trace",
                    "--nocapture",
                ])
                .env("FERROSA_TEST_PUMP_COUNTER_CHILD", "1")
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "counter child failed: {} {}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            );
        }
    }

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
