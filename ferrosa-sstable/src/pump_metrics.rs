//! Fixed component labels: no registry lookup, allocation, or lock per write.
use crate::direct::DirectMode;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub const COMPONENTS: [&str; 9] = [
    "Data.db",
    "Partitions.db",
    "Rows.db",
    "Filter.db",
    "Statistics.db",
    "CompressionInfo.db",
    "CRC.db",
    "Digest.crc32",
    "TOC.txt",
];
const MODES: [&str; 3] = ["direct", "nocache", "buffered"];
static FILES: [[AtomicU64; 3]; 9] = [const { [const { AtomicU64::new(0) }; 3] }; 9];
static BYTES: [AtomicU64; 9] = [const { AtomicU64::new(0) }; 9];
static WRITES: [AtomicU64; 9] = [const { AtomicU64::new(0) }; 9];

fn component(path: &Path) -> Option<usize> {
    let name = path.file_name()?.to_str()?;
    COMPONENTS.iter().position(|component| {
        name == *component
            || name
                .strip_suffix(component)
                .is_some_and(|prefix| prefix.ends_with('-'))
    })
}
fn mode_index(mode: DirectMode) -> usize {
    match mode {
        DirectMode::Direct => 0,
        DirectMode::NoCache => 1,
        DirectMode::Buffered => 2,
    }
}
pub(super) fn opened(path: &Path, mode: DirectMode) {
    if let Some(index) = component(path) {
        FILES[index][mode_index(mode)].fetch_add(1, Ordering::Relaxed);
    }
}

/// Resolved once at file creation. Successful sink requests count padded physical
/// bytes; a vectored request counts as one write (including any kernel retries).
pub(super) struct Counter(Option<usize>);
impl Counter {
    pub(super) fn new(path: &Path) -> Self {
        Self(component(path))
    }
    pub(super) fn written(&self, bytes: usize) {
        if let Some(index) = self.0 {
            BYTES[index].fetch_add(bytes as u64, Ordering::Relaxed);
            WRITES[index].fetch_add(1, Ordering::Relaxed);
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct ComponentSnapshot {
    pub component: &'static str,
    /// Direct, nocache, buffered, in that order.
    pub files: [u64; 3],
    pub bytes: u64,
    pub writes: u64,
}
pub fn snapshot() -> [ComponentSnapshot; 9] {
    std::array::from_fn(|i| ComponentSnapshot {
        component: COMPONENTS[i],
        files: std::array::from_fn(|m| FILES[i][m].load(Ordering::Relaxed)),
        bytes: BYTES[i].load(Ordering::Relaxed),
        writes: WRITES[i].load(Ordering::Relaxed),
    })
}
pub(super) fn render_prometheus(out: &mut String) {
    use std::fmt::Write;
    out.push_str("# HELP write_pump_files_total SSTable component pumps opened by component and I/O mode.\n# TYPE write_pump_files_total counter\n# HELP write_pump_bytes_total Successful physical component bytes submitted, including padding and header rewrites.\n# TYPE write_pump_bytes_total counter\n# HELP write_pump_writes_total Successful component sink write requests, with vectored batches counted once.\n# TYPE write_pump_writes_total counter\n");
    for entry in snapshot() {
        for (mode, files) in MODES.iter().zip(entry.files) {
            let _ = writeln!(
                out,
                "write_pump_files_total{{component=\"{}\",mode=\"{mode}\"}} {files}",
                entry.component
            );
        }
        let _ = writeln!(
            out,
            "write_pump_bytes_total{{component=\"{}\"}} {}",
            entry.component, entry.bytes
        );
        let _ = writeln!(
            out,
            "write_pump_writes_total{{component=\"{}\"}} {}",
            entry.component, entry.writes
        );
    }
}
