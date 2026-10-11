//! Keep a write-set staging file's mapped pages CLEAN.
//!
//! A staging file is written once and immediately memory-mapped read-only
//! (see [`crate::write_set_spill`] and [`crate::write_set_stage`]). That mapping
//! charges the file's pages to this process, and a **dirty** file-backed page
//! cannot be reclaimed until it has been written back — on a memory-limited node
//! it is therefore effectively unreleasable and pushes the process toward the
//! OOM killer. The pages only become reclaimable once they are CLEAN.
//!
//! `BufWriter::flush` does not achieve that. It drains the userspace buffer into
//! the kernel's page cache and then calls `File::flush`, which is a **no-op** in
//! `std`; nothing writes the pages back. So the payload sits DIRTY in the page
//! cache and is mapped in dirty. [`flush_staged_pages`] forces the writeback
//! before the mapping is built, so the mapped pages are clean from the moment
//! they exist.
//!
//! This is the same policy `ferrosa_sstable::pump` applies to its own output
//! when it is not in direct mode (`AlignedPump::finish_sync`: `sync_data`, then
//! `POSIX_FADV_DONTNEED`). The staging file has no reuse value, so there is
//! nothing to keep in the cache; a clean page is evictable under pressure, which
//! is all a DB needs here.

use std::fs::File;

/// Write a freshly-written staging file's dirty pages back, so any mapping built
/// over it is clean — and therefore reclaimable — before it exists.
///
/// # Errors
///
/// A writeback failure is returned to the caller: a staging file whose pages
/// could not be cleaned must not be silently mapped, or the staging region would
/// be exactly the unreclaimable dirty file-backed memory this exists to avoid.
pub(crate) fn flush_staged_pages(file: &File) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        // On Apple targets `File::sync_data` is `F_FULLFSYNC` — a device-cache
        // flush that a throwaway staging file does not need. Plain `fsync`
        // already writes the dirty pages back, which is all "clean" requires, so
        // use it and skip the F_FULLFSYNC cost on the commit path.
        file.sync_all()
    }
    #[cfg(not(target_os = "macos"))]
    {
        // `fdatasync`: writes the dirty DATA pages back (they become clean)
        // without the metadata flush a staging file does not need. The same call
        // the SSTable write pump makes (`ferrosa_sstable::direct::sync_data`).
        file.sync_data()
    }
}

/// Kernel-reported page-state probes for the staging tests (Linux only: the
/// metric is `/proc/self/smaps`, the kernel's own accounting).
///
/// Kept here rather than in one test module so both `write_set_spill` and
/// `write_set_stage` assert the SAME quantity through the SAME code.
#[cfg(all(test, target_os = "linux"))]
pub(crate) mod probe {
    use std::path::Path;

    /// The kernel's own count of a mapping's DIRTY, file-backed, resident bytes:
    /// the sum of `Private_Dirty` over the `/proc/self/smaps` regions backed by
    /// `path`. This is the real page state, not our instrumentation's opinion, and
    /// it is exactly the class that is unreclaimable until writeback. `None` when
    /// nothing in this process is backed by `path`.
    pub(crate) fn mapping_private_dirty_bytes(path: &Path) -> Option<u64> {
        let text = std::fs::read_to_string("/proc/self/smaps").ok()?;
        let needle = path.to_str()?;
        let mut total: u64 = 0;
        let mut in_region = false;
        for line in text.lines() {
            // A region header's first field is an `addr-addr` range; every field
            // line's first field is a name (`Size:`, `Private_Dirty:`, ...).
            let header = line
                .split_whitespace()
                .next()
                .is_some_and(|token| token.contains('-'));
            if header {
                in_region = line.contains(needle);
                continue;
            }
            if in_region {
                if let Some(value) = line.strip_prefix("Private_Dirty:") {
                    if let Some(kb) = value
                        .split_whitespace()
                        .next()
                        .and_then(|v| v.parse::<u64>().ok())
                    {
                        total += kb * 1024;
                    }
                }
            }
        }
        Some(total)
    }

    /// Whether `dir`'s filesystem writes pages BACK somewhere. tmpfs/ramfs have no
    /// backing store, so a sync has nothing to write to and "clean" is not a state
    /// their pages can reach; the cleanliness assertion does not apply there (and
    /// CI's `/tmp` is a real disk).
    ///
    /// Reads the fstype of the longest `/proc/mounts` mount point that prefixes
    /// `dir`, rather than `statfs`, so no `f_type` widening cast is needed (which
    /// would be an `unnecessary_cast` on 64-bit Linux).
    pub(crate) fn staging_fs_has_writeback(dir: &Path) -> bool {
        let canonical = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
        let Ok(mounts) = std::fs::read_to_string("/proc/mounts") else {
            return true;
        };
        let mut best: Option<(usize, &str)> = None;
        for line in mounts.lines() {
            let mut fields = line.split_whitespace();
            let (Some(_dev), Some(mount_point), Some(fstype)) =
                (fields.next(), fields.next(), fields.next())
            else {
                continue;
            };
            if canonical.starts_with(mount_point)
                && best.is_none_or(|(longest, _)| mount_point.len() > longest)
            {
                best = Some((mount_point.len(), fstype));
            }
        }
        best.is_none_or(|(_, fstype)| fstype != "tmpfs" && fstype != "ramfs")
    }

    /// Fault every mapped page in: only a RESIDENT page can be charged dirty, so
    /// the kernel's count is meaningless until each page has been touched. Returns
    /// a value derived from the bytes read so the work cannot be optimised away.
    pub(crate) fn touch_every_page(bytes: &[u8]) -> u64 {
        bytes
            .iter()
            .step_by(4096)
            .fold(0u64, |sum, b| sum + u64::from(*b))
    }
}
