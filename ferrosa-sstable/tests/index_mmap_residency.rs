//! Residency measurement for the SSTable index components (`Partitions.db`,
//! `Rows.db`) — the file-backed memory class that dominates the coordinator's
//! COMMIT peak.
//!
//! # Why this test measures the VM map, not the heap
//!
//! A counting allocator sees only the anonymous heap. The measured COMMIT peak
//! is dominated by FILE-BACKED resident pages: the OS's own page accounting
//! (`vm_region_extended_info`, exactly what `vmmap`/`mempeek.dylib` read) shows
//! megabytes of resident-and-dirty pages belonging to `-Partitions.db` mappings
//! while the whole anonymous heap is a fraction of that. A heap counter cannot
//! see those bytes, and a VALUE assertion passes both before and after such a
//! fix — the broken version returns correct bytes while holding the whole file
//! resident. So the guard here reads the process's own VM map.
//!
//! # The measurement
//!
//! `mempeek.dylib` (the on-host inspector) is injected with
//! `DYLD_INSERT_LIBRARIES`; `task_for_pid` is refused on this host, so no
//! external tool can walk another process. A process can always inspect ITSELF
//! (`mach_task_self()` needs no permission), so this file reimplements just the
//! relevant slice — walk `mach_vm_region(VM_REGION_EXTENDED_INFO)`, join the
//! backing path with `proc_regionfilename`, and sum `pages_resident` /
//! `pages_dirtied` for the component under test.
#![cfg(target_os = "macos")]

use std::sync::Mutex;

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::io::{FileReadAt, ReadAt};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};

// ── the tunable under test ───────────────────────────────────────────────────
//
// The index component is mapped whole when it is small enough to be a "small,
// hot index" and served through the bounded, fd-cached `pread` path otherwise.
// The switch is a byte ceiling read from the environment at open time; it is a
// streaming BUFFER bound, not a cap: a component larger than it is read in
// full, never refused and never truncated.
const INDEX_MMAP_MAX_ENV: &str = "FERROSA_SSTABLE_INDEX_MMAP_MAX_BYTES";

// `std::env::set_var` races across threads under the libtest harness, so every
// test that steers the ceiling holds this lock for the whole set-measure span.
static ENV_LOCK: Mutex<()> = Mutex::new(());

// ── VM-region inspector (in-process, no permission needed) ──────────────────

mod vm {
    use std::ffi::CStr;

    #[repr(C)]
    struct Extended {
        protection: i32,
        user_tag: u32,
        pages_resident: u32,
        pages_shared_now_private: u32,
        pages_swapped_out: u32,
        pages_dirtied: u32,
        ref_count: u32,
        shadow_depth: u16,
        external_pager: u8,
        share_mode: u8,
        pages_reusable: u32,
    }

    const VM_REGION_EXTENDED_INFO: i32 = 13;
    const COUNT: u32 = (std::mem::size_of::<Extended>() / std::mem::size_of::<u32>()) as u32;
    const PAGE: u64 = 16 * 1024;

    extern "C" {
        static mach_task_self_: u32;
        fn mach_vm_region(
            task: u32,
            address: *mut u64,
            size: *mut u64,
            flavor: i32,
            info: *mut i32,
            count: *mut u32,
            object: *mut u32,
        ) -> i32;
        fn mach_port_deallocate(task: u32, name: u32) -> i32;
        fn proc_regionfilename(pid: i32, address: u64, buffer: *mut u8, size: u32) -> i32;
    }

    pub struct Region {
        pub path: String,
        pub prot: i32,
        pub share: u8,
        pub user_tag: u32,
        pub resident: u64,
        pub dirtied: u64,
    }

    pub fn regions() -> Vec<Region> {
        let task = unsafe { mach_task_self_ };
        let pid = unsafe { libc::getpid() };
        let mut out = Vec::new();
        let mut addr: u64 = 0;
        loop {
            let mut a = addr;
            let mut size: u64 = 0;
            let mut info = std::mem::MaybeUninit::<Extended>::zeroed();
            let mut count = COUNT;
            let mut object: u32 = 0;
            // SAFETY: `info` is a correctly-sized `vm_region_extended_info`
            // buffer; the kernel fills at most COUNT ints of it.
            let kr = unsafe {
                mach_vm_region(
                    task,
                    &mut a,
                    &mut size,
                    VM_REGION_EXTENDED_INFO,
                    info.as_mut_ptr() as *mut i32,
                    &mut count,
                    &mut object,
                )
            };
            if object != 0 {
                unsafe { mach_port_deallocate(task, object) };
            }
            if kr != 0 || size == 0 {
                break;
            }
            let info = unsafe { info.assume_init() };
            let mut buf = [0u8; 4096];
            let n = unsafe { proc_regionfilename(pid, a, buf.as_mut_ptr(), buf.len() as u32) };
            let path = if n > 0 {
                unsafe { CStr::from_ptr(buf.as_ptr() as *const std::os::raw::c_char) }
                    .to_string_lossy()
                    .into_owned()
            } else {
                String::new()
            };
            out.push(Region {
                path,
                prot: info.protection,
                share: info.share_mode,
                user_tag: info.user_tag,
                resident: info.pages_resident as u64 * PAGE,
                dirtied: info.pages_dirtied as u64 * PAGE,
            });
            addr = a.saturating_add(size);
        }
        out
    }

    /// (resident, dirtied) bytes across every region backed by `path`.
    ///
    /// The kernel reports the backing file's canonical path (`proc_regionfilename`
    /// resolves symlinks, so a `/var/...` tmpdir comes back as `/private/var/...`),
    /// so both sides are canonicalized before comparing.
    pub fn residency_of(path: &std::path::Path) -> (u64, u64) {
        let (resident, dirtied, _) = residency_detail(path);
        (resident, dirtied)
    }

    /// (resident, dirtied, descriptors) for every region backed by `path`.
    pub fn residency_detail(path: &std::path::Path) -> (u64, u64, Vec<String>) {
        let want = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let mut resident = 0u64;
        let mut dirtied = 0u64;
        let mut detail = Vec::new();
        for r in regions() {
            if std::path::Path::new(&r.path) == want {
                resident += r.resident;
                dirtied += r.dirtied;
                detail.push(format!(
                    "prot={} share={} tag={} resident={} dirtied={}",
                    r.prot, r.share, r.user_tag, r.resident, r.dirtied
                ));
            }
        }
        (resident, dirtied, detail)
    }

    /// (total resident bytes, region count) over the whole process — a sanity
    /// check that the walk itself sees the process's own memory.
    pub fn total_resident() -> (u64, usize) {
        let rs = regions();
        (rs.iter().map(|r| r.resident).sum(), rs.len())
    }

    /// Every region the kernel reports a backing path for, as diagnostics.
    pub fn all_paths() -> Vec<String> {
        regions()
            .into_iter()
            .filter(|r| !r.path.is_empty())
            .map(|r| {
                format!(
                    "prot={} share={} tag={} resident={} dirtied={} {}",
                    r.prot, r.share, r.user_tag, r.resident, r.dirtied, r.path
                )
            })
            .collect()
    }

    /// The path / protection / tag the kernel reports for the region that
    /// contains `addr`.
    pub fn path_at(addr: u64) -> (String, i32, u32) {
        let task = unsafe { mach_task_self_ };
        let pid = unsafe { libc::getpid() };
        let mut a: u64 = 0;
        loop {
            let mut base = a;
            let mut size: u64 = 0;
            let mut info = std::mem::MaybeUninit::<Extended>::zeroed();
            let mut count = COUNT;
            let mut object: u32 = 0;
            let kr = unsafe {
                mach_vm_region(
                    task,
                    &mut base,
                    &mut size,
                    VM_REGION_EXTENDED_INFO,
                    info.as_mut_ptr() as *mut i32,
                    &mut count,
                    &mut object,
                )
            };
            if object != 0 {
                unsafe { mach_port_deallocate(task, object) };
            }
            if kr != 0 || size == 0 {
                break;
            }
            if addr >= base && addr < base + size {
                let info = unsafe { info.assume_init() };
                let mut buf = [0u8; 4096];
                let n =
                    unsafe { proc_regionfilename(pid, base, buf.as_mut_ptr(), buf.len() as u32) };
                let path = if n > 0 {
                    unsafe { CStr::from_ptr(buf.as_ptr() as *const std::os::raw::c_char) }
                        .to_string_lossy()
                        .into_owned()
                } else {
                    String::new()
                };
                return (path, info.protection, info.user_tag);
            }
            a = base.saturating_add(size);
        }
        (String::new(), 0, 0)
    }
}

// ── fixture ─────────────────────────────────────────────────────────────────

fn header() -> SerializationHeader {
    SerializationHeader {
        complex_collections: false,
        min_timestamp: 0,
        min_local_deletion_time: 0,
        min_ttl: 0,
        max_timestamp: i64::MAX,
        key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
        clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".to_string()],
        static_columns: vec![],
        regular_columns: vec![(
            b"r0".to_vec(),
            "org.apache.cassandra.db.marshal.BytesType".to_string(),
        )],
    }
}

fn partition(idx: usize) -> Partition {
    let key = DecoratedKey::new(PartitionKey::new(format!("pk-{idx:016}").into_bytes()));
    Partition {
        key,
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: 1i32.to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(vec![b'x'; 16], 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        }],
    }
}

/// Write a file-backed SSTable of `n` partitions and return the tempdir (kept
/// alive) plus the `Partitions.db` path and its byte length.
fn write_sstable(n: usize) -> (tempfile::TempDir, std::path::PathBuf, u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    let staging = dir.path().join("staging");
    let options = WriteOptions {
        compression: None,
        bloom_fp_chance: 0.01,
        chunk_size: 16 * 1024,
        verify_output: false,
    };
    let mut writer = SSTableWriter::new_file_backed(options, header(), staging.join("Data.db"))
        .expect("new_file_backed");
    let mut parts: Vec<Partition> = (0..n).map(partition).collect();
    parts.sort_by_key(|p| p.key.token);
    for p in &parts {
        writer.add_partition(p).expect("add_partition");
    }
    let files = writer.finish_to_directory(&staging).expect("finish");
    // Production maps only generation-prefixed index components
    // (`<gen>-Partitions.db`): `should_mmap_component` matches the `-`-suffixed
    // name. `finish_to_directory` writes the bare `Partitions.db` into staging;
    // the promote step renames it. Model the published name so the mmap branch
    // under test is the one exercised.
    let published = dir.path().join("1-Partitions.db");
    std::fs::rename(&files.partitions, &published).expect("rename to published name");
    let len = std::fs::metadata(&published).expect("stat").len();
    (dir, published, len)
}

/// Read every byte of the component through the reader (this is what makes a
/// whole-file mapping resident).
fn touch_all(reader: &FileReadAt) {
    let len = reader.len().expect("len");
    let mut buf = vec![0u8; 64 * 1024];
    let mut off = 0u64;
    while off < len {
        let want = (buf.len() as u64).min(len - off) as usize;
        let n = reader.read_at(&mut buf[..want], off).expect("read");
        if n == 0 {
            break;
        }
        off += n as u64;
    }
}

/// N partitions — an index component that is a few MiB, far above the 1 MiB
/// ceiling used below.
const N: usize = 150_000;
const BUDGET_BYTES: u64 = 1024 * 1024;

/// A part of a `Partitions.db` is read back byte-exactly regardless of which
/// reader mode the ceiling selects.
fn read_back_first_key(path: &std::path::Path) -> u64 {
    let reader = FileReadAt::open(path).expect("open");
    let mut buf = vec![0u8; 16];
    let len = reader.len().expect("len");
    let n = reader.read_at(&mut buf, 0).expect("read");
    assert_eq!(n, 16, "the first key is 16 bytes");
    len
}

/// INVARIANT: the bounded (pread) reader and the mapped reader must return
/// byte-identical content for every byte of the component — the ceiling must
/// change WHICH reader serves the bytes, never WHICH bytes are returned.
///
/// Pins "every row written in full; no truncation; byte-identical to the
/// unbounded path": a bound that dropped or refused a component would show up
/// here as a length or content mismatch.
#[test]
fn bounded_and_mapped_readers_return_identical_bytes() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_dir, partitions, len) = write_sstable(20_000);
    let on_disk = std::fs::read(&partitions).expect("read file");

    let read_all = |env: &str| -> Vec<u8> {
        std::env::set_var(INDEX_MMAP_MAX_ENV, env);
        let reader = FileReadAt::open(&partitions).expect("open");
        let n = reader.len().expect("len");
        assert_eq!(n, len, "the reader must report the whole file length");
        let mut out = vec![0u8; n as usize];
        reader.read_exact_at(&mut out, 0).expect("read all");
        std::env::remove_var(INDEX_MMAP_MAX_ENV);
        out
    };

    let mapped = read_all(&u64::MAX.to_string()); // ceiling lifted: mmap path
    let bounded = read_all(&(1024u64).to_string()); // tiny ceiling: pread path

    assert_eq!(mapped.len(), on_disk.len(), "mapped read length");
    assert_eq!(bounded.len(), on_disk.len(), "bounded read length");
    assert_eq!(mapped, on_disk, "mapped reader must be byte-exact");
    assert_eq!(
        bounded, on_disk,
        "bounded reader must be byte-identical to the unbounded mapping — never truncated"
    );
}

/// GUARD: with the index ceiling at [`BUDGET_BYTES`], opening and reading a
/// `Partitions.db` several MiB larger must NOT leave the whole file resident.
///
/// RED before the fix: `FileReadAt::open` maps every `-Partitions.db` whole,
/// so the measured residency reads back the file size however large it is.
#[test]
fn index_component_residency_is_bounded_by_the_tunable_buffer() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_dir, partitions, len) = write_sstable(N);
    assert!(
        len > BUDGET_BYTES * 2,
        "fixture too small to be a bound test: Partitions.db is {len} B, budget {BUDGET_BYTES} B"
    );

    std::env::set_var(INDEX_MMAP_MAX_ENV, BUDGET_BYTES.to_string());
    read_back_first_key(&partitions); // sanity: bytes are readable in this mode

    let reader = FileReadAt::open(&partitions).expect("open for residency");
    touch_all(&reader);
    let (resident, dirtied) = vm::residency_of(&partitions);
    eprintln!(
        "INDEX_RESIDENCY n={N} partitions_db_bytes={len} budget={BUDGET_BYTES} \
         resident={resident} dirtied={dirtied}"
    );
    std::env::remove_var(INDEX_MMAP_MAX_ENV);

    assert!(
        resident <= BUDGET_BYTES,
        "a {len} B Partitions.db stayed {resident} B resident (budget {BUDGET_BYTES} B): the \
         index ceiling is not bounding the mapping"
    );
    assert!(
        dirtied <= BUDGET_BYTES,
        "a {len} B Partitions.db held {dirtied} B of dirty file-backed pages (budget \
         {BUDGET_BYTES} B)"
    );
}

/// DIAGNOSIS CONTROL: a plain anonymous allocation must not be attributed to a
/// backing file path. `mempeek`/`core_mem.py` trust `proc_regionfilename` to
/// separate file-backed memory from anonymous memory; if that call names a file
/// for anonymous pages, the class split is wrong and so is any conclusion drawn
/// from it (e.g. "the SSTable mappings dominate the COMMIT peak").
#[test]
fn anonymous_memory_is_not_attributed_to_a_file_path() {
    const ANON_BYTES: usize = 256 * 1024 * 1024;
    let mut v = vec![0u8; ANON_BYTES];
    for i in (0..v.len()).step_by(16 * 1024) {
        v[i] = 1;
    }
    let base = v.as_ptr() as u64;
    let (path, prot, tag) = vm::path_at(base);
    eprintln!("ANON_ATTRIBUTION base=0x{base:x} prot={prot} tag={tag} path={path:?}");
    std::hint::black_box(&mut v);
    assert!(
        path.is_empty(),
        "a {ANON_BYTES} B anonymous allocation at 0x{base:x} was attributed to the backing file \
         {path:?} (prot={prot} tag={tag}); proc_regionfilename is not a reliable file-backed vs \
         anonymous classifier on this host"
    );
}

/// NEGATIVE CONTROL: with the ceiling lifted, the same measurement must show
/// the whole file resident — otherwise the guard above is vacuous (it would
/// pass for a reason unrelated to the ceiling).
#[test]
fn control_lifting_the_ceiling_maps_the_whole_index_resident() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_dir, partitions, len) = write_sstable(N);

    std::env::set_var(INDEX_MMAP_MAX_ENV, (u64::MAX).to_string());
    let reader = FileReadAt::open(&partitions).expect("open");
    touch_all(&reader);
    let (resident, dirtied, detail) = vm::residency_detail(&partitions);
    let (all_res, all_n) = vm::total_resident();
    let all_paths = vm::all_paths();
    eprintln!("ALL_PATHED_REGIONS:");
    for line in &all_paths {
        eprintln!("  {line}");
    }
    eprintln!(
        "CONTROL_INDEX_RESIDENCY partitions_db_bytes={len} resident={resident} dirtied={dirtied} \
         (all_regions={all_n} all_resident={all_res}) detail={detail:?}"
    );
    assert!(
        resident as f64 >= len as f64 * 0.9,
        "control expected the whole {len} B Partitions.db resident, saw {resident} B — the \
         residency measurement cannot see a whole-file mapping, so the guard proves nothing"
    );
}
