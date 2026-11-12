//! Verification of the existing staging kernel, not a new-behavior RED cycle.
//!
//! Test list: positively calibrate System allocation/reallocation counting;
//! measure borrowed writes through the exact production write/hash loop after
//! construction, fixed input/digest preallocation and one priming write; verify
//! the sealed artifact's complete contents and cleanup outside that region.
//! Finish/sync/reopen/readback/cleanup and network framing are not measured.
//! This makes no end-to-end zero-allocation claim.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::io::Read;

use sha2::{Digest, Sha256};

// Included cfg(test) cases resolve their existing quarantine import here.
// The exact test filter leaves those functional cases unexecuted.
mod raft {
    pub use ferrosa_cluster::raft::quarantine;
}

#[path = "../src/raft/repair_artifact.rs"]
mod repair_artifact;

use repair_artifact::RepairArtifactStage;

struct CountingAllocator;

thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static REALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

fn record_allocation(reallocation: bool) {
    TRACKING.with(|tracking| {
        if tracking.get() {
            if reallocation {
                REALLOCATIONS.with(|count| count.set(count.get() + 1));
            } else {
                ALLOCATIONS.with(|count| count.set(count.get() + 1));
            }
        }
    });
}

// As in ann_topk_alloc: unsafe operations only forward the contract to System.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(false);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(false);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation(true);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct Measurement;

impl Measurement {
    fn start() -> Self {
        // Initialize/reset every thread-local counter before enabling tracking.
        ALLOCATIONS.with(|count| count.set(0));
        REALLOCATIONS.with(|count| count.set(0));
        TRACKING.with(|tracking| assert!(!tracking.replace(true)));
        Self
    }

    fn finish(self) -> (usize, usize) {
        TRACKING.with(|tracking| tracking.set(false));
        (ALLOCATIONS.with(Cell::get), REALLOCATIONS.with(Cell::get))
    }
}

impl Drop for Measurement {
    fn drop(&mut self) {
        TRACKING.with(|tracking| tracking.set(false));
    }
}

#[test]
fn borrowed_chunk_writes_allocate_nothing_after_priming() {
    const CHUNKS: usize = 4096;
    // Cross SHA-256 block boundaries rather than writing only aligned blocks.
    const CHUNK_BYTES: usize = 1031;
    const PRIMING_BYTES: usize = 17;
    const TOTAL_BYTES: usize = PRIMING_BYTES + CHUNKS * CHUNK_BYTES;

    // Positive control on this thread, through the same System-backed allocator
    // and measurement region. black_box keeps the real heap operations visible.
    let calibration = Measurement::start();
    let mut probe = Vec::<u8>::with_capacity(black_box(1));
    probe.push(black_box(0x5a));
    black_box(&mut probe);
    probe.reserve_exact(black_box(4096));
    black_box(&mut probe);
    let (calibration_allocations, calibration_reallocations) = calibration.finish();
    println!(
        "REPAIR_ARTIFACT_ALLOC_CALIBRATION {{\"allocations\":{calibration_allocations},\"reallocations\":{calibration_reallocations}}}"
    );
    assert!(
        calibration_allocations > 0,
        "System allocation must register"
    );
    assert!(
        calibration_reallocations > 0,
        "System reallocation must register"
    );
    assert_eq!(probe[0], 0x5a);
    drop(probe);

    // Given: all fixed input and expected digest exist before construction and
    // priming. Construction/path ownership is deliberately outside measurement.
    let input: Vec<u8> = (0..TOTAL_BYTES)
        .map(|index| ((index * 31) ^ (index >> 9)) as u8)
        .collect();
    let expected_digest: [u8; 32] = Sha256::digest(&input).into();
    let directory = tempfile::tempdir().expect("private staging directory");
    let path = directory.path().join("allocation-verification.part");
    let mut stage = RepairArtifactStage::begin(
        &path,
        TOTAL_BYTES as u64,
        TOTAL_BYTES as u64,
        expected_digest,
    )
    .expect("construct the production stage");
    stage
        .write_chunk(0, &input[..PRIMING_BYTES])
        .expect("one priming write");

    // When: borrow thousands of chunks and execute the actual file-write/hash
    // kernel. Thread-local counters exclude unrelated test-harness activity.
    let mut offset = PRIMING_BYTES as u64;
    let mut written_chunks = 0;
    let measurement = Measurement::start();
    for chunk in input[PRIMING_BYTES..].chunks_exact(CHUNK_BYTES) {
        stage
            .write_chunk(offset, black_box(chunk))
            .expect("write a borrowed chunk through the production kernel");
        offset += chunk.len() as u64;
        written_chunks += 1;
    }
    let (allocations, reallocations) = measurement.finish();
    println!(
        "REPAIR_ARTIFACT_ALLOC_REGION {{\"thread_local\":true,\"chunks\":{written_chunks},\"chunk_bytes\":{CHUNK_BYTES},\"priming_bytes\":{PRIMING_BYTES},\"measured_bytes\":{},\"allocations\":{allocations},\"reallocations\":{reallocations}}}",
        offset - PRIMING_BYTES as u64
    );
    assert_eq!(written_chunks, CHUNKS);
    assert_eq!(offset, TOTAL_BYTES as u64);

    // Then: finish includes digest verification, sync and read-only reopen.
    // Full readback, EOF and cleanup are checked with tracking disabled, even
    // if the measured allocation counts will subsequently fail the invariant.
    let mut artifact = stage.finish().expect("seal the exact production artifact");
    let mut actual = vec![0; TOTAL_BYTES];
    artifact.read_exact(&mut actual).expect("complete readback");
    assert_eq!(
        actual, input,
        "all artifact bytes must match the fixed input"
    );
    let mut extra = [0; 1];
    assert_eq!(artifact.read(&mut extra).expect("read EOF"), 0);
    assert!(path.is_file(), "sealed owner retains its staging file");
    drop(artifact);
    assert!(
        !path.exists(),
        "dropping the owner removes its staging file"
    );
    directory
        .close()
        .expect("remove the private staging directory");
    println!(
        "REPAIR_ARTIFACT_ALLOC_READBACK {{\"bytes\":{TOTAL_BYTES},\"contents_match\":true,\"eof\":true,\"cleanup\":true}}"
    );

    assert_eq!(allocations, 0, "borrowed write/hash loop must not allocate");
    assert_eq!(
        reallocations, 0,
        "borrowed write/hash loop must not reallocate"
    );
}
