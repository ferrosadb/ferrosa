//! Release timing and allocation probe for replica eligibility selection only.
//! Run: cargo run --release -p ferrosa-cluster --example replica_scope_probe
//! This does not measure complete read/write latency or instrumentation overhead.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::coordinator::cl_routing::{
    eligible_replicas_for_cl, retain_eligible_replicas_for_cl,
};
use ferrosa_cluster::raft::{NodeInfo, NodeState};
use ferrosa_cluster::ring::TokenRing;
use uuid::Uuid;

struct CountingAllocator;
static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static REALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            REALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn main() {
    let mut ring = TokenRing::new();
    for id in 1..=9u64 {
        ring.add_node(
            id,
            NodeInfo {
                host_id: Uuid::from_u128(id.into()),
                addr: format!("127.0.0.1:{}", 7000 + id),
                data_center: format!("dc{}", (id - 1) / 3 + 1),
                rack: "rack1".into(),
                state: NodeState::Normal,
                cql_broadcast: None,
            },
        );
        ring.assign_tokens(id, &[(id * 100) as i64]);
    }
    let mut replicas: Vec<u64> = (1..=9).collect();
    let iterations = 100_000;
    for _ in 0..1000 {
        retain_eligible_replicas_for_cl(ConsistencyLevel::Quorum, &mut replicas, &ring);
    }
    let original_ptr = replicas.as_ptr();
    // Positive control: the compatibility API returns a fresh owned vector.
    COUNTING.store(true, Ordering::Relaxed);
    drop(black_box(eligible_replicas_for_cl(
        ConsistencyLevel::Quorum,
        black_box(&replicas),
        black_box(&ring),
    )));
    COUNTING.store(false, Ordering::Relaxed);
    let control_allocations = ALLOCATIONS.swap(0, Ordering::Relaxed);
    REALLOCATIONS.store(0, Ordering::Relaxed);
    assert!(
        control_allocations > 0,
        "allocation counter positive control failed"
    );
    COUNTING.store(true, Ordering::Relaxed);
    for _ in 0..iterations {
        retain_eligible_replicas_for_cl(
            ConsistencyLevel::Quorum,
            black_box(&mut replicas),
            black_box(&ring),
        );
    }
    COUNTING.store(false, Ordering::Relaxed);
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let reallocations = REALLOCATIONS.load(Ordering::Relaxed);
    assert_eq!(
        (allocations, reallocations),
        (0, 0),
        "eligibility selection allocated"
    );
    assert_eq!(
        replicas.as_ptr(),
        original_ptr,
        "replica buffer must be retained"
    );
    assert_eq!(
        replicas,
        (1..=9).collect::<Vec<_>>(),
        "healthy identities changed"
    );
    let mut timings = Vec::with_capacity(7);
    for _ in 0..7 {
        let start = Instant::now();
        for _ in 0..iterations {
            retain_eligible_replicas_for_cl(
                ConsistencyLevel::Quorum,
                black_box(&mut replicas),
                black_box(&ring),
            );
        }
        timings.push(start.elapsed().as_nanos() as f64 / f64::from(iterations));
    }
    timings.sort_by(f64::total_cmp);
    println!("{{\"scope\":\"replica eligibility selection only\",\"release\":{},\"replicas\":9,\"iterations\":{},\"positive_control_allocations\":{},\"allocations\":{},\"reallocations\":{},\"median_ns_per_call\":{:.2}}}",
        !cfg!(debug_assertions), iterations, control_allocations, allocations, reallocations, timings[3]);
}
