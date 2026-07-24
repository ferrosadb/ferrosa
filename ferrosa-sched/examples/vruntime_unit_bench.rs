//! Microbench for DD-1 (B2 T2.3): the `vruntime` accounting unit.
//!
//! Run: `cargo run --release --example vruntime_unit_bench -p ferrosa-sched`
//!
//! Compares the per-chunk cost of the candidate units and motivates the choice
//! recorded in ADR-022. **wall-elapsed** (`Instant::now()` + `.elapsed()`) is the
//! chosen unit: cheap, and it captures I/O wait, not just CPU. **count-proxy** (a
//! bare increment) is the pre-T2.2 unit: cheapest, but blind to I/O wait, so an
//! I/O-bound scan looked "cheap" and got free turns. Thread-CPU time
//! (`CLOCK_THREAD_CPUTIME_ID`) is deliberately not used: it is not in `std`,
//! needs a per-platform syscall (heavier than `Instant::now`), and by
//! construction excludes I/O wait — the exact signal the I/O dimension needs.

use std::time::Instant;

fn main() {
    const N: u64 = 50_000_000;
    let mut sink = 0u64;

    // Unit A: wall-elapsed — the cost `ScanSlot::tick` pays per chunk
    // (`Instant::now()` at the previous tick, `.elapsed()` now).
    let t0 = Instant::now();
    for _ in 0..N {
        let start = Instant::now();
        sink = sink.wrapping_add(start.elapsed().as_micros() as u64);
    }
    let wall = t0.elapsed();

    // Unit B: count-proxy — the pre-T2.2 unit (a bare increment).
    let t1 = Instant::now();
    for _ in 0..N {
        sink = sink.wrapping_add(1);
    }
    let count = t1.elapsed();

    let wall_ns = wall.as_nanos() as f64 / N as f64;
    let count_ns = count.as_nanos() as f64 / N as f64;
    println!("vruntime-unit microbench (N={N}, sink={sink}):");
    println!("  wall-elapsed : {wall_ns:6.2} ns/chunk  (Instant::now + elapsed)");
    println!("  count-proxy  : {count_ns:6.2} ns/chunk  (bare increment)");
    println!(
        "  wall overhead over count: {:.2} ns/chunk",
        wall_ns - count_ns
    );
    println!();
    println!(
        "A chunk is {} partitions; a partition decode is microseconds to",
        ferrosa_sched::DEFAULT_SCAN_CHUNK_BUDGET
    );
    println!("milliseconds, so the wall unit's ~{wall_ns:.0} ns overhead is well under 0.01%");
    println!("of chunk time — and unlike the count proxy it captures I/O wait.");
}
