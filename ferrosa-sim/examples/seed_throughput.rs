//! Manual throughput benchmark for the simulator (W5.10 / W5.11).
//!
//! Run with `cargo run --release --example seed_throughput -p ferrosa-sim`.
//! Reports seeds/minute as the headline KPI of Sprint 5.

use ferrosa_sim::cluster::SimulatedCluster;
use ferrosa_sim::refinement::check_trace;
use std::time::Instant;

fn main() {
    let n_seeds: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);

    let start = Instant::now();
    let mut total_steps = 0_u64;
    for seed in 0..n_seeds {
        let mut cluster = SimulatedCluster::with_voters(3, seed);
        cluster.run_until_leader(10_000);
        check_trace(cluster.trace()).expect("trace must refine");
        total_steps += cluster.trace().len() as u64;
    }
    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64();
    let seeds_per_min = (n_seeds as f64) * 60.0 / secs;
    println!(
        "ran {n_seeds} seeds in {:.3}s — {seeds_per_min:.0} seeds/min, \
         {total_steps} total trace steps",
        secs
    );
}
