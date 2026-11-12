//! Exercise production WAL boundaries and check allocation/payload identity.
//! Correctness: exact replay bytes, zero warmed write allocations, complete capture.
//! Last revised: 2026-10-29
//! Last changed: Added an isolated correctness and performance experiment.

#[path = "correctness_probe/allocation.rs"]
mod allocation;
#[path = "correctness_probe/capture.rs"]
mod capture;
#[path = "correctness_probe/workload.rs"]
mod workload;

#[global_allocator]
static ALLOCATOR: allocation::CountingAllocator = allocation::CountingAllocator;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("capture") => workload::capture(args.get(2).ok_or("capture requires output path")?),
        Some("bench") => workload::bench(),
        _ => Err("usage: correctness_probe capture <trace-path> | bench".into()),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(feature = "correctness-tracing")]
    fn production_wal_capture_is_complete_and_replays_exact_bytes() {
        super::workload::verify_recovery().unwrap();
    }

    #[test]
    fn warmed_encoding_and_shared_payloads_do_not_allocate() {
        super::workload::verify_allocations().unwrap();
    }

    #[test]
    fn corruption_and_torn_tail_are_rejected() {
        super::workload::verify_faults().unwrap();
    }
}
