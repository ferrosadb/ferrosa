use std::collections::HashSet;
use std::time::Duration;

use ferrosa_index::vector::hnsw::{build_and_serialize, search_from_bytes};
use ferrosa_index::vector::{distance, RowPosition};
use ferrosa_index::DistanceMetric;

#[derive(Debug)]
struct BaselineMetrics {
    sidecar_bytes: usize,
    bytes_read_per_query: usize,
    p50_latency: Duration,
    p95_latency: Duration,
    mean_recall_at_k: f32,
}

fn clustered_corpus() -> Vec<(RowPosition, Vec<f32>)> {
    let clusters = 12usize;
    let per_cluster = 16usize;
    let dimensions = 16usize;

    let mut entries = Vec::with_capacity(clusters * per_cluster);
    for cluster in 0..clusters {
        for member in 0..per_cluster {
            let mut vector = vec![0.0; dimensions];
            vector[cluster % dimensions] = 10.0;
            vector[(cluster * 5 + 3) % dimensions] += 1.5;
            for (dim, value) in vector.iter_mut().enumerate() {
                let jitter = ((member * 17 + dim * 31 + cluster * 7) % 100) as f32 / 10_000.0;
                *value += jitter;
            }
            entries.push((RowPosition::new(entries.len() as u64 * 128), vector));
        }
    }
    entries
}

fn brute_force_top_k(entries: &[(RowPosition, Vec<f32>)], query: &[f32], k: usize) -> HashSet<u64> {
    let mut scored: Vec<(f32, u64)> = entries
        .iter()
        .map(|(position, vector)| {
            (
                distance(&DistanceMetric::L2, query, vector),
                position.offset,
            )
        })
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    scored
        .into_iter()
        .take(k)
        .map(|(_, offset)| offset)
        .collect()
}

fn recall_at_k(actual_offsets: &[u64], exact_offsets: &HashSet<u64>) -> f32 {
    let hits = actual_offsets
        .iter()
        .filter(|offset| exact_offsets.contains(offset))
        .count();
    hits as f32 / exact_offsets.len() as f32
}

fn capture_hnsw_json_sidecar_baseline(
    entries: &[(RowPosition, Vec<f32>)],
    queries: &[Vec<f32>],
    k: usize,
) -> BaselineMetrics {
    let sidecar = build_and_serialize(16, 200, DistanceMetric::L2, entries.to_vec())
        .expect("synthetic HNSW JSON sidecar should serialize");
    let sidecar_bytes = sidecar.len();

    let mut latencies = Vec::with_capacity(queries.len());
    let mut total_recall = 0.0f32;
    for query in queries {
        let exact_offsets = brute_force_top_k(entries, query, k);
        let start = std::time::Instant::now();
        let results = search_from_bytes(&sidecar, query, k, 64)
            .expect("baseline HNSW JSON sidecar should answer synthetic query");
        latencies.push(start.elapsed());

        let actual_offsets: Vec<u64> = results
            .iter()
            .map(|result| result.position.offset)
            .collect();
        total_recall += recall_at_k(&actual_offsets, &exact_offsets);
    }

    latencies.sort_unstable();
    let p50_latency = percentile_latency(&latencies, 50);
    let p95_latency = percentile_latency(&latencies, 95);

    BaselineMetrics {
        sidecar_bytes,
        bytes_read_per_query: sidecar_bytes,
        p50_latency,
        p95_latency,
        mean_recall_at_k: total_recall / queries.len() as f32,
    }
}

fn percentile_latency(sorted_latencies: &[Duration], percentile: usize) -> Duration {
    assert!(
        !sorted_latencies.is_empty(),
        "baseline requires at least one query"
    );
    let last = sorted_latencies.len() - 1;
    let idx = (last * percentile).div_ceil(100);
    sorted_latencies[idx]
}

#[test]
fn baseline_captures_current_hnsw_json_sidecar_metrics() {
    let entries = clustered_corpus();
    let queries: Vec<Vec<f32>> = entries
        .iter()
        .step_by(17)
        .map(|(_, vector)| vector.clone())
        .collect();

    let metrics = capture_hnsw_json_sidecar_baseline(&entries, &queries, 8);

    println!(
        "baseline hnsw json sidecar: sidecar_bytes={} bytes_read_per_query={} p50_us={} p95_us={} mean_recall_at_8={:.3}",
        metrics.sidecar_bytes,
        metrics.bytes_read_per_query,
        metrics.p50_latency.as_micros(),
        metrics.p95_latency.as_micros(),
        metrics.mean_recall_at_k
    );

    assert!(metrics.sidecar_bytes > 0, "sidecar size must be captured");
    assert_eq!(
        metrics.bytes_read_per_query, metrics.sidecar_bytes,
        "current JSON sidecar path reads/decodes the whole sidecar per query"
    );
    assert!(
        metrics.p50_latency > Duration::ZERO,
        "p50 latency must be captured"
    );
    assert!(
        metrics.p95_latency >= metrics.p50_latency,
        "p95 must be >= p50"
    );
    assert!(
        metrics.mean_recall_at_k >= 0.75,
        "clustered synthetic corpus should produce useful baseline recall"
    );
}
