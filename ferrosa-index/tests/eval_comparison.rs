//! Head-to-head evaluation of the original HNSW vector index against the new
//! quantized HVQ (staged IVF) index on one shared corpus.
//!
//! Measures, against exact brute-force truth: recall@k, bytes read per query,
//! p50/p95 query latency, and on-disk index size. Run with:
//!
//!   cargo test -p ferrosa-index --test eval_comparison -- --nocapture
//!
//! The printed numbers feed the published Vector Indexes evaluation page.

use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};

use ferrosa_index::vector::hnsw::{build_and_serialize, search_from_bytes};
use ferrosa_index::vector::quantized::ivf_staged::{
    QuantizedIvfBuilder, QuantizedIvfConfig, QuantizedIvfReader, QuantizedIvfSearchOptions,
};
use ferrosa_index::vector::{distance, RowPosition};
use ferrosa_index::DistanceMetric;

const DIMENSIONS: usize = 16;
const CLUSTERS: usize = 12;
const PER_CLUSTER: usize = 16;
const K: usize = 10;
const PROBES: usize = 4;
const PAGE_SIZE: usize = 8;

#[derive(Debug)]
struct Eval {
    label: &'static str,
    index_bytes: u64,
    mean_bytes_read_per_query: f64,
    p50: Duration,
    p95: Duration,
    mean_recall_at_k: f32,
}

fn clustered_corpus() -> Vec<(RowPosition, Vec<f32>)> {
    let mut entries = Vec::with_capacity(CLUSTERS * PER_CLUSTER);
    for cluster in 0..CLUSTERS {
        for member in 0..PER_CLUSTER {
            let mut vector = vec![0.0; DIMENSIONS];
            vector[cluster % DIMENSIONS] = 10.0;
            vector[(cluster * 5 + 3) % DIMENSIONS] += 1.5;
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

fn recall_at_k(actual: &[u64], exact: &HashSet<u64>) -> f32 {
    let hits = actual
        .iter()
        .filter(|offset| exact.contains(offset))
        .count();
    hits as f32 / exact.len() as f32
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    assert!(!sorted.is_empty(), "evaluation requires at least one query");
    let last = sorted.len() - 1;
    sorted[(last * percentile).div_ceil(100)]
}

/// Total bytes of every file beneath `dir` (manifest + all pages).
fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    for entry in std::fs::read_dir(dir)
        .expect("artifact dir is readable")
        .flatten()
    {
        let meta = entry.metadata().expect("entry metadata");
        total += if meta.is_dir() {
            dir_size(&entry.path())
        } else {
            meta.len()
        };
    }
    total
}

/// Mean bytes of one staged page, used to weight the reader's page-read count.
fn mean_page_bytes(dir: &Path) -> f64 {
    let pages_dir = dir.join("quantized_ivf");
    let mut bytes = 0u64;
    let mut count = 0u64;
    for entry in std::fs::read_dir(&pages_dir)
        .expect("pages dir is readable")
        .flatten()
    {
        bytes += entry.metadata().expect("page metadata").len();
        count += 1;
    }
    assert!(count > 0, "staged artifact must emit at least one page");
    bytes as f64 / count as f64
}

fn eval_hnsw(entries: &[(RowPosition, Vec<f32>)], queries: &[Vec<f32>], k: usize) -> Eval {
    let sidecar = build_and_serialize(16, 200, DistanceMetric::L2, entries.to_vec())
        .expect("HNSW sidecar serializes");
    let mut latencies = Vec::with_capacity(queries.len());
    let mut total_recall = 0.0f32;
    for query in queries {
        let exact = brute_force_top_k(entries, query, k);
        let start = Instant::now();
        let results = search_from_bytes(&sidecar, query, k, 64).expect("HNSW answers query");
        latencies.push(start.elapsed());
        let offsets: Vec<u64> = results.iter().map(|r| r.position.offset).collect();
        total_recall += recall_at_k(&offsets, &exact);
    }
    latencies.sort_unstable();
    Eval {
        label: "HNSW (full sidecar)",
        index_bytes: sidecar.len() as u64,
        // The HNSW path reads and decodes the whole sidecar per query.
        mean_bytes_read_per_query: sidecar.len() as f64,
        p50: percentile(&latencies, 50),
        p95: percentile(&latencies, 95),
        mean_recall_at_k: total_recall / queries.len() as f32,
    }
}

fn eval_hvq(
    entries: &[(RowPosition, Vec<f32>)],
    queries: &[Vec<f32>],
    k: usize,
    dir: &Path,
) -> Eval {
    let config = QuantizedIvfConfig {
        lists: CLUSTERS,
        metric: DistanceMetric::L2,
        page_size: PAGE_SIZE,
    };
    let mut builder = QuantizedIvfBuilder::new(dir, config);
    for (position, vector) in entries {
        builder
            .add_vector(*position, vector)
            .expect("HVQ accepts vector");
    }
    builder.finish().expect("HVQ artifact builds");

    let reader = QuantizedIvfReader::open(dir).expect("HVQ artifact opens");
    let page_bytes = mean_page_bytes(dir);
    let options = QuantizedIvfSearchOptions {
        k,
        probes: PROBES,
        max_page_reads: 10_000,
    };

    let mut latencies = Vec::with_capacity(queries.len());
    let mut total_recall = 0.0f32;
    let mut total_bytes_read = 0.0f64;
    for query in queries {
        let exact = brute_force_top_k(entries, query, k);
        let start = Instant::now();
        let result = reader.search(query, options).expect("HVQ answers query");
        latencies.push(start.elapsed());
        total_bytes_read += result.page_reads as f64 * page_bytes;
        let offsets: Vec<u64> = result.hits.iter().map(|h| h.position.offset).collect();
        total_recall += recall_at_k(&offsets, &exact);
    }
    latencies.sort_unstable();
    Eval {
        label: "HVQ (staged quantized IVF)",
        index_bytes: dir_size(dir),
        mean_bytes_read_per_query: total_bytes_read / queries.len() as f64,
        p50: percentile(&latencies, 50),
        p95: percentile(&latencies, 95),
        mean_recall_at_k: total_recall / queries.len() as f32,
    }
}

fn print_row(eval: &Eval) {
    println!(
        "{:<28} index_bytes={:>7} bytes_read/q={:>9.0} p50_us={:>5} p95_us={:>5} recall@{}={:.3}",
        eval.label,
        eval.index_bytes,
        eval.mean_bytes_read_per_query,
        eval.p50.as_micros(),
        eval.p95.as_micros(),
        K,
        eval.mean_recall_at_k,
    );
}

#[test]
fn eval_compares_hnsw_and_hvq_on_shared_corpus() {
    let entries = clustered_corpus();
    let queries: Vec<Vec<f32>> = entries.iter().step_by(11).map(|(_, v)| v.clone()).collect();
    assert!(!queries.is_empty(), "query set must be non-empty");

    let hnsw = eval_hnsw(&entries, &queries, K);
    let tmp = tempfile::tempdir().expect("temp dir for HVQ artifact");
    let hvq = eval_hvq(&entries, &queries, K, tmp.path());

    println!(
        "\nvector index evaluation  corpus={} vectors dim={} queries={} probes={}/{}",
        entries.len(),
        DIMENSIONS,
        queries.len(),
        PROBES,
        CLUSTERS,
    );
    print_row(&hnsw);
    print_row(&hvq);
    println!(
        "bytes_read/query reduction: {:.1}x",
        hnsw.mean_bytes_read_per_query / hvq.mean_bytes_read_per_query
    );

    // The staged reader's core thesis: it reads only the probed pages, never the
    // whole index, so it moves far fewer bytes per query than the full sidecar.
    assert!(
        hvq.mean_bytes_read_per_query < hnsw.mean_bytes_read_per_query,
        "HVQ staged read ({:.0} B/q) must read fewer bytes than the full HNSW sidecar ({:.0} B/q)",
        hvq.mean_bytes_read_per_query,
        hnsw.mean_bytes_read_per_query
    );
    // Recall must stay useful on the clustered corpus for both indexes.
    assert!(
        hnsw.mean_recall_at_k >= 0.80,
        "HNSW recall too low: {}",
        hnsw.mean_recall_at_k
    );
    assert!(
        hvq.mean_recall_at_k >= 0.80,
        "HVQ recall too low: {}",
        hvq.mean_recall_at_k
    );
}
