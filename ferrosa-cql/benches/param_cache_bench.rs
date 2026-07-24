//! Transparent param-cache throughput: the per-request cost of resolving an
//! unprepared inline-literal INSERT three ways — no cache (today), a cache HIT
//! (repeated shape), and a cache MISS (first sight). Quantifies the hot-path
//! win that motivates t_48d5eeaa.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ferrosa_cql::param_cache::TransparentCache;
use ferrosa_cql::parser;

/// Distinct-per-row INSERTs of ONE shape — what the loadgen sends. Each has a
/// unique string + int + hex, so an exact-text cache would miss; the normalized
/// skeleton is identical across all of them.
fn insert(i: usize) -> String {
    format!(
        "INSERT INTO baselines.data (pk, ck, val) VALUES ('machine-{i:012}', {i}, 0xDEADBEEF{i:08X})"
    )
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("param_cache");
    // Pre-build a pool of distinct same-shape INSERTs so `format!` is NOT in the
    // timed loop — we measure parse vs resolve, not string building.
    let pool: Vec<String> = (0..256).map(insert).collect();
    let pick = |i: usize| pool[i % pool.len()].as_str();

    // Baseline: today's path — full parse every request, no cache.
    group.bench_function("no_cache_parse", |b| {
        let mut i = 0usize;
        b.iter(|| {
            i = i.wrapping_add(1);
            black_box(parser::parse(black_box(pick(i))).expect("parse"))
        })
    });

    // HIT: warm the skeleton once, then resolve fresh same-shape queries. Each
    // call normalizes + binds extracted spans into the cached template — no
    // full parse.
    group.bench_function("cache_hit", |b| {
        let cache = TransparentCache::new(64);
        let _ = cache.resolve(pick(0), parser::parse); // warm (miss)
        let mut i = 0usize;
        b.iter(|| {
            i = i.wrapping_add(1);
            black_box(cache.resolve(black_box(pick(i)), parser::parse).0)
        })
    });

    // MISS: a cold cache every iteration — normalize + full parse + verify +
    // insert. The one-time cost paid once per distinct shape.
    group.bench_function("cache_miss", |b| {
        let mut i = 0usize;
        b.iter(|| {
            i = i.wrapping_add(1);
            let cache = TransparentCache::new(64);
            black_box(cache.resolve(black_box(pick(i)), parser::parse).0)
        })
    });

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
