//! Reject incompatible RDF join candidates without copying their rows.
use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine, SparqlResult};
use ferrosa_storage::{StorageEngine, StorageEngineConfig};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

struct CountingAllocator;
thread_local! {
    static BYTES: Cell<(bool, usize)> = const { Cell::new((false, 0)) };
}

fn allocated(bytes: usize) {
    let _ = BYTES.try_with(|counter| {
        let (enabled, total) = counter.get();
        if enabled {
            counter.set((true, total + bytes));
        }
    });
}

// SAFETY: forward System's pointer/layout contract unchanged. Const TLS neither
// allocates nor borrows heap state; try_with tolerates thread teardown.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocated(size);
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

async fn sample(n: usize) -> usize {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.flush_threshold_bytes = 64 * 1024 * 1024;
    config.commit_log.segment_size = 4 * 1024 * 1024;
    let storage = Arc::new(StorageEngine::new(config, None).unwrap());
    let engine = SparqlEngine::new(
        Arc::clone(&storage),
        Arc::new(WritePath::direct(storage)),
        SparqlConfig {
            default_graph: "default".into(),
            max_rows: 10000,
        },
    );
    let mut update = String::from("INSERT DATA {");
    for i in 0..n {
        update.push_str(&format!(
            "<urn:s{i}> <urn:pk> {i}; <urn:bucket> {}; <urn:payload> \"{}\" .",
            i % 8,
            "x".repeat(96)
        ));
    }
    update.push('}');
    engine.execute_update(&update, "default").await.unwrap();
    let query = "SELECT ?pk ?bucket ?payload WHERE { ?s <urn:pk> ?pk; <urn:bucket> ?bucket; <urn:payload> ?payload } LIMIT 10000";
    engine.execute(query, "default").await.unwrap();
    BYTES.with(|counter| counter.set((true, 0)));
    let started = std::time::Instant::now();
    let result = engine.execute(query, "default").await.unwrap();
    let elapsed = started.elapsed();
    let (_, bytes) = BYTES.with(|counter| counter.replace((false, 0)));
    let SparqlResult::Select(result) = result else {
        panic!("SELECT result")
    };
    assert_eq!(result.results.bindings.len(), n);
    let mut keys = std::collections::HashSet::new();
    for row in result.results.bindings {
        let key: usize = row["pk"].scalar_value().unwrap().parse().unwrap();
        assert!(keys.insert(key));
        assert_eq!(row["bucket"].scalar_value().unwrap(), (key % 8).to_string());
        assert_eq!(row["payload"].scalar_value().unwrap(), "x".repeat(96));
    }
    assert_eq!(keys, (0..n).collect());
    eprintln!("join n={n}: requested={bytes} bytes, elapsed={elapsed:?}");
    bytes
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_join_candidates_do_not_allocate_quadratically() {
    let small = sample(32).await;
    let large = sample(128).await;
    assert!(
        large < small * 7,
        "4x rows requested {small} -> {large} bytes: rejected pairs must not clone solutions"
    );
}

#[tokio::test]
async fn borrowed_subject_join_preserves_bags_and_repeated_variables() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap());
    let engine = SparqlEngine::new(
        Arc::clone(&storage),
        Arc::new(WritePath::direct(storage)),
        SparqlConfig {
            default_graph: "default".into(),
            max_rows: 100,
        },
    );
    engine.execute_update("INSERT DATA { <urn:a> <urn:p> 1, 2; <urn:q> 3, 4; <urn:self> <urn:a> . <urn:b> <urn:self> <urn:c> . }", "default").await.unwrap();
    let SparqlResult::Select(result) = engine
        .execute(
            "SELECT ?p ?q WHERE { ?s <urn:p> ?p; <urn:q> ?q }",
            "default",
        )
        .await
        .unwrap()
    else {
        panic!("SELECT")
    };
    let values: std::collections::HashSet<_> = result
        .results
        .bindings
        .iter()
        .map(|row| {
            (
                row["p"].scalar_value().unwrap(),
                row["q"].scalar_value().unwrap(),
            )
        })
        .collect();
    assert_eq!(result.results.bindings.len(), 4);
    assert_eq!(
        values,
        [("1", "3"), ("1", "4"), ("2", "3"), ("2", "4")]
            .into_iter()
            .collect()
    );
    let SparqlResult::Select(result) = engine
        .execute("SELECT ?s WHERE { ?s <urn:self> ?s }", "default")
        .await
        .unwrap()
    else {
        panic!("SELECT")
    };
    assert_eq!(result.results.bindings.len(), 1);
    assert_eq!(
        result.results.bindings[0]["s"].scalar_value(),
        Some("urn:a")
    );
}
