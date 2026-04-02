# UCS Load Test — Compiled Project Plan

> Agent-executable plan with dependency DAG, parallel batches, and verification.
>
> Source artifacts: `ucs-load-test-architecture.md`

## Task Work Packets

### Batch 1: Foundation (no dependencies — all parallelizable)

---

#### WP-001: LoadProfile struct and predefined profiles

**Sprint:** 1 | **Size:** S

**Context:** The `LoadProfile` struct configures each load test variant. Three predefined profiles cover the test matrix: read-heavy, balanced, write-heavy. Each profile controls operation ratios, key space, concurrency, cache size, and target data volume.

**Files to create:**
- `ferrosa-storage/src/load/mod.rs`
- `ferrosa-storage/src/load/profile.rs`

**Implementation:**
1. Create `ferrosa-storage/src/load/` module with `mod.rs` re-exporting submodules
2. Define `LoadProfile` struct with fields: `name`, `read_ratio`, `write_ratio`, `key_space_size`, `value_size_range`, `num_writers`, `num_readers`, `duration`, `flush_threshold_bytes`, `local_cache_max_bytes`, `target_data_size_bytes`, `fan_factor`
3. Add `pub mod load;` to `ferrosa-storage/src/lib.rs`
4. Three factory functions:
   - `read_heavy()` — 90/10, 100K keys, 1-4 KB values, 4w/16r, 10 MB cache, 256 KB flush, 200 MB target, W=4
   - `balanced()` — 50/50, 50K keys, 1-4 KB values, 8w/8r, 10 MB cache, 128 KB flush, 200 MB target, W=4
   - `write_heavy()` — 10/90, 200K keys, 1-4 KB values, 16w/4r, 10 MB cache, 64 KB flush, 500 MB target, W=4

**Tests:**
```rust
#[test]
fn load_profile_read_heavy_ratios() {
    let p = LoadProfile::read_heavy();
    assert_eq!(p.read_ratio, 0.9);
    assert_eq!(p.write_ratio, 0.1);
    assert!(p.num_readers > p.num_writers);
}

#[test]
fn load_profile_write_heavy_ratios() {
    let p = LoadProfile::write_heavy();
    assert_eq!(p.read_ratio, 0.1);
    assert_eq!(p.write_ratio, 0.9);
    assert!(p.num_writers > p.num_readers);
}

#[test]
fn load_profile_ratios_sum_to_one() {
    for p in [LoadProfile::read_heavy(), LoadProfile::balanced(), LoadProfile::write_heavy()] {
        assert!((p.read_ratio + p.write_ratio - 1.0).abs() < f64::EPSILON);
    }
}

#[test]
fn load_profile_target_exceeds_cache() {
    for p in [LoadProfile::read_heavy(), LoadProfile::balanced(), LoadProfile::write_heavy()] {
        assert!(p.target_data_size_bytes > p.local_cache_max_bytes);
    }
}
```

**Verification:** `cargo test -p ferrosa-storage load::profile`

---

#### WP-002: Proptest strategies for keys, values, operations

**Sprint:** 1 | **Size:** S

**Files to create:**
- `ferrosa-storage/src/load/generator.rs`

**Implementation:**
1. `key_strategy(key_space: usize) -> impl Strategy<Value = String>` — generates keys `k00000000` through `k{key_space-1}`, zero-padded for sort order
2. `value_strategy(min: usize, max: usize) -> impl Strategy<Value = Vec<u8>>` — random byte vectors in size range
3. `OpType` enum: `Read`, `Write`
4. `operation_strategy(read_ratio: f64) -> impl Strategy<Value = OpType>` — weighted boolean mapped to op type
5. `WorkloadBatch` struct: `Vec<(String, OpType, Option<Vec<u8>>)>` — a batch of operations with keys and optional write values
6. `workload_batch_strategy(profile: &LoadProfile, batch_size: usize) -> impl Strategy<Value = WorkloadBatch>` — generates a batch of operations according to the profile

**Tests:**
```rust
proptest! {
    #[test]
    fn key_strategy_in_range(key in key_strategy(1000)) {
        let num: usize = key[1..].parse().unwrap();
        prop_assert!(num < 1000);
    }

    #[test]
    fn value_strategy_in_size_range(val in value_strategy(100, 4096)) {
        prop_assert!(val.len() >= 100 && val.len() <= 4096);
    }

    #[test]
    fn operation_strategy_distribution(ops in prop::collection::vec(operation_strategy(0.9), 1000)) {
        let reads = ops.iter().filter(|o| matches!(o, OpType::Read)).count();
        // With 90% read ratio, expect 800-1000 reads out of 1000 (loose bound)
        prop_assert!(reads > 700);
    }
}
```

**Verification:** `cargo test -p ferrosa-storage load::generator`

---

#### WP-003: Ground truth tracker (thread-safe)

**Sprint:** 1 | **Size:** M

**Files to create:**
- `ferrosa-storage/src/load/ground_truth.rs`

**Implementation:**
1. `GroundTruth` struct: 64 shards (parking_lot::Mutex<HashMap<String, (Vec<u8>, i64)>>), atomics for write/read/hit/miss counters
2. `new() -> Self` — initialize empty shards
3. `record_write(&self, key: &str, value: &[u8], timestamp: i64)` — update shard for key
4. `expected_value(&self, key: &str) -> Option<(Vec<u8>, i64)>` — read expected value
5. `record_read(&self, key: &str, got: Option<&[u8]>) -> ReadResult` — compare against expected, increment counters, return `Match | Mismatch | Missing | NotYetWritten`
6. `snapshot(&self) -> GroundTruthSnapshot` — clone all entries (for final verification)
7. `stats(&self) -> (u64, u64, u64, u64)` — (writes, reads, hits, misses)

**Key invariant:** Sharding by `key.as_bytes()[0] % 64` (or hash) to minimize contention.

**Tests:**
```rust
#[test]
fn ground_truth_write_then_read() {
    let gt = GroundTruth::new();
    gt.record_write("k1", b"hello", 1000);
    let result = gt.record_read("k1", Some(b"hello"));
    assert_eq!(result, ReadResult::Match);
}

#[test]
fn ground_truth_mismatch_detected() {
    let gt = GroundTruth::new();
    gt.record_write("k1", b"hello", 1000);
    let result = gt.record_read("k1", Some(b"wrong"));
    assert_eq!(result, ReadResult::Mismatch);
}

#[test]
fn ground_truth_missing_detected() {
    let gt = GroundTruth::new();
    gt.record_write("k1", b"hello", 1000);
    let result = gt.record_read("k1", None);
    assert_eq!(result, ReadResult::Missing);
}

#[test]
fn ground_truth_last_write_wins() {
    let gt = GroundTruth::new();
    gt.record_write("k1", b"v1", 1000);
    gt.record_write("k1", b"v2", 2000);
    let (val, ts) = gt.expected_value("k1").unwrap();
    assert_eq!(val, b"v2");
    assert_eq!(ts, 2000);
}

#[test]
fn ground_truth_concurrent_writes() {
    let gt = Arc::new(GroundTruth::new());
    let handles: Vec<_> = (0..16).map(|t| {
        let gt = gt.clone();
        std::thread::spawn(move || {
            for i in 0..1000 {
                gt.record_write(&format!("k{}", i % 100), &[t as u8; 64], (t * 1000 + i) as i64);
            }
        })
    }).collect();
    for h in handles { h.join().unwrap(); }
    let (writes, _, _, _) = gt.stats();
    assert_eq!(writes, 16_000);
}
```

**Verification:** `cargo test -p ferrosa-storage load::ground_truth`

---

#### WP-004: Stats collector and reporting

**Sprint:** 1 | **Size:** S

**Files to create:**
- `ferrosa-storage/src/load/stats.rs`

**Implementation:**
1. `LoadStats` struct: all fields from architecture spec
2. `StatsSnapshot` struct: periodic sample with timestamp, counters, rates
3. `StatsCollector` struct: collects periodic snapshots, computes rates
4. `new(interval: Duration) -> Self`
5. `record_write(&self)`, `record_read(&self)`, `record_error(&self, is_write: bool)`
6. `snapshot(&self, engine: &StorageEngine, table_id: &TableId) -> StatsSnapshot` — capture current counters + engine stats
7. `finalize(self) -> LoadStats` — compute totals and rates
8. `Display` impl for `LoadStats` — formatted summary output

**Tests:**
```rust
#[test]
fn stats_collector_tracks_writes_and_reads() {
    let sc = StatsCollector::new(Duration::from_secs(5));
    for _ in 0..100 { sc.record_write(); }
    for _ in 0..50 { sc.record_read(); }
    let stats = sc.finalize();
    assert_eq!(stats.total_writes, 100);
    assert_eq!(stats.total_reads, 50);
}

#[test]
fn stats_rates_computed_correctly() {
    let sc = StatsCollector::new(Duration::from_secs(5));
    // Simulate: 1000 writes over ~1 second
    for _ in 0..1000 { sc.record_write(); }
    std::thread::sleep(Duration::from_millis(100));
    let stats = sc.finalize();
    assert!(stats.writes_per_sec > 0.0);
}
```

**Verification:** `cargo test -p ferrosa-storage load::stats`

---

### Batch 2: Integrity and Orchestration (depends on Batch 1)

---

#### WP-005: Integrity verifier (full scan + sample check)

**Sprint:** 2 | **Size:** M | **Depends on:** WP-003

**Files to create:**
- `ferrosa-storage/src/load/integrity.rs`

**Implementation:**
1. `IntegrityReport` struct: `keys_checked`, `keys_ok`, `missing_keys: Vec<String>`, `mismatched_keys: Vec<(String, String)>`, `elapsed: Duration`
2. `verify_all(engine, table_id, ground_truth) -> IntegrityReport` — iterate all keys in ground truth snapshot, read each from engine, compare
3. `verify_sample(engine, table_id, ground_truth, sample_size) -> IntegrityReport` — random sample of N keys from ground truth
4. Both methods handle: key present + correct, key present + wrong value, key missing (not found in engine)

**Tests:**
```rust
#[test]
fn integrity_all_keys_present() {
    // Setup engine with known writes
    // verify_all reports 0 missing, 0 mismatched
}

#[test]
fn integrity_detects_missing_key() {
    // Write to ground truth but not engine
    // verify_all reports 1 missing
}

#[test]
fn integrity_detects_mismatch() {
    // Write different values to ground truth vs engine
    // verify_all reports 1 mismatched
}

#[test]
fn integrity_sample_subset_of_full() {
    // verify_sample(100) checks <= 100 keys
}
```

**Verification:** `cargo test -p ferrosa-storage load::integrity`

---

#### WP-006: Load test orchestrator

**Sprint:** 2 | **Size:** L | **Depends on:** WP-001, WP-002, WP-003, WP-004, WP-005

**Files to create:**
- `ferrosa-storage/src/load/orchestrator.rs`

**Implementation:**
1. `run_load_test(profile: &LoadProfile, engine_config: StorageEngineConfig) -> LoadStats`
2. Steps:
   a. Create `StorageEngine` from config
   b. Register table with UCS compaction (`fan_factor` from profile)
   c. Create `GroundTruth`, `StatsCollector`
   d. Spawn `profile.num_writers` tokio tasks — each loops: pick random key from key_space, generate random value, write to engine + ground_truth, record stats
   e. Spawn `profile.num_readers` tokio tasks — each loops: pick random key, read from engine, check against ground_truth, record stats
   f. Spawn stats snapshot task — every 5s: take snapshot from collector
   g. Spawn periodic integrity task — every 30s: `verify_sample(engine, ground_truth, 1000)`
   h. Run until `profile.duration` elapses or `target_data_size_bytes` reached
   i. Stop all tasks via `CancellationToken`
   j. Final `verify_all` integrity check
   k. Return `LoadStats`

3. Writer task inner loop:
   ```rust
   let key = format!("k{:08}", rng.gen_range(0..profile.key_space_size));
   let value_len = rng.gen_range(profile.value_size_range.0..=profile.value_size_range.1);
   let value: Vec<u8> = (0..value_len).map(|_| rng.gen()).collect();
   let timestamp = chrono::Utc::now().timestamp_micros();
   engine.write(&table_id, &decorated_key, row, timestamp)?;
   ground_truth.record_write(&key, &value, timestamp);
   stats.record_write();
   ```

4. Reader task inner loop:
   ```rust
   let key = format!("k{:08}", rng.gen_range(0..profile.key_space_size));
   let result = engine.read(&table_id, &decorated_key);
   ground_truth.record_read(&key, result.as_deref());
   stats.record_read();
   ```

**Tests:**
```rust
#[tokio::test]
async fn orchestrator_runs_without_panic() {
    let mut profile = LoadProfile::balanced();
    profile.duration = Duration::from_secs(2);
    profile.key_space_size = 100;
    profile.target_data_size_bytes = 1024 * 1024; // 1 MB
    let stats = run_load_test(&profile, test_config()).await;
    assert!(stats.total_writes > 0);
    assert!(stats.total_reads > 0);
    assert_eq!(stats.missing_keys, 0);
    assert_eq!(stats.data_mismatches, 0);
}
```

**Verification:** `cargo test -p ferrosa-storage load::orchestrator`

---

### Batch 3: End-to-End Test Entries (depends on Batch 2)

---

#### WP-007: In-process load tests (3 profiles, no S3)

**Sprint:** 3 | **Size:** M | **Depends on:** WP-006

**Files to create:**
- `ferrosa-storage/tests/ucs_load_test.rs`

**Implementation:** Three tests, one per profile. Each uses `StorageEngine` with local-only storage (no S3). Verifies data integrity, compaction triggers, and performance stats.

```rust
#[tokio::test]
async fn ucs_load_read_heavy() {
    let mut profile = LoadProfile::read_heavy();
    profile.duration = Duration::from_secs(30);
    profile.target_data_size_bytes = 50 * 1024 * 1024; // 50 MB
    let stats = run_load_test(&profile, local_config()).await;

    assert_eq!(stats.missing_keys, 0, "no data loss");
    assert_eq!(stats.data_mismatches, 0, "no corruption");
    assert!(stats.compaction_tasks_completed > 0, "compaction must trigger");
    assert!(stats.reads_per_sec > 0.0);
    println!("{stats}"); // print summary
}

#[tokio::test]
async fn ucs_load_balanced() { /* similar, 50/50 */ }

#[tokio::test]
async fn ucs_load_write_heavy() { /* similar, 10/90, higher data target */ }
```

**Verification:** `cargo test -p ferrosa-storage --test ucs_load_test`

---

#### WP-008: S3 pipeline load tests (RustFS, exceeds disk)

**Sprint:** 3 | **Size:** L | **Depends on:** WP-006

**Files to create:**
- `ferrosa-storage/tests/ucs_load_s3_test.rs`

**Implementation:** Same three profiles but with S3 enabled via RustFS. Requires `FERROSA_TEST_CONTAINERS=1`. Uses dedicated compose file `tests/docker-compose.compaction-test.yml` with 2xxxx ports to avoid conflicts with the main cluster. Sets `local_cache_max_bytes` = 10 MB and generates 200+ MB to force S3 eviction/read-back.

```bash
# Start infrastructure (separate from main cluster)
docker compose -f tests/docker-compose.compaction-test.yml up -d --build

# Run tests
FERROSA_TEST_CONTAINERS=1 \
FERROSA_COMPACTION_TEST_NODES="127.0.0.1:29042,127.0.0.1:29043,127.0.0.1:29044" \
FERROSA_COMPACTION_TEST_S3_ENDPOINT="http://127.0.0.1:29000" \
cargo test -p ferrosa-storage --test ucs_load_s3_test -- --nocapture
```

```rust
#[tokio::test]
async fn ucs_load_s3_write_heavy() {
    if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
        panic!("FERROSA_TEST_CONTAINERS not set — run: docker compose -f tests/docker-compose.compaction-test.yml up -d --build");
    }

    let mut profile = LoadProfile::write_heavy();
    profile.duration = Duration::from_secs(120);
    profile.target_data_size_bytes = 200 * 1024 * 1024; // 200 MB
    profile.local_cache_max_bytes = 10 * 1024 * 1024;   // 10 MB cache
    
    // RustFS on port 29000 (compaction-test compose)
    let config = s3_config(); // reads FERROSA_COMPACTION_TEST_S3_ENDPOINT
    let stats = run_load_test(&profile, config).await;

    assert_eq!(stats.missing_keys, 0, "no data loss");
    assert_eq!(stats.data_mismatches, 0, "no corruption");
    assert!(stats.s3_uploads > 0, "S3 uploads must occur");
    assert!(stats.s3_reads > 0, "S3 reads must occur (cache eviction)");
    assert!(stats.compaction_tasks_completed > 0, "compaction must trigger");
    assert!(stats.bytes_written > profile.local_cache_max_bytes,
            "data must exceed cache to force S3 reads");
    println!("{stats}");
}
```

**Verification:**
```bash
docker compose -f tests/docker-compose.compaction-test.yml up -d --build
FERROSA_TEST_CONTAINERS=1 \
FERROSA_COMPACTION_TEST_S3_ENDPOINT="http://127.0.0.1:29000" \
cargo test -p ferrosa-storage --test ucs_load_s3_test -- --nocapture
```

---

#### WP-009: Proptest-driven fuzz profiles

**Sprint:** 3 | **Size:** M | **Depends on:** WP-006

**Files to add to:**
- `ferrosa-storage/tests/ucs_load_test.rs`

**Implementation:** Proptest that generates random LoadProfile parameters within safe bounds, runs a short load test, and verifies invariants hold regardless of configuration.

```rust
proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]
    #[test]
    fn ucs_load_random_profile(
        read_pct in 0.0f64..=1.0,
        key_space in 10usize..=10_000,
        val_min in 64usize..=512,
        val_max in 512usize..=4096,
        fan_factor in 2u32..=16,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let profile = LoadProfile {
                name: "fuzz".into(),
                read_ratio: read_pct,
                write_ratio: 1.0 - read_pct,
                key_space_size: key_space,
                value_size_range: (val_min, val_max.max(val_min + 1)),
                num_writers: 4,
                num_readers: 4,
                duration: Duration::from_secs(3),
                flush_threshold_bytes: 64 * 1024,
                local_cache_max_bytes: 5 * 1024 * 1024,
                target_data_size_bytes: 5 * 1024 * 1024,
                fan_factor,
            };
            let stats = run_load_test(&profile, local_config()).await;
            prop_assert_eq!(stats.missing_keys, 0);
            prop_assert_eq!(stats.data_mismatches, 0);
        });
    }
}
```

**Verification:** `cargo test -p ferrosa-storage --test ucs_load_test ucs_load_random_profile`

---

#### WP-010: Compaction correctness under load

**Sprint:** 3 | **Size:** M | **Depends on:** WP-006

**Files to add to:**
- `ferrosa-storage/tests/ucs_load_test.rs`

**Implementation:** Verify that UCS compaction produces the correct number of level transitions and that SSTable count stays bounded.

```rust
#[tokio::test]
async fn ucs_compaction_reduces_sstable_count() {
    let mut profile = LoadProfile::write_heavy();
    profile.duration = Duration::from_secs(30);
    profile.fan_factor = 2; // aggressive compaction
    profile.flush_threshold_bytes = 32 * 1024; // 32 KB → many small SSTables
    
    let stats = run_load_test(&profile, local_config()).await;
    
    assert!(stats.compaction_tasks_completed > 0);
    // With fan_factor=2 and many flushes, SSTable count should be bounded
    assert!(stats.sstable_count_final < 20,
            "UCS with W=2 should keep SSTable count low; got {}",
            stats.sstable_count_final);
    assert_eq!(stats.missing_keys, 0);
    assert_eq!(stats.data_mismatches, 0);
}
```

**Verification:** `cargo test -p ferrosa-storage --test ucs_load_test ucs_compaction_reduces`

---

## Dependency DAG

```
Batch 1 (parallel):
  WP-001 ─┐
  WP-002 ─┤
  WP-003 ─┼─→ Batch 2
  WP-004 ─┘

Batch 2 (depends on Batch 1):
  WP-005 (needs WP-003) ─┐
  WP-006 (needs all B1)  ─┼─→ Batch 3

Batch 3 (depends on Batch 2):
  WP-007 (in-process) ─┐
  WP-008 (S3/RustFS)   ─┤  all parallel
  WP-009 (fuzz)        ─┤
  WP-010 (compaction)  ─┘
```

## Verification Protocol

### Tier 1: Unit (per work packet)
Each WP has specific `cargo test` command in its verification field.

### Tier 2: Integration (per batch)
- After Batch 1: `cargo test -p ferrosa-storage load::`
- After Batch 2: `cargo test -p ferrosa-storage load::integrity load::orchestrator`
- After Batch 3: `cargo test -p ferrosa-storage --test ucs_load_test`

### Tier 3: Full suite
```bash
cargo fmt -- --check
cargo clippy -p ferrosa-storage --all-targets
cargo test -p ferrosa-storage --test ucs_load_test

# S3 tests (requires compaction-test compose on 2xxxx ports)
docker compose -f tests/docker-compose.compaction-test.yml up -d --build
FERROSA_TEST_CONTAINERS=1 \
FERROSA_COMPACTION_TEST_S3_ENDPOINT="http://127.0.0.1:29000" \
cargo test -p ferrosa-storage --test ucs_load_s3_test -- --nocapture
docker compose -f tests/docker-compose.compaction-test.yml down -v
```

## Status Tracking

| WP | Description | Status | Verified |
|----|-------------|--------|----------|
| WP-001 | LoadProfile struct + predefined profiles | complete | load::profile (6 tests) |
| WP-002 | Proptest strategies (keys, values, ops) | complete | load::generator (5 tests) |
| WP-003 | Ground truth tracker (thread-safe) | complete | load::ground_truth (10 tests) |
| WP-004 | Stats collector and reporting | complete | load::stats (5 tests) |
| WP-005 | Integrity verifier (full scan + sample) | complete | load::integrity |
| WP-006 | Load test orchestrator | complete | load::orchestrator |
| WP-007 | In-process load tests (3 profiles) | complete | ucs_load_test (3 tests) |
| WP-008 | S3 pipeline load tests (RustFS) | complete | ucs_load_s3_test (3 tests, all PASS) |
| WP-009 | Proptest-driven fuzz profiles | complete | ucs_load_test (1 proptest) |
| WP-010 | Compaction correctness under load | complete | ucs_load_test (1 test) |
