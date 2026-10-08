//! `pgo-workload` — deterministic, self-terminating PGO training run for the
//! shipped `ferrosa` binary.
//!
//! This is not a product surface. It exists so an *instrumented* build can be
//! executed to emit `.profraw` files, and so a *profile-use* build can be
//! executed to confirm the optimized binary still behaves.
//!
//! ```text
//! target/release/pgo-workload            # emits profraw, prints a summary
//! target/release/pgo-workload --check-hits --json
//! ```
//!
//! It drives the **real** `StorageEngine` in-process: the same memtable,
//! flush, compaction, SSTable-read and merge code the server runs. That is a
//! strictly better training input than a mock store, and it covers the hot
//! inner code a profile can actually reach. What it does NOT cover is the
//! network/server shell — the CQL wire path, the internode RPC, TLS and the
//! tokio accept loop — because those need sockets and a peer. Training those
//! means running the shipped binary against a real cluster and stopping it
//! gracefully; see `specs/pgo-release-build.md`.
//!
//! ## Why this is a separate bin in the `ferrosa` package, not a new crate
//!
//! `-C metadata` — the crate disambiguator rustc bakes into every mangled
//! symbol and matches profile data against — derives from the package,
//! version, target, profile and the *set* of enabled features. Cargo passes
//! only the features enabled *on this package* into that hash, not the ones a
//! local dependency enables for itself. So if the trainer were a separate
//! crate that turns on a `ferrosa-storage` feature the server does not (for
//! instance `compaction-validator`), only the trainer's build of
//! `ferrosa-storage` would carry the extra feature; the `ferrosa` build would
//! not, the two builds would disagree, and `-Cprofile-use` would silently
//! discard the storage crate's profile while exiting 0. Hosting the trainer
//! here, behind `ferrosa`'s own `pgo-bench` feature, keeps every library crate
//! byte-for-byte identical in configuration between the instrumented run and
//! the optimized build.
//!
//! ## Determinism
//!
//! Fixed seed, a local xorshift PRNG, and a fixed operation count. The run
//! terminates on its own terms (it does not wait on a wall clock), which is
//! the property the profile depends on: an instrumented process that is
//! SIGKILLed writes a profraw whose counters are all zero, and the compiler
//! accepts that file while optimizing nothing. See
//! `specs/pgo-release-build.md` for the measurement.
//!
//! Exit status is the contract:
//!
//! * `0` — the run stayed inside its error budget (and hit its search path,
//!   when `--check-hits` is passed).
//! * `1` — the run fell below `PGO_WORKLOAD_MIN_OK_RATE`, or every read missed.
//! * `2` — a malformed environment knob, or the engine failed to start.

use std::process::ExitCode;
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};

/// A small, well-behaved PRNG. No external crate, no global state, and the
/// same sequence on every platform — which is what makes two runs of one
/// revision produce an identical profile input.
struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> Self {
        // Never zero: xorshift's fixed point is 0 and would emit only zeroes.
        Self(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        // `n` is small in every call site here, so the multiply-high bias is
        // immaterial; this avoids a modulo and its division.
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }

    fn value(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            out.extend_from_slice(&self.next_u64().to_le_bytes());
        }
        out.truncate(len);
        out
    }
}

#[derive(Debug)]
struct Config {
    iterations: u64,
    seed: u64,
    num_keys: u64,
    value_len: usize,
    min_ok_rate: f64,
    check_hits: bool,
    json: bool,
    data_dir: std::path::PathBuf,
}

/// A malformed knob is a hard error: a profile gathered at the wrong iteration
/// count, or with a different corpus, is worse than no profile at all. The
/// script reads these defaults out of this file's `DEFAULT_*` constants.
const DEFAULT_ITERATIONS: u64 = 20_000;
const DEFAULT_SEED: u64 = 0x5EED_F00D_1234_5678;
const DEFAULT_NUM_KEYS: u64 = 4_096;
const DEFAULT_VALUE_LEN: usize = 512;
const DEFAULT_MIN_OK_RATE: f64 = 0.99;

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Err(_) => Ok(default),
        Ok(v) if v.trim().is_empty() => Ok(default),
        Ok(v) => v
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("{name}={v:?} is not an unsigned integer")),
    }
}

fn env_usize(name: &str, default: usize) -> Result<usize, String> {
    Ok(env_u64(name, default as u64)? as usize)
}

fn env_f64(name: &str, default: f64) -> Result<f64, String> {
    match std::env::var(name) {
        Err(_) => Ok(default),
        Ok(v) if v.trim().is_empty() => Ok(default),
        Ok(v) => v
            .trim()
            .parse::<f64>()
            .map_err(|_| format!("{name}={v:?} is not a number")),
    }
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let mut cfg = Config {
            iterations: env_u64("PGO_WORKLOAD_ITERATIONS", DEFAULT_ITERATIONS)?,
            seed: env_u64("PGO_WORKLOAD_SEED", DEFAULT_SEED)?,
            num_keys: env_u64("PGO_WORKLOAD_NUM_KEYS", DEFAULT_NUM_KEYS)?,
            value_len: env_usize("PGO_WORKLOAD_VALUE_LEN", DEFAULT_VALUE_LEN)?,
            min_ok_rate: env_f64("PGO_WORKLOAD_MIN_OK_RATE", DEFAULT_MIN_OK_RATE)?,
            check_hits: false,
            json: false,
            data_dir: std::env::temp_dir()
                .join(format!("ferrosa-pgo-workload-{}", std::process::id())),
        };
        for arg in std::env::args().skip(1) {
            match arg.as_str() {
                "--check-hits" => cfg.check_hits = true,
                "--json" => cfg.json = true,
                "-h" | "--help" => {
                    println!(
                        "pgo-workload [--json] [--check-hits]\n\n\
                         Env: PGO_WORKLOAD_ITERATIONS, PGO_WORKLOAD_SEED,\n\
                         \x20    PGO_WORKLOAD_NUM_KEYS, PGO_WORKLOAD_VALUE_LEN,\n\
                         \x20    PGO_WORKLOAD_MIN_OK_RATE"
                    );
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument {other:?}")),
            }
        }
        if cfg.iterations == 0 {
            return Err("PGO_WORKLOAD_ITERATIONS must be > 0".into());
        }
        if cfg.num_keys == 0 {
            return Err("PGO_WORKLOAD_NUM_KEYS must be > 0".into());
        }
        if !(0.0..=1.0).contains(&cfg.min_ok_rate) {
            return Err("PGO_WORKLOAD_MIN_OK_RATE must be in [0, 1]".into());
        }
        Ok(cfg)
    }
}

fn load_test_schema() -> TableSchema {
    TableSchema {
        keyspace: "pgo".to_string(),
        table: "training".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.BytesType".to_string(),
        }],
        extensions: Default::default(),
    }
}

fn make_key(s: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
}

fn make_row(value: &[u8], timestamp: i64) -> Row {
    Row {
        clustering: vec![0x00, 0x00, 0x00, 0x01],
        cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

fn make_tombstone_row(timestamp: i64) -> Row {
    Row {
        clustering: vec![0x00, 0x00, 0x00, 0x01],
        cells: vec![],
        deletion: DeletionTime::new(timestamp, timestamp as u32),
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

struct Outcome {
    calls: u64,
    errors: u64,
    reads: u64,
    read_hits: u64,
    flushes: u64,
    compactions: u64,
}

impl Outcome {
    fn ok_rate(&self) -> f64 {
        if self.calls == 0 {
            0.0
        } else {
            (self.calls - self.errors) as f64 / self.calls as f64
        }
    }
}

fn main() -> ExitCode {
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("pgo-workload: {e}");
            return ExitCode::from(2);
        }
    };

    if std::fs::remove_dir_all(&cfg.data_dir).is_ok() {
        // Fresh profile every run: a stale commit log would add startup work
        // the shipped binary does not have, and would drift the profile.
    }
    if let Err(e) = std::fs::create_dir_all(&cfg.data_dir) {
        eprintln!("pgo-workload: cannot create data dir: {e}");
        return ExitCode::from(2);
    }

    let engine_cfg = StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 32 * 1024 * 1024,
            max_segment_age: Duration::from_secs(300),
            sync_strategy: SyncStrategyConfig::Periodic {
                sync_interval: Duration::from_millis(10),
            },
            batch: Default::default(),
            log_dir: cfg.data_dir.join("commitlog"),
            checkpoint_dir: cfg.data_dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(cfg.data_dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 256 * 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 8 * 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 30,
        data_dir: cfg.data_dir.clone(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 16,
        cache_hot_window_secs: 900,
        write_verify: false,
    };

    let engine = match StorageEngine::new(engine_cfg, None) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("pgo-workload: cannot start engine: {e}");
            return ExitCode::from(2);
        }
    };

    let table_id = TableId::new("pgo", "training");
    if let Err(e) = engine.register_table(load_test_schema()) {
        eprintln!("pgo-workload: register_table failed: {e}");
        let _ = std::fs::remove_dir_all(&cfg.data_dir);
        return ExitCode::from(2);
    }

    // Compaction polling needs a runtime; the load path itself does not.
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("pgo-workload: cannot start runtime: {e}");
            let _ = std::fs::remove_dir_all(&cfg.data_dir);
            return ExitCode::from(2);
        }
    };

    let out = run(&cfg, &engine, &table_id, &rt);

    // An orderly shutdown: the engine flushes and closes its commit log. The
    // profraw is written by the runtime at process exit, and this path exits
    // cleanly (never by signal), which is what makes the counters non-zero.
    if let Err(e) = engine.shutdown() {
        eprintln!("pgo-workload: engine shutdown failed: {e}");
        let _ = std::fs::remove_dir_all(&cfg.data_dir);
        return ExitCode::from(2);
    }
    let _ = std::fs::remove_dir_all(&cfg.data_dir);

    if cfg.json {
        println!(
            "{{\"iterations\":{},\"calls\":{},\"errors\":{},\"ok_rate\":{:.6},\"reads\":{},\"read_hits\":{},\"flushes\":{},\"compactions\":{},\"seed\":{}}}",
            cfg.iterations,
            out.calls,
            out.errors,
            out.ok_rate(),
            out.reads,
            out.read_hit_count(),
            out.flushes,
            out.compactions,
            cfg.seed,
        );
    } else {
        println!(
            "pgo-workload: {} calls, {} errors, ok_rate {:.4}, {} reads ({} hit), {} flushes, {} compactions, seed {:#x}",
            out.calls,
            out.errors,
            out.ok_rate(),
            out.reads,
            out.read_hit_count(),
            out.flushes,
            out.compactions,
            cfg.seed,
        );
    }

    if out.ok_rate() < cfg.min_ok_rate {
        eprintln!(
            "pgo-workload: ok_rate {:.4} below required {:.4} — refusing to profile this run",
            out.ok_rate(),
            cfg.min_ok_rate
        );
        return ExitCode::FAILURE;
    }
    if cfg.check_hits && out.reads > 0 && out.read_hits == 0 {
        eprintln!(
            "pgo-workload: {} reads returned nothing; refusing to profile a read path that \
             never hits",
            out.reads
        );
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

impl Outcome {
    fn read_hit_count(&self) -> u64 {
        self.read_hits
    }
}

fn run(
    cfg: &Config,
    engine: &StorageEngine,
    table_id: &TableId,
    rt: &tokio::runtime::Runtime,
) -> Outcome {
    let mut rng = XorShift::new(cfg.seed);
    let mut out = Outcome {
        calls: 0,
        errors: 0,
        reads: 0,
        read_hits: 0,
        flushes: 0,
        compactions: 0,
    };
    let mut ts: i64 = 1;

    // Flush every `flush_every` iterations so the run spends real time in the
    // flush + SSTable-write path, then poll compactions so the merge path runs
    // too. Without this the profile covers only memtable inserts.
    let flush_every = (cfg.iterations / 20).max(1);

    for i in 0..cfg.iterations {
        let key_idx = rng.below(cfg.num_keys);
        let key_str = format!("pgo-key-{key_idx:08}");
        let dk = make_key(&key_str);

        // 60% write, 15% update, 10% delete, 15% read — write-heavy, so the
        // profile sees the memtable and flush paths, but reads exercise the
        // merge path against whatever has already been flushed.
        let roll = rng.below(100);
        out.calls += 1;
        if roll < 75 {
            // 75% writes: 60% fresh inserts + 15% updates. The engine treats a
            // write to an existing key as an update, so both are the same call;
            // the split exists only to keep the key-reuse pattern realistic.
            ts += 1;
            let value = rng.value(cfg.value_len);
            if engine
                .write(table_id, &dk, make_row(&value, ts), ts)
                .is_err()
            {
                out.errors += 1;
            }
        } else if roll < 85 {
            ts += 1;
            if engine
                .write(table_id, &dk, make_tombstone_row(ts), ts)
                .is_err()
            {
                out.errors += 1;
            }
        } else {
            out.reads += 1;
            match engine.read(table_id, &dk) {
                Ok(Some(p)) => {
                    let hit = p
                        .rows
                        .iter()
                        .any(|r| r.cells.iter().any(|(_, c)| c.value.is_some()));
                    if hit {
                        out.read_hits += 1;
                    }
                }
                Ok(None) => {}
                Err(_) => out.errors += 1,
            }
        }

        if i % flush_every == 0 {
            if engine.flush(table_id).is_ok() {
                out.flushes += 1;
            }
            let _ = engine.discard_completed_commit_log_segments();
            rt.block_on(engine.poll_compactions());
            out.compactions += 1;
        }
    }

    // A final flush so the tail of the corpus also trains the writer.
    if engine.flush(table_id).is_ok() {
        out.flushes += 1;
    }
    rt.block_on(engine.poll_compactions());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two runs of one revision must produce the same operation sequence, or
    /// every profile is a different experiment and no A/B is meaningful.
    #[test]
    fn prng_is_deterministic_for_a_seed() {
        let seq = |seed| {
            let mut r = XorShift::new(seed);
            (0..64).map(|_| r.below(100)).collect::<Vec<_>>()
        };
        assert_eq!(seq(DEFAULT_SEED), seq(DEFAULT_SEED));
        assert_ne!(seq(DEFAULT_SEED), seq(DEFAULT_SEED ^ 1));
    }

    /// xorshift's fixed point is zero: a zero seed would emit only zeroes, so
    /// the constructor must map it away rather than trust the caller.
    #[test]
    fn zero_seed_is_not_a_fixed_point() {
        let mut r = XorShift::new(0);
        let first = r.next_u64();
        assert_ne!(first, 0, "zero seed produced a zero first value");
        assert_ne!(r.next_u64(), first);
    }

    /// The operation mix must actually be write-heavy: if the draws skew to
    /// reads, the profile trains the read path and not the flush/write path
    /// this workload exists to cover.
    #[test]
    fn operation_mix_is_write_heavy() {
        let mut r = XorShift::new(DEFAULT_SEED);
        let n = 10_000;
        let writes = (0..n).filter(|_| r.below(100) < 75).count();
        let frac = writes as f64 / n as f64;
        assert!(
            (0.70..0.80).contains(&frac),
            "writes were {frac:.3} of the mix; expected ~0.75"
        );
    }

    /// `below(n)` must stay in range for every n it is called with, including
    /// small ones where a naive modulo or a bad multiply-high would drift.
    #[test]
    fn below_stays_in_range() {
        let mut r = XorShift::new(DEFAULT_SEED);
        for n in [1u64, 2, 3, 9, 100, 4096] {
            for _ in 0..1000 {
                assert!(r.below(n) < n, "below({n}) returned out of range");
            }
        }
    }

    /// A malformed knob is a hard error, never a silent fallback: a profile
    /// gathered at the wrong iteration count is worse than no profile.
    #[test]
    fn malformed_env_is_an_error() {
        // This binary's only env reader is `Config::from_env`, called here, so
        // mutating the process environment from the test thread is contained.
        std::env::set_var("PGO_WORKLOAD_ITERATIONS", "not-a-number");
        let err = Config::from_env().unwrap_err();
        std::env::remove_var("PGO_WORKLOAD_ITERATIONS");
        assert!(err.contains("PGO_WORKLOAD_ITERATIONS"), "got: {err}");

        std::env::set_var("PGO_WORKLOAD_ITERATIONS", "0");
        let err = Config::from_env().unwrap_err();
        std::env::remove_var("PGO_WORKLOAD_ITERATIONS");
        assert!(err.contains("> 0"), "got: {err}");
    }

    /// The documented defaults are the defaults in the code. If a constant
    /// moves, this test is where the spec has to move with it.
    #[test]
    fn documented_defaults_hold() {
        assert_eq!(DEFAULT_ITERATIONS, 20_000);
        assert_eq!(DEFAULT_SEED, 0x5EED_F00D_1234_5678);
        assert_eq!(DEFAULT_NUM_KEYS, 4_096);
        assert_eq!(DEFAULT_VALUE_LEN, 512);
    }
}
