//! Cache-eviction invariants.
//!
//! Local disk is a cache over the object store and is always smaller than the
//! database, so "an evicted SSTable is read back from the object store" is the
//! steady state, not an edge case. These tests run an engine whose cache holds
//! a small fraction of its data, drive many flush + sync cycles (eviction runs
//! at sync), and check after every cycle that:
//!
//! - **I1** uploaded local bytes stay within the cache cap plus the bytes of
//!   hot (recently read) tables, which the evictor never touches;
//! - **I2** every row ever written is readable through every read path
//!   (point, clustering-row, range, limited-row range, token range, bounded
//!   token range, token walk, secondary index, full-text), compared against a
//!   model of what was written;
//! - **I3** an evicted SSTable read back from the object store returns the
//!   identical rows, across repeated evict -> rehydrate -> evict cycles;
//! - **I4** a restart over the same data dir and object store preserves I2;
//! - **I5** when the object store loses an evicted SSTable's objects, every
//!   read path returns `Err`, never fewer rows as `Ok`.
//!
//! The data comes from a seeded generator, so a failure replays exactly. The
//! default variants are sized for PR CI; the larger sweep is in `mod slow`.
//! They back FMEA ST-40 (read-recency eviction) and ST-41 (range reads fail
//! loud when rehydrate fails).

use super::*;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::PartitionKey;
use ferrosa_common::schema::ColumnDefinition;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo};
use std::collections::BTreeMap;

const KEYSPACE: &str = "inv_ks";
const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";
const INT32: &str = "org.apache.cassandra.db.marshal.Int32Type";
const INDEX_NAME: &str = "inv_val_idx";
const FTS_NAME: &str = "inv_fts_idx";
/// Distinct values the indexed table's `val` column takes (`v0`..`v7`).
const INDEX_VALUES: u64 = 8;
const PARTITIONS: u64 = 40;
const CLUSTERINGS: u64 = 6;
const VOCAB: [&str; 6] = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];
/// The engine refuses `read_range` limits above its materialization cap.
const MAX_RANGE_LIMIT: usize = 10_000;
/// Hard bound on the cursor-following loop of the bounded token read.
const MAX_BOUNDED_PAGES: usize = 100_000;

type Res<T> = ferrosa_common::Result<T>;
type RowKey = (String, i32);
type RowMap = BTreeMap<RowKey, Vec<u8>>;

/// xorshift64*: deterministic, dependency-free, good enough for test data.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0) has no answer");
        self.next_u64() % n
    }

    fn alnum(&mut self, len: u64) -> Vec<u8> {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        (0..len)
            .map(|_| ALPHABET[self.below(ALPHABET.len() as u64) as usize])
            .collect()
    }
}

#[derive(Clone, Copy)]
struct Params {
    seed: u64,
    cycles: usize,
    rows_per_cycle: usize,
    cache_cap: u64,
    hot_window_secs: u64,
}

impl Params {
    /// Default sizing: about 20 uploaded KB per cycle against a 4 KiB cache.
    fn default_with(hot_window_secs: u64) -> Self {
        Self {
            seed: 0x5eed_0007,
            cycles: 5,
            rows_per_cycle: 40,
            cache_cap: 4096,
            hot_window_secs,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Plain,
    Indexed,
    FullText,
}

struct Model {
    name: &'static str,
    tid: TableId,
    kind: Kind,
    rows: RowMap,
    next_ck: i32,
}

impl Model {
    fn new(name: &'static str, kind: Kind) -> Self {
        Self {
            name,
            tid: TableId::new(KEYSPACE, name),
            kind,
            rows: RowMap::new(),
            next_ck: 0,
        }
    }

    fn partitions(&self) -> Vec<String> {
        let mut pks: Vec<String> = self.rows.keys().map(|(pk, _)| pk.clone()).collect();
        pks.dedup();
        pks
    }

    /// `(word, row key)` per row whose text contains `word` as a whole token.
    /// A hit is the framed row key: `0x01`, the partition key length as a
    /// big-endian u32, the partition key, then the 4-byte clustering key.
    fn fts_expected(&self) -> BTreeMap<(String, Vec<u8>), ()> {
        let mut out = BTreeMap::new();
        for ((pk, ck), val) in &self.rows {
            let text = String::from_utf8_lossy(val);
            let mut hit = vec![1_u8];
            hit.extend((pk.len() as u32).to_be_bytes());
            hit.extend(pk.as_bytes());
            hit.extend(ck.to_be_bytes());
            for word in VOCAB {
                if text.split(' ').any(|token| token == word) {
                    out.insert((word.to_string(), hit.clone()), ());
                }
            }
        }
        out
    }
}

fn schema(table: &str) -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: table.to_string(),
        key_type: UTF8.to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: INT32.to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: UTF8.to_string(),
        }],
        extensions: Default::default(),
    }
}

fn make_key(pk: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(pk.as_bytes().to_vec()))
}

fn make_row(ck: i32, val: &[u8], ts: i64) -> Row {
    Row {
        clustering: ck.to_be_bytes().to_vec(),
        cells: vec![(0, CellValue::live(val.to_vec(), ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

fn add_partition(out: &mut RowMap, partition: &Partition) {
    let pk = String::from_utf8(partition.key.key.as_bytes().to_vec())
        .expect("test partition keys are UTF-8");
    for row in &partition.rows {
        let ck_bytes: [u8; 4] = row.clustering[..4]
            .try_into()
            .expect("test clustering keys are 4-byte Int32");
        let val = row
            .cells
            .first()
            .and_then(|(_, cell)| cell.value.clone())
            .expect("every test row has a live val cell");
        out.insert((pk.clone(), i32::from_be_bytes(ck_bytes)), val);
    }
}

fn rows_of(partitions: &[Partition]) -> RowMap {
    let mut out = RowMap::new();
    for partition in partitions {
        add_partition(&mut out, partition);
    }
    out
}

/// The first `n` rows of every partition: what a per-partition row cap keeps.
fn first_rows_per_partition(rows: &RowMap, n: usize) -> RowMap {
    let mut seen: BTreeMap<&String, usize> = BTreeMap::new();
    let mut out = RowMap::new();
    for ((pk, ck), val) in rows {
        let count = seen.entry(pk).or_insert(0);
        if *count < n {
            out.insert((pk.clone(), *ck), val.clone());
        }
        *count += 1;
    }
    out
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Match,
    /// The read returned `Err`: loud, and the right answer when the object
    /// store has lost what the read needs.
    ReadErr(String),
    /// The read returned `Ok` with the wrong content: the silent failure.
    Mismatch(String),
}

struct Finding {
    table: &'static str,
    path: &'static str,
    outcome: Outcome,
}

impl std::fmt::Display for Finding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}: {:?}", self.table, self.path, self.outcome)
    }
}

fn describe(findings: &[Finding]) -> String {
    findings
        .iter()
        .map(|f| format!("\n  {f}"))
        .collect::<String>()
}

fn diff_summary<K: Ord + std::fmt::Debug, V: PartialEq>(
    got: &BTreeMap<K, V>,
    expected: &BTreeMap<K, V>,
) -> String {
    let missing: Vec<&K> = expected
        .iter()
        .filter(|(k, v)| got.get(*k) != Some(*v))
        .map(|(k, _)| k)
        .collect();
    let unexpected: Vec<&K> = got.keys().filter(|k| !expected.contains_key(*k)).collect();
    format!(
        "expected {} entries, observed {}; {} missing or wrong (first {:?}); \
         {} unexpected (first {:?})",
        expected.len(),
        got.len(),
        missing.len(),
        missing.first(),
        unexpected.len(),
        unexpected.first()
    )
}

fn judge<K: Ord + std::fmt::Debug, V: PartialEq>(
    observed: Res<BTreeMap<K, V>>,
    expected: &BTreeMap<K, V>,
) -> Outcome {
    match observed {
        Err(e) => Outcome::ReadErr(e.to_string()),
        Ok(got) if &got == expected => Outcome::Match,
        Ok(got) => Outcome::Mismatch(diff_summary(&got, expected)),
    }
}

/// A per-partition row cap is applied to each source before the merge, so a
/// merged partition may carry more than `n` rows, and a row past the cap can be
/// a superseded version (the newer source cut it, the older kept it). That is
/// the cap's contract, not eviction's. What must hold: the first `n` rows of
/// every partition are present with their current values, and no row is
/// returned that was never written.
fn judge_limited(observed: Res<RowMap>, rows: &RowMap, n: usize) -> Outcome {
    let got = match observed {
        Ok(got) => got,
        Err(e) => return Outcome::ReadErr(e.to_string()),
    };
    let floor = first_rows_per_partition(rows, n);
    let missing = floor
        .iter()
        .filter(|(k, v)| got.get(*k) != Some(*v))
        .count();
    let foreign = got.keys().filter(|k| !rows.contains_key(*k)).count();
    if missing == 0 && foreign == 0 {
        return Outcome::Match;
    }
    Outcome::Mismatch(format!(
        "{missing} of the first-{n}-per-partition rows are missing or wrong; \
         {foreign} rows returned that were never written"
    ))
}

/// Every read path over one table, each returning what it observed.
struct Reader<'a> {
    engine: &'a StorageEngine,
    model: &'a Model,
}

impl Reader<'_> {
    fn point(&self) -> Res<RowMap> {
        let mut out = RowMap::new();
        for pk in self.model.partitions() {
            if let Some(partition) = self.engine.read(&self.model.tid, &make_key(&pk))? {
                add_partition(&mut out, &partition);
            }
        }
        Ok(out)
    }

    fn clustering(&self) -> Res<RowMap> {
        let mut out = RowMap::new();
        for (pk, ck) in self.model.rows.keys() {
            let found = self.engine.read_clustering_row(
                &self.model.tid,
                &make_key(pk),
                &ck.to_be_bytes(),
            )?;
            if let Some(partition) = found {
                add_partition(&mut out, &partition);
            }
        }
        Ok(out)
    }

    fn range(&self) -> Res<RowMap> {
        let parts = self
            .engine
            .read_range(&self.model.tid, None, None, MAX_RANGE_LIMIT)?;
        Ok(rows_of(&parts))
    }

    fn range_limited(&self) -> Res<RowMap> {
        let parts =
            self.engine
                .read_range_limited_rows(&self.model.tid, None, None, MAX_RANGE_LIMIT, 2)?;
        Ok(rows_of(&parts))
    }

    fn token_range(&self) -> Res<RowMap> {
        let parts = self
            .engine
            .read_token_range(&self.model.tid, i64::MIN, i64::MAX, 1_000_000)?;
        Ok(rows_of(&parts))
    }

    fn token_bounded(&self) -> Res<RowMap> {
        let mut out = RowMap::new();
        let mut start = i64::MIN;
        for _ in 0..MAX_BOUNDED_PAGES {
            let (parts, cursor) = self.engine.read_token_range_bounded(
                &self.model.tid,
                start,
                i64::MAX,
                7,
                usize::MAX,
            )?;
            out.extend(rows_of(&parts));
            match cursor {
                Some(next) => start = next,
                None => return Ok(out),
            }
        }
        Err(ferrosa_common::Error::InvalidData(format!(
            "bounded token read of {} did not finish in {MAX_BOUNDED_PAGES} pages",
            self.model.tid
        )))
    }

    fn walk(&self) -> Res<RowMap> {
        let mut out = RowMap::new();
        self.engine
            .walk_token_range(&self.model.tid, i64::MIN, i64::MAX, |partition| {
                add_partition(&mut out, partition);
                Ok(())
            })?;
        Ok(out)
    }

    fn index(&self) -> Res<RowMap> {
        let mut out = RowMap::new();
        for n in 0..INDEX_VALUES {
            let word = format!("v{n}").into_bytes();
            let mut parts = Vec::new();
            self.engine.read_by_index_each(
                &self.model.tid,
                INDEX_NAME,
                &ferrosa_index::IndexKey(word.clone()),
                &mut |partition| {
                    parts.push(partition);
                    std::ops::ControlFlow::Continue(())
                },
            )?;
            out.extend(rows_of(&parts).into_iter().filter(|(_, v)| *v == word));
        }
        Ok(out)
    }

    fn fts(&self) -> Res<BTreeMap<(String, Vec<u8>), ()>> {
        let mut out = BTreeMap::new();
        for word in VOCAB {
            let hits = self
                .engine
                .fulltext_search(&self.model.tid, FTS_NAME, word, None)?;
            for hit in hits {
                out.insert((word.to_string(), hit), ());
            }
        }
        Ok(out)
    }

    fn findings(&self) -> Vec<Finding> {
        let rows = &self.model.rows;
        let mut checks = vec![
            ("point", judge(self.point(), rows)),
            ("clustering_row", judge(self.clustering(), rows)),
            ("read_range", judge(self.range(), rows)),
            (
                "read_range_limited_rows",
                judge_limited(self.range_limited(), rows, 2),
            ),
            ("read_token_range", judge(self.token_range(), rows)),
            (
                "read_token_range_bounded",
                judge(self.token_bounded(), rows),
            ),
            ("walk_token_range", judge(self.walk(), rows)),
        ];
        match self.model.kind {
            Kind::Plain => {}
            Kind::Indexed => checks.push(("secondary_index", judge(self.index(), rows))),
            Kind::FullText => checks.push((
                "fulltext_search",
                judge(self.fts(), &self.model.fts_expected()),
            )),
        }
        checks
            .into_iter()
            .map(|(path, outcome)| Finding {
                table: self.model.name,
                path,
                outcome,
            })
            .collect()
    }
}

struct Accounting {
    /// Local bytes of uploaded SSTables the evictor could remove or skip.
    local: u64,
    /// Bytes of those that belong to hot tables.
    hot: u64,
    /// Bytes the manifest says the object store holds.
    stored: u64,
    /// Local candidate bytes per table, for the failure message.
    per_table: BTreeMap<String, u64>,
}

struct Harness {
    dir: tempfile::TempDir,
    store: Arc<dyn object_store::ObjectStore>,
    prefix: String,
    params: Params,
    engine: Option<StorageEngine>,
    models: Vec<Model>,
    rng: Rng,
    ts: i64,
}

impl Harness {
    fn new(prefix: &str, params: Params) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let models = vec![
            Model::new("plain", Kind::Plain),
            Model::new("indexed", Kind::Indexed),
            Model::new("fts", Kind::FullText),
        ];
        let engine = Self::open_engine(dir.path(), &store, prefix, &params, &models);
        Self {
            dir,
            store,
            prefix: prefix.to_string(),
            params,
            engine: Some(engine),
            models,
            rng: Rng::new(params.seed),
            ts: 1_000,
        }
    }

    fn open_engine(
        dir: &std::path::Path,
        store: &Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        params: &Params,
        models: &[Model],
    ) -> StorageEngine {
        let mut config = StorageEngineConfig::test_config(dir);
        config.local_cache_max_bytes = params.cache_cap;
        config.cache_hot_window_secs = params.hot_window_secs;
        // Compaction rehydrates its evicted inputs and holds them until its
        // result is finalized, so an automatic trigger mid-scenario makes the
        // cache bound timing-dependent. Compaction under a small cache has its
        // own test, which drives it explicitly.
        config.compaction.min_threshold = 1_000;
        config.object_store = Some(crate::upload::ObjectStoreConfig {
            prefix: prefix.to_string(),
            ..crate::upload::ObjectStoreConfig::test_config()
        });
        let engine = StorageEngine::new_with_upload_store(
            config,
            Arc::clone(store),
            prefix.to_string(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        // The test constructor does not install the read-through hook the
        // production constructors do.
        StorageEngine::install_s3_file_read_rehydration_hook(
            dir.to_path_buf(),
            prefix.to_string(),
            Arc::clone(store),
        );
        for model in models {
            Self::register(&engine, model);
        }
        engine
    }

    fn register(engine: &StorageEngine, model: &Model) {
        let schema = schema(model.name);
        match model.kind {
            Kind::Plain => engine.register_table(schema).unwrap(),
            Kind::Indexed => engine
                .register_table_with_indexes(schema, vec![(INDEX_NAME.to_string(), 0_usize)])
                .unwrap(),
            Kind::FullText => {
                engine.register_table(schema).unwrap();
                engine.add_fulltext_index(&model.tid, FTS_NAME, 0).unwrap();
            }
        }
    }

    fn engine(&self) -> &StorageEngine {
        self.engine.as_ref().expect("the engine is running")
    }

    fn restart(&mut self) {
        let old = self.engine.take().expect("the engine is running");
        old.shutdown().unwrap();
        drop(old);
        self.engine = Some(Self::open_engine(
            self.dir.path(),
            &self.store,
            &self.prefix,
            &self.params,
            &self.models,
        ));
    }

    fn next_row(&mut self, i: usize) -> (String, i32, Vec<u8>) {
        let pk = format!("p{:03}", self.rng.below(PARTITIONS));
        match self.models[i].kind {
            Kind::Plain => {
                let ck = 1 + self.rng.below(CLUSTERINGS) as i32;
                let len = 40 + self.rng.below(80);
                (pk, ck, self.rng.alnum(len))
            }
            Kind::Indexed => {
                let ck = 1 + self.rng.below(CLUSTERINGS) as i32;
                (
                    pk,
                    ck,
                    format!("v{}", self.rng.below(INDEX_VALUES)).into_bytes(),
                )
            }
            Kind::FullText => {
                // Insert-only with a unique clustering key, so a search hit
                // never depends on how an overwrite supersedes a posting.
                self.models[i].next_ck += 1;
                let ck = self.models[i].next_ck;
                let mut words = Vec::new();
                for _ in 0..3 {
                    words.push(VOCAB[self.rng.below(VOCAB.len() as u64) as usize]);
                }
                (pk, ck, format!("{} n{ck}", words.join(" ")).into_bytes())
            }
        }
    }

    fn write_cycle(&mut self) {
        for i in 0..self.models.len() {
            for _ in 0..self.params.rows_per_cycle {
                let (pk, ck, val) = self.next_row(i);
                self.ts += 1;
                let tid = self.models[i].tid.clone();
                self.engine()
                    .write(&tid, &make_key(&pk), make_row(ck, &val, self.ts), self.ts)
                    .unwrap();
                self.models[i].rows.insert((pk, ck), val);
            }
        }
    }

    fn flush_all(&self) {
        for model in &self.models {
            self.engine().flush(&model.tid).unwrap();
        }
    }

    /// Run one sync, which uploads then evicts, and return what it uploaded.
    /// Then drop the pooled readers and open descriptors of every evicted
    /// generation, so the next read has to reopen it by path and cannot be
    /// served from a descriptor that outlived its file.
    async fn sync(&self) -> usize {
        let uploaded = self.engine().sync_sstables_to_s3().await.unwrap();
        let sstables = self.dir.path().join("sstables");
        for (dir_name, gens) in StorageEngine::evicted_generations(&sstables) {
            let model = self
                .models
                .iter()
                .find(|m| m.tid.to_string() == dir_name)
                .unwrap_or_else(|| panic!("eviction marker in unknown table dir {dir_name}"));
            for gen in gens {
                self.engine()
                    .evict_pooled_reader_for_test(&model.tid, gen.parse().unwrap());
                ferrosa_sstable::io::evict_global_fd_for_test(
                    sstables.join(&dir_name).join(format!("{gen}-Data.db")),
                );
            }
        }
        uploaded
    }

    async fn flush_and_sync(&self) {
        self.flush_all();
        let uploaded = self.sync().await;
        assert!(uploaded >= 1, "the sync uploaded nothing after a flush");
    }

    async fn accounting(&self) -> Accounting {
        let (manifest, _) = crate::manifest::Manifest::load(self.store.as_ref(), &self.prefix)
            .await
            .unwrap();
        let candidates = self.engine().collect_uploaded_local_sstables(&manifest);
        let local = candidates.iter().map(|c| c.size).sum();
        let mut per_table = BTreeMap::new();
        for c in &candidates {
            *per_table.entry(c.table.clone()).or_insert(0) += c.size;
        }
        let order = crate::eviction_plan::order_for_eviction(
            candidates,
            &self.engine().last_foreground_reads(),
            std::time::SystemTime::now(),
            std::time::Duration::from_secs(self.params.hot_window_secs),
        );
        let stored = manifest.sstables.values().flatten().map(|e| e.size).sum();
        Accounting {
            local,
            hot: order.hot_bytes,
            stored,
            per_table,
        }
    }

    /// I1, plus: nothing sits in a table dir that the manifest does not know
    /// (every local generation is accounted for by the bound).
    async fn assert_cache_bound(&self, label: &str) -> Accounting {
        let acct = self.accounting().await;
        let cap = self.params.cache_cap;
        let (manifest, _) = crate::manifest::Manifest::load(self.store.as_ref(), &self.prefix)
            .await
            .unwrap();
        let layout: Vec<String> = self
            .models
            .iter()
            .map(|m| {
                let dir = self.engine().table_sstable_dir(&m.tid);
                let local = StorageEngine::list_generations_in_dir(&dir);
                let listed: Vec<String> = manifest
                    .sstables
                    .get(&m.tid.to_string())
                    .map(|entries| entries.iter().map(|e| e.id.clone()).collect())
                    .unwrap_or_default();
                format!("{}: local {local:?}, manifest {listed:?}", m.name)
            })
            .collect();
        assert!(
            acct.local <= cap + acct.hot,
            "I1 {label}: {} uploaded bytes are local ({:?}), over the {cap}-byte cap plus {} hot bytes; {layout:?}",
            acct.local,
            acct.per_table,
            acct.hot
        );
        for model in &self.models {
            let listed: Vec<String> = manifest
                .sstables
                .get(&model.tid.to_string())
                .map(|entries| entries.iter().map(|e| e.id.clone()).collect())
                .unwrap_or_default();
            let dir = self.engine().table_sstable_dir(&model.tid);
            for gen in StorageEngine::list_generations_in_dir(&dir) {
                assert!(
                    listed.contains(&gen.to_string()),
                    "I1 {label}: {} generation {gen} is local but not in the manifest, \
                     so the bound does not account for it",
                    model.name
                );
            }
        }
        acct
    }

    fn findings(&self, only: Option<&str>) -> Vec<Finding> {
        self.models
            .iter()
            .filter(|m| only.is_none_or(|name| m.name == name))
            .flat_map(|model| {
                Reader {
                    engine: self.engine(),
                    model,
                }
                .findings()
            })
            .collect()
    }

    /// I2: every read path returns exactly the model.
    fn assert_readable(&self, label: &str, only: Option<&str>) {
        let bad: Vec<Finding> = self
            .findings(only)
            .into_iter()
            .filter(|f| f.outcome != Outcome::Match)
            .collect();
        assert!(bad.is_empty(), "I2 {label}: {}", describe(&bad));
    }

    /// I2 for the index paths run BEFORE any data read touches the table. A
    /// data read rehydrates the table's evicted SSTables, which can hide an
    /// index read that cannot see an evicted SSTable's postings on its own.
    fn assert_index_paths_readable_first(&self, label: &str) {
        let mut bad = Vec::new();
        for model in &self.models {
            let reader = Reader {
                engine: self.engine(),
                model,
            };
            let (path, outcome) = match model.kind {
                Kind::Plain => continue,
                Kind::Indexed => ("secondary_index", judge(reader.index(), &model.rows)),
                Kind::FullText => (
                    "fulltext_search",
                    judge(reader.fts(), &model.fts_expected()),
                ),
            };
            if outcome != Outcome::Match {
                bad.push(Finding {
                    table: model.name,
                    path,
                    outcome,
                });
            }
        }
        assert!(
            bad.is_empty(),
            "I2 {label} (index read first): {}",
            describe(&bad)
        );
    }

    /// I5 for the index paths run BEFORE any data read has tried (and failed)
    /// to rehydrate the table.
    fn assert_index_paths_err_first(&self, label: &str) {
        let mut bad = Vec::new();
        for model in &self.models {
            let reader = Reader {
                engine: self.engine(),
                model,
            };
            let (path, outcome) = match model.kind {
                Kind::Plain => continue,
                Kind::Indexed => ("secondary_index", judge(reader.index(), &model.rows)),
                Kind::FullText => (
                    "fulltext_search",
                    judge(reader.fts(), &model.fts_expected()),
                ),
            };
            if !matches!(outcome, Outcome::ReadErr(_)) {
                bad.push(Finding {
                    table: model.name,
                    path,
                    outcome,
                });
            }
        }
        assert!(
            bad.is_empty(),
            "I5 {label} (index read first): paths that returned Ok: {}",
            describe(&bad)
        );
    }

    /// I5: every read path fails loud; none returns `Ok`.
    fn assert_every_path_errs(&self, label: &str) {
        let bad: Vec<Finding> = self
            .findings(None)
            .into_iter()
            .filter(|f| !matches!(f.outcome, Outcome::ReadErr(_)))
            .collect();
        assert!(
            bad.is_empty(),
            "I5 {label}: paths that returned Ok: {}",
            describe(&bad)
        );
    }

    fn evicted_count(&self) -> usize {
        StorageEngine::evicted_generations(&self.dir.path().join("sstables"))
            .values()
            .map(|gens| gens.len())
            .sum()
    }

    /// File names in the table's SSTable directory, sorted.
    fn local_files(&self, model: &Model) -> Vec<String> {
        let dir = self.engine().table_sstable_dir(&model.tid);
        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("listing {}: {e}", dir.display()))
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    fn local_generations(&self, model: &Model) -> usize {
        StorageEngine::list_generations_in_dir(&self.engine().table_sstable_dir(&model.tid)).len()
    }
}

/// Remove every object from `store`, as if the bucket lost them.
async fn delete_all_objects(store: &Arc<dyn object_store::ObjectStore>) {
    use futures::StreamExt;
    let mut locations = Vec::new();
    let mut listing = store.list(None);
    while let Some(object) = listing.next().await {
        locations.push(object.expect("listing the in-memory store").location);
    }
    drop(listing);
    assert!(
        !locations.is_empty(),
        "the syncs uploaded objects to remove"
    );
    for location in locations {
        store.delete(&location).await.expect("deleting an object");
    }
}

/// The cycle loop shared by I1, I2 and I4. Returns the harness for follow-up
/// assertions.
async fn run_cycles(prefix: &str, params: Params, restart_every: Option<usize>) -> Harness {
    let mut h = Harness::new(prefix, params);
    for cycle in 0..params.cycles {
        h.write_cycle();
        h.flush_and_sync().await;
        assert!(
            h.evicted_count() > 0,
            "cycle {cycle}: the sync evicted nothing against a {}-byte cache, so it tested nothing",
            params.cache_cap
        );
        h.assert_cache_bound(&format!("cycle {cycle}")).await;
        h.assert_index_paths_readable_first(&format!("cycle {cycle}"));
        h.assert_readable(&format!("cycle {cycle}"), None);
        if restart_every.is_some_and(|n| (cycle + 1) % n == 0) {
            h.restart();
            h.assert_readable(&format!("cycle {cycle} after restart"), None);
        }
    }
    h
}

fn assert_data_dwarfs_cache(acct: &Accounting, cap: u64) {
    assert!(
        acct.stored >= 10 * cap,
        "the scenario must hold at least 10x the cache: {} bytes stored against a {cap}-byte cap",
        acct.stored
    );
}

/// I1 + I2: with hot-table skipping off, every cycle ends with the cache
/// within its cap and every row readable through every path.
#[tokio::test(flavor = "multi_thread")]
async fn i1_i2_every_read_path_survives_continuous_eviction() {
    let params = Params::default_with(0);
    let h = run_cycles("inv-continuous", params, None).await;
    // The last cycle's reads rehydrated tables; the bound holds after a sync.
    h.sync().await;
    let acct = h.assert_cache_bound("final").await;
    assert_data_dwarfs_cache(&acct, params.cache_cap);
    assert!(
        h.evicted_count() > 0,
        "the scenario never evicted, so it tested nothing"
    );
    h.engine().shutdown().unwrap();
}

/// I1 with a hot table: the table the foreground reads stays local and the
/// bound holds as cap plus its bytes, while the cold tables are evicted.
#[tokio::test(flavor = "multi_thread")]
async fn i1_hot_table_bytes_are_the_only_excess_over_the_cap() {
    let params = Params::default_with(900);
    let mut h = Harness::new("inv-hot", params);
    let mut saw_hot_bytes = false;
    for cycle in 0..params.cycles {
        h.write_cycle();
        h.flush_and_sync().await;
        let acct = h.assert_cache_bound(&format!("cycle {cycle}")).await;
        saw_hot_bytes |= acct.hot > 0;
        // Only the plain table is read, so only it becomes hot.
        h.assert_readable(&format!("cycle {cycle} (plain table)"), Some("plain"));
        let cold: Vec<usize> = h.models[1..]
            .iter()
            .map(|m| h.local_generations(m))
            .collect();
        assert_eq!(
            cold,
            vec![0, 0],
            "cycle {cycle}: the never-read tables must be fully evicted"
        );
    }
    assert!(saw_hot_bytes, "the read table never counted as hot");
    h.engine().shutdown().unwrap();
}

/// I3: read an evicted table back, evict it again, repeat. Every round
/// rehydrates from the object store and returns the identical partitions,
/// byte for byte and timestamp for timestamp.
#[tokio::test(flavor = "multi_thread")]
async fn i3_evict_rehydrate_evict_cycles_return_identical_rows() {
    let mut params = Params::default_with(0);
    params.cache_cap = 1;
    let mut h = Harness::new("inv-roundtrip", params);
    for _ in 0..3 {
        h.write_cycle();
        h.flush_and_sync().await;
    }
    let snapshot = |h: &Harness| -> Vec<Vec<Partition>> {
        h.models
            .iter()
            .map(|m| {
                h.engine()
                    .read_token_range(&m.tid, i64::MIN, i64::MAX, 1_000_000)
                    .unwrap()
            })
            .collect()
    };
    let baseline = snapshot(&h);
    for round in 0..4 {
        h.sync().await;
        assert!(h.evicted_count() > 0, "round {round}: nothing was evicted");
        for model in &h.models {
            assert_eq!(
                h.local_generations(model),
                0,
                "round {round}: {}",
                model.name
            );
        }
        h.assert_readable(&format!("round {round}"), None);
        for model in &h.models {
            assert!(
                h.local_generations(model) > 0,
                "round {round}: reading {} must have rehydrated it",
                model.name
            );
        }
        assert!(
            snapshot(&h) == baseline,
            "I3 round {round}: the rehydrated rows differ from the first read"
        );
    }
    h.engine().shutdown().unwrap();
}

/// A rehydrate must restore the generation's index artifacts with its data
/// components. Found by I4: the read-through hook restored only the fixed
/// component list, so a generation read back after eviction held its rows but
/// not its `.sidecar` / full-text files; the eviction marker was cleared, a
/// restart found the generation complete, and every index read answered `Ok`
/// with no rows.
#[tokio::test(flavor = "multi_thread")]
async fn i3_rehydrate_restores_index_artifacts_with_the_data_components() {
    let mut params = Params::default_with(0);
    params.cache_cap = 1;
    let mut h = Harness::new("inv-artifacts", params);
    h.write_cycle();
    h.flush_and_sync().await;
    for model in &h.models {
        assert_eq!(
            h.local_generations(model),
            0,
            "precondition: {}",
            model.name
        );
    }
    // A plain data read: it rehydrates, and does not go through an index.
    for model in &h.models {
        let reader = Reader {
            engine: h.engine(),
            model,
        };
        reader
            .point()
            .expect("point reads rehydrate an evicted table");
    }
    let has = |model: &Model, wanted: &dyn Fn(&str) -> bool| {
        h.local_files(model).iter().any(|name| wanted(name))
    };
    assert!(
        has(&h.models[1], &|n| n.ends_with(".sidecar")),
        "indexed table: no .sidecar after rehydrate: {:?}",
        h.local_files(&h.models[1])
    );
    assert!(
        has(&h.models[2], &|n| n.contains("-FTI-")),
        "full-text table: no FTI file after rehydrate: {:?}",
        h.local_files(&h.models[2])
    );
    h.restart();
    h.assert_index_paths_readable_first("after a restart that followed a rehydrate");
    h.engine().shutdown().unwrap();
}

/// I4: restart over the same data dir and object store, repeatedly, between
/// cycles of writes. Every restart preserves I2.
#[tokio::test(flavor = "multi_thread")]
async fn i4_restart_preserves_every_row_across_evictions() {
    let params = Params::default_with(0);
    let h = run_cycles("inv-restart", params, Some(3)).await;
    h.engine().shutdown().unwrap();
}

/// I5: when the object store loses the objects of evicted SSTables, no read
/// path may answer `Ok` with what is left; each must fail.
#[tokio::test(flavor = "multi_thread")]
async fn i5_lost_objects_fail_every_read_path_loud() {
    let mut params = Params::default_with(0);
    params.cache_cap = 1;
    let mut h = Harness::new("inv-lost", params);
    for _ in 0..2 {
        h.write_cycle();
        h.flush_and_sync().await;
    }
    for model in &h.models {
        assert_eq!(
            h.local_generations(model),
            0,
            "precondition: {}",
            model.name
        );
    }
    delete_all_objects(&h.store).await;
    h.assert_every_path_errs("every SSTable evicted and its objects lost");
    h.engine().shutdown().unwrap();
}

/// I1 + I2 across a compaction whose inputs are all evicted: compaction
/// rehydrates them, merges, and finalizes; the next sync brings the cache back
/// under its cap and every row stays readable through every path.
#[tokio::test(flavor = "multi_thread")]
async fn i1_i2_compaction_over_evicted_inputs_keeps_the_bound_and_every_row() {
    let params = Params::default_with(0);
    let mut h = Harness::new("inv-compact", params);
    for _ in 0..4 {
        h.write_cycle();
        h.flush_and_sync().await;
    }
    h.engine().force_compact_all();
    assert!(
        h.engine()
            .await_compaction_result(std::time::Duration::from_secs(30)),
        "compaction over evicted inputs produced no result"
    );
    h.engine().poll_compactions().await;
    h.sync().await;
    let acct = h.assert_cache_bound("after compaction").await;
    assert_data_dwarfs_cache(&acct, params.cache_cap);
    h.assert_index_paths_readable_first("after compaction");
    h.assert_readable("after compaction", None);
    h.restart();
    h.assert_readable("after compaction and restart", None);
    h.engine().shutdown().unwrap();
}

/// I5, index paths only and first: a lost object must fail the index read
/// itself, not only the data reads that follow it.
#[tokio::test(flavor = "multi_thread")]
async fn i5_lost_objects_fail_index_reads_loud_before_any_data_read() {
    let mut params = Params::default_with(0);
    params.cache_cap = 1;
    let mut h = Harness::new("inv-lost-index", params);
    for _ in 0..2 {
        h.write_cycle();
        h.flush_and_sync().await;
    }
    delete_all_objects(&h.store).await;
    h.assert_index_paths_err_first("every SSTable evicted and its objects lost");
    h.engine().shutdown().unwrap();
}

/// The larger randomized sweep: several seeds, more cycles and rows, a
/// restart every fourth cycle. Excluded from PR CI; run nightly.
#[cfg(feature = "slow-tests")]
mod slow {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn i1_to_i4_hold_across_seeds_with_restarts() {
        for seed in [1_u64, 0xdead_beef, 0x0bad_cafe, 7_777_777] {
            let params = Params {
                seed,
                cycles: 24,
                rows_per_cycle: 150,
                cache_cap: 16 * 1024,
                hot_window_secs: 0,
            };
            let h = run_cycles(&format!("inv-slow-{seed}"), params, Some(4)).await;
            h.sync().await;
            let acct = h.assert_cache_bound("final").await;
            assert_data_dwarfs_cache(&acct, params.cache_cap);
            h.engine().shutdown().unwrap();
        }
    }
}
