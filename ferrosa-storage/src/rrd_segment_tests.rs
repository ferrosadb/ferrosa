// Included from `engine.rs`'s `mod tests` (see the `include!` list there), so
// `use super::*` gives these tests the private `StorageEngine` internals —
// `maybe_compact`, `strategy_for_table`, `collect_sstable_metadata` — that the
// production compaction planner itself uses. Testing through those, rather than
// through a helper written for the test, is the point: the assertions fail when
// the real planner schedules work.

/// Table schema with `rrd.*` (or any other) extensions set.
///
/// The shape is the one an RRD tier actually has — a single 8-byte `bigint`
/// clustering component holding the data timestamp — because that component is
/// what a segmented flush places a row's window from. `test_schema()` has a
/// 4-byte `int` clustering, which cannot carry a timestamp.
fn rrd_schema(table: &str, ext: &[(&str, &str)]) -> TableSchema {
    let mut schema = test_schema();
    schema.table = table.to_string();
    schema.clustering_columns = vec![ColumnDefinition {
        name: "ts".to_string(),
        type_name: "org.apache.cassandra.db.marshal.LongType".to_string(),
    }];
    schema.extensions = ext
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    schema
}

/// A row whose clustering component is the data timestamp, in the unit the
/// tier's first clustering column declares (`bigint` here, so epoch micros).
fn rrd_row(ts_micros: i64, value: &[u8], write_ts: i64) -> Row {
    Row {
        clustering: ts_micros.to_be_bytes().to_vec(),
        cells: vec![(0, CellValue::live(value.to_vec(), write_ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(write_ts),
    }
}

fn rrd_table_id(table: &str) -> TableId {
    TableId::new("test_ks", table)
}

/// Write `flushes` distinct partitions, one forced flush each, so every flush
/// leaves its own SSTable behind.
fn write_partitions_and_flush(engine: &StorageEngine, table_id: &TableId, flushes: usize) {
    for i in 0..flushes {
        let key = make_key(&format!("k{i:04}"));
        let ts = 1_000_000i64 + i as i64;
        engine
            .write(table_id, &key, rrd_row(ts, format!("v{i}").as_bytes(), ts), ts)
            .expect("write");
        engine.flush(table_id).expect("flush");
    }
}

/// INVARIANT 3: no compaction task is ever produced for an rrd-segmented table.
///
/// `maybe_compact` runs from `flush()` and from the registration path, so the
/// only observable that cannot be raced is the pipeline itself: no task is in
/// flight, and no completed task is waiting to be polled. A task can leave the
/// executor only through `poll_results`, which this test never calls.
#[test]
fn rrd_table_produces_no_compaction_task_at_any_size() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    // Six flushes cross `max_inputs`, taking the bounded-backlog branch — the
    // branch that bypasses `CompactionStrategy::select` and would otherwise
    // rewrite a segmented table on every flush.
    config.compaction.min_threshold = 2;
    config.compaction.max_threshold = 4;
    let engine = StorageEngine::new(config, None).unwrap();

    let rrd = rrd_table_id("rrd_table");
    engine
        .register_table(rrd_schema(
            "rrd_table",
            &[("rrd.segment_window", "5m"), ("rrd.retain", "1h")],
        ))
        .expect("segmented table registers");

    const FLUSHES: usize = 6;
    write_partitions_and_flush(&engine, &rrd, FLUSHES);

    let state = engine.table_state(&rrd).expect("rrd table registered");
    assert_eq!(
        state.store.sstable_count(),
        FLUSHES,
        "every flush of a segmented table must remain its own segment"
    );
    assert!(
        !engine.maybe_compact(&rrd, &state),
        "the compaction planner must refuse an rrd-segmented table outright, \
         including through the bounded-backlog branch that never consults a \
         compaction strategy"
    );
    assert_eq!(
        engine.compaction_executor.compaction_in_flight()
            + engine.compaction_executor.pending_result_count(),
        0,
        "an rrd-segmented table must never enter the compaction pipeline: a queued \
         or finished task means its segments were rewritten"
    );
}

/// The control for the test above: the same cadence, the same engine
/// configuration, no `rrd.*` extension — the planner schedules work. Without
/// this, a planner that never schedules anything would pass the rrd assertion.
#[test]
fn plain_table_with_the_same_cadence_produces_a_compaction_task() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.compaction.min_threshold = 2;
    config.compaction.max_threshold = 4;
    let engine = StorageEngine::new(config, None).unwrap();

    let plain = rrd_table_id("plain_table");
    engine
        .register_table(rrd_schema("plain_table", &[]))
        .expect("plain table registers");

    write_partitions_and_flush(&engine, &plain, 6);

    let state = engine.table_state(&plain).expect("plain table registered");
    assert!(
        engine.compaction_executor.compaction_in_flight() + engine.compaction_executor.pending_result_count() >= 1,
        "six flushes of a plain table must schedule compaction — otherwise the \
         rrd fixture proves nothing"
    );
    assert!(
        engine.maybe_compact(&plain, &state) || state.store.sstable_count() < 6,
        "the plain table's segments are eligible for compaction"
    );
}

/// INVARIANT 3, the "at any size" half.
///
/// Compaction strategy selection is the engine's only source of merge tasks
/// below the backlog bound, and UCS triggers on density, so a segmented table
/// must select nothing no matter how its segments are shaped. The fixture spans
/// six orders of magnitude of size and density: from 3 KiB (the design's raw
/// tier) to 4 GiB, single-token and whole-ring spans.
#[test]
fn rrd_strategy_selects_nothing_at_any_segment_size() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.compaction.min_threshold = 2;
    config.compaction.max_threshold = 64;
    let engine = StorageEngine::new(config, None).unwrap();
    engine
        .register_table(rrd_schema(
            "rrd_table",
            &[
                ("rrd.segment_window", "5m"),
                ("rrd.retain", "2d"),
                ("compaction.class", "UnifiedCompactionStrategy"),
            ],
        ))
        .expect("segmented table registers");

    let table_id = rrd_table_id("rrd_table");
    let state = engine.table_state(&table_id).expect("registered");
    let sizes = [
        3 * 1024u64,
        64 * 1024,
        1024 * 1024,
        100 * 1024 * 1024,
        1024 * 1024 * 1024,
        4 * 1024 * 1024 * 1024,
    ];
    let mut metadata = Vec::new();
    for (i, size) in sizes.iter().enumerate() {
        for span in [1i64, 1_000_000, i64::MAX / 4, i64::MAX - 1] {
            let base = (i as i64 * 7).saturating_add(span);
            metadata.push(crate::compaction::metadata::SSTableMetadata {
                id: format!("gen-{i}-{span}"),
                path: std::path::PathBuf::from(format!("/tmp/gen-{i}-{span}")),
                size_bytes: *size,
                min_token: base,
                max_token: base.saturating_add(span),
                min_timestamp: 1,
                max_timestamp: 2,
                partition_count: 1,
                legacy_format: false,
            });
        }
    }
    // A compacted table would merge here: 24 overlapping SSTables, uniform
    // density buckets, `fan_factor = 4` default.
    let selected = engine
        .strategy_for_table(&state)
        .select(&metadata, &state.schema(), &table_id);
    assert!(
        selected.is_empty(),
        "an rrd-segmented table must select no compaction at any segment size; \
         got {} task(s) over {} segments",
        selected.len(),
        metadata.len()
    );
}

/// A table cannot be half-segmented: an unparseable `rrd.*` value is a DDL error,
/// not a silent fall back to compaction (which would rewrite every segment on
/// every flush — the exact cost the extension exists to avoid).
#[test]
fn register_table_rejects_a_malformed_rrd_extension() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageEngineConfig::test_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();

    let error = engine
        .register_table(rrd_schema(
            "bad_window",
            &[("rrd.segment_window", "five minutes"), ("rrd.retain", "1h")],
        ))
        .expect_err("a malformed segment window must fail registration");
    let message = error.to_string();
    assert!(
        message.contains("rrd.segment_window"),
        "registration error must name the offending extension, got: {message}"
    );

    let error = engine
        .register_table(rrd_schema("no_retain", &[("rrd.segment_window", "5m")]))
        .expect_err("a segment window without retention must fail registration");
    assert!(
        error.to_string().contains("rrd.retain"),
        "registration error must name the missing extension, got: {error}"
    );
}

// ── The segment writer: one immutable generation per window ──────────────
//
// Every fixture below uses a 17-minute window. A test that also passes with
// 5m — the value the step-1 fixtures happen to use — would be proving a
// constant rather than the configured window.

const SEGMENT_WINDOW_MICROS: i64 = 17 * 60 * 1_000_000;

fn segmented_schema(table: &str) -> TableSchema {
    rrd_schema(
        table,
        &[("rrd.segment_window", "17m"), ("rrd.retain", "4h")],
    )
}

/// Start of the window that contains `ts_micros`, computed here rather than
/// taken from the code under test.
fn window_start(ts_micros: i64) -> i64 {
    ts_micros - ts_micros.rem_euclid(SEGMENT_WINDOW_MICROS)
}

fn clustered_ts(row: &Row) -> i64 {
    let bytes: [u8; 8] = row
        .clustering
        .as_slice()
        .try_into()
        .expect("this fixture's clustering component is an 8-byte timestamp");
    i64::from_be_bytes(bytes)
}

/// The generation ids the table's store holds.
fn generations(engine: &StorageEngine, table_id: &TableId) -> Vec<String> {
    engine
        .require_table(table_id)
        .expect("table registered")
        .store
        .sstable_generation_ids()
}

/// The partition `key` as ONE generation holds it; `None` when that generation
/// does not hold the partition at all.
fn generation_rows(
    engine: &StorageEngine,
    table_id: &TableId,
    gen: &str,
    key: &DecoratedKey,
) -> Option<ferrosa_sstable::types::Partition> {
    engine
        .require_table(table_id)
        .expect("table registered")
        .store
        .read_from_generation_for_test(gen, key)
        .expect("read one generation")
}

/// Write one row per `(ts, value)` into `key`, then flush once.
fn write_and_flush_once(
    engine: &StorageEngine,
    table_id: &TableId,
    key: &DecoratedKey,
    rows: &[(i64, &[u8])],
) {
    for (ts, value) in rows {
        engine
            .write(table_id, key, rrd_row(*ts, value, *ts), *ts)
            .expect("write");
    }
    engine.flush(table_id).expect("flush");
}

/// INVARIANT 1: every generation a segmented flush writes holds rows from
/// exactly one window — which is what lets retention unlink a whole file.
#[test]
fn rrd_segment_generation_holds_rows_from_exactly_one_window() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
    let table_id = rrd_table_id("rrd_ts");
    engine
        .register_table(segmented_schema("rrd_ts"))
        .expect("register");
    let key = make_key("s1");

    // Two windows' worth of rows in ONE memtable: two at the end of window 0,
    // then a row exactly on the edge (which belongs to the window starting
    // there) and one just after it.
    let rows: [(i64, &[u8]); 4] = [
        (SEGMENT_WINDOW_MICROS - 1, b"w0_last"),
        (SEGMENT_WINDOW_MICROS - 1_000_000, b"w0_first"),
        (SEGMENT_WINDOW_MICROS, b"w1_edge"),
        (SEGMENT_WINDOW_MICROS + 1, b"w1_next"),
    ];
    write_and_flush_once(&engine, &table_id, &key, &rows);

    let gens = generations(&engine, &table_id);
    assert_eq!(
        gens.len(),
        2,
        "a memtable holding rows in two 17m windows must flush to two generations, got {gens:?}"
    );

    let mut windows_written = Vec::new();
    for gen in &gens {
        let partition =
            generation_rows(&engine, &table_id, gen, &key).unwrap_or_else(|| {
                panic!("generation {gen} holds no rows for the flushed partition")
            });
        let mut file_windows: Vec<i64> = partition
            .rows
            .iter()
            .map(|row| window_start(clustered_ts(row)))
            .collect();
        file_windows.sort_unstable();
        file_windows.dedup();
        assert_eq!(
            file_windows.len(),
            1,
            "generation {gen} holds rows from windows {file_windows:?}: a segmented flush must \
             write one generation per window, or no window can be deleted as a file"
        );
        windows_written.push((file_windows[0], partition.rows.len()));
    }
    windows_written.sort_unstable();
    assert_eq!(
        windows_written,
        vec![(0, 2), (SEGMENT_WINDOW_MICROS, 2)],
        "each window's generation must hold exactly that window's rows — the row on the edge \
         belongs to the window that starts there"
    );

    // The values, per window, so "two rows each" cannot pass with the wrong two.
    let mut by_value: Vec<(Vec<u8>, i64)> = Vec::new();
    for gen in &gens {
        let partition = generation_rows(&engine, &table_id, gen, &key).expect("partition");
        for row in &partition.rows {
            let value = row
                .cells
                .first()
                .and_then(|(_, cell)| cell.value.clone())
                .expect("value");
            by_value.push((value, window_start(clustered_ts(row))));
        }
    }
    by_value.sort();
    assert_eq!(
        by_value,
        vec![
            (b"w0_first".to_vec(), 0),
            (b"w0_last".to_vec(), 0),
            (b"w1_edge".to_vec(), SEGMENT_WINDOW_MICROS),
            (b"w1_next".to_vec(), SEGMENT_WINDOW_MICROS),
        ]
    );
}

/// INVARIANT 2: a flush touching N windows writes N generations — not one per
/// partition, not one per row, and none for a window no row fell in.
#[test]
fn rrd_flush_writes_one_generation_per_window_touched() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
    let table_id = rrd_table_id("rrd_gaps");
    engine
        .register_table(segmented_schema("rrd_gaps"))
        .expect("register");

    // Three windows with an EMPTY window between each pair: 0, 2 and 4.
    let a = make_key("sensor_a");
    let b = make_key("sensor_b");
    for (key, ts, value) in [
        (&a, 10i64, b"a0".as_slice()),
        (&b, SEGMENT_WINDOW_MICROS + 10, b"b1".as_slice()),
        // A second row in the window sensor_b already touched: same generation.
        (&a, SEGMENT_WINDOW_MICROS + 20, b"a1".as_slice()),
        (&a, 4 * SEGMENT_WINDOW_MICROS + 10, b"a4".as_slice()),
    ] {
        engine
            .write(&table_id, key, rrd_row(ts, value, ts), ts)
            .expect("write");
    }
    engine.flush(&table_id).expect("flush");

    let gens = generations(&engine, &table_id);
    assert_eq!(
        gens.len(),
        3,
        "three touched windows (0, 1w, 4w) must write three generations and nothing for the \
         untouched windows between them, got {gens:?}"
    );

    let mut covered_windows = Vec::new();
    for gen in &gens {
        let mut windows_here = Vec::new();
        for key in [&a, &b] {
            if let Some(partition) = generation_rows(&engine, &table_id, gen, key) {
                windows_here.extend(partition.rows.iter().map(|row| window_start(clustered_ts(row))));
            }
        }
        windows_here.sort_unstable();
        windows_here.dedup();
        assert_eq!(
            windows_here.len(),
            1,
            "generation {gen} covers windows {windows_here:?}"
        );
        covered_windows.push(windows_here[0]);
    }
    covered_windows.sort_unstable();
    assert_eq!(
        covered_windows,
        vec![0, SEGMENT_WINDOW_MICROS, 4 * SEGMENT_WINDOW_MICROS],
        "exactly the touched windows get a generation"
    );
}

/// INVARIANT 2, the N = 0 half: an empty memtable writes no generation at all.
#[test]
fn rrd_flush_of_an_empty_memtable_writes_no_generation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
    let table_id = rrd_table_id("rrd_empty");
    engine
        .register_table(segmented_schema("rrd_empty"))
        .expect("register");

    engine.flush(&table_id).expect("flush of an empty memtable");

    assert_eq!(
        generations(&engine, &table_id).len(),
        0,
        "a flush with no rows must write no generation, not an empty one"
    );
    assert_eq!(engine.sstable_count(&table_id), 0);
}

/// INVARIANT 3: a partition whose rows span two windows is readable across
/// both, with no row lost and the clustering order preserved.
#[test]
fn rrd_partition_spanning_two_windows_reads_across_both_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
    let table_id = rrd_table_id("rrd_span");
    engine
        .register_table(segmented_schema("rrd_span"))
        .expect("register");
    let key = make_key("s1");

    // Interleaved write order (window 1 first) so the read has to order them,
    // not inherit insertion order from one file.
    let rows: [(i64, &[u8]); 4] = [
        (SEGMENT_WINDOW_MICROS + 2, b"c"),
        (1, b"a"),
        (SEGMENT_WINDOW_MICROS + 1, b"b"),
        (2, b"d"),
    ];
    write_and_flush_once(&engine, &table_id, &key, &rows);

    assert_eq!(
        generations(&engine, &table_id).len(),
        2,
        "the fixture must actually split this partition across two generations"
    );

    let partition = engine
        .read(&table_id, &key)
        .expect("read")
        .expect("the partition must be readable");
    let timestamps: Vec<i64> = partition.rows.iter().map(clustered_ts).collect();
    let mut sorted = timestamps.clone();
    sorted.sort_unstable();
    assert_eq!(
        timestamps, sorted,
        "a partition read across two generations must come back in clustering order, got \
         {timestamps:?}"
    );
    assert_eq!(
        timestamps,
        vec![1, 2, SEGMENT_WINDOW_MICROS + 1, SEGMENT_WINDOW_MICROS + 2],
        "every row of both windows must be present, got {timestamps:?}"
    );
    let values: Vec<Vec<u8>> = partition
        .rows
        .iter()
        .map(|row| {
            row.cells
                .first()
                .and_then(|(_, cell)| cell.value.clone())
                .expect("value")
        })
        .collect();
    assert_eq!(
        values,
        vec![
            b"a".to_vec(),
            b"d".to_vec(),
            b"b".to_vec(),
            b"c".to_vec()
        ]
    );
}

/// INVARIANT 4: a table without `rrd.segment_window` is untouched — one
/// generation per flush, however the rows' timestamps spread.
#[test]
fn rrd_plain_table_still_writes_one_generation_per_flush() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
    let table_id = rrd_table_id("plain_ts");
    // The same time-shaped schema as a segmented table, and NOT segmented:
    // the only difference is the extension.
    engine
        .register_table(rrd_schema("plain_ts", &[]))
        .expect("register");
    let key = make_key("s1");

    // Rows in four different 17m windows, flushed as one memtable.
    write_and_flush_once(
        &engine,
        &table_id,
        &key,
        &[
            (1, b"a".as_slice()),
            (SEGMENT_WINDOW_MICROS + 1, b"b".as_slice()),
            (2 * SEGMENT_WINDOW_MICROS + 1, b"c".as_slice()),
            (3 * SEGMENT_WINDOW_MICROS + 1, b"d".as_slice()),
        ],
    );
    assert_eq!(
        generations(&engine, &table_id).len(),
        1,
        "a plain table must still write exactly one generation per flush"
    );
    let gens = generations(&engine, &table_id);
    let partition = generation_rows(&engine, &table_id, &gens[0], &key).expect("partition");
    assert_eq!(
        partition.rows.len(),
        4,
        "the one generation holds every row of the memtable"
    );

    // A second flush adds exactly one more generation.
    write_and_flush_once(
        &engine,
        &table_id,
        &key,
        &[(4 * SEGMENT_WINDOW_MICROS + 1, b"e".as_slice())],
    );
    assert_eq!(
        generations(&engine, &table_id).len(),
        2,
        "two flushes of a plain table are two generations"
    );
}

/// A segmented flush cannot window a table with no 8-byte timestamp component,
/// so it fails loudly instead of writing a file that spans windows (which
/// retention would then be unable to expire).
#[test]
fn rrd_flush_rejects_a_table_without_a_timestamp_clustering_component() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
    let table_id = rrd_table_id("rrd_no_ts");
    // `test_schema()`'s 4-byte `int` clustering.
    let mut schema = test_schema();
    schema.table = "rrd_no_ts".to_string();
    schema.extensions = [
        ("rrd.segment_window".to_string(), "17m".to_string()),
        ("rrd.retain".to_string(), "4h".to_string()),
    ]
    .into_iter()
    .collect();
    engine.register_table(schema).expect("register");

    let key = make_key("s1");
    engine
        .write(&table_id, &key, make_row(b"v", 1_000), 1_000)
        .expect("write");

    let error = engine
        .flush(&table_id)
        .expect_err("a segmented table with no timestamp component cannot be windowed");
    let message = error.to_string();
    assert!(
        message.contains("rrd.segment_window"),
        "the error must name the extension that demands a window, got: {message}"
    );
    assert!(
        message.contains("clustering"),
        "the error must say what the schema lacks, got: {message}"
    );
}

/// A segmented table's secondary-index sidecars are built from the memtable's
/// postings, which are not windowed, so one generation's sidecar would point at
/// rows another generation holds. Refuse rather than serve a wrong index.
#[test]
fn rrd_flush_refuses_a_segmented_table_with_a_secondary_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
    let table_id = rrd_table_id("rrd_indexed");
    engine
        .register_table_with_indexes(
            segmented_schema("rrd_indexed"),
            vec![("val_idx".to_string(), 0_usize)],
        )
        .expect("register");

    let key = make_key("s1");
    engine
        .write(&table_id, &key, rrd_row(1, b"a", 1), 1)
        .expect("write");

    let error = engine
        .flush(&table_id)
        .expect_err("a segmented flush cannot write correct index sidecars");
    let message = error.to_string();
    assert!(
        message.contains("index"),
        "the error must say an index is what a segmented flush cannot serve, got: {message}"
    );
    // The rows are still there: a refused flush must not lose the memtable.
    assert_eq!(
        engine
            .read(&table_id, &key)
            .expect("read")
            .expect("partition")
            .rows
            .len(),
        1,
        "a refused segmented flush must leave its rows readable"
    );
}