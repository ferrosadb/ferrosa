// Included from `engine.rs`'s `mod tests` (see the `include!` list there), so
// `use super::*` gives these tests the private `StorageEngine` internals —
// `maybe_compact`, `strategy_for_table`, `collect_sstable_metadata` — that the
// production compaction planner itself uses. Testing through those, rather than
// through a helper written for the test, is the point: the assertions fail when
// the real planner schedules work.

/// Table schema with `rrd.*` (or any other) extensions set.
fn rrd_schema(table: &str, ext: &[(&str, &str)]) -> TableSchema {
    let mut schema = test_schema();
    schema.table = table.to_string();
    schema.extensions = ext
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    schema
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
            .write(table_id, &key, make_row(format!("v{i}").as_bytes(), ts), ts)
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