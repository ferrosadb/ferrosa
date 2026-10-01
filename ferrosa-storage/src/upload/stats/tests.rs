use super::*;
use futures::TryStreamExt;
use object_store::memory::InMemory;

fn sstable_key(table: &str, gen: &str, component: &str) -> Path {
    Path::from(format!("prefix/ab/{table}/{gen}/{gen}-{component}"))
}

fn fixture() -> (StatsStore, Arc<ObjectStoreStats>) {
    let stats = Arc::new(ObjectStoreStats::new());
    let store = StatsStore::new(Arc::new(InMemory::new()), Arc::clone(&stats));
    (store, stats)
}

fn op<'a>(snaps: &'a [OpSnapshot], label: &str) -> &'a OpSnapshot {
    snaps.iter().find(|s| s.op == label).expect("op present")
}

fn object<'a>(snaps: &'a [ObjectSnapshot], table: &str, component: &str) -> &'a ObjectSnapshot {
    snaps
        .iter()
        .find(|s| s.key.table == table && s.key.component == component)
        .unwrap_or_else(|| panic!("no stats for {table}/{component}: {snaps:?}"))
}

#[test]
fn the_stats_flag_is_strict() {
    assert!(!parse_flag("X", None).unwrap());
    assert!(!parse_flag("X", Some("")).unwrap());
    for on in ["1", "true", "ON", "yes"] {
        assert!(parse_flag("X", Some(on)).unwrap(), "{on}");
    }
    for off in ["0", "false", "off", "no"] {
        assert!(!parse_flag("X", Some(off)).unwrap(), "{off}");
    }
    let err = parse_flag("FERROSA_S3_STATS", Some("maybe"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("FERROSA_S3_STATS"), "{err}");
}

#[test]
fn object_keys_parse_table_and_component() {
    let key = parse_object_key(&sstable_key("ks.events", "1790000000000009", "Data.db"));
    assert_eq!(key.table, "ks.events");
    assert_eq!(key.component, "Data.db");
    // An empty prefix (path normalises away the empty segment) still parses.
    let key = parse_object_key(&Path::from("ab/ks.t/5/5-Partitions.db"));
    assert_eq!(
        (key.table.as_str(), key.component.as_str()),
        ("ks.t", "Partitions.db")
    );
    for other in [
        "manifest.json",
        "p/manifest/v1.json",
        "p/ab/ks.t/5/other.txt",
    ] {
        let key = parse_object_key(&Path::from(other));
        assert_eq!(
            (key.table.as_str(), key.component.as_str()),
            ("-", "other"),
            "{other}"
        );
    }
}

#[test]
fn histogram_quantiles_report_bucket_upper_bounds() {
    let h = Histogram::new(&LATENCY_BUCKETS_MS);
    assert_eq!(h.quantile_upper(0.5), None);
    for _ in 0..98 {
        h.observe(8);
    }
    h.observe(400);
    h.observe(60_000);
    assert_eq!(h.quantile_upper(0.5), Some(10));
    assert_eq!(h.quantile_upper(0.99), Some(500));
    assert_eq!(
        h.quantile_upper(1.0),
        Some(30_000),
        "overflow reports the largest bound"
    );
    assert_eq!(h.count(), 100);
}

#[tokio::test]
async fn puts_gets_heads_and_deletes_are_counted_by_operation() {
    let (store, stats) = fixture();
    let path = sstable_key("ks.t", "7", "Data.db");
    store
        .put(&path, PutPayload::from(vec![1u8; 1000]))
        .await
        .unwrap();
    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.len(), 1000);
    store.head(&path).await.unwrap();
    store.delete(&path).await.unwrap();

    let ops = stats.op_snapshots();
    assert_eq!((op(&ops, "put").requests, op(&ops, "put").bytes), (1, 1000));
    assert_eq!((op(&ops, "get").requests, op(&ops, "get").bytes), (1, 1000));
    assert_eq!(op(&ops, "head").requests, 1);
    assert_eq!(op(&ops, "delete").requests, 1);
    assert_eq!(op(&ops, "get").errors, 0);
}

#[tokio::test]
async fn a_failed_request_counts_as_an_error() {
    let (store, stats) = fixture();
    let missing = sstable_key("ks.t", "9", "Data.db");
    assert!(store.get(&missing).await.is_err());
    let ops = stats.op_snapshots();
    assert_eq!((op(&ops, "get").requests, op(&ops, "get").errors), (1, 1));
    assert_eq!(op(&ops, "get").rate_limited, 0, "a 404 is not a 429");
}

#[test]
fn a_429_is_counted_as_rate_limited_and_retries_are_attributed() {
    let stats = ObjectStoreStats::new();
    let e = object_store::Error::Generic {
        store: "S3",
        source: "HTTP status client error (429 Too Many Requests)".into(),
    };
    stats.finish(Op::GetRange, Instant::now(), 0, Some(&e));
    stats.record_retry("get_range");
    stats.record_retry("put_multipart");
    let ops = stats.op_snapshots();
    let get_range = op(&ops, "get_range");
    assert_eq!(
        (get_range.errors, get_range.rate_limited, get_range.retries),
        (1, 1, 1)
    );
    assert_eq!(op(&ops, "multipart").retries, 1);
}

#[tokio::test]
async fn gets_are_attributed_to_table_and_component_whole_versus_ranged() {
    let (store, stats) = fixture();
    let data = sstable_key("ks.a", "7", "Data.db");
    let rows = sstable_key("ks.b", "8", "Rows.db");
    store
        .put(&data, PutPayload::from(vec![0u8; 4096]))
        .await
        .unwrap();
    store
        .put(&rows, PutPayload::from(vec![0u8; 100]))
        .await
        .unwrap();

    let whole = store.get(&data).await.unwrap();
    let _consumed: Vec<Bytes> = whole.into_stream().try_collect().await.unwrap();
    store.get_range(&data, 0..1000).await.unwrap();
    store.get(&rows).await.unwrap().bytes().await.unwrap();

    let objects = stats.object_snapshots();
    let a = object(&objects, "ks.a", "Data.db");
    assert_eq!((a.get_whole, a.get_ranged), (1, 1));
    assert_eq!(a.bytes_fetched, 4096 + 1000);
    assert_eq!(
        a.bytes_requested, 1000,
        "only the ranged caller stated what it wanted"
    );
    assert_eq!(a.bytes_put, 4096);
    assert_eq!(a.objects_seen, 1, "sizes are sampled by whole GETs");
    assert_eq!(a.size_max, 4096);
    let b = object(&objects, "ks.b", "Rows.db");
    assert_eq!((b.get_whole, b.bytes_fetched), (1, 100));
}

#[test]
fn a_download_record_feeds_throughput_and_read_amplification() {
    let stats = ObjectStoreStats::new();
    let path = sstable_key("ks.t", "7", "Data.db");
    stats.record_download(&DownloadRecord {
        path: &path,
        object_bytes: 100_000_000,
        elapsed: Duration::from_secs(2),
        parts: 7,
        ranged: true,
        part_retries: 3,
    });
    let objects = stats.object_snapshots();
    let s = object(&objects, "ks.t", "Data.db");
    assert_eq!((s.downloads, s.ranged_downloads, s.part_retries), (1, 1, 3));
    assert!((s.download_mb_per_sec() - 50.0).abs() < 0.01);
    assert_eq!(s.bytes_requested, 100_000_000);
    // Nothing fetched through a StatsStore yet, so fetched/requested is 0.
    assert_eq!(s.read_amplification(), 0.0);
}

#[test]
fn read_amplification_is_one_when_nothing_was_requested() {
    let stats = ObjectStoreStats::new();
    stats.record_requested(&sstable_key("ks.t", "7", "Data.db"), 0);
    let objects = stats.object_snapshots();
    assert_eq!(
        object(&objects, "ks.t", "Data.db").read_amplification(),
        1.0
    );
}

#[test]
fn label_cardinality_is_bounded() {
    let stats = ObjectStoreStats::new();
    let limit = tracked_label_pairs();
    for i in 0..(limit + 50) {
        stats.record_requested(&sstable_key(&format!("ks.t{i}"), "1", "Data.db"), 1);
    }
    let objects = stats.object_snapshots();
    assert_eq!(objects.len(), limit + 1, "the cap plus one overflow bucket");
    let overflow = object(&objects, "-", "overflow");
    assert_eq!(overflow.bytes_requested, 50);
}

#[tokio::test]
async fn prometheus_text_carries_op_and_per_table_series() {
    let (store, stats) = fixture();
    let path = sstable_key("ks.q", "7", "Data.db");
    store
        .put(&path, PutPayload::from(vec![0u8; 10]))
        .await
        .unwrap();
    store.get(&path).await.unwrap().bytes().await.unwrap();

    let text = stats.render_prometheus();

    assert!(
        text.contains("ferrosa_s3_requests_total{op=\"put\"} 1"),
        "{text}"
    );
    assert!(
        text.contains("ferrosa_s3_request_bytes_total{op=\"get\"} 10"),
        "{text}"
    );
    assert!(
        text.contains("ferrosa_s3_request_duration_seconds_count{op=\"get\"} 1"),
        "{text}"
    );
    assert!(
        text.contains(
            "ferrosa_s3_object_bytes_fetched_total{table=\"ks.q\",component=\"Data.db\"} 10"
        ),
        "{text}"
    );
    assert!(
        text.contains("ferrosa_s3_object_size_bytes_bucket{table=\"ks.q\",component=\"Data.db\",le=\"65536\"} 1"),
        "{text}"
    );
}

#[test]
fn label_values_are_escaped() {
    assert_eq!(escape("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
}

#[test]
fn global_rendering_is_empty_until_enabled() {
    // The process-wide handle is only ever set by `enable`; with no test
    // enabling it, the metrics endpoint must add nothing.
    assert!(global().is_none());
    assert_eq!(render_prometheus(), "");
}

#[test]
fn pool_gauges_report_the_recorded_settings() {
    record_pool_settings(64, 96);
    let text = render_pool_gauges();
    assert!(text.contains("ferrosa_s3_max_in_flight 64\n"), "{text}");
    assert!(
        text.contains("ferrosa_s3_pool_max_idle_per_host 96\n"),
        "{text}"
    );
}
