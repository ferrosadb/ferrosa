//! Property-based tests for ferrosa-storage.

use proptest::prelude::*;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::WriteOptions;

use ferrosa_storage::flush::InMemoryFlushTarget;
use ferrosa_storage::memtable::Memtable;
use ferrosa_storage::merge::merge_partitions;
use ferrosa_storage::store::TableStore;
use ferrosa_storage::ShardedBTreeMemtable;

fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    }
}

fn make_key(s: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
}

fn make_partition(key: &str, value: &[u8], ts: i64) -> Partition {
    Partition {
        key: make_key(key),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(value.to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }],
    }
}

proptest! {
    #[test]
    fn memtable_round_trip(
        key_suffix in "[a-z]{1,10}",
        value in prop::collection::vec(any::<u8>(), 0..100),
        timestamp in 1i64..1_000_000,
    ) {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        let key = make_key(&key_suffix);
        let row = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(value.clone(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        };

        mem.put(&key, row, &schema).unwrap();
        let result = mem.get(&key).unwrap();
        prop_assert!(result.is_some());
        let partition = result.unwrap();
        prop_assert_eq!(partition.rows[0].cells[0].1.value.as_deref(), Some(value.as_slice()));
    }

    #[test]
    fn merge_commutativity(
        ts_a in 1i64..1_000_000,
        ts_b in 1i64..1_000_000,
    ) {
        let p_a = make_partition("k1", b"val_a", ts_a);
        let p_b = make_partition("k1", b"val_b", ts_b);

        let m1 = merge_partitions(vec![p_a.clone(), p_b.clone()]);
        let m2 = merge_partitions(vec![p_b, p_a]);

        prop_assert_eq!(m1.rows[0].cells[0].1.timestamp, m2.rows[0].cells[0].1.timestamp);
        prop_assert_eq!(m1.rows[0].cells[0].1.value.clone(), m2.rows[0].cells[0].1.value.clone());
    }

    #[test]
    fn timestamp_ordering(
        ts_low in 1i64..500_000,
        ts_high in 500_001i64..1_000_000,
    ) {
        let p_low = make_partition("k1", b"low", ts_low);
        let p_high = make_partition("k1", b"high", ts_high);

        let merged = merge_partitions(vec![p_low, p_high]);
        prop_assert_eq!(merged.rows[0].cells[0].1.timestamp, ts_high);
        prop_assert_eq!(merged.rows[0].cells[0].1.value.as_deref(), Some(b"high".as_slice()));
    }

    #[test]
    fn flush_preserves_all_data(n in 1usize..20) {
        let store = TableStore::new(
            test_schema(),
            InMemoryFlushTarget,
            WriteOptions { compression: None, ..WriteOptions::default() },
        );

        let mut keys = Vec::new();
        for i in 0..n {
            let key = make_key(&format!("prop_key_{i:04}"));
            let row = Row {
                clustering: vec![0x00, 0x00, 0x00, 0x01],
                cells: vec![(0, CellValue::live(format!("v{i}").into_bytes(), 1000 + i as i64))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000 + i as i64),
            };
            store.write(&key, row).unwrap();
            keys.push(key);
        }

        store.flush().unwrap();

        for key in &keys {
            let result = store.read(key).unwrap();
            prop_assert!(result.is_some(), "partition lost after flush");
        }
    }
}
