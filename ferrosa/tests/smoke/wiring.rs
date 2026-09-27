use super::*;
use ferrosa_sstable::pump::test_support::{PumpOverrides, PumpTrace};
use ferrosa_storage::TableId;

// A separate runtime per phase drops the listener and all its engine references.
// Merely dropping CqlServer would leave its spawned accept loop running.
#[test]
fn wiring_cql_flush_compact_restart() {
    let runtime = || {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    };
    let first = runtime();
    let (dir, schema, tables, trace) = first.block_on(async {
        let (state, dir) = setup_state_with_log_segment(1024 * 1024);
        let trace = PumpTrace::install(dir.path().to_path_buf(), PumpOverrides {
            segment_bytes: Some(262_144), queue_depth: Some(1),
        });
        let server = CqlServer::new(test_config(), state.clone());
        let mut client = CqlClient::connect(server.start_background().await.unwrap()).await.unwrap();
        client.query("CREATE KEYSPACE wiring WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}").await.unwrap();
        let mut tables = Vec::new();
        for (table, compression) in [("plain", "{'enabled':'false'}"), ("compressed", "{'class':'LZ4Compressor'}")] {
            client.query(&format!("CREATE TABLE wiring.{table} (id int PRIMARY KEY, value text) WITH compression = {compression}")).await.unwrap();
            let tid = TableId::new("wiring", table);
            for batch in 0..2 {
                for n in batch * 4..batch * 4 + 4 {
                    let value = char::from(b'a' + n as u8).to_string().repeat(if n == 0 { 300_000 } else { 64 });
                    client.query(&format!("INSERT INTO wiring.{table} (id,value) VALUES ({n},'{value}')")).await.unwrap();
                }
                state.core.engine.flush(&tid).unwrap();
            }
            assert_eq!(state.core.engine.sstable_count(&tid), 2);
            let checksum = if table == "compressed" { "CompressionInfo.db" } else { "CRC.db" };
            assert_eq!(trace.files().iter().filter(|f| f.opened.path.starts_with(state.core.engine.table_sstable_dir(&tid)) && f.opened.path.ends_with(checksum)).count(), 2);

            tables.push(state.core.engine.table_schema(&tid).unwrap());
        }
        trace.assert_complete_sstables(4);
        state.core.engine.force_compact_all();
        assert!(state.core.engine.await_compaction_result(std::time::Duration::from_secs(60)));
        state.core.engine.poll_compactions().await;
        if tables.iter().any(|t| state.core.engine.sstable_count(&TableId::new(&t.keyspace, &t.table)) != 1) {
            assert!(state.core.engine.await_compaction_result(std::time::Duration::from_secs(60)));
            state.core.engine.poll_compactions().await;
        }
        for table in &tables {
            assert_eq!(state.core.engine.sstable_count(&TableId::new(&table.keyspace, &table.table)), 1);
        }
        trace.assert_complete_sstables(6);
        (dir, state.core.schema.clone(), tables, trace)
    });
    drop(first);
    let second = runtime();
    second.block_on(async {
        let engine = Arc::new(
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap(),
        );
        for table in tables {
            engine.register_table(table).unwrap();
        }
        for table in ["plain", "compressed"] {
            let dir = engine.table_sstable_dir(&TableId::new("wiring", table));
            let generations = StorageEngine::list_generations_in_dir(&dir);
            assert_eq!(generations.len(), 1);
            let target =
                ferrosa_storage::flush::FileFlushTarget::new_starting_at(dir.clone()).unwrap();
            // Compaction atomically publishes a generation directory.
            let component_dir = dir.join(generations[0].to_string());
            assert!(component_dir.is_dir());
            let reader = ferrosa_storage::flush::FlushTarget::open_reader(
                &target,
                &component_dir,
                generations[0],
            )
            .unwrap();
            assert!(reader.digest_loaded());
            reader.verify_digest().unwrap();
        }
        let state = setup_state_for(engine, schema);
        let server = CqlServer::new(test_config(), state);
        let mut client = CqlClient::connect(server.start_background().await.unwrap())
            .await
            .unwrap();
        for table in ["plain", "compressed"] {
            let rows = client
                .query(&format!("SELECT id,value FROM wiring.{table}"))
                .await
                .unwrap();
            assert_eq!(rows.rows.len(), 8);
            let mut found = std::collections::BTreeSet::new();
            for row in &rows.rows {
                let id_bytes = row.columns[column_index(&rows, "id").unwrap()]
                    .as_ref()
                    .unwrap();
                let n = i32::from_be_bytes(id_bytes.as_slice().try_into().unwrap());
                assert!(found.insert(n));
                let expected = char::from(b'a' + n as u8).to_string().repeat(if n == 0 {
                    300_000
                } else {
                    64
                });
                assert_eq!(
                    cell_as_str(row, column_index(&rows, "value").unwrap()),
                    Some(expected)
                );
            }
            assert_eq!(found, (0..8).collect());
        }
    });
    drop(second);
    trace.assert_complete_sstables(6);
}
