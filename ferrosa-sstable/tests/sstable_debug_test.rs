// Integration test to read a real SSTable from disk
#[cfg(test)]
mod sstable_debug {
    use ferrosa_sstable::io::FileReadAt;
    use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};

    #[test]
    fn read_real_sstable_sequential() {
        let dir = "/tmp/sstable-debug";
        let data = FileReadAt::open(format!("{dir}/1-Data.db")).unwrap();
        let partitions = FileReadAt::open(format!("{dir}/1-Partitions.db")).unwrap();
        let rows = FileReadAt::open(format!("{dir}/1-Rows.db")).unwrap();
        let filter = std::fs::read(format!("{dir}/1-Filter.db")).unwrap();
        let statistics = std::fs::read(format!("{dir}/1-Statistics.db")).unwrap();

        let reader = SSTableReader::open(SSTableComponents {
            data,
            partitions,
            rows,
            filter,
            compression_info: None,
            statistics,
        })
        .unwrap();

        eprintln!("key_count: {}", reader.key_count());
        eprintln!("header key_type: {}", reader.header().key_type);

        // Sequential scan (no index lookup)
        let parts = reader.read_all_partitions().unwrap();
        eprintln!("partitions read: {}", parts.len());
        for (i, p) in parts.iter().enumerate() {
            eprintln!(
                "  partition {}: key={:?} token={} rows={}",
                i,
                p.key.key,
                p.key.token.0,
                p.rows.len()
            );
        }
        assert!(!parts.is_empty(), "should have at least one partition");
    }

    #[test]
    fn read_all_entity_store_sstables() {
        let dir = "/tmp/sstable-debug/entity_store";
        let mut total_partitions = 0;
        let mut total_rows = 0;
        let mut errors = Vec::new();

        let mut gens: Vec<u64> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_str()?.to_string();
                if name.ends_with("-Data.db") {
                    name.split('-').next()?.parse::<u64>().ok()
                } else {
                    None
                }
            })
            .collect();
        gens.sort();

        for gen in &gens {
            let data = match FileReadAt::open(format!("{dir}/{gen}-Data.db")) {
                Ok(d) => d,
                Err(e) => {
                    errors.push(format!("gen {gen}: open Data.db: {e}"));
                    continue;
                }
            };
            let partitions = match FileReadAt::open(format!("{dir}/{gen}-Partitions.db")) {
                Ok(p) => p,
                Err(e) => {
                    errors.push(format!("gen {gen}: open Partitions.db: {e}"));
                    continue;
                }
            };
            let rows = match FileReadAt::open(format!("{dir}/{gen}-Rows.db")) {
                Ok(r) => r,
                Err(e) => {
                    errors.push(format!("gen {gen}: open Rows.db: {e}"));
                    continue;
                }
            };
            let filter = match std::fs::read(format!("{dir}/{gen}-Filter.db")) {
                Ok(f) => f,
                Err(e) => {
                    errors.push(format!("gen {gen}: read Filter.db: {e}"));
                    continue;
                }
            };
            let statistics = match std::fs::read(format!("{dir}/{gen}-Statistics.db")) {
                Ok(s) => s,
                Err(e) => {
                    errors.push(format!("gen {gen}: read Statistics.db: {e}"));
                    continue;
                }
            };
            let compression_info = std::fs::read(format!("{dir}/{gen}-CompressionInfo.db")).ok();

            let reader = match SSTableReader::open(SSTableComponents {
                data,
                partitions,
                rows,
                filter,
                compression_info,
                statistics,
            }) {
                Ok(r) => r,
                Err(e) => {
                    errors.push(format!("gen {gen}: open: {e}"));
                    continue;
                }
            };

            match reader.read_all_partitions() {
                Ok(parts) => {
                    let row_count: usize = parts.iter().map(|p| p.rows.len()).sum();
                    total_partitions += parts.len();
                    total_rows += row_count;
                    eprintln!(
                        "gen {gen}: {parts} partitions, {rows} rows",
                        parts = parts.len(),
                        rows = row_count
                    );
                }
                Err(e) => {
                    errors.push(format!("gen {gen}: read_all_partitions: {e}"));
                }
            }
        }

        eprintln!(
            "\nTotal: {total_partitions} partitions, {total_rows} rows across {} SSTables",
            gens.len()
        );
        if !errors.is_empty() {
            eprintln!("\nErrors ({}):", errors.len());
            for e in &errors {
                eprintln!("  {e}");
            }
        }
        assert!(errors.is_empty(), "had {} SSTable errors", errors.len());
    }

    #[test]
    fn find_broken_audit_log_sstable() {
        use ferrosa_common::{DecoratedKey, PartitionKey, Token};

        let dir = "/tmp/sstable-debug/audit_log";
        let key_bytes: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let dk = DecoratedKey::new(PartitionKey::from(key_bytes.as_slice()));
        eprintln!("Lookup key token: {}", dk.token.0);

        let mut gens: Vec<u64> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_str()?.to_string();
                if name.ends_with("-Data.db") {
                    name.split('-').next()?.parse::<u64>().ok()
                } else {
                    None
                }
            })
            .collect();
        gens.sort();

        for gen in &gens {
            let data = FileReadAt::open(format!("{dir}/{gen}-Data.db")).unwrap();
            let partitions = FileReadAt::open(format!("{dir}/{gen}-Partitions.db")).unwrap();
            let rows = FileReadAt::open(format!("{dir}/{gen}-Rows.db")).unwrap();
            let filter = std::fs::read(format!("{dir}/{gen}-Filter.db")).unwrap();
            let statistics = std::fs::read(format!("{dir}/{gen}-Statistics.db")).unwrap();
            let ci = std::fs::read(format!("{dir}/{gen}-CompressionInfo.db")).ok();

            let reader = match SSTableReader::open(SSTableComponents {
                data,
                partitions,
                rows,
                filter,
                compression_info: ci,
                statistics,
            }) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("gen {gen}: OPEN ERROR: {e}");
                    continue;
                }
            };

            match reader.get_partition(&dk) {
                Ok(Some(p)) => eprintln!("gen {gen}: FOUND {} rows", p.rows.len()),
                Ok(None) => eprintln!("gen {gen}: not found (bloom/index miss)"),
                Err(e) => eprintln!("gen {gen}: LOOKUP ERROR: {e}"),
            }
        }
    }

    #[test]
    fn point_lookup_real_sstable() {
        use ferrosa_common::{DecoratedKey, PartitionKey, Token};

        let dir = "/tmp/sstable-debug";
        let data = FileReadAt::open(format!("{dir}/1-Data.db")).unwrap();
        let partitions = FileReadAt::open(format!("{dir}/1-Partitions.db")).unwrap();
        let rows = FileReadAt::open(format!("{dir}/1-Rows.db")).unwrap();
        let filter = std::fs::read(format!("{dir}/1-Filter.db")).unwrap();
        let statistics = std::fs::read(format!("{dir}/1-Statistics.db")).unwrap();

        let reader = SSTableReader::open(SSTableComponents {
            data,
            partitions,
            rows,
            filter,
            compression_info: None,
            statistics,
        })
        .unwrap();

        // The partition key from the sequential scan
        let key_bytes: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let token = Token(2589554819249504804);
        let dk = DecoratedKey {
            token,
            key: PartitionKey::from(key_bytes.as_slice()),
        };
        eprintln!("Looking up key with EXACT token {}", token.0);

        match reader.get_partition(&dk) {
            Ok(Some(p)) => {
                eprintln!("Point lookup: found partition with {} rows", p.rows.len());
                assert_eq!(p.rows.len(), 1);
            }
            Ok(None) => {
                eprintln!("Point lookup: NOT FOUND");
                let (h1, h2) = dk.filter_hash();
                let in_bloom = reader.bloom_filter().is_present(h1, h2);
                eprintln!("  bloom filter present: {}", in_bloom);
            }
            Err(e) => {
                eprintln!("Point lookup ERROR: {}", e);
            }
        }

        // Now try with DecoratedKey::new which computes token via Murmur3
        let dk2 = DecoratedKey::new(PartitionKey::from(key_bytes.as_slice()));
        eprintln!(
            "\nLooking up with computed token (from_raw_key): {}",
            dk2.token.0
        );
        eprintln!("Token match: {}", dk2.token.0 == token.0);

        match reader.get_partition(&dk2) {
            Ok(Some(p)) => {
                eprintln!("Computed-token lookup: found {} rows", p.rows.len());
            }
            Ok(None) => {
                eprintln!("Computed-token lookup: NOT FOUND");
                let (h1, h2) = dk2.filter_hash();
                let in_bloom = reader.bloom_filter().is_present(h1, h2);
                eprintln!("  bloom filter present: {}", in_bloom);
            }
            Err(e) => {
                eprintln!("Computed-token lookup ERROR: {}", e);
            }
        }
    }
}
