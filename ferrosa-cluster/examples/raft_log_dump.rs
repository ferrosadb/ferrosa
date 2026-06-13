//! Forensic decode of raft log entries from a sled db copy.
//! Usage: cargo run -p ferrosa-cluster --example raft_log_dump -- /tmp/raft-forensics 6000 5
use ferrosa_cluster::raft::FerrosRaftConfig;
use ferrosa_schema::metadata::table::TableMetadata;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = sled::open(&args[1]).expect("open sled");
    let log = db.open_tree("log").expect("log tree");
    let start: u64 = args[2].parse().expect("index");
    let count: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
    let mut ok = 0u64;
    let mut bad = vec![];
    for i in start..start + count {
        let Some(v) = log.get(i.to_be_bytes()).expect("get") else {
            continue;
        };
        let bytes = v.as_ref();
        assert_eq!(&bytes[..4], b"FRE1", "entry {i} not FRE1");
        match bincode::deserialize::<openraft::Entry<FerrosRaftConfig>>(&bytes[5..]) {
            Ok(_) => ok += 1,
            Err(e) => {
                bad.push(i);
                println!("entry {i}: ENTRY DECODE FAILED: {e}");
                // payload starts after log_id (term u64 + node u64 + index u64)
                // + payload tag u32; op tag u32 follows.
                let p = 5 + 8 + 8 + 8 + 4;
                let op_tag = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap());
                println!("  op tag = {op_tag}");
                if op_tag == 2 {
                    // CreateTable(Box<TableMetadata>)
                    let mut cur = std::io::Cursor::new(&bytes[p + 4..]);
                    match bincode::deserialize_from::<_, TableMetadata>(&mut cur) {
                        Ok(tm) => println!(
                            "  TableMetadata OK: {}.{} consumed {} of {} payload bytes",
                            tm.keyspace,
                            tm.name,
                            cur.position(),
                            bytes.len() - p - 4
                        ),
                        Err(e2) => {
                            let pos = cur.position() as usize;
                            let tail: String = bytes[p + 4 + pos.saturating_sub(8)..]
                                .iter()
                                .take(48)
                                .map(|b| format!("{b:02x} "))
                                .collect();
                            println!("  TableMetadata FAILED at payload offset {pos}: {e2}");
                            println!("  bytes around failure: {tail}");
                        }
                    }
                }
            }
        }
    }
    println!(
        "decoded ok: {ok}, failed: {} {:?}",
        bad.len(),
        &bad[..bad.len().min(10)]
    );
}
// (extended below via env var DUMP_RAW=1: write raw bytes of listed entries)
