fn main() {
    let db = sled::open("/tmp/raft-forensics").unwrap();
    let log = db.open_tree("log").unwrap();
    for i in [6064u64, 6072u64] {
        let v = log.get(i.to_be_bytes()).unwrap().unwrap();
        std::fs::write(format!("/tmp/entry-{i}.bin"), v.as_ref()).unwrap();
        let hex: String = v.iter().take(220).map(|b| format!("{b:02x} ")).collect();
        let ascii: String = v
            .iter()
            .take(220)
            .map(|&b| {
                if (32..127).contains(&b) {
                    b as char
                } else {
                    '.'
                }
            })
            .collect();
        println!("== {i} len={} ==\n{hex}\n{ascii}\n", v.len());
    }
}
