//! Import data from a Cassandra SSTable into Ferrosa storage.
//!
//! Usage: `ferrosa-sstable-import <sstable-dir> <generation-id> <target-data-dir> <keyspace> <table>`
//!
//! Copies SSTable component files into the Ferrosa data directory
//! structure so the engine picks them up on next startup.

use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        eprintln!(
            "Usage: ferrosa-sstable-import <sstable-dir> <generation-id> \
             <target-data-dir> <keyspace> <table>"
        );
        eprintln!();
        eprintln!("Copies SSTable component files into the Ferrosa data directory");
        eprintln!("structure so the engine picks them up on next startup.");
        std::process::exit(1);
    }

    let source_dir = PathBuf::from(&args[1]);
    let gen = &args[2];
    let target_dir = PathBuf::from(&args[3]);
    let keyspace = &args[4];
    let table = &args[5];

    let data_path = source_dir.join(format!("{gen}-Data.db"));
    if !data_path.exists() {
        eprintln!("Error: Data.db not found: {}", data_path.display());
        std::process::exit(1);
    }

    let table_dir = target_dir
        .join("sstables")
        .join(format!("{keyspace}.{table}"));
    if let Err(e) = std::fs::create_dir_all(&table_dir) {
        eprintln!("Error creating target dir: {e}");
        std::process::exit(1);
    }

    let extensions = [
        "Data.db",
        "Partitions.db",
        "Rows.db",
        "Filter.db",
        "Statistics.db",
        "CompressionInfo.db",
        "TOC.txt",
    ];

    let mut copied = 0;
    for ext in &extensions {
        let src = source_dir.join(format!("{gen}-{ext}"));
        if src.exists() {
            let dst = table_dir.join(format!("{gen}-{ext}"));
            match std::fs::copy(&src, &dst) {
                Ok(bytes) => {
                    println!("  {gen}-{ext} ({bytes} bytes)");
                    copied += 1;
                }
                Err(e) => {
                    eprintln!("  Error copying {gen}-{ext}: {e}");
                }
            }
        }
    }

    println!("Imported {copied} files to {}/", table_dir.display());
    println!("Restart ferrosa to load the imported SSTable.");
}
