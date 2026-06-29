//! Import data from a Cassandra SSTable into Ferrosa storage.
//!
//! Usage: `ferrosa-sstable-import <sstable-dir> <generation-id> <target-data-dir> <keyspace> <table>`
//!
//! Copies SSTable component files into the Ferrosa data directory
//! structure so the engine picks them up on next startup.

use std::path::PathBuf;

fn temp_import_directory(table_dir: &std::path::Path, gen: &str) -> PathBuf {
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_nanos())
        .unwrap_or(0);
    table_dir.join(format!(".import-{gen}-{}-{suffix}", std::process::id()))
}

fn sync_directory(dir: &std::path::Path) -> std::io::Result<()> {
    let file = std::fs::File::open(dir)?;
    file.sync_all()
}

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

    let required_components = ["Data.db", "Partitions.db", "Rows.db"];
    for component in required_components {
        let path = source_dir.join(format!("{gen}-{component}"));
        if !path.exists() {
            eprintln!(
                "Error: required SSTable component {component} not found: {}",
                path.display()
            );
            std::process::exit(1);
        }
    }

    let table_dir = target_dir
        .join("sstables")
        .join(format!("{keyspace}.{table}"));
    if let Err(e) = std::fs::create_dir_all(&table_dir) {
        eprintln!("Error creating target dir: {e}");
        std::process::exit(1);
    }
    let final_dir = table_dir.join(gen);
    if final_dir.exists() {
        eprintln!(
            "Error: target generation directory already exists: {}",
            final_dir.display()
        );
        std::process::exit(1);
    }
    let staging_dir = temp_import_directory(&table_dir, gen);
    if let Err(e) = std::fs::create_dir(&staging_dir) {
        eprintln!("Error creating import staging dir: {e}");
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
            let dst = staging_dir.join(format!("{gen}-{ext}"));
            match std::fs::copy(&src, &dst) {
                Ok(bytes) => {
                    println!("  {gen}-{ext} ({bytes} bytes)");
                    copied += 1;
                }
                Err(e) => {
                    eprintln!("  Error copying {gen}-{ext}: {e}");
                    let _ = std::fs::remove_dir_all(&staging_dir);
                    std::process::exit(1);
                }
            }
        }
    }

    if let Err(e) = sync_directory(&staging_dir) {
        eprintln!("Error syncing import staging dir: {e}");
        let _ = std::fs::remove_dir_all(&staging_dir);
        std::process::exit(1);
    }
    if let Err(e) = std::fs::rename(&staging_dir, &final_dir) {
        eprintln!("Error promoting import into {}: {e}", final_dir.display());
        let _ = std::fs::remove_dir_all(&staging_dir);
        std::process::exit(1);
    }
    if let Err(e) = sync_directory(&table_dir) {
        eprintln!("Error syncing target table dir: {e}");
        std::process::exit(1);
    }

    println!("Imported {copied} files to {}/", final_dir.display());
    println!("Restart ferrosa to load the imported SSTable.");
}
