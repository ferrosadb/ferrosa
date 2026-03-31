//! Import data from a Cassandra SSTable into Ferrosa storage.
//!
//! Usage: `ferrosa-sstable-import <path-to-sstable-dir> <target-data-dir>`

use std::path::Path;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: ferrosa-sstable-import <path-to-sstable-dir> <target-data-dir>");
        eprintln!();
        eprintln!("Reads Cassandra BTI-format SSTables and imports them into");
        eprintln!("a Ferrosa data directory for migration.");
        std::process::exit(1);
    }

    let source = Path::new(&args[1]);
    let target = Path::new(&args[2]);

    if !source.exists() {
        eprintln!("Error: source not found: {}", source.display());
        std::process::exit(1);
    }
    if !target.exists() {
        eprintln!("Error: target directory not found: {}", target.display());
        std::process::exit(1);
    }

    // TODO: Discover SSTable components in source dir, read each table,
    // write into Ferrosa storage format at target dir.
    eprintln!(
        "ferrosa-sstable-import: {} -> {}",
        source.display(),
        target.display()
    );
    eprintln!("SSTable import not yet wired — see ferrosa-sstable::reader + ferrosa-storage");
}
