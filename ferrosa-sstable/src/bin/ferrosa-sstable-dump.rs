//! Dump an SSTable to human-readable output.
//!
//! Usage: `ferrosa-sstable-dump <path-to-data-component>`

use std::path::Path;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: ferrosa-sstable-dump <path-to-data-component>");
        eprintln!();
        eprintln!("Reads a Cassandra BTI-format SSTable and prints partition");
        eprintln!("keys and row counts in human-readable format.");
        std::process::exit(1);
    }

    let path = Path::new(&args[1]);
    if !path.exists() {
        eprintln!("Error: file not found: {}", path.display());
        std::process::exit(1);
    }

    // TODO: Open SSTable components and iterate partitions.
    // The reader API requires SSTableComponents (Data, PartitionIndex,
    // RowIndex, etc.) — wire up path-based discovery here.
    eprintln!("ferrosa-sstable-dump: reading {}", path.display());
    eprintln!("SSTable reading not yet wired — see ferrosa-sstable::reader");
}
