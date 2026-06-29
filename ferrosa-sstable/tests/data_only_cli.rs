use std::process::Command;

fn write_data_only_sstable(dir: &std::path::Path, gen: &str) {
    std::fs::write(dir.join(format!("{gen}-Data.db")), b"nonzero data").unwrap();
}

#[test]
fn dump_reports_missing_required_components_without_panicking() {
    let dir = tempfile::tempdir().unwrap();
    write_data_only_sstable(dir.path(), "1");

    let output = Command::new(env!("CARGO_BIN_EXE_ferrosa-sstable-dump"))
        .arg(dir.path())
        .arg("1")
        .output()
        .expect("run ferrosa-sstable-dump");

    assert!(
        !output.status.success(),
        "Data-only SSTable dump must fail closed"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Partitions.db"),
        "error should identify the missing required component, got: {stderr}"
    );
    assert!(
        !stderr.contains("panicked at"),
        "dump must return a controlled error, not a Rust panic: {stderr}"
    );
}

#[test]
fn import_rejects_data_only_source_without_publishing_live_data_file() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    write_data_only_sstable(source.path(), "7");

    let output = Command::new(env!("CARGO_BIN_EXE_ferrosa-sstable-import"))
        .arg(source.path())
        .arg("7")
        .arg(target.path())
        .arg("ks")
        .arg("tbl")
        .output()
        .expect("run ferrosa-sstable-import");

    assert!(
        !output.status.success(),
        "Data-only import must fail closed instead of publishing an orphan"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Partitions.db"),
        "error should identify the missing required component, got: {stderr}"
    );

    let published = target
        .path()
        .join("sstables")
        .join("ks.tbl")
        .join("7-Data.db");
    assert!(
        !published.exists(),
        "import must not publish live Data-only SSTable component at {}",
        published.display()
    );
}
