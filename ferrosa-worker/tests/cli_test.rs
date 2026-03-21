use std::process::Command;

#[test]
fn worker_prints_usage_with_no_args() {
    let output = Command::new(env!("CARGO_BIN_EXE_ferrosa-worker"))
        .output()
        .expect("failed to run ferrosa-worker");

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let result: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(result["success"], false);
    assert!(result["error"].as_str().unwrap().contains("Usage"));
}

#[test]
fn worker_rejects_invalid_json() {
    let output = Command::new(env!("CARGO_BIN_EXE_ferrosa-worker"))
        .arg("not valid json")
        .output()
        .expect("failed to run ferrosa-worker");

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let result: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(result["success"], false);
    assert!(result["error"]
        .as_str()
        .unwrap()
        .contains("Failed to parse"));
}

#[test]
fn worker_accepts_index_build_task_stub() {
    let task = serde_json::json!({
        "type": "IndexBuild",
        "sstable_s3_paths": ["s3://bucket/ks/tbl/sst-001-Data.db"],
        "keyspace": "ks",
        "table": "tbl",
        "index_name": "idx_email",
        "index_metadata_json": "{}",
        "table_schema_json": "{}",
        "output_s3_prefix": "s3://bucket/ks/tbl/indexes/"
    });

    let output = Command::new(env!("CARGO_BIN_EXE_ferrosa-worker"))
        .arg(task.to_string())
        .output()
        .expect("failed to run ferrosa-worker");

    // Stub returns failure (not yet implemented), but parses correctly.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let result: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(result["success"], false);
    assert!(result["error"]
        .as_str()
        .unwrap()
        .contains("not yet implemented"));
}
