//! Regression coverage for RustFS test credentials in Jepsen compose files.

use std::path::{Path, PathBuf};

fn compose_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn jepsen_compose_files_allow_rustfs_default_credentials() {
    for relative in [
        "tests/docker/jepsen-cluster.yml",
        "tests/docker/jepsen-cluster-t3.yml",
    ] {
        assert_rustfs_default_credentials_are_explicitly_allowed(&compose_path(relative));
    }
}

fn assert_rustfs_default_credentials_are_explicitly_allowed(path: &Path) {
    let raw =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let parsed: serde_yaml::Value =
        serde_yaml::from_str(&raw).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
    let services = parsed
        .get("services")
        .and_then(|v| v.as_mapping())
        .unwrap_or_else(|| panic!("{} missing services mapping", path.display()));
    let rustfs = services
        .get(serde_yaml::Value::String("rustfs".into()))
        .unwrap_or_else(|| panic!("{} missing rustfs service", path.display()));
    let env = rustfs
        .get("environment")
        .and_then(|v| v.as_mapping())
        .unwrap_or_else(|| panic!("{} rustfs service missing environment", path.display()));

    let access_key = env
        .get(serde_yaml::Value::String("RUSTFS_ACCESS_KEY".into()))
        .and_then(|v| v.as_str());
    let secret_key = env
        .get(serde_yaml::Value::String("RUSTFS_SECRET_KEY".into()))
        .and_then(|v| v.as_str());

    if access_key == Some("rustfsadmin") && secret_key == Some("rustfsadmin") {
        assert_eq!(
            env.get(serde_yaml::Value::String(
                "RUSTFS_ALLOW_INSECURE_DEFAULT_CREDENTIALS".into()
            ))
            .and_then(|v| v.as_str()),
            Some("true"),
            "{} uses RustFS default credentials and must set \
             RUSTFS_ALLOW_INSECURE_DEFAULT_CREDENTIALS=true",
            path.display()
        );
    }
}
