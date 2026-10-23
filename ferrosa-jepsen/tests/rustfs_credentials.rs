//! Regression coverage for RustFS test credentials in Jepsen compose files.

use std::path::{Path, PathBuf};

use yaml_rust2::{Yaml, YamlLoader};

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
    let docs =
        YamlLoader::load_from_str(&raw).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
    let services = docs
        .first()
        .and_then(|doc| doc["services"].as_hash())
        .unwrap_or_else(|| panic!("{} missing services mapping", path.display()));
    let rustfs = services
        .get(&Yaml::String("rustfs".into()))
        .unwrap_or_else(|| panic!("{} missing rustfs service", path.display()));
    let env = rustfs["environment"]
        .as_hash()
        .unwrap_or_else(|| panic!("{} rustfs service missing environment", path.display()));
    let env_str = |key: &str| env.get(&Yaml::String(key.into())).and_then(Yaml::as_str);

    let access_key = env_str("RUSTFS_ACCESS_KEY");
    let secret_key = env_str("RUSTFS_SECRET_KEY");

    if access_key == Some("rustfsadmin") && secret_key == Some("rustfsadmin") {
        assert_eq!(
            env_str("RUSTFS_ALLOW_INSECURE_DEFAULT_CREDENTIALS"),
            Some("true"),
            "{} uses RustFS default credentials and must set \
             RUSTFS_ALLOW_INSECURE_DEFAULT_CREDENTIALS=true",
            path.display()
        );
    }
}
