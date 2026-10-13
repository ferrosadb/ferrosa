//! Shared helpers for `ferrosa-jepsen` integration tests.
//!
//! Integration test files are separate crates, so each one that needs these
//! helpers declares `mod support;`. A given helper may be used by only one of
//! those crates, so dead code is allowed here rather than per-item.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// The lowest port the Linux kernel may hand out as an outbound connection's
/// source port (`net.ipv4.ip_local_port_range` defaults to `32768 60999`).
///
/// Any *fixed* host port at or above this value races the kernel: an image pull
/// or registry/HTTP call can already hold it as a source port, and then
/// `docker compose up` fails with
/// `failed to bind host port for 0.0.0.0:<port> ... address already in use`.
/// macOS's floor is higher (49152), so 32768 is the safe common floor.
pub const EPHEMERAL_PORT_FLOOR: u16 = 32768;

/// The workspace root, derived from the test crate's `CARGO_MANIFEST_DIR`.
pub fn repo_root() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .expect("CARGO_MANIFEST_DIR must be set under cargo test");
    manifest_dir.join("..")
}

/// Every `.yml`/`.yaml` file under `root`, sorted for deterministic output.
///
/// Skips VCS directories and build output (`target/`, `.target*`,
/// `node_modules/`, `vendor/`) so the walk stays bounded.
pub fn yaml_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_yaml(root, &mut out);
    out.sort();
    out
}

fn collect_yaml(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            if name == ".git"
                || name == "target"
                || name == "node_modules"
                || name == "vendor"
                || name.starts_with(".target")
            {
                continue;
            }
            collect_yaml(&path, out);
        } else if name.ends_with(".yml") || name.ends_with(".yaml") {
            out.push(path);
        }
    }
}

/// The `(host, container)` port pairs published by every `ports:` list entry in
/// a compose/workflow YAML document, in file order.
///
/// Deliberately line-oriented: a `ports:` entry is unambiguously
/// `- "HOST:CONTAINER"` (optionally `IP:HOST:CONTAINER`), and locating it with
/// a small indent tracker avoids depending on a YAML parser being able to read
/// every workflow file in the repo. `expose:`-style container-only entries and
/// bare container integers (which get a kernel-chosen host port) publish no
/// fixed host port and are ignored.
pub fn published_port_pairs(yaml: &str) -> Vec<(u16, u16)> {
    let mut out = Vec::new();
    let mut ports_indent: Option<usize> = None;

    for line in yaml.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent = line.len() - line.trim_start().len();

        // A `ports:` list ends when indentation returns to (or above) its key.
        if ports_indent.is_some_and(|pi| indent <= pi) {
            ports_indent = None;
        }

        if trimmed == "ports:" {
            ports_indent = Some(indent);
            continue;
        }

        if !ports_indent.is_some_and(|pi| indent > pi) {
            continue;
        }

        let Some(entry) = trimmed.strip_prefix('-') else {
            continue;
        };
        let entry = entry.trim();
        // Drop a trailing `# comment`; keep the first whitespace-delimited token.
        let token = entry.split_whitespace().next().unwrap_or("");
        let token = token.trim_matches(|c| c == '"' || c == '\'');
        let parts: Vec<&str> = token.split(':').collect();
        if parts.len() < 2 {
            continue;
        }
        let host = parts[parts.len() - 2];
        let container = parts[parts.len() - 1]
            .split('/')
            .next()
            .unwrap_or(parts[parts.len() - 1]);
        if let (Ok(host), Ok(container)) = (host.parse::<u16>(), container.parse::<u16>()) {
            out.push((host, container));
        }
    }

    out
}

/// The HOST side of every `ports:` publish mapping in a YAML document.
pub fn published_host_ports(yaml: &str) -> Vec<u16> {
    published_port_pairs(yaml)
        .into_iter()
        .map(|(host, _)| host)
        .collect()
}

/// Host ports named by `localhost:<port>` / `127.0.0.1:<port>` literals anywhere
/// in a YAML document (broadcast addresses, `FERROSA_TEST_*_URLS`, `curl`
/// probes, `cqlsh` invocations).
pub fn host_endpoint_ports(yaml: &str) -> Vec<u16> {
    let mut out = Vec::new();
    for marker in ["localhost:", "127.0.0.1:"] {
        for (idx, _) in yaml.match_indices(marker) {
            let rest = &yaml[idx + marker.len()..];
            let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(port) = digits.parse::<u16>() {
                out.push(port);
            }
        }
    }
    out
}

/// Host ports named by workflow `node_specs`-style entries that read
/// `"<host_port>|<...>"` (e.g. `"29090|11111111-..."`).
pub fn quoted_pipe_host_ports(yaml: &str) -> Vec<u16> {
    let mut out = Vec::new();
    for (idx, _) in yaml.match_indices('"') {
        let rest = &yaml[idx + 1..];
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            continue;
        }
        if rest[digits.len()..].starts_with('|') {
            if let Ok(port) = digits.parse::<u16>() {
                out.push(port);
            }
        }
    }
    out
}
