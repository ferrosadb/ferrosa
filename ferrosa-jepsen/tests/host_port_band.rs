//! Guard: no host port this repo publishes or dials may sit inside the kernel's
//! ephemeral range.
//!
//! The PostgreSQL Jepsen fault-schedule CI job brings the cluster up with
//! `docker compose -f ferrosa-jepsen/tests/docker/jepsen-cluster.yml up -d`.
//! Every host port that compose published used to live in the 49000s, i.e.
//! inside Linux's `net.ipv4.ip_local_port_range` (`32768 60999`). The kernel
//! hands ports from that range to *outbound* connections — image pulls,
//! registry traffic, the job's own `curl` probes — so one of them could already
//! hold `0.0.0.0:49092` when Docker tried to publish it, and the whole stack
//! died before any ferrosa code ran:
//!
//! ```text
//! Error response from daemon: failed to set up container networking: driver
//! failed programming external connectivity on endpoint ferrosa-jepsen-node3
//! (...): failed to bind host port for 0.0.0.0:49092:172.18.0.4:9090/tcp:
//! address already in use
//! ```
//!
//! Ports **below** the floor can never be chosen as a source port, so moving
//! them out of the range removes the race instead of tolerating it. Re-running
//! the job or serialising jobs would leave the defect in place.
//!
//! `32768` is Linux's floor; macOS's is higher (`49152`), so the lower value is
//! the safe common floor. Every host-facing port in the repo must stay below it
//! — this matches the 29xxx band the multi-DC nightly topology
//! (`jepsen-cluster-t3.yml`, `jepsen-multi-dc-nightly.yml`) already uses.

mod support;

use ferrosa_jepsen::chaos::DEFAULT_ADMIN_URL;
use ferrosa_jepsen::docker_provision::NODE_CQL_PORTS;
use support::{
    host_endpoint_ports, published_host_ports, published_port_pairs, quoted_pipe_host_ports,
    repo_root, yaml_files, EPHEMERAL_PORT_FLOOR,
};

#[test]
fn published_host_ports_stay_below_the_ephemeral_range() {
    let root = repo_root();
    let mut violations: Vec<String> = Vec::new();

    // (A) Every compose/workflow YAML: the HOST side of every `ports:` publish
    // mapping, plus any `localhost:`/`127.0.0.1:` broadcast or probe endpoint and
    // any workflow `"<host_port>|..."` node_spec.
    for path in yaml_files(&root) {
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .display()
            .to_string();

        for port in published_host_ports(&text) {
            if port >= EPHEMERAL_PORT_FLOOR {
                violations.push(format!(
                    "{rel}: publishes host port {port} (>= {EPHEMERAL_PORT_FLOOR})"
                ));
            }
        }
        for port in host_endpoint_ports(&text)
            .into_iter()
            .chain(quoted_pipe_host_ports(&text))
        {
            if port >= EPHEMERAL_PORT_FLOOR {
                violations.push(format!(
                    "{rel}: references host endpoint port {port} (>= {EPHEMERAL_PORT_FLOOR})"
                ));
            }
        }
    }

    // (B) The harness's own host-facing port constants.
    for (index, port) in NODE_CQL_PORTS.iter().enumerate() {
        if *port >= EPHEMERAL_PORT_FLOOR {
            violations.push(format!(
                "docker_provision::NODE_CQL_PORTS[{index}] = {port} (>= {EPHEMERAL_PORT_FLOOR})"
            ));
        }
    }
    let admin_port = admin_port_of(DEFAULT_ADMIN_URL);
    match admin_port {
        Some(port) if port < EPHEMERAL_PORT_FLOOR => {}
        other => violations.push(format!(
            "chaos::DEFAULT_ADMIN_URL ({DEFAULT_ADMIN_URL}) host port {other:?} \
             is not below {EPHEMERAL_PORT_FLOOR}"
        )),
    }

    assert!(
        violations.is_empty(),
        "fixed host ports must stay BELOW {EPHEMERAL_PORT_FLOOR} (the kernel's ephemeral \
         range floor) or an outbound connection can already hold one as its source port and \
         `docker compose up` fails to bind it. Offending ports:\n  {}",
        violations.join("\n  ")
    );

    // (C) The harness constant and the compose must not drift: NODE_CQL_PORTS is
    // the host-side CQL port list the driver dials, so it must equal the compose's
    // published CQL host ports, in service order.
    let compose_path = root.join("ferrosa-jepsen/tests/docker/jepsen-cluster.yml");
    let compose = std::fs::read_to_string(&compose_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", compose_path.display()));
    let compose_cql_hosts: Vec<u16> = published_port_pairs(&compose)
        .into_iter()
        .filter(|(_, container)| *container == 9042)
        .map(|(host, _)| host)
        .collect();
    assert_eq!(
        compose_cql_hosts,
        NODE_CQL_PORTS.to_vec(),
        "docker_provision::NODE_CQL_PORTS must match the CQL host ports published by \
         jepsen-cluster.yml; the driver dials the constant, so a mismatch would dial a port \
         the cluster does not expose"
    );
}

/// Parse the host port out of an URL like
/// `http://localhost:29090/admin/membership-snapshot`.
fn admin_port_of(url: &str) -> Option<u16> {
    url.rsplit(':')
        .next()?
        .split('/')
        .next()?
        .parse::<u16>()
        .ok()
}
