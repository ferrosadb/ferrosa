//! The port band the real-process node harnesses allocate from.
//!
//! One guard per property the harness needs, and each one is a property the OLD
//! `bind(":0")` allocator violated — the first of them is the red the fix was
//! written against (it failed with `64 of 64 ports ... inside the kernel's
//! ephemeral range (>= 49152)`, listing `50822..=50948`), so this file is not a
//! post-hoc description of the new code: it is the reproduction, moved to the
//! module that now owns the behaviour.
#![cfg(unix)]

mod common;

use std::collections::HashSet;

use common::ports::{
    kernel_ephemeral_floor, port_is_free, process_lease_state, reserved_in_process, PortAllocator,
    BAND_END, BAND_START, SLICES, SLICE_LEN,
};

/// Invariant 5 — capacity, and the reason it needs its own guard: both halves of
/// the band's arithmetic can be shrunk without breaking anything that runs on one
/// machine, and either shrink silently reintroduces the collision above. A slice
/// must hold the widest cluster a fixture stands up, and there must be a slice for
/// every node-spawning test the runner has in flight.
#[test]
fn the_band_holds_a_slice_per_concurrent_process_and_a_cluster_per_slice() {
    /// cql, web, internode, graph, bolt, postgres, sparql.
    const PORTS_PER_NODE: u16 = 7;
    /// The widest fixture in `ferrosa/tests/`.
    const WIDEST_FIXTURE_NODES: u16 = 5;
    /// `ferrosa/tests/` alone holds six real-process harnesses, and nextest runs
    /// them in parallel processes beside the rest of the workspace's tests.
    const CONCURRENT_HARNESSES: u16 = 8;

    // `black_box` keeps these from being folded at compile time: an assertion
    // whose value is known statically is denied by clippy, and this check has to
    // run — and be reported by name — the moment someone changes the band's
    // arithmetic, rather than becoming a build error nobody can attribute.
    let slice_len = std::hint::black_box(SLICE_LEN);
    let slices = std::hint::black_box(SLICES);

    assert!(
        slice_len >= PORTS_PER_NODE * WIDEST_FIXTURE_NODES,
        "a slice is {slice_len} ports, too narrow for a {WIDEST_FIXTURE_NODES}-node cluster \
         taking {PORTS_PER_NODE} ports per node: a fixture would scan out of its slice and onto a \
         port another process is using"
    );
    assert!(
        slices >= CONCURRENT_HARNESSES,
        "{slices} slices cannot cover {CONCURRENT_HARNESSES} node-spawning tests running at once, \
         so two processes would be leased overlapping ports"
    );
    assert!(
        BAND_START + slices * slice_len <= BAND_END + 1,
        "the {slices} slices are {slice_len} ports wide, which runs past BAND_END={BAND_END}"
    );
}

/// RED as written, against the pre-fix allocator: every candidate came from
/// `bind(":0")`, i.e. from inside the kernel's ephemeral range, so this failed
/// on all 64 ports and named the defect rather than a missing symbol.
#[test]
fn every_port_handed_to_a_node_is_below_the_kernel_ephemeral_floor() {
    let floor = kernel_ephemeral_floor();
    let mut alloc = PortAllocator::new();
    let ports: Vec<u16> = (0..64).map(|_| alloc.get()).collect();

    let outside: Vec<u16> = ports
        .iter()
        .copied()
        .filter(|p| *p < BAND_START || *p > BAND_END)
        .collect();
    assert!(
        outside.is_empty(),
        "{} of {} ports handed to real node processes are outside the harness band \
         {BAND_START}..={BAND_END}: {outside:?}",
        outside.len(),
        ports.len()
    );

    let in_range: Vec<u16> = ports.iter().copied().filter(|p| *p >= floor).collect();
    assert!(
        in_range.is_empty(),
        "{} of {} ports handed to real node processes are inside the kernel's \
         ephemeral range (>= {floor}), where the kernel can already hold one as an \
         outbound source port for anything else in the VM — this is how a node's \
         SPARQL listener lost its port in CI on 2026-10-13 and the harness read the \
         90 s readiness timeout as a product red. In-range ports: {in_range:?}",
        in_range.len(),
        ports.len()
    );

    let distinct: HashSet<u16> = ports.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        ports.len(),
        "the allocator handed the same port to two nodes: {ports:?}"
    );
}

#[test]
fn a_port_a_listener_already_holds_is_never_handed_to_a_node() {
    // Order-independent: pick a band port that this process has not already
    // been given and that nothing is listening on, so the squatter below — not
    // an earlier test's reservation — is what the allocator must skip.
    let port = (BAND_START..=BAND_END)
        .find(|p| !reserved_in_process(*p) && port_is_free(*p))
        .expect("a free, unreserved port in the band");
    let squatter = std::net::TcpListener::bind(("127.0.0.1", port)).expect("squat a band port");

    let mut alloc = PortAllocator::beginning_at(port);
    let handed: Vec<u16> = (0..8).map(|_| alloc.get()).collect();

    assert!(
        !handed.contains(&port),
        "port {port} is being served by another process, and the allocator handed \
         it to a node anyway (it would fail to bind and never become ready): {handed:?}"
    );
    drop(squatter);
}

/// RED as written, against the per-process allocator this band was introduced
/// with.
///
/// The earlier guards hold *within* one process and are all satisfied by an
/// allocator that begins every process at [`BAND_START`]. CI does not run one
/// process: `cargo nextest run --tests` runs **one process per test**, scheduled
/// across every core, so six node-spawning harnesses are routinely in flight at
/// once — and each one handed its nodes the same first ports (`web=20001`, and
/// `failed to bind 127.0.0.1:20002: address in use`, then a 90 s readiness
/// timeout reported as a product red). Reproduced locally in 10 s by running two
/// harness binaries at once: `graph HTTP server failed ... Address already in
/// use` on 20003 and the SPARQL listener on 20006.
///
/// The children are this same binary re-run with a filter, so the guard measures
/// the allocator instead of a stand-in for it.
#[test]
fn concurrent_test_processes_are_never_handed_the_same_port() {
    const MARKER: &str = "FERROSA_PORT_BAND_PROBE";
    let exe = std::env::current_exe().expect("the test binary's own path");

    // One child at a time, reading each child's port BEFORE the next starts: every
    // child then blocks on its stdin, so all four hold their leases AT ONCE.
    // Spawning all four and waiting for them to EXIT measures nothing — an exited
    // child has released its slice, so the next child is given that same slice
    // legitimately. That protocol mistake made this guard fail in CI with
    // `[20600, 20600, 20600, 20800]` — four LEASED slices, three of them released
    // and re-let — while every harness in the same job passed.
    let mut children: Vec<std::process::Child> = Vec::new();
    let mut handed = Vec::new();
    let mut states = Vec::new();
    for _ in 0..4 {
        let mut child = std::process::Command::new(&exe)
            .args(["--exact", "port_band_process_probe", "--nocapture"])
            .env(MARKER, "1")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("a probe process running this same binary");
        let mut reader = std::io::BufReader::new(child.stdout.take().expect("piped stdout"));
        // Scan past libtest's own banner: the child prints `running 1 test` and
        // friends first, so only the PROBE_PORT line identifies the port — and the
        // child blocks on its stdin right after printing it, holding its slice.
        let mut seen = Vec::new();
        let mut port = None;
        let mut state = String::new();
        for _ in 0..64 {
            let mut line = String::new();
            if std::io::BufRead::read_line(&mut reader, &mut line).unwrap_or(0) == 0 {
                break;
            }
            seen.push(line.trim().to_string());
            if let Some(rest) = line.trim().strip_prefix("PROBE_PORT=") {
                port = rest
                    .split_whitespace()
                    .next()
                    .and_then(|v| v.parse::<u16>().ok());
                state = line.trim().to_string();
                break;
            }
        }
        let port = port.unwrap_or_else(|| {
            panic!("a probe process printed no PROBE_PORT; its output was {seen:#?}")
        });
        handed.push(port);
        states.push(state);
        children.push(child);
    }

    // All four are alive and holding their slices; releasing them is now safe.
    for mut child in children {
        drop(child.stdin.take());
        let _ = child.wait();
    }

    let distinct: HashSet<u16> = handed.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        handed.len(),
        "two test PROCESSES alive at the same time were handed the same port, so their \
         nodes bind it seconds later and one of them dies with `Address already in \
         use`: {handed:?}\n  {states:#?}"
    );
}

/// The probe the guard above runs in its own process. Inert unless the parent
/// asks, so it costs nothing in an ordinary suite run.
#[test]
fn port_band_process_probe() {
    if std::env::var("FERROSA_PORT_BAND_PROBE").is_err() {
        return;
    }
    let mut alloc = PortAllocator::new();
    let port = alloc.get();
    println!("PROBE_PORT={port} {}", process_lease_state());
    // Hold the lease — and the port — until the parent closes this child's stdin.
    // Returning here instead would release the slice, and the sibling that follows
    // would be given it; that is the difference between a real collision and a
    // released-and-re-let slice, and the guard exists to tell them apart.
    let mut release = String::new();
    let _ = std::io::stdin().read_line(&mut release);
}

#[test]
fn two_tests_in_one_process_are_never_handed_the_same_port() {
    let mut first = PortAllocator::new();
    let mut second = PortAllocator::new();
    let mut all = Vec::new();
    for _ in 0..16 {
        all.push(first.get());
        all.push(second.get());
    }
    let distinct: HashSet<u16> = all.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        all.len(),
        "two allocations in one process were handed the same port, so their nodes \
         would fight over it: {all:?}"
    );
}
