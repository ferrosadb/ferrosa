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
    kernel_ephemeral_floor, port_is_free, reserved_in_process, PortAllocator, BAND_END, BAND_START,
};

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
