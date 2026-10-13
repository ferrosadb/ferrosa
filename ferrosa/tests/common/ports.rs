//! Listener ports for the harnesses that spawn REAL `ferrosa` node processes.
//!
//! # Why a fixed band and not `bind("127.0.0.1:0")`
//!
//! The kernel hands ports out as **source** ports to every outbound connection,
//! and it draws them from its ephemeral range — `32768 60999` on Linux, `49152
//! 65535` on macOS. A *listening* port taken from that range is therefore
//! already spoken for whenever anything else in the VM opened a connection in
//! the window between the harness proving the port free and the child binding
//! it. That window is not small: a node binds its SPARQL listener at step 11 of
//! its startup, seconds after launch, and its PostgreSQL listener after that.
//!
//! Measured, 2026-10-13: a node in `drop_table_replica_process` lost its SPARQL
//! port to exactly this race in CI —
//! `ERROR ferrosa: SPARQL HTTP server failed e=Address already in use (os error
//! 98)` — stayed `503 waiting_for="listeners"` on `/readyz`, and the harness
//! reported its 90 s readiness timeout as a product red. Every one of the 64
//! ports the old `bind(":0")` allocator handed out on that host was inside the
//! range (`50822..=50948`). The same mechanism killed the PostgreSQL Jepsen CI
//! job, where docker could not publish its fixed host port:
//! `failed to bind host port for 0.0.0.0:49092 ... address already in use`.
//!
//! A port **below** the range can never be handed out as a source port, so the
//! race is removed rather than tolerated. The harness still proves every port it
//! hands over is free at that instant, still never issues the same port twice
//! within a process, and a node that loses one anyway still fails loud.
//!
//! The band is deliberately below both platforms' floors and clear of every
//! fixed port this repo uses (the dev cluster's 19042-19044/19090-19092/17474,
//! the compose examples' 9042/9090/8815, the Jepsen topologies' 29xxx).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};

/// The lower bound of the kernel's ephemeral range, if the platform will not
/// tell us. Both known floors are at or above this.
pub const EPHEMERAL_FLOOR_FALLBACK: u16 = 32_768;

/// First port of the band this harness hands to node processes.
pub const BAND_START: u16 = 20_000;
/// Last port of the band, inclusive. Kept below the 29xxx family the Jepsen
/// topologies publish so a locally running cluster cannot collide with a test.
pub const BAND_END: u16 = 27_999;

const BAND_LEN: u32 = (BAND_END as u32 - BAND_START as u32) + 1;

/// The kernel's ephemeral range floor: `net.ipv4.ip_local_port_range` on Linux,
/// `net.inet.ip.portrange.first` on macOS/BSD.
///
/// A listening port must be BELOW this to be safe on both platforms, so we read
/// the platform's own value rather than assuming one.
pub fn kernel_ephemeral_floor() -> u16 {
    if let Ok(text) = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range") {
        if let Some(first) = text.split_whitespace().next() {
            if let Ok(floor) = first.parse::<u16>() {
                return floor;
            }
        }
    }
    if let Ok(out) = std::process::Command::new("sysctl")
        .args(["-n", "net.inet.ip.portrange.first"])
        .output()
    {
        if let Ok(floor) = String::from_utf8_lossy(&out.stdout).trim().parse::<u16>() {
            return floor;
        }
    }
    EPHEMERAL_FLOOR_FALLBACK
}

/// Whether the harness can bind `port` right now.
///
/// A successful bind is the proof: `std`'s `TcpListener::bind` sets
/// `SO_REUSEADDR`, which does NOT let a second listener share a port that is
/// live (it only permits one over a `TIME_WAIT`), so a bind that succeeds means
/// nothing is serving that address — including a wildcard listener, which the
/// kernel reports as a conflict for the loopback address too. The bind is
/// dropped immediately; [`PortAllocator`] then records the port as spoken for
/// by this process, so it is not re-issued to another test thread.
pub fn port_is_free(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    match std::net::TcpListener::bind(addr) {
        Ok(listener) => {
            drop(listener);
            true
        }
        Err(_) => false,
    }
}

/// Whether some other allocation in THIS process already owns `port`.
pub fn reserved_in_process(port: u16) -> bool {
    process_reserved().lock().unwrap().contains(&port)
}

/// Every port this process has handed out, across all its test threads.
///
/// A per-test set alone lets two tests draw the SAME port: the allocator proves
/// a port free at that instant and both threads then hand it to their own node.
/// The sibling never-ready fixture is where that showed — its node's PostgreSQL
/// listener reported `Address already in use` under load, because a port another
/// test had been handed was still bound (t_4e9a3e61).
fn process_reserved() -> &'static Mutex<HashSet<u16>> {
    static RESERVED: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
    RESERVED.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Hands out ports from [`BAND_START`]..=[`BAND_END`], skipping any that this
/// process already owns or that something is listening on.
///
/// One allocator per test; the process-wide reservation is shared by all of
/// them, so two tests running at once can never be given the same port.
pub struct PortAllocator {
    next: u16,
    taken: HashSet<u16>,
}

impl PortAllocator {
    pub fn new() -> Self {
        Self::beginning_at(BAND_START)
    }

    /// An allocator whose scan starts at `port`. A test seam: it lets a test
    /// hand the first attempt a port it already holds, so a re-draw can be
    /// proved rather than asserted.
    pub fn beginning_at(port: u16) -> Self {
        assert!(
            (BAND_START..=BAND_END).contains(&port),
            "port {port} is outside the test band {BAND_START}..={BAND_END}"
        );
        Self {
            next: port,
            taken: HashSet::new(),
        }
    }

    /// A band port nothing is listening on, reserved for this process.
    pub fn get(&mut self) -> u16 {
        assert!(
            BAND_END < kernel_ephemeral_floor(),
            "the test band {BAND_START}..={BAND_END} must sit entirely below the \
             kernel's ephemeral floor ({}); a listening port inside that range can \
             be handed to an outbound connection before the node binds it",
            kernel_ephemeral_floor()
        );
        for _ in 0..BAND_LEN {
            let port = self.next;
            self.next = if port >= BAND_END {
                BAND_START
            } else {
                port + 1
            };
            if self.taken.contains(&port) {
                continue;
            }
            if !port_is_free(port) {
                continue;
            }
            if !process_reserved().lock().unwrap().insert(port) {
                continue;
            }
            self.taken.insert(port);
            return port;
        }
        panic!(
            "no free port in the {BAND_START}-{BAND_END} band: all {BAND_LEN} \
             candidates are held by this process or by another listener. The band \
             is below the kernel's ephemeral floor on purpose — something is \
             squatting a wide range of it."
        );
    }
}

impl Default for PortAllocator {
    fn default() -> Self {
        Self::new()
    }
}
