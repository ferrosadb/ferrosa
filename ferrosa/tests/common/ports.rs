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
//! within a process, and still leases each PROCESS its own slice of the band,
//! because `cargo nextest run --tests` runs one process per test and several
//! node-spawning harnesses are in flight at once; a node that loses a port anyway
//! still fails loud.
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

/// Ports per slice. A fixture's widest case is a 5-node cluster taking 7 ports
/// per node, so 200 leaves room for several fixtures' worth inside one slice.
pub const SLICE_LEN: u16 = 200;

/// How many processes may hold a disjoint slice of the band at once.
///
/// The number that matters is how many node-spawning tests the runner has in
/// flight, not how many cores the machine has: `cargo nextest run --tests` runs
/// **one process per test** and schedules them across every core, so the six
/// harnesses in `ferrosa/tests/` can all be running at the same time, alongside
/// every other test in the workspace.
pub const SLICES: u16 = (BAND_LEN / SLICE_LEN as u32) as u16;

/// Directory holding one lock file per slice, under the platform's temp dir —
/// shared by every test process of the run, and unique to this machine.
const SLICE_LOCK_DIR: &str = "ferrosa-test-port-band";

/// The port this process starts its scan from: the first port of a slice it
/// holds exclusively for as long as it lives.
///
/// A per-process allocator is correct only while one process runs at a time, and
/// that is not how CI runs the suite. Measured on 2026-10-13: with every process
/// starting at [`BAND_START`], `drop_table_replica_process` and
/// `pg_public_keyspace_rf_process` ran concurrently and both handed their nodes
/// `20000`/`20001`/`20002` — one node died with `failed to bind 127.0.0.1:20002:
/// address in use` and the other spent 90 s waiting for a cluster whose third
/// node had died the same way. Two harness binaries reproduce it locally in 10 s.
///
/// The lease is an OS file lock, so a process that is killed — the normal end of
/// a panic — releases it in the kernel and leaves no stale claim for the next
/// run to trip over.
fn process_slice_start() -> u16 {
    static START: OnceLock<u16> = OnceLock::new();
    *START.get_or_init(|| {
        if let Some(index) = claim_slice_lease() {
            return BAND_START + index * SLICE_LEN;
        }
        // No lock could be taken — an unwritable temp dir, or a platform without
        // file locks. Key the slice on this process's own id instead of falling
        // back to `BAND_START`, so two processes still start from different
        // ports rather than from the same one.
        let slice = (std::process::id() as u32 % SLICES as u32) as u16;
        BAND_START + slice * SLICE_LEN
    })
}

/// Claims a slice by taking an exclusive lock on its file, and holds the lock —
/// and the file descriptor under it — for the life of the process.
fn claim_slice_lease() -> Option<u16> {
    let dir = std::env::temp_dir().join(SLICE_LOCK_DIR);
    std::fs::create_dir_all(&dir).ok()?;
    for index in 0..SLICES {
        let path = dir.join(format!("slice-{index:02}.lock"));
        let Ok(file) = std::fs::OpenOptions::new()
            .create(true)
            // The file's CONTENT is irrelevant — it is a lock, not a record — so
            // it must not be truncated: truncating on every claim would be a
            // write against a file another process may be holding.
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
        else {
            continue;
        };
        if file.try_lock().is_ok() {
            // Deliberately leaked: dropping the handle would release the lock.
            std::mem::forget(file);
            return Some(index);
        }
    }
    None
}

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

/// Hands out ports from this process's slice of [`BAND_START`]..=[`BAND_END`],
/// skipping any that this process already owns or that something is listening on.
///
/// One allocator per test; the process-wide reservation is shared by all of them,
/// so two tests running at once can never be given the same port — and
/// [`process_slice_start`] leases each process its own slice, so two *processes*
/// cannot either.
pub struct PortAllocator {
    next: u16,
    taken: HashSet<u16>,
}

impl PortAllocator {
    pub fn new() -> Self {
        Self::beginning_at(process_slice_start())
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
