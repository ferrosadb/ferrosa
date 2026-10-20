//! Bounded-latency `host:port` resolution for the `system.peers` read path.
//!
//! `ClusterState::peers()` is synchronous and runs on the CQL request runtime:
//! every driver's control connection queries `system.peers` while it connects.
//! Resolving a peer's hostname there with `ToSocketAddrs` is a blocking
//! `getaddrinfo` on a runtime worker. For a peer that is down that call can
//! take the resolver's full timeout -- 5 s on the GitHub runner for a stopped
//! compose container's name -- and freezes the CQL runtime for that long
//! (`ferrosa_sched_runtime_stall_*`). A client connecting to a SURVIVING node
//! then times out before it can issue a single statement, which is what failed
//! the nightly docker smoke's "QUORUM write with 1 of 3 nodes down" every night.
//!
//! [`NonBlockingAddrCache::get`] never runs a lookup on the caller's thread:
//!
//! - a literal `ip:port` is parsed in place;
//! - a hostname is answered from the last successful resolution, kept while
//!   the host is down so a dead peer keeps its last address (what Cassandra
//!   shows for a down node); a refresh is due every `refresh_after` and runs on
//!   its own thread, one at a time per hostname;
//! - a hostname that has NEVER resolved waits for its first lookup for at most
//!   `first_resolve_wait`, so a healthy name (`localhost:19043`) is answered on
//!   the first query, and a hanging one costs a bounded wait instead of the
//!   resolver timeout. Only the call that starts the lookup waits.
//!
//! A failed refresh is logged at WARN and keeps the last good address.

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Resolves `host:port` to socket addresses. Blocking; only ever called on a
/// dedicated background thread, never on the caller of [`NonBlockingAddrCache::get`].
pub type HostLookup = Arc<dyn Fn(&str) -> std::io::Result<Vec<SocketAddr>> + Send + Sync>;

/// How long a resolution is served before a background refresh is due.
pub const DEFAULT_REFRESH_AFTER: Duration = Duration::from_secs(10);

/// The longest a caller waits for a hostname's FIRST resolution. Below the
/// CQL runtime's 300 ms stall threshold and far below a driver's 5 s connect
/// timeout.
pub const DEFAULT_FIRST_RESOLVE_WAIT: Duration = Duration::from_millis(200);

/// Upper bound on distinct hostnames tracked; one per ring member in practice.
const MAX_ENTRIES: usize = 4096;

#[derive(Default)]
struct Entry {
    addr: Option<SocketAddr>,
    /// When the last resolution was STARTED. Refreshes are spaced by
    /// `refresh_after` whatever their outcome, so a down host costs at most
    /// one resolver thread per interval.
    attempted_at: Option<Instant>,
    in_flight: bool,
}

/// See the module docs.
#[derive(Clone)]
pub struct NonBlockingAddrCache {
    entries: Arc<Mutex<HashMap<String, Entry>>>,
    lookup: HostLookup,
    refresh_after: Duration,
    first_resolve_wait: Duration,
}

impl Default for NonBlockingAddrCache {
    fn default() -> Self {
        Self::with_lookup(
            Arc::new(|host_port: &str| host_port.to_socket_addrs().map(Iterator::collect)),
            DEFAULT_REFRESH_AFTER,
            DEFAULT_FIRST_RESOLVE_WAIT,
        )
    }
}

impl NonBlockingAddrCache {
    /// A cache resolving through `lookup`; see the module docs for the timings.
    pub fn with_lookup(
        lookup: HostLookup,
        refresh_after: Duration,
        first_resolve_wait: Duration,
    ) -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            lookup,
            refresh_after,
            first_resolve_wait,
        }
    }

    /// The address for `host_port`, waiting at most `first_resolve_wait`.
    ///
    /// `None`: the input has no numeric port, or the hostname has not resolved
    /// yet (its lookup keeps running in the background and a later call sees
    /// the result).
    pub fn get(&self, host_port: &str) -> Option<SocketAddr> {
        let host_port = host_port.trim();
        if let Ok(addr) = host_port.parse::<SocketAddr>() {
            return Some(addr);
        }
        // Same contract as the `ToSocketAddrs` call this replaces: no port, no address.
        let (_, port) = host_port.rsplit_once(':')?;
        port.parse::<u16>().ok()?;

        let first_lookup = {
            let mut entries = self.entries.lock();
            if !entries.contains_key(host_port) && entries.len() >= MAX_ENTRIES {
                tracing::warn!(
                    host = host_port,
                    cap = MAX_ENTRIES,
                    "peer address cache full; hostname not resolved"
                );
                return None;
            }
            let entry = entries.entry(host_port.to_owned()).or_default();
            let due = entry
                .attempted_at
                .is_none_or(|at| at.elapsed() >= self.refresh_after);
            if !due || entry.in_flight {
                return entry.addr;
            }
            entry.attempted_at = Some(Instant::now());
            let Some(done) = self.spawn_refresh(host_port) else {
                return entry.addr;
            };
            entry.in_flight = true;
            if entry.addr.is_some() {
                return entry.addr;
            }
            done
        };
        // Lock released: the resolver thread needs it to record its result.
        if first_lookup.recv_timeout(self.first_resolve_wait).is_err() {
            tracing::warn!(
                host = host_port,
                waited_ms = self.first_resolve_wait.as_millis() as u64,
                "peer hostname did not resolve in time; resolution continues in the background"
            );
        }
        self.entries.lock().get(host_port).and_then(|e| e.addr)
    }

    /// Start one background resolution of `host_port`. The receiver fires once
    /// its result is recorded. `None` if no thread could be spawned (logged;
    /// the next due call tries again).
    fn spawn_refresh(&self, host_port: &str) -> Option<mpsc::Receiver<()>> {
        let entries = Arc::clone(&self.entries);
        let lookup = Arc::clone(&self.lookup);
        let owned = host_port.to_owned();
        let (done_tx, done_rx) = mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("peer-addr-dns".to_owned())
            .spawn(move || {
                let result = lookup(&owned);
                record_resolution(&entries, &owned, result);
                // The waiter may have timed out and gone; nothing to tell then.
                let _ = done_tx.send(());
            });
        match spawned {
            Ok(_) => Some(done_rx),
            Err(e) => {
                tracing::warn!(host = host_port, error = %e, "could not spawn peer address resolver thread");
                None
            }
        }
    }
}

fn record_resolution(
    entries: &Mutex<HashMap<String, Entry>>,
    host_port: &str,
    result: std::io::Result<Vec<SocketAddr>>,
) {
    let mut entries = entries.lock();
    let Some(entry) = entries.get_mut(host_port) else {
        return;
    };
    entry.in_flight = false;
    match result.map(|addrs| addrs.into_iter().next()) {
        Ok(Some(addr)) => entry.addr = Some(addr),
        Ok(None) => tracing::warn!(
            host = host_port,
            last_known = ?entry.addr,
            "peer hostname resolved to no address; keeping last known address"
        ),
        Err(e) => tracing::warn!(
            host = host_port,
            error = %e,
            last_known = ?entry.addr,
            "peer hostname did not resolve; keeping last known address"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;

    const WAIT: Duration = Duration::from_secs(10);

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    /// Poll `cache.get(host)` until it yields an address, bounded by `WAIT`.
    fn wait_for(cache: &NonBlockingAddrCache, host: &str) -> SocketAddr {
        let deadline = Instant::now() + WAIT;
        while Instant::now() < deadline {
            if let Some(a) = cache.get(host) {
                return a;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("{host} never resolved within {WAIT:?}");
    }

    #[test]
    fn literal_socket_addr_is_parsed_without_a_lookup() {
        let cache = NonBlockingAddrCache::with_lookup(
            Arc::new(|h: &str| panic!("literal {h} must not be looked up")),
            DEFAULT_REFRESH_AFTER,
            DEFAULT_FIRST_RESOLVE_WAIT,
        );
        assert_eq!(cache.get("172.18.0.5:7000"), Some(addr("172.18.0.5:7000")));
        assert_eq!(cache.get(" [::1]:9042 "), Some(addr("[::1]:9042")));
    }

    /// The bug: a resolver that hangs (a stopped container's name on the CI
    /// runner takes 5 s) must never hang the caller.
    #[test]
    fn get_never_waits_for_a_hanging_lookup() {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        let calls = Arc::new(AtomicUsize::new(0));
        let lookup_calls = Arc::clone(&calls);
        let cache = NonBlockingAddrCache::with_lookup(
            Arc::new(move |_: &str| {
                lookup_calls.fetch_add(1, Ordering::SeqCst);
                release_rx
                    .lock()
                    .recv_timeout(WAIT)
                    .expect("test released the lookup");
                Ok(vec![addr("172.18.0.5:7000")])
            }),
            DEFAULT_REFRESH_AFTER,
            Duration::from_millis(50),
        );

        let started = Instant::now();
        assert_eq!(
            cache.get("node3:7000"),
            None,
            "a first lookup is not awaited"
        );
        assert_eq!(cache.get("node3:7000"), None);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "get blocked for {:?} on a hanging resolver",
            started.elapsed()
        );

        release_tx.send(()).unwrap();
        assert_eq!(wait_for(&cache, "node3:7000"), addr("172.18.0.5:7000"));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "one lookup per due refresh"
        );
    }

    #[test]
    fn failed_refresh_keeps_the_last_known_address() {
        let calls = Arc::new(AtomicUsize::new(0));
        let lookup_calls = Arc::clone(&calls);
        let cache = NonBlockingAddrCache::with_lookup(
            Arc::new(
                move |_: &str| match lookup_calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Ok(vec![addr("172.18.0.5:7000")]),
                    _ => Err(std::io::Error::other("Name or service not known")),
                },
            ),
            Duration::ZERO,
            WAIT,
        );
        assert_eq!(wait_for(&cache, "node3:7000"), addr("172.18.0.5:7000"));

        let deadline = Instant::now() + WAIT;
        while calls.load(Ordering::SeqCst) < 3 && Instant::now() < deadline {
            assert_eq!(cache.get("node3:7000"), Some(addr("172.18.0.5:7000")));
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(calls.load(Ordering::SeqCst) >= 3, "refreshes kept running");
        assert_eq!(cache.get("node3:7000"), Some(addr("172.18.0.5:7000")));
    }

    /// A healthy hostname (a dev cluster's `localhost:19043` broadcast) is
    /// answered on the very first query, not one query late.
    #[test]
    fn first_resolution_is_answered_by_the_call_that_starts_it() {
        let cache = NonBlockingAddrCache::with_lookup(
            Arc::new(|_: &str| Ok(vec![addr("127.0.0.1:19043")])),
            DEFAULT_REFRESH_AFTER,
            WAIT,
        );
        assert_eq!(cache.get("localhost:19043"), Some(addr("127.0.0.1:19043")));
    }

    #[test]
    fn input_without_a_numeric_port_is_not_looked_up() {
        let cache = NonBlockingAddrCache::with_lookup(
            Arc::new(|h: &str| panic!("{h} has no port and must not be looked up")),
            DEFAULT_REFRESH_AFTER,
            WAIT,
        );
        assert_eq!(cache.get("node3"), None);
        assert_eq!(cache.get("node3:http"), None);
        assert_eq!(cache.get(""), None);
    }
}
