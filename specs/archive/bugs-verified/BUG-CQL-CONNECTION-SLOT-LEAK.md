# BUG: CQL server leaks per-IP connection slots on client death

**Status:** RESOLVED — commit `5063ca6` (IpSlotGuard RAII + TCP keepalive 30s/10s)

## Symptom

After killing MCP client processes with `kill`, the ferrosa CQL server continues rejecting new connections from the same IP with:

```
WARN ferrosa_cql::server: per-IP limit reached for 10.89.0.128, rejecting
```

Even though no client processes are running, the server still counts the dead connections against the per-IP limit (default 64). New clients cannot connect until the server is restarted.

## Reproduction

1. Start a 3-node ferrosa cluster
2. Connect an MCP client with 3 contact points (opens ~6 connections)
3. Open additional clients (e.g., 3 MCP processes from `/mcp` reconnects)
4. Kill all client processes: `pkill -f ferrosa-memory-mcp`
5. Wait 30+ seconds
6. Start a new client → rejected with "per-IP limit reached"

In our case, 3 MCP processes accumulated 310 connections. After killing them, all 310 slots remained allocated in `IpConnectionTracker`, permanently blocking the IP.

## Root cause

`ferrosa-cql/src/server.rs:257-297` — the connection handler task:

```rust
tokio::spawn(async move {
    // handle_connection runs until the client disconnects
    handle_connection(stream, peer, ...).await;
    ip_tracker.release(peer_ip);      // line 295 — only runs when handle_connection returns
    active.fetch_sub(1, Ordering::Relaxed);
});
```

`ip_tracker.release()` only runs when `handle_connection()` returns. If the client is killed:

1. The TCP socket enters a half-open state (client side is dead, server side doesn't know)
2. `handle_connection` blocks on `framed.next().await` reading from the dead socket
3. TCP keepalive (if enabled) may eventually detect the dead peer, but the default OS timeout is **2 hours** (macOS) or **2+ hours** (Linux `tcp_keepalive_time`)
4. During this window, the connection slot stays acquired in `IpConnectionTracker`

With enough dead connections, the per-IP limit is exhausted and new connections are permanently rejected.

## Impact

- **Severe in development**: frequent process restarts (Ctrl+C, kill, crash) accumulate dead slots
- **Severe in production**: client crashes or network partitions leak slots until the server is restarted
- **Amplified by inter-node traffic**: cluster nodes also use CQL connections to each other; if a node restarts, its old connection slots on the peer leak

## Fix options

### Option 1: TCP keepalive with aggressive timeouts (recommended)

Set TCP keepalive on accepted sockets before passing to `handle_connection`:

```rust
// On the TcpStream, before spawning the handler:
stream.set_keepalive(Some(Duration::from_secs(30)))?;  // probe after 30s idle
// Or use socket2 for full control:
// keepalive_time: 30s, keepalive_interval: 10s, keepalive_count: 3
// → dead connection detected in ~60s
```

This causes the OS to send TCP keepalive probes. When the dead peer doesn't respond after 3 probes, the socket returns an error, `handle_connection` exits, and `release()` runs.

### Option 2: Read timeout on the framed codec

Add a timeout wrapper around `framed.next()`:

```rust
match tokio::time::timeout(Duration::from_secs(300), framed.next()).await {
    Ok(Some(Ok(frame))) => { /* process */ }
    Ok(Some(Err(e))) => { /* codec error */ break; }
    Ok(None) => break,           // clean close
    Err(_) => break,             // 5-minute idle timeout → close
}
```

### Option 3: Periodic slot cleanup

Add a background task that periodically checks `IpConnectionTracker` entries against actual open file descriptors or connection state. More complex and less reliable than options 1-2.

### Option 4: Guard-based release (defense in depth)

Wrap the IP slot in a RAII guard so it's released on drop, even if the task panics:

```rust
struct IpSlotGuard {
    tracker: Arc<IpConnectionTracker>,
    ip: IpAddr,
}

impl Drop for IpSlotGuard {
    fn drop(&mut self) {
        self.tracker.release(self.ip);
    }
}
```

This doesn't fix the half-open problem but prevents leaks from panics.

**Recommendation**: Combine options 1 + 4. TCP keepalive handles dead peers; RAII guard handles panics.

## Files

- `ferrosa-cql/src/server.rs:59-94` — `IpConnectionTracker` (acquire/release)
- `ferrosa-cql/src/server.rs:257-297` — spawned connection handler (release at line 295)
- `ferrosa-cql/src/connection.rs` — `handle_connection` (blocks on socket read)

## Related

- ferrosa-memory MCP server now bounds its connection pool to 2 local + 1 remote per node (max 9 total) to reduce pressure on the per-IP limit. But the leak still matters for any client that crashes.
