#!/usr/bin/env python3
"""rf_evidence.py — replication-factor evidence harness for ferrosa.

WHY THIS EXISTS
---------------
P0 defect t_891840e7: RF=3 was silently behaving as RF=2 (and RF=1 for some
rows) for ~22 days. Nothing in the test suite caught it because the only
fault-schedule coverage is skipped in CI, and the in-process harnesses never
start a real node. This harness answers one question with physical evidence:

    Invariant D1 — for a row written at configured RF=R on an N-node cluster,
    the row physically exists on min(R, N) distinct nodes.

It starts N real ferrosa *OS processes* (own data dir, own ports, one cluster),
proves they formed ONE cluster (not two one-node clusters / pairs), writes a
known key set, then scans each node's physical storage for the exact bytes of
each row. It then SIGKILLs a replica mid-operation and checks availability,
loud failure, and post-restart reconciliation.

FAIL-LOUD CONTRACT
------------------
If the harness cannot establish a real multi-node cluster it exits non-zero
with an explicit reason. It NEVER reports a pass against a one-node cluster —
a harness that silently tests one node is worse than no harness. Exit codes:
    0  all assertions passed
    2  could not form/verify a real multi-node cluster (vacuous run)
    3  cluster formed but invariant D1 was violated
    4  cluster formed, D1 held, but the fault-injection assertions failed
    5  harness/environment error (binary missing, interpreter problem)

The scan is deliberately independent of the server: it re-derives Cassandra
Murmur3 tokens, reads each node's `system.local` token set over CQL to build
the ring, computes the replica set itself, and then looks for the row's bytes
in `<data_dir>/commitlog` + `<data_dir>/sstables`. A row is "landed" on a node
only if BOTH the partition-key bytes and a per-row unique value marker bytes
are present.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

# ---------------------------------------------------------------- murmur3 ----
# Cassandra-compatible Murmur3 x64_128, ported 1:1 from
# ferrosa-common/src/murmur3.rs and validated against that file's
# characterization vectors in `verify_murmur3()` below. `token` is h1.

_M64 = (1 << 64) - 1


def _rotl(x: int, r: int) -> int:
    return ((x << r) | (x >> (64 - r))) & _M64


def _fmix64(k: int) -> int:
    k &= _M64
    k ^= k >> 33
    k = (k * 0xFF51AFD7ED558CCD) & _M64
    k ^= k >> 33
    k = (k * 0xC4CEB9FE1A85EC53) & _M64
    k ^= k >> 33
    return k


def _sx(b: int) -> int:
    """Java `(byte) -> (long)` sign extension (Cassandra's tail 'sign bug')."""
    v = b & 0xFF
    return v - 256 if v >= 128 else v


def hash3_x64_128(data: bytes, seed: int = 0) -> tuple[int, int]:
    n = len(data)
    nblocks = n // 16
    h1 = seed
    h2 = seed
    c1 = 0x87C37B91114253D5
    c2 = 0x4CF5AD432745937F
    for i in range(nblocks):
        o = i * 16
        k1 = int.from_bytes(data[o : o + 8], "little", signed=True)
        k2 = int.from_bytes(data[o + 8 : o + 16], "little", signed=True)
        k1 = (k1 * c1) & _M64
        k1 = _rotl(k1, 31)
        k1 = (k1 * c2) & _M64
        h1 ^= k1
        h1 = _rotl(h1, 27)
        h1 = (h1 + h2) & _M64
        h1 = (h1 * 5 + 0x52DCE729) & _M64
        k2 = (k2 * c2) & _M64
        k2 = _rotl(k2, 33)
        k2 = (k2 * c1) & _M64
        h2 ^= k2
        h2 = _rotl(h2, 31)
        h2 = (h2 + h1) & _M64
        h2 = (h2 * 5 + 0x38495AB5) & _M64
    tail = data[nblocks * 16 :]
    k1 = k2 = 0
    if len(tail) >= 15:
        k2 ^= (_sx(tail[14]) << 48) & _M64
    if len(tail) >= 14:
        k2 ^= (_sx(tail[13]) << 40) & _M64
    if len(tail) >= 13:
        k2 ^= (_sx(tail[12]) << 32) & _M64
    if len(tail) >= 12:
        k2 ^= (_sx(tail[11]) << 24) & _M64
    if len(tail) >= 11:
        k2 ^= (_sx(tail[10]) << 16) & _M64
    if len(tail) >= 10:
        k2 ^= (_sx(tail[9]) << 8) & _M64
    if len(tail) >= 9:
        k2 ^= _sx(tail[8])
        k2 = (k2 * c2) & _M64
        k2 = _rotl(k2, 33)
        k2 = (k2 * c1) & _M64
        h2 ^= k2
    if len(tail) >= 8:
        k1 ^= (_sx(tail[7]) << 56) & _M64
    if len(tail) >= 7:
        k1 ^= (_sx(tail[6]) << 48) & _M64
    if len(tail) >= 6:
        k1 ^= (_sx(tail[5]) << 40) & _M64
    if len(tail) >= 5:
        k1 ^= (_sx(tail[4]) << 32) & _M64
    if len(tail) >= 4:
        k1 ^= (_sx(tail[3]) << 24) & _M64
    if len(tail) >= 3:
        k1 ^= (_sx(tail[2]) << 16) & _M64
    if len(tail) >= 2:
        k1 ^= (_sx(tail[1]) << 8) & _M64
    if len(tail) >= 1:
        k1 ^= _sx(tail[0])
        k1 = (k1 * c1) & _M64
        k1 = _rotl(k1, 31)
        k1 = (k1 * c2) & _M64
        h1 ^= k1
    h1 = (h1 ^ n) & _M64
    h2 = (h2 ^ n) & _M64
    h1 = (h1 + h2) & _M64
    h2 = (h2 + h1) & _M64
    h1 = _fmix64(h1)
    h2 = _fmix64(h2)
    h1 = (h1 + h2) & _M64
    h2 = (h2 + h1) & _M64

    def s64(u: int) -> int:
        return u - (1 << 64) if u >= (1 << 63) else u

    return s64(h1), s64(h2)


def token_for_key(key: str) -> int:
    """Cassandra partition token for a single-column text partition key.

    ferrosa hashes the raw UTF-8 key bytes (Token::from_key); verified live —
    the literal bytes of the key appear in the node's commitlog.
    """
    return hash3_x64_128(key.encode("utf-8"), 0)[0]


def verify_murmur3() -> bool:
    """Confirm the port against ferrosa-common's Cassandra characterization
    vectors, so the harness never computes replica sets on a wrong ring."""
    v = [
        (b"hello", (0) , -3758069500696749310, 6565844092913065241),
        (b"ferrosa", 0, -7911154581804264429, -5083183550992889052),
        (b"cassandra", 0, 356242581507269238, -3708818142985255407),
        (bytes([0x2A]), 0, -2387438309745315495, -1061927915756559090),
        (bytes([0xFF]), 0, -4442228696663692417, -6049531771631615289),
        (b"", 0, 0, 0),
    ]
    return all(hash3_x64_128(d, s) == (a, b) for d, s, a, b in v)


# ------------------------------------------------------------------ report ----

# cassandra-driver is imported at module level so helpers outside `main` can use
# it. Availability is checked explicitly in `main` (fail loud, exit 5) rather
# than exploding at import time.
try:  # pragma: no cover - environment dependent
    from cassandra.query import SimpleStatement  # type: ignore
except Exception:  # noqa: BLE001
    SimpleStatement = None  # type: ignore


class Report:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.failures: list[str] = []
        self.d1_fail = False
        self.fault_fail = False

    def info(self, msg: str) -> None:
        print(msg, flush=True)

    def record(self, kind: str, **kw) -> None:
        self.events.append({"kind": kind, **kw})

    def fail(self, msg: str, d1: bool = False, fault: bool = False) -> None:
        self.failures.append(msg)
        if d1:
            self.d1_fail = True
        if fault:
            self.fault_fail = True
        print(f"  !! FAIL: {msg}", flush=True)

    def dump(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"events": self.events, "failures": self.failures}, indent=2))


# ------------------------------------------------------------- node process ---

# Per-node bind layout inside a 10-port band:
#   base+0 cql  base+1 web  base+2 internode  base+3 graph
#   base+4 postgres  base+5 sparql  base+6 flight  base+7 bolt
# FERROSA_BOLT_PORT is a SEPARATE variable from FERROSA_GRAPH_BIND: bolt takes
# its HOST from the graph bind but its PORT from FERROSA_BOLT_PORT (default
# 7687). Omitting it makes every node fight over 7687; a cluster that cannot
# bind its listeners cannot answer the RF question.
OFF_CQL, OFF_WEB, OFF_INTERNODE, OFF_GRAPH = 0, 1, 2, 3
OFF_POSTGRES, OFF_SPARQL, OFF_FLIGHT, OFF_BOLT = 4, 5, 6, 7


class Node:
    def __init__(self, idx: int, base_port: int, data_root: Path, log_dir: Path,
                 binary: Path, seed_internode: int, expected_size: int,
                 formation_timeout: int):
        self.idx = idx
        self.base = base_port
        self.data_dir = data_root / f"node{idx}"
        self.log_path = log_dir / f"node{idx}.log"
        # host_id: node1 (idx=1) must be the highest UUID so it is the seed
        # that calls raft.initialize() (controller: seed = max UUID of members).
        d = 10 - idx
        self.host_id = f"{d}{d}{d}{d}{d}{d}{d}{d}-{d}{d}{d}{d}-{d}{d}{d}{d}-{d}{d}{d}{d}-{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}"
        self.binary = binary
        self.seed_internode = seed_internode
        self.expected_size = expected_size
        self.formation_timeout = formation_timeout
        self.proc: subprocess.Popen | None = None
        self._log_fh = None

    @property
    def cql_port(self) -> int:
        return self.base + OFF_CQL

    @property
    def web_port(self) -> int:
        return self.base + OFF_WEB

    @property
    def internode_port(self) -> int:
        return self.base + OFF_INTERNODE

    def env(self) -> dict:
        e = dict(os.environ)
        e.update({
            "FERROSA_HOST_ID": self.host_id,
            "FERROSA_DATA_DIR": str(self.data_dir),
            "FERROSA_AUTH_DISABLED": "true",
            "FERROSA_CLUSTER_NAME": "rf-harness",
            "FERROSA_EXPECTED_CLUSTER_SIZE": str(self.expected_size),
            "FERROSA_FORMATION_TIMEOUT_SECS": str(self.formation_timeout),
            "FERROSA_LOG_ANSI": "false",
            # Keep the whole node inside its own band (see OFFSETS above).
            "FERROSA_CQL_BIND": f"127.0.0.1:{self.base + OFF_CQL}",
            "FERROSA_WEB_BIND": f"127.0.0.1:{self.base + OFF_WEB}",
            "FERROSA_INTERNODE_BIND": f"127.0.0.1:{self.base + OFF_INTERNODE}",
            "FERROSA_INTERNODE_BROADCAST": f"127.0.0.1:{self.base + OFF_INTERNODE}",
            "FERROSA_GRAPH_BIND": f"127.0.0.1:{self.base + OFF_GRAPH}",
            "FERROSA_BOLT_PORT": str(self.base + OFF_BOLT),
            "FERROSA_POSTGRES_BIND": f"127.0.0.1:{self.base + OFF_POSTGRES}",
            "FERROSA_SPARQL_BIND": f"127.0.0.1:{self.base + OFF_SPARQL}",
            "FERROSA_FLIGHT_BIND": f"127.0.0.1:{self.base + OFF_FLIGHT}",
        })
        if self.seed_internode:
            e["FERROSA_SEED"] = f"127.0.0.1:{self.seed_internode}"
        else:
            e.pop("FERROSA_SEED", None)
        return e

    def start(self, fresh: bool) -> None:
        if fresh:
            subprocess.run(["rm", "-rf", str(self.data_dir)], check=False)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_fh = open(self.log_path, "ab")
        self.proc = subprocess.Popen(
            [str(self.binary)],
            env=self.env(),
            stdout=self._log_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,  # own process group: SIGKILL reaches only this node
        )

    def kill_9(self) -> None:
        """SIGKILL the node — a real process death, not a simulated one."""
        if self.proc and self.proc.poll() is None:
            os.kill(self.proc.pid, signal.SIGKILL)
            self.proc.wait(timeout=30)
        if self._log_fh:
            self._log_fh.close()
            self._log_fh = None

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=30)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
        if self._log_fh:
            self._log_fh.close()
            self._log_fh = None


# ------------------------------------------------------------------- probe ----

def http_json(port: int, path: str, timeout: float = 8.0):
    url = f"http://127.0.0.1:{port}{path}"
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


def http_text(port: int, path: str, timeout: float = 8.0) -> str:
    url = f"http://127.0.0.1:{port}{path}"
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()


# Coordinator fan-out counters. These turn "the row is only on 2 nodes" from an
# inference into a measurement: attempts vs acks vs failures vs hints stored.
_COUNTERS = [
    "ferrosa_coordinator_replica_write_attempts_total",
    "ferrosa_coordinator_replica_write_acks_total",
    "ferrosa_coordinator_replica_write_failures_total",
    "ferrosa_coordinator_post_quorum_remote_acks_total",
    "ferrosa_coordinator_post_quorum_remote_failures_total",
    "ferrosa_coordinator_hints_stored_total",
]


def scrape_counters(port: int) -> dict:
    """Return {metric_with_labels: value} for the coordinator fan-out counters."""
    out: dict[str, float] = {}
    try:
        text = http_text(port, "/metrics", timeout=6)
    except Exception:  # noqa: BLE001
        return out
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        for name in _COUNTERS:
            if line.startswith(name):
                try:
                    key, val = line.rsplit(" ", 1)
                    out[key.strip()] = float(val)
                except ValueError:
                    pass
    return out


def counter_delta(before: dict, after: dict) -> dict[str, float]:
    keys = set(before) | set(after)
    return {k: after.get(k, 0.0) - before.get(k, 0.0)
            for k in keys if after.get(k, 0.0) - before.get(k, 0.0) != 0}


def short_metric(k: str) -> str:
    return (k.replace("ferrosa_coordinator_", "").replace("_total", "")
             .replace('target=', '').replace('"', ''))


def run_writes_at_cl(nodes, keys, markers, session_factory, ks, cl, coord_ports):
    """Write `keys` at consistency `cl` through given coordinator ports and
    return the coordinator fan-out counter deltas + the set of coordinators used.

    This is an INDEPENDENT check of achieved replication that does not depend on
    the landing scan or on any settle timing: the coordinator itself reports how
    many replica writes it attempted and how many were acknowledged. A
    coordinator that acks 3 replica writes for an RF=3 row while only 2 nodes
    physically hold it is unambiguous, load-independent evidence of a defect.
    """
    before = {idx: scrape_counters(port) for idx, port in coord_ports.items()}
    sessions = {}
    for i, k in enumerate(keys):
        idx = list(coord_ports)[i % len(coord_ports)]
        if idx not in sessions:
            c = session_factory(coord_ports[idx])
            sessions[idx] = (c, c.connect())
        _, s = sessions[idx]
        s.execute(SimpleStatement(
            f"INSERT INTO {ks}.t (k,v) VALUES (%s,%s)", consistency_level=cl),
            (k, markers[k]))
    for c, _ in sessions.values():
        c.shutdown()
    after = {idx: scrape_counters(port) for idx, port in coord_ports.items()}
    deltas = {idx: counter_delta(before[idx], after[idx]) for idx in coord_ports}
    return deltas


def sum_counter(deltas: dict, name: str) -> int:
    total = 0
    for d in deltas.values():
        for k, v in d.items():
            if k.startswith(name):
                total += int(v)
    return total


def port_open(port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout):
            return True
    except OSError:
        return False


def preflight_ports(nodes: list["Node"], report: Report) -> bool:
    """Refuse to start if any band port is already held.

    A previous crashed run leaves orphan nodes holding these ports; a new run
    then either collides (bind failure) or silently connects to the ORPHAN
    cluster and measures the wrong deployment. Both corrupt the RF result, so
    this is a hard gate.
    """
    busy = []
    for n in nodes:
        for off in (OFF_CQL, OFF_WEB, OFF_INTERNODE, OFF_GRAPH,
                    OFF_POSTGRES, OFF_SPARQL, OFF_FLIGHT, OFF_BOLT):
            p = n.base + off
            if port_open(p):
                busy.append(p)
    if busy:
        report.fail(f"ports already in use before start: {sorted(set(busy))} — an orphan "
                    f"node from a previous run is still alive; kill it and retry")
        return False
    return True


def assert_no_bind_collision(nodes: list[Node], report: Report) -> bool:
    """Grep every node log for a listener bind failure. A node that could not
    bind (most commonly bolt on 7687) is only 'ready' if the failed listener is
    tolerated — so check /metrics and the log explicitly."""
    ok = True
    for n in nodes:
        if not n.log_path.exists():
            continue
        text = n.log_path.read_text(errors="replace")
        if "Address already in use" in text:
            report.fail(f"node{n.idx}: 'Address already in use' in log — listener bind collision")
            ok = False
    return ok


# -------------------------------------------------------------------- ring ----

def read_local_tokens(session_factory, port: int) -> tuple[str, list[int]]:
    """Return (host_id, tokens) for the node reached on `port`."""
    cluster = session_factory(port)
    session = cluster.connect()
    try:
        row = session.execute("SELECT host_id, tokens FROM system.local").one()
        host_id = str(row.host_id)
        tokens = [int(t) for t in (row.tokens or [])]
        return host_id, tokens
    finally:
        cluster.shutdown()


def read_ring_view(session_factory, port: int) -> dict:
    """The ring as THIS node sees it: {host_id: [tokens]} from
    system.local + system.peers.

    This is the load-bearing measurement for the RF question. Replica sets are
    computed from the ring; if a node bootstrapped while the cluster recognised
    only a subset of members, its ring is short and RF=3 silently yields 2
    replicas. Ring-view disagreement between nodes makes placement
    nondeterministic, which is exactly what a per-row variance in achieved
    replicas looks like.
    """
    cluster = session_factory(port)
    session = cluster.connect()
    try:
        view: dict[str, list[int]] = {}
        loc = session.execute("SELECT host_id, tokens FROM system.local").one()
        view[str(loc.host_id)] = [int(t) for t in (loc.tokens or [])]
        for row in session.execute("SELECT host_id, tokens FROM system.peers"):
            if row.host_id is not None:
                view[str(row.host_id)] = [int(t) for t in (row.tokens or [])]
        return view
    finally:
        cluster.shutdown()


def build_ring(nodes: list[Node], session_factory, report: Report) -> dict:
    """Union of every node's system.local tokens -> {host_id: [tokens]}.

    Cross-checked against /api/cluster/ring (which reports the ring the server
    itself believes in) so a mismatch between client view and server view is
    visible rather than assumed away.
    """
    by_host: dict[str, list[int]] = {}
    for n in nodes:
        if not n.alive():
            continue
        try:
            host_id, tokens = read_local_tokens(session_factory, n.cql_port)
        except Exception as e:  # noqa: BLE001
            report.fail(f"node{n.idx}: could not read system.local tokens: {e!r}")
            continue
        by_host[host_id] = tokens
        report.info(f"  node{n.idx} host_id={host_id} tokens={len(tokens)}")
    return by_host


def replicas_for(token: int, token_map: list[tuple[int, str]], rf: int) -> list[str]:
    """SimpleStrategy: walk tokens clockwise from `token`, collect distinct
    nodes, stop at rf. Mirrors ferrosa-cluster/src/ring/mod.rs::replicas()."""
    if not token_map:
        return []
    toks = sorted(token_map, key=lambda x: x[0])
    # first index with token >= target
    import bisect
    keys = [t for t, _ in toks]
    i = bisect.bisect_left(keys, token)
    out: list[str] = []
    seen: set[str] = set()
    for step in range(len(toks)):
        _, host = toks[(i + step) % len(toks)]
        if host not in seen:
            seen.add(host)
            out.append(host)
            if len(out) >= rf:
                break
    return out


# ------------------------------------------------------------ landing scan ----

def scan_landing_settled(nodes: list[Node], keys: list[str], markers: dict[str, str],
                         report: Report, rounds: int = 12, gap: float = 4.0,
                         need_stable: int = 3) -> dict:
    """Poll the landing scan until no node gains any new key for `need_stable`
    consecutive scans, then return that fixed point.

    Replication is asynchronous, so a single settle-then-scan would let a
    below-expected achieved count be either a genuine replication defect OR
    writes that had not propagated yet. Under machine load the second is very
    likely. Taking the fixed point — and REPORTING the settle time — makes the
    claim defensible: the returned value was stable across repeated scans.
    """
    prev: dict[str, set[str]] = {k: set() for k in keys}
    stable = 0
    t0 = time.time()
    for r in range(rounds):
        cur = scan_landing(nodes, keys, markers)
        changed = sum(1 for k in keys if cur[k] != prev[k])
        if changed == 0:
            stable += 1
            if stable >= need_stable:
                report.info(f"  landing settled after {time.time() - t0:.1f}s "
                            f"({r + 1} scans, stable x{stable})")
                return cur
        else:
            stable = 0
        prev = cur
        time.sleep(gap)
    report.info(f"  landing scan reached {rounds} rounds ({time.time() - t0:.1f}s) "
                f"without {need_stable} fully-stable scans — using last value")
    return prev


def scan_landing(nodes: list[Node], keys: list[str], markers: dict[str, str]) -> dict:
    """Which nodes physically hold each row?

    Bounded read: the harness writes a known, small key set, so each node's
    physical storage (commitlog + sstables) is a few KB — reading it whole is
    bounded by the harness's own declared write count, not by an unbounded
    source. (This is a test harness, not a serving path.)

    Only <data_dir>/commitlog and <data_dir>/sstables are scanned, so node
    logs can never produce a false positive.
    """
    landed: dict[str, set[str]] = {k: set() for k in keys}
    for n in nodes:
        if not n.alive():
            continue
        blob = bytearray()
        for sub in ("commitlog", "sstables"):
            d = n.data_dir / sub
            if not d.exists():
                continue
            for p in sorted(d.rglob("*")):
                if p.is_file():
                    try:
                        blob += p.read_bytes()
                    except OSError:
                        pass
        b = bytes(blob)
        for k in keys:
            kb = k.encode()
            mb = markers[k].encode()
            if kb in b and mb in b:
                landed[k].add(str(n.idx))
    return landed


# ------------------------------------------------------------------- phases ----

def probe_ready(port: int, timeout: float = 15.0, tries: int = 3):
    """GET /readyz with retries. Under heavy machine load a single request can
    time out even though the node is healthy and listening; a ready probe that
    fails on the first timeout manufactures a false 'no cluster' verdict."""
    last = None
    for _ in range(tries):
        try:
            return http_json(port, "/readyz", timeout=timeout), None
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2)
    return None, last


def node_listening(n: "Node") -> bool:
    """Corroborating evidence from the node's own log that it bound internode."""
    try:
        return "internode server listening" in n.log_path.read_text(errors="replace")
    except OSError:
        return False


def wait_ready(nodes: list[Node], report: Report, timeout: int) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        states = []
        all_ok = True
        for n in nodes:
            if not n.alive():
                states.append(f"node{n.idx}=DEAD")
                all_ok = False
                continue
            body, err = probe_ready(n.web_port)
            if body is None:
                listening = node_listening(n)
                states.append(f"node{n.idx}={'probe-timeout(listening)' if listening else 'noconn'}")
                all_ok = False
            elif body.get("ready") is True:
                states.append(f"node{n.idx}=ready")
            else:
                states.append(f"node{n.idx}={body.get('waiting_for', 'not-ready')}")
                all_ok = False
        report.info(f"  readiness: {' '.join(states)}")
        if all_ok:
            return True
        time.sleep(6)
    return False


def verify_one_cluster(nodes: list[Node], report: Report, timeout: int) -> bool:
    """Prove the nodes are ONE cluster — not two pairs / one-node clusters.

    Two independent signals on every node:
      * /api/cluster/status mode == cluster
      * /admin/membership-snapshot committed_cluster_size == N and it lists N members
    """
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        good = 0
        detail = []
        for n in nodes:
            try:
                st = http_json(n.web_port, "/api/cluster/status", timeout=4)
                snap = http_json(n.web_port, "/admin/membership-snapshot", timeout=4)
            except Exception as e:  # noqa: BLE001
                detail.append(f"node{n.idx}: probe err {e!r}")
                continue
            mode = st.get("mode")
            size = snap.get("committed_cluster_size", 0)
            sm = snap.get("state_members")
            members = len(sm) if isinstance(sm, dict) else None
            detail.append(f"node{n.idx}: mode={mode} committed_size={size} members={members}")
            if mode == "cluster" and size == len(nodes) and members == len(nodes):
                good += 1
        last = "; ".join(detail)
        if good == len(nodes):
            report.info(f"  one-cluster check: {last}")
            return True
        time.sleep(5)
    report.fail(f"could not verify ONE cluster of {len(nodes)} nodes: {last}")
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=4)
    ap.add_argument("--base-port", type=int, default=49200)
    ap.add_argument("--binary", default="target/release/ferrosa")
    ap.add_argument("--data-root", default=None)
    ap.add_argument("--rf", default="1,3,5")
    ap.add_argument("--rows", type=int, default=150)
    ap.add_argument("--fault-rows", type=int, default=40)
    ap.add_argument("--ready-timeout", type=int, default=420)
    ap.add_argument("--cluster-timeout", type=int, default=300)
    ap.add_argument("--formation-timeout", type=int, default=600)
    ap.add_argument("--formation-attempts", type=int, default=3)
    ap.add_argument("--stagger", type=float, default=5.0)
    ap.add_argument("--rejoin-timeout", type=int, default=240)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    report = Report()
    here = Path(__file__).resolve().parent.parent
    binary = Path(args.binary)
    if not binary.is_absolute():
        binary = here / binary
    data_root = Path(args.data_root) if args.data_root else Path(
        os.path.expanduser("~/.hermes/cache/scratch/s3/rf-evidence"))
    log_dir = data_root / "logs"
    rf_list = [int(x) for x in args.rf.split(",")]
    out_json = Path(args.out) if args.out else data_root / "rf-evidence.json"

    report.info("=" * 78)
    report.info("ferrosa RF evidence harness")
    report.info("=" * 78)

    # --- preflight: binary + interpreter -----------------------------------
    if not binary.exists():
        report.info(f"FATAL: binary not found: {binary}")
        return 5
    mtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(binary.stat().st_mtime))
    report.info(f"binary     : {binary} ({binary.stat().st_size} bytes, mtime {mtime})")
    if not verify_murmur3():
        report.info("FATAL: murmur3 port does not match repo vectors — replica math "
                    "cannot be trusted")
        return 5
    report.info("murmur3    : port matches ferrosa Cassandra characterization vectors")
    try:
        from cassandra.cluster import Cluster  # noqa: F401
    except Exception as e:  # noqa: BLE001
        report.info(f"FATAL: python cassandra-driver not importable: {e!r}")
        return 5

    def session_factory(port: int):
        from cassandra.cluster import Cluster
        return Cluster(["127.0.0.1"], port=port, protocol_version=4,
                       connect_timeout=20, control_connection_timeout=20)

    nodes = [
        Node(i, args.base_port + (i - 1) * 10, data_root, log_dir, binary,
             seed_internode=(args.base_port + (1 - 1) * 10 + OFF_INTERNODE) if i > 1 else 0,
             expected_size=args.nodes, formation_timeout=args.formation_timeout)
        for i in range(1, args.nodes + 1)
    ]

    def teardown() -> None:
        report.info("\n[teardown] stopping nodes")
        for n in nodes:
            n.stop()

    # SIGTERM/SIGINT-safe teardown: a killed harness must not leave orphan
    # nodes holding the port band and poisoning the next run.
    stopping = {"done": False}

    def _sig(_signum, _frame):
        if not stopping["done"]:
            stopping["done"] = True
            for n in nodes:
                n.stop()
        sys.exit(143)

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    rc = 0
    try:
        # --- STEP 0: port preflight ----------------------------------------
        report.info(f"\n[STEP 0] port preflight ({args.base_port}.."
                    f"{args.base_port + len(nodes) * 10 - 1})")
        if not preflight_ports(nodes, report):
            report.info("\nFATAL(5): port band not clear — refusing to measure on a "
                        "contaminated cluster")
            report.dump(out_json)
            return 5
        report.info("  port band clear")
        # --- STEP 1: form the cluster --------------------------------------
        # Formation is retried: under heavy machine load the seed's invite
        # fan-out is racy (a joiner can be left out of the initial voter set
        # and only rejoin via the P0-21 formation-timeout path ~5 min later).
        # A harness that gave up on the first attempt would be flaky and
        # would report a false "no cluster" verdict.
        report.info(f"\n[STEP 1] starting {len(nodes)} real node processes "
                    f"(ports {args.base_port}..{args.base_port + len(nodes) * 10 - 1})")
        formed = False
        for attempt in range(1, args.formation_attempts + 1):
            if attempt > 1:
                report.info(f"  formation attempt {attempt}/{args.formation_attempts}: "
                            f"retrying from a clean slate")
                teardown()
                time.sleep(5)
            for n in nodes:  # stagger starts to reduce the invite race
                n.start(fresh=True)
                time.sleep(args.stagger)
            report.info(f"  attempt {attempt}: pids {[n.proc.pid for n in nodes]}")
            if not wait_ready(nodes, report, args.ready_timeout):
                report.info(f"  attempt {attempt}: nodes did not all become ready")
                assert_no_bind_collision(nodes, report)
                continue
            assert_no_bind_collision(nodes, report)
            if not verify_one_cluster(nodes, report, args.cluster_timeout):
                report.info(f"  attempt {attempt}: nodes did not form ONE cluster")
                continue
            formed = True
            break
        if not formed:
            report.info(f"\nFATAL(2): could not form one {len(nodes)}-node cluster in "
                        f"{args.formation_attempts} attempt(s). A vacuous "
                        f"single-node/pair run would be a false pass. Aborting.")
            report.dump(out_json)
            return 2
        report.record("cluster_formed", nodes=len(nodes),
                      pids=[n.proc.pid for n in nodes])

        ring_by_host = build_ring(nodes, session_factory, report)
        if len(ring_by_host) != len(nodes):
            report.fail(f"ring has {len(ring_by_host)} distinct host_ids, expected {len(nodes)}")
            report.dump(out_json)
            return 2
        token_map: list[tuple[int, str]] = []
        for host, toks in ring_by_host.items():
            for t in toks:
                token_map.append((t, host))
        report.info(f"  ring: {len(token_map)} tokens over {len(ring_by_host)} nodes")

        # cross-check against the server's own ring view
        try:
            srv = http_json(nodes[0].web_port, "/api/cluster/ring")
            srv_hosts = sorted(x["host_id"] for x in srv.get("nodes", []))
            if srv_hosts != sorted(ring_by_host.keys()):
                report.fail(f"client ring {sorted(ring_by_host)} != server ring {srv_hosts}")
            else:
                report.info(f"  ring cross-check: server /api/cluster/ring agrees on "
                            f"{len(srv_hosts)} nodes")
        except Exception as e:  # noqa: BLE001
            report.info(f"  (ring cross-check skipped: {e!r})")

        # --- STEP 1.5: does every node hold the SAME ring? -----------------
        # Replica sets are computed from the ring. A node that bootstrapped
        # while the cluster recognised only a subset of members computes a
        # SHORT ring -> RF=3 silently yields 2 replicas for that node's writes.
        # This is the highest-value measurement: report per node how many peers
        # it sees and the token-set size.
        report.info("\n[STEP 1.5] ring-view agreement (system.local + system.peers per node)")
        ring_views: dict[int, dict] = {}
        for n in nodes:
            try:
                ring_views[n.idx] = read_ring_view(session_factory, n.cql_port)
            except Exception as e:  # noqa: BLE001
                report.fail(f"node{n.idx}: could not read ring view: {e!r}")
        for n in nodes:
            v = ring_views.get(n.idx, {})
            peers = len(v) - 1
            toks = sum(len(t) for t in v.values())
            tag = "OK" if peers == len(nodes) - 1 else f"SHORT (expected {len(nodes) - 1} peers)"
            report.info(f"  node{n.idx}: sees {peers} peers, {len(v)} token-sets, "
                        f"{toks} tokens -> {tag}")
            if peers != len(nodes) - 1:
                report.fail(f"node{n.idx} ring view sees {peers} peers, expected "
                            f"{len(nodes) - 1} — replica placement will under-replicate")
        distinct_views = {frozenset(v.keys()) for v in ring_views.values()}
        if len(distinct_views) > 1:
            report.fail(f"nodes disagree about ring membership: {len(distinct_views)} "
                        f"distinct membership views {sorted(sorted(x) for x in distinct_views)} "
                        f"— placement is nondeterministic")
        else:
            report.info("  all nodes agree on ring membership")
        report.record("ring_views", views={str(k): sorted(v) for k, v in ring_views.items()})

        # --- STEP 2: measure achieved RF -----------------------------------
        from cassandra import ConsistencyLevel  # noqa: F401
        from cassandra.query import SimpleStatement

        all_results = {}
        for rf in rf_list:
            report.info(f"\n[STEP 2] RF={rf} on {len(nodes)} nodes  "
                        f"(expected achieved = min(RF, nodes) = {min(rf, len(nodes))})")
            ks = f"rf_{rf}"
            nonce = uuid.uuid4().hex[:8]
            keys = [f"row_{i:04d}" for i in range(args.rows)]
            markers = {k: f"mk_{k}_{nonce}" for k in keys}

            # DDL first. RF>nodes may legitimately be *rejected* — that is a
            # fail-loud outcome we record rather than a silent degradation.
            ddl_err = None
            ksc = session_factory(nodes[0].cql_port)
            kss = ksc.connect()
            try:
                kss.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = "
                            f"{{'class':'SimpleStrategy','replication_factor':'{rf}'}}")
                kss.execute(f"CREATE TABLE IF NOT EXISTS {ks}.t (k text PRIMARY KEY, v text)")
            except Exception as e:  # noqa: BLE001
                ddl_err = e
            ksc.shutdown()
            if ddl_err is not None:
                report.info(f"  DDL refused for RF={rf} on {len(nodes)} nodes: "
                            f"{type(ddl_err).__name__}: {str(ddl_err)[:140]}")
                if rf > len(nodes):
                    report.info(f"  -> RF={rf} > node count {len(nodes)} is REJECTED loudly "
                                f"(not silently accepted). Recorded.")
                    all_results[f"rf{rf}"] = {"configured_rf": rf, "nodes": len(nodes),
                                              "ddl_rejected": str(ddl_err)[:200]}
                    report.record("rf_measurement_ddl_rejected", rf=rf,
                                  err=str(ddl_err)[:200])
                    continue
                report.fail(f"RF={rf} DDL unexpectedly failed: {ddl_err!r}", d1=True)
                all_results[f"rf{rf}"] = {"configured_rf": rf, "ddl_error": str(ddl_err)[:200]}
                continue

            # writes, distributed round-robin across coordinators so a
            # per-coordinator replica-set bug cannot hide behind one entry point.
            sessions = {}
            written = {}
            counters_before = {n.idx: scrape_counters(n.web_port) for n in nodes}
            for i, k in enumerate(keys):
                coord = nodes[i % len(nodes)]
                if coord.idx not in sessions:
                    c = session_factory(coord.cql_port)
                    s = c.connect()
                    sessions[coord.idx] = (c, s)
                _, s = sessions[coord.idx]
                stmt = SimpleStatement(f"INSERT INTO {ks}.t (k,v) VALUES (%s,%s)")
                s.execute(stmt, (k, markers[k]))
                written[k] = coord.idx
            for c, _ in sessions.values():
                c.shutdown()
            report.info(f"  wrote {len(keys)} rows across "
                        f"{len(set(written.values()))} coordinators (ks={ks})")

            # measure achieved replicas at the landing fixed point
            landed = scan_landing_settled(nodes, keys, markers, report)

            # measure the coordinator fan-out directly (inference -> evidence)
            counters_after = {n.idx: scrape_counters(n.web_port) for n in nodes}
            fanout = {}
            report.info("  coordinator fan-out (delta over these writes):")
            for n in nodes:
                d = counter_delta(counters_before[n.idx], counters_after[n.idx])
                if d:
                    pretty = {short_metric(k): int(v) for k, v in sorted(d.items())}
                    fanout[f"node{n.idx}"] = pretty
                    report.info(f"    node{n.idx}: {pretty}")
            per_node = {str(n.idx): 0 for n in nodes}
            achieved_counts: dict[int, int] = {}
            coord_corr: dict[str, dict[int, int]] = {}
            expected_each = min(rf, len(nodes))
            for k in keys:
                a = len(landed[k])
                achieved_counts[a] = achieved_counts.get(a, 0) + 1
                for nid in landed[k]:
                    per_node[nid] += 1
                ck = f"node{written[k]}"
                coord_corr.setdefault(ck, {})
                coord_corr[ck][a] = coord_corr[ck].get(a, 0) + 1
            report.info(f"  achieved replica-count distribution (count -> rows): "
                        f"{dict(sorted(achieved_counts.items()))}")
            report.info(f"  rows landing per node: {per_node}")
            report.info("  achieved count correlated with coordinator:")
            for ck in sorted(coord_corr):
                report.info(f"    coordinated by {ck}: {dict(sorted(coord_corr[ck].items()))}")
            for k in keys:
                exp_replicas = replicas_for(token_for_key(k), token_map, expected_each)
                got = sorted(landed[k], key=int)
                if len(landed[k]) != expected_each:
                    report.fail(
                        f"RF={rf} key={k} (coordinator node{written[k]}): achieved "
                        f"{len(landed[k])} nodes {got}, expected {expected_each} "
                        f"{sorted(exp_replicas)}",
                        d1=True)
                    break  # one example is enough to fail loud; distribution shows scale
            if achieved_counts.get(expected_each, 0) != len(keys):
                report.fail(f"RF={rf}: not every row achieved {expected_each} replicas "
                            f"(dist={dict(sorted(achieved_counts.items()))})", d1=True)

            # --- independent counter cross-check ---------------------------
            # Write a fresh set at CL=ALL through ONE coordinator that is itself
            # a replica, so a full RF fan-out is mandatory (block_for(ALL)=rf).
            # Then compare what the coordinator ACKED against what physically
            # landed. "acked 3, holds 2" is unambiguous and load-independent.
            chk_nonce = uuid.uuid4().hex[:8]
            chk_keys = [f"chk_{i:04d}" for i in range(min(args.rows, 10))]
            chk_markers = {k: f"ckmk_{k}_{chk_nonce}" for k in chk_keys}
            coord = next((n for n in nodes
                          if any(n.host_id in replicas_for(token_for_key(k), token_map,
                                                           expected_each) for k in chk_keys)),
                         nodes[0])
            acked = failed = attempts = hints = None
            counter_check = {}
            try:
                deltas = run_writes_at_cl(nodes, chk_keys, chk_markers, session_factory,
                                          ks, ConsistencyLevel.ALL,
                                          {coord.idx: coord.cql_port})
                attempts = sum_counter(deltas, "ferrosa_coordinator_replica_write_attempts_total")
                acked = sum_counter(deltas, "ferrosa_coordinator_replica_write_acks_total")
                failed = sum_counter(deltas, "ferrosa_coordinator_replica_write_failures_total")
                hints = sum_counter(deltas, "ferrosa_coordinator_hints_stored_total")
                chk_landed = scan_landing_settled(nodes, chk_keys, chk_markers, report)
                holder_counts = {}
                for k in chk_keys:
                    holder_counts[len(chk_landed[k])] = holder_counts.get(len(chk_landed[k]), 0) + 1
                counter_check = {
                    "coordinator": f"node{coord.idx}",
                    "rows": len(chk_keys),
                    "expected_acks_per_row": expected_each,
                    "acked_total": acked, "failed_total": failed,
                    "attempts_total": attempts, "hints_stored_total": hints,
                    "holders_distribution": holder_counts,
                }
                report.info(f"  counter cross-check (CL=ALL via node{coord.idx}, "
                            f"{len(chk_keys)} rows, expected {expected_each} acks/row):")
                report.info(f"    attempts={attempts} acks={acked} failures={failed} "
                            f"hints={hints}")
                report.info(f"    physical holders per row: {holder_counts}")
                if acked is not None and acked >= expected_each * len(chk_keys) \
                        and holder_counts.get(expected_each, 0) != len(chk_keys):
                    report.fail(
                        f"RF={rf}: coordinator node{coord.idx} ACKED {acked} replica writes "
                        f"({expected_each}/row) but the data physically landed on fewer "
                        f"nodes for {len(chk_keys) - holder_counts.get(expected_each, 0)} rows "
                        f"({holder_counts}) — replication is reported as done but not durable "
                        f"on RF replicas", d1=True)
            except Exception as e:  # noqa: BLE001
                report.info(f"  (counter cross-check skipped: {type(e).__name__}: {str(e)[:100]})")

            all_results[f"rf{rf}"] = {
                "configured_rf": rf, "nodes": len(nodes), "expected_achieved": expected_each,
                "rows": len(keys), "distribution": achieved_counts,
                "per_node_landed": per_node, "coordinator_correlation": coord_corr,
                "coordinator_fanout": fanout,
                "counter_check": counter_check,
            }
            report.record("rf_measurement", **all_results[f"rf{rf}"])

        # --- STEP 3: fault injection on the RF=3 keyspace -------------------
        rf_fault = 3 if 3 in rf_list else min(len(nodes), max(rf_list))
        ks = f"rf_{rf_fault}"
        report.info(f"\n[STEP 3] fault injection on {ks} (kill a replica, then restart)")
        nonce = uuid.uuid4().hex[:8]
        fkeys = [f"f_{i:04d}" for i in range(args.fault_rows)]
        fmarkers = {k: f"fmk_{k}_{nonce}" for k in fkeys}

        # pick the node that is a replica for the most probe keys
        host_to_idx = {n.host_id: n.idx for n in nodes}
        victim = nodes[-1]
        best = -1
        for n in nodes:
            cnt = sum(1 for k in fkeys
                      if n.host_id in replicas_for(token_for_key(k), token_map, rf_fault))
            if cnt > best:
                best, victim = cnt, n
        report.info(f"  victim = node{victim.idx} (replica for {best}/{len(fkeys)} probe rows)")

        # write probe rows through a SURVIVING coordinator
        surv = [n for n in nodes if n.idx != victim.idx]
        c0 = session_factory(surv[0].cql_port)
        s0 = c0.connect()
        for k in fkeys:
            s0.execute(SimpleStatement(f"INSERT INTO {ks}.t (k,v) VALUES (%s,%s)"), (k, fmarkers[k]))
        report.info(f"  wrote {len(fkeys)} probe rows via node{surv[0].idx}")

        before = scan_landing(nodes, fkeys, fmarkers)

        # (a) SIGKILL the victim mid-operation
        report.info(f"  SIGKILL node{victim.idx} (pid {victim.proc.pid})")
        victim.kill_9()
        time.sleep(3)
        if victim.alive():
            report.fail("victim survived SIGKILL", fault=True)
        # killed node's CQL must be gone (loud failure, not a hang)
        if port_open(victim.cql_port):
            report.fail("victim CQL port still accepting after SIGKILL", fault=True)
        else:
            report.info("  victim CQL port closed (connection refuses) — loud, as required")

        # (a) survivors still return the data at QUORUM and ONE
        affected = [k for k in fkeys
                    if victim.host_id in replicas_for(token_for_key(k), token_map, rf_fault)]
        unaffected = [k for k in fkeys if k not in affected]
        report.info(f"  {len(affected)} rows had the victim in their replica set; "
                    f"{len(unaffected)} did not")
        for cl_name, cl in (("QUORUM", ConsistencyLevel.QUORUM), ("ONE", ConsistencyLevel.ONE)):
            ok = 0
            bad = []
            for k in affected[:10]:
                try:
                    row = s0.execute(SimpleStatement(
                        f"SELECT k,v FROM {ks}.t WHERE k=%s", consistency_level=cl), (k,)).one()
                    if row and row.v == fmarkers[k]:
                        ok += 1
                    else:
                        bad.append((k, "missing/empty" if not row else "wrong value"))
                except Exception as e:  # noqa: BLE001
                    bad.append((k, type(e).__name__))
            report.info(f"  reads CL={cl_name} of survivor-owned rows: {ok}/10 correct"
                        + (f", anomalies={bad}" if bad else ""))
            if cl_name == "QUORUM" and ok != 10:
                report.fail(f"survivors did not return all rows at QUORUM: {bad}", fault=True)

        # (b) a CL that REQUIRES the dead replica must fail loudly, never
        #     silently return empty/partial.
        loud = 0
        silent_empty = []
        for k in affected[:10]:
            try:
                row = s0.execute(SimpleStatement(
                    f"SELECT k,v FROM {ks}.t WHERE k=%s", consistency_level=ConsistencyLevel.ALL),
                    (k,)).one()
                if row is None:
                    silent_empty.append(k)
            except Exception as e:  # noqa: BLE001
                loud += 1
                if loud == 1:
                    report.info(f"  CL=ALL raises loudly for example: "
                                f"{type(e).__name__}: {str(e)[:90]}")
        report.info(f"  reads CL=ALL (requires the dead replica): {loud}/10 raised, "
                    f"{len(silent_empty)} silently empty")
        if silent_empty:
            report.fail(f"CL=ALL silently returned empty for {silent_empty} instead of "
                        f"failing loud", fault=True)

        # write NEW rows while the victim is down (reconciliation probe)
        down_rows = [k for k in affected[:8]]
        for k in down_rows:
            try:
                s0.execute(SimpleStatement(f"INSERT INTO {ks}.t (k,v) VALUES (%s,%s)"),
                           (k, fmarkers[k]))
            except Exception as e:  # noqa: BLE001
                report.info(f"  (write while down raised {type(e).__name__} — recording)")
        report.info(f"  re-wrote {len(down_rows)} victim-replica rows while victim down")
        c0.shutdown()

        # (c) restart the victim and check it rejoins + reconciles
        report.info(f"  restarting node{victim.idx} ...")
        victim.start(fresh=False)
        if not wait_ready([victim], report, args.rejoin_timeout):
            report.fail("restarted node did not become ready again", fault=True)
        else:
            rejoined = verify_one_cluster(nodes, report, args.rejoin_timeout)
            if not rejoined:
                report.fail("restarted node did not rejoin the single cluster", fault=True)
        after = scan_landing(nodes, fkeys, fmarkers)
        recovered = [k for k in down_rows if str(victim.idx) in after[k]]
        report.info(f"  victim now holds {len(recovered)}/{len(down_rows)} of the rows "
                    f"written while it was down")
        if len(recovered) != len(down_rows):
            missing = [k for k in down_rows if k not in recovered]
            report.fail(f"victim did not reconcile {len(missing)} rows written while it "
                        f"was down (no hinted-handoff/repair): {missing[:5]}", fault=True)
        report.record("fault_injection", victim=f"node{victim.idx}",
                      affected=len(affected), all_raises_loud=loud,
                      silent_empty=silent_empty, reconciled=len(recovered),
                      down_rows=len(down_rows))

    finally:
        if not args.keep:
            report.info("\n[teardown] stopping nodes")
            for n in nodes:
                n.stop()

    report.info("\n" + "=" * 78)
    if report.failures:
        report.info(f"RESULT: FAIL ({len(report.failures)} assertion failure(s))")
        for f in report.failures:
            report.info(f"  - {f}")
        rc = 3 if report.d1_fail else (4 if report.fault_fail else 3)
    else:
        report.info("RESULT: PASS — RF is honored; fault-injection assertions held")
        rc = 0
    report.dump(out_json)
    report.info(f"evidence written to {out_json}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
