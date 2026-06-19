# ferrosa-driver (Python)

A **drop-in replacement for the DataStax `cassandra-driver`** that adds
ferrosa's `SUBSCRIBE` — real-time, push-based change streaming over the CQL
wire (port 9042).

Standard statements (`SELECT`/`INSERT`/`CREATE`/…) go through the real
`cassandra-driver` unchanged. `SUBSCRIBE` cannot — it is a *continuous server
push*, not one-response-per-query, which the stock driver's request/response
model can't consume — so it runs over a dedicated raw connection that streams
each change as it commits.

## Install

```bash
pip install ./drivers          # or: cd drivers && pip install -e .
```

Requires `cassandra-driver>=3.0` (pulled in automatically).

## Drop-in usage

Swap the import; everything else is identical:

```python
# before:  from cassandra.cluster import Cluster
#          from cassandra.auth import PlainTextAuthProvider
from ferrosa_driver import Cluster, PlainTextAuthProvider

cluster = Cluster(["127.0.0.1"], port=9042,
                  auth_provider=PlainTextAuthProvider("cassandra", "cassandra"))
session = cluster.connect()
session.execute("INSERT INTO demo.t (id, v) VALUES (1, 'a')")   # standard path
```

## SUBSCRIBE (the addition)

Two equivalent entry points — a standalone function, or `session.subscribe(...)`
which reuses the session's contact point + credentials:

```python
from ferrosa_driver import subscribe

with subscribe("127.0.0.1", "SUBSCRIBE demo.t ON COMMITTED",
               username="cassandra", password="cassandra") as stream:
    for change in stream:        # blocks until the next change is *pushed*
        print(change)            # namedtuple keyed by the result columns

# or, reusing an existing session:
with session.subscribe("SUBSCRIBE demo.t ON LOCAL") as stream:
    for change in stream:
        print(change)
```

- `ON LOCAL` → the **WrittenOnNode** stream: every local commit (commit-log append).
- `ON COMMITTED` → the **CommittedToCluster** stream: changes committed across the cluster.

Each item is a `namedtuple` keyed by the result columns — the same shape the
`cassandra-driver` yields for a `SELECT` row.

## Real-time, not polling

Delivery is **event-driven push**: the iterator blocks on the socket and is
woken the instant the server pushes a change frame — there is no polling
interval. Latency is the commit cost plus the wire hop, with no poll-interval
floor.

## Two-process demo

Against a running ferrosa server (CQL on 9042):

```bash
# terminal 1 — subscribe (prints each change live)
python examples/subscribe_process.py --stream COMMITTED

# terminal 2 — write (standard driver INSERTs)
python examples/write_process.py --rows 5
```

Each `INSERT` in terminal 2 appears in terminal 1 in real time.

## Offline tests

The wire codec has server-free tests (synthetic frames):

```bash
python tests/test_protocol.py
```

## Protocol note

`SUBSCRIBE` is delivered as a sequence of CQL `RESULT`/`Rows` frames pushed on
the query's stream id over time. This package speaks CQL native protocol v4
directly for that path (STARTUP → optional PLAIN auth → `QUERY(SUBSCRIBE)` →
loop-read `RESULT` frames), decoding the common scalar/temporal types. All other
functionality is delegated to `cassandra-driver`.
