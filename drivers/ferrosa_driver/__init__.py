"""ferrosa_driver — a drop-in replacement for the DataStax ``cassandra-driver``
that adds ferrosa's ``SUBSCRIBE`` (real-time change streaming).

The full standard driver API is re-exported unchanged, so existing code keeps
working by swapping the import:

    # before
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    # after — identical behaviour, plus SUBSCRIBE
    from ferrosa_driver import Cluster, PlainTextAuthProvider

Standard statements (SELECT/INSERT/...) go through the real driver. ``SUBSCRIBE``
cannot — it is a continuous server push, not one-response-per-query — so it runs
over a dedicated connection via :func:`subscribe` (or the added
``session.subscribe(...)``), yielding each change in real time:

    cluster = Cluster(["127.0.0.1"], port=9042,
                      auth_provider=PlainTextAuthProvider("cassandra", "cassandra"))
    session = cluster.connect()
    session.execute("INSERT INTO ks.t (id, v) VALUES (1, 'a')")   # standard path
    with session.subscribe("SUBSCRIBE ks.t ON COMMITTED") as stream:
        for change in stream:                                      # pushed in real time
            print(change)
"""

from __future__ import annotations

# Re-export the standard driver API so this is a drop-in import swap.
from cassandra.cluster import (  # noqa: F401
    Cluster,
    ExecutionProfile,
    ResultSet,
    Session,
)
from cassandra.auth import PlainTextAuthProvider  # noqa: F401
from cassandra.query import (  # noqa: F401
    BatchStatement,
    PreparedStatement,
    SimpleStatement,
)

from .subscribe import SubscribeStream, subscribe

__all__ = [
    "Cluster",
    "Session",
    "ExecutionProfile",
    "ResultSet",
    "PlainTextAuthProvider",
    "SimpleStatement",
    "PreparedStatement",
    "BatchStatement",
    "subscribe",
    "SubscribeStream",
]


def _session_subscribe(self, query, *, timeout=None):
    """``session.subscribe(query)`` — open a SUBSCRIBE reusing this session's
    cluster contact point, port, and (PlainText) credentials."""
    cluster = self.cluster
    contact_points = list(getattr(cluster, "contact_points", []) or [])
    host = str(contact_points[0]) if contact_points else "127.0.0.1"
    port = getattr(cluster, "port", 9042) or 9042
    auth = getattr(cluster, "auth_provider", None)
    username = getattr(auth, "username", None) if auth is not None else None
    password = getattr(auth, "password", None) if auth is not None else None
    return subscribe(
        host, query, port=port, username=username, password=password, timeout=timeout
    )


# Add `.subscribe(...)` to the standard driver Session (drop-in ergonomics).
Session.subscribe = _session_subscribe
