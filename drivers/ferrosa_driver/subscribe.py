"""SUBSCRIBE support layered on top of the standard cassandra-driver API.

Standard queries go through the real driver (re-exported from the package
root); ``subscribe`` opens its own raw connection because the continuous push
does not fit the driver's one-response-per-stream model.
"""

from __future__ import annotations

import keyword
import re
from collections import namedtuple
from typing import Iterator, List, Optional

from . import _protocol

_IDENT_RE = re.compile(r"\W|^(?=\d)")


def _sanitize(names: List[str]) -> List[str]:
    """Make column names valid, unique Python identifiers for a namedtuple."""
    out: List[str] = []
    seen: dict[str, int] = {}
    for i, name in enumerate(names):
        ident = _IDENT_RE.sub("_", name) or f"col{i}"
        if keyword.iskeyword(ident):
            ident += "_"
        if ident in seen:
            seen[ident] += 1
            ident = f"{ident}_{seen[ident]}"
        else:
            seen[ident] = 0
        out.append(ident)
    return out


class SubscribeStream:
    """An open SUBSCRIBE. Iterate to receive change events in real time.

    Each item is a ``namedtuple`` keyed by the result column names — the same
    shape the cassandra-driver yields for a SELECT row. The iterator blocks
    until the next change is pushed (event-driven; there is no polling). Use as
    a context manager, or call :meth:`close`, to tear down the connection.
    """

    def __init__(self, sock, query: str):
        self._sock = sock
        self.query = query
        self._row_cls = None
        self._buffered: list = []

    def __iter__(self) -> Iterator:
        return self

    def __next__(self):
        while True:
            if self._buffered:
                return self._buffered.pop(0)
            names, rows = _protocol.read_result_rows(self._sock)
            if self._row_cls is None and names:
                self._row_cls = namedtuple("Row", _sanitize(names))
            if self._row_cls is not None:
                self._buffered = [self._row_cls(*r) for r in rows]
            else:
                self._buffered = list(rows)
            # An empty frame (0 rows) is a keep-alive — loop and keep waiting.

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass

    def __enter__(self) -> "SubscribeStream":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


def subscribe(
    contact_point: str,
    query: str,
    *,
    port: int = 9042,
    username: Optional[str] = None,
    password: Optional[str] = None,
    timeout: Optional[float] = None,
) -> SubscribeStream:
    """Open a live SUBSCRIBE against ``contact_point:port`` and stream changes.

    >>> with subscribe("127.0.0.1", "SUBSCRIBE ks.t ON COMMITTED",
    ...                 username="cassandra", password="cassandra") as stream:
    ...     for change in stream:
    ...         print(change)

    ``query`` must be a ``SUBSCRIBE`` statement; ``ON LOCAL`` streams local
    commits (WrittenOnNode), ``ON COMMITTED`` streams cluster-committed changes
    (CommittedToCluster). Returns a :class:`SubscribeStream`.
    """
    if not query.lstrip().upper().startswith("SUBSCRIBE"):
        raise ValueError("subscribe() requires a SUBSCRIBE statement")
    sock = _protocol.open_connection(contact_point, port, username, password, timeout)
    _protocol.send_query(sock, query)
    return SubscribeStream(sock, query)
