"""Offline tests for the SUBSCRIBE wire codec — no server required.

Builds synthetic RESULT/Rows frames and feeds them through a fake socket to
verify decoding + SubscribeStream iteration.
"""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ferrosa_driver import _protocol  # noqa: E402
from ferrosa_driver.subscribe import SubscribeStream  # noqa: E402


def _rows_body(rows):
    """Encode a RESULT/Rows body for a fixed (id int, name text) schema."""
    b = struct.pack(">i", _protocol.RESULT_ROWS)
    b += struct.pack(">i", 0x0001)  # flags: global_tables_spec
    b += struct.pack(">i", 2)  # column count
    b += _protocol._string("demo") + _protocol._string("t")  # global ks/table
    b += _protocol._string("id") + struct.pack(">H", _protocol._TYPE_INT)
    b += _protocol._string("name") + struct.pack(">H", _protocol._TYPE_VARCHAR)
    b += struct.pack(">i", len(rows))
    for (id_, name) in rows:
        idv = struct.pack(">i", id_)
        b += struct.pack(">i", len(idv)) + idv
        nv = name.encode("utf-8")
        b += struct.pack(">i", len(nv)) + nv
    return b


def _result_frame(body):
    return struct.pack(">BBhBI", 0x84, 0, 1, _protocol.OP_RESULT, len(body)) + body


class FakeSocket:
    """Serves a fixed byte stream over recv(); raises EOF-like when drained."""

    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    def recv(self, n: int) -> bytes:
        if self._pos >= len(self._data):
            return b""  # closed
        chunk = self._data[self._pos : self._pos + n]
        self._pos += len(chunk)
        return chunk

    def close(self):
        pass


def test_decode_rows_typed_values():
    names, rows = _protocol._decode_rows(_rows_body([(1, "alice"), (2, "bob")]))
    assert names == ["id", "name"]
    assert rows == [(1, "alice"), (2, "bob")]


def test_subscribe_stream_yields_namedtuples():
    # Two pushed frames, one row each — the continuous-push shape.
    stream_bytes = _result_frame(_rows_body([(10, "x")])) + _result_frame(_rows_body([(20, "y")]))
    stream = SubscribeStream(FakeSocket(stream_bytes), "SUBSCRIBE SELECT * FROM demo.t ON COMMITTED")
    first = next(stream)
    assert (first.id, first.name) == (10, "x")
    second = next(stream)
    assert (second.id, second.name) == (20, "y")


def test_empty_frame_is_skipped_then_next_delivered():
    # An empty (0-row) keep-alive frame followed by a real one.
    stream_bytes = _result_frame(_rows_body([])) + _result_frame(_rows_body([(7, "z")]))
    stream = SubscribeStream(FakeSocket(stream_bytes), "SUBSCRIBE SELECT * FROM demo.t ON LOCAL")
    row = next(stream)
    assert (row.id, row.name) == (7, "z")


if __name__ == "__main__":
    test_decode_rows_typed_values()
    test_subscribe_stream_yields_namedtuples()
    test_empty_frame_is_skipped_then_next_delivered()
    print("ferrosa_driver protocol codec: all offline tests passed")
