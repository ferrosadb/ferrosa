"""Minimal CQL native-protocol v4 codec — just enough for ferrosa SUBSCRIBE.

The standard cassandra-driver models every request as exactly one response on a
stream id, then frees the stream. ferrosa's ``SUBSCRIBE`` instead pushes a
*continuous* sequence of RESULT/Rows frames on the query's stream id as change
events occur. The high-level driver cannot consume that, so this module speaks
the wire protocol directly on a dedicated socket: STARTUP (+ optional PLAIN
auth) -> QUERY(subscribe) -> read RESULT/Rows frames forever.

Only the subset needed for SUBSCRIBE is implemented; everything else (prepared
statements, batches, paging, the full type system) stays in the real driver,
which this package re-exports unchanged.
"""

from __future__ import annotations

import socket
import struct
import uuid
from datetime import datetime, timezone
from ipaddress import ip_address

# --- frame opcodes (CQL v4) ------------------------------------------------
OP_STARTUP = 0x01
OP_READY = 0x02
OP_AUTHENTICATE = 0x03
OP_AUTH_RESPONSE = 0x0F
OP_AUTH_SUCCESS = 0x10
OP_QUERY = 0x07
OP_RESULT = 0x08
OP_ERROR = 0x00

VERSION_REQUEST = 0x04
VERSION_RESPONSE = 0x84

# RESULT kinds
RESULT_VOID = 0x0001
RESULT_ROWS = 0x0002

# consistency levels
CL_ONE = 0x0001

# column type ids (option id -> name); enough for SUBSCRIBE result rows.
_TYPE_CUSTOM = 0x0000
_TYPE_ASCII = 0x0001
_TYPE_BIGINT = 0x0002
_TYPE_BLOB = 0x0003
_TYPE_BOOLEAN = 0x0004
_TYPE_COUNTER = 0x0005
_TYPE_DOUBLE = 0x0007
_TYPE_FLOAT = 0x0008
_TYPE_INT = 0x0009
_TYPE_TIMESTAMP = 0x000B
_TYPE_UUID = 0x000C
_TYPE_VARCHAR = 0x000D
_TYPE_TIMEUUID = 0x000F
_TYPE_INET = 0x0010
_TYPE_SMALLINT = 0x0013
_TYPE_TINYINT = 0x0014
_COLLECTION_TYPES = {0x0020, 0x0021, 0x0022}  # list, map, set (nested option follows)


class ProtocolError(Exception):
    """A wire-level protocol failure (bad frame, server ERROR, auth failure)."""


# --- low-level reads/writes ------------------------------------------------
def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ProtocolError("connection closed by server")
        buf.extend(chunk)
    return bytes(buf)


def _write_frame(sock: socket.socket, stream: int, opcode: int, body: bytes) -> None:
    header = struct.pack(">BBhBI", VERSION_REQUEST, 0, stream, opcode, len(body))
    sock.sendall(header + body)


def _read_frame(sock: socket.socket) -> tuple[int, int, bytes]:
    """Return (stream_id, opcode, body) of the next frame."""
    header = _recv_exact(sock, 9)
    _version, _flags, stream, opcode, length = struct.unpack(">BBhBI", header)
    body = _recv_exact(sock, length) if length else b""
    return stream, opcode, body


def _string(s: str) -> bytes:
    raw = s.encode("utf-8")
    return struct.pack(">H", len(raw)) + raw


def _long_string(s: str) -> bytes:
    raw = s.encode("utf-8")
    return struct.pack(">I", len(raw)) + raw


def _string_map(m: dict[str, str]) -> bytes:
    out = struct.pack(">H", len(m))
    for k, v in m.items():
        out += _string(k) + _string(v)
    return out


# --- a cursor over a frame body -------------------------------------------
class _Reader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def u8(self) -> int:
        v = self.data[self.pos]
        self.pos += 1
        return v

    def i16(self) -> int:
        (v,) = struct.unpack_from(">h", self.data, self.pos)
        self.pos += 2
        return v

    def u16(self) -> int:
        (v,) = struct.unpack_from(">H", self.data, self.pos)
        self.pos += 2
        return v

    def i32(self) -> int:
        (v,) = struct.unpack_from(">i", self.data, self.pos)
        self.pos += 4
        return v

    def string(self) -> str:
        n = self.u16()
        s = self.data[self.pos : self.pos + n].decode("utf-8")
        self.pos += n
        return s

    def bytes_(self) -> bytes | None:
        n = self.i32()
        if n < 0:
            return None  # NULL
        v = self.data[self.pos : self.pos + n]
        self.pos += n
        return v

    def option(self) -> int:
        """Read a [option] type id, skipping nested option(s) for collections."""
        type_id = self.u16()
        if type_id in _COLLECTION_TYPES:
            self.option()  # element type
            if type_id == 0x0021:  # map: a second nested option
                self.option()
        elif type_id == _TYPE_CUSTOM:
            self.string()  # custom class name
        return type_id


# --- value decoding --------------------------------------------------------
def _decode_value(type_id: int, raw: bytes | None):
    if raw is None:
        return None
    if type_id in (_TYPE_VARCHAR, _TYPE_ASCII):
        return raw.decode("utf-8", "replace")
    if type_id == _TYPE_INT:
        return struct.unpack(">i", raw)[0]
    if type_id in (_TYPE_BIGINT, _TYPE_COUNTER):
        return struct.unpack(">q", raw)[0]
    if type_id == _TYPE_SMALLINT:
        return struct.unpack(">h", raw)[0]
    if type_id == _TYPE_TINYINT:
        return struct.unpack(">b", raw)[0]
    if type_id == _TYPE_BOOLEAN:
        return raw != b"\x00"
    if type_id == _TYPE_FLOAT:
        return struct.unpack(">f", raw)[0]
    if type_id == _TYPE_DOUBLE:
        return struct.unpack(">d", raw)[0]
    if type_id in (_TYPE_UUID, _TYPE_TIMEUUID):
        return uuid.UUID(bytes=raw)
    if type_id == _TYPE_TIMESTAMP:
        millis = struct.unpack(">q", raw)[0]
        return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)
    if type_id == _TYPE_INET:
        return ip_address(raw)
    # blob / unknown -> raw bytes (lossless)
    return raw


def _decode_rows(body: bytes) -> tuple[list[str], list[tuple]]:
    """Decode a RESULT/Rows body into (column_names, [row_tuples])."""
    r = _Reader(body)
    kind = r.i32()
    if kind != RESULT_ROWS:
        # VOID or other — no rows to deliver.
        return [], []
    flags = r.i32()
    col_count = r.i32()
    has_more_pages = bool(flags & 0x0002)
    if has_more_pages:
        r.bytes_()  # paging state (ignored for a live subscription)
    no_metadata = bool(flags & 0x0004)
    global_spec = bool(flags & 0x0001)
    names: list[str] = []
    type_ids: list[int] = []
    if not no_metadata:
        if global_spec:
            r.string()  # global keyspace
            r.string()  # global table
        for _ in range(col_count):
            if not global_spec:
                r.string()  # per-col keyspace
                r.string()  # per-col table
            names.append(r.string())
            type_ids.append(r.option())
    row_count = r.i32()
    rows: list[tuple] = []
    for _ in range(row_count):
        rows.append(tuple(_decode_value(type_ids[c], r.bytes_()) for c in range(col_count)))
    return names, rows


# --- handshake + query -----------------------------------------------------
def open_connection(
    host: str,
    port: int,
    username: str | None = None,
    password: str | None = None,
    timeout: float | None = None,
) -> socket.socket:
    """Open a socket and complete STARTUP (+ optional PLAIN auth)."""
    sock = socket.create_connection((host, port), timeout=timeout)
    sock.settimeout(timeout)
    _write_frame(sock, 0, OP_STARTUP, _string_map({"CQL_VERSION": "3.0.0"}))
    _stream, opcode, body = _read_frame(sock)
    if opcode == OP_READY:
        return sock
    if opcode == OP_AUTHENTICATE:
        token = b"\x00" + (username or "").encode() + b"\x00" + (password or "").encode()
        _write_frame(sock, 0, OP_AUTH_RESPONSE, struct.pack(">I", len(token)) + token)
        _stream, opcode, body = _read_frame(sock)
        if opcode == OP_AUTH_SUCCESS:
            return sock
        raise ProtocolError(f"authentication failed (opcode 0x{opcode:02x})")
    if opcode == OP_ERROR:
        raise ProtocolError(f"server ERROR during STARTUP: {_error_message(body)}")
    raise ProtocolError(f"unexpected STARTUP response opcode 0x{opcode:02x}")


def send_query(sock: socket.socket, cql: str, stream: int = 1) -> None:
    body = _long_string(cql) + struct.pack(">HB", CL_ONE, 0x00)
    _write_frame(sock, stream, OP_QUERY, body)


def _error_message(body: bytes) -> str:
    r = _Reader(body)
    code = r.i32()
    try:
        msg = r.string()
    except Exception:
        msg = ""
    return f"[code 0x{code:08x}] {msg}"


def read_result_rows(sock: socket.socket) -> tuple[list[str], list[tuple]]:
    """Block for the next frame; return decoded (names, rows). Raises on ERROR."""
    _stream, opcode, body = _read_frame(sock)
    if opcode == OP_RESULT:
        return _decode_rows(body)
    if opcode == OP_ERROR:
        raise ProtocolError(f"server ERROR: {_error_message(body)}")
    raise ProtocolError(f"unexpected frame opcode 0x{opcode:02x} while subscribed")
