"""Raw CQL v4 client to test fresh-connection count(*) under load WITHOUT the
python driver's connection/response machinery. We control the stream-id and
inspect the raw response header, so we can tell server-vs-driver definitively.

Setup (keyspaces+data) is done via the high-level driver on a persistent conn;
the COUNT is issued over a hand-rolled raw socket on a fresh TCP connection.
"""
import socket, struct, sys, threading, time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

HOST, PORT = "127.0.0.1", 19042
USER = PASS = "ferrosa_admin"
V_REQ, V_RESP = 0x04, 0x84
OP_ERROR, OP_STARTUP, OP_READY, OP_AUTHENTICATE = 0x00, 0x01, 0x02, 0x03
OP_AUTH_RESPONSE, OP_AUTH_SUCCESS, OP_QUERY, OP_RESULT = 0x0F, 0x10, 0x07, 0x08

def _str(s):
    b = s.encode(); return struct.pack(">H", len(b)) + b
def _lstr(s):
    b = s.encode(); return struct.pack(">i", len(b)) + b
def _bytes(b):
    return struct.pack(">i", len(b)) + b

def frame(op, body, stream=0):
    return struct.pack(">BBhB", V_REQ, 0, stream, op) + struct.pack(">i", len(body)) + body

def read_frame(sock):
    hdr = b""
    while len(hdr) < 9:
        chunk = sock.recv(9 - len(hdr))
        if not chunk: raise EOFError("closed during header")
        hdr += chunk
    ver, flags, stream, op = struct.unpack(">BBhB", hdr[:5])
    (length,) = struct.unpack(">i", hdr[5:9])
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk: raise EOFError("closed during body")
        body += chunk
    return ver, flags, stream, op, body

def handshake(sock):
    sock.sendall(frame(OP_STARTUP, struct.pack(">H", 1) + _str("CQL_VERSION") + _str("3.0.0"), stream=0))
    _, _, _, op, body = read_frame(sock)
    if op == OP_AUTHENTICATE:
        token = b"\x00" + USER.encode() + b"\x00" + PASS.encode()
        sock.sendall(frame(OP_AUTH_RESPONSE, _bytes(token), stream=0))
        _, _, _, op, body = read_frame(sock)
        if op != OP_AUTH_SUCCESS:
            raise RuntimeError(f"auth failed op=0x{op:02x}")
    elif op == OP_READY:
        pass
    else:
        raise RuntimeError(f"unexpected handshake op=0x{op:02x}")

def parse_count_result(body):
    # RESULT: [int kind]; kind=2 Rows. Returns the single bigint value or raises.
    (kind,) = struct.unpack(">i", body[:4]); off = 4
    if kind != 2:
        return ("non-rows-result", kind)
    (flags,) = struct.unpack(">i", body[off:off+4]); off += 4
    (col_count,) = struct.unpack(">i", body[off:off+4]); off += 4
    # global_tables_spec flag = 0x0001 -> [string ks][string table] follows
    if flags & 0x0001:
        for _ in range(2):
            (l,) = struct.unpack(">H", body[off:off+2]); off += 2 + l
    # column specs: per col -> [string name][option id]. With global spec, name only + type.
    for _ in range(col_count):
        (l,) = struct.unpack(">H", body[off:off+2]); off += 2 + l   # col name
        (opt,) = struct.unpack(">H", body[off:off+2]); off += 2       # type id (bigint=0x0002)
    (row_count,) = struct.unpack(">i", body[off:off+4]); off += 4
    if row_count < 1:
        return ("zero-rows", row_count)
    (vlen,) = struct.unpack(">i", body[off:off+4]); off += 4
    val = body[off:off+vlen]
    if vlen == 8:
        return ("ok", struct.unpack(">q", val)[0])
    return ("badlen", vlen)

def raw_count(ks, stream_id):
    sock = socket.create_connection((HOST, PORT), timeout=25)
    try:
        handshake(sock)
        q = f"SELECT count(*) FROM {ks}.t"
        body = _lstr(q) + struct.pack(">H", 0x0001) + struct.pack(">B", 0x00)  # consistency ONE, no flags
        sock.sendall(frame(OP_QUERY, body, stream=stream_id))
        ver, flags, rstream, op, rbody = read_frame(sock)
        return {"req_stream": stream_id, "resp_stream": rstream, "op": op, "body": rbody}
    finally:
        sock.close()

# ---- setup keyspaces + data via the high-level driver (persistent) ----
auth = PlainTextAuthProvider(username=USER, password=PASS)
s = Cluster([HOST], port=PORT, auth_provider=auth, connect_timeout=25).connect()
N = 50
WORKERS = int(sys.argv[1]) if len(sys.argv) > 1 else 6
ITERS = int(sys.argv[2]) if len(sys.argv) > 2 else 30

anomalies = []
lock = threading.Lock()

def worker(wid):
    for it in range(ITERS):
        ks = f"raw_{wid}_{it}"
        try:
            s.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication={{'class':'SimpleStrategy','replication_factor':1}}")
            s.execute(f"CREATE TABLE IF NOT EXISTS {ks}.t (id int PRIMARY KEY, v int)")
            from cassandra.query import SimpleStatement
            for i in range(N):
                s.execute(SimpleStatement(f"INSERT INTO {ks}.t (id,v) VALUES (%s,%s)"), (i, i*10))
        except Exception as e:
            continue  # setup miss (propagation) — not what we're testing
        sid = 0x4242
        try:
            r = raw_count(ks, sid)
        except Exception as e:
            with lock: anomalies.append((ks, "RAW_EXC", str(e)[:50]))
            continue
        if r["resp_stream"] != sid:
            with lock: anomalies.append((ks, "STREAM_MISMATCH", f"req={sid:#x} resp={r['resp_stream']:#x} op=0x{r['op']:02x}"))
            continue
        if r["op"] == OP_ERROR:
            # server returned an error frame (matched stream) — acceptable (loud)
            continue
        if r["op"] != OP_RESULT:
            with lock: anomalies.append((ks, "WRONG_OPCODE", f"op=0x{r['op']:02x}"))
            continue
        status, val = parse_count_result(r["body"])
        if status != "ok" or val != N:
            with lock: anomalies.append((ks, "WRONG_COUNT", f"status={status} val={val}"))

threads = [threading.Thread(target=worker, args=(i,)) for i in range(WORKERS)]
t0 = time.time()
for t in threads: t.start()
for t in threads: t.join()
print(f"raw-protocol fresh-conn count test: workers={WORKERS} iters={ITERS} elapsed={time.time()-t0:.1f}s")
print(f"anomalies: {len(anomalies)}")
for a in anomalies[:40]:
    print(f"  {a[0]}: {a[1]} {a[2]}")
