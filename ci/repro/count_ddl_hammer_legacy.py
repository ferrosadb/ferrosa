import threading, time, sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
auth = PlainTextAuthProvider(username='ferrosa_admin', password='ferrosa_admin')

def newconn():
    c = Cluster(['127.0.0.1'], port=19042, auth_provider=auth, connect_timeout=20)
    return c, c.connect()

N = 50
WORKERS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
ITERS = int(sys.argv[2]) if len(sys.argv) > 2 else 25
FRESH = (sys.argv[3] != "reuse") if len(sys.argv) > 3 else True

bad = []
lock = threading.Lock()

def worker(wid):
    c, s = newconn()
    for it in range(ITERS):
        ks = f"k_{wid}_{it}"
        s.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication={{'class':'SimpleStrategy','replication_factor':1}}")
        s.execute(f"CREATE TABLE IF NOT EXISTS {ks}.t (id int PRIMARY KEY, v int)")
        for i in range(N):
            s.execute(SimpleStatement(f"INSERT INTO {ks}.t (id,v) VALUES (%s,%s)"), (i, i*10))
        # IMMEDIATE count in the post-DDL window, from a fresh connection.
        if FRESH:
            cc, ss = newconn()
            try:
                cnt = ss.execute(SimpleStatement(f"SELECT count(*) FROM {ks}.t")).one()[0]
                full = len(list(ss.execute(SimpleStatement(f"SELECT id FROM {ks}.t", fetch_size=100000))))
            finally:
                cc.shutdown()
        else:
            cnt = s.execute(SimpleStatement(f"SELECT count(*) FROM {ks}.t")).one()[0]
            full = len(list(s.execute(SimpleStatement(f"SELECT id FROM {ks}.t", fetch_size=100000))))
        if cnt != N or full != N:
            with lock:
                bad.append((ks, cnt, full))
    c.shutdown()

t0 = time.time()
threads = [threading.Thread(target=worker, args=(i,)) for i in range(WORKERS)]
for t in threads: t.start()
for t in threads: t.join()
total = WORKERS * ITERS
print(f"workers={WORKERS} iters={ITERS} fresh_conn={FRESH} total_cycles={total} elapsed={time.time()-t0:.1f}s")
print(f"DEFECTS: {len(bad)} / {total}")
for b in bad[:40]:
    print(f"  ks={b[0]} count(*)={b[1]} full={b[2]}")
