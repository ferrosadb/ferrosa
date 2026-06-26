import threading, time, sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
auth = PlainTextAuthProvider(username='ferrosa_admin', password='ferrosa_admin')

def newconn():
    c = Cluster(['127.0.0.1'], port=19042, auth_provider=auth, connect_timeout=25)
    return c, c.connect()

N = 50
WORKERS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
ITERS = int(sys.argv[2]) if len(sys.argv) > 2 else 25

defects = []   # (ks, first_count, full, recount_500ms, recount_2s)
errors = 0
lock = threading.Lock()

def cnt(s, ks):
    return s.execute(SimpleStatement(f"SELECT count(*) FROM {ks}.t")).one()[0]
def full(s, ks):
    return len(list(s.execute(SimpleStatement(f"SELECT id FROM {ks}.t", fetch_size=100000))))

def worker(wid):
    global errors
    c, s = newconn()
    for it in range(ITERS):
        ks = f"k_{wid}_{it}"
        try:
            s.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication={{'class':'SimpleStrategy','replication_factor':1}}")
            s.execute(f"CREATE TABLE IF NOT EXISTS {ks}.t (id int PRIMARY KEY, v int)")
            for i in range(N):
                s.execute(SimpleStatement(f"INSERT INTO {ks}.t (id,v) VALUES (%s,%s)"), (i, i*10))
            cc, ss = newconn()
            try:
                c0 = cnt(ss, ks)
                f0 = full(ss, ks)
                if c0 != N or f0 != N:
                    time.sleep(0.5); c1 = cnt(ss, ks)
                    time.sleep(1.5); c2 = cnt(ss, ks); f2 = full(ss, ks)
                    with lock:
                        defects.append((ks, c0, f0, c1, c2, f2))
            finally:
                cc.shutdown()
        except Exception as e:
            with lock:
                errors += 1
            continue
    c.shutdown()

t0 = time.time()
threads = [threading.Thread(target=worker, args=(i,)) for i in range(WORKERS)]
for t in threads: t.start()
for t in threads: t.join()
print(f"elapsed={time.time()-t0:.1f}s errors={errors} defects={len(defects)}")
print("ks                first_count full recount_0.5s recount_2s full_2s")
for d in defects[:40]:
    print(f"  {d[0]:14s} {d[1]:11d} {d[2]:4d} {d[3]:11d} {d[4]:10d} {d[5]:6d}")
