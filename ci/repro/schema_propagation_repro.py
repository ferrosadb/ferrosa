import sys, time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
auth = PlainTextAuthProvider(username='ferrosa_admin', password='ferrosa_admin')

def conn():
    c = Cluster(['127.0.0.1'], port=19042, auth_provider=auth, connect_timeout=15)
    return c, c.connect()

# Session A: create keyspace+table, insert, confirm.
cA, sA = conn()
sA.execute("CREATE KEYSPACE IF NOT EXISTS vis_test WITH replication={'class':'SimpleStrategy','replication_factor':1}")
sA.execute("CREATE TABLE IF NOT EXISTS vis_test.t (id int PRIMARY KEY, v int)")
for i in range(50):
    sA.execute("INSERT INTO vis_test.t (id,v) VALUES (%s,%s)", (i, i*10))
n = sA.execute("SELECT count(*) FROM vis_test.t").one()[0]
print(f"[A] created + wrote; count={n}")
ksA = [r.keyspace_name for r in sA.execute("SELECT keyspace_name FROM system_schema.keyspaces") if r.keyspace_name=='vis_test']
print(f"[A] sees vis_test in system_schema.keyspaces: {ksA}")

time.sleep(2)

# Session B: FRESH connection (mimics loadgen's post-load verifier).
cB, sB = conn()
ksB = [r.keyspace_name for r in sB.execute("SELECT keyspace_name FROM system_schema.keyspaces") if r.keyspace_name=='vis_test']
print(f"[B-fresh] sees vis_test in system_schema.keyspaces: {ksB}")
try:
    sB.execute("USE vis_test"); print("[B-fresh] USE vis_test: OK")
except Exception as e:
    print(f"[B-fresh] USE vis_test: FAILED -> {e}")
try:
    cnt = sB.execute("SELECT count(*) FROM vis_test.t").one()[0]
    print(f"[B-fresh] fully-qualified SELECT count = {cnt}")
except Exception as e:
    print(f"[B-fresh] fully-qualified SELECT: FAILED -> {e}")
