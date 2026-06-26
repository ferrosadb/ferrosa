import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
auth = PlainTextAuthProvider(username='ferrosa_admin', password='ferrosa_admin')
c = Cluster(['127.0.0.1'], port=19042, auth_provider=auth, connect_timeout=15)
s = c.connect()
s.execute("DROP KEYSPACE IF EXISTS cnt_test")
s.execute("CREATE KEYSPACE cnt_test WITH replication={'class':'SimpleStrategy','replication_factor':1}")
s.execute("CREATE TABLE cnt_test.t (id int PRIMARY KEY, v int)")
N = 50
for i in range(N):
    s.execute("INSERT INTO cnt_test.t (id,v) VALUES (%s,%s)", (i, i*10))
print(f"inserted {N}")
def cnt():
    return s.execute(SimpleStatement("SELECT count(*) FROM cnt_test.t")).one()[0]
def full():
    return len(list(s.execute(SimpleStatement("SELECT id FROM cnt_test.t", fetch_size=100000))))
# Many samples over ~15s to see convergence vs structural.
for k in range(20):
    print(f"t={k*0.7:4.1f}s  count(*)={cnt():3d}  full={full():3d}")
    time.sleep(0.7)
