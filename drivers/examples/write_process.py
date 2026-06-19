#!/usr/bin/env python3
"""Writer process: standard cassandra-driver writes that the subscriber sees live.

Uses the re-exported standard driver API (no SUBSCRIBE here) — proving the
package is a drop-in for ordinary work. Run after starting subscribe_process.py.

    python write_process.py --rows 5
"""
import argparse
import sys
import time

from ferrosa_driver import Cluster, PlainTextAuthProvider


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9042)
    ap.add_argument("--keyspace", default="demo")
    ap.add_argument("--table", default="t")
    ap.add_argument("--user", default="cassandra")
    ap.add_argument("--password", default="cassandra")
    ap.add_argument("--rows", type=int, default=5)
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between inserts")
    args = ap.parse_args()

    cluster = Cluster(
        [args.host],
        port=args.port,
        auth_provider=PlainTextAuthProvider(args.user, args.password),
    )
    session = cluster.connect()
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {args.keyspace} "
        "WITH replication = {'class':'SimpleStrategy','replication_factor':1}"
    )
    session.execute(
        f"CREATE TABLE IF NOT EXISTS {args.keyspace}.{args.table} (id int PRIMARY KEY, v text)"
    )
    for i in range(1, args.rows + 1):
        session.execute(
            f"INSERT INTO {args.keyspace}.{args.table} (id, v) VALUES (%s, %s)",
            (i, f"value-{i}"),
        )
        print(f"[writer] inserted id={i}", flush=True)
        time.sleep(args.interval)
    cluster.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
