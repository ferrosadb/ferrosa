#!/usr/bin/env python3
"""Subscriber process: stream live changes from a ferrosa table over CQL (9042).

Run this in one terminal, then run write_process.py in another against the same
ferrosa server — each INSERT shows up here in real time (event-driven push, not
polling).

    python subscribe_process.py --stream COMMITTED   # cluster-committed changes
    python subscribe_process.py --stream LOCAL        # local commits (WrittenOnNode)
"""
import argparse
import sys

from ferrosa_driver import subscribe


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9042)
    ap.add_argument("--keyspace", default="demo")
    ap.add_argument("--table", default="t")
    ap.add_argument("--stream", choices=["LOCAL", "COMMITTED"], default="COMMITTED")
    ap.add_argument("--user", default="cassandra")
    ap.add_argument("--password", default="cassandra")
    ap.add_argument("--count", type=int, default=0, help="stop after N changes (0 = forever)")
    args = ap.parse_args()

    query = f"SUBSCRIBE {args.keyspace}.{args.table} ON {args.stream}"
    print(f"[subscriber] {query}  ({args.host}:{args.port})", flush=True)
    n = 0
    with subscribe(
        args.host, query, port=args.port, username=args.user, password=args.password
    ) as stream:
        for change in stream:
            n += 1
            print(f"[subscriber] change #{n}: {change}", flush=True)
            if args.count and n >= args.count:
                break
    return 0


if __name__ == "__main__":
    sys.exit(main())
