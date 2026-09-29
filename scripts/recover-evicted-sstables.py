#!/usr/bin/env python3
"""Mark SSTables that a pre-marker ferrosa evicted, so the next start restores them.

Before FMEA ST-38 was fixed, the uploaded-SSTable cache evictor deleted local
copies under disk pressure without recording it, and a restart then left
every evicted SSTable out of its table (S3 still holds them). A ferrosa with
the fix restores, before any table registers, every generation that has a
`<data_dir>/sstables/<table>/<gen>.evicted` marker and is still listed in the
S3 manifest.

This script recovers the record from the node's log: each
`s3-sync: evicted uploaded local SSTable from cache table="T" sstable="G"`
line becomes a marker for T/G, unless G is already back on local disk.

Dry run by default; pass --apply to write the markers. Stop the node first.

    scripts/recover-evicted-sstables.py \\
        --log ~/.ferrosa/logs/node1.out.log \\
        --data-dir ~/data/ferrosa-memory/node1 [--apply]
"""

import argparse
import collections
import os
import re
import sys

ANSI = re.compile(rb"\x1b\[[0-9;]*m")
EVICTED = re.compile(
    rb'evicted uploaded local SSTable from cache table="([^"]+)" sstable="(\d+)"'
)


def evictions(log_path, tail_mb):
    """Yield (table, generation) for every eviction the log records."""
    with open(log_path, "rb") as f:
        if tail_mb:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - tail_mb * 1024 * 1024))
            f.readline()  # drop the partial first line
        for line in f:
            if b"evicted uploaded local SSTable" not in line:
                continue
            m = EVICTED.search(ANSI.sub(b"", line))
            if m:
                yield m.group(1).decode(), m.group(2).decode()


def is_local(table_dir, gen):
    return os.path.exists(os.path.join(table_dir, f"{gen}-Data.db")) or os.path.exists(
        os.path.join(table_dir, gen, f"{gen}-Data.db")
    )


def write_marker(table_dir, gen):
    marker = os.path.join(table_dir, f"{gen}.evicted")
    fd = os.open(marker, os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    dfd = os.open(table_dir, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--log", required=True, help="the node's stdout log")
    ap.add_argument("--data-dir", required=True, help="the node's data_dir")
    ap.add_argument("--tail-mb", type=int, default=0, help="scan only the last N MiB (0 = all)")
    ap.add_argument("--apply", action="store_true", help="write the markers")
    args = ap.parse_args()

    sstables = os.path.join(os.path.expanduser(args.data_dir), "sstables")
    if not os.path.isdir(sstables):
        sys.exit(f"no sstables directory at {sstables}")

    planned = collections.Counter()
    local = missing_dir = 0
    seen = set()
    for table, gen in evictions(os.path.expanduser(args.log), args.tail_mb):
        if (table, gen) in seen:
            continue
        seen.add((table, gen))
        table_dir = os.path.join(sstables, table)
        if not os.path.isdir(table_dir):
            missing_dir += 1
            print(f"skip {table}/{gen}: table directory is gone (table dropped?)")
            continue
        if is_local(table_dir, gen):
            local += 1
            continue
        planned[table] += 1
        if args.apply:
            write_marker(table_dir, gen)

    for table, n in sorted(planned.items()):
        print(f"{'marked' if args.apply else 'would mark'} {n:4d}  {table}")
    print(
        f"{'marked' if args.apply else 'would mark'} {sum(planned.values())} generation(s); "
        f"{local} already local; {missing_dir} with no table directory; "
        f"{len(seen)} evictions in the log"
    )
    if not seen:
        sys.exit("no evictions found in the log — nothing to recover (wrong log?)")


if __name__ == "__main__":
    main()
