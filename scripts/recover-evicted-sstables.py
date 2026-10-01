#!/usr/bin/env python3
"""UNSUPPORTED. Reference only. Superseded by `ferrosa-ctl sstable mark-evicted`.

    !! DO NOT USE THIS TO RECOVER DATA. !!

Python scripts must not recover or mutate production data (see
scripts/README.md). The supported, tested path is:

    ferrosa-ctl sstable mark-evicted --data-dir <dir> --log <node.log> [--apply]

It is dry-run by default, refuses a data directory whose node is running, never
overwrites an existing marker, and writes a marker that records who wrote it.

This file is kept as a reference for the log format and the recovery idea. It
refuses to run unless you pass `--i-understand-this-is-unsupported`.

Why it is unsupported: on 2026-09-29 it wrote eviction markers straight into
live node data directories. It and the evictor both wrote an EMPTY
`<gen>.evicted` file, so afterwards nobody could tell a real eviction from
this script's own marking, and the reason for ~1000 evictions could not be
established. The marker now carries a record (trigger, byte figures, source);
this script still writes the empty legacy form.

What it does: before FMEA ST-38 was fixed, the uploaded-SSTable cache evictor
deleted local copies under disk pressure without recording it, and a restart
then left every evicted SSTable out of its table (S3 still holds them). This
script recovers the record from the node's log: each
`s3-sync: evicted uploaded local SSTable from cache table="T" sstable="G"`
line becomes a marker for T/G, unless G is already back on local disk.
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
    # Unsupported reference copy: main() refuses to run without --i-understand-this-is-unsupported.
    fd = os.open(marker, os.O_WRONLY | os.O_CREAT, 0o644)  # data-dir-write-ok: unsupported reference copy, gated behind an explicit flag
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
    ap.add_argument(
        "--i-understand-this-is-unsupported",
        action="store_true",
        help="required: this script is reference only; use `ferrosa-ctl sstable mark-evicted`",
    )
    args = ap.parse_args()
    if not args.i_understand_this_is_unsupported:
        sys.exit(
            "refusing to run: this script is UNSUPPORTED and reference only. "
            "Use `ferrosa-ctl sstable mark-evicted --data-dir <dir> --log <log> [--apply]`. "
            "(Override with --i-understand-this-is-unsupported.)"
        )

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
