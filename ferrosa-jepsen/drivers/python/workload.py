"""Ferrosa-Jepsen workload generator — Python driver.

Connects to a Ferrosa/Cassandra cluster via CQL and runs register, bank,
or LWT workload patterns, recording operation history as JSONL.
"""

import argparse
import json
import os
import random
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event

from cassandra.cluster import Cluster

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------

NUM_ACCOUNTS = 10
INITIAL_BALANCE = 1000
STOP = Event()

LWT_PATTERNS = [
    "lwt-1-insert-if-not-exists",
    "lwt-2-update-if",
    "lwt-3-delete-if",
    "lwt-4-insert-if-not-exists-ttl",
    "lwt-5-update-if-exists",
    "lwt-6-replace-if",
    "lwt-7-increment-if",
    "lwt-8-batch-insert",
    "lwt-9-batch-mixed",
    "lwt-10-collections",
    "lwt-11-udt",
    "lwt-12-counter",
    "lwt-13-timestamp",
    "lwt-14-wire-format",
    "lwt-15-serial-read",
    "lwt-16-multi-statement",
]


def now_us():
    """Current UTC time in microseconds."""
    return int(time.time() * 1_000_000)


def record_op(out, client_id, invoke, complete, op, result):
    """Write one JSONL line."""
    line = json.dumps(
        {
            "client_id": client_id,
            "invoke_us": invoke,
            "complete_us": complete,
            "op": op,
            "result": result,
        },
        separators=(",", ":"),
    )
    out.write(line + "\n")
    out.flush()


# ---------------------------------------------------------------------------
# Schema setup
# ---------------------------------------------------------------------------

CREATE_KS = (
    "CREATE KEYSPACE IF NOT EXISTS jepsen "
    "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"
)


def setup_register(session):
    session.execute(CREATE_KS)
    session.execute(
        "CREATE TABLE IF NOT EXISTS jepsen.register " "(id int PRIMARY KEY, val int)"
    )
    session.execute("INSERT INTO jepsen.register (id, val) VALUES (0, 0)")


def setup_bank(session):
    session.execute(CREATE_KS)
    session.execute(
        "CREATE TABLE IF NOT EXISTS jepsen.accounts "
        "(id int PRIMARY KEY, balance bigint)"
    )
    for i in range(NUM_ACCOUNTS):
        session.execute(
            f"INSERT INTO jepsen.accounts (id, balance) VALUES ({i}, {INITIAL_BALANCE})"
        )


def setup_lwt(session, pattern_num):
    session.execute(CREATE_KS)
    session.execute(
        f"CREATE TABLE IF NOT EXISTS jepsen.lwt{pattern_num} "
        "(id text PRIMARY KEY, val text)"
    )


# ---------------------------------------------------------------------------
# Workload runners (single-thread, called from pool)
# ---------------------------------------------------------------------------


def run_register(session, out, client_id, duration_s):
    """Register workload: 50% reads, 30% writes, 20% CAS on key 0."""
    deadline = time.monotonic() + duration_s
    counter = 1

    while time.monotonic() < deadline and not STOP.is_set():
        r = random.random()
        if r < 0.5:
            op = {"Read": {"key": "0"}}
            invoke = now_us()
            try:
                rows = session.execute("SELECT val FROM jepsen.register WHERE id = 0")
                complete = now_us()
                row = rows.one()
                val = row.val if row else None
                result = {"Value": val}
            except Exception as exc:
                complete = now_us()
                if "timeout" in str(exc).lower():
                    result = "Timeout"
                else:
                    result = {"Err": str(exc)}
        elif r < 0.8:
            op = {"Write": {"key": "0", "value": counter}}
            invoke = now_us()
            try:
                session.execute(
                    f"UPDATE jepsen.register SET val = {counter} WHERE id = 0"
                )
                complete = now_us()
                result = "Ok"
            except Exception as exc:
                complete = now_us()
                if "timeout" in str(exc).lower():
                    result = "Timeout"
                else:
                    result = {"Err": str(exc)}
            counter += 1
        else:
            expected = counter - 1
            op = {"Cas": {"key": "0", "expected": expected, "value": counter}}
            invoke = now_us()
            try:
                rows = session.execute(
                    f"UPDATE jepsen.register SET val = {counter} "
                    f"WHERE id = 0 IF val = {expected}"
                )
                complete = now_us()
                row = rows.one()
                applied = row.applied if row else False
                result = {"Applied": applied}
            except Exception as exc:
                complete = now_us()
                if "timeout" in str(exc).lower():
                    result = "Timeout"
                else:
                    result = {"Err": str(exc)}
            counter += 1

        record_op(out, client_id, invoke, complete, op, result)


def run_bank(session, out, client_id, duration_s):
    """Bank workload: 70% transfers, 30% balance reads."""
    deadline = time.monotonic() + duration_s

    while time.monotonic() < deadline and not STOP.is_set():
        r = random.random()
        if r < 0.7:
            from_id = random.randint(0, NUM_ACCOUNTS - 1)
            to_id = random.randint(0, NUM_ACCOUNTS - 1)
            if to_id == from_id:
                to_id = (from_id + 1) % NUM_ACCOUNTS
            amount = random.randint(1, 100)

            # Read source balance
            op = {"Read": {"key": f"account-{from_id}"}}
            invoke = now_us()
            try:
                rows = session.execute(
                    f"SELECT balance FROM jepsen.accounts WHERE id = {from_id}"
                )
                complete = now_us()
                row = rows.one()
                balance = row.balance if row else None
                result = {"Value": balance}
            except Exception as exc:
                complete = now_us()
                result = (
                    "Timeout" if "timeout" in str(exc).lower() else {"Err": str(exc)}
                )
                record_op(out, client_id, invoke, complete, op, result)
                continue

            record_op(out, client_id, invoke, complete, op, result)
            if balance is None or balance < amount:
                continue

            # CAS debit
            new_balance = balance - amount
            op = {
                "Cas": {
                    "key": f"account-{from_id}",
                    "expected": balance,
                    "value": new_balance,
                }
            }
            invoke = now_us()
            try:
                rows = session.execute(
                    f"UPDATE jepsen.accounts SET balance = {new_balance} "
                    f"WHERE id = {from_id} IF balance = {balance}"
                )
                complete = now_us()
                row = rows.one()
                applied = row.applied if row else False
                result = {"Applied": applied}
            except Exception as exc:
                complete = now_us()
                result = (
                    "Timeout" if "timeout" in str(exc).lower() else {"Err": str(exc)}
                )
                record_op(out, client_id, invoke, complete, op, result)
                continue

            record_op(out, client_id, invoke, complete, op, result)
            if not applied:
                continue

            # Credit destination
            op = {"Write": {"key": f"account-{to_id}", "value": amount}}
            invoke = now_us()
            try:
                session.execute(
                    f"UPDATE jepsen.accounts SET balance = balance + {amount} "
                    f"WHERE id = {to_id}"
                )
                complete = now_us()
                result = "Ok"
            except Exception as exc:
                complete = now_us()
                result = (
                    "Timeout" if "timeout" in str(exc).lower() else {"Err": str(exc)}
                )
            record_op(out, client_id, invoke, complete, op, result)

        else:
            # Read all balances
            op = {"SerialRead": {"key": "all-accounts"}}
            invoke = now_us()
            values = []
            had_error = False
            for i in range(NUM_ACCOUNTS):
                try:
                    rows = session.execute(
                        f"SELECT balance FROM jepsen.accounts WHERE id = {i}"
                    )
                    row = rows.one()
                    val = str(row.balance) if row else "0"
                    values.append([f"account-{i}", val])
                except Exception as exc:
                    complete = now_us()
                    result = (
                        "Timeout"
                        if "timeout" in str(exc).lower()
                        else {"Err": str(exc)}
                    )
                    record_op(out, client_id, invoke, complete, op, result)
                    had_error = True
                    break
            if not had_error:
                complete = now_us()
                result = {"CurrentValues": values}
                record_op(out, client_id, invoke, complete, op, result)


def run_lwt(session, out, client_id, duration_s, pattern):
    """LWT workload: runs INSERT IF NOT EXISTS / UPDATE IF patterns."""
    pattern_num = int(pattern.split("-")[1])
    table = f"jepsen.lwt{pattern_num}"
    deadline = time.monotonic() + duration_s
    seq = 0

    while time.monotonic() < deadline and not STOP.is_set():
        if pattern_num in (1, 4, 8):
            # INSERT IF NOT EXISTS patterns
            val = f"v{seq}"
            op = {
                "InsertIfNotExists": {
                    "table": table,
                    "pk": "pk-0",
                    "values": [["val", val]],
                }
            }
            invoke = now_us()
            try:
                cql = f"INSERT INTO {table} (id, val) VALUES ('pk-0', '{val}') IF NOT EXISTS"
                rows = session.execute(cql)
                complete = now_us()
                row = rows.one()
                applied = row.applied if row else False
                result = {"Applied": applied}
            except Exception as exc:
                complete = now_us()
                result = (
                    "Timeout" if "timeout" in str(exc).lower() else {"Err": str(exc)}
                )
        elif pattern_num == 3:
            # DELETE IF
            op = {
                "DeleteIf": {
                    "table": table,
                    "pk": "pk-0",
                    "condition": "val IS NOT NULL",
                }
            }
            invoke = now_us()
            try:
                rows = session.execute(
                    f"DELETE FROM {table} WHERE id = 'pk-0' IF val != null"
                )
                complete = now_us()
                row = rows.one()
                applied = row.applied if row else False
                result = {"Applied": applied}
            except Exception as exc:
                complete = now_us()
                result = (
                    "Timeout" if "timeout" in str(exc).lower() else {"Err": str(exc)}
                )
        else:
            # UPDATE IF patterns (default for most LWT numbers)
            expected = seq
            new_val = seq + 1
            op = {
                "UpdateIf": {
                    "table": table,
                    "pk": "pk-0",
                    "condition": f"val = {expected}",
                    "assignments": [["val", str(new_val)]],
                }
            }
            invoke = now_us()
            try:
                rows = session.execute(
                    f"UPDATE {table} SET val = '{new_val}' "
                    f"WHERE id = 'pk-0' IF val = '{expected}'"
                )
                complete = now_us()
                row = rows.one()
                applied = row.applied if row else False
                result = {"Applied": applied}
                if applied:
                    seq = new_val
            except Exception as exc:
                complete = now_us()
                result = (
                    "Timeout" if "timeout" in str(exc).lower() else {"Err": str(exc)}
                )

        record_op(out, client_id, invoke, complete, op, result)
        seq += 1


# ---------------------------------------------------------------------------
# Thread worker
# ---------------------------------------------------------------------------


def worker(contact_points, workload, duration_s, output_dir, client_id, thread_idx):
    """Single thread worker: connect, run workload, write JSONL."""
    tid = f"{client_id}-{thread_idx}"
    cluster = Cluster(contact_points)
    session = cluster.connect()
    session.default_timeout = 10.0

    path = os.path.join(output_dir, f"{tid}.jsonl")
    with open(path, "w") as out:
        if workload == "register":
            run_register(session, out, tid, duration_s)
        elif workload == "bank":
            run_bank(session, out, tid, duration_s)
        elif workload in LWT_PATTERNS:
            run_lwt(session, out, tid, duration_s, workload)
        else:
            print(f"Unknown workload: {workload}", file=sys.stderr)
            sys.exit(1)

    cluster.shutdown()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Ferrosa-Jepsen Python workload")
    parser.add_argument(
        "--contact-points", required=True, help="Comma-separated contact points"
    )
    parser.add_argument("--workload", required=True, help="Workload name")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--threads", type=int, default=4, help="Number of threads")
    parser.add_argument(
        "--output-dir", required=True, help="Output directory for JSONL"
    )
    parser.add_argument("--client-id", default="python", help="Client ID prefix")
    args = parser.parse_args()

    contact_points = [cp.strip() for cp in args.contact_points.split(",")]
    os.makedirs(args.output_dir, exist_ok=True)

    # Setup schema with a single connection
    cluster = Cluster(contact_points)
    session = cluster.connect()
    session.default_timeout = 30.0

    if args.workload == "register":
        setup_register(session)
    elif args.workload == "bank":
        setup_bank(session)
    elif args.workload in LWT_PATTERNS:
        pattern_num = int(args.workload.split("-")[1])
        setup_lwt(session, pattern_num)
    else:
        print(f"Unknown workload: {args.workload}", file=sys.stderr)
        sys.exit(1)

    cluster.shutdown()

    # Run workload threads
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        futures = []
        for i in range(args.threads):
            f = pool.submit(
                worker,
                contact_points,
                args.workload,
                args.duration,
                args.output_dir,
                args.client_id,
                i,
            )
            futures.append(f)

        for f in as_completed(futures):
            try:
                f.result()
            except Exception as exc:
                print(f"Worker error: {exc}", file=sys.stderr)


def handle_signal(signum, frame):
    STOP.set()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    try:
        main()
    except Exception as exc:
        print(f"Fatal: {exc}", file=sys.stderr)
        sys.exit(1)
