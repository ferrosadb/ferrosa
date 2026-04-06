"""Reproduce compaction data loss on a 3-node cluster.

The bug: after high-volume ingest, compaction destroys data. This test
writes data, forces compaction via the debug endpoint, and verifies
all data survives.

Run against the 3-node cluster:
    podman compose -f tests/docker-compose.cluster.yml --profile trio up -d --build
    # Wait for cluster to form (~30s)
    pytest tests/cluster/test_compaction_data_loss.py -v -s
"""

import os
import subprocess
import time
import uuid

import pytest
import requests
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

FERROSA_HOST = os.environ.get("FERROSA_HOST", "127.0.0.1")
FERROSA_CQL_PORT = int(os.environ.get("FERROSA_CQL_PORT", "9042"))

# Web/debug ports for each node
NODE_WEB_PORTS = [9090, 9091, 9092]

KEYSPACE = "compaction_test"
TABLE = "entities"


@pytest.fixture(scope="module")
def session():
    cluster = Cluster(
        contact_points=[FERROSA_HOST],
        port=FERROSA_CQL_PORT,
        load_balancing_policy=RoundRobinPolicy(),
        protocol_version=4,
        schema_metadata_enabled=False,
        token_metadata_enabled=False,
    )
    sess = cluster.connect()
    yield sess
    sess.shutdown()
    cluster.shutdown()


@pytest.fixture(scope="module")
def schema(session):
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute(
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} ("
        "  partition_id text,"
        "  row_id int,"
        "  data text,"
        "  PRIMARY KEY (partition_id, row_id)"
        ")"
    )
    yield


def dump_node_logs(label=""):
    """Dump recent compaction-related logs from all 3 nodes."""
    rt = os.environ.get("CONTAINER_RUNTIME", "podman")
    for i, name in enumerate(["tests_node1_1", "tests_node2_1", "tests_node3_1"]):
        try:
            result = subprocess.run(
                [rt, "logs", "--tail", "100", name],
                capture_output=True, text=True, timeout=10,
            )
            # Filter for compaction-related lines
            lines = [l for l in result.stderr.split("\n")
                     if "compaction" in l.lower() or "merge" in l.lower()
                     or "swap" in l.lower() or "WARNING" in l]
            if lines:
                print(f"\n--- Node {i+1} logs {label} ---")
                for line in lines[-30:]:
                    print(f"  {line}")
        except Exception as e:
            print(f"  Could not get logs for {name}: {e}")


def count_rows(session, partition_id):
    rows = list(session.execute(
        f"SELECT count(*) FROM {KEYSPACE}.{TABLE} "
        f"WHERE partition_id = '{partition_id}'"
    ))
    return rows[0].count if rows else 0


def read_all_row_ids(session, partition_id):
    """Return a set of all row_ids for a given partition."""
    rows = list(session.execute(
        f"SELECT row_id FROM {KEYSPACE}.{TABLE} "
        f"WHERE partition_id = '{partition_id}'"
    ))
    return {r.row_id for r in rows}


class TestCompactionDataLoss:
    """Reproduce data loss that occurs specifically during/after compaction."""

    def test_data_survives_compaction(self, session, schema):
        """Write enough data to trigger compaction, verify all rows survive.

        Strategy:
        1. Write 10,000 rows to force multiple memtable flushes -> SSTables
        2. Verify all 10,000 present immediately
        3. Wait 30s for compaction to run
        4. Verify all 10,000 still present after compaction
        """
        partition = "compaction_test_1"
        total_rows = 15000
        data_payload = "x" * 500  # 500 bytes per row to trigger flushes faster

        print(f"\n--- Writing {total_rows} rows ---")
        for i in range(total_rows):
            session.execute(
                f"INSERT INTO {KEYSPACE}.{TABLE} "
                f"(partition_id, row_id, data) "
                f"VALUES ('{partition}', {i}, '{data_payload}')"
            )
            if (i + 1) % 1000 == 0:
                current = count_rows(session, partition)
                print(f"  Written: {i+1}/{total_rows}, readable: {current}")

        # Immediate verification
        pre_compaction = count_rows(session, partition)
        print(f"\nPre-compaction count: {pre_compaction}/{total_rows}")

        assert pre_compaction == total_rows, (
            f"DATA LOSS before compaction: expected {total_rows}, "
            f"got {pre_compaction}. {total_rows - pre_compaction} rows lost."
        )

        # Try force compaction (works with instrumented builds)
        print("\nForcing compaction on all nodes...")
        for port in NODE_WEB_PORTS:
            try:
                r = requests.post(
                    f"http://{FERROSA_HOST}:{port}/api/debug/force-compact",
                    timeout=30,
                )
                print(f"  node (port {port}): {r.status_code} {r.text.strip()}")
            except Exception as e:
                print(f"  node (port {port}): force-compact failed: {e}")

        # Wait for natural compaction (STCS triggers at 4+ similar-size SSTables)
        print("Waiting 60s for compaction to complete...")
        time.sleep(60)

        # Post-compaction verification
        post_compaction = count_rows(session, partition)
        print(f"Post-compaction count: {post_compaction}/{total_rows}")

        if post_compaction < total_rows:
            # Find exactly which rows are missing
            present = read_all_row_ids(session, partition)
            expected = set(range(total_rows))
            missing = expected - present
            sample = sorted(list(missing))[:20]
            print(f"\nDATA LOSS: {len(missing)} rows lost after compaction")
            print(f"Missing row_ids (first 20): {sample}")

        if post_compaction < total_rows:
            dump_node_logs("after compaction")

        assert post_compaction == total_rows, (
            f"COMPACTION DATA LOSS: expected {total_rows}, "
            f"got {post_compaction}. {total_rows - post_compaction} rows lost "
            f"after compaction ran."
        )

    def test_multi_partition_compaction(self, session, schema):
        """Write to many partitions, verify compaction preserves all.

        Different from test_data_survives_compaction: this uses many partitions
        (not one large partition), which creates different SSTable layout.
        """
        total_partitions = 500
        rows_per_partition = 5
        data_payload = "y" * 100

        print(f"\n--- Writing {total_partitions} partitions x {rows_per_partition} rows ---")
        expected_total = total_partitions * rows_per_partition

        for p in range(total_partitions):
            partition = f"multi_p_{p}"
            for r in range(rows_per_partition):
                session.execute(
                    f"INSERT INTO {KEYSPACE}.{TABLE} "
                    f"(partition_id, row_id, data) "
                    f"VALUES ('{partition}', {r}, '{data_payload}')"
                )
            if (p + 1) % 100 == 0:
                print(f"  Partitions written: {p+1}/{total_partitions}")

        # Immediate check — count a sample of partitions
        check_partitions = [f"multi_p_{i}" for i in [0, 50, 100, 250, 499]]
        pre_ok = True
        for cp in check_partitions:
            c = count_rows(session, cp)
            if c != rows_per_partition:
                print(f"  MISSING: {cp} has {c}/{rows_per_partition}")
                pre_ok = False

        assert pre_ok, "Data loss BEFORE compaction in multi-partition test"

        # Force compaction
        print("\nForcing compaction on all nodes...")
        for port in NODE_WEB_PORTS:
            try:
                r = requests.post(
                    f"http://{FERROSA_HOST}:{port}/api/debug/force-compact",
                    timeout=30,
                )
                print(f"  node (port {port}): {r.status_code}")
            except Exception as e:
                print(f"  node (port {port}): {e}")
        time.sleep(10)

        # Post-compaction: check all partitions
        lost_partitions = []
        total_found = 0
        for p in range(total_partitions):
            partition = f"multi_p_{p}"
            c = count_rows(session, partition)
            total_found += c
            if c != rows_per_partition:
                lost_partitions.append((partition, c))

        print(f"\nPost-compaction: {total_found}/{expected_total} rows across {total_partitions} partitions")
        if lost_partitions:
            print(f"Partitions with data loss: {len(lost_partitions)}")
            for (pname, cnt) in lost_partitions[:10]:
                print(f"  {pname}: {cnt}/{rows_per_partition}")

        assert total_found == expected_total, (
            f"COMPACTION DATA LOSS (multi-partition): expected {expected_total}, "
            f"got {total_found}. {len(lost_partitions)} partitions affected."
        )

    def test_multi_table_concurrent_compaction(self, session, schema):
        """Write to multiple tables simultaneously, force compaction on all.

        This exercises the compaction output directory collision bug:
        if all tables compact to the same shared directory, generation
        IDs collide and one table's output overwrites another's.
        """
        # Create a second table
        session.execute(
            f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.entities2 ("
            "  partition_id text,"
            "  row_id int,"
            "  data text,"
            "  PRIMARY KEY (partition_id, row_id)"
            ")"
        )

        total_per_table = 3000
        data_payload = "z" * 200

        # Write to both tables
        for i in range(total_per_table):
            session.execute(
                f"INSERT INTO {KEYSPACE}.{TABLE} "
                f"(partition_id, row_id, data) "
                f"VALUES ('multi_t1', {i}, '{data_payload}')"
            )
            session.execute(
                f"INSERT INTO {KEYSPACE}.entities2 "
                f"(partition_id, row_id, data) "
                f"VALUES ('multi_t2', {i}, '{data_payload}')"
            )
            if (i + 1) % 1000 == 0:
                print(f"  Written: {i+1}/{total_per_table} to both tables")

        # Force compaction on all nodes
        print("\nForcing compaction...")
        for port in NODE_WEB_PORTS:
            try:
                requests.post(
                    f"http://{FERROSA_HOST}:{port}/api/debug/force-compact",
                    timeout=30,
                )
            except Exception:
                pass
        time.sleep(15)

        # Verify both tables
        t1_count = count_rows(session, "multi_t1")
        t2_count = list(session.execute(
            f"SELECT count(*) FROM {KEYSPACE}.entities2 "
            "WHERE partition_id = 'multi_t2'"
        ))[0].count

        print(f"\nTable 1: {t1_count}/{total_per_table}")
        print(f"Table 2: {t2_count}/{total_per_table}")

        assert t1_count == total_per_table, (
            f"TABLE 1 DATA LOSS: {total_per_table - t1_count} rows lost "
            f"(compaction directory collision between tables?)"
        )
        assert t2_count == total_per_table, (
            f"TABLE 2 DATA LOSS: {total_per_table - t2_count} rows lost "
            f"(compaction directory collision between tables?)"
        )

    def test_interleaved_writes_and_compaction(self, session, schema):
        """Write in waves with pauses to trigger multiple compaction rounds."""
        partition = "wave_test"
        wave_size = 2000
        num_waves = 3
        total_expected = wave_size * num_waves

        for wave in range(num_waves):
            offset = wave * wave_size
            print(f"\n--- Wave {wave+1}: writing rows {offset}-{offset + wave_size - 1} ---")
            for i in range(wave_size):
                row_id = offset + i
                session.execute(
                    f"INSERT INTO {KEYSPACE}.{TABLE} "
                    f"(partition_id, row_id, data) "
                    f"VALUES ('{partition}', {row_id}, 'wave{wave}_data')"
                )

            # Count after each wave
            current = count_rows(session, partition)
            expected_so_far = (wave + 1) * wave_size
            print(f"  After wave {wave+1}: {current}/{expected_so_far} rows")

            # Short pause to let flushes happen
            time.sleep(10)

        # Force final compaction
        print("\nForcing compaction on all nodes...")
        for port in NODE_WEB_PORTS:
            try:
                requests.post(
                    f"http://{FERROSA_HOST}:{port}/api/debug/force-compact",
                    timeout=30,
                )
            except Exception:
                pass
        time.sleep(10)

        final_count = count_rows(session, partition)
        print(f"\nFinal count: {final_count}/{total_expected}")

        if final_count < total_expected:
            present = read_all_row_ids(session, partition)
            expected = set(range(total_expected))
            missing = expected - present

            # Determine which wave the missing rows belong to
            wave_losses = {}
            for row_id in missing:
                wave_num = row_id // wave_size
                wave_losses[wave_num] = wave_losses.get(wave_num, 0) + 1
            print(f"Loss per wave: {wave_losses}")

        assert final_count == total_expected, (
            f"COMPACTION DATA LOSS (multi-wave): expected {total_expected}, "
            f"got {final_count}. {total_expected - final_count} rows lost."
        )
