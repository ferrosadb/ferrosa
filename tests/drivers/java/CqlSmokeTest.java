package ferrosa.test;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CQL driver smoke tests using the DataStax Java driver 4.x.
 *
 * Each test is idempotent and uses the "java_test" keyspace.
 * Exits with code 0 on success, 1 on any failure.
 */
public class CqlSmokeTest {

    private static final String KEYSPACE = "java_test";

    private static String ferrosaHost() {
        String h = System.getenv("FERROSA_HOST");
        return (h != null && !h.isEmpty()) ? h : "127.0.0.1";
    }

    private static int ferrosaPort() {
        String p = System.getenv("FERROSA_CQL_PORT");
        if (p != null && !p.isEmpty()) {
            try {
                return Integer.parseInt(p);
            } catch (NumberFormatException ignored) {
            }
        }
        return 9042;
    }

    private static int passed = 0;
    private static int failed = 0;

    /**
     * A session with LZ4 negotiated on protocol v5.
     *
     * Protocol and compression are set on the config loader because
     * {@code withCompression} does not exist on the builder in 4.x; the
     * behaviour is selected by {@link DefaultDriverOption#PROTOCOL_COMPRESSION}.
     */
    private static CqlSession lz4Session() {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(ferrosaHost(), ferrosaPort()))
                .withLocalDatacenter("datacenter1")
                .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                        .withString(DefaultDriverOption.PROTOCOL_VERSION, "V5")
                        .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, "lz4")
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(10))
                        .build())
                .build();
    }

    private static void test(String name, Runnable fn) {
        try {
            fn.run();
            System.out.printf("  PASS  %s%n", name);
            passed++;
        } catch (Exception e) {
            System.out.printf("  FAIL  %s%n", name);
            System.out.printf("        %s%n", e.getMessage());
            failed++;
        }
    }

    private static void assertEqual(Object actual, Object expected, String label) {
        if (!expected.equals(actual)) {
            throw new AssertionError(
                    String.format("%s: expected %s, got %s", label, expected, actual));
        }
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    public static void main(String[] args) {
        System.out.println("Java CQL driver smoke tests");
        System.out.println("===========================\n");

        // ---- Connect --------------------------------------------------------

        CqlSession session;
        try {
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(ferrosaHost(), ferrosaPort()))
                    // Must match the DC ferrosa advertises in system.local
                    // ("datacenter1", the Cassandra default). The DataStax
                    // driver ignores nodes outside the configured local DC, so a
                    // mismatch yields "No node was available to execute the query".
                    .withLocalDatacenter("datacenter1")
                    .withConfigLoader(com.datastax.oss.driver.api.core.config.DriverConfigLoader.programmaticBuilder()
                            .withString(com.datastax.oss.driver.api.core.config.DefaultDriverOption.PROTOCOL_VERSION, "V5")
                            .withDuration(com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(10))
                            .build())
                    .build();
            System.out.println("  PASS  connect");
            passed++;
        } catch (Exception e) {
            System.out.printf("  FAIL  connect: %s%n", e.getMessage());
            System.exit(1);
            return;
        }

        // ---- Introspection --------------------------------------------------

        test("system.local", () -> {
            ResultSet rs = session.execute("SELECT cluster_name, data_center FROM system.local");
            Row row = rs.one();
            assertTrue(row != null, "expected at least 1 row");
            assertTrue(row.getString("cluster_name") != null, "cluster_name should not be null");
        });

        // ---- LZ4 compression on protocol v5 ---------------------------------
        //
        // ferrosa-cql#664. easy-db-lab runs cassandra-easy-stress, which uses
        // this driver with LZ4 on by default, and it could not open a session:
        // the server decompressed v5 envelope bodies a second time and closed
        // the connection at its very first query.
        //
        // This smoke test is the acceptance that the Rust-level fixtures cannot
        // give: they are captured Python-driver bytes, and that driver leaves
        // the envelope flags at 0x00, so nothing in CI ever negotiated LZ4 with
        // a real driver. The assertion below is the exact statement the issue
        // reports failing, over a connection whose compression is checked to be
        // LZ4 and not silently absent.
        test("lz4 compression on v5 (issue #664)", () -> {
            try (CqlSession compressed = lz4Session()) {
                assertEqual(
                        compressed.getContext().getProtocolVersion().name(),
                        "V5",
                        "compression test must run on protocol V5");
                assertEqual(
                        compressed.getContext().getConfig()
                                .getDefaultProfile()
                                .getString(DefaultDriverOption.PROTOCOL_COMPRESSION),
                        "lz4",
                        "compression test must actually negotiate LZ4");

                // The statement from the issue's failure trace, step 3.
                Row row = compressed
                        .execute("SELECT cluster_name FROM system.local")
                        .one();
                assertTrue(row != null, "expected a row from system.local");
                assertTrue(
                        row.getString("cluster_name") != null,
                        "cluster_name should not be null over LZ4");

                // More than the setup query: DDL and a read/write round trip
                // prove the compressed framing survives response bodies too,
                // not only requests.
                compressed.execute(
                        "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + "_lz4"
                                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
                compressed.execute(
                        "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "_lz4.kv (k text PRIMARY KEY, v text)");
                compressed.execute(
                        "INSERT INTO " + KEYSPACE + "_lz4.kv (k, v) VALUES ('a', 'lz4-roundtrip')");
                Row kv = compressed
                        .execute("SELECT v FROM " + KEYSPACE + "_lz4.kv WHERE k = 'a'")
                        .one();
                assertTrue(kv != null, "expected the written row back");
                assertEqual(kv.getString("v"), "lz4-roundtrip", "value over LZ4");

                compressed.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE + "_lz4");
            }
        });

        test("system.peers", () -> {
            ResultSet rs = session.execute("SELECT * FROM system.peers");
            assertTrue(rs != null, "result should not be null");
        });

        // ---- DDL ------------------------------------------------------------

        test("CREATE KEYSPACE", () -> {
            session.execute(
                    "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        });

        test("CREATE TABLE users", () -> {
            session.execute(
                    "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".users (" +
                            "id int PRIMARY KEY, " +
                            "name text, " +
                            "email text, " +
                            "active boolean, " +
                            "score float, " +
                            "rating double, " +
                            "age bigint" +
                            ")");
        });

        test("CREATE TABLE events", () -> {
            session.execute(
                    "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".events (" +
                            "user_id int, " +
                            "ts timestamp, " +
                            "data text, " +
                            "PRIMARY KEY (user_id, ts)" +
                            ")");
        });

        // ---- DML writes -----------------------------------------------------

        test("INSERT text, int", () -> {
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".users (id, name, email) " +
                            "VALUES (1, 'Alice', 'alice@test.com')");
        });

        test("INSERT boolean", () -> {
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".users (id, active) VALUES (2, true)");
        });

        test("INSERT float, double", () -> {
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".users (id, score, rating) VALUES (3, 95.5, 99.12345678)");
        });

        test("INSERT bigint", () -> {
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".users (id, age) VALUES (4, 9223372036854775807)");
        });

        test("INSERT events", () -> {
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".events (user_id, ts, data) " +
                            "VALUES (1, '2024-01-01T00:00:00Z', 'login')");
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".events (user_id, ts, data) " +
                            "VALUES (1, '2024-01-01T01:00:00Z', 'logout')");
        });

        // ---- DML reads ------------------------------------------------------

        test("SELECT by PK", () -> {
            ResultSet rs = session.execute(
                    "SELECT * FROM " + KEYSPACE + ".users WHERE id = 1");
            Row row = rs.one();
            assertTrue(row != null, "expected 1 row");
            assertEqual(row.getString("name"), "Alice", "name");
            assertEqual(row.getString("email"), "alice@test.com", "email");
        });

        test("SELECT boolean", () -> {
            ResultSet rs = session.execute(
                    "SELECT active FROM " + KEYSPACE + ".users WHERE id = 2");
            Row row = rs.one();
            assertTrue(row != null, "expected 1 row");
            assertEqual(row.getBoolean("active"), true, "active");
        });

        test("SELECT float, double", () -> {
            ResultSet rs = session.execute(
                    "SELECT score, rating FROM " + KEYSPACE + ".users WHERE id = 3");
            Row row = rs.one();
            assertTrue(row != null, "expected 1 row");
            assertTrue(Math.abs(row.getFloat("score") - 95.5f) < 0.01f, "score mismatch");
            assertTrue(Math.abs(row.getDouble("rating") - 99.12345678) < 0.0001, "rating mismatch");
        });

        test("SELECT bigint", () -> {
            ResultSet rs = session.execute(
                    "SELECT age FROM " + KEYSPACE + ".users WHERE id = 4");
            Row row = rs.one();
            assertTrue(row != null, "expected 1 row");
            assertEqual(row.getLong("age"), 9223372036854775807L, "age");
        });

        test("SELECT clustering range", () -> {
            ResultSet rs = session.execute(
                    "SELECT data FROM " + KEYSPACE + ".events WHERE user_id = 1 ORDER BY ts ASC");
            List<Row> rows = rs.all();
            assertEqual(rows.size(), 2, "row count");
            assertEqual(rows.get(0).getString("data"), "login", "first event");
            assertEqual(rows.get(1).getString("data"), "logout", "second event");
        });

        // ---- Prepared statements --------------------------------------------

        test("prepared INSERT", () -> {
            PreparedStatement ps = session.prepare(
                    "INSERT INTO " + KEYSPACE + ".users (id, name, email) VALUES (?, ?, ?)");
            session.execute(ps.bind(100, "JavaPrepared", "java-prepared@test.com"));
        });

        test("prepared SELECT", () -> {
            PreparedStatement ps = session.prepare(
                    "SELECT name FROM " + KEYSPACE + ".users WHERE id = ?");
            ResultSet rs = session.execute(ps.bind(100));
            Row row = rs.one();
            assertTrue(row != null, "expected 1 row");
            assertEqual(row.getString("name"), "JavaPrepared", "name");
        });

        // ---- Collections ----------------------------------------------------

        test("CREATE TABLE collections", () -> {
            session.execute(
                    "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".collections (" +
                            "id int PRIMARY KEY, " +
                            "tags list<text>, " +
                            "scores set<int>, " +
                            "props map<text, text>" +
                            ")");
        });

        test("INSERT collections", () -> {
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".collections (id, tags, scores, props) " +
                            "VALUES (1, ['tag1', 'tag2', 'tag3'], {10, 20, 30}, {'k1': 'v1', 'k2': 'v2'})");
        });

        test("SELECT list", () -> {
            ResultSet rs = session.execute(
                    "SELECT tags FROM " + KEYSPACE + ".collections WHERE id = 1");
            Row row = rs.one();
            assertTrue(row != null, "expected 1 row");
            List<String> tags = row.getList("tags", String.class);
            assertEqual(tags.size(), 3, "list size");
            assertEqual(tags.get(0), "tag1", "tags[0]");
            assertEqual(tags.get(1), "tag2", "tags[1]");
            assertEqual(tags.get(2), "tag3", "tags[2]");
        });

        test("SELECT set", () -> {
            ResultSet rs = session.execute(
                    "SELECT scores FROM " + KEYSPACE + ".collections WHERE id = 1");
            Row row = rs.one();
            assertTrue(row != null, "expected 1 row");
            Set<Integer> scores = row.getSet("scores", Integer.class);
            assertEqual(scores.size(), 3, "set size");
            assertTrue(scores.contains(10), "set should contain 10");
            assertTrue(scores.contains(20), "set should contain 20");
            assertTrue(scores.contains(30), "set should contain 30");
        });

        test("SELECT map", () -> {
            ResultSet rs = session.execute(
                    "SELECT props FROM " + KEYSPACE + ".collections WHERE id = 1");
            Row row = rs.one();
            assertTrue(row != null, "expected 1 row");
            Map<String, String> props = row.getMap("props", String.class, String.class);
            assertEqual(props.size(), 2, "map size");
            assertEqual(props.get("k1"), "v1", "map[k1]");
            assertEqual(props.get("k2"), "v2", "map[k2]");
        });

        // ---- ALTER / DELETE / UPDATE ----------------------------------------

        test("ALTER TABLE add column", () -> {
            session.execute(
                    "ALTER TABLE " + KEYSPACE + ".users ADD phone text");
        });

        test("DELETE row", () -> {
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (900, 'ToDelete')");
            session.execute(
                    "DELETE FROM " + KEYSPACE + ".users WHERE id = 900");
            ResultSet rs = session.execute(
                    "SELECT * FROM " + KEYSPACE + ".users WHERE id = 900");
            Row row = rs.one();
            assertTrue(row == null, "row should be deleted");
        });

        test("UPDATE row", () -> {
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".users (id, name, email) " +
                            "VALUES (901, 'BeforeUpdate', 'old@test.com')");
            session.execute(
                    "UPDATE " + KEYSPACE + ".users SET email = 'new@test.com' WHERE id = 901");
            ResultSet rs = session.execute(
                    "SELECT email FROM " + KEYSPACE + ".users WHERE id = 901");
            Row row = rs.one();
            assertTrue(row != null, "expected 1 row");
            assertEqual(row.getString("email"), "new@test.com", "updated email");
        });

        test("INSERT IF NOT EXISTS", () -> {
            // First insert should succeed
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (902, 'LwtUser') IF NOT EXISTS");
            // Second insert should not apply
            ResultSet rs = session.execute(
                    "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (902, 'LwtUserDup') IF NOT EXISTS");
            assertTrue(!rs.wasApplied(), "[applied] should be false");
        });

        // ---- Batch ----------------------------------------------------------

        test("batch INSERT", () -> {
            BatchStatement batch = BatchStatement.builder(DefaultBatchType.LOGGED)
                    .addStatement(SimpleStatement.newInstance(
                            "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (801, 'BatchUser1')"))
                    .addStatement(SimpleStatement.newInstance(
                            "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (802, 'BatchUser2')"))
                    .addStatement(SimpleStatement.newInstance(
                            "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (803, 'BatchUser3')"))
                    .build();
            session.execute(batch);

            // Verify all three rows
            for (int id = 801; id <= 803; id++) {
                ResultSet rs = session.execute(
                        "SELECT name FROM " + KEYSPACE + ".users WHERE id = " + id);
                Row row = rs.one();
                assertTrue(row != null, "batch row " + id + " should exist");
                assertEqual(row.getString("name"), "BatchUser" + (id - 800), "batch row " + id + " name");
            }
        });

        // ---- TTL ------------------------------------------------------------

        test("INSERT with TTL", () -> {
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (950, 'TtlUser') USING TTL 1");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ResultSet rs = session.execute(
                    "SELECT * FROM " + KEYSPACE + ".users WHERE id = 950");
            Row row = rs.one();
            assertTrue(row == null, "row should have expired via TTL");
        });

        // ---- Counts & Limits ------------------------------------------------

        test("SELECT COUNT(*)", () -> {
            ResultSet rs = session.execute(
                    "SELECT COUNT(*) FROM " + KEYSPACE + ".users");
            Row row = rs.one();
            assertTrue(row != null, "expected result row");
            long count = row.getLong(0);
            assertTrue(count > 0, "expected count > 0, got " + count);
        });

        test("SELECT LIMIT", () -> {
            ResultSet rs = session.execute(
                    "SELECT * FROM " + KEYSPACE + ".users LIMIT 2");
            List<Row> rows = rs.all();
            assertTrue(rows.size() <= 2, "expected at most 2 rows, got " + rows.size());
        });

        // ---- Error handling -------------------------------------------------

        test("query nonexistent table", () -> {
            boolean caught = false;
            try {
                session.execute("SELECT * FROM " + KEYSPACE + ".no_such_table");
            } catch (InvalidQueryException e) {
                caught = true;
            }
            assertTrue(caught, "expected InvalidQueryException for nonexistent table");
        });

        test("invalid CQL syntax", () -> {
            boolean caught = false;
            try {
                session.execute("NOT VALID CQL AT ALL");
            } catch (SyntaxError e) {
                caught = true;
            }
            assertTrue(caught, "expected SyntaxError for invalid CQL syntax");
        });

        // ---- System schema --------------------------------------------------

        test("system_schema.keyspaces", () -> {
            ResultSet rs = session.execute(
                    "SELECT keyspace_name FROM system_schema.keyspaces");
            List<Row> rows = rs.all();
            boolean found = false;
            for (Row row : rows) {
                if (KEYSPACE.equals(row.getString("keyspace_name"))) {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "expected " + KEYSPACE + " in system_schema.keyspaces");
        });

        // ---- NULL handling --------------------------------------------------

        test("INSERT NULL", () -> {
            session.execute(
                    "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (960, null)");
        });

        test("SELECT NULL", () -> {
            ResultSet rs = session.execute(
                    "SELECT name FROM " + KEYSPACE + ".users WHERE id = 960");
            Row row = rs.one();
            assertTrue(row != null, "expected 1 row");
            assertTrue(row.isNull("name"), "name should be null");
        });

        // ---- Index ----------------------------------------------------------

        test("CREATE INDEX", () -> {
            session.execute(
                    "CREATE INDEX IF NOT EXISTS ON " + KEYSPACE + ".users (name)");
        });

        // ---- Cleanup --------------------------------------------------------

        test("DROP KEYSPACE", () -> {
            session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        });

        session.close();

        // ---- Report ---------------------------------------------------------

        System.out.println("\n===========================");
        System.out.printf("Results: %d passed, %d failed%n", passed, failed);
        System.out.println("===========================");

        System.exit(failed > 0 ? 1 : 0);
    }
}
