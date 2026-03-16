package ferrosa.test;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;

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
                    .withLocalDatacenter("datacenter1")
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
