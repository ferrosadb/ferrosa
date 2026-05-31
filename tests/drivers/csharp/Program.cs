/// CQL driver smoke tests using the DataStax CassandraCSharpDriver.
///
/// Each test is idempotent (IF NOT EXISTS / IF EXISTS). The entire suite
/// uses the "csharp_test" keyspace to avoid collisions with other drivers.
/// Exits with code 0 on success, 1 on any failure.

using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

class CqlSmokeTest
{
    private const string KEYSPACE = "csharp_test";

    private static int passed = 0;
    private static int failed = 0;

    private static string FerrosaHost()
    {
        string h = Environment.GetEnvironmentVariable("FERROSA_HOST");
        return !string.IsNullOrEmpty(h) ? h : "127.0.0.1";
    }

    private static int FerrosaPort()
    {
        string p = Environment.GetEnvironmentVariable("FERROSA_CQL_PORT");
        if (!string.IsNullOrEmpty(p) && int.TryParse(p, out int port))
        {
            return port;
        }
        return 9042;
    }

    private static void Test(string name, Action fn)
    {
        try
        {
            fn();
            Console.WriteLine($"  PASS  {name}");
            passed++;
        }
        catch (Exception e)
        {
            Console.WriteLine($"  FAIL  {name}");
            Console.WriteLine($"        {e.Message}");
            failed++;
        }
    }

    private static void AssertEqual<T>(T actual, T expected, string label)
    {
        if (!EqualityComparer<T>.Default.Equals(actual, expected))
        {
            throw new Exception(
                $"{label}: expected {expected}, got {actual}");
        }
    }

    private static void AssertTrue(bool condition, string message)
    {
        if (!condition)
        {
            throw new Exception(message);
        }
    }

    private static void AssertThrows<TException>(Action fn, string label)
        where TException : Exception
    {
        bool threw = false;
        try
        {
            fn();
        }
        catch (TException)
        {
            threw = true;
        }
        catch (AggregateException ae) when (ae.InnerExceptions.Any(e => e is TException))
        {
            threw = true;
        }
        if (!threw)
        {
            throw new Exception($"{label}: expected {typeof(TException).Name} but none was thrown");
        }
    }

    static void Main(string[] args)
    {
        Console.WriteLine("C# CQL driver smoke tests");
        Console.WriteLine("=========================\n");

        string host = FerrosaHost();
        int port = FerrosaPort();

        // ---- Connect --------------------------------------------------------

        ISession session;
        Cluster cluster;
        try
        {
            cluster = Cluster.Builder()
                .AddContactPoint(host)
                .WithPort(port)
                // Let the driver auto-negotiate protocol version
                .Build();
            session = cluster.Connect();
            Console.WriteLine("  PASS  connect");
            passed++;
        }
        catch (Exception e)
        {
            Console.WriteLine($"  FAIL  connect: {e.Message}");
            Environment.Exit(1);
            return;
        }

        // ---- Connection & Introspection ------------------------------------

        Test("system_local", () =>
        {
            var rs = session.Execute("SELECT cluster_name, data_center FROM system.local");
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected at least 1 row");
            AssertTrue(row.GetValue<string>("cluster_name") != null, "cluster_name should not be null");
        });

        Test("system_peers", () =>
        {
            var rs = session.Execute("SELECT * FROM system.peers");
            AssertTrue(rs != null, "result should not be null");
        });

        Test("system_schema_keyspaces", () =>
        {
            var rs = session.Execute("SELECT * FROM system_schema.keyspaces");
            AssertTrue(rs != null, "result should not be null");
            var rows = rs.ToList();
            AssertTrue(rows.Count >= 1, "expected at least 1 keyspace");
        });

        // ---- DDL ------------------------------------------------------------

        Test("create_keyspace", () =>
        {
            session.Execute(
                "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        });

        Test("create_table_users", () =>
        {
            session.Execute(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".users (" +
                "id int PRIMARY KEY, " +
                "name text, " +
                "email text, " +
                "active boolean, " +
                "score float, " +
                "rating double, " +
                "age bigint, " +
                "profile blob, " +
                "user_uuid uuid, " +
                "created_at timestamp" +
                ")");
        });

        Test("create_table_events", () =>
        {
            session.Execute(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".events (" +
                "user_id int, " +
                "ts timestamp, " +
                "data text, " +
                "PRIMARY KEY (user_id, ts)" +
                ")");
        });

        Test("create_table_collections", () =>
        {
            session.Execute(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".collections (" +
                "id int PRIMARY KEY, " +
                "tags list<text>, " +
                "scores set<int>, " +
                "props map<text, text>" +
                ")");
        });

        Test("alter_table_add_column", () =>
        {
            session.Execute("ALTER TABLE " + KEYSPACE + ".users ADD phone text");
        });

        Test("create_index", () =>
        {
            session.Execute("CREATE INDEX IF NOT EXISTS ON " + KEYSPACE + ".users (name)");
        });

        // ---- DML Writes -----------------------------------------------------

        Test("insert_text_int", () =>
        {
            session.Execute(
                "INSERT INTO " + KEYSPACE + ".users (id, name, email) " +
                "VALUES (1, 'Alice', 'alice@test.com')");
        });

        Test("insert_boolean", () =>
        {
            session.Execute(
                "INSERT INTO " + KEYSPACE + ".users (id, active) VALUES (2, true)");
        });

        Test("insert_float_double", () =>
        {
            session.Execute(
                "INSERT INTO " + KEYSPACE + ".users (id, score, rating) VALUES (3, 95.5, 99.12345678)");
        });

        Test("insert_bigint", () =>
        {
            session.Execute(
                "INSERT INTO " + KEYSPACE + ".users (id, age) VALUES (4, 9223372036854775807)");
        });

        Test("insert_blob", () =>
        {
            session.Execute(
                "INSERT INTO " + KEYSPACE + ".users (id, profile) VALUES (5, 0xdeadbeef)");
        });

        Test("insert_uuid", () =>
        {
            var testUuid = Guid.NewGuid();
            var ps = session.Prepare(
                "INSERT INTO " + KEYSPACE + ".users (id, user_uuid) VALUES (?, ?)");
            session.Execute(ps.Bind(6, testUuid));
        });

        Test("insert_timestamp", () =>
        {
            var now = DateTimeOffset.UtcNow;
            var ps = session.Prepare(
                "INSERT INTO " + KEYSPACE + ".users (id, created_at) VALUES (?, ?)");
            session.Execute(ps.Bind(7, now));
        });

        Test("insert_null", () =>
        {
            session.Execute(
                "INSERT INTO " + KEYSPACE + ".users (id, name, email) VALUES (8, null, null)");
        });

        Test("insert_if_not_exists", () =>
        {
            // First insert should be applied
            var rs1 = session.Execute(
                "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (9, 'LWT_First') IF NOT EXISTS");
            var row1 = rs1.FirstOrDefault();
            AssertTrue(row1 != null, "expected result row");
            AssertTrue(row1.GetValue<bool>("[applied]"), "first INSERT IF NOT EXISTS should be applied");

            // Second insert with same PK should not be applied
            var rs2 = session.Execute(
                "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (9, 'LWT_Second') IF NOT EXISTS");
            var row2 = rs2.FirstOrDefault();
            AssertTrue(row2 != null, "expected result row");
            AssertTrue(!row2.GetValue<bool>("[applied]"), "second INSERT IF NOT EXISTS should not be applied");
        });

        Test("update_row", () =>
        {
            session.Execute(
                "UPDATE " + KEYSPACE + ".users SET email = 'alice_updated@test.com' WHERE id = 1");
        });

        Test("delete_row", () =>
        {
            session.Execute(
                "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (99, 'ToDelete')");
            session.Execute(
                "DELETE FROM " + KEYSPACE + ".users WHERE id = 99");
            var rs = session.Execute(
                "SELECT * FROM " + KEYSPACE + ".users WHERE id = 99");
            var row = rs.FirstOrDefault();
            AssertTrue(row == null, "row should be deleted");
        });

        Test("insert_collections", () =>
        {
            session.Execute(
                "INSERT INTO " + KEYSPACE + ".collections (id, tags, scores, props) " +
                "VALUES (1, ['rust', 'cql', 'ferrosa'], {100, 200, 300}, " +
                "{'env': 'test', 'lang': 'csharp'})");
        });

        Test("insert_events", () =>
        {
            session.Execute(
                "INSERT INTO " + KEYSPACE + ".events (user_id, ts, data) " +
                "VALUES (1, '2024-01-01T00:00:00Z', 'login')");
            session.Execute(
                "INSERT INTO " + KEYSPACE + ".events (user_id, ts, data) " +
                "VALUES (1, '2024-01-01T01:00:00Z', 'logout')");
        });

        // ---- DML Reads ------------------------------------------------------

        Test("select_by_pk", () =>
        {
            var rs = session.Execute(
                "SELECT * FROM " + KEYSPACE + ".users WHERE id = 1");
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected 1 row");
            AssertEqual(row.GetValue<string>("name"), "Alice", "name");
        });

        Test("select_boolean", () =>
        {
            var rs = session.Execute(
                "SELECT active FROM " + KEYSPACE + ".users WHERE id = 2");
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected 1 row");
            AssertEqual(row.GetValue<bool>("active"), true, "active");
        });

        Test("select_float_double", () =>
        {
            var rs = session.Execute(
                "SELECT score, rating FROM " + KEYSPACE + ".users WHERE id = 3");
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected 1 row");
            AssertTrue(Math.Abs(row.GetValue<float>("score") - 95.5f) < 0.01f, "score mismatch");
            AssertTrue(Math.Abs(row.GetValue<double>("rating") - 99.12345678) < 0.0001, "rating mismatch");
        });

        Test("select_bigint", () =>
        {
            var rs = session.Execute(
                "SELECT age FROM " + KEYSPACE + ".users WHERE id = 4");
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected 1 row");
            AssertEqual(row.GetValue<long>("age"), 9223372036854775807L, "age");
        });

        Test("select_clustering_range", () =>
        {
            var rs = session.Execute(
                "SELECT data FROM " + KEYSPACE + ".events WHERE user_id = 1 ORDER BY ts ASC");
            var rows = rs.ToList();
            AssertEqual(rows.Count, 2, "row count");
            AssertEqual(rows[0].GetValue<string>("data"), "login", "first event");
            AssertEqual(rows[1].GetValue<string>("data"), "logout", "second event");
        });

        Test("select_count", () =>
        {
            var rs = session.Execute(
                "SELECT COUNT(*) FROM " + KEYSPACE + ".users");
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected result row");
            var count = row.GetValue<long>("count");
            AssertTrue(count >= 1, $"expected count >= 1, got {count}");
        });

        Test("select_limit", () =>
        {
            var rs = session.Execute(
                "SELECT * FROM " + KEYSPACE + ".users LIMIT 2");
            var rows = rs.ToList();
            AssertTrue(rows.Count <= 2, $"expected at most 2 rows, got {rows.Count}");
        });

        Test("select_collections", () =>
        {
            var rs = session.Execute(
                "SELECT tags, scores, props FROM " + KEYSPACE + ".collections WHERE id = 1");
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected 1 row");

            var tags = row.GetValue<List<string>>("tags");
            AssertTrue(tags != null, "tags should not be null");
            AssertEqual(tags.Count, 3, "tags count");
            AssertTrue(tags.Contains("rust"), "tags should contain 'rust'");
            AssertTrue(tags.Contains("cql"), "tags should contain 'cql'");
            AssertTrue(tags.Contains("ferrosa"), "tags should contain 'ferrosa'");

            var scores = row.GetValue<SortedSet<int>>("scores");
            AssertTrue(scores != null, "scores should not be null");
            AssertEqual(scores.Count, 3, "scores count");
            AssertTrue(scores.Contains(100), "scores should contain 100");
            AssertTrue(scores.Contains(200), "scores should contain 200");
            AssertTrue(scores.Contains(300), "scores should contain 300");

            // The C# driver materializes a CQL map as a SortedDictionary
            // (maps are key-ordered), not a plain Dictionary.
            var props = row.GetValue<SortedDictionary<string, string>>("props");
            AssertTrue(props != null, "props should not be null");
            AssertEqual(props.Count, 2, "props count");
            AssertEqual(props["env"], "test", "props['env']");
            AssertEqual(props["lang"], "csharp", "props['lang']");
        });

        Test("select_after_update", () =>
        {
            var rs = session.Execute(
                "SELECT email FROM " + KEYSPACE + ".users WHERE id = 1");
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected 1 row");
            AssertEqual(row.GetValue<string>("email"), "alice_updated@test.com", "email after update");
        });

        Test("select_after_delete", () =>
        {
            var rs = session.Execute(
                "SELECT * FROM " + KEYSPACE + ".users WHERE id = 99");
            var row = rs.FirstOrDefault();
            AssertTrue(row == null, "row 99 should still be deleted");
        });

        // ---- Prepared Statements --------------------------------------------

        Test("prepared_insert", () =>
        {
            var ps = session.Prepare(
                "INSERT INTO " + KEYSPACE + ".users (id, name, email) VALUES (?, ?, ?)");
            session.Execute(ps.Bind(100, "CSharpPrepared", "csharp-prepared@test.com"));
        });

        Test("prepared_select", () =>
        {
            var ps = session.Prepare(
                "SELECT name, email FROM " + KEYSPACE + ".users WHERE id = ?");
            var rs = session.Execute(ps.Bind(100));
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected 1 row");
            AssertEqual(row.GetValue<string>("name"), "CSharpPrepared", "name");
            AssertEqual(row.GetValue<string>("email"), "csharp-prepared@test.com", "email");
        });

        Test("prepared_update", () =>
        {
            var ps = session.Prepare(
                "UPDATE " + KEYSPACE + ".users SET email = ? WHERE id = ?");
            session.Execute(ps.Bind("csharp-updated@test.com", 100));

            var rs = session.Execute(
                "SELECT email FROM " + KEYSPACE + ".users WHERE id = 100");
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected 1 row");
            AssertEqual(row.GetValue<string>("email"), "csharp-updated@test.com", "email after prepared update");
        });

        Test("prepared_delete", () =>
        {
            var ps = session.Prepare(
                "DELETE FROM " + KEYSPACE + ".users WHERE id = ?");
            session.Execute(ps.Bind(100));

            var rs = session.Execute(
                "SELECT * FROM " + KEYSPACE + ".users WHERE id = 100");
            var row = rs.FirstOrDefault();
            AssertTrue(row == null, "row 100 should be deleted after prepared delete");
        });

        // ---- Batch ----------------------------------------------------------

        Test("batch_insert", () =>
        {
            var batch = new BatchStatement();
            var ps = session.Prepare(
                "INSERT INTO " + KEYSPACE + ".users (id, name, email) VALUES (?, ?, ?)");
            batch.Add(ps.Bind(201, "Batch1", "batch1@test.com"));
            batch.Add(ps.Bind(202, "Batch2", "batch2@test.com"));
            batch.Add(ps.Bind(203, "Batch3", "batch3@test.com"));
            session.Execute(batch);

            var rs = session.Execute(
                "SELECT name FROM " + KEYSPACE + ".users WHERE id = 201");
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected batch row 201");
            AssertEqual(row.GetValue<string>("name"), "Batch1", "batch row 201 name");

            rs = session.Execute(
                "SELECT name FROM " + KEYSPACE + ".users WHERE id = 202");
            row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected batch row 202");
            AssertEqual(row.GetValue<string>("name"), "Batch2", "batch row 202 name");

            rs = session.Execute(
                "SELECT name FROM " + KEYSPACE + ".users WHERE id = 203");
            row = rs.FirstOrDefault();
            AssertTrue(row != null, "expected batch row 203");
            AssertEqual(row.GetValue<string>("name"), "Batch3", "batch row 203 name");
        });

        // ---- TTL ------------------------------------------------------------

        Test("insert_with_ttl", () =>
        {
            session.Execute(
                "INSERT INTO " + KEYSPACE + ".users (id, name) VALUES (300, 'TTLUser') USING TTL 1");

            // Verify the row exists immediately
            var rs = session.Execute(
                "SELECT name FROM " + KEYSPACE + ".users WHERE id = 300");
            var row = rs.FirstOrDefault();
            AssertTrue(row != null, "TTL row should exist immediately after insert");
            AssertEqual(row.GetValue<string>("name"), "TTLUser", "TTL row name");

            // Wait for TTL to expire
            Thread.Sleep(2000);

            // Verify the row is gone
            rs = session.Execute(
                "SELECT * FROM " + KEYSPACE + ".users WHERE id = 300");
            row = rs.FirstOrDefault();
            AssertTrue(row == null, "TTL row should be gone after expiry");
        });

        // ---- Error Handling -------------------------------------------------

        Test("query_nonexistent_table", () =>
        {
            AssertThrows<Exception>(() =>
            {
                session.Execute("SELECT * FROM " + KEYSPACE + ".nonexistent_table_xyz");
            }, "query nonexistent table");
        });

        Test("invalid_cql_syntax", () =>
        {
            AssertThrows<Exception>(() =>
            {
                session.Execute("SELECTT BOGUS SYNTAX HERE");
            }, "invalid CQL syntax");
        });

        // ---- Cleanup --------------------------------------------------------

        Test("drop_keyspace", () =>
        {
            session.Execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        });

        cluster.Shutdown();

        // ---- Report ---------------------------------------------------------

        Console.WriteLine("\n=========================");
        Console.WriteLine($"Results: {passed} passed, {failed} failed");
        Console.WriteLine("=========================");

        Environment.Exit(failed > 0 ? 1 : 0);
    }
}
