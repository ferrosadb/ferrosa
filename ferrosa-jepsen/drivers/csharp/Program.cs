// Ferrosa-Jepsen workload generator -- C# driver.
//
// Connects to a Ferrosa/Cassandra cluster via CQL and runs register, bank,
// or LWT workload patterns, recording operation history as JSONL.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace FerrosJepsen;

public static class Program
{
    private const int NumAccounts = 10;
    private const long InitialBalance = 1000;
    private static readonly CancellationTokenSource Cts = new();

    private static long NowUS() =>
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000L;

    private static void WriteOp(StreamWriter w, string clientId, long invoke,
        long complete, object op, object result)
    {
        var record = new Dictionary<string, object>
        {
            ["client_id"] = clientId,
            ["invoke_us"] = invoke,
            ["complete_us"] = complete,
            ["op"] = op,
            ["result"] = result,
        };
        w.WriteLine(JsonSerializer.Serialize(record));
        w.Flush();
    }

    private static object ResultFromErr(Exception e)
    {
        var msg = e.Message ?? e.ToString();
        if (msg.Contains("timeout", StringComparison.OrdinalIgnoreCase))
            return "Timeout";
        return new Dictionary<string, string> { ["Err"] = msg };
    }

    // ---------------------------------------------------------------------------
    // Schema setup
    // ---------------------------------------------------------------------------

    private const string CreateKS =
        "CREATE KEYSPACE IF NOT EXISTS jepsen " +
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";

    private static void SetupRegister(ISession session)
    {
        session.Execute(CreateKS);
        session.Execute(
            "CREATE TABLE IF NOT EXISTS jepsen.register (id int PRIMARY KEY, val int)");
        session.Execute("INSERT INTO jepsen.register (id, val) VALUES (0, 0)");
    }

    private static void SetupBank(ISession session)
    {
        session.Execute(CreateKS);
        session.Execute(
            "CREATE TABLE IF NOT EXISTS jepsen.accounts (id int PRIMARY KEY, balance bigint)");
        for (int i = 0; i < NumAccounts; i++)
            session.Execute($"INSERT INTO jepsen.accounts (id, balance) VALUES ({i}, {InitialBalance})");
    }

    private static void SetupLWT(ISession session, int num)
    {
        session.Execute(CreateKS);
        session.Execute(
            $"CREATE TABLE IF NOT EXISTS jepsen.lwt{num} (id text PRIMARY KEY, val text)");
    }

    // ---------------------------------------------------------------------------
    // Workload runners
    // ---------------------------------------------------------------------------

    private static void RunRegister(ISession session, StreamWriter w, string clientId,
        int durationSec, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(durationSec);
        long counter = 1;
        var rng = new Random();

        while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            double r = rng.NextDouble();

            if (r < 0.5)
            {
                // Read
                var op = new Dictionary<string, object>
                {
                    ["Read"] = new Dictionary<string, string> { ["key"] = "0" }
                };
                long invoke = NowUS();
                try
                {
                    var rs = session.Execute("SELECT val FROM jepsen.register WHERE id = 0");
                    long complete = NowUS();
                    var row = rs.FirstOrDefault();
                    object val = row != null ? (object)row.GetValue<int>("val") : null;
                    WriteOp(w, clientId, invoke, complete, op,
                        new Dictionary<string, object> { ["Value"] = val });
                }
                catch (Exception e)
                {
                    WriteOp(w, clientId, invoke, NowUS(), op, ResultFromErr(e));
                }
            }
            else if (r < 0.8)
            {
                // Write
                var op = new Dictionary<string, object>
                {
                    ["Write"] = new Dictionary<string, object> { ["key"] = "0", ["value"] = counter }
                };
                long invoke = NowUS();
                try
                {
                    session.Execute($"UPDATE jepsen.register SET val = {counter} WHERE id = 0");
                    WriteOp(w, clientId, invoke, NowUS(), op, "Ok");
                }
                catch (Exception e)
                {
                    WriteOp(w, clientId, invoke, NowUS(), op, ResultFromErr(e));
                }
                counter++;
            }
            else
            {
                // CAS
                long expected = counter - 1;
                var op = new Dictionary<string, object>
                {
                    ["Cas"] = new Dictionary<string, object>
                    {
                        ["key"] = "0", ["expected"] = expected, ["value"] = counter
                    }
                };
                long invoke = NowUS();
                try
                {
                    var rs = session.Execute(
                        $"UPDATE jepsen.register SET val = {counter} WHERE id = 0 IF val = {expected}");
                    long complete = NowUS();
                    var row = rs.FirstOrDefault();
                    bool applied = row != null && row.GetValue<bool>("[applied]");
                    WriteOp(w, clientId, invoke, complete, op,
                        new Dictionary<string, object> { ["Applied"] = applied });
                }
                catch (Exception e)
                {
                    WriteOp(w, clientId, invoke, NowUS(), op, ResultFromErr(e));
                }
                counter++;
            }
        }
    }

    private static void RunBank(ISession session, StreamWriter w, string clientId,
        int durationSec, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(durationSec);
        var rng = new Random();

        while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            double r = rng.NextDouble();

            if (r < 0.7)
            {
                int fromId = rng.Next(NumAccounts);
                int toId = rng.Next(NumAccounts);
                if (toId == fromId) toId = (fromId + 1) % NumAccounts;
                long amount = rng.Next(1, 101);

                // Read source balance
                var readOp = new Dictionary<string, object>
                {
                    ["Read"] = new Dictionary<string, string> { ["key"] = $"account-{fromId}" }
                };
                long readInvoke = NowUS();
                long balance;
                try
                {
                    var rs = session.Execute(
                        $"SELECT balance FROM jepsen.accounts WHERE id = {fromId}");
                    long complete = NowUS();
                    var row = rs.FirstOrDefault();
                    if (row == null)
                    {
                        WriteOp(w, clientId, readInvoke, complete, readOp,
                            new Dictionary<string, object> { ["Value"] = null });
                        continue;
                    }
                    balance = row.GetValue<long>("balance");
                    WriteOp(w, clientId, readInvoke, complete, readOp,
                        new Dictionary<string, object> { ["Value"] = balance });
                }
                catch (Exception e)
                {
                    WriteOp(w, clientId, readInvoke, NowUS(), readOp, ResultFromErr(e));
                    continue;
                }

                if (balance < amount) continue;

                // CAS debit
                long newBalance = balance - amount;
                var casOp = new Dictionary<string, object>
                {
                    ["Cas"] = new Dictionary<string, object>
                    {
                        ["key"] = $"account-{fromId}",
                        ["expected"] = balance,
                        ["value"] = newBalance
                    }
                };
                long casInvoke = NowUS();
                bool applied;
                try
                {
                    var rs = session.Execute(
                        $"UPDATE jepsen.accounts SET balance = {newBalance} " +
                        $"WHERE id = {fromId} IF balance = {balance}");
                    long complete = NowUS();
                    var row = rs.FirstOrDefault();
                    applied = row != null && row.GetValue<bool>("[applied]");
                    WriteOp(w, clientId, casInvoke, complete, casOp,
                        new Dictionary<string, object> { ["Applied"] = applied });
                }
                catch (Exception e)
                {
                    WriteOp(w, clientId, casInvoke, NowUS(), casOp, ResultFromErr(e));
                    continue;
                }
                if (!applied) continue;

                // Credit destination
                var creditOp = new Dictionary<string, object>
                {
                    ["Write"] = new Dictionary<string, object>
                    {
                        ["key"] = $"account-{toId}", ["value"] = amount
                    }
                };
                long creditInvoke = NowUS();
                try
                {
                    session.Execute(
                        $"UPDATE jepsen.accounts SET balance = balance + {amount} WHERE id = {toId}");
                    WriteOp(w, clientId, creditInvoke, NowUS(), creditOp, "Ok");
                }
                catch (Exception e)
                {
                    WriteOp(w, clientId, creditInvoke, NowUS(), creditOp, ResultFromErr(e));
                }
            }
            else
            {
                // Read all balances
                var op = new Dictionary<string, object>
                {
                    ["SerialRead"] = new Dictionary<string, string> { ["key"] = "all-accounts" }
                };
                long invoke = NowUS();
                var values = new List<string[]>();
                bool hadError = false;
                for (int i = 0; i < NumAccounts; i++)
                {
                    try
                    {
                        var rs = session.Execute(
                            $"SELECT balance FROM jepsen.accounts WHERE id = {i}");
                        var row = rs.FirstOrDefault();
                        string val = row != null ? row.GetValue<long>("balance").ToString() : "0";
                        values.Add(new[] { $"account-{i}", val });
                    }
                    catch (Exception e)
                    {
                        WriteOp(w, clientId, invoke, NowUS(), op, ResultFromErr(e));
                        hadError = true;
                        break;
                    }
                }
                if (!hadError)
                {
                    WriteOp(w, clientId, invoke, NowUS(), op,
                        new Dictionary<string, object> { ["CurrentValues"] = values });
                }
            }
        }
    }

    private static void RunLWT(ISession session, StreamWriter w, string clientId,
        int durationSec, int patternNum, CancellationToken ct)
    {
        string table = $"jepsen.lwt{patternNum}";
        var deadline = DateTime.UtcNow.AddSeconds(durationSec);
        int seq = 0;

        while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            if (patternNum is 1 or 4 or 8)
            {
                // INSERT IF NOT EXISTS
                string val = $"v{seq}";
                var op = new Dictionary<string, object>
                {
                    ["InsertIfNotExists"] = new Dictionary<string, object>
                    {
                        ["table"] = table, ["pk"] = "pk-0",
                        ["values"] = new[] { new[] { "val", val } }
                    }
                };
                long invoke = NowUS();
                try
                {
                    var rs = session.Execute(
                        $"INSERT INTO {table} (id, val) VALUES ('pk-0', '{val}') IF NOT EXISTS");
                    long complete = NowUS();
                    var row = rs.FirstOrDefault();
                    bool applied = row != null && row.GetValue<bool>("[applied]");
                    WriteOp(w, clientId, invoke, complete, op,
                        new Dictionary<string, object> { ["Applied"] = applied });
                }
                catch (Exception e)
                {
                    WriteOp(w, clientId, invoke, NowUS(), op, ResultFromErr(e));
                }
            }
            else if (patternNum == 3)
            {
                // DELETE IF
                var op = new Dictionary<string, object>
                {
                    ["DeleteIf"] = new Dictionary<string, object>
                    {
                        ["table"] = table, ["pk"] = "pk-0",
                        ["condition"] = "val IS NOT NULL"
                    }
                };
                long invoke = NowUS();
                try
                {
                    var rs = session.Execute(
                        $"DELETE FROM {table} WHERE id = 'pk-0' IF EXISTS");
                    long complete = NowUS();
                    var row = rs.FirstOrDefault();
                    bool applied = row != null && row.GetValue<bool>("[applied]");
                    WriteOp(w, clientId, invoke, complete, op,
                        new Dictionary<string, object> { ["Applied"] = applied });
                }
                catch (Exception e)
                {
                    WriteOp(w, clientId, invoke, NowUS(), op, ResultFromErr(e));
                }
            }
            else
            {
                // UPDATE IF (default)
                int expected = seq;
                int newVal = seq + 1;
                var op = new Dictionary<string, object>
                {
                    ["UpdateIf"] = new Dictionary<string, object>
                    {
                        ["table"] = table, ["pk"] = "pk-0",
                        ["condition"] = $"val = {expected}",
                        ["assignments"] = new[] { new[] { "val", newVal.ToString() } }
                    }
                };
                long invoke = NowUS();
                try
                {
                    var rs = session.Execute(
                        $"UPDATE {table} SET val = '{newVal}' WHERE id = 'pk-0' IF val = '{expected}'");
                    long complete = NowUS();
                    var row = rs.FirstOrDefault();
                    bool applied = row != null && row.GetValue<bool>("[applied]");
                    WriteOp(w, clientId, invoke, complete, op,
                        new Dictionary<string, object> { ["Applied"] = applied });
                    if (applied) seq = newVal;
                }
                catch (Exception e)
                {
                    WriteOp(w, clientId, invoke, NowUS(), op, ResultFromErr(e));
                }
            }
            seq++;
        }
    }

    // ---------------------------------------------------------------------------
    // Main
    // ---------------------------------------------------------------------------

    public static async Task Main(string[] args)
    {
        string contactPointsStr = "";
        string workload = "";
        int duration = 60;
        int threads = 4;
        string outputDir = "";
        string clientId = "csharp";

        for (int i = 0; i < args.Length; i += 2)
        {
            if (i + 1 >= args.Length) break;
            switch (args[i])
            {
                case "--contact-points": contactPointsStr = args[i + 1]; break;
                case "--workload": workload = args[i + 1]; break;
                case "--duration": duration = int.Parse(args[i + 1]); break;
                case "--threads": threads = int.Parse(args[i + 1]); break;
                case "--output-dir": outputDir = args[i + 1]; break;
                case "--client-id": clientId = args[i + 1]; break;
            }
        }

        if (string.IsNullOrEmpty(contactPointsStr) || string.IsNullOrEmpty(workload) ||
            string.IsNullOrEmpty(outputDir))
        {
            Console.Error.WriteLine("Required: --contact-points, --workload, --output-dir");
            Environment.Exit(1);
        }

        Directory.CreateDirectory(outputDir);

        var contactPoints = contactPointsStr.Split(',').Select(s => s.Trim()).ToArray();

        // Setup schema
        var builder = Cluster.Builder();
        foreach (var cp in contactPoints)
            builder.AddContactPoint(cp);
        var cluster = builder.Build();
        var setupSession = cluster.Connect();

        switch (workload)
        {
            case "register": SetupRegister(setupSession); break;
            case "bank": SetupBank(setupSession); break;
            default:
                if (workload.StartsWith("lwt-"))
                {
                    int num = int.Parse(workload.Split('-')[1]);
                    SetupLWT(setupSession, num);
                }
                else
                {
                    Console.Error.WriteLine($"Unknown workload: {workload}");
                    Environment.Exit(1);
                }
                break;
        }
        setupSession.Dispose();
        cluster.Dispose();

        // Signal handling
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; Cts.Cancel(); };
        AppDomain.CurrentDomain.ProcessExit += (_, _) => Cts.Cancel();

        int patternNum = workload.StartsWith("lwt-")
            ? int.Parse(workload.Split('-')[1]) : 0;

        // Run workers
        var tasks = new List<Task>();
        for (int t = 0; t < threads; t++)
        {
            int idx = t;
            tasks.Add(Task.Run(() =>
            {
                string tid = $"{clientId}-{idx}";
                var wBuilder = Cluster.Builder();
                foreach (var cp in contactPoints)
                    wBuilder.AddContactPoint(cp);
                var wCluster = wBuilder.Build();
                var wSession = wCluster.Connect();

                string filePath = Path.Combine(outputDir, $"{tid}.jsonl");
                using var sw = new StreamWriter(filePath);

                try
                {
                    switch (workload)
                    {
                        case "register":
                            RunRegister(wSession, sw, tid, duration, Cts.Token); break;
                        case "bank":
                            RunBank(wSession, sw, tid, duration, Cts.Token); break;
                        default:
                            RunLWT(wSession, sw, tid, duration, patternNum, Cts.Token); break;
                    }
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine($"Worker {tid} error: {e.Message}");
                }
                finally
                {
                    wSession.Dispose();
                    wCluster.Dispose();
                }
            }));
        }

        await Task.WhenAll(tasks);
    }
}
