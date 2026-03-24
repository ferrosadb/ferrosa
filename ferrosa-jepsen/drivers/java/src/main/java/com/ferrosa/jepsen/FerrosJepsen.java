package com.ferrosa.jepsen;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ferrosa-Jepsen workload generator -- Java driver.
 *
 * Connects to a Ferrosa/Cassandra cluster via CQL and runs register, bank,
 * or LWT workload patterns, recording operation history as JSONL.
 */
public class FerrosJepsen {

    private static final int NUM_ACCOUNTS = 10;
    private static final long INITIAL_BALANCE = 1000L;
    private static final AtomicBoolean STOPPING = new AtomicBoolean(false);

    private static long nowUS() {
        return System.currentTimeMillis() * 1000L;
    }

    // ---------------------------------------------------------------------------
    // JSON helpers (no dependency on Jackson — keep it minimal)
    // ---------------------------------------------------------------------------

    private static String jsonStr(String s) {
        if (s == null) return "null";
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String jsonKV(String k, String v) {
        return jsonStr(k) + ":" + v;
    }

    private static String jsonObj(String... kvs) {
        return "{" + String.join(",", kvs) + "}";
    }

    private static String jsonArr(List<String> items) {
        return "[" + String.join(",", items) + "]";
    }

    private static void writeOp(BufferedWriter w, String clientId, long invoke,
                                 long complete, String op, String result) throws IOException {
        String line = jsonObj(
            jsonKV("client_id", jsonStr(clientId)),
            jsonKV("invoke_us", String.valueOf(invoke)),
            jsonKV("complete_us", String.valueOf(complete)),
            jsonKV("op", op),
            jsonKV("result", result)
        );
        w.write(line);
        w.newLine();
        w.flush();
    }

    private static String resultFromErr(Exception e) {
        String msg = e.getMessage() != null ? e.getMessage() : e.toString();
        if (msg.toLowerCase().contains("timeout")) {
            return jsonStr("Timeout");
        }
        return jsonObj(jsonKV("Err", jsonStr(msg)));
    }

    // ---------------------------------------------------------------------------
    // Schema setup
    // ---------------------------------------------------------------------------

    private static final String CREATE_KS =
        "CREATE KEYSPACE IF NOT EXISTS jepsen " +
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";

    private static void setupRegister(CqlSession session) {
        session.execute(CREATE_KS);
        session.execute("CREATE TABLE IF NOT EXISTS jepsen.register (id int PRIMARY KEY, val int)");
        session.execute("INSERT INTO jepsen.register (id, val) VALUES (0, 0)");
    }

    private static void setupBank(CqlSession session) {
        session.execute(CREATE_KS);
        session.execute("CREATE TABLE IF NOT EXISTS jepsen.accounts (id int PRIMARY KEY, balance bigint)");
        for (int i = 0; i < NUM_ACCOUNTS; i++) {
            session.execute("INSERT INTO jepsen.accounts (id, balance) VALUES (" + i + ", " + INITIAL_BALANCE + ")");
        }
    }

    private static void setupLWT(CqlSession session, int num) {
        session.execute(CREATE_KS);
        session.execute("CREATE TABLE IF NOT EXISTS jepsen.lwt" + num + " (id text PRIMARY KEY, val text)");
    }

    // ---------------------------------------------------------------------------
    // Workload runners
    // ---------------------------------------------------------------------------

    private static void runRegister(CqlSession session, BufferedWriter w, String clientId,
                                     int durationSec) throws IOException {
        long deadline = System.currentTimeMillis() + durationSec * 1000L;
        long counter = 1;

        while (System.currentTimeMillis() < deadline && !STOPPING.get()) {
            double r = ThreadLocalRandom.current().nextDouble();

            if (r < 0.5) {
                // Read
                String op = jsonObj(jsonKV("Read", jsonObj(jsonKV("key", jsonStr("0")))));
                long invoke = nowUS();
                try {
                    ResultSet rs = session.execute("SELECT val FROM jepsen.register WHERE id = 0");
                    long complete = nowUS();
                    Row row = rs.one();
                    if (row != null) {
                        int val = row.getInt("val");
                        writeOp(w, clientId, invoke, complete, op,
                            jsonObj(jsonKV("Value", String.valueOf(val))));
                    } else {
                        writeOp(w, clientId, invoke, complete, op,
                            jsonObj(jsonKV("Value", "null")));
                    }
                } catch (Exception e) {
                    writeOp(w, clientId, invoke, nowUS(), op, resultFromErr(e));
                }
            } else if (r < 0.8) {
                // Write
                String op = jsonObj(jsonKV("Write", jsonObj(
                    jsonKV("key", jsonStr("0")),
                    jsonKV("value", String.valueOf(counter))
                )));
                long invoke = nowUS();
                try {
                    session.execute("UPDATE jepsen.register SET val = " + counter + " WHERE id = 0");
                    writeOp(w, clientId, invoke, nowUS(), op, jsonStr("Ok"));
                } catch (Exception e) {
                    writeOp(w, clientId, invoke, nowUS(), op, resultFromErr(e));
                }
                counter++;
            } else {
                // CAS
                long expected = counter - 1;
                String op = jsonObj(jsonKV("Cas", jsonObj(
                    jsonKV("key", jsonStr("0")),
                    jsonKV("expected", String.valueOf(expected)),
                    jsonKV("value", String.valueOf(counter))
                )));
                long invoke = nowUS();
                try {
                    ResultSet rs = session.execute(
                        "UPDATE jepsen.register SET val = " + counter +
                        " WHERE id = 0 IF val = " + expected);
                    long complete = nowUS();
                    Row row = rs.one();
                    boolean applied = row != null && row.getBoolean("[applied]");
                    writeOp(w, clientId, invoke, complete, op,
                        jsonObj(jsonKV("Applied", String.valueOf(applied))));
                } catch (Exception e) {
                    writeOp(w, clientId, invoke, nowUS(), op, resultFromErr(e));
                }
                counter++;
            }
        }
    }

    private static void runBank(CqlSession session, BufferedWriter w, String clientId,
                                 int durationSec) throws IOException {
        long deadline = System.currentTimeMillis() + durationSec * 1000L;

        while (System.currentTimeMillis() < deadline && !STOPPING.get()) {
            double r = ThreadLocalRandom.current().nextDouble();

            if (r < 0.7) {
                int fromId = ThreadLocalRandom.current().nextInt(NUM_ACCOUNTS);
                int toId = ThreadLocalRandom.current().nextInt(NUM_ACCOUNTS);
                if (toId == fromId) toId = (fromId + 1) % NUM_ACCOUNTS;
                long amount = ThreadLocalRandom.current().nextLong(1, 101);

                // Read source balance
                String readOp = jsonObj(jsonKV("Read",
                    jsonObj(jsonKV("key", jsonStr("account-" + fromId)))));
                long readInvoke = nowUS();
                long balance;
                try {
                    ResultSet rs = session.execute(
                        "SELECT balance FROM jepsen.accounts WHERE id = " + fromId);
                    long complete = nowUS();
                    Row row = rs.one();
                    if (row == null) {
                        writeOp(w, clientId, readInvoke, complete, readOp,
                            jsonObj(jsonKV("Value", "null")));
                        continue;
                    }
                    balance = row.getLong("balance");
                    writeOp(w, clientId, readInvoke, complete, readOp,
                        jsonObj(jsonKV("Value", String.valueOf(balance))));
                } catch (Exception e) {
                    writeOp(w, clientId, readInvoke, nowUS(), readOp, resultFromErr(e));
                    continue;
                }

                if (balance < amount) continue;

                // CAS debit
                long newBalance = balance - amount;
                String casOp = jsonObj(jsonKV("Cas", jsonObj(
                    jsonKV("key", jsonStr("account-" + fromId)),
                    jsonKV("expected", String.valueOf(balance)),
                    jsonKV("value", String.valueOf(newBalance))
                )));
                long casInvoke = nowUS();
                boolean applied;
                try {
                    ResultSet rs = session.execute(
                        "UPDATE jepsen.accounts SET balance = " + newBalance +
                        " WHERE id = " + fromId + " IF balance = " + balance);
                    long complete = nowUS();
                    Row row = rs.one();
                    applied = row != null && row.getBoolean("[applied]");
                    writeOp(w, clientId, casInvoke, complete, casOp,
                        jsonObj(jsonKV("Applied", String.valueOf(applied))));
                } catch (Exception e) {
                    writeOp(w, clientId, casInvoke, nowUS(), casOp, resultFromErr(e));
                    continue;
                }
                if (!applied) continue;

                // Credit destination
                String creditOp = jsonObj(jsonKV("Write", jsonObj(
                    jsonKV("key", jsonStr("account-" + toId)),
                    jsonKV("value", String.valueOf(amount))
                )));
                long creditInvoke = nowUS();
                try {
                    session.execute(
                        "UPDATE jepsen.accounts SET balance = balance + " + amount +
                        " WHERE id = " + toId);
                    writeOp(w, clientId, creditInvoke, nowUS(), creditOp, jsonStr("Ok"));
                } catch (Exception e) {
                    writeOp(w, clientId, creditInvoke, nowUS(), creditOp, resultFromErr(e));
                }
            } else {
                // Read all balances
                String op = jsonObj(jsonKV("SerialRead",
                    jsonObj(jsonKV("key", jsonStr("all-accounts")))));
                long invoke = nowUS();
                List<String> values = new ArrayList<>();
                boolean hadError = false;
                for (int i = 0; i < NUM_ACCOUNTS; i++) {
                    try {
                        ResultSet rs = session.execute(
                            "SELECT balance FROM jepsen.accounts WHERE id = " + i);
                        Row row = rs.one();
                        String val = row != null ? String.valueOf(row.getLong("balance")) : "0";
                        values.add("[" + jsonStr("account-" + i) + "," + jsonStr(val) + "]");
                    } catch (Exception e) {
                        writeOp(w, clientId, invoke, nowUS(), op, resultFromErr(e));
                        hadError = true;
                        break;
                    }
                }
                if (!hadError) {
                    String result = jsonObj(jsonKV("CurrentValues", "[" + String.join(",", values) + "]"));
                    writeOp(w, clientId, invoke, nowUS(), op, result);
                }
            }
        }
    }

    private static void runLWT(CqlSession session, BufferedWriter w, String clientId,
                                int durationSec, int patternNum) throws IOException {
        String table = "jepsen.lwt" + patternNum;
        long deadline = System.currentTimeMillis() + durationSec * 1000L;
        int seq = 0;

        while (System.currentTimeMillis() < deadline && !STOPPING.get()) {
            if (patternNum == 1 || patternNum == 4 || patternNum == 8) {
                // INSERT IF NOT EXISTS
                String val = "v" + seq;
                String op = jsonObj(jsonKV("InsertIfNotExists", jsonObj(
                    jsonKV("table", jsonStr(table)),
                    jsonKV("pk", jsonStr("pk-0")),
                    jsonKV("values", "[[" + jsonStr("val") + "," + jsonStr(val) + "]]")
                )));
                long invoke = nowUS();
                try {
                    ResultSet rs = session.execute(
                        "INSERT INTO " + table + " (id, val) VALUES ('pk-0', '" + val + "') IF NOT EXISTS");
                    long complete = nowUS();
                    Row row = rs.one();
                    boolean applied = row != null && row.getBoolean("[applied]");
                    writeOp(w, clientId, invoke, complete, op,
                        jsonObj(jsonKV("Applied", String.valueOf(applied))));
                } catch (Exception e) {
                    writeOp(w, clientId, invoke, nowUS(), op, resultFromErr(e));
                }
            } else if (patternNum == 3) {
                // DELETE IF
                String op = jsonObj(jsonKV("DeleteIf", jsonObj(
                    jsonKV("table", jsonStr(table)),
                    jsonKV("pk", jsonStr("pk-0")),
                    jsonKV("condition", jsonStr("val IS NOT NULL"))
                )));
                long invoke = nowUS();
                try {
                    ResultSet rs = session.execute(
                        "DELETE FROM " + table + " WHERE id = 'pk-0' IF EXISTS");
                    long complete = nowUS();
                    Row row = rs.one();
                    boolean applied = row != null && row.getBoolean("[applied]");
                    writeOp(w, clientId, invoke, complete, op,
                        jsonObj(jsonKV("Applied", String.valueOf(applied))));
                } catch (Exception e) {
                    writeOp(w, clientId, invoke, nowUS(), op, resultFromErr(e));
                }
            } else {
                // UPDATE IF (default)
                int expected = seq;
                int newVal = seq + 1;
                String op = jsonObj(jsonKV("UpdateIf", jsonObj(
                    jsonKV("table", jsonStr(table)),
                    jsonKV("pk", jsonStr("pk-0")),
                    jsonKV("condition", jsonStr("val = " + expected)),
                    jsonKV("assignments", "[[" + jsonStr("val") + "," + jsonStr(String.valueOf(newVal)) + "]]")
                )));
                long invoke = nowUS();
                try {
                    ResultSet rs = session.execute(
                        "UPDATE " + table + " SET val = '" + newVal +
                        "' WHERE id = 'pk-0' IF val = '" + expected + "'");
                    long complete = nowUS();
                    Row row = rs.one();
                    boolean applied = row != null && row.getBoolean("[applied]");
                    writeOp(w, clientId, invoke, complete, op,
                        jsonObj(jsonKV("Applied", String.valueOf(applied))));
                    if (applied) seq = newVal;
                } catch (Exception e) {
                    writeOp(w, clientId, invoke, nowUS(), op, resultFromErr(e));
                }
            }
            seq++;
        }
    }

    // ---------------------------------------------------------------------------
    // Main
    // ---------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        String contactPointsStr = "";
        String workload = "";
        int duration = 60;
        int threads = 4;
        String outputDir = "";
        String clientId = "java";

        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 >= args.length) break;
            switch (args[i]) {
                case "--contact-points" -> contactPointsStr = args[i + 1];
                case "--workload" -> workload = args[i + 1];
                case "--duration" -> duration = Integer.parseInt(args[i + 1]);
                case "--threads" -> threads = Integer.parseInt(args[i + 1]);
                case "--output-dir" -> outputDir = args[i + 1];
                case "--client-id" -> clientId = args[i + 1];
            }
        }

        if (contactPointsStr.isEmpty() || workload.isEmpty() || outputDir.isEmpty()) {
            System.err.println("Required: --contact-points, --workload, --output-dir");
            System.exit(1);
        }

        Files.createDirectories(Paths.get(outputDir));

        // Parse contact points
        List<InetSocketAddress> contactPoints = new ArrayList<>();
        for (String cp : contactPointsStr.split(",")) {
            cp = cp.trim();
            contactPoints.add(new InetSocketAddress(cp, 9042));
        }

        // Setup schema
        try (CqlSession session = CqlSession.builder()
                .addContactPoints(contactPoints)
                .withLocalDatacenter("datacenter1")
                .build()) {

            switch (workload) {
                case "register" -> setupRegister(session);
                case "bank" -> setupBank(session);
                default -> {
                    if (workload.startsWith("lwt-")) {
                        int num = Integer.parseInt(workload.split("-")[1]);
                        setupLWT(session, num);
                    } else {
                        System.err.println("Unknown workload: " + workload);
                        System.exit(1);
                    }
                }
            }
        }

        // Signal handling
        Runtime.getRuntime().addShutdownHook(new Thread(() -> STOPPING.set(true)));

        // Run workers
        int numThreads = threads;
        int dur = duration;
        String wl = workload;
        String outDir = outputDir;
        String cid = clientId;
        int patternNum = workload.startsWith("lwt-") ?
            Integer.parseInt(workload.split("-")[1]) : 0;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int t = 0; t < numThreads; t++) {
            int idx = t;
            executor.submit(() -> {
                String tid = cid + "-" + idx;
                try (CqlSession workerSession = CqlSession.builder()
                        .addContactPoints(contactPoints)
                        .withLocalDatacenter("datacenter1")
                        .build()) {

                    Path filePath = Paths.get(outDir, tid + ".jsonl");
                    try (BufferedWriter w = Files.newBufferedWriter(filePath)) {
                        switch (wl) {
                            case "register" -> runRegister(workerSession, w, tid, dur);
                            case "bank" -> runBank(workerSession, w, tid, dur);
                            default -> runLWT(workerSession, w, tid, dur, patternNum);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Worker " + tid + " error: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }
}
