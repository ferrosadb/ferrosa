/**
 * CQL driver smoke tests using the DataStax Node.js cassandra-driver.
 *
 * Each test is idempotent and uses the "node_test" keyspace.
 * Exits with code 0 on success, 1 on any failure.
 */

"use strict";

const cassandra = require("cassandra-driver");

const FERROSA_HOST = process.env.FERROSA_HOST || "127.0.0.1";
const FERROSA_CQL_PORT = parseInt(process.env.FERROSA_CQL_PORT || "9042", 10);
const KEYSPACE = "node_test";

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    console.log(`  PASS  ${name}`);
    passed++;
  } catch (err) {
    console.error(`  FAIL  ${name}`);
    console.error(`        ${err.message}`);
    failed++;
  }
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message || "assertion failed");
  }
}

function assertEqual(actual, expected, label) {
  if (actual !== expected) {
    throw new Error(
      `${label || "assertEqual"}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`
    );
  }
}

async function main() {
  console.log("Node.js CQL driver smoke tests");
  console.log("==============================\n");

  // ---- Connect ----------------------------------------------------------

  const client = new cassandra.Client({
    contactPoints: [FERROSA_HOST],
    protocolOptions: { port: FERROSA_CQL_PORT },
    localDataCenter: "datacenter1",
    // Node.js driver negotiates protocol version automatically
  });

  await test("connect", async () => {
    await client.connect();
  });

  // ---- Introspection ----------------------------------------------------

  await test("system.local", async () => {
    const result = await client.execute(
      "SELECT cluster_name, data_center FROM system.local"
    );
    assert(result.rows.length >= 1, "expected at least 1 row");
    assert(result.rows[0].cluster_name, "cluster_name should not be null");
  });

  await test("system.peers", async () => {
    const result = await client.execute("SELECT * FROM system.peers");
    assert(result !== null, "result should not be null");
  });

  // ---- DDL --------------------------------------------------------------

  await test("CREATE KEYSPACE", async () => {
    await client.execute(
      `CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE} ` +
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    );
  });

  await test("CREATE TABLE users", async () => {
    await client.execute(`
      CREATE TABLE IF NOT EXISTS ${KEYSPACE}.users (
        id int PRIMARY KEY,
        name text,
        email text,
        active boolean,
        score float,
        rating double,
        age bigint
      )
    `);
  });

  await test("CREATE TABLE events", async () => {
    await client.execute(`
      CREATE TABLE IF NOT EXISTS ${KEYSPACE}.events (
        user_id int,
        ts timestamp,
        data text,
        PRIMARY KEY (user_id, ts)
      )
    `);
  });

  // ---- DML writes -------------------------------------------------------

  await test("INSERT text, int", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')`
    );
  });

  await test("INSERT boolean", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.users (id, active) VALUES (2, true)`
    );
  });

  await test("INSERT float, double", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.users (id, score, rating) VALUES (3, 95.5, 99.12345678)`
    );
  });

  await test("INSERT bigint", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.users (id, age) VALUES (4, 9223372036854775807)`
    );
  });

  await test("INSERT events", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.events (user_id, ts, data) VALUES (1, '2024-01-01T00:00:00Z', 'login')`
    );
    await client.execute(
      `INSERT INTO ${KEYSPACE}.events (user_id, ts, data) VALUES (1, '2024-01-01T01:00:00Z', 'logout')`
    );
  });

  // ---- DML reads --------------------------------------------------------

  await test("SELECT by PK", async () => {
    const result = await client.execute(
      `SELECT * FROM ${KEYSPACE}.users WHERE id = 1`
    );
    assertEqual(result.rows.length, 1, "row count");
    assertEqual(result.rows[0].name, "Alice", "name");
    assertEqual(result.rows[0].email, "alice@test.com", "email");
  });

  await test("SELECT boolean", async () => {
    const result = await client.execute(
      `SELECT active FROM ${KEYSPACE}.users WHERE id = 2`
    );
    assertEqual(result.rows.length, 1, "row count");
    assertEqual(result.rows[0].active, true, "active");
  });

  await test("SELECT clustering range", async () => {
    const result = await client.execute(
      `SELECT data FROM ${KEYSPACE}.events WHERE user_id = 1 ORDER BY ts ASC`
    );
    assertEqual(result.rows.length, 2, "row count");
    assertEqual(result.rows[0].data, "login", "first event");
    assertEqual(result.rows[1].data, "logout", "second event");
  });

  // ---- Prepared statements ----------------------------------------------

  await test("prepared INSERT", async () => {
    const query = `INSERT INTO ${KEYSPACE}.users (id, name, email) VALUES (?, ?, ?)`;
    await client.execute(query, [100, "NodePrepared", "node-prepared@test.com"], {
      prepare: true,
    });
  });

  await test("prepared SELECT", async () => {
    const query = `SELECT name FROM ${KEYSPACE}.users WHERE id = ?`;
    const result = await client.execute(query, [100], { prepare: true });
    assertEqual(result.rows.length, 1, "row count");
    assertEqual(result.rows[0].name, "NodePrepared", "name");
  });

  // ---- Cleanup ----------------------------------------------------------

  await test("DROP KEYSPACE", async () => {
    await client.execute(`DROP KEYSPACE IF EXISTS ${KEYSPACE}`);
  });

  await client.shutdown();

  // ---- Report -----------------------------------------------------------

  console.log(`\n==============================`);
  console.log(`Results: ${passed} passed, ${failed} failed`);
  console.log(`==============================`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
