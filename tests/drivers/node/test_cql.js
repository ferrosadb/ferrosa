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

function assertDeepEqual(actual, expected, label) {
  const a = JSON.stringify(actual);
  const b = JSON.stringify(expected);
  if (a !== b) {
    throw new Error(
      `${label || "assertDeepEqual"}: expected ${b}, got ${a}`
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

  // ---- Collections ------------------------------------------------------

  await test("CREATE TABLE collections", async () => {
    await client.execute(`
      CREATE TABLE IF NOT EXISTS ${KEYSPACE}.collections (
        id int PRIMARY KEY,
        tags list<text>,
        scores set<int>,
        props map<text, text>
      )
    `);
  });

  await test("INSERT list", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.collections (id, tags) VALUES (?, ?)`,
      [1, ["tag1", "tag2", "tag3"]],
      { prepare: true }
    );
  });

  await test("SELECT list", async () => {
    const result = await client.execute(
      `SELECT tags FROM ${KEYSPACE}.collections WHERE id = ?`,
      [1],
      { prepare: true }
    );
    assertEqual(result.rows.length, 1, "row count");
    const tags = result.rows[0].tags;
    assertEqual(tags.length, 3, "list length");
    assertEqual(tags[0], "tag1", "tag[0]");
    assertEqual(tags[1], "tag2", "tag[1]");
    assertEqual(tags[2], "tag3", "tag[2]");
  });

  await test("INSERT set", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.collections (id, scores) VALUES (?, ?)`,
      [2, [10, 20, 30]],
      { prepare: true }
    );
  });

  await test("SELECT set", async () => {
    const result = await client.execute(
      `SELECT scores FROM ${KEYSPACE}.collections WHERE id = ?`,
      [2],
      { prepare: true }
    );
    assertEqual(result.rows.length, 1, "row count");
    const scores = result.rows[0].scores;
    // Sets are returned as arrays sorted by value
    assert(scores.length === 3, "set size should be 3");
    assert(scores.includes(10), "set should contain 10");
    assert(scores.includes(20), "set should contain 20");
    assert(scores.includes(30), "set should contain 30");
  });

  await test("INSERT map", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.collections (id, props) VALUES (?, ?)`,
      [3, { k1: "v1", k2: "v2" }],
      { prepare: true }
    );
  });

  await test("SELECT map", async () => {
    const result = await client.execute(
      `SELECT props FROM ${KEYSPACE}.collections WHERE id = ?`,
      [3],
      { prepare: true }
    );
    assertEqual(result.rows.length, 1, "row count");
    const props = result.rows[0].props;
    assertEqual(props.k1, "v1", "map[k1]");
    assertEqual(props.k2, "v2", "map[k2]");
  });

  // ---- ALTER / DELETE / UPDATE ------------------------------------------

  await test("ALTER TABLE add column", async () => {
    await client.execute(
      `ALTER TABLE ${KEYSPACE}.users ADD phone text`
    );
  });

  await test("DELETE row", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.users (id, name) VALUES (900, 'ToDelete')`
    );
    await client.execute(
      `DELETE FROM ${KEYSPACE}.users WHERE id = 900`
    );
    const result = await client.execute(
      `SELECT * FROM ${KEYSPACE}.users WHERE id = 900`
    );
    assertEqual(result.rows.length, 0, "row should be deleted");
  });

  await test("UPDATE row", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.users (id, name, email) VALUES (901, 'BeforeUpdate', 'old@test.com')`
    );
    await client.execute(
      `UPDATE ${KEYSPACE}.users SET email = 'new@test.com' WHERE id = 901`
    );
    const result = await client.execute(
      `SELECT email FROM ${KEYSPACE}.users WHERE id = 901`
    );
    assertEqual(result.rows.length, 1, "row count");
    assertEqual(result.rows[0].email, "new@test.com", "updated email");
  });

  await test("INSERT IF NOT EXISTS", async () => {
    // First insert should succeed
    await client.execute(
      `INSERT INTO ${KEYSPACE}.users (id, name) VALUES (902, 'LwtUser') IF NOT EXISTS`
    );
    // Second insert should not apply
    const result = await client.execute(
      `INSERT INTO ${KEYSPACE}.users (id, name) VALUES (902, 'LwtUserDup') IF NOT EXISTS`
    );
    const row = result.first();
    assertEqual(row["[applied]"], false, "[applied] should be false");
  });

  // ---- Batch ------------------------------------------------------------

  await test("batch INSERT", async () => {
    const queries = [
      {
        query: `INSERT INTO ${KEYSPACE}.users (id, name) VALUES (?, ?)`,
        params: [801, "BatchUser1"],
      },
      {
        query: `INSERT INTO ${KEYSPACE}.users (id, name) VALUES (?, ?)`,
        params: [802, "BatchUser2"],
      },
      {
        query: `INSERT INTO ${KEYSPACE}.users (id, name) VALUES (?, ?)`,
        params: [803, "BatchUser3"],
      },
    ];
    await client.batch(queries, { prepare: true });

    // Verify all three rows were inserted
    for (const [id, name] of [[801, "BatchUser1"], [802, "BatchUser2"], [803, "BatchUser3"]]) {
      const result = await client.execute(
        `SELECT name FROM ${KEYSPACE}.users WHERE id = ?`,
        [id],
        { prepare: true }
      );
      assertEqual(result.rows.length, 1, `batch row ${id} count`);
      assertEqual(result.rows[0].name, name, `batch row ${id} name`);
    }
  });

  // ---- TTL --------------------------------------------------------------

  await test("INSERT with TTL", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.users (id, name) VALUES (950, 'TtlUser') USING TTL 1`
    );
    // Wait for TTL to expire
    await new Promise((r) => setTimeout(r, 2000));
    const result = await client.execute(
      `SELECT * FROM ${KEYSPACE}.users WHERE id = 950`
    );
    assertEqual(result.rows.length, 0, "row should have expired via TTL");
  });

  // ---- Counts & Limits --------------------------------------------------

  await test("SELECT COUNT(*)", async () => {
    const result = await client.execute(
      `SELECT COUNT(*) FROM ${KEYSPACE}.users`
    );
    const count = result.rows[0].count.toInt
      ? result.rows[0].count.toInt()
      : Number(result.rows[0].count);
    assert(count > 0, `expected count > 0, got ${count}`);
  });

  await test("SELECT LIMIT", async () => {
    const result = await client.execute(
      `SELECT * FROM ${KEYSPACE}.users LIMIT 2`
    );
    assert(result.rows.length <= 2, `expected at most 2 rows, got ${result.rows.length}`);
  });

  // ---- Error handling ---------------------------------------------------

  await test("query nonexistent table", async () => {
    let caught = false;
    try {
      await client.execute(`SELECT * FROM ${KEYSPACE}.no_such_table`);
    } catch (err) {
      caught = true;
    }
    assert(caught, "expected error querying nonexistent table");
  });

  await test("invalid CQL syntax", async () => {
    let caught = false;
    try {
      await client.execute("NOT VALID CQL AT ALL");
    } catch (err) {
      caught = true;
    }
    assert(caught, "expected error for invalid CQL syntax");
  });

  // ---- System schema ----------------------------------------------------

  await test("system_schema.keyspaces", async () => {
    const result = await client.execute(
      "SELECT keyspace_name FROM system_schema.keyspaces"
    );
    const names = result.rows.map((r) => r.keyspace_name);
    assert(
      names.includes(KEYSPACE),
      `expected ${KEYSPACE} in system_schema.keyspaces, got: ${names.join(", ")}`
    );
  });

  // ---- NULL handling ----------------------------------------------------

  await test("INSERT NULL", async () => {
    await client.execute(
      `INSERT INTO ${KEYSPACE}.users (id, name) VALUES (960, null)`
    );
  });

  await test("SELECT NULL", async () => {
    const result = await client.execute(
      `SELECT name FROM ${KEYSPACE}.users WHERE id = 960`
    );
    assertEqual(result.rows.length, 1, "row count");
    assertEqual(result.rows[0].name, null, "name should be null");
  });

  // ---- Index ------------------------------------------------------------

  await test("CREATE INDEX", async () => {
    await client.execute(
      `CREATE INDEX IF NOT EXISTS ON ${KEYSPACE}.users (name)`
    );
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
