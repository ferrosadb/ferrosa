/**
 * Ferrosa-Jepsen workload generator — Node.js driver.
 *
 * Connects to a Ferrosa/Cassandra cluster via CQL and runs register, bank,
 * or LWT workload patterns, recording operation history as JSONL.
 */

"use strict";

const cassandra = require("cassandra-driver");
const fs = require("fs");
const path = require("path");

const NUM_ACCOUNTS = 10;
const INITIAL_BALANCE = 1000;

let stopping = false;

function nowUS() {
  return Math.floor(Date.now() * 1000);
}

function writeOp(stream, clientId, invokeUs, completeUs, op, result) {
  const line = JSON.stringify({
    client_id: clientId,
    invoke_us: invokeUs,
    complete_us: completeUs,
    op,
    result,
  });
  stream.write(line + "\n");
}

function resultFromErr(err) {
  if (!err) return "Ok";
  const msg = String(err.message || err);
  if (msg.toLowerCase().includes("timeout")) return "Timeout";
  return { Err: msg };
}

function parseArgs() {
  const args = {};
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i].replace(/^--/, "");
    args[key] = argv[i + 1];
  }
  return {
    contactPoints: (args["contact-points"] || "").split(",").map((s) => s.trim()),
    workload: args["workload"] || "",
    duration: parseInt(args["duration"] || "60", 10),
    threads: parseInt(args["threads"] || "4", 10),
    outputDir: args["output-dir"] || "",
    clientId: args["client-id"] || "node",
  };
}

// ---------------------------------------------------------------------------
// Schema setup
// ---------------------------------------------------------------------------

const CREATE_KS =
  "CREATE KEYSPACE IF NOT EXISTS jepsen " +
  "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}";

async function setupRegister(client) {
  await client.execute(CREATE_KS);
  await client.execute(
    "CREATE TABLE IF NOT EXISTS jepsen.register (id int PRIMARY KEY, val int)"
  );
  await client.execute("INSERT INTO jepsen.register (id, val) VALUES (0, 0)");
}

async function setupBank(client) {
  await client.execute(CREATE_KS);
  await client.execute(
    "CREATE TABLE IF NOT EXISTS jepsen.accounts (id int PRIMARY KEY, balance bigint)"
  );
  for (let i = 0; i < NUM_ACCOUNTS; i++) {
    await client.execute(
      `INSERT INTO jepsen.accounts (id, balance) VALUES (${i}, ${INITIAL_BALANCE})`
    );
  }
}

async function setupLWT(client, num) {
  await client.execute(CREATE_KS);
  await client.execute(
    `CREATE TABLE IF NOT EXISTS jepsen.lwt${num} (id text PRIMARY KEY, val text)`
  );
}

// ---------------------------------------------------------------------------
// Workload runners
// ---------------------------------------------------------------------------

async function runRegister(client, stream, clientId, durationSec) {
  const deadline = Date.now() + durationSec * 1000;
  let counter = 1;

  while (Date.now() < deadline && !stopping) {
    const r = Math.random();

    if (r < 0.5) {
      // Read
      const op = { Read: { key: "0" } };
      const invoke = nowUS();
      try {
        const rs = await client.execute(
          "SELECT val FROM jepsen.register WHERE id = 0"
        );
        const complete = nowUS();
        const row = rs.first();
        const val = row ? row.val : null;
        writeOp(stream, clientId, invoke, complete, op, { Value: val });
      } catch (err) {
        writeOp(stream, clientId, invoke, nowUS(), op, resultFromErr(err));
      }
    } else if (r < 0.8) {
      // Write
      const op = { Write: { key: "0", value: counter } };
      const invoke = nowUS();
      try {
        await client.execute(
          `UPDATE jepsen.register SET val = ${counter} WHERE id = 0`
        );
        writeOp(stream, clientId, invoke, nowUS(), op, "Ok");
      } catch (err) {
        writeOp(stream, clientId, invoke, nowUS(), op, resultFromErr(err));
      }
      counter++;
    } else {
      // CAS
      const expected = counter - 1;
      const op = { Cas: { key: "0", expected, value: counter } };
      const invoke = nowUS();
      try {
        const rs = await client.execute(
          `UPDATE jepsen.register SET val = ${counter} WHERE id = 0 IF val = ${expected}`
        );
        const complete = nowUS();
        const row = rs.first();
        const applied = row ? row["[applied]"] === true : false;
        writeOp(stream, clientId, invoke, complete, op, { Applied: applied });
      } catch (err) {
        writeOp(stream, clientId, invoke, nowUS(), op, resultFromErr(err));
      }
      counter++;
    }
  }
}

async function runBank(client, stream, clientId, durationSec) {
  const deadline = Date.now() + durationSec * 1000;

  while (Date.now() < deadline && !stopping) {
    const r = Math.random();

    if (r < 0.7) {
      // Transfer
      const fromId = Math.floor(Math.random() * NUM_ACCOUNTS);
      let toId = Math.floor(Math.random() * NUM_ACCOUNTS);
      if (toId === fromId) toId = (fromId + 1) % NUM_ACCOUNTS;
      const amount = Math.floor(Math.random() * 100) + 1;

      // Read source
      const readOp = { Read: { key: `account-${fromId}` } };
      const readInvoke = nowUS();
      let balance;
      try {
        const rs = await client.execute(
          `SELECT balance FROM jepsen.accounts WHERE id = ${fromId}`
        );
        const complete = nowUS();
        const row = rs.first();
        balance = row ? Number(row.balance) : null;
        writeOp(stream, clientId, readInvoke, complete, readOp, {
          Value: balance,
        });
      } catch (err) {
        writeOp(stream, clientId, readInvoke, nowUS(), readOp, resultFromErr(err));
        continue;
      }

      if (balance === null || balance < amount) continue;

      // CAS debit
      const newBalance = balance - amount;
      const casOp = {
        Cas: {
          key: `account-${fromId}`,
          expected: balance,
          value: newBalance,
        },
      };
      const casInvoke = nowUS();
      let applied = false;
      try {
        const rs = await client.execute(
          `UPDATE jepsen.accounts SET balance = ${newBalance} WHERE id = ${fromId} IF balance = ${balance}`
        );
        const complete = nowUS();
        const row = rs.first();
        applied = row ? row["[applied]"] === true : false;
        writeOp(stream, clientId, casInvoke, complete, casOp, {
          Applied: applied,
        });
      } catch (err) {
        writeOp(stream, clientId, casInvoke, nowUS(), casOp, resultFromErr(err));
        continue;
      }
      if (!applied) continue;

      // Credit
      const creditOp = { Write: { key: `account-${toId}`, value: amount } };
      const creditInvoke = nowUS();
      try {
        await client.execute(
          `UPDATE jepsen.accounts SET balance = balance + ${amount} WHERE id = ${toId}`
        );
        writeOp(stream, clientId, creditInvoke, nowUS(), creditOp, "Ok");
      } catch (err) {
        writeOp(stream, clientId, creditInvoke, nowUS(), creditOp, resultFromErr(err));
      }
    } else {
      // Read all balances
      const op = { SerialRead: { key: "all-accounts" } };
      const invoke = nowUS();
      const values = [];
      let hadError = false;
      for (let i = 0; i < NUM_ACCOUNTS; i++) {
        try {
          const rs = await client.execute(
            `SELECT balance FROM jepsen.accounts WHERE id = ${i}`
          );
          const row = rs.first();
          const val = row ? String(row.balance) : "0";
          values.push([`account-${i}`, val]);
        } catch (err) {
          writeOp(stream, clientId, invoke, nowUS(), op, resultFromErr(err));
          hadError = true;
          break;
        }
      }
      if (!hadError) {
        writeOp(stream, clientId, invoke, nowUS(), op, {
          CurrentValues: values,
        });
      }
    }
  }
}

async function runLWT(client, stream, clientId, durationSec, patternNum) {
  const table = `jepsen.lwt${patternNum}`;
  const deadline = Date.now() + durationSec * 1000;
  let seq = 0;

  while (Date.now() < deadline && !stopping) {
    if (patternNum === 1 || patternNum === 4 || patternNum === 8) {
      // INSERT IF NOT EXISTS
      const val = `v${seq}`;
      const op = {
        InsertIfNotExists: {
          table,
          pk: "pk-0",
          values: [["val", val]],
        },
      };
      const invoke = nowUS();
      try {
        const rs = await client.execute(
          `INSERT INTO ${table} (id, val) VALUES ('pk-0', '${val}') IF NOT EXISTS`
        );
        const complete = nowUS();
        const row = rs.first();
        const applied = row ? row["[applied]"] === true : false;
        writeOp(stream, clientId, invoke, complete, op, { Applied: applied });
      } catch (err) {
        writeOp(stream, clientId, invoke, nowUS(), op, resultFromErr(err));
      }
    } else if (patternNum === 3) {
      // DELETE IF
      const op = {
        DeleteIf: { table, pk: "pk-0", condition: "val IS NOT NULL" },
      };
      const invoke = nowUS();
      try {
        const rs = await client.execute(
          `DELETE FROM ${table} WHERE id = 'pk-0' IF EXISTS`
        );
        const complete = nowUS();
        const row = rs.first();
        const applied = row ? row["[applied]"] === true : false;
        writeOp(stream, clientId, invoke, complete, op, { Applied: applied });
      } catch (err) {
        writeOp(stream, clientId, invoke, nowUS(), op, resultFromErr(err));
      }
    } else {
      // UPDATE IF (default)
      const expected = seq;
      const newVal = seq + 1;
      const op = {
        UpdateIf: {
          table,
          pk: "pk-0",
          condition: `val = ${expected}`,
          assignments: [["val", String(newVal)]],
        },
      };
      const invoke = nowUS();
      try {
        const rs = await client.execute(
          `UPDATE ${table} SET val = '${newVal}' WHERE id = 'pk-0' IF val = '${expected}'`
        );
        const complete = nowUS();
        const row = rs.first();
        const applied = row ? row["[applied]"] === true : false;
        writeOp(stream, clientId, invoke, complete, op, { Applied: applied });
        if (applied) seq = newVal;
      } catch (err) {
        writeOp(stream, clientId, invoke, nowUS(), op, resultFromErr(err));
      }
    }
    seq++;
  }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

async function worker(contactPoints, workload, durationSec, outputDir, clientId, idx) {
  const tid = `${clientId}-${idx}`;
  const client = new cassandra.Client({
    contactPoints,
    localDataCenter: "datacenter1",
    socketOptions: { readTimeout: 10000 },
  });
  await client.connect();

  const filePath = path.join(outputDir, `${tid}.jsonl`);
  const stream = fs.createWriteStream(filePath, { flags: "w" });

  try {
    if (workload === "register") {
      await runRegister(client, stream, tid, durationSec);
    } else if (workload === "bank") {
      await runBank(client, stream, tid, durationSec);
    } else if (workload.startsWith("lwt-")) {
      const num = parseInt(workload.split("-")[1], 10);
      await runLWT(client, stream, tid, durationSec, num);
    }
  } finally {
    stream.end();
    await client.shutdown();
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const args = parseArgs();

  if (!args.contactPoints.length || !args.workload || !args.outputDir) {
    console.error("Required: --contact-points, --workload, --output-dir");
    process.exit(1);
  }

  fs.mkdirSync(args.outputDir, { recursive: true });

  // Setup schema
  const setupClient = new cassandra.Client({
    contactPoints: args.contactPoints,
    localDataCenter: "datacenter1",
    socketOptions: { readTimeout: 30000 },
  });
  await setupClient.connect();

  if (args.workload === "register") {
    await setupRegister(setupClient);
  } else if (args.workload === "bank") {
    await setupBank(setupClient);
  } else if (args.workload.startsWith("lwt-")) {
    const num = parseInt(args.workload.split("-")[1], 10);
    await setupLWT(setupClient, num);
  } else {
    console.error(`Unknown workload: ${args.workload}`);
    process.exit(1);
  }
  await setupClient.shutdown();

  // Run workers concurrently
  const workers = [];
  for (let i = 0; i < args.threads; i++) {
    workers.push(
      worker(
        args.contactPoints,
        args.workload,
        args.duration,
        args.outputDir,
        args.clientId,
        i
      )
    );
  }

  await Promise.all(workers);
}

process.on("SIGTERM", () => {
  stopping = true;
});
process.on("SIGINT", () => {
  stopping = true;
});

main().catch((err) => {
  console.error(`Fatal: ${err}`);
  process.exit(1);
});
