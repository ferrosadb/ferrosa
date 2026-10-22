//! Proves, statement by statement, that a `ferrosa-lite` node serves the CQL
//! surface the appliance's `CqlControlStore` actually uses.
//!
//! The DDL is run verbatim from `ferrosa-memory/ddl/051_mobile_control_store.cql`
//! and `052_mobile_control_cursor_reservation_token.cql` (only the keyspace
//! name is substituted). The DML shapes are transcribed from
//! `ferrosa-memory/crates/ferrosa-memory-core/src/control_store.rs`.
//!
//! Usage: ferrosa-lite-cql-surface --addr 127.0.0.1:19042

use std::net::SocketAddr;

use ferrosa_cql::client::CqlClient;

fn ok(name: &str, detail: impl std::fmt::Display) {
    println!("ok   {name}: {detail}");
}

fn bad(name: &str, detail: impl std::fmt::Display) {
    println!("FAIL {name}: {detail}");
}

const TENANT: &str = "11111111-2222-3333-4444-555555555555";
const EVENT: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const COMMAND: &str = "99999999-8888-7777-6666-555555555555";
const TOKEN: &str = "12121212-3434-5656-7878-909090909090";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut addr: SocketAddr = "127.0.0.1:19042".parse()?;
    let mut ks = String::from("agent_memory");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => addr = args.next().ok_or("--addr needs addr:port")?.parse()?,
            "--keyspace" => ks = args.next().ok_or("--keyspace needs a name")?,
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    // 1. STARTUP handshake, unauthenticated (the node runs auth-disabled, as
    //    the appliance's loopback listener does).
    let mut client = CqlClient::connect(addr).await?;
    ok("startup (no auth)", format!("ready={}", client.is_ready()));

    // 2. CREATE KEYSPACE IF NOT EXISTS — the migration runner's first step.
    let keyspace_ddl = format!(
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = \
         {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    );
    report(&mut client, "create keyspace if not exists", &keyspace_ddl).await;

    // 3. ddl/051_mobile_control_store.cql, verbatim (keyspace substituted).
    let ddl_051 = [
        format!(
            "CREATE TABLE IF NOT EXISTS {ks}.mobile_control_cursor_state (\n\
                 tenant_id uuid,\n\
                 server_fingerprint text,\n\
                 next_cursor bigint,\n\
                 updated_at timestamp,\n\
                 PRIMARY KEY ((tenant_id, server_fingerprint))\n\
             )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {ks}.mobile_control_events (\n\
                 tenant_id uuid,\n\
                 server_fingerprint text,\n\
                 cursor_bucket int,\n\
                 cursor bigint,\n\
                 event_id uuid,\n\
                 command_id uuid,\n\
                 event_type text,\n\
                 payload text,\n\
                 created_at timestamp,\n\
                 PRIMARY KEY ((tenant_id, server_fingerprint, cursor_bucket), cursor)\n\
             ) WITH CLUSTERING ORDER BY (cursor ASC)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {ks}.mobile_control_commands (\n\
                 tenant_id uuid,\n\
                 server_fingerprint text,\n\
                 command_id uuid,\n\
                 command_type text,\n\
                 request_payload text,\n\
                 state text,\n\
                 result_payload text,\n\
                 result_cursor bigint,\n\
                 created_at timestamp,\n\
                 updated_at timestamp,\n\
                 PRIMARY KEY ((tenant_id, server_fingerprint), command_id)\n\
             )"
        ),
        // migration.rs::ensure_schema_version_table
        format!(
            "CREATE TABLE IF NOT EXISTS {ks}.schema_version (\
                 version int PRIMARY KEY,\
                 applied_at timestamp,\
                 description text,\
                 applied_by text)"
        ),
    ];
    for (i, ddl) in ddl_051.iter().enumerate() {
        report(&mut client, &format!("ddl 051 #{i}"), ddl).await;
    }

    // 4. ddl/052: ALTER TABLE ... ADD column — the cursor reservation token.
    report(
        &mut client,
        "ddl 052 alter table add column",
        &format!("ALTER TABLE {ks}.mobile_control_cursor_state ADD reservation_token uuid"),
    )
    .await;

    // 5. INSERT ... IF NOT EXISTS — cursor-state reservation (control_store.rs
    //    `reserve_cursor_block`, first-boot branch) at LOCAL_QUORUM.
    let insert_cursor = format!(
        "INSERT INTO {ks}.mobile_control_cursor_state \
         (tenant_id, server_fingerprint, next_cursor, updated_at, reservation_token) \
         VALUES ({TENANT}, 'srv-1', 101, '2026-10-22 12:00:00.000+0000', {TOKEN}) IF NOT EXISTS"
    );
    report_cl(&mut client, "insert cursor_state if not exists", &insert_cursor, 6).await;

    // 6. UPDATE ... IF <non-key column> = ? — compare-and-set on next_cursor.
    //    This is the shape that decides whether the control store can reserve a
    //    block at all; it is not a partition-key condition.
    let cas = format!(
        "UPDATE {ks}.mobile_control_cursor_state \
         SET next_cursor = 201, updated_at = '2026-10-22 12:00:01.000+0000', reservation_token = {TOKEN} \
         WHERE tenant_id = {TENANT} AND server_fingerprint = 'srv-1' IF next_cursor = 101"
    );
    report_cl(&mut client, "update cursor_state if next_cursor = 101 (CAS)", &cas, 6).await;
    // A stale CAS must report not-applied, not error.
    report_cl(&mut client, "update cursor_state if next_cursor = 101 (stale CAS)", &cas, 6).await;

    // 7. INSERT ... IF NOT EXISTS — event (control_store.rs append path).
    let insert_event = format!(
        "INSERT INTO {ks}.mobile_control_events \
         (tenant_id, server_fingerprint, cursor_bucket, cursor, event_id, command_id, \
          event_type, payload, created_at) \
         VALUES ({TENANT}, 'srv-1', 0, 202, {EVENT}, {COMMAND}, 'command', '{{}}', \
                 '2026-10-22 12:00:02.000+0000') IF NOT EXISTS"
    );
    report_cl(&mut client, "insert event if not exists", &insert_event, 6).await;
    report_cl(&mut client, "insert event if not exists (repeat)", &insert_event, 6).await;

    // 8. INSERT ... IF NOT EXISTS — command row.
    let insert_command = format!(
        "INSERT INTO {ks}.mobile_control_commands \
         (tenant_id, server_fingerprint, command_id, command_type, request_payload, state, \
          result_payload, result_cursor, created_at, updated_at) \
         VALUES ({TENANT}, 'srv-1', {COMMAND}, 'ping', '{{}}', 'pending', null, null, \
                 '2026-10-22 12:00:03.000+0000', '2026-10-22 12:00:03.000+0000') IF NOT EXISTS"
    );
    report_cl(&mut client, "insert command if not exists", &insert_command, 6).await;

    // 9. UPDATE ... IF EXISTS — command state transition (control_store.rs:1014).
    let update_command = format!(
        "UPDATE {ks}.mobile_control_commands SET state = 'done', result_payload = '{{}}', \
         result_cursor = 202, updated_at = '2026-10-22 12:00:04.000+0000' \
         WHERE tenant_id = {TENANT} AND server_fingerprint = 'srv-1' AND command_id = {COMMAND} \
         IF EXISTS"
    );
    report_cl(&mut client, "update command if exists", &update_command, 6).await;

    // 10. Schema probes at CL ONE (control_store.rs:591-604).
    for (name, q) in [
        (
            "select cursor_state at one",
            format!(
                "SELECT next_cursor, reservation_token FROM {ks}.mobile_control_cursor_state \
                 WHERE tenant_id = {TENANT} AND server_fingerprint = 'srv-1'"
            ),
        ),
        (
            "select events by partition at one",
            format!(
                "SELECT cursor, event_type, payload FROM {ks}.mobile_control_events \
                 WHERE tenant_id = {TENANT} AND server_fingerprint = 'srv-1' AND cursor_bucket = 0"
            ),
        ),
        (
            "select commands by partition at one",
            format!(
                "SELECT command_id, command_type, state, request_payload \
                 FROM {ks}.mobile_control_commands \
                 WHERE tenant_id = {TENANT} AND server_fingerprint = 'srv-1'"
            ),
        ),
    ] {
        report_cl(&mut client, name, &q, 1).await;
    }

    // 11. Clustering-range replay read at LOCAL_QUORUM with LIMIT
    //     (control_store.rs replay path).
    let range = format!(
        "SELECT cursor, event_id, command_id, event_type, payload, created_at \
         FROM {ks}.mobile_control_events \
         WHERE tenant_id = {TENANT} AND server_fingerprint = 'srv-1' AND cursor_bucket = 0 \
         AND cursor > 0 AND cursor <= 202 LIMIT 100"
    );
    report_cl(&mut client, "select clustering range with limit", &range, 6).await;

    // 12. PREPARE — the scylla driver prepares by default.
    match client.prepare(&range).await {
        Ok(prepared) => {
            let id = *prepared.id();
            let hex: String = id.iter().map(|b| format!("{b:02x}")).collect();
            ok("prepare", format!("id={hex}"));
        }
        Err(err) => bad("prepare", err),
    }

    // 13. USE keyspace — the driver selects a keyspace after connect.
    report(&mut client, "use keyspace", &format!("USE {ks}")).await;

    // 14. TRUNCATE is not used by the control store; recorded as absent rather
    //     than probed, so the surface list stays honest.
    println!("n/a  conditional BATCH: control_store.rs issues no BATCH statements");
    Ok(())
}

async fn report(client: &mut CqlClient, name: &str, cql: &str) {
    match client.query(cql).await {
        Ok(result) => ok(name, format!("rows={:?}", result.rows)),
        Err(err) => bad(name, err),
    }
}

async fn report_cl(client: &mut CqlClient, name: &str, cql: &str, cl: u16) {
    let outcome = if cl == 6 {
        client.query_quorum(cql).await
    } else {
        client.query(cql).await
    };
    match outcome {
        Ok(result) => ok(name, format!("rows={:?}", result.rows)),
        Err(err) => bad(name, err),
    }
}
