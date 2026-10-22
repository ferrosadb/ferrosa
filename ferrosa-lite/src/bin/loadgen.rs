//! Drives the same conditional-write shape the appliance's `CqlControlStore`
//! uses (unprepared `INSERT ... IF NOT EXISTS` at LOCAL_QUORUM, then an
//! `UPDATE ... IF EXISTS`) so the "RSS after 10k control writes" row is
//! measured against a running node, not estimated.
//!
//! Usage: ferrosa-lite-loadgen --addr 127.0.0.1:19042 --rows 10000

use std::net::SocketAddr;
use std::time::Instant;

use ferrosa_cql::client::CqlClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut addr: SocketAddr = "127.0.0.1:19042".parse()?;
    let mut rows: usize = 10_000;
    let mut keyspace = String::from("lite_wl");
    let mut conns: usize = 1;
    let mut select: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => addr = args.next().ok_or("--addr needs addr:port")?.parse()?,
            "--rows" => rows = args.next().ok_or("--rows needs a count")?.parse()?,
            "--keyspace" => keyspace = args.next().ok_or("--keyspace needs a name")?,
            // Writers in flight at once. 1 = the sequential shape; >1 tests the
            // claim the spike exists to test: that SQLite serialises every
            // write behind one process-wide lock.
            "--conns" => conns = args.next().ok_or("--conns needs a count")?.parse()?,
            // Read one statement and print the rows, then exit. Used for the
            // restart/replay check.
            "--select" => select = Some(args.next().ok_or("--select needs CQL")?),
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    if let Some(cql) = select {
        let mut client = CqlClient::connect(addr).await?;
        let result = client.query_quorum(&cql).await?;
        println!("rows={}", result.rows.len());
        for row in &result.rows {
            println!("{row:?}");
        }
        return Ok(());
    }

    let mut client = CqlClient::connect(addr).await?;
    println!("connected {addr} ready={}", client.is_ready());

    // Keyspace + table: what the control store does on first start
    // (`CREATE KEYSPACE IF NOT EXISTS ...`) and on migration.
    client
        .query(&format!(
            "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = \
             {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        ))
        .await?;
    client
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.control (id text PRIMARY KEY, v text)"
        ))
        .await?;

    // 10k conditional writes. LOCAL_QUORUM = 6 in the CQL v4 protocol.
    // `--conns` writers run in parallel, each on its own connection, splitting
    // the key space so no two writers touch the same partition.
    let started = Instant::now();
    let per = if conns == 0 { 0 } else { rows / conns };
    let mut handles = Vec::new();
    for c in 0..conns.max(1) {
        let keyspace = keyspace.clone();
        handles.push(tokio::spawn(async move {
            let mut client = CqlClient::connect(addr).await?;
            let mut applied = 0usize;
            for i in 0..per {
                let id = c * per + i;
                let cql = format!(
                    "INSERT INTO {keyspace}.control (id, v) VALUES ('k{id}', 'v{id}') IF NOT EXISTS"
                );
                let result = client.query_quorum(&cql).await?;
                applied += result.rows.len();
            }
            Ok::<usize, Box<dyn std::error::Error + Send + Sync>>(applied)
        }));
    }
    let mut applied = 0usize;
    for handle in handles {
        // Join and task errors are flattened to strings so they convert into
        // the binary's `Box<dyn Error>` return type.
        let partial = handle.await.map_err(|e| e.to_string())?;
        applied += partial.map_err(|e| e.to_string())?;
    }
    let write_elapsed = started.elapsed();

    let mut client = CqlClient::connect(addr).await?;

    // One update of an existing row, the `UPDATE ... IF EXISTS` shape the
    // cursor reservation uses.
    client
        .query_quorum(&format!(
            "UPDATE {keyspace}.control SET v = 'moved' WHERE id = 'k0' IF EXISTS"
        ))
        .await?;

    let read = client
        .query_quorum(&format!("SELECT id, v FROM {keyspace}.control WHERE id = 'k1'"))
        .await?;

    println!(
        "rows={rows} applied={applied} write_elapsed_ms={} rows_per_sec={:.0} read_back={:?}",
        write_elapsed.as_millis(),
        rows as f64 / write_elapsed.as_secs_f64(),
        read.rows.first().map(|r| format!("{r:?}"))
    );
    Ok(())
}
