//! `ferrosa-lite` — the minimum node that can own a state volume.
//!
//! Scope, deliberately: open the storage engine on a data directory, serve
//! CQL on loopback (this is the protocol the appliance's `CqlControlStore`
//! speaks), and — only when the `sql` feature is on — link the in-process
//! relational engine so its bytes can be measured as their own row.
//!
//! Not here, on purpose: graph/Cypher, SPARQL, the Postgres wire front-end,
//! the web console, Flight, CDC consumers, cluster supervision, scheduling.
//!
//! The single-node execution context comes from `ferrosa_cql::test_util`
//! (`standalone_for_test`), which is the only in-tree constructor of a
//! working `SharedState` outside the `ferrosa` binary. Using it keeps this
//! crate to the question the spike is asking — "what does the wiring cost in
//! bytes?" — instead of re-deriving several hundred lines of node startup.

use std::net::SocketAddr;
use std::path::PathBuf;

mod standalone;

fn main() {
    if let Err(err) = run() {
        eprintln!("ferrosa-lite: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let mut data_dir: Option<PathBuf> = None;
    let mut listen: Option<SocketAddr> = None;
    let mut rss_probe = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-dir" => {
                data_dir = Some(PathBuf::from(
                    args.next().ok_or("--data-dir needs a path")?,
                ))
            }
            "--listen" => {
                listen = Some(args.next().ok_or("--listen needs addr:port")?.parse()?)
            }
            // Print the process' own peak RSS (VmHWM) then keep serving, so a
            // harness can read the number from the node rather than from ps.
            "--rss-probe" => rss_probe = true,
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    let data_dir = data_dir
        .or_else(|| std::env::var_os("FERROSA_LITE_DATA").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("ferrosa-lite-data"));
    let listen = listen
        .or_else(|| std::env::var("FERROSA_LITE_CQL").ok().and_then(|v| v.parse().ok()))
        .unwrap_or_else(|| "127.0.0.1:19042".parse().expect("literal addr"));
    std::fs::create_dir_all(&data_dir)?;

    // --- Optional SQL engine path ---------------------------------------
    // Compiled only with `--features sql`. The call is real: parse a
    // statement, plan it, execute it against a provider, print the row. That
    // keeps the whole engine reachable from `main`, so the linked bytes are
    // the engine's, not the linker's opinion about dead code.
    #[cfg(feature = "sql")]
    {
        use ferrosa_sql::{
            execute, parse, Column, ColumnType, InMemoryTable, MapCatalog, RelSchema, Row, Value,
        };
        let schema = RelSchema::new(vec![
            Column::new("id", ColumnType::Int),
            Column::new("v", ColumnType::Int),
        ]);
        let table = InMemoryTable::new(
            schema.clone(),
            vec![
                Row::new(vec![Value::Int(1), Value::Int(10)]),
                Row::new(vec![Value::Int(2), Value::Int(32)]),
            ],
        );
        let catalog = MapCatalog::new().with_table("lite", "probe", std::sync::Arc::new(table));
        let select = parse("SELECT v FROM lite.probe WHERE id = 2")?;
        let result = execute(&select, &catalog, "lite", &[])?;
        println!(
            "sql-selftest: {} row(s), first={:?}",
            result.rows.len(),
            result.rows.first()
        );
    }

    // --- Node ------------------------------------------------------------
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let (state, boot) = standalone::build(&data_dir)?;
        println!(
            "boot: commitlog_replay={} replayed_mutations={} restored_tables={} registered_tables={}",
            boot.commitlog_replay,
            boot.replayed_mutations,
            boot.restored_tables,
            boot.registered_tables
        );

        let config = ferrosa_cql::server::ServerConfig {
            bind_addr: listen,
            // The lite node is a loopback service behind the appliance's own
            // unit ordering; it has no user-facing auth surface.
            auth_disabled: true,
            ..ferrosa_cql::server::ServerConfig::default()
        };
        let server = ferrosa_cql::server::CqlServer::new(config, state);
        let bound = server.start_background().await?;
        println!("ferrosa-lite listening on {bound} data_dir={}", data_dir.display());
        if rss_probe {
            print_rss("after-start");
        }
        // Serve until SIGINT/SIGTERM.
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
        if rss_probe {
            print_rss("at-shutdown");
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

/// Peak resident set in KiB, read from this process' own status file.
/// `VmHWM` is the high-water mark and does not decay, which is what "RSS
/// after 10k writes" means for a server.
fn print_rss(stage: &str) {
    match std::fs::read_to_string("/proc/self/status") {
        Ok(status) => {
            for line in status.lines() {
                if line.starts_with("VmHWM:") || line.starts_with("VmRSS:") {
                    println!("rss[{stage}] {line}");
                }
            }
        }
        Err(err) => println!("rss[{stage}] unavailable: {err}"),
    }
}
