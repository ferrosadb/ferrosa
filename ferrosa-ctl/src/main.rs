//! `ferrosa-ctl` — administration and observability CLI for Ferrosa.
//!
//! Connects to a running Ferrosa node via the CQL native protocol and issues
//! SELECT queries against `system_observability.*` virtual tables.
//!
//! # Usage
//!
//! ```text
//! ferrosa-ctl [--host 127.0.0.1:9042] <SUBCOMMAND>
//!
//! Subcommands:
//!   status       Node health summary
//!   connections  Active CQL connections
//!   queries      Currently running queries
//!   storage      Storage engine statistics
//!   topology     Ring topology / token assignments
//!   peers        Peer node list
//!   monitor      Interactive TUI dashboard (T24)
//! ```

use std::net::SocketAddr;
use std::process;

use clap::{Parser, Subcommand};

mod commands;

/// Administration and observability CLI for Ferrosa.
#[derive(Debug, Parser)]
#[command(
    name = "ferrosa-ctl",
    version,
    about = "Ferrosa node administration and observability"
)]
struct Cli {
    /// Address of the Ferrosa CQL endpoint.
    #[arg(long, default_value = "127.0.0.1:9042")]
    host: String,

    #[command(subcommand)]
    command: Commands,
}

/// Available subcommands.
#[derive(Debug, Subcommand)]
enum Commands {
    /// Show node status and a connection count summary.
    Status,

    /// List active CQL connections.
    Connections {
        /// Sort results by this column name.
        #[arg(long)]
        sort: Option<String>,
    },

    /// List currently running CQL queries.
    Queries {
        /// Show only the longest-running queries (sorted by elapsed time).
        #[arg(long)]
        long_running: bool,
    },

    /// Show storage engine statistics.
    Storage,

    /// Show ring topology and token assignments.
    Topology,

    /// List peer nodes.
    Peers,

    /// Launch the interactive TUI dashboard (T24).
    Monitor {
        /// Panel to display (e.g. "connections", "storage").
        #[arg(long)]
        panel: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Resolve the host argument to a SocketAddr.
    let addr: SocketAddr = cli.host.parse().unwrap_or_else(|e| {
        eprintln!("error: invalid host address '{}': {e}", cli.host);
        process::exit(1);
    });

    let result = match cli.command {
        Commands::Status => commands::run_status(addr).await,
        Commands::Connections { sort } => commands::run_connections(addr, sort.as_deref()).await,
        Commands::Queries { long_running } => commands::run_queries(addr, long_running).await,
        Commands::Storage => commands::run_storage(addr).await,
        Commands::Topology => commands::run_topology(addr).await,
        Commands::Peers => commands::run_peers(addr).await,
        Commands::Monitor { panel } => commands::run_monitor(addr, panel.as_deref()).await,
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        process::exit(1);
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    /// Verify that the CLI definition itself is internally consistent.
    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    /// Verify default host value is parsed correctly.
    #[test]
    fn default_host_parses_as_socket_addr() {
        let addr: SocketAddr = "127.0.0.1:9042".parse().unwrap();
        assert_eq!(addr.port(), 9042);
    }

    /// Verify all subcommands are reachable via clap's try_parse_from.
    #[test]
    fn subcommand_status_parses() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "status"]).unwrap();
        assert!(matches!(cli.command, Commands::Status));
    }

    #[test]
    fn subcommand_connections_no_sort() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "connections"]).unwrap();
        assert!(matches!(cli.command, Commands::Connections { sort: None }));
    }

    #[test]
    fn subcommand_connections_with_sort() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "connections", "--sort", "addr"]).unwrap();
        match cli.command {
            Commands::Connections { sort: Some(s) } => assert_eq!(s, "addr"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn subcommand_queries_default() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "queries"]).unwrap();
        assert!(matches!(
            cli.command,
            Commands::Queries {
                long_running: false
            }
        ));
    }

    #[test]
    fn subcommand_queries_long_running() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "queries", "--long-running"]).unwrap();
        assert!(matches!(
            cli.command,
            Commands::Queries { long_running: true }
        ));
    }

    #[test]
    fn subcommand_storage_parses() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "storage"]).unwrap();
        assert!(matches!(cli.command, Commands::Storage));
    }

    #[test]
    fn subcommand_topology_parses() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "topology"]).unwrap();
        assert!(matches!(cli.command, Commands::Topology));
    }

    #[test]
    fn subcommand_peers_parses() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "peers"]).unwrap();
        assert!(matches!(cli.command, Commands::Peers));
    }

    #[test]
    fn subcommand_monitor_no_panel() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "monitor"]).unwrap();
        assert!(matches!(cli.command, Commands::Monitor { panel: None }));
    }

    #[test]
    fn subcommand_monitor_with_panel() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "monitor", "--panel", "storage"]).unwrap();
        match cli.command {
            Commands::Monitor { panel: Some(p) } => assert_eq!(p, "storage"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn custom_host_overrides_default() {
        let cli =
            Cli::try_parse_from(["ferrosa-ctl", "--host", "10.0.0.1:9042", "status"]).unwrap();
        assert_eq!(cli.host, "10.0.0.1:9042");
    }
}
