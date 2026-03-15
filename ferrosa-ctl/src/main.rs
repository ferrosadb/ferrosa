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
mod tui;

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

    /// Port of the Ferrosa web admin API.
    #[arg(long, default_value_t = 9090)]
    web_port: u16,

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

    /// Pre-approve a node for cluster join.
    AddNode {
        /// Host ID of the node to approve.
        host_id: String,
    },

    /// Decommission a node from the cluster.
    Decommission {
        /// Host ID of the node to decommission (defaults to the local node).
        host_id: Option<String>,
    },

    /// Show token ring distribution.
    Ring,

    /// Rebalance token distribution across the cluster.
    Rebalance,
}

/// Unified error type used by `main` so all match arms have the same type.
type MainError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Resolve the host argument to a SocketAddr (used for CQL commands).
    let addr: SocketAddr = cli.host.parse().unwrap_or_else(|e| {
        eprintln!("error: invalid host address '{}': {e}", cli.host);
        process::exit(1);
    });

    // Extract just the IP/hostname for HTTP web-API commands.
    let web_host = addr.ip().to_string();
    let web_port = cli.web_port;

    let result: Result<(), MainError> = match cli.command {
        Commands::Status => commands::run_status(addr).await.map_err(Into::into),
        Commands::Connections { sort } => commands::run_connections(addr, sort.as_deref())
            .await
            .map_err(Into::into),
        Commands::Queries { long_running } => commands::run_queries(addr, long_running)
            .await
            .map_err(Into::into),
        Commands::Storage => commands::run_storage(addr).await.map_err(Into::into),
        Commands::Topology => commands::run_topology(addr).await.map_err(Into::into),
        Commands::Peers => commands::run_peers(addr).await.map_err(Into::into),
        Commands::Monitor { panel } => commands::run_monitor(addr, panel.as_deref())
            .await
            .map_err(Into::into),
        Commands::AddNode { host_id } => commands::add_node(&web_host, web_port, &host_id).await,
        Commands::Decommission { host_id } => {
            commands::decommission(&web_host, web_port, host_id.as_deref()).await
        }
        Commands::Ring => commands::ring(&web_host, web_port).await,
        Commands::Rebalance => commands::rebalance(&web_host, web_port).await,
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

    #[test]
    fn web_port_default_is_9090() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "ring"]).unwrap();
        assert_eq!(cli.web_port, 9090);
    }

    #[test]
    fn web_port_override() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "--web-port", "8080", "ring"]).unwrap();
        assert_eq!(cli.web_port, 8080);
    }

    #[test]
    fn subcommand_add_node_parses() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "add-node", "host-uuid-1234"]).unwrap();
        match cli.command {
            Commands::AddNode { host_id } => assert_eq!(host_id, "host-uuid-1234"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn subcommand_decommission_no_host_id() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "decommission"]).unwrap();
        assert!(matches!(
            cli.command,
            Commands::Decommission { host_id: None }
        ));
    }

    #[test]
    fn subcommand_decommission_with_host_id() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "decommission", "host-uuid-5678"]).unwrap();
        match cli.command {
            Commands::Decommission { host_id: Some(id) } => assert_eq!(id, "host-uuid-5678"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn subcommand_ring_parses() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "ring"]).unwrap();
        assert!(matches!(cli.command, Commands::Ring));
    }

    #[test]
    fn subcommand_rebalance_parses() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "rebalance"]).unwrap();
        assert!(matches!(cli.command, Commands::Rebalance));
    }
}
