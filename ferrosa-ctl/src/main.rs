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
//!   snapshot     Manage node snapshots (create / list / delete)
//!   restore      Restore from a snapshot, optionally to a point in time
//!   auth         Manage installer-seeded admin credentials
//! ```

use std::net::SocketAddr;
use std::process;

use clap::{Parser, Subcommand};

mod auth;
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

    /// Manage node snapshots (create, list, delete).
    Snapshot {
        #[command(subcommand)]
        action: SnapshotAction,
    },

    /// Restore from a snapshot, optionally to a point in time.
    Restore {
        /// Name of the snapshot to restore from.
        snapshot_name: String,

        /// Restore to this point in time (RFC 3339 timestamp, e.g. 2026-03-18T12:00:00Z).
        #[arg(long)]
        point_in_time: Option<String>,

        /// Skip confirmation prompt and proceed even if data will be overwritten.
        #[arg(long)]
        force: bool,
    },

    /// Manage installer-seeded admin credentials in `~/.ferrosa/config/auth.yaml`.
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },
}

/// Auth sub-actions.
#[derive(Debug, Subcommand)]
enum AuthAction {
    /// Hash a password (read from stdin, no echo) and write it to auth.yaml.
    SetPassword {
        /// Username for the credential (e.g. `admin`).
        #[arg(long)]
        user: String,

        /// Realm — `cql` for the CQL native protocol, `graph` for HTTP/Bolt basic auth.
        #[arg(long)]
        realm: String,

        /// Path to auth.yaml (defaults to `$HOME/.ferrosa/config/auth.yaml`).
        #[arg(long)]
        config: Option<std::path::PathBuf>,

        /// Overwrite an existing entry without prompting.
        #[arg(long)]
        force: bool,
    },
}

/// Snapshot sub-actions.
#[derive(Debug, Subcommand)]
enum SnapshotAction {
    /// Create a new snapshot.
    Create {
        /// Name for the new snapshot.
        name: String,

        /// Time-to-live in hours before the snapshot is automatically removed.
        #[arg(long)]
        ttl_hours: Option<u64>,
    },

    /// List existing snapshots.
    List,

    /// Delete a snapshot.
    Delete {
        /// Name of the snapshot to delete.
        name: String,
    },
}

/// Unified error type used by `main` so all match arms have the same type.
type MainError = Box<dyn std::error::Error + Send + Sync>;

/// Synchronous wrapper for `auth set-password` — wires the CLI args to the
/// `auth::run_set_password` core function.
fn run_auth_set_password(
    user: String,
    realm: String,
    config: Option<std::path::PathBuf>,
    force: bool,
) -> Result<(), auth::AuthError> {
    let realm: auth::Realm = realm
        .parse()
        .map_err(|e: String| auth::AuthError::Hash(e))?;
    let config_path = match config {
        Some(p) => p,
        None => auth::default_config_path()?,
    };
    let opts = auth::SetPasswordOpts {
        realm,
        user,
        config_path,
        force,
    };
    let mut source = auth::StdioPasswordSource;
    auth::run_set_password(&opts, &mut source, chrono::Utc::now())
}

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
        Commands::Snapshot { action } => match action {
            SnapshotAction::Create { name, ttl_hours } => {
                commands::snapshot_create(&web_host, web_port, &name, ttl_hours).await
            }
            SnapshotAction::List => commands::snapshot_list(&web_host, web_port).await,
            SnapshotAction::Delete { name } => {
                commands::snapshot_delete(&web_host, web_port, &name).await
            }
        },
        Commands::Restore {
            snapshot_name,
            point_in_time,
            force,
        } => {
            commands::restore(
                &web_host,
                web_port,
                &snapshot_name,
                point_in_time.as_deref(),
                force,
            )
            .await
        }
        Commands::Auth { action } => match action {
            AuthAction::SetPassword {
                user,
                realm,
                config,
                force,
            } => run_auth_set_password(user, realm, config, force).map_err(Into::into),
        },
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

    // ── Snapshot / Restore argument-parsing tests ─────────────────────────────

    #[test]
    fn parse_snapshot_create_minimal() {
        let cli =
            Cli::try_parse_from(["ferrosa-ctl", "snapshot", "create", "daily-backup"]).unwrap();
        match cli.command {
            Commands::Snapshot {
                action: SnapshotAction::Create { name, ttl_hours },
            } => {
                assert_eq!(name, "daily-backup");
                assert!(ttl_hours.is_none());
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_snapshot_create_with_ttl() {
        let cli = Cli::try_parse_from([
            "ferrosa-ctl",
            "snapshot",
            "create",
            "weekly",
            "--ttl-hours",
            "168",
        ])
        .unwrap();
        match cli.command {
            Commands::Snapshot {
                action: SnapshotAction::Create { name, ttl_hours },
            } => {
                assert_eq!(name, "weekly");
                assert_eq!(ttl_hours, Some(168));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_snapshot_list() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "snapshot", "list"]).unwrap();
        assert!(matches!(
            cli.command,
            Commands::Snapshot {
                action: SnapshotAction::List,
            }
        ));
    }

    #[test]
    fn parse_snapshot_delete() {
        let cli =
            Cli::try_parse_from(["ferrosa-ctl", "snapshot", "delete", "daily-backup"]).unwrap();
        match cli.command {
            Commands::Snapshot {
                action: SnapshotAction::Delete { name },
            } => assert_eq!(name, "daily-backup"),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_restore_minimal() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "restore", "my-snapshot"]).unwrap();
        match cli.command {
            Commands::Restore {
                snapshot_name,
                point_in_time,
                force,
            } => {
                assert_eq!(snapshot_name, "my-snapshot");
                assert!(point_in_time.is_none());
                assert!(!force);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_restore_with_force() {
        let cli =
            Cli::try_parse_from(["ferrosa-ctl", "restore", "my-snapshot", "--force"]).unwrap();
        match cli.command {
            Commands::Restore { force, .. } => assert!(force),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_restore_with_point_in_time() {
        let cli = Cli::try_parse_from([
            "ferrosa-ctl",
            "restore",
            "my-snapshot",
            "--point-in-time",
            "2026-03-18T12:00:00Z",
        ])
        .unwrap();
        match cli.command {
            Commands::Restore { point_in_time, .. } => {
                assert_eq!(point_in_time.as_deref(), Some("2026-03-18T12:00:00Z"));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_auth_set_password_minimal() {
        let cli = Cli::try_parse_from([
            "ferrosa-ctl",
            "auth",
            "set-password",
            "--user",
            "admin",
            "--realm",
            "cql",
        ])
        .unwrap();
        match cli.command {
            Commands::Auth {
                action:
                    AuthAction::SetPassword {
                        user,
                        realm,
                        config,
                        force,
                    },
            } => {
                assert_eq!(user, "admin");
                assert_eq!(realm, "cql");
                assert!(config.is_none());
                assert!(!force);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_auth_set_password_all_flags() {
        let cli = Cli::try_parse_from([
            "ferrosa-ctl",
            "auth",
            "set-password",
            "--user",
            "admin",
            "--realm",
            "graph",
            "--config",
            "/tmp/foo/auth.yaml",
            "--force",
        ])
        .unwrap();
        match cli.command {
            Commands::Auth {
                action:
                    AuthAction::SetPassword {
                        user,
                        realm,
                        config,
                        force,
                    },
            } => {
                assert_eq!(user, "admin");
                assert_eq!(realm, "graph");
                assert_eq!(
                    config.as_deref().unwrap().to_str().unwrap(),
                    "/tmp/foo/auth.yaml"
                );
                assert!(force);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_restore_all_flags() {
        let cli = Cli::try_parse_from([
            "ferrosa-ctl",
            "restore",
            "backup-2026",
            "--point-in-time",
            "2026-03-01T00:00:00Z",
            "--force",
        ])
        .unwrap();
        match cli.command {
            Commands::Restore {
                snapshot_name,
                point_in_time,
                force,
            } => {
                assert_eq!(snapshot_name, "backup-2026");
                assert_eq!(point_in_time.as_deref(), Some("2026-03-01T00:00:00Z"));
                assert!(force);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }
}
