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

    /// Manage role authentication (set passwords, etc.).
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },

    /// Raft administration commands (ADR-012, W1.11, ADR-015).
    Raft {
        #[command(subcommand)]
        action: RaftAction,
    },

    /// Cluster operations — multi-DC bootstrap, etc. (ADR-015).
    Cluster {
        #[command(subcommand)]
        action: ClusterAction,
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
}

/// Raft administration sub-actions.
///
/// Unifies the W1.11 "reset" command with the ADR-012 "transfer-leader"
/// command. Both share the same Raft-engine entry point, so they live
/// under one subcommand to avoid clap's "multiple variants with the
/// same name" error.
#[derive(Debug, Subcommand)]
enum RaftAction {
    /// Wipe a node's persisted Raft state (log + meta trees) so it
    /// rejoins the cluster as a fresh learner. The node must be stopped
    /// before running this — sled holds an exclusive flock(2) on the
    /// data directory.
    ///
    /// Use case: recovery from the disruptor-partition runaway-term
    /// failure mode (specs/in-process/bug-raft-stale-candidate-runaway-
    /// term-no-prevote.md). The leader's InstallSnapshot / AppendEntries
    /// will replay committed history onto the reset node.
    Reset {
        /// Path to the node's Raft data directory (e.g. `/var/lib/ferrosa/raft`).
        #[arg(long)]
        data_dir: std::path::PathBuf,

        /// Print what would be cleared without actually clearing.
        #[arg(long)]
        dry_run: bool,
    },

    /// Transfer Raft leadership to a target node (Ongaro §3.10).
    ///
    /// Useful for graceful drains during DC-aware operations and for
    /// multi-DC failover. Calls `raft.trigger().transfer_to(target)` on the
    /// current leader; the target becomes leader within `election_timeout × 2`
    /// or the command returns `TransferError::Timeout`.
    ///
    /// **Status**: this subcommand is wired but the underlying
    /// `transfer_to` API is not yet implemented in the openraft fork. See
    /// `specs/in-process/sprint-03-openraft-patches.md` (W3.9). Until the
    /// engine work lands, the command emits a clear "not yet implemented"
    /// diagnostic.
    TransferLeader {
        /// Host ID of the target node (must currently be a Raft voter).
        #[arg(long = "to")]
        to: String,
    },
}

/// Auth sub-actions.
#[derive(Debug, Subcommand)]
enum AuthAction {
    /// Set the password for an existing role via CQL `ALTER ROLE`.
    ///
    /// The new password is read interactively from the terminal (no echo).
    /// The CLI authenticates as `--admin-user` using `--admin-password-env`
    /// (or a prompt if unset), then issues
    /// `ALTER ROLE "<user>" WITH PASSWORD = '<new>'`. The server hashes
    /// the cleartext via its own `PasswordHasher`. No password material
    /// is written to disk by this command.
    ///
    /// # Password policy
    ///
    /// - non-empty
    /// - at least 8 characters
    SetPassword {
        /// Role whose password to change. Defaults to the seed admin.
        #[arg(long, default_value = "ferrosa_admin")]
        user: String,

        /// Role used for the admin connection. Defaults to the seed admin.
        #[arg(long, default_value = "ferrosa_admin")]
        admin_user: String,

        /// Environment variable holding the admin password. If unset, the
        /// CLI tries the seed default first; on auth failure, prompts.
        #[arg(long)]
        admin_password_env: Option<String>,

        /// Use TLS for the CQL connection (currently a no-op placeholder;
        /// `CqlClient` does not yet support TLS). Errors out if requested.
        #[arg(long)]
        ssl: bool,

        /// Skip the SUPERUSER warning.
        #[arg(long)]
        force: bool,

        /// Skip the second-prompt confirmation (for scripting).
        #[arg(long)]
        no_confirm: bool,
    },
}

/// Cluster sub-actions (ADR-015).
#[derive(Debug, Subcommand)]
enum ClusterAction {
    /// Bootstrap a new per-DC Raft group (W6.7, ADR-015).
    ///
    /// Creates a fresh Raft group identified by `RaftGroupId::for_dc(<dc>)`
    /// with the listed seed nodes as the initial voter set. Existing DC
    /// groups continue running unchanged. Useful for adding a third DC
    /// after initial cluster bring-up, or rebuilding a per-DC group
    /// after a catastrophic failure.
    ///
    /// **Status**: in-process scaffolding (Sprint 6). The HTTP wire-up
    /// to the running node lands alongside the multi-DC formation
    /// rollout in Sprint 7. Until then, the command computes the
    /// derived `RaftGroupId` and prints it so operators can verify
    /// the per-DC namespace before they pull the trigger.
    BootstrapDc {
        /// DC name. Must match the `FERROSA_DATA_CENTER` env on the
        /// seed nodes.
        #[arg(long = "dc")]
        dc: String,

        /// Comma-separated list of seed addresses, e.g.
        /// `node3a:7000,node3b:7000,node3c:7000`.
        #[arg(long = "seeds", value_delimiter = ',')]
        seeds: Vec<String>,
    },

    /// W8.5 — Add a long-lived learner replica to the cluster (ADR-014).
    ///
    /// The learner receives `AppendEntries` and applies log entries but
    /// does not vote. With `--owns-tokens=true` (the default) it
    /// participates in the ring as a read replica; with
    /// `--owns-tokens=false` it is a state-machine-only follower
    /// (analytics / future witness role).
    AddLearner {
        /// Host ID (UUID) of the new learner.
        host_id: String,
        /// Internode address `<host>:<port>` for the learner.
        addr: String,
        /// Whether the learner owns ring tokens (default: true).
        #[arg(long = "owns-tokens", default_value_t = true)]
        owns_tokens: bool,
    },

    /// W8.5 — Promote a learner to a voter (ADR-014).
    PromoteToVoter {
        /// Host ID of the learner to promote.
        host_id: String,
    },

    /// W8.5 — Demote a voter to a learner (ADR-014).
    ///
    /// If the target is the current Raft leader, leadership is
    /// transferred first (W4.14 self-transfer pattern).
    DemoteToLearner {
        /// Host ID of the voter to demote.
        host_id: String,
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
        Commands::Auth { action } => match action {
            AuthAction::SetPassword {
                user,
                admin_user,
                admin_password_env,
                ssl,
                force,
                no_confirm,
            } => {
                if let Err(e) = commands::run_auth_set_password(
                    addr,
                    &user,
                    &admin_user,
                    admin_password_env.as_deref(),
                    ssl,
                    force,
                    no_confirm,
                )
                .await
                {
                    eprintln!("error: {}", e.message());
                    process::exit(e.exit_code());
                }
                Ok(())
            }
        },
        Commands::Raft { action } => match action {
            RaftAction::Reset { data_dir, dry_run } => commands::raft_reset(&data_dir, dry_run),
            RaftAction::TransferLeader { to } => {
                commands::raft_transfer_leader(&web_host, web_port, &to).await
            }
        },
        Commands::Cluster { action } => match action {
            ClusterAction::BootstrapDc { dc, seeds } => commands::cluster_bootstrap_dc(&dc, &seeds),
            ClusterAction::AddLearner {
                host_id,
                addr,
                owns_tokens,
            } => {
                commands::cluster_add_learner(&web_host, web_port, &host_id, &addr, owns_tokens)
                    .await
            }
            ClusterAction::PromoteToVoter { host_id } => {
                commands::cluster_promote_to_voter(&web_host, web_port, &host_id).await
            }
            ClusterAction::DemoteToLearner { host_id } => {
                commands::cluster_demote_to_learner(&web_host, web_port, &host_id).await
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

    /// W1.11 CLI parsing: `ferrosa-ctl raft reset --data-dir <path>` is
    /// recognized and produces the expected RaftAction::Reset variant.
    #[test]
    fn subcommand_raft_reset_parses() {
        let cli = Cli::try_parse_from([
            "ferrosa-ctl",
            "raft",
            "reset",
            "--data-dir",
            "/var/lib/ferrosa/raft",
        ])
        .unwrap();
        match cli.command {
            Commands::Raft {
                action: RaftAction::Reset { data_dir, dry_run },
            } => {
                assert_eq!(data_dir, std::path::PathBuf::from("/var/lib/ferrosa/raft"));
                assert!(!dry_run);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// W1.11 CLI parsing: `--dry-run` is supported.
    #[test]
    fn subcommand_raft_reset_dry_run_flag() {
        let cli = Cli::try_parse_from([
            "ferrosa-ctl",
            "raft",
            "reset",
            "--data-dir",
            "/tmp/x",
            "--dry-run",
        ])
        .unwrap();
        match cli.command {
            Commands::Raft {
                action: RaftAction::Reset { dry_run, .. },
            } => assert!(dry_run),
            other => panic!("unexpected: {other:?}"),
        }
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

    // ── auth set-password CLI tests ──────────────────────────────────────────

    #[test]
    fn parse_auth_set_password_defaults() {
        let cli = Cli::try_parse_from(["ferrosa-ctl", "auth", "set-password"]).unwrap();
        match cli.command {
            Commands::Auth {
                action:
                    AuthAction::SetPassword {
                        user,
                        admin_user,
                        admin_password_env,
                        ssl,
                        force,
                        no_confirm,
                    },
            } => {
                assert_eq!(user, "ferrosa_admin");
                assert_eq!(admin_user, "ferrosa_admin");
                assert!(admin_password_env.is_none());
                assert!(!ssl);
                assert!(!force);
                assert!(!no_confirm);
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
            "alice",
            "--admin-user",
            "root",
            "--admin-password-env",
            "MY_PW",
            "--ssl",
            "--force",
            "--no-confirm",
        ])
        .unwrap();
        match cli.command {
            Commands::Auth {
                action:
                    AuthAction::SetPassword {
                        user,
                        admin_user,
                        admin_password_env,
                        ssl,
                        force,
                        no_confirm,
                    },
            } => {
                assert_eq!(user, "alice");
                assert_eq!(admin_user, "root");
                assert_eq!(admin_password_env.as_deref(), Some("MY_PW"));
                assert!(ssl);
                assert!(force);
                assert!(no_confirm);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_auth_set_password_only_user() {
        let cli =
            Cli::try_parse_from(["ferrosa-ctl", "auth", "set-password", "--user", "bob"]).unwrap();
        match cli.command {
            Commands::Auth {
                action: AuthAction::SetPassword { user, .. },
            } => assert_eq!(user, "bob"),
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

    /// W3.13 (ADR-012): `ferrosa-ctl raft transfer-leader --to <host_id>`
    /// parses correctly. The actual server-side handler is deferred (W3.9).
    #[test]
    fn parse_raft_transfer_leader() {
        let cli = Cli::try_parse_from([
            "ferrosa-ctl",
            "raft",
            "transfer-leader",
            "--to",
            "host-uuid-abc",
        ])
        .unwrap();
        match cli.command {
            Commands::Raft {
                action: RaftAction::TransferLeader { to },
            } => {
                assert_eq!(to, "host-uuid-abc");
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }
}
