// Scaffold phase: types are defined before their consumers exist.
#![allow(dead_code)]

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

mod config;

use config::{Concurrency, RunConfig, Tier, Topology};

#[derive(Parser)]
#[command(name = "ferrosa-jepsen")]
#[command(about = "Jepsen-style correctness verification for Ferrosa")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a test suite
    Run {
        /// Test tier
        #[arg(long)]
        tier: Tier,

        /// Override topology (default: determined by tier)
        #[arg(long)]
        topology: Option<Topology>,

        /// Filter to a specific nemesis
        #[arg(long)]
        nemesis: Option<String>,

        /// Filter to a specific LWT pattern
        #[arg(long)]
        pattern: Option<String>,

        /// Filter to a specific driver
        #[arg(long)]
        driver: Option<String>,

        /// Override concurrency level
        #[arg(long)]
        concurrency: Option<Concurrency>,

        /// Output directory for results
        #[arg(long, default_value = "jepsen-results")]
        output_dir: PathBuf,

        /// Fly.io regions (comma-separated, e.g. "iad,cdg,nrt")
        #[arg(long, value_delimiter = ',')]
        fly_region: Vec<String>,

        /// Webhook URL for alerting on failures
        #[arg(long)]
        alert_webhook: Option<String>,

        /// Output results as JSON
        #[arg(long)]
        output: Option<OutputFormat>,
    },

    /// Generate or compare reports
    Report {
        #[command(subcommand)]
        action: ReportAction,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum OutputFormat {
    Json,
}

#[derive(Subcommand)]
enum ReportAction {
    /// List archived runs
    List,
    /// Compare two runs
    Compare {
        /// First run ID
        run_a: String,
        /// Second run ID
        run_b: String,
    },
    /// Regenerate HTML report for a run
    Render {
        /// Run ID
        run_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            tier,
            topology,
            nemesis,
            pattern,
            driver,
            concurrency,
            output_dir,
            fly_region,
            alert_webhook,
            output,
        } => {
            let run_id = format!(
                "{}-{}",
                chrono::Utc::now().format("%Y%m%d-%H%M%S"),
                &uuid::Uuid::new_v4().to_string()[..8],
            );

            let config = RunConfig {
                tier,
                topology,
                nemesis,
                pattern,
                driver,
                concurrency,
                run_id: run_id.clone(),
                output_dir,
                fly_regions: fly_region,
                alert_webhook,
                output_json: matches!(output, Some(OutputFormat::Json)),
            };

            tracing::info!(
                run_id = %run_id,
                tier = ?config.tier,
                "Starting Jepsen run"
            );

            // TODO: orchestrator::run(config).await?
            tracing::info!("Orchestrator not yet implemented — scaffold only");

            Ok(())
        }
        Commands::Report { action } => {
            match action {
                ReportAction::List => {
                    tracing::info!("Report listing not yet implemented");
                }
                ReportAction::Compare { run_a, run_b } => {
                    tracing::info!(
                        run_a = %run_a,
                        run_b = %run_b,
                        "Report comparison not yet implemented"
                    );
                }
                ReportAction::Render { run_id } => {
                    tracing::info!(
                        run_id = %run_id,
                        "Report rendering not yet implemented"
                    );
                }
            }
            Ok(())
        }
    }
}
