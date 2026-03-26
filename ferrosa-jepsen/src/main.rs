// Scaffold phase: types are defined before their consumers exist.
#![allow(dead_code)]

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

mod alert;
mod archive;
mod chaos;
mod checker;
mod cluster;
mod config;
mod driver;
mod endurance;
mod firecracker;
mod flyio;
mod history;
mod orchestrator;
mod report;
mod ssh;
mod workload;

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

            let report = orchestrator::run(&config).await?;

            if config.output_json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("\n=== ferrosa-jepsen run: {} ===", config.run_id);
                println!("  Total:  {}", report.total);
                println!("  Passed: {}", report.passed);
                println!("  Failed: {}", report.failed);
                if !report.all_passed() {
                    println!("\nFailures:");
                    for f in report.failures() {
                        println!(
                            "  - {} / {}: {:?}",
                            f.workload, f.nemesis, f.invariant_error
                        );
                    }
                }
            }

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
