use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use ferrosa_jepsen::config::{Concurrency, RunConfig, Tier, Topology};
use tracing_subscriber::EnvFilter;

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

    /// W8.9 — Run the sim-equivalent endurance test (path b per
    /// ADR-016) when Fly.io is unavailable. Headline acceptance
    /// gate for Sprint 8.
    TierEnduranceSim {
        /// Smoke variant — completes in <1s.
        #[arg(long)]
        smoke: bool,
        /// Output results as JSON.
        #[arg(long)]
        output: Option<OutputFormat>,
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

            let report = ferrosa_jepsen::orchestrator::run(&config).await?;

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
                        // Distinguish a harness/infrastructure failure from a real
                        // correctness result (issue #303). A setup failure produced
                        // NO history, so no invariant was evaluated — saying it
                        // "violated an invariant" announces a flaky nightly as a
                        // correctness regression.
                        if let Some(setup) = &f.setup_error {
                            println!(
                                "  - {} / {}: SETUP FAILED (no invariant evaluated): {}",
                                f.workload, f.nemesis, setup
                            );
                        } else {
                            println!(
                                "  - {} / {}: INVARIANT VIOLATED: {:?}",
                                f.workload, f.nemesis, f.invariant_error
                            );
                        }
                    }
                }
            }

            // FAIL LOUD (t_5fdf25f0): the process exit code must reflect the run.
            // Previously this returned Ok(()) unconditionally, so a run that
            // executed ZERO combinations (e.g. cluster provisioning failed and the
            // only topology was skipped) — or even one with failing invariants —
            // exited 0 and showed green in CI, masking a broken harness.
            if report.total == 0 {
                anyhow::bail!(
                    "jepsen run executed 0 combinations — nothing was verified \
                     (cluster provisioning likely failed / every topology skipped). \
                     Refusing to report a false green."
                );
            }
            if !report.all_passed() {
                // Report the two failure classes separately. Both still fail the
                // run (a harness that cannot provision is not a green result),
                // but calling a setup failure an invariant violation misdirects
                // triage at a correctness bug that was never observed (#303).
                let setup_failures = report
                    .failures()
                    .iter()
                    .filter(|f| f.is_setup_failure())
                    .count();
                let invariant_failures = report.failed.saturating_sub(setup_failures);
                match (setup_failures, invariant_failures) {
                    (s, 0) => anyhow::bail!(
                        "jepsen run failed: {s} of {} combination(s) FAILED TO RUN \
                         (setup/infrastructure — no invariant was evaluated, so this is \
                         not a correctness result)",
                        report.total
                    ),
                    (0, i) => anyhow::bail!(
                        "jepsen run failed: {i} of {} combination(s) violated an invariant",
                        report.total
                    ),
                    (s, i) => anyhow::bail!(
                        "jepsen run failed: {i} of {} combination(s) violated an invariant; \
                         {s} more FAILED TO RUN (setup/infrastructure)",
                        report.total
                    ),
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
        Commands::TierEnduranceSim { smoke, output } => {
            let cfg = if smoke {
                ferrosa_jepsen::endurance_sim::EnduranceSimConfig::smoke()
            } else {
                ferrosa_jepsen::endurance_sim::EnduranceSimConfig::tri_dc_24h()
            };
            tracing::info!(
                total_ticks = cfg.total_ticks,
                learners_per_dc = cfg.learners_per_dc,
                "Starting tier-endurance-sim (W8.9)"
            );
            let result = ferrosa_jepsen::endurance_sim::run_endurance_sim(&cfg)?;
            let json = matches!(output, Some(OutputFormat::Json));
            if json {
                let body = serde_json::json!({
                    "total_transfers": result.total_transfers,
                    "conservation_failures": result.conservation_failures,
                    "learner_divergence_failures": result.learner_divergence_failures,
                    "verifications_run": result.verifications_run,
                    "partition_cycles": result.partition_cycles,
                    "final_converged": result.final_converged,
                    "wall_clock_secs": result.wall_clock.as_secs_f64(),
                    "passed": result.passed(),
                });
                println!("{}", serde_json::to_string_pretty(&body)?);
            } else {
                println!("\n=== ferrosa-jepsen tier-endurance-sim ===");
                println!("  total_transfers:        {}", result.total_transfers);
                println!("  conservation_failures:  {}", result.conservation_failures);
                println!(
                    "  learner_divergence:     {}",
                    result.learner_divergence_failures
                );
                println!("  verifications_run:      {}", result.verifications_run);
                println!("  partition_cycles:       {}", result.partition_cycles);
                println!("  final_converged:        {}", result.final_converged);
                println!(
                    "  wall_clock:             {:.2}s",
                    result.wall_clock.as_secs_f64()
                );
                println!(
                    "  result:                 {}",
                    if result.passed() { "PASS" } else { "FAIL" }
                );
            }
            if !result.passed() {
                anyhow::bail!("tier-endurance-sim failed acceptance criteria");
            }
            Ok(())
        }
    }
}
