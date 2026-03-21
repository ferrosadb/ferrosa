//! Ferrosa Worker -- standalone task executor.
//!
//! Reads a task descriptor from CLI argument, stdin, or S3 path.
//! Executes the task (index build, compaction, etc.) and writes
//! the result to stdout or S3.
//!
//! No cluster membership, no Raft, no persistent state.
//! Pure function: input -> compute -> output.

use serde::{Deserialize, Serialize};

/// Task descriptor -- describes what work the worker should perform.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TaskDescriptor {
    /// Build a secondary index for SSTables read from S3.
    IndexBuild {
        /// S3 paths of SSTable data files.
        sstable_s3_paths: Vec<String>,
        /// Keyspace name.
        keyspace: String,
        /// Table name.
        table: String,
        /// Index name.
        index_name: String,
        /// Index metadata (JSON string from IndexMetadata).
        index_metadata_json: String,
        /// Table schema (JSON string from TableSchema).
        table_schema_json: String,
        /// S3 path prefix for output sidecar files.
        output_s3_prefix: String,
    },
}

/// Result descriptor -- describes the outcome of a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    /// Whether the task succeeded.
    pub success: bool,
    /// Output S3 paths (if any).
    pub output_paths: Vec<String>,
    /// Error message (if failed).
    pub error: Option<String>,
}

fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        let result = TaskResult {
            success: false,
            output_paths: vec![],
            error: Some("Usage: ferrosa-worker <task-json>".into()),
        };
        println!("{}", serde_json::to_string(&result).unwrap());
        std::process::exit(1);
    }

    let task_json = &args[1];
    let task: TaskDescriptor = match serde_json::from_str(task_json) {
        Ok(t) => t,
        Err(e) => {
            let result = TaskResult {
                success: false,
                output_paths: vec![],
                error: Some(format!("Failed to parse task descriptor: {e}")),
            };
            println!("{}", serde_json::to_string(&result).unwrap());
            std::process::exit(1);
        }
    };

    tracing::info!(?task, "Starting task");

    let result = match task {
        TaskDescriptor::IndexBuild { .. } => {
            // Stub: actual S3 read + index build will be wired in a follow-up.
            tracing::info!("IndexBuild task received (stub -- not yet implemented)");
            TaskResult {
                success: false,
                output_paths: vec![],
                error: Some("IndexBuild not yet implemented".into()),
            }
        }
    };

    println!("{}", serde_json::to_string(&result).unwrap());

    if !result.success {
        std::process::exit(1);
    }
}
