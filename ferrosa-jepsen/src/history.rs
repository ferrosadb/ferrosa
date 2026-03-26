use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// A CQL operation in the history.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Op {
    /// Read a single key's value.
    Read { key: String },
    /// Write a value to a key.
    Write { key: String, value: i64 },
    /// Compare-and-swap.
    Cas {
        key: String,
        expected: i64,
        value: i64,
    },
    /// INSERT IF NOT EXISTS.
    InsertIfNotExists {
        table: String,
        pk: String,
        values: Vec<(String, String)>,
    },
    /// UPDATE ... IF condition.
    UpdateIf {
        table: String,
        pk: String,
        condition: String,
        assignments: Vec<(String, String)>,
    },
    /// DELETE ... IF condition.
    DeleteIf {
        table: String,
        pk: String,
        condition: String,
    },
    /// SELECT with SERIAL consistency.
    SerialRead { key: String },
    /// Multi-statement transaction (Accord).
    Transaction { statements: Vec<Op> },
}

/// Result of an operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OpResult {
    /// Success with no return value.
    Ok,
    /// LWT applied result (true = applied, false = not applied).
    Applied(bool),
    /// Read returned a value.
    Value(Option<i64>),
    /// LWT returned current values when not applied.
    CurrentValues(Vec<(String, String)>),
    /// Operation failed with error.
    Err(String),
    /// Operation timed out.
    Timeout,
}

/// A recorded operation with timing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Operation {
    pub client_id: String,
    /// Invoke time in microseconds (UTC epoch).
    pub invoke_us: u64,
    /// Complete time in microseconds (UTC epoch).
    pub complete_us: u64,
    pub op: Op,
    pub result: OpResult,
}

/// Complete history of operations from one or more clients.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct History {
    pub operations: Vec<Operation>,
}

impl History {
    /// Merge multiple client histories, sorted by invoke_us.
    pub fn merge(histories: Vec<History>) -> History {
        let mut all_ops: Vec<Operation> =
            histories.into_iter().flat_map(|h| h.operations).collect();
        all_ops.sort_by_key(|op| op.invoke_us);
        History {
            operations: all_ops,
        }
    }

    /// Read a history from a JSONL file (one Operation per line).
    pub fn from_jsonl(path: &Path) -> Result<History> {
        let file =
            std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let reader = BufReader::new(file);
        let mut operations = Vec::new();
        for (line_num, line) in reader.lines().enumerate() {
            let line = line.with_context(|| format!("reading line {}", line_num + 1))?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let op: Operation = serde_json::from_str(trimmed)
                .with_context(|| format!("parsing line {}", line_num + 1))?;
            operations.push(op);
        }
        Ok(History { operations })
    }

    /// Write this history to a JSONL file (one Operation per line).
    pub fn to_jsonl(&self, path: &Path) -> Result<()> {
        let mut file =
            std::fs::File::create(path).with_context(|| format!("creating {}", path.display()))?;
        for op in &self.operations {
            let json = serde_json::to_string(op).context("serializing operation")?;
            writeln!(file, "{json}").context("writing line")?;
        }
        Ok(())
    }

    /// Number of operations in the history.
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Whether the history is empty.
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Extract operations that involve a specific key.
    ///
    /// Matches `Read`, `Write`, `Cas`, and `SerialRead` operations whose
    /// `key` field equals the provided key. Other operation variants are
    /// excluded since they use table/pk addressing.
    pub fn filter_key(&self, key: &str) -> History {
        let ops = self
            .operations
            .iter()
            .filter(|op| op_matches_key(&op.op, key))
            .cloned()
            .collect();
        History { operations: ops }
    }

    /// Return the time range (earliest invoke, latest complete) in microseconds,
    /// or `None` if the history is empty.
    pub fn time_range(&self) -> Option<(u64, u64)> {
        if self.operations.is_empty() {
            return None;
        }
        let earliest = self
            .operations
            .iter()
            .map(|op| op.invoke_us)
            .min()
            .expect("non-empty history must have a minimum");
        let latest = self
            .operations
            .iter()
            .map(|op| op.complete_us)
            .max()
            .expect("non-empty history must have a maximum");
        Some((earliest, latest))
    }
}

/// Check whether an `Op` involves the given key.
fn op_matches_key(op: &Op, key: &str) -> bool {
    match op {
        Op::Read { key: k }
        | Op::Write { key: k, .. }
        | Op::Cas { key: k, .. }
        | Op::SerialRead { key: k } => k == key,
        Op::Transaction { statements } => statements.iter().any(|s| op_matches_key(s, key)),
        Op::InsertIfNotExists { .. } | Op::UpdateIf { .. } | Op::DeleteIf { .. } => false,
    }
}

/// Per-client history recorder.
///
/// Records invocation and completion timestamps for each operation, producing
/// a `History` when finished.
pub struct HistoryRecorder {
    client_id: String,
    pending: Option<(u64, Op)>,
    operations: Vec<Operation>,
}

impl HistoryRecorder {
    /// Create a new recorder for the given client.
    pub fn new(client_id: &str) -> Self {
        assert!(!client_id.is_empty(), "client_id must not be empty");
        Self {
            client_id: client_id.to_string(),
            pending: None,
            operations: Vec::new(),
        }
    }

    /// Record an operation invocation. Captures the current UTC timestamp.
    ///
    /// # Panics
    ///
    /// Panics if there is already a pending operation that has not been
    /// completed.
    pub fn invoke(&mut self, op: Op) {
        assert!(
            self.pending.is_none(),
            "cannot invoke while another operation is pending"
        );
        let now_us = chrono::Utc::now().timestamp_micros() as u64;
        self.pending = Some((now_us, op));
    }

    /// Record the completion of the pending operation. Captures the current
    /// UTC timestamp and pushes the finished `Operation` to the internal list.
    ///
    /// # Panics
    ///
    /// Panics if there is no pending operation.
    pub fn complete(&mut self, result: OpResult) {
        let (invoke_us, op) = self
            .pending
            .take()
            .expect("complete called with no pending operation");
        let complete_us = chrono::Utc::now().timestamp_micros() as u64;
        self.operations.push(Operation {
            client_id: self.client_id.clone(),
            invoke_us,
            complete_us,
            op,
            result,
        });
    }

    /// Consume the recorder and return the collected history.
    pub fn finish(self) -> History {
        assert!(
            self.pending.is_none(),
            "cannot finish with a pending operation"
        );
        History {
            operations: self.operations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build an Operation with explicit timestamps.
    fn make_op(client: &str, invoke: u64, complete: u64, op: Op, result: OpResult) -> Operation {
        Operation {
            client_id: client.to_string(),
            invoke_us: invoke,
            complete_us: complete,
            op,
            result,
        }
    }

    #[test]
    fn test_record_and_complete() {
        let mut rec = HistoryRecorder::new("c1");
        rec.invoke(Op::Write {
            key: "x".into(),
            value: 42,
        });
        rec.complete(OpResult::Ok);

        let history = rec.finish();
        assert_eq!(history.len(), 1);

        let op = &history.operations[0];
        assert_eq!(op.client_id, "c1");
        assert!(op.invoke_us <= op.complete_us);
        assert_eq!(
            op.op,
            Op::Write {
                key: "x".into(),
                value: 42
            }
        );
        assert_eq!(op.result, OpResult::Ok);
    }

    #[test]
    fn test_multiple_operations() {
        let mut rec = HistoryRecorder::new("c2");

        rec.invoke(Op::Write {
            key: "a".into(),
            value: 1,
        });
        rec.complete(OpResult::Ok);

        rec.invoke(Op::Read { key: "a".into() });
        rec.complete(OpResult::Value(Some(1)));

        rec.invoke(Op::Cas {
            key: "a".into(),
            expected: 1,
            value: 2,
        });
        rec.complete(OpResult::Applied(true));

        let history = rec.finish();
        assert_eq!(history.len(), 3);

        // Verify ordering: each invoke <= complete, and successive invokes are non-decreasing.
        for i in 0..history.operations.len() {
            let op = &history.operations[i];
            assert!(op.invoke_us <= op.complete_us);
            if i > 0 {
                assert!(op.invoke_us >= history.operations[i - 1].invoke_us);
            }
        }

        // Verify specific results.
        assert_eq!(history.operations[0].result, OpResult::Ok);
        assert_eq!(history.operations[1].result, OpResult::Value(Some(1)));
        assert_eq!(history.operations[2].result, OpResult::Applied(true));
    }

    #[test]
    fn test_merge_histories() {
        let h1 = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };
        let h2 = History {
            operations: vec![
                make_op(
                    "c2",
                    150,
                    250,
                    Op::Write {
                        key: "y".into(),
                        value: 2,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    350,
                    450,
                    Op::Read { key: "y".into() },
                    OpResult::Value(Some(2)),
                ),
            ],
        };

        let merged = History::merge(vec![h1, h2]);
        assert_eq!(merged.len(), 4);

        // Verify sorted by invoke_us.
        let invoke_times: Vec<u64> = merged.operations.iter().map(|o| o.invoke_us).collect();
        assert_eq!(invoke_times, vec![100, 150, 300, 350]);

        // Verify client ids interleave.
        let clients: Vec<&str> = merged
            .operations
            .iter()
            .map(|o| o.client_id.as_str())
            .collect();
        assert_eq!(clients, vec!["c1", "c2", "c1", "c2"]);
    }

    #[test]
    fn test_jsonl_roundtrip() {
        let original = History {
            operations: vec![
                make_op(
                    "c1",
                    1000,
                    2000,
                    Op::Write {
                        key: "k1".into(),
                        value: 10,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    3000,
                    4000,
                    Op::Read { key: "k1".into() },
                    OpResult::Value(Some(10)),
                ),
                make_op(
                    "c2",
                    2500,
                    3500,
                    Op::InsertIfNotExists {
                        table: "t1".into(),
                        pk: "pk1".into(),
                        values: vec![("col1".into(), "val1".into())],
                    },
                    OpResult::Applied(true),
                ),
            ],
        };

        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("history.jsonl");

        original.to_jsonl(&path).expect("write jsonl");
        let loaded = History::from_jsonl(&path).expect("read jsonl");

        assert_eq!(original, loaded);
    }

    #[test]
    fn test_filter_key() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    200,
                    300,
                    Op::Write {
                        key: "y".into(),
                        value: 2,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
                make_op(
                    "c1",
                    400,
                    500,
                    Op::Cas {
                        key: "y".into(),
                        expected: 2,
                        value: 3,
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c1",
                    500,
                    600,
                    Op::SerialRead { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };

        let filtered_x = history.filter_key("x");
        assert_eq!(filtered_x.len(), 3); // Write x, Read x, SerialRead x

        let filtered_y = history.filter_key("y");
        assert_eq!(filtered_y.len(), 2); // Write y, Cas y

        let filtered_z = history.filter_key("z");
        assert_eq!(filtered_z.len(), 0);
    }

    #[test]
    fn test_time_range() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    500,
                    700,
                    Op::Read { key: "a".into() },
                    OpResult::Value(None),
                ),
                make_op(
                    "c2",
                    100,
                    900,
                    Op::Write {
                        key: "b".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    800,
                    Op::Read { key: "c".into() },
                    OpResult::Value(None),
                ),
            ],
        };

        let (earliest, latest) = history.time_range().expect("non-empty history");
        assert_eq!(earliest, 100); // c2's invoke
        assert_eq!(latest, 900); // c2's complete
    }

    #[test]
    fn test_empty_history() {
        let empty = History { operations: vec![] };
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.time_range(), None);
        assert!(empty.filter_key("anything").is_empty());

        // Merge of empties is empty.
        let merged = History::merge(vec![empty.clone(), empty]);
        assert!(merged.is_empty());

        // JSONL roundtrip of empty.
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("empty.jsonl");
        merged.to_jsonl(&path).expect("write empty jsonl");
        let loaded = History::from_jsonl(&path).expect("read empty jsonl");
        assert!(loaded.is_empty());
    }
}
