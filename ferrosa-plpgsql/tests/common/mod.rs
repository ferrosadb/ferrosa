//! A scripted [`Host`] that records every call.
#![allow(dead_code)] // each test binary uses a different subset

use std::collections::HashMap;

use ferrosa_plpgsql::{CursorId, Host, SqlError, Value, OID_BOOL, OID_TEXT};

/// One recorded host call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Call {
    Query(String, Vec<Value>),
    Fetch(u32),
    Close,
    Execute(String, Vec<Value>),
    Notify(String, String),
}

pub fn text(s: &str) -> Value {
    Value::text(OID_TEXT, s)
}

pub fn bool_value(b: bool) -> Value {
    Value::text(OID_BOOL, if b { "t" } else { "f" })
}

/// Answers each query whose SQL starts with a scripted prefix with its rows; any other query
/// yields no rows.
#[derive(Default)]
pub struct ScriptedHost {
    answers: Vec<(String, Vec<Vec<Value>>)>,
    fetch_failures: Vec<(String, SqlError)>,
    cursors: HashMap<CursorId, (std::vec::IntoIter<Vec<Value>>, Option<SqlError>)>,
    next: CursorId,
    calls: Vec<Call>,
    produced: usize,
}

impl ScriptedHost {
    pub fn answer(mut self, prefix: &str, rows: Vec<Vec<Value>>) -> Self {
        self.answers.push((prefix.to_string(), rows));
        self
    }

    pub fn fail_fetch(mut self, prefix: &str, sqlstate: &str, message: &str) -> Self {
        self.fetch_failures
            .push((prefix.to_string(), SqlError::new(sqlstate, message)));
        self
    }

    pub fn calls(&self) -> Vec<Call> {
        self.calls.clone() // test helper: the caller inspects a snapshot
    }

    pub fn rows_produced(&self) -> usize {
        self.produced
    }
}

impl Host for ScriptedHost {
    fn query(&mut self, sql: &str, params: &[Value]) -> Result<CursorId, SqlError> {
        self.calls
            .push(Call::Query(sql.to_string(), params.to_vec()));
        let rows = self
            .answers
            .iter()
            .find(|(p, _)| sql.starts_with(p.as_str()))
            .map(|(_, rows)| rows.clone()) // test helper: a script may answer many queries
            .unwrap_or_default();
        let failure = self
            .fetch_failures
            .iter()
            .find(|(p, _)| sql.starts_with(p.as_str()))
            .map(|(_, e)| e.clone()); // test helper
        self.next += 1;
        self.cursors.insert(self.next, (rows.into_iter(), failure));
        Ok(self.next)
    }

    fn fetch(&mut self, cursor: CursorId, max: u32) -> Result<Vec<Vec<Value>>, SqlError> {
        self.calls.push(Call::Fetch(max));
        let (rows, failure) = self.cursors.get_mut(&cursor).expect("open cursor");
        if let Some(error) = failure.take() {
            return Err(error);
        }
        let batch: Vec<_> = rows.take(max as usize).collect();
        self.produced += batch.len();
        Ok(batch)
    }

    fn close(&mut self, cursor: CursorId) {
        self.calls.push(Call::Close);
        assert!(
            self.cursors.remove(&cursor).is_some(),
            "close of an unknown cursor"
        );
    }

    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, SqlError> {
        self.calls
            .push(Call::Execute(sql.to_string(), params.to_vec()));
        Ok(1)
    }

    fn notify(&mut self, channel: &str, payload: &str) -> Result<(), SqlError> {
        self.calls
            .push(Call::Notify(channel.to_string(), payload.to_string()));
        Ok(())
    }
}
