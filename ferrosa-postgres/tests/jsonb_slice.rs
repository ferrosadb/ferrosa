//! PG-first jsonb slice acceptance, no infrastructure (T-301, D24, D26, D2a,
//! D6b).
//!
//! `tokio-postgres` against the in-process server: `CREATE TABLE` with a jsonb
//! column through PG DDL; `INSERT` by literal, by text-format `$1` and by
//! binary-format `$1`; `SELECT` in text and in binary format. The expected
//! outputs are Postgres 16's, byte for byte; the postgres:16 differential in
//! `differential_oracle.rs` (`differential_oracle_jsonb_*`) checks the same
//! corpus against a real server, so a wrong expectation here fails there.

#[path = "common/jsonb_corpus.rs"]
mod jsonb_corpus;

use jsonb_corpus::{
    binary_of, invalid_corpus, run_corpus, start_ferrosa_pg, valid_corpus, Case, Expect, Outcome,
    Raw, Server, Via, CREATE_TABLE, PATHS,
};

async fn ferrosa_with_table() -> jsonb_corpus::FerrosaPg {
    let fx = start_ferrosa_pg().await;
    fx.client
        .batch_execute(CREATE_TABLE)
        .await
        .expect("CREATE TABLE with a jsonb column");
    fx
}

/// Every mismatch, not only the first, so one run shows the whole picture.
fn disagreements(cases: &[Case], got: &[(String, Via, Outcome)]) -> Vec<String> {
    let mut problems = Vec::new();
    assert_eq!(got.len(), cases.len() * PATHS.len(), "one outcome per path");
    for (case, chunk) in cases.iter().zip(got.chunks(PATHS.len())) {
        for (label, path, outcome) in chunk {
            let want = match &case.expect {
                Expect::Text(text) => Outcome::Stored {
                    text: text.clone(),
                    binary: binary_of(text),
                },
                Expect::Err(code) => Outcome::Refused((*code).to_string()),
            };
            if *outcome != want {
                problems.push(format!(
                    "[{label} / {path:?}] input {:.60?}\n    want {:.120?}\n    got  {:.120?}",
                    case.input, want, outcome
                ));
            }
        }
    }
    problems
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonb_slice_valid_corpus_round_trips_on_every_path_and_format() {
    let fx = ferrosa_with_table().await;
    let cases = valid_corpus();
    assert!(cases.len() >= 50, "the corpus is realistic in size");
    let got = run_corpus(&fx.client, Server::Ferrosa, &cases)
        .await
        .expect("the corpus runs");
    let problems = disagreements(&cases, &got);
    assert!(
        problems.is_empty(),
        "{} of {} runs differ from Postgres 16:\n{}",
        problems.len(),
        got.len(),
        problems.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonb_slice_invalid_corpus_is_refused_with_the_postgres_sqlstate_and_no_row() {
    let fx = ferrosa_with_table().await;
    let cases = invalid_corpus();
    let got = run_corpus(&fx.client, Server::Ferrosa, &cases)
        .await
        .expect("the corpus runs and no refused write leaves a row");
    let problems = disagreements(&cases, &got);
    assert!(
        problems.is_empty(),
        "{} of {} runs differ from Postgres 16:\n{}",
        problems.len(),
        got.len(),
        problems.join("\n")
    );
    let rows = fx
        .client
        .simple_query("SELECT id FROM t")
        .await
        .expect("count rows");
    let stored = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(stored, 0, "no invalid document was stored");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonb_slice_all_three_input_paths_store_the_same_bytes() {
    let fx = ferrosa_with_table().await;
    let cases = valid_corpus();
    let got = run_corpus(&fx.client, Server::Ferrosa, &cases)
        .await
        .expect("the corpus runs");
    for chunk in got.chunks(PATHS.len()) {
        let first = &chunk[0];
        for other in &chunk[1..] {
            assert_eq!(
                first.2, other.2,
                "[{}] {:?} and {:?} disagree",
                first.0, first.1, other.1
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonb_slice_bad_binary_version_byte_is_refused_and_writes_nothing() {
    let fx = ferrosa_with_table().await;
    let stmt = fx
        .client
        .prepare("INSERT INTO t (id, doc) VALUES ($1, $2)")
        .await
        .expect("prepare");
    for version in [0u8, 2, 255] {
        let error = fx
            .client
            .execute(&stmt, &[&1i32, &Raw::binary(version, "{}")])
            .await
            .expect_err("a bad version byte is refused");
        assert_eq!(
            jsonb_corpus::code_of(&error),
            jsonb_corpus::BAD_VERSION_SQLSTATE,
            "version {version}"
        );
    }
    let empty = fx
        .client
        .execute(&stmt, &[&1i32, &Raw::binary_bytes(Vec::new())])
        .await
        .expect_err("a binary value with no version byte is refused");
    assert_eq!(
        jsonb_corpus::code_of(&empty),
        jsonb_corpus::MISSING_VERSION_SQLSTATE
    );
    let rows = fx
        .client
        .simple_query("SELECT id FROM t")
        .await
        .expect("count rows");
    assert!(
        !rows
            .iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))),
        "the refused writes left no row"
    );
}
