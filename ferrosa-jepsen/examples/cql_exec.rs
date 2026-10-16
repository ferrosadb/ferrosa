//! Run CQL statements against ONE node and report what happened, for the
//! multi-DC invariant harness (`deploy/fly-accord-elle/certify-multi-dc.sh`).
//!
//! `elle_list_append` drives a workload; this asks a node a question. It is
//! deliberately narrow, so that a shell can assert on it without parsing CQL
//! rows:
//!
//!   cql_exec <host:port> <statement> [<statement> ...]
//!
//! For each statement it prints `OK <statement>` or `ROWS <n> <statement>`, and
//! exits non-zero on the first statement the node refuses. That makes two kinds
//! of assertion one-liners:
//!
//! * does this node have this schema? — `USE <keyspace>` fails when it does not.
//! * what does this node say about itself? — `SELECT data_center FROM system.local
//!   WHERE data_center = 'dc1'` prints `ROWS 1` when the row matches and `ROWS 0`
//!   when it does not, which is a boolean without decoding any CQL value.
//!
//! The consistency of the statement is whatever the node defaults to; the DC
//! harness varies it with `cql_exec_cl`-style explicit statements when a CL
//! matters, which keeps this binary free of a CL argument it would otherwise
//! have to thread through the driver.

use anyhow::Result;
use scylla::client::session_builder::SessionBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((contact, statements)) = args.split_first() else {
        eprintln!("usage: cql_exec <host:port> <statement> [<statement> ...]");
        std::process::exit(2);
    };
    if statements.is_empty() {
        eprintln!("cql_exec needs at least one statement");
        std::process::exit(2);
    }

    let session = SessionBuilder::new().known_node(contact).build().await?;

    for statement in statements {
        match session.query_unpaged(statement.as_str(), &[]).await {
            Ok(result) => match result.into_rows_result() {
                // A SELECT: the row count is the answer, and it is all a shell
                // assertion needs.
                Ok(rows) => println!("ROWS {} {}", rows.rows_num(), statement),
                // A statement that returns no rows (DDL, USE, INSERT): success is
                // the answer.
                Err(_) => println!("OK {statement}"),
            },
            Err(e) => {
                eprintln!("ERROR {statement}: {e}");
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
