//! Every PL/pgSQL statement of the Oban census (`ferrosa-postgres/tests/fixtures/
//! oban_census.sql`) is recognised and its body parses — the parse half of D13's acceptance.

use ferrosa_plpgsql::utility::{parse_utility, Utility};
use ferrosa_plpgsql::{parse_do_block, parse_function};

const CENSUS: &str = include_str!("../../ferrosa-postgres/tests/fixtures/oban_census.sql");

fn kind(statement: &Utility) -> &'static str {
    match statement {
        Utility::Do { .. } => "DO",
        Utility::CreateFunction(_) => "CREATE FUNCTION",
        Utility::DropFunction { .. } => "DROP FUNCTION",
        Utility::CreateTrigger(_) => "CREATE TRIGGER",
        Utility::DropTrigger { .. } => "DROP TRIGGER",
        Utility::Listen(_) => "LISTEN",
        Utility::Unlisten(_) => "UNLISTEN",
        Utility::Notify { .. } => "NOTIFY",
        Utility::PgNotify { .. } => "pg_notify",
    }
}

#[test]
fn every_census_plpgsql_statement_parses_with_its_body() {
    let mut seen = Vec::new();
    for line in CENSUS.lines() {
        let Some(parsed) = parse_utility(line) else {
            continue;
        };
        let statement = parsed.unwrap_or_else(|e| panic!("refused: {line}\n{e:?}"));
        match &statement {
            Utility::Do { body } => {
                parse_do_block(body).unwrap_or_else(|e| panic!("DO body: {e:?}\n{line}"));
            }
            Utility::CreateFunction(f) => {
                parse_function(&f.body, &f.signature)
                    .unwrap_or_else(|e| panic!("function body: {e:?}\n{line}"));
            }
            _ => {}
        }
        seen.push(kind(&statement));
    }
    assert_eq!(
        seen,
        vec![
            "DO",
            "CREATE FUNCTION",
            "DROP TRIGGER",
            "CREATE TRIGGER",
            "CREATE FUNCTION",
            "DROP FUNCTION",
            "CREATE FUNCTION",
            "CREATE TRIGGER",
            "DROP FUNCTION",
        ]
    );
}
