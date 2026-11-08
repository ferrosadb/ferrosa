//! The interpreter against a scripted host: every host call it makes, in order, and what it
//! returns.

mod common;

use common::{bool_value, text, Call as HostCall, ScriptedHost};
use ferrosa_plpgsql::{
    interpret, parse_do_block, parse_function, Call, FunctionSignature, ReturnType, TriggerCall,
    Value, FETCH_BATCH,
};

const CENSUS_DO: &str = " BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'oban_job_state' AND typnamespace = 'public'::regnamespace::oid) THEN CREATE TYPE \"public\".oban_job_state AS ENUM ( 'available', 'suspended', 'scheduled', 'executing', 'retryable', 'completed', 'discarded', 'cancelled' ); END IF; END";

const OBAN_NOTIFY_V2: &str = " DECLARE channel text; notice json; BEGIN IF NEW.state = 'available' THEN channel = 'public.oban_insert'; notice = json_build_object('queue', NEW.queue); PERFORM pg_notify(channel, notice::text); END IF; RETURN NULL; END; ";

fn trigger() -> FunctionSignature {
    FunctionSignature {
        args: vec![],
        returns: ReturnType::Trigger,
    }
}

#[test]
fn the_census_do_block_creates_the_type_when_it_is_absent() {
    let program = parse_do_block(CENSUS_DO).expect("parses");
    let mut host = ScriptedHost::default(); // every query: no rows
    let result = interpret(&program, Call::default(), &mut host).expect("runs");
    assert_eq!(result, None);
    let calls = host.calls();
    assert!(
        matches!(&calls[0], HostCall::Query(sql, p) if sql.starts_with("SELECT 1 FROM pg_type") && p.is_empty())
    );
    assert_eq!(
        calls[1],
        HostCall::Fetch(1),
        "EXISTS asks for one row, not a batch"
    );
    assert_eq!(calls[2], HostCall::Close);
    assert!(
        matches!(&calls[3], HostCall::Execute(sql, _) if sql.starts_with("CREATE TYPE \"public\".oban_job_state"))
    );
    assert_eq!(calls.len(), 4);
}

#[test]
fn the_census_do_block_does_nothing_when_the_type_exists() {
    let program = parse_do_block(CENSUS_DO).expect("parses");
    let mut host =
        ScriptedHost::default().answer("SELECT 1 FROM pg_type", vec![vec![text("1")]; 5]);
    interpret(&program, Call::default(), &mut host).expect("runs");
    let calls = host.calls();
    assert_eq!(
        calls.len(),
        3,
        "query, fetch(1), close — no CREATE TYPE: {calls:?}"
    );
    assert_eq!(
        host.rows_produced(),
        1,
        "the host produced one row of the five"
    );
}

#[test]
fn assignments_evaluate_as_select_and_bind_variables_as_parameters() {
    let program = parse_do_block("DECLARE x text; BEGIN x := 'a' || 'b'; PERFORM x || 'c'; END")
        .expect("parses");
    let mut host = ScriptedHost::default().answer("SELECT 'a' || 'b'", vec![vec![text("ab")]]);
    interpret(&program, Call::default(), &mut host).expect("runs");
    let calls = host.calls();
    assert_eq!(
        calls[0],
        HostCall::Query("SELECT 'a' || 'b'".into(), vec![])
    );
    assert_eq!(
        calls[3],
        HostCall::Query("SELECT $1 || 'c'".into(), vec![text("ab")])
    );
}

#[test]
fn perform_drains_its_query_in_bounded_batches() {
    let program = parse_do_block("BEGIN PERFORM 1; END").expect("parses");
    let mut host = ScriptedHost::default().answer("SELECT 1", vec![vec![text("1")]; 150]);
    interpret(&program, Call::default(), &mut host).expect("runs");
    let fetches: Vec<_> = host
        .calls()
        .into_iter()
        .filter(|c| matches!(c, HostCall::Fetch(_)))
        .collect();
    assert_eq!(
        fetches,
        vec![HostCall::Fetch(FETCH_BATCH); 4],
        "64 + 64 + 22 + empty"
    );
    assert_eq!(host.rows_produced(), 150);
}

#[test]
fn a_trigger_function_notifies_with_new_bound() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger()).expect("parses");
    assert_eq!(program.new_fields(), vec!["state", "queue"]);
    let mut host = ScriptedHost::default()
        .answer("SELECT $1 = 'available'", vec![vec![bool_value(true)]])
        .answer(
            "SELECT 'public.oban_insert'",
            vec![vec![text("public.oban_insert")]],
        )
        .answer(
            "SELECT json_build_object",
            vec![vec![Value::text(114, "{\"queue\" : \"default\"}")]],
        )
        .answer(
            "SELECT $1::text",
            vec![vec![text("{\"queue\" : \"default\"}")]],
        );
    let call = Call {
        args: vec![],
        trigger: Some(TriggerCall {
            op: "INSERT".into(),
            new_row: Some(vec![text("available"), text("default")]),
            old_row: None,
        }),
    };
    let result = interpret(&program, call, &mut host).expect("runs");
    assert_eq!(result, Some(Value::null(0)), "RETURN NULL");
    let calls = host.calls();
    assert_eq!(
        calls[0],
        HostCall::Query("SELECT $1 = 'available'".into(), vec![text("available")])
    );
    assert!(calls.contains(&HostCall::Query(
        "SELECT json_build_object('queue', $1)".into(),
        vec![text("default")]
    )));
    let notify = HostCall::Notify(
        "public.oban_insert".into(),
        "{\"queue\" : \"default\"}".into(),
    );
    let at = calls.iter().position(|c| *c == notify).expect("notified");
    // Then RETURN NULL: `SELECT NULL`, one row read, closed.
    assert_eq!(calls[at + 1], HostCall::Query("SELECT NULL".into(), vec![]));
    assert_eq!(calls.len(), at + 4);
}

#[test]
fn a_false_or_null_condition_takes_the_else_branch() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger()).expect("parses");
    let mut host =
        ScriptedHost::default().answer("SELECT $1 = 'available'", vec![vec![Value::null(16)]]);
    let call = Call {
        args: vec![],
        trigger: Some(TriggerCall {
            op: "INSERT".into(),
            new_row: Some(vec![Value::null(25), text("q")]),
            old_row: None,
        }),
    };
    interpret(&program, call, &mut host).expect("runs");
    assert!(!host
        .calls()
        .iter()
        .any(|c| matches!(c, HostCall::Notify(..))));
}

#[test]
fn a_scalar_function_returns_its_value_and_reads_a_bare_variable_directly() {
    let sig = FunctionSignature {
        args: vec![(Some("v".into()), "int".into())],
        returns: ReturnType::Scalar("int".into()),
    };
    let program = parse_function("BEGIN RETURN v; END", &sig).expect("parses");
    let mut host = ScriptedHost::default();
    let result = interpret(
        &program,
        Call {
            args: vec![Value::text(23, "7")],
            trigger: None,
        },
        &mut host,
    )
    .expect("runs");
    assert_eq!(result, Some(Value::text(23, "7")));
    assert!(
        host.calls().is_empty(),
        "`SELECT $1` is the variable itself"
    );
}

#[test]
fn falling_off_the_end_of_a_function_is_2f005() {
    let sig = FunctionSignature {
        args: vec![],
        returns: ReturnType::Scalar("int".into()),
    };
    let program = parse_function("BEGIN NULL; END", &sig).expect("parses");
    let error =
        interpret(&program, Call::default(), &mut ScriptedHost::default()).expect_err("no RETURN");
    assert_eq!(error.sqlstate, "2F005");
}

#[test]
fn select_into_takes_the_first_row_and_null_when_there_is_none() {
    let program = parse_do_block(
        "DECLARE a text; b text; BEGIN SELECT v INTO a FROM t WHERE k = 1; SELECT v INTO b FROM t WHERE k = 2; PERFORM a || b; END",
    )
    .expect("parses");
    let mut host = ScriptedHost::default().answer(
        "SELECT v FROM t WHERE k = 1",
        vec![vec![text("x")], vec![text("y")]],
    );
    interpret(&program, Call::default(), &mut host).expect("runs");
    let last_query = host
        .calls()
        .into_iter()
        .rev()
        .find(|c| matches!(c, HostCall::Query(..)));
    assert_eq!(
        last_query,
        Some(HostCall::Query(
            "SELECT $1 || $2".into(),
            vec![text("x"), Value::null(25)]
        ))
    );
}

#[test]
fn a_host_error_stops_the_run_and_closes_the_cursor() {
    let program = parse_do_block("BEGIN PERFORM 1; INSERT INTO t VALUES (1); END").expect("parses");
    let mut host = ScriptedHost::default().fail_fetch("SELECT 1", "22012", "division by zero");
    let error = interpret(&program, Call::default(), &mut host).expect_err("fails");
    assert_eq!(error.sqlstate, "22012");
    assert!(host.calls().contains(&HostCall::Close));
    assert!(!host
        .calls()
        .iter()
        .any(|c| matches!(c, HostCall::Execute(..))));
}

#[test]
fn an_expression_returning_two_columns_is_refused() {
    let program = parse_do_block("DECLARE x text; BEGIN x := 1, 2; END").expect("parses");
    let mut host = ScriptedHost::default().answer("SELECT 1, 2", vec![vec![text("1"), text("2")]]);
    let error = interpret(&program, Call::default(), &mut host).expect_err("two columns");
    assert_eq!(error.sqlstate, "42601");
}

#[test]
fn a_non_boolean_condition_is_42804() {
    let program = parse_do_block("BEGIN IF 1 THEN NULL; END IF; END").expect("parses");
    let mut host = ScriptedHost::default().answer("SELECT 1", vec![vec![Value::text(23, "1")]]);
    let error = interpret(&program, Call::default(), &mut host).expect_err("not boolean");
    assert_eq!(error.sqlstate, "42804");
}

#[test]
fn an_assignment_of_another_type_is_refused_rather_than_silently_kept() {
    let program = parse_do_block("DECLARE n int; BEGIN n := 'abc'; END").expect("parses");
    let mut host = ScriptedHost::default().answer("SELECT 'abc'", vec![vec![text("abc")]]);
    let error = interpret(&program, Call::default(), &mut host).expect_err("type");
    assert_eq!(error.sqlstate, "42804");
}

#[test]
fn a_trigger_function_called_without_a_trigger_is_refused() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger()).expect("parses");
    let error = interpret(&program, Call::default(), &mut ScriptedHost::default())
        .expect_err("not a trigger call");
    assert_eq!(error.sqlstate, "0A000");
}

#[test]
fn a_trigger_call_with_the_wrong_row_shape_is_refused() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger()).expect("parses");
    let call = Call {
        args: vec![],
        trigger: Some(TriggerCall {
            op: "INSERT".into(),
            new_row: Some(vec![text("only one")]),
            old_row: None,
        }),
    };
    let error = interpret(&program, call, &mut ScriptedHost::default()).expect_err("shape");
    assert_eq!(error.sqlstate, "XX000");
}

#[test]
fn declarations_are_initialised_in_order_and_default_to_null() {
    let program =
        parse_do_block("DECLARE a text := 'x'; b text; BEGIN PERFORM a || b; END").expect("parses");
    let mut host = ScriptedHost::default().answer("SELECT 'x'", vec![vec![text("x")]]);
    interpret(&program, Call::default(), &mut host).expect("runs");
    assert!(host.calls().contains(&HostCall::Query(
        "SELECT $1 || $2".into(),
        vec![text("x"), Value::null(25)]
    )));
}
