//! The converter's output shape. That the output, compiled and run, agrees with the
//! interpreter is checked end to end in `ferrosa-udf` (it needs the AssemblyScript compiler);
//! here the contract with the component world is pinned: imports, exports, slots, and the
//! SQL each statement sends.

use ferrosa_plpgsql::assemblyscript::{convert, HOST_MODULE};
use ferrosa_plpgsql::{parse_do_block, parse_function, FunctionSignature, ReturnType};

const OBAN_NOTIFY_V2: &str = " DECLARE channel text; notice json; BEGIN IF NEW.state = 'available' THEN channel = 'public.oban_insert'; notice = json_build_object('queue', NEW.queue); PERFORM pg_notify(channel, notice::text); END IF; RETURN NULL; END; ";

fn bytes_of(s: &str) -> String {
    s.bytes()
        .map(|b| b.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn trigger() -> FunctionSignature {
    FunctionSignature {
        args: vec![],
        returns: ReturnType::Trigger,
    }
}

#[test]
fn the_output_imports_the_host_and_exports_the_routine_world() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger()).expect("parses");
    let source = convert(&program);
    assert_eq!(HOST_MODULE, "ferrosa:plpgsql/host@0.1.0");
    for import in ["query", "fetch", "close", "execute", "notify"] {
        assert!(
            source.contains(&format!("@external(\"{HOST_MODULE}\", \"{import}\")")),
            "missing import {import}"
        );
    }
    for export in [
        "export function call(",
        "export function cabi_realloc(",
        "export function cabi_post_call(",
    ] {
        assert!(source.contains(export), "missing {export}");
    }
}

#[test]
fn every_slot_is_allocated_and_trigger_fields_are_bound_in_order() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger()).expect("parses");
    // channel, notice, NEW.state, NEW.queue
    assert_eq!(program.variables.len(), 4);
    let source = convert(&program);
    assert!(source.contains("__slots = __alloc(16 * 4);"), "{source}");
    assert!(
        source.contains("if (newDisc != 0 && newLen != 2)"),
        "NEW shape is checked"
    );
    assert!(
        source.contains("if (tDisc == 0)"),
        "a direct call is refused"
    );
}

#[test]
fn each_expression_sends_select_with_its_sql_as_utf8_bytes() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger()).expect("parses");
    let source = convert(&program);
    for sql in [
        "SELECT $1 = 'available'",
        "SELECT 'public.oban_insert'",
        "SELECT json_build_object('queue', $1)",
        "SELECT $1::text",
        "SELECT NULL",
    ] {
        assert!(
            source.contains(&format!("memory.data<u8>([{}])", bytes_of(sql))),
            "missing {sql}"
        );
    }
}

#[test]
fn a_bare_variable_is_read_from_its_slot_without_a_host_call() {
    let sig = FunctionSignature {
        args: vec![(Some("v".into()), "int".into())],
        returns: ReturnType::Scalar("int".into()),
    };
    let program = parse_function("BEGIN RETURN v; END", &sig).expect("parses");
    let source = convert(&program);
    let body = &source[source.find("export function call(").expect("entry")..];
    assert!(
        !body.contains("__scalar("),
        "no SELECT for a bare variable: {body}"
    );
    assert!(body.contains("__fit("), "the return value is fitted to int");
}

#[test]
fn the_census_do_block_converts_to_an_exists_test_guarding_an_execute() {
    let program = parse_do_block(" BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'x') THEN CREATE TYPE t AS ENUM ('a'); END IF; END").expect("parses");
    let source = convert(&program);
    let body = &source[source.find("export function call(").expect("entry")..];
    assert!(body.contains("__first("));
    assert!(
        body.contains("!= true"),
        "NOT EXISTS negates the found flag: {body}"
    );
    assert!(body.contains("__execute("));
    assert!(body.contains(&bytes_of("CREATE TYPE t AS ENUM ('a')")));
}

#[test]
fn conversion_is_deterministic() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger()).expect("parses");
    assert_eq!(convert(&program), convert(&program));
}

#[test]
fn every_failure_check_returns_the_error_result() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger()).expect("parses");
    let source = convert(&program);
    let body = &source[source.find("export function call(").expect("entry")..];
    let calls = body.matches("__scalar(").count() + body.matches("__notify(").count();
    let checks = body.matches("if (__failed) return __err();").count();
    assert!(
        checks >= calls,
        "{checks} checks for {calls} fallible calls"
    );
}
