//! D13 end to end: PL/pgSQL compiled to a component agrees with the interpreter.
//!
//! Every case is run twice against identically scripted hosts — once by
//! `ferrosa_plpgsql::interpret`, once compiled (`component::compile_plpgsql`) and called through
//! `UdfExecutor::call_plpgsql` — and both the result and the full sequence of host calls must
//! match. This is the executable counterpart of `Ferrosa.Sql.Plpgsql.convertList_agrees`.
//!
//! Needs the AssemblyScript toolchain bundle:
//! `FERROSA_ASC_BUNDLE=/path/asc-bundle.mjs cargo test -p ferrosa-udf --features 'asc-udf live-infra-tests' --test plpgsql_compiled`
#![cfg(all(feature = "asc-udf", feature = "live-infra-tests"))]

use ferrosa_plpgsql::testing::{bool_value, text, Call as HostCall, ScriptedHost};
use ferrosa_plpgsql::{
    interpret, parse_do_block, parse_function, Call, FunctionSignature, Program, ReturnType,
    SqlError, TriggerCall, Value,
};
use ferrosa_udf::component::compile_plpgsql;
use ferrosa_udf::{SandboxConfig, UdfExecutor};

fn require_bundle() {
    assert!(
        std::env::var_os("FERROSA_ASC_BUNDLE").is_some(),
        "FERROSA_ASC_BUNDLE is not set. Build the bundle with\n  \
         bash ferrosa-udf/examples/asc-poc/build-bundle.sh /tmp/asc-host/asc-bundle.mjs\n\
         and run with FERROSA_ASC_BUNDLE=/tmp/asc-host/asc-bundle.mjs"
    );
}

type Outcome = (Result<Option<Value>, SqlError>, Vec<HostCall>);

fn interpreted(program: &Program, call: Call, host: ScriptedHost) -> Outcome {
    let mut host = host;
    let result = interpret(program, call, &mut host);
    (result, host.calls())
}

/// Run a case both ways and compare everything.
fn agree(label: &str, program: Program, call: Call, script: impl Fn() -> ScriptedHost) {
    require_bundle();
    let executor = UdfExecutor::new(SandboxConfig::default()).expect("executor");
    let (want, want_calls) = interpreted(&program, call.clone(), script());
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let host = Logged {
        inner: script(),
        log: std::sync::Arc::clone(&log),
    };
    let bytes = compile_plpgsql(&program).unwrap_or_else(|e| panic!("{label}: compile: {e}"));
    let component = executor.load_plpgsql(&bytes).expect("component loads");
    let (got, _host) = executor.call_plpgsql(&component, call, Box::new(host));
    let got_calls = log.lock().expect("log").clone(); // snapshot of the shared log for comparison
    assert_eq!(got, want, "{label}: result");
    assert_eq!(got_calls, want_calls, "{label}: host calls");
}

/// A scripted host that also appends every call to a shared log the test reads afterwards
/// (the host itself is moved into the wasm store).
struct Logged {
    inner: ScriptedHost,
    log: std::sync::Arc<std::sync::Mutex<Vec<HostCall>>>,
}

impl Logged {
    fn sync(&self) {
        *self.log.lock().expect("log") = self.inner.calls();
    }
}

impl ferrosa_plpgsql::Host for Logged {
    fn query(&mut self, sql: &str, params: &[Value]) -> Result<u32, SqlError> {
        let r = self.inner.query(sql, params);
        self.sync();
        r
    }
    fn fetch(&mut self, cursor: u32, max: u32) -> Result<Vec<Vec<Value>>, SqlError> {
        let r = self.inner.fetch(cursor, max);
        self.sync();
        r
    }
    fn close(&mut self, cursor: u32) {
        self.inner.close(cursor);
        self.sync();
    }
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<u64, SqlError> {
        let r = self.inner.execute(sql, params);
        self.sync();
        r
    }
    fn notify(&mut self, channel: &str, payload: &str) -> Result<(), SqlError> {
        let r = self.inner.notify(channel, payload);
        self.sync();
        r
    }
}

const CENSUS_DO: &str = " BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'oban_job_state' AND typnamespace = 'public'::regnamespace::oid) THEN CREATE TYPE \"public\".oban_job_state AS ENUM ( 'available', 'suspended', 'scheduled', 'executing', 'retryable', 'completed', 'discarded', 'cancelled' ); END IF; END";

const OBAN_NOTIFY_V2: &str = " DECLARE channel text; notice json; BEGIN IF NEW.state = 'available' THEN channel = 'public.oban_insert'; notice = json_build_object('queue', NEW.queue); PERFORM pg_notify(channel, notice::text); END IF; RETURN NULL; END; ";

fn trigger_sig() -> FunctionSignature {
    FunctionSignature {
        args: vec![],
        returns: ReturnType::Trigger,
    }
}

fn int_fn(body: &str) -> Program {
    let sig = FunctionSignature {
        args: vec![(Some("v".into()), "int".into())],
        returns: ReturnType::Scalar("int".into()),
    };
    parse_function(body, &sig).expect("parses")
}

fn int_call(v: &str) -> Call {
    Call {
        args: vec![Value::text(23, v)],
        trigger: None,
    }
}

fn insert_trigger(state: Value) -> Call {
    Call {
        args: vec![],
        trigger: Some(TriggerCall {
            op: "INSERT".into(),
            new_row: Some(vec![state, text("default")]),
            old_row: None,
        }),
    }
}

fn notify_script() -> ScriptedHost {
    ScriptedHost::default()
        .answer("SELECT $1 = 'available'", vec![vec![bool_value(true)]])
        .answer(
            "SELECT 'public.oban_insert'",
            vec![vec![text("public.oban_insert")]],
        )
        .answer(
            "SELECT json_build_object",
            vec![vec![Value::text(114, "{\"queue\":\"default\"}")]],
        )
        .answer(
            "SELECT $1::text",
            vec![vec![text("{\"queue\":\"default\"}")]],
        )
}

#[test]
fn the_census_do_block_agrees_when_the_type_is_absent_and_present() {
    let program = parse_do_block(CENSUS_DO).expect("parses");
    agree(
        "absent",
        program.clone(),
        Call::default(),
        ScriptedHost::default,
    );
    agree("present", program, Call::default(), || {
        ScriptedHost::default().answer("SELECT 1 FROM pg_type", vec![vec![text("1")]; 3])
    });
}

#[test]
fn the_oban_notify_trigger_agrees_on_both_branches() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger_sig()).expect("parses");
    agree(
        "available",
        program.clone(),
        insert_trigger(text("available")),
        notify_script,
    );
    agree(
        "null state",
        program,
        insert_trigger(Value::null(25)),
        || ScriptedHost::default().answer("SELECT $1 = 'available'", vec![vec![Value::null(16)]]),
    );
}

#[test]
fn scalar_functions_agree_on_values_and_errors() {
    agree(
        "bare variable",
        int_fn("BEGIN RETURN v; END"),
        int_call("7"),
        ScriptedHost::default,
    );
    agree(
        "select",
        int_fn("BEGIN RETURN v + 1; END"),
        int_call("7"),
        || ScriptedHost::default().answer("SELECT $1 + 1", vec![vec![Value::text(23, "8")]]),
    );
    agree(
        "no return",
        int_fn("BEGIN NULL; END"),
        int_call("1"),
        ScriptedHost::default,
    );
    agree(
        "cast refused",
        int_fn("BEGIN RETURN 'x'; END"),
        int_call("1"),
        || ScriptedHost::default().answer("SELECT 'x'", vec![vec![text("x")]]),
    );
    agree(
        "two columns",
        int_fn("BEGIN RETURN 1, 2; END"),
        int_call("1"),
        || ScriptedHost::default().answer("SELECT 1, 2", vec![vec![text("1"), text("2")]]),
    );
}

#[test]
fn statements_agree_on_host_traffic() {
    let program = parse_do_block(
        "DECLARE a text; b text; BEGIN SELECT v INTO a FROM t WHERE k = 1; SELECT v INTO b FROM t WHERE k = 2; PERFORM a || b; INSERT INTO t (k, v) VALUES (3, a); END",
    )
    .expect("parses");
    agree(
        "select into + perform + insert",
        program,
        Call::default(),
        || {
            ScriptedHost::default()
                .answer(
                    "SELECT v FROM t WHERE k = 1",
                    vec![vec![text("x")], vec![text("y")]],
                )
                .answer("SELECT $1 || $2", vec![vec![text("x")]; 150])
        },
    );
    let program = parse_do_block("BEGIN PERFORM 1; INSERT INTO t VALUES (1); END").expect("parses");
    agree("host error", program, Call::default(), || {
        ScriptedHost::default().fail_fetch("SELECT 1", "22012", "division by zero")
    });
    let program = parse_do_block("BEGIN IF 1 THEN NULL; END IF; END").expect("parses");
    agree("non-boolean", program, Call::default(), || {
        ScriptedHost::default().answer("SELECT 1", vec![vec![Value::text(23, "1")]])
    });
    let program = parse_do_block("BEGIN PERFORM pg_notify('', 'x'); END").expect("parses");
    agree("empty channel", program, Call::default(), || {
        ScriptedHost::default().answer("SELECT ''", vec![vec![text("")]])
    });
}

#[test]
fn trigger_call_shapes_agree() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger_sig()).expect("parses");
    agree(
        "direct call",
        program.clone(),
        Call::default(),
        ScriptedHost::default,
    );
    let bad = Call {
        args: vec![],
        trigger: Some(TriggerCall {
            op: "INSERT".into(),
            new_row: Some(vec![text("one")]),
            old_row: None,
        }),
    };
    agree("row shape", program, bad, ScriptedHost::default);
}

#[test]
fn a_runaway_guest_is_stopped_by_fuel_not_left_running() {
    require_bundle();
    // PERFORM drains until the host returns an empty batch; a host that never does makes the
    // guest loop, and the sandbox must stop it with a resource error.
    struct Endless;
    impl ferrosa_plpgsql::Host for Endless {
        fn query(&mut self, _: &str, _: &[Value]) -> Result<u32, SqlError> {
            Ok(1)
        }
        fn fetch(&mut self, _: u32, _: u32) -> Result<Vec<Vec<Value>>, SqlError> {
            Ok(vec![vec![]])
        }
        fn close(&mut self, _: u32) {}
        fn execute(&mut self, _: &str, _: &[Value]) -> Result<u64, SqlError> {
            Ok(0)
        }
        fn notify(&mut self, _: &str, _: &str) -> Result<(), SqlError> {
            Ok(())
        }
    }
    let config = SandboxConfig {
        max_fuel: 2_000_000,
        ..SandboxConfig::default()
    };
    let executor = UdfExecutor::new(config).expect("executor");
    let program = parse_do_block("BEGIN PERFORM 1; END").expect("parses");
    let bytes = compile_plpgsql(&program).expect("compiles");
    let component = executor.load_plpgsql(&bytes).expect("loads");
    let (result, _) = executor.call_plpgsql(&component, Call::default(), Box::new(Endless));
    let error = result.expect_err("stopped");
    assert_eq!(error.sqlstate, "54000", "{error}");
}
