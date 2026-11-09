//! The PL/pgSQL subset's parser: what it accepts (every construct the Oban census uses) and
//! what it refuses (by name, with `0A000`).

use ferrosa_plpgsql::{
    parse_do_block, parse_function, Cond, FunctionSignature, ReturnType, RoutineKind, Stmt, VarKind,
};

/// The census's DO block (Oban migration v1), verbatim.
const CENSUS_DO: &str = " BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'oban_job_state' AND typnamespace = 'public'::regnamespace::oid) THEN CREATE TYPE \"public\".oban_job_state AS ENUM ( 'available', 'suspended', 'scheduled', 'executing', 'retryable', 'completed', 'discarded', 'cancelled' ); END IF; END";

/// Oban's v1 notify trigger function as Oban sends it (with its newlines: the census flattens
/// them, which puts the rest of the body inside the `--` comment).
const OBAN_NOTIFY_V1: &str = "
DECLARE
  channel text;
  notice json;
BEGIN
  IF (TG_OP = 'INSERT') THEN
    channel = 'public.oban_insert';
    notice = json_build_object('queue', NEW.queue, 'state', NEW.state);

    -- No point triggering for a job that isn't scheduled to run now
    IF NEW.scheduled_at IS NOT NULL AND NEW.scheduled_at > now() AT TIME ZONE 'utc' THEN
      RETURN null;
    END IF;
  ELSE
    channel = 'public.oban_update';
    notice = json_build_object('queue', NEW.queue, 'new_state', NEW.state, 'old_state', OLD.state);
  END IF;

  PERFORM pg_notify(channel, notice::text);

  RETURN NULL;
END;
";

const OBAN_NOTIFY_V2: &str = " DECLARE channel text; notice json; BEGIN IF NEW.state = 'available' THEN channel = 'public.oban_insert'; notice = json_build_object('queue', NEW.queue); PERFORM pg_notify(channel, notice::text); END IF; RETURN NULL; END; ";

const OBAN_WRAP_ID: &str = " BEGIN RETURN (CASE WHEN value > 2147483647 THEN mod(value, 2147483647) ELSE value END)::int; END; ";

fn trigger_signature() -> FunctionSignature {
    FunctionSignature {
        args: vec![],
        returns: ReturnType::Trigger,
    }
}

fn exists_query(cond: &Cond) -> (&str, bool) {
    match cond {
        Cond::Exists { negated, query } => (query.sql.as_str(), *negated),
        other => panic!("expected an EXISTS condition, got {other:?}"),
    }
}

#[test]
fn the_census_do_block_parses_to_an_exists_guarded_execute() {
    let program = parse_do_block(CENSUS_DO).expect("census DO block parses");
    assert_eq!(program.kind, RoutineKind::Do);
    let [Stmt::If {
        branches,
        otherwise,
    }] = program.body.as_slice()
    else {
        panic!("expected one IF, got {:?}", program.body);
    };
    assert!(otherwise.is_empty());
    let (cond, then) = &branches[0];
    let (sql, negated) = exists_query(cond);
    assert!(negated, "NOT EXISTS");
    assert!(sql.starts_with("SELECT 1 FROM pg_type WHERE typname = 'oban_job_state'"));
    assert!(sql.ends_with("'public'::regnamespace::oid"), "{sql}");
    let [Stmt::Execute(create)] = then.as_slice() else {
        panic!("expected the CREATE TYPE to execute, got {then:?}");
    };
    assert!(create
        .sql
        .starts_with("CREATE TYPE \"public\".oban_job_state AS ENUM"));
    assert!(create.sql.ends_with("'cancelled' )"), "{}", create.sql);
    assert!(create.refs.is_empty());
}

#[test]
fn oban_notify_v1_binds_new_old_and_tg_op_as_parameters() {
    let program = parse_function(OBAN_NOTIFY_V1, &trigger_signature()).expect("parses");
    assert_eq!(program.kind, RoutineKind::Trigger);
    assert_eq!(program.declarations.len(), 2);
    assert_eq!(program.new_fields(), vec!["queue", "state", "scheduled_at"]);
    assert_eq!(program.old_fields(), vec!["state"]);
    let Stmt::If {
        branches,
        otherwise,
    } = &program.body[0]
    else {
        panic!("expected IF first, got {:?}", program.body[0]);
    };
    let Cond::Expr(cond) = &branches[0].0 else {
        panic!("TG_OP test is an expression condition");
    };
    assert_eq!(cond.sql, "($1 = 'INSERT')");
    assert_eq!(program.variables[cond.refs[0].0].kind, VarKind::TgOp);
    // channel = '...'; notice = json_build_object(...)
    let Stmt::Assign { expr, .. } = &branches[0].1[1] else {
        panic!("expected an assignment");
    };
    assert_eq!(
        expr.sql, "json_build_object('queue', $1, 'state', $2)",
        "NEW.queue and NEW.state are bound, the comment is gone"
    );
    // The nested IF with the comment before it is still there.
    assert!(matches!(&branches[0].1[2], Stmt::If { .. }));
    assert_eq!(otherwise.len(), 2);
    assert!(matches!(
        &program.body[1],
        Stmt::Notify { channel, payload } if channel.sql == "$1" && payload.sql == "$1::text"
    ));
    assert_eq!(
        program.body[2],
        Stmt::Return(Some(ferrosa_plpgsql::Expr {
            sql: "NULL".into(),
            refs: vec![],
        }))
    );
}

#[test]
fn oban_notify_v2_parses() {
    let program = parse_function(OBAN_NOTIFY_V2, &trigger_signature()).expect("parses");
    assert_eq!(program.new_fields(), vec!["state", "queue"]);
    assert!(program.old_fields().is_empty());
}

#[test]
fn oban_wrap_id_binds_its_argument_by_name() {
    let sig = FunctionSignature {
        args: vec![(Some("value".into()), "bigint".into())],
        returns: ReturnType::Scalar("int".into()),
    };
    let program = parse_function(OBAN_WRAP_ID, &sig).expect("parses");
    assert_eq!(program.kind, RoutineKind::Scalar(23));
    let [Stmt::Return(Some(expr))] = program.body.as_slice() else {
        panic!("expected RETURN expr, got {:?}", program.body);
    };
    assert_eq!(
        expr.sql, "(CASE WHEN $1 > 2147483647 THEN mod($1, 2147483647) ELSE $1 END)::int",
        "every mention of `value` is one placeholder"
    );
    assert_eq!(expr.refs.len(), 1);
    assert_eq!(program.variables[expr.refs[0].0].kind, VarKind::Argument(0));
}

#[test]
fn positional_arguments_bind_to_the_same_slot_as_the_name() {
    let sig = FunctionSignature {
        args: vec![(Some("v".into()), "int".into())],
        returns: ReturnType::Scalar("int".into()),
    };
    let program = parse_function("BEGIN RETURN $1 + v; END", &sig).expect("parses");
    let [Stmt::Return(Some(expr))] = program.body.as_slice() else {
        panic!()
    };
    assert_eq!(expr.sql, "$1 + $1");
    assert_eq!(expr.refs.len(), 1);
}

#[test]
fn elsif_else_and_both_assignment_forms_parse() {
    let body = "DECLARE x text; y int := 1; BEGIN
        IF y = 1 THEN x := 'one'; ELSIF y = 2 THEN x = 'two'; ELSE x := 'many'; END IF;
        NULL;
    END";
    let program = parse_do_block(body).expect("parses");
    assert_eq!(program.declarations.len(), 2);
    assert!(program.declarations[1].default.is_some());
    let Stmt::If {
        branches,
        otherwise,
    } = &program.body[0]
    else {
        panic!()
    };
    assert_eq!(branches.len(), 2);
    assert_eq!(otherwise.len(), 1);
    assert_eq!(program.body[1], Stmt::Null);
}

#[test]
fn select_into_removes_the_into_clause_and_binds_variables() {
    let body = "DECLARE k text := 'a'; v text; BEGIN SELECT val INTO v FROM kv WHERE key = k; END";
    let program = parse_do_block(body).expect("parses");
    let [Stmt::SelectInto { targets, query }] = program.body.as_slice() else {
        panic!("{:?}", program.body)
    };
    assert_eq!(targets.len(), 1);
    assert_eq!(query.sql, "SELECT val FROM kv WHERE key = $1");
}

#[test]
fn identifiers_in_name_positions_are_not_variables() {
    let body = "DECLARE key text := 'x'; BEGIN INSERT INTO t (key, v) VALUES (key, 'key'); UPDATE t SET key = key WHERE t.key = key; CREATE TABLE key (key text PRIMARY KEY); END";
    let program = parse_do_block(body).expect("parses");
    let Stmt::Execute(insert) = &program.body[0] else {
        panic!()
    };
    // The INSERT target list names columns; VALUES holds expressions; a string is a string.
    assert_eq!(insert.sql, "INSERT INTO t (key, v) VALUES ($1, 'key')");
    let Stmt::Execute(update) = &program.body[1] else {
        panic!()
    };
    // A SET target and a qualified column are names; the right-hand sides are expressions.
    assert_eq!(update.sql, "UPDATE t SET key = $1 WHERE t.key = $1");
    let Stmt::Execute(create) = &program.body[2] else {
        panic!()
    };
    // PostgreSQL never substitutes variables into a utility statement.
    assert_eq!(create.sql, "CREATE TABLE key (key text PRIMARY KEY)");
    assert!(create.refs.is_empty());
}

fn refused(body: &str) -> ferrosa_plpgsql::PlError {
    parse_do_block(body).expect_err("must be refused")
}

#[test]
fn unsupported_constructs_are_refused_by_name_with_0a000() {
    for (body, construct) in [
        ("BEGIN RAISE NOTICE 'x'; END", "RAISE"),
        ("BEGIN LOOP EXIT; END LOOP; END", "LOOP"),
        ("BEGIN WHILE true LOOP NULL; END LOOP; END", "WHILE"),
        ("BEGIN FOR i IN 1..3 LOOP NULL; END LOOP; END", "FOR"),
        ("BEGIN EXECUTE 'select 1'; END", "EXECUTE"),
        (
            "BEGIN NULL; EXCEPTION WHEN others THEN NULL; END",
            "EXCEPTION",
        ),
        ("BEGIN CASE WHEN true THEN NULL; END CASE; END", "CASE"),
        ("BEGIN GET DIAGNOSTICS x = ROW_COUNT; END", "GET"),
        ("<<outer>> BEGIN NULL; END", "<<label>>"),
        ("DECLARE r record; BEGIN NULL; END", "type record"),
        ("BEGIN COMMIT; END", "COMMIT"),
    ] {
        let error = refused(body);
        assert_eq!(error.sqlstate, "0A000", "{body}: {error:?}");
        assert!(
            error.message.contains(construct),
            "{body}: message must name {construct}: {}",
            error.message
        );
    }
}

#[test]
fn a_select_without_into_is_a_syntax_error_not_a_silent_discard() {
    let error = refused("BEGIN SELECT 1; END");
    assert_eq!(error.sqlstate, "42601");
    assert!(
        error.message.contains("no destination"),
        "{}",
        error.message
    );
}

#[test]
fn return_with_a_value_in_a_do_block_is_refused() {
    let error = refused("BEGIN RETURN 1; END");
    assert_eq!(error.sqlstate, "42804");
}

#[test]
fn new_outside_a_trigger_function_is_refused() {
    let error = refused("BEGIN PERFORM NEW.x; END");
    assert_eq!(error.sqlstate, "42P01");
}

#[test]
fn other_tg_variables_are_refused_by_name() {
    let error = parse_function(
        "BEGIN PERFORM TG_TABLE_NAME; RETURN NULL; END",
        &trigger_signature(),
    )
    .expect_err("refused");
    assert_eq!(error.sqlstate, "0A000");
    assert!(error.message.contains("TG_TABLE_NAME"), "{}", error.message);
}

#[test]
fn assigning_an_undeclared_variable_is_an_error() {
    let error = refused("BEGIN x := 1; END");
    assert_eq!(error.sqlstate, "42601");
    assert!(error.message.contains("\"x\""), "{}", error.message);
}

#[test]
fn unterminated_blocks_and_strings_are_syntax_errors() {
    assert_eq!(refused("BEGIN NULL;").sqlstate, "42601");
    assert_eq!(refused("BEGIN PERFORM 'abc; END").sqlstate, "42601");
    assert_eq!(refused("BEGIN IF true THEN NULL; END").sqlstate, "42601");
}

#[test]
fn a_quoted_identifier_keeps_its_case_and_matches_only_exactly() {
    let body = "DECLARE \"Mixed\" text := 'a'; BEGIN PERFORM \"Mixed\"; END";
    let program = parse_do_block(body).expect("parses");
    let [Stmt::Perform(expr)] = program.body.as_slice() else {
        panic!()
    };
    assert_eq!(expr.sql, "$1");
}

#[test]
fn block_comments_and_dollar_quotes_are_lexed() {
    let body = "BEGIN /* a /* nested */ comment */ PERFORM $q$it's$q$; END";
    let program = parse_do_block(body).expect("parses");
    let [Stmt::Perform(expr)] = program.body.as_slice() else {
        panic!()
    };
    assert_eq!(expr.sql, "$q$it's$q$");
}
