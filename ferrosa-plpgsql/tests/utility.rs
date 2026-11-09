//! The front-end statements this crate owns, as the Oban census sends them.

use ferrosa_plpgsql::utility::{
    parse_utility, CreateFunction, CreateTrigger, NotifyArg, QualifiedName, TriggerEvent, Utility,
};
use ferrosa_plpgsql::ReturnType;

fn parsed(sql: &str) -> Utility {
    parse_utility(sql)
        .unwrap_or_else(|| panic!("not recognised: {sql}"))
        .unwrap_or_else(|e| panic!("refused: {sql}: {e:?}"))
}

fn refused(sql: &str) -> ferrosa_plpgsql::PlError {
    parse_utility(sql)
        .unwrap_or_else(|| panic!("not recognised: {sql}"))
        .expect_err("must be refused")
}

fn qn(schema: Option<&str>, name: &str) -> QualifiedName {
    QualifiedName {
        schema: schema.map(str::to_string),
        name: name.to_string(),
    }
}

#[test]
fn the_census_do_block_yields_its_body() {
    let sql =
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1) THEN CREATE TYPE t AS ENUM ('a'); END IF; END$$;;";
    let Utility::Do { body } = parsed(sql) else {
        panic!()
    };
    assert_eq!(
        body,
        " BEGIN IF NOT EXISTS (SELECT 1) THEN CREATE TYPE t AS ENUM ('a'); END IF; END"
    );
}

#[test]
fn do_with_another_language_is_refused() {
    let error = refused("DO LANGUAGE plperl $$ 1; $$");
    assert_eq!(error.sqlstate, "0A000");
    assert!(error.message.contains("plperl"));
}

#[test]
fn the_census_trigger_function_header_parses() {
    let sql = "CREATE OR REPLACE FUNCTION \"public\".oban_jobs_notify() RETURNS trigger AS $$ DECLARE channel text; BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;;";
    let Utility::CreateFunction(CreateFunction {
        or_replace,
        name,
        signature,
        body,
        ..
    }) = parsed(sql)
    else {
        panic!()
    };
    assert!(or_replace);
    assert_eq!(name, qn(Some("public"), "oban_jobs_notify"));
    assert!(signature.args.is_empty());
    assert_eq!(signature.returns, ReturnType::Trigger);
    assert!(body.starts_with(" DECLARE channel text;"));
}

#[test]
fn the_census_scalar_function_header_parses_with_volatility() {
    let sql = "CREATE OR REPLACE FUNCTION \"public\".oban_wrap_id(value bigint) RETURNS int AS $$ BEGIN RETURN value; END; $$ LANGUAGE plpgsql IMMUTABLE;;";
    let Utility::CreateFunction(f) = parsed(sql) else {
        panic!()
    };
    assert_eq!(
        f.signature.args,
        vec![(Some("value".into()), "bigint".into())]
    );
    assert_eq!(f.signature.returns, ReturnType::Scalar("int".into()));
    assert_eq!(f.volatility.as_deref(), Some("immutable"));
}

#[test]
fn an_unnamed_argument_is_a_type() {
    let Utility::CreateFunction(f) = parsed(
        "CREATE FUNCTION f(bigint, text) RETURNS void LANGUAGE plpgsql AS 'BEGIN NULL; END'",
    ) else {
        panic!()
    };
    assert_eq!(
        f.signature.args,
        vec![(None, "bigint".into()), (None, "text".into())]
    );
    assert_eq!(f.signature.returns, ReturnType::Void);
    assert_eq!(f.body, "BEGIN NULL; END");
}

#[test]
fn function_features_outside_the_subset_are_refused_by_name() {
    for (sql, construct) in [
        (
            "CREATE FUNCTION f() RETURNS int AS $$ select 1 $$ LANGUAGE sql",
            "LANGUAGE sql",
        ),
        (
            "CREATE FUNCTION f(OUT x int) RETURNS int AS $$ BEGIN END $$ LANGUAGE plpgsql",
            "OUT",
        ),
        (
            "CREATE FUNCTION f(x int DEFAULT 1) RETURNS int AS $$ BEGIN END $$ LANGUAGE plpgsql",
            "DEFAULT",
        ),
        (
            "CREATE FUNCTION f() RETURNS SETOF int AS $$ BEGIN END $$ LANGUAGE plpgsql",
            "SETOF",
        ),
        (
            "CREATE FUNCTION f() RETURNS int AS $$ BEGIN END $$ LANGUAGE plpgsql STRICT",
            "STRICT",
        ),
        (
            "CREATE FUNCTION f() RETURNS int AS $$ BEGIN END $$ LANGUAGE plpgsql SECURITY DEFINER",
            "SECURITY",
        ),
    ] {
        let error = refused(sql);
        assert_eq!(error.sqlstate, "0A000", "{sql}");
        assert!(
            error.message.contains(construct),
            "{sql}: {}",
            error.message
        );
    }
}

#[test]
fn drop_function_forms_parse() {
    assert_eq!(
        parsed("DROP FUNCTION IF EXISTS \"public\".oban_wrap_id(value bigint);"),
        Utility::DropFunction {
            if_exists: true,
            name: qn(Some("public"), "oban_wrap_id"),
            arg_types: Some(vec!["bigint".into()]),
        }
    );
    assert_eq!(
        parsed("DROP FUNCTION IF EXISTS \"public\".oban_jobs_notify();"),
        Utility::DropFunction {
            if_exists: true,
            name: qn(Some("public"), "oban_jobs_notify"),
            arg_types: Some(vec![]),
        }
    );
    assert_eq!(
        parsed("drop function f"),
        Utility::DropFunction {
            if_exists: false,
            name: qn(None, "f"),
            arg_types: None,
        }
    );
    assert_eq!(refused("DROP FUNCTION f() CASCADE").sqlstate, "0A000");
}

#[test]
fn the_census_triggers_parse() {
    let sql = "CREATE TRIGGER oban_notify AFTER INSERT OR UPDATE OF state ON \"public\".oban_jobs FOR EACH ROW EXECUTE PROCEDURE \"public\".oban_jobs_notify();;";
    assert_eq!(
        parsed(sql),
        Utility::CreateTrigger(CreateTrigger {
            name: "oban_notify".into(),
            events: vec![
                TriggerEvent::Insert,
                TriggerEvent::Update {
                    columns: vec!["state".into()]
                }
            ],
            table: qn(Some("public"), "oban_jobs"),
            function: qn(Some("public"), "oban_jobs_notify"),
        })
    );
    let Utility::CreateTrigger(t) = parsed("CREATE TRIGGER oban_notify AFTER INSERT ON \"public\".oban_jobs FOR EACH ROW EXECUTE FUNCTION \"public\".oban_jobs_notify()") else {
        panic!()
    };
    assert_eq!(t.events, vec![TriggerEvent::Insert]);
}

#[test]
fn trigger_features_outside_the_subset_are_refused_by_name() {
    for (sql, construct) in [
        (
            "CREATE TRIGGER t BEFORE INSERT ON x FOR EACH ROW EXECUTE PROCEDURE f()",
            "BEFORE",
        ),
        (
            "CREATE TRIGGER t INSTEAD OF INSERT ON x FOR EACH ROW EXECUTE PROCEDURE f()",
            "INSTEAD OF",
        ),
        (
            "CREATE TRIGGER t AFTER INSERT ON x FOR EACH STATEMENT EXECUTE PROCEDURE f()",
            "FOR EACH STATEMENT",
        ),
        (
            "CREATE TRIGGER t AFTER INSERT ON x EXECUTE PROCEDURE f()",
            "FOR EACH STATEMENT",
        ),
        (
            "CREATE TRIGGER t AFTER DELETE ON x FOR EACH ROW EXECUTE PROCEDURE f()",
            "DELETE",
        ),
        (
            "CREATE TRIGGER t AFTER TRUNCATE ON x FOR EACH ROW EXECUTE PROCEDURE f()",
            "TRUNCATE",
        ),
        (
            "CREATE TRIGGER t AFTER INSERT ON x FOR EACH ROW WHEN (true) EXECUTE PROCEDURE f()",
            "WHEN",
        ),
        (
            "CREATE TRIGGER t AFTER INSERT ON x FOR EACH ROW EXECUTE PROCEDURE f('a')",
            "trigger arguments",
        ),
        (
            "CREATE CONSTRAINT TRIGGER t AFTER INSERT ON x FOR EACH ROW EXECUTE PROCEDURE f()",
            "CONSTRAINT",
        ),
        (
            "CREATE OR REPLACE TRIGGER t AFTER INSERT ON x FOR EACH ROW EXECUTE PROCEDURE f()",
            "OR REPLACE",
        ),
    ] {
        let error = refused(sql);
        assert_eq!(error.sqlstate, "0A000", "{sql}");
        assert!(
            error.message.contains(construct),
            "{sql}: {}",
            error.message
        );
    }
}

#[test]
fn drop_trigger_parses() {
    assert_eq!(
        parsed("DROP TRIGGER IF EXISTS oban_notify ON \"public\".oban_jobs;"),
        Utility::DropTrigger {
            if_exists: true,
            name: "oban_notify".into(),
            table: qn(Some("public"), "oban_jobs"),
        }
    );
}

#[test]
fn listen_unlisten_and_notify_parse() {
    assert_eq!(parsed("LISTEN jobs"), Utility::Listen("jobs".into()));
    assert_eq!(
        parsed("LISTEN \"Mixed.Case\";"),
        Utility::Listen("Mixed.Case".into())
    );
    assert_eq!(
        parsed("UNLISTEN jobs"),
        Utility::Unlisten(Some("jobs".into()))
    );
    assert_eq!(parsed("UNLISTEN *"), Utility::Unlisten(None));
    assert_eq!(
        parsed("NOTIFY jobs, 'it''s here'"),
        Utility::Notify {
            channel: "jobs".into(),
            payload: String::new() + "it's here",
        }
    );
    assert_eq!(
        parsed("NOTIFY jobs"),
        Utility::Notify {
            channel: "jobs".into(),
            payload: String::new(),
        }
    );
}

#[test]
fn select_pg_notify_with_literals_or_parameters_parses() {
    assert_eq!(
        parsed("SELECT pg_notify('public.oban_insert', '{\"queue\":\"q\"}')"),
        Utility::PgNotify {
            channel: NotifyArg::Literal("public.oban_insert".into()),
            payload: NotifyArg::Literal("{\"queue\":\"q\"}".into()),
        }
    );
    assert_eq!(
        parsed("SELECT pg_notify($1, $2)"),
        Utility::PgNotify {
            channel: NotifyArg::Param(1),
            payload: NotifyArg::Param(2),
        }
    );
}

#[test]
fn other_statements_are_not_ours() {
    for sql in [
        "SELECT 1",
        "SELECT pg_notify(a, b) FROM t",
        "CREATE TABLE t (a int)",
        "DROP TABLE t",
        "INSERT INTO t VALUES (1)",
        "CREATE TYPE t AS ENUM ('a')",
        "",
    ] {
        assert!(parse_utility(sql).is_none(), "{sql}");
    }
}
