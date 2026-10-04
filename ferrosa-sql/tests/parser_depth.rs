//! The SQL parser reads untrusted query text. Nesting depth is client-chosen,
//! so a recursive-descent parser without a depth limit lets one query overflow
//! the stack of the thread parsing it, which aborts the whole server. Each
//! shape must parse at a modest depth (so the deep case is not refused for
//! an unrelated reason) and come back as a parse error at a hostile depth, on
//! a thread no larger than a tokio worker's (2 MiB).

const DEPTH: usize = 100_000;
const MODEST: usize = 16;
const WORKER_STACK: usize = 2 * 1024 * 1024;

fn parse_on_worker_stack(sql: String) -> Result<(), ferrosa_sql::ParseError> {
    std::thread::Builder::new()
        .stack_size(WORKER_STACK)
        .spawn(move || ferrosa_sql::parse_statement(&sql).map(drop))
        .expect("spawn parser thread")
        .join()
        .expect("the parser thread must not panic")
}

fn check(shape: fn(usize) -> String) {
    assert_eq!(
        parse_on_worker_stack(shape(MODEST)),
        Ok(()),
        "the shape must parse at depth {MODEST}: {}",
        shape(MODEST)
    );
    assert_eq!(
        parse_on_worker_stack(shape(ferrosa_sql::MAX_EXPR_DEPTH)),
        Ok(()),
        "the limit itself must parse"
    );
    assert_eq!(
        parse_on_worker_stack(shape(DEPTH)),
        Err(ferrosa_sql::ParseError::TooDeep),
        "depth {DEPTH} must be refused as too deep"
    );
}

#[test]
fn nested_parentheses_in_where() {
    check(|d| {
        format!(
            "SELECT a FROM t WHERE {}a = 1{}",
            "(".repeat(d),
            ")".repeat(d)
        )
    });
}

#[test]
fn chained_not() {
    check(|d| format!("SELECT a FROM t WHERE {}a = 1", "NOT ".repeat(d)));
}

#[test]
fn alternating_not_and_parentheses() {
    check(|d| {
        format!(
            "SELECT a FROM t WHERE {}a = 1{}",
            "NOT (".repeat(d / 2),
            ")".repeat(d / 2)
        )
    });
}
