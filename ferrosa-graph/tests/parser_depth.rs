//! Cypher text is untrusted; a recursive-descent parser must refuse hostile
//! nesting rather than overflow the stack of the thread parsing it (which
//! aborts the process). Every shape parses at a modest depth, so a refusal at
//! a hostile depth is the depth limit and not some unrelated syntax error.

const DEPTH: usize = 100_000;
const MODEST: usize = 8;
/// A tokio worker's stack. Unoptimised builds spend several times more stack
/// per expression level (64 levels of nested lists overflow 2 MiB there), so
/// a debug run gets 8 MiB; release, which is what serves queries, is held to
/// the real 2 MiB.
const WORKER_STACK: usize = if cfg!(debug_assertions) {
    8 * 1024 * 1024
} else {
    2 * 1024 * 1024
};

fn parse_on_worker_stack(query: String) -> Result<(), String> {
    std::thread::Builder::new()
        .stack_size(WORKER_STACK)
        .spawn(move || {
            ferrosa_graph::parser::parse(&query)
                .map(drop)
                .map_err(|e| e.to_string())
        })
        .expect("spawn parser thread")
        .join()
        .expect("the parser thread must not panic")
}

fn check(shape: fn(usize) -> String) {
    assert_eq!(
        parse_on_worker_stack(shape(MODEST)),
        Ok(()),
        "must parse at depth {MODEST}: {}",
        shape(MODEST)
    );
    assert!(parse_on_worker_stack(shape(DEPTH)).is_err());
}

#[test]
fn nested_parentheses() {
    check(|d| format!("RETURN {}1{}", "(".repeat(d), ")".repeat(d)));
}

#[test]
fn nested_list_literals() {
    check(|d| format!("RETURN {}1{}", "[".repeat(d), "]".repeat(d)));
}

#[test]
fn nested_map_literals() {
    check(|d| format!("RETURN {}1{}", "{a: ".repeat(d), "}".repeat(d)));
}

#[test]
fn nested_function_calls() {
    check(|d| format!("RETURN {}1{}", "abs(".repeat(d), ")".repeat(d)));
}

#[test]
fn chained_not() {
    check(|d| format!("MATCH (n) WHERE {}true RETURN n", "NOT ".repeat(d)));
}

#[test]
fn chained_unary_minus() {
    check(|d| format!("RETURN {}1", "- ".repeat(d)));
}
