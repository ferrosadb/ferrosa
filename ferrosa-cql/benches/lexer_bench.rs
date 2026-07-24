//! Lexer throughput micro-benchmark + per-token-class attribution harness.
//!
//! Lexing dominates unprepared-query write CPU (~36% in the perf flamegraph,
//! t_a8be92e7 / t_48d5eeaa). This harness lexes representative statements AND
//! isolated token-class strings so each lexer path (identifier/keyword,
//! whitespace, number, string, uuid) can be attributed and each optimization
//! measured independently.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ferrosa_cql::lexer::{Lexer, TokenKind};

/// Lex `q` fully (every token to Eof). Returns the token count so the work
/// cannot be optimized away.
fn lex_all(q: &str) -> usize {
    let mut lexer = Lexer::new(q).expect("lexer construct");
    let mut n = 0usize;
    // `while let Ok` exits on the first lex error; Eof ends a well-formed lex.
    while let Ok(tok) = lexer.next_token() {
        n += 1;
        if matches!(tok.kind, TokenKind::Eof) {
            break;
        }
    }
    n
}

fn bench_case(c: &mut Criterion, name: &str, q: &str) {
    let mut group = c.benchmark_group("lexer");
    group.throughput(Throughput::Bytes(q.len() as u64));
    group.bench_function(name, |b| b.iter(|| black_box(lex_all(black_box(q)))));
    group.finish();
}

fn bench_lexer(c: &mut Criterion) {
    // End-to-end representative statements.
    bench_case(
        c,
        "insert_inline_literals",
        "INSERT INTO baselines.data (pk, ck, val) VALUES \
         ('machine-abc-000012345', 1, 0xDEADBEEF0123456789ABCDEF)",
    );
    bench_case(
        c,
        "select_where_limit",
        "SELECT pk, ck, val FROM baselines.iot \
         WHERE machine_id = 'm1' AND sensor_name = 'temperature' LIMIT 100",
    );

    // Isolated token-class strings (equal-ish length) to attribute cost.
    // identifiers + keywords (exercises read_identifier + keyword lookup +
    // the to_ascii_uppercase path):
    bench_case(
        c,
        "class_idents_keywords",
        "select insert update delete from where and limit values into table \
         alpha bravo charlie delta echo foxtrot golf hotel india juliet",
    );
    // whitespace-heavy (exercises skip_whitespace_and_comments):
    bench_case(
        c,
        "class_whitespace",
        "a                              b                              c \
                                        d                              e",
    );
    // numbers (read_number):
    bench_case(
        c,
        "class_numbers",
        "1 22 333 4444 55555 666666 7777777 88888888 999999999 1000000000 \
         12 345 6789 12345 678901 2345678 90123456 789012345",
    );
    // strings (read_string_literal):
    bench_case(
        c,
        "class_strings",
        "'alpha' 'bravo charlie' 'delta echo foxtrot' 'golf hotel india juliet' \
         'kilo lima mike november' 'oscar papa quebec romeo sierra'",
    );
    // uuids (read_identifier UUID speculative path + try_read_uuid_tail):
    bench_case(
        c,
        "class_uuids",
        "550e8400-e29b-41d4-a716-446655440000 6ba7b810-9dad-11d1-80b4-00c04fd430c8 \
         6ba7b811-9dad-11d1-80b4-00c04fd430c8 00112233-4455-6677-8899-aabbccddeeff",
    );
}

criterion_group!(benches, bench_lexer);
criterion_main!(benches);
