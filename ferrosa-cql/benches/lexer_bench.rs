//! Lexer throughput micro-benchmark.
//!
//! Lexing is ~12% of write-path CPU on unprepared queries (perf profile,
//! t_a8be92e7). This benchmark lexes representative CQL statements token-by-token
//! to Eof so lexer changes (e.g. `#[inline]` hints) can be measured directly,
//! far more sensitively than an end-to-end cluster run.

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

fn bench_lexer(c: &mut Criterion) {
    // Representative of the write hot path (loadgen-style INSERT with inline
    // literals: string, int, hex blob), plus a SELECT with a WHERE + LIMIT.
    let insert = "INSERT INTO baselines.data (pk, ck, val) VALUES \
                  ('machine-abc-000012345', 1, 0xDEADBEEF0123456789ABCDEF)";
    let select = "SELECT pk, ck, val FROM baselines.iot \
                  WHERE machine_id = 'm1' AND sensor_name = 'temperature' LIMIT 100";

    let mut group = c.benchmark_group("lexer");
    group.throughput(Throughput::Bytes(insert.len() as u64));
    group.bench_function("insert_inline_literals", |b| {
        b.iter(|| black_box(lex_all(black_box(insert))))
    });
    group.throughput(Throughput::Bytes(select.len() as u64));
    group.bench_function("select_where_limit", |b| {
        b.iter(|| black_box(lex_all(black_box(select))))
    });
    group.finish();
}

criterion_group!(benches, bench_lexer);
criterion_main!(benches);
