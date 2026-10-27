//! Ungrouped aggregates consume rows without sorting or retaining payloads.
use ferrosa_sql::exec::{hash_aggregate, AggFunc, AggSpec, TryRowStream};
use ferrosa_sql::spill::{DirReserver, SpillCtx, SpillError};
use ferrosa_sql::types::{Row, Value};
use std::sync::Arc;

#[test]
fn scalar_count_and_sum_need_no_spill_storage() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let ctx = SpillCtx::new(Arc::new(DirReserver::new(file.path())), 1);
    let rows: TryRowStream<'static> =
        Box::new((0..1000).map(|i| Ok(Row(vec![Value::Int(i), Value::Text("x".repeat(4096))]))));
    let started = std::time::Instant::now();
    let result = hash_aggregate(
        rows,
        &[],
        &[
            AggSpec::new(AggFunc::Count, None),
            AggSpec::typed(AggFunc::Sum, Some(0), ferrosa_sql::ColumnType::Int),
        ],
        &ctx,
    )
    .expect("a scalar accumulator needs no scratch directory")
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    eprintln!(
        "scalar 1000 x 4096B: {:?}; spills={:?}",
        started.elapsed(),
        ctx.stats().spilled()
    );
    assert_eq!(
        result,
        vec![Row(vec![Value::Int(1000), Value::Int(499500)])]
    );
    assert!(!ctx.stats().spilled());
    assert!(ctx.stats().reserved_paths().is_empty());
}

#[test]
fn scalar_count_does_not_hide_input_errors() {
    let rows: TryRowStream<'static> = Box::new(
        [
            Ok(Row(vec![Value::Int(1)])),
            Err(SpillError::new("source", "read failed")),
        ]
        .into_iter(),
    );
    let error = hash_aggregate(
        rows,
        &[],
        &[AggSpec::new(AggFunc::Count, None)],
        &SpillCtx::default(),
    )
    .err()
    .expect("input failure must abort aggregate");
    assert!(error.to_string().contains("read failed"));
}
