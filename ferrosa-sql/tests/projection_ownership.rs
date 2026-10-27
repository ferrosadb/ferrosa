//! Owned projections move heap values; only repeated output columns fork data.
use ferrosa_sql::exec::{project, try_project};
use ferrosa_sql::types::{Row, Value};

fn text_pointer(row: &Row, column: usize) -> *const u8 {
    let Value::Text(value) = &row.0[column] else {
        panic!("text value")
    };
    value.as_ptr()
}

#[test]
fn fallible_projection_moves_reordered_payloads() {
    let input = Row(vec![
        Value::Text("first".repeat(1000)),
        Value::Text("second".repeat(1000)),
    ]);
    let pointers = [text_pointer(&input, 0), text_pointer(&input, 1)];
    let output = try_project(Box::new(std::iter::once(Ok(input))), vec![1, 0])
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(text_pointer(&output, 0), pointers[1]);
    assert_eq!(text_pointer(&output, 1), pointers[0]);
    assert_eq!(output.0[0], Value::Text("second".repeat(1000)));
}

#[test]
fn repeated_projection_moves_one_copy_and_preserves_both_values() {
    let input = Row(vec![Value::Text("payload".repeat(1000))]);
    let pointer = text_pointer(&input, 0);
    let output = project(Box::new(std::iter::once(input)), vec![0, 0])
        .next()
        .unwrap();
    assert_eq!(output.0[0], output.0[1]);
    assert_ne!(text_pointer(&output, 0), text_pointer(&output, 1));
    assert_eq!(text_pointer(&output, 1), pointer);
}

#[test]
#[should_panic]
fn identity_projection_rejects_a_short_input_row() {
    let input = Row(vec![Value::Int(1)]);
    let _ = try_project(Box::new(std::iter::once(Ok(input))), vec![0, 1])
        .next()
        .unwrap();
}
