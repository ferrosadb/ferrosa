//! Ferrosa scalar UDF guest: `coalesce(int, int) -> int`
//!
//! Returns the first non-null argument, or null if both are null.
//! Compiled to a WASM Component Model module for the `udf` world.

wit_bindgen::generate!({
    world: "udf",
    path: "wit/ferrosa-udf.wit",
    generate_all,
});

/// Returns the first non-null `int` argument, or `null` if both are null.
///
/// Signature: `coalesce(int, int) -> int`
fn coalesce_invoke(args: Vec<CqlValue>) -> Result<CqlValue, String> {
    if args.len() != 2 {
        return Err(format!(
            "coalesce: expected 2 arguments, got {}",
            args.len()
        ));
    }

    for arg in args {
        match arg {
            CqlValue::Null => continue,
            CqlValue::IntVal(_) => return Ok(arg),
            other => {
                return Err(format!(
                    "coalesce: expected int-val or null, got {other:?}"
                ))
            }
        }
    }

    Ok(CqlValue::Null)
}

struct Coalesce;

impl Guest for Coalesce {
    fn invoke(args: Vec<CqlValue>) -> Result<CqlValue, String> {
        coalesce_invoke(args)
    }
}

export!(Coalesce);
