//! Ferrosa scalar UDF guest implementations.
//!
//! This crate compiles to a WASM Component Model module that the Ferrosa
//! `UdfExecutor` loads and invokes via the WIT `udf` world.
//!
//! Each public `*_invoke` function follows the WIT signature:
//!   `fn(args: Vec<CqlValue>) -> Result<CqlValue, String>`.
//!
//! The active `invoke` export (wired via the Component Model below) implements
//! `to_celsius`. To ship `coalesce` as a separate WASM module, create a
//! second crate whose `impl Guest` block calls `coalesce_invoke`.

wit_bindgen::generate!({
    world: "udf",
    path: "wit/ferrosa-udf.wit",
    generate_all,
});

// ── to_celsius ────────────────────────────────────────────────────────────────

/// Converts a Fahrenheit temperature to Celsius: `(f - 32) * 5 / 9`.
///
/// Returns `CqlValue::Null` when the input is null. Returns an error string
/// for any non-null, non-double input so the host can surface a type error.
pub fn to_celsius_invoke(args: Vec<CqlValue>) -> Result<CqlValue, String> {
    let arg = args
        .into_iter()
        .next()
        .ok_or_else(|| "to_celsius: expected 1 argument, got 0".to_string())?;

    match arg {
        CqlValue::Null => Ok(CqlValue::Null),
        CqlValue::DoubleVal(f) => {
            let celsius = (f - 32.0) * 5.0 / 9.0;
            Ok(CqlValue::DoubleVal(celsius))
        }
        other => Err(format!(
            "to_celsius: expected double-val, got {other:?}"
        )),
    }
}

// ── coalesce ──────────────────────────────────────────────────────────────────

/// Returns the first non-null `int` argument, or `null` if both are null.
///
/// Signature: `coalesce(int, int) -> int`
pub fn coalesce_invoke(args: Vec<CqlValue>) -> Result<CqlValue, String> {
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

// ── Component Model export (to_celsius) ───────────────────────────────────────

/// The exported `invoke` function implements `to_celsius`.
struct ToCelsius;

impl Guest for ToCelsius {
    fn invoke(args: Vec<CqlValue>) -> Result<CqlValue, String> {
        to_celsius_invoke(args)
    }
}

export!(ToCelsius);
