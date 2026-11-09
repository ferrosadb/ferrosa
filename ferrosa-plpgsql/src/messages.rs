//! The runtime errors both runtimes raise themselves (as opposed to errors the host returns),
//! with identical SQLSTATEs and text, so a compiled function and the interpreter fail the same
//! way on the same input.

/// `(sqlstate, message)`.
pub(crate) type RuntimeError = (&'static str, &'static str);

pub(crate) const NOT_ONE_COLUMN: RuntimeError = (
    "42601",
    "query did not return exactly one column where one value was expected",
);
pub(crate) const INTO_COLUMNS: RuntimeError = (
    "42601",
    "query returned a different number of columns than INTO has targets",
);
pub(crate) const ASSIGNMENT_CAST: RuntimeError = (
    "42804",
    "cannot store a value of another type: PL/pgSQL assignment casts are not supported",
);
pub(crate) const NOT_BOOLEAN: RuntimeError = ("42804", "IF condition must be boolean");
pub(crate) const EMPTY_CHANNEL: RuntimeError = ("22023", "channel name cannot be empty");
pub(crate) const NO_RETURN: RuntimeError =
    ("2F005", "control reached end of function without RETURN");
pub(crate) const NOT_A_TRIGGER_CALL: RuntimeError =
    ("0A000", "trigger functions can only be called as triggers");
pub(crate) const ARGUMENT_SHAPE: RuntimeError = (
    "XX000",
    "call does not match the function's declared arguments",
);
pub(crate) const ROW_SHAPE: RuntimeError = (
    "XX000",
    "trigger row does not match the fields the function references",
);

/// The error value for a runtime error constant.
pub(crate) fn err(e: RuntimeError) -> crate::SqlError {
    crate::SqlError::new(e.0, e.1)
}
