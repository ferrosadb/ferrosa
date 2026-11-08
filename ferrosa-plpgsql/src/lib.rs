//! The PL/pgSQL subset Ferrosa's PostgreSQL front end runs (decision D13).
//!
//! * [`parse_do_block`] / [`parse_function`] turn a body into a [`Program`]: a block whose
//!   variables are resolved to slots and whose expressions are SQL text with the variables
//!   they mention bound as `$1 … $n`. A construct outside the subset is refused with
//!   `0A000` naming it — never accepted and ignored.
//! * [`interpret`] runs a [`Program`] (the `DO` path).
//! * [`assemblyscript::convert`] turns a [`Program`] into AssemblyScript that the
//!   `ferrosa-udf` compiler builds into a component (the `CREATE FUNCTION` path).
//! * [`utility::parse_utility`] recognises the front-end statements this crate owns: `DO`,
//!   `CREATE [OR REPLACE] FUNCTION … LANGUAGE plpgsql`, `DROP FUNCTION`, `CREATE TRIGGER`,
//!   `DROP TRIGGER`, `LISTEN`, `UNLISTEN`, `NOTIFY`, `SELECT pg_notify(…)`.
//!
//! Both runtimes reach the database only through [`Host`]: `query` (a cursor read in bounded
//! batches), `execute` and `notify`. Every expression is evaluated the way PostgreSQL
//! evaluates it — as `SELECT <expr>` through the host — so there is no second expression
//! evaluator here. `specs/lean4/Ferrosa/Sql/Plpgsql.lean` proves that the converted program
//! and the interpreter agree on this subset.

pub mod assemblyscript;
mod ast;
mod error;
mod host;
mod interp;
mod lexer;
mod parser;
mod types;
pub mod utility;

pub use ast::{Cond, Declaration, Expr, Program, RoutineKind, Stmt, VarId, VarKind, Variable};
pub use error::PlError;
pub use host::{CursorId, Host, SqlError, TriggerCall, Value, FETCH_BATCH};
pub use interp::{interpret, Call};
pub use parser::{parse_do_block, parse_function, FunctionSignature, ReturnType};
pub use types::{type_oid, OID_BOOL, OID_TEXT};
