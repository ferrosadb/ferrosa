//! Constant expressions: a column `DEFAULT` (lane W2).
//!
//! A `DEFAULT` is compiled once per statement and evaluated once per inserted
//! row, with that row's statement time as `now()`. It reads no column, no
//! subquery and no parameter. Each of those is refused when it is compiled, as
//! PostgreSQL refuses it at `CREATE TABLE`, rather than failing on some later
//! row.

use std::sync::{Arc, Mutex};

use super::eval::{eval, Env};
use super::run::{Fence, Rt};
use super::{CExpr, Compiler, Ctx, Scope};
use crate::ast::ScalarExpr;
use crate::catalog::MapCatalog;
use crate::plan::ExecError;
use crate::spill::SpillCtx;
use crate::types::{ColumnType, Row, Value};

/// A compiled column `DEFAULT`.
#[derive(Debug)]
pub struct DefaultExpr {
    expr: CExpr,
    ty: Option<ColumnType>,
}

/// Parse and compile the `DEFAULT` expression `sql`.
///
/// # Errors
///
/// `42601` for text that is not one expression; `0A000` for a column
/// reference or a subquery; `42P02` for a parameter; any compile error of the
/// expression itself (an unknown function, a refused sequence function).
pub fn compile_default(sql: &str) -> Result<DefaultExpr, ExecError> {
    let expr = crate::parser::parse_scalar_expression(sql)
        .map_err(|e| ExecError::eval("42601", format!("DEFAULT {sql}: {e}")))?;
    refuse_non_constant(&expr)?;
    let catalog = MapCatalog::new();
    let compiler = Compiler::new(&catalog, "public");
    let scope = Scope {
        bindings: Vec::new(),
        parent: None,
    };
    compiler.refs.borrow_mut().push(usize::MAX);
    let compiled = compiler.expr(&expr, &scope, &[], &mut Ctx::Row { clause: "DEFAULT" });
    compiler.refs.borrow_mut().pop();
    let (expr, ty) = compiled?;
    Ok(DefaultExpr { expr, ty })
}

/// A column, subquery or parameter anywhere in `expr` is not a constant.
fn refuse_non_constant(expr: &ScalarExpr) -> Result<(), ExecError> {
    match expr {
        ScalarExpr::Column(c) => Err(ExecError::eval(
            "0A000",
            format!(
                "cannot use column reference \"{}\" in DEFAULT expression",
                c.name
            ),
        )),
        ScalarExpr::Subquery(_) | ScalarExpr::ArraySubquery(_) | ScalarExpr::Exists { .. } => Err(
            ExecError::eval("0A000", "cannot use subquery in DEFAULT expression"),
        ),
        ScalarExpr::Param(n) => Err(ExecError::eval(
            "42P02",
            format!("there is no parameter ${n} in a DEFAULT expression"),
        )),
        _ => {
            let mut verdict = Ok(());
            expr.for_each_child(|child| {
                if verdict.is_ok() {
                    verdict = refuse_non_constant(child);
                }
            });
            verdict
        }
    }
}

impl DefaultExpr {
    /// The expression's static type; `None` for an untyped `NULL`.
    pub fn column_type(&self) -> Option<ColumnType> {
        self.ty
    }

    /// Evaluate for one row whose statement started at `now_micros`.
    ///
    /// # Errors
    ///
    /// The expression's evaluation error (22012, 22003, …), never a NULL.
    pub fn eval(&self, now_micros: i64) -> Result<Value, ExecError> {
        let rt = Arc::new(Rt {
            params: Arc::from(Vec::new()),
            ctx: SpillCtx::default(),
            fence: Arc::new(Fence::default()),
            now_micros,
            memo: Mutex::new(std::collections::HashMap::new()),
        });
        let row = Row(Vec::new());
        let env = Env {
            row: &row,
            frame: None,
            outer: None,
            rt: &rt,
        };
        eval(&self.expr, &env)
    }
}
