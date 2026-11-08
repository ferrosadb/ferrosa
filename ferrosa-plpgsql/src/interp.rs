//! The interpreter: runs a [`Program`] against a [`Host`] (the `DO` path, and the reference
//! the compiled path is checked against).
//!
//! Every expression is `SELECT <expr>` through the host with the referenced slots bound as
//! parameters; the only shortcut is an expression that *is* one variable (`$1` alone), which
//! is that variable's value — exactly what `SELECT $1` would return. The generated
//! AssemblyScript takes the same shortcut, so the two make the same host calls.

use crate::ast::{Cond, Expr, Program, RoutineKind, Stmt, VarId, VarKind};
use crate::host::{Host, SqlError, TriggerCall, Value, FETCH_BATCH};
use crate::types::{OID_BOOL, OID_TEXT};

/// The inputs of one run.
#[derive(Debug, Clone, Default)]
pub struct Call {
    /// Argument values, in order.
    pub args: Vec<Value>,
    /// The trigger context for a trigger function.
    pub trigger: Option<TriggerCall>,
}

/// Run `program` against `host`. Returns the `RETURN` value (`None` when the body ended
/// without one, which only a `DO` block or a `void` function may do).
///
/// # Errors
///
/// The first host error, or a runtime error with its SQLSTATE: `2F005` (no RETURN), `42804`
/// (a non-boolean condition, an assignment of another type), `42601` (an expression of more
/// than one column), `0A000` (a trigger function called directly), `XX000` (a call whose
/// shape does not match the program — a caller bug).
pub fn interpret(
    program: &Program,
    call: Call,
    host: &mut dyn Host,
) -> Result<Option<Value>, SqlError> {
    let slots = initial_slots(program, call)?;
    let mut machine = Machine {
        program,
        slots,
        host,
    };
    for decl in &program.declarations {
        if let Some(expr) = &decl.default {
            let value = machine.eval(expr)?;
            machine.assign(decl.var, value)?;
        }
    }
    match machine.run_list(&program.body)? {
        Flow::Returned(value) => Ok(Some(value)),
        Flow::Next => match program.kind {
            RoutineKind::Do | RoutineKind::Void => Ok(None),
            RoutineKind::Scalar(_) | RoutineKind::Trigger => Err(SqlError::new(
                "2F005",
                "control reached end of function without RETURN",
            )),
        },
    }
}

/// Bind the call into the slots: arguments, `NEW`/`OLD` fields, `TG_OP`; locals start NULL.
fn initial_slots(program: &Program, call: Call) -> Result<Vec<Value>, SqlError> {
    let Call { args, trigger } = call;
    if args.len() != program.arg_count {
        return Err(SqlError::new(
            "XX000",
            format!(
                "called with {} arguments, declared {}",
                args.len(),
                program.arg_count
            ),
        ));
    }
    let trigger = match (&program.kind, trigger) {
        (RoutineKind::Trigger, Some(t)) => Some(t),
        (RoutineKind::Trigger, None) => {
            return Err(SqlError::new(
                "0A000",
                "trigger functions can only be called as triggers",
            ))
        }
        (_, _) => None,
    };
    let mut args = args.into_iter();
    let (mut new_row, mut old_row, op) = split_trigger(program, trigger)?;
    program
        .variables
        .iter()
        .map(|var| {
            Ok(match &var.kind {
                VarKind::Local => Value::null(var.type_oid.unwrap_or(0)),
                VarKind::Argument(_) => args.next().ok_or_else(|| shape("argument"))?,
                VarKind::NewField(_) => new_row.next().ok_or_else(|| shape("NEW"))?,
                VarKind::OldField(_) => old_row.next().ok_or_else(|| shape("OLD"))?,
                VarKind::TgOp => Value::text(OID_TEXT, op.as_str()),
            })
        })
        .collect()
}

type RowIter = std::vec::IntoIter<Value>;

/// The trigger rows as iterators in slot order, checked against the fields referenced.
fn split_trigger(
    program: &Program,
    trigger: Option<TriggerCall>,
) -> Result<(RowIter, RowIter, String), SqlError> {
    let Some(TriggerCall {
        op,
        new_row,
        old_row,
    }) = trigger
    else {
        return Ok((
            Vec::new().into_iter(),
            Vec::new().into_iter(),
            String::new(),
        ));
    };
    let new_row = fit_row(new_row, program.new_fields().len(), "NEW")?;
    let old_row = fit_row(old_row, program.old_fields().len(), "OLD")?;
    Ok((new_row.into_iter(), old_row.into_iter(), op))
}

/// A trigger row must hold exactly the referenced fields. An absent row (`NEW` in a DELETE)
/// reads as NULLs, as PostgreSQL's unassigned record does for an AFTER trigger.
fn fit_row(row: Option<Vec<Value>>, want: usize, which: &str) -> Result<Vec<Value>, SqlError> {
    match row {
        Some(values) if values.len() == want => Ok(values),
        Some(values) => Err(SqlError::new(
            "XX000",
            format!(
                "{which} carries {} fields, the function references {want}",
                values.len()
            ),
        )),
        None => Ok(vec![Value::null(0); want]),
    }
}

fn shape(what: &str) -> SqlError {
    SqlError::new("XX000", format!("trigger call is missing a {what} value"))
}

/// Whether execution falls through to the next statement or has returned.
enum Flow {
    Next,
    Returned(Value),
}

struct Machine<'p, 'h> {
    program: &'p Program,
    slots: Vec<Value>,
    host: &'h mut dyn Host,
}

impl Machine<'_, '_> {
    fn run_list(&mut self, body: &[Stmt]) -> Result<Flow, SqlError> {
        for stmt in body {
            if let Flow::Returned(value) = self.run(stmt)? {
                return Ok(Flow::Returned(value));
            }
        }
        Ok(Flow::Next)
    }

    fn run(&mut self, stmt: &Stmt) -> Result<Flow, SqlError> {
        match stmt {
            Stmt::Assign { target, expr } => {
                let value = self.eval(expr)?;
                self.assign(*target, value)?;
            }
            Stmt::If {
                branches,
                otherwise,
            } => return self.run_if(branches, otherwise),
            Stmt::Return(None) => return Ok(Flow::Returned(Value::null(0))),
            Stmt::Return(Some(expr)) => {
                let value = self.eval(expr)?;
                return Ok(Flow::Returned(self.fit_return(value)?));
            }
            Stmt::Perform(expr) => self.perform(expr)?,
            Stmt::Notify { channel, payload } => self.notify(channel, payload)?,
            Stmt::SelectInto { targets, query } => self.select_into(targets, query)?,
            Stmt::Execute(expr) => {
                let params = self.params(expr);
                self.host.execute(&expr.sql, &params)?;
            }
            Stmt::Null => {}
        }
        Ok(Flow::Next)
    }

    fn run_if(
        &mut self,
        branches: &[(Cond, Vec<Stmt>)],
        otherwise: &[Stmt],
    ) -> Result<Flow, SqlError> {
        for (cond, body) in branches {
            if self.test(cond)? {
                return self.run_list(body);
            }
        }
        self.run_list(otherwise)
    }

    /// The parameters for `expr`: the referenced slots' values (shared, not copied).
    fn params(&self, expr: &Expr) -> Vec<Value> {
        expr.refs
            .iter()
            .map(|id| self.slots[id.0].clone()) // Arc-backed: shares the text; the slot keeps its value
            .collect()
    }

    /// `SELECT <expr>`'s single value (or the variable itself for a bare `$1`).
    fn eval(&mut self, expr: &Expr) -> Result<Value, SqlError> {
        if let Some(id) = bare_variable(expr) {
            return Ok(self.slots[id.0].clone()); // Arc-backed share: the variable keeps its value
        }
        let row = self.first_row(&format!("SELECT {}", expr.sql), expr)?;
        let Some(row) = row else {
            return Ok(Value::null(0));
        };
        single_column(row)
    }

    /// The first row of `sql` (bound from `expr`), reading one row and closing the query.
    fn first_row(&mut self, sql: &str, expr: &Expr) -> Result<Option<Vec<Value>>, SqlError> {
        let params = self.params(expr);
        let cursor = self.host.query(sql, &params)?;
        let batch = self.host.fetch(cursor, 1);
        self.host.close(cursor);
        Ok(batch?.into_iter().next())
    }

    fn test(&mut self, cond: &Cond) -> Result<bool, SqlError> {
        match cond {
            Cond::Exists { negated, query } => {
                let found = self.first_row(&query.sql, query)?.is_some();
                Ok(found != *negated)
            }
            Cond::Expr(expr) => {
                let value = self.eval(expr)?;
                if !value.is_null() && value.type_oid != OID_BOOL {
                    return Err(SqlError::new(
                        "42804",
                        format!(
                            "IF condition must be boolean, got type OID {}",
                            value.type_oid
                        ),
                    ));
                }
                Ok(value.is_true())
            }
        }
    }

    /// `PERFORM`: run the query to the end in bounded batches, discarding the rows.
    fn perform(&mut self, expr: &Expr) -> Result<(), SqlError> {
        let params = self.params(expr);
        let cursor = self.host.query(&format!("SELECT {}", expr.sql), &params)?;
        let drained = self.drain(cursor);
        self.host.close(cursor);
        drained
    }

    fn drain(&mut self, cursor: crate::CursorId) -> Result<(), SqlError> {
        // Bounded: each batch is at most FETCH_BATCH rows and the loop ends on the empty batch
        // the host must return once the query is exhausted.
        loop {
            if self.host.fetch(cursor, FETCH_BATCH)?.is_empty() {
                return Ok(());
            }
        }
    }

    fn notify(&mut self, channel: &Expr, payload: &Expr) -> Result<(), SqlError> {
        let channel = self.eval(channel)?;
        let payload = self.eval(payload)?;
        let Some(channel) = channel.text.filter(|c| !c.is_empty()) else {
            return Err(SqlError::new("22023", "channel name cannot be empty"));
        };
        self.host
            .notify(&channel, payload.text.as_deref().unwrap_or(""))
    }

    fn select_into(&mut self, targets: &[VarId], query: &Expr) -> Result<(), SqlError> {
        let Some(row) = self.first_row(&query.sql, query)? else {
            for target in targets {
                let oid = self.declared(*target).unwrap_or(0);
                self.slots[target.0] = Value::null(oid);
            }
            return Ok(());
        };
        if row.len() != targets.len() {
            return Err(SqlError::new(
                "42601",
                format!(
                    "query returned {} columns into {} targets",
                    row.len(),
                    targets.len()
                ),
            ));
        }
        for (target, value) in targets.iter().zip(row) {
            self.assign(*target, value)?;
        }
        Ok(())
    }

    fn declared(&self, id: VarId) -> Option<u32> {
        self.program.variables[id.0].type_oid
    }

    fn assign(&mut self, target: VarId, value: Value) -> Result<(), SqlError> {
        let var = &self.program.variables[target.0];
        self.slots[target.0] = fit_type(value, var.type_oid, &var.name)?;
        Ok(())
    }

    fn fit_return(&self, value: Value) -> Result<Value, SqlError> {
        match self.program.kind {
            RoutineKind::Scalar(oid) => fit_type(value, Some(oid), "the return value"),
            _ => Ok(value),
        }
    }
}

/// `$1` alone, bound to one slot: the variable itself.
pub(crate) fn bare_variable(expr: &Expr) -> Option<VarId> {
    match expr.refs.as_slice() {
        [id] if expr.sql == "$1" => Some(*id),
        _ => None,
    }
}

fn single_column(row: Vec<Value>) -> Result<Value, SqlError> {
    let n = row.len();
    let mut columns = row.into_iter();
    match (columns.next(), columns.next()) {
        (Some(value), None) => Ok(value),
        _ => Err(SqlError::new(
            "42601",
            format!("query returned {n} columns where one value was expected"),
        )),
    }
}

/// Text-like types relabel freely between each other.
fn text_like(oid: u32) -> bool {
    matches!(oid, OID_TEXT | 1043)
}

/// A value stored into a slot (or returned) of a declared type. NULL takes the declared type;
/// a value of that type (or text into varchar) is kept. Anything else would need an assignment
/// cast, which this subset does not perform, so it is refused rather than stored mistyped.
pub(crate) fn fit_type(value: Value, declared: Option<u32>, what: &str) -> Result<Value, SqlError> {
    let Some(oid) = declared else {
        return Ok(value);
    };
    if value.is_null() {
        return Ok(Value::null(oid));
    }
    if value.type_oid == oid || (text_like(value.type_oid) && text_like(oid)) {
        return Ok(Value {
            type_oid: oid,
            text: value.text,
        });
    }
    Err(SqlError::new(
        "42804",
        format!(
            "cannot store a value of type OID {} into {what} of type OID {oid}: assignment casts are not supported",
            value.type_oid
        ),
    ))
}
