//! The parsed form of a PL/pgSQL body.
//!
//! Variables are resolved to slots ([`VarId`]) at parse time and every expression is SQL text
//! whose variable references have been replaced by `$1 … $n` placeholders, with
//! [`Expr::refs`] listing the slot bound to each placeholder. Both runtimes work on this
//! form: the interpreter directly, the converter by emitting code over the same slots.

/// A variable's slot: an index into [`Program::variables`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VarId(pub usize);

/// What a slot holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VarKind {
    /// A `DECLARE`d local.
    Local,
    /// The function's argument at this position (0-based).
    Argument(usize),
    /// `NEW.<column>` in a trigger function.
    NewField(String),
    /// `OLD.<column>` in a trigger function.
    OldField(String),
    /// `TG_OP` in a trigger function.
    TgOp,
}

/// One slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Variable {
    /// The name as written (lowercased when unquoted); `new.queue` for a field.
    pub name: String,
    /// What it holds.
    pub kind: VarKind,
    /// The declared type's OID, when the slot has a declared type (locals and arguments).
    pub type_oid: Option<u32>,
}

/// An expression as PostgreSQL evaluates it: `SELECT <sql>` with `refs[i]` bound as `$i+1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expr {
    /// The SQL text with variables replaced by placeholders.
    pub sql: String,
    /// The slot bound to each placeholder, in placeholder order.
    pub refs: Vec<VarId>,
}

/// An `IF` / `ELSIF` condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cond {
    /// A boolean expression (`SELECT <expr>`).
    Expr(Expr),
    /// `[NOT] EXISTS (<query>)`: decided by whether the query yields a first row; the host
    /// is never asked for a second.
    Exists {
        /// `NOT EXISTS`.
        negated: bool,
        /// The inner query.
        query: Expr,
    },
}

/// A `DECLARE`d local with its optional initial value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declaration {
    /// The local's slot.
    pub var: VarId,
    /// `DEFAULT` / `:=` initialiser.
    pub default: Option<Expr>,
}

/// A statement of the supported subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stmt {
    /// `target := expr` / `target = expr`.
    Assign {
        /// The assigned slot.
        target: VarId,
        /// The value.
        expr: Expr,
    },
    /// `IF … THEN … [ELSIF … THEN …]* [ELSE …] END IF`.
    If {
        /// Each `IF`/`ELSIF` condition with its branch, in order.
        branches: Vec<(Cond, Vec<Stmt>)>,
        /// The `ELSE` branch (empty when absent).
        otherwise: Vec<Stmt>,
    },
    /// `RETURN [expr]`.
    Return(Option<Expr>),
    /// `PERFORM <expr>`: run and discard.
    Perform(Expr),
    /// `PERFORM pg_notify(channel, payload)`.
    Notify {
        /// Channel expression.
        channel: Expr,
        /// Payload expression.
        payload: Expr,
    },
    /// `SELECT … INTO targets …`: the first row's columns into the targets.
    SelectInto {
        /// The slots receiving the columns, in order.
        targets: Vec<VarId>,
        /// The query, with the `INTO` clause removed.
        query: Expr,
    },
    /// A SQL statement run for its effect through `execute` (`CREATE TYPE …`, `INSERT …`).
    Execute(Expr),
    /// `NULL;` — PL/pgSQL's explicit do-nothing statement.
    Null,
}

/// What kind of routine a body belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutineKind {
    /// A `DO` block: no arguments, no result.
    Do,
    /// A function returning a value of this type OID.
    Scalar(u32),
    /// A function returning `void`.
    Void,
    /// A trigger function (`RETURNS trigger`).
    Trigger,
}

/// A parsed body: its slots, its declarations and its statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Program {
    /// Every slot, indexed by [`VarId`].
    pub variables: Vec<Variable>,
    /// The `DECLARE` section, in order.
    pub declarations: Vec<Declaration>,
    /// The statements between `BEGIN` and `END`.
    pub body: Vec<Stmt>,
    /// What kind of routine it is.
    pub kind: RoutineKind,
    /// The argument count (each argument occupies a slot of kind `Argument`).
    pub arg_count: usize,
}

impl Program {
    /// The `NEW.<column>` names referenced, in slot order: the order a trigger call passes
    /// `NEW`'s values in.
    pub fn new_fields(&self) -> Vec<&str> {
        self.variables
            .iter()
            .filter_map(|v| match &v.kind {
                VarKind::NewField(column) => Some(column.as_str()),
                _ => None,
            })
            .collect()
    }

    /// The `OLD.<column>` names referenced, in slot order.
    pub fn old_fields(&self) -> Vec<&str> {
        self.variables
            .iter()
            .filter_map(|v| match &v.kind {
                VarKind::OldField(column) => Some(column.as_str()),
                _ => None,
            })
            .collect()
    }
}
