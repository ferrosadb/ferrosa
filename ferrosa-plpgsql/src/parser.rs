//! The PL/pgSQL subset's parser.
//!
//! A body becomes a [`Program`]: variables resolved to slots, expressions lowered to SQL text
//! with `$1 … $n` placeholders. Anything outside the subset is refused here, naming the
//! construct, so neither runtime ever sees it.

use std::collections::HashSet;

use crate::ast::{Cond, Declaration, Expr, Program, RoutineKind, Stmt, VarId, VarKind, Variable};
use crate::lexer::{tokenize, Tok, Token};
use crate::types::{type_oid, OID_TEXT};
use crate::PlError;

/// A function's declared signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionSignature {
    /// `(name, type)` per argument; the name is optional.
    pub args: Vec<(Option<String>, String)>,
    /// What it returns.
    pub returns: ReturnType,
}

/// A function's declared return type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReturnType {
    /// `RETURNS trigger`.
    Trigger,
    /// `RETURNS void`.
    Void,
    /// `RETURNS <type>`.
    Scalar(String),
}

/// Real PL/pgSQL statements this subset refuses, by their leading keyword.
const REFUSED_STATEMENTS: &[&str] = &[
    "raise",
    "loop",
    "while",
    "for",
    "foreach",
    "exit",
    "continue",
    "case",
    "execute",
    "get",
    "open",
    "fetch",
    "move",
    "close",
    "commit",
    "rollback",
    "assert",
    "call",
    "with",
    "savepoint",
];

/// SQL statements whose text takes parameters (variables are bound into them).
const DML_STATEMENTS: &[&str] = &["insert", "update", "delete"];

/// Utility statements: PostgreSQL never substitutes variables into these.
const UTILITY_STATEMENTS: &[&str] = &[
    "create", "alter", "drop", "truncate", "comment", "grant", "revoke", "lock", "analyze",
    "vacuum", "reindex",
];

/// Parse a `DO` block body (the text between the dollar quotes).
///
/// # Errors
///
/// A [`PlError`] naming the refused construct or the syntax error.
pub fn parse_do_block(body: &str) -> Result<Program, PlError> {
    Parser::new(body, RoutineKind::Do)?.program()
}

/// Parse a function body under its signature.
///
/// # Errors
///
/// A [`PlError`] naming the refused construct, an unsupported argument or return type, or the
/// syntax error.
pub fn parse_function(body: &str, sig: &FunctionSignature) -> Result<Program, PlError> {
    let kind = match &sig.returns {
        ReturnType::Trigger => RoutineKind::Trigger,
        ReturnType::Void => RoutineKind::Void,
        ReturnType::Scalar(name) => RoutineKind::Scalar(
            type_oid(name).ok_or_else(|| PlError::unsupported(&format!("return type {name}")))?,
        ),
    };
    if kind == RoutineKind::Trigger && !sig.args.is_empty() {
        return Err(PlError::new(
            "42P13",
            "trigger functions cannot have declared arguments",
        ));
    }
    let mut parser = Parser::new(body, kind)?;
    for (i, (name, ty)) in sig.args.iter().enumerate() {
        parser.add_argument(i, name.as_deref(), ty)?;
    }
    parser.program()
}

/// How identifiers in a token range are treated when it is lowered to SQL.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Subst {
    /// An expression: every variable reference is bound.
    Expression,
    /// INSERT/UPDATE/DELETE: bound, except in name positions (target list, SET targets).
    Dml,
    /// A utility statement: nothing is bound.
    None,
}

struct Parser<'a> {
    src: &'a str,
    toks: Vec<Token>,
    pos: usize,
    vars: Vec<Variable>,
    /// Name → slot for locals and named arguments (later entries shadow earlier ones).
    scope: Vec<(String, VarId)>,
    kind: RoutineKind,
    arg_count: usize,
}

impl<'a> Parser<'a> {
    fn new(src: &'a str, kind: RoutineKind) -> Result<Self, PlError> {
        Ok(Self {
            src,
            toks: tokenize(src)?,
            pos: 0,
            vars: Vec::new(),
            scope: Vec::new(),
            kind,
            arg_count: 0,
        })
    }

    fn add_argument(&mut self, index: usize, name: Option<&str>, ty: &str) -> Result<(), PlError> {
        let oid =
            type_oid(ty).ok_or_else(|| PlError::unsupported(&format!("argument type {ty}")))?;
        let display = name.map_or_else(|| format!("${}", index + 1), str::to_ascii_lowercase);
        let id = self.push_var(display, VarKind::Argument(index), Some(oid));
        if let Some(name) = name {
            self.scope.push((name.to_ascii_lowercase(), id));
        }
        self.arg_count = index + 1;
        Ok(())
    }

    fn push_var(&mut self, name: String, kind: VarKind, type_oid: Option<u32>) -> VarId {
        self.vars.push(Variable {
            name,
            kind,
            type_oid,
        });
        VarId(self.vars.len() - 1)
    }

    // ---- token access ----

    fn peek(&self) -> Option<&Token> {
        self.toks.get(self.pos)
    }

    fn peek_word(&self, w: &str) -> bool {
        self.peek().is_some_and(|t| t.is_word(w))
    }

    fn peek_op(&self, op: &str) -> bool {
        self.peek().is_some_and(|t| t.is_op(op))
    }

    fn text(&self, i: usize) -> &str {
        &self.src[self.toks[i].start..self.toks[i].end]
    }

    fn near(&self) -> String {
        self.peek().map_or_else(
            || "end of input".to_string(),
            |t| format!("\"{}\"", &self.src[t.start..t.end]),
        )
    }

    fn expect_word(&mut self, w: &str) -> Result<(), PlError> {
        if self.peek_word(w) {
            self.pos += 1;
            return Ok(());
        }
        Err(PlError::syntax(format!(
            "expected {} at or near {}",
            w.to_ascii_uppercase(),
            self.near()
        )))
    }

    fn expect_op(&mut self, op: &str) -> Result<(), PlError> {
        if self.peek_op(op) {
            self.pos += 1;
            return Ok(());
        }
        Err(PlError::syntax(format!(
            "expected \"{op}\" at or near {}",
            self.near()
        )))
    }

    // ---- blocks ----

    fn program(mut self) -> Result<Program, PlError> {
        if self.peek_op("<<") {
            return Err(PlError::unsupported("<<label>>"));
        }
        let declarations = if self.peek_word("declare") {
            self.pos += 1;
            self.declarations()?
        } else {
            Vec::new()
        };
        self.expect_word("begin")?;
        let body = self.statements(&["end", "exception"])?;
        if self.peek_word("exception") {
            return Err(PlError::unsupported("EXCEPTION"));
        }
        self.expect_word("end")?;
        if self.peek_op(";") {
            self.pos += 1;
        }
        if self.pos != self.toks.len() {
            return Err(PlError::syntax(format!(
                "unexpected {} after the block's END",
                self.near()
            )));
        }
        Ok(Program {
            variables: self.vars,
            declarations,
            body,
            kind: self.kind,
            arg_count: self.arg_count,
        })
    }

    fn declarations(&mut self) -> Result<Vec<Declaration>, PlError> {
        let mut out = Vec::new();
        while !self.peek_word("begin") {
            if self.peek().is_none() {
                return Err(PlError::syntax("missing BEGIN after DECLARE"));
            }
            out.push(self.declaration()?);
        }
        Ok(out)
    }

    fn declaration(&mut self) -> Result<Declaration, PlError> {
        let name = self.name_token()?;
        for (word, construct) in [
            ("constant", "CONSTANT"),
            ("alias", "ALIAS"),
            ("cursor", "CURSOR"),
        ] {
            if self.peek_word(word) {
                return Err(PlError::unsupported(construct));
            }
        }
        let type_start = self.pos;
        while let Some(t) = self.peek() {
            if t.is_op(";") || t.is_op(":=") || t.is_op("=") || t.is_word("default") {
                break;
            }
            if t.is_word("not") || t.is_word("collate") {
                return Err(PlError::unsupported(&format!(
                    "{} in a declaration",
                    self.text(self.pos).to_ascii_uppercase()
                )));
            }
            self.pos += 1;
        }
        if self.pos == type_start || self.peek().is_none() {
            return Err(PlError::syntax(format!(
                "declaration of \"{name}\" has no type"
            )));
        }
        let ty = &self.src[self.toks[type_start].start..self.toks[self.pos - 1].end];
        let oid = type_oid(ty).ok_or_else(|| PlError::unsupported(&format!("type {ty}")))?;
        let default = if self.peek_op(";") {
            None
        } else {
            self.pos += 1; // `:=`, `=` or DEFAULT
            let range = self.until_semicolon()?;
            Some(self.lower(range.collect(), Subst::Expression)?)
        };
        if self.peek_op(";") {
            self.pos += 1;
        }
        // The name is kept twice: as the slot's display name and as its scope key.
        let var = self.push_var(name.clone(), VarKind::Local, Some(oid));
        self.scope.push((name, var));
        Ok(Declaration { var, default })
    }

    /// An identifier: unquoted is lowercased, quoted is exact.
    fn name_token(&mut self) -> Result<String, PlError> {
        let name = match self.peek().map(|t| &t.tok) {
            Some(Tok::Word(w)) => w.clone(), // owned copy: the token stays in the stream
            Some(Tok::Quoted(q)) => q.clone(), // owned copy: the token stays in the stream
            _ => {
                return Err(PlError::syntax(format!(
                    "expected a name at or near {}",
                    self.near()
                )))
            }
        };
        self.pos += 1;
        Ok(name)
    }

    /// Statements until one of `terminators` (left unconsumed).
    fn statements(&mut self, terminators: &[&str]) -> Result<Vec<Stmt>, PlError> {
        let mut out = Vec::new();
        loop {
            let Some(t) = self.peek() else {
                return Err(PlError::syntax(
                    "unexpected end of PL/pgSQL body: missing END",
                ));
            };
            if terminators.iter().any(|w| t.is_word(w)) {
                return Ok(out);
            }
            out.push(self.statement()?);
        }
    }

    fn statement(&mut self) -> Result<Stmt, PlError> {
        if self.peek_op("<<") {
            return Err(PlError::unsupported("<<label>>"));
        }
        let word = match self.peek().map(|t| &t.tok) {
            Some(Tok::Word(w)) => w.clone(), // owned copy: matched below while `self` is borrowed mutably
            Some(Tok::Quoted(_)) => return self.assignment(),
            _ => {
                return Err(PlError::syntax(format!(
                    "syntax error at or near {}",
                    self.near()
                )))
            }
        };
        match word.as_str() {
            "if" => self.if_statement(),
            "return" => self.return_statement(),
            "perform" => self.perform(),
            "select" => self.select_into(),
            "null" if self.toks.get(self.pos + 1).is_some_and(|t| t.is_op(";")) => {
                self.pos += 2;
                Ok(Stmt::Null)
            }
            "begin" => Err(PlError::unsupported("nested BEGIN block")),
            "declare" => Err(PlError::unsupported("nested DECLARE block")),
            w if REFUSED_STATEMENTS.contains(&w) => {
                Err(PlError::unsupported(&w.to_ascii_uppercase()))
            }
            w if DML_STATEMENTS.contains(&w) => self.sql_statement(Subst::Dml),
            w if UTILITY_STATEMENTS.contains(&w) => self.sql_statement(Subst::None),
            _ => self.assignment(),
        }
    }

    fn assignment(&mut self) -> Result<Stmt, PlError> {
        let at = self.pos;
        let next = self.toks.get(at + 1);
        if next.is_some_and(|t| t.is_op("."))
            && (self.toks[at].is_word("new") || self.toks[at].is_word("old"))
        {
            return Err(PlError::unsupported("assignment to NEW or OLD"));
        }
        if !next.is_some_and(|t| t.is_op(":=") || t.is_op("=")) {
            return Err(PlError::syntax(format!(
                "syntax error at or near {}",
                self.near()
            )));
        }
        let name = self.name_token()?;
        let target = self
            .lookup(&name)
            .ok_or_else(|| PlError::syntax(format!("\"{name}\" is not a known variable")))?;
        self.pos += 1; // `:=` / `=`
        let range = self.until_semicolon()?;
        let expr = self.lower(range.collect(), Subst::Expression)?;
        self.expect_op(";")?;
        Ok(Stmt::Assign { target, expr })
    }

    fn lookup(&self, name: &str) -> Option<VarId> {
        self.scope
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .map(|(_, id)| *id)
    }

    /// The token range up to (not including) the next `;` at paren depth 0; `pos` is left on
    /// the `;`.
    fn until_semicolon(&mut self) -> Result<std::ops::Range<usize>, PlError> {
        let start = self.pos;
        let mut depth = 0i32;
        while let Some(t) = self.peek() {
            match &t.tok {
                Tok::Op(o) if o == "(" || o == "[" => depth += 1,
                Tok::Op(o) if o == ")" || o == "]" => depth -= 1,
                Tok::Op(o) if o == ";" && depth <= 0 => return Ok(start..self.pos),
                _ => {}
            }
            self.pos += 1;
        }
        Err(PlError::syntax(
            "missing \";\" at end of PL/pgSQL statement",
        ))
    }

    fn if_statement(&mut self) -> Result<Stmt, PlError> {
        self.pos += 1; // IF
        let mut branches = Vec::new();
        let mut otherwise = Vec::new();
        loop {
            let cond = self.condition()?;
            let body = self.statements(&["elsif", "elseif", "else", "end"])?;
            branches.push((cond, body));
            if self.peek_word("elsif") || self.peek_word("elseif") {
                self.pos += 1;
                continue;
            }
            if self.peek_word("else") {
                self.pos += 1;
                otherwise = self.statements(&["end"])?;
            }
            break;
        }
        self.expect_word("end")?;
        self.expect_word("if")?;
        self.expect_op(";")?;
        Ok(Stmt::If {
            branches,
            otherwise,
        })
    }

    /// The condition between IF/ELSIF and THEN (`THEN` inside a `CASE` belongs to the CASE).
    fn condition(&mut self) -> Result<Cond, PlError> {
        let start = self.pos;
        let (mut depth, mut case_depth) = (0i32, 0i32);
        loop {
            let Some(t) = self.peek() else {
                return Err(PlError::syntax("missing THEN in IF statement"));
            };
            match &t.tok {
                Tok::Op(o) if o == "(" => depth += 1,
                Tok::Op(o) if o == ")" => depth -= 1,
                Tok::Op(o) if o == ";" => {
                    return Err(PlError::syntax("missing THEN in IF statement"))
                }
                Tok::Word(w) if w == "case" => case_depth += 1,
                Tok::Word(w) if w == "end" && case_depth > 0 => case_depth -= 1,
                Tok::Word(w) if w == "then" && depth == 0 && case_depth == 0 => break,
                _ => {}
            }
            self.pos += 1;
        }
        let range = start..self.pos;
        self.pos += 1; // THEN
        if range.is_empty() {
            return Err(PlError::syntax("IF has no condition"));
        }
        self.classify_condition(range)
    }

    /// `[NOT] EXISTS ( query )` spanning the whole condition is an existence test; anything
    /// else is a boolean expression.
    fn classify_condition(&mut self, range: std::ops::Range<usize>) -> Result<Cond, PlError> {
        let negated = self.toks[range.start].is_word("not");
        let exists_at = range.start + usize::from(negated);
        let is_exists = self
            .toks
            .get(exists_at)
            .is_some_and(|t| t.is_word("exists"))
            && self.toks.get(exists_at + 1).is_some_and(|t| t.is_op("("))
            && self.matching_close(exists_at + 1) == Some(range.end - 1);
        if !is_exists {
            return Ok(Cond::Expr(self.lower(range.collect(), Subst::Expression)?));
        }
        let query = self.lower((exists_at + 2..range.end - 1).collect(), Subst::Expression)?;
        Ok(Cond::Exists { negated, query })
    }

    /// The index of the `)` matching the `(` at `open`.
    fn matching_close(&self, open: usize) -> Option<usize> {
        let mut depth = 0i32;
        for (i, t) in self.toks.iter().enumerate().skip(open) {
            if t.is_op("(") {
                depth += 1;
            } else if t.is_op(")") {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
        }
        None
    }

    fn return_statement(&mut self) -> Result<Stmt, PlError> {
        self.pos += 1; // RETURN
        if self.peek_word("next") || self.peek_word("query") {
            let which = self.text(self.pos).to_ascii_uppercase();
            return Err(PlError::unsupported(&format!("RETURN {which}")));
        }
        if self.peek_op(";") {
            self.pos += 1;
            if matches!(self.kind, RoutineKind::Scalar(_) | RoutineKind::Trigger) {
                return Err(PlError::syntax(
                    "RETURN needs a value in a function that returns one",
                ));
            }
            return Ok(Stmt::Return(None));
        }
        if matches!(self.kind, RoutineKind::Do | RoutineKind::Void) {
            return Err(PlError::new(
                "42804",
                "RETURN cannot have a parameter in a DO block or a function returning void",
            ));
        }
        let range = self.until_semicolon()?;
        let expr = self.lower(range.collect(), Subst::Expression)?;
        self.expect_op(";")?;
        Ok(Stmt::Return(Some(expr)))
    }

    fn perform(&mut self) -> Result<Stmt, PlError> {
        self.pos += 1; // PERFORM
        let range = self.until_semicolon()?;
        self.expect_op(";")?;
        if range.is_empty() {
            return Err(PlError::syntax("PERFORM needs an expression"));
        }
        if let Some((channel, payload)) = self.pg_notify_args(range.start..range.end) {
            let channel = self.lower(channel, Subst::Expression)?;
            let payload = self.lower(payload, Subst::Expression)?;
            return Ok(Stmt::Notify { channel, payload });
        }
        Ok(Stmt::Perform(
            self.lower(range.collect(), Subst::Expression)?,
        ))
    }

    /// `pg_notify(a, b)` as the whole expression: the token indices of `a` and `b`.
    fn pg_notify_args(&self, range: std::ops::Range<usize>) -> Option<(Vec<usize>, Vec<usize>)> {
        let open = range.start + 1;
        let is_call = self.toks[range.start].is_word("pg_notify")
            && self.toks.get(open).is_some_and(|t| t.is_op("("))
            && self.matching_close(open) == Some(range.end - 1);
        if !is_call {
            return None;
        }
        let mut depth = 0i32;
        let mut args = vec![Vec::new()];
        for i in open + 1..range.end - 1 {
            let t = &self.toks[i];
            if t.is_op("(") {
                depth += 1;
            } else if t.is_op(")") {
                depth -= 1;
            } else if t.is_op(",") && depth == 0 {
                args.push(Vec::new());
                continue;
            }
            args.last_mut()?.push(i);
        }
        let mut args = args.into_iter();
        match (args.next(), args.next(), args.next()) {
            (Some(a), Some(b), None) if !a.is_empty() && !b.is_empty() => Some((a, b)),
            _ => None,
        }
    }

    fn select_into(&mut self) -> Result<Stmt, PlError> {
        let range = self.until_semicolon()?;
        self.expect_op(";")?;
        let Some(into) = self.depth0_word(range.start..range.end, "into") else {
            return Err(PlError::syntax(
                "query has no destination for result data: use PERFORM or SELECT … INTO",
            ));
        };
        if self.toks.get(into + 1).is_some_and(|t| t.is_word("strict")) {
            return Err(PlError::unsupported("INTO STRICT"));
        }
        let (targets, after) = self.parse_into_targets(into + 1, range.end)?;
        let indices = (range.start..into).chain(after..range.end).collect();
        let query = self.lower(indices, Subst::Expression)?;
        Ok(Stmt::SelectInto { targets, query })
    }

    /// The `INTO` target list starting at `i`: its slots and the index after it.
    fn parse_into_targets(&self, mut i: usize, end: usize) -> Result<(Vec<VarId>, usize), PlError> {
        let mut targets = Vec::new();
        while i < end {
            let name = match &self.toks[i].tok {
                Tok::Word(w) => w.as_str(),
                Tok::Quoted(q) => q.as_str(),
                _ => break,
            };
            let var = self
                .lookup(name)
                .ok_or_else(|| PlError::syntax(format!("\"{name}\" is not a known variable")))?;
            targets.push(var);
            i += 1;
            if i < end && self.toks[i].is_op(",") {
                i += 1;
            } else {
                break;
            }
        }
        if targets.is_empty() {
            return Err(PlError::syntax("INTO needs at least one target variable"));
        }
        Ok((targets, i))
    }

    fn depth0_word(&self, range: std::ops::Range<usize>, word: &str) -> Option<usize> {
        let mut depth = 0i32;
        for i in range {
            let t = &self.toks[i];
            if t.is_op("(") {
                depth += 1;
            } else if t.is_op(")") {
                depth -= 1;
            } else if depth == 0 && t.is_word(word) {
                return Some(i);
            }
        }
        None
    }

    fn sql_statement(&mut self, subst: Subst) -> Result<Stmt, PlError> {
        let range = self.until_semicolon()?;
        self.expect_op(";")?;
        if subst == Subst::Dml {
            if let Some(ret) = self.depth0_word(range.start..range.end, "returning") {
                if self.depth0_word(ret..range.end, "into").is_some() {
                    return Err(PlError::unsupported("RETURNING … INTO"));
                }
            }
        }
        Ok(Stmt::Execute(self.lower(range.collect(), subst)?))
    }

    // ---- lowering an expression to SQL ----

    /// Rebuild the SQL text of `indices` (token positions, ascending), replacing variable
    /// references with placeholders. Adjacent tokens keep their source spacing; a gap that
    /// held a comment, or a skipped run, becomes one space.
    fn lower(&mut self, indices: Vec<usize>, subst: Subst) -> Result<Expr, PlError> {
        let excluded = match subst {
            Subst::Dml => self.dml_name_positions(&indices),
            _ => HashSet::new(),
        };
        let mut sql = String::new();
        let mut refs: Vec<VarId> = Vec::new();
        let mut prev: Option<usize> = None;
        let mut k = 0;
        while k < indices.len() {
            let i = indices[k];
            if let Some(p) = prev {
                let gap = &self.src[self.toks[p].end..self.toks[i].start];
                let adjacent = p + 1 == i && gap.chars().all(char::is_whitespace);
                sql.push_str(if adjacent { gap } else { " " });
            }
            let consumed = if subst == Subst::None || excluded.contains(&i) {
                sql.push_str(self.text(i));
                1
            } else {
                self.lower_token(i, &mut sql, &mut refs)?
            };
            prev = Some(indices[k + consumed - 1]);
            k += consumed;
        }
        Ok(Expr { sql, refs })
    }

    /// Lower token `i`; returns how many tokens it consumed.
    fn lower_token(
        &mut self,
        i: usize,
        sql: &mut String,
        refs: &mut Vec<VarId>,
    ) -> Result<usize, PlError> {
        let prev_is = |op: &str| i > 0 && self.toks[i - 1].is_op(op);
        let prev_word_as = i > 0 && self.toks[i - 1].is_word("as");
        let next = self.toks.get(i + 1);
        let next_is_dot = next.is_some_and(|t| t.is_op("."));
        let next_is_call = next.is_some_and(|t| t.is_op("("));
        let plain_position =
            !(prev_is(".") || prev_is("::") || prev_word_as || next_is_call || next_is_dot);
        if plain_position && matches!(&self.toks[i].tok, Tok::Word(w) if w.starts_with("tg_")) {
            let word = self.text(i).to_ascii_lowercase();
            match self.trigger_variable(&word)? {
                Some(id) => push_placeholder(sql, refs, id),
                None => sql.push_str(self.text(i)),
            }
            return Ok(1);
        }
        let var = match &self.toks[i].tok {
            Tok::Param(n) => Some(self.positional(*n)?),
            Tok::Word(w) if w == "new" || w == "old" => {
                return self.record_field(i, sql, refs);
            }
            _ if !plain_position => None,
            Tok::Word(w) | Tok::Quoted(w) => self.lookup(w),
            _ => None,
        };
        match var {
            Some(id) => push_placeholder(sql, refs, id),
            None => sql.push_str(self.text(i)),
        }
        Ok(1)
    }

    /// `$n` in a function body: its argument's slot.
    fn positional(&self, n: usize) -> Result<VarId, PlError> {
        if self.kind == RoutineKind::Do || n == 0 || n > self.arg_count {
            return Err(PlError::new("42P02", format!("there is no parameter ${n}")));
        }
        Ok(VarId(n - 1))
    }

    /// `NEW.col` / `OLD.col` in a trigger function.
    fn record_field(
        &mut self,
        i: usize,
        sql: &mut String,
        refs: &mut Vec<VarId>,
    ) -> Result<usize, PlError> {
        let which = self.text(i).to_ascii_lowercase();
        let qualified = self.toks.get(i + 1).is_some_and(|t| t.is_op("."));
        if !qualified && i > 0 && (self.toks[i - 1].is_op(".") || self.toks[i - 1].is_op("::")) {
            sql.push_str(self.text(i));
            return Ok(1);
        }
        if self.kind != RoutineKind::Trigger {
            return Err(PlError::new(
                "42P01",
                format!("missing FROM-clause entry for table \"{which}\": {} exists only in trigger functions", which.to_ascii_uppercase()),
            ));
        }
        let column = match self.toks.get(i + 2).map(|t| &t.tok) {
            Some(Tok::Word(c)) | Some(Tok::Quoted(c)) if qualified => c.clone(), // owned copy: becomes the slot's column name
            _ => {
                return Err(PlError::unsupported(&format!(
                    "{} as a whole record",
                    which.to_ascii_uppercase()
                )))
            }
        };
        let kind = if which == "new" {
            VarKind::NewField(column.clone()) // owned copy: the kind and the display name both keep it
        } else {
            VarKind::OldField(column.clone()) // owned copy: the kind and the display name both keep it
        };
        let id = self.special_var(kind, format!("{which}.{column}"), None);
        push_placeholder(sql, refs, id);
        Ok(3)
    }

    fn trigger_variable(&mut self, word: &str) -> Result<Option<VarId>, PlError> {
        if self.kind != RoutineKind::Trigger {
            return Ok(self.lookup(word));
        }
        if word == "tg_op" {
            return Ok(Some(self.special_var(
                VarKind::TgOp,
                "tg_op".into(),
                Some(OID_TEXT),
            )));
        }
        Err(PlError::unsupported(&word.to_ascii_uppercase()))
    }

    fn special_var(&mut self, kind: VarKind, name: String, oid: Option<u32>) -> VarId {
        match self.vars.iter().position(|v| v.kind == kind) {
            Some(existing) => VarId(existing),
            None => self.push_var(name, kind, oid),
        }
    }

    /// Token positions in a DML statement that name things rather than hold expressions:
    /// the head (`INSERT INTO t (cols)`, `UPDATE t [AS a] SET`, `DELETE FROM t`) and each
    /// `SET` target.
    fn dml_name_positions(&self, indices: &[usize]) -> HashSet<usize> {
        let mut out = HashSet::new();
        let Some(&first) = indices.first() else {
            return out;
        };
        let head_end: &[&str] = if self.toks[first].is_word("insert") {
            &["values", "select", "default", "overriding"]
        } else if self.toks[first].is_word("update") {
            &["set"]
        } else {
            &["where", "using", "returning"]
        };
        let mut depth = 0i32;
        let mut in_head = true;
        let mut in_set = false;
        for (k, &i) in indices.iter().enumerate() {
            let t = &self.toks[i];
            if depth == 0 && in_head && head_end.iter().any(|w| t.is_word(w)) {
                in_head = false;
                in_set = t.is_word("set");
                out.insert(i);
                continue;
            }
            if depth == 0
                && in_set
                && (t.is_word("where") || t.is_word("from") || t.is_word("returning"))
            {
                in_set = false;
            }
            let set_target = in_set
                && depth == 0
                && indices.get(k + 1).is_some_and(|&n| self.toks[n].is_op("="))
                && k > 0
                && (self.toks[indices[k - 1]].is_word("set")
                    || self.toks[indices[k - 1]].is_op(","));
            if in_head || set_target {
                out.insert(i);
            }
            if t.is_op("(") {
                depth += 1;
            } else if t.is_op(")") {
                depth -= 1;
            }
        }
        out
    }
}

/// Append the placeholder for `id`, reusing its number when it is already bound.
fn push_placeholder(sql: &mut String, refs: &mut Vec<VarId>, id: VarId) {
    let n = match refs.iter().position(|r| *r == id) {
        Some(p) => p + 1,
        None => {
            refs.push(id);
            refs.len()
        }
    };
    sql.push('$');
    sql.push_str(&n.to_string());
}
