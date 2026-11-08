//! The front-end statements this crate owns: `DO`, `CREATE [OR REPLACE] FUNCTION … LANGUAGE
//! plpgsql`, `DROP FUNCTION`, `CREATE TRIGGER`, `DROP TRIGGER`, `LISTEN`, `UNLISTEN`,
//! `NOTIFY`, and `SELECT pg_notify(a, b)` with literal or `$n` arguments.
//!
//! [`parse_utility`] answers `None` for any other statement, so the front end hands it to the
//! SQL engine unchanged. Once a statement is recognised, a clause outside the subset is refused
//! with `0A000` naming it.

use crate::lexer::{tokenize, Tok, Token};
use crate::{FunctionSignature, PlError, ReturnType};

/// `schema.name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedName {
    /// Schema, when written.
    pub schema: Option<String>,
    /// Name.
    pub name: String,
}

/// `CREATE [OR REPLACE] FUNCTION … LANGUAGE plpgsql`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFunction {
    /// `OR REPLACE`.
    pub or_replace: bool,
    /// The function's name.
    pub name: QualifiedName,
    /// Arguments and return type.
    pub signature: FunctionSignature,
    /// The body text.
    pub body: String,
    /// `immutable` / `stable` / `volatile`, when written.
    pub volatility: Option<String>,
}

/// A trigger event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerEvent {
    /// `INSERT`.
    Insert,
    /// `UPDATE [OF columns]` (empty: any update).
    Update {
        /// The `OF` columns.
        columns: Vec<String>,
    },
}

/// `CREATE TRIGGER … AFTER … FOR EACH ROW EXECUTE PROCEDURE f()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTrigger {
    /// Trigger name.
    pub name: String,
    /// Events, in order written.
    pub events: Vec<TriggerEvent>,
    /// The table.
    pub table: QualifiedName,
    /// The trigger function.
    pub function: QualifiedName,
}

/// A `pg_notify` argument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotifyArg {
    /// A string literal's value.
    Literal(String),
    /// `$n`.
    Param(usize),
}

/// A recognised statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Utility {
    /// `DO`.
    Do {
        /// The block body.
        body: String,
    },
    /// `CREATE FUNCTION`.
    CreateFunction(CreateFunction),
    /// `DROP FUNCTION`.
    DropFunction {
        /// `IF EXISTS`.
        if_exists: bool,
        /// Name.
        name: QualifiedName,
        /// Argument types, when a list was written.
        arg_types: Option<Vec<String>>,
    },
    /// `CREATE TRIGGER`.
    CreateTrigger(CreateTrigger),
    /// `DROP TRIGGER`.
    DropTrigger {
        /// `IF EXISTS`.
        if_exists: bool,
        /// Trigger name.
        name: String,
        /// Table.
        table: QualifiedName,
    },
    /// `LISTEN channel`.
    Listen(String),
    /// `UNLISTEN channel` / `UNLISTEN *` (`None`).
    Unlisten(Option<String>),
    /// `NOTIFY channel [, payload]`.
    Notify {
        /// Channel.
        channel: String,
        /// Payload (empty when absent).
        payload: String,
    },
    /// `SELECT pg_notify(a, b)`.
    PgNotify {
        /// Channel argument.
        channel: NotifyArg,
        /// Payload argument.
        payload: NotifyArg,
    },
}

/// Recognise a statement this crate owns: `None` when it is not one of them.
pub fn parse_utility(sql: &str) -> Option<Result<Utility, PlError>> {
    let toks = tokenize(sql).ok()?;
    let mut c = Cursor {
        sql,
        toks,
        pos: 0,
        not_ours: false,
    };
    let first = c.word_at(0)?;
    let second = c.word_at(1);
    let result = match (first.as_str(), second.as_deref()) {
        ("do", _) => c.do_block(),
        ("create", Some("function" | "or" | "trigger" | "constraint")) => c.create(),
        ("drop", Some("function")) => c.drop_function(),
        ("drop", Some("trigger")) => c.drop_trigger(),
        ("listen", _) => c.listen(),
        ("unlisten", _) => c.unlisten(),
        ("notify", _) => c.notify(),
        ("select", Some("pg_notify")) => return c.pg_notify(),
        _ => return None,
    };
    // CREATE OR REPLACE <something else> is not ours; the SQL engine answers it.
    if c.not_ours {
        return None;
    }
    Some(result)
}

struct Cursor<'a> {
    sql: &'a str,
    toks: Vec<Token>,
    pos: usize,
    /// Set when a statement turned out to be someone else's (`CREATE OR REPLACE VIEW`).
    not_ours: bool,
}

impl Cursor<'_> {
    fn word_at(&self, i: usize) -> Option<String> {
        match &self.toks.get(i)?.tok {
            Tok::Word(w) => Some(w.clone()), // owned copy: dispatch inspects it while the cursor moves
            _ => None,
        }
    }

    fn peek_word(&self, w: &str) -> bool {
        self.toks.get(self.pos).is_some_and(|t| t.is_word(w))
    }

    fn peek_op(&self, op: &str) -> bool {
        self.toks.get(self.pos).is_some_and(|t| t.is_op(op))
    }

    fn eat_word(&mut self, w: &str) -> bool {
        let yes = self.peek_word(w);
        self.pos += usize::from(yes);
        yes
    }

    fn eat_op(&mut self, op: &str) -> bool {
        let yes = self.peek_op(op);
        self.pos += usize::from(yes);
        yes
    }

    fn near(&self) -> String {
        self.toks.get(self.pos).map_or_else(
            || "end of input".into(),
            |t| format!("\"{}\"", &self.sql[t.start..t.end]),
        )
    }

    fn expect_word(&mut self, w: &str) -> Result<(), PlError> {
        if self.eat_word(w) {
            return Ok(());
        }
        Err(PlError::syntax(format!(
            "expected {} at or near {}",
            w.to_ascii_uppercase(),
            self.near()
        )))
    }

    fn expect_op(&mut self, op: &str) -> Result<(), PlError> {
        if self.eat_op(op) {
            return Ok(());
        }
        Err(PlError::syntax(format!(
            "expected \"{op}\" at or near {}",
            self.near()
        )))
    }

    /// Trailing semicolons, then the end.
    fn finish(&mut self) -> Result<(), PlError> {
        while self.eat_op(";") {}
        if self.pos == self.toks.len() {
            return Ok(());
        }
        Err(PlError::syntax(format!("unexpected {}", self.near())))
    }

    fn ident(&mut self) -> Result<String, PlError> {
        let name = match self.toks.get(self.pos).map(|t| &t.tok) {
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

    fn qualified(&mut self) -> Result<QualifiedName, PlError> {
        let first = self.ident()?;
        if !self.eat_op(".") {
            return Ok(QualifiedName {
                schema: None,
                name: first,
            });
        }
        let name = self.ident()?;
        Ok(QualifiedName {
            schema: Some(first),
            name,
        })
    }

    /// The value of the string literal at the cursor.
    fn string(&mut self) -> Result<String, PlError> {
        let Some(t) = self.toks.get(self.pos).filter(|t| t.tok == Tok::Str) else {
            return Err(PlError::syntax(format!(
                "expected a string at or near {}",
                self.near()
            )));
        };
        let value = string_value(&self.sql[t.start..t.end])?;
        self.pos += 1;
        Ok(value)
    }

    fn do_block(&mut self) -> Result<Utility, PlError> {
        self.pos = 1;
        let mut body = None;
        while self.pos < self.toks.len() && !self.peek_op(";") {
            if self.eat_word("language") {
                check_language(&self.ident()?)?;
            } else if body.is_none() {
                body = Some(self.string()?);
            } else {
                return Err(PlError::syntax(format!("unexpected {} in DO", self.near())));
            }
        }
        self.finish()?;
        let body = body.ok_or_else(|| PlError::syntax("DO needs a body"))?;
        Ok(Utility::Do { body })
    }
}

impl Cursor<'_> {
    fn create(&mut self) -> Result<Utility, PlError> {
        self.pos = 1;
        let or_replace = self.eat_word("or");
        if or_replace {
            self.expect_word("replace")?;
        }
        if self.eat_word("function") {
            return self.create_function(or_replace);
        }
        if self.peek_word("constraint") {
            return Err(PlError::unsupported("CONSTRAINT trigger"));
        }
        if self.eat_word("trigger") {
            if or_replace {
                return Err(PlError::unsupported("CREATE OR REPLACE TRIGGER"));
            }
            return self.create_trigger();
        }
        self.not_ours = true;
        Err(PlError::syntax("not a PL/pgSQL statement"))
    }

    fn create_function(&mut self, or_replace: bool) -> Result<Utility, PlError> {
        let name = self.qualified()?;
        let args = self.argument_list()?;
        let mut returns = None;
        let mut body = None;
        let mut language = None;
        let mut volatility = None;
        while self.pos < self.toks.len() && !self.peek_op(";") {
            if self.eat_word("returns") {
                returns = Some(self.return_type()?);
            } else if self.eat_word("as") {
                body = Some(self.string()?);
            } else if self.eat_word("language") {
                language = Some(self.ident()?);
            } else if let Some(v) = ["immutable", "stable", "volatile"]
                .into_iter()
                .find(|v| self.peek_word(v))
            {
                self.pos += 1;
                volatility = Some(v.to_string());
            } else {
                return Err(self.refused_function_clause());
            }
        }
        self.finish()?;
        check_language(
            &language.ok_or_else(|| PlError::syntax("CREATE FUNCTION needs LANGUAGE"))?,
        )?;
        Ok(Utility::CreateFunction(CreateFunction {
            or_replace,
            name,
            signature: FunctionSignature {
                args,
                returns: returns.ok_or_else(|| PlError::syntax("CREATE FUNCTION needs RETURNS"))?,
            },
            body: body.ok_or_else(|| PlError::syntax("CREATE FUNCTION needs AS 'body'"))?,
            volatility,
        }))
    }

    fn refused_function_clause(&self) -> PlError {
        let word = self
            .word_at(self.pos)
            .unwrap_or_default()
            .to_ascii_uppercase();
        if word.is_empty() {
            return PlError::syntax(format!("unexpected {} in CREATE FUNCTION", self.near()));
        }
        PlError::unsupported(&format!("{word} (in CREATE FUNCTION)"))
    }

    fn return_type(&mut self) -> Result<ReturnType, PlError> {
        for refused in ["setof", "table"] {
            if self.peek_word(refused) {
                return Err(PlError::unsupported(&format!(
                    "RETURNS {}",
                    refused.to_ascii_uppercase()
                )));
            }
        }
        let ty = self.type_name(&[
            "as",
            "language",
            "immutable",
            "stable",
            "volatile",
            "strict",
            "security",
            "cost",
            "rows",
            "parallel",
            "leakproof",
            "called",
            "set",
            "window",
        ])?;
        Ok(match ty.to_ascii_lowercase().as_str() {
            "trigger" => ReturnType::Trigger,
            "void" => ReturnType::Void,
            _ => ReturnType::Scalar(ty),
        })
    }

    /// Type-name tokens up to a stop word, `,`, `)` or `;`, as source text.
    fn type_name(&mut self, stop: &[&str]) -> Result<String, PlError> {
        let start = self.pos;
        let mut depth = 0i32;
        while let Some(t) = self.toks.get(self.pos) {
            if depth == 0
                && (t.is_op(",")
                    || t.is_op(")")
                    || t.is_op(";")
                    || stop.iter().any(|w| t.is_word(w)))
            {
                break;
            }
            if t.is_op("(") {
                depth += 1;
            } else if t.is_op(")") {
                depth -= 1;
            }
            self.pos += 1;
        }
        if self.pos == start {
            return Err(PlError::syntax(format!(
                "expected a type at or near {}",
                self.near()
            )));
        }
        Ok(self.sql[self.toks[start].start..self.toks[self.pos - 1].end].to_string())
    }

    /// `( [name] type, … )`.
    fn argument_list(&mut self) -> Result<Vec<(Option<String>, String)>, PlError> {
        self.expect_op("(")?;
        let mut args = Vec::new();
        while !self.eat_op(")") {
            for mode in ["out", "inout", "variadic"] {
                if self.peek_word(mode) {
                    return Err(PlError::unsupported(&format!(
                        "{} argument",
                        mode.to_ascii_uppercase()
                    )));
                }
            }
            self.eat_word("in");
            args.push(self.argument()?);
            if self.peek_word("default") || self.peek_op("=") {
                return Err(PlError::unsupported("DEFAULT argument value"));
            }
            if !self.eat_op(",") && !self.peek_op(")") {
                return Err(PlError::syntax(format!(
                    "unexpected {} in argument list",
                    self.near()
                )));
            }
        }
        Ok(args)
    }

    fn argument(&mut self) -> Result<(Option<String>, String), PlError> {
        let start = self.pos;
        let whole = self.type_name(&["default"])?;
        let tokens = self.pos - start;
        // `name type` when the text after the first token is a type on its own.
        if tokens >= 2 && matches!(self.toks[start].tok, Tok::Word(_) | Tok::Quoted(_)) {
            let rest = &self.sql[self.toks[start + 1].start..self.toks[self.pos - 1].end];
            if crate::type_oid(rest).is_some() && crate::type_oid(&whole).is_none() {
                let name = match &self.toks[start].tok {
                    Tok::Word(w) | Tok::Quoted(w) => w.clone(), // owned copy: becomes the argument's name
                    _ => unreachable!("matched above"),
                };
                return Ok((Some(name), rest.to_string()));
            }
        }
        Ok((None, whole))
    }

    fn drop_function(&mut self) -> Result<Utility, PlError> {
        self.pos = 2;
        let if_exists = self.if_exists()?;
        let name = self.qualified()?;
        let arg_types = if self.peek_op("(") {
            Some(
                self.argument_list()?
                    .into_iter()
                    .map(|(_, ty)| ty)
                    .collect(),
            )
        } else {
            None
        };
        self.refuse_cascade()?;
        self.finish()?;
        Ok(Utility::DropFunction {
            if_exists,
            name,
            arg_types,
        })
    }

    fn if_exists(&mut self) -> Result<bool, PlError> {
        if !self.eat_word("if") {
            return Ok(false);
        }
        self.expect_word("exists")?;
        Ok(true)
    }

    fn refuse_cascade(&mut self) -> Result<(), PlError> {
        if self.peek_word("cascade") {
            return Err(PlError::unsupported("CASCADE"));
        }
        self.eat_word("restrict");
        Ok(())
    }

    fn create_trigger(&mut self) -> Result<Utility, PlError> {
        let name = self.ident()?;
        if self.peek_word("before") {
            return Err(PlError::unsupported("BEFORE trigger"));
        }
        if self.peek_word("instead") {
            return Err(PlError::unsupported("INSTEAD OF trigger"));
        }
        self.expect_word("after")?;
        let events = self.trigger_events()?;
        self.expect_word("on")?;
        let table = self.qualified()?;
        for refused in ["referencing", "from", "not", "deferrable", "initially"] {
            if self.peek_word(refused) {
                return Err(PlError::unsupported(&refused.to_ascii_uppercase()));
            }
        }
        self.for_each_row()?;
        if self.peek_word("when") {
            return Err(PlError::unsupported("WHEN condition on a trigger"));
        }
        self.expect_word("execute")?;
        if !self.eat_word("procedure") {
            self.expect_word("function")?;
        }
        let function = self.qualified()?;
        self.expect_op("(")?;
        if !self.eat_op(")") {
            return Err(PlError::unsupported("trigger arguments (TG_ARGV)"));
        }
        self.finish()?;
        Ok(Utility::CreateTrigger(CreateTrigger {
            name,
            events,
            table,
            function,
        }))
    }

    fn trigger_events(&mut self) -> Result<Vec<TriggerEvent>, PlError> {
        let mut events = Vec::new();
        loop {
            if self.eat_word("insert") {
                events.push(TriggerEvent::Insert);
            } else if self.eat_word("update") {
                let mut columns = Vec::new();
                if self.eat_word("of") {
                    columns.push(self.ident()?);
                    while self.eat_op(",") {
                        columns.push(self.ident()?);
                    }
                }
                events.push(TriggerEvent::Update { columns });
            } else if let Some(w) = ["delete", "truncate"]
                .into_iter()
                .find(|w| self.peek_word(w))
            {
                return Err(PlError::unsupported(&format!(
                    "{} trigger event",
                    w.to_ascii_uppercase()
                )));
            } else {
                return Err(PlError::syntax(format!(
                    "expected a trigger event at or near {}",
                    self.near()
                )));
            }
            if !self.eat_word("or") {
                return Ok(events);
            }
        }
    }

    fn for_each_row(&mut self) -> Result<(), PlError> {
        if !self.eat_word("for") {
            // PostgreSQL's default is FOR EACH STATEMENT.
            return Err(PlError::unsupported("FOR EACH STATEMENT trigger"));
        }
        self.eat_word("each");
        if self.eat_word("row") {
            return Ok(());
        }
        if self.peek_word("statement") {
            return Err(PlError::unsupported("FOR EACH STATEMENT trigger"));
        }
        Err(PlError::syntax(format!(
            "expected ROW at or near {}",
            self.near()
        )))
    }

    fn drop_trigger(&mut self) -> Result<Utility, PlError> {
        self.pos = 2;
        let if_exists = self.if_exists()?;
        let name = self.ident()?;
        self.expect_word("on")?;
        let table = self.qualified()?;
        self.refuse_cascade()?;
        self.finish()?;
        Ok(Utility::DropTrigger {
            if_exists,
            name,
            table,
        })
    }

    fn listen(&mut self) -> Result<Utility, PlError> {
        self.pos = 1;
        let channel = self.ident()?;
        self.finish()?;
        Ok(Utility::Listen(channel))
    }

    fn unlisten(&mut self) -> Result<Utility, PlError> {
        self.pos = 1;
        let channel = if self.eat_op("*") {
            None
        } else {
            Some(self.ident()?)
        };
        self.finish()?;
        Ok(Utility::Unlisten(channel))
    }

    fn notify(&mut self) -> Result<Utility, PlError> {
        self.pos = 1;
        let channel = self.ident()?;
        let payload = if self.eat_op(",") {
            self.string()?
        } else {
            String::new()
        };
        self.finish()?;
        Ok(Utility::Notify { channel, payload })
    }

    /// `SELECT pg_notify(a, b)` exactly; any other shape is not ours (`None`).
    fn pg_notify(&mut self) -> Option<Result<Utility, PlError>> {
        self.pos = 2;
        if !self.eat_op("(") {
            return None;
        }
        let channel = self.notify_arg()?;
        if !self.eat_op(",") {
            return None;
        }
        let payload = self.notify_arg()?;
        if !self.eat_op(")") {
            return None;
        }
        while self.eat_op(";") {}
        if self.pos != self.toks.len() {
            return None;
        }
        Some(Ok(Utility::PgNotify { channel, payload }))
    }

    fn notify_arg(&mut self) -> Option<NotifyArg> {
        let t = self.toks.get(self.pos)?;
        let arg = match &t.tok {
            Tok::Param(n) => NotifyArg::Param(*n),
            Tok::Str => NotifyArg::Literal(string_value(&self.sql[t.start..t.end]).ok()?),
            _ => return None,
        };
        self.pos += 1;
        Some(arg)
    }
}

fn check_language(language: &str) -> Result<(), PlError> {
    if language.eq_ignore_ascii_case("plpgsql") {
        return Ok(());
    }
    Err(PlError::unsupported(&format!("LANGUAGE {language}")))
}

/// A string literal's value: `'…'` (`''` escapes), `$tag$…$tag$`, or `E'…'` with the common
/// backslash escapes (anything else in an `E` string is refused rather than guessed).
fn string_value(text: &str) -> Result<String, PlError> {
    if let Some(rest) = text.strip_prefix('$') {
        let tag_end = rest.find('$').map(|i| i + 2).unwrap_or(text.len());
        let tag_len = tag_end;
        return Ok(text[tag_len..text.len() - tag_len].to_string());
    }
    if let Some(inner) = text.strip_prefix(['E', 'e']) {
        return escape_string(&inner[1..inner.len() - 1]);
    }
    Ok(text[1..text.len() - 1].replace("''", "'"))
}

fn escape_string(inner: &str) -> Result<String, PlError> {
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => out.push(match chars.next() {
                Some('n') => '\n',
                Some('t') => '\t',
                Some('r') => '\r',
                Some('\\') => '\\',
                Some('\'') => '\'',
                other => {
                    return Err(PlError::unsupported(&format!(
                        "escape \\{} in an E'' string",
                        other.map(String::from).unwrap_or_default()
                    )))
                }
            }),
            '\'' => {
                chars.next(); // the second quote of ''
                out.push('\'');
            }
            c => out.push(c),
        }
    }
    Ok(out)
}
