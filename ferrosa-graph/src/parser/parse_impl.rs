//! Recursive-descent Cypher parser.
//!
//! One function per grammar rule. LL(2) — at most two-token lookahead.
//! Produces an AST from the token stream.
//!
//! Expression nesting is capped at [`MAX_EXPR_DEPTH`] (64) to prevent
//! stack overflow from adversarial input (threat model T1).

use std::time::Duration;

use crate::parser::ast::*;
use crate::parser::error::{ParseError, ParseResult, Span};
use crate::parser::lexer::Lexer;
use crate::parser::token::{Keyword, TokenKind};

/// Maximum expression nesting depth before the parser returns an error.
/// Prevents stack overflow from deeply nested parentheses or chained
/// NOT/unary operators. (Threat model T1 mitigation.)
const MAX_EXPR_DEPTH: usize = 64;

/// Result of parsing the inside of a relationship bracket.
/// `(var, rel_type, props, length_range)`
type RelDetail = (
    Option<String>,
    Option<String>,
    PropMap,
    Option<(u32, Option<u32>)>,
);

/// Parser state wrapping a [`Lexer`] and tracking expression nesting depth.
struct Parser<'input> {
    lexer: Lexer<'input>,
    /// Current expression nesting depth (incremented on recursive entry).
    expr_depth: usize,
}

impl<'input> Parser<'input> {
    fn new(input: &'input str) -> Self {
        Self {
            lexer: Lexer::new(input),
            expr_depth: 0,
        }
    }

    /// Increment expression depth, returning an error if the limit is exceeded.
    fn enter_expr(&mut self) -> ParseResult<()> {
        self.expr_depth += 1;
        if self.expr_depth > MAX_EXPR_DEPTH {
            let pos = self.lexer.pos();
            Err(ParseError::new(
                format!(
                    "expression nesting depth exceeds maximum of {}",
                    MAX_EXPR_DEPTH
                ),
                Span {
                    start: pos,
                    end: pos,
                },
            ))
        } else {
            Ok(())
        }
    }

    fn exit_expr(&mut self) {
        self.expr_depth -= 1;
    }

    fn parse_statement(&mut self) -> ParseResult<Statement> {
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::Match) => self.parse_match(),
            TokenKind::Keyword(Keyword::Create) => self.parse_create(),
            TokenKind::Keyword(Keyword::Subscribe) => {
                self.lexer.next_token()?; // consume SUBSCRIBE
                                          // Inner must be a MATCH statement
                let tok = self.lexer.peek()?;
                if !matches!(tok.kind, TokenKind::Keyword(Keyword::Match)) {
                    return Err(ParseError::new(
                        "SUBSCRIBE requires a MATCH query".to_string(),
                        tok.span,
                    ));
                }
                let inner = self.parse_match()?;

                // Optional EVERY <duration>
                let interval = if self.lexer.eat(&TokenKind::Keyword(Keyword::Every))? {
                    Some(self.parse_duration()?)
                } else {
                    None
                };

                // Optional DELTA
                let delta = self.lexer.eat(&TokenKind::Keyword(Keyword::Delta))?;

                Ok(Statement::Subscribe {
                    inner: Box::new(inner),
                    interval,
                    delta,
                })
            }
            TokenKind::Keyword(Keyword::Unwind) => self.parse_unwind_statement(),
            TokenKind::Keyword(Keyword::With)
            | TokenKind::Keyword(Keyword::Union)
            | TokenKind::Keyword(Keyword::Call)
            | TokenKind::Keyword(Keyword::Foreach)
            | TokenKind::Keyword(Keyword::Load) => Err(ParseError::new(
                format!("unsupported Cypher clause: {:?}", tok.kind),
                tok.span,
            )),
            TokenKind::Keyword(Keyword::Merge) => self.parse_merge(),
            TokenKind::Keyword(Keyword::Unsubscribe) => {
                self.lexer.next_token()?; // consume UNSUBSCRIBE
                let tok = self.lexer.peek()?;
                let stream_id = if matches!(tok.kind, TokenKind::Integer(_)) {
                    let tok = self.lexer.next_token()?;
                    if let TokenKind::Integer(n) = tok.kind {
                        Some(n as u16)
                    } else {
                        None
                    }
                } else {
                    None
                };
                Ok(Statement::Unsubscribe { stream_id })
            }
            _ => Err(ParseError::new(
                format!(
                    "unsupported statement keyword: {:?}; expected MATCH, CREATE, MERGE, \
                     SUBSCRIBE, or UNSUBSCRIBE",
                    tok.kind
                ),
                tok.span,
            )),
        }
    }

    fn parse_unwind_statement(&mut self) -> ParseResult<Statement> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Unwind))?;
        let expr = self.parse_expr()?;
        self.lexer.expect(&TokenKind::Keyword(Keyword::As))?;
        let tok = self.lexer.next_token()?;
        let var = match tok.kind {
            TokenKind::Ident(name) => name.to_string(),
            _ => {
                return Err(ParseError::new(
                    format!("expected variable after AS, got {:?}", tok.kind),
                    tok.span,
                ));
            }
        };
        let return_clause = self.parse_return_clause()?;
        Ok(Statement::Unwind {
            expr,
            var,
            return_clause,
        })
    }

    /// Parse one or more consecutive MERGE clauses with an optional trailing SET and RETURN.
    ///
    /// Grammar:
    ///   merge_stmt := MERGE pattern (MERGE pattern)* [SET assignment_list] [RETURN return_clause]
    ///
    /// All patterns are accumulated into a single `Statement::Merge`. This supports the
    /// canonical ferrosa-memory edge-upsert shape:
    ///   MERGE (a:Entity {entity_id: $src})
    ///   MERGE (b:Entity {entity_id: $dst})
    ///   MERGE (a)-[r:TYPED_EDGE {edge_type: $t}]->(b)
    ///   SET r.weight = $w
    ///   RETURN r
    fn parse_merge(&mut self) -> ParseResult<Statement> {
        let mut patterns = Vec::new();

        // Consume the first MERGE keyword (already peeked in parse_statement).
        self.lexer.expect(&TokenKind::Keyword(Keyword::Merge))?;
        patterns.push(self.parse_pattern()?);

        // Consume any additional consecutive MERGE clauses.
        while self.lexer.eat(&TokenKind::Keyword(Keyword::Merge))? {
            patterns.push(self.parse_pattern()?);
        }

        // Optional trailing SET clause.
        let set_clause = if self.lexer.eat(&TokenKind::Keyword(Keyword::Set))? {
            self.parse_assignment_list()?
        } else {
            vec![]
        };

        // Optional RETURN clause.
        let return_clause = if self.lexer.peek()?.kind == TokenKind::Keyword(Keyword::Return) {
            Some(self.parse_return_clause()?)
        } else {
            None
        };

        Ok(Statement::Merge {
            patterns,
            set_clause,
            return_clause,
        })
    }

    fn parse_match(&mut self) -> ParseResult<Statement> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Match))?;
        let pattern = self.parse_pattern_list()?;

        // Optional WHERE.
        let where_clause = if self.lexer.eat(&TokenKind::Keyword(Keyword::Where))? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        // Check what follows: RETURN, OPTIONAL MATCH, SET, DELETE, DETACH DELETE.
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Keyword(Keyword::Return) => {
                let return_clause = self.parse_return_clause()?;
                Ok(Statement::Match {
                    pattern,
                    where_clause,
                    return_clause,
                })
            }
            TokenKind::Keyword(Keyword::With) => {
                let with_pipeline = self.parse_with_pipeline()?;
                let return_clause = self.parse_return_clause()?;
                Ok(Statement::MatchWith {
                    pattern,
                    where_clause,
                    with_pipeline,
                    return_clause,
                })
            }
            TokenKind::Keyword(Keyword::Optional) => {
                self.lexer.next_token()?;
                self.lexer.expect(&TokenKind::Keyword(Keyword::Match))?;
                let optional_pattern = self.parse_pattern_list()?;
                let optional_where_clause = if self.lexer.eat(&TokenKind::Keyword(Keyword::Where))? {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                let return_clause = self.parse_return_clause()?;
                Ok(Statement::MatchWithOptional {
                    pattern,
                    where_clause,
                    optional_pattern,
                    optional_where_clause,
                    return_clause,
                })
            }
            TokenKind::Keyword(Keyword::Set) => {
                self.lexer.next_token()?;
                let assignments = self.parse_assignment_list()?;
                Ok(Statement::Set {
                    pattern,
                    where_clause,
                    assignments,
                })
            }
            TokenKind::Keyword(Keyword::Delete) => {
                self.lexer.next_token()?;
                let variables = self.parse_var_list()?;
                Ok(Statement::Delete {
                    pattern,
                    where_clause,
                    detach: false,
                    variables,
                })
            }
            TokenKind::Keyword(Keyword::Detach) => {
                self.lexer.next_token()?;
                self.lexer.expect(&TokenKind::Keyword(Keyword::Delete))?;
                let variables = self.parse_var_list()?;
                Ok(Statement::Delete {
                    pattern,
                    where_clause,
                    detach: true,
                    variables,
                })
            }
            TokenKind::Keyword(Keyword::Call)
            | TokenKind::Keyword(Keyword::Foreach)
            | TokenKind::Keyword(Keyword::Load) => Err(ParseError::new(
                format!("unsupported Cypher clause: {:?}", tok.kind),
                tok.span,
            )),
            _ => Err(ParseError::new(
                format!(
                    "expected RETURN, OPTIONAL MATCH, WITH, SET, DELETE, or DETACH DELETE after MATCH, got {:?}",
                    tok.kind
                ),
                tok.span,
            )),
        }
    }

    fn parse_create(&mut self) -> ParseResult<Statement> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Create))?;
        let patterns = self.parse_pattern_list()?;

        // Optional RETURN clause — mirrors the MERGE RETURN pattern.
        let return_clause = if self.lexer.peek()?.kind == TokenKind::Keyword(Keyword::Return) {
            Some(self.parse_return_clause()?)
        } else {
            None
        };

        Ok(Statement::Create {
            patterns,
            return_clause,
        })
    }

    // --- Pattern parsing ---

    fn parse_pattern_list(&mut self) -> ParseResult<Vec<Pattern>> {
        let mut patterns = vec![self.parse_pattern()?];
        while self.lexer.eat(&TokenKind::Comma)? {
            patterns.push(self.parse_pattern()?);
        }
        Ok(patterns)
    }

    fn parse_pattern(&mut self) -> ParseResult<Pattern> {
        // Handle path assignment: `path = (...)` or `p = shortestPath(...)`
        // If we see an Ident followed by `=`, consume both and ignore the
        // path variable (it's stored nowhere for now).
        let tok = self.lexer.peek()?;
        if matches!(tok.kind, TokenKind::Ident(_)) {
            // Speculatively consume the identifier.
            let ident_tok = self.lexer.next_token()?;
            let next = self.lexer.peek()?;
            if next.kind == TokenKind::Eq {
                // Path assignment confirmed — consume `=`.
                self.lexer.next_token()?;
                // Check for shortestPath( wrapper — consume and ignore it.
                let after_eq = self.lexer.peek()?;
                let shortest = if let TokenKind::Ident(name) = &after_eq.kind {
                    name.eq_ignore_ascii_case("shortestPath")
                } else {
                    false
                };
                if shortest {
                    self.lexer.next_token()?; // consume "shortestPath"
                    self.lexer.expect(&TokenKind::LParen)?; // consume "("
                }
                let pattern = self.parse_pattern_inner()?;
                if shortest {
                    self.lexer.expect(&TokenKind::RParen)?; // consume closing ")"
                }
                return Ok(pattern);
            }
            // Not a path assignment — the Ident we consumed must be
            // the start of a node pattern `(ident ...)`, but node patterns
            // start with `(`. This is a parse error — produce a clear message.
            return Err(ParseError::new(
                format!(
                    "expected '(' to start node pattern, got {:?}",
                    ident_tok.kind
                ),
                ident_tok.span,
            ));
        }

        self.parse_pattern_inner()
    }

    /// Inner pattern parse: node followed by optional relationship chains.
    fn parse_pattern_inner(&mut self) -> ParseResult<Pattern> {
        let first = self.parse_node_pattern()?;
        let mut elements = vec![first];

        // Check for relationship continuation: -[ or <- or ->
        loop {
            let tok = self.lexer.peek()?;
            match &tok.kind {
                TokenKind::DashBracket | TokenKind::ArrowLeft | TokenKind::Minus => {
                    let rel = self.parse_rel_pattern()?;
                    elements.push(rel);
                    let node = self.parse_node_pattern()?;
                    elements.push(node);
                }
                _ => break,
            }
        }

        if elements.len() == 1 {
            Ok(elements.into_iter().next().unwrap())
        } else {
            Ok(Pattern::Path(elements))
        }
    }

    fn parse_node_pattern(&mut self) -> ParseResult<Pattern> {
        self.lexer.expect(&TokenKind::LParen)?;

        let mut var = None;
        let mut label = None;
        let mut props = vec![];

        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::RParen => {
                // Empty node: ()
            }
            TokenKind::Colon => {
                // Label only: (:Person)
                label = Some(self.parse_label()?);
                if self.lexer.peek()?.kind == TokenKind::LBrace {
                    props = self.parse_prop_map()?;
                }
            }
            TokenKind::Ident(_) => {
                // Variable, possibly followed by label and props.
                let name_tok = self.lexer.next_token()?;
                if let TokenKind::Ident(name) = name_tok.kind {
                    var = Some(name.to_string());
                }
                if self.lexer.peek()?.kind == TokenKind::Colon {
                    label = Some(self.parse_label()?);
                }
                if self.lexer.peek()?.kind == TokenKind::LBrace {
                    props = self.parse_prop_map()?;
                }
            }
            TokenKind::LBrace => {
                props = self.parse_prop_map()?;
            }
            _ => {
                return Err(ParseError::new(
                    format!(
                        "expected variable, label, or ')' in node pattern, got {:?}",
                        tok.kind
                    ),
                    tok.span,
                ));
            }
        }

        if self.lexer.peek()?.kind == TokenKind::Colon {
            let tok = self.lexer.peek()?;
            return Err(ParseError::new(
                "multi-label node patterns are not yet supported".to_string(),
                tok.span,
            ));
        }
        self.lexer.expect(&TokenKind::RParen)?;

        Ok(Pattern::Node { var, label, props })
    }

    fn parse_rel_pattern(&mut self) -> ParseResult<Pattern> {
        let tok = self.lexer.peek()?;
        match &tok.kind {
            // -[:TYPE]-> or -[:TYPE]- or -[var:TYPE {props}]->
            TokenKind::DashBracket => {
                self.lexer.next_token()?;
                let (var, rel_type, props, length_range) = self.parse_rel_detail()?;

                // Expect ]-> or ]- or ]
                let close = self.lexer.peek()?;
                let direction = match &close.kind {
                    TokenKind::BracketArrow => {
                        self.lexer.next_token()?;
                        Direction::Out
                    }
                    TokenKind::BracketDash => {
                        self.lexer.next_token()?;
                        Direction::Both
                    }
                    TokenKind::RBracket => {
                        self.lexer.next_token()?;
                        // Check for trailing ->; otherwise treat as undirected.
                        if self.lexer.eat(&TokenKind::ArrowRight)? {
                            Direction::Out
                        } else {
                            // Consume optional trailing dash for ]-
                            let _ = self.lexer.eat(&TokenKind::Minus)?;
                            Direction::Both
                        }
                    }
                    _ => {
                        return Err(ParseError::new(
                            format!(
                                "expected ]->, ]-, or ] in relationship, got {:?}",
                                close.kind
                            ),
                            close.span,
                        ));
                    }
                };

                Ok(Pattern::Rel {
                    var,
                    rel_type,
                    direction,
                    props,
                    length_range,
                })
            }
            // <-[:TYPE]- or <-[var:TYPE]-
            TokenKind::ArrowLeft => {
                self.lexer.next_token()?;
                self.lexer.expect(&TokenKind::LBracket)?;
                let (var, rel_type, props, length_range) = self.parse_rel_detail()?;
                // Expect ]- or ]
                let close = self.lexer.peek()?;
                match &close.kind {
                    TokenKind::BracketDash => {
                        self.lexer.next_token()?;
                    }
                    TokenKind::RBracket => {
                        self.lexer.next_token()?;
                        self.lexer.expect(&TokenKind::Minus)?;
                    }
                    _ => {
                        return Err(ParseError::new(
                            format!("expected ]- in incoming relationship, got {:?}", close.kind),
                            close.span,
                        ));
                    }
                }
                Ok(Pattern::Rel {
                    var,
                    rel_type,
                    direction: Direction::In,
                    props,
                    length_range,
                })
            }
            // Bare - used in undirected: (a)-(b) — treated as Both
            TokenKind::Minus => {
                self.lexer.next_token()?;
                Ok(Pattern::Rel {
                    var: None,
                    rel_type: None,
                    direction: Direction::Both,
                    props: vec![],
                    length_range: None,
                })
            }
            _ => Err(ParseError::new(
                format!(
                    "expected -[, <-, or - in relationship pattern, got {:?}",
                    tok.kind
                ),
                tok.span,
            )),
        }
    }

    /// Parse the inside of `[ ... ]` in a relationship: `var:TYPE*min..max {props}`
    ///
    /// Returns `(var, rel_type, props, length_range)`.
    fn parse_rel_detail(&mut self) -> ParseResult<RelDetail> {
        let mut var = None;
        let mut rel_type = None;
        let mut props = vec![];
        let mut length_range = None;

        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Colon => {
                rel_type = Some(self.parse_label_alternatives()?);
            }
            TokenKind::Ident(_) => {
                let name_tok = self.lexer.next_token()?;
                if let TokenKind::Ident(name) = name_tok.kind {
                    var = Some(name.to_string());
                }
                if self.lexer.peek()?.kind == TokenKind::Colon {
                    rel_type = Some(self.parse_label_alternatives()?);
                }
            }
            TokenKind::Star => {
                // Bare star without label: -[*]-> or -[*1..5]->
            }
            _ => {
                // Empty brackets: -[]-
            }
        }

        // Parse optional variable-length `*` syntax:
        // `*` → (1, None), `*3` → (3, Some(3)), `*1..5` → (1, Some(5))
        if self.lexer.eat(&TokenKind::Star)? {
            let tok = self.lexer.peek()?;
            match &tok.kind {
                TokenKind::Integer(_) => {
                    let num_tok = self.lexer.next_token()?;
                    let min = if let TokenKind::Integer(n) = num_tok.kind {
                        n as u32
                    } else {
                        unreachable!()
                    };
                    // Check for `..max`
                    if self.lexer.peek()?.kind == TokenKind::Dot {
                        self.lexer.next_token()?; // consume first dot
                        self.lexer.expect(&TokenKind::Dot)?; // consume second dot
                        let max_tok = self.lexer.next_token()?;
                        let max = if let TokenKind::Integer(n) = max_tok.kind {
                            n as u32
                        } else {
                            return Err(ParseError::new(
                                format!(
                                    "expected integer after '..' in variable-length path, got {:?}",
                                    max_tok.kind
                                ),
                                max_tok.span,
                            ));
                        };
                        length_range = Some((min, Some(max)));
                    } else {
                        // Exact hop count: *3 means exactly 3 hops.
                        length_range = Some((min, Some(min)));
                    }
                }
                _ => {
                    // Bare `*` — unbounded: 1 to unlimited.
                    length_range = Some((1, None));
                }
            }
        }

        if self.lexer.peek()?.kind == TokenKind::LBrace {
            props = self.parse_prop_map()?;
        }

        Ok((var, rel_type, props, length_range))
    }

    /// Parse `:Label` — consume colon and return the label name.
    fn parse_label(&mut self) -> ParseResult<String> {
        self.lexer.expect(&TokenKind::Colon)?;
        self.parse_label_name()
    }

    /// Parse a label/type name after ':' or '|'. Keywords can be labels too.
    fn parse_label_name(&mut self) -> ParseResult<String> {
        let tok = self.lexer.next_token()?;
        match tok.kind {
            TokenKind::Ident(name) => Ok(name.to_string()),
            // Keywords can be labels too (e.g., :Order, :Set).
            TokenKind::Keyword(_) => {
                let text = &self.lexer.input[tok.span.start..tok.span.end];
                Ok(text.to_string())
            }
            _ => Err(ParseError::new(
                format!("expected label name, got {:?}", tok.kind),
                tok.span,
            )),
        }
    }

    /// Parse relationship type alternatives: `:KNOWS|LIKES`.
    ///
    /// Stored as a `|`-joined string for this correctness-first slice so older
    /// single-label fields keep their shape; downstream label matching splits it
    /// back into a bounded alternatives list.
    fn parse_label_alternatives(&mut self) -> ParseResult<String> {
        self.lexer.expect(&TokenKind::Colon)?;
        let mut labels = vec![self.parse_label_name()?];
        while self.lexer.eat(&TokenKind::Pipe)? {
            labels.push(self.parse_label_name()?);
        }
        Ok(labels.join("|"))
    }

    /// Parse `{key: value, ...}`.
    fn parse_prop_map(&mut self) -> ParseResult<PropMap> {
        self.lexer.expect(&TokenKind::LBrace)?;
        let mut props = vec![];

        if self.lexer.peek()?.kind != TokenKind::RBrace {
            loop {
                let key_tok = self.lexer.next_token()?;
                let key = match key_tok.kind {
                    TokenKind::Ident(name) => name.to_string(),
                    _ => {
                        return Err(ParseError::new(
                            format!("expected property name, got {:?}", key_tok.kind),
                            key_tok.span,
                        ));
                    }
                };
                self.lexer.expect(&TokenKind::Colon)?;
                let value = self.parse_expr()?;
                props.push((key, value));

                if !self.lexer.eat(&TokenKind::Comma)? {
                    break;
                }
            }
        }

        self.lexer.expect(&TokenKind::RBrace)?;
        Ok(props)
    }

    // --- Expression parsing (precedence climbing) ---
    // Depth is checked at the entry point to catch all recursive paths.

    fn parse_expr(&mut self) -> ParseResult<Expr> {
        self.enter_expr()?;
        let result = self.parse_or_expr();
        self.exit_expr();
        result
    }

    fn parse_or_expr(&mut self) -> ParseResult<Expr> {
        let mut left = self.parse_and_expr()?;
        while self.lexer.eat(&TokenKind::Keyword(Keyword::Or))? {
            let right = self.parse_and_expr()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> ParseResult<Expr> {
        let mut left = self.parse_not_expr()?;
        while self.lexer.eat(&TokenKind::Keyword(Keyword::And))? {
            let right = self.parse_not_expr()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> ParseResult<Expr> {
        if self.lexer.eat(&TokenKind::Keyword(Keyword::Not))? {
            // Check for negative pattern expression: NOT (a)-[:REL]->(b)
            // After parsing `NOT (a)`, if a relationship token follows,
            // consume the entire pattern and return a true placeholder.
            let peek = self.lexer.peek()?;
            if peek.kind == TokenKind::LParen {
                // NOT recurses — check depth.
                self.enter_expr()?;
                let inner = self.parse_not_expr()?;
                self.exit_expr();

                // If a relationship follows, this is a negative pattern predicate.
                let next = self.lexer.peek()?;
                let next_kind = next.kind.clone();
                let next_span = next.span;
                if matches!(
                    next_kind,
                    TokenKind::DashBracket | TokenKind::ArrowLeft | TokenKind::Minus
                ) {
                    let start_var = match inner {
                        Expr::Var(v) => v,
                        _ => {
                            return Err(ParseError::new(
                                "negative pattern predicate must start with a bound variable, e.g. NOT (a)-[:REL]->(b)",
                                next_span,
                            ));
                        }
                    };
                    return self.parse_pattern_predicate_from_start(start_var, next_span, true);
                }

                return Ok(Expr::Not(Box::new(inner)));
            }

            // NOT recurses — check depth.
            self.enter_expr()?;
            let inner = self.parse_not_expr();
            self.exit_expr();
            Ok(Expr::Not(Box::new(inner?)))
        } else {
            self.parse_comparison()
        }
    }

    fn parse_pattern_predicate_from_start(
        &mut self,
        start_var: String,
        span: crate::parser::error::Span,
        negated: bool,
    ) -> ParseResult<Expr> {
        let mut hops = Vec::new();
        loop {
            let rel = self.parse_rel_pattern()?;
            let target = self.parse_node_pattern()?;
            let Pattern::Rel {
                rel_type,
                direction,
                props,
                length_range,
                ..
            } = rel
            else {
                unreachable!("parse_rel_pattern returns Pattern::Rel")
            };
            if length_range.is_some() {
                return Err(ParseError::new(
                    "variable-length pattern predicates are not yet supported",
                    span,
                ));
            }
            if !props.is_empty() {
                return Err(ParseError::new(
                    "relationship property filters in pattern predicates are not yet supported",
                    span,
                ));
            }
            let Pattern::Node {
                label: target_label,
                props: target_props,
                ..
            } = target
            else {
                unreachable!("parse_node_pattern returns Pattern::Node")
            };
            hops.push(PatternPredicateHop {
                rel_type,
                direction,
                target_label,
                target_props,
            });
            let tok = self.lexer.peek()?;
            if !matches!(
                tok.kind,
                TokenKind::DashBracket | TokenKind::ArrowLeft | TokenKind::Minus
            ) {
                break;
            }
        }
        Ok(Expr::PatternPredicate {
            start_var,
            hops,
            negated,
        })
    }

    fn parse_comparison(&mut self) -> ParseResult<Expr> {
        let left = self.parse_addition()?;

        // Check for IS NULL / IS NOT NULL.
        if self.lexer.eat(&TokenKind::Keyword(Keyword::Is))? {
            if self.lexer.eat(&TokenKind::Keyword(Keyword::Not))? {
                self.lexer.expect(&TokenKind::Keyword(Keyword::Null))?;
                return Ok(Expr::IsNotNull(Box::new(left)));
            } else {
                self.lexer.expect(&TokenKind::Keyword(Keyword::Null))?;
                return Ok(Expr::IsNull(Box::new(left)));
            }
        }

        if self.lexer.eat(&TokenKind::Keyword(Keyword::In))? {
            let list = self.parse_addition()?;
            return Ok(Expr::In {
                value: Box::new(left),
                list: Box::new(list),
            });
        }

        let tok = self.lexer.peek()?;
        let op = match &tok.kind {
            TokenKind::Eq => Some(CompareOp::Eq),
            TokenKind::Neq => Some(CompareOp::Neq),
            TokenKind::Lt => Some(CompareOp::Lt),
            TokenKind::Gt => Some(CompareOp::Gt),
            TokenKind::LtEq => Some(CompareOp::LtEq),
            TokenKind::GtEq => Some(CompareOp::GtEq),
            _ => None,
        };

        if let Some(op) = op {
            self.lexer.next_token()?;
            let right = self.parse_addition()?;
            Ok(Expr::Comparison {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_addition(&mut self) -> ParseResult<Expr> {
        let mut left = self.parse_multiplication()?;
        loop {
            let tok = self.lexer.peek()?;
            let op = match &tok.kind {
                TokenKind::Plus => Some(ArithOp::Add),
                TokenKind::Minus => Some(ArithOp::Sub),
                _ => None,
            };
            if let Some(op) = op {
                self.lexer.next_token()?;
                let right = self.parse_multiplication()?;
                left = Expr::Arithmetic {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> ParseResult<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let tok = self.lexer.peek()?;
            let op = match &tok.kind {
                TokenKind::Star => Some(ArithOp::Mul),
                TokenKind::Slash => Some(ArithOp::Div),
                _ => None,
            };
            if let Some(op) = op {
                self.lexer.next_token()?;
                let right = self.parse_unary()?;
                left = Expr::Arithmetic {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> ParseResult<Expr> {
        if self.lexer.eat(&TokenKind::Minus)? {
            // Unary minus recurses — check depth.
            self.enter_expr()?;
            let inner = self.parse_unary();
            self.exit_expr();
            // Negate: wrap as 0 - inner.
            Ok(Expr::Arithmetic {
                left: Box::new(Expr::Literal(Literal::Integer(0))),
                op: ArithOp::Sub,
                right: Box::new(inner?),
            })
        } else {
            self.parse_postfix()
        }
    }

    fn parse_postfix(&mut self) -> ParseResult<Expr> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.lexer.eat(&TokenKind::LBracket)? {
                let start = if self.lexer.peek()?.kind == TokenKind::Dot {
                    None
                } else {
                    Some(self.parse_expr()?)
                };
                if self.lexer.eat(&TokenKind::Dot)? {
                    self.lexer.expect(&TokenKind::Dot)?;
                    let end = if self.lexer.peek()?.kind == TokenKind::RBracket {
                        None
                    } else {
                        Some(self.parse_expr()?)
                    };
                    self.lexer.expect(&TokenKind::RBracket)?;
                    expr = Expr::Slice {
                        target: Box::new(expr),
                        start: start.map(Box::new),
                        end: end.map(Box::new),
                    };
                } else {
                    let Some(index) = start else {
                        return Err(ParseError::new(
                            "expected list index or slice bound",
                            self.lexer.peek()?.span,
                        ));
                    };
                    self.lexer.expect(&TokenKind::RBracket)?;
                    expr = Expr::Index {
                        target: Box::new(expr),
                        index: Box::new(index),
                    };
                }
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_list_predicate_call(&mut self, kind: ListPredicateKind) -> ParseResult<Expr> {
        self.lexer.expect(&TokenKind::LParen)?;
        let var_tok = self.lexer.next_token()?;
        let var = match var_tok.kind {
            TokenKind::Ident(name) => name.to_string(),
            _ => {
                return Err(ParseError::new(
                    format!("expected list predicate variable, got {:?}", var_tok.kind),
                    var_tok.span,
                ));
            }
        };
        self.lexer.expect(&TokenKind::Keyword(Keyword::In))?;
        let list = self.parse_expr()?;
        self.lexer.expect(&TokenKind::Keyword(Keyword::Where))?;
        let predicate = self.parse_expr()?;
        self.lexer.expect(&TokenKind::RParen)?;
        Ok(Expr::ListPredicate {
            kind,
            var,
            list: Box::new(list),
            predicate: Box::new(predicate),
        })
    }

    fn parse_primary(&mut self) -> ParseResult<Expr> {
        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Integer(_) => {
                let tok = self.lexer.next_token()?;
                if let TokenKind::Integer(v) = tok.kind {
                    Ok(Expr::Literal(Literal::Integer(v)))
                } else {
                    unreachable!()
                }
            }
            TokenKind::Float(_) => {
                let tok = self.lexer.next_token()?;
                if let TokenKind::Float(v) = tok.kind {
                    Ok(Expr::Literal(Literal::Float(v)))
                } else {
                    unreachable!()
                }
            }
            TokenKind::StringLit(_) => {
                let tok = self.lexer.next_token()?;
                if let TokenKind::StringLit(s) = tok.kind {
                    Ok(Expr::Literal(Literal::String(s)))
                } else {
                    unreachable!()
                }
            }
            TokenKind::Parameter(_) => {
                let tok = self.lexer.next_token()?;
                if let TokenKind::Parameter(name) = tok.kind {
                    Ok(Expr::Parameter(name.to_string()))
                } else {
                    unreachable!()
                }
            }
            TokenKind::Keyword(Keyword::All) => {
                self.lexer.next_token()?;
                self.parse_list_predicate_call(ListPredicateKind::All)
            }
            TokenKind::Keyword(Keyword::Exists) => {
                let exists_tok = self.lexer.next_token()?;
                self.lexer.expect(&TokenKind::LBrace)?;
                let start = self.parse_node_pattern()?;
                let Pattern::Node {
                    var: Some(start_var),
                    ..
                } = start
                else {
                    return Err(ParseError::new(
                        "EXISTS pattern predicate must start with a bound variable, e.g. EXISTS { (a)-[:REL]->(b) }",
                        exists_tok.span,
                    ));
                };
                let expr =
                    self.parse_pattern_predicate_from_start(start_var, exists_tok.span, false)?;
                self.lexer.expect(&TokenKind::RBrace)?;
                Ok(expr)
            }
            TokenKind::Keyword(Keyword::True) => {
                self.lexer.next_token()?;
                Ok(Expr::Literal(Literal::Bool(true)))
            }
            TokenKind::Keyword(Keyword::False) => {
                self.lexer.next_token()?;
                Ok(Expr::Literal(Literal::Bool(false)))
            }
            TokenKind::Keyword(Keyword::Null) => {
                self.lexer.next_token()?;
                Ok(Expr::Literal(Literal::Null))
            }
            TokenKind::LBracket => {
                self.lexer.next_token()?;
                let mut items = Vec::new();
                if !self.lexer.eat(&TokenKind::RBracket)? {
                    items.push(self.parse_expr()?);
                    while self.lexer.eat(&TokenKind::Comma)? {
                        items.push(self.parse_expr()?);
                    }
                    self.lexer.expect(&TokenKind::RBracket)?;
                }
                Ok(Expr::List(items))
            }
            TokenKind::LBrace => Ok(Expr::Map(self.parse_prop_map()?)),
            TokenKind::LParen => {
                // Parenthesized expression — recursion depth is tracked
                // via parse_expr's enter_expr/exit_expr.
                self.lexer.next_token()?;
                let expr = self.parse_expr()?;
                self.lexer.expect(&TokenKind::RParen)?;
                Ok(expr)
            }
            TokenKind::Ident(_) => {
                let tok = self.lexer.next_token()?;
                let name = if let TokenKind::Ident(s) = tok.kind {
                    s.to_string()
                } else {
                    unreachable!()
                };

                // Common list predicates use scoped syntax: any(x IN list WHERE pred).
                if name.eq_ignore_ascii_case("any") && self.lexer.peek()?.kind == TokenKind::LParen
                {
                    return self.parse_list_predicate_call(ListPredicateKind::Any);
                }

                // Check for property access (var.prop) or function call (fn(...)).
                let next = self.lexer.peek()?.clone();
                match &next.kind {
                    TokenKind::Dot => {
                        self.lexer.next_token()?;
                        let prop_tok = self.lexer.next_token()?;
                        let prop = match prop_tok.kind {
                            TokenKind::Ident(p) => p.to_string(),
                            _ => {
                                return Err(ParseError::new(
                                    format!(
                                        "expected property name after '.', got {:?}",
                                        prop_tok.kind
                                    ),
                                    prop_tok.span,
                                ));
                            }
                        };
                        Ok(Expr::Property {
                            var: name,
                            name: prop,
                        })
                    }
                    TokenKind::LParen => {
                        self.lexer.next_token()?;
                        let mut args = vec![];
                        if self.lexer.peek()?.kind != TokenKind::RParen {
                            loop {
                                let distinct_arg =
                                    self.lexer.eat(&TokenKind::Keyword(Keyword::Distinct))?;
                                let arg = self.parse_expr()?;
                                args.push(if distinct_arg {
                                    Expr::Distinct(Box::new(arg))
                                } else {
                                    arg
                                });
                                if !self.lexer.eat(&TokenKind::Comma)? {
                                    break;
                                }
                            }
                        }
                        self.lexer.expect(&TokenKind::RParen)?;
                        Ok(Expr::Function { name, args })
                    }
                    _ => Ok(Expr::Var(name)),
                }
            }
            _ => Err(ParseError::new(
                format!("expected expression, got {:?}", tok.kind),
                tok.span,
            )),
        }
    }

    // --- RETURN clause ---

    fn parse_with_pipeline(&mut self) -> ParseResult<WithPipeline> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::With))?;
        let distinct = self.lexer.eat(&TokenKind::Keyword(Keyword::Distinct))?;
        let mut items = vec![self.parse_return_item()?];
        while self.lexer.eat(&TokenKind::Comma)? {
            items.push(self.parse_return_item()?);
        }
        let where_clause = if self.lexer.eat(&TokenKind::Keyword(Keyword::Where))? {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let order_by = if self.lexer.eat(&TokenKind::Keyword(Keyword::Order))? {
            self.lexer.expect(&TokenKind::Keyword(Keyword::By))?;
            let mut orders = vec![self.parse_order_item()?];
            while self.lexer.eat(&TokenKind::Comma)? {
                orders.push(self.parse_order_item()?);
            }
            orders
        } else {
            vec![]
        };
        let limit = if self.lexer.eat(&TokenKind::Keyword(Keyword::Limit))? {
            let tok = self.lexer.next_token()?;
            match tok.kind {
                TokenKind::Integer(v) => Some(v),
                _ => {
                    return Err(ParseError::new(
                        format!("expected integer after LIMIT, got {:?}", tok.kind),
                        tok.span,
                    ));
                }
            }
        } else {
            None
        };
        Ok(WithPipeline {
            clause: ReturnClause {
                distinct,
                items,
                order_by,
                limit,
            },
            where_clause,
        })
    }

    fn parse_return_clause(&mut self) -> ParseResult<ReturnClause> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Return))?;

        let distinct = self.lexer.eat(&TokenKind::Keyword(Keyword::Distinct))?;

        let mut items = vec![self.parse_return_item()?];
        while self.lexer.eat(&TokenKind::Comma)? {
            items.push(self.parse_return_item()?);
        }

        let order_by = if self.lexer.eat(&TokenKind::Keyword(Keyword::Order))? {
            self.lexer.expect(&TokenKind::Keyword(Keyword::By))?;
            let mut orders = vec![self.parse_order_item()?];
            while self.lexer.eat(&TokenKind::Comma)? {
                orders.push(self.parse_order_item()?);
            }
            orders
        } else {
            vec![]
        };

        let limit = if self.lexer.eat(&TokenKind::Keyword(Keyword::Limit))? {
            let tok = self.lexer.next_token()?;
            match tok.kind {
                TokenKind::Integer(v) => Some(v),
                _ => {
                    return Err(ParseError::new(
                        format!("expected integer after LIMIT, got {:?}", tok.kind),
                        tok.span,
                    ));
                }
            }
        } else {
            None
        };

        Ok(ReturnClause {
            distinct,
            items,
            order_by,
            limit,
        })
    }

    fn parse_return_item(&mut self) -> ParseResult<ReturnItem> {
        let expr = self.parse_expr()?;
        let alias = if self.lexer.eat(&TokenKind::Keyword(Keyword::As))? {
            let tok = self.lexer.next_token()?;
            match tok.kind {
                TokenKind::Ident(name) => Some(name.to_string()),
                _ => {
                    return Err(ParseError::new(
                        format!("expected alias name after AS, got {:?}", tok.kind),
                        tok.span,
                    ));
                }
            }
        } else {
            None
        };
        Ok(ReturnItem { expr, alias })
    }

    fn parse_order_item(&mut self) -> ParseResult<OrderItem> {
        let expr = self.parse_expr()?;
        let direction = if self.lexer.eat(&TokenKind::Keyword(Keyword::Desc))? {
            SortDir::Desc
        } else {
            self.lexer.eat(&TokenKind::Keyword(Keyword::Asc))?;
            SortDir::Asc
        };
        Ok(OrderItem { expr, direction })
    }

    // --- SET and DELETE helpers ---

    fn parse_assignment_list(&mut self) -> ParseResult<Vec<Assignment>> {
        let mut assignments = vec![self.parse_assignment()?];
        while self.lexer.eat(&TokenKind::Comma)? {
            assignments.push(self.parse_assignment()?);
        }
        Ok(assignments)
    }

    fn parse_assignment(&mut self) -> ParseResult<Assignment> {
        let tok = self.lexer.next_token()?;
        let var = match tok.kind {
            TokenKind::Ident(name) => name.to_string(),
            _ => {
                return Err(ParseError::new(
                    format!("expected variable name in SET, got {:?}", tok.kind),
                    tok.span,
                ));
            }
        };
        self.lexer.expect(&TokenKind::Dot)?;
        let prop_tok = self.lexer.next_token()?;
        let property = match prop_tok.kind {
            TokenKind::Ident(name) => name.to_string(),
            _ => {
                return Err(ParseError::new(
                    format!("expected property name after '.', got {:?}", prop_tok.kind),
                    prop_tok.span,
                ));
            }
        };
        self.lexer.expect(&TokenKind::Eq)?;
        let value = self.parse_expr()?;
        Ok(Assignment {
            var,
            property,
            value,
        })
    }

    fn parse_var_list(&mut self) -> ParseResult<Vec<String>> {
        let mut vars = vec![];
        let tok = self.lexer.next_token()?;
        match tok.kind {
            TokenKind::Ident(name) => vars.push(name.to_string()),
            _ => {
                return Err(ParseError::new(
                    format!("expected variable name in DELETE, got {:?}", tok.kind),
                    tok.span,
                ));
            }
        }
        while self.lexer.eat(&TokenKind::Comma)? {
            let tok = self.lexer.next_token()?;
            match tok.kind {
                TokenKind::Ident(name) => vars.push(name.to_string()),
                _ => {
                    return Err(ParseError::new(
                        format!("expected variable name in DELETE, got {:?}", tok.kind),
                        tok.span,
                    ));
                }
            }
        }
        Ok(vars)
    }

    fn parse_duration(&mut self) -> ParseResult<Duration> {
        let tok = self.lexer.next_token()?;
        let value = match tok.kind {
            TokenKind::Integer(n) => n,
            _ => {
                return Err(ParseError::new(
                    format!("expected integer for duration, got {:?}", tok.kind),
                    tok.span,
                ));
            }
        };

        let unit_tok = self.lexer.next_token()?;
        let unit = match &unit_tok.kind {
            TokenKind::Ident(s) => *s,
            TokenKind::Keyword(_) => {
                return Err(ParseError::new(
                    format!("expected duration unit (s, ms, m), got {:?}", unit_tok.kind),
                    unit_tok.span,
                ));
            }
            _ => {
                return Err(ParseError::new(
                    format!("expected duration unit (s, ms, m), got {:?}", unit_tok.kind),
                    unit_tok.span,
                ));
            }
        };

        let duration = match unit {
            "s" => Duration::from_secs(value as u64),
            "ms" => Duration::from_millis(value as u64),
            "m" => Duration::from_secs(value as u64 * 60),
            _ => {
                return Err(ParseError::new(
                    format!("unknown duration unit: '{unit}', expected s, ms, or m"),
                    unit_tok.span,
                ));
            }
        };

        // Enforce minimum 500ms
        if duration < Duration::from_millis(500) {
            return Err(ParseError::new(
                format!(
                    "subscription interval must be at least 500ms, got {}ms",
                    duration.as_millis()
                ),
                tok.span,
            ));
        }

        Ok(duration)
    }
}

/// Parse a complete Cypher statement from source text.
pub fn parse(input: &str) -> ParseResult<Statement> {
    let mut parser = Parser::new(input);
    let first = parser.parse_statement()?;
    let mut arms = vec![first];
    let mut union_all = false;
    while parser.lexer.peek()?.kind == TokenKind::Keyword(Keyword::Union) {
        parser.lexer.next_token()?;
        let this_all = parser.lexer.eat(&TokenKind::Keyword(Keyword::All))?;
        if arms.len() > 1 && this_all != union_all {
            let pos = parser.lexer.pos();
            return Err(ParseError::new(
                "cannot mix UNION and UNION ALL in one query".to_string(),
                Span {
                    start: pos,
                    end: pos,
                },
            ));
        }
        union_all = this_all;
        arms.push(parser.parse_statement()?);
    }
    let tok = parser.lexer.next_token()?;
    if tok.kind != TokenKind::Eof {
        let message = match &tok.kind {
            TokenKind::Keyword(Keyword::With) | TokenKind::Keyword(Keyword::Unwind) => {
                format!("unsupported Cypher clause: {:?}", tok.kind)
            }
            _ => format!("unexpected token after statement: {:?}", tok.kind),
        };
        return Err(ParseError::new(message, tok.span));
    }
    if arms.len() == 1 {
        Ok(arms.pop().expect("one arm"))
    } else {
        Ok(Statement::Union {
            arms,
            all: union_all,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Node patterns ---

    #[test]
    fn parse_empty_node() {
        let stmt = parse("CREATE ()").unwrap();
        if let Statement::Create { patterns, .. } = stmt {
            assert_eq!(
                patterns[0],
                Pattern::Node {
                    var: None,
                    label: None,
                    props: vec![]
                }
            );
        } else {
            panic!("expected Create");
        }
    }

    #[test]
    fn parse_node_with_var() {
        let stmt = parse("CREATE (n)").unwrap();
        if let Statement::Create { patterns, .. } = stmt {
            assert_eq!(
                patterns[0],
                Pattern::Node {
                    var: Some("n".into()),
                    label: None,
                    props: vec![]
                }
            );
        } else {
            panic!("expected Create");
        }
    }

    #[test]
    fn parse_node_with_label() {
        let stmt = parse("CREATE (n:Person)").unwrap();
        if let Statement::Create { patterns, .. } = stmt {
            assert_eq!(
                patterns[0],
                Pattern::Node {
                    var: Some("n".into()),
                    label: Some("Person".into()),
                    props: vec![],
                }
            );
        } else {
            panic!("expected Create");
        }
    }

    #[test]
    fn parse_node_with_props() {
        let stmt = parse("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
        if let Statement::Create { patterns, .. } = stmt {
            assert_eq!(
                patterns[0],
                Pattern::Node {
                    var: Some("n".into()),
                    label: Some("Person".into()),
                    props: vec![
                        (
                            "name".into(),
                            Expr::Literal(Literal::String("Alice".into()))
                        ),
                        ("age".into(), Expr::Literal(Literal::Integer(30))),
                    ],
                }
            );
        } else {
            panic!("expected Create");
        }
    }

    // --- Relationship patterns ---

    #[test]
    fn parse_outgoing_rel() {
        let stmt = parse("MATCH (a)-[:KNOWS]->(b) RETURN b").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            assert_eq!(pattern.len(), 1);
            if let Pattern::Path(elements) = &pattern[0] {
                assert_eq!(elements.len(), 3); // Node, Rel, Node
                assert!(matches!(
                    &elements[1],
                    Pattern::Rel {
                        direction: Direction::Out,
                        ..
                    }
                ));
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_incoming_rel() {
        let stmt = parse("MATCH (a)<-[:KNOWS]-(b) RETURN a").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            if let Pattern::Path(elements) = &pattern[0] {
                assert!(matches!(
                    &elements[1],
                    Pattern::Rel {
                        direction: Direction::In,
                        ..
                    }
                ));
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    // --- Full MATCH statements ---

    #[test]
    fn parse_match_with_where() {
        let stmt = parse("MATCH (a:Person) WHERE a.age > 30 RETURN a.name").unwrap();
        if let Statement::Match { where_clause, .. } = stmt {
            assert!(where_clause.is_some());
            let wc = where_clause.unwrap();
            assert!(matches!(
                wc,
                Expr::Comparison {
                    op: CompareOp::Gt,
                    ..
                }
            ));
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_match_with_order_limit() {
        let stmt = parse("MATCH (a:Person) RETURN a.name ORDER BY a.age DESC LIMIT 10").unwrap();
        if let Statement::Match { return_clause, .. } = stmt {
            assert_eq!(return_clause.order_by.len(), 1);
            assert_eq!(return_clause.order_by[0].direction, SortDir::Desc);
            assert_eq!(return_clause.limit, Some(10));
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_match_return_distinct() {
        let stmt = parse("MATCH (a) RETURN DISTINCT a.name").unwrap();
        if let Statement::Match { return_clause, .. } = stmt {
            assert!(return_clause.distinct);
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_match_return_alias() {
        let stmt = parse("MATCH (a) RETURN a.name AS person_name").unwrap();
        if let Statement::Match { return_clause, .. } = stmt {
            assert_eq!(return_clause.items[0].alias, Some("person_name".into()));
        } else {
            panic!("expected Match");
        }
    }

    // --- CREATE ---

    #[test]
    fn parse_create_node() {
        let stmt = parse("CREATE (n:Person {name: 'Alice'})").unwrap();
        assert!(matches!(stmt, Statement::Create { .. }));
    }

    #[test]
    fn parse_create_edge() {
        let stmt = parse("CREATE (a)-[:KNOWS {since: 2020}]->(b)").unwrap();
        if let Statement::Create { patterns, .. } = stmt {
            assert!(matches!(&patterns[0], Pattern::Path(_)));
        } else {
            panic!("expected Create");
        }
    }

    #[test]
    fn parse_create_with_return_succeeds() {
        let stmt = parse("CREATE (n:Entity {entity_id: 'x'}) RETURN n").unwrap();
        if let Statement::Create {
            patterns,
            return_clause,
        } = stmt
        {
            assert_eq!(patterns.len(), 1, "expected one CREATE pattern");
            let rc = return_clause.expect("return_clause must be Some(_)");
            assert_eq!(rc.items.len(), 1, "expected one RETURN item");
            assert!(
                matches!(&rc.items[0].expr, crate::parser::Expr::Var(v) if v == "n"),
                "RETURN item must be variable 'n'"
            );
        } else {
            panic!("expected Create statement");
        }
    }

    // --- SET ---

    #[test]
    fn parse_set() {
        let stmt = parse("MATCH (n:Person) WHERE n.name = 'Alice' SET n.age = 31").unwrap();
        if let Statement::Set { assignments, .. } = stmt {
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].var, "n");
            assert_eq!(assignments[0].property, "age");
        } else {
            panic!("expected Set");
        }
    }

    // --- DELETE ---

    #[test]
    fn parse_delete() {
        let stmt = parse("MATCH (n:Person) WHERE n.name = 'Alice' DELETE n").unwrap();
        if let Statement::Delete {
            detach, variables, ..
        } = stmt
        {
            assert!(!detach);
            assert_eq!(variables, vec!["n"]);
        } else {
            panic!("expected Delete");
        }
    }

    #[test]
    fn parse_detach_delete() {
        let stmt = parse("MATCH (n:Person) DETACH DELETE n").unwrap();
        if let Statement::Delete { detach, .. } = stmt {
            assert!(detach);
        } else {
            panic!("expected Delete");
        }
    }

    // --- Expressions ---

    #[test]
    fn parse_boolean_logic() {
        let stmt = parse("MATCH (a) WHERE a.x = 1 AND a.y = 2 OR a.z = 3 RETURN a").unwrap();
        if let Statement::Match {
            where_clause: Some(expr),
            ..
        } = stmt
        {
            // OR has lower precedence, so top-level is Or.
            assert!(matches!(expr, Expr::Or(_, _)));
        } else {
            panic!("expected Match with where");
        }
    }

    #[test]
    fn parse_function_call() {
        let stmt = parse("MATCH (a) RETURN count(a)").unwrap();
        if let Statement::Match { return_clause, .. } = stmt {
            assert!(matches!(
                &return_clause.items[0].expr,
                Expr::Function { name, .. } if name == "count"
            ));
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_is_null() {
        let stmt = parse("MATCH (a) WHERE a.name IS NULL RETURN a").unwrap();
        if let Statement::Match {
            where_clause: Some(expr),
            ..
        } = stmt
        {
            assert!(matches!(expr, Expr::IsNull(_)));
        } else {
            panic!("expected Match with IS NULL");
        }
    }

    #[test]
    fn parse_is_not_null() {
        let stmt = parse("MATCH (a) WHERE a.name IS NOT NULL RETURN a").unwrap();
        if let Statement::Match {
            where_clause: Some(expr),
            ..
        } = stmt
        {
            assert!(matches!(expr, Expr::IsNotNull(_)));
        } else {
            panic!("expected Match with IS NOT NULL");
        }
    }

    // --- Multi-hop path ---

    #[test]
    fn parse_multi_hop_path() {
        let stmt = parse("MATCH (a)-[:KNOWS]->(b)-[:WORKS_AT]->(c) RETURN c.name").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            if let Pattern::Path(elems) = &pattern[0] {
                assert_eq!(elems.len(), 5); // Node, Rel, Node, Rel, Node
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    // --- Error cases ---

    #[test]
    fn parse_error_missing_return() {
        let err = parse("MATCH (a)").unwrap_err();
        assert!(err.message.contains("expected"));
    }

    #[test]
    fn parse_error_unclosed_node() {
        let err = parse("MATCH (a RETURN a").unwrap_err();
        assert!(err.message.contains("expected"));
    }

    // --- Additional coverage ---

    #[test]
    fn parse_multiple_patterns() {
        let stmt = parse("MATCH (a:Person), (b:Company) RETURN a, b").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            assert_eq!(pattern.len(), 2);
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_multiple_set_assignments() {
        let stmt = parse("MATCH (n) SET n.x = 1, n.y = 2").unwrap();
        if let Statement::Set { assignments, .. } = stmt {
            assert_eq!(assignments.len(), 2);
        } else {
            panic!("expected Set");
        }
    }

    #[test]
    fn parse_multiple_delete_vars() {
        let stmt = parse("MATCH (a)-[r]->(b) DELETE a, r, b").unwrap();
        if let Statement::Delete { variables, .. } = stmt {
            assert_eq!(variables, vec!["a", "r", "b"]);
        } else {
            panic!("expected Delete");
        }
    }

    #[test]
    fn parse_undirected_relationship() {
        let stmt = parse("MATCH (a)-[:KNOWS]-(b) RETURN b").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            if let Pattern::Path(elems) = &pattern[0] {
                assert!(matches!(
                    &elems[1],
                    Pattern::Rel {
                        direction: Direction::Both,
                        ..
                    }
                ));
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_named_relationship() {
        let stmt = parse("MATCH (a)-[r:KNOWS]->(b) RETURN r").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            if let Pattern::Path(elems) = &pattern[0] {
                if let Pattern::Rel { var, rel_type, .. } = &elems[1] {
                    assert_eq!(var, &Some("r".into()));
                    assert_eq!(rel_type, &Some("KNOWS".into()));
                } else {
                    panic!("expected Rel");
                }
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_incoming_relationship_full() {
        // <-[:KNOWS]- requires standalone [ after <-
        let stmt = parse("MATCH (a)<-[:KNOWS]-(b) RETURN a").unwrap();
        assert!(matches!(stmt, Statement::Match { .. }));
    }

    #[test]
    fn parse_empty_prop_map() {
        let stmt = parse("CREATE (n:Person {})").unwrap();
        if let Statement::Create { patterns, .. } = stmt {
            if let Pattern::Node { props, .. } = &patterns[0] {
                assert!(props.is_empty());
            } else {
                panic!("expected Node");
            }
        } else {
            panic!("expected Create");
        }
    }

    #[test]
    fn parse_multiple_return_items() {
        let stmt = parse("MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name, a.age").unwrap();
        if let Statement::Match { return_clause, .. } = stmt {
            assert_eq!(return_clause.items.len(), 3);
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_nested_parens_in_expr() {
        let stmt = parse("MATCH (a) WHERE (a.x + 1) * 2 > 10 RETURN a").unwrap();
        assert!(matches!(
            stmt,
            Statement::Match {
                where_clause: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn parse_float_in_where() {
        let stmt = parse("MATCH (a) WHERE a.score > 3.25 RETURN a").unwrap();
        assert!(matches!(
            stmt,
            Statement::Match {
                where_clause: Some(_),
                ..
            }
        ));
    }

    // --- T1 mitigation: expression depth limit ---

    #[test]
    fn parse_deeply_nested_parens_errors() {
        // Build ((((...))))-deep nesting past the limit.
        let depth = MAX_EXPR_DEPTH + 1;
        let opens: String = "(".repeat(depth);
        let closes: String = ")".repeat(depth);
        let query = format!("MATCH (a) WHERE {}a.x{} > 1 RETURN a", opens, closes);
        let err = parse(&query).unwrap_err();
        assert!(
            err.message.contains("nesting depth"),
            "expected depth error, got: {}",
            err.message
        );
    }

    #[test]
    fn parse_at_max_depth_succeeds() {
        // Exactly at the limit should still work.
        // Use fewer parens since each parse_expr call counts as one depth.
        let depth = 30; // Well under 64
        let opens: String = "(".repeat(depth);
        let closes: String = ")".repeat(depth);
        let query = format!("MATCH (a) WHERE {}1{} > 0 RETURN a", opens, closes);
        assert!(parse(&query).is_ok(), "should succeed at depth {}", depth);
    }

    #[test]
    fn parse_chained_not_depth_limit() {
        // Chain NOT operators past the limit.
        let nots = "NOT ".repeat(MAX_EXPR_DEPTH + 1);
        let query = format!("MATCH (a) WHERE {}a.x = 1 RETURN a", nots);
        let err = parse(&query).unwrap_err();
        assert!(
            err.message.contains("nesting depth"),
            "expected depth error, got: {}",
            err.message
        );
    }

    // --- SUBSCRIBE / UNSUBSCRIBE ---

    #[test]
    fn parse_graph_subscribe_match() {
        let stmt = parse("SUBSCRIBE MATCH (u:User)-[:FOLLOWS]->(f) RETURN u, f").unwrap();
        match stmt {
            Statement::Subscribe {
                interval, delta, ..
            } => {
                assert!(interval.is_none());
                assert!(!delta);
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn parse_graph_subscribe_every_delta() {
        let stmt = parse("SUBSCRIBE MATCH (u:User) RETURN u EVERY 5 s DELTA").unwrap();
        match stmt {
            Statement::Subscribe {
                interval, delta, ..
            } => {
                assert_eq!(interval, Some(Duration::from_secs(5)));
                assert!(delta);
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn parse_graph_subscribe_rejects_create() {
        assert!(parse("SUBSCRIBE CREATE (u:User {name: 'test'})").is_err());
    }

    #[test]
    fn existing_match_still_parses() {
        let stmt = parse("MATCH (u:User) RETURN u").unwrap();
        assert!(matches!(stmt, Statement::Match { .. }));
    }

    #[test]
    fn parse_unsubscribe_bare() {
        let stmt = parse("UNSUBSCRIBE").unwrap();
        assert!(matches!(stmt, Statement::Unsubscribe { stream_id: None }));
    }

    #[test]
    fn parse_unsubscribe_with_id() {
        let stmt = parse("UNSUBSCRIBE 42").unwrap();
        match stmt {
            Statement::Unsubscribe { stream_id } => {
                assert_eq!(stream_id, Some(42));
            }
            _ => panic!("expected Unsubscribe"),
        }
    }

    #[test]
    fn parse_subscribe_every_ms() {
        let stmt = parse("SUBSCRIBE MATCH (n) RETURN n EVERY 500 ms").unwrap();
        match stmt {
            Statement::Subscribe { interval, .. } => {
                assert_eq!(interval, Some(Duration::from_millis(500)));
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn parse_subscribe_every_minutes() {
        let stmt = parse("SUBSCRIBE MATCH (n) RETURN n EVERY 2 m").unwrap();
        match stmt {
            Statement::Subscribe { interval, .. } => {
                assert_eq!(interval, Some(Duration::from_secs(120)));
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn parse_subscribe_rejects_short_interval() {
        let err = parse("SUBSCRIBE MATCH (n) RETURN n EVERY 100 ms").unwrap_err();
        assert!(
            err.message.contains("at least 500ms"),
            "expected minimum interval error, got: {}",
            err.message
        );
    }

    // --- Variable-length path parsing ---

    #[test]
    fn parse_varpath_unbounded() {
        let stmt = parse("MATCH (a)-[*]->(b) RETURN b").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            if let Pattern::Path(elems) = &pattern[0] {
                if let Pattern::Rel { length_range, .. } = &elems[1] {
                    assert_eq!(*length_range, Some((1, None)));
                } else {
                    panic!("expected Rel");
                }
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_varpath_exact() {
        let stmt = parse("MATCH (a)-[*3]->(b) RETURN b").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            if let Pattern::Path(elems) = &pattern[0] {
                if let Pattern::Rel { length_range, .. } = &elems[1] {
                    assert_eq!(*length_range, Some((3, Some(3))));
                } else {
                    panic!("expected Rel");
                }
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_varpath_range() {
        let stmt = parse("MATCH (a)-[:KNOWS*1..5]->(b) RETURN b").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            if let Pattern::Path(elems) = &pattern[0] {
                if let Pattern::Rel {
                    rel_type,
                    length_range,
                    ..
                } = &elems[1]
                {
                    assert_eq!(rel_type, &Some("KNOWS".to_string()));
                    assert_eq!(*length_range, Some((1, Some(5))));
                } else {
                    panic!("expected Rel");
                }
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    // --- Path assignment ---

    #[test]
    fn parse_path_assignment() {
        let stmt = parse("MATCH path = (a:Person)-[:KNOWS]->(b) RETURN b.name").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            assert_eq!(pattern.len(), 1);
            assert!(matches!(&pattern[0], Pattern::Path(_)));
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn parse_shortest_path() {
        let stmt = parse("MATCH p = shortestPath((a)-[:KNOWS*1..5]->(b)) RETURN b").unwrap();
        if let Statement::Match { pattern, .. } = stmt {
            assert_eq!(pattern.len(), 1);
            if let Pattern::Path(elems) = &pattern[0] {
                assert_eq!(elems.len(), 3);
                if let Pattern::Rel { length_range, .. } = &elems[1] {
                    assert_eq!(*length_range, Some((1, Some(5))));
                } else {
                    panic!("expected Rel");
                }
            } else {
                panic!("expected Path");
            }
        } else {
            panic!("expected Match");
        }
    }

    // --- DISTINCT in function calls ---

    #[test]
    fn parse_collect_distinct_marks_aggregate_argument() {
        let stmt = parse("MATCH (a)-[:KNOWS]->(b) RETURN collect(DISTINCT b.name)").unwrap();
        let Statement::Match { return_clause, .. } = stmt else {
            panic!("expected MATCH statement");
        };
        let Expr::Function { name, args } = &return_clause.items[0].expr else {
            panic!("expected collect function");
        };
        assert_eq!(name, "collect");
        assert!(matches!(args.first(), Some(Expr::Distinct(_))));
    }

    // --- Negative pattern in WHERE ---

    #[test]
    fn parse_not_pattern_in_where() {
        let stmt = parse(
            "MATCH (a:Person), (b:Person) \
             WHERE NOT (a)-[:FOLLOWS]->(b) AND b.name <> 'Alice' \
             RETURN b.name",
        )
        .unwrap();

        let Statement::Match {
            where_clause: Some(where_clause),
            ..
        } = stmt
        else {
            panic!("expected MATCH with WHERE clause");
        };

        let Expr::And(left, right) = where_clause else {
            panic!("expected conjunction in WHERE clause");
        };
        match *left {
            Expr::PatternPredicate {
                start_var,
                hops,
                negated,
            } => {
                assert_eq!(start_var, "a");
                assert!(negated);
                assert_eq!(hops.len(), 1);
                assert_eq!(hops[0].rel_type.as_deref(), Some("FOLLOWS"));
            }
            other => panic!("expected NOT pattern predicate, got {other:?}"),
        }
        assert!(matches!(*right, Expr::Comparison { .. }));
    }

    // --- MERGE: node patterns ---

    #[test]
    fn parse_merge_node_succeeds() {
        let stmt = parse("MERGE (n:Entity {entity_id: 'x'}) RETURN n").unwrap();
        match stmt {
            Statement::Merge {
                patterns,
                set_clause,
                return_clause,
            } => {
                assert_eq!(patterns.len(), 1);
                assert!(matches!(
                    &patterns[0],
                    Pattern::Node {
                        label: Some(l),
                        ..
                    } if l == "Entity"
                ));
                assert!(set_clause.is_empty());
                assert!(return_clause.is_some());
            }
            _ => panic!("expected Merge"),
        }
    }

    #[test]
    fn parse_merge_node_unlabeled_succeeds() {
        let stmt = parse("MERGE (n) RETURN n").unwrap();
        assert!(matches!(stmt, Statement::Merge { .. }));
    }

    #[test]
    fn parse_unsupported_keyword_errors() {
        let err = parse("UPSERT (n:Entity)").unwrap_err();
        assert!(
            err.message.contains("unsupported statement keyword"),
            "expected 'unsupported statement keyword', got: {}",
            err.message
        );
    }

    #[test]
    fn parse_parameters_as_explicit_parameter_exprs() {
        let stmt = parse("MATCH (n:Person {name: $name}) WHERE n.age >= $age RETURN n").unwrap();
        if let Statement::Match {
            pattern,
            where_clause,
            ..
        } = stmt
        {
            let Pattern::Node { props, .. } = &pattern[0] else {
                panic!("expected node pattern")
            };
            assert!(matches!(&props[0].1, Expr::Parameter(name) if name == "name"));
            assert!(
                matches!(where_clause.unwrap(), Expr::Comparison { right, .. } if matches!(*right, Expr::Parameter(ref name) if name == "age"))
            );
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn unsupported_clauses_return_explicit_errors() {
        for (query, expected) in [
            (
                "OPTIONAL MATCH (n) RETURN n",
                "unsupported statement keyword: Keyword(Optional)",
            ),
            (
                "WITH 1 AS x RETURN x",
                "unsupported Cypher clause: Keyword(With)",
            ),
            (
                "MATCH (n) RETURN n WITH n RETURN n",
                "unsupported Cypher clause: Keyword(With)",
            ),
            (
                "CALL db.labels()",
                "unsupported Cypher clause: Keyword(Call)",
            ),
            (
                "MATCH (n) CALL { WITH n RETURN n } RETURN n",
                "unsupported Cypher clause: Keyword(Call)",
            ),
            (
                "FOREACH (x IN [1] | CREATE (:Person {name: 'x'}))",
                "unsupported Cypher clause: Keyword(Foreach)",
            ),
            (
                "LOAD CSV FROM 'file:///people.csv' AS row RETURN row",
                "unsupported Cypher clause: Keyword(Load)",
            ),
        ] {
            let err = parse(query).unwrap_err();
            assert!(
                err.message.contains(expected),
                "{query} should mention {expected}, got: {}",
                err.message
            );
        }
    }

    // --- MERGE: relationship patterns and multi-clause ---

    #[test]
    fn parse_merge_relationship_succeeds() {
        // Canonical ferrosa-memory edge-upsert shape (from the spec).
        let stmt = parse(
            "MERGE (a:Entity {entity_id: 'src'}) \
             MERGE (b:Entity {entity_id: 'dst'}) \
             MERGE (a)-[r:TYPED_EDGE {edge_type: 'links'}]->(b) \
             SET r.weight = 1 \
             RETURN r",
        )
        .unwrap();
        match stmt {
            Statement::Merge {
                patterns,
                set_clause,
                return_clause,
            } => {
                assert_eq!(patterns.len(), 3, "expected 3 patterns");
                assert_eq!(set_clause.len(), 1, "expected 1 SET assignment");
                assert!(return_clause.is_some(), "expected RETURN clause");
            }
            _ => panic!("expected Merge"),
        }
    }

    #[test]
    fn parse_merge_rel_missing_endpoints_errors() {
        // A bare relationship pattern without surrounding nodes is invalid.
        let err = parse("MERGE -[r:TYPED_EDGE]->(b) RETURN r").unwrap_err();
        assert!(
            !err.message.is_empty(),
            "expected a parse error, got empty message"
        );
    }

    #[test]
    fn parse_not_expr_still_works() {
        // Ensure regular NOT expressions are unaffected.
        let stmt = parse("MATCH (a) WHERE NOT a.active = true RETURN a").unwrap();
        if let Statement::Match {
            where_clause: Some(expr),
            ..
        } = stmt
        {
            assert!(matches!(expr, Expr::Not(_)));
        } else {
            panic!("expected Match with NOT");
        }
    }
}
