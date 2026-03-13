//! Recursive-descent Cypher parser.
//!
//! One function per grammar rule. LL(2) — at most two-token lookahead.
//! Produces an AST from the token stream.
//!
//! Expression nesting is capped at [`MAX_EXPR_DEPTH`] (64) to prevent
//! stack overflow from adversarial input (threat model T1).

use crate::parser::ast::*;
use crate::parser::error::{ParseError, ParseResult, Span};
use crate::parser::lexer::Lexer;
use crate::parser::token::{Keyword, TokenKind};

/// Maximum expression nesting depth before the parser returns an error.
/// Prevents stack overflow from deeply nested parentheses or chained
/// NOT/unary operators. (Threat model T1 mitigation.)
const MAX_EXPR_DEPTH: usize = 64;

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
            _ => Err(ParseError::new(
                format!("expected MATCH or CREATE, got {:?}", tok.kind),
                tok.span,
            )),
        }
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

        // Check what follows: RETURN, SET, DELETE, DETACH DELETE.
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
            _ => Err(ParseError::new(
                format!(
                    "expected RETURN, SET, DELETE, or DETACH DELETE after MATCH, got {:?}",
                    tok.kind
                ),
                tok.span,
            )),
        }
    }

    fn parse_create(&mut self) -> ParseResult<Statement> {
        self.lexer.expect(&TokenKind::Keyword(Keyword::Create))?;
        let patterns = self.parse_pattern_list()?;
        Ok(Statement::Create { patterns })
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

        self.lexer.expect(&TokenKind::RParen)?;

        Ok(Pattern::Node { var, label, props })
    }

    fn parse_rel_pattern(&mut self) -> ParseResult<Pattern> {
        let tok = self.lexer.peek()?;
        match &tok.kind {
            // -[:TYPE]-> or -[:TYPE]- or -[var:TYPE {props}]->
            TokenKind::DashBracket => {
                self.lexer.next_token()?;
                let (var, rel_type, props) = self.parse_rel_detail()?;

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
                })
            }
            // <-[:TYPE]- or <-[var:TYPE]-
            TokenKind::ArrowLeft => {
                self.lexer.next_token()?;
                self.lexer.expect(&TokenKind::LBracket)?;
                let (var, rel_type, props) = self.parse_rel_detail()?;
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

    /// Parse the inside of `[ ... ]` in a relationship: `var:TYPE {props}`
    fn parse_rel_detail(&mut self) -> ParseResult<(Option<String>, Option<String>, PropMap)> {
        let mut var = None;
        let mut rel_type = None;
        let mut props = vec![];

        let tok = self.lexer.peek()?;
        match &tok.kind {
            TokenKind::Colon => {
                rel_type = Some(self.parse_label()?);
            }
            TokenKind::Ident(_) => {
                let name_tok = self.lexer.next_token()?;
                if let TokenKind::Ident(name) = name_tok.kind {
                    var = Some(name.to_string());
                }
                if self.lexer.peek()?.kind == TokenKind::Colon {
                    rel_type = Some(self.parse_label()?);
                }
            }
            _ => {
                // Empty brackets: -[]-
            }
        }

        if self.lexer.peek()?.kind == TokenKind::LBrace {
            props = self.parse_prop_map()?;
        }

        Ok((var, rel_type, props))
    }

    /// Parse `:Label` — consume colon and return the label name.
    fn parse_label(&mut self) -> ParseResult<String> {
        self.lexer.expect(&TokenKind::Colon)?;
        let tok = self.lexer.next_token()?;
        match tok.kind {
            TokenKind::Ident(name) => Ok(name.to_string()),
            // Keywords can be labels too (e.g., :Order, :Set).
            TokenKind::Keyword(_) => {
                let text = &self.lexer.input[tok.span.start..tok.span.end];
                Ok(text.to_string())
            }
            _ => Err(ParseError::new(
                format!("expected label name after ':', got {:?}", tok.kind),
                tok.span,
            )),
        }
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
            // NOT recurses — check depth.
            self.enter_expr()?;
            let inner = self.parse_not_expr();
            self.exit_expr();
            Ok(Expr::Not(Box::new(inner?)))
        } else {
            self.parse_comparison()
        }
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
            self.parse_primary()
        }
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

                // Check for property access (var.prop) or function call (fn(...)).
                let next = self.lexer.peek()?;
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
                                args.push(self.parse_expr()?);
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
}

/// Parse a complete Cypher statement from source text.
pub fn parse(input: &str) -> ParseResult<Statement> {
    let mut parser = Parser::new(input);
    let stmt = parser.parse_statement()?;
    // Ensure we consumed all input.
    let tok = parser.lexer.next_token()?;
    if tok.kind != TokenKind::Eof {
        return Err(ParseError::new(
            format!("unexpected token after statement: {:?}", tok.kind),
            tok.span,
        ));
    }
    Ok(stmt)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Node patterns ---

    #[test]
    fn parse_empty_node() {
        let stmt = parse("CREATE ()").unwrap();
        if let Statement::Create { patterns } = stmt {
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
        if let Statement::Create { patterns } = stmt {
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
        if let Statement::Create { patterns } = stmt {
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
        if let Statement::Create { patterns } = stmt {
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
        if let Statement::Create { patterns } = stmt {
            assert!(matches!(&patterns[0], Pattern::Path(_)));
        } else {
            panic!("expected Create");
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
        if let Statement::Create { patterns } = stmt {
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
}
