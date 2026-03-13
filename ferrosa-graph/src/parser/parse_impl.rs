//! Recursive-descent Cypher parser.
//!
//! One function per grammar rule. LL(2) — at most two-token lookahead.
//! Produces an AST from the token stream.

use crate::parser::ast::*;
use crate::parser::error::{ParseError, ParseResult};
use crate::parser::lexer::Lexer;
use crate::parser::token::{Keyword, TokenKind};

/// Parse a complete Cypher statement from source text.
pub fn parse(input: &str) -> ParseResult<Statement> {
    let mut lexer = Lexer::new(input);
    let stmt = parse_statement(&mut lexer)?;
    // Ensure we consumed all input.
    let tok = lexer.next_token()?;
    if tok.kind != TokenKind::Eof {
        return Err(ParseError::new(
            format!("unexpected token after statement: {:?}", tok.kind),
            tok.span,
        ));
    }
    Ok(stmt)
}

fn parse_statement(lexer: &mut Lexer<'_>) -> ParseResult<Statement> {
    let tok = lexer.peek()?;
    match &tok.kind {
        TokenKind::Keyword(Keyword::Match) => parse_match(lexer),
        TokenKind::Keyword(Keyword::Create) => parse_create(lexer),
        _ => Err(ParseError::new(
            format!("expected MATCH or CREATE, got {:?}", tok.kind),
            tok.span,
        )),
    }
}

fn parse_match(lexer: &mut Lexer<'_>) -> ParseResult<Statement> {
    lexer.expect(&TokenKind::Keyword(Keyword::Match))?;
    let pattern = parse_pattern_list(lexer)?;

    // Optional WHERE.
    let where_clause = if lexer.eat(&TokenKind::Keyword(Keyword::Where))? {
        Some(parse_expr(lexer)?)
    } else {
        None
    };

    // Check what follows: RETURN, SET, DELETE, DETACH DELETE.
    let tok = lexer.peek()?;
    match &tok.kind {
        TokenKind::Keyword(Keyword::Return) => {
            let return_clause = parse_return_clause(lexer)?;
            Ok(Statement::Match {
                pattern,
                where_clause,
                return_clause,
            })
        }
        TokenKind::Keyword(Keyword::Set) => {
            lexer.next_token()?;
            let assignments = parse_assignment_list(lexer)?;
            Ok(Statement::Set {
                pattern,
                where_clause,
                assignments,
            })
        }
        TokenKind::Keyword(Keyword::Delete) => {
            lexer.next_token()?;
            let variables = parse_var_list(lexer)?;
            Ok(Statement::Delete {
                pattern,
                where_clause,
                detach: false,
                variables,
            })
        }
        TokenKind::Keyword(Keyword::Detach) => {
            lexer.next_token()?;
            lexer.expect(&TokenKind::Keyword(Keyword::Delete))?;
            let variables = parse_var_list(lexer)?;
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

fn parse_create(lexer: &mut Lexer<'_>) -> ParseResult<Statement> {
    lexer.expect(&TokenKind::Keyword(Keyword::Create))?;
    let patterns = parse_pattern_list(lexer)?;
    Ok(Statement::Create { patterns })
}

// --- Pattern parsing ---

fn parse_pattern_list(lexer: &mut Lexer<'_>) -> ParseResult<Vec<Pattern>> {
    let mut patterns = vec![parse_pattern(lexer)?];
    while lexer.eat(&TokenKind::Comma)? {
        patterns.push(parse_pattern(lexer)?);
    }
    Ok(patterns)
}

fn parse_pattern(lexer: &mut Lexer<'_>) -> ParseResult<Pattern> {
    let first = parse_node_pattern(lexer)?;
    let mut elements = vec![first];

    // Check for relationship continuation: -[ or <- or ->
    loop {
        let tok = lexer.peek()?;
        match &tok.kind {
            TokenKind::DashBracket | TokenKind::ArrowLeft | TokenKind::Minus => {
                let rel = parse_rel_pattern(lexer)?;
                elements.push(rel);
                let node = parse_node_pattern(lexer)?;
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

pub(crate) fn parse_node_pattern(lexer: &mut Lexer<'_>) -> ParseResult<Pattern> {
    lexer.expect(&TokenKind::LParen)?;

    let mut var = None;
    let mut label = None;
    let mut props = vec![];

    let tok = lexer.peek()?;
    match &tok.kind {
        TokenKind::RParen => {
            // Empty node: ()
        }
        TokenKind::Colon => {
            // Label only: (:Person)
            label = Some(parse_label(lexer)?);
            if lexer.peek()?.kind == TokenKind::LBrace {
                props = parse_prop_map(lexer)?;
            }
        }
        TokenKind::Ident(_) => {
            // Variable, possibly followed by label and props.
            let name_tok = lexer.next_token()?;
            if let TokenKind::Ident(name) = name_tok.kind {
                var = Some(name.to_string());
            }
            if lexer.peek()?.kind == TokenKind::Colon {
                label = Some(parse_label(lexer)?);
            }
            if lexer.peek()?.kind == TokenKind::LBrace {
                props = parse_prop_map(lexer)?;
            }
        }
        TokenKind::LBrace => {
            props = parse_prop_map(lexer)?;
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

    lexer.expect(&TokenKind::RParen)?;

    Ok(Pattern::Node { var, label, props })
}

fn parse_rel_pattern(lexer: &mut Lexer<'_>) -> ParseResult<Pattern> {
    let tok = lexer.peek()?;
    match &tok.kind {
        // -[:TYPE]-> or -[:TYPE]- or -[var:TYPE {props}]->
        TokenKind::DashBracket => {
            lexer.next_token()?;
            let (var, rel_type, props) = parse_rel_detail(lexer)?;

            // Expect ]-> or ]- or ]
            let close = lexer.peek()?;
            let direction = match &close.kind {
                TokenKind::BracketArrow => {
                    lexer.next_token()?;
                    Direction::Out
                }
                TokenKind::BracketDash => {
                    lexer.next_token()?;
                    Direction::Both
                }
                TokenKind::RBracket => {
                    lexer.next_token()?;
                    // Check for trailing ->; otherwise treat as undirected.
                    if lexer.eat(&TokenKind::ArrowRight)? {
                        Direction::Out
                    } else {
                        // Consume optional trailing dash for ]-
                        let _ = lexer.eat(&TokenKind::Minus)?;
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
            lexer.next_token()?;
            lexer.expect(&TokenKind::LBracket)?;
            let (var, rel_type, props) = parse_rel_detail(lexer)?;
            // Expect ]- or ]
            let close = lexer.peek()?;
            match &close.kind {
                TokenKind::BracketDash => {
                    lexer.next_token()?;
                }
                TokenKind::RBracket => {
                    lexer.next_token()?;
                    lexer.expect(&TokenKind::Minus)?;
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
            lexer.next_token()?;
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
fn parse_rel_detail(
    lexer: &mut Lexer<'_>,
) -> ParseResult<(Option<String>, Option<String>, PropMap)> {
    let mut var = None;
    let mut rel_type = None;
    let mut props = vec![];

    let tok = lexer.peek()?;
    match &tok.kind {
        TokenKind::Colon => {
            rel_type = Some(parse_label(lexer)?);
        }
        TokenKind::Ident(_) => {
            let name_tok = lexer.next_token()?;
            if let TokenKind::Ident(name) = name_tok.kind {
                var = Some(name.to_string());
            }
            if lexer.peek()?.kind == TokenKind::Colon {
                rel_type = Some(parse_label(lexer)?);
            }
        }
        _ => {
            // Empty brackets: -[]-
        }
    }

    if lexer.peek()?.kind == TokenKind::LBrace {
        props = parse_prop_map(lexer)?;
    }

    Ok((var, rel_type, props))
}

/// Parse `:Label` — consume colon and return the label name.
fn parse_label(lexer: &mut Lexer<'_>) -> ParseResult<String> {
    lexer.expect(&TokenKind::Colon)?;
    let tok = lexer.next_token()?;
    match tok.kind {
        TokenKind::Ident(name) => Ok(name.to_string()),
        // Keywords can be labels too (e.g., :Order, :Set).
        TokenKind::Keyword(_) => {
            let text = &lexer.input[tok.span.start..tok.span.end];
            Ok(text.to_string())
        }
        _ => Err(ParseError::new(
            format!("expected label name after ':', got {:?}", tok.kind),
            tok.span,
        )),
    }
}

/// Parse `{key: value, ...}`.
fn parse_prop_map(lexer: &mut Lexer<'_>) -> ParseResult<PropMap> {
    lexer.expect(&TokenKind::LBrace)?;
    let mut props = vec![];

    if lexer.peek()?.kind != TokenKind::RBrace {
        loop {
            let key_tok = lexer.next_token()?;
            let key = match key_tok.kind {
                TokenKind::Ident(name) => name.to_string(),
                _ => {
                    return Err(ParseError::new(
                        format!("expected property name, got {:?}", key_tok.kind),
                        key_tok.span,
                    ));
                }
            };
            lexer.expect(&TokenKind::Colon)?;
            let value = parse_expr(lexer)?;
            props.push((key, value));

            if !lexer.eat(&TokenKind::Comma)? {
                break;
            }
        }
    }

    lexer.expect(&TokenKind::RBrace)?;
    Ok(props)
}

// --- Expression parsing (precedence climbing) ---

pub(crate) fn parse_expr(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    parse_or_expr(lexer)
}

fn parse_or_expr(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let mut left = parse_and_expr(lexer)?;
    while lexer.eat(&TokenKind::Keyword(Keyword::Or))? {
        let right = parse_and_expr(lexer)?;
        left = Expr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_and_expr(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let mut left = parse_not_expr(lexer)?;
    while lexer.eat(&TokenKind::Keyword(Keyword::And))? {
        let right = parse_not_expr(lexer)?;
        left = Expr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_not_expr(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    if lexer.eat(&TokenKind::Keyword(Keyword::Not))? {
        let inner = parse_not_expr(lexer)?;
        Ok(Expr::Not(Box::new(inner)))
    } else {
        parse_comparison(lexer)
    }
}

fn parse_comparison(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let left = parse_addition(lexer)?;

    // Check for IS NULL / IS NOT NULL.
    if lexer.eat(&TokenKind::Keyword(Keyword::Is))? {
        if lexer.eat(&TokenKind::Keyword(Keyword::Not))? {
            lexer.expect(&TokenKind::Keyword(Keyword::Null))?;
            return Ok(Expr::IsNotNull(Box::new(left)));
        } else {
            lexer.expect(&TokenKind::Keyword(Keyword::Null))?;
            return Ok(Expr::IsNull(Box::new(left)));
        }
    }

    let tok = lexer.peek()?;
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
        lexer.next_token()?;
        let right = parse_addition(lexer)?;
        Ok(Expr::Comparison {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    } else {
        Ok(left)
    }
}

fn parse_addition(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let mut left = parse_multiplication(lexer)?;
    loop {
        let tok = lexer.peek()?;
        let op = match &tok.kind {
            TokenKind::Plus => Some(ArithOp::Add),
            TokenKind::Minus => Some(ArithOp::Sub),
            _ => None,
        };
        if let Some(op) = op {
            lexer.next_token()?;
            let right = parse_multiplication(lexer)?;
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

fn parse_multiplication(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let mut left = parse_unary(lexer)?;
    loop {
        let tok = lexer.peek()?;
        let op = match &tok.kind {
            TokenKind::Star => Some(ArithOp::Mul),
            TokenKind::Slash => Some(ArithOp::Div),
            _ => None,
        };
        if let Some(op) = op {
            lexer.next_token()?;
            let right = parse_unary(lexer)?;
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

fn parse_unary(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    if lexer.eat(&TokenKind::Minus)? {
        let inner = parse_unary(lexer)?;
        // Negate: wrap as 0 - inner.
        Ok(Expr::Arithmetic {
            left: Box::new(Expr::Literal(Literal::Integer(0))),
            op: ArithOp::Sub,
            right: Box::new(inner),
        })
    } else {
        parse_primary(lexer)
    }
}

fn parse_primary(lexer: &mut Lexer<'_>) -> ParseResult<Expr> {
    let tok = lexer.peek()?;
    match &tok.kind {
        TokenKind::Integer(_) => {
            let tok = lexer.next_token()?;
            if let TokenKind::Integer(v) = tok.kind {
                Ok(Expr::Literal(Literal::Integer(v)))
            } else {
                unreachable!()
            }
        }
        TokenKind::Float(_) => {
            let tok = lexer.next_token()?;
            if let TokenKind::Float(v) = tok.kind {
                Ok(Expr::Literal(Literal::Float(v)))
            } else {
                unreachable!()
            }
        }
        TokenKind::StringLit(_) => {
            let tok = lexer.next_token()?;
            if let TokenKind::StringLit(s) = tok.kind {
                Ok(Expr::Literal(Literal::String(s)))
            } else {
                unreachable!()
            }
        }
        TokenKind::Keyword(Keyword::True) => {
            lexer.next_token()?;
            Ok(Expr::Literal(Literal::Bool(true)))
        }
        TokenKind::Keyword(Keyword::False) => {
            lexer.next_token()?;
            Ok(Expr::Literal(Literal::Bool(false)))
        }
        TokenKind::Keyword(Keyword::Null) => {
            lexer.next_token()?;
            Ok(Expr::Literal(Literal::Null))
        }
        TokenKind::LParen => {
            lexer.next_token()?;
            let expr = parse_expr(lexer)?;
            lexer.expect(&TokenKind::RParen)?;
            Ok(expr)
        }
        TokenKind::Ident(_) => {
            let tok = lexer.next_token()?;
            let name = if let TokenKind::Ident(s) = tok.kind {
                s.to_string()
            } else {
                unreachable!()
            };

            // Check for property access (var.prop) or function call (fn(...)).
            let next = lexer.peek()?;
            match &next.kind {
                TokenKind::Dot => {
                    lexer.next_token()?;
                    let prop_tok = lexer.next_token()?;
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
                    lexer.next_token()?;
                    let mut args = vec![];
                    if lexer.peek()?.kind != TokenKind::RParen {
                        loop {
                            args.push(parse_expr(lexer)?);
                            if !lexer.eat(&TokenKind::Comma)? {
                                break;
                            }
                        }
                    }
                    lexer.expect(&TokenKind::RParen)?;
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

fn parse_return_clause(lexer: &mut Lexer<'_>) -> ParseResult<ReturnClause> {
    lexer.expect(&TokenKind::Keyword(Keyword::Return))?;

    let distinct = lexer.eat(&TokenKind::Keyword(Keyword::Distinct))?;

    let mut items = vec![parse_return_item(lexer)?];
    while lexer.eat(&TokenKind::Comma)? {
        items.push(parse_return_item(lexer)?);
    }

    let order_by = if lexer.eat(&TokenKind::Keyword(Keyword::Order))? {
        lexer.expect(&TokenKind::Keyword(Keyword::By))?;
        let mut orders = vec![parse_order_item(lexer)?];
        while lexer.eat(&TokenKind::Comma)? {
            orders.push(parse_order_item(lexer)?);
        }
        orders
    } else {
        vec![]
    };

    let limit = if lexer.eat(&TokenKind::Keyword(Keyword::Limit))? {
        let tok = lexer.next_token()?;
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

fn parse_return_item(lexer: &mut Lexer<'_>) -> ParseResult<ReturnItem> {
    let expr = parse_expr(lexer)?;
    let alias = if lexer.eat(&TokenKind::Keyword(Keyword::As))? {
        let tok = lexer.next_token()?;
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

fn parse_order_item(lexer: &mut Lexer<'_>) -> ParseResult<OrderItem> {
    let expr = parse_expr(lexer)?;
    let direction = if lexer.eat(&TokenKind::Keyword(Keyword::Desc))? {
        SortDir::Desc
    } else {
        lexer.eat(&TokenKind::Keyword(Keyword::Asc))?;
        SortDir::Asc
    };
    Ok(OrderItem { expr, direction })
}

// --- SET and DELETE helpers ---

fn parse_assignment_list(lexer: &mut Lexer<'_>) -> ParseResult<Vec<Assignment>> {
    let mut assignments = vec![parse_assignment(lexer)?];
    while lexer.eat(&TokenKind::Comma)? {
        assignments.push(parse_assignment(lexer)?);
    }
    Ok(assignments)
}

fn parse_assignment(lexer: &mut Lexer<'_>) -> ParseResult<Assignment> {
    let tok = lexer.next_token()?;
    let var = match tok.kind {
        TokenKind::Ident(name) => name.to_string(),
        _ => {
            return Err(ParseError::new(
                format!("expected variable name in SET, got {:?}", tok.kind),
                tok.span,
            ));
        }
    };
    lexer.expect(&TokenKind::Dot)?;
    let prop_tok = lexer.next_token()?;
    let property = match prop_tok.kind {
        TokenKind::Ident(name) => name.to_string(),
        _ => {
            return Err(ParseError::new(
                format!("expected property name after '.', got {:?}", prop_tok.kind),
                prop_tok.span,
            ));
        }
    };
    lexer.expect(&TokenKind::Eq)?;
    let value = parse_expr(lexer)?;
    Ok(Assignment {
        var,
        property,
        value,
    })
}

fn parse_var_list(lexer: &mut Lexer<'_>) -> ParseResult<Vec<String>> {
    let mut vars = vec![];
    let tok = lexer.next_token()?;
    match tok.kind {
        TokenKind::Ident(name) => vars.push(name.to_string()),
        _ => {
            return Err(ParseError::new(
                format!("expected variable name in DELETE, got {:?}", tok.kind),
                tok.span,
            ));
        }
    }
    while lexer.eat(&TokenKind::Comma)? {
        let tok = lexer.next_token()?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::*;

    // --- Node patterns ---

    #[test]
    fn parse_empty_node() {
        let mut lexer = Lexer::new("()");
        let node = parse_node_pattern(&mut lexer).unwrap();
        assert_eq!(
            node,
            Pattern::Node {
                var: None,
                label: None,
                props: vec![]
            }
        );
    }

    #[test]
    fn parse_node_with_var() {
        let mut lexer = Lexer::new("(n)");
        let node = parse_node_pattern(&mut lexer).unwrap();
        assert_eq!(
            node,
            Pattern::Node {
                var: Some("n".into()),
                label: None,
                props: vec![]
            }
        );
    }

    #[test]
    fn parse_node_with_label() {
        let mut lexer = Lexer::new("(n:Person)");
        let node = parse_node_pattern(&mut lexer).unwrap();
        assert_eq!(
            node,
            Pattern::Node {
                var: Some("n".into()),
                label: Some("Person".into()),
                props: vec![],
            }
        );
    }

    #[test]
    fn parse_node_with_props() {
        let mut lexer = Lexer::new("(n:Person {name: 'Alice', age: 30})");
        let node = parse_node_pattern(&mut lexer).unwrap();
        assert_eq!(
            node,
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

    // --- Additional coverage (from review) ---

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
        let stmt = parse("MATCH (a) WHERE a.score > 3.14 RETURN a").unwrap();
        assert!(matches!(
            stmt,
            Statement::Match {
                where_clause: Some(_),
                ..
            }
        ));
    }
}
