//! Cypher AST types.
//!
//! Represents the parsed structure of Cypher queries. Each variant maps
//! to a production rule in the grammar. The AST is produced by the parser
//! and consumed by the planner (in a later phase).

/// Direction of a relationship in a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// `(a)-[:REL]->(b)` — left to right.
    Out,
    /// `(a)<-[:REL]-(b)` — right to left.
    In,
    /// `(a)-[:REL]-(b)` — either direction.
    Both,
}

/// Comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,   // =
    Neq,  // <>
    Lt,   // <
    Gt,   // >
    LtEq, // <=
    GtEq, // >=
}

/// Arithmetic operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

/// A literal value in Cypher.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

/// An expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Variable reference: `n`
    Var(String),
    /// Property access: `n.name`
    Property { var: String, name: String },
    /// Literal value.
    Literal(Literal),
    /// Function call: `count(n)`, `id(n)`
    Function { name: String, args: Vec<Expr> },
    /// Comparison: `a.age > 30`
    Comparison {
        left: Box<Expr>,
        op: CompareOp,
        right: Box<Expr>,
    },
    /// Arithmetic: `a.age + 1`
    Arithmetic {
        left: Box<Expr>,
        op: ArithOp,
        right: Box<Expr>,
    },
    /// Boolean AND.
    And(Box<Expr>, Box<Expr>),
    /// Boolean OR.
    Or(Box<Expr>, Box<Expr>),
    /// Boolean NOT.
    Not(Box<Expr>),
    /// `expr IS NULL`
    IsNull(Box<Expr>),
    /// `expr IS NOT NULL`
    IsNotNull(Box<Expr>),
}

/// A property map in a node or edge pattern: `{name: 'Alice', age: 30}`.
pub type PropMap = Vec<(String, Expr)>;

/// A graph pattern element.
#[derive(Debug, Clone, PartialEq)]
pub enum Pattern {
    /// Node: `(n:Person {name: 'Alice'})`
    Node {
        var: Option<String>,
        label: Option<String>,
        props: PropMap,
    },
    /// Relationship: `-[:KNOWS {since: 2020}]->`
    ///
    /// Only appears inside `Pattern::Path` — never stands alone.
    /// The path provides the node context (src/dst).
    Rel {
        var: Option<String>,
        rel_type: Option<String>,
        direction: Direction,
        props: PropMap,
    },
    /// A path is an alternating sequence of nodes and relationships:
    /// `(a)-[:KNOWS]->(b)-[:WORKS_AT]->(c)`
    /// Stored as: [Node, Rel, Node, Rel, Node]
    Path(Vec<Pattern>),
}

/// A single item in a RETURN clause: `a.name AS alias`.
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

/// A single item in an ORDER BY clause.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub direction: SortDir,
}

/// The RETURN clause with optional DISTINCT, ORDER BY, and LIMIT.
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<i64>,
}

/// A property assignment in a SET clause: `n.name = 'Bob'`.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub var: String,
    pub property: String,
    pub value: Expr,
}

/// Top-level Cypher statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// `MATCH pattern [WHERE expr] RETURN ...`
    Match {
        pattern: Vec<Pattern>,
        where_clause: Option<Expr>,
        return_clause: ReturnClause,
    },
    /// `CREATE pattern, ...`
    Create { patterns: Vec<Pattern> },
    /// `MATCH pattern [WHERE expr] SET assignments`
    Set {
        pattern: Vec<Pattern>,
        where_clause: Option<Expr>,
        assignments: Vec<Assignment>,
    },
    /// `MATCH pattern [WHERE expr] [DETACH] DELETE vars`
    Delete {
        pattern: Vec<Pattern>,
        where_clause: Option<Expr>,
        detach: bool,
        variables: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_match_statement() {
        let stmt = Statement::Match {
            pattern: vec![Pattern::Path(vec![
                Pattern::Node {
                    var: Some("a".into()),
                    label: Some("Person".into()),
                    props: vec![(
                        "name".into(),
                        Expr::Literal(Literal::String("Alice".into())),
                    )],
                },
                Pattern::Rel {
                    var: None,
                    rel_type: Some("KNOWS".into()),
                    direction: Direction::Out,
                    props: vec![],
                },
                Pattern::Node {
                    var: Some("b".into()),
                    label: None,
                    props: vec![],
                },
            ])],
            where_clause: None,
            return_clause: ReturnClause {
                distinct: false,
                items: vec![ReturnItem {
                    expr: Expr::Property {
                        var: "b".into(),
                        name: "name".into(),
                    },
                    alias: None,
                }],
                order_by: vec![],
                limit: None,
            },
        };
        // Verify it's a Match variant.
        assert!(matches!(stmt, Statement::Match { .. }));
    }

    #[test]
    fn construct_create_statement() {
        let stmt = Statement::Create {
            patterns: vec![Pattern::Node {
                var: Some("n".into()),
                label: Some("Person".into()),
                props: vec![
                    (
                        "name".into(),
                        Expr::Literal(Literal::String("Alice".into())),
                    ),
                    ("age".into(), Expr::Literal(Literal::Integer(30))),
                ],
            }],
        };
        assert!(matches!(stmt, Statement::Create { .. }));
    }
}
