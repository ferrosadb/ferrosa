//! Cypher AST types.
//!
//! Represents the parsed structure of Cypher queries. Each variant maps
//! to a production rule in the grammar. The AST is produced by the parser
//! and consumed by the planner (in a later phase).

use std::time::Duration;

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

/// Common Cypher list predicate kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListPredicateKind {
    Any,
    All,
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
    /// Query parameter reference: `$name`. Bound before planning/execution.
    Parameter(String),
    /// Function call: `count(n)`, `id(n)`
    Function { name: String, args: Vec<Expr> },
    /// DISTINCT marker used inside aggregate functions, e.g. `count(DISTINCT n.age)`.
    Distinct(Box<Expr>),
    /// Comparison: `a.age > 30`
    Comparison {
        left: Box<Expr>,
        op: CompareOp,
        right: Box<Expr>,
    },
    /// Membership: `expr IN list`.
    In { value: Box<Expr>, list: Box<Expr> },
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
    /// List literal: `[1, 2, 3]`.
    List(Vec<Expr>),
    /// Scoped list predicate, e.g. `any(x IN xs WHERE x > 0)`.
    ListPredicate {
        kind: ListPredicateKind,
        var: String,
        list: Box<Expr>,
        predicate: Box<Expr>,
    },
    /// List comprehension: `[x IN list WHERE pred | expr]`.
    ///
    /// `filter` (the `WHERE pred`) and `projection` (the `| expr`) are both
    /// optional. `[x IN xs]` is the whole list; `[x IN xs WHERE p]` filters;
    /// `[x IN xs | e]` maps; `[x IN xs WHERE p | e]` filters then maps.
    ListComprehension {
        var: String,
        list: Box<Expr>,
        filter: Option<Box<Expr>>,
        projection: Option<Box<Expr>>,
    },
    /// Pattern comprehension: `[ (n)-[:R]->(m) WHERE pred | m.prop ]`.
    ///
    /// Evaluates a graph pattern starting from an already-bound variable,
    /// optionally filters each match, and projects an expression per match,
    /// collecting the results into a list. Requires graph-aware evaluation.
    ///
    /// Unlike [`Expr::PatternPredicate`], each hop carries its target node's
    /// binding variable so the `WHERE`/projection expressions can reference
    /// matched nodes (e.g. `m` in `| m.name`).
    PatternComprehension {
        start_var: String,
        hops: Vec<PatternComprehensionHop>,
        filter: Option<Box<Expr>>,
        projection: Box<Expr>,
    },
    /// Map literal: `{name: 'Alice'}`.
    Map(PropMap),
    /// Map projection over a bound variable: `n {.name, .age, foo: expr, .*}`.
    ///
    /// Builds a map by selecting properties off `var` (and/or computed
    /// entries). Distinct from a [`Expr::Map`] literal, which has no base
    /// variable.
    MapProjection {
        var: String,
        selectors: Vec<MapProjectionSelector>,
    },
    /// List/map/string indexing: `expr[index]`.
    Index { target: Box<Expr>, index: Box<Expr> },
    /// List/string slicing: `expr[start..end]`.
    Slice {
        target: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },
    PatternPredicate {
        start_var: String,
        hops: Vec<PatternPredicateHop>,
        negated: bool,
    },
    /// `expr IS NULL`
    IsNull(Box<Expr>),
    /// `expr IS NOT NULL`
    IsNotNull(Box<Expr>),
}

/// A property map in a node or edge pattern: `{name: 'Alice', age: 30}`.
pub type PropMap = Vec<(String, Expr)>;

/// One selector inside a map projection: `n {.name, alias: expr, .*}`.
#[derive(Debug, Clone, PartialEq)]
pub enum MapProjectionSelector {
    /// `.prop` — copy `var.prop` into the projected map under key `prop`.
    Property(String),
    /// `key: expr` — insert a computed entry under `key`.
    Literal { key: String, value: Expr },
    /// `.*` — copy every property of `var` into the projected map.
    All,
}

/// One relationship+target-node hop in a WHERE pattern predicate.
#[derive(Debug, Clone, PartialEq)]
pub struct PatternPredicateHop {
    pub rel_type: Option<String>,
    pub direction: Direction,
    pub target_label: Option<String>,
    pub target_props: PropMap,
}

/// One relationship+target-node hop in a pattern comprehension.
///
/// Like [`PatternPredicateHop`] but additionally preserves the target node's
/// binding variable so the comprehension's filter and projection can refer to
/// matched nodes.
#[derive(Debug, Clone, PartialEq)]
pub struct PatternComprehensionHop {
    pub rel_type: Option<String>,
    pub direction: Direction,
    pub target_var: Option<String>,
    pub target_label: Option<String>,
    pub target_props: PropMap,
}

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
        /// Variable-length path range: `[*1..5]`, `[*]`, `[*3]`
        /// None = single hop (default). Some((min, max)) where max=None means unbounded.
        length_range: Option<(u32, Option<u32>)>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct WithPipeline {
    pub clause: ReturnClause,
    pub where_clause: Option<Expr>,
}

/// A property assignment in a SET clause: `n.name = 'Bob'`.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub var: String,
    pub property: String,
    pub value: Expr,
}

/// A single target of a REMOVE clause: either a property to unset
/// (`n.prop`) or a label to drop (`n:Label`).
#[derive(Debug, Clone, PartialEq)]
pub enum RemoveItem {
    /// `REMOVE n.prop` — unset a property on the matched node/rel.
    Property { var: String, property: String },
    /// `REMOVE n:Label` — drop a label from the matched node.
    Label { var: String, label: String },
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
    /// `MATCH pattern [WHERE expr] WITH ... RETURN ...`
    MatchWith {
        pattern: Vec<Pattern>,
        where_clause: Option<Expr>,
        with_pipeline: WithPipeline,
        return_clause: ReturnClause,
    },
    /// Top-level scalar projection: `RETURN expr [, ...]`.
    Return {
        return_clause: ReturnClause,
    },
    /// `UNWIND expr AS var RETURN ...`
    Unwind {
        expr: Expr,
        var: String,
        with_pipeline: Option<WithPipeline>,
        return_clause: ReturnClause,
    },
    Union {
        arms: Vec<Statement>,
        all: bool,
    },
    MatchWithOptional {
        pattern: Vec<Pattern>,
        where_clause: Option<Expr>,
        optional_pattern: Vec<Pattern>,
        optional_where_clause: Option<Expr>,
        return_clause: ReturnClause,
    },
    /// `CREATE pattern, ... [RETURN ...]`
    Create {
        patterns: Vec<Pattern>,
        return_clause: Option<ReturnClause>,
    },
    /// `MATCH pattern [WHERE expr] SET assignments`
    Set {
        pattern: Vec<Pattern>,
        where_clause: Option<Expr>,
        assignments: Vec<Assignment>,
    },
    /// `MATCH pattern [WHERE expr] REMOVE items`
    Remove {
        pattern: Vec<Pattern>,
        where_clause: Option<Expr>,
        items: Vec<RemoveItem>,
    },
    /// `MATCH pattern [WHERE expr] [DETACH] DELETE vars`
    Delete {
        pattern: Vec<Pattern>,
        where_clause: Option<Expr>,
        detach: bool,
        variables: Vec<String>,
    },
    /// `SUBSCRIBE MATCH pattern ... [EVERY duration] [DELTA]`
    Subscribe {
        inner: Box<Statement>,
        interval: Option<Duration>,
        delta: bool,
    },
    /// `UNSUBSCRIBE [stream_id]`
    Unsubscribe {
        stream_id: Option<u16>,
    },
    /// `MERGE pattern ... [MERGE pattern ...] [SET assignments] [RETURN ...]`
    Merge {
        patterns: Vec<Pattern>,
        set_clause: Vec<Assignment>,
        return_clause: Option<ReturnClause>,
    },
    /// `FOREACH (var IN list_expr | update_clause [update_clause ...])`
    ///
    /// Executes each contained update clause once per element of `list`, with
    /// `var` bound to the current element. The body may contain only update
    /// clauses (CREATE / SET / MERGE / DELETE / REMOVE / nested FOREACH); it
    /// never projects rows. Atomic with the surrounding statement: if any
    /// iteration's clause fails, the whole FOREACH writes nothing.
    Foreach {
        /// Loop variable bound to each element of `list` in turn.
        var: String,
        /// Expression evaluated once to the list being iterated.
        list: Expr,
        /// Update clauses executed per element, in order.
        body: Vec<Statement>,
    },
    /// `MATCH ... CALL { WITH <imports> <inner> } [RETURN ...]`
    ///
    /// A correlated subquery: the `inner` statement runs once per row produced by
    /// the `outer` statement, with each variable in `imports` (the inner leading
    /// `WITH`) bound to that outer row. Inner results are UNITED across rows.
    ///
    /// Two shapes are supported:
    /// - **Returning** subquery (`inner` projects rows): each inner row is paired
    ///   with its driving outer row; the trailing `return_clause` (if present)
    ///   projects over the combined `outer ⨯ inner` bindings.
    /// - **Unit** subquery (`inner` performs only updates, no RETURN): executed for
    ///   its write side effects per outer row, leaving outer cardinality unchanged.
    CallSubquery {
        /// The driving query whose rows are imported into the subquery, one at a
        /// time. Must project every name in `imports`.
        outer: Box<Statement>,
        /// Variables imported into the subquery via its leading `WITH`.
        imports: Vec<String>,
        /// The subquery body, run once per outer row with `imports` bound.
        inner: Box<Statement>,
        /// Optional trailing `RETURN` after the `CALL {}` block.
        return_clause: Option<ReturnClause>,
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
                    length_range: None,
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
    fn construct_merge_statement() {
        let stmt = Statement::Merge {
            patterns: vec![Pattern::Node {
                var: Some("n".into()),
                label: Some("Entity".into()),
                props: vec![(
                    "entity_id".into(),
                    Expr::Literal(Literal::String("x".into())),
                )],
            }],
            set_clause: vec![],
            return_clause: None,
        };
        assert!(matches!(stmt, Statement::Merge { .. }));
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
            return_clause: None,
        };
        assert!(matches!(stmt, Statement::Create { .. }));
    }
}
