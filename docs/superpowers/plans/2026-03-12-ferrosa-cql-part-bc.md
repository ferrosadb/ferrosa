# ferrosa-cql Parts B+C Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the CQL protocol layer so cqlsh and standard CQL drivers connect to Ferrosa and run queries end-to-end against a single-node storage engine.

**Architecture:** Bottom-up layered build: AST types → lexer → parser → bridge → result encoder → prepared cache → router → connection handler. Each layer is independently testable. TDD throughout — write failing test, implement, pass, commit.

**Tech Stack:** Rust, tokio, tokio-util (codec/framed), bytes, phf (keyword map), moka (prepared cache), md-5, ferrosa-common, ferrosa-schema, ferrosa-storage, ferrosa-sstable, proptest (fuzz)

**Spec:** `docs/superpowers/specs/2026-03-12-ferrosa-cql-part-bc-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `ferrosa-cql/src/ast.rs` (new) | `Statement` enum, all AST node types, `Term` enum |
| `ferrosa-cql/src/lexer.rs` (new) | Zero-alloc tokenizer, `Token<'input>`, `Keyword` enum, `phf` map |
| `ferrosa-cql/src/parser.rs` (new) | Recursive descent parser, one function per grammar rule |
| `ferrosa-cql/src/bridge.rs` (new) | `CqlValue` ↔ `CellValue` conversion, key serialization, `term_to_cql_value` |
| `ferrosa-cql/src/result.rs` (new) | RESULT frame body encoding (Rows, Void, Prepared, SetKeyspace, SchemaChange) |
| `ferrosa-cql/src/prepared.rs` (new) | `PreparedCache` wrapping moka, `PreparedPlan`, schema invalidation |
| `ferrosa-cql/src/router.rs` (new) | Query dispatch: AST → Schema/StorageEngine/system queries, `SharedState` |
| `ferrosa-cql/src/connection.rs` (replace) | Full protocol handler replacing stub |
| `ferrosa-cql/src/server.rs` (modify) | Add `SharedState`, pass to connection tasks |
| `ferrosa-cql/src/lib.rs` (modify) | Add module declarations |
| `ferrosa-cql/Cargo.toml` (modify) | Add moka, ferrosa-sstable, indexmap deps |

---

## Chunk 1: Foundation (AST + Lexer + Parser)

### Task 1: AST Types

**Files:**

- Create: `ferrosa-cql/src/ast.rs`
- Modify: `ferrosa-cql/src/lib.rs`

- [ ] **Step 1: Add module declaration to lib.rs**

Add `pub mod ast;` to `ferrosa-cql/src/lib.rs`.

- [ ] **Step 2: Write AST types**

Create `ferrosa-cql/src/ast.rs` with all types from the spec. Key types:

```rust
use uuid::Uuid;

/// Top-level parsed statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Batch(BatchStatement),
    CreateKeyspace(CreateKeyspaceStatement),
    AlterKeyspace(AlterKeyspaceStatement),
    DropKeyspace(DropKeyspaceStatement),
    CreateTable(CreateTableStatement),
    AlterTable(AlterTableStatement),
    DropTable(DropTableStatement),
    CreateRole(CreateRoleStatement),
    AlterRole(AlterRoleStatement),
    DropRole(DropRoleStatement),
    Grant(GrantStatement),
    Revoke(RevokeStatement),
    Use(UseStatement),
    Truncate(TruncateStatement),
}

/// A value expression in DML statements.
#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    StringLiteral(String),
    IntegerLiteral(i64),
    FloatLiteral(f64),
    UuidLiteral(Uuid),
    BlobLiteral(Vec<u8>),
    BoolLiteral(bool),
    Null,
    BindMarker(Option<String>),
    InList(Vec<Term>),
    ListLiteral(Vec<Term>),
    MapLiteral(Vec<(Term, Term)>),
    SetLiteral(Vec<Term>),
    TupleLiteral(Vec<Term>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonOp { Eq, Lt, Gt, Le, Ge, In, Ne }

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub column: String,
    pub op: ComparisonOp,
    pub value: Term,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectColumn {
    Star,
    Column(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderDirection { Asc, Desc }

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<SelectColumn>,
    pub where_clauses: Vec<WhereClause>,
    pub order_by: Vec<(String, OrderDirection)>,
    pub limit: Option<i32>,
    pub allow_filtering: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Term>,
    pub if_not_exists: bool,
    pub using_timestamp: Option<i64>,
    pub using_ttl: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub assignments: Vec<(String, Term)>,
    pub where_clauses: Vec<WhereClause>,
    pub if_exists: bool,
    pub using_timestamp: Option<i64>,
    pub using_ttl: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<String>, // empty = delete entire row
    pub where_clauses: Vec<WhereClause>,
    pub if_exists: bool,
    pub using_timestamp: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchType { Logged, Unlogged, Counter }

#[derive(Debug, Clone, PartialEq)]
pub struct BatchStatement {
    pub batch_type: BatchType,
    pub statements: Vec<Statement>,
    pub using_timestamp: Option<i64>,
}

/// CQL type name as written in CREATE TABLE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CqlTypeName {
    Simple(String),                              // text, int, uuid, ...
    List(Box<CqlTypeName>),                      // list<T>
    Set(Box<CqlTypeName>),                       // set<T>
    Map(Box<CqlTypeName>, Box<CqlTypeName>),     // map<K, V>
    Tuple(Vec<CqlTypeName>),                     // tuple<T1, T2, ...>
    Frozen(Box<CqlTypeName>),                    // frozen<T>
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusteringOrder { Asc, Desc }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub keyspace: Option<String>,
    pub name: String,
    pub columns: Vec<(String, CqlTypeName)>,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<(String, ClusteringOrder)>,
    pub if_not_exists: bool,
    pub table_options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub add_columns: Vec<(String, CqlTypeName)>,
    pub drop_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub keyspace: Option<String>,
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateKeyspaceStatement {
    pub name: String,
    pub if_not_exists: bool,
    pub replication: Vec<(String, String)>,
    pub durable_writes: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterKeyspaceStatement {
    pub name: String,
    pub replication: Option<Vec<(String, String)>>,
    pub durable_writes: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropKeyspaceStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UseStatement {
    pub keyspace: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncateStatement {
    pub keyspace: Option<String>,
    pub table: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRoleStatement {
    pub name: String,
    pub if_not_exists: bool,
    pub password: Option<String>,
    pub superuser: Option<bool>,
    pub login: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterRoleStatement {
    pub name: String,
    pub password: Option<String>,
    pub superuser: Option<bool>,
    pub login: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropRoleStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantResource {
    AllKeyspaces,
    Keyspace(String),
    Table(Option<String>, String),
    AllRoles,
    Role(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantStatement {
    pub permissions: Vec<String>,
    pub resource: GrantResource,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeStatement {
    pub permissions: Vec<String>,
    pub resource: GrantResource,
    pub role: String,
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p ferrosa-cql 2>&1 | head -20`
Expected: success (pure data types, no logic to test)

- [ ] **Step 4: Commit**

```bash
git add ferrosa-cql/src/ast.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add AST types for CQL parser"
```

---

### Task 2: Lexer

**Files:**

- Create: `ferrosa-cql/src/lexer.rs`
- Modify: `ferrosa-cql/src/lib.rs`

Mirror the pattern from `ferrosa-graph/src/parser/lexer.rs`: zero-alloc `Lexer<'input>` with `Token<'input>`, `phf` keyword map, `peek()`/`next_token()`/`expect()`/`eat()`.

- [ ] **Step 1: Add module declaration**

Add `pub mod lexer;` to `lib.rs`.

- [ ] **Step 2: Write lexer with keyword map, token types, and core methods**

Create `ferrosa-cql/src/lexer.rs`. Key elements:

```rust
use phf::phf_map;
use crate::error::CqlError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Select, Insert, Update, Delete, Create, Alter, Drop,
    From, Where, And, Or, In, Set, Into, Values,
    If, Exists, Not, Primary, Key, Table, Keyspace, Role,
    Grant, Revoke, On, To, Of, Use, Batch, Begin, Apply,
    Unlogged, Counter, Logged, Truncate, Order, By, Asc, Desc,
    Limit, Allow, Filtering, With, Replication, DurableWrites,
    Password, Superuser, Login, Nosuperuser, Nologin,
    True, False, Null, Using, Timestamp, Ttl,
    Int, Bigint, Text, Varchar, Blob, Boolean, Float, Double,
    Uuid, Timeuuid, Inet, Varint, Decimal, Date, Time,
    Smallint, Tinyint, Ascii,
    List, Map, Tuple, Frozen, Static, Clustering, Compact, Storage,
    Token, Writetime, All, Permissions, Of as OfKw,
}

static KEYWORDS: phf::Map<&'static str, Keyword> = phf_map! {
    "SELECT" => Keyword::Select,
    "INSERT" => Keyword::Insert,
    // ... all keywords ...
};

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind<'input> {
    Keyword(Keyword),
    Ident(&'input str),
    QuotedIdent(String),
    StringLiteral(String),
    IntegerLiteral(i64),
    FloatLiteral(f64),
    UuidLiteral(uuid::Uuid),
    BlobLiteral(Vec<u8>),
    QuestionMark,
    NamedBind(String),     // :name
    Eq, Lt, Gt, LtEq, GtEq, NotEq,
    Plus, Minus, Star,
    LParen, RParen, LBracket, RBracket, LBrace, RBrace,
    Comma, Dot, Semicolon, Colon,
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token<'input> {
    pub kind: TokenKind<'input>,
    pub pos: usize,
}

pub struct Lexer<'input> {
    input: &'input str,
    bytes: &'input [u8],
    pos: usize,
    peeked: Option<Token<'input>>,
}

impl<'input> Lexer<'input> {
    pub fn new(input: &'input str) -> Self;
    pub fn peek(&mut self) -> Result<&Token<'input>, CqlError>;
    pub fn next_token(&mut self) -> Result<Token<'input>, CqlError>;
    pub fn expect(&mut self, expected: &TokenKind<'_>) -> Result<Token<'input>, CqlError>;
    pub fn eat(&mut self, expected: &TokenKind<'_>) -> Result<bool, CqlError>;
    fn advance(&mut self) -> Result<Token<'input>, CqlError>;
    fn skip_whitespace_and_comments(&mut self);
    fn read_identifier(&mut self) -> Token<'input>;
    fn read_number(&mut self) -> Result<Token<'input>, CqlError>;
    fn read_string_literal(&mut self) -> Result<Token<'input>, CqlError>;
    fn read_quoted_identifier(&mut self) -> Result<Token<'input>, CqlError>;
    fn read_hex_blob(&mut self) -> Result<Token<'input>, CqlError>;
}
```

The `advance()` method is the core: skip whitespace/comments, then match on first byte to dispatch to specialized readers. For identifiers, uppercase and look up in `KEYWORDS` phf map. UUID detection: if an identifier matches `[0-9a-fA-F]{8}-...` pattern, parse as UUID.

- [ ] **Step 3: Write lexer unit tests**

Tests in a `#[cfg(test)] mod tests` block inside `lexer.rs`:

```rust
#[test]
fn lex_select_query() {
    let mut lex = Lexer::new("SELECT * FROM users WHERE id = 42");
    assert_eq!(lex.next_token().unwrap().kind, TokenKind::Keyword(Keyword::Select));
    assert_eq!(lex.next_token().unwrap().kind, TokenKind::Star);
    assert_eq!(lex.next_token().unwrap().kind, TokenKind::Keyword(Keyword::From));
    // ...
    assert_eq!(lex.next_token().unwrap().kind, TokenKind::Eof);
}

#[test]
fn lex_string_literal() { /* 'hello world' → StringLiteral("hello world") */ }

#[test]
fn lex_escaped_string() { /* 'it''s' → StringLiteral("it's") */ }

#[test]
fn lex_integer_and_float() { /* 42, 3.14 */ }

#[test]
fn lex_uuid() { /* 550e8400-e29b-41d4-a716-446655440000 */ }

#[test]
fn lex_hex_blob() { /* 0xDEADBEEF → BlobLiteral */ }

#[test]
fn lex_bind_markers() { /* ? and :name */ }

#[test]
fn lex_operators() { /* = < > <= >= != */ }

#[test]
fn lex_keywords_case_insensitive() { /* select, SELECT, Select all → Keyword::Select */ }

#[test]
fn lex_quoted_identifier() { /* "MyTable" → QuotedIdent("MyTable") */ }

#[test]
fn lex_line_comment() { /* -- comment\nSELECT → Keyword::Select */ }

#[test]
fn lex_block_comment() { /* /* comment */SELECT → Keyword::Select */ }

#[test]
fn lex_unterminated_string_error() { /* 'hello → SyntaxError */ }

#[test]
fn lex_empty_input() { /* "" → Eof */ }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-cql lexer -- --nocapture 2>&1 | tail -20`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/lexer.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add zero-alloc CQL lexer with phf keyword map"
```

---

### Task 3: Parser

**Files:**

- Create: `ferrosa-cql/src/parser.rs`
- Modify: `ferrosa-cql/src/lib.rs`

Mirror `ferrosa-graph/src/parser/parse_impl.rs` pattern: one function per grammar rule, LL(2), no backtracking.

- [ ] **Step 1: Add module declaration**

Add `pub mod parser;` to `lib.rs`.

- [ ] **Step 2: Write parser core and SELECT/INSERT/UPDATE/DELETE**

Create `ferrosa-cql/src/parser.rs` with:

```rust
use crate::ast::*;
use crate::error::CqlError;
use crate::lexer::{Keyword, Lexer, Token, TokenKind};

pub fn parse(input: &str) -> Result<Statement, CqlError> {
    let mut parser = Parser::new(input);
    parser.parse_statement()
}

struct Parser<'input> {
    lexer: Lexer<'input>,
}

impl<'input> Parser<'input> {
    fn new(input: &'input str) -> Self;
    fn parse_statement(&mut self) -> Result<Statement, CqlError>;

    // DML
    fn parse_select(&mut self) -> Result<Statement, CqlError>;
    fn parse_insert(&mut self) -> Result<Statement, CqlError>;
    fn parse_update(&mut self) -> Result<Statement, CqlError>;
    fn parse_delete(&mut self) -> Result<Statement, CqlError>;
    fn parse_batch(&mut self) -> Result<Statement, CqlError>;

    // DDL
    fn parse_create(&mut self) -> Result<Statement, CqlError>;
    fn parse_alter(&mut self) -> Result<Statement, CqlError>;
    fn parse_drop(&mut self) -> Result<Statement, CqlError>;
    fn parse_use(&mut self) -> Result<Statement, CqlError>;
    fn parse_truncate(&mut self) -> Result<Statement, CqlError>;
    fn parse_grant(&mut self) -> Result<Statement, CqlError>;
    fn parse_revoke(&mut self) -> Result<Statement, CqlError>;

    // Shared
    fn parse_table_ref(&mut self) -> Result<(Option<String>, String), CqlError>;
    fn parse_where_clauses(&mut self) -> Result<Vec<WhereClause>, CqlError>;
    fn parse_term(&mut self) -> Result<Term, CqlError>;
    fn parse_cql_type_name(&mut self) -> Result<CqlTypeName, CqlError>;
    fn parse_using_clause(&mut self) -> Result<(Option<i64>, Option<i32>), CqlError>;
}
```

Entry point: `parse_statement()` peeks at first token and dispatches:

- `Keyword::Select` → `parse_select()`
- `Keyword::Insert` → `parse_insert()`
- `Keyword::Update` → `parse_update()`
- `Keyword::Delete` → `parse_delete()`
- `Keyword::Create` → `parse_create()` (peek next: Table/Keyspace/Role → parse, else SyntaxError("not yet supported"))
- `Keyword::Alter` → `parse_alter()`
- `Keyword::Drop` → `parse_drop()`
- `Keyword::Use` → `parse_use()`
- `Keyword::Begin` → `parse_batch()`
- `Keyword::Truncate` → `parse_truncate()`
- `Keyword::Grant` → `parse_grant()`
- `Keyword::Revoke` → `parse_revoke()`
- else → SyntaxError

`parse_term()`: match on token kind:

- `StringLiteral(s)` → `Term::StringLiteral(s)`
- `IntegerLiteral(n)` → `Term::IntegerLiteral(n)`
- `FloatLiteral(f)` → `Term::FloatLiteral(f)`
- `UuidLiteral(u)` → `Term::UuidLiteral(u)`
- `BlobLiteral(b)` → `Term::BlobLiteral(b)`
- `Keyword(True)` → `Term::BoolLiteral(true)`
- `Keyword(False)` → `Term::BoolLiteral(false)`
- `Keyword(Null)` → `Term::Null`
- `QuestionMark` → `Term::BindMarker(None)`
- `NamedBind(name)` → `Term::BindMarker(Some(name))`
- `LBracket` → parse list literal `[a, b, c]`
- `LBrace` → parse map `{k:v}` or set `{a, b}` (disambiguate: read first two elements; if colon after first → map)
- `LParen` → parse tuple `(a, b)`

- [ ] **Step 3: Write parser unit tests for DML**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_select_star() {
        let stmt = parse("SELECT * FROM users").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.table, "users");
                assert_eq!(s.columns, vec![SelectColumn::Star]);
                assert!(s.where_clauses.is_empty());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_with_keyspace() {
        let stmt = parse("SELECT id, name FROM ks.users WHERE id = 42").unwrap();
        // verify keyspace = Some("ks"), table = "users", 2 columns, 1 where clause
    }

    #[test]
    fn parse_select_with_order_limit() {
        let stmt = parse("SELECT * FROM events WHERE pk = 1 ORDER BY ts DESC LIMIT 10").unwrap();
    }

    #[test]
    fn parse_insert() {
        let stmt = parse("INSERT INTO users (id, name) VALUES (1, 'alice')").unwrap();
        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.columns, vec!["id", "name"]);
                assert_eq!(i.values.len(), 2);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_insert_if_not_exists() {
        let stmt = parse("INSERT INTO t (k) VALUES (1) IF NOT EXISTS").unwrap();
    }

    #[test]
    fn parse_insert_using_timestamp_ttl() {
        let stmt = parse("INSERT INTO t (k, v) VALUES (1, 'x') USING TIMESTAMP 12345 AND TTL 3600").unwrap();
    }

    #[test]
    fn parse_update() {
        let stmt = parse("UPDATE users SET name = 'bob' WHERE id = 1").unwrap();
    }

    #[test]
    fn parse_delete() {
        let stmt = parse("DELETE FROM users WHERE id = 1").unwrap();
    }

    #[test]
    fn parse_delete_columns() {
        let stmt = parse("DELETE name, email FROM users WHERE id = 1").unwrap();
    }

    #[test]
    fn parse_batch() {
        let stmt = parse("BEGIN BATCH INSERT INTO t (k) VALUES (1); INSERT INTO t (k) VALUES (2); APPLY BATCH").unwrap();
    }

    #[test]
    fn parse_bind_markers() {
        let stmt = parse("INSERT INTO t (k, v) VALUES (?, :val)").unwrap();
    }

    #[test]
    fn parse_collection_literals() {
        let stmt = parse("INSERT INTO t (k, tags) VALUES (1, ['a', 'b'])").unwrap();
    }
}
```

- [ ] **Step 4: Write parser unit tests for DDL**

```rust
    #[test]
    fn parse_create_keyspace() {
        let stmt = parse("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}").unwrap();
    }

    #[test]
    fn parse_create_table() {
        let stmt = parse(
            "CREATE TABLE ks.users (id uuid, name text, age int, PRIMARY KEY (id))"
        ).unwrap();
    }

    #[test]
    fn parse_create_table_composite_key() {
        let stmt = parse(
            "CREATE TABLE t (pk1 text, pk2 int, ck timestamp, v text, PRIMARY KEY ((pk1, pk2), ck)) WITH CLUSTERING ORDER BY (ck DESC)"
        ).unwrap();
    }

    #[test]
    fn parse_alter_table_add() {
        let stmt = parse("ALTER TABLE users ADD email text").unwrap();
    }

    #[test]
    fn parse_drop_table() {
        let stmt = parse("DROP TABLE IF EXISTS ks.users").unwrap();
    }

    #[test]
    fn parse_use() {
        let stmt = parse("USE my_keyspace").unwrap();
    }

    #[test]
    fn parse_truncate() {
        let stmt = parse("TRUNCATE users").unwrap();
    }

    #[test]
    fn parse_create_role() {
        let stmt = parse("CREATE ROLE admin WITH PASSWORD = 'secret' AND SUPERUSER = true AND LOGIN = true").unwrap();
    }

    #[test]
    fn parse_grant() {
        let stmt = parse("GRANT SELECT ON ks.users TO reader").unwrap();
    }

    #[test]
    fn parse_unsupported_returns_error() {
        assert!(parse("CREATE INDEX idx ON t (col)").is_err());
        assert!(parse("CREATE VIEW v AS SELECT * FROM t WHERE k IS NOT NULL PRIMARY KEY (k)").is_err());
    }

    #[test]
    fn parse_syntax_error() {
        assert!(parse("SELECTT * FROM t").is_err());
        assert!(parse("").is_err());
    }
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p ferrosa-cql parser -- --nocapture 2>&1 | tail -30`
Expected: all pass

- [ ] **Step 6: Add proptest for parser safety**

Add to parser tests:

```rust
#[cfg(test)]
mod proptests {
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn parser_never_panics(input in "\\PC{0,200}") {
            let _ = super::parse(&input);
        }
    }
}
```

Run: `cargo test -p ferrosa-cql parser::proptests -- --nocapture`
Expected: pass (parser returns Err, never panics)

- [ ] **Step 7: Commit**

```bash
git add ferrosa-cql/src/parser.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add recursive descent CQL parser with full DML+DDL support"
```

---

## Chunk 2: Bridge + Result Encoder

### Task 4: Update Cargo.toml Dependencies

**Files:**

- Modify: `ferrosa-cql/Cargo.toml`

- [ ] **Step 1: Add new dependencies**

Add to `[dependencies]`:

```toml
ferrosa-sstable = { path = "../ferrosa-sstable" }
moka = { version = "0.12", features = ["sync"] }
indexmap = "2"
```

- [ ] **Step 2: Verify build**

Run: `cargo build -p ferrosa-cql 2>&1 | head -20`
Expected: success

- [ ] **Step 3: Commit**

```bash
git add ferrosa-cql/Cargo.toml
git commit -m "chore(cql): add moka, ferrosa-sstable, indexmap dependencies"
```

---

### Task 5: Bridge

**Files:**

- Create: `ferrosa-cql/src/bridge.rs`
- Modify: `ferrosa-cql/src/lib.rs`

Stateless pure functions converting between protocol types and storage types.

- [ ] **Step 1: Add module declaration**

Add `pub mod bridge;` to `lib.rs`.

- [ ] **Step 2: Write `term_to_cql_value` and `parse_cql_type`**

These are the two functions with no storage dependencies — pure CQL type work.

`term_to_cql_value(term: &Term, target: &CqlType) -> Result<CqlValue, CqlError>`:

- `IntegerLiteral(n)` + `CqlType::Int` → `CqlValue::Int(n as i32)` (range check)
- `IntegerLiteral(n)` + `CqlType::Bigint` → `CqlValue::Bigint(n)`
- `StringLiteral(s)` + `CqlType::Varchar`/`Text`/`Ascii` → `CqlValue::Text(s)` / `CqlValue::Ascii(s)`
- `BoolLiteral(b)` + `CqlType::Boolean` → `CqlValue::Boolean(b)`
- `UuidLiteral(u)` + `CqlType::Uuid`/`Timeuuid` → `CqlValue::Uuid(u)` / `CqlValue::Timeuuid(u)`
- `BlobLiteral(b)` + `CqlType::Blob` → `CqlValue::Blob(b)`
- `FloatLiteral(f)` + `CqlType::Float` → `CqlValue::Float((f as f32).to_bits())`
- `FloatLiteral(f)` + `CqlType::Double` → `CqlValue::Double(f.to_bits())`
- `IntegerLiteral(n)` + `CqlType::Timestamp` → `CqlValue::Timestamp(n)`
- `StringLiteral(s)` + `CqlType::Inet` → parse as `IpAddr`, wrap in `CqlValue::Inet`
- `Null` → return `Ok(CqlValue::Null)` for any type
- `ListLiteral(items)` + `CqlType::List(elem_type)` → recursively convert each item
- `SetLiteral(items)` + `CqlType::Set(elem_type)` → recursively convert
- `MapLiteral(pairs)` + `CqlType::Map(k_type, v_type)` → recursively convert pairs
- Mismatched → `CqlError::Invalid("type mismatch: ...")`

`parse_cql_type(s: &str) -> Result<CqlType, CqlError>`:

- Simple types: `"text"` → `CqlType::Varchar`, `"int"` → `CqlType::Int`, etc.
- Parameterized: `"list<int>"` → `CqlType::List(Box::new(CqlType::Int))`
- Nested: `"frozen<map<text, list<int>>>"` → recursive parse
- Uses a small recursive descent parser on the type string

- [ ] **Step 3: Write tests for term_to_cql_value and parse_cql_type**

```rust
#[test]
fn term_int_to_cql_int() {
    let val = term_to_cql_value(&Term::IntegerLiteral(42), &CqlType::Int).unwrap();
    assert_eq!(val, CqlValue::Int(42));
}

#[test]
fn term_int_to_cql_bigint() {
    let val = term_to_cql_value(&Term::IntegerLiteral(42), &CqlType::Bigint).unwrap();
    assert_eq!(val, CqlValue::Bigint(42));
}

#[test]
fn term_string_to_text() {
    let val = term_to_cql_value(&Term::StringLiteral("hello".into()), &CqlType::Varchar).unwrap();
    assert_eq!(val, CqlValue::Text("hello".into()));
}

#[test]
fn term_type_mismatch() {
    let result = term_to_cql_value(&Term::StringLiteral("hello".into()), &CqlType::Int);
    assert!(result.is_err());
}

#[test]
fn term_null_any_type() {
    assert_eq!(term_to_cql_value(&Term::Null, &CqlType::Int).unwrap(), CqlValue::Null);
}

#[test]
fn term_list_literal() {
    let term = Term::ListLiteral(vec![Term::IntegerLiteral(1), Term::IntegerLiteral(2)]);
    let val = term_to_cql_value(&term, &CqlType::List(Box::new(CqlType::Int))).unwrap();
    assert_eq!(val, CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]));
}

#[test]
fn parse_simple_type() {
    assert_eq!(parse_cql_type("text").unwrap(), CqlType::Varchar);
    assert_eq!(parse_cql_type("int").unwrap(), CqlType::Int);
}

#[test]
fn parse_collection_type() {
    assert_eq!(
        parse_cql_type("list<int>").unwrap(),
        CqlType::List(Box::new(CqlType::Int))
    );
}

#[test]
fn parse_nested_type() {
    assert_eq!(
        parse_cql_type("map<text, list<int>>").unwrap(),
        CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::List(Box::new(CqlType::Int))))
    );
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `cargo test -p ferrosa-cql bridge -- --nocapture 2>&1 | tail -20`

- [ ] **Step 5: Write `build_decorated_key`**

```rust
pub fn build_decorated_key(
    pk_values: &[CqlValue],
    pk_types: &[CqlType],
) -> Result<DecoratedKey, CqlError> {
    if pk_values.len() != pk_types.len() {
        return Err(CqlError::Invalid("partition key component count mismatch".into()));
    }
    let pk_bytes = if pk_values.len() == 1 {
        pk_values[0].encode_value()
    } else {
        // Composite: [2-byte len][value bytes][0x00] per component
        let mut buf = Vec::new();
        for val in pk_values {
            let encoded = val.encode_value();
            buf.extend_from_slice(&(encoded.len() as u16).to_be_bytes());
            buf.extend_from_slice(&encoded);
            buf.push(0x00);
        }
        buf
    };
    Ok(DecoratedKey::new(PartitionKey::new(pk_bytes)))
}
```

- [ ] **Step 6: Write `build_row` and `build_delete_row`**

`build_row`: For each (column_name, value) pair, look up column index from `column_map`, encode value via `CqlValue::encode_value()`, wrap in `CellValue::live()` or `CellValue::expiring()`. Build clustering bytes from clustering column values (length-prefixed concatenation). Return `Row`.

`build_delete_row`: Produces tombstone cells or row-level deletion.

- [ ] **Step 7: Write `partition_to_rows`**

Converts a `Partition` from storage into `(Vec<String>, Vec<CqlType>, Vec<Vec<Option<CqlValue>>>)` for the result encoder. For each row in `partition.rows`, decode each cell by column index, skip tombstones (return None).

- [ ] **Step 8: Write bridge integration tests**

```rust
#[test]
fn build_decorated_key_single_column() {
    let key = build_decorated_key(
        &[CqlValue::Int(42)],
        &[CqlType::Int],
    ).unwrap();
    assert_eq!(key.key.as_bytes(), &42i32.to_be_bytes());
}

#[test]
fn build_decorated_key_composite() {
    let key = build_decorated_key(
        &[CqlValue::Text("hello".into()), CqlValue::Int(1)],
        &[CqlType::Varchar, CqlType::Int],
    ).unwrap();
    // Verify composite format: [2-byte len][bytes][0x00] per component
    let bytes = key.key.as_bytes();
    assert_eq!(&bytes[0..2], &5u16.to_be_bytes()); // "hello" length
}

#[test]
fn build_row_simple() {
    // Build a row with one int column and verify CellValue::live encoding
}

#[test]
fn build_delete_row_tombstone() {
    // Build a delete row for specific columns, verify CellValue::tombstone cells
}

#[test]
fn build_delete_row_partition_level() {
    // Build a partition-level delete, verify DeletionTime is set
}

#[test]
fn partition_to_rows_roundtrip() {
    // Build a partition manually, convert to rows, verify values match
}
```

- [ ] **Step 9: Run all bridge tests**

Run: `cargo test -p ferrosa-cql bridge -- --nocapture`
Expected: all pass

- [ ] **Step 10: Commit**

```bash
git add ferrosa-cql/src/bridge.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add bridge for CqlValue/CellValue conversion and key serialization"
```

---

### Task 6: Result Encoder

**Files:**

- Create: `ferrosa-cql/src/result.rs`
- Modify: `ferrosa-cql/src/lib.rs`

- [ ] **Step 1: Add module declaration**

Add `pub mod result;` to `lib.rs`.

- [ ] **Step 2: Write result encoding functions**

```rust
use bytes::{BufMut, BytesMut};
use crate::types::{CqlType, CqlValue};

/// Encode a Void result (e.g., after INSERT/UPDATE/DELETE).
pub fn encode_void() -> BytesMut;

/// Encode a SetKeyspace result (after USE statement).
pub fn encode_set_keyspace(keyspace: &str) -> BytesMut;

/// Encode a SchemaChange result (after DDL).
pub fn encode_schema_change(change_type: &str, target: &str, options: &[&str]) -> BytesMut;

/// Encode a Rows result with column metadata and row data.
pub fn encode_rows(
    column_names: &[String],
    column_types: &[CqlType],
    keyspace: &str,
    table: &str,
    rows: &[Vec<Option<CqlValue>>],
) -> BytesMut;

/// Encode a Prepared result with statement ID and metadata.
pub fn encode_prepared(
    id: &[u8; 16],
    result_column_names: &[String],
    result_column_types: &[CqlType],
    bound_names: &[String],
    bound_types: &[CqlType],
    keyspace: &str,
    table: &str,
) -> BytesMut;

// Private helpers
fn encode_string(buf: &mut BytesMut, s: &str);
fn encode_column_spec(buf: &mut BytesMut, ks: &str, table: &str, name: &str, cql_type: &CqlType);
fn encode_type_id(buf: &mut BytesMut, cql_type: &CqlType);
```

Result kind codes: Void=0x0001, Rows=0x0002, SetKeyspace=0x0003, Prepared=0x0004, SchemaChange=0x0005.

Rows format: `[int kind=0x0002][int flags][int col_count][col_specs...][int row_count][row_data...]`
Each column spec: `[string ks][string table][string name][short type_id][type_params]`
Each cell: `[int byte_length][bytes]` or `[int -1]` for null.

- [ ] **Step 3: Write tests**

```rust
#[test]
fn encode_void_result() {
    let buf = encode_void();
    assert_eq!(&buf[0..4], &0x0001i32.to_be_bytes());
}

#[test]
fn encode_set_keyspace_result() {
    let buf = encode_set_keyspace("my_ks");
    assert_eq!(&buf[0..4], &0x0003i32.to_be_bytes());
    // Verify keyspace string follows
}

#[test]
fn encode_rows_single_int_column() {
    let buf = encode_rows(
        &["id".into()],
        &[CqlType::Int],
        "ks",
        "users",
        &[vec![Some(CqlValue::Int(42))]],
    );
    assert_eq!(&buf[0..4], &0x0002i32.to_be_bytes());
    // Verify metadata and row data
}

#[test]
fn encode_rows_null_cell() {
    let buf = encode_rows(
        &["v".into()],
        &[CqlType::Varchar],
        "ks", "t",
        &[vec![None]],
    );
    // Verify null is encoded as -1 length
}

#[test]
fn encode_schema_change_created() {
    let buf = encode_schema_change("CREATED", "TABLE", &["ks", "users"]);
    assert_eq!(&buf[0..4], &0x0005i32.to_be_bytes());
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-cql result -- --nocapture`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/result.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add RESULT frame body encoder (Rows, Void, Prepared, SchemaChange)"
```

---

## Chunk 3: Prepared Cache + Router + Connection Handler

### Task 7: Prepared Statement Cache

**Files:**

- Create: `ferrosa-cql/src/prepared.rs`
- Modify: `ferrosa-cql/src/lib.rs`

- [ ] **Step 1: Add module declaration**

Add `pub mod prepared;` to `lib.rs`.

- [ ] **Step 2: Write PreparedCache and PreparedPlan**

```rust
use std::sync::Arc;
use md5::{Md5, Digest};
use moka::sync::Cache;
use crate::ast::Statement;
use crate::types::CqlType;

#[derive(Debug, Clone)]
pub struct PreparedPlan {
    pub id: [u8; 16],
    pub query: String,
    pub statement: Statement,
    pub keyspace: Option<String>,
    pub result_columns: Vec<(String, CqlType)>,  // name, type for result metadata
    pub bound_columns: Vec<(String, CqlType)>,    // name, type for bind markers
    pub table_keyspace: String,
    pub table_name: String,
}

pub struct PreparedCache {
    cache: Cache<[u8; 16], Arc<PreparedPlan>>,
}

impl PreparedCache {
    pub fn new(max_weight: u64) -> Self {
        let cache = Cache::builder()
            .weigher(|_key: &[u8; 16], value: &Arc<PreparedPlan>| -> u32 {
                (value.query.len() + std::mem::size_of::<Statement>() + 128) as u32
            })
            .max_capacity(max_weight)
            .build();
        Self { cache }
    }

    pub fn get(&self, id: &[u8; 16]) -> Option<Arc<PreparedPlan>> {
        self.cache.get(id)
    }

    pub fn insert(&self, plan: PreparedPlan) {
        let id = plan.id;
        self.cache.insert(id, Arc::new(plan));
    }

    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    pub fn compute_id(query: &str) -> [u8; 16] {
        let mut hasher = Md5::new();
        hasher.update(query.as_bytes());
        let result = hasher.finalize();
        let mut id = [0u8; 16];
        id.copy_from_slice(&result);
        id
    }
}
```

- [ ] **Step 3: Write tests**

```rust
#[test]
fn compute_id_deterministic() {
    let id1 = PreparedCache::compute_id("SELECT * FROM users");
    let id2 = PreparedCache::compute_id("SELECT * FROM users");
    assert_eq!(id1, id2);
}

#[test]
fn insert_and_get() {
    let cache = PreparedCache::new(10 * 1024 * 1024);
    let plan = PreparedPlan { /* ... */ };
    let id = plan.id;
    cache.insert(plan);
    assert!(cache.get(&id).is_some());
}

#[test]
fn get_missing_returns_none() {
    let cache = PreparedCache::new(10 * 1024 * 1024);
    assert!(cache.get(&[0u8; 16]).is_none());
}

#[test]
fn invalidate_all_clears() {
    let cache = PreparedCache::new(10 * 1024 * 1024);
    // insert, invalidate_all, get returns None
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-cql prepared -- --nocapture`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/prepared.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add prepared statement cache with moka W-TinyLFU"
```

---

### Task 8: Router

**Files:**

- Create: `ferrosa-cql/src/router.rs`
- Modify: `ferrosa-cql/src/lib.rs`

This is the largest and most complex task. It ties together parser output, bridge, schema, storage, prepared cache, system queries, and result encoding.

- [ ] **Step 1: Add module declaration**

Add `pub mod router;` to `lib.rs`.

- [ ] **Step 2: Write SharedState and RequestContext**

```rust
use std::sync::Arc;
use ferrosa_schema::Schema;
use ferrosa_schema::system::local::NodeConfig;
use ferrosa_schema::system::peers::ClusterState;
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_storage::StorageEngine;
use crate::prepared::PreparedCache;

pub struct SharedState {
    pub engine: Arc<StorageEngine>,
    pub schema: Arc<Schema>,
    pub node_config: Arc<NodeConfig>,
    pub cluster_state: Arc<dyn ClusterState>,
    pub prepared_cache: Arc<PreparedCache>,
}

pub struct RequestContext<'a> {
    pub auth: &'a AuthContext,
    pub current_keyspace: &'a Option<String>,
}

/// Stub for single-node mode.
pub struct SingleNodeClusterState;
impl ClusterState for SingleNodeClusterState {
    fn peers(&self) -> Vec<ferrosa_schema::system::peers::PeerInfo> { vec![] }
}
```

- [ ] **Step 3: Write the `route` dispatch function**

The router returns `RouteResult` instead of raw `BytesMut` so the connection
handler can extract side-effects (USE sets the connection-local keyspace):

```rust
pub enum RouteResult {
    /// Encoded RESULT frame body ready to send.
    Result(BytesMut),
    /// USE statement: keyspace name + encoded SetKeyspace body.
    SetKeyspace(String, BytesMut),
}

pub fn route(
    state: &SharedState,
    ctx: &RequestContext<'_>,
    stmt: Statement,
) -> Result<RouteResult, CqlError> {
    match stmt {
        Statement::Select(s) => route_select(state, ctx, s).map(RouteResult::Result),
        Statement::Insert(i) => route_insert(state, ctx, i).map(RouteResult::Result),
        Statement::Update(u) => route_update(state, ctx, u).map(RouteResult::Result),
        Statement::Delete(d) => route_delete(state, ctx, d).map(RouteResult::Result),
        Statement::Batch(b) => route_batch(state, ctx, b).map(RouteResult::Result),
        Statement::CreateKeyspace(ck) => route_create_keyspace(state, ctx, ck).map(RouteResult::Result),
        Statement::CreateTable(ct) => route_create_table(state, ctx, ct).map(RouteResult::Result),
        Statement::DropTable(dt) => route_drop_table(state, ctx, dt).map(RouteResult::Result),
        Statement::DropKeyspace(dk) => route_drop_keyspace(state, ctx, dk).map(RouteResult::Result),
        Statement::AlterKeyspace(ak) => route_alter_keyspace(state, ctx, ak).map(RouteResult::Result),
        Statement::AlterTable(at) => route_alter_table(state, ctx, at).map(RouteResult::Result),
        Statement::CreateRole(cr) => route_create_role(state, ctx, cr).map(RouteResult::Result),
        Statement::AlterRole(ar) => route_alter_role(state, ctx, ar).map(RouteResult::Result),
        Statement::DropRole(dr) => route_drop_role(state, ctx, dr).map(RouteResult::Result),
        Statement::Grant(g) => route_grant(state, ctx, g).map(RouteResult::Result),
        Statement::Revoke(r) => route_revoke(state, ctx, r).map(RouteResult::Result),
        Statement::Use(u) => {
            let body = result::encode_set_keyspace(&u.keyspace);
            Ok(RouteResult::SetKeyspace(u.keyspace, body))
        }
        Statement::Truncate(t) => route_truncate(state, ctx, t).map(RouteResult::Result),
    }
}
```

**System table dispatch** inside `route_select`: detect system keyspaces by
name and dispatch to the appropriate query function:

```rust
fn route_select(state: &SharedState, ctx: &RequestContext<'_>, s: SelectStatement) -> Result<BytesMut, CqlError> {
    let ks = s.keyspace.as_deref()
        .or(ctx.current_keyspace.as_deref())
        .ok_or_else(|| CqlError::Invalid("no keyspace specified".into()))?;

    match (ks, s.table.as_str()) {
        ("system", "local") => {
            let info = ferrosa_schema::system::local::query_local(&state.schema, &state.node_config);
            // Convert LocalInfo fields to result rows
            encode_system_rows(/* ... */)
        }
        ("system", "peers" | "peers_v2") => {
            let peers = ferrosa_schema::system::peers::query_peers(&state.schema, state.cluster_state.as_ref());
            encode_system_rows(/* ... */)
        }
        ("system_schema", "keyspaces") => {
            let snap = state.schema.snapshot();
            let rows = ferrosa_schema::system::schema_tables::query_keyspaces(&snap);
            encode_system_rows(/* ... */)
        }
        ("system_schema", "tables") => {
            let snap = state.schema.snapshot();
            let rows = ferrosa_schema::system::schema_tables::query_tables(&snap);
            encode_system_rows(/* ... */)
        }
        ("system_schema", "columns") => {
            let snap = state.schema.snapshot();
            let rows = ferrosa_schema::system::schema_tables::query_columns(&snap);
            encode_system_rows(/* ... */)
        }
        ("system_auth", "roles") => {
            let snap = state.schema.snapshot();
            let rows = ferrosa_schema::system::auth_tables::query_roles(&snap, ctx.auth);
            encode_system_rows(/* ... */)
        }
        ("system_auth", "role_members") => {
            let snap = state.schema.snapshot();
            let rows = ferrosa_schema::system::auth_tables::query_role_members(&snap);
            encode_system_rows(/* ... */)
        }
        ("system_auth", "role_permissions") => {
            let snap = state.schema.snapshot();
            let rows = ferrosa_schema::system::auth_tables::query_role_permissions(&snap);
            encode_system_rows(/* ... */)
        }
        _ => {
            // User table: use bridge + storage engine
            route_select_user_table(state, ctx, ks, &s)
        }
    }
}
```

Each `route_*` function:

1. Resolves table/keyspace (explicit or from `ctx.current_keyspace`)
2. Gets schema snapshot via `state.schema.snapshot()`
3. Looks up `TableMetadata`
4. For SELECT on system tables: dispatches to appropriate `query_*()` function, converts result structs to `Vec<Vec<Option<CqlValue>>>`, calls `result::encode_rows()`
5. For DML on user tables: uses bridge functions to build storage types, calls engine
6. For DDL: calls schema methods, then `table_metadata.to_storage_schema()` + `engine.register_table()` for CREATE TABLE

Key helper:

```rust
fn resolve_table<'a>(
    snap: &'a Arc<ferrosa_schema::registry::SchemaSnapshot>,
    keyspace: &Option<String>,
    table: &str,
    current_keyspace: &Option<String>,
) -> Result<(&'a str, &'a ferrosa_schema::metadata::table::TableMetadata), CqlError> {
    let ks = keyspace.as_deref()
        .or(current_keyspace.as_deref())
        .ok_or_else(|| CqlError::Invalid("no keyspace specified".into()))?;
    let meta = snap.tables.get(&(ks.to_string(), table.to_string()))
        .ok_or_else(|| CqlError::Invalid(format!("table {}.{} not found", ks, table)))?;
    Ok((ks, meta))
}
```

- [ ] **Step 4: Write router integration tests**

These need real `Schema` and `StorageEngine` instances:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (SharedState, TempDir) {
        let dir = TempDir::new().unwrap();
        let engine_config = StorageEngineConfig { /* ... with dir.path() ... */ };
        let engine = Arc::new(StorageEngine::new(engine_config, None).unwrap());
        let schema_config = SchemaConfig { /* dev mode, no auth */ };
        let schema = Arc::new(Schema::new(schema_config).unwrap());
        let node_config = Arc::new(NodeConfig { /* test defaults */ });
        let prepared_cache = Arc::new(PreparedCache::new(10 * 1024 * 1024));
        let cluster_state = Arc::new(SingleNodeClusterState);
        let state = SharedState { engine, schema, node_config, cluster_state, prepared_cache };
        (state, dir)
    }

    fn dev_auth() -> AuthContext {
        AuthContext { role: "cassandra".into(), is_superuser: true, must_change_password: false }
    }

    #[test]
    fn create_keyspace_then_table_then_insert_then_select() {
        let (state, _dir) = setup();
        let ctx = RequestContext { auth: &dev_auth(), current_keyspace: &None };

        // CREATE KEYSPACE
        let stmt = crate::parser::parse("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'}").unwrap();
        route(&state, &ctx, stmt).unwrap();

        // CREATE TABLE
        let stmt = crate::parser::parse("CREATE TABLE ks.users (id int PRIMARY KEY, name text)").unwrap();
        route(&state, &ctx, stmt).unwrap();

        // INSERT
        let stmt = crate::parser::parse("INSERT INTO ks.users (id, name) VALUES (1, 'alice')").unwrap();
        route(&state, &ctx, stmt).unwrap();

        // SELECT
        let stmt = crate::parser::parse("SELECT * FROM ks.users WHERE id = 1").unwrap();
        let result = route(&state, &ctx, stmt).unwrap();
        // Verify result contains Rows with 1 row, id=1, name='alice'
        assert_eq!(&result[0..4], &0x0002i32.to_be_bytes()); // Rows kind
    }

    #[test]
    fn use_sets_default_keyspace() {
        let (state, _dir) = setup();
        let ctx = RequestContext { auth: &dev_auth(), current_keyspace: &None };
        let stmt = crate::parser::parse("USE my_ks").unwrap();
        let result = route(&state, &ctx, stmt).unwrap();
        assert_eq!(&result[0..4], &0x0003i32.to_be_bytes()); // SetKeyspace kind
    }

    #[test]
    fn select_system_local() {
        let (state, _dir) = setup();
        let ctx = RequestContext { auth: &dev_auth(), current_keyspace: &None };
        let stmt = crate::parser::parse("SELECT * FROM system.local").unwrap();
        let result = route(&state, &ctx, stmt).unwrap();
        assert_eq!(&result[0..4], &0x0002i32.to_be_bytes()); // Rows kind
    }

    #[test]
    fn no_keyspace_returns_invalid() {
        let (state, _dir) = setup();
        let ctx = RequestContext { auth: &dev_auth(), current_keyspace: &None };
        let stmt = crate::parser::parse("SELECT * FROM users WHERE id = 1").unwrap();
        assert!(route(&state, &ctx, stmt).is_err());
    }
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p ferrosa-cql router -- --nocapture`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add ferrosa-cql/src/router.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add query router with full DML/DDL/system query dispatch"
```

---

### Task 9: Connection Handler

**Files:**

- Modify: `ferrosa-cql/src/connection.rs`
- Modify: `ferrosa-cql/src/server.rs`

- [ ] **Step 1: Update server.rs to use SharedState**

Modify `CqlServer` to hold `SharedState`. Update `new()` to accept `SharedState`. Pass clone of `SharedState` to each connection task.

```rust
pub struct CqlServer {
    config: ServerConfig,
    state: Arc<SharedState>,
    active_connections: Arc<AtomicUsize>,
}

impl CqlServer {
    pub fn new(config: ServerConfig, state: Arc<SharedState>) -> Self {
        Self {
            config,
            state,
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }
    // start_background() passes state.clone() to handle_connection
}
```

Update `start_background()` to clone `self.state` into the spawned task:

```rust
// Inside start_background(), the accept loop spawns:
let state = self.state.clone();
let config = self.config.clone();
tokio::spawn(async move {
    handle_connection(stream, peer, state, &config).await;
    // decrement active_connections on drop
});
```

Update **existing server.rs unit tests** — both `server_accepts_connection` and
`server_rejects_over_limit_with_overloaded` call `CqlServer::new(config)`.
Add a test helper:

```rust
#[cfg(test)]
fn test_shared_state(dir: &std::path::Path) -> Arc<SharedState> {
    use ferrosa_storage::{StorageEngine, StorageEngineConfig};
    use ferrosa_schema::{Schema, SchemaConfig};
    // Minimal configs for testing
    let engine_config = StorageEngineConfig { data_dir: dir.to_path_buf(), /* defaults */ };
    let engine = Arc::new(StorageEngine::new(engine_config, None).unwrap());
    let schema_config = SchemaConfig { /* dev mode, no auth, log sink */ };
    let schema = Arc::new(Schema::new(schema_config).unwrap());
    let node_config = Arc::new(NodeConfig {
        cluster_name: "Test".into(),
        data_center: "dc1".into(),
        rack: "rack1".into(),
        rpc_port: 9042,
        host_id: uuid::Uuid::new_v4(),
        listen_address: "127.0.0.1".parse().unwrap(),
        listen_port: 7000,
        broadcast_address: "127.0.0.1".parse().unwrap(),
        broadcast_port: 7000,
        rpc_address: "127.0.0.1".parse().unwrap(),
        tokens: vec![],
    });
    Arc::new(SharedState {
        engine,
        schema,
        node_config,
        cluster_state: Arc::new(SingleNodeClusterState),
        prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
    })
}
```

Update both tests to use `CqlServer::new(config, test_shared_state(dir.path()))`.

- [ ] **Step 2: Rewrite connection.rs**

Replace the stub with the full protocol handler:

```rust
pub async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<SharedState>,
    config: &ServerConfig,
) {
    let codec = CqlCodec::new(config.max_frame_size);
    let mut framed = Framed::new(stream, codec);
    let mut conn_state = ConnectionState {
        auth_context: None,
        current_keyspace: None,
        auth_attempts: 0,
    };

    while let Some(result) = framed.next().await {
        match result {
            Ok(frame) => {
                let stream_id = frame.header.stream_id;
                let response = handle_frame(&state, &mut conn_state, &config, frame);
                match response {
                    HandleResult::Send(opcode, body) => {
                        send_response(&mut framed, stream_id, opcode, body).await;
                    }
                    HandleResult::Close => break,
                }
            }
            Err(e) => {
                // Protocol error → close connection
                break;
            }
        }
    }
}
```

`handle_frame()` returns `HandleResult` (either `Send(Opcode, BytesMut)` or
`Close`). It dispatches by opcode:

- **Startup**: validate CQL_VERSION, return READY or AUTHENTICATE
- **Options**: return SUPPORTED
- **AuthResponse**: parse SASL, authenticate via schema, return AUTH_SUCCESS or ERROR
- **Query**: decode query string from body, parse, call `route()`. If
  `RouteResult::SetKeyspace(ks, body)`, set `conn_state.current_keyspace = Some(ks)`
  then return `Send(Result, body)`. If `RouteResult::Result(body)`, return
  `Send(Result, body)`.
- **Prepare**: parse query, build PreparedPlan, cache, return Prepared result
- **Execute**: decode prepared ID + bound values from body, lookup, bind, route
- **Batch**: decode batch from body, parse/lookup each statement, execute
- **Register**: accept, return READY (event push is deferred)

Query body decoding: `[long string query][short consistency][byte flags]...`
Execute body decoding: `[short id_len][bytes id][short consistency][byte flags]...`

- [ ] **Step 3: Un-ignore handshake tests**

In `ferrosa-cql/tests/handshake.rs`, remove `#[ignore]` from all 4 tests.
Update the `test_config` helper to also return an `Arc<SharedState>` using
the same `test_shared_state()` pattern from server.rs tests. Update test
setup to pass the shared state to `CqlServer::new(config, state)`.
Each test needs a `TempDir` for the storage engine data directory.

- [ ] **Step 4: Write connection integration tests**

Add new tests in `tests/handshake.rs` or a new `tests/integration.rs`:

```rust
#[tokio::test]
async fn query_creates_keyspace_and_table_and_inserts_and_selects() {
    // Start server with real SharedState
    // Connect via TCP
    // Send STARTUP → READY
    // Send QUERY(CREATE KEYSPACE ...) → RESULT(SchemaChange)
    // Send QUERY(CREATE TABLE ...) → RESULT(SchemaChange)
    // Send QUERY(INSERT INTO ...) → RESULT(Void)
    // Send QUERY(SELECT ...) → RESULT(Rows) with expected data
}

#[tokio::test]
async fn prepare_and_execute() {
    // PREPARE "INSERT INTO ks.t (k, v) VALUES (?, ?)" → Prepared result with ID
    // EXECUTE with ID + bound values → Void result
}

#[tokio::test]
async fn stream_id_preserved() {
    // Send query with stream_id=42, verify response has stream_id=42
}
```

- [ ] **Step 5: Run all tests**

Run: `cargo test -p ferrosa-cql -- --nocapture`
Expected: all pass (including un-ignored handshake tests)

- [ ] **Step 6: Run clippy**

Run: `cargo clippy -p ferrosa-cql --all-targets 2>&1 | tail -20`
Expected: no warnings

- [ ] **Step 7: Commit**

```bash
git add ferrosa-cql/src/connection.rs ferrosa-cql/src/server.rs ferrosa-cql/tests/
git commit -m "feat(cql): replace connection stub with full protocol handler

Implements STARTUP, OPTIONS, AUTH, QUERY, PREPARE, EXECUTE, BATCH,
and REGISTER opcodes. Un-ignores handshake tests. End-to-end
CREATE→INSERT→SELECT works via cqlsh."
```

---

## Chunk 4: Full Crate Verification

### Task 10: Full Build + Test + Clippy

- [ ] **Step 1: Run full workspace build**

Run: `cargo build --workspace 2>&1 | tail -20`
Expected: success

- [ ] **Step 2: Run full workspace tests**

Run: `cargo test --workspace 2>&1 | tail -40`
Expected: all pass

- [ ] **Step 3: Run clippy on workspace**

Run: `cargo clippy --workspace --all-targets 2>&1 | tail -20`
Expected: no warnings

- [ ] **Step 4: Run fmt check**

Run: `cargo fmt --check 2>&1`
Expected: clean

- [ ] **Step 5: Final commit if any fixups**

```bash
git add -A
git commit -m "chore(cql): fix clippy warnings and formatting"
```

---

## Summary

| Task | Component | Est. Steps | Dependencies |
|------|-----------|-----------|--------------|
| 1 | AST types | 4 | None |
| 2 | Lexer | 5 | ast |
| 3 | Parser | 7 | ast, lexer |
| 4 | Cargo.toml | 3 | None |
| 5 | Bridge | 10 | types, Cargo.toml |
| 6 | Result encoder | 5 | types |
| 7 | Prepared cache | 5 | ast, Cargo.toml |
| 8 | Router | 6 | parser, bridge, result, prepared |
| 9 | Connection handler | 7 | router, frame, auth |
| 10 | Full verification | 5 | All |

Tasks 1-3 (Chunk 1) and Tasks 4-6 (Chunk 2) can be parallelized across subagents since they have no cross-dependencies until the Router (Task 8) ties them together.
