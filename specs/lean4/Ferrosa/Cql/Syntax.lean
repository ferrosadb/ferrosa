/-
  Ferrosa formal specification — CQL front end, part 1: abstract syntax.
  =====================================================================

  Scope, stated up front.  This is the front end's **semantic** surface: the
  shape of a statement, what it reads, what it writes, what it conditions on,
  and which consistency level it was sent with.  It is *not* a grammar and not a
  parser model.  `ferrosa-cql/src/parser.rs` is 220 KB of recursive descent; its
  correctness is a question about a concrete program, best answered by
  differential testing against this AST (`Conformance.lean`), not by a proof that
  some grammar recognises exactly CQL.

  What is formalised is what the rest of the system's guarantees depend on:

    * which statements read, which write, which do both (LWT);
    * where a statement's consistency level comes from (frame or keyspace
      default) — because `Topology/ConsistencyLevels.lean` says what an ack means
      only once the level is known;
    * how a sequence of statements becomes one transaction with a read set, a
      write set and a condition — which is what `Accord.Protocol` consumes.

  Two modelling notes:

    * a partition key is an opaque `Key` (a token).  `transaction_keys.rs` turns a
      `Term` list into one; the front end's job is only to say *which* terms are
      partition keys, so token hashing stays out of the spec;
    * `Stmt` is flat and `Statement` adds the batch wrapper, because **CQL does
      not allow nested batches**.  Splitting them this way is faithful to the
      grammar and also keeps the classification functions structurally
      recursive, so their properties are provable rather than `partial`.

  Rust shapes mirrored (`ferrosa-cql/src/ast.rs`): `Statement` (line 45),
  `SelectStatement`, `InsertStatement`, `UpdateStatement`, `DeleteStatement`,
  `BatchStatement`, `Term`, `WhereClause`, `IfCondition`, `Assignment`,
  `BatchType`.
-/
import Ferrosa.Core

namespace Ferrosa.Cql

open Ferrosa

/-- A partition key, as the transaction layer sees it: the token computed from a
    statement's partition-key terms (`ferrosa-cluster/src/transaction_keys.rs`). -/
abbrev Key := Nat

/-- A term.  Semantically the only distinction that matters is whether the value
    is known at plan time, because that decides whether a statement can be routed
    and acknowledged without a read. -/
inductive Term where
  | literal (v : Nat)
  | bind (idx : Nat)
  | null
  /-- `c = c + {v}` — a collection append.  Reads the target column to compute the
      new value, which is why it cannot be a blind write. -/
  | selfAppend (v : Nat)
  /-- `c = c - {v}` — a collection removal. -/
  | selfRemove (v : Nat)
  deriving DecidableEq, Repr

/-- `WHERE col <op> term`. -/
structure WhereClause where
  col : String
  op : String
  term : Term
  deriving DecidableEq, Repr

/-- `IF col <op> term` — the LWT condition (`if_conditions` in `ast.rs`). -/
structure IfCondition where
  col : String
  op : String
  term : Term
  deriving DecidableEq, Repr

/-- `SET col = term`. -/
structure Assignment where
  col : String
  term : Term
  deriving DecidableEq, Repr

inductive BatchType where
  | logged
  | unlogged
  | counter
  deriving DecidableEq, Repr

/-- One statement.  Flat: this type cannot contain a batch, matching CQL. -/
inductive Stmt where
  | select (table : String) (where_ : List WhereClause)
  | insert (table : String) (values : List Assignment) (ifNotExists : Bool)
  | update (table : String) (assigns : List Assignment) (where_ : List WhereClause)
      (ifExists : Bool) (conds : List IfCondition)
  | delete (table : String) (where_ : List WhereClause) (ifExists : Bool)
      (conds : List IfCondition)
  | beginTxn
  | commit
  | rollback
  /-- Everything that does not route through the coordinator's write path: DDL,
      roles, indexes, CDC, functions, … -/
  | other
  deriving DecidableEq, Repr

/-- A query: one statement, or one batch of them. -/
inductive Statement where
  | single (s : Stmt)
  | batch (kind : BatchType) (stmts : List Stmt)
  deriving DecidableEq, Repr

/-- One query as the front end hands it to the coordinator: a statement plus the
    consistency level carried in the frame.  CQL puts CL in the QUERY/EXECUTE
    frame header, not in the statement text, so `none` means "no level in the
    frame" and the session/keyspace default applies. -/
structure Query where
  stmt : Statement
  cl : Option ConsistencyLevel
  deriving DecidableEq, Repr

/-! ## Classification

The questions the rest of the spec has to be able to ask of a statement.  Each is
a `Bool`, so a corpus can be tabulated by `decide` in `Conformance.lean`. -/

/-- Reads the keyspace (directly, or as the read half of an LWT). -/
def Stmt.reads : Stmt → Bool
  | .select _ _ => true
  | .insert _ _ ine => ine        -- IF NOT EXISTS reads the partition
  | .update _ _ _ _ _ => true     -- LWT reads its condition
  | .delete _ _ _ _ => true       -- IF EXISTS likewise
  | _ => false

/-- Writes the keyspace. -/
def Stmt.writes : Stmt → Bool
  | .select _ _ => false
  | .insert _ _ _ => true
  | .update _ _ _ _ _ => true
  | .delete _ _ _ _ => true
  | _ => false

/-- Carries an `IF` condition: a compare-and-set rather than a blind mutation
    (`if_not_exists` / `if_exists` / `if_conditions`). -/
def Stmt.conditional : Stmt → Bool
  | .insert _ _ ine => ine
  | .update _ _ _ ie cs => ie || !cs.isEmpty
  | .delete _ _ ie cs => ie || !cs.isEmpty
  | _ => false

/-- Control statements that delimit a transaction. -/
def Stmt.isTxnControl : Stmt → Bool
  | .beginTxn => true
  | .commit => true
  | .rollback => true
  | _ => false

/-- A blind write: it writes, and nothing it does depends on prior state.  These
    are the statements that may take Accord's fast path without a read phase. -/
def Stmt.blind : Stmt → Bool
  | .select _ _ => false
  | .insert _ _ ine => !ine
  | s => !s.conditional && s.writes

def Statement.reads : Statement → Bool
  | .single s => s.reads
  | .batch _ ss => ss.any Stmt.reads

def Statement.writes : Statement → Bool
  | .single s => s.writes
  | .batch _ ss => ss.any Stmt.writes

def Statement.conditional : Statement → Bool
  | .single s => s.conditional
  | .batch _ ss => ss.any Stmt.conditional

def Statement.isTxnControl : Statement → Bool
  | .single s => s.isTxnControl
  | .batch _ _ => false

def Statement.blind : Statement → Bool
  | .single s => s.blind
  | .batch _ ss => ss.all Stmt.blind

end Ferrosa.Cql
