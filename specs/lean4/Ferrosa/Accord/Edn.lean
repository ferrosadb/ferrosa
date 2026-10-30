/-
  Ferrosa formal specification — Accord, part 6: reading real Jepsen histories.
  ==========================================================================

  A checker that only reads hand-built histories cannot be pointed at a run, and the EDN
  the `ferrosa-jepsen` Elle generator writes is the form a run actually produces — parsed
  here with no Clojure runtime and no JSON, into the `History` that
  `Accord/Serializable.lean` scores.  Two limitations are visible in every verdict it
  produces: a transaction is identified by its process and invoke ordinal, because the EDN
  carries no transaction id, and the recorded invoke and complete times are zero, so a
  parsed history can be judged serializable but never strictly serializable.  `:fail` and
  `:info` events are kept but held out of the serial order, matching how an unknown outcome
  is treated in `History.committed`.
-/
import Ferrosa.Accord.Strict

namespace Ferrosa.Accord

/-! ## A minimal reader for the dialect the generator writes -/

inductive Sexp where
  | atom (s : String)
  | vec (xs : List Sexp)
  | map (kvs : List (String × Sexp))
  deriving Repr

namespace Edn

def isWs (c : Char) : Bool :=
  c = ' ' || c = '\n' || c = '\t' || c = '\r' || c = ','

partial def skipWs : List Char → List Char
  | c :: cs => if isWs c then skipWs cs else c :: cs
  | [] => []

partial def readAtom (acc : List Char) : List Char → String × List Char
  | [] => (String.mk acc.reverse, [])
  | c :: cs =>
      if isWs c || c = ']' || c = '}' || c = '[' || c = '{' then
        (String.mk acc.reverse, c :: cs)
      else readAtom (c :: acc) cs

mutual

partial def parseSexp (cs : List Char) : Option (Sexp × List Char) :=
  match skipWs cs with
  | '[' :: rest => parseVec rest []
  | '{' :: rest => parseMap rest []
  | [] => none
  | cs@(c :: _) =>
      if c = ']' || c = '}' then none
      else
        let (a, rest) := readAtom [] cs
        if a = "" then none else some (Sexp.atom a, rest)

partial def parseVec (cs : List Char) (acc : List Sexp) : Option (Sexp × List Char) :=
  match skipWs cs with
  | ']' :: rest => some (Sexp.vec acc.reverse, rest)
  | [] => none
  | cs' =>
      match parseSexp cs' with
      | none => none
      | some (x, rest) => parseVec rest (x :: acc)

partial def parseMap (cs : List Char) (acc : List (String × Sexp)) : Option (Sexp × List Char) :=
  match skipWs cs with
  | '}' :: rest => some (Sexp.map acc.reverse, rest)
  | [] => none
  | cs' =>
      match parseSexp cs' with
      | none => none
      | some (Sexp.atom k, rest) =>
          match parseSexp rest with
          | none => none
          | some (v, rest2) => parseMap rest2 ((k, v) :: acc)
      | some (_, _) => none

end

/-- Parse the whole file: the top-level vector's elements. -/
def parseAll (s : String) : List Sexp :=
  match parseSexp s.toList with
  | some (Sexp.vec xs, _) => xs
  | some (x, _) => [x]
  | none => []

end Edn

/-! ## From S-expressions to histories -/

namespace Edn

/-- Plain decimal parse of an atom. -/
def natOfAtom (s : String) : Nat :=
  s.toList.foldl (fun acc c => if c.isDigit then acc * 10 + (c.toNat - 48) else acc) 0

def fieldNat (e : Sexp) (k : String) : Nat :=
  match e with
  | .map kvs =>
      match kvs.find? (fun p => p.1 = k) with
      | some (_, .atom a) => a.toList.foldl (fun acc c => if c.isDigit then acc * 10 + (c.toNat - 48) else acc) 0
      | _ => 0
  | _ => 0

def fieldKw (e : Sexp) (k : String) : String :=
  match e with
  | .map kvs =>
      match kvs.find? (fun p => p.1 = k) with
      | some (_, .atom a) => a
      | _ => ""
  | _ => ""

def fieldOf (e : Sexp) (k : String) : Option Sexp :=
  match e with
  | .map kvs => (kvs.find? (fun p => p.1 = k)).map (·.2)
  | _ => none

/-- `[:append 7 1]` → `.append 7 1`; `[:r 5 _]` → `.read 5`. -/
def opOf (e : Sexp) : Option Op :=
  match e with
  | .vec (.atom "append" :: .atom k :: .atom v :: _) => some (.append (natOfAtom k) (natOfAtom v))
  | .vec (.atom ":append" :: .atom k :: .atom v :: _) => some (.append (natOfAtom k) (natOfAtom v))
  | .vec (.atom "r" :: .atom k :: _) => some (.read (natOfAtom k))
  | .vec (.atom ":r" :: .atom k :: _) => some (.read (natOfAtom k))
  | _ => none

/-- Values of a read's result vector: `[:r 5 [1 2]]` → `[1, 2]`. -/
def readResult (e : Sexp) : Option (List Val) :=
  match e with
  | .vec (_ :: _ :: .vec vs :: _) => some (vs.map (fun v => match v with
      | .atom a => natOfAtom a
      | _ => 0))
  | .vec (_ :: _ :: .atom "nil" :: _) => some []
  | _ => none

end Edn

open Edn

def counterOf (cs : List (Node × Nat)) (p : Node) : Nat :=
  match cs.find? (fun q => q.1 = p) with
  | some q => q.2
  | none => 0

def bump (cs : List (Node × Nat)) (p : Node) : List (Node × Nat) :=
  match cs.find? (fun q => q.1 = p) with
  | some _ => cs.map (fun q => if q.1 = p then (q.1, q.2 + 1) else q)
  | none => cs ++ [(p, 1)]

/-- Turn the parsed events into a `History`, numbering each process's
    transactions by invoke order (the EDN carries no transaction id). -/
def toHistory (xs : List Sexp) : History :=
  let step := fun (acc : List (Node × Nat) × List Ev) (e : Sexp) =>
    let p := fieldNat e ":process"
    let kind := fieldKw e ":type"
    let vs := match fieldOf e ":value" with
      | some (Sexp.vec vs) => vs
      | _ => []
    let ops := vs.filterMap opOf
    let reads := vs.filterMap readResult
    let n := counterOf acc.1 p
    -- ids must be unique across processes, or `History.committed` would
    -- collapse two different client transactions into one
    let id := p * 1000000 + n
    let ev := match kind with
      | ":invoke" => ⟨p, .invoke, id, ops, [], 0, 0⟩
      | ":ok" => ⟨p, .ok, id - 1, ops, reads, 0, 0⟩
      | ":info" => ⟨p, .info, id - 1, ops, reads, 0, 0⟩
      | _ => ⟨p, .fail, id - 1, ops, reads, 0, 0⟩
    let cs := if kind = ":invoke" then bump acc.1 p else acc.1
    (cs, acc.2 ++ [ev])
  let (_, evs) := xs.foldl step ([], [])
  ⟨evs⟩

/-- Parse a history file. -/
def loadHistory (path : System.FilePath) : IO History := do
  let s ← IO.FS.readFile path
  return toHistory (parseAll s)

end Ferrosa.Accord
