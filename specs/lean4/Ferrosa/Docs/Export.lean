/-
  Ferrosa formal specification — the book exporter.
  =================================================

  `specs/lean4/docs/` renders this corpus as a PDF. This is the half that reads the **compiler**
  rather than the text.

  The distinction is not pedantry. `Ferrosa/Topology/Pair.lean` writes the degraded pair as the
  positional `⟨.primary, .secondary, false, true⟩`; the elaborator prints it as
  `{ r0 := Role.primary, r1 := Role.secondary, up0 := false, up1 := true }`. A reader who parses the
  source form has to hold four field positions in their head to see that the surviving node is the
  secondary and therefore not writable; the printed form says it. Every statement in the document
  therefore comes from `ppExprLegacy` over the environment, never from the source text.

  Prose comes from the source, because that is where it belongs: the section blocks and the
  docstrings the modules already carry are the claim sentences, written next to the code they
  describe. Lean nests block comments, so a comment here may not contain the sequence that opens
  one — that is why this paragraph does not spell them out.

  Run: `lake exe docs build/specs.json`
-/
import Lean
import Ferrosa
import Ferrosa.Docs.Book

open Lean

namespace Ferrosa.Docs.Export

/-! ## The source walk

  Two facts about this corpus make a line scan honest where it would be a hack elsewhere: every
  file opens with a plain block header, and blocks do not nest: docstrings and sections are the
  only things that run to a terminator. So the scanner tracks exactly the
  state it needs — the namespace stack, the docstring waiting for its declaration, and whether it
  is inside a block — and no more.
-/

/-- A declaration, as the document prints it. -/
structure DeclView where
  /-- 1-based line in the source file. -/
  line : Nat
  /-- `theorem`, `def`, `structure`, … -/
  kind : String
  /-- The name as written (`degraded_pair_is_read_only`). -/
  short : String
  /-- The name in the environment (`Ferrosa.Topology.Pair.degraded_pair_is_read_only`). -/
  full : String
  /-- The docstring that preceded it in the source, verbatim. Empty when there is none. -/
  doc : String

/-- One thing in a module, in source order: a section, or a declaration. -/
inductive Ev where
  | sect (level line : Nat) (text : String)
  | decl (d : DeclView)

/-- A scanned module: its header essay, its events, and the namespaces it opens, which is what the
    statement printer needs in order to shorten `Ferrosa.Topology.Pair.Readable` to `Readable`. -/
structure ModuleView where
  key : String
  path : String
  header : String
  evs : Array Ev
  namespaces : Array String

/-- `s` contains `sub`, tested the way `Ferrosa/Ledgers.lean` tests for a definition. -/
def hasSub (s sub : String) : Bool := (s.splitOn sub).length > 1

/-! In this toolchain `String.drop`/`takeWhile`/`trim` yield a `Slice`, so every use needs a
conversion. Named helpers keep that out of the scanner's control flow. -/

/-- The string without leading and trailing whitespace. -/
def trim (s : String) : String := s.trimAscii.toString

/-- `s` without its first `n` characters. -/
def dropStr (s : String) (n : Nat) : String := (s.drop n).toString

/-- The longest prefix of `s` whose characters satisfy `p`. -/
def takeWhileStr (s : String) (p : Char → Bool) : String := (s.takeWhile p).toString

/-- Strip the modifiers this corpus puts in front of a declaration keyword. Bounded rather than
    recursive, so there is no termination argument to get wrong. -/
def stripMods (s : String) : String := Id.run do
  let mut t := s
  for _ in [0:6] do
    for p in ["private ", "protected ", "noncomputable ", "partial ", "unsafe ", "@[unimplemented] "] do
      if t.startsWith p then t := trim (dropStr t p.length)
  return t

/-- The declaration keyword a line opens, if any. -/
def declKeyword (s : String) : Option String :=
  ["theorem", "def", "structure", "inductive", "abbrev", "class", "instance"].findSome?
    (fun kw => if s.startsWith (kw ++ " ") then some kw else none)

/-- The identifier that follows a keyword: up to whitespace, `:`, `(`, `{` or `[`. -/
def identAfter (kw s : String) : String :=
  takeWhileStr (trim (dropStr s (kw.length + 1)))
    (fun c => !c.isWhitespace && c != ':' && c != '(' && c != '{' && c != '[')

/-- Number of leading `#` in a line, and the line without them: `## Admission` → `(2, "Admission")`. -/
def headingOf (s : String) : Nat × String :=
  let t := trim s
  let lvl := (t.toList.takeWhile (· == '#')).length
  (lvl, trim (dropStr t lvl))

/-- Read a block that opens on line `i` and runs to its terminator. Returns the body without the opener
    and terminator, and the index just past the terminator. Written flat — one loop, no nesting —
    because a `while` inside an `if` inside a `while` is where `do` layout stops being obvious. -/
def collectBlock (lines : Array String) (i : Nat) (opener : String) : String × Nat := Id.run do
  let mut body := opener
  let mut j := i
  let mut closed := hasSub body "-/"
  while !closed && j + 1 < lines.size do
    j := j + 1
    body := body ++ "\n" ++ lines[j]!
    closed := hasSub (lines[j]!) "-/"
  return (trim ((body.splitOn "-/").head!), j + 1)

/-- Scan one module: header, sections, declarations, in source order. -/
def scanFile (key src : String) (path : String) : IO ModuleView := do
  let lines := (src.splitOn "\n").toArray
  let mut evs : Array Ev := #[]
  let mut header : String := ""
  let mut ns : Array String := #[]
  let mut nss : Array String := #[]
  let mut pending : Option String := none
  let mut i := 0
  while i < lines.size do
    let t := trim (lines[i]!)
    if t.startsWith "/--" then
      -- a docstring: hold it for the declaration below
      let (body, next) := collectBlock lines i (dropStr t 3)
      pending := some body
      i := next
    else if t.startsWith "/-!" then
      -- a section: `/-! ## Title` … `-/`
      let (body, next) := collectBlock lines i (dropStr t 3)
      let firstLine := trim ((body.splitOn "\n").head!)
      let (lvl, title) := headingOf firstLine
      let rest := trim (dropStr body firstLine.length)
      let text := if title.isEmpty then rest
        else if rest.isEmpty then title else title ++ "\n\n" ++ rest
      evs := evs.push (.sect (if lvl == 0 then 2 else lvl) (i + 1) text)
      i := next
    else if t.startsWith "/-" then
      -- the module header essay (not a docstring, not a section)
      let (body, next) := collectBlock lines i (dropStr t 2)
      if header.isEmpty then header := body
      i := next
    else if t.startsWith "namespace " then
      -- `namespace X` inside `namespace A` means `A.X`, so the stack holds full names
      let n := trim (dropStr t 10)
      let full := match ns.back? with
        | some p => if p.isEmpty then n else p ++ "." ++ n
        | none => n
      ns := ns.push full
      if !nss.contains full then nss := nss.push full
      i := i + 1
    else if t.startsWith "section" then
      ns := ns.push ""
      i := i + 1
    else if t == "end" then
      if ns.size > 0 then ns := ns.pop
      i := i + 1
    else if t.startsWith "end " then
      if ns.size > 0 then ns := ns.pop
      i := i + 1
    else
      let s := stripMods t
      match declKeyword s with
      | none => i := i + 1
      | some kw =>
        let short := identAfter kw s
        if short.isEmpty || short.startsWith "_" || !(short.toList.head!.isAlpha || short.toList.head! == '_') then
          i := i + 1
        else
          let full := String.intercalate "." (ns.toList ++ [short])
          evs := evs.push (.decl {
            line := i + 1, kind := kw, short, full, doc := pending.getD ""
          })
          pending := none
          i := i + 1
  return { key, path, header, evs, namespaces := nss }

/-! ## The module key, and the walk -/

/-- `Ferrosa/Storage/Mvcc.lean` → `Ferrosa.Storage.Mvcc`. -/
def moduleKeyOf (path : String) : String :=
  let parts := (System.FilePath.mk path).components
  match parts.reverse with
  | last :: revInit =>
    match last.splitOn "." with
    | [stem, "lean"] => String.intercalate "." (revInit.reverse ++ [stem])
    | _ => String.intercalate "." parts
  | [] => path

/-- Every `.lean` file under `Ferrosa/`, in path order. -/
def leanFiles : IO (Array String) := do
  -- `enter` decides whether to descend into a *directory*, so it must accept everything; the
  -- extension filter is applied to the files it yields.
  let fs ← System.FilePath.walkDir "Ferrosa" fun _ => pure true
  let lean := fs.filter (fun p => p.extension == some "lean")
  return (lean.map (·.toString)).qsort (fun a b => a < b)

/-! ## The ledgers, read from the registry

  `Ferrosa.Unimplemented.areas` is the one enumeration of the area ledgers, so reading it here means
  the document's claim list cannot drift from the one `ledgerTotal` sums.
-/

def statusJson (i : Ferrosa.Ledger.SpecItem) : Json :=
  match i.status with
  | .met proof => Json.mkObj [("claim", Json.str i.name), ("kind", Json.str "met"), ("proof", Json.str proof)]
  | .failing why wit =>
    Json.mkObj [("claim", Json.str i.name), ("kind", Json.str "failing"),
      ("why", Json.str why), ("witness", Json.str wit)]

/-- `full` ends with the segment sequence `segs`, on segment boundaries. -/
def endsWithSegs (full : String) (segs : List String) : Bool :=
  let fs := full.splitOn "."
  fs.length >= segs.length && fs.drop (fs.length - segs.length) == segs

/-- Resolve a ledger's proof string to a declaration. The ledger writes the name as the module
    writes it — `Storage.WritePath.batch_ack_is_durable (per-partition)`, `Aggregate.count_star_is_exact`
    — which is rarely the fully qualified name: a ledger may name a theorem in another module, and
    may write a shortened path to it. So match on the longest segment tail that resolves to exactly
    one declaration, longest first. A name that resolves to nothing is reported, not guessed. -/
def resolve (decls : Array (String × String)) (s : String) : Option (String × String) := Id.run do
  let s := trim ((s.splitOn " (").head!)
  let ss := s.splitOn "."
  let mut k := 0
  let mut found : Option (String × String) := none
  while k < ss.length && found.isNone do
    let tail := ss.drop k
    let hits := decls.filter (fun (full, _) => endsWithSegs full tail)
    if hits.size == 1 then found := some hits[0]!
    k := k + 1
  return found

def run (args : List String) : IO UInt32 := do
  let out := args.head?.getD "build/specs.json"
  let files ← leanFiles

  let mut mods : Array ModuleView := #[]
  let mut decls : Array (String × String) := #[]
  for f in files do
    let key := moduleKeyOf f
    -- the root module is imports only, and `Ferrosa.Docs` is this machinery, not the corpus
    if key == "Ferrosa" || key.startsWith "Ferrosa.Docs" then continue
    let src ← IO.FS.readFile f
    let m ← scanFile key src f
    mods := mods.push m
    for e in m.evs do
      match e with
      | .decl d => decls := decls.push (d.full, m.key)
      | .sect .. => pure ()

  -- ledgers, from the one registry
  let areas := Ferrosa.Unimplemented.areas
  let mut items := 0
  let mut reds := 0
  let mut unresolved : Array String := #[]
  let mut areasJson : Array Json := #[]
  for a in areas do
    for i in a.ledger do
      items := items + 1
      match i.status with
      | .failing .. => reds := reds + 1
      | .met proof => if (resolve decls proof).isNone then unresolved := unresolved.push proof
    areasJson := areasJson.push <| Json.mkObj [
      ("key", Json.str a.key),
      ("items", Json.arr (a.ledger.toArray.map statusJson))]

  -- the invariant: the document's red count is the one the corpus pins
  if reds != Ferrosa.Unimplemented.ledgerTotal then
    throw <| IO.userError s!"red count mismatch: document {reds}, ledgerTotal {Ferrosa.Unimplemented.ledgerTotal}"
  if areas.length != 21 then
    throw <| IO.userError s!"registered areas {areas.length}, expected 21 (registered_ledger_count)"

  let unplaced := areas.filter (fun a =>
    !Ferrosa.Docs.Book.parts.any (fun p => p.modules.contains a.key))

  let evJson : Ev → Json
    | .sect level line text => Json.mkObj [("k", Json.str "sect"), ("level", toJson level),
        ("line", toJson line), ("text", Json.str text)]
    | .decl d => Json.mkObj [("k", Json.str "decl"), ("kind", Json.str d.kind),
        ("short", Json.str d.short), ("full", Json.str d.full), ("doc", Json.str d.doc),
        ("line", toJson d.line)]

  let modJson := mods.map fun m => Json.mkObj [
    ("key", Json.str m.key), ("path", Json.str m.path), ("header", Json.str m.header),
    ("namespaces", Json.arr (m.namespaces.map Json.str)),
    ("events", Json.arr (m.evs.map evJson))]

  let partJson := Ferrosa.Docs.Book.parts.toArray.map fun p => Json.mkObj [
    ("numeral", Json.str p.numeral), ("title", Json.str p.title),
    ("intro", Json.str p.intro), ("modules", Json.arr (p.modules.toArray.map Json.str))]

  let toolchain ← (IO.FS.readFile "lean-toolchain").map trim
  let root := Json.mkObj [
    ("toolchain", Json.str toolchain),
    ("parts", Json.arr partJson),
    ("modules", Json.arr modJson),
    ("areas", Json.arr areasJson),
    ("totals", Json.mkObj [
      ("areas", toJson areas.length), ("items", toJson items),
      ("red", toJson reds), ("modules", toJson mods.size),
      ("theorems", toJson (decls.size))])]

  IO.FS.createDirAll "build"
  IO.FS.writeFile out (root.pretty 80 ++ "\n")
  IO.println s!"docs: {mods.size} modules, {items} claims, {reds} red, {decls.size} declarations → {out}"
  if !unresolved.isEmpty then
    IO.println s!"docs: {unresolved.size} ledger claims name no declaration:"
    for u in unresolved do IO.println s!"  ! {u}"
  if !unplaced.isEmpty then
    IO.println s!"docs: {unplaced.length} ledgers unplaced in Book.parts:"
    for a in unplaced do IO.println s!"  ! {a.key}"
  if !unresolved.isEmpty || !unplaced.isEmpty then return 1
  return 0

end Ferrosa.Docs.Export

/-- Entry point. It has to live outside the namespace: Lean recognises the entry point by the
    unqualified name `main` in the root module, and a namespaced one is just another definition.
    `Ferrosa/Harness.lean` does the same for the same reason. -/
def main (args : List String) : IO UInt32 := Ferrosa.Docs.Export.run args
