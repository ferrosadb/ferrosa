/-
  The runner: one list, printed and counted
  =========================================

  One `main` prints every area ledger, the unspecified-surface manifest, the refactoring record, the
  broken list and the classification, then fails if the registry has drifted. The ledgers it prints are
  exactly `Ferrosa.Unimplemented.areas` — the list `ledgerTotal` sums — so a ledger cannot print without
  being counted, or be counted without printing. It also re-scans the source tree for files that define
  a ledger and fails on three kinds of drift: a ledger module missing from the registry, a registered
  key with no ledger behind it, and a scan that found almost nothing and would otherwise pass
  vacuously. Any of those exits non-zero, so a lost ledger is a build failure rather than a quietly
  smaller total. `lake env lean --run Ferrosa/Ledgers.lean` runs the whole report.
-/
import Ferrosa.Cql.AggregateObligations
import Ferrosa.Storage.Recovery
import Ferrosa.Storage.Tombstone
import Ferrosa.Storage.Format
import Ferrosa.Sql.Semantics
import Ferrosa.Bolt.Session
import Ferrosa.Sparql.Semantics
import Ferrosa.Sql.Index
import Ferrosa.Sql.Txn
import Ferrosa.Storage.Mvcc
import Ferrosa.Storage.Streaming
import Ferrosa.Sql.Consistency
import Ferrosa.Cdc.Subscribe
import Ferrosa.Cql.Subscribe
import Ferrosa.Flight.Subscribe
import Ferrosa.Commons.Tactics
import Ferrosa.Commons.Hll
import Ferrosa.Commons.FrontEnd
import Ferrosa.Testing.Jepsen
import Ferrosa.Testing.JepsenModels
import Ferrosa.Testing.Histories
import Ferrosa.Testing.Guarantees
import Ferrosa.Unsafe.Invariants
import Ferrosa.Todo
import Ferrosa.Broken
import Ferrosa.Unimplemented

namespace Ferrosa.Runner

/-- The module key of a path under `Ferrosa/`: `Ferrosa/Storage/Mvcc.lean` → `Ferrosa.Storage.Mvcc`. -/
def moduleKeyOf (path : System.FilePath) : String :=
  let parts := path.components
  match parts.reverse with
    | last :: revInit =>
      match last.splitOn "." with
      | [stem, "lean"] => String.intercalate "." (revInit.reverse ++ [stem])
      | _ => String.intercalate "." parts
    | [] => String.intercalate "." parts

/-- Whether a source file defines an area ledger. -/
def definesLedger (path : System.FilePath) : IO Bool := do
  if path.extension == some "lean" then
    let src ← IO.FS.readFile path
    -- a definition at column zero, not the phrase in a docstring
    return (src.splitOn "\ndef ledger : List SpecItem := [").length > 1
  else
    return false

/-- The module keys of every file under `Ferrosa/` that defines an area ledger, discovered from the
    tree rather than from a list. -/
def ledgerModules : IO (List String) := do
  let files ← System.FilePath.walkDir "Ferrosa" fun _ => pure true
  let mut keys : List String := []
  for f in files.toList do
    if (← definesLedger f) then
      keys := moduleKeyOf f :: keys
  return keys

/-- Registry drift: a ledger module the registry does not hold (so it prints nothing and is counted
    by nothing), a registered key that names no ledger module, or a scan that found nothing and
    would otherwise pass vacuously. -/
def driftReport : IO (List String) := do
  let found ← ledgerModules
  let registered := Ferrosa.Unimplemented.areas.map (·.key)
  let missing := found.filter fun k => !registered.contains k
  let dangling := registered.filter fun k => !found.contains k
  let vacuous :=
    if found.length < 2 then
      ["the ledger scan found no modules; refusing to pass vacuously (run from specs/lean4)"]
    else []
  return vacuous
    ++ (missing.map fun k =>
      s!"ledger module {k} defines a ledger but is not in Ferrosa.Unimplemented.areas: it prints nothing and is counted by nothing")
    ++ (dangling.map fun k =>
      s!"Ferrosa.Unimplemented.areas names {k}, but no file under Ferrosa/ defines a ledger for it")

/-- Print every registered ledger, then the classification, then fail on registry drift. -/
def main : IO Unit := do
  Ferrosa.Unimplemented.printAllLedgers
  Ferrosa.Todo.main
  IO.println ""
  Ferrosa.Commons.Refactors.main
  IO.println ""
  Ferrosa.Broken.main
  IO.println ""
  Ferrosa.Unimplemented.main
  IO.println ""
  let drift ← driftReport
  if drift.isEmpty then
    IO.println s!"registry OK — {Ferrosa.Unimplemented.areas.length} ledgers, printed and counted from one list"
  else
    IO.println "REGISTRY DRIFT"
    for d in drift do
      IO.println s!"  {d}"
    IO.Process.exit 1

end Ferrosa.Runner

def main : IO Unit := Ferrosa.Runner.main
