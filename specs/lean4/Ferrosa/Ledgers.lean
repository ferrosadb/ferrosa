/-
  Runners for the obligation ledgers.
  ==================================

  One `main` in the library, so the root can import every module. Run:

  ```text
  lake env lean --run Ferrosa/Ledgers.lean
  ```
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

/-- Print every ledger: the CQL aggregate obligations, then crash recovery, then
    tombstones/TTL/purge. -/
def main : IO Unit := do
  Ferrosa.Cql.AggregateObligations.main
  IO.println ""
  Ferrosa.Storage.Recovery.main
  IO.println ""
  Ferrosa.Storage.Tombstone.main
  IO.println ""
  Ferrosa.Storage.Format.main
  IO.println ""
  Ferrosa.Cdc.main
  IO.println ""
  Ferrosa.Cql.Subscribe.main
  IO.println ""
  Ferrosa.Flight.main
  IO.println ""
  Ferrosa.Todo.main
  IO.println ""
  Ferrosa.Testing.Jepsen.main
  IO.println ""
  Ferrosa.Testing.Models.main
  IO.println ""
  Ferrosa.Testing.Histories.main
  IO.println ""
  Ferrosa.Testing.Guarantees.main
  IO.println ""
  Ferrosa.Unsafe.main
  IO.println ""
  Ferrosa.Commons.Hll.main
  IO.println ""
  Ferrosa.Commons.Refactors.main
  IO.println ""
  Ferrosa.Sql.main
  IO.println ""
  Ferrosa.Bolt.main
  IO.println ""
  Ferrosa.Sparql.main
  IO.println ""
  Ferrosa.Sql.Index.main
  IO.println ""
  Ferrosa.Sql.Txn.main
  IO.println ""
  Ferrosa.Storage.Mvcc.main
  IO.println ""
  Ferrosa.Storage.Streaming.main
  IO.println ""
  Ferrosa.Sql.Consistency.main
  IO.println ""
  Ferrosa.Broken.main
  IO.println ""
  Ferrosa.Unimplemented.main
