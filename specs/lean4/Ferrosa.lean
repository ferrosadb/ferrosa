/-
  Ferrosa formal specification — root module.
  ==========================================

  Build:   lake build
  Review:  start with README.md, then FINDINGS.md.

  Trace 1 (write-path correctness):  Ferrosa.WritePath
  Trace 2 (replication ladder):      Ferrosa.Topology.Ladder + the per-rung files
  Accord / Jepsen serializability:   Ferrosa.Accord.*  (History → Serializable →
                                     Protocol → Recovery → Strict → Edn)
  Existing-spec audit:               Ferrosa.TlaCheck, Ferrosa.Accord.AccordMultiDcLean
-/
import Ferrosa.Core
import Ferrosa.Ledger
import Ferrosa.WritePath
import Ferrosa.TlaCheck
import Ferrosa.Accord.AccordMultiDcLean
import Ferrosa.Topology.Basic
import Ferrosa.Topology.Pair
import Ferrosa.Topology.Single
import Ferrosa.Topology.ThreeNode
import Ferrosa.Topology.FiveNode
import Ferrosa.Topology.TwoDc
import Ferrosa.Topology.ThreeDcRing
import Ferrosa.Topology.Ladder
import Ferrosa.Accord.History
import Ferrosa.Accord.Serializable
import Ferrosa.Accord.Protocol
import Ferrosa.Accord.Recovery
import Ferrosa.Accord.Strict
import Ferrosa.Accord.Edn
import Ferrosa.Topology.ConsistencyLevels
import Ferrosa.Topology.DcQuorum
import Ferrosa.Cql.Syntax
import Ferrosa.Cql.Classify
import Ferrosa.Cql.Conformance
import Ferrosa.Cql.Lwt
import Ferrosa.Cql.Refine
import Ferrosa.Cql.Collections
import Ferrosa.Cql.Index
import Ferrosa.Cql.Aggregate
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
import Ferrosa.Commons.Flow
import Ferrosa.Commons.Stream
import Ferrosa.Commons.FrontEnd
import Ferrosa.Testing.Jepsen
import Ferrosa.Testing.JepsenModels
import Ferrosa.Testing.Histories
import Ferrosa.Testing.Guarantees
import Ferrosa.Unsafe.Invariants
import Ferrosa.Todo
import Ferrosa.Broken
import Ferrosa.Unimplemented
import Ferrosa.Witnesses
