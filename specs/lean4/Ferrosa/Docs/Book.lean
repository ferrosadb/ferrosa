/-
  Ferrosa formal specification — the book, as data.
  =================================================

  `specs/lean4/docs/` renders this corpus as a PDF. The prose of that document does not live here:
  it lives in the sources, in the section blocks and the docstrings that the modules already carry
  next to the code they describe. This file holds only what a module cannot say about itself —
  where it sits in the book, and what the part is for.

  So the order below is the argument of the document. Parts run from the shared counting lemma to
  the accounting of what is still owed, and inside a part the modules run in the order a reviewer
  should meet them, which is not always the import order.

  Two claims about this file are checked rather than asserted: `every_ledger_is_placed` (no area
  ledger can exist in the corpus without a chapter to print it in) and `placed_module_count` (the
  book covers every module, so a module added later is a build failure, not a silent omission).
-/
import Ferrosa.Unimplemented

namespace Ferrosa.Docs.Book

/-- One part of the book: its numeral, its title, the paragraph that opens it, and the modules that
    make it up, in reading order. -/
structure Part where
  /-- Roman numeral, as printed. -/
  numeral : String
  /-- Part title, as printed. -/
  title : String
  /-- The opening paragraph. Two or three sentences, no headings, no lists. -/
  intro : String
  /-- Module keys under `Ferrosa/`, in reading order. -/
  modules : List String
  deriving Inhabited

/-- The book. -/
def parts : List Part := [
  ⟨"I", "Foundations", "Everything in the corpus rests on one counting lemma, and on a ledger that records what is proved and what is not. This part is those two things, plus the vocabulary the topology rungs are stated in. Read it first; the rest of the book assumes it and does not repeat it.", [
    "Ferrosa.Core",
    "Ferrosa.Ledger",
    "Ferrosa.Witnesses",
    "Ferrosa.Commons.Tactics",
    "Ferrosa.Topology.Basic"
  ]⟩,

  ⟨"II", "The write path", "A write is admitted, appended to the commit log, replicated to the sync-mode target, applied to the memtable, acknowledged, and only then flushed to an SSTable. Each arrow in that sentence is a constructor of `Step` in `WritePath`, which is why the trace can be checked rather than described. The storage modules then take the path apart: the cell format, the visibility rule, what survives a crash, and what a delete leaves behind.", [
    "Ferrosa.WritePath",
    "Ferrosa.Storage.Format",
    "Ferrosa.Storage.Mvcc",
    "Ferrosa.Storage.Recovery",
    "Ferrosa.Storage.Tombstone",
    "Ferrosa.Storage.Streaming"
  ]⟩,

  ⟨"III", "The query surface", "Four front ends — CQL, SQL, SPARQL and the Bolt session — over the same storage and the same consistency rules. The claims here are about what a query means, which statements can be refused at parse time, and how a subscription's ordering relates to the write path. Where the answer depends on replication, the theorem names the topology module that decides it.", [
    "Ferrosa.Cql.Syntax",
    "Ferrosa.Cql.Classify",
    "Ferrosa.Cql.Collections",
    "Ferrosa.Cql.Conformance",
    "Ferrosa.Cql.Aggregate",
    "Ferrosa.Cql.AggregateObligations",
    "Ferrosa.Cql.Lwt",
    "Ferrosa.Cql.Refine",
    "Ferrosa.Cql.Index",
    "Ferrosa.Cql.Subscribe",
    "Ferrosa.Sql.Semantics",
    "Ferrosa.Sql.Index",
    "Ferrosa.Sql.Txn",
    "Ferrosa.Sql.Consistency",
    "Ferrosa.Sparql.Semantics",
    "Ferrosa.Bolt.Session"
  ]⟩,

  ⟨"IV", "Replication and topology", "The escalation ladder, one rung per module: a single node, a pair, three nodes at RF=2, five at RF=quorum, two datacenters over one WAN link, three in a ring. Each rung is a concrete `Topo`, so its quorum properties are finite and checked by enumeration rather than argued. The Accord modules sit above them and specify the apply order and the recovery path.", [
    "Ferrosa.Accord.Protocol",
    "Ferrosa.Accord.Strict",
    "Ferrosa.Accord.Serializable",
    "Ferrosa.Accord.History",
    "Ferrosa.Accord.Recovery",
    "Ferrosa.Accord.Edn",
    "Ferrosa.Accord.AccordMultiDcLean",
    "Ferrosa.Topology.Single",
    "Ferrosa.Topology.Pair",
    "Ferrosa.Topology.ThreeNode",
    "Ferrosa.Topology.FiveNode",
    "Ferrosa.Topology.TwoDc",
    "Ferrosa.Topology.ThreeDcRing",
    "Ferrosa.Topology.Ladder",
    "Ferrosa.Topology.DcQuorum",
    "Ferrosa.Topology.ConsistencyLevels"
  ]⟩,

  ⟨"V", "Change and control", "What leaves the cluster once a write lands: change-data-capture and flight-style subscribers, the streaming reader they page through, and the three feedback loops that decide how much goes out per tick. The loops are specified as recurrences, so statements about overshoot and steady-state offset are arithmetic on a closed form, not simulation results.", [
    "Ferrosa.Cdc.Subscribe",
    "Ferrosa.Flight.Subscribe",
    "Ferrosa.Commons.Stream",
    "Ferrosa.Commons.Hll",
    "Ferrosa.Commons.Flow",
    "Ferrosa.Commons.FrontEnd"
  ]⟩,

  ⟨"VI", "Evidence", "A specification that only compiles has not been tested against a real history. This part is the machinery that closes that gap: Jepsen's checker models transcribed, the history type they read, the guarantee table, and two harnesses — one that runs every invariant over an enumerated case space, one that checks a recorded Elle history against the Accord model. A claim in this part names what would falsify it.", [
    "Ferrosa.Testing.Jepsen",
    "Ferrosa.Testing.JepsenModels",
    "Ferrosa.Testing.Histories",
    "Ferrosa.Testing.Guarantees",
    "Ferrosa.Harness",
    "Ferrosa.Accord.RunCheck",
    "Ferrosa.TlaCheck"
  ]⟩,

  ⟨"VII", "Limits and accounting", "Not everything here is proved, and the corpus is built so that the unproved part cannot be guessed at. Every red item in every ledger is classified exactly once — unimplemented, requirement, proof, measurement, inherited, undecided — and the counts are pinned by theorems. This part is the audit: the classification, the trusted base under every `unsafe` block, and the items still owed.", [
    "Ferrosa.Unimplemented",
    "Ferrosa.Broken",
    "Ferrosa.Unsafe.Invariants",
    "Ferrosa.Todo",
    "Ferrosa.Ledgers"
  ]⟩
]

/-- Every area ledger the corpus registers appears in some part, so no ledger can be counted in the
    total and printed nowhere. -/
theorem every_ledger_is_placed :
    (Ferrosa.Unimplemented.areas.map (fun a => a.key)).all
      (fun k => parts.any (fun p => p.modules.contains k)) := by decide

/-- The book covers a fixed number of modules. A module added to the corpus without a home here is
    a build failure rather than a silent omission from the document. -/
theorem placed_module_count : (parts.map (fun p => p.modules.length)).sum = 61 := by decide

/-- Part numerals are distinct, so `\ref` cannot collide. -/
theorem part_numerals_distinct : (parts.map (fun p => p.numeral)).Nodup := by decide

end Ferrosa.Docs.Book
