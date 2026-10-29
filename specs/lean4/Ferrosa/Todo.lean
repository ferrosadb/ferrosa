/-
  Ferrosa formal specification — the surfaces that have no spec yet.
  ================================================================

  Every area in `specs/lean4/` is a ledger of obligations, each either proved or red. This file is
  not a ledger: it is the manifest of surfaces that have **no obligations at all yet**, which is a
  different and worse state. A red item says "we know what is required and it is not built"; an
  entry here says "we have not decided what is required".

  Each entry names the crate and the questions that must be settled before it can become a ledger,
  because a stub without questions decays into a name. `gapCount` is pinned by a theorem, so an
  entry can only be removed by adding its module — the list can shrink on purpose, never by
  accident.

  Ordering below is by risk, not by effort: JSONB and views first because a wrong semantic there is
  silent, while `ferrosa-ctl` and the harnesses are deliberately absent from this list because they
  carry no data semantics to get wrong.
-/
import Ferrosa.Core
import Ferrosa.Ledger

namespace Ferrosa.Todo

open Ferrosa Ferrosa.Ledger

structure Gap where
  /-- The surface. -/
  area : String
  /-- Where the code lives. -/
  crate : String
  /-- What must be decided and proved before this becomes a ledger. -/
  questions : List String
  deriving Repr

def gaps : List Gap := [
  ⟨"JSONB",
   "ferrosa-jsonb, ferrosa-jsonb-conformance",
   [ "equality and ordering rules: which of them match Postgres, and which diverge on purpose",
     "null, absent, and empty: three different things, and which operators confuse them",
     "normalisation on write (key order, duplicate keys, number canonicalisation)",
     "index eligibility per operator, and the per-kind O(1) claim for each",
     "what the conformance corpus pins, and what it silently does not" ]⟩,
  ⟨"Materialized views",
   "ferrosa-view",
   [ "the staleness window: what a reader is guaranteed to see, and when",
     "refresh against compaction, and against the MVCC snapshot horizon",
     "TTL interaction: does a view row expire with its base row or with its own write",
     "a read of a view whose base row was concurrently deleted",
     "whether a view is maintained synchronously in the write path or asynchronously" ]⟩,
  ⟨"User-defined functions",
   "ferrosa-udf",
   [ "determinism classes, and whether a UDF may appear in a key predicate or a partition expression",
     "sandbox and resource limits: memory, time, and what a UDF may import",
     "aggregate UDFs against the three existing aggregate contracts, which are not interchangeable",
     "does a UDF forbid materialisation? the no-copy streaming rule must survive a function call",
     "schema validation at CREATE and at ALTER, and what happens when the language runtime changes" ]⟩,
  ⟨"Index build",
   "ferrosa-index-builder",
   [ "online build against concurrent writes: what a reader sees mid-backfill",
     "backfill visibility under MVCC, and whether a half-built index may be consulted at all",
     "resume after a failed build, and how a stale entry is distinguished from a wrong one",
     "the atomic-boundary question already red for index maintenance belongs to the same answer" ]⟩,
  ⟨"Sessions and prepared statements",
   "ferrosa-session",
   [ "the prepared-statement cache, and its invalidation on schema change",
     "session-scoped settings, and whether a session may hold rows between statements",
     "cursor and paging state: what a session owns and what the client owns",
     "what happens to prepared statements on a node restart or a schema_version bump" ]⟩,
  ⟨"Arrow Flight",
   "ferrosa-flight",
   [ "batch framing and backpressure on the streaming path",
     "ticket semantics, and whether a ticket is a query, a cursor, or a snapshot",
     "the read path only: the subscription transport is settled in Ferrosa.Flight.Subscribe",
     "type mapping from CQL and SQL result sets to Arrow, and where precision is lost" ]⟩,
  ⟨"Type coercion",
   "ferrosa-row-bridge",
   [ "CQL to PG and PG to CQL type mapping, in both directions",
     "numeric widening and narrowing: where truncation or rounding may happen silently",
     "timestamp precision and timezone, against the ldt 2038 wrap already red elsewhere",
     "null versus absent versus empty on each crossing" ]⟩,
  ⟨"Background work",
   "ferrosa-sched, ferrosa-worker",
   [ "compaction, repair, and index build against the request path: admission control and fairness",
     "starvation: what a request must never wait behind, and how long is too long",
     "the same shared-pool question the streaming rules answer for clients, applied to internal work",
     "what happens to a background job when its node loses leadership mid-run" ]⟩,
  ⟨"Internode transport",
   "ferrosa-net",
   [ "framing, retries, and ordering between nodes, below the dc_quorum rules",
     "how a slow peer is reported, and whether acks exert backpressure or are simply counted",
     "retry safety: which internode messages are idempotent and which are not",
     "what a partial ack means when the connection dies after the write but before the reply" ]⟩,
  ⟨"Postgres wire protocol",
   "ferrosa-postgres",
   [ "the extended query protocol, and COPY in and out",
     "error-code mapping, and whether a retryable write-skew exception can be expressed in it",
     "ack coalescing and NODELAY: the unmerged `c38add89` work, specified rather than carried as folklore",
     "the relationship between protocol-level flow control and the streaming backpressure rules" ]⟩
]

def gapCount : Nat := gaps.length

/-- The manifest is pinned: an entry leaves only when its module arrives. -/
theorem the_unspecified_surfaces_are_ten : gapCount = 10 := by decide

def renderGap (g : Gap) : String :=
  let qs := String.intercalate "\n" (g.questions.map (fun q => s!"          - {q}"))
  s!"  TODO  {g.area}  [{g.crate}]\n{qs}"

def main : IO Unit := do
  IO.println s!"unspecified surfaces ({gaps.length} areas, and the questions that must be settled)\n"
  IO.println (String.intercalate "\n\n" (gaps.map renderGap))
  IO.println ""

end Ferrosa.Todo
