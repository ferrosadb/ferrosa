import FerrosaProduct.ObjectStorage
import FerrosaProduct.Cql
import FerrosaProduct.Transactions
import FerrosaProduct.Scaling
import FerrosaProduct.QuerySurfaces
import FerrosaProduct.Streaming
import FerrosaProduct.Subscriptions
import FerrosaProduct.Uncovered
import FerrosaProduct.KnownDefects
import FerrosaProduct.Register

/-!
# Ferrosa: the specification

Ferrosa is a distributed database written in Rust. It speaks the Cassandra CQL client protocol, and
it keeps the durable copy of every table in S3-compatible object storage, with local NVMe as a
write-behind cache. Postgres, Cypher over Bolt, SPARQL and Arrow Flight run against the same tables
and the same cluster.

This site is the specification of what Ferrosa promises, written so that the promises can be checked.
Every guarantee here is a theorem in Lean 4, and the site is built from the compiled proofs. If a
theorem is renamed, deleted or stops proving, the site fails to build, so the site cannot state a
guarantee that the proofs do not hold.

## How to read it

The pages come in three layers.

1. **Product features.** The pages under this one take the claims made on
   [ferrosa.ai](https://www.ferrosa.ai/database/) one at a time. Each claim is followed by the
   theorems that back it. Where the specification has found that the code does *not* meet a claim
   yet, the page says so and cites the theorem that shows the failure. Claims that nothing in the
   specification addresses are listed on their own page, *What the specification does not cover*.
2. **The specification modules.** Under `Ferrosa` in the navigation, one page per module: the query
   surface (`Cql`, `Sql`, `Sparql`, `Bolt`), consistency and topology (`Topology`, `Accord`),
   change streams (`Cdc`, `Flight`, `Commons`), the write path and storage internals (`WritePath`,
   `Storage`), the shared foundations (`Core`), the evidence harnesses (`Testing`), and the accounting
   of what is not yet proved (`Unimplemented`, `Broken`). Each module opens with what it guarantees,
   then states the model and the theorems in the order a reviewer should meet them.
3. **The theorem register.** Every theorem in the specification, grouped by module, with its
   statement as Lean holds it. Use it to review the specification exhaustively; the feature pages
   only cite the theorems that carry a claim.

## Two kinds of theorem

Most theorems are **guarantees**: they prove that a property holds for every input the model admits.
`no_two_writable` says that in every reachable state of a replica pair, at most one node accepts
writes.

Some are **witnesses of a defect**, called *red* in the specification. A red theorem proves that a
property *fails*: it exhibits the input on which the code, or the code's design, goes wrong.
`Ferrosa.Storage.Tombstone.purge_before_propagation_resurrects` proves that purging a tombstone
before every replica has applied it brings deleted data back. A red theorem is not a bug in the
specification. It is a finding, and the ledger in `Ferrosa.Unimplemented` tracks each one until the
code is fixed and the theorem can be replaced by a guarantee.

## What is modelled and what is not

The specification models behaviour, not performance. Quorum sizes and overlap are modelled; token
placement is not. Accord's apply order and recovery are modelled; its message layer is represented by
its effect on the log. Storage is modelled as ordered records with a durability boundary; S3 upload
and manifest compare-and-set are behind that boundary. Clocks are abstract timestamps. Latency,
throughput and cost claims are measurements and never appear as theorems.

Ferrosa is a developer preview. The specification is the accounting of how far that preview has got:
read the red theorems before depending on a feature.
-/
