# RDF term identity and explicit offline cutover

V2 uses `rdf_triples_v2` with graph/subject partition keys and
predicate/lexical/full-identity clustering keys. Kind, datatype and normalized
language distinguish terms. Plain literals canonicalize to xsd:string; numeric
lexical spelling, unknown datatypes and ill-typed values remain unchanged.
Metadata ordinals are datatype=0, language=1, object_type=2. Historical v1 retains
object_type=0, datatype=1, language=2. Reads validate the versioned, tagged,
length-checked key against metadata. Lexical index hits undergo full-term matching.

## Operator contract

Use `migration::OfflineMigration::open(config, DeploymentMode::Direct,
QuiescedCutover::AllOldReadersWritersAndLifecycleOperationsExcluded)`, then
`run(keyspace, MigrationOptions::default())`. The caller must exclude all old and
new readers/writers, other migrators, and compaction/lifecycle operations before
opening and throughout interrupted passes. The attestation is not an automatic
cluster fence. Pair, Cluster and degraded modes are refused before creating
storage. Do not run an older binary against the retained v1 after cutover.

The engine must recover the actual registered source through `StorageEngine::open`.
Both tables must pass `assert_table_recovery_complete`. This includes decoded-row
quarantine, pending replay, unreadable recovery status and rejected reader frames.
A successful survivor scan cannot certify completeness by itself. Recovery evidence
is sticky; silently lost history from older binaries requires authoritative recovery.

## Copy and publication

1. Refuse an existing v2 target without migration ownership. Persist source and
   target incarnation UUIDs and an incomplete cutover record. Drop/truncate removes
   the per-directory identity and invalidates admission.
2. Stream reconciled v1 fragments. Decode strict historical metadata, rejecting
   hybrid timestamps, expiry, unknown columns and malformed keys. Write canonical
   v2 rows, flushing every 256 survivors. Memory is bounded by storage fragments
   and individual terms, rather than total table width.
3. Wait for the lexical index to become current. Revisit each source identity via
   bounded index callbacks and independently stream/count/validate the target.
   Refuse missing identities or extra target rows. Recheck recovery and provenance.
4. Flush target/schema and publish completion through atomic JSON replacement with
   file/directory fsync. Marker reads are capped at 4 KiB. An empty registry snapshot
   continues to use the complete flat storage catalog on restart.

`max_rows_per_run` is a copy budget for interruption testing/operator pacing;
exhaustion returns an error and retains an unservable partial target. A later pass
replays idempotent writes and validates the whole survivor set. A completed pass
never recopies v1, so subsequent v2 deletes remain deleted. Serving checks complete
provenance before reads and writes. V1 is retained but never unioned into queries.

## Evidence and limits

Tests cover full term identity, actual legacy `"01"^^integer` preservation,
malformed metadata, empty and wide sources, WAL rejection, fresh-target conflict,
interrupted-copy resume, damaged markers, source replacement, catalog restart and
post-cutover deletes/reinsertion. No test skips replace these assertions.

A marker does not recover variants already overwritten in v1 and cannot exclude
other processes or nodes. Keeping old data does not make downgrade safe after v2
writes; rollback requires a checkpoint or explicit export/reimport. Production
rollout and matched performance reruns are separate from these correctness tests.
