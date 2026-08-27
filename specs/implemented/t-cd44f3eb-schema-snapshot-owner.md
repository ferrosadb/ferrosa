# T-007 / t_cd44f3eb — crash-safe schema snapshot ownership

Status: implemented; independent verification and PR pending.

The incident was caused by two same-build components writing incompatible JSON
documents to `{data_dir}/schema.json`. The composition root wrote the complete
registry `SchemaSnapshot`; `StorageEngine::flush` wrote a table-only array. A
restart after the array won the race could expose an empty CQL registry while
all SSTables remained intact.

The implementation makes `SchemaSnapshotStore` the only publisher of
`schema.json`. It streams through a fixed 64 MiB input/output bound, uses an
explicit format discriminator, serializes concurrent publishers with a file
lock, stages beside the live file, flushes and fsyncs, parses the staged file,
atomically renames it, and fsyncs the containing directory. It retains three
verified generations. Storage-only recovery metadata is independently and
atomically written to `storage-schema.json` without collecting an intermediate
JSON byte vector.

Startup validates the registry snapshot before opening storage. A malformed,
oversized, unknown-format, or legacy table-array document is moved to a unique
`schema.json.unparseable-*` evidence path and returned as an error. The node
does not continue with an empty schema and persistence never overwrites the
unreadable input. Pre-discriminator registry objects remain readable and are
migrated on the next persist.

Verification covers the original two-writer production flush path, concurrent
publishers, legacy object/array and corrupt inputs, the hard byte bound, fixed
generation retention, stale staging cleanup, and simulated crashes after stage
creation, stage fsync, verification, rename, and directory fsync.
