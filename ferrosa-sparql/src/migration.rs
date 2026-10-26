//! Explicit, quiesced local RDF v1-to-v2 migration. Never called by serving.
use crate::error::SparqlError;
use crate::executor::OBJECT_INDEX_NAME;
use crate::triple_store::*;
use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    schema_snapshot::persist_bounded_json, StorageEngine, StorageEngineConfig, TableId,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

const MARKER: &str = "rdf-v2-cutover.json";
const IDENTITY: &str = "rdf-migration-incarnation.json";
const MAX_DOCUMENT: u64 = 4096;

/// Deployment mode supplied by the offline operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeploymentMode {
    Direct,
    Pair,
    Cluster,
    Degraded,
}
/// Operator attestation, not distributed fencing. Hold this exclusion throughout
/// all passes, including interrupted passes, until publication succeeds.
#[derive(Clone, Copy, Debug)]
pub enum QuiescedCutover {
    AllOldReadersWritersAndLifecycleOperationsExcluded,
}
/// Optional copy budget. Exhaustion retains an unservable, resumable target.
#[derive(Clone, Copy, Debug, Default)]
pub struct MigrationOptions {
    pub max_rows_per_run: Option<usize>,
}
/// Original survivor count, including on an already-completed rerun.
#[derive(Debug)]
pub struct MigrationReport {
    pub survivors: u64,
}
/// Owns an engine opened and recovered through `StorageEngine::open`.
pub struct OfflineMigration {
    storage: Arc<StorageEngine>,
    data_dir: PathBuf,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Incarnation {
    id: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cutover {
    version: u8,
    keyspace: String,
    source: String,
    target: String,
    complete: bool,
    survivors: u64,
}
fn invalid(message: impl std::fmt::Display) -> SparqlError {
    SparqlError::Execution(format!("RDF offline migration: {message}"))
}
fn load<T: serde::de::DeserializeOwned>(dir: &Path, name: &str) -> Result<Option<T>, SparqlError> {
    let file = match File::open(dir.join(name)) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(invalid(e)),
    };
    let mut bytes = Vec::new();
    file.take(MAX_DOCUMENT + 1)
        .read_to_end(&mut bytes)
        .map_err(invalid)?;
    if bytes.len() as u64 > MAX_DOCUMENT {
        return Err(invalid("oversized cutover document"));
    }
    serde_json::from_slice(&bytes).map(Some).map_err(invalid)
}
fn persist<T: Serialize>(dir: &Path, name: &str, value: &T) -> Result<(), SparqlError> {
    persist_bounded_json(dir, name, value)?;
    // Persist creation of the table directory as well as its contents.
    File::open(
        dir.parent()
            .ok_or_else(|| invalid("missing parent directory"))?,
    )
    .and_then(|f| f.sync_all())
    .map_err(invalid)?;
    Ok(())
}
fn identity(dir: &Path) -> Result<Option<String>, SparqlError> {
    load::<Incarnation>(dir, IDENTITY)?
        .map(|value| {
            uuid::Uuid::parse_str(&value.id).map_err(invalid)?;
            Ok(value.id)
        })
        .transpose()
}
fn validate(storage: &StorageEngine, keyspace: &str, marker: &Cutover) -> Result<(), SparqlError> {
    if marker.version != 1 || marker.keyspace != keyspace {
        return Err(invalid("cutover provenance mismatch"));
    }
    for (table, expected, schema) in [
        (
            TableId::new(keyspace, RDF_TRIPLES_V1_TABLE),
            &marker.source,
            rdf_triples_v1_schema(keyspace),
        ),
        (
            triples_table_id(keyspace),
            &marker.target,
            rdf_triples_schema(keyspace),
        ),
    ] {
        if identity(&storage.table_sstable_dir(&table))?.as_ref() != Some(expected) {
            return Err(invalid("table incarnation changed or is missing"));
        }
        let actual = storage
            .table_schema(&table)
            .ok_or_else(|| invalid("missing registered table"))?;
        if actual.key_type != schema.key_type
            || actual.clustering_columns != schema.clustering_columns
            || actual.regular_columns != schema.regular_columns
            || actual.static_columns != schema.static_columns
        {
            return Err(invalid("table schema mismatch"));
        }
        storage.assert_table_recovery_complete(&table)?;
    }
    Ok(())
}
/// Validate durable cutover when a marker exists, including when v1 was dropped.
/// `false` means no migration owns the target; the caller must still fence v1.
pub(crate) fn admit(storage: &StorageEngine, keyspace: &str) -> Result<bool, SparqlError> {
    let Some(marker) = load::<Cutover>(
        &storage.table_sstable_dir(&triples_table_id(keyspace)),
        MARKER,
    )?
    else {
        return Ok(false);
    };
    validate(storage, keyspace, &marker)?;
    if !marker.complete {
        return Err(invalid("copy is incomplete; serving and writes refused"));
    }
    Ok(true)
}
impl OfflineMigration {
    /// Recover local storage. Exclude old binaries, writers, readers and lifecycle
    /// operations before opening, and keep them excluded across interrupted runs.
    pub fn open(
        config: StorageEngineConfig,
        mode: DeploymentMode,
        _confirmation: QuiescedCutover,
    ) -> Result<Self, SparqlError> {
        if mode != DeploymentMode::Direct {
            return Err(invalid("requires local Direct mode"));
        }
        let data_dir = config.data_dir.clone();
        let (storage, pending) = StorageEngine::open(config, None)?;
        storage.replay_mutations(pending)?;
        Ok(Self {
            storage: Arc::new(storage),
            data_dir,
        })
    }
    /// Recovered engine for offline inspection or serving after completion.
    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    // Preserve a registry-owned catalog if present; flat storage schema alone is
    // not authoritative when schema.json contains tables.
    fn persist_catalog(&self, keyspace: &str) -> Result<(), SparqlError> {
        use ferrosa_schema::metadata::{
            column::{ClusteringOrder, ColumnKind, ColumnMetadata},
            table::TableMetadata,
        };
        let store = ferrosa_storage::schema_snapshot::SchemaSnapshotStore::new(&self.data_dir);
        if let Some(mut snapshot) = store.load()? {
            // Empty registries deliberately defer to the complete flat storage
            // catalog. Adding only v2 would hide the existing v1 on restart.
            if snapshot.tables.is_empty() {
                return Ok(());
            }
            let key = (keyspace.to_owned(), RDF_TRIPLES_TABLE.to_owned());
            if let std::collections::hash_map::Entry::Vacant(entry) = snapshot.tables.entry(key) {
                let columns = [
                    ("graph", "text", ColumnKind::PartitionKey, 0),
                    ("subject", "text", ColumnKind::PartitionKey, 1),
                    ("predicate", "text", ColumnKind::Clustering, 0),
                    ("object", "text", ColumnKind::Clustering, 1),
                    ("object_identity", "blob", ColumnKind::Clustering, 2),
                    ("datatype", "text", ColumnKind::Regular, -1),
                    ("language", "text", ColumnKind::Regular, -1),
                    ("object_type", "text", ColumnKind::Regular, -1),
                ]
                .into_iter()
                .map(|(name, ty, kind, position)| {
                    (
                        name.to_owned(),
                        ColumnMetadata {
                            name: name.into(),
                            column_type: ty.into(),
                            kind,
                            position,
                            clustering_order: if kind == ColumnKind::Clustering {
                                ClusteringOrder::Asc
                            } else {
                                ClusteringOrder::None
                            },
                            mask: None,
                        },
                    )
                })
                .collect();
                entry.insert(TableMetadata {
                    keyspace: keyspace.into(),
                    name: RDF_TRIPLES_TABLE.into(),
                    id: uuid::Uuid::new_v4(),
                    columns,
                    partition_key: vec!["graph".into(), "subject".into()],
                    clustering_key: vec![
                        ("predicate".into(), ClusteringOrder::Asc),
                        ("object".into(), ClusteringOrder::Asc),
                        ("object_identity".into(), ClusteringOrder::Asc),
                    ],
                    params: Default::default(),
                    flags: Default::default(),
                    extensions: Default::default(),
                    is_system: false,
                });
                snapshot.version = uuid::Uuid::new_v4();
                store.persist(&snapshot)?;
            }
        }
        Ok(())
    }
    /// Copy surviving full terms in bounded fragments, validate both table and
    /// lexical-index coverage, then durably publish. A completed rerun never
    /// copies v1 again, so later v2 deletes cannot resurrect retired data.
    pub async fn run(
        &self,
        keyspace: &str,
        options: MigrationOptions,
    ) -> Result<MigrationReport, SparqlError> {
        if keyspace.is_empty() || keyspace.contains(['/', '\\', '\0']) {
            return Err(invalid("invalid keyspace"));
        }
        let source = TableId::new(keyspace, RDF_TRIPLES_V1_TABLE);
        let target = triples_table_id(keyspace);
        self.storage.assert_table_recovery_complete(&source)?;
        if self.storage.table_schema(&source).is_none() {
            return Err(invalid("source is not registered"));
        }
        let source_dir = self.storage.table_sstable_dir(&source);
        let target_dir = self.storage.table_sstable_dir(&target);
        let mut marker = match load::<Cutover>(&target_dir, MARKER)? {
            Some(marker) => marker,
            None => {
                if self.storage.table_schema(&target).is_some()
                    || target_dir.try_exists().map_err(invalid)?
                {
                    return Err(invalid("target already exists without migration ownership"));
                }
                let source_id = match identity(&source_dir)? {
                    Some(id) => id,
                    None => {
                        let id = uuid::Uuid::new_v4().to_string();
                        persist(&source_dir, IDENTITY, &Incarnation { id: id.clone() })?;
                        id
                    }
                };
                let marker = Cutover {
                    version: 1,
                    keyspace: keyspace.into(),
                    source: source_id,
                    target: uuid::Uuid::new_v4().to_string(),
                    complete: false,
                    survivors: 0,
                };
                persist(&target_dir, MARKER, &marker)?;
                marker
            }
        };
        if marker.version != 1
            || marker.keyspace != keyspace
            || identity(&source_dir)?.as_ref() != Some(&marker.source)
        {
            return Err(invalid("source incarnation or cutover provenance changed"));
        }
        if marker.complete {
            validate(&self.storage, keyspace, &marker)?;
            return Ok(MigrationReport {
                survivors: marker.survivors,
            });
        }
        if identity(&target_dir)?.is_none() {
            if self.storage.table_schema(&target).is_some() {
                return Err(invalid("target lost its incarnation"));
            }
            persist(
                &target_dir,
                IDENTITY,
                &Incarnation {
                    id: marker.target.clone(),
                },
            )?;
        }
        self.storage.register_table(rdf_triples_schema(keyspace))?;
        validate(&self.storage, keyspace, &marker)?;
        self.persist_catalog(keyspace)?;
        self.storage.flush(&target)?;
        let index_options = std::collections::HashMap::new();
        if !self.storage.register_index_in_engine(
            &target,
            ferrosa_storage::engine::IndexToRegister {
                index_name: OBJECT_INDEX_NAME,
                index_type: ferrosa_index::IndexType::BTree,
                target_col: OBJECT_COLUMN,
                partition_key_columns: &[],
                filter_predicate: None,
                options: &index_options,
                defer_until_table_exists: false,
                site: "offline RDF migration",
            },
        )? {
            return Err(invalid("object index declaration refused"));
        }
        let mut stream = self.storage.range_iter_fragmented(&source, None, None);
        let mut count = 0u64;
        while let Some(fragment) = stream.next().await {
            let fragment = fragment?;
            decode_partition(fragment.key.key.as_bytes())?;
            for row in &fragment.rows {
                let Some((predicate, term)) = decode_v1_row(row)? else {
                    continue;
                };
                if options
                    .max_rows_per_run
                    .is_some_and(|budget| count >= budget as u64)
                {
                    self.storage.flush(&target)?;
                    return Err(invalid(
                        "copy budget exhausted; resume while still quiesced",
                    ));
                }
                let timestamp = row.primary_key_liveness.timestamp;
                let mut cells = vec![(
                    COL_OBJECT_TYPE,
                    CellValue::live(term.binding_type.as_bytes().to_vec(), timestamp),
                )];
                for (id, value) in [(COL_DATATYPE, &term.datatype), (COL_LANGUAGE, &term.lang)] {
                    if let Some(value) = value {
                        cells.push((id, CellValue::live(value.as_bytes().to_vec(), timestamp)));
                    }
                }
                cells.sort_by_key(|(id, _)| *id);
                self.storage.write(
                    &target,
                    &fragment.key,
                    Row {
                        clustering: encode_clustering(&predicate, &term)?,
                        cells,
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
                    },
                    timestamp,
                )?;
                count += 1;
                if count.is_multiple_of(256) {
                    self.storage.flush(&target)?;
                }
            }
        }
        self.storage.flush(&target)?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while !self.storage.index_is_current(&target, OBJECT_INDEX_NAME) {
            if tokio::time::Instant::now() >= deadline {
                return Err(invalid("object index is not ready"));
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // Every source identity must be reachable via its lexical index. The
        // independently counted target forbids extras left by an invalid resume.
        let mut stream = self.storage.range_iter_fragmented(&source, None, None);
        let mut checked = 0u64;
        while let Some(fragment) = stream.next().await {
            let fragment = fragment?;
            for row in &fragment.rows {
                let Some((predicate, term)) = decode_v1_row(row)? else {
                    continue;
                };
                let clustering = encode_clustering(&predicate, &term)?;
                let mut found = false;
                self.storage.read_by_index_each_bytes(
                    &target,
                    OBJECT_INDEX_NAME,
                    term.value.as_bytes(),
                    &mut |part| {
                        if part.key == fragment.key {
                            found = part.rows.iter().any(|row| {
                                row.clustering == clustering
                                    && decode_v2_row(row).ok().flatten().as_ref()
                                        == Some(&(predicate.clone(), term.clone()))
                            });
                        }
                        if found {
                            std::ops::ControlFlow::Break(())
                        } else {
                            std::ops::ControlFlow::Continue(())
                        }
                    },
                )?;
                if !found {
                    return Err(invalid("target/index identity validation failed"));
                }
                checked += 1;
            }
        }
        let mut target_count = 0u64;
        let mut stream = self.storage.range_iter_fragmented(&target, None, None);
        while let Some(fragment) = stream.next().await {
            let fragment = fragment?;
            decode_partition(fragment.key.key.as_bytes())?;
            for row in &fragment.rows {
                if decode_v2_row(row)?.is_some() {
                    target_count += 1;
                }
            }
        }
        if count != checked || count != target_count {
            return Err(invalid("source/target survivor counts differ"));
        }
        validate(&self.storage, keyspace, &marker)?;
        marker.complete = true;
        marker.survivors = count;
        persist(&target_dir, MARKER, &marker)?;
        Ok(MigrationReport { survivors: count })
    }
}
