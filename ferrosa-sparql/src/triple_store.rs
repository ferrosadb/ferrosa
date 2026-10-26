//! RDF triple ↔ CQL row translation.
//!
//! Maps RDF triples to ferrosa's CQL storage model. Each keyspace gets an
//! `rdf_triples_v2` table keyed by graph/subject and predicate/lexical/identity.
//! The separate v1 schema and decoder are retained for explicit offline migration.

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_storage::TableId;

/// Table name for RDF triples within a keyspace.
pub const RDF_TRIPLES_TABLE: &str = "rdf_triples_v2";
/// Historical table, retained for explicit migration only.
pub const RDF_TRIPLES_V1_TABLE: &str = "rdf_triples";

/// The `object` column: clustering component 1 of the `(predicate, object, identity)`
/// clustering key.
///
/// A clustering column's value is not a row cell — it lives in the clustering-key
/// bytes — so an index on it has to be declared through the clustering-column
/// path. Naming it in one place keeps the schema below and the index the SPARQL
/// engine declares on registration from drifting apart.
pub const OBJECT_COLUMN: &str = "object";

/// V2 regular cell ordinals, in storage column-name order.
pub const COL_DATATYPE: u16 = 0;
pub const COL_LANGUAGE: u16 = 1;
pub const COL_OBJECT_TYPE: u16 = 2;

/// Historical v1 schema, for migration input only.
///
/// Schema: `((graph, subject), predicate, object)` with regular columns
/// `object_type`, `datatype`, `language`.
pub fn rdf_triples_v1_schema(keyspace: &str) -> TableSchema {
    TableSchema {
        keyspace: keyspace.to_string(),
        table: RDF_TRIPLES_V1_TABLE.to_string(),
        // Composite partition key: (graph, subject) encoded as CompositeType
        key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                   org.apache.cassandra.db.marshal.UTF8Type,\
                   org.apache.cassandra.db.marshal.UTF8Type)"
            .to_string(),
        clustering_columns: vec![
            ColumnDefinition {
                name: "predicate".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            ColumnDefinition {
                name: OBJECT_COLUMN.to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
        ],
        static_columns: vec![],
        regular_columns: vec![
            ColumnDefinition {
                name: "object_type".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            ColumnDefinition {
                name: "datatype".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            ColumnDefinition {
                name: "language".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
        ],
        extensions: Default::default(),
    }
}

/// Fresh v2 schema. Metadata columns follow Cassandra's byte-name ordering;
/// the v1 schema above preserves its historical ordinals for migration.
pub fn rdf_triples_schema(keyspace: &str) -> TableSchema {
    let mut schema = rdf_triples_v1_schema(keyspace);
    schema.table = RDF_TRIPLES_TABLE.into();
    schema.clustering_columns.push(ColumnDefinition {
        name: "object_identity".into(),
        type_name: "org.apache.cassandra.db.marshal.BytesType".into(),
    });
    schema
        .regular_columns
        .sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
    schema
}

/// Build a TableId for the RDF triples table in the given keyspace.
pub fn triples_table_id(keyspace: &str) -> TableId {
    TableId::new(keyspace, RDF_TRIPLES_TABLE)
}

/// Legacy partition-key helper retained for callers constructing v1 fixtures.
/// New data uses [`checked_partition_key`] before any storage mutation.
///
/// The partition key is `(graph, subject)` concatenated as length-prefixed
/// components, matching CQL composite partition key encoding.
pub fn partition_key(graph: &str, subject: &str) -> DecoratedKey {
    let mut key_bytes = Vec::new();
    // CQL composite key encoding: [u16 len][bytes] for each component
    key_bytes.extend_from_slice(&(graph.len() as u16).to_be_bytes());
    key_bytes.extend_from_slice(graph.as_bytes());
    key_bytes.push(0); // end-of-component
    key_bytes.extend_from_slice(&(subject.len() as u16).to_be_bytes());
    key_bytes.extend_from_slice(subject.as_bytes());
    key_bytes.push(0);
    DecoratedKey::new(PartitionKey::new(key_bytes))
}

/// An RDF triple with optional graph, datatype, and language tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdfTriple {
    pub graph: String,
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub object_type: ObjectType,
    pub datatype: Option<String>,
    pub language: Option<String>,
}

/// The type of an RDF object value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectType {
    Iri,
    Literal,
    BlankNode,
}

impl ObjectType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Iri => "iri",
            Self::Literal => "literal",
            Self::BlankNode => "bnode",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "iri" => Self::Iri,
            "bnode" => Self::BlankNode,
            _ => Self::Literal,
        }
    }
}

/// RDF 1.1 simple literals have this datatype.
pub const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
/// RDF 1.1 language-tagged literals have this datatype.
pub const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

use crate::error::SparqlError;
use crate::results::Binding;
use ferrosa_sstable::types::Row;

fn invalid(reason: &str) -> SparqlError {
    SparqlError::Execution(format!("invalid RDF term identity: {reason}"))
}

/// Canonical RDF identity, without value-space coercion. Unknown datatypes and
/// ill-typed lexical values are terms too. Only language case and the RDF 1.1
/// simple-literal convention are normalized.
pub fn canonical_term(
    kind: &str,
    value: &str,
    datatype: Option<&str>,
    language: Option<&str>,
) -> Result<Binding, SparqlError> {
    let (datatype, lang) = match kind {
        "uri" | "bnode" => {
            if datatype.is_some() || language.is_some() {
                return Err(invalid("non-literal has literal metadata"));
            }
            (None, None)
        }
        "literal" => match language {
            Some(lang) => {
                if lang.is_empty()
                    || !lang.is_ascii()
                    || lang.split('-').any(|part| {
                        part.is_empty() || !part.chars().all(|c| c.is_ascii_alphanumeric())
                    })
                    || !lang
                        .split('-')
                        .next()
                        .unwrap_or("")
                        .chars()
                        .all(|c| c.is_ascii_alphabetic())
                {
                    return Err(invalid("invalid language tag"));
                }
                if datatype.is_some_and(|dt| dt != RDF_LANG_STRING) {
                    return Err(invalid("language/datatype mismatch"));
                }
                (
                    Some(RDF_LANG_STRING.to_owned()),
                    Some(lang.to_ascii_lowercase()),
                )
            }
            None => {
                let dt = datatype.unwrap_or(XSD_STRING);
                if dt.is_empty() || dt == RDF_LANG_STRING {
                    return Err(invalid("missing datatype or language"));
                }
                (Some(dt.to_owned()), None)
            }
        },
        _ => return Err(invalid("unsupported object kind")),
    };
    Ok(Binding {
        binding_type: kind.into(),
        value: value.into(),
        datatype,
        lang,
    })
}

fn component(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), SparqlError> {
    let len = u16::try_from(bytes.len()).map_err(|_| invalid("component exceeds 65535 bytes"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

/// Checked CompositeType partition encoding (with end-of-component bytes).
pub fn checked_partition_key(graph: &str, subject: &str) -> Result<DecoratedKey, SparqlError> {
    let mut out = Vec::new();
    component(&mut out, graph.as_bytes())?;
    out.push(0);
    component(&mut out, subject.as_bytes())?;
    out.push(0);
    u16::try_from(out.len()).map_err(|_| invalid("partition key exceeds 65535 bytes"))?;
    Ok(DecoratedKey::new(PartitionKey::new(out)))
}

/// Strict partition decoder; never returns lossy text or guessed components.
pub fn decode_partition(bytes: &[u8]) -> Result<(String, String), SparqlError> {
    if bytes.len() > u16::MAX as usize {
        return Err(invalid("partition key exceeds 65535 bytes"));
    }
    let mut input = bytes;
    let graph = text(take_component(&mut input)?)?;
    if input.first() != Some(&0) {
        return Err(invalid("partition separator"));
    }
    input = &input[1..];
    let subject = text(take_component(&mut input)?)?;
    if input != [0] {
        return Err(invalid("partition trailing bytes or separator"));
    }
    Ok((graph, subject))
}

/// V2 clustering uses storage's three length-prefixed components WITHOUT
/// partition separators. Identity = version, kind, tagged datatype, tagged
/// language; its whole encoded length must fit the outer u16 component too.
pub fn encode_clustering(predicate: &str, term: &Binding) -> Result<Vec<u8>, SparqlError> {
    let term = canonical_term(
        &term.binding_type,
        &term.value,
        term.datatype.as_deref(),
        term.lang.as_deref(),
    )?;
    let kind = match term.binding_type.as_str() {
        "uri" => 0,
        "bnode" => 1,
        "literal" => 2,
        _ => unreachable!(),
    };
    let mut identity = vec![2, kind, 0x10];
    component(
        &mut identity,
        term.datatype.as_deref().unwrap_or("").as_bytes(),
    )?;
    identity.push(0x11);
    component(&mut identity, term.lang.as_deref().unwrap_or("").as_bytes())?;
    let mut out = Vec::new();
    component(&mut out, predicate.as_bytes())?;
    component(&mut out, term.value.as_bytes())?;
    component(&mut out, &identity)?;
    Ok(out)
}

fn take_component<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], SparqlError> {
    if input.len() < 2 {
        return Err(invalid("truncated component length"));
    }
    let len = u16::from_be_bytes([input[0], input[1]]) as usize;
    if input.len() - 2 < len {
        return Err(invalid("truncated component value"));
    }
    let value = &input[2..2 + len];
    *input = &input[2 + len..];
    Ok(value)
}
fn text(bytes: &[u8]) -> Result<String, SparqlError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| invalid("invalid UTF-8"))
}
fn metadata(row: &Row, column: u16) -> Result<Option<String>, SparqlError> {
    let mut values = row.cells.iter().filter(|(id, _)| *id == column);
    let value = values
        .next()
        .and_then(|(_, cell)| cell.value.as_deref())
        .map(text)
        .transpose()?;
    if values.next().is_some() {
        return Err(invalid("duplicate metadata cell"));
    }
    Ok(value)
}

// Reconciliation retains the old row deletion even after a newer INSERT.
// A deletion marker alone therefore does not mean the current row is dead.
fn row_is_deleted(row: &Row) -> bool {
    !row.deletion.is_live()
        && row.primary_key_liveness.timestamp <= row.deletion.marked_for_delete_at
        && !row
            .cells
            .iter()
            .any(|(_, cell)| cell.is_live() && cell.timestamp > row.deletion.marked_for_delete_at)
}

/// Decode and validate v2 identity and redundant metadata. Tombstones are not
/// terms; malformed live rows are errors, never omitted query results.
pub fn decode_v2_row(row: &Row) -> Result<Option<(String, Binding)>, SparqlError> {
    if row_is_deleted(row) {
        return Ok(None);
    }
    let mut input = row.clustering.as_slice();
    let predicate = text(take_component(&mut input)?)?;
    let lexical = text(take_component(&mut input)?)?;
    let identity = take_component(&mut input)?;
    if !input.is_empty() {
        return Err(invalid("trailing clustering bytes"));
    }
    if identity.len() < 3 || identity[0] != 2 {
        return Err(invalid("unsupported identity version"));
    }
    let kind = match identity[1] {
        0 => "uri",
        1 => "bnode",
        2 => "literal",
        _ => return Err(invalid("unknown kind tag")),
    };
    if identity[2] != 0x10 {
        return Err(invalid("datatype tag"));
    }
    let mut rest = &identity[3..];
    let dt = text(take_component(&mut rest)?)?;
    if rest.first() != Some(&0x11) {
        return Err(invalid("language tag field"));
    }
    rest = &rest[1..];
    let lang = text(take_component(&mut rest)?)?;
    if !rest.is_empty() {
        return Err(invalid("trailing identity bytes"));
    }
    let term = canonical_term(
        kind,
        &lexical,
        (!dt.is_empty()).then_some(dt.as_str()),
        (!lang.is_empty()).then_some(lang.as_str()),
    )?;
    // Reject noncanonical encodings rather than permitting multiple keys for
    // the same term (e.g. uppercase language or absent xsd:string).
    if encode_clustering(&predicate, &term)? != row.clustering {
        return Err(invalid("noncanonical key"));
    }
    if metadata(row, COL_OBJECT_TYPE)?.as_deref() != Some(kind)
        || metadata(row, COL_DATATYPE)? != term.datatype
        || metadata(row, COL_LANGUAGE)? != term.lang
    {
        return Err(invalid("key/cell mismatch"));
    }
    Ok(Some((predicate, term)))
}

/// Decode a historical two-component v1 survivor for explicit offline migration.
/// This decoder is never used to serve or union v1 into v2 reads. Historical
/// ordinals remain object_type=0, datatype=1, language=2.
pub fn decode_v1_row(row: &Row) -> Result<Option<(String, Binding)>, SparqlError> {
    if row_is_deleted(row) {
        return Ok(None);
    }
    let mut input = row.clustering.as_slice();
    let predicate = text(take_component(&mut input)?)?;
    let lexical = text(take_component(&mut input)?)?;
    if !input.is_empty() {
        return Err(invalid("v1 trailing clustering bytes"));
    }
    // A reconciled mixture of independently timestamped metadata cannot be
    // interpreted as one original RDF term. Refuse it instead of guessing.
    if row.cells.iter().any(|(id, cell)| {
        *id > 2
            || cell.path.is_some()
            || cell.ttl != 0
            || cell.timestamp != row.primary_key_liveness.timestamp
            || !cell.is_live()
    }) || row.primary_key_liveness.ttl != 0
    {
        return Err(invalid("v1 ambiguous, expiring or unknown metadata"));
    }
    let kind = metadata(row, 0)?.ok_or_else(|| invalid("v1 missing kind"))?;
    let datatype = metadata(row, 1)?;
    let language = metadata(row, 2)?;
    Ok(Some((
        predicate,
        canonical_term(&kind, &lexical, datatype.as_deref(), language.as_deref())?,
    )))
}

/// Fence legacy data until explicit offline migration durably publishes a
/// complete cutover bound to both table incarnations. Fresh v2 needs no marker.
pub fn refuse_legacy(
    storage: &ferrosa_storage::StorageEngine,
    keyspace: &str,
) -> Result<(), SparqlError> {
    if crate::migration::admit(storage, keyspace)? {
        return Ok(());
    }
    let legacy = TableId::new(keyspace, "rdf_triples");
    if storage.table_schema(&legacy).is_some()
        || storage
            .table_sstable_dir(&legacy)
            .try_exists()
            .map_err(|e| {
                SparqlError::Execution(format!("cannot inspect legacy RDF directory: {e}"))
            })?
        || storage
            .replay_set_aside_status()
            .tables()
            .contains_key(&legacy.to_string())
    {
        return Err(SparqlError::Execution(format!(
            "legacy {keyspace}.rdf_triples dataset requires explicit RDF v2 migration; migration is pending, serving and writes refused"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_key_encodes_graph_and_subject() {
        let key = partition_key("default", "http://example.org/alice");
        assert!(!key.key.as_bytes().is_empty());
    }

    #[test]
    fn object_type_roundtrip() {
        assert_eq!(ObjectType::parse(ObjectType::Iri.as_str()), ObjectType::Iri);
        assert_eq!(
            ObjectType::parse(ObjectType::Literal.as_str()),
            ObjectType::Literal
        );
        assert_eq!(
            ObjectType::parse(ObjectType::BlankNode.as_str()),
            ObjectType::BlankNode
        );
    }
}

#[cfg(test)]
mod identity_codec_tests {
    use super::*;
    use crate::results::Binding;
    use ferrosa_common::CellValue;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    fn term(kind: &str, dt: Option<&str>, lang: Option<&str>) -> Binding {
        canonical_term(kind, "\0|:é", dt, lang).unwrap()
    }

    #[test]
    fn v2_codec_is_injective_strict_and_uses_storage_component_conventions() {
        let terms = vec![
            term("uri", None, None),
            term("bnode", None, None),
            term("literal", None, None),
            term("literal", Some("urn:a|b"), None),
            term("literal", Some("urn:a"), None),
            term("literal", None, Some("EN")),
            term("literal", None, Some("fr")),
        ];
        let mut keys = std::collections::HashSet::new();
        for t in terms {
            let clustering = encode_clustering("urn:p", &t).unwrap();
            ferrosa_common::schema::validate_clustering_shape(
                &rdf_triples_schema("rdf").clustering_columns,
                &clustering,
            )
            .unwrap();
            assert!(keys.insert(clustering.clone()));
            let mut row = Row {
                clustering,
                cells: vec![(
                    COL_OBJECT_TYPE,
                    CellValue::live(t.binding_type.as_bytes().to_vec(), 1),
                )],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1),
            };
            if let Some(dt) = &t.datatype {
                row.cells
                    .push((COL_DATATYPE, CellValue::live(dt.as_bytes().to_vec(), 1)));
            }
            if let Some(lang) = &t.lang {
                row.cells
                    .push((COL_LANGUAGE, CellValue::live(lang.as_bytes().to_vec(), 1)));
            }
            let (p, decoded) = decode_v2_row(&row).unwrap().unwrap();
            assert_eq!(p, "urn:p");
            assert_eq!(decoded, t);
            for length in 0..row.clustering.len() {
                let mut bad = row.clone();
                bad.clustering.truncate(length);
                assert!(decode_v2_row(&bad).is_err(), "truncation {length}");
            }
            let mut bad = row.clone();
            bad.clustering.push(0);
            assert!(decode_v2_row(&bad).is_err());
            let identity_start = 2 + "urn:p".len() + 2 + t.value.len() + 2;
            let mut bad = row.clone();
            bad.clustering[identity_start] = 99;
            assert!(decode_v2_row(&bad).is_err());
            let mut bad = row.clone();
            bad.cells[0].1 = CellValue::live(b"wrong".to_vec(), 1);
            assert!(decode_v2_row(&bad).is_err());
            let mut bad = row.clone();
            bad.cells.clear();
            assert!(decode_v2_row(&bad).is_err());
        }
    }

    #[test]
    fn v2_codec_bounds_and_literal_conventions() {
        let plain = canonical_term("literal", "01", None, None).unwrap();
        assert_eq!(
            plain,
            canonical_term("literal", "01", Some(XSD_STRING), None).unwrap()
        );
        assert_eq!(
            term("literal", None, Some("EN")),
            term("literal", Some(RDF_LANG_STRING), Some("en"))
        );
        assert!(canonical_term("uri", "x", Some(XSD_STRING), None).is_err());
        assert!(canonical_term("literal", "x", Some(XSD_STRING), Some("en")).is_err());
        let max = "x".repeat(65535);
        let over = "x".repeat(65536);
        assert!(checked_partition_key(&"x".repeat(65529), "").is_ok());
        assert!(checked_partition_key(&"x".repeat(65530), "").is_err());
        assert!(checked_partition_key(&max, &max).is_err());
        assert!(checked_partition_key(&over, "s").is_err());
        assert!(checked_partition_key("g", &over).is_err());
        let t = canonical_term("literal", &max, None, None).unwrap();
        assert!(encode_clustering(&max, &t).is_ok());
        assert!(encode_clustering(&over, &t).is_err());
        assert!(
            encode_clustering("p", &canonical_term("literal", &over, None, None).unwrap()).is_err()
        );
        // Identity is version + kind + two tagged u16-length fields: 8 bytes.
        let dt = format!("urn:{}", "x".repeat(65535 - 8 - 4));
        assert!(encode_clustering(
            "p",
            &canonical_term("literal", "x", Some(&dt), None).unwrap()
        )
        .is_ok());
        assert!(encode_clustering(
            "p",
            &canonical_term("literal", "x", Some(&(dt + "x")), None).unwrap()
        )
        .is_err());
    }
}
