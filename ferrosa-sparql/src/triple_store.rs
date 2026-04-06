//! RDF triple ↔ CQL row translation.
//!
//! Maps RDF triples to ferrosa's CQL storage model. Each keyspace gets an
//! `rdf_triples` table with composite primary key `((graph, subject), predicate, object)`.

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_storage::TableId;

/// Table name for RDF triples within a keyspace.
pub const RDF_TRIPLES_TABLE: &str = "rdf_triples";

/// Column positions in the rdf_triples table schema.
pub const COL_OBJECT_TYPE: u16 = 0;
pub const COL_DATATYPE: u16 = 1;
pub const COL_LANGUAGE: u16 = 2;

/// Build a TableId for the RDF triples table in the given keyspace.
pub fn triples_table_id(keyspace: &str) -> TableId {
    TableId::new(keyspace, RDF_TRIPLES_TABLE)
}

/// Build a partition key for an RDF subject in a named graph.
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
