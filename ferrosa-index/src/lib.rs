pub mod btree;
pub mod composite;
pub mod filtered;
pub mod hash;
pub mod phonetic;
pub mod vector;

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    Composite {
        columns: Vec<String>,
    },
    Phonetic {
        algorithm: PhoneticAlgorithm,
    },
    Filtered {
        predicate: FilterPredicate,
        inner: Box<IndexType>,
    },
    Vector {
        method: VectorMethod,
        metric: DistanceMetric,
        dimensions: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum VectorMethod {
    Hnsw { m: u16, ef_construction: u16 },
    IvfFlat { lists: u32 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DistanceMetric {
    L2,
    Cosine,
    InnerProduct,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PhoneticAlgorithm {
    Soundex,
    Metaphone,
    DoubleMetaphone,
    Caverphone,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowPosition {
    pub partition_key: Vec<u8>,
    pub clustering_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexKey {
    Bytes(Vec<u8>),
    Text(String),
    Composite(Vec<Vec<u8>>),
    Vector(Vec<f32>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexFiles {
    pub data_path: PathBuf,
    pub meta_path: PathBuf,
    pub meta: IndexFileMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexFileMeta {
    pub index_type: IndexType,
    pub index_name: String,
    pub row_count: u64,
    pub build_timestamp: u64,
    pub sstable_id: String,
    pub file_size: u64,
    pub checksum: u32,
}

#[derive(Debug, Clone)]
pub struct IndexConfig {
    pub index_type: IndexType,
    pub column_positions: Vec<usize>,
    pub output_dir: PathBuf,
    pub sstable_prefix: String,
    pub index_name: String,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct IndexCapabilities: u8 {
        const POINT_LOOKUP = 0b0001;
        const RANGE_SCAN   = 0b0010;
        const NEAREST      = 0b0100;
        const PHONETIC     = 0b1000;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterPredicate {
    pub column: String,
    pub op: FilterOp,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FilterOp {
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_type_serde_roundtrip() {
        let types = vec![
            IndexType::BTree,
            IndexType::Hash,
            IndexType::Composite {
                columns: vec!["a".into(), "b".into()],
            },
            IndexType::Phonetic {
                algorithm: PhoneticAlgorithm::Soundex,
            },
            IndexType::Vector {
                method: VectorMethod::Hnsw {
                    m: 16,
                    ef_construction: 200,
                },
                metric: DistanceMetric::Cosine,
                dimensions: 768,
            },
            IndexType::Vector {
                method: VectorMethod::IvfFlat { lists: 100 },
                metric: DistanceMetric::L2,
                dimensions: 1536,
            },
        ];
        for t in &types {
            let json = serde_json::to_string(t).unwrap();
            let back: IndexType = serde_json::from_str(&json).unwrap();
            assert_eq!(*t, back);
        }
    }

    #[test]
    fn row_position_equality() {
        let a = RowPosition {
            partition_key: vec![1, 2, 3],
            clustering_key: vec![4, 5],
        };
        let b = a.clone();
        assert_eq!(a, b);
        let c = RowPosition {
            partition_key: vec![1, 2, 3],
            clustering_key: vec![4, 6],
        };
        assert_ne!(a, c);
    }

    #[test]
    fn index_key_variants() {
        let keys = vec![
            IndexKey::Bytes(vec![0xFF, 0x00]),
            IndexKey::Text("hello".into()),
            IndexKey::Composite(vec![vec![1, 2], vec![3, 4]]),
            IndexKey::Vector(vec![0.1, 0.2, 0.3]),
        ];
        for k in &keys {
            let json = serde_json::to_string(k).unwrap();
            let back: IndexKey = serde_json::from_str(&json).unwrap();
            assert_eq!(*k, back);
        }
    }

    #[test]
    fn index_capabilities_bitflags() {
        let caps = IndexCapabilities::POINT_LOOKUP | IndexCapabilities::RANGE_SCAN;
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(caps.contains(IndexCapabilities::RANGE_SCAN));
        assert!(!caps.contains(IndexCapabilities::NEAREST));
        assert!(!caps.contains(IndexCapabilities::PHONETIC));
    }

    #[test]
    fn filter_predicate_serde() {
        let pred = FilterPredicate {
            column: "status".into(),
            op: FilterOp::Eq,
            value: b"active".to_vec(),
        };
        let json = serde_json::to_string(&pred).unwrap();
        let back: FilterPredicate = serde_json::from_str(&json).unwrap();
        assert_eq!(pred, back);
    }

    #[test]
    fn filtered_index_type_wraps_inner() {
        let filtered = IndexType::Filtered {
            predicate: FilterPredicate {
                column: "status".into(),
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            },
            inner: Box::new(IndexType::BTree),
        };
        let json = serde_json::to_string(&filtered).unwrap();
        let back: IndexType = serde_json::from_str(&json).unwrap();
        assert_eq!(filtered, back);
    }
}
