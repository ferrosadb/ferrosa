//! Schema metadata types: keyspaces, columns, tables, indexes, functions, aggregates.

pub mod aggregate;
pub mod column;
pub mod function;
pub mod index;
pub mod keyspace;
pub mod table;
pub mod user_type;

pub use aggregate::UserAggregateMetadata;
pub use column::{ClusteringOrder, ColumnKind, ColumnMask, ColumnMetadata};
pub use function::UserFunctionMetadata;
pub use index::IndexMetadata;
pub use keyspace::{KeyspaceMetadata, KeyspaceUpdates, ReplicationParams};
pub use table::{CachingParams, TableFlag, TableMetadata, TableParams, TableUpdates};
pub use user_type::UserTypeMetadata;
