//! `ferrosa-view` — materialized-view core.
//!
//! Protocol-agnostic, I/O-free core for ferrosa materialized views:
//!
//! - [`metadata`] — [`ViewMetadata`], [`ViewKind`], [`ViewColumn`]: the
//!   schema-replicated description of a view.
//! - [`validate`] — [`validate_view_def`], the DDL validation rules shared by
//!   every frontend (CQL and the forthcoming Postgres frontend).
//!
//! This crate is deliberately **pure**: it depends only on `ferrosa-schema` (and
//! leaves) and never on `ferrosa-storage`, `ferrosa-cluster` (Accord), or any
//! protocol frontend. Maintenance execution (the Accord-coordinated base+view
//! commit) lives in the engine layer and *consumes* this crate. See
//! `specs/materialized-views/dsm-coupling.md` and `dsm-proposed.md` for the
//! forbidden-edge rules this layering enforces.
#![forbid(unsafe_code)]

pub mod metadata;
pub mod validate;

pub use metadata::{ColumnSource, ViewColumn, ViewKind, ViewMetadata, ViewPredicate};
pub use validate::{validate_view_def, ViewDefError};
