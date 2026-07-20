//! CRDT per-element collection cell encoding/assembly.
//!
//! The implementation lives in [`ferrosa_row_bridge::collection`] — one layer
//! down — because both the write-side builder ([`build_collection_cells`]) and
//! the read-side assembly ([`assemble_collection`]) use that crate's
//! `encode_value`/`decode_value` codecs and `ferrosa_common::reconcile`, and the
//! primary SELECT read path (`ferrosa_row_bridge::row::decode_output_row`) must
//! call the assembly directly. This module re-exports them so existing
//! `crate::collection_cells::*` call sites (the write paths and the metadata
//! read variant in [`crate::bridge`]) resolve unchanged.
//!
//! Last revised: 2026-07-20
//! Last changed: Relocated the implementation into `ferrosa-row-bridge` so the
//!   lower-level SELECT read path can assemble complex columns; this module is
//!   now a thin re-export (crdt-collections increment 3, part D-read-2).

pub use ferrosa_row_bridge::collection::{
    assemble_collection, assemble_column_cells, build_collection_cells, list_cell_path,
    timeuuid_time, AssembleError, CollectionOp, UnsupportedCollectionOp,
};
