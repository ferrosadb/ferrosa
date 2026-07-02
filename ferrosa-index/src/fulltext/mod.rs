//! Full-text index (FTI) implementation.
//!
//! Provides an inverted-index pipeline for CQL `column = fts_match(query)` queries:
//!
//! - [`analyzer`]: text analyzers (Standard, Simple, Keyword).
//! - [`builder`]: FTI builder — ingests documents, serializes to bytes.
//! - [`reader`]: FTI reader — deserializes bytes, executes BM25-scored queries.
//! - [`query`]: query parser — converts query strings into [`query::FtsQuery`] trees.
//! - [`scoring`]: BM25 scoring function.
//! - [`merge`]: compaction merge — merges two FTI byte buffers into one.
//! - [`stream`]: bounded-memory streaming search over on-disk FTI sidecars.
//! - [`topk`]: bounded top-k hit selection for `LIMIT k` queries.

pub mod analyzer;
pub mod builder;
pub mod keys;
pub mod merge;
pub mod query;
pub mod reader;
pub mod scoring;
pub mod stream;
pub mod topk;
