//! Full-text search index (FTI format).
//!
//! This module provides an inverted index pipeline for CQL text columns:
//!
//! - [`analyzer`]: text analysis (tokenization, stop-word removal, stemming)
//! - [`stemmer`]: Porter stemmer for English
//! - [`builder`]: in-memory inverted index builder that serializes to FTI format
//! - [`reader`]: binary reader with binary-search term lookup

pub mod analyzer;
pub mod builder;
pub mod reader;
pub mod stemmer;
