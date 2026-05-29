//! Quantized vector index formats, codecs, and readers.
//!
//! Module: Quantized vector index support for page-addressable `.qvec` artifacts and scalar codecs.
//! Correctness: Correct when malformed containers fail loudly, readers range-read pages without materializing whole artifacts, and codecs round-trip within declared error bounds.
//! Last revised: 2026-05-29
//! Last changed: Integrated container and codec modules for HVQ `.qvec` artifacts.

pub mod codec;
pub mod container;
