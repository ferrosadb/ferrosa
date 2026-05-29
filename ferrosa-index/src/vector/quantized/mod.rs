//! Quantized vector index formats, codecs, builders, and readers.
//!
//! Module: Quantized vector index support for page-addressable `.qvec` artifacts, scalar codecs, and deterministic IVFFlat artifact generation.
//! Correctness: Correct when malformed containers fail loudly, deterministic builders emit stable manifests/pages, readers range-read pages without materializing whole artifacts, and codecs round-trip within declared error bounds.
//! Last revised: 2026-05-29
//! Last changed: Integrated container, codec, and quantized IVFFlat builder modules for HVQ `.qvec` artifacts.

pub mod codec;
pub mod container;
pub mod ivf;
