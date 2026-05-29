//! Quantized vector index formats and builders.
//!
//! Module: Quantized vector index support for page-addressable `.qvec` artifacts.
//! Correctness: Correct when deterministic builders emit stable manifests and pages without depending on storage or CQL types.
//! Last revised: 2026-05-29
//! Last changed: Added the quantized IVFFlat builder seam for tiered `.qvec` artifact generation.

pub mod ivf;
