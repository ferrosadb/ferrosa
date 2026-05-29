//! Quantized vector index formats and readers.
//!
//! Module: Quantized vector index support for page-addressable `.qvec` artifacts.
//! Correctness: Correct when malformed containers fail loudly and later readers can range-read pages without materializing whole artifacts.
//! Last revised: 2026-05-29
//! Last changed: Added the container module seam for HVQ `.qvec` artifacts.

pub mod container;
