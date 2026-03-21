//! Per-query bump arena for UDF type conversion allocations.
//!
//! Reduces allocation pressure during UDF evaluation by bulk-freeing
//! all intermediate WitCqlValue allocations when the query completes.

use bumpalo::Bump;

/// Per-query arena. Created at query start, dropped at query end.
/// Optimizes the CqlValue -> WitCqlValue intermediate allocations.
///
/// Note: The WitCqlValue -> Val conversion produces owned Val::String
/// and Val::List values required by Wasmtime's API — these cannot use
/// the arena. The benefit is in argument preparation (once per arg per row).
pub struct UdfArena {
    bump: Bump,
}

impl UdfArena {
    /// Create a new arena with default capacity.
    pub fn new() -> Self {
        Self { bump: Bump::new() }
    }

    /// Create a new arena pre-allocated for an estimated number of rows.
    pub fn with_capacity(estimated_rows: usize, args_per_row: usize) -> Self {
        // ~64 bytes per WitCqlValue on average
        let capacity = estimated_rows * args_per_row * 64;
        Self {
            bump: Bump::with_capacity(capacity),
        }
    }

    /// Get a reference to the underlying bump allocator.
    pub fn bump(&self) -> &Bump {
        &self.bump
    }

    /// Reset the arena, freeing all allocations.
    pub fn reset(&mut self) {
        self.bump.reset();
    }
}

impl Default for UdfArena {
    fn default() -> Self {
        Self::new()
    }
}
