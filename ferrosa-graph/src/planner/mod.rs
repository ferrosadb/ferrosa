//! Graph query planner.

pub mod logical;

pub use logical::{validate, LogicalPlan, ResolvedTable};
