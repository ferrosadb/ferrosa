//! Graph query executor.

pub mod aggregate;
pub mod eval;
pub mod expand;
pub mod result;

pub use eval::{eval_expr, filter_passes, partition_to_json};
pub use result::{GraphResult, QueryStats};
