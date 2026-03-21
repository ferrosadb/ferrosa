//! Guest UDA crate targeting the `uda` WASM Component Model world.
//!
//! This single component multiplexes two UDAs:
//!
//!   - `weighted_avg(double, double) -> double`
//!   - `histogram(double)            -> map<text, bigint>`
//!
//! Dispatch strategy (a limitation of co-locating UDAs in one component):
//!
//!   - `accumulate`: 2 args => weighted_avg, 1 arg => histogram
//!   - `merge`:      17-byte state => weighted_avg, 40-byte state => histogram
//!   - `serialize-state`: returns the state of whichever UDA received data last
//!
//! NOTE: In production each UDA should be its own separate component so the
//! host can load and unload them independently without any multiplexing.

// wit-bindgen generates the Rust bindings from the WIT world "uda".
// The macro expands into the current module, generating:
//   - `CqlValue` (re-exported from the types interface)
//   - `Guest` trait (one method per exported function)
//   - `export!` macro for registering the implementation
wit_bindgen::generate!({
    path: "wit",
    world: "uda",
});

mod histogram;
mod weighted_avg;

use std::cell::RefCell;

// ---------------------------------------------------------------------------
// Active UDA tracker — used to dispatch serialize_state correctly
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum ActiveUda {
    None,
    WeightedAvg,
    Histogram,
}

thread_local! {
    static ACTIVE: RefCell<ActiveUda> = RefCell::new(ActiveUda::None);
}

// ---------------------------------------------------------------------------
// Serialized state sizes — used for merge dispatch
// ---------------------------------------------------------------------------

/// Byte length of a serialized weighted_avg state: 8 (f64) + 8 (f64) + 1 (bool).
const WEIGHTED_AVG_STATE_LEN: usize = 17;

/// Byte length of a serialized histogram state: 5 * 8 (i64).
const HISTOGRAM_STATE_LEN: usize = 40;

// ---------------------------------------------------------------------------
// WIT export implementation
// ---------------------------------------------------------------------------

struct GuestUda;

impl Guest for GuestUda {
    /// Reset internal state for the upcoming aggregation pass.
    ///
    /// The `init_cond` is forwarded to both sub-UDAs; each resets to zero
    /// and ignores any initial condition (standard Cassandra behavior when
    /// no INITCOND is specified in CREATE AGGREGATE).
    fn init(init_cond: Option<CqlValue>) -> Result<(), String> {
        ACTIVE.with(|a| *a.borrow_mut() = ActiveUda::None);
        // Clone to forward to both; Option<CqlValue> does not implement Copy.
        let ic2 = init_cond.clone();
        weighted_avg::init(init_cond)?;
        histogram::init(ic2)?;
        Ok(())
    }

    /// Process one row of input.
    ///
    /// Dispatch by argument count:
    ///   - 2 args -> weighted_avg(value double, weight double)
    ///   - 1 arg  -> histogram(value double)
    fn accumulate(args: Vec<CqlValue>) -> Result<(), String> {
        match args.len() {
            2 => {
                ACTIVE.with(|a| *a.borrow_mut() = ActiveUda::WeightedAvg);
                weighted_avg::accumulate(&args)
            }
            1 => {
                ACTIVE.with(|a| *a.borrow_mut() = ActiveUda::Histogram);
                histogram::accumulate(&args)
            }
            n => Err(format!(
                "guest-uda: accumulate received {n} args; \
                 expected 1 (histogram) or 2 (weighted_avg)"
            )),
        }
    }

    /// Merge a partial state from another replica.
    ///
    /// Dispatch by serialized state length:
    ///   - 17 bytes -> weighted_avg
    ///   - 40 bytes -> histogram
    fn merge(serialized_state: Vec<u8>) -> Result<(), String> {
        match serialized_state.len() {
            WEIGHTED_AVG_STATE_LEN => {
                ACTIVE.with(|a| *a.borrow_mut() = ActiveUda::WeightedAvg);
                weighted_avg::merge(&serialized_state)
            }
            HISTOGRAM_STATE_LEN => {
                ACTIVE.with(|a| *a.borrow_mut() = ActiveUda::Histogram);
                histogram::merge(&serialized_state)
            }
            n => Err(format!(
                "guest-uda: merge received {n} bytes; \
                 expected {WEIGHTED_AVG_STATE_LEN} (weighted_avg) \
                 or {HISTOGRAM_STATE_LEN} (histogram)"
            )),
        }
    }

    /// Serialize the current state for transmission to the coordinator.
    ///
    /// Delegates to whichever sub-UDA most recently received data via
    /// `accumulate` or `merge`. Returns an empty vec if `init` was called but
    /// no data has arrived yet (both sub-UDA states are empty).
    fn serialize_state() -> Vec<u8> {
        ACTIVE.with(|a| match *a.borrow() {
            ActiveUda::WeightedAvg => weighted_avg::serialize_state(),
            ActiveUda::Histogram => histogram::serialize_state(),
            // No data accumulated yet; return empty state.
            ActiveUda::None => Vec::new(),
        })
    }

    /// Compute and return the final aggregate result.
    ///
    /// Delegates to whichever sub-UDA most recently received data. Falls back
    /// to weighted_avg null if no UDA was active.
    fn finalize() -> Result<CqlValue, String> {
        ACTIVE.with(|a| match *a.borrow() {
            ActiveUda::WeightedAvg => weighted_avg::finalize(),
            ActiveUda::Histogram => histogram::finalize(),
            // No accumulate/merge was called; weighted_avg returns Null.
            ActiveUda::None => weighted_avg::finalize(),
        })
    }
}

// Register the component implementation with the generated export! macro.
export!(GuestUda);
