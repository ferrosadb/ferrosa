//! Weighted average UDA: `weighted_avg(double, double) -> double`
//!
//! Accumulates (value * weight, sum_weights) pairs. Finalize divides the
//! weighted sum by the total weight. Returns null when no rows were
//! processed. Serialized state is 17 bytes: 8 (f64 weighted_sum) +
//! 8 (f64 weight_sum) + 1 (bool has_data, 0x00 / 0x01, little-endian).

use std::cell::RefCell;

use crate::CqlValue;

// ---------------------------------------------------------------------------
// Thread-local state
// ---------------------------------------------------------------------------

struct State {
    weighted_sum: f64,
    weight_sum: f64,
    has_data: bool,
}

impl State {
    const fn new() -> Self {
        Self {
            weighted_sum: 0.0,
            weight_sum: 0.0,
            has_data: false,
        }
    }

    fn reset(&mut self) {
        self.weighted_sum = 0.0;
        self.weight_sum = 0.0;
        self.has_data = false;
    }
}

thread_local! {
    static STATE: RefCell<State> = RefCell::new(State::new());
}

// ---------------------------------------------------------------------------
// Public interface (called from lib.rs dispatcher)
// ---------------------------------------------------------------------------

/// Reset state, optionally seeding from an initial condition.
///
/// The `init_cond` for weighted_avg is expected to be null or omitted;
/// we reset to zero regardless.
pub fn init(_init_cond: Option<CqlValue>) -> Result<(), String> {
    STATE.with(|s| s.borrow_mut().reset());
    Ok(())
}

/// Process one row: args[0] = value (double), args[1] = weight (double).
pub fn accumulate(args: &[CqlValue]) -> Result<(), String> {
    // Precondition: exactly two arguments
    if args.len() != 2 {
        return Err(format!(
            "weighted_avg expects 2 args, got {}",
            args.len()
        ));
    }

    let value = extract_double(&args[0]).ok_or("weighted_avg: arg 0 is not a double")?;
    let weight = extract_double(&args[1]).ok_or("weighted_avg: arg 1 is not a double")?;

    // Guard: weights must be non-negative
    if weight < 0.0 {
        return Err(format!("weighted_avg: weight must be >= 0, got {weight}"));
    }

    STATE.with(|s| {
        let mut st = s.borrow_mut();
        st.weighted_sum += value * weight;
        st.weight_sum += weight;
        st.has_data = true;
    });

    Ok(())
}

/// Merge a serialized partial state (17 bytes) produced by another replica
/// into the current state. Used for multi-partition aggregation.
///
/// NOTE: In production each UDA would be a separate component; this
/// merge path is shared here only because both UDAs live in one component.
pub fn merge(serialized_state: &[u8]) -> Result<(), String> {
    // Precondition: state buffer must be exactly 17 bytes
    if serialized_state.len() != 17 {
        return Err(format!(
            "weighted_avg: merge expected 17 bytes, got {}",
            serialized_state.len()
        ));
    }

    let weighted_sum = f64::from_le_bytes(serialized_state[0..8].try_into().unwrap());
    let weight_sum = f64::from_le_bytes(serialized_state[8..16].try_into().unwrap());
    let has_data = serialized_state[16] != 0;

    STATE.with(|s| {
        let mut st = s.borrow_mut();
        st.weighted_sum += weighted_sum;
        st.weight_sum += weight_sum;
        st.has_data = st.has_data || has_data;
    });

    Ok(())
}

/// Serialize current state to 17 bytes for transmission to coordinator.
pub fn serialize_state() -> Vec<u8> {
    let mut buf = Vec::with_capacity(17);
    STATE.with(|s| {
        let st = s.borrow();
        buf.extend_from_slice(&st.weighted_sum.to_le_bytes());
        buf.extend_from_slice(&st.weight_sum.to_le_bytes());
        buf.push(u8::from(st.has_data));
    });
    // Postcondition: buffer is exactly 17 bytes
    debug_assert_eq!(buf.len(), 17, "serialize_state produced wrong length");
    buf
}

/// Compute the final result: weighted_sum / weight_sum, or null if no data.
pub fn finalize() -> Result<CqlValue, String> {
    STATE.with(|s| {
        let st = s.borrow();

        if !st.has_data {
            return Ok(CqlValue::Null);
        }

        // Guard: avoid division by zero
        if st.weight_sum == 0.0 {
            return Ok(CqlValue::Null);
        }

        let result = st.weighted_sum / st.weight_sum;
        Ok(CqlValue::DoubleVal(result))
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_double(v: &CqlValue) -> Option<f64> {
    match v {
        CqlValue::DoubleVal(d) => Some(*d),
        CqlValue::FloatVal(f) => Some(f64::from(*f)),
        CqlValue::IntVal(i) => Some(f64::from(*i)),
        CqlValue::BigintVal(i) => Some(*i as f64),
        _ => None,
    }
}
